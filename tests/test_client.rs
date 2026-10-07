//! Integration tests for `ntex_amqp::client`: connector, connection and errors.
use std::sync::{Arc, Mutex};

use ntex::io::Io;
use ntex::server::{TestServer, TestServerBuilder, test_server};
use ntex::service::{Pipeline, boxed, boxed::BoxService, fn_service};
use ntex::util::{ByteString, Bytes, Either};
use ntex::{SharedCfg, rt, time::Millis, time::Seconds, time::timeout, url::Url};
use ntex_amqp::codec::types::Symbol;
use ntex_amqp::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, SaslFrame};
use ntex_amqp::codec::{AmqpCodecError, ProtocolIdError, protocol, protocol::ProtocolId};
use ntex_amqp::error::LinkError;
use ntex_amqp::{
    AmqpServiceConfig, ControlFrame, ControlFrameKind, client, client::ConnectError, server, types,
};
use ntex_error::ErrorDiagnostic;

type Log = Arc<Mutex<Vec<String>>>;

fn log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn push(log: &Log, item: impl Into<String>) {
    log.lock().unwrap().push(item.into());
}

fn amqp_codec() -> AmqpCodec<AmqpFrame> {
    AmqpCodec::new()
}

fn sasl_codec() -> AmqpCodec<SaslFrame> {
    AmqpCodec::new()
}

/// Start a test server which handles raw connections with `f`.
fn raw_server<F, Fut>(f: F) -> TestServer
where
    F: Fn(Io) -> Fut + Send + Clone + 'static,
    Fut: Future<Output = ()> + 'static,
{
    test_server(async move || {
        let f = f.clone();
        fn_service(async move |io: Io| {
            f(io).await;
            Ok::<_, ()>(())
        })
    })
}

/// Client side configuration
fn client_cfg(cfg: AmqpServiceConfig) -> SharedCfg {
    SharedCfg::new("AMQP-CLIENT").add(cfg).build()
}

fn uri(srv: &TestServer) -> Url {
    Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap()
}

async fn connect_cfg(
    srv: &TestServer,
    cfg: AmqpServiceConfig,
) -> Result<client::Client, ntex_error::Error<ConnectError>> {
    Pipeline::new(client_cfg(cfg), client::Connector::new())
        .call(client::Connect::new(uri(srv)))
        .await
}

async fn connect(srv: &TestServer) -> Result<client::Client, ntex_error::Error<ConnectError>> {
    connect_cfg(srv, AmqpServiceConfig::new()).await
}

/// Server side of the plain amqp protocol negotiation, returns client's open frame
async fn negotiate_plain(io: &Io, server_open: protocol::Open) -> protocol::Open {
    let proto = io.recv(&ProtocolIdCodec).await.unwrap().unwrap();
    assert_eq!(proto, ProtocolId::Amqp);
    io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();

    let frame = io.recv(&amqp_codec()).await.unwrap().unwrap();
    let open = match frame.performative() {
        protocol::Frame::Open(open) => open.clone(),
        frame => panic!("unexpected frame: {frame:?}"),
    };
    io.send(
        AmqpFrame::new(0, protocol::Frame::Open(server_open)),
        &amqp_codec(),
    )
    .await
    .unwrap();
    open
}

fn server_open() -> protocol::Open {
    protocol::Open::build()
        .container_id(ByteString::from_static("raw-server"))
        .max_frame_size(65535)
        .channel_max(64)
        .finish()
}

fn link_service() -> BoxService<types::Link<()>, types::Transfer, types::Outcome, LinkError> {
    boxed::service(fn_service(async |_| Ok(types::Outcome::Accept)))
}

/// Start a real amqp test server
fn amqp_server(cfg: AmqpServiceConfig) -> TestServer {
    TestServerBuilder::new(async move || {
        server::Server::builder(|con: server::Handshake| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        })
        .build(
            server::Router::<()>::builder()
                .service("test", async |_: &types::Link<()>| {
                    Ok::<_, LinkError>(link_service())
                })
                .build(),
        )
    })
    .config(SharedCfg::new("AMQP").add(cfg))
    .start()
}

#[ntex::test]
async fn test_client_connect_session_and_transfer() {
    let srv = amqp_server(AmqpServiceConfig::new());

    let client = connect(&srv).await.unwrap();
    let sink = client.sink();
    assert!(sink.is_opened());
    assert!(sink.get_error().is_none());
    assert!(!sink.tag().is_empty());

    rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();

    let delivery = link
        .transfer(Bytes::from_static(b"data"))
        .send()
        .await
        .unwrap();
    let st = delivery.wait().await.unwrap().unwrap();
    assert_eq!(st, protocol::DeliveryState::Accepted(protocol::Accepted {}));

    // session is registered within the connection
    assert!(sink.get_session_by_local_id(0).is_some());

    sink.close().await.unwrap();
    assert!(!sink.is_opened());
}

#[ntex::test]
async fn test_client_close_with_error() {
    let srv = amqp_server(AmqpServiceConfig::new());

    let client = connect(&srv).await.unwrap();
    let sink = client.sink();
    rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let _session = sink.open_session().await.unwrap();
    sink.close_with_error(LinkError::force_detach().description("stop"))
        .await
        .unwrap();
    assert!(!sink.is_opened());
}

/// Client's open frame is generated out of `AmqpServiceConfig`
#[ntex::test]
async fn test_client_open_frame_from_config() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            let open = negotiate_plain(&io, server_open()).await;
            push(&log, open.container_id().to_string());
            push(&log, format!("{:?}", open.hostname()));
            push(&log, open.max_frame_size().to_string());
            push(&log, open.channel_max().to_string());
            push(&log, format!("{:?}", open.idle_time_out()));
            let _ = io.recv(&amqp_codec()).await;
        }
    });

    let cfg = AmqpServiceConfig::new()
        .set_container_id("my-container")
        .set_hostname("my-host")
        .set_max_frame_size(8192)
        .set_channel_max(42)
        .set_idle_timeout(30);

    let client = connect_cfg(&srv, cfg).await.unwrap();
    // remote open frame is used for the remote config
    assert!(client.sink().is_opened());
    drop(client);

    let items = log.lock().unwrap().clone();
    assert_eq!(items[0], "my-container");
    assert_eq!(items[1], "Some(\"my-host\")");
    assert_eq!(items[2], "8192");
    assert_eq!(items[3], "42");
    // idle timeout is configured in seconds, sent in milliseconds
    assert_eq!(items[4], "Some(30000)");
}

/// `Connect::hostname()` overrides configured hostname for the open frame
#[ntex::test]
async fn test_client_connect_hostname_override() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            let open = negotiate_plain(&io, server_open()).await;
            push(&log, format!("{:?}", open.hostname()));
            push(&log, open.container_id().to_string());
            let _ = io.recv(&amqp_codec()).await;
        }
    });

    let cfg = AmqpServiceConfig::new().set_hostname("cfg-host");
    let connect = client::Connect::new(uri(&srv)).hostname("connect-host");
    assert!(format!("{connect:?}").contains("connect-host"));

    let client = Pipeline::new(client_cfg(cfg), client::Connector::new())
        .call(connect)
        .await
        .unwrap();
    drop(client);

    let items = log.lock().unwrap().clone();
    assert_eq!(items[0], "Some(\"connect-host\")");
    // container id is randomly generated when it is not configured
    assert!(!items[1].is_empty());
}

#[ntex::test]
async fn test_client_connect_error() {
    // take an address of a closed port
    let lst = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = lst.local_addr().unwrap();
    drop(lst);

    let uri = Url::try_from(format!("amqp://{}:{}", addr.ip(), addr.port())).unwrap();
    let err = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .err()
        .unwrap();

    assert!(matches!(*err, ConnectError::Connect(_)), "{err:?}");
    assert_eq!(err.to_string(), "Amqp connect");
    // connect errors delegate the diagnostic signature to the io layer
    assert!(!err.signature().is_empty());
    assert!(matches!((*err).clone(), ConnectError::Connect(_)));
    assert!(
        !ConnectError::Io(std::io::Error::other("x"))
            .signature()
            .is_empty()
    );
}

#[ntex::test]
async fn test_client_handshake_timeout() {
    // server does not reply to the protocol header
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        ntex::time::sleep(Millis(10_000)).await;
    });

    let cfg = AmqpServiceConfig::new().set_handshake_timeout(Seconds(1));
    let err = connect_cfg(&srv, cfg).await.err().unwrap();

    assert!(matches!(*err, ConnectError::HandshakeTimeout), "{err:?}");
    assert_eq!(err.to_string(), "Handshake timeout");
    assert_eq!(err.signature(), "amqp-client-HandshakeTimeout");
}

#[ntex::test]
async fn test_client_disconnect_during_protocol_negotiation() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.close();
    });

    let err = connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::Disconnected), "{err:?}");
    assert_eq!(err.to_string(), "Peer disconnected");
    assert_eq!(err.signature(), "amqp-client-Disconnected");
}

#[ntex::test]
async fn test_client_protocol_negotiation_mismatch() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.send(ProtocolId::AmqpTls, &ProtocolIdCodec)
            .await
            .unwrap();
        let _ = io.recv(&ProtocolIdCodec).await;
    });

    let err = connect(&srv).await.err().unwrap();
    assert!(
        matches!(
            *err,
            ConnectError::ProtocolNegotiation(ProtocolIdError::Unexpected {
                exp: ProtocolId::Amqp,
                got: ProtocolId::AmqpTls
            })
        ),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "Protocol negotiation");
    assert_eq!(err.signature(), "amqp-client-ProtocolNegotiation");
}

#[ntex::test]
async fn test_client_disconnect_before_open() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();
        let _ = io.recv(&amqp_codec()).await;
        io.close();
    });

    let err = connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::Disconnected), "{err:?}");
}

#[ntex::test]
async fn test_client_expect_open_frame() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();
        let _ = io.recv(&amqp_codec()).await;
        io.send(
            AmqpFrame::new(
                0,
                protocol::Begin(Box::new(protocol::BeginInner {
                    remote_channel: None,
                    next_outgoing_id: 1,
                    incoming_window: 0,
                    outgoing_window: 0,
                    handle_max: 10,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                }))
                .into(),
            ),
            &amqp_codec(),
        )
        .await
        .unwrap();
        let _ = io.recv(&amqp_codec()).await;
    });

    let err = connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::ExpectOpenFrame(_)), "{err:?}");
    assert!(
        err.to_string().starts_with("Expect open frame, got:"),
        "{err}"
    );
    assert_eq!(err.signature(), "amqp-client-ExpectOpenFrame");
}

#[ntex::test]
async fn test_client_invalid_max_frame_size() {
    let srv = raw_server(move |io: Io| async move {
        let open = protocol::Open::build()
            .container_id(ByteString::from_static("raw-server"))
            .max_frame_size(100)
            .finish();
        negotiate_plain(&io, open).await;
        let _ = io.recv(&amqp_codec()).await;
    });

    let err = connect(&srv).await.err().unwrap();
    assert!(
        matches!(*err, ConnectError::InvalidMaxFrameSize(100)),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "Invalid remote max frame size: 100");
    assert_eq!(err.signature(), "amqp-client-InvalidMaxFrameSize");
}

/// Server side of the sasl negotiation up to and including the outcome
async fn negotiate_sasl(io: &Io, code: protocol::SaslCode, log: &Log) {
    let proto = io.recv(&ProtocolIdCodec).await.unwrap().unwrap();
    push(log, format!("{proto:?}"));
    io.send(ProtocolId::AmqpSasl, &ProtocolIdCodec)
        .await
        .unwrap();

    let mechs = protocol::SaslMechanisms {
        sasl_server_mechanisms: vec![Symbol::from("PLAIN")].into(),
    };
    io.send(mechs.into(), &sasl_codec()).await.unwrap();

    let frame = io.recv(&sasl_codec()).await.unwrap().unwrap();
    match frame.body {
        protocol::SaslFrameBody::SaslInit(init) => {
            push(log, init.mechanism().as_str().to_string());
            push(log, format!("{:?}", init.hostname()));
            push(
                log,
                String::from_utf8_lossy(init.initial_response().unwrap()).into_owned(),
            );
        }
        body => panic!("unexpected sasl body: {body:?}"),
    }

    io.send(
        protocol::SaslOutcome {
            code,
            additional_data: None,
        }
        .into(),
        &sasl_codec(),
    )
    .await
    .unwrap();
}

#[ntex::test]
async fn test_client_sasl_connect() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_sasl(&io, protocol::SaslCode::Ok, &log).await;
            let open = negotiate_plain(&io, server_open()).await;
            push(&log, open.container_id().to_string());
            let _ = io.recv(&amqp_codec()).await;
        }
    });

    let cfg = AmqpServiceConfig::new()
        .set_hostname("sasl-host")
        .set_container_id("sasl-client");
    let connect = client::Connect::new(uri(&srv)).sasl_auth(
        ByteString::from_static("authz"),
        ByteString::from_static("user"),
        ByteString::from_static("pass"),
    );
    let client = Pipeline::new(client_cfg(cfg), client::Connector::new())
        .call(connect)
        .await
        .unwrap();
    assert!(client.sink().is_opened());
    drop(client);

    let items = log.lock().unwrap().clone();
    assert_eq!(items[0], "AmqpSasl");
    assert_eq!(items[1], "PLAIN");
    assert_eq!(items[2], "Some(\"sasl-host\")");
    assert_eq!(items[3], "authz\0user\0pass");
    assert_eq!(items[4], "sasl-client");
}

#[ntex::test]
async fn test_client_sasl_auth_failure() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_sasl(&io, protocol::SaslCode::Auth, &log).await;
            let _ = io.recv(&sasl_codec()).await;
        }
    });

    let err = sasl_connect(&srv).await.err().unwrap();
    assert!(
        matches!(*err, ConnectError::Sasl(protocol::SaslCode::Auth)),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "Sasl error code Auth");
    assert_eq!(err.signature(), "amqp-client-Sasl");
}

async fn sasl_connect(srv: &TestServer) -> Result<client::Client, ntex_error::Error<ConnectError>> {
    let connect = client::Connect::new(uri(srv)).sasl_auth(
        ByteString::from_static("authz"),
        ByteString::from_static("user"),
        ByteString::from_static("pass"),
    );
    Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(connect)
        .await
}

#[ntex::test]
async fn test_client_sasl_protocol_mismatch() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();
        let _ = io.recv(&ProtocolIdCodec).await;
    });

    let err = sasl_connect(&srv).await.err().unwrap();
    assert!(
        matches!(
            *err,
            ConnectError::ProtocolNegotiation(ProtocolIdError::Unexpected {
                exp: ProtocolId::AmqpSasl,
                got: ProtocolId::Amqp
            })
        ),
        "{err:?}"
    );
}

#[ntex::test]
async fn test_client_sasl_disconnect_after_protocol_id() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.close();
    });

    let err = sasl_connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::Disconnected), "{err:?}");
}

#[ntex::test]
async fn test_client_sasl_disconnect_instead_of_mechanisms() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.send(ProtocolId::AmqpSasl, &ProtocolIdCodec)
            .await
            .unwrap();
        io.close();
    });

    let err = sasl_connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::Disconnected), "{err:?}");
}

/// client expects sasl outcome, anything else is treated as a disconnect
#[ntex::test]
async fn test_client_sasl_challenge_instead_of_outcome() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.send(ProtocolId::AmqpSasl, &ProtocolIdCodec)
            .await
            .unwrap();
        io.send(
            protocol::SaslMechanisms {
                sasl_server_mechanisms: vec![Symbol::from("PLAIN")].into(),
            }
            .into(),
            &sasl_codec(),
        )
        .await
        .unwrap();
        let _ = io.recv(&sasl_codec()).await;
        io.send(
            protocol::SaslChallenge {
                challenge: Bytes::from_static(b"c"),
            }
            .into(),
            &sasl_codec(),
        )
        .await
        .unwrap();
        let _ = io.recv(&sasl_codec()).await;
    });

    let err = sasl_connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::Disconnected), "{err:?}");
}

#[ntex::test]
async fn test_client_sasl_disconnect_after_outcome() {
    let log = log();
    let log2 = log.clone();
    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_sasl(&io, protocol::SaslCode::Ok, &log).await;
            let _ = io.recv(&ProtocolIdCodec).await;
            io.close();
        }
    });

    let err = sasl_connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::Disconnected), "{err:?}");
}

/// `Connector::negotiate` / `negotiate_sasl` run over an already connected socket
#[ntex::test]
async fn test_client_negotiate_over_existing_io() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            let open = negotiate_plain(&io, server_open()).await;
            push(&log, format!("{:?}", open.hostname()));
            let _ = io.recv(&amqp_codec()).await;
        }
    });

    let shared = client_cfg(AmqpServiceConfig::new().set_container_id("negotiated"));
    let cfg = shared.get::<AmqpServiceConfig>();

    let io = ntex::connect::connect(uri(&srv)).await.unwrap();
    let connector = client::Connector::<Url, _>::with(ntex::connect::Connector::default());
    let client = connector
        .negotiate(
            io.into(),
            Some(ByteString::from_static("negotiate-host")),
            cfg,
        )
        .await
        .unwrap();
    assert!(client.sink().is_opened());
    drop(client);

    let items = log.lock().unwrap().clone();
    assert_eq!(items[0], "Some(\"negotiate-host\")");
}

#[ntex::test]
async fn test_client_negotiate_sasl_over_existing_io() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_sasl(&io, protocol::SaslCode::Ok, &log).await;
            negotiate_plain(&io, server_open()).await;
            let _ = io.recv(&amqp_codec()).await;
        }
    });

    let shared = client_cfg(AmqpServiceConfig::new());
    let cfg = shared.get::<AmqpServiceConfig>();

    let io = ntex::connect::connect(uri(&srv)).await.unwrap();
    let connector =
        client::Connector::<Url, ()>::new().connector(ntex::connect::Connector::default());
    let client = connector
        .negotiate_sasl(
            io.into(),
            client::SaslAuth {
                authz_id: ByteString::from_static("z"),
                authn_id: ByteString::from_static("n"),
                password: ByteString::from_static("p"),
            },
            None,
            cfg,
        )
        .await
        .unwrap();
    assert!(client.sink().is_opened());
    drop(client);

    let items = log.lock().unwrap().clone();
    assert_eq!(items[3], "z\0n\0p");
}

/// `Client::state()` and `Client::start()` with a control service
#[ntex::test]
async fn test_client_control_service() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| async move {
        negotiate_plain(&io, server_open()).await;
        // close connection from the server side
        io.send(
            AmqpFrame::new(
                0,
                protocol::Close {
                    error: Some(
                        protocol::Error::build()
                            .condition(protocol::ErrorCondition::Custom(Symbol::from(
                                "amqp:internal-error",
                            )))
                            .description(ByteString::from_static("stop"))
                            .finish(),
                    ),
                }
                .into(),
            ),
            &amqp_codec(),
        )
        .await
        .unwrap();
        let _ = io.recv(&amqp_codec()).await;
    });

    let client = connect(&srv).await.unwrap().state(123usize);
    let sink = client.sink();

    let res = timeout(
        Millis(5_000),
        client.start(fn_service(async move |frame: ControlFrame| {
            push(&log2, format!("{:?}", frame.kind()));
            Ok::<_, LinkError>(())
        })),
    )
    .await
    .unwrap();
    // remote close with an error is reported as a dispatcher error
    let err = res.err().unwrap();
    assert!(err.to_string().contains("InternalError"), "{err}");

    let items = log.lock().unwrap().clone();
    assert!(
        items.iter().any(|i| i.starts_with("ProtocolError")),
        "{items:?}"
    );
    assert!(!sink.is_opened());
    assert!(sink.get_error().is_some());
}

#[ntex::test]
async fn test_client_error_conversions() {
    let err: ConnectError =
        Either::<AmqpCodecError, std::io::Error>::Left(AmqpCodecError::MaxSizeExceeded).into();
    assert!(matches!(err, ConnectError::Codec(_)));
    assert_eq!(err.to_string(), "Amqp codec");
    assert_eq!(err.signature(), "amqp-client-Codec");
    assert_eq!(err.clone().to_string(), err.to_string());

    let err: ConnectError =
        Either::<AmqpCodecError, std::io::Error>::Right(std::io::Error::other("io-err")).into();
    assert!(matches!(err, ConnectError::Io(_)));
    assert_eq!(err.to_string(), "");
    assert!(matches!(err.clone(), ConnectError::Io(_)));

    let err: ConnectError =
        Either::<ProtocolIdError, std::io::Error>::Left(ProtocolIdError::Unknown).into();
    assert!(matches!(
        err,
        ConnectError::ProtocolNegotiation(ProtocolIdError::Unknown)
    ));
    assert!(matches!(
        err.clone(),
        ConnectError::ProtocolNegotiation(ProtocolIdError::Unknown)
    ));

    let err: ConnectError = Either::<ProtocolIdError, std::io::Error>::Right(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "pipe",
    ))
    .into();
    assert!(matches!(err, ConnectError::Io(_)));

    let err = ConnectError::ExpectOpenFrame(Box::new(AmqpFrame::new(
        3,
        protocol::Close { error: None }.into(),
    )));
    assert!(matches!(err.clone(), ConnectError::ExpectOpenFrame(_)));

    let err = ConnectError::Sasl(protocol::SaslCode::Sys);
    assert_eq!(err.to_string(), "Sasl error code Sys");
    assert!(matches!(
        err.clone(),
        ConnectError::Sasl(protocol::SaslCode::Sys)
    ));

    let err = ConnectError::InvalidMaxFrameSize(7);
    assert_eq!(err.clone().to_string(), "Invalid remote max frame size: 7");

    let err = ConnectError::HandshakeTimeout;
    assert!(matches!(err.clone(), ConnectError::HandshakeTimeout));
    let err = ConnectError::Disconnected;
    assert!(matches!(err.clone(), ConnectError::Disconnected));

    let err = ConnectError::from(AmqpCodecError::InvalidFrameSize);
    assert_eq!(err.signature(), "amqp-client-Codec");
}

/// control frames are delivered for remotely initiated session end
#[ntex::test]
async fn test_client_control_session_ended() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| async move {
        negotiate_plain(&io, server_open()).await;
        // wait for begin frame
        let frame = io.recv(&amqp_codec()).await.unwrap().unwrap();
        let channel = frame.channel_id();
        assert!(matches!(frame.performative(), protocol::Frame::Begin(_)));
        io.send(
            AmqpFrame::new(
                channel,
                protocol::Begin(Box::new(protocol::BeginInner {
                    remote_channel: Some(channel),
                    next_outgoing_id: 1,
                    incoming_window: 100,
                    outgoing_window: 100,
                    handle_max: 10,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                }))
                .into(),
            ),
            &amqp_codec(),
        )
        .await
        .unwrap();
        // end the session remotely
        io.send(
            AmqpFrame::new(channel, protocol::End { error: None }.into()),
            &amqp_codec(),
        )
        .await
        .unwrap();
        let _ = io.recv(&amqp_codec()).await;
        let _ = io.recv(&amqp_codec()).await;
    });

    let client = connect(&srv).await.unwrap();
    let sink = client.sink();
    rt::spawn(async move {
        let _ = client
            .start(fn_service(async move |frame: ControlFrame| {
                if let ControlFrameKind::RemoteSessionEnded(_) = frame.kind() {
                    push(&log2, "RemoteSessionEnded");
                }
                Ok::<_, LinkError>(())
            }))
            .await;
    });

    let session = timeout(Millis(5_000), sink.open_session())
        .await
        .unwrap()
        .unwrap();
    drop(session);

    for _ in 0..100 {
        if !log.lock().unwrap().is_empty() {
            break;
        }
        ntex::time::sleep(Millis(10)).await;
    }
    assert_eq!(log.lock().unwrap().clone(), vec!["RemoteSessionEnded"]);
}
