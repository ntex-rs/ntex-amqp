//! Integration tests for `ntex_amqp::client`: connector, connection and errors.
use std::sync::{Arc, Mutex};

use ntex::io::Io;
use ntex::server::{TestServer, TestServerBuilder, test_server};
use ntex::service::{Pipeline, boxed, boxed::BoxService, fn_service};
use ntex::util::{ByteString, Bytes, Either};
use ntex::{SharedCfg, rt, time::Millis, time::Seconds, time::timeout};
use ntex_amqp::codec::types::Symbol;
use ntex_amqp::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, SaslFrame};
use ntex_amqp::codec::{AmqpCodecError, ProtocolIdError, protocol, protocol::ProtocolId};
use ntex_amqp::error::LinkError;
use ntex_amqp::{
    AmqpServiceConfig, ControlFrame, ControlFrameKind, client, client::ConnectError, server, types,
};
use ntex_error::ErrorDiagnostic;
use urly::Url;

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
    test_server(async move |_| {
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
async fn negotiate_plain(io: &Io, server_open: protocol::Open) {
    negotiate_plain_with(io, server_open, |_| ()).await;
}

/// Server side of the plain amqp protocol negotiation, `f` inspects client's
/// open frame before the reply, so it completes before the client is connected
async fn negotiate_plain_with(
    io: &Io,
    server_open: protocol::Open,
    f: impl FnOnce(&protocol::Open),
) {
    let proto = io.recv(&ProtocolIdCodec).await.unwrap().unwrap();
    assert_eq!(proto, ProtocolId::Amqp);
    io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();

    let frame = io.recv(&amqp_codec()).await.unwrap().unwrap();
    match frame.performative() {
        protocol::Frame::Open(open) => f(open),
        frame => panic!("unexpected frame: {frame:?}"),
    }
    io.send(
        AmqpFrame::new(0, protocol::Frame::Open(server_open)),
        &amqp_codec(),
    )
    .await
    .unwrap();
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
    TestServerBuilder::new(async move |_| {
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

/// STATEMENT: A client connects to an AMQP server, opens a session and sender link,
/// gets a transfer accepted, and closes cleanly.
///
/// HOW:
/// - Connect to `amqp_server`; the sink is opened, has no error and a non-empty tag.
/// - Open a session, attach a sender link and send a transfer; it is `Accepted`.
/// - The session is found by local id 0; after `close` the sink is no longer opened.
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

/// STATEMENT: A client sends keepalive frames before a short remote idle timeout
/// expires, with both the default and a custom control service.
///
/// HOW:
/// - A raw server advertises a 1s `idle_time_out` in its `Open`.
/// - Start the client with `start_default` or a custom control service.
/// - The server receives an `Empty` frame within 900ms and logs `ping`.
#[ntex::test]
async fn test_client_short_remote_idle_timeout() {
    for custom_control in [false, true] {
        let log = log();
        let log2 = log.clone();
        let srv = raw_server(move |io: Io| {
            let log = log2.clone();
            async move {
                let mut open = server_open();
                open.0.idle_time_out = Some(1_000);
                negotiate_plain(&io, open).await;
                let frame = timeout(Millis(900), io.recv(&amqp_codec()))
                    .await
                    .expect("no keepalive before the peer idle deadline")
                    .unwrap()
                    .unwrap();
                assert!(matches!(frame.performative(), protocol::Frame::Empty));
                push(&log, "ping");
            }
        });

        let client = connect(&srv).await.unwrap();
        let _ = timeout(Millis(2_000), async move {
            if custom_control {
                client
                    .start(fn_service(async |_: ControlFrame| Ok::<_, LinkError>(())))
                    .await
            } else {
                client.start_default().await
            }
        })
        .await
        .unwrap();
        assert_eq!(*log.lock().unwrap(), ["ping"]);
    }
}

/// STATEMENT: The server sends keepalive frames before a short client idle timeout
/// expires.
///
/// HOW:
/// - Negotiate with `amqp_server` over a raw connection, sending an `Open` with a 1s
///   `idle_time_out`, and receive the server `Open`.
/// - Assert an `Empty` frame arrives within 900ms.
#[ntex::test]
async fn test_server_short_remote_idle_timeout() {
    let srv = amqp_server(AmqpServiceConfig::new());
    let io = ntex::connect::connect(uri(&srv)).await.unwrap();
    io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();
    assert_eq!(
        io.recv(&ProtocolIdCodec).await.unwrap(),
        Some(ProtocolId::Amqp)
    );
    let mut open = AmqpServiceConfig::new().to_open();
    open.0.idle_time_out = Some(1_000);
    io.send(AmqpFrame::new(0, open.into()), &amqp_codec())
        .await
        .unwrap();
    assert!(matches!(
        io.recv(&amqp_codec())
            .await
            .unwrap()
            .unwrap()
            .performative(),
        protocol::Frame::Open(_)
    ));

    let frame = timeout(Millis(900), io.recv(&amqp_codec()))
        .await
        .expect("no keepalive before the peer idle deadline")
        .unwrap()
        .unwrap();
    assert!(matches!(frame.performative(), protocol::Frame::Empty));
}

/// STATEMENT: `close_with_error` closes an open client connection.
///
/// HOW:
/// - Connect to `amqp_server` and open a session.
/// - Call `close_with_error` with a force-detach error; it succeeds.
/// - Assert the sink is no longer opened.
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

/// STATEMENT: The client's `Open` frame is generated from `AmqpServiceConfig`.
///
/// HOW:
/// - Configure container id, hostname, max frame size, channel max and idle timeout.
/// - A raw server logs the received `Open` fields; the client connects and is opened.
/// - Assert each field matches, with the 30s idle timeout sent as 30000ms.
#[ntex::test]
async fn test_client_open_frame_from_config() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_plain_with(&io, server_open(), |open| {
                push(&log, open.container_id().to_string());
                push(&log, format!("{:?}", open.hostname()));
                push(&log, open.max_frame_size().to_string());
                push(&log, open.channel_max().to_string());
                push(&log, format!("{:?}", open.idle_time_out()));
            })
            .await;
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

/// STATEMENT: `Connect::hostname()` overrides the configured hostname in the `Open`
/// frame.
///
/// HOW:
/// - Configure `cfg-host` and connect with `Connect::hostname("connect-host")`, whose
///   `Debug` output contains the override.
/// - A raw server logs the `Open` hostname `connect-host`.
/// - The unconfigured container id is generated and non-empty.
#[ntex::test]
async fn test_client_connect_hostname_override() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_plain_with(&io, server_open(), |open| {
                push(&log, format!("{:?}", open.hostname()));
                push(&log, open.container_id().to_string());
            })
            .await;
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

/// STATEMENT: Connecting to a closed port fails with `ConnectError::Connect`, which
/// delegates its diagnostic signature to the io layer.
///
/// HOW:
/// - Bind and drop a listener to get a closed port, then connect to it.
/// - Assert `ConnectError::Connect`, the "Amqp connect" message and a non-empty
///   signature that survives `clone`.
/// - Assert `ConnectError::Io` also has a non-empty signature.
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

/// STATEMENT: The client fails with `HandshakeTimeout` when the server does not reply
/// within the configured handshake timeout.
///
/// HOW:
/// - A raw server reads the protocol header and never replies.
/// - Connect with a 1s handshake timeout.
/// - Assert `HandshakeTimeout`, its message and `amqp-client-HandshakeTimeout`.
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

/// STATEMENT: The client fails with `Disconnected` when the server closes during
/// protocol negotiation.
///
/// HOW:
/// - A raw server reads the protocol header and closes the connection.
/// - Assert `Disconnected`, "Peer disconnected" and `amqp-client-Disconnected`.
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

/// STATEMENT: The client fails with `ProtocolNegotiation` when the server replies with
/// an unexpected protocol id.
///
/// HOW:
/// - A raw server answers the plain AMQP header with `AmqpTls`.
/// - Assert `ProtocolIdError::Unexpected` with `exp: Amqp` and `got: AmqpTls`.
/// - Assert the message and `amqp-client-ProtocolNegotiation` signature.
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

/// STATEMENT: The client fails with `Disconnected` when the server closes before
/// sending its `Open` frame.
///
/// HOW:
/// - A raw server completes protocol negotiation, reads the client `Open`, and closes.
/// - Assert the connect error is `Disconnected`.
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

/// STATEMENT: The client fails with `ExpectOpenFrame` when the server answers its
/// `Open` with another performative.
///
/// HOW:
/// - A raw server completes protocol negotiation and replies with a `Begin` frame.
/// - Assert `ExpectOpenFrame`, an "Expect open frame, got:" message prefix and the
///   `amqp-client-ExpectOpenFrame` signature.
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

/// STATEMENT: The client rejects a remote `Open` with a too small max frame size.
///
/// HOW:
/// - A raw server replies with an `Open` advertising `max_frame_size` 100.
/// - Assert `InvalidMaxFrameSize(100)`, its message and
///   `amqp-client-InvalidMaxFrameSize` signature.
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

/// STATEMENT: A client with SASL auth negotiates PLAIN with its credentials and
/// hostname, then opens the AMQP connection.
///
/// HOW:
/// - A raw server runs SASL with outcome `Ok`, then plain AMQP negotiation.
/// - Connect with `sasl_auth("authz", "user", "pass")` and a configured hostname.
/// - Assert the server logged `AmqpSasl`, `PLAIN`, the hostname, the
///   `authz\0user\0pass` response and the configured container id.
#[ntex::test]
async fn test_client_sasl_connect() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_sasl(&io, protocol::SaslCode::Ok, &log).await;
            negotiate_plain_with(&io, server_open(), |open| {
                push(&log, open.container_id().to_string());
            })
            .await;
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

/// STATEMENT: A SASL outcome other than `Ok` fails the connect with `ConnectError::Sasl`.
///
/// HOW:
/// - A raw server runs SASL negotiation and returns outcome code `Auth`.
/// - Assert `Sasl(Auth)`, "Sasl error code Auth" and `amqp-client-Sasl`.
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

/// STATEMENT: A SASL client fails with `ProtocolNegotiation` when the server replies
/// with the plain AMQP protocol id.
///
/// HOW:
/// - A raw server answers the SASL protocol header with `Amqp`.
/// - Assert `ProtocolIdError::Unexpected` with `exp: AmqpSasl` and `got: Amqp`.
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

/// STATEMENT: A SASL client fails with `Disconnected` when the server closes during
/// protocol negotiation.
///
/// HOW:
/// - A raw server reads the SASL protocol header and closes the connection.
/// - Assert the connect error is `Disconnected`.
#[ntex::test]
async fn test_client_sasl_disconnect_after_protocol_id() {
    let srv = raw_server(move |io: Io| async move {
        let _ = io.recv(&ProtocolIdCodec).await;
        io.close();
    });

    let err = sasl_connect(&srv).await.err().unwrap();
    assert!(matches!(*err, ConnectError::Disconnected), "{err:?}");
}

/// STATEMENT: A SASL client fails with `Disconnected` when the server closes instead of
/// sending SASL mechanisms.
///
/// HOW:
/// - A raw server answers with `AmqpSasl` and closes the connection.
/// - Assert the connect error is `Disconnected`.
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

/// STATEMENT: A SASL client expects a SASL outcome after init, and treats any other
/// SASL frame as a disconnect.
///
/// HOW:
/// - A raw server sends PLAIN mechanisms, reads the init and replies with a
///   `SaslChallenge`.
/// - Assert the connect error is `Disconnected`.
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

/// STATEMENT: A SASL client fails with `Disconnected` when the server closes after a
/// successful SASL outcome.
///
/// HOW:
/// - A raw server completes SASL with `Ok`, reads the AMQP protocol header and closes.
/// - Assert the connect error is `Disconnected`.
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

/// STATEMENT: `Connector::negotiate` runs plain AMQP negotiation over an already
/// connected socket, using the given hostname.
///
/// HOW:
/// - Connect a raw socket to a raw server and call `negotiate` with `negotiate-host`.
/// - Assert the client is opened and the server logged the `Open` hostname.
#[ntex::test]
async fn test_client_negotiate_over_existing_io() {
    let log = log();
    let log2 = log.clone();

    let srv = raw_server(move |io: Io| {
        let log = log2.clone();
        async move {
            negotiate_plain_with(&io, server_open(), |open| {
                push(&log, format!("{:?}", open.hostname()));
            })
            .await;
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

/// STATEMENT: `Connector::negotiate_sasl` runs SASL and AMQP negotiation over an
/// already connected socket.
///
/// HOW:
/// - Connect a raw socket to a raw server running SASL with outcome `Ok`.
/// - Call `negotiate_sasl` with `SaslAuth` credentials; the client is opened.
/// - Assert the server logged the `z\0n\0p` initial response.
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

/// STATEMENT: A client started with `Client::state()` and a control service reports a
/// remote `Close` with an error to the control service and as a dispatcher error.
///
/// HOW:
/// - A raw server opens the connection, then sends `Close` with an internal error.
/// - Start the client with a control service that logs frame kinds.
/// - Assert `start` fails with an `InternalError`, a `ProtocolError` frame was logged,
///   and the sink is closed with an error.
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

/// STATEMENT: Codec, protocol id and io errors convert into the matching
/// `ConnectError` variants, and cloned variants keep their kind and message.
///
/// HOW:
/// - Convert `Either` codec, protocol id and io errors and assert the variants,
///   messages and `amqp-client-Codec` signature.
/// - Clone `ExpectOpenFrame`, `Sasl`, `InvalidMaxFrameSize`, `HandshakeTimeout` and
///   `Disconnected` and assert the variant or message is kept.
/// - Assert `ConnectError::from(AmqpCodecError)` has the codec signature.
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

/// STATEMENT: A remotely initiated session end is delivered to the client's control
/// service as `RemoteSessionEnded`.
///
/// HOW:
/// - A raw server answers the client `Begin`, then sends `End` for that channel.
/// - Start the client with a control service that logs `RemoteSessionEnded`.
/// - Open a session and poll until the log equals `["RemoteSessionEnded"]`.
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
