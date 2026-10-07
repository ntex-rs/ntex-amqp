//! Integration tests for the server side handshake and sasl negotiation.
use std::sync::{Arc, Mutex};
use std::{future::Future, net};

use ntex::server::{TestServer, TestServerBuilder, test_server};
use ntex::service::{Pipeline, boxed, boxed::BoxService, fn_service};
use ntex::time::{Millis, Seconds, sleep, timeout};
use ntex::util::{ByteString, Bytes};
use ntex::{SharedCfg, io::Io, url::Url};
use ntex_amqp::codec::protocol::{self, ProtocolId, SaslCode, SaslFrameBody, SaslOutcome};
use ntex_amqp::codec::types::Symbol;
use ntex_amqp::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, SaslFrame};
use ntex_amqp::error::LinkError;
use ntex_amqp::{AmqpServiceConfig, ControlFrame, client, server, types};

// ======================= helpers =======================

type Log = Arc<Mutex<Vec<String>>>;

fn log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn push(log: &Log, item: impl Into<String>) {
    log.lock().unwrap().push(item.into());
}

/// Wait until the server side recorded at least `n` entries.
async fn wait_log(log: &Log, n: usize) -> Vec<String> {
    for _ in 0..200 {
        if log.lock().unwrap().len() >= n {
            break;
        }
        sleep(Millis(10)).await;
    }
    let items = log.lock().unwrap().clone();
    assert!(
        items.len() >= n,
        "expected at least {n} log entries, got {items:?}"
    );
    items
}

async fn link_service(
    _: &types::Link<()>,
) -> Result<BoxService<types::Link<()>, types::Transfer, types::Outcome, LinkError>, LinkError> {
    Ok(boxed::service(fn_service(async |_| {
        Ok::<_, LinkError>(types::Outcome::Accept)
    })))
}

/// Start an amqp server with the given handshake handler and configuration.
fn start_server<F, Fut>(cfg: AmqpServiceConfig, f: F) -> TestServer
where
    F: Fn(server::Handshake) -> Fut + Send + Clone + 'static,
    Fut: Future<Output = Result<server::HandshakeAck<()>, server::HandshakeError>> + 'static,
{
    TestServerBuilder::new(async move || {
        let f = f.clone();
        server::Server::builder(async move |conn: server::Handshake| {
            assert!(!conn.io().tag().is_empty());
            f(conn).await.map_err(|_| ())
        })
        .build(
            server::Router::<()>::builder()
                .service("test", link_service)
                .build(),
        )
    })
    .config(SharedCfg::new("AMQP").add(cfg))
    .start()
}

fn sasl_codec() -> AmqpCodec<SaslFrame> {
    AmqpCodec::new()
}

fn amqp_codec() -> AmqpCodec<AmqpFrame> {
    AmqpCodec::new()
}

/// Connect and negotiate the `AMQP3` (sasl) protocol header.
async fn sasl_connect(addr: net::SocketAddr) -> Io {
    let io = ntex::connect::connect(addr).await.unwrap();
    io.send(ProtocolId::AmqpSasl, &ProtocolIdCodec)
        .await
        .unwrap();
    assert_eq!(
        timeout(Millis(2000), io.recv(&ProtocolIdCodec))
            .await
            .unwrap()
            .unwrap(),
        Some(ProtocolId::AmqpSasl)
    );
    io
}

async fn sasl_recv(io: &Io) -> SaslFrameBody {
    timeout(Millis(2000), io.recv(&sasl_codec()))
        .await
        .unwrap()
        .unwrap()
        .expect("connection closed, expected a sasl frame")
        .body
}

async fn sasl_send(io: &Io, body: SaslFrameBody) {
    io.send(SaslFrame { body }, &sasl_codec()).await.unwrap();
}

/// Read the advertised mechanisms.
async fn recv_mechanisms(io: &Io) -> Vec<String> {
    match sasl_recv(io).await {
        SaslFrameBody::SaslMechanisms(m) => m
            .sasl_server_mechanisms()
            .iter()
            .map(|s| s.as_str().to_string())
            .collect(),
        body => panic!("unexpected sasl frame: {body:?}"),
    }
}

fn sasl_init(
    mechanism: &'static str,
    response: Option<Bytes>,
    hostname: Option<ByteString>,
) -> SaslFrameBody {
    SaslFrameBody::SaslInit(protocol::SaslInit {
        mechanism: Symbol::from_static(mechanism),
        initial_response: response,
        hostname,
    })
}

async fn expect_outcome(io: &Io, code: SaslCode) {
    match sasl_recv(io).await {
        SaslFrameBody::SaslOutcome(outcome) => assert_eq!(outcome.code(), code),
        body => panic!("unexpected sasl frame: {body:?}"),
    }
}

fn client_open() -> protocol::Open {
    protocol::Open::build()
        .container_id(ByteString::from_static("test-client"))
        .hostname(ByteString::from_static("test-host"))
        .max_frame_size(65535)
        .channel_max(17)
        .idle_time_out(45_000)
        .finish()
}

fn begin_frame() -> protocol::Frame {
    protocol::Begin(Box::new(protocol::BeginInner {
        remote_channel: None,
        next_outgoing_id: 1,
        incoming_window: 100,
        outgoing_window: 100,
        handle_max: 10,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }))
    .into()
}

/// Negotiate the plain amqp protocol header.
async fn amqp_hello(io: &Io) {
    io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();
    assert_eq!(
        timeout(Millis(2000), io.recv(&ProtocolIdCodec))
            .await
            .unwrap()
            .unwrap(),
        Some(ProtocolId::Amqp)
    );
}

/// Negotiate protocol header and exchange open frames.
async fn amqp_open(io: &Io) -> protocol::Open {
    amqp_hello(io).await;
    io.send(AmqpFrame::new(0, client_open().into()), &amqp_codec())
        .await
        .unwrap();

    match timeout(Millis(2000), io.recv(&amqp_codec()))
        .await
        .unwrap()
        .unwrap()
        .expect("expected server Open")
        .performative()
    {
        protocol::Frame::Open(open) => open.clone(),
        frame => panic!("unexpected frame: {frame:?}"),
    }
}

/// Assert that the server dropped the connection.
async fn expect_eof(io: &Io) {
    let res = timeout(Millis(3000), io.recv(&amqp_codec())).await.unwrap();
    assert!(matches!(res, Ok(None) | Err(_)), "{res:?}");
}

/// Default handshake handler: accept both plain and sasl.
async fn accept_all(
    conn: server::Handshake,
) -> Result<server::HandshakeAck<()>, server::HandshakeError> {
    match conn {
        server::Handshake::Amqp(conn) => Ok(conn.open().await?.ack(())),
        server::Handshake::Sasl(conn) => {
            let init = conn
                .mechanism("PLAIN")
                .mechanism("ANONYMOUS")
                .init()
                .await?;
            Ok(init.outcome(SaslCode::Ok).await?.open().await?.ack(()))
        }
    }
}

// ======================= sasl mechanisms / init =======================

#[ntex::test]
async fn test_sasl_mechanisms_advertised() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(
        AmqpServiceConfig::new().set_container_id("srv-mechanisms"),
        move |conn| {
            let log = log2.clone();
            async move {
                match conn {
                    server::Handshake::Amqp(_) => panic!("expected sasl"),
                    server::Handshake::Sasl(conn) => {
                        assert_eq!(conn.st(), &());
                        assert!(!conn.io().is_closed());

                        let init = conn
                            .mechanism("PLAIN")
                            .mechanism("ANONYMOUS")
                            .mechanism("EXTERNAL".to_string())
                            .init()
                            .await?;

                        push(&log, format!("mechanism:{}", init.mechanism()));
                        assert!(format!("{init:?}").starts_with("SaslInit"));
                        push(
                            &log,
                            format!("response:{:?}", init.initial_response().map(<[u8]>::to_vec)),
                        );
                        push(&log, format!("hostname:{:?}", init.hostname()));
                        assert_eq!(init.st(), &());
                        assert!(!init.io().is_closed());

                        Ok(init.outcome(SaslCode::Ok).await?.open().await?.ack(()))
                    }
                }
            }
        },
    );

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(
        recv_mechanisms(&io).await,
        ["PLAIN", "ANONYMOUS", "EXTERNAL"]
    );

    sasl_send(
        &io,
        sasl_init(
            "PLAIN",
            Some(Bytes::from_static(b"\x00user\x00pass")),
            Some(ByteString::from_static("vhost")),
        ),
    )
    .await;
    expect_outcome(&io, SaslCode::Ok).await;

    let items = wait_log(&log, 3).await;
    assert_eq!(items[0], "mechanism:PLAIN");
    assert_eq!(
        items[1],
        "response:Some([0, 117, 115, 101, 114, 0, 112, 97, 115, 115])"
    );
    assert_eq!(items[2], "hostname:Some(\"vhost\")");

    // connection is usable after sasl
    let open = amqp_open(&io).await;
    assert_eq!(open.container_id(), "srv-mechanisms");
    io.close();
}

#[ntex::test]
async fn test_sasl_anonymous_without_initial_response() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("ANONYMOUS").init().await?;
                    push(&log, format!("mechanism:{}", init.mechanism()));
                    push(&log, format!("response:{:?}", init.initial_response()));
                    push(&log, format!("hostname:{:?}", init.hostname()));
                    Ok(init.outcome(SaslCode::Ok).await?.open().await?.ack(()))
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["ANONYMOUS"]);
    sasl_send(&io, sasl_init("ANONYMOUS", None, None)).await;
    expect_outcome(&io, SaslCode::Ok).await;

    let items = wait_log(&log, 3).await;
    assert_eq!(items[0], "mechanism:ANONYMOUS");
    assert_eq!(items[1], "response:None");
    assert_eq!(items[2], "hostname:None");
    io.close();
}

#[ntex::test]
async fn test_sasl_outcome_codes() {
    for code in [
        SaslCode::Ok,
        SaslCode::Auth,
        SaslCode::Sys,
        SaslCode::SysPerm,
    ] {
        let srv = start_server(AmqpServiceConfig::new(), move |conn| async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("PLAIN").init().await?;
                    // outcome() always succeeds, the code is only reported to the peer
                    Ok(init.outcome(code).await?.open().await?.ack(()))
                }
            }
        });

        let io = sasl_connect(srv.addr()).await;
        assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
        sasl_send(&io, sasl_init("PLAIN", None, None)).await;
        expect_outcome(&io, code).await;

        // the server keeps waiting for the amqp protocol header in all cases
        let open = amqp_open(&io).await;
        assert!(open.max_frame_size() >= 512);
        io.close();
    }
}

#[ntex::test]
async fn test_sasl_client_auth_failure() {
    let srv = start_server(AmqpServiceConfig::new(), |conn| async move {
        match conn {
            server::Handshake::Amqp(_) => panic!("expected sasl"),
            server::Handshake::Sasl(conn) => {
                let init = conn.mechanism("PLAIN").init().await?;
                Ok(init.outcome(SaslCode::Auth).await?.open().await?.ack(()))
            }
        }
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let err = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri).sasl_auth("".into(), "user".into(), "pass".into()))
        .await
        .err()
        .expect("expected auth error");

    assert!(
        matches!(*err, client::ConnectError::Sasl(SaslCode::Auth)),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "Sasl error code Auth");
}

#[ntex::test]
async fn test_sasl_challenge_response() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("CRAM-MD5").init().await?;
                    let resp = init.challenge_with(Bytes::from_static(b"nonce-1")).await?;
                    assert!(format!("{resp:?}").starts_with("SaslResponse"));
                    push(&log, format!("response:{:?}", resp.response().to_vec()));
                    assert_eq!(resp.st(), &());
                    assert!(!resp.io().is_closed());

                    let res = resp.outcome(SaslCode::Ok).await;
                    push(&log, format!("outcome:{:?}", res.err()));
                    Err(server::HandshakeError::Timeout)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["CRAM-MD5"]);
    sasl_send(&io, sasl_init("CRAM-MD5", None, None)).await;

    match sasl_recv(&io).await {
        SaslFrameBody::SaslChallenge(c) => {
            assert_eq!(c.challenge(), &Bytes::from_static(b"nonce-1"));
        }
        body => panic!("unexpected sasl frame: {body:?}"),
    }

    sasl_send(
        &io,
        SaslFrameBody::SaslResponse(protocol::SaslResponse {
            response: Bytes::from_static(b"digest"),
        }),
    )
    .await;
    expect_outcome(&io, SaslCode::Ok).await;

    let items = wait_log(&log, 2).await;
    assert_eq!(items[0], "response:[100, 105, 103, 101, 115, 116]");
    assert_eq!(items[1], "outcome:None");
    io.close();
}

/// Empty challenge, then the amqp connection is opened right after the sasl outcome.
#[ntex::test]
async fn test_sasl_challenge_response_then_open() {
    let srv = start_server(AmqpServiceConfig::new(), move |conn| async move {
        match conn {
            server::Handshake::Amqp(_) => panic!("expected sasl"),
            server::Handshake::Sasl(conn) => {
                let init = conn.mechanism("CRAM-MD5").init().await?;
                let resp = init.challenge().await?;
                assert!(resp.response().is_empty());
                Ok(resp.outcome(SaslCode::Ok).await?.open().await?.ack(()))
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["CRAM-MD5"]);
    sasl_send(&io, sasl_init("CRAM-MD5", None, None)).await;

    match sasl_recv(&io).await {
        SaslFrameBody::SaslChallenge(c) => assert!(c.challenge().is_empty()),
        body => panic!("unexpected sasl frame: {body:?}"),
    }
    sasl_send(
        &io,
        SaslFrameBody::SaslResponse(protocol::SaslResponse {
            response: Bytes::new(),
        }),
    )
    .await;
    expect_outcome(&io, SaslCode::Ok).await;

    amqp_open(&io).await;
    io.close();
}

// ======================= sasl protocol errors =======================

#[ntex::test]
async fn test_sasl_unexpected_body_frame() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let err = conn.mechanism("PLAIN").init().await.err().unwrap();
                    push(&log, format!("{err:?}"));
                    push(&log, err.to_string());
                    Err(err)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
    // outcome instead of init
    sasl_send(
        &io,
        SaslFrameBody::SaslOutcome(SaslOutcome {
            code: SaslCode::Ok,
            additional_data: None,
        }),
    )
    .await;

    let items = wait_log(&log, 2).await;
    assert!(
        items[0].starts_with("UnexpectedSaslBodyFrame(SaslOutcome"),
        "{:?}",
        items[0]
    );
    assert_eq!(
        items[1],
        "Unexpected sasl frame body: SaslOutcome(SaslOutcome { code: Ok, additional_data: None })"
    );
    expect_eof(&io).await;
}

#[ntex::test]
async fn test_sasl_unexpected_frame_instead_of_response() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("PLAIN").init().await?;
                    let err = init.challenge().await.err().unwrap();
                    push(&log, format!("{err:?}"));
                    Err(err)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
    sasl_send(&io, sasl_init("PLAIN", None, None)).await;
    match sasl_recv(&io).await {
        SaslFrameBody::SaslChallenge(_) => (),
        body => panic!("unexpected sasl frame: {body:?}"),
    }
    // init instead of response
    sasl_send(&io, sasl_init("PLAIN", None, None)).await;

    let items = wait_log(&log, 1).await;
    assert!(
        items[0].starts_with("UnexpectedSaslBodyFrame(SaslInit"),
        "{:?}",
        items[0]
    );
}

#[ntex::test]
async fn test_sasl_disconnect_after_mechanisms() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let err = conn.mechanism("PLAIN").init().await.err().unwrap();
                    push(&log, format!("{err:?}"));
                    push(&log, err.to_string());
                    Err(err)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
    io.close();

    let items = wait_log(&log, 2).await;
    assert_eq!(items[0], "Disconnected(None)");
    assert_eq!(items[1], "Peer disconnected, with error None");
}

#[ntex::test]
async fn test_sasl_wrong_protocol_id_after_outcome() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("PLAIN").init().await?;
                    let success = init.outcome(SaslCode::Ok).await?;
                    assert_eq!(success.st(), &());
                    assert!(!success.io().is_closed());
                    let err = success.open().await.err().unwrap();
                    push(&log, format!("{err:?}"));
                    push(&log, err.to_string());
                    Err(err)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
    sasl_send(&io, sasl_init("PLAIN", None, None)).await;
    expect_outcome(&io, SaslCode::Ok).await;
    io.send(ProtocolId::AmqpTls, &ProtocolIdCodec)
        .await
        .unwrap();

    let items = wait_log(&log, 2).await;
    assert_eq!(
        items[0],
        "ProtocolNegotiation(Unexpected { exp: Amqp, got: AmqpTls })"
    );
    assert_eq!(
        items[1],
        "Protocol negotiation error: Expected Amqp protocol id, seen AmqpTls instead."
    );
}

#[ntex::test]
async fn test_sasl_unexpected_frame_instead_of_open() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("PLAIN").init().await?;
                    let err = init
                        .outcome(SaslCode::Ok)
                        .await?
                        .open()
                        .await
                        .err()
                        .unwrap();
                    push(&log, format!("{err:?}"));
                    Err(err)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
    sasl_send(&io, sasl_init("PLAIN", None, None)).await;
    expect_outcome(&io, SaslCode::Ok).await;
    amqp_hello(&io).await;
    io.send(AmqpFrame::new(0, begin_frame()), &amqp_codec())
        .await
        .unwrap();

    let items = wait_log(&log, 1).await;
    assert!(items[0].starts_with("Unexpected(Begin"), "{:?}", items[0]);
}

#[ntex::test]
async fn test_sasl_invalid_max_frame_size() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("PLAIN").init().await?;
                    let err = init
                        .outcome(SaslCode::Ok)
                        .await?
                        .open()
                        .await
                        .err()
                        .unwrap();
                    push(&log, format!("{err:?}"));
                    push(&log, err.to_string());
                    Err(err)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
    sasl_send(&io, sasl_init("PLAIN", None, None)).await;
    expect_outcome(&io, SaslCode::Ok).await;
    amqp_hello(&io).await;

    let open = protocol::Open::build().max_frame_size(16).finish();
    io.send(AmqpFrame::new(0, open.into()), &amqp_codec())
        .await
        .unwrap();

    let items = wait_log(&log, 2).await;
    assert_eq!(items[0], "InvalidMaxFrameSize(16)");
    assert_eq!(items[1], "Invalid remote max frame size: 16");
}

#[ntex::test]
async fn test_sasl_disconnect_before_open() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Amqp(_) => panic!("expected sasl"),
                server::Handshake::Sasl(conn) => {
                    let init = conn.mechanism("PLAIN").init().await?;
                    let err = init
                        .outcome(SaslCode::Ok)
                        .await?
                        .open()
                        .await
                        .err()
                        .unwrap();
                    push(&log, format!("{err:?}"));
                    Err(err)
                }
            }
        }
    });

    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN"]);
    sasl_send(&io, sasl_init("PLAIN", None, None)).await;
    expect_outcome(&io, SaslCode::Ok).await;
    io.close();

    let items = wait_log(&log, 1).await;
    assert_eq!(items[0], "Disconnected(None)");
}

// ======================= plain handshake =======================

#[ntex::test]
async fn test_handshake_open_accessors() {
    let log = log();
    let log2 = log.clone();

    let cfg = AmqpServiceConfig::new()
        .set_container_id("srv-container")
        .set_hostname("srv-host")
        .set_channel_max(42)
        .set_idle_timeout(30);

    let srv = start_server(cfg, move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Sasl(_) => panic!("expected plain"),
                server::Handshake::Amqp(conn) => {
                    assert_eq!(conn.st(), &());
                    assert!(!conn.io().is_closed());
                    let conn = conn.open().await?;

                    let frame = conn.frame();
                    push(&log, format!("container:{}", frame.container_id()));
                    push(&log, format!("hostname:{:?}", frame.hostname()));
                    push(&log, format!("max_frame_size:{}", frame.max_frame_size()));
                    push(&log, format!("channel_max:{}", frame.channel_max()));
                    push(&log, format!("idle_time_out:{:?}", frame.idle_time_out()));
                    push(
                        &log,
                        format!("remote_max_frame:{}", conn.remote_config().max_frame_size),
                    );
                    push(
                        &log,
                        format!("remote_hostname:{:?}", conn.remote_config().hostname),
                    );
                    push(
                        &log,
                        format!("local_channel_max:{}", conn.local_config().channel_max),
                    );
                    push(&log, format!("sink_opened:{}", conn.sink().is_opened()));
                    assert_eq!(conn.st(), &());
                    assert!(!conn.io().is_closed());

                    Ok(conn.ack(()))
                }
            }
        }
    });

    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    let open = amqp_open(&io).await;

    assert_eq!(open.container_id(), "srv-container");
    assert_eq!(open.hostname().map(ByteString::as_str), Some("srv-host"));
    assert_eq!(open.channel_max(), 42);
    assert_eq!(open.idle_time_out(), Some(30_000));
    assert_eq!(open.max_frame_size(), 16 * 1024);

    let items = wait_log(&log, 9).await;
    assert_eq!(items[0], "container:test-client");
    assert_eq!(items[1], "hostname:Some(\"test-host\")");
    assert_eq!(items[2], "max_frame_size:65535");
    assert_eq!(items[3], "channel_max:17");
    assert_eq!(items[4], "idle_time_out:Some(45000)");
    assert_eq!(items[5], "remote_max_frame:65535");
    assert_eq!(items[6], "remote_hostname:Some(\"test-host\")");
    assert_eq!(items[7], "local_channel_max:42");
    assert_eq!(items[8], "sink_opened:true");
    io.close();
}

#[ntex::test]
async fn test_handshake_rejected_by_service() {
    let srv = start_server(AmqpServiceConfig::new(), |conn| async move {
        match conn {
            server::Handshake::Sasl(_) => panic!("expected plain"),
            server::Handshake::Amqp(conn) => {
                let _conn = conn.open().await?;
                Err(server::HandshakeError::Timeout)
            }
        }
    });

    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    amqp_hello(&io).await;
    io.send(AmqpFrame::new(0, client_open().into()), &amqp_codec())
        .await
        .unwrap();

    // no Open from the server, the connection is dropped
    expect_eof(&io).await;
}

#[ntex::test]
async fn test_handshake_unexpected_first_frame() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Sasl(_) => panic!("expected plain"),
                server::Handshake::Amqp(conn) => {
                    let err = conn.open().await.err().unwrap();
                    push(&log, format!("{err:?}"));
                    push(&log, err.to_string());
                    Err(err)
                }
            }
        }
    });

    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    amqp_hello(&io).await;
    io.send(AmqpFrame::new(0, begin_frame()), &amqp_codec())
        .await
        .unwrap();

    let items = wait_log(&log, 2).await;
    assert!(items[0].starts_with("Unexpected(Begin"), "{:?}", items[0]);
    assert!(
        items[1].starts_with("Unexpected frame, got: Begin(Begin(BeginInner {"),
        "{:?}",
        items[1]
    );
    expect_eof(&io).await;
}

#[ntex::test]
async fn test_handshake_invalid_max_frame_size() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Sasl(_) => panic!("expected plain"),
                server::Handshake::Amqp(conn) => {
                    let err = conn.open().await.err().unwrap();
                    push(&log, format!("{err:?}"));
                    push(&log, err.to_string());
                    Err(err)
                }
            }
        }
    });

    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    amqp_hello(&io).await;
    let open = protocol::Open::build().max_frame_size(511).finish();
    io.send(AmqpFrame::new(0, open.into()), &amqp_codec())
        .await
        .unwrap();

    let items = wait_log(&log, 2).await;
    assert_eq!(items[0], "InvalidMaxFrameSize(511)");
    assert_eq!(items[1], "Invalid remote max frame size: 511");
}

#[ntex::test]
async fn test_handshake_disconnect_before_open() {
    let log = log();
    let log2 = log.clone();

    let srv = start_server(AmqpServiceConfig::new(), move |conn| {
        let log = log2.clone();
        async move {
            match conn {
                server::Handshake::Sasl(_) => panic!("expected plain"),
                server::Handshake::Amqp(conn) => {
                    let err = conn.open().await.err().unwrap();
                    push(&log, format!("{err:?}"));
                    Err(err)
                }
            }
        }
    });

    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    amqp_hello(&io).await;
    io.close();

    let items = wait_log(&log, 1).await;
    assert_eq!(items[0], "Disconnected(None)");
}

#[ntex::test]
async fn test_handshake_wrong_protocol_header() {
    let srv = start_server(AmqpServiceConfig::new(), accept_all);

    // tls is not supported
    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    io.send(ProtocolId::AmqpTls, &ProtocolIdCodec)
        .await
        .unwrap();
    let res = timeout(Millis(3000), io.recv(&ProtocolIdCodec))
        .await
        .unwrap();
    assert!(matches!(res, Ok(None) | Err(_)), "{res:?}");

    // garbage
    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    io.encode_slice(b"HTTP/1.1 GET /\r\n\r\n").unwrap();
    let res = timeout(Millis(3000), io.recv(&ProtocolIdCodec))
        .await
        .unwrap();
    assert!(matches!(res, Ok(None) | Err(_)), "{res:?}");
}

#[ntex::test]
async fn test_handshake_timeout() {
    let srv = start_server(
        AmqpServiceConfig::new().set_handshake_timeout(Seconds(1)),
        accept_all,
    );

    // the server drops the connection without ever receiving a protocol header
    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    let res = timeout(Millis(4000), io.recv(&ProtocolIdCodec))
        .await
        .unwrap();
    assert!(matches!(res, Ok(None) | Err(_)), "{res:?}");

    // same during the sasl exchange
    let io = sasl_connect(srv.addr()).await;
    assert_eq!(recv_mechanisms(&io).await, ["PLAIN", "ANONYMOUS"]);
    let res = timeout(Millis(4000), io.recv(&sasl_codec())).await.unwrap();
    assert!(matches!(res, Ok(None) | Err(_)), "{res:?}");
}

// ======================= server control service =======================

#[ntex::test]
async fn test_server_control_service() {
    let log = log();
    let log2 = log.clone();

    let srv = test_server(async move || {
        let log = log2.clone();
        server::Server::builder(async move |conn: server::Handshake| {
            accept_all(conn).await.map_err(|_| ())
        })
        .control(move |msg: ControlFrame| {
            let log = log.clone();
            async move {
                push(&log, format!("{:?}", msg.kind()));
                Ok::<_, ()>(())
            }
        })
        .build(
            server::Router::<()>::builder()
                .service("test", link_service)
                .build(),
        )
    });

    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    amqp_open(&io).await;

    // remote close
    io.send(
        AmqpFrame::new(0, protocol::Close { error: None }.into()),
        &amqp_codec(),
    )
    .await
    .unwrap();

    let items = wait_log(&log, 1).await;
    assert!(
        items.iter().any(|i| i.starts_with("ProtocolError")),
        "{items:?}"
    );
    io.close();
}

#[ntex::test]
async fn test_server_error_display() {
    use ntex::util::Either;
    use ntex_amqp::codec::{AmqpCodecError, ProtocolIdError};

    let err: server::HandshakeError =
        Either::<AmqpCodecError, std::io::Error>::Left(AmqpCodecError::MaxSizeExceeded).into();
    assert_eq!(format!("{err:?}"), "Codec(MaxSizeExceeded)");
    let err: server::ServerError<LinkError> = err.into();
    assert!(format!("{err:?}").starts_with("Handshake(Codec"), "{err:?}");

    let err: server::HandshakeError =
        Either::<AmqpCodecError, std::io::Error>::Right(std::io::Error::other("boom")).into();
    assert!(
        format!("{err:?}").starts_with("Disconnected(Some"),
        "{err:?}"
    );

    let err: server::HandshakeError =
        Either::<ProtocolIdError, std::io::Error>::Left(ProtocolIdError::InvalidHeader).into();
    assert_eq!(format!("{err:?}"), "ProtocolNegotiation(InvalidHeader)");

    let err: server::HandshakeError =
        Either::<ProtocolIdError, std::io::Error>::Right(std::io::Error::other("boom")).into();
    assert!(
        format!("{err:?}").starts_with("Disconnected(Some"),
        "{err:?}"
    );

    let err: server::ServerError<LinkError> = AmqpCodecError::MaxSizeExceeded.into();
    assert_eq!(err.to_string(), "Amqp codec error: MaxSizeExceeded");

    let err: server::ServerError<LinkError> =
        ntex_amqp::error::AmqpProtocolError::Disconnected.into();
    assert_eq!(err.to_string(), "Amqp protocol error: Disconnected");

    let err: server::ServerError<LinkError> = server::HandshakeError::Timeout.into();
    assert_eq!(err.to_string(), "Handshake error: Handshake timeout");

    let err: server::ServerError<LinkError> =
        server::ServerError::Dispatcher(ntex_amqp::error::AmqpDispatcherError::Service);
    assert_eq!(err.to_string(), "Amqp dispatcher error: Service");

    let err = server::HandshakeError::Sasl(ntex_amqp::codec::protocol::SaslCode::Auth);
    assert_eq!(err.to_string(), "Sasl error code: Auth");

    let err = server::HandshakeError::UnsupportedSaslMechanism("X".to_string());
    assert_eq!(err.to_string(), "Unsupported sasl mechanism: X");

    let err: server::ServerError<LinkError> =
        server::ServerError::Service(LinkError::force_detach());
    assert_eq!(err.to_string(), "Message handler service error");

    let err = server::HandshakeError::ProtocolNegotiation(ProtocolIdError::Incompatible);
    assert_eq!(err.to_string(), "Protocol negotiation error: Incompatible");
}

/// Peer connects and disconnects without sending any protocol header
#[ntex::test]
async fn test_server_disconnect_before_protocol_header() {
    let srv = start_server(AmqpServiceConfig::new(), move |con| async move {
        match con {
            server::Handshake::Amqp(con) => Ok(con.open().await?.ack(())),
            server::Handshake::Sasl(_) => Err(server::HandshakeError::Timeout),
        }
    });

    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    io.close();
    drop(io);

    // server survives the aborted connection
    let io = ntex::connect::connect(srv.addr()).await.unwrap();
    let open = amqp_open(&io).await;
    assert!(!open.container_id().is_empty());
}
