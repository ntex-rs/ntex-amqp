use std::sync::{Arc, Mutex, atomic::AtomicUsize, atomic::Ordering};
use std::{cell::Cell, rc::Rc};

use ntex::server::{TestServerBuilder, test_server};
use ntex::service::{Ctx, Pipeline, Service, boxed, boxed::BoxService, fn_factory, fn_service};
use ntex::util::{Bytes, Either};
use ntex::{SharedCfg, rt, time::Millis, time::sleep, time::timeout, url::Url};
use ntex_amqp::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, protocol::ProtocolId};
use ntex_amqp::{
    AmqpServiceConfig, ControlFrame, ControlFrameKind, client, codec::protocol, error::LinkError,
    server, types,
};
use rand::{Rng, distr::Alphanumeric};

async fn server(
    _link: &types::Link<()>,
) -> Result<BoxService<types::Link<()>, types::Transfer, types::Outcome, LinkError>, LinkError> {
    Ok(boxed::service(fn_service(async |_req| {
        Ok(types::Outcome::Accept)
    })))
}

async fn server_count(
    count: Arc<AtomicUsize>,
) -> Result<BoxService<types::Link<()>, types::Transfer, types::Outcome, LinkError>, LinkError> {
    Ok(boxed::service(fn_service(async move |_req| {
        let val = count.load(Ordering::Relaxed);
        count.store(val + 1, Ordering::Release);
        Ok(types::Outcome::Accept)
    })))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let count = Arc::new(AtomicUsize::new(0));

    let count2 = count.clone();
    let srv = test_server(async move || {
        let count = count2.clone();
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
                .service("test", async move |_: &types::Link<()>| {
                    server_count(count.clone()).await
                })
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();

    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();

    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();
    let delivery = link
        .transfer(Bytes::from(b"test".as_ref()))
        .send()
        .await
        .unwrap();
    let st = delivery.wait().await.unwrap().unwrap();
    assert_eq!(st, protocol::DeliveryState::Accepted(protocol::Accepted {}));

    let delivery = link
        .transfer(Bytes::from(b"test".as_ref()))
        .settled()
        .send()
        .await
        .unwrap();
    let st = delivery.wait().await.unwrap();
    assert_eq!(st, None);
    sleep(Millis(250)).await;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    Ok(())
}

#[ntex::test]
async fn test_large_transfer() -> std::io::Result<()> {
    let mut rng = rand::rng();
    let data: String = (0..2048)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();

    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();
    let srv = TestServerBuilder::new(async move || {
        let count = count2.clone();
        server::Server::builder(|con: server::Handshake| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        })
        .control(|msg: ControlFrame| async move {
            if let ControlFrameKind::AttachReceiver(_, _, rcv) = msg.kind() {
                rcv.set_max_message_size(10 * 1024);
            }
            Ok::<_, ()>(())
        })
        .build(
            server::Router::<()>::builder()
                .service("test", async move |_: &types::Link<()>| {
                    server_count(count.clone()).await
                })
                .build(),
        )
    })
    .config(SharedCfg::new("AMQP").add(AmqpServiceConfig::new().set_max_frame_size(1024)))
    .start();

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();

    let delivery = link
        .transfer(Bytes::from(data.clone()))
        .send()
        .await
        .unwrap();
    let st = delivery.wait().await.unwrap().unwrap();
    assert_eq!(st, protocol::DeliveryState::Accepted(protocol::Accepted {}));
    sleep(Millis(250)).await;

    assert_eq!(count.load(Ordering::Relaxed), 1);
    Ok(())
}

async fn sasl_auth(auth: server::Sasl) -> Result<server::HandshakeAck<()>, server::HandshakeError> {
    let init = auth
        .mechanism("PLAIN")
        .mechanism("ANONYMOUS")
        .mechanism("MSSBCBS")
        .mechanism("AMQPCBS")
        .init()
        .await?;

    if init.mechanism() == "PLAIN"
        && let Some(resp) = init.initial_response()
        && resp == b"\0user1\0password1"
    {
        let succ = init
            .outcome(ntex_amqp_codec::protocol::SaslCode::Ok)
            .await?;
        return Ok(succ.open().await?.ack(()));
    }

    let succ = init
        .outcome(ntex_amqp_codec::protocol::SaslCode::Auth)
        .await?;
    Ok(succ.open().await?.ack(()))
}

#[ntex::test]
async fn test_sasl() -> std::io::Result<()> {
    let srv = test_server(async || {
        server::Server::builder(async move |conn: server::Handshake| match conn {
            server::Handshake::Amqp(conn) => {
                let conn = conn.open().await.unwrap();
                Ok(conn.ack(()))
            }
            server::Handshake::Sasl(auth) => sasl_auth(auth).await.map_err(|_| ()),
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();

    let _client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri).sasl_auth("".into(), "user1".into(), "password1".into()))
        .await;

    Ok(())
}

#[ntex::test]
async fn test_handshake_max_frame_size() -> std::io::Result<()> {
    let srv = TestServerBuilder::new(async || {
        server::Server::builder(async move |conn: server::Handshake| match conn {
            server::Handshake::Amqp(conn) => {
                let conn = conn.open().await.map_err(|_| ())?;
                Ok::<_, ()>(conn.ack(()))
            }
            server::Handshake::Sasl(auth) => {
                let init = auth.mechanism("PLAIN").init().await.map_err(|_| ())?;
                let succ = init.outcome(protocol::SaslCode::Ok).await.map_err(|_| ())?;
                Ok(succ.open().await.map_err(|_| ())?.ack(()))
            }
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    })
    .config(SharedCfg::new("AMQP").add(AmqpServiceConfig::new().set_max_frame_size(512)))
    .start();

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let large = "x".repeat(1024);
    let large_cfg = SharedCfg::new("CLIENT").add(AmqpServiceConfig::new().set_container_id(&large));

    // open frame
    let res = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri.clone()))
        .await;
    assert!(res.is_ok());
    let res = Pipeline::new(large_cfg.build(), client::Connector::new())
        .call(client::Connect::new(uri.clone()))
        .await;
    assert!(res.is_err());

    // sasl init frame
    let res = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri.clone()).sasl_auth(
            "".into(),
            "user1".into(),
            "password1".into(),
        ))
        .await;
    assert!(res.is_ok());
    let res = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri).sasl_auth("".into(), "user1".into(), large.as_str().into()))
        .await;
    assert!(res.is_err());

    Ok(())
}

fn max_frame_size_server(
    cfg: AmqpServiceConfig,
    errors: Arc<Mutex<Vec<bool>>>,
) -> ntex::server::TestServer {
    TestServerBuilder::new(async move || {
        let errors = errors.clone();
        server::Server::builder(async move |conn: server::Handshake| {
            let res = match conn {
                server::Handshake::Amqp(conn) => conn.open().await,
                server::Handshake::Sasl(auth) => {
                    let init = auth.mechanism("PLAIN").init().await.map_err(|_| ())?;
                    let succ = init.outcome(protocol::SaslCode::Ok).await.map_err(|_| ())?;
                    succ.open().await
                }
            };
            match res {
                Ok(conn) => Ok(conn.ack(())),
                Err(err) => {
                    errors.lock().unwrap().push(matches!(
                        err,
                        server::HandshakeError::InvalidMaxFrameSize(511)
                    ));
                    Err(())
                }
            }
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    })
    .config(SharedCfg::new("AMQP").add(cfg))
    .start()
}

#[ntex::test]
async fn test_remote_max_frame_size() -> std::io::Result<()> {
    for size in [0, 511] {
        let res = std::panic::catch_unwind(|| AmqpServiceConfig::new().set_max_frame_size(size));
        assert!(res.is_err(), "{size}");
    }

    let connect = async |srv: &ntex::server::TestServer, cfg: AmqpServiceConfig, sasl: bool| {
        let uri =
            Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
        let mut req = client::Connect::new(uri);
        if sasl {
            req = req.sasl_auth("".into(), "user1".into(), "password1".into());
        }
        Pipeline::new(
            SharedCfg::new("CLIENT").add(cfg).build(),
            client::Connector::new(),
        )
        .call(req)
        .await
    };
    // server rejects remote open
    let errors = Arc::new(Mutex::new(Vec::new()));
    let srv = max_frame_size_server(AmqpServiceConfig::new(), errors.clone());
    for sasl in [false, true] {
        let mut cfg = AmqpServiceConfig::new();
        cfg.max_frame_size = 511;
        assert!(connect(&srv, cfg, sasl).await.is_err());
        let cfg = AmqpServiceConfig::new().set_max_frame_size(512);
        assert!(connect(&srv, cfg, sasl).await.is_ok());
    }
    sleep(Millis(50)).await;
    assert_eq!(*errors.lock().unwrap(), [true, true]);

    // client rejects remote open
    let mut small = AmqpServiceConfig::new();
    small.max_frame_size = 511;
    let srv = max_frame_size_server(small, errors.clone());
    let err = connect(&srv, AmqpServiceConfig::new(), false)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(*err, client::ConnectError::InvalidMaxFrameSize(511)),
        "{err:?}"
    );

    // local `0` is advertised as unlimited
    let mut unlimited = AmqpServiceConfig::new();
    unlimited.max_frame_size = 0;
    let srv = max_frame_size_server(unlimited, errors);
    assert!(connect(&srv, AmqpServiceConfig::new(), false).await.is_ok());

    Ok(())
}

async fn raw_connect(addr: std::net::SocketAddr) -> ntex::io::Io {
    let io = ntex::connect::connect(addr).await.unwrap();
    io.send(ProtocolId::Amqp, &ProtocolIdCodec).await.unwrap();
    assert_eq!(
        io.recv(&ProtocolIdCodec).await.unwrap(),
        Some(ProtocolId::Amqp)
    );
    let open = AmqpServiceConfig::new().to_open();
    io.send(AmqpFrame::new(0, open.into()), &AmqpCodec::new())
        .await
        .unwrap();
    let frame = io.recv(&AmqpCodec::<AmqpFrame>::new()).await.unwrap();
    assert!(matches!(
        frame.unwrap().performative(),
        protocol::Frame::Open(_)
    ));
    io
}

/// Send begin frames, return number of begin responses until connection is closed
async fn raw_begin(io: &ntex::io::Io, channels: &[u16]) -> usize {
    let codec = AmqpCodec::<AmqpFrame>::new();
    for ch in channels {
        let begin = protocol::Begin(Box::new(protocol::BeginInner {
            remote_channel: None,
            next_outgoing_id: 1,
            incoming_window: 100,
            outgoing_window: 100,
            handle_max: 10,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }));
        io.send(AmqpFrame::new(*ch, begin.into()), &codec)
            .await
            .unwrap();
    }

    let mut count = 0;
    ntex::time::timeout(Millis(2000), async {
        while let Ok(Some(frame)) = io.recv(&codec).await {
            match frame.performative() {
                protocol::Frame::Begin(_) => count += 1,
                protocol::Frame::Close(_) => break,
                _ => (),
            }
        }
    })
    .await
    .unwrap();
    count
}

#[ntex::test]
async fn test_remote_begin_channels() -> std::io::Result<()> {
    let srv = TestServerBuilder::new(async || {
        server::Server::builder(async move |conn: server::Handshake| match conn {
            server::Handshake::Amqp(conn) => {
                let conn = conn.open().await.map_err(|_| ())?;
                Ok::<_, ()>(conn.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    })
    .config(SharedCfg::new("AMQP").add(AmqpServiceConfig::new().set_channel_max(2)))
    .start();

    // channel in use
    let io = raw_connect(srv.addr()).await;
    assert_eq!(raw_begin(&io, &[0, 0]).await, 1);

    // channel number above channel-max
    let io = raw_connect(srv.addr()).await;
    assert_eq!(raw_begin(&io, &[3]).await, 0);

    // all channels up to channel-max
    let io = raw_connect(srv.addr()).await;
    assert_eq!(raw_begin(&io, &[0, 1, 2, 1]).await, 3);

    // local sessions up to channel-max
    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let cfg = SharedCfg::new("CLIENT").add(AmqpServiceConfig::new().set_channel_max(1));
    let client = Pipeline::new(cfg.build(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });
    let _s0 = sink.open_session().await.unwrap();
    let _s1 = sink.open_session().await.unwrap();
    assert!(matches!(
        sink.open_session().await,
        Err(ntex_amqp::error::AmqpProtocolError::TooManyChannels)
    ));

    Ok(())
}

async fn raw_send(io: &ntex::io::Io, channel: u16, frame: protocol::Frame) {
    io.send(AmqpFrame::new(channel, frame), &AmqpCodec::new())
        .await
        .unwrap();
}

async fn raw_recv(io: &ntex::io::Io) -> protocol::Frame {
    ntex::time::timeout(Millis(2000), io.recv(&AmqpCodec::<AmqpFrame>::new()))
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_parts()
        .1
}

fn raw_attach(handle: u32, role: protocol::Role) -> protocol::Frame {
    let target = protocol::Target {
        address: Some("test".into()),
        durable: protocol::TerminusDurability::None,
        expiry_policy: protocol::TerminusExpiryPolicy::SessionEnd,
        timeout: 0,
        dynamic: false,
        dynamic_node_properties: None,
        capabilities: None,
    };
    protocol::Attach(Box::new(protocol::AttachInner {
        name: format!("link-{handle}").into(),
        handle,
        role,
        snd_settle_mode: protocol::SenderSettleMode::Mixed,
        rcv_settle_mode: protocol::ReceiverSettleMode::First,
        source: None,
        target: Some(target),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }))
    .into()
}

/// Begin session on channel 0, returns advertised handle-max
async fn raw_begin_session(io: &ntex::io::Io) -> u32 {
    let begin = protocol::Begin(Box::new(protocol::BeginInner {
        remote_channel: None,
        next_outgoing_id: 1,
        incoming_window: 100,
        outgoing_window: 100,
        handle_max: 10,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }));
    raw_send(io, 0, begin.into()).await;
    match raw_recv(io).await {
        protocol::Frame::Begin(begin) => begin.handle_max(),
        frm => panic!("Unexpected frame: {frm:?}"),
    }
}

/// Wait for End frame, returns error condition
async fn raw_end(io: &ntex::io::Io) -> protocol::ErrorCondition {
    loop {
        if let protocol::Frame::End(end) = raw_recv(io).await {
            raw_send(io, 0, protocol::End { error: None }.into()).await;
            return end.error.unwrap().0.condition.clone();
        }
    }
}

#[ntex::test]
async fn test_remote_attach_handles() -> std::io::Result<()> {
    let srv = TestServerBuilder::new(async || {
        server::Server::builder(async move |conn: server::Handshake| match conn {
            server::Handshake::Amqp(conn) => {
                let conn = conn.open().await.map_err(|_| ())?;
                Ok::<_, ()>(conn.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .control(async |msg: ControlFrame| {
            // keep remote sender link unconfirmed
            if let ControlFrameKind::AttachSender(..) = msg.kind() {
                sleep(Millis(50)).await;
            }
            Ok::<_, ()>(())
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    })
    .config(SharedCfg::new("AMQP").add(AmqpServiceConfig::new().set_handle_max(2)))
    .start();
    let handle_in_use = protocol::ErrorCondition::SessionError(protocol::SessionError::HandleInUse);

    let io = raw_connect(srv.addr()).await;
    assert_eq!(raw_begin_session(&io).await, 2);

    // handle in use
    raw_send(&io, 0, raw_attach(0, protocol::Role::Sender)).await;
    assert!(matches!(raw_recv(&io).await, protocol::Frame::Attach(_)));
    raw_send(&io, 0, raw_attach(0, protocol::Role::Sender)).await;
    assert_eq!(raw_end(&io).await, handle_in_use);

    // handle above handle-max, connection is still alive
    assert_eq!(raw_begin_session(&io).await, 2);
    raw_send(&io, 0, raw_attach(2, protocol::Role::Sender)).await;
    assert!(matches!(raw_recv(&io).await, protocol::Frame::Attach(_)));
    raw_send(&io, 0, raw_attach(3, protocol::Role::Sender)).await;
    assert_eq!(
        raw_end(&io).await,
        protocol::ErrorCondition::AmqpError(protocol::AmqpError::ResourceLimitExceeded)
    );

    // handle in use, remote sender link is not confirmed yet
    assert_eq!(raw_begin_session(&io).await, 2);
    raw_send(&io, 0, raw_attach(0, protocol::Role::Receiver)).await;
    raw_send(&io, 0, raw_attach(0, protocol::Role::Receiver)).await;
    assert_eq!(raw_end(&io).await, handle_in_use);
    sleep(Millis(100)).await;
    assert_eq!(raw_begin_session(&io).await, 2);

    Ok(())
}

#[ntex::test]
async fn test_remote_receiver_detach_before_confirm() -> std::io::Result<()> {
    let released = Arc::new(AtomicUsize::new(0));
    let detached = Arc::new(AtomicUsize::new(0));
    let released2 = released.clone();
    let detached2 = detached.clone();

    let srv = test_server(async move || {
        let released = released2.clone();
        let detached = detached2.clone();
        server::Server::builder(async move |conn: server::Handshake| match conn {
            server::Handshake::Amqp(conn) => {
                let conn = conn.open().await.map_err(|_| ())?;
                Ok::<_, ()>(conn.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .control(async move |msg: ControlFrame| {
            if let ControlFrameKind::RemoteDetachReceiver(..) = msg.kind() {
                detached.fetch_add(1, Ordering::SeqCst);
            }
            Ok::<_, ()>(())
        })
        .build(
            server::Router::<()>::builder()
                .service("test", move |_: &types::Link<()>| {
                    let released = released.clone();
                    async move {
                        // keep link unconfirmed
                        sleep(Millis(50)).await;
                        let guard = CountGuard(released);
                        Ok::<_, LinkError>(boxed::service(fn_service(move |_req| {
                            let _ = &guard;
                            async { Ok::<_, LinkError>(types::Outcome::Accept) }
                        })))
                    }
                })
                .build(),
        )
    });

    let io = raw_connect(srv.addr()).await;
    raw_begin_session(&io).await;

    // remote detach before confirmation
    raw_send(&io, 0, raw_attach(0, protocol::Role::Sender)).await;
    raw_send(&io, 0, raw_detach(0)).await;
    assert!(matches!(raw_recv(&io).await, protocol::Frame::Attach(_)));
    let protocol::Frame::Detach(detach) = raw_recv(&io).await else {
        panic!()
    };
    assert!(detach.closed());
    sleep(Millis(100)).await;
    assert_eq!(released.load(Ordering::SeqCst), 1);
    assert_eq!(detached.load(Ordering::SeqCst), 1);

    // remote handle is released, link credit is sent for confirmed link only
    raw_send(&io, 0, raw_attach(0, protocol::Role::Sender)).await;
    assert!(matches!(raw_recv(&io).await, protocol::Frame::Attach(_)));
    let protocol::Frame::Flow(flow) = raw_recv(&io).await else {
        panic!()
    };
    assert_eq!(flow.link_credit(), Some(50));

    Ok(())
}

struct CountGuard(Arc<AtomicUsize>);

impl Drop for CountGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn raw_detach(handle: u32) -> protocol::Frame {
    protocol::Detach(Box::new(protocol::DetachInner {
        handle,
        closed: true,
        error: None,
    }))
    .into()
}

#[ntex::test]
async fn test_remote_disposition_range() -> std::io::Result<()> {
    let srv = TestServerBuilder::new(async || {
        server::Server::builder(async move |conn: server::Handshake| match conn {
            server::Handshake::Amqp(conn) => {
                let conn = conn.open().await.map_err(|_| ())?;
                Ok::<_, ()>(conn.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    })
    .start();

    let io = raw_connect(srv.addr()).await;
    raw_begin_session(&io).await;

    // disposition for every delivery id must not stall the connection
    for role in [protocol::Role::Sender, protocol::Role::Receiver] {
        let disp = protocol::Disposition(Box::new(protocol::DispositionInner {
            role,
            first: 0,
            last: Some(u32::MAX),
            settled: true,
            state: None,
            batchable: false,
        }));
        raw_send(&io, 0, disp.into()).await;
    }
    raw_send(&io, 0, protocol::End { error: None }.into()).await;
    assert!(matches!(raw_recv(&io).await, protocol::Frame::End(_)));

    Ok(())
}

#[ntex::test]
async fn test_session_end() -> std::io::Result<()> {
    let link_names = Arc::new(Mutex::new(Vec::new()));
    let link_names2 = link_names.clone();

    let srv = test_server(async move || {
        let srv = server::Server::builder(async move |con: server::Handshake| match con {
            server::Handshake::Amqp(con) => {
                let con = con.open().await.unwrap();
                Ok(con.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        });

        let link_names = link_names2.clone();
        srv.control(async move |frm: ControlFrame| {
            if let ControlFrameKind::RemoteSessionEnded(links) = frm.kind() {
                let mut names = link_names.lock().unwrap();
                for lnk in links {
                    match lnk {
                        Either::Left(lnk) => {
                            names.push(lnk.name().clone());
                        }
                        Either::Right(lnk) => {
                            names.push(lnk.name().clone());
                        }
                    }
                }
            }
            Ok::<_, ()>(())
        })
        .build(server::Router::builder().service("test", server).build())
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();
    let _delivery = link
        .transfer(Bytes::from(b"test".as_ref()))
        .send()
        .await
        .unwrap();
    session.end().await.unwrap();
    sleep(Millis(150)).await;

    assert_eq!(link_names.lock().unwrap()[0], "test");
    assert!(sink.is_opened());

    Ok(())
}

#[ntex::test]
async fn test_link_detach() -> std::io::Result<()> {
    let srv = test_server(async move || {
        server::Server::builder(async move |con: server::Handshake| match con {
            server::Handshake::Amqp(con) => {
                let con = con.open().await.unwrap();
                Ok(con.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .control(async move |frm: ControlFrame| {
            if let ControlFrameKind::AttachSender(_, _, link) = frm.kind() {
                let link = link.clone();
                rt::spawn(async move {
                    sleep(Millis(150)).await;
                    let _ = link.close().await;
                });
            }
            Ok::<_, ()>(())
        })
        .build(
            server::Router::<()>::builder()
                .service("test", async move |link: &types::Link<()>| {
                    let link = link.clone();

                    rt::spawn(async move {
                        sleep(Millis(150)).await;
                        let _ = link.receiver().close().await;
                    });

                    Ok::<_, LinkError>(boxed::service(fn_service(async move |_| {
                        Ok::<_, LinkError>(types::Outcome::Accept)
                    })))
                })
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();

    link.on_close().await;
    assert!(link.is_closed());
    assert!(!link.is_opened());

    let link = session
        .build_receiver_link("test", "test")
        .attach()
        .await
        .unwrap();
    sleep(Millis(350)).await;
    assert!(link.is_closed());

    Ok(())
}

#[ntex::test]
async fn test_link_detach_on_session_end() -> std::io::Result<()> {
    let srv = test_server(async move || {
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
                .service("test", async move |link: &types::Link<()>| {
                    let link = link.clone();
                    rt::spawn(async move {
                        sleep(Millis(150)).await;
                        let _ = link.session().end().await;
                    });

                    Ok::<_, LinkError>(boxed::service(fn_service(async move |_| {
                        Ok::<_, LinkError>(types::Outcome::Accept)
                    })))
                })
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();

    link.on_close().await;
    assert!(link.is_closed());
    assert!(!link.is_opened());

    Ok(())
}

#[ntex::test]
async fn test_link_detach_on_disconnect() -> std::io::Result<()> {
    let srv = test_server(async move || {
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
                .service("test", async move |link: &types::Link<()>| {
                    let link = link.clone();
                    rt::spawn(async move {
                        sleep(Millis(150)).await;
                        let _ = link.session().connection().close().await;
                    });

                    Ok::<_, LinkError>(boxed::service(fn_service(async move |_| {
                        Ok::<_, LinkError>(types::Outcome::Accept)
                    })))
                })
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();

    link.on_close().await;
    assert!(link.is_closed());
    assert!(!link.is_opened());

    Ok(())
}

#[ntex::test]
async fn test_drop_delivery_on_link_detach() -> std::io::Result<()> {
    let srv = test_server(async move || {
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
                .service("test", async move |link: &types::Link<()>| {
                    let link = link.clone();

                    rt::spawn(async move {
                        sleep(Millis(150)).await;
                        let _ = link.receiver().close().await;
                    });

                    Ok::<_, LinkError>(boxed::service(fn_service(async move |_| {
                        sleep(Millis(1500000)).await;
                        Ok::<_, LinkError>(types::Outcome::Accept)
                    })))
                })
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();

    let delivery = link
        .transfer(Bytes::from(b"test".as_ref()))
        .format(1)
        .send()
        .await
        .unwrap();

    let res = delivery.wait().await;
    assert!(res.is_err());

    let res = delivery.wait().await;
    assert!(res.is_err());

    assert!(link.is_closed());
    Ok(())
}

struct GatedPublish(Rc<Cell<bool>>);

impl Service<server::State<()>, types::Message> for GatedPublish {
    type Res = ();
    type Error = LinkError;

    async fn ready(&self, _: Ctx<'_, Self, server::State<()>>) -> Result<(), LinkError> {
        while self.0.get() {
            sleep(Millis(10)).await;
        }
        Ok(())
    }

    async fn call(
        &self,
        _: types::Message,
        _: Ctx<'_, Self, server::State<()>>,
    ) -> Result<(), LinkError> {
        Ok(())
    }
}

#[ntex::test]
async fn test_control_frames_while_publish_not_ready() -> std::io::Result<()> {
    let srv = test_server(async move || {
        // publish service is not ready while the control service is processing a frame
        let blocked = Rc::new(Cell::new(false));
        let blocked2 = blocked.clone();

        server::Server::builder(async move |con: server::Handshake| match con {
            server::Handshake::Amqp(con) => {
                let con = con.open().await.unwrap();
                Ok(con.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .control(async move |frm: ControlFrame| {
            if let ControlFrameKind::AttachSender(..) = frm.kind() {
                blocked.set(true);
                sleep(Millis(50)).await;
                blocked.set(false);
            }
            Ok::<_, ()>(())
        })
        .build(fn_factory(async move |_: &server::State<()>| {
            Ok::<_, std::convert::Infallible>(GatedPublish(blocked2.clone()))
        }))
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let res = timeout(
        Millis(2000),
        session.build_receiver_link("test", "test").attach(),
    )
    .await;
    assert!(matches!(res, Ok(Ok(_))), "{res:?}");

    Ok(())
}

#[ntex::test]
async fn test_control_flow_order() -> std::io::Result<()> {
    let credits = Arc::new(Mutex::new(Vec::new()));
    let credit = Arc::new(Mutex::new(None));
    let credits2 = credits.clone();
    let credit2 = credit.clone();

    let srv = test_server(async move || {
        let credits = credits2.clone();
        let credit = credit2.clone();
        server::Server::builder(async move |con: server::Handshake| match con {
            server::Handshake::Amqp(con) => {
                let con = con.open().await.unwrap();
                Ok(con.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .control(async move |frm: ControlFrame| {
            if let ControlFrameKind::Flow(flow, link) = frm.kind() {
                credits.lock().unwrap().push(flow.link_credit().unwrap());
                if flow.link_credit() == Some(5) {
                    // first flow completes after the second one
                    sleep(Millis(100)).await;
                } else {
                    let link = link.clone();
                    let credit = credit.clone();
                    rt::spawn(async move {
                        sleep(Millis(300)).await;
                        *credit.lock().unwrap() = Some(link.credit());
                    });
                }
            }
            Ok::<_, ()>(())
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_receiver_link("test", "test")
        .attach()
        .await
        .unwrap();
    link.set_link_credit(5);
    link.set_link_credit(10);

    sleep(Millis(500)).await;
    assert_eq!(*credits.lock().unwrap(), vec![5, 15]);
    assert_eq!(*credit.lock().unwrap(), Some(15));
    Ok(())
}

struct DropGuard(Arc<std::sync::atomic::AtomicBool>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[ntex::test]
async fn test_control_call_cancelled_on_disconnect() -> std::io::Result<()> {
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started2 = started.clone();
    let dropped2 = dropped.clone();

    let srv = test_server(async move || {
        let started = started2.clone();
        let dropped = dropped2.clone();
        server::Server::builder(async move |con: server::Handshake| match con {
            server::Handshake::Amqp(con) => {
                let con = con.open().await.unwrap();
                Ok(con.ack(()))
            }
            server::Handshake::Sasl(_) => Err(()),
        })
        .control(async move |frm: ControlFrame| {
            if let ControlFrameKind::Flow(..) = frm.kind() {
                let _guard = DropGuard(dropped.clone());
                started.store(true, Ordering::SeqCst);
                sleep(Millis(10_000)).await;
            }
            Ok::<_, ()>(())
        })
        .build(
            server::Router::<()>::builder()
                .service("test", server)
                .build(),
        )
    });

    let uri = Url::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new(uri))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        let _ = client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_receiver_link("test", "test")
        .attach()
        .await
        .unwrap();
    link.set_link_credit(5);

    sleep(Millis(200)).await;
    assert!(started.load(Ordering::SeqCst));
    assert!(!dropped.load(Ordering::SeqCst));

    sink.close().await.unwrap();
    sleep(Millis(300)).await;
    assert!(dropped.load(Ordering::SeqCst));
    Ok(())
}
