use std::{convert::TryFrom, sync::Arc, sync::Mutex};

use ntex::server::test_server;
use ntex::service::{boxed, boxed::BoxService, fn_factory_with_config, fn_service};
use ntex::util::{Bytes, Either, Ready};
use ntex::{http::Uri, rt, time::sleep, time::Millis};
use ntex_amqp::{
    client, codec::protocol, error::LinkError, server, types, ControlFrame, ControlFrameKind,
};

async fn server(
    link: types::Link<()>,
) -> Result<BoxService<types::Transfer, types::Outcome, LinkError>, LinkError> {
    println!("OPEN LINK: {:?}", link);
    Ok(boxed::service(fn_service(|_req| {
        Ready::Ok(types::Outcome::Accept)
    })))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv = test_server(|| {
        server::Server::build(|con: server::Handshake| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        })
        .finish(
            server::Router::<()>::new()
                .service("test", fn_factory_with_config(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();

    let client = client::Connector::new().connect(uri).await.unwrap();

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
        .delivery(Bytes::from(b"test".as_ref()))
        .send()
        .await
        .unwrap();
    let st = delivery.wait().await.unwrap().unwrap();
    assert_eq!(st, protocol::DeliveryState::Accepted(protocol::Accepted {}));

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

    if init.mechanism() == "PLAIN" {
        if let Some(resp) = init.initial_response() {
            if resp == b"\0user1\0password1" {
                let succ = init
                    .outcome(ntex_amqp_codec::protocol::SaslCode::Ok)
                    .await?;
                return Ok(succ.open().await?.ack(()));
            }
        }
    }

    let succ = init
        .outcome(ntex_amqp_codec::protocol::SaslCode::Auth)
        .await?;
    Ok(succ.open().await?.ack(()))
}

#[ntex::test]
async fn test_sasl() -> std::io::Result<()> {
    let srv = test_server(|| {
        server::Server::build(|conn: server::Handshake| async move {
            match conn {
                server::Handshake::Amqp(conn) => {
                    let conn = conn.open().await.unwrap();
                    Ok(conn.ack(()))
                }
                server::Handshake::Sasl(auth) => sasl_auth(auth).await.map_err(|_| ()),
            }
        })
        .finish(
            server::Router::<()>::new()
                .service("test", fn_factory_with_config(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();

    let _client = client::Connector::new()
        .connect_sasl(
            uri,
            client::SaslAuth {
                authz_id: "".into(),
                authn_id: "user1".into(),
                password: "password1".into(),
            },
        )
        .await;

    Ok(())
}

#[ntex::test]
async fn test_session_end() -> std::io::Result<()> {
    let link_names = Arc::new(Mutex::new(Vec::new()));
    let link_names2 = link_names.clone();

    let srv = test_server(move || {
        let srv = server::Server::build(|con: server::Handshake| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        });

        let link_names = link_names2.clone();
        srv.control(move |frm: ControlFrame| {
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
            Ready::<_, ()>::Ok(())
        })
        .finish(
            server::Router::<()>::new()
                .service("test", fn_factory_with_config(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = client::Connector::new().connect(uri).await.unwrap();

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
        .delivery(Bytes::from(b"test".as_ref()))
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
    let _ = env_logger::try_init();

    let srv = test_server(move || {
        server::Server::build(|con: server::Handshake| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        })
        .control(move |frm: ControlFrame| {
            if let ControlFrameKind::AttachSender(_, ref link) = frm.kind() {
                let link = link.clone();
                rt::spawn(async move {
                    sleep(Millis(150)).await;
                    let _ = link.close().await;
                });
            }
            Ready::<_, ()>::Ok(())
        })
        .finish(
            server::Router::<()>::new()
                .service(
                    "test",
                    fn_factory_with_config(|link: types::Link<()>| async move {
                        rt::spawn(async move {
                            sleep(Millis(150)).await;
                            let _ = link.receiver().close().await;
                        });

                        Ok::<_, LinkError>(boxed::service(fn_service(|_req| async move {
                            Ok::<_, LinkError>(types::Outcome::Accept)
                        })))
                    }),
                )
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = client::Connector::new().connect(uri).await.unwrap();

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
    let _ = env_logger::try_init();

    let srv = test_server(move || {
        server::Server::build(|con: server::Handshake| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        })
        .finish(
            server::Router::<()>::new()
                .service(
                    "test",
                    fn_factory_with_config(|link: types::Link<()>| async move {
                        rt::spawn(async move {
                            sleep(Millis(150)).await;
                            let _ = link.session().end().await;
                        });

                        Ok::<_, LinkError>(boxed::service(fn_service(|_req| async move {
                            Ok::<_, LinkError>(types::Outcome::Accept)
                        })))
                    }),
                )
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = client::Connector::new().connect(uri).await.unwrap();

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
    let _ = env_logger::try_init();

    let srv = test_server(move || {
        server::Server::build(|con: server::Handshake| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        })
        .finish(
            server::Router::<()>::new()
                .service(
                    "test",
                    fn_factory_with_config(|link: types::Link<()>| async move {
                        rt::spawn(async move {
                            sleep(Millis(150)).await;
                            let _ = link.session().connection().close().await;
                        });

                        Ok::<_, LinkError>(boxed::service(fn_service(|_req| async move {
                            Ok::<_, LinkError>(types::Outcome::Accept)
                        })))
                    }),
                )
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let client = client::Connector::new().connect(uri).await.unwrap();

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
