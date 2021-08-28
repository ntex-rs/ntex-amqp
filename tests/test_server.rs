use std::{cell::Cell, convert::TryFrom, rc::Rc};

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::server::test_server;
use ntex::service::{fn_factory_with_config, fn_service, Service};
use ntex::{http::Uri, time::sleep, util::Bytes, util::Ready};
use ntex_amqp::{client, error::LinkError, server, types};

async fn server(
    link: types::Link<()>,
) -> Result<
    Box<
        dyn Service<
                Request = types::Transfer,
                Response = types::Outcome,
                Error = LinkError,
                Future = Ready<types::Outcome, LinkError>,
            > + 'static,
    >,
    LinkError,
> {
    println!("OPEN LINK: {:?}", link);
    Ok(Box::new(fn_service(|_req| {
        Ready::Ok(types::Outcome::Accept)
    })))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_amqp=trace");
    env_logger::init();

    let srv = test_server(|| {
        let srv = server::Server::new(|con: server::Handshake<_>| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(()),
            }
        });

        srv.finish(
            server::Router::<()>::new()
                .service("test", fn_factory_with_config(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();

    let client = client::Connector::new().connect(uri).await.unwrap();

    let sink = client.sink();
    ntex::rt::spawn(async move {
        client.start_default().await;
    });

    let session = sink.open_session().await.unwrap();

    let link = session
        .build_sender_link("test", "test")
        .attach()
        .await
        .unwrap();
    link.send(Bytes::from(b"test".as_ref())).await.unwrap();

    let res = Rc::new(Cell::new(false));
    let res2 = res.clone();

    link.on_disposition(move |_tag, result| {
        if result.is_ok() {
            res2.set(true);
        }
    });
    link.send_no_block(Bytes::from(b"test".as_ref())).unwrap();
    sleep(500).await;
    assert!(res.get());

    Ok(())
}

async fn sasl_auth<Io: AsyncRead + AsyncWrite + Unpin>(
    auth: server::Sasl<Io>,
) -> Result<server::HandshakeAck<Io, ()>, server::HandshakeError> {
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
        server::Server::new(|conn: server::Handshake<_>| async move {
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

    let client = client::Connector::new()
        .connect_sasl(
            uri,
            client::SaslAuth {
                authz_id: "".into(),
                authn_id: "user1".into(),
                password: "password1".into(),
            },
        )
        .await;
    println!("E: {:?}", client.err());

    Ok(())
}
