use std::convert::TryFrom;

use futures::future::Ready;
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::http::Uri;
use ntex::server::test_server;
use ntex::service::{fn_factory_with_config, Service};
use ntex_amqp::{client, error::LinkError, server, types};

async fn server(
    link: types::Link<()>,
) -> Result<
    Box<
        dyn Service<
                Request = types::Transfer<()>,
                Response = types::Outcome,
                Error = LinkError,
                Future = Ready<Result<types::Outcome, LinkError>>,
            > + 'static,
    >,
    LinkError,
> {
    println!("OPEN LINK: {:?}", link);
    Err(LinkError::force_detach().description("unimplemented"))
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

    let client = client::Connector::new()
        .connect_sasl(
            uri,
            client::SaslAuth {
                authz_id: "".to_string(),
                authn_id: "user1".to_string(),
                password: "password1".to_string(),
            },
        )
        .await;
    println!("E: {:?}", client.err());

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
                authz_id: "".to_string(),
                authn_id: "user1".to_string(),
                password: "password1".to_string(),
            },
        )
        .await;
    println!("E: {:?}", client.err());

    Ok(())
}
