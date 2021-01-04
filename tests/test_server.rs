use std::convert::TryFrom;

use futures::future::Ready;
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::connect::Connector;
use ntex::http::Uri;
use ntex::server::test_server;
use ntex::service::{fn_factory_with_config, Service};
use ntex_amqp::server::{self, AmqpError, LinkError};
use ntex_amqp::{sasl, Configuration};

async fn server(
    link: server::Link<()>,
) -> Result<
    Box<
        dyn Service<
                Request = server::Transfer<()>,
                Response = server::Outcome,
                Error = AmqpError,
                Future = Ready<Result<server::Outcome, AmqpError>>,
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
        server::Server::new(|con: server::Handshake<_>| async move {
            match con {
                server::Handshake::Amqp(con) => {
                    let con = con.open().await.unwrap();
                    Ok(con.ack(()))
                }
                server::Handshake::Sasl(_) => Err(AmqpError::not_implemented()),
            }
        })
        .finish(
            server::App::<()>::new()
                .service("test", fn_factory_with_config(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let sasl_srv = sasl::connect_service(Connector::default());
    let req = sasl::SaslConnect {
        uri,
        config: Configuration::default(),
        time: None,
        auth: sasl::SaslAuth {
            authz_id: "".to_string(),
            authn_id: "user1".to_string(),
            password: "password1".to_string(),
        },
    };
    let res = sasl_srv.call(req).await;
    println!("E: {:?}", res.err());

    Ok(())
}

async fn sasl_auth<Io: AsyncRead + AsyncWrite + Unpin>(
    auth: server::Sasl<Io>,
) -> Result<server::HandshakeAck<Io, ()>, server::ServerError<()>> {
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
                server::Handshake::Sasl(auth) => {
                    sasl_auth(auth).await.map_err(|_| server::Error::from)
                }
            }
        })
        .finish(
            server::App::<()>::new()
                .service("test", fn_factory_with_config(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.addr().ip(), srv.addr().port())).unwrap();
    let sasl_srv = sasl::connect_service(Connector::default());

    let req = sasl::SaslConnect {
        uri,
        config: Configuration::default(),
        time: None,
        auth: sasl::SaslAuth {
            authz_id: "".to_string(),
            authn_id: "user1".to_string(),
            password: "password1".to_string(),
        },
    };
    let res = sasl_srv.call(req).await;
    println!("E: {:?}", res.err());

    Ok(())
}
