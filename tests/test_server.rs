use actix_amqp::server::{self, errors};
use actix_amqp::{self, sasl, Configuration};
use actix_codec::{AsyncRead, AsyncWrite};
use actix_connect::{default_connector, TcpConnector};
use actix_service::{new_service_cfg, IntoNewService, NewService, Service};
use actix_test_server::TestServer;
use futures::future::{err, lazy, Either, FutureResult};
use futures::Future;
use http::{HttpTryFrom, Uri};

fn server(
    link: &server::Link<()>,
) -> impl Future<
    Item = Box<
        dyn Service<
                Request = server::Message<()>,
                Response = server::Outcome,
                Error = errors::AmqpError,
                Future = FutureResult<server::Message<()>, server::Outcome>,
            > + 'static,
    >,
    Error = errors::LinkError,
> {
    println!("OPEN LINK: {:?}", link);
    err(errors::LinkError::force_detach().description("unimplemented"))
}

#[test]
fn test_simple() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "actix_codec=info,actix_server=trace,actix_connector=trace,amqp_transport=trace",
    );
    env_logger::init();

    let mut srv = TestServer::with(|| {
        server::Server::new(
            server::Handshake::new(|conn: server::Connect<_>| {
                conn.open()
                    .map_err(|_| panic!())
                    .and_then(|conn| Ok::<_, errors::AmqpError>(conn.ack(())))
            })
            .sasl(server::sasl::no_sasl()),
        )
        .finish(
            server::App::<()>::new()
                .service("test", new_service_cfg(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.host(), srv.port())).unwrap();
    let mut sasl_srv = srv
        .block_on(lazy(|| {
            Ok::<_, ()>(sasl::connect_service(default_connector()))
        }))
        .unwrap();
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
    let res = srv.block_on(sasl_srv.call(req));
    println!("E: {:?}", res.err());

    Ok(())
}

fn sasl_auth<Io: AsyncRead + AsyncWrite>(
    auth: server::Sasl<Io>,
) -> impl Future<Item = server::ConnectAck<Io, ()>, Error = server::errors::ServerError<()>> {
    auth.mechanism("PLAIN")
        .mechanism("ANONYMOUS")
        .mechanism("MSSBCBS")
        .mechanism("AMQPCBS")
        .init()
        .and_then(|init| {
            if init.mechanism() == "PLAIN" {
                if let Some(resp) = init.initial_response() {
                    if resp == b"\0user1\0password1" {
                        return Either::A(
                            init.outcome(amqp_codec::protocol::SaslCode::Ok)
                                .and_then(|succ| succ.open().map(|f| f.ack(()))),
                        );
                    }
                }
            }
            Either::B(
                init.outcome(amqp_codec::protocol::SaslCode::Auth)
                    .and_then(|succ| succ.open().map(|f| f.ack(()))),
            )
        })
        .map_err(|e| e.into())
}

#[test]
fn test_sasl() -> std::io::Result<()> {
    let mut srv = TestServer::with(|| {
        server::Server::new(
            server::Handshake::new(|conn: server::Connect<_>| {
                conn.open()
                    .map_err(|_| panic!())
                    .and_then(|conn| Ok::<_, errors::Error>(conn.ack(())))
            })
            .sasl(sasl_auth.into_new_service().map_err(|e| e.into())),
        )
        .finish(
            server::App::<()>::new()
                .service("test", new_service_cfg(server))
                .finish(),
        )
    });

    let uri = Uri::try_from(format!("amqp://{}:{}", srv.host(), srv.port())).unwrap();
    let mut sasl_srv = srv
        .block_on(lazy(|| {
            Ok::<_, ()>(sasl::connect_service(TcpConnector::new()))
        }))
        .unwrap();
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
    let res = srv.block_on(sasl_srv.call(req));
    println!("E: {:?}", res.err());

    Ok(())
}
