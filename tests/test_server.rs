use actix_connector::{Connect, Connector};
use actix_service::{IntoNewService, NewService, Service};
use actix_test_server::TestServer;
use amqp_transport::server::{self, errors};
use amqp_transport::{self, client, sasl, Configuration};
use futures::future::{err, lazy};
use futures::Future;

fn server(link: server::OpenLink<()>) -> impl Future<Item = (), Error = errors::LinkError> {
    println!("OPEN LINK");
    let link = link.open(10);
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
        server::ServerFactory::new(
            Configuration::default(),
            server::ServiceFactory::service(
                server::App::<()>::new()
                    .service(
                        "test",
                        server
                            .into_new_service()
                            .map_init_err(|_| errors::LinkError::force_detach()),
                    )
                    .finish(),
            ),
        )
        .map_err(|_| ())
        .and_then(server::ServerDispatcher::default())
    });

    let mut sasl_srv = srv
        .block_on(lazy(|| {
            Ok::<_, ()>(sasl::connect_service(Connector::default()))
        }))
        .unwrap();
    let req = sasl::SaslConnect {
        connect: Connect::new(srv.host(), srv.port()),
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

fn sasl_auth(
    auth: server::SaslAuth,
) -> impl Future<Item = (), Error = amqp_codec::protocol::Error> {
    auth.mechanism("PLAIN")
        .mechanism("ANONYMOUS")
        .mechanism("MSSBCBS")
        .mechanism("AMQPCBS")
        .send()
        .and_then(|init| {
            if init.mechanism() == "PLAIN" {
                if let Some(resp) = init.initial_response() {
                    if resp == b"\0user1\0password1" {
                        return init.outcome(amqp_codec::protocol::SaslCode::Ok);
                    }
                }
            }
            init.outcome(amqp_codec::protocol::SaslCode::Auth)
        })
        .map_err(|e| e.into())
}

#[test]
fn test_sasl() -> std::io::Result<()> {
    let mut srv = TestServer::with(|| {
        server::ServerFactory::new(
            Configuration::default(),
            server::ServiceFactory::sasl(sasl_auth).service(
                server::App::<()>::new()
                    .service(
                        "test",
                        server
                            .into_new_service()
                            .map_init_err(|_| errors::LinkError::force_detach()),
                    )
                    .finish(),
            ),
        )
        .map_err(|_| ())
        .and_then(server::ServerDispatcher::default())
    });

    let mut sasl_srv = srv
        .block_on(lazy(|| {
            Ok::<_, ()>(sasl::connect_service(Connector::default()))
        }))
        .unwrap();
    let req = sasl::SaslConnect {
        connect: Connect::new(srv.host(), srv.port()),
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
