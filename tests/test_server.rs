use actix_service::{IntoNewService, NewService, Service};
use actix_test_server::TestServer;
use amqp;
use amqp_transport::server::{self, errors};
use amqp_transport::{self, client, Configuration, Connection};
use futures::future::err;
use futures::{Future, Sink};

fn server(link: server::OpenLink<()>) -> impl Future<Item = (), Error = errors::LinkError> {
    println!("OPEN LINK");
    let link = link.open();
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
        server::handshake(Configuration::default())
            .map_err(|_| ())
            .and_then(server::ServerFactory::new(server::ServiceFactory::service(
                server
                    .into_new_service()
                    .map_init_err(|_| errors::LinkError::force_detach()),
            )))
    });

    let stream = srv.connect()?;
    let framed = client::ProtocolNegotiation::framed(stream);
    let mut proto = client::ProtocolNegotiation::default();
    let framed = srv.block_on(proto.call(framed)).unwrap();
    let framed = framed.into_framed(amqp::AmqpCodec::new());

    let open = Configuration::default().to_open(None);
    let framed = srv
        .block_on(framed.send(amqp::AmqpFrame::new(0, open.clone().into())))
        .unwrap();

    let mut conn = Connection::new(framed, Configuration::default(), (&open).into(), None);
    let session = conn.open_session();
    srv.spawn(conn.map_err(|_| ()));

    let mut session = srv.block_on(session).unwrap();
    let link = srv.block_on(session.open_sender_link("test", "test"));

    // println!("RES: {:?}", res);

    Ok(())
}
