use ntex::service::{fn_factory_with_config, Service};
use ntex::util::Ready;
use ntex_amqp::{error::AmqpError, error::LinkError, server};

async fn server(
    link: server::Link<()>,
) -> Result<
    Box<
        dyn Service<
                server::Transfer,
                Response = server::Outcome,
                Error = AmqpError,
                Future = Ready<server::Outcome, AmqpError>,
            > + 'static,
    >,
    LinkError,
> {
    println!("OPEN LINK: {:?}", link);
    Err(LinkError::force_detach().description("unimplemented"))
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "trace,ntex_io=info");
    env_logger::init();

    ntex::server::Server::build()
        .bind("amqp", "127.0.0.1:5671", |_| {
            server::Server::build(|con: server::Handshake| async move {
                match con {
                    server::Handshake::Amqp(con) => {
                        let con = con.open().await.unwrap();
                        Ok(con.ack(()))
                    }
                    server::Handshake::Sasl(_) => Err(AmqpError::not_implemented()),
                }
            })
            .finish(
                server::Router::new()
                    .service("test", fn_factory_with_config(server))
                    .finish(),
            )
        })?
        .workers(1)
        .run()
        .await
}
