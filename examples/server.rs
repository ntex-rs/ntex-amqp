use futures::future::Ready;

use ntex::service::{fn_factory_with_config, Service};
use ntex_amqp::{error::AmqpError, error::LinkError, server};

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

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_amqp=trace,basic=trace");
    env_logger::init();

    ntex::server::Server::build()
        .bind("amqp", "127.0.0.1:5671", || {
            let srv = server::Server::new(|con: server::Handshake<_>| async move {
                println!("===============");
                match con {
                    server::Handshake::Amqp(con) => {
                        let con = con.open().await.unwrap();
                        Ok(con.ack(()))
                    }
                    server::Handshake::Sasl(_) => Err(AmqpError::not_implemented()),
                }
            });
            srv.finish(
                server::Router::new()
                    .service("test", fn_factory_with_config(server))
                    .finish(),
            )
        })?
        .workers(1)
        .run()
        .await
}
