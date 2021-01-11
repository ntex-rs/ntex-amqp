use ntex_amqp::client;

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_amqp=trace,basic=trace");
    env_logger::init();

    let client = client::AmqpConnector::new("127.0.0.1:5671")
        .connect()
        .await
        .unwrap();

    Ok(())
}
