use ntex::{ServiceFactory, SharedCfg};
use ntex_amqp::client;

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_amqp=trace,basic=trace");
    env_logger::init();

    let driver = client::Connector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call("127.0.0.1:5671")
        .await
        .unwrap();
    let sink = driver.sink();

    ntex::rt::spawn(driver.start_default());

    let _session = sink.open_session().await.unwrap();

    Ok(())
}
