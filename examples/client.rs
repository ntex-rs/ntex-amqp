use futures::{SinkExt, StreamExt};

use ntex::connect::{Connect, Connector};
use ntex::Service;
use ntex_amqp::{codec, codec::protocol, Configuration};
use ntex_codec::Framed;

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_amqp=trace,basic=trace");
    env_logger::init();

    let connector = Connector::default();

    let io = connector
        .call(Connect::new("127.0.0.1:1883"))
        .await
        .unwrap();
    let mut framed = Framed::new(io, codec::ProtocolIdCodec);
    framed
        .send(codec::protocol::ProtocolId::Amqp)
        .await
        .unwrap();

    let frame = framed.next().await.unwrap();
    println!("FRM 1: {:#?}", frame);

    let mut framed = framed.map_codec(|_| codec::AmqpCodec::<codec::AmqpFrame>::new());
    framed
        .send(codec::AmqpFrame::new(
            0,
            protocol::Frame::Open(Configuration::default().to_open()),
        ))
        .await
        .unwrap();

    let frame = framed.next().await.unwrap();
    println!("FRM 2: {:#?}", frame);

    framed
        .send(codec::AmqpFrame::new(
            0,
            protocol::Frame::Begin(protocol::Begin {
                remote_channel: Some(1),
                next_outgoing_id: 0,
                incoming_window: 1024,
                outgoing_window: 1024,
                handle_max: 1024,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            }),
        ))
        .await
        .unwrap();

    let begin = framed.next().await.unwrap();
    println!("FRM 3: {:#?}", begin);

    Ok(())
}
