# AMQP 1.0 Client/Server Framework

[![build status](https://github.com/ntex-rs/ntex-amqp/actions/workflows/linux.yml/badge.svg?branch=master&event=push)](https://github.com/ntex-rs/ntex-amqp/actions/workflows/linux.yml/badge.svg) [![codecov](https://codecov.io/gh/ntex-rs/ntex-amqp/branch/master/graph/badge.svg)](https://codecov.io/gh/ntex-rs/ntex-amqp) [![crates.io](https://img.shields.io/crates/v/ntex-amqp.svg)](https://crates.io/crates/ntex-amqp)

`ntex-amqp` is an asynchronous [AMQP 1.0](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html)
client and server framework built on [ntex](https://github.com/ntex-rs/ntex).
It can be used to build AMQP clients, servers, brokers and protocol gateways.

The crate handles connections, sessions, sender and receiver links, flow
control, multi-frame transfers and delivery settlement. On the server side,
incoming links can be routed to services by address. SASL authentication,
idle timeouts and configurable handshake and link-attach timeouts are
supported as well.

AMQP types and message encoding are provided by
[`ntex-amqp-codec`](codec).

## Getting started

```toml
[dependencies]
ntex = "4"
ntex-amqp = "6"
```

### Running a server

Every incoming connection goes through a handshake handler that accepts or
rejects it. Once connected, receiver links are routed by target address to a
service. The example below accepts regular AMQP connections and SASL `PLAIN`
connections, then handles messages sent to `queue`.

```rust
use ntex::{SharedCfg, service::fn_service};
use ntex_amqp::{AmqpServiceConfig, codec::protocol::SaslCode, error::LinkError, server};

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // Connection-wide AMQP settings
    let cfg = SharedCfg::new("AMQP").add(AmqpServiceConfig::new().set_max_frame_size(64 * 1024));

    ntex::server::Server::builder()
        .bind("amqp", "127.0.0.1:5672", cfg, async |_| {
            server::Server::builder(async |con: server::Handshake| match con {
                server::Handshake::Amqp(con) => Ok(con.open().await?.ack(())),
                server::Handshake::Sasl(sasl) => {
                    let init = sasl.mechanism("PLAIN").init().await?;

                    // PLAIN uses `authzid\0authcid\0password`
                    if init.initial_response() == Some(b"\0user\0password") {
                        Ok(init.outcome(SaslCode::Ok).await?.open().await?.ack(()))
                    } else {
                        let _ = init.outcome(SaslCode::Auth).await;
                        Err(server::HandshakeError::Disconnected(None))
                    }
                }
            })
            .build(
                server::Router::<()>::builder()
                    .service("queue", async |link: &server::Link<()>| {
                        println!("Link attached: {:?}", link.path());
                        Ok::<_, LinkError>(fn_service(async |tr: server::Transfer| {
                            println!("Message: {:?}", tr.get_body());
                            Ok::<_, LinkError>(server::Outcome::Accept)
                        }))
                    })
                    .build(),
            )
        })?
        .run()
        .await
}
```

For more control over connection and link lifecycle events, add a control
service with `ServerBuilder::control()`. The same `AmqpServiceConfig` can be
used by clients by adding it to the `SharedCfg` passed to `client::Connector`.

### Sending a message

The client connection is driven in a background task. After opening a session
and attaching a sender link, a message can be sent and its delivery outcome
awaited:

```rust
use ntex::{Pipeline, SharedCfg, util::Bytes};
use ntex_amqp::client;

#[ntex::main]
async fn main() {
    let driver = Pipeline::new(SharedCfg::default(), client::Connector::new())
        .call(client::Connect::new("127.0.0.1:5672").sasl_auth(
            "".into(),
            "user".into(),
            "password".into(),
        ))
        .await
        .unwrap();

    let sink = driver.sink();
    ntex::rt::spawn(driver.start_default());

    let session = sink.open_session().await.unwrap();
    let link = session
        .build_sender_link("sender", "queue")
        .attach()
        .await
        .unwrap();

    let delivery = link
        .transfer(Bytes::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    println!("Delivery state: {:?}", delivery.wait().await);

    sink.close().await.unwrap();
}
```

To receive messages instead, open a link with
`Session::build_receiver_link()`. Read incoming deliveries with
`ReceiverLink::recv()` and settle them with `Delivery::settle()`.

## Examples

Run the server in one terminal and the client in another:

```sh
cargo run --example server
cargo run --example client
```

To log incoming and outgoing AMQP frames, enable the `frame-trace` feature and
set the log level to `trace`.

## Rust version

The minimum supported Rust version is 1.97.

## License

This project is available under either of the following licenses, at your
option:

* [Apache License, Version 2.0](LICENSE-APACHE)
* [MIT License](LICENSE-MIT)
