#![feature(proc_macro, conservative_impl_trait, generators, vec_resize_default)]

extern crate amqp1 as amqp;
extern crate bytes;
extern crate futures_await as futures;
extern crate hex_slice;
extern crate tokio_core;
extern crate tokio_io;
extern crate native_tls;
extern crate tokio_tls;
extern crate uuid;
extern crate chrono;

use futures::prelude::*;
use tokio_core::net::TcpStream;
use tokio_core::reactor;
use std::net::SocketAddr;
use futures::{future, Future};

use amqp::{transport, Error, ErrorKind, Result};
use amqp::protocol::ProtocolId;
use bytes::Bytes;
use native_tls::TlsConnector;
use tokio_tls::TlsConnectorExt;
use std::net::ToSocketAddrs;
use std::time::Instant;

const SASL_ID: &str = "";
const SASL_KEY: &str = "";
const HOST_NAME: &str = "";
const EVENT_HUB_NAME: &str = "";

fn main() {
    let mut core = reactor::Core::new().unwrap();
    let handle = core.handle();
    let client = send(handle);
    core.run(client).unwrap();
}

#[async]
fn send(handle: reactor::Handle) -> Result<()> {
    let mut input = String::new();
    //std::io::stdin().read_line(&mut input);

    let addr = format!("{}:5671", HOST_NAME).to_socket_addrs().unwrap().next().unwrap();
    let socket = await!(TcpStream::connect(&addr, &handle))?;
    let tls_context = TlsConnector::builder().unwrap().build().unwrap();
    let io = await!(tls_context.connect_async(HOST_NAME, socket).map_err(|e| Error::with_chain(e, ErrorKind::Msg("TLS handshake failed".into()))))?;

    let io = await!(transport::sasl_auth("".into(), SASL_ID.into(), SASL_KEY.into(), io))?;

    let conn = await!(transport::Connection::open(HOST_NAME.into(), handle, io))?;
    let session = await!(conn.open_session())?;

    let partition_link = await!(session.open_sender_link(format!("{}/Partitions/00", EVENT_HUB_NAME), "00".into()))?;

    let start_time = Instant::now();

    let deliveries: Vec<_> = (0..10).map(|_| partition_link.send(Bytes::from(vec![1,2,3,4,5,6]))).collect();
    await!(future::join_all(deliveries))?;

    println!("transfers completed in {}.", chrono::Duration::from_std(start_time.elapsed()).unwrap());

    std::io::stdin().read_line(&mut input);

    Ok(())
}