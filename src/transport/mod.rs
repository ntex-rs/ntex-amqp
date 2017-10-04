use futures::prelude::*;
use futures::{Sink, Stream, Async};
use futures::task::{self, Task};
use futures::unsync::oneshot;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::io::{read_exact, write_all};
use uuid::Uuid;
use std::rc::Rc;
use std::cell::RefCell;

use io::AmqpCodec;
use framing::SaslFrame;
use types::{Symbol, ByteStr};
use errors::*;
use protocol::*;

mod link;
mod session;
mod connection;

pub use self::link::*;
pub use self::session::*;
pub use self::connection::*;

pub enum Delivery {
    Resolved(Result<()>),
    Pending(oneshot::Receiver<Result<()>>),
    Gone
}

type DeliveryPromise = oneshot::Sender<Result<()>>;

impl Future for Delivery {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Delivery::Pending(ref mut receiver) = *self {
            return match receiver.poll() {
                Ok(Async::Ready(t)) => Ok(Async::Ready(())),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e.into())
            };
        }

        let old_v = ::std::mem::replace(self, Delivery::Gone);
        if let Delivery::Resolved(r) = old_v {
            return match r {
                Ok(_) => Ok(Async::Ready(())),
                Err(e) => Err(e)
            };
        }
        panic!("Polling Delivery after it was polled as ready is an error.");
    }
}

struct HandleVec<T>{
    items: Vec<Option<T>>,
    empty_count: u32,
    //max_handle: u32 todo: do we need max_handle checks in HandleTable?
}

impl<T: Clone> HandleVec<T> {
    pub fn new(/*max_handle: Handle*/) -> HandleVec<T> {
        HandleVec {
            items: Vec::<Option<T>>::with_capacity(4),
            empty_count: 0,
            //max_handle: max_handle
        }
    }

    pub fn push(&mut self, item: T) -> Handle {
        if self.empty_count == 0 {
            let len = self.items.len();
            // if len > self.max_handle {
            //     return Err("Handle pool is exhausted".into());
            // }
            self.items.push(Some(item));
            return len as Handle;
        }
        let index = self.items.iter().position(|i| i.is_none()).expect("empty_count got out of sync.");
        self.items[index as usize] = Some(item);
        self.empty_count -= 1;
        index as Handle
    }

    pub fn get(&self, handle: Handle) -> Option<T> {
        if let Some(ref r) = self.items[handle as usize] {
            return Some(r.clone())
        }
        None
    }

    pub fn set(&mut self, handle: Handle, item: T) {
        let handle = handle as usize;
        let len = self.items.len();
        if handle >= len {
            self.empty_count += (handle + 1 - len) as u32;
            self.items.resize_default(handle + 1);
        }
        if self.items[handle as usize].is_some() {
            panic!("handle is set while it must not be");
        }
        self.items[handle as usize] = Some(item);
        self.empty_count -= 1;
    }

    pub fn remove(&mut self, handle: Handle) -> Option<T> {
        self.empty_count += 1;
        self.items[handle as usize].take()
    }
}

#[async]
fn negotiate_protocol<T: AsyncRead + AsyncWrite + 'static>(protocol_id: ProtocolId, io: T) -> Result<T> {
    let header_buf = encode_protocol_header(protocol_id);
    let (io, _) = await!(write_all(io, header_buf))?;
    let header_buf = [0; 8];
    let (io, header_buf) = await!(read_exact(io, header_buf))?;
    let recv_protocol_id = decode_protocol_header(&header_buf)?; // todo: surface for higher level to be able to respond properly / validate
    if recv_protocol_id != protocol_id {
        return Err(
            format!(
                "Expected `{:?}` protocol id, seen `{:?} instead.`",
                protocol_id,
                recv_protocol_id
            ).into(),
        );
    }
    Ok(io)
}

/// negotiating SASL authentication
#[async]
pub fn sasl_auth<T: AsyncRead + AsyncWrite + 'static>(authz_id: String, authn_id: String, password: String, io: T) -> Result<T> {
    let io = await!(negotiate_protocol(ProtocolId::AmqpSasl, io))?;

    let sasl_io = io.framed(AmqpCodec::<SaslFrame>::new());

    // processing sasl-mechanisms
    let (sasl_frame, sasl_io) = await!(sasl_io.into_future()).map_err(|e| e.0)?;
    let plain_symbol = Symbol::from_static("PLAIN");
    if let Some(SaslFrame { body: SaslFrameBody::SaslMechanisms(mechs) }) = sasl_frame {
        if !mechs
            .sasl_server_mechanisms()
            .iter()
            .any(|m| *m == plain_symbol)
        {
            return Err(format!("only PLAIN SASL mechanism is supported. server supports: {:?}", mechs.sasl_server_mechanisms()).into());
        }
    } else {
        return Err(format!("expected SASL Mechanisms frame to arrive, seen `{:?}` instead.", sasl_frame).into());
    }
    // sending sasl-init
    let initial_response = SaslInit::prepare_response(&authz_id, &authn_id, &password);
    let sasl_init = SaslInit {
        mechanism: plain_symbol,
        initial_response: Some(initial_response),
        hostname: None,
    };
    let sasl_io = await!(sasl_io.send(SaslFrame::new(SaslFrameBody::SaslInit(sasl_init))))?;
    // processing sasl-outcome
    let (sasl_frame, sasl_io) = await!(sasl_io.into_future()).map_err(|e| e.0)?;
    if let Some(SaslFrame {
        body: SaslFrameBody::SaslOutcome(outcome),
    }) = sasl_frame
    {
        if outcome.code() != SaslCode::Ok {
            return Err(
                format!(
                    "SASL auth did not result in Ok outcome, seen `{:?}` instead. More info: {:?}",
                    outcome.code(),
                    outcome.additional_data()
                ).into(),
            );
        }
    } else {
        return Err(
            format!(
                "expected SASL Outcome frame to arrive, seen `{:?}` instead.",
                sasl_frame
            ).into(),
        );
    }

    let io = sasl_io.into_inner();
    Ok(io)
}
