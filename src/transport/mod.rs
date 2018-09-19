use futures::prelude::*;
use futures::task::{self, Task};
use futures::unsync::oneshot;
use futures::{Async, Future, Sink, Stream};
use std::cell::RefCell;
use std::rc::Rc;
use tokio_codec::Framed;
use tokio_io::io::{read_exact, write_all};
use tokio_io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

use errors::*;
use framing::SaslFrame;
use io::AmqpCodec;
use protocol::*;
use types::{ByteStr, Symbol};

mod connection;
mod link;
mod message;
mod session;

pub use self::connection::*;
pub use self::link::*;
pub use self::message::*;
pub use self::session::*;

pub enum Delivery {
    Resolved(Result<Outcome>),
    Pending(oneshot::Receiver<Result<Outcome>>),
    Gone,
}

type DeliveryPromise = oneshot::Sender<Result<Outcome>>;

impl Future for Delivery {
    type Item = Outcome;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Delivery::Pending(ref mut receiver) = *self {
            return match receiver.poll() {
                Ok(Async::Ready(r)) => r.map(|state| Async::Ready(state)),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e.into()),
            };
        }

        let old_v = ::std::mem::replace(self, Delivery::Gone);
        if let Delivery::Resolved(r) = old_v {
            return match r {
                Ok(state) => Ok(Async::Ready(state)),
                Err(e) => Err(e),
            };
        }
        panic!("Polling Delivery after it was polled as ready is an error.");
    }
}

struct HandleVec<T> {
    items: Vec<Option<T>>,
    empty_count: u32,
    //max_handle: u32 todo: do we need max_handle checks in HandleVec?
}

impl<T: Clone> HandleVec<T> {
    pub fn new() -> HandleVec<T> {
        HandleVec {
            items: Vec::<Option<T>>::with_capacity(4),
            empty_count: 0,
            //max_handle: max_handle
        }
    }

    pub fn push(&mut self, item: T) -> Handle {
        if self.empty_count == 0 {
            let len = self.items.len();
            // ensure!(len <= self.max_handle, "Handle pool is exhausted");
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
            return Some(r.clone());
        }
        None
    }

    pub fn set(&mut self, handle: Handle, item: T) {
        let handle = handle as usize;
        let len = self.items.len();
        if handle >= len {
            self.empty_count += (handle + 1 - len) as u32;
            while self.items.len() <= handle {
                self.items.push(None); // resize_default(handle + 1);
            }
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

fn negotiate_protocol<T>(protocol_id: ProtocolId, io: T) -> impl Future<Item = T, Error = Error>
where
    T: AsyncRead + AsyncWrite,
{
    let header_buf = encode_protocol_header(protocol_id);
    write_all(io, header_buf).map_err(Error::from).and_then(|(io, _)| {
        let header_buf = [0; 8];
        read_exact(io, header_buf).map_err(Error::from).and_then(|(io, header_buf)| {
            let recv_protocol_id = decode_protocol_header(&header_buf)?; // todo: surface for higher level to be able to respond properly / validate
                                                                         // ensure!(
                                                                         //     recv_protocol_id == protocol_id,
                                                                         //     "Expected `{:?}` protocol id, seen `{:?} instead.`",
                                                                         //     protocol_id,
                                                                         //     recv_protocol_id);
            Ok(io)
        })
    })
}

/// negotiating SASL authentication
pub fn sasl_auth<T>(authz_id: String, authn_id: String, password: String, io: T) -> impl Future<Item = T, Error = Error>
where
    T: AsyncRead + AsyncWrite,
{
    negotiate_protocol(ProtocolId::AmqpSasl, io).and_then(move |io| {
        let sasl_io = Framed::new(io, AmqpCodec::<SaslFrame>::new());

        // processing sasl-mechanisms
        sasl_io.into_future().map_err(|e| e.0).and_then(move |(sasl_frame, sasl_io)| {
            let plain_symbol = Symbol::from_static("PLAIN");
            // if let Some(SaslFrame { body: SaslFrameBody::SaslMechanisms(mechs) }) = sasl_frame {
            //     if !mechs
            //         .sasl_server_mechanisms()
            //         .iter()
            //         .any(|m| *m == plain_symbol)
            //     {
            //         bail!("only PLAIN SASL mechanism is supported. server supports: {:?}", mechs.sasl_server_mechanisms());
            //     }
            // } else {
            //     bail!("expected SASL Mechanisms frame to arrive, seen `{:?}` instead.", sasl_frame);
            // }

            // sending sasl-init
            let initial_response = SaslInit::prepare_response(&authz_id, &authn_id, &password);
            let sasl_init = SaslInit {
                mechanism: plain_symbol,
                initial_response: Some(initial_response),
                hostname: None,
            };
            sasl_io.send(SaslFrame::new(SaslFrameBody::SaslInit(sasl_init))).map_err(Error::from).and_then(|sasl_io| {
                // processing sasl-outcome
                sasl_io.into_future().map_err(|e| Error::from(e.0)).and_then(|(sasl_frame, sasl_io)| {
                    if let Some(SaslFrame {
                        body: SaslFrameBody::SaslOutcome(outcome),
                    }) = sasl_frame
                    {
                        ensure!(
                            outcome.code() == SaslCode::Ok,
                            "SASL auth did not result in Ok outcome, seen `{:?}` instead. More info: {:?}",
                            outcome.code(),
                            outcome.additional_data()
                        );
                    } else {
                        bail!("expected SASL Outcome frame to arrive, seen `{:?}` instead.", sasl_frame);
                    }

                    let io = sasl_io.into_inner();
                    Ok(io)
                })
            })
        })
    })
}
