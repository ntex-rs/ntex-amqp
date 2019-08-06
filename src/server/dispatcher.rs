use std::fmt;

use actix_codec::{AsyncRead, AsyncWrite};
use actix_service::Service;
use amqp_codec::protocol::{Error, Frame, Role};
use amqp_codec::AmqpCodecError;
use futures::{Async, Future, Poll};
use slab::Slab;

use crate::cell::Cell;
use crate::connection::{ChannelState, Connection};
use crate::rcvlink::ReceiverLink;

use super::link::Link;

/// Amqp server connection dispatcher.
pub struct Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite,
    Sr: Service<Request = Link<St>, Response = ()>,
{
    conn: Connection<Io>,
    state: Cell<St>,
    service: Sr,
    links: Vec<(ReceiverLink, Sr::Future)>,
    channels: slab::Slab<ChannelState>,
}

impl<Io, St, Sr> Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite,
    Sr: Service<Request = Link<St>, Response = ()>,
    Sr::Error: fmt::Display + Into<Error>,
{
    pub(crate) fn new(conn: Connection<Io>, state: Cell<St>, service: Sr) -> Self {
        Dispatcher {
            conn,
            service,
            state,
            links: Vec::with_capacity(16),
            channels: Slab::with_capacity(16),
        }
    }
}

impl<Io, St, Sr> Future for Dispatcher<Io, St, Sr>
where
    Io: AsyncRead + AsyncWrite,
    Sr: Service<Request = Link<St>, Response = ()>,
    Sr::Error: fmt::Display + Into<Error>,
{
    type Item = ();
    type Error = AmqpCodecError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            // handle remote begin and attach
            match self.conn.poll_incoming() {
                Ok(Async::Ready(Some(frame))) => {
                    let (channel_id, frame) = frame.into_parts();
                    let channel_id = channel_id as usize;

                    match frame {
                        Frame::Begin(frm) => {
                            self.conn.register_remote_session(channel_id as u16, &frm);
                        }
                        Frame::Attach(attach) => match attach.role {
                            Role::Receiver => {
                                // remotly opened sender link
                                let session = self.conn.get_session(channel_id);
                                let cell = session.clone();
                                session.get_mut().confirm_sender_link(cell, attach);
                            }
                            Role::Sender => {
                                // receiver link
                                let session = self.conn.get_session(channel_id);
                                let cell = session.clone();
                                let link = session.get_mut().open_receiver_link(cell, attach);
                                let fut = self
                                    .service
                                    .call(Link::new(link.clone(), self.state.clone()));
                                self.links.push((link, fut));
                            }
                        },
                        _ => {
                            println!("===== {:?}", frame);
                        }
                    }
                }
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Err(e) => return Err(e),
            }
        }

        // process service responses
        let mut idx = 0;
        while idx < self.links.len() {
            match self.links[idx].1.poll() {
                Ok(Async::Ready(_detach)) => {
                    let (mut link, _) = self.links.swap_remove(idx);
                    link.close();
                }
                Ok(Async::NotReady) => idx += 1,
                Err(e) => {
                    let (mut link, _) = self.links.swap_remove(idx);
                    error!("Error in link handler: {}", e);
                    link.close_with_error(e.into());
                }
            }
        }

        let res = self.conn.poll_outgoing();
        self.conn.register_write_task();
        res
    }
}
