use std::marker::PhantomData;

use actix_codec::{AsyncRead, AsyncWrite};
use actix_service::{NewService, Service};
use amqp::protocol::{Error, Frame, Role};
use futures::future::{ok, FutureResult};
use futures::{Async, Future, Poll};
use slab::Slab;

use crate::cell::Cell;
use crate::connection::{ChannelState, Connection};
use crate::link::ReceiverLink;

use super::link::OpenLink;

/// Sasl server dispatcher service factory
pub struct ServerDispatcher<Io, St, S> {
    _t: PhantomData<(Io, St, S)>,
}

impl<Io, St, S> Default for ServerDispatcher<Io, St, S>
where
    Io: AsyncRead + AsyncWrite,
    S: Service<OpenLink<St>, Response = (), Error = Error>,
{
    fn default() -> Self {
        ServerDispatcher { _t: PhantomData }
    }
}

impl<Io, St, S> NewService<(St, S, Connection<Io>)> for ServerDispatcher<Io, St, S>
where
    Io: AsyncRead + AsyncWrite,
    S: Service<OpenLink<St>, Response = (), Error = Error>,
{
    type Response = ();
    type Error = ();
    type InitError = ();
    type Service = ServerDispatcherImpl<Io, St, S>;
    type Future = FutureResult<Self::Service, Self::Error>;

    fn new_service(&self) -> Self::Future {
        ok(ServerDispatcherImpl::default())
    }
}

/// Sasl server dispatcher service
pub struct ServerDispatcherImpl<Io, St, S> {
    _t: PhantomData<(Io, St, S)>,
}

impl<Io, St, S> Default for ServerDispatcherImpl<Io, St, S>
where
    Io: AsyncRead + AsyncWrite,
    S: Service<OpenLink<St>, Response = (), Error = Error>,
{
    fn default() -> Self {
        ServerDispatcherImpl { _t: PhantomData }
    }
}

impl<Io, St, S> Service<(St, S, Connection<Io>)> for ServerDispatcherImpl<Io, St, S>
where
    Io: AsyncRead + AsyncWrite,
    S: Service<OpenLink<St>, Response = (), Error = Error>,
{
    type Response = ();
    type Error = ();
    type Future = Dispatcher<Io, St, S>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, (st, srv, conn): (St, S, Connection<Io>)) -> Self::Future {
        Dispatcher::new(conn, st, srv)
    }
}

/// Amqp server connection dispatcher.
pub struct Dispatcher<Io, St, S>
where
    Io: AsyncRead + AsyncWrite,
    S: Service<OpenLink<St>, Response = (), Error = Error>,
{
    conn: Connection<Io>,
    state: Cell<St>,
    service: S,
    links: Vec<(ReceiverLink, S::Future)>,
    channels: slab::Slab<ChannelState>,
}

impl<Io, St, S> Dispatcher<Io, St, S>
where
    Io: AsyncRead + AsyncWrite,
    S: Service<OpenLink<St>, Response = (), Error = Error>,
{
    pub fn new(conn: Connection<Io>, state: St, service: S) -> Self {
        Dispatcher {
            conn,
            service,
            links: Vec::with_capacity(16),
            state: Cell::new(state),
            channels: Slab::with_capacity(16),
        }
    }
}

impl<Io, St, S> Future for Dispatcher<Io, St, S>
where
    Io: AsyncRead + AsyncWrite,
    S: Service<OpenLink<St>, Response = (), Error = Error>,
{
    type Item = ();
    type Error = ();

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
                        Frame::Attach(attach) => {
                            if attach.role == Role::Receiver {
                                let mut session = self.conn.get_session(channel_id);
                                let cell = session.clone();
                                session.get_mut().confirm_sender_link(cell, attach);
                            } else {
                                let mut session = self.conn.get_session(channel_id);
                                let cell = session.clone();
                                let link = session.get_mut().open_receiver_link(cell, attach);

                                let fut = self.service.call(OpenLink {
                                    state: self.state.clone(),
                                    link: link.clone(),
                                });
                                self.links.push((link, fut));
                            }
                        }
                        _ => {
                            println!("===== {:?}", frame);
                        }
                    }
                }
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Err(_) => return Err(()),
            }
        }

        // process service responses
        let mut idx = 0;
        while idx < self.links.len() {
            match self.links[idx].1.poll() {
                Ok(Async::Ready(detach)) => {
                    let (link, _) = self.links.swap_remove(idx);
                    link.close();
                }
                Ok(Async::NotReady) => idx += 1,
                Err(e) => {
                    let (link, _) = self.links.swap_remove(idx);
                    error!("Error in link handler: {}", e);
                    link.close_with_error(e.into());
                }
            }
        }

        self.conn.poll_outgoing().map_err(|_| ())
    }
}
