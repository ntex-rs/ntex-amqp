use std::fmt::Display;

use actix_codec::{AsyncRead, AsyncWrite};
use actix_service::Service;
use amqp::protocol::{Error, Frame};
use futures::{Async, Future, Poll};
use slab::Slab;

use crate::cell::Cell;
use crate::connection::{ChannelState, Connection};
use crate::link::ReceiverLink;

use super::link::OpenLink;

/// Amqp server connection dispatcher.
pub struct Dispatcher<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite,
    F: Service<(), Response = (St, S)> + Clone,
    F::Error: Display + Into<Error>,
    S: Service<OpenLink<St>, Response = ()>,
    S::Error: Display + Into<Error>,
{
    conn: Connection<Io>,
    state: State<F, St, S>,
    links: Slab<()>,
    channels: slab::Slab<ChannelState>,
}

impl<Io, F, St, S> Dispatcher<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite,
    F: Service<(), Response = (St, S)> + Clone,
    F::Error: Display + Into<Error>,
    S: Service<OpenLink<St>, Response = ()>,
    S::Error: Display + Into<Error>,
{
    pub fn new(conn: Connection<Io>, factory: &mut F) -> Self {
        Dispatcher {
            conn,
            links: Slab::with_capacity(16),
            state: State::CreateService(factory.call(())),
            channels: Slab::with_capacity(16),
        }
    }
}

enum State<F, St, S>
where
    F: Service<(), Response = (St, S)> + Clone,
    F::Error: Display + Into<Error>,
    S: Service<OpenLink<St>, Response = ()>,
    S::Error: Display + Into<Error>,
{
    CreateService(F::Future),
    Processing(S, Cell<St>, Vec<(ReceiverLink, S::Future)>),
    Done,
}

impl<Io, F, St, S> Future for Dispatcher<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite,
    F: Service<(), Response = (St, S)> + Clone,
    F::Error: Display + Into<Error>,
    S: Service<OpenLink<St>, Response = ()>,
    S::Error: Display + Into<Error>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!("DISP POLL");

        match self.state {
            State::CreateService(ref mut fut) => match fut.poll() {
                Ok(Async::NotReady) => (),
                Ok(Async::Ready((state, srv))) => {
                    self.state = State::Processing(srv, Cell::new(state), Vec::new());
                    return self.poll();
                }
                Err(err) => {
                    self.conn.close_with_error(err.into());
                    return Ok(Async::Ready(()));
                }
            },
            State::Processing(ref mut srv, ref mut st, ref mut links) => {
                loop {
                    // handle remote begin and attach
                    match self.conn.poll_incoming() {
                        Ok(Async::Ready(Some(frame))) => {
                            let (channel_id, frame) = frame.into_parts();

                            match frame {
                                Frame::Begin(frm) => {
                                    self.conn.register_remote_session(channel_id, &frm);
                                }
                                Frame::Attach(attach) => {
                                    let channel_id = channel_id as usize;
                                    let entry = self.links.vacant_entry();
                                    let token = entry.key();
                                    entry.insert(());

                                    let mut session = self.conn.get_session(channel_id);
                                    let cell = session.clone();
                                    let link = session.get_mut().open_receiver_link(cell, attach);

                                    let fut = srv.call(OpenLink {
                                        state: st.clone(),
                                        link: link.clone(),
                                    });
                                    links.push((link, fut));
                                }
                                _ => {
                                    println!("===== {:?}", frame);
                                }
                            }
                        }
                        Ok(Async::Ready(None)) => break,
                        Ok(Async::NotReady) => break,
                        Err(_) => return Err(()),
                    }
                }

                // process service responses
                let mut idx = 0;
                while idx < links.len() {
                    match links[idx].1.poll() {
                        Ok(Async::Ready(detach)) => {
                            println!("READY: {:?}", detach);
                            let (link, _) = links.swap_remove(idx);
                            link.close();
                        }
                        Ok(Async::NotReady) => idx += 1,
                        Err(e) => {
                            let (link, _) = links.swap_remove(idx);
                            error!("Error in link handler: {}", e);
                            link.close_with_error(e.into());
                        }
                    }
                }
            }
            State::Done => (),
        }

        self.conn.poll_outgoing().map_err(|_| ())?;
        Ok(Async::NotReady)
    }
}
