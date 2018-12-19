use std::collections::VecDeque;

use amqp::protocol::{Flow, Outcome, SequenceNo};
use futures::{unsync::oneshot, Future};

use crate::cell::Cell;
use crate::errors::AmqpTransportError;
use crate::session::SessionInner;
use crate::{Delivery, DeliveryPromise, Handle, Message};

#[derive(Clone)]
pub struct SenderLink {
    inner: Cell<SenderLinkInner>,
}

pub(crate) struct SenderLinkInner {
    session: Cell<SessionInner>,
    remote_handle: Handle,
    delivery_count: SequenceNo,
    link_credit: u32,
    pending_transfers: VecDeque<PendingTransfer>,
    error: Option<AmqpTransportError>,
}

struct PendingTransfer {
    message: Message,
    promise: DeliveryPromise,
}

impl SenderLink {
    pub(crate) fn new(inner: Cell<SenderLinkInner>) -> SenderLink {
        SenderLink { inner }
    }

    pub fn send(
        &mut self,
        message: Message,
    ) -> impl Future<Item = Outcome, Error = AmqpTransportError> {
        self.inner.get_mut().send(message)
    }
}

impl SenderLinkInner {
    pub(crate) fn new(session: Cell<SessionInner>, handle: Handle) -> SenderLinkInner {
        SenderLinkInner {
            session,
            remote_handle: handle,
            delivery_count: 0,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            error: None,
        }
    }

    pub(crate) fn set_error(&mut self, err: AmqpTransportError) {
        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            let _ = tr.promise.send(Err(err.clone()));
        }

        self.error = Some(err);
    }

    pub fn apply_flow(&mut self, flow: &Flow) {
        if let Some(credit) = flow.link_credit() {
            let delta = (flow.delivery_count.unwrap_or(0) + credit)
                - (self.delivery_count + self.link_credit);
            if delta > 0 {
                // println!("link received credit. delta: {}, pending: {}", delta, self.pending_transfers.len());
                let old_credit = self.link_credit;
                self.link_credit += delta;
                if old_credit == 0 {
                    // credit became available => drain pending_transfers
                    while let Some(transfer) = self.pending_transfers.pop_front() {
                        // can't move to a fn because of self colliding with session
                        self.link_credit -= 1;
                        self.delivery_count += 1;
                        self.session.get_mut().send_transfer_conn(
                            self.remote_handle,
                            transfer.message,
                            transfer.promise,
                        );
                        if self.link_credit == 0 {
                            break;
                        }
                    }
                }
            } else {
                self.link_credit += ::std::cmp::max(0, self.link_credit + delta);
            }
        }

        if flow.echo() {
            // todo: send flow
        }
    }

    pub fn send(&mut self, message: Message) -> Delivery {
        let (delivery_tx, delivery_rx) = oneshot::channel();
        if self.link_credit == 0 {
            self.pending_transfers.push_back(PendingTransfer {
                message,
                promise: delivery_tx,
            });
        } else {
            let session = self.session.get_mut();
            // can't move to a fn because of self colliding with session
            self.link_credit -= 1;
            self.delivery_count += 1;
            session.send_transfer(self.remote_handle, message, delivery_tx);
        }
        Delivery::Pending(delivery_rx)
    }
}
