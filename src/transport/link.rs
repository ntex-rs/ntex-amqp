use bytes::Bytes;
use futures::unsync::oneshot;

use protocol::*;
use super::*;
use std::collections::VecDeque;

#[derive(Clone)]
pub struct SenderLink {
    inner: Rc<RefCell<SenderLinkInner>>,
}

pub(crate) struct SenderLinkInner {
    session: Rc<RefCell<SessionInner>>,
    remote_handle: Handle,
    delivery_count: SequenceNo,
    link_credit: u32,
    pending_transfers: VecDeque<PendingTransfer>,
}

struct PendingTransfer {
    message: Message,
    promise: DeliveryPromise,
}

impl SenderLink {
    pub(crate) fn new(inner: Rc<RefCell<SenderLinkInner>>) -> SenderLink {
        SenderLink { inner }
    }

    pub fn send(&self, message: Message) -> Delivery {
        self.inner.borrow_mut().send(message)
    }
}

impl SenderLinkInner {
    pub(crate) fn new(session: Rc<RefCell<SessionInner>>, handle: Handle) -> SenderLinkInner {
        SenderLinkInner {
            session,
            remote_handle: handle,
            delivery_count: 0,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
        }
    }

    pub fn apply_flow(&mut self, flow: &Flow, session: &mut SessionInner, conn: &mut ConnectionInner) {
        if let Some(credit) = flow.link_credit() {
            let delta = (flow.delivery_count.unwrap_or(0) + credit) - (self.delivery_count + self.link_credit);
            if delta > 0 {
                println!("link received credit. delta: {}, pending: {}", delta, self.pending_transfers.len());
                let old_credit = self.link_credit;
                self.link_credit += delta;
                if old_credit == 0 {
                    // credit became available => drain pending_transfers
                    while let Some(transfer) = self.pending_transfers.pop_front() {
                        // can't move to a fn because of self colliding with session
                        self.link_credit -= 1;
                        self.delivery_count += 1;
                        session.send_transfer_conn(conn, self.remote_handle, transfer.message, transfer.promise);
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
            let mut session = self.session.borrow_mut();
            // can't move to a fn because of self colliding with session
            self.link_credit -= 1;
            self.delivery_count += 1;
            session.send_transfer(self.remote_handle, message, delivery_tx);
        }
        Delivery::Pending(delivery_rx)
    }
}
