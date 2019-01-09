use std::collections::VecDeque;

use amqp::protocol::{Attach, Disposition, Error, Flow, Outcome, SequenceNo, Transfer};
use amqp::types::ByteStr;
use futures::task::AtomicTask;
use futures::{unsync::oneshot, Async, Future, Poll, Stream};

use crate::cell::Cell;
use crate::errors::AmqpTransportError;
use crate::session::SessionInner;
use crate::{Delivery, DeliveryPromise, Handle, Message};

#[derive(Clone)]
pub struct SenderLink {
    inner: Cell<SenderLinkInner>,
}

pub(crate) struct SenderLinkInner {
    id: usize,
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
    pub(crate) fn new(session: Cell<SessionInner>, id: usize, handle: Handle) -> SenderLinkInner {
        SenderLinkInner {
            id,
            session,
            remote_handle: handle,
            delivery_count: 0,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            error: None,
        }
    }

    pub fn id(&self) -> u32 {
        self.id as u32
    }

    pub(crate) fn detached(&mut self, err: AmqpTransportError) {
        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            let _ = tr.promise.send(Err(err.clone()));
        }

        self.error = Some(err);
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
                        self.session.get_mut().send_transfer(
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

#[derive(Clone)]
pub struct ReceiverLink {
    inner: Cell<ReceiverLinkInner>,
}

impl ReceiverLink {
    pub(crate) fn new(inner: Cell<ReceiverLinkInner>) -> ReceiverLink {
        ReceiverLink { inner }
    }

    pub fn frame(&self) -> &Attach {
        &self.inner.get_ref().attach
    }

    pub fn open(&mut self) {
        let inner = self.inner.get_mut();
        inner
            .session
            .get_mut()
            .confirm_receiver_link(inner.handle, &inner.attach);
    }

    pub fn set_flow(&mut self) {
        let inner = self.inner.get_mut();

        let flow = Flow {
            next_incoming_id: Some(1),
            incoming_window: 5000,
            next_outgoing_id: 1,
            outgoing_window: 0,
            handle: Some(inner.handle as u32),
            delivery_count: Some(0),
            link_credit: Some(5000),
            available: Some(0),
            drain: false,
            echo: false,
            properties: None,
            body: None,
        };
        inner.session.get_mut().post_frame(flow.into());
    }

    /// Send disposition frame
    pub fn send_disposition(&mut self, disp: Disposition) {
        self.inner
            .get_mut()
            .session
            .get_mut()
            .post_frame(disp.into());
    }

    pub fn close(mut self) -> impl Future<Item = (), Error = AmqpTransportError> {
        self.inner.get_mut().close(None)
    }

    pub fn close_with_error(
        mut self,
        error: Error,
    ) -> impl Future<Item = (), Error = AmqpTransportError> {
        self.inner.get_mut().close(Some(error))
    }
}

impl Stream for ReceiverLink {
    type Item = Transfer;
    type Error = AmqpTransportError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let inner = self.inner.get_mut();

        if let Some(tr) = inner.queue.pop_front() {
            Ok(Async::Ready(Some(tr)))
        } else {
            if inner.closed {
                Ok(Async::Ready(None))
            } else {
                inner.reader_task.register();
                Ok(Async::NotReady)
            }
        }
    }
}

pub(crate) struct ReceiverLinkInner {
    handle: usize,
    attach: Attach,
    session: Cell<SessionInner>,
    closed: bool,
    reader_task: AtomicTask,
    queue: VecDeque<Transfer>,
}

impl ReceiverLinkInner {
    pub(crate) fn new(
        session: Cell<SessionInner>,
        handle: usize,
        attach: Attach,
    ) -> ReceiverLinkInner {
        ReceiverLinkInner {
            handle,
            attach,
            session,
            closed: false,
            reader_task: AtomicTask::new(),
            queue: VecDeque::with_capacity(4),
        }
    }

    pub fn name(&self) -> &ByteStr {
        &self.attach.name
    }

    pub fn close(
        &mut self,
        error: Option<Error>,
    ) -> impl Future<Item = (), Error = AmqpTransportError> {
        let (tx, rx) = oneshot::channel();
        if self.closed {
            let _ = tx.send(Ok(()));
        } else {
            self.session
                .get_mut()
                .detach_receiver_link(self.handle, true, error, tx);
        }
        rx.then(|res| match res {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AmqpTransportError::Disconnected),
        })
    }

    pub fn handle_transfer(&mut self, transfer: Transfer) {
        self.queue.push_back(transfer);
        if self.queue.len() == 1 {
            self.reader_task.notify()
        }
    }
}
