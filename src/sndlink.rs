use std::collections::VecDeque;

use amqp_codec::protocol::{Flow, Outcome, SequenceNo, TransferBody};
use bytes::Bytes;
use futures::{unsync::oneshot, Future};

use crate::cell::Cell;
use crate::errors::AmqpTransportError;
use crate::session::SessionInner;
use crate::{Delivery, DeliveryPromise, Handle};

#[derive(Clone)]
pub struct SenderLink {
    pub(crate) inner: Cell<SenderLinkInner>,
}

impl std::fmt::Debug for SenderLink {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_tuple("SenderLink")
            .field(&std::ops::Deref::deref(&self.inner.get_ref().name))
            .finish()
    }
}

pub(crate) struct SenderLinkInner {
    id: usize,
    idx: u32,
    name: string::String<Bytes>,
    session: Cell<SessionInner>,
    remote_handle: Handle,
    delivery_count: SequenceNo,
    link_credit: u32,
    pending_transfers: VecDeque<PendingTransfer>,
    error: Option<AmqpTransportError>,
    _closed: bool,
}

struct PendingTransfer {
    idx: u32,
    body: TransferBody,
    promise: DeliveryPromise,
}

impl SenderLink {
    pub(crate) fn new(inner: Cell<SenderLinkInner>) -> SenderLink {
        SenderLink { inner }
    }

    pub fn send<T>(&self, body: T) -> impl Future<Item = Outcome, Error = AmqpTransportError>
    where
        T: Into<TransferBody>,
    {
        self.inner.get_mut().send(body)
    }

    // pub fn close(&mut self) -> impl Future<Item = (), Error = AmqpTransportError> {
    //     self.inner.get_mut().close(None)
    // }

    // pub fn close_with_error(
    //     &mut self,
    //     error: Error,
    // ) -> impl Future<Item = (), Error = AmqpTransportError> {
    //     self.inner.get_mut().close(Some(error))
    // }
}

impl SenderLinkInner {
    pub(crate) fn new(
        id: usize,
        name: string::String<Bytes>,
        handle: Handle,
        delivery_count: SequenceNo,
        session: Cell<SessionInner>,
    ) -> SenderLinkInner {
        SenderLinkInner {
            id,
            name,
            session,
            delivery_count,
            idx: 0,
            remote_handle: handle,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            error: None,
            _closed: false,
        }
    }

    pub fn id(&self) -> u32 {
        self.id as u32
    }

    pub(crate) fn name(&self) -> &string::String<Bytes> {
        &self.name
    }

    pub(crate) fn detached(&mut self, err: AmqpTransportError) {
        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            let _ = tr.promise.send(Err(err.clone()));
        }

        self.error = Some(err);
    }

    // pub fn close(
    //     &mut self,
    //     error: Option<Error>,
    // ) -> impl Future<Item = (), Error = AmqpTransportError> {
    //     let (tx, rx) = oneshot::channel();
    //     if self.closed {
    //         let _ = tx.send(Ok(()));
    //     } else {
    //         self.session
    //             .inner
    //             .get_mut()
    //             .detach_receiver_link(self.handle, true, error, tx);
    //     }
    //     rx.then(|res| match res {
    //         Ok(Ok(_)) => Ok(()),
    //         Ok(Err(e)) => Err(e),
    //         Err(_) => Err(AmqpTransportError::Disconnected),
    //     })
    // }

    pub(crate) fn set_error(&mut self, err: AmqpTransportError) {
        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            let _ = tr.promise.send(Err(err.clone()));
        }

        self.error = Some(err);
    }

    pub fn apply_flow(&mut self, flow: &Flow) {
        // #2.7.6
        if let Some(credit) = flow.link_credit() {
            let delta = flow
                .delivery_count
                .unwrap_or(0)
                .saturating_add(credit)
                .saturating_sub(self.delivery_count);

            let session = self.session.get_mut();

            // credit became available => drain pending_transfers
            self.link_credit += delta;
            while self.link_credit > 0 {
                if let Some(transfer) = self.pending_transfers.pop_front() {
                    self.link_credit -= 1;
                    let _ = self.delivery_count.saturating_add(1);
                    session.send_transfer(
                        self.remote_handle,
                        transfer.idx,
                        transfer.body,
                        transfer.promise,
                    );
                } else {
                    break;
                }
            }
        }

        if flow.echo() {
            // todo: send flow
        }
    }

    pub fn send<T: Into<TransferBody>>(&mut self, body: T) -> Delivery {
        let body = body.into();
        let (delivery_tx, delivery_rx) = oneshot::channel();
        if self.link_credit == 0 {
            self.pending_transfers.push_back(PendingTransfer {
                body,
                idx: self.idx,
                promise: delivery_tx,
            });
        } else {
            let session = self.session.get_mut();
            self.link_credit -= 1;
            let _ = self.delivery_count.saturating_add(1);
            session.send_transfer(self.remote_handle, self.idx, body, delivery_tx);
        }
        let _ = self.idx.saturating_add(1);
        Delivery::Pending(delivery_rx)
    }
}
