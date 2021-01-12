use std::{collections::VecDeque, future::Future, pin::Pin, task::Context, task::Poll};

use bytes::BytesMut;
use bytestring::ByteString;
use futures::Stream;
use ntex::{channel::oneshot, task::LocalWaker};
use ntex_amqp_codec::protocol::{
    Attach, DeliveryNumber, Disposition, Error, Handle, LinkError, ReceiverSettleMode, Role,
    SenderSettleMode, Source, TerminusDurability, TerminusExpiryPolicy, Transfer, TransferBody,
};
use ntex_amqp_codec::Encode;

use crate::cell::Cell;
use crate::error::AmqpProtocolError;
use crate::session::{Session, SessionInner};

#[derive(Clone, Debug)]
pub struct ReceiverLink {
    pub(crate) inner: Cell<ReceiverLinkInner>,
}

impl ReceiverLink {
    pub(crate) fn new(inner: Cell<ReceiverLinkInner>) -> ReceiverLink {
        ReceiverLink { inner }
    }

    pub fn handle(&self) -> Handle {
        self.inner.get_ref().handle as Handle
    }

    pub fn credit(&self) -> u32 {
        self.inner.get_ref().credit
    }

    pub fn session(&self) -> &Session {
        &self.inner.get_ref().session
    }

    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.inner.get_mut().session
    }

    pub fn frame(&self) -> &Attach {
        &self.inner.get_ref().attach
    }

    pub fn open(&mut self) {
        let inner = self.inner.get_mut();
        inner
            .session
            .inner
            .get_mut()
            .confirm_receiver_link(inner.handle, &inner.attach);
    }

    pub fn set_link_credit(&self, credit: u32) {
        self.inner.get_mut().set_link_credit(credit);
    }

    /// Set max total size for partial transfers.
    ///
    /// Default is 256Kb
    pub fn set_max_partial_transfer_size(&self, size: usize) {
        self.inner.get_mut().set_max_partial_transfer(size);
    }

    /// Send disposition frame
    pub fn send_disposition(&self, disp: Disposition) {
        self.inner
            .get_mut()
            .session
            .inner
            .get_mut()
            .post_frame(disp.into());
    }

    /// Wait for disposition with specified number
    pub fn wait_disposition(
        &self,
        id: DeliveryNumber,
    ) -> impl Future<Output = Result<Disposition, AmqpProtocolError>> {
        self.inner.get_mut().session.wait_disposition(id)
    }

    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        self.inner.get_mut().close(None)
    }

    pub fn close_with_error<E>(
        &self,
        error: E,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>>
    where
        Error: From<E>,
    {
        self.inner.get_mut().close(Some(error.into()))
    }

    pub(crate) fn remote_closed(&self, error: Option<Error>) {
        trace!("Receiver link has been closed remotely");
        let inner = self.inner.get_mut();
        inner.closed = true;
        inner.error = error;
        inner.reader_task.wake();
    }
}

impl Stream for ReceiverLink {
    type Item = Result<Transfer, AmqpProtocolError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.inner.get_mut();

        if inner.partial_body.is_some() && inner.queue.len() == 1 {
            if inner.closed {
                if let Some(err) = inner.error.take() {
                    Poll::Ready(Some(Err(AmqpProtocolError::LinkDetached(Some(err)))))
                } else {
                    Poll::Ready(None)
                }
            } else {
                inner.reader_task.register(cx.waker());
                Poll::Pending
            }
        } else if let Some(tr) = inner.queue.pop_front() {
            Poll::Ready(Some(Ok(tr)))
        } else if inner.closed {
            if let Some(err) = inner.error.take() {
                Poll::Ready(Some(Err(AmqpProtocolError::LinkDetached(Some(err)))))
            } else {
                Poll::Ready(None)
            }
        } else {
            inner.reader_task.register(cx.waker());
            Poll::Pending
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReceiverLinkInner {
    handle: Handle,
    attach: Attach,
    session: Session,
    closed: bool,
    reader_task: LocalWaker,
    queue: VecDeque<Transfer>,
    credit: u32,
    delivery_count: u32,
    error: Option<Error>,
    partial_body: Option<BytesMut>,
    partial_body_max: usize,
}

impl ReceiverLinkInner {
    pub(crate) fn new(
        session: Cell<SessionInner>,
        handle: Handle,
        attach: Attach,
    ) -> ReceiverLinkInner {
        ReceiverLinkInner {
            handle,
            session: Session::new(session),
            closed: false,
            reader_task: LocalWaker::new(),
            queue: VecDeque::with_capacity(4),
            credit: 0,
            error: None,
            partial_body: None,
            partial_body_max: 262144,
            delivery_count: attach.initial_delivery_count().unwrap_or(0),
            attach,
        }
    }

    pub(crate) fn detached(&mut self) {
        // drop pending transfers
        self.queue.clear();
        self.closed = true;
    }

    pub(crate) fn close(
        &mut self,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        let (tx, rx) = oneshot::channel();
        if self.closed {
            let _ = tx.send(Ok(()));
        } else {
            self.session
                .inner
                .get_mut()
                .detach_receiver_link(self.handle, true, error, tx);
        }
        self.reader_task.wake();

        async move {
            match rx.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(AmqpProtocolError::Disconnected),
            }
        }
    }

    fn set_max_partial_transfer(&mut self, size: usize) {
        self.partial_body_max = size;
    }

    pub(crate) fn set_link_credit(&mut self, credit: u32) {
        self.credit += credit;
        self.session
            .inner
            .get_mut()
            .rcv_link_flow(self.handle as u32, self.delivery_count, credit);
    }

    pub(crate) fn handle_transfer(&mut self, mut transfer: Transfer) {
        if self.credit == 0 {
            // check link credit
            let err = Error {
                condition: LinkError::TransferLimitExceeded.into(),
                description: None,
                info: None,
            };
            let _ = self.close(Some(err));
        } else {
            self.credit -= 1;

            if let Some(ref mut body) = self.partial_body {
                if transfer.delivery_id.is_some() {
                    // if delivery_id is set, then it should be equal to first transfer
                    if self.queue.back().unwrap().delivery_id != transfer.delivery_id {
                        let err = Error {
                            condition: LinkError::DetachForced.into(),
                            description: Some(ByteString::from_static("delivery_id is wrong")),
                            info: None,
                        };
                        let _ = self.close(Some(err));
                        return;
                    }
                }

                // merge transfer data and check size
                if let Some(transfer_body) = transfer.body.take() {
                    if body.len() + transfer_body.len() > self.partial_body_max {
                        let err = Error {
                            condition: LinkError::MessageSizeExceeded.into(),
                            description: None,
                            info: None,
                        };
                        let _ = self.close(Some(err));
                        return;
                    }

                    transfer_body.encode(body);
                }

                // received last partial transfer
                if !transfer.more {
                    self.delivery_count += 1;
                    self.queue.back_mut().unwrap().body = Some(TransferBody::Data(
                        self.partial_body.take().unwrap().freeze(),
                    ));
                    if self.queue.len() == 1 {
                        self.reader_task.wake()
                    }
                }
            } else if transfer.more {
                if transfer.delivery_id.is_none() {
                    let err = Error {
                        condition: LinkError::DetachForced.into(),
                        description: Some(ByteString::from_static("delivery_id is required")),
                        info: None,
                    };
                    let _ = self.close(Some(err));
                } else {
                    let body = if let Some(body) = transfer.body.take() {
                        match body {
                            TransferBody::Data(data) => BytesMut::from(data.as_ref()),
                            TransferBody::Message(msg) => {
                                let mut buf = BytesMut::with_capacity(msg.encoded_size());
                                msg.encode(&mut buf);
                                buf
                            }
                        }
                    } else {
                        BytesMut::new()
                    };
                    self.partial_body = Some(body);
                    self.queue.push_back(transfer);
                }
            } else {
                self.delivery_count += 1;
                self.queue.push_back(transfer);
                if self.queue.len() == 1 {
                    self.reader_task.wake()
                }
            }
        }
    }
}

pub struct ReceiverLinkBuilder {
    frame: Attach,
    session: Cell<SessionInner>,
}

impl ReceiverLinkBuilder {
    pub(crate) fn new(name: ByteString, address: ByteString, session: Cell<SessionInner>) -> Self {
        let source = Source {
            address: Some(address),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            dynamic: false,
            dynamic_node_properties: None,
            distribution_mode: None,
            filter: None,
            default_outcome: None,
            outcomes: None,
            capabilities: None,
        };
        let frame = Attach {
            name,
            handle: 0_u32,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(source),
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: Some(65536 * 4),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };

        ReceiverLinkBuilder { frame, session }
    }

    pub fn max_message_size(mut self, size: u64) -> Self {
        self.frame.max_message_size = Some(size);
        self
    }

    pub async fn open(self) -> Result<ReceiverLink, AmqpProtocolError> {
        let cell = self.session.clone();
        let res = self
            .session
            .get_mut()
            .open_local_receiver_link(cell, self.frame)
            .await;

        match res {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(AmqpProtocolError::Disconnected),
        }
    }
}
