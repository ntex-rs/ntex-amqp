use std::{
    collections::VecDeque, future::poll_fn, future::Future, hash, pin::Pin, task::Context,
    task::Poll,
};

use ntex::util::{ByteString, Bytes, BytesMut, PoolRef, Stream};
use ntex::{channel::oneshot, task::LocalWaker};
use ntex_amqp_codec::protocol::{
    self as codec, Attach, Disposition, Error, Handle, LinkError, ReceiverSettleMode, Role,
    SenderSettleMode, Source, TerminusDurability, TerminusExpiryPolicy, Transfer, TransferBody,
};
use ntex_amqp_codec::{types::Symbol, types::Variant, Encode};

use crate::session::{Session, SessionInner};
use crate::{cell::Cell, error::AmqpProtocolError, types::Action, Delivery};

#[derive(Clone, Debug)]
pub struct ReceiverLink {
    pub(crate) inner: Cell<ReceiverLinkInner>,
}

#[derive(Debug)]
pub(crate) struct ReceiverLinkInner {
    name: ByteString,
    handle: Handle,
    remote_handle: Handle,
    session: Session,
    closed: bool,
    reader_task: LocalWaker,
    queue: VecDeque<(Delivery, Transfer)>,
    credit: u32,
    delivery_count: u32,
    error: Option<Error>,
    partial_body: Option<BytesMut>,
    max_message_size: u64,
    pool: PoolRef,
}

impl Eq for ReceiverLink {}

impl PartialEq<ReceiverLink> for ReceiverLink {
    fn eq(&self, other: &ReceiverLink) -> bool {
        std::ptr::eq(self.inner.get_ref(), other.inner.get_ref())
    }
}

impl hash::Hash for ReceiverLink {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        (self.inner.get_ref() as *const _ as usize).hash(state);
    }
}

impl ReceiverLink {
    pub(crate) fn new(inner: Cell<ReceiverLinkInner>) -> ReceiverLink {
        ReceiverLink { inner }
    }

    pub fn name(&self) -> &ByteString {
        &self.inner.get_ref().name
    }

    pub fn handle(&self) -> Handle {
        self.inner.get_ref().handle as Handle
    }

    pub fn remote_handle(&self) -> Handle {
        self.inner.get_ref().remote_handle as Handle
    }

    pub fn credit(&self) -> u32 {
        self.inner.get_ref().credit
    }

    pub fn session(&self) -> &Session {
        &self.inner.get_ref().session
    }

    pub fn is_closed(&self) -> bool {
        self.inner.get_ref().closed
    }

    pub fn error(&self) -> Option<&Error> {
        self.inner.get_ref().error.as_ref()
    }

    pub(crate) fn confirm_receiver_link(&self, frm: &Attach) {
        let inner = self.inner.get_mut();
        let size = self.inner.get_ref().max_message_size;
        let size = if size != 0 { Some(size) } else { None };
        inner
            .session
            .inner
            .get_mut()
            .confirm_receiver_link(inner.handle, frm, size);
    }

    pub fn set_link_credit(&self, credit: u32) {
        self.inner.get_mut().set_link_credit(credit);
    }

    /// Set max message size.
    pub fn set_max_message_size(&self, size: u64) {
        self.inner.get_mut().max_message_size = size;
    }

    /// Check deliveries
    pub fn has_deliveries(&self) -> bool {
        let inner = self.inner.get_ref();
        if inner.partial_body.is_none() {
            !inner.queue.is_empty()
        } else {
            inner.queue.len() > 1
        }
    }

    /// Get delivery
    pub fn get_delivery(&self) -> Option<(Delivery, Transfer)> {
        let inner = self.inner.get_mut();
        if inner.partial_body.is_none() || inner.queue.len() > 1 {
            inner.queue.pop_front()
        } else {
            None
        }
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

    pub(crate) fn remote_detached(&self, error: Option<Error>) {
        let inner = self.inner.get_mut();
        if let Some(ref error) = error {
            log::error!(
                "{}: Receiver link has been closed remotely handle: {:?} name: {:?} error: {:?}",
                inner.session.tag(),
                inner.remote_handle,
                inner.name,
                error
            );
        } else {
            log::trace!(
                "{}: Receiver link has been closed remotely handle: {:?} name: {:?}",
                inner.session.tag(),
                inner.remote_handle,
                inner.name
            );
        }
        inner.closed = true;
        inner.error = error;
        inner.wake();
    }

    /// Attempt to pull out the next value of this receiver, registering
    /// the current task for wakeup if the value is not yet available,
    /// and returning None if the stream is exhausted.
    pub async fn recv(&self) -> Option<Result<(Delivery, Transfer), AmqpProtocolError>> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Attempt to pull out the next value of this receiver, registering
    /// the current task for wakeup if the value is not yet available,
    /// and returning None if the stream is exhausted.
    pub fn poll_recv(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(Delivery, Transfer), AmqpProtocolError>>> {
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

impl Stream for ReceiverLink {
    type Item = Result<(Delivery, Transfer), AmqpProtocolError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_recv(cx)
    }
}

impl ReceiverLinkInner {
    pub(crate) fn new(
        session: Cell<SessionInner>,
        handle: Handle,
        remote_handle: Handle,
        frame: &Attach,
    ) -> ReceiverLinkInner {
        let pool = session.get_ref().memory_pool();
        let mut name = frame.name().clone();
        name.trimdown();

        ReceiverLinkInner {
            name,
            handle,
            remote_handle,
            pool,
            session: Session::new(session),
            closed: false,
            queue: VecDeque::with_capacity(4),
            credit: 0,
            error: None,
            partial_body: None,
            delivery_count: frame.initial_delivery_count().unwrap_or(0),
            max_message_size: 262_144,
            reader_task: LocalWaker::new(),
        }
    }

    fn wake(&self) {
        self.reader_task.wake();
    }

    pub(crate) fn id(&self) -> Handle {
        self.handle
    }

    pub(crate) fn name(&self) -> &ByteString {
        &self.name
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
        self.closed = true;
        self.wake();

        async move {
            match rx.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(AmqpProtocolError::Disconnected),
            }
        }
    }

    pub(crate) fn set_link_credit(&mut self, credit: u32) {
        self.credit += credit;
        self.session
            .inner
            .get_mut()
            .rcv_link_flow(self.handle, self.delivery_count, credit);
    }

    #[allow(clippy::unnecessary_unwrap)]
    pub(crate) fn handle_transfer(
        &mut self,
        mut transfer: Transfer,
        inner: &Cell<ReceiverLinkInner>,
    ) -> Result<Action, AmqpProtocolError> {
        if self.credit == 0 {
            // check link credit
            let err = Error(Box::new(codec::ErrorInner {
                condition: LinkError::TransferLimitExceeded.into(),
                description: None,
                info: None,
            }));
            let _ = self.close(Some(err));
            Ok(Action::None)
        } else {
            if !transfer.0.more {
                self.credit -= 1;
            }

            // handle batched transfer
            if let Some(ref mut body) = self.partial_body {
                if transfer.0.delivery_id.is_some() {
                    // if delivery_id is set, then it should be equal to first transfer
                    if self
                        .queue
                        .back()
                        .map_or(true, |back| Some(back.0.id()) != transfer.0.delivery_id)
                    {
                        let err = Error(Box::new(codec::ErrorInner {
                            condition: LinkError::DetachForced.into(),
                            description: Some(ByteString::from_static("delivery_id is wrong")),
                            info: None,
                        }));
                        let _ = self.close(Some(err));
                        return Ok(Action::None);
                    }
                }

                // merge transfer data and check size
                if let Some(transfer_body) = transfer.0.body.take() {
                    if body.len() + transfer_body.len() > self.max_message_size as usize {
                        let err = Error(Box::new(codec::ErrorInner {
                            condition: LinkError::MessageSizeExceeded.into(),
                            description: None,
                            info: None,
                        }));
                        let _ = self.close(Some(err));
                        return Ok(Action::None);
                    }

                    transfer_body.encode(body);
                }

                if transfer.more() {
                    // dont need to update queue, we use first transfer frame as primary
                    Ok(Action::None)
                } else {
                    // received last partial transfer
                    self.delivery_count += 1;
                    let partial_body = self.partial_body.take();
                    if partial_body.is_some() && !self.queue.is_empty() {
                        self.queue.back_mut().unwrap().1 .0.body =
                            Some(TransferBody::Data(partial_body.unwrap().freeze()));
                        if self.queue.len() == 1 {
                            self.wake();
                        }
                        Ok(Action::Transfer(ReceiverLink {
                            inner: inner.clone(),
                        }))
                    } else {
                        log::error!("{}: Inconsistent state, bug", self.session.tag());
                        let err = Error(Box::new(codec::ErrorInner {
                            condition: LinkError::DetachForced.into(),
                            description: Some(ByteString::from_static("Internal error")),
                            info: None,
                        }));
                        let _ = self.close(Some(err));
                        Ok(Action::None)
                    }
                }
            } else if transfer.more() {
                // handle first transfer in batch
                if let Some(id) = transfer.delivery_id() {
                    let body = if let Some(body) = transfer.0.body.take() {
                        match body {
                            TransferBody::Data(data) => BytesMut::copy_from_slice(&data),
                            TransferBody::Message(msg) => {
                                let mut buf = self.pool.buf_with_capacity(msg.encoded_size());
                                msg.encode(&mut buf);
                                buf
                            }
                        }
                    } else {
                        self.pool.buf_with_capacity(16)
                    };
                    self.partial_body = Some(body);

                    let delivery = Delivery::new_rcv(
                        id,
                        self.handle,
                        transfer.delivery_tag().cloned().unwrap_or_else(Bytes::new),
                        transfer.settled().unwrap_or_default(),
                        self.session.clone(),
                    );
                    self.queue.push_back((delivery, transfer));
                    Ok(Action::None)
                } else {
                    let err = Error(Box::new(codec::ErrorInner {
                        condition: LinkError::DetachForced.into(),
                        description: Some(ByteString::from_static("delivery_id is required")),
                        info: None,
                    }));
                    let _ = self.close(Some(err));
                    Ok(Action::None)
                }
            } else if let Some(id) = transfer.delivery_id() {
                self.delivery_count += 1;
                let delivery = Delivery::new_rcv(
                    id,
                    self.handle,
                    transfer.delivery_tag().cloned().unwrap_or_else(Bytes::new),
                    transfer.settled().unwrap_or_default(),
                    self.session.clone(),
                );
                self.queue.push_back((delivery, transfer));
                if self.queue.len() == 1 {
                    self.wake();
                }
                Ok(Action::Transfer(ReceiverLink {
                    inner: inner.clone(),
                }))
            } else {
                let err = Error(Box::new(codec::ErrorInner {
                    condition: LinkError::DetachForced.into(),
                    description: Some(ByteString::from_static("delivery_id is required")),
                    info: None,
                }));
                let _ = self.close(Some(err));
                Ok(Action::None)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct EstablishedReceiverLink(ReceiverLink);

impl EstablishedReceiverLink {
    pub(crate) fn new(inner: Cell<ReceiverLinkInner>) -> EstablishedReceiverLink {
        EstablishedReceiverLink(ReceiverLink::new(inner))
    }
}

impl std::ops::Deref for EstablishedReceiverLink {
    type Target = ReceiverLink;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for EstablishedReceiverLink {
    fn drop(&mut self) {
        self.0.inner.get_mut().closed = true;
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
        let frame = Attach(Box::new(codec::AttachInner {
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
        }));

        ReceiverLinkBuilder { frame, session }
    }

    /// Set attach frame max message size
    pub fn max_message_size(mut self, size: u64) -> Self {
        self.frame.0.max_message_size = Some(size);
        self
    }

    /// Set or reset a receive link property
    pub fn property<K, V>(mut self, key: K, value: Option<V>) -> Self
    where
        Symbol: From<K>,
        Variant: From<V>,
    {
        let key = key.into();
        let props = self.frame.get_properties_mut();

        match value {
            Some(value) => props.insert(key, value.into()),
            None => props.remove(&key),
        };
        self
    }

    /// Modify attach frame
    pub fn with_frame<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Attach),
    {
        f(&mut self.frame);
        self
    }

    /// Attach receiver link
    pub async fn attach(self) -> Result<ReceiverLink, AmqpProtocolError> {
        let cell = self.session.clone();
        let res = self
            .session
            .get_mut()
            .attach_local_receiver_link(cell, self.frame)
            .await;

        match res {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(AmqpProtocolError::Disconnected),
        }
    }
}
