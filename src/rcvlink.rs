use std::{
    collections::VecDeque, future::Future, future::poll_fn, hash, pin::Pin, task::Context,
    task::Poll,
};

use ntex_amqp_codec::protocol::{
    self as codec, Attach, Disposition, Error, Flow, Handle, LinkError, ReceiverSettleMode, Role,
    SenderSettleMode, SequenceNo, Source, Symbols, TerminusDurability, TerminusExpiryPolicy,
    Transfer, TransferBody,
};
use ntex_amqp_codec::{Encode, types::Symbol, types::Variant};
use ntex_bytes::{BytePages, ByteString, Bytes};
use ntex_util::time::{Seconds, timeout_checked};
use ntex_util::{Stream, task::LocalWaker};

use crate::session::{Session, SessionInner};
use crate::{Delivery, cell::Cell, detach, error::AmqpProtocolError, types::Action};

#[derive(Clone)]
pub struct ReceiverLink {
    pub(crate) inner: Cell<ReceiverLinkInner>,
}

pub(crate) struct ReceiverLinkInner {
    name: ByteString,
    handle: Handle,
    remote_handle: Handle,
    session: Session,
    closed: bool,
    // remote link is not confirmed yet
    opening: bool,
    reader_task: LocalWaker,
    queue: VecDeque<(Delivery, Transfer)>,
    credit: u32,
    delivery_count: SequenceNo,
    error: Option<AmqpProtocolError>,
    partial_body: Option<BytePages>,
    max_message_size: u64,
}

impl std::fmt::Debug for ReceiverLink {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_tuple("ReceiverLink")
            .field(&self.inner.get_ref().name)
            .finish()
    }
}

impl std::fmt::Debug for ReceiverLinkInner {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_tuple("ReceiverLinkInner")
            .field(&&*self.name)
            .finish()
    }
}

impl Eq for ReceiverLink {}

impl PartialEq<ReceiverLink> for ReceiverLink {
    fn eq(&self, other: &ReceiverLink) -> bool {
        std::ptr::eq(self.inner.get_ref(), other.inner.get_ref())
    }
}

impl hash::Hash for ReceiverLink {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        (std::ptr::from_ref(self.inner.get_ref()) as usize).hash(state);
    }
}

impl ReceiverLink {
    pub(crate) fn new(inner: Cell<ReceiverLinkInner>) -> ReceiverLink {
        ReceiverLink { inner }
    }

    /// Name of the receiver link
    #[inline]
    pub fn name(&self) -> &ByteString {
        &self.inner.get_ref().name
    }

    /// Local handle
    #[inline]
    pub fn handle(&self) -> Handle {
        self.inner.get_ref().handle
    }

    /// Remote handle
    #[inline]
    pub fn remote_handle(&self) -> Handle {
        self.inner.get_ref().remote_handle
    }

    /// Returns available link credit
    #[inline]
    pub fn credit(&self) -> u32 {
        self.inner.get_ref().credit
    }

    /// Reference to session
    #[inline]
    pub fn session(&self) -> &Session {
        &self.inner.get_ref().session
    }

    /// Check if link is closed
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.get_ref().closed
    }

    /// Remote detach error
    pub fn error(&self) -> Option<&Error> {
        if let Some(AmqpProtocolError::LinkDetached(Some(err))) = &self.inner.get_ref().error {
            Some(err)
        } else {
            None
        }
    }

    /// Confirm remote link, returns `false` if link is not established
    pub(crate) fn confirm_receiver_link(&self, response: Attach) -> bool {
        let inner = self.inner.get_ref();
        let size = inner.max_message_size;
        let size = if size != 0 { Some(size) } else { None };
        inner
            .session
            .inner
            .get_mut()
            .confirm_receiver_link(&self.inner, response, size)
    }

    /// Add credit to the link.
    ///
    /// Each queued, not yet received delivery may keep its read buffer
    /// alive, credit bounds the memory retained by the link.
    pub fn set_link_credit(&self, credit: u32) {
        self.inner.get_mut().set_link_credit(credit);
    }

    /// Set max message size.
    ///
    /// Larger messages detach the link with `message-size-exceeded` error.
    /// If max size is set to `0`, size is unlimited.
    pub fn set_max_message_size(&self, size: u64) {
        self.inner.get_mut().max_message_size = size;
    }

    /// Check if link has completely received deliveries
    pub fn has_deliveries(&self) -> bool {
        self.inner.get_ref().ready_deliveries() > 0
    }

    /// Get completely received delivery
    pub fn get_delivery(&self) -> Option<(Delivery, Transfer)> {
        let inner = self.inner.get_mut();
        if inner.ready_deliveries() > 0 {
            inner.queue.pop_front()
        } else {
            None
        }
    }

    /// Send disposition frame
    pub fn send_disposition(&self, disp: Disposition) {
        self.inner
            .get_ref()
            .session
            .inner
            .get_mut()
            .post_frame(disp.into());
    }

    /// Close receiver link
    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        self.inner.get_mut().close(None)
    }

    /// Close receiver link with error
    pub fn close_with_error<E>(
        &self,
        error: E,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>>
    where
        Error: From<E>,
    {
        self.inner.get_mut().close(Some(error.into()))
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
        if let Some(tr) = self.get_delivery() {
            return Poll::Ready(Some(Ok(tr)));
        }

        let inner = self.inner.get_mut();
        if inner.closed {
            Poll::Ready(inner.error.take().map(Err))
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
        max_message_size: u64,
        opening: bool,
    ) -> ReceiverLinkInner {
        let mut name = frame.name().clone();
        name.trimdown();

        ReceiverLinkInner {
            name,
            handle,
            remote_handle,
            session: Session::new(session),
            closed: false,
            opening,
            queue: VecDeque::with_capacity(4),
            credit: 0,
            error: None,
            partial_body: None,
            delivery_count: frame.initial_delivery_count().unwrap_or(0),
            max_message_size,
            reader_task: LocalWaker::new(),
        }
    }

    fn wake(&self) {
        self.reader_task.wake();
    }

    /// Number of completely received deliveries
    fn ready_deliveries(&self) -> usize {
        self.queue
            .len()
            .saturating_sub(usize::from(self.partial_body.is_some()))
    }

    /// Link is detached by peer
    pub(crate) fn remote_detached(&mut self, error: Option<Error>) {
        if let Some(ref error) = error {
            log::warn!(
                "{}: Receiver link has been closed remotely handle: {:?} name: {:?} error: {:?}",
                self.session.tag(),
                self.remote_handle,
                self.name,
                error
            );
        } else {
            log::trace!(
                "{}: Receiver link has been closed remotely handle: {:?} name: {:?}",
                self.session.tag(),
                self.remote_handle,
                self.name
            );
        }
        self.set_closed(error.map(|err| AmqpProtocolError::LinkDetached(Some(err))));
    }

    /// Session is ended or connection is closed
    pub(crate) fn session_ended(&mut self, err: AmqpProtocolError) {
        log::trace!(
            "{}: Receiver link is closed by session error handle: {:?} name: {:?} error: {err:?}",
            self.session.tag(),
            self.remote_handle,
            self.name,
        );
        self.set_closed(Some(err));
    }

    fn set_closed(&mut self, error: Option<AmqpProtocolError>) {
        self.closed = true;
        self.error = error;
        self.wake();
    }

    /// Link flow state `(handle, delivery-count, link-credit, drain)`
    pub(crate) fn flow_state(&self) -> (Handle, SequenceNo, u32, bool) {
        (self.handle, self.delivery_count, self.credit, false)
    }

    pub(crate) fn id(&self) -> Handle {
        self.handle
    }

    pub(crate) fn set_delivery_count(&mut self, delivery_count: SequenceNo) {
        self.delivery_count = delivery_count;
    }

    /// Apply sender's delivery-count, delivery-limit is preserved (#2.6.7)
    pub(crate) fn apply_flow(&mut self, flow: &Flow) {
        // sender counts multi-frame delivery at first frame, receiver at last frame
        if self.partial_body.is_some() {
            return;
        }
        if let Some(snd_delivery_count) = flow.delivery_count() {
            let limit = self.delivery_count.wrapping_add(self.credit);
            let credit = limit.wrapping_sub(snd_delivery_count);
            if credit > i32::MAX as u32 {
                log::warn!(
                    "{}: Receiver link {:?} flow delivery count {:?} is beyond delivery limit {:?}",
                    self.session.tag(),
                    self.name,
                    snd_delivery_count,
                    limit
                );
                self.credit = 0;
            } else {
                self.credit = credit;
            }
            self.delivery_count = snd_delivery_count;
        }
    }

    pub(crate) fn name(&self) -> &ByteString {
        &self.name
    }

    pub(crate) fn detached(&mut self) {
        // drop pending transfers
        self.queue.clear();
        self.closed = true;
    }

    /// Link is detached locally, wake up reader
    pub(crate) fn local_detached(&mut self) {
        self.closed = true;
        self.wake();
    }

    pub(crate) fn close(
        &mut self,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        let detach = (!self.closed).then(|| {
            self.session
                .inner
                .get_mut()
                .detach_receiver_link(self.handle, true, error)
        });
        self.local_detached();

        async move {
            match detach {
                Some(detach) => detach.await,
                None => Ok(()),
            }
        }
    }

    fn close_size_exceeded(&mut self) -> Action {
        let err = Error(Box::new(codec::ErrorInner {
            condition: LinkError::MessageSizeExceeded.into(),
            description: None,
            info: None,
        }));
        let _ = self.close(Some(err));
        Action::None
    }

    /// Aborted delivery is implicitly settled and its payload is ignored.
    ///
    /// Application never sees aborted delivery, so its credit is returned
    /// to the sender, otherwise the link could stall without credit.
    fn delivery_aborted(&mut self) -> Action {
        self.delivery_count = self.delivery_count.wrapping_add(1);
        self.session
            .inner
            .get_mut()
            .rcv_link_flow(self.handle, self.delivery_count, self.credit);
        Action::None
    }

    pub(crate) fn set_link_credit(&mut self, credit: u32) {
        // link handle could be reused by another link
        if self.closed {
            return;
        }
        self.credit = self.credit.saturating_add(credit);

        // credit is sent after link confirmation
        if !self.opening {
            self.session.inner.get_mut().rcv_link_flow(
                self.handle,
                self.delivery_count,
                self.credit,
            );
        }
    }

    /// Mark remote link as confirmed, returns pending link credit
    pub(crate) fn confirmed(&mut self) -> Option<(SequenceNo, u32)> {
        self.opening = false;
        if self.credit > 0 {
            Some((self.delivery_count, self.credit))
        } else {
            None
        }
    }

    #[allow(clippy::unnecessary_unwrap)]
    pub(crate) fn handle_transfer(
        &mut self,
        mut transfer: Transfer,
        inner: &Cell<ReceiverLinkInner>,
    ) -> Action {
        if self.credit == 0 {
            // check link credit
            let err = Error(Box::new(codec::ErrorInner {
                condition: LinkError::TransferLimitExceeded.into(),
                description: None,
                info: None,
            }));
            let _ = self.close(Some(err));
            Action::None
        } else {
            // aborted transfer ends the delivery regardless of `more`,
            // credit used by aborted delivery is returned to the sender
            let aborted = transfer.0.aborted;
            if !transfer.0.more && !aborted {
                self.credit -= 1;
            }

            // handle batched transfer
            if let Some(ref mut body) = self.partial_body {
                if transfer.0.delivery_id.is_some() {
                    // if delivery_id is set, then it should be equal to first transfer
                    if self
                        .queue
                        .back()
                        .is_none_or(|back| Some(back.0.id()) != transfer.0.delivery_id)
                    {
                        let err = Error(Box::new(codec::ErrorInner {
                            condition: LinkError::DetachForced.into(),
                            description: Some(ByteString::from_static("delivery_id is wrong")),
                            info: None,
                        }));
                        let _ = self.close(Some(err));
                        return Action::None;
                    }
                }

                if aborted {
                    self.partial_body = None;
                    if let Some((delivery, _)) = self.queue.pop_back() {
                        delivery.discard();
                    }
                    return self.delivery_aborted();
                }

                // merge transfer data and check size
                if let Some(transfer_body) = transfer.0.body.take() {
                    if size_exceeded(self.max_message_size, body.len() + transfer_body.len()) {
                        return self.close_size_exceeded();
                    }

                    append_body(body, transfer_body);
                }

                if transfer.more() {
                    // dont need to update queue, we use first transfer frame as primary
                    Action::None
                } else {
                    // received last partial transfer
                    self.delivery_count = self.delivery_count.wrapping_add(1);
                    let partial_body = self.partial_body.take();
                    if partial_body.is_some() && !self.queue.is_empty() {
                        self.queue.back_mut().unwrap().1.0.body =
                            Some(TransferBody::Data(partial_body.unwrap().freeze()));
                        if self.queue.len() == 1 {
                            self.wake();
                        }
                        Action::Transfer(ReceiverLink {
                            inner: inner.clone(),
                        })
                    } else {
                        log::error!("{}: Inconsistent state, bug", self.session.tag());
                        let err = Error(Box::new(codec::ErrorInner {
                            condition: LinkError::DetachForced.into(),
                            description: Some(ByteString::from_static("Internal error")),
                            info: None,
                        }));
                        let _ = self.close(Some(err));
                        Action::None
                    }
                }
            } else if aborted {
                self.delivery_aborted()
            } else if transfer.more() {
                // handle first transfer in batch
                if let Some(id) = transfer.delivery_id() {
                    if size_exceeded(self.max_message_size, body_len(&transfer)) {
                        return self.close_size_exceeded();
                    }

                    let mut body = BytePages::default();
                    if let Some(data) = transfer.0.body.take() {
                        append_body(&mut body, data);
                    }
                    self.partial_body = Some(body);

                    // transfer is stored until the last partial transfer
                    if let Some(tag) = transfer.0.delivery_tag.as_mut() {
                        tag.trimdown();
                    }
                    if let Some(state) = transfer.0.state.as_ref() {
                        transfer.0.state = Some(detach(state));
                    }

                    let delivery = Delivery::new_rcv(
                        id,
                        self.handle,
                        transfer.delivery_tag().cloned().unwrap_or_else(Bytes::new),
                        transfer.settled().unwrap_or_default(),
                        self.session.clone(),
                    );
                    self.queue.push_back((delivery, transfer));
                    Action::None
                } else {
                    let err = Error(Box::new(codec::ErrorInner {
                        condition: LinkError::DetachForced.into(),
                        description: Some(ByteString::from_static("delivery_id is required")),
                        info: None,
                    }));
                    let _ = self.close(Some(err));
                    Action::None
                }
            } else if let Some(id) = transfer.delivery_id() {
                if size_exceeded(self.max_message_size, body_len(&transfer)) {
                    return self.close_size_exceeded();
                }

                self.delivery_count = self.delivery_count.wrapping_add(1);
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
                Action::Transfer(ReceiverLink {
                    inner: inner.clone(),
                })
            } else {
                let err = Error(Box::new(codec::ErrorInner {
                    condition: LinkError::DetachForced.into(),
                    description: Some(ByteString::from_static("delivery_id is required")),
                    info: None,
                }));
                let _ = self.close(Some(err));
                Action::None
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
        self.0.inner.get_mut().local_detached();
    }
}

pub struct ReceiverLinkBuilder {
    frame: Attach,
    session: Cell<SessionInner>,
    timeout: Seconds,
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

        let timeout = session.get_ref().link_attach_timeout();
        ReceiverLinkBuilder {
            frame,
            session,
            timeout,
        }
    }

    /// Set max message size
    ///
    /// Larger messages detach the link with `message-size-exceeded` error.
    /// If max size is set to `0`, size is unlimited.
    #[must_use]
    pub fn max_message_size(mut self, size: u64) -> Self {
        self.frame.0.max_message_size = Some(size);
        self
    }

    /// Set or reset a receive link property
    #[must_use]
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

    /// Set link capabilities
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn capabilities(mut self, caps: Symbols) -> Self {
        self.frame.source_mut().as_mut().unwrap().capabilities = Some(caps);
        self
    }

    /// Set link attach timeout
    ///
    /// By default connection's link attach timeout is used.
    /// Use `Seconds::ZERO` to disable timeout.
    #[must_use]
    pub fn attach_timeout(mut self, timeout: Seconds) -> Self {
        self.timeout = timeout;
        self
    }

    /// Modify attach frame
    #[must_use]
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
        let rx = self
            .session
            .get_mut()
            .attach_local_receiver_link(cell, self.frame);
        timeout_checked(self.timeout, rx.recv())
            .await
            .map_err(|()| AmqpProtocolError::LinkAttachTimeout)?
    }
}

/// Default max message size of remotely attached receiver links
pub(crate) const DEFAULT_MAX_MESSAGE_SIZE: u64 = 262_144;

/// Check message size against max message size, `0` means unlimited
fn size_exceeded(max_message_size: u64, size: usize) -> bool {
    max_message_size != 0 && size as u64 > max_message_size
}

fn body_len(transfer: &Transfer) -> usize {
    transfer.body().map_or(0, TransferBody::len)
}

/// Max size of transfer data copied into the message body
const BODY_COPY_LIMIT: usize = 4096;

/// Append partial transfer data to the message body
///
/// Data is a slice of the read buffer and keeps the whole buffer alive, small
/// data is copied, so a message cannot retain many read buffers.
fn append_body(body: &mut BytePages, data: TransferBody) {
    match data {
        TransferBody::Data(data) if data.len() <= BODY_COPY_LIMIT => body.extend_from_slice(&data),
        data => data.encode(body),
    }
}
