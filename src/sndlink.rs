use std::task::{Context, Poll, Waker, ready};
use std::{collections::VecDeque, future::Future, future::poll_fn, pin::Pin};

use ntex_amqp_codec::protocol::{
    self as codec, Attach, DeliveryNumber, Error, Flow, MessageFormat, ReceiverSettleMode, Role,
    SenderSettleMode, SequenceNo, Target, TerminusDurability, TerminusExpiryPolicy, TransferBody,
};
use ntex_bytes::{BufMut, ByteString, Bytes};
use ntex_util::channel::{condition, pool};
use ntex_util::time::{Seconds, timeout_checked};

use crate::delivery::TransferBuilder;
use crate::session::{Session, SessionInner, WindowClaim, WindowWaiter};
use crate::{Handle, cell::Cell, error::AmqpProtocolError};

#[derive(Clone)]
pub struct SenderLink {
    pub(crate) inner: Cell<SenderLinkInner>,
}

pub(crate) struct SenderLinkInner {
    pub(crate) id: usize,
    name: ByteString,
    address: Option<ByteString>,
    pub(crate) session: Session,
    remote_handle: Handle,
    delivery_count: SequenceNo,
    // used if receiver has not received attach yet
    initial_delivery_count: SequenceNo,
    delivery_tag: u32,
    link_credit: u32,
    pending_transfers: VecDeque<pool::Sender<Result<(), AmqpProtocolError>>>,
    // woken credit waiters and in-flight transfers
    active: u32,
    // active transfers waiting for session window
    window_waiters: u32,
    // delivery is partially sent, frames of deliveries must not interleave
    partial: bool,
    // receiver drain mode
    drain: bool,
    pub(crate) error: Option<AmqpProtocolError>,
    pub(crate) closed: bool,
    pub(crate) max_message_size: Option<u32>,
    on_close: condition::Condition,
    on_credit: condition::Condition,
}

impl std::fmt::Debug for SenderLink {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_tuple("SenderLink")
            .field(&self.inner.get_ref().name)
            .finish()
    }
}

impl std::fmt::Debug for SenderLinkInner {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_tuple("SenderLinkInner")
            .field(&&*self.name)
            .finish()
    }
}

impl SenderLink {
    pub(crate) fn new(inner: Cell<SenderLinkInner>) -> SenderLink {
        SenderLink { inner }
    }

    /// Id of the sender link
    #[inline]
    pub fn id(&self) -> u32 {
        self.inner.get_ref().id()
    }

    /// Name of the sender link
    #[inline]
    pub fn name(&self) -> &ByteString {
        &self.inner.get_ref().name
    }

    /// Address of the node the link is attached to
    ///
    /// Source address for remotely opened links, target address for locally opened links.
    #[inline]
    pub fn address(&self) -> Option<&ByteString> {
        self.inner.get_ref().address.as_ref()
    }

    /// Remote handle
    #[inline]
    pub fn remote_handle(&self) -> Handle {
        self.inner.get_ref().remote_handle
    }

    /// Reference to session
    #[inline]
    pub fn session(&self) -> &Session {
        &self.inner.get_ref().session
    }

    /// Returns available send credit
    #[inline]
    pub fn credit(&self) -> u32 {
        self.inner.get_ref().link_credit
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Packet could be sent if link has credit and remote session
    /// incoming window is available. Result indicates if link is alive
    pub async fn ready(&self) -> bool {
        loop {
            let (credit, window) = {
                let inner = self.inner.get_ref();
                if inner.closed {
                    return false;
                }
                // closed link notifies credit waiters
                let credit = inner.on_credit.wait();
                if inner.link_credit == 0 {
                    (credit, None)
                } else {
                    let session = inner.session.inner.get_ref();
                    if session.has_window() {
                        return true;
                    }
                    (credit, Some(session.on_window.wait()))
                }
            };
            poll_fn(|cx| {
                if credit.poll_ready(cx).is_ready()
                    || window.as_ref().is_some_and(|w| w.poll_ready(cx).is_ready())
                {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
    }

    /// Check if link is closed
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.get_ref().closed
    }

    /// Check if link is opened
    #[inline]
    pub fn is_opened(&self) -> bool {
        !self.is_closed()
    }

    /// Link error
    pub fn error(&self) -> Option<&AmqpProtocolError> {
        self.inner.get_ref().error.as_ref()
    }

    /// Start delivery process
    #[doc(hidden)]
    #[deprecated]
    pub fn delivery<T>(&self, body: T) -> TransferBuilder
    where
        T: Into<TransferBody>,
    {
        self.transfer(body)
    }

    /// Start delivery process
    pub fn transfer<T>(&self, body: T) -> TransferBuilder
    where
        T: Into<TransferBody>,
    {
        TransferBuilder::new(body.into(), self.inner.clone())
    }

    /// Close sender link
    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        self.inner.get_mut().close(None)
    }

    /// Close sender link with error
    pub fn close_with_error<E>(
        &self,
        error: E,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>>
    where
        Error: From<E>,
    {
        self.inner.get_mut().close(Some(error.into()))
    }

    /// Notify when link is closed
    pub fn on_close(&self) -> condition::Waiter {
        self.inner.get_ref().on_close.wait()
    }

    /// Notify when credit get updated
    ///
    /// After notification credit must be checked again,
    /// other waiters could consume it.
    pub fn on_credit_update(&self) -> condition::Waiter {
        self.inner.get_ref().on_credit.wait()
    }

    /// Max message size, `None` means unlimited
    pub fn max_message_size(&self) -> Option<u32> {
        self.inner.get_ref().max_message_size
    }

    /// Set max message size.
    ///
    /// Sending larger messages fails with `AmqpProtocolError::BodyTooLarge`.
    /// If max size is set to `0`, size is unlimited.
    pub fn set_max_message_size(&self, value: u32) {
        self.inner.get_mut().max_message_size = (value != 0).then_some(value);
    }
}

impl SenderLinkInner {
    pub(crate) fn new(
        id: usize,
        name: ByteString,
        address: Option<ByteString>,
        handle: Handle,
        delivery_count: SequenceNo,
        session: Cell<SessionInner>,
        max_message_size: Option<u32>,
    ) -> SenderLinkInner {
        SenderLinkInner {
            id,
            name,
            address,
            delivery_count,
            initial_delivery_count: delivery_count,
            max_message_size,
            session: Session::new(session),
            remote_handle: handle,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            active: 0,
            window_waiters: 0,
            partial: false,
            drain: false,
            error: None,
            closed: false,
            delivery_tag: 0,
            on_close: condition::Condition::new(),
            on_credit: condition::Condition::new(),
        }
    }

    pub(crate) fn with(id: usize, frame: &Attach, session: Cell<SessionInner>) -> SenderLinkInner {
        let mut name = frame.name().clone();
        name.trimdown();
        let address = frame
            .source()
            .and_then(|s| s.address.clone())
            .map(|mut addr| {
                addr.trimdown();
                addr
            });

        let delivery_count = frame.initial_delivery_count().unwrap_or(0);
        SenderLinkInner::new(
            id,
            name,
            address,
            frame.handle(),
            delivery_count,
            session,
            remote_max_message_size(frame),
        )
    }

    pub(crate) fn id(&self) -> u32 {
        self.id as u32
    }

    #[cfg(test)]
    pub(crate) fn pending_transfers(&self) -> usize {
        self.pending_transfers.len()
    }

    pub(crate) fn remote_detached(&mut self, err: AmqpProtocolError) {
        log::trace!(
            "{}: Detaching sender link {:?} with error {:?}",
            self.session.tag(),
            self.name,
            err
        );

        // drop pending transfers
        for tx in self.pending_transfers.drain(..) {
            let _ = tx.send(Err(err.clone()));
        }

        self.closed = true;
        self.error = Some(err);
        self.on_close.notify_and_lock(());
        self.on_credit.notify_and_lock(());
    }

    /// Link is detached locally, fail transfers waiting for link credit
    pub(crate) fn local_detached(&mut self, err: &AmqpProtocolError) {
        for tx in self.pending_transfers.drain(..) {
            let _ = tx.send(Err(err.clone()));
        }
        if !self.closed {
            self.closed = true;
            self.on_close.notify_and_lock(());
            self.on_credit.notify_and_lock(());
        }
    }

    pub(crate) async fn close(&mut self, error: Option<Error>) -> Result<(), AmqpProtocolError> {
        if self.closed {
            Ok(())
        } else {
            // fail transfers waiting for link credit or session window
            let err = AmqpProtocolError::Disconnected;
            self.local_detached(&err);
            let session = self.session.inner.get_mut();
            session.drop_link_transfers(self.id as Handle, &err);
            session
                .detach_sender_link(self.id as Handle, true, error)
                .await
        }
    }

    /// Link flow state `(handle, delivery-count, link-credit, drain)`
    pub(crate) fn flow_state(&self) -> (Handle, SequenceNo, u32, bool) {
        (
            self.id as Handle,
            self.delivery_count,
            self.link_credit,
            self.drain,
        )
    }

    /// Apply remote flow, returns `true` if link credit is drained
    pub(crate) fn apply_flow(&mut self, flow: &Flow) -> bool {
        // #2.7.6
        if let Some(credit) = flow.link_credit() {
            // delivery-count is null if receiver has not received attach yet
            let rcv_delivery_count = flow.delivery_count().unwrap_or(self.initial_delivery_count);

            // deliveries sent after flow was issued consume its credit
            let in_flight = self.delivery_count.wrapping_sub(rcv_delivery_count);
            let new_credit = if in_flight > i32::MAX as u32 {
                log::warn!(
                    "{}: Sender link {:?} flow delivery count {:?} is ahead of local delivery count {:?}",
                    self.session.tag(),
                    self.name,
                    rcv_delivery_count,
                    self.delivery_count
                );
                credit
            } else {
                credit.saturating_sub(in_flight)
            };

            log::trace!(
                "{}: Apply sender link {:?} flow, credit: {:?}, delivery count: {:?}, local delivery count: {:?}, pending: {:?}, old credit {:?}",
                self.session.tag(),
                self.name,
                new_credit,
                rcv_delivery_count,
                self.delivery_count,
                self.pending_transfers.len(),
                self.link_credit
            );

            self.link_credit = new_credit;
            self.drain = flow.drain();

            // credit became available => wake up pending transfers
            self.wake_pending();

            // notify available credit waiters
            if self.link_credit > 0 {
                self.on_credit.notify(());
            }
            self.drain_credit()
        } else {
            false
        }
    }

    /// Wake up transfers waiting for link credit
    ///
    /// Number of woken transfers is limited by credit not claimed by active transfers
    fn wake_pending(&mut self) {
        if self.partial {
            return;
        }
        let mut available = self.link_credit.saturating_sub(self.active);
        while available > 0
            && let Some(tx) = self.pending_transfers.pop_front()
        {
            if tx.send(Ok(())).is_ok() {
                self.active += 1;
                available -= 1;
            }
        }
    }

    /// Consume remaining credit if receiver requested drain and no transfer
    /// can be sent right away
    ///
    /// Transfers blocked by remote session window do not delay drain, they wait
    /// for new credit afterwards. Queued transfers are blocked either by such
    /// transfers or by partial delivery, which waits for session window as well.
    ///
    /// AMQP 1.0 2.6.7, returns `true` if link credit is drained
    fn drain_credit(&mut self) -> bool {
        if self.drain && self.link_credit > 0 && self.active == self.window_waiters && !self.closed
        {
            log::trace!(
                "{}: Drain sender link {:?} credit {:?}",
                self.session.tag(),
                self.name,
                self.link_credit
            );
            self.delivery_count = self.delivery_count.wrapping_add(self.link_credit);
            self.link_credit = 0;
            true
        } else {
            false
        }
    }

    /// Drain link credit and send link state to the receiver
    fn drain_and_post(&mut self) {
        if self.drain_credit() {
            let state = self.flow_state();
            self.session.inner.get_mut().post_flow(Some(state));
        }
    }

    pub(crate) async fn send<T: Into<TransferBody>>(
        link: &Cell<SenderLinkInner>,
        body: T,
        tag: Option<Bytes>,
        settled: bool,
        format: Option<MessageFormat>,
    ) -> Result<(DeliveryNumber, Bytes), AmqpProtocolError> {
        let inner = link.get_mut();
        if let Some(ref err) = inner.error {
            return Err(err.clone());
        }
        let body = body.into();
        let tag = inner.get_tag(tag);

        // woken transfer claims credit ahead of queued transfers
        let mut woken = false;
        // woken transfer claims session window ahead of queued transfers
        let mut claim: Option<WindowClaim> = None;
        loop {
            let inner = link.get_mut();
            if let Some(ref err) = inner.error {
                return Err(err.clone());
            } else if inner.closed {
                return Err(AmqpProtocolError::Disconnected);
            }
            // credit claimed by woken transfers is not available
            if inner.link_credit <= inner.active
                || inner.partial
                || (!woken && !inner.pending_transfers.is_empty())
            {
                // claimed window is released for other transfers
                drop(claim.take());
                log::trace!(
                    "{}: Sender link credit is 0({:?}), push to pending queue hnd:{}({} -> {}), queue size: {}",
                    inner.session.tag(),
                    inner.link_credit,
                    inner.name,
                    inner.id,
                    inner.remote_handle,
                    inner.pending_transfers.len()
                );
                let (tx, rx) = inner.session.inner.get_ref().pool_credit.channel();
                inner.pending_transfers.push_back(tx);
                CreditWaiter {
                    rx: Some(rx),
                    link: link.clone(),
                }
                .await?;
                woken = true;
                continue;
            }

            // waiting transfer claims link credit, but it does not delay credit drain
            let handle = inner.id as Handle;
            let session = inner.session.inner.get_mut();
            if let Some(rx) = session.window_waiter(handle, claim.is_some())? {
                drop(claim.take());
                inner.active += 1;
                inner.window_waiters += 1;
                // woken transfer could delay drain, it is blocked now
                inner.drain_and_post();
                let guard = ActiveTransfer(Some(link.clone()));
                claim = Some(WindowWaiter::new(rx, inner.session.inner.clone()).await?);
                guard.resume();
                woken = true;
                continue;
            }
            break;
        }

        // transfer is sent and link credit is consumed together
        let inner = link.get_mut();
        let handle = inner.id as Handle;
        let (id, chunks) = inner
            .session
            .inner
            .get_mut()
            .send_transfer(handle, &tag, body, settled, format)?;
        if let Some(claim) = claim.take() {
            claim.consume();
        }
        inner.link_credit -= 1;
        inner.delivery_count = inner.delivery_count.wrapping_add(1);
        inner.drain_and_post();

        // body does not fit into one frame, each frame consumes session window
        if let Some(mut chunks) = chunks {
            inner.partial = true;
            let guard = PartialDelivery(Some((link.clone(), id)));
            loop {
                let inner = link.get_mut();
                if let Some(ref err) = inner.error {
                    return Err(err.clone());
                } else if inner.closed {
                    return Err(AmqpProtocolError::Disconnected);
                }
                let session = inner.session.inner.get_mut();
                if let Some(rx) = session.window_waiter(handle, claim.is_some())? {
                    drop(claim.take());
                    claim = Some(WindowWaiter::new(rx, inner.session.inner.clone()).await?);
                } else {
                    let last = session.send_transfer_chunk(handle, &mut chunks);
                    if let Some(claim) = claim.take() {
                        claim.consume();
                    }
                    if last {
                        break;
                    }
                }
            }
            guard.complete();
        }

        Ok((id, tag))
    }

    /// Delivery is sent or cancelled
    fn end_delivery(&mut self, aborted: Option<DeliveryNumber>) {
        self.partial = false;
        if let Some(id) = aborted {
            let handle = self.id as Handle;
            let session = self.session.inner.get_mut();
            session.unsettled_snd_deliveries.remove(&id);
            if !self.closed {
                session.abort_transfer(handle, id);
            }
        }
        self.wake_pending();
        self.drain_and_post();
    }

    fn get_tag(&mut self, tag: Option<Bytes>) -> Bytes {
        tag.unwrap_or_else(|| {
            let delivery_tag = self.delivery_tag;
            self.delivery_tag = delivery_tag.wrapping_add(1);

            let mut buf = self
                .session
                .connection()
                .config()
                .read_buf()
                .buf_with_capacity(16);
            buf.put_u32(delivery_tag);
            buf.freeze()
        })
    }
}

/// Waits for link credit
///
/// Woken waiter is counted as active until it resumes or gets dropped,
/// cancelled waiter leaves the queue
struct CreditWaiter {
    // `None` after completion
    rx: Option<pool::Receiver<Result<(), AmqpProtocolError>>>,
    link: Cell<SenderLinkInner>,
}

impl Future for CreditWaiter {
    type Output = Result<(), AmqpProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(rx) = self.rx.as_ref() else {
            return Poll::Ready(Err(AmqpProtocolError::ConnectionDropped));
        };
        let result = ready!(rx.poll_recv(cx));
        self.rx = None;
        Poll::Ready(match result {
            Ok(Ok(())) => {
                self.link.get_mut().active -= 1;
                Ok(())
            }
            Ok(Err(err)) => Err(err),
            Err(_) => Err(AmqpProtocolError::ConnectionDropped),
        })
    }
}

impl Drop for CreditWaiter {
    fn drop(&mut self) {
        if let Some(rx) = self.rx.take() {
            let link = self.link.get_mut();
            match rx.poll_recv(&mut Context::from_waker(Waker::noop())) {
                // waiter is dropped after wake up
                Poll::Ready(Ok(Ok(()))) => {
                    link.active -= 1;
                    link.wake_pending();
                    link.drain_and_post();
                }
                // waiter is still queued
                Poll::Pending => {
                    drop(rx);
                    link.pending_transfers.retain(|tx| !tx.is_canceled());
                }
                Poll::Ready(_) => (),
            }
        }
    }
}

/// Transfer waits for session window, claimed credit is released after it is gone
struct ActiveTransfer(Option<Cell<SenderLinkInner>>);

impl ActiveTransfer {
    /// Transfer continues without awaiting
    fn resume(mut self) {
        if let Some(link) = self.0.take() {
            let link = link.get_mut();
            link.active -= 1;
            link.window_waiters -= 1;
        }
    }
}

impl Drop for ActiveTransfer {
    fn drop(&mut self) {
        if let Some(link) = self.0.take() {
            let link = link.get_mut();
            link.active -= 1;
            link.window_waiters -= 1;
            link.wake_pending();
            link.drain_and_post();
        }
    }
}

/// Partially sent delivery, cancelled delivery gets aborted
struct PartialDelivery(Option<(Cell<SenderLinkInner>, DeliveryNumber)>);

impl PartialDelivery {
    fn complete(mut self) {
        if let Some((link, _)) = self.0.take() {
            link.get_mut().end_delivery(None);
        }
    }
}

impl Drop for PartialDelivery {
    fn drop(&mut self) {
        if let Some((link, id)) = self.0.take() {
            link.get_mut().end_delivery(Some(id));
        }
    }
}

/// Max message size of remote link endpoint, `0` or unset means unlimited
pub(crate) fn remote_max_message_size(attach: &Attach) -> Option<u32> {
    match attach.max_message_size() {
        None | Some(0) => None,
        Some(size) => Some(u32::try_from(size).unwrap_or(u32::MAX)),
    }
}

#[derive(Debug)]
pub(crate) struct EstablishedSenderLink(SenderLink);

impl EstablishedSenderLink {
    pub(crate) fn new(inner: Cell<SenderLinkInner>) -> EstablishedSenderLink {
        EstablishedSenderLink(SenderLink::new(inner))
    }
}

impl std::ops::Deref for EstablishedSenderLink {
    type Target = SenderLink;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for EstablishedSenderLink {
    fn drop(&mut self) {
        self.0
            .inner
            .get_mut()
            .local_detached(&AmqpProtocolError::Disconnected);
    }
}

pub struct SenderLinkBuilder {
    frame: Attach,
    session: Cell<SessionInner>,
    timeout: Seconds,
}

impl SenderLinkBuilder {
    pub(crate) fn new(name: ByteString, address: ByteString, session: Cell<SessionInner>) -> Self {
        let target = Target {
            address: Some(address),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            dynamic: false,
            dynamic_node_properties: None,
            capabilities: None,
        };
        let frame = Attach(Box::new(codec::AttachInner {
            name,
            handle: 0_u32,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: Some(target),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            max_message_size: Some(65536 * 4),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }));

        let timeout = session.get_ref().link_attach_timeout();
        SenderLinkBuilder {
            frame,
            session,
            timeout,
        }
    }

    /// Set max message size
    #[must_use]
    pub fn max_message_size(mut self, size: u64) -> Self {
        self.frame.0.max_message_size = Some(size);
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

    /// Initiate attach sender process
    pub async fn attach(self) -> Result<SenderLink, AmqpProtocolError> {
        let rx = self.session.get_mut().attach_local_sender_link(self.frame);
        let inner = timeout_checked(self.timeout, rx.recv())
            .await
            .map_err(|()| AmqpProtocolError::LinkAttachTimeout)??;
        Ok(SenderLink::new(inner))
    }
}
