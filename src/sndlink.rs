use std::{collections::VecDeque, future::Future};

use ntex_amqp_codec::protocol::{
    self as codec, Attach, DeliveryNumber, Error, Flow, MessageFormat, ReceiverSettleMode, Role,
    SenderSettleMode, SequenceNo, Target, TerminusDurability, TerminusExpiryPolicy, TransferBody,
};
use ntex_bytes::{BufMut, ByteString, Bytes};
use ntex_util::channel::{condition, oneshot, pool};
use ntex_util::future::{Either, Ready};

use crate::delivery::TransferBuilder;
use crate::session::{Session, SessionInner};
use crate::{Handle, cell::Cell, error::AmqpProtocolError};

#[derive(Clone)]
pub struct SenderLink {
    pub(crate) inner: Cell<SenderLinkInner>,
}

pub(crate) struct SenderLinkInner {
    pub(crate) id: usize,
    name: ByteString,
    pub(crate) session: Session,
    remote_handle: Handle,
    delivery_count: SequenceNo,
    delivery_tag: u32,
    link_credit: u32,
    pending_transfers: VecDeque<pool::Sender<Result<(), AmqpProtocolError>>>,
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

    #[inline]
    /// Id of the sender link
    pub fn id(&self) -> u32 {
        self.inner.id as u32
    }

    #[inline]
    /// Name of the sender link
    pub fn name(&self) -> &ByteString {
        &self.inner.name
    }

    #[inline]
    /// Remote handle
    pub fn remote_handle(&self) -> Handle {
        self.inner.remote_handle
    }

    #[inline]
    /// Reference to session
    pub fn session(&self) -> &Session {
        &self.inner.get_ref().session
    }

    #[inline]
    /// Returns available send credit
    pub fn credit(&self) -> u32 {
        self.inner.get_ref().link_credit
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive
    pub async fn ready(&self) -> bool {
        loop {
            let waiter = {
                let inner = self.inner.get_ref();
                if inner.closed {
                    return false;
                }
                if inner.link_credit > 0 {
                    return true;
                }
                inner.on_credit.wait()
            };
            waiter.await;
        }
    }

    #[inline]
    /// Check is link is closed
    pub fn is_closed(&self) -> bool {
        self.inner.closed
    }

    #[inline]
    /// Check link state
    pub fn is_opened(&self) -> bool {
        !self.inner.closed
    }

    /// Check link error
    pub fn error(&self) -> Option<&AmqpProtocolError> {
        self.inner.get_ref().error.as_ref()
    }

    #[doc(hidden)]
    #[deprecated]
    /// Start delivery process
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

    pub fn max_message_size(&self) -> Option<u32> {
        self.inner.get_ref().max_message_size
    }

    pub fn set_max_message_size(&self, value: u32) {
        self.inner.get_mut().max_message_size = Some(value);
    }
}

impl SenderLinkInner {
    pub(crate) fn new(
        id: usize,
        name: ByteString,
        handle: Handle,
        delivery_count: SequenceNo,
        session: Cell<SessionInner>,
        max_message_size: Option<u32>,
    ) -> SenderLinkInner {
        SenderLinkInner {
            id,
            name,
            delivery_count,
            max_message_size,
            session: Session::new(session),
            remote_handle: handle,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            error: None,
            closed: false,
            delivery_tag: 0,
            on_close: condition::Condition::new(),
            on_credit: condition::Condition::new(),
        }
    }

    pub(crate) fn with(id: usize, frame: &Attach, session: Cell<SessionInner>) -> SenderLinkInner {
        let mut name = None;
        if let Some(source) = frame.source()
            && let Some(ref addr) = source.address
        {
            name = Some(addr.clone());
        }
        let mut name = name.unwrap_or_default();
        name.trimdown();

        let delivery_count = frame.initial_delivery_count().unwrap_or(0);
        let max_message_size = frame
            .max_message_size()
            .map(|size| u32::try_from(size).unwrap_or(u32::MAX));

        SenderLinkInner::new(
            id,
            name,
            frame.handle(),
            delivery_count,
            session,
            max_message_size,
        )
    }

    pub(crate) fn id(&self) -> u32 {
        self.id as u32
    }

    pub(crate) fn remote_handle(&self) -> Handle {
        self.remote_handle
    }

    pub(crate) fn name(&self) -> &ByteString {
        &self.name
    }

    pub(crate) fn max_message_size(&self) -> Option<u32> {
        self.max_message_size
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
        self.on_close.notify_and_lock_readiness();
        self.on_credit.notify_and_lock_readiness();
    }

    pub(crate) fn close(
        &mut self,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        if self.closed {
            Either::Left(Ready::Ok(()))
        } else {
            self.closed = true;
            self.on_close.notify_and_lock_readiness();
            self.on_credit.notify_and_lock_readiness();

            let (tx, rx) = oneshot::channel();

            self.session
                .inner
                .get_mut()
                .detach_sender_link(self.id as Handle, true, error, tx);

            Either::Right(async move {
                match rx.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(AmqpProtocolError::Disconnected),
                }
            })
        }
    }

    pub(crate) fn apply_flow(&mut self, flow: &Flow) {
        // #2.7.6
        if let Some(credit) = flow.link_credit() {
            let new_credit = flow
                .delivery_count()
                .unwrap_or(0)
                .wrapping_add(credit)
                .wrapping_sub(self.delivery_count);

            log::trace!(
                "{}: Apply sender link {:?} flow, credit: {:?}, delivery count: {:?}, local delivery count: {:?}, pending: {:?}, old credit {:?}",
                self.session.tag(),
                self.name,
                new_credit,
                flow.delivery_count().unwrap_or(0),
                self.delivery_count,
                self.pending_transfers.len(),
                self.link_credit
            );

            self.link_credit = new_credit;

            // credit became available => drain pending_transfers
            while let Some(tx) = self.pending_transfers.pop_front() {
                let _ = tx.send(Ok(()));
            }

            // notify available credit waiters
            if self.link_credit > 0 {
                self.on_credit.notify();
            }
        }
    }

    pub(crate) async fn send<T: Into<TransferBody>>(
        &mut self,
        body: T,
        tag: Option<Bytes>,
        settled: bool,
        format: Option<MessageFormat>,
    ) -> Result<(DeliveryNumber, Bytes), AmqpProtocolError> {
        if let Some(ref err) = self.error {
            Err(err.clone())
        } else {
            let body = body.into();
            let tag = self.get_tag(tag);

            loop {
                if self.link_credit == 0 || !self.pending_transfers.is_empty() {
                    log::trace!(
                        "{}: Sender link credit is 0({:?}), push to pending queue hnd:{}({} -> {}), queue size: {}",
                        self.session.tag(),
                        self.link_credit,
                        self.name,
                        self.id,
                        self.remote_handle,
                        self.pending_transfers.len()
                    );
                    let (tx, rx) = self.session.inner.get_ref().pool_credit.channel();
                    self.pending_transfers.push_back(tx);
                    rx.await
                        .map_err(|_| AmqpProtocolError::ConnectionDropped)
                        .and_then(|v| v)?;
                    continue;
                }
                break;
            }

            // reduce link credit
            self.link_credit -= 1;
            self.delivery_count = self.delivery_count.wrapping_add(1);
            let id = self
                .session
                .inner
                .get_mut()
                .send_transfer(self.id as u32, tag.clone(), body, settled, format)
                .await?;

            Ok((id, tag))
        }
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
        let inner = self.0.inner.get_mut();
        if !inner.closed {
            inner.closed = true;
            inner.on_close.notify_and_lock_readiness();
            inner.on_credit.notify_and_lock_readiness();
        }
    }
}

pub struct SenderLinkBuilder {
    frame: Attach,
    session: Cell<SessionInner>,
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
            initial_delivery_count: None,
            max_message_size: Some(65536 * 4),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }));

        SenderLinkBuilder { frame, session }
    }

    #[must_use]
    /// Set max message size
    pub fn max_message_size(mut self, size: u64) -> Self {
        self.frame.0.max_message_size = Some(size);
        self
    }

    #[must_use]
    /// Modify attach frame
    pub fn with_frame<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Attach),
    {
        f(&mut self.frame);
        self
    }

    /// Initiate attach sender process
    pub async fn attach(self) -> Result<SenderLink, AmqpProtocolError> {
        let result = self
            .session
            .get_mut()
            .attach_local_sender_link(self.frame)
            .await;

        match result {
            Ok(Ok(inner)) => Ok(SenderLink { inner }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AmqpProtocolError::Disconnected),
        }
    }
}
