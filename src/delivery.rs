use std::cell::Cell as StdCell;

use ntex_amqp_codec::protocol::{
    DeliveryNumber, DeliveryState, Disposition, DispositionInner, Error, ErrorCondition, Handle,
    MessageFormat, Rejected, Role, TransferBody,
};
use ntex_amqp_codec::types::{Str, Symbol};
use ntex_bytes::Bytes;
use ntex_util::channel::pool;

use crate::{cell::Cell, error::AmqpProtocolError, session::Session, sndlink::SenderLinkInner};

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug)]
    struct Flags: u8 {
        const SENDER         = 0b0000_0001;
        const LOCAL_SETTLED  = 0b0000_0100;
        const REMOTE_SETTLED = 0b0000_1000;
    }
}

#[derive(Debug)]
pub struct Delivery {
    id: DeliveryNumber,
    tag: Bytes,
    session: Session,
    flags: StdCell<Flags>,
}

#[derive(Default, Debug)]
pub(crate) struct DeliveryInner {
    handle: Handle,
    settled: bool,
    state: Option<DeliveryState>,
    error: Option<AmqpProtocolError>,
    tx: Option<pool::Sender<()>>,
    // concurrent `wait()` calls
    waiters: Vec<pool::Sender<()>>,
}

impl Delivery {
    pub(crate) fn new_rcv(
        id: DeliveryNumber,
        link_handle: Handle,
        tag: Bytes,
        settled: bool,
        session: Session,
    ) -> Delivery {
        if !settled {
            session
                .inner
                .get_mut()
                .unsettled_rcv_deliveries
                .insert(id, DeliveryInner::new(link_handle));
        }

        Delivery {
            id,
            tag,
            session,
            flags: StdCell::new(if settled {
                Flags::LOCAL_SETTLED
            } else {
                Flags::empty()
            }),
        }
    }

    /// Drop delivery without sending disposition
    pub(crate) fn discard(self) {
        self.session
            .inner
            .get_mut()
            .unsettled_deliveries(self.is_set(Flags::SENDER))
            .remove(&self.id);
    }

    pub fn id(&self) -> DeliveryNumber {
        self.id
    }

    pub fn tag(&self) -> &Bytes {
        &self.tag
    }

    pub fn remote_state(&self) -> Option<DeliveryState> {
        if let Some(inner) = self
            .session
            .inner
            .get_mut()
            .unsettled_deliveries(self.is_set(Flags::SENDER))
            .get_mut(&self.id)
        {
            inner.state.clone()
        } else {
            None
        }
    }

    pub fn is_remote_settled(&self) -> bool {
        self.remote_settled()
    }

    pub fn settle(&mut self, state: DeliveryState) {
        // remote side is settled, not need to send disposition
        if self.remote_settled() {
            return;
        }

        if !self.is_set(Flags::LOCAL_SETTLED) {
            self.set_flag(Flags::LOCAL_SETTLED);

            let disp = Disposition(Box::new(DispositionInner {
                role: if self.is_set(Flags::SENDER) {
                    Role::Sender
                } else {
                    Role::Receiver
                },
                first: self.id,
                last: None,
                settled: true,
                state: Some(state),
                batchable: false,
            }));
            self.session.inner.get_mut().post_frame(disp.into());
        }
    }

    pub fn update_state(&mut self, state: DeliveryState) {
        // remote side is settled, not need to send disposition
        if self.is_set(Flags::LOCAL_SETTLED) || self.remote_settled() {
            return;
        }

        let disp = Disposition(Box::new(DispositionInner {
            role: if self.is_set(Flags::SENDER) {
                Role::Sender
            } else {
                Role::Receiver
            },
            first: self.id,
            last: None,
            settled: false,
            state: Some(state),
            batchable: false,
        }));
        self.session.inner.get_mut().post_frame(disp.into());
    }

    fn is_set(&self, flag: Flags) -> bool {
        self.flags.get().contains(flag)
    }

    fn set_flag(&self, flag: Flags) {
        let mut flags = self.flags.get();
        flags.insert(flag);
        self.flags.set(flags);
    }

    /// Check if remote side settled delivery, disposition could arrive before `wait()` call
    fn remote_settled(&self) -> bool {
        if !self.is_set(Flags::REMOTE_SETTLED)
            && self
                .session
                .inner
                .get_mut()
                .unsettled_deliveries(self.is_set(Flags::SENDER))
                .get(&self.id)
                .is_some_and(|inner| inner.settled)
        {
            self.set_flag(Flags::REMOTE_SETTLED);
        }
        self.is_set(Flags::REMOTE_SETTLED)
    }

    /// Wait for delivery outcome
    ///
    /// Resolves when remote side settles delivery or sends terminal outcome,
    /// non-terminal `Received` state is not returned. Returns `Ok(None)` if delivery
    /// is settled locally or remote side settled it without outcome.
    pub async fn wait(&self) -> Result<Option<DeliveryState>, AmqpProtocolError> {
        if self.flags.get().contains(Flags::LOCAL_SETTLED) {
            log::debug!("Delivery {:?} is settled locally", self.id);
            return Ok(None);
        }

        loop {
            let Some(inner) = self
                .session
                .inner
                .get_mut()
                .unsettled_deliveries(self.is_set(Flags::SENDER))
                .get_mut(&self.id)
            else {
                return Err(self.session_error());
            };
            if inner.settled {
                self.set_flag(Flags::REMOTE_SETTLED);
            }
            if let Some(res) = inner.outcome() {
                return res;
            }

            let (tx, rx) = self.session.inner.get_ref().pool_notify.channel();
            inner.add_waiter(tx);
            let _ = rx.await;
        }
    }

    /// Unsettled deliveries are dropped on session end, return session error
    fn session_error(&self) -> AmqpProtocolError {
        self.session
            .inner
            .get_ref()
            .error()
            .cloned()
            .unwrap_or(AmqpProtocolError::LinkDetached(None))
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        let inner = self.session.inner.get_mut();
        let deliveries = inner.unsettled_deliveries(self.is_set(Flags::SENDER));

        if let Some(delivery) = deliveries.remove(&self.id)
            && !delivery.settled
            && !self.is_set(Flags::REMOTE_SETTLED)
            && !self.is_set(Flags::LOCAL_SETTLED)
        {
            let (role, state) = if self.is_set(Flags::SENDER) {
                // settle with remote outcome, outcome is decided by receiver
                let state = delivery
                    .state
                    .clone()
                    .filter(|st| !matches!(st, DeliveryState::Received(_)));
                (Role::Sender, state)
            } else {
                let err = Error::build()
                    .condition(ErrorCondition::Custom(Symbol(Str::from_static(
                        "Internal error",
                    ))))
                    .finish();
                (
                    Role::Receiver,
                    Some(DeliveryState::Rejected(Rejected { error: Some(err) })),
                )
            };

            let disp = Disposition(Box::new(DispositionInner {
                role,
                first: self.id,
                last: None,
                settled: true,
                state,
                batchable: false,
            }));
            inner.post_frame(disp.into());
        }
    }
}

impl DeliveryInner {
    pub(crate) fn new(handle: Handle) -> Self {
        Self {
            handle,
            tx: None,
            waiters: Vec::new(),
            state: None,
            error: None,
            settled: false,
        }
    }

    /// Delivery result, `None` if remote outcome is not received yet
    fn outcome(&self) -> Option<Result<Option<DeliveryState>, AmqpProtocolError>> {
        match self.state {
            None | Some(DeliveryState::Received(_)) if self.settled => Some(Ok(None)),
            None | Some(DeliveryState::Received(_)) => self.error.clone().map(Err),
            _ => Some(Ok(self.state.clone())),
        }
    }

    fn add_waiter(&mut self, tx: pool::Sender<()>) {
        if self.tx.as_ref().is_none_or(pool::Sender::is_canceled) {
            self.tx = Some(tx);
        } else {
            self.waiters.retain(|tx| !tx.is_canceled());
            self.waiters.push(tx);
        }
    }

    #[cfg(test)]
    pub(crate) fn waiters(&self) -> usize {
        self.waiters.len()
    }

    fn notify(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
        for tx in self.waiters.drain(..) {
            let _ = tx.send(());
        }
    }

    pub(crate) fn handle(&self) -> Handle {
        self.handle
    }

    pub(crate) fn set_error(&mut self, error: AmqpProtocolError) {
        self.error = Some(error);
        self.notify();
    }

    pub(crate) fn handle_disposition(&mut self, settled: bool, state: Option<&DeliveryState>) {
        if settled {
            self.settled = true;
        }
        if let Some(state) = state {
            self.state = Some(state.clone());
        }
        self.notify();
    }
}

impl Drop for DeliveryInner {
    fn drop(&mut self) {
        self.notify();
    }
}

pub struct TransferBuilder {
    tag: Option<Bytes>,
    settled: bool,
    data: TransferBody,
    format: Option<MessageFormat>,
    sender: Cell<SenderLinkInner>,
}

impl TransferBuilder {
    pub(crate) fn new(data: TransferBody, sender: Cell<SenderLinkInner>) -> Self {
        Self {
            tag: None,
            settled: false,
            format: None,
            data,
            sender,
        }
    }

    #[must_use]
    pub fn tag(mut self, tag: Bytes) -> Self {
        self.tag = Some(tag);
        self
    }

    #[must_use]
    pub fn settled(mut self) -> Self {
        self.settled = true;
        self
    }

    #[must_use]
    pub fn format(mut self, fmt: MessageFormat) -> Self {
        self.format = Some(fmt);
        self
    }

    /// Send delivery to the peer
    pub async fn send(self) -> Result<Delivery, AmqpProtocolError> {
        let inner = self.sender.get_ref();

        if let Some(ref err) = inner.error {
            Err(err.clone())
        } else if inner.closed {
            Err(AmqpProtocolError::Disconnected)
        } else {
            if let Some(limit) = inner.max_message_size
                && self.data.len() > limit as usize
            {
                return Err(AmqpProtocolError::BodyTooLarge);
            }

            let (id, tag) =
                SenderLinkInner::send(&self.sender, self.data, self.tag, self.settled, self.format)
                    .await?;

            Ok(Delivery {
                id,
                tag,
                session: self.sender.get_ref().session.clone(),
                flags: StdCell::new(if self.settled {
                    Flags::SENDER | Flags::LOCAL_SETTLED
                } else {
                    Flags::SENDER
                }),
            })
        }
    }
}
