use std::cell::Cell as StdCell;

use ntex::{channel::pool, util::Bytes};
use ntex_amqp_codec::protocol::{
    DeliveryNumber, DeliveryState, Disposition, DispositionInner, Error, ErrorCondition, Rejected,
    Role, TransferBody,
};
use ntex_amqp_codec::types::{Str, Symbol};

use crate::session::Session;
use crate::{cell::Cell, error::AmqpProtocolError, sndlink::SenderLinkInner};

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
    session: Session,
    flags: StdCell<Flags>,
}

#[derive(Default, Debug)]
pub(crate) struct DeliveryInner {
    settled: bool,
    state: Option<DeliveryState>,
    error: Option<AmqpProtocolError>,
    tx: Option<pool::Sender<()>>,
}

impl Delivery {
    pub(crate) fn new_rcv(id: DeliveryNumber, settled: bool, session: Session) -> Delivery {
        if !settled {
            session
                .inner
                .get_mut()
                .unsettled_rcv_deliveries
                .insert(id, DeliveryInner::new());
        }

        Delivery {
            id,
            session,
            flags: StdCell::new(if settled {
                Flags::LOCAL_SETTLED
            } else {
                Flags::empty()
            }),
        }
    }

    pub fn id(&self) -> DeliveryNumber {
        self.id
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
        self.is_set(Flags::REMOTE_SETTLED)
    }

    pub fn settle(&mut self, state: DeliveryState) {
        // remote side is settled, not need to send disposition
        if self.is_set(Flags::REMOTE_SETTLED) {
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
        if self.is_set(Flags::REMOTE_SETTLED) || self.is_set(Flags::LOCAL_SETTLED) {
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

    pub async fn wait(&self) -> Result<Option<DeliveryState>, AmqpProtocolError> {
        if self.flags.get().contains(Flags::LOCAL_SETTLED) {
            return Ok(None);
        }

        let rx = if let Some(inner) = self
            .session
            .inner
            .get_mut()
            .unsettled_deliveries(self.is_set(Flags::SENDER))
            .get_mut(&self.id)
        {
            if let Some(st) = self.check_inner(inner) {
                return st;
            }

            let (tx, rx) = self.session.inner.get_ref().pool_notify.channel();
            inner.tx = Some(tx);
            rx
        } else {
            return Ok(None);
        };
        if rx.await.is_err() {
            return Err(AmqpProtocolError::ConnectionDropped);
        }

        if let Some(inner) = self
            .session
            .inner
            .get_mut()
            .unsettled_deliveries(self.is_set(Flags::SENDER))
            .get_mut(&self.id)
        {
            if inner.settled {
                self.set_flag(Flags::REMOTE_SETTLED);
            }
            if let Some(st) = self.check_inner(inner) {
                return st;
            }
        }
        Ok(None)
    }

    fn check_inner(
        &self,
        inner: &mut DeliveryInner,
    ) -> Option<Result<Option<DeliveryState>, AmqpProtocolError>> {
        if let Some(ref st) = inner.state {
            if matches!(st, DeliveryState::Modified(..)) {
                // non terminal state
                Some(Ok(Some(inner.state.take().unwrap())))
            } else {
                // return clone of terminal state
                Some(Ok(Some(st.clone())))
            }
        } else {
            inner.error.as_ref().map(|err| Err(err.clone()))
        }
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        let inner = self.session.inner.get_mut();
        let deliveries = inner.unsettled_deliveries(self.is_set(Flags::SENDER));

        if deliveries.contains_key(&self.id) {
            deliveries.remove(&self.id);

            if !self.is_set(Flags::REMOTE_SETTLED) && !self.is_set(Flags::LOCAL_SETTLED) {
                let err = Error::build()
                    .condition(ErrorCondition::Custom(Symbol(Str::Static(
                        "Internal error",
                    ))))
                    .finish();

                let disp = Disposition(Box::new(DispositionInner {
                    role: if self.is_set(Flags::SENDER) {
                        Role::Sender
                    } else {
                        Role::Receiver
                    },
                    first: self.id,
                    last: None,
                    settled: true,
                    state: Some(DeliveryState::Rejected(Rejected { error: Some(err) })),
                    batchable: false,
                }));
                inner.post_frame(disp.into());
            }
        }
    }
}

impl DeliveryInner {
    pub(crate) fn new() -> Self {
        Self {
            tx: None,
            state: None,
            error: None,
            settled: false,
        }
    }

    pub(crate) fn set_error(&mut self, error: AmqpProtocolError) {
        self.error = Some(error);
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }

    pub(crate) fn handle_disposition(&mut self, disp: Disposition) {
        if disp.settled() {
            self.settled = true;
        }
        if let Some(state) = disp.state() {
            self.state = Some(state.clone());
        }
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for DeliveryInner {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

pub struct DeliveryBuilder {
    tag: Option<Bytes>,
    settled: bool,
    data: TransferBody,
    sender: Cell<SenderLinkInner>,
}

impl DeliveryBuilder {
    pub(crate) fn new(data: TransferBody, sender: Cell<SenderLinkInner>) -> Self {
        Self {
            tag: None,
            settled: false,
            data,
            sender,
        }
    }

    pub fn tag(mut self, tag: Bytes) -> Self {
        self.tag = Some(tag);
        self
    }

    pub fn settled(mut self) -> Self {
        self.settled = true;
        self
    }

    pub async fn send(self) -> Result<Delivery, AmqpProtocolError> {
        let inner = self.sender.get_ref();

        if let Some(ref err) = inner.error {
            Err(err.clone())
        } else if inner.closed {
            Err(AmqpProtocolError::Disconnected)
        } else {
            let id = self
                .sender
                .get_mut()
                .send(self.data, self.tag, self.settled)
                .await?;

            Ok(Delivery {
                id,
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
