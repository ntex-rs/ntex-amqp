use std::fmt;

use ntex_amqp_codec::protocol;

use crate::cell::Cell;
use crate::error::AmqpProtocolError;
use crate::rcvlink::ReceiverLink;
use crate::session::SessionInner;
use crate::sndlink::SenderLink;

pub struct ControlFrame<E>(pub(super) Cell<FrameInner<E>>);

pub(super) struct FrameInner<E> {
    pub(super) kind: ControlFrameKind<E>,
    pub(super) session: Option<Cell<SessionInner>>,
}

impl<E: fmt::Debug> fmt::Debug for ControlFrame<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlFrame")
            .field("kind", &self.0.get_ref().kind)
            .finish()
    }
}

#[derive(Debug)]
pub enum ControlFrameKind<E> {
    AttachReceiver(ReceiverLink),
    AttachSender(Box<protocol::Attach>, SenderLink),
    Flow(protocol::Flow, SenderLink),
    DetachSender(protocol::Detach, SenderLink),
    DetachReceiver(protocol::Detach, ReceiverLink),
    Error(E),
    ProtocolError(AmqpProtocolError),
    Closed(bool),
}

impl<E> ControlFrame<E> {
    pub(crate) fn new(session: Cell<SessionInner>, kind: ControlFrameKind<E>) -> Self {
        ControlFrame(Cell::new(FrameInner {
            session: Some(session),
            kind,
        }))
    }

    pub(crate) fn new_kind(kind: ControlFrameKind<E>) -> Self {
        ControlFrame(Cell::new(FrameInner {
            session: None,
            kind,
        }))
    }

    pub(crate) fn clone(&self) -> Self {
        ControlFrame(self.0.clone())
    }

    pub(crate) fn session(&self) -> &Cell<SessionInner> {
        self.0.get_ref().session.as_ref().unwrap()
    }

    #[inline]
    pub fn frame(&self) -> &ControlFrameKind<E> {
        &self.0.kind
    }
}
