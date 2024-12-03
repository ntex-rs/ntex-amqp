use std::{cell::RefCell, collections::VecDeque, fmt, io};

use ntex::{task::LocalWaker, util::Either};
use ntex_amqp_codec::protocol;

use crate::cell::Cell;
use crate::error::AmqpProtocolError;
use crate::rcvlink::ReceiverLink;
use crate::session::{Session, SessionInner};
use crate::sndlink::SenderLink;

pub struct ControlFrame(pub(super) Cell<FrameInner>);

pub(super) struct FrameInner {
    pub(super) kind: ControlFrameKind,
    pub(super) session: Option<Cell<SessionInner>>,
}

impl fmt::Debug for ControlFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlFrame")
            .field("kind", &self.0.get_ref().kind)
            .finish()
    }
}

#[derive(Debug)]
pub enum ControlFrameKind {
    AttachSender(protocol::Attach, SenderLink),
    AttachReceiver(protocol::Attach, ReceiverLink),
    Flow(protocol::Flow, SenderLink),
    LocalDetachSender(protocol::Detach, SenderLink),
    RemoteDetachSender(protocol::Detach, SenderLink),
    LocalDetachReceiver(protocol::Detach, ReceiverLink),
    RemoteDetachReceiver(protocol::Detach, ReceiverLink),
    LocalSessionEnded(Vec<Either<SenderLink, ReceiverLink>>),
    RemoteSessionEnded(Vec<Either<SenderLink, ReceiverLink>>),
    ProtocolError(AmqpProtocolError),
    Disconnected(Option<io::Error>),
    Closed,
}

impl ControlFrame {
    pub(crate) fn new(session: Cell<SessionInner>, kind: ControlFrameKind) -> Self {
        ControlFrame(Cell::new(FrameInner {
            session: Some(session),
            kind,
        }))
    }

    pub(crate) fn new_kind(kind: ControlFrameKind) -> Self {
        ControlFrame(Cell::new(FrameInner {
            session: None,
            kind,
        }))
    }

    pub(crate) fn clone(&self) -> Self {
        ControlFrame(self.0.clone())
    }

    pub(crate) fn session_cell(&self) -> &Cell<SessionInner> {
        self.0.get_ref().session.as_ref().unwrap()
    }

    #[inline]
    pub fn kind(&self) -> &ControlFrameKind {
        &self.0.kind
    }

    pub fn session(&self) -> Option<Session> {
        self.0.get_ref().session.clone().map(Session::new)
    }
}

#[derive(Default, Debug)]
pub(crate) struct ControlQueue {
    pub(crate) pending: RefCell<VecDeque<ControlFrame>>,
    pub(crate) waker: LocalWaker,
}

impl ControlQueue {
    pub(crate) fn enqueue_frame(&self, frame: ControlFrame) {
        self.pending.borrow_mut().push_back(frame);
        self.waker.wake();
    }
}
