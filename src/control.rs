use std::{cell::Cell as StdCell, cell::RefCell, collections::VecDeque, fmt, io, rc::Rc};

use ntex_amqp_codec::protocol;
use ntex_service::ReadyNotifier;
use ntex_util::future::Either;

use crate::error::AmqpProtocolError;
use crate::rcvlink::ReceiverLink;
use crate::session::{Session, SessionInner};
use crate::sndlink::SenderLink;
use crate::types::Wrapper;

pub struct ControlFrame(pub(super) Rc<FrameInner>);

pub(super) struct FrameInner {
    pub(super) kind: ControlFrameKind,
    pub(super) session: Option<Rc<SessionInner>>,
}

impl fmt::Debug for ControlFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlFrame")
            .field("kind", &self.0.kind)
            .finish()
    }
}

#[derive(Debug)]
pub enum ControlFrameKind {
    AttachSender(protocol::Attach, Wrapper<protocol::Attach>, SenderLink),
    AttachReceiver(protocol::Attach, Wrapper<protocol::Attach>, ReceiverLink),
    /// Sender link flow, link credit is already applied
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
    pub(crate) fn new(session: Rc<SessionInner>, kind: ControlFrameKind) -> Self {
        ControlFrame(Rc::new(FrameInner {
            session: Some(session),
            kind,
        }))
    }

    pub(crate) fn new_kind(kind: ControlFrameKind) -> Self {
        ControlFrame(Rc::new(FrameInner {
            session: None,
            kind,
        }))
    }

    pub(crate) fn clone(&self) -> Self {
        ControlFrame(self.0.clone())
    }

    pub(crate) fn session_cell(&self) -> &Rc<SessionInner> {
        self.0.session.as_ref().unwrap()
    }

    #[inline]
    pub fn kind(&self) -> &ControlFrameKind {
        &self.0.kind
    }

    pub fn session(&self) -> Option<Session> {
        self.0.session.clone().map(Session::new)
    }
}

#[derive(Default, Debug)]
pub(crate) struct ControlQueue {
    pub(crate) pending: RefCell<VecDeque<ControlFrame>>,
    pub(crate) changed: ReadyNotifier,
    closed: StdCell<bool>,
}

impl ControlQueue {
    pub(crate) fn enqueue_frame(&self, frame: ControlFrame) {
        if self.closed.get() {
            log::trace!("Control queue is closed, discarding frame");
        } else {
            self.pending.borrow_mut().push_back(frame);
            self.changed.notify();
        }
    }

    pub(crate) fn close(&self) {
        self.closed.set(true);
    }

    pub(crate) fn clear(&self) {
        drop(self.pending.take());
    }
}
