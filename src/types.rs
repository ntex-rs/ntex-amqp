use std::{fmt, rc::Rc};

use ntex::router::Path;
use ntex::util::{ByteString, Either};

use crate::codec::protocol::{Accepted, Attach, DeliveryState, Detach, Error, Flow, Rejected};
use crate::{
    error::AmqpProtocolError, rcvlink::ReceiverLink, session::Session, sndlink::SenderLink, Handle,
    State,
};

pub use crate::codec::protocol::Transfer;

#[derive(Debug)]
pub enum Message {
    Attached(Attach, ReceiverLink),
    Detached(ReceiverLink),
    DetachedAll(Vec<ReceiverLink>),
    Transfer(ReceiverLink),
}

pub(crate) enum Action {
    None,
    AttachSender(SenderLink, Attach),
    AttachReceiver(ReceiverLink, Attach),
    DetachSender(SenderLink, Detach),
    DetachReceiver(ReceiverLink, Detach),
    SessionEnded(Vec<Either<SenderLink, ReceiverLink>>),
    Flow(SenderLink, Flow),
    Transfer(ReceiverLink),
    RemoteClose(AmqpProtocolError),
}

pub struct Link<S> {
    pub(crate) state: State<S>,
    pub(crate) link: ReceiverLink,
    pub(crate) path: Path<ByteString>,
    pub(crate) attach: Rc<Attach>,
}

impl<S> Link<S> {
    pub(crate) fn new(
        attach: Attach,
        link: ReceiverLink,
        state: State<S>,
        path: ByteString,
    ) -> Self {
        Link {
            state,
            link,
            attach: Rc::new(attach),
            path: Path::new(path),
        }
    }

    pub fn path(&self) -> &Path<ByteString> {
        &self.path
    }

    pub fn path_mut(&mut self) -> &mut Path<ByteString> {
        &mut self.path
    }

    pub fn state(&self) -> &State<S> {
        &self.state
    }

    pub fn handle(&self) -> Handle {
        self.link.handle()
    }

    pub fn frame(&self) -> &Attach {
        self.attach.as_ref()
    }

    pub fn session(&self) -> &Session {
        self.link.session()
    }

    pub fn receiver(&self) -> &ReceiverLink {
        &self.link
    }

    pub fn receiver_mut(&mut self) -> &mut ReceiverLink {
        &mut self.link
    }

    pub fn link_credit(&self, credit: u32) {
        self.link.set_link_credit(credit);
    }
}

impl<S> Clone for Link<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            link: self.link.clone(),
            path: self.path.clone(),
            attach: self.attach.clone(),
        }
    }
}

impl<S> fmt::Debug for Link<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Link<S>")
            .field("frame", self.frame())
            .finish()
    }
}

#[derive(Debug, From)]
pub enum Outcome {
    Accept,
    Reject,
    Error(Error),
}

impl Outcome {
    pub(crate) fn into_delivery_state(self) -> DeliveryState {
        match self {
            Outcome::Accept => DeliveryState::Accepted(Accepted {}),
            Outcome::Reject => DeliveryState::Rejected(Rejected { error: None }),
            Outcome::Error(e) => DeliveryState::Rejected(Rejected { error: Some(e) }),
        }
    }
}
