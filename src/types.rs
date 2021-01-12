use std::fmt;

use bytes::Bytes;
use bytestring::ByteString;
use ntex::router::Path;

use crate::codec::protocol::{
    self, Accepted, Attach, DeliveryState, Error, Rejected, TransferBody,
};
use crate::codec::{AmqpParseError, Decode};
use crate::{rcvlink::ReceiverLink, session::Session, Handle, State};

pub struct Link<S> {
    pub(crate) state: State<S>,
    pub(crate) link: ReceiverLink,
    pub(crate) path: Path<ByteString>,
}

impl<S> Link<S> {
    pub(crate) fn new(link: ReceiverLink, state: State<S>) -> Self {
        Link {
            state,
            link,
            path: Path::new(ByteString::from_static("")),
        }
    }

    pub fn path(&self) -> &Path<ByteString> {
        &self.path
    }

    pub fn path_mut(&mut self) -> &mut Path<ByteString> {
        &mut self.path
    }

    pub fn frame(&self) -> &Attach {
        self.link.frame()
    }

    pub fn state(&self) -> &S {
        self.state.get_ref()
    }

    pub fn handle(&self) -> Handle {
        self.link.handle()
    }

    pub fn session(&self) -> &Session {
        self.link.session()
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.link.session_mut()
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
        }
    }
}

impl<S> fmt::Debug for Link<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Link<S>")
            .field("frame", self.link.frame())
            .finish()
    }
}

pub struct Transfer<S> {
    state: State<S>,
    frame: protocol::Transfer,
    link: ReceiverLink,
}

#[derive(Debug)]
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

impl<S> Transfer<S> {
    pub(crate) fn new(state: State<S>, frame: protocol::Transfer, link: ReceiverLink) -> Self {
        Transfer { state, frame, link }
    }

    pub fn state(&self) -> &S {
        self.state.get_ref()
    }

    pub fn session(&self) -> &Session {
        self.link.session()
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.link.session_mut()
    }

    pub fn frame(&self) -> &protocol::Transfer {
        &self.frame
    }

    pub fn body(&self) -> Option<&Bytes> {
        match self.frame.body {
            Some(TransferBody::Data(ref b)) => Some(b),
            _ => None,
        }
    }

    pub fn load_message<T: Decode>(&self) -> Result<T, AmqpParseError> {
        if let Some(TransferBody::Data(ref b)) = self.frame.body {
            Ok(T::decode(b)?.1)
        } else {
            Err(AmqpParseError::UnexpectedType("body"))
        }
    }
}

impl<S> fmt::Debug for Transfer<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Transfer<S>")
            .field("frame", &self.frame)
            .finish()
    }
}
