use std::fmt;

use bytestring::ByteString;
use ntex::router::Path;
use ntex_amqp_codec::protocol::Attach;

use crate::rcvlink::ReceiverLink;
use crate::server::State;
use crate::session::Session;
use crate::{Configuration, Handle};

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

    pub fn state_mut(&mut self) -> &mut S {
        self.state.get_mut()
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

    #[inline]
    /// Get remote connection configuration
    pub fn remote_config(&self) -> &Configuration {
        &self.link.remote_config()
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
