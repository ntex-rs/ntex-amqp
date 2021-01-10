mod app;
mod control;
mod default;
mod dispatcher;
mod errors;
mod handshake;
mod link;
pub mod sasl;
mod service;
mod transfer;

pub use self::app::App;
pub use self::control::{ControlFrame, ControlFrameKind};
pub use self::errors::ServerError;
pub use self::handshake::{Handshake, HandshakeAck, HandshakeAmqpOpened};
pub use self::link::Link;
pub use self::sasl::Sasl;
pub use self::service::Server;
pub use self::transfer::{Outcome, Transfer};
pub use crate::errors::{AmqpError, Error, LinkError};

use crate::cell::Cell;

#[doc(hidden)]
pub type Message<T> = Transfer<T>;

#[derive(Debug)]
pub struct State<St>(Cell<St>);

impl<St> State<St> {
    pub(crate) fn new(st: St) -> Self {
        State(Cell::new(st))
    }

    pub(crate) fn clone(&self) -> Self {
        State(self.0.clone())
    }

    pub fn get_ref(&self) -> &St {
        self.0.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut St {
        self.0.get_mut()
    }
}

impl<St> std::ops::Deref for State<St> {
    type Target = St;

    fn deref(&self) -> &Self::Target {
        self.get_ref()
    }
}

impl<St> std::ops::DerefMut for State<St> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}
