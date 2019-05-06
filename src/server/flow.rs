use std::fmt;

use crate::cell::Cell;
use crate::rcvlink::ReceiverLink;
use crate::session::Session;

pub struct Flow<S> {
    state: Cell<S>,
    link: ReceiverLink,
}

pub struct FlowCredit(u32);

impl From<u32> for FlowCredit {
    fn from(credit: u32) -> Self {
        FlowCredit(credit)
    }
}

impl<S> fmt::Debug for Flow<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Flow<S>").finish()
    }
}

impl<S> Flow<S> {
    pub(crate) fn new(state: Cell<S>, link: ReceiverLink) -> Flow<S> {
        Flow { state, link }
    }

    pub fn state(&self) -> &S {
        self.state.get_ref()
    }

    pub fn state_mut(&mut self) -> &mut S {
        self.state.get_mut()
    }

    pub fn session(&self) -> &Session {
        self.link.session()
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.link.session_mut()
    }
}
