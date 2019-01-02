use amqp::protocol::Attach;

use crate::cell::Cell;
use crate::link::ReceiverLink;

pub struct OpenLink<S> {
    pub(crate) state: Cell<S>,
    pub(crate) link: ReceiverLink,
}

impl<S> OpenLink<S> {
    pub fn frame(&self) -> &Attach {
        self.link.frame()
    }

    pub fn open(mut self) -> Link<S> {
        self.link.open();

        Link {
            state: self.state,
            link: self.link,
        }
    }
}

pub struct Link<S> {
    pub(crate) state: Cell<S>,
    pub(crate) link: ReceiverLink,
}

impl<S> Link<S> {
    pub fn state(&self) -> &S {
        self.state.get_ref()
    }

    pub fn state_mut(&mut self) -> &mut S {
        self.state.get_mut()
    }
}
