use std::fmt;

use amqp_codec::protocol::{Accepted, DeliveryState, Disposition, Error, Rejected, Role, Transfer};
use amqp_codec::Decode;
use bytes::Bytes;

use super::errors::AmqpError;
use crate::cell::Cell;
use crate::rcvlink::ReceiverLink;
use crate::session::Session;

#[derive(Debug, From)]
pub enum Frame<S> {
    Message(Message<S>),
    Flow(Flow<S>),
}

pub trait ServerFrame<S> {
    fn state(&self) -> &S;

    fn state_mut(&mut self) -> &mut S;

    fn session(&self) -> &Session;

    fn session_mut(&mut self) -> &mut Session;
}

pub struct Message<S> {
    state: Cell<S>,
    frame: Transfer,
    link: Option<ReceiverLink>,
}

impl<S> fmt::Debug for Message<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Message<S>")
            .field("frame", &self.frame)
            .finish()
    }
}

impl<S> Drop for Message<S> {
    fn drop(&mut self) {
        if let Some(ref mut link) = self.link.take() {
            let disposition = Disposition {
                role: Role::Receiver,
                first: self.frame.delivery_id().unwrap(),
                last: None,
                settled: false,
                state: Some(DeliveryState::Rejected(Rejected { error: None })),
                batchable: false,
            };
            link.send_disposition(disposition);
        }
    }
}

impl<S> Message<S> {
    pub(crate) fn new(state: Cell<S>, frame: Transfer, link: ReceiverLink) -> Self {
        Message {
            state,
            frame,
            link: Some(link),
        }
    }

    pub fn state(&self) -> &S {
        self.state.get_ref()
    }

    pub fn state_mut(&mut self) -> &mut S {
        self.state.get_mut()
    }

    pub fn session(&self) -> &Session {
        self.link.as_ref().unwrap().session()
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.link.as_mut().unwrap().session_mut()
    }

    pub fn frame(&self) -> &Transfer {
        &self.frame
    }

    pub fn body(&self) -> Option<&Bytes> {
        self.frame.body.as_ref()
    }

    pub fn load_message(&self) -> Result<amqp_codec::Message, AmqpError> {
        if let Some(ref b) = self.frame.body {
            if let Ok((_, msg)) = amqp_codec::Message::decode(b) {
                Ok(msg)
            } else {
                Err(AmqpError::decode_error().description("Can not decode message"))
            }
        } else {
            Err(AmqpError::invalid_field().description("Empty body"))
        }
    }

    pub fn accept(self) {
        self.settle(DeliveryState::Accepted(Accepted {}))
    }

    pub fn reject(self, error: Option<Error>) {
        self.settle(DeliveryState::Rejected(Rejected { error }))
    }

    pub fn settle(mut self, state: DeliveryState) {
        if let Some(mut link) = self.link.take() {
            let disposition = Disposition {
                state: Some(state),
                role: Role::Receiver,
                first: self.frame.delivery_id.unwrap(),
                last: None,
                settled: true,
                batchable: false,
            };
            link.send_disposition(disposition);
        }
    }
}

pub struct Flow<S> {
    state: Cell<S>,
    link: ReceiverLink,
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

    /// Set new credit
    pub fn credit(mut self, credit: u32) {
        self.link.set_link_credit(credit)
    }
}
