use amqp_codec::protocol::{
    Accepted, Attach, DeliveryState, Disposition, Error, Rejected, Role, Transfer,
};
use amqp_codec::Decode;
use bytes::Bytes;
use futures::{Async, Poll, Stream};

use super::errors::AmqpError;
use crate::cell::Cell;
use crate::errors::AmqpTransportError;
use crate::link::ReceiverLink;
use crate::session::Session;

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
        self.link.set_flow();

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

impl<S> Stream for Link<S> {
    type Item = Message<S>;
    type Error = AmqpTransportError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            return match self.link.poll()? {
                Async::Ready(Some(transfer)) => {
                    Ok(Async::Ready(Some(Message {
                        state: self.state.clone(),
                        transfer,
                        link: Some(self.link.clone()),
                    })))
                    //  else {
                    //     let disposition = Disposition {
                    //         role: Role::Receiver,
                    //         first: 1, // DeliveryNumber,
                    //         last: None,
                    //         settled: false,
                    //         state: Some(DeliveryState::Rejected(Rejected { error: None })),
                    //         batchable: false,
                    //         body: None,
                    //     };
                    //     self.link.send_disposition(disposition);
                    //     continue;
                    // }
                }
                Async::Ready(None) => Ok(Async::Ready(None)),
                Async::NotReady => Ok(Async::NotReady),
            };
        }
    }
}

// #[derive(Debug)]
pub struct Message<S> {
    state: Cell<S>,
    transfer: Transfer,
    link: Option<ReceiverLink>,
}

impl<S> Drop for Message<S> {
    fn drop(&mut self) {
        if let Some(ref mut link) = self.link.take() {
            let disposition = Disposition {
                role: Role::Receiver,
                first: 1, // DeliveryNumber,
                last: None,
                settled: false,
                state: Some(DeliveryState::Rejected(Rejected { error: None })),
                batchable: false,
                body: None,
            };
            link.send_disposition(disposition);
        }
    }
}

impl<S> Message<S> {
    pub fn state(&self) -> &S {
        self.state.get_ref()
    }

    pub fn state_mut(&mut self) -> &mut S {
        self.state.get_mut()
    }

    pub fn transfer(&self) -> &Transfer {
        &self.transfer
    }

    pub fn body(&self) -> Option<&Bytes> {
        self.transfer.body.as_ref()
    }

    pub fn session(&self) -> &Session {
        self.link.as_ref().unwrap().session()
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.link.as_mut().unwrap().session_mut()
    }

    pub fn load_message(&self) -> Result<amqp_codec::Message, AmqpError> {
        if let Some(ref b) = self.transfer.body {
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
                first: self.transfer.delivery_id.unwrap_or(0), // DeliveryNumber,
                last: None,
                settled: true,
                batchable: false,
                body: None,
            };
            link.send_disposition(disposition);
        }
    }
}
