use amqp_codec::protocol::{
    Accepted, Attach, DeliveryState, Disposition, Rejected, Role, Transfer,
};
use amqp_codec::Decode;
use futures::{Async, Poll, Stream};

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
                    if let Some(ref b) = transfer.body {
                        if let Ok((_, msg)) = amqp_codec::Message::decode(b) {
                            let msg = Message {
                                state: self.state.clone(),
                                transfer,
                                link: Some(self.link.clone()),
                                message: msg,
                            };
                            Ok(Async::Ready(Some(msg)))
                        } else {
                            continue;
                        }
                    // println!("DECODE: {:#?}", msg);
                    } else {
                        let disposition = Disposition {
                            role: Role::Receiver,
                            first: 1, // DeliveryNumber,
                            last: None,
                            settled: false,
                            state: Some(DeliveryState::Rejected(Rejected { error: None })),
                            batchable: false,
                            body: None,
                        };
                        self.link.send_disposition(disposition);
                        continue;
                    }
                    // let msg = Message::deserialize(&b).unwrap();
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
    message: amqp_codec::Message,
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
    pub fn transfer(&self) -> &Transfer {
        &self.transfer
    }

    pub fn message(&self) -> &amqp_codec::Message {
        &self.message
    }

    pub fn accept(self) {
        self.settle(DeliveryState::Accepted(Accepted {}))
    }

    pub fn session(&self) -> &Session {
        self.link.as_ref().unwrap().session()
    }

    pub fn session_mut(&mut self) -> &mut Session {
        self.link.as_mut().unwrap().session_mut()
    }

    pub fn reply_message(&self) -> amqp_codec::Message {
        amqp_codec::Message::default().if_some(&self.message.properties, |msg, data| {
            msg.set_properties(|props| props.correlation_id = data.message_id.clone())
        })
    }

    pub fn settle(mut self, state: DeliveryState) {
        if let Some(mut link) = self.link.take() {
            let disposition = Disposition {
                state: Some(state),
                role: Role::Receiver,
                first: 0, // DeliveryNumber,
                last: None,
                settled: true,
                batchable: false,
                body: None,
            };
            link.send_disposition(disposition);
        }
    }
}
