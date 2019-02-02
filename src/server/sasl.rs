use std::fmt;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::Service;
use amqp_codec::protocol::{
    self, Error, SaslChallenge, SaslCode, SaslFrameBody, SaslMechanisms, SaslOutcome, Symbols,
};
use amqp_codec::{AmqpCodec, SaslFrame};
use bytes::Bytes;
use futures::unsync::{mpsc, oneshot};
use futures::{future::err, Async, Future, Poll, Sink, Stream};
use string::{self, TryFrom};

use crate::cell::Cell;

use super::errors::{AmqpError, HandshakeError, SaslError};
use super::factory::Inner;
use super::link::OpenLink;

pub struct SaslAuth {
    sender: mpsc::UnboundedSender<(SaslFrame, oneshot::Sender<SaslFrame>)>,
    mechanisms: Symbols,
}

impl fmt::Debug for SaslAuth {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SaslAuth")
            .field("mechanisms", &self.mechanisms)
            .finish()
    }
}

impl SaslAuth {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<(SaslFrame, oneshot::Sender<SaslFrame>)>,
    ) -> Self {
        SaslAuth {
            sender,
            mechanisms: Symbols::default(),
        }
    }
}

impl SaslAuth {
    /// Add supported sasl mechanism
    pub fn mechanism<U: Into<String>>(mut self, symbol: U) -> Self {
        self.mechanisms.push(
            string::String::try_from(Bytes::from(symbol.into().into_bytes()))
                .unwrap()
                .into(),
        );
        self
    }

    pub fn send(self) -> impl Future<Item = SaslInit, Error = SaslError> {
        let SaslAuth {
            sender, mechanisms, ..
        } = self;

        let frame = SaslMechanisms {
            sasl_server_mechanisms: mechanisms,
        }
        .into();

        let (tx, rx) = oneshot::channel();
        sender
            .send((frame, tx))
            .map_err(|_| SaslError::Disconnected)
            .and_then(move |sender| {
                rx.then(move |res| match res {
                    Ok(frame) => match frame.body {
                        SaslFrameBody::SaslInit(frame) => Ok(SaslInit { frame, sender }),
                        body => Err(SaslError::Unexpected(body)),
                    },
                    Err(_) => Err(SaslError::Disconnected),
                })
            })
    }
}

pub struct SaslInit {
    frame: protocol::SaslInit,
    sender: mpsc::UnboundedSender<(SaslFrame, oneshot::Sender<SaslFrame>)>,
}

impl fmt::Debug for SaslInit {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SaslInit")
            .field("frame", &self.frame)
            .finish()
    }
}

impl SaslInit {
    /// Sasl mechanism
    pub fn mechanism(&self) -> &str {
        self.frame.mechanism.as_str()
    }

    /// Sasl initial response
    pub fn initial_response(&self) -> Option<&[u8]> {
        self.frame.initial_response.as_ref().map(|b| b.as_ref())
    }

    /// Sasl initial response
    pub fn hostname(&self) -> Option<&str> {
        self.frame.hostname.as_ref().map(|b| b.as_ref())
    }

    /// Initiate sasl challenge
    pub fn challenge(self) -> impl Future<Item = SaslResponse, Error = SaslError> {
        self.challenge_with(Bytes::new())
    }

    /// Initiate sasl challenge with challenge payload
    pub fn challenge_with(
        self,
        challenge: Bytes,
    ) -> impl Future<Item = SaslResponse, Error = SaslError> {
        let sender = self.sender;
        let frame = SaslChallenge { challenge }.into();

        let (tx, rx) = oneshot::channel();
        sender
            .send((frame, tx))
            .map_err(|_| SaslError::Disconnected)
            .and_then(move |sender| {
                rx.then(move |res| match res {
                    Ok(frame) => match frame.body {
                        SaslFrameBody::SaslResponse(frame) => Ok(SaslResponse { frame, sender }),
                        body => Err(SaslError::Unexpected(body)),
                    },
                    Err(_) => Err(SaslError::Disconnected),
                })
            })
    }

    /// Sasl challenge outcome
    pub fn outcome(self, code: SaslCode) -> impl Future<Item = (), Error = SaslError> {
        let sender = self.sender;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();

        let (tx, rx) = oneshot::channel();
        sender
            .send((frame, tx))
            .map_err(|_| SaslError::Disconnected)
            .and_then(move |sender| {
                rx.then(move |res| match res {
                    Ok(frame) => Ok(()),
                    Err(_) => Err(SaslError::Disconnected),
                })
            })
    }
}

pub struct SaslResponse {
    frame: protocol::SaslResponse,
    sender: mpsc::UnboundedSender<(SaslFrame, oneshot::Sender<SaslFrame>)>,
}

impl fmt::Debug for SaslResponse {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SaslResponse")
            .field("frame", &self.frame)
            .finish()
    }
}

impl SaslResponse {
    pub fn response(&self) -> &[u8] {
        &self.frame.response[..]
    }

    pub fn outcome(self, code: SaslCode) -> impl Future<Item = (), Error = SaslError> {
        let sender = self.sender;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();

        let (tx, rx) = oneshot::channel();
        sender
            .send((frame, tx))
            .map_err(|_| SaslError::Disconnected)
            .and_then(move |sender| {
                rx.then(move |res| match res {
                    Ok(frame) => Ok(()),
                    Err(_) => Err(SaslError::Disconnected),
                })
            })
    }
}

pub(crate) fn no_sasl_auth<S>(sasl: SaslAuth) -> impl Future<Item = S, Error = Error> {
    err(AmqpError::not_implemented().into())
}

pub(crate) struct Sasl<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite + 'static,
    F: Service<Request = Option<SaslAuth>, Response = (St, S), Error = Error> + 'static,
    S: Service<Request = OpenLink<St>, Response = (), Error = Error> + 'static,
    St: 'static,
{
    fut: F::Future,
    framed: Option<Framed<Io, AmqpCodec<SaslFrame>>>,
    rx: mpsc::UnboundedReceiver<(SaslFrame, oneshot::Sender<SaslFrame>)>,
    tx: Option<oneshot::Sender<SaslFrame>>,
    state: SaslState,
    outcome: Option<SaslOutcome>,
}

#[derive(PartialEq, Debug)]
enum SaslState {
    New,
    Mechanisms,
    Init,
    Challenge,
    Response,
    Success,
    Error,
}

impl SaslState {
    fn is_completed(&self) -> bool {
        *self == SaslState::Success
    }

    fn next(&mut self, frame: &SaslFrameBody) -> bool {
        match self {
            SaslState::New => {
                if let SaslFrameBody::SaslMechanisms(_) = frame {
                    *self = SaslState::Mechanisms;
                    true
                } else {
                    false
                }
            }
            SaslState::Mechanisms => {
                if let SaslFrameBody::SaslInit(_) = frame {
                    *self = SaslState::Init;
                    true
                } else {
                    false
                }
            }
            SaslState::Init => match frame {
                SaslFrameBody::SaslChallenge(_) => {
                    *self = SaslState::Challenge;
                    true
                }
                SaslFrameBody::SaslOutcome(_) => {
                    *self = SaslState::Response;
                    true
                }
                _ => false,
            },
            SaslState::Challenge => {
                if let SaslFrameBody::SaslResponse(_) = frame {
                    *self = SaslState::Response;
                    true
                } else {
                    false
                }
            }
            SaslState::Response => {
                if let SaslFrameBody::SaslOutcome(ref out) = frame {
                    true
                } else {
                    false
                }
            }
            SaslState::Success | SaslState::Error => false,
        }
    }
}

impl<Io, F, St, S> Sasl<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite + 'static,
    F: Service<Request = Option<SaslAuth>, Response = (St, S), Error = Error> + 'static,
    S: Service<Request = OpenLink<St>, Response = (), Error = Error> + 'static,
    St: 'static,
{
    pub(super) fn new(
        cell: &mut Cell<Inner<Io, F, St, S>>,
        framed: Framed<Io, AmqpCodec<SaslFrame>>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded();
        let auth = SaslAuth::new(tx);

        Sasl {
            rx,
            tx: None,
            framed: Some(framed),
            fut: cell.get_mut().factory.call(Some(auth)),
            state: SaslState::New,
            outcome: None,
        }
    }
}

impl<Io, F, St, S> Future for Sasl<Io, F, St, S>
where
    Io: AsyncRead + AsyncWrite + 'static,
    F: Service<Request = Option<SaslAuth>, Response = (St, S), Error = Error> + 'static,
    S: Service<Request = OpenLink<St>, Response = (), Error = Error> + 'static,
    St: 'static,
{
    type Item = (St, S, Framed<Io, AmqpCodec<SaslFrame>>);
    type Error = HandshakeError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fut.poll() {
            Ok(Async::Ready((st, srv))) => {
                return Ok(Async::Ready((st, srv, self.framed.take().unwrap())));
            }
            Ok(Async::NotReady) => (),
            Err(e) => {
                trace!("Error during sasl handling: {}", e);
                return Err(HandshakeError::Sasl);
            }
        }

        if self.tx.is_some() {
            match self.framed.as_mut().unwrap().poll() {
                Ok(Async::Ready(Some(frame))) => {
                    if !self.state.next(&frame.body) {
                        return Err(HandshakeError::UnexpectedSasl(frame));
                    }
                    if self.tx.take().unwrap().send(frame).is_err() {
                        return Err(HandshakeError::Disconnected);
                    }
                }
                Ok(Async::Ready(None)) => return Err(HandshakeError::Disconnected),
                Ok(Async::NotReady) => (),
                Err(e) => return Err(e.into()),
            }
        } else {
            match self.rx.poll() {
                Ok(Async::NotReady) => (),
                Ok(Async::Ready(Some((frame, tx)))) => {
                    if !self.state.next(&frame.body) {
                        return Err(HandshakeError::UnexpectedSasl(frame));
                    }
                    if let SaslFrameBody::SaslOutcome(ref frame) = frame.body {
                        self.outcome = Some(frame.clone());
                    }
                    let _ = self.framed.as_mut().unwrap().force_send(frame);
                    self.tx = Some(tx);
                    return self.poll();
                }
                Ok(Async::Ready(None)) => return Err(HandshakeError::Disconnected),
                Err(e) => return Err(HandshakeError::Sasl),
            }
        }

        if !self.framed.as_mut().unwrap().is_write_buf_empty() {
            match self.framed.as_mut().unwrap().poll_complete() {
                Ok(Async::NotReady) => (),
                Err(e) => {
                    trace!("error sending data: {}", e);
                    return Err(e.into());
                }
                Ok(Async::Ready(_)) => {
                    if let Some(outcome) = self.outcome.take() {
                        if outcome.code == SaslCode::Ok {
                            self.state = SaslState::Success;
                        } else {
                            self.state = SaslState::Error;
                        }
                        if self.tx.take().unwrap().send(outcome.into()).is_err() {
                            return Err(HandshakeError::Disconnected);
                        }
                    }
                }
            }
        }

        Ok(Async::NotReady)
    }
}
