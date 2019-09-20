use std::fmt;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::{NewService, Service};
use amqp_codec::protocol::{
    self, ProtocolId, SaslChallenge, SaslCode, SaslFrameBody, SaslMechanisms, SaslOutcome, Symbols,
};
use amqp_codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, SaslFrame};
use bytes::Bytes;
use futures::future::{err, ok, Either, FutureResult};
use futures::{Async, Future, Poll, Sink, Stream};
use string::{self, TryFrom};

use super::connect::{ConnectAck, ConnectOpened};
use super::errors::{AmqpError, ServerError};

pub struct Sasl<Io> {
    framed: Framed<Io, ProtocolIdCodec>,
    mechanisms: Symbols,
}

impl<Io> fmt::Debug for Sasl<Io> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SaslAuth")
            .field("mechanisms", &self.mechanisms)
            .finish()
    }
}

impl<Io> Sasl<Io> {
    pub(crate) fn new(framed: Framed<Io, ProtocolIdCodec>) -> Self {
        Sasl {
            framed,
            mechanisms: Symbols::default(),
        }
    }
}

impl<Io> Sasl<Io>
where
    Io: AsyncRead + AsyncWrite,
{
    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        self.framed.get_ref()
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        self.framed.get_mut()
    }

    /// Add supported sasl mechanism
    pub fn mechanism<U: Into<String>>(mut self, symbol: U) -> Self {
        self.mechanisms.push(
            string::String::try_from(Bytes::from(symbol.into().into_bytes()))
                .unwrap()
                .into(),
        );
        self
    }

    /// Initialize sasl auth procedure
    pub fn init(self) -> impl Future<Item = Init<Io>, Error = ServerError<()>> {
        let Sasl {
            framed, mechanisms, ..
        } = self;

        let framed = framed.into_framed(AmqpCodec::<SaslFrame>::new());
        let frame = SaslMechanisms {
            sasl_server_mechanisms: mechanisms,
        }
        .into();

        framed
            .send(frame)
            .map_err(ServerError::from)
            .and_then(move |framed| {
                framed
                    .into_future()
                    .map_err(|res| ServerError::from(res.0))
                    .and_then(|(res, framed)| match res {
                        Some(frame) => match frame.body {
                            SaslFrameBody::SaslInit(frame) => Ok(Init { frame, framed }),
                            body => Err(ServerError::UnexpectedSaslBodyFrame(body)),
                        },
                        None => Err(ServerError::Disconnected),
                    })
            })
    }
}

/// Initialization stage of sasl negotiation
pub struct Init<Io> {
    frame: protocol::SaslInit,
    framed: Framed<Io, AmqpCodec<SaslFrame>>,
}

impl<Io> fmt::Debug for Init<Io> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SaslInit")
            .field("frame", &self.frame)
            .finish()
    }
}

impl<Io> Init<Io>
where
    Io: AsyncRead + AsyncWrite,
{
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
    pub fn challenge(self) -> impl Future<Item = Response<Io>, Error = ServerError<()>> {
        self.challenge_with(Bytes::new())
    }

    /// Initiate sasl challenge with challenge payload
    pub fn challenge_with(
        self,
        challenge: Bytes,
    ) -> impl Future<Item = Response<Io>, Error = ServerError<()>> {
        let framed = self.framed;
        let frame = SaslChallenge { challenge }.into();

        framed
            .send(frame)
            .map_err(ServerError::from)
            .and_then(move |framed| {
                framed
                    .into_future()
                    .map_err(|res| ServerError::from(res.0))
                    .and_then(|(res, framed)| match res {
                        Some(frame) => match frame.body {
                            SaslFrameBody::SaslResponse(frame) => Ok(Response { frame, framed }),
                            body => Err(ServerError::UnexpectedSaslBodyFrame(body)),
                        },
                        None => Err(ServerError::Disconnected),
                    })
            })
    }

    /// Sasl challenge outcome
    pub fn outcome(
        self,
        code: SaslCode,
    ) -> impl Future<Item = Success<Io>, Error = ServerError<()>> {
        let framed = self.framed;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();

        framed
            .send(frame)
            .map_err(ServerError::from)
            .map(move |framed| Success { framed })
    }
}

pub struct Response<Io> {
    frame: protocol::SaslResponse,
    framed: Framed<Io, AmqpCodec<SaslFrame>>,
}

impl<Io> fmt::Debug for Response<Io> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SaslResponse")
            .field("frame", &self.frame)
            .finish()
    }
}

impl<Io> Response<Io>
where
    Io: AsyncRead + AsyncWrite,
{
    /// Client response payload
    pub fn response(&self) -> &[u8] {
        &self.frame.response[..]
    }

    /// Sasl challenge outcome
    pub fn outcome(
        self,
        code: SaslCode,
    ) -> impl Future<Item = Success<Io>, Error = ServerError<()>> {
        let framed = self.framed;
        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();

        framed
            .send(frame)
            .map_err(ServerError::from)
            .and_then(move |framed| {
                framed
                    .into_future()
                    .map_err(|res| ServerError::from(res.0))
                    .and_then(|(res, framed)| match res {
                        Some(_) => Ok(Success { framed }),
                        None => Err(ServerError::Disconnected),
                    })
            })
    }
}

pub struct Success<Io> {
    framed: Framed<Io, AmqpCodec<SaslFrame>>,
}

impl<Io> Success<Io>
where
    Io: AsyncRead + AsyncWrite,
{
    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        self.framed.get_ref()
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        self.framed.get_mut()
    }

    /// Wait for connection open frame
    pub fn open(self) -> impl Future<Item = ConnectOpened<Io>, Error = ServerError<()>> {
        let framed = self.framed.into_framed(ProtocolIdCodec);

        framed
            .into_future()
            .map_err(|res| ServerError::from(res.0))
            .and_then(|(protocol, framed)| match protocol {
                Some(ProtocolId::Amqp) => {
                    Either::A(
                        // confirm protocol
                        framed
                            .send(ProtocolId::Amqp)
                            .map_err(ServerError::from)
                            .and_then(move |framed| {
                                // Wait for connection open frame
                                let framed = framed.into_framed(AmqpCodec::<AmqpFrame>::new());
                                framed
                                    .into_future()
                                    .map_err(|(err, _)| ServerError::from(err))
                                    .and_then(|(frame, framed)| {
                                        if let Some(frame) = frame {
                                            let frame = frame.into_parts().1;
                                            match frame {
                                                protocol::Frame::Open(frame) => {
                                                    trace!("Got open frame: {:?}", frame);
                                                    Ok(ConnectOpened::new(frame, framed))
                                                }
                                                frame => Err(ServerError::Unexpected(frame)),
                                            }
                                        } else {
                                            Err(ServerError::Disconnected)
                                        }
                                    })
                            }),
                    )
                }
                Some(proto) => Either::B(err(ProtocolIdError::Unexpected {
                    exp: ProtocolId::Amqp,
                    got: proto,
                }
                .into())),
                None => Either::B(err(ProtocolIdError::Disconnected.into())),
            })
    }
}

/// Create service factory with disabled sasl support
pub fn no_sasl<Io, St, E>() -> NoSaslService<Io, St, E> {
    NoSaslService::default()
}

pub struct NoSaslService<Io, St, E>(std::marker::PhantomData<(Io, St, E)>);

impl<Io, St, E> Default for NoSaslService<Io, St, E> {
    fn default() -> Self {
        NoSaslService(std::marker::PhantomData)
    }
}

impl<Io, St, E> NewService for NoSaslService<Io, St, E> {
    type Config = ();
    type Request = Sasl<Io>;
    type Response = ConnectAck<Io, St>;
    type Error = AmqpError;
    type InitError = E;
    type Service = NoSaslService<Io, St, E>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &()) -> Self::Future {
        ok(NoSaslService(std::marker::PhantomData))
    }
}

impl<Io, St, E> Service for NoSaslService<Io, St, E> {
    type Request = Sasl<Io>;
    type Response = ConnectAck<Io, St>;
    type Error = AmqpError;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: Self::Request) -> Self::Future {
        err(AmqpError::not_implemented())
    }
}
