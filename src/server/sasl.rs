use std::fmt;

use bytes::Bytes;
use bytestring::ByteString;
use futures::{SinkExt, StreamExt};
use ntex_amqp_codec::protocol::{
    self, ProtocolId, SaslChallenge, SaslCode, SaslFrameBody, SaslMechanisms, SaslOutcome, Symbols,
};
use ntex_amqp_codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, SaslFrame};
use ntex_codec::{AsyncRead, AsyncWrite, Framed};

use super::{handshake::HandshakeAmqpOpened, ServerError};
use crate::connection::ConnectionController;

pub struct Sasl<Io> {
    framed: Framed<Io, ProtocolIdCodec>,
    mechanisms: Symbols,
    controller: ConnectionController,
}

impl<Io> fmt::Debug for Sasl<Io> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("SaslAuth")
            .field("mechanisms", &self.mechanisms)
            .finish()
    }
}

impl<Io> Sasl<Io> {
    pub(crate) fn new(
        framed: Framed<Io, ProtocolIdCodec>,
        controller: ConnectionController,
    ) -> Self {
        Sasl {
            framed,
            controller,
            mechanisms: Symbols::default(),
        }
    }
}

impl<Io> Sasl<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin,
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
        self.mechanisms.push(ByteString::from(symbol.into()).into());
        self
    }

    /// Initialize sasl auth procedure
    pub async fn init(self) -> Result<SaslInit<Io>, ServerError<()>> {
        let Sasl {
            framed,
            mechanisms,
            controller,
            ..
        } = self;

        let mut framed = framed.into_framed(AmqpCodec::<SaslFrame>::new());
        let frame = SaslMechanisms {
            sasl_server_mechanisms: mechanisms,
        }
        .into();

        framed.send(frame).await.map_err(ServerError::from)?;
        let frame = framed
            .next()
            .await
            .ok_or(ServerError::Disconnected)?
            .map_err(ServerError::from)?;

        match frame.body {
            SaslFrameBody::SaslInit(frame) => Ok(SaslInit {
                frame,
                framed,
                controller,
            }),
            body => Err(ServerError::UnexpectedSaslBodyFrame(body)),
        }
    }
}

/// Initialization stage of sasl negotiation
pub struct SaslInit<Io> {
    frame: protocol::SaslInit,
    framed: Framed<Io, AmqpCodec<SaslFrame>>,
    controller: ConnectionController,
}

impl<Io> fmt::Debug for SaslInit<Io> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("SaslInit")
            .field("frame", &self.frame)
            .finish()
    }
}

impl<Io> SaslInit<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin,
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

    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        self.framed.get_ref()
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        self.framed.get_mut()
    }

    /// Initiate sasl challenge
    pub async fn challenge(self) -> Result<SaslResponse<Io>, ServerError<()>> {
        self.challenge_with(Bytes::new()).await
    }

    /// Initiate sasl challenge with challenge payload
    pub async fn challenge_with(
        self,
        challenge: Bytes,
    ) -> Result<SaslResponse<Io>, ServerError<()>> {
        let mut framed = self.framed;
        let controller = self.controller;
        let frame = SaslChallenge { challenge }.into();

        framed.send(frame).await.map_err(ServerError::from)?;
        let frame = framed
            .next()
            .await
            .ok_or(ServerError::Disconnected)?
            .map_err(ServerError::from)?;

        match frame.body {
            SaslFrameBody::SaslResponse(frame) => Ok(SaslResponse {
                frame,
                framed,
                controller,
            }),
            body => Err(ServerError::UnexpectedSaslBodyFrame(body)),
        }
    }

    /// Sasl challenge outcome
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<Io>, ServerError<()>> {
        let mut framed = self.framed;
        let controller = self.controller;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();
        framed.send(frame).await.map_err(ServerError::from)?;

        Ok(SaslSuccess { framed, controller })
    }
}

pub struct SaslResponse<Io> {
    frame: protocol::SaslResponse,
    framed: Framed<Io, AmqpCodec<SaslFrame>>,
    controller: ConnectionController,
}

impl<Io> fmt::Debug for SaslResponse<Io> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("SaslResponse")
            .field("frame", &self.frame)
            .finish()
    }
}

impl<Io> SaslResponse<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    /// Client response payload
    pub fn response(&self) -> &[u8] {
        &self.frame.response[..]
    }

    /// Sasl challenge outcome
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<Io>, ServerError<()>> {
        let mut framed = self.framed;
        let controller = self.controller;
        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();

        framed.send(frame).await.map_err(ServerError::from)?;
        framed
            .next()
            .await
            .ok_or(ServerError::Disconnected)?
            .map_err(ServerError::from)?;

        Ok(SaslSuccess { framed, controller })
    }
}

pub struct SaslSuccess<Io> {
    framed: Framed<Io, AmqpCodec<SaslFrame>>,
    controller: ConnectionController,
}

impl<Io> SaslSuccess<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin,
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
    pub async fn open(self) -> Result<HandshakeAmqpOpened<Io>, ServerError<()>> {
        let mut framed = self.framed.into_framed(ProtocolIdCodec);
        let mut controller = self.controller;

        let protocol = framed
            .next()
            .await
            .ok_or_else(|| ServerError::Disconnected)?
            .map_err(ServerError::from)?;

        match protocol {
            ProtocolId::Amqp => {
                // confirm protocol
                framed
                    .send(ProtocolId::Amqp)
                    .await
                    .map_err(ServerError::from)?;

                // Wait for connection open frame
                let mut framed = framed.into_framed(AmqpCodec::<AmqpFrame>::new());
                let frame = framed
                    .next()
                    .await
                    .ok_or(ServerError::Disconnected)?
                    .map_err(ServerError::from)?;

                let frame = frame.into_parts().1;
                match frame {
                    protocol::Frame::Open(frame) => {
                        trace!("Got open frame: {:?}", frame);
                        controller.set_remote((&frame).into());
                        Ok(HandshakeAmqpOpened::new(frame, framed, controller))
                    }
                    frame => Err(ServerError::Unexpected(Box::new(frame))),
                }
            }
            proto => Err(ProtocolIdError::Unexpected {
                exp: ProtocolId::Amqp,
                got: proto,
            }
            .into()),
        }
    }
}
