use std::{fmt, rc::Rc};

use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::framed::State;
use ntex::util::{ByteString, Bytes};

use crate::codec::protocol::{
    self, ProtocolId, SaslChallenge, SaslCode, SaslFrameBody, SaslMechanisms, SaslOutcome, Symbols,
};
use crate::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, SaslFrame};

use super::{handshake::HandshakeAmqpOpened, HandshakeError};
use crate::{connection::Connection, Configuration};

pub struct Sasl<Io> {
    io: Io,
    state: State,
    mechanisms: Symbols,
    local_config: Rc<Configuration>,
}

impl<Io> fmt::Debug for Sasl<Io> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("SaslAuth")
            .field("mechanisms", &self.mechanisms)
            .finish()
    }
}

impl<Io> Sasl<Io> {
    pub(crate) fn new(io: Io, state: State, local_config: Rc<Configuration>) -> Self {
        Sasl {
            io,
            state,
            local_config,
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
        &self.io
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        &mut self.io
    }

    /// Add supported sasl mechanism
    pub fn mechanism<U: Into<String>>(mut self, symbol: U) -> Self {
        self.mechanisms.push(ByteString::from(symbol.into()).into());
        self
    }

    /// Initialize sasl auth procedure
    pub async fn init(self) -> Result<SaslInit<Io>, HandshakeError> {
        let Sasl {
            mut io,
            state,
            mechanisms,
            local_config,
            ..
        } = self;

        let frame = SaslMechanisms {
            sasl_server_mechanisms: mechanisms,
        }
        .into();

        let codec = AmqpCodec::<SaslFrame>::new();
        state
            .send(&mut io, &codec, frame)
            .await
            .map_err(HandshakeError::from)?;
        let frame = state
            .next(&mut io, &codec)
            .await
            .map_err(HandshakeError::from)?
            .ok_or(HandshakeError::Disconnected)?;

        match frame.body {
            SaslFrameBody::SaslInit(frame) => Ok(SaslInit {
                frame,
                io,
                state,
                codec,
                local_config,
            }),
            body => Err(HandshakeError::UnexpectedSaslBodyFrame(Box::new(body))),
        }
    }
}

/// Initialization stage of sasl negotiation
pub struct SaslInit<Io> {
    frame: protocol::SaslInit,
    io: Io,
    state: State,
    codec: AmqpCodec<SaslFrame>,
    local_config: Rc<Configuration>,
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
        self.frame.initial_response.as_ref().map(AsRef::as_ref)
    }

    /// Sasl initial response
    pub fn hostname(&self) -> Option<&str> {
        self.frame.hostname.as_ref().map(AsRef::as_ref)
    }

    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        &self.io
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        &mut self.io
    }

    /// Initiate sasl challenge
    pub async fn challenge(self) -> Result<SaslResponse<Io>, HandshakeError> {
        self.challenge_with(Bytes::new()).await
    }

    /// Initiate sasl challenge with challenge payload
    pub async fn challenge_with(
        self,
        challenge: Bytes,
    ) -> Result<SaslResponse<Io>, HandshakeError> {
        let mut io = self.io;
        let state = self.state;
        let codec = self.codec;
        let local_config = self.local_config;
        let frame = SaslChallenge { challenge }.into();

        state
            .send(&mut io, &codec, frame)
            .await
            .map_err(HandshakeError::from)?;
        let frame = state
            .next(&mut io, &codec)
            .await
            .map_err(HandshakeError::from)?
            .ok_or(HandshakeError::Disconnected)?;

        match frame.body {
            SaslFrameBody::SaslResponse(frame) => Ok(SaslResponse {
                frame,
                io,
                state,
                codec,
                local_config,
            }),
            body => Err(HandshakeError::UnexpectedSaslBodyFrame(Box::new(body))),
        }
    }

    /// Sasl challenge outcome
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<Io>, HandshakeError> {
        let mut io = self.io;
        let state = self.state;
        let codec = self.codec;
        let local_config = self.local_config;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();
        state
            .send(&mut io, &codec, frame)
            .await
            .map_err(HandshakeError::from)?;

        Ok(SaslSuccess {
            io,
            state,
            local_config,
        })
    }
}

pub struct SaslResponse<Io> {
    frame: protocol::SaslResponse,
    io: Io,
    state: State,
    codec: AmqpCodec<SaslFrame>,
    local_config: Rc<Configuration>,
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
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<Io>, HandshakeError> {
        let mut io = self.io;
        let state = self.state;
        let codec = self.codec;
        let local_config = self.local_config;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();
        state
            .send(&mut io, &codec, frame)
            .await
            .map_err(HandshakeError::from)?;
        state
            .next(&mut io, &codec)
            .await
            .map_err(HandshakeError::from)?
            .ok_or(HandshakeError::Disconnected)?;

        Ok(SaslSuccess {
            io,
            state,
            local_config,
        })
    }
}

pub struct SaslSuccess<Io> {
    io: Io,
    state: State,
    local_config: Rc<Configuration>,
}

impl<Io> SaslSuccess<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    /// Returns reference to io object
    pub fn get_ref(&self) -> &Io {
        &self.io
    }

    /// Returns mutable reference to io object
    pub fn get_mut(&mut self) -> &mut Io {
        &mut self.io
    }

    /// Wait for connection open frame
    pub async fn open(self) -> Result<HandshakeAmqpOpened<Io>, HandshakeError> {
        let mut io = self.io;
        let state = self.state;

        let protocol = state
            .next(&mut io, &ProtocolIdCodec)
            .await
            .map_err(HandshakeError::from)?
            .ok_or(HandshakeError::Disconnected)?;

        match protocol {
            ProtocolId::Amqp => {
                // confirm protocol
                state
                    .send(&mut io, &ProtocolIdCodec, ProtocolId::Amqp)
                    .await
                    .map_err(HandshakeError::from)?;

                // Wait for connection open frame
                let codec = AmqpCodec::<AmqpFrame>::new();
                let frame = state
                    .next(&mut io, &codec)
                    .await
                    .map_err(HandshakeError::from)?
                    .ok_or(HandshakeError::Disconnected)?;

                let frame = frame.into_parts().1;
                match frame {
                    protocol::Frame::Open(frame) => {
                        trace!("Got open frame: {:?}", frame);

                        let local_config = self.local_config;
                        let remote_config = (&frame).into();
                        let sink = Connection::new(state.clone(), &local_config, &remote_config);

                        Ok(HandshakeAmqpOpened::new(
                            frame,
                            io,
                            sink,
                            state,
                            local_config,
                            remote_config,
                        ))
                    }
                    frame => Err(HandshakeError::Unexpected(frame)),
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
