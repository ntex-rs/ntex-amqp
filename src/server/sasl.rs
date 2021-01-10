use std::{fmt, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex_amqp_codec::protocol::{
    self, ProtocolId, SaslChallenge, SaslCode, SaslFrameBody, SaslMechanisms, SaslOutcome, Symbols,
};
use ntex_amqp_codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, SaslFrame};

use super::{handshake::HandshakeAmqpOpened, ServerError};
use crate::{connection::Connection, error::AmqpProtocolError, io::IoState, Configuration};

pub struct Sasl<Io> {
    io: Io,
    state: IoState<AmqpCodec<SaslFrame>>,
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
    pub(crate) fn new(
        io: Io,
        state: IoState<AmqpCodec<SaslFrame>>,
        local_config: Rc<Configuration>,
    ) -> Self {
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
    pub async fn init(self) -> Result<SaslInit<Io>, ServerError<()>> {
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

        state
            .send(&mut io, frame)
            .await
            .map_err(ServerError::from)?;
        let frame = state
            .next(&mut io)
            .await
            .map_err(ServerError::from)?
            .ok_or(ServerError::Disconnected)?;

        match frame.body {
            SaslFrameBody::SaslInit(frame) => Ok(SaslInit {
                frame,
                io,
                state,
                local_config,
            }),
            body => Err(ServerError::UnexpectedSaslBodyFrame(body)),
        }
    }
}

/// Initialization stage of sasl negotiation
pub struct SaslInit<Io> {
    frame: protocol::SaslInit,
    io: Io,
    state: IoState<AmqpCodec<SaslFrame>>,
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
        self.frame.initial_response.as_ref().map(|b| b.as_ref())
    }

    /// Sasl initial response
    pub fn hostname(&self) -> Option<&str> {
        self.frame.hostname.as_ref().map(|b| b.as_ref())
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
    pub async fn challenge(self) -> Result<SaslResponse<Io>, ServerError<()>> {
        self.challenge_with(Bytes::new()).await
    }

    /// Initiate sasl challenge with challenge payload
    pub async fn challenge_with(
        self,
        challenge: Bytes,
    ) -> Result<SaslResponse<Io>, ServerError<()>> {
        let mut io = self.io;
        let state = self.state;
        let local_config = self.local_config;
        let frame = SaslChallenge { challenge }.into();

        state
            .send(&mut io, frame)
            .await
            .map_err(ServerError::from)?;
        let frame = state
            .next(&mut io)
            .await
            .map_err(ServerError::from)?
            .ok_or(ServerError::Disconnected)?;

        match frame.body {
            SaslFrameBody::SaslResponse(frame) => Ok(SaslResponse {
                frame,
                io,
                state,
                local_config,
            }),
            body => Err(ServerError::UnexpectedSaslBodyFrame(body)),
        }
    }

    /// Sasl challenge outcome
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<Io>, ServerError<()>> {
        let mut io = self.io;
        let state = self.state;
        let local_config = self.local_config;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();
        state
            .send(&mut io, frame)
            .await
            .map_err(ServerError::from)?;

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
    state: IoState<AmqpCodec<SaslFrame>>,
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
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<Io>, ServerError<()>> {
        let mut io = self.io;
        let state = self.state;
        let local_config = self.local_config;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();
        state
            .send(&mut io, frame)
            .await
            .map_err(ServerError::from)?;
        state
            .next(&mut io)
            .await
            .map_err(ServerError::from)?
            .ok_or(ServerError::Disconnected)?;

        Ok(SaslSuccess {
            io,
            state,
            local_config,
        })
    }
}

pub struct SaslSuccess<Io> {
    io: Io,
    state: IoState<AmqpCodec<SaslFrame>>,
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
    pub async fn open(self) -> Result<HandshakeAmqpOpened<Io>, ServerError<()>> {
        let mut io = self.io;
        let state = self.state.map_codec(|_| ProtocolIdCodec);

        let protocol = state
            .next(&mut io)
            .await
            .map_err(ServerError::from)?
            .ok_or(ServerError::Disconnected)?;

        match protocol {
            ProtocolId::Amqp => {
                // confirm protocol
                state
                    .send(&mut io, ProtocolId::Amqp)
                    .await
                    .map_err(ServerError::from)?;

                // Wait for connection open frame
                let state = state.map_codec(|_| AmqpCodec::<AmqpFrame>::new());
                let frame = state
                    .next(&mut io)
                    .await
                    .map_err(ServerError::from)?
                    .ok_or(ServerError::Disconnected)?;

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
                    frame => Err(AmqpProtocolError::Unexpected(Box::new(frame)).into()),
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
