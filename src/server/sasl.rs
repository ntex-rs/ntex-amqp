use std::fmt;

use ntex_bytes::{ByteString, Bytes};
use ntex_io::IoBoxed;
use ntex_service::cfg::Cfg;

use crate::codec::protocol::{
    self, ProtocolId, SaslChallenge, SaslCode, SaslFrameBody, SaslMechanisms, SaslOutcome, Symbols,
};
use crate::codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, ProtocolIdError, SaslFrame};
use crate::{AmqpServiceConfig, RemoteServiceConfig, connection::Connection};

use super::{HandshakeError, handshake::HandshakeAmqpOpened};

#[derive(Debug)]
pub struct Sasl<St = ()> {
    st: St,
    io: IoBoxed,
    mechanisms: Symbols,
    local_config: Cfg<AmqpServiceConfig>,
}

impl<St> Sasl<St> {
    pub(crate) fn new(st: St, io: IoBoxed, local_config: Cfg<AmqpServiceConfig>) -> Self {
        Sasl {
            st,
            io,
            local_config,
            mechanisms: Symbols::default(),
        }
    }
}

impl<St> Sasl<St> {
    /// Returns reference to state object
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    #[must_use]
    /// Add supported sasl mechanism
    pub fn mechanism<U: Into<String>>(mut self, symbol: U) -> Self {
        self.mechanisms.push(ByteString::from(symbol.into()).into());
        self
    }

    /// Initialize sasl auth procedure
    pub async fn init(self) -> Result<SaslInit<St>, HandshakeError> {
        let Sasl {
            st,
            io,
            mechanisms,
            local_config,
            ..
        } = self;

        let frame = SaslMechanisms {
            sasl_server_mechanisms: mechanisms,
        }
        .into();

        let codec = AmqpCodec::<SaslFrame>::new();
        io.send(frame, &codec).await.map_err(HandshakeError::from)?;
        let frame = io
            .recv(&codec)
            .await?
            .ok_or(HandshakeError::Disconnected(None))?;

        match frame.body {
            SaslFrameBody::SaslInit(frame) => Ok(SaslInit {
                st,
                io,
                frame,
                codec,
                local_config,
            }),
            body => Err(HandshakeError::UnexpectedSaslBodyFrame(Box::new(body))),
        }
    }
}

/// Initialization stage of sasl negotiation
pub struct SaslInit<St> {
    st: St,
    io: IoBoxed,
    frame: protocol::SaslInit,
    codec: AmqpCodec<SaslFrame>,
    local_config: Cfg<AmqpServiceConfig>,
}

impl<St> fmt::Debug for SaslInit<St> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("SaslInit")
            .field("frame", &self.frame)
            .finish()
    }
}

impl<St> SaslInit<St> {
    /// Returns reference to state object
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

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

    /// Initiate sasl challenge
    pub async fn challenge(self) -> Result<SaslResponse<St>, HandshakeError> {
        self.challenge_with(Bytes::new()).await
    }

    /// Initiate sasl challenge with challenge payload
    pub async fn challenge_with(
        self,
        challenge: Bytes,
    ) -> Result<SaslResponse<St>, HandshakeError> {
        let SaslInit {
            st,
            io,
            codec,
            local_config,
            frame: _,
        } = self;

        let frame = SaslChallenge { challenge }.into();

        io.send(frame, &codec).await.map_err(HandshakeError::from)?;
        let frame = io
            .recv(&codec)
            .await?
            .ok_or(HandshakeError::Disconnected(None))?;

        match frame.body {
            SaslFrameBody::SaslResponse(frame) => Ok(SaslResponse {
                st,
                io,
                frame,
                codec,
                local_config,
            }),
            body => Err(HandshakeError::UnexpectedSaslBodyFrame(Box::new(body))),
        }
    }

    /// Sasl challenge outcome
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<St>, HandshakeError> {
        let SaslInit {
            st,
            io,
            codec,
            local_config,
            frame: _,
        } = self;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();
        io.send(frame, &codec).await.map_err(HandshakeError::from)?;

        Ok(SaslSuccess {
            st,
            io,
            local_config,
        })
    }
}

pub struct SaslResponse<St> {
    st: St,
    io: IoBoxed,
    frame: protocol::SaslResponse,
    codec: AmqpCodec<SaslFrame>,
    local_config: Cfg<AmqpServiceConfig>,
}

impl<St> fmt::Debug for SaslResponse<St> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("SaslResponse")
            .field("frame", &self.frame)
            .finish()
    }
}

impl<St> SaslResponse<St> {
    /// Returns reference to state object
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    /// Client response payload
    pub fn response(&self) -> &[u8] {
        &self.frame.response[..]
    }

    /// Sasl challenge outcome
    pub async fn outcome(self, code: SaslCode) -> Result<SaslSuccess<St>, HandshakeError> {
        let SaslResponse {
            st,
            io,
            codec,
            local_config,
            frame: _,
        } = self;

        let frame = SaslOutcome {
            code,
            additional_data: None,
        }
        .into();
        io.send(frame, &codec).await.map_err(HandshakeError::from)?;
        io.recv(&codec)
            .await?
            .ok_or(HandshakeError::Disconnected(None))?;

        Ok(SaslSuccess {
            st,
            io,
            local_config,
        })
    }
}

pub struct SaslSuccess<St> {
    st: St,
    io: IoBoxed,
    local_config: Cfg<AmqpServiceConfig>,
}

impl<St> SaslSuccess<St> {
    /// Returns reference to state object
    pub fn st(&self) -> &St {
        &self.st
    }

    /// Returns reference to io object
    pub fn io(&self) -> &IoBoxed {
        &self.io
    }

    /// Wait for connection open frame
    pub async fn open(self) -> Result<HandshakeAmqpOpened<St>, HandshakeError> {
        let SaslSuccess {
            st,
            io,
            local_config: _,
        } = self;

        let protocol = io
            .recv(&ProtocolIdCodec)
            .await?
            .ok_or(HandshakeError::Disconnected(None))?;

        match protocol {
            ProtocolId::Amqp => {
                // confirm protocol
                io.send(ProtocolId::Amqp, &ProtocolIdCodec)
                    .await
                    .map_err(HandshakeError::from)?;

                // Wait for connection open frame
                let codec = AmqpCodec::<AmqpFrame>::new();
                let frame = io
                    .recv(&codec)
                    .await?
                    .ok_or(HandshakeError::Disconnected(None))?;

                let frame = frame.into_parts().1;
                match frame {
                    protocol::Frame::Open(frame) => {
                        log::trace!("{}: Got open frame: {:?}", io.tag(), frame);

                        let local_config = self.local_config;
                        let remote_config = RemoteServiceConfig::new(&frame);
                        let sink = Connection::new(io.clone(), &local_config, &remote_config);

                        Ok(HandshakeAmqpOpened::new(
                            st,
                            io,
                            frame,
                            sink,
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
