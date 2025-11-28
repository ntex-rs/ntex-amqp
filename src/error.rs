use std::io;

use ntex::util::{ByteString, Either};

pub use crate::codec::protocol::{Error, ErrorInner};
pub use crate::codec::{AmqpCodecError, AmqpParseError, ProtocolIdError};
use crate::{codec::protocol, types::Outcome};

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, thiserror::Error)]
pub enum AmqpDispatcherError {
    #[error("Service error")]
    /// Service error
    Service,
    /// Amqp protocol error
    #[error("Amqp protocol error: {:?}", _0)]
    Protocol(#[from] AmqpProtocolError),
    /// Peer disconnect
    #[error("Peer disconnected error: {:?}", _0)]
    Disconnected(Option<io::Error>),
}

impl Clone for AmqpDispatcherError {
    fn clone(&self) -> Self {
        match self {
            AmqpDispatcherError::Service => AmqpDispatcherError::Service,
            AmqpDispatcherError::Protocol(err) => AmqpDispatcherError::Protocol(err.clone()),
            AmqpDispatcherError::Disconnected(Some(err)) => AmqpDispatcherError::Disconnected(
                Some(io::Error::new(err.kind(), format!("{err}"))),
            ),
            AmqpDispatcherError::Disconnected(None) => AmqpDispatcherError::Disconnected(None),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum AmqpProtocolError {
    #[error("Codec error: {:?}", _0)]
    Codec(#[from] AmqpCodecError),
    #[error("Too many channels")]
    TooManyChannels,
    #[error("Body is too large")]
    BodyTooLarge,
    #[error("Keep-alive timeout")]
    KeepAliveTimeout,
    #[error("Read timeout")]
    ReadTimeout,
    #[error("Disconnected")]
    Disconnected,
    #[error("Unknown session: {:?}", _0)]
    UnknownSession(protocol::Frame),
    #[error("Unknown link in session: {:?}", _0)]
    UnknownLink(protocol::Frame),
    #[error("Connection closed, error: {:?}", _0)]
    Closed(Option<protocol::Error>),
    #[error("Session ended, error: {:?}", _0)]
    SessionEnded(Option<protocol::Error>),
    #[error("Link detached, error: {:?}", _0)]
    LinkDetached(Option<protocol::Error>),
    #[error("Unexpected frame for opening state, got: {:?}", _0)]
    UnexpectedOpeningState(protocol::Frame),
    #[error("Unexpected frame: {:?}", _0)]
    Unexpected(protocol::Frame),
    #[error("Connection is dropped")]
    ConnectionDropped,
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Amqp error: {:?} {:?} ({:?})", err, description, info)]
pub struct AmqpError {
    err: Either<protocol::AmqpError, protocol::ErrorCondition>,
    description: Option<ByteString>,
    info: Option<protocol::FieldsVec>,
}

impl AmqpError {
    pub fn new(err: protocol::AmqpError) -> Self {
        AmqpError {
            err: Either::Left(err),
            description: None,
            info: None,
        }
    }

    pub fn with_error(err: protocol::ErrorCondition) -> Self {
        AmqpError {
            err: Either::Right(err),
            description: None,
            info: None,
        }
    }

    pub fn internal_error() -> Self {
        Self::new(protocol::AmqpError::InternalError)
    }

    pub fn not_found() -> Self {
        Self::new(protocol::AmqpError::NotFound)
    }

    pub fn unauthorized_access() -> Self {
        Self::new(protocol::AmqpError::UnauthorizedAccess)
    }

    pub fn decode_error() -> Self {
        Self::new(protocol::AmqpError::DecodeError)
    }

    pub fn invalid_field() -> Self {
        Self::new(protocol::AmqpError::InvalidField)
    }

    pub fn not_allowed() -> Self {
        Self::new(protocol::AmqpError::NotAllowed)
    }

    pub fn not_implemented() -> Self {
        Self::new(protocol::AmqpError::NotImplemented)
    }

    pub fn text(mut self, text: &'static str) -> Self {
        self.description = Some(ByteString::from_static(text));
        self
    }

    pub fn description<T>(mut self, text: T) -> Self
    where
        ByteString: From<T>,
    {
        self.description = Some(ByteString::from(text));
        self
    }

    pub fn set_description(mut self, text: ByteString) -> Self {
        self.description = Some(text);
        self
    }
}

impl From<AmqpError> for protocol::Error {
    fn from(e: AmqpError) -> protocol::Error {
        let condition = match e.err {
            Either::Left(err) => err.into(),
            Either::Right(err) => err,
        };
        protocol::Error(Box::new(ErrorInner {
            condition,
            description: e.description,
            info: e.info,
        }))
    }
}

impl TryFrom<AmqpError> for Outcome {
    type Error = Error;

    fn try_from(err: AmqpError) -> Result<Self, Error> {
        Ok(Outcome::Error(err.into()))
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Link error: {:?} {:?} ({:?})", err, description, info)]
pub struct LinkError {
    err: Either<protocol::LinkError, protocol::ErrorCondition>,
    description: Option<ByteString>,
    info: Option<protocol::FieldsVec>,
}

impl LinkError {
    pub fn new(error: protocol::ErrorCondition) -> Self {
        LinkError {
            err: Either::Right(error),
            description: None,
            info: None,
        }
    }

    pub fn force_detach() -> Self {
        LinkError {
            err: Either::Left(protocol::LinkError::DetachForced),
            description: None,
            info: None,
        }
    }

    pub fn redirect() -> Self {
        LinkError {
            err: Either::Left(protocol::LinkError::Redirect),
            description: None,
            info: None,
        }
    }

    pub fn text(mut self, text: &'static str) -> Self {
        self.description = Some(ByteString::from_static(text));
        self
    }

    pub fn description<T>(mut self, text: T) -> Self
    where
        ByteString: From<T>,
    {
        self.description = Some(ByteString::from(text));
        self
    }

    pub fn set_description(mut self, text: ByteString) -> Self {
        self.description = Some(text);
        self
    }

    pub fn fields(mut self, fields: protocol::FieldsVec) -> Self {
        self.info = Some(fields);
        self
    }
}

impl From<LinkError> for protocol::Error {
    fn from(e: LinkError) -> protocol::Error {
        let condition = match e.err {
            Either::Left(err) => err.into(),
            Either::Right(err) => err,
        };

        protocol::Error(Box::new(ErrorInner {
            condition,
            description: e.description,
            info: e.info,
        }))
    }
}

impl TryFrom<LinkError> for Outcome {
    type Error = Error;

    fn try_from(err: LinkError) -> Result<Self, Error> {
        Ok(Outcome::Error(err.into()))
    }
}
