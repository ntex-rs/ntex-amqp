use std::{convert::TryFrom, io};

use ntex::util::{ByteString, Either};

pub use crate::codec::protocol::Error;
pub use crate::codec::{AmqpCodecError, AmqpParseError, ProtocolIdError};
use crate::{codec::protocol, types::Outcome};

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, Display, From)]
pub enum DispatcherError {
    #[display(fmt = "Service error")]
    /// Service error
    Service,
    /// Amqp codec error
    #[display(fmt = "Amqp codec error: {:?}", _0)]
    Codec(AmqpCodecError),
    /// Amqp protocol error
    #[display(fmt = "Amqp protocol error: {:?}", _0)]
    Protocol(AmqpProtocolError),
    /// Peer disconnect
    Disconnected,
    /// Unexpected io error
    Io(io::Error),
}

impl From<Either<AmqpCodecError, io::Error>> for DispatcherError {
    fn from(err: Either<AmqpCodecError, io::Error>) -> Self {
        match err {
            Either::Left(err) => DispatcherError::Codec(err),
            Either::Right(err) => DispatcherError::Io(err),
        }
    }
}

#[derive(Clone, Debug, Display)]
pub enum AmqpProtocolError {
    Codec(AmqpCodecError),
    TooManyChannels,
    KeepAliveTimeout,
    Disconnected,
    #[display(fmt = "Unknown session: {} {:?}", _0, _1)]
    UnknownSession(usize, Box<protocol::Frame>),
    #[display(fmt = "Connection closed, error: {:?}", _0)]
    Closed(Option<protocol::Error>),
    #[display(fmt = "Session ended, error: {:?}", _0)]
    SessionEnded(Option<protocol::Error>),
    #[display(fmt = "Link detached, error: {:?}", _0)]
    LinkDetached(Option<protocol::Error>),
    #[display(fmt = "Unexpected frame for opening state, got: {:?}", _0)]
    UnexpectedOpeningState(Box<protocol::Frame>),
    #[display(fmt = "Unexpected frame, got: {:?}", _0)]
    Unexpected(Box<protocol::Frame>),
}

impl From<AmqpCodecError> for AmqpProtocolError {
    fn from(err: AmqpCodecError) -> Self {
        AmqpProtocolError::Codec(err)
    }
}

#[derive(Debug, Display)]
#[display(fmt = "Amqp error: {:?} {:?} ({:?})", err, description, info)]
pub struct AmqpError {
    err: Either<protocol::AmqpError, protocol::ErrorCondition>,
    description: Option<ByteString>,
    info: Option<protocol::Fields>,
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

    pub fn description<T: AsRef<str>>(mut self, text: T) -> Self {
        self.description = Some(ByteString::from(text.as_ref()));
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
        protocol::Error {
            condition,
            description: e.description,
            info: e.info,
        }
    }
}

impl TryFrom<AmqpError> for Outcome {
    type Error = Error;

    fn try_from(err: AmqpError) -> Result<Self, Error> {
        Ok(Outcome::Error(err.into()))
    }
}

#[derive(Debug, Display)]
#[display(fmt = "Link error: {:?} {:?} ({:?})", err, description, info)]
pub struct LinkError {
    err: Either<protocol::LinkError, protocol::ErrorCondition>,
    description: Option<ByteString>,
    info: Option<protocol::Fields>,
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

    pub fn description<T: AsRef<str>>(mut self, text: T) -> Self {
        self.description = Some(ByteString::from(text.as_ref()));
        self
    }

    pub fn set_description(mut self, text: ByteString) -> Self {
        self.description = Some(text);
        self
    }

    #[allow(clippy::mutable_key_type)]
    pub fn fields(mut self, fields: protocol::Fields) -> Self {
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

        protocol::Error {
            condition,
            description: e.description,
            info: e.info,
        }
    }
}

impl TryFrom<LinkError> for Outcome {
    type Error = Error;

    fn try_from(err: LinkError) -> Result<Self, Error> {
        Ok(Outcome::Error(err.into()))
    }
}
