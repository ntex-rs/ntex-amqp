use std::io;

use bytestring::ByteString;
use either::Either;

use crate::codec::protocol;
pub use crate::codec::protocol::Error;
pub use crate::codec::{AmqpCodecError, AmqpParseError, ProtocolIdError};

/// Errors which can occur when attempting to handle amqp connection.
#[derive(Debug, Display)]
pub enum AmqpServiceError<E> {
    #[display(fmt = "Message handler service error")]
    /// Message handler service error
    Service(E),
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

impl<E> Into<protocol::Error> for AmqpServiceError<E> {
    fn into(self) -> protocol::Error {
        protocol::Error {
            condition: protocol::AmqpError::InternalError.into(),
            description: Some(ByteString::from(format!("{}", self))),
            info: None,
        }
    }
}

impl<E> From<AmqpCodecError> for AmqpServiceError<E> {
    fn from(err: AmqpCodecError) -> Self {
        AmqpServiceError::Codec(err)
    }
}

impl<E> From<AmqpProtocolError> for AmqpServiceError<E> {
    fn from(err: AmqpProtocolError) -> Self {
        AmqpServiceError::Protocol(err)
    }
}

impl<E> From<io::Error> for AmqpServiceError<E> {
    fn from(err: io::Error) -> Self {
        AmqpServiceError::Io(err)
    }
}

impl<E> From<Either<AmqpCodecError, io::Error>> for AmqpServiceError<E> {
    fn from(err: Either<AmqpCodecError, io::Error>) -> Self {
        match err {
            Either::Left(err) => AmqpServiceError::Codec(err),
            Either::Right(err) => AmqpServiceError::Io(err),
        }
    }
}

#[derive(Debug, Display)]
pub enum AmqpProtocolError {
    Codec(AmqpCodecError),
    TooManyChannels,
    Disconnected,
    Timeout,
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
    /// Unexpected io error
    #[display(fmt = "Unexpected io error: {:?}", _0)]
    Io(Option<io::Error>),
}

impl Clone for AmqpProtocolError {
    fn clone(&self) -> Self {
        match self {
            AmqpProtocolError::Codec(err) => AmqpProtocolError::Codec(err.clone()),
            AmqpProtocolError::TooManyChannels => AmqpProtocolError::TooManyChannels,
            AmqpProtocolError::Disconnected => AmqpProtocolError::Disconnected,
            AmqpProtocolError::Timeout => AmqpProtocolError::Timeout,
            AmqpProtocolError::UnknownSession(id, err) => {
                AmqpProtocolError::UnknownSession(*id, err.clone())
            }
            AmqpProtocolError::Closed(err) => AmqpProtocolError::Closed(err.clone()),
            AmqpProtocolError::SessionEnded(err) => AmqpProtocolError::SessionEnded(err.clone()),
            AmqpProtocolError::LinkDetached(err) => AmqpProtocolError::LinkDetached(err.clone()),
            AmqpProtocolError::UnexpectedOpeningState(err) => {
                AmqpProtocolError::UnexpectedOpeningState(err.clone())
            }
            AmqpProtocolError::Unexpected(err) => AmqpProtocolError::Unexpected(err.clone()),
            AmqpProtocolError::Io(_) => AmqpProtocolError::Io(None),
        }
    }
}

impl From<AmqpCodecError> for AmqpProtocolError {
    fn from(err: AmqpCodecError) -> Self {
        AmqpProtocolError::Codec(err)
    }
}

impl From<io::Error> for AmqpProtocolError {
    fn from(err: io::Error) -> Self {
        AmqpProtocolError::Io(Some(err))
    }
}

// #[derive(Debug, Display)]
// #[display(fmt = "Amqp error: {:?} {:?} ({:?})", err, description, info)]
// pub struct AmqpError {
//     err: Either<protocol::AmqpError, protocol::ErrorCondition>,
//     description: Option<ByteString>,
//     info: Option<protocol::Fields>,
// }

// impl AmqpError {
//     pub fn new(err: protocol::AmqpError) -> Self {
//         AmqpError {
//             err: Either::Left(err),
//             description: None,
//             info: None,
//         }
//     }

//     pub fn with_error(err: protocol::ErrorCondition) -> Self {
//         AmqpError {
//             err: Either::Right(err),
//             description: None,
//             info: None,
//         }
//     }

//     pub fn internal_error() -> Self {
//         Self::new(protocol::AmqpError::InternalError)
//     }

//     pub fn not_found() -> Self {
//         Self::new(protocol::AmqpError::NotFound)
//     }

//     pub fn unauthorized_access() -> Self {
//         Self::new(protocol::AmqpError::UnauthorizedAccess)
//     }

//     pub fn decode_error() -> Self {
//         Self::new(protocol::AmqpError::DecodeError)
//     }

//     pub fn invalid_field() -> Self {
//         Self::new(protocol::AmqpError::InvalidField)
//     }

//     pub fn not_allowed() -> Self {
//         Self::new(protocol::AmqpError::NotAllowed)
//     }

//     pub fn not_implemented() -> Self {
//         Self::new(protocol::AmqpError::NotImplemented)
//     }

//     pub fn description<T: AsRef<str>>(mut self, text: T) -> Self {
//         self.description = Some(ByteString::from(text.as_ref()));
//         self
//     }

//     pub fn set_description(mut self, text: ByteString) -> Self {
//         self.description = Some(text);
//         self
//     }
// }

// impl Into<protocol::Error> for AmqpError {
//     fn into(self) -> protocol::Error {
//         let condition = match self.err {
//             Either::Left(err) => err.into(),
//             Either::Right(err) => err,
//         };
//         protocol::Error {
//             condition,
//             description: self.description,
//             info: self.info,
//         }
//     }
// }

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

    //     pub fn unauthorized_access() -> Self {
    //         Self::new(protocol::AmqpError::UnauthorizedAccess)
    //     }

    //     pub fn decode_error() -> Self {
    //         Self::new(protocol::AmqpError::DecodeError)
    //     }

    //     pub fn invalid_field() -> Self {
    //         Self::new(protocol::AmqpError::InvalidField)
    //     }

    //     pub fn not_allowed() -> Self {
    //         Self::new(protocol::AmqpError::NotAllowed)
    //     }

    pub fn not_implemented() -> Self {
        Self::new(protocol::AmqpError::NotImplemented.into())
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

impl Into<protocol::Error> for LinkError {
    fn into(self) -> protocol::Error {
        let condition = match self.err {
            Either::Left(err) => err.into(),
            Either::Right(err) => err,
        };

        protocol::Error {
            condition,
            description: self.description,
            info: self.info,
        }
    }
}

impl From<LinkError> for std::convert::Infallible {
    fn from(_: LinkError) -> Self {
        unreachable!()
    }
}
