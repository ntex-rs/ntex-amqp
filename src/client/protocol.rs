use std::marker::PhantomData;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::Service;
use futures::{Async, Future, Poll, Sink, Stream};

use amqp_codec::protocol::ProtocolId;
use amqp_codec::{ProtocolIdCodec, ProtocolIdError};

pub struct ProtocolNegotiation<Io> {
    proto: ProtocolId,
    _r: PhantomData<Io>,
}

impl<Io> Clone for ProtocolNegotiation<Io> {
    fn clone(&self) -> Self {
        ProtocolNegotiation {
            proto: self.proto.clone(),
            _r: PhantomData,
        }
    }
}

impl<Io> ProtocolNegotiation<Io> {
    pub fn new(proto: ProtocolId) -> Self {
        ProtocolNegotiation {
            proto,
            _r: PhantomData,
        }
    }

    pub fn framed(stream: Io) -> Framed<Io, ProtocolIdCodec>
    where
        Io: AsyncRead + AsyncWrite,
    {
        Framed::new(stream, ProtocolIdCodec)
    }
}

impl<Io> Default for ProtocolNegotiation<Io> {
    fn default() -> Self {
        Self::new(ProtocolId::Amqp)
    }
}

impl<Io> Service<Framed<Io, ProtocolIdCodec>> for ProtocolNegotiation<Io>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    type Response = Framed<Io, ProtocolIdCodec>;
    type Error = ProtocolIdError;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Framed<Io, ProtocolIdCodec>) -> Self::Future {
        let proto = self.proto;

        Box::new(
            req.send(proto)
                .from_err()
                .and_then(|framed| framed.into_future().map_err(|e| e.0))
                .and_then(move |(protocol, framed)| {
                    if let Some(protocol) = protocol {
                        if proto == protocol {
                            Ok(framed)
                        } else {
                            Err(ProtocolIdError::Unexpected {
                                exp: proto,
                                got: protocol,
                            })
                        }
                    } else {
                        Err(ProtocolIdError::Disconnected)
                    }
                }),
        )
    }
}
