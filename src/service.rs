use std::marker::PhantomData;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::Service;
use futures::{Async, Future, Poll, Sink, Stream};

use amqp::errors::ProtocolIdError;
use amqp::protocol::ProtocolId;
use amqp::ProtocolIdCodec;

pub(crate) struct ProtocolNegotiation<T> {
    proto: ProtocolId,
    _r: PhantomData<T>,
}

impl<T> ProtocolNegotiation<T> {
    pub fn new(proto: ProtocolId) -> Self {
        ProtocolNegotiation {
            proto,
            _r: PhantomData,
        }
    }
}

impl<T> Service<Framed<T, ProtocolIdCodec>> for ProtocolNegotiation<T>
where
    T: AsyncRead + AsyncWrite + 'static,
{
    type Response = Framed<T, ProtocolIdCodec>;
    type Error = ProtocolIdError;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Framed<T, ProtocolIdCodec>) -> Self::Future {
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
