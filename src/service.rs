use std::marker::PhantomData;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::{NewService, Service};
use futures::future::{ok, FutureResult};
use futures::{Async, Future, Poll, Sink, Stream};

use amqp::protocol::ProtocolId;
use amqp::{ProtocolIdCodec, ProtocolIdError};

pub struct ProtocolNegotiation<T> {
    proto: ProtocolId,
    _r: PhantomData<T>,
}

impl<T> Clone for ProtocolNegotiation<T> {
    fn clone(&self) -> Self {
        ProtocolNegotiation {
            proto: self.proto.clone(),
            _r: PhantomData,
        }
    }
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
                    println!("PROTO: {:?}", protocol);
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

pub struct ServerProtocolNegotiation<T> {
    proto: ProtocolId,
    _r: PhantomData<T>,
}

impl<T> Clone for ServerProtocolNegotiation<T> {
    fn clone(&self) -> Self {
        ServerProtocolNegotiation {
            proto: self.proto.clone(),
            _r: PhantomData,
        }
    }
}

impl<T> ServerProtocolNegotiation<T> {
    pub fn new(proto: ProtocolId) -> Self {
        ServerProtocolNegotiation {
            proto,
            _r: PhantomData,
        }
    }
}

impl<T> NewService<T> for ServerProtocolNegotiation<T>
where
    T: AsyncRead + AsyncWrite + 'static,
{
    type Response = Framed<T, ProtocolIdCodec>;
    type Error = ProtocolIdError;
    type InitError = ();
    type Service = ServerProtocolNegotiationImpl;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self) -> Self::Future {
        ok(ServerProtocolNegotiationImpl {
            proto: self.proto.clone(),
        })
    }
}

pub struct ServerProtocolNegotiationImpl {
    proto: ProtocolId,
}

impl<T> Service<T> for ServerProtocolNegotiationImpl
where
    T: AsyncRead + AsyncWrite + 'static,
{
    type Response = Framed<T, ProtocolIdCodec>;
    type Error = ProtocolIdError;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: T) -> Self::Future {
        let framed = Framed::new(req, ProtocolIdCodec);
        let proto = self.proto;

        Box::new(
            framed
                .into_future()
                .map_err(|e| e.0)
                .and_then(move |(protocol, framed)| {
                    println!("PROTO: {:?}", protocol);
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
                })
                .and_then(move |framed| framed.send(proto).from_err()),
        )
    }
}
