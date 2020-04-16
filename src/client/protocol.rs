use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Future, SinkExt, StreamExt};
use ntex::codec::{AsyncRead, AsyncWrite, Framed};
use ntex::service::Service;

use ntex_amqp_codec::protocol::ProtocolId;
use ntex_amqp_codec::{ProtocolIdCodec, ProtocolIdError};

pub struct ProtocolNegotiation<Io> {
    proto: ProtocolId,
    _r: PhantomData<Io>,
}

impl<Io> Clone for ProtocolNegotiation<Io> {
    fn clone(&self) -> Self {
        ProtocolNegotiation {
            proto: self.proto,
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
        Io: AsyncRead + AsyncWrite + Unpin,
    {
        Framed::new(stream, ProtocolIdCodec)
    }
}

impl<Io> Default for ProtocolNegotiation<Io> {
    fn default() -> Self {
        Self::new(ProtocolId::Amqp)
    }
}

impl<Io> Service for ProtocolNegotiation<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    type Request = Framed<Io, ProtocolIdCodec>;
    type Response = Framed<Io, ProtocolIdCodec>;
    type Error = ProtocolIdError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    #[inline]
    fn poll_ready(&self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&self, mut framed: Framed<Io, ProtocolIdCodec>) -> Self::Future {
        let proto = self.proto;

        Box::pin(async move {
            framed.send(proto).await?;
            let protocol = framed.next().await.ok_or(ProtocolIdError::Disconnected)??;

            if proto == protocol {
                Ok(framed)
            } else {
                Err(ProtocolIdError::Unexpected {
                    exp: proto,
                    got: protocol,
                })
            }
        })
    }
}
