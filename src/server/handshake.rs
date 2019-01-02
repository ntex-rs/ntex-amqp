use std::time::Duration;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::NewService;
use actix_utils::time::LowResTimeService;
use amqp::protocol::{Frame, ProtocolId};
use amqp::{AmqpCodec, AmqpFrame};
use futures::{Future, Sink, Stream};

use super::errors::HandshakeError;
use super::protocol::protocol_negotiation;
use crate::{Configuration, Connection};

/// Create new service for Amqp handshake pipeline.
/// It negotiates amqp protocol and opens connection.
pub fn handshake<Io>(
    cfg: Configuration,
) -> impl NewService<Io, Response = Connection<Io>, Error = HandshakeError, InitError = ()>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    let time = LowResTimeService::with(Duration::from_secs(1));

    protocol_negotiation(ProtocolId::Amqp).from_err().and_then(
        move |framed: Framed<Io, AmqpCodec<AmqpFrame>>| {
            let cfg = cfg.clone();
            let time = time.clone();

            // read Open frame
            framed
                .into_future()
                .map_err(|res| HandshakeError::from(res.0))
                .and_then(|(frame, framed)| {
                    if let Some(frame) = frame {
                        let frame = frame.into_parts().1;
                        match frame {
                            Frame::Open(open) => {
                                trace!("Got open: {:?}", open);
                                Ok((open, framed))
                            }
                            frame => Err(HandshakeError::Unexpected(frame)),
                        }
                    } else {
                        Err(HandshakeError::Disconnected)
                    }
                })
                .and_then(move |(open, framed)| {
                    // confirm Open
                    let local = cfg.to_open(None);
                    framed
                        .send(AmqpFrame::new(0, local.into()))
                        .map_err(HandshakeError::from)
                        .map(move |framed| {
                            Connection::new(framed, cfg.clone(), (&open).into(), Some(time.clone()))
                        })
                })
        },
    )
}
