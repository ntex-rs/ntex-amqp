use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_service::{FnNewService, NewService};
use amqp::{AmqpCodec, AmqpFrame};
use futures::{Future, Sink, Stream};

use amqp::protocol::ProtocolId;
use amqp::{ProtocolIdCodec, ProtocolIdError};

/// Server protocol negotitation service
pub fn protocol_negotiation<Io>(
    proto: ProtocolId,
) -> impl NewService<
    Io,
    Response = Framed<Io, AmqpCodec<AmqpFrame>>,
    Error = ProtocolIdError,
    InitError = (),
>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    FnNewService::new(move |req: Io| {
        Framed::new(req, ProtocolIdCodec)
            .into_future()
            .map_err(|e| e.0)
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
            })
            .and_then(move |framed| {
                framed
                    .send(proto)
                    .from_err()
                    .map(|framed| framed.into_framed(AmqpCodec::new()))
            })
    })
}
