use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_connector::{Connect, RequestHost};
use actix_service::{FnService, IntoService, Service, ServiceExt};
use actix_utils::time::LowResTimeService;
use either::Either;
use futures::future::{ok, Future};
use futures::{Sink, Stream};

use amqp_codec::protocol::{Frame, ProtocolId, SaslCode, SaslFrameBody, SaslInit};
use amqp_codec::types::Symbol;
use amqp_codec::{AmqpCodec, AmqpFrame, ProtocolIdCodec, SaslFrame};

use crate::connection::Connection;
use crate::service::ProtocolNegotiation;

use super::Configuration;
pub use crate::errors::SaslConnectError;

#[derive(Debug)]
/// Sasl connect request
pub struct SaslConnect {
    pub connect: Connect,
    pub config: Configuration,
    pub auth: SaslAuth,
    pub time: Option<LowResTimeService>,
}

#[derive(Debug)]
/// Sasl authentication parameters
pub struct SaslAuth {
    pub authz_id: String,
    pub authn_id: String,
    pub password: String,
}

/// Create service that connects to amqp server and authenticate itself via sasl.
/// This service uses supplied connector service. Service resolves to
/// a `Connection<_>` instance.
pub fn connect_service<T, Io>(
    connector: T,
) -> impl Service<
    Request = SaslConnect,
    Response = Connection<Io>,
    Error = either::Either<SaslConnectError, T::Error>,
>
where
    T: Service<Request = Connect, Response = (Connect, Io)>,
    T::Error: 'static,
    Io: AsyncRead + AsyncWrite + 'static,
{
    FnService::new(|connect: SaslConnect| {
        let SaslConnect {
            connect,
            config,
            auth,
            time,
        } = connect;
        ok::<_, either::Either<SaslConnectError, T::Error>>((connect, config, auth, time))
    })
    // connect to host
    .apply_fn(
        connector.map_err(|e| either::Right(e)),
        |(connect, config, auth, time), srv| {
            srv.call(connect)
                .map(|(connect, io)| (io, connect, config, auth, time))
        },
    )
    // sasl protocol negotiation
    .apply_fn(
        ProtocolNegotiation::new(ProtocolId::AmqpSasl)
            .map_err(|e| Either::Left(SaslConnectError::from(e))),
        |(io, connect, config, auth, time): (Io, Connect, Configuration, SaslAuth, _), srv| {
            let framed = Framed::new(io, ProtocolIdCodec);
            srv.call(framed)
                .map(move |framed| (framed, connect, config, auth, time))
        },
    )
    // sasl auth
    .apply_fn(
        sasl_connect.into_service().map_err(Either::Left),
        |(framed, connect, config, auth, time), srv| {
            srv.call((framed, auth))
                .map(move |framed| (connect, config, framed, time))
        },
    )
    // protocol negotiation
    .apply_fn(
        ProtocolNegotiation::new(ProtocolId::Amqp)
            .map_err(|e| Either::Left(SaslConnectError::from(e))),
        |(connect, config, framed, time): (
            Connect,
            Configuration,
            Framed<Io, ProtocolIdCodec>,
            _,
        ),
         srv| {
            srv.call(framed)
                .map(move |framed| (connect, config, framed, time))
        },
    )
    // open connection
    .and_then(
        |(connect, config, framed, time): (
            Connect,
            Configuration,
            Framed<Io, ProtocolIdCodec>,
            _,
        )| {
            let framed = framed.into_framed(AmqpCodec::<AmqpFrame>::new());
            let open = config.to_open(Some(connect.host()));
            trace!("Open connection: {:?}", open);
            framed
                .send(AmqpFrame::new(0, Frame::Open(open)))
                .map_err(|e| Either::Left(SaslConnectError::from(e)))
                .map(move |framed| (config, framed, time))
        },
    )
    // read open frame
    .and_then(
        move |(config, framed, time): (Configuration, Framed<_, AmqpCodec<AmqpFrame>>, _)| {
            framed
                .into_future()
                .map_err(|e| Either::Left(SaslConnectError::from(e.0)))
                .and_then(move |(frame, framed)| {
                    if let Some(frame) = frame {
                        if let Frame::Open(open) = frame.performative() {
                            trace!("Open confirmed: {:?}", open);
                            Ok(Connection::new(framed, config, open.into(), time))
                        } else {
                            Err(Either::Left(SaslConnectError::ExpectedOpenFrame))
                        }
                    } else {
                        Err(Either::Left(SaslConnectError::Disconnected))
                    }
                })
        },
    )
}

fn sasl_connect<Io: AsyncRead + AsyncWrite>(
    (framed, auth): (Framed<Io, ProtocolIdCodec>, SaslAuth),
) -> impl Future<Item = Framed<Io, ProtocolIdCodec>, Error = SaslConnectError> {
    let sasl_io = framed.into_framed(AmqpCodec::<SaslFrame>::new());
    // processing sasl-mechanisms
    sasl_io
        .into_future()
        .map_err(|e| SaslConnectError::from(e.0))
        .and_then(move |(_sasl_frame, sasl_io)| {
            let initial_response =
                SaslInit::prepare_response(&auth.authz_id, &auth.authn_id, &auth.password);
            let sasl_init = SaslInit {
                mechanism: Symbol::from("PLAIN"),
                initial_response: Some(initial_response),
                hostname: None,
            };
            sasl_io
                .send(sasl_init.into())
                .map_err(SaslConnectError::from)
                .and_then(|sasl_io| {
                    // processing sasl-outcome
                    sasl_io
                        .into_future()
                        .map_err(|e| SaslConnectError::from(e.0))
                        .and_then(|(sasl_frame, framed)| {
                            if let Some(SaslFrame {
                                body: SaslFrameBody::SaslOutcome(outcome),
                            }) = sasl_frame
                            {
                                if outcome.code() != SaslCode::Ok {
                                    return Err(SaslConnectError::Sasl(outcome.code()));
                                }
                            } else {
                                return Err(SaslConnectError::Disconnected);
                            }
                            Ok(framed.into_framed(ProtocolIdCodec))
                        })
                })
        })
}
