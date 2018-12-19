use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_connector::{Connect, RequestHost};
use actix_service::{FnService, IntoService, Service, ServiceExt};
use bytes::Bytes;
use either::Either;
use futures::future::{ok, Future};
use futures::{Sink, Stream};

use amqp::framing::{AmqpFrame, SaslFrame};
use amqp::protocol::{Frame, ProtocolId, SaslCode, SaslFrameBody, SaslInit};
use amqp::types::Symbol;
use amqp::{AmqpCodec, ProtocolIdCodec};

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
    SaslConnect,
    Response = Connection<Io>,
    Error = either::Either<SaslConnectError, T::Error>,
>
where
    T: Service<Connect, Response = (Connect, Io)>,
    T::Error: 'static,
    Io: AsyncRead + AsyncWrite + 'static,
{
    FnService::new(|connect: SaslConnect| {
        let SaslConnect {
            connect,
            config,
            auth,
        } = connect;
        ok((connect, config, auth))
    })
    // connect to host
    .apply(
        connector.map_err(|e| either::Right(e)),
        |(connect, config, auth), srv| {
            srv.call(connect)
                .map(|(connect, io)| (io, connect, config, auth))
        },
    )
    // sasl protocol negotiation
    .apply(
        ProtocolNegotiation::new(ProtocolId::AmqpSasl)
            .map_err(|e| Either::Left(SaslConnectError::from(e))),
        |(io, connect, config, auth): (Io, Connect, Configuration, SaslAuth), srv| {
            let framed = Framed::new(io, ProtocolIdCodec);
            srv.call(framed)
                .map(move |framed| (framed, connect, config, auth))
        },
    )
    // sasl auth
    .apply(
        sasl_connect.into_service().map_err(Either::Left),
        |(framed, connect, config, auth), srv| {
            srv.call((framed, auth))
                .map(move |framed| (connect, config, framed))
        },
    )
    // protocol negotiation
    .apply(
        ProtocolNegotiation::new(ProtocolId::Amqp)
            .map_err(|e| Either::Left(SaslConnectError::from(e))),
        |(connect, config, framed): (Connect, Configuration, Framed<Io, ProtocolIdCodec>), srv| {
            srv.call(framed)
                .map(move |framed| (connect, config, framed))
        },
    )
    // open connection
    .and_then(
        |(connect, config, framed): (Connect, Configuration, Framed<Io, ProtocolIdCodec>)| {
            let framed = framed.into_framed(AmqpCodec::<AmqpFrame>::new());
            let open = config.into_open(Some(connect.host()));
            trace!("Open connection: {:?}", open);
            framed
                .send(AmqpFrame::new(0, Frame::Open(open), Bytes::new()))
                .map_err(|e| Either::Left(SaslConnectError::from(e)))
                .map(|framed| (config, framed))
        },
    )
    // read open frame
    .and_then(
        |(config, framed): (Configuration, Framed<_, AmqpCodec<AmqpFrame>>)| {
            framed
                .into_future()
                .map_err(|e| Either::Left(SaslConnectError::from(e.0)))
                .and_then(|(frame, framed)| {
                    if let Some(frame) = frame {
                        if let Frame::Open(open) = frame.performative() {
                            trace!("Open confirmed: {:?}", open);
                            Ok(Connection::new(framed, config, open.into()))
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
                .send(SaslFrame::new(SaslFrameBody::SaslInit(sasl_init)))
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
