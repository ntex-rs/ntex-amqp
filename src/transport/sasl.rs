use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_connector::{Connect, ConnectorError, RequestHost};
use actix_service::{FnService, Service, ServiceExt};
use bytes::Bytes;
use futures::future::{ok, Future};
use futures::{Sink, Stream};
use uuid::Uuid;

use crate::errors::ProtocolIdError;
use crate::framing::AmqpFrame;
use crate::protocol::ProtocolId;
use crate::transport::codec::ProtocolIdCodec;
use crate::transport::connection::Connection;

use crate::errors::*;
use crate::framing::SaslFrame;
use crate::io::AmqpCodec;
use crate::protocol::*;
use crate::types::{ByteStr, Symbol};

struct SaslConnect {
    host: String,
    authz_id: String,
    authn_id: String,
    password: String,
}

enum SaslConnectError {
    Connector(ConnectorError),
    Protocol(ProtocolIdError),
    AmqpError(Error),
    Sasl(SaslCode),
    ExpectedOpenFrame,
    Disconnected,
}

impl From<ConnectorError> for SaslConnectError {
    fn from(e: ConnectorError) -> Self {
        SaslConnectError::Connector(e)
    }
}

impl From<ProtocolIdError> for SaslConnectError {
    fn from(e: ProtocolIdError) -> Self {
        SaslConnectError::Protocol(e)
    }
}

impl From<Error> for SaslConnectError {
    fn from(e: Error) -> Self {
        SaslConnectError::AmqpError(e)
    }
}

pub fn sasl_connect_service<T, Io>(
    connector: T,
) -> impl Service<SaslConnect, Response = Connection<Io>, Error = SaslConnectError>
where
    T: Service<Connect, Response = (Connect, Io), Error = ConnectorError>,
    Io: AsyncRead + AsyncWrite + 'static,
{
    FnService::new(|connect: SaslConnect| {
        let SaslConnect {
            host,
            authz_id,
            authn_id,
            password,
        } = connect;
        ok((Connect::with(&host).unwrap(), authz_id, authn_id, password))
    })
    // connect to host
    .apply(
        connector.from_err(),
        |(connect, authz_id, authn_id, password), srv| {
            srv.call(connect)
                .map(|(connect, io)| (io, connect, authz_id, authn_id, password))
        },
    )
    // sasl auth
    .apply(
        sasl_connect,
        |(io, connect, authz_id, authn_id, password), srv| {
            srv.call((io, authz_id, authn_id, password))
                // open connection
                .and_then(move |framed| {
                    let framed = framed.into_framed(AmqpCodec::<AmqpFrame>::new());
                    let open = Open {
                        container_id: ByteStr::from(&Uuid::new_v4().simple().to_string()[..]),
                        hostname: Some(ByteStr::from(&connect.host()[..])),
                        max_frame_size: ::std::u16::MAX as u32,
                        channel_max: 1,                     //::std::u16::MAX,
                        idle_time_out: Some(2 * 60 * 1000), // 2 min
                        outgoing_locales: None,
                        incoming_locales: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    };

                    framed
                        .send(AmqpFrame::new(0, Frame::Open(open), Bytes::new()))
                        .map_err(|e| SaslConnectError::from(Error::from(e.0)))
                        .and_then(|framed| {
                            framed
                                .into_future()
                                .map_err(|e| SaslConnectError::from(Error::from(e.0)))
                                .and_then(|(frame, framed)| {
                                    if let Some(frame) = frame {
                                        if let Frame::Open(ref _open) = *frame.performative() {
                                            Ok(framed)
                                        } else {
                                            Err(SaslConnectError::ExpectedOpenFrame)
                                        }
                                    } else {
                                        Err(SaslConnectError::Disconnected)
                                    }
                                })
                        })
                })
                .map(|framed| Connection::new(framed))
        },
    )
}

fn sasl_connect<Io: AsyncRead + AsyncWrite>(
    (io, authz_id, authn_id, password): (Io, String, String, String),
) -> impl Future<Item = Framed<Io, ProtocolIdCodec>, Error = SaslConnectError> {
    let framed = Framed::new(io, ProtocolIdCodec);

    framed
        .send(ProtocolId::AmqpSasl)
        .from_err()
        .and_then(|framed| {
            let sasl_io = framed.into_framed(AmqpCodec::<SaslFrame>::new());
            // processing sasl-mechanisms
            sasl_io
                .into_future()
                .map_err(|e| SaslConnectError::from(e.0))
                .and_then(move |(_sasl_frame, sasl_io)| {
                    let plain_symbol = Symbol::from_static("PLAIN");
                    let initial_response =
                        SaslInit::prepare_response(&authz_id, &authn_id, &password);
                    let sasl_init = SaslInit {
                        mechanism: plain_symbol,
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
        })
}
