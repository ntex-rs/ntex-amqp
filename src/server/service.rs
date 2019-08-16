use std::fmt;
use std::marker::PhantomData;

use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_server_config::{Io as IoStream, ServerConfig};
use actix_service::{IntoNewService, NewService, Service};
use amqp_codec::protocol::{Error, ProtocolId};
use amqp_codec::{AmqpCodecError, AmqpFrame, ProtocolIdCodec, ProtocolIdError};
use futures::future::{err, Either};
use futures::{Async, Future, Poll, Sink, Stream};

use crate::cell::Cell;
use crate::connection::Connection;
use crate::Configuration;

use super::connect::{Connect, ConnectAck};
use super::dispatcher::Dispatcher;
use super::errors::ServerError;
use super::link::Link;
use super::sasl::Sasl;

/// Amqp connection type
pub type AmqpConnect<Io> = either::Either<Connect<Io>, Sasl<Io>>;

/// Server dispatcher factory
pub struct Server<Io, St, Cn> {
    connect: Cn,
    config: Configuration,
    disconnect: Option<Box<dyn Fn(&mut St, Option<&AmqpCodecError>)>>,
    _t: PhantomData<(Io, St)>,
}

pub(super) struct ServerInner<St, Cn, Pb> {
    connect: Cn,
    publish: Pb,
    config: Configuration,
    disconnect: Option<Box<dyn Fn(&mut St, Option<&AmqpCodecError>)>>,
}

impl<Io, St, Cn> Server<Io, St, Cn>
where
    Io: 'static,
    St: 'static,
    Io: AsyncRead + AsyncWrite,
    Cn: NewService<Config = (), Request = AmqpConnect<Io>, Response = ConnectAck<Io, St>> + 'static,
{
    /// Create server factory and provide connect service
    pub fn new<F>(connect: F) -> Self
    where
        F: IntoNewService<Cn>,
    {
        Self {
            connect: connect.into_new_service(),
            config: Configuration::default(),
            disconnect: None,
            _t: PhantomData,
        }
    }

    /// Provide connection configuration
    pub fn config(mut self, config: Configuration) -> Self {
        self.config = config;
        self
    }

    /// Callback to execute on disconnect
    ///
    /// Second parameter indicates error occured during disconnect.
    pub fn disconnect<F, Out>(self, disconnect: F) -> Self
    where
        F: Fn(&mut St, Option<&AmqpCodecError>) -> Out + 'static,
        Out: futures::IntoFuture,
        Out::Future: 'static,
    {
        Server {
            connect: self.connect,
            config: self.config,
            disconnect: Some(Box::new(move |st, err| {
                let fut = disconnect(st, err).into_future();
                tokio_current_thread::spawn(fut.map_err(|_| ()).map(|_| ()));
            })),
            _t: PhantomData,
        }
    }

    /// Set service to execute for incoming links and create service factory
    pub fn finish<F, Pb>(
        self,
        service: F,
    ) -> impl NewService<Config = ServerConfig, Request = IoStream<Io>, Response = (), Error = ()>
    where
        F: IntoNewService<Pb>,
        Pb: NewService<Config = St, Request = Link<St>, Response = ()> + 'static,
        Pb::Error: fmt::Display + Into<Error>,
        Pb::InitError: fmt::Display + Into<Error>,
    {
        ServerImpl {
            inner: Cell::new(ServerInner {
                connect: self.connect,
                config: self.config,
                publish: service.into_new_service(),
                disconnect: self.disconnect,
            }),
            _t: PhantomData,
        }
    }
}

struct ServerImpl<Io, St, Cn, Pb> {
    inner: Cell<ServerInner<St, Cn, Pb>>,
    _t: PhantomData<(Io,)>,
}

impl<Io, St, Cn, Pb> NewService for ServerImpl<Io, St, Cn, Pb>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    Cn: NewService<Config = (), Request = AmqpConnect<Io>, Response = ConnectAck<Io, St>> + 'static,
    Pb: NewService<Config = St, Request = Link<St>, Response = ()> + 'static,
    Pb::Error: fmt::Display + Into<Error>,
    Pb::InitError: fmt::Display + Into<Error>,
{
    type Config = ServerConfig;
    type Request = IoStream<Io>;
    type Response = ();
    type Error = ();
    type Service = ServerImplService<Io, St, Cn, Pb>;
    type InitError = Cn::InitError;
    type Future = Box<dyn Future<Item = Self::Service, Error = Cn::InitError>>;

    fn new_service(&self, _: &ServerConfig) -> Self::Future {
        let inner = self.inner.clone();

        Box::new(
            self.inner
                .connect
                .new_service(&())
                .map(move |connect| ServerImplService {
                    inner,
                    connect: Cell::new(connect),
                    _t: PhantomData,
                }),
        )
    }
}

struct ServerImplService<Io, St, Cn: NewService, Pb> {
    connect: Cell<Cn::Service>,
    inner: Cell<ServerInner<St, Cn, Pb>>,
    _t: PhantomData<(Io,)>,
}

impl<Io, St, Cn, Pb> Service for ServerImplService<Io, St, Cn, Pb>
where
    St: 'static,
    Io: AsyncRead + AsyncWrite + 'static,
    Cn: NewService<Config = (), Request = AmqpConnect<Io>, Response = ConnectAck<Io, St>> + 'static,
    Pb: NewService<Config = St, Request = Link<St>, Response = ()> + 'static,
    Pb::Error: fmt::Display + Into<Error>,
    Pb::InitError: fmt::Display + Into<Error>,
{
    type Request = IoStream<Io>;
    type Response = ();
    type Error = ();
    type Future = Box<dyn Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        Box::new(handshake(self.connect.clone(), self.inner.clone(), req))
    }
}

fn handshake<Io, St, Cn: NewService, Pb, I>(
    connect: Cell<Cn::Service>,
    inner: Cell<ServerInner<St, Cn, Pb>>,
    stream: IoStream<Io, I>,
) -> impl Future<Item = (), Error = ()>
where
    Io: AsyncRead + AsyncWrite + 'static,
    Cn: NewService<Config = (), Request = AmqpConnect<Io>, Response = ConnectAck<Io, St>>,
    Pb: NewService<Config = St, Request = Link<St>, Response = ()> + 'static,
    Pb::Error: fmt::Display + Into<Error>,
    Pb::InitError: fmt::Display + Into<Error>,
{
    let (io, _, _) = stream.into_parts();

    let inner2 = inner.clone();
    let framed = Framed::new(io, ProtocolIdCodec);

    framed
        .into_future()
        .map_err(|e| ServerError::Handshake(e.0))
        .and_then(move |(protocol, mut framed)| {
            match protocol {
                // start amqp processing
                Some(ProtocolId::Amqp) | Some(ProtocolId::AmqpSasl) => {
                    if let Err(e) = framed.force_send(protocol.unwrap()) {
                        return Either::B(err(ServerError::from(e)));
                    }

                    Either::A(
                        connect
                            .get_mut()
                            .call(if protocol == Some(ProtocolId::Amqp) {
                                either::Either::Left(Connect::new(framed))
                            } else {
                                either::Either::Right(Sasl::new(framed))
                            })
                            .map_err(|e| ServerError::Service(e))
                            .and_then(move |ack| {
                                let (st, open, framed) = ack.into_inner();
                                let st = Cell::new(st);
                                let cfg = inner.get_ref().config.clone();

                                // confirm Open
                                let local = cfg.to_open();
                                framed
                                    .send(AmqpFrame::new(0, local.into()))
                                    .map_err(ServerError::from)
                                    .map(move |framed| {
                                        Connection::new(framed, cfg.clone(), (&open).into(), None)
                                    })
                                    .and_then(move |conn| {
                                        // create publish service
                                        inner
                                            .publish
                                            .new_service(st.get_ref())
                                            .map_err(|e| {
                                                error!("Can not construct app service");
                                                ServerError::ProtocolError(e.into())
                                            })
                                            .map(move |srv| (st, srv, conn))
                                    })
                            }),
                    )
                }
                Some(ProtocolId::AmqpTls) => {
                    Either::B(err(ServerError::from(ProtocolIdError::Unexpected {
                        exp: ProtocolId::Amqp,
                        got: ProtocolId::AmqpTls,
                    })))
                }
                None => Either::B(err(ServerError::Disconnected.into())),
            }
        })
        .map_err(|_| ())
        .and_then(move |(st, srv, conn)| {
            let st2 = st.clone();
            Dispatcher::new(conn, st, srv)
                .then(move |res| {
                    if inner2.disconnect.is_some() {
                        (*inner2.get_mut().disconnect.as_mut().unwrap())(
                            st2.get_mut(),
                            res.as_ref().err(),
                        )
                    }
                    res
                })
                .map_err(|_| ())
        })
}
