use actix_service::{IntoNewService, NewService};

use super::connect::ConnectAck;
use super::sasl::{no_sasl, NoSaslService};

pub struct Handshake<Io, St, A, B> {
    a: A,
    b: B,
    _t: std::marker::PhantomData<(Io, St)>,
}

impl<Io, St, A> Handshake<Io, St, A, ()>
where
    A: NewService<Config = ()>,
{
    pub fn new<F>(srv: F) -> Handshake<Io, St, A, NoSaslService<Io, St, A::Error>>
    where
        F: IntoNewService<A>,
    {
        Handshake {
            a: srv.into_new_service(),
            b: no_sasl(),
            _t: std::marker::PhantomData,
        }
    }
}

impl<Io, St, A, B> Handshake<Io, St, A, B>
where
    A: NewService<Config = (), Response = ConnectAck<Io, St>>,
    B: NewService<Config = (), Response = ConnectAck<Io, St>>,
{
    pub fn sasl<F, B1>(self, srv: F) -> Handshake<Io, St, A, B1>
    where
        F: IntoNewService<B1>,
        B1: NewService<Response = A::Response, Error = A::Error, InitError = A::InitError>,
        B1::Error: Into<amqp_codec::protocol::Error>,
    {
        Handshake {
            a: self.a,
            b: srv.into_new_service(),
            _t: std::marker::PhantomData,
        }
    }
}

impl<Io, St, A, B> IntoNewService<actix_utils::either::Either<A, B>> for Handshake<Io, St, A, B>
where
    A: NewService<Config = (), Response = ConnectAck<Io, St>>,
    B: NewService<Config = (), Response = A::Response, Error = A::Error, InitError = A::InitError>,
{
    fn into_new_service(self) -> actix_utils::either::Either<A, B> {
        actix_utils::either::Either::new(self.a, self.b)
    }
}
