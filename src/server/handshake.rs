use actix_service::{IntoNewService, NewService};

use super::connect::ConnectAck;

pub fn handshake<Io, St, A, F>(srv: F) -> Handshake<Io, St, A>
where
    F: IntoNewService<A>,
    A: NewService<Config = (), Response = ConnectAck<Io, St>>,
{
    Handshake::new(srv)
}

pub struct Handshake<Io, St, A> {
    a: A,
    _t: std::marker::PhantomData<(Io, St)>,
}

impl<Io, St, A> Handshake<Io, St, A>
where
    A: NewService<Config = ()>,
{
    pub fn new<F>(srv: F) -> Handshake<Io, St, A>
    where
        F: IntoNewService<A>,
    {
        Handshake {
            a: srv.into_new_service(),
            _t: std::marker::PhantomData,
        }
    }
}

impl<Io, St, A> Handshake<Io, St, A>
where
    A: NewService<Config = (), Response = ConnectAck<Io, St>>,
{
    pub fn sasl<F, B>(self, srv: F) -> actix_utils::either::Either<A, B>
    where
        F: IntoNewService<B>,
        B: NewService<
            Config = (),
            Response = A::Response,
            Error = A::Error,
            InitError = A::InitError,
        >,
        B::Error: Into<amqp_codec::protocol::Error>,
    {
        actix_utils::either::Either::new(self.a, srv.into_new_service())
    }
}
