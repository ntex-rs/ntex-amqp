use ntex::io::{Dispatcher as IoDispatcher, IoBoxed, Timer};
use ntex::service::{fn_service, Service};
use ntex::time::Seconds;
use ntex::util::Ready;
use ntex::IntoService;

use crate::codec::{AmqpCodec, AmqpFrame};
use crate::control::ControlFrame;
use crate::error::{DispatcherError, LinkError};
use crate::{dispatcher::Dispatcher, Configuration, Connection, State};

/// Mqtt client
pub struct Client<St = ()> {
    io: IoBoxed,
    codec: AmqpCodec<AmqpFrame>,
    connection: Connection,
    keepalive: Seconds,
    remote_config: Configuration,
    timer: Timer,
    _st: State<St>,
}

impl Client {
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: IoBoxed,
        codec: AmqpCodec<AmqpFrame>,
        connection: Connection,
        keepalive: Seconds,
        remote_config: Configuration,
        timer: Timer,
    ) -> Self {
        Client {
            io,
            codec,
            connection,
            keepalive,
            remote_config,
            timer,
            _st: State::new(()),
        }
    }
}

impl<St> Client<St>
where
    St: 'static,
{
    #[inline]
    /// Get client sink
    pub fn sink(&self) -> Connection {
        self.connection.clone()
    }

    #[inline]
    /// Set connection state
    pub fn state<T: 'static>(self, st: T) -> Client<T> {
        Client {
            io: self.io,
            codec: self.codec,
            connection: self.connection,
            keepalive: self.keepalive,
            remote_config: self.remote_config,
            timer: self.timer,
            _st: State::new(st),
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) -> Result<(), DispatcherError> {
        let dispatcher = Dispatcher::new(
            self.connection,
            fn_service(|_| Ready::<_, LinkError>::Ok(())),
            fn_service(|_| Ready::<_, LinkError>::Ok(())),
            self.remote_config.timeout_remote_secs().into(),
        )
        .map(|_| Option::<AmqpFrame>::None);

        let keepalive = if self.keepalive.non_zero() {
            self.keepalive + Seconds(5)
        } else {
            Seconds::ZERO
        };

        IoDispatcher::new(self.io, self.codec, dispatcher, self.timer)
            .keepalive_timeout(keepalive)
            .await
    }

    pub async fn start<F, S>(self, service: F) -> Result<(), DispatcherError>
    where
        F: IntoService<S>,
        S: Service<Request = ControlFrame, Response = ()>,
        S::Error: Into<crate::error::Error> + std::fmt::Debug + 'static,
        S: 'static,
    {
        let dispatcher = Dispatcher::new(
            self.connection,
            fn_service(|_| Ready::<_, LinkError>::Ok(())),
            service.into_service(),
            self.remote_config.timeout_remote_secs().into(),
        )
        .map(|_| Option::<AmqpFrame>::None);

        let keepalive = if self.keepalive.non_zero() {
            self.keepalive + Seconds(5)
        } else {
            Seconds::ZERO
        };

        IoDispatcher::new(self.io, self.codec, dispatcher, self.timer)
            .keepalive_timeout(keepalive)
            .await
    }
}
