use ntex::io::{Dispatcher as IoDispatcher, IoBoxed};
use ntex::service::{fn_service, IntoService, Pipeline, Service};
use ntex::util::Ready;

use crate::codec::{AmqpCodec, AmqpFrame};
use crate::control::ControlFrame;
use crate::error::{AmqpDispatcherError, LinkError};
use crate::{dispatcher::Dispatcher, Configuration, Connection, State};

/// Mqtt client
pub struct Client<St = ()> {
    io: IoBoxed,
    codec: AmqpCodec<AmqpFrame>,
    connection: Connection,
    remote_config: Configuration,
    _st: State<St>,
}

impl Client {
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: IoBoxed,
        codec: AmqpCodec<AmqpFrame>,
        connection: Connection,
        remote_config: Configuration,
    ) -> Self {
        Client {
            io,
            codec,
            connection,
            remote_config,
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
            remote_config: self.remote_config,
            _st: State::new(st),
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) -> Result<(), AmqpDispatcherError> {
        let dispatcher = Dispatcher::new(
            self.connection,
            Pipeline::new(fn_service(|_| Ready::<_, LinkError>::Ok(()))),
            Pipeline::new(fn_service(|_| Ready::<_, LinkError>::Ok(()))),
            self.remote_config.timeout_remote_secs().into(),
        );

        IoDispatcher::with_config(
            self.io,
            self.codec,
            dispatcher,
            &self.remote_config.disp_config,
        )
        .await
    }

    pub async fn start<F, S>(self, service: F) -> Result<(), AmqpDispatcherError>
    where
        F: IntoService<S, ControlFrame>,
        S: Service<ControlFrame, Response = ()>,
        S::Error: std::fmt::Debug + 'static,
        crate::error::Error: From<S::Error>,
        S: 'static,
    {
        let dispatcher = Dispatcher::new(
            self.connection,
            Pipeline::new(fn_service(|_| Ready::<_, S::Error>::Ok(()))),
            Pipeline::new(service.into_service()),
            self.remote_config.timeout_remote_secs().into(),
        );

        IoDispatcher::with_config(
            self.io,
            self.codec,
            dispatcher,
            &self.remote_config.disp_config,
        )
        .await
    }
}
