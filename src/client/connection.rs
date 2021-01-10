use std::time::{Duration, Instant};
use std::{cell::RefCell, convert::TryFrom, marker::PhantomData, num::NonZeroU16};

use bytestring::ByteString;
use futures::future::{ok, Either, Future};
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::router::{IntoPattern, Path, Router, RouterBuilder};
use ntex::rt::time::{delay_until, Instant as RtInstant};
use ntex::service::boxed::BoxService;
use ntex::service::{into_service, IntoService, Service};

use crate::codec::{AmqpCodec, AmqpFrame};
use crate::io::{IoDispatcher, IoState, Timer};
use crate::HashMap;

// use super::control::ControlMessage;
// use super::dispatcher::create_dispatcher;

/// Mqtt client
pub struct Client<Io> {
    io: Io,
    state: IoState<AmqpCodec<AmqpFrame>>,
    connection: Connection,
    keepalive: u16,
    disconnect_timeout: u16,
    remote_config: Configuration,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: T,
        state: IoState<AmqpCodec<AmqpFrame>>,
        connection: Connection,
        keepalive: u16,
        disconnect_timeout: u16,
        remote_config: Configuration,
    ) -> Self {
        Client {
            io,
            state,
            connection,
            keepalive,
            disconnect_timeout,
            remote_config,
        }
    }
}

impl<Io> Client<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    #[inline]
    /// Get client sink
    pub fn sink(&self) -> Connection {
        self.sink.clone()
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) {
        let idle_timeout = self.remote_config.idle_time_out.unwrap_or(0);
        if idle_timeout > 0 {
            ntex::rt::spawn(keepalive(self.sink.clone(), self.idle_timeout));
        }

        let dispatcher = create_dispatcher(
            self.sink,
            self.max_receive,
            16,
            into_service(|pkt| ok(either::Left(pkt))),
            into_service(
                |msg: ControlMessage<()>| ok(msg.disconnect(codec::Disconnect::default())),
            ),
        );

        let _ = IoDispatcher::with(
            self.io,
            self.state,
            dispatcher,
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(self.keepalive)
        .disconnect_timeout(self.disconnect_timeout)
        .await;
    }
}

async fn keepalive(sink: MqttSink, timeout: u16) {
    log::debug!("start mqtt client keep-alive task");

    let keepalive = Duration::from_secs(timeout as u64);
    loop {
        let expire = RtInstant::from_std(Instant::now() + keepalive);
        delay_until(expire).await;

        if !sink.ping() {
            // connection is closed
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }
    }
}
