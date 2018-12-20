#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate log;

use std::time::Duration;

use amqp::protocol::{Handle, Milliseconds, Open, Outcome};
use bytes::Bytes;
use futures::{unsync::oneshot, Async, Future, Poll};
use string::{String, TryFrom};
use uuid::Uuid;

mod cell;
mod connection;
mod errors;
mod hb;
mod link;
mod message;
pub mod sasl;
mod service;
mod session;

pub use self::connection::Connection;
pub use self::errors::AmqpTransportError;
pub use self::link::SenderLink;
pub use self::message::{Message, MessageBody};
pub use self::session::Session;

pub enum Delivery {
    Resolved(Result<Outcome, AmqpTransportError>),
    Pending(oneshot::Receiver<Result<Outcome, AmqpTransportError>>),
    Gone,
}

type DeliveryPromise = oneshot::Sender<Result<Outcome, AmqpTransportError>>;

impl Future for Delivery {
    type Item = Outcome;
    type Error = AmqpTransportError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Delivery::Pending(ref mut receiver) = *self {
            return match receiver.poll() {
                Ok(Async::Ready(r)) => r.map(|state| Async::Ready(state)),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(_) => Err(AmqpTransportError::Disconnected),
            };
        }

        let old_v = ::std::mem::replace(self, Delivery::Gone);
        if let Delivery::Resolved(r) = old_v {
            return match r {
                Ok(state) => Ok(Async::Ready(state)),
                Err(e) => Err(e),
            };
        }
        panic!("Polling Delivery after it was polled as ready is an error.");
    }
}

/// Amqp1 transport configuration.
#[derive(Debug, Clone)]
pub struct Configuration {
    max_frame_size: u32,
    channel_max: usize,
    idle_time_out: Option<Milliseconds>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self::new()
    }
}

impl Configuration {
    /// Create connection configuration.
    pub fn new() -> Self {
        Configuration {
            max_frame_size: std::u16::MAX as u32,
            channel_max: 1024,
            idle_time_out: Some(120000),
        }
    }

    /// The channel-max value is the highest channel number that
    /// may be used on the Connection. This value plus one is the maximum
    /// number of Sessions that can be simultaneously active on the Connection.
    ///
    /// By default channel max value is set to 1024
    pub fn channel_max(mut self, num: u16) -> Self {
        self.channel_max = num as usize;
        self
    }

    /// Set max frame size for the connection.
    ///
    /// By default max size is set to 65535
    pub fn max_frame_size(mut self, size: u32) -> Self {
        self.max_frame_size = size;
        self
    }

    /// Set idle time-out for the connection in milliseconds
    ///
    /// By default idle time-out is set to 120000 milliseconds
    pub fn idle_time_out(mut self, timeout: u32) -> Self {
        self.idle_time_out = Some(timeout as Milliseconds);
        self
    }

    /// Create `Open` performative for this configuration.
    pub fn into_open(&self, hostname: Option<&str>) -> Open {
        Open {
            container_id: String::<Bytes>::try_from(Bytes::from(
                Uuid::new_v4().to_simple().to_string(),
            ))
            .unwrap(),
            hostname: hostname.map(|h| String::<Bytes>::from_str(h)),
            max_frame_size: self.max_frame_size,
            channel_max: self.channel_max as u16,
            idle_time_out: self.idle_time_out,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub(crate) fn timeout(&self) -> Option<Duration> {
        self.idle_time_out
            .map(|v| Duration::from_millis(((v as f32) * 0.8) as u64))
    }
}

impl<'a> From<&'a Open> for Configuration {
    fn from(open: &'a Open) -> Self {
        Configuration {
            max_frame_size: open.max_frame_size,
            channel_max: open.channel_max as usize,
            idle_time_out: open.idle_time_out,
        }
    }
}
