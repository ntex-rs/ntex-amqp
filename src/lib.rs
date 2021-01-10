#![deny(rust_2018_idioms, unreachable_pub)]
#![allow(clippy::type_complexity)]
#![allow(dead_code, unreachable_pub)]

#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate log;

use std::{future::Future, pin::Pin, task::Context, task::Poll, time::Duration};

use bytestring::ByteString;
use ntex::channel::oneshot;
use ntex_amqp_codec::protocol::{Disposition, Handle, Milliseconds, Open};
use uuid::Uuid;

#[macro_use]
mod utils;

mod cell;
pub mod client;
mod connection;
mod control;
pub mod error_code;
mod errors;
mod hb;
mod io;
mod rcvlink;
pub mod server;
mod service;
mod session;
mod sndlink;
mod state;

pub use ntex_amqp_codec::protocol::Error;

pub use self::connection::Connection;
pub use self::control::{ControlFrame, ControlFrameKind};
pub use self::errors::{AmqpError, AmqpProtocolError, LinkError};
pub use self::rcvlink::{ReceiverLink, ReceiverLinkBuilder};
pub use self::session::Session;
pub use self::sndlink::{SenderLink, SenderLinkBuilder};
pub use self::state::State;

pub mod codec {
    pub use ntex_amqp_codec::*;
}

type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;
type HashSet<V> = std::collections::HashSet<V, ahash::RandomState>;

pub enum Delivery {
    Resolved(Result<Disposition, AmqpProtocolError>),
    Pending(oneshot::Receiver<Result<Disposition, AmqpProtocolError>>),
    Gone,
}

type DeliveryPromise = oneshot::Sender<Result<Disposition, AmqpProtocolError>>;

impl Future for Delivery {
    type Output = Result<Disposition, AmqpProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Delivery::Pending(ref mut receiver) = *self {
            return match Pin::new(receiver).poll(cx) {
                Poll::Ready(Ok(r)) => Poll::Ready(r),
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => {
                    trace!("delivery oneshot is gone: {:?}", e);
                    Poll::Ready(Err(AmqpProtocolError::Disconnected))
                }
            };
        }

        let old_v = ::std::mem::replace(&mut *self, Delivery::Gone);
        if let Delivery::Resolved(r) = old_v {
            return match r {
                Ok(state) => Poll::Ready(Ok(state)),
                Err(e) => Poll::Ready(Err(e)),
            };
        }
        panic!("Polling Delivery after it was polled as ready is an error.");
    }
}

/// Amqp1 transport configuration.
#[derive(Debug, Clone)]
pub struct Configuration {
    pub max_frame_size: u32,
    pub channel_max: usize,
    pub idle_time_out: Milliseconds,
    pub hostname: Option<ByteString>,
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
            idle_time_out: 120_000,
            hostname: None,
        }
    }

    /// The channel-max value is the highest channel number that
    /// may be used on the Connection. This value plus one is the maximum
    /// number of Sessions that can be simultaneously active on the Connection.
    ///
    /// By default channel max value is set to 1024
    pub fn channel_max(&mut self, num: u16) -> &mut Self {
        self.channel_max = num as usize;
        self
    }

    /// Set max frame size for the connection.
    ///
    /// By default max size is set to 64kb
    pub fn max_frame_size(&mut self, size: u32) -> &mut Self {
        self.max_frame_size = size;
        self
    }

    /// Get max frame size for the connection.
    pub fn get_max_frame_size(&self) -> usize {
        self.max_frame_size as usize
    }

    /// Set idle time-out for the connection in seconds.
    ///
    /// By default idle time-out is set to 120 seconds
    pub fn idle_timeout(&mut self, timeout: u16) -> &mut Self {
        self.idle_time_out = (timeout * 1000) as Milliseconds;
        self
    }

    /// Set connection hostname
    ///
    /// Hostname is not set by default
    pub fn hostname(&mut self, hostname: &str) -> &mut Self {
        self.hostname = Some(ByteString::from(hostname));
        self
    }

    /// Create `Open` performative for this configuration.
    pub fn to_open(&self) -> Open {
        Open {
            container_id: ByteString::from(Uuid::new_v4().to_simple().to_string()),
            hostname: self.hostname.clone(),
            max_frame_size: self.max_frame_size,
            channel_max: self.channel_max as u16,
            idle_time_out: if self.idle_time_out > 0 {
                Some(self.idle_time_out)
            } else {
                None
            },
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub(crate) fn timeout_secs(&self) -> usize {
        if self.idle_time_out > 0 {
            ((self.idle_time_out as f32) * 0.8 / 1000.0) as usize
        } else {
            0
        }
    }
}

impl<'a> From<&'a Open> for Configuration {
    fn from(open: &'a Open) -> Self {
        Configuration {
            max_frame_size: open.max_frame_size,
            channel_max: open.channel_max as usize,
            idle_time_out: open.idle_time_out.unwrap_or(0),
            hostname: open.hostname.clone(),
        }
    }
}
