//#![deny(rust_2018_idioms, warnings, unreachable_pub)]
#![allow(clippy::type_complexity, clippy::let_underscore_future)]

#[macro_use]
extern crate derive_more;

use ntex::{time::Seconds, util::ByteString};
use ntex_amqp_codec::protocol::{Handle, Milliseconds, Open, OpenInner, Symbols};
use ntex_amqp_codec::types::Symbol;
use uuid::Uuid;

mod cell;
pub mod client;
mod connection;
mod control;
mod default;
mod delivery;
mod dispatcher;
pub mod error;
pub mod error_code;
mod rcvlink;
mod router;
pub mod server;
mod session;
mod sndlink;
mod state;
pub mod types;

pub use self::connection::{Connection, ConnectionRef, OpenSession};
pub use self::control::{ControlFrame, ControlFrameKind};
pub use self::delivery::{Delivery, TransferBuilder};
pub use self::rcvlink::{ReceiverLink, ReceiverLinkBuilder};
pub use self::session::Session;
pub use self::sndlink::{SenderLink, SenderLinkBuilder};
pub use self::state::State;

pub mod codec {
    pub use ntex_amqp_codec::*;
}

/// Amqp1 transport configuration.
#[derive(Debug, Clone)]
pub struct Configuration {
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_time_out: Milliseconds,
    pub hostname: Option<ByteString>,
    pub offered_capabilities: Option<Symbols>,
    pub desired_capabilities: Option<Symbols>,
    pub(crate) max_size: usize,
    pub(crate) handshake_timeout: Seconds,
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
            max_size: 0,
            max_frame_size: u16::MAX as u32,
            channel_max: 1024,
            idle_time_out: 120_000,
            hostname: None,
            handshake_timeout: Seconds(5),
            offered_capabilities: None,
            desired_capabilities: None,
        }
    }

    /// The channel-max value is the highest channel number that
    /// may be used on the Connection. This value plus one is the maximum
    /// number of Sessions that can be simultaneously active on the Connection.
    ///
    /// By default channel max value is set to 1024
    pub fn channel_max(&mut self, num: u16) -> &mut Self {
        self.channel_max = num;
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
    pub fn get_max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Set idle time-out for the connection in seconds.
    ///
    /// By default idle time-out is set to 120 seconds
    pub fn idle_timeout(&mut self, timeout: u16) -> &mut Self {
        self.idle_time_out = (timeout as Milliseconds) * 1000;
        self
    }

    /// Set connection hostname
    ///
    /// Hostname is not set by default
    pub fn hostname(&mut self, hostname: &str) -> &mut Self {
        self.hostname = Some(ByteString::from(hostname));
        self
    }

    /// Set offered capabilities
    pub fn offered_capabilities(&mut self, caps: Symbols) -> &mut Self {
        self.offered_capabilities = Some(caps);
        self
    }

    /// Set desired capabilities
    pub fn desired_capabilities(&mut self, caps: Symbols) -> &mut Self {
        self.desired_capabilities = Some(caps);
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(&mut self, size: usize) -> &mut Self {
        self.max_size = size;
        self
    }

    /// Set handshake timeout.
    ///
    /// By default handshake timeout is 5 seconds.
    pub fn handshake_timeout(&mut self, timeout: Seconds) -> &mut Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Get offered capabilities
    pub fn get_offered_capabilities(&self) -> &[Symbol] {
        if let Some(caps) = &self.offered_capabilities {
            &caps.0
        } else {
            &[]
        }
    }

    /// Get desired capabilities
    pub fn get_desired_capabilities(&self) -> &[Symbol] {
        if let Some(caps) = &self.desired_capabilities {
            &caps.0
        } else {
            &[]
        }
    }

    /// Create `Open` performative for this configuration.
    pub fn to_open(&self) -> Open {
        Open(Box::new(OpenInner {
            container_id: ByteString::from(Uuid::new_v4().simple().to_string()),
            hostname: self.hostname.clone(),
            max_frame_size: self.max_frame_size,
            channel_max: self.channel_max,
            idle_time_out: if self.idle_time_out > 0 {
                Some(self.idle_time_out)
            } else {
                None
            },
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: self.offered_capabilities.clone(),
            desired_capabilities: self.desired_capabilities.clone(),
            properties: None,
        }))
    }

    pub(crate) fn timeout_remote_secs(&self) -> Seconds {
        if self.idle_time_out > 0 {
            Seconds::checked_new(((self.idle_time_out as f32) * 0.75 / 1000.0) as usize)
        } else {
            Seconds::ZERO
        }
    }

    pub fn from_remote(&self, open: &Open) -> Configuration {
        Configuration {
            max_frame_size: open.max_frame_size(),
            channel_max: open.channel_max(),
            idle_time_out: open.idle_time_out().unwrap_or(0),
            hostname: open.hostname().cloned(),
            max_size: self.max_size,
            handshake_timeout: self.handshake_timeout,
            offered_capabilities: open.0.offered_capabilities.clone(),
            desired_capabilities: open.0.desired_capabilities.clone(),
        }
    }
}
