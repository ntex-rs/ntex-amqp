#![deny(
    rust_2018_idioms,
    warnings,
    unreachable_pub,
    // missing_debug_implementations,
    clippy::pedantic
)]
#![allow(
    clippy::clone_on_copy,
    clippy::cast_possible_truncation,
    clippy::let_underscore_future,
    clippy::missing_fields_in_debug,
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::similar_names,
    clippy::struct_field_names,
    clippy::too_many_lines,
    clippy::type_complexity
)]

#[macro_use]
extern crate derive_more;

use ntex_amqp_codec::protocol::{Handle, Milliseconds, Open, OpenInner, Symbols};
use ntex_amqp_codec::types::Symbol;
use ntex_bytes::ByteString;
use ntex_service::cfg::{CfgContext, Configuration as SvcConfiguration};
use ntex_util::time::Seconds;
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
#[derive(Debug)]
pub struct AmqpServiceConfig {
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_time_out: Milliseconds,
    pub hostname: Option<ByteString>,
    pub offered_capabilities: Option<Symbols>,
    pub desired_capabilities: Option<Symbols>,
    pub(crate) max_size: usize,
    pub(crate) handshake_timeout: Seconds,
    config: CfgContext,
}

/// Amqp1 transport configuration.
#[derive(Debug)]
pub struct RemoteServiceConfig {
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_time_out: Milliseconds,
    pub hostname: Option<ByteString>,
    pub offered_capabilities: Option<Symbols>,
    pub desired_capabilities: Option<Symbols>,
}

impl Default for AmqpServiceConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl SvcConfiguration for AmqpServiceConfig {
    const NAME: &str = "AMQP Configuration";

    fn ctx(&self) -> &CfgContext {
        &self.config
    }

    fn set_ctx(&mut self, ctx: CfgContext) {
        self.config = ctx;
    }
}

impl AmqpServiceConfig {
    /// Create connection configuration.
    pub fn new() -> Self {
        AmqpServiceConfig {
            max_size: 0,
            max_frame_size: u32::from(u16::MAX),
            channel_max: 1024,
            idle_time_out: 120_000,
            hostname: None,
            handshake_timeout: Seconds(5),
            offered_capabilities: None,
            desired_capabilities: None,
            config: CfgContext::default(),
        }
    }

    #[must_use]
    /// The channel-max value is the highest channel number that
    /// may be used on the Connection. This value plus one is the maximum
    /// number of Sessions that can be simultaneously active on the Connection.
    ///
    /// By default channel max value is set to 1024
    pub fn set_channel_max(mut self, num: u16) -> Self {
        self.channel_max = num;
        self
    }

    #[must_use]
    /// Set max frame size for the connection.
    ///
    /// By default max size is set to 64kb
    pub fn set_max_frame_size(mut self, size: u32) -> Self {
        self.max_frame_size = size;
        self
    }

    /// Get max frame size for the connection.
    pub fn get_max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    #[must_use]
    /// Set idle time-out for the connection in seconds.
    ///
    /// By default idle time-out is set to 120 seconds
    pub fn set_idle_timeout(mut self, timeout: u16) -> Self {
        self.idle_time_out = Milliseconds::from(timeout) * 1000;
        self
    }

    #[must_use]
    /// Set connection hostname
    ///
    /// Hostname is not set by default
    pub fn set_hostname(mut self, hostname: &str) -> Self {
        self.hostname = Some(ByteString::from(hostname));
        self
    }

    #[must_use]
    /// Set offered capabilities
    pub fn set_offered_capabilities(mut self, caps: Symbols) -> Self {
        self.offered_capabilities = Some(caps);
        self
    }

    #[must_use]
    /// Set desired capabilities
    pub fn set_desired_capabilities(mut self, caps: Symbols) -> Self {
        self.desired_capabilities = Some(caps);
        self
    }

    #[must_use]
    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    #[must_use]
    /// Set handshake timeout.
    ///
    /// By default handshake timeout is 5 seconds.
    pub fn set_handshake_timeout(mut self, timeout: Seconds) -> Self {
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

    #[must_use]
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
}

impl RemoteServiceConfig {
    #[must_use]
    pub fn new(open: &Open) -> RemoteServiceConfig {
        RemoteServiceConfig {
            max_frame_size: open.max_frame_size(),
            channel_max: open.channel_max(),
            idle_time_out: open.idle_time_out().unwrap_or(0),
            hostname: open.hostname().cloned(),
            offered_capabilities: open.0.offered_capabilities.clone(),
            desired_capabilities: open.0.desired_capabilities.clone(),
        }
    }

    #[allow(clippy::cast_sign_loss, clippy::cast_precision_loss)]
    pub(crate) fn timeout_remote_secs(&self) -> Seconds {
        if self.idle_time_out > 0 {
            Seconds::checked_new(((self.idle_time_out as f32) * 0.75 / 1000.0) as usize)
        } else {
            Seconds::ZERO
        }
    }
}
