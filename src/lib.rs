#![deny(clippy::pedantic)]
#![allow(
    clippy::clone_on_copy,
    clippy::cast_possible_truncation,
    clippy::let_underscore_future,
    clippy::missing_fields_in_debug,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::similar_names,
    clippy::struct_field_names,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::unused_async,
    clippy::unused_async_trait_impl
)]

#[macro_use]
extern crate derive_more;

use ntex_amqp_codec::protocol::{Fields, Handle, Milliseconds, Open, OpenInner, Symbols};
use ntex_amqp_codec::types::Symbol;
use ntex_amqp_codec::{Decode, Encode};
use ntex_bytes::{BytePages, ByteString};
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
///
/// Session incoming window is not limited, memory used by received
/// transfers is bounded by link credit, max message size, handle-max
/// and channel-max.
#[derive(Debug)]
pub struct AmqpServiceConfig {
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub handle_max: u32,
    pub idle_time_out: Milliseconds,
    pub container_id: Option<ByteString>,
    pub hostname: Option<ByteString>,
    pub offered_capabilities: Option<Symbols>,
    pub desired_capabilities: Option<Symbols>,
    pub properties: Option<Fields>,
    pub(crate) handshake_timeout: Seconds,
    pub(crate) link_attach_timeout: Seconds,
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
            max_frame_size: 16 * 1024,
            channel_max: 1024,
            handle_max: 1024,
            idle_time_out: 120_000,
            container_id: None,
            hostname: None,
            handshake_timeout: Seconds(5),
            link_attach_timeout: Seconds(30),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
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
    /// The handle-max value is the highest link handle that the remote peer
    /// may use in a session. This value plus one is the maximum number of
    /// remotely attached links per session.
    ///
    /// By default handle max value is set to 1024
    pub fn set_handle_max(mut self, num: u32) -> Self {
        self.handle_max = num;
        self
    }

    #[must_use]
    /// Set max frame size for the connection.
    ///
    /// Also limits inbound frames, including handshake frames.
    ///
    /// By default max frame size is set to 16kb
    ///
    /// # Panics
    ///
    /// Panics if `size` is lower than 512 (`MIN_MAX_FRAME_SIZE`), peers
    /// must accept frames of at least 512 bytes.
    pub fn set_max_frame_size(mut self, size: u32) -> Self {
        assert!(
            size >= codec::protocol::MIN_MAX_FRAME_SIZE,
            "max frame size must be at least {}, got {size}",
            codec::protocol::MIN_MAX_FRAME_SIZE
        );
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
    /// Set container-id
    ///
    /// Container id is not set by default.
    pub fn set_container_id(mut self, cid: &str) -> Self {
        self.container_id = Some(ByteString::from(cid));
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
    /// Set open frame properties
    pub fn set_properties(mut self, props: Fields) -> Self {
        self.properties = Some(props);
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

    #[must_use]
    /// Set local link attach timeout.
    ///
    /// Link attach fails with `AmqpProtocolError::LinkAttachTimeout` if remote
    /// peer does not respond in time. Link is detached if remote attach is
    /// received later, link name stays in use until then.
    ///
    /// The same timeout applies to local link detach. If remote peer does not
    /// confirm detach in time, detach and unsettled link deliveries fail with
    /// `AmqpProtocolError::LinkDetached(None)`. Link handle stays in use until
    /// remote detach is received.
    ///
    /// Use `Seconds::ZERO` to disable timeout.
    ///
    /// By default link attach timeout is 30 seconds.
    pub fn set_link_attach_timeout(mut self, timeout: Seconds) -> Self {
        self.link_attach_timeout = timeout;
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
            container_id: self
                .container_id
                .clone()
                .unwrap_or_else(|| ByteString::from(Uuid::new_v4().simple().to_string())),
            hostname: self.hostname.clone(),
            // `0` is unlimited locally, advertise max value
            max_frame_size: if self.max_frame_size == 0 {
                u32::MAX
            } else {
                self.max_frame_size
            },
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
            properties: self.properties.clone(),
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
            hostname: open.hostname().map(|h| {
                let mut h = h.clone();
                h.trimdown();
                h
            }),
            offered_capabilities: open.0.offered_capabilities.as_ref().map(detach),
            desired_capabilities: open.0.desired_capabilities.as_ref().map(detach),
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

/// Copy decoded value into a buffer of its own
///
/// Slices of a decoded frame keep the whole read buffer alive.
pub(crate) fn detach<T: Encode + Decode + Clone>(val: &T) -> T {
    let mut buf = BytePages::default();
    val.encode(&mut buf);
    let mut buf = buf.freeze();
    buf.trimdown();
    T::decode(&mut buf).unwrap_or_else(|_| val.clone())
}

#[cfg(test)]
mod tests;
