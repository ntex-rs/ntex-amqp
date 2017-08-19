use std::time::Duration;
use codec::symbol;
use framing::INVALID_FRAME;
use nom::{be_u8, be_u16, be_u32, ErrorKind, IResult};
use types::{ByteStr, Symbol};

const DEFAULT_MAX_FRAME_SIZE: u32 = 4294967295;
const DEFAULT_CHANNEL_MAX: u16 = 65535;

/// The first frame sent on a connection in either direction.
/// The fields indicate the capabilities and limitations of the sending peer.
#[derive(Debug, PartialEq, Eq)]
pub struct Open {
    container_id: ByteStr,
    hostname: ByteStr,
    max_frame_size: u32,
    channel_max: u16,
    idle_timeout: Option<Duration>,
    // outgoing_locales: Symbol,      // should support multiple
    // incoming_locales: Symbol,      // should support multiple
    // offered_capabilities: Symbol,  // should support multiple
    // desired_capabilities: Symbol,  // should support multiple
    // properies: HashMap<Symbol, Variant>,
}

impl Open {
    /// The id of the source container.
    pub fn container_id(&self) -> &str {
        self.container_id.as_str()
    }

    /// The name of the host to which the sending peer is connecting.
    pub fn hostname(&self) -> &str {
        self.hostname.as_str()
    }

    /// The largest frame size that the sending peer is able to accept on this connection.
    /// If this field is not set, it means that the peer does not impose any specific limit.
    /// A peer MUST NOT send frames larger than its partner can handle.
    /// Default: 4294967295
    pub fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// The maximum channel number that can be used on the connection.
    /// This number plus one is the maximum number of sessions that can be simultaneously
    /// active on the connection.
    pub fn channel_max(&self) -> u16 {
        self.channel_max
    }

    /// The idle timeout required by the sender. A value of zero is the same as if it was not set.
    /// If the receiver is unable or unwilling to support the idle time-out then it SHOULD close
    /// the connection with an error explaining why (e.g., because it is too small).
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }
}

/// A builder to help construct an Open performative.
pub struct OpenBuilder {
    container_id: ByteStr,
    hostname: ByteStr,
    max_frame_size: u32,
    channel_max: u16,
    idle_timeout: Option<Duration>,
}

impl OpenBuilder {
    /// Creates a new builder. The container_id is required.
    pub fn new(container_id: &str) -> OpenBuilder {
        OpenBuilder {
            container_id: ByteStr::from(container_id),
            hostname: ByteStr::from(""),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            channel_max: DEFAULT_CHANNEL_MAX,
            idle_timeout: None,
        }
    }

    /// Configures the hostname for the open frame. The default is "".
    pub fn hostname(mut self, hostname: &str) -> OpenBuilder {
        self.hostname = ByteStr::from(hostname);
        self
    }

    /// Configures the maximum frame size. The default is 4294967295.
    pub fn max_frame_size(mut self, max_frame_size: u32) -> OpenBuilder {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Configures the maximum number of channels. The default is 65535.
    pub fn channel_max(mut self, channel_max: u16) -> OpenBuilder {
        self.channel_max = channel_max;
        self
    }

    /// Configures the idle timeout. The default is None.
    pub fn idle_timeout(mut self, timeout: Duration) -> OpenBuilder {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Creates the Open performative from the configuration.
    pub fn build(self) -> Open {
        Open {
            container_id: self.container_id,
            hostname: self.hostname,
            max_frame_size: self.max_frame_size,
            channel_max: self.channel_max,
            idle_timeout: self.idle_timeout,
        }
    }
}

named!(pub open<Open>,
    do_parse!(
        tag!([0x00u8]) >>              //  composite type constructor

        descriptor: symbol >>
        error_if!(descriptor != Symbol::from("amqp:open:list"), INVALID_FRAME) >>

        

        (OpenBuilder::new("container1").build())
    ));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn builder() {
        let open1 = OpenBuilder::new("container1")
            .hostname("host1")
            .max_frame_size(42)
            .channel_max(1)
            .idle_timeout(Duration::from_millis(10))
            .build();
        let open2 = OpenBuilder::new("container1")
            .hostname("host1")
            .max_frame_size(42)
            .channel_max(1)
            .idle_timeout(Duration::from_millis(10))
            .build();

        assert_eq!("container1", open1.container_id());
        assert_eq!("host1", open1.hostname());
        assert_eq!(42, open1.max_frame_size());
        assert_eq!(1, open1.channel_max());
        assert_eq!(Some(Duration::from_millis(10)), open1.idle_timeout());

        assert_eq!(open1, open2);
    }
}