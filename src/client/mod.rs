use ntex_bytes::ByteString;
use ntex_net::connect::Address;

mod connection;
mod connector;
mod error;

pub use self::connection::Client;
pub use self::connector::{Connector, ConnectorService};
pub use self::error::ConnectError;

#[derive(Clone, Debug)]
/// Connect message
pub struct Connect<T: Address> {
    addr: T,
    sasl: Option<SaslAuth>,
    hostname: Option<ByteString>,
}

impl<T: Address> Connect<T> {
    pub fn new(addr: T) -> Self {
        Self {
            addr,
            sasl: None,
            hostname: None,
        }
    }

    #[must_use]
    /// Use Sasl auth
    pub fn sasl_auth(
        mut self,
        authz_id: ByteString,
        authn_id: ByteString,
        password: ByteString,
    ) -> Self {
        self.sasl = Some(SaslAuth {
            authz_id,
            authn_id,
            password,
        });
        self
    }

    #[must_use]
    /// Set connection hostname
    ///
    /// Hostname is not set by default
    pub fn hostname<U>(mut self, hostname: U) -> Self
    where
        U: Into<ByteString>,
    {
        self.hostname = Some(hostname.into());
        self
    }

    fn into_parts(self) -> (T, Option<SaslAuth>, Option<ByteString>) {
        (self.addr, self.sasl, self.hostname)
    }
}

#[derive(Clone, Debug)]
/// Sasl authentication parameters
pub struct SaslAuth {
    pub authz_id: ByteString,
    pub authn_id: ByteString,
    pub password: ByteString,
}
