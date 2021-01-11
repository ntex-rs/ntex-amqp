use bytestring::ByteString;

mod connection;
mod connector;
mod error;

pub use self::connection::Client;
pub use self::connector::Connector;
pub use self::error::ConnectError;

#[derive(Debug)]
/// Sasl authentication parameters
pub struct SaslAuth {
    pub authz_id: ByteString,
    pub authn_id: ByteString,
    pub password: ByteString,
}
