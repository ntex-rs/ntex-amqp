mod connection;
mod connector;
mod error;
// mod dispatcher;

pub use self::connection::Client;
pub use self::connector::AmqpConnector;
pub use self::error::ClientError;

#[derive(Debug)]
/// Sasl authentication parameters
pub struct SaslAuth {
    pub authz_id: String,
    pub authn_id: String,
    pub password: String,
}
