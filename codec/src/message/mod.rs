mod body;

#[allow(clippy::module_inception)]
mod message;

pub use self::body::MessageBody;
pub use self::message::Message;

const SECTION_PREFIX_LENGTH: usize = 3;
