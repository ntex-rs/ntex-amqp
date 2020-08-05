mod body;
mod message;

pub use self::body::MessageBody;
pub use self::message::Message;

pub(self) const SECTION_PREFIX_LENGTH: usize = 3;
