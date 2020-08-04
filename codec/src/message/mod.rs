mod body;
mod inmessage;
mod message;

pub use self::body::MessageBody;
pub use self::message::Message;

#[doc(hidden)]
pub type OutMessage = Message;
#[doc(hidden)]
pub use self::inmessage::InMessage;

pub(self) const SECTION_PREFIX_LENGTH: usize = 3;
