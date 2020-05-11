//! Defines the standard AMQP error codes.
use ntex_amqp_codec::types::Symbol;

// amqp errors
pub const INTERNAL_ERROR: Symbol = Symbol::from_static("amqp:internal-error");
pub const NOT_FOUND: Symbol = Symbol::from_static("amqp:not-found");
pub const UNAUTHORIZED_ACCESS: Symbol = Symbol::from_static("amqp:unauthorized-access");
pub const DECODE_ERROR: Symbol = Symbol::from_static("amqp:decode-error");
pub const RESOURCE_LIMIT_EXCEEDED: Symbol = Symbol::from_static("amqp:resource-limit-exceeded");
pub const NOT_ALLOWED: Symbol = Symbol::from_static("amqp:not-allowed");
pub const INVALID_FIELD: Symbol = Symbol::from_static("amqp:invalid-field");
pub const NOT_IMPLEMENTED: Symbol = Symbol::from_static("amqp:not-implemented");
pub const RESOURCE_LOCKED: Symbol = Symbol::from_static("amqp:resource-locked");
pub const PRECONDITION_FAILED: Symbol = Symbol::from_static("amqp:precondition-failed");
pub const RESOUORCE_DELETED: Symbol = Symbol::from_static("amqp:resource-deleted");
pub const ILLEGAL_STATE: Symbol = Symbol::from_static("amqp:illegal-state");
pub const FRAME_SIZE_TOO_SMALL: Symbol = Symbol::from_static("amqp:frame-size-too-small");

// connection errors
pub const CONNECTION_FORCED: Symbol = Symbol::from_static("amqp:connection:forced");
pub const FRAMING_ERROR: Symbol = Symbol::from_static("amqp:connection:framing-error");
pub const CONNECTION_REDIRECT: Symbol = Symbol::from_static("amqp:connection:redirect");

// session errors
pub const WINDOW_VIOLATION: Symbol = Symbol::from_static("amqp:session:window-violation");
pub const ERRANT_LINK: Symbol = Symbol::from_static("amqp:session-errant-link");
pub const HANDLE_IN_USE: Symbol = Symbol::from_static("amqp:session:handle-in-use");
pub const UNATTACHED_HANDLE: Symbol = Symbol::from_static("amqp:session:unattached-handle");

// link errors
pub const DETACH_FORCED: Symbol = Symbol::from_static("amqp:link:detach-forced");
pub const TRANSFER_LIMIT_EXCEEDED: Symbol =
    Symbol::from_static("amqp:link:transfer-limit-exceeded");
pub const MESSAGE_SIZE_EXCEEDED: Symbol = Symbol::from_static("amqp:link:message-size-exceeded");
pub const LINK_REDIRECT: Symbol = Symbol::from_static("amqp:link:redirect");
pub const STOLEN: Symbol = Symbol::from_static("amqp:link:stolen");

// tx error conditions
pub const TRANSACTION_UNKNOWN_ID: Symbol = Symbol::from_static("amqp:transaction:unknown-id");
pub const TRANSACTION_ROLLBACK: Symbol = Symbol::from_static("amqp:transaction:rollback");
pub const TRANSACTION_TIMEOUT: Symbol = Symbol::from_static("amqp:transaction:timeout");
