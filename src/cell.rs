//! Shared error cell
use std::{cell, fmt};

use crate::error::AmqpProtocolError;

/// Error slot that is shared through `&self`.
#[derive(Default)]
pub(crate) struct ErrorCell(cell::Cell<Option<AmqpProtocolError>>);

impl ErrorCell {
    pub(crate) fn get(&self) -> Option<AmqpProtocolError> {
        let err = self.0.take();
        self.0.set(err.clone());
        err
    }

    pub(crate) fn is_some(&self) -> bool {
        let err = self.0.take();
        let is_some = err.is_some();
        self.0.set(err);
        is_some
    }

    pub(crate) fn is_none(&self) -> bool {
        !self.is_some()
    }

    pub(crate) fn set(&self, err: Option<AmqpProtocolError>) {
        self.0.set(err);
    }

    pub(crate) fn take(&self) -> Option<AmqpProtocolError> {
        self.0.take()
    }

    /// Keeps the first error and returns the stored one.
    pub(crate) fn get_or_insert(&self, err: AmqpProtocolError) -> AmqpProtocolError {
        let err = self.0.take().unwrap_or(err);
        self.0.set(Some(err.clone()));
        err
    }
}

impl fmt::Debug for ErrorCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}
