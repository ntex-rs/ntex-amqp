//! Custom cell impl
use std::{cell, cell::UnsafeCell, fmt, ops::Deref, rc::Rc};

use crate::error::AmqpProtocolError;

pub(crate) struct Cell<T> {
    inner: Rc<UnsafeCell<T>>,
}

impl<T> Clone for Cell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Deref for Cell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get_ref()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Cell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get_ref().fmt(f)
    }
}

impl<T> Cell<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner: Rc::new(UnsafeCell::new(inner)),
        }
    }

    pub(crate) fn get_ref(&self) -> &T {
        unsafe { &*self.inner.as_ref().get() }
    }

    #[cfg(test)]
    pub(crate) fn downgrade(&self) -> std::rc::Weak<UnsafeCell<T>> {
        Rc::downgrade(&self.inner)
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_mut(&self) -> &mut T {
        unsafe { &mut *self.inner.as_ref().get() }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: `Cell` clones share one value, and `Deref`/`Debug` expose that value.
    ///
    /// HOW:
    /// - Clone a `Cell` holding `[1]` and push `2` through the clone's `get_mut`.
    /// - Assert the original's `get_ref`, `len` and `Debug` output show `[1, 2]`.
    #[test]
    fn cell_shares_state() {
        let cell = Cell::new(vec![1_u8]);
        let cell2 = cell.clone();
        cell2.get_mut().push(2);

        assert_eq!(*cell.get_ref(), [1, 2]);
        assert_eq!(cell.len(), 2);
        assert_eq!(format!("{cell:?}"), "[1, 2]");
    }
}
