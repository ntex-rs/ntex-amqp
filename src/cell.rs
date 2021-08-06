//! Custom cell impl
use std::cell::UnsafeCell;
use std::ops::Deref;
use std::rc::{Rc, Weak};

pub(crate) struct Cell<T> {
    inner: Rc<UnsafeCell<T>>,
}

pub(crate) struct WeakCell<T> {
    inner: Weak<UnsafeCell<T>>,
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

    pub(crate) fn downgrade(&self) -> WeakCell<T> {
        WeakCell {
            inner: Rc::downgrade(&self.inner),
        }
    }

    pub(crate) fn get_ref(&self) -> &T {
        unsafe { &*self.inner.as_ref().get() }
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_mut(&self) -> &mut T {
        unsafe { &mut *self.inner.as_ref().get() }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for WeakCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> WeakCell<T> {
    pub(crate) fn upgrade(&self) -> Option<Cell<T>> {
        self.inner.upgrade().map(|inner| Cell { inner })
    }
}
