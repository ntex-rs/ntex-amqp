use futures::prelude::*;
use futures::task::{self, Task};
use futures::unsync::oneshot;
use futures::{Async, Future};
use std::cell::RefCell;
use std::rc::Rc;

use crate::errors::*;
use crate::protocol::*;

mod codec;
mod connection;
mod link;
mod message;
mod sasl;
mod session;

pub use self::sasl::sasl_connect_service;

pub use self::connection::*;
pub use self::link::*;
pub use self::message::*;
pub use self::session::*;

pub enum Delivery {
    Resolved(Result<Outcome>),
    Pending(oneshot::Receiver<Result<Outcome>>),
    Gone,
}

type DeliveryPromise = oneshot::Sender<Result<Outcome>>;

impl Future for Delivery {
    type Item = Outcome;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Delivery::Pending(ref mut receiver) = *self {
            return match receiver.poll() {
                Ok(Async::Ready(r)) => r.map(|state| Async::Ready(state)),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e.into()),
            };
        }

        let old_v = ::std::mem::replace(self, Delivery::Gone);
        if let Delivery::Resolved(r) = old_v {
            return match r {
                Ok(state) => Ok(Async::Ready(state)),
                Err(e) => Err(e),
            };
        }
        panic!("Polling Delivery after it was polled as ready is an error.");
    }
}

struct HandleVec<T> {
    items: Vec<Option<T>>,
    empty_count: u32,
    //max_handle: u32 todo: do we need max_handle checks in HandleVec?
}

impl<T: Clone> HandleVec<T> {
    pub fn new() -> HandleVec<T> {
        HandleVec {
            items: Vec::<Option<T>>::with_capacity(4),
            empty_count: 0,
            //max_handle: max_handle
        }
    }

    pub fn push(&mut self, item: T) -> Handle {
        if self.empty_count == 0 {
            let len = self.items.len();
            // ensure!(len <= self.max_handle, "Handle pool is exhausted");
            self.items.push(Some(item));
            return len as Handle;
        }
        let index = self
            .items
            .iter()
            .position(|i| i.is_none())
            .expect("empty_count got out of sync.");
        self.items[index as usize] = Some(item);
        self.empty_count -= 1;
        index as Handle
    }

    pub fn get(&self, handle: Handle) -> Option<T> {
        if let Some(ref r) = self.items[handle as usize] {
            return Some(r.clone());
        }
        None
    }

    pub fn set(&mut self, handle: Handle, item: T) {
        let handle = handle as usize;
        let len = self.items.len();
        if handle >= len {
            self.empty_count += (handle + 1 - len) as u32;
            while self.items.len() <= handle {
                self.items.push(None); // resize_default(handle + 1);
            }
        }
        if self.items[handle as usize].is_some() {
            panic!("handle is set while it must not be");
        }
        self.items[handle as usize] = Some(item);
        self.empty_count -= 1;
    }

    pub fn remove(&mut self, handle: Handle) -> Option<T> {
        self.empty_count += 1;
        self.items[handle as usize].take()
    }
}
