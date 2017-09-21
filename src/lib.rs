#![feature(proc_macro, conservative_impl_trait, generators)]
#![feature(trace_macros)]

extern crate bytes;
extern crate chrono;
extern crate uuid;
extern crate ordered_float;
#[macro_use]
extern crate error_chain;
extern crate tokio_io;

//use futures::prelude::*;

#[macro_use]
pub mod codec;
pub mod framing;
pub mod types;
mod errors;
pub use errors::*; // todo: revisit API guidelines for this
pub mod io;
pub mod protocol;