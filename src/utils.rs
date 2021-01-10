use std::task::{Context, Poll};
use std::{convert::TryFrom, future::Future, pin::Pin};

use either::Either;
use ntex::service::Service;

/// Unwrap result and return `err` future
///
/// Err(e) get converted to err(e)
macro_rules! try_ready_err {
    ($e:expr) => {
        match $e {
            Ok(value) => value,
            Err(e) => return futures::future::err(e),
        }
    };
}

macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            return Err($fmt, $($arg)+);
        }
    };
}

pin_project_lite::pin_project! {
    pub(crate) struct Select<A, B> {
        #[pin]
        fut_a: A,
        #[pin]
        fut_b: B,
    }
}

impl<A, B> Select<A, B> {
    pub(crate) fn new(fut_a: A, fut_b: B) -> Self {
        Self { fut_a, fut_b }
    }
}

impl<A, B> Future for Select<A, B>
where
    A: Future,
    B: Future,
{
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Poll::Ready(item) = this.fut_a.poll(cx) {
            return Poll::Ready(Either::Left(item));
        }

        if let Poll::Ready(item) = this.fut_b.poll(cx) {
            return Poll::Ready(Either::Right(item));
        }

        Poll::Pending
    }
}

/// Check service readiness
pub(crate) fn ready<S>(service: &S) -> Ready<'_, S> {
    Ready(service)
}

pub(crate) struct Ready<'a, S>(&'a S);

impl<'a, S> Unpin for Ready<'a, S> {}

impl<'a, S: Service> Future for Ready<'a, S> {
    type Output = Result<(), S::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_ready(cx)
    }
}
