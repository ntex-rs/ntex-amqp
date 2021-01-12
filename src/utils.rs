use std::task::{Context, Poll};
use std::{future::Future, pin::Pin};

use either::Either;

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
