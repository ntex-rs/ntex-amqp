use std::collections::VecDeque;
use std::{future::Future, mem, pin::Pin, task::Context, task::Poll};

use ntex::channel::{condition, oneshot, pool};
use ntex::util::{BufMut, ByteString, Bytes, BytesMut, Either, Ready};
use ntex_amqp_codec::protocol::{
    self as codec, Attach, DeliveryNumber, DeliveryState, Disposition, Error, Flow, MessageFormat,
    ReceiverSettleMode, Role, SenderSettleMode, SequenceNo, Target, TerminusDurability,
    TerminusExpiryPolicy, TransferBody,
};
use ntex_amqp_codec::Encode;

use crate::cell::Cell;
use crate::error::AmqpProtocolError;
use crate::session::{Session, SessionInner, TransferState};
use crate::Handle;

#[derive(Clone)]
pub struct SenderLink {
    pub(crate) inner: Cell<SenderLinkInner>,
}

pub(crate) struct SenderLinkInner {
    pub(crate) id: usize,
    name: ByteString,
    session: Session,
    remote_handle: Handle,
    delivery_count: SequenceNo,
    delivery_tag: u32,
    link_credit: u32,
    pending_transfers: VecDeque<PendingTransfer>,
    error: Option<AmqpProtocolError>,
    closed: bool,
    on_close: condition::Condition,
    on_disposition: Box<dyn Fn(Bytes, Result<Disposition, AmqpProtocolError>)>,
    max_message_size: Option<usize>,
}

struct PendingTransfer {
    body: Option<TransferBody>,
    state: TransferState,
    settle: Option<bool>,
    message_format: Option<MessageFormat>,
}

impl std::fmt::Debug for SenderLink {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_tuple("SenderLink")
            .field(&std::ops::Deref::deref(&self.inner.get_ref().name))
            .finish()
    }
}

impl std::fmt::Debug for SenderLinkInner {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_tuple("SenderLinkInner")
            .field(&std::ops::Deref::deref(&self.name))
            .finish()
    }
}

impl SenderLink {
    pub(crate) fn new(inner: Cell<SenderLinkInner>) -> SenderLink {
        SenderLink { inner }
    }

    pub fn id(&self) -> u32 {
        self.inner.id as u32
    }

    pub fn name(&self) -> &ByteString {
        &self.inner.name
    }

    pub fn remote_handle(&self) -> Handle {
        self.inner.remote_handle
    }

    pub fn session(&self) -> &Session {
        &self.inner.get_ref().session
    }

    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.inner.get_mut().session
    }

    pub fn send<T>(&self, body: T) -> impl Future<Output = Result<Disposition, AmqpProtocolError>>
    where
        T: Into<TransferBody>,
    {
        self.inner.get_mut().send(body, None)
    }

    pub fn send_with_tag<T>(
        &self,
        body: T,
        tag: Bytes,
    ) -> impl Future<Output = Result<Disposition, AmqpProtocolError>>
    where
        T: Into<TransferBody>,
    {
        self.inner.get_mut().send(body, Some(tag))
    }

    pub fn send_no_block<T>(&self, body: T) -> Result<Bytes, AmqpProtocolError>
    where
        T: Into<TransferBody>,
    {
        self.inner
            .get_mut()
            .send_no_block(body, None, self.inner.clone())
    }

    pub fn send_no_block_with_tag<T>(&self, body: T, tag: Bytes) -> Result<Bytes, AmqpProtocolError>
    where
        T: Into<TransferBody>,
    {
        self.inner
            .get_mut()
            .send_no_block(body, Some(tag), self.inner.clone())
    }

    pub fn settle_message(&self, id: DeliveryNumber, state: DeliveryState) {
        self.inner.get_mut().settle_message(id, state);
    }

    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        self.inner.get_mut().close(None)
    }

    pub fn close_with_error<E>(
        &self,
        error: E,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>>
    where
        Error: From<E>,
    {
        self.inner.get_mut().close(Some(error.into()))
    }

    pub fn on_close(&self) -> condition::Waiter {
        self.inner.get_ref().on_close.wait()
    }

    pub fn on_disposition<F>(&self, f: F)
    where
        F: Fn(Bytes, Result<Disposition, AmqpProtocolError>) + 'static,
    {
        self.inner.get_mut().on_disposition = Box::new(f);
    }

    pub fn set_max_message_size(&self, value: usize) {
        self.inner.get_mut().max_message_size = Some(value)
    }
}

impl SenderLinkInner {
    pub(crate) fn new(
        id: usize,
        name: ByteString,
        handle: Handle,
        delivery_count: SequenceNo,
        session: Cell<SessionInner>,
        max_message_size: Option<usize>,
    ) -> SenderLinkInner {
        SenderLinkInner {
            id,
            name,
            delivery_count,
            session: Session::new(session),
            remote_handle: handle,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            error: None,
            closed: false,
            delivery_tag: 0,
            on_close: condition::Condition::new(),
            on_disposition: Box::new(|_, _| ()),
            max_message_size,
        }
    }

    pub(crate) fn with(frame: &Attach, session: Cell<SessionInner>) -> SenderLinkInner {
        let mut name = None;
        if let Some(source) = frame.source() {
            if let Some(ref addr) = source.address {
                name = Some(addr.clone());
            }
        }
        let delivery_count = frame.initial_delivery_count().unwrap_or(0);

        SenderLinkInner {
            delivery_count,
            id: 0,
            name: name.unwrap_or_else(ByteString::default),
            session: Session::new(session),
            remote_handle: frame.handle(),
            link_credit: 0,
            delivery_tag: 0,
            pending_transfers: VecDeque::new(),
            error: None,
            closed: false,
            on_close: condition::Condition::new(),
            on_disposition: Box::new(|_, _| ()),
            max_message_size: Some(65536),
        }
    }

    pub(crate) fn id(&self) -> u32 {
        self.id as u32
    }

    pub(crate) fn remote_handle(&self) -> Handle {
        self.remote_handle
    }

    pub(crate) fn name(&self) -> &ByteString {
        &self.name
    }

    pub(crate) fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }

    pub(crate) fn detached(&mut self, err: AmqpProtocolError) {
        trace!("Detaching sender link {:?} with error {:?}", self.name, err);

        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            if let TransferState::First(tx, _) | TransferState::Only(tx, _) = tr.state {
                tx.ready(Err(err.clone()));
            }
        }

        self.error = Some(err);
        self.on_close.notify();
    }

    pub(crate) fn close(
        &mut self,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        if self.closed {
            Either::Left(Ready::Ok(()))
        } else {
            self.closed = true;
            self.on_close.notify();

            let (tx, rx) = oneshot::channel();

            self.session
                .inner
                .get_mut()
                .detach_sender_link(self.id, true, error, tx);

            Either::Right(async move {
                match rx.await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(AmqpProtocolError::Disconnected),
                }
            })
        }
    }

    pub(crate) fn apply_flow(&mut self, flow: &Flow) {
        // #2.7.6
        if let Some(credit) = flow.link_credit() {
            trace!(
                "Apply sender link {:?} flow, credit: {:?} flow count: {:?}, delivery count: {:?}",
                self.name,
                credit,
                flow.delivery_count().unwrap_or(0),
                self.delivery_count
            );

            let delta = flow
                .delivery_count()
                .unwrap_or(0)
                .saturating_add(credit)
                .saturating_sub(self.delivery_count);
            self.link_credit += delta;

            let session = self.session.inner.get_mut();

            // credit became available => drain pending_transfers
            while self.link_credit > 0 {
                if let Some(transfer) = self.pending_transfers.pop_front() {
                    self.link_credit -= 1;
                    self.delivery_count = self.delivery_count.saturating_add(1);
                    session.send_transfer(
                        self.id as u32,
                        transfer.body,
                        transfer.state,
                        transfer.settle,
                        transfer.message_format,
                    );
                } else {
                    break;
                }
            }
        }
    }

    fn send<T: Into<TransferBody>>(&mut self, body: T, tag: Option<Bytes>) -> Delivery {
        if let Some(ref err) = self.error {
            Delivery::Resolved(Err(err.clone()))
        } else {
            let body = body.into();
            let message_format = body.message_format();

            if let Some(limit) = self.max_message_size {
                if body.len() > limit {
                    return Delivery::Resolved(Err(AmqpProtocolError::BodyTooLarge));
                }
            }

            let (delivery_tx, delivery_rx) = self.session.inner.get_ref().pool.channel();

            let max_frame_size = self.session.inner.get_ref().max_frame_size();
            let max_frame_size = if max_frame_size > 2048 {
                max_frame_size - 2048
            } else if max_frame_size == 0 {
                usize::MAX
            } else {
                max_frame_size
            };

            // body is larger than allowed frame size, send body as a set of transfers
            if body.len() > max_frame_size {
                let mut body = match body {
                    TransferBody::Data(data) => data,
                    TransferBody::Message(msg) => {
                        let mut buf = BytesMut::with_capacity(msg.encoded_size());
                        msg.encode(&mut buf);
                        buf.freeze()
                    }
                };

                let chunk = body.split_to(std::cmp::min(max_frame_size, body.len()));
                let tag = self.get_tag(tag);

                self.send_inner(
                    chunk.into(),
                    message_format,
                    TransferState::First(DeliveryPromise::new(delivery_tx), tag),
                );

                loop {
                    let chunk = body.split_to(std::cmp::min(max_frame_size, body.len()));

                    // last chunk
                    if body.is_empty() {
                        self.send_inner(chunk.into(), message_format, TransferState::Last);
                        break;
                    }

                    self.send_inner(chunk.into(), message_format, TransferState::Continue);
                }
            } else {
                let st = TransferState::Only(DeliveryPromise::new(delivery_tx), self.get_tag(tag));
                self.send_inner(body, message_format, st);
            }

            Delivery::Pending(delivery_rx)
        }
    }

    fn send_no_block<T: Into<TransferBody>>(
        &mut self,
        body: T,
        tag: Option<Bytes>,
        link: Cell<SenderLinkInner>,
    ) -> Result<Bytes, AmqpProtocolError> {
        if let Some(ref err) = self.error {
            Err(err.clone())
        } else {
            let body = body.into();
            let message_format = body.message_format();

            if let Some(limit) = self.max_message_size {
                if body.len() > limit {
                    return Err(AmqpProtocolError::BodyTooLarge);
                }
            }

            let max_frame_size = self.session.inner.get_ref().max_frame_size();
            let max_frame_size = if max_frame_size > 2048 {
                max_frame_size - 2048
            } else if max_frame_size == 0 {
                usize::MAX
            } else {
                max_frame_size
            };

            // body is larger than allowed frame size, send body as a set of transfers
            if body.len() > max_frame_size {
                let mut body = match body {
                    TransferBody::Data(data) => data,
                    TransferBody::Message(msg) => {
                        let mut buf = BytesMut::with_capacity(msg.encoded_size());
                        msg.encode(&mut buf);
                        buf.freeze()
                    }
                };

                let chunk = body.split_to(std::cmp::min(max_frame_size, body.len()));
                let tag = self.get_tag(tag);

                self.send_inner(
                    chunk.into(),
                    message_format,
                    TransferState::First(DeliveryPromise::new_link(link, tag.clone()), tag.clone()),
                );

                loop {
                    let chunk = body.split_to(std::cmp::min(max_frame_size, body.len()));

                    // last chunk
                    if body.is_empty() {
                        self.send_inner(chunk.into(), message_format, TransferState::Last);
                        break;
                    }

                    self.send_inner(chunk.into(), message_format, TransferState::Continue);
                }
                Ok(tag)
            } else {
                let tag = self.get_tag(tag);
                let st =
                    TransferState::Only(DeliveryPromise::new_link(link, tag.clone()), tag.clone());
                self.send_inner(body, message_format, st);
                Ok(tag)
            }
        }
    }

    fn send_inner(
        &mut self,
        body: TransferBody,
        message_format: Option<MessageFormat>,
        state: TransferState,
    ) {
        if self.link_credit == 0 {
            log::trace!(
                "Sender link credit is 0, push to pending queue hnd:{} {:?}, queue size: {}",
                self.id as u32,
                state,
                self.pending_transfers.len()
            );
            self.pending_transfers.push_back(PendingTransfer {
                state,
                message_format,
                settle: Some(false),
                body: Some(body),
            });
        } else {
            self.link_credit -= 1;
            self.delivery_count = self.delivery_count.saturating_add(1);
            self.session.inner.get_mut().send_transfer(
                self.id as u32,
                Some(body),
                state,
                None,
                message_format,
            );
        }
    }

    pub(crate) fn settle_message(&mut self, id: DeliveryNumber, state: DeliveryState) {
        let disp = Disposition(Box::new(codec::DispositionInner {
            role: Role::Sender,
            first: id,
            last: None,
            settled: true,
            state: Some(state),
            batchable: false,
        }));
        self.session.inner.get_mut().post_frame(disp.into());
    }

    fn get_tag(&mut self, tag: Option<Bytes>) -> Bytes {
        tag.unwrap_or_else(|| {
            let delivery_tag = self.delivery_tag;
            self.delivery_tag = delivery_tag.saturating_add(1);

            let mut buf = BytesMut::new();
            buf.put_u32(delivery_tag);
            buf.freeze()
        })
    }
}

pub struct SenderLinkBuilder {
    frame: Attach,
    session: Cell<SessionInner>,
}

impl SenderLinkBuilder {
    pub(crate) fn new(name: ByteString, address: ByteString, session: Cell<SessionInner>) -> Self {
        let target = Target {
            address: Some(address),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            dynamic: false,
            dynamic_node_properties: None,
            capabilities: None,
        };
        let frame = Attach(Box::new(codec::AttachInner {
            name,
            handle: 0_u32,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: Some(target),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: Some(65536 * 4),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }));

        SenderLinkBuilder { frame, session }
    }

    pub fn max_message_size(mut self, size: u64) -> Self {
        self.frame.0.max_message_size = Some(size);
        self
    }

    pub fn with_frame<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Attach),
    {
        f(&mut self.frame);
        self
    }

    pub async fn attach(self) -> Result<SenderLink, AmqpProtocolError> {
        let result = self
            .session
            .get_mut()
            .attach_local_sender_link(self.frame)
            .await;

        match result {
            Ok(Ok(inner)) => Ok(SenderLink { inner }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AmqpProtocolError::Disconnected),
        }
    }

    #[doc(hidden)]
    pub async fn open(self) -> Result<SenderLink, AmqpProtocolError> {
        self.attach().await
    }
}

enum Delivery {
    Resolved(Result<Disposition, AmqpProtocolError>),
    Pending(pool::Receiver<Result<Disposition, AmqpProtocolError>>),
    Gone,
}

impl Future for Delivery {
    type Output = Result<Disposition, AmqpProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Delivery::Pending(ref mut receiver) = *self {
            return match Pin::new(receiver).poll(cx) {
                Poll::Ready(Ok(r)) => Poll::Ready(r),
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => {
                    trace!("delivery oneshot is gone: {:?}", e);
                    Poll::Ready(Err(AmqpProtocolError::Disconnected))
                }
            };
        }

        let old_v = mem::replace(&mut *self, Delivery::Gone);
        if let Delivery::Resolved(r) = old_v {
            return match r {
                Ok(state) => Poll::Ready(Ok(state)),
                Err(e) => Poll::Ready(Err(e)),
            };
        }
        panic!("Polling Delivery after it was polled as ready is an error.");
    }
}

#[derive(Debug)]
pub(crate) struct DeliveryPromise(
    Either<pool::Sender<Result<Disposition, AmqpProtocolError>>, (Cell<SenderLinkInner>, Bytes)>,
);

impl DeliveryPromise {
    fn new(tx: pool::Sender<Result<Disposition, AmqpProtocolError>>) -> Self {
        DeliveryPromise(Either::Left(tx))
    }

    fn new_link(link: Cell<SenderLinkInner>, tag: Bytes) -> Self {
        DeliveryPromise(Either::Right((link, tag)))
    }

    pub(crate) fn ready(self, result: Result<Disposition, AmqpProtocolError>) {
        match self.0 {
            Either::Left(tx) => {
                let _r = tx.send(result);
            }
            Either::Right((inner, tag)) => (*inner.get_ref().on_disposition)(tag, result),
        }
    }
}
