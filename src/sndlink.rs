use std::collections::VecDeque;
use std::future::Future;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use futures::future::{ok, Either};
use ntex::channel::{condition, oneshot};
use ntex_amqp_codec::protocol::{
    Attach, DeliveryNumber, DeliveryState, Disposition, Error, Flow, MessageFormat,
    ReceiverSettleMode, Role, SenderSettleMode, SequenceNo, Target, TerminusDurability,
    TerminusExpiryPolicy, TransferBody,
};
use ntex_amqp_codec::Encode;

use crate::cell::Cell;
use crate::error::AmqpProtocolError;
use crate::session::{Session, SessionInner, TransferState};
use crate::{Delivery, DeliveryPromise, Handle};

#[derive(Clone)]
pub struct SenderLink {
    pub(crate) inner: Cell<SenderLinkInner>,
}

impl std::fmt::Debug for SenderLink {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_tuple("SenderLink")
            .field(&std::ops::Deref::deref(&self.inner.get_ref().name))
            .finish()
    }
}

pub(crate) struct SenderLinkInner {
    pub(crate) id: usize,
    idx: u32,
    name: ByteString,
    session: Session,
    remote_handle: Handle,
    delivery_count: SequenceNo,
    link_credit: u32,
    pending_transfers: VecDeque<PendingTransfer>,
    error: Option<AmqpProtocolError>,
    closed: bool,
    on_close: condition::Condition,
}

struct PendingTransfer {
    idx: u32,
    tag: Option<Bytes>,
    body: Option<TransferBody>,
    promise: Option<DeliveryPromise>,
    settle: Option<bool>,
    more: TransferState,
    message_format: Option<MessageFormat>,
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

    pub fn settle_message(&self, id: DeliveryNumber, state: DeliveryState) {
        self.inner.get_mut().settle_message(id, state)
    }

    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        self.inner.get_mut().close(None)
    }

    pub fn close_with_error(
        &self,
        error: Error,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        self.inner.get_mut().close(Some(error))
    }

    pub fn on_close(&self) -> condition::Waiter {
        self.inner.get_ref().on_close.wait()
    }
}

impl SenderLinkInner {
    pub(crate) fn new(
        id: usize,
        name: ByteString,
        handle: Handle,
        delivery_count: SequenceNo,
        session: Cell<SessionInner>,
    ) -> SenderLinkInner {
        SenderLinkInner {
            id,
            name,
            delivery_count,
            idx: 0,
            session: Session::new(session),
            remote_handle: handle,
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            error: None,
            closed: false,
            on_close: condition::Condition::new(),
        }
    }

    pub(crate) fn with(frame: &Attach, session: Cell<SessionInner>) -> SenderLinkInner {
        let mut name = None;
        if let Some(ref source) = frame.source {
            if let Some(ref addr) = source.address {
                name = Some(addr.clone());
            }
        }
        let delivery_count = frame.initial_delivery_count.unwrap_or(0);

        SenderLinkInner {
            delivery_count,
            id: 0,
            idx: 0,
            name: name.unwrap_or_else(ByteString::default),
            session: Session::new(session),
            remote_handle: frame.handle(),
            link_credit: 0,
            pending_transfers: VecDeque::new(),
            error: None,
            closed: false,
            on_close: condition::Condition::new(),
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

    pub(crate) fn detached(&mut self, err: AmqpProtocolError) {
        trace!("Detaching sender link {:?} with error {:?}", self.name, err);

        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            if let Some(tx) = tr.promise {
                let _ = tx.send(Err(err.clone()));
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
            Either::Left(ok(()))
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
                flow.delivery_count.unwrap_or(0),
                self.delivery_count
            );

            let delta = flow
                .delivery_count
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
                        transfer.idx,
                        transfer.body,
                        transfer.promise,
                        transfer.tag,
                        transfer.settle,
                        transfer.more,
                        transfer.message_format,
                    );
                } else {
                    break;
                }
            }
        }

        if flow.echo() {
            // todo: send flow
        }
    }

    pub(crate) fn send<T: Into<TransferBody>>(&mut self, body: T, tag: Option<Bytes>) -> Delivery {
        if let Some(ref err) = self.error {
            Delivery::Resolved(Err(err.clone()))
        } else {
            let body = body.into();
            let message_format = body.message_format();
            let (delivery_tx, delivery_rx) = oneshot::channel();

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
                self.send_inner(
                    chunk.into(),
                    tag,
                    TransferState::First,
                    Some(delivery_tx),
                    message_format,
                );

                loop {
                    let chunk = body.split_to(std::cmp::min(max_frame_size, body.len()));

                    // last chunk
                    if body.is_empty() {
                        self.send_inner(
                            chunk.into(),
                            None,
                            TransferState::Last,
                            None,
                            message_format,
                        );
                        break;
                    } else {
                        self.send_inner(
                            chunk.into(),
                            None,
                            TransferState::Continue,
                            None,
                            message_format,
                        );
                    }
                }
            } else {
                self.send_inner(
                    body,
                    tag,
                    TransferState::Only,
                    Some(delivery_tx),
                    message_format,
                );
            }

            Delivery::Pending(delivery_rx)
        }
    }

    fn send_inner(
        &mut self,
        body: TransferBody,
        tag: Option<Bytes>,
        more: TransferState,
        promise: Option<DeliveryPromise>,
        message_format: Option<MessageFormat>,
    ) {
        if self.link_credit == 0 {
            log::trace!(
                "Sender link credit is 0, push to pending queue hnd:{} {:?}, queue size: {}",
                self.id as u32,
                tag,
                self.pending_transfers.len()
            );
            self.pending_transfers.push_back(PendingTransfer {
                tag,
                promise,
                more,
                message_format,
                settle: Some(false),
                body: Some(body),
                idx: self.idx,
            });
        } else {
            self.link_credit -= 1;
            self.delivery_count = self.delivery_count.saturating_add(1);
            self.session.inner.get_mut().send_transfer(
                self.id as u32,
                self.idx,
                Some(body),
                promise,
                tag,
                None,
                more,
                message_format,
            );
        }
        self.idx = self.idx.saturating_add(1);
    }

    pub(crate) fn settle_message(&mut self, id: DeliveryNumber, state: DeliveryState) {
        let disp = Disposition {
            role: Role::Sender,
            first: id,
            last: None,
            settled: true,
            state: Some(state),
            batchable: false,
        };
        let _ = self.session.inner.get_mut().post_frame(disp.into());
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
        let frame = Attach {
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
        };

        SenderLinkBuilder { frame, session }
    }

    pub fn max_message_size(mut self, size: u64) -> Self {
        self.frame.max_message_size = Some(size);
        self
    }

    pub fn with_frame<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Attach),
    {
        f(&mut self.frame);
        self
    }

    pub async fn open(self) -> Result<SenderLink, AmqpProtocolError> {
        let result = self.session.get_mut().open_sender_link(self.frame).await;

        match result {
            Ok(Ok(link)) => Ok(link),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AmqpProtocolError::Disconnected),
        }
    }
}
