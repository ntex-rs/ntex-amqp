use std::collections::{BTreeMap, VecDeque};

use bytes::{BufMut, Bytes, BytesMut};
use either::Either;
use futures::{future, unsync::oneshot, Future};
use hashbrown::HashMap;
use slab::Slab;
use string::{self, TryFrom};

use amqp_codec::protocol::{
    Accepted, Attach, DeliveryNumber, DeliveryState, Detach, Disposition, Error, Flow, Frame,
    Handle, Outcome, ReceiverSettleMode, Role, SenderSettleMode, Target, TerminusDurability,
    TerminusExpiryPolicy, Transfer, TransferBody, TransferNumber,
};
use amqp_codec::AmqpFrame;

use crate::cell::Cell;
use crate::connection::ConnectionController;
use crate::errors::AmqpTransportError;
use crate::rcvlink::{ReceiverLink, ReceiverLinkInner};
use crate::sndlink::{SenderLink, SenderLinkInner};
use crate::{Configuration, DeliveryPromise};

const INITIAL_OUTGOING_ID: TransferNumber = 0;

#[derive(Clone)]
pub struct Session {
    pub(crate) inner: Cell<SessionInner>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.inner.get_mut().drop_session()
    }
}

impl Session {
    pub(crate) fn new(inner: Cell<SessionInner>) -> Session {
        Session { inner }
    }

    #[inline]
    /// Get remote connection configuration
    pub fn remote_config(&self) -> &Configuration {
        &self.inner.connection.remote_config()
    }

    pub fn close(&self) -> impl Future<Item = (), Error = AmqpTransportError> {
        future::ok(())
    }

    pub fn get_sender_link(&self, name: &str) -> Option<&SenderLink> {
        let inner = self.inner.get_ref();

        if let Some(id) = inner.sender_links.get(name) {
            if let Some(Either::Left(SenderLinkState::Established(ref link))) = inner.links.get(*id)
            {
                return Some(link);
            }
        }
        None
    }

    /// Open sender link
    pub fn open_sender_link<T: Into<String>, U: Into<String>>(
        &mut self,
        address: T,
        name: U,
    ) -> impl Future<Item = SenderLink, Error = AmqpTransportError> {
        let inner = self.inner.get_mut();
        let (tx, rx) = oneshot::channel();

        let entry = inner.links.vacant_entry();
        let token = entry.key();
        entry.insert(Either::Left(SenderLinkState::Opening(tx)));

        let name = string::String::try_from(Bytes::from(name.into())).unwrap();
        let address = string::String::try_from(Bytes::from(address.into())).unwrap();

        let target = Target {
            address: Some(address),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            dynamic: false,
            dynamic_node_properties: None,
            capabilities: None,
        };
        let attach = Attach {
            name: name.clone(),
            handle: token as Handle,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: Some(target),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: Some(65536),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        inner.sender_links.insert(name, token);
        inner.post_frame(Frame::Attach(attach));
        rx.map_err(|_e| AmqpTransportError::Disconnected)
    }

    pub fn detach_receiver_link(
        &mut self,
        handle: usize,
        error: Option<Error>,
    ) -> impl Future<Item = (), Error = AmqpTransportError> {
        let (tx, rx) = oneshot::channel();

        self.inner
            .get_mut()
            .detach_receiver_link(handle, false, error, tx);

        rx.then(|res| match res {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(AmqpTransportError::Disconnected),
        })
    }
}

enum SenderLinkState {
    Opening(oneshot::Sender<SenderLink>),
    Established(SenderLink),
    Closing(SenderLink),
}

enum ReceiverLinkState {
    Opening(Option<Cell<ReceiverLinkInner>>),
    Established(Cell<ReceiverLinkInner>),
    Closing(Option<oneshot::Sender<Result<(), AmqpTransportError>>>),
}

impl SenderLinkState {
    fn is_opening(&self) -> bool {
        match self {
            SenderLinkState::Opening(_) => true,
            _ => false,
        }
    }
}

pub(crate) struct SessionInner {
    id: usize,
    connection: ConnectionController,
    next_outgoing_id: TransferNumber,
    local: bool,

    remote_channel_id: u16,
    next_incoming_id: TransferNumber,
    remote_outgoing_window: u32,
    remote_incoming_window: u32,

    unsettled_deliveries: BTreeMap<DeliveryNumber, DeliveryPromise>,
    links: Slab<Either<SenderLinkState, ReceiverLinkState>>,
    sender_links: HashMap<string::String<Bytes>, usize>,
    remote_handles: HashMap<Handle, usize>,
    pending_transfers: VecDeque<PendingTransfer>,
    error: Option<AmqpTransportError>,
}

struct PendingTransfer {
    link_handle: Handle,
    idx: u32,
    body: TransferBody,
    promise: DeliveryPromise,
}

impl SessionInner {
    pub fn new(
        id: usize,
        local: bool,
        connection: ConnectionController,
        remote_channel_id: u16,
        next_incoming_id: DeliveryNumber,
        remote_incoming_window: u32,
        remote_outgoing_window: u32,
    ) -> SessionInner {
        SessionInner {
            id,
            local,
            connection,
            next_incoming_id,
            remote_channel_id,
            remote_incoming_window,
            remote_outgoing_window,
            next_outgoing_id: INITIAL_OUTGOING_ID,
            unsettled_deliveries: BTreeMap::new(),
            links: Slab::new(),
            sender_links: HashMap::new(),
            remote_handles: HashMap::new(),
            pending_transfers: VecDeque::new(),
            error: None,
        }
    }

    /// Local channel id
    pub fn id(&self) -> u16 {
        self.id as u16
    }

    /// Set error. New operations will return error.
    pub(crate) fn set_error(&mut self, err: AmqpTransportError) {
        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            let _ = tr.promise.send(Err(err.clone()));
        }

        // drop links
        self.sender_links.clear();
        for (_, st) in self.links.iter_mut() {
            match st {
                Either::Left(SenderLinkState::Opening(_)) => (),
                Either::Left(SenderLinkState::Established(ref mut link))
                | Either::Left(SenderLinkState::Closing(ref mut link)) => {
                    link.inner.get_mut().set_error(err.clone())
                }
                _ => (),
            }
        }
        self.links.clear();

        self.error = Some(err);
    }

    fn drop_session(&mut self) {
        self.connection.drop_session_copy(self.id);
    }

    /// Register remote sender link
    pub(crate) fn confirm_sender_link(&mut self, cell: Cell<SessionInner>, attach: Attach) {
        trace!("Remote sender link opened: {:?}", attach.name());
        let entry = self.links.vacant_entry();
        let token = entry.key();
        let delivery_count = attach.initial_delivery_count.unwrap_or(0);

        let mut name = None;
        if let Some(ref source) = attach.source {
            if let Some(ref addr) = source.address {
                name = Some(addr.clone());
                self.sender_links.insert(addr.clone(), token);
            }
        }

        self.remote_handles.insert(attach.handle(), token);
        let link = Cell::new(SenderLinkInner::new(
            token,
            name.unwrap_or_else(|| string::String::default()),
            attach.handle(),
            delivery_count,
            cell,
        ));
        entry.insert(Either::Left(SenderLinkState::Established(SenderLink::new(
            link,
        ))));

        let attach = Attach {
            name: attach.name.clone(),
            handle: token as Handle,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Mixed,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: attach.source.clone(),
            target: attach.target.clone(),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(delivery_count),
            max_message_size: Some(65536),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        self.post_frame(attach.into());
    }

    /// Register receiver link
    pub(crate) fn open_receiver_link(
        &mut self,
        cell: Cell<SessionInner>,
        attach: Attach,
    ) -> ReceiverLink {
        let handle = attach.handle();
        let entry = self.links.vacant_entry();
        let token = entry.key();

        let inner = Cell::new(ReceiverLinkInner::new(cell, token, attach));
        entry.insert(Either::Right(ReceiverLinkState::Opening(Some(
            inner.clone(),
        ))));
        self.remote_handles.insert(handle, token);
        ReceiverLink::new(inner)
    }

    pub(crate) fn confirm_receiver_link(&mut self, token: usize, attach: &Attach) {
        if let Some(Either::Right(link)) = self.links.get_mut(token) {
            match link {
                ReceiverLinkState::Opening(l) => {
                    let attach = Attach {
                        name: attach.name.clone(),
                        handle: token as Handle,
                        role: Role::Receiver,
                        snd_settle_mode: SenderSettleMode::Mixed,
                        rcv_settle_mode: ReceiverSettleMode::First,
                        source: attach.source.clone(),
                        target: attach.target.clone(),
                        unsettled: None,
                        incomplete_unsettled: false,
                        initial_delivery_count: Some(0),
                        max_message_size: Some(65536),
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    };
                    *link = ReceiverLinkState::Established(l.take().unwrap());
                    self.post_frame(attach.into());
                }
                _ => error!("Unexpected receiver link state"),
            }
        }
    }

    /// Close receiver link
    pub(crate) fn detach_receiver_link(
        &mut self,
        id: usize,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpTransportError>>,
    ) {
        if let Some(Either::Right(link)) = self.links.get_mut(id) {
            match link {
                ReceiverLinkState::Opening(inner) => {
                    let attach = Attach {
                        name: inner.as_ref().unwrap().get_ref().name().clone(),
                        handle: id as Handle,
                        role: Role::Sender,
                        snd_settle_mode: SenderSettleMode::Mixed,
                        rcv_settle_mode: ReceiverSettleMode::First,
                        source: None,
                        target: None,
                        unsettled: None,
                        incomplete_unsettled: false,
                        initial_delivery_count: None,
                        max_message_size: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    };
                    let detach = Detach {
                        handle: id as u32,
                        closed,
                        error,
                    };
                    *link = ReceiverLinkState::Closing(Some(tx));
                    self.post_frame(attach.into());
                    self.post_frame(detach.into());
                }
                ReceiverLinkState::Established(_) => {
                    let detach = Detach {
                        handle: id as u32,
                        closed,
                        error,
                    };
                    *link = ReceiverLinkState::Closing(Some(tx));
                    self.post_frame(detach.into());
                }
                ReceiverLinkState::Closing(_) => {
                    let _ = tx.send(Ok(()));
                    error!("Unexpected receiver link state: closing - {}", id);
                }
            }
        } else {
            let _ = tx.send(Ok(()));
            error!("Receiver link does not exist: {}", id);
        }
    }

    pub fn handle_frame(&mut self, frame: AmqpFrame) {
        if self.error.is_none() {
            match frame.into_parts().1 {
                Frame::Disposition(disp) => self.settle_deliveries(disp),
                Frame::Flow(flow) => self.apply_flow(flow),
                Frame::Transfer(transfer) => {
                    let idx = if let Some(idx) = self.remote_handles.get(&transfer.handle()) {
                        *idx
                    } else {
                        error!("Transfer's link {:?} is unknown", transfer.handle());
                        return;
                    };

                    if let Some(link) = self.links.get_mut(idx) {
                        match link {
                            Either::Left(_) => error!("Got trasfer from sender link"),
                            Either::Right(link) => match link {
                                ReceiverLinkState::Opening(_) => {
                                    error!(
                                        "Got transfer for opening link: {} -> {}",
                                        transfer.handle(),
                                        idx
                                    );
                                }
                                ReceiverLinkState::Established(link) => {
                                    // self.outgoing_window -= 1;
                                    let _ = self.next_incoming_id.wrapping_add(1);
                                    link.get_mut().handle_transfer(transfer);
                                }
                                ReceiverLinkState::Closing(_) => (),
                            },
                        }
                    } else {
                        error!(
                            "Remote link handle mapped to non-existing link: {} -> {}",
                            transfer.handle(),
                            idx
                        );
                    }
                }
                frame => error!("Unexpected frame: {:?}", frame),
            }
        }
    }

    /// Handle `Attach` frame. return false if attach frame is remote and can not be handled
    pub fn handle_attach(&mut self, attach: &Attach, cell: Cell<SessionInner>) -> bool {
        let name = attach.name();
        if let Some(index) = self.sender_links.get(name) {
            match self.links.get_mut(*index) {
                Some(Either::Left(item)) => {
                    if item.is_opening() {
                        trace!(
                            "sender link opened: {:?} {} -> {}",
                            name,
                            index,
                            attach.handle()
                        );

                        if item.is_opening() {
                            self.remote_handles.insert(attach.handle(), *index);
                            let delivery_count = attach.initial_delivery_count.unwrap_or(0);
                            let link = Cell::new(SenderLinkInner::new(
                                *index,
                                name.clone(),
                                attach.handle(),
                                delivery_count,
                                cell,
                            ));
                            let local_sender = std::mem::replace(
                                item,
                                SenderLinkState::Established(SenderLink::new(link.clone())),
                            );

                            if let SenderLinkState::Opening(tx) = local_sender {
                                let _ = tx.send(SenderLink::new(link));
                            }
                        }
                    }
                }
                _ => {
                    // TODO: error in proto, have to close connection
                }
            }
            true
        } else {
            // cannot handle remote attach
            false
        }
    }

    /// Handle `Detach` frame.
    pub fn handle_detach(&mut self, detach: &Detach) {
        // get local link instance
        let idx = if let Some(idx) = self.remote_handles.get(&detach.handle()) {
            *idx
        } else {
            // should not happen, error
            return;
        };

        let remove = if let Some(link) = self.links.get_mut(idx) {
            match link {
                Either::Left(link) => match link {
                    SenderLinkState::Opening(_) => true,
                    SenderLinkState::Established(link) => {
                        // detach from remote endpoint
                        let detach = Detach {
                            handle: link.inner.get_ref().id(),
                            closed: true,
                            error: detach.error.clone(),
                        };
                        let err = AmqpTransportError::LinkDetached(detach.error.clone());

                        // remove name
                        self.sender_links.remove(link.inner.name());

                        // drop pending transfers
                        let mut idx = 0;
                        let handle = link.inner.get_ref().remote_handle();
                        while idx < self.pending_transfers.len() {
                            if self.pending_transfers[idx].link_handle == handle {
                                let tr = self.pending_transfers.remove(idx).unwrap();
                                let _ = tr.promise.send(Err(err.clone()));
                            } else {
                                idx += 1;
                            }
                        }

                        // detach snd link
                        link.inner.get_mut().detached(err);
                        self.connection
                            .post_frame(AmqpFrame::new(self.remote_channel_id, detach.into()));
                        true
                    }
                    SenderLinkState::Closing(_) => true,
                },
                Either::Right(link) => match link {
                    ReceiverLinkState::Opening(_) => false,
                    ReceiverLinkState::Established(_) => false,
                    ReceiverLinkState::Closing(tx) => {
                        // detach confirmation
                        if let Some(tx) = tx.take() {
                            if let Some(err) = detach.error.clone() {
                                let _ = tx.send(Err(AmqpTransportError::LinkDetached(Some(err))));
                            } else {
                                let _ = tx.send(Ok(()));
                            }
                        }
                        true
                    }
                },
            }
        } else {
            false
        };

        if remove {
            self.links.remove(idx);
            self.remote_handles.remove(&detach.handle());
        }
    }

    fn settle_deliveries(&mut self, disposition: Disposition) {
        assert!(disposition.settled()); // we can only work with settled for now
        let from = disposition.first;
        let to = disposition.last.unwrap_or(from);
        let actionable = self
            .unsettled_deliveries
            .range(from..to + 1)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        let outcome: Outcome;
        match disposition.state().map(|s| s.clone()) {
            Some(DeliveryState::Received(_v)) => {
                return;
            } // todo: apply more thinking
            Some(DeliveryState::Accepted(v)) => outcome = Outcome::Accepted(v.clone()),
            Some(DeliveryState::Rejected(v)) => outcome = Outcome::Rejected(v.clone()),
            Some(DeliveryState::Released(v)) => outcome = Outcome::Released(v.clone()),
            Some(DeliveryState::Modified(v)) => outcome = Outcome::Modified(v.clone()),
            None => outcome = Outcome::Accepted(Accepted {}),
        }
        // let state = disposition.state().map(|s| s.clone()).unwrap_or(DeliveryState::Accepted(Accepted {})).clone(); // todo: honor Source.default_outcome()
        for k in actionable {
            let _ = self
                .unsettled_deliveries
                .remove(&k)
                .unwrap()
                .send(Ok(outcome.clone()));
        }
    }

    fn apply_flow(&mut self, flow: Flow) {
        // # AMQP1.0 2.5.6
        self.next_incoming_id = flow.next_outgoing_id();
        self.remote_outgoing_window = flow.outgoing_window();

        self.remote_incoming_window = flow
            .next_incoming_id()
            .unwrap_or(INITIAL_OUTGOING_ID)
            .saturating_add(flow.incoming_window())
            .saturating_sub(self.next_outgoing_id);

        trace!(
            "session received credit. window: {}, pending: {}",
            self.remote_outgoing_window,
            self.pending_transfers.len()
        );

        while let Some(t) = self.pending_transfers.pop_front() {
            self.send_transfer(t.link_handle, t.idx, t.body, t.promise);
            if self.remote_outgoing_window == 0 {
                break;
            }
        }

        // apply link flow
        if let Some(Either::Left(link)) = flow.handle().and_then(|h| self.links.get_mut(h as usize))
        {
            match link {
                SenderLinkState::Established(ref mut link) => {
                    link.inner.get_mut().apply_flow(&flow);
                }
                _ => warn!("Received flow frame"),
            }
        }
        if flow.echo() {
            self.send_flow();
        }
    }

    fn send_flow(&mut self) {
        let flow = Flow {
            next_incoming_id: if self.local {
                Some(self.next_incoming_id)
            } else {
                None
            },
            incoming_window: std::u32::MAX,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.remote_incoming_window,
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: false,
            echo: false,
            properties: None,
        };
        self.post_frame(flow.into());
    }

    pub(crate) fn rcv_link_flow(&mut self, handle: u32, delivery_count: u32, credit: u32) {
        let flow = Flow {
            next_incoming_id: if self.local {
                Some(self.next_incoming_id)
            } else {
                None
            },
            incoming_window: std::u32::MAX,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.remote_incoming_window,
            handle: Some(handle),
            delivery_count: Some(delivery_count),
            link_credit: Some(credit),
            available: None,
            drain: false,
            echo: false,
            properties: None,
        };
        self.post_frame(flow.into());
    }

    pub fn post_frame(&mut self, frame: Frame) {
        self.connection
            .post_frame(AmqpFrame::new(self.remote_channel_id, frame));
    }

    pub fn send_transfer(
        &mut self,
        link_handle: Handle,
        idx: u32,
        body: TransferBody,
        promise: DeliveryPromise,
    ) {
        if self.remote_incoming_window == 0 {
            self.pending_transfers.push_back(PendingTransfer {
                link_handle,
                idx,
                body,
                promise,
            });
            return;
        }
        let frame = self.prepare_transfer(link_handle, body, promise);
        self.post_frame(frame);
    }

    pub fn prepare_transfer(
        &mut self,
        link_handle: Handle,
        body: TransferBody,
        promise: DeliveryPromise,
    ) -> Frame {
        let delivery_id = self.next_outgoing_id;

        let mut buf = BytesMut::new();
        buf.put_u32_be(delivery_id);

        self.next_outgoing_id += 1;
        self.remote_incoming_window -= 1;

        let transfer = Transfer {
            handle: link_handle,
            delivery_id: Some(delivery_id),
            delivery_tag: Some(buf.freeze()),
            message_format: body.message_format(),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
            body: Some(body),
        };
        self.unsettled_deliveries.insert(delivery_id, promise);
        Frame::Transfer(transfer)
    }
}
