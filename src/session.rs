use std::collections::{BTreeMap, HashMap, VecDeque};

use bytes::Bytes;
use either::Either;
use futures::{future, unsync::oneshot, Future};
use slab::Slab;
use string::{self, TryFrom};
use uuid::Uuid;

use amqp::protocol::{
    Accepted, Attach, DeliveryNumber, DeliveryState, Detach, Disposition, Error, Flow, Frame,
    Handle, Outcome, ReceiverSettleMode, Role, SenderSettleMode, Target, TerminusDurability,
    TerminusExpiryPolicy, Transfer,
};
use amqp::AmqpFrame;

use crate::cell::Cell;
use crate::connection::ConnectionController;
use crate::errors::AmqpTransportError;
use crate::link::{ReceiverLink, ReceiverLinkInner, SenderLink, SenderLinkInner};
use crate::message::Message;
use crate::DeliveryPromise;

#[derive(Clone)]
pub struct Session {
    inner: Cell<SessionInner>,
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

    pub fn close(&self) -> impl Future<Item = (), Error = AmqpTransportError> {
        future::ok(())
    }

    /// Open sender link
    pub fn open_sender_link<T: Into<String>, U: Into<String>>(
        &mut self,
        address: T,
        name: U,
    ) -> impl Future<Item = SenderLink, Error = AmqpTransportError> {
        let cell = self.inner.clone();
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
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
            body: None,
        };
        inner.pending_links.insert(name, token);
        inner.post_frame(Frame::Attach(attach));
        rx.map_err(|_e| AmqpTransportError::Disconnected)
    }
}

enum SenderLinkState {
    Opening(oneshot::Sender<SenderLink>),
    Established(Cell<SenderLinkInner>),
    Closing(Cell<SenderLinkInner>),
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
    remote_channel_id: u16,
    next_outgoing_id: DeliveryNumber,
    outgoing_window: u32,
    next_incoming_id: DeliveryNumber,
    incoming_window: u32,
    unsettled_deliveries: BTreeMap<DeliveryNumber, DeliveryPromise>,
    links: Slab<Either<SenderLinkState, ReceiverLinkState>>,
    remote_handles: HashMap<Handle, usize>,
    pending_links: HashMap<string::String<Bytes>, usize>,
    pending_transfers: VecDeque<PendingTransfer>,
    error: Option<AmqpTransportError>,
}

struct PendingTransfer {
    link_handle: Handle,
    message: Message,
    promise: DeliveryPromise,
}

impl SessionInner {
    pub fn new(
        id: usize,
        connection: ConnectionController,
        remote_channel_id: u16,
        outgoing_window: u32,
        next_incoming_id: DeliveryNumber,
        incoming_window: u32,
    ) -> SessionInner {
        SessionInner {
            id,
            connection,
            remote_channel_id,
            next_outgoing_id: 1,
            outgoing_window: outgoing_window,
            next_incoming_id,
            incoming_window,
            unsettled_deliveries: BTreeMap::new(),
            links: Slab::new(),
            remote_handles: HashMap::new(),
            pending_links: HashMap::new(),
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
        self.pending_links.clear();
        for (_, st) in self.links.iter_mut() {
            match st {
                Either::Left(SenderLinkState::Opening(_)) => (),
                Either::Left(SenderLinkState::Established(ref mut link))
                | Either::Left(SenderLinkState::Closing(ref mut link)) => {
                    link.get_mut().set_error(err.clone())
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

    /// Register remote link
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

    pub(crate) fn confirm_receiver_link(&mut self, idx: usize, attach: &Attach) {
        if let Some(Either::Right(link)) = self.links.get_mut(idx) {
            match link {
                ReceiverLinkState::Opening(l) => {
                    let attach = Attach {
                        name: attach.name.clone(),
                        handle: idx as Handle,
                        role: Role::Receiver,
                        snd_settle_mode: SenderSettleMode::Mixed,
                        rcv_settle_mode: ReceiverSettleMode::First,
                        source: attach.source.clone(),
                        target: attach.target.clone(),
                        unsettled: None,
                        incomplete_unsettled: false,
                        initial_delivery_count: None,
                        max_message_size: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                        body: None,
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
                        role: Role::Receiver,
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
                        body: None,
                    };
                    let detach = Detach {
                        handle: id as u32,
                        closed,
                        error,
                        body: None,
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
                        body: None,
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
            match *frame.performative() {
                Frame::Disposition(ref disp) => self.settle_deliveries(disp),
                Frame::Flow(ref flow) => self.apply_flow(flow),
                _ => error!("Unexpected frame: {:?}", frame),
            }
        }
    }

    /// Handle `Attach` frame. return false if attach frame is remote and can not be handled
    pub fn handle_attach(&mut self, attach: &Attach, cell: Cell<SessionInner>) -> bool {
        let name = attach.name();
        if let Some(index) = self.pending_links.remove(name) {
            match self.links.get_mut(index) {
                Some(Either::Left(item)) => {
                    if item.is_opening() {
                        trace!("sender link opened: {:?}", name);

                        if item.is_opening() {
                            self.remote_handles.insert(attach.handle(), index);
                            let link =
                                Cell::new(SenderLinkInner::new(cell, index, attach.handle()));
                            let local_sender =
                                std::mem::replace(item, SenderLinkState::Established(link.clone()));

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
                    SenderLinkState::Opening(tx) => {
                        println!("sender-link: opening");
                        true
                    }
                    SenderLinkState::Established(link) => {
                        // detach from remote endpoint
                        let detach = Detach {
                            handle: link.get_ref().id(),
                            closed: true,
                            error: None,
                            body: None,
                        };

                        link.get_mut()
                            .detached(AmqpTransportError::LinkDetached(detach.error.clone()));
                        self.connection
                            .post_frame(AmqpFrame::new(self.remote_channel_id, detach.into()));
                        true
                    }
                    SenderLinkState::Closing(link) => {
                        println!("sender-link: closing");
                        true
                    }
                },
                Either::Right(link) => match link {
                    ReceiverLinkState::Opening(link) => {
                        println!("receiver-link: opening");
                        false
                    }
                    ReceiverLinkState::Established(link) => {
                        println!("receiver-link: established");
                        false
                    }
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

    fn settle_deliveries(&mut self, disposition: &Disposition) {
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

    fn apply_flow(&mut self, flow: &Flow) {
        self.outgoing_window =
            flow.next_incoming_id().unwrap_or(0) + flow.incoming_window() - self.next_outgoing_id;
        trace!(
            "session received credit. window: {}, pending: {}",
            self.outgoing_window,
            self.pending_transfers.len()
        );
        while let Some(t) = self.pending_transfers.pop_front() {
            self.send_transfer(t.link_handle, t.message, t.promise);
            if self.outgoing_window == 0 {
                break;
            }
        }
        if let Some(Either::Left(link)) = flow.handle().and_then(|h| self.links.get_mut(h as usize))
        {
            match link {
                SenderLinkState::Established(ref mut link) => {
                    link.get_mut().apply_flow(flow);
                }
                _ => (),
            }
        } else if flow.echo() {
            self.send_flow();
        }
    }

    fn send_flow(&mut self) {
        let flow = Flow {
            next_incoming_id: Some(self.next_incoming_id), // todo: derive from begin/flow
            incoming_window: self.incoming_window,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.outgoing_window,
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: false,
            echo: false,
            properties: None,
            body: None,
        };
        self.post_frame(flow.into());
    }

    fn post_frame(&mut self, frame: Frame) {
        self.connection
            .post_frame(AmqpFrame::new(self.remote_channel_id, frame));
    }

    pub fn send_transfer(
        &mut self,
        link_handle: Handle,
        message: Message,
        promise: DeliveryPromise,
    ) {
        if self.outgoing_window == 0 {
            // todo: queue up instead
            self.pending_transfers.push_back(PendingTransfer {
                link_handle,
                message,
                promise,
            });
            return;
        }
        let frame = self.prepare_transfer(link_handle, message, promise);
        self.post_frame(frame);
    }

    pub fn prepare_transfer(
        &mut self,
        link_handle: Handle,
        message: Message,
        promise: DeliveryPromise,
    ) -> Frame {
        self.outgoing_window -= 1;
        let delivery_id = self.next_outgoing_id;
        self.next_outgoing_id += 1;
        let delivery_tag = Bytes::from(&Uuid::new_v4().as_bytes()[..]);
        let transfer = Transfer {
            handle: link_handle,
            delivery_id: Some(delivery_id),
            delivery_tag: Some(delivery_tag.clone()),
            message_format: message.message_format,
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
            body: Some(message.serialize()),
        };
        self.unsettled_deliveries.insert(delivery_id, promise);
        Frame::Transfer(transfer)
    }
}
