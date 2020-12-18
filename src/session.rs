use std::collections::VecDeque;
use std::future::Future;

use bytes::{BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use either::Either;
use futures::future::ok;
use fxhash::FxHashMap;
use ntex::channel::oneshot;
use slab::Slab;

use ntex_amqp_codec::protocol::{
    Accepted, Attach, DeliveryNumber, DeliveryState, Detach, Disposition, Error, Flow, Frame,
    Handle, MessageFormat, ReceiverSettleMode, Role, SenderSettleMode, Transfer, TransferBody,
    TransferNumber,
};
use ntex_amqp_codec::AmqpFrame;

use crate::cell::Cell;
use crate::connection::ConnectionController;
use crate::errors::AmqpTransportError;
use crate::rcvlink::{ReceiverLink, ReceiverLinkBuilder, ReceiverLinkInner};
use crate::sndlink::{SenderLink, SenderLinkBuilder, SenderLinkInner};
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

impl std::fmt::Debug for Session {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("Session").finish()
    }
}

impl Session {
    pub(crate) fn new(inner: Cell<SessionInner>) -> Session {
        Session { inner }
    }

    #[inline]
    /// Get remote connection configuration
    pub fn remote_config(&self) -> &Configuration {
        self.inner.connection.remote_config()
    }

    pub fn close(&self) -> impl Future<Output = Result<(), AmqpTransportError>> {
        ok(())
    }

    pub fn get_sender_link(&self, name: &str) -> Option<&SenderLink> {
        let inner = self.inner.get_ref();

        if let Some(id) = inner.links_by_name.get(name) {
            if let Some(Either::Left(SenderLinkState::Established(ref link))) = inner.links.get(*id)
            {
                return Some(link);
            }
        }
        None
    }

    pub fn get_sender_link_by_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        self.inner.get_ref().get_sender_link_by_handle(hnd)
    }

    pub fn get_receiver_link_by_handle(&self, hnd: Handle) -> Option<&ReceiverLink> {
        self.inner.get_ref().get_receiver_link_by_handle(hnd)
    }

    /// Open sender link
    pub fn build_sender_link<T: Into<ByteString>, U: Into<ByteString>>(
        &mut self,
        name: U,
        address: T,
    ) -> SenderLinkBuilder {
        let name = name.into();
        let address = address.into();
        SenderLinkBuilder::new(name, address, self.inner.clone())
    }

    /// Open receiver link
    pub fn build_receiver_link<T: Into<ByteString>, U: Into<ByteString>>(
        &mut self,
        name: U,
        address: T,
    ) -> ReceiverLinkBuilder {
        let name = name.into();
        let address = address.into();
        ReceiverLinkBuilder::new(name, address, self.inner.clone())
    }

    /// Detach receiver link
    pub fn detach_receiver_link(
        &mut self,
        handle: Handle,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpTransportError>> {
        let (tx, rx) = oneshot::channel();

        self.inner
            .get_mut()
            .detach_receiver_link(handle, false, error, tx);

        async move {
            match rx.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => {
                    log::trace!("Cannot complete detach receiver link {:?}", e);
                    Err(e)
                }
                Err(_) => {
                    log::trace!("Cannot complete detach receiver link, connection is gone");
                    Err(AmqpTransportError::Disconnected)
                }
            }
        }
    }

    pub fn wait_disposition(
        &mut self,
        id: DeliveryNumber,
    ) -> impl Future<Output = Result<Disposition, AmqpTransportError>> {
        self.inner.get_mut().wait_disposition(id)
    }
}

#[derive(Debug)]
enum SenderLinkState {
    Established(SenderLink),
    Opening(Option<oneshot::Sender<Result<SenderLink, AmqpTransportError>>>),
    Closing(Option<oneshot::Sender<Result<(), AmqpTransportError>>>),
}

#[derive(Debug)]
enum ReceiverLinkState {
    Opening(Option<Cell<ReceiverLinkInner>>),
    OpeningLocal(
        Option<(
            Cell<ReceiverLinkInner>,
            oneshot::Sender<Result<ReceiverLink, AmqpTransportError>>,
        )>,
    ),
    Established(ReceiverLink),
    Closing(Option<oneshot::Sender<Result<(), AmqpTransportError>>>),
}

impl SenderLinkState {
    fn is_opening(&self) -> bool {
        matches!(self, SenderLinkState::Opening(_))
    }
}

impl ReceiverLinkState {
    fn is_opening(&self) -> bool {
        matches!(self, ReceiverLinkState::OpeningLocal(_))
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

    unsettled_deliveries: FxHashMap<DeliveryNumber, DeliveryPromise>,

    links: Slab<Either<SenderLinkState, ReceiverLinkState>>,
    links_by_name: FxHashMap<ByteString, usize>,
    remote_handles: FxHashMap<Handle, usize>,
    pending_transfers: VecDeque<PendingTransfer>,
    disposition_subscribers: FxHashMap<DeliveryNumber, oneshot::Sender<Disposition>>,
    error: Option<AmqpTransportError>,
}

struct PendingTransfer {
    link_handle: Handle,
    idx: u32,
    body: Option<TransferBody>,
    promise: Option<DeliveryPromise>,
    tag: Option<Bytes>,
    settled: Option<bool>,
    more: TransferState,
    message_format: Option<MessageFormat>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum TransferState {
    First,
    Continue,
    Last,
    Only,
}

impl SessionInner {
    pub(crate) fn new(
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
            unsettled_deliveries: FxHashMap::default(),
            links: Slab::new(),
            links_by_name: FxHashMap::default(),
            remote_handles: FxHashMap::default(),
            pending_transfers: VecDeque::new(),
            disposition_subscribers: FxHashMap::default(),
            error: None,
        }
    }

    /// Local channel id
    pub(crate) fn id(&self) -> u16 {
        self.id as u16
    }

    /// Set error. New operations will return error.
    pub(crate) fn set_error(&mut self, err: AmqpTransportError) {
        log::trace!("Connection is failed, dropping state: {:?}", err);

        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            if let Some(tx) = tr.promise {
                let _ = tx.send(Err(err.clone()));
            }
        }

        // drop links
        self.links_by_name.clear();
        for (_, st) in self.links.iter_mut() {
            match st {
                Either::Left(SenderLinkState::Opening(_)) => (),
                Either::Left(SenderLinkState::Established(ref mut link)) => {
                    link.inner.get_mut().detached(err.clone())
                }
                Either::Left(SenderLinkState::Closing(ref mut link)) => {
                    if let Some(tx) = link.take() {
                        let _ = tx.send(Err(err.clone()));
                    }
                }
                Either::Right(ReceiverLinkState::Established(ref mut link)) => {
                    link.remote_closed(None)
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

    fn wait_disposition(
        &mut self,
        id: DeliveryNumber,
    ) -> impl Future<Output = Result<Disposition, AmqpTransportError>> {
        let (tx, rx) = oneshot::channel();
        self.disposition_subscribers.insert(id, tx);
        async move { rx.await.map_err(|_| AmqpTransportError::Disconnected) }
    }

    pub(crate) fn max_frame_size(&self) -> usize {
        self.connection.remote_config().max_frame_size as usize
    }

    /// Detach unconfirmed sender link
    pub(crate) fn detach_unconfirmed_sender_link(&mut self, attach: &Attach, error: Option<Error>) {
        let detach = Detach {
            handle: attach.handle(),
            closed: true,
            error,
        };
        self.post_frame(detach.into());
    }

    /// Register remote sender link
    pub(crate) fn confirm_sender_link(
        &mut self,
        attach: &Attach,
        cell: Cell<SessionInner>,
    ) -> SenderLink {
        trace!("Remote sender link opened: {:?}", attach.name());
        let link = Cell::new(SenderLinkInner::with(attach, cell));
        self.confirm_sender_link_inner(attach, link)
    }

    /// Register remote sender link
    pub(crate) fn confirm_sender_link_inner(
        &mut self,
        attach: &Attach,
        link: Cell<SenderLinkInner>,
    ) -> SenderLink {
        trace!("Remote sender link opened: {:?}", attach.name());
        let entry = self.links.vacant_entry();
        let token = entry.key();

        if let Some(ref source) = attach.source {
            if let Some(ref addr) = source.address {
                self.links_by_name.insert(addr.clone(), token);
            }
        }

        link.get_mut().id = token;
        self.remote_handles.insert(attach.handle(), token);
        entry.insert(Either::Left(SenderLinkState::Established(SenderLink::new(
            link.clone(),
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
            initial_delivery_count: Some(attach.initial_delivery_count.unwrap_or(0)),
            max_message_size: Some(65536),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        self.post_frame(attach.into());

        SenderLink::new(link)
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

        let inner = Cell::new(ReceiverLinkInner::new(cell, token as u32, attach));
        entry.insert(Either::Right(ReceiverLinkState::Opening(Some(
            inner.clone(),
        ))));
        self.remote_handles.insert(handle, token);
        ReceiverLink::new(inner)
    }

    pub(crate) fn open_local_receiver_link(
        &mut self,
        cell: Cell<SessionInner>,
        mut frame: Attach,
    ) -> oneshot::Receiver<Result<ReceiverLink, AmqpTransportError>> {
        let (tx, rx) = oneshot::channel();

        let entry = self.links.vacant_entry();
        let token = entry.key();

        let inner = Cell::new(ReceiverLinkInner::new(cell, token as u32, frame.clone()));
        entry.insert(Either::Right(ReceiverLinkState::OpeningLocal(Some((
            inner, tx,
        )))));

        frame.handle = token as Handle;

        self.links_by_name.insert(frame.name.clone(), token);
        self.post_frame(Frame::Attach(frame));
        rx
    }

    pub(crate) fn confirm_receiver_link(&mut self, token: Handle, attach: &Attach) {
        if let Some(Either::Right(link)) = self.links.get_mut(token as usize) {
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
                    *link = ReceiverLinkState::Established(ReceiverLink::new(l.take().unwrap()));
                    self.post_frame(attach.into());
                }
                _ => error!("Unexpected receiver link state"),
            }
        }
    }

    /// Close receiver link
    pub(crate) fn detach_receiver_link(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpTransportError>>,
    ) {
        if let Some(Either::Right(link)) = self.links.get_mut(id as usize) {
            match link {
                ReceiverLinkState::Opening(_inner) => {
                    let detach = Detach {
                        handle: id,
                        closed,
                        error,
                    };
                    self.post_frame(detach.into());
                    let _ = tx.send(Ok(()));
                    let _ = self.links.remove(id as usize);
                }
                ReceiverLinkState::Established(_) => {
                    let detach = Detach {
                        handle: id,
                        closed,
                        error,
                    };
                    *link = ReceiverLinkState::Closing(Some(tx));
                    self.post_frame(detach.into());
                }
                ReceiverLinkState::Closing(_) => {
                    let _ = tx.send(Ok(()));
                    let _ = self.links.remove(id as usize);
                    error!("Unexpected receiver link state: closing - {}", id);
                }
                ReceiverLinkState::OpeningLocal(_inner) => unimplemented!(),
            }
        } else {
            let _ = tx.send(Ok(()));
            error!("Receiver link does not exist while detaching: {}", id);
        }
    }

    pub(crate) fn detach_sender_link(
        &mut self,
        id: usize,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpTransportError>>,
    ) {
        if let Some(Either::Left(link)) = self.links.get_mut(id) {
            match link {
                SenderLinkState::Opening(_) => {
                    let detach = Detach {
                        handle: id as u32,
                        closed,
                        error,
                    };
                    *link = SenderLinkState::Closing(Some(tx));
                    self.post_frame(detach.into());
                }
                SenderLinkState::Established(_) => {
                    let detach = Detach {
                        handle: id as u32,
                        closed,
                        error,
                    };
                    *link = SenderLinkState::Closing(Some(tx));
                    self.post_frame(detach.into());
                }
                SenderLinkState::Closing(_) => {
                    let _ = tx.send(Ok(()));
                    error!("Unexpected receiver link state: closing - {}", id);
                }
            }
        } else {
            let _ = tx.send(Ok(()));
            error!("Receiver link does not exist while detaching: {}", id);
        }
    }

    pub(crate) fn get_sender_link_by_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        if let Some(id) = self.remote_handles.get(&hnd) {
            if let Some(Either::Left(SenderLinkState::Established(ref link))) = self.links.get(*id)
            {
                return Some(link);
            }
        }
        None
    }

    pub(crate) fn get_receiver_link_by_handle(&self, hnd: Handle) -> Option<&ReceiverLink> {
        if let Some(id) = self.remote_handles.get(&hnd) {
            if let Some(Either::Right(ReceiverLinkState::Established(ref link))) =
                self.links.get(*id)
            {
                return Some(link);
            }
        }
        None
    }

    pub(crate) fn handle_frame(&mut self, frame: Frame) {
        if self.error.is_none() {
            match frame {
                Frame::Flow(flow) => self.apply_flow(&flow),
                Frame::Disposition(disp) => {
                    if let Some(sender) = self.disposition_subscribers.remove(&disp.first) {
                        let _ = sender.send(disp);
                    } else {
                        self.settle_deliveries(disp);
                    }
                }
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
                                ReceiverLinkState::OpeningLocal(_) => {
                                    error!(
                                        "Got transfer for opening link: {} -> {}",
                                        transfer.handle(),
                                        idx
                                    );
                                }
                                ReceiverLinkState::Established(link) => {
                                    // self.outgoing_window -= 1;
                                    let _ = self.next_incoming_id.wrapping_add(1);
                                    link.inner.get_mut().handle_transfer(transfer);
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
                Frame::Detach(mut detach) => {
                    self.handle_detach(&mut detach);
                }
                frame => error!("Unexpected frame: {:?}", frame),
            }
        }
    }

    /// Handle `Attach` frame. return false if attach frame is remote and can not be handled
    pub(crate) fn handle_attach(&mut self, attach: &Attach, cell: Cell<SessionInner>) -> bool {
        let name = attach.name();

        if let Some(index) = self.links_by_name.get(name) {
            match self.links.get_mut(*index) {
                Some(Either::Left(item)) => {
                    if item.is_opening() {
                        trace!(
                            "Sender link opened: {:?} {} -> {}",
                            name,
                            index,
                            attach.handle()
                        );

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

                        if let SenderLinkState::Opening(Some(tx)) = local_sender {
                            let _ = tx.send(Ok(SenderLink::new(link)));
                        }
                    }
                }
                Some(Either::Right(item)) => {
                    if item.is_opening() {
                        trace!(
                            "Receiver link opened: {:?} {} -> {}",
                            name,
                            index,
                            attach.handle()
                        );
                        if let ReceiverLinkState::OpeningLocal(opt_item) = item {
                            let (link, tx) = opt_item.take().unwrap();
                            self.remote_handles.insert(attach.handle(), *index);

                            *item = ReceiverLinkState::Established(ReceiverLink::new(link.clone()));
                            let _ = tx.send(Ok(ReceiverLink::new(link)));
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
    pub(crate) fn handle_detach(&mut self, detach: &mut Detach) {
        // get local link instance
        let idx = if let Some(idx) = self.remote_handles.get(&detach.handle()) {
            *idx
        } else if self.links.contains(detach.handle() as usize) {
            detach.handle() as usize
        } else {
            // should not happen, error
            log::info!("Detaching unknown link: {:?}", detach);
            return;
        };

        let remove = if let Some(link) = self.links.get_mut(idx) {
            match link {
                Either::Left(link) => match link {
                    SenderLinkState::Opening(ref mut tx) => {
                        if let Some(tx) = tx.take() {
                            let err = AmqpTransportError::LinkDetached(detach.error.clone());
                            let _ = tx.send(Err(err));
                        }
                        true
                    }
                    SenderLinkState::Established(link) => {
                        // detach from remote endpoint
                        let detach = Detach {
                            handle: link.inner.get_ref().id(),
                            closed: true,
                            error: detach.error.clone(),
                        };
                        let err = AmqpTransportError::LinkDetached(detach.error.clone());

                        // remove name
                        self.links_by_name.remove(link.inner.name());

                        // drop pending transfers
                        let mut idx = 0;
                        let handle = link.inner.get_ref().remote_handle();
                        while idx < self.pending_transfers.len() {
                            if self.pending_transfers[idx].link_handle == handle {
                                let tr = self.pending_transfers.remove(idx).unwrap();
                                if let Some(tx) = tr.promise {
                                    let _ = tx.send(Err(err.clone()));
                                }
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
                    ReceiverLinkState::OpeningLocal(ref mut item) => {
                        let (inner, tx) = item.take().unwrap();
                        inner.get_mut().detached();
                        if let Some(err) = detach.error.clone() {
                            let _ = tx.send(Err(AmqpTransportError::LinkDetached(Some(err))));
                        } else {
                            let _ = tx.send(Err(AmqpTransportError::LinkDetached(None)));
                        }

                        true
                    }
                    ReceiverLinkState::Established(link) => {
                        link.remote_closed(detach.error.take());

                        // detach from remote endpoint
                        let detach = Detach {
                            handle: link.handle(),
                            closed: true,
                            error: None,
                        };

                        // detach rcv link
                        self.connection
                            .post_frame(AmqpFrame::new(self.remote_channel_id, detach.into()));
                        true
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

    fn settle_deliveries(&mut self, disposition: Disposition) {
        let from = disposition.first;
        let to = disposition.last.unwrap_or(from);

        if cfg!(feature = "frame-trace") {
            trace!("Settle delivery: {:#?}", disposition);
        } else {
            trace!(
                "Settle delivery from {}, state {:?} settled: {:?}",
                from,
                disposition.state,
                disposition.settled
            );
        }

        if from == to {
            if let Some(val) = self.unsettled_deliveries.remove(&from) {
                if !disposition.settled {
                    let mut disp = disposition.clone();
                    disp.role = Role::Sender;
                    disp.settled = true;
                    disp.state = Some(DeliveryState::Accepted(Accepted {}));
                    self.post_frame(Frame::Disposition(disp));
                }
                let _ = val.send(Ok(disposition));
            }
        } else {
            if !disposition.settled {
                let mut disp = disposition.clone();
                disp.role = Role::Sender;
                disp.settled = true;
                disp.state = Some(DeliveryState::Accepted(Accepted {}));
                self.post_frame(Frame::Disposition(disp));
            }

            for k in from..=to {
                if let Some(val) = self.unsettled_deliveries.remove(&k) {
                    let _ = val.send(Ok(disposition.clone()));
                }
            }
        }
    }

    pub(crate) fn apply_flow(&mut self, flow: &Flow) {
        // # AMQP1.0 2.5.6
        self.next_incoming_id = flow.next_outgoing_id();
        self.remote_outgoing_window = flow.outgoing_window();

        self.remote_incoming_window = flow
            .next_incoming_id()
            .unwrap_or(INITIAL_OUTGOING_ID)
            .saturating_add(flow.incoming_window())
            .saturating_sub(self.next_outgoing_id);

        trace!(
            "Session received credit {:?}. window: {}, pending: {}",
            flow.link_credit(),
            self.remote_outgoing_window,
            self.pending_transfers.len()
        );

        while let Some(t) = self.pending_transfers.pop_front() {
            self.send_transfer(
                t.link_handle,
                t.idx,
                t.body,
                t.promise,
                t.tag,
                t.settled,
                t.more,
                t.message_format,
            );
            if self.remote_outgoing_window == 0 {
                break;
            }
        }

        // apply link flow
        if let Some(Either::Left(link)) = flow
            .handle()
            .and_then(|h| self.remote_handles.get(&h).copied())
            .and_then(|h| self.links.get_mut(h))
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

    pub(crate) fn post_frame(&mut self, frame: Frame) {
        self.connection
            .post_frame(AmqpFrame::new(self.remote_channel_id, frame));
    }

    pub(crate) fn open_sender_link(
        &mut self,
        mut frame: Attach,
    ) -> oneshot::Receiver<Result<SenderLink, AmqpTransportError>> {
        let (tx, rx) = oneshot::channel();

        let entry = self.links.vacant_entry();
        let token = entry.key();
        entry.insert(Either::Left(SenderLinkState::Opening(Some(tx))));

        frame.handle = token as Handle;

        self.links_by_name.insert(frame.name.clone(), token);
        self.post_frame(Frame::Attach(frame));
        rx
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_transfer(
        &mut self,
        link_handle: Handle,
        idx: u32,
        body: Option<TransferBody>,
        promise: Option<DeliveryPromise>,
        tag: Option<Bytes>,
        settled: Option<bool>,
        more: TransferState,
        message_format: Option<MessageFormat>,
    ) {
        if self.remote_incoming_window == 0 {
            log::trace!(
                "Remote window is 0, push to pending queue, hnd:{:?}",
                link_handle
            );
            self.pending_transfers.push_back(PendingTransfer {
                link_handle,
                idx,
                body,
                promise,
                tag,
                settled,
                more,
                message_format,
            });
        } else {
            let frame = self.prepare_transfer(
                link_handle,
                body,
                promise,
                tag,
                settled,
                more,
                message_format,
            );
            log::trace!(
                "Sending transfer over {} window: {}",
                link_handle,
                self.remote_incoming_window
            );
            self.post_frame(frame);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_transfer(
        &mut self,
        link_handle: Handle,
        body: Option<TransferBody>,
        promise: Option<DeliveryPromise>,
        delivery_tag: Option<Bytes>,
        settled: Option<bool>,
        more: TransferState,
        message_format: Option<MessageFormat>,
    ) -> Frame {
        self.remote_incoming_window -= 1;

        let settled2 = settled.clone().unwrap_or(false);
        let state = if settled2 {
            Some(DeliveryState::Accepted(Accepted {}))
        } else {
            None
        };

        let mut transfer = Transfer {
            body,
            settled,
            state, //: Some(DeliveryState::Accepted(Accepted {})),
            message_format,
            more: false,
            handle: link_handle,
            delivery_id: None,
            delivery_tag: None,
            rcv_settle_mode: None,
            resume: false,
            aborted: false,
            batchable: false,
        };

        match more {
            TransferState::First | TransferState::Only => {
                let delivery_id = self.next_outgoing_id;
                self.next_outgoing_id += 1;

                transfer.delivery_id = Some(delivery_id);
                transfer.delivery_tag = if let Some(tag) = delivery_tag {
                    Some(tag)
                } else {
                    let mut buf = BytesMut::new();
                    buf.put_u32(delivery_id);
                    Some(buf.freeze())
                };

                transfer.more = more == TransferState::First;
                transfer.batchable = more == TransferState::First;

                self.unsettled_deliveries
                    .insert(delivery_id, promise.unwrap());
            }
            TransferState::Continue => {
                transfer.more = true;
                transfer.batchable = true;
            }
            TransferState::Last => {
                transfer.more = false;
            }
        }

        Frame::Transfer(transfer)
    }
}
