use std::{collections::VecDeque, fmt, future::Future};

use ntex::channel::{oneshot, pool};
use ntex::util::{ByteString, Bytes, Either, HashMap, Ready};
use slab::Slab;

use ntex_amqp_codec::protocol::{
    Accepted, Attach, DeliveryNumber, DeliveryState, Detach, Disposition, Error, Flow, Frame,
    Handle, MessageFormat, ReceiverSettleMode, Role, SenderSettleMode, Transfer, TransferBody,
    TransferNumber,
};
use ntex_amqp_codec::AmqpFrame;

use crate::connection::Connection;
use crate::error::AmqpProtocolError;
use crate::rcvlink::{ReceiverLink, ReceiverLinkBuilder, ReceiverLinkInner};
use crate::sndlink::{DeliveryPromise, SenderLink, SenderLinkBuilder, SenderLinkInner};
use crate::{cell::Cell, types::Action};

const INITIAL_OUTGOING_ID: TransferNumber = 0;

#[derive(Clone)]
pub struct Session {
    pub(crate) inner: Cell<SessionInner>,
}

impl fmt::Debug for Session {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Session").finish()
    }
}

impl Session {
    pub(crate) fn new(inner: Cell<SessionInner>) -> Session {
        Session { inner }
    }

    pub fn connection(&self) -> &Connection {
        &self.inner.get_ref().sink
    }

    pub fn close(&self) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        Ready::Ok(())
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
        &self,
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
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
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
                    Err(AmqpProtocolError::Disconnected)
                }
            }
        }
    }

    pub fn wait_disposition(
        &mut self,
        id: DeliveryNumber,
    ) -> impl Future<Output = Result<Disposition, AmqpProtocolError>> {
        self.inner.get_mut().wait_disposition(id)
    }
}

#[derive(Debug)]
enum SenderLinkState {
    Established(SenderLink),
    Opening(Option<oneshot::Sender<Result<Cell<SenderLinkInner>, AmqpProtocolError>>>),
    Closing(Option<oneshot::Sender<Result<(), AmqpProtocolError>>>),
}

#[derive(Debug)]
enum ReceiverLinkState {
    Opening(Option<Cell<ReceiverLinkInner>>),
    OpeningLocal(
        Option<(
            Cell<ReceiverLinkInner>,
            oneshot::Sender<Result<ReceiverLink, AmqpProtocolError>>,
        )>,
    ),
    Established(ReceiverLink),
    Closing(Option<oneshot::Sender<Result<(), AmqpProtocolError>>>),
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
    sink: Connection,
    next_outgoing_id: TransferNumber,
    local: bool,

    remote_channel_id: u16,
    next_incoming_id: TransferNumber,
    remote_outgoing_window: u32,
    remote_incoming_window: u32,

    links: Slab<Either<SenderLinkState, ReceiverLinkState>>,
    links_by_name: HashMap<ByteString, usize>,
    remote_handles: HashMap<Handle, usize>,
    error: Option<AmqpProtocolError>,

    pending_transfers: VecDeque<PendingTransfer>,
    unsettled_deliveries: HashMap<DeliveryNumber, DeliveryPromise>,
    disposition_subscribers: HashMap<DeliveryNumber, pool::Sender<Disposition>>,

    pub(crate) pool: pool::Pool<Result<Disposition, AmqpProtocolError>>,
    pool_disp: pool::Pool<Disposition>,
}

struct PendingTransfer {
    link_handle: Handle,
    body: Option<TransferBody>,
    state: TransferState,
    settled: Option<bool>,
    message_format: Option<MessageFormat>,
}

#[derive(Debug)]
pub(crate) enum TransferState {
    First(DeliveryPromise, Bytes),
    Continue,
    Last,
    Only(DeliveryPromise, Bytes),
}

impl TransferState {
    fn more(&self) -> bool {
        !matches!(self, TransferState::Only(_, _) | TransferState::Last)
    }
}

impl SessionInner {
    pub(crate) fn new(
        id: usize,
        local: bool,
        sink: Connection,
        remote_channel_id: u16,
        next_incoming_id: DeliveryNumber,
        remote_incoming_window: u32,
        remote_outgoing_window: u32,
    ) -> SessionInner {
        SessionInner {
            id,
            local,
            sink,
            next_incoming_id,
            remote_channel_id,
            remote_incoming_window,
            remote_outgoing_window,
            next_outgoing_id: INITIAL_OUTGOING_ID,
            unsettled_deliveries: HashMap::default(),
            links: Slab::new(),
            links_by_name: HashMap::default(),
            remote_handles: HashMap::default(),
            pending_transfers: VecDeque::new(),
            disposition_subscribers: HashMap::default(),
            error: None,
            pool: pool::new(),
            pool_disp: pool::new(),
        }
    }

    /// Local channel id
    pub(crate) fn id(&self) -> u16 {
        self.id as u16
    }

    /// Set error. New operations will return error.
    pub(crate) fn set_error(&mut self, err: AmqpProtocolError) {
        log::trace!("Connection is failed, dropping state: {:?}", err);

        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            if let TransferState::First(tx, _) | TransferState::Only(tx, _) = tr.state {
                tx.ready(Err(err.clone()));
            }
        }

        // drop unsettled deliveries
        for (_, promise) in self.unsettled_deliveries.drain() {
            promise.ready(Err(err.clone()))
        }

        self.disposition_subscribers.clear();

        // drop links
        self.links_by_name.clear();
        for (_, st) in self.links.iter_mut() {
            match st {
                Either::Left(SenderLinkState::Opening(_)) => (),
                Either::Left(SenderLinkState::Established(ref mut link)) => {
                    link.inner.get_mut().detached(err.clone());
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

    fn wait_disposition(
        &mut self,
        id: DeliveryNumber,
    ) -> impl Future<Output = Result<Disposition, AmqpProtocolError>> {
        let (tx, rx) = self.pool_disp.channel();
        self.disposition_subscribers.insert(id, tx);
        async move { rx.await.map_err(|_| AmqpProtocolError::Disconnected) }
    }

    pub(crate) fn max_frame_size(&self) -> usize {
        self.sink.0.max_frame_size
    }

    /// Open sender link
    pub(crate) fn attach_local_sender_link(
        &mut self,
        mut frame: Attach,
    ) -> oneshot::Receiver<Result<Cell<SenderLinkInner>, AmqpProtocolError>> {
        trace!("Local sender link opening: {:?}", frame.name());
        let (tx, rx) = oneshot::channel();

        let entry = self.links.vacant_entry();
        let token = entry.key();
        entry.insert(Either::Left(SenderLinkState::Opening(Some(tx))));

        frame.handle = token as Handle;

        self.links_by_name.insert(frame.name.clone(), token);
        self.post_frame(Frame::Attach(frame));
        rx
    }

    /// Register remote sender link
    pub(crate) fn attach_remote_sender_link(
        &mut self,
        attach: &Attach,
        link: Cell<SenderLinkInner>,
    ) -> SenderLink {
        trace!("Remote sender link attached: {:?}", attach.name());
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

    pub(crate) fn detach_sender_link(
        &mut self,
        id: usize,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpProtocolError>>,
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

    /// Detach unconfirmed sender link
    pub(crate) fn detach_unconfirmed_sender_link(&mut self, attach: &Attach, error: Option<Error>) {
        let detach = Detach {
            handle: attach.handle(),
            closed: true,
            error,
        };
        self.post_frame(detach.into());
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

    /// Register receiver link
    pub(crate) fn attach_remote_receiver_link(
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

    pub(crate) fn attach_local_receiver_link(
        &mut self,
        cell: Cell<SessionInner>,
        mut frame: Attach,
    ) -> oneshot::Receiver<Result<ReceiverLink, AmqpProtocolError>> {
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
            if let ReceiverLinkState::Opening(l) = link {
                if let Some(l) = l.take() {
                    let attach = Attach {
                        name: attach.name.clone(),
                        handle: token as Handle,
                        role: Role::Receiver,
                        snd_settle_mode: attach.snd_settle_mode(),
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
                    *link = ReceiverLinkState::Established(ReceiverLink::new(l));
                    self.post_frame(attach.into());
                    return;
                }
            }
        }
        // TODO: close session
        error!("Unexpected receiver link state");
    }

    /// Close receiver link
    pub(crate) fn detach_receiver_link(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpProtocolError>>,
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

    pub(crate) fn handle_frame(&mut self, frame: Frame) -> Result<Action, AmqpProtocolError> {
        if self.error.is_none() {
            match frame {
                Frame::Flow(flow) => {
                    // apply link flow
                    if let Some(Either::Left(link)) = flow
                        .handle()
                        .and_then(|h| self.remote_handles.get(&h).copied())
                        .and_then(|h| self.links.get_mut(h))
                    {
                        if let SenderLinkState::Established(ref mut link) = link {
                            return Ok(Action::Flow(link.clone(), flow));
                        } else {
                            warn!("Received flow frame");
                        }
                    }
                    self.handle_flow(&flow, None);
                    Ok(Action::None)
                }
                Frame::Disposition(disp) => {
                    if let Some(sender) = self.disposition_subscribers.remove(&disp.first) {
                        let _ = sender.send(disp);
                    } else {
                        self.settle_deliveries(disp);
                    }
                    Ok(Action::None)
                }
                Frame::Transfer(transfer) => {
                    let idx = if let Some(idx) = self.remote_handles.get(&transfer.handle()) {
                        *idx
                    } else {
                        error!("Transfer's link {:?} is unknown", transfer.handle());
                        return Err(AmqpProtocolError::UnknownLink {
                            session: self.id,
                            link_handle: transfer.handle(),
                            frame: Box::new(Frame::Transfer(transfer)),
                        });
                    };

                    if let Some(link) = self.links.get_mut(idx) {
                        match link {
                            Either::Left(_) => {
                                error!("Got trasfer from sender link");
                                Err(AmqpProtocolError::Unexpected(
                                    "Got trasfer from sender link",
                                    Box::new(Frame::Transfer(transfer)),
                                ))
                            }
                            Either::Right(link) => match link {
                                ReceiverLinkState::Opening(_) => {
                                    error!(
                                        "Got transfer for opening link: {} -> {}",
                                        transfer.handle(),
                                        idx
                                    );
                                    Err(AmqpProtocolError::UnexpectedOpeningState(Box::new(
                                        Frame::Transfer(transfer),
                                    )))
                                }
                                ReceiverLinkState::OpeningLocal(_) => {
                                    error!(
                                        "Got transfer for opening link: {} -> {}",
                                        transfer.handle(),
                                        idx
                                    );
                                    Err(AmqpProtocolError::UnexpectedOpeningState(Box::new(
                                        Frame::Transfer(transfer),
                                    )))
                                }
                                ReceiverLinkState::Established(link) => {
                                    // self.outgoing_window -= 1;
                                    let _ = self.next_incoming_id.wrapping_add(1);
                                    link.inner.get_mut().handle_transfer(transfer, &link.inner)
                                }
                                ReceiverLinkState::Closing(_) => Ok(Action::None),
                            },
                        }
                    } else {
                        Err(AmqpProtocolError::UnknownLink {
                            session: self.id,
                            link_handle: transfer.handle(),
                            frame: Box::new(Frame::Transfer(transfer)),
                        })
                    }
                }
                Frame::Detach(detach) => Ok(self.handle_detach(detach)),
                frame => {
                    error!("Unexpected frame: {:?}", frame);
                    Ok(Action::None)
                }
            }
        } else {
            Ok(Action::None)
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
                            "Local sender link attached: {:?} {} -> {}",
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
                            let _ = tx.send(Ok(link));
                        }
                    }
                }
                Some(Either::Right(item)) => {
                    if item.is_opening() {
                        trace!(
                            "Local receiver link attached: {:?} {} -> {}",
                            name,
                            index,
                            attach.handle()
                        );
                        if let ReceiverLinkState::OpeningLocal(opt_item) = item {
                            if let Some((link, tx)) = opt_item.take() {
                                self.remote_handles.insert(attach.handle(), *index);

                                *item =
                                    ReceiverLinkState::Established(ReceiverLink::new(link.clone()));
                                let _ = tx.send(Ok(ReceiverLink::new(link)));
                            } else {
                                // TODO: close session
                                error!("Inconsistent session state, bug");
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
    pub(crate) fn handle_detach(&mut self, mut frame: Detach) -> Action {
        // get local link instance
        let idx = if let Some(idx) = self.remote_handles.get(&frame.handle()) {
            *idx
        } else if self.links.contains(frame.handle() as usize) {
            frame.handle() as usize
        } else {
            // should not happen, error
            log::info!("Detaching unknown link: {:?}", frame);
            return Action::None;
        };

        let handle = frame.handle();
        let mut action = Action::None;

        let remove = if let Some(link) = self.links.get_mut(idx) {
            match link {
                Either::Left(link) => match link {
                    SenderLinkState::Opening(ref mut tx) => {
                        if let Some(tx) = tx.take() {
                            let err = AmqpProtocolError::LinkDetached(frame.error.clone());
                            let _ = tx.send(Err(err));
                        }
                        true
                    }
                    SenderLinkState::Established(link) => {
                        // detach from remote endpoint
                        let detach = Detach {
                            handle: link.inner.get_ref().id(),
                            closed: true,
                            error: frame.error.clone(),
                        };
                        let err = AmqpProtocolError::LinkDetached(detach.error.clone());

                        // remove name
                        self.links_by_name.remove(link.inner.name());

                        // drop pending transfers
                        let mut idx = 0;
                        let handle = link.inner.get_ref().remote_handle();
                        while idx < self.pending_transfers.len() {
                            if self.pending_transfers[idx].link_handle == handle {
                                let tr = self.pending_transfers.remove(idx).unwrap();
                                if let TransferState::First(tx, _) | TransferState::Only(tx, _) =
                                    tr.state
                                {
                                    tx.ready(Err(err.clone()));
                                }
                            } else {
                                idx += 1;
                            }
                        }

                        // detach snd link
                        link.inner.get_mut().detached(err);
                        self.sink
                            .post_frame(AmqpFrame::new(self.remote_channel_id, detach.into()));
                        action = Action::DetachSender(link.clone(), frame);
                        true
                    }
                    SenderLinkState::Closing(_) => true,
                },
                Either::Right(link) => match link {
                    ReceiverLinkState::Opening(_) => false,
                    ReceiverLinkState::OpeningLocal(ref mut item) => {
                        if let Some((inner, tx)) = item.take() {
                            inner.get_mut().detached();
                            if let Some(err) = frame.error.clone() {
                                let _ = tx.send(Err(AmqpProtocolError::LinkDetached(Some(err))));
                            } else {
                                let _ = tx.send(Err(AmqpProtocolError::LinkDetached(None)));
                            }
                        } else {
                            error!("Inconsistent session state, bug");
                        }

                        true
                    }
                    ReceiverLinkState::Established(link) => {
                        link.remote_closed(frame.error.take());

                        // detach from remote endpoint
                        let detach = Detach {
                            handle: link.handle(),
                            closed: true,
                            error: None,
                        };

                        // detach rcv link
                        self.sink
                            .post_frame(AmqpFrame::new(self.remote_channel_id, detach.into()));

                        action = Action::DetachReceiver(link.clone(), frame);
                        true
                    }
                    ReceiverLinkState::Closing(tx) => {
                        // detach confirmation
                        if let Some(tx) = tx.take() {
                            if let Some(err) = frame.error {
                                let _ = tx.send(Err(AmqpProtocolError::LinkDetached(Some(err))));
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
            self.remote_handles.remove(&handle);
        }
        action
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
                val.ready(Ok(disposition));
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
                    val.ready(Ok(disposition.clone()));
                }
            }
        }
    }

    pub(crate) fn handle_flow(&mut self, flow: &Flow, link: Option<&SenderLink>) {
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
            self.send_transfer(t.link_handle, t.body, t.state, t.settled, t.message_format);
            if self.remote_outgoing_window == 0 {
                break;
            }
        }

        if flow.echo() {
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

        // apply link flow
        if let Some(link) = link {
            link.inner.get_mut().apply_flow(flow);
        }
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

    pub(crate) fn send_transfer(
        &mut self,
        link_handle: Handle,
        body: Option<TransferBody>,
        state: TransferState,
        settled: Option<bool>,
        message_format: Option<MessageFormat>,
    ) {
        if self.remote_incoming_window == 0 {
            log::trace!(
                "Remote window is 0, push to pending queue, hnd:{:?}",
                link_handle
            );
            self.pending_transfers.push_back(PendingTransfer {
                link_handle,
                body,
                state,
                settled,
                message_format,
            });
        } else {
            self.remote_incoming_window -= 1;

            log::trace!(
                "Sending transfer over {} window: {}",
                link_handle,
                self.remote_incoming_window
            );

            let settled2 = settled.unwrap_or(false);
            let tr_settled = if settled2 {
                Some(DeliveryState::Accepted(Accepted {}))
            } else {
                None
            };

            let mut transfer = Transfer {
                body,
                settled,
                message_format,
                more: false,
                handle: link_handle,
                state: tr_settled,
                delivery_id: None,
                delivery_tag: None,
                rcv_settle_mode: None,
                resume: false,
                aborted: false,
                batchable: false,
            };

            let more = state.more();
            match state {
                TransferState::First(promise, delivery_tag)
                | TransferState::Only(promise, delivery_tag) => {
                    let delivery_id = self.next_outgoing_id;
                    self.next_outgoing_id += 1;

                    transfer.more = more;
                    transfer.batchable = more;
                    transfer.delivery_id = Some(delivery_id);
                    transfer.delivery_tag = Some(delivery_tag);
                    self.unsettled_deliveries.insert(delivery_id, promise);
                }
                TransferState::Continue => {
                    transfer.more = true;
                    transfer.batchable = true;
                }
                TransferState::Last => {
                    transfer.more = false;
                }
            }

            self.post_frame(Frame::Transfer(transfer));
        }
    }

    pub(crate) fn post_frame(&mut self, frame: Frame) {
        self.sink
            .post_frame(AmqpFrame::new(self.remote_channel_id, frame));
    }
}
