use std::{cmp, collections::VecDeque, fmt, future::Future, mem};

use ntex_bytes::{BytePages, ByteString, Bytes};
use ntex_util::channel::{condition, oneshot, pool};
use ntex_util::{HashMap, future::Either};
use slab::Slab;

use ntex_amqp_codec::protocol::{
    self as codec, Accepted, Attach, Begin, DeliveryNumber, DeliveryState, Detach, Disposition,
    End, Error, Flow, Frame, Handle, MessageFormat, ReceiverSettleMode, Role, SenderSettleMode,
    Source, Transfer, TransferBody, TransferNumber,
};
use ntex_amqp_codec::{AmqpFrame, Encode};

use crate::delivery::DeliveryInner;
use crate::error::AmqpProtocolError;
use crate::rcvlink::{
    DEFAULT_MAX_MESSAGE_SIZE, EstablishedReceiverLink, ReceiverLink, ReceiverLinkBuilder,
    ReceiverLinkInner,
};
use crate::sndlink::{
    EstablishedSenderLink, SenderLink, SenderLinkBuilder, SenderLinkInner, remote_max_message_size,
};
use crate::{ConnectionRef, ControlFrame, cell::Cell, detach, types::Action};

const FRAME_HEADER_LEN: usize = 8;

pub(crate) const INITIAL_NEXT_OUTGOING_ID: TransferNumber = 1;

#[derive(Clone)]
pub struct Session {
    pub(crate) inner: Cell<SessionInner>,
}

#[derive(Debug)]
pub(crate) struct SessionInner {
    id: usize,
    sink: ConnectionRef,
    next_outgoing_id: TransferNumber,
    flags: Flags,
    begin: Begin,

    remote_channel_id: u16,
    next_incoming_id: TransferNumber,
    remote_outgoing_window: u32,
    remote_incoming_window: u32,

    links: Slab<Either<SenderLinkState, ReceiverLinkState>>,
    links_by_name: HashMap<ByteString, usize>,
    remote_handles: HashMap<Handle, usize>,
    error: Option<AmqpProtocolError>,
    closed: condition::Condition,

    pending_transfers: VecDeque<PendingTransfer>,
    pub(crate) unsettled_snd_deliveries: HashMap<DeliveryNumber, DeliveryInner>,
    pub(crate) unsettled_rcv_deliveries: HashMap<DeliveryNumber, DeliveryInner>,

    pub(crate) pool_notify: pool::Pool<()>,
    pub(crate) pool_credit: pool::Pool<Result<(), AmqpProtocolError>>,
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

    #[inline]
    /// Get begin frame reference
    pub fn frame(&self) -> &Begin {
        &self.inner.get_ref().begin
    }

    #[inline]
    /// Get io tag for current connection
    pub fn tag(&self) -> &'static str {
        self.inner.get_ref().sink.tag()
    }

    #[inline]
    pub fn connection(&self) -> &ConnectionRef {
        &self.inner.get_ref().sink
    }

    #[inline]
    pub fn local_channel_id(&self) -> u16 {
        self.inner.get_ref().id()
    }

    #[inline]
    pub fn remote_channel_id(&self) -> u16 {
        self.inner.get_ref().remote_channel_id
    }

    #[inline]
    /// Get remote window size
    pub fn remote_window_size(&self) -> u32 {
        self.inner.get_ref().remote_incoming_window
    }

    pub async fn end(&self) -> Result<(), AmqpProtocolError> {
        {
            let inner = self.inner.get_mut();

            if inner.flags.contains(Flags::ENDED) {
                return Ok(());
            }

            if !inner.flags.contains(Flags::ENDING) {
                inner.sink.close_session(inner.id);
                inner.post_frame(Frame::End(End { error: None }));
                inner.flags.insert(Flags::ENDING);
                inner
                    .sink
                    .get_control_queue()
                    .enqueue_frame(ControlFrame::new_kind(
                        crate::ControlFrameKind::LocalSessionEnded(inner.get_all_links()),
                    ));
            }

            self.inner.closed.wait().await;
            if let Some(err @ AmqpProtocolError::SessionEnded(Some(_))) = self.inner.error.clone() {
                Err(err)
            } else {
                Ok(())
            }
        }
    }

    pub fn get_sender_link(&self, name: &str) -> Option<&SenderLink> {
        let inner = self.inner.get_ref();

        if let Some(id) = inner.links_by_name.get(name)
            && let Some(Either::Left(SenderLinkState::Established(link))) = inner.links.get(*id)
        {
            return Some(link);
        }
        None
    }

    #[inline]
    pub fn get_sender_link_by_local_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        self.inner.get_ref().get_sender_link_by_local_handle(hnd)
    }

    #[inline]
    pub fn get_sender_link_by_remote_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        self.inner.get_ref().get_sender_link_by_remote_handle(hnd)
    }

    #[inline]
    pub fn get_receiver_link_by_local_handle(&self, hnd: Handle) -> Option<&ReceiverLink> {
        self.inner.get_ref().get_receiver_link_by_local_handle(hnd)
    }

    #[inline]
    pub fn get_receiver_link_by_remote_handle(&self, hnd: Handle) -> Option<&ReceiverLink> {
        self.inner.get_ref().get_receiver_link_by_remote_handle(hnd)
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
        &self,
        name: U,
        address: T,
    ) -> ReceiverLinkBuilder {
        let name = name.into();
        let address = address.into();
        ReceiverLinkBuilder::new(name, address, self.inner.clone())
    }

    /// Detach receiver link
    pub fn detach_receiver_link(
        &self,
        handle: Handle,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        let (tx, rx) = oneshot::channel();

        self.inner
            .get_mut()
            .detach_receiver_link(handle, false, error, tx);

        async move {
            match rx.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    log::trace!("Cannot complete detach receiver link {e:?}");
                    Err(e)
                }
                Err(_) => {
                    log::trace!("Cannot complete detach receiver link, connection is gone");
                    Err(AmqpProtocolError::Disconnected)
                }
            }
        }
    }

    /// Detach sender link
    pub fn detach_sender_link(
        &self,
        handle: Handle,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> {
        let (tx, rx) = oneshot::channel();

        self.inner
            .get_mut()
            .detach_sender_link(handle, false, error, tx);

        async move {
            match rx.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    log::trace!("Cannot complete detach sender link {e:?}");
                    Err(e)
                }
                Err(_) => {
                    log::trace!("Cannot complete detach sender link, connection is gone");
                    Err(AmqpProtocolError::Disconnected)
                }
            }
        }
    }
}

#[derive(Debug)]
enum SenderLinkState {
    Established(EstablishedSenderLink),
    /// Remote link waits for control service confirmation, keeps last
    /// link flow and remote detach received before confirmation
    OpeningRemote {
        link: Cell<SenderLinkInner>,
        flow: Option<Flow>,
        detach: Option<Detach>,
    },
    Opening(Option<oneshot::Sender<Result<Cell<SenderLinkInner>, AmqpProtocolError>>>),
    Closing(Option<oneshot::Sender<Result<(), AmqpProtocolError>>>),
}

#[derive(Debug)]
enum ReceiverLinkState {
    Opening(Box<Option<(Cell<ReceiverLinkInner>, Option<Source>)>>),
    OpeningLocal(
        Option<(
            Cell<ReceiverLinkInner>,
            oneshot::Sender<Result<ReceiverLink, AmqpProtocolError>>,
        )>,
    ),
    Established(EstablishedReceiverLink),
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

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug)]
    struct Flags: u8 {
        const LOCAL =  0b0000_0001;
        const ENDED =  0b0000_0010;
        const ENDING = 0b0000_0100;
    }
}

#[derive(Debug)]
struct PendingTransfer {
    tx: pool::Sender<Result<(), AmqpProtocolError>>,
    link_handle: Handle,
}

impl SessionInner {
    pub(crate) fn new(
        id: usize,
        local: bool,
        sink: ConnectionRef,
        remote_channel_id: u16,
        begin: Begin,
    ) -> SessionInner {
        SessionInner {
            next_incoming_id: begin.next_outgoing_id(),
            remote_incoming_window: begin.incoming_window(),
            remote_outgoing_window: begin.outgoing_window(),
            flags: if local { Flags::LOCAL } else { Flags::empty() },
            next_outgoing_id: INITIAL_NEXT_OUTGOING_ID,
            unsettled_snd_deliveries: HashMap::default(),
            unsettled_rcv_deliveries: HashMap::default(),
            links: Slab::new(),
            links_by_name: HashMap::default(),
            remote_handles: HashMap::default(),
            pending_transfers: VecDeque::new(),
            error: None,
            pool_notify: pool::new(),
            pool_credit: pool::new(),
            closed: condition::Condition::new(),
            id,
            sink,
            begin,
            remote_channel_id,
        }
    }

    /// Local channel id
    pub(crate) fn id(&self) -> u16 {
        self.id as u16
    }

    pub(crate) fn tag(&self) -> &'static str {
        self.sink.tag()
    }

    pub(crate) fn unsettled_deliveries(
        &mut self,
        sender: bool,
    ) -> &mut HashMap<DeliveryNumber, DeliveryInner> {
        if sender {
            &mut self.unsettled_snd_deliveries
        } else {
            &mut self.unsettled_rcv_deliveries
        }
    }

    /// Set error. New operations will return error.
    pub(crate) fn set_error(&mut self, err: AmqpProtocolError) {
        log::trace!(
            "{}: Connection is failed, dropping state: {err:?}",
            self.tag()
        );

        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            let _ = tr.tx.send(Err(err.clone()));
        }

        // drop unsettled deliveries
        for (_, mut promise) in self.unsettled_snd_deliveries.drain() {
            promise.set_error(err.clone());
        }
        for (_, mut promise) in self.unsettled_rcv_deliveries.drain() {
            promise.set_error(err.clone());
        }

        // drop links
        self.links_by_name.clear();
        for (_, st) in &mut self.links {
            match st {
                Either::Left(SenderLinkState::Established(link)) => {
                    link.inner.get_mut().remote_detached(err.clone());
                }
                Either::Left(SenderLinkState::OpeningRemote { link, .. }) => {
                    link.get_mut().remote_detached(err.clone());
                }
                Either::Left(SenderLinkState::Closing(link)) => {
                    if let Some(tx) = link.take() {
                        let _ = tx.send(Err(err.clone()));
                    }
                }
                Either::Right(ReceiverLinkState::Established(link)) => {
                    link.remote_detached(None);
                }
                _ => (),
            }
        }
        self.links.clear();

        self.error = Some(err);
        self.flags.insert(Flags::ENDED);
        self.closed.notify_and_lock(());
    }

    /// End session.
    pub(crate) fn end(&mut self, err: AmqpProtocolError) -> Action {
        log::trace!("{}: Session is ended: {err:?}", self.tag());

        let links = self.get_all_links();
        self.set_error(err);

        Action::SessionEnded(links)
    }

    fn get_all_links(&self) -> Vec<Either<SenderLink, ReceiverLink>> {
        self.links
            .iter()
            .filter_map(|(_, st)| match st {
                Either::Left(SenderLinkState::Established(link)) => {
                    Some(Either::Left((*link).clone()))
                }
                Either::Left(SenderLinkState::OpeningRemote { link, .. }) => {
                    Some(Either::Left(SenderLink::new(link.clone())))
                }
                Either::Right(ReceiverLinkState::Established(link)) => {
                    Some(Either::Right((*link).clone()))
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) fn max_frame_size(&self) -> u32 {
        self.sink.0.max_frame_size
    }

    /// Check if next link handle is within remote handle-max
    pub(crate) fn check_handle(&self) -> bool {
        self.links.vacant_key() <= self.begin.handle_max() as usize
    }

    /// Initialize creation of remote sender link
    pub(crate) fn new_remote_sender(
        &mut self,
        cell: Cell<SessionInner>,
        attach: &Attach,
    ) -> (SenderLink, Attach) {
        let entry = self.links.vacant_entry();
        let id = entry.key();
        let link = Cell::new(SenderLinkInner::with(id, attach, cell));
        entry.insert(Either::Left(SenderLinkState::OpeningRemote {
            link: link.clone(),
            flow: None,
            detach: None,
        }));
        self.remote_handles.insert(attach.handle(), id);

        let attach = Attach(Box::new(codec::AttachInner {
            name: attach.0.name.clone(),
            handle: id as Handle,
            role: Role::Sender,
            snd_settle_mode: attach.snd_settle_mode(),
            rcv_settle_mode: attach.rcv_settle_mode(),
            source: attach.0.source.clone(),
            target: attach.0.target.clone(),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(attach.initial_delivery_count().unwrap_or(0)),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }));

        (SenderLink::new(link), attach)
    }

    /// Check if remote sender link waits for confirmation and session is not ending
    fn is_opening_remote(&self, token: usize) -> bool {
        !self.flags.intersects(Flags::ENDING | Flags::ENDED)
            && matches!(
                self.links.get(token),
                Some(Either::Left(SenderLinkState::OpeningRemote { .. }))
            )
    }

    /// Open sender link
    pub(crate) fn attach_local_sender_link(
        &mut self,
        mut frame: Attach,
    ) -> oneshot::Receiver<Result<Cell<SenderLinkInner>, AmqpProtocolError>> {
        let (tx, rx) = oneshot::channel();
        if !self.check_handle() {
            let _ = tx.send(Err(AmqpProtocolError::TooManyLinks));
            return rx;
        }

        let entry = self.links.vacant_entry();
        let token = entry.key();
        entry.insert(Either::Left(SenderLinkState::Opening(Some(tx))));
        log::trace!(
            "{}: Local sender link opening: {:?} hnd:{token:?}",
            self.tag(),
            frame.name(),
        );

        frame.0.handle = token as Handle;

        self.links_by_name.insert(frame.0.name.clone(), token);
        self.post_frame(Frame::Attach(frame));
        rx
    }

    /// Register remote sender link
    pub(crate) fn attach_remote_sender_link(
        &mut self,
        attach: &Attach,
        mut response: Attach,
        link: Cell<SenderLinkInner>,
    ) -> SenderLink {
        log::trace!(
            "{}: Remote sender link attached: {:?}",
            self.tag(),
            attach.name()
        );
        let token = link.id;

        // session could be ended while link was waiting for confirmation,
        // link is closed by session error
        let state = if self.flags.intersects(Flags::ENDING | Flags::ENDED) {
            None
        } else {
            self.links.get_mut(token)
        };
        let Some(Either::Left(SenderLinkState::OpeningRemote { flow, detach, .. })) = state else {
            log::debug!(
                "{}: Remote sender link is not opening: {:?}",
                self.tag(),
                attach.name()
            );
            return SenderLink::new(link);
        };
        let (flow, detach) = (flow.take(), detach.take());

        *response.handle_mut() = token as Handle;
        *response.max_message_size_mut() = link.max_message_size().map(u64::from);
        self.post_frame(response.into());

        // remote detached link before confirmation
        if let Some(detach) = detach {
            log::trace!(
                "{}: Remote sender link detached before confirmation: {:?}",
                self.tag(),
                attach.name()
            );
            self.links.remove(token);
            self.remote_handles.remove(&attach.handle());
            self.post_frame(
                Detach(Box::new(codec::DetachInner {
                    handle: token as Handle,
                    closed: true,
                    error: None,
                }))
                .into(),
            );

            let link = SenderLink::new(link);
            link.inner
                .get_mut()
                .remote_detached(AmqpProtocolError::LinkDetached(detach.0.error.clone()));
            self.sink
                .get_control_queue()
                .enqueue_frame(ControlFrame::new(
                    link.session().inner.clone(),
                    crate::ControlFrameKind::RemoteDetachSender(detach, link.clone()),
                ));
            return link;
        }

        self.links[token] = Either::Left(SenderLinkState::Established(EstablishedSenderLink::new(
            link.clone(),
        )));

        if let Some(source) = attach.source()
            && let Some(ref addr) = source.address
        {
            self.links_by_name.insert(addr.clone(), token);
        }

        // link flow received before confirmation
        if let Some(flow) = flow {
            link.get_mut().apply_flow(&flow);
        }
        SenderLink::new(link)
    }

    /// Detach sender link
    pub(crate) fn detach_sender_link(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpProtocolError>>,
    ) {
        if let Some(Either::Left(link)) = self.links.get_mut(id as usize) {
            match link {
                SenderLinkState::Opening(_) => {
                    let detach = Detach(Box::new(codec::DetachInner {
                        handle: id,
                        closed,
                        error,
                    }));
                    *link = SenderLinkState::Closing(Some(tx));
                    self.post_frame(detach.into());
                }
                SenderLinkState::Established(sender_link) => {
                    let sender_link = sender_link.clone();
                    let detach = Detach(Box::new(codec::DetachInner {
                        handle: id,
                        closed,
                        error,
                    }));

                    *link = SenderLinkState::Closing(Some(tx));
                    self.post_frame(detach.clone().into());
                    self.sink
                        .get_control_queue()
                        .enqueue_frame(ControlFrame::new_kind(
                            crate::ControlFrameKind::LocalDetachSender(detach, sender_link),
                        ));
                }
                SenderLinkState::OpeningRemote { .. } => {
                    let _ = tx.send(Ok(()));
                    log::error!(
                        "{}: Unexpected sender link state: opening remote - {id}",
                        self.tag()
                    );
                }
                SenderLinkState::Closing(_) => {
                    let _ = tx.send(Ok(()));
                    log::error!(
                        "{}: Unexpected sender link state: closing - {id}",
                        self.tag()
                    );
                }
            }
        } else {
            let _ = tx.send(Ok(()));
            log::debug!(
                "{}: Sender link does not exist while detaching: {id}",
                self.tag()
            );
        }
    }

    /// Detach unconfirmed sender link
    pub(crate) fn detach_unconfirmed_sender_link(
        &mut self,
        attach: &Attach,
        link: &Cell<SenderLinkInner>,
        error: Option<Error>,
    ) {
        let token = link.id;
        if !link.get_ref().closed {
            link.get_mut()
                .remote_detached(AmqpProtocolError::LinkDetached(error.clone()));
        }

        // session could be ended while link was waiting for confirmation
        if !self.is_opening_remote(token) {
            log::debug!(
                "{}: Remote sender link is not opening: {:?}",
                self.tag(),
                attach.name()
            );
            return;
        }
        self.remote_handles.remove(&attach.handle());

        let attach = Attach(Box::new(codec::AttachInner {
            name: attach.0.name.clone(),
            handle: token as Handle,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Unsettled,
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
        }));
        self.post_frame(attach.into());

        let detach = Detach(Box::new(codec::DetachInner {
            handle: token as Handle,
            closed: true,
            error,
        }));
        self.post_frame(detach.into());

        if self.links.contains(token) {
            self.links.remove(token);
        }
    }

    pub(crate) fn is_remote_handle_used(&self, hnd: Handle) -> bool {
        self.remote_handles.contains_key(&hnd)
    }

    pub(crate) fn get_sender_link_by_local_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        if let Some(Either::Left(SenderLinkState::Established(link))) = self.links.get(hnd as usize)
        {
            Some(link)
        } else {
            None
        }
    }

    pub(crate) fn get_sender_link_by_remote_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        if let Some(id) = self.remote_handles.get(&hnd)
            && let Some(Either::Left(SenderLinkState::Established(link))) = self.links.get(*id)
        {
            return Some(link);
        }
        None
    }

    /// Register receiver link
    pub(crate) fn attach_remote_receiver_link(
        &mut self,
        cell: Cell<SessionInner>,
        attach: &Attach,
    ) -> (Attach, ReceiverLink) {
        let handle = attach.handle();
        let entry = self.links.vacant_entry();
        let token = entry.key();

        let inner = Cell::new(ReceiverLinkInner::new(
            cell,
            token as u32,
            handle,
            attach,
            DEFAULT_MAX_MESSAGE_SIZE,
        ));
        entry.insert(Either::Right(ReceiverLinkState::Opening(Box::new(Some((
            inner.clone(),
            attach.source().cloned(),
        ))))));
        self.remote_handles.insert(handle, token);

        let response = Attach(Box::new(codec::AttachInner {
            name: attach.0.name.clone(),
            handle: token as Handle,
            role: Role::Receiver,
            snd_settle_mode: attach.snd_settle_mode(),
            rcv_settle_mode: ReceiverSettleMode::First,
            source: attach.0.source.clone(),
            target: attach.0.target.clone(),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            offered_capabilities: None,
            desired_capabilities: None,
            max_message_size: None,
            properties: None,
        }));

        (response, ReceiverLink::new(inner))
    }

    pub(crate) fn attach_local_receiver_link(
        &mut self,
        cell: Cell<SessionInner>,
        mut frame: Attach,
    ) -> oneshot::Receiver<Result<ReceiverLink, AmqpProtocolError>> {
        let (tx, rx) = oneshot::channel();
        if !self.check_handle() {
            let _ = tx.send(Err(AmqpProtocolError::TooManyLinks));
            return rx;
        }

        let entry = self.links.vacant_entry();
        let token = entry.key();

        let inner = Cell::new(ReceiverLinkInner::new(
            cell,
            token as u32,
            token as u32,
            &frame,
            frame.max_message_size().unwrap_or(0),
        ));
        entry.insert(Either::Right(ReceiverLinkState::OpeningLocal(Some((
            inner, tx,
        )))));

        frame.0.handle = token as Handle;

        self.links_by_name.insert(frame.0.name.clone(), token);
        self.post_frame(Frame::Attach(frame));
        rx
    }

    pub(crate) fn confirm_receiver_link(
        &mut self,
        token: Handle,
        mut response: Attach,
        max_message_size: Option<u64>,
    ) {
        if let Some(Either::Right(link)) = self.links.get_mut(token as usize)
            && let ReceiverLinkState::Opening(l) = link
            && let Some((l, _)) = l.take()
        {
            *response.max_message_size_mut() = max_message_size;
            *link = ReceiverLinkState::Established(EstablishedReceiverLink::new(l));
            self.post_frame(response.into());
            return;
        }
        // TODO: close session
        log::error!("{}: Unexpected receiver link state", self.tag());
    }

    /// Detach receiver link
    pub(crate) fn detach_receiver_link(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpProtocolError>>,
    ) {
        if let Some(Either::Right(link)) = self.links.get_mut(id as usize) {
            match link {
                ReceiverLinkState::Opening(inner) => {
                    if let Some((inner, source)) = inner.take() {
                        let attach = Attach(Box::new(codec::AttachInner {
                            source,
                            max_message_size: None,
                            name: inner.name().clone(),
                            handle: id,
                            role: Role::Receiver,
                            snd_settle_mode: SenderSettleMode::Mixed,
                            rcv_settle_mode: ReceiverSettleMode::First,
                            target: None,
                            unsettled: None,
                            incomplete_unsettled: false,
                            initial_delivery_count: Some(0),
                            offered_capabilities: None,
                            desired_capabilities: None,
                            properties: None,
                        }));
                        self.post_frame(attach.into());
                    }
                    let detach = Detach(Box::new(codec::DetachInner {
                        closed,
                        error,
                        handle: id,
                    }));
                    self.post_frame(detach.into());
                    let _ = tx.send(Ok(()));
                    if self.links.contains(id as usize) {
                        let _ = self.links.remove(id as usize);
                    }
                }
                ReceiverLinkState::Established(receiver_link) => {
                    let receiver_link = receiver_link.clone();
                    let detach = Detach(Box::new(codec::DetachInner {
                        handle: id,
                        closed,
                        error,
                    }));
                    *link = ReceiverLinkState::Closing(Some(tx));
                    self.post_frame(detach.clone().into());
                    self.sink
                        .get_control_queue()
                        .enqueue_frame(ControlFrame::new_kind(
                            crate::ControlFrameKind::LocalDetachReceiver(detach, receiver_link),
                        ));
                }
                ReceiverLinkState::Closing(_) => {
                    let _ = tx.send(Ok(()));
                    if self.links.contains(id as usize) {
                        let _ = self.links.remove(id as usize);
                    }
                    log::error!(
                        "{}: Unexpected receiver link state: closing - {id}",
                        self.tag()
                    );
                }
                ReceiverLinkState::OpeningLocal(_inner) => unimplemented!(),
            }
        } else {
            let _ = tx.send(Ok(()));
            log::error!(
                "{}: Receiver link does not exist while detaching: {id}",
                self.tag()
            );
        }
    }

    pub(crate) fn get_receiver_link_by_local_handle(&self, hnd: Handle) -> Option<&ReceiverLink> {
        if let Some(Either::Right(ReceiverLinkState::Established(link))) =
            self.links.get(hnd as usize)
        {
            Some(link)
        } else {
            None
        }
    }

    pub(crate) fn get_receiver_link_by_remote_handle(&self, hnd: Handle) -> Option<&ReceiverLink> {
        if let Some(id) = self.remote_handles.get(&hnd)
            && let Some(Either::Right(ReceiverLinkState::Established(link))) = self.links.get(*id)
        {
            return Some(link);
        }
        None
    }

    pub(crate) fn handle_frame(&mut self, frame: Frame) -> Result<Action, AmqpProtocolError> {
        if self.error.is_none() {
            match frame {
                Frame::Flow(flow) => {
                    let mut established = None;
                    match flow
                        .handle()
                        .and_then(|h| self.remote_handles.get(&h).copied())
                        .and_then(|h| self.links.get_mut(h))
                    {
                        // link credit is applied in frames order, control service is notified
                        Some(Either::Left(SenderLinkState::Established(link))) => {
                            established = Some((*link).clone());
                        }
                        // link is not confirmed yet, link credit is applied after confirmation
                        Some(Either::Left(SenderLinkState::OpeningRemote {
                            flow: pending,
                            detach: None,
                            ..
                        })) if flow.link_credit().is_some() => {
                            *pending = Some(flow.clone());
                        }
                        _ => (),
                    }
                    // session flow state is applied in frames order
                    self.handle_flow(&flow);
                    if let Some(link) = established {
                        link.inner.get_mut().apply_flow(&flow);
                        Ok(Action::Flow(link, flow))
                    } else {
                        Ok(Action::None)
                    }
                }
                Frame::Disposition(disp) => {
                    self.settle_deliveries(&disp);
                    Ok(Action::None)
                }
                Frame::Transfer(transfer) => {
                    // incoming window is not limited (u32::MAX), memory is bounded
                    // by link credit, max message size and handle-max
                    self.next_incoming_id = self.next_incoming_id.wrapping_add(1);

                    let idx = if let Some(idx) = self.remote_handles.get(&transfer.handle()) {
                        *idx
                    } else {
                        log::debug!(
                            "{}: Transfer's link {:?} is unknown",
                            self.tag(),
                            transfer.handle()
                        );
                        return Err(AmqpProtocolError::UnknownLink(Frame::Transfer(transfer)));
                    };

                    if let Some(link) = self.links.get_mut(idx) {
                        match link {
                            Either::Left(_) => {
                                log::debug!(
                                    "{}: Got unexpected trasfer from sender link",
                                    self.tag()
                                );
                                Err(AmqpProtocolError::Unexpected(Frame::Transfer(transfer)))
                            }
                            Either::Right(link) => match link {
                                ReceiverLinkState::Opening(_)
                                | ReceiverLinkState::OpeningLocal(_) => {
                                    log::debug!(
                                        "{}: Got transfer for opening link: {} -> {idx}",
                                        self.tag(),
                                        transfer.handle()
                                    );
                                    Err(AmqpProtocolError::UnexpectedOpeningState(Frame::Transfer(
                                        transfer,
                                    )))
                                }
                                ReceiverLinkState::Established(link) => {
                                    Ok(link.inner.get_mut().handle_transfer(transfer, &link.inner))
                                }
                                ReceiverLinkState::Closing(_) => Ok(Action::None),
                            },
                        }
                    } else {
                        Err(AmqpProtocolError::UnknownLink(Frame::Transfer(transfer)))
                    }
                }
                Frame::Detach(detach) => Ok(self.handle_detach(detach)),
                frame => {
                    log::debug!("{}: Unexpected frame: {frame:?}", self.tag());
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
                        log::trace!(
                            "{}: Local sender link attached: {name:?} {index} -> {}, {:?}",
                            self.sink.tag(),
                            attach.handle(),
                            self.remote_handles.contains_key(&attach.handle())
                        );

                        self.remote_handles.insert(attach.handle(), *index);
                        let delivery_count = attach.initial_delivery_count().unwrap_or(0);
                        let link = Cell::new(SenderLinkInner::new(
                            *index,
                            name.clone(),
                            attach.handle(),
                            delivery_count,
                            cell,
                            remote_max_message_size(attach),
                        ));
                        let local_sender = mem::replace(
                            item,
                            SenderLinkState::Established(EstablishedSenderLink::new(link.clone())),
                        );

                        if let SenderLinkState::Opening(Some(tx)) = local_sender {
                            let _ = tx.send(Ok(link));
                        }
                    }
                }
                Some(Either::Right(item)) => {
                    if item.is_opening() {
                        log::trace!(
                            "{}: Local receiver link attached: {name:?} {index} -> {}",
                            self.sink.tag(),
                            attach.handle()
                        );
                        if let ReceiverLinkState::OpeningLocal(opt_item) = item {
                            if let Some((link, tx)) = opt_item.take() {
                                self.remote_handles.insert(attach.handle(), *index);

                                *item = ReceiverLinkState::Established(
                                    EstablishedReceiverLink::new(link.clone()),
                                );
                                let _ = tx.send(Ok(ReceiverLink::new(link)));
                            } else {
                                // TODO: close session
                                log::error!("{}: Inconsistent session state, bug", self.tag());
                            }
                        }
                    }
                }
                _ => {
                    // TODO: error in proto, have to close connection
                    log::trace!("Protocol error");
                }
            }
            true
        } else {
            // cannot handle remote attach
            false
        }
    }

    #[allow(clippy::too_many_lines)]
    /// Handle `Detach` frame.
    pub(crate) fn handle_detach(&mut self, mut frame: Detach) -> Action {
        // get local link instance
        let idx = if let Some(idx) = self.remote_handles.get(&frame.handle()) {
            *idx
        } else if self.links.contains(frame.handle() as usize) {
            frame.handle() as usize
        } else {
            // should not happen, error
            log::info!("{}: Detaching unknown link: {frame:?}", self.tag());
            return Action::None;
        };

        let handle = frame.handle();
        let mut action = Action::None;

        let remove = if let Some(link) = self.links.get_mut(idx) {
            match link {
                Either::Left(link) => match link {
                    SenderLinkState::Opening(tx) => {
                        if let Some(tx) = tx.take() {
                            let err = AmqpProtocolError::LinkDetached(frame.0.error.clone());
                            let _ = tx.send(Err(err));
                        }
                        true
                    }
                    SenderLinkState::Established(link) => {
                        // detach from remote endpoint
                        let detach = Detach(Box::new(codec::DetachInner {
                            handle: link.inner.get_ref().id(),
                            closed: true,
                            error: frame.error().cloned(),
                        }));
                        let err = AmqpProtocolError::LinkDetached(detach.0.error.clone());

                        // remove name
                        self.links_by_name.remove(link.inner.name());

                        // drop pending transfers
                        let mut idx = 0;
                        let handle = link.inner.get_ref().remote_handle();
                        while idx < self.pending_transfers.len() {
                            if self.pending_transfers[idx].link_handle == handle {
                                let tr = self.pending_transfers.remove(idx).unwrap();
                                let _ = tr.tx.send(Err(err.clone()));
                            } else {
                                idx += 1;
                            }
                        }

                        // drop unsettled transfers
                        let handle = link.inner.get_ref().id() as Handle;
                        for delivery in self.unsettled_snd_deliveries.values_mut() {
                            if delivery.handle() == handle {
                                delivery.set_error(err.clone());
                            }
                        }

                        // detach snd link
                        link.inner.get_mut().remote_detached(err);
                        self.sink
                            .post_frame(AmqpFrame::new(self.id as u16, detach.into()));
                        action = Action::DetachSender(link.clone(), frame);
                        true
                    }
                    SenderLinkState::OpeningRemote { detach, .. } => {
                        // detach is confirmed and link is removed after control service confirmation
                        if detach.is_some() {
                            log::warn!(
                                "{}: Duplicate detach frame for unconfirmed sender link: {frame:?}",
                                self.sink.tag()
                            );
                        } else {
                            log::trace!(
                                "{}: Detach frame received for unconfirmed sender link: {frame:?}",
                                self.sink.tag()
                            );
                            *detach = Some(frame);
                        }
                        false
                    }
                    SenderLinkState::Closing(tx) => {
                        if let Some(tx) = tx.take() {
                            if let Some(err) = frame.0.error {
                                let _ = tx.send(Err(AmqpProtocolError::LinkDetached(Some(err))));
                            } else {
                                let _ = tx.send(Ok(()));
                            }
                        }
                        true
                    }
                },
                Either::Right(link) => match link {
                    ReceiverLinkState::Opening(_) => false,
                    ReceiverLinkState::OpeningLocal(item) => {
                        if let Some((inner, tx)) = item.take() {
                            inner.get_mut().detached();
                            if let Some(err) = frame.0.error.clone() {
                                let _ = tx.send(Err(AmqpProtocolError::LinkDetached(Some(err))));
                            } else {
                                let _ = tx.send(Err(AmqpProtocolError::LinkDetached(None)));
                            }
                        } else {
                            log::error!("{}: Inconsistent session state, bug", self.tag());
                        }

                        true
                    }
                    ReceiverLinkState::Established(link) => {
                        let error = frame.0.error.take();

                        // drop unsettled transfers
                        let err = AmqpProtocolError::LinkDetached(error.clone());
                        let handle = link.inner.get_ref().id();
                        for delivery in self.unsettled_rcv_deliveries.values_mut() {
                            if delivery.handle() == handle {
                                delivery.set_error(err.clone());
                            }
                        }

                        // detach from remote endpoint
                        let detach = Detach(Box::new(codec::DetachInner {
                            handle: link.handle(),
                            closed: true,
                            error: None,
                        }));
                        self.sink
                            .post_frame(AmqpFrame::new(self.id as u16, detach.into()));

                        // detach rcv link
                        link.remote_detached(error);
                        action = Action::DetachReceiver(link.clone(), frame);
                        true
                    }
                    ReceiverLinkState::Closing(tx) => {
                        // detach confirmation
                        if let Some(tx) = tx.take() {
                            if let Some(err) = frame.0.error {
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
            if self.links.contains(idx) {
                self.links.remove(idx);
            }
            self.remote_handles.remove(&handle);
        }
        action
    }

    fn settle_deliveries(&mut self, disp: &Disposition) {
        let from = disp.first();
        let to = disp.last();

        if cfg!(feature = "frame-trace") {
            log::trace!("{}: Settle delivery: {disp:#?}", self.tag());
        } else {
            log::trace!(
                "{}: Settle delivery from {from} - {to:?}, state {:?} settled: {:?}",
                self.tag(),
                disp.state(),
                disp.settled()
            );
        }

        let deliveries = if disp.role() == Role::Receiver {
            &mut self.unsettled_snd_deliveries
        } else {
            &mut self.unsettled_rcv_deliveries
        };

        // state is stored with deliveries, error and annotations are detached from read buffer
        let state = match disp.state() {
            Some(st @ (DeliveryState::Rejected(_) | DeliveryState::Modified(_))) => {
                Some(detach(st))
            }
            st => st.cloned(),
        };
        let settled = disp.settled();
        for_each_in_range(deliveries, from, to.unwrap_or(from), |delivery| {
            delivery.handle_disposition(settled, state.as_ref());
        });
    }

    pub(crate) fn handle_flow(&mut self, flow: &Flow) {
        // # AMQP1.0 2.5.6
        self.next_incoming_id = flow.next_outgoing_id();
        self.remote_outgoing_window = flow.outgoing_window();

        self.remote_incoming_window = flow
            .next_incoming_id()
            .unwrap_or(0)
            .wrapping_add(flow.incoming_window())
            .wrapping_sub(self.next_outgoing_id);

        log::trace!(
            "{}: Session received credit {:?}. window: {}, pending: {}",
            self.tag(),
            flow.link_credit(),
            self.remote_incoming_window,
            self.pending_transfers.len(),
        );

        while let Some(tr) = self.pending_transfers.pop_front() {
            let _ = tr.tx.send(Ok(()));
        }

        if flow.echo() {
            let flow = Flow(Box::new(codec::FlowInner {
                next_incoming_id: Some(self.next_incoming_id),
                incoming_window: u32::MAX,
                next_outgoing_id: self.next_outgoing_id,
                outgoing_window: self.remote_incoming_window,
                handle: None,
                delivery_count: None,
                link_credit: None,
                available: None,
                drain: false,
                echo: false,
                properties: None,
            }));
            self.post_frame(flow.into());
        }
    }

    pub(crate) fn rcv_link_flow(&mut self, handle: u32, delivery_count: u32, credit: u32) {
        let flow = Flow(Box::new(codec::FlowInner {
            next_incoming_id: Some(self.next_incoming_id),
            incoming_window: u32::MAX,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.remote_incoming_window,
            handle: Some(handle),
            delivery_count: Some(delivery_count),
            link_credit: Some(credit),
            available: None,
            drain: false,
            echo: false,
            properties: None,
        }));
        self.post_frame(flow.into());
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn send_transfer(
        &mut self,
        link_handle: Handle,
        tag: Bytes,
        body: TransferBody,
        settled: bool,
        format: Option<MessageFormat>,
    ) -> Result<DeliveryNumber, AmqpProtocolError> {
        loop {
            if self.remote_incoming_window == 0 {
                log::trace!(
                    "{}: Remote window is 0, push to pending queue, hnd:{link_handle:?}",
                    self.sink.tag()
                );
                let (tx, rx) = self.pool_credit.channel();
                self.pending_transfers
                    .push_back(PendingTransfer { tx, link_handle });

                rx.await
                    .map_err(|_| AmqpProtocolError::ConnectionDropped)
                    .and_then(|v| v)?;
                continue;
            }
            break;
        }

        self.remote_incoming_window -= 1;

        let delivery_id = self.next_outgoing_id;
        self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);

        let tr_settled = if settled {
            Some(DeliveryState::Accepted(Accepted {}))
        } else {
            None
        };
        let message_format = if format.is_none() {
            body.message_format()
        } else {
            format
        };

        let mut transfer = Transfer(Box::default());
        transfer.0.handle = link_handle;
        transfer.0.state = tr_settled;
        transfer.0.delivery_id = Some(delivery_id);
        transfer.0.delivery_tag = Some(tag.clone());
        transfer.0.message_format = message_format;

        if settled {
            transfer.0.settled = Some(true);
        } else {
            self.unsettled_snd_deliveries
                .insert(delivery_id, DeliveryInner::new(link_handle));
        }

        let max_chunk = max_transfer_chunk(self.max_frame_size(), transfer.encoded_size());

        // body is larger than allowed frame size, send body as a set of transfers
        if body.len() > max_chunk {
            let mut body = match body {
                TransferBody::Data(data) => data,
                TransferBody::Message(msg) => {
                    let mut buf = BytePages::default();
                    msg.encode(&mut buf);
                    buf.freeze()
                }
                TransferBody::Pages(mut data) => data.freeze(),
            };

            let chunk = body.split_to(cmp::min(max_chunk, body.len()));
            transfer.0.body = Some(TransferBody::Data(chunk));
            transfer.0.more = true;
            transfer.0.batchable = true;

            log::trace!(
                "{}: Sending transfer over handle {link_handle}. window: {} delivery_id: {:?} delivery_tag: {:?}, more: {:?}, batchable: {:?}, settled: {:?}",
                self.sink.tag(),
                self.remote_incoming_window,
                transfer.delivery_id(),
                transfer.delivery_tag(),
                transfer.more(),
                transfer.batchable(),
                transfer.settled(),
            );
            self.post_frame(Frame::Transfer(transfer));

            loop {
                // last chunk
                if body.is_empty() {
                    log::trace!("{}: Last tranfer for {tag:?} is sent", self.tag());
                    break;
                }

                let chunk = body.split_to(cmp::min(max_chunk, body.len()));

                log::trace!("{}: Sending chunk tranfer for {tag:?}", self.tag());
                let mut transfer = Transfer(Box::default());
                transfer.0.handle = link_handle;
                transfer.0.body = Some(TransferBody::Data(chunk));
                transfer.0.more = !body.is_empty();
                transfer.0.batchable = true;
                transfer.0.message_format = message_format;
                self.post_frame(Frame::Transfer(transfer));
            }
        } else {
            transfer.0.body = Some(body);
            self.post_frame(Frame::Transfer(transfer));
        }

        Ok(delivery_id)
    }

    pub(crate) fn post_frame(&mut self, frame: Frame) {
        self.sink.post_frame(AmqpFrame::new(self.id(), frame));
    }
}

/// Max transfer body chunk, frame header and transfer performative must fit
/// into remote max frame size.
///
/// Frame size is a 32-bit field on the wire, `0` is treated as `u32::MAX`.
fn max_transfer_chunk(max_frame_size: u32, transfer_size: usize) -> usize {
    let max_frame_size = match max_frame_size {
        0 => u32::MAX,
        size => size,
    };
    (max_frame_size as usize)
        .saturating_sub(FRAME_HEADER_LEN + transfer_size)
        .max(1)
}

/// Call `f` for each entry with key in `from..=to` range (RFC-1982 serial numbers).
///
/// Cost is bounded by the smaller of the range length and the map size.
fn for_each_in_range<T>(
    map: &mut HashMap<DeliveryNumber, T>,
    from: DeliveryNumber,
    to: DeliveryNumber,
    mut f: impl FnMut(&mut T),
) {
    let len = to.wrapping_sub(from);
    if (len as usize) < map.len() {
        for idx in 0..=len {
            if let Some(item) = map.get_mut(&from.wrapping_add(idx)) {
                f(item);
            }
        }
    } else {
        for (no, item) in map.iter_mut() {
            if no.wrapping_sub(from) <= len {
                f(item);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(map: &mut HashMap<DeliveryNumber, DeliveryNumber>, from: u32, to: u32) -> Vec<u32> {
        let mut res = Vec::new();
        for_each_in_range(map, from, to, |v| res.push(*v));
        res.sort_unstable();
        res
    }

    #[test]
    fn range() {
        let mut map = HashMap::default();
        for no in [0, 1, 2, 5, u32::MAX - 1, u32::MAX] {
            map.insert(no, no);
        }

        // range smaller than map
        assert_eq!(collect(&mut map, 1, 2), vec![1, 2]);
        assert_eq!(collect(&mut map, 5, 5), vec![5]);
        assert_eq!(collect(&mut map, 3, 4), Vec::<u32>::new());
        assert_eq!(collect(&mut map, u32::MAX, 1), vec![0, 1, u32::MAX]);
        assert_eq!(
            collect(&mut map, u32::MAX - 1, 2),
            vec![0, 1, 2, u32::MAX - 1, u32::MAX]
        );

        // range larger than map
        assert_eq!(collect(&mut map, 1, 100), vec![1, 2, 5]);
        assert_eq!(collect(&mut map, u32::MAX, 10), vec![0, 1, 2, 5, u32::MAX]);
        assert_eq!(collect(&mut map, 0, u32::MAX).len(), 6);
        assert_eq!(collect(&mut map, 6, 100), Vec::<u32>::new());
    }

    #[test]
    fn transfer_chunk() {
        assert_eq!(max_transfer_chunk(512, 20), 512 - 28);
        assert_eq!(max_transfer_chunk(512, 1000), 1);
        assert_eq!(max_transfer_chunk(u32::MAX, 20), u32::MAX as usize - 28);
        // unlimited frame size is still limited by 32-bit frame size field
        assert_eq!(max_transfer_chunk(0, 20), u32::MAX as usize - 28);
    }
}
