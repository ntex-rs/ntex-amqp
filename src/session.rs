use std::task::{Context, Poll, Waker, ready};
use std::{cmp, collections::VecDeque, fmt, future::Future, mem, pin::Pin, ptr};

use ntex_bytes::{BytePages, ByteString, Bytes};
use ntex_util::channel::{condition, oneshot, pool};
use ntex_util::{HashMap, future::Either, time::Seconds};
use slab::Slab;

use ntex_amqp_codec::protocol::{
    self as codec, Accepted, Attach, Begin, DeliveryNumber, DeliveryState, Detach, Disposition,
    End, Error, Flow, Frame, Handle, MessageFormat, ReceiverSettleMode, Role, SenderSettleMode,
    SequenceNo, Source, Transfer, TransferBody, TransferNumber,
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
    // transfer frame id, AMQP 1.0 2.5.6
    next_outgoing_id: TransferNumber,
    next_delivery_id: DeliveryNumber,
    flags: Flags,
    begin: Begin,

    remote_channel_id: u16,
    next_incoming_id: TransferNumber,
    remote_outgoing_window: u32,
    remote_incoming_window: u32,
    // window claimed by woken transfers that have not sent their frame yet
    pub(crate) window_woken: u32,
    // notified if remote incoming window becomes available
    pub(crate) on_window: condition::Condition,

    links: Slab<Either<SenderLinkState, ReceiverLinkState>>,
    // link names by direction, and names of links by index
    pub(crate) sender_names: HashMap<ByteString, usize>,
    receiver_names: HashMap<ByteString, usize>,
    pub(crate) link_names: HashMap<usize, ByteString>,
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
        let inner = self.inner.get_ref();
        fmt.debug_struct("Session")
            .field("local_channel_id", &inner.id())
            .field("remote_channel_id", &inner.remote_channel_id)
            .finish()
    }
}

impl Session {
    pub(crate) fn new(inner: Cell<SessionInner>) -> Session {
        Session { inner }
    }

    /// Get begin frame reference
    #[inline]
    pub fn frame(&self) -> &Begin {
        &self.inner.get_ref().begin
    }

    /// Get io tag for current connection
    #[inline]
    pub fn tag(&self) -> &'static str {
        self.inner.get_ref().sink.tag()
    }

    /// Get connection reference
    #[inline]
    pub fn connection(&self) -> &ConnectionRef {
        &self.inner.get_ref().sink
    }

    /// Get local channel id
    #[inline]
    pub fn local_channel_id(&self) -> u16 {
        self.inner.get_ref().id()
    }

    /// Get remote channel id
    #[inline]
    pub fn remote_channel_id(&self) -> u16 {
        self.inner.get_ref().remote_channel_id
    }

    /// Get remote incoming window size
    #[inline]
    pub fn remote_window_size(&self) -> u32 {
        self.inner.get_ref().remote_incoming_window
    }

    /// End session
    ///
    /// Sends `End` frame and waits for peer's `End`. Returns error if
    /// peer ended session with an error.
    pub async fn end(&self) -> Result<(), AmqpProtocolError> {
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
        match self.inner.error.clone() {
            Some(err @ AmqpProtocolError::SessionEnded(Some(_))) => Err(err),
            _ => Ok(()),
        }
    }

    /// Find established sender link by name
    pub fn get_sender_link(&self, name: &str) -> Option<&SenderLink> {
        let inner = self.inner.get_ref();

        if let Some(id) = inner.sender_names.get(name)
            && let Some(Either::Left(SenderLinkState::Established(link))) = inner.links.get(*id)
        {
            return Some(link);
        }
        None
    }

    /// Find established sender link by address
    ///
    /// Multiple links could be attached to the same address, first found link is returned.
    pub fn get_sender_link_by_address(&self, address: &str) -> Option<&SenderLink> {
        self.inner
            .get_ref()
            .links
            .iter()
            .find_map(|(_, st)| match st {
                Either::Left(SenderLinkState::Established(link))
                    if link.address().is_some_and(|addr| addr == address) =>
                {
                    Some(&**link)
                }
                _ => None,
            })
    }

    /// Find established sender link by local handle
    #[inline]
    pub fn get_sender_link_by_local_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        self.inner.get_ref().get_sender_link_by_local_handle(hnd)
    }

    /// Find established sender link by remote handle
    #[inline]
    pub fn get_sender_link_by_remote_handle(&self, hnd: Handle) -> Option<&SenderLink> {
        self.inner.get_ref().get_sender_link_by_remote_handle(hnd)
    }

    /// Find established receiver link by local handle
    #[inline]
    pub fn get_receiver_link_by_local_handle(&self, hnd: Handle) -> Option<&ReceiverLink> {
        self.inner.get_ref().get_receiver_link_by_local_handle(hnd)
    }

    /// Find established receiver link by remote handle
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
        SenderLinkBuilder::new(name.into(), address.into(), self.inner.clone())
    }

    /// Open receiver link
    pub fn build_receiver_link<T: Into<ByteString>, U: Into<ByteString>>(
        &self,
        name: U,
        address: T,
    ) -> ReceiverLinkBuilder {
        ReceiverLinkBuilder::new(name.into(), address.into(), self.inner.clone())
    }

    /// Detach receiver link
    ///
    /// Link that is not attached yet cannot be detached, returns
    /// `AmqpProtocolError::LinkNotAttached`. Drop link attach future to cancel attach.
    pub fn detach_receiver_link(
        &self,
        handle: Handle,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> + use<> {
        self.inner
            .get_mut()
            .detach_receiver_link(handle, false, error)
    }

    /// Detach sender link
    ///
    /// Link that is not attached yet cannot be detached, returns
    /// `AmqpProtocolError::LinkNotAttached`. Drop link attach future to cancel attach.
    pub fn detach_sender_link(
        &self,
        handle: Handle,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> + use<> {
        self.inner
            .get_mut()
            .detach_sender_link(handle, false, error)
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
    /// Local link waits for remote attach, keeps initial delivery-count
    Opening(
        Option<oneshot::Sender<Result<Cell<SenderLinkInner>, AmqpProtocolError>>>,
        SequenceNo,
    ),
    Closing(Option<oneshot::Sender<Result<(), AmqpProtocolError>>>),
}

#[derive(Debug)]
enum ReceiverLinkState {
    /// Remote link waits for confirmation, keeps remote detach
    /// received before confirmation
    Opening(
        Box<Option<(Cell<ReceiverLinkInner>, Option<Source>)>>,
        Option<Detach>,
    ),
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
        matches!(self, SenderLinkState::Opening(..))
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

/// Remaining body of partially sent delivery
pub(crate) struct TransferChunks {
    body: Bytes,
    max_chunk: usize,
    message_format: Option<MessageFormat>,
}

#[derive(Debug)]
struct PendingTransfer {
    kind: Pending,
    link_handle: Handle,
}

#[derive(Debug)]
enum Pending {
    /// Transfer waits for session window
    Wake(pool::Sender<Result<(), AmqpProtocolError>>),
    /// Abort of cancelled delivery waits for session window
    Abort(DeliveryNumber),
}

impl PendingTransfer {
    fn fail(self, err: &AmqpProtocolError) {
        if let Pending::Wake(tx) = self.kind {
            let _ = tx.send(Err(err.clone()));
        }
    }
}

/// Fail link transfers and drop aborts waiting for session window
fn drop_pending_transfers(
    pending: &mut VecDeque<PendingTransfer>,
    link_handle: Handle,
    err: &AmqpProtocolError,
) {
    let mut idx = 0;
    while idx < pending.len() {
        if pending[idx].link_handle == link_handle {
            pending.remove(idx).unwrap().fail(err);
        } else {
            idx += 1;
        }
    }
}

/// Waits for session window
///
/// Woken waiter claims window until it sends transfer or gets dropped
pub(crate) struct WindowWaiter {
    rx: pool::Receiver<Result<(), AmqpProtocolError>>,
    session: Cell<SessionInner>,
    done: bool,
}

impl WindowWaiter {
    pub(crate) fn new(
        rx: pool::Receiver<Result<(), AmqpProtocolError>>,
        session: Cell<SessionInner>,
    ) -> Self {
        WindowWaiter {
            rx,
            session,
            done: false,
        }
    }
}

impl Future for WindowWaiter {
    type Output = Result<WindowClaim, AmqpProtocolError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(self.rx.poll_recv(cx));
        self.done = true;
        Poll::Ready(match result {
            Ok(Ok(())) => Ok(WindowClaim(Some(self.session.clone()))),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(AmqpProtocolError::ConnectionDropped),
        })
    }
}

impl Drop for WindowWaiter {
    fn drop(&mut self) {
        // waiter is dropped after wake up
        if !self.done
            && let Poll::Ready(Ok(Ok(()))) =
                self.rx.poll_recv(&mut Context::from_waker(Waker::noop()))
        {
            drop(WindowClaim(Some(self.session.clone())));
        }
    }
}

/// Session window claimed by woken transfer
///
/// Dropped claim wakes up next waiting transfer
pub(crate) struct WindowClaim(Option<Cell<SessionInner>>);

impl WindowClaim {
    /// Transfer frame is sent
    pub(crate) fn consume(mut self) {
        if let Some(session) = self.0.take() {
            session.get_mut().window_woken -= 1;
        }
    }
}

impl Drop for WindowClaim {
    fn drop(&mut self) {
        if let Some(session) = self.0.take() {
            let session = session.get_mut();
            session.window_woken -= 1;
            session.wake_window_waiters();
        }
    }
}

/// Local link attach response
///
/// Link attached after the attach future is dropped is detached.
pub(crate) struct AttachReceiver<T> {
    rx: oneshot::Receiver<Result<T, AmqpProtocolError>>,
    cancel: fn(&T),
}

impl<T> AttachReceiver<T> {
    pub(crate) async fn recv(&self) -> Result<T, AmqpProtocolError> {
        match self.rx.recv().await {
            Ok(res) => res,
            Err(_) => Err(AmqpProtocolError::Disconnected),
        }
    }
}

impl<T> Drop for AttachReceiver<T> {
    fn drop(&mut self) {
        // link is attached, but not received
        if let Poll::Ready(Ok(Ok(link))) =
            self.rx.poll_recv(&mut Context::from_waker(Waker::noop()))
        {
            (self.cancel)(&link);
        }
    }
}

/// Wait for link detach confirmation
async fn detach_response(
    rx: oneshot::Receiver<Result<(), AmqpProtocolError>>,
) -> Result<(), AmqpProtocolError> {
    let res = rx.await.unwrap_or(Err(AmqpProtocolError::Disconnected));
    if let Err(ref e) = res {
        log::trace!("Cannot complete link detach: {e:?}");
    }
    res
}

fn detach_closed(idx: usize) -> Detach {
    Detach(Box::new(codec::DetachInner {
        handle: idx as Handle,
        closed: true,
        error: None,
    }))
}

fn cancel_sender_attach(link: &Cell<SenderLinkInner>) {
    let inner = link.get_ref().session.inner.clone();
    let inner = inner.get_mut();
    let idx = link.get_ref().id() as usize;
    if matches!(inner.links.get(idx), Some(Either::Left(SenderLinkState::Established(l)))
        if ptr::eq(l.inner.get_ref(), link.get_ref()))
    {
        inner.detach_cancelled_link(idx);
    }
}

fn cancel_receiver_attach(link: &ReceiverLink) {
    let inner = link.session().inner.clone();
    let inner = inner.get_mut();
    let idx = link.inner.get_ref().id() as usize;
    if matches!(inner.links.get(idx), Some(Either::Right(ReceiverLinkState::Established(l)))
        if ptr::eq(l.inner.get_ref(), link.inner.get_ref()))
    {
        inner.detach_cancelled_link(idx);
    }
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
            window_woken: 0,
            on_window: condition::Condition::new(),
            remote_outgoing_window: begin.outgoing_window(),
            flags: if local { Flags::LOCAL } else { Flags::empty() },
            next_outgoing_id: INITIAL_NEXT_OUTGOING_ID,
            next_delivery_id: 0,
            unsettled_snd_deliveries: HashMap::default(),
            unsettled_rcv_deliveries: HashMap::default(),
            links: Slab::new(),
            sender_names: HashMap::default(),
            receiver_names: HashMap::default(),
            link_names: HashMap::default(),
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
        // session state is dropped already, keep first error
        if self.flags.contains(Flags::ENDED) {
            return;
        }
        log::trace!(
            "{}: Connection is failed, dropping state: {err:?}",
            self.tag()
        );

        // drop pending transfers
        for tr in self.pending_transfers.drain(..) {
            tr.fail(&err);
        }

        // drop unsettled deliveries
        for (_, mut promise) in self.unsettled_snd_deliveries.drain() {
            promise.set_error(err.clone());
        }
        for (_, mut promise) in self.unsettled_rcv_deliveries.drain() {
            promise.set_error(err.clone());
        }

        // drop links
        self.sender_names.clear();
        self.receiver_names.clear();
        self.link_names.clear();
        for (_, st) in &mut self.links {
            match st {
                Either::Left(SenderLinkState::Established(link)) => {
                    link.inner.get_mut().remote_detached(err.clone());
                }
                Either::Left(SenderLinkState::OpeningRemote { link, .. }) => {
                    link.get_mut().remote_detached(err.clone());
                }
                Either::Left(SenderLinkState::Opening(tx, _)) => {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Err(err.clone()));
                    }
                }
                Either::Left(SenderLinkState::Closing(tx))
                | Either::Right(ReceiverLinkState::Closing(tx)) => {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Err(err.clone()));
                    }
                }
                Either::Right(ReceiverLinkState::Established(link)) => {
                    link.inner.get_mut().session_ended(err.clone());
                }
                Either::Right(ReceiverLinkState::Opening(link, _)) => {
                    if let Some((link, _)) = link.as_ref() {
                        link.get_mut().session_ended(err.clone());
                    }
                }
                Either::Right(ReceiverLinkState::OpeningLocal(item)) => {
                    if let Some((link, tx)) = item.take() {
                        link.get_mut().detached();
                        let _ = tx.send(Err(err.clone()));
                    }
                }
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
                Either::Right(ReceiverLinkState::Opening(link, _)) => link
                    .as_ref()
                    .as_ref()
                    .map(|(link, _)| Either::Right(ReceiverLink::new(link.clone()))),
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
        self.set_link_name(id, attach.name().clone(), true);

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

    fn set_link_name(&mut self, idx: usize, name: ByteString, sender: bool) {
        let names = if sender {
            &mut self.sender_names
        } else {
            &mut self.receiver_names
        };
        names.insert(name.clone(), idx);
        self.link_names.insert(idx, name);
    }

    pub(crate) fn link_attach_timeout(&self) -> Seconds {
        self.sink.0.get_ref().link_attach_timeout
    }

    /// Check if link name is used by not closing link
    /// Remote handle is registered for local link
    fn has_remote_handle(&self, idx: usize) -> bool {
        self.remote_handles.values().any(|i| *i == idx)
    }

    fn is_link_name_used(&self, name: &str, sender: bool) -> bool {
        let names = if sender {
            &self.sender_names
        } else {
            &self.receiver_names
        };
        match names.get(name).and_then(|idx| self.links.get(*idx)) {
            Some(
                Either::Left(SenderLinkState::Closing(_))
                | Either::Right(ReceiverLinkState::Closing(_)),
            )
            | None => false,
            Some(_) => true,
        }
    }

    /// Remove link and its name
    fn remove_link(&mut self, idx: usize) {
        if !self.links.contains(idx) {
            return;
        }
        let sender = matches!(self.links.remove(idx), Either::Left(_));
        if let Some(name) = self.link_names.remove(&idx) {
            let names = if sender {
                &mut self.sender_names
            } else {
                &mut self.receiver_names
            };
            // name could be taken by newer link
            if names.get(&name) == Some(&idx) {
                names.remove(&name);
            }
        }
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
    ) -> AttachReceiver<Cell<SenderLinkInner>> {
        let (tx, rx) = oneshot::channel();
        let rx = AttachReceiver {
            rx,
            cancel: cancel_sender_attach,
        };
        if let Some(err) = self.ending_error() {
            let _ = tx.send(Err(err));
            return rx;
        }
        if !self.check_handle() {
            let _ = tx.send(Err(AmqpProtocolError::TooManyLinks));
            return rx;
        }
        if self.is_link_name_used(frame.name(), true) {
            let _ = tx.send(Err(AmqpProtocolError::LinkNameInUse));
            return rx;
        }

        // delivery-count is initialized by the sender
        let delivery_count = *frame.0.initial_delivery_count.get_or_insert(0);
        let entry = self.links.vacant_entry();
        let token = entry.key();
        entry.insert(Either::Left(SenderLinkState::Opening(
            Some(tx),
            delivery_count,
        )));
        log::trace!(
            "{}: Local sender link opening: {:?} hnd:{token:?}",
            self.tag(),
            frame.name(),
        );

        frame.0.handle = token as Handle;

        self.set_link_name(token, frame.0.name.clone(), true);
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
        *response.max_message_size_mut() = link.max_message_size.map(u64::from);
        self.post_frame(response.into());

        // remote detached link before confirmation
        if let Some(detach) = detach {
            log::trace!(
                "{}: Remote sender link detached before confirmation: {:?}",
                self.tag(),
                attach.name()
            );
            self.remove_link(token);
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

        // link flow received before confirmation
        if let Some(flow) = flow
            && link.get_mut().apply_flow(&flow)
        {
            self.post_flow(Some(link.get_ref().flow_state()));
        }
        SenderLink::new(link)
    }

    /// Detach sender link, returns detach confirmation future
    pub(crate) fn detach_sender_link(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> + use<> {
        let (tx, rx) = oneshot::channel();
        // session is ending, links are removed on session end
        if self.flags.intersects(Flags::ENDING | Flags::ENDED) {
            let _ = tx.send(Ok(()));
        } else {
            self.detach_sender_link_inner(id, closed, error, tx);
        }
        detach_response(rx)
    }

    fn detach_sender_link_inner(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpProtocolError>>,
    ) {
        let has_remote_handle = self.has_remote_handle(id as usize);
        if let Some(Either::Left(link)) = self.links.get_mut(id as usize) {
            match link {
                // attach response is not received
                SenderLinkState::Opening(..) if !has_remote_handle => {
                    let _ = tx.send(Err(AmqpProtocolError::LinkNotAttached));
                }
                // refused link, waiting for remote detach
                SenderLinkState::Opening(attach_tx, _) => {
                    if let Some(attach_tx) = attach_tx.take() {
                        let _ = attach_tx.send(Err(AmqpProtocolError::LinkDetached(error.clone())));
                    }
                    let detach = Detach(Box::new(codec::DetachInner {
                        handle: id,
                        closed,
                        error,
                    }));
                    *link = SenderLinkState::Closing(Some(tx));
                    self.post_frame(detach.into());
                }
                SenderLinkState::Established(sender_link) => {
                    // fail transfers waiting for link credit or session window
                    let err = AmqpProtocolError::Disconnected;
                    drop_pending_transfers(&mut self.pending_transfers, id, &err);
                    sender_link.inner.get_mut().local_detached(&err);

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
                // link is not confirmed by control service
                SenderLinkState::OpeningRemote { .. } => {
                    let _ = tx.send(Err(AmqpProtocolError::LinkNotAttached));
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
        let remote_detached = matches!(
            self.links.get(token),
            Some(Either::Left(SenderLinkState::OpeningRemote {
                detach: Some(_),
                ..
            }))
        );
        let remote_handle = attach.handle();

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

        if remote_detached {
            self.remove_link(token);
            self.remote_handles.remove(&remote_handle);
        } else {
            // handles are in use until remote detach
            self.links[token] = Either::Left(SenderLinkState::Closing(None));
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
            true,
        ));
        entry.insert(Either::Right(ReceiverLinkState::Opening(
            Box::new(Some((inner.clone(), attach.source().cloned()))),
            None,
        )));
        self.remote_handles.insert(handle, token);
        self.set_link_name(token, attach.name().clone(), false);

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
    ) -> AttachReceiver<ReceiverLink> {
        let (tx, rx) = oneshot::channel();
        let rx = AttachReceiver {
            rx,
            cancel: cancel_receiver_attach,
        };
        if let Some(err) = self.ending_error() {
            let _ = tx.send(Err(err));
            return rx;
        }
        if !self.check_handle() {
            let _ = tx.send(Err(AmqpProtocolError::TooManyLinks));
            return rx;
        }
        if self.is_link_name_used(frame.name(), false) {
            let _ = tx.send(Err(AmqpProtocolError::LinkNameInUse));
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
            false,
        ));
        entry.insert(Either::Right(ReceiverLinkState::OpeningLocal(Some((
            inner, tx,
        )))));

        frame.0.handle = token as Handle;

        self.set_link_name(token, frame.0.name.clone(), false);
        self.post_frame(Frame::Attach(frame));
        rx
    }

    /// Confirm remote receiver link, returns `false` if link is not established
    pub(crate) fn confirm_receiver_link(
        &mut self,
        inner: &Cell<ReceiverLinkInner>,
        mut response: Attach,
        max_message_size: Option<u64>,
    ) -> bool {
        let token = inner.get_ref().id();
        // session could be ended while link was waiting for confirmation
        if self.flags.intersects(Flags::ENDING | Flags::ENDED) {
            log::debug!("{}: Session is ending, receiver link: {token}", self.tag());
            return false;
        }

        // handle could be reused by another link
        let (link, detach) = match self.links.get_mut(token as usize) {
            Some(Either::Right(ReceiverLinkState::Opening(link, detach)))
                if link
                    .as_ref()
                    .as_ref()
                    .is_some_and(|(l, _)| std::ptr::eq(l.get_ref(), inner.get_ref())) =>
            {
                (link.take(), detach.take())
            }
            _ => (None, None),
        };
        let Some((link, _)) = link else {
            log::debug!("{}: Receiver link is not opening: {token}", self.tag());
            return false;
        };

        *response.max_message_size_mut() = max_message_size;
        self.post_frame(response.into());

        // remote detached link before confirmation
        if let Some(detach) = detach {
            log::trace!(
                "{}: Remote receiver link detached before confirmation: {:?}",
                self.tag(),
                link.get_ref().name()
            );
            self.remove_link(token as usize);
            self.remote_handles.remove(&detach.handle());
            self.post_frame(
                Detach(Box::new(codec::DetachInner {
                    handle: token,
                    closed: true,
                    error: None,
                }))
                .into(),
            );

            link.get_mut().remote_detached(detach.0.error.clone());
            let link = ReceiverLink::new(link);
            self.sink
                .get_control_queue()
                .enqueue_frame(ControlFrame::new(
                    link.session().inner.clone(),
                    crate::ControlFrameKind::RemoteDetachReceiver(detach, link),
                ));
            return false;
        }

        let credit = link.get_mut().confirmed();
        self.links[token as usize] = Either::Right(ReceiverLinkState::Established(
            EstablishedReceiverLink::new(link),
        ));
        if let Some((delivery_count, credit)) = credit {
            self.rcv_link_flow(token, delivery_count, credit);
        }
        true
    }

    /// Detach receiver link, returns detach confirmation future
    pub(crate) fn detach_receiver_link(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
    ) -> impl Future<Output = Result<(), AmqpProtocolError>> + use<> {
        let (tx, rx) = oneshot::channel();
        // session is ending, links are removed on session end
        if self.flags.intersects(Flags::ENDING | Flags::ENDED) {
            let _ = tx.send(Ok(()));
        } else {
            self.detach_receiver_link_inner(id, closed, error, tx);
        }
        detach_response(rx)
    }

    fn detach_receiver_link_inner(
        &mut self,
        id: Handle,
        closed: bool,
        error: Option<Error>,
        tx: oneshot::Sender<Result<(), AmqpProtocolError>>,
    ) {
        let has_remote_handle = self.has_remote_handle(id as usize);
        if let Some(Either::Right(link)) = self.links.get_mut(id as usize) {
            match link {
                ReceiverLinkState::Opening(inner, remote_detach) => {
                    let inner = inner.take();
                    let remote_detach = remote_detach.take();
                    if remote_detach.is_some() {
                        let _ = tx.send(Ok(()));
                    } else {
                        // handles are in use until remote detach
                        *link = ReceiverLinkState::Closing(Some(tx));
                    }
                    if let Some((inner, source)) = inner {
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

                    // remote detached link before rejection
                    if let Some(detach) = remote_detach {
                        self.remove_link(id as usize);
                        self.remote_handles.remove(&detach.handle());
                    }
                }
                ReceiverLinkState::Established(receiver_link) => {
                    receiver_link.inner.get_mut().local_detached();
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
                    // link is removed on remote detach
                    let _ = tx.send(Ok(()));
                    log::debug!("{}: Receiver link is closing already - {id}", self.tag());
                }
                // attach response is not received
                ReceiverLinkState::OpeningLocal(_) if !has_remote_handle => {
                    let _ = tx.send(Err(AmqpProtocolError::LinkNotAttached));
                }
                // refused link, waiting for remote detach
                ReceiverLinkState::OpeningLocal(item) => {
                    if let Some((inner, attach_tx)) = item.take() {
                        inner.get_mut().detached();
                        let _ = attach_tx.send(Err(AmqpProtocolError::LinkDetached(error.clone())));
                    }
                    let detach = Detach(Box::new(codec::DetachInner {
                        handle: id,
                        closed,
                        error,
                    }));
                    *link = ReceiverLinkState::Closing(Some(tx));
                    self.post_frame(detach.into());
                }
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
                    let mut receiver = None;
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
                        Some(Either::Right(ReceiverLinkState::Established(link))) => {
                            receiver = Some(link.inner.get_ref().flow_state());
                        }
                        _ => (),
                    }
                    // session flow state is applied in frames order
                    self.handle_flow(&flow);
                    let mut drained = false;
                    let state = if let Some(ref link) = established {
                        let inner = link.inner.get_mut();
                        drained = inner.apply_flow(&flow);
                        Some(inner.flow_state())
                    } else {
                        receiver
                    };
                    // echo reply and drained credit carry link state of attached link
                    if flow.echo() || drained {
                        self.post_flow(state);
                    }
                    if let Some(link) = established {
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
                                ReceiverLinkState::Opening(..)
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

    /// Detach local link, attach future of which is dropped
    ///
    /// Link is removed on remote detach.
    fn detach_cancelled_link(&mut self, idx: usize) {
        log::trace!("{}: Link attach is cancelled, detaching {idx}", self.tag());
        match self.links.get_mut(idx) {
            Some(Either::Left(link)) => *link = SenderLinkState::Closing(None),
            Some(Either::Right(link)) => *link = ReceiverLinkState::Closing(None),
            None => return,
        }
        self.post_frame(detach_closed(idx).into());
    }

    /// Handle `Attach` frame. return false if attach frame is remote and can not be handled
    pub(crate) fn handle_attach(&mut self, attach: &Attach, cell: Cell<SessionInner>) -> bool {
        let name = attach.name();

        // response to locally opened link has opposite role
        let index = if attach.role() == Role::Receiver {
            self.sender_names.get(name)
        } else {
            self.receiver_names.get(name)
        };
        let Some(index) = index.copied() else {
            // cannot handle remote attach
            return false;
        };

        match self.links.get_mut(index) {
            Some(Either::Left(item)) if item.is_opening() => {
                // refused link, attach fails on remote detach
                if attach.target().is_none() {
                    log::trace!(
                        "{}: Local sender link attach is refused: {name:?} {index} -> {}",
                        self.sink.tag(),
                        attach.handle()
                    );
                    self.remote_handles.insert(attach.handle(), index);
                    return true;
                }
                log::trace!(
                    "{}: Local sender link attached: {name:?} {index} -> {}, {:?}",
                    self.sink.tag(),
                    attach.handle(),
                    self.remote_handles.contains_key(&attach.handle())
                );

                self.remote_handles.insert(attach.handle(), index);
                // remote receiver initial delivery-count is ignored
                let SenderLinkState::Opening(_, delivery_count) = *item else {
                    unreachable!()
                };
                let link = Cell::new(SenderLinkInner::new(
                    index,
                    name.clone(),
                    attach.target().and_then(|t| t.address.clone()),
                    attach.handle(),
                    delivery_count,
                    cell,
                    remote_max_message_size(attach),
                ));
                let local_sender = mem::replace(
                    item,
                    SenderLinkState::Established(EstablishedSenderLink::new(link.clone())),
                );

                // attach future is dropped
                if let SenderLinkState::Opening(Some(tx), _) = local_sender
                    && tx.send(Ok(link)).is_err()
                {
                    self.detach_cancelled_link(index);
                }
                true
            }
            Some(Either::Right(item)) if item.is_opening() => {
                // refused link, attach fails on remote detach
                if attach.source().is_none() {
                    log::trace!(
                        "{}: Local receiver link attach is refused: {name:?} {index} -> {}",
                        self.sink.tag(),
                        attach.handle()
                    );
                    self.remote_handles.insert(attach.handle(), index);
                    return true;
                }
                log::trace!(
                    "{}: Local receiver link attached: {name:?} {index} -> {}",
                    self.sink.tag(),
                    attach.handle()
                );
                if let ReceiverLinkState::OpeningLocal(opt_item) = item {
                    if let Some((link, tx)) = opt_item.take() {
                        self.remote_handles.insert(attach.handle(), index);
                        // delivery-count is initialized by the sender
                        link.get_mut()
                            .set_delivery_count(attach.initial_delivery_count().unwrap_or(0));

                        *item = ReceiverLinkState::Established(EstablishedReceiverLink::new(
                            link.clone(),
                        ));
                        // attach future is dropped
                        if tx.send(Ok(ReceiverLink::new(link))).is_err() {
                            self.detach_cancelled_link(index);
                        }
                    } else {
                        // TODO: close session
                        log::error!("{}: Inconsistent session state, bug", self.tag());
                    }
                }
                true
            }
            // link with the same name is not opening, handle as remote attach
            _ => false,
        }
    }

    #[allow(clippy::too_many_lines)]
    /// Handle `Detach` frame.
    pub(crate) fn handle_detach(&mut self, mut frame: Detach) -> Action {
        // get local link instance
        let idx = if let Some(idx) = self.remote_handles.get(&frame.handle()) {
            *idx
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
                    SenderLinkState::Opening(tx, _) => {
                        // refused link
                        if let Some(tx) = tx.take() {
                            let err = AmqpProtocolError::LinkDetached(frame.0.error.clone());
                            let _ = tx.send(Err(err));
                        }
                        self.sink
                            .post_frame(AmqpFrame::new(self.id as u16, detach_closed(idx).into()));
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

                        // drop pending and unsettled transfers
                        let handle = link.inner.get_ref().id() as Handle;
                        drop_pending_transfers(&mut self.pending_transfers, handle, &err);
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
                        // detach confirmation, unsettled deliveries cannot be settled
                        let err = AmqpProtocolError::LinkDetached(frame.0.error.clone());
                        for delivery in self.unsettled_snd_deliveries.values_mut() {
                            if delivery.handle() == idx as Handle {
                                delivery.set_error(err.clone());
                            }
                        }
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
                    ReceiverLinkState::Opening(_, detach) => {
                        // detach is confirmed and link is removed after link confirmation
                        if detach.is_some() {
                            log::warn!(
                                "{}: Duplicate detach frame for unconfirmed receiver link: {frame:?}",
                                self.sink.tag()
                            );
                        } else {
                            log::trace!(
                                "{}: Detach frame received for unconfirmed receiver link: {frame:?}",
                                self.sink.tag()
                            );
                            *detach = Some(frame);
                        }
                        false
                    }
                    ReceiverLinkState::OpeningLocal(item) => {
                        // refused link
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
                        self.sink
                            .post_frame(AmqpFrame::new(self.id as u16, detach_closed(idx).into()));
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
                        link.inner.get_mut().remote_detached(error);
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
            self.remove_link(idx);
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

        // next-incoming-id is null if remote has not received begin yet
        let next_incoming_id = flow.next_incoming_id().unwrap_or(INITIAL_NEXT_OUTGOING_ID);

        // transfers sent after flow was issued consume its window
        let in_flight = self.next_outgoing_id.wrapping_sub(next_incoming_id);
        self.remote_incoming_window = if in_flight > i32::MAX as u32 {
            log::warn!(
                "{}: Session flow next-incoming-id {:?} is ahead of next-outgoing-id {:?}",
                self.tag(),
                next_incoming_id,
                self.next_outgoing_id
            );
            flow.incoming_window()
        } else {
            flow.incoming_window().saturating_sub(in_flight)
        };

        log::trace!(
            "{}: Session received credit {:?}. window: {}, pending: {}",
            self.tag(),
            flow.link_credit(),
            self.remote_incoming_window,
            self.pending_transfers.len(),
        );

        self.wake_window_waiters();
    }

    /// Wake up transfers and send aborts waiting for session window, in order
    ///
    /// Number of woken transfers is limited by window not claimed by woken transfers
    fn wake_window_waiters(&mut self) {
        while self.has_window()
            && let Some(tr) = self.pending_transfers.pop_front()
        {
            match tr.kind {
                Pending::Wake(tx) => {
                    if tx.send(Ok(())).is_ok() {
                        self.window_woken += 1;
                    }
                }
                Pending::Abort(id) => self.post_abort(tr.link_handle, id),
            }
        }
        if self.has_window() {
            self.on_window.notify(());
        }
    }

    /// Remote incoming window not claimed by woken transfers is available
    ///
    /// Queued transfers and aborts leave no window
    pub(crate) fn has_window(&self) -> bool {
        self.remote_incoming_window > self.window_woken
    }

    pub(crate) fn rcv_link_flow(&mut self, handle: u32, delivery_count: u32, credit: u32) {
        self.post_flow(Some((handle, delivery_count, credit, false)));
    }

    /// Send session flow, with link state `(handle, delivery-count, link-credit, drain)`
    pub(crate) fn post_flow(&mut self, link: Option<(Handle, codec::SequenceNo, u32, bool)>) {
        let flow = Flow(Box::new(codec::FlowInner {
            next_incoming_id: Some(self.next_incoming_id),
            incoming_window: u32::MAX,
            next_outgoing_id: self.next_outgoing_id,
            // outgoing transfers are limited by remote incoming window only
            outgoing_window: u32::MAX,
            handle: link.map(|l| l.0),
            delivery_count: link.map(|l| l.1),
            link_credit: link.map(|l| l.2),
            available: None,
            drain: link.is_some_and(|l| l.3),
            echo: false,
            properties: None,
        }));
        self.post_frame(flow.into());
    }

    #[cfg(test)]
    pub(crate) fn pending_transfers(&self) -> usize {
        self.pending_transfers.len()
    }

    /// Fail sender link transfers waiting for session window
    pub(crate) fn drop_link_transfers(&mut self, link_handle: Handle, err: &AmqpProtocolError) {
        drop_pending_transfers(&mut self.pending_transfers, link_handle, err);
    }

    /// Remote incoming window waiter, `None` if window is available
    ///
    /// Window claimed by woken transfers is not available, except own claim.
    /// Waiters are woken up to the window, queued waiters leave no window.
    pub(crate) fn window_waiter(
        &mut self,
        link_handle: Handle,
        claimed: bool,
    ) -> Result<Option<pool::Receiver<Result<(), AmqpProtocolError>>>, AmqpProtocolError> {
        let claimed_by_others = self.window_woken - u32::from(claimed);
        if let Some(err) = self.ending_error() {
            Err(err)
        } else if self.remote_incoming_window <= claimed_by_others {
            log::trace!(
                "{}: Remote window is 0, push to pending queue, hnd:{link_handle:?}",
                self.sink.tag()
            );
            let (tx, rx) = self.pool_credit.channel();
            self.pending_transfers.push_back(PendingTransfer {
                kind: Pending::Wake(tx),
                link_handle,
            });
            Ok(Some(rx))
        } else {
            Ok(None)
        }
    }

    /// Start delivery, remote incoming window must be available
    ///
    /// Sends first transfer frame, returns remaining chunks of the body
    pub(crate) fn send_transfer(
        &mut self,
        link_handle: Handle,
        tag: &Bytes,
        body: TransferBody,
        settled: bool,
        format: Option<MessageFormat>,
    ) -> Result<(DeliveryNumber, Option<TransferChunks>), AmqpProtocolError> {
        if let Some(err) = self.ending_error() {
            return Err(err);
        }

        let delivery_id = self.next_delivery_id;
        self.next_delivery_id = self.next_delivery_id.wrapping_add(1);

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
            self.post_transfer(transfer);

            Ok((
                delivery_id,
                Some(TransferChunks {
                    body,
                    max_chunk,
                    message_format,
                }),
            ))
        } else {
            transfer.0.body = Some(body);
            self.post_transfer(transfer);
            Ok((delivery_id, None))
        }
    }

    /// Send next chunk of partially sent delivery, remote incoming window must be available
    ///
    /// Returns `true` if last chunk is sent
    pub(crate) fn send_transfer_chunk(
        &mut self,
        link_handle: Handle,
        chunks: &mut TransferChunks,
    ) -> bool {
        let chunk = chunks
            .body
            .split_to(cmp::min(chunks.max_chunk, chunks.body.len()));

        let mut transfer = Transfer(Box::default());
        transfer.0.handle = link_handle;
        transfer.0.body = Some(TransferBody::Data(chunk));
        transfer.0.more = !chunks.body.is_empty();
        transfer.0.batchable = true;
        transfer.0.message_format = chunks.message_format;

        log::trace!(
            "{}: Sending chunk transfer over handle {link_handle}, more: {:?}",
            self.tag(),
            transfer.more()
        );
        self.post_transfer(transfer);
        chunks.body.is_empty()
    }

    /// Abort partially sent delivery, AMQP 1.0 2.6.14
    ///
    /// Abort waits in order with transfers if remote incoming window
    /// not claimed by woken transfers is not available
    pub(crate) fn abort_transfer(&mut self, link_handle: Handle, delivery_id: DeliveryNumber) {
        if self.has_window() {
            self.post_abort(link_handle, delivery_id);
        } else {
            log::trace!(
                "{}: Remote window is not available, push abort of delivery {delivery_id:?} to pending queue, hnd:{link_handle:?}",
                self.tag()
            );
            self.pending_transfers.push_back(PendingTransfer {
                kind: Pending::Abort(delivery_id),
                link_handle,
            });
        }
    }

    fn post_abort(&mut self, link_handle: Handle, delivery_id: DeliveryNumber) {
        log::trace!(
            "{}: Abort delivery {delivery_id:?} over handle {link_handle}",
            self.tag()
        );
        let mut transfer = Transfer(Box::default());
        transfer.0.handle = link_handle;
        transfer.0.delivery_id = Some(delivery_id);
        transfer.0.aborted = true;
        self.post_transfer(transfer);
    }

    /// Each transfer frame consumes remote incoming window, AMQP 1.0 2.5.6
    fn post_transfer(&mut self, transfer: Transfer) {
        debug_assert!(self.remote_incoming_window > 0);
        self.remote_incoming_window = self.remote_incoming_window.saturating_sub(1);
        self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);
        self.post_frame(Frame::Transfer(transfer));
    }

    pub(crate) fn post_frame(&mut self, frame: Frame) {
        // no frames are sent after session end, AMQP 1.0 2.5.5
        if self.flags.intersects(Flags::ENDING | Flags::ENDED) {
            log::trace!(
                "{}: Session is ending, drop frame {}",
                self.tag(),
                frame.name()
            );
        } else {
            self.sink.post_frame(AmqpFrame::new(self.id(), frame));
        }
    }

    /// Session error, set when session is ended
    pub(crate) fn error(&self) -> Option<&AmqpProtocolError> {
        self.error.as_ref()
    }

    /// Session end error, if session is ending or ended
    fn ending_error(&self) -> Option<AmqpProtocolError> {
        if self.flags.intersects(Flags::ENDING | Flags::ENDED) {
            Some(
                self.error
                    .clone()
                    .unwrap_or(AmqpProtocolError::SessionEnded(None)),
            )
        } else {
            None
        }
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
