use ntex::codec::{Decoder, Encoder};
use ntex_amqp_codec::AmqpCodecError;
use ntex_amqp_codec::protocol::{
    Accepted, Attach, AttachInner, Begin, BeginInner, DeliveryState, Detach, DetachInner,
    Disposition, DispositionInner, Flow, FlowInner, LinkError, Open, OpenInner, Received,
    ReceiverSettleMode, Rejected, SenderSettleMode, Source, Target, TerminusDurability,
    TerminusExpiryPolicy, Transfer, TransferBody, TransferInner,
};
use ntex_amqp_codec::types::{Multiple, Symbol, Variant};
use ntex_bytes::{BytePages, Bytes, BytesMut};
use ntex_io::{Io, testing::IoTest};
use ntex_service::cfg::SharedCfg;

use ntex::time::{Millis, sleep, timeout};
use ntex_util::time::Seconds;
use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::{future::Future, future::poll_fn, pin::Pin};

use crate::codec::protocol::{
    self as codec, AmqpError, Close, End, Error, Frame, Role, SessionError,
};
use crate::codec::{AmqpCodec, AmqpFrame};
use crate::connection::*;
use crate::delivery::DeliveryInner;
use crate::error::AmqpProtocolError;
use crate::rcvlink::ReceiverLink;
use crate::session::Session;
use crate::sndlink::SenderLink;
use crate::types::Action;
use crate::{AmqpServiceConfig, RemoteServiceConfig};

mod deliveries;
mod frames;
mod local_links;
mod receiver_links;
mod remote_links;
mod sender_links;
mod sessions;

const LONG: &str = "value-that-does-not-fit-into-inline-storage";
const ACCEPTED: DeliveryState = DeliveryState::Accepted(Accepted {});

fn symbols() -> Multiple<Symbol> {
    Multiple(vec![Symbol::from(LONG)])
}

fn rejected() -> DeliveryState {
    DeliveryState::Rejected(Rejected {
        error: Some(Error(Box::new(codec::ErrorInner {
            condition: AmqpError::InternalError.into(),
            description: Some(LONG.into()),
            info: None,
        }))),
    })
}

pub(crate) fn begin() -> Frame {
    Begin(Box::new(BeginInner {
        remote_channel: None,
        next_outgoing_id: 1,
        incoming_window: 100,
        outgoing_window: 100,
        handle_max: 10,
        offered_capabilities: Some(symbols()),
        desired_capabilities: None,
        properties: None,
    }))
    .into()
}

fn attach() -> Frame {
    Attach(Box::new(AttachInner {
        name: LONG.into(),
        handle: 0,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source {
            address: Some(LONG.into()),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::SessionEnd,
            timeout: 0,
            dynamic: false,
            dynamic_node_properties: None,
            distribution_mode: None,
            filter: None,
            default_outcome: None,
            outcomes: None,
            capabilities: Some(symbols()),
        }),
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }))
    .into()
}

pub(crate) fn transfer(
    delivery_id: u32,
    more: bool,
    state: Option<DeliveryState>,
    body: u8,
) -> Frame {
    Transfer(Box::new(TransferInner {
        handle: 0,
        delivery_id: Some(delivery_id),
        delivery_tag: Some(Bytes::from(LONG)),
        message_format: None,
        settled: Some(false),
        more,
        rcv_settle_mode: None,
        state,
        resume: false,
        aborted: false,
        batchable: false,
        body: Some(TransferBody::Data(Bytes::from(vec![body; 10]))),
    }))
    .into()
}

async fn attach_with_timeout(
    s: Session,
    sender: bool,
    timeout: Option<Seconds>,
) -> Result<(), AmqpProtocolError> {
    if sender {
        let mut builder = s.build_sender_link("x", "l");
        if let Some(timeout) = timeout {
            builder = builder.attach_timeout(timeout);
        }
        builder.attach().await.map(|_| ())
    } else {
        let mut builder = s.build_receiver_link("x", "l");
        if let Some(timeout) = timeout {
            builder = builder.attach_timeout(timeout);
        }
        builder.attach().await.map(|_| ())
    }
}

pub(crate) fn handle_frame(conn: &Connection, frame: Frame) -> Result<Action, AmqpProtocolError> {
    let inner = conn.get_ref();
    inner.handle_frame(AmqpFrame::new(0, frame))
}

pub(crate) fn connection() -> (Io, Connection, IoTest) {
    connection_with(AmqpServiceConfig::new())
}

fn connection_with(cfg: AmqpServiceConfig) -> (Io, Connection, IoTest) {
    let remote = RemoteServiceConfig::new(&Open(Box::default()));
    let (server, client) = IoTest::create();
    client.remote_buffer_cap(64 * 1024);
    let cfg = SharedCfg::new("T").add(cfg).build();
    let io = Io::new(server, cfg.clone());
    let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
    (io, conn, client)
}

fn session(conn: &Connection) -> Session {
    let inner = conn.get_ref();
    let SessionState::Established(session) =
        &inner.0.get_ref().sessions[inner.0.get_ref().sessions_map[&0]]
    else {
        panic!()
    };
    Session::new(session.clone())
}

fn peer_attach(role: Role, max_message_size: Option<u64>) -> Frame {
    let Frame::Attach(mut attach) = attach() else {
        panic!()
    };
    attach.0.role = role;
    attach.0.max_message_size = max_message_size;
    attach.0.target = Some(Target::default());
    attach.into()
}

/// Receive message, returns (delivered, detached with message-size-exceeded)
async fn receive(local: bool, max: u64, frames: &[bool]) -> (bool, bool) {
    let (_io, conn, client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();

    let link = if local {
        let session = session(&conn);
        let fut = ntex::rt::spawn(async move {
            session
                .build_receiver_link(LONG, LONG)
                .max_message_size(max)
                .attach()
                .await
        });
        sleep(Millis(10)).await;
        handle(attach()).unwrap();
        fut.await.unwrap().unwrap()
    } else {
        let Ok(Action::AttachReceiver(link, _, response)) = handle(attach()) else {
            panic!()
        };
        link.set_max_message_size(max);
        link.confirm_receiver_link(response);
        link
    };
    link.set_link_credit(10);
    for (idx, more) in frames.iter().enumerate() {
        handle(transfer(0, *more, None, idx as u8)).unwrap();
    }

    sleep(Millis(50)).await;
    let exceeded = detaches(&client).iter().any(|(_, _, err)| {
        err.as_ref()
            .is_some_and(|e| *e.condition() == LinkError::MessageSizeExceeded.into())
    });
    (link.get_delivery().is_some(), exceeded)
}

async fn sender(local: bool, max: Option<u64>) -> (SenderLink, (Io, Connection, IoTest)) {
    let (io, conn, client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();

    let link = if local {
        let session = session(&conn);
        let fut =
            ntex::rt::spawn(async move { session.build_sender_link(LONG, LONG).attach().await });
        sleep(Millis(10)).await;
        handle(peer_attach(Role::Receiver, max)).unwrap();
        fut.await.unwrap().unwrap()
    } else {
        let Ok(Action::AttachSender(link, _, _)) = handle(peer_attach(Role::Receiver, max)) else {
            panic!()
        };
        link
    };
    (link, (io, conn, client))
}

fn peer_flow(link_credit: Option<u32>) -> Frame {
    Flow(Box::new(FlowInner {
        next_incoming_id: Some(1),
        incoming_window: 100,
        next_outgoing_id: 1,
        outgoing_window: 100,
        handle: Some(0),
        delivery_count: Some(0),
        link_credit,
        available: None,
        drain: false,
        echo: false,
        properties: None,
    }))
    .into()
}

pub(crate) fn named_attach(role: Role, name: &str, address: &str, handle: u32) -> Frame {
    let Frame::Attach(mut attach) = attach() else {
        panic!()
    };
    attach.0.role = role;
    attach.0.name = name.into();
    attach.0.handle = handle;
    attach.0.source.as_mut().unwrap().address = Some(address.into());
    attach.0.target = Some(Target {
        address: Some(address.into()),
        durable: TerminusDurability::None,
        expiry_policy: TerminusExpiryPolicy::SessionEnd,
        timeout: 0,
        dynamic: false,
        dynamic_node_properties: None,
        capabilities: None,
    });
    attach.into()
}

fn peer_detach(handle: u32) -> Frame {
    Detach(Box::new(DetachInner {
        handle,
        closed: true,
        error: None,
    }))
    .into()
}

/// Decode frames written to the peer, returns (channel, frame)
fn read_channel_frames(client: &IoTest) -> Vec<(u16, Frame)> {
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    let mut frames = Vec::new();
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        frames.push(frame.into_parts());
    }
    frames
}

/// Decode frames written to the peer
fn read_frames(client: &IoTest) -> Vec<Frame> {
    read_channel_frames(client)
        .into_iter()
        .map(|(_, frame)| frame)
        .collect()
}

/// Detach frames written to the peer, (handle, closed, error)
fn detaches(client: &IoTest) -> Vec<(u32, bool, Option<Error>)> {
    read_frames(client)
        .into_iter()
        .filter_map(|frame| match frame {
            Frame::Detach(det) => Some((det.handle(), det.closed(), det.error().cloned())),
            _ => None,
        })
        .collect()
}

/// Flow frames written to the peer
fn flows(client: &IoTest) -> Vec<Flow> {
    read_frames(client)
        .into_iter()
        .filter_map(|frame| match frame {
            Frame::Flow(flow) => Some(flow),
            _ => None,
        })
        .collect()
}

fn frame_names(client: &IoTest) -> Vec<String> {
    read_frames(client)
        .into_iter()
        .map(|frame| match frame {
            Frame::Attach(att) => format!("Attach {} {}", att.name(), att.handle()),
            Frame::Detach(det) => format!("Detach {}", det.handle()),
            Frame::Flow(flow) => {
                format!("Flow {:?} {:?}", flow.handle(), flow.link_credit())
            }
            frame => frame.name().to_string(),
        })
        .collect()
}

/// Number of unsettled (sender, receiver) deliveries in session
fn unsettled(conn: &Connection) -> (usize, usize) {
    let s = session(conn);
    let inner = s.inner.get_ref();
    (
        inner.unsettled_snd_deliveries.len(),
        inner.unsettled_rcv_deliveries.len(),
    )
}

/// Poll future once with noop waker
fn poll_once<F: Future + ?Sized>(fut: Pin<&mut F>) -> Poll<F::Output> {
    fut.poll(&mut Context::from_waker(Waker::noop()))
}

/// Waker that counts wake-ups
struct WakeCounter(AtomicUsize);

impl WakeCounter {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicUsize::new(0)))
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Returns true if future is pending
async fn pending<F: Future + ?Sized>(mut fut: Pin<&mut F>) -> bool {
    poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_pending())).await
}

/// Local receiver link "r", attached by remote sender with `handle`
async fn local_receiver(conn: &Connection, handle: u32) -> ReceiverLink {
    let s = session(conn);
    let fut = ntex::rt::spawn(async move { s.build_receiver_link("r", "r").attach().await });
    sleep(Millis(10)).await;
    let Ok(Action::None) = handle_frame(conn, named_attach(Role::Sender, "r", "r", handle)) else {
        panic!()
    };
    fut.await.unwrap().unwrap()
}

fn small_frames_sender() -> (Io, Connection, IoTest, SenderLink) {
    let remote = RemoteServiceConfig::new(&Open(Box::new(OpenInner {
        max_frame_size: 512,
        ..Default::default()
    })));
    let (server, client) = IoTest::create();
    client.remote_buffer_cap(64 * 1024);
    let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
    let io = Io::new(server, cfg.clone());
    let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
    handle_frame(&conn, begin()).unwrap();

    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
    else {
        panic!()
    };
    let snd = session(&conn).inner.get_mut().attach_remote_sender_link(
        &attach,
        response,
        snd.inner.clone(),
    );
    let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
        panic!()
    };
    flow.0.handle = Some(4);
    flow.0.incoming_window = 2;
    handle_frame(&conn, flow.into()).unwrap();
    (io, conn, client, snd)
}

fn session_window(conn: &Connection, next_incoming_id: u32, window: u32) {
    let Frame::Flow(mut flow) = peer_flow(None) else {
        panic!()
    };
    flow.0.handle = None;
    flow.0.next_incoming_id = Some(next_incoming_id);
    flow.0.incoming_window = window;
    handle_frame(conn, flow.into()).unwrap();
}

fn transfer_frames(client: &IoTest) -> Vec<String> {
    read_frames(client)
        .into_iter()
        .map(|frame| match frame {
            Frame::Transfer(tr) => format!(
                "Transfer {:?} more:{} aborted:{}",
                tr.delivery_id(),
                tr.more(),
                tr.aborted()
            ),
            Frame::Flow(flow) => format!("Flow {}", flow.next_outgoing_id()),
            frame => frame.name().to_string(),
        })
        .collect()
}

/// Sender link with link credit 10, sets session window
fn add_sender(conn: &Connection, name: &str, handle: u32, window: u32) -> SenderLink {
    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(conn, named_attach(Role::Receiver, name, name, handle))
    else {
        panic!()
    };
    let snd = session(conn).inner.get_mut().attach_remote_sender_link(
        &attach,
        response,
        snd.inner.clone(),
    );
    link_flow(conn, handle, (0, 10, false), 1, window);
    snd
}

/// Link flow with `(delivery-count, link-credit, drain)` and session window
fn link_flow(
    conn: &Connection,
    handle: u32,
    link: (u32, u32, bool),
    next_incoming_id: u32,
    window: u32,
) {
    let Frame::Flow(mut flow) = peer_flow(Some(link.1)) else {
        panic!()
    };
    flow.0.handle = Some(handle);
    flow.0.delivery_count = Some(link.0);
    flow.0.drain = link.2;
    flow.0.next_incoming_id = Some(next_incoming_id);
    flow.0.incoming_window = window;
    handle_frame(conn, flow.into()).unwrap();
}

/// Sender with partial delivery waiting for session window, and second sender waiting
async fn queued_abort_sender() -> (
    Io,
    Connection,
    IoTest,
    SenderLink,
    Pin<Box<dyn Future<Output = Result<crate::Delivery, AmqpProtocolError>>>>,
) {
    let (io, conn, client, snd) = small_frames_sender();
    let snd2 = add_sender(&conn, "s2", 5, 1);
    sleep(Millis(50)).await;
    transfer_frames(&client);

    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
    let mut t2: Pin<Box<dyn Future<Output = _>>> =
        Box::pin(snd2.transfer(Bytes::from_static(b"2")).send());
    assert!(pending(t1.as_mut()).await);
    assert!(pending(t2.as_mut()).await);
    drop(t1);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(0) more:true aborted:false"]
    );
    (io, conn, client, snd, t2)
}
