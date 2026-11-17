//! Dispatcher control-frame handling, service errors, idle ping and shutdown.
use std::cell::{Cell as StdCell, RefCell};
use std::rc::Rc;
use std::{io, marker::PhantomData};

use ntex_dispatcher::{DispatchItem, Reason};
use ntex_service::{BindCtx, Ctx, ReadyNotifier, Service, pipeline::Pipeline};

use super::*;
use crate::dispatcher::Dispatcher;
use crate::error::{AmqpDispatcherError, Error as ProtoError, LinkError as Link};
use crate::types::Message;
use crate::{ControlFrame, ControlFrameKind};

type Item = DispatchItem<AmqpCodec<AmqpFrame>>;

fn kind_name(kind: &ControlFrameKind) -> String {
    match kind {
        ControlFrameKind::AttachSender(..) => "AttachSender".into(),
        ControlFrameKind::AttachReceiver(..) => "AttachReceiver".into(),
        ControlFrameKind::Flow(flow, _) => format!("Flow {:?}", flow.link_credit()),
        ControlFrameKind::LocalDetachSender(..) => "LocalDetachSender".into(),
        ControlFrameKind::RemoteDetachSender(..) => "RemoteDetachSender".into(),
        ControlFrameKind::LocalDetachReceiver(..) => "LocalDetachReceiver".into(),
        ControlFrameKind::RemoteDetachReceiver(..) => "RemoteDetachReceiver".into(),
        ControlFrameKind::LocalSessionEnded(links) => format!("LocalSessionEnded {}", links.len()),
        ControlFrameKind::RemoteSessionEnded(links) => {
            format!("RemoteSessionEnded {}", links.len())
        }
        ControlFrameKind::ProtocolError(err) => format!("ProtocolError {err}"),
        ControlFrameKind::Disconnected(err) => {
            format!("Disconnected {:?}", err.as_ref().map(io::Error::kind))
        }
        ControlFrameKind::Closed => "Closed".into(),
    }
}

fn link_flow_frame(handle: u32, credit: u32) -> Frame {
    let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
        panic!()
    };
    flow.0.handle = Some(handle);
    flow.into()
}

/// Shared recording state of a test service
#[derive(Clone)]
struct Flags {
    log: Rc<RefCell<Vec<String>>>,
    call_err: Rc<StdCell<bool>>,
    ready_err: Rc<StdCell<bool>>,
    ready_pending: Rc<StdCell<bool>>,
    changed: Rc<ReadyNotifier>,
    shutdown: Rc<StdCell<usize>>,
}

impl Flags {
    fn new() -> Self {
        Flags {
            log: Rc::new(RefCell::new(Vec::new())),
            call_err: Rc::new(StdCell::new(false)),
            ready_err: Rc::new(StdCell::new(false)),
            ready_pending: Rc::new(StdCell::new(false)),
            changed: Rc::new(ReadyNotifier::new()),
            shutdown: Rc::new(StdCell::new(0)),
        }
    }

    fn take(&self) -> Vec<String> {
        self.log.borrow_mut().drain(..).collect()
    }

    fn error(reason: &'static str) -> ProtoError {
        Link::force_detach().description(reason).into()
    }
}

struct Svc<T> {
    flags: Flags,
    _t: PhantomData<T>,
}

impl<T> Svc<T> {
    fn new(flags: &Flags) -> Self {
        Svc {
            flags: flags.clone(),
            _t: PhantomData,
        }
    }

    fn record(&self, name: String) -> Result<(), ProtoError> {
        self.flags.log.borrow_mut().push(name);
        if self.flags.call_err.get() {
            Err(Flags::error("call failed"))
        } else {
            Ok(())
        }
    }
}

impl Service<(), Message> for Svc<Message> {
    type Res = ();
    type Error = ProtoError;

    fn bind(&self, ctx: BindCtx<'_>) {
        ctx.subscribe(&self.flags.changed);
    }

    async fn call(&self, req: Message, _: Ctx<'_, Self, ()>) -> Result<(), ProtoError> {
        self.record(match req {
            Message::Attached(..) => "Attached".into(),
            Message::Detached(_) => "Detached".into(),
            Message::DetachedAll(links) => format!("DetachedAll {}", links.len()),
            Message::Transfer(_) => "Transfer".into(),
        })
    }

    async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), ProtoError> {
        poll_fn(|_| {
            if self.flags.ready_pending.get() {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        if self.flags.ready_err.get() {
            Err(Flags::error("publish is not ready"))
        } else {
            Ok(())
        }
    }

    async fn shutdown(&self, _: Ctx<'_, Self, ()>) {
        self.flags.shutdown.set(self.flags.shutdown.get() + 1);
    }
}

impl Service<(), ControlFrame> for Svc<ControlFrame> {
    type Res = ();
    type Error = ProtoError;

    fn bind(&self, ctx: BindCtx<'_>) {
        ctx.subscribe(&self.flags.changed);
    }

    async fn call(&self, req: ControlFrame, _: Ctx<'_, Self, ()>) -> Result<(), ProtoError> {
        self.record(kind_name(req.kind()))
    }

    async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), ProtoError> {
        if self.flags.ready_err.get() {
            Err(Flags::error("control is not ready"))
        } else {
            Ok(())
        }
    }

    async fn shutdown(&self, _: Ctx<'_, Self, ()>) {
        self.flags.shutdown.set(self.flags.shutdown.get() + 1);
    }
}

struct Harness {
    _io: Io,
    conn: ConnectionRef,
    client: IoTest,
    disp: Pipeline<Item, Option<AmqpFrame>, AmqpDispatcherError>,
    publish: Flags,
    control: Flags,
}

impl Harness {
    fn new(idle_timeout: Millis) -> Harness {
        let (io, conn, client) = connection();
        let publish = Flags::new();
        let control = Flags::new();
        let cref = conn.get_ref();
        let disp = Pipeline::new(
            (),
            Dispatcher::new(
                conn,
                Pipeline::new((), Svc::<Message>::new(&publish)),
                Pipeline::new((), Svc::<ControlFrame>::new(&control)),
                idle_timeout,
            ),
        );
        Harness {
            _io: io,
            conn: cref,
            client,
            disp,
            publish,
            control,
        }
    }

    async fn frame(&self, frame: Frame) -> Result<Option<AmqpFrame>, AmqpDispatcherError> {
        self.disp
            .call(DispatchItem::Item(AmqpFrame::new(0, frame)))
            .await
    }

    /// Run queued control/publish calls
    async fn settle(&self) {
        sleep(Millis(25)).await;
        let _ = self.disp.ready().await;
        sleep(Millis(25)).await;
    }
}

/// STATEMENT: Dispatcher stop reasons are reported to the control service as
/// `ProtocolError` or `Disconnected`, except `Reason::Service`.
///
/// HOW:
/// - Dispatch each codec, timeout and I/O `Stop` reason on a fresh harness.
/// - Confirm the call returns `None`, the control service logs the expected kind and the
///   publish service logs nothing.
/// - Dispatch `Stop(Reason::Service)` and confirm the control service logs nothing.
#[ntex::test]
async fn stop_reasons_map_to_control_frames() {
    let cases: Vec<(fn() -> Item, &str)> = vec![
        (
            || DispatchItem::Stop(Reason::Decoder(AmqpCodecError::UnparsedBytesLeft)),
            "ProtocolError Codec error: UnparsedBytesLeft",
        ),
        (
            || DispatchItem::Stop(Reason::Encoder(AmqpCodecError::MaxSizeExceeded)),
            "ProtocolError Codec error: MaxSizeExceeded",
        ),
        (
            || DispatchItem::Stop(Reason::KeepAlive),
            "ProtocolError Keep-alive timeout",
        ),
        (
            || DispatchItem::Stop(Reason::ReadTimeout),
            "ProtocolError Read timeout",
        ),
        (
            || DispatchItem::Stop(Reason::WriteTimeout),
            "ProtocolError Write timeout",
        ),
        (
            || DispatchItem::Stop(Reason::Io(Some(io::Error::from(io::ErrorKind::BrokenPipe)))),
            "Disconnected Some(BrokenPipe)",
        ),
        (|| DispatchItem::Stop(Reason::Io(None)), "Disconnected None"),
    ];

    for (item, expected) in cases {
        let h = Harness::new(Millis::ZERO);
        assert!(h.disp.call(item()).await.unwrap().is_none());
        sleep(Millis(25)).await;
        assert_eq!(h.control.take(), [expected]);
        assert_eq!(h.publish.take(), Vec::<String>::new());
    }

    // these are not reported to the control service
    let h = Harness::new(Millis::ZERO);
    assert!(
        h.disp
            .call(DispatchItem::Stop(Reason::Service))
            .await
            .unwrap()
            .is_none()
    );
    sleep(Millis(25)).await;
    assert_eq!(h.control.take(), Vec::<String>::new());
}

/// STATEMENT: A protocol stop reason fails dispatcher readiness and closes the
/// connection with that error.
///
/// HOW:
/// - Dispatch `Stop(Reason::KeepAlive)`.
/// - Confirm `ready` fails with `Protocol(KeepAliveTimeout)`.
/// - Confirm the connection is not opened and its error is `"Keep-alive timeout"`.
#[ntex::test]
async fn protocol_error_fails_dispatcher() {
    let h = Harness::new(Millis::ZERO);
    h.disp
        .call(DispatchItem::Stop(Reason::KeepAlive))
        .await
        .unwrap();
    sleep(Millis(25)).await;

    // control frame handling reports the error through readiness
    let err = h.disp.ready().await.err().unwrap();
    assert!(matches!(
        err,
        AmqpDispatcherError::Protocol(AmqpProtocolError::KeepAliveTimeout)
    ));
    assert!(!h.conn.is_opened());
    assert_eq!(
        h.conn.get_error().unwrap().to_string(),
        "Keep-alive timeout"
    );
}

/// STATEMENT: A remote `Close` with an error is reported to the control service and
/// fails dispatcher readiness.
///
/// HOW:
/// - Dispatch a `Close` with an `InternalError` described as `"boom"`.
/// - Confirm the control service logs a `ProtocolError` carrying that error.
/// - Confirm `ready` fails.
#[ntex::test]
async fn remote_close_with_error() {
    let h = Harness::new(Millis::ZERO);
    h.frame(
        Close {
            error: Some(Error(Box::new(codec::ErrorInner {
                condition: AmqpError::InternalError.into(),
                description: Some("boom".into()),
                info: None,
            }))),
        }
        .into(),
    )
    .await
    .unwrap();
    sleep(Millis(25)).await;

    assert_eq!(
        h.control.take(),
        [
            "ProtocolError Connection closed, error: Some(Error(ErrorInner \
             { condition: AmqpError(InternalError), description: Some(\"boom\"), info: None }))"
        ]
    );
    assert!(h.disp.ready().await.is_err());
}

/// STATEMENT: A remote sender attach is confirmed through the control service, and its
/// transfers and detach are routed to the publish service.
///
/// HOW:
/// - Dispatch `Begin` and a sender `Attach`; confirm `AttachReceiver` and `Attached`
///   are logged and `Begin`, `Attach r 0` and a 50-credit `Flow` are written.
/// - Dispatch a `Transfer` and confirm the publish service logs `Transfer`.
/// - Dispatch a peer `Detach` and confirm `Detached` and `RemoteDetachReceiver`.
#[ntex::test]
async fn attach_receiver_flow() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.frame(named_attach(Role::Sender, "r", "r", 0))
        .await
        .unwrap();
    h.settle().await;

    // control service confirms the link, publish service is notified
    assert_eq!(h.control.take(), ["AttachReceiver"]);
    assert_eq!(h.publish.take(), ["Attached"]);
    assert_eq!(
        frame_names(&h.client),
        ["Begin", "Attach r 0", "Flow Some(0) Some(50)"]
    );

    // transfers are routed to the publish service
    h.frame(transfer(0, false, Some(ACCEPTED), 1))
        .await
        .unwrap();
    assert_eq!(h.publish.take(), ["Transfer"]);

    // remote detach releases publish resources
    h.frame(peer_detach(0)).await.unwrap();
    h.settle().await;
    assert_eq!(h.publish.take(), ["Detached"]);
    assert_eq!(h.control.take(), ["RemoteDetachReceiver"]);
}

/// STATEMENT: A publish service error for a transfer detaches the receiver link with
/// that error.
///
/// HOW:
/// - Attach a receiver link, then make the publish service `call` fail.
/// - Dispatch a `Transfer`.
/// - Confirm one closing `Detach` for handle 0 described as `"call failed"`.
#[ntex::test]
async fn publish_error_closes_link() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.frame(named_attach(Role::Sender, "r", "r", 0))
        .await
        .unwrap();
    h.settle().await;
    let _ = frame_names(&h.client);

    h.publish.call_err.set(true);
    h.frame(transfer(0, false, None, 1)).await.unwrap();
    sleep(Millis(25)).await;

    // the link is detached with the publish service error
    let detaches = detaches(&h.client);
    assert_eq!(detaches.len(), 1);
    let (handle, closed, err) = &detaches[0];
    assert_eq!(*handle, 0);
    assert!(closed);
    assert_eq!(
        err.as_ref()
            .unwrap()
            .description()
            .map(ntex_bytes::ByteString::as_str),
        Some("call failed")
    );
}

/// STATEMENT: A control service error for a remote sender attach rejects the receiver
/// link without involving the publish service.
///
/// HOW:
/// - Make the control service `call` fail and dispatch a sender `Attach` on handle 1.
/// - Confirm the publish service logs nothing.
/// - Confirm written frames start with `Begin` and include `Detach 0`, with no further
///   detach left to read.
#[ntex::test]
async fn control_error_closes_receiver() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.control.call_err.set(true);
    h.frame(named_attach(Role::Sender, "r", "r", 1))
        .await
        .unwrap();
    h.settle().await;

    // link is never confirmed, publish service is not involved
    assert_eq!(h.publish.take(), Vec::<String>::new());
    let names = frame_names(&h.client);
    assert_eq!(names[0], "Begin");
    assert_eq!(detaches(&h.client).len(), 0);
    assert!(names.iter().any(|n| n == "Detach 0"));
}

/// STATEMENT: A remote receiver attach opens a sender link through the control service,
/// and its flow and detach are reported to the control service only.
///
/// HOW:
/// - Dispatch a receiver `Attach`; confirm `AttachSender`, an opened sender link `"s"`
///   and a written `Attach s 0`.
/// - Dispatch a link `Flow` with credit 7; confirm `Flow Some(7)` and credit 7.
/// - Dispatch a peer `Detach`; confirm `RemoteDetachSender` and no publish calls.
#[ntex::test]
async fn attach_sender_and_flow() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.frame(named_attach(Role::Receiver, "s", "s", 2))
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(h.control.take(), ["AttachSender"]);

    // sender link is attached by the control service
    let snd = session_ref(&h.conn).get_sender_link("s").unwrap();
    assert!(snd.is_opened());
    assert!(frame_names(&h.client).iter().any(|n| n == "Attach s 0"));

    // remote flow is reported after link credit is applied
    let Frame::Flow(mut flow) = peer_flow(Some(7)) else {
        panic!()
    };
    flow.0.handle = Some(2);
    h.frame(flow.into()).await.unwrap();
    h.settle().await;
    assert_eq!(h.control.take(), ["Flow Some(7)"]);
    assert_eq!(snd.credit(), 7);

    // remote detach of a sender link
    h.frame(peer_detach(2)).await.unwrap();
    h.settle().await;
    assert_eq!(h.control.take(), ["RemoteDetachSender"]);
    assert_eq!(h.publish.take(), Vec::<String>::new());
}

/// STATEMENT: A control service error for a remote receiver attach detaches the
/// unconfirmed sender link with that error.
///
/// HOW:
/// - Make the control service `call` fail and dispatch a receiver `Attach`.
/// - Confirm sender link `"s"` is not registered.
/// - Confirm one `Detach` is written described as `"call failed"`.
#[ntex::test]
async fn attach_sender_control_error() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.control.call_err.set(true);
    h.frame(named_attach(Role::Receiver, "s", "s", 2))
        .await
        .unwrap();
    h.settle().await;

    // unconfirmed sender link is detached with the error
    assert!(session_ref(&h.conn).get_sender_link("s").is_none());
    let detaches = detaches(&h.client);
    assert_eq!(detaches.len(), 1);
    assert_eq!(
        detaches[0]
            .2
            .as_ref()
            .unwrap()
            .description()
            .map(ntex_bytes::ByteString::as_str),
        Some("call failed")
    );
}

/// STATEMENT: A remote `End` reports all session links to the control service and only
/// receiver links to the publish service.
///
/// HOW:
/// - Attach a receiver link and a sender link.
/// - Dispatch `End`.
/// - Confirm `RemoteSessionEnded 2` for control and `DetachedAll 1` for publish.
#[ntex::test]
async fn remote_session_ended() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.frame(named_attach(Role::Sender, "r", "r", 1))
        .await
        .unwrap();
    h.frame(named_attach(Role::Receiver, "s", "s", 2))
        .await
        .unwrap();
    h.settle().await;
    let _ = (h.control.take(), h.publish.take());

    h.frame(End { error: None }.into()).await.unwrap();
    h.settle().await;

    // both links are reported, only the receiver goes to the publish service
    assert_eq!(h.control.take(), ["RemoteSessionEnded 2"]);
    assert_eq!(h.publish.take(), ["DetachedAll 1"]);
}

/// STATEMENT: A locally closed receiver link is reported to the control and publish
/// services once the peer confirms the detach.
///
/// HOW:
/// - Attach a receiver link and start `close` on it.
/// - Dispatch the peer `Detach` and confirm `close` succeeds.
/// - Confirm `LocalDetachReceiver` for control and `Detached` for publish.
#[ntex::test]
async fn local_detach_control_frames() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.frame(named_attach(Role::Sender, "r", "r", 0))
        .await
        .unwrap();
    h.settle().await;
    let _ = (h.control.take(), h.publish.take());

    // locally closed link is queued as a control frame
    let link = session_ref(&h.conn)
        .get_receiver_link_by_remote_handle(0)
        .unwrap();
    let fut = ntex::rt::spawn(async move { link.close().await });
    sleep(Millis(10)).await;
    h.frame(peer_detach(0)).await.unwrap();
    fut.await.unwrap().unwrap();
    h.settle().await;

    assert_eq!(h.control.take(), ["LocalDetachReceiver"]);
    assert_eq!(h.publish.take(), ["Detached"]);
}

/// STATEMENT: A positive idle timeout sends periodic empty frames until shutdown, and a
/// zero timeout disables them.
///
/// HOW:
/// - With 100ms, confirm nothing is written at `ready`, then two reads contain only
///   empty frames, and nothing is written after `shutdown`.
/// - With a zero timeout, confirm nothing is written after 80ms.
/// - With 1ms, confirm empty frames are still written after halving the interval.
#[ntex::test]
async fn idle_timeout_ping() {
    // Activity is checked at half of the idle timeout budget.
    let h = Harness::new(Millis(100));
    h.disp.ready().await.unwrap();
    assert_eq!(read_frames(&h.client), Vec::<Frame>::new());

    // Pings continue without further readiness checks.
    for _ in 0..2 {
        let bytes = timeout(Millis(500), h.client.read())
            .await
            .unwrap()
            .unwrap();
        assert!(!bytes.is_empty());
        for frame in bytes.chunks(8) {
            assert_eq!(frame, b"\0\0\0\x08\x02\0\0\0");
        }
    }

    h.disp.shutdown().await;
    sleep(Millis(100)).await;
    assert_eq!(read_frames(&h.client), Vec::<Frame>::new());

    // zero timeout disables the ping
    let h = Harness::new(Millis::ZERO);
    h.disp.ready().await.unwrap();
    sleep(Millis(80)).await;
    h.disp.ready().await.unwrap();
    assert_eq!(read_frames(&h.client), Vec::<Frame>::new());

    // A positive timeout must not disable the timer when the interval is halved.
    let h = Harness::new(Millis(1));
    let bytes = timeout(Millis(500), h.client.read())
        .await
        .unwrap()
        .unwrap();
    assert!(!bytes.is_empty());
    for frame in bytes.chunks(8) {
        assert_eq!(frame, b"\0\0\0\x08\x02\0\0\0");
    }
}

/// STATEMENT: Pings are sent at half the idle timeout, and a very large timeout does not
/// cause early pings.
///
/// HOW:
/// - Create harnesses with 4000ms and 3_221_225_471ms idle timeouts.
/// - Confirm neither writes anything within 1300ms.
/// - Confirm the 4000ms harness writes a single empty frame within the next 1500ms.
#[ntex::test]
async fn idle_timeout_negotiated_interval() {
    let h = Harness::new(Millis(4_000));
    let long = Harness::new(Millis(3_221_225_471));
    sleep(Millis(1_300)).await;
    assert_eq!(read_frames(&h.client), Vec::<Frame>::new());
    assert_eq!(read_frames(&long.client), Vec::<Frame>::new());

    let bytes = timeout(Millis(1_500), h.client.read())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..], b"\0\0\0\x08\x02\0\0\0");
}

/// STATEMENT: Outgoing frames from every write path count as idle activity, and the
/// ping follows within the idle timeout of the last write.
///
/// HOW:
/// - With 400ms, write a frame every 80ms six times via `ConnectionRef::post_frame`, the
///   inner `post_frame`, or a `Begin` reply.
/// - Confirm each interval holds exactly one non-empty frame.
/// - After the last write, confirm an empty frame arrives within 500ms of it.
#[ntex::test]
async fn idle_timeout_outgoing_activity() {
    for path in 0..3 {
        let h = Harness::new(Millis(400));
        for channel in 0..6 {
            let last_write = std::time::Instant::now();
            match path {
                0 => h.conn.post_frame(AmqpFrame::new(0, peer_flow(None))),
                1 => h.conn.0.post_frame(AmqpFrame::new(0, peer_flow(None))),
                _ => {
                    // The Begin reply uses the fallible encoding path.
                    h.conn
                        .handle_frame(AmqpFrame::new(channel, begin()))
                        .unwrap();
                }
            }
            sleep(Millis(80)).await;
            let frames = read_frames(&h.client);
            assert_eq!(frames.len(), 1, "write path {path}: {frames:?}");
            assert!(!matches!(frames[0], Frame::Empty));

            if channel == 5 {
                let bytes = timeout(Millis(400), h.client.read())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(&bytes[..], b"\0\0\0\x08\x02\0\0\0");
                assert!(last_write.elapsed() < std::time::Duration::from_millis(500));
            }
        }
    }
}

/// STATEMENT: Incoming frames alone do not suppress outgoing idle pings.
///
/// HOW:
/// - With 400ms, dispatch an incoming empty frame every 80ms six times.
/// - Confirm every written frame is empty and at least one was written.
#[ntex::test]
async fn idle_timeout_incoming_only() {
    let h = Harness::new(Millis(400));
    let mut pings = 0;
    for _ in 0..6 {
        h.frame(Frame::Empty).await.unwrap();
        sleep(Millis(80)).await;
        for frame in read_frames(&h.client) {
            assert!(matches!(frame, Frame::Empty));
            pings += 1;
        }
    }
    assert!(pings > 0);
}

/// STATEMENT: An overdue idle timer callback does not treat an old write as fresh
/// activity.
///
/// HOW:
/// - With 400ms, post a `Flow` and read it back.
/// - Block the thread for 340ms or 400ms.
/// - Confirm an empty frame is written within the next 100ms.
#[ntex::test]
async fn idle_timeout_delayed_activity_check() {
    for delay in [340, 400] {
        let h = Harness::new(Millis(400));
        h.conn.post_frame(AmqpFrame::new(0, peer_flow(None)));
        sleep(Millis(20)).await;
        assert!(matches!(&read_frames(&h.client)[..], [Frame::Flow(_)]));

        // An overdue callback must not treat an old write as fresh activity.
        std::thread::sleep(std::time::Duration::from_millis(delay));
        let bytes = timeout(Millis(100), h.client.read())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes[..], b"\0\0\0\x08\x02\0\0\0");
    }
}

/// STATEMENT: Writes made before the dispatcher is created do not delay the first idle
/// ping.
///
/// HOW:
/// - Post a `Flow` on a connection and read it back.
/// - Create a dispatcher with a 400ms idle timeout.
/// - Confirm an empty frame is written within 300ms.
#[ntex::test]
async fn idle_timeout_activity_before_start() {
    let (_io, conn, client) = connection();
    conn.post_frame(AmqpFrame::new(0, peer_flow(None)));
    sleep(Millis(20)).await;
    assert!(matches!(&read_frames(&client)[..], [Frame::Flow(_)]));

    let flags = Flags::new();
    let _dispatcher = Dispatcher::new(
        conn,
        Pipeline::new((), Svc::<Message>::new(&flags)),
        Pipeline::new((), Svc::<ControlFrame>::new(&flags)),
        Millis(400),
    );
    let bytes = timeout(Millis(300), client.read()).await.unwrap().unwrap();
    assert_eq!(&bytes[..], b"\0\0\0\x08\x02\0\0\0");
}

/// STATEMENT: A control frame queued while publish readiness is pending is handled by
/// the control service, and the publish call waits until the publish service is ready.
///
/// HOW:
/// - Attach a receiver link, make publish readiness pending and poll `ready` once.
/// - Close the link; confirm `ready` stays pending, `LocalDetachReceiver` is logged and
///   publish logs nothing.
/// - Make publish ready; confirm `ready` completes and publish logs `Detached`.
#[ntex::test]
async fn queued_control_frames_invalidate_pending_readiness() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.frame(named_attach(Role::Sender, "r", "r", 0))
        .await
        .unwrap();
    h.settle().await;
    let _ = (h.control.take(), h.publish.take());

    h.publish.ready_pending.set(true);
    h.publish.changed.notify();
    let mut readiness = Box::pin(h.disp.ready());
    assert!(poll_fn(|cx| Poll::Ready(readiness.as_mut().poll(cx).is_pending())).await);

    let link = session_ref(&h.conn)
        .get_receiver_link_by_remote_handle(0)
        .unwrap();
    let _ = link.close();

    // Keep readiness pending while the local detach reaches the control service.
    assert!(timeout(Millis(50), readiness.as_mut()).await.is_err());
    assert_eq!(h.control.take(), ["LocalDetachReceiver"]);
    assert_eq!(h.publish.take(), Vec::<String>::new());

    h.publish.ready_pending.set(false);
    h.publish.changed.notify();
    timeout(Millis(500), readiness).await.unwrap().unwrap();
    sleep(Millis(25)).await;
    assert_eq!(h.publish.take(), ["Detached"]);
}

/// STATEMENT: A publish or control service readiness error fails the dispatcher and
/// closes the connection with that error.
///
/// HOW:
/// - Make the publish or control service `ready` fail and notify the change.
/// - Confirm `ready` fails with `AmqpDispatcherError::Service`.
/// - Confirm a `Close` carrying the readiness error description is written and the
///   connection is not opened.
#[ntex::test]
async fn service_readiness_errors() {
    for publish in [true, false] {
        let h = Harness::new(Millis::ZERO);
        h.disp.ready().await.unwrap();
        let flags = if publish { &h.publish } else { &h.control };
        flags.ready_err.set(true);
        flags.changed.notify();

        let err = h.disp.ready().await.err().unwrap();
        assert!(matches!(err, AmqpDispatcherError::Service));

        // connection is closed with the readiness error
        sleep(Millis(25)).await;
        let [Frame::Close(close)] = &read_frames(&h.client)[..] else {
            panic!("expected Close frame, publish: {publish}");
        };
        assert_eq!(
            close
                .error
                .as_ref()
                .and_then(|e| e.description())
                .map(ntex_bytes::ByteString::as_str),
            Some(if publish {
                "publish is not ready"
            } else {
                "control is not ready"
            })
        );
        assert!(!h.conn.is_opened());
    }
}

/// STATEMENT: Queued control frames are released when the dispatcher shuts down or is
/// dropped, so the connection and links are freed.
///
/// HOW:
/// - Queue a local receiver detach and confirm one pending control frame.
/// - Optionally `shutdown`: confirm an empty queue, `Closed` for control and one publish
///   shutdown.
/// - Drop the harness; confirm an empty queue and dead link and connection weak refs.
#[ntex::test]
async fn pending_controls_release_on_dispatcher_stop() {
    for shutdown in [false, true] {
        let h = Harness::new(Millis::ZERO);
        h.frame(begin()).await.unwrap();
        h.frame(named_attach(Role::Sender, "r", "r", 0))
            .await
            .unwrap();
        h.settle().await;
        let _ = (h.control.take(), h.publish.take());

        let session = session_ref(&h.conn);
        let receiver = session.get_receiver_link_by_remote_handle(0).unwrap();
        let weak = Rc::downgrade(&h.conn.0);
        let receiver_weak = Rc::downgrade(&receiver.inner);
        let queue = h.conn.get_control_queue().clone();
        drop(session.detach_receiver_link(receiver.handle(), None));
        assert_eq!(queue.pending.borrow().len(), 1);
        drop(receiver);
        drop(session);

        if shutdown {
            h.disp.shutdown().await;
            assert!(queue.pending.borrow().is_empty());
            assert_eq!(h.control.take(), ["Closed"]);
            assert_eq!(h.publish.shutdown.get(), 1);
        }
        drop(h);
        sleep(Millis(10)).await;
        assert!(queue.pending.borrow().is_empty());
        assert!(receiver_weak.upgrade().is_none());
        assert!(weak.upgrade().is_none());
    }
}

/// STATEMENT: Dispatcher shutdown reports `Closed`, shuts down both services and marks
/// the connection disconnected.
///
/// HOW:
/// - Dispatch `Begin`, then call `shutdown`.
/// - Confirm control logs `Closed` and each service's `shutdown` ran once.
/// - Confirm the connection error is `"Disconnected"`.
#[ntex::test]
async fn shutdown_notifies_services() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.settle().await;

    h.disp.shutdown().await;
    assert_eq!(h.control.take(), ["Closed"]);
    assert_eq!(h.publish.shutdown.get(), 1);
    assert_eq!(h.control.shutdown.get(), 1);
    assert_eq!(h.conn.get_error().unwrap().to_string(), "Disconnected");
}

/// STATEMENT: A control service error for a sender link `Flow` or remote detach closes
/// the link.
///
/// HOW:
/// - Attach sender link `"s"`, make control `call` fail, and dispatch a `Flow` or peer
///   `Detach`.
/// - Confirm `Flow Some(5)` and `LocalDetachSender`, or `RemoteDetachSender`, are logged.
/// - Confirm the link is closed and one closing `Detach 0` is written, described as
///   `"call failed"` for `Flow` and without error for the remote detach.
#[ntex::test]
async fn control_error_closes_established_links() {
    // control service errors for `Flow` and `RemoteDetachSender` close the link
    for detach in [false, true] {
        let h = Harness::new(Millis::ZERO);
        h.frame(begin()).await.unwrap();
        h.frame(named_attach(Role::Receiver, "s", "s", 2))
            .await
            .unwrap();
        h.settle().await;
        let snd = session_ref(&h.conn).get_sender_link("s").unwrap();
        let _ = (h.control.take(), read_frames(&h.client));

        h.control.call_err.set(true);
        let frame = if detach {
            peer_detach(2)
        } else {
            link_flow_frame(2, 5)
        };
        h.frame(frame).await.unwrap();
        h.settle().await;

        let ctx = format!("detach: {detach}");
        assert_eq!(
            h.control.take(),
            if detach {
                vec!["RemoteDetachSender"]
            } else {
                vec!["Flow Some(5)", "LocalDetachSender"]
            },
            "{ctx}"
        );
        // remotely detached link is already closed, detach response carries no error
        assert!(snd.is_closed(), "{ctx}");
        assert_eq!(
            detaches(&h.client)
                .iter()
                .map(|d| (
                    d.0,
                    d.1,
                    d.2.as_ref()
                        .and_then(|e| e.description())
                        .map(|s| s.as_str().to_string())
                ))
                .collect::<Vec<_>>(),
            vec![(0, true, (!detach).then(|| "call failed".to_string()))],
            "{ctx}"
        );
    }
}

/// STATEMENT: A control service error while handling a protocol error is ignored and
/// the protocol error stops the dispatcher.
///
/// HOW:
/// - Make control `call` fail and dispatch `Stop(Reason::KeepAlive)`.
/// - Confirm `ready` fails with `Protocol(KeepAliveTimeout)` and the connection error is
///   `KeepAliveTimeout`.
/// - Confirm a `Close` without error is written.
#[ntex::test]
async fn control_error_on_protocol_error_stops_dispatcher() {
    let h = Harness::new(Millis::ZERO);
    h.control.call_err.set(true);
    h.disp
        .call(DispatchItem::Stop(Reason::KeepAlive))
        .await
        .unwrap();

    // control service error is ignored, the protocol error is propagated
    sleep(Millis(25)).await;
    let err = h.disp.ready().await.err().unwrap();
    assert!(
        matches!(
            err,
            AmqpDispatcherError::Protocol(AmqpProtocolError::KeepAliveTimeout)
        ),
        "{err:?}"
    );
    assert!(matches!(
        h.conn.get_error(),
        Some(AmqpProtocolError::KeepAliveTimeout)
    ));
    // connection is closed
    sleep(Millis(25)).await;
    let [Frame::Close(close)] = &read_frames(&h.client)[..] else {
        panic!("expected Close frame");
    };
    assert!(close.error.is_none());
}

/// STATEMENT: A control service error on remote close or disconnect still records the
/// matching connection error.
///
/// HOW:
/// - Make control `call` fail and dispatch a remote `Close` or `Stop(Reason::Io(None))`.
/// - Confirm the connection error is `Closed(None)` or `Disconnected`.
#[ntex::test]
async fn control_error_on_disconnect_marks_connection() {
    for closed in [false, true] {
        let h = Harness::new(Millis::ZERO);
        h.control.call_err.set(true);
        let item = if closed {
            DispatchItem::Item(AmqpFrame::new(0, Close { error: None }.into()))
        } else {
            DispatchItem::Stop(Reason::Io(None))
        };
        h.disp.call(item).await.unwrap();
        h.settle().await;

        // remote close is recorded before the control service fails, a transport
        // error is reported as a disconnect
        assert_eq!(
            format!("{:?}", h.conn.get_error()),
            if closed {
                "Some(Closed(None))"
            } else {
                "Some(Disconnected)"
            },
            "closed: {closed}"
        );
    }
}

/// STATEMENT: A locally ended session reports its receiver links to the control and
/// publish services at once.
///
/// HOW:
/// - Attach a receiver link and start `end` on the session.
/// - Dispatch the peer `End` and confirm `end` succeeds.
/// - Confirm `LocalSessionEnded 1` for control and `DetachedAll 1` for publish.
#[ntex::test]
async fn local_session_end_detaches_receivers() {
    let h = Harness::new(Millis::ZERO);
    h.frame(begin()).await.unwrap();
    h.frame(named_attach(Role::Sender, "r", "r", 0))
        .await
        .unwrap();
    h.settle().await;
    let _ = (h.control.take(), h.publish.take());

    // locally ended session reports all its receiver links at once
    let s = session_ref(&h.conn);
    let fut = ntex::rt::spawn(async move { s.end().await });
    sleep(Millis(10)).await;
    h.frame(End { error: None }.into()).await.unwrap();
    timeout(Millis(1000), fut).await.unwrap().unwrap().unwrap();
    h.settle().await;

    assert_eq!(h.control.take(), ["LocalSessionEnded 1"]);
    assert_eq!(h.publish.take(), ["DetachedAll 1"]);
}
