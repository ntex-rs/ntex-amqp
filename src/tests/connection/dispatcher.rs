//! Dispatcher control-frame handling, service errors, idle ping and shutdown.
use std::cell::{Cell as StdCell, RefCell};
use std::rc::Rc;
use std::{io, marker::PhantomData};

use ntex_dispatcher::{DispatchItem, Reason};
use ntex_service::{Ctx, Service, pipeline::Pipeline};

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
    shutdown: Rc<StdCell<usize>>,
}

impl Flags {
    fn new() -> Self {
        Flags {
            log: Rc::new(RefCell::new(Vec::new())),
            call_err: Rc::new(StdCell::new(false)),
            ready_err: Rc::new(StdCell::new(false)),
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

    async fn call(&self, req: Message, _: Ctx<'_, Self, ()>) -> Result<(), ProtoError> {
        self.record(match req {
            Message::Attached(..) => "Attached".into(),
            Message::Detached(_) => "Detached".into(),
            Message::DetachedAll(links) => format!("DetachedAll {}", links.len()),
            Message::Transfer(_) => "Transfer".into(),
        })
    }

    async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), ProtoError> {
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
        assert!(h.publish.take().is_empty());
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
    assert!(h.control.take().is_empty());
}

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
    assert!(h.publish.take().is_empty());
    let names = frame_names(&h.client);
    assert_eq!(names[0], "Begin");
    assert_eq!(detaches(&h.client).len(), 0);
    assert!(names.iter().any(|n| n == "Detach 0"));
}

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
    let snd = session_ref(&h.conn).get_sender_link("s").cloned().unwrap();
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
    assert!(h.publish.take().is_empty());
}

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
        .cloned()
        .unwrap();
    let fut = ntex::rt::spawn(async move { link.close().await });
    sleep(Millis(10)).await;
    h.frame(peer_detach(0)).await.unwrap();
    fut.await.unwrap().unwrap();
    h.settle().await;

    assert_eq!(h.control.take(), ["LocalDetachReceiver"]);
    assert_eq!(h.publish.take(), ["Detached"]);
}

#[ntex::test]
async fn idle_timeout_ping() {
    // ping interval is half of the idle timeout, capped at a second
    let h = Harness::new(Millis(100));
    h.disp.ready().await.unwrap();
    assert!(read_frames(&h.client).is_empty());

    sleep(Millis(80)).await;
    h.disp.ready().await.unwrap();
    sleep(Millis(10)).await;
    assert_eq!(frame_names(&h.client), ["Empty"]);

    // and keeps pinging
    sleep(Millis(80)).await;
    h.disp.ready().await.unwrap();
    sleep(Millis(10)).await;
    assert_eq!(frame_names(&h.client), ["Empty"]);

    // zero timeout disables the ping
    let h = Harness::new(Millis::ZERO);
    h.disp.ready().await.unwrap();
    sleep(Millis(80)).await;
    h.disp.ready().await.unwrap();
    assert!(read_frames(&h.client).is_empty());
}

#[ntex::test]
async fn service_readiness_errors() {
    for publish in [true, false] {
        let h = Harness::new(Millis::ZERO);
        let flags = if publish { &h.publish } else { &h.control };
        flags.ready_err.set(true);

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
        let snd = session_ref(&h.conn).get_sender_link("s").cloned().unwrap();
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
