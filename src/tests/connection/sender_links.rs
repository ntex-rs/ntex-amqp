use super::*;

#[ntex::test]
async fn local_sender_initial_delivery_count() {
    // (attach initial delivery-count, sent value, flow delivery-count)
    let cases = [
        (None, 0, Some(0)),
        (Some(Some(5)), 5, Some(5)),
        (Some(None), 0, Some(0)),
        (Some(Some(5)), 5, None),
    ];
    for (initial, sent, flow_count) in cases {
        let ctx = format!("initial: {initial:?} flow: {flow_count:?}");
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let fut = ntex::rt::spawn(async move {
            let mut builder = s.build_sender_link("s", "s");
            if let Some(initial) = initial {
                builder = builder.with_frame(|f| f.0.initial_delivery_count = initial);
            }
            builder.attach().await
        });
        ntex::time::sleep(ntex::time::Millis(10)).await;

        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut counts = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Attach(att) = frame.into_parts().1 {
                counts.push(att.initial_delivery_count());
            }
        }
        assert_eq!(counts, [Some(sent)], "{ctx}");

        // receiver initial delivery-count is ignored
        let Frame::Attach(mut attach) = named_attach(Role::Receiver, "s", "s", 0) else {
            panic!()
        };
        attach.0.initial_delivery_count = Some(100);
        let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
            panic!("{ctx}")
        };
        let link = ntex::time::timeout(ntex::time::Millis(1000), fut)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
            panic!()
        };
        flow.0.delivery_count = flow_count;
        handle_frame(&conn, flow.into()).unwrap();
        assert_eq!(link.credit(), 10, "{ctx}");

        // delivery-count is advanced by transfer
        ntex::time::timeout(
            ntex::time::Millis(1000),
            link.transfer(Bytes::from_static(b"1")).settled().send(),
        )
        .await
        .unwrap()
        .unwrap();
        let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
            panic!()
        };
        flow.0.delivery_count = Some(sent + 1);
        handle_frame(&conn, flow.into()).unwrap();
        assert_eq!(link.credit(), 10, "{ctx}");
    }
}

#[ntex::test]
async fn sender_max_message_size() {
    for local in [false, true] {
        // peer max-message-size 0 means no limit
        let (link, _conn) = sender(local, Some(0)).await;
        assert_eq!(link.max_message_size(), None);

        let (link, _conn) = sender(local, Some(10)).await;
        assert_eq!(link.max_message_size(), Some(10));
        assert!(matches!(
            link.transfer(Bytes::from(vec![0; 11])).send().await,
            Err(AmqpProtocolError::BodyTooLarge)
        ));
        link.set_max_message_size(0);
        assert_eq!(link.max_message_size(), None);

        let (link, _conn) = sender(local, Some(u64::MAX)).await;
        assert_eq!(link.max_message_size(), Some(u32::MAX));
    }
}

#[ntex::test]
async fn sender_link_drain() {
    use ntex::time::{Millis, sleep, timeout};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);
    let flow = |credit: u32, delivery_count: u32, drain: bool| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.handle = Some(4);
        flow.0.delivery_count = Some(delivery_count);
        flow.0.drain = drain;
        handle_frame(&conn, flow.into()).unwrap();
    };
    let frames = || {
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            frames.push(match frame.into_parts().1 {
                Frame::Flow(flow) => format!(
                    "Flow {:?} {:?} {:?} {}",
                    flow.handle(),
                    flow.delivery_count(),
                    flow.link_credit(),
                    flow.drain()
                ),
                frame => frame.name().to_string(),
            });
        }
        frames
    };

    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
    else {
        panic!()
    };
    let snd =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, snd.inner.clone());
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Begin", "Attach"]);

    // idle link, credit is drained immediately
    flow(5, 0, true);
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Flow Some(0) Some(5) Some(0) true"]);

    // queued transfers are sent before credit is drained
    let t1 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"1")).settled().send());
    let t2 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"2")).settled().send());
    sleep(Millis(50)).await;
    flow(3, 5, true);
    assert!(timeout(Millis(500), t1).await.unwrap().unwrap().is_ok());
    assert!(timeout(Millis(500), t2).await.unwrap().unwrap().is_ok());
    sleep(Millis(50)).await;
    assert_eq!(
        frames(),
        ["Transfer", "Transfer", "Flow Some(0) Some(8) Some(0) true"]
    );

    // woken transfer is dropped before it resumes
    let mut t3 = Box::pin(snd.transfer(Bytes::from_static(b"3")).settled().send());
    assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
    flow(2, 8, true);
    sleep(Millis(50)).await;
    assert!(frames().is_empty());
    assert_eq!(snd.credit(), 2);
    drop(t3);
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Flow Some(0) Some(10) Some(0) true"]);

    // no drain
    flow(2, 10, false);
    assert_eq!(snd.credit(), 2);
    sleep(Millis(50)).await;
    assert!(frames().is_empty());

    // drain flow received before link confirmation
    let Ok(Action::AttachSender(lnk, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "d", "d", 6))
    else {
        panic!()
    };
    let Frame::Flow(mut pending) = peer_flow(Some(3)) else {
        panic!()
    };
    pending.0.handle = Some(6);
    pending.0.drain = true;
    handle_frame(&conn, pending.into()).unwrap();
    let lnk =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, lnk.inner.clone());
    assert_eq!(lnk.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Attach", "Flow Some(1) Some(3) Some(0) true"]);
}

#[ntex::test]
async fn sender_link_credit_on_window_wait() {
    use ntex::time::{Millis, sleep, timeout};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);
    let flow = |delivery_count: u32, credit: u32, drain: bool, window: u32| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.handle = Some(4);
        flow.0.delivery_count = Some(delivery_count);
        flow.0.drain = drain;
        flow.0.incoming_window = window;
        handle_frame(&conn, flow.into()).unwrap();
    };
    let frames = || {
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            frames.push(match frame.into_parts().1 {
                Frame::Flow(flow) => format!(
                    "Flow {:?} {:?} {:?} {}",
                    flow.handle(),
                    flow.delivery_count(),
                    flow.link_credit(),
                    flow.drain()
                ),
                frame => frame.name().to_string(),
            });
        }
        frames
    };

    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
    else {
        panic!()
    };
    let snd =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, snd.inner.clone());
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Begin", "Attach"]);

    // transfer waits for session window, link credit is kept on cancel
    flow(0, 2, false, 0);
    let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
    assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
    assert_eq!(snd.credit(), 2);
    drop(t1);
    assert_eq!(snd.credit(), 2);

    // transfer blocked by session window does not delay drain
    let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).settled().send());
    assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
    flow(0, 2, true, 0);
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Flow Some(0) Some(2) Some(0) true"]);
    drop(t2);
    sleep(Millis(50)).await;
    assert!(frames().is_empty());

    // woken transfer delays drain until it gets blocked by session window,
    // then it waits for new credit
    let t3 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"3")).settled().send());
    sleep(Millis(50)).await;
    flow(2, 2, true, 0);
    assert_eq!(snd.credit(), 2);
    sleep(Millis(50)).await;
    assert_eq!(snd.credit(), 0);
    flow(4, 0, false, 10);
    sleep(Millis(50)).await;
    assert!(!t3.is_finished());
    flow(4, 1, false, 10);
    assert!(timeout(Millis(500), t3).await.unwrap().unwrap().is_ok());
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Flow Some(0) Some(4) Some(0) true", "Transfer"]);

    // resumed transfer does not delay later drain
    flow(5, 2, true, 10);
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Flow Some(0) Some(7) Some(0) true"]);

    // link is detached while transfer waits for session window
    let session_flow = |handle: Option<u32>, window: u32| {
        let Frame::Flow(mut flow) = peer_flow(Some(2)) else {
            panic!()
        };
        flow.0.handle = handle;
        flow.0.delivery_count = Some(7);
        flow.0.next_incoming_id = Some(2);
        flow.0.incoming_window = window;
        handle_frame(&conn, flow.into()).unwrap();
    };
    session_flow(Some(4), 0);
    let t4 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"4")).settled().send());
    sleep(Millis(50)).await;
    let _ = handle_frame(&conn, peer_detach(4));
    assert!(timeout(Millis(500), t4).await.unwrap().unwrap().is_err());
    session_flow(None, 10);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Detach"]);
}

#[ntex::test]
async fn sender_link_flow_wakes_by_credit() {
    use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
    use std::task::{Wake, Waker};

    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    type Fut = Pin<Box<dyn Future<Output = Result<crate::Delivery, AmqpProtocolError>>>>;
    struct Tr(Fut, Arc<Counter>);
    impl Tr {
        fn poll(&mut self) -> Poll<Result<crate::Delivery, AmqpProtocolError>> {
            let waker = Waker::from(self.1.clone());
            self.0.as_mut().poll(&mut Context::from_waker(&waker))
        }
        fn woken(&self) -> usize {
            self.1.0.load(Ordering::SeqCst)
        }
    }

    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);
    // link flow, `sent` transfers are received by remote
    let flow = |sent: u32, credit: u32, window: u32| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.handle = Some(4);
        flow.0.delivery_count = Some(sent);
        flow.0.next_incoming_id = Some(1 + sent);
        flow.0.incoming_window = window;
        handle_frame(&conn, flow.into()).unwrap();
    };
    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
    else {
        panic!()
    };
    let snd =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, snd.inner.clone());
    let transfer = || {
        let mut tr = Tr(
            Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send()),
            Arc::new(Counter(AtomicUsize::new(0))),
        );
        assert!(tr.poll().is_pending());
        tr
    };

    // waiters are woken up to available credit
    let (mut t1, mut t2, mut t3) = (transfer(), transfer(), transfer());
    flow(0, 1, 10);
    assert_eq!((t1.woken(), t2.woken(), t3.woken()), (1, 0, 0));
    assert!(t1.poll().is_ready());
    flow(1, 2, 10);
    assert_eq!((t2.woken(), t3.woken()), (1, 1));
    assert!(t2.poll().is_ready());
    assert!(t3.poll().is_ready());

    // credit of dropped woken waiter is passed to next waiter
    let (t4, t5) = (transfer(), transfer());
    flow(3, 1, 10);
    assert_eq!((t4.woken(), t5.woken()), (1, 0));
    drop(t4);
    assert_eq!(t5.woken(), 1);
    drop(t5);
    assert_eq!(snd.credit(), 1);

    // credit is claimed by transfer waiting for session window
    flow(3, 1, 0);
    let mut t6 = transfer();
    flow(3, 0, 0);
    let mut t7 = transfer();
    flow(3, 1, 0);
    assert_eq!((t6.woken(), t7.woken()), (0, 0));

    // woken transfer is not queued behind credit waiters
    flow(3, 1, 10);
    assert_eq!((t6.woken(), t7.woken()), (1, 0));
    assert!(t6.poll().is_ready());
    flow(4, 1, 10);
    assert_eq!(t7.woken(), 1);
    assert!(t7.poll().is_ready());

    // credit of dropped window waiter is passed to next waiter
    flow(5, 0, 0);
    let (mut t8, t9) = (transfer(), transfer());
    flow(5, 1, 0);
    assert!(t8.poll().is_pending());
    assert_eq!(t9.woken(), 0);
    drop(t8);
    assert_eq!(t9.woken(), 1);

    // waiters are not woken while delivery is partially sent
    let (_io, conn, _client, snd) = small_frames_sender();
    let transfer = |body: Bytes| {
        let mut tr = Tr(
            Box::pin(snd.transfer(body).send()),
            Arc::new(Counter(AtomicUsize::new(0))),
        );
        assert!(tr.poll().is_pending());
        tr
    };
    let mut t1 = transfer(Bytes::from(vec![b'a'; 1200]));
    let t2 = transfer(Bytes::from_static(b"2"));
    let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
        panic!()
    };
    flow.0.handle = Some(4);
    flow.0.incoming_window = 2;
    handle_frame(&conn, flow.into()).unwrap();
    assert_eq!((t1.woken(), t2.woken()), (0, 0));
    session_window(&conn, 3, 1);
    assert!(t1.poll().is_ready());
    assert_eq!(t2.woken(), 1);
}

#[ntex::test]
async fn sender_link_close_fails_waiting_transfers() {
    use ntex::time::{Millis, sleep, timeout};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);
    let flow = |handle: u32, credit: u32, window: u32| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.handle = Some(handle);
        flow.0.incoming_window = window;
        handle_frame(&conn, flow.into()).unwrap();
    };
    let attach = |name: &str, handle: u32| {
        let Ok(Action::AttachSender(snd, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, name, name, handle))
        else {
            panic!()
        };
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, snd.inner.clone())
    };
    let send = |snd: &SenderLink, body: &'static [u8]| {
        ntex::rt::spawn(snd.transfer(Bytes::from_static(body)).settled().send())
    };

    // transfer waits for link credit
    let a = attach("a", 4);
    let t1 = send(&a, b"1");
    // transfer waits for session window
    let b = attach("b", 5);
    flow(5, 1, 0);
    let t2 = send(&b, b"2");
    sleep(Millis(50)).await;
    frame_names(&client);

    let (a2, b2) = (a.clone(), b.clone());
    let _c1 = ntex::rt::spawn(async move { a2.close().await });
    let _c2 = ntex::rt::spawn(async move { b2.close().await });
    assert!(timeout(Millis(500), t1).await.unwrap().unwrap().is_err());
    assert!(timeout(Millis(500), t2).await.unwrap().unwrap().is_err());

    // link is closed after transfer is woken up
    let c = attach("c", 6);
    let t3 = send(&c, b"3");
    sleep(Millis(50)).await;
    assert_eq!(frame_names(&client), ["Detach 0", "Detach 1", "Attach c 2"]);
    flow(6, 1, 10);
    let mut close = Box::pin(c.close());
    assert!(poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx).is_pending())).await);
    assert!(timeout(Millis(500), t3).await.unwrap().unwrap().is_err());

    // no transfers after detach
    sleep(Millis(50)).await;
    assert_eq!(frame_names(&client), ["Detach 2"]);
}

#[ntex::test]
async fn sender_link_detach_fails_waiting_transfers() {
    use ntex::time::{Millis, sleep, timeout};

    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);
    let flow = |handle: u32, credit: u32, window: u32| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.handle = Some(handle);
        flow.0.incoming_window = window;
        handle_frame(&conn, flow.into()).unwrap();
    };
    let attach = |name: &str, handle: u32| {
        let Ok(Action::AttachSender(snd, attach, response)) =
            handle_frame(&conn, named_attach(Role::Receiver, name, name, handle))
        else {
            panic!()
        };
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, snd.inner.clone())
    };
    let send = |snd: &SenderLink, body: &'static [u8]| {
        ntex::rt::spawn(snd.transfer(Bytes::from_static(body)).settled().send())
    };

    // transfer waits for link credit
    let a = attach("a", 4);
    let t1 = send(&a, b"1");
    // transfer waits for session window
    let b = attach("b", 5);
    flow(5, 1, 0);
    let t2 = send(&b, b"2");
    sleep(Millis(50)).await;
    frame_names(&client);

    let _d1 = session.detach_sender_link(a.inner.get_ref().id(), None);
    let _d2 = session.detach_sender_link(b.inner.get_ref().id(), None);
    let res = timeout(Millis(500), t1).await.unwrap().unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));
    let res = timeout(Millis(500), t2).await.unwrap().unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));
    assert!(a.transfer(Bytes::from_static(b"3")).send().await.is_err());

    // no transfers after detach
    flow(5, 1, 10);
    sleep(Millis(50)).await;
    assert_eq!(frame_names(&client), ["Detach 0", "Detach 1"]);
}

#[ntex::test]
async fn sender_link_detach_fails_partial_delivery() {
    use ntex::time::{Millis, sleep, timeout};

    let (_io, conn, client, snd) = small_frames_sender();
    let session = session(&conn);
    sleep(Millis(50)).await;
    transfer_frames(&client);

    // delivery waits for session window, next transfer waits for delivery
    let body = Bytes::from(vec![0u8; 2048]);
    let t1 = ntex::rt::spawn(snd.transfer(body).settled().send());
    let t2 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"2")).settled().send());
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        [
            "Transfer Some(0) more:true aborted:false",
            "Transfer None more:true aborted:false"
        ]
    );

    let _d = session.detach_sender_link(snd.inner.get_ref().id(), None);
    let res = timeout(Millis(500), t1).await.unwrap().unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));
    let res = timeout(Millis(500), t2).await.unwrap().unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));

    // no transfers after detach
    session_window(&conn, 1, 10);
    sleep(Millis(50)).await;
    assert_eq!(transfer_frames(&client), ["Detach"]);
}

#[ntex::test]
async fn sender_link_detach_fails_unsettled() {
    use ntex::time::{Millis, sleep, timeout};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, _client, snd) = small_frames_sender();
    let session = session(&conn);
    let delivery = snd.transfer(Bytes::from_static(b"1")).send().await.unwrap();
    let mut wait = Box::pin(delivery.wait());
    assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);

    // disposition could be received before detach confirmation
    let _d = session.detach_sender_link(snd.inner.get_ref().id(), None);
    sleep(Millis(50)).await;
    assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);

    handle_frame(&conn, peer_detach(4)).unwrap();
    let res = timeout(Millis(500), wait).await.unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::LinkDetached(None))));
}

#[ntex::test]
async fn sender_link_stale_flow() {
    for initial in [0, u32::MAX - 1] {
        sender_link_stale_flow_with(initial).await;
    }
}

async fn sender_link_stale_flow_with(initial: u32) {
    use ntex::time::{Millis, sleep, timeout};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);
    let flow = |delivery_count: Option<u32>, credit: u32| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.handle = Some(4);
        flow.0.delivery_count = delivery_count.map(|dc| initial.wrapping_add(dc));
        handle_frame(&conn, flow.into()).unwrap();
    };

    let Frame::Attach(mut attach) = named_attach(Role::Receiver, "s", "s", 4) else {
        panic!()
    };
    attach.0.initial_delivery_count = Some(initial);
    let Ok(Action::AttachSender(snd, attach, response)) = handle_frame(&conn, attach.into()) else {
        panic!()
    };
    let snd =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, snd.inner.clone());

    // receiver has not received attach yet, initial delivery count is used
    flow(None, 10);
    assert_eq!(snd.credit(), 10);
    for _ in 0..3 {
        let t = snd.transfer(Bytes::from_static(b"1")).settled().send();
        assert!(timeout(Millis(500), t).await.unwrap().is_ok());
    }
    assert_eq!(snd.credit(), 7);
    flow(None, 10);
    assert_eq!(snd.credit(), 7);

    // flow issued before receiver got sent transfers
    flow(Some(0), 2);
    assert_eq!(snd.credit(), 0);
    let mut t = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
    assert!(poll_fn(|cx| Poll::Ready(t.as_mut().poll(cx).is_pending())).await);
    flow(Some(0), 5);
    assert!(timeout(Millis(500), t).await.unwrap().is_ok());
    assert_eq!(snd.credit(), 1);

    // receiver delivery count is ahead, credit is used as is
    flow(Some(10), 4);
    assert_eq!(snd.credit(), 4);
    sleep(Millis(10)).await;
}

#[ntex::test]
async fn sender_drain_with_partial_delivery() {
    use ntex::time::{Millis, sleep, timeout};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client, snd) = small_frames_sender();
    let flow = |delivery_count: u32, credit: u32, drain: bool, window: u32| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.handle = Some(4);
        flow.0.delivery_count = Some(delivery_count);
        flow.0.drain = drain;
        flow.0.next_incoming_id = Some(2);
        flow.0.incoming_window = window;
        handle_frame(&conn, flow.into()).unwrap();
    };
    let link_flows = || {
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            frames.push(match frame.into_parts().1 {
                Frame::Flow(flow) => format!(
                    "Flow {:?} {:?} {}",
                    flow.delivery_count(),
                    flow.link_credit(),
                    flow.drain()
                ),
                frame => frame.name().to_string(),
            });
        }
        frames
    };
    sleep(Millis(50)).await;
    link_flows();

    // partial delivery waits for session window, next transfer is queued
    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).settled().send());
    assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
    let t2 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"2")).settled().send());
    sleep(Millis(50)).await;
    assert_eq!(snd.credit(), 9);
    assert_eq!(link_flows(), ["Transfer", "Transfer"]);

    // drain is not delayed by transfers blocked by session window
    flow(0, 10, true, 0);
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(link_flows(), ["Flow Some(10) Some(0) true"]);

    // queued transfer waits for new credit
    session_window(&conn, 2, 10);
    assert!(timeout(Millis(500), t1).await.unwrap().is_ok());
    sleep(Millis(50)).await;
    assert!(!t2.is_finished());
    assert_eq!(link_flows(), ["Transfer"]);
    flow(10, 1, false, 10);
    assert!(timeout(Millis(500), t2).await.unwrap().unwrap().is_ok());
    assert_eq!(snd.credit(), 0);
}

#[ntex::test]
async fn sender_multi_frame_delivery_window() {
    use ntex::time::{Millis, sleep};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client, snd) = small_frames_sender();
    sleep(Millis(50)).await;
    transfer_frames(&client);

    // each transfer frame consumes session window
    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
    let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).send());
    assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
    assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
    assert_eq!(snd.credit(), 9);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        [
            "Transfer Some(0) more:true aborted:false",
            "Transfer None more:true aborted:false"
        ]
    );

    // frames of the delivery are not interleaved with next delivery
    session_window(&conn, 3, 1);
    assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
    sleep(Millis(50)).await;
    assert!(transfer_frames(&client).is_empty());
    let Poll::Ready(Ok(d1)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d1.id(), 0);
    assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer None more:false aborted:false"]
    );

    // delivery id is not a transfer id
    session_window(&conn, 4, 5);
    let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d2.id(), 1);
    assert_eq!(snd.credit(), 8);

    // next-outgoing-id counts transfer frames
    let Frame::Flow(mut flow) = peer_flow(None) else {
        panic!()
    };
    flow.0.handle = None;
    flow.0.echo = true;
    handle_frame(&conn, flow.into()).unwrap();
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(1) more:false aborted:false", "Flow 5"]
    );
}

#[ntex::test]
async fn sender_cancelled_delivery_aborted() {
    use ntex::time::{Millis, sleep, timeout};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client, snd) = small_frames_sender();
    let body = Bytes::from(vec![b'a'; 1200]);
    let unsettled = || {
        session(&conn)
            .inner
            .get_ref()
            .unsettled_snd_deliveries
            .len()
    };

    // delivery is cancelled while it waits for session window
    let mut t1 = Box::pin(snd.transfer(body.clone()).send());
    assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
    assert_eq!(unsettled(), 1);
    drop(t1);
    assert_eq!(unsettled(), 0);
    sleep(Millis(50)).await;
    assert_eq!(transfer_frames(&client).len(), 4);

    // queued abort is sent by session flow, before next delivery
    session_window(&conn, 3, 5);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(0) more:false aborted:true"]
    );
    let t2 = ntex::rt::spawn(snd.transfer(Bytes::from_static(b"2")).settled().send());
    assert_eq!(
        timeout(Millis(500), t2)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .id(),
        1
    );
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(1) more:false aborted:false"]
    );

    // abort is sent if session window is available
    session_window(&conn, 5, 2);
    let mut t3 = Box::pin(snd.transfer(body).send());
    assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
    session_window(&conn, 7, 5);
    drop(t3);
    assert_eq!(unsettled(), 0);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        [
            "Transfer Some(2) more:true aborted:false",
            "Transfer None more:true aborted:false",
            "Transfer Some(2) more:false aborted:true"
        ]
    );
    assert_eq!(snd.credit(), 7);
}

#[ntex::test]
async fn sender_woken_transfer_keeps_link_credit() {
    use ntex::time::{Millis, sleep};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client, snd) = small_frames_sender();
    link_flow(&conn, 4, (0, 0, false), 1, 10);
    sleep(Millis(50)).await;
    transfer_frames(&client);
    let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).send());
    assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);

    // new transfer does not take credit of woken transfer
    link_flow(&conn, 4, (0, 1, false), 1, 10);
    let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).send());
    assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
    let Poll::Ready(Ok(d1)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d1.id(), 0);
    assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);
    assert_eq!(snd.credit(), 0);

    link_flow(&conn, 4, (1, 1, false), 2, 10);
    let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d2.id(), 1);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        [
            "Transfer Some(0) more:false aborted:false",
            "Transfer Some(1) more:false aborted:false"
        ]
    );
}

#[ntex::test]
async fn sender_abort_does_not_take_claimed_window() {
    use ntex::time::{Millis, sleep};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client, snd) = small_frames_sender();
    let snd2 = add_sender(&conn, "s2", 5, 1);
    sleep(Millis(50)).await;
    transfer_frames(&client);

    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
    let mut t2 = Box::pin(snd2.transfer(Bytes::from_static(b"2")).send());
    assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
    assert!(poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx).is_pending())).await);

    // abort of cancelled delivery is queued behind next waiter
    session_window(&conn, 2, 1);
    drop(t1);
    let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d2.id(), 1);

    // queued abort is sent by session flow, before new transfers
    let mut t3 = Box::pin(snd.transfer(Bytes::from_static(b"3")).send());
    let mut t4 = Box::pin(snd2.transfer(Bytes::from_static(b"4")).send());
    assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
    assert!(poll_fn(|cx| Poll::Ready(t4.as_mut().poll(cx).is_pending())).await);
    session_window(&conn, 3, 1);
    assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
    assert!(poll_fn(|cx| Poll::Ready(t4.as_mut().poll(cx).is_pending())).await);
    session_window(&conn, 4, 2);
    let Poll::Ready(Ok(d4)) = poll_fn(|cx| Poll::Ready(t4.as_mut().poll(cx))).await else {
        panic!()
    };
    let Poll::Ready(Ok(d3)) = poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!((d3.id(), d4.id()), (3, 2));
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        [
            "Transfer Some(0) more:true aborted:false",
            "Transfer Some(1) more:false aborted:false",
            "Transfer Some(0) more:false aborted:true",
            "Transfer Some(2) more:false aborted:false",
            "Transfer Some(3) more:false aborted:false"
        ]
    );
}

#[ntex::test]
async fn sender_queued_abort_keeps_order() {
    use ntex::time::{Millis, sleep};
    use std::{future::poll_fn, task::Poll};

    let (_io, conn, client, snd, mut t2) = queued_abort_sender().await;

    // new delivery on the same link waits behind its abort
    let mut t3 = Box::pin(snd.transfer(Bytes::from_static(b"3")).send());
    assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);

    session_window(&conn, 2, 1);
    let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d2.id(), 1);
    assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(1) more:false aborted:false"]
    );

    session_window(&conn, 3, 1);
    assert!(poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx).is_pending())).await);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(0) more:false aborted:true"]
    );

    session_window(&conn, 4, 1);
    let Poll::Ready(Ok(d3)) = poll_fn(|cx| Poll::Ready(t3.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d3.id(), 2);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(2) more:false aborted:false"]
    );
}

#[ntex::test]
async fn sender_detach_drops_queued_abort() {
    use ntex::time::{Millis, sleep};
    use std::{future::poll_fn, task::Poll};

    for local in [false, true] {
        let (_io, conn, client, snd, mut t2) = queued_abort_sender().await;
        if local {
            let mut fut = Box::pin(snd.close());
            assert!(poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_pending())).await);
        } else {
            handle_frame(&conn, peer_detach(4)).unwrap();
        }

        // window goes to next waiter, abort is not sent
        session_window(&conn, 2, 2);
        let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
            panic!()
        };
        assert_eq!(d2.id(), 1);
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client)
                .into_iter()
                .filter(|f| f.starts_with("Transfer"))
                .collect::<Vec<_>>(),
            ["Transfer Some(1) more:false aborted:false"]
        );
        assert_eq!(session(&conn).inner.get_ref().pending_transfers(), 0);
    }
}

#[ntex::test]
async fn sender_ready_waits_for_session_window() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let snd = add_sender(&conn, "s", 4, 0);
    assert_eq!(snd.credit(), 10);
    let poll = |fut: &mut Pin<Box<dyn Future<Output = bool> + '_>>| {
        fut.as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
    };

    // link credit is available, remote session window is not
    let mut ready: Pin<Box<dyn Future<Output = bool>>> = Box::pin(snd.ready());
    assert!(poll(&mut ready).is_pending());
    session_window(&conn, 1, 1);
    assert_eq!(poll(&mut ready), Poll::Ready(true));
    assert!(snd.ready().await);

    // window claimed by woken transfer is not available
    session_window(&conn, 1, 0);
    let mut tr = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
    assert!(
        tr.as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    let mut ready: Pin<Box<dyn Future<Output = bool>>> = Box::pin(snd.ready());
    assert!(poll(&mut ready).is_pending());
    session_window(&conn, 1, 1);
    assert!(poll(&mut ready).is_pending());

    // released claim makes window available
    drop(tr);
    assert_eq!(poll(&mut ready), Poll::Ready(true));

    // window is available, link credit is not
    link_flow(&conn, 4, (0, 0, false), 1, 1);
    assert_eq!(snd.credit(), 0);
    let mut ready: Pin<Box<dyn Future<Output = bool>>> = Box::pin(snd.ready());
    assert!(poll(&mut ready).is_pending());
    link_flow(&conn, 4, (0, 5, false), 1, 1);
    assert_eq!(poll(&mut ready), Poll::Ready(true));

    // detached link wakes window waiter
    session_window(&conn, 1, 0);
    let mut ready: Pin<Box<dyn Future<Output = bool>>> = Box::pin(snd.ready());
    assert!(poll(&mut ready).is_pending());
    handle_frame(&conn, peer_detach(4)).unwrap();
    assert_eq!(poll(&mut ready), Poll::Ready(false));
}

#[ntex::test]
async fn sender_cancelled_waiters_leave_queue() {
    fn poll<F: Future + ?Sized>(fut: &mut Pin<Box<F>>) -> Poll<F::Output> {
        fut.as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
    }
    use std::task::Poll;

    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let snd = add_sender(&conn, "s", 4, 0);
    let send = |body: &'static [u8]| {
        let mut fut = Box::pin(snd.transfer(Bytes::from_static(body)).settled().send());
        assert!(poll(&mut fut).is_pending());
        fut
    };
    // (link credit waiters, session window waiters)
    let pending = || {
        (
            snd.inner.get_ref().pending_transfers(),
            session(&conn).inner.get_ref().pending_transfers(),
        )
    };

    // transfers wait for session window
    let t1 = send(b"1");
    let mut t2 = send(b"2");
    assert_eq!(pending(), (0, 2));
    drop(t1);
    assert_eq!(pending(), (0, 1));

    // transfers wait for link credit
    link_flow(&conn, 4, (0, 0, false), 1, 0);
    let t3 = send(b"3");
    let mut t4 = send(b"4");
    assert_eq!(pending(), (2, 1));
    drop(t3);
    assert_eq!(pending(), (1, 1));

    // repeatedly cancelled transfers do not accumulate
    for _ in 0..10 {
        drop(send(b"5"));
    }
    assert_eq!(pending(), (1, 1));

    // remaining waiters are served
    link_flow(&conn, 4, (0, 2, false), 1, 2);
    assert!(matches!(poll(&mut t2), Poll::Ready(Ok(_))));
    assert!(matches!(poll(&mut t4), Poll::Ready(Ok(_))));
    assert_eq!(pending(), (0, 0));
    ntex::time::sleep(ntex::time::Millis(50)).await;
    assert_eq!(
        transfer_frames(&client)
            .into_iter()
            .filter(|f| f.starts_with("Transfer"))
            .collect::<Vec<_>>(),
        [
            "Transfer Some(0) more:false aborted:false",
            "Transfer Some(1) more:false aborted:false"
        ]
    );
}

#[ntex::test]
async fn sender_cancelled_waiter_keeps_queued_abort() {
    use ntex::time::{Millis, sleep};

    let (_io, conn, client, _snd, t2) = queued_abort_sender().await;
    let s = session(&conn);

    // waiter leaves the queue, abort of cancelled delivery stays
    drop(t2);
    assert_eq!(s.inner.get_ref().pending_transfers(), 1);
    session_window(&conn, 2, 1);
    assert_eq!(s.inner.get_ref().pending_transfers(), 0);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(0) more:false aborted:true"]
    );
}
