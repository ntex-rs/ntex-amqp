use super::*;

/// STATEMENT: A locally attached sender sends its configured initial delivery-count,
/// ignores the receiver's value, and computes credit from it and sent transfers.
///
/// HOW:
/// - For default, `Some(5)` and `None` initial counts, attach a sender and check the
///   sent `Attach` carries the expected `initial_delivery_count`.
/// - Reply with a receiver attach carrying delivery-count 100 and confirm it is ignored.
/// - Send flows with credit 10, with and without delivery-count, and assert credit 10.
/// - Send one transfer, then a flow with delivery-count advanced by one; credit stays 10.
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
        sleep(Millis(10)).await;

        let counts: Vec<_> = read_frames(&client)
            .into_iter()
            .filter_map(|frame| match frame {
                Frame::Attach(att) => Some(att.initial_delivery_count()),
                _ => None,
            })
            .collect();
        assert_eq!(counts, [Some(sent)], "{ctx}");

        // receiver initial delivery-count is ignored
        let Frame::Attach(mut attach) = named_attach(Role::Receiver, "s", "s", 0) else {
            panic!()
        };
        attach.0.initial_delivery_count = Some(100);
        let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
            panic!("{ctx}")
        };
        let link = timeout(Millis(1000), fut).await.unwrap().unwrap().unwrap();

        let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
            panic!()
        };
        flow.0.delivery_count = flow_count;
        handle_frame(&conn, flow.into()).unwrap();
        assert_eq!(link.credit(), 10, "{ctx}");

        // delivery-count is advanced by transfer
        timeout(
            Millis(1000),
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

/// STATEMENT: A sender adopts the peer's max-message-size, treating 0 as unlimited,
/// clamping to `u32::MAX`, and rejecting larger bodies.
///
/// HOW:
/// - For local and remote attaches, peer size 0 gives `max_message_size()` `None`.
/// - Peer size 10 gives `Some(10)`; an 11-byte transfer fails with `BodyTooLarge`.
/// - `set_max_message_size(0)` resets the limit to `None`.
/// - Peer size `u64::MAX` is reported as `Some(u32::MAX)`.
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

/// STATEMENT: A drain flow consumes the sender's remaining credit and is answered with
/// a drain flow, after queued transfers and including drains received before attach.
///
/// HOW:
/// - Drain an idle link; credit becomes 0 and a flow with advanced delivery-count is
///   sent.
/// - Drain with two queued transfers; both are sent before the answering drain flow.
/// - Drop a woken transfer after drain; its credit is drained and a flow is sent.
/// - Check a non-drain flow only sets credit, and a drain flow handled before the link is
///   confirmed is answered right after its `Attach`.
#[ntex::test]
async fn sender_link_drain() {
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
    let frames = || snd_frames(&client);

    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
    else {
        panic!()
    };
    let snd = session
        .inner
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
    assert!(pending(t3.as_mut()).await);
    flow(2, 8, true);
    sleep(Millis(50)).await;
    assert_eq!(frames(), Vec::<String>::new());
    assert_eq!(snd.credit(), 2);
    drop(t3);
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Flow Some(0) Some(10) Some(0) true"]);

    // no drain
    flow(2, 10, false);
    assert_eq!(snd.credit(), 2);
    sleep(Millis(50)).await;
    assert_eq!(frames(), Vec::<String>::new());

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
    let lnk = session
        .inner
        .attach_remote_sender_link(&attach, response, lnk.inner.clone());
    assert_eq!(lnk.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Attach", "Flow Some(1) Some(3) Some(0) true"]);
}

/// STATEMENT: Transfers waiting for the session window keep link credit on cancel and
/// do not delay drain, and fail when the link is detached.
///
/// HOW:
/// - With window 0, start and drop a transfer; link credit stays 2.
/// - Drain while a transfer waits for the window; credit becomes 0 and the drain flow
///   is sent at once.
/// - A woken transfer delays drain until window-blocked, then waits for new credit
///   before sending.
/// - Detach the link while a transfer waits for the window; the transfer fails and
///   only `Detach` is written.
#[ntex::test]
async fn sender_link_credit_on_window_wait() {
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
    let frames = || snd_frames(&client);

    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 4))
    else {
        panic!()
    };
    let snd = session
        .inner
        .attach_remote_sender_link(&attach, response, snd.inner.clone());
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Begin", "Attach"]);

    // transfer waits for session window, link credit is kept on cancel
    flow(0, 2, false, 0);
    let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
    assert!(pending(t1.as_mut()).await);
    assert_eq!(snd.credit(), 2);
    drop(t1);
    assert_eq!(snd.credit(), 2);

    // transfer blocked by session window does not delay drain
    let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).settled().send());
    assert!(pending(t2.as_mut()).await);
    flow(0, 2, true, 0);
    assert_eq!(snd.credit(), 0);
    sleep(Millis(50)).await;
    assert_eq!(frames(), ["Flow Some(0) Some(2) Some(0) true"]);
    drop(t2);
    sleep(Millis(50)).await;
    assert_eq!(frames(), Vec::<String>::new());

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

/// STATEMENT: Sender flows wake only as many waiting transfers as there is credit and
/// window, and pass the credit of dropped waiters on to the next waiter.
///
/// HOW:
/// - Poll transfers with counting wakers; credit 1 wakes one, credit 2 wakes two.
/// - Drop woken credit and window waiters and check the next waiter is woken.
/// - Check credit is claimed by a window waiter and that a woken transfer is not queued
///   behind credit waiters.
/// - With a partially sent multi-frame delivery, the next transfer is not woken until
///   the delivery completes.
#[ntex::test]
async fn sender_link_flow_wakes_by_credit() {
    type Fut = Pin<Box<dyn Future<Output = Result<crate::Delivery, AmqpProtocolError>>>>;
    struct Tr(Fut, Arc<WakeCounter>);
    impl Tr {
        fn poll(&mut self) -> Poll<Result<crate::Delivery, AmqpProtocolError>> {
            let waker = Waker::from(self.1.clone());
            self.0.as_mut().poll(&mut Context::from_waker(&waker))
        }
        fn woken(&self) -> usize {
            self.1.count()
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
    let snd = session
        .inner
        .attach_remote_sender_link(&attach, response, snd.inner.clone());
    let transfer = || {
        let mut tr = Tr(
            Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send()),
            WakeCounter::new(),
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
        let mut tr = Tr(Box::pin(snd.transfer(body).send()), WakeCounter::new());
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

/// STATEMENT: Closing a sender link fails its transfers waiting for credit, session
/// window or resumption, and no transfers are written after `Detach`.
///
/// HOW:
/// - Close one link with a transfer waiting for credit and one waiting for the window;
///   both transfers fail.
/// - Wake a transfer with credit, then close its link before it resumes; it fails.
/// - Assert only `Detach` and `Attach` frames are written, with no `Transfer`.
#[ntex::test]
async fn sender_link_close_fails_waiting_transfers() {
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
    assert!(pending(close.as_mut()).await);
    assert!(timeout(Millis(500), t3).await.unwrap().unwrap().is_err());

    // no transfers after detach
    sleep(Millis(50)).await;
    assert_eq!(frame_names(&client), ["Detach 2"]);
}

/// STATEMENT: Detaching a sender link from the session fails its waiting transfers with
/// `Disconnected` and rejects new transfers.
///
/// HOW:
/// - Wait one transfer for link credit and one for the session window on two links.
/// - Call `detach_sender_link` for both; both transfers fail with `Disconnected`.
/// - A new transfer on the detached link fails, and a later flow writes no `Transfer`.
#[ntex::test]
async fn sender_link_detach_fails_waiting_transfers() {
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

    let _d1 = session.detach_sender_link(a.inner.id(), None);
    let _d2 = session.detach_sender_link(b.inner.id(), None);
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

/// STATEMENT: Detaching a sender link fails a partially sent delivery and the transfer
/// queued behind it, and stops sending the remaining frames.
///
/// HOW:
/// - Send a 2048-byte body with small frames; two frames go out before the window ends.
/// - Detach the link; both transfers fail with `Disconnected`.
/// - Open the session window and assert only `Detach` is written.
#[ntex::test]
async fn sender_link_detach_fails_partial_delivery() {
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

    let _d = session.detach_sender_link(snd.inner.id(), None);
    let res = timeout(Millis(500), t1).await.unwrap().unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));
    let res = timeout(Millis(500), t2).await.unwrap().unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::Disconnected)));

    // no transfers after detach
    session_window(&conn, 1, 10);
    sleep(Millis(50)).await;
    assert_eq!(transfer_frames(&client), ["Detach"]);
}

/// STATEMENT: An unsettled delivery stays pending after a local detach until the peer
/// confirms it, then fails with `LinkDetached`.
///
/// HOW:
/// - Send an unsettled transfer and wait on `Delivery::wait`.
/// - Detach the link locally; the wait is still pending.
/// - Handle the peer `Detach`; the wait fails with `LinkDetached(None)`.
#[ntex::test]
async fn sender_link_detach_fails_unsettled() {
    let (_io, conn, _client, snd) = small_frames_sender();
    let session = session(&conn);
    let delivery = snd.transfer(Bytes::from_static(b"1")).send().await.unwrap();
    let mut wait = Box::pin(delivery.wait());
    assert!(pending(wait.as_mut()).await);

    // disposition could be received before detach confirmation
    let _d = session.detach_sender_link(snd.inner.id(), None);
    sleep(Millis(50)).await;
    assert!(pending(wait.as_mut()).await);

    handle_frame(&conn, peer_detach(4)).unwrap();
    let res = timeout(Millis(500), wait).await.unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::LinkDetached(None))));
}

/// STATEMENT: Sender credit accounts for transfers the receiver has not yet seen,
/// including across delivery-count wrap-around.
///
/// HOW:
/// - For initial delivery-count 0 and `u32::MAX - 1`, a flow without delivery-count
///   uses the initial count; after 3 transfers a repeated flow leaves credit at 7.
/// - A stale flow with credit 2 gives 0 credit; credit 5 unblocks a transfer, leaving 1.
/// - A flow with delivery-count ahead of the sender uses its credit 4 as is.
#[ntex::test]
async fn sender_link_stale_flow() {
    for initial in [0, u32::MAX - 1] {
        sender_link_stale_flow_with(initial).await;
    }
}

async fn sender_link_stale_flow_with(initial: u32) {
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
    let snd = session
        .inner
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
    assert!(pending(t.as_mut()).await);
    flow(Some(0), 5);
    assert!(timeout(Millis(500), t).await.unwrap().is_ok());
    assert_eq!(snd.credit(), 1);

    // receiver delivery count is ahead, credit is used as is
    flow(Some(10), 4);
    assert_eq!(snd.credit(), 4);
    sleep(Millis(10)).await;
}

/// STATEMENT: A drain is not delayed by a partial delivery blocked on the session
/// window, and the queued transfer behind it waits for new credit.
///
/// HOW:
/// - Start a multi-frame delivery that blocks on the window, and queue a second transfer.
/// - Drain; credit becomes 0 and a drain flow with delivery-count 10 is sent.
/// - Open the window; the partial delivery completes but the queued one waits.
/// - Grant credit 1; the queued transfer completes and credit is 0.
#[ntex::test]
async fn sender_drain_with_partial_delivery() {
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
        read_frames(&client)
            .into_iter()
            .map(|frame| match frame {
                Frame::Flow(flow) => format!(
                    "Flow {:?} {:?} {}",
                    flow.delivery_count(),
                    flow.link_credit(),
                    flow.drain()
                ),
                frame => frame.name().to_string(),
            })
            .collect::<Vec<_>>()
    };
    sleep(Millis(50)).await;
    link_flows();

    // partial delivery waits for session window, next transfer is queued
    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).settled().send());
    assert!(pending(t1.as_mut()).await);
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

/// STATEMENT: Each frame of a multi-frame delivery consumes session window, frames of
/// one delivery are not interleaved with the next, and ids are counted separately.
///
/// HOW:
/// - Send a 1200-byte body with window 2; two frames are written and a second transfer
///   waits.
/// - Open window 1; the last frame completes delivery 0 while the next transfer waits.
/// - Open more window; the next delivery gets id 1 and credit is 8.
/// - An echo flow reports `next-outgoing-id` 5, counting transfer frames.
#[ntex::test]
async fn sender_multi_frame_delivery_window() {
    let (_io, conn, client, snd) = small_frames_sender();
    sleep(Millis(50)).await;
    transfer_frames(&client);

    // each transfer frame consumes session window
    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
    let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).send());
    assert!(pending(t1.as_mut()).await);
    assert!(pending(t2.as_mut()).await);
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
    assert!(pending(t2.as_mut()).await);
    sleep(Millis(50)).await;
    assert_eq!(transfer_frames(&client), Vec::<String>::new());
    let Poll::Ready(Ok(d1)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d1.id(), 0);
    assert!(pending(t2.as_mut()).await);
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

/// STATEMENT: Cancelling a partially sent delivery removes it from unsettled and sends an
/// aborted transfer before the next delivery.
///
/// HOW:
/// - Drop a multi-frame delivery waiting for the window; unsettled count drops to 0.
/// - Open the window; an aborted transfer is sent, then the next delivery gets id 1.
/// - Drop another blocked delivery while window is available; the abort is sent at once
///   and link credit is 7.
#[ntex::test]
async fn sender_cancelled_delivery_aborted() {
    let (_io, conn, client, snd) = small_frames_sender();
    let body = Bytes::from(vec![b'a'; 1200]);
    let unsettled = || unsettled(&conn).0;

    // delivery is cancelled while it waits for session window
    let mut t1 = Box::pin(snd.transfer(body.clone()).send());
    assert!(pending(t1.as_mut()).await);
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
    assert!(pending(t3.as_mut()).await);
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

/// STATEMENT: A new transfer does not take link credit granted to an already woken
/// transfer.
///
/// HOW:
/// - Start a transfer with no credit, then grant credit 1 and start a second transfer.
/// - Poll the first; it completes with id 0 while the second stays pending at credit 0.
/// - Grant more credit; the second completes with id 1 and both frames are written.
#[ntex::test]
async fn sender_woken_transfer_keeps_link_credit() {
    let (_io, conn, client, snd) = small_frames_sender();
    link_flow(&conn, 4, (0, 0, false), 1, 10);
    sleep(Millis(50)).await;
    transfer_frames(&client);
    let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).send());
    assert!(pending(t1.as_mut()).await);

    // new transfer does not take credit of woken transfer
    link_flow(&conn, 4, (0, 1, false), 1, 10);
    let mut t2 = Box::pin(snd.transfer(Bytes::from_static(b"2")).send());
    assert!(pending(t2.as_mut()).await);
    let Poll::Ready(Ok(d1)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d1.id(), 0);
    assert!(pending(t2.as_mut()).await);
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

/// STATEMENT: `ready()` does not report link credit claimed by a woken transfer, and
/// resolves once the claim is released.
///
/// HOW:
/// - Start a transfer with no credit, then grant credit 1 so the queued transfer is
///   woken and claims it.
/// - Assert `ready()` stays pending while the session window is open.
/// - Drop the woken transfer without polling it; `ready()` resolves `true` and a new
///   transfer takes the released credit with id 0.
#[ntex::test]
async fn sender_ready_skips_claimed_credit() {
    let (_io, conn, _client, snd) = small_frames_sender();
    link_flow(&conn, 4, (0, 0, false), 1, 10);
    let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).send());
    assert!(pending(t1.as_mut()).await);

    link_flow(&conn, 4, (0, 1, false), 1, 10);
    let mut ready = Box::pin(snd.ready());
    assert!(pending(ready.as_mut()).await);

    // released credit wakes readiness waiter
    drop(t1);
    assert!(timeout(Millis(1000), ready).await.unwrap());
    let d2 = snd.transfer(Bytes::from_static(b"2")).send().await.unwrap();
    assert_eq!(d2.id(), 0);
}

/// STATEMENT: `ready()` stays pending while a partial delivery is in progress, and
/// resolves once the delivery completes.
///
/// HOW:
/// - Send a 3-frame delivery with session window 1, so it waits after the first frame.
/// - Open window 3; the delivery is woken and claims 1, leaving window for new
///   transfers, and assert `ready()` is still pending.
/// - Poll the delivery to completion and assert `ready()` resolves `true`.
#[ntex::test]
async fn sender_ready_waits_for_partial_delivery() {
    let (_io, conn, _client, snd) = small_frames_sender();
    link_flow(&conn, 4, (0, 10, false), 1, 1);
    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
    assert!(pending(t1.as_mut()).await);

    session_window(&conn, 2, 3);
    let mut ready = Box::pin(snd.ready());
    assert!(pending(ready.as_mut()).await);

    // completed delivery wakes readiness waiter
    let Poll::Ready(Ok(d1)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d1.id(), 0);
    assert!(timeout(Millis(1000), ready).await.unwrap());
}

/// STATEMENT: The abort of a cancelled delivery does not take session window already
/// claimed by a woken waiter, and is sent before new transfers.
///
/// HOW:
/// - Block a partial delivery on one link and a transfer on another, open window 1,
///   then drop the partial delivery; the other transfer completes with id 1.
/// - Start two new transfers; window 1 sends only the abort, window 2 sends both.
/// - Assert the written frame order and delivery ids 2 and 3.
#[ntex::test]
async fn sender_abort_does_not_take_claimed_window() {
    let (_io, conn, client, snd) = small_frames_sender();
    let snd2 = add_sender(&conn, "s2", 5, 1);
    sleep(Millis(50)).await;
    transfer_frames(&client);

    let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
    let mut t2 = Box::pin(snd2.transfer(Bytes::from_static(b"2")).send());
    assert!(pending(t1.as_mut()).await);
    assert!(pending(t2.as_mut()).await);

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
    assert!(pending(t3.as_mut()).await);
    assert!(pending(t4.as_mut()).await);
    session_window(&conn, 3, 1);
    assert!(pending(t3.as_mut()).await);
    assert!(pending(t4.as_mut()).await);
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

/// STATEMENT: A new delivery on a link with a queued abort waits until the abort is
/// sent.
///
/// HOW:
/// - Use `queued_abort_sender` and start a new transfer on the aborted link.
/// - Window 1 completes the other link's transfer; window 1 then sends only the abort.
/// - The next window 1 sends the new delivery with id 2.
#[ntex::test]
async fn sender_queued_abort_keeps_order() {
    let (_io, conn, client, snd, mut t2) = queued_abort_sender().await;

    // new delivery on the same link waits behind its abort
    let mut t3 = Box::pin(snd.transfer(Bytes::from_static(b"3")).send());
    assert!(pending(t3.as_mut()).await);

    session_window(&conn, 2, 1);
    let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d2.id(), 1);
    assert!(pending(t3.as_mut()).await);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(1) more:false aborted:false"]
    );

    session_window(&conn, 3, 1);
    assert!(pending(t3.as_mut()).await);
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

/// STATEMENT: Detaching a link drops its queued abort so the session window goes to the
/// next waiter.
///
/// HOW:
/// - Use `queued_abort_sender`, then close the link locally or handle a peer `Detach`.
/// - Open the window; the other link's transfer completes with id 1.
/// - Assert no aborted transfer is written and no session transfers remain pending.
#[ntex::test]
async fn sender_detach_drops_queued_abort() {
    for local in [false, true] {
        let (_io, conn, client, snd, mut t2) = queued_abort_sender().await;
        if local {
            let mut fut = Box::pin(snd.close());
            assert!(pending(fut.as_mut()).await);
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
        assert_eq!(session(&conn).inner.pending_transfers(), 0);
    }
}

/// STATEMENT: `SenderLink::ready` waits for both link credit and unclaimed session
/// window, and resolves `false` when the link is detached.
///
/// HOW:
/// - With credit but no window, `ready` is pending until the window opens.
/// - Window claimed by a woken transfer keeps `ready` pending until the transfer drops.
/// - With window but no credit, `ready` is pending until credit is granted.
/// - A peer `Detach` resolves a pending `ready` with `false`.
#[ntex::test]
async fn sender_ready_waits_for_session_window() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let snd = add_sender(&conn, "s", 4, 0);
    assert_eq!(snd.credit(), 10);
    let poll = |fut: &mut Pin<Box<dyn Future<Output = bool> + '_>>| poll_once(fut.as_mut());

    // link credit is available, remote session window is not
    let mut ready: Pin<Box<dyn Future<Output = bool>>> = Box::pin(snd.ready());
    assert!(poll(&mut ready).is_pending());
    session_window(&conn, 1, 1);
    assert_eq!(poll(&mut ready), Poll::Ready(true));
    assert!(snd.ready().await);

    // window claimed by woken transfer is not available
    session_window(&conn, 1, 0);
    let mut tr = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
    assert!(poll_once(tr.as_mut()).is_pending());
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

/// STATEMENT: Cancelled transfers leave the link credit and session window wait queues,
/// so repeated cancellation does not accumulate waiters.
///
/// HOW:
/// - Drop one of two window waiters and one of two credit waiters; queue sizes drop.
/// - Drop ten more pending transfers; queue sizes stay `(1, 1)`.
/// - Grant credit and window; the remaining two transfers complete and are written.
#[ntex::test]
async fn sender_cancelled_waiters_leave_queue() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let snd = add_sender(&conn, "s", 4, 0);
    let send = |body: &'static [u8]| {
        let mut fut = Box::pin(snd.transfer(Bytes::from_static(body)).settled().send());
        assert!(poll_once(fut.as_mut()).is_pending());
        fut
    };
    // (link credit waiters, session window waiters)
    let pending = || {
        (
            snd.inner.pending_transfers(),
            session(&conn).inner.pending_transfers(),
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
    assert!(matches!(poll_once(t2.as_mut()), Poll::Ready(Ok(_))));
    assert!(matches!(poll_once(t4.as_mut()), Poll::Ready(Ok(_))));
    assert_eq!(pending(), (0, 0));
    sleep(Millis(50)).await;
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

/// STATEMENT: Dropping a waiter leaves the queued abort of a cancelled delivery in the
/// session queue, and it is still sent.
///
/// HOW:
/// - Use `queued_abort_sender` and drop the other link's waiting transfer.
/// - One session transfer remains pending; opening the window clears it.
/// - Assert the aborted transfer for delivery 0 is written.
#[ntex::test]
async fn sender_cancelled_waiter_keeps_queued_abort() {
    let (_io, conn, client, _snd, t2) = queued_abort_sender().await;
    let s = session(&conn);

    // waiter leaves the queue, abort of cancelled delivery stays
    drop(t2);
    assert_eq!(s.inner.pending_transfers(), 1);
    session_window(&conn, 2, 1);
    assert_eq!(s.inner.pending_transfers(), 0);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        ["Transfer Some(0) more:false aborted:true"]
    );
}

/// Frame names, flow frames with (handle, delivery-count, link-credit, drain)
fn snd_frames(client: &IoTest) -> Vec<String> {
    read_frames(client)
        .into_iter()
        .map(|frame| match frame {
            Frame::Flow(flow) => format!(
                "Flow {:?} {:?} {:?} {}",
                flow.handle(),
                flow.delivery_count(),
                flow.link_credit(),
                flow.drain()
            ),
            frame => frame.name().to_string(),
        })
        .collect()
}
