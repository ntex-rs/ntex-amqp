use super::*;

fn remote_disposition(conn: &Connection, role: Role, id: u32, settled: bool) {
    let disp = Disposition(Box::new(DispositionInner {
        role,
        first: id,
        last: None,
        settled,
        state: Some(DeliveryState::Accepted(Accepted {})),
        batchable: false,
    }));
    handle_frame(conn, disp.into()).unwrap();
}

#[ntex::test]
async fn sender_delivery_remote_settled() {
    use ntex::time::{Millis, sleep};

    // 0 - wait() after disposition, 1 - drop without wait(), 2 - settle(), 3 - update_state()
    for case in 0..4 {
        let (_io, conn, client, snd) = small_frames_sender();
        sleep(Millis(50)).await;
        transfer_frames(&client);

        let mut d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
        remote_disposition(&conn, Role::Receiver, d.id(), true);
        match case {
            0 => assert!(
                matches!(d.wait().await, Ok(Some(DeliveryState::Accepted(_)))),
                "case: {case}"
            ),
            2 => d.settle(DeliveryState::Accepted(Accepted {})),
            3 => d.update_state(DeliveryState::Accepted(Accepted {})),
            _ => (),
        }
        drop(d);
        assert!(
            session(&conn)
                .inner
                .get_ref()
                .unsettled_snd_deliveries
                .is_empty()
        );
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            ["Transfer Some(0) more:false aborted:false"],
            "case: {case}"
        );
    }

    // remote settled state is preserved after session end
    let (_io, conn, _client, snd) = small_frames_sender();
    let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
    remote_disposition(&conn, Role::Receiver, d.id(), true);
    assert!(matches!(
        d.wait().await,
        Ok(Some(DeliveryState::Accepted(_)))
    ));
    let s = session(&conn);
    let _ = handle_frame(&conn, End { error: None }.into());
    assert!(s.inner.get_ref().unsettled_snd_deliveries.is_empty());
    assert!(d.is_remote_settled());
}

fn dispositions(client: &IoTest) -> Vec<String> {
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    let mut frames = Vec::new();
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        if let Frame::Disposition(disp) = frame.into_parts().1 {
            frames.push(format!(
                "{:?} {} {} {:?}",
                disp.role(),
                disp.first(),
                disp.settled(),
                disp.state()
            ));
        }
    }
    frames
}

#[ntex::test]
async fn sender_delivery_drop_settles_with_remote_outcome() {
    use ntex::time::{Millis, sleep};

    let rejected = DeliveryState::Rejected(Rejected { error: None });
    let received = DeliveryState::Received(Received {
        section_number: 0,
        section_offset: 1,
    });
    for (state, expected) in [
        (None, "Sender 0 true None"),
        (
            Some(DeliveryState::Accepted(Accepted {})),
            "Sender 0 true Some(Accepted(Accepted))",
        ),
        (
            Some(rejected),
            "Sender 0 true Some(Rejected(Rejected { error: None }))",
        ),
        (Some(received), "Sender 0 true None"),
    ] {
        let (_io, conn, client, snd) = small_frames_sender();
        sleep(Millis(50)).await;
        transfer_frames(&client);
        let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
        if let Some(state) = state {
            let disp = Disposition(Box::new(DispositionInner {
                role: Role::Receiver,
                first: d.id(),
                last: None,
                settled: false,
                state: Some(state),
                batchable: false,
            }));
            handle_frame(&conn, disp.into()).unwrap();
            assert!(!d.is_remote_settled());
        }
        drop(d);
        assert!(
            session(&conn)
                .inner
                .get_ref()
                .unsettled_snd_deliveries
                .is_empty()
        );
        sleep(Millis(50)).await;
        assert_eq!(dispositions(&client), [expected]);
    }
}

#[ntex::test]
async fn receiver_delivery_remote_settled() {
    use ntex::time::{Millis, sleep};

    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let s = session(&conn);
    let fut = ntex::rt::spawn({
        let s = s.clone();
        async move { s.build_receiver_link("r", "r").attach().await }
    });
    sleep(Millis(10)).await;
    let Ok(Action::None) = handle_frame(&conn, named_attach(Role::Sender, "r", "r", 0)) else {
        panic!()
    };
    let link = fut.await.unwrap().unwrap();
    link.set_link_credit(10);
    for id in 0..4 {
        handle_frame(&conn, transfer(id, false, None, 1)).unwrap();
    }
    let mut deliveries = Vec::new();
    for _ in 0..4 {
        deliveries.push(link.recv().await.unwrap().unwrap().0);
    }
    sleep(Millis(50)).await;
    frame_names(&client);

    // remote settled: settle() and drop do not send disposition
    remote_disposition(&conn, Role::Sender, 0, true);
    remote_disposition(&conn, Role::Sender, 1, true);
    let d3 = deliveries.pop().unwrap();
    let mut d2 = deliveries.pop().unwrap();
    let d1 = deliveries.pop().unwrap();
    let mut d0 = deliveries.pop().unwrap();
    assert!(d0.is_remote_settled());
    d0.settle(DeliveryState::Accepted(Accepted {}));
    drop(d0);
    drop(d1);
    sleep(Millis(50)).await;
    assert!(frame_names(&client).is_empty());

    // not settled by remote
    assert!(!d2.is_remote_settled());
    d2.settle(DeliveryState::Accepted(Accepted {}));
    drop(d2);
    drop(d3);
    assert!(s.inner.get_ref().unsettled_rcv_deliveries.is_empty());
    sleep(Millis(50)).await;
    let disp = dispositions(&client);
    assert_eq!(disp.len(), 2);
    assert_eq!(disp[0], "Receiver 2 true Some(Accepted(Accepted))");
    assert!(disp[1].starts_with("Receiver 3 true Some(Rejected("));
}

#[ntex::test]
async fn delivery_wait_returns_session_error() {
    use ntex::time::{Millis, timeout};
    use std::{future::poll_fn, task::Poll};

    let err = Error(Box::new(codec::ErrorInner {
        condition: AmqpError::InternalError.into(),
        description: None,
        info: None,
    }));
    let ended = format!("{:?}", AmqpProtocolError::SessionEnded(Some(err.clone())));
    for remote_end in [true, false] {
        let expected = if remote_end {
            ended.clone()
        } else {
            format!("{:?}", AmqpProtocolError::Disconnected)
        };
        let (_io, conn, _client, snd) = small_frames_sender();
        let d1 = snd.transfer(Bytes::from_static(b"1")).send().await.unwrap();
        let d2 = snd.transfer(Bytes::from_static(b"2")).send().await.unwrap();
        let mut wait = Box::pin(d1.wait());
        assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);

        if remote_end {
            handle_frame(
                &conn,
                End {
                    error: Some(err.clone()),
                }
                .into(),
            )
            .unwrap();
        } else {
            conn.get_ref()
                .0
                .get_mut()
                .set_error(AmqpProtocolError::Disconnected);
        }

        // pending and late `wait()` return session error
        let res = timeout(Millis(500), wait).await.unwrap();
        assert_eq!(format!("{:?}", res.unwrap_err()), expected);
        assert_eq!(format!("{:?}", d2.wait().await.unwrap_err()), expected);
    }

    // receiver delivery
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let Ok(Action::AttachReceiver(link, _, response)) = handle_frame(&conn, attach()) else {
        panic!()
    };
    link.confirm_receiver_link(response);
    link.set_link_credit(1);
    handle_frame(&conn, transfer(0, false, None, 0)).unwrap();
    let (delivery, _) = link.get_delivery().unwrap();
    handle_frame(
        &conn,
        End {
            error: Some(err.clone()),
        }
        .into(),
    )
    .unwrap();
    assert_eq!(format!("{:?}", delivery.wait().await.unwrap_err()), ended);
}

fn remote_state(conn: &Connection, id: u32, settled: bool, state: Option<DeliveryState>) {
    let disp = Disposition(Box::new(DispositionInner {
        role: Role::Receiver,
        first: id,
        last: None,
        settled,
        state,
        batchable: false,
    }));
    handle_frame(conn, disp.into()).unwrap();
}

#[ntex::test]
async fn delivery_wait_outcome() {
    use ntex::time::{Millis, sleep, timeout};
    use ntex_amqp_codec::protocol::Modified;
    use std::future::poll_fn;
    use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let received = || {
        Some(DeliveryState::Received(Received {
            section_number: 0,
            section_offset: 0,
        }))
    };
    let modified = || {
        Some(DeliveryState::Modified(Modified {
            delivery_failed: Some(true),
            undeliverable_here: None,
            message_annotations: None,
        }))
    };

    // non-terminal dispositions do not resolve `wait()`
    for settled in [false, true] {
        let (_io, conn, _client, snd) = small_frames_sender();
        let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
        let mut wait = Box::pin(d.wait());
        assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);
        remote_state(&conn, d.id(), false, None);
        assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);
        remote_state(&conn, d.id(), false, received());
        assert!(poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await);

        if settled {
            // settled without outcome
            remote_state(&conn, d.id(), true, None);
            assert!(timeout(Millis(500), wait).await.unwrap().unwrap().is_none());
            assert!(d.is_remote_settled());
        } else {
            remote_state(
                &conn,
                d.id(),
                false,
                Some(DeliveryState::Accepted(Accepted {})),
            );
            assert!(matches!(
                timeout(Millis(500), wait).await.unwrap(),
                Ok(Some(DeliveryState::Accepted(_)))
            ));
            assert!(!d.is_remote_settled());
        }
    }

    // `Modified` outcome is not consumed by `wait()`
    let (_io, conn, client, snd) = small_frames_sender();
    sleep(Millis(50)).await;
    transfer_frames(&client);
    let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
    remote_state(&conn, d.id(), false, modified());
    for _ in 0..2 {
        assert!(matches!(
            d.wait().await,
            Ok(Some(DeliveryState::Modified(_)))
        ));
    }
    drop(d);
    sleep(Millis(50)).await;
    let disp = dispositions(&client);
    assert_eq!(disp.len(), 1);
    assert!(
        disp[0].starts_with("Sender 0 true Some(Modified("),
        "{disp:?}"
    );

    // concurrent `wait()` calls receive outcome, and do not wake each other
    let (_io, conn, _client, snd) = small_frames_sender();
    let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
    let woken = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(woken.clone());
    let mut w1 = Box::pin(d.wait());
    let mut w2 = Box::pin(d.wait());
    let mut cx = Context::from_waker(&waker);
    assert!(w1.as_mut().poll(&mut cx).is_pending());
    assert!(poll_fn(|cx| Poll::Ready(w2.as_mut().poll(cx).is_pending())).await);
    assert_eq!(woken.0.load(Ordering::Relaxed), 0);
    remote_state(
        &conn,
        d.id(),
        true,
        Some(DeliveryState::Accepted(Accepted {})),
    );
    assert_eq!(woken.0.load(Ordering::Relaxed), 1);
    for w in [w1, w2] {
        assert!(matches!(
            timeout(Millis(500), w).await.unwrap(),
            Ok(Some(DeliveryState::Accepted(_)))
        ));
    }

    // dropped waiters are not accumulated
    let (_io, conn, _client, snd) = small_frames_sender();
    let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
    for _ in 0..3 {
        let mut w = Box::pin(d.wait());
        assert!(poll_fn(|cx| Poll::Ready(w.as_mut().poll(cx).is_pending())).await);
    }
    let inner = session(&conn).inner;
    assert_eq!(
        inner.get_ref().unsettled_snd_deliveries[&d.id()].waiters(),
        0
    );
    let mut live = Box::pin(d.wait());
    assert!(poll_fn(|cx| Poll::Ready(live.as_mut().poll(cx).is_pending())).await);
    for _ in 0..3 {
        let mut w = Box::pin(d.wait());
        assert!(poll_fn(|cx| Poll::Ready(w.as_mut().poll(cx).is_pending())).await);
    }
    assert_eq!(
        inner.get_ref().unsettled_snd_deliveries[&d.id()].waiters(),
        1
    );
    drop(live);

    // dropped waiter does not block notification of active waiter
    let (_io, conn, _client, snd) = small_frames_sender();
    let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
    let mut w1 = Box::pin(d.wait());
    let mut w2 = Box::pin(d.wait());
    assert!(poll_fn(|cx| Poll::Ready(w1.as_mut().poll(cx).is_pending())).await);
    assert!(poll_fn(|cx| Poll::Ready(w2.as_mut().poll(cx).is_pending())).await);
    drop(w1);
    let mut w3 = Box::pin(d.wait());
    assert!(poll_fn(|cx| Poll::Ready(w3.as_mut().poll(cx).is_pending())).await);
    remote_state(
        &conn,
        d.id(),
        false,
        Some(DeliveryState::Accepted(Accepted {})),
    );
    for w in [w2, w3] {
        assert!(matches!(
            timeout(Millis(500), w).await.unwrap(),
            Ok(Some(DeliveryState::Accepted(_)))
        ));
    }
}

#[ntex::test]
async fn delivery_no_disposition_after_link_detach() {
    use ntex::time::{Millis, sleep};

    let rejected = || DeliveryState::Rejected(Rejected { error: None });

    // 0 - settle(), 1 - update_state(), 2 - drop
    for case in 0..3 {
        // remote detach, local detach confirmed by remote
        for local in [false, true] {
            let (_io, conn, client, snd) = small_frames_sender();
            let mut d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
            let s = session(&conn);
            let _d = local.then(|| s.detach_sender_link(snd.inner.get_ref().id(), None));
            handle_frame(&conn, peer_detach(4)).unwrap();
            sleep(Millis(50)).await;
            let _ = dispositions(&client);

            match case {
                0 => d.settle(rejected()),
                1 => d.update_state(rejected()),
                _ => (),
            }
            drop(d);
            sleep(Millis(50)).await;
            assert!(
                dispositions(&client).is_empty(),
                "case: {case} local: {local}"
            );
            assert!(
                session(&conn)
                    .inner
                    .get_ref()
                    .unsettled_snd_deliveries
                    .is_empty()
            );
        }

        // receiver link
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let Ok(Action::AttachReceiver(link, _, response)) = handle_frame(&conn, attach()) else {
            panic!()
        };
        link.confirm_receiver_link(response);
        link.set_link_credit(1);
        handle_frame(&conn, transfer(0, false, None, 0)).unwrap();
        let (mut d, _) = link.get_delivery().unwrap();
        handle_frame(&conn, peer_detach(0)).unwrap();
        sleep(Millis(50)).await;
        let _ = dispositions(&client);

        match case {
            0 => d.settle(rejected()),
            1 => d.update_state(rejected()),
            _ => (),
        }
        drop(d);
        sleep(Millis(50)).await;
        assert!(dispositions(&client).is_empty(), "case: {case}");
        assert!(
            session(&conn)
                .inner
                .get_ref()
                .unsettled_rcv_deliveries
                .is_empty()
        );
    }
}

type CloseFuture = Pin<Box<dyn Future<Output = Result<(), AmqpProtocolError>>>>;

/// Established link with unsettled delivery
async fn link_with_delivery(conn: &Connection, sender: bool) -> (crate::Delivery, CloseFuture) {
    if sender {
        let snd = add_sender(conn, "s", 4, 10);
        let d = snd.transfer(Bytes::from_static(b"x")).send().await.unwrap();
        (d, Box::pin(async move { snd.close().await }))
    } else {
        let Ok(Action::AttachReceiver(link, _, response)) = handle_frame(conn, attach()) else {
            panic!()
        };
        link.confirm_receiver_link(response);
        link.set_link_credit(1);
        handle_frame(conn, transfer(0, false, None, 0)).unwrap();
        let (d, _) = link.get_delivery().unwrap();
        (d, Box::pin(async move { link.close().await }))
    }
}

/// Local handle of new link
fn new_link_handle(conn: &Connection, sender: bool, remote: u32) -> u32 {
    if sender {
        add_sender(conn, "s2", remote, 10).id()
    } else {
        let Ok(Action::AttachReceiver(link, _, _)) =
            handle_frame(conn, named_attach(Role::Sender, "r2", "r2", remote))
        else {
            panic!()
        };
        link.handle()
    }
}

#[ntex::test]
async fn link_detach_timeout() {
    use ntex::time::{Millis, sleep, timeout};

    for sender in [true, false] {
        for cfg in [Seconds(1), Seconds::ZERO] {
            let ctx = format!("sender: {sender} timeout: {cfg:?}");
            let (_io, conn, client) =
                connection_with(AmqpServiceConfig::new().set_link_attach_timeout(cfg));
            handle_frame(&conn, begin()).unwrap();
            let (d, mut close) = link_with_delivery(&conn, sender).await;
            let mut wait = Box::pin(d.wait());
            let remote = if sender { 4 } else { 0 };

            // remote does not respond to detach
            assert!(timeout(Millis(500), &mut close).await.is_err(), "{ctx}");
            assert!(timeout(Millis(10), &mut wait).await.is_err(), "{ctx}");
            assert!(
                frame_names(&client).ends_with(&["Detach 0".to_string()]),
                "{ctx}"
            );
            if cfg.is_zero() {
                sleep(Millis(1000)).await;
                assert!(timeout(Millis(10), &mut close).await.is_err(), "{ctx}");
                assert!(timeout(Millis(10), &mut wait).await.is_err(), "{ctx}");
                continue;
            }

            let res = timeout(Millis(1500), &mut close).await.expect(&ctx);
            assert!(
                matches!(res, Err(AmqpProtocolError::LinkDetached(None))),
                "{ctx}: {res:?}"
            );
            let res = timeout(Millis(10), &mut wait).await.expect(&ctx);
            assert!(
                matches!(res, Err(AmqpProtocolError::LinkDetached(None))),
                "{ctx}: {res:?}"
            );

            // handle is in use until remote detach
            assert_eq!(new_link_handle(&conn, sender, 5), 1, "{ctx}");
            handle_frame(&conn, peer_detach(5)).unwrap();
            let Ok(Action::None) = handle_frame(&conn, peer_detach(remote)) else {
                panic!("{ctx}")
            };
            assert_eq!(new_link_handle(&conn, sender, 6), 0, "{ctx}");
        }
    }
}

#[ntex::test]
async fn link_detach_confirmation_fails_deliveries() {
    use ntex::time::{Millis, timeout};

    for sender in [true, false] {
        for error in [None, Some(LinkError::DetachForced)] {
            let ctx = format!("sender: {sender} error: {error:?}");
            let (_io, conn, _client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let (d, mut close) = link_with_delivery(&conn, sender).await;
            let mut wait = Box::pin(d.wait());
            let remote = if sender { 4 } else { 0 };

            assert!(timeout(Millis(10), &mut close).await.is_err(), "{ctx}");
            assert!(timeout(Millis(10), &mut wait).await.is_err(), "{ctx}");

            let err = error.map(|c| {
                Error(Box::new(codec::ErrorInner {
                    condition: c.into(),
                    description: None,
                    info: None,
                }))
            });
            let detach: Frame = Detach(Box::new(DetachInner {
                handle: remote,
                closed: true,
                error: err.clone(),
            }))
            .into();
            handle_frame(&conn, detach).unwrap();

            let res = timeout(Millis(10), &mut close).await.expect(&ctx);
            assert_eq!(res.is_ok(), err.is_none(), "{ctx}: {res:?}");
            let res = timeout(Millis(10), &mut wait).await.expect(&ctx);
            assert!(
                matches!(&res, Err(AmqpProtocolError::LinkDetached(e)) if *e == err),
                "{ctx}: {res:?}"
            );
        }
    }
}

#[ntex::test]
async fn link_detach_confirmed_before_timeout() {
    use ntex::time::{Millis, sleep, timeout};

    let (_io, conn, _client) =
        connection_with(AmqpServiceConfig::new().set_link_attach_timeout(Seconds(1)));
    handle_frame(&conn, begin()).unwrap();
    let (_d, mut close) = link_with_delivery(&conn, true).await;
    assert!(timeout(Millis(900), &mut close).await.is_err());
    handle_frame(&conn, peer_detach(4)).unwrap();
    timeout(Millis(10), &mut close).await.unwrap().unwrap();

    // confirmed detach does not expire link that reuses handle
    let snd = add_sender(&conn, "s2", 5, 10);
    assert_eq!(snd.id(), 0);
    let mut close = Box::pin(async move { snd.close().await });
    assert!(timeout(Millis(600), &mut close).await.is_err());
    handle_frame(&conn, peer_detach(5)).unwrap();
    timeout(Millis(10), &mut close).await.unwrap().unwrap();
    sleep(Millis(10)).await;
}

#[ntex::test]
async fn duplicate_delivery_id_detaches_link() {
    use ntex::time::{Millis, sleep};

    // second transfer is single frame or first frame of multi-frame delivery
    for more in [false, true] {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let fut = ntex::rt::spawn({
            let s = s.clone();
            async move { s.build_receiver_link("r", "r").attach().await }
        });
        sleep(Millis(10)).await;
        let _ = handle_frame(&conn, named_attach(Role::Sender, "r", "r", 0));
        let link = fut.await.unwrap().unwrap();
        link.set_link_credit(10);
        handle_frame(&conn, transfer(5, false, None, 1)).unwrap();
        sleep(Millis(50)).await;
        frame_names(&client);

        handle_frame(&conn, transfer(5, more, None, 2)).unwrap();
        sleep(Millis(50)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut detached = false;
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Detach(detach) = frame.into_parts().1 {
                let err = detach.error().unwrap();
                detached = *err.condition() == LinkError::DetachForced.into()
                    && err.description()
                        == Some(&ntex_bytes::ByteString::from_static(
                            "duplicate delivery_id",
                        ));
            }
        }
        assert!(detached);

        // original delivery is settled, duplicate is dropped
        let mut d = link.get_delivery().unwrap().0;
        assert!(link.get_delivery().is_none());
        assert_eq!(s.inner.get_ref().unsettled_rcv_deliveries.len(), 1);
        d.settle(DeliveryState::Accepted(Accepted {}));
        drop(d);
        sleep(Millis(50)).await;
        assert_eq!(
            dispositions(&client),
            ["Receiver 5 true Some(Accepted(Accepted))"]
        );
    }
}

#[ntex::test]
async fn continuation_transfer_settles_delivery() {
    use ntex::time::{Millis, sleep};

    // index of the continuation frame with `settled` flag
    for settled_frame in [1, 2] {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let fut = ntex::rt::spawn({
            let s = s.clone();
            async move { s.build_receiver_link("r", "r").attach().await }
        });
        sleep(Millis(10)).await;
        let _ = handle_frame(&conn, named_attach(Role::Sender, "r", "r", 0));
        let link = fut.await.unwrap().unwrap();
        link.set_link_credit(10);
        for idx in 0..3 {
            let mut frame = transfer(0, idx < 2, None, idx);
            if let Frame::Transfer(ref mut tr) = frame {
                if idx > 0 {
                    tr.0.delivery_id = None;
                }
                tr.0.settled = Some(idx == settled_frame);
            }
            handle_frame(&conn, frame).unwrap();
        }
        let mut d = link.recv().await.unwrap().unwrap().0;
        sleep(Millis(50)).await;
        frame_names(&client);

        assert!(d.is_remote_settled());
        d.settle(DeliveryState::Accepted(Accepted {}));
        drop(d);
        assert!(s.inner.get_ref().unsettled_rcv_deliveries.is_empty());
        sleep(Millis(50)).await;
        assert!(dispositions(&client).is_empty());
    }
}

#[ntex::test]
async fn sender_delivery_settled_mid_transfer() {
    use ntex::time::{Millis, sleep};
    use std::{future::poll_fn, task::Poll};

    // (receiver settles delivery, last frame)
    let cases = [
        (true, "Transfer Some(0) more:false aborted:true"),
        (false, "Transfer None more:false aborted:false"),
    ];
    for (settled, last) in cases {
        let (_io, conn, client, snd) = small_frames_sender();
        sleep(Millis(50)).await;
        transfer_frames(&client);

        let mut t1 = Box::pin(snd.transfer(Bytes::from(vec![b'a'; 1200])).send());
        assert!(poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx).is_pending())).await);
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            [
                "Transfer Some(0) more:true aborted:false",
                "Transfer None more:true aborted:false"
            ]
        );

        // remaining frames are aborted if receiver settled delivery
        remote_disposition(&conn, Role::Receiver, 0, settled);
        session_window(&conn, 2, 5);
        let Poll::Ready(Ok(d)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
            panic!("settled: {settled}")
        };
        sleep(Millis(50)).await;
        assert_eq!(transfer_frames(&client), [last], "settled: {settled}");
        assert_eq!(d.is_remote_settled(), settled);
        assert!(matches!(
            d.wait().await,
            Ok(Some(DeliveryState::Accepted(_)))
        ));

        // link is usable for next delivery
        let d = snd.transfer(Bytes::from_static(b"2")).send().await.unwrap();
        assert_eq!(d.id(), 1);
        sleep(Millis(50)).await;
        assert_eq!(
            transfer_frames(&client),
            ["Transfer Some(1) more:false aborted:false"]
        );
    }
}
