use super::*;

#[ntex::test]
async fn session_end_error_propagated() {
    let err = Error(Box::new(codec::ErrorInner {
        condition: AmqpError::InternalError.into(),
        description: None,
        info: None,
    }));
    let ended: [(Frame, AmqpProtocolError); 2] = [
        (
            End {
                error: Some(err.clone()),
            }
            .into(),
            AmqpProtocolError::SessionEnded(Some(err.clone())),
        ),
        (
            Close {
                error: Some(err.clone()),
            }
            .into(),
            AmqpProtocolError::Closed(Some(err.clone())),
        ),
    ];
    for (frame, expected) in ended {
        let ctx = format!("{expected:?}");
        let expected = Some(format!("{expected:?}"));
        let same = |res: Result<(), AmqpProtocolError>| res.err().map(|e| format!("{e:?}"));
        let (_io, conn, _client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let mut cx = Context::from_waker(Waker::noop());

        // established and closing receivers
        let mut links = Vec::new();
        for (name, handle) in [("e", 1), ("c", 2)] {
            let fut = ntex::rt::spawn({
                let s = s.clone();
                async move { s.build_receiver_link(name, name).attach().await }
            });
            sleep(Millis(10)).await;
            let Ok(Action::None) =
                handle_frame(&conn, named_attach(Role::Sender, name, name, handle))
            else {
                panic!("{ctx}")
            };
            links.push(fut.await.unwrap().unwrap());
        }
        let mut close = Box::pin(links[1].close());
        assert!(close.as_mut().poll(&mut cx).is_pending(), "{ctx}");

        // pending local attaches
        let mut snd = Box::pin(attach_with_timeout(s.clone(), true, None));
        assert!(snd.as_mut().poll(&mut cx).is_pending(), "{ctx}");
        let mut rcv = Box::pin(attach_with_timeout(s.clone(), false, None));
        assert!(rcv.as_mut().poll(&mut cx).is_pending(), "{ctx}");

        // unconfirmed remote receiver
        let Ok(Action::AttachReceiver(remote, ..)) =
            handle_frame(&conn, named_attach(Role::Sender, "u", "u", 9))
        else {
            panic!("{ctx}")
        };

        handle_frame(&conn, frame).unwrap();

        let Poll::Ready(res) = snd.as_mut().poll(&mut cx) else {
            panic!("{ctx}")
        };
        assert_eq!(same(res), expected, "{ctx}");
        let Poll::Ready(res) = rcv.as_mut().poll(&mut cx) else {
            panic!("{ctx}")
        };
        assert_eq!(same(res), expected, "{ctx}");
        let Poll::Ready(res) = close.as_mut().poll(&mut cx) else {
            panic!("{ctx}")
        };
        assert_eq!(same(res), expected, "{ctx}");
        for link in [&links[0], &remote] {
            assert!(link.error().is_none(), "{ctx}");
            let Poll::Ready(Some(Err(e))) = link.poll_recv(&mut cx) else {
                panic!("{ctx}")
            };
            assert_eq!(same(Err(e)), expected, "{ctx}");
            assert!(
                matches!(link.poll_recv(&mut cx), Poll::Ready(None)),
                "{ctx}"
            );
        }
    }
}

#[ntex::test]
async fn session_end_uses_local_channel() {
    let (_io, conn, client) = connection();
    let inner = conn.get_ref();
    let handle = |channel: u16, frame: Frame| inner.handle_frame(AmqpFrame::new(channel, frame));

    // local and remote channel ids are different
    handle(1, begin()).unwrap();
    handle(0, begin()).unwrap();
    let session = conn.get_ref().get_session_by_local_id(0).unwrap();
    assert_eq!(session.remote_channel_id(), 1);

    let fut = ntex::rt::spawn(async move { session.end().await });
    sleep(Millis(10)).await;
    assert!(matches!(
        inner.0.get_ref().sessions[0],
        SessionState::Closing(_)
    ));
    assert!(matches!(
        inner.0.get_ref().sessions[1],
        SessionState::Established(_)
    ));

    // other session handles frames
    let flow = Flow(Box::new(FlowInner {
        next_incoming_id: Some(1),
        incoming_window: 100,
        next_outgoing_id: 1,
        outgoing_window: 100,
        echo: true,
        ..Default::default()
    }));
    handle(0, flow.into()).unwrap();

    // remote confirms session end
    handle(1, End { error: None }.into()).unwrap();
    assert!(fut.await.unwrap().is_ok());
    assert!(!inner.0.get_ref().sessions.contains(0));
    assert!(inner.0.get_ref().sessions.contains(1));

    sleep(Millis(50)).await;
    let frames: Vec<_> = read_channel_frames(&client)
        .into_iter()
        .filter_map(|frame| match frame {
            (ch, Frame::End(_)) => Some(("end", ch)),
            (ch, Frame::Flow(_)) => Some(("flow", ch)),
            _ => None,
        })
        .collect();
    assert_eq!(frames, [("end", 0), ("flow", 1)]);
}

#[ntex::test]
async fn session_flow_window() {
    let (_io, conn, client, snd) = small_frames_sender();
    let session = session(&conn);
    sleep(Millis(50)).await;
    transfer_frames(&client);
    assert_eq!(session.remote_window_size(), 2);
    for _ in 0..2 {
        let t = snd.transfer(Bytes::from_static(b"1")).settled().send();
        assert!(timeout(Millis(500), t).await.unwrap().is_ok());
    }
    assert_eq!(session.remote_window_size(), 0);

    // flow issued before remote got sent transfers
    session_window(&conn, 1, 1);
    assert_eq!(session.remote_window_size(), 0);
    let mut t = Box::pin(snd.transfer(Bytes::from_static(b"1")).settled().send());
    assert!(pending(t.as_mut()).await);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        [
            "Transfer Some(0) more:false aborted:false",
            "Transfer Some(1) more:false aborted:false"
        ]
    );
    session_window(&conn, 1, 3);
    assert!(timeout(Millis(500), t).await.unwrap().is_ok());
    assert_eq!(session.remote_window_size(), 0);

    // remote has not received begin yet, initial outgoing id is used
    let Frame::Flow(mut flow) = peer_flow(None) else {
        panic!()
    };
    flow.0.handle = None;
    flow.0.next_incoming_id = None;
    flow.0.incoming_window = 5;
    handle_frame(&conn, flow.into()).unwrap();
    assert_eq!(session.remote_window_size(), 2);

    // remote next-incoming-id is ahead, window is used as is
    session_window(&conn, 10, 3);
    assert_eq!(session.remote_window_size(), 3);
}

#[ntex::test]
async fn session_outgoing_window() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();

    let Ok(Action::AttachReceiver(rcv, _, response)) =
        handle_frame(&conn, named_attach(Role::Sender, "r", "r", 3))
    else {
        panic!()
    };
    assert!(rcv.confirm_receiver_link(response));
    rcv.set_link_credit(10);

    let Frame::Flow(mut flow) = peer_flow(None) else {
        panic!()
    };
    flow.0.handle = None;
    flow.0.incoming_window = 7;
    flow.0.echo = true;
    handle_frame(&conn, flow.into()).unwrap();
    sleep(Millis(50)).await;

    // local outgoing window is not limited, peer windows are not echoed
    let windows: Vec<_> = read_frames(&client)
        .into_iter()
        .filter_map(|frame| match frame {
            Frame::Begin(begin) => Some(("Begin", begin.outgoing_window())),
            Frame::Flow(flow) => Some(("Flow", flow.outgoing_window())),
            _ => None,
        })
        .collect();
    assert_eq!(
        windows,
        [("Begin", u32::MAX), ("Flow", u32::MAX), ("Flow", u32::MAX)]
    );
}

#[ntex::test]
async fn closing_session_connection_error() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);

    // local links wait for attach response
    let s = session.clone();
    let rcv = ntex::rt::spawn(async move { s.build_receiver_link("r", "r").attach().await });
    let s = session.clone();
    let snd = ntex::rt::spawn(async move { s.build_sender_link("s", "s").attach().await });
    sleep(Millis(10)).await;

    // local end waits for remote end
    let s = session.clone();
    let end = ntex::rt::spawn(async move { s.end().await });
    sleep(Millis(10)).await;
    assert!(!end.is_finished());

    conn.get_ref()
        .0
        .get_mut()
        .set_error(AmqpProtocolError::Disconnected);
    assert!(matches!(
        timeout(Millis(100), rcv).await.unwrap().unwrap(),
        Err(AmqpProtocolError::Disconnected)
    ));
    assert!(matches!(
        timeout(Millis(100), snd).await.unwrap().unwrap(),
        Err(AmqpProtocolError::Disconnected)
    ));
    timeout(Millis(100), end).await.unwrap().unwrap().unwrap();

    // session ended with error keeps the error
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = super::session(&conn);
    let Ok(Action::AttachReceiver(..)) =
        handle_frame(&conn, named_attach(Role::Sender, "a", "a", 3))
    else {
        panic!()
    };
    let Ok(Action::SessionEnded(_)) = handle_frame(&conn, named_attach(Role::Sender, "b", "b", 3))
    else {
        panic!()
    };
    conn.get_ref()
        .0
        .get_mut()
        .set_error(AmqpProtocolError::Disconnected);
    let err = session
        .build_receiver_link("c", "c")
        .attach()
        .await
        .unwrap_err();
    assert!(
        matches!(err, AmqpProtocolError::SessionEnded(Some(ref e)) if e.condition() == &SessionError::HandleInUse.into()),
        "{err:?}"
    );
}

#[ntex::test]
async fn session_ending_sends_no_frames() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);

    let Ok(Action::AttachReceiver(rcv, _, response)) =
        handle_frame(&conn, named_attach(Role::Sender, "r", "r", 3))
    else {
        panic!()
    };
    assert!(rcv.confirm_receiver_link(response));
    rcv.set_link_credit(10);
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
    let Frame::Flow(mut flow) = peer_flow(Some(10)) else {
        panic!()
    };
    flow.0.handle = Some(4);
    handle_frame(&conn, flow.into()).unwrap();
    assert_eq!(snd.credit(), 10);
    sleep(Millis(50)).await;
    assert_eq!(
        frame_names(&client),
        ["Begin", "Attach r 0", "Flow Some(0) Some(10)", "Attach s 1"]
    );

    let s = session.clone();
    let end = ntex::rt::spawn(async move { s.end().await });
    sleep(Millis(10)).await;

    // new operations fail, links are closed without frames
    let tr = timeout(Millis(500), snd.transfer(Bytes::from_static(b"m")).send());
    assert!(tr.await.unwrap().is_err());
    let attach = timeout(Millis(500), session.build_sender_link("a", "a").attach());
    assert!(attach.await.unwrap().is_err());
    let attach = timeout(Millis(500), session.build_receiver_link("b", "b").attach());
    assert!(attach.await.unwrap().is_err());
    rcv.set_link_credit(5);
    assert!(timeout(Millis(500), rcv.close()).await.unwrap().is_ok());
    assert!(timeout(Millis(500), snd.close()).await.unwrap().is_ok());
    sleep(Millis(50)).await;
    assert_eq!(frame_names(&client), ["End"]);

    handle_frame(&conn, End { error: None }.into()).unwrap();
    assert!(end.await.unwrap().is_ok());
}

#[ntex::test]
async fn session_window_claimed_by_woken_transfers() {
    let (_io, conn, client, snd) = small_frames_sender();
    let snd2 = add_sender(&conn, "s2", 5, 0);
    let session = session(&conn);
    let woken = || session.inner.get_ref().window_woken;
    sleep(Millis(50)).await;
    transfer_frames(&client);

    let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).send());
    let mut t2 = Box::pin(snd2.transfer(Bytes::from_static(b"2")).send());
    let mut t3 = Box::pin(snd.transfer(Bytes::from_static(b"3")).send());
    assert!(pending(t1.as_mut()).await);
    assert!(pending(t2.as_mut()).await);
    assert!(pending(t3.as_mut()).await);

    // flow wakes transfers up to the window
    session_window(&conn, 1, 1);
    assert_eq!(woken(), 1);
    assert!(pending(t3.as_mut()).await);
    assert!(pending(t2.as_mut()).await);

    // new transfers do not take window of woken transfer
    let mut t4 = Box::pin(snd.transfer(Bytes::from_static(b"4")).send());
    let mut t5 = Box::pin(snd2.transfer(Bytes::from_static(b"5")).send());
    assert!(pending(t4.as_mut()).await);
    assert!(pending(t5.as_mut()).await);
    let Poll::Ready(Ok(d1)) = poll_fn(|cx| Poll::Ready(t1.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d1.id(), 0);
    assert_eq!(woken(), 0);

    // waiters are woken in order
    session_window(&conn, 2, 1);
    assert!(pending(t3.as_mut()).await);
    let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d2.id(), 1);

    session_window(&conn, 3, 3);
    assert_eq!(woken(), 3);
    for t in [&mut t5, &mut t4, &mut t3] {
        let Poll::Ready(Ok(_)) = poll_fn(|cx| Poll::Ready(t.as_mut().poll(cx))).await else {
            panic!()
        };
    }
    assert_eq!(woken(), 0);
    assert_eq!(session.remote_window_size(), 0);
    sleep(Millis(50)).await;
    let transfers: Vec<_> = transfer_frames(&client)
        .into_iter()
        .filter(|f| f.starts_with("Transfer"))
        .collect();
    assert_eq!(
        transfers,
        (0..5)
            .map(|id| format!("Transfer Some({id}) more:false aborted:false"))
            .collect::<Vec<_>>()
    );
}

#[ntex::test]
async fn woken_window_waiter_releases_claim() {
    let (_io, conn, client, snd) = small_frames_sender();
    let snd2 = add_sender(&conn, "s2", 5, 0);
    let session = session(&conn);
    let woken = || session.inner.get_ref().window_woken;
    sleep(Millis(50)).await;
    transfer_frames(&client);

    // dropped waiter is skipped
    let mut t0 = Box::pin(snd2.transfer(Bytes::from_static(b"0")).send());
    assert!(pending(t0.as_mut()).await);
    drop(t0);

    // woken transfer is dropped before it resumes
    let mut t1 = Box::pin(snd.transfer(Bytes::from_static(b"1")).send());
    let mut t2 = Box::pin(snd2.transfer(Bytes::from_static(b"2")).send());
    assert!(pending(t1.as_mut()).await);
    assert!(pending(t2.as_mut()).await);
    session_window(&conn, 1, 1);
    drop(t1);
    assert_eq!(woken(), 1);
    let Poll::Ready(Ok(d2)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d2.id(), 0);
    assert_eq!(woken(), 0);

    // woken transfer has no link credit
    let mut t3 = Box::pin(snd.transfer(Bytes::from_static(b"3")).send());
    let mut t4 = Box::pin(snd2.transfer(Bytes::from_static(b"4")).send());
    assert!(pending(t3.as_mut()).await);
    assert!(pending(t4.as_mut()).await);
    link_flow(&conn, 4, (0, 10, true), 2, 0);
    assert_eq!(snd.credit(), 0);
    session_window(&conn, 2, 1);
    assert!(pending(t4.as_mut()).await);
    assert!(pending(t3.as_mut()).await);
    assert_eq!(woken(), 1);
    let Poll::Ready(Ok(d4)) = poll_fn(|cx| Poll::Ready(t4.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d4.id(), 1);
    assert_eq!(woken(), 0);

    // window is gone before woken transfer resumes
    let mut t5 = Box::pin(snd2.transfer(Bytes::from_static(b"5")).send());
    assert!(pending(t5.as_mut()).await);
    session_window(&conn, 3, 1);
    session_window(&conn, 3, 0);
    assert!(pending(t5.as_mut()).await);
    assert_eq!(woken(), 0);
    session_window(&conn, 3, 1);
    let Poll::Ready(Ok(d5)) = poll_fn(|cx| Poll::Ready(t5.as_mut().poll(cx))).await else {
        panic!()
    };
    assert_eq!(d5.id(), 2);
    sleep(Millis(50)).await;
    assert_eq!(
        transfer_frames(&client),
        [
            "Transfer Some(0) more:false aborted:false",
            "Flow 2",
            "Transfer Some(1) more:false aborted:false",
            "Transfer Some(2) more:false aborted:false"
        ]
    );
}

#[ntex::test]
async fn session_end_drops_queued_abort() {
    let (_io, conn, client, _snd, mut t2) = queued_abort_sender().await;
    let s = session(&conn);
    // next transfer and abort of cancelled delivery
    assert_eq!(s.inner.get_ref().pending_transfers(), 2);
    handle_frame(&conn, End { error: None }.into()).unwrap();
    assert_eq!(s.inner.get_ref().pending_transfers(), 0);
    let Poll::Ready(Err(_)) = poll_fn(|cx| Poll::Ready(t2.as_mut().poll(cx))).await else {
        panic!()
    };
    sleep(Millis(50)).await;
    assert!(
        !transfer_frames(&client)
            .iter()
            .any(|f| f.starts_with("Transfer"))
    );
}
