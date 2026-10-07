use super::*;

#[ntex::test]
async fn receiver_delivery_count_wraps() {
    // overflow on single and on multi-frame transfer
    receiver_delivery_count(false).await;
    receiver_delivery_count(true).await;
}

async fn receiver_delivery_count(multi_first: bool) {
    let remote = RemoteServiceConfig::new(&Open(Box::default()));
    let (server, client) = IoTest::create();
    client.remote_buffer_cap(64 * 1024);
    let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
    let io = Io::new(server, cfg.clone());
    let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));

    handle(begin()).unwrap();
    let Frame::Attach(mut attach) = attach() else {
        panic!()
    };
    attach.0.initial_delivery_count = Some(u32::MAX);
    let Ok(Action::AttachReceiver(link, _, response)) = handle(attach.into()) else {
        panic!()
    };
    link.confirm_receiver_link(response);
    link.set_link_credit(10);

    for id in 0..2 {
        if (id == 0) == multi_first {
            handle(transfer(id, true, None, 0)).unwrap();
        }
        handle(transfer(id, false, None, 0)).unwrap();
        link.set_link_credit(1);
    }

    ntex::time::sleep(ntex::time::Millis(50)).await;
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    let mut counts = Vec::new();
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        if let Frame::Flow(flow) = frame.into_parts().1 {
            counts.push(flow.delivery_count());
        }
    }
    assert_eq!(counts, [Some(u32::MAX), Some(0), Some(1)]);
}

#[ntex::test]
async fn receiver_aborted_transfer() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let Ok(Action::AttachReceiver(link, _, response)) = handle_frame(&conn, attach()) else {
        panic!()
    };
    link.confirm_receiver_link(response);
    link.set_link_credit(1);
    let aborted = |id: Option<u32>, more: bool| {
        let Frame::Transfer(mut tr) = transfer(0, more, None, 9) else {
            panic!()
        };
        tr.0.delivery_id = id;
        tr.0.aborted = true;
        handle_frame(&conn, tr.into()).unwrap()
    };

    // multi-frame delivery aborted by last frame
    handle_frame(&conn, transfer(0, true, None, 0)).unwrap();
    assert!(matches!(aborted(Some(0), false), Action::None));
    // multi-frame delivery aborted with `more` set
    handle_frame(&conn, transfer(1, true, None, 1)).unwrap();
    assert!(matches!(aborted(None, true), Action::None));
    // single-frame aborted delivery, delivery-id is not required
    assert!(matches!(aborted(None, false), Action::None));
    assert!(matches!(aborted(Some(2), true), Action::None));

    // aborted deliveries are discarded and implicitly settled,
    // credit is returned to the sender
    assert!(!link.has_deliveries());
    assert!(
        session(&conn)
            .inner
            .get_ref()
            .unsettled_rcv_deliveries
            .is_empty()
    );
    assert_eq!(link.credit(), 1);

    // next delivery is not merged into aborted one
    let Ok(Action::Transfer(_)) = handle_frame(&conn, transfer(3, false, None, 3)) else {
        panic!()
    };
    let (delivery, tr) = link.get_delivery().unwrap();
    assert_eq!(delivery.id(), 3);
    assert_eq!(
        tr.body(),
        Some(&TransferBody::Data(Bytes::from(vec![3; 10])))
    );
    drop(delivery);
    assert_eq!(link.credit(), 0);

    // delivery-count includes aborted deliveries
    link.set_link_credit(1);
    ntex::time::sleep(ntex::time::Millis(10)).await;
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    let mut frames = Vec::new();
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        match frame.into_parts().1 {
            Frame::Flow(flow) => frames.push(format!(
                "Flow {:?} {:?}",
                flow.delivery_count(),
                flow.link_credit()
            )),
            Frame::Disposition(disp) => frames.push(format!("Disposition {}", disp.first())),
            frame => frames.push(frame.name().to_string()),
        }
    }
    assert_eq!(
        frames,
        [
            "Begin",
            "Attach",
            "Flow Some(0) Some(1)",
            "Flow Some(1) Some(1)",
            "Flow Some(2) Some(1)",
            "Flow Some(3) Some(1)",
            "Flow Some(4) Some(1)",
            "Disposition 3",
            "Flow Some(5) Some(1)",
        ]
    );

    // wrong delivery-id in aborted transfer
    handle_frame(&conn, transfer(4, true, None, 4)).unwrap();
    aborted(Some(5), false);
    ntex::time::sleep(ntex::time::Millis(10)).await;
    assert_eq!(frame_names(&client), ["Detach 0"]);
}

#[ntex::test]
async fn local_receiver_delivery_count() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let s = session(&conn);
    let fut = ntex::rt::spawn(async move { s.build_receiver_link("r", "r").attach().await });
    ntex::time::sleep(ntex::time::Millis(10)).await;

    // remote sender initializes delivery-count
    let Frame::Attach(mut attach) = named_attach(Role::Sender, "r", "r", 0) else {
        panic!()
    };
    attach.0.initial_delivery_count = Some(100);
    let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
        panic!()
    };
    let link = fut.await.unwrap().unwrap();
    link.set_link_credit(10);
    handle_frame(&conn, transfer(0, false, None, 0)).unwrap();
    link.set_link_credit(1);

    ntex::time::sleep(ntex::time::Millis(10)).await;
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    let mut flows = Vec::new();
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        if let Frame::Flow(flow) = frame.into_parts().1 {
            flows.push((flow.delivery_count(), flow.link_credit()));
        }
    }
    assert_eq!(flows, [(Some(100), Some(10)), (Some(101), Some(10))]);
}

#[ntex::test]
async fn receiver_remote_detach_error() {
    let err = Error(Box::new(codec::ErrorInner {
        condition: AmqpError::InternalError.into(),
        description: None,
        info: None,
    }));
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let s = session(&conn);
    let fut = ntex::rt::spawn({
        let s = s.clone();
        async move { s.build_receiver_link("r", "r").attach().await }
    });
    ntex::time::sleep(ntex::time::Millis(10)).await;
    let Ok(Action::None) = handle_frame(&conn, named_attach(Role::Sender, "r", "r", 1)) else {
        panic!()
    };
    let link = fut.await.unwrap().unwrap();

    let detach = Detach(Box::new(DetachInner {
        handle: 1,
        closed: true,
        error: Some(err.clone()),
    }));
    let Ok(Action::DetachReceiver(..)) = handle_frame(&conn, detach.into()) else {
        panic!()
    };
    assert_eq!(link.error(), Some(&err));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let Poll::Ready(Some(Err(AmqpProtocolError::LinkDetached(Some(e))))) = link.poll_recv(&mut cx)
    else {
        panic!()
    };
    assert_eq!(e, err);
    assert!(matches!(link.poll_recv(&mut cx), Poll::Ready(None)));
}

#[ntex::test]
async fn receiver_link_detach_wakes_recv() {
    use ntex::time::{Millis, sleep, timeout};

    for queued in [false, true] {
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
        if queued {
            handle_frame(&conn, transfer(0, false, None, 1)).unwrap();
        }
        let rx = ntex::rt::spawn({
            let link = link.clone();
            async move {
                let mut items = Vec::new();
                while let Some(item) = link.recv().await {
                    items.push(item.map(|(_, tr)| tr.delivery_id()));
                }
                items
            }
        });
        sleep(Millis(50)).await;
        frame_names(&client);

        let _d = s.detach_receiver_link(link.inner.get_ref().id(), None);
        let items = timeout(Millis(500), rx).await.unwrap().unwrap();
        if queued {
            assert!(matches!(items[..], [Ok(Some(0))]));
        } else {
            assert!(items.is_empty());
        }
        assert!(link.is_closed());
        sleep(Millis(50)).await;
        assert_eq!(frame_names(&client), ["Detach 0"]);
    }
}

#[ntex::test]
async fn receiver_max_message_size() {
    // remotely attached link, max size is set by `set_max_message_size`
    assert_eq!(receive(false, 0, &[true, true, false]).await, (true, false));
    assert_eq!(
        receive(false, 30, &[true, true, false]).await,
        (true, false)
    );
    assert_eq!(
        receive(false, 25, &[true, true, false]).await,
        (false, true)
    );
    assert_eq!(receive(false, 5, &[true]).await, (false, true));
    assert_eq!(receive(false, 10, &[false]).await, (true, false));
    assert_eq!(receive(false, 5, &[false]).await, (false, true));

    // locally attached link, max size is set by link builder
    assert_eq!(receive(true, 0, &[true, true, false]).await, (true, false));
    assert_eq!(receive(true, 25, &[true, true, false]).await, (false, true));
}

#[ntex::test]
async fn receiver_partial_delivery_and_detach() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let Ok(Action::AttachReceiver(link, _, response)) = handle_frame(&conn, attach()) else {
        panic!()
    };
    link.confirm_receiver_link(response);
    link.set_link_credit(10);
    assert_eq!(format!("{link:?}"), format!("ReceiverLink({LONG:?})"));

    let mut cx = Context::from_waker(std::task::Waker::noop());

    // partial delivery is not visible
    handle_frame(&conn, transfer(0, true, None, 0)).unwrap();
    assert!(!link.has_deliveries());
    assert!(link.get_delivery().is_none());
    assert!(link.poll_recv(&mut cx).is_pending());

    // completed delivery
    handle_frame(&conn, transfer(0, false, None, 1)).unwrap();
    assert!(link.has_deliveries());
    let Poll::Ready(Some(Ok((delivery, _)))) = link.poll_recv(&mut cx) else {
        panic!()
    };
    assert_eq!(delivery.id(), 0);
    drop(delivery);
    assert!(!link.has_deliveries());

    // completed delivery is visible while next one is partial
    handle_frame(&conn, transfer(1, false, None, 2)).unwrap();
    handle_frame(&conn, transfer(2, true, None, 3)).unwrap();
    assert!(link.has_deliveries());
    assert_eq!(link.get_delivery().unwrap().0.id(), 1);
    assert!(!link.has_deliveries());

    // remote detach with error, partial delivery is dropped
    let error = Error(Box::new(crate::codec::protocol::ErrorInner {
        condition: LinkError::DetachForced.into(),
        description: None,
        info: None,
    }));
    let detach = Detach(Box::new(DetachInner {
        handle: 0,
        closed: true,
        error: Some(error.clone()),
    }));
    handle_frame(&conn, detach.into()).unwrap();
    assert!(link.is_closed());
    assert_eq!(link.error(), Some(&error));
    assert!(!link.has_deliveries());
    let Poll::Ready(Some(Err(AmqpProtocolError::LinkDetached(Some(err))))) =
        link.poll_recv(&mut cx)
    else {
        panic!()
    };
    assert_eq!(err, error);
    assert!(matches!(link.poll_recv(&mut cx), Poll::Ready(None)));
}

#[ntex::test]
async fn receiver_applies_sender_flow() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let Ok(Action::AttachReceiver(link, _, response)) = handle_frame(&conn, attach()) else {
        panic!()
    };
    link.confirm_receiver_link(response);
    link.set_link_credit(10);
    // sender's link-credit is its own view, receiver uses delivery-count only
    let sender_flow = |delivery_count: u32, echo: bool| {
        let Frame::Flow(mut flow) = peer_flow(Some(100)) else {
            panic!()
        };
        flow.0.delivery_count = Some(delivery_count);
        flow.0.echo = echo;
        handle_frame(&conn, flow.into()).unwrap();
        link.credit()
    };
    let flows = || {
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut flows = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Flow(flow) = frame.into_parts().1 {
                flows.push((flow.delivery_count(), flow.link_credit()));
            }
        }
        flows
    };

    handle_frame(&conn, transfer(0, false, None, 0)).unwrap();
    assert_eq!(link.credit(), 9);

    // sender ahead, delivery-limit 10 is preserved
    assert_eq!(sender_flow(3, false), 7);
    // sender behind
    assert_eq!(sender_flow(1, false), 9);
    // sender beyond delivery-limit
    assert_eq!(sender_flow(12, false), 0);
    link.set_link_credit(5);

    // sender counts multi-frame delivery at first frame
    handle_frame(&conn, transfer(1, true, None, 0)).unwrap();
    assert_eq!(sender_flow(13, false), 5);
    handle_frame(&conn, transfer(1, false, None, 0)).unwrap();
    assert_eq!(link.credit(), 4);

    // echo reply carries applied state
    assert_eq!(sender_flow(15, true), 2);

    ntex::time::sleep(ntex::time::Millis(50)).await;
    assert_eq!(
        flows(),
        [
            (Some(0), Some(10)),
            (Some(12), Some(5)),
            (Some(15), Some(2))
        ]
    );
}

#[ntex::test]
async fn receiver_transfer_error_detaches_link() {
    let no_id = |more: bool| {
        let Frame::Transfer(mut tr) = transfer(0, more, None, 0) else {
            panic!()
        };
        tr.0.delivery_id = None;
        Frame::from(tr)
    };
    let cases = [
        (
            0,
            transfer(0, false, None, 0),
            LinkError::TransferLimitExceeded,
        ),
        (1, no_id(false), LinkError::DetachForced),
        (1, no_id(true), LinkError::DetachForced),
    ];
    for (credit, frame, condition) in cases {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let Ok(Action::AttachReceiver(link, _, response)) = handle_frame(&conn, attach()) else {
            panic!()
        };
        link.confirm_receiver_link(response);
        link.set_link_credit(credit);

        assert!(matches!(handle_frame(&conn, frame), Ok(Action::None)));
        assert!(link.is_closed());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(link.poll_recv(&mut cx), Poll::Ready(None)));
        // link is closing, transfers are ignored
        assert!(matches!(
            handle_frame(&conn, transfer(1, false, None, 0)),
            Ok(Action::None)
        ));

        ntex::time::sleep(ntex::time::Millis(10)).await;
        let codec = AmqpCodec::<AmqpFrame>::new();
        let mut buf = BytesMut::from(&client.read_any()[..]);
        let mut detaches = Vec::new();
        while let Some(frame) = codec.decode(&mut buf).unwrap() {
            if let Frame::Detach(det) = frame.into_parts().1 {
                detaches.push((det.handle(), det.closed(), det.error().cloned()));
            }
        }
        let [(0, true, Some(err))] = &detaches[..] else {
            panic!("{detaches:?}")
        };
        assert_eq!(err.condition(), &condition.into());
    }
}
