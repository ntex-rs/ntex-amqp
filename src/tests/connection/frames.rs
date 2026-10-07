use super::*;

/// Decode frame from a read buffer, returns frame and the buffer
fn read(frame: Frame) -> (Frame, BytesMut) {
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut pages = BytePages::default();
    codec.encode(AmqpFrame::new(0, frame), &mut pages).unwrap();
    let mut buf = BytesMut::with_capacity(16 * 1024);
    buf.extend_from_slice(&pages.freeze());
    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert!(buf.is_empty() && !buf.is_unique());
    (frame.into_parts().1, buf)
}

#[ntex::test]
async fn frames_detached_from_read_buffer() {
    // remote open
    let open = Open(Box::new(OpenInner {
        container_id: LONG.into(),
        hostname: Some(LONG.into()),
        max_frame_size: 1024,
        channel_max: 10,
        idle_time_out: None,
        outgoing_locales: None,
        incoming_locales: None,
        offered_capabilities: Some(symbols()),
        desired_capabilities: Some(symbols()),
        properties: None,
    }));
    let (Frame::Open(open), buf) = read(open.into()) else {
        panic!()
    };
    let remote = RemoteServiceConfig::new(&open);
    drop(open);
    assert!(buf.is_unique(), "open");

    let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
    let io = Io::new(IoTest::create().0, cfg.clone());
    let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));

    // remote begin
    let (frame, buf) = read(begin());
    handle(frame).unwrap();
    assert!(buf.is_unique(), "begin");

    // remote attach
    let (frame, buf) = read(attach());
    let Ok(Action::AttachReceiver(link, _, response)) = handle(frame) else {
        panic!()
    };
    assert!(buf.is_unique(), "attach");
    link.confirm_receiver_link(response);
    link.set_link_credit(10);

    // partial transfers
    for (idx, more) in [true, true, false].into_iter().enumerate() {
        let (frame, buf) = read(transfer(0, more, (idx == 0).then(rejected), idx as u8));
        handle(frame).unwrap();
        assert!(buf.is_unique(), "transfer {idx}");
    }
    let (delivery, transfer) = link.get_delivery().unwrap();
    assert_eq!(delivery.tag(), LONG.as_bytes());
    let Some(TransferBody::Data(body)) = transfer.body() else {
        panic!()
    };
    assert_eq!(body, &[[0; 10], [1; 10], [2; 10]].concat());

    // disposition with state
    let session = inner.0.get_ref().sessions_map[&0];
    let SessionState::Established(session) = &inner.0.get_ref().sessions[session] else {
        panic!()
    };
    session
        .get_mut()
        .unsettled_snd_deliveries
        .insert(0, DeliveryInner::new(0));
    let disp = Disposition(Box::new(DispositionInner {
        role: Role::Receiver,
        first: 0,
        last: None,
        settled: false,
        state: Some(rejected()),
        batchable: false,
    }));
    let (frame, buf) = read(disp.into());
    handle(frame).unwrap();
    assert!(buf.is_unique(), "disposition");
}

#[ntex::test]
async fn transfers_advance_next_incoming_id() {
    let remote = RemoteServiceConfig::new(&Open(Box::default()));
    let (server, client) = IoTest::create();
    client.remote_buffer_cap(64 * 1024);
    let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
    let io = Io::new(server, cfg.clone());
    let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));

    handle(begin()).unwrap();
    let Ok(Action::AttachReceiver(link, _, response)) = handle(attach()) else {
        panic!()
    };
    link.confirm_receiver_link(response);
    link.set_link_credit(10);
    for id in 0..3 {
        handle(transfer(id, false, None, 0)).unwrap();
    }
    handle(transfer(3, true, None, 0)).unwrap();
    link.set_link_credit(5);
    assert_eq!(link.credit(), 12);

    // session flow with echo, reply carries next-incoming-id
    let flow = Flow(Box::new(FlowInner {
        next_incoming_id: Some(1),
        incoming_window: 100,
        next_outgoing_id: 5,
        outgoing_window: 100,
        echo: true,
        ..Default::default()
    }));
    handle(flow.into()).unwrap();

    ntex::time::sleep(ntex::time::Millis(50)).await;
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    let mut flows = Vec::new();
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        if let Frame::Flow(flow) = frame.into_parts().1 {
            flows.push((flow.next_incoming_id(), flow.link_credit()));
        }
    }
    assert_eq!(
        flows,
        [(Some(1), Some(10)), (Some(5), Some(12)), (Some(5), None)]
    );
}

#[ntex::test]
async fn outbound_frames_limited_by_remote_max_frame_size() {
    let remote = RemoteServiceConfig::new(&Open(Box::new(OpenInner {
        max_frame_size: 512,
        ..Default::default()
    })));
    let (server, client) = IoTest::create();
    client.remote_buffer_cap(64 * 1024);
    let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
    let io = Io::new(server, cfg.clone());
    let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);

    // fits
    conn.get_ref().post_frame(AmqpFrame::new(0, attach()));
    assert!(conn.get_error().is_none());

    // properties do not fit into remote max frame size
    let Frame::Attach(mut attach) = attach() else {
        panic!()
    };
    attach.0.properties = Some(
        [(Symbol::from("key"), Variant::from("a".repeat(512)))]
            .into_iter()
            .collect(),
    );
    conn.get_ref().post_frame(AmqpFrame::new(0, attach.into()));
    assert!(matches!(
        conn.get_error(),
        Some(AmqpProtocolError::Codec(
            AmqpCodecError::MaxOutboundSizeExceeded
        ))
    ));

    ntex::time::sleep(ntex::time::Millis(50)).await;
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    assert!(matches!(
        codec.decode(&mut buf).unwrap().unwrap().performative(),
        Frame::Attach(_)
    ));
    assert!(buf.is_empty());
}
