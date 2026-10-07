use super::*;

#[ntex::test]
async fn session_accessors() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let s = session(&conn);

    assert_eq!(s.tag(), "T");
    assert_eq!(s.local_channel_id(), 0);
    assert_eq!(s.remote_channel_id(), 0);
    assert_eq!(s.frame().incoming_window(), 100);
    assert_eq!(s.frame().offered_capabilities(), Some(&symbols()));
    assert!(s.connection().is_opened());
    assert_eq!(
        format!("{s:?}"),
        "Session { local_channel_id: 0, remote_channel_id: 0 }"
    );
}

#[ntex::test]
async fn transfer_for_invalid_links() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();

    // no link for the handle at all
    assert!(matches!(
        handle_frame(&conn, transfer(0, false, None, 1)),
        Err(AmqpProtocolError::UnknownLink(_))
    ));

    // transfer addressed to a sender link
    handle_frame(&conn, named_attach(Role::Receiver, "s", "s", 0)).unwrap();
    assert!(matches!(
        handle_frame(&conn, transfer(0, false, None, 1)),
        Err(AmqpProtocolError::Unexpected(Frame::Transfer(_)))
    ));

    // transfer for a receiver link that is not confirmed yet
    let Ok(Action::AttachReceiver(..)) =
        handle_frame(&conn, named_attach(Role::Sender, "r", "r", 1))
    else {
        panic!()
    };
    let Frame::Transfer(mut tr) = transfer(0, false, None, 1) else {
        panic!()
    };
    tr.0.handle = 1;
    assert!(matches!(
        handle_frame(&conn, Frame::Transfer(tr)),
        Err(AmqpProtocolError::UnexpectedOpeningState(_))
    ));
}

#[ntex::test]
async fn transfer_for_closing_receiver() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let link = local_receiver(&conn, 0).await;
    link.set_link_credit(5);

    let fut = ntex::rt::spawn({
        let link = link.clone();
        async move { link.close().await }
    });
    sleep(Millis(10)).await;

    // transfers for a closing link are silently dropped
    assert!(matches!(
        handle_frame(&conn, transfer(0, false, None, 1)),
        Ok(Action::None)
    ));

    handle_frame(&conn, peer_detach(0)).unwrap();
    fut.await.unwrap().unwrap();
}

#[ntex::test]
async fn unexpected_session_frame_ignored() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();

    // frames that a session does not handle are dropped
    assert!(matches!(
        handle_frame(&conn, Open(Box::default()).into()),
        Ok(Action::None)
    ));
}

#[ntex::test]
async fn large_message_and_pages_bodies_are_chunked() {
    // (body kind, body)
    let mut msg = ntex_amqp_codec::Message::default();
    msg.set_body(|b| b.set_data(Bytes::from(vec![b'a'; 1200])));
    let mut pages = BytePages::default();
    pages.extend_from_slice(&[b'b'; 1200]);
    let cases: [(&str, TransferBody); 2] = [
        ("message", TransferBody::from(msg)),
        ("pages", TransferBody::from(pages)),
    ];

    for (kind, body) in cases {
        let (_io, conn, client, snd) = small_frames_sender();
        sleep(Millis(50)).await;
        transfer_frames(&client);

        let fut = ntex::rt::spawn(async move { snd.transfer(body).send().await });
        sleep(Millis(50)).await;

        // body does not fit into the 512 bytes frame, it is split into chunks
        assert_eq!(
            transfer_frames(&client),
            [
                "Transfer Some(0) more:true aborted:false",
                "Transfer None more:true aborted:false"
            ],
            "{kind}"
        );

        // remaining chunks are sent when session window is replenished
        session_window(&conn, 2, 10);
        sleep(Millis(50)).await;
        let frames = transfer_frames(&client);
        assert!(!frames.is_empty(), "{kind}");
        assert_eq!(
            frames.last().unwrap(),
            "Transfer None more:false aborted:false",
            "{kind}"
        );

        let disp = Disposition(Box::new(DispositionInner {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: true,
            state: Some(ACCEPTED),
            batchable: false,
        }));
        handle_frame(&conn, disp.into()).unwrap();
        assert_eq!(
            fut.await.unwrap().unwrap().remote_state(),
            Some(ACCEPTED),
            "{kind}"
        );
    }
}

#[ntex::test]
async fn duplicate_detach_for_unconfirmed_links() {
    for sender in [true, false] {
        let ctx = format!("sender: {sender}");
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();

        sleep(Millis(25)).await;
        read_frames(&client);

        let role = if sender { Role::Receiver } else { Role::Sender };
        let action = handle_frame(&conn, named_attach(role, "l", "l", 3)).unwrap();

        // link is not confirmed yet, detach frames are stored and handled on confirmation
        for _ in 0..2 {
            assert!(
                matches!(handle_frame(&conn, peer_detach(3)), Ok(Action::None)),
                "{ctx}"
            );
        }
        sleep(Millis(25)).await;
        assert!(read_frames(&client).is_empty(), "{ctx}");

        match action {
            Action::AttachSender(link, attach, response) => {
                session(&conn).inner.get_mut().attach_remote_sender_link(
                    &attach,
                    response,
                    link.inner.clone(),
                );
                assert!(link.is_closed(), "{ctx}");
            }
            Action::AttachReceiver(link, _, response) => {
                assert!(!link.confirm_receiver_link(response), "{ctx}");
                assert!(link.is_closed(), "{ctx}");
            }
            _ => panic!("{ctx}"),
        }
        sleep(Millis(25)).await;
        // detach is echoed with the local handle
        assert_eq!(detaches(&client), [(0, true, None)], "{ctx}");
    }
}
