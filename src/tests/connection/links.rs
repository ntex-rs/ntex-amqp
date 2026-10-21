use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ntex_bytes::ByteString;
use ntex_util::Stream;

use super::*;
use crate::{ControlFrame, ControlFrameKind};

const RECEIVED: DeliveryState = DeliveryState::Received(Received {
    section_number: 0,
    section_offset: 0,
});

/// Remote sender link without credit
fn remote_sender(conn: &Connection, name: &str, handle: u32) -> SenderLink {
    let Ok(Action::AttachSender(snd, attach, response)) =
        handle_frame(conn, named_attach(Role::Receiver, name, name, handle))
    else {
        panic!()
    };
    session(conn)
        .inner
        .get_mut()
        .attach_remote_sender_link(&attach, response, snd.inner.clone())
}

async fn named_local_receiver(conn: &Connection, name: &'static str, handle: u32) -> ReceiverLink {
    let s = session(conn);
    let fut = ntex::rt::spawn(async move { s.build_receiver_link(name, name).attach().await });
    sleep(Millis(10)).await;
    let Ok(Action::None) = handle_frame(conn, named_attach(Role::Sender, name, name, handle))
    else {
        panic!()
    };
    fut.await.unwrap().unwrap()
}

fn hash<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

fn dispositions(client: &IoTest) -> Vec<(bool, Option<DeliveryState>)> {
    read_frames(client)
        .into_iter()
        .filter_map(|f| match f {
            Frame::Disposition(d) => Some((d.settled(), d.state().cloned())),
            _ => None,
        })
        .collect()
}

async fn check_lookup_handles_after_shutdown(end_session: bool) {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    remote_sender(&conn, "s", 3);
    let Ok(Action::AttachReceiver(receiver, _, response)) =
        handle_frame(&conn, named_attach(Role::Sender, "r", "r", 4))
    else {
        panic!()
    };
    assert!(receiver.confirm_receiver_link(response));
    drop(receiver);

    let session = session(&conn);
    let senders = [
        session.get_sender_link("s").unwrap(),
        session.get_sender_link_by_address("s").unwrap(),
        session.get_sender_link_by_local_handle(0).unwrap(),
        session.get_sender_link_by_remote_handle(3).unwrap(),
    ];
    let receivers = [
        session.get_receiver_link_by_local_handle(1).unwrap(),
        session.get_receiver_link_by_remote_handle(4).unwrap(),
    ];

    if end_session {
        handle_frame(&conn, End { error: None }.into()).unwrap();
    } else {
        conn.force_close();
    }
    assert!(session.get_sender_link("s").is_none());
    assert!(session.get_receiver_link_by_remote_handle(4).is_none());
    drop(session);
    drop(conn);

    for sender in senders {
        assert_eq!(sender.name(), "s");
        assert!(sender.is_closed());
        assert!(!sender.ready().await);
        assert!(sender.transfer(Bytes::new()).send().await.is_err());
    }
    for receiver in &receivers {
        assert_eq!(receiver.name(), "r");
        assert!(receiver.is_closed());
    }
    assert!(matches!(receivers[0].recv().await, Some(Err(_))));
    assert!(receivers[1].recv().await.is_none());
}

#[ntex::test]
async fn pending_controls_release_connection() {
    for notification in ["sender", "receiver", "session"] {
        for termination in ["drop", "force", "error", "remote"] {
            // Detach watchers own separate connection handles until the executor runs.
            let (_io, conn, _client) =
                connection_with(AmqpServiceConfig::new().set_link_attach_timeout(Seconds::ZERO));
            handle_frame(&conn, begin()).unwrap();
            let sender = remote_sender(&conn, "s", 3);
            let Ok(Action::AttachReceiver(receiver, _, response)) =
                handle_frame(&conn, named_attach(Role::Sender, "r", "r", 4))
            else {
                panic!()
            };
            assert!(receiver.confirm_receiver_link(response));
            let session = session(&conn);
            let connection_weak = conn.0.downgrade();
            let session_weak = session.inner.downgrade();
            let sender_weak = sender.inner.downgrade();
            let receiver_weak = receiver.inner.downgrade();
            let queue = conn.get_control_queue().clone();

            if notification != "sender" {
                receiver.set_link_credit(1);
                let Frame::Transfer(mut frame) = transfer(1, false, None, 1) else {
                    panic!()
                };
                frame.0.handle = receiver.handle();
                // Isolate cleanup from the connection/session frame-dispatch borrows.
                assert!(matches!(
                    receiver
                        .inner
                        .get_mut()
                        .handle_transfer(frame, &receiver.inner),
                    crate::rcvlink::TransferResult::Action(Action::Transfer(_))
                ));
                assert!(receiver.has_deliveries());
            }

            match notification {
                "sender" => drop(session.detach_sender_link(sender.id(), None)),
                "receiver" => drop(session.detach_receiver_link(receiver.handle(), None)),
                _ => {
                    let mut end = std::pin::pin!(session.end());
                    assert!(pending(end.as_mut()).await);
                }
            }
            assert_eq!(
                queue.pending.borrow().len(),
                1,
                "{notification}/{termination}"
            );
            drop(sender);
            drop(receiver);
            drop(session);

            match termination {
                "force" => conn.force_close(),
                "error" => conn.set_error(AmqpProtocolError::Disconnected),
                "remote" => {
                    handle_frame(&conn, Close { error: None }.into()).unwrap();
                }
                _ => (),
            }
            if termination != "drop" {
                assert!(
                    queue.pending.borrow().is_empty(),
                    "{notification}/{termination}"
                );
            }
            drop(conn);

            assert!(
                queue.pending.borrow().is_empty(),
                "{notification}/{termination}"
            );
            assert!(
                connection_weak.upgrade().is_none(),
                "{notification}/{termination}"
            );
            assert!(
                session_weak.upgrade().is_none(),
                "{notification}/{termination}"
            );
            assert!(
                sender_weak.upgrade().is_none(),
                "{notification}/{termination}"
            );
            assert!(
                receiver_weak.upgrade().is_none(),
                "{notification}/{termination}"
            );
        }
    }
}

#[ntex::test]
async fn closed_control_queue_rejects_retaining_frames() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let sender = remote_sender(&conn, "s", 3);
    let frame = ControlFrame::new_kind(ControlFrameKind::LocalSessionEnded(vec![
        ntex_util::future::Either::Left(sender),
    ]));
    let weak = conn.0.downgrade();
    let queue = conn.get_control_queue().clone();
    conn.force_close();
    drop(conn);
    assert!(weak.upgrade().is_some());

    queue.enqueue_frame(frame);
    assert!(queue.pending.borrow().is_empty());
    assert!(weak.upgrade().is_none());
}

#[ntex::test]
async fn lookup_handles_survive_connection_close() {
    check_lookup_handles_after_shutdown(false).await;
}

#[ntex::test]
async fn lookup_handles_survive_session_end() {
    check_lookup_handles_after_shutdown(true).await;
}

#[ntex::test]
async fn lookup_handles_survive_growth_and_detach() {
    let (_io, conn, _client) = connection();
    let Frame::Begin(mut frame) = begin() else { panic!() };
    frame.0.handle_max = 64;
    handle_frame(&conn, frame.into()).unwrap();
    remote_sender(&conn, "s", 3);
    let Ok(Action::AttachReceiver(receiver, _, response)) =
        handle_frame(&conn, named_attach(Role::Sender, "r", "r", 4))
    else {
        panic!()
    };
    assert!(receiver.confirm_receiver_link(response));
    drop(receiver);

    let session = session(&conn);
    let sender = session.get_sender_link("s").unwrap();
    let receiver = session.get_receiver_link_by_remote_handle(4).unwrap();
    for handle in 5..21 {
        remote_sender(&conn, &format!("extra-{handle}"), handle);
    }
    assert_eq!(sender.name(), "s");
    assert_eq!(receiver.name(), "r");
    assert!(!sender.is_closed());
    assert!(!receiver.is_closed());

    handle_frame(&conn, peer_detach(3)).unwrap();
    let replacement = remote_sender(&conn, "replacement", 3);
    assert_eq!(replacement.id(), sender.id());
    assert_eq!(sender.name(), "s");
    assert!(sender.is_closed());
    assert!(!replacement.is_closed());
    assert!(!sender.ready().await);

    handle_frame(&conn, peer_detach(4)).unwrap();
    let Ok(Action::AttachReceiver(replacement, _, response)) =
        handle_frame(&conn, named_attach(Role::Sender, "replacement", "r", 4))
    else {
        panic!()
    };
    assert!(replacement.confirm_receiver_link(response));
    assert_eq!(replacement.handle(), receiver.handle());
    assert_eq!(receiver.name(), "r");
    assert!(receiver.is_closed());
    assert!(!replacement.is_closed());
    assert!(receiver.recv().await.is_none());
}

#[ntex::test]
async fn sender_link_accessors() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let link = remote_sender(&conn, "s", 3);

    assert_eq!(link.id(), 0);
    assert_eq!(link.name(), "s");
    assert_eq!(link.address().map(ByteString::as_str), Some("s"));
    assert_eq!(link.remote_handle(), 3);
    assert_eq!(link.session().local_channel_id(), 0);
    assert_eq!(link.credit(), 0);
    assert_eq!(link.max_message_size(), None);
    assert!(link.error().is_none());
    assert!(!link.is_closed());
    assert!(!link.is_closed());
    assert_eq!(format!("{link:?}"), "SenderLink(\"s\")");
    assert_eq!(
        format!("{:?}", link.inner.get_ref()),
        "SenderLinkInner(\"s\")"
    );

    // `ready()` resolves once the peer grants credit
    {
        let mut ready = std::pin::pin!(link.ready());
        assert!(pending(ready.as_mut()).await);
        link_flow(&conn, 3, (0, 5, false), 1, 100);
        assert!(timeout(Millis(1000), ready).await.unwrap());
    }
    assert_eq!(link.credit(), 5);

    // closed link is never ready
    handle_frame(&conn, peer_detach(3)).unwrap();
    assert!(!timeout(Millis(1000), link.ready()).await.unwrap());
    assert!(link.is_closed());
    assert!(matches!(
        link.error(),
        Some(AmqpProtocolError::LinkDetached(None))
    ));
}

#[ntex::test]
async fn sender_link_waiters_notified_on_close() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let link = remote_sender(&conn, "s", 3);

    let credit = ntex::rt::spawn(link.on_credit_update());
    sleep(Millis(10)).await;
    link_flow(&conn, 3, (0, 2, false), 1, 100);
    timeout(Millis(1000), credit).await.unwrap().unwrap();

    // closing the link wakes both credit and close waiters
    let credit = ntex::rt::spawn(link.on_credit_update());
    let closed = ntex::rt::spawn(link.on_close());
    sleep(Millis(10)).await;
    handle_frame(&conn, peer_detach(3)).unwrap();
    timeout(Millis(1000), credit).await.unwrap().unwrap();
    timeout(Millis(1000), closed).await.unwrap().unwrap();
}

#[ntex::test]
async fn transfer_builder_errors() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();

    // remote attach advertises a max message size
    let Frame::Attach(mut attach) = named_attach(Role::Receiver, "s", "s", 3) else {
        panic!()
    };
    attach.0.max_message_size = Some(4);
    let Ok(Action::AttachSender(snd, attach, response)) = handle_frame(&conn, attach.into()) else {
        panic!()
    };
    let link = session(&conn).inner.get_mut().attach_remote_sender_link(
        &attach,
        response,
        snd.inner.clone(),
    );
    assert_eq!(link.max_message_size(), Some(4));

    assert!(matches!(
        link.transfer(Bytes::from_static(b"0123456789"))
            .send()
            .await,
        Err(AmqpProtocolError::BodyTooLarge)
    ));

    // link is detached by the peer
    handle_frame(&conn, peer_detach(3)).unwrap();
    assert!(matches!(
        link.transfer(Bytes::from_static(b"a")).send().await,
        Err(AmqpProtocolError::LinkDetached(None))
    ));
}

#[ntex::test]
async fn transfer_builder_options() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let link = add_sender(&conn, "s", 3, 100);

    let delivery = link
        .transfer(Bytes::from_static(b"body"))
        .tag(Bytes::from_static(b"tag"))
        .format(7)
        .settled()
        .send()
        .await
        .unwrap();
    assert_eq!(delivery.tag(), &Bytes::from_static(b"tag"));

    // settled delivery is not tracked and `wait` resolves immediately
    assert_eq!(unsettled(&conn).0, 0);
    assert_eq!(delivery.wait().await.unwrap(), None);

    sleep(Millis(25)).await;
    let frames = read_frames(&client);
    let Some(Frame::Transfer(tr)) = frames.last() else {
        panic!("{frames:?}")
    };
    assert_eq!(tr.message_format(), Some(7));
    assert_eq!(tr.settled(), Some(true));
    assert_eq!(tr.delivery_tag(), Some(&Bytes::from_static(b"tag")));
}

#[ntex::test]
async fn receiver_link_accessors() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let link = local_receiver(&conn, 3).await;

    assert_eq!(link.name(), "r");
    assert_eq!(link.handle(), 0);
    // locally opened links do not track the handle chosen by the peer
    assert_eq!(link.remote_handle(), link.handle());
    assert_eq!(link.credit(), 0);
    assert!(!link.is_closed());
    assert_eq!(format!("{link:?}"), "ReceiverLink(\"r\")");
    assert_eq!(
        format!("{:?}", link.inner.get_ref()),
        "ReceiverLinkInner(\"r\")"
    );

    // equality and hashing are identity based
    let other = named_local_receiver(&conn, "r2", 4).await;
    assert_eq!(link, link.clone());
    assert_ne!(link, other);
    assert_eq!(hash(&link), hash(&link.clone()));
    assert_ne!(hash(&link), hash(&other));

    // dispositions can be sent manually
    link.send_disposition(Disposition(Box::new(DispositionInner {
        role: Role::Receiver,
        first: 1,
        last: None,
        settled: true,
        state: Some(ACCEPTED),
        batchable: false,
    })));
    sleep(Millis(25)).await;
    assert!(frame_names(&client).contains(&"Disposition".to_string()));
}

#[ntex::test]
async fn receiver_link_stream() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let link = local_receiver(&conn, 0).await;
    link.set_link_credit(5);
    sleep(Millis(25)).await;

    handle_frame(&conn, transfer(1, false, None, 1)).unwrap();
    let mut link = std::pin::pin!(link);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(Ok((_, tr)))) = link.as_mut().poll_next(&mut cx) else {
        panic!()
    };
    assert_eq!(tr.delivery_id(), Some(1));

    // ended session terminates the stream with an error
    handle_frame(&conn, End { error: None }.into()).unwrap();
    assert!(matches!(
        link.as_mut().poll_next(&mut cx),
        Poll::Ready(Some(Err(AmqpProtocolError::SessionEnded(None))))
    ));
    assert!(matches!(link.poll_next(&mut cx), Poll::Ready(None)));
}

#[ntex::test]
async fn delivery_accessors_and_discard() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let link = add_sender(&conn, "s", 3, 100);

    let mut delivery = link
        .transfer(Bytes::from_static(b"body"))
        .tag(Bytes::from_static(b"t"))
        .send()
        .await
        .unwrap();
    assert_eq!(delivery.id(), 0);
    assert_eq!(delivery.tag(), &Bytes::from_static(b"t"));
    assert!(delivery.remote_state().is_none());
    assert!(!delivery.is_remote_settled());
    sleep(Millis(25)).await;
    let _ = read_frames(&client);

    // non-terminal update does not settle the delivery
    delivery.update_state(RECEIVED);
    sleep(Millis(25)).await;
    assert_eq!(dispositions(&client), [(false, Some(RECEIVED))]);

    delivery.settle(ACCEPTED);
    sleep(Millis(25)).await;
    assert_eq!(dispositions(&client), [(true, Some(ACCEPTED))]);

    // already settled locally, no more dispositions
    delivery.settle(ACCEPTED);
    delivery.update_state(ACCEPTED);
    sleep(Millis(25)).await;
    assert_eq!(
        dispositions(&client),
        Vec::<(bool, Option<DeliveryState>)>::new()
    );

    // discarded delivery is removed from the unsettled map
    let delivery = link
        .transfer(Bytes::from_static(b"body"))
        .send()
        .await
        .unwrap();
    assert_eq!(unsettled(&conn).0, 2);
    delivery.discard();
    assert_eq!(unsettled(&conn).0, 1);
}

#[ntex::test]
async fn receiver_link_builder_options() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();

    let s = session(&conn);
    let fut = ntex::rt::spawn(async move {
        s.build_receiver_link("r", "addr")
            .max_message_size(1024)
            .property("keep", Some(1u32))
            .property("drop", Some(2u32))
            .property::<_, u32>("drop", None)
            .capabilities(symbols())
            .attach_timeout(Seconds(0))
            .with_frame(|f| f.0.rcv_settle_mode = ReceiverSettleMode::Second)
            .attach()
            .await
    });
    sleep(Millis(10)).await;

    let frames = read_frames(&client);
    let Some(Frame::Attach(attach)) = frames.last() else {
        panic!("unexpected frames")
    };
    assert_eq!(attach.max_message_size(), Some(1024));
    assert_eq!(attach.rcv_settle_mode(), ReceiverSettleMode::Second);
    assert_eq!(attach.source().unwrap().capabilities(), Some(&symbols()));
    let props = attach.properties().unwrap();
    assert_eq!(props.get(&Symbol::from("keep")), Some(&Variant::from(1u32)));
    assert!(!props.contains_key(&Symbol::from("drop")));

    // attach timeout is disabled, link stays pending
    sleep(Millis(50)).await;
    assert!(!fut.is_finished());
    handle_frame(&conn, named_attach(Role::Sender, "r", "addr", 0)).unwrap();
    assert!(fut.await.unwrap().is_ok());
}
