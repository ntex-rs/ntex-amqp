use super::*;

#[ntex::test]
async fn detach_unconfirmed_remote_sender_link() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let s = session(&conn);
    let Ok(Action::AttachSender(link, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "n", "a", 3))
    else {
        panic!()
    };
    let res = timeout(Millis(1000), s.detach_sender_link(link.id(), None))
        .await
        .unwrap();
    assert!(matches!(res, Err(AmqpProtocolError::LinkNotAttached)));

    // link is confirmed
    s.inner
        .get_mut()
        .attach_remote_sender_link(&attach, response, link.inner.clone());
    assert!(s.get_sender_link("n").is_some());
    sleep(Millis(10)).await;
    assert_eq!(frame_names(&client), ["Begin", "Attach n 0"]);
}

#[ntex::test]
async fn remote_sender_flow_before_confirm() {
    let (_io, conn, _client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();
    let session = session(&conn);

    let Ok(Action::AttachSender(link, attach, response)) =
        handle(peer_attach(Role::Receiver, None))
    else {
        panic!()
    };

    // link flow before confirmation, last flow with link credit is applied
    for credit in [Some(5), Some(10), None] {
        let Ok(Action::None) = handle(peer_flow(credit)) else {
            panic!()
        };
    }
    assert_eq!(link.credit(), 0);

    let link =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, link.inner.clone());
    assert_eq!(link.credit(), 10);
    assert!(link.ready().await);
}

#[ntex::test]
async fn remote_sender_link_names() {
    let (_io, conn, _client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();
    let session = session(&conn);
    let confirm = |frame| {
        let Ok(Action::AttachSender(link, attach, response)) = handle(frame) else {
            panic!()
        };
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, link.inner.clone())
    };

    // remote sender link is registered by link name
    let link = confirm(named_attach(Role::Receiver, "n", "a", 0));
    assert_eq!(link.name(), "n");
    assert_eq!(link.address().unwrap(), "a");
    assert_eq!(session.get_sender_link("n").unwrap().id(), link.id());
    assert!(session.get_sender_link("a").is_none());
    assert_eq!(
        session.get_sender_link_by_address("a").unwrap().id(),
        link.id()
    );
    assert!(session.get_sender_link_by_address("n").is_none());

    // link named as address of other link
    let link2 = confirm(named_attach(Role::Receiver, "a", "a", 1));
    assert_eq!(session.get_sender_link("a").unwrap().id(), link2.id());

    // duplicate name, newer link takes the name
    let link3 = confirm(named_attach(Role::Receiver, "a", "b", 2));
    assert_eq!(session.get_sender_link("a").unwrap().id(), link3.id());
    let Ok(Action::DetachSender(..)) = handle(peer_detach(1)) else {
        panic!()
    };
    assert_eq!(session.get_sender_link("a").unwrap().id(), link3.id());

    // name is removed on detach
    let Ok(Action::DetachSender(..)) = handle(peer_detach(0)) else {
        panic!()
    };
    assert!(session.get_sender_link("n").is_none());
    assert!(session.get_sender_link_by_address("a").is_none());
    let Ok(Action::DetachSender(..)) = handle(peer_detach(2)) else {
        panic!()
    };
    let inner = session.inner.get_ref();
    assert!(inner.sender_names.is_empty() && inner.link_names.is_empty());
}

#[ntex::test]
async fn remote_sender_flow_order() {
    let (_io, conn, _client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();
    let session = session(&conn);

    let Ok(Action::AttachSender(link, attach, response)) =
        handle(peer_attach(Role::Receiver, None))
    else {
        panic!()
    };
    let link =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, link.inner.clone());

    let flow = |credit, window| {
        let Frame::Flow(mut flow) = peer_flow(Some(credit)) else {
            panic!()
        };
        flow.0.incoming_window = window;
        Frame::Flow(flow)
    };

    // session and link flows are applied in frames order,
    // before control service is notified
    let Ok(Action::Flow(..)) = handle(flow(10, 50)) else {
        panic!()
    };
    assert_eq!(link.credit(), 10);
    let window = session.remote_window_size();
    let Ok(Action::Flow(..)) = handle(flow(3, 80)) else {
        panic!()
    };
    assert_eq!(session.remote_window_size(), window + 30);
    assert_eq!(link.credit(), 3);
}

#[ntex::test]
async fn remote_sender_detach_before_confirm() {
    for accept in [true, false] {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref();
        let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
        handle(begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachSender(link, attach, response)) =
            handle(peer_attach(Role::Receiver, None))
        else {
            panic!()
        };
        let detach = Detach(Box::new(DetachInner {
            handle: 0,
            closed: true,
            error: None,
        }));
        let Ok(Action::None) = handle(detach.into()) else {
            panic!()
        };
        // flow after detach is ignored
        let Ok(Action::None) = handle(peer_flow(Some(10))) else {
            panic!()
        };

        if accept {
            let link = session.inner.get_mut().attach_remote_sender_link(
                &attach,
                response,
                link.inner.clone(),
            );
            assert!(link.is_closed());
            assert_eq!(link.credit(), 0);
            assert!(!link.ready().await);

            // control service is notified
            let conn_ref = conn.get_ref();
            let queue = conn_ref.get_control_queue().pending.borrow();
            assert!(matches!(
                queue.back().unwrap().kind(),
                crate::ControlFrameKind::RemoteDetachSender(..)
            ));
        } else {
            session
                .inner
                .get_mut()
                .detach_unconfirmed_sender_link(&attach, &link.inner, None);
            assert!(link.is_closed());
        }

        // remote handle and link name are released
        let Ok(Action::AttachSender(..)) = handle(peer_attach(Role::Receiver, None)) else {
            panic!()
        };

        sleep(Millis(50)).await;
        assert_eq!(link_frames(&client), ["attach 0 Sender", "detach 0 true"]);
    }
}

#[ntex::test]
async fn remote_receiver_reject_detach() {
    let (_io, conn, _client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();
    let session = session(&conn);

    // rejected link keeps handles until remote detach
    let Ok(Action::AttachReceiver(link, _, _)) = handle(named_attach(Role::Sender, "r", "a", 5))
    else {
        panic!()
    };
    let closed = link.close_with_error(crate::error::LinkError::force_detach());
    assert!(session.inner.get_ref().is_remote_handle_used(5));

    let Ok(Action::AttachReceiver(link2, _, response)) =
        handle(named_attach(Role::Sender, "r2", "b", 6))
    else {
        panic!()
    };
    assert_eq!(link2.handle(), 1);
    link2.confirm_receiver_link(response);

    // remote detach releases rejected link only
    let Ok(Action::None) = handle(peer_detach(5)) else {
        panic!()
    };
    closed.await.unwrap();
    assert!(!link2.is_closed());
    assert!(!session.inner.get_ref().is_remote_handle_used(5));

    let Ok(Action::AttachReceiver(link3, _, _)) = handle(named_attach(Role::Sender, "r3", "c", 5))
    else {
        panic!()
    };
    assert_eq!(link3.handle(), 0);
}

#[ntex::test]
async fn flow_echo_link_state() {
    let (_io, conn, client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let session = session(&conn);
    let echo = |handle: u32, link_credit: Option<u32>| {
        let Frame::Flow(mut flow) = peer_flow(link_credit) else {
            panic!()
        };
        flow.0.handle = Some(handle);
        flow.0.echo = true;
        handle_frame(&conn, flow.into()).unwrap();
    };

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
    session
        .inner
        .get_mut()
        .attach_remote_sender_link(&attach, response, snd.inner.clone());
    let Ok(Action::AttachSender(..)) =
        handle_frame(&conn, named_attach(Role::Receiver, "o", "o", 5))
    else {
        panic!()
    };
    sleep(Millis(50)).await;
    assert_eq!(
        frame_names(&client),
        ["Begin", "Attach r 0", "Flow Some(0) Some(10)", "Attach s 1"]
    );

    // echo reply carries state of established links
    echo(4, Some(7));
    echo(3, None);
    // opening and unknown links, session state only
    echo(5, Some(7));
    echo(9, Some(7));
    sleep(Millis(50)).await;
    assert_eq!(
        frame_names(&client),
        [
            "Flow Some(1) Some(7)",
            "Flow Some(0) Some(10)",
            "Flow None None",
            "Flow None None"
        ]
    );
}

#[ntex::test]
async fn remote_receiver_stale_confirm() {
    let (_io, conn, client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();
    let session = session(&conn);

    let Ok(Action::AttachReceiver(link_a, _, response_a)) =
        handle(named_attach(Role::Sender, "a", "a", 0))
    else {
        panic!()
    };
    let closed = link_a.close_with_error(crate::error::LinkError::force_detach());
    handle(peer_detach(0)).unwrap();
    closed.await.unwrap();

    // new link reuses handle of closed link
    let Ok(Action::AttachReceiver(link_b, _, response_b)) =
        handle(named_attach(Role::Sender, "b", "b", 1))
    else {
        panic!()
    };
    assert_eq!(link_a.handle(), link_b.handle());

    // stale confirmation and credit are ignored
    assert!(!session.inner.get_mut().confirm_receiver_link(
        &link_a.inner,
        response_a.clone(),
        None
    ));
    assert!(!link_a.confirm_receiver_link(response_a));
    link_a.set_link_credit(10);
    assert_eq!(link_a.credit(), 0);

    assert!(link_b.confirm_receiver_link(response_b));
    link_b.set_link_credit(5);

    sleep(Millis(50)).await;
    assert_eq!(
        frame_names(&client),
        [
            "Begin",
            "Attach a 0",
            "Detach 0",
            "Attach b 0",
            "Flow Some(0) Some(5)"
        ]
    );
}

#[ntex::test]
async fn remote_receiver_credit_before_confirm() {
    let (_io, conn, client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();

    let Ok(Action::AttachReceiver(link, _, response)) =
        handle(named_attach(Role::Sender, "a", "a", 0))
    else {
        panic!()
    };

    // credit is sent after attach response
    link.set_link_credit(10);
    sleep(Millis(50)).await;
    assert_eq!(frame_names(&client), ["Begin"]);

    assert!(link.confirm_receiver_link(response));
    link.set_link_credit(5);

    sleep(Millis(50)).await;
    assert_eq!(
        frame_names(&client),
        [
            "Attach a 0",
            "Flow Some(0) Some(10)",
            "Flow Some(0) Some(15)"
        ]
    );
}

#[ntex::test]
async fn remote_sender_reject_detach() {
    let (_io, conn, _client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();
    let session = session(&conn);

    // rejected link keeps handles until remote detach
    let Ok(Action::AttachSender(link, attach, _)) =
        handle(named_attach(Role::Receiver, "s", "a", 1))
    else {
        panic!()
    };
    session
        .inner
        .get_mut()
        .detach_unconfirmed_sender_link(&attach, &link.inner, None);
    assert!(session.inner.get_ref().is_remote_handle_used(1));

    // local handle of established link is equal to remote handle of rejected link
    let Ok(Action::AttachSender(link2, attach, response)) =
        handle(named_attach(Role::Receiver, "s2", "b", 7))
    else {
        panic!()
    };
    let link2 =
        session
            .inner
            .get_mut()
            .attach_remote_sender_link(&attach, response, link2.inner.clone());
    assert_eq!(link2.id(), 1);

    // remote detach releases rejected link only
    let Ok(Action::None) = handle(peer_detach(1)) else {
        panic!()
    };
    assert!(!link2.is_closed());
    assert!(!session.inner.get_ref().is_remote_handle_used(1));

    // detach of unknown remote handle is ignored
    let Ok(Action::None) = handle(peer_detach(1)) else {
        panic!()
    };
    assert!(!link2.is_closed());

    let Ok(Action::AttachSender(link3, ..)) = handle(named_attach(Role::Receiver, "s3", "c", 1))
    else {
        panic!()
    };
    assert_eq!(link3.id(), 0);
}

#[ntex::test]
async fn remote_receiver_detach_before_confirm() {
    for accept in [true, false] {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref();
        let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
        handle(begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachReceiver(link, _, response)) =
            handle(named_attach(Role::Sender, "r", "a", 5))
        else {
            panic!()
        };
        let Ok(Action::None) = handle(peer_detach(5)) else {
            panic!()
        };
        assert!(!link.is_closed());

        if accept {
            assert!(!link.confirm_receiver_link(response));
            assert!(link.is_closed());

            // control service is notified
            let conn_ref = conn.get_ref();
            let queue = conn_ref.get_control_queue().pending.borrow();
            assert!(matches!(
                queue.back().unwrap().kind(),
                crate::ControlFrameKind::RemoteDetachReceiver(..)
            ));
        } else {
            link.close_with_error(crate::error::LinkError::force_detach())
                .await
                .unwrap();
        }

        // remote handle and link are released
        assert!(!session.inner.get_ref().is_remote_handle_used(5));
        let Ok(Action::AttachReceiver(link2, ..)) = handle(named_attach(Role::Sender, "r", "a", 5))
        else {
            panic!()
        };
        assert_eq!(link2.handle(), 0);

        sleep(Millis(50)).await;
        assert_eq!(link_frames(&client), ["attach 0 Receiver", "detach 0 true"]);
    }
}

#[ntex::test]
async fn remote_sender_end_before_confirm() {
    for (local, accept) in [(false, true), (false, false), (true, true), (true, false)] {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref();
        let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
        handle(begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachSender(link, attach, response)) =
            handle(peer_attach(Role::Receiver, None))
        else {
            panic!()
        };

        if local {
            let s = session.clone();
            ntex::rt::spawn(async move { s.end().await });
            sleep(Millis(10)).await;

            let conn_ref = conn.get_ref();
            let queue = conn_ref.get_control_queue().pending.borrow();
            let crate::ControlFrameKind::LocalSessionEnded(links) = queue.back().unwrap().kind()
            else {
                panic!()
            };
            assert_eq!(links.len(), 1);
            assert!(!link.is_closed());
        } else {
            let Ok(Action::SessionEnded(links)) = handle(End { error: None }.into()) else {
                panic!()
            };
            assert_eq!(links.len(), 1);
            assert!(link.is_closed());
            let ready = timeout(Millis(100), link.ready()).await;
            assert!(matches!(ready, Ok(false)));
        }

        // confirmation after end does not send frames
        if accept {
            session.inner.get_mut().attach_remote_sender_link(
                &attach,
                response,
                link.inner.clone(),
            );
        } else {
            session
                .inner
                .get_mut()
                .detach_unconfirmed_sender_link(&attach, &link.inner, None);
        }

        if local {
            // remote end confirms local end
            handle(End { error: None }.into()).unwrap();
            assert!(link.is_closed());
        }

        sleep(Millis(50)).await;
        assert_eq!(
            frame_names(&client),
            ["Begin", "End"],
            "local: {local} accept: {accept}"
        );
    }
}

#[ntex::test]
async fn remote_receiver_end_before_confirm() {
    for (local, accept) in [(false, true), (false, false), (true, true), (true, false)] {
        let (_io, conn, client) = connection();
        let inner = conn.get_ref();
        let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
        handle(begin()).unwrap();
        let session = session(&conn);

        let Ok(Action::AttachReceiver(link, _, response)) =
            handle(named_attach(Role::Sender, "r", "a", 0))
        else {
            panic!()
        };

        if local {
            let s = session.clone();
            ntex::rt::spawn(async move { s.end().await });
            sleep(Millis(10)).await;

            let conn_ref = conn.get_ref();
            let queue = conn_ref.get_control_queue().pending.borrow();
            let crate::ControlFrameKind::LocalSessionEnded(links) = queue.back().unwrap().kind()
            else {
                panic!()
            };
            assert_eq!(links.len(), 1);
            assert!(!link.is_closed());
        } else {
            let Ok(Action::SessionEnded(links)) = handle(End { error: None }.into()) else {
                panic!()
            };
            assert!(matches!(&links[..], [ntex::util::Either::Right(l)] if *l == link));
            assert!(link.is_closed());
        }

        // confirmation after end does not send frames
        if accept {
            assert!(!link.confirm_receiver_link(response));
        } else {
            timeout(
                Millis(500),
                link.close_with_error(crate::error::LinkError::force_detach()),
            )
            .await
            .unwrap()
            .unwrap();
        }

        if local {
            // remote end confirms local end
            handle(End { error: None }.into()).unwrap();
            assert!(link.is_closed());
        }

        sleep(Millis(50)).await;
        assert_eq!(
            frame_names(&client),
            ["Begin", "End"],
            "local: {local} accept: {accept}"
        );
    }
}

/// Attach and detach frames written to the peer
fn link_frames(client: &IoTest) -> Vec<String> {
    read_frames(client)
        .into_iter()
        .filter_map(|frame| match frame {
            Frame::Attach(att) => Some(format!("attach {} {:?}", att.handle(), att.role())),
            Frame::Detach(det) => Some(format!("detach {} {}", det.handle(), det.closed())),
            _ => None,
        })
        .collect()
}
