use super::*;

/// STATEMENT: Detaching a remote sender link before it is confirmed fails with
/// `LinkNotAttached` and leaves the link confirmable.
///
/// HOW:
/// - Receive a remote attach and call `detach_sender_link` before confirming;
///   assert `LinkNotAttached`.
/// - Confirm the link and assert it is found by name and only `Begin` and
///   `Attach n 0` are written.
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

/// STATEMENT: Link flows received before a remote sender is confirmed are applied on
/// confirmation, using the last flow that carried link credit.
///
/// HOW:
/// - Deliver flows with credit 5, 10 and none before confirming; assert credit is 0.
/// - Confirm with `attach_remote_sender_link` and assert credit is 10 and `ready`
///   returns `true`.
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

/// STATEMENT: Remote sender links are indexed by name and address, a newer duplicate
/// name wins, and names are removed on detach.
///
/// HOW:
/// - Confirm link `n` at address `a`; assert lookups by name and address only match
///   the right key.
/// - Confirm link `a`, then another `a`; assert the newer one owns the name even
///   after the older one is detached.
/// - Detach all links and assert the name maps are empty.
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

/// STATEMENT: Session and link state from flow frames is applied in frame order before
/// the control service is notified.
///
/// HOW:
/// - Deliver a flow with credit 10 and window 50 and assert `Action::Flow` and
///   credit 10.
/// - Deliver a flow with credit 3 and window 80 and assert the remote window grew
///   by 30 and credit is 3.
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

/// STATEMENT: A remote sender detached before confirmation is closed and releases its
/// handle and name whether it is then accepted or rejected.
///
/// HOW:
/// - Deliver a peer detach and a later flow (ignored) before confirming.
/// - If accepted, assert the link is closed, has no credit, is not ready and a
///   `RemoteDetachSender` control frame is queued; if rejected, assert it is closed.
/// - Assert the same attach is accepted again and frames are `attach 0 Sender`,
///   `detach 0 true`.
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

/// STATEMENT: A rejected remote receiver keeps its remote handle until the peer
/// detaches, without affecting other links.
///
/// HOW:
/// - Reject receiver `r` on remote handle 5 with `close_with_error`; assert the
///   handle is in use and the next link gets local handle 1.
/// - Deliver a peer detach for 5 and assert the close completes, the other link
///   stays open and handle 5 is free.
/// - Attach a new receiver on handle 5 and assert it reuses local handle 0.
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

/// STATEMENT: Flow echo replies carry link state for established links and only
/// session state for opening or unknown links.
///
/// HOW:
/// - Establish receiver `r` with credit 10 and sender `s`, and leave sender `o`
///   opening.
/// - Deliver echo flows for `s`, `r`, `o` and an unknown handle.
/// - Assert replies `Flow Some(1) Some(7)`, `Flow Some(0) Some(10)` and two
///   `Flow None None`.
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

/// STATEMENT: Confirmation and credit for a closed remote receiver are ignored once its
/// handle is reused by a new link.
///
/// HOW:
/// - Close receiver `a` and attach `b`, which reuses the same local handle.
/// - Assert both confirm paths for `a` return `false` and setting its credit keeps 0.
/// - Confirm `b` with credit 5 and assert only `b`'s attach and flow follow `a`'s
///   attach and detach.
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

/// STATEMENT: Receiver credit set before a remote link is confirmed is sent only after
/// the attach response.
///
/// HOW:
/// - Set credit 10 on an unconfirmed remote receiver and assert only `Begin` is
///   written.
/// - Confirm and add 5 credit; assert `Attach a 0`, then flows with credit 10 and 15.
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

/// STATEMENT: A rejected remote sender keeps its remote handle until the peer detaches,
/// without affecting an established link sharing that handle value locally.
///
/// HOW:
/// - Reject sender `s` on remote handle 1 and assert the handle stays in use;
///   confirm `s2`, which gets local id 1.
/// - Deliver a peer detach for 1 twice and assert `s2` stays open and the handle is
///   released; the second detach is ignored.
/// - Attach `s3` on remote handle 1 and assert it gets local id 0.
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

/// STATEMENT: A remote receiver detached before confirmation is closed and releases its
/// handle whether it is then accepted or rejected.
///
/// HOW:
/// - Deliver a peer detach before confirming and assert the link is still open.
/// - If accepted, assert confirm returns `false`, the link is closed and a
///   `RemoteDetachReceiver` control frame is queued; otherwise close it with error.
/// - Assert the handle is free, a re-attach gets handle 0, and frames are
///   `attach 0 Receiver`, `detach 0 true`.
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

/// STATEMENT: Confirming or rejecting a remote sender after its session ends sends no
/// link frames.
///
/// HOW:
/// - End the session locally (asserting a queued `LocalSessionEnded` with the open
///   link) or remotely (asserting the link is closed and not ready).
/// - Accept or reject the pending sender; for local end, deliver the peer `End`
///   and assert the link is closed.
/// - Assert only `Begin` and `End` are written.
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

/// STATEMENT: Confirming or rejecting a remote receiver after its session ends sends no
/// link frames.
///
/// HOW:
/// - End the session locally (asserting a queued `LocalSessionEnded` with the open
///   link) or remotely (asserting `SessionEnded` holds the closed link).
/// - Accept (confirm returns `false`) or close with error; for local end, deliver
///   the peer `End` and assert the link is closed.
/// - Assert only `Begin` and `End` are written.
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

/// STATEMENT: A detach reply that exceeds the peer frame size fails the connection
/// without dropping link or session state during detach handling.
///
/// HOW:
/// - With a 512-byte peer frame limit, confirm a remote sender and drop all external
///   handles to it.
/// - Deliver a peer detach with a 1024-byte error description; assert
///   `DetachSender` returns the closed link.
/// - Assert `MaxOutboundSizeExceeded`, the connection is not open and the session is
///   removed.
#[ntex::test]
async fn remote_sender_detach_reply_exceeds_frame_size() {
    let (_io, conn, _client) = small_frame_connection();
    handle_frame(&conn, begin()).unwrap();
    let Ok(Action::AttachSender(link, attach, response)) =
        handle_frame(&conn, named_attach(Role::Receiver, "n", "a", 3))
    else {
        panic!()
    };
    session(&conn)
        .inner
        .get_mut()
        .attach_remote_sender_link(&attach, response, link.inner.clone());
    // session slab holds the only link reference
    drop(link);

    // detach reply echoes the remote error and fails to encode,
    // link and session state must outlive detach handling
    let detach: Frame = Detach(Box::new(DetachInner {
        handle: 3,
        closed: true,
        error: Some(Error(Box::new(codec::ErrorInner {
            condition: AmqpError::InternalError.into(),
            description: Some("x".repeat(1024).into()),
            info: None,
        }))),
    }))
    .into();
    let Ok(Action::DetachSender(link, _)) = handle_frame(&conn, detach) else {
        panic!()
    };
    assert!(link.is_closed());

    // connection is failed and session state is dropped after frame handling
    assert!(matches!(
        conn.get_error(),
        Some(AmqpProtocolError::Codec(
            AmqpCodecError::MaxOutboundSizeExceeded
        ))
    ));
    assert!(!conn.is_opened());
    assert!(conn.get_ref().0.get_ref().sessions.is_empty());
    assert!(conn.get_session_by_local_id(0).is_none());
}

/// STATEMENT: An attach reply that exceeds the peer frame size fails the connection
/// without dropping link state during attach confirmation.
///
/// HOW:
/// - With a 512-byte peer frame limit, attach sender and receiver links with
///   1024-byte names and confirm them.
/// - Assert confirmation succeeds and the error is `MaxOutboundSizeExceeded`.
/// - Handle another frame and assert the session is removed.
#[ntex::test]
async fn remote_link_attach_reply_exceeds_frame_size() {
    for role in [Role::Receiver, Role::Sender] {
        let (_io, conn, _client) = small_frame_connection();
        handle_frame(&conn, begin()).unwrap();

        // attach reply echoes the remote link name and fails to encode,
        // link state must outlive attach confirmation
        let name = "n".repeat(1024);
        match handle_frame(&conn, named_attach(role, &name, "a", 3)) {
            Ok(Action::AttachSender(link, attach, response)) => {
                let link = session(&conn).inner.get_mut().attach_remote_sender_link(
                    &attach,
                    response,
                    link.inner.clone(),
                );
                assert_eq!(link.name(), &name);
            }
            Ok(Action::AttachReceiver(link, _, response)) => {
                assert!(link.confirm_receiver_link(response));
            }
            _ => panic!(),
        }
        assert!(matches!(
            conn.get_error(),
            Some(AmqpProtocolError::Codec(
                AmqpCodecError::MaxOutboundSizeExceeded
            ))
        ));

        // session state is dropped with the next frame
        handle_frame(&conn, Frame::Empty).unwrap();
        assert!(conn.get_session_by_local_id(0).is_none());
    }
}

/// Connection to a remote that accepts frames up to 512 bytes
fn small_frame_connection() -> (Io, Connection, IoTest) {
    let open = OpenInner {
        max_frame_size: 512,
        ..Default::default()
    };
    let remote = RemoteServiceConfig::new(&Open(Box::new(open)));
    let (server, client) = IoTest::create();
    let cfg = SharedCfg::new("T").add(AmqpServiceConfig::new()).build();
    let io = Io::new(server, cfg.clone());
    let conn = Connection::new(io.get_ref(), &cfg.get(), &remote);
    (io, conn, client)
}
