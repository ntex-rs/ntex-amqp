use super::*;

#[ntex::test]
async fn cancelled_local_attach_detached() {
    for sender in [true, false] {
        // attach response is received after or before attach future is dropped
        for delivered in [false, true] {
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let role = if sender { Role::Receiver } else { Role::Sender };
            let attach = |s: Session| async move {
                if sender {
                    s.build_sender_link("l", "l").attach().await.map(|_| ())
                } else {
                    s.build_receiver_link("l", "l").attach().await.map(|_| ())
                }
            };
            {
                let mut fut = std::pin::pin!(attach(s.clone()));
                let pending =
                    std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_pending()))
                        .await;
                assert!(pending);
                if delivered {
                    let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0))
                    else {
                        panic!()
                    };
                }
            }
            if !delivered {
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0)) else {
                    panic!()
                };
            }
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let ctx = format!("sender: {sender} delivered: {delivered}");
            assert_eq!(
                frame_names(&client),
                ["Begin", "Attach l 0", "Detach 0"],
                "{ctx}"
            );
            assert!(s.get_sender_link("l").is_none(), "{ctx}");
            assert!(s.get_sender_link_by_local_handle(0).is_none(), "{ctx}");
            assert!(s.get_receiver_link_by_local_handle(0).is_none(), "{ctx}");

            // name and handles are released on remote detach
            let Ok(Action::None) = handle_frame(&conn, peer_detach(0)) else {
                panic!()
            };
            let fut = ntex::rt::spawn(attach(s.clone()));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0)) else {
                panic!()
            };
            ntex::time::timeout(ntex::time::Millis(1000), fut)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(frame_names(&client), ["Attach l 0"], "{ctx}");
        }
    }
}

#[ntex::test]
async fn cancelled_local_attach_slot_reused() {
    for sender in [true, false] {
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let role = if sender { Role::Receiver } else { Role::Sender };
        let attach = |s: Session, name: &'static str| async move {
            if sender {
                s.build_sender_link(name, "l").attach().await.map(|_| ())
            } else {
                s.build_receiver_link(name, "l").attach().await.map(|_| ())
            }
        };
        {
            let mut fut = std::pin::pin!(attach(s.clone(), "l"));
            let pending =
                std::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx).is_pending())).await;
            assert!(pending);
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "l", "l", 0)) else {
                panic!()
            };

            // remote detach releases slot, new link reuses it
            handle_frame(&conn, peer_detach(0)).unwrap();
            let new = ntex::rt::spawn(attach(s.clone(), "m"));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "m", "l", 1)) else {
                panic!()
            };
            ntex::time::timeout(ntex::time::Millis(1000), new)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        ntex::time::sleep(ntex::time::Millis(10)).await;
        assert_eq!(
            frame_names(&client),
            ["Begin", "Attach l 0", "Detach 0", "Attach m 0"],
            "sender: {sender}"
        );
        if sender {
            assert!(s.get_sender_link("m").is_some());
        } else {
            assert!(s.get_receiver_link_by_local_handle(0).is_some());
        }
    }
}

enum TestLink {
    Sender(SenderLink),
    Receiver(crate::ReceiverLink),
}

impl TestLink {
    async fn attach(
        s: Session,
        sender: bool,
        name: &'static str,
    ) -> Result<Self, AmqpProtocolError> {
        if sender {
            s.build_sender_link(name, "l")
                .attach()
                .await
                .map(TestLink::Sender)
        } else {
            s.build_receiver_link(name, "l")
                .attach()
                .await
                .map(TestLink::Receiver)
        }
    }

    async fn close(self) -> Result<(), AmqpProtocolError> {
        match self {
            TestLink::Sender(l) => l.close().await,
            TestLink::Receiver(l) => l.close().await,
        }
    }
}

#[ntex::test]
async fn duplicate_local_link_name() {
    for sender in [true, false] {
        let ctx = format!("sender: {sender}");
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let role = if sender { Role::Receiver } else { Role::Sender };
        let other = if sender { Role::Sender } else { Role::Receiver };
        let in_use = async |sender| {
            let res = ntex::time::timeout(
                ntex::time::Millis(1000),
                TestLink::attach(s.clone(), sender, "x"),
            )
            .await
            .unwrap();
            assert!(
                matches!(res, Err(AmqpProtocolError::LinkNameInUse)),
                "{ctx}"
            );
        };

        // name is used by opening link
        let first = ntex::rt::spawn(TestLink::attach(s.clone(), sender, "x"));
        ntex::time::sleep(ntex::time::Millis(10)).await;
        in_use(sender).await;

        let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 0)) else {
            panic!("{ctx}")
        };
        let first = ntex::time::timeout(ntex::time::Millis(1000), first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // name is used by established link
        in_use(sender).await;

        // same name in other direction is a different link
        let opposite = ntex::rt::spawn(TestLink::attach(s.clone(), !sender, "x"));
        ntex::time::sleep(ntex::time::Millis(10)).await;
        let Ok(Action::None) = handle_frame(&conn, named_attach(other, "x", "l", 1)) else {
            panic!("{ctx}")
        };
        ntex::time::timeout(ntex::time::Millis(1000), opposite)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        ntex::time::sleep(ntex::time::Millis(10)).await;
        assert_eq!(
            frame_names(&client),
            ["Begin", "Attach x 0", "Attach x 1"],
            "{ctx}"
        );

        // name of closing link can be reused
        let closed = ntex::rt::spawn(first.close());
        ntex::time::sleep(ntex::time::Millis(10)).await;
        let second = ntex::rt::spawn(TestLink::attach(s.clone(), sender, "x"));
        ntex::time::sleep(ntex::time::Millis(10)).await;
        handle_frame(&conn, peer_detach(0)).unwrap();
        let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 2)) else {
            panic!("{ctx}")
        };
        ntex::time::timeout(ntex::time::Millis(1000), closed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        ntex::time::timeout(ntex::time::Millis(1000), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        ntex::time::sleep(ntex::time::Millis(10)).await;
        assert_eq!(frame_names(&client), ["Detach 0", "Attach x 2"], "{ctx}");
        if sender {
            assert_eq!(s.get_sender_link("x").unwrap().remote_handle(), 2, "{ctx}");
        } else {
            let link = s.get_receiver_link_by_remote_handle(2).unwrap();
            assert_eq!(link.handle(), 2, "{ctx}");
        }
    }
}

#[ntex::test]
async fn local_link_name_used_by_remote_link() {
    for sender in [true, false] {
        let ctx = format!("sender: {sender}");
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);

        // remote receiver opens sender link, remote sender opens receiver link
        let role = if sender { Role::Receiver } else { Role::Sender };
        match handle_frame(&conn, named_attach(role, "x", "l", 0)) {
            Ok(Action::AttachSender(..)) if sender => (),
            Ok(Action::AttachReceiver(..)) if !sender => (),
            _ => panic!("{ctx}"),
        }
        let res = ntex::time::timeout(
            ntex::time::Millis(1000),
            TestLink::attach(s.clone(), sender, "x"),
        )
        .await
        .unwrap();
        assert!(
            matches!(res, Err(AmqpProtocolError::LinkNameInUse)),
            "{ctx}"
        );
        ntex::time::sleep(ntex::time::Millis(10)).await;
        assert_eq!(frame_names(&client), ["Begin"], "{ctx}");
    }
}

#[ntex::test]
async fn link_attach_timeout() {
    // (config timeout, builder timeout)
    let cases = [(Seconds(1), None), (Seconds::ZERO, Some(Seconds(1)))];
    for sender in [true, false] {
        for (config, builder) in cases {
            let ctx = format!("sender: {sender} config: {config:?} builder: {builder:?}");
            let (_io, conn, client) =
                connection_with(AmqpServiceConfig::new().set_link_attach_timeout(config));
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let role = if sender { Role::Receiver } else { Role::Sender };

            let res = ntex::time::timeout(
                ntex::time::Millis(3000),
                attach_with_timeout(s.clone(), sender, builder),
            )
            .await
            .expect(&ctx);
            assert!(
                matches!(res, Err(AmqpProtocolError::LinkAttachTimeout)),
                "{ctx}"
            );

            // late attach response detaches link
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 0)) else {
                panic!("{ctx}")
            };
            ntex::time::sleep(ntex::time::Millis(10)).await;
            assert_eq!(
                frame_names(&client),
                ["Begin", "Attach x 0", "Detach 0"],
                "{ctx}"
            );
            handle_frame(&conn, peer_detach(0)).unwrap();

            // name is released
            let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, builder));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 1)) else {
                panic!("{ctx}")
            };
            ntex::time::timeout(ntex::time::Millis(1000), fut)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }
}

#[ntex::test]
async fn link_attached_before_timeout() {
    for sender in [true, false] {
        let ctx = format!("sender: {sender}");
        let (_io, conn, client) =
            connection_with(AmqpServiceConfig::new().set_link_attach_timeout(Seconds(1)));
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let role = if sender { Role::Receiver } else { Role::Sender };

        let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
        ntex::time::sleep(ntex::time::Millis(10)).await;
        let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 0)) else {
            panic!("{ctx}")
        };
        ntex::time::timeout(ntex::time::Millis(1000), fut)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        ntex::time::sleep(ntex::time::Millis(1500)).await;
        assert_eq!(frame_names(&client), ["Begin", "Attach x 0"], "{ctx}");
        if sender {
            assert!(s.get_sender_link("x").is_some(), "{ctx}");
        } else {
            assert!(s.get_receiver_link_by_local_handle(0).is_some(), "{ctx}");
        }
    }
}

#[ntex::test]
async fn refused_local_attach() {
    let not_found = Error(Box::new(codec::ErrorInner {
        condition: AmqpError::NotFound.into(),
        description: None,
        info: None,
    }));
    for sender in [true, false] {
        for error in [None, Some(not_found.clone())] {
            for cancel in [false, true] {
                let ctx = format!("sender: {sender} error: {error:?} cancel: {cancel}");
                let (_io, conn, client) = connection();
                handle_frame(&conn, begin()).unwrap();
                let s = session(&conn);
                let role = if sender { Role::Receiver } else { Role::Sender };

                let mut fut = Some(Box::pin(attach_with_timeout(s.clone(), sender, None)));
                let mut cx = Context::from_waker(std::task::Waker::noop());
                assert!(
                    fut.as_mut().unwrap().as_mut().poll(&mut cx).is_pending(),
                    "{ctx}"
                );

                // refused, attach waits for remote detach
                let Frame::Attach(mut attach) = named_attach(role, "x", "l", 3) else {
                    panic!()
                };
                if sender {
                    attach.0.target = None;
                } else {
                    attach.0.source = None;
                }
                let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
                    panic!("{ctx}")
                };
                assert!(
                    fut.as_mut().unwrap().as_mut().poll(&mut cx).is_pending(),
                    "{ctx}"
                );
                if cancel {
                    fut = None;
                }

                let detach = Detach(Box::new(DetachInner {
                    handle: 3,
                    closed: true,
                    error: error.clone(),
                }));
                let Ok(Action::None) = handle_frame(&conn, detach.into()) else {
                    panic!("{ctx}")
                };
                if let Some(mut fut) = fut {
                    let Poll::Ready(Err(AmqpProtocolError::LinkDetached(err))) =
                        fut.as_mut().poll(&mut cx)
                    else {
                        panic!("{ctx}")
                    };
                    assert_eq!(err, error, "{ctx}");
                }
                ntex::time::sleep(ntex::time::Millis(10)).await;
                assert_eq!(
                    frame_names(&client),
                    ["Begin", "Attach x 0", "Detach 0"],
                    "{ctx}"
                );

                // name and handle are released
                let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
                ntex::time::sleep(ntex::time::Millis(10)).await;
                let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 3)) else {
                    panic!("{ctx}")
                };
                ntex::time::timeout(ntex::time::Millis(1000), fut)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(frame_names(&client), ["Attach x 0"], "{ctx}");
            }
        }
    }
}

async fn detach_by_handle(
    s: &Session,
    sender: bool,
    handle: u32,
    error: Option<Error>,
) -> Result<(), AmqpProtocolError> {
    if sender {
        s.detach_sender_link(handle, error).await
    } else {
        s.detach_receiver_link(handle, error).await
    }
}

#[ntex::test]
async fn detach_opening_local_link() {
    for sender in [true, false] {
        let ctx = format!("sender: {sender}");
        let (_io, conn, client) = connection();
        handle_frame(&conn, begin()).unwrap();
        let s = session(&conn);
        let role = if sender { Role::Receiver } else { Role::Sender };

        let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
        ntex::time::sleep(ntex::time::Millis(10)).await;

        // attach response is not received
        let res = ntex::time::timeout(
            ntex::time::Millis(1000),
            detach_by_handle(&s, sender, 0, None),
        )
        .await
        .expect(&ctx);
        assert!(
            matches!(res, Err(AmqpProtocolError::LinkNotAttached)),
            "{ctx}"
        );

        // attach is not affected
        let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 3)) else {
            panic!("{ctx}")
        };
        ntex::time::timeout(ntex::time::Millis(1000), fut)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        ntex::time::sleep(ntex::time::Millis(10)).await;
        assert_eq!(frame_names(&client), ["Begin", "Attach x 0"], "{ctx}");
        if sender {
            assert!(s.get_sender_link("x").is_some(), "{ctx}");
        } else {
            assert!(s.get_receiver_link_by_local_handle(0).is_some(), "{ctx}");
        }
    }
}

#[ntex::test]
async fn detach_refused_local_link() {
    let not_found = Error(Box::new(codec::ErrorInner {
        condition: AmqpError::NotFound.into(),
        description: None,
        info: None,
    }));
    for sender in [true, false] {
        for error in [None, Some(not_found.clone())] {
            let ctx = format!("sender: {sender} error: {error:?}");
            let (_io, conn, client) = connection();
            handle_frame(&conn, begin()).unwrap();
            let s = session(&conn);
            let role = if sender { Role::Receiver } else { Role::Sender };
            let mut cx = Context::from_waker(std::task::Waker::noop());

            let mut fut = Box::pin(attach_with_timeout(s.clone(), sender, None));
            assert!(fut.as_mut().poll(&mut cx).is_pending(), "{ctx}");
            let Frame::Attach(mut attach) = named_attach(role, "x", "l", 3) else {
                panic!()
            };
            if sender {
                attach.0.target = None;
            } else {
                attach.0.source = None;
            }
            let Ok(Action::None) = handle_frame(&conn, attach.into()) else {
                panic!("{ctx}")
            };

            // local detach of refused link fails attach
            let mut detach = Box::pin(detach_by_handle(&s, sender, 0, error.clone()));
            assert!(detach.as_mut().poll(&mut cx).is_pending(), "{ctx}");
            let Poll::Ready(Err(AmqpProtocolError::LinkDetached(err))) = fut.as_mut().poll(&mut cx)
            else {
                panic!("{ctx}")
            };
            assert_eq!(err, error, "{ctx}");

            // remote detach completes local detach
            let Ok(Action::None) = handle_frame(&conn, peer_detach(3)) else {
                panic!("{ctx}")
            };
            let Poll::Ready(Ok(())) = detach.as_mut().poll(&mut cx) else {
                panic!("{ctx}")
            };
            ntex::time::sleep(ntex::time::Millis(10)).await;
            assert_eq!(
                frame_names(&client),
                ["Begin", "Attach x 0", "Detach 0"],
                "{ctx}"
            );

            // name and handle are released
            let fut = ntex::rt::spawn(attach_with_timeout(s.clone(), sender, None));
            ntex::time::sleep(ntex::time::Millis(10)).await;
            let Ok(Action::None) = handle_frame(&conn, named_attach(role, "x", "l", 3)) else {
                panic!("{ctx}")
            };
            ntex::time::timeout(ntex::time::Millis(1000), fut)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(frame_names(&client), ["Attach x 0"], "{ctx}");
        }
    }
}

#[ntex::test]
async fn links_limited_by_remote_handle_max() {
    let (_io, conn, client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));

    // remote handle-max is 1, two links are allowed
    let Frame::Begin(mut begin) = begin() else { panic!() };
    begin.0.handle_max = 1;
    handle(begin.into()).unwrap();
    let session = session(&conn);

    // remotely opened link
    let Frame::Attach(mut r0) = attach() else { panic!() };
    r0.0.name = "r0".into();
    let Ok(Action::AttachReceiver(..)) = handle(r0.into()) else {
        panic!()
    };

    // locally opened links
    let s = session.clone();
    ntex::rt::spawn(async move { s.build_sender_link("s1", LONG).attach().await });
    ntex::time::sleep(ntex::time::Millis(10)).await;
    let ms = ntex::time::Millis(100);
    let s2 = ntex::time::timeout(ms, session.build_sender_link("s2", LONG).attach());
    assert!(matches!(s2.await, Ok(Err(AmqpProtocolError::TooManyLinks))));
    let s3 = ntex::time::timeout(ms, session.build_receiver_link("s3", LONG).attach());
    assert!(matches!(s3.await, Ok(Err(AmqpProtocolError::TooManyLinks))));

    // remotely opened link, no free local handles
    let Frame::Attach(mut r1) = attach() else { panic!() };
    r1.0.name = "r1".into();
    r1.0.handle = 1;
    let Ok(Action::SessionEnded(_)) = handle(r1.into()) else {
        panic!()
    };

    ntex::time::sleep(ntex::time::Millis(50)).await;
    let codec = AmqpCodec::<AmqpFrame>::new();
    let mut buf = BytesMut::from(&client.read_any()[..]);
    let mut frames = Vec::new();
    while let Some(frame) = codec.decode(&mut buf).unwrap() {
        match frame.into_parts().1 {
            Frame::Attach(attach) => frames.push(format!("attach {}", attach.handle())),
            Frame::End(end) => frames.push(format!(
                "end {}",
                *end.error.unwrap().condition() == AmqpError::ResourceLimitExceeded.into()
            )),
            _ => (),
        }
    }
    assert_eq!(frames, ["attach 1", "end true"]);
}

#[ntex::test]
async fn local_link_names() {
    let (_io, conn, _client) = connection();
    let inner = conn.get_ref();
    let handle = |frame: Frame| inner.handle_frame(AmqpFrame::new(0, frame));
    handle(begin()).unwrap();
    let session = session(&conn);

    let s = session.clone();
    let fut = ntex::rt::spawn(async move { s.build_sender_link("x", "addr").attach().await });
    ntex::time::sleep(ntex::time::Millis(10)).await;

    // peer opens link with the same name in other direction
    let Ok(Action::AttachReceiver(..)) = handle(named_attach(Role::Sender, "x", "x", 0)) else {
        panic!()
    };
    assert!(!fut.is_finished());

    // local link confirmation
    let Ok(Action::None) = handle(named_attach(Role::Receiver, "x", "peer", 1)) else {
        panic!()
    };
    let link = fut.await.unwrap().unwrap();
    assert_eq!(link.name(), "x");
    assert_eq!(link.address().unwrap(), "peer");
    assert_eq!(session.get_sender_link("x").unwrap().id(), link.id());

    // name is removed after detach confirmation
    let l = link.clone();
    let fut = ntex::rt::spawn(async move { l.close().await });
    ntex::time::sleep(ntex::time::Millis(10)).await;
    handle(peer_detach(1)).unwrap();
    fut.await.unwrap().unwrap();
    assert!(session.get_sender_link("x").is_none());
    assert!(!session.inner.get_ref().sender_names.contains_key("x"));

    // remote attach with the name of removed link
    let Ok(Action::AttachSender(..)) = handle(named_attach(Role::Receiver, "x", "x", 2)) else {
        panic!()
    };
}
