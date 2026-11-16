use super::*;

fn channel_frame(
    conn: &Connection,
    channel: u16,
    frame: Frame,
) -> Result<Action, AmqpProtocolError> {
    conn.get_ref().handle_frame(AmqpFrame::new(channel, frame))
}

fn remote_begin(remote_channel: Option<u16>) -> Frame {
    let Frame::Begin(mut begin) = begin() else { panic!() };
    begin.0.remote_channel = remote_channel;
    begin.into()
}

/// STATEMENT: Connection accessors report state, and `force_close` marks the connection
/// failed, wakes `on_close` waiters and drops sessions.
///
/// HOW:
/// - Check `tag`, `config`, `is_opened`, `get_error` and `Debug` on a fresh connection.
/// - Handle a remote `Begin` and confirm session 0 is found by local id.
/// - Poll `on_close` (pending), call `force_close`, then confirm it is ready, the error
///   is `ConnectionDropped` and session 0 is gone.
#[ntex::test]
async fn connection_accessors() {
    let (_io, conn, _client) = connection();
    assert_eq!(conn.tag(), "T");
    assert_eq!(conn.config().tag(), "T");
    assert!(conn.is_opened());
    assert!(conn.get_error().is_none());
    assert!(conn.get_session_by_local_id(0).is_none());
    assert_eq!(format!("{:?}", conn.get_ref()), "ConnectionRef");

    handle_frame(&conn, begin()).unwrap();
    assert_eq!(conn.get_session_by_local_id(0).unwrap().inner.id(), 0);

    let mut waiter = conn.on_close();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut waiter).poll(&mut cx).is_pending());

    conn.force_close();
    assert!(!conn.is_opened());
    assert!(matches!(
        conn.get_error(),
        Some(AmqpProtocolError::ConnectionDropped)
    ));
    assert!(Pin::new(&mut waiter).poll(&mut cx).is_ready());

    // sessions are dropped on error
    assert!(conn.get_session_by_local_id(0).is_none());
}

/// STATEMENT: Local `close` and `close_with_error` write a `Close` frame without and
/// with the given error.
///
/// HOW:
/// - Call `close` and confirm a single `Close` frame with no error is written.
/// - On a new connection call `close_with_error` with description `"bye"` and confirm
///   the written `Close` carries that description.
#[ntex::test]
async fn close_frames() {
    // local close without and with an error
    let (_io, conn, client) = connection();
    conn.close().await.unwrap();
    sleep(Millis(25)).await;
    let [Frame::Close(close)] = &read_frames(&client)[..] else {
        panic!()
    };
    assert!(close.error.is_none());

    let (_io, conn, client) = connection();
    conn.close_with_error(crate::error::LinkError::force_detach().description("bye"))
        .await
        .unwrap();
    sleep(Millis(25)).await;
    let [Frame::Close(close)] = &read_frames(&client)[..] else {
        panic!()
    };
    assert_eq!(
        close
            .error
            .as_ref()
            .unwrap()
            .description()
            .map(ntex_bytes::ByteString::as_str),
        Some("bye")
    );
}

/// STATEMENT: A remote `Close` is reported with its error, answered with an error-free
/// `Close`, and later frames are ignored.
///
/// HOW:
/// - Handle a remote `Close` with description `"remote"` and confirm
///   `Action::RemoteClose(Closed(Some(_)))` carries it.
/// - Confirm a single `Close` without error is written back.
/// - Confirm a following `Begin` yields `Action::None`.
#[ntex::test]
async fn remote_close_responds_and_reports() {
    let (_io, conn, client) = connection();
    let err = Error(Box::new(codec::ErrorInner {
        condition: AmqpError::InternalError.into(),
        description: Some("remote".into()),
        info: None,
    }));
    let Ok(Action::RemoteClose(AmqpProtocolError::Closed(Some(reported)))) =
        handle_frame(&conn, Close { error: Some(err) }.into())
    else {
        panic!()
    };
    assert_eq!(
        reported.description().map(ntex_bytes::ByteString::as_str),
        Some("remote")
    );

    // a `Close` response without error is sent back
    sleep(Millis(25)).await;
    let [Frame::Close(close)] = &read_frames(&client)[..] else {
        panic!()
    };
    assert!(close.error.is_none());

    // further frames are ignored
    assert!(matches!(handle_frame(&conn, begin()), Ok(Action::None)));
}

/// STATEMENT: A peer `Close` after a local close confirms it without a reply and leaves
/// the connection `Disconnected`.
///
/// HOW:
/// - Call `close`, confirm `is_opened` is false and a `Close` without error is written.
/// - Handle the peer `Close` and confirm `Action::None` and a `Disconnected` error.
/// - Confirm no further frames are written.
#[ntex::test]
async fn close_confirmation() {
    let (_io, conn, client) = connection();
    conn.close().await.unwrap();
    assert!(!conn.is_opened());
    sleep(Millis(25)).await;
    let [Frame::Close(close)] = &read_frames(&client)[..] else {
        panic!()
    };
    assert!(close.error.is_none());

    // peer's `Close` confirms local close, nothing is sent back
    assert!(matches!(
        handle_frame(&conn, Close { error: None }.into()),
        Ok(Action::None)
    ));
    assert!(matches!(
        conn.get_error(),
        Some(AmqpProtocolError::Disconnected)
    ));
    sleep(Millis(25)).await;
    assert_eq!(read_frames(&client), Vec::<Frame>::new());
}

/// STATEMENT: A remote `Begin` is rejected when its channel is in use or the channel
/// limit is exceeded.
///
/// HOW:
/// - Handle `Begin` twice on channel 0 and confirm the second is
///   `Unexpected(Frame::Begin)`.
/// - With `channel_max(1)`, begin channels 0 and 1, then confirm channel 2 fails with
///   `TooManyChannels`.
#[ntex::test]
async fn begin_errors() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();

    // channel is already in use
    assert!(matches!(
        handle_frame(&conn, begin()),
        Err(AmqpProtocolError::Unexpected(Frame::Begin(_)))
    ));

    // too many channels
    let (_io, conn, _client) = connection_with(AmqpServiceConfig::new().set_channel_max(1));
    channel_frame(&conn, 0, begin()).unwrap();
    channel_frame(&conn, 1, begin()).unwrap();
    assert!(matches!(
        channel_frame(&conn, 2, begin()),
        Err(AmqpProtocolError::TooManyChannels)
    ));
}

/// STATEMENT: A `Begin` response that names an unknown or already established local
/// channel is ignored.
///
/// HOW:
/// - Handle a `Begin` with `remote_channel` 3; confirm `Action::None` and no session 3.
/// - Establish session 0, handle a `Begin` with `remote_channel` 0 on channel 2 and
///   confirm `Action::None` with session 0 unchanged.
#[ntex::test]
async fn begin_response_for_unexpected_channel() {
    let (_io, conn, _client) = connection();

    // begin response for an unknown local channel is ignored
    assert!(matches!(
        channel_frame(&conn, 1, remote_begin(Some(3))),
        Ok(Action::None)
    ));
    assert!(conn.get_session_by_local_id(3).is_none());

    // begin response for an established (not opening) channel is ignored
    handle_frame(&conn, begin()).unwrap();
    assert!(matches!(
        channel_frame(&conn, 2, remote_begin(Some(0))),
        Ok(Action::None)
    ));
    assert_eq!(conn.get_session_by_local_id(0).unwrap().inner.id(), 0);
}

/// STATEMENT: Frames for a channel without an established session are reported as
/// `UnknownSession`, and are dropped once the connection has failed.
///
/// HOW:
/// - Handle `Attach` with no session and confirm `UnknownSession`.
/// - Start a local `open_session` and confirm `Attach` is still `UnknownSession`.
/// - Call `force_close`, confirm the pending open fails with `Disconnected` and a later
///   `Attach` yields `Action::None`.
#[ntex::test]
async fn frames_for_unknown_and_opening_sessions() {
    let (_io, conn, _client) = connection();

    // no session for the channel
    assert!(matches!(
        handle_frame(&conn, attach()),
        Err(AmqpProtocolError::UnknownSession(_))
    ));

    // a session that is still opening is not registered in the channel map,
    // so its frames are reported as unknown as well
    let fut = ntex::rt::spawn({
        let c = conn.get_ref();
        async move { c.open_session().attach().await }
    });
    sleep(Millis(25)).await;
    assert!(matches!(
        handle_frame(&conn, attach()),
        Err(AmqpProtocolError::UnknownSession(_))
    ));

    conn.force_close();
    assert!(matches!(
        fut.await.unwrap(),
        Err(AmqpProtocolError::Disconnected)
    ));

    // frames received after the connection failed are dropped
    assert!(matches!(handle_frame(&conn, attach()), Ok(Action::None)));
}

/// STATEMENT: Frames for a closing session are dropped, the peer `End` completes the
/// close, and the session is then removed.
///
/// HOW:
/// - Establish a session and start `end` on it.
/// - Confirm `Attach` and `End` both yield `Action::None` and the `end` future succeeds.
/// - Confirm a later `Attach` fails with `UnknownSession`.
#[ntex::test]
async fn frames_for_closing_session() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let s = session(&conn);
    let fut = ntex::rt::spawn(async move { s.end().await });
    sleep(Millis(25)).await;

    // session is in `Closing` state, non-end frames are dropped
    assert!(matches!(handle_frame(&conn, attach()), Ok(Action::None)));
    assert!(matches!(
        handle_frame(&conn, End { error: None }.into()),
        Ok(Action::None)
    ));
    fut.await.unwrap().unwrap();

    // session is removed
    assert!(matches!(
        handle_frame(&conn, attach()),
        Err(AmqpProtocolError::UnknownSession(_))
    ));
}

/// STATEMENT: A remote `Attach` with a handle above `handle_max` or already in use ends
/// the session with the matching error condition.
///
/// HOW:
/// - With `handle_max(10)`, attach link `"a"` on handle 0.
/// - Attach link `"b"` on handle 11 and on handle 0 and confirm `Action::SessionEnded`.
/// - Confirm the last written `End` carries `ResourceLimitExceeded` or `HandleInUse`, and
///   a further `Attach` yields `Action::None`.
#[ntex::test]
async fn attach_handle_limits() {
    // (handle, expected error condition)
    let cases = [
        (
            11,
            codec::ErrorCondition::AmqpError(AmqpError::ResourceLimitExceeded),
        ),
        (
            0,
            codec::ErrorCondition::SessionError(SessionError::HandleInUse),
        ),
    ];

    for (handle, condition) in cases {
        let (_io, conn, client) = connection_with(AmqpServiceConfig::new().set_handle_max(10));
        handle_frame(&conn, begin()).unwrap();
        handle_frame(&conn, named_attach(Role::Sender, "a", "a", 0)).unwrap();

        let Ok(Action::SessionEnded(_)) =
            handle_frame(&conn, named_attach(Role::Sender, "b", "b", handle))
        else {
            panic!()
        };
        sleep(Millis(25)).await;

        let frames = read_frames(&client);
        let Some(Frame::End(end)) = frames.last() else {
            panic!()
        };
        assert_eq!(end.error.as_ref().unwrap().condition(), &condition);

        // session is closing, further frames are ignored
        assert!(matches!(handle_frame(&conn, attach()), Ok(Action::None)));
    }
}

/// STATEMENT: `open_session` sends a `Begin` with the configured capabilities and
/// properties and resolves when the peer responds.
///
/// HOW:
/// - Open a session with offered/desired capabilities and two properties.
/// - Confirm the `Begin` on channel 0 has no `remote_channel` and carries those values.
/// - Respond with a `Begin` on channel 5 and confirm the session has local id 0 and
///   remote channel 5.
#[ntex::test]
async fn open_session_builder() {
    let (_io, conn, client) = connection();
    let fut = ntex::rt::spawn({
        let c = conn.get_ref();
        async move {
            c.open_session()
                .offered_capabilities(symbols())
                .desired_capabilities(symbols())
                .property("k1", 1u32)
                .property("k2", 2u32)
                .await
        }
    });
    sleep(Millis(25)).await;

    let [(channel, Frame::Begin(begin))] = &read_channel_frames(&client)[..] else {
        panic!()
    };
    assert_eq!(*channel, 0);
    assert!(begin.remote_channel().is_none());
    assert_eq!(begin.offered_capabilities(), Some(&symbols()));
    assert_eq!(begin.desired_capabilities(), Some(&symbols()));
    assert_eq!(begin.properties().unwrap().len(), 2);

    channel_frame(&conn, 5, remote_begin(Some(0))).unwrap();
    let session = fut.await.unwrap().unwrap();
    assert_eq!(session.inner.id(), 0);
    assert_eq!(session.remote_channel_id(), 5);
}

/// STATEMENT: `open_session` fails with `ConnectionDropped` on a failed connection and
/// with `TooManyChannels` when the local channel limit is reached.
///
/// HOW:
/// - Call `force_close` and confirm `open_session` fails with `ConnectionDropped`.
/// - With `channel_max(0)`, start one `open_session`, then confirm a second fails with
///   `TooManyChannels`.
/// - Call `force_close` and confirm the first open fails.
#[ntex::test]
async fn open_session_errors() {
    // connection in error state
    let (_io, conn, _client) = connection();
    conn.force_close();
    assert!(matches!(
        conn.open_session().attach().await,
        Err(AmqpProtocolError::ConnectionDropped)
    ));

    // local channel limit
    let (_io, conn, _client) = connection_with(AmqpServiceConfig::new().set_channel_max(0));
    let fut = ntex::rt::spawn({
        let c = conn.get_ref();
        async move { c.open_session().attach().await }
    });
    sleep(Millis(25)).await;
    assert!(matches!(
        conn.open_session().attach().await,
        Err(AmqpProtocolError::TooManyChannels)
    ));

    conn.force_close();
    assert!(fut.await.unwrap().is_err());
}
