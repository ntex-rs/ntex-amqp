//! Display/From/accessor coverage for `error.rs`, `types.rs`, `state.rs`,
//! `default.rs` and the configuration builders in `lib.rs`.
use std::io;

use ntex_amqp_codec::protocol::{
    self as codec, Attach, DeliveryState, ErrorCondition, Fields, Frame, Open, OpenInner, Rejected,
    Role, Symbols,
};
use ntex_amqp_codec::types::{Multiple, Symbol, Variant, VecSymbolMap};
use ntex_amqp_codec::{AmqpCodecError, AmqpParseError, Decode, Encode};
use ntex_bytes::BytePages;
use ntex_error::ErrorDiagnostic;
use ntex_service::{Pipeline, ServiceFactory};
use ntex_util::time::Seconds;

use crate::control::{ControlFrame, ControlFrameKind};
use crate::default::{DefaultControlService, DefaultPublishService};
use crate::error::{AmqpDispatcherError, AmqpError, AmqpProtocolError, Error, LinkError};
use crate::tests::connection::{begin, connection, handle_frame, named_attach};
use crate::types::{Action, Link, Outcome, Wrapper};
use crate::{AmqpServiceConfig, RemoteServiceConfig, State, detach, error_code};

/// Wire symbol of an error condition
fn symbol(cond: &ErrorCondition) -> Symbol {
    let mut buf = BytePages::default();
    cond.encode(&mut buf);
    let mut buf = buf.freeze();
    Symbol::decode(&mut buf).unwrap()
}

fn fields() -> VecSymbolMap {
    VecSymbolMap(vec![(Symbol::from("k"), Variant::from("v"))])
}

#[test]
fn protocol_error_display() {
    let cases: Vec<(AmqpProtocolError, &str)> = vec![
        (
            AmqpProtocolError::Codec(AmqpCodecError::MaxSizeExceeded),
            "Codec error: MaxSizeExceeded",
        ),
        (AmqpProtocolError::TooManyChannels, "Too many channels"),
        (AmqpProtocolError::TooManyLinks, "Too many links"),
        (AmqpProtocolError::LinkNameInUse, "Link name is in use"),
        (AmqpProtocolError::LinkAttachTimeout, "Link attach timeout"),
        (AmqpProtocolError::LinkNotAttached, "Link is not attached"),
        (AmqpProtocolError::BodyTooLarge, "Body is too large"),
        (AmqpProtocolError::KeepAliveTimeout, "Keep-alive timeout"),
        (AmqpProtocolError::ReadTimeout, "Read timeout"),
        (AmqpProtocolError::WriteTimeout, "Write timeout"),
        (AmqpProtocolError::Disconnected, "Disconnected"),
        (
            AmqpProtocolError::ConnectionDropped,
            "Connection is dropped",
        ),
        (
            AmqpProtocolError::UnknownSession(Frame::Empty),
            "Unknown session: Empty",
        ),
        (
            AmqpProtocolError::UnknownLink(Frame::Empty),
            "Unknown link in session: Empty",
        ),
        (
            AmqpProtocolError::Closed(None),
            "Connection closed, error: None",
        ),
        (
            AmqpProtocolError::SessionEnded(None),
            "Session ended, error: None",
        ),
        (
            AmqpProtocolError::LinkDetached(None),
            "Link detached, error: None",
        ),
        (
            AmqpProtocolError::UnexpectedOpeningState(Frame::Empty),
            "Unexpected frame for opening state, got: Empty",
        ),
        (
            AmqpProtocolError::Unexpected(Frame::Empty),
            "Unexpected frame: Empty",
        ),
    ];

    for (err, expected) in cases {
        assert_eq!(err.to_string(), expected, "{err:?}");
        // all protocol errors are cloneable
        assert_eq!(err.clone().to_string(), expected);
    }

    assert_eq!(
        AmqpProtocolError::TooManyLinks.signature(),
        "ntex-amqp-protocol"
    );
}

#[test]
fn protocol_error_from_parse_error() {
    // parse errors are wrapped into codec errors
    let err = AmqpProtocolError::from(AmqpParseError::InvalidSize);
    assert!(matches!(
        err,
        AmqpProtocolError::Codec(AmqpCodecError::ParseError(AmqpParseError::InvalidSize))
    ));
    assert_eq!(err.to_string(), "Codec error: ParseError(InvalidSize)");

    let err = AmqpProtocolError::from(AmqpCodecError::UnparsedBytesLeft);
    assert_eq!(err.to_string(), "Codec error: UnparsedBytesLeft");
}

#[test]
fn dispatcher_error() {
    let err = AmqpDispatcherError::Service;
    assert_eq!(err.to_string(), "Service error");
    assert!(matches!(err.clone(), AmqpDispatcherError::Service));

    let err = AmqpDispatcherError::from(AmqpProtocolError::TooManyChannels);
    assert_eq!(err.to_string(), "Amqp protocol error: TooManyChannels");
    let AmqpDispatcherError::Protocol(inner) = err.clone() else {
        panic!()
    };
    assert_eq!(inner.to_string(), "Too many channels");

    // io error is not `Clone`, kind and message are preserved
    let err = AmqpDispatcherError::Disconnected(Some(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "pipe is gone",
    )));
    let AmqpDispatcherError::Disconnected(Some(io_err)) = err.clone() else {
        panic!()
    };
    assert_eq!(io_err.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(io_err.to_string(), "pipe is gone");
    assert!(err.to_string().starts_with("Peer disconnected error: "));

    let err = AmqpDispatcherError::Disconnected(None);
    assert_eq!(err.to_string(), "Peer disconnected error: None");
    assert!(matches!(
        err.clone(),
        AmqpDispatcherError::Disconnected(None)
    ));
}

#[test]
fn amqp_error_conditions() {
    let cases: Vec<(AmqpError, codec::AmqpError, Symbol)> = vec![
        (
            AmqpError::internal_error(),
            codec::AmqpError::InternalError,
            error_code::INTERNAL_ERROR,
        ),
        (
            AmqpError::not_found(),
            codec::AmqpError::NotFound,
            error_code::NOT_FOUND,
        ),
        (
            AmqpError::unauthorized_access(),
            codec::AmqpError::UnauthorizedAccess,
            error_code::UNAUTHORIZED_ACCESS,
        ),
        (
            AmqpError::decode_error(),
            codec::AmqpError::DecodeError,
            error_code::DECODE_ERROR,
        ),
        (
            AmqpError::invalid_field(),
            codec::AmqpError::InvalidField,
            error_code::INVALID_FIELD,
        ),
        (
            AmqpError::not_allowed(),
            codec::AmqpError::NotAllowed,
            error_code::NOT_ALLOWED,
        ),
        (
            AmqpError::not_implemented(),
            codec::AmqpError::NotImplemented,
            error_code::NOT_IMPLEMENTED,
        ),
        (
            AmqpError::new(codec::AmqpError::ResourceLimitExceeded),
            codec::AmqpError::ResourceLimitExceeded,
            error_code::RESOURCE_LIMIT_EXCEEDED,
        ),
    ];

    for (err, condition, sym) in cases {
        let err: Error = err.into();
        assert_eq!(*err.condition(), ErrorCondition::AmqpError(condition));
        assert_eq!(symbol(err.condition()), sym);
        assert_eq!(err.description(), None);
        assert_eq!(err.info(), None);
    }

    // custom condition is passed through as is
    let custom = ErrorCondition::Custom(Symbol::from("my:error"));
    let err: Error = AmqpError::with_error(custom.clone())
        .text("static text")
        .into();
    assert_eq!(*err.condition(), custom);
    assert_eq!(
        err.description().map(ntex_bytes::ByteString::as_str),
        Some("static text")
    );

    // `description` accepts owned values
    let err: Error = AmqpError::not_found()
        .description(format!("no {} here", "node"))
        .into();
    assert_eq!(
        err.description().map(ntex_bytes::ByteString::as_str),
        Some("no node here")
    );

    // amqp error converts into a rejecting outcome
    let outcome = Outcome::try_from(AmqpError::not_allowed()).unwrap();
    assert_eq!(
        outcome.into_delivery_state(),
        DeliveryState::Rejected(Rejected {
            error: Some(AmqpError::not_allowed().into())
        })
    );
}

#[test]
fn amqp_error_display() {
    assert_eq!(
        AmqpError::internal_error().to_string(),
        "Amqp error: Left(InternalError) None (None)"
    );
    assert_eq!(
        AmqpError::not_found().text("gone").to_string(),
        "Amqp error: Left(NotFound) Some(\"gone\") (None)"
    );
}

#[test]
fn link_error_conditions() {
    let err: Error = LinkError::force_detach().into();
    assert_eq!(
        *err.condition(),
        ErrorCondition::LinkError(codec::LinkError::DetachForced)
    );
    assert_eq!(symbol(err.condition()), error_code::DETACH_FORCED);

    let err: Error = LinkError::redirect().into();
    assert_eq!(
        *err.condition(),
        ErrorCondition::LinkError(codec::LinkError::Redirect)
    );
    assert_eq!(symbol(err.condition()), error_code::LINK_REDIRECT);

    // `new` takes a raw condition
    let err: Error = LinkError::new(ErrorCondition::LinkError(codec::LinkError::Stolen))
        .text("taken")
        .fields(fields())
        .into();
    assert_eq!(symbol(err.condition()), error_code::STOLEN);
    assert_eq!(
        err.description().map(ntex_bytes::ByteString::as_str),
        Some("taken")
    );
    assert_eq!(err.info(), Some(&fields()));

    let err: Error = LinkError::force_detach()
        .description(format!("address {} is unknown", "x"))
        .into();
    assert_eq!(
        err.description().map(ntex_bytes::ByteString::as_str),
        Some("address x is unknown")
    );

    let outcome = Outcome::try_from(LinkError::force_detach()).unwrap();
    let DeliveryState::Rejected(Rejected { error: Some(err) }) = outcome.into_delivery_state()
    else {
        panic!()
    };
    assert_eq!(symbol(err.condition()), error_code::DETACH_FORCED);
}

#[test]
fn link_error_display() {
    assert_eq!(
        LinkError::force_detach().to_string(),
        "Link error: Left(DetachForced) None (None)"
    );
    assert_eq!(
        LinkError::new(ErrorCondition::Custom(Symbol::from("x")))
            .text("t")
            .to_string(),
        "Link error: Right(Custom(Symbol(\"x\"))) Some(\"t\") (None)"
    );
}

#[test]
fn outcome_delivery_state() {
    assert_eq!(
        Outcome::Accept.into_delivery_state(),
        DeliveryState::Accepted(codec::Accepted {})
    );
    assert_eq!(
        Outcome::Reject.into_delivery_state(),
        DeliveryState::Rejected(Rejected { error: None })
    );

    // `Error` variant is built via `From`
    let err: Error = AmqpError::internal_error().into();
    let outcome: Outcome = Outcome::from(err.clone());
    assert_eq!(
        outcome.into_delivery_state(),
        DeliveryState::Rejected(Rejected { error: Some(err) })
    );
}

#[test]
fn wrapper_takes_value_once() {
    let w = Wrapper::new(1_u8);
    assert_eq!(w.with(|v| *v), 1);
    w.with(|v| *v = 2);
    assert_eq!(w.take(), 2);
}

#[test]
#[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
fn wrapper_take_twice_panics() {
    let w = Wrapper::new(());
    w.take();
    w.take();
}

#[test]
fn state_deref() {
    let state = State::new(vec![1_u8, 2]);
    assert_eq!(state.get_ref(), &[1, 2]);
    // `Deref` to the inner value
    assert_eq!(state.len(), 2);

    let cloned = state.clone();
    assert!(std::ptr::eq(cloned.get_ref(), state.get_ref()));
    assert!(format!("{state:?}").starts_with("State("));
}

#[ntex::test]
async fn link_accessors() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let Ok(Action::AttachReceiver(rcv, attach, _)) =
        handle_frame(&conn, named_attach(Role::Sender, "n", "addr/1", 3))
    else {
        panic!()
    };

    let state = State::new(7_u8);
    let mut link = Link::new(attach.clone(), rcv.clone(), state, "addr/1".into());
    assert_eq!(*link.state().get_ref(), 7);
    assert_eq!(link.handle(), rcv.handle());
    assert_eq!(link.frame().name(), attach.name());
    assert_eq!(link.path().get_ref(), "addr/1");
    assert_eq!(link.session().local_channel_id(), 0);
    assert_eq!(link.receiver(), &rcv);
    assert!(format!("{link:?}").contains("Link<S>"));

    // path is mutable, `receiver_mut` returns the same link
    link.path_mut().set("addr/2".into());
    assert_eq!(link.path().get_ref(), "addr/2");
    assert_eq!(link.receiver_mut().name(), "n");

    // credit is applied to the receiver link
    link.link_credit(5);
    assert_eq!(rcv.credit(), 5);

    let cloned = link.clone();
    assert_eq!(cloned.path().get_ref(), "addr/2");
    assert_eq!(cloned.handle(), link.handle());
}

#[ntex::test]
async fn default_services() {
    let (_io, conn, _client) = connection();
    handle_frame(&conn, begin()).unwrap();
    let Ok(Action::AttachReceiver(rcv, attach, _)) =
        handle_frame(&conn, named_attach(Role::Sender, "d", "d", 0))
    else {
        panic!()
    };
    let link = Link::new(attach, rcv, State::new(()), "d".into());

    // publish service is not configured, factory fails
    let factory = DefaultPublishService::<(), Error>::default();
    let err = ServiceFactory::<State<()>, Link<()>>::create(&factory, &State::new(()))
        .await
        .err()
        .unwrap();
    let err: Error = err.into();
    assert_eq!(symbol(err.condition()), error_code::DETACH_FORCED);
    assert_eq!(
        err.description().map(ntex_bytes::ByteString::as_str),
        Some("not configured")
    );

    // the service itself only warns
    let svc = Pipeline::new(
        State::new(()),
        DefaultPublishService::<(), Error>::default(),
    );
    svc.call(link).await.unwrap();

    // default control service accepts everything
    let factory = DefaultControlService::<()>::default();
    let svc = ServiceFactory::<State<()>, ControlFrame>::create(&factory, &State::new(()))
        .await
        .unwrap();
    let svc = Pipeline::new(State::new(()), svc);
    let frame = ControlFrame::new_kind(ControlFrameKind::Closed);
    assert!(frame.session().is_none());
    assert!(format!("{frame:?}").contains("Closed"));
    svc.call(frame).await.unwrap();
}

#[test]
fn service_config_getters() {
    let props: Fields = [(Symbol::from("p"), Variant::from(1_u32))]
        .into_iter()
        .collect();
    let cfg = AmqpServiceConfig::new()
        .set_channel_max(7)
        .set_handle_max(9)
        .set_max_frame_size(1024)
        .set_idle_timeout(30)
        .set_container_id("cid")
        .set_hostname("host")
        .set_offered_capabilities(Multiple(vec![Symbol::from("o")]))
        .set_desired_capabilities(Multiple(vec![Symbol::from("d")]))
        .set_properties(props.clone())
        .set_handshake_timeout(Seconds(3))
        .set_link_attach_timeout(Seconds(11));

    assert_eq!(cfg.channel_max, 7);
    assert_eq!(cfg.handle_max, 9);
    assert_eq!(cfg.get_max_frame_size(), 1024);
    assert_eq!(cfg.idle_time_out, 30_000);
    assert_eq!(cfg.handshake_timeout, Seconds(3));
    assert_eq!(cfg.link_attach_timeout, Seconds(11));
    assert_eq!(cfg.get_offered_capabilities(), [Symbol::from("o")]);
    assert_eq!(cfg.get_desired_capabilities(), [Symbol::from("d")]);

    // settings are propagated to the `Open` performative
    let open = cfg.to_open();
    assert_eq!(open.container_id(), "cid");
    assert_eq!(
        open.hostname().map(ntex_bytes::ByteString::as_str),
        Some("host")
    );
    assert_eq!(open.max_frame_size(), 1024);
    assert_eq!(open.channel_max(), 7);
    assert_eq!(open.idle_time_out(), Some(30_000));
    assert_eq!(
        open.offered_capabilities(),
        Some(&Multiple(vec![Symbol::from("o")]))
    );
    assert_eq!(open.properties(), Some(&props));

    // capabilities are not set by default
    let cfg = AmqpServiceConfig::default();
    assert!(cfg.get_offered_capabilities().is_empty());
    assert!(cfg.get_desired_capabilities().is_empty());
    assert_eq!(cfg.get_max_frame_size(), 16 * 1024);

    // generated container-id is a simple uuid, idle timeout is advertised
    let open = cfg.to_open();
    assert_eq!(open.container_id().len(), 32);
    assert!(open.hostname().is_none());
    assert_eq!(open.idle_time_out(), Some(120_000));
}

#[test]
fn service_config_unlimited() {
    let mut cfg = AmqpServiceConfig::new();
    cfg.max_frame_size = 0;
    cfg.idle_time_out = 0;

    // `0` is unlimited locally, max value is advertised
    let open = cfg.to_open();
    assert_eq!(open.max_frame_size(), u32::MAX);
    assert_eq!(open.idle_time_out(), None);
}

#[test]
#[should_panic(expected = "max frame size must be at least 512, got 511")]
fn service_config_min_frame_size() {
    let _ = AmqpServiceConfig::new().set_max_frame_size(511);
}

#[test]
fn remote_config() {
    let open = OpenInner {
        max_frame_size: 2048,
        channel_max: 5,
        idle_time_out: Some(8_000),
        hostname: Some("remote-host".into()),
        offered_capabilities: Some(Multiple(vec![Symbol::from("ro")])),
        desired_capabilities: Some(Multiple(vec![Symbol::from("rd")])),
        ..Default::default()
    };

    let cfg = RemoteServiceConfig::new(&Open(Box::new(open)));
    assert_eq!(cfg.max_frame_size, 2048);
    assert_eq!(cfg.channel_max, 5);
    assert_eq!(cfg.idle_time_out, 8_000);
    assert_eq!(cfg.hostname.as_deref(), Some("remote-host"));
    assert_eq!(
        cfg.offered_capabilities,
        Some(Multiple(vec![Symbol::from("ro")]))
    );
    assert_eq!(
        cfg.desired_capabilities,
        Some(Multiple(vec![Symbol::from("rd")]))
    );
    // keep-alive is sent at 75% of the remote idle timeout
    assert_eq!(cfg.timeout_remote_secs(), Seconds(6));

    // no idle timeout disables keep-alive
    let cfg = RemoteServiceConfig::new(&Open(Box::default()));
    assert_eq!(cfg.idle_time_out, 0);
    assert!(cfg.hostname.is_none());
    assert!(cfg.offered_capabilities.is_none());
    assert_eq!(cfg.timeout_remote_secs(), Seconds::ZERO);
}

#[test]
fn detach_copies_value() {
    let caps: Symbols = Multiple(vec![Symbol::from("a-fairly-long-capability-name")]);
    assert_eq!(detach(&caps), caps);

    let attach = {
        let Frame::Attach(attach) = named_attach(Role::Sender, "name", "address", 1) else {
            panic!()
        };
        attach
    };
    let copy: Attach = detach(&attach);
    assert_eq!(copy.name(), attach.name());
    assert_eq!(copy.handle(), attach.handle());
    assert_eq!(copy.role(), attach.role());
    assert_eq!(
        copy.source().and_then(|s| s.address.clone()),
        Some("address".into())
    );
}
