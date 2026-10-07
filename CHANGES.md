# Changes

## [6.2.0] - Unreleased

* Fix server sasl challenge/response handshake, `SaslResponse::outcome()` waited for an extra sasl frame

* Fix codec failed to decode `ulong0` encoded descriptor

* Fix `AmqpParseError`, `ServerError` and `HandshakeError` display messages did not include error details

* Abort multi-frame delivery if receiver settled it before last frame is sent

* Fix router added link credit to link paused by `ReceiverLink::reset_link_credit(0)`

* Fix receiver ignored `settled` flag on continuation transfers, dropped delivery was rejected

* Fix duplicate delivery-id of unsettled delivery left original delivery without disposition,
  receiver link is detached

* Fix receiver link unsettled deliveries were not failed when remote peer confirmed local link detach

* Add `ReceiverLink::reset_link_credit()`, it can reduce link credit, transfers in flight under
  the previous credit are accepted

* Fix cancelled sender link transfers stayed in credit and session window queues

* Fix local link detach waited forever if remote peer did not confirm it, use link attach timeout

* Fix receiver link transfer errors re-entered session state while it was borrowed

* Fix receiver link did not apply sender's delivery-count from flow

* Fix `SenderLink::ready()` did not wait for remote session incoming window

* `ReceiverLink` debug output contains link name only, same as `SenderLink`

* `Session::detach_sender_link()`/`detach_receiver_link()` futures do not borrow `Session`, `Session` debug output includes channel ids

* Fix dispositions were sent for unsettled deliveries of detached links

* Fix `Delivery::wait()` returned on non-terminal disposition (no state or `Received`), consumed `Modified` outcome, and failed concurrent waiters with `ConnectionDropped`

* Fix `Delivery::wait()` returned `LinkDetached(None)` instead of session or connection error after session end

* Fix abort of cancelled delivery was not sent until next transfer on the same link; abort waits for session window in order with other transfers

* Fix new transfers could take link credit or session window of woken transfers; session flow wakes transfers up to the window, cancelled delivery abort does not take window of woken transfers

* Fix dropped unsettled sender delivery was settled as rejected; delivery is settled with remote outcome if received

* Fix delivery sent rejected disposition on drop if remote settled it before `wait()` call; `settle()`, `update_state()` and `is_remote_settled()` ignored remote settlement until `wait()` call

* Fix sender link drain was delayed by transfers waiting for session window

* Fix receiver link ignored aborted transfers; aborted deliveries are discarded, implicitly settled and their credit is returned to the sender

* Fix sender link flow woke all transfers waiting for credit or session window; waiters are woken up to available credit

* Fix router link service leaked until disconnect after local receiver link detach or local session end

* Fix receiver link recv() hung after local receiver link detach

* Fix transfers waiting for link credit or session window hung after local sender link detach; unsettled deliveries fail on detach confirmation

* Fix session remote incoming window wrapped on flow issued before remote got sent transfers; null flow next-incoming-id uses initial outgoing id

* Fix sender link credit wrapped on flow issued before receiver got sent transfers; null flow delivery-count uses initial delivery count

* Fix session window was consumed per delivery instead of per transfer frame, multi-frame deliveries could stall sending; delivery ids are separate from transfer ids, cancelled partially sent deliveries are aborted

* Fix session end and connection errors were lost for pending link attaches, receiver links and receiver link detaches

* Fix detach of not attached link panicked or hung, add `AmqpProtocolError::LinkNotAttached`

* Fix refused local link attach returned attached link

* Add local link attach timeout, `AmqpServiceConfig::set_link_attach_timeout()` and `attach_timeout()` link builder methods

* Fix local sender link did not set initial delivery count and used remote receiver value

* Fix duplicate local link name hung first link attach, add `AmqpProtocolError::LinkNameInUse`

* Fix cancelled local link attach leaked attached link

* Fix local receiver link ignored remote sender initial delivery count

* Fix link attach and session end hang if connection fails while session is ending

* Fix sender link transfers waiting for session window or link credit were not failed on link detach or close

* Fix cancelled transfer waiting for session window consumed sender link credit

* Fix session `outgoing-window` in `Begin` and `Flow` frames was set from remote incoming window

* Handle receiver `drain` request for sender links, remaining link credit is consumed and reported once queued transfers are sent

* Fix session frames (`Detach`, `Flow`, `Disposition`, `Attach`, `Transfer`) could be sent after `End`. Link close during session end completes without `Detach`, new link attach and transfer fail with session end error

* Fix `Flow` echo reply for attached link did not carry link state (handle, delivery-count, link-credit)

* Fix router kept a handler entry for every link whose link service failed to create, router link services were not shutdown on connection close

* Fix stale remote receiver link confirmation could establish another link reusing the same handle; link credit was sent for closed links and before the link's `Attach` response. Credit set before confirmation is sent after the `Attach` response

* Fix publish service `Message::Detached` and `Message::DetachedAll` calls were not cancelled on dispatcher shutdown, pending calls kept publish service alive after disconnect

* Fix publish service `Message::Attached` call for remotely opened receiver link was not cancelled on dispatcher shutdown, pending link service creation kept connection and link state alive after disconnect

* Fix remotely opened receiver link was not closed on session end before link confirmation and was not included in session ended links. Confirmation or rejection after session end sent `Attach` and `Detach` frames after `End`, publish service was notified with `Message::Attached` for closed link

* Fix remote `Detach` for remotely opened receiver link was ignored before link confirmation, link was confirmed with link credit and never detached, router link service was not released. Remote `Detach` is applied after control and publish services complete, link is detached and closed, control service is notified with `RemoteDetachReceiver` and publish service with `Message::Detached`

* Fix rejected remotely opened link released its handle before remote `Detach`, remote handle stayed registered for rejected receiver link. Remote `Detach` could close unrelated link, re-attach with the same remote handle ended session with `handle-in-use`. Rejected links keep handles until remote `Detach`, `Detach` with unknown remote handle is ignored

* Fix remotely opened sender link was registered by source address instead of link name, link names were not removed on link removal. Remote attach with the name of established, closing or unrelated link was ignored, attach with the name of opening local link in the same direction was handled as its confirmation. `SenderLink::name()` returns link name for remotely opened links, `Session::get_sender_link()` finds link by name. Add `SenderLink::address()` and `Session::get_sender_link_by_address()`

* Fix stale sender link `Flow` could overwrite newer link credit when control service calls completed out of order, and session flow state was applied only after control service call. Session and link flows are applied in frames order before control service is notified, control service calls are spawned and cancelled on dispatcher shutdown

* Fix server dispatcher stopped processing control frames, link attach confirmations, control service errors and keep-alive pings while publish service was not ready

* Fix remotely opened sender link waiting for control service confirmation was not closed on session end or connection error and was missing from session ended links. Confirmation after session end no longer sends `Attach`/`Detach` frames, rejected link is closed, rejection `Attach` uses sender role

* Fix remotely opened sender link lost link credit from `Flow` received before control service confirmation, and remote `Detach` received before confirmation was never answered and confirmed link stayed open. Detach is answered after confirmation, link is closed and `RemoteDetachSender` control frame is sent

* Respect remote session handle-max: opening a link without free handle fails with new `AmqpProtocolError::TooManyLinks` error, remotely opened link without free local handle ends the session with `amqp:resource-limit-exceeded` error

* Fix `Session::end()` used remote channel id to find the session, when local and remote channel ids differ it marked a wrong session as closing and sent a second `End` frame

* Fix max-message-size handling: remote max-message-size `0` means unlimited instead of rejecting all sends, locally attached receiver links use max size from `ReceiverLinkBuilder::max_message_size()` instead of hard-coded 256KiB, `0` max size means unlimited for receiver and sender links, single-frame transfers are checked against receiver max size

* Fix receiver link `delivery-count` overflow panic (with overflow checks) when remote `initial-delivery-count` is near `u32::MAX`, delivery count wraps around

* Enforce remote max-frame-size for all outgoing frames, oversized frames fail the connection with new `AmqpCodecError::MaxOutboundSizeExceeded` error instead of being sent. Add `AmqpCodec::max_encode_size()` and `AmqpCodec::set_max_encode_size()`

* Fix encoded sizes larger than `u32::MAX` were silently truncated, producing corrupted frames. Encoding now panics on 32-bit size overflow, transfers with unlimited remote max-frame-size are split into frames up to `u32::MAX`

* Reject remote `Open` with max-frame-size below 512 (`MIN_MAX_FRAME_SIZE`), new `HandshakeError::InvalidMaxFrameSize` and `ConnectError::InvalidMaxFrameSize` errors. `AmqpServiceConfig::set_max_frame_size()` panics on values below 512, `max_frame_size` field set to `0` is advertised as unlimited

* Fix `ReceiverLink::set_link_credit()` advertised only added credit in `Flow` instead of total link credit, credit overflow saturates

* Fix session `next-incoming-id` was not advanced on received transfers, always send it in `Flow` frames

* Fix memory amplification, received frames retained whole read buffers: small chunks of partial transfers are copied, stored `Begin`, `Attach`, `Open` data, delivery tags and states are detached from the read buffer

* Fix CPU exhaustion on remote `Disposition` with a large delivery-id range, the range is walked up to the number of unsettled deliveries. Ranges wrapping past `u32::MAX` are supported

* Add `AmqpServiceConfig::set_handle_max()`, limits remotely attached links per session (default 1024)

* End session on remote `Attach` with a handle already in use or above `handle-max`, orphaned links leaked memory

* Fix panic when a session ends while a remote sender link is waiting for confirmation

* Fix off-by-one in local session open, channel number equal to `channel-max` was rejected

* Reject remote `Begin` on a channel already in use or above `channel-max`, orphaned sessions leaked memory

* Remove `AmqpServiceConfig::set_max_size()`, inbound frames, including handshake frames (open, SASL), are limited by `max_frame_size`. Change default `max_frame_size` from 64kb to 16kb

* Add `AmqpProtocolError::WriteTimeout`, reported when write backpressure exceeds the write timeout

* codec: Fix panic when decoding a map with a map as a key (implement `Hash` for `VariantMap`)

* codec: Limit nesting depth of decoded lists, maps and described values, deep nesting overflowed the stack

* codec: Reject arrays with more elements than bytes, arrays of zero-width elements (`null`, `true`, `uint0`, ...) decoded to megabytes from a few bytes

* codec: Fix overflow when decoding a list, map or array with a declared size smaller than its count field

* codec: Decode list and array elements within the declared size, malformed lists desynced decoding of following fields

* codec: Fix stale `Message` encoded size after `set_value()`, `body_mut()` and `*_mut()` accessors

* codec: Make `Message` fields private, direct field changes left the cached encoded size stale. Add `Message::message_format()`, `delivery_annotation()`, `add_delivery_annotation()`, `footer()`, `footer_mut()` and `set_footer()`

* codec: Ignore cached encoded size when comparing `Message`s

* codec: Use ARRAY32 encoding for arrays with more than 255 elements, element count was truncated to u8

* codec: Fix `Multiple<T>` decoding of a described value, the error was `InvalidFormatCode(0x00)` instead of `InvalidDescriptor`

* codec: Fix decoding of negative timestamps, -1..-999 ms decoded as positive and whole-second values failed

* codec: Fix split transfer frames exceeding remote `max-frame-size`, frame header and transfer performative were not accounted for

## [6.0.0] - 2026-09-14

* Update to ntex-service 5.0

## [5.9.0] - 2026-08-12

* Allow to set container_id and properties for Open frame

## [5.8.0] - 2026-05-05

* Use new codec api with BytePages support

## [codec-2.3.0] - 2026-05-05

* Use new codec api with BytePages support

## [5.7.2] - 2026-04-02

* Update ntex-error 2.0

## [5.7.1] - 2026-03-26

* Update ntex-error::Error

## [codec-2.2.0] - 2026-03-11

* Add ListDescribed<T> type for list of compound types

## [5.7.0] - 2026-03-08

* Use ntex-error::Error for client

## [5.6.0] - 2026-02-16

* AmqpServiceConfig is not Clone

## [5.5.0] - 2026-02-16

* SharedCfg is not Copy

## [5.4.0] - 2026-01-29

* Use ntex_dispatcher::Dispatcher instead of ntex-io

## [5.3.0] - 2026-01-26

* Add `Begin` frame to `Session`

## [5.2.1] - 2026-01-16

* Fix link credit handling

## [5.2.0] - 2025-12-17

* Upgrade to ntex-service v4

## [5.1.2] - 2025-12-04

* Allow to set hostname for client connect

* Fix types for client connector

## [5.1.1] - 2025-12-04

* Add missing public exports

## [5.1.0] - 2025-12-03

* Refactor service configuration

## [5.0.0] - 2025-12-03

* Using individual ntex crates

## [5.0.0-pre.0] - 2025-11-28

* Update ntex to 3.0

* Use shared configuration

## [4.0.0] - 2025-09-10

* Add support for described compound types in Variant #68

* Use ahash instead of fxhash

## [codec-1.0.0] - 2025-09-10

* Add support for described compound types in Variant #68

* Use ahash instead of fxhash

## [3.6.0] - 2025-06-25

* Allow to configure open session (Begin) frame

* Refactor error types

## [codec-0.9.7] - 2025-04-24

* Add default impl to definitions

## [3.5.3] - 2025-04-10

* Allow to change response Attach frame

## [3.5.2] - 2025-04-03

* Allow to change client config

## [3.5.1] - 2025-03-25

* Allow to provide offered/desired capabilities

## [3.5.0] - 2025-01-29

* Rename DeliveryBuilder to TransferBuilder

## [3.4.0] - 2025-01-08

* Allow to set Transfer format

## [3.3.1] - 2025-01-02

* Fix rcv-settle-mode for sender links

## [3.3.0] - 2024-12-04

* Use updated Service trait

## [3.2.1] - 2024-12-03

* Handle unknown links

## [3.2.0] - 2024-12-03

* Fix control queue handling

## [3.1.0] - 2024-12-01

* Set "next_incoming_id" for Flow frame

## [3.0.4] - 2024-09-17

* Use derive_more 1.0

## [3.0.3] - 2024-07-12

* Tune log levels

## [3.0.2] - 2024-06-27

* Tune idle timeout

## [3.0.1] - 2024-06-04

* Better handling "session end" for inflight deliveries

## [3.0.0] - 2024-05-28

* Use ntex-service 3.0

## [2.1.7] - 2024-05-12

* Cleanup pending transfers and deliveries on link detach

## [codec-0.9.4] - 2024-04-30

* Add Variant::Array() type

## [codec-0.9.3] - 2024-04-29

* Fix `Variant::List` encoding

## [2.1.6] - 2024-04-29

* Give access to delivery tag

## [2.1.5] - 2024-04-17

* Fix receiver's delivery queue handling

## [2.1.4] - 2024-04-13

* Fix large transfers handling

* Fix Receiver link message size handling

## [2.1.3] - 2024-04-11

* Handle settled transfers

## [2.1.2] - 2024-03-17

* Set transfer handle

## [2.1.1] - 2024-03-12

* Fix default flow's next-incoming-id

## [2.1.0] - 2024-03-08

* Add proper delivery handling on receiver side

## [2.0.0] - 2024-03-06

* Add proper delivery handling

## [1.1.0] - 2024-03-04

* Add proper delivery handling

## [codec-0.9.2] - 2024-02-01

* Add more buffer length checks

## [1.0.2] - 2024-01-19

* SenderLink close notification

## [1.0.1] - 2024-01-18

* Fix SenderLink closed state, if link is closed remotely

## [1.0.0] - 2024-01-09

* Release

## [1.0.0-b.0] - 2024-01-07

* Use "async fn" in trait for Service definition

## [0.8.9] - 2024-01-04

* Remove internal circular references

## [0.8.8] - 2024-01-03

* Use io tags for logging

## [0.8.7] - 2023-12-04

* Fix overflow in Configuration::idle_timeout()

## [0.8.6] - 2023-11-27

* Better server builder

* Do not handle transfers if connection is down

## [0.8.5] - 2023-11-12

* Update io

## [0.8.4] - 2023-10-09

* Fix credit limit handling

## [0.8.2] - 2023-08-10

* Update ntex deps

## [0.8.1] - 2023-06-23

* Fix client connector usage, fixes lifetime constraint

## [0.8.0] - 2023-06-22

* Release v0.8.0

## [0.8.0-beta.3] - 2023-06-19

* Use ServiceCtx instead of Ctx

## [0.8.0-beta.2] - 2023-06-19

* Use container for client connector

## [0.8.0-beta.1] - 2023-06-19

* Make session id accessible

* Fix broken channel id handling

* Fix session managment for sender links

* Fix router leaks service handlers

* Local detach/end handling

* Send message to router that allows it to release service handlers for detached links

## [0.8.0-beta.0] - 2023-06-17

* Migrate to ntex-0.7

## [0.7.2] - 2023-05-11

* Fix session flow frame handling, could cause tight loop and 100% cpu consumption

## [0.7.1] - 2023-04-24

* Fix handling sync multiple control frames

* Add SendLink::ready() helper, allow to wait for available credit

* Add SendLink::on_credit_update() helper, allow to wait for credit updates

## [0.7.0] - 2023-01-04

* 0.7 Release

* Use uuid-1.2

## [0.7.0-beta.0] - 2022-12-28

* Migrate to ntex-service 1.0

## [0.6.4] - 2022-08-22

* Must respond with attach before detach when rejecting links #24

## [codec-0.8.2] - 2022-08-22

* Missing derives

## [0.6.3] - 2022-02-18

* Do not store Attach frame in ReceiverLink

* Expose available sender link

* Expose available session remove window size

## [0.6.2] - 2022-01-18

* Allow to change max message size for receiver link

## [0.6.1] - 2022-01-10

* Cleanup server errors

* Cleanup client connector interface

## [codec-0.8.1] - 2022-01-10

* Use new ByteString api

## [0.6.0] - 2021-12-30

* Upgrade to ntex 0.5.0

## [0.6.0-b.5] - 2021-12-28

* Make Server universal, accept both Io<F> and IoBoxed

## [0.6.0-b.4] - 2021-12-27

* Upgrade to ntex 0.5-b4

## [0.6.0-b.3] - 2021-12-24

* Upgrade to ntex-service 0.3.0

## [0.6.0-b.2] - 2021-12-22

* Add ReceiverLink::poll_recv() method, replace for Stream::poll_next()

* Allow to access io object during handshake

* Refactor AmqpDispatcherError, add Disconnected entry

* Upgrade to ntex 0.5.0 b.2

## [0.6.0-b.1] - 2021-12-20

* Upgrade to ntex 0.5.0 b.1

## [0.6.0-b.0] - 2021-12-19

* Upgrade to ntex 0.5

## [codec-0.8.0] - 2021-12-19

* Upgrade to ntex-codec 0.6

## [0.5.9] - 2021-12-14

* Send the close frame in close and close_with_error
* Allow the control service to handle remote_close
* Propagate IO errors
* Change dispatcher trait bounds to allow different error types from Sr and Ctl
* Hold shutdown of dispatcher until control service has handled the close control message
* Add client start with custom control service

## [0.5.8] - 2021-12-14

* Cleanup session end flow #17

## [codec-0.7.4] - 2021-12-03

* Fix overflow in frame decoder

## [0.5.7] - 2021-12-02

* Add memory pools support

## [0.5.6] - 2021-11-29

* Set SenderLink's max_message_size from Attach frame

* Set ReceiverLink's max_message_size from Attach frame

## [0.5.5] - 2021-11-08

* Add Clone impls for error types

## [0.5.4] - 2021-11-04

* Add helper method `Session::detach_sender_link()`

## [0.5.3] - 2021-11-02

* Add set_max_message_size on SenderLink

## [0.5.2] - 2021-10-06

* Add ControlFrame::SessionEnded control frame

* Allow to set attach properties for receiver link builder

## [0.5.1] - 2021-09-18

* Add std Error impl for errors

## [0.5.0] - 2021-09-17

* No changes

## [codec-0.7.3] - 2021-09-14

* Refactor codec's Decode trait

## [0.5.0-b.11] - 2021-09-08

* Handle keep-alive and io errors

## [0.5.0-b.10] - 2021-08-28

* use new ntex's timer api

## [codec-0.7.2] - 2021-08-23

* Add `.get_properties_mut()` helper method to some frames

## [codec-0.7.1] - 2021-08-22

* Auto-generate mut methods for type fields

## [0.5.0-b.9] - 2021-08-21

* Upgrade to codec 0.7

## [codec-0.7.0] - 2021-08-22

* Optimize memory layout

## [0.5.0-b.8] - 2021-08-13

* Fix handling for error during opennig link

## [0.5.0-b.6] - 2021-08-12

* Various cleanups

## [0.5.0-b.5] - 2021-08-11

* Refactor server dispatch process

## [codec-0.6.2] - 2021-08-11

* Add helper methods to Transfer type

## [0.5.0-b.3] - 2021-08-10

* Add Session::connection() method, returns ref to Connection

* Add stream handling for transfer dispositions

* Refactor sender link disposition handling

## [codec-0.6.1] - 2021-08-10

* Regenerate spec with inlines

## [0.5.0-b.2] - 2021-08-06

* Cleanup Session internal state on disconnect

* Use ntex::channel::pool instead of oneshot

## [codec-0.6.0] - 2021-06-27

* Replace bytes witth ntex-bytes

* Use ntex-codec v0.5

## [0.5.0-b.1] - 2021-06-27

* Upgrade to ntex-0.4

## [0.4.5] - 2021-04-20

* agree with remote terminus on snd-settle-mode #9

## [0.4.4] - 2021-04-03

* upgrade ntex, drop direct futures dependency

## [0.4.3] - 2021-03-15

* Add `.buffer_params()` config method

## [0.4.2] - 2021-03-05

* Allow to override io buffer params

## [0.4.1] - 2021-02-25

* Cleanup dependencies

## [0.4.0] - 2021-02-24

* Upgrade to ntex v0.3

## [0.3.0] - 2021-02-21

* Upgrade to ntex v0.2

## [codec-0.4.0] - 2021-01-21

* Use ntex-codec v0.3

## [0.3.0-b.5] - 2021-02-04

* Fix client idle timeout

* Fix frame-trace feature

* Re-use timer for client connector

## [0.3.0-b.4] - 2021-01-27

* Upgrade to ntex v0.2.0-b.7

## [0.3.0-b.3] - 2021-01-24

* Upgrade to ntex v0.2.0-b.5

## [codec-0.4.0-b.1] - 2021-01-24

* Use ntex-codec v0.3

## [0.3.0-b.2] - 2021-01-21

* Fix session level Flow frame handling

* Cleanup unwraps

## [0.3.0-b.1] - 2021-01-19

* Use ntex-0.2

## [0.2.0] - 2021-01-13

* Refactor server and client api

* Use ntex-codec 0.3

* Use ahash instead of fxhash

## [codec-0.3.1] - 2021-01-13

* Clippy warnings

* Update deps

## [codec-0.3.0] - 2021-01-12

* Use ntex-codec 0.2

## [0.1.22] - 2020-12-19

* Support partial transfers on receiver side

## [0.1.21] - 2020-12-14

* Split large message into smaller transfers

## [0.1.20] - 2020-11-25

* Do not log error for remote closed connections

## [0.1.19] - 2020-10-23

* Fix flow frame handling

* Use proper handle for sender link

## [codec-0.2.1] - 2020-09-17

* Do not add empty Message section to encoded buffer

## [codec-0.2.0] - 2020-08-05

* Drop In/OutMessage

* Use vec for message annotations and message app propperties

## [0.1.17] - 2020-08-04

* Rename server::Message to server::Transfer

## [codec-0.1.4] - 2020-08-04

* Deprecated In/OutMessage, replaced with Message

## [0.1.16] - 2020-07-31

* Add receiver/receiver_mut for server Link

## [0.1.15] - 2020-07-25

* Fix sender link apply flow

## [0.1.14] - 2020-07-25

* Notify sender link detached

## [0.1.13] - 2020-07-23

* Better logging

## [0.1.10] - 2020-05-12

* Add AttachReceiver control frame

## [0.1.9] - 2020-05-11

* Add standard error code constants

## [0.1.8] - 2020-05-04

* Proper handling of errors during sender link opening

## [0.1.7] - 2020-05-02

* Add `LinkError::redirect()`

## [codec-0.1.2] - 2020-05-02

* Add const `Symbol::from_static()` helper method.

## [0.1.5] - 2020-04-28

* Fix open multiple sessions

## [0.1.4] - 2020-04-21

* Refactor server control frame

* Wakeup receiver link on disconnect

## [0.1.3] - 2020-04-21

* Fix OutMessage and InMessage encoding

* Move LinkError to root

## [0.1.2] - 2020-04-20

* Fix handshake timeout

* Propagate receiver remote close errors

## [0.1.1] - 2020-04-14

* Handle detach during reciver link open

## [0.1.0] - 2020-04-01

* Switch to ntex

## [0.1.4] - 2020-03-05

* Add server handshake timeout

## [0.1.3] - 2020-02-10

* Allow to override sender link attach frame

## [0.1.2] - 2019-12-25

* Allow to specify multi-pattern for topics

## [0.1.1] - 2019-12-18

* Separate control frame entries for detach sender qand detach receiver

* Proper detach remote receiver

* Replace `async fn` with `impl Future`

## [0.1.0] - 2019-12-11

* Initial release
