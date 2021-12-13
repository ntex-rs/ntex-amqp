# Changes


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
