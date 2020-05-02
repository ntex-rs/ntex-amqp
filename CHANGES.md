# Changes

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
