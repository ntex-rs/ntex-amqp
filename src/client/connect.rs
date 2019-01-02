use actix_codec::Framed;

use crate::Configuration;

trait IntoFramed<T, U: Default> {
    fn into_framed(self) -> Framed<T, U>;
}

pub struct Handshake {
    cfg: Configuration,
}
