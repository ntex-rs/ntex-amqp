/// Unwrap result and return `err` future
///
/// Err(e) get converted to err(e)
macro_rules! try_ready_err {
    ($e:expr) => {
        match $e {
            Ok(value) => value,
            Err(e) => return ntex::util::Ready::Err(e),
        }
    };
}
