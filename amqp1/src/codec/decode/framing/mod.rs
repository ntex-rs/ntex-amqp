use bytes::Bytes;
use framing::{AmqpFrame, Frame, HEADER_LEN, AMQP_TYPE};
use nom::{be_u8, be_u16, be_u32, ErrorKind, IResult};

pub const INVALID_FRAME: u32 = 0x0001;

macro_rules! error_if (
  ($i:expr, $cond:expr, $code:expr) => (
    {
      if $cond {
        IResult::Error(error_code!(ErrorKind::Custom($code)))
      } else {
        IResult::Done($i, ())
      }
    }
  );
  ($i:expr, $cond:expr, $err:expr) => (
    error!($i, $cond, $err);
  );
);

named!(pub decode_frame<Frame>,
    do_parse!(
        size: be_u32 >>
        error_if!(size < HEADER_LEN as u32, INVALID_FRAME) >>

        doff: be_u8 >>
        error_if!(doff < 2, INVALID_FRAME) >>

        frame: alt!(
            // AMQP Frame
            do_parse!(
                typ:  tag!([AMQP_TYPE]) >>  // Amqp frame Type

                channel_id: be_u16 >>
                extended_header: map!(take!(doff as u32 * 4 - 8), Bytes::from)  >>
                body: map!(take!(size - doff as u32 * 4), Bytes::from) >>

                (Frame::Amqp(AmqpFrame::new(channel_id, body)))
            )
        ) >>
        (frame)
    ));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use codec::Encode;

    #[test]
    fn round_trip() {
        let b1 = &mut BytesMut::with_capacity(0);
        let f = AmqpFrame::new(21, Bytes::from("the body"));
        f.encode(b1);
        let result = decode_frame(b1).to_full_result();

        assert_eq!(Ok(Frame::Amqp(f)), result);
    }

    #[test]
    fn no_body() {
        let b1 = &mut BytesMut::with_capacity(0);
        let f = AmqpFrame::new(21, Bytes::new());
        f.encode(b1);
        let result = decode_frame(b1).to_full_result();

        assert_eq!(Ok(Frame::Amqp(f)), result);
    }
}