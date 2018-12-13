#![allow(unused_doc_comments)]

error_chain! {
    errors {
        Incomplete(n: Option<usize>) {
            description("More data required during frame parsing")
            display("More data required during frame parsing: '{:?}'", n)
        }
        InvalidFormatCode(code: u8) {
            description("Unexpected format code")
            display("Unexpected format code: '{}'", code)
        }
        InvalidDescriptor(descriptor: crate::types::Descriptor) {
            description("Unexpected descriptor")
            display("Unexpected descriptor: '{:?}'", descriptor)
        }
    }
    foreign_links{
        Io(::std::io::Error);
        Canceled(::futures::Canceled);
        UuidParseError(::uuid::ParseError);
        Utf8Error(::std::str::Utf8Error);
    }
}
