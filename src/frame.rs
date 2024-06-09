use bytes::{Buf, Bytes};
use core::panic;
use std::{fmt, string::FromUtf8Error};

#[derive(PartialEq, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    /// Parse a frame from the given buffer.
    /// Return how many bytes should be consumed and the frame if succeed.
    pub fn parse(mut buf: &[u8]) -> Result<(usize, Frame), Error> {
        match buf.get_u8() {
            b'+' => {
                if let Some((advance, line)) = get_line(buf) {
                    let s = String::from_utf8(line.to_vec())?;
                    return Ok((1 + advance, Frame::Simple(s)));
                }
            }
            b'-' => {
                if let Some((advance, line)) = get_line(buf) {
                    let s = String::from_utf8(line.to_vec())?;
                    return Ok((1 + advance, Frame::Error(s)));
                }
            }
            b':' => {
                if let Some((advance, decimal)) = get_decimal(buf) {
                    let frame = Frame::Integer(decimal);
                    return Ok((1 + advance, frame));
                }
            }
            b'$' => {
                if let Some((advance, len)) = get_decimal(buf) {
                    if len == -1 {
                        // Null bulk string
                        return Ok((5, Frame::Null));
                    }

                    if len < 0 {
                        return Err(Error::Other(INVALID_FORMAT.into()));
                    }

                    let data_buf = &buf[advance..];
                    let len = len as usize;
                    if data_buf.len() >= len + 2
                        && data_buf[len] == b'\r'
                        && data_buf[len + 1] == b'\n'
                    {
                        let data = Bytes::copy_from_slice(&data_buf[..len]);
                        return Ok((1 + advance + len + 2, Frame::Bulk(data)));
                    }
                }
            }
            b'*' => {
                let mut total_advance = 1;
                if let Some((decimal_advance, n_elements)) = get_decimal(buf) {
                    if n_elements < 0 {
                        return Err(Error::Other(INVALID_FORMAT.into()));
                    }

                    total_advance += decimal_advance;
                    buf.advance(decimal_advance);

                    let mut array = Vec::with_capacity(n_elements as usize);
                    for _ in 0..n_elements {
                        let (advance, frame) = Frame::parse(buf)?;
                        total_advance += advance;
                        buf.advance(advance);
                        array.push(frame);
                    }

                    return Ok((total_advance, Frame::Array(array)));
                }
            }
            _ => unimplemented!(),
        }

        Err(Error::Incomplete.into())
    }

    pub fn parse_simple(buf: &[u8]) -> crate::Result<Frame> {
        let s = String::from_utf8(buf.to_vec())?;
        Ok(Frame::Simple(s))
    }

    pub fn parse_error(buf: &[u8]) -> crate::Result<Frame> {
        let s = String::from_utf8(buf.to_vec())?;
        Ok(Frame::Error(s))
    }

    pub fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(frames) => {
                frames.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub fn push_int(&mut self, value: i64) {
        match self {
            Frame::Array(frames) => {
                frames.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }
}

static INVALID_FORMAT: &str = "protocol error; invalid frame format";

/// Find a new-line terminated decimal.
/// Return how many bytes should be consumed and the line itself.
pub fn get_decimal(buf: &[u8]) -> Option<(usize, i64)> {
    use atoi::atoi;
    let (n, line) = get_line(buf)?;
    let decimal = atoi(line)?;
    Some((n, decimal))
}

/// Find a line.
/// Return how many bytes should be consumed and the line itself.
// todo: change return type.
pub fn get_line(buf: &[u8]) -> Option<(usize, &[u8])> {
    for i in 0..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((i + 2, &buf[..i]));
        }
    }
    None
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(e) => e.fmt(fmt),
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Error::Other(e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_signed_decimal() {
        let buf = b"+123\r\n";
        let (n, decimal) = get_decimal(buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(decimal, 123);

        let buf = b"-123\r\n";
        let (n, decimal) = get_decimal(buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(decimal, -123);
    }

    #[test]
    fn parse_simple() {
        let buf = b"+OK\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 5);
        assert_eq!(frame, Frame::Simple("OK".to_string()));
    }

    #[test]
    fn parse_empty_simple() {
        let buf = b"+\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 3);
        assert_eq!(frame, Frame::Simple(String::new()));
    }

    #[test]
    fn parse_error() {
        let buf = b"-ERR invalid password\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 23);
        assert_eq!(frame, Frame::Error("ERR invalid password".to_string()));
    }

    #[test]
    fn parse_integer() {
        let buf = b":123\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 6);
        assert_eq!(frame, Frame::Integer(123));
    }

    #[test]
    fn parse_null_bulk_string() {
        let buf = b"$-1\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 5);
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn parse_bulk_string() {
        let buf = b"$6\r\nfoobar\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 12);
        assert_eq!(frame, Frame::Bulk(Bytes::from_static(b"foobar")));
    }

    #[test]
    fn parse_array() {
        let buf = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 26);
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"hello")),
                Frame::Bulk(Bytes::from_static(b"world")),
            ])
        );
    }

    #[test]
    fn parse_empty_array() {
        let buf = b"*0\r\n";
        let (advance, frame) = Frame::parse(buf).unwrap();
        assert_eq!(advance, 4);
        assert_eq!(frame, Frame::Array(vec![]));
    }
}
