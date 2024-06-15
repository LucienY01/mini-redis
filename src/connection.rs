use std::io;

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::frame::{self, Frame};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            if !self.buffer.is_empty() {
                let mut buf = self.buffer.as_ref();

                match Frame::parse(&mut buf) {
                    Ok((advance, frame)) => {
                        self.buffer.advance(advance);
                        return Ok(Some(frame));
                    }
                    Err(frame::Error::Incomplete) => {}
                    Err(frame::Error::Other(e)) => return Err(e),
                }
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(frames) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(frames.len() as i64).await?;
                for frame in frames {
                    self.write_value(frame).await?;
                }
            }
            _ => {
                self.write_value(frame).await?;
            }
        }

        self.stream.flush().await
    }

    /// Write a non-array frame to the stream.
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(s) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(s.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(s) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(s.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(num) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*num).await?;
            }
            Frame::Bulk(val) => {
                self.stream.write_u8(b'$').await?;
                self.write_decimal(val.len() as i64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Array(_) => unreachable!(),
        }

        Ok(())
    }

    /// Write a decimal line to the stream.
    async fn write_decimal(&mut self, num: i64) -> io::Result<()> {
        self.stream.write_all(num.to_string().as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }
}
