mod get;
pub use get::Get;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

use crate::{connection::Connection, db::Db, shutdown::Shutdown};

use super::frame::Frame;
use std::vec::IntoIter;

use bytes::Bytes;

pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let cmd = match parse.next_string()? {
            Some(name) => match name.as_str() {
                "get" => Command::Get(Get::from_frame(parse)?),
                "publish" => Command::Publish(Publish::from_frame(parse)?),
                "set" => Command::Set(Set::from_frame(parse)?),
                "subscribe" => Command::Subscribe(Subscribe::from_frame(parse)?),
                "unsubscribe" => Command::Unsubscribe(Unsubscribe::from_frame(parse)?),
                "ping" => Command::Ping(Ping::from_frame(parse)?),
                _ => Command::Unknown(Unknown::new(name)),
            },
            None => {
                return Err(format!("protocol error; expected command name").into());
            }
        };

        Ok(cmd)
    }

    pub async fn apply(
        self,
        db: &mut Db,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, conn).await,
            Publish(cmd) => cmd.apply(db, conn).await,
            Set(cmd) => cmd.apply(db, conn).await,
            Subscribe(cmd) => cmd.apply(db, conn, shutdown).await,
            Ping(cmd) => cmd.apply(conn).await,
            Unknown(cmd) => cmd.apply(conn).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }

    pub fn get_name(&self) -> &str {
        use Command::*;

        match self {
            Get(_) => "get",
            Publish(_) => "pub",
            Set(_) => "set",
            Subscribe(_) => "subscribe",
            Unsubscribe(_) => "unsubscribe",
            Ping(_) => "ping",
            Unknown(cmd) => cmd.get_name(),
        }
    }
}

pub struct Parse {
    frames: IntoIter<Frame>,
}

impl Parse {
    pub fn new(arr: Frame) -> crate::Result<Parse> {
        if let Frame::Array(arr) = arr {
            Ok(Parse {
                frames: arr.into_iter(),
            })
        } else {
            Err(format!("protocol error; expected array, got {:?}", arr).into())
        }
    }

    pub fn next_string(&mut self) -> crate::Result<Option<String>> {
        if let Some(frame) = self.next() {
            match frame {
                Frame::Simple(s) => Ok(Some(s)),
                Frame::Bulk(bytes) => Ok(Some(String::from_utf8(bytes.to_vec())?)),
                _ => Err(format!(
                    "protocol error; expected simple or bulk string, got {:?}",
                    frame
                )
                .into()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn next(&mut self) -> Option<Frame> {
        self.frames.next()
    }

    pub fn next_bytes(&mut self) -> crate::Result<Option<Bytes>> {
        let frame = match self.next() {
            Some(frame) => frame,
            None => return Ok(None),
        };

        match frame {
            Frame::Simple(s) => Ok(Some(Bytes::from(s.into_bytes()))),
            Frame::Bulk(bytes) => Ok(Some(bytes)),
            _ => Err(format!(
                "protocol error; expected simple or bulk string, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub fn next_int(&mut self) -> crate::Result<Option<i64>> {
        use atoi::atoi;

        let frame = match self.next() {
            Some(frame) => frame,
            None => return Ok(None),
        };

        match frame {
            Frame::Integer(i) => Ok(Some(i)),
            Frame::Simple(s) => Ok(Some(s.parse()?)),
            Frame::Bulk(val) => match atoi(val.as_ref()) {
                Some(i) => Ok(Some(i)),
                None => Err("protocol error; invalid number".into()),
            },
            _ => Err(format!("protocol error; expected int frame but got {:?}", frame).into()),
        }
    }
}
