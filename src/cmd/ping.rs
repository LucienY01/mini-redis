use bytes::Bytes;

use crate::{connection::Connection, frame::Frame};

use super::Parse;

pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    pub fn from_frame(mut parse: Parse) -> crate::Result<Self> {
        match parse.next_bytes()? {
            Some(msg) => Ok(Ping { msg: Some(msg) }),
            None => Ok(Ping { msg: None }),
        }
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            Some(msg) => Frame::Bulk(msg),
            None => Frame::Simple(String::from("PONG")),
        };

        conn.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }
        frame
    }
}
