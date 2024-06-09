use bytes::Bytes;

use crate::{connection::Connection, frame::Frame};

use super::Parse;

pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
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
}
