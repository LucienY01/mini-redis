use bytes::Bytes;

use crate::{connection::Connection, db::Db, frame::Frame};

use super::Parse;

pub struct Publish {
    channel: String,
    message: Bytes,
}

impl Publish {
    pub fn from_frame(mut parse: Parse) -> crate::Result<Publish> {
        let channel = match parse.next_string()? {
            Some(channel) => channel,
            None => return Err("protocol error: expected channel name".into()),
        };

        let message = match parse.next_string()? {
            Some(message) => Bytes::from(message),
            None => return Err("protocol error: expected message".into()),
        };

        Ok(Publish { channel, message })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let num_subscribers = db.publish(self.channel, self.message);

        let response = Frame::Integer(num_subscribers as i64);
        conn.write_frame(&response).await?;

        Ok(())
    }
}
