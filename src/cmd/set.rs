use std::time::Duration;

use bytes::Bytes;

use crate::{connection::Connection, db::Db, frame::Frame};

use super::Parse;

pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn from_frame(mut parse: Parse) -> crate::Result<Set> {
        let key = match parse.next_string()? {
            Some(key) => key,
            None => return Err("protocol error: expected key".into()),
        };

        let value = match parse.next_bytes()? {
            Some(value) => value,
            None => return Err("protocol error: expected value".into()),
        };

        let expire = match parse.next_string()? {
            Some(s) => match s.as_str() {
                "EX" => match parse.next_int()? {
                    Some(secs) => Some(Duration::from_secs(secs.try_into()?)),
                    None => return Err("protocol error; expected seconds for EX".into()),
                },
                "PX" => match parse.next_int()? {
                    Some(secs) => Some(Duration::from_millis(secs.try_into()?)),
                    None => return Err("protocol error; expected seconds for EX".into()),
                },
                _ => return Err("currently `SET` only supports the expiration option".into()),
            },
            None => None,
        };

        Ok(Set { key, value, expire })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        db.set(self.key, self.value, self.expire);

        let response = Frame::Simple("OK".to_string());
        conn.write_frame(&response).await?;

        Ok(())
    }
}
