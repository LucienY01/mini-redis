use crate::{connection::Connection, db::Db, frame::Frame};

use super::Parse;

pub struct Get {
    key: String,
}

impl Get {
    pub fn from_frame(mut parse: Parse) -> crate::Result<Get> {
        match parse.next_string()? {
            Some(key) => Ok(Get { key }),
            None => return Err("protocol error: expected key".into()),
        }
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let response = match db.get(&self.key) {
            Some(entry) => Frame::Bulk(entry),
            None => Frame::Null,
        };

        conn.write_frame(&response).await?;

        Ok(())
    }
}
