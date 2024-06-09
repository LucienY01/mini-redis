use crate::{connection::Connection, frame::Frame};

pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub fn new(name: impl ToString) -> Self {
        Self {
            command_name: name.to_string(),
        }
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        conn.write_frame(&response).await?;

        Ok(())
    }

    pub fn get_name(&self) -> &str {
        &self.command_name
    }
}
