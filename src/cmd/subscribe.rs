use std::{pin::Pin, vec};

use bytes::Bytes;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::{connection::Connection, db::Db, frame::Frame, shutdown::Shutdown};

use super::{unknown::Unknown, Command, Parse};

pub struct Subscribe {
    channels: Vec<String>,
}

impl Subscribe {
    pub fn from_frame(mut parse: Parse) -> crate::Result<Subscribe> {
        let mut channels = Vec::new();
        match parse.next_string()? {
            Some(channel) => channels.push(channel),
            None => return Err("protocol error; expected at least one channel".into()),
        }

        while let Some(channel) = parse.next_string()? {
            channels.push(channel);
        }

        Ok(Subscribe { channels })
    }

    pub async fn apply(
        self,
        db: &Db,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        let mut subscriptions = StreamMap::new();

        for channel in self.channels {
            subscribe_channel(&mut subscriptions, channel, db, conn).await?;
        }

        loop {
            tokio::select! {
                Some((channel, msg)) = subscriptions.next() => {
                    let mut response = Frame::Array(vec![]);
                    response.push_bulk(Bytes::from_static(b"message"));
                    response.push_bulk(Bytes::from(channel));
                    response.push_bulk(msg);

                    conn.write_frame(&response).await?;
                }
                res = conn.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // This happens if the remote connection is closed.
                        None => return Ok(()),
                    };

                    handle_command(frame, &mut subscriptions, db, conn).await?;
                }
                _ = shutdown.recv() => return Ok(()),
            }
        }
    }
}

async fn subscribe_channel(
    subscriptions: &mut StreamMap<String, Message>,
    channel: String,
    db: &Db,
    conn: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(&channel);

    let stream = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(val) => yield val,
                Err(broadcast::error::RecvError::Lagged(_)) => {},
                Err(_) => break,
            }
        }
    });

    subscriptions.insert(channel.clone(), stream);

    let mut response = Frame::Array(vec![]);
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel));
    response.push_int(subscriptions.len() as i64);

    conn.write_frame(&response).await?;

    Ok(())
}

type Message = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

async fn handle_command(
    frame: Frame,
    subscriptions: &mut StreamMap<String, Message>,
    db: &Db,
    conn: &mut Connection,
) -> crate::Result<()> {
    let cmd = Command::from_frame(frame)?;

    // Only `SUBSCRIBE` and `UNSUBSCRIBE` commands are permitted
    // in this context.
    match cmd {
        Command::Subscribe(Subscribe { channels }) => {
            for channel in channels {
                subscribe_channel(subscriptions, channel, db, conn).await?;
            }
        }
        Command::Unsubscribe(Unsubscribe { mut channels }) => {
            if channels.is_empty() {
                channels = subscriptions.keys().cloned().collect();
            }

            for channel in channels {
                subscriptions.remove(&channel);

                let mut response = Frame::Array(vec![]);
                response.push_bulk(Bytes::from_static(b"unsubscribe"));
                response.push_bulk(Bytes::from(channel));
                response.push_int(subscriptions.len() as i64);

                conn.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(conn).await?;
        }
    }
    Ok(())
}

pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Unsubscribe {
    pub fn from_frame(mut parse: Parse) -> crate::Result<Self> {
        let mut channels = Vec::new();

        while let Some(s) = parse.next_string()? {
            channels.push(s);
        }

        Ok(Self { channels })
    }
}
