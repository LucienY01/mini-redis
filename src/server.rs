use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tracing::error;

use tokio::net::{TcpListener, TcpStream};

use crate::cmd::Command;
use crate::connection::Connection;
use crate::db::{Db, DbDropGuard};
use crate::shutdown::Shutdown;

struct Listener {
    listener: TcpListener,
    db_holder: DbDropGuard,
    limit_connections: Arc<Semaphore>,
    /// to notify all handlers to shutdown
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&mut self) -> crate::Result<()> {
        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;

            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete_tx: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }

                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let socket = self.listener.accept().await?.0;
        Ok(socket)
    }
}

const MAX_CONNECTIONS: usize = 250;

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = ?err, "failed to accept");
            }
        }
        _ = shutdown => {}
    }

    // wait for all handlers to complete
    let Listener {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    shutdown_complete_rx.recv().await;
}

struct Handler {
    db: Db,
    connection: Connection,
    shutdown: Shutdown,
    /// Not used directly. Instead, used when `Handler` is dropped.
    _shutdown_complete_tx: mpsc::Sender<()>,
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let cmd = Command::from_frame(frame)?;

            cmd.apply(&mut self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
