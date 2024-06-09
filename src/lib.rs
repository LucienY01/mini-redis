pub mod clients;
pub mod cmd;
pub mod connection;
pub mod db;
pub mod frame;
pub mod server;
pub mod shutdown;

pub const DEFAULT_PORT: u16 = 6379;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;
