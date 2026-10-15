//! EchoAgentCore connection layer.
//!
//! Runs a reverse-WebSocket server that OneBot implementations (e.g. NapCat)
//! connect to, parses OneBot v11 events, and dispatches them to registered
//! handlers that can call actions back over the same connection.

pub mod connection;
pub mod error;
pub mod registry;
pub mod server;

pub use error::EchoServerError;
pub use registry::{Context, HandleResult, Handler, HandlerRegistry};
pub use server::{ConnCallback, ConnectionInfo, ConnectionTracker, Server, ServerConfig};
