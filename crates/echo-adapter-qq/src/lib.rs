//! QQ / OneBot v11 adapter for EchoAgentCore.
//!
//! Provides [`QqAdapter`] — a reverse-WebSocket server that bridges
//! QQ messages (via NapCat / OneBot v11) into the agent framework as
//! platform-agnostic [`IncomingMessage`](crate::types::IncomingMessage) values.
//!
//! # Filter Pipeline
//!
//! The adapter supports a configurable filter pipeline with allowlist/denylist,
//! rate limiting, keyword blocking, and admin bypass — see [`QqFilterConfig`].

pub mod adapter;
pub mod config;
pub mod config_store;
pub mod file_bridge;
pub mod files;
pub mod filter;
pub mod handler;
pub mod media;
pub mod napcat;
pub mod plugin;
pub mod types;
pub mod ui;

pub use adapter::QqAdapter;
pub use config::{QqAdapterConfig, QqFilterConfig, QqServerConfig, QqTriggerConfig};
pub use napcat::service::{NapCatService, NapCatServiceState};
pub use napcat::NapCatClient;
