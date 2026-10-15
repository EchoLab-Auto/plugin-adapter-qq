//! Platform-agnostic message types for adapters.
//!
//! Definitions live in `echo-defs` (the capability-definition layer); this
//! module re-exports them so `echo_adapter::types::…` paths keep working.

pub use echo_defs::chat::{
    AdapterEvent, ChannelType, IncomingFile, IncomingMessage, MessageTarget, SendResult,
};
