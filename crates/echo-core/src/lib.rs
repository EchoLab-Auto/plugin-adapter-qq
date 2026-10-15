//! EchoAgentCore core: OneBot v11 protocol types.
//!
//! This crate performs no I/O. It models the OneBot v11 wire format —
//! events, actions, and message segments — as serde types, plus a small
//! [`MessageBuilder`] for constructing outbound messages.

pub mod action;
pub mod event;
pub mod face;
pub mod message;
pub mod model;
pub mod segment;

pub use action::{ApiRequest, ApiResponse};
pub use event::{Event, MessageEvent, MetaEvent, NoticeEvent, RequestEvent};
pub use message::MessageBuilder;
pub use segment::{KnownSegment, Segment};
