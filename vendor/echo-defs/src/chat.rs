//! The chat-platform capability seam — platform-agnostic message/target
//! types and the `ChatAdapter` Service Definition.
//!
//! A platform (QQ, Telegram, ...) is a `ChatAdapter` provider: it translates
//! platform-specific events into [`IncomingMessage`] values and accepts
//! [`MessageTarget`] values to send replies. Consumers (the agent loop, the
//! generic send/list tools) depend only on this crate — never on a concrete
//! platform crate.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Distinguishes between direct messages and group messages across platforms.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChannelType {
    /// Private / direct message.
    Direct,
    /// Group / guild message.
    Group {
        /// Platform-specific group/channel id.
        group_id: String,
    },
}

impl ChannelType {
    pub fn is_group(&self) -> bool {
        matches!(self, ChannelType::Group { .. })
    }

    pub fn group_id(&self) -> Option<&str> {
        match self {
            ChannelType::Direct => None,
            ChannelType::Group { group_id } => Some(group_id.as_str()),
        }
    }

    /// String representation for use in session IDs / display.
    pub fn as_scope(&self) -> &str {
        match self {
            ChannelType::Direct => "dm",
            ChannelType::Group { .. } => "group",
        }
    }
}

impl std::fmt::Display for ChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelType::Direct => write!(f, "direct"),
            ChannelType::Group { group_id } => write!(f, "group:{}", group_id),
        }
    }
}

/// An inbound file attachment, downloaded to the local machine.
///
/// Adapters that can receive files (e.g. QQ group uploads / private files)
/// download the bytes next to the incoming message and surface them here;
/// the agent reads `path` with its file/bash tools.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncomingFile {
    /// File name as shown on the platform (e.g. `report.pdf`).
    pub name: String,
    /// Absolute path of the downloaded file; `None` when the download
    /// failed (see `error`) or was skipped (e.g. over the size limit).
    pub path: Option<String>,
    /// File size in bytes as reported by the platform (0 = unknown).
    pub size: u64,
    /// Download failure/skip reason when `path` is `None`.
    pub error: Option<String>,
}

/// An inbound message from a platform adapter.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// The adapter that received this message (e.g., "qq").
    pub adapter_name: String,
    /// Platform identifier string (e.g., "qq").
    pub platform: String,
    /// Platform user ID as a string (e.g., "123456789").
    pub user_id: String,
    /// Display name of the sender.
    pub user_name: String,
    /// The channel the message was sent in.
    pub channel: ChannelType,
    /// Group/channel display name, if applicable.
    pub group_name: Option<String>,
    /// Plain text content.
    pub content: String,
    /// Unix timestamp from the platform.
    pub timestamp: i64,
    /// Whether the bot was @-mentioned.
    pub at_me: bool,
    /// Opaque metadata for platform-specific use.
    pub metadata: serde_json::Value,
    /// Multimodal media attached to the message: image URLs or `data:` URIs.
    ///
    /// Populated by adapters that carry media (e.g. QQ images); the agent
    /// forwards them to vision-capable models. Empty for text-only messages.
    pub images: Vec<String>,
    /// File attachments already downloaded to this machine.
    ///
    /// Empty for messages without files. Populated by adapters that support
    /// file transfer (e.g. QQ group uploads); see [`IncomingFile`].
    pub files: Vec<IncomingFile>,
}

/// Describes where to send a reply message.
#[derive(Debug, Clone)]
pub struct MessageTarget {
    /// Adapter to use (e.g., "qq").
    pub adapter_name: String,
    /// Channel type.
    pub channel: ChannelType,
    /// The target user id (for DM replies or @-mentions).
    pub user_id: String,
}

/// Result of sending a message.
#[derive(Debug, Clone)]
pub struct SendResult {
    /// Platform message ID, if available.
    pub message_id: Option<String>,
    /// Whether the send succeeded.
    pub success: bool,
    /// Error details if failed.
    pub error: Option<String>,
}

/// Events emitted by adapters for the agent to consume.
#[derive(Debug, Clone)]
pub enum AdapterEvent {
    /// A new message was received from the platform.
    ///
    /// Boxed: `IncomingMessage` is much larger than the other variants.
    MessageReceived(Box<IncomingMessage>),
    /// Connection state changed.
    ConnectionState {
        adapter_name: String,
        connected: bool,
        self_id: Option<String>,
    },
    /// The adapter was stopped.
    Stopped { adapter_name: String },
    /// An error occurred in the adapter.
    Error {
        adapter_name: String,
        message: String,
    },
}

/// Adapter lifecycle and capability errors.
#[derive(Debug, thiserror::Error)]
pub enum ChatAdapterError {
    #[error("adapter {0} is not configured")]
    NotConfigured(String),
    #[error("adapter {0} is already running")]
    AlreadyRunning(String),
    #[error("adapter {0} is not running")]
    NotRunning(String),
    #[error("adapter error: {0}")]
    Internal(String),
    #[error("send failed: {0}")]
    SendFailed(String),
    #[error("timeout")]
    Timeout,
}

/// Connection state of a chat adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChatAdapterConnectionState {
    /// Not started.
    Stopped,
    /// Running and connected.
    Connected,
    /// Running but disconnected (no client connected).
    Disconnected,
    /// In the process of starting.
    Starting,
    /// In the process of stopping.
    Stopping,
    /// Encountered an error.
    Error,
}

/// Snapshot of adapter status for the TUI / API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatAdapterInfo {
    /// Unique adapter identifier (e.g., "qq").
    pub name: String,
    /// Human-readable label (e.g., "QQ/OneBot").
    pub display_name: String,
    /// Connection status.
    pub status: ChatAdapterConnectionState,
    /// Self ID on the platform (e.g., QQ number as string).
    pub self_id: Option<String>,
    /// When the adapter was started (Unix timestamp).
    pub started_at: Option<i64>,
    /// Platform name for session scoping.
    pub platform: String,
    /// Whether this adapter is configured and enabled.
    pub configured: bool,
}

/// An abstraction over a messaging platform (QQ, Telegram, Discord, etc.).
///
/// Adapters translate platform-specific events into platform-agnostic
/// [`IncomingMessage`] values, and accept [`MessageTarget`] values to send
/// replies. This is the **Service Definition** of the chat-platform seam:
/// providers implement it, consumers inject it by key.
///
/// # Lifecycle
///
/// 1. Created with configuration (may be incomplete — see `is_configured()`).
/// 2. `subscribe()` — the agent hooks in to receive events.
/// 3. `start()` — the adapter begins listening for platform events.
/// 4. `stop()` — graceful shutdown.
#[async_trait]
pub trait ChatAdapter: Send + Sync {
    /// Unique adapter identifier (e.g., "qq").
    fn name(&self) -> &str;

    /// Human-readable label (e.g., "QQ / OneBot").
    fn display_name(&self) -> &str;

    /// Platform tag used in session scoping (e.g., "qq").
    fn platform(&self) -> &str;

    /// Whether this adapter has a valid configuration to run.
    fn is_configured(&self) -> bool;

    /// Current status info for display.
    fn status_info(&self) -> ChatAdapterInfo;

    /// Start the adapter.
    async fn start(&self) -> Result<(), ChatAdapterError>;

    /// Stop the adapter gracefully.
    async fn stop(&self) -> Result<(), ChatAdapterError>;

    /// Send a text message to a specific platform target.
    async fn send_text(
        &self,
        target: &MessageTarget,
        content: &str,
    ) -> Result<SendResult, ChatAdapterError>;

    /// Subscribe to adapter events. May be called multiple times.
    fn subscribe(&self, tx: tokio::sync::mpsc::UnboundedSender<AdapterEvent>);

    /// Get the group list for this platform. Default: empty.
    async fn get_groups(&self) -> Result<Vec<(i64, String)>, ChatAdapterError> {
        let _ = self;
        Ok(Vec::new())
    }

    /// Get the friend list for this platform. Default: empty.
    async fn get_friend_list(&self) -> Result<Vec<(i64, String)>, ChatAdapterError> {
        let _ = self;
        Ok(Vec::new())
    }
}
