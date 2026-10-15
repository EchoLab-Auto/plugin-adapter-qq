//! QQ 适配器插件的领域词汇（P1.7c）：入站消息 / 出站目标 / 事件 / 错误与状态。
//!
//! 这些类型原属宿主 `echo-defs` / `echo-adapter` 的同名类型；插件化后归插件
//! 自有（不再随宿主 crate 漂移）。线协议一侧由 `echo-plugin-api`
//! （`AdapterMessage` / `AdapterTarget` / `AdapterChannel`）承载，两侧转换
//! 收敛在 `plugin.rs` 边界。

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

/// Connection state of an adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdapterConnectionState {
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
pub struct AdapterInfo {
    /// Unique adapter identifier (e.g., "qq").
    pub name: String,
    /// Human-readable label (e.g., "QQ/OneBot").
    pub display_name: String,
    /// Connection status.
    pub status: AdapterConnectionState,
    /// Self ID on the platform (e.g., QQ number as string).
    pub self_id: Option<String>,
    /// When the adapter was started (Unix timestamp).
    pub started_at: Option<i64>,
    /// Platform name for session scoping.
    pub platform: String,
    /// Whether this adapter is configured and enabled.
    pub configured: bool,
    // 平台专属附加信息不在此（2026-10 抽象泄漏重构）：QQ 实例的
    // persona / 容器 / URL / 绑定地址经 `QqAdapter::cached_state` 暴露，
    // 通用快照保持平台中立。
}

/// Hook that platform adapters use to deliver inbound messages into the
/// agent framework, without depending on echo-agent directly.
///
/// This boundary is deliberately one-way. Agent output is not returned from
/// the hook; sending a platform message must happen through an adapter tool.
#[async_trait]
pub trait InboundMessageHook: Send + Sync {
    /// Deliver one inbound platform message to the agent.
    ///
    /// The hook owns session lookup, event emission, and LLM processing.
    /// A successful return only acknowledges processing; it never contains
    /// text for the adapter to send automatically.
    async fn on_incoming_message(&self, msg: IncomingMessage) -> Result<(), String>;

    /// Notify the agent that an adapter's connection state changed.
    fn on_connection_state(&self, adapter_name: &str, connected: bool, self_id: Option<String>);
}

/// Adapter errors.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
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
