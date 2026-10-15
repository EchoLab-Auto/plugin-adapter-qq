//! Adapter trait and associated types.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::types::{AdapterEvent, IncomingMessage, MessageTarget, SendResult};

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
    // persona/容器/URL/绑定地址经
    // [`AdapterManagement::cached_state`]（能力扩展 trait）暴露，
    // 通用快照保持平台中立。
    //
    // [`AdapterManagement::cached_state`]: crate::capabilities::AdapterManagement::cached_state
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

/// Gating mode: which users/groups may interact with the bot.
///
/// `GateMode` is part of the frontend wire contract (`SetQqGateMode` /
/// `QqGateMode`), defined in `echo-defs` and re-exported through
/// `echo-protocol`; re-exported here so `echo_adapter::GateMode` keeps
/// working.
pub use echo_defs::GateMode;

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

/// An abstraction over a messaging platform (QQ, Telegram, Discord, etc.).
///
/// Adapters translate platform-specific events into platform-agnostic
/// [`IncomingMessage`] values, and
/// accept [`MessageTarget`] values to send replies.
///
/// # Lifecycle
///
/// 1. Created with configuration (may be incomplete — see `is_configured()`).
/// 2. `subscribe()` — the agent hooks in to receive events.
/// 3. `start()` — the adapter begins listening for platform events.
/// 4. `stop()` — graceful shutdown.
#[async_trait]
pub trait Adapter: Send + Sync {
    /// Unique adapter identifier (e.g., "qq").
    fn name(&self) -> &str;

    /// Human-readable label (e.g., "QQ / OneBot").
    fn display_name(&self) -> &str;

    /// Platform tag used in session scoping (e.g., "qq").
    fn platform(&self) -> &str;

    /// Whether this adapter has a valid configuration to run.
    fn is_configured(&self) -> bool;

    /// Current status info for display.
    fn status_info(&self) -> AdapterInfo;

    /// Start the adapter. For a QQ adapter this binds the reverse-WS server.
    async fn start(&self) -> Result<(), AdapterError>;

    /// Stop the adapter gracefully.
    async fn stop(&self) -> Result<(), AdapterError>;

    /// 彻底移除实例资源（实例**删除**语义，2026-10 巡检遗留）。
    ///
    /// 与 `stop`（临时停用，保留数据/登录态）不同：实现方应停止适配器
    /// 并回收外部资源（QQ/NapCat：容器 + 数据卷 + compose 文件）。
    /// 默认 no-op（无外部资源的适配器无需实现）。幂等：重复调用安全。
    async fn remove_instance(&self) -> Result<(), AdapterError> {
        Ok(())
    }

    /// Send a text message to a specific platform target.
    async fn send_message(
        &self,
        target: &MessageTarget,
        content: &str,
    ) -> Result<SendResult, AdapterError>;

    /// Subscribe to adapter events. May be called multiple times.
    fn subscribe(&self, tx: mpsc::UnboundedSender<AdapterEvent>);

    /// 平台中立的适配器管理面（W3.1，2026-10 适配器管理通道）。
    ///
    /// 平台管理能力的唯一入口；`None` = 该适配器不提供
    /// 管理面。消费方（命令域 / 面板通道 / 远程适配器）只应依赖本访问器。
    fn management(&self) -> Option<&dyn crate::capabilities::AdapterManagement> {
        None
    }
}
