//! Shared policy enums used across capabilities and the frontend protocol.

use serde::{Deserialize, Serialize};

/// Gating mode: which users/groups may interact with the bot.
///
/// Serialised as `"none" | "allowlist" | "denylist"` (snake_case), matching
/// the legacy string-based protocol so WS payloads stay wire-compatible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateMode {
    /// Everyone may interact.
    #[default]
    None,
    /// Only allowlisted users/groups may interact.
    Allowlist,
    /// Allowlisted-excluded: only denylisted users/groups are blocked.
    Denylist,
}

impl GateMode {
    pub fn as_str(self) -> &'static str {
        match self {
            GateMode::None => "none",
            GateMode::Allowlist => "allowlist",
            GateMode::Denylist => "denylist",
        }
    }
}

/// Whether model thinking/reasoning is enabled for DeepSeek-compatible APIs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingMode {
    #[default]
    Enabled,
    Disabled,
}

impl ThinkingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

/// DeepSeek reasoning effort level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    Low,
    High,
    #[default]
    Max,
}

impl ReasoningEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::High => "high",
            Self::Max => "max",
        }
    }
}

/// Agent loop mode — how a conversation admits and interleaves work.
///
/// Two mutually exclusive loop plugins carry the choice in the per-persona
/// capability lists: `echo-agent.loop.single` (default) and
/// `echo-agent.loop.parallel`. The mode is a **policy** of the loop, not a
/// different driver: both run the same `TurnRunner` turn/step state machine.
///
/// - [`LoopMode::Single`]（单会话，默认）：一个会话同一时刻只跑一个 turn，
///   后续输入按 FIFO 排队（不同会话仍可并行——另启一个会话即另一条并行通道）；
///   不发射 `ReplyBranch*` 可见性事件，面板不显示会话管理 UI。
/// - [`LoopMode::Parallel`]（并行多会话）：同一会话可并发多个 turn 分支，
///   发射 `ReplyBranch*`，面板显示会话列表/全局分组/临时分支卡。
///
/// serde default = `single`（默认值与插件全开语义解耦：并行必须显式选择）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopMode {
    /// 单会话：会话内 turn 串行排队（默认）。
    #[default]
    Single,
    /// 并行多会话：会话内 turn 并发分支。
    Parallel,
}

impl LoopMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Parallel => "parallel",
        }
    }
}
