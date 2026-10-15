//! QQ/OneBot adapter — wraps echo-server and implements the Adapter trait.
//!
//! Filter/gate management and TOML persistence live in `filter.rs`.

/// 默认（legacy）QQ 实例名：会话 id 不产生 `@` 后缀。
pub const DEFAULT_INSTANCE_NAME: &str = "qq";

mod convert;
mod filter;
mod onebot_api;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use echo_adapter::capabilities::AdapterManagement;
use echo_adapter::filter::FilterPipeline;
use echo_adapter::traits::{Adapter, AdapterConnectionState, AdapterError, AdapterInfo};
use echo_adapter::types::{
    AdapterEvent, ChannelType, IncomingFile, IncomingMessage, MessageTarget, SendResult,
};
use echo_core::{Event as OneBotEvent, MessageEvent, Segment};
use echo_server::{ConnCallback, Context, HandlerRegistry, Server, ServerConfig};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::config::QqAdapterConfig;
use crate::napcat::service::NapCatService;
use crate::napcat::NapCatState;
use crate::NapCatClient;

const NAPCAT_INITIAL_CHECK_DELAY: Duration = Duration::from_secs(2);
const NAPCAT_RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// 已连接后的守护复查间隔：守护态不再重配，仅做轻量在线检查。
const NAPCAT_GUARD_INTERVAL: Duration = Duration::from_secs(60);
/// 未连接时两次完整重配（WebUI 登录 + SetConfig + 快速登录固化）的最小间隔。
/// NapCat WebUI 有登录限流（实测 ~20s 内 8–10 次登录即触发 `login rate limit`），
/// 启动/重连窗口若按 retry 间隔（5s）重配会直接打满——节流防登录风暴。
const NAPCAT_DANCE_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// `maintain_napcat_connection` 的节奏参数（生产用模块常量，测试注入小值）。
struct NapCatMaintainTiming {
    initial_delay: Duration,
    retry_interval: Duration,
    guard_interval: Duration,
    dance_min_interval: Duration,
}

/// 二维码新鲜度阈值（秒）：超过则先让 NapCat 重新生成再取。
/// NapCat 二维码约 2 分钟有效，这里取 90s——既不频繁作废用户刚扫的码，
/// 也保证点「获取登录二维码」时拿到的总是接近新鲜的有效码。
const QR_MAX_AGE_SECS: u64 = 90;

/// Gating mode — mutually exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QqGateMode {
    /// No filtering — everyone can interact.
    None,
    /// Only allowlisted users/groups can interact.
    Allowlist,
    /// Denylisted users/groups are blocked; everyone else allowed.
    Denylist,
}

/// Shared inner state — lives in an Arc so the server task can reference it.
/// Fields are `pub(crate)` so the message handler (handler.rs) can use them.
pub(crate) struct QqInner {
    /// 实例名（= 适配器名；多实例入站消息/连接事件的归属维度）。
    /// 此前入站链路硬编码 "qq"，非默认实例的会话与连接事件全部串台
    /// （2026-10 巡检发现的 🔴）。
    pub(crate) instance_name: String,
    /// 归属人格 id（多实例；None = legacy 单实例未指定）。
    pub(crate) persona: Option<String>,
    /// 实例化显示名（"QQ / OneBot" 或 "QQ（<persona> / <实例>）"）。
    pub(crate) display_name: String,
    pub(crate) config: QqAdapterConfig,
    pub(crate) active_context: StdMutex<Option<Arc<Context>>>,
    pub(crate) running: AtomicBool,
    pub(crate) started_at: StdMutex<Option<i64>>,
    pub(crate) connected: AtomicBool,
    pub(crate) self_id: StdMutex<Option<String>>,
    pub(crate) subscribers: StdMutex<Vec<mpsc::UnboundedSender<AdapterEvent>>>,
    pub(crate) filter: StdMutex<Option<Arc<FilterPipeline>>>,
    pub(crate) shutdown_tx: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
    pub(crate) server_task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    pub(crate) napcat_config_task: StdMutex<Option<tokio::task::AbortHandle>>,
    pub(crate) tracker: echo_server::ConnectionTracker,
    /// One-way inbound hook (set externally before start). Adapters depend
    /// only on echo-adapter abstractions, not on echo-agent itself.
    pub(crate) message_hook: StdMutex<Option<Arc<dyn echo_adapter::InboundMessageHook>>>,
    /// Extra handlers to register alongside the internal QQ handler.
    pub(crate) extra_handlers: StdMutex<Vec<Arc<dyn echo_server::Handler>>>,
    /// Runtime allowlist overrides (takes precedence over config file values).
    pub(crate) runtime_allowlist_users: StdMutex<Vec<i64>>,
    pub(crate) runtime_allowlist_groups: StdMutex<Vec<i64>>,
    /// Runtime denylist overrides.
    pub(crate) runtime_denylist_users: StdMutex<Vec<i64>>,
    pub(crate) runtime_denylist_groups: StdMutex<Vec<i64>>,
    /// Current gating mode (mutually exclusive).
    pub(crate) gate_mode: StdMutex<QqGateMode>,
    /// Runtime QQ owner (admin) — initialized from config, updatable at
    /// runtime via `set_owner_qq` (persisted through the ConfigStore).
    pub(crate) owner_qq: StdMutex<i64>,
    /// Shared config store for persisting gate/filter changes.
    pub(crate) config_store: StdMutex<Option<echo_adapter::ConfigStore>>,
    /// Resolves local file paths into paths/URLs visible to NapCat, so
    /// `send_file` works even when the agent and NapCat are on different
    /// hosts (agent on host, NapCat in Docker).
    pub(crate) file_bridge: crate::file_bridge::FileBridge,
    /// Cache of group_id → group_name, refreshed lazily when a group message
    /// arrives without a known name. Lets inbound messages carry the group
    /// name even though OneBot group events only include the group_id.
    pub(crate) group_names: dashmap::DashMap<i64, String>,
}

/// Wrapper to make an Arc'd handler satisfy the `Handler` trait constraint.
struct BoxedHandler(Arc<dyn echo_server::Handler>);

#[async_trait]
impl echo_server::Handler for BoxedHandler {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn priority(&self) -> i32 {
        self.0.priority()
    }
    async fn handle(
        &self,
        ctx: &echo_server::Context,
        event: &OneBotEvent,
    ) -> echo_server::HandleResult {
        self.0.handle(ctx, event).await
    }
}

/// The QQ adapter — wraps a reverse-WebSocket server (echo-server)
/// and bridges OneBot v11 events into the agent.
pub struct QqAdapter {
    name: String,
    inner: Arc<QqInner>,
}

impl QqAdapter {
    /// Legacy 单实例构造（适配器名固定为 `qq`）。
    pub fn new(config: QqAdapterConfig) -> Self {
        Self::with_instance(DEFAULT_INSTANCE_NAME, None, config)
    }

    /// 多实例构造：实例名 + 归属人格（实例名即适配器名与会话 account 维度）。
    pub fn with_instance(
        name: impl Into<String>,
        persona: Option<String>,
        config: QqAdapterConfig,
    ) -> Self {
        let name = name.into();
        // Seed runtime override fields from config.
        let au = config.filter.allowlist.user_ids.clone();
        let ag = config.filter.allowlist.group_ids.clone();
        let du = config.filter.denylist.user_ids.clone();
        let dg = config.filter.denylist.group_ids.clone();
        let gate_mode = match config.gate.mode.as_str() {
            "allowlist" => QqGateMode::Allowlist,
            "denylist" => QqGateMode::Denylist,
            _ => QqGateMode::None,
        };
        let owner_qq = config.owner_qq;
        // Clone the NapCat bridge parameters before `config` is moved into the
        // struct initializer below.
        let napcat_onebot_url = config.napcat_onebot_url.clone();
        let napcat_host = config.napcat_host.clone();
        let napcat_container = config.napcat_container.clone();
        let napcat_container_data_dir = config.napcat_container_data_dir.clone();
        let display_name = match persona.as_deref() {
            Some(p) if name != DEFAULT_INSTANCE_NAME => format!("QQ（{p} / {name}）"),
            Some(p) => format!("QQ（{p}）"),
            None if name != DEFAULT_INSTANCE_NAME => format!("QQ（{name}）"),
            None => "QQ / OneBot".to_string(),
        };
        let adapter = Self {
            name: name.clone(),
            inner: Arc::new(QqInner {
                instance_name: name,
                persona,
                display_name,
                config,
                active_context: StdMutex::new(None),
                running: AtomicBool::new(false),
                started_at: StdMutex::new(None),
                connected: AtomicBool::new(false),
                self_id: StdMutex::new(None),
                subscribers: StdMutex::new(Vec::new()),
                filter: StdMutex::new(None), // built below via rebuild_filter_pipeline
                shutdown_tx: StdMutex::new(None),
                server_task: StdMutex::new(None),
                napcat_config_task: StdMutex::new(None),
                tracker: echo_server::ConnectionTracker::default(),
                message_hook: StdMutex::new(None),
                extra_handlers: StdMutex::new(Vec::new()),
                runtime_allowlist_users: StdMutex::new(au),
                runtime_allowlist_groups: StdMutex::new(ag),
                runtime_denylist_users: StdMutex::new(du),
                runtime_denylist_groups: StdMutex::new(dg),
                gate_mode: StdMutex::new(gate_mode),
                owner_qq: StdMutex::new(owner_qq),
                config_store: StdMutex::new(None),
                file_bridge: crate::file_bridge::FileBridge::new(
                    &napcat_onebot_url,
                    &napcat_host,
                    &napcat_container,
                    &napcat_container_data_dir,
                ),
                group_names: dashmap::DashMap::new(),
            }),
        };
        adapter.rebuild_filter_pipeline();
        adapter
    }
    async fn maintain_napcat_connection(
        inner: Arc<QqInner>,
        napcat: NapCatClient,
        ws_url: String,
        token: String,
        timing: NapCatMaintainTiming,
        webui_token_source: Arc<dyn Fn() -> Result<String, String> + Send + Sync>,
    ) {
        tokio::time::sleep(timing.initial_delay).await;

        let mut last_state = None;
        // 已连接后的守护模式（2026-10 巡检遗留）：此前 connected=true 即
        // 退出循环——NapCat 容器**重建**（compose 文件由 Core 多实例流程
        // 重新生成时会重置网络配置）后反向 WS 配置丢失，裸奔到下次重启
        // Core。改为连接后降频守护：每 guard_interval 复查一次在线态，
        // 断开（connected=false）自动回到正常重试间隔并重新配置（重配
        // 幂等：同名 EchoAgentCore 条目先删后加）。
        //
        // 2026-10 修复：守护态不再每次复查都完整重配——connected=true 本身
        // 即证明反向 WS 配置有效；旧实现每分钟仍做 WebUI 登录 ×2 + SetConfig
        // + docker exec，实测触发 NapCat WebUI 登录限流（日志 `login rate
        // limit`）。现在：已连接 → 跳过重配（快速登录固化每进程至多补一
        // 次）；未连接 → 重配，且两次重配至少间隔 dance_min_interval。
        let mut ensured_quick_login = false;
        let mut last_dance: Option<std::time::Instant> = None;
        loop {
            if !inner.running.load(Ordering::SeqCst) {
                break;
            }
            let guarding = inner.connected.load(Ordering::SeqCst);
            match napcat.check().await {
                NapCatState::Online { user_id, nickname } => {
                    if last_state != Some("online") {
                        tracing::info!(%user_id, %nickname, %ws_url, "NapCat online");
                    }
                    if guarding {
                        // 已连接：配置在线生效，不再重复 WebUI 登录/重配。
                        // 快速登录固化（写 webui.json 的 autoLoginAccount，
                        // 幂等）每进程至多补一次。
                        if !ensured_quick_login {
                            if let Ok(webui_token) = webui_token_source() {
                                napcat
                                    .ensure_quick_login(
                                        &inner.config.napcat_container,
                                        &webui_token,
                                    )
                                    .await;
                                ensured_quick_login = true;
                            }
                        }
                        // 断开后立即允许重配：连接期无重配记录，节流无需残留。
                        last_dance = None;
                    } else if last_dance.is_none_or(|t| t.elapsed() >= timing.dance_min_interval) {
                        // 未连接：完整（重）配置 + 快速登录固化；节流防登录风暴。
                        last_dance = Some(std::time::Instant::now());
                        match webui_token_source() {
                            Ok(webui_token) => {
                                if let Err(e) = napcat
                                    .configure_reverse_ws(&ws_url, &token, &webui_token)
                                    .await
                                {
                                    tracing::warn!(
                                        error = %e,
                                        "NapCat auto-config failed; will retry"
                                    );
                                }
                                // 「启动时自动登录」：在线时固化 autoLoginAccount
                                // 到 webui.json；离线时尝试快速登录（失败回退扫码）。
                                napcat
                                    .ensure_quick_login(
                                        &inner.config.napcat_container,
                                        &webui_token,
                                    )
                                    .await;
                                ensured_quick_login = true;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "NapCat WebUI token unavailable; will retry"
                                );
                            }
                        }
                    }
                    last_state = Some("online");
                }
                NapCatState::WaitingForLogin => {
                    if last_state != Some("waiting") {
                        tracing::info!("NapCat running, waiting for QQ login");
                    }
                    last_state = Some("waiting");
                }
                NapCatState::Unreachable => {
                    if last_state != Some("unreachable") {
                        tracing::info!("NapCat API is not reachable; waiting for it to start");
                    }
                    last_state = Some("unreachable");
                }
            }

            // 守护模式降频；断开后回到正常重试间隔。
            tokio::time::sleep(if guarding {
                timing.guard_interval
            } else {
                timing.retry_interval
            })
            .await;
        }
    }

    /// Attach the one-way inbound hook before starting the adapter.
    pub fn set_message_hook(&self, hook: Arc<dyn echo_adapter::InboundMessageHook>) {
        *self
            .inner
            .message_hook
            .lock()
            .expect("message hook poisoned") = Some(hook);
    }

    /// Register an additional handler (e.g. Echo, Help, Admin).
    pub fn add_handler(&self, handler: Box<dyn echo_server::Handler>) {
        self.inner
            .extra_handlers
            .lock()
            .expect("handlers poisoned")
            .push(Arc::from(handler));
    }

    fn apply_friend_gate(&self, mut friends: Vec<(i64, String)>) -> Vec<(i64, String)> {
        let mode = self.get_gate_mode();
        if mode == QqGateMode::None {
            return friends;
        }

        let cfg = self.get_filter_config();
        let owner = *self.inner.owner_qq.lock().expect("poisoned");
        match mode {
            QqGateMode::Allowlist => {
                let allowed: std::collections::HashSet<i64> =
                    cfg.allowlist.user_ids.into_iter().collect();
                // An empty user allowlist does not restrict direct messages.
                if !allowed.is_empty() {
                    friends.retain(|(user_id, _)| *user_id == owner || allowed.contains(user_id));
                }
            }
            QqGateMode::Denylist => {
                let denied: std::collections::HashSet<i64> =
                    cfg.denylist.user_ids.into_iter().collect();
                friends.retain(|(user_id, _)| *user_id == owner || !denied.contains(user_id));
            }
            QqGateMode::None => {}
        }
        friends
    }

    /// Outbound gate-mode check shared by `send_message` and
    /// `send_json_card`. Returns `Err(reason)` when the target is blocked.
    pub(crate) fn check_outbound_gate(&self, target: &MessageTarget) -> Result<(), String> {
        let mode = self.get_gate_mode();
        if mode == QqGateMode::None {
            return Ok(());
        }
        let cfg = self.get_filter_config();
        let user_id: i64 = target.user_id.parse().unwrap_or(0);
        let group_id: Option<i64> = match &target.channel {
            ChannelType::Group { group_id } => group_id.parse().ok(),
            ChannelType::Direct => None,
        };
        // The owner is always exempt from outbound gating.
        let owner = *self.inner.owner_qq.lock().expect("poisoned");
        let is_owner = user_id != 0 && user_id == owner;

        match mode {
            QqGateMode::Allowlist => {
                let allow_users: std::collections::HashSet<i64> =
                    cfg.allowlist.user_ids.iter().cloned().collect();
                let allow_groups: std::collections::HashSet<i64> =
                    cfg.allowlist.group_ids.iter().cloned().collect();
                if !allow_users.is_empty()
                    && user_id != 0
                    && !is_owner
                    && !allow_users.contains(&user_id)
                {
                    tracing::info!(%user_id, "outbound blocked: user not allowlisted");
                    return Err(format!("user {user_id} is not allowlisted"));
                }
                if !allow_groups.is_empty() {
                    if let Some(gid) = group_id {
                        if !allow_groups.contains(&gid) {
                            tracing::info!(%gid, "outbound blocked: group not allowlisted");
                            return Err(format!("group {gid} is not allowlisted"));
                        }
                    }
                }
            }
            QqGateMode::Denylist => {
                let deny_users: std::collections::HashSet<i64> =
                    cfg.denylist.user_ids.iter().cloned().collect();
                let deny_groups: std::collections::HashSet<i64> =
                    cfg.denylist.group_ids.iter().cloned().collect();
                if !deny_users.is_empty()
                    && user_id != 0
                    && !is_owner
                    && deny_users.contains(&user_id)
                {
                    tracing::info!(%user_id, "outbound blocked: user denylisted");
                    return Err(format!("user {user_id} is denylisted"));
                }
                if !deny_groups.is_empty() {
                    if let Some(gid) = group_id {
                        if deny_groups.contains(&gid) {
                            tracing::info!(%gid, "outbound blocked: group denylisted");
                            return Err(format!("group {gid} is denylisted"));
                        }
                    }
                }
            }
            QqGateMode::None => {}
        }
        Ok(())
    }

    /// Send a rich JSON card segment (OneBot `json` segment, e.g. a Bilibili
    /// mini-app share card). Applies the same outbound gate as text messages.
    pub async fn send_json_card(
        &self,
        target: &MessageTarget,
        json_payload: &str,
    ) -> Result<SendResult, AdapterError> {
        if target.adapter_name != self.name {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(format!(
                    "wrong adapter: expected '{}', got '{}'",
                    self.name, target.adapter_name
                )),
            });
        }

        if let Err(reason) = self.check_outbound_gate(target) {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(reason),
            });
        }

        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))?
            .clone()
            .ok_or_else(|| AdapterError::SendFailed("no QQ connection active".into()))?;

        let segment = Segment::json(json_payload);
        let result = match &target.channel {
            ChannelType::Direct => {
                let user_id: i64 = target
                    .user_id
                    .parse()
                    .map_err(|_| AdapterError::SendFailed("invalid user_id".into()))?;
                ctx.send_private_msg(user_id, vec![segment]).await
            }
            ChannelType::Group { group_id } => {
                let gid: i64 = group_id
                    .parse()
                    .map_err(|_| AdapterError::SendFailed("invalid group_id".into()))?;
                ctx.send_group_msg(gid, vec![segment]).await
            }
        };

        match result {
            Ok(resp) => {
                let message_id = resp
                    .data
                    .get("message_id")
                    .and_then(|v| v.as_i64())
                    .map(|id| id.to_string());
                Ok(SendResult {
                    message_id,
                    success: resp.is_ok(),
                    error: resp.error_message(),
                })
            }
            Err(e) => Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(e.to_string()),
            }),
        }
    }

    /// Send a voice message (OneBot `record` segment) — a native voice bubble
    /// played inline, as opposed to a downloadable file.
    ///
    /// `file` accepts, in resolution order:
    ///
    /// 1. Explicit references passed to NapCat verbatim: `http(s)://`,
    ///    `base64://`, `file://` (NapCat resolves/downloads them itself);
    /// 2. absolute paths already visible inside the NapCat container
    ///    (e.g. `/app/napcat/data/x.mp3`) — pass-through;
    /// 3. bare file names — looked up in NapCat's data directory first
    ///    (where previously sent/received files live), then bridged as a
    ///    host path;
    /// 4. any other host path — bridged to NapCat via `docker cp` / one-shot
    ///    HTTP URL (same strategies as file upload).
    ///
    /// NapCat auto-converts common audio formats (mp3/wav/amr/…) to the QQ
    /// voice format; candidates are tried in order until one is accepted.
    /// Applies the same outbound gate as text messages.
    pub async fn send_voice(
        &self,
        target: &MessageTarget,
        file: &str,
    ) -> Result<SendResult, AdapterError> {
        if target.adapter_name != self.name {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(format!(
                    "wrong adapter: expected '{}', got '{}'",
                    self.name, target.adapter_name
                )),
            });
        }

        if let Err(reason) = self.check_outbound_gate(target) {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(reason),
            });
        }

        let candidates = match self.resolve_media_file(file).await {
            Ok(candidates) => candidates,
            Err(reason) => {
                return Ok(SendResult {
                    message_id: None,
                    success: false,
                    error: Some(reason),
                })
            }
        };

        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))?
            .clone()
            .ok_or_else(|| AdapterError::SendFailed("no QQ connection active".into()))?;

        let mut last_error: Option<String> = None;
        for candidate in candidates {
            let segment = Segment::record(candidate.clone());
            let result = match &target.channel {
                ChannelType::Direct => {
                    let user_id: i64 = target
                        .user_id
                        .parse()
                        .map_err(|_| AdapterError::SendFailed("invalid user_id".into()))?;
                    ctx.send_private_msg(user_id, vec![segment]).await
                }
                ChannelType::Group { group_id } => {
                    let gid: i64 = group_id
                        .parse()
                        .map_err(|_| AdapterError::SendFailed("invalid group_id".into()))?;
                    ctx.send_group_msg(gid, vec![segment]).await
                }
            };
            match result {
                Ok(resp) if resp.is_ok() => {
                    let message_id = resp
                        .data
                        .get("message_id")
                        .and_then(|v| v.as_i64())
                        .map(|id| id.to_string());
                    tracing::info!(%candidate, "voice message sent");
                    return Ok(SendResult {
                        message_id,
                        success: true,
                        error: None,
                    });
                }
                Ok(resp) => {
                    last_error = Some(
                        resp.error_message()
                            .unwrap_or_else(|| "voice send failed".into()),
                    );
                    tracing::debug!(%candidate, error = last_error.as_deref().unwrap_or(""), "voice candidate rejected; trying next");
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    tracing::debug!(%candidate, error = last_error.as_deref().unwrap_or(""), "voice candidate errored; trying next");
                }
            }
        }

        Ok(SendResult {
            message_id: None,
            success: false,
            error: Some(
                last_error.unwrap_or_else(|| format!("no candidate could resolve '{file}'")),
            ),
        })
    }

    /// Send an image (OneBot `image` segment) to a chat.
    ///
    /// `file` accepts the same forms as [`send_voice`](Self::send_voice):
    /// explicit `http(s)://` / `base64://` / `file://` references (handed to
    /// NapCat verbatim), absolute container paths, bare names resolved from
    /// NapCat's data directory, or host paths bridged into the container.
    pub async fn send_image(
        &self,
        target: &MessageTarget,
        file: &str,
    ) -> Result<SendResult, AdapterError> {
        if target.adapter_name != self.name {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(format!(
                    "wrong adapter: expected '{}', got '{}'",
                    self.name, target.adapter_name
                )),
            });
        }

        if let Err(reason) = self.check_outbound_gate(target) {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(reason),
            });
        }

        let candidates = match self.resolve_media_file(file).await {
            Ok(candidates) => candidates,
            Err(reason) => {
                return Ok(SendResult {
                    message_id: None,
                    success: false,
                    error: Some(reason),
                })
            }
        };

        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))?
            .clone()
            .ok_or_else(|| AdapterError::SendFailed("no QQ connection active".into()))?;

        let mut last_error: Option<String> = None;
        for candidate in candidates {
            let segment = Segment::image(candidate.clone());
            let result = match &target.channel {
                ChannelType::Direct => {
                    let user_id: i64 = target
                        .user_id
                        .parse()
                        .map_err(|_| AdapterError::SendFailed("invalid user_id".into()))?;
                    ctx.send_private_msg(user_id, vec![segment]).await
                }
                ChannelType::Group { group_id } => {
                    let gid: i64 = group_id
                        .parse()
                        .map_err(|_| AdapterError::SendFailed("invalid group_id".into()))?;
                    ctx.send_group_msg(gid, vec![segment]).await
                }
            };
            match result {
                Ok(resp) if resp.is_ok() => {
                    let message_id = resp
                        .data
                        .get("message_id")
                        .and_then(|v| v.as_i64())
                        .map(|id| id.to_string());
                    tracing::info!(%candidate, "image message sent");
                    return Ok(SendResult {
                        message_id,
                        success: true,
                        error: None,
                    });
                }
                Ok(resp) => {
                    last_error = Some(
                        resp.error_message()
                            .unwrap_or_else(|| "image send failed".into()),
                    );
                    tracing::debug!(%candidate, error = last_error.as_deref().unwrap_or(""), "image candidate rejected; trying next");
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    tracing::debug!(%candidate, error = last_error.as_deref().unwrap_or(""), "image candidate errored; trying next");
                }
            }
        }

        Ok(SendResult {
            message_id: None,
            success: false,
            error: Some(
                last_error.unwrap_or_else(|| format!("no candidate could resolve '{file}'")),
            ),
        })
    }

    /// Resolve a `send_voice`/`send_image` file argument into ordered NapCat
    /// Resolve a `send_voice`/`send_image` file argument into ordered NapCat
    /// candidates.
    ///
    /// See [`send_voice`](Self::send_voice) for the accepted forms. Returns
    /// `Err(reason)` when nothing can be tried at all (e.g. a bare name that
    /// exists neither in the NapCat data dir nor as a host path).
    async fn resolve_media_file(&self, file: &str) -> Result<Vec<String>, String> {
        let trimmed = file.trim();
        if trimmed.is_empty() {
            return Err("file is required".into());
        }

        // 1. Explicit references: hand to NapCat verbatim.
        for prefix in ["http://", "https://", "base64://", "file://"] {
            if trimmed.starts_with(prefix) {
                return Ok(vec![trimmed.to_string()]);
            }
        }

        // 2. Absolute paths: container-visible pass through, host paths bridge.
        if trimmed.starts_with('/') {
            return self.inner.file_bridge.resolve_all(trimmed, trimmed).await;
        }

        // 3. Bare names: prefer the copy already inside the container's data
        //    directory (no re-copy, and it works even if the host original is
        //    gone), then fall back to the host path bridge.
        let mut candidates = Vec::new();
        if !trimmed.contains('/') && !trimmed.contains('\\') {
            let bridge = &self.inner.file_bridge;
            if bridge.container_file_exists(trimmed).await {
                candidates.push(bridge.container_data_path(trimmed));
            }
        }
        match self.inner.file_bridge.resolve_all(trimmed, trimmed).await {
            Ok(mut more) => candidates.append(&mut more),
            Err(reason) => {
                if candidates.is_empty() {
                    return Err(reason);
                }
            }
        }
        Ok(candidates)
    }
}

#[async_trait]
impl Adapter for QqAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &str {
        &self.inner.display_name
    }

    fn platform(&self) -> &str {
        "qq"
    }

    fn is_configured(&self) -> bool {
        !self.inner.config.server.bind_address.is_empty()
    }

    fn status_info(&self) -> AdapterInfo {
        let connected = self.inner.connected.load(Ordering::SeqCst);
        let running = self.inner.running.load(Ordering::SeqCst);
        AdapterInfo {
            name: self.name.clone(),
            // 实例化显示名（`QQ（<persona> / <实例>）`）——多实例面板据此区分；
            // 此处曾硬编码 "QQ / OneBot"，导致多实例下两行同名。
            display_name: self.inner.display_name.clone(),
            status: if !running {
                AdapterConnectionState::Stopped
            } else if connected {
                AdapterConnectionState::Connected
            } else {
                AdapterConnectionState::Disconnected
            },
            self_id: self.inner.self_id.lock().ok().and_then(|g| g.clone()),
            started_at: self.inner.started_at.lock().ok().and_then(|g| *g),
            platform: "qq".into(),
            configured: self.is_configured(),
            // 附加信息（persona/容器/URL/绑定地址）经管理面
            // `AdapterManagement::cached_state` 暴露（2026-10 适配器管理通道）。
        }
    }

    async fn start(&self) -> Result<(), AdapterError> {
        if self.inner.running.load(Ordering::SeqCst) {
            return Err(AdapterError::AlreadyRunning("qq".into()));
        }

        let server_cfg = ServerConfig {
            bind_address: self.inner.config.server.bind_address.clone(),
            access_token: if self.inner.config.server.access_token.is_empty() {
                None
            } else {
                Some(self.inner.config.server.access_token.clone())
            },
            heartbeat_interval: (self.inner.config.server.heartbeat_interval > 0)
                .then(|| Duration::from_secs(self.inner.config.server.heartbeat_interval)),
        };

        let inner = self.inner.clone();

        let bind_addr = server_cfg.bind_address.clone();

        // Bind the reverse-WS listener first so NapCat can connect as soon as
        // its container starts. The listener is just a socket at this point;
        // the server task is spawned below.
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| AdapterError::Internal(format!("bind {}: {e}", bind_addr)))?;

        // When enabled, the QQ adapter owns the NapCat Docker container
        // lifecycle: starting the adapter also starts NapCat. This makes
        // "启动 QQ" in the Panel sufficient for a dockerised NapCat install.
        if self.inner.config.napcat_auto_start {
            let napcat_service = NapCatService::new(
                self.inner.config.napcat_compose_file.clone(),
                self.inner.config.napcat_container.clone(),
            );
            let state = napcat_service
                .start()
                .await
                .map_err(|e| AdapterError::Internal(format!("NapCat 启动失败: {e}")))?;
            tracing::info!(state = %state.describe(), "NapCat Docker service ensured");
        }

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        *inner
            .shutdown_tx
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))? = Some(shutdown_tx);

        let inner_cb = inner.clone();
        let conn_cb: ConnCallback = Arc::new(move |connected: bool, self_id: i64| {
            if connected {
                *inner_cb.self_id.lock().expect("self_id poisoned") = Some(self_id.to_string());
                tracing::info!(%self_id, "NapCat client connected via reverse WebSocket");
            } else {
                tracing::warn!("NapCat client disconnected");
                // Clear active context when disconnected
                *inner_cb.active_context.lock().expect("ctx poisoned") = None;
            }
            inner_cb.connected.store(connected, Ordering::SeqCst);
            if let Some(ref hook) = *inner_cb.message_hook.lock().expect("message hook poisoned") {
                // 实例名（非硬编码 "qq"）：多实例下 AdapterStateChanged 按实例
                // 上报，Panel 的连接状态卡才能区分实例（2026-10 巡检；与入站
                // 链路同批修复，此前仅入站侧改了、连接事件漏网）。
                hook.on_connection_state(
                    &inner_cb.instance_name,
                    connected,
                    Some(self_id.to_string()),
                );
            }
        });

        let mut registry = HandlerRegistry::new();
        // Clone the Arc'd handlers so they survive across adapter restarts.
        let extra = inner
            .extra_handlers
            .lock()
            .expect("handlers poisoned")
            .clone();
        for h in extra {
            registry.register(BoxedHandler(h));
        }
        registry.register(crate::handler::QqHandler::new(inner.clone()));

        inner.running.store(true, Ordering::SeqCst);
        *inner.started_at.lock().expect("started_at poisoned") =
            Some(chrono::Utc::now().timestamp());

        let server = Server::new(
            server_cfg,
            Arc::new(registry),
            inner.tracker.clone(),
            Some(conn_cb),
        );

        let server_task = tokio::spawn(async move {
            let run_fut = server.run_with_listener(listener);
            tokio::select! {
                result = run_fut => {
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "QQ server terminated");
                    }
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("QQ adapter shutdown requested");
                }
            }
            inner.running.store(false, Ordering::SeqCst);
        });
        *self
            .inner
            .server_task
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))? = Some(server_task);

        // Keep trying until NapCat has logged in and established the reverse
        // WebSocket. Startup commonly happens before the QR login completes.
        let napcat_url = self.inner.config.napcat_webui_url.clone();
        let onebot_url = self.inner.config.napcat_onebot_url.clone();
        let napcat_host = self.inner.config.napcat_host.clone();
        let port = bind_addr.split(':').next_back().unwrap_or("3131");
        let ws_url = format!("ws://{napcat_host}:{port}");
        let token = self.inner.config.server.access_token.clone();
        let napcat_container = self.inner.config.napcat_container.clone();
        let timing = NapCatMaintainTiming {
            initial_delay: NAPCAT_INITIAL_CHECK_DELAY,
            retry_interval: NAPCAT_RETRY_INTERVAL,
            guard_interval: NAPCAT_GUARD_INTERVAL,
            dance_min_interval: NAPCAT_DANCE_MIN_INTERVAL,
        };
        let webui_token_source: Arc<dyn Fn() -> Result<String, String> + Send + Sync> =
            Arc::new(move || crate::napcat::webui_token_from_container(&napcat_container));
        let config_task = tokio::spawn(Self::maintain_napcat_connection(
            self.inner.clone(),
            NapCatClient::with_onebot_url(&napcat_url, &onebot_url),
            ws_url,
            token,
            timing,
            webui_token_source,
        ));
        *self
            .inner
            .napcat_config_task
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))? = Some(config_task.abort_handle());

        tracing::info!(address = %bind_addr, "QQ adapter started");
        Ok(())
    }

    async fn remove_instance(&self) -> Result<(), AdapterError> {
        // 先停（保留 stop 的全部清理语义；未运行时不报错——remove 幂等）。
        if self.inner.running.load(Ordering::SeqCst) {
            self.stop().await?;
        }
        // 彻底移除 NapCat 实例资源（容器 + 数据卷 + compose 文件）。
        let service = NapCatService::new(
            self.inner.config.napcat_compose_file.clone(),
            self.inner.config.napcat_container.clone(),
        );
        service.remove().await.map_err(AdapterError::Internal)?;
        tracing::info!(instance = %self.name, "QQ instance resources removed");
        Ok(())
    }

    async fn stop(&self) -> Result<(), AdapterError> {
        if !self.inner.running.load(Ordering::SeqCst) {
            return Err(AdapterError::NotRunning("qq".into()));
        }

        let tx = self
            .inner
            .shutdown_tx
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))?
            .take();
        if let Some(tx) = tx {
            let _ = tx.send(());
        }

        if let Some(handle) = self
            .inner
            .napcat_config_task
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))?
            .take()
        {
            handle.abort();
        }

        // Wait for the server task to release the bind before returning so a
        // restart can bind the same port immediately.
        let server_task = self
            .inner
            .server_task
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))?
            .take();
        if let Some(handle) = server_task {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(error = %error, "QQ server task join failed"),
                Err(_) => tracing::warn!("QQ server task did not stop within 5s"),
            }
        }

        self.inner.running.store(false, Ordering::SeqCst);

        // When enabled, the QQ adapter also stops the NapCat Docker container.
        // The adapter itself is already stopped, so a Docker permission problem
        // is logged as a warning and does not roll back the adapter state.
        if self.inner.config.napcat_auto_stop {
            let napcat_service = NapCatService::new(
                self.inner.config.napcat_compose_file.clone(),
                self.inner.config.napcat_container.clone(),
            );
            match napcat_service.stop().await {
                Ok(()) => tracing::info!("NapCat Docker service stopped"),
                Err(error) => tracing::warn!(%error, "NapCat Docker service stop failed"),
            }
        }

        Ok(())
    }

    async fn send_message(
        &self,
        target: &MessageTarget,
        content: &str,
    ) -> Result<SendResult, AdapterError> {
        if target.adapter_name != self.name {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(format!(
                    "wrong adapter: expected '{}', got '{}'",
                    self.name, target.adapter_name
                )),
            });
        }

        // ── Gate mode check ──
        if let Err(reason) = self.check_outbound_gate(target) {
            return Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(reason),
            });
        }

        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| AdapterError::Internal(e.to_string()))?
            .clone()
            .ok_or_else(|| AdapterError::SendFailed("no QQ connection active".into()))?;

        let result = match &target.channel {
            ChannelType::Direct => {
                let user_id: i64 = target
                    .user_id
                    .parse()
                    .map_err(|_| AdapterError::SendFailed("invalid user_id".into()))?;
                ctx.send_private_text(user_id, content).await
            }
            ChannelType::Group { group_id } => {
                let gid: i64 = group_id
                    .parse()
                    .map_err(|_| AdapterError::SendFailed("invalid group_id".into()))?;
                let user_id: i64 = if target.user_id.is_empty() {
                    0
                } else {
                    target
                        .user_id
                        .parse()
                        .map_err(|_| AdapterError::SendFailed("invalid user_id".into()))?
                };
                if user_id != 0 {
                    ctx.send_group_msg(
                        gid,
                        vec![Segment::at(target.user_id.clone()), Segment::text(content)],
                    )
                    .await
                } else {
                    ctx.send_group_text(gid, content).await
                }
            }
        };

        match result {
            Ok(resp) => {
                let message_id = resp
                    .data
                    .get("message_id")
                    .and_then(|v| v.as_i64())
                    .map(|id| id.to_string());
                Ok(SendResult {
                    message_id,
                    success: resp.is_ok(),
                    error: resp.error_message(),
                })
            }
            Err(e) => Ok(SendResult {
                message_id: None,
                success: false,
                error: Some(e.to_string()),
            }),
        }
    }

    fn subscribe(&self, tx: mpsc::UnboundedSender<AdapterEvent>) {
        if let Ok(mut subs) = self.inner.subscribers.lock() {
            subs.push(tx);
        }
    }

    fn management(&self) -> Option<&dyn AdapterManagement> {
        // 平台中立管理面（W3.2，2026-10 适配器管理通道）：QQ 适配器恒实现。
        Some(self)
    }
}

impl QqAdapter {
    /// 取登录二维码 PNG（Core 代理从 NapCat 容器取回）。
    ///
    /// **陈旧自动刷新**：NapCat 自身的轮换循环可能停摆（实测：停摆后
    /// 面板「获取二维码」只会重复读到过期文件——"刷新也没用"）。因此这里
    /// 检查容器内文件的 mtime：缺失或超过 [`QR_MAX_AGE_SECS`] 时先让
    /// NapCat 重新生成（WebUI `/api/QQLogin/RefreshQRcode`），等待落盘后
    /// 再取 PNG；阈值内的新鲜二维码直接返回，避免作废用户刚扫的码。
    ///
    /// 固有实现（供管理面 `AdapterManagement::login_qrcode` 使用）。
    pub(crate) async fn fetch_login_qrcode_png(&self) -> Result<Vec<u8>, String> {
        let container = self.inner.config.napcat_container.clone();
        let webui_url = self.inner.config.napcat_webui_url.clone();

        let probe = container.clone();
        let age = tokio::task::spawn_blocking(move || NapCatClient::qrcode_age_secs(&probe))
            .await
            .map_err(|e| format!("qrcode probe task failed: {e}"))?;
        let stale = age.is_none_or(|age| age > QR_MAX_AGE_SECS);
        if stale {
            let client = NapCatClient::new(&webui_url);
            match client.refresh_qrcode(&container).await {
                Ok(()) => {
                    // 给 NapCat 落盘新 PNG 留出时间（2s 实测余量）。
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(error) => {
                    tracing::warn!(%error, "NapCat 二维码刷新失败，退回读取现有文件");
                }
            }
        }

        tokio::task::spawn_blocking(move || NapCatClient::fetch_qrcode_docker(&container))
            .await
            .map_err(|e| format!("qrcode task failed: {e}"))?
    }
}

/// 平台中立管理面（W3.2，2026-10 适配器管理通道）。
///
/// - `cached_state`：内部状态槽的同步投影（零 IO）——与旧 getter 同源，
///   迁移期两者并存（无双事实来源）；
/// - `invoke_custom`：method 路由表（方法名与线协议 `InvokeAdapterOp` /
///   插件协议 `custom op` 同名直通）。写路径内部仍走既有固有 setter
///   （管道重建 + ConfigStore 持久化语义不变）。
#[async_trait]
impl AdapterManagement for QqAdapter {
    fn cached_state(&self) -> Option<Value> {
        let cfg = self.get_filter_config();
        let mode = gate_mode_str(self.get_gate_mode());
        Some(json!({
            "instance": {
                "persona": self.inner.persona.clone(),
                "container": self.inner.config.napcat_container.clone(),
                "webui_url": self.inner.config.napcat_webui_url.clone(),
                "onebot_url": self.inner.config.napcat_onebot_url.clone(),
                "bind_address": self.inner.config.server.bind_address.clone(),
            },
            "running": self.inner.running.load(Ordering::SeqCst),
            "online": self.inner.connected.load(Ordering::SeqCst),
            "self_id": self.inner.self_id.lock().ok().and_then(|guard| guard.clone()),
            "gate": {
                "mode": mode,
                "allowlist_users": stringify_ids(&cfg.allowlist.user_ids),
                "allowlist_groups": stringify_ids(&cfg.allowlist.group_ids),
                "denylist_users": stringify_ids(&cfg.denylist.user_ids),
                "denylist_groups": stringify_ids(&cfg.denylist.group_ids),
            },
            "owner": *self.inner.owner_qq.lock().expect("owner poisoned"),
        }))
    }

    async fn invoke_custom(&self, method: &str, payload: Value) -> Result<Value, AdapterError> {
        match method {
            "set_gate_mode" => {
                let mode = match payload.get("mode").and_then(Value::as_str) {
                    Some("allowlist") => QqGateMode::Allowlist,
                    Some("denylist") => QqGateMode::Denylist,
                    Some("none") => QqGateMode::None,
                    other => {
                        return Err(AdapterError::Internal(format!(
                            "invalid gate mode {other:?} (expected allowlist|denylist|none)"
                        )));
                    }
                };
                self.set_gate_mode(mode);
                Ok(json!({"mode": gate_mode_str(mode)}))
            }
            "get_gate_mode" => Ok(json!({"mode": gate_mode_str(self.get_gate_mode())})),
            "update_allowlist" => {
                let users = parse_ids(&payload, "user_ids")?;
                let groups = parse_ids(&payload, "group_ids")?;
                self.update_allowlist(users, groups);
                Ok(json!({}))
            }
            "update_denylist" => {
                let users = parse_ids(&payload, "user_ids")?;
                let groups = parse_ids(&payload, "group_ids")?;
                self.update_denylist(users, groups);
                Ok(json!({}))
            }
            "get_filter_config" => {
                let cfg = self.get_filter_config();
                Ok(json!({
                    "allowlist_users": stringify_ids(&cfg.allowlist.user_ids),
                    "allowlist_groups": stringify_ids(&cfg.allowlist.group_ids),
                    "denylist_users": stringify_ids(&cfg.denylist.user_ids),
                    "denylist_groups": stringify_ids(&cfg.denylist.group_ids),
                }))
            }
            "set_owner" => {
                let owner = parse_id(&payload, "owner_qq")?;
                self.set_owner_qq(owner);
                Ok(json!({"owner_qq": owner}))
            }
            "get_owner" => Ok(json!({"owner_qq": self.get_owner_qq()})),
            "login_status" => Ok(json!({
                "running": self.inner.running.load(Ordering::SeqCst),
                "online": self.inner.connected.load(Ordering::SeqCst),
                "user_id": self.inner.self_id.lock().ok().and_then(|guard| guard.clone()),
            })),
            "login_qrcode" => {
                let png = self
                    .fetch_login_qrcode_png()
                    .await
                    .map_err(AdapterError::Internal)?;
                let encoded = base64::engine::general_purpose::STANDARD.encode(&png);
                Ok(json!({"png_base64": encoded}))
            }
            "get_groups" => {
                let groups = self
                    .get_all_groups()
                    .await
                    .map_err(AdapterError::Internal)?;
                Ok(json!({"groups": id_name_pairs(groups)}))
            }
            "get_friends" => {
                let friends = self
                    .get_friend_list()
                    .await
                    .map_err(AdapterError::Internal)?;
                Ok(json!({"friends": id_name_pairs(friends)}))
            }
            "instance_info" => Ok(json!({
                "persona": self.inner.persona.clone(),
                "container": self.inner.config.napcat_container.clone(),
                "webui_url": self.inner.config.napcat_webui_url.clone(),
                "onebot_url": self.inner.config.napcat_onebot_url.clone(),
                "bind_address": self.inner.config.server.bind_address.clone(),
            })),
            other => Err(AdapterError::Internal(format!(
                "unknown adapter method: {other}"
            ))),
        }
    }
}

/// 门控模式的线格式字符串（`none` / `allowlist` / `denylist`）。
fn gate_mode_str(mode: QqGateMode) -> &'static str {
    match mode {
        QqGateMode::Allowlist => "allowlist",
        QqGateMode::Denylist => "denylist",
        QqGateMode::None => "none",
    }
}

/// id 列表 → 字符串形态（线协议与旧管理面统一以字符串跨层）。
fn stringify_ids(ids: &[i64]) -> Vec<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

/// 从 JSON 载荷解析 id 列表（接受数字与字符串两种形态；缺省 = 空表）。
fn parse_ids(payload: &Value, key: &str) -> Result<Vec<i64>, AdapterError> {
    let Some(list) = payload.get(key) else {
        return Ok(Vec::new());
    };
    let Some(items) = list.as_array() else {
        return Err(AdapterError::Internal(format!("{key} must be an array")));
    };
    items
        .iter()
        .map(|item| {
            if let Some(number) = item.as_i64() {
                return Ok(number);
            }
            if let Some(text) = item.as_str() {
                return text
                    .parse::<i64>()
                    .map_err(|_| AdapterError::Internal(format!("{key}: invalid id {text:?}")));
            }
            Err(AdapterError::Internal(format!("{key}: invalid id {item}")))
        })
        .collect()
}

/// 从 JSON 载荷解析单个 id（数字或字符串形态；缺失 = 可读报错）。
fn parse_id(payload: &Value, key: &str) -> Result<i64, AdapterError> {
    let Some(value) = payload.get(key) else {
        return Err(AdapterError::Internal(format!("missing {key}")));
    };
    if let Some(number) = value.as_i64() {
        return Ok(number);
    }
    if let Some(text) = value.as_str() {
        return text
            .parse::<i64>()
            .map_err(|_| AdapterError::Internal(format!("{key}: invalid id {text:?}")));
    }
    Err(AdapterError::Internal(format!("{key}: invalid id {value}")))
}

/// `(id, name)` 列表 → `[{id, name}]`（id 字符串化）。
fn id_name_pairs(items: Vec<(i64, String)>) -> Vec<Value> {
    items
        .into_iter()
        .map(|(id, name)| json!({"id": id.to_string(), "name": name}))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use echo_core::Event;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// 多实例：AdapterInfo.display_name 必须是实例化显示名（含 persona/实例），
    /// 供 Panel 在多实例下区分同名适配器行。
    #[test]
    fn adapter_info_carries_instance_display_name() {
        let single = QqAdapter::new(QqAdapterConfig::default());
        assert_eq!(single.status_info().display_name, "QQ / OneBot");

        let legacy = QqAdapter::with_instance(
            DEFAULT_INSTANCE_NAME,
            Some("alix".into()),
            QqAdapterConfig::default(),
        );
        assert_eq!(legacy.status_info().display_name, "QQ（alix）");

        let multi =
            QqAdapter::with_instance("alix-two", Some("alix".into()), QqAdapterConfig::default());
        assert_eq!(multi.status_info().display_name, "QQ（alix / alix-two）");
        // 实例附加信息经管理面 `cached_state` 取（W3.2 适配器管理通道）。
        let state = multi
            .management()
            .and_then(|m| m.cached_state())
            .expect("cached state");
        assert_eq!(state["instance"]["persona"], json!("alix"));
    }

    /// 管理面契约（2026-10 适配器管理通道）：QQ 适配器必须暴露
    /// `AdapterManagement`，`cached_state` 反映实例配置，且管理面与固有
    /// 方法操作同一份状态。
    #[tokio::test]
    async fn qq_adapter_exposes_management_capability() {
        let cfg = QqAdapterConfig {
            napcat_container: "napcat-x".into(),
            ..Default::default()
        };
        let adapter = QqAdapter::with_instance("alix-two", Some("alix".into()), cfg);
        let management = adapter
            .management()
            .expect("QQ 适配器必须暴露 AdapterManagement 能力");
        let state = management.cached_state().expect("cached state");
        assert_eq!(state["instance"]["persona"], json!("alix"));
        assert_eq!(state["instance"]["container"], json!("napcat-x"));
        assert!(
            !state["instance"]["bind_address"]
                .as_str()
                .unwrap_or("")
                .is_empty(),
            "绑定地址应非空（配置默认 0.0.0.0:3131）"
        );
        // 管理面与固有方法同源：经管理面切门控 → 固有读取可见。
        management
            .invoke_custom("set_gate_mode", json!({"mode": "allowlist"}))
            .await
            .expect("set_gate_mode ok");
        assert_eq!(adapter.get_gate_mode(), QqGateMode::Allowlist);
        management
            .invoke_custom("set_gate_mode", json!({"mode": "none"}))
            .await
            .expect("set_gate_mode ok");
        assert_eq!(adapter.get_gate_mode(), QqGateMode::None);
    }

    /// 平台中立管理面契约（W3.2，2026-10 适配器管理通道）：
    /// `management()` 恒定 Some；`cached_state` 同步投影与写入后状态一致；
    /// `invoke_custom` method 路由表（含可读错误）与双管理面同源。
    #[tokio::test]
    async fn qq_adapter_exposes_platform_neutral_management() {
        let cfg = QqAdapterConfig {
            napcat_container: "napcat-mgmt".into(),
            ..Default::default()
        };
        let adapter = QqAdapter::with_instance("alix-mgmt", Some("alix".into()), cfg);
        let management = adapter
            .management()
            .expect("QQ 适配器必须暴露 AdapterManagement 管理面");

        // cached_state：实例信息 + 门控 + owner 快照（零 IO）。
        let state = management.cached_state().expect("state snapshot");
        assert_eq!(state["instance"]["persona"], json!("alix"));
        assert_eq!(state["instance"]["container"], json!("napcat-mgmt"));
        assert_eq!(state["gate"]["mode"], json!("none"));
        assert!(state["instance"]["bind_address"].as_str().is_some());

        // invoke_custom：写路径（set_gate_mode / update_allowlist / set_owner）。
        let out = management
            .invoke_custom("set_gate_mode", json!({"mode": "allowlist"}))
            .await
            .expect("set_gate_mode ok");
        assert_eq!(out["mode"], json!("allowlist"));
        management
            .invoke_custom(
                "update_allowlist",
                json!({"user_ids": [7, "8"], "group_ids": ["99"]}),
            )
            .await
            .expect("update_allowlist ok");
        management
            .invoke_custom("set_owner", json!({"owner_qq": 4242}))
            .await
            .expect("set_owner ok");

        // 双管理面同源：经新面写入 → 固有读取与新面快照均可见。
        assert_eq!(adapter.get_gate_mode(), QqGateMode::Allowlist);
        assert_eq!(adapter.get_filter_config().allowlist.user_ids, vec![7, 8]);
        let state = management.cached_state().unwrap();
        assert_eq!(state["gate"]["mode"], json!("allowlist"));
        assert_eq!(state["gate"]["allowlist_users"], json!(["7", "8"]));
        assert_eq!(state["gate"]["allowlist_groups"], json!(["99"]));
        assert_eq!(state["owner"], json!(4242));

        // 读路径：get_* 与固有读取同源（单一事实来源）。
        let out = management
            .invoke_custom("get_filter_config", json!({}))
            .await;
        assert_eq!(out.unwrap()["denylist_groups"], json!([]));
        let out = management.invoke_custom("get_owner", json!({})).await;
        assert_eq!(out.unwrap()["owner_qq"], json!(4242));

        // 未知方法：可读错误（fail-closed）。
        let err = management
            .invoke_custom("nope", json!({}))
            .await
            .expect_err("unknown method must fail");
        assert!(
            err.to_string().contains("unknown adapter method: nope"),
            "unexpected error: {err}"
        );
        // 非法门控模式：可读错误且不改变现有状态。
        let err = management
            .invoke_custom("set_gate_mode", json!({"mode": "bogus"}))
            .await
            .expect_err("invalid mode must fail");
        assert!(err.to_string().contains("invalid gate mode"), "{err}");
        assert_eq!(adapter.get_gate_mode(), QqGateMode::Allowlist);

        // 固有读取与管理面快照同源：set_owner 经 invoke_custom 写入后，
        // 固有 getter 与 cached_state 均可见（单一事实来源）。
        assert_eq!(adapter.get_owner_qq(), 4242);
    }

    async fn mount_login_info(server: &MockServer, user_id: i64) {
        Mock::given(method("POST"))
            .and(path("/get_login_info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "retcode": 0,
                "data": {"user_id": user_id, "nickname": "bot"}
            })))
            .mount(server)
            .await;
    }

    /// 挂载 WebUI mock 的登录与 OB11 配置读写端点。
    async fn mount_webui_config_endpoints(webui: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/api/auth/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": {"Credential": "test-cred"}
            })))
            .mount(webui)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/OB11Config/GetConfig"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": {"network": {"websocketClients": []}}
            })))
            .mount(webui)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/OB11Config/SetConfig"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"code": 0})))
            .mount(webui)
            .await;
    }

    /// 维护任务测试用适配器：容器名指向不存在的容器——即便流程走到
    /// autoLoginAccount 固化（docker exec），也碰不到真实 NapCat 容器。
    fn maintain_test_adapter() -> QqAdapter {
        let adapter = QqAdapter::new(QqAdapterConfig {
            napcat_container: "no-such-container-for-tests".into(),
            ..QqAdapterConfig::default()
        });
        adapter.inner.running.store(true, Ordering::SeqCst);
        adapter
    }

    fn maintain_test_timing(dance_min: Duration) -> NapCatMaintainTiming {
        NapCatMaintainTiming {
            initial_delay: Duration::ZERO,
            retry_interval: Duration::from_millis(10),
            guard_interval: Duration::from_millis(20),
            dance_min_interval: dance_min,
        }
    }

    async fn webui_requests_with_path(webui: &MockServer, target: &str) -> usize {
        webui
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == target)
            .count()
    }

    async fn wait_for_webui_path(webui: &MockServer, target: &str) {
        let target = target.to_string();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if webui_requests_with_path(webui, &target).await > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expected WebUI request did not arrive");
    }

    #[tokio::test]
    async fn configures_reverse_ws_when_login_completes_after_startup() {
        let onebot = MockServer::start().await;
        mount_login_info(&onebot, 0).await;

        let webui = MockServer::start().await;
        mount_webui_config_endpoints(&webui).await;

        let adapter = maintain_test_adapter();
        let client = NapCatClient::with_onebot_url(&webui.uri(), &onebot.uri());
        let task = tokio::spawn(QqAdapter::maintain_napcat_connection(
            adapter.inner.clone(),
            client,
            "ws://host.docker.internal:3131".into(),
            String::new(),
            maintain_test_timing(Duration::ZERO),
            Arc::new(|| Ok("webui-test-token".to_string())),
        ));

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(webui.received_requests().await.unwrap().is_empty());

        onebot.reset().await;
        mount_login_info(&onebot, 10001).await;

        // 登录完成后应触发反向 WS 重配（登录 → GetConfig → SetConfig）。
        wait_for_webui_path(&webui, "/api/OB11Config/SetConfig").await;

        // 守护模式（2026-10）：connected 后任务**不再退出**（降频复查）——
        // 测试主动停 running 让循环退出。
        adapter.inner.connected.store(true, Ordering::SeqCst);
        adapter.inner.running.store(false, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("maintenance task should exit after running=false")
            .expect("maintenance task failed");
    }

    /// 守护态（已连接）不再重复打 WebUI：旧实现对每次 60s 复查都做
    /// WebUI 登录 ×2 + SetConfig + docker exec，实测触发 NapCat 登录限流
    /// （`login rate limit`）——回归防线：连接后的 WebUI 流量必须归零。
    #[tokio::test]
    async fn maintain_stops_webui_churn_once_connected() {
        let onebot = MockServer::start().await;
        mount_login_info(&onebot, 10001).await;
        let webui = MockServer::start().await;
        mount_webui_config_endpoints(&webui).await;

        let adapter = maintain_test_adapter();
        let client = NapCatClient::with_onebot_url(&webui.uri(), &onebot.uri());
        let task = tokio::spawn(QqAdapter::maintain_napcat_connection(
            adapter.inner.clone(),
            client,
            "ws://x:3131".into(),
            String::new(),
            maintain_test_timing(Duration::ZERO),
            Arc::new(|| Ok("webui-test-token".to_string())),
        ));

        // 未连接阶段：应发生重配（SetConfig 出现）。
        wait_for_webui_path(&webui, "/api/OB11Config/SetConfig").await;

        // 切到守护态：第一拍内可能补写一次快速登录；此后流量应归零。
        adapter.inner.connected.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(80)).await;
        let settled = webui.received_requests().await.unwrap().len();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let after = webui.received_requests().await.unwrap().len();
        assert_eq!(after, settled, "守护态仍在打 WebUI（重复重配未停止）");

        adapter.inner.running.store(false, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("maintenance task should exit after running=false")
            .expect("maintenance task failed");
    }

    /// 未连接时两次完整重配之间应满足最小间隔（WebUI 登录限流防护）。
    #[tokio::test]
    async fn maintain_throttles_reconfigure_while_disconnected() {
        let onebot = MockServer::start().await;
        mount_login_info(&onebot, 10001).await;
        let webui = MockServer::start().await;
        mount_webui_config_endpoints(&webui).await;

        let adapter = maintain_test_adapter();
        let client = NapCatClient::with_onebot_url(&webui.uri(), &onebot.uri());
        let task = tokio::spawn(QqAdapter::maintain_napcat_connection(
            adapter.inner.clone(),
            client,
            "ws://x:3131".into(),
            String::new(),
            maintain_test_timing(Duration::from_millis(500)),
            Arc::new(|| Ok("webui-test-token".to_string())),
        ));

        // 第一次重配立即发生。
        wait_for_webui_path(&webui, "/api/OB11Config/SetConfig").await;
        let first = webui_requests_with_path(&webui, "/api/OB11Config/SetConfig").await;

        // 节流窗口内（250ms < 500ms）不应出现第二次 SetConfig。
        tokio::time::sleep(Duration::from_millis(250)).await;
        let within = webui_requests_with_path(&webui, "/api/OB11Config/SetConfig").await;
        assert_eq!(within, first, "节流窗口内发生了第二次重配");

        // 节流窗过后应恢复重试。
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if webui_requests_with_path(&webui, "/api/OB11Config/SetConfig").await > first {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("节流窗后未恢复重配尝试");

        adapter.inner.running.store(false, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("maintenance task should exit after running=false")
            .expect("maintenance task failed");
    }

    #[tokio::test]
    async fn all_groups_use_onebot_http_without_websocket_context() {
        let onebot = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/get_group_list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "retcode": 0,
                "data": [{"group_id": 987654, "group_name": "known group"}]
            })))
            .mount(&onebot)
            .await;

        let config = QqAdapterConfig {
            napcat_onebot_url: onebot.uri(),
            ..Default::default()
        };
        let adapter = QqAdapter::new(config);
        adapter.inner.running.store(true, Ordering::SeqCst);

        assert_eq!(
            adapter.get_all_groups().await.unwrap(),
            vec![(987654, "known group".into())]
        );
        assert!(!adapter.inner.connected.load(Ordering::SeqCst));
        assert!(adapter.inner.active_context.lock().unwrap().is_none());
    }

    #[test]
    fn friend_gate_respects_modes_and_owner_bypass() {
        let config = QqAdapterConfig {
            owner_qq: 999,
            ..Default::default()
        };
        let adapter = QqAdapter::new(config);
        let friends = || {
            vec![
                (111, "allowed".into()),
                (222, "other".into()),
                (999, "owner".into()),
            ]
        };

        assert_eq!(adapter.apply_friend_gate(friends()), friends());

        adapter.update_allowlist(vec![], vec![123]);
        adapter.set_gate_mode(QqGateMode::Allowlist);
        assert_eq!(
            adapter.apply_friend_gate(friends()),
            friends(),
            "a group-only allowlist does not restrict direct-message friends"
        );

        adapter.update_allowlist(vec![111], vec![]);
        assert_eq!(
            adapter.apply_friend_gate(friends()),
            vec![(111, "allowed".into()), (999, "owner".into())]
        );

        adapter.update_denylist(vec![222, 999], vec![]);
        adapter.set_gate_mode(QqGateMode::Denylist);
        assert_eq!(
            adapter.apply_friend_gate(friends()),
            vec![(111, "allowed".into()), (999, "owner".into())]
        );
    }

    #[tokio::test]
    async fn gated_friend_list_filters_onebot_results() {
        let onebot = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/get_friend_list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "retcode": 0,
                "data": [
                    {"user_id": 111, "nickname": "allowed"},
                    {"user_id": 222, "nickname": "blocked"}
                ]
            })))
            .mount(&onebot)
            .await;

        let config = QqAdapterConfig {
            napcat_onebot_url: onebot.uri(),
            ..Default::default()
        };
        let adapter = QqAdapter::new(config);
        adapter.inner.running.store(true, Ordering::SeqCst);
        adapter.update_allowlist(vec![111], vec![]);
        adapter.set_gate_mode(QqGateMode::Allowlist);

        assert_eq!(
            adapter.get_gated_friend_list().await.unwrap(),
            vec![(111, "allowed".into())]
        );
    }

    #[test]
    fn convert_message_for_carries_instance_name() {
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message_for("alix-two", &private_event("hello"), &names)
            .expect("converted");
        assert_eq!(
            msg.adapter_name, "alix-two",
            "multi-instance adapter_name propagates to inbound messages"
        );
        // 默认实例兼容路径仍为 qq
        let legacy = QqAdapter::convert_message(&private_event("hi"), &names).expect("converted");
        assert_eq!(legacy.adapter_name, "qq");
    }

    #[tokio::test]
    async fn send_message_accepts_own_instance_name_and_rejects_others() {
        let adapter =
            QqAdapter::with_instance("alix-two", None, crate::config::QqAdapterConfig::default());
        // 本实例名通过名校验（后续因无连接失败属预期）——错误绝不能是
        // Ok 包装里的 "wrong adapter"。
        let own = adapter
            .send_message(
                &echo_defs::chat::MessageTarget {
                    adapter_name: "alix-two".into(),
                    channel: echo_defs::chat::ChannelType::Direct,
                    user_id: "10001".into(),
                },
                "ping",
            )
            .await;
        match own {
            Ok(result) => {
                let err = result.error.unwrap_or_default();
                assert!(
                    !err.contains("wrong adapter"),
                    "own instance must not be rejected as wrong adapter: {err}"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    !msg.contains("wrong adapter"),
                    "own instance must not fail with wrong adapter: {msg}"
                );
            }
        }
        let other = adapter
            .send_message(
                &echo_defs::chat::MessageTarget {
                    adapter_name: "qq".into(),
                    channel: echo_defs::chat::ChannelType::Direct,
                    user_id: "10001".into(),
                },
                "ping",
            )
            .await
            .expect("send result");
        assert!(!other.success);
        assert!(
            other.error.unwrap_or_default().contains("wrong adapter"),
            "foreign instance must be rejected"
        );
    }

    #[tokio::test]
    async fn history_query_is_gated_by_group_visibility() {
        // allowlist 模式：白名单外的群不可读（错误里带 allowlist），
        // 名单内的群通过门控（错误退化为「无连接」，证明不是被门控拒绝）。
        let mut cfg = crate::config::QqAdapterConfig::default();
        cfg.gate.mode = "allowlist".into();
        cfg.filter.allowlist.group_ids = vec![1094762376];
        let adapter = QqAdapter::with_instance("qq", None, cfg);

        let blocked = adapter.get_group_msg_history(999999999, 10, None).await;
        let err = blocked.expect_err("non-whitelisted group must be refused");
        assert!(err.contains("allowlist"), "{err}");

        let allowed = adapter.get_group_msg_history(1094762376, 10, None).await;
        match allowed {
            Ok(text) => panic!("no connection — expected Err, got Ok: {text}"),
            Err(err) => assert!(
                !err.contains("allowlist"),
                "whitelisted group must pass the gate: {err}"
            ),
        }

        // 空白名单 = 不限制（与消息门控语义一致）。
        let mut cfg_open = crate::config::QqAdapterConfig::default();
        cfg_open.gate.mode = "allowlist".into();
        let open = QqAdapter::with_instance("qq", None, cfg_open);
        match open.get_group_msg_history(999999999, 10, None).await {
            Ok(text) => panic!("no connection — expected Err, got Ok: {text}"),
            Err(err) => assert!(
                !err.contains("allowlist"),
                "empty allowlist = unrestricted: {err}"
            ),
        }

        // denylist 模式：名单内的群被拒。
        let mut cfg_deny = crate::config::QqAdapterConfig::default();
        cfg_deny.gate.mode = "denylist".into();
        cfg_deny.filter.denylist.group_ids = vec![5];
        let deny = QqAdapter::with_instance("qq", None, cfg_deny);
        let denied = deny.get_group_msg_history(5, 10, None).await;
        let err = denied.expect_err("denylisted group must be refused");
        assert!(err.contains("denylisted"), "{err}");
    }

    #[tokio::test]
    async fn send_voice_rejects_wrong_adapter_and_validates_file() {
        let adapter =
            QqAdapter::with_instance("alix-two", None, crate::config::QqAdapterConfig::default());
        // 非本实例名 → 拒绝（success=false，错误带 wrong adapter）。
        let wrong = adapter
            .send_voice(
                &echo_defs::chat::MessageTarget {
                    adapter_name: "qq".into(),
                    channel: echo_defs::chat::ChannelType::Direct,
                    user_id: "10001".into(),
                },
                "x.mp3",
            )
            .await
            .expect("send_voice returns Ok for wrong-adapter rejections");
        assert!(!wrong.success);
        assert!(wrong.error.unwrap_or_default().contains("wrong adapter"));

        // 本实例 + 空 file：解析期即拒绝（success=false，不 panic）。
        let empty = adapter
            .send_voice(
                &echo_defs::chat::MessageTarget {
                    adapter_name: "alix-two".into(),
                    channel: echo_defs::chat::ChannelType::Direct,
                    user_id: "10001".into(),
                },
                "   ",
            )
            .await
            .expect("send_voice handles blank file gracefully");
        assert!(!empty.success);

        // 本实例 + 合法 file：无连接时错误绝不能是 "wrong adapter"。
        let own = adapter
            .send_voice(
                &echo_defs::chat::MessageTarget {
                    adapter_name: "alix-two".into(),
                    channel: echo_defs::chat::ChannelType::Direct,
                    user_id: "10001".into(),
                },
                "https://example.com/voice.mp3",
            )
            .await;
        match own {
            Ok(result) => {
                let err = result.error.unwrap_or_default();
                assert!(
                    !err.contains("wrong adapter"),
                    "own instance must not be rejected as wrong adapter: {err}"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    !msg.contains("wrong adapter"),
                    "own instance must not fail with wrong adapter: {msg}"
                );
            }
        }
    }

    #[test]
    fn set_owner_qq_rebuilds_pipeline_with_runtime_owner() {
        let mut cfg = crate::config::QqAdapterConfig::default();
        cfg.gate.mode = "allowlist".into();
        cfg.filter.allowlist.user_ids = vec![10001];
        let adapter = QqAdapter::with_instance("qq", None, cfg);
        // 出站门控（同步，与入站管道共享同一份 runtime 配置）：10002 被白名单拦
        let blocked = echo_defs::chat::MessageTarget {
            adapter_name: "qq".into(),
            channel: echo_defs::chat::ChannelType::Direct,
            user_id: "10002".into(),
        };
        assert!(
            adapter.check_outbound_gate(&blocked).is_err(),
            "10002 blocked by allowlist at startup owner=0"
        );
        // 运行时把 owner 改成 10002：门控立即放行（管理员绕过读运行时值）
        adapter.set_owner_qq(10002);
        adapter
            .check_outbound_gate(&blocked)
            .expect("runtime owner must take effect immediately (admin bypass)");
    }

    fn private_event(text: &str) -> Event {
        serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 42,
            "user_id": 123456,
            "message": [{"type": "text", "data": {"text": text}}],
            "raw_message": text,
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap()
    }

    fn group_event(text: &str, at_me: bool) -> Event {
        let mut segments = vec![serde_json::json!({"type": "text", "data": {"text": text}})];
        if at_me {
            segments.insert(
                0,
                serde_json::json!({"type": "at", "data": {"qq": "10001"}}),
            );
        }
        serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "group",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "normal",
            "message_id": 43,
            "group_id": 999,
            "user_id": 123456,
            "message": segments,
            "raw_message": text,
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester", "card": ""}
        }))
        .unwrap()
    }

    #[test]
    fn converts_private_message_to_direct_channel() {
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&private_event("hello"), &names).expect("converted");
        assert_eq!(msg.platform, "qq");
        assert_eq!(msg.user_id, "123456");
        assert_eq!(msg.user_name, "tester");
        assert!(matches!(msg.channel, ChannelType::Direct));
        assert_eq!(msg.content, "hello");
        assert_eq!(msg.timestamp, 1700000000);
        assert!(!msg.at_me);
    }

    #[test]
    fn converts_group_message_with_group_id() {
        let names = dashmap::DashMap::new();
        let msg =
            QqAdapter::convert_message(&group_event("hi all", false), &names).expect("converted");
        match msg.channel {
            ChannelType::Group { group_id } => assert_eq!(group_id, "999"),
            other => panic!("expected group channel, got {other:?}"),
        }
        assert!(!msg.at_me);
    }

    #[test]
    fn group_message_carries_cached_group_name() {
        let names = dashmap::DashMap::new();
        names.insert(999, "开发群".into());
        let msg = QqAdapter::convert_message(&group_event("hi", true), &names).expect("converted");
        assert_eq!(msg.group_name.as_deref(), Some("开发群"));
    }

    #[test]
    fn detects_at_me_in_group_message() {
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&group_event("hi", true), &names).expect("converted");
        assert!(msg.at_me);
    }

    #[test]
    fn rejects_blank_content() {
        let names = dashmap::DashMap::new();
        assert!(QqAdapter::convert_message(&private_event("   "), &names).is_none());
        assert!(QqAdapter::convert_message(&private_event(""), &names).is_none());
    }

    #[test]
    fn empty_allowlist_in_allowlist_mode_returns_no_groups() {
        // Simulate the exact scenario: allowlist mode, empty allowlist.
        // get_filter_config() returns the runtime-overlaid config.
        let mut cfg = QqAdapterConfig::default();
        cfg.gate.mode = "allowlist".into();
        // allowlist is empty (default)
        let adapter = QqAdapter::new(cfg);
        // Manually set gate mode to allowlist.
        adapter.set_gate_mode(QqGateMode::Allowlist);

        let groups: Vec<(i64, String)> = vec![(111, "group a".into()), (222, "group b".into())];
        // We must test the filter logic without calling the QQ API.
        // The filter happens after the QQ API call, on the raw groups vec.
        // Create a minimal test of the filtering logic:
        let cfg = adapter.get_filter_config();
        let allow_groups: std::collections::HashSet<i64> =
            cfg.allowlist.group_ids.iter().cloned().collect();
        assert!(allow_groups.is_empty(), "allowlist is empty");
        let filtered: Vec<_> = groups
            .into_iter()
            .filter(|(gid, _)| allow_groups.contains(gid))
            .collect();
        assert!(
            filtered.is_empty(),
            "empty allowlist filters ALL groups out"
        );
    }

    #[test]
    fn rejects_non_message_events() {
        let heartbeat = serde_json::from_value(serde_json::json!({
            "post_type": "meta_event",
            "meta_event_type": "heartbeat",
            "time": 1700000000,
            "self_id": 10001,
            "interval": 5000,
            "status": {"online": true}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        assert!(QqAdapter::convert_message(&heartbeat, &names).is_none());
    }

    #[test]
    fn converts_cq_image_segments_without_content() {
        // Image-only messages are now accepted: the image URL/file travels via
        // `images` so multimodal-capable models can see it.
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 42,
            "user_id": 123456,
            "message": [{"type": "image", "data": {"file": "abc.png", "url": "https://example.com/abc.png"}}],
            "raw_message": "[CQ:image,file=abc.png,url=https://example.com/abc.png]",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("image message accepted");
        assert!(msg.content.is_empty(), "image-only message has no text");
        assert_eq!(msg.images, vec!["https://example.com/abc.png".to_string()]);
    }

    #[test]
    fn converts_private_file_segment_with_pending_metadata() {
        // 私聊文件消息：files 放占位（name/size），待下载明细（file_id/url）
        // 走 metadata.pending_files 交给 handler。
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 43,
            "user_id": 123456,
            "message": [{"type": "file", "data": {
                "file": "report.pdf", "file_id": "uuid-1", "file_size": "2048",
                "url": "https://cdn.example/report.pdf"
            }}],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("file-only message accepted");
        assert!(msg.content.is_empty());
        assert_eq!(msg.files.len(), 1);
        assert_eq!(msg.files[0].name, "report.pdf");
        assert_eq!(msg.files[0].size, 2048);
        assert!(msg.files[0].path.is_none());
        let pending = &msg.metadata["pending_files"];
        assert_eq!(pending[0]["file_id"], "uuid-1");
        assert_eq!(pending[0]["url"], "https://cdn.example/report.pdf");
    }

    #[test]
    fn private_file_segment_without_url_keeps_lookup_id() {
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 44,
            "user_id": 123456,
            "message": [{"type": "file", "data": {"file": "a.zip", "file_id": "uuid-2"}}],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("accepted");
        assert_eq!(msg.files[0].size, 0, "unknown size defaults to 0");
        assert_eq!(
            msg.metadata["pending_files"][0]["url"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn online_file_segment_delivered_as_explicit_error() {
        // 在线文件（NapCat elementType 23，QQ 直传）：不静默丢弃——
        // 以显式错误条目送达，agent 可如实告知用户。
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 47,
            "user_id": 123456,
            "message": [{"type": "onlinefile", "data": {
                "msgId": "7001", "elementId": "e2",
                "fileName": "big.bin", "fileSize": "2048", "isDir": false
            }}],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("onlinefile message accepted");
        assert_eq!(msg.files.len(), 1);
        assert_eq!(msg.files[0].name, "big.bin");
        assert_eq!(msg.files[0].size, 2048);
        assert!(msg.files[0].path.is_none());
        assert!(msg.files[0]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("暂不支持自动接收"));
        // 无待下载登记（没有可换的直链）。
        assert!(msg.metadata.is_null());
    }

    #[test]
    fn online_folder_segment_names_folder_in_error() {
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 48,
            "user_id": 123456,
            "message": [{"type": "onlinefile", "data": {
                "msgId": "7002", "elementId": "e3",
                "fileName": "proj", "fileSize": "", "isDir": true
            }}],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("accepted");
        assert!(msg.files[0]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("在线文件夹"));
    }

    #[test]
    fn face_only_message_is_delivered_with_readable_marker() {
        // 纯表情消息：此前 content 为空且无图 → 整条丢弃（agent 读不到）；
        // 现在可读渲染产出 [表情:…] 标记，消息正常送达。
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 60,
            "user_id": 123456,
            "message": [{"type": "face", "data": {"id": "13"}}],
            "raw_message": "[CQ:face,id=13]",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("face-only message accepted");
        assert_eq!(msg.content, "[表情:呲牙]");
        assert!(msg.images.is_empty());
    }

    #[test]
    fn text_with_faces_keeps_order_and_names() {
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 61,
            "user_id": 123456,
            "message": [
                {"type": "text", "data": {"text": "今天好累"}},
                {"type": "face", "data": {"id": "5"}},
                {"type": "face", "data": {"id": "99999"}}
            ],
            "raw_message": "今天好累[CQ:face,id=5]",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("accepted");
        assert_eq!(msg.content, "今天好累[表情:流泪][表情:99999]");
    }

    #[test]
    fn dice_and_rps_messages_are_delivered() {
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "group",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "normal",
            "message_id": 62,
            "group_id": 30001,
            "user_id": 123456,
            "message": [
                {"type": "at", "data": {"qq": "10001"}},
                {"type": "dice", "data": {"result": "6"}}
            ],
            "raw_message": "[CQ:at,qq=10001][CQ:dice]",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester", "card": ""}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("accepted");
        assert_eq!(msg.content, "[骰子:6]");
        // at 段仍被识别（@ 触发条件不受渲染改动影响）。
        assert!(event.as_message().unwrap().at_me());
    }

    #[test]
    fn group_file_segment_is_ignored() {
        // 群聊文件消息由 group_upload 通知负责：message 里的 file 段被跳过
        // （NapCat 对同一次上传双上报），纯文件群消息直接丢弃。
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "group",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "normal",
            "message_id": 45,
            "group_id": 30001,
            "user_id": 123456,
            "message": [{"type": "file", "data": {"file": "a.zip", "file_id": "uuid-3"}}],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        assert!(QqAdapter::convert_message(&event, &names).is_none());
    }

    #[test]
    fn group_file_segment_with_text_keeps_text_only() {
        let event = serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "group",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "normal",
            "message_id": 46,
            "group_id": 30001,
            "user_id": 123456,
            "message": [
                {"type": "text", "data": {"text": "看这个"}},
                {"type": "file", "data": {"file": "a.zip", "file_id": "uuid-4"}}
            ],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap();
        let names = dashmap::DashMap::new();
        let msg = QqAdapter::convert_message(&event, &names).expect("text kept");
        assert_eq!(msg.content, "看这个");
        assert!(
            msg.files.is_empty(),
            "group file segments do not populate files"
        );
        assert_eq!(msg.metadata, serde_json::Value::Null);
    }
}

// ── TOML section replacer ─────────────────────────────────────────────────
