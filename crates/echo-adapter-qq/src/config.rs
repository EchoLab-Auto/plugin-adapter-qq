//! Configuration types for the QQ adapter.

use echo_adapter::filter::{
    AdminBypassFilter, AllowlistFilter, ContentLengthFilter, DenylistFilter, FilterPipeline,
    KeywordBlockFilter, RateLimitEntry, RateLimitFilter,
};
use serde::{Deserialize, Serialize};

/// Full QQ adapter configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct QqAdapterConfig {
    /// Whether this adapter is enabled.
    pub enabled: bool,
    /// Reverse-WS server settings.
    pub server: QqServerConfig,
    /// Trigger gating (what messages auto-reply to).
    pub trigger: QqTriggerConfig,
    /// Owner QQ number for admin bypass.
    pub owner_qq: i64,
    /// Command prefix for built-in commands.
    pub command_prefix: String,
    /// Filter configuration.
    pub filter: QqFilterConfig,
    /// Gate mode configuration.
    #[serde(default)]
    pub gate: QqGateConfig,
    /// NapCat WebUI URL for API integration.
    /// Default `http://localhost:6099`.
    #[serde(default = "default_napcat_url")]
    pub napcat_webui_url: String,
    /// OneBot HTTP API URL used for management queries such as the full
    /// group/friend list. Default `http://localhost:3000`.
    #[serde(default = "default_onebot_url")]
    pub napcat_onebot_url: String,
    /// Host used to reach NapCat's reverse-WS port when auto-configuring it.
    /// Defaults to `host.docker.internal` (NapCat in Docker); set to
    /// `127.0.0.1` when running everything on one host.
    #[serde(default = "default_napcat_host")]
    pub napcat_host: String,
    /// Docker container name of NapCat. Used to copy files into the container
    /// (`docker cp`) when the Docker CLI is available.
    #[serde(default = "default_napcat_container")]
    pub napcat_container: String,
    /// Writable directory inside the NapCat container where files are copied
    /// to, and which is considered "already visible to NapCat".
    #[serde(default = "default_napcat_data_dir")]
    pub napcat_container_data_dir: String,
    /// Start the NapCat Docker container automatically when the QQ adapter
    /// starts. When false, NapCat is assumed to be managed externally.
    #[serde(default = "default_true")]
    pub napcat_auto_start: bool,
    /// Stop the NapCat Docker container automatically when the QQ adapter
    /// stops. Does not remove the container or its data volumes.
    ///
    /// 默认 **false**（2026-09-18 起）：Core 停机/重启不再联动停止 NapCat——
    /// 容器保持运行则 QQ 始终保持在线（NapCat 自身维持与 QQ 服务器的连接），
    /// Core 重启只断反向 WS、重启后自动重连，用户无感知；同时避免频繁容器
    /// 重启触发 QQ 安全策略导致快速登录凭证失效（"用户身份已失效"）。
    /// 需要联动停止时显式配置 `napcat_auto_stop = true`。
    #[serde(default)]
    pub napcat_auto_stop: bool,
    /// Docker Compose file used to start/stop the NapCat service. The file is
    /// relative to the Core working directory when no absolute path is given.
    #[serde(default = "default_napcat_compose_file")]
    pub napcat_compose_file: String,
    /// 接收文件（群文件上传 / 私聊文件）的行为配置。
    #[serde(default)]
    pub files: QqFilesConfig,
}

fn default_true() -> bool {
    true
}

fn default_napcat_url() -> String {
    "http://localhost:6099".into()
}

fn default_onebot_url() -> String {
    "http://localhost:3000".into()
}

fn default_napcat_host() -> String {
    "host.docker.internal".into()
}

fn default_napcat_container() -> String {
    "napcat".into()
}

fn default_napcat_data_dir() -> String {
    "/app/napcat/data".into()
}

fn default_napcat_compose_file() -> String {
    "docker-compose.yml".into()
}

impl Default for QqAdapterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server: QqServerConfig::default(),
            trigger: QqTriggerConfig::default(),
            owner_qq: 0,
            command_prefix: "/".into(),
            filter: QqFilterConfig::default(),
            gate: QqGateConfig::default(),
            napcat_webui_url: default_napcat_url(),
            napcat_onebot_url: default_onebot_url(),
            napcat_host: default_napcat_host(),
            napcat_container: default_napcat_container(),
            napcat_container_data_dir: default_napcat_data_dir(),
            napcat_auto_start: default_true(),
            files: QqFilesConfig::default(),
            // 与字段级 `#[serde(default)]` 保持一致：**false**（见字段注释）。
            // 0f9c9a1 修正了 serde 路径，但漏了这里的 Default impl——无
            // `[adapters.qq]` 段的配置走 CoreConfig::default() 时仍会拿到
            // true（2026-09-26 审计发现并修复）。
            napcat_auto_stop: false,
            napcat_compose_file: default_napcat_compose_file(),
        }
    }
}

impl QqAdapterConfig {
    /// Build the filter pipeline from this config.
    pub fn build_filter_pipeline(&self) -> FilterPipeline {
        let mut pipeline = FilterPipeline::new();

        // Admin bypass filter (runs first).
        let admin_ids: Vec<String> = if self.owner_qq > 0 {
            vec![self.owner_qq.to_string()]
        } else {
            vec![]
        };
        let admin_filter = AdminBypassFilter::new(admin_ids);
        if admin_filter.is_active() {
            pipeline.push(Box::new(admin_filter));
        }

        // Allowlist.
        let allowlist = AllowlistFilter::new(
            self.filter
                .allowlist
                .user_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
            self.filter
                .allowlist
                .group_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
        )
        .with_group_members_open(self.filter.allowlist.group_members_open);
        if allowlist.is_active() {
            pipeline.push(Box::new(allowlist));
        }

        // Denylist.
        let denylist = DenylistFilter::new(
            self.filter
                .denylist
                .user_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
            self.filter
                .denylist
                .group_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
        );
        if denylist.is_active() {
            pipeline.push(Box::new(denylist));
        }

        // Rate limit.
        let rate_limiter = RateLimitFilter::new(
            self.filter
                .rate_limit
                .per_user
                .as_ref()
                .map(|r| RateLimitEntry {
                    max_requests: r.max_requests,
                    window_seconds: r.window_seconds,
                }),
            self.filter
                .rate_limit
                .per_group
                .as_ref()
                .map(|r| RateLimitEntry {
                    max_requests: r.max_requests,
                    window_seconds: r.window_seconds,
                }),
            self.filter
                .rate_limit
                .global
                .as_ref()
                .map(|r| RateLimitEntry {
                    max_requests: r.max_requests,
                    window_seconds: r.window_seconds,
                }),
        );
        if rate_limiter.is_active() {
            pipeline.push(Box::new(rate_limiter));
        }

        // Keyword blocking.
        let keyword = KeywordBlockFilter::new(
            self.filter.keyword.block_keywords.clone(),
            &self.filter.keyword.block_regex,
        );
        if keyword.is_active() {
            pipeline.push(Box::new(keyword));
        }

        // Content length.
        if self.filter.content.max_length > 0 {
            pipeline.push(Box::new(ContentLengthFilter::new(
                self.filter.content.max_length,
            )));
        }

        pipeline
    }

    /// Build the filter pipeline respecting a gate mode (allowlist-only,
    /// denylist-only, or neither). The default [`build_filter_pipeline`]
    /// pushes both unconditionally.
    pub fn build_filter_pipeline_gated(&self, mode: &str) -> FilterPipeline {
        let mut pipeline = FilterPipeline::new();

        // Admin bypass.
        let admin_ids: Vec<String> = if self.owner_qq > 0 {
            vec![self.owner_qq.to_string()]
        } else {
            vec![]
        };
        let admin_filter = AdminBypassFilter::new(admin_ids);
        if admin_filter.is_active() {
            pipeline.push(Box::new(admin_filter));
        }

        // Allowlist / Denylist — mutually exclusive based on gate mode.
        match mode {
            "allowlist" => {
                let f = AllowlistFilter::new(
                    self.filter
                        .allowlist
                        .user_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect(),
                    self.filter
                        .allowlist
                        .group_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect(),
                )
                .with_group_members_open(self.filter.allowlist.group_members_open);
                if f.is_active() {
                    // 启动/重建时的白名单口径留痕（含群成员放行开关）——
                    // 排障时一眼确认线上生效的名单与开关。
                    tracing::info!(
                        users = ?self.filter.allowlist.user_ids,
                        groups = ?self.filter.allowlist.group_ids,
                        group_members_open = self.filter.allowlist.group_members_open,
                        "QQ allowlist filter built"
                    );
                    pipeline.push(Box::new(f));
                }
            }
            "denylist" => {
                let f = DenylistFilter::new(
                    self.filter
                        .denylist
                        .user_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect(),
                    self.filter
                        .denylist
                        .group_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect(),
                );
                if f.is_active() {
                    pipeline.push(Box::new(f));
                }
            }
            _ => {} // none — no allowlist or denylist
        }

        // Rate limit, keyword, content length — shared.
        let rate_limiter = RateLimitFilter::new(
            self.filter
                .rate_limit
                .per_user
                .as_ref()
                .map(|r| RateLimitEntry {
                    max_requests: r.max_requests,
                    window_seconds: r.window_seconds,
                }),
            self.filter
                .rate_limit
                .per_group
                .as_ref()
                .map(|r| RateLimitEntry {
                    max_requests: r.max_requests,
                    window_seconds: r.window_seconds,
                }),
            self.filter
                .rate_limit
                .global
                .as_ref()
                .map(|r| RateLimitEntry {
                    max_requests: r.max_requests,
                    window_seconds: r.window_seconds,
                }),
        );
        if rate_limiter.is_active() {
            pipeline.push(Box::new(rate_limiter));
        }

        let keyword = KeywordBlockFilter::new(
            self.filter.keyword.block_keywords.clone(),
            &self.filter.keyword.block_regex,
        );
        if keyword.is_active() {
            pipeline.push(Box::new(keyword));
        }

        if self.filter.content.max_length > 0 {
            pipeline.push(Box::new(ContentLengthFilter::new(
                self.filter.content.max_length,
            )));
        }

        pipeline
    }
}

// ---------------------------------------------------------------------------
// Sub-configs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct QqServerConfig {
    pub bind_address: String,
    pub access_token: String,
    pub heartbeat_interval: u64,
}

impl Default for QqServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:3131".into(),
            access_token: String::new(),
            heartbeat_interval: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct QqTriggerConfig {
    /// Auto-reply to private messages.
    pub dm_auto_reply: bool,
    /// Reply to group messages only when @-mentioned.
    pub group_at_reply: bool,
}

impl Default for QqTriggerConfig {
    fn default() -> Self {
        Self {
            dm_auto_reply: true,
            group_at_reply: true,
        }
    }
}

/// 接收文件的行为配置。
///
/// 群文件上传（`group_upload` 通知）与私聊文件（message 里的 `file` 段）
/// 在 NapCat 侧可各自独立开关；下载失败/超限时仍会把条目送达 agent
/// （`path = null` + `error` 说明），以免用户发了个文件却「石沉大海」。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct QqFilesConfig {
    /// 接收文件的保存目录；空 = 默认数据目录
    /// （`~/.local/share/echo-agent-core/downloads`，HOME 不可用时为
    /// 工作目录下的 `downloads`）。
    pub dir: String,
    /// 单文件大小上限（MB）。超过则跳过下载（条目仍送达，带原因）。
    pub max_mb: u64,
    /// 是否接收群文件上传通知。
    pub accept_group_upload: bool,
    /// 是否接收私聊文件。
    pub accept_private_file: bool,
}

impl Default for QqFilesConfig {
    fn default() -> Self {
        Self {
            dir: String::new(),
            max_mb: 100,
            accept_group_upload: true,
            accept_private_file: true,
        }
    }
}

impl QqFilesConfig {
    /// 单文件大小上限（字节）；`max_mb` 为 0 时回退默认 100MB。
    pub fn max_bytes(&self) -> u64 {
        let mb = if self.max_mb == 0 { 100 } else { self.max_mb };
        mb.saturating_mul(1024 * 1024)
    }
}

// ---------------------------------------------------------------------------
// Filter config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct QqFilterConfig {
    pub allowlist: AllowlistConfig,
    pub denylist: DenylistConfig,
    pub rate_limit: RateLimitConfig,
    pub keyword: KeywordConfig,
    pub content: ContentConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct AllowlistConfig {
    pub user_ids: Vec<i64>,
    pub group_ids: Vec<i64>,
    /// 群成员放行：`true` 时**群消息只校验 `group_ids`**——白名单群内
    /// 全体成员可交互，`user_ids` 此时仅约束私聊；`false`（默认）时群消息
    /// 需同时命中 `user_ids` 与 `group_ids`（“群里只有名单用户能触发”）。
    /// 启动期开关（修改配置文件 + 重启生效）。
    pub group_members_open: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct DenylistConfig {
    pub user_ids: Vec<i64>,
    pub group_ids: Vec<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct RateLimitConfig {
    pub per_user: Option<RateLimitEntryConfig>,
    pub per_group: Option<RateLimitEntryConfig>,
    pub global: Option<RateLimitEntryConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitEntryConfig {
    pub max_requests: u32,
    pub window_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct KeywordConfig {
    pub block_keywords: Vec<String>,
    pub block_regex: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct ContentConfig {
    pub max_length: usize,
}

/// Gate mode — persisted across restarts.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct QqGateConfig {
    /// "none" | "allowlist" | "denylist"
    pub mode: String,
}

impl Default for QqGateConfig {
    fn default() -> Self {
        Self {
            mode: "none".into(),
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_config_defaults_to_none() {
        let cfg = QqGateConfig::default();
        assert_eq!(cfg.mode, "none");
    }

    #[test]
    fn gate_config_deserializes_from_toml() {
        let toml = r#"
[adapters.qq.gate]
mode = "allowlist"
"#;
        #[derive(serde::Deserialize)]
        struct Wrap {
            #[serde(rename = "adapters")]
            a: AdaptersWrap,
        }
        #[derive(serde::Deserialize)]
        struct AdaptersWrap {
            qq: QqAdapterConfig,
        }
        let w: Wrap = toml::from_str(toml).unwrap();
        assert_eq!(w.a.qq.gate.mode, "allowlist");
    }

    #[test]
    fn allowlist_config_defaults_empty() {
        let cfg = AllowlistConfig::default();
        assert!(cfg.user_ids.is_empty());
        assert!(cfg.group_ids.is_empty());
    }

    #[test]
    fn filter_pipeline_empty_by_default() {
        let cfg = QqAdapterConfig::default();
        let pipeline = cfg.build_filter_pipeline();
        assert_eq!(pipeline.len(), 0); // No filters active when empty defaults
    }

    #[test]
    fn filter_pipeline_includes_admin_bypass() {
        let cfg = QqAdapterConfig {
            owner_qq: 12345,
            ..Default::default()
        };
        let pipeline = cfg.build_filter_pipeline();
        assert!(!pipeline.is_empty());
    }

    #[test]
    fn filter_pipeline_includes_allowlist_when_populated() {
        let mut cfg = QqAdapterConfig::default();
        cfg.filter.allowlist.user_ids = vec![111, 222];
        let pipeline = cfg.build_filter_pipeline();
        assert!(!pipeline.is_empty());
    }

    #[test]
    fn gated_pipeline_none_mode_has_no_lists() {
        let mut cfg = QqAdapterConfig::default();
        cfg.filter.allowlist.user_ids = vec![111];
        cfg.filter.denylist.user_ids = vec![222];
        // none: neither allowlist nor denylist is active.
        let pipeline = cfg.build_filter_pipeline_gated("none");
        assert_eq!(pipeline.len(), 0, "no filters for gate mode none");
    }

    #[test]
    fn gated_pipeline_allowlist_excludes_denylist() {
        let mut cfg = QqAdapterConfig::default();
        cfg.filter.allowlist.user_ids = vec![111];
        cfg.filter.denylist.user_ids = vec![222];
        let pipeline = cfg.build_filter_pipeline_gated("allowlist");
        // Allowlist active; denylist must NOT be in the pipeline.
        assert!(!pipeline.is_empty());
        let names = pipeline.filter_names();
        assert!(names.iter().any(|n| n.contains("allowlist")));
        assert!(
            !names.iter().any(|n| n.contains("denylist")),
            "denylist excluded in allowlist mode"
        );
    }

    #[test]
    fn gated_pipeline_denylist_excludes_allowlist() {
        let mut cfg = QqAdapterConfig::default();
        cfg.filter.allowlist.user_ids = vec![111];
        cfg.filter.denylist.user_ids = vec![222];
        let pipeline = cfg.build_filter_pipeline_gated("denylist");
        let names = pipeline.filter_names();
        assert!(names.iter().any(|n| n.contains("denylist")));
        assert!(
            !names.iter().any(|n| n.contains("allowlist")),
            "allowlist excluded in denylist mode"
        );
    }

    #[test]
    fn gated_pipeline_keeps_shared_filters() {
        let mut cfg = QqAdapterConfig::default();
        cfg.filter.content.max_length = 500;
        cfg.owner_qq = 12345;
        let pipeline = cfg.build_filter_pipeline_gated("none");
        // Admin bypass + content length still apply.
        assert!(pipeline.len() >= 2, "shared filters must survive gating");
    }

    #[test]
    fn gated_pipeline_unknown_mode_falls_back_to_none() {
        let mut cfg = QqAdapterConfig::default();
        cfg.filter.allowlist.user_ids = vec![111];
        let pipeline = cfg.build_filter_pipeline_gated("bogus-mode");
        assert_eq!(pipeline.len(), 0);
    }

    /// 端到端契约（群消息白名单口径）：allowlist 模式下 user_ids 与
    /// group_ids **都非空**时，群消息必须同时命中两者——「群里只有白名单
    /// 用户的消息能进来」。用消息实际穿过 gated 管线（include admin bypass
    /// 与共享过滤器）验证，而不仅是单过滤器单测。
    #[tokio::test]
    async fn allowlist_pipeline_limits_group_messages_to_whitelisted_users() {
        let mut cfg = QqAdapterConfig::default();
        cfg.gate.mode = "allowlist".into();
        cfg.filter.allowlist.user_ids = vec![1828980067];
        cfg.filter.allowlist.group_ids = vec![1094762376, 1091798103];
        let pipeline = cfg.build_filter_pipeline_gated("allowlist");

        let msg = |user: &str, group: Option<&str>| echo_adapter::types::IncomingMessage {
            adapter_name: "qq".into(),
            platform: "qq".into(),
            user_id: user.into(),
            user_name: "tester".into(),
            channel: match group {
                Some(gid) => echo_adapter::types::ChannelType::Group {
                    group_id: gid.into(),
                },
                None => echo_adapter::types::ChannelType::Direct,
            },
            group_name: None,
            content: "hi".into(),
            timestamp: 0,
            at_me: true,
            metadata: serde_json::Value::Null,
            images: vec![],
            files: vec![],
        };

        // 白名单用户 + 白名单群 → 放行
        let (ok, _) = pipeline
            .should_accept(&msg("1828980067", Some("1094762376")))
            .await;
        assert!(ok, "whitelisted user in whitelisted group must pass");
        let (ok, _) = pipeline
            .should_accept(&msg("1828980067", Some("1091798103")))
            .await;
        assert!(ok, "whitelisted user in second whitelisted group must pass");

        // 非白名单用户 + 白名单群 → 拦截（关键：群成员身份 ≠ 用户白名单）
        let (ok, _) = pipeline
            .should_accept(&msg("1945079185", Some("1091798103")))
            .await;
        assert!(
            !ok,
            "non-whitelisted user in whitelisted group must be blocked"
        );
        let (ok, _) = pipeline
            .should_accept(&msg("1945079185", Some("1094762376")))
            .await;
        assert!(
            !ok,
            "non-whitelisted user in whitelisted group must be blocked"
        );

        // 白名单用户 + 非白名单群 → 拦截
        let (ok, _) = pipeline
            .should_accept(&msg("1828980067", Some("999999999")))
            .await;
        assert!(
            !ok,
            "whitelisted user outside whitelisted groups must be blocked"
        );

        // DM：不受群维度约束；用户必须在白名单
        let (ok, _) = pipeline.should_accept(&msg("1828980067", None)).await;
        assert!(ok, "whitelisted user DM must pass");
        let (ok, _) = pipeline.should_accept(&msg("1945079185", None)).await;
        assert!(!ok, "non-whitelisted user DM must be blocked");
    }

    /// 群成员放行开关的端到端口径（实际部署 ID）：`group_members_open =
    /// true` 时白名单群内任何成员可触发（不再要求 sender 在 `user_ids` 中），
    /// 私聊仍只有 `user_ids` 中的用户可触发，群名单之外照常拦截。
    #[tokio::test]
    async fn group_members_open_pipeline_admits_any_member_in_whitelisted_groups() {
        let mut cfg = QqAdapterConfig::default();
        cfg.gate.mode = "allowlist".into();
        cfg.filter.allowlist.user_ids = vec![1828980067];
        cfg.filter.allowlist.group_ids = vec![1094762376, 1091798103];
        cfg.filter.allowlist.group_members_open = true;
        let pipeline = cfg.build_filter_pipeline_gated("allowlist");

        let msg = |user: &str, group: Option<&str>| echo_adapter::types::IncomingMessage {
            adapter_name: "qq".into(),
            platform: "qq".into(),
            user_id: user.into(),
            user_name: "tester".into(),
            channel: match group {
                Some(gid) => echo_adapter::types::ChannelType::Group {
                    group_id: gid.into(),
                },
                None => echo_adapter::types::ChannelType::Direct,
            },
            group_name: None,
            content: "hi".into(),
            timestamp: 0,
            at_me: true,
            metadata: serde_json::Value::Null,
            images: vec![],
            files: vec![],
        };

        // 非白名单用户（此前被拦的真实用户）在白名单群 → 放行
        let (ok, _) = pipeline
            .should_accept(&msg("1945079185", Some("1091798103")))
            .await;
        assert!(
            ok,
            "any member in a whitelisted group must pass when group_members_open"
        );
        let (ok, _) = pipeline
            .should_accept(&msg("1945079185", Some("1094762376")))
            .await;
        assert!(ok, "same in the second whitelisted group");

        // 非白名单群 → 仍拦截（群名单照常生效）
        let (ok, _) = pipeline
            .should_accept(&msg("1945079185", Some("999999999")))
            .await;
        assert!(!ok, "groups outside the whitelist stay blocked");

        // 私聊仍受用户名单约束：非白名单用户拦截、白名单用户放行
        let (ok, _) = pipeline.should_accept(&msg("1945079185", None)).await;
        assert!(!ok, "DM from a non-whitelisted user stays blocked");
        let (ok, _) = pipeline.should_accept(&msg("1828980067", None)).await;
        assert!(ok, "whitelisted user DM still passes");
    }

    /// 两条默认路径必须一致：`Default` impl（程序化构造 / 无 `[adapters.qq]`
    /// 段的配置）与 serde 字段默认（TOML 缺字段）。`napcat_auto_stop` 曾因
    /// `Default` impl 残留 `default_true()` 而与 serde 的 false 矛盾。
    #[test]
    fn default_matches_serde_field_defaults() {
        let programmatic = QqAdapterConfig::default();
        assert!(
            !programmatic.napcat_auto_stop,
            "Default::default() must not auto-stop NapCat"
        );

        let from_toml: QqAdapterConfig = toml::from_str("enabled = true").expect("parse");
        assert!(!from_toml.napcat_auto_stop, "serde default must stay false");

        assert_eq!(programmatic.napcat_auto_stop, from_toml.napcat_auto_stop);
    }
}
