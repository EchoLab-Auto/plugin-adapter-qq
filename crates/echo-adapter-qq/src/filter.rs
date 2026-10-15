//! Message filter pipeline.
//!
//! Filters gate inbound messages before they reach the agent, providing
//! allowlist/denylist, rate limiting, keyword blocking, and content validation.

use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::types::IncomingMessage;

// ---------------------------------------------------------------------------
// Filter trait
// ---------------------------------------------------------------------------

/// The result of a single filter check.
#[derive(Debug, Clone)]
pub enum FilterResult {
    /// The message passes this filter.
    Allow,
    /// The message passes this filter AND skips all remaining filters.
    AllowAndSkip,
    /// The message is silently dropped.
    Block,
    /// The message is dropped and the caller should send this reply.
    BlockWithMessage(String),
}

/// A single filter in the pipeline.
#[async_trait]
pub trait MessageFilter: Send + Sync {
    /// Unique filter name for logging.
    fn name(&self) -> &str;

    /// Check whether the incoming message should be allowed.
    async fn check(&self, msg: &IncomingMessage) -> FilterResult;
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// An ordered pipeline of filters. Messages pass through in insertion order;
/// the first non-`Allow` result short-circuits.
pub struct FilterPipeline {
    filters: Vec<Box<dyn MessageFilter>>,
}

impl std::fmt::Debug for FilterPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterPipeline")
            .field("len", &self.filters.len())
            .finish()
    }
}

impl Default for FilterPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterPipeline {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Append a filter to the end of the pipeline.
    pub fn push(&mut self, filter: Box<dyn MessageFilter>) {
        self.filters.push(filter);
    }

    /// Number of filters in the pipeline.
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Whether the pipeline is empty.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Names of all filters, in order (for diagnostics and tests).
    pub fn filter_names(&self) -> Vec<String> {
        self.filters.iter().map(|f| f.name().to_string()).collect()
    }

    /// Run all filters in order.
    ///
    /// Returns `(accepted, optional_reply)`.
    /// `accepted == true` means the message should be processed.
    pub async fn should_accept(&self, msg: &IncomingMessage) -> (bool, Option<String>) {
        for filter in &self.filters {
            match filter.check(msg).await {
                FilterResult::Allow => continue,
                FilterResult::AllowAndSkip => return (true, None),
                FilterResult::Block => {
                    tracing::debug!(
                        filter = %filter.name(),
                        user = %msg.user_id,
                        adapter = %msg.adapter_name,
                        "message blocked by filter",
                    );
                    return (false, None);
                }
                FilterResult::BlockWithMessage(reply) => {
                    tracing::debug!(
                        filter = %filter.name(),
                        user = %msg.user_id,
                        adapter = %msg.adapter_name,
                        "message blocked with reply",
                    );
                    return (false, Some(reply));
                }
            }
        }
        (true, None)
    }
}

// ---------------------------------------------------------------------------
// AllowlistFilter
// ---------------------------------------------------------------------------

/// Only accepts messages from listed users/groups.
/// When both `user_ids` and `group_ids` are empty, all messages pass.
pub struct AllowlistFilter {
    pub user_ids: std::collections::HashSet<String>,
    pub group_ids: std::collections::HashSet<String>,
    /// 群成员放行：`true` 时群消息只校验 `group_ids`（白名单群内全体成员
    /// 可交互），`user_ids` 仅约束私聊；`false`（默认）时群消息需同时命中
    /// `user_ids` 与 `group_ids`。见 `AllowlistConfig::group_members_open`。
    pub group_members_open: bool,
}

impl AllowlistFilter {
    pub fn new(user_ids: Vec<String>, group_ids: Vec<String>) -> Self {
        Self {
            user_ids: user_ids.into_iter().collect(),
            group_ids: group_ids.into_iter().collect(),
            group_members_open: false,
        }
    }

    /// 群成员放行开关（builder 形态，语义见字段说明与
    /// `AllowlistConfig::group_members_open`）。
    pub fn with_group_members_open(mut self, open: bool) -> Self {
        self.group_members_open = open;
        self
    }

    /// Whether this filter is active (has entries).
    pub fn is_active(&self) -> bool {
        !self.user_ids.is_empty() || !self.group_ids.is_empty()
    }
}

#[async_trait]
impl MessageFilter for AllowlistFilter {
    fn name(&self) -> &str {
        "allowlist"
    }

    async fn check(&self, msg: &IncomingMessage) -> FilterResult {
        if !self.is_active() {
            return FilterResult::Allow;
        }
        // 群成员放行开关：白名单群内不再校验用户名单（群内全体成员可交互）；
        // 私聊仍受 `user_ids` 约束。
        let in_group = msg.channel.group_id().is_some();
        let user_ok = (self.group_members_open && in_group)
            || self.user_ids.is_empty()
            || self.user_ids.contains(&msg.user_id);
        let group_ok = self.group_ids.is_empty()
            || match msg.channel.group_id() {
                Some(gid) => self.group_ids.contains(gid),
                None => true, // DMs always pass group filter
            };
        if user_ok && group_ok {
            FilterResult::Allow
        } else {
            FilterResult::Block
        }
    }
}

// ---------------------------------------------------------------------------
// DenylistFilter
// ---------------------------------------------------------------------------

/// Blocks messages from listed users/groups.
pub struct DenylistFilter {
    pub user_ids: std::collections::HashSet<String>,
    pub group_ids: std::collections::HashSet<String>,
}

impl DenylistFilter {
    pub fn new(user_ids: Vec<String>, group_ids: Vec<String>) -> Self {
        Self {
            user_ids: user_ids.into_iter().collect(),
            group_ids: group_ids.into_iter().collect(),
        }
    }

    pub fn is_active(&self) -> bool {
        !self.user_ids.is_empty() || !self.group_ids.is_empty()
    }
}

#[async_trait]
impl MessageFilter for DenylistFilter {
    fn name(&self) -> &str {
        "denylist"
    }

    async fn check(&self, msg: &IncomingMessage) -> FilterResult {
        if !self.is_active() {
            return FilterResult::Allow;
        }
        let user_blocked = !self.user_ids.is_empty() && self.user_ids.contains(&msg.user_id);
        let group_blocked = match msg.channel.group_id() {
            Some(gid) => !self.group_ids.is_empty() && self.group_ids.contains(gid),
            None => false,
        };
        if user_blocked || group_blocked {
            FilterResult::Block
        } else {
            FilterResult::Allow
        }
    }
}

// ---------------------------------------------------------------------------
// RateLimitFilter
// ---------------------------------------------------------------------------

/// Sliding-window rate limiter.
///
/// Supports three independent buckets: per-user, per-group, and global.
pub struct RateLimitFilter {
    per_user: Option<RateLimitEntry>,
    per_group: Option<RateLimitEntry>,
    global: Option<RateLimitEntry>,
    state: Mutex<RateLimitState>,
}

#[derive(Debug, Clone)]
pub struct RateLimitEntry {
    pub max_requests: u32,
    pub window_seconds: u64,
}

#[derive(Debug, Clone, Default)]
struct RateLimitState {
    users: HashMap<String, Vec<Instant>>,
    groups: HashMap<String, Vec<Instant>>,
    global: Vec<Instant>,
}

impl RateLimitFilter {
    pub fn new(
        per_user: Option<RateLimitEntry>,
        per_group: Option<RateLimitEntry>,
        global: Option<RateLimitEntry>,
    ) -> Self {
        Self {
            per_user,
            per_group,
            global,
            state: Mutex::new(RateLimitState::default()),
        }
    }

    /// Whether any limit is configured.
    pub fn is_active(&self) -> bool {
        self.per_user.is_some() || self.per_group.is_some() || self.global.is_some()
    }

    /// Check a single bucket. Returns `true` if the request should be allowed.
    fn check_bucket(now: Instant, window: &[Instant], entry: &RateLimitEntry) -> bool {
        let cutoff = now - std::time::Duration::from_secs(entry.window_seconds);
        let recent: Vec<&Instant> = window.iter().filter(|t| **t >= cutoff).collect();
        (recent.len() as u32) < entry.max_requests
    }

    /// Record a request in a single bucket, pruning expired entries.
    fn record_bucket(bucket: &mut Vec<Instant>, now: Instant, entry: &RateLimitEntry) {
        let cutoff = now - std::time::Duration::from_secs(entry.window_seconds);
        bucket.retain(|t| *t >= cutoff);
        bucket.push(now);
    }
}

#[async_trait]
impl MessageFilter for RateLimitFilter {
    fn name(&self) -> &str {
        "rate_limit"
    }

    async fn check(&self, msg: &IncomingMessage) -> FilterResult {
        if !self.is_active() {
            return FilterResult::Allow;
        }

        let now = Instant::now();
        let mut state = self.state.lock().await;

        // Periodic cleanup of empty buckets (every ~200th call).
        {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static CLEANUP: AtomicUsize = AtomicUsize::new(0);
            if CLEANUP.fetch_add(1, Ordering::Relaxed).is_multiple_of(200) {
                state.users.retain(|_, v| !v.is_empty());
                state.groups.retain(|_, v| !v.is_empty());
            }
        }

        // Global check first (cheapest).
        if let Some(ref entry) = self.global {
            if !Self::check_bucket(now, &state.global, entry) {
                return FilterResult::Block;
            }
        }

        // Per-user check.
        if let Some(ref entry) = self.per_user {
            let bucket = state.users.entry(msg.user_id.clone()).or_default();
            if !Self::check_bucket(now, bucket, entry) {
                return FilterResult::Block;
            }
        }

        // Per-group check.
        if let Some(ref entry) = self.per_group {
            if let Some(gid) = msg.channel.group_id() {
                let bucket = state.groups.entry(gid.to_string()).or_default();
                if !Self::check_bucket(now, bucket, entry) {
                    return FilterResult::Block;
                }
            }
        }

        // All checks passed — record the request.
        if let Some(ref entry) = self.global {
            Self::record_bucket(&mut state.global, now, entry);
        }
        if let Some(ref entry) = self.per_user {
            let bucket = state.users.entry(msg.user_id.clone()).or_default();
            Self::record_bucket(bucket, now, entry);
        }
        if let Some(ref entry) = self.per_group {
            if let Some(gid) = msg.channel.group_id() {
                let bucket = state.groups.entry(gid.to_string()).or_default();
                Self::record_bucket(bucket, now, entry);
            }
        }

        FilterResult::Allow
    }
}

// ---------------------------------------------------------------------------
// KeywordBlockFilter
// ---------------------------------------------------------------------------

/// Blocks messages containing any of a list of keywords or matching a regex.
pub struct KeywordBlockFilter {
    pub keywords: Vec<String>,
    lowered_keywords: Vec<String>,
    pub regex: Option<regex::Regex>,
}

impl KeywordBlockFilter {
    pub fn new(keywords: Vec<String>, regex_pattern: &str) -> Self {
        let lowered_keywords: Vec<String> = keywords.iter().map(|k| k.to_lowercase()).collect();
        let regex = if regex_pattern.is_empty() {
            None
        } else {
            regex::Regex::new(regex_pattern).ok()
        };
        Self {
            keywords,
            lowered_keywords,
            regex,
        }
    }

    pub fn is_active(&self) -> bool {
        !self.keywords.is_empty() || self.regex.is_some()
    }
}

#[async_trait]
impl MessageFilter for KeywordBlockFilter {
    fn name(&self) -> &str {
        "keyword_block"
    }

    async fn check(&self, msg: &IncomingMessage) -> FilterResult {
        if !self.is_active() {
            return FilterResult::Allow;
        }

        let lower = msg.content.to_lowercase();

        // Check keywords (case-insensitive contains, using pre-lowered keywords).
        for kw in &self.lowered_keywords {
            if !kw.is_empty() && lower.contains(kw.as_str()) {
                return FilterResult::Block;
            }
        }

        // Check regex.
        if let Some(ref re) = self.regex {
            if re.is_match(&msg.content) {
                return FilterResult::Block;
            }
        }

        FilterResult::Allow
    }
}

// ---------------------------------------------------------------------------
// ContentLengthFilter
// ---------------------------------------------------------------------------

/// Blocks messages exceeding a maximum character length.
pub struct ContentLengthFilter {
    pub max_length: usize,
}

impl ContentLengthFilter {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    pub fn is_active(&self) -> bool {
        self.max_length > 0
    }
}

#[async_trait]
impl MessageFilter for ContentLengthFilter {
    fn name(&self) -> &str {
        "content_length"
    }

    async fn check(&self, msg: &IncomingMessage) -> FilterResult {
        if !self.is_active() || msg.content.chars().count() <= self.max_length {
            FilterResult::Allow
        } else {
            FilterResult::Block
        }
    }
}

// ---------------------------------------------------------------------------
// AdminBypassFilter
// ---------------------------------------------------------------------------

/// Allows admin users (by user_id) to bypass downstream filters.
///
/// Must be placed **first** in the pipeline: when it sees an admin message,
/// it short-circuits with `Allow`.
pub struct AdminBypassFilter {
    pub admin_user_ids: Vec<String>,
}

impl AdminBypassFilter {
    pub fn new(admin_user_ids: Vec<String>) -> Self {
        Self { admin_user_ids }
    }

    pub fn is_active(&self) -> bool {
        !self.admin_user_ids.is_empty()
    }
}

#[async_trait]
impl MessageFilter for AdminBypassFilter {
    fn name(&self) -> &str {
        "admin_bypass"
    }

    async fn check(&self, msg: &IncomingMessage) -> FilterResult {
        if !self.is_active() {
            return FilterResult::Allow;
        }
        if self.admin_user_ids.contains(&msg.user_id) {
            return FilterResult::AllowAndSkip;
        }
        FilterResult::Allow // Not an admin — let downstream filters decide.
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChannelType;

    fn dm(user_id: &str, content: &str) -> IncomingMessage {
        IncomingMessage {
            adapter_name: "test".into(),
            platform: "test".into(),
            user_id: user_id.into(),
            user_name: "tester".into(),
            channel: ChannelType::Direct,
            group_name: None,
            content: content.into(),
            timestamp: 0,
            at_me: true,
            metadata: serde_json::Value::Null,
            images: vec![],
            files: vec![],
        }
    }

    fn group_msg(user_id: &str, group_id: &str, content: &str) -> IncomingMessage {
        IncomingMessage {
            adapter_name: "test".into(),
            platform: "test".into(),
            user_id: user_id.into(),
            user_name: "tester".into(),
            channel: ChannelType::Group {
                group_id: group_id.into(),
            },
            group_name: Some("Test Group".into()),
            content: content.into(),
            timestamp: 0,
            at_me: true,
            metadata: serde_json::Value::Null,
            images: vec![],
            files: vec![],
        }
    }

    // ---- AllowlistFilter ----

    #[tokio::test]
    async fn allowlist_allows_when_empty() {
        let f = AllowlistFilter::new(vec![], vec![]);
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
    }

    #[tokio::test]
    async fn allowlist_blocks_unknown_user() {
        let f = AllowlistFilter::new(vec!["u1".into()], vec![]);
        assert!(matches!(
            f.check(&dm("u2", "hi")).await,
            FilterResult::Block
        ));
    }

    #[tokio::test]
    async fn allowlist_allows_known_user() {
        let f = AllowlistFilter::new(vec!["u1".into()], vec![]);
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
    }

    #[tokio::test]
    async fn allowlist_dm_passes_group_filter() {
        let f = AllowlistFilter::new(vec![], vec!["g1".into()]);
        // DMs always pass the group allowlist
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
    }

    #[tokio::test]
    async fn allowlist_blocks_unknown_group() {
        let f = AllowlistFilter::new(vec![], vec!["g1".into()]);
        assert!(matches!(
            f.check(&group_msg("u1", "g2", "hi")).await,
            FilterResult::Block
        ));
    }

    /// 组合语义契约（群消息的白名单口径）：`user_ids` 与 `group_ids` 都非空时，
    /// 群消息必须**同时**命中两者——即「群里只有白名单用户的消息能进来」，
    /// 名单内群里的其他成员 @ 机器人也会被拦截；名单用户在不属于名单的群里
    /// 同样被拦截。DM 不受 group 维度影响（但受 user 维度约束）。
    #[tokio::test]
    async fn allowlist_group_message_requires_both_user_and_group() {
        let f = AllowlistFilter::new(vec!["u1".into()], vec!["g1".into()]);

        // 名单用户 + 名单群 → 放行
        assert!(matches!(
            f.check(&group_msg("u1", "g1", "hi")).await,
            FilterResult::Allow
        ));
        // 非名单用户 + 名单群 → 拦截（关键：群成员身份不能替代用户名单）
        assert!(matches!(
            f.check(&group_msg("u2", "g1", "hi")).await,
            FilterResult::Block
        ));
        // 名单用户 + 非名单群 → 拦截
        assert!(matches!(
            f.check(&group_msg("u1", "g2", "hi")).await,
            FilterResult::Block
        ));
        // 非名单用户 + 非名单群 → 拦截
        assert!(matches!(
            f.check(&group_msg("u2", "g2", "hi")).await,
            FilterResult::Block
        ));
        // DM：不受 group 维度约束；非名单用户 → 拦截
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
        assert!(matches!(
            f.check(&dm("u2", "hi")).await,
            FilterResult::Block
        ));
    }

    /// 群成员放行开关：开启后群消息不再受用户名单约束（白名单群内全体
    /// 成员可交互），私聊仍受 `user_ids` 约束；群名单照常生效。
    #[tokio::test]
    async fn allowlist_group_members_open_skips_user_check_in_groups() {
        let f = AllowlistFilter::new(vec!["u1".into()], vec!["g1".into()])
            .with_group_members_open(true);
        // 非名单用户 + 名单群 → 放行（开关生效）
        assert!(matches!(
            f.check(&group_msg("u2", "g1", "hi")).await,
            FilterResult::Allow
        ));
        // 名单用户 + 名单群 → 照常放行
        assert!(matches!(
            f.check(&group_msg("u1", "g1", "hi")).await,
            FilterResult::Allow
        ));
        // 非名单群 → 仍拦截（群名单照常生效）
        assert!(matches!(
            f.check(&group_msg("u2", "g2", "hi")).await,
            FilterResult::Block
        ));
        // 私聊不受开关影响：非名单用户仍拦截、名单用户放行
        assert!(matches!(
            f.check(&dm("u2", "hi")).await,
            FilterResult::Block
        ));
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
    }

    // ---- DenylistFilter ----

    #[tokio::test]
    async fn denylist_allows_when_empty() {
        let f = DenylistFilter::new(vec![], vec![]);
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
    }

    #[tokio::test]
    async fn denylist_blocks_known_user() {
        let f = DenylistFilter::new(vec!["u1".into()], vec![]);
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Block
        ));
    }

    #[tokio::test]
    async fn denylist_allows_unknown_user() {
        let f = DenylistFilter::new(vec!["u1".into()], vec![]);
        assert!(matches!(
            f.check(&dm("u2", "hi")).await,
            FilterResult::Allow
        ));
    }

    // ---- RateLimitFilter ----

    #[tokio::test]
    async fn rate_limit_allows_first_request() {
        let f = RateLimitFilter::new(
            Some(RateLimitEntry {
                max_requests: 3,
                window_seconds: 60,
            }),
            None,
            None,
        );
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
    }

    #[tokio::test]
    async fn rate_limit_blocks_when_exceeded() {
        let f = RateLimitFilter::new(
            Some(RateLimitEntry {
                max_requests: 2,
                window_seconds: 3600,
            }),
            None,
            None,
        );
        assert!(matches!(f.check(&dm("u1", "a")).await, FilterResult::Allow));
        assert!(matches!(f.check(&dm("u1", "b")).await, FilterResult::Allow));
        assert!(matches!(f.check(&dm("u1", "c")).await, FilterResult::Block));
    }

    // ---- KeywordBlockFilter ----

    #[tokio::test]
    async fn keyword_filter_blocks_match() {
        let f = KeywordBlockFilter::new(vec!["spam".into(), "广告".into()], "");
        assert!(matches!(
            f.check(&dm("u1", "广告信息")).await,
            FilterResult::Block
        ));
        assert!(matches!(
            f.check(&dm("u1", "SPAM here")).await,
            FilterResult::Block
        ));
    }

    #[tokio::test]
    async fn keyword_filter_allows_no_match() {
        let f = KeywordBlockFilter::new(vec!["spam".into()], "");
        assert!(matches!(
            f.check(&dm("u1", "hello")).await,
            FilterResult::Allow
        ));
    }

    #[tokio::test]
    async fn regex_filter_blocks_match() {
        let f = KeywordBlockFilter::new(vec![], r"http[s]?://.*\.ru");
        assert!(matches!(
            f.check(&dm("u1", "visit https://x.ru now")).await,
            FilterResult::Block
        ));
        assert!(matches!(
            f.check(&dm("u1", "visit https://x.com")).await,
            FilterResult::Allow
        ));
    }

    // ---- ContentLengthFilter ----

    #[tokio::test]
    async fn content_length_blocks_too_long() {
        let f = ContentLengthFilter::new(10);
        assert!(matches!(
            f.check(&dm("u1", "short")).await,
            FilterResult::Allow
        ));
        assert!(matches!(
            f.check(&dm("u1", "this is way too long")).await,
            FilterResult::Block
        ));
    }

    #[tokio::test]
    async fn content_length_allows_when_zero() {
        let f = ContentLengthFilter::new(0);
        assert!(matches!(
            f.check(&dm("u1", &"x".repeat(9999))).await,
            FilterResult::Allow
        ));
    }

    // ---- AdminBypassFilter ----

    #[tokio::test]
    async fn admin_bypass_always_allows() {
        let f = AdminBypassFilter::new(vec!["admin1".into()]);
        // Admins get AllowAndSkip (bypass remaining filters).
        assert!(matches!(
            f.check(&dm("admin1", "hi")).await,
            FilterResult::AllowAndSkip
        ));
        // Non-admins still pass through (Allow, not blocked).
        assert!(matches!(
            f.check(&dm("user1", "hi")).await,
            FilterResult::Allow
        ));
    }

    // ---- FilterPipeline ----

    #[tokio::test]
    async fn pipeline_stops_at_first_block() {
        let mut p = FilterPipeline::new();
        p.push(Box::new(DenylistFilter::new(vec!["bad".into()], vec![])));
        p.push(Box::new(KeywordBlockFilter::new(vec!["spam".into()], "")));

        let (accepted, _) = p.should_accept(&dm("bad", "hello")).await;
        assert!(!accepted);
    }

    #[tokio::test]
    async fn pipeline_ordered_allow_then_block() {
        let mut p = FilterPipeline::new();
        p.push(Box::new(AllowlistFilter::new(vec!["good".into()], vec![])));
        p.push(Box::new(DenylistFilter::new(vec!["good".into()], vec![])));

        // "good" passes allowlist but then gets blocked by denylist
        let (accepted, _) = p.should_accept(&dm("good", "hi")).await;
        assert!(!accepted);
    }

    #[tokio::test]
    async fn pipeline_all_allows() {
        let mut p = FilterPipeline::new();
        p.push(Box::new(AllowlistFilter::new(vec!["good".into()], vec![])));
        p.push(Box::new(ContentLengthFilter::new(100)));

        let (accepted, _) = p.should_accept(&dm("good", "hello")).await;
        assert!(accepted);
    }

    #[tokio::test]
    async fn pipeline_block_with_message() {
        use async_trait::async_trait;

        struct ReplyFilter;
        #[async_trait]
        impl MessageFilter for ReplyFilter {
            fn name(&self) -> &str {
                "reply_test"
            }
            async fn check(&self, _msg: &IncomingMessage) -> FilterResult {
                FilterResult::BlockWithMessage("对不起，此功能暂不可用。".into())
            }
        }

        let mut p = FilterPipeline::new();
        p.push(Box::new(ReplyFilter));
        let (accepted, reply) = p.should_accept(&dm("u1", "hi")).await;
        assert!(!accepted);
        assert_eq!(reply, Some("对不起，此功能暂不可用。".into()));
    }

    // ---- Rate limit window / bucket isolation ----

    #[tokio::test]
    async fn rate_limit_window_expires_and_allows_again() {
        let f = RateLimitFilter::new(
            Some(RateLimitEntry {
                max_requests: 2,
                window_seconds: 1,
            }),
            None,
            None,
        );
        assert!(matches!(f.check(&dm("u1", "a")).await, FilterResult::Allow));
        assert!(matches!(f.check(&dm("u1", "b")).await, FilterResult::Allow));
        assert!(matches!(f.check(&dm("u1", "c")).await, FilterResult::Block));
        // Wait for the 1s window to slide past.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        assert!(
            matches!(f.check(&dm("u1", "d")).await, FilterResult::Allow),
            "window expired → request allowed again"
        );
    }

    #[tokio::test]
    async fn rate_limit_buckets_are_per_user() {
        let f = RateLimitFilter::new(
            Some(RateLimitEntry {
                max_requests: 2,
                window_seconds: 3600,
            }),
            None,
            None,
        );
        assert!(matches!(f.check(&dm("u1", "a")).await, FilterResult::Allow));
        assert!(matches!(f.check(&dm("u1", "b")).await, FilterResult::Allow));
        assert!(matches!(f.check(&dm("u1", "c")).await, FilterResult::Block));
        // A different user is unaffected.
        assert!(matches!(f.check(&dm("u2", "x")).await, FilterResult::Allow));
    }

    #[tokio::test]
    async fn rate_limit_group_bucket_isolated_from_dm() {
        let f = RateLimitFilter::new(
            None,
            Some(RateLimitEntry {
                max_requests: 1,
                window_seconds: 3600,
            }),
            None,
        );
        let group_msg = |content: &str| IncomingMessage {
            adapter_name: "qq".into(),
            platform: "qq".into(),
            user_id: "u1".into(),
            user_name: "x".into(),
            channel: ChannelType::Group {
                group_id: "g1".into(),
            },
            group_name: None,
            content: content.into(),
            timestamp: 0,
            at_me: true,
            metadata: serde_json::Value::Null,
            images: vec![],
            files: vec![],
        };
        assert!(matches!(
            f.check(&group_msg("a")).await,
            FilterResult::Allow
        ));
        assert!(matches!(
            f.check(&group_msg("b")).await,
            FilterResult::Block
        ));
        // DM traffic is not limited by the group bucket.
        assert!(matches!(
            f.check(&dm("u1", "hi")).await,
            FilterResult::Allow
        ));
    }

    #[tokio::test]
    async fn rate_limit_global_and_per_user_both_apply() {
        let f = RateLimitFilter::new(
            Some(RateLimitEntry {
                max_requests: 10,
                window_seconds: 3600,
            }),
            None,
            Some(RateLimitEntry {
                max_requests: 1,
                window_seconds: 3600,
            }),
        );
        assert!(matches!(f.check(&dm("u1", "a")).await, FilterResult::Allow));
        // Global bucket exhausted — even another user is blocked.
        assert!(matches!(f.check(&dm("u2", "b")).await, FilterResult::Block));
    }
}
