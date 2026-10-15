//! The main QQ message handler — trigger gating, filter pipeline, and
//! delivery to the agent's inbound hook.
//!
//! Extracted from `QqAdapter::start()` so the handler logic can be tested
//! independently.

use std::sync::Arc;

use async_trait::async_trait;
use echo_adapter::types::{AdapterEvent, ChannelType, IncomingFile, IncomingMessage};
use echo_core::{Event as OneBotEvent, NoticeEvent};
use echo_server::{Context, HandleResult};

use crate::adapter::qq::QqInner;
use crate::adapter::QqAdapter;

/// Primary handler: converts OneBot events into inbound messages, applies
/// trigger gating and the filter pipeline, then routes to the agent bridge.
pub struct QqHandler {
    inner: Arc<QqInner>,
}

impl QqHandler {
    pub(crate) fn new(inner: Arc<QqInner>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl echo_server::Handler for QqHandler {
    fn name(&self) -> &str {
        "qq-handler"
    }

    fn priority(&self) -> i32 {
        1000
    }

    async fn handle(&self, ctx: &Context, event: &OneBotEvent) -> HandleResult {
        // Store context for outbound sends.
        *self.inner.active_context.lock().expect("ctx poisoned") = Some(Arc::new(ctx.clone()));

        // Lazily learn group names: OneBot group events carry only group_id,
        // so query the name once and cache it for later messages. Group file
        // uploads (notice) are covered too — their payload only has the id.
        let event_group_id = event
            .as_message()
            .and_then(|msg| msg.group_id())
            .or(match event {
                OneBotEvent::Notice {
                    inner: NoticeEvent::GroupUpload { group_id, .. },
                } => Some(*group_id),
                _ => None,
            });
        if let Some(gid) = event_group_id {
            if !self.inner.group_names.contains_key(&gid) {
                let group_names = self.inner.group_names.clone();
                let ctx = Arc::new(ctx.clone());
                tokio::spawn(async move {
                    let request = echo_core::action::actions::get_group_info(gid);
                    match ctx.send_api(request).await {
                        Ok(resp) => {
                            if let Some(name) = resp
                                .data
                                .get("group_name")
                                .and_then(|n| n.as_str())
                                .filter(|n| !n.trim().is_empty())
                            {
                                group_names.insert(gid, name.trim().to_string());
                            }
                        }
                        Err(error) => {
                            tracing::debug!(%gid, %error, "group name lookup failed");
                        }
                    }
                });
            }
        }

        // ── 群文件上传通知 ──
        // NapCat 对「群成员上传文件」双上报：`group_upload` 通知 + 一条
        // 带 file 段的消息。消息侧已在 convert_message 跳过 file 段
        // （群聊），这里独占处理，避免重复下载与重复送达。
        if let Some(mut file_msg) = self.group_upload_message(event) {
            if !self.inner.config.files.accept_group_upload {
                tracing::debug!(group = %file_msg.channel, "QQ group file dropped: accept_group_upload disabled");
                return HandleResult::Pass;
            }
            // 群文件没有 @ 语义：不受 group_at_reply 控制，但同样走过滤管线
            // （白/黑名单、限流等与普通消息一致）。
            let pipeline_opt = self.inner.filter.lock().expect("filter poisoned").clone();
            if let Some(pipeline) = pipeline_opt {
                let (accepted, _) = pipeline.should_accept(&file_msg).await;
                if !accepted {
                    tracing::info!(user = %file_msg.user_id, channel = %file_msg.channel, "QQ group file blocked by filter pipeline");
                    return HandleResult::Handled;
                }
            }
            self.resolve_pending_files(&mut file_msg).await;
            tracing::info!(
                user = %file_msg.user_id,
                channel = %file_msg.channel,
                files = file_msg.files.len(),
                "QQ group file upload accepted, routing to agent"
            );
            self.deliver(file_msg).await;
            return HandleResult::Handled;
        }

        let Some(msg) = QqAdapter::convert_message_for(
            &self.inner.instance_name,
            event,
            &self.inner.group_names,
        ) else {
            return HandleResult::Pass;
        };
        // convert_message returns Some only for message events, so this
        // reference is always valid — no unwrap required.
        let message_event = event.as_message();

        // Trigger gating.
        let trigger = &self.inner.config.trigger;
        if msg.channel.is_group() {
            if !trigger.group_at_reply || !msg.at_me {
                tracing::debug!(user = %msg.user_id, "QQ message dropped by trigger gating (not @me or group reply disabled)");
                return HandleResult::Pass;
            }
        } else if !trigger.dm_auto_reply {
            tracing::debug!(user = %msg.user_id, "QQ DM dropped: dm_auto_reply disabled");
            return HandleResult::Pass;
        }

        // Filter pipeline — clone Arc out of the lock so the guard is
        // dropped before .await.
        let pipeline_opt = self.inner.filter.lock().expect("filter poisoned").clone();
        if let Some(pipeline) = pipeline_opt {
            let (accepted, block_reply) = pipeline.should_accept(&msg).await;
            if !accepted {
                tracing::info!(user = %msg.user_id, channel = %msg.channel, "QQ message blocked by filter pipeline");
                if let (Some(reply), Some(message)) = (block_reply, message_event) {
                    let _ = ctx.reply_to(message, reply).await;
                }
                return HandleResult::Handled;
            }
        }

        let mut msg = msg;
        // 私聊文件接收关闭：剥掉文件段——纯文件消息整体丢弃（与群文件
        // 接收关闭时丢弃通知的语义一致），文本/图片照常送达。
        if !self.inner.config.files.accept_private_file && !msg.files.is_empty() {
            tracing::debug!(user = %msg.user_id, "QQ private file dropped: accept_private_file disabled");
            msg.files.clear();
            if let Some(obj) = msg.metadata.as_object_mut() {
                obj.remove("pending_files");
                if obj.is_empty() {
                    msg.metadata = serde_json::Value::Null;
                }
            }
            if msg.content.trim().is_empty() && msg.images.is_empty() {
                return HandleResult::Pass;
            }
        }
        // QQ CDN 图片链接的 rkey 会过期：下载并**落盘为媒体文件**，链路上只留
        // `/media/<id>` 引用（2026-09-24 改造：此前内嵌 data URI，单张数 MB
        // 的图片会把时间线/会话文件拖到几十 MB）。必须在触发门控与过滤管道
        // **通过之后**才下载（2026-09-29 下移）：此前紧跟 convert_message，
        // 被门控（群聊非 @我 / dm_auto_reply=false）或过滤拦截的消息也会白存
        // 图片，媒体库积累无主文件。模型侧在发往 LLM 前由
        // `media_store::inline_media_refs_in_messages` 还原。
        msg = persist_remote_images(msg).await;
        // 私聊文件（消息 file 段）：在过滤通过后下载，避免被拦截的消息
        // 白白拉取大文件。
        self.resolve_pending_files(&mut msg).await;

        tracing::info!(
            user = %msg.user_id,
            channel = %msg.channel,
            "QQ message accepted, routing to agent"
        );

        self.deliver(msg).await;
        HandleResult::Handled
    }
}

impl QqHandler {
    /// 把 `group_upload` 通知转换成内部 IncomingMessage（含待下载登记）。
    fn group_upload_message(&self, event: &OneBotEvent) -> Option<IncomingMessage> {
        let OneBotEvent::Notice {
            inner:
                NoticeEvent::GroupUpload {
                    time,
                    group_id,
                    user_id,
                    file,
                    ..
                },
        } = event
        else {
            return None;
        };
        let name = if file.name.trim().is_empty() {
            "未命名文件".to_string()
        } else {
            file.name.clone()
        };
        let size = file.size.max(0) as u64;
        let pending = serde_json::json!([{
            "name": name,
            "size": size,
            "file_id": file.id,
            "url": serde_json::Value::Null,
            "group_id": group_id,
        }]);
        Some(IncomingMessage {
            adapter_name: self.inner.instance_name.clone(),
            platform: "qq".into(),
            user_id: user_id.to_string(),
            user_name: String::new(),
            channel: ChannelType::Group {
                group_id: group_id.to_string(),
            },
            group_name: self.inner.group_names.get(group_id).map(|n| n.clone()),
            content: String::new(),
            timestamp: *time,
            at_me: false,
            metadata: serde_json::json!({ "pending_files": pending }),
            images: vec![],
            files: vec![IncomingFile {
                name,
                path: None,
                size,
                error: None,
            }],
        })
    }

    /// 通过 OneBot 接口换文件下载直链：群文件走 `get_group_file_url`，
    /// 私聊文件走 `get_private_file_url`。
    async fn fetch_file_url(&self, file_id: &str, group_id: Option<i64>) -> Result<String, String> {
        let ctx = self
            .inner
            .active_context
            .lock()
            .expect("ctx poisoned")
            .clone()
            .ok_or_else(|| "no QQ connection active".to_string())?;
        let request = match group_id {
            Some(gid) => echo_core::action::actions::get_group_file_url(gid, file_id),
            None => echo_core::action::actions::get_private_file_url(file_id),
        };
        let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
        if !resp.is_ok() {
            return Err(resp
                .error_message()
                .unwrap_or_else(|| "get file url failed".to_string()));
        }
        resp.data
            .get("url")
            .and_then(|v| v.as_str())
            .filter(|u| !u.is_empty())
            .map(str::to_string)
            .ok_or_else(|| "file url missing in response".to_string())
    }

    /// 下载 `metadata.pending_files` 登记的文件（与 `files` 占位按序对应），
    /// 把结果写回 `files`（path/error），并把人类可读描述拼进 `content`。
    async fn resolve_pending_files(&self, msg: &mut IncomingMessage) {
        let pending: Vec<serde_json::Value> =
            match msg.metadata.get("pending_files").and_then(|v| v.as_array()) {
                Some(list) if !list.is_empty() => list.clone(),
                _ => Vec::new(),
            };
        if msg.files.is_empty() {
            // 无文件（普通文本/图片消息）：无事可做。
            return;
        }
        let cfg = &self.inner.config.files;
        let dir = crate::files::resolve_dir(&cfg.dir);
        let max_bytes = cfg.max_bytes();

        for (index, entry) in pending.iter().enumerate() {
            if index >= msg.files.len() {
                break;
            }
            let name = entry
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let file_id = entry
                .get("file_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let url = entry
                .get("url")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let group_id = entry.get("group_id").and_then(|v| v.as_i64());

            // 已知大小超限：直接跳过（条目仍送达，带原因）。
            if msg.files[index].size > max_bytes {
                msg.files[index].error = Some(format!(
                    "超过大小上限（{} > {}）",
                    format_size(msg.files[index].size),
                    format_size(max_bytes)
                ));
                continue;
            }

            let download_url = match url {
                Some(u) if !u.is_empty() => u,
                _ if !file_id.is_empty() => match self.fetch_file_url(&file_id, group_id).await {
                    Ok(u) => u,
                    Err(error) => {
                        tracing::warn!(%error, file = %name, "QQ file url lookup failed");
                        msg.files[index].error = Some(format!("获取下载链接失败：{error}"));
                        continue;
                    }
                },
                _ => {
                    msg.files[index].error = Some("文件缺少下载标识（file_id/url）".into());
                    continue;
                }
            };

            match crate::files::download_file(&download_url, &dir, &name, max_bytes).await {
                Ok((path, size)) => {
                    tracing::info!(file = %name, path = %path.display(), %size, "QQ file downloaded");
                    msg.files[index].path = Some(path.display().to_string());
                    if msg.files[index].size == 0 {
                        msg.files[index].size = size;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, file = %name, "QQ file download failed");
                    msg.files[index].error = Some(error);
                }
            }
        }

        // 清掉内部登记，不把下载细节带进下游 payload。
        if let Some(obj) = msg.metadata.as_object_mut() {
            obj.remove("pending_files");
            if obj.is_empty() {
                msg.metadata = serde_json::Value::Null;
            }
        }
        // 人类可读描述进 content（面板展示用）；结构化明细在 files 字段。
        let note = describe_files(&msg.files);
        if !note.is_empty() {
            if msg.content.trim().is_empty() {
                msg.content = note;
            } else {
                msg.content.push('\n');
                msg.content.push_str(&note);
            }
        }
    }

    /// 投递到单向 hook；无 hook 时转发给订阅者。
    /// Agent 输出只能经显式 send 工具回到 QQ，绝不从这条边界回流。
    async fn deliver(&self, msg: IncomingMessage) {
        let hook = self
            .inner
            .message_hook
            .lock()
            .expect("message hook poisoned")
            .clone();
        if let Some(hook) = hook {
            if let Err(error) = hook.on_incoming_message(msg).await {
                tracing::warn!(%error, "inbound message hook failed");
            }
        } else {
            // No hook — forward to subscribers.
            let adapter_event = AdapterEvent::MessageReceived(Box::new(msg));
            let subs = self.inner.subscribers.lock().expect("subs poisoned");
            for tx in subs.iter() {
                let _ = tx.send(adapter_event.clone());
            }
        }
    }
}

/// 人类可读字节数（content 描述用；files 字段里保留精确值）。
fn format_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let value = bytes as f64;
    if bytes == 0 {
        "大小未知".to_string()
    } else if value >= GB {
        format!("{:.1} GB", value / GB)
    } else if value >= MB {
        format!("{:.1} MB", value / MB)
    } else if value >= KB {
        format!("{:.1} KB", value / KB)
    } else {
        format!("{bytes} B")
    }
}

/// 文件列表 → content 里的简短描述（人类可读；结构化信息在 files 字段）。
fn describe_files(files: &[IncomingFile]) -> String {
    files
        .iter()
        .map(|f| {
            let size = if f.size > 0 {
                format!("（{}）", format_size(f.size))
            } else {
                String::new()
            };
            match (&f.path, &f.error) {
                (None, Some(error)) => format!("（收到文件）{}{}未下载：{}", f.name, size, error),
                _ => format!("（收到文件）{}{}", f.name, size),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// 单张图片最大下载体积；超出则保留原 URL（链路上以文本占位降级）。
const MAX_EMBEDDED_IMAGE_BYTES: usize = 10 * 1024 * 1024;

/// Shared HTTP client for image downloads (built once, process-wide).
fn image_client() -> reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "reqwest builder failed, using client without timeouts");
                reqwest::Client::new()
            })
    })
    .clone()
}

/// 把消息里的远程图片 URL 逐个下载并落盘，改写为 `/media/<id>` 引用；
/// 下载/落盘失败的保留原 URL（请求构建侧的占位替换会兜住过期链接，
/// 不会再毒化请求）。
async fn persist_remote_images(mut msg: IncomingMessage) -> IncomingMessage {
    if msg.images.is_empty() {
        return msg;
    }
    let mut images = Vec::with_capacity(msg.images.len());
    for image in std::mem::take(&mut msg.images) {
        images.push(persist_remote_image(image).await);
    }
    msg.images = images;
    msg
}

async fn persist_remote_image(url: String) -> String {
    if echo_defs::media_store::is_media_ref(&url)
        || url.starts_with("data:")
        || !url.starts_with("http")
    {
        return url;
    }
    match download_and_store(&url).await {
        Ok(reference) => reference,
        Err(error) => {
            tracing::warn!(%error, "image download/store failed, keeping original URL");
            url
        }
    }
}

/// 下载远端图片并落盘，返回 `/media/<id>` 引用。
async fn download_and_store(url: &str) -> Result<String, String> {
    let resp = image_client()
        .get(url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    // 边下边检（2026-10 巡检 P2）：先信 Content-Length（超限直接拒，
    // 不读体），再流式读体、累计超 MAX 即中断——超限图不再整载入内存。
    if let Some(len) = resp.content_length() {
        if len > MAX_EMBEDDED_IMAGE_BYTES as u64 {
            return Err(format!("image too large (content-length {len})"));
        }
    }
    let mime = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| raw.split(';').next())
        .map(str::trim)
        .filter(|value| value.starts_with("image/"))
        .unwrap_or("image/jpeg")
        .to_string();
    let mut bytes = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        if bytes.len() + chunk.len() > MAX_EMBEDDED_IMAGE_BYTES {
            return Err(format!(
                "image too large (> {} bytes while streaming)",
                MAX_EMBEDDED_IMAGE_BYTES
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let id = echo_defs::media_store::save_image_bytes(&bytes, &mime)?;
    Ok(echo_defs::media_store::media_ref(&id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::QqGateMode;
    use crate::config::{QqAdapterConfig, QqTriggerConfig};
    use echo_core::Event;
    use echo_server::connection::Outgoing;
    use echo_server::Handler;
    use std::sync::atomic::AtomicBool;
    use std::sync::Mutex as StdMutex;

    /// A scriptable inbound hook for handler tests.
    struct MockHook {
        result: StdMutex<Option<Result<(), String>>>,
        messages: StdMutex<Vec<echo_adapter::IncomingMessage>>,
    }

    impl MockHook {
        fn new(result: Result<(), String>) -> Self {
            Self {
                result: StdMutex::new(Some(result)),
                messages: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl echo_adapter::InboundMessageHook for MockHook {
        async fn on_incoming_message(
            &self,
            msg: echo_adapter::IncomingMessage,
        ) -> Result<(), String> {
            self.messages.lock().unwrap().push(msg);
            self.result.lock().unwrap().take().unwrap_or(Ok(()))
        }
        fn on_connection_state(
            &self,
            _adapter_name: &str,
            _connected: bool,
            _self_id: Option<String>,
        ) {
        }
    }

    fn test_inner(trigger: QqTriggerConfig) -> Arc<QqInner> {
        let cfg = QqAdapterConfig {
            trigger,
            ..Default::default()
        };
        test_inner_with_config(cfg)
    }

    fn test_inner_with_config(cfg: QqAdapterConfig) -> Arc<QqInner> {
        Arc::new(QqInner {
            instance_name: crate::adapter::DEFAULT_INSTANCE_NAME.to_string(),
            persona: None,
            display_name: "QQ / OneBot".to_string(),
            config: cfg,
            active_context: StdMutex::new(None),
            running: AtomicBool::new(false),
            started_at: StdMutex::new(None),
            connected: AtomicBool::new(false),
            self_id: StdMutex::new(None),
            subscribers: StdMutex::new(Vec::new()),
            filter: StdMutex::new(None),
            shutdown_tx: StdMutex::new(None),
            server_task: StdMutex::new(None),
            napcat_config_task: StdMutex::new(None),
            tracker: echo_server::ConnectionTracker::default(),
            message_hook: StdMutex::new(None),
            extra_handlers: StdMutex::new(Vec::new()),
            runtime_allowlist_users: StdMutex::new(Vec::new()),
            runtime_allowlist_groups: StdMutex::new(Vec::new()),
            runtime_denylist_users: StdMutex::new(Vec::new()),
            runtime_denylist_groups: StdMutex::new(Vec::new()),
            gate_mode: StdMutex::new(QqGateMode::None),
            owner_qq: StdMutex::new(0),
            config_store: StdMutex::new(None),
            file_bridge: crate::file_bridge::FileBridge::new(
                "http://localhost:3000",
                "127.0.0.1",
                "napcat",
                "/app/napcat/data",
            ),
            group_names: dashmap::DashMap::new(),
        })
    }

    fn test_ctx() -> (Context, tokio::sync::mpsc::Receiver<Outgoing>) {
        let (api_tx, api_rx) = tokio::sync::mpsc::channel(16);
        let ctx = Context::new(
            10001,
            "conn-test".into(),
            api_tx,
            std::sync::Arc::new(dashmap::DashMap::new()),
        );
        (ctx, api_rx)
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

    /// 私聊图片事件（image 段带远程 URL，content 为空）。
    fn private_image_event(url: &str) -> Event {
        serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 44,
            "user_id": 123456,
            "message": [{"type": "image", "data": {"file": "x.png", "url": url}}],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap()
    }

    /// 群聊图片事件（`at_me` 控制是否 @ 机器人）。
    fn group_image_event(url: &str, at_me: bool) -> Event {
        let mut segments =
            vec![serde_json::json!({"type": "image", "data": {"file": "x.png", "url": url}})];
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
            "message_id": 45,
            "group_id": 999,
            "user_id": 123456,
            "message": segments,
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester", "card": ""}
        }))
        .unwrap()
    }

    fn heartbeat_event() -> Event {
        serde_json::from_value(serde_json::json!({
            "post_type": "meta_event",
            "meta_event_type": "heartbeat",
            "time": 1700000000,
            "self_id": 10001,
            "interval": 5000,
            "status": {"online": true}
        }))
        .unwrap()
    }

    fn group_upload_event(file_id: &str, name: &str, size: i64) -> Event {
        serde_json::from_value(serde_json::json!({
            "post_type": "notice",
            "notice_type": "group_upload",
            "time": 1700000000,
            "self_id": 10001,
            "group_id": 999,
            "user_id": 123456,
            "file": {"id": file_id, "name": name, "size": size, "busid": 102}
        }))
        .unwrap()
    }

    /// 极简 HTTP 文件服务（group_upload 直链下载测试用）。
    fn serve_once(body: Vec<u8>) -> std::net::SocketAddr {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
            }
        });
        addr
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "echo-qq-handler-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn group_upload_disabled_passes_without_delivery() {
        let mut cfg = QqAdapterConfig::default();
        cfg.files.accept_group_upload = false;
        let inner = test_inner_with_config(cfg);
        let handler = QqHandler::new(inner.clone());
        let (ctx, _rx) = test_ctx();
        assert_eq!(
            handler
                .handle(&ctx, &group_upload_event("fid", "a.zip", 1024))
                .await,
            HandleResult::Pass
        );
    }

    #[tokio::test]
    async fn group_upload_delivers_failed_entry_when_api_unavailable() {
        // 连接侧已断开（api 通道关闭）→ 直链获取失败；条目仍送达
        // （error 说明），避免用户发的文件「石沉大海」。
        let inner = test_inner(QqTriggerConfig::default());
        let hook = Arc::new(MockHook::new(Ok(())));
        *inner.message_hook.lock().unwrap() = Some(hook.clone());
        let handler = QqHandler::new(inner);
        let (ctx, rx) = test_ctx();
        drop(rx); // api 通道关闭：send_api 立即以 ConnectionClosed 失败
        assert_eq!(
            handler
                .handle(&ctx, &group_upload_event("fid-1", "report.pdf", 2048))
                .await,
            HandleResult::Handled
        );
        let messages = hook.messages.lock().unwrap();
        assert_eq!(messages.len(), 1, "file notice delivered once");
        let msg = &messages[0];
        assert!(msg.channel.is_group());
        assert_eq!(msg.files.len(), 1);
        assert_eq!(msg.files[0].name, "report.pdf");
        assert!(msg.files[0].path.is_none());
        let error = msg.files[0].error.as_deref().unwrap_or("");
        assert!(error.contains("获取下载链接失败"), "err: {error}");
        assert!(error.contains("connection closed"), "err: {error}");
        assert!(
            msg.content
                .contains("（收到文件）report.pdf（2.0 KB）未下载："),
            "content: {}",
            msg.content
        );
        // 内部登记不进入下游 payload。
        assert!(msg.metadata.get("pending_files").is_none());
    }

    #[tokio::test]
    async fn private_file_disabled_drops_file_only_message() {
        let mut cfg = QqAdapterConfig::default();
        cfg.files.accept_private_file = false;
        let inner = test_inner_with_config(cfg);
        let hook = Arc::new(MockHook::new(Ok(())));
        *inner.message_hook.lock().unwrap() = Some(hook.clone());
        let handler = QqHandler::new(inner);
        let (ctx, _rx) = test_ctx();
        assert_eq!(
            handler.handle(&ctx, &private_file_event("test.txt")).await,
            HandleResult::Pass
        );
        assert!(hook.messages.lock().unwrap().is_empty(), "no delivery");
    }

    #[tokio::test]
    async fn private_file_disabled_keeps_text_only_message() {
        let mut cfg = QqAdapterConfig::default();
        cfg.files.accept_private_file = false;
        let inner = test_inner_with_config(cfg);
        let hook = Arc::new(MockHook::new(Ok(())));
        *inner.message_hook.lock().unwrap() = Some(hook.clone());
        let handler = QqHandler::new(inner);
        let (ctx, _rx) = test_ctx();
        assert_eq!(
            handler
                .handle(&ctx, &private_text_and_file_event("看这个"))
                .await,
            HandleResult::Handled
        );
        let messages = hook.messages.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "看这个");
        assert!(messages[0].files.is_empty(), "file stripped");
        assert!(messages[0].metadata.is_null());
    }

    #[tokio::test]
    async fn resolve_pending_files_appends_note_for_error_only_files() {
        // 在线文件（无 pending 登记、仅错误条目）：note 仍应写入 content。
        let inner = test_inner(QqTriggerConfig::default());
        let handler = QqHandler::new(inner);
        let mut msg = IncomingMessage {
            adapter_name: "qq".into(),
            platform: "qq".into(),
            user_id: "1".into(),
            user_name: "t".into(),
            channel: ChannelType::Direct,
            group_name: None,
            content: String::new(),
            timestamp: 0,
            at_me: false,
            metadata: serde_json::Value::Null,
            images: vec![],
            files: vec![IncomingFile {
                name: "big.bin".into(),
                path: None,
                size: 2048,
                error: Some("在线文件（QQ 直传）暂不支持自动接收".into()),
            }],
        };
        handler.resolve_pending_files(&mut msg).await;
        assert!(
            msg.content
                .contains("（收到文件）big.bin（2.0 KB）未下载：在线文件"),
            "content: {}",
            msg.content
        );
    }

    #[tokio::test]
    async fn resolve_pending_files_reports_missing_identifier_without_network() {
        let inner = test_inner(QqTriggerConfig::default());
        let handler = QqHandler::new(inner);
        let mut msg = IncomingMessage {
            adapter_name: "qq".into(),
            platform: "qq".into(),
            user_id: "1".into(),
            user_name: "t".into(),
            channel: ChannelType::Direct,
            group_name: None,
            content: String::new(),
            timestamp: 0,
            at_me: false,
            metadata: serde_json::json!({"pending_files": [{
                "name": "x.bin", "size": 1, "file_id": "", "url": null
            }]}),
            images: vec![],
            files: vec![IncomingFile {
                name: "x.bin".into(),
                path: None,
                size: 1,
                error: None,
            }],
        };
        handler.resolve_pending_files(&mut msg).await;
        let error = msg.files[0].error.as_deref().unwrap_or("");
        assert!(error.contains("缺少下载标识"), "err: {error}");
    }

    #[tokio::test]
    async fn resolve_pending_files_skips_over_limit() {
        let mut cfg = QqAdapterConfig::default();
        cfg.files.max_mb = 1;
        let inner = test_inner_with_config(cfg);
        let handler = QqHandler::new(inner);
        let mut msg = IncomingMessage {
            adapter_name: "qq".into(),
            platform: "qq".into(),
            user_id: "1".into(),
            user_name: "t".into(),
            channel: ChannelType::Direct,
            group_name: None,
            content: String::new(),
            timestamp: 0,
            at_me: false,
            metadata: serde_json::json!({"pending_files": [{
                "name": "big.bin", "size": 2 * 1024 * 1024u64,
                "file_id": "fid", "url": "https://example.invalid/x"
            }]}),
            images: vec![],
            files: vec![IncomingFile {
                name: "big.bin".into(),
                path: None,
                size: 2 * 1024 * 1024,
                error: None,
            }],
        };
        handler.resolve_pending_files(&mut msg).await;
        assert!(msg.files[0].path.is_none());
        let error = msg.files[0].error.as_deref().unwrap_or("");
        assert!(error.contains("超过大小上限"), "err: {error}");
        assert!(msg.content.contains("未下载"), "content: {}", msg.content);
        assert!(msg.metadata.get("pending_files").is_none());
    }

    #[tokio::test]
    async fn resolve_pending_files_downloads_direct_url() {
        let body = b"pdf-bytes".to_vec();
        let addr = serve_once(body.clone());
        let dir = temp_dir("download");
        let mut cfg = QqAdapterConfig::default();
        cfg.files.dir = dir.display().to_string();
        let inner = test_inner_with_config(cfg);
        let handler = QqHandler::new(inner);
        let mut msg = IncomingMessage {
            adapter_name: "qq".into(),
            platform: "qq".into(),
            user_id: "1".into(),
            user_name: "t".into(),
            channel: ChannelType::Direct,
            group_name: None,
            content: "看看这个".into(),
            timestamp: 0,
            at_me: false,
            metadata: serde_json::json!({"pending_files": [{
                "name": "report.pdf", "size": 9,
                "file_id": "fid", "url": format!("http://{addr}/f")
            }]}),
            images: vec![],
            files: vec![IncomingFile {
                name: "report.pdf".into(),
                path: None,
                size: 9,
                error: None,
            }],
        };
        handler.resolve_pending_files(&mut msg).await;
        let path = msg.files[0].path.as_deref().expect("downloaded path");
        assert!(path.ends_with("-report.pdf"), "path: {path}");
        assert_eq!(std::fs::read(path).unwrap(), body);
        assert!(msg.content.starts_with("看看这个"));
        assert!(
            msg.content.contains("（收到文件）report.pdf"),
            "content: {}",
            msg.content
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn format_and_describe_helpers() {
        assert_eq!(format_size(0), "大小未知");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(2048), "2.0 KB");
        assert_eq!(format_size(5 * 1024 * 1024), "5.0 MB");
        let files = vec![
            IncomingFile {
                name: "a.zip".into(),
                path: Some("/tmp/a.zip".into()),
                size: 2048,
                error: None,
            },
            IncomingFile {
                name: "b.zip".into(),
                path: None,
                size: 10,
                error: Some("boom".into()),
            },
        ];
        let text = describe_files(&files);
        assert!(text.contains("（收到文件）a.zip（2.0 KB）"), "text: {text}");
        assert!(
            text.contains("（收到文件）b.zip（10 B）未下载：boom"),
            "text: {text}"
        );
    }

    fn private_file_event(text: &str) -> Event {
        serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 51,
            "user_id": 123456,
            "message": [{"type": "file", "data": {
                "file": "test.txt", "file_id": "fid-t", "file_size": "5"
            }}],
            "raw_message": text,
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap()
    }

    fn private_text_and_file_event(text: &str) -> Event {
        serde_json::from_value(serde_json::json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1700000000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 52,
            "user_id": 123456,
            "message": [
                {"type": "text", "data": {"text": text}},
                {"type": "file", "data": {
                    "file": "test.txt", "file_id": "fid-t", "file_size": "5"
                }}
            ],
            "raw_message": text,
            "font": 0,
            "sender": {"user_id": 123456, "nickname": "tester"}
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn non_message_event_passes() {
        let inner = test_inner(QqTriggerConfig::default());
        let handler = QqHandler::new(inner);
        let (ctx, _rx) = test_ctx();
        assert_eq!(
            handler.handle(&ctx, &heartbeat_event()).await,
            HandleResult::Pass
        );
    }

    #[tokio::test]
    async fn dm_dropped_when_dm_auto_reply_disabled() {
        let trigger = QqTriggerConfig {
            dm_auto_reply: false,
            ..Default::default()
        };
        let handler = QqHandler::new(test_inner(trigger));
        let (ctx, _rx) = test_ctx();
        assert_eq!(
            handler.handle(&ctx, &private_event("hi")).await,
            HandleResult::Pass
        );
    }

    #[tokio::test]
    async fn group_message_without_at_me_passes() {
        let handler = QqHandler::new(test_inner(QqTriggerConfig::default()));
        let (ctx, _rx) = test_ctx();
        assert_eq!(
            handler.handle(&ctx, &group_event("hello", false)).await,
            HandleResult::Pass
        );
    }

    #[tokio::test]
    async fn group_message_with_at_me_handled() {
        let handler = QqHandler::new(test_inner(QqTriggerConfig::default()));
        let (ctx, _rx) = test_ctx();
        assert_eq!(
            handler.handle(&ctx, &group_event("hello", true)).await,
            HandleResult::Handled
        );
    }

    #[tokio::test]
    async fn dm_message_routes_to_hook_without_automatic_reply() {
        let hook = Arc::new(MockHook::new(Ok(())));
        let inner = test_inner(QqTriggerConfig::default());
        *inner.message_hook.lock().unwrap() = Some(hook.clone());
        let handler = QqHandler::new(inner);
        let (ctx, mut api_rx) = test_ctx();

        assert_eq!(
            handler.handle(&ctx, &private_event("ping")).await,
            HandleResult::Handled
        );
        assert_eq!(hook.messages.lock().unwrap().len(), 1);
        assert!(
            api_rx.try_recv().is_err(),
            "hook must not auto-send a reply"
        );
    }

    #[tokio::test]
    async fn hook_error_does_not_send_platform_message() {
        let hook: Arc<dyn echo_adapter::InboundMessageHook> =
            Arc::new(MockHook::new(Err("llm down".into())));
        let inner = test_inner(QqTriggerConfig::default());
        *inner.message_hook.lock().unwrap() = Some(hook);
        let handler = QqHandler::new(inner);
        let (ctx, mut api_rx) = test_ctx();

        assert_eq!(
            handler.handle(&ctx, &private_event("ping")).await,
            HandleResult::Handled
        );
        assert!(api_rx.try_recv().is_err(), "hook errors must stay internal");
    }

    #[tokio::test]
    async fn no_bridge_forwards_to_subscribers() {
        let inner = test_inner(QqTriggerConfig::default());
        let (sub_tx, mut sub_rx) = tokio::sync::mpsc::unbounded_channel();
        inner.subscribers.lock().unwrap().push(sub_tx);
        let handler = QqHandler::new(inner);
        let (ctx, _api_rx) = test_ctx();

        assert_eq!(
            handler.handle(&ctx, &private_event("hi")).await,
            HandleResult::Handled
        );
        let event = sub_rx.recv().await.expect("subscriber notified");
        assert!(matches!(
            event,
            echo_adapter::AdapterEvent::MessageReceived(_)
        ));
    }

    /// 媒体落盘测试的进程级环境变量锁：`ECHO_MEDIA_DIR` 是进程级，多个测试
    /// 并行设置/清除会互相踩（tokio Mutex：跨 await 持有不触发
    /// clippy::await_holding_lock）。
    static MEDIA_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// 媒体落盘测试共享一个隔离目录（环境变量是进程级，串行化）。
    fn media_test_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "echo-adapter-qq-media-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn download_and_store_writes_media_file() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/img.png"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(vec![1u8, 2, 3])
                    .insert_header("content-type", "image/png"),
            )
            .mount(&server)
            .await;

        let _serial = MEDIA_ENV_LOCK.lock().await;
        let dir = media_test_dir("store");
        std::env::set_var("ECHO_MEDIA_DIR", &dir);
        let reference = download_and_store(&format!("{}/img.png", server.uri()))
            .await
            .expect("download+store succeeds");
        std::env::remove_var("ECHO_MEDIA_DIR");
        assert!(reference.starts_with("/media/"), "{reference}");
        assert!(reference.ends_with(".png"), "{reference}");
        // 落盘文件的内容与源一致
        let id = echo_defs::media_store::id_of_ref(&reference).unwrap();
        let stored = std::fs::read(dir.join(id)).expect("media file exists");
        assert_eq!(stored, vec![1u8, 2, 3]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn persist_remote_image_keeps_url_on_failure() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let url = format!("{}/missing.png", server.uri());
        assert_eq!(
            persist_remote_image(url.clone()).await,
            url,
            "failed downloads keep the original URL"
        );
    }

    #[tokio::test]
    async fn persist_remote_image_passes_through_data_uris_and_refs() {
        let data = "data:image/png;base64,QUJD".to_string();
        assert_eq!(persist_remote_image(data.clone()).await, data);
        let reference = "/media/abc.png".to_string();
        assert_eq!(persist_remote_image(reference.clone()).await, reference);
    }

    /// 回归（2026-09-29）：被触发门控丢弃的消息（群聊非 @我）**不得**预下载
    /// 图片——此前 `persist_remote_images` 紧跟 convert_message，丢弃的消息也
    /// 会把图片落盘，媒体库积累无主文件（2026-09-29 实测清理出 37 张）。
    #[tokio::test]
    async fn gated_drop_does_not_download_images() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let _serial = MEDIA_ENV_LOCK.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(vec![1u8, 2, 3])
                    .insert_header("content-type", "image/png"),
            )
            .mount(&server)
            .await;

        let dir = media_test_dir("gate-skip");
        std::env::set_var("ECHO_MEDIA_DIR", &dir);
        let handler = QqHandler::new(test_inner(QqTriggerConfig::default()));
        let (ctx, _rx) = test_ctx();
        let image_url = format!("{}/img.png", server.uri());
        assert_eq!(
            handler
                .handle(&ctx, &group_image_event(&image_url, false))
                .await,
            HandleResult::Pass
        );
        std::env::remove_var("ECHO_MEDIA_DIR");

        let received = server.received_requests().await.unwrap_or_default();
        assert!(
            received.is_empty(),
            "gated-drop message must not fetch images: {received:?}"
        );
        assert!(
            !dir.exists(),
            "gated-drop message must not create media files"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 门控与过滤通过的消息：图片在投递前落盘为 `/media/<id>` 引用。
    #[tokio::test]
    async fn delivered_message_persists_images_before_hook() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let _serial = MEDIA_ENV_LOCK.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/img.png"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(vec![9u8, 8, 7])
                    .insert_header("content-type", "image/png"),
            )
            .mount(&server)
            .await;

        let dir = media_test_dir("deliver-store");
        std::env::set_var("ECHO_MEDIA_DIR", &dir);
        let hook = Arc::new(MockHook::new(Ok(())));
        let inner = test_inner(QqTriggerConfig::default());
        *inner.message_hook.lock().unwrap() = Some(hook.clone());
        let handler = QqHandler::new(inner);
        let (ctx, _rx) = test_ctx();

        let image_url = format!("{}/img.png", server.uri());
        assert_eq!(
            handler.handle(&ctx, &private_image_event(&image_url)).await,
            HandleResult::Handled
        );
        std::env::remove_var("ECHO_MEDIA_DIR");

        let messages = hook.messages.lock().unwrap();
        assert_eq!(messages.len(), 1, "hook receives the message");
        let images = &messages[0].images;
        assert_eq!(images.len(), 1);
        assert!(
            images[0].starts_with("/media/"),
            "image must be persisted as a ref before delivery: {}",
            images[0]
        );
        let id = echo_defs::media_store::id_of_ref(&images[0]).expect("valid ref");
        assert_eq!(
            std::fs::read(dir.join(id)).expect("stored bytes"),
            vec![9u8, 8, 7]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
