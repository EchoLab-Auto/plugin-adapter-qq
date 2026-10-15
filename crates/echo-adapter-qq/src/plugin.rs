//! 适配器插件入口（P1.7b）：把 [`QqAdapter`] 接到 `echo-plugin-sdk` 的
//! `AdapterPlugin` 接口上——宿主经 stdio 拉起本进程后，用
//! `Invoke("<实例名>:<op>")` 寻址；入站消息与连接态经 `adapter_host`
//! 宿主服务回呼。
//!
//! # 宿主下发配置（`Hello.config`）
//!
//! ```jsonc
//! {
//!   "name": "qq",                 // 实例名（= Invoke 前缀；缺省 "qq"）
//!   "persona": "default",         // 归属人格（可选；显示名与实例区分用）
//!   "state_file": "/path/qq.toml",// 门控 / owner 运行期持久化文件（可选）
//!   "config": { /* QqAdapterConfig */ } // 适配器配置（含端口分配结果；缺省全默认）
//! }
//! ```
//!
//! # op 路由（见 `document/adapter-plugin.md` §宿主动作）
//!
//! `start` / `stop` / `status` / `send_text` / `send_media` /
//! `get_groups` / `get_friends` / `custom`（平台专属动作透传：
//! 门控、名单、owner、登录二维码、`instance_info` 等）。

use std::sync::{Arc, OnceLock};

use echo_plugin_sdk::echo_plugin_api::{
    AdapterCapabilities, AdapterContribution, InvokeOutcome,
};
use echo_plugin_sdk::{AdapterPlugin, HostCallError, HostClient};
use serde_json::{json, Value};

use echo_adapter::types::{ChannelType, IncomingMessage, MessageTarget};
use echo_adapter::{
    Adapter, AdapterConnectionState, AdapterManagement, ConfigStore, InboundMessageHook,
};

use crate::adapter::qq::DEFAULT_INSTANCE_NAME;
use crate::adapter::QqAdapter;
use crate::config::QqAdapterConfig;

/// 适配器宿主服务名（`HostCall.service`）。
const ADAPTER_HOST_SERVICE: &str = "adapter_host";

/// QQ 适配器插件。
///
/// 可 Clone（内部态共享）：`configure` 之后 [`adapter_handle`] 可取到已
/// 构建的 [`QqAdapter`]（测试 / 集成自检用）。
///
/// [`adapter_handle`]: QqPlugin::adapter_handle
#[derive(Clone)]
pub struct QqPlugin {
    state: Arc<QqPluginState>,
}

struct QqPluginState {
    /// `configure`（Hello 配置）后构建的适配器。
    adapter: OnceLock<Arc<QqAdapter>>,
    /// 握手完成后由 SDK 交付的宿主客户端。
    host: OnceLock<HostClient>,
}

impl QqPlugin {
    /// 新插件实例（等待经 `Hello.config` 配置）。
    pub fn new() -> Self {
        Self {
            state: Arc::new(QqPluginState {
                adapter: OnceLock::new(),
                host: OnceLock::new(),
            }),
        }
    }

    /// 已构建的适配器句柄（`configure` 之后可用）。
    pub fn adapter_handle(&self) -> Option<Arc<QqAdapter>> {
        self.state.adapter.get().cloned()
    }

    /// 宿主客户端句柄（握手完成后可用；入站投递与 P1.7c 的媒体登记用）。
    pub fn host_handle(&self) -> Option<HostClient> {
        self.state.host.get().cloned()
    }
}

impl Default for QqPlugin {
    fn default() -> Self {
        Self::new()
    }
}

/// 宿主下发配置（`Hello.config` 的强类型视图）。
#[derive(Debug, serde::Deserialize)]
#[serde(default)]
struct PluginConfig {
    /// 实例名（= Invoke 前缀；宿主分配，全局唯一）。
    name: String,
    /// 归属人格（显示名 / 会话归属用）。
    persona: Option<String>,
    /// 门控 / owner 运行期持久化文件（可选；缺省不持久化）。
    state_file: Option<String>,
    /// 适配器配置（含端口分配结果）。
    config: QqAdapterConfig,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            name: DEFAULT_INSTANCE_NAME.to_string(),
            persona: None,
            state_file: None,
            config: QqAdapterConfig::default(),
        }
    }
}

#[echo_plugin_sdk::async_trait]
impl AdapterPlugin for QqPlugin {
    async fn configure(&self, config: Value) -> Result<(), String> {
        let parsed: PluginConfig = if config.is_null() {
            PluginConfig::default()
        } else {
            serde_json::from_value(config).map_err(|e| format!("invalid qq plugin config: {e}"))?
        };
        let adapter = QqAdapter::with_instance(parsed.name.clone(), parsed.persona.clone(), parsed.config);
        if let Some(path) = &parsed.state_file {
            adapter.set_config_store(ConfigStore::new(path.clone()));
        }
        self.state
            .adapter
            .set(Arc::new(adapter))
            .map_err(|_| "configure called twice".to_string())?;
        Ok(())
    }

    fn adapter(&self) -> AdapterContribution {
        let (name, display_name) = match self.state.adapter.get() {
            Some(adapter) => (adapter.name().to_string(), adapter.display_name().to_string()),
            // 未配置（SDK 必先调 configure；此处仅防御）：占位身份。
            None => (DEFAULT_INSTANCE_NAME.to_string(), "QQ / OneBot".to_string()),
        };
        AdapterContribution {
            name,
            platform: "qq".to_string(),
            display_name,
            capabilities: AdapterCapabilities {
                send_text: true,
                send_media: true,
                groups: true,
                friends: true,
                gating: true,
                login: true,
            },
            config_schema: None,
            ui: None,
            session_labels: None,
        }
    }

    async fn invoke_op(&self, op: &str, payload: Value) -> InvokeOutcome {
        let Some(adapter) = self.state.adapter.get().cloned() else {
            return error_outcome("not_configured", "qq plugin received no config");
        };
        match op {
            "start" => match adapter.start().await {
                Ok(()) => ok_result(json!({})),
                Err(e) => error_outcome("adapter_error", e.to_string()),
            },
            "stop" => match adapter.stop().await {
                Ok(()) => ok_result(json!({})),
                Err(e) => error_outcome("adapter_error", e.to_string()),
            },
            "status" => {
                let info = adapter.status_info();
                ok_result(json!({
                    "state": state_str(info.status),
                    "self_id": info.self_id,
                    "started_at": info.started_at,
                }))
            }
            "send_text" => send_text(&adapter, &payload).await,
            "send_media" => send_media(&adapter, &payload).await,
            "get_groups" | "get_friends" => custom_op(&adapter, op, Value::Null).await,
            "custom" => {
                let Some(method) = payload.get("method").and_then(Value::as_str) else {
                    return error_outcome("invalid_payload", "custom requires `method`");
                };
                let inner = payload.get("payload").cloned().unwrap_or(Value::Null);
                custom_op(&adapter, method, inner).await
            }
            other => error_outcome("unknown_op", format!("unknown adapter op: {other}")),
        }
    }

    async fn on_ready(&self, host: HostClient) {
        if let Some(adapter) = self.state.adapter.get().cloned() {
            // 入站 / 连接态经宿主服务回呼（替代旧的内嵌 hook）。
            let bridge = Arc::new(HostBridge {
                host: host.clone(),
                adapter: adapter.name().to_string(),
            });
            adapter.set_message_hook(bridge);
        } else {
            tracing::warn!("qq plugin on_ready before configure; inbound bridge not installed");
        }
        let _ = self.state.host.set(host);
    }

    async fn on_drain(&self, _deadline_ms: u64) {
        // 尽力优雅停机（NapCat auto-stop / 服务器关闭）；失败只记日志，
        // 宿主在 deadline 后强杀。
        if let Some(adapter) = self.state.adapter.get().cloned() {
            if let Err(e) = adapter.stop().await {
                tracing::debug!(error = %e, "qq adapter stop during drain returned error");
            }
        }
    }
}

/// 入站消息钩子 → `adapter_host` 宿主服务（`deliver_message` / `report_state`）。
struct HostBridge {
    host: HostClient,
    adapter: String,
}

#[echo_plugin_sdk::async_trait]
impl InboundMessageHook for HostBridge {
    async fn on_incoming_message(&self, msg: IncomingMessage) -> Result<(), String> {
        let payload = json!({"message": message_to_wire(&msg)});
        // 反压退避：宿主队列满（`backpressure`）时小步重试；耗尽后报错，
        // 由 QqHandler 侧记日志。
        let mut backoff = std::time::Duration::from_millis(100);
        for attempt in 0..3 {
            match self
                .host
                .call(ADAPTER_HOST_SERVICE, "deliver_message", payload.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(HostCallError::Service { code, .. }) if code == "backpressure" && attempt < 2 => {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => return Err(e.to_string()),
            }
        }
        Err("deliver_message: backpressure persisted after retries".to_string())
    }

    fn on_connection_state(&self, _adapter_name: &str, connected: bool, self_id: Option<String>) {
        // 同步钩子：spawn 后台任务上报（宿主侧只更新缓存，不需要回执等待）。
        let host = self.host.clone();
        let adapter = self.adapter.clone();
        tokio::spawn(async move {
            let payload = json!({
                "adapter": adapter,
                "connected": connected,
                "self_id": self_id,
            });
            if let Err(e) = host.call(ADAPTER_HOST_SERVICE, "report_state", payload).await {
                tracing::debug!(error = %e, "report_state host call failed");
            }
        });
    }
}

/// `IncomingMessage` → `deliver_message` 线格式（`AdapterMessage` 同构）。
fn message_to_wire(msg: &IncomingMessage) -> Value {
    let channel = match &msg.channel {
        ChannelType::Direct => json!({"type": "dm"}),
        ChannelType::Group { group_id } => json!({"type": "group", "group_id": group_id}),
    };
    let files: Vec<Value> = msg
        .files
        .iter()
        .map(|file| {
            json!({
                "name": file.name,
                "path": file.path,
                "size": file.size,
                "error": file.error,
            })
        })
        .collect();
    json!({
        "adapter": msg.adapter_name,
        "platform": msg.platform,
        "user_id": msg.user_id,
        "user_name": if msg.user_name.is_empty() { None } else { Some(&msg.user_name) },
        "channel": channel,
        "group_name": msg.group_name,
        "content": msg.content,
        "timestamp": msg.timestamp,
        "at_me": msg.at_me,
        "metadata": msg.metadata,
        "images": msg.images,
        "files": files,
    })
}

/// 出站目标解析（`{channel: {type …}, user_id}` → [`MessageTarget`]）。
fn parse_target(payload: &Value, adapter_name: &str) -> Result<MessageTarget, (String, String)> {
    let invalid = |msg: String| ("invalid_payload".to_string(), msg);
    let Some(target) = payload.get("target") else {
        return Err(invalid("missing target".to_string()));
    };
    let channel_json = &target["channel"];
    let channel = match channel_json.get("type").and_then(Value::as_str) {
        Some("dm") => ChannelType::Direct,
        Some("group") => {
            let Some(group_id) = channel_json.get("group_id").and_then(Value::as_str) else {
                return Err(invalid("group target requires group_id".to_string()));
            };
            ChannelType::Group {
                group_id: group_id.to_string(),
            }
        }
        other => {
            return Err(invalid(format!(
                "unknown channel type {other:?} (expected dm|group)"
            )))
        }
    };
    let user_id = target
        .get("user_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if matches!(channel, ChannelType::Direct) && user_id.is_empty() {
        return Err(invalid("dm target requires user_id".to_string()));
    }
    Ok(MessageTarget {
        adapter_name: adapter_name.to_string(),
        channel,
        user_id,
    })
}

/// `send_text`：`{target, content}` → `{message_id?, success, error?}`。
async fn send_text(adapter: &QqAdapter, payload: &Value) -> InvokeOutcome {
    let target = match parse_target(payload, adapter.name()) {
        Ok(target) => target,
        Err((code, message)) => return error_outcome(code, message),
    };
    let content = payload.get("content").and_then(Value::as_str).unwrap_or_default();
    match adapter.send_message(&target, content).await {
        Ok(result) => ok_result(json!({
            "message_id": result.message_id,
            "success": result.success,
            "error": result.error,
        })),
        Err(e) => error_outcome("send_failed", e.to_string()),
    }
}

/// `send_media`：`{target, kind: "image"|"file", source: {url|path|data_uri}, file_name?}`。
async fn send_media(adapter: &QqAdapter, payload: &Value) -> InvokeOutcome {
    let target = match parse_target(payload, adapter.name()) {
        Ok(target) => target,
        Err((code, message)) => return error_outcome(code, message),
    };
    let source = payload.get("source").cloned().unwrap_or(Value::Null);
    // source 接受字符串（直接给 URL / 路径）或 `{url|path|data_uri}` 对象。
    let (url, path, data_uri, bare) = match &source {
        Value::String(s) => (None, None, None, Some(s.clone())),
        Value::Object(map) => (
            map.get("url").and_then(Value::as_str).map(str::to_string),
            map.get("path").and_then(Value::as_str).map(str::to_string),
            map.get("data_uri").and_then(Value::as_str).map(str::to_string),
            None,
        ),
        _ => (None, None, None, None),
    };
    match payload.get("kind").and_then(Value::as_str) {
        Some("image") => {
            let file = if let Some(file) = url.or(path).or(bare) {
                file
            } else if let Some(uri) = data_uri {
                match data_uri_to_onebot_source(&uri) {
                    Ok(file) => file,
                    Err((code, message)) => return error_outcome(code, message),
                }
            } else {
                return error_outcome("invalid_payload", "send_media requires source url|path|data_uri");
            };
            match adapter.send_image(&target, &file).await {
                Ok(result) => ok_result(json!({
                    "message_id": result.message_id,
                    "success": result.success,
                    "error": result.error,
                })),
                Err(e) => error_outcome("send_failed", e.to_string()),
            }
        }
        Some("file") => {
            // 文件上传要求同机本地路径（NapCat 经 file_bridge 桥接）；
            // URL / data URI 形态暂不支持（先 store_media 落盘再传 path）。
            let path = path.or(bare).filter(|p| !p.trim().is_empty());
            let Some(path) = path.filter(|_| url.is_none()) else {
                return error_outcome(
                    "unsupported_source",
                    "file upload requires a local path source (url/data_uri not supported)",
                );
            };
            if path.starts_with("http://") || path.starts_with("https://") {
                return error_outcome(
                    "unsupported_source",
                    "file upload requires a local path source (url/data_uri not supported)",
                );
            }
            let file_name = payload
                .get("file_name")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    std::path::Path::new(&path)
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                })
                .unwrap_or_else(|| "file".to_string());
            let result = match &target.channel {
                ChannelType::Direct => {
                    let Ok(user_id) = target.user_id.parse::<i64>() else {
                        return error_outcome("invalid_payload", "invalid user_id");
                    };
                    adapter.upload_private_file(user_id, &path, &file_name).await
                }
                ChannelType::Group { group_id } => {
                    let Ok(group_id) = group_id.parse::<i64>() else {
                        return error_outcome("invalid_payload", "invalid group_id");
                    };
                    adapter.upload_group_file(group_id, &path, &file_name).await
                }
            };
            match result {
                Ok(detail) => ok_result(json!({"success": true, "detail": detail})),
                Err(e) => error_outcome("send_failed", e),
            }
        }
        other => error_outcome(
            "invalid_payload",
            format!("unknown media kind {other:?} (expected image|file)"),
        ),
    }
}

/// 平台专属动作（门控 / 名单 / owner / 登录 / 目录）透传到
/// [`AdapterManagement::invoke_custom`]。
async fn custom_op(adapter: &QqAdapter, method: &str, payload: Value) -> InvokeOutcome {
    match adapter.invoke_custom(method, payload).await {
        Ok(result) => ok_result(result),
        Err(e) => error_outcome("adapter_error", e.to_string()),
    }
}

/// `data:` URI → OneBot `base64://` 文件参数（NapCat 直接接收）。
fn data_uri_to_onebot_source(uri: &str) -> Result<String, (String, String)> {
    let unsupported = |msg: &str| ("unsupported_source".to_string(), msg.to_string());
    let rest = uri
        .strip_prefix("data:")
        .ok_or_else(|| unsupported("data_uri must start with `data:`"))?;
    let Some(comma) = rest.find(',') else {
        return Err(unsupported("data_uri is missing the `,` separator"));
    };
    let (meta, body) = rest.split_at(comma);
    let body = &body[1..];
    if !meta.contains("base64") {
        return Err(unsupported("only base64 data URIs are supported"));
    }
    if body.is_empty() {
        return Err(unsupported("data_uri payload is empty"));
    }
    Ok(format!("base64://{body}"))
}

/// `AdapterConnectionState` → 线格式状态字符串（`status` op）。
fn state_str(state: AdapterConnectionState) -> &'static str {
    match state {
        AdapterConnectionState::Stopped => "stopped",
        AdapterConnectionState::Connected => "connected",
        AdapterConnectionState::Disconnected => "disconnected",
        AdapterConnectionState::Starting => "starting",
        AdapterConnectionState::Stopping => "stopping",
        AdapterConnectionState::Error => "error",
    }
}

/// 成功终态：结构化结果经 `result` 承载（`Ok.text` 对适配器 op 忽略）。
fn ok_result(result: Value) -> InvokeOutcome {
    InvokeOutcome::Ok {
        text: String::new(),
        images: Vec::new(),
        result,
    }
}

/// 错误终态。
fn error_outcome(code: impl Into<String>, message: impl Into<String>) -> InvokeOutcome {
    InvokeOutcome::Error {
        code: code.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use echo_adapter::types::IncomingFile;
    use echo_plugin_sdk::echo_plugin_api::{
        Contribution, Hello, HostCallOutcome, HostCallResult, HostToPlugin, Invoke, PluginToHost,
        PROTOCOL_NAME, PROTOCOL_VERSION,
    };
    use echo_plugin_sdk::{serve_io_with, AdapterHandler, ServeOptions};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    /// 假宿主：帧读写 + 手机会话（`serve_io_with` 的客户端侧）。
    struct FakeHost {
        stream: DuplexStream,
        session: tokio::task::JoinHandle<std::io::Result<()>>,
    }

    impl FakeHost {
        fn start(plugin: QqPlugin) -> (Self, QqPlugin) {
            let (server, client) = tokio::io::duplex(256 * 1024);
            let (reader, writer) = tokio::io::split(server);
            let handle = plugin.clone();
            let session = tokio::spawn(serve_io_with(
                AdapterHandler::new(plugin),
                ServeOptions::default(),
                reader,
                writer,
            ));
            (
                Self {
                    stream: client,
                    session,
                },
                handle,
            )
        }

        async fn send(&mut self, msg: &HostToPlugin) {
            let body = serde_json::to_vec(msg).expect("encode frame");
            let mut frame = Vec::with_capacity(4 + body.len());
            frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
            frame.extend_from_slice(&body);
            self.stream.write_all(&frame).await.expect("write frame");
        }

        async fn recv(&mut self) -> PluginToHost {
            let mut len_bytes = [0u8; 4];
            self.stream
                .read_exact(&mut len_bytes)
                .await
                .expect("read frame length");
            let mut body = vec![0u8; u32::from_le_bytes(len_bytes) as usize];
            self.stream
                .read_exact(&mut body)
                .await
                .expect("read frame body");
            serde_json::from_slice(&body).expect("decode frame")
        }

        /// 握手并断言注册的适配器贡献（实例名 + 能力位）。
        async fn handshake(&mut self, config: Value, expected_adapter: &str) {
            self.send(&HostToPlugin::Hello(Hello {
                protocol: PROTOCOL_NAME.to_string(),
                version: PROTOCOL_VERSION,
                plugin_id: "qq-plugin-test".to_string(),
                config,
            }))
            .await;
            assert!(matches!(self.recv().await, PluginToHost::Welcome(_)));
            match self.recv().await {
                PluginToHost::Register(register) => {
                    let adapter = register
                        .contributions
                        .iter()
                        .find_map(|c| match c {
                            Contribution::Adapter(a) => Some(a),
                            _ => None,
                        })
                        .expect("adapter contribution registered");
                    assert_eq!(adapter.name, expected_adapter);
                    assert_eq!(adapter.platform, "qq");
                    let caps = &adapter.capabilities;
                    assert!(
                        caps.send_text
                            && caps.send_media
                            && caps.groups
                            && caps.friends
                            && caps.gating
                            && caps.login,
                        "all qq capabilities expected: {caps:?}"
                    );
                }
                other => panic!("expected Register, got {other:?}"),
            }
            assert_eq!(self.recv().await, PluginToHost::Ready);
        }

        /// 发一个 Invoke 并读回结果。
        async fn invoke(&mut self, contribution: &str, payload: Value) -> InvokeOutcome {
            self.send(&HostToPlugin::Invoke(Invoke {
                call_id: format!("test:{contribution}"),
                contribution: contribution.to_string(),
                ctx: Default::default(),
                payload,
            }))
            .await;
            match self.recv().await {
                PluginToHost::InvokeResult(result) => result.outcome,
                other => panic!("expected InvokeResult, got {other:?}"),
            }
        }

        /// 读一个插件发出的 HostCall 并回执。
        async fn answer_host_call(&mut self, expected_method: &str) -> Value {
            match self.recv().await {
                PluginToHost::HostCall(call) => {
                    assert_eq!(call.service, "adapter_host");
                    assert_eq!(call.method, expected_method);
                    self.send(&HostToPlugin::HostCallResult(HostCallResult {
                        call_id: call.call_id.clone(),
                        outcome: HostCallOutcome::Ok {
                            result: json!({"accepted": true}),
                        },
                    }))
                    .await;
                    call.payload
                }
                other => panic!("expected HostCall, got {other:?}"),
            }
        }
    }

    fn example_message() -> IncomingMessage {
        IncomingMessage {
            adapter_name: "qq-test".to_string(),
            platform: "qq".to_string(),
            user_id: "123".to_string(),
            user_name: "Alice".to_string(),
            channel: ChannelType::Group {
                group_id: "456".to_string(),
            },
            group_name: Some("开发群".to_string()),
            content: "hello".to_string(),
            timestamp: 1733814154,
            at_me: true,
            metadata: json!({"message_id": 7}),
            images: vec!["/media/ab12.png".to_string()],
            files: vec![IncomingFile {
                name: "report.pdf".to_string(),
                path: Some("/tmp/report.pdf".to_string()),
                size: 42,
                error: None,
            }],
        }
    }

    /// `message_to_wire`：入站消息 → `deliver_message` 线格式（字段完整）。
    #[test]
    fn message_to_wire_conversion() {
        let wire = message_to_wire(&example_message());
        assert_eq!(wire["adapter"], json!("qq-test"));
        assert_eq!(wire["channel"], json!({"type": "group", "group_id": "456"}));
        assert_eq!(wire["user_name"], json!("Alice"));
        assert_eq!(wire["at_me"], json!(true));
        assert_eq!(wire["metadata"], json!({"message_id": 7}));
        assert_eq!(wire["images"], json!(["/media/ab12.png"]));
        assert_eq!(wire["files"][0]["name"], json!("report.pdf"));
        assert_eq!(wire["files"][0]["size"], json!(42));

        // 私聊 + 空显示名 → user_name 省略（None）。
        let mut dm = example_message();
        dm.channel = ChannelType::Direct;
        dm.user_name = String::new();
        let wire = message_to_wire(&dm);
        assert_eq!(wire["channel"], json!({"type": "dm"}));
        assert_eq!(wire["user_name"], Value::Null);
    }

    /// `data:` URI → `base64://`（NapCat 文件参数）。
    #[test]
    fn data_uri_to_onebot_source_conversion() {
        assert_eq!(
            data_uri_to_onebot_source("data:image/png;base64,AAAA").unwrap(),
            "base64://AAAA"
        );
        assert!(data_uri_to_onebot_source("data:image/png,plain").is_err());
        assert!(data_uri_to_onebot_source("http://x/y.png").is_err());
        assert!(data_uri_to_onebot_source("data:image/png;base64,").is_err());
    }

    /// 全链路（内存流）：注册 → custom（门控 / owner）→ status →
    /// send_text 失败语义 → 入站投递（HostCall）→ 连接态上报。
    #[tokio::test]
    async fn plugin_end_to_end_over_duplex() {
        let (mut host, plugin) = FakeHost::start(QqPlugin::new());
        host.handshake(json!({"name": "qq-test", "persona": "tester"}), "qq-test")
            .await;

        // 内建 op：不需要网络连接即可工作。
        match host.invoke("qq-test:status", json!({})).await {
            InvokeOutcome::Ok { result, .. } => {
                assert_eq!(result["state"], json!("stopped"));
            }
            other => panic!("status failed: {other:?}"),
        }

        // custom：owner 读写（persist 无 state_file，仅内存）。
        match host
            .invoke(
                "qq-test:custom",
                json!({"method": "set_owner", "payload": {"owner_qq": 10001}}),
            )
            .await
        {
            InvokeOutcome::Ok { result, .. } => assert_eq!(result["owner_qq"], json!(10001)),
            other => panic!("set_owner failed: {other:?}"),
        }
        match host
            .invoke("qq-test:custom", json!({"method": "get_owner"}))
            .await
        {
            InvokeOutcome::Ok { result, .. } => assert_eq!(result["owner_qq"], json!(10001)),
            other => panic!("get_owner failed: {other:?}"),
        }

        // custom：门控模式往返。
        match host
            .invoke(
                "qq-test:custom",
                json!({"method": "set_gate_mode", "payload": {"mode": "allowlist"}}),
            )
            .await
        {
            InvokeOutcome::Ok { result, .. } => assert_eq!(result["mode"], json!("allowlist")),
            other => panic!("set_gate_mode failed: {other:?}"),
        }

        // 未知 op / 未知 custom 方法：可读错误。
        match host.invoke("qq-test:nope", json!({})).await {
            InvokeOutcome::Error { code, .. } => assert_eq!(code, "unknown_op"),
            other => panic!("expected unknown_op, got {other:?}"),
        }
        match host
            .invoke("qq-test:custom", json!({"method": "no_such_method"}))
            .await
        {
            InvokeOutcome::Error { code, .. } => assert_eq!(code, "adapter_error"),
            other => panic!("expected adapter_error, got {other:?}"),
        }

        // send_text：无连接 → send_failed（可读错误，不是 panic）。
        match host
            .invoke(
                "qq-test:send_text",
                json!({"target": {"channel": {"type": "dm"}, "user_id": "123"}, "content": "hi"}),
            )
            .await
        {
            InvokeOutcome::Error { code, message } => {
                assert_eq!(code, "send_failed");
                assert!(message.contains("no QQ connection"), "{message}");
            }
            other => panic!("expected send_failed, got {other:?}"),
        }

        // send_media：dm 缺 user_id → invalid_payload；file 缺 path → unsupported_source。
        match host
            .invoke(
                "qq-test:send_media",
                json!({"target": {"channel": {"type": "dm"}}, "kind": "image",
                       "source": {"path": "/tmp/x.png"}}),
            )
            .await
        {
            InvokeOutcome::Error { code, .. } => assert_eq!(code, "invalid_payload"),
            other => panic!("expected invalid_payload, got {other:?}"),
        }
        match host
            .invoke(
                "qq-test:send_media",
                json!({"target": {"channel": {"type": "group", "group_id": "456"}}, "kind": "file",
                       "source": {"url": "https://x/y.pdf"}}),
            )
            .await
        {
            InvokeOutcome::Error { code, .. } => assert_eq!(code, "unsupported_source"),
            other => panic!("expected unsupported_source, got {other:?}"),
        }

        // 入站投递：hook（on_ready 已装）→ HostCall deliver_message。
        let adapter = plugin.adapter_handle().expect("adapter built");
        let hook = adapter
            .message_hook()
            .expect("hook installed by on_ready");
        let deliver = tokio::spawn(async move { hook.on_incoming_message(example_message()).await });
        let payload = host.answer_host_call("deliver_message").await;
        assert_eq!(payload["message"]["adapter"], json!("qq-test"));
        assert_eq!(payload["message"]["content"], json!("hello"));
        assert!(deliver.await.expect("join").is_ok());

        // 连接态上报：report_state（spawn 的旁路任务）。
        adapter
            .message_hook()
            .expect("hook")
            .on_connection_state("qq-test", true, Some("4242".to_string()));
        let payload = host.answer_host_call("report_state").await;
        assert_eq!(payload["adapter"], json!("qq-test"));
        assert_eq!(payload["connected"], json!(true));
        assert_eq!(payload["self_id"], json!("4242"));

        // 收尾：断流 → 会话干净退出。
        let FakeHost { stream, session } = host;
        drop(stream);
        let result = tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .expect("session should finish after stream close")
            .expect("session task should not panic");
        result.expect("session should exit cleanly");
    }

    /// `Hello.config` 非法 → `Failed{invalid_config}`，会话以错误退出。
    #[tokio::test]
    async fn invalid_config_fails_handshake() {
        let (mut host, _plugin) = FakeHost::start(QqPlugin::new());
        host.send(&HostToPlugin::Hello(Hello {
            protocol: PROTOCOL_NAME.to_string(),
            version: PROTOCOL_VERSION,
            plugin_id: "qq-plugin-test".to_string(),
            config: json!({"name": 5}),
        }))
        .await;
        match host.recv().await {
            PluginToHost::Failed(failure) => assert_eq!(failure.code, "invalid_config"),
            other => panic!("expected Failed, got {other:?}"),
        }
        let FakeHost { stream, session } = host;
        drop(stream);
        let result = tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .expect("session should finish after config rejection")
            .expect("session task should not panic");
        assert!(result.is_err(), "rejected config must surface as an error");
    }
}
