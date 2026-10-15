//! NapCat WebUI API client.
//!
//! Provides programmatic access to NapCat's management API:
//! - Auto-configure reverse WebSocket client
//! - Check login / connection status
//! - Fetch QR code for TUI display
//! - Health check
//!
//! API base URL defaults to `http://localhost:6099` (NapCat WebUI).

pub mod service;

use serde::Deserialize;
use serde_json::Value;

/// NapCat API client.
pub struct NapCatClient {
    base_url: String,
    /// OneBot HTTP API base (default `http://localhost:3000`, overridable for tests).
    onebot_url: String,
    client: reqwest::Client,
}

/// Login / connection status returned by NapCat.
#[derive(Debug, Clone, Deserialize)]
pub struct NapCatStatus {
    /// Whether QQ is logged in.
    #[serde(default)]
    pub online: bool,
    /// QQ user ID if logged in. Some NapCat versions report it as an
    /// integer — normalised to a string.
    #[serde(default, deserialize_with = "deserialize_optional_id")]
    pub user_id: Option<String>,
    /// QQ nickname.
    #[serde(default)]
    pub nickname: Option<String>,
    /// Raw status string for fallback.
    #[serde(default)]
    pub message: Option<String>,
}

/// Deserialise an optional user id that may arrive as string or integer.
fn deserialize_optional_id<'de, D>(d: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize as _;
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::String(s)) => Some(s),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        Some(serde_json::Value::Null) | None => None,
        _ => None,
    })
}

fn json_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

/// NapCat connection result.
#[derive(Debug)]
pub enum NapCatState {
    /// QQ is logged in and connected.
    Online { user_id: String, nickname: String },
    /// NapCat is running but not logged into QQ.
    WaitingForLogin,
    /// NapCat is not reachable (container not running or port not exposed).
    Unreachable,
}

/// `send().await.ok()` 响应体的 JSON 解析（async 版）。
///
/// 此前用 `block_in_place + Handle::block_on` 在 and_then 闭包里同步
/// 等 JSON——current_thread runtime 直接 panic，multi_thread 也是反
/// 模式（2026-10 巡检 P2）。本函数等价纯 async：None → None。
async fn json_body(resp: Option<reqwest::Response>) -> Option<serde_json::Value> {
    match resp {
        Some(r) => r.json::<serde_json::Value>().await.ok(),
        None => None,
    }
}

impl NapCatClient {
    /// Create a new NapCat API client.
    /// `webui_url` is the NapCat WebUI address, e.g. `http://localhost:6099`.
    pub fn new(webui_url: &str) -> Self {
        Self::with_onebot_url(webui_url, "http://localhost:3000")
    }

    /// Like [`new`](Self::new), with an explicit OneBot HTTP API URL
    /// (used by tests to point at a mock server).
    pub fn with_onebot_url(webui_url: &str, onebot_url: &str) -> Self {
        Self {
            base_url: webui_url.trim_end_matches('/').to_string(),
            onebot_url: onebot_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "reqwest builder failed, using default client");
                    reqwest::Client::new()
                }),
        }
    }

    // ---- health / status ----

    /// Check if NapCat is reachable and logged in.
    /// Returns `NapCatState` summarising the result.
    pub async fn check(&self) -> NapCatState {
        match self.login_status().await {
            Ok(status) if status.online => NapCatState::Online {
                user_id: status.user_id.unwrap_or_else(|| "?".into()),
                nickname: status.nickname.unwrap_or_else(|| "?".into()),
            },
            Ok(_) => NapCatState::WaitingForLogin,
            Err(_) => NapCatState::Unreachable,
        }
    }

    /// Query NapCat's login status.
    ///
    /// 优先使用 OneBot HTTP API（docker-compose 将容器 3000 端口映射到宿主机）。
    /// 这与 TUI 的 `/qq login` 完全一致，已验证能正确识别登录状态。
    /// 仅当 OneBot API 不可达时，才回退到 NapCat WebUI API。
    pub async fn login_status(&self) -> Result<NapCatStatus, String> {
        // 1) 优先 OneBot HTTP API —— 与 /qq login 行为一致
        match self.login_status_via_onebot().await {
            Ok(status) => return Ok(status),
            Err(e) => tracing::debug!(error = %e, "OneBot API unavailable, trying WebUI"),
        }

        // 2) 回退：NapCat WebUI /api/login/status
        // 注意：该接口的 data 里登录字段名（online/isLogin 等）在不同版本不一致，
        // 且未带 token 时可能返回 code!=0，因此仅作 fallback。
        let url = format!("{}/api/login/status", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("HTTP error: {e}"))?;
        if resp.status().is_success() {
            let raw: serde_json::Value =
                resp.json().await.map_err(|e| format!("parse error: {e}"))?;
            let code = raw.get("code").and_then(|c| c.as_i64());
            if code == Some(0) {
                if let Some(data) = raw.get("data") {
                    let status: NapCatStatus = serde_json::from_value(data.clone())
                        .map_err(|e| format!("data parse error: {e}"))?;
                    if status.online {
                        return Ok(status);
                    }
                    tracing::warn!(body = %raw, "WebUI reports offline — trusting OneBot API result");
                }
            }
            tracing::warn!(body = %raw, "NapCat WebUI status response not usable");
        }
        // 3) 最终再尝试一次 OneBot API
        self.login_status_via_onebot().await
    }

    async fn login_status_via_onebot(&self) -> Result<NapCatStatus, String> {
        // OneBot HTTP API is typically on port 3000 inside the container.
        // Try localhost:3000 (common port mapping).
        let url = format!("{}/get_login_info", self.onebot_url);
        let resp = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .body("{}")
            .send()
            .await
            .map_err(|e| format!("OneBot HTTP error: {e}"))?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("OneBot parse error: {e}"))?;
        let data = &body["data"];
        let user_id = data["user_id"].as_i64().map(|id| id.to_string());
        let nickname = data["nickname"].as_str().map(|s| s.to_string());
        let online = user_id.is_some() && user_id.as_deref() != Some("0");
        Ok(NapCatStatus {
            online,
            user_id,
            nickname,
            message: Some("via OneBot API".into()),
        })
    }

    async fn onebot_action(&self, action: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/{action}", self.onebot_url);
        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body("{}")
            .send()
            .await
            .map_err(|e| format!("OneBot {action} HTTP error: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("OneBot {action} returned HTTP {status}"));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("OneBot {action} parse error: {e}"))?;
        let retcode = body.get("retcode").and_then(json_i64).unwrap_or(0);
        if retcode != 0 || body.get("status").and_then(|v| v.as_str()) == Some("failed") {
            let message = body
                .get("message")
                .or_else(|| body.get("wording"))
                .and_then(|value| value.as_str())
                .unwrap_or("unknown OneBot error");
            return Err(format!("OneBot {action} failed ({retcode}): {message}"));
        }
        body.get("data")
            .cloned()
            .ok_or_else(|| format!("OneBot {action} response has no data"))
    }

    /// Fetch every group visible to the logged-in QQ account.
    pub async fn get_group_list(&self) -> Result<Vec<(i64, String)>, String> {
        let data = self.onebot_action("get_group_list").await?;
        let groups = data
            .as_array()
            .ok_or("OneBot get_group_list data is not an array")?
            .iter()
            .filter_map(|item| {
                let id = json_i64(item.get("group_id")?)?;
                let name = item
                    .get("group_name")
                    .and_then(|value| value.as_str())
                    .unwrap_or("(unknown)")
                    .to_string();
                Some((id, name))
            })
            .collect();
        Ok(groups)
    }

    /// Fetch every friend visible to the logged-in QQ account.
    pub async fn get_friend_list(&self) -> Result<Vec<(i64, String)>, String> {
        let data = self.onebot_action("get_friend_list").await?;
        let friends = data
            .as_array()
            .ok_or("OneBot get_friend_list data is not an array")?
            .iter()
            .filter_map(|item| {
                let id = json_i64(item.get("user_id")?)?;
                let name = item
                    .get("nickname")
                    .and_then(|value| value.as_str())
                    .unwrap_or("(unknown)")
                    .to_string();
                Some((id, name))
            })
            .collect();
        Ok(friends)
    }

    // ---- reverse WebSocket configuration ----

    /// Log into the NapCat WebUI with `webui.json`'s token and return the
    /// short-lived credential used by the WebUI API.
    /// 确保 NapCat「启动时自动登录」已开启：
    /// 1. WebUI CheckLoginStatus：已登录则把该账号写入 webui.json 的
    ///    `autoLoginAccount`（容器内 sed 原地改，重启容器后 NapCat 直接快速登录）；
    /// 2. 未登录则调用 SetQuickLogin 拉快速登录（失败忽略——回退到扫码流程）。
    ///
    /// 在适配器启动（NapCat 容器就绪后）调用一次；失败只告警不阻塞启动。
    pub async fn ensure_quick_login(&self, container_name: &str, webui_token: &str) {
        let credential = match self.webui_login(webui_token).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(%e, "NapCat webui login failed; skip quick-login ensure");
                return;
            }
        };

        // 已登录？→ 把当前账号固化到 webui.json 的 autoLoginAccount。
        let status_url = format!("{}/api/QQLogin/CheckLoginStatus", self.base_url);
        let resp = self
            .client
            .post(&status_url)
            .bearer_auth(&credential)
            .json(&serde_json::json!({}))
            .send()
            .await
            .ok();
        let is_login = json_body(resp)
            .await
            .and_then(|body: Value| body.pointer("/data/isLogin").and_then(|v| v.as_bool()))
            .unwrap_or(false);

        let uin: Option<String> = if is_login {
            // 登录中：取快速登录列表第一个账号（NapCat 只支持单账号）。
            let list_url = format!("{}/api/QQLogin/GetQuickLoginList", self.base_url);
            json_body(
                self.client
                    .post(&list_url)
                    .bearer_auth(&credential)
                    .json(&serde_json::json!({}))
                    .send()
                    .await
                    .ok(),
            )
            .await
            .and_then(|body: Value| {
                body.get("data")
                    .and_then(|d| d.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|u| {
                        u.as_str()
                            .map(str::to_string)
                            .or_else(|| u.as_i64().map(|n| n.to_string()))
                    })
            })
        } else {
            None
        };

        let container = container_name.to_string();
        if is_login {
            if let Some(uin) = uin {
                let uin_for_log = uin.clone();
                let written = tokio::task::spawn_blocking(move || {
                    ensure_autologin_in_container(&container, &uin)
                })
                .await;
                match written {
                    Ok(Ok(())) => {
                        tracing::info!(uin = %uin_for_log, "NapCat autoLoginAccount ensured")
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "write autoLoginAccount failed")
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "write autoLoginAccount task failed")
                    }
                }
            }
            return;
        }

        // 未登录：尝试快速登录（登录态已失效/被踢时由扫码流程兜底）。
        let set_url = format!("{}/api/QQLogin/SetQuickLogin", self.base_url);
        let body = json_body(
            self.client
                .post(&set_url)
                .bearer_auth(&credential)
                .json(&serde_json::json!({}))
                .send()
                .await
                .ok(),
        )
        .await;
        match body {
            Some(b) if b.get("code").and_then(|c| c.as_i64()) == Some(0) => {
                tracing::info!("NapCat quick login succeeded");
            }
            other => {
                tracing::info!(
                    ?other,
                    "NapCat quick login unavailable; fall back to QR flow"
                );
            }
        }
    }

    async fn webui_login(&self, webui_token: &str) -> Result<String, String> {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(format!("{webui_token}.napcat").as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        let url = format!("{}/api/auth/login", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "hash": hash, "totpCode": "" }))
            .send()
            .await
            .map_err(|e| format!("NapCat WebUI login HTTP error: {e}"))?;

        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("NapCat WebUI login parse error: {e}"))?;
        let code = body.get("code").and_then(|c| c.as_i64());
        if code != Some(0) {
            return Err(format!(
                "NapCat WebUI login failed: {}",
                body.get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error")
            ));
        }
        body.pointer("/data/Credential")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| "NapCat WebUI login response missing Credential".to_string())
    }

    /// Auto-configure NapCat to connect back to our reverse WebSocket server.
    ///
    /// `ws_url` is the address NapCat should connect to, e.g.
    /// `ws://host.docker.internal:3131`. `token` is the reverse-WS access
    /// token configured in `[adapters.qq.server]`; `webui_token` is the
    /// NapCat WebUI token from `webui.json`.
    pub async fn configure_reverse_ws(
        &self,
        ws_url: &str,
        token: &str,
        webui_token: &str,
    ) -> Result<(), String> {
        let credential = self.webui_login(webui_token).await?;

        // Fetch the current OneBot 11 network config via the NapCat v4 WebUI API.
        let get_url = format!("{}/api/OB11Config/GetConfig", self.base_url);
        let get_resp = self
            .client
            .post(&get_url)
            .bearer_auth(&credential)
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| format!("NapCat OB11Config GetConfig HTTP error: {e}"))?;
        let get_body: Value = get_resp
            .json()
            .await
            .map_err(|e| format!("NapCat OB11Config GetConfig parse error: {e}"))?;
        if get_body.get("code").and_then(|c| c.as_i64()) != Some(0) {
            return Err(format!(
                "NapCat OB11Config GetConfig failed: {}",
                get_body
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error")
            ));
        }

        let mut config = get_body
            .get("data")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        if !config.is_object() {
            config = serde_json::json!({});
        }

        let network = config
            .as_object_mut()
            .ok_or_else(|| "NapCat OB11 config is not an object".to_string())?
            .entry("network")
            .or_insert_with(|| serde_json::json!({}));
        let clients = network
            .as_object_mut()
            .ok_or_else(|| "NapCat OB11 network is not an object".to_string())?
            .entry("websocketClients")
            .or_insert_with(|| serde_json::json!([]));
        let clients = clients
            .as_array_mut()
            .ok_or_else(|| "NapCat websocketClients is not an array".to_string())?;
        clients
            .retain(|client| client.get("name").and_then(|n| n.as_str()) != Some("EchoAgentCore"));
        clients.push(serde_json::json!({
            "enable": true,
            "name": "EchoAgentCore",
            "url": ws_url,
            "reportSelfMessage": false,
            "messagePostFormat": "array",
            "token": token,
            "debug": false,
            "heartInterval": 30000,
            "reconnectInterval": 30000,
            "verifyCertificate": true,
        }));

        let set_url = format!("{}/api/OB11Config/SetConfig", self.base_url);
        let set_resp = self
            .client
            .post(&set_url)
            .bearer_auth(&credential)
            .json(&serde_json::json!({ "config": config.to_string() }))
            .send()
            .await
            .map_err(|e| format!("NapCat OB11Config SetConfig HTTP error: {e}"))?;
        let set_body: Value = set_resp
            .json()
            .await
            .map_err(|e| format!("NapCat OB11Config SetConfig parse error: {e}"))?;
        if set_body.get("code").and_then(|c| c.as_i64()) == Some(0) {
            tracing::info!(%ws_url, "NapCat reverse WS configured");
            Ok(())
        } else {
            Err(format!(
                "NapCat OB11Config SetConfig failed: {}",
                set_body
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error")
            ))
        }
    }

    // ---- QR code ----

    /// Fetch the login QR code PNG from NapCat's container via Docker.
    /// Returns raw PNG bytes, or an error if Docker/NapCat is not available.
    pub fn fetch_qrcode_docker(container_name: &str) -> Result<Vec<u8>, String> {
        use std::process::Command;
        let output = Command::new("docker")
            .args([
                "exec",
                container_name,
                "cat",
                "/app/napcat/cache/qrcode.png",
            ])
            .output()
            .map_err(|e| format!("docker exec failed: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "docker exec failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        if output.stdout.is_empty() {
            return Err("QR code not yet available (waiting for NapCat to generate)".into());
        }
        Ok(output.stdout)
    }

    /// Try to fetch QR code via the WebUI API.
    pub async fn fetch_qrcode_web(&self) -> Result<Vec<u8>, String> {
        let url = format!("{}/api/qrcode", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("HTTP error: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| format!("read error: {e}"))
    }

    /// Age (seconds) of the QR PNG inside the container; `None` when the file
    /// is missing or Docker/the container is unreachable.
    pub fn qrcode_age_secs(container_name: &str) -> Option<u64> {
        use std::process::Command;
        let output = Command::new("docker")
            .args([
                "exec",
                container_name,
                "stat",
                "-c",
                "%Y",
                "/app/napcat/cache/qrcode.png",
            ])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let mtime: u64 = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .ok()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs();
        Some(now.saturating_sub(mtime))
    }

    /// Ask NapCat to regenerate the login QR code (WebUI `/api/QQLogin/RefreshQRcode`).
    ///
    /// NapCat's own auto-refresh loop can stall (observed: stops regenerating
    /// after a while, leaving an expired QR that "refresh" in the panel cannot
    /// fix because Core would just re-read the stale file). This call forces a
    /// fresh QR on demand. The WebUI credential is derived from the container's
    /// `webui.json` token: `sha256(token + ".napcat")` → `/api/auth/login`.
    pub async fn refresh_qrcode(&self, container_name: &str) -> Result<(), String> {
        let container = container_name.to_string();
        let token = tokio::task::spawn_blocking(move || webui_token_from_container(&container))
            .await
            .map_err(|e| format!("token task failed: {e}"))??;
        self.refresh_qrcode_with_token(&token).await
    }

    /// Same as [`refresh_qrcode`](Self::refresh_qrcode) but takes the WebUI
    /// token directly (pure HTTP — unit-testable without Docker).
    pub async fn refresh_qrcode_with_token(&self, token: &str) -> Result<(), String> {
        let hash = sha256_hex(&format!("{token}.napcat"));

        // 1) WebUI 登录换取 Credential
        let login_url = format!("{}/api/auth/login", self.base_url);
        let login: Value = self
            .client
            .post(&login_url)
            .json(&serde_json::json!({ "hash": hash, "totpCode": "" }))
            .send()
            .await
            .map_err(|e| format!("NapCat WebUI 登录失败: {e}"))?
            .json()
            .await
            .map_err(|e| format!("NapCat WebUI 登录响应解析失败: {e}"))?;
        let credential = login
            .pointer("/data/Credential")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("NapCat WebUI 登录响应缺少 Credential: {login}"))?;

        // 2) 请求刷新二维码
        let refresh_url = format!("{}/api/QQLogin/RefreshQRcode", self.base_url);
        let body: Value = self
            .client
            .post(&refresh_url)
            .bearer_auth(credential)
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| format!("无法刷新 NapCat 二维码: {e}"))?
            .json()
            .await
            .map_err(|e| format!("NapCat 刷新响应解析失败: {e}"))?;
        if body.get("code").and_then(|c| c.as_i64()) != Some(0) {
            return Err(format!("NapCat 刷新二维码失败: {body}"));
        }
        Ok(())
    }
}

/// Hex SHA-256 of the input (used for the NapCat WebUI credential hash).
fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Read the NapCat WebUI token from the container's `webui.json`.
/// 在容器内把账号写入 webui.json 的 autoLoginAccount（sed 原地替换；
/// 覆盖空值与旧值两种形态）。
fn ensure_autologin_in_container(container_name: &str, uin: &str) -> Result<(), String> {
    let script = format!(
        "sed -i -E 's/\"autoLoginAccount\": \"[^\"]*\"/\"autoLoginAccount\": \"{uin}\"/' /app/napcat/config/webui.json"
    );
    let out = std::process::Command::new("docker")
        .args(["exec", container_name, "sh", "-c", &script])
        .output()
        .map_err(|e| format!("docker exec failed: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "docker exec sed failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

pub fn webui_token_from_container(container_name: &str) -> Result<String, String> {
    use std::process::Command;
    let output = Command::new("docker")
        .args([
            "exec",
            container_name,
            "cat",
            "/app/napcat/config/webui.json",
        ])
        .output()
        .map_err(|e| format!("无法读取 NapCat webui.json: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "无法读取 NapCat webui.json: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let body: Value =
        serde_json::from_slice(&output.stdout).map_err(|e| format!("webui.json 解析失败: {e}"))?;
    body.get("token")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| "webui.json 缺少 token".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn onebot_mock(data: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/get_login_info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "retcode": 0,
                "data": data,
            })))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn check_reports_online_when_onebot_has_user() {
        let server = onebot_mock(serde_json::json!({"user_id": 123456, "nickname": "bot"})).await;
        let client = NapCatClient::with_onebot_url("http://localhost:9999", &server.uri());
        match client.check().await {
            NapCatState::Online { user_id, nickname } => {
                assert_eq!(user_id, "123456");
                assert_eq!(nickname, "bot");
            }
            other => panic!("expected Online, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn check_reports_waiting_when_not_logged_in() {
        let server = onebot_mock(serde_json::json!({"user_id": 0})).await;
        let client = NapCatClient::with_onebot_url("http://localhost:9999", &server.uri());
        assert!(matches!(client.check().await, NapCatState::WaitingForLogin));
    }

    #[tokio::test]
    async fn check_reports_unreachable_when_both_apis_fail() {
        // OneBot mock that always fails + a WebUI that is unreachable.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/get_login_info"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let client = NapCatClient::with_onebot_url("http://localhost:1", &server.uri());
        assert!(matches!(client.check().await, NapCatState::Unreachable));
    }

    #[tokio::test]
    async fn login_status_falls_back_to_webui() {
        // OneBot fails; WebUI reports online via /api/login/status.
        let onebot = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/get_login_info"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&onebot)
            .await;

        let webui = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/login/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": {"online": true, "user_id": 777, "nickname": "webui-bot"}
            })))
            .mount(&webui)
            .await;

        let client = NapCatClient::with_onebot_url(&webui.uri(), &onebot.uri());
        let status = client.login_status().await.expect("fallback works");
        assert!(status.online);
        assert_eq!(status.user_id.as_deref(), Some("777"));
    }

    #[tokio::test]
    async fn get_group_list_returns_all_groups_with_numeric_or_string_ids() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/get_group_list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "retcode": 0,
                "data": [
                    {"group_id": 111, "group_name": "alpha"},
                    {"group_id": "222", "group_name": "beta"}
                ]
            })))
            .mount(&server)
            .await;
        let client = NapCatClient::with_onebot_url("http://localhost:1", &server.uri());

        assert_eq!(
            client.get_group_list().await.unwrap(),
            vec![(111, "alpha".into()), (222, "beta".into())]
        );
    }

    #[tokio::test]
    async fn get_group_list_reports_onebot_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/get_group_list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "failed",
                "retcode": 1404,
                "message": "not logged in",
                "data": null
            })))
            .mount(&server)
            .await;
        let client = NapCatClient::with_onebot_url("http://localhost:1", &server.uri());

        let error = client.get_group_list().await.unwrap_err();
        assert!(error.contains("not logged in"), "{error}");
    }

    #[tokio::test]
    async fn configure_reverse_ws_posts_config() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/auth/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": { "Credential": "test-cred" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/OB11Config/GetConfig"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": { "network": { "websocketClients": [] } }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/OB11Config/SetConfig"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0
            })))
            .mount(&server)
            .await;
        let client = NapCatClient::new(&server.uri());
        client
            .configure_reverse_ws("ws://host.docker.internal:3131", "tok123", "webui-token")
            .await
            .expect("config accepted");
    }

    #[tokio::test]
    async fn configure_reverse_ws_reports_login_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/auth/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": -1,
                "message": "Unauthorized"
            })))
            .mount(&server)
            .await;
        let client = NapCatClient::new(&server.uri());
        let error = client
            .configure_reverse_ws("ws://x:3131", "", "bad-token")
            .await
            .unwrap_err();
        assert!(error.contains("login failed"), "{error}");
    }

    #[tokio::test]
    async fn fetch_qrcode_web_returns_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/qrcode"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1, 2, 3, 4]))
            .mount(&server)
            .await;
        let client = NapCatClient::new(&server.uri());
        let bytes = client.fetch_qrcode_web().await.expect("qrcode");
        assert_eq!(bytes, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn refresh_qrcode_logs_in_and_requests_refresh() {
        use wiremock::matchers::{body_json, header, method, path};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/auth/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": {"Credential": "cred-123"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/QQLogin/RefreshQRcode"))
            .and(header("authorization", "Bearer cred-123"))
            .and(body_json(serde_json::json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "message": "success"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = NapCatClient::new(&server.uri());
        client
            .refresh_qrcode_with_token("e9df24198bdf")
            .await
            .expect("refresh succeeds");
    }

    #[tokio::test]
    async fn refresh_qrcode_surfaces_login_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/auth/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": -1,
                "message": "Unauthorized"
            })))
            .mount(&server)
            .await;
        let client = NapCatClient::new(&server.uri());
        let error = client
            .refresh_qrcode_with_token("token")
            .await
            .expect_err("must fail without Credential");
        assert!(error.contains("Credential"), "unexpected: {error}");
    }

    #[tokio::test]
    async fn refresh_qrcode_surfaces_refresh_rejection() {
        use wiremock::matchers::path;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/auth/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": 0,
                "data": {"Credential": "cred-123"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/QQLogin/RefreshQRcode"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": -1,
                "message": "busy"
            })))
            .mount(&server)
            .await;
        let client = NapCatClient::new(&server.uri());
        let error = client
            .refresh_qrcode_with_token("token")
            .await
            .expect_err("non-zero code must fail");
        assert!(error.contains("刷新二维码失败"), "unexpected: {error}");
    }

    #[test]
    fn credential_hash_matches_napcat_scheme() {
        // 已知向量（本机 webui.json token 实测核对）：
        // python3: hashlib.sha256(b"e9df24198bdf.napcat").hexdigest()
        let hex = sha256_hex("e9df24198bdf.napcat");
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(hex, sha256_hex("e9df24198bdf.napcat"), "deterministic");
        // 与 Python hashlib 实测值对照（NapCat WebUI 前端同款算法）：
        assert_eq!(
            hex,
            "46b719efc18c97a479fffaec3ae293edf561b763e9121bfe094b16f012f9b162"
        );
        // 空输入向量兜底校验实现正确性：
        assert_eq!(
            sha256_hex(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[tokio::test]
    async fn fetch_qrcode_web_errors_on_non_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/qrcode"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let client = NapCatClient::new(&server.uri());
        assert!(client.fetch_qrcode_web().await.is_err());
    }
}
