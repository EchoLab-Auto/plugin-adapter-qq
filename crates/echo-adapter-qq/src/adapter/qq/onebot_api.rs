//! OneBot HTTP API 客户端方法群（出站调用封装）。
//!
//! 从 `adapter/qq/mod.rs` 拆出（框架优化议题 3）：群/好友列表、成员
//! 信息、文件上传等纯 OneBot HTTP 调用——本层**不懂门控**（门控在
//! 调用方：`impl Adapter` 的 get_groups/get_friend_list 与 send 路径）。

use super::*;

impl QqAdapter {
    /// Get ALL groups without gate-mode filtering (privileged — used by TUI).
    pub async fn get_all_groups(&self) -> Result<Vec<(i64, String)>, String> {
        // Add diagnostic logging
        let connected = self.inner.connected.load(Ordering::SeqCst);
        let running = self.inner.running.load(Ordering::SeqCst);
        let has_context = self
            .inner
            .active_context
            .lock()
            .ok()
            .map(|g| g.is_some())
            .unwrap_or(false);

        tracing::info!(
            "get_all_groups called - connected: {}, running: {}, has_context: {}, self_id: {:?}",
            connected,
            running,
            has_context,
            self.inner.self_id.lock().ok()
        );

        // First check if QQ adapter is running
        if !running {
            return Err("QQ适配器未启动，请检查配置".to_string());
        }

        let napcat = NapCatClient::with_onebot_url(
            &self.inner.config.napcat_webui_url,
            &self.inner.config.napcat_onebot_url,
        );
        match napcat.get_group_list().await {
            Ok(groups) => return Ok(groups),
            Err(error) => {
                tracing::debug!(%error, "OneBot HTTP group list unavailable; trying WebSocket");
            }
        }

        // Fall back to reverse WebSocket when the HTTP API is unavailable.
        if !connected {
            let self_id = self.inner.self_id.lock().ok().and_then(|g| g.clone());
            return Err(format!(
                "QQ WebSocket未连接{}，请先使用 /qq login 登录",
                self_id
                    .map(|id| format!(" (账号: {id})"))
                    .unwrap_or_default()
            ));
        }

        // Check for active context and provide guidance if missing
        let ctx_opt = self
            .inner
            .active_context
            .lock()
            .map_err(|e| format!("lock poisoned: {e}"))?
            .clone();

        let ctx = match ctx_opt {
            Some(ctx) => ctx,
            None => {
                // QQ is connected but API context not yet activated
                return Err(
                    "QQ已连接但API上下文尚未激活，请发送任意消息到QQ群组来激活API功能".to_string(),
                );
            }
        };

        let request = echo_core::action::actions::get_group_list();
        let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
        let groups: Vec<(i64, String)> = resp
            .data
            .as_array()
            .ok_or("unexpected response format")?
            .iter()
            .filter_map(|v| {
                let gid = v.get("group_id")?.as_i64()?;
                let name = v
                    .get("group_name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("(unknown)")
                    .to_string();
                Some((gid, name))
            })
            .collect();
        Ok(groups)
    }

    /// Get the QQ friend list via the active QQ connection.
    pub async fn get_friend_list(&self) -> Result<Vec<(i64, String)>, String> {
        // Add diagnostic logging
        let connected = self.inner.connected.load(Ordering::SeqCst);
        let running = self.inner.running.load(Ordering::SeqCst);
        let has_context = self
            .inner
            .active_context
            .lock()
            .ok()
            .map(|g| g.is_some())
            .unwrap_or(false);

        tracing::info!(
            "get_friend_list called - connected: {}, running: {}, has_context: {}, self_id: {:?}",
            connected,
            running,
            has_context,
            self.inner.self_id.lock().ok()
        );

        // First check if QQ adapter is running
        if !running {
            return Err("QQ适配器未启动，请检查配置".to_string());
        }

        let napcat = NapCatClient::with_onebot_url(
            &self.inner.config.napcat_webui_url,
            &self.inner.config.napcat_onebot_url,
        );
        match napcat.get_friend_list().await {
            Ok(friends) => return Ok(friends),
            Err(error) => {
                tracing::debug!(%error, "OneBot HTTP friend list unavailable; trying WebSocket");
            }
        }

        // Fall back to reverse WebSocket when the HTTP API is unavailable.
        if !connected {
            let self_id_opt = self.inner.self_id.lock().ok().and_then(|g| g.clone());
            return Err(format!(
                "QQ WebSocket未连接{}，请先使用 /qq login 登录",
                self_id_opt
                    .map(|id| format!(" (账号: {id})"))
                    .unwrap_or_default()
            ));
        }
        let ctx_opt = self
            .inner
            .active_context
            .lock()
            .map_err(|e| format!("lock poisoned: {e}"))?
            .clone();

        let ctx = match ctx_opt {
            Some(ctx) => ctx,
            None => {
                // QQ is connected but API context not yet activated
                return Err(
                    "QQ已连接但API上下文尚未激活，请发送任意消息到QQ群组来激活API功能".to_string(),
                );
            }
        };

        let request = echo_core::action::actions::get_friend_list();
        let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
        let friends: Vec<(i64, String)> = resp
            .data
            .as_array()
            .ok_or("unexpected response format")?
            .iter()
            .filter_map(|v| {
                let uid = v.get("user_id")?.as_i64()?;
                let name = v
                    .get("nickname")
                    .and_then(|n| n.as_str())
                    .unwrap_or("(unknown)")
                    .to_string();
                Some((uid, name))
            })
            .collect();
        Ok(friends)
    }

    /// Get the friend list visible under the current gate configuration.
    /// The TUI deliberately uses [`get_friend_list`](Self::get_friend_list)
    /// instead so administrators can still edit the complete list.
    pub async fn get_gated_friend_list(&self) -> Result<Vec<(i64, String)>, String> {
        let friends = self.get_friend_list().await?;
        Ok(self.apply_friend_gate(friends))
    }

    /// Get group list via the active QQ connection.
    pub async fn get_group_list(&self) -> Result<Vec<(i64, String)>, String> {
        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| format!("lock poisoned: {e}"))?
            .clone()
            .ok_or("no QQ connection active")?;
        let request = echo_core::action::actions::get_group_list();
        let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
        // The response data is an array of { group_id, group_name, ... }.
        let groups: Vec<(i64, String)> = resp
            .data
            .as_array()
            .ok_or("unexpected response format")?
            .iter()
            .filter_map(|v| {
                let gid = v.get("group_id")?.as_i64()?;
                let name = v
                    .get("group_name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("(unknown)")
                    .to_string();
                Some((gid, name))
            })
            .collect();

        // Apply gate-mode filtering to the group list.
        let mode = self.get_gate_mode();
        if mode != QqGateMode::None {
            let cfg = self.get_filter_config();
            let mut filtered = groups;
            match mode {
                QqGateMode::Allowlist => {
                    let allow_groups: std::collections::HashSet<i64> =
                        cfg.allowlist.group_ids.iter().cloned().collect();
                    filtered.retain(|(gid, _)| allow_groups.contains(gid));
                }
                QqGateMode::Denylist => {
                    let deny_groups: std::collections::HashSet<i64> =
                        cfg.denylist.group_ids.iter().cloned().collect();
                    if !deny_groups.is_empty() {
                        filtered.retain(|(gid, _)| !deny_groups.contains(gid));
                    }
                }
                QqGateMode::None => {}
            }
            return Ok(filtered);
        }
        Ok(groups)
    }

    /// Get group member info via the active QQ connection.
    pub async fn get_group_member_info(
        &self,
        group_id: i64,
        user_id: i64,
    ) -> Result<String, String> {
        // ── Gate mode check ──
        {
            let mode = self.get_gate_mode();
            if mode != QqGateMode::None {
                let cfg = self.get_filter_config();
                match mode {
                    QqGateMode::Allowlist => {
                        let allow_groups: std::collections::HashSet<i64> =
                            cfg.allowlist.group_ids.iter().cloned().collect();
                        if !allow_groups.is_empty() && !allow_groups.contains(&group_id) {
                            return Err(format!("group {group_id} is not allowlisted"));
                        }
                    }
                    QqGateMode::Denylist => {
                        let deny_groups: std::collections::HashSet<i64> =
                            cfg.denylist.group_ids.iter().cloned().collect();
                        if !deny_groups.is_empty() && deny_groups.contains(&group_id) {
                            return Err(format!("group {group_id} is denylisted"));
                        }
                    }
                    QqGateMode::None => {}
                }
            }
        }
        let _ = user_id; // reserved for future per-user gating

        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| format!("lock poisoned: {e}"))?
            .clone();
        let ctx = ctx.ok_or("no QQ connection active")?;
        let request = echo_core::action::actions::get_group_member_info(group_id, user_id);
        let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
        serde_json::to_string(&resp.data).map_err(|e| e.to_string())
    }

    /// Upload a file to a group via the active QQ connection.
    ///
    /// `file_path` is a path on the machine running the agent (the host). It
    /// is transparently bridged to a location NapCat can read: container paths
    /// pass through, `docker cp` is attempted when available, and otherwise a
    /// local HTTP file server hands the file to NapCat via URL.
    /// Fetch a group's recent message history (NapCat `get_group_msg_history`),
    /// rendered as a readable transcript.
    ///
    /// - **Gated** by group visibility ([`visible_group_check`]): the agent can
    ///   only read history of groups it may interact with.
    /// - `count` is clamped to `1..=`[`HISTORY_COUNT_MAX`].
    /// - `since_minutes` keeps only messages within the last N minutes.
    ///
    /// Unlike the inbound pipeline, this includes messages that did not
    /// mention the bot (and messages the gate filtered out) — it is the
    /// "catch up on group context" path.
    pub async fn get_group_msg_history(
        &self,
        group_id: i64,
        count: i64,
        since_minutes: Option<u64>,
    ) -> Result<String, String> {
        self.visible_group_check(group_id)?;

        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| format!("lock poisoned: {e}"))?
            .clone()
            .ok_or("no QQ connection active")?;

        let count = count.clamp(1, HISTORY_COUNT_MAX as i64);
        let request = echo_core::action::actions::get_group_msg_history(group_id, None, count);
        let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
        if !resp.is_ok() {
            return Err(resp
                .error_message()
                .unwrap_or_else(|| "get_group_msg_history failed".into()));
        }
        render_group_msg_history(
            &resp.data,
            group_id,
            since_minutes,
            chrono::Utc::now().timestamp(),
        )
    }

    /// `file_name` is the display name shown in QQ.
    pub async fn upload_group_file(
        &self,
        group_id: i64,
        file_path: &str,
        file_name: &str,
    ) -> Result<String, String> {
        // 出站门控（2026-10 巡检 P1）：文件上传此前绕开
        // check_outbound_gate——send_message/send_json_card 都过门控，
        // upload 不应是后门。构造等效 MessageTarget 复用同一门控。
        let target = echo_defs::chat::MessageTarget {
            adapter_name: self.name.clone(),
            channel: echo_defs::chat::ChannelType::Group {
                group_id: group_id.to_string(),
            },
            user_id: String::new(),
        };
        self.check_outbound_gate(&target)?;
        let candidates = self
            .inner
            .file_bridge
            .resolve_all(file_path, file_name)
            .await?;
        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| format!("lock poisoned: {e}"))?
            .clone()
            .ok_or("no QQ connection active")?;

        let mut last_error = None;
        for remote in &candidates {
            let request =
                echo_core::action::actions::upload_group_file(group_id, remote, file_name);
            let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
            if resp.is_ok() {
                tracing::info!(%group_id, %remote, "group file uploaded");
                return Ok(format!("file uploaded to group {group_id}"));
            }
            last_error = Some(
                resp.error_message()
                    .unwrap_or_else(|| "upload failed".into()),
            );
            tracing::debug!(%remote, error = last_error.as_deref().unwrap_or(""), "upload candidate failed; trying next");
        }
        Err(last_error.unwrap_or_else(|| "upload failed".into()))
    }

    /// Upload a file to a private chat via the active QQ connection.
    ///
    /// `file_path` is a path on the machine running the agent (the host). It
    /// is transparently bridged to a location NapCat can read (see
    /// [`upload_group_file`](Self::upload_group_file)).
    /// `file_name` is the display name shown in QQ.
    pub async fn upload_private_file(
        &self,
        user_id: i64,
        file_path: &str,
        file_name: &str,
    ) -> Result<String, String> {
        // 出站门控（同 upload_group_file 注释）。
        let target = echo_defs::chat::MessageTarget {
            adapter_name: self.name.clone(),
            channel: echo_defs::chat::ChannelType::Direct,
            user_id: user_id.to_string(),
        };
        self.check_outbound_gate(&target)?;
        let candidates = self
            .inner
            .file_bridge
            .resolve_all(file_path, file_name)
            .await?;
        let ctx = self
            .inner
            .active_context
            .lock()
            .map_err(|e| format!("lock poisoned: {e}"))?
            .clone()
            .ok_or("no QQ connection active")?;

        let mut last_error = None;
        for remote in &candidates {
            let request =
                echo_core::action::actions::upload_private_file(user_id, remote, file_name);
            let resp = ctx.send_api(request).await.map_err(|e| e.to_string())?;
            if resp.is_ok() {
                tracing::info!(%user_id, %remote, "private file uploaded");
                return Ok(format!("file uploaded to user {user_id}"));
            }
            last_error = Some(
                resp.error_message()
                    .unwrap_or_else(|| "upload failed".into()),
            );
            tracing::debug!(%remote, error = last_error.as_deref().unwrap_or(""), "upload candidate failed; trying next");
        }
        Err(last_error.unwrap_or_else(|| "upload failed".into()))
    }
}

/// `get_group_msg_history` 的拉取上限：NapCat 本地数据窗口本就有限，
/// 封顶防止超大请求（也是给 LLM 输出的体量上限）。
pub(crate) const HISTORY_COUNT_MAX: usize = 500;

/// 把 `get_group_msg_history` 响应渲染为可读转写文本（纯函数，便于单测）。
///
/// 每条：`[MM-DD HH:MM:SS] 昵称(QQ号): 正文`；正文用
/// [`echo_core::segment::render_transcript`]（[图片]/[语音]/@提及等标记）。
/// 段解析失败时回退 `raw_message`。`since_minutes` 为 `Some` 时只保留
/// `now_ts - n*60` 之后的消息。
pub(crate) fn render_group_msg_history(
    data: &serde_json::Value,
    group_id: i64,
    since_minutes: Option<u64>,
    now_ts: i64,
) -> Result<String, String> {
    let messages = data
        .get("messages")
        .and_then(|m| m.as_array())
        .ok_or("unexpected response format (missing messages)")?;

    let cutoff = since_minutes.map(|n| now_ts.saturating_sub((n as i64).saturating_mul(60)));
    let mut lines: Vec<String> = Vec::new();
    let mut first_ts: Option<i64> = None;
    let mut last_ts: Option<i64> = None;

    for m in messages {
        let ts = m.get("time").and_then(|t| t.as_i64()).unwrap_or(0);
        if let Some(cutoff) = cutoff {
            if ts < cutoff {
                continue;
            }
        }
        first_ts.get_or_insert(ts);
        last_ts = Some(ts);

        let uid = m.get("user_id").and_then(|u| u.as_i64()).unwrap_or(0);
        let sender = m.get("sender");
        let nickname = sender
            .and_then(|s| s.get("nickname"))
            .and_then(|n| n.as_str())
            .filter(|n| !n.trim().is_empty())
            .unwrap_or("(unknown)");
        // 群名片优先（与入站 sender_nickname 口径一致）。
        let card = sender
            .and_then(|s| s.get("card"))
            .and_then(|c| c.as_str())
            .map(str::trim)
            .filter(|c| !c.is_empty());
        let display = card.unwrap_or(nickname);

        let body = m
            .get("message")
            .and_then(|msg| serde_json::from_value::<Vec<echo_core::Segment>>(msg.clone()).ok())
            .map(|segs| echo_core::segment::render_transcript(&segs))
            .filter(|text| !text.trim().is_empty())
            .or_else(|| {
                m.get("raw_message")
                    .and_then(|r| r.as_str())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_default();

        lines.push(format!(
            "[{}] {}({}): {}",
            format_history_ts(ts),
            display,
            uid,
            body
        ));
    }

    if lines.is_empty() {
        return Ok(format!(
            "group {group_id}: no messages found in the requested window"
        ));
    }

    let range = match (first_ts, last_ts) {
        (Some(a), Some(b)) => format!(", {} .. {}", format_history_ts(a), format_history_ts(b)),
        _ => String::new(),
    };
    Ok(format!(
        "group {group_id}: {} message(s){range}\n{}",
        lines.len(),
        lines.join("\n")
    ))
}

/// `[MM-DD HH:MM:SS]`（本地时区）；越界时间戳回退原始数字。
fn format_history_ts(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| ts.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 样例取自 NapCat 真实响应（精简），含文本/@/图片/语音各一段。
    fn sample_data(now: i64) -> serde_json::Value {
        json!({
            "messages": [
                {
                    "time": now - 300,
                    "user_id": 1828980067,
                    "sender": {"nickname": "Evence", "card": ""},
                    "message": [
                        {"type": "at", "data": {"qq": "2911759808"}},
                        {"type": "text", "data": {"text": " 笑一个"}}
                    ],
                    "raw_message": "[CQ:at,qq=2911759808] 笑一个"
                },
                {
                    "time": now - 200,
                    "user_id": 2911759808i64,
                    "sender": {"nickname": "Alix", "card": "Alix2"},
                    "message": [
                        {"type": "record", "data": {"file": "x.amr"}}
                    ],
                    "raw_message": "[CQ:record,file=x.amr]"
                },
                {
                    "time": now - 100,
                    "user_id": 478974252,
                    "sender": {"nickname": "一只坚果。"},
                    "message": [
                        {"type": "image", "data": {"file": "a.jpg"}},
                        {"type": "text", "data": {"text": "看图"}}
                    ],
                    "raw_message": "[CQ:image,file=a.jpg]看图"
                },
                {
                    "time": now - 86400,
                    "user_id": 123,
                    "sender": {"nickname": "旧消息"},
                    "message": [
                        {"type": "text", "data": {"text": "昨天的"}}
                    ],
                    "raw_message": "昨天的"
                }
            ]
        })
    }

    #[test]
    fn renders_transcript_with_markers_and_card_name() {
        let now = 1_800_000_000;
        let text = render_group_msg_history(&sample_data(now), 1094762376, None, now).unwrap();
        assert!(text.starts_with("group 1094762376: 4 message(s)"), "{text}");
        assert!(text.contains("@2911759808 笑一个"), "{text}");
        assert!(
            text.contains("Alix2(2911759808): [语音]"),
            "群名片优先: {text}"
        );
        assert!(text.contains("一只坚果。(478974252): [图片]看图"), "{text}");
        assert!(text.contains("旧消息(123): 昨天的"), "{text}");
    }

    #[test]
    fn since_minutes_filters_older_messages() {
        let now = 1_800_000_000;
        let text = render_group_msg_history(&sample_data(now), 1094762376, Some(10), now).unwrap();
        assert!(text.starts_with("group 1094762376: 3 message(s)"), "{text}");
        assert!(!text.contains("昨天的"), "{text}");
    }

    #[test]
    fn empty_window_and_missing_field_are_distinct() {
        let now = 1_800_000_000;
        let text = render_group_msg_history(&sample_data(now), 1094762376, Some(1), now).unwrap();
        assert!(text.contains("no messages found"), "{text}");

        let err = render_group_msg_history(&json!({}), 1, None, now).unwrap_err();
        assert!(err.contains("missing messages"), "{err}");
    }

    #[test]
    fn falls_back_to_raw_message_when_segments_unparsable() {
        let now = 1_800_000_000;
        let data = json!({
            "messages": [{
                "time": now,
                "user_id": 1,
                "sender": {"nickname": "n"},
                "message": "not-an-array",
                "raw_message": "[CQ:unknown,x=1] 兜底文本"
            }]
        });
        let text = render_group_msg_history(&data, 42, None, now).unwrap();
        assert!(text.contains("兜底文本"), "{text}");
    }
}
