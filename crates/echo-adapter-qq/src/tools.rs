//! 适配器贡献给模型的工具：声明 + 调用适配。
//!
//! **平台语义住在这里**：工具名、描述、参数 schema 都是 QQ / OneBot 的概念
//! （群 / 私聊 / 卡片 / 语音 / 群历史）。宿主（EchoAgentCore）不认识任何一个
//! 工具名——它按 [`AdapterContribution::tools`] 原样注册，调用时把参数透传回
//! 来（`Invoke("<instance>:<工具名>")`），命中本模块的 [`invoke`]。
//!
//! 这样新增一个平台（Telegram / Discord…）只需在它的插件里写同样一份声明，
//! Core 零改动。
//!
//! [`AdapterContribution::tools`]: echo_plugin_api::AdapterContribution::tools

use echo_plugin_sdk::echo_plugin_api::{InvokeOutcome, ToolContribution};
use serde_json::{json, Value};

use crate::plugin::{error_outcome, ok_result};
use crate::QqAdapter;

/// 工具名清单（宿主注册后用于打「所属包」标签；顺序即声明顺序）。
pub fn contributions() -> Vec<ToolContribution> {
    vec![
        send_group_msg(),
        send_private_msg(),
        get_group_member_info(),
        get_group_list(),
        get_friend_list(),
        get_group_msg_history(),
        send_file(),
        send_json_card(),
        send_voice(),
        send_image(),
    ]
}

/// 构造一个工具声明。
///
/// `visible_reply` = 成功调用即产生**平台可见输出**（宿主据此抑制「临时等待
/// 回复」）——只有真正把内容送到用户眼前的工具才为 true。
fn tool(name: &str, description: &str, parameters: Value, visible_reply: bool) -> ToolContribution {
    ToolContribution {
        name: name.to_string(),
        description: description.to_string(),
        parameters,
        category: "adapter".to_string(),
        timeout_hint_secs: None,
        // 与这两个技能同包：面板按包批量勾选时「工具 + 技能」是一组。
        package: Some("echo-agent.adapter.qq".to_string()),
        visible_reply,
    }
}

// ── 工具声明（schema 与原 Core 内置版逐字一致，模型行为不变）──

fn send_group_msg() -> ToolContribution {
    tool(
        "send_group_msg",
        "Send a text message to a group. This is the only way to produce group-visible output. Requires group_id and content.",
        json!({
            "type": "object",
            "properties": {
                "group_id": {"type": "integer", "description": "group ID"},
                "content": {"type": "string", "description": "Message content"}
            },
            "required": ["group_id", "content"]
        }),
        true,
    )
}

fn send_private_msg() -> ToolContribution {
    tool(
        "send_private_msg",
        "Send a private text message to a user. This is the only way to produce private-visible output. Requires user_id and content.",
        json!({
            "type": "object",
            "properties": {
                "user_id": {"type": "integer", "description": "user ID"},
                "content": {"type": "string", "description": "Message content"}
            },
            "required": ["user_id", "content"]
        }),
        true,
    )
}

fn get_group_member_info() -> ToolContribution {
    tool(
        "get_group_member_info",
        "Get group member information. Requires group_id and user_id.",
        json!({
            "type": "object",
            "properties": {
                "group_id": {"type": "integer", "description": "group ID"},
                "user_id": {"type": "integer", "description": "user ID"}
            },
            "required": ["group_id", "user_id"]
        }),
        false,
    )
}

fn get_group_list() -> ToolContribution {
    tool(
        "get_group_list",
        "List all groups the bot has joined. Returns group IDs and names.",
        json!({ "type": "object", "properties": {} }),
        false,
    )
}

fn get_friend_list() -> ToolContribution {
    tool(
        "get_friend_list",
        "List friends visible under the current gate mode. Returns user IDs and nicknames.",
        json!({ "type": "object", "properties": {} }),
        false,
    )
}

fn get_group_msg_history() -> ToolContribution {
    tool(
        "get_group_msg_history",
        "Fetch a group's recent message history as a readable transcript. Unlike normal inbound messages, this includes messages that did NOT mention you and messages the inbound gate filtered out — use it to catch up on group context when asked what was discussed recently, or when you need background for a reply. Gated: only groups visible under the current gate mode are readable. Requires group_id; optional count (default 50, max 500) and since_minutes (keep only messages from the last N minutes).",
        json!({
            "type": "object",
            "properties": {
                "group_id": {"type": "integer", "description": "group ID"},
                "count": {"type": "integer", "description": "Max messages to fetch (default 50, max 500)"},
                "since_minutes": {"type": "integer", "description": "Only keep messages from the last N minutes"}
            },
            "required": ["group_id"]
        }),
        false,
    )
}

fn send_file() -> ToolContribution {
    tool(
        "send_file",
        "Upload a local file to a chat (group or private).          file_path is an absolute path on THIS machine (where the agent          runs) — the framework transparently bridges it to the adapter          runtime. file_name is the display name in the chat. Requires          target_type (group|private), target_id, file_path and file_name.",
        json!({
            "type": "object",
            "properties": {
                "target_type": {
                    "type": "string",
                    "enum": ["group", "private"],
                    "description": "Where to send: 'group' uploads to a group, 'private' to a user"
                },
                "target_id": {
                    "type": "integer",
                    "description": "group ID (when target_type=group) or user ID (when private)"
                },
                "file_path": {
                    "type": "string",
                    "description": "Absolute path to the file on THIS machine (the agent host); the framework bridges it to the adapter automatically"
                },
                "file_name": {
                    "type": "string",
                    "description": "Display file name in the chat, e.g. report.pdf"
                }
            },
            "required": ["target_type", "target_id", "file_path", "file_name"]
        }),
        true,
    )
}

fn send_json_card() -> ToolContribution {
    tool(
        "send_json_card",
        "Send a rich JSON card (platform json segment) to a chat — this is how structured shares like video mini-app cards are sent. The 'json' argument must be the complete card payload as a JSON string (e.g. {\"app\":\"com.tencent.miniapp_01\",\"config\":{...},\"meta\":{...}}). Requires target_type (group|private), target_id and json.",
        json!({
            "type": "object",
            "properties": {
                "target_type": {
                    "type": "string",
                    "enum": ["group", "private"],
                    "description": "Where to send: 'group' or 'private'"
                },
                "target_id": {
                    "type": "integer",
                    "description": "group ID (when target_type=group) or user ID (when private)"
                },
                "json": {
                    "type": "string",
                    "description": "Complete card payload as a JSON string"
                }
            },
            "required": ["target_type", "target_id", "json"]
        }),
        true,
    )
}

fn send_voice() -> ToolContribution {
    tool(
        "send_voice",
        "Send a voice message (a native voice bubble played inline), not a downloadable file — use send_file when the user wants to save the file. file accepts: an http(s):// URL, base64:// data, a path inside the adapter runtime (e.g. /data/x.mp3), or a path on THIS machine (the framework bridges it to the adapter automatically). A bare file name is looked up in the adapter's data directory first (where files exchanged in chat live). Common audio formats (mp3/wav/amr/silk) are converted automatically. Requires target_type (group|private), target_id and file.",
        json!({
            "type": "object",
            "properties": {
                "target_type": {
                    "type": "string",
                    "enum": ["group", "private"],
                    "description": "Where to send: 'group' or 'private'"
                },
                "target_id": {
                    "type": "integer",
                    "description": "group ID (when target_type=group) or user ID (when private)"
                },
                "file": {
                    "type": "string",
                    "description": "Voice source: URL (http/https), base64:// data, an in-container path, a path on THIS machine, or a bare file name found in the adapter's data directory"
                }
            },
            "required": ["target_type", "target_id", "file"]
        }),
        true,
    )
}

fn send_image() -> ToolContribution {
    tool(
        "send_image",
        "Send an image to a chat (group or private). image can be a local absolute path on THIS machine (bridged to the adapter automatically), an http(s) URL, a base64:// string, or a bare file name found in the adapter's data directory. Requires target_type (group|private), target_id and image.",
        json!({
            "type": "object",
            "properties": {
                "target_type": {
                    "type": "string",
                    "enum": ["group", "private"],
                    "description": "Where to send: 'group' or 'private'"
                },
                "target_id": {
                    "type": "integer",
                    "description": "group ID (when target_type=group) or user ID (when private)"
                },
                "image": {
                    "type": "string",
                    "description": "Image source: local absolute path on this machine, http(s) URL, base64:// data, or a file name in the adapter's data directory"
                }
            },
            "required": ["target_type", "target_id", "image"]
        }),
        true,
    )
}

/// `target_type` / `target_id` → 内部管理面的 `target` 对象形状。
fn tool_target(payload: &Value) -> Result<Value, (String, String)> {
    let target_type = payload
        .get("target_type")
        .and_then(Value::as_str)
        .ok_or_else(|| ("bad_args".to_string(), "target_type required".to_string()))?;
    let target_id = scalar_id(payload, "target_id")?;
    Ok(match target_type {
        "group" => json!({
            "channel": {"type": "group", "group_id": target_id},
            "user_id": "",
        }),
        "private" => json!({
            "channel": {"type": "dm"},
            "user_id": target_id,
        }),
        other => {
            return Err((
                "bad_args".to_string(),
                format!("target_type must be 'group' or 'private', got '{other}'"),
            ))
        }
    })
}

/// 宽容读取 id：模型可能给 JSON number 或 string，统一成字符串（线协议一律字符串）。
fn scalar_id(payload: &Value, key: &str) -> Result<String, (String, String)> {
    match payload.get(key) {
        Some(Value::Number(number)) => Ok(number.to_string()),
        Some(Value::String(text)) if !text.is_empty() => Ok(text.clone()),
        _ => Err((
            "bad_args".to_string(),
            format!("{key} required (string or number)"),
        )),
    }
}

/// 工具调用 → 内部 handler。
///
/// 返回 `None` = 这个 op 不是工具（交给 `invoke_op` 的既有分支处理）。
pub async fn invoke(adapter: &QqAdapter, op: &str, payload: Value) -> Option<InvokeOutcome> {
    Some(match op {
        // ── 文本：群 / 私聊（内部 send_text）────────────────────────────
        "send_group_msg" | "send_private_msg" => {
            let target = if op == "send_group_msg" {
                let group_id = match scalar_id(&payload, "group_id") {
                    Ok(id) => id,
                    Err((code, message)) => return Some(error_outcome(&code, message)),
                };
                json!({"channel": {"type": "group", "group_id": group_id}, "user_id": ""})
            } else {
                let user_id = match scalar_id(&payload, "user_id") {
                    Ok(id) => id,
                    Err((code, message)) => return Some(error_outcome(&code, message)),
                };
                json!({"channel": {"type": "dm"}, "user_id": user_id})
            };
            let content = payload
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let inner = json!({"target": target, "content": content});
            crate::plugin::send_text(adapter, &inner).await
        }
        // ── 目录 / 历史：直接转发内部 custom op ─────────────────────────
        "get_group_list" => crate::plugin::custom_op(adapter, "get_group_list", json!({})).await,
        "get_friend_list" => {
            crate::plugin::custom_op(adapter, "get_gated_friend_list", json!({})).await
        }
        "get_group_member_info" => {
            let mut inner = json!({});
            for key in ["group_id", "user_id"] {
                match scalar_id(&payload, key) {
                    Ok(id) => inner[key] = json!(id),
                    Err((code, message)) => return Some(error_outcome(&code, message)),
                }
            }
            crate::plugin::custom_op(adapter, "get_group_member_info", inner).await
        }
        "get_group_msg_history" => {
            let group_id = match scalar_id(&payload, "group_id") {
                Ok(id) => id,
                Err((code, message)) => return Some(error_outcome(&code, message)),
            };
            let mut inner = json!({
                "group_id": group_id,
                "count": payload.get("count").and_then(Value::as_i64).unwrap_or(50),
            });
            if let Some(since) = payload.get("since_minutes").and_then(Value::as_u64) {
                inner["since_minutes"] = json!(since);
            }
            crate::plugin::custom_op(adapter, "get_group_msg_history", inner).await
        }
        // ── 媒体 / 卡片：`target_type`+`target_id` → 内部 `target` ──────
        "send_file" | "send_json_card" | "send_voice" | "send_image" => {
            let target = match tool_target(&payload) {
                Ok(target) => target,
                Err((code, message)) => return Some(error_outcome(&code, message)),
            };
            let mut inner = json!({"target": target});
            for key in ["file_path", "file_name", "json", "file", "image"] {
                if let Some(value) = payload.get(key) {
                    inner[key] = value.clone();
                }
            }
            crate::plugin::custom_op(adapter, op, inner).await
        }
        _ => return None,
    })
}

/// `ok_result` 在 `plugin` 模块里；这里再导出一次给调用方复用。
pub fn result_ok(value: Value) -> InvokeOutcome {
    ok_result(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declares_all_ten_tools_with_qq_package() {
        let tools = contributions();
        assert_eq!(tools.len(), 10, "10 个 QQ 工具");
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        for expected in [
            "send_group_msg",
            "send_private_msg",
            "get_group_member_info",
            "get_group_list",
            "get_friend_list",
            "get_group_msg_history",
            "send_file",
            "send_json_card",
            "send_voice",
            "send_image",
        ] {
            assert!(names.contains(&expected), "missing {expected} in {names:?}");
        }
        for tool in &tools {
            assert_eq!(
                tool.package.as_deref(),
                Some("echo-agent.adapter.qq"),
                "工具与 QQ 技能同包（面板按包勾选）"
            );
            assert_eq!(tool.parameters["type"], json!("object"));
            assert!(
                tool.parameters["properties"].is_object(),
                "{} 需要参数 schema",
                tool.name
            );
        }
    }

    #[test]
    fn send_tools_describe_the_explicit_output_boundary() {
        let tools = contributions();
        for name in ["send_private_msg", "send_group_msg"] {
            let tool = tools.iter().find(|t| t.name == name).expect(name);
            assert!(
                tool.description.contains("only way"),
                "{name} 必须说明是唯一可见输出通道"
            );
        }
    }

    #[test]
    fn tool_target_maps_group_and_private() {
        let group = tool_target(&json!({"target_type": "group", "target_id": 123})).unwrap();
        assert_eq!(group["channel"]["type"], json!("group"));
        assert_eq!(group["channel"]["group_id"], json!("123"));

        let dm = tool_target(&json!({"target_type": "private", "target_id": "456"})).unwrap();
        assert_eq!(dm["channel"]["type"], json!("dm"));
        assert_eq!(dm["user_id"], json!("456"));

        assert!(tool_target(&json!({"target_type": "channel", "target_id": 1})).is_err());
        assert!(tool_target(&json!({"target_type": "group"})).is_err());
    }

    #[test]
    fn scalar_id_accepts_number_and_string() {
        assert_eq!(scalar_id(&json!({"id": 123}), "id").unwrap(), "123");
        assert_eq!(scalar_id(&json!({"id": "123"}), "id").unwrap(), "123");
        assert!(scalar_id(&json!({}), "id").is_err());
        assert!(scalar_id(&json!({"id": ""}), "id").is_err());
    }
}
