//! OneBot v11 actions (API calls) and their responses.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An API request sent to the OneBot implementation.
///
/// Serialized as `{"action": "...", "params": {...}, "echo": "..."}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiRequest {
    pub action: String,
    #[serde(default)]
    pub params: Value,
    /// Correlation id; the implementation echoes it back in the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub echo: Option<String>,
}

impl ApiRequest {
    pub fn new(action: impl Into<String>, params: Value) -> Self {
        Self {
            action: action.into(),
            params,
            echo: None,
        }
    }
}

/// The implementation's response, correlated with the request via `echo`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ApiResponse {
    /// `ok` or `failed`。部分实现只发 retcode，缺省时按 retcode==0 推导。
    #[serde(default)]
    pub status: String,
    /// 0 for success; other values are implementation-specific errors.
    #[serde(default)]
    pub retcode: i32,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub echo: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub wording: Option<String>,
}

impl ApiResponse {
    pub fn is_ok(&self) -> bool {
        self.status == "ok" || self.retcode == 0
    }

    /// Human-readable error description if the call failed.
    pub fn error_message(&self) -> Option<String> {
        self.message.clone().or_else(|| self.wording.clone())
    }
}

/// Typed builders for common OneBot v11 actions.
pub mod actions {
    use super::ApiRequest;
    use serde_json::{json, Value};

    /// Send a message to a private chat.
    pub fn send_private_msg(user_id: i64, message: Vec<crate::segment::Segment>) -> ApiRequest {
        ApiRequest::new(
            "send_private_msg",
            json!({ "user_id": user_id, "message": message }),
        )
    }

    /// Send a message to a group.
    pub fn send_group_msg(group_id: i64, message: Vec<crate::segment::Segment>) -> ApiRequest {
        ApiRequest::new(
            "send_group_msg",
            json!({ "group_id": group_id, "message": message }),
        )
    }

    /// Upload a file to a group chat.
    ///
    /// `file` is a path on the machine running the OneBot implementation
    /// (e.g. inside the NapCat container). `name` is the display name in QQ.
    pub fn upload_group_file(group_id: i64, file: &str, name: &str) -> ApiRequest {
        ApiRequest::new(
            "upload_group_file",
            json!({ "group_id": group_id, "file": file, "name": name }),
        )
    }

    /// Upload a file to a private chat.
    ///
    /// `file` is a path on the machine running the OneBot implementation
    /// (e.g. inside the NapCat container). `name` is the display name in QQ.
    pub fn upload_private_file(user_id: i64, file: &str, name: &str) -> ApiRequest {
        ApiRequest::new(
            "upload_private_file",
            json!({ "user_id": user_id, "file": file, "name": name }),
        )
    }
    /// 获取群文件下载直链（NapCat：参数 group_id/file_id 均为字符串）。
    ///
    /// `file_id` 是 NapCat 编码的文件 ID（群文件上传通知或消息 file 段给出）。
    pub fn get_group_file_url(group_id: i64, file_id: &str) -> ApiRequest {
        ApiRequest::new(
            "get_group_file_url",
            json!({ "group_id": group_id.to_string(), "file_id": file_id }),
        )
    }

    /// 获取私聊文件下载直链（NapCat 扩展接口）。
    pub fn get_private_file_url(file_id: &str) -> ApiRequest {
        ApiRequest::new("get_private_file_url", json!({ "file_id": file_id }))
    }

    /// Send a message to either a group or a private chat.
    pub fn send_msg(
        message_type: &str,
        target_id: i64,
        message: Vec<crate::segment::Segment>,
    ) -> ApiRequest {
        let params = if message_type == "group" {
            json!({ "message_type": "group", "group_id": target_id, "message": message })
        } else {
            json!({ "message_type": "private", "user_id": target_id, "message": message })
        };
        ApiRequest::new("send_msg", params)
    }

    /// Recall a message.
    pub fn delete_msg(message_id: i64) -> ApiRequest {
        ApiRequest::new("delete_msg", json!({ "message_id": message_id }))
    }

    /// Send a group message built from node segments (forward chain).
    pub fn send_group_forward_msg(group_id: i64, nodes: Vec<Value>) -> ApiRequest {
        ApiRequest::new(
            "send_group_forward_msg",
            json!({ "group_id": group_id, "messages": nodes }),
        )
    }

    /// Info about the logged-in bot account.
    pub fn get_login_info() -> ApiRequest {
        ApiRequest::new("get_login_info", json!({}))
    }

    pub fn get_group_info(group_id: i64) -> ApiRequest {
        ApiRequest::new("get_group_info", json!({ "group_id": group_id }))
    }

    /// List all groups the bot has joined.
    pub fn get_group_list() -> ApiRequest {
        ApiRequest::new("get_group_list", json!({}))
    }

    /// Fetch a group's recent message history (NapCat extension).
    ///
    /// `message_seq` anchors the window (messages from that seq onward);
    /// `None` returns the latest `count` messages. Returns
    /// `{"messages": [...]}` on success.
    pub fn get_group_msg_history(
        group_id: i64,
        message_seq: Option<i64>,
        count: i64,
    ) -> ApiRequest {
        let mut params = json!({ "group_id": group_id, "count": count });
        if let Some(seq) = message_seq {
            params["message_seq"] = json!(seq);
        }
        ApiRequest::new("get_group_msg_history", params)
    }

    pub fn get_group_member_info(group_id: i64, user_id: i64) -> ApiRequest {
        ApiRequest::new(
            "get_group_member_info",
            json!({ "group_id": group_id, "user_id": user_id }),
        )
    }

    pub fn set_group_kick(group_id: i64, user_id: i64, reject_add_request: bool) -> ApiRequest {
        ApiRequest::new(
            "set_group_kick",
            json!({ "group_id": group_id, "user_id": user_id, "reject_add_request": reject_add_request }),
        )
    }

    pub fn set_group_ban(group_id: i64, user_id: i64, duration: i64) -> ApiRequest {
        ApiRequest::new(
            "set_group_ban",
            json!({ "group_id": group_id, "user_id": user_id, "duration": duration }),
        )
    }

    pub fn set_group_card(group_id: i64, user_id: i64, card: &str) -> ApiRequest {
        ApiRequest::new(
            "set_group_card",
            json!({ "group_id": group_id, "user_id": user_id, "card": card }),
        )
    }

    pub fn get_friend_list() -> ApiRequest {
        ApiRequest::new("get_friend_list", json!({}))
    }

    /// Implementation version info (`impl`, `version`, `onebot_version`).
    pub fn get_version() -> ApiRequest {
        ApiRequest::new("get_version", json!({}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Segment;

    #[test]
    fn request_serialization() {
        let req = actions::send_group_msg(30001, vec![Segment::at("20001"), Segment::text("hi")]);
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["action"], "send_group_msg");
        assert_eq!(json["params"]["group_id"], 30001);
        assert_eq!(json["params"]["message"][0]["type"], "at");
        assert!(json.get("echo").is_none());

        let mut req = req;
        req.echo = Some("abc".into());
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["echo"], "abc");
    }

    #[test]
    fn response_parsing() {
        let resp: ApiResponse = serde_json::from_str(
            r#"{"status":"ok","retcode":0,"data":{"message_id":123},"echo":"abc"}"#,
        )
        .unwrap();
        assert!(resp.is_ok());
        assert_eq!(resp.echo.as_deref(), Some("abc"));
        assert_eq!(resp.data["message_id"], 123);

        let failed: ApiResponse = serde_json::from_str(
            r#"{"status":"failed","retcode":100,"data":null,"message":"bad"}"#,
        )
        .unwrap();
        assert!(!failed.is_ok());
        assert_eq!(failed.error_message().as_deref(), Some("bad"));
    }

    #[test]
    fn send_msg_routes_group_and_private() {
        let group = actions::send_msg("group", 30001, vec![Segment::text("hi")]);
        let json = serde_json::to_value(&group).unwrap();
        assert_eq!(json["action"], "send_msg");
        assert_eq!(json["params"]["message_type"], "group");
        assert_eq!(json["params"]["group_id"], 30001);

        let private = actions::send_msg("private", 20001, vec![Segment::text("hi")]);
        let json = serde_json::to_value(&private).unwrap();
        assert_eq!(json["params"]["message_type"], "private");
        assert_eq!(json["params"]["user_id"], 20001);
    }

    #[test]
    fn moderation_actions_carry_params() {
        let ban = actions::set_group_ban(100, 200, 3600);
        let json = serde_json::to_value(&ban).unwrap();
        assert_eq!(json["params"]["group_id"], 100);
        assert_eq!(json["params"]["user_id"], 200);
        assert_eq!(json["params"]["duration"], 3600);

        let kick = actions::set_group_kick(100, 200, true);
        let json = serde_json::to_value(&kick).unwrap();
        assert_eq!(json["params"]["reject_add_request"], true);

        let card = actions::set_group_card(100, 200, "new name");
        let json = serde_json::to_value(&card).unwrap();
        assert_eq!(json["params"]["card"], "new name");
    }

    #[test]
    fn info_actions_have_correct_action_names() {
        assert_eq!(actions::get_login_info().action, "get_login_info");
        assert_eq!(actions::get_group_list().action, "get_group_list");
        assert_eq!(actions::get_version().action, "get_version");
        assert_eq!(actions::get_friend_list().action, "get_friend_list");
        assert_eq!(actions::delete_msg(42).action, "delete_msg");
    }

    #[test]
    fn upload_actions_carry_target_file_and_name() {
        let group = actions::upload_group_file(30001, "/tmp/a.pdf", "a.pdf");
        let json = serde_json::to_value(&group).unwrap();
        assert_eq!(json["action"], "upload_group_file");
        assert_eq!(json["params"]["group_id"], 30001);
        assert_eq!(json["params"]["file"], "/tmp/a.pdf");
        assert_eq!(json["params"]["name"], "a.pdf");

        let private = actions::upload_private_file(20001, "/tmp/b.zip", "b.zip");
        let json = serde_json::to_value(&private).unwrap();
        assert_eq!(json["action"], "upload_private_file");
        assert_eq!(json["params"]["user_id"], 20001);
        assert_eq!(json["params"]["file"], "/tmp/b.zip");
        assert_eq!(json["params"]["name"], "b.zip");
    }

    #[test]
    fn file_url_actions_use_string_params() {
        // NapCat 的 schema 要求 group_id/file_id 为字符串；数字会被拒或需隐式转换。
        let group = actions::get_group_file_url(30001, "file-uuid-1");
        let json = serde_json::to_value(&group).unwrap();
        assert_eq!(json["action"], "get_group_file_url");
        assert_eq!(json["params"]["group_id"], "30001");
        assert_eq!(json["params"]["file_id"], "file-uuid-1");

        let private = actions::get_private_file_url("file-uuid-2");
        let json = serde_json::to_value(&private).unwrap();
        assert_eq!(json["action"], "get_private_file_url");
        assert_eq!(json["params"]["file_id"], "file-uuid-2");
    }

    #[test]
    fn response_error_message_falls_back_to_wording() {
        let resp: ApiResponse = serde_json::from_str(
            r#"{"status":"failed","retcode":100,"data":null,"wording":"rate limited"}"#,
        )
        .unwrap();
        assert!(!resp.is_ok());
        assert_eq!(resp.error_message().as_deref(), Some("rate limited"));
    }

    #[test]
    fn forward_msg_uses_messages_key() {
        let req = actions::send_group_forward_msg(30001, vec![serde_json::json!({"type": "node"})]);
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["params"]["messages"][0]["type"], "node");
    }
}
