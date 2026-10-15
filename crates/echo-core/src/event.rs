//! OneBot v11 events.
//!
//! Events are discriminated by `post_type` into four families: message,
//! notice, request, and meta events. Each family is further discriminated by
//! its own tag field (`message_type`, `notice_type`, ...).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::segment::Segment;

/// Top-level event envelope, discriminated by `post_type`.
///
/// `large_enum_variant` is expected: message events carry a full message
/// payload while notice/meta events are small.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "post_type")]
pub enum Event {
    #[serde(rename = "message")]
    Message {
        #[serde(flatten)]
        inner: MessageEvent,
    },
    #[serde(rename = "notice")]
    Notice {
        #[serde(flatten)]
        inner: NoticeEvent,
    },
    #[serde(rename = "request")]
    Request {
        #[serde(flatten)]
        inner: RequestEvent,
    },
    #[serde(rename = "meta_event")]
    Meta {
        #[serde(flatten)]
        inner: MetaEvent,
    },
    /// 未识别的 post_type（宽容解析：不因未知子类型丢弃整个事件）。
    #[serde(other)]
    Unknown,
}

impl Event {
    /// Returns the message payload if this is a message event.
    pub fn as_message(&self) -> Option<&MessageEvent> {
        match self {
            Event::Message { inner } => Some(inner),
            _ => None,
        }
    }

    /// The QQ number of the bot on the connection that delivered this event.
    pub fn self_id(&self) -> Option<i64> {
        match self {
            Event::Message { inner } => Some(inner.self_id()),
            Event::Notice { inner } => Some(inner.self_id()),
            Event::Request { inner } => Some(inner.self_id()),
            Event::Meta { inner } => Some(inner.self_id()),
            Event::Unknown => None,
        }
    }
}

/// Message events, discriminated by `message_type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "message_type", rename_all = "snake_case")]
pub enum MessageEvent {
    Private {
        time: i64,
        self_id: i64,
        /// `friend` | `group` | `other`
        sub_type: String,
        message_id: i64,
        user_id: i64,
        message: Vec<Segment>,
        raw_message: String,
        #[serde(default)]
        font: i32,
        sender: PrivateSender,
    },
    Group {
        time: i64,
        self_id: i64,
        /// `normal` | `anonymous` | `notice`
        sub_type: String,
        message_id: i64,
        group_id: i64,
        user_id: i64,
        anonymous: Option<Anonymous>,
        message: Vec<Segment>,
        raw_message: String,
        #[serde(default)]
        font: i32,
        sender: GroupSender,
    },
    /// 未识别的 message_type（如 "discuss"），宽容解析。
    #[serde(other)]
    Unknown,
}

impl MessageEvent {
    pub fn self_id(&self) -> i64 {
        match self {
            MessageEvent::Private { self_id, .. } => *self_id,
            MessageEvent::Group { self_id, .. } => *self_id,
            MessageEvent::Unknown => 0,
        }
    }

    pub fn message_id(&self) -> i64 {
        match self {
            MessageEvent::Private { message_id, .. } => *message_id,
            MessageEvent::Group { message_id, .. } => *message_id,
            MessageEvent::Unknown => 0,
        }
    }

    /// Unix timestamp of the message.
    pub fn timestamp(&self) -> i64 {
        match self {
            MessageEvent::Private { time, .. } => *time,
            MessageEvent::Group { time, .. } => *time,
            MessageEvent::Unknown => 0,
        }
    }

    pub fn user_id(&self) -> i64 {
        match self {
            MessageEvent::Private { user_id, .. } => *user_id,
            MessageEvent::Group { user_id, .. } => *user_id,
            MessageEvent::Unknown => 0,
        }
    }

    /// Some(group_id) for group messages, None for private chats.
    pub fn group_id(&self) -> Option<i64> {
        match self {
            MessageEvent::Private { .. } | MessageEvent::Unknown => None,
            MessageEvent::Group { group_id, .. } => Some(*group_id),
        }
    }

    pub fn is_group(&self) -> bool {
        self.group_id().is_some()
    }

    /// The sender's display nickname (group card if set, else nickname).
    pub fn sender_nickname(&self) -> &str {
        match self {
            MessageEvent::Private { sender, .. } => &sender.nickname,
            MessageEvent::Group { sender, .. } => sender
                .card
                .as_deref()
                .filter(|c| !c.is_empty())
                .unwrap_or(&sender.nickname),
            MessageEvent::Unknown => "",
        }
    }

    /// Concatenation of all text segments.
    pub fn plain_text(&self) -> String {
        let mut out = String::new();
        for seg in self.message() {
            if let Segment::Known(crate::segment::KnownSegment::Text { data }) = seg {
                out.push_str(&data.text);
            }
        }
        out
    }

    /// 渲染为「人类/模型可读」的文本：文本原样，表情类消息段
    /// （face / dice / rps / poke）渲染为可读标记，其余段跳过。
    ///
    /// 与 [`plain_text`](Self::plain_text) 的分工：命令解析等需要「纯文本」
    /// 的场景用 `plain_text`（标记不能干扰 `/help` 之类的前缀判断）；
    /// 把消息交给 agent（QQ 适配器的 content）或展示时用本方法——表情
    /// 是消息语义的一部分（QQ 的 `face` 段只带数字 id，直接丢弃会让模型
    /// 「读不到」消息里的表情）。
    pub fn readable_text(&self) -> String {
        let mut out = String::new();
        for seg in self.message() {
            match seg {
                Segment::Known(crate::segment::KnownSegment::Text { data }) => {
                    out.push_str(&data.text);
                }
                Segment::Known(crate::segment::KnownSegment::Face { data }) => {
                    out.push_str(&crate::face::face_marker(&data.id));
                }
                Segment::Known(crate::segment::KnownSegment::Dice { data }) => {
                    out.push_str(&dice_marker(data));
                }
                Segment::Known(crate::segment::KnownSegment::Rps { data }) => {
                    out.push_str(&rps_marker(data));
                }
                Segment::Known(crate::segment::KnownSegment::Poke { .. }) => {
                    out.push_str("[戳一戳]");
                }
                _ => {}
            }
        }
        out
    }

    pub fn message(&self) -> &[Segment] {
        match self {
            MessageEvent::Private { message, .. } => message,
            MessageEvent::Group { message, .. } => message,
            MessageEvent::Unknown => &[],
        }
    }

    /// True if the message @s the given QQ number or mentions everyone.
    pub fn has_at(&self, qq: i64) -> bool {
        let qq = qq.to_string();
        self.message().iter().any(|seg| {
            matches!(
                seg,
                Segment::Known(crate::segment::KnownSegment::At {
                    data: crate::segment::AtData { qq: at, .. }
                }) if at == &qq || at == "all"
            )
        })
    }

    /// True if the bot itself is mentioned in this message.
    pub fn at_me(&self) -> bool {
        self.has_at(self.self_id())
    }

    /// The message id this message replies to, if any.
    pub fn reply_to(&self) -> Option<&str> {
        self.message().iter().find_map(|seg| match seg {
            Segment::Known(crate::segment::KnownSegment::Reply { data }) => Some(data.id.as_str()),
            _ => None,
        })
    }
}

/// Sender payload of a private message.
///
/// OneBot v11 规范说明 sender 字段"不保证每个字段都一定存在"，全部可缺省。
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PrivateSender {
    #[serde(default)]
    pub user_id: i64,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub sex: String,
    #[serde(default)]
    pub age: i32,
}

/// Sender payload of a group message.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct GroupSender {
    #[serde(default)]
    pub user_id: i64,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub card: Option<String>,
    #[serde(default)]
    pub sex: String,
    #[serde(default)]
    pub age: i32,
    #[serde(default)]
    pub area: String,
    #[serde(default)]
    pub level: String,
    /// `owner` | `admin` | `member`
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub title: String,
}

impl GroupSender {
    pub fn is_admin(&self) -> bool {
        self.role == "owner" || self.role == "admin"
    }
}

/// Anonymous group sender info, present when `sub_type == "anonymous"`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Anonymous {
    pub id: i64,
    pub name: String,
    pub flag: String,
}

/// A file attached to a `group_upload` notice.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FileInfo {
    pub id: String,
    pub name: String,
    pub size: i64,
    pub busid: i64,
}

/// Notice events, discriminated by `notice_type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "notice_type", rename_all = "snake_case")]
pub enum NoticeEvent {
    /// Someone uploaded a file to the group.
    GroupUpload {
        time: i64,
        self_id: i64,
        group_id: i64,
        user_id: i64,
        file: FileInfo,
    },
    /// Someone became/unbecame group admin (`sub_type`: set | unset).
    GroupAdmin {
        time: i64,
        self_id: i64,
        group_id: i64,
        user_id: i64,
        sub_type: String,
    },
    /// Someone left the group (`sub_type`: leave | kick | kick_me).
    GroupDecrease {
        time: i64,
        self_id: i64,
        group_id: i64,
        operator_id: Option<i64>,
        user_id: i64,
        sub_type: String,
    },
    /// Someone joined the group (`sub_type`: approve | invite).
    GroupIncrease {
        time: i64,
        self_id: i64,
        group_id: i64,
        operator_id: Option<i64>,
        user_id: i64,
        sub_type: String,
    },
    /// Someone was muted/unmuted (`sub_type`: ban | lift_ban).
    GroupBan {
        time: i64,
        self_id: i64,
        group_id: i64,
        operator_id: Option<i64>,
        user_id: i64,
        sub_type: String,
        duration: i64,
    },
    /// New friend added the bot.
    FriendAdd {
        time: i64,
        self_id: i64,
        user_id: i64,
    },
    /// A group message was recalled.
    GroupRecall {
        time: i64,
        self_id: i64,
        group_id: i64,
        user_id: i64,
        operator_id: Option<i64>,
        message_id: i64,
    },
    /// A private message was recalled.
    FriendRecall {
        time: i64,
        self_id: i64,
        user_id: i64,
        message_id: i64,
    },
    /// Group events: poke, honor, lucky king (`sub_type` distinguishes).
    /// The OneBot wire tag for this family is `"notify"`, not `"group_notify"`.
    #[serde(rename = "notify")]
    GroupNotify {
        time: i64,
        self_id: i64,
        group_id: i64,
        user_id: Option<i64>,
        target_id: Option<i64>,
        sub_type: String,
    },
    /// A member's group card was changed.
    GroupCard {
        time: i64,
        self_id: i64,
        group_id: i64,
        user_id: i64,
        card_new: String,
        card_old: String,
    },
    /// 未识别的 notice_type（如 group_essence/group_lucky_king）。
    #[serde(other)]
    Unknown,
}

impl NoticeEvent {
    pub fn self_id(&self) -> i64 {
        match self {
            NoticeEvent::GroupUpload { self_id, .. }
            | NoticeEvent::GroupAdmin { self_id, .. }
            | NoticeEvent::GroupDecrease { self_id, .. }
            | NoticeEvent::GroupIncrease { self_id, .. }
            | NoticeEvent::GroupBan { self_id, .. }
            | NoticeEvent::FriendAdd { self_id, .. }
            | NoticeEvent::GroupRecall { self_id, .. }
            | NoticeEvent::FriendRecall { self_id, .. }
            | NoticeEvent::GroupNotify { self_id, .. }
            | NoticeEvent::GroupCard { self_id, .. } => *self_id,
            NoticeEvent::Unknown => 0,
        }
    }
}

/// Request events (friend requests, group invitations), discriminated by
/// `request_type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "request_type", rename_all = "snake_case")]
pub enum RequestEvent {
    /// Someone wants to add the bot as a friend.
    Friend {
        time: i64,
        self_id: i64,
        user_id: i64,
        comment: String,
        flag: String,
    },
    /// Someone invited the bot to a group or a user requests to join.
    Group {
        time: i64,
        self_id: i64,
        /// `add` | `invite`
        sub_type: String,
        group_id: i64,
        user_id: i64,
        comment: String,
        flag: String,
    },
    #[serde(other)]
    Unknown,
}

impl RequestEvent {
    pub fn self_id(&self) -> i64 {
        match self {
            RequestEvent::Friend { self_id, .. } => *self_id,
            RequestEvent::Group { self_id, .. } => *self_id,
            RequestEvent::Unknown => 0,
        }
    }
}

/// Meta events, discriminated by `meta_event_type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "meta_event_type", rename_all = "snake_case")]
pub enum MetaEvent {
    /// `enable` | `disable` | `connect`
    Lifecycle {
        time: i64,
        self_id: i64,
        sub_type: String,
    },
    Heartbeat {
        time: i64,
        self_id: i64,
        status: Value,
        interval: i64,
    },
    #[serde(other)]
    Unknown,
}

impl MetaEvent {
    pub fn self_id(&self) -> i64 {
        match self {
            MetaEvent::Lifecycle { self_id, .. } => *self_id,
            MetaEvent::Heartbeat { self_id, .. } => *self_id,
            MetaEvent::Unknown => 0,
        }
    }
}

/// 表情类段的 `result` 字段（骰子点数 / 猜拳结果；字符串或数字皆可）。
fn segment_result(data: &Value) -> Option<String> {
    let value = data.get("result")?;
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_i64().map(|n| n.to_string()))
}

/// `[骰子:4]`；无结果字段时 `[骰子]`。
pub(crate) fn dice_marker(data: &Value) -> String {
    match segment_result(data) {
        Some(result) => format!("[骰子:{result}]"),
        None => "[骰子]".to_string(),
    }
}

/// `[石头剪刀布:剪刀]`（OneBot v11：1 石头、2 剪刀、3 布）；未知结果时回退数字。
pub(crate) fn rps_marker(data: &Value) -> String {
    let Some(result) = segment_result(data) else {
        return "[石头剪刀布]".to_string();
    };
    match result.as_str() {
        "1" => "[石头剪刀布:石头]".to_string(),
        "2" => "[石头剪刀布:剪刀]".to_string(),
        "3" => "[石头剪刀布:布]".to_string(),
        _ => format!("[石头剪刀布:{result}]"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_private_message() {
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "message",
                "message_type": "private",
                "time": 1696352000,
                "self_id": 10001,
                "sub_type": "friend",
                "message_id": 9001,
                "user_id": 20001,
                "message": [
                    {"type": "text", "data": {"text": "hello"}},
                    {"type": "face", "data": {"id": 123}}
                ],
                "raw_message": "hello[CQ:face,id=123]",
                "font": 14,
                "sender": {"user_id": 20001, "nickname": "Alice", "sex": "female", "age": 18}
            }"#,
        )
        .unwrap();

        let msg = event.as_message().expect("should be a message event");
        match msg {
            MessageEvent::Private {
                user_id,
                self_id,
                message,
                ..
            } => {
                assert_eq!(*user_id, 20001);
                assert_eq!(*self_id, 10001);
                assert_eq!(message.len(), 2);
                assert_eq!(msg.plain_text(), "hello");
                assert_eq!(msg.user_id(), 20001);
                assert_eq!(msg.group_id(), None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn readable_text_renders_faces_and_keeps_plain_text_pure() {
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "message",
                "message_type": "private",
                "time": 1696352000,
                "self_id": 10001,
                "sub_type": "friend",
                "message_id": 9100,
                "user_id": 20001,
                "message": [
                    {"type": "text", "data": {"text": "你好"}},
                    {"type": "face", "data": {"id": "13"}},
                    {"type": "text", "data": {"text": "在吗"}},
                    {"type": "face", "data": {"id": "99999"}}
                ],
                "raw_message": "你好[CQ:face,id=13]在吗",
                "font": 0,
                "sender": {"user_id": 20001, "nickname": "Alice"}
            }"#,
        )
        .unwrap();
        let msg = event.as_message().unwrap();
        // 可读渲染：文本原样 + 表情标记（未知 id 保留数字，可排查）。
        assert_eq!(msg.readable_text(), "你好[表情:呲牙]在吗[表情:99999]");
        // 命令解析等场景仍用纯文本：表情不进入 plain_text。
        assert_eq!(msg.plain_text(), "你好在吗");
    }

    #[test]
    fn readable_text_face_only_message_is_not_empty() {
        // 纯表情消息此前 content 为空 → 被适配器整条丢弃；可读渲染非空。
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "message",
                "message_type": "private",
                "time": 1696352000,
                "self_id": 10001,
                "sub_type": "friend",
                "message_id": 9101,
                "user_id": 20001,
                "message": [{"type": "face", "data": {"id": "14"}}],
                "raw_message": "[CQ:face,id=14]",
                "font": 0,
                "sender": {"user_id": 20001, "nickname": "Alice"}
            }"#,
        )
        .unwrap();
        let msg = event.as_message().unwrap();
        assert_eq!(msg.readable_text(), "[表情:微笑]");
        assert!(msg.plain_text().is_empty());
    }

    #[test]
    fn readable_text_renders_dice_rps_and_poke() {
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "message",
                "message_type": "group",
                "time": 1696352000,
                "self_id": 10001,
                "sub_type": "normal",
                "message_id": 9102,
                "group_id": 30001,
                "user_id": 20001,
                "message": [
                    {"type": "dice", "data": {"result": "4"}},
                    {"type": "rps", "data": {"result": 2}},
                    {"type": "poke", "data": {"type": "1", "id": "2"}}
                ],
                "raw_message": "",
                "font": 0,
                "sender": {"user_id": 20001, "nickname": "Alice"}
            }"#,
        )
        .unwrap();
        let msg = event.as_message().unwrap();
        assert_eq!(msg.readable_text(), "[骰子:4][石头剪刀布:剪刀][戳一戳]");
    }

    #[test]
    fn readable_text_skips_media_but_keeps_order() {
        // 图片/文件等段不进入文本渲染（分别经 images/files 通道），
        // 但文本顺序不受影响。
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "message",
                "message_type": "private",
                "time": 1696352000,
                "self_id": 10001,
                "sub_type": "friend",
                "message_id": 9103,
                "user_id": 20001,
                "message": [
                    {"type": "text", "data": {"text": "看图 "}},
                    {"type": "image", "data": {"file": "a.png", "url": "https://example.com/a.png"}},
                    {"type": "text", "data": {"text": " 表情 "}},
                    {"type": "face", "data": {"id": "74"}}
                ],
                "raw_message": "看图 [CQ:image,file=a.png] 表情 [CQ:face,id=74]",
                "font": 0,
                "sender": {"user_id": 20001, "nickname": "Alice"}
            }"#,
        )
        .unwrap();
        let msg = event.as_message().unwrap();
        // 74 = 太阳（NapCat sysface id 空间；注意与旧式 CQ 表不同）。
        assert_eq!(msg.readable_text(), "看图  表情 [表情:太阳]");
    }

    #[test]
    fn parse_group_message() {
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "message",
                "message_type": "group",
                "time": 1696352001,
                "self_id": 10001,
                "sub_type": "normal",
                "message_id": 9002,
                "group_id": 30001,
                "user_id": 20001,
                "anonymous": null,
                "message": [
                    {"type": "at", "data": {"qq": "10001", "name": "Bot"}},
                    {"type": "text", "data": {"text": "hi"}}
                ],
                "raw_message": "[CQ:at,qq=10001]hi",
                "font": 14,
                "sender": {
                    "user_id": 20001, "nickname": "Alice", "card": "Card",
                    "sex": "female", "age": 18, "area": "", "level": "1",
                    "role": "member", "title": ""
                }
            }"#,
        )
        .unwrap();

        let msg = event.as_message().unwrap();
        match msg {
            MessageEvent::Group { group_id, .. } => {
                assert_eq!(*group_id, 30001);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(msg.group_id(), Some(30001));
        assert!(msg.is_group());
        assert!(msg.at_me());
        assert_eq!(msg.sender_nickname(), "Card");
        assert_eq!(msg.plain_text(), "hi");
    }

    #[test]
    fn parse_meta_heartbeat() {
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "meta_event",
                "meta_event_type": "heartbeat",
                "time": 1696352002,
                "self_id": 10001,
                "status": {"online": true, "good": true},
                "interval": 5000
            }"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Meta {
                inner: MetaEvent::Heartbeat { interval: 5000, .. }
            }
        ));
    }

    #[test]
    fn parse_lifecycle() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"meta_event","meta_event_type":"lifecycle","time":1696352003,"self_id":10001,"sub_type":"connect"}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Meta {
                inner: MetaEvent::Lifecycle { sub_type, .. }
            } if sub_type == "connect"
        ));
    }

    #[test]
    fn parse_group_increase_notice() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_increase","time":1696352004,"self_id":10001,"group_id":30001,"operator_id":20001,"user_id":40001,"sub_type":"approve"}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Notice {
                inner: NoticeEvent::GroupIncrease { user_id: 40001, .. }
            }
        ));
    }

    #[test]
    fn parse_friend_request() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"request","request_type":"friend","time":1696352005,"self_id":10001,"user_id":50001,"comment":"hello","flag":"abc123"}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Request {
                inner: RequestEvent::Friend { user_id: 50001, .. }
            }
        ));
    }

    #[test]
    fn serialize_roundtrip() {
        let raw = r#"{
            "post_type": "message",
            "message_type": "private",
            "time": 1696352000,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 9001,
            "user_id": 20001,
            "message": [{"type": "text", "data": {"text": "hi"}}],
            "raw_message": "hi",
            "font": 14,
            "sender": {"user_id": 20001, "nickname": "Alice", "sex": "female", "age": 18}
        }"#;
        let event: Event = serde_json::from_str(raw).unwrap();
        let json = serde_json::to_value(&event).unwrap();
        let back: Event = serde_json::from_value(json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn reply_to_detection() {
        let event: Event = serde_json::from_str(
            r#"{
                "post_type": "message",
                "message_type": "group",
                "time": 1, "self_id": 10001, "sub_type": "normal",
                "message_id": 1, "group_id": 30001, "user_id": 20001,
                "anonymous": null,
                "message": [
                    {"type": "reply", "data": {"id": "42"}},
                    {"type": "text", "data": {"text": "hi"}}
                ],
                "raw_message": "[CQ:reply,id=42]hi", "font": 14,
                "sender": {"user_id": 20001, "nickname": "Alice"}
            }"#,
        )
        .unwrap();
        let msg = event.as_message().unwrap();
        assert_eq!(msg.reply_to(), Some("42"));
    }

    #[test]
    fn parse_group_upload_notice() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_upload","time":1,"self_id":10001,"group_id":30001,"user_id":20001,"file":{"id":"f1","name":"x.pdf","size":100,"busid":0}}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Notice {
                inner: NoticeEvent::GroupUpload { user_id: 20001, .. }
            }
        ));
    }

    #[test]
    fn parse_group_admin_notice() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_admin","time":1,"self_id":10001,"group_id":30001,"user_id":20001,"sub_type":"set"}"#,
        )
        .unwrap();
        match event {
            Event::Notice {
                inner: NoticeEvent::GroupAdmin { sub_type, .. },
            } => assert_eq!(sub_type, "set"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_group_decrease_notice() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_decrease","time":1,"self_id":10001,"group_id":30001,"operator_id":50001,"user_id":20001,"sub_type":"kick"}"#,
        )
        .unwrap();
        match event {
            Event::Notice {
                inner:
                    NoticeEvent::GroupDecrease {
                        operator_id,
                        sub_type,
                        ..
                    },
            } => {
                assert_eq!(operator_id, Some(50001));
                assert_eq!(sub_type, "kick");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_group_ban_notice() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_ban","time":1,"self_id":10001,"group_id":30001,"operator_id":50001,"user_id":20001,"sub_type":"ban","duration":600}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Notice {
                inner: NoticeEvent::GroupBan { duration: 600, .. }
            }
        ));
    }

    #[test]
    fn parse_recall_notices() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_recall","time":1,"self_id":10001,"group_id":30001,"user_id":20001,"operator_id":50001,"message_id":42}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Notice {
                inner: NoticeEvent::GroupRecall { message_id: 42, .. }
            }
        ));
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"friend_recall","time":1,"self_id":10001,"user_id":20001,"message_id":7}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Notice {
                inner: NoticeEvent::FriendRecall { message_id: 7, .. }
            }
        ));
    }

    #[test]
    fn parse_group_notify_poke() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"notify","time":1,"self_id":10001,"group_id":30001,"user_id":20001,"target_id":10001,"sub_type":"poke"}"#,
        )
        .unwrap();
        match event {
            Event::Notice {
                inner:
                    NoticeEvent::GroupNotify {
                        sub_type,
                        target_id,
                        ..
                    },
            } => {
                assert_eq!(sub_type, "poke");
                assert_eq!(target_id, Some(10001));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_group_card_notice() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_card","time":1,"self_id":10001,"group_id":30001,"user_id":20001,"card_new":"New","card_old":"Old"}"#,
        )
        .unwrap();
        match event {
            Event::Notice {
                inner: NoticeEvent::GroupCard { card_new, .. },
            } => assert_eq!(card_new, "New"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_notice_type_is_preserved() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"group_essence","time":1,"self_id":10001,"group_id":30001,"user_id":20001,"operator_id":50001,"message_id":9}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Notice {
                inner: NoticeEvent::Unknown
            }
        ));
    }

    #[test]
    fn parse_group_request() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"request","request_type":"group","time":1,"self_id":10001,"sub_type":"add","group_id":30001,"user_id":20001,"comment":"pls","flag":"g123"}"#,
        )
        .unwrap();
        match event {
            Event::Request {
                inner:
                    RequestEvent::Group {
                        sub_type,
                        group_id,
                        flag,
                        ..
                    },
            } => {
                assert_eq!(sub_type, "add");
                assert_eq!(group_id, 30001);
                assert_eq!(flag, "g123");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_friend_add_notice() {
        let event: Event = serde_json::from_str(
            r#"{"post_type":"notice","notice_type":"friend_add","time":1,"self_id":10001,"user_id":20001}"#,
        )
        .unwrap();
        assert!(matches!(
            event,
            Event::Notice {
                inner: NoticeEvent::FriendAdd { user_id: 20001, .. }
            }
        ));
    }

    // ── Property tests ─────────────────────────────────────────────────────

    use proptest::prelude::*;
    use serde_json::json;

    fn message_event_json(text: &str, user_id: i64, group_id: i64, is_group: bool) -> Value {
        let message = json!([{"type": "text", "data": {"text": text}}]);
        let sender = json!({"user_id": user_id, "nickname": "prop-user"});
        if is_group {
            json!({
                "post_type": "message",
                "message_type": "group",
                "time": 1700000000,
                "self_id": 10001,
                "sub_type": "normal",
                "message_id": 1,
                "group_id": group_id,
                "user_id": user_id,
                "message": message,
                "raw_message": text,
                "font": 0,
                "sender": sender
            })
        } else {
            json!({
                "post_type": "message",
                "message_type": "private",
                "time": 1700000000,
                "self_id": 10001,
                "sub_type": "friend",
                "message_id": 1,
                "user_id": user_id,
                "message": message,
                "raw_message": text,
                "font": 0,
                "sender": sender
            })
        }
    }

    proptest! {
        /// Any message text (Unicode, control chars, quotes) round-trips
        /// stably: parse → serialize → parse yields the same event.
        #[test]
        fn message_event_json_roundtrip_is_stable(
            text in ".*",
            user_id in 0i64..1_000_000_000i64,
            group_id in 0i64..1_000_000_000i64,
            is_group in proptest::bool::ANY,
        ) {
            let raw = message_event_json(&text, user_id, group_id, is_group);
            let ev: Event = serde_json::from_value(raw).expect("parse");
            let ev2: Event =
                serde_json::from_value(serde_json::to_value(&ev).expect("serialize"))
                    .expect("re-parse");
            prop_assert_eq!(ev, ev2);
        }

        /// Segment lists with arbitrary text survive message round-trips.
        #[test]
        fn segment_rich_message_roundtrip(
            texts in proptest::collection::vec("[a-zA-Z0-9 你好世界]{0,20}", 0..8),
            user_id in 0i64..1_000_000i64,
        ) {
            let message: Vec<Value> = texts
                .iter()
                .map(|t| json!({"type": "text", "data": {"text": t}}))
                .collect();
            let raw = json!({
                "post_type": "message",
                "message_type": "private",
                "time": 1,
                "self_id": 10001,
                "sub_type": "friend",
                "message_id": 1,
                "user_id": user_id,
                "message": message,
                "raw_message": texts.join(""),
                "font": 0,
                "sender": {"user_id": user_id, "nickname": "x"}
            });
            let ev: Event = serde_json::from_value(raw).expect("parse");
            let ev2: Event =
                serde_json::from_value(serde_json::to_value(&ev).expect("serialize"))
                    .expect("re-parse");
            prop_assert_eq!(ev, ev2);
        }
    }

    fn group_msg_with_at(at_qq: Option<&str>) -> Event {
        let at_segment = match at_qq {
            Some(qq) => serde_json::json!({"type": "at", "data": {"qq": qq}}),
            None => serde_json::json!({"type": "text", "data": {"text": "hi"}}),
        };
        serde_json::from_value(json!({
            "post_type": "message",
            "message_type": "group",
            "time": 1,
            "self_id": 10001,
            "sub_type": "normal",
            "message_id": 1,
            "group_id": 30001,
            "user_id": 20001,
            "message": [at_segment],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 20001, "nickname": "x"}
        }))
        .unwrap()
    }

    #[test]
    fn has_at_matches_specific_user_and_all() {
        let event = group_msg_with_at(Some("10001"));
        let msg = event.as_message().unwrap();
        assert!(msg.has_at(10001));
        assert!(!msg.has_at(20002), "other user not mentioned");
        assert!(msg.at_me(), "self_id 10001 is mentioned");

        let event = group_msg_with_at(Some("all"));
        let all = event.as_message().unwrap();
        assert!(all.has_at(10001), "@all matches anyone");
        assert!(all.at_me());
    }

    #[test]
    fn has_at_false_without_mentions() {
        let event = group_msg_with_at(None);
        let msg = event.as_message().unwrap();
        assert!(!msg.has_at(10001));
        assert!(!msg.at_me());
    }

    fn private_msg_with_nickname(nickname: &str) -> Event {
        serde_json::from_value(json!({
            "post_type": "message",
            "message_type": "private",
            "time": 1,
            "self_id": 10001,
            "sub_type": "friend",
            "message_id": 1,
            "user_id": 20001,
            "message": [{"type": "text", "data": {"text": "hi"}}],
            "raw_message": "hi",
            "font": 0,
            "sender": {"user_id": 20001, "nickname": nickname}
        }))
        .unwrap()
    }

    fn group_msg_with_card(card: Option<&str>) -> Event {
        let mut sender = json!({"user_id": 20001, "nickname": "nick"});
        if let Some(card) = card {
            sender["card"] = json!(card);
        }
        serde_json::from_value(json!({
            "post_type": "message",
            "message_type": "group",
            "time": 1,
            "self_id": 10001,
            "sub_type": "normal",
            "message_id": 1,
            "group_id": 30001,
            "user_id": 20001,
            "message": [{"type": "text", "data": {"text": "hi"}}],
            "raw_message": "hi",
            "font": 0,
            "sender": sender
        }))
        .unwrap()
    }

    #[test]
    fn sender_nickname_prefers_group_card() {
        let event = group_msg_with_card(Some("群名片"));
        let msg = event.as_message().unwrap();
        assert_eq!(msg.sender_nickname(), "群名片");
    }

    #[test]
    fn sender_nickname_falls_back_to_nickname() {
        // No card field at all.
        let event = group_msg_with_card(None);
        assert_eq!(event.as_message().unwrap().sender_nickname(), "nick");
        // Empty card must also fall back.
        let event = group_msg_with_card(Some(""));
        assert_eq!(event.as_message().unwrap().sender_nickname(), "nick");
    }

    #[test]
    fn private_sender_nickname_is_used_directly() {
        let event = private_msg_with_nickname("私聊昵称");
        assert_eq!(event.as_message().unwrap().sender_nickname(), "私聊昵称");
    }
}
