//! OneBot v11 message segments.
//!
//! A message is an array of segments, each shaped like
//! `{"type": "<segment_type>", "data": {...}}`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single message segment.
///
/// Known segment types are fully typed. Anything unrecognized — NapCat or
/// future extensions — is preserved verbatim in [`Segment::Unknown`] so it
/// survives serialization round-trips.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Segment {
    Known(KnownSegment),
    Unknown(Value),
}

/// Known segment types, discriminated by the `type` field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KnownSegment {
    /// Plain text. `{"type":"text","data":{"text":"hi"}}`
    Text { data: TextData },
    /// QQ built-in emoji. `{"type":"face","data":{"id":"123"}}`
    Face { data: FaceData },
    /// Image. `file` may be a URL, a local path, or `base64://...`.
    Image { data: ImageData },
    /// Voice message. `{"type":"record","data":{"file":"..."}}`
    Record { data: RecordData },
    /// File attachment (NapCat extension). `file` is the display name,
    /// `file_id` the NapCat file UUID; `url` is only present when NapCat's
    /// packet channel could resolve a direct link at conversion time.
    File { data: FileData },
    /// Online file / folder (NapCat extension, elementType 23/30):
    /// no direct link — downloadable only via `get_file` keyed by
    /// msgId+elementId, which the adapter does not support yet.
    #[serde(rename = "onlinefile")]
    OnlineFile { data: OnlineFileData },
    /// Video. `{"type":"video","data":{"file":"..."}}`
    Video { data: VideoData },
    /// @ a user; `qq = "all"` mentions everyone in a group.
    At { data: AtData },
    /// Reply to a previous message. `{"type":"reply","data":{"id":"9001"}}`
    Reply { data: ReplyData },
    /// Forwarded message list (for `send_group_forward_msg`).
    Forward { data: ForwardData },
    /// A message node inside a forward chain.
    Node { data: NodeData },
    /// Rich XML message. `{"type":"xml","data":{"data":"<msg>..."}}`
    Xml { data: XmlData },
    /// Rich JSON message (JSON string). `{"type":"json","data":{"data":"{...}"}}`
    Json { data: JsonData },
    /// Poke / poke sticker. `{"type":"poke","data":{"type":"1","id":"..."}}`
    Poke { data: PokeData },
    /// Card image. `{"type":"cardimage","data":{"file":"..."}}`
    #[serde(rename = "cardimage")]
    CardImage { data: CardImageData },
    /// Dice roll — the spec defines no `data` payload.
    Dice {
        #[serde(default, skip_serializing_if = "Value::is_null")]
        data: Value,
    },
    /// Rock-paper-scissors — no payload.
    Rps {
        #[serde(default, skip_serializing_if = "Value::is_null")]
        data: Value,
    },
    /// Shake window — no payload.
    Shake {
        #[serde(default, skip_serializing_if = "Value::is_null")]
        data: Value,
    },
}

impl Segment {
    pub fn text(content: impl Into<String>) -> Self {
        Segment::Known(KnownSegment::Text {
            data: TextData {
                text: content.into(),
            },
        })
    }

    pub fn at(qq: impl Into<String>) -> Self {
        Segment::Known(KnownSegment::At {
            data: AtData {
                qq: qq.into(),
                name: None,
            },
        })
    }

    pub fn at_all() -> Self {
        Segment::at("all")
    }

    pub fn image(file: impl Into<String>) -> Self {
        Segment::Known(KnownSegment::Image {
            data: ImageData {
                file: file.into(),
                url: None,
                flash: None,
                cache: None,
                proxy: None,
                timeout: None,
            },
        })
    }

    pub fn face(id: impl Into<String>) -> Self {
        Segment::Known(KnownSegment::Face {
            data: FaceData { id: id.into() },
        })
    }

    pub fn reply(message_id: impl Into<String>) -> Self {
        Segment::Known(KnownSegment::Reply {
            data: ReplyData {
                id: message_id.into(),
            },
        })
    }

    /// Rich JSON card. `data` is the card payload as a JSON string.
    pub fn json(data: impl Into<String>) -> Self {
        Segment::Known(KnownSegment::Json {
            data: JsonData { data: data.into() },
        })
    }

    /// Voice message. `file` may be a path (host/container), an `http(s)://`
    /// URL, or a `base64://` payload — the OneBot implementation resolves and
    /// converts it (NapCat: mp3/amr/wav/silk…, auto-converted to QQ voice).
    pub fn record(file: impl Into<String>) -> Self {
        Segment::Known(KnownSegment::Record {
            data: RecordData {
                file: file.into(),
                url: None,
                magic: None,
            },
        })
    }
}

/// Render a segment list as a readable **transcript** body — richer than
/// [`MessageEvent::readable_text`](crate::event::MessageEvent::readable_text):
/// media segments become compact markers (`[图片]` / `[语音]` / `[文件:名]`),
/// @-mentions render as `@name`/`@qq`, and card/forward/reply segments get
/// markers too. Used to present fetched message history (e.g. NapCat's
/// `get_group_msg_history`) without binary payloads.
pub fn render_transcript(segments: &[Segment]) -> String {
    let mut out = String::new();
    for seg in segments {
        match seg {
            Segment::Known(KnownSegment::Text { data }) => out.push_str(&data.text),
            Segment::Known(KnownSegment::Face { data }) => {
                out.push_str(&crate::face::face_marker(&data.id));
            }
            Segment::Known(KnownSegment::Image { .. })
            | Segment::Known(KnownSegment::CardImage { .. }) => out.push_str("[图片]"),
            Segment::Known(KnownSegment::Record { .. }) => out.push_str("[语音]"),
            Segment::Known(KnownSegment::Video { .. }) => out.push_str("[视频]"),
            Segment::Known(KnownSegment::File { data }) => {
                let name = data.file.trim();
                if name.is_empty() {
                    out.push_str("[文件]");
                } else {
                    out.push_str(&format!("[文件:{name}]"));
                }
            }
            Segment::Known(KnownSegment::OnlineFile { data }) => {
                let name = data.file_name.trim();
                if name.is_empty() {
                    out.push_str("[在线文件]");
                } else {
                    out.push_str(&format!("[在线文件:{name}]"));
                }
            }
            Segment::Known(KnownSegment::At { data }) => {
                if data.qq == "all" {
                    out.push_str("@全体成员");
                } else if let Some(name) = data.name.as_deref().filter(|n| !n.trim().is_empty()) {
                    out.push_str(&format!("@{}", name.trim()));
                } else {
                    out.push_str(&format!("@{}", data.qq));
                }
            }
            Segment::Known(KnownSegment::Reply { .. }) => out.push_str("[回复]"),
            Segment::Known(KnownSegment::Forward { .. })
            | Segment::Known(KnownSegment::Node { .. }) => out.push_str("[转发消息]"),
            Segment::Known(KnownSegment::Xml { .. })
            | Segment::Known(KnownSegment::Json { .. }) => out.push_str("[卡片]"),
            Segment::Known(KnownSegment::Poke { .. }) => out.push_str("[戳一戳]"),
            Segment::Known(KnownSegment::Dice { data }) => {
                out.push_str(&crate::event::dice_marker(data));
            }
            Segment::Known(KnownSegment::Rps { data }) => {
                out.push_str(&crate::event::rps_marker(data));
            }
            Segment::Known(KnownSegment::Shake { .. }) => out.push_str("[窗口抖动]"),
            Segment::Unknown(_) => {}
        }
    }
    out
}

// -- data payloads ----------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TextData {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FaceData {
    /// May arrive as a number or a string; normalized to a string.
    #[serde(deserialize_with = "de_string_or_int")]
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ImageData {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// 闪照标记（NapCat 用 flash 字段，部分实现用 type="flash"）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flash: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RecordData {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub magic: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct FileData {
    /// Display name of the file (NapCat: `file`).
    #[serde(default)]
    pub file: String,
    /// NapCat file UUID (usable with get_private_file_url / get_group_file_url).
    #[serde(default)]
    pub file_id: String,
    /// Size in bytes; implementations send it as a string or a number.
    #[serde(
        default,
        deserialize_with = "de_opt_string_or_int",
        skip_serializing_if = "Option::is_none"
    )]
    pub file_size: Option<String>,
    /// Direct download link, when available (packet channel online).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct OnlineFileData {
    /// Message id carrying the file element (NapCat uses camelCase keys).
    #[serde(default, rename = "msgId", deserialize_with = "de_string_or_int")]
    pub msg_id: String,
    /// Element id inside that message.
    #[serde(default, rename = "elementId", deserialize_with = "de_string_or_int")]
    pub element_id: String,
    #[serde(default, rename = "fileName")]
    pub file_name: String,
    #[serde(
        default,
        rename = "fileSize",
        deserialize_with = "de_opt_string_or_int",
        skip_serializing_if = "Option::is_none"
    )]
    pub file_size: Option<String>,
    #[serde(default, rename = "isDir")]
    pub is_dir: bool,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct VideoData {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct AtData {
    /// Target QQ number, or `"all"` for everyone.
    #[serde(deserialize_with = "de_string_or_int")]
    pub qq: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ReplyData {
    #[serde(deserialize_with = "de_string_or_int")]
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ForwardData {
    #[serde(deserialize_with = "de_string_or_int")]
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct NodeData {
    /// Set for received forward messages.
    #[serde(
        default,
        deserialize_with = "de_opt_string_or_int",
        skip_serializing_if = "Option::is_none"
    )]
    pub id: Option<String>,
    /// Set when building a forward chain to send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<Segment>>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct XmlData {
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct JsonData {
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PokeData {
    #[serde(
        rename = "type",
        default,
        deserialize_with = "de_opt_string_or_int",
        skip_serializing_if = "Option::is_none"
    )]
    pub poke_type: Option<String>,
    #[serde(
        default,
        deserialize_with = "de_opt_string_or_int",
        skip_serializing_if = "Option::is_none"
    )]
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CardImageData {
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minwidth: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minheight: Option<i64>,
}

// -- lenient deserializers ---------------------------------------------------

/// Deserialize a field that implementations may send as either a string or a
/// number, normalizing to a string.
fn de_string_or_int<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = String;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a string or an integer")
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            Ok(v)
        }

        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }

        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(v.to_string())
        }
    }

    deserializer.deserialize_any(Visitor)
}

/// Like [`de_string_or_int`], but for `Option<String>` fields.
fn de_opt_string_or_int<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    de_string_or_int(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_segment() {
        let seg: Segment = serde_json::from_str(r#"{"type":"text","data":{"text":"hi"}}"#).unwrap();
        assert_eq!(
            seg,
            Segment::Known(KnownSegment::Text {
                data: TextData {
                    text: "hi".to_string()
                }
            })
        );
    }

    #[test]
    fn face_id_accepts_number() {
        let seg: Segment = serde_json::from_str(r#"{"type":"face","data":{"id":123}}"#).unwrap();
        match seg {
            Segment::Known(KnownSegment::Face { data }) => assert_eq!(data.id, "123"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_segment_is_preserved() {
        let raw = r#"{"type":"custom_ext","data":{"some":1}}"#;
        let seg: Segment = serde_json::from_str(raw).unwrap();
        assert!(matches!(seg, Segment::Unknown(_)));
        // Compare as values: serde_json's map may reorder keys on serialize.
        assert_eq!(
            serde_json::to_value(&seg).unwrap(),
            serde_json::from_str::<Value>(raw).unwrap()
        );
    }

    #[test]
    fn dice_without_data() {
        let seg: Segment = serde_json::from_str(r#"{"type":"dice"}"#).unwrap();
        assert!(matches!(seg, Segment::Known(KnownSegment::Dice { .. })));
        assert_eq!(serde_json::to_string(&seg).unwrap(), r#"{"type":"dice"}"#);
    }

    #[test]
    fn builder_constructors() {
        let segs = vec![
            Segment::text("hi"),
            Segment::at("12345"),
            Segment::at_all(),
            Segment::image("https://example.com/a.png"),
            Segment::face("1"),
            Segment::reply("9001"),
            Segment::record("/app/napcat/data/hello.mp3"),
        ];
        let json = serde_json::to_value(&segs).unwrap();
        // record 段线格式：只带 file 字段（url/magic 缺省省略）。
        assert_eq!(
            json[6],
            serde_json::json!({"type": "record", "data": {"file": "/app/napcat/data/hello.mp3"}})
        );
        let back: Vec<Segment> = serde_json::from_value(json).unwrap();
        assert_eq!(segs, back);
    }

    #[test]
    fn render_transcript_marks_media_mentions_and_text() {
        let segs: Vec<Segment> = serde_json::from_value(serde_json::json!([
            {"type": "at", "data": {"qq": "2911759808"}},
            {"type": "text", "data": {"text": " 笑一个 "}},
            {"type": "image", "data": {"file": "a.jpg"}},
            {"type": "face", "data": {"id": "14"}},
            {"type": "record", "data": {"file": "b.amr"}},
            {"type": "file", "data": {"file": "report.pdf"}},
            {"type": "reply", "data": {"id": "9001"}},
            {"type": "json", "data": {"data": "{}"}},
            {"type": "at", "data": {"qq": "all"}},
        ]))
        .unwrap();
        assert_eq!(
            render_transcript(&segs),
            "@2911759808 笑一个 [图片][表情:微笑][语音][文件:report.pdf][回复][卡片]@全体成员"
        );
    }

    #[test]
    fn render_transcript_prefers_at_name_and_skips_unknown() {
        let segs: Vec<Segment> = serde_json::from_value(serde_json::json!([
            {"type": "at", "data": {"qq": "123", "name": "Evence"}},
            {"type": "custom_ext", "data": {"some": 1}},
            {"type": "text", "data": {"text": " hi"}},
        ]))
        .unwrap();
        assert_eq!(render_transcript(&segs), "@Evence hi");
    }

    #[test]
    fn parses_media_segments() {
        let record: Segment =
            serde_json::from_str(r#"{"type":"record","data":{"file":"a.amr"}}"#).unwrap();
        assert!(matches!(
            record,
            Segment::Known(KnownSegment::Record { .. })
        ));
        let video: Segment =
            serde_json::from_str(r#"{"type":"video","data":{"file":"b.mp4"}}"#).unwrap();
        assert!(matches!(video, Segment::Known(KnownSegment::Video { .. })));
        let image: Segment =
            serde_json::from_str(r#"{"type":"image","data":{"file":"c.png","url":"http://x"}}"#)
                .unwrap();
        match image {
            Segment::Known(KnownSegment::Image { data }) => {
                assert_eq!(data.file, "c.png");
                assert_eq!(data.url.as_deref(), Some("http://x"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_rich_and_action_segments() {
        let xml: Segment =
            serde_json::from_str(r#"{"type":"xml","data":{"data":"<msg>hi</msg>"}}"#).unwrap();
        assert!(matches!(xml, Segment::Known(KnownSegment::Xml { .. })));
        let json: Segment =
            serde_json::from_str(r#"{"type":"json","data":{"data":"{\"k\":1}"}}"#).unwrap();
        assert!(matches!(json, Segment::Known(KnownSegment::Json { .. })));
        let poke: Segment =
            serde_json::from_str(r#"{"type":"poke","data":{"type":"1","id":"2"}}"#).unwrap();
        assert!(matches!(poke, Segment::Known(KnownSegment::Poke { .. })));
        let card: Segment =
            serde_json::from_str(r#"{"type":"cardimage","data":{"file":"d.png"}}"#).unwrap();
        assert!(matches!(
            card,
            Segment::Known(KnownSegment::CardImage { .. })
        ));
        let rps: Segment = serde_json::from_str(r#"{"type":"rps"}"#).unwrap();
        assert!(matches!(rps, Segment::Known(KnownSegment::Rps { .. })));
        let shake: Segment = serde_json::from_str(r#"{"type":"shake"}"#).unwrap();
        assert!(matches!(shake, Segment::Known(KnownSegment::Shake { .. })));
    }

    #[test]
    fn parses_forward_and_node() {
        let forward: Segment =
            serde_json::from_str(r#"{"type":"forward","data":{"id":"123"}}"#).unwrap();
        assert!(matches!(
            forward,
            Segment::Known(KnownSegment::Forward { .. })
        ));
        let node: Segment = serde_json::from_str(r#"{"type":"node","data":{"id":"456"}}"#).unwrap();
        assert!(matches!(node, Segment::Known(KnownSegment::Node { .. })));
    }

    #[test]
    fn at_all_serializes_as_qq_all() {
        let seg = Segment::at_all();
        let json = serde_json::to_value(&seg).unwrap();
        assert_eq!(json["type"], "at");
        assert_eq!(json["data"]["qq"], "all");
    }

    #[test]
    fn parses_file_segment_with_url_and_size_as_string() {
        // NapCat 的 file 段：file=显示名、file_id=UUID、file_size 可能是字符串。
        let seg: Segment = serde_json::from_str(
            r#"{"type":"file","data":{"file":"report.pdf","file_id":"abc123","file_size":"2048","url":"https://cdn.example/x"}}"#,
        )
        .unwrap();
        match seg {
            Segment::Known(KnownSegment::File { data }) => {
                assert_eq!(data.file, "report.pdf");
                assert_eq!(data.file_id, "abc123");
                assert_eq!(data.file_size.as_deref(), Some("2048"));
                assert_eq!(data.url.as_deref(), Some("https://cdn.example/x"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_file_segment_without_url() {
        let seg: Segment = serde_json::from_str(
            r#"{"type":"file","data":{"file":"a.zip","file_id":"u1","file_size":1024}}"#,
        )
        .unwrap();
        match seg {
            Segment::Known(KnownSegment::File { data }) => {
                assert_eq!(data.file_size.as_deref(), Some("1024"));
                assert_eq!(data.url, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_online_file_segment() {
        let seg: Segment = serde_json::from_str(
            r#"{"type":"onlinefile","data":{"msgId":"7001","elementId":"e2","fileName":"big.bin","fileSize":"99","isDir":false}}"#,
        )
        .unwrap();
        match seg {
            Segment::Known(KnownSegment::OnlineFile { data }) => {
                assert_eq!(data.msg_id, "7001");
                assert_eq!(data.element_id, "e2");
                assert_eq!(data.file_name, "big.bin");
                assert!(!data.is_dir);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_fields_on_known_segments_are_ignored() {
        let seg: Segment =
            serde_json::from_str(r#"{"type":"text","data":{"text":"hi","extra_field":123}}"#)
                .unwrap();
        match seg {
            Segment::Known(KnownSegment::Text { data }) => assert_eq!(data.text, "hi"),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
