//! 入站消息转换（OneBot Event → IncomingMessage）。
//!
//! 从 `adapter/qq/mod.rs` 拆出（框架优化议题 3）：CQ 码/图片/文件/
//! 表情/dice/rps 分段解析，纯函数性质（只读 group_names 缓存），
//! 独立可测。

use super::*;

impl QqAdapter {
    /// Legacy/测试兼容入口（默认实例 `qq`）；生产 handler 走
    /// [`Self::convert_message_for`] 传实例名。
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn convert_message(
        event: &OneBotEvent,
        group_names: &dashmap::DashMap<i64, String>,
    ) -> Option<IncomingMessage> {
        Self::convert_message_for(crate::adapter::DEFAULT_INSTANCE_NAME, event, group_names)
    }

    /// 多实例变体：归属实例名由调用方传入（入站会话/连接事件的
    /// `@<实例>` 维度）。`convert_message` 保留默认实例兼容（测试与
    /// legacy 路径）；生产 handler 一律走本方法传 `inner.instance_name`。
    pub(crate) fn convert_message_for(
        adapter_name: &str,
        event: &OneBotEvent,
        group_names: &dashmap::DashMap<i64, String>,
    ) -> Option<IncomingMessage> {
        let msg = event.as_message()?;
        if matches!(msg, MessageEvent::Unknown) {
            return None;
        }
        // 内容用「可读渲染」：文本 + 表情类标记（[表情:微笑] / [骰子:4] /
        // [石头剪刀布:布] / [戳一戳]）。此前用 plain_text()，消息里的 QQ
        // 表情被整个丢弃——纯表情消息 content 为空还会被整条丢弃，
        // agent 完全「读不到」表情。
        let content = msg.readable_text();
        let images: Vec<String> = msg
            .message()
            .iter()
            .filter_map(|seg| match seg {
                echo_core::segment::Segment::Known(echo_core::segment::KnownSegment::Image {
                    data,
                }) => data
                    .url
                    .clone()
                    .or_else(|| (!data.file.is_empty()).then(|| data.file.clone())),
                _ => None,
            })
            .collect();
        // 文件段（NapCat 扩展）：**私聊专收**。群聊的文件消息由 group_upload
        // 通知负责——同一上传会在 NapCat 侧双上报（message + notice），这里
        // 跳过群聊 file 段，避免重复下载与重复送达。
        // pending 明细（file_id/url）经 metadata 传给 handler 去换直链并下载；
        // files 先放占位条目（name/size），两者按顺序一一对应。
        let (channel, group_name) = if let Some(gid) = msg.group_id() {
            (
                ChannelType::Group {
                    group_id: gid.to_string(),
                },
                group_names.get(&gid).map(|name| name.clone()),
            )
        } else {
            (ChannelType::Direct, None)
        };
        let mut files: Vec<IncomingFile> = Vec::new();
        let mut pending: Vec<serde_json::Value> = Vec::new();
        if !channel.is_group() {
            for seg in msg.message() {
                match seg {
                    echo_core::segment::Segment::Known(
                        echo_core::segment::KnownSegment::File { data },
                    ) => {
                        let name = if data.file.trim().is_empty() {
                            "未命名文件".to_string()
                        } else {
                            data.file.clone()
                        };
                        let size = data
                            .file_size
                            .as_deref()
                            .and_then(|raw| raw.parse::<u64>().ok())
                            .unwrap_or(0);
                        files.push(IncomingFile {
                            name: name.clone(),
                            path: None,
                            size,
                            error: None,
                        });
                        pending.push(serde_json::json!({
                            "name": name,
                            "size": size,
                            "file_id": data.file_id,
                            "url": data.url,
                        }));
                    }
                    echo_core::segment::Segment::Known(
                        echo_core::segment::KnownSegment::OnlineFile { data },
                    ) => {
                        // 「在线文件/文件夹」（QQ 直传，NapCat elementType 23/30）：
                        // 不提供直链，必须先经 receive_online_file 触发接收，
                        // 且接收后字节仍在 NapCat 侧（容器内）无法取回——
                        // 暂不支持自动接收。但**不静默丢弃**：以显式错误条目
                        // 送达，让 agent 如实告知用户（用户才能改用其他方式发）。
                        let kind = if data.is_dir {
                            "在线文件夹"
                        } else {
                            "在线文件"
                        };
                        let name = if data.file_name.trim().is_empty() {
                            "未命名文件".to_string()
                        } else {
                            data.file_name.clone()
                        };
                        let size = data
                            .file_size
                            .as_deref()
                            .and_then(|raw| raw.parse::<u64>().ok())
                            .unwrap_or(0);
                        files.push(IncomingFile {
                            name,
                            path: None,
                            size,
                            error: Some(format!("{kind}（QQ 直传）暂不支持自动接收")),
                        });
                    }
                    _ => {}
                }
            }
        }
        // 纯图片/纯文件消息（无文本）也要送达：图片经 images、文件经 files
        // 字段传递，模型据此解读或读取本地文件。
        if content.trim().is_empty() && images.is_empty() && files.is_empty() {
            return None;
        }
        let metadata = if pending.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::json!({ "pending_files": pending })
        };
        Some(IncomingMessage {
            adapter_name: adapter_name.to_string(),
            platform: "qq".into(),
            user_id: msg.user_id().to_string(),
            user_name: msg.sender_nickname().to_string(),
            channel,
            group_name,
            content,
            timestamp: msg.timestamp(),
            at_me: msg.at_me(),
            metadata,
            images,
            files,
        })
    }
}
