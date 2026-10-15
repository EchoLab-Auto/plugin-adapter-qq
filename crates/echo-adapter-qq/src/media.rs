//! 媒体登记通道（P1.7c）：插件 → 宿主 `adapter_host.store_media` 回呼的
//! 插件侧抽象。
//!
//! 入站图片下载后不再本地落盘（旧 `media_store` 行为），而是经本通道把
//! 字节交给宿主的媒体库统一登记（内容寻址命名 + 同目录复用），拿回
//! `/media/<id>` 引用；模型侧还原由宿主承担（`inline_media_refs_in_messages`）。

use async_trait::async_trait;

/// 媒体引用的 URL 前缀（`/media/<id>`；与宿主 media_store 同口径）。
pub const MEDIA_URL_PREFIX: &str = "/media/";

/// 是否为媒体引用（宿主媒体库的内容寻址引用）。
pub fn is_media_ref(value: &str) -> bool {
    value.starts_with(MEDIA_URL_PREFIX)
}

/// 图片登记：字节 + MIME → `/media/<id>` 引用。
///
/// 实现见 `plugin.rs`（`HostMediaStore`：写临时文件 → `store_media`
/// HostCall（同机传 path，避免大载荷过协议帧）→ 清理临时文件）。
#[async_trait]
pub trait MediaStore: Send + Sync {
    /// 登记一张图片，返回宿主媒体引用。
    async fn store_image(&self, bytes: &[u8], mime: &str) -> Result<String, String>;
}
