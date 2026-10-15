//! 入站图片的落盘缓存（媒体库）。
//!
//! 背景（2026-09-24 性能治理）：QQ 入站图片此前以内嵌 `data:` URI 的形式
//! 贯穿全链路——hook JSON、`MessageReceived` 事件、会话事件日志、显示
//! 时间线。单张 GIF 表情包可达数 MB，导致：面板启动拉取的时间线快照
//! 8MB+（实测），会话文件 24MB 且每写一次全量重写，实时订阅也被大帧拖慢。
//!
//! 新模型：**图片一律落盘，链路上只传引用**（`/media/<id>`）。
//! - 写入侧：QQ 适配器下载远端图片后落盘（[`save_image_bytes`]）；面板上传的
//!   data URI 由 Core 在入站时落盘（[`save_data_uri`]）。
//! - 展示侧：Panel 的 web 后端按 `/media/<id>` 提供文件（同机读同一目录），
//!   浏览器 `<img>` 懒加载 + 强缓存（内容哈希命名，天然不可变）。
//! - 模型侧：发往 LLM 前把引用还原为 data URI（[`inline_media_refs_in_messages`]），
//!   多模态端点照常收到图片；省下的只是链路与落盘的体积。
//!
//! 目录：`$ECHO_MEDIA_DIR`，缺省 `~/.local/share/echo-agent-core/media`
//! （与文件下载目录同一数据根）。文件名为内容哈希（FNV-1a 128 十六进制）
//! + 扩展名——同图去重、天然防目录穿越（id 字符集受限）。

use std::path::{Path, PathBuf};


/// 媒体引用的 URL 前缀（Panel web 后端的同源路由）。
pub const MEDIA_URL_PREFIX: &str = "/media/";

/// 时间线/事件里「图片已省略」的占位（数组保留该元素以维持计数）。
///
/// 仅用于落盘失败且原始 data URI 过大的兜底——正常情况下图片都能落盘、
/// 以引用形式保留。
pub const ELIDED_IMAGE: &str = "";

/// 达到该长度的内嵌 base64 才值得落盘：短串（小图标、测试夹具）保持原样，
/// 避免把小文件也变成磁盘依赖。
const SPILL_MIN_CHARS: usize = 4096;

/// 单文件落盘上限（防御异常输入；适配器侧另有 10MB 下载上限）。
const MAX_STORED_BYTES: usize = 32 * 1024 * 1024;

/// 媒体库目录：`$ECHO_MEDIA_DIR` 优先；否则 HOME 下的 Core 数据根；
/// 再否则工作目录下的 `media`（与文件下载目录的兜底策略一致）。
pub fn media_dir() -> PathBuf {
    media_dir_with_home(std::env::var_os("ECHO_MEDIA_DIR"), std::env::var_os("HOME"))
}

fn media_dir_with_home(
    override_dir: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> PathBuf {
    if let Some(dir) = override_dir.filter(|d| !d.is_empty()) {
        return PathBuf::from(dir);
    }
    match home {
        Some(home) => PathBuf::from(home).join(".local/share/echo-agent-core/media"),
        None => PathBuf::from("media"),
    }
}

/// 内容访问引用（`/media/<id>`）。
pub fn media_ref(id: &str) -> String {
    format!("{MEDIA_URL_PREFIX}{id}")
}

/// 从引用里取出 id；非引用或 id 含越界字符时返回 `None`。
///
/// id 字符集限定为 `[A-Za-z0-9._-]` 且不得为 `.`/`..`——磁盘读取侧
/// （Panel web 后端）同样校验，这里是源头约束。
pub fn id_of_ref(value: &str) -> Option<&str> {
    let id = value.strip_prefix(MEDIA_URL_PREFIX)?;
    if id.is_empty() || id == "." || id == ".." {
        return None;
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return None;
    }
    Some(id)
}

/// 是否为媒体库引用。
pub fn is_media_ref(value: &str) -> bool {
    id_of_ref(value).is_some()
}

/// 扩展名 → MIME（落盘时的映射；未知扩展名回退 `application/octet-stream`）。
fn mime_for_ext(ext: &str) -> &'static str {
    match ext.to_ascii_lowercase().as_str() {
        "gif" => "image/gif",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "svg" => "image/svg+xml",
        "avif" => "image/avif",
        _ => "application/octet-stream",
    }
}

/// MIME → 扩展名（无法识别时 `bin`，仍可经 `/media/` 取回原字节）。
fn ext_for_mime(mime: &str) -> &'static str {
    let mime = mime
        .split(';')
        .next()
        .unwrap_or(mime)
        .trim()
        .to_ascii_lowercase();
    match mime.as_str() {
        "image/gif" => "gif",
        "image/png" => "png",
        "image/jpeg" | "image/jpg" => "jpg",
        "image/webp" => "webp",
        "image/bmp" => "bmp",
        "image/svg+xml" => "svg",
        "image/avif" => "avif",
        _ => "bin",
    }
}

/// mime_from_data_uri 的辅助：从 `data:<mime>;base64,` 取 mime。
fn mime_of_data_uri(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("data:")?;
    let (mime, _) = rest.split_once(';')?;
    if mime.is_empty() {
        None
    } else {
        Some(mime)
    }
}

/// FNV-1a 128：内容寻址的文件名（同图去重；自带依赖零、跨平台稳定）。
fn content_id(bytes: &[u8]) -> String {
    const OFFSET: u128 = 0x6c62272e07bb014262b821756295c58d;
    const PRIME: u128 = 0x0000000001000000000000000000013B;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u128::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    format!("{hash:032x}")
}

/// 原子上写：临时文件 + rename，避免读到半截文件。
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create media dir failed: {e}"))?;
    }
    let temporary = path.with_extension(format!(
        "{}.part{}",
        path.extension().and_then(|e| e.to_str()).unwrap_or("bin"),
        std::process::id()
    ));
    std::fs::write(&temporary, bytes).map_err(|e| format!("write media file failed: {e}"))?;
    std::fs::rename(&temporary, path).map_err(|e| {
        let _ = std::fs::remove_file(&temporary);
        format!("commit media file failed: {e}")
    })
}

/// 图片字节落盘，返回媒体 id（`<hash>.<ext>`）。内容相同则复用既有文件。
pub fn save_image_bytes(bytes: &[u8], mime: &str) -> Result<String, String> {
    save_image_bytes_in(&media_dir(), bytes, mime)
}

/// 同 [`save_image_bytes`]，落盘到指定媒体目录（宿主服务 / 测试注入用）。
pub fn save_image_bytes_in(
    dir: &std::path::Path,
    bytes: &[u8],
    mime: &str,
) -> Result<String, String> {
    if bytes.is_empty() {
        return Err("empty image payload".into());
    }
    if bytes.len() > MAX_STORED_BYTES {
        return Err(format!(
            "image too large to store ({} bytes > {MAX_STORED_BYTES})",
            bytes.len()
        ));
    }
    let id = format!("{}.{}", content_id(bytes), ext_for_mime(mime));
    let path = dir.join(&id);
    if !path.exists() {
        write_atomic(&path, bytes)?;
    }
    Ok(id)
}

/// 内嵌 data URI 落盘，返回媒体 id。仅支持 `;base64,` 载荷。
pub fn save_data_uri(uri: &str) -> Result<String, String> {
    use base64::Engine;
    let mime = mime_of_data_uri(uri).ok_or_else(|| "not a data URI".to_string())?;
    let Some((_, payload)) = uri.split_once(";base64,") else {
        return Err("data URI without base64 payload".into());
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload.trim())
        .map_err(|e| format!("base64 decode failed: {e}"))?;
    save_image_bytes(&bytes, mime)
}

/// 读取媒体文件并还原为 data URI（发往 LLM 前调用）。
pub fn load_data_uri(id: &str) -> Result<String, String> {
    use base64::Engine;
    if id_of_ref(&media_ref(id)).is_none() {
        return Err(format!("invalid media id: {id}"));
    }
    let path = media_dir().join(id);
    let bytes = std::fs::read(&path).map_err(|e| format!("read media file failed: {e}"))?;
    let mime = mime_for_ext(path.extension().and_then(|e| e.to_str()).unwrap_or(""));
    Ok(format!(
        "data:{mime};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    ))
}

/// 引用 → data URI。
pub fn resolve_ref(reference: &str) -> Result<String, String> {
    let id = id_of_ref(reference).ok_or_else(|| "not a media reference".to_string())?;
    load_data_uri(id)
}

/// 链路上的单张图片规范化：
/// - 过长的 data URI → 落盘，替换为引用；
/// - 落盘失败 → [`ELIDED_IMAGE`]（保留数组元素，计数不丢）；
/// - 引用/短串/非 data 值 → 原样保留。
pub fn spill_or_keep(image: &str) -> String {
    if !image.starts_with("data:") || image.len() < SPILL_MIN_CHARS {
        return image.to_string();
    }
    match save_data_uri(image) {
        Ok(id) => media_ref(&id),
        Err(error) => {
            tracing::warn!(%error, "media spill failed; eliding inline image");
            ELIDED_IMAGE.to_string()
        }
    }
}

/// 把一段文本里所有足够长的内嵌 data URI 落盘，并替换为 `/media/<id>` 引用。
///
/// 返回替换后的文本；没有任何长 data URI 时返回 `None`（调用方据此跳过
/// 写回）。落盘失败的单张图退化为 [`ELIDED_IMAGE`]（省略占位）。
///
/// 用途：事件日志的 `content` 字段（QQ hook JSON 里原样序列化的 images）
/// 与显示时间线——历史数据里内嵌的图片在这次迁移中落盘，文件随即收缩到
/// KB 级（实测：24.6MB 的会话文件迁移后 ~0.4MB，图片仍可从媒体库取回）。
pub fn spill_inline_data_uris(text: &str) -> Option<String> {
    const MARKER: &str = "data:";
    /// base64 头（与 [`elide_inline_data_uris`] 同一常量语义）。
    const BASE64_HEADER: &str = ";base64,";

    fn is_base64_char(ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '+' || ch == '/' || ch == '='
    }

    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut changed = false;
    while let Some(position) = rest.find(MARKER) {
        let (head, tail) = rest.split_at(position);
        out.push_str(head);
        let Some(header_position) = tail.find(BASE64_HEADER) else {
            out.push_str(&tail[..MARKER.len()]);
            rest = &tail[MARKER.len()..];
            continue;
        };
        let body_position = header_position + BASE64_HEADER.len();
        let (prefix, body) = tail.split_at(body_position);
        let run = body.len() - body.trim_start_matches(is_base64_char).len();
        if run < SPILL_MIN_CHARS {
            out.push_str(prefix);
            out.push_str(&body[..run]);
            rest = &body[run..];
            continue;
        }
        // 完整 data URI = prefix（含 `data:...;base64,`）+ 载荷
        let uri = format!("{prefix}{}", &body[..run]);
        match save_data_uri(&uri) {
            Ok(id) => out.push_str(&media_ref(&id)),
            Err(_) => out.push_str(ELIDED_IMAGE),
        }
        changed = true;
        rest = &body[run..];
    }
    out.push_str(rest);
    changed.then_some(out)
}

/// 测试辅助：标准 base64 编码（供上层 crate 的测试构造 data URI，
/// 无需各自引入 base64 依赖）。
pub fn tests_support_base64(bytes: Vec<u8>) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_media_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "echo-media-test-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    struct DirGuard(PathBuf);
    impl Drop for DirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn with_media_dir<R>(dir: &Path, f: impl FnOnce() -> R) -> R {
        // 环境变量是进程级的：测试里用全局锁串行化，避免互相踩。
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var_os("ECHO_MEDIA_DIR");
        std::env::set_var("ECHO_MEDIA_DIR", dir);
        let out = f();
        match previous {
            Some(value) => std::env::set_var("ECHO_MEDIA_DIR", value),
            None => std::env::remove_var("ECHO_MEDIA_DIR"),
        }
        out
    }

    fn png_data_uri(payload: &[u8]) -> String {
        use base64::Engine;
        format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(payload)
        )
    }

    #[test]
    fn ref_roundtrip_and_validation() {
        assert_eq!(media_ref("abc.png"), "/media/abc.png");
        assert_eq!(id_of_ref("/media/abc.png"), Some("abc.png"));
        assert!(is_media_ref("/media/abc.png"));
        // 越界字符与空 id 拒绝
        assert_eq!(id_of_ref("/media/"), None);
        assert_eq!(id_of_ref("/media/../etc/passwd"), None);
        assert_eq!(id_of_ref("/media/a/b.png"), None);
        assert_eq!(id_of_ref("/other/abc.png"), None);
        assert_eq!(id_of_ref("/media/.."), None);
    }

    #[test]
    fn save_and_load_roundtrip_dedups_by_content() {
        let dir = temp_media_dir("roundtrip");
        let _cleanup = DirGuard(dir.clone());
        with_media_dir(&dir, || {
            let id_one = save_image_bytes(b"GIF89a-fake", "image/gif").unwrap();
            let id_two = save_image_bytes(b"GIF89a-fake", "image/gif").unwrap();
            assert_eq!(id_one, id_two, "same bytes → same id");
            assert!(id_one.ends_with(".gif"));
            let uri = load_data_uri(&id_one).unwrap();
            assert!(uri.starts_with("data:image/gif;base64,"));
            assert_eq!(resolve_ref(&media_ref(&id_one)).unwrap(), uri);
            // 内容不同 → id 不同
            let other = save_image_bytes(b"GIF89a-other", "image/gif").unwrap();
            assert_ne!(id_one, other);
        });
    }

    #[test]
    fn data_uri_spill_and_inline_roundtrip() {
        let dir = temp_media_dir("spill");
        let _cleanup = DirGuard(dir.clone());
        with_media_dir(&dir, || {
            // 载荷需超过落盘阈值（base64 后 ~5.4KB）
            let payload: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
            let uri = png_data_uri(&payload);
            assert!(uri.len() >= SPILL_MIN_CHARS);
            let spilled = spill_or_keep(&uri);
            assert!(spilled.starts_with("/media/"), "{spilled}");
            assert!(spilled.ends_with(".png"));
            let restored = resolve_ref(&spilled).unwrap();
            assert_eq!(restored, uri);

            // 短 data URI 保持原样（不落盘）
            let short = "data:image/png;base64,AAAA";
            assert_eq!(spill_or_keep(short), short);
            // 非 data 值原样
            assert_eq!(
                spill_or_keep("https://example.com/a.png"),
                "https://example.com/a.png"
            );
        });
    }

    #[test]
    fn spill_failure_elides_instead_of_panicking() {
        // 用一个「父路径是文件」的目录让落盘必然失败 → 走省略占位分支。
        let blocker = temp_media_dir("elide-blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let _cleanup = DirGuard(blocker.clone());
        with_media_dir(&blocker, || {
            let payload: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
            let uri = png_data_uri(&payload);
            assert_eq!(spill_or_keep(&uri), ELIDED_IMAGE);
        });
    }

    #[test]
    fn oversized_payload_is_rejected() {
        let dir = temp_media_dir("oversize");
        let _cleanup = DirGuard(dir.clone());
        with_media_dir(&dir, || {
            let error = save_image_bytes(&vec![0u8; MAX_STORED_BYTES + 1], "image/png")
                .expect_err("must reject");
            assert!(error.contains("too large"), "{error}");
        });
    }

    #[test]
    fn spill_inline_data_uris_rewrites_long_payloads_only() {
        let dir = temp_media_dir("spill-text");
        let _cleanup = DirGuard(dir.clone());
        with_media_dir(&dir, || {
            let payload: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
            let uri = png_data_uri(&payload);
            // 模拟 QQ hook JSON：content 里原样内嵌 images
            let text = format!("{{\"content\":\"看图\",\"images\":[\"{uri}\"]}}");
            let rewritten = spill_inline_data_uris(&text).expect("changed");
            assert!(rewritten.contains("/media/"), "{rewritten}");
            assert!(!rewritten.contains("data:image"), "payload gone");
            // 结构仍是合法 JSON
            let value: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
            let reference = value["images"][0].as_str().unwrap();
            assert!(reference.starts_with("/media/"));

            // 短 data URI 不改动
            let short = "{\"images\":[\"data:image/png;base64,AAAA\"]}";
            assert_eq!(spill_inline_data_uris(short), None);

            // 幂等：第二次没有长 data URI
            assert_eq!(spill_inline_data_uris(&rewritten), None);
        });
    }

    #[test]
    fn media_dir_defaults_and_override() {
        let default = media_dir_with_home(None, Some("/home/x".into()));
        assert_eq!(
            default,
            PathBuf::from("/home/x/.local/share/echo-agent-core/media")
        );
        let overridden = media_dir_with_home(Some("/custom/media".into()), Some("/home/x".into()));
        assert_eq!(overridden, PathBuf::from("/custom/media"));
        let no_home = media_dir_with_home(None, None);
        assert_eq!(no_home, PathBuf::from("media"));
    }
}
