//! 接收文件：把 QQ 侧的文件下载到本机，供 agent 读取。
//!
//! 通路（2026-09-18 起）：
//! - **群文件上传**（`group_upload` 通知）：适配器经 OneBot
//!   `get_group_file_url` 换取腾讯直链后下载；
//! - **私聊文件**（message 里的 `file` 段）：段带 `url` 时直接下载，
//!   否则经 `get_private_file_url` 换直链再下载。
//!
//! 下载为流式写盘：先按 `Content-Length` 快速拒绝超限文件，写入过程中
//! 再次校验大小（防伪造长度），超限/中断时删除半成品文件。

use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

/// 下载请求的连接超时。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// 单个文件的整体下载超时（大文件在慢网络下的兜底上限）。
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
/// 文件名（含时间戳前缀）的最大字符数——文件系统通常限制 255 字节，
/// 为 UTF-8 多字节留出余量。
const MAX_NAME_CHARS: usize = 120;

/// 解析接收文件的保存目录（配置为空时用默认数据目录）。
pub fn resolve_dir(configured: &str) -> PathBuf {
    resolve_dir_with_home(configured, std::env::var_os("HOME"))
}

fn resolve_dir_with_home(configured: &str, home: Option<std::ffi::OsString>) -> PathBuf {
    let trimmed = configured.trim();
    if !trimmed.is_empty() {
        return PathBuf::from(trimmed);
    }
    match home {
        Some(home) => PathBuf::from(home).join(".local/share/echo-agent-core/downloads"),
        None => PathBuf::from("downloads"),
    }
}

/// 消毒文件名：剥掉目录成分（防穿越）、控制字符与开头点号（防隐藏文件
/// 覆盖），并限制长度；全部剥光后用 `unnamed` 兜底。
pub fn sanitize_file_name(raw: &str) -> String {
    let base = raw.rsplit(['/', '\\']).next().unwrap_or(raw).trim();
    let cleaned: String = base
        .chars()
        .filter(|ch| !ch.is_control() && *ch != '\0')
        .collect();
    let cleaned = cleaned.trim().trim_start_matches('.').trim();
    let truncated: String = cleaned.chars().take(MAX_NAME_CHARS).collect();
    if truncated.is_empty() {
        "unnamed".to_string()
    } else {
        truncated
    }
}

/// 流式下载 `url` 到 `dir`，落地文件名 = `<毫秒时间戳>-<消毒后的名字>`。
///
/// 返回 `(落地路径, 实际字节数)`。超过 `max_bytes` 时返回 Err 并清理
/// 半成品文件。
pub async fn download_file(
    url: &str,
    dir: &Path,
    name: &str,
    max_bytes: u64,
) -> Result<(PathBuf, u64), String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(format!(
            "unsupported url scheme: {}",
            url.chars().take(40).collect::<String>()
        ));
    }
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .map_err(|e| format!("http client init failed: {e}"))?;
    let resp = client
        .get(url)
        .timeout(DOWNLOAD_TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("download request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("download HTTP {}", resp.status()));
    }
    if let Some(len) = resp.content_length() {
        if len > max_bytes {
            return Err(format!(
                "file too large ({} bytes > limit {})",
                len, max_bytes
            ));
        }
    }

    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| format!("create download dir failed: {e}"))?;
    let safe = sanitize_file_name(name);
    let stamp = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let file_name = format!("{}-{}", stamp.as_millis(), safe);
    let mut path = dir.join(&file_name);

    // 时间戳毫秒 + 名字仍可能撞车（同毫秒到达的同名文件）：create_new
    // 保证独占创建，冲突时追加序号重试（上限 100 次后放弃）。
    let mut attempt = 1u32;
    let mut file = loop {
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => break file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && attempt < 100 => {
                attempt += 1;
                path = dir.join(format!("{file_name}-{attempt}"));
            }
            Err(error) => return Err(format!("create file failed: {error}")),
        }
    };
    let mut stream = resp.bytes_stream();
    let mut total: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                drop(file);
                let _ = tokio::fs::remove_file(&path).await;
                return Err(format!("download interrupted: {error}"));
            }
        };
        total += chunk.len() as u64;
        if total > max_bytes {
            drop(file);
            let _ = tokio::fs::remove_file(&path).await;
            return Err(format!("file too large (exceeded limit {max_bytes} bytes)"));
        }
        if let Err(error) = file.write_all(&chunk).await {
            drop(file);
            let _ = tokio::fs::remove_file(&path).await;
            return Err(format!("write failed: {error}"));
        }
    }
    if let Err(error) = file.flush().await {
        drop(file);
        let _ = tokio::fs::remove_file(&path).await;
        return Err(format!("flush failed: {error}"));
    }
    Ok((path, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// 极简 HTTP 服务：返回固定字节体（或指定 Content-Length 假体）。
    fn serve_once(body: Vec<u8>, declared_len: Option<usize>) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let len = declared_len.unwrap_or(body.len());
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        addr
    }

    #[test]
    fn sanitize_strips_paths_controls_and_leading_dots() {
        assert_eq!(sanitize_file_name("report.pdf"), "report.pdf");
        assert_eq!(sanitize_file_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name("a\\b\\c.zip"), "c.zip");
        assert_eq!(sanitize_file_name(".hidden"), "hidden");
        assert_eq!(sanitize_file_name("bad\u{0}name"), "badname");
        assert_eq!(sanitize_file_name("   "), "unnamed");
        assert_eq!(sanitize_file_name("点点.点."), "点点.点.");
    }

    #[test]
    fn resolve_dir_prefers_configured_value() {
        let dir = resolve_dir_with_home("/tmp/qq-files", Some("/home/x".into()));
        assert_eq!(dir, PathBuf::from("/tmp/qq-files"));
        let fallback = resolve_dir_with_home("  ", Some("/home/x".into()));
        assert_eq!(
            fallback,
            PathBuf::from("/home/x/.local/share/echo-agent-core/downloads")
        );
        let no_home = resolve_dir_with_home("", None);
        assert_eq!(no_home, PathBuf::from("downloads"));
    }

    #[tokio::test]
    async fn downloads_streamed_body_to_timestamped_path() {
        let body = b"hello qq file".to_vec();
        let addr = serve_once(body.clone(), None);
        let dir = std::env::temp_dir().join(format!(
            "echo-file-test-{}",
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let url = format!("http://{addr}/f");
        let (path, size) = download_file(&url, &dir, "a b/../测试.txt", 1024)
            .await
            .expect("download ok");
        assert_eq!(size, body.len() as u64);
        assert!(path.starts_with(&dir), "path inside dir: {path:?}");
        assert!(
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-测试.txt")),
            "sanitized name kept: {path:?}"
        );
        let saved = std::fs::read(&path).expect("read saved");
        assert_eq!(saved, body);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_file_over_limit_by_content_length() {
        let body = vec![b'x'; 64];
        let addr = serve_once(body, None);
        let dir = std::env::temp_dir().join("echo-file-test-too-large");
        let url = format!("http://{addr}/big");
        let err = download_file(&url, &dir, "big.bin", 16)
            .await
            .expect_err("must reject");
        assert!(err.contains("too large"), "err: {err}");
        // 目录里不应残留半成品（Content-Length 判断在写入之前）。
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .map(|it| it.flatten().collect())
            .unwrap_or_default();
        assert!(leftovers.is_empty(), "no partial files: {leftovers:?}");
    }

    #[tokio::test]
    async fn truncated_body_cleans_up_partial_file() {
        // Content-Length 声明 64，实际只发 10 字节后断开 → 流中断 → 清理。
        let addr = serve_once(vec![b'y'; 10], Some(64));
        let dir = std::env::temp_dir().join(format!(
            "echo-file-test-truncated-{}",
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let url = format!("http://{addr}/cut");
        let err = download_file(&url, &dir, "cut.bin", 1024)
            .await
            .expect_err("must fail");
        assert!(
            err.contains("interrupted") || err.contains("failed"),
            "err: {err}"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .map(|it| it.flatten().collect())
            .unwrap_or_default();
        assert!(leftovers.is_empty(), "no partial files: {leftovers:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn same_name_downloads_do_not_overwrite_each_other() {
        // 同名文件连续下载：即使落毫秒相同，create_new + 序号兜底也保证
        // 两份文件都保留（不互相覆盖）。
        let body = b"first".to_vec();
        let addr = serve_once(body.clone(), None);
        let dir = std::env::temp_dir().join(format!(
            "echo-file-test-collision-{}",
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let url = format!("http://{addr}/f");
        let (first, _) = download_file(&url, &dir, "same.txt", 1024)
            .await
            .expect("first ok");
        // 第二次下载同名文件（同一毫秒也安全）。
        let addr2 = serve_once(b"second".to_vec(), None);
        let url2 = format!("http://{addr2}/f");
        let (second, _) = download_file(&url2, &dir, "same.txt", 1024)
            .await
            .expect("second ok");
        assert_ne!(first, second, "两个路径必须不同: {first:?} vs {second:?}");
        let files: Vec<_> = std::fs::read_dir(&dir)
            .map(|it| it.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        assert_eq!(files.len(), 2, "两份文件都保留: {files:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_non_http_scheme() {
        let dir = std::env::temp_dir();
        let err = download_file("file:///etc/passwd", &dir, "x", 1024)
            .await
            .expect_err("must reject");
        assert!(err.contains("unsupported url scheme"), "err: {err}");
    }
}
