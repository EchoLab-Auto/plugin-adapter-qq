//! File bridge — makes a local file readable by NapCat (which usually runs in
//! a separate Docker container).
//!
//! The OneBot `upload_group_file` / `upload_private_file` actions require a
//! path that is visible to the OneBot implementation (i.e. inside the NapCat
//! container). The agent itself runs on the host, so a plain local path like
//! `/home/user/x/hello.py` is meaningless inside the container.
//!
//! This bridge resolves a local path into `file` parameter(s) NapCat can use,
//! trying strategies in order:
//!
//! 1. **Container path pass-through** — if the path already starts with the
//!    NapCat data directory (e.g. `/app/napcat/data/...`) it is used as-is.
//! 2. **Docker copy** — if the Docker CLI is available, copy the file into the
//!    NapCat container (`docker cp`) and return the in-container path.
//! 3. **Local HTTP fallback** — start a tiny local HTTP file server bound on
//!    `0.0.0.0` with a random per-file token, and return one or more
//!    `http://<host>:<port>/f/<token>` URLs. NapCat downloads the file from
//!    the host itself. Multiple candidate hosts are produced so the upload
//!    caller can try each until one is reachable from inside the container:
//!    the auto-detected docker bridge gateway (e.g. `172.17.0.1`), the
//!    configured `napcat_host` (default `host.docker.internal`), and finally
//!    loopback. This covers deployments where `host.docker.internal` is not
//!    mapped in the container (a common docker-compose omission).
//!
//! Strategy 3 is the workhorse: it needs no Docker socket and no shared
//! volume, and works for any deployment where NapCat can reach the host.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long a registered file stays downloadable before being garbage
/// collected. Uploads complete in seconds; this is a safety net.
const FILE_TTL: Duration = Duration::from_secs(600);
/// Cleanup runs on every accepted connection, so no separate task is needed.
const MAX_REGISTERED_FILES: usize = 512;

/// Resolves local file paths into `file` parameters usable by NapCat.
pub struct FileBridge {
    /// OneBot HTTP API base (used only for a docker-capability probe; the
    /// actual upload goes through the WebSocket action as before).
    _onebot_url: String,
    /// Host name NapCat should use to reach this machine (e.g.
    /// `host.docker.internal` when NapCat runs in Docker).
    napcat_host: String,
    /// Docker container name for `docker cp` (e.g. `napcat`).
    container_name: String,
    /// In-container writable directory (e.g. `/app/napcat/data`).
    container_data_dir: String,
    /// Docker CLI 可用性探测缓存（`Arc` 以便克隆进 `spawn_blocking` 闭包；
    /// 进程级 static 被 `no_global_state` 测试禁止——缓存必须挂在实例上）。
    docker_ok: Arc<OnceLock<bool>>,
    /// Lazily-started local HTTP file server (strategy 3).
    http_server: OnceLock<Arc<HttpFileServer>>,
}

/// Tiny HTTP/1.1 file server backed by a token → path map.
struct HttpFileServer {
    /// Bound address; the port is what NapCat connects to.
    addr: SocketAddr,
    /// token → (absolute local path, registration time).
    files: DashMap<String, (PathBuf, Instant)>,
}

impl FileBridge {
    pub fn new(
        onebot_url: &str,
        napcat_host: &str,
        container_name: &str,
        container_data_dir: &str,
    ) -> Self {
        Self {
            _onebot_url: onebot_url.to_string(),
            napcat_host: napcat_host.to_string(),
            container_name: container_name.to_string(),
            container_data_dir: container_data_dir.to_string(),
            docker_ok: Arc::new(OnceLock::new()),
            http_server: OnceLock::new(),
        }
    }

    /// Resolve a local path into candidate `file` parameters NapCat can use,
    /// ordered by preference. The upload caller tries each candidate until
    /// NapCat accepts one.
    ///
    /// `file_name` is the display name used when copying into the container.
    pub async fn resolve_all(
        &self,
        local_path: &str,
        file_name: &str,
    ) -> Result<Vec<String>, String> {
        let path = Path::new(local_path);

        // Strategy 1: already a NapCat-visible path → pass through.
        let is_container_path = local_path.starts_with(&self.container_data_dir)
            || local_path.starts_with("/app/napcat/")
            || local_path.starts_with("/napcat/");
        if is_container_path {
            return Ok(vec![local_path.to_string()]);
        }

        let mut candidates = Vec::new();

        // Strategy 2: docker cp into the NapCat container.
        if let Some(remote) = self.try_docker_cp(path, file_name).await {
            tracing::info!(%local_path, %remote, "file copied into NapCat container");
            candidates.push(remote);
        }

        // Strategy 3: local HTTP file server, NapCat pulls the URL itself.
        // Produce one candidate per plausible host so the caller can try them
        // in order until one is reachable from inside the container.
        candidates.extend(self.http_urls(path).await?);

        if candidates.is_empty() {
            return Err(format!("no strategy could resolve '{}'", local_path));
        }
        Ok(candidates)
    }

    /// In-container path for a file name (`<container_data_dir>/<name>`).
    pub fn container_data_path(&self, name: &str) -> String {
        format!("{}/{}", self.container_data_dir.trim_end_matches('/'), name)
    }

    /// Whether a **bare file name** already exists in the NapCat container's
    /// data directory (i.e. a file that was previously sent/received and thus
    /// already sits where NapCat can read it — no re-copy needed).
    ///
    /// Only bare names are probed (a name containing path separators is not a
    /// data-dir lookup); requires the Docker CLI and the container to exist.
    pub async fn container_file_exists(&self, name: &str) -> bool {
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return false;
        }
        let container_name = self.container_name.clone();
        let path = self.container_data_path(name);
        let docker_ok = self.docker_ok.clone();
        // docker 探测 + exec 均为子进程往返（可达数百 ms），放 blocking 池，
        // 避免卡住 async 调度线程（2026-10 巡检：async 路径直接跑 Command）。
        tokio::task::spawn_blocking(move || {
            if !*docker_ok.get_or_init(probe_docker_cli) {
                return false;
            }
            Command::new("docker")
                .args(["exec", &container_name, "test", "-f", &path])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }

    // ---- strategy 2: docker cp ----

    async fn try_docker_cp(&self, local: &Path, file_name: &str) -> Option<String> {
        let name = sanitize_file_name(file_name).unwrap_or_else(|| {
            local
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "file".into())
        });
        let dest = format!("{}/{}", self.container_data_dir.trim_end_matches('/'), name);
        let target = format!("{}:{dest}", self.container_name);
        let local = local.to_str().unwrap_or_default().to_string();
        let docker_ok = self.docker_ok.clone();
        // `docker cp` 大文件可达秒级——必须走 blocking 池（见 container_file_exists）。
        tokio::task::spawn_blocking(move || {
            if !*docker_ok.get_or_init(probe_docker_cli) {
                return None;
            }
            let output = Command::new("docker")
                .args(["cp", &local, &target])
                .output()
                .ok()?;
            if output.status.success() {
                Some(dest)
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::debug!(%stderr, "docker cp failed — falling back to HTTP");
                None
            }
        })
        .await
        .ok()
        .flatten()
    }

    // （docker 可用性探测已提为自由函数 `docker_cli_available`：主机级一次
    //  缓存 + 只在 blocking 上下文调用。）

    // ---- strategy 3: local HTTP server ----

    /// Register the file and produce one download URL per candidate host.
    /// Hosts are ordered: docker bridge gateway (auto-detected), configured
    /// `napcat_host`, then loopback. Duplicates are removed.
    async fn http_urls(&self, local: &Path) -> Result<Vec<String>, String> {
        let local = local
            .canonicalize()
            .map_err(|e| format!("cannot resolve file '{}': {e}", local.display()))?;
        if !local.is_file() {
            return Err(format!("'{}' is not a regular file", local.display()));
        }
        let server = self.ensure_http_server().await?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        // 上限保护（2026-10 巡检 P2 附带）：注册超上限先强制 prune 再插
        // ——TTL 常扫后 512 不再是扫描触发条件，但注册表本身仍需要
        // 一个硬上限防内存膨胀（极端场景：高频 send_file 打满）。
        if server.files.len() >= MAX_REGISTERED_FILES {
            let now = Instant::now();
            server
                .files
                .retain(|_, (_, registered)| now.duration_since(*registered) < FILE_TTL);
        }
        server.files.insert(token.clone(), (local, Instant::now()));

        let mut hosts: Vec<String> = Vec::new();
        // 1. Docker bridge gateway — reachable from containers even when
        //    `host.docker.internal` is not mapped (common docker-compose miss).
        //    `ip` 为子进程调用：放 blocking 池（本函数每次调用都会探测）。
        if let Some(gw) = tokio::task::spawn_blocking(detect_docker_gateway)
            .await
            .ok()
            .flatten()
        {
            hosts.push(gw);
        }
        // 2. Explicitly configured host (default host.docker.internal).
        let configured = self.napcat_host.trim();
        if !configured.is_empty() {
            hosts.push(configured.to_string());
        }
        // 3. Loopback — correct when NapCat runs on the same host.
        hosts.push("127.0.0.1".into());

        let mut seen = std::collections::HashSet::new();
        let mut urls = Vec::new();
        for host in hosts {
            if seen.insert(host.clone()) {
                urls.push(format!("http://{host}:{}/f/{token}", server.addr.port()));
            }
        }
        Ok(urls)
    }

    async fn ensure_http_server(&self) -> Result<Arc<HttpFileServer>, String> {
        if let Some(server) = self.http_server.get() {
            return Ok(server.clone());
        }
        // Bind on all interfaces: NapCat reaches us via the docker bridge
        // gateway (host.docker.internal), which does not hit loopback.
        let listener = TcpListener::bind("0.0.0.0:0")
            .await
            .map_err(|e| format!("cannot bind file server: {e}"))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("cannot read file server address: {e}"))?;
        let server = Arc::new(HttpFileServer {
            addr,
            files: DashMap::new(),
        });
        let serve_files = server.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        let files = serve_files.files.clone();
                        tokio::spawn(async move {
                            let _ = handle_client(socket, files).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "file server accept failed");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        });
        let _ = self.http_server.set(server.clone());
        tracing::info!(port = addr.port(), "local file server listening for NapCat");
        Ok(server)
    }
}

/// 探测 Docker CLI 可用性（`docker info` 子进程往返）。**只应在 blocking
/// 上下文调用**（spawn_blocking 闭包内）；结果由调用方的实例级缓存持有。
fn probe_docker_cli() -> bool {
    Command::new("docker")
        .args(["info"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Best-effort detection of the docker0 bridge gateway IP (e.g. `172.17.0.1`)
/// that containers can use to reach this host. Returns `None` when docker0 is
/// absent (no Docker, non-Linux, or a non-default bridge name).
fn detect_docker_gateway() -> Option<String> {
    // `ip -4 -o addr show docker0` prints e.g.:
    //   2: docker0    inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0
    let output = Command::new("ip")
        .args(["-4", "-o", "addr", "show", "docker0"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().next()?;
    let ip = line
        .split_whitespace()
        .nth(3)? // "inet"
        .split('/')
        .next()?;
    if ip.is_empty() || ip == "0.0.0.0" {
        None
    } else {
        Some(ip.to_string())
    }
}

// ---- HTTP handling ----

async fn handle_client(
    mut socket: TcpStream,
    files: DashMap<String, (PathBuf, Instant)>,
) -> std::io::Result<()> {
    cleanup_expired(&files);

    let mut buf = [0u8; 4096];
    let n = socket.read(&mut buf).await?;
    let head = String::from_utf8_lossy(&buf[..n]);
    let request_line = head.lines().next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");

    if method != "GET" {
        return write_response(&mut socket, 405, b"method not allowed").await;
    }
    let token = match path.strip_prefix("/f/") {
        Some(t) => t.trim(),
        None => return write_response(&mut socket, 404, b"not found").await,
    };

    let file_path = match files.get(token) {
        Some(entry) => {
            let (path, registered) = entry.value();
            if Instant::now().duration_since(*registered) >= FILE_TTL {
                // 已过期：即时移除并 404（不依赖 cleanup 的扫描时机）。
                drop(entry);
                files.remove(token);
                return write_response(&mut socket, 404, b"not found").await;
            }
            path.clone()
        }
        None => return write_response(&mut socket, 404, b"not found").await,
    };
    // Registrations are garbage-collected by TTL (see cleanup_expired), so a
    // download can be retried if NapCat needs to re-fetch the file.
    // serve 上限（2026-10 巡检 P2）：此前整文件读内存无限制。
    const MAX_SERVED_FILE_BYTES: u64 = 64 * 1024 * 1024;
    match tokio::fs::metadata(&file_path).await {
        Ok(meta) if meta.len() > MAX_SERVED_FILE_BYTES => {
            tracing::warn!(path = %file_path.display(), len = meta.len(), "file server: too large");
            write_response(&mut socket, 404, b"not found").await
        }
        _ => match tokio::fs::read(&file_path).await {
            Ok(data) => write_response(&mut socket, 200, &data).await,
            Err(e) => {
                tracing::warn!(error = %e, path = %file_path.display(), "file server read failed");
                write_response(&mut socket, 404, b"not found").await
            }
        },
    }
}

async fn write_response(socket: &mut TcpStream, status: u16, body: &[u8]) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await?;
    socket.write_all(body).await?;
    socket.flush().await
}

fn cleanup_expired(files: &DashMap<String, (PathBuf, Instant)>) {
    // TTL 必须常扫（2026-10 巡检 P2）：此前只在接近 512 上限时才扫描，
    // 少量文件**永远不过期**、token 可无限期下载。每次请求都扫的成本
    // 可忽略（注册数通常 < 几十），上限不再是扫描的触发条件。
    let now = Instant::now();
    files.retain(|_, (_, registered)| now.duration_since(*registered) < FILE_TTL);
}

/// Reduce an arbitrary display name to a safe file name for the container.
fn sanitize_file_name(file_name: &str) -> Option<String> {
    let base = Path::new(file_name)
        .file_name()?
        .to_string_lossy()
        .into_owned();
    if base.is_empty() || base == "." || base == ".." {
        None
    } else {
        Some(base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn container_paths_pass_through_unchanged() {
        let bridge = FileBridge::new(
            "http://localhost:3000",
            "host.docker.internal",
            "napcat",
            "/app/napcat/data",
        );
        let remote = bridge
            .resolve_all("/app/napcat/data/x.pdf", "x.pdf")
            .await
            .unwrap();
        assert_eq!(
            remote.first().map(String::as_str),
            Some("/app/napcat/data/x.pdf")
        );
    }

    #[test]
    fn sanitizes_display_names() {
        assert_eq!(sanitize_file_name("a.pdf").as_deref(), Some("a.pdf"));
        assert_eq!(sanitize_file_name("../evil.sh").as_deref(), Some("evil.sh"));
        assert_eq!(sanitize_file_name(".."), None);
        assert_eq!(sanitize_file_name("/"), None);
    }
    #[tokio::test]
    async fn http_fallback_serves_the_file_and_cleans_up() {
        // Docker is unavailable in CI, so strategy 3 kicks in.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("echo-file-bridge-test-{}.txt", std::process::id()));
        std::fs::write(&path, b"hello from bridge").unwrap();

        let bridge = FileBridge::new(
            "http://localhost:3000",
            "127.0.0.1",
            "napcat",
            "/app/napcat/data",
        );
        let urls = bridge
            .resolve_all(path.to_str().unwrap(), "note.txt")
            .await
            .unwrap();
        assert!(!urls.is_empty(), "expected at least one candidate URL");

        // Loopback must always be among the candidates (NapCat on same host).
        assert!(
            urls.iter().any(|u| u.starts_with("http://127.0.0.1:")),
            "loopback candidate missing: {urls:?}"
        );

        // Every HTTP candidate must serve the file. When Docker is available,
        // resolve_all may also return a valid in-container path first.
        for url in urls.iter().filter(|url| url.starts_with("http://")) {
            let port = url
                .split(':')
                .nth(2)
                .and_then(|p| p.split('/').next())
                .and_then(|p| p.parse::<u16>().ok())
                .expect("port in url");
            assert!(port > 0, "port must be a real ephemeral port, got {port}");

            let resp = reqwest::get(url).await.expect("fetch works");
            assert_eq!(resp.status(), 200);
            let body = resp.text().await.unwrap();
            assert_eq!(body, "hello from bridge");
        }

        // Unknown tokens are rejected: the URL is protected by its random token.
        let loopback_url = urls
            .iter()
            .find(|url| url.starts_with("http://127.0.0.1:"))
            .expect("loopback candidate");
        let port = loopback_url
            .split(':')
            .nth(2)
            .and_then(|p| p.split('/').next())
            .and_then(|p| p.parse::<u16>().ok())
            .expect("port in url");
        let unknown = format!("http://127.0.0.1:{port}/f/does-not-exist");
        let miss = reqwest::get(unknown).await.unwrap();
        assert_eq!(miss.status(), 404);
    }

    #[test]
    fn file_bridge_rejects_missing_files() {
        let bridge = FileBridge::new(
            "http://localhost:3000",
            "127.0.0.1",
            "napcat",
            "/app/napcat/data",
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result =
            rt.block_on(bridge.resolve_all("/nonexistent/definitely-missing.bin", "m.bin"));
        assert!(result.is_err());
    }
}
