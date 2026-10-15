//! NapCat Docker service management.
//!
//! The QQ adapter can own the NapCat container lifecycle when
//! `napcat_auto_start` / `napcat_auto_stop` are enabled. This module wraps
//! the `docker` / `docker compose` CLI without any other process spawns.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// Maximum time to wait for the NapCat container to reach the desired state.
const DOCKER_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const DOCKER_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Snapshot of the NapCat Docker service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NapCatServiceState {
    /// Docker CLI is missing, or the daemon is unreachable / permission denied.
    DockerUnavailable(String),
    /// Docker is available, but the configured container does not exist.
    ContainerMissing,
    /// Container exists and is running.
    ContainerRunning { image: String },
    /// Container exists but is stopped.
    ContainerStopped { image: String },
    /// Container exists but is paused.
    ContainerPaused { image: String },
    /// Container exists in another state.
    Unknown(String),
}

impl NapCatServiceState {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::ContainerRunning { .. })
    }

    pub fn describe(&self) -> String {
        match self {
            Self::DockerUnavailable(message) => format!("Docker 不可用: {message}"),
            Self::ContainerMissing => "容器不存在".to_string(),
            Self::ContainerRunning { image } => format!("容器运行中 (image={image})"),
            Self::ContainerStopped { image } => format!("容器已停止 (image={image})"),
            Self::ContainerPaused { image } => format!("容器已暂停 (image={image})"),
            Self::Unknown(raw) => format!("容器状态未知 ({raw})"),
        }
    }
}

/// Thin wrapper around `docker` / `docker compose` for the NapCat container.
pub struct NapCatService {
    compose_file: String,
    container_name: String,
}

#[derive(Debug, Clone)]
struct ContainerInspect {
    running: bool,
    state: String,
    image: String,
}

impl NapCatService {
    pub fn new(compose_file: impl Into<String>, container_name: impl Into<String>) -> Self {
        Self {
            compose_file: compose_file.into(),
            container_name: container_name.into(),
        }
    }

    /// Start the NapCat container and wait until it is running.
    pub async fn start(&self) -> Result<NapCatServiceState, String> {
        let compose_file = self.compose_file.clone();
        let container_name = self.container_name.clone();
        tokio::task::spawn_blocking(move || Self::start_sync(&compose_file, &container_name))
            .await
            .map_err(|e| format!("NapCat 服务线程异常: {e}"))?
    }

    /// Stop the NapCat container (keeps the container and its data volumes).
    pub async fn stop(&self) -> Result<(), String> {
        let compose_file = self.compose_file.clone();
        let container_name = self.container_name.clone();
        tokio::task::spawn_blocking(move || Self::stop_sync(&compose_file, &container_name))
            .await
            .map_err(|e| format!("NapCat 服务线程异常: {e}"))?
    }

    /// Query the current NapCat container status.
    pub async fn status(&self) -> NapCatServiceState {
        let container_name = self.container_name.clone();
        tokio::task::spawn_blocking(move || Self::status_sync(&container_name))
            .await
            .unwrap_or_else(|e| NapCatServiceState::DockerUnavailable(format!("服务线程异常: {e}")))
    }

    fn start_sync(compose_file: &str, container_name: &str) -> Result<NapCatServiceState, String> {
        ensure_docker()?;

        let existing = inspect_container(container_name)?;
        if let Some(info) = &existing {
            if info.running {
                return Ok(NapCatServiceState::ContainerRunning {
                    image: info.image.clone(),
                });
            }
        }

        let compose_ready = Path::new(compose_file).exists() && docker_compose_available()?;
        if compose_ready {
            run_docker(&["compose", "-f", compose_file, "up", "-d"])?;
        } else if let Some(info) = &existing {
            // A paused container must be unpaused before it can run; a stopped
            // container can be started directly.
            if info.state == "paused" {
                run_docker(&["unpause", container_name])?;
            } else {
                run_docker(&["start", container_name])?;
            }
        } else {
            return Err(format!(
                "NapCat 容器 '{container_name}' 不存在，且找不到 Compose 文件 '{compose_file}'；请先准备 docker-compose.yml 或手动创建容器"
            ));
        }

        wait_for(container_name, true)
    }

    /// 彻底移除实例资源（实例删除路径，2026-10 巡检遗留）：容器 + 数据卷
    /// + compose 文件。幂等：资源不存在时静默成功。
    ///
    /// 步骤：`compose down --volumes`（存在 compose 文件时，连容器带卷）
    /// → `docker rm -f`（无 compose 或 down 失败的兜底）→ `docker volume rm`
    /// （compose 没清掉的命名卷兜底）→ 删 compose 文件与实例目录。
    pub async fn remove(&self) -> Result<(), String> {
        let compose_file = self.compose_file.clone();
        let container_name = self.container_name.clone();
        tokio::task::spawn_blocking(move || Self::remove_sync(&compose_file, &container_name))
            .await
            .map_err(|e| format!("remove task failed: {e}"))?
    }

    fn remove_sync(compose_file: &str, container_name: &str) -> Result<(), String> {
        ensure_docker()?;
        let mut notes: Vec<String> = Vec::new();
        // 1) compose down --volumes（最干净的路径）
        if Path::new(compose_file).exists() && docker_compose_available()? {
            if run_docker(&["compose", "-f", compose_file, "down", "--volumes"]).is_ok() {
                tracing::info!(container = %container_name, "napcat instance removed via compose down");
            } else {
                notes.push("compose down failed".into());
            }
        }
        // 2) 容器兜底（compose 缺失或 down 失败）
        if inspect_container(container_name)?.is_some() {
            if let Err(e) = run_docker(&["rm", "-f", container_name]) {
                notes.push(format!("docker rm failed: {e}"));
            }
        }
        // 3) 命名卷兜底（compose down --volumes 未覆盖的场景）
        for volume in [
            format!("{container_name}-data"),
            format!("{container_name}-config"),
        ] {
            // 卷名按 compose 项目名前缀可能与容器名不同——尝试两种命名。
            for candidate in [volume.clone(), format!("napcat_{volume}")] {
                let _ = run_docker(&["volume", "rm", "-f", &candidate]);
            }
        }
        // 4) compose 文件与实例目录
        let compose_path = Path::new(compose_file);
        if compose_path.exists() {
            if let Err(e) = std::fs::remove_file(compose_path) {
                notes.push(format!("remove compose file failed: {e}"));
            }
            if let Some(dir) = compose_path.parent() {
                let _ = std::fs::remove_dir(dir); // 空目录才成功，非空静默跳过
            }
        }
        if notes.is_empty() {
            Ok(())
        } else {
            Err(notes.join("; "))
        }
    }

    fn stop_sync(compose_file: &str, container_name: &str) -> Result<(), String> {
        ensure_docker()?;

        match inspect_container(container_name)? {
            None => return Ok(()),
            Some(info) if !info.running && info.state != "paused" => return Ok(()),
            Some(info) if info.state == "paused" => {
                run_docker(&["unpause", container_name])?;
            }
            _ => {}
        }

        if Path::new(compose_file).exists() && docker_compose_available()? {
            // `docker compose stop` preserves the container and data volumes;
            // if it fails (e.g. stale compose project) fall back to `docker stop`.
            if run_docker(&["compose", "-f", compose_file, "stop"]).is_ok() {
                wait_for(container_name, false)?;
                return Ok(());
            }
        }

        run_docker(&["stop", container_name])?;
        wait_for(container_name, false)?;
        Ok(())
    }

    fn status_sync(container_name: &str) -> NapCatServiceState {
        if let Err(message) = ensure_docker() {
            return NapCatServiceState::DockerUnavailable(message);
        }
        match inspect_container(container_name) {
            Err(message) => NapCatServiceState::DockerUnavailable(message),
            Ok(None) => NapCatServiceState::ContainerMissing,
            Ok(Some(info)) => {
                if info.running {
                    NapCatServiceState::ContainerRunning { image: info.image }
                } else if info.state == "paused" {
                    NapCatServiceState::ContainerPaused { image: info.image }
                } else if info.state == "exited" || info.state == "created" || info.state == "dead"
                {
                    NapCatServiceState::ContainerStopped { image: info.image }
                } else {
                    NapCatServiceState::Unknown(info.state)
                }
            }
        }
    }
}

fn ensure_docker() -> Result<(), String> {
    let output = Command::new("docker")
        .args(["info"])
        .output()
        .map_err(|e| format!("无法执行 docker 命令: {e}；请安装 Docker 或把 NapCat 交给外部管理（napcat_auto_start=false）"))?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr.trim();
    if detail.contains("permission denied")
        || detail.contains("Got permission denied")
        || detail.to_lowercase().contains("daemon")
    {
        Err(format!(
            "Docker 权限不足: {detail}；请将当前用户加入 docker 组（sudo usermod -aG docker $USER）后重新登录，或配置 napcat_auto_start=false"
        ))
    } else {
        Err(format!("Docker daemon 不可用: {detail}"))
    }
}

fn docker_compose_available() -> Result<bool, String> {
    let output = Command::new("docker")
        .args(["compose", "version"])
        .output()
        .map_err(|e| format!("无法执行 docker compose: {e}"))?;
    Ok(output.status.success())
}

fn inspect_container(container_name: &str) -> Result<Option<ContainerInspect>, String> {
    let output = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.State.Running}}|{{.State.Status}}|{{.Config.Image}}",
            container_name,
        ])
        .output()
        .map_err(|e| format!("无法执行 docker inspect: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr_lower = stderr.to_lowercase();
        if stderr_lower.contains("no such object") || stderr_lower.contains("not found") {
            return Ok(None);
        }
        return Err(format!("docker inspect 失败: {}", stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(parse_inspect_output(&stdout))
}

fn parse_inspect_output(stdout: &str) -> Option<ContainerInspect> {
    let mut parts = stdout.trim().split('|');
    let running = parts.next()? == "true";
    let state = parts.next()?.to_string();
    let image = parts.next()?.to_string();
    Some(ContainerInspect {
        running,
        state,
        image,
    })
}

fn run_docker(args: &[&str]) -> Result<(), String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("无法执行 docker {}: {e}", args.join(" ")))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(format!("docker {} 失败: {}", args.join(" "), stderr.trim()))
}

fn wait_for(container_name: &str, want_running: bool) -> Result<NapCatServiceState, String> {
    let deadline = std::time::Instant::now() + DOCKER_WAIT_TIMEOUT;
    loop {
        let state = NapCatService::status_sync(container_name);
        match &state {
            NapCatServiceState::ContainerRunning { .. } if want_running => return Ok(state),
            NapCatServiceState::ContainerStopped { .. } | NapCatServiceState::ContainerMissing
                if !want_running =>
            {
                return Ok(state)
            }
            NapCatServiceState::DockerUnavailable(message) => {
                return Err(format!("等待容器状态时 Docker 不可用: {message}"))
            }
            _ => {}
        }

        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "等待 NapCat 容器 '{}' 变为 {} 超时（当前状态: {}）",
                container_name,
                if want_running { "running" } else { "stopped" },
                state.describe()
            ));
        }
        std::thread::sleep(DOCKER_POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_describes_itself() {
        assert_eq!(
            NapCatServiceState::ContainerRunning {
                image: "img".into()
            }
            .describe(),
            "容器运行中 (image=img)"
        );
        assert_eq!(
            NapCatServiceState::ContainerMissing.describe(),
            "容器不存在"
        );
    }

    #[test]
    fn parses_docker_inspect_output() {
        let parsed = parse_inspect_output("true|running|mlikiowa/napcat-docker:latest\n")
            .expect("running container");
        assert!(parsed.running);
        assert_eq!(parsed.state, "running");
        assert_eq!(parsed.image, "mlikiowa/napcat-docker:latest");

        let parsed = parse_inspect_output("false|exited|mlikiowa/napcat-docker:latest")
            .expect("stopped container");
        assert!(!parsed.running);
        assert_eq!(parsed.state, "exited");

        assert!(parse_inspect_output("").is_none());
        assert!(parse_inspect_output("true|running").is_none());
    }
}
