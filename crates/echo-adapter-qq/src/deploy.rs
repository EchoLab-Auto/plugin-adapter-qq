//! 部署形状：端口分配 + 容器名 / URL 派生 + compose 渲染（**原在 Core**）。
//!
//! 2026-10 去适配器化收尾：Core 不再认识 NapCat。以前宿主
//! `instance_deploy` 会替每个实例分端口、算容器名、生成 `docker-compose.yml`
//! 再塞进 `Hello.config`——那等于让框架持有某个平台的部署 schema。现在宿主
//! 只下发**原始配置**（共享层 + 实例覆盖）��本模块负责：
//!
//! 1. 深合并两张 TOML 表；
//! 2. 端口三元组（反向 WS / OneBot HTTP / WebUI）——缺省时探测空闲端口并
//!   **持久化到实例自己的 state 文件**（跨重启稳定，否则容器映射每次都变，
//!   重连失效）；
//! 3. 派生 `napcat_container` / `napcat_webui_url` / `napcat_onebot_url` /
//!   `napcat_compose_file` / `server.bind_address`；
//! 4. 渲染并写出 `docker-compose.yml`（幂等：内容不变不写）。
//!
//! **默认实例（id == 平台名 `qq`）走 legacy 语义**：缺省沿用
//! 3131 / 3000 / 6099 且不探测——那三个端口由既有容器持有，宿主侧 bind
//! 探测必然失败。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config_store::ConfigStore;

/// legacy 默认实例名（会话 id 不产生 `@` 后缀）。
pub const DEFAULT_INSTANCE: &str = "qq";

/// 实例的宿主端口三元组（自动分配并持久化）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Ports {
    /// 反向 WS 监听端口（NapCat 连回本插件）。
    pub reverse_ws: u16,
    /// OneBot HTTP API 宿主端口。
    pub onebot_http: u16,
    /// NapCat WebUI 宿主端口。
    pub webui: u16,
}

/// 端口探测区间（非默认实例）。
const ONEBOT_RANGE: (u16, u16) = (3010, 3090);
const WEBUI_RANGE: (u16, u16) = (6100, 6190);
const REVERSE_RANGE: (u16, u16) = (3140, 3190);

/// 深合并两张 TOML 表（`overlay` 覆盖 `base`；表递归合并）。
pub fn merge_tables(base: &toml::Table, overlay: &toml::Table) -> toml::Table {
    let mut out = base.clone();
    for (key, value) in overlay {
        match (out.get_mut(key), value) {
            (Some(toml::Value::Table(inner)), toml::Value::Table(over)) => {
                let merged = merge_tables(inner, over);
                *inner = merged;
            }
            _ => {
                out.insert(key.clone(), value.clone());
            }
        }
    }
    out
}

/// 从合并后的配置里读端口：`[server].bind_address` / `napcat_webui_url` /
/// `napcat_onebot_url`。缺失或非法 → 返回 `None`（交给分配器）。
fn ports_from_config(config: &toml::Table) -> Ports {
    let parse_port = |raw: &str| -> Option<u16> {
        raw.rsplit(':')
            .next()
            .and_then(|port| port.trim_end_matches('/').parse::<u16>().ok())
    };
    let get = |path: &[&str]| -> Option<String> {
        let mut cur = config;
        for key in &path[..path.len() - 1] {
            cur = cur.get(*key)?.as_table()?;
        }
        Some(cur.get(path[path.len() - 1])?.as_str()?.to_string())
    };
    Ports {
        reverse_ws: get(&["server", "bind_address"])
            .and_then(|v| parse_port(&v))
            .unwrap_or(0),
        webui: get(&["napcat_webui_url"])
            .and_then(|v| parse_port(&v))
            .unwrap_or(0),
        onebot_http: get(&["napcat_onebot_url"])
            .and_then(|v| parse_port(&v))
            .unwrap_or(0),
    }
}

/// 端口是否可用（bind 探测；0 视为「未指定」）。
fn port_free(port: u16) -> bool {
    if port == 0 {
        return false;
    }
    std::net::TcpListener::bind(("0.0.0.0", port)).is_ok()
}

fn find_free(range: (u16, u16), taken: &mut std::collections::HashSet<u16>) -> Option<u16> {
    for port in range.0..range.1 {
        if taken.contains(&port) || !port_free(port) {
            continue;
        }
        taken.insert(port);
        return Some(port);
    }
    None
}

/// 解析端口：已配置的沿用，缺失的分配。
///
/// `persisted` 是本实例上次持久化的值（见 [`load_persisted_ports`]）——优先
/// 于探测，保证跨重启稳定。默认实例（`id == DEFAULT_INSTANCE`）缺省走
/// legacy 三元组且**不探测**。
pub fn resolve_ports(config: &toml::Table, id: &str, persisted: Ports) -> Ports {
    let configured = ports_from_config(config);
    let pick = |configured: u16, persisted: u16| -> u16 {
        if configured > 0 {
            configured
        } else {
            persisted
        }
    };
    let mut ports = Ports {
        reverse_ws: pick(configured.reverse_ws, persisted.reverse_ws),
        onebot_http: pick(configured.onebot_http, persisted.onebot_http),
        webui: pick(configured.webui, persisted.webui),
    };
    if id == DEFAULT_INSTANCE {
        if ports.reverse_ws == 0 {
            ports.reverse_ws = 3131;
        }
        if ports.onebot_http == 0 {
            ports.onebot_http = 3000;
        }
        if ports.webui == 0 {
            ports.webui = 6099;
        }
        return ports;
    }
    let mut taken = std::collections::HashSet::new();
    if ports.reverse_ws == 0 {
        ports.reverse_ws = find_free(REVERSE_RANGE, &mut taken).unwrap_or(3140);
    }
    if ports.onebot_http == 0 {
        ports.onebot_http = find_free(ONEBOT_RANGE, &mut taken).unwrap_or(3010);
    }
    if ports.webui == 0 {
        ports.webui = find_free(WEBUI_RANGE, &mut taken).unwrap_or(6100);
    }
    ports
}

/// 把派生键写进合并后的配置（实例/共享层的显式值一律优先）。
///
/// 多实例（`id != DEFAULT_INSTANCE`）未显式覆盖 → 由 id / 端口派生
/// （多容器互异）；默认实例未覆盖 → 沿用配置里的显式值，缺省时不写
/// （插件默认与迁移前的内置默认一致）。
pub fn derive(
    config: &mut toml::Table,
    overrides: &toml::Table,
    id: &str,
    ports: &Ports,
    data_dir: Option<&Path>,
) {
    let auto = id != DEFAULT_INSTANCE;

    // server.bind_address：端口分配结果强制（其余 server 键允许实例覆盖）。
    {
        let server = config
            .entry("server")
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if let Some(table) = server.as_table_mut() {
            table.insert(
                "bind_address".into(),
                toml::Value::String(format!("0.0.0.0:{}", ports.reverse_ws)),
            );
        }
    }

    for (key, derived) in [
        ("napcat_container", format!("echo-napcat-{id}")),
        (
            "napcat_webui_url",
            format!("http://localhost:{}", ports.webui),
        ),
        (
            "napcat_onebot_url",
            format!("http://localhost:{}", ports.onebot_http),
        ),
    ] {
        // 实例级显式覆盖一律优先（用户写了就照用）；否则多实例派生、
        // 默认实例仅当值为空串时兜底。
        let action: Option<String> = if overrides.contains_key(key) {
            None
        } else if auto {
            Some(derived)
        } else {
            match config.get(key).and_then(toml::Value::as_str) {
                Some(value) if value.trim().is_empty() => Some(derived),
                _ => None,
            }
        };
        if let Some(value) = action {
            config.insert(key.into(), toml::Value::String(value));
        }
    }

    if auto {
        if let Some(path) = auto_compose_path(data_dir, id) {
            config.insert("napcat_compose_file".into(), toml::Value::String(path));
        }
    }
}

/// 自动生成的 compose 路径（`<data_dir>/napcat/<id>/docker-compose.yml`）。
pub fn auto_compose_path(data_dir: Option<&Path>, id: &str) -> Option<String> {
    Some(
        data_dir?
            .join("napcat")
            .join(id)
            .join("docker-compose.yml")
            .to_string_lossy()
            .to_string(),
    )
}

/// 单个实例的 compose 内容（独立容器名 / 端口 / 数据卷）。
pub fn render_compose(id: &str, ports: &Ports) -> String {
    format!(
        r#"# 由 echo-adapter-qq 生成（实例 `{id}`）；手动改动会被下次启动覆盖。
services:
  napcat-{id}:
    container_name: echo-napcat-{id}
    image: mlikiowa/napcat-docker:latest
    restart: unless-stopped
    ports:
      - "{onebot}:3000"
      - "{webui}:6099"
    volumes:
      - ./data/{id}:/app/napcat/config
      - ./data/{id}-files:/app/napcat/files
    extra_hosts:
      - "host.docker.internal:host-gateway"
"#,
        id = id,
        onebot = ports.onebot_http,
        webui = ports.webui,
    )
}

/// 写实例 compose（幂等；内容变化才写）。
pub fn write_compose(data_dir: &Path, id: &str, ports: &Ports) -> Result<PathBuf, String> {
    let dir = data_dir.join("napcat").join(id);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let path = dir.join("docker-compose.yml");
    let content = render_compose(id, ports);
    if std::fs::read_to_string(&path).ok().as_deref() == Some(content.as_str()) {
        return Ok(path);
    }
    std::fs::write(&path, content).map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(path)
}

/// 从实例 state 文件读上次持久化的端口（`[deploy.ports]`）。
pub fn load_persisted_ports(store: &ConfigStore) -> Ports {
    let Ok(root) = store.read() else {
        return Ports::default();
    };
    root.get("deploy")
        .and_then(|d| d.get("ports"))
        .and_then(|p| p.clone().try_into::<Ports>().ok())
        .unwrap_or_default()
}

/// 把端口写回实例 state 文件（幂等：值相同不写）。
pub fn persist_ports(store: &ConfigStore, ports: &Ports) -> Result<(), String> {
    if load_persisted_ports(store) == *ports {
        return Ok(());
    }
    store
        .patch(|root| {
            let deploy = crate::config_store::ensure_table(root, "deploy");
            let value = toml::Value::try_from(ports).map_err(|e| format!("ports: {e}"))?;
            deploy.insert("ports".into(), value);
            Ok(())
        })
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(pairs: &[(&str, &str)]) -> toml::Table {
        let mut t = toml::Table::new();
        for (k, v) in pairs {
            t.insert((*k).into(), toml::Value::String((*v).into()));
        }
        t
    }

    #[test]
    fn merge_is_deep_and_overlay_wins() {
        let mut base = toml::Table::new();
        base.insert(
            "server".into(),
            toml::Value::Table(table(&[
                ("access_token", "base"),
                ("bind_address", "0.0.0.0:3131"),
            ])),
        );
        let mut overlay = toml::Table::new();
        overlay.insert(
            "server".into(),
            toml::Value::Table(table(&[("access_token", "over")])),
        );
        let merged = merge_tables(&base, &overlay);
        assert_eq!(merged["server"]["access_token"].as_str(), Some("over"));
        // 未覆盖的键保留
        assert_eq!(
            merged["server"]["bind_address"].as_str(),
            Some("0.0.0.0:3131")
        );
    }

    #[test]
    fn default_instance_keeps_legacy_ports() {
        let cfg = toml::Table::new();
        let ports = resolve_ports(&cfg, DEFAULT_INSTANCE, Ports::default());
        assert_eq!(ports.reverse_ws, 3131);
        assert_eq!(ports.onebot_http, 3000);
        assert_eq!(ports.webui, 6099);
    }

    #[test]
    fn configured_ports_win_over_defaults() {
        let mut cfg = toml::Table::new();
        cfg.insert(
            "server".into(),
            toml::Value::Table(table(&[("bind_address", "0.0.0.0:3141")])),
        );
        cfg.insert("napcat_webui_url".into(), "http://localhost:7100".into());
        let ports = resolve_ports(&cfg, DEFAULT_INSTANCE, Ports::default());
        assert_eq!(ports.reverse_ws, 3141);
        assert_eq!(ports.webui, 7100);
    }

    #[test]
    fn persisted_ports_are_reused_when_config_has_none() {
        let cfg = toml::Table::new();
        let persisted = Ports {
            reverse_ws: 3150,
            onebot_http: 3050,
            webui: 6150,
        };
        let ports = resolve_ports(&cfg, "alix", persisted.clone());
        assert_eq!(ports, persisted);
    }

    #[test]
    fn derive_names_are_instance_scoped() {
        let mut cfg = toml::Table::new();
        let ports = Ports {
            reverse_ws: 3140,
            onebot_http: 3010,
            webui: 6100,
        };
        derive(
            &mut cfg,
            &toml::Table::new(),
            "alix",
            &ports,
            Some(Path::new("/tmp/x")),
        );
        assert_eq!(cfg["server"]["bind_address"].as_str(), Some("0.0.0.0:3140"));
        assert_eq!(cfg["napcat_container"].as_str(), Some("echo-napcat-alix"));
        assert_eq!(
            cfg["napcat_webui_url"].as_str(),
            Some("http://localhost:6100")
        );
        assert_eq!(
            cfg["napcat_onebot_url"].as_str(),
            Some("http://localhost:3010")
        );
        assert!(cfg["napcat_compose_file"]
            .as_str()
            .unwrap()
            .ends_with("/napcat/alix/docker-compose.yml"));
    }

    #[test]
    fn derive_keeps_explicit_instance_override() {
        let mut cfg = table(&[("napcat_container", "from-shared")]);
        let overrides = table(&[("napcat_container", "mine")]);
        let ports = Ports {
            reverse_ws: 3140,
            onebot_http: 3010,
            webui: 6100,
        };
        // 实例覆盖里显式写了 → 不派生（合并后的值已经是实例自己的）。
        cfg.insert("napcat_container".into(), "mine".into());
        derive(&mut cfg, &overrides, "alix", &ports, None);
        assert_eq!(cfg["napcat_container"].as_str(), Some("mine"));
    }

    #[test]
    fn compose_carries_instance_scoped_names_and_ports() {
        let text = render_compose(
            "alix",
            &Ports {
                reverse_ws: 3140,
                onebot_http: 3010,
                webui: 6100,
            },
        );
        assert!(text.contains("container_name: echo-napcat-alix"));
        assert!(text.contains("\"3010:3000\""));
        assert!(text.contains("\"6100:6099\""));
        assert!(text.contains("mlikiowa/napcat-docker"));
    }

    #[test]
    fn write_compose_is_idempotent() {
        let dir = std::env::temp_dir().join(format!("echo-qq-compose-{}", std::process::id()));
        let ports = Ports {
            reverse_ws: 3140,
            onebot_http: 3010,
            webui: 6100,
        };
        let path = write_compose(&dir, "alix", &ports).expect("write ok");
        let first = std::fs::read_to_string(&path).unwrap();
        let path2 = write_compose(&dir, "alix", &ports).expect("rewrite ok");
        assert_eq!(path, path2);
        assert_eq!(first, std::fs::read_to_string(&path2).unwrap());
        std::fs::remove_dir_all(&dir).ok();
    }
}
