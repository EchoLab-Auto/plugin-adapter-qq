//! Centralised, atomic TOML config persistence.
//!
//! Both `Agent::persist_config` and `QqAdapter::persist_filter` used to read,
//! patch and rewrite the same config file independently — concurrent writes
//! could silently lose each other's changes. `ConfigStore` serialises all
//! writes through a mutex and writes atomically (tmp file + rename).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Error type for config store operations.
#[derive(Debug, thiserror::Error)]
pub enum ConfigStoreError {
    #[error("config read failed: {0}")]
    Read(String),
    #[error("config parse failed: {0}")]
    Parse(String),
    #[error("config serialize failed: {0}")]
    Serialize(String),
    #[error("config write failed: {0}")]
    Write(String),
}

/// Shared, atomic access to one TOML config file.
#[derive(Clone)]
pub struct ConfigStore {
    inner: Arc<ConfigStoreInner>,
}

struct ConfigStoreInner {
    path: PathBuf,
    /// Serialises read-modify-write cycles; held across the file I/O so two
    /// patchers cannot interleave and lose updates.
    ///
    /// Deliberately a `std::sync::Mutex` (not tokio): `patch` is called from
    /// both async contexts and from synchronous `Adapter` trait methods.
    write_lock: Mutex<()>,
}

impl ConfigStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            inner: Arc::new(ConfigStoreInner {
                path: path.into(),
                write_lock: Mutex::new(()),
            }),
        }
    }

    /// The config file path this store manages.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Read the whole document as a TOML table.
    pub fn read(&self) -> Result<toml::Table, ConfigStoreError> {
        let content = std::fs::read_to_string(&self.inner.path)
            .map_err(|e| ConfigStoreError::Read(e.to_string()))?;
        toml::from_str(&content).map_err(|e| ConfigStoreError::Parse(e.to_string()))
    }

    /// Atomically apply `f` to the document and write it back.
    ///
    /// `f` is responsible for mutating only what it owns; the whole file is
    /// serialised and rewritten so no section is ever lost. A missing file
    /// starts from an empty document (the write recreates it).
    pub fn patch(
        &self,
        f: impl FnOnce(&mut toml::Table) -> Result<(), String>,
    ) -> Result<(), ConfigStoreError> {
        let _guard = self.inner.write_lock.lock().expect("config store poisoned");
        let mut root = match self.read() {
            Ok(r) => r,
            Err(ConfigStoreError::Read(_)) => toml::Table::new(),
            Err(e) => return Err(e),
        };
        f(&mut root).map_err(ConfigStoreError::Write)?;
        self.write(&root)
    }

    /// Serialise and atomically write the whole document.
    fn write(&self, root: &toml::Table) -> Result<(), ConfigStoreError> {
        let updated =
            toml::to_string_pretty(root).map_err(|e| ConfigStoreError::Serialize(e.to_string()))?;
        let tmp = format!("{}.tmp", self.inner.path.display());
        std::fs::write(&tmp, &updated).map_err(|e| ConfigStoreError::Write(e.to_string()))?;
        if let Err(e) = std::fs::rename(&tmp, &self.inner.path) {
            // Don't leave a stale .tmp behind.
            let _ = std::fs::remove_file(&tmp);
            return Err(ConfigStoreError::Write(format!(
                "rename to {}: {e}",
                self.inner.path.display()
            )));
        }
        Ok(())
    }
}

/// Get or create a sub-table inside `parent`.
///
/// If the key holds a non-table value (hand-edited/corrupted config), it is
/// replaced with an empty table instead of panicking.
pub fn ensure_table<'a>(parent: &'a mut toml::Table, key: &str) -> &'a mut toml::Table {
    if !matches!(parent.get(key), Some(toml::Value::Table(_))) {
        tracing::warn!(key = %key, "config section is not a table, replacing with empty table");
        parent.insert(key.into(), toml::Value::Table(toml::Table::new()));
    }
    match parent.get_mut(key) {
        Some(toml::Value::Table(t)) => t,
        _ => unreachable!("just inserted a table at {key}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "echo-config-store-{name}-{}.toml",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn patch_creates_missing_file_and_section() {
        let path = temp_path("create");
        let store = ConfigStore::new(path.clone());
        store
            .patch(|root| {
                ensure_table(ensure_table(root, "adapters"), "qq")
                    .insert("mode".into(), toml::Value::String("allowlist".into()));
                Ok(())
            })
            .unwrap();
        let root = store.read().unwrap();
        assert_eq!(root["adapters"]["qq"]["mode"].as_str(), Some("allowlist"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn patch_preserves_unrelated_sections() {
        let path = temp_path("preserve");
        std::fs::write(
            &path,
            "[agent]\nprovider = \"openai\"\nmodel = \"m1\"\n\n[other]\nkeep = true\n",
        )
        .unwrap();
        let store = ConfigStore::new(path.clone());
        store
            .patch(|root| {
                let agent = ensure_table(root, "agent");
                agent.insert("model".into(), toml::Value::String("m2".into()));
                Ok(())
            })
            .unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("model = \"m2\""));
        assert!(
            content.contains("keep = true"),
            "unrelated section lost:\n{content}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn patch_overwrites_non_table_value_safely() {
        let path = temp_path("nontable");
        std::fs::write(&path, "[adapters]\nqq = \"disabled\"\n").unwrap();
        let store = ConfigStore::new(path.clone());
        store
            .patch(|root| {
                ensure_table(ensure_table(root, "adapters"), "qq")
                    .insert("mode".into(), toml::Value::String("none".into()));
                Ok(())
            })
            .unwrap();
        let root = store.read().unwrap();
        assert_eq!(root["adapters"]["qq"]["mode"].as_str(), Some("none"));
        let _ = std::fs::remove_file(&path);
    }
}
