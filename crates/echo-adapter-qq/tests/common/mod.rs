//! 集成测试共享辅助。

/// Create a temporary TOML config file in the system temp dir.
///
/// The file is removed on drop.
pub fn temp_config_file(name: &str, content: &str) -> TempFile {
    let path = std::env::temp_dir().join(format!("echo-test-{name}-{}.toml", uuid_lite()));
    std::fs::write(&path, content).expect("write temp config");
    TempFile { path }
}

/// RAII guard that removes a temp file when dropped.
pub struct TempFile {
    path: std::path::PathBuf,
}

impl TempFile {
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A short random-ish suffix (test-only, uniqueness is all that matters).
fn uuid_lite() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{nanos:x}")
}
