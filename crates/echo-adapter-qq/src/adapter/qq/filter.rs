//! Filter and gate management for the QQ adapter.
//!
//! Split out of `mod.rs`: allowlist/denylist updates, gate mode, and
//! TOML persistence through the shared ConfigStore.

use std::sync::Arc;

use crate::adapter::{QqAdapter, QqGateMode};

impl QqAdapter {
    pub(crate) fn rebuild_filter_pipeline(&self) {
        // Merge runtime overrides into config.
        let mut cfg = self.inner.config.clone();
        // 运行时 owner 优先（文档「门控豁免一律读运行时值」）：set_owner_qq
        // 只写 runtime 槽 + 持久化，启动值 config.owner_qq 不再反映当前
        // owner——不 merge 则管道里的 AdminBypassFilter 永远停在启动值。
        cfg.owner_qq = *self.inner.owner_qq.lock().expect("poisoned");
        cfg.filter.allowlist.user_ids = self
            .inner
            .runtime_allowlist_users
            .lock()
            .expect("poisoned")
            .clone();
        cfg.filter.allowlist.group_ids = self
            .inner
            .runtime_allowlist_groups
            .lock()
            .expect("poisoned")
            .clone();
        cfg.filter.denylist.user_ids = self
            .inner
            .runtime_denylist_users
            .lock()
            .expect("poisoned")
            .clone();
        cfg.filter.denylist.group_ids = self
            .inner
            .runtime_denylist_groups
            .lock()
            .expect("poisoned")
            .clone();

        let mode_str = match self.get_gate_mode() {
            QqGateMode::Allowlist => "allowlist",
            QqGateMode::Denylist => "denylist",
            _ => "none",
        };

        let pipeline = cfg.build_filter_pipeline_gated(mode_str);
        *self.inner.filter.lock().expect("filter poisoned") = Some(Arc::new(pipeline));
    }

    /// Update the runtime allowlist. Changes take effect immediately and persist.
    pub fn update_allowlist(&self, user_ids: Vec<i64>, group_ids: Vec<i64>) {
        *self.inner.runtime_allowlist_users.lock().expect("poisoned") = user_ids;
        *self
            .inner
            .runtime_allowlist_groups
            .lock()
            .expect("poisoned") = group_ids;
        self.rebuild_filter_pipeline();
        self.persist_filter();
        tracing::info!("QQ allowlist updated");
    }

    /// Update the runtime denylist. Changes take effect immediately and persist.
    pub fn update_denylist(&self, user_ids: Vec<i64>, group_ids: Vec<i64>) {
        *self.inner.runtime_denylist_users.lock().expect("poisoned") = user_ids;
        *self.inner.runtime_denylist_groups.lock().expect("poisoned") = group_ids;
        self.rebuild_filter_pipeline();
        self.persist_filter();
        tracing::info!("QQ denylist updated");
    }

    /// 群可见性门控（历史查询等「读」工具用）：群必须处于当前门控模式的
    /// 可见范围内。语义与**消息门控**一致（白名单为空 = 不限制；黑名单只
    /// 排除名单内）——「agent 读得到的世界 = 它能交互的世界」。
    pub(crate) fn visible_group_check(&self, group_id: i64) -> Result<(), String> {
        match self.get_gate_mode() {
            QqGateMode::None => Ok(()),
            QqGateMode::Allowlist => {
                let allow = &self.get_filter_config().allowlist.group_ids;
                if allow.is_empty() || allow.contains(&group_id) {
                    Ok(())
                } else {
                    Err(format!(
                        "group {group_id} is not in the allowlist; its history is not readable"
                    ))
                }
            }
            QqGateMode::Denylist => {
                let deny = &self.get_filter_config().denylist.group_ids;
                if deny.contains(&group_id) {
                    Err(format!("group {group_id} is denylisted"))
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Set the path to the config file for persisting gate changes.
    pub fn set_config_path(&self, path: String) {
        self.set_config_store(echo_adapter::ConfigStore::new(path));
    }

    /// Attach a shared [`echo_adapter::ConfigStore`] for gate/filter changes.
    ///
    /// Sharing one store with the Agent guarantees concurrent TOML writes
    /// cannot lose each other's sections.
    pub fn set_config_store(&self, store: echo_adapter::ConfigStore) {
        *self.inner.config_store.lock().expect("poisoned") = Some(store);
    }

    /// Persist the current gate mode, allowlist, and denylist to the config
    /// file through the shared ConfigStore (atomic read-modify-write).
    fn persist_filter(&self) {
        let store = match self.inner.config_store.lock().expect("poisoned").clone() {
            Some(s) => s,
            None => {
                tracing::warn!("QQ filter persist skipped: no config store set");
                return;
            }
        };

        let filter_cfg = self.get_filter_config();
        let mode_str = match self.get_gate_mode() {
            QqGateMode::Allowlist => "allowlist",
            QqGateMode::Denylist => "denylist",
            QqGateMode::None => "none",
        };

        if let Err(e) = store.patch(|root| {
            let qq = echo_adapter::ensure_table(echo_adapter::ensure_table(root, "adapters"), "qq");

            qq.insert(
                "gate".into(),
                toml::Value::Table({
                    let mut t = toml::Table::new();
                    t.insert("mode".into(), toml::Value::String(mode_str.to_string()));
                    t
                }),
            );

            let filter = echo_adapter::ensure_table(qq, "filter");
            // 白名单表是**整表替换**写入：`group_members_open` 必须随名单
            // 一起带回，否则用户设置的群成员放行开关会被 Panel 的任一名单
            // 编辑（UpdateQqAllowlist → persist）静默清掉。
            let mut allowlist_table = build_list_table(
                &filter_cfg.allowlist.user_ids,
                &filter_cfg.allowlist.group_ids,
            );
            allowlist_table.insert(
                "group_members_open".into(),
                toml::Value::Boolean(filter_cfg.allowlist.group_members_open),
            );
            filter.insert("allowlist".into(), toml::Value::Table(allowlist_table));
            filter.insert(
                "denylist".into(),
                toml::Value::Table(build_list_table(
                    &filter_cfg.denylist.user_ids,
                    &filter_cfg.denylist.group_ids,
                )),
            );
            Ok(())
        }) {
            tracing::warn!(error = %e, "failed to persist QQ filter config");
        } else {
            tracing::info!(mode = mode_str, "QQ filter config persisted");
        }
    }

    /// Set the gating mode. Rebuilds the filter pipeline and persists.
    pub fn set_gate_mode(&self, mode: QqGateMode) {
        *self.inner.gate_mode.lock().expect("poisoned") = mode;
        self.rebuild_filter_pipeline();
        self.persist_filter();
        tracing::info!(mode = ?mode, "QQ gate mode updated");
    }

    /// Get the current gating mode.
    pub fn get_gate_mode(&self) -> QqGateMode {
        *self.inner.gate_mode.lock().expect("poisoned")
    }

    /// Set the QQ owner (admin) at runtime. Updates the in-memory value used
    /// for gating exemptions and persists `[adapters.qq] owner_qq` through the
    /// shared ConfigStore, so the change survives a restart.
    pub fn set_owner_qq(&self, owner_qq: i64) {
        *self.inner.owner_qq.lock().expect("poisoned") = owner_qq;
        // 管理员绕过读运行时值（文档语义）：owner 变更必须重建过滤管道——
        // 此前 rebuild 只在名单/模式变更时触发，新 owner 重启前照常被拦
        // （2026-10 巡检发现的 🔴）。rebuild 内部会读取最新 runtime owner。
        self.rebuild_filter_pipeline();
        let store = match self.inner.config_store.lock().expect("poisoned").clone() {
            Some(s) => s,
            None => {
                tracing::warn!("QQ owner persist skipped: no config store set");
                return;
            }
        };
        if let Err(e) = store.patch(|root| {
            let qq = echo_adapter::ensure_table(echo_adapter::ensure_table(root, "adapters"), "qq");
            qq.insert("owner_qq".into(), toml::Value::Integer(owner_qq));
            Ok(())
        }) {
            tracing::warn!(error = %e, "failed to persist QQ owner");
        } else {
            tracing::info!(owner_qq, "QQ owner persisted");
        }
    }

    /// Get the current QQ owner (admin) — the runtime value.
    pub fn get_owner_qq(&self) -> i64 {
        *self.inner.owner_qq.lock().expect("poisoned")
    }

    /// Return the current effective filter config (runtime overrides merged).
    pub fn get_filter_config(&self) -> crate::config::QqFilterConfig {
        let mut cfg = self.inner.config.filter.clone();
        cfg.allowlist.user_ids = self
            .inner
            .runtime_allowlist_users
            .lock()
            .expect("poisoned")
            .clone();
        cfg.allowlist.group_ids = self
            .inner
            .runtime_allowlist_groups
            .lock()
            .expect("poisoned")
            .clone();
        cfg.denylist.user_ids = self
            .inner
            .runtime_denylist_users
            .lock()
            .expect("poisoned")
            .clone();
        cfg.denylist.group_ids = self
            .inner
            .runtime_denylist_groups
            .lock()
            .expect("poisoned")
            .clone();
        cfg
    }
}

fn build_list_table(user_ids: &[i64], group_ids: &[i64]) -> toml::Table {
    let mut t = toml::Table::new();
    t.insert(
        "user_ids".into(),
        toml::Value::Array(
            user_ids
                .iter()
                .map(|&id| toml::Value::Integer(id))
                .collect(),
        ),
    );
    t.insert(
        "group_ids".into(),
        toml::Value::Array(
            group_ids
                .iter()
                .map(|&id| toml::Value::Integer(id))
                .collect(),
        ),
    );
    t
}
