//! QQ 白名单持久化契约：`persist_filter` 对名单表是整表替换写入，
//! 必须原样带回 `group_members_open`（群成员放行开关）——否则 Panel 的
//! 任一名单编辑都会把开关静默清掉。同时覆盖"运行时改名单立即落盘"。

use echo_adapter_qq::config::QqAdapterConfig;
use echo_adapter_qq::QqAdapter;

#[test]
fn update_allowlist_persists_group_members_open() {
    let file = echo_test_utils::temp_config_file(
        "group-members-open",
        "[adapters.qq]\nenabled = false\n\n\
         [adapters.qq.filter.allowlist]\nuser_ids = [1]\ngroup_ids = [2]\n\
         group_members_open = true\n",
    );

    let mut config = QqAdapterConfig::default();
    config.filter.allowlist.user_ids = vec![1];
    config.filter.allowlist.group_ids = vec![2];
    config.filter.allowlist.group_members_open = true;

    let adapter = QqAdapter::new(config);
    adapter.set_config_store(echo_adapter::ConfigStore::new(file.path().to_path_buf()));

    // 模拟 Panel 交互：运行时替换名单 → 触发 rebuild + persist。
    adapter.update_allowlist(vec![3], vec![4]);

    let text = std::fs::read_to_string(file.path()).expect("read persisted config");
    let doc: toml::Value = toml::from_str(&text).expect("parse persisted config");
    let allow = &doc["adapters"]["qq"]["filter"]["allowlist"];
    assert_eq!(
        allow["user_ids"][0].as_integer(),
        Some(3),
        "runtime user list must be persisted"
    );
    assert_eq!(
        allow["group_ids"][0].as_integer(),
        Some(4),
        "runtime group list must be persisted"
    );
    assert_eq!(
        allow["group_members_open"].as_bool(),
        Some(true),
        "整表替换不得丢失群成员放行开关"
    );
}

#[test]
fn update_allowlist_persists_group_members_open_false_as_is() {
    // 关闭态同样必须显式落盘（false 不是"缺省"，而是明确的口径）。
    let file = echo_test_utils::temp_config_file(
        "group-members-closed",
        "[adapters.qq]\nenabled = false\n\n[adapters.qq.filter.allowlist]\n\
         user_ids = [11]\ngroup_ids = [22]\n",
    );

    let mut config = QqAdapterConfig::default();
    config.filter.allowlist.user_ids = vec![11];
    config.filter.allowlist.group_ids = vec![22];

    let adapter = QqAdapter::new(config);
    adapter.set_config_store(echo_adapter::ConfigStore::new(file.path().to_path_buf()));
    adapter.update_allowlist(vec![33], vec![44]);

    let text = std::fs::read_to_string(file.path()).expect("read persisted config");
    let doc: toml::Value = toml::from_str(&text).expect("parse persisted config");
    let allow = &doc["adapters"]["qq"]["filter"]["allowlist"];
    assert_eq!(allow["group_members_open"].as_bool(), Some(false));
}
