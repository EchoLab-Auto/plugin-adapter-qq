//! QQ owner (admin) runtime update and persistence.

use echo_adapter_qq::config::QqAdapterConfig;
use echo_adapter_qq::QqAdapter;

#[test]
fn set_owner_qq_updates_runtime_value_and_persists() {
    let config = QqAdapterConfig {
        owner_qq: 0,
        ..Default::default()
    };
    let adapter = QqAdapter::new(config);
    // No config store attached — the runtime value still updates.
    adapter.set_owner_qq(123456789);
    assert_eq!(adapter.get_owner_qq(), 123456789);
}

#[test]
fn set_owner_qq_zero_clears_owner() {
    let config = QqAdapterConfig {
        owner_qq: 999,
        ..Default::default()
    };
    let adapter = QqAdapter::new(config);
    assert_eq!(adapter.get_owner_qq(), 999, "initialized from config");
    adapter.set_owner_qq(0);
    assert_eq!(adapter.get_owner_qq(), 0, "cleared");
}

#[test]
fn set_owner_qq_persists_through_config_store() {
    let file = echo_test_utils::temp_config_file(
        "owner-qq",
        "[adapters.qq]\nenabled = false\nowner_qq = 0\n",
    );
    let config = QqAdapterConfig {
        owner_qq: 0,
        ..Default::default()
    };
    let adapter = QqAdapter::new(config);
    adapter.set_config_store(echo_adapter::ConfigStore::new(file.path().to_path_buf()));
    adapter.set_owner_qq(777);

    // The runtime value is updated immediately.
    assert_eq!(adapter.get_owner_qq(), 777);

    // The on-disk config now carries the owner.
    let content = std::fs::read_to_string(file.path()).unwrap();
    assert!(
        content.contains("owner_qq = 777"),
        "persisted owner missing in config: {content}"
    );
}
