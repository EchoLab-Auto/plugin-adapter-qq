//! 插件侧静态元数据：UI 描述符 / 会话标签 / 配置 Schema（泛化 P1）。
//!
//! 三份数据经 [`crate::plugin::QqPlugin::adapter`] 填入 `AdapterContribution`，
//! 随注册下发宿主，再由 AdapterList 原样透传给 Panel 的通用渲染器——
//! 插件只贡献 schema，渲染逻辑在 Panel（Panel UI 解耦）。
//!
//! 形状即契约：UI 描述符按 design/adapter-genericization/01a §2.1（3 panel /
//! 7 类节点），`bind` 路径与状态文档形状（evidence/00-frozen-interfaces §三
//! `cached_state`）严格一致；本模块快照测试锁定结构。

use serde_json::{json, Value};

/// UI 描述符 v1：3 个 panel（`login` / `gate` / `owner`）、7 类节点
/// （status / text / image / link / select / list_editor / button）。
///
/// - `bind` = `cached_state()` 内路径（00 §三）；`action` = custom 方法 +
///   参数模板（`$value` 为节点当前值，`arg` 可嵌套对象）；
/// - 扩展字段（01a §3.1 写入 v1）：select `options` 对象形态的 `hint`、
///   image `clear_when`、text `editable`。
pub fn ui_descriptor() -> Value {
    json!({
        "version": 1,
        "panels": [
            {
                "id": "login",
                "title": "登录",
                "nodes": [
                    { "type": "status", "bind": "online", "label": "QQ 连接" },
                    { "type": "text", "bind": "self_id", "label": "账号" },
                    {
                        "type": "image",
                        "label": "登录二维码",
                        "clear_when": "online",
                        "fetch": { "method": "login_qrcode", "result_key": "png_base64" }
                    },
                    { "type": "link", "bind": "instance.webui_url", "label": "打开 NapCat WebUI" }
                ]
            },
            {
                "id": "gate",
                "title": "消息门控",
                "nodes": [
                    {
                        "type": "select",
                        "bind": "gate.mode",
                        "label": "门控模式",
                        "options": [
                            { "value": "none", "label": "不限制", "hint": "所有消息均响应" },
                            { "value": "allowlist", "label": "白名单", "hint": "仅响应名单内用户/群" },
                            { "value": "denylist", "label": "黑名单", "hint": "忽略名单内用户/群" }
                        ],
                        "action": { "method": "set_gate_mode", "arg": { "mode": "$value" } }
                    },
                    {
                        "type": "list_editor",
                        "bind": "gate.allowlist_users",
                        "label": "白名单用户",
                        "action": { "method": "update_allowlist", "arg": { "user_ids": "$value" } }
                    },
                    {
                        "type": "list_editor",
                        "bind": "gate.allowlist_groups",
                        "label": "白名单群",
                        "action": { "method": "update_allowlist", "arg": { "group_ids": "$value" } }
                    },
                    {
                        "type": "list_editor",
                        "bind": "gate.denylist_users",
                        "label": "黑名单用户",
                        "action": { "method": "update_denylist", "arg": { "user_ids": "$value" } }
                    },
                    {
                        "type": "list_editor",
                        "bind": "gate.denylist_groups",
                        "label": "黑名单群",
                        "action": { "method": "update_denylist", "arg": { "group_ids": "$value" } }
                    }
                ]
            },
            {
                "id": "owner",
                "title": "主人",
                "nodes": [
                    {
                        "type": "text",
                        "bind": "owner",
                        "label": "主人 QQ",
                        "editable": true,
                        "action": { "method": "set_owner", "arg": { "owner_qq": "$value" } }
                    },
                    {
                        "type": "button",
                        "label": "刷新状态",
                        "action": { "method": "get_state" }
                    }
                ]
            }
        ]
    })
}

/// 会话分组标签（`SessionSwitcher` 等按 platform 分组时的显示名）。
pub fn session_labels() -> Value {
    json!({ "dm": "QQ 私聊", "group": "QQ 群" })
}

/// 插件配置 JSON Schema（draft-07 子集；当前仅作声明，Core / Panel v1 不消费）。
///
/// 字段对齐插件配置结构（`PluginConfig.config` = `QqAdapterConfig`）。
/// `bind_address` 的 `default` 照抄 01a §2.2 草案；插件运行期实际缺省为
/// `"0.0.0.0:3131"`（config.rs `QqServerConfig::default`），01a 标注该值
/// 「待验证」，此处保持草案原文、留待契约确认后统一。
pub fn config_schema() -> Value {
    json!({
        "type": "object",
        "required": ["bind_address"],
        "properties": {
            "bind_address":      { "type": "string", "title": "OneBot 反向 WS 监听地址", "default": "127.0.0.1:0" },
            "napcat_container":  { "type": "string", "title": "NapCat 容器名" },
            "napcat_webui_url":  { "type": "string", "format": "uri", "title": "NapCat WebUI" },
            "napcat_onebot_url": { "type": "string", "format": "uri", "title": "OneBot URL" },
            "persona":           { "type": "string", "title": "绑定人格" }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 描述符快照（01a §2.1）：版本 1 + 三 panel + 7 类节点。
    #[test]
    fn ui_descriptor_snapshot_shape() {
        let ui = ui_descriptor();
        assert_eq!(ui["version"], json!(1), "描述符版本必须为 1");

        let panels = ui["panels"].as_array().expect("panels 数组");
        let ids: Vec<&str> = panels
            .iter()
            .filter_map(|panel| panel["id"].as_str())
            .collect();
        assert_eq!(ids, vec!["login", "gate", "owner"]);

        // 7 类节点齐备（03a §2 节点表；QQ parity 前提）。
        let mut types: Vec<&str> = Vec::new();
        for panel in panels {
            for node in panel["nodes"].as_array().expect("nodes 数组") {
                if let Some(kind) = node["type"].as_str() {
                    types.push(kind);
                }
                assert!(
                    node["label"]
                        .as_str()
                        .is_some_and(|label| !label.is_empty()),
                    "节点必须有非空 label：{node}"
                );
            }
        }
        types.sort_unstable();
        types.dedup();
        assert_eq!(
            types,
            vec![
                "button",
                "image",
                "link",
                "list_editor",
                "select",
                "status",
                "text"
            ],
            "7 类节点必须齐备"
        );
    }

    /// `bind` 路径与状态文档形状（evidence/00 §三 cached_state）一致。
    #[test]
    fn ui_descriptor_binds_match_state_shape() {
        let ui = ui_descriptor();
        let binds: Vec<String> = ui["panels"]
            .as_array()
            .expect("panels 数组")
            .iter()
            .flat_map(|panel| panel["nodes"].as_array().cloned().unwrap_or_default())
            .filter_map(|node| node["bind"].as_str().map(str::to_string))
            .collect();
        assert_eq!(
            binds,
            vec![
                "online",
                "self_id",
                "instance.webui_url",
                "gate.mode",
                "gate.allowlist_users",
                "gate.allowlist_groups",
                "gate.denylist_users",
                "gate.denylist_groups",
                "owner",
            ],
            "bind 路径即 00 §三 冻结的状态文档路径"
        );
        // 根路径必须落在 cached_state 的顶层键内。
        for bind in &binds {
            let root = bind.split('.').next().unwrap_or(bind);
            assert!(
                ["instance", "online", "self_id", "gate", "owner"].contains(&root),
                "bind 根路径不在 cached_state 顶层：{bind}"
            );
        }
    }

    /// 扩展字段（01a §3.1）：select `hint`、image `clear_when`、text `editable`、
    /// 动作模板与 custom 方法名。
    #[test]
    fn ui_descriptor_extended_fields_snapshot() {
        let ui = ui_descriptor();
        let panel_nodes = |id: &str| -> Vec<Value> {
            ui["panels"]
                .as_array()
                .expect("panels 数组")
                .iter()
                .find(|panel| panel["id"] == json!(id))
                .expect("panel 存在")["nodes"]
                .as_array()
                .cloned()
                .expect("nodes 数组")
        };

        // login：二维码 image 声明 clear_when + fetch；link 指向 WebUI。
        let login = panel_nodes("login");
        let image = login
            .iter()
            .find(|node| node["type"] == json!("image"))
            .expect("二维码 image 节点");
        assert_eq!(image["clear_when"], json!("online"));
        assert_eq!(image["fetch"]["method"], json!("login_qrcode"));
        assert_eq!(image["fetch"]["result_key"], json!("png_base64"));
        assert_eq!(login[0]["type"], json!("status"));
        assert_eq!(login[0]["bind"], json!("online"));

        // gate：select 的 options 为对象形态（value/label/hint）+ 四个 list_editor。
        let gate = panel_nodes("gate");
        let select = &gate[0];
        assert_eq!(select["type"], json!("select"));
        assert_eq!(
            select["action"],
            json!({ "method": "set_gate_mode", "arg": { "mode": "$value" } })
        );
        let options = select["options"].as_array().expect("options 数组");
        assert_eq!(options.len(), 3, "none / allowlist / denylist");
        for option in options {
            assert!(option["value"].as_str().is_some(), "option.value：{option}");
            assert!(option["label"].as_str().is_some(), "option.label：{option}");
            assert!(option["hint"].as_str().is_some(), "option.hint：{option}");
        }
        let editor_actions: Vec<Value> = gate
            .iter()
            .filter(|node| node["type"] == json!("list_editor"))
            .map(|node| node["action"].clone())
            .collect();
        assert_eq!(
            editor_actions,
            vec![
                json!({ "method": "update_allowlist", "arg": { "user_ids": "$value" } }),
                json!({ "method": "update_allowlist", "arg": { "group_ids": "$value" } }),
                json!({ "method": "update_denylist", "arg": { "user_ids": "$value" } }),
                json!({ "method": "update_denylist", "arg": { "group_ids": "$value" } }),
            ],
            "四张名单卡：方法名 + 单表单字段（部分载荷）"
        );

        // owner：可编辑 text（set_owner）+ 刷新按钮（get_state，01a §1.3）。
        let owner = panel_nodes("owner");
        assert_eq!(owner[0]["type"], json!("text"));
        assert_eq!(owner[0]["bind"], json!("owner"));
        assert_eq!(owner[0]["editable"], json!(true));
        assert_eq!(
            owner[0]["action"],
            json!({ "method": "set_owner", "arg": { "owner_qq": "$value" } })
        );
        assert_eq!(owner[1]["type"], json!("button"));
        assert_eq!(owner[1]["action"], json!({ "method": "get_state" }));
    }

    /// 会话标签（01a §2.2）：私聊 / 群聊显示名。
    #[test]
    fn session_labels_snapshot() {
        assert_eq!(session_labels(), json!({ "dm": "QQ 私聊", "group": "QQ 群" }));
    }

    /// 配置 Schema（01a §2.2）：对象 + required + 五个配置字段。
    #[test]
    fn config_schema_snapshot() {
        let schema = config_schema();
        assert_eq!(schema["type"], json!("object"));
        assert_eq!(schema["required"], json!(["bind_address"]));
        for key in [
            "bind_address",
            "napcat_container",
            "napcat_webui_url",
            "napcat_onebot_url",
            "persona",
        ] {
            assert!(
                schema["properties"][key].is_object(),
                "缺少配置字段声明：{key}"
            );
        }
    }
}
