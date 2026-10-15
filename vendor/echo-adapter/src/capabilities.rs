//! 适配器管理能力扩展（capability extension）。
//!
//! `Adapter` trait 只保留**平台中立**的最小面：生命周期（start/stop/
//! remove_instance）、收发（send_message/subscribe）、状态（status_info）；
//! 平台管理能力经本模块的 [`AdapterManagement`] 暴露——**唯一的能力扩展
//! 形态**（2026-10 适配器管理通道：原逐平台专属 trait 已删除，见
//! `document/adapter-plugin.md` §P0 W3.5）。

use async_trait::async_trait;
use serde_json::Value;

use crate::traits::AdapterError;

/// 平台中立的适配器管理面（W3.1，2026-10 适配器管理通道）。
///
/// 平台专属管理能力的目标形态（2026-10 适配器管理通道；本 trait 已是唯一能力扩展）：
///
/// - 管理动作经 **method 路由表**（[`AdapterManagement::invoke_custom`]）
///   异步执行——方法名由平台自定义；约定与插件协议 `custom op` 及线协议
///   `InvokeAdapterOp` 同名直通（`set_gate_mode` / `update_allowlist` / …）；
/// - UI / 管理状态经 [`AdapterManagement::cached_state`] **同步读**
///   （inproc = 内部状态投影；remote = epoch 缓存）——实现必须零 IO、
///   不可阻塞（调用方在命令处理路径上同步读取）。
///
/// 取用方式：`adapter.management()` 返回 `Option<&dyn AdapterManagement>`；
/// `None` = 该适配器不提供管理面。
#[async_trait]
pub trait AdapterManagement: Send + Sync {
    /// 当前 UI / 管理状态文档（同步读、零 IO；`None` = 暂无状态）。
    ///
    /// 约定键（与适配器插件规范的 UI 描述符 `bind` 对齐；实现可扩展）：
    /// - `instance`：`{ persona?, container?, webui_url?, onebot_url?, bind_address? }`
    /// - `online` / `self_id`：连接态快照
    /// - `gate`：`{ mode, allowlist_users[], allowlist_groups[], denylist_users[], denylist_groups[] }`
    /// - `owner`：管理员 id（0 = 未设置）
    fn cached_state(&self) -> Option<Value>;

    /// 执行一个管理方法（写路径；方法名与参数由平台定义）。
    ///
    /// 推荐方法名（与 QQ 参考实现对齐，供跨平台消费方复用）：
    /// `set_gate_mode` / `get_gate_mode` / `update_allowlist` /
    /// `update_denylist` / `get_filter_config` / `set_owner` / `get_owner` /
    /// `login_status` / `login_qrcode` / `get_groups` / `get_friends` /
    /// `instance_info`。未知方法返回可读错误（fail-closed）。
    async fn invoke_custom(&self, method: &str, payload: Value) -> Result<Value, AdapterError>;
}
