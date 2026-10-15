//! echo-defs 的 vendored 子集（过渡期）：仅保留 QQ 适配器实际用到的
//! `chat`（消息 / 目标类型）、`mode`（`GateMode`）、`media_store`（入站
//! 图片落盘）。插件化完成后由 `echo-plugin-api` 类型与 `store_media`
//! HostCall 取代，届时整个 vendor 目录删除。

pub mod chat;
pub mod media_store;
pub mod mode;

pub use mode::GateMode;
