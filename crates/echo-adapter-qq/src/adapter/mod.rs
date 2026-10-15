//! 平台适配器命名空间：QQ 实现在 [`qq`] 子模块。
//!
//! 未来新增平台适配器（微信/飞书/钉钉等）以并列子模块收纳
//! （`adapter/wechat/` …），`adapter/mod.rs` 只做 re-export，不承载实现。

pub mod qq;

pub use qq::{QqAdapter, QqGateMode, DEFAULT_INSTANCE_NAME};
