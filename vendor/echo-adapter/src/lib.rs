//! echo-adapter 的 vendored 子集（过渡期）：`Adapter` / `AdapterManagement`
//! trait、门控过滤管道、`ConfigStore`。宿主侧 `AdapterRegistry` 不在此。
//! 插件化完成后 trait 由 `echo-plugin-sdk::AdapterPlugin` 取代。

pub mod capabilities;
pub mod config_store;
pub mod filter;
pub mod traits;
pub mod types;

pub use capabilities::AdapterManagement;
pub use config_store::{ensure_table, ConfigStore};

pub use filter::{FilterPipeline, FilterResult, MessageFilter};
pub use traits::{
    Adapter, AdapterConnectionState, AdapterError, AdapterInfo, GateMode, InboundMessageHook,
};
pub use types::{AdapterEvent, ChannelType, IncomingMessage, MessageTarget, SendResult};
