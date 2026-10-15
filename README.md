# echo-adapter-qq

QQ / OneBot v11 适配器，从 EchoAgentCore 迁出，目标形态为独立进程的适配器插件（stdio 协议，见 EchoAgentCore `document/adapter-plugin.md`）。

## 布局

| 路径 | 内容 |
|---|---|
| `crates/echo-adapter-qq` | 适配器主体：反向 WS 接入、NapCat 容器管理、门控 / 过滤、事件转换 |
| `crates/echo-core` | OneBot v11 协议类型（事件 / 消息段 / action） |
| `crates/echo-server` | 反向 WebSocket 服务端与 handler 分发 |
| `vendor/*` | 过渡期从 EchoAgentCore 复制的子集（`echo-adapter` trait / filter / ConfigStore，`echo-defs` chat / mode / media_store，测试辅助） |

## 迁出进度

- [x] P1.7a 代码迁入、独立编译，测试与原宿主 crate 一致（194 个 QQ 测试）
- [ ] P1.7b 插件入口：实现 `echo_plugin_sdk::AdapterPlugin`，`main.rs` 走 `serve_adapter_stdio`
- [ ] P1.7c 去 vendor：类型换成 `echo-plugin-api`，入站图片改走 `adapter_host.store_media`
- [ ] P1.8 宿主端到端验证；P1.9 宿主删除内置 QQ

`vendor/` 在 P1.7c 完成后整体删除。

## 构建

```bash
cargo test --workspace
```
