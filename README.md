# echo-adapter-qq

QQ / OneBot v11 适配器——独立进程的适配器插件（stdio 协议，见 EchoAgentCore `document/adapter-plugin.md`）。

## 布局

| 路径 | 内容 |
|---|---|
| `crates/echo-adapter-qq` | 适配器主体：反向 WS 接入、NapCat 容器管理、门控 / 过滤、事件转换、插件入口 |
| `crates/echo-core` | OneBot v11 协议类型（事件 / 消息段 / action） |
| `crates/echo-server` | 反向 WebSocket 服务端与 handler 分发 |

插件内部模块（`crates/echo-adapter-qq/src/`）：`types.rs`（领域词汇：入站消息 / 出站目标 / 事件 / 状态）、`filter.rs` + `config_store.rs`（门控过滤管道与 TOML 持久化）、`media.rs`（宿主媒体登记通道抽象）、`plugin.rs`（`AdapterPlugin` 实现）。全部为插件自有代码，不再依赖任何 echo-* 中间 crate。

## 插件形态（P1.7b）

二进制 `echo-adapter-qq` 经 stdio 承载协议帧（4 字节 LE 长度前缀 + JSON）。
宿主下发配置（`Hello.config`）：

```jsonc
{
  "name": "qq",                  // 实例名（= Invoke 前缀；缺省 "qq"）
  "persona": "default",          // 归属人格（可选）
  "state_file": "/path/qq.toml", // 运行期状态持久化文件（可选；门控 / 名单 / owner，
                                 //  重启时覆盖宿主基线——崩溃不回退）
  "config": { /* QqAdapterConfig：端口 / NapCat / 过滤…… */ }
}
```

op 路由（`Invoke("qq:<op>")`）：

| op | 载荷 | 返回 |
|---|---|---|
| `start` / `stop` | `{}` | `{}` |
| `status` | `{}` | `{state, self_id?, started_at?}` |
| `send_text` | `{target, content}` | `{message_id?, success, error?}` |
| `send_media` | `{target, kind: "image"\|"file", source: {url\|path\|data_uri}, file_name?}` | `{message_id?, success, error?}` |
| `get_groups` / `get_friends` | `{}` | `{groups\|friends: [{id, name}]}` |
| `custom` | `{method, payload}` | 平台动作透传（门控 / 名单 / owner / 登录二维码 / `instance_info`…） |

入站消息与连接态经宿主服务 `adapter_host` 回呼（`deliver_message` /
`report_state`；反压 `backpressure` 自动小步重试）。
入站图片下载后经 `adapter_host.store_media` 登记到宿主媒体库（写临时文件传
path，字节不过协议帧），链路上只留 `/media/<id>` 引用。

## 迁出进度（2026-10 完成）

- [x] P1.7a 代码迁入、独立编译（194 个 QQ 测试与原宿主 crate 一致）
- [x] P1.7b 插件入口：`AdapterPlugin` 实现 + `serve_adapter_stdio`；握手 / op 路由 / 入站投递全链路测试 + 真实进程冒烟
- [x] P1.7c 去 vendor：领域词汇 / filter / ConfigStore 收归插件自有（trait 退役为固有方法）；入站图片走宿主 `store_media`；`vendor/` 已删除，仅依赖 `echo-plugin-sdk` + 传输 crate（`echo-core` / `echo-server`）
- [x] P1.8 宿主装配：Core 按实例组装 `Hello.config` 拉起本插件；入站 worker / 状态转发 / 工具管理面路由；真实 core + 真插件进程 + 真 NapCat 连接冒烟
- [x] P1.9 宿主收尾：命令域迁移（插件侧标注 `metadata.sender_role`）、宿主内置 QQ 装配删除——**本仓库的 `crates/echo-core` / `crates/echo-server` / `crates/echo-adapter-qq` 为唯一代码源**（宿主工作区已删除同名 crate）

> 宿主侧操作手册（配置 / op 路由 / 回呼协议）见 EchoAgentCore 仓库的
> `document/adapter-plugin.md`。

## 构建 / 测试

```bash
cargo test --workspace
ECHO_LOG=debug ./target/debug/echo-adapter-qq   # 手动调试（日志走 stderr）
```

开发期 `echo-plugin-sdk` 以本机 path 依赖（`~/.local/share/echo-agent-core/source/source/plugin/echo-plugin-sdk`）；SDK 发布后切换 git tag。

## 部署

```bash
scripts/deploy.sh              # 一键：构建 → 安装 → 重启 Core → 健康检查（等待 QQ 重连）
scripts/deploy.sh --check      # 只读健康检查（安装 / Core / 插件进程 / QQ 连接）
scripts/deploy.sh --rollback   # 回滚到上一版本（<target>.rollback）并重启
scripts/deploy.sh --help       # 全部选项
```

`scripts/deploy.sh` 与 Core 自更新脚本（`~/.local/libexec/echo-agent-core/update.sh`）
语义对齐：同目录临时文件 + 原子替换（可安全替换运行中的二进制）、替换前保留
`<target>.rollback` 备份、与 Core 自更新共用 `update.lock`、重启后等待 NapCat
自动重连（40s 窗口，超时才 `docker restart` 兜底）。安装路径缺省读
`core.toml` 的 `[adapters.qq.plugin] command`，兜底约定目录
`~/.local/libexec/echo-agent-core/plugins/echo-adapter-qq/echo-adapter-qq`。

> 注意：若在 Core 服务的 cgroup 内运行（例如经 Agent 工具调用），`restart` 会
> 连带终止脚本本身（验证与自动回滚不会执行）——请在独立终端运行，或用
> `systemd-run --user --scope` 脱出 cgroup。
