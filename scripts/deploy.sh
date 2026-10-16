#!/usr/bin/env bash
# =============================================================================
# echo-adapter-qq 部署脚本：构建 → 安装 → 重启 Core → 健康检查
#
# 把本仓库构建的 `echo-adapter-qq` 插件二进制部署到 EchoAgentCore 的插件
# 约定目录（~/.local/libexec/echo-agent-core/plugins/echo-adapter-qq/），并
# （默认）重启 Core 使其以新二进制运行、等待 NapCat 反向 WS 自动重连。
#
# 与 Core 自更新脚本（~/.local/libexec/echo-agent-core/update.sh）对齐：
#   - 安装用「同目录临时文件 + 原子 rename」，可安全替换运行中的二进制；
#   - 替换前保留 `<target>.rollback` 备份，失败可用 --rollback 恢复；
#   - 与 Core 自更新共用一把锁（update.lock），避免并发重启；
#   - 重启后等待 NapCat 自动重连（40s 窗口；超时才 docker restart 兜底）。
#
# 注意：若在 Core 服务的 cgroup 内运行（例如经 Agent 工具调用），`restart`
# 会连带终止本脚本（后续验证 / 自动回滚不会执行）——该场景请用独立终端，
# 或 `systemd-run --user --scope` 脱出 cgroup 后运行。
#
# 用法：scripts/deploy.sh [选项]（--help 查看全部选项）
# =============================================================================
set -euo pipefail

STATE_HOME=${XDG_STATE_HOME:-"$HOME/.local/state"}
CONFIG_HOME=${XDG_CONFIG_HOME:-"$HOME/.config"}
PREFIX=${ECHO_PREFIX:-"$HOME/.local"}

CORE_SERVICE=${ECHO_CORE_SERVICE:-echo-agent-core.service}
CORE_STATE_DIR="$STATE_HOME/echo-agent-core"
LOCK_FILE="$CORE_STATE_DIR/update.lock"
CORE_CONFIG="$CONFIG_HOME/echo-agent-core/core.toml"
DEFAULT_TARGET="$PREFIX/libexec/echo-agent-core/plugins/echo-adapter-qq/echo-adapter-qq"

REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

MODE=deploy            # deploy | rollback | check
OPT_BUILD=1
OPT_RESTART=1
OPT_BINARY=""
OPT_TARGET=""

die() {
    local code=1
    if [[ ${1:-} =~ ^[0-9]+$ ]]; then code=$1; shift; fi
    printf '错误：%s\n' "$*" >&2
    exit "$code"
}
warn() { printf '警告：%s\n' "$*" >&2; }
step() { printf '\n== %s ==\n' "$*"; }

usage() {
    cat <<'EOF'
用法：scripts/deploy.sh [选项]

构建本仓库的 echo-adapter-qq 插件并部署到 Core 插件目录；默认重启 Core
并等待 QQ 恢复。选项：

  --no-build        跳过 cargo 构建，直接部署已有产物（配合 --binary 使用）
  --binary <path>   构建产物路径（缺省 <仓库>/target/release/echo-adapter-qq）
  --target <path>   插件安装路径（缺省读 core.toml 的 [adapters.qq.plugin] command，
                    兜底 ~/.local/libexec/echo-agent-core/plugins/echo-adapter-qq/echo-adapter-qq）
  --no-restart      仅替换二进制，不重启 Core（下次 Core 启动后生效）
  --rollback        回滚：以 <target>.rollback 覆盖安装并（默认）重启 Core
  --check           只读健康检查（安装状态 / Core / 插件进程 / QQ 连接）
  -h, --help        显示本帮助

退出码：0 成功；1 失败（含已自动回滚）；2 用法错误 / 已有更新在进行。
EOF
}

# ── 参数解析 ─────────────────────────────────────────────────────────────────
while (($#)); do
    case "$1" in
        --no-build)   OPT_BUILD=0 ;;
        --no-restart) OPT_RESTART=0 ;;
        --binary)     OPT_BINARY=${2:?--binary 需要一个路径参数}; shift ;;
        --target)     OPT_TARGET=${2:?--target 需要一个路径参数}; shift ;;
        --rollback)   MODE=rollback ;;
        --check)      MODE=check ;;
        -h|--help)    usage; exit 0 ;;
        *)            usage >&2; die 2 "未知选项：$1" ;;
    esac
    shift
done

[[ -f "$REPO_ROOT/Cargo.toml" ]] || die "定位仓库失败（$REPO_ROOT 下没有 Cargo.toml）"

# ── 目标路径解析：--target > core.toml 配置 > 约定默认 ───────────────────────
resolve_target() {
    if [[ -n $OPT_TARGET ]]; then
        printf '%s' "$OPT_TARGET"
        return 0
    fi
    local cmd=""
    if [[ -f $CORE_CONFIG ]]; then
        if command -v python3 >/dev/null 2>&1; then
            cmd=$(python3 - "$CORE_CONFIG" 2>/dev/null <<'PY' || true
import sys
try:
    import tomllib
except ImportError:
    raise SystemExit(1)
try:
    with open(sys.argv[1], "rb") as fh:
        data = tomllib.load(fh)
except Exception:
    raise SystemExit(1)
qq = data.get("adapters", {}).get("qq", {})
plugin = qq.get("plugin", {}) if isinstance(qq, dict) else {}
value = plugin.get("command", "") if isinstance(plugin, dict) else ""
if isinstance(value, str):
    print(value.strip())
PY
            )
        fi
        if [[ -z $cmd ]]; then
            # awk 兜底：仅识别 [adapters.qq.plugin] 段内的 command = …
            cmd=$(awk '
                /^\[adapters\.qq\.plugin\]/ { f = 1; next }
                /^\[/ { f = 0 }
                f && /^[[:space:]]*command[[:space:]]*=/ {
                    sub(/^[^=]*=/, ""); print; exit
                }' "$CORE_CONFIG")
        fi
    fi
    # 去掉首尾空白与引号
    cmd="${cmd#"${cmd%%[![:space:]]*}"}"
    cmd="${cmd%"${cmd##*[![:space:]]}"}"
    cmd="${cmd%\"}"; cmd="${cmd#\"}"
    cmd="${cmd%\'}"; cmd="${cmd#\'}"
    if [[ -n $cmd ]]; then
        if [[ $cmd != /* ]]; then
            die "core.toml 中插件 command 不是绝对路径（$cmd）；请修正配置或显式传 --target"
        fi
        printf '%s' "$cmd"
        return 0
    fi
    printf '%s' "$DEFAULT_TARGET"
}

TARGET=$(resolve_target)
BINARY=${OPT_BINARY:-"$REPO_ROOT/target/release/echo-adapter-qq"}

# ── 基础工具函数 ─────────────────────────────────────────────────────────────
service_known()  { systemctl --user cat "$CORE_SERVICE" >/dev/null 2>&1; }
service_active() { systemctl --user is-active --quiet "$CORE_SERVICE" 2>/dev/null; }
core_main_pid()  { systemctl --user show -p MainPID --value "$CORE_SERVICE" 2>/dev/null || true; }

running_in_core_cgroup() {
    local self_cg service_cg
    self_cg=$(cat /proc/self/cgroup 2>/dev/null || true)
    service_cg=$(systemctl --user show -p ControlGroup --value "$CORE_SERVICE" 2>/dev/null || true)
    [[ -n $self_cg && -n $service_cg && $self_cg == *"$service_cg"* ]]
}

wait_service_active() { # $1=窗口秒数
    local deadline=$((SECONDS + ${1:-30}))
    while (( SECONDS < deadline )); do
        if service_active; then return 0; fi
        sleep 0.5
    done
    return 1
}

restart_core_and_wait() {
    echo "重启 Core（systemctl --user restart $CORE_SERVICE）…"
    systemctl --user restart "$CORE_SERVICE" >/dev/null 2>&1 || true
    if wait_service_active 30; then
        echo "Core 已恢复 active（PID $(core_main_pid)）"
        return 0
    fi
    return 1
}

# ── 进程识别：/proc/PID/exe 的读取受 ptrace/yama（scope=1）限制，在不同
#    运行上下文（终端 / 服务 cgroup / Agent 工具）下权限不一致；cmdline 与
#    /proc/PID/stat 则可跨上下文读取。故用三重识别 + 启动时间校验。 ──────────
stat_after_comm() { # $1=pid → comm 之后的字段（state 起；comm 本身可含空格）
    local line
    line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1
    printf '%s' "${line##*) }"
}

proc_comm() { # $1=pid → 进程名（comm，≤15 字符）
    local line
    line=$(cat "/proc/$1/stat" 2>/dev/null) || return 1
    line=${line#*(}
    printf '%s' "${line%%)*}"
}

proc_ppid() { # $1=pid → 父进程 PID
    local rest
    rest=$(stat_after_comm "$1") || return 1
    set -- $rest     # rest: state ppid pgrp ……
    printf '%s' "${2:-}"
}

proc_start_epoch() { # $1=pid → 启动时刻（epoch 秒；stat 第 22 字段 starttime）
    local rest starttime hz btime
    rest=$(stat_after_comm "$1") || return 1
    set -- $rest     # comm 之后第 20 个 = starttime
    starttime=${20:-}
    [[ -n $starttime ]] || return 1
    hz=$(getconf CLK_TCK 2>/dev/null || echo 100)
    btime=$(awk '/^btime/{print $2}' /proc/stat 2>/dev/null)
    [[ -n $btime ]] || return 1
    printf '%s' "$(( btime + starttime / hz ))"
}

proc_arg0() { # $1=pid → 命令行第一个参数（cmdline 首段）
    head -c 4096 "/proc/$1/cmdline" 2>/dev/null | tr '\0' '\n' | head -n1
}

plugin_pids() { # $1=目标 exe 路径 → 输出插件进程 PID（按序短路的三重识别）
    local target=$1 base core_pid pid arg0 exe
    base=$(basename -- "$target"); base=${base:0:15}
    core_pid=$(core_main_pid)
    for d in /proc/[0-9]*; do
        pid=${d#/proc/}
        # ① cmdline 首参精确匹配（跨上下文最稳）
        arg0=$(proc_arg0 "$pid" || true)
        if [[ -n $arg0 && $arg0 == "$target" ]]; then printf '%s\n' "$pid"; continue; fi
        # ② /proc/PID/exe（部分上下文不可读，读不到静默跳过）
        exe=$(readlink "$d/exe" 2>/dev/null) || exe=""
        if [[ $exe == "$target" ]]; then printf '%s\n' "$pid"; continue; fi
        # ③ 兜底：Core 的直接子进程 + comm 匹配（stat 始终可读）
        if [[ -n $core_pid && "$(proc_ppid "$pid")" == "$core_pid" && "$(proc_comm "$pid")" == "$base" ]]; then
            printf '%s\n' "$pid"
        fi
    done
}

wait_plugin_process() { # 20s 窗口内出现「新版本」进程（starttime ≥ 目标 mtime）且两次采样稳定
    local target=$1 mtime deadline pid first="" cand start
    mtime=$(stat -c %Y "$target" 2>/dev/null || echo 0)
    deadline=$((SECONDS + 20))
    while (( SECONDS < deadline )); do
        pid=""
        for cand in $(plugin_pids "$target"); do
            start=$(proc_start_epoch "$cand" 2>/dev/null || true)
            # 容差 2s：starttime 按 tick 整除 + btime 截断，最多损失 ≈1s
            if [[ -n $start ]] && (( start + 2 >= mtime )); then
                pid=$cand
                break
            fi
        done
        if [[ -n $pid ]]; then
            if [[ $pid == "$first" ]]; then
                printf '%s' "$pid"
                return 0
            fi
            first=$pid
        else
            first=""
        fi
        sleep 1
    done
    return 1
}

qq_connected() {
    command -v ss >/dev/null 2>&1 || return 1
    ss -Htn state established 2>/dev/null \
        | grep -qE ':(3131|31[4-9][0-9]|3[2-3][0-9][0-9])\b'
}

qq_enabled() { # 语义与 update.sh 一致：配置存在且未写 enabled=true ⇒ 视为关闭
    if [[ -f $CORE_CONFIG ]] && ! awk '
        /^\[adapters\.qq\]/{f=1;next}
        /^\[/{f=0}
        f && /^enabled[[:space:]]*=/ {gsub(/[[:space:]]/,""); if ($0=="enabled=true") found=1}
        END{exit !found}' "$CORE_CONFIG"; then
        return 1
    fi
    return 0
}

napcat_containers() {
    command -v docker >/dev/null 2>&1 || return 0
    docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^(napcat|echo-napcat-.+)$' || true
}

qq_recover_and_wait() {
    if ! qq_enabled; then
        echo "QQ 适配器未启用（[adapters.qq] enabled != true）—— 跳过 QQ 恢复等待"
        return 0
    fi
    local containers
    containers=$(napcat_containers)
    if [[ -z $containers ]]; then
        echo "未发现运行中的 NapCat 容器 —— 跳过 QQ 恢复等待"
        return 0
    fi
    echo "等待 NapCat 自动重连（reconnectInterval≈5s，窗口 40s）…"
    local i container
    for ((i = 0; i < 40; i++)); do
        if qq_connected; then echo "QQ 反向 WS 已连接"; return 0; fi
        sleep 1
    done
    warn "40s 内 NapCat 未自动重连，执行 docker restart 兜底（登录态保存在卷中，会自动恢复）"
    for container in $containers; do
        docker restart "$container" >/dev/null 2>&1 || true
    done
    for ((i = 0; i < 90; i++)); do
        if qq_connected; then echo "QQ 反向 WS 已连接（容器重启后恢复）"; return 0; fi
        sleep 1
    done
    warn "QQ 反向 WS 仍未连接；请检查：docker ps / journalctl --user -u $CORE_SERVICE"
    return 1
}

atomic_install() { # $1=源 $2=目标（同目录临时文件 + 原子 rename）
    local src=$1 dst=$2 tmp
    tmp=$(mktemp "${dst}.new.XXXXXX")
    install -m 755 "$src" "$tmp"
    mv -f "$tmp" "$dst"
}

short_sha() {
    local h
    h=$(sha256sum "$1" 2>/dev/null | cut -c1-12 || true)
    printf '%s' "${h:-unknown}"
}

file_size() {
    local s
    s=$(du -h "$1" 2>/dev/null | cut -f1 || true)
    printf '%s' "${s:-?}"
}

# ── 只读健康检查 ─────────────────────────────────────────────────────────────
do_check() {
    step "部署状态检查"
    echo "目标: $TARGET"
    local healthy=0

    if [[ -f $TARGET ]]; then
        echo "  安装:     存在（$(file_size "$TARGET")，sha256:$(short_sha "$TARGET")，mtime: $(date -r "$TARGET" '+%F %T' 2>/dev/null || echo '?')）"
    else
        echo "  安装:     不存在"
        healthy=1
    fi

    if [[ -f $BINARY ]]; then
        if [[ -f $TARGET && "$(short_sha "$BINARY")" == "$(short_sha "$TARGET")" ]]; then
            echo "  仓库产物: 存在（与安装一致）"
        else
            echo "  仓库产物: 存在（与安装不一致）"
        fi
    else
        echo "  仓库产物: 未构建（$BINARY）"
    fi

    if service_known; then
        if service_active; then
            echo "  Core:     active（PID $(core_main_pid)）"
        else
            echo "  Core:     inactive"
            healthy=1
        fi
    else
        echo "  Core:     未找到 systemd user 服务（$CORE_SERVICE）"
        healthy=1
    fi

    # 进程识别（三重识别见 plugin_pids）；按「启动时刻 vs 目标文件 mtime」
    # 区分 新版运行中 / 旧版未重启。
    local cand mtime_now start fresh="" stale=""
    mtime_now=$(stat -c %Y "$TARGET" 2>/dev/null || echo 0)
    for cand in $(plugin_pids "$TARGET"); do
        start=$(proc_start_epoch "$cand" 2>/dev/null || true)
        # 容差 2s（同 wait_plugin_process：starttime 截断最多损失 ≈1s）
        if [[ -n $start ]] && (( start + 2 >= mtime_now )); then
            fresh+="$cand "
        else
            stale+="$cand "
        fi
    done
    if [[ -n ${fresh// /} ]]; then
        local p ppids=""
        for p in $fresh; do
            ppids+="$p(PPID=$(proc_ppid "$p")) "
        done
        echo "  插件进程: $ppids"
    elif [[ -n ${stale// /} ]]; then
        echo "  插件进程: 旧版本仍在运行（PID $stale；二进制已替换，重启后生效）"
        healthy=1
    else
        echo "  插件进程: 未发现"
        healthy=1
    fi
    if ! qq_enabled; then
        echo "  QQ:       未启用"
    elif qq_connected; then
        echo "  QQ:       反向 WS 已连接"
    else
        echo "  QQ:       未连接"
    fi

    step "结论"
    if (( healthy == 0 )); then
        echo "健康"
        return 0
    fi
    echo "存在异常项（见上）"
    return 1
}

# ── 失败回滚（仅在确有 .rollback 备份时动作） ────────────────────────────────
rollback_after_failure() { # $1=target $2=原因
    local target=$1 backup="${1}.rollback"
    warn "$2"
    if [[ -x $backup ]]; then
        warn "自动回滚到上一版本并重启…"
        atomic_install "$backup" "$target" && rm -f "$backup" || true
        if restart_core_and_wait && wait_plugin_process "$target" >/dev/null; then
            warn "已回滚，Core 与插件已恢复运行。请检查新版本问题后重试部署"
        else
            warn "已回滚，但 Core / 插件未确认恢复 —— 请手动检查（journalctl --user -u $CORE_SERVICE）"
        fi
    else
        warn "无回滚备份（${backup} 不存在）—— 请手动检查与修复"
    fi
}

# ── 部署后的重启 + 验证链（成功后打印总结；失败时自动回滚并退出 1） ─────────
deploy_restart_and_verify() {
    local target=$1

    if ! service_known; then
        warn "未找到 systemd user 服务（$CORE_SERVICE）—— 跳过重启；二进制已就位，下次 Core 启动后生效"
        return 0
    fi
    if ! service_active; then
        echo "Core 服务当前未运行 —— 跳过重启；二进制已就位，下次 Core 启动后生效"
        return 0
    fi
    if running_in_core_cgroup; then
        warn "脚本运行在 Core 服务 cgroup 内：restart 会连带终止本脚本（验证与自动回滚不会执行）"
        warn "建议在独立终端运行，或用 systemd-run --user --scope 脱出 cgroup"
    fi

    local t0=$SECONDS
    if ! restart_core_and_wait; then
        rollback_after_failure "$target" "Core 重启后 30s 内未恢复 active"
        exit 1
    fi

    local pid
    if ! pid=$(wait_plugin_process "$target"); then
        rollback_after_failure "$target" "20s 内未检测到稳定的新插件进程"
        exit 1
    fi
    local ppid
    ppid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
    echo "插件进程已就绪（PID $pid，父进程 $ppid）"

    local qq_rc=0
    qq_recover_and_wait || qq_rc=1

    step "部署完成"
    echo "二进制: $target（$(file_size "$target")，sha256:$(short_sha "$target")）"
    echo "Core:   active（PID $(core_main_pid)）"
    echo "插件:   PID $pid"
    if (( qq_rc == 0 )); then
        echo "QQ:     反向 WS 已连接"
    else
        echo "QQ:     未连接（见上方警告）"
    fi
    echo "用时:   $((SECONDS - t0))s"
    if [[ -f "${target}.rollback" ]]; then
        echo "回滚:   scripts/deploy.sh --rollback（备份：${target}.rollback）"
    fi

    if (( qq_rc != 0 )); then
        warn "部署完成，但 QQ 反向 WS 未连接；请 journalctl 检查，必要时 scripts/deploy.sh --rollback"
        exit 1
    fi
    return 0
}

# ── 回滚模式 ─────────────────────────────────────────────────────────────────
do_rollback() {
    local backup="${TARGET}.rollback"
    step "回滚插件二进制"
    [[ -x $backup ]] || die "未找到回滚备份：$backup（无可回滚的上一版本）"
    echo "备份: $backup（$(file_size "$backup")，sha256:$(short_sha "$backup")）"
    if [[ -f $TARGET ]]; then
        echo "当前: $TARGET（$(file_size "$TARGET")，sha256:$(short_sha "$TARGET")）"
    fi
    install -d -m 755 "$(dirname -- "$TARGET")"
    atomic_install "$backup" "$TARGET"
    rm -f "$backup"
    echo "已回滚到上一版本（备份已消耗）"
    if (( OPT_RESTART )); then
        deploy_restart_and_verify "$TARGET"
    else
        echo "已跳过重启（--no-restart）：下次 Core 启动后生效"
    fi
}

# ── 部署模式 ─────────────────────────────────────────────────────────────────
do_deploy() {
    echo "== echo-adapter-qq 部署 =="
    echo "仓库: $REPO_ROOT"
    echo "目标: $TARGET"

    # 1) 构建
    if (( OPT_BUILD )); then
        step "构建 release 二进制"
        command -v cargo >/dev/null 2>&1 || die "未找到 cargo（请先安装 Rust 工具链）"
        (cd "$REPO_ROOT" && cargo build --release -p echo-adapter-qq)
    fi
    [[ -x $BINARY ]] || die "构建产物不存在或不可执行：$BINARY（先运行不带 --no-build，或检查 --binary）"

    # 2) 冒烟（stdin 关闭应立即干净退出；仅提示，不阻断）
    step "安装插件二进制"
    local smoke_rc=0
    timeout 5 "$BINARY" </dev/null >/dev/null 2>&1 || smoke_rc=$?
    if (( smoke_rc == 0 )); then
        echo "冒烟通过（stdin 关闭干净退出）"
    else
        warn "冒烟异常（exit=$smoke_rc，124=超时）；请留意，继续部署"
    fi

    local old_sha=""
    if [[ -f $TARGET ]]; then
        old_sha=$(short_sha "$TARGET")
        echo "旧版本: $(file_size "$TARGET")，sha256:$old_sha"
    fi
    local new_sha
    new_sha=$(short_sha "$BINARY")
    echo "新版本: $(file_size "$BINARY")，sha256:$new_sha"
    if [[ -n $old_sha && $old_sha == "$new_sha" ]]; then
        echo "（内容与旧版本一致——可能未发生代码变更）"
    fi

    # 3) 安装（备份 + 原子替换）
    install -d -m 755 "$(dirname -- "$TARGET")"
    if [[ -e $TARGET ]]; then
        atomic_install "$TARGET" "${TARGET}.rollback"
        echo "已保留回滚备份：${TARGET}.rollback"
    fi
    atomic_install "$BINARY" "$TARGET"
    echo "已安装: $TARGET"

    # 4) 重启 + 验证
    if (( OPT_RESTART )); then
        deploy_restart_and_verify "$TARGET"
    else
        echo "已跳过重启（--no-restart）：下次 Core 启动后生效"
    fi
}

# ── 入口 ─────────────────────────────────────────────────────────────────────
if [[ $MODE == check ]]; then
    do_check
    exit $?
fi

# 部署 / 回滚都会重启 Core：与 Core 自更新共用锁，避免并发重启
install -d -m 755 "$CORE_STATE_DIR"
exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
    flock -n 9 || die 2 "另一个更新 / 部署正在进行（$LOCK_FILE 被占用），稍后重试"
fi

if [[ $MODE == rollback ]]; then
    do_rollback
else
    do_deploy
fi
