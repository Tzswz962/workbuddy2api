#!/bin/bash
# ===========================================================================
# workbuddy2api 看门狗（Linux / 宝塔面板）
# ---------------------------------------------------------------------------
# 作用：服务不健康时自动拉起，避免「宕机后一直等人手动重启」。
#
# 为什么需要它（真实事故，不是假想）：
#   2026-09-29 03:36 系统 dnf-makecache 刷元数据触发**全机 OOM**，
#   内核杀掉了 uvicorn；而 main.py 的设计是「子进程死 → 整体退出
#   （避免孤儿进程）」，于是整个服务停摆。
#   看门狗**确实被 cron 每 2 分钟触发了**，检测逻辑也没问题，
#   却连续 113 次全部失败在最后一步：
#
#       line 86: runuser: command not found
#
#   根因：脚本没声明 PATH，而 cron 给的 PATH 极简
#   （实测 /usr/local/bin:/usr/bin），**不含 /usr/sbin**；
#   runuser 恰恰在 /usr/sbin/runuser。
#   结果：停机 3.5 小时（03:38 → 07:05）只能靠人工重启。
#   2026-09-26 也有同样 8 次失败 —— 说明看门狗从上线起就没生效过。
#
# 本脚本已修掉该问题，并额外做了：
#   1. 显式 PATH + runuser 绝对路径（双保险）
#   2. OOM 保护：把服务调成「最后才被杀」
#   3. flock 防 cron 重叠
#   4. 健康判定用**真实 HTTP**，而不是「进程存在」
#      （进程活着但事件循环卡死时，nginx 侧同样 502/504）
#
# 安装（root）：
#   install -m 755 scripts/watchdog.sh \
#       /www/server/python_project/vhost/scripts/workbuddy2api_watchdog.sh
#   crontab -e      # 追加一行：
#   */2 * * * * /www/server/python_project/vhost/scripts/workbuddy2api_watchdog.sh >/dev/null 2>&1
#
# 装完**务必实测**（否则等于没装）：
#   pkill -f 'admin.server:app'; pkill -f 'python main.py'   # 模拟宕机
#   bash /www/server/python_project/vhost/scripts/workbuddy2api_watchdog.sh
#   curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8790/admin   # 应为 200
#
# 所有路径都可用环境变量覆盖（见下方 ${WB_*} 默认值），
# 换机器 / 换 Python 版本时不必改脚本正文。
# ===========================================================================
set -u

# ---------------------------------------------------------------------------
# 显式声明 PATH —— **本脚本最要紧的一行**（原因见文件头）。
# 无论从 cron、systemd 还是交互式 shell 调用，都能找到 runuser / curl / ss。
# ---------------------------------------------------------------------------
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# ------------------------------ 可配置项 -----------------------------------
PROJ=${WB_PROJ:-/www/wwwroot/workbuddy2api}
PY=${WB_PY:-/www/server/pyporject_evn/versions/3.10.20/bin/python}
RUN_USER=${WB_RUN_USER:-www}          # 服务以谁的身份跑（不要用 root）
PORT=${WB_PORT:-8790}
LOG=${WB_LOG:-/www/wwwlogs/python/workbuddy2api/watchdog.log}
PIDFILE=${WB_PIDFILE:-/www/server/python_project/vhost/pids/workbuddy2api.pid}
LOCK=${WB_LOCK:-/tmp/workbuddy2api-watchdog.lock}
ERRLOG=${WB_ERRLOG:-/www/wwwlogs/python/workbuddy2api/error.log}
OOM_ADJ=${WB_OOM_ADJ:--500}           # 负值 = OOM 时最后才被杀（需 root）

# runuser 解析成绝对路径，避免再受 PATH 影响（双保险）。
RUNUSER=$(command -v runuser 2>/dev/null || true)
[ -n "${RUNUSER:-}" ] || RUNUSER=/usr/sbin/runuser

# Python 解释器兜底：默认路径不存在时自动找一个，别让脚本白跑。
if [ ! -x "$PY" ]; then
  for c in "$PROJ/.venv/bin/python3" /www/server/pyporject_evn/versions/*/bin/python \
           "$(command -v python3 2>/dev/null)"; do
    [ -x "${c:-}" ] && PY="$c" && break
  done
fi

mkdir -p "$(dirname "$LOG")" "$(dirname "$PIDFILE")" 2>/dev/null
touch "$LOG" 2>/dev/null

log() { echo "[$(date '+%F %T')] $*" >> "$LOG"; }

# 防止上一轮还没跑完就又起一轮（cron 重叠）
exec 9>"$LOCK"
if ! flock -n 9; then
  exit 0
fi

# ------------------------------ 健康判定 -----------------------------------
# 用真实 HTTP 而不是「进程存在」：进程活着但卡死时 nginx 侧同样是 502。
# --max-time 要大于正常 TTFB（实测 1~4s），避免误判为不健康而反复重启。
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 \
       "http://127.0.0.1:${PORT}/admin" 2>/dev/null || echo 000)
if [ "$code" = "200" ]; then
  exit 0   # 健康，什么都不做
fi

log "健康检查失败（HTTP $code），准备重启服务"

# 记录现场，便于事后排查
{
  echo "--- 进程 ---"
  ps -ef | grep -E "${PROJ}|uvicorn admin\.server" | grep -v grep
  echo "--- 端口 ---"
  ss -lntp "sport = :${PORT}" 2>/dev/null
} >> "$LOG" 2>&1

# ------------------------------ 停掉旧进程 ---------------------------------
# 只按「本项目」特征匹配，避免误杀同机其它 python 服务。
kill_pid() {
  local p="$1"
  [ -n "${p:-}" ] || return 0
  kill -0 "$p" 2>/dev/null || return 0
  kill "$p" 2>/dev/null
  sleep 2
  kill -9 "$p" 2>/dev/null
}

if [ -f "$PIDFILE" ]; then
  kill_pid "$(cat "$PIDFILE" 2>/dev/null | tr -dc '0-9')"
fi
# 精确匹配：uvicorn 带本端口
for p in $(pgrep -f "uvicorn admin\.server:app .*--port ${PORT}" 2>/dev/null); do
  kill_pid "$p"
done
# main.py：仅当工作目录属于本项目时才杀
for p in $(pgrep -f "python .*main\.py" 2>/dev/null); do
  if [ "$(readlink -f /proc/$p/cwd 2>/dev/null)" = "$PROJ" ]; then
    kill_pid "$p"
  fi
done
sleep 2

# ------------------------------ 拉起 ---------------------------------------
# 与宝塔 Python 项目相同的方式/用户/日志路径。
# 用 runuser 降权到 www：服务不需要 root，也不该有 root 权限
# （最初版本由 root cron 直接拉起，服务以 root 运行 —— 一旦被攻破就是整机沦陷）。
cd "$PROJ" || { log "拉起失败：项目目录不存在 $PROJ"; exit 1; }
chown -R "$RUN_USER:$RUN_USER" "$PROJ/logs" "$(dirname "$LOG")" 2>/dev/null
chown "$RUN_USER:$RUN_USER" "$PROJ/.env" 2>/dev/null

"$RUNUSER" -u "$RUN_USER" -- bash -c \
  "cd '$PROJ' && nohup '$PY' main.py >> '$ERRLOG' 2>&1 & echo \$!" \
  > /tmp/wb_wd_pid 2>/tmp/wb_wd_err

NEW=$(tr -dc '0-9' < /tmp/wb_wd_pid 2>/dev/null)
if [ -n "${NEW:-}" ]; then
  echo "$NEW" > "$PIDFILE"
  chown "$RUN_USER:$RUN_USER" "$PIDFILE" 2>/dev/null
  log "已拉起新进程 pid=$NEW (user=$RUN_USER)"

  # ---------------------- OOM 保护 -----------------------------------------
  # 本机内存常年吃紧，任何内存尖峰都会触发 OOM；这里把服务调成
  # 「最后才被杀」。只有 root 能把 oom_score_adj 设成负值，
  # 所以只能在这个（root）脚本里做 —— main.py 自己无权设置。
  #
  # 必须**反复扫描整棵进程树**，不能只扫一次：
  # main.py 约 1 秒后才 fork 出 uvicorn，而 oom_score_adj 是
  # **在 fork 那一刻继承**的 —— 只设一次的话，后来才出现的 uvicorn
  # 仍是 0，真正的内存大户反而不受保护（这个坑实测踩过）。
  if [ "$OOM_ADJ" -lt 0 ] 2>/dev/null; then
    for _ in $(seq 1 10); do
      for p in $(pgrep -f "python .*main\.py|uvicorn admin\.server:app" 2>/dev/null); do
        # 只动本项目自己的进程：工作目录必须是 $PROJ
        if [ "$(readlink -f /proc/$p/cwd 2>/dev/null)" = "$PROJ" ]; then
          cur=$(cat /proc/$p/oom_score_adj 2>/dev/null || echo "")
          if [ "$cur" != "$OOM_ADJ" ] && echo "$OOM_ADJ" > "/proc/$p/oom_score_adj" 2>/dev/null; then
            log "已为 pid=$p 设置 oom_score_adj=$OOM_ADJ（OOM 时最后才被杀）"
          fi
        fi
      done
      sleep 1
    done
  fi
else
  log "拉起失败：$(cat /tmp/wb_wd_err 2>/dev/null | tail -3)"
fi

# ------------------------------ 确认恢复 -----------------------------------
for i in $(seq 1 20); do
  sleep 3
  c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
      "http://127.0.0.1:${PORT}/admin" 2>/dev/null || echo 000)
  if [ "$c" = "200" ]; then
    log "重启成功（第 ${i} 次探测，HTTP 200）"
    exit 0
  fi
done
log "重启后仍未恢复，请人工检查"
exit 1
