"""轻量后台定时任务调度器（无第三方依赖）。

用守护线程每 15s 轮询 schedules 表，到点（next_run_at <= now）的任务就执行，
执行完更新 last_run_at / next_run_at / last_result。

支持的任务：
  - refresh_balances：遍历 active 账号刷新余额（统计平台总积分）
  - sync_models：从后端拉取最新模型列表并 upsert 倍率
"""
import json
import threading
import time
from datetime import datetime, timedelta

from admin.db import SessionLocal
from admin.models import Schedule
from admin.config import settings

#: 账号间保活节流间隔（秒），从配置读取，缺省 0.8。
_KEEPALIVE_ACCOUNT_GAP = settings.KEEPALIVE_ACCOUNT_GAP

#: last_result 是 TEXT，存得下完整 JSON；但仍给一个上限，
#: 防止某个任务的明细异常膨胀把这一行撑爆。
_RESULT_LIMIT = 20000


def _account_label(a) -> str:
    """给账号起个**人认得出**的短标签。

    后台只显示 `acc12` 的话，用户看到「acc12 没领成功」还得自己去号池里
    对 id，等于没说。优先用账号名（多数是手机号或昵称），没有才退回 id。
    手机号做脱敏：中间四位打码，既够辨认又不把完整号码写进结果里
    （结果会被截图、也会随日志流转）。
    """
    name = str(getattr(a, "name", "") or "").strip()
    if name:
        if len(name) == 11 and name.isdigit():
            return f"{name[:3]}****{name[-4:]}"
        return name[:20]
    return f"账号#{getattr(a, 'id', '?')}"


def _short_error(e) -> str:
    """把异常压成一句可读原因（去掉类名前缀与多余空白）。"""
    msg = str(e).strip() or e.__class__.__name__
    return msg[:160]


def _dump_result(result: dict) -> str:
    """序列化任务结果。

    这里**绝不能截断 JSON 字符串**：以前是 `json.dumps(...)[:500]`，
    一截断就成了非法 JSON，前端 `JSON.parse` 直接失败，用户看到的是
    一段被腰斩的原始文本 —— 正是「别只显示一堆 json 数据」的成因之一。
    超长时改为丢弃明细数组里的项，保住 JSON 结构完整。
    """
    try:
        text = json.dumps(result, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps({"ok": False, "error": "结果无法序列化"},
                          ensure_ascii=False)
    if len(text) <= _RESULT_LIMIT:
        return text
    # 超长：先砍 detail（逐账号明细），保留计数与 summary
    slim = dict(result)
    detail = slim.get("detail")
    if isinstance(detail, list):
        for keep in (50, 20, 5, 0):
            slim["detail"] = detail[:keep]
            slim["detail_truncated"] = True
            text = json.dumps(slim, ensure_ascii=False)
            if len(text) <= _RESULT_LIMIT:
                return text
    slim.pop("detail", None)
    text = json.dumps(slim, ensure_ascii=False)
    return text if len(text) <= _RESULT_LIMIT else text[:_RESULT_LIMIT - 40] + '"}'



def run_task(task: str, db, schedule: "Schedule | None" = None) -> dict:
    """执行某个任务，返回结果摘要字典。"""
    if task == "refresh_balances":
        from admin.routers import accounts as acc_router
        from admin.models import Account
        ok = fail = 0
        failed_names: list[str] = []
        for a in db.query(Account).filter(Account.status == "active").all():
            # with_expiry=True：顺带统计「快过期积分」。
            # 这个字段是选号策略（oldest / weighted）的依据 —— 之前一直是 0，
            # 导致「先用快过期的额度」这条规则实际上从未生效。
            if acc_router._refresh_balance(a, with_expiry=True):
                ok += 1
            else:
                fail += 1
                failed_names.append(_account_label(a))
            db.commit()
        expiring = sum(int(a.credits_expiring or 0)
                       for a in db.query(Account).filter(
                           Account.status == "active").all())
        return {"task": task, "ok": True, "refreshed": ok, "failed": fail,
                "credits_expiring": expiring,
                "failed_accounts": failed_names[:10],
                "summary": (f"刷新成功 {ok} 个账号"
                            + (f"，快过期积分 {expiring}" if expiring else "")
                            + (f"，{fail} 个失败：{'、'.join(failed_names[:3])}"
                               if fail else ""))}
    if task == "sync_models":
        from admin.routers import models as models_router
        res = models_router._do_sync_models(db)
        if isinstance(res, dict) and "summary" not in res:
            res["summary"] = (f"新增 {res.get('added', 0)} 个、"
                              f"更新 {res.get('updated', 0)} 个，"
                              f"共 {res.get('total_in_db', 0)} 个模型")
        return res
    if task == "daily_checkin":
        return run_daily_checkin(db, schedule)
    if task == "refresh_growth_tasks":
        return run_refresh_growth_tasks()
    if task == "run_growth_tasks":
        return run_growth_tasks()
    if task == "keepalive_tokens":
        return run_keepalive_tokens(db)
    return {"task": task, "error": "未知任务类型"}


#: token 保活：连续多少次「session 已死」才把账号禁用。
#: 与 proxy 的 SESSION_DEAD_THRESHOLD 同源 —— 一次 12153 只说明这次刷新失败，
#: 可能是上游抖动；连续失败才说明这个号的登录态真的废了，需要人工重登。
_KEEPALIVE_DEAD_THRESHOLD = 3


def run_keepalive_tokens(db) -> dict:
    """定时刷新所有活跃账号的 token，保持登录态存活（「保活」）。

    为什么需要：上游的登录态有绝对有效期。如果一个账号长期没有请求，它的
    refresh token 会在某天静默失效 —— 等到真有人来用，才在第一次请求时
    发现要重登。用户感知就是「号池里明明有余额的号，用的时候报错」。

    这也正是参考实现 internal/scheduler/scheduler.go 的 KeepaliveHours
    （默认 [22]，即每晚 22 点）在做的事：主动 refresh 一遍所有 token。

    实现要点（对齐参考实现，也贴合上游真实行为）：
      * **串行 + 节流**：账号之间有 KEEPALIVE_ACCOUNT_GAP 秒间隔。批量并发地
        刷新 token 是一个很明显的机器特征，节流后与真人逐个使用的节奏接近。
      * **只刷新不调用**：仅走 token 刷新路径（`get_headers()`），不发起对话。
        这样不消耗积分、不产生对话记录，纯保活。
      * **12153 连续计数**：刷新抛「session 已死」时累加 `session_dead_fails`；
        达到阈值才禁用账号。一次就禁用会造成误杀（上游抖动/并发刷新都会临时触发）。
      * **成功清零**：刷新成功即把 `session_dead_fails` 归零。

    返回结果摘要，写入 schedules.last_result 供后台查看。
    """
    from admin.backend import AccountSession
    from admin.models import Account

    accounts = (db.query(Account).filter(Account.status == "active")
                .order_by(Account.id.asc()).all())
    if not accounts:
        return {"task": "keepalive_tokens", "ok": True, "total": 0, "msg": "没有活跃账号"}

    total = len(accounts)
    ok = 0
    refreshed = 0
    dead_disabled = 0
    failed = 0
    errors: list[str] = []

    for idx, a in enumerate(accounts):
        # 账号间节流：批量并发刷新 token 是明显的机器特征
        if idx and _KEEPALIVE_ACCOUNT_GAP > 0:
            time.sleep(_KEEPALIVE_ACCOUNT_GAP)
        sess = None
        try:
            sess = AccountSession(a.auth_json)
            before = getattr(sess, "token", None)
            headers = sess.get_headers()  # 内部按需触发 token 刷新
            after = getattr(sess, "token", None)
            if after and after != before:
                refreshed += 1
            del headers
            ok += 1
            # 刷新成功 → 清零 session-dead 连续计数
            if a.session_dead_fails:
                a.session_dead_fails = 0
            a.last_err_at = None
            db.commit()
        except Exception as e:
            msg = str(e)
            from admin.routers.proxy import _classify_error
            kind = _classify_error(0, msg)
            if kind in ("session_dead", "transport"):
                # 「刷新失败」基本等价于登录态可能已失效，但必须连续计数后才禁用
                a.session_dead_fails = (a.session_dead_fails or 0) + 1
                a.cool_kind = "session_dead"
                a.last_err_at = datetime.utcnow()
                a.last_err_msg = (msg or "keepalive failed")[:255]
                if a.session_dead_fails >= _KEEPALIVE_DEAD_THRESHOLD:
                    a.status = "disabled"
                    dead_disabled += 1
                db.commit()
                failed += 1
                if len(errors) < 5:
                    errors.append(f"#{a.id}: {msg[:120]}")
            else:
                failed += 1
                if len(errors) < 5:
                    errors.append(f"#{a.id}: {msg[:120]}")
        finally:
            if sess is not None:
                try:
                    sess.close()
                except Exception:
                    pass

    summary = f"保活成功 {ok}/{total}"
    if refreshed:
        summary += f"，其中 {refreshed} 个刷新了 token"
    if dead_disabled:
        summary += f"，{dead_disabled} 个登录态失效已禁用"
    if failed and not dead_disabled:
        summary += f"，{failed} 个失败"
    return {"task": "keepalive_tokens", "ok": True, "total": total,
            "alive": ok, "refreshed": refreshed, "failed": failed,
            "disabled": dead_disabled, "errors": errors,
            "summary": summary}



#: 执行成长任务前，若列表刚刷新过不足这个秒数，则等待补足。
#: 上游任务定义与账号进度之间存在同步延迟，刷新列表后立刻执行会拿到
#: 旧的任务定义/进度，导致「新任务没被识别」或「重复触发」。定时任务里
#: 把本任务排在刷新之后几分钟，就是这个原因。
_GROWTH_MIN_AFTER_REFRESH = 180


def run_growth_tasks() -> dict:
    """自动完成号池内所有账号可自动化的成长任务，并领取奖励。

    与后台「批量做任务」按钮走的是同一套逻辑（growth 路由的 run_accounts），
    因此串行执行、节流规则与「执行后自动领取」的行为完全一致。

    过滤规则（与手动执行一致）：
      - 只处理策略表里 actionable 的任务（其余为需客户端完成，跳过）
      - 已 claimed 的跳过，保证幂等，重复跑无副作用
      - 单次失败只记录、不重试，避免对注定失败的任务反复发请求
    """
    from admin.models import Account
    from admin.routers import growth as growth_router

    started = datetime.utcnow()
    db = SessionLocal()
    try:
        # 1) 先刷新任务定义，保证本轮基于最新的任务列表与进度
        try:
            growth_router.growth_tasks(refresh=True, db=db)
        except Exception:
            pass  # 刷新失败不阻断执行（可能只是网络抖动，用旧缓存继续）

        accounts = (db.query(Account)
                    .filter(Account.status == "active")
                    .order_by(Account.id.asc()).all())
        ids = [a.id for a in accounts]
        if not ids:
            return {"task": "run_growth_tasks", "ok": True, "accounts": 0,
                    "msg": "没有可用账号"}

        run_res = growth_router.run_accounts(
            ids, None, db,
            # 整批预算：定时任务在**调度线程**里同步跑，跑多久占多久。
            # 没有这个上限时，23 个账号最坏能占住调度线程一个多小时，
            # 期间整点刷新余额 / 签到 / token 保活全部停摆
            # （表现为「定时任务卡住了」）。超出预算的账号如实记录、留待下次。
            soft_budget=growth_router._SCHEDULE_SOFT_BUDGET)
    except Exception as e:
        return {"task": "run_growth_tasks", "ok": False, "error": str(e)}
    finally:
        db.close()

    # 汇总（run_accounts 内部已逐个账号执行 + 领取，无需再单独 claim 一遍）
    results = run_res.get("results") or []
    credit = sum((r.get("credit") or 0) for r in results)
    energy = sum((r.get("energy") or 0) for r in results)
    fired = sum(1 for a in results
                for t in (a.get("tasks") or []) if t.get("ok") and not t.get("skipped"))
    failed_acc = [a["account_id"] for a in results if not a.get("ok")]

    # 逐账号明细：失败原因同样要落到具体账号上，而不是只给一个数字
    labels = {a.id: _account_label(a) for a in accounts}
    detail = []
    for r in results:
        aid = r.get("account_id")
        label = labels.get(aid, f"账号#{aid}")
        credit_i = int(r.get("credit") or 0)
        n_done = sum(1 for t in (r.get("tasks") or [])
                     if t.get("ok") and not t.get("skipped"))
        if r.get("ok"):
            detail.append({"account_id": aid, "account": label,
                           "ok": True, "credit": credit_i,
                           "tasks_done": n_done,
                           "reason": f"完成 {n_done} 个任务、+{credit_i} 积分"
                           if n_done else "无待做任务"})
        else:
            detail.append({"account_id": aid, "account": label,
                           "ok": False, "credit": credit_i, "tasks_done": n_done,
                           "reason": _short_error(r.get("error") or "未知原因")})

    result = {
        "task": "run_growth_tasks",
        "ok": True,
        "accounts": len(ids),
        "tasks_done": fired,
        "credit": credit,
        "energy": energy,
        "failed_accounts": failed_acc[:10],
        "elapsed_s": round((datetime.utcnow() - started).total_seconds(), 1),
        "detail": detail,
    }
    result["summary"] = _growth_summary(result)
    return result


def _growth_summary(r: dict) -> str:
    """成长任务结果的一句话摘要。"""
    parts = []
    done = r.get("tasks_done") or 0
    if done:
        parts.append(f"完成 {done} 个任务")
    if r.get("credit"):
        parts.append(f"+{r['credit']} 积分")
    if r.get("energy"):
        parts.append(f"+{r['energy']} 能量")
    if not done:
        parts.append("本轮无待做任务")
    bad = [d for d in (r.get("detail") or []) if not d.get("ok")]
    if bad:
        who = "；".join(f"{d['account']}（{d['reason']}）" for d in bad[:3])
        more = f" 等 {len(bad)} 个" if len(bad) > 3 else ""
        parts.append(f"{len(bad)} 个账号失败：{who}{more}")
    return "，".join(parts)



def run_refresh_growth_tasks() -> dict:
    """刷新成长任务定义缓存。

    任务定义对所有账号一致，所以只需一个可用登录态即可（不遍历账号，
    避免无谓的上游请求）。失败时保留旧缓存，不影响面板使用。
    """
    from admin.routers import growth as growth_router

    try:
        db = SessionLocal()
        try:
            data = growth_router.growth_tasks(refresh=True, db=db)
        finally:
            db.close()
    except Exception as e:
        return {"task": "refresh_growth_tasks", "ok": False, "error": str(e)}

    tasks = data.get("tasks") or []
    actionable = sum(1 for t in tasks if t.get("actionable"))
    return {
        "task": "refresh_growth_tasks",
        "ok": True,
        "total": len(tasks),
        "actionable": actionable,
        "synced_at": data.get("synced_at"),
        "source_account_id": data.get("source_account_id"),
        "summary": f"共 {len(tasks)} 个任务，其中 {actionable} 个可自动完成",
    }


def run_daily_checkin(db, schedule: "Schedule | None" = None) -> dict:
    """遍历活跃账号执行每日签到领取积分。

    风控要点：
      - 全部请求经 CredentialManager 注入 X-Device-Token（与桌面端一致）。
      - 若任务配置了 stop_after（下次停止领取时间），到达后直接跳过，不再发领取请求，
        避免活动下线后继续请求触发上游风控。
      - 若某账号领取返回 EventEnded(1003)，自动把 stop_after 设为今天，后续不再尝试。

    结果里除了计数，还逐账号记录「领到多少分 / 没领成功的原因」：
    只给一个 `failed: 3` 等于没说 —— 用户真正想知道的是**哪个号**失败了、**为什么**。
    """
    from admin.models import Account
    from admin.backend import AccountSession

    now = datetime.utcnow()

    # 停止领取时间：到达则跳过
    if schedule is not None and schedule.stop_after is not None:
        if now > schedule.stop_after:
            return {"task": "daily_checkin", "ok": True,
                    "skipped": "已超过停止领取时间，不再请求",
                    "stop_after": schedule.stop_after.isoformat()}

    accounts = (db.query(Account).filter(Account.status == "active")
                .order_by(Account.id.asc()).all())
    if not accounts:
        return {"task": "daily_checkin", "ok": True, "total": 0,
                "msg": "没有可用账号"}

    claimed = skipped_already = failed = 0
    total_credit = 0
    ended = False
    errors: list[str] = []          # 保留旧字段，兼容既有调用方
    detail: list[dict] = []         # 新增：逐账号明细（供后台展示）

    for a in accounts:
        label = _account_label(a)
        try:
            with AccountSession(a.auth_json) as sess:
                st = sess.get_checkin_status()
                if st.get("today_checked_in"):
                    skipped_already += 1
                    detail.append({"account_id": a.id, "account": label,
                                   "ok": True, "status": "already",
                                   "credit": 0, "reason": "今日已领过"})
                else:
                    res = sess.claim_daily_checkin()
                    if res.get("ok"):
                        credit = int(res.get("credit") or 0)
                        claimed += 1
                        total_credit += credit
                        detail.append({
                            "account_id": a.id, "account": label, "ok": True,
                            "status": "claimed", "credit": credit,
                            "streak_days": int(res.get("streak_days") or 0),
                            "reason": f"+{credit} 积分",
                        })
                    elif res.get("status") == "event_ended":
                        ended = True
                        failed += 1
                        reason = "活动已结束"
                        errors.append(f"{label}:{reason}")
                        detail.append({"account_id": a.id, "account": label,
                                       "ok": False, "status": "event_ended",
                                       "credit": 0, "reason": reason})
                    else:
                        failed += 1
                        reason = res.get("status") or res.get("msg") or "未知原因"
                        errors.append(f"{label}:{reason}")
                        detail.append({"account_id": a.id, "account": label,
                                       "ok": False, "status": "failed",
                                       "credit": 0, "reason": str(reason)})
                # 写回可能已刷新的 token（签到请求会触发鉴权头刷新）
                a.auth_json = sess.updated_json()
        except Exception as e:
            failed += 1
            reason = _short_error(e)
            errors.append(f"{label}:{reason}")
            detail.append({"account_id": a.id, "account": label, "ok": False,
                           "status": "error", "credit": 0, "reason": reason})

    # 发现活动已结束：自动把停止时间设为今天，防止后续继续请求
    if ended and schedule is not None:
        schedule.stop_after = now
        db.commit()

    result = {
        "task": "daily_checkin",
        "ok": True,
        "total": len(accounts),
        "claimed": claimed,
        "skipped_already": skipped_already,
        "failed": failed,
        "credit": total_credit,
        "activity_ended": ended,
        "errors": errors[:10],
        "detail": detail,
    }
    result["summary"] = _checkin_summary(result)
    return result


def _checkin_summary(r: dict) -> str:
    """把签到结果写成一句人话，后台直接显示这句。"""
    if r.get("skipped"):
        return f"已跳过：{r['skipped']}"
    if r.get("msg"):
        return r["msg"]
    parts = []
    claimed = r.get("claimed") or 0
    credit = r.get("credit") or 0
    if claimed:
        parts.append(f"今日新领 {claimed} 个账号，共 +{credit} 积分")
    already = r.get("skipped_already") or 0
    if already:
        parts.append(f"{already} 个今日已领过")
    failed = r.get("failed") or 0
    if failed:
        # 带上具体是哪个号、什么原因 —— 这才是用户要的信息
        bad = [d for d in (r.get("detail") or []) if not d.get("ok")]
        who = "；".join(f"{d['account']}（{d['reason']}）" for d in bad[:3])
        more = f" 等 {len(bad)} 个" if len(bad) > 3 else ""
        parts.append(f"{failed} 个未领成功：{who}{more}" if who
                     else f"{failed} 个未领成功")
    if r.get("activity_ended"):
        parts.append("活动已结束，已自动停止")
    return "，".join(parts) if parts else "无可领取账号"



def _run_one(s: Schedule, db, now: datetime):
    # 保活是「指定整点执行」而非「每 N 分钟执行」：若本轮不是保活整点，
    # 只顺延到下一个整点、不执行。这样避免每天多刷几次 token（无谓的请求
    # 本身就是可被观测的机器行为）。
    if s.task == "keepalive_tokens" and not _is_keepalive_hour(now):
        s.next_run_at = _next_keepalive_at(now)
        db.commit()
        return
    try:
        result = run_task(s.task, db, s)
        s.last_result = _dump_result(result)
    except Exception as e:  # 单个任务失败不影响调度循环
        s.last_result = json.dumps(
            {"ok": False, "error": _short_error(e)}, ensure_ascii=False)[:2000]
    s.last_run_at = now
    if s.task == "keepalive_tokens":
        s.next_run_at = _next_keepalive_at(now)
    else:
        s.next_run_at = now + timedelta(minutes=s.interval_minutes or 60)
    db.commit()


def _loop():
    while True:
        db = None
        try:
            db = SessionLocal()
            now = datetime.utcnow()
            for s in db.query(Schedule).filter(Schedule.enabled == 1).all():
                if s.next_run_at is None or s.next_run_at <= now:
                    _run_one(s, db, now)
        except Exception:
            # 数据库短暂不可用（如 MySQL 重启）时静默跳过本轮，下一轮再试。
            # 原先的写法在 `SessionLocal()` 本身抛异常时会引用未赋值的 db，
            # 让异常从 except 块里再次抛出并终止调度线程。
            pass
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass
        time.sleep(15)


def seed_defaults(db):
    """首次启动若无任何任务则写入默认任务（含每日签到）。"""
    if db.query(Schedule).count() == 0:
        now = datetime.utcnow()
        db.add(Schedule(name="整点刷新平台总积分", task="refresh_balances",
                        interval_minutes=60, enabled=1, next_run_at=now))
        db.add(Schedule(name="每日同步模型列表", task="sync_models",
                        interval_minutes=1440, enabled=1, next_run_at=now))
        db.add(Schedule(name="每日签到领取积分", task="daily_checkin",
                        interval_minutes=1440, enabled=1, next_run_at=now))
        db.add(Schedule(name="每日更新成长任务列表", task="refresh_growth_tasks",
                        interval_minutes=1440, enabled=1, next_run_at=now))
        db.add(Schedule(name="每日自动做成长任务", task="run_growth_tasks",
                        interval_minutes=1440, enabled=1,
                        next_run_at=now + timedelta(seconds=_GROWTH_MIN_AFTER_REFRESH)))
        db.commit()


def ensure_daily_checkin(db):
    """已存在其它任务但缺每日签到时，补一个默认签到任务（幂等）。

    保证「定期自动签到」在任意已运行实例上都有配置：今天已领的账号会被跳过，
    活动结束（EventEnded）时调度器自动把 stop_after 置为今天，不会误发请求触发风控。
    """
    if db.query(Schedule).filter(Schedule.task == "daily_checkin").count() == 0:
        now = datetime.utcnow()
        db.add(Schedule(name="每日签到领取积分", task="daily_checkin",
                        interval_minutes=1440, enabled=1, next_run_at=now))
        db.commit()


def ensure_growth_schedules(db):
    """补齐成长任务相关定时任务（幂等）。

    两个任务必须成对存在且有先后顺序：
      - refresh_growth_tasks：先刷新任务列表
      - run_growth_tasks：    几分钟后再执行
    执行任务排在后面是硬性要求，见 _GROWTH_MIN_AFTER_REFRESH 的说明。

    这里用 next_run_at 保证先后：刷新任务排在 now，执行任务排在 now+延迟；
    若执行任务已存在但时间早于刷新任务，直接顺延。
    """
    now = datetime.utcnow()
    changed = False

    refresh = (db.query(Schedule)
               .filter(Schedule.task == "refresh_growth_tasks").first())
    if refresh is None:
        refresh = Schedule(name="每日更新成长任务列表", task="refresh_growth_tasks",
                           interval_minutes=1440, enabled=1, next_run_at=now)
        db.add(refresh)
        db.flush()  # 拿到 id / 默认值
        changed = True

    run = db.query(Schedule).filter(Schedule.task == "run_growth_tasks").first()
    if run is None:
        base = refresh.next_run_at or now
        db.add(Schedule(name="每日自动做成长任务", task="run_growth_tasks",
                        interval_minutes=1440, enabled=1,
                        next_run_at=base + timedelta(seconds=_GROWTH_MIN_AFTER_REFRESH)))
        changed = True
    else:
        # 已存在则校正先后：执行必须晚于刷新
        base = refresh.next_run_at or now
        want = base + timedelta(seconds=_GROWTH_MIN_AFTER_REFRESH)
        if run.next_run_at is None or run.next_run_at < want:
            run.next_run_at = want
            changed = True

    if changed:
        db.commit()


def ensure_keepalive_schedule(db):
    """补齐 token 保活定时任务（幂等）。

    为什么用「每天的整点」而不是固定 interval：上游登录态的失效是按自然时间
    推进的，保活要在**没人用号的时候**做（夜里），这样既不影响白天请求，
    又保证第二天早上所有号都是热的。

    参考实现（internal/scheduler/scheduler.go）默认 KeepaliveHours=[22]，
    即每晚 22 点。我们沿用这个思路：默认 22 点，可用
    ADMIN_KEEPALIVE_HOURS 配置多个小时（逗号分隔），或设
    ADMIN_KEEPALIVE_ENABLED=0 关闭。

    这里把 interval_minutes 设为 1440（每天一次），并且只在当前小时命中
    列表时才真正执行 —— 见 _is_keepalive_hour。
    """
    if not settings.KEEPALIVE_ENABLED:
        # 显式关闭：若存在则禁用（不删除，保留后台可见与手动触发能力）
        row = db.query(Schedule).filter(Schedule.task == "keepalive_tokens").first()
        if row is not None and row.enabled:
            row.enabled = 0
            db.commit()
        return

    row = db.query(Schedule).filter(Schedule.task == "keepalive_tokens").first()
    if row is None:
        now = datetime.utcnow()
        target = _next_keepalive_at(now)
        db.add(Schedule(name="每日 token 保活刷新", task="keepalive_tokens",
                        interval_minutes=1440, enabled=1, next_run_at=target))
        db.commit()


def _next_keepalive_at(now: datetime) -> datetime:
    """返回下一个保活整点时刻。"""
    from datetime import time as _time
    hours = sorted(set(h for h in settings.KEEPALIVE_HOURS if 0 <= h <= 23)) or [22]
    for h in hours:
        cand = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if cand > now:
            return cand
    # 今天的都过了 → 明天第一个整点
    return (now + timedelta(days=1)).replace(
        hour=hours[0], minute=0, second=0, microsecond=0)


def _is_keepalive_hour(now: datetime) -> bool:
    """当前小时是否属于保活整点（容差 1 小时：调度器每 15s 轮询，
    若上一轮因数据库短暂不可用被跳过，下一轮仍应补上）。"""
    hours = set(settings.KEEPALIVE_HOURS)
    return now.hour in hours or (now.hour - 1) in hours


_scheduler_lock = threading.Lock()
_scheduler_started = False


def start_scheduler():
    """在 FastAPI 启动时调用：播种默认任务并拉起守护线程。

    幂等：重复调用只会有一个调度线程。uvicorn --reload 或多 worker 场景下
    会多次触发 startup，没有这个守卫就会起多个线程、把定时任务重复执行。
    """
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True
    try:
        db = SessionLocal()
        seed_defaults(db)
        ensure_daily_checkin(db)
        ensure_growth_schedules(db)
        ensure_keepalive_schedule(db)
        db.close()
    except Exception:
        pass
    t = threading.Thread(target=_loop, daemon=True, name="wb-scheduler")
    t.start()
