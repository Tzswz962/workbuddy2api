"""成长计划任务：列表 / 参与 / 完成 / 领奖。

设计要点
--------
* **任务列表不绑账号**：任意一个可用登录态即可拉取任务定义，结果缓存在
  内存里供前端与定时任务复用（任务定义对所有账号一致，仓库里只存"定义"）。
* **完成操作按 task_code 走策略表**（admin/growth_plans.py）：只对已实测
  可行的任务发包，其余标记为需人工，避免盲目请求污染上游日志。
* **串行 + 限速**：逐个账号、逐个任务执行，中间 sleep，避免触发风控。
"""
import time
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from admin import backend, growth_plans, jobrunner
from admin.config import settings
from admin.db import SessionLocal, get_db
from admin.models import Account
from admin.security import require_admin

# ⚠️ 必须整组挂 require_admin（安全审计「严重」项）。
# 历史实现的这个 router **一个鉴权依赖都没有**，于是：
#   * `/api/growth/accounts/{id}/tasks` 可以被匿名者从 id=1 递增枚举，
#     直接拿到全部账号的 uid 与**显示名**（实测泄露 180****7641 这类手机号）；
#   * `/api/growth/run-async`、`/run`、`/accept`、`/claim` 可以被匿名者直接调用，
#     在服务器上启动批量任务、真实消耗账号额度并写库 —— 不只是信息泄露，
#     是**有副作用**的未授权操作。
# 挂在 router 上而不是逐个接口，是为了杜绝「以后新增接口又忘了加」。
router = APIRouter(prefix="/api/growth", tags=["growth"],
                   dependencies=[Depends(require_admin)])

#: 任务定义缓存 {"tasks": [...], "synced_at": iso}
_task_cache: dict = {}

#: 单账号任务列表的短缓存（秒）。
#: 上游拉一次任务列表实测 1~3.3 秒，而前端「执行完 / 领奖完」都会立刻再拉一次；
#: 缓存一个很短的窗口能让连续刷新**秒回**，又不会让状态明显过时。
_ACCT_TASKS_TTL = 3.0
_acct_tasks_cache: dict = {}


def _invalidate_acct_tasks(account_ids) -> None:
    """清掉这些账号的任务缓存（做完任务 / 领完奖后必须调）。

    否则前端紧接着的刷新会拿到**旧的**状态，用户看到「刚做完却还显示未完成」。
    """
    for aid in (account_ids or []):
        _acct_tasks_cache.pop(aid, None)

#: 执行节流（秒）
_EVENT_GAP = 1.2          # 同一任务内两次触发之间
_ACCOUNT_GAP = 1.0        # 两个账号之间

#: 轮询参数：等上游落库时用「轮询到就绪」，而不是固定 sleep。
#: 超时只是兜底上限，正常情况远早于它返回。
_POLL_INTERVAL = 0.5      # 轮询间隔
_ACCEPT_TIMEOUT = 6.0     # 等 accept 参与状态落库
_VERIFY_TIMEOUT = 6.0     # 等触发后进度落库
#: 等「任务变成可领取」的上限（秒）。
#: 上游把任务标成 completed 有延迟，跑完立刻查经常还是 in_progress ——
#: 这正是以前「做完了但没自动领、要手动再点」的根因。
#: 一旦查到可领取就立刻领（不等满这个时长）。
_CLAIM_WAIT = 8.0

_MAX_TIMES = 10           # 单任务最多触发次数上限

#: 单个账号的总时间预算（秒）。
#:
#: 实测（15 个可自动化任务、上游每次约 3s）跑完一个账号要约 310s：
#:   chat_5 / expert_5 / template_5 这类 MULTI 任务本身就要触发 5 次，
#:   每次「触发 + 间隔」约 4.2s，单个任务就 20s+。
#: 原来给 45s —— 结果必然在做完 2~3 个任务后就报「本账号超时，剩余任务留待下次」，
#: 用户得反复点好几次才能做完，这就是体感上的「老是超时」。
#:
#: 现在给 300s（5 分钟）：
#:   * 够跑完一个账号的全部可自动化任务；
#:   * 仍保留上限，异常账号不会无限拖住整个批量（这是当初加预算的目的）；
#:   * 定时任务/异步任务走后台线程，不受 HTTP 超时约束，长一点没问题。
#:     同步接口（后台「执行」按钮）如果账号多，仍建议用 run-async。
_ACCOUNT_BUDGET = 300.0

#: 同步执行时的软预算：超过就**停止接新任务**（但不打断当前任务）。
#:
#: 与 `_ACCOUNT_BUDGET` 的区别：那个是「单账号硬上限」，这个是给
#: 同步接口的「别让整个请求超过 nginx 60s」用的。异步/定时任务不受它限制。
_SYNC_SOFT_BUDGET = 50.0

#: 定时任务整批的预算（秒）。
#:
#: 为什么必须有：定时任务是**在调度线程里同步跑**的，跑多久就占多久。
#: xx 个账号 × 单账号最长 300s，最坏能把调度线程占住 1 个多小时 ——
#: 期间整点刷新余额、签到、token 保活**全都不会执行**。
#: 这就是「定时任务会不会卡住」的答案：会，而且卡的是整个调度器。
#:
#: 25 分钟足够跑完「少数账号有新任务」的日常情况
#: （已完成的老号走快速通道，每个约 1 秒，不占时间）。
#: 跑不完的账号如实记录「留待下次」，第二天继续。
_SCHEDULE_SOFT_BUDGET = 1500.0

#: 触发事件时优先使用的免费模型（0 倍率），用完再退回低倍率
_PREFERRED_MODELS = ["hy3", "hunyuan-chat"]


def _pick_free_model(db: Session) -> str:
    """挑一个免费（0 倍率）模型，没有则退回传参默认值。

    任务记账不依赖模型输出质量，用免费模型可以把成本压到 0。
    """
    from admin.models import ModelConfig
    for mid in _PREFERRED_MODELS:
        row = (db.query(ModelConfig)
               .filter(ModelConfig.model_id == mid, ModelConfig.enabled == 1)
               .first())
        if row and (row.credit_multiplier or 0) == 0:
            return mid
    row = (db.query(ModelConfig)
           .filter(ModelConfig.enabled == 1, ModelConfig.credit_multiplier == 0)
           .order_by(ModelConfig.model_id.asc()).first())
    return row.model_id if row else "hy3"


def _pick_account(db: Session) -> Account | None:
    """挑一个可用账号（仅用于拉取任务定义，不写任何东西）。"""
    return (db.query(Account)
            .filter(Account.status == "active")
            .order_by(Account.id.asc()).first())


def _classify_all(tasks: list[dict]) -> list[dict]:
    return [growth_plans.classify(t) for t in tasks]


class AcceptIn(BaseModel):
    account_ids: list[int]
    task_codes: list[str]


class RunIn(BaseModel):
    account_ids: list[int] = []           # 空 = 全部可用账号
    task_codes: list[str] | None = None   # 为空表示「所有可自动完成的任务」


class ClaimIn(BaseModel):
    account_ids: list[int]
    task_codes: list[str] | None = None   # 为空表示「所有已完成待领取的任务」


@router.get("/tasks")
def growth_tasks(refresh: bool = False, db: Session = Depends(get_db)):
    """获取全量任务列表（带分级信息），不绑定具体账号。

    任务定义对所有账号一致，所以随便用一个可用登录态拉取即可。
    """
    if _task_cache.get("tasks") and not refresh:
        cached = dict(_task_cache)
        cached["cached"] = True
        return cached

    acc = _pick_account(db)
    if not acc:
        raise HTTPException(400, "没有可用账号，无法拉取任务列表")

    try:
        with backend.AccountSession(acc.auth_json) as s:
            raw = s.growth_tasks()
    except Exception as e:
        # 拉取失败时退回旧缓存，避免面板整体不可用
        if _task_cache.get("tasks"):
            stale = dict(_task_cache)
            stale.update({"cached": True, "stale": True, "error": str(e)})
            return stale
        raise HTTPException(502, f"拉取任务列表失败: {e}")

    _task_cache.clear()
    _task_cache.update({
        "tasks": _classify_all(raw),
        "synced_at": datetime.utcnow().isoformat(timespec="seconds"),
        "source_account_id": acc.id,
        "cached": False,
    })
    return dict(_task_cache)


def _run_accept(acc: Account, codes: list[str]) -> dict:
    """在一个账号上执行参与，并回写可能被刷新的凭据。"""
    with backend.AccountSession(acc.auth_json) as s:
        try:
            st = s.growth_accept(codes)
        finally:
            acc.auth_json = _updated(s, acc)
    return st


@router.post("/accept")
def growth_accept(payload: AcceptIn, db: Session = Depends(get_db)):
    """批量参与任务（未参与的任务不会累计进度）。"""
    results = []
    for aid in payload.account_ids:
        acc = db.query(Account).get(aid)
        if not acc:
            results.append({"account_id": aid, "ok": False, "msg": "账号不存在"})
            continue
        try:
            st = _run_accept(acc, payload.task_codes)
            results.append({"account_id": aid, "ok": True, "results": st})
        except Exception as e:
            results.append({"account_id": aid, "ok": False, "msg": str(e)})
        time.sleep(_ACCOUNT_GAP)
    db.commit()
    return {"results": results}


def _find_task(session, task_code: str) -> dict | None:
    """重新读取单个任务的最新状态（accept 后刷新用）。

    注意：每次调用都会拉一次完整任务列表（上游 1.3~2.4 秒）。
    因此**绝不能**把它放进高频轮询里——否则轮询间隔会被单次请求耗时
    顶到几秒，8 秒的上限内反复拉列表，账号一多就非常慢。
    轮询应改用 _task_state（带缓存）。
    """
    try:
        for t in session.growth_tasks():
            if t.get("task_code") == task_code:
                return t
    except Exception:
        pass
    return None


class _TaskState:
    """轮询用的任务状态缓存：在 min_interval 内复用上一次的列表结果。

    上游拉一次列表要 1~2 秒，如果轮询里每次都重新拉，既慢又浪费，
    还会让「8 秒超时」变成实际十几秒。这里缓存一个短窗口（默认 1 秒），
    既能看到状态变化，又不会把上游打爆。
    """

    def __init__(self, session, min_interval: float = 1.0):
        self._s = session
        self._min = min_interval
        self._ts = 0.0
        self._snapshot: list | None = None

    def _list(self, force: bool = False) -> list:
        now = time.monotonic()
        if force or self._snapshot is None or (now - self._ts) >= self._min:
            try:
                self._snapshot = self._s.growth_tasks()
                self._ts = now
            except Exception:
                if self._snapshot is None:
                    self._snapshot = []
                # 拉取失败时保留旧快照，等下次窗口再试
        return self._snapshot or []

    def get(self, task_code: str) -> dict | None:
        for t in self._list():
            if t.get("task_code") == task_code:
                return t
        return None

    def refresh(self) -> list:
        """强制拉一次最新列表（需要精确状态时用）。"""
        return self._list(force=True)


def _updated(session, acc: Account) -> str:
    """AccountSession 关闭前读回最新凭据（token 可能被刷新）。"""
    try:
        return session.updated_json()
    except Exception:
        return acc.auth_json


#: 等「first_buddy 解锁完成」的上限（秒）。
#: 补完门槛对话后，上游需要一点时间才让其它任务可参与；
#: 太早进入任务循环会白跑一整轮（实测 id=47 的 14 个任务全被前置条件拦下）。
_GATE_UNLOCK_WAIT = 12.0


def _gate_unlocked(session) -> bool:
    """`first_buddy` 是否已达标（completed/claimed）。异常一律当「未达」。"""
    try:
        for t in session.growth_tasks():
            if t.get("task_code") == "first_buddy":
                return t.get("accept_status") in ("completed", "claimed")
    except Exception:
        pass
    return False


def _reload_after_gate(session, before: list) -> list:
    """门槛处理之后重新拉任务列表；失败则退回旧列表（不要因为网络抖动就崩）。"""
    try:
        return session.growth_tasks()
    except Exception:
        return before


def run_accounts(account_ids: list[int], task_codes: list[str] | None,
                 db: Session, on_done=None, on_beat=None,
                 soft_budget: float | None = None,
                 on_task=None) -> dict:
    """对一批账号执行「自动参与 + 触发完成 + 领取」。供接口与定时任务共用。

    这里沉淀了串行执行与节流逻辑，定时任务必须复用本函数，
    避免绕过 accept 落库等待而出现「任务不成功」的问题。

    与早期版本的差别：不再用固定 sleep 死等上游落库，改成**轮询到就绪为止**
    （`jobrunner.wait_for`）。固定 sleep 在两种情况下都吃亏：
      * 上游快时白等（原本每账号约 27 秒，大半是干等）
      * 上游慢时不够（复查时进度还没落库，于是报「已完成未领取」，
        用户得再点一次才领到 —— 就是体感上的「分成两三步」）

    Args:
        on_done: 每完成一个账号回调一次，用于上报进度（可为 None）。
        on_beat: 处理每个账号前的心跳回调（可为 None）。
        on_task: 每处理完一个**任务**回调一次，用于上报细粒度进度
            （形如 `on_task(account_name, task_item)`）。一个账号要跑
            十几个任务、耗时几分钟，只有账号级进度的话界面会长时间不动，
            用户以为卡住了。
        soft_budget: 整个批量的软预算（秒）。到点**停止接新账号**，
            但不打断正在处理的账号 —— 给同步接口用，避免请求本身超时。
            None = 不限。
    """
    model = _pick_free_model(db)
    batch_start = time.monotonic()
    # 统一顺序：创建时间倒序（新 → 老）。
    # 放在这里而不是各个调用方，是为了让同步接口、异步任务、定时任务
    # **三者顺序完全一致** —— 否则「定时任务和手动点的顺序不一样」会很难解释，
    # 用户也会觉得「我点了新号却先从老号开始跑」。
    account_ids = _ordered_ids(list(account_ids), db)
    out = []

    def _push_task(acc_log: dict, item: dict, acc_name: str = "") -> None:
        """记录一个任务结果，并上报细粒度进度。

        统一走这里（而不是各处直接 append）是为了保证每一条都回调 on_task ——
        漏掉某一类分支就会出现「界面停在那里不动」，而实际后端在推进。
        """
        acc_log["tasks"].append(item)
        if on_task:
            try:
                on_task(acc_name, item)
            except Exception:
                pass

    for aid in account_ids:
        # 软预算：整批已经跑太久了，剩下的账号直接如实标注「留待下次」，
        # 让请求体面返回，而不是被 nginx 掐断（那样前端只会看到 504，更糟）。
        if soft_budget is not None and (time.monotonic() - batch_start) >= soft_budget:
            item = {"account_id": aid, "ok": False,
                    "msg": "整批已用满本次时间预算，剩余账号留待下次"}
            out.append(item)
            if on_done:
                on_done(item)
            continue

        acc = db.query(Account).get(aid)
        if not acc:
            item = {"account_id": aid, "ok": False, "msg": "账号不存在"}
            out.append(item)
            if on_done:
                on_done(item)
            continue

        # 开始处理这个账号时先报一次心跳，界面才能显示「正在处理 xxx」，
        # 否则慢账号期间进度条纹丝不动，看起来像卡死
        if on_beat:
            try:
                on_beat(acc.name or str(aid))
            except Exception:
                pass

        acc_log = {"account_id": aid, "name": acc.name, "model": model,
                   "ok": True, "tasks": []}
        snapshot = None
        # 单账号总预算：无论上游多慢、任务多少，超过就收尾进入下一个账号。
        # 没有这个兜底时，一个异常账号可能把整个批量拖住很久（表现为
        # 「卡在 N/M 不动了」）。达到预算会记录超时提示，不会静默丢弃。
        acc_deadline = time.monotonic() + _ACCOUNT_BUDGET
        try:
            with backend.AccountSession(acc.auth_json) as s:
                tasks = s.growth_tasks()
                by_code = {t.get("task_code"): t for t in tasks}

                # ── 前置门槛：first_buddy ───────────────────────────────
                # 上游对绝大多数任务都要求先完成 first_buddy，否则 accept 直接回
                #   {"status":"error","message":"prerequisite not met: first_buddy
                #    (no buddy instance found)"}
                # 而 HTTP 仍是 200 —— 不检查 status 就会被当成成功，
                # 于是「满屏成功、实际一个没做」（本账号实测 15 个里 13 个如此）。
                #
                # first_buddy 的条件是「至少一次对话」，补一次就解锁。
                # 这套逻辑本来就写在 backend._clear_buddy_gate（猫猫旅行在用），
                # 这里复用同一实现，不要另写一份。
                gate_state = (by_code.get("first_buddy") or {}).get("accept_status")
                if gate_state not in ("completed", "claimed"):
                    try:
                        ok_gate, gate_msg = s._clear_buddy_gate()
                        acc_log["buddy_gate"] = {"ok": ok_gate, "msg": gate_msg}
                        _logger.info("growth: 账号 %s 前置门槛 first_buddy -> %s（%s）",
                                     acc.name, ok_gate, gate_msg)
                    except Exception as e:
                        acc_log["buddy_gate"] = {"ok": False, "msg": str(e)[:160]}
                    # 门槛处理完重新拉一次：解锁后的任务状态会变
                    tasks = _reload_after_gate(s, tasks)
                    by_code = {t.get("task_code"): t for t in tasks}

                # ⚠️ 上面的「重新拉一次」有可能仍拿到未解锁的快照：
                # `_clear_buddy_gate` 内部只 sleep 1.5s 就复查，而上游把
                # first_buddy 落库并解锁其余任务是有延迟的。实测 id=47 就是
                # 这样：first_buddy 已被标记 claimed，但紧接着对 14 个任务
                # accept 全部回 `prerequisite not met: first_buddy` —— 白跑一轮。
                # 所以这里再加一道「确认解锁」的轮询，确认不了才继续
                # （宁可多等几秒，也不要白跑一整轮）。
                if not task_codes and gate_state not in ("completed", "claimed"):
                    unlocked = jobrunner.wait_for(
                        lambda: _gate_unlocked(s), _GATE_UNLOCK_WAIT, _POLL_INTERVAL)
                    if unlocked:
                        tasks = s.growth_tasks()
                        by_code = {t.get("task_code"): t for t in tasks}
                        acc_log["buddy_gate"]["unlocked"] = True
                    else:
                        acc_log["buddy_gate"]["unlocked"] = False
                        _logger.warning(
                            "growth: 账号 %s 的 first_buddy 未在 %.0fs 内确认解锁，"
                            "本轮任务可能仍被前置条件拦下",
                            acc.name, _GATE_UNLOCK_WAIT)

                wanted = task_codes or [
                    t.get("task_code") for t in tasks
                    if growth_plans.plan_for(t.get("task_code") or "").actionable
                    and t.get("accept_status") not in ("claimed", "completed")
                ]

                # 快速通道：没有可做的任务、也没有待领取的奖励时，
                # 直接返回。拉一次列表已经是全部开销，不再多打任何请求。
                # （号池做完后就是这种状态，应秒过而不是等几十秒）
                claimable = [t.get("task_code") for t in tasks
                             if t.get("accept_status") == "completed"]
                if not wanted and not claimable:
                    auto_total = claimed_n = 0
                    for t in tasks:
                        plan = growth_plans.plan_for(t.get("task_code") or "")
                        if not plan.actionable:
                            continue
                        auto_total += 1
                        if t.get("accept_status") == "claimed":
                            claimed_n += 1
                    acc_log["tasks"] = [
                        {"task_code": t.get("task_code"),
                         "title": t.get("title") or t.get("task_code"),
                         "level": growth_plans.plan_for(t.get("task_code") or "").level,
                         "ok": True, "skipped": "无可做任务"}
                        for t in tasks
                        if growth_plans.plan_for(t.get("task_code") or "").actionable
                    ]
                    acc_log["noop"] = True
                    # 把「为什么没得做」说清楚，否则界面只有一句「无可做任务」，
                    # 看不出是「已全部领完」还是「任务被卡住了」
                    acc_log["noop_reason"] = (
                        f"可自动化的 {auto_total} 个任务已全部领取" if auto_total
                        else "该账号没有可自动化的任务"
                    )
                    if auto_total:
                        acc_log["skipped_claimed"] = claimed_n
                    out.append(acc_log)
                    if on_done:
                        on_done(acc_log)
                    continue

                for code in wanted:
                    info = by_code.get(code) or {}
                    plan = growth_plans.plan_for(code)
                    item = {"task_code": code, "title": info.get("title") or code,
                            "level": plan.level}

                    if info.get("accept_status") == "claimed":
                        item.update({"ok": True, "skipped": "已领取"})
                        _push_task(acc_log, item, acc.name)
                        continue
                    if not plan.actionable:
                        # 需人工完成的任务直接跳过，绝不发请求：
                        # 之前这里也会走完整流程（含拉列表复查），是纯浪费
                        item.update({"ok": True, "skipped": plan.reason or "需人工完成"})
                        _push_task(acc_log, item, acc.name)
                        continue
                    if time.monotonic() >= acc_deadline:
                        # 单账号预算用完：如实标记，不静默跳过
                        item.update({"ok": False, "skipped": "本账号超时，剩余任务留待下次"})
                        _push_task(acc_log, item, acc.name)
                        acc_log["budget_exceeded"] = True
                        continue

                    # 参与（未参与的任务不计进度）：
                    # 轮询等 accept 落库，而不是固定 sleep 3 秒。
                    # 用缓存轮询：上游拉一次列表要 1~2 秒，直接放在轮询里
                    # 会把「8 秒上限」拖成实际十几秒。
                    state = _TaskState(s)
                    if info.get("accept_status") == "not_accepted":
                        try:
                            ar = s.growth_accept([code])
                            # ⚠️ accept 的失败**不会抛异常**：上游在信封里回
                            # `{"<code>": "error"}` 外加一句 message（HTTP 仍是 200）。
                            # 原来只看异常，于是把「参与失败」当成成功，
                            # 后面照样发事件、照样报 ok=True —— 用户看到满屏成功，
                            # 实际一个任务都没计入（这次实测 15 个里 13 个是这种）。
                            st = (ar or {}).get(code)
                            if st and st not in ("accepted", "already_accepted"):
                                item["accept_failed"] = st
                                # 把上游原话带出来，这是最有价值的信息
                                # （实测就是它指出「需先完成 first_buddy」）
                                msg = s.last_accept_message(code)
                                if msg:
                                    item["accept_message"] = msg
                                # 参与都没成功，进度不可能计入 —— 直接跳过触发，
                                # 省掉一次必然无效的上游请求
                                item.update({"ok": False,
                                             "skipped": msg or "参与任务失败"})
                                _push_task(acc_log, item, acc.name)
                                acc_log.setdefault("accept_blocked", []).append(code)
                                continue

                            def _accepted(c=code):
                                t = state.get(c)
                                return bool(t and t.get("accept_status") not in
                                            (None, "", "not_accepted"))

                            jobrunner.wait_for(_accepted, _ACCEPT_TIMEOUT,
                                               _POLL_INTERVAL)
                            info = state.refresh() and state.get(code) or info
                        except Exception as e:
                            item["accept_error"] = str(e)

                    prog = info.get("progress") or {}
                    target = prog.get("target")
                    current = prog.get("current") or 0
                    times = (target - current) if isinstance(target, int) and target > current else 1
                    times = max(1, min(times, _MAX_TIMES))

                    fire_model = plan.model or model
                    # Ardot 类任务的前置条件：账号必须已绑定 Ardot，否则取票会
                    # 得到 10101 access token not found，任务**必然**失败。
                    # 实测多数账号初始未绑定，所以这里主动补绑定，
                    # 而不是让用户看到一条「未换取 Ardot access token」的报错。
                    if plan.firer == "fire_design_canvas":
                        try:
                            if not s.ensure_ardot_connected():
                                item["last_error"] = (
                                    "账号未能绑定 Ardot（connector 授权失败），"
                                    "该任务需要先完成 Ardot 授权")
                                _push_task(acc_log, item, acc.name)
                                continue
                        except Exception as e:
                            item["last_error"] = f"Ardot 绑定检查失败：{e}"

                    fired = 0
                    for i in range(times):
                        # 两种触发方式：
                        #   firer 非空 -> 调用 AccountSession 上的对应方法
                        #                （上报真实业务事件，如 fire_library_read）
                        #   firer 为空 -> 走 chat/completions 的 growthEvent
                        if plan.firer:
                            fn = getattr(s, plan.firer, None)
                            r = (fn() if fn else
                                 {"ok": False, "msg": f"缺少触发方法 {plan.firer}"})
                        else:
                            r = s.growth_fire_event(plan.event_codes, model=fire_model)
                        if r.get("ok"):
                            fired += 1
                        else:
                            item["last_error"] = r.get("msg")
                        # 最后一次之后不用等 —— 原来每次白等 _EVENT_GAP，
                        # 5 次任务就白花 6s，多个任务累加很可观。
                        if i < times - 1:
                            time.sleep(_EVENT_GAP)

                    # 复查：轮询到进度变化为止，避免「已完成但没领」的假象
                    def _advanced(c=code, cur=current):
                        n = state.get(c)
                        if not n:
                            return False
                        st = n.get("accept_status")
                        if st in ("completed", "claimed"):
                            return True
                        np_ = (n.get("progress") or {}).get("current")
                        return isinstance(np_, int) and isinstance(cur, int) and np_ > cur

                    jobrunner.wait_for(_advanced, _VERIFY_TIMEOUT, _POLL_INTERVAL)

                    try:
                        snapshot = state.refresh()
                        now = {t.get("task_code"): t for t in snapshot}
                        cur_task = now.get(code) or {}
                        np_ = cur_task.get("progress") or {}
                        item["progress"] = f"{np_.get('current')}/{np_.get('target')}"
                        item["status"] = cur_task.get("accept_status")
                    except Exception:
                        pass

                    # ⚠️ `ok` 的判据必须是「上游真的记上了」，而不是「我们发过请求」。
                    # 原来写的是 `fired > 0` —— 只要 HTTP 200 就算成功，
                    # 于是实测 15 个任务全报 ok=True，实际 13 个连参与都没成功
                    # （进度仍是 not_accepted）。这种假成功比直接报错更有害：
                    # 用户以为做完了，实际没做。
                    progressed = item.get("status") in ("completed", "claimed")
                    item.update({"ok": progressed, "fired": fired, "times": times})
                    if not progressed:
                        item["ok"] = False
                        if not item.get("skipped"):
                            item["skipped"] = (item.get("accept_message")
                                               or item.get("last_error")
                                               or "上游未记录进度（参与未通过或事件无效）")
                    if plan.model:
                        item["model"] = plan.model
                    _push_task(acc_log, item, acc.name)

                # 本账号跑完顺手领取，避免用户还要再点一次「领奖」。
                #
                # ⚠️ 这里必须**重新拉一次最新快照**（而不是复用任务循环里
                # 那次复查的结果）：上游把任务标成 completed 有延迟，
                # 循环里那次复查可能还看到 in_progress。之前就是复用了旧快照，
                # 于是「任务做完了但没自动领」——用户得自己再点一次领奖，
                # 正是反馈里的「为啥我要手动再点击领取」。
                #
                # 做法：轮询等到「有可领取的任务」或超时，再统一领取。
                try:
                    claimable: list[str] = []

                    def _has_claimable() -> bool:
                        nonlocal claimable
                        snap = state.refresh()
                        claimable = [t.get("task_code") for t in snap
                                     if t.get("accept_status") == "completed"
                                     and t.get("task_code")]
                        return bool(claimable)

                    # 最多等 _CLAIM_WAIT，但一旦看到可领取就立刻领（不等满）
                    jobrunner.wait_for(_has_claimable, _CLAIM_WAIT, _POLL_INTERVAL)

                    claimed = []
                    for code in claimable:
                        try:
                            r = s.growth_claim(code)
                            d = r.get("data") or {}
                            claimed.append({"task_code": code,
                                            "ok": bool(r.get("ok")),
                                            "credit": d.get("credit") or 0,
                                            "energy": d.get("energy") or 0})
                        except Exception as e:
                            claimed.append({"task_code": code, "ok": False,
                                            "msg": str(e)[:120]})
                        if len(claimable) > 1:
                            time.sleep(_EVENT_GAP)
                    if claimed:
                        acc_log["claimed"] = claimed
                        acc_log["credit"] = sum(c.get("credit") or 0 for c in claimed)
                        acc_log["energy"] = sum(c.get("energy") or 0 for c in claimed)
                        ok_n = sum(1 for c in claimed if c.get("ok"))
                        if ok_n < len(claimed):
                            # 有领取失败的，如实记下来（否则界面显示领取成功、
                            # 实际没到账，比报错更难排查）
                            acc_log["claim_failed"] = [
                                c for c in claimed if not c.get("ok")]
                except Exception as e:
                    acc_log["claim_error"] = str(e)[:160]

                acc.auth_json = _updated(s, acc)

                # 做完 + 领完之后**顺手刷一次真实余额**并落库。
                #
                # 为什么必须在这里刷：做完任务/领了奖，积分确实到账了，
                # 但库里 `balance_remain` 还是旧值 —— 前端列表拿到旧数字，
                # 用户会看到「任务完成了但积分没变」，以为白做了。
                # 复用同一个 AccountSession，省掉重新建连的开销。
                try:
                    bal = s.fetch_balance()
                    acc.balance_total = int(bal.get("total", 0) or 0)
                    acc.balance_remain = int(bal.get("remain", 0) or 0)
                    acc.last_sync_at = datetime.utcnow()
                    acc_log["balance_total"] = acc.balance_total
                    acc_log["balance_remain"] = acc.balance_remain
                except Exception as e:
                    # 刷新失败不影响任务结果，但要说清楚，便于排查
                    acc_log["balance_error"] = str(e)[:120]
        except Exception as e:
            acc_log.update({"ok": False, "msg": str(e)})
        out.append(acc_log)
        # 每跑完一个账号就上报进度。之前只在「账号不存在」分支调用，
        # 正常路径从不回调，前端因此一直停在 0/N，直到全部跑完才跳到 N/N。
        if on_done:
            on_done(acc_log)
        # 单账号场景不必等（用户就等这一个结果，白等 1 秒纯属浪费）
        if len(account_ids) > 1:
            time.sleep(_ACCOUNT_GAP)

    # 做完任务后必须清掉这些账号的任务缓存，否则前端紧接着的「刷新」
    # 会拿到旧状态，看起来像「做完但界面没变」。
    _invalidate_acct_tasks(account_ids)
    db.commit()
    return {"results": out, "model": model}


def claim_accounts(account_ids: list[int], task_codes: list[str] | None,
                   db: Session) -> dict:
    """对一批账号领取奖励。供接口与定时任务共用。"""
    out = []
    # 与做任务同序（新 → 老），保证界面上的顺序稳定、可预期
    account_ids = _ordered_ids(list(account_ids), db)
    for aid in account_ids:
        acc = db.query(Account).get(aid)
        if not acc:
            out.append({"account_id": aid, "ok": False, "msg": "账号不存在"})
            continue

        acc_log = {"account_id": aid, "name": acc.name, "ok": True,
                   "claimed": [], "total_credit": 0, "total_energy": 0}
        try:
            with backend.AccountSession(acc.auth_json) as s:
                if task_codes:
                    codes = list(task_codes)
                else:
                    tasks = s.growth_tasks()
                    codes = [t.get("task_code") for t in tasks
                             if t.get("accept_status") == "completed"]

                for i, code in enumerate(codes):
                    try:
                        r = s.growth_claim(code)
                        d = r.get("data") or {}
                        credit = d.get("credit") or 0
                        energy = d.get("energy") or 0
                        acc_log["claimed"].append({
                            "task_code": code,
                            "ok": bool(r.get("ok")),
                            "already": bool(d.get("already_claimed")),
                            "credit": credit,
                            "energy": energy,
                            "msg": r.get("msg") or "",
                        })
                        acc_log["total_credit"] += credit
                        acc_log["total_energy"] += energy
                    except Exception as e:
                        acc_log["claimed"].append(
                            {"task_code": code, "ok": False, "msg": str(e)})
                    # 只在**还有下一个**时才等：原来每次（含最后一次）都 sleep，
                    # 领 10 个任务就白等 12 秒 —— 这是「领奖卡卡的」主因之一。
                    if i + 1 < len(codes):
                        time.sleep(_EVENT_GAP)
                acc.auth_json = _updated(s, acc)

                # 领完**顺手刷一次真实余额**并落库。
                # 不刷的话前端拿到的还是旧余额，用户会以为「领了但没到账」
                # （实际到账了，只是库里没更新）。
                # 放在同一个 AccountSession 里，省掉重新建连的开销。
                try:
                    bal = s.fetch_balance()
                    acc.balance_total = int(bal.get("total", 0) or 0)
                    acc.balance_remain = int(bal.get("remain", 0) or 0)
                    acc_log["balance_total"] = acc.balance_total
                    acc_log["balance_remain"] = acc.balance_remain
                    acc.last_sync_at = datetime.utcnow()
                except Exception as e:
                    acc_log["balance_error"] = str(e)[:120]
        except Exception as e:
            acc_log.update({"ok": False, "msg": str(e)})
        out.append(acc_log)
        # 账号之间保留间隔（并发打上游不礼貌），但单账号场景不该等：
        # 只有确实还有下一个账号时才睡。
        if len(account_ids) > 1:
            time.sleep(_ACCOUNT_GAP)

    _invalidate_acct_tasks(account_ids)
    db.commit()
    return {"results": out}


@router.post("/run")
def growth_run(payload: RunIn, db: Session = Depends(get_db)):
    """执行任务：自动参与 + 触发完成事件。

    只处理策略表里标记为可自动的任务；其它任务跳过并在返回里说明原因。

    ⚠️ 这是**同步**接口：账号多/任务多时会跑很久，容易撞 nginx 的 60s 超时。
    所以：
      * 单账号硬上限仍是 `_ACCOUNT_BUDGET`；
      * 这里额外传 `soft_budget=_SYNC_SOFT_BUDGET`，到点就**停止接新任务**
        （不打断当前任务），保证请求本身能及时返回；
      * 要一次跑完多个账号请用 `/run-async`（后台线程，不受此限）。

    `account_ids` 为空 = 全部可用账号（与 `/run-async` 同口径）。
    之前这里直接把空列表透传给 `run_accounts`，于是「不传账号」= 什么都不做，
    而 `/run-async` 的同一个参数却是「全部账号」—— 同一个字段两种语义，
    前端稍不注意就会「点了没反应」。
    """
    ids = _resolve_ids(payload.account_ids or None)
    return run_accounts(ids, payload.task_codes, db,
                        soft_budget=_SYNC_SOFT_BUDGET)


#: 后台任务 key：同 key 同时只允许一个在跑
JOB_KEY = "growth_run"


def _ordered_ids(ids: list[int], db: Session) -> list[int]:
    """把账号 id 按**创建时间倒序**（新 → 老）排好。

    为什么按创建时间而不是 id 大小：批量做任务时用户希望**先看到新号**的结果
    —— 新录入的号最可能需要处理，老号大多已经做完了。原先按 `id.asc()`
    排，结果是「新号排在最后，前面一堆老号在快速通道里空转」，
    用户盯着进度条等半天看不到自己刚加的号。

    用 id 作为次级键：id 是自增的，与创建顺序一致，
    且能保证同秒创建的账号也有稳定顺序（不会每次跑顺序都变）。
    """
    if not ids:
        return []
    rows = (db.query(Account.id, Account.created_at)
            .filter(Account.id.in_(ids)).all())
    created = {r.id: r.created_at for r in rows}
    # created_at 为空的排最后（理论上不该有，兜底不让它插队）
    return sorted(ids,
                  key=lambda i: (created.get(i) is None,
                                 -(created.get(i).timestamp() if created.get(i)
                                   else 0),
                                 -i))


def _resolve_ids(payload_ids: list[int] | None) -> list[int]:
    """把请求里的 ids 解析成实际要处理的账号 id 列表（空 = 全部 active）。

    返回顺序统一为**创建时间倒序（新 → 老）**，见 `_ordered_ids`。
    """
    db = SessionLocal()
    try:
        if payload_ids:
            return _ordered_ids(list(payload_ids), db)
        rows = (db.query(Account)
                .filter(Account.status == "active")
                .order_by(Account.created_at.desc(), Account.id.desc()).all())
        return [a.id for a in rows]
    finally:
        db.close()


@router.post("/run-async")
def growth_run_async(payload: RunIn):
    """异步批量做任务：立即返回 job_id，前端轮询进度。

    为什么异步：一个账号约 27 秒（串行 + 限速），xx 个账号要 6 分钟以上，
    同步等会让 nginx 先超时（默认 60s）→ 前端吃 504、体感「点了没反应」。

    重复点击不会叠起并发批量：同 key 已有任务在跑时直接返回现有 job。
    """
    running = jobrunner.RUNNER.get(JOB_KEY)
    if running and running.status == "running":
        return {"reused": True, **running.snapshot()}

    # 统一按创建时间倒序（新 → 老），与 /run 的 id 解析口径一致
    ids = _resolve_ids(payload.account_ids or None)
    tasks = payload.task_codes

    def worker(job: jobrunner.Job) -> dict:
        db = SessionLocal()
        try:
            job.set_phase("执行中")
            res = run_accounts(ids, tasks, db, on_done=job.add_item,
                               on_beat=job.beat,
                               on_task=job.task_beat)
        finally:
            db.close()
        results = res.get("results") or []
        credit = sum((r.get("credit") or 0) for r in results)
        energy = sum((r.get("energy") or 0) for r in results)
        done = sum(1 for r in results for t in (r.get("tasks") or [])
                   if t.get("ok") and not t.get("skipped"))
        return {"accounts": len(ids), "tasks_done": done,
                "credit": credit, "energy": energy,
                "failed": [r["account_id"] for r in results if not r.get("ok")]}

    job = jobrunner.RUNNER.start(JOB_KEY, len(ids), worker, title="批量做任务")
    return job.snapshot()


@router.get("/job/{job_id}")
def growth_job(job_id: str):
    """查询后台任务进度。"""
    job = jobrunner.RUNNER.by_id(job_id)
    if not job:
        raise HTTPException(404, "任务不存在或已过期")
    return job.snapshot()


@router.post("/claim")
def growth_claim(payload: ClaimIn, db: Session = Depends(get_db)):
    """批量领取奖励。

    task_codes 为空时，自动领取所有「已完成但未领取」（completed）的任务。
    """
    return claim_accounts(payload.account_ids, payload.task_codes, db)


@router.get("/accounts/{account_id}/tasks")
def account_tasks(account_id: int, refresh: bool = False,
                  db: Session = Depends(get_db)):
    """单个账号的任务完成情况（面板弹窗用）。

    短缓存：上游拉一次任务列表实测要 **1~3.3 秒**，而前端在「执行完 → 刷新」
    「领奖完 → 刷新」时会立刻再拉一次，用户体感就是「卡卡的」。
    这里缓存一个很短的窗口（默认 3 秒），既能让连续刷新秒回，
    又不至于让状态显示过时（执行完刷新时传 refresh=True 可强制绕过）。
    """
    if not refresh:
        hit = _acct_tasks_cache.get(account_id)
        if hit and (time.time() - hit[0]) < _ACCT_TASKS_TTL:
            data = dict(hit[1])
            data["cached"] = True
            return data

    acc = db.query(Account).get(account_id)
    if not acc:
        raise HTTPException(404, "账号不存在")
    try:
        with backend.AccountSession(acc.auth_json) as s:
            tasks = s.growth_tasks()
            profile = s.growth_profile()
    except Exception as e:
        # 拉取失败时退回旧缓存（哪怕过期），别让弹窗直接白屏
        hit = _acct_tasks_cache.get(account_id)
        if hit:
            data = dict(hit[1])
            data.update({"cached": True, "stale": True, "error": str(e)[:160]})
            return data
        raise HTTPException(502, f"查询失败: {e}")

    items = _classify_all(tasks)
    done = sum(1 for t in items if t.get("accept_status") in ("completed", "claimed"))
    #: 可领取奖励（completed 但未 claim）
    claimable = [t for t in items if t.get("accept_status") == "completed"]
    out = {
        "account_id": account_id,
        "name": acc.name,
        "tasks": items,
        "profile": profile,
        "summary": {
            "total": len(items),
            "done": done,
            "claimable": len(claimable),
            "claimable_credit": sum(t.get("reward_credit") or 0 for t in claimable),
            "actionable": sum(1 for t in items if t.get("actionable")
                              and t.get("accept_status") != "claimed"),
        },
    }
    _acct_tasks_cache[account_id] = (time.time(), out)
    # 简单防膨胀：条目远多于账号数时清掉最旧的
    if len(_acct_tasks_cache) > 500:
        for k in sorted(_acct_tasks_cache, key=lambda k: _acct_tasks_cache[k][0])[:200]:
            _acct_tasks_cache.pop(k, None)
    return out


@router.get("/plans")
def growth_plans_view():
    """任务策略表（分级依据），便于管理员了解哪些能自动完成。"""
    return {
        "levels": growth_plans.LEVEL_LABEL,
        "plans": [
            {"task_code": p.code, "level": p.level,
             "level_label": growth_plans.LEVEL_LABEL.get(p.level, ""),
             "event_codes": p.event_codes, "reason": p.reason,
             "actionable": p.actionable}
            for p in growth_plans.TASK_PLANS.values()
        ],
    }
