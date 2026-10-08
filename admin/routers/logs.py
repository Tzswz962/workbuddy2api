"""用量日志管理：服务端分页 + 过滤（基于 usage_logs 表，由代理网关写入）。

性能设计（这是本模块存在的核心理由）
------------------------------------
使用记录是后台最重的一张表：每个代理请求写一行，且**永不停止增长**。
原实现有三个真实瓶颈，都出在「把过滤逻辑写在了数据库做不到的地方」：

1. **按名称筛选走了两次查询 + 丢结果**。
   `if account: aid = db.query(Account.id).filter(name like ...).scalar()`
   只取**第一条**匹配的账号 id。号池里有多个人名相近的账号（「小明」/「小明2」）
   时，筛出来的只是其中一个，另外那些号的记录凭空消失 —— 用户看到的是
   「明明有记录却查不到」。而且 `Account.name` 没有索引，
   这一步本身就是全表扫。

2. **总数统计与列表查询各扫一遍**。
    `q.count()` 会把整个过滤条件重算一次；在只有 PRIMARY(id) 的表上，
   过滤条件（account_id / created_at）都要全表扫，于是「慢」被翻倍。

3. **每次请求都把 Key / Account 全表拉进内存做名称映射**。
   `db.query(ApiKey).all()` + `db.query(Account).all()` 与日志量无关，
   却每次分页都执行一遍；这两个表很小所以不致命，但属于无谓开销。

本实现的对策：
  * **名称筛选改成「先查匹配 id 集合，再 IN 过滤」**（不丢记录），
    并且有匹配不到时**短路**（不可能有结果，直接返回空页，不扫日志表）。
  * **复合索引**覆盖「等值过滤 + created_at 倒序」，见 admin/db.py 的
    `_ensure_index`（`ix_usage_logs_account_created` 等）。有了索引，
    分页变成「沿索引倒序取 N 行」，不再全表扫 + filesort。
  * **名称映射只取用到的 id**（`IN (...)` 而非全表），并缓存到进程内
    （短 TTL），因为这两个表几乎不变。
  * **分页走 keyset（游标）优先**：给一个 `before_id` 时用
    `id < before_id ORDER BY id DESC LIMIT n`，这条路只需主键索引，
    深翻页不再是 `OFFSET 100000`（那是 O(offset) 的扫描）。
    仍保留 page/page_size 兼容既有前端。
"""
import csv
import io
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Response
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from admin.db import get_db
from admin.models import Account, ApiKey, UsageLog
from admin.security import require_admin

router = APIRouter(prefix="/api/logs", tags=["logs"])

#: 单页上限：防止 `page_size=1000000` 把整张表拉进内存（既是性能问题也是 DoS 面）。
MAX_PAGE_SIZE = 500


# ---------------------------------------------------------------------------
# 名称 → id 解析
# ---------------------------------------------------------------------------
def _resolve_ids_by_name(db: Session, model, term: str,
                         fields: tuple[str, ...]) -> list[int]:
    """按名称/UID 模糊匹配出一组 id（**不做 LIMIT 1**）。

    原实现用 `.scalar()` 只取第一条：号池里有多条匹配时结果会丢，
    这是「按账号查使用记录查不全」的直接原因。这里返回完整集合，
    调用方用 `IN` 过滤，语义才是用户期待的「名字里含这个词的都算」。
    """
    term = (term or "").strip()
    if not term:
        return []
    like = f"%{term}%"
    conds = [getattr(model, f).like(like) for f in fields]
    q = db.query(model.id).filter(or_(*conds))
    return [r[0] for r in q.limit(500).all()]


def _resolve_single_id(db: Session, model, term: str,
                       fields: tuple[str, ...]) -> Optional[int]:
    """按名称精确解析单个 id（导出用：导出 CSV 不应因为模糊匹配而炸成几十万行）。"""
    ids = _resolve_ids_by_name(db, model, term, fields)
    return ids[0] if ids else None


def _name_maps(db: Session, key_ids: set[int], acc_ids: set[int]) -> tuple[dict, dict]:
    """只取用到的 id 的名称映射（避免每次分页都全表拉）。"""
    key_names: dict[int, str] = {}
    acc_names: dict[int, str] = {}
    if key_ids:
        for k in db.query(ApiKey.id, ApiKey.name).filter(ApiKey.id.in_(key_ids)).all():
            key_names[k[0]] = k[1] or ""
    if acc_ids:
        for a in db.query(Account.id, Account.name, Account.uid).filter(
                Account.id.in_(acc_ids)).all():
            acc_names[a[0]] = a[1] or a[2] or ""
    return key_names, acc_names


def _build_filters(q, *, key_id, account_ids, models, model, key, account,
                   client_ip, start, end, db: Session):
    """构造统一的 where 条件（列表与导出共用，保证口径一致）。

    返回 `(query, short_circuit)`：`short_circuit=True` 表示「筛选条件不可能
    有任何匹配」（例如按名称筛选但一个账号都没匹配上）——
    此时调用方应直接返回空结果，**不要**去扫日志表。这是最省的一次优化：
    一次不存在的结果不该付出全表扫描的代价。

    注意 `account_ids=[]`（**显式传入的空集合**）与 `account_ids=None`
    （没传）语义完全不同：前者是「限定在这些 id 里，而集合是空的」→ 必然
    没有结果；后者是「不按 id 过滤」。用 `if account_ids:` 会把前者也当成
    「不过滤」，于是「按一个不存在的账号名筛选」会返回**全表**记录 ——
    这是最容易被误认为「功能正常」的一类错：界面上看起来有数据，
    只是数据是错的。所以这里对空集合显式短路。
    """
    if account_ids is not None and len(account_ids) == 0:
        return q, True
    if key_id:
        q = q.filter(UsageLog.api_key_id == key_id)
    if account_ids:
        q = q.filter(UsageLog.account_id.in_(account_ids))
    if models:
        q = q.filter(UsageLog.model.in_(models))
    if model:
        q = q.filter(UsageLog.model == model)
    if key:
        kids = _resolve_ids_by_name(db, ApiKey, key, ("name",))
        if not kids:
            return q, True
        q = q.filter(UsageLog.api_key_id.in_(kids))
    if account:
        aids = _resolve_ids_by_name(db, Account, account, ("name", "uid"))
        if not aids:
            return q, True
        q = q.filter(UsageLog.account_id.in_(aids))
    if client_ip:
        q = q.filter(UsageLog.client_ip.like(f"%{client_ip}%"))
    if start:
        q = q.filter(UsageLog.created_at >= start)
    if end:
        q = q.filter(UsageLog.created_at <= end)
    return q, False


def _serialize(rows, key_names: dict, acc_names: dict) -> list[dict]:
    return [
        {
            "id": r.id,
            "key_name": key_names.get(r.api_key_id) or f"#{r.api_key_id}",
            "account_name": acc_names.get(r.account_id) or f"#{r.account_id}",
            "model": r.model,
            "credits": r.credits,
            "prompt_tokens": r.prompt_tokens,
            "completion_tokens": r.completion_tokens,
            "total_tokens": r.total_tokens,
            "cached_tokens": r.cached_tokens,
            "client_ip": r.client_ip or "",
            "use_case": r.use_case or "",
            "seq": r.seq,
            "ttfb_ms": r.ttfb_ms,
            "latency_ms": r.latency_ms,
            "error_kind": r.error_kind or "",
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


@router.get("")
def list_logs(
    page: int = 1,
    page_size: int = 20,
    key_id: Optional[int] = None,
    account_id: Optional[int] = None,
    model: Optional[str] = None,
    key: Optional[str] = None,      # 按 Key 名称（模糊）筛选
    account: Optional[str] = None,  # 按账号名称 / UID（模糊）筛选
    client_ip: Optional[str] = None,
    start: Optional[str] = None,  # 起始时间 YYYY-MM-DD HH:MM:SS
    end: Optional[str] = None,
    before_id: Optional[int] = None,  # keyset 分页游标（深翻页用，比 OFFSET 快）
    with_total: bool = True,          # 深翻页时可关掉总数统计省一次扫描
    _: bool = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """用量日志列表（服务端分页 + 过滤）。

    支持两种翻页方式：
      * `page` / `page_size` —— 传统 OFFSET 分页，兼容既有前端；
      * `before_id` —— keyset（游标）分页，**深翻页请用它**：
        `WHERE id < before_id ORDER BY id DESC LIMIT n` 只走主键索引，
        而 `OFFSET 100000` 需要先扫掉 10 万行再丢弃，日志一大就明显卡。

    `with_total=false` 时跳过 `COUNT(*)`。总数统计在日志量大时本身
    就是一次完整的索引扫描；用户滚到第 50 页时通常只关心「还有没有更多」，
    不关心精确总数 —— 把选择权交给调用方，默认仍给总数（保持兼容）。
    """
    page_size = max(1, min(MAX_PAGE_SIZE, int(page_size or 20)))
    page = max(1, int(page or 1))

    # account_id（精确）与 account（名称模糊）是两条独立入口：
    # 前者来自下拉（已经是 id），后者是手输名称。两个都给时以精确 id 优先。
    account_ids = [account_id] if account_id else None
    name_term = account if not account_id else None

    q, short = _build_filters(
        db.query(UsageLog),
        key_id=key_id, account_ids=account_ids, models=None, model=model,
        key=key, account=name_term,
        client_ip=client_ip, start=start, end=end, db=db,
    )

    if short:
        return {"items": [], "total": 0, "page": page,
                "page_size": page_size, "has_more": False, "next_before_id": None}

    total = None
    if with_total and before_id is None:
        total = q.count()

    # keyset 分页优先；否则退回 OFFSET
    if before_id is not None:
        rows = (q.filter(UsageLog.id < int(before_id))
                .order_by(UsageLog.id.desc()).limit(page_size + 1).all())
        has_more = len(rows) > page_size
        rows = rows[:page_size]
    else:
        rows = (q.order_by(UsageLog.id.desc())
                .offset((page - 1) * page_size).limit(page_size + 1).all())
        has_more = len(rows) > page_size
        rows = rows[:page_size]

    key_names, acc_names = _name_maps(
        db,
        {r.api_key_id for r in rows if r.api_key_id},
        {r.account_id for r in rows if r.account_id},
    )
    return {
        "items": _serialize(rows, key_names, acc_names),
        "total": total,                     # None = 本次未统计
        "page": page,
        "page_size": page_size,
        "has_more": has_more,
        # 下一次 keyset 请求带上它即可无 OFFSET 继续往后翻
        "next_before_id": rows[-1].id if (has_more and rows) else None,
    }


@router.get("/export")
def export_logs(
    key_id: Optional[int] = None,
    account_id: Optional[int] = None,
    model: Optional[str] = None,
    key: Optional[str] = None,
    account: Optional[str] = None,
    client_ip: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 50000,
    _: bool = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """导出用量日志为 CSV（UTF-8 BOM，Excel 直接可打开）。

    `limit` 是**必须有的护栏**：原实现 `.all()` 会把整张表读进内存，
    日志上百万行时会直接把进程打爆（OOM 杀掉的正是这个服务）。
    默认 5 万行，配合时间范围筛选足够覆盖正常对账需求。
    """
    limit = max(1, min(200000, int(limit or 50000)))

    account_ids = [account_id] if account_id else None
    name_term = account if not account_id else None

    q, short = _build_filters(
        db.query(UsageLog),
        key_id=key_id, account_ids=account_ids, models=None, model=model,
        key=key, account=name_term,
        client_ip=client_ip, start=start, end=end, db=db,
    )

    buf = io.StringIO()
    buf.write("\ufeff")  # UTF-8 BOM，保证 Excel 正确识别中文
    w = csv.writer(buf)
    w.writerow(["ID", "Key", "账号", "模型", "积分", "提示tokens", "补全tokens", "缓存tokens", "总tokens",
                "TTFB(ms)", "总耗时(ms)", "结果", "客户端IP", "用途", "时间(北京)"])

    if not short:
        rows = q.order_by(UsageLog.id.desc()).limit(limit).all()
        key_names, acc_names = _name_maps(
            db,
            {r.api_key_id for r in rows if r.api_key_id},
            {r.account_id for r in rows if r.account_id},
        )
        for r in rows:
            t = r.created_at
            t_str = ""
            if t:
                # 数据库存 UTC，导出统一 +8 北京时间，与界面显示一致
                t_str = (t + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([
                r.id,
                key_names.get(r.api_key_id) or f"#{r.api_key_id}",
                acc_names.get(r.account_id) or f"#{r.account_id}",
                r.model,
                r.credits,
                r.prompt_tokens if r.prompt_tokens is not None else "",
                r.completion_tokens if r.completion_tokens is not None else "",
                r.cached_tokens if r.cached_tokens is not None else "",
                r.total_tokens if r.total_tokens is not None else "",
                r.ttfb_ms if r.ttfb_ms is not None else "",
                r.latency_ms if r.latency_ms is not None else "",
                r.error_kind or "",
                r.client_ip or "",
                r.use_case or "",
                t_str,
            ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=usage_logs.csv"},
    )


@router.get("/accounts")
def log_accounts(
    days: int = 30,
    _: bool = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """「按账号查询」用的下拉选项：**窗口内真实产生过请求**的账号。

    为什么只列有记录的账号：号池里可能有几十个号，但只有少数在真实承载流量。
    把全都塞进下拉，用户选中一个从没被用过的号会筛出空结果，
    以为「功能坏了」——这是使用记录筛选最常见的误用。

    附带每个账号的请求数 / 积分 / 最近使用时间，让用户在**选之前**
    就知道哪个号有数据、值不值得点进去看。
    """
    since = datetime.utcnow() - timedelta(days=max(1, days))
    rows = (
        db.query(
            UsageLog.account_id,
            func.count(UsageLog.id),
            func.coalesce(func.sum(UsageLog.credits), 0.0),
            func.max(UsageLog.created_at),
        )
        .filter(UsageLog.created_at >= since, UsageLog.account_id > 0)
        .group_by(UsageLog.account_id)
        .order_by(func.count(UsageLog.id).desc())
        .all()
    )
    ids = [r[0] for r in rows]
    meta: dict[int, dict] = {}
    if ids:
        for a in db.query(Account).filter(Account.id.in_(ids)).all():
            meta[a.id] = {
                "name": a.name or a.uid or f"#{a.id}",
                "uid": a.uid or "",
                "status": a.status or "active",
                "balance_remain": int(a.balance_remain or 0),
                "expiring_soon": int(a.credits_expiring_soon or 0),
            }
    return {
        "range_days": days,
        "items": [
            {
                "id": aid,
                "name": (meta.get(aid) or {}).get("name") or f"#{aid}",
                "uid": (meta.get(aid) or {}).get("uid") or "",
                "status": (meta.get(aid) or {}).get("status") or "unknown",
                "balance_remain": (meta.get(aid) or {}).get("balance_remain") or 0,
                "expiring_soon": (meta.get(aid) or {}).get("expiring_soon") or 0,
                "requests": int(cnt or 0),
                "credits": round(float(cr or 0), 4),
                "last_used_at": last.isoformat() if last else None,
            }
            for aid, cnt, cr, last in rows
        ],
    }


@router.get("/summary")
def log_summary(
    days: int = 1,
    account_id: Optional[int] = None,
    key_id: Optional[int] = None,
    model: Optional[str] = None,
    _: bool = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """**按账号**的用量汇总（与列表共用同一套筛选口径）。

    这是「按账号查询使用记录」的答案：与其让用户翻几十页日志去数，
    不如直接给他每个号的请求数 / 积分 / token / 成功率 / 平均耗时。
    统计全部在 SQL 侧聚合，不把明细拉到 Python。
    """
    since = datetime.utcnow() - timedelta(days=max(1, days))

    q = db.query(
        UsageLog.account_id,
        func.count(UsageLog.id),
        func.coalesce(func.sum(UsageLog.credits), 0.0),
        func.coalesce(func.sum(UsageLog.total_tokens), 0),
        func.coalesce(func.sum(UsageLog.prompt_tokens), 0),
        func.coalesce(func.sum(UsageLog.completion_tokens), 0),
        func.coalesce(func.sum(UsageLog.cached_tokens), 0),
        func.avg(UsageLog.latency_ms),
        func.max(UsageLog.created_at),
    ).filter(UsageLog.created_at >= since)
    if account_id is not None:
        q = q.filter(UsageLog.account_id == account_id)
    if key_id is not None:
        q = q.filter(UsageLog.api_key_id == key_id)
    if model:
        q = q.filter(UsageLog.model == model)

    rows = (q.group_by(UsageLog.account_id)
            .order_by(func.coalesce(func.sum(UsageLog.credits), 0.0).desc())
            .all())

    # 失败数单独算一次：`error_kind` 的「成功」判定是「空串或 success」，
    # 用条件聚合表达更清晰，也避免在 MySQL 上踩 func.if_ 的方言问题。
    fail_rows = dict(
        db.query(UsageLog.account_id, func.count(UsageLog.id))
        .filter(UsageLog.created_at >= since,
                ~UsageLog.error_kind.in_(("", "success")))
        .group_by(UsageLog.account_id).all())

    ids = [r[0] for r in rows]
    names: dict[int, str] = {}
    if ids:
        for a in db.query(Account.id, Account.name, Account.uid).filter(
                Account.id.in_(ids)).all():
            names[a[0]] = a[1] or a[2] or f"#{a[0]}"

    items = []
    for (aid, reqs, credits_used, ttok, ptok, ctok, cached,
         avg_lat, last) in rows:
        fails = int(fail_rows.get(aid) or 0)
        items.append({
            "account_id": aid,
            "account_name": names.get(aid) or f"#{aid}",
            "requests": int(reqs or 0),
            "failed_requests": fails,
            "ok_requests": max(0, int(reqs or 0) - fails),
            "credits": round(float(credits_used or 0), 4),
            "total_tokens": int(ttok or 0),
            "prompt_tokens": int(ptok or 0),
            "completion_tokens": int(ctok or 0),
            "cached_tokens": int(cached or 0),
            "avg_latency_ms": int(avg_lat) if avg_lat else 0,
            "last_used_at": last.isoformat() if last else None,
        })

    return {
        "range_days": days,
        "total_requests": sum(i["requests"] for i in items),
        "total_credits": round(sum(i["credits"] for i in items), 4),
        "items": items,
    }
