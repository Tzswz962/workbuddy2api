"""后台任务执行器：把耗时批量操作从 HTTP 请求里挪出去。

为什么需要
----------
批量做任务 / 猫猫旅行是「串行 + 限速」的：每个账号要等 accept 落库、
逐次触发、复查进度。实测一个账号约 27 秒，13 个账号就是 6 分钟以上。

这类操作如果直接在请求里同步跑：
  * 请求会一直挂到跑完，nginx 的 proxy_read_timeout（默认 60s）先到点，
    前端直接吃 504，体感就是「点了没反应」；
  * 虽然 FastAPI 给同步端点配了线程池（40），单个批量只占 1 个线程，
    不会拖垮并发，但用户体验依然很差。

做法
----
POST 立即返回 job_id，真正的执行放到守护线程里；前端拿 job_id 轮询进度。
同一个 job 键（如 "cat_travel"）复用一个 runner，因此「重复点击」不会
叠起多个并发批量 —— 第二个请求会直接收到「正在执行」，而不是排队再跑一遍。
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable

#: 已完成任务在内存里保留多久（秒），供前端取最终结果
_KEEP_DONE_SECONDS = 1800


class Job:
    """一次后台批量执行的进度与结果。"""

    def __init__(self, key: str, total: int, title: str = ""):
        self.id = uuid.uuid4().hex[:12]
        self.key = key
        self.title = title
        self.total = total
        self.done = 0
        self.status = "running"          # running | finished | failed
        self.phase = "准备中"
        self.started_at = datetime.utcnow()
        self.finished_at: datetime | None = None
        self.result: dict | None = None
        self.error: str | None = None
        self.items: list[dict] = []      # 每个账号的实时结果
        self.current: str = ""           # 正在处理的账号名（心跳）
        self.beat_at: datetime | None = None
        #: 细粒度进度：当前账号已完成的任务（用于「正在做 xxx 3/15」）。
        #: 只有账号级进度时，一个账号要跑几分钟，界面会长时间不动 ——
        #: 用户以为卡死了。这里让前端能显示到任务级。
        self.task_total: int = 0
        self.task_done: int = 0
        self.task_current: str = ""
        self._lock = threading.Lock()

    def add_item(self, item: dict) -> None:
        """记录一个账号的结果（线程安全）。"""
        with self._lock:
            self.items.append(item)
            self.done = len(self.items)
            self.current = ""
            # 换账号了：任务级计数归零，重新数
            self.task_done = 0
            self.task_current = ""
            self.task_total = 0

    def beat(self, account: str = "") -> None:
        """心跳：表示「还活着，正在处理某个账号」。

        慢账号（上游要等几秒）如果什么都不上报，前端会一直显示同一个
        进度，看起来像卡死。心跳让界面能显示「正在处理 xxx」。
        """
        with self._lock:
            self.current = account
            self.beat_at = datetime.utcnow()
            self.task_done = 0
            self.task_current = ""
            self.task_total = 0

    def task_beat(self, account: str = "", item: dict | None = None) -> None:
        """细粒度心跳：一个任务处理完了。

        没这个的话，前端在一个账号上最多要等好几分钟都看不到任何变化。
        """
        with self._lock:
            self.task_done += 1
            self.beat_at = datetime.utcnow()
            if account:
                self.current = account
            if item:
                self.task_current = str(item.get("title") or item.get("task_code") or "")
            # 预估总数：账号级总数 × 每个账号约 15 个任务，仅用于显示「已做 N 个」
            self.task_total = max(self.task_total, self.task_done)

    def set_phase(self, phase: str) -> None:
        self.phase = phase

    def snapshot(self) -> dict:
        """给前端的进度快照（不含内部字段）。"""
        elapsed = ((self.finished_at or datetime.utcnow())
                   - self.started_at).total_seconds()
        return {
            "job_id": self.id,
            "key": self.key,
            "title": self.title,
            "status": self.status,
            "phase": self.phase,
            "total": self.total,
            "done": self.done,
            "elapsed_s": round(elapsed, 1),
            "percent": int(self.done / self.total * 100) if self.total else 0,
            "current": self.current,
            "task_done": self.task_done,
            "task_current": self.task_current,
            "items": list(self.items),
            "result": self.result,
            "error": self.error,
        }


class JobRunner:
    """按 key 管理后台任务；同 key 同时只允许一个在跑。"""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Job | None:
        with self._lock:
            self._purge_locked()
            return self._jobs.get(key)

    def by_id(self, job_id: str) -> Job | None:
        with self._lock:
            self._purge_locked()
            for j in self._jobs.values():
                if j.id == job_id:
                    return j
        return None

    def is_running(self, key: str) -> bool:
        j = self.get(key)
        return bool(j and j.status == "running")

    def _purge_locked(self) -> None:
        """清掉过期的已完成任务，避免内存无限增长。"""
        now = datetime.utcnow()
        dead = [
            k for k, j in self._jobs.items()
            if j.status != "running" and j.finished_at
            and (now - j.finished_at).total_seconds() > _KEEP_DONE_SECONDS
        ]
        for k in dead:
            self._jobs.pop(k, None)

    def start(self, key: str, total: int, worker: Callable[[Job], dict],
              title: str = "") -> Job:
        """启动一个后台任务。

        worker 收到 Job，负责逐个处理并调用 job.add_item() 上报进度，
        返回值会作为 job.result 存下来。
        """
        job = Job(key, total, title=title)
        with self._lock:
            self._purge_locked()
            self._jobs[key] = job

        def _run() -> None:
            try:
                job.result = worker(job)
                job.status = "finished"
            except Exception as e:  # 后台线程里必须兜住，否则静默丢失
                job.status = "failed"
                job.error = f"{type(e).__name__}: {e}"
            finally:
                job.finished_at = datetime.utcnow()

        threading.Thread(target=_run, daemon=True,
                         name=f"wb-job-{key}").start()
        return job


#: 全局单例
RUNNER = JobRunner()


def wait_for(predicate: Callable[[], bool], timeout: float,
             interval: float = 0.4) -> bool:
    """轮询等待条件成立，最多等 timeout 秒。

    用来替代「固定 sleep N 秒」：上游快时立刻返回，慢时也不会误判。
    返回是否在超时前成立。
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            if predicate():
                return True
        except Exception:
            pass  # 查询本身出错不算命中，继续试到超时
        if time.monotonic() >= deadline:
            return False
        time.sleep(interval)
