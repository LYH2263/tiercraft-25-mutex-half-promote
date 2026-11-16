"""互斥裁决证明实体（mutex_verdicts）。

每次互斥裁决都在此表留证，字段覆盖验收要求：
- 在途类型：本次申请持锁的类型 request_lock_type；
- 冲突对端：对端在途类型 conflict_peer_type 与对端任务编号 conflict_peer_job_id；
- 是否待重跑：needs_rerun。

两类裁决：
- VERDICT_CONFLICT（互斥拒绝）：对端在途，本次请求未启动，无需重跑（needs_rerun=0）；
- VERDICT_RERUN（失败待重跑）：任务启动后失败，本趟变更已回滚，主题域不可用待重跑（needs_rerun=1）。

证明实体只增不改，是可查询的持久事实，不依赖进程内状态，刷新页面或重启后仍在。
"""
from __future__ import annotations

import sqlite3

VERDICT_CONFLICT = "mutex_conflict"
VERDICT_RERUN = "needs_rerun"

VERDICT_LABELS = {
    VERDICT_CONFLICT: "互斥拒绝",
    VERDICT_RERUN: "失败待重跑",
}


def insert_verdict(
    conn: sqlite3.Connection,
    *,
    domain_id: int,
    job_id: int,
    verdict_type: str,
    request_lock_type: str,
    conflict_peer_type: str = "",
    conflict_peer_job_id: int | None = None,
    needs_rerun: bool = False,
    reason: str = "",
) -> int:
    cur = conn.execute(
        """
        INSERT INTO mutex_verdicts(
          domain_id, job_id, verdict_type, request_lock_type,
          conflict_peer_type, conflict_peer_job_id, needs_rerun, reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            domain_id,
            job_id,
            verdict_type,
            request_lock_type,
            conflict_peer_type,
            conflict_peer_job_id,
            1 if needs_rerun else 0,
            reason,
        ),
    )
    return int(cur.lastrowid)


def _decorate(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    # 延迟导入避免 locks ↔ verdicts 循环依赖
    from app.locks import LOCK_LABELS

    d = dict(row)
    d["verdict_type_label"] = VERDICT_LABELS.get(d["verdict_type"], d["verdict_type"])
    d["request_lock_label"] = LOCK_LABELS.get(d["request_lock_type"], d["request_lock_type"])
    d["conflict_peer_label"] = LOCK_LABELS.get(
        d["conflict_peer_type"], d["conflict_peer_type"]
    )
    d["needs_rerun"] = bool(d["needs_rerun"])
    return d


def get_verdict(conn: sqlite3.Connection, verdict_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM mutex_verdicts WHERE id=?", (verdict_id,)
    ).fetchone()
    return _decorate(conn, row) if row else None


def list_verdicts(
    conn: sqlite3.Connection, job_id: int | None = None
) -> list[dict]:
    sql = "SELECT * FROM mutex_verdicts"
    args: tuple = ()
    if job_id is not None:
        sql += " WHERE job_id=?"
        args = (job_id,)
    sql += " ORDER BY id"
    return [_decorate(conn, r) for r in conn.execute(sql, args).fetchall()]
