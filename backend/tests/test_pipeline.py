import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import db, seed
from app.db import connect


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("FIXTURES_DIR", str(REPO_ROOT / "fixtures"))
    monkeypatch.setattr(db, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "data" / "app.db")
    monkeypatch.setattr(db, "FIXTURES_DIR", REPO_ROOT / "fixtures")
    monkeypatch.setattr(seed, "FIXTURES_DIR", REPO_ROOT / "fixtures")
    seed.init_db()
    from app.main import app

    with TestClient(app) as c:
        yield c


def insert_inflight_lock(domain_id: int, lock_type: str, step: str) -> int:
    """直接构造一条已提交的「在途任务 + 在途锁」，模拟对端任务在途。"""
    conn = connect()
    try:
        cur = conn.execute(
            "INSERT INTO layer_jobs(domain_id, step, status, started_at) "
            "VALUES (?, ?, '在途', datetime('now'))",
            (domain_id, step),
        )
        job_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO domain_locks(domain_id, lock_type, job_id) VALUES (?, ?, ?)",
            (domain_id, lock_type, job_id),
        )
        conn.commit()
        return job_id
    finally:
        conn.close()


def release_inflight_lock(job_id: int, status: str = "成功") -> None:
    conn = connect()
    try:
        conn.execute("DELETE FROM domain_locks WHERE job_id=?", (job_id,))
        conn.execute(
            "UPDATE layer_jobs SET status=?, finished_at=datetime('now') WHERE id=?",
            (status, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def build_baseline(client: TestClient) -> None:
    """跑通主链，得到对齐基线：6 贴源行、5 明细行、4 汇总键。"""
    r = client.post("/api/jobs/paste-to-detail", json={"domain_id": 1})
    assert r.status_code == 200, r.text
    r = client.post("/api/jobs/detail-to-summary", json={"domain_id": 1})
    assert r.status_code == 200, r.text
    rows = client.get("/api/layers/detail").json()
    summary = client.get("/api/layers/summary").json()
    assert len(rows) == 5
    assert len(summary) == 4
    assert sum(s["line_count"] for s in summary) == 5


# ---------------------------------------------------------------------------
# 基础主链
# ---------------------------------------------------------------------------
def test_basic_chain_and_reconcile(client):
    build_baseline(client)
    r = client.post("/api/reconcile", json={"domain_id": 1})
    assert r.status_code == 200
    assert r.json()["status"] == "一致"


def test_detail_without_summary_is_marked_not_aligned(client):
    # 只跑明细不跑汇总：绝不允许展示成已对齐
    r = client.post("/api/jobs/paste-to-detail", json={"domain_id": 1})
    assert r.status_code == 200
    domains = client.get("/api/domains").json()
    assert domains[0]["alignment"] == "未对齐"
    assert domains[0]["available"] == 1


# ---------------------------------------------------------------------------
# 在途锁互斥
# ---------------------------------------------------------------------------
def test_conflict_fails_with_holder_type_and_is_persisted(client):
    holder_job = insert_inflight_lock(1, "backfill", "backfill")
    for url in (
        "/api/jobs/paste-to-detail",
        "/api/jobs/detail-to-summary",
    ):
        r = client.post(url, json={"domain_id": 1})
        assert r.status_code == 409
        detail = r.json()["detail"]
        assert detail["对端在途类型"] == "定点回刷"
        assert detail["对端任务编号"] == holder_job
        assert "在途" in detail["原因"]

    # 回刷在途时，回刷自身也互斥
    r = client.post("/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2026-11-10"})
    assert r.status_code == 409

    # 互斥失败落库，刷新后仍可查，且锁仍归对端持有
    jobs = client.get("/api/jobs?domain_id=1").json()
    failed = [j for j in jobs if j["status"] == "互斥失败"]
    assert len(failed) == 3
    for j in failed:
        assert j["conflict_holder_type"] == "backfill"
        assert j["conflict_holder_job_id"] == holder_job
        assert "定点回刷" in j["message"]
    locks = client.get("/api/locks?domain_id=1").json()
    assert len(locks) == 1 and locks[0]["job_id"] == holder_job


def test_priority_both_directions(client):
    # 提升在途时，回刷与另一类提升都被拒绝
    holder = insert_inflight_lock(1, "paste_to_detail", "paste_to_detail")
    r = client.post("/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2026-11-10"})
    assert r.status_code == 409
    assert r.json()["detail"]["对端在途类型"] == "提升·贴源到明细"
    r = client.post("/api/jobs/detail-to-summary", json={"domain_id": 1})
    assert r.status_code == 409
    assert r.json()["detail"]["对端任务编号"] == holder

    release_inflight_lock(holder)
    r = client.post("/api/jobs/detail-to-summary", json={"domain_id": 1})
    assert r.status_code == 200


def test_real_concurrency_holder_blocks_peer(client, monkeypatch):
    from app.services import pipeline

    reached = threading.Event()
    proceed = threading.Event()

    def blocking_hook(conn, domain_id, biz_day):
        reached.set()
        if not proceed.wait(timeout=5):
            raise RuntimeError("测试等待超时")

    monkeypatch.setattr(pipeline, "detail_phase_hook", blocking_hook)

    holder_error = []

    def worker():
        try:
            pipeline.run_paste_to_detail(1)
        except Exception as exc:  # noqa: BLE001
            holder_error.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    assert reached.wait(5)

    # 在途锁可查询，且在途任务同源落库
    locks = client.get("/api/locks?domain_id=1").json()
    assert len(locks) == 1
    assert locks[0]["lock_type_label"] == "提升·贴源到明细"
    inflight = [j for j in client.get("/api/jobs").json() if j["in_flight"]]
    assert len(inflight) == 1 and inflight[0]["status"] == "在途"

    # 对端并发请求：先短暂等待后放行持锁任务，冲突仍须失败留痕
    timer = threading.Timer(0.5, proceed.set)
    timer.start()
    r = client.post("/api/jobs/detail-to-summary", json={"domain_id": 1})
    timer.join()
    t.join(timeout=5)
    assert holder_error == []
    assert r.status_code == 409
    assert r.json()["detail"]["对端在途类型"] == "提升·贴源到明细"

    # 持锁任务正常成功，锁释放
    assert client.get("/api/locks?domain_id=1").json() == []
    jobs = client.get("/api/jobs?domain_id=1").json()
    ok_rows = [j for j in jobs if j["status"] == "成功"]
    assert any(j["step"] == "paste_to_detail" for j in ok_rows)
    conflict_rows = [j for j in jobs if j["status"] == "互斥失败"]
    assert len(conflict_rows) == 1


# ---------------------------------------------------------------------------
# 失败不留可见半态
# ---------------------------------------------------------------------------
def test_detail_failure_rolls_back_and_marks_unavailable(client, monkeypatch):
    build_baseline(client)
    from app.services import pipeline

    before = client.get("/api/layers/detail").json()

    def boom(conn, domain_id, biz_day):
        raise RuntimeError("模拟明细清洗阶段失败")

    monkeypatch.setattr(pipeline, "detail_phase_hook", boom)
    r = client.post("/api/jobs/paste-to-detail", json={"domain_id": 1})
    assert r.status_code == 200
    job = r.json()
    assert job["status"] == "失败"
    assert "回滚" in job["message"]

    # 明细未出现半成品，锁已释放，主题域不可用待重跑
    assert client.get("/api/layers/detail").json() == before
    assert client.get("/api/locks").json() == []
    domain = client.get("/api/domains").json()[0]
    assert domain["available"] == 0
    assert "不可用待重跑" in domain["unavailable_reason"]

    report = client.post("/api/reconcile", json={"domain_id": 1}).json()
    assert report["status"] == "主题域不可用待重跑"

    # 重跑成功后恢复可用（只复位故障钩子，保留临时库路径）
    monkeypatch.setattr(pipeline, "detail_phase_hook", lambda conn, domain_id, biz_day: None)
    r = client.post("/api/jobs/paste-to-detail", json={"domain_id": 1})
    assert r.json()["status"] == "成功"
    assert client.get("/api/domains").json()[0]["available"] == 1


def test_summary_failure_keeps_old_summary_and_marks_unavailable(client, monkeypatch):
    build_baseline(client)
    from app.services import pipeline

    before_summary = client.get("/api/layers/summary").json()
    before_detail = client.get("/api/layers/detail").json()

    def boom(conn, domain_id, biz_day):
        raise RuntimeError("模拟汇总聚合阶段失败")

    monkeypatch.setattr(pipeline, "summary_phase_hook", boom)
    r = client.post("/api/jobs/detail-to-summary", json={"domain_id": 1})
    assert r.json()["status"] == "失败"

    # 不允许出现「新明细旧汇总却显示成功」：汇总保持旧值且任务明确失败、域不可用
    assert client.get("/api/layers/summary").json() == before_summary
    assert client.get("/api/layers/detail").json() == before_detail
    assert client.get("/api/locks").json() == []
    assert client.get("/api/domains").json()[0]["available"] == 0


# ---------------------------------------------------------------------------
# 定点回刷
# ---------------------------------------------------------------------------
def test_backfill_rebuilds_one_day_only(client):
    build_baseline(client)
    # 给 2026-11-10 追加一条贴源数据
    conn = connect()
    conn.execute(
        "INSERT INTO layer_paste(domain_id, biz_day, store_code, store_name, category, amount, qty, raw_json) "
        "VALUES (1, '2026-11-10', 'S09', '新城店', '饮料', 33.3, 2, '{}')"
    )
    conn.commit()
    conn.close()

    r = client.post("/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2026-11-10"})
    assert r.status_code == 200, r.text
    job = r.json()
    assert job["status"] == "成功"
    assert job["target_biz_day"] == "2026-11-10"

    detail = client.get("/api/layers/detail").json()
    day_rows = [d for d in detail if d["biz_day"] == "2026-11-10"]
    other_rows = [d for d in detail if d["biz_day"] != "2026-11-10"]
    assert any(d["store_code"] == "S09" for d in day_rows)
    assert len(other_rows) == 2  # 2026-11-11 两个门店不受影响

    summary = client.get("/api/layers/summary").json()
    day_keys = [s for s in summary if s["biz_day"] == "2026-11-10"]
    assert {s["store_code"] for s in day_keys} == {"S01", "S02", "S09"}
    assert sum(s["line_count"] for s in summary) == len(detail)

    report = client.post("/api/reconcile", json={"domain_id": 1}).json()
    assert report["status"] == "一致"


def test_backfill_failure_rolls_back_both_layers(client, monkeypatch):
    build_baseline(client)
    from app.services import pipeline

    before_detail = client.get("/api/layers/detail").json()
    before_summary = client.get("/api/layers/summary").json()

    def boom(conn, domain_id, biz_day):
        if biz_day == "2026-11-10":
            raise RuntimeError("模拟回刷明细阶段失败")

    monkeypatch.setattr(pipeline, "detail_phase_hook", boom)
    r = client.post("/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2026-11-10"})
    assert r.json()["status"] == "失败"
    assert client.get("/api/layers/detail").json() == before_detail
    assert client.get("/api/layers/summary").json() == before_summary
    assert client.get("/api/locks").json() == []
    assert client.get("/api/domains").json()[0]["available"] == 0


def test_backfill_validation(client):
    r = client.post("/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2026/11/10"})
    assert r.status_code == 400
    r = client.post("/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2030-01-01"})
    assert r.status_code == 400
    r = client.post("/api/jobs/backfill", json={"domain_id": 999, "biz_day": "2026-11-10"})
    assert r.status_code == 400


def test_backfill_holds_lock_and_mutual_exclusion(client, monkeypatch):
    from app.services import pipeline

    reached = threading.Event()
    proceed = threading.Event()

    def blocking_hook(conn, domain_id, biz_day):
        reached.set()
        if not proceed.wait(5):
            raise RuntimeError("测试等待超时")

    monkeypatch.setattr(pipeline, "detail_phase_hook", blocking_hook)
    t = threading.Thread(target=pipeline.run_backfill, args=(1, "2026-11-10"))
    t.start()
    assert reached.wait(5)

    locks = client.get("/api/locks").json()
    assert locks[0]["lock_type_label"] == "定点回刷"
    r = client.post("/api/jobs/paste-to-detail", json={"domain_id": 1})
    assert r.status_code == 409
    assert r.json()["detail"]["对端在途类型"] == "定点回刷"

    proceed.set()
    t.join(5)
    assert client.get("/api/locks").json() == []


# ---------------------------------------------------------------------------
# 重启回收
# ---------------------------------------------------------------------------
def test_stale_lock_recovered_on_restart(client):
    holder = insert_inflight_lock(1, "detail_to_summary", "detail_to_summary")
    seed.init_db()  # 模拟进程重启

    assert client.get("/api/locks").json() == []
    job = next(j for j in client.get("/api/jobs").json() if j["id"] == holder)
    assert job["status"] == "失败"
    assert "进程中断" in job["message"]
    domain = client.get("/api/domains").json()[0]
    assert domain["available"] == 0


def test_dashboard_exposes_inflight_and_domain_state(client):
    build_baseline(client)
    insert_inflight_lock(1, "backfill", "backfill")
    data = client.get("/api/dashboard").json()
    assert len(data["在途任务"]) == 1
    assert data["在途任务"][0]["在途类型"] == "定点回刷"
    assert data["主题域"][0]["在途类型"] == "定点回刷"


# ---------------------------------------------------------------------------
# 互斥裁决证明实体（在途类型 / 冲突对端 / 是否待重跑）
# ---------------------------------------------------------------------------
def test_conflict_creates_mutex_verdict_with_peer_and_readonly_apis(client):
    holder_job = insert_inflight_lock(1, "backfill", "backfill")
    r = client.post("/api/jobs/paste-to-detail", json={"domain_id": 1})
    assert r.status_code == 409
    body = r.json()["detail"]
    assert body["对端在途类型"] == "定点回刷"
    verdict_id = body["互斥裁决证明编号"]
    conflict_job_id = body["互斥失败任务编号"]

    # 只读接口：按证明 id 查询
    verdict = client.get(f"/api/mutex-verdicts/{verdict_id}").json()
    assert verdict["request_lock_type"] == "paste_to_detail"
    assert verdict["request_lock_label"] == "提升·贴源到明细"
    assert verdict["conflict_peer_type"] == "backfill"
    assert verdict["conflict_peer_label"] == "定点回刷"
    assert verdict["conflict_peer_job_id"] == holder_job
    assert verdict["needs_rerun"] is False  # 互斥拒绝：任务未启动，无需重跑
    assert verdict["verdict_type_label"] == "互斥拒绝"

    # 只读接口：按任务 id 查询（列表过滤 + 任务子资源两种路径）
    by_job = client.get(f"/api/mutex-verdicts?job_id={conflict_job_id}").json()
    assert [v["id"] for v in by_job] == [verdict_id]
    sub = client.get(f"/api/jobs/{conflict_job_id}/mutex-verdicts").json()
    assert [v["id"] for v in sub] == [verdict_id]

    # 任务详情内嵌证明，刷新后仍在
    job = client.get(f"/api/jobs/{conflict_job_id}").json()
    assert job["status"] == "互斥失败"
    assert job["mutex_verdicts"][0]["id"] == verdict_id
    assert job["mutex_verdicts"][0]["conflict_peer_job_id"] == holder_job

    # 只读：证明实体只增不改，未知 id 返回 404
    assert client.get("/api/mutex-verdicts/99999").status_code == 404
    assert client.get("/api/jobs/99999/mutex-verdicts").status_code == 404


def test_failed_job_verdict_marks_needs_rerun(client, monkeypatch):
    build_baseline(client)
    from app.services import pipeline

    def boom(conn, domain_id, biz_day):
        raise RuntimeError("模拟汇总聚合阶段失败")

    monkeypatch.setattr(pipeline, "summary_phase_hook", boom)
    r = client.post("/api/jobs/detail-to-summary", json={"domain_id": 1})
    job = r.json()
    assert job["status"] == "失败"

    verdicts = client.get(f"/api/jobs/{job['id']}/mutex-verdicts").json()
    assert len(verdicts) == 1
    v = verdicts[0]
    assert v["verdict_type"] == "needs_rerun"
    assert v["verdict_type_label"] == "失败待重跑"
    assert v["request_lock_type"] == "detail_to_summary"
    assert v["needs_rerun"] is True
    assert "回滚" in v["reason"]

    # 列表接口同样可查，且主题域确实不可用待重跑
    all_verdicts = client.get("/api/mutex-verdicts").json()
    assert any(x["id"] == v["id"] and x["needs_rerun"] is True for x in all_verdicts)
    assert client.get("/api/domains").json()[0]["available"] == 0


def test_backfill_summary_failure_rolls_back_detail_and_marks_rerun(
    client, monkeypatch
):
    """明细成功后汇总失败：不得留下「明细已变、汇总未变」的可见半态。"""
    build_baseline(client)
    from app.services import pipeline

    conn = connect()
    conn.execute(
        "INSERT INTO layer_paste(domain_id, biz_day, store_code, store_name, category, amount, qty, raw_json) "
        "VALUES (1, '2026-11-10', 'S09', '新城店', '饮料', 33.3, 2, '{}')"
    )
    conn.commit()
    conn.close()

    before_detail = client.get("/api/layers/detail").json()
    before_summary = client.get("/api/layers/summary").json()

    def boom(conn, domain_id, biz_day):
        if biz_day == "2026-11-10":
            raise RuntimeError("模拟回刷汇总阶段失败")

    monkeypatch.setattr(pipeline, "summary_phase_hook", boom)
    r = client.post(
        "/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2026-11-10"}
    )
    job = r.json()
    assert job["status"] == "失败"

    # 明细与汇总都回到本趟之前，无可见半态
    assert client.get("/api/layers/detail").json() == before_detail
    assert client.get("/api/layers/summary").json() == before_summary
    assert client.get("/api/locks").json() == []
    domain = client.get("/api/domains").json()[0]
    assert domain["available"] == 0

    # 裁决证明写明：回刷在途类型、待重跑
    verdicts = client.get(f"/api/jobs/{job['id']}/mutex-verdicts").json()
    assert len(verdicts) == 1
    assert verdicts[0]["request_lock_type"] == "backfill"
    assert verdicts[0]["needs_rerun"] is True


def test_backfill_success_recomputes_summary_in_same_trip(client):
    """成功回刷：明细与汇总同趟更新，任务成功且域保持可用。"""
    build_baseline(client)
    conn = connect()
    conn.execute(
        "INSERT INTO layer_paste(domain_id, biz_day, store_code, store_name, category, amount, qty, raw_json) "
        "VALUES (1, '2026-11-10', 'S09', '新城店', '饮料', 33.3, 2, '{}')"
    )
    conn.commit()
    conn.close()

    r = client.post(
        "/api/jobs/backfill", json={"domain_id": 1, "biz_day": "2026-11-10"}
    )
    job = r.json()
    assert job["status"] == "成功"
    assert job["out_count"] >= 1  # 汇总键真实重算，不再恒为 0

    summary = client.get("/api/layers/summary").json()
    day_keys = [s for s in summary if s["biz_day"] == "2026-11-10"]
    assert {s["store_code"] for s in day_keys} == {"S01", "S02", "S09"}
    detail = client.get("/api/layers/detail").json()
    assert sum(s["line_count"] for s in summary) == len(detail)
    # 成功任务没有失败类裁决证明
    assert client.get(f"/api/jobs/{job['id']}/mutex-verdicts").json() == []
    assert client.get("/api/domains").json()[0]["available"] == 1


def test_stale_lock_recovery_leaves_rerun_verdict(client):
    holder = insert_inflight_lock(1, "detail_to_summary", "detail_to_summary")
    seed.init_db()  # 模拟进程重启

    verdicts = client.get(f"/api/jobs/{holder}/mutex-verdicts").json()
    assert len(verdicts) == 1
    v = verdicts[0]
    assert v["verdict_type"] == "needs_rerun"
    assert v["request_lock_type"] == "detail_to_summary"
    assert v["needs_rerun"] is True
    assert client.get("/api/domains").json()[0]["available"] == 0


def test_backend_rejects_concurrent_writes_without_frontend(client, monkeypatch):
    """绕过界面直接并发调接口：后端在途锁仍然裁决，不依赖按钮禁用。

    SQLite 同一时刻只允许一个写事务，互斥拒绝记录的写入会在忙等待窗口内排队，
    因此沿用 test_real_concurrency_holder_blocks_peer 的既定模式：
    持锁任务短暂在途期间连发一个直连接口请求，必须由后端裁决为 409。
    """
    from app.services import pipeline

    reached = threading.Event()
    proceed = threading.Event()

    def blocking_hook(conn, domain_id, biz_day):
        reached.set()
        if not proceed.wait(timeout=5):
            raise RuntimeError("测试等待超时")

    monkeypatch.setattr(pipeline, "detail_phase_hook", blocking_hook)
    t = threading.Thread(target=pipeline.run_backfill, args=(1, "2026-11-10"))
    t.start()
    assert reached.wait(5)

    # 在途锁可查询：回刷持锁中
    locks = client.get("/api/locks?domain_id=1").json()
    assert len(locks) == 1 and locks[0]["lock_type"] == "backfill"

    # 绕过前端、直接打后端接口：两类提升与回刷自身均须被拒
    timer = threading.Timer(0.3, proceed.set)
    timer.start()
    r = client.post("/api/jobs/paste-to-detail", json={"domain_id": 1})
    timer.join()
    t.join(10)
    assert r.status_code == 409
    assert r.json()["detail"]["对端在途类型"] == "定点回刷"

    # 裁决证明：申请的在途类型、冲突对端（定点回刷）、无需重跑
    verdict_id = r.json()["detail"]["互斥裁决证明编号"]
    verdict = client.get(f"/api/mutex-verdicts/{verdict_id}").json()
    assert verdict["request_lock_type"] == "paste_to_detail"
    assert verdict["conflict_peer_type"] == "backfill"
    assert verdict["conflict_peer_label"] == "定点回刷"
    assert verdict["needs_rerun"] is False

    # 锁随持锁任务正常结束而释放，冲突未留下半态
    assert client.get("/api/locks?domain_id=1").json() == []
    failed = [
        j
        for j in client.get("/api/jobs?domain_id=1").json()
        if j["status"] == "互斥失败"
    ]
    assert len(failed) == 1
    assert failed[0]["mutex_verdicts"][0]["id"] == verdict_id
