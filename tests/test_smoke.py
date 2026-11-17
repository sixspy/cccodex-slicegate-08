import sqlite3

import pytest
from fastapi.testclient import TestClient

import app as app_module
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    app_module._SLICE_LOCKS.clear()
    init_db()
    yield


def _login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _db_rows(table):
    with sqlite3.connect(app_module.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY id")]


def _watermarks():
    """既有 watermarks 表结构不动，批次/口径从旁表 watermark_details 取。"""
    with sqlite3.connect(app_module.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(r)
            for r in conn.execute(
                """SELECT w.*, d.batch_id, d.caliber, d.consumer_name, d.previous_watermark
                   FROM watermarks w LEFT JOIN watermark_details d ON d.watermark_id=w.id
                   ORDER BY w.id"""
            )
        ]


def _releases():
    with sqlite3.connect(app_module.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(r)
            for r in conn.execute(
                """SELECT r.*, d.batch_id, d.caliber, d.consumer_name,
                          d.watermark_biz_date, d.previous_watermark
                   FROM releases r LEFT JOIN release_details d ON d.release_id=r.id
                   ORDER BY r.id"""
            )
        ]


def _new_complete_slice(c, h, slice_no, biz_date="2026-11-05"):
    r = c.post(
        "/api/slices",
        headers=h,
        json={
            "slice_no": slice_no,
            "biz_date": biz_date,
            "partition_key": "east",
            "row_count": 100,
            "expected_count": 100,
        },
    )
    assert r.status_code == 200
    return r.json()["id"]


def test_legacy_pipeline():
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "日切放行"
    h = _login(c)
    assert len(c.get("/api/slices", headers=h).json()["items"]) >= 1
    # partial cannot release
    bad = c.post("/api/slices/1/release", headers=h, json={"consumer": "x"})
    assert bad.status_code == 409
    # register complete and release
    sid = _new_complete_slice(c, h, "SL-T-1", "2026-11-04")
    rel = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    assert rel.status_code == 200
    assert any(w["source_slice"] == "SL-T-1" for w in c.get("/api/watermarks", headers=h).json()["items"])


def test_multi_jia_pushes_every_consumer_watermark():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-JIA", "2026-11-06")
    consumers = ["ads_order", "dwd_trade", "bi_sales"]
    r = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": consumers, "caliber": "甲"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "全部成功" and body["rolled_back"] is False
    assert body["batch"]["caliber"] == "甲"
    assert body["success_count"] == 3

    # 每个消费方各有独立水位，来源为同一切片，批次号一致
    wms = [w for w in _watermarks() if w["source_slice"] == "SL-M-JIA"]
    assert {w["consumer"] for w in wms} == set(consumers)
    assert len({w["id"] for w in wms}) == len(consumers)
    assert {w["batch_id"] for w in wms} == {body["batch"]["id"]}

    # 放行流水逐方成功
    details = [d for d in _releases() if d["batch_id"] == body["batch"]["id"]]
    assert {d["consumer"] for d in details} == set(consumers)
    assert all(d["status"] == "成功" for d in details)


def test_multi_jia_rolls_back_whole_batch_when_one_consumer_fails():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-JIA-BAD", "2026-11-07")
    # 一个不存在的消费方令整笔事务失败
    r = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "no_such_consumer"], "caliber": "甲"},
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == "JIA_ROLLED_BACK"
    assert body["rolled_back"] is True
    batch = body["batch"]
    assert batch["status"] == "整笔回滚" and batch["success_count"] == 0

    # 事务回滚：没有任何成功放行/水位落到其他消费方
    assert not [w for w in _watermarks() if w["source_slice"] == "SL-M-JIA-BAD"]
    details = [d for d in _releases() if d["batch_id"] == batch["id"]]
    assert details and all(d["status"] == "回滚" for d in details)
    assert not [d for d in _releases() if d["slice_id"] == sid and d["status"] == "成功"]
    sl = [s for s in _db_rows("slices") if s["id"] == sid][0]
    assert sl["release_status"] == "未放行"


def test_multi_yi_keeps_success_and_lists_failed_separately():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-YI", "2026-11-08")
    r = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "no_such_consumer"], "caliber": "乙"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["status"] == "部分成功"
    assert body["success_count"] == 1 and body["fail_count"] == 1
    assert body["batch"]["status"] == "部分成功"

    details = {d["consumer"]: d for d in _releases() if d["batch_id"] == body["batch"]["id"]}
    assert details["ads_order"]["status"] == "成功"
    assert details["no_such_consumer"]["status"] == "失败"
    # 成功方保留水位，失败方不写水位
    wms = [w for w in _watermarks() if w["source_slice"] == "SL-M-YI"]
    assert [w["consumer"] for w in wms] == ["ads_order"]

    latest = {w["code"]: w for w in c.get("/api/watermarks/summary", headers=h).json()["items"]}
    assert latest["ads_order"]["source_slice"] == "SL-M-YI"
    assert latest["dwd_trade"]["source_slice"] is None


def test_partial_success_is_never_reported_as_all_success():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-YI-2", "2026-11-09")
    body = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "ghost"], "caliber": "乙"},
    ).json()
    assert body["all_success"] is False
    assert body["batch"]["status"] != "全部成功"
    batch_listed = c.get("/api/release-batches", headers=h).json()["items"]
    assert all(b["status"] != "全部成功" for b in batch_listed if b["id"] == body["batch"]["id"])


def test_caliber_locked_per_biz_date_and_deviation_recorded():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-LOCK", "2026-11-10")
    ok = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order"], "caliber": "甲"},
    )
    assert ok.status_code == 200, ok.text

    pol = c.get("/api/release-policy?biz_date=2026-11-10", headers=h).json()
    assert pol["chosen"]["caliber"] == "甲"

    sid2 = _new_complete_slice(c, h, "SL-M-LOCK-2", "2026-11-10")
    bad = c.post(
        f"/api/slices/{sid2}/multi-release",
        headers=h,
        json={"consumers": ["dwd_trade"], "caliber": "乙"},
    )
    assert bad.status_code == 409
    assert bad.json()["code"] == "CALIBER_DEVIATION"

    incidents = c.get("/api/release-incidents", headers=h).json()["items"]
    assert any(i["kind"] == "多消费口径偏了" for i in incidents)
    # 违规请求不得落任何成功水位
    assert not [w for w in _watermarks() if w["source_slice"] == "SL-M-LOCK-2"]


def test_caliber_required_for_multi_release():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-NOCAL", "2026-11-11")
    r = c.post(f"/api/slices/{sid}/multi-release", headers=h, json={"consumers": ["ads_order"]})
    assert r.status_code == 400
    assert r.json()["code"] == "CALIBER_REQUIRED"


def test_contended_release_is_recorded_and_not_committed():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-LOCKED", "2026-11-12")
    lock = app_module._thread_lock(sid)
    assert lock.acquire(blocking=False)
    try:
        r = c.post(
            f"/api/slices/{sid}/multi-release",
            headers=h,
            json={"consumers": ["ads_order", "dwd_trade"], "caliber": "甲"},
        )
    finally:
        lock.release()
    assert r.status_code == 409
    assert r.json()["code"] == "CONTENDED"
    incidents = c.get("/api/release-incidents", headers=h).json()["items"]
    assert any(i["kind"] == "多消费争用" for i in incidents)
    # 后到请求不得默成
    assert not [w for w in _watermarks() if w["source_slice"] == "SL-M-LOCKED"]
    assert not [b for b in _db_rows("release_batches") if b["slice_no"] == "SL-M-LOCKED"]


def test_cross_connection_slice_lock_is_contended():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-XLOCK", "2026-11-13")
    with sqlite3.connect(app_module.DB_PATH) as conn:
        conn.execute(
            "INSERT INTO slice_locks(slice_id,owner,acquired_at) VALUES (?,?,?)",
            (sid, "other-shift", str(int(__import__("time").time()))),
        )
        conn.commit()
    r = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order"], "caliber": "乙"},
    )
    assert r.status_code == 409
    assert r.json()["code"] == "CONTENDED"
    assert not [w for w in _watermarks() if w["source_slice"] == "SL-M-XLOCK"]


def test_rerun_for_already_released_consumer_does_not_duplicate_watermark():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-RERUN", "2026-11-19")
    first = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "ghost"], "caliber": "乙"},
    ).json()
    assert first["success_count"] == 1

    # 补跑：已成功方不得重复推进水位，失败方仍可补
    second = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "dwd_trade"], "caliber": "乙"},
    ).json()
    assert second["success_count"] == 1 and second["fail_count"] == 1
    detail = {d["consumer"]: d for d in second["batch"]["items"]}
    assert detail["ads_order"]["status"] == "失败"
    assert "已在本切片成功放行" in detail["ads_order"]["note"]
    assert detail["dwd_trade"]["status"] == "成功"
    wms = [w for w in _watermarks() if w["source_slice"] == "SL-M-RERUN"]
    assert sorted(w["consumer"] for w in wms) == ["ads_order", "dwd_trade"]


def test_duplicate_consumer_list_rejected():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-DUP", "2026-11-14")
    r = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "ads_order"], "caliber": "甲"},
    )
    assert r.status_code == 400
    assert r.json()["code"] == "DUPLICATE_CONSUMERS"


def test_watermark_regression_blocks_and_is_listed():
    c = TestClient(app)
    h = _login(c)
    # ads_order 已有 2026-11-01 水位；放一个更早业务日的切片应被拦下
    sid = _new_complete_slice(c, h, "SL-M-OLD", "2026-10-30")
    r = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "dwd_trade"], "caliber": "乙"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "部分成功"
    details = {d["consumer"]: d for d in _releases() if d["batch_id"] == body["batch"]["id"]}
    assert details["ads_order"]["status"] == "失败"
    assert details["dwd_trade"]["status"] == "成功"
    assert "水位回拨" in details["ads_order"]["note"]


def test_directory_release_views_are_consistent():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-CONSIST", "2026-11-15")
    consumers = ["ads_order", "dwd_trade", "bi_sales", "risk_alert"]
    body = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": consumers, "caliber": "甲"},
    ).json()

    directory = {x["code"]: x for x in c.get("/api/consumers", headers=h).json()["items"]}
    policy = c.get("/api/release-policy?biz_date=2026-11-15", headers=h).json()
    summary = {x["code"]: x for x in c.get("/api/watermarks/summary", headers=h).json()["items"]}
    detail = c.get(f"/api/release-batches/{body['batch']['batch_no']}", headers=h).json()["item"]
    wm_rows = [w for w in c.get("/api/watermarks", headers=h).json()["items"] if w["source_slice"] == "SL-M-CONSIST"]

    # 目录、策略旁注、水位页、放行详情同源
    assert set(policy["consumers"][0].keys()) >= {"code", "name", "status"}
    assert {x["code"] for x in policy["consumers"]} <= set(directory)
    for code in consumers:
        assert summary[code]["source_slice"] == "SL-M-CONSIST"
        assert summary[code]["batch_id"] == body["batch"]["id"]
        assert summary[code]["caliber"] == "甲"
        assert code in directory
    assert {d["consumer"] for d in detail["items"]} == set(consumers)
    assert {w["consumer"] for w in wm_rows} == set(consumers)
    assert len({w["id"] for w in wm_rows}) == len(consumers)
    # 水位画面与放行详情的水位值一致
    detail_wm = {d["consumer"]: d["watermark_biz_date"] + "T23:59:59" for d in detail["items"]}
    for w in wm_rows:
        assert detail_wm[w["consumer"]] == w["watermark"]


def test_shared_single_watermark_cannot_impersonate_multi():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-SHARE", "2026-11-16")
    consumers = ["ads_order", "dwd_trade"]
    body = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": consumers, "caliber": "甲"},
    ).json()
    wms = [w for w in _watermarks() if w["source_slice"] == "SL-M-SHARE"]
    assert len(wms) == len(consumers)
    assert len({w["consumer"] for w in wms}) == len(consumers)
    # 每个水位指向各自消费方，不能出现一条水位被两个消费方共用
    assert len({w["id"] for w in wms}) == len(wms)
    assert {w["batch_id"] for w in wms} == {body["batch"]["id"]}


def test_jia_mid_write_failure_rolls_back_everything(monkeypatch):
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-JIA-MID", "2026-11-17")
    original = app_module._apply_success
    seen = {"n": 0}

    def flaky(conn, **kwargs):
        seen["n"] += 1
        if seen["n"] == 2:
            raise RuntimeError("模拟第二方写水位失败")
        return original(conn, **kwargs)

    monkeypatch.setattr(app_module, "_apply_success", flaky)
    r = c.post(
        f"/api/slices/{sid}/multi-release",
        headers=h,
        json={"consumers": ["ads_order", "dwd_trade", "bi_sales"], "caliber": "甲"},
    )
    assert r.status_code == 409, r.text
    assert r.json()["code"] == "JIA_ROLLED_BACK"

    # 第一方已经写下的放行与水位必须被整笔回滚
    assert not [w for w in _watermarks() if w["source_slice"] == "SL-M-JIA-MID"]
    assert not [
        d for d in _releases() if d["slice_id"] == sid and d["status"] == "成功"
    ]
    sl = [s for s in _db_rows("slices") if s["id"] == sid][0]
    assert sl["release_status"] == "未放行"
    batch = [b for b in _db_rows("release_batches") if b["slice_no"] == "SL-M-JIA-MID"][0]
    assert batch["status"] == "整笔回滚" and batch["success_count"] == 0


def test_legacy_single_release_still_works_after_multiday_lock():
    c = TestClient(app)
    h = _login(c)
    sid = _new_complete_slice(c, h, "SL-M-LEGACY", "2026-11-18")
    multi = _new_complete_slice(c, h, "SL-M-LEGACY-2", "2026-11-18")
    assert c.post(
        f"/api/slices/{multi}/multi-release",
        headers=h,
        json={"consumers": ["ads_order"], "caliber": "甲"},
    ).status_code == 200
    # 单方放行接口保持原有行为，不受多消费口径锁定影响
    single = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "bi_sales"})
    assert single.status_code == 200
    assert single.json()["status"] == "全部成功"
    assert any(w["source_slice"] == "SL-M-LEGACY" for w in _watermarks())
