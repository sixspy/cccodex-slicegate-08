import hashlib
import json
import secrets
import sqlite3
import threading
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

# 多消费口径只有甲乙两种。口径一旦在某个业务日选定，就写入多消费提交簿，
# 当日所有多消费放行都必须沿用，不能顺手在甲乙之间来回切换。
MODE_DEFS = [
    {
        "code": "甲",
        "label": "甲 · 整笔事务",
        "summary": "同一事务内为清单全部消费方写成功放行并各自推进水位",
        "failure_rule": "任一方失败整笔回滚",
        "rollback_on_partial": True,
    },
    {
        "code": "乙",
        "label": "乙 · 逐方事务",
        "summary": "逐消费方独立事务，成功方保留，失败方单列",
        "failure_rule": "失败方单列，整单禁止标成全成功",
        "rollback_on_partial": False,
    },
]
MODE_MAP = {m["code"]: m for m in MODE_DEFS}
CALIBER_ALIASES = {
    "甲": "甲",
    "甲口径": "甲",
    "A": "甲",
    "JIA": "甲",
    "ATOMIC": "甲",
    "ALL_OR_NOTHING": "甲",
    "乙": "乙",
    "乙口径": "乙",
    "B": "乙",
    "YI": "乙",
    "PER_CONSUMER": "乙",
    "INDEPENDENT": "乙",
}
LOCK_STALE_SECONDS = 120

# 进程内按切片的闸。数据库 slice_locks 负责跨连接/跨进程互斥；
# 这把锁负责让同一瞬间落笔的第二个值班立刻观测到在途锁，而不是排完队默成。
_SLICE_LOCKS: dict[int, threading.Lock] = {}
_SLICE_LOCKS_GUARD = threading.Lock()


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


@contextmanager
def tx(conn):
    """显式提交边界。多消费提交只能从这里走，逐条自动提交不算整笔事务。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def normalize_caliber(value: Any) -> str | None:
    if value is None:
        return None
    key = str(value).strip().upper()
    return CALIBER_ALIASES.get(key)


class ReleaseRejected(Exception):
    def __init__(self, status_code: int, message: str, **extra):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.extra = extra


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


def ensure_consumers(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS consumers(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        contact TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT '启用',
        sort_order INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )""")
    if conn.execute("SELECT COUNT(*) c FROM consumers").fetchone()["c"] == 0:
        rows = [
            ("ads_order", "订单宽表", "ads@local", "启用", 10),
            ("dwd_trade", "交易明细层", "dwd@local", "启用", 20),
            ("bi_sales", "销售日报集市", "bi@local", "启用", 30),
            ("risk_alert", "风控告警流", "risk@local", "启用", 40),
        ]
        for code, name, contact, status, order in rows:
            conn.execute(
                "INSERT INTO consumers(code,name,contact,status,sort_order,created_at) VALUES (?,?,?,?,?,?)",
                (code, name, contact, status, order, now_iso()),
            )


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


app = FastAPI(title=PRODUCT)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def init_db():
    with closing(connect()) as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS release_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_no TEXT UNIQUE NOT NULL,
          slice_id INTEGER NOT NULL,
          slice_no TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          caliber TEXT NOT NULL,
          consumers TEXT NOT NULL,
          status TEXT NOT NULL,
          success_count INTEGER NOT NULL DEFAULT 0,
          fail_count INTEGER NOT NULL DEFAULT 0,
          operator TEXT NOT NULL DEFAULT '',
          note TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS caliber_choices(
          biz_date TEXT PRIMARY KEY,
          caliber TEXT NOT NULL,
          batch_no TEXT NOT NULL,
          operator TEXT NOT NULL,
          chosen_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS release_incidents(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          slice_id INTEGER NOT NULL DEFAULT 0,
          slice_no TEXT NOT NULL DEFAULT '',
          biz_date TEXT NOT NULL DEFAULT '',
          batch_no TEXT NOT NULL DEFAULT '',
          caliber TEXT NOT NULL DEFAULT '',
          consumers TEXT NOT NULL DEFAULT '[]',
          operator TEXT NOT NULL DEFAULT '',
          detail TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS slice_locks(
          slice_id INTEGER PRIMARY KEY,
          owner TEXT NOT NULL,
          acquired_at TEXT NOT NULL
        );
        -- 既有 releases / watermarks 表结构保持原样，多消费附加字段放这边的旁表，
        -- 通过 release_id / watermark_id 一一对应，不 ALTER 原表。
        CREATE TABLE IF NOT EXISTS release_details(
          release_id INTEGER PRIMARY KEY,
          batch_id INTEGER,
          consumer_name TEXT NOT NULL DEFAULT '',
          caliber TEXT NOT NULL DEFAULT '',
          watermark_biz_date TEXT NOT NULL DEFAULT '',
          previous_watermark TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS watermark_details(
          watermark_id INTEGER PRIMARY KEY,
          batch_id INTEGER,
          consumer_name TEXT NOT NULL DEFAULT '',
          caliber TEXT NOT NULL DEFAULT '',
          previous_watermark TEXT NOT NULL DEFAULT ''
        );
        """)
        ensure_consumers(conn)

        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute(
                    "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                    r,
                )
            conn.execute(
                "INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')"
            )
            conn.execute(
                "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west')"
            )


@app.on_event("startup")
def _start():
    init_db()


@app.get("/api/health")
def health():
    with closing(connect()) as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with closing(connect()) as c:
        row = c.execute(
            "SELECT username, role FROM users WHERE username=? AND password_sha=?",
            (body.username, sha(body.password)),
        ).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}


class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int


@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = (
        "完整"
        if body.row_count >= body.expected_count and body.expected_count > 0
        else ("部分完整" if body.row_count > 0 else "待检")
    )
    with closing(connect()) as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count, body.expected_count, comp, "未放行"),
            )
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")


@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = (
            "完整"
            if row["row_count"] >= row["expected_count"] and row["expected_count"] > 0
            else ("部分完整" if row["row_count"] > 0 else "待检")
        )
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
    return {"id": sid, "completeness": comp}


# ---------------------------------------------------------------- 消费方目录


class ConsumerIn(BaseModel):
    code: str
    name: str
    contact: str = ""
    status: str = "启用"
    sort_order: int = 100


@app.get("/api/consumers")
def list_consumers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        rows = c.execute("SELECT * FROM consumers ORDER BY sort_order, id").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.post("/api/consumers")
def add_consumer(body: ConsumerIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    code = body.code.strip()
    if not code:
        raise HTTPException(400, "消费方编码不能为空")
    if body.status not in ("启用", "停用"):
        raise HTTPException(400, "状态只能是 启用/停用")
    with closing(connect()) as c:
        try:
            cur = c.execute(
                "INSERT INTO consumers(code,name,contact,status,sort_order,created_at) VALUES (?,?,?,?,?,?)",
                (code, body.name.strip() or code, body.contact.strip(), body.status, body.sort_order, now_iso()),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "消费方已存在")
        row = c.execute("SELECT * FROM consumers WHERE id=?", (cur.lastrowid,)).fetchone()
    return {"ok": True, "item": dict(row)}


@app.patch("/api/consumers/{code}")
def update_consumer(code: str, body: ConsumerIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    if body.status not in ("启用", "停用"):
        raise HTTPException(400, "状态只能是 启用/停用")
    with closing(connect()) as c:
        row = c.execute("SELECT * FROM consumers WHERE code=?", (code,)).fetchone()
        if not row:
            raise HTTPException(404, "消费方不存在")
        c.execute(
            "UPDATE consumers SET name=?, contact=?, status=?, sort_order=? WHERE code=?",
            (body.name.strip() or code, body.contact.strip(), body.status, body.sort_order, code),
        )
        row = c.execute("SELECT * FROM consumers WHERE code=?", (code,)).fetchone()
    return {"ok": True, "item": dict(row)}


# ------------------------------------------------------------ 多消费放行主链路


class ReleaseIn(BaseModel):
    consumer: str | None = None
    consumers: list[Any] | None = None
    consumer_codes: list[Any] | None = None
    caliber: str | None = None
    mode: str | None = None
    note: str | None = None


def _extract_consumers(body: ReleaseIn) -> list[str]:
    raw: list[Any] | None
    if body.consumers is not None:
        raw = body.consumers
    elif body.consumer_codes is not None:
        raw = body.consumer_codes
    elif body.consumer:
        raw = [body.consumer]
    else:
        raw = []
    out: list[str] = []
    for item in raw:
        if isinstance(item, dict):
            value = item.get("code") or item.get("consumer") or ""
        else:
            value = item
        code = str(value).strip()
        if code:
            out.append(code)
    return out


def _record_incident(
    conn,
    kind: str,
    *,
    slice_id: int = 0,
    slice_no: str = "",
    biz_date: str = "",
    batch_no: str = "",
    caliber: str = "",
    consumers: list[str] | None = None,
    operator: str = "",
    detail: str = "",
):
    conn.execute(
        """INSERT INTO release_incidents
           (kind,slice_id,slice_no,biz_date,batch_no,caliber,consumers,operator,detail,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            kind,
            slice_id,
            slice_no,
            biz_date,
            batch_no,
            caliber,
            json.dumps(consumers or [], ensure_ascii=False),
            operator,
            detail,
            now_iso(),
        ),
    )


def _thread_lock(slice_id: int) -> threading.Lock:
    with _SLICE_LOCKS_GUARD:
        return _SLICE_LOCKS.setdefault(slice_id, threading.Lock())


def _acquire_slice_lock(slice_id: int, owner: str) -> bool:
    # 用 epoch 秒比较，避免本地时/UTC 与字符串格式差异把在途锁误判成过期。
    cutoff = int(datetime.now().timestamp()) - LOCK_STALE_SECONDS
    with closing(connect()) as c:
        c.execute("DELETE FROM slice_locks WHERE CAST(acquired_at AS INTEGER) < ?", (cutoff,))
        try:
            c.execute(
                "INSERT INTO slice_locks(slice_id,owner,acquired_at) VALUES (?,?,?)",
                (slice_id, owner, str(int(datetime.now().timestamp()))),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def _release_slice_lock(slice_id: int, owner: str):
    with closing(connect()) as c:
        c.execute("DELETE FROM slice_locks WHERE slice_id=? AND owner=?", (slice_id, owner))


def _batch_no() -> str:
    return "MR-" + datetime.now().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3).upper()


def _new_batch(
    conn,
    *,
    batch_no: str,
    slice_row,
    caliber: str,
    consumers: list[str],
    status: str,
    success_count: int,
    fail_count: int,
    operator: str,
    note: str,
) -> int:
    cur = conn.execute(
        """INSERT INTO release_batches
           (batch_no,slice_id,slice_no,biz_date,caliber,consumers,status,success_count,fail_count,operator,note,created_at,finished_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            batch_no,
            slice_row["id"],
            slice_row["slice_no"],
            slice_row["biz_date"],
            caliber,
            json.dumps(consumers, ensure_ascii=False),
            status,
            success_count,
            fail_count,
            operator,
            note,
            now_iso(),
            None if status == "处理中" else now_iso(),
        ),
    )
    return int(cur.lastrowid)


def _insert_detail(
    conn,
    *,
    batch_id: int | None,
    slice_row,
    consumer: str,
    consumer_name: str,
    status: str,
    note: str,
    caliber: str,
    watermark: str = "",
    previous_watermark: str = "",
):
    cur = conn.execute(
        "INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
        (slice_row["id"], consumer, status, note, now_iso()),
    )
    conn.execute(
        """INSERT INTO release_details
           (release_id,batch_id,consumer_name,caliber,watermark_biz_date,previous_watermark)
           VALUES (?,?,?,?,?,?)""",
        (
            int(cur.lastrowid),
            batch_id,
            consumer_name,
            caliber,
            slice_row["biz_date"] if watermark else "",
            previous_watermark,
        ),
    )


def _current_watermark(conn, consumer: str):
    return conn.execute(
        WATERMARK_SELECT + " WHERE w.consumer=? ORDER BY w.id DESC LIMIT 1", (consumer,)
    ).fetchone()


def _watermark_value(biz_date: str) -> str:
    # 业务日水位落到当日末尾，既可比、也不会因为重跑时间不同而乱跳。
    return f"{biz_date}T23:59:59"


def _apply_success(
    conn,
    *,
    batch_id: int,
    slice_row,
    consumer: str,
    consumer_name: str,
    caliber: str,
):
    prev = _current_watermark(conn, consumer)
    previous_watermark = prev["watermark"] if prev else ""
    watermark = _watermark_value(slice_row["biz_date"])
    _insert_detail(
        conn,
        batch_id=batch_id,
        slice_row=slice_row,
        consumer=consumer,
        consumer_name=consumer_name,
        status="成功",
        note=f"{caliber}口径放行成功，独立推进水位",
        caliber=caliber,
        watermark=watermark,
        previous_watermark=previous_watermark,
    )
    cur = conn.execute(
        "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES (?,?,?,?)",
        (
            consumer,
            slice_row["biz_date"],
            watermark,
            slice_row["slice_no"],
        ),
    )
    conn.execute(
        """INSERT INTO watermark_details
           (watermark_id,batch_id,consumer_name,caliber,previous_watermark)
           VALUES (?,?,?,?,?)""",
        (int(cur.lastrowid), batch_id, consumer_name, caliber, previous_watermark),
    )
    return watermark, previous_watermark


def _consumer_plan(conn, consumers: list[str], slice_row) -> list[dict]:
    """把目录校验、重复放行校验与水位回拨校验都前置成逐方计划，提交时不再猜。"""
    plan = []
    for code in consumers:
        row = conn.execute("SELECT * FROM consumers WHERE code=?", (code,)).fetchone()
        name = row["name"] if row else code
        error = ""
        if not row:
            error = f"消费方目录中不存在：{code}"
        elif row["status"] != "启用":
            error = f"消费方已停用：{code}"
        elif conn.execute(
            "SELECT 1 FROM releases WHERE slice_id=? AND consumer=? AND status='成功' LIMIT 1",
            (slice_row["id"], code),
        ).fetchone():
            # 同一消费方对同一切片重复放行会重复推进水位，直接按已放行拒绝。
            error = f"该消费方已在本切片成功放行：{code}"
        else:
            prev = _current_watermark(conn, code)
            if prev and prev["biz_date"] > slice_row["biz_date"]:
                error = (
                    f"水位回拨：{code} 当前水位日 {prev['biz_date']} "
                    f"晚于切片业务日 {slice_row['biz_date']}"
                )
        plan.append({"code": code, "name": name, "error": error})
    return plan


RELEASE_SELECT = """SELECT r.id, r.slice_id, r.consumer, r.status, r.note, r.created_at,
                           d.batch_id, d.caliber, d.watermark_biz_date,
                           d.previous_watermark,
                           COALESCE(cn.name, d.consumer_name, r.consumer) AS consumer_name
                    FROM releases r
                    LEFT JOIN release_details d ON d.release_id=r.id
                    LEFT JOIN consumers cn ON cn.code=r.consumer"""

WATERMARK_SELECT = """SELECT w.id, w.consumer, w.biz_date, w.watermark, w.source_slice,
                             d.batch_id, d.caliber, d.previous_watermark,
                             COALESCE(cn.name, d.consumer_name, w.consumer) AS consumer_name
                      FROM watermarks w
                      LEFT JOIN watermark_details d ON d.watermark_id=w.id
                      LEFT JOIN consumers cn ON cn.code=w.consumer"""


def _batch_payload(conn, batch_id: int) -> dict:
    row = conn.execute("SELECT * FROM release_batches WHERE id=?", (batch_id,)).fetchone()
    if not row:
        raise HTTPException(404, "提交簿记录不存在")
    batch = dict(row)
    batch["consumers"] = json.loads(batch["consumers"] or "[]")
    details = conn.execute(
        RELEASE_SELECT + " WHERE d.batch_id=? ORDER BY r.id", (batch_id,)
    ).fetchall()
    batch["items"] = [dict(r) for r in details]
    return batch


def _commit_multi_jia(
    conn,
    *,
    batch_no: str,
    slice_row,
    consumers: list[str],
    plan: list[dict],
    caliber: str,
    operator: str,
    note: str,
) -> dict:
    failures = [p for p in plan if p["error"]]
    if failures:
        reason = "；".join(f"{p['code']}：{p['error']}" for p in failures)
        # 整笔回滚：只留提交簿与逐方失败明细，成功放行/水位一条不写。
        with tx(conn):
            batch_id = _new_batch(
                conn,
                batch_no=batch_no,
                slice_row=slice_row,
                caliber=caliber,
                consumers=consumers,
                status="整笔回滚",
                success_count=0,
                fail_count=len(consumers),
                operator=operator,
                note=f"甲口径整笔回滚：{reason}",
            )
            for p in plan:
                _insert_detail(
                    conn,
                    batch_id=batch_id,
                    slice_row=slice_row,
                    consumer=p["code"],
                    consumer_name=p["name"],
                    status="回滚",
                    note=p["error"] or "同批其他消费方失败，整笔回滚",
                    caliber=caliber,
                )
        batch = _batch_payload(conn, batch_id)
        raise ReleaseRejected(
            409,
            f"甲口径整笔回滚：{reason}",
            code="JIA_ROLLED_BACK",
            rolled_back=True,
            batch=batch,
        )

    try:
        with tx(conn):
            batch_id = _new_batch(
                conn,
                batch_no=batch_no,
                slice_row=slice_row,
                caliber=caliber,
                consumers=consumers,
                status="处理中",
                success_count=0,
                fail_count=0,
                operator=operator,
                note=note,
            )
            for p in plan:
                _apply_success(
                    conn,
                    batch_id=batch_id,
                    slice_row=slice_row,
                    consumer=p["code"],
                    consumer_name=p["name"],
                    caliber=caliber,
                )
            conn.execute(
                "UPDATE release_batches SET status='全部成功',success_count=?,fail_count=0,finished_at=? WHERE id=?",
                (len(plan), now_iso(), batch_id),
            )
            conn.execute(
                "UPDATE slices SET release_status='已放行' WHERE id=?", (slice_row["id"],)
            )
    except Exception as exc:  # noqa: BLE001 - 任何一方写失败都必须整笔回滚并留痕
        with tx(conn):
            batch_id = _new_batch(
                conn,
                batch_no=batch_no,
                slice_row=slice_row,
                caliber=caliber,
                consumers=consumers,
                status="整笔回滚",
                success_count=0,
                fail_count=len(consumers),
                operator=operator,
                note=f"甲口径整笔回滚：{exc}",
            )
            for p in plan:
                _insert_detail(
                    conn,
                    batch_id=batch_id,
                    slice_row=slice_row,
                    consumer=p["code"],
                    consumer_name=p["name"],
                    status="回滚",
                    note=f"同批提交异常，整笔回滚：{exc}",
                    caliber=caliber,
                )
        raise ReleaseRejected(
            409,
            f"甲口径整笔回滚：{exc}",
            code="JIA_ROLLED_BACK",
            rolled_back=True,
            batch=_batch_payload(conn, batch_id),
        ) from exc

    return {
        "ok": True,
        "status": "全部成功",
        "all_success": True,
        "rolled_back": False,
        "success_count": len(plan),
        "fail_count": 0,
        "batch": _batch_payload(conn, batch_id),
    }


def _commit_multi_yi(
    conn,
    *,
    batch_no: str,
    slice_row,
    consumers: list[str],
    plan: list[dict],
    caliber: str,
    operator: str,
    note: str,
) -> dict:
    # 提交簿先落，再逐方独立事务。失败方单列，绝不把整单标成全部成功。
    with tx(conn):
        batch_id = _new_batch(
            conn,
            batch_no=batch_no,
            slice_row=slice_row,
            caliber=caliber,
            consumers=consumers,
            status="处理中",
            success_count=0,
            fail_count=0,
            operator=operator,
            note=note,
        )

    success = 0
    fail = 0
    for p in plan:
        if p["error"]:
            with tx(conn):
                _insert_detail(
                    conn,
                    batch_id=batch_id,
                    slice_row=slice_row,
                    consumer=p["code"],
                    consumer_name=p["name"],
                    status="失败",
                    note=p["error"],
                    caliber=caliber,
                )
            fail += 1
            continue
        try:
            with tx(conn):
                # 独立事务内再取一次当前水位，防止与同消费方的其它提交串水位。
                prev = _current_watermark(conn, p["code"])
                if prev and prev["biz_date"] > slice_row["biz_date"]:
                    raise RuntimeError(
                        f"水位回拨：{p['code']} 当前水位日 {prev['biz_date']} "
                        f"晚于切片业务日 {slice_row['biz_date']}"
                    )
                _apply_success(
                    conn,
                    batch_id=batch_id,
                    slice_row=slice_row,
                    consumer=p["code"],
                    consumer_name=p["name"],
                    caliber=caliber,
                )
            success += 1
        except Exception as exc:  # noqa: BLE001 - 该消费方独立失败，保留其它成功方
            with tx(conn):
                _insert_detail(
                    conn,
                    batch_id=batch_id,
                    slice_row=slice_row,
                    consumer=p["code"],
                    consumer_name=p["name"],
                    status="失败",
                    note=f"乙口径独立提交失败：{exc}",
                    caliber=caliber,
                )
            fail += 1

    if fail == 0:
        status = "全部成功"
    elif success == 0:
        status = "全部失败"
    else:
        status = "部分成功"
    with tx(conn):
        conn.execute(
            "UPDATE release_batches SET status=?,success_count=?,fail_count=?,finished_at=? WHERE id=?",
            (status, success, fail, now_iso(), batch_id),
        )
        if fail == 0:
            slice_status = "已放行"
        elif success > 0:
            slice_status = "部分放行"
        else:
            slice_status = slice_row["release_status"]
        conn.execute("UPDATE slices SET release_status=? WHERE id=?", (slice_status, slice_row["id"]))

    batch = _batch_payload(conn, batch_id)
    all_ok = fail == 0
    return {
        "ok": all_ok,
        "status": status,
        "all_success": all_ok,
        "rolled_back": False,
        "success_count": success,
        "fail_count": fail,
        "batch": batch,
    }


def _ensure_legacy_consumer(conn, code: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM consumers WHERE code=?", (code,)).fetchone()
    if row:
        return row
    conn.execute(
        "INSERT INTO consumers(code,name,contact,status,sort_order,created_at) VALUES (?,?,?,?,?,?)",
        (code, code, "", "启用", 999, now_iso()),
    )
    return conn.execute("SELECT * FROM consumers WHERE code=?", (code,)).fetchone()


def _legacy_single(
    conn,
    *,
    batch_no: str,
    slice_row,
    consumer: str,
    operator: str,
    note: str,
) -> dict:
    row = _ensure_legacy_consumer(conn, consumer)
    name = row["name"]
    if row["status"] != "启用":
        with tx(conn):
            batch_id = _new_batch(
                conn,
                batch_no=batch_no,
                slice_row=slice_row,
                caliber="单方",
                consumers=[consumer],
                status="全部失败",
                success_count=0,
                fail_count=1,
                operator=operator,
                note=f"单方放行失败：消费方已停用 {consumer}",
            )
            _insert_detail(
                conn,
                batch_id=batch_id,
                slice_row=slice_row,
                consumer=consumer,
                consumer_name=name,
                status="失败",
                note=f"消费方已停用：{consumer}",
                caliber="单方",
            )
        raise ReleaseRejected(
            409, f"消费方已停用：{consumer}", code="CONSUMER_DISABLED", batch=_batch_payload(conn, batch_id)
        )

    prev = _current_watermark(conn, consumer)
    if prev and prev["biz_date"] > slice_row["biz_date"]:
        with tx(conn):
            batch_id = _new_batch(
                conn,
                batch_no=batch_no,
                slice_row=slice_row,
                caliber="单方",
                consumers=[consumer],
                status="全部失败",
                success_count=0,
                fail_count=1,
                operator=operator,
                note="单方放行失败：水位回拨",
            )
            _insert_detail(
                conn,
                batch_id=batch_id,
                slice_row=slice_row,
                consumer=consumer,
                consumer_name=name,
                status="失败",
                note=f"水位回拨：当前水位日 {prev['biz_date']} 晚于切片业务日 {slice_row['biz_date']}",
                caliber="单方",
            )
        raise ReleaseRejected(
            409,
            f"水位回拨：当前水位日 {prev['biz_date']} 晚于切片业务日 {slice_row['biz_date']}",
            code="WATERMARK_REGRESSION",
            batch=_batch_payload(conn, batch_id),
        )

    with tx(conn):
        batch_id = _new_batch(
            conn,
            batch_no=batch_no,
            slice_row=slice_row,
            caliber="单方",
            consumers=[consumer],
            status="处理中",
            success_count=0,
            fail_count=0,
            operator=operator,
            note=note,
        )
        _apply_success(
            conn,
            batch_id=batch_id,
            slice_row=slice_row,
            consumer=consumer,
            consumer_name=name,
            caliber="单方",
        )
        conn.execute(
            "UPDATE release_batches SET status='全部成功',success_count=1,fail_count=0,finished_at=? WHERE id=?",
            (now_iso(), batch_id),
        )
        conn.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (slice_row["id"],))
    return {"ok": True, "status": "全部成功", "batch": _batch_payload(conn, batch_id)}


def _record_contention(
    sid: int,
    consumers: list[str],
    operator: str,
    batch_no: str,
    caliber: str,
    reason: str,
) -> str:
    """后到者一律留痕，并把在途批次号回给调用方，避免“争用”只是口头提示。"""
    with closing(connect()) as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        holder = c.execute("SELECT owner FROM slice_locks WHERE slice_id=?", (sid,)).fetchone()
        holder_no = holder["owner"] if holder else ""
        _record_incident(
            c,
            "多消费争用",
            slice_id=sid,
            slice_no=row["slice_no"] if row else "",
            biz_date=row["biz_date"] if row else "",
            batch_no=batch_no,
            caliber=caliber or "",
            consumers=consumers,
            operator=operator,
            detail=reason + (f"；在途批次 {holder_no}" if holder_no else ""),
        )
    return holder_no


def _do_release(sid: int, body: ReleaseIn, operator: str, *, force_multi: bool) -> dict:
    consumers = _extract_consumers(body)
    if not consumers:
        raise ReleaseRejected(400, "放行清单不能为空", code="EMPTY_CONSUMERS")
    if len(set(consumers)) != len(consumers):
        raise ReleaseRejected(400, "放行清单中消费方重复，禁止重复推进水位", code="DUPLICATE_CONSUMERS")

    caliber = normalize_caliber(body.caliber if body.caliber is not None else body.mode)
    explicit_list = body.consumers is not None or body.consumer_codes is not None
    is_multi = force_multi or explicit_list or len(consumers) > 1 or body.caliber is not None or body.mode is not None

    if body.caliber is not None or body.mode is not None:
        if not caliber:
            raise ReleaseRejected(400, "口径只能是甲或乙", code="BAD_CALIBER")
    if is_multi and not caliber:
        raise ReleaseRejected(400, "多消费放行须先选定甲乙口径", code="CALIBER_REQUIRED")
    if not is_multi:
        caliber = "单方"

    batch_no = _batch_no()
    note = (body.note or "").strip()
    tlock = _thread_lock(sid)
    if not tlock.acquire(blocking=False):
        holder_no = _record_contention(
            sid, consumers, operator, batch_no, caliber or "",
            "同一瞬间已有另一名值班对同一切片落笔，后到请求未提交",
        )
        raise ReleaseRejected(
            409,
            "多消费争用：同一切片已有放行在途，后到请求不得默成",
            code="CONTENDED",
            batch_no=batch_no,
            holder_batch=holder_no,
        )

    locked_db = False
    try:
        if not _acquire_slice_lock(sid, batch_no):
            holder_no = _record_contention(
                sid, consumers, operator, batch_no, caliber or "",
                "同一切片已有跨连接/跨进程在途锁，后到请求未提交",
            )
            raise ReleaseRejected(
                409,
                "多消费争用：同一切片已有放行在途，后到请求不得默成",
                code="CONTENDED",
                batch_no=batch_no,
                holder_batch=holder_no,
            )
        locked_db = True

        with closing(connect()) as conn:
            slice_row = conn.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
            if not slice_row:
                raise ReleaseRejected(404, "切片不存在", code="SLICE_NOT_FOUND")
            if slice_row["completeness"] != "完整":
                with tx(conn):
                    for code in consumers:
                        _insert_detail(
                            conn,
                            batch_id=None,
                            slice_row=slice_row,
                            consumer=code,
                            consumer_name=code,
                            status="拒绝",
                            note="完整度未达标",
                            caliber=caliber or "单方",
                        )
                raise ReleaseRejected(409, "未完整禁止放行", code="INCOMPLETE")

            if is_multi:
                # 口径选择先落入提交簿；同业务日再改口直接拒绝并记口径偏了。
                # INSERT OR IGNORE 让并发落笔也只有一个口径能写进去。
                conn.execute(
                    """INSERT OR IGNORE INTO caliber_choices(biz_date,caliber,batch_no,operator,chosen_at)
                       VALUES (?,?,?,?,?)""",
                    (slice_row["biz_date"], caliber, batch_no, operator, now_iso()),
                )
                choice = conn.execute(
                    "SELECT * FROM caliber_choices WHERE biz_date=?", (slice_row["biz_date"],)
                ).fetchone()
                if choice["caliber"] != caliber:
                    _record_incident(
                        conn,
                        "多消费口径偏了",
                        slice_id=sid,
                        slice_no=slice_row["slice_no"],
                        biz_date=slice_row["biz_date"],
                        batch_no=batch_no,
                        caliber=caliber,
                        consumers=consumers,
                        operator=operator,
                        detail=(
                            f"多消费提交簿当日已定为{choice['caliber']}口径"
                            f"（{choice['batch_no']}），本次请求为{caliber}口径，拒绝提交"
                        ),
                    )
                    raise ReleaseRejected(
                        409,
                        f"多消费口径偏了：当日提交簿已定为{choice['caliber']}口径，禁止改口为{caliber}",
                        code="CALIBER_DEVIATION",
                        chosen=dict(choice),
                    )

                plan = _consumer_plan(conn, consumers, slice_row)
                if caliber == "甲":
                    return _commit_multi_jia(
                        conn,
                        batch_no=batch_no,
                        slice_row=slice_row,
                        consumers=consumers,
                        plan=plan,
                        caliber=caliber,
                        operator=operator,
                        note=note,
                    )
                return _commit_multi_yi(
                    conn,
                    batch_no=batch_no,
                    slice_row=slice_row,
                    consumers=consumers,
                    plan=plan,
                    caliber=caliber,
                    operator=operator,
                    note=note,
                )

            return _legacy_single(
                conn,
                batch_no=batch_no,
                slice_row=slice_row,
                consumer=consumers[0],
                operator=operator,
                note=note,
            )
    finally:
        if locked_db:
            _release_slice_lock(sid, batch_no)
        tlock.release()


def _release_response(sid: int, body: ReleaseIn, authorization: str | None, *, force_multi: bool):
    operator = require_user(authorization)
    try:
        result = _do_release(sid, body, operator, force_multi=force_multi)
    except ReleaseRejected as exc:
        return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.message, **exc.extra})
    return result


@app.post("/api/slices/{sid}/multi-release")
def multi_release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    return _release_response(sid, body, authorization, force_multi=True)


@app.post("/api/slices/{sid}/release-batch")
def release_batch_alias(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    return _release_response(sid, body, authorization, force_multi=True)


@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    return _release_response(sid, body, authorization, force_multi=False)


# ---------------------------------------------------------------- 查询与对账


def _batch_row(conn, row) -> dict:
    batch = dict(row)
    try:
        batch["consumers"] = json.loads(batch.get("consumers") or "[]")
    except (TypeError, json.JSONDecodeError):
        batch["consumers"] = []
    return batch


@app.get("/api/release-batches")
def list_batches(
    authorization: str | None = Header(default=None),
    slice_id: int | None = None,
    biz_date: str | None = None,
    limit: int = 100,
):
    require_user(authorization)
    where = []
    args: list[Any] = []
    if slice_id is not None:
        where.append("slice_id=?")
        args.append(slice_id)
    if biz_date:
        where.append("biz_date=?")
        args.append(biz_date)
    sql = "SELECT * FROM release_batches"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    args.append(max(1, min(limit, 500)))
    with closing(connect()) as c:
        rows = c.execute(sql, args).fetchall()
    return {"items": [_batch_row(c, r) for r in rows]}


@app.get("/api/multi-releases")
def list_multi_releases(
    authorization: str | None = Header(default=None),
    slice_id: int | None = None,
    biz_date: str | None = None,
    limit: int = 100,
):
    return list_batches(authorization=authorization, slice_id=slice_id, biz_date=biz_date, limit=limit)


@app.get("/api/release-batches/{ident}")
def get_batch(ident: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        if ident.isdigit():
            row = c.execute("SELECT * FROM release_batches WHERE id=?", (int(ident),)).fetchone()
        else:
            row = c.execute("SELECT * FROM release_batches WHERE batch_no=?", (ident,)).fetchone()
        if not row:
            raise HTTPException(404, "提交簿记录不存在")
        batch = _batch_row(c, row)
        batch["items"] = [
            dict(r)
            for r in c.execute(
                RELEASE_SELECT + " WHERE d.batch_id=? ORDER BY r.id", (row["id"],)
            ).fetchall()
        ]
    return {"item": batch}


@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None), limit: int = 200):
    require_user(authorization)
    with closing(connect()) as c:
        rows = c.execute(
            """SELECT r.id, r.slice_id, r.consumer, r.status, r.note, r.created_at,
                      s.slice_no,
                      d.batch_id, d.caliber, d.watermark_biz_date,
                      d.previous_watermark,
                      COALESCE(cn.name, d.consumer_name, r.consumer) AS consumer_name,
                      b.batch_no, b.status AS batch_status, b.caliber AS batch_caliber
               FROM releases r
               JOIN slices s ON s.id=r.slice_id
               LEFT JOIN release_details d ON d.release_id=r.id
               LEFT JOIN consumers cn ON cn.code=r.consumer
               LEFT JOIN release_batches b ON b.id=d.batch_id
               ORDER BY r.id DESC LIMIT ?""",
            (max(1, min(limit, 500)),),
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        rows = c.execute(
            """SELECT w.*, COALESCE(c.name, d.consumer_name, w.consumer) AS consumer_label,
                      c.status AS consumer_status, b.batch_no, b.status AS batch_status,
                      d.batch_id, d.caliber, d.previous_watermark
               FROM watermarks w
               LEFT JOIN consumers c ON c.code=w.consumer
               LEFT JOIN watermark_details d ON d.watermark_id=w.id
               LEFT JOIN release_batches b ON b.id=d.batch_id
               ORDER BY w.consumer, w.id DESC"""
        ).fetchall()
        latest: dict[str, int] = {}
        items = []
        for r in rows:
            d = dict(r)
            if d["consumer"] not in latest:
                latest[d["consumer"]] = d["id"]
            d["is_latest"] = latest[d["consumer"]] == d["id"]
            items.append(d)
    return {"items": items}


@app.get("/api/watermarks/summary")
def watermark_summary(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        consumers = c.execute("SELECT * FROM consumers ORDER BY sort_order, id").fetchall()
        items = []
        for row in consumers:
            wm = c.execute(
                WATERMARK_SELECT + " WHERE w.consumer=? ORDER BY w.id DESC LIMIT 1",
                (row["code"],),
            ).fetchone()
            items.append(
                {
                    "code": row["code"],
                    "name": row["name"],
                    "status": row["status"],
                    "latest_watermark": wm["watermark"] if wm else None,
                    "biz_date": wm["biz_date"] if wm else None,
                    "source_slice": wm["source_slice"] if wm else None,
                    "batch_id": wm["batch_id"] if wm else None,
                    "caliber": wm["caliber"] if wm else None,
                }
            )
    return {"items": items}


@app.get("/api/release-policy")
def release_policy(
    authorization: str | None = Header(default=None), biz_date: str | None = None
):
    require_user(authorization)
    day = biz_date or datetime.now().date().isoformat()
    with closing(connect()) as c:
        choice = c.execute("SELECT * FROM caliber_choices WHERE biz_date=?", (day,)).fetchone()
        consumers = c.execute(
            "SELECT code,name,status FROM consumers WHERE status='启用' ORDER BY sort_order, id"
        ).fetchall()
    return {
        "product": PRODUCT,
        "biz_date": day,
        "calibers": MODE_DEFS,
        "chosen": dict(choice) if choice else None,
        "consumers": [dict(r) for r in consumers],
        "rules": {
            "daily_lock": "口径选定后写入多消费提交簿，当日禁止改口",
            "deviation": "偏离提交簿口径则拒绝并记多消费口径偏了",
            "contention": "同一切片同时落笔互斥串行，后到记多消费争用且不得默成",
            "watermark": "每个成功消费方各自一条水位，禁止共享一条水位冒充多消费方",
            "partial": "整单状态按逐方结果汇总，部分成功不得标成全部成功",
        },
    }


@app.get("/api/release-incidents")
def list_incidents(
    authorization: str | None = Header(default=None), kind: str | None = None, limit: int = 100
):
    require_user(authorization)
    with closing(connect()) as c:
        if kind:
            rows = c.execute(
                "SELECT * FROM release_incidents WHERE kind=? ORDER BY id DESC LIMIT ?",
                (kind, max(1, min(limit, 500))),
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM release_incidents ORDER BY id DESC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
    items = []
    for row in rows:
        d = dict(row)
        try:
            d["consumers"] = json.loads(d.get("consumers") or "[]")
        except (TypeError, json.JSONDecodeError):
            d["consumers"] = []
        items.append(d)
    return {"items": items}


@app.get("/api/multi-release/incidents")
def list_incidents_alias(
    authorization: str | None = Header(default=None), kind: str | None = None, limit: int = 100
):
    return list_incidents(authorization=authorization, kind=kind, limit=limit)


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
