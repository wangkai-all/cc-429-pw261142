"""SQLite 持久化层：口径版本、输入快照、检查点、事件、排除记录、报告。

设计要点：
- mapping_versions / input_versions 均为只追加快照，激活版本单独记录，历史版永不改写；
- 加工按阶段写入 checkpoint，中断后用同一输入版与冻结的映射快照继续；
- 被拒绝/被排除的记录全部落 retained_records，不静默丢弃。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

from . import models
from .models import (
    METRIC, OBS_FULL, OBS_SUCCESS_ONLY, RETRY_COUNTED, RETRY_EXCLUDED, RETRY_NA,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS mapping_versions (
    mapping_version TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    note            TEXT NOT NULL,
    snapshot        TEXT NOT NULL           -- {source: mapping...}
);
CREATE TABLE IF NOT EXISTS active_mapping (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    mapping_version TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS input_versions (
    input_version   TEXT PRIMARY KEY,
    parent_version  TEXT,
    created_at      TEXT NOT NULL,
    note            TEXT NOT NULL,
    mapping_version TEXT NOT NULL,          -- 加工时冻结的映射版
    sources         TEXT NOT NULL,          -- [source,...]
    payload         TEXT NOT NULL           -- {source: [records...]} 完整快照
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    input_version   TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    status          TEXT NOT NULL,          -- running|completed|interrupted|failed
    last_stage      TEXT,
    strategy        TEXT,                   -- narrow|stratified（完成后写）
    error           TEXT
);
CREATE TABLE IF NOT EXISTS checkpoints (
    job_id   TEXT NOT NULL,
    stage    TEXT NOT NULL,
    status   TEXT NOT NULL,                 -- done|interrupted
    detail   TEXT,
    PRIMARY KEY (job_id, stage)
);
CREATE TABLE IF NOT EXISTS raw_events (
    job_id        TEXT NOT NULL,
    source        TEXT NOT NULL,
    record_index  INTEGER NOT NULL,
    raw           TEXT NOT NULL,
    PRIMARY KEY (job_id, source, record_index)
);
CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT NOT NULL,
    source       TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    event_id     TEXT,
    bucket       TEXT NOT NULL,
    service      TEXT NOT NULL,
    region       TEXT NOT NULL,
    outcome      TEXT,                      -- success_only 来源为 NULL
    retry_count  INTEGER NOT NULL,
    attempts     INTEGER NOT NULL,         -- 折算后的有效尝试数
    is_success   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS retained_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT NOT NULL,
    input_version TEXT NOT NULL,
    stage        TEXT NOT NULL,            -- parse|canonicalize|compatibility|compare
    source       TEXT NOT NULL,
    record_index INTEGER,
    unit         TEXT,
    reason_code  TEXT NOT NULL,
    detail       TEXT,
    raw          TEXT
);
CREATE TABLE IF NOT EXISTS verdicts (
    job_id          TEXT NOT NULL,
    source          TEXT NOT NULL,
    observed_type   TEXT NOT NULL,
    retry_policy    TEXT NOT NULL,
    caliber_sig     TEXT NOT NULL,
    compatible      INTEGER NOT NULL,
    reason_code     TEXT,
    PRIMARY KEY (job_id, source)
);
CREATE TABLE IF NOT EXISTS exclusions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT NOT NULL,
    source       TEXT NOT NULL,
    unit         TEXT,
    reason_code  TEXT NOT NULL,
    detail       TEXT,
    record_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS comparable_units (
    job_id       TEXT NOT NULL,
    unit         TEXT NOT NULL,
    strategy     TEXT NOT NULL,            -- narrow|stratified
    stratum_sig  TEXT,
    sources      TEXT NOT NULL,            -- [source,...] 实际参与来源
    excluded     TEXT NOT NULL,            -- [{source,reason_code,detail}]
    PRIMARY KEY (job_id, strategy, unit, stratum_sig)
);
CREATE TABLE IF NOT EXISTS metric_rows (
    job_id      TEXT NOT NULL,
    strategy    TEXT NOT NULL,
    unit        TEXT NOT NULL,
    stratum_sig TEXT,
    source      TEXT NOT NULL,
    successes   INTEGER NOT NULL,
    attempts    INTEGER NOT NULL,
    success_volume INTEGER NOT NULL,      -- 成功-only 来源唯一可报量
    rate        REAL,                      -- NULL 表示不可计算（禁止补零）
    note        TEXT,
    PRIMARY KEY (job_id, strategy, unit, source)
);
CREATE TABLE IF NOT EXISTS reports (
    job_id       TEXT PRIMARY KEY,
    input_version TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    strategy     TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    body         TEXT NOT NULL
);
"""

# --- 三个虚构来源的初始口径映射 ----------------------------------------------
SEED_MAPPING = {
    "alpha-gateway": {
        "source": "alpha-gateway",
        "observed_type": OBS_FULL,
        "retry_policy": RETRY_EXCLUDED,
        "field_map": {
            "event_id": "id",
            "ts": "timestamp",
            "service": "svc",
            "region": "area",
            "outcome": "result",
            "retry_count": "retries",
        },
        "value_map": {"ok": "success", "fail": "failure"},
    },
    "beta-pipeline": {
        "source": "beta-pipeline",
        "observed_type": OBS_FULL,
        "retry_policy": RETRY_COUNTED,
        "field_map": {
            "event_id": "eventId",
            "ts": "time",
            "service": "service",
            "region": "region",
            "outcome": "status",
            "retry_count": "retry",
        },
        "value_map": {"SUCCESS": "success", "FAILURE": "failure"},
    },
    "gamma-bridge": {
        "source": "gamma-bridge",
        "observed_type": OBS_SUCCESS_ONLY,
        "retry_policy": RETRY_NA,
        "field_map": {
            "event_id": "event_id",
            "ts": "event_time",
            "service": "service_name",
            "region": "zone",
            "retry_count": "attempt",
        },
        "value_map": {},
    },
}


class Store:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        # ThreadingHTTPServer 会在工作线程复用同一连接；check_same_thread=False
        # 配合 self._lock 串行化所有写事务
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._seed()

    def close(self):
        with self._lock:
            self.conn.close()

    @contextmanager
    def tx(self):
        with self._lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    # --- 初始化 ---------------------------------------------------------------
    def _seed(self):
        row = self.conn.execute("SELECT 1 FROM mapping_versions LIMIT 1").fetchone()
        if row:
            return
        mv = "mapping-v1"
        with self.tx() as c:
            c.execute(
                "INSERT INTO mapping_versions VALUES (?,?,?,?)",
                (mv, models.now_iso(), "初始口径映射：alpha/beta 全量上报，gamma 仅成功",
                 models.dumps(SEED_MAPPING)),
            )
            c.execute("INSERT INTO active_mapping(id, mapping_version) VALUES (1,?)", (mv,))

    # --- 口径映射 --------------------------------------------------------------
    def active_mapping_version(self) -> str:
        return self.conn.execute(
            "SELECT mapping_version FROM active_mapping WHERE id=1").fetchone()[0]

    def get_mapping(self, mapping_version: str) -> dict:
        row = self.conn.execute(
            "SELECT snapshot FROM mapping_versions WHERE mapping_version=?",
            (mapping_version,)).fetchone()
        if row is None:
            raise KeyError(f"mapping version not found: {mapping_version}")
        return models.loads(row["snapshot"])

    def list_mapping_versions(self):
        rows = self.conn.execute(
            "SELECT * FROM mapping_versions ORDER BY rowid").fetchall()
        active = self.active_mapping_version()
        return [dict(r, is_active=r["mapping_version"] == active) for r in rows]

    def save_mapping_version(self, mapping_version: str, snapshot: dict, note: str):
        with self.tx() as c:
            c.execute("INSERT INTO mapping_versions VALUES (?,?,?,?)",
                      (mapping_version, models.now_iso(), note, models.dumps(snapshot)))
            c.execute("UPDATE active_mapping SET mapping_version=? WHERE id=1",
                      (mapping_version,))

    # --- 输入快照 --------------------------------------------------------------
    def save_input_version(self, iv, parent, note, mv, sources, payload):
        with self.tx() as c:
            c.execute("INSERT INTO input_versions VALUES (?,?,?,?,?,?,?)",
                      (iv, parent, models.now_iso(), note, mv,
                       models.dumps(sources), models.dumps(payload)))

    def get_input_version(self, iv: str):
        row = self.conn.execute(
            "SELECT * FROM input_versions WHERE input_version=?", (iv,)).fetchone()
        if row is None:
            raise KeyError(f"input version not found: {iv}")
        d = dict(row)
        d["sources"] = models.loads(d["sources"])
        d["payload"] = models.loads(d["payload"])
        return d

    def list_input_versions(self):
        rows = self.conn.execute(
            "SELECT input_version, parent_version, created_at, note, mapping_version, sources "
            "FROM input_versions ORDER BY rowid").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["sources"] = models.loads(d["sources"])
            out.append(d)
        return out

    # --- 作业与检查点 -----------------------------------------------------------
    def create_job(self, job_id, iv, mv):
        with self.tx() as c:
            c.execute("INSERT INTO jobs(job_id,input_version,mapping_version,created_at,status)"
                      " VALUES (?,?,?,?, 'running')",
                      (job_id, iv, mv, models.now_iso()))

    def update_job(self, job_id, **fields):
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.tx() as c:
            c.execute(f"UPDATE jobs SET {cols} WHERE job_id=?",
                      (*fields.values(), job_id))

    def get_job(self, job_id):
        r = self.conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(r) if r else None

    def list_jobs(self):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM jobs ORDER BY created_at").fetchall()]

    def resumable_jobs(self):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM jobs WHERE status IN ('running','interrupted') ORDER BY created_at"
        ).fetchall()]

    def checkpoint_done(self, job_id, stage):
        r = self.conn.execute(
            "SELECT 1 FROM checkpoints WHERE job_id=? AND stage=? AND status='done'",
            (job_id, stage)).fetchone()
        return r is not None

    def mark_checkpoint(self, job_id, stage, status, detail=None):
        with self.tx() as c:
            c.execute("""INSERT INTO checkpoints(job_id,stage,status,detail) VALUES(?,?,?,?)
                         ON CONFLICT(job_id,stage) DO UPDATE SET status=excluded.status, detail=excluded.detail""",
                      (job_id, stage, status, models.dumps(detail) if detail else None))

    def list_checkpoints(self, job_id):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM checkpoints WHERE job_id=? ORDER BY rowid", (job_id,)).fetchall()]

    # --- 事件与保留记录 ---------------------------------------------------------
    def insert_raw(self, job_id, source, idx, raw):
        self.conn.execute("INSERT OR REPLACE INTO raw_events VALUES(?,?,?,?)",
                          (job_id, source, idx, raw))

    def insert_event(self, job_id, source, idx, ev):
        self.conn.execute(
            """INSERT INTO events(job_id,source,record_index,event_id,bucket,service,region,
                                  outcome,retry_count,attempts,is_success)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (job_id, source, idx, ev["event_id"], ev["bucket"], ev["service"], ev["region"],
             ev["outcome"], ev["retry_count"], ev["attempts"], ev["is_success"]))

    def insert_retained(self, job_id, iv, stage, source, idx, unit, reason, detail, raw):
        self.conn.execute(
            """INSERT INTO retained_records(job_id,input_version,stage,source,record_index,
                                            unit,reason_code,detail,raw)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (job_id, iv, stage, source, idx, unit, reason,
             models.dumps(detail) if detail is not None else None,
             models.dumps(raw, ensure_ascii=False) if raw is not None else None))

    def list_retained(self, job_id=None, reason_code=None):
        q, args = "SELECT * FROM retained_records WHERE 1=1", []
        if job_id:
            q += " AND job_id=?"; args.append(job_id)
        if reason_code:
            q += " AND reason_code=?"; args.append(reason_code)
        return [dict(r) for r in self.conn.execute(q + " ORDER BY id", args).fetchall()]

    def upsert_verdict(self, job_id, source, ot, rp, sig, compatible, reason=None):
        with self.tx() as c:
            c.execute("""INSERT INTO verdicts VALUES(?,?,?,?,?,?,?)
                         ON CONFLICT(job_id,source) DO UPDATE SET
                           observed_type=excluded.observed_type, retry_policy=excluded.retry_policy,
                           caliber_sig=excluded.caliber_sig, compatible=excluded.compatible,
                           reason_code=excluded.reason_code""",
                      (job_id, source, ot, rp, sig, int(compatible), reason))

    def list_verdicts(self, job_id):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM verdicts WHERE job_id=? ORDER BY source", (job_id,)).fetchall()]

    def insert_exclusion(self, job_id, source, unit, reason, detail=None, count=0):
        with self.tx() as c:
            c.execute("INSERT INTO exclusions(job_id,source,unit,reason_code,detail,record_count)"
                      " VALUES(?,?,?,?,?,?)",
                      (job_id, source, unit, reason,
                       models.dumps(detail) if detail is not None else None, count))

    def list_exclusions(self, job_id):
        rows = self.conn.execute(
            "SELECT * FROM exclusions WHERE job_id=? ORDER BY id", (job_id,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["detail"] = models.loads(d["detail"])
            out.append(d)
        return out

    def save_comparable_unit(self, job_id, unit, strategy, sig, sources, excluded):
        with self.tx() as c:
            c.execute("""INSERT INTO comparable_units VALUES(?,?,?,?,?,?)
                         ON CONFLICT(job_id,strategy,unit,stratum_sig) DO UPDATE SET
                           sources=excluded.sources, excluded=excluded.excluded""",
                      (job_id, unit, strategy, sig, models.dumps(sources),
                       models.dumps(excluded)))

    def list_comparable_units(self, job_id, strategy):
        rows = self.conn.execute(
            "SELECT * FROM comparable_units WHERE job_id=? AND strategy=? ORDER BY unit",
            (job_id, strategy)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["sources"] = models.loads(d["sources"])
            d["excluded"] = models.loads(d["excluded"])
            out.append(d)
        return out

    def upsert_metric_row(self, job_id, strategy, unit, sig, source,
                          successes, attempts, success_volume, rate, note=None):
        with self.tx() as c:
            c.execute("""INSERT INTO metric_rows VALUES(?,?,?,?,?,?,?,?,?,?)
                         ON CONFLICT(job_id,strategy,unit,source) DO UPDATE SET
                           stratum_sig=excluded.stratum_sig, successes=excluded.successes,
                           attempts=excluded.attempts, success_volume=excluded.success_volume,
                           rate=excluded.rate, note=excluded.note""",
                      (job_id, strategy, unit, sig, source, successes, attempts,
                       success_volume, rate, note))

    def list_metric_rows(self, job_id, strategy=None):
        q = "SELECT * FROM metric_rows WHERE job_id=?"
        args = [job_id]
        if strategy:
            q += " AND strategy=?"; args.append(strategy)
        return [dict(r) for r in self.conn.execute(q + " ORDER BY unit, source", args).fetchall()]

    def save_report(self, job_id, iv, mv, strategy, body):
        with self.tx() as c:
            c.execute("""INSERT INTO reports VALUES(?,?,?,?,?,?)
                         ON CONFLICT(job_id) DO UPDATE SET body=excluded.body,
                           input_version=excluded.input_version, mapping_version=excluded.mapping_version,
                           strategy=excluded.strategy, created_at=excluded.created_at""",
                      (job_id, iv, mv, strategy, models.now_iso(), models.dumps(body)))

    def get_report(self, job_id):
        r = self.conn.execute("SELECT * FROM reports WHERE job_id=?", (job_id,)).fetchone()
        if not r:
            return None
        d = dict(r)
        d["body"] = models.loads(d["body"])
        return d

    def list_reports(self):
        rows = self.conn.execute(
            "SELECT job_id,input_version,mapping_version,strategy,created_at FROM reports"
            " ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def aggregated_events(self, job_id):
        """每来源×单元的事件聚合（供计算阶段）。"""
        return [dict(r) for r in self.conn.execute(
            """SELECT source, bucket, service, region,
                      SUM(is_success) AS successes,
                      SUM(attempts)  AS attempts,
                      COUNT(*)       AS event_count
               FROM events WHERE job_id=?
               GROUP BY source, bucket, service, region
               ORDER BY source, bucket, service, region""", (job_id,)).fetchall()]

    def units_by_source(self, job_id):
        out: dict[str, set[str]] = {}
        for r in self.conn.execute(
                "SELECT DISTINCT source, bucket, service, region FROM events WHERE job_id=?",
                (job_id,)).fetchall():
            out.setdefault(r["source"], set()).add(
                models.unit_key(r["bucket"], r["service"], r["region"]))
        return out
