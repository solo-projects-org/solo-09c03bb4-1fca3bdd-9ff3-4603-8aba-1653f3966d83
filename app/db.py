"""SQLite 存储层：批次、跨度(调用记录)、入仓表、配置、撤回名单。单连接 + 锁，WAL 模式。"""
import contextlib
import json
import os
import sqlite3
import threading
import time

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "orch.db"))

_lock = threading.RLock()
_local = threading.local()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row
_conn.execute("PRAGMA journal_mode=WAL")
_conn.execute("PRAGMA busy_timeout=5000")

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at REAL NOT NULL,
  sample_ids TEXT NOT NULL,          -- JSON array
  status TEXT NOT NULL DEFAULT 'running',   -- running / done / failed
  finished_at REAL,
  conclusion TEXT                    -- JSON, 完成结论(含关键链/阻断)
);
CREATE TABLE IF NOT EXISTS spans (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  sample_id TEXT NOT NULL,
  stage TEXT NOT NULL,               -- fetch / qc_nonneg / qc_match / warehouse
  side TEXT NOT NULL,                -- client(编排侧) / server(服务端上报)
  service TEXT NOT NULL,             -- orchestrator / source / qc
  attempt INTEGER NOT NULL DEFAULT 1,
  request_id TEXT NOT NULL,          -- 跨服务调用关联
  status TEXT NOT NULL,              -- running/effective/ok/failed/timeout/late
  start_ts REAL NOT NULL,
  end_ts REAL,
  queue_ms REAL,                     -- 服务端等名额(槽位)耗时
  detail TEXT,                       -- JSON
  note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_spans_batch ON spans(batch_id);
CREATE INDEX IF NOT EXISTS idx_spans_req ON spans(request_id);
CREATE TABLE IF NOT EXISTS warehouse (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  sample_id TEXT NOT NULL,
  concentration REAL NOT NULL,
  stored_at REAL NOT NULL,
  request_id TEXT NOT NULL,          -- 产生该入仓记录的取数尝试
  UNIQUE(batch_id, sample_id)        -- 幂等：迟到/重复结果不得重复入仓
);
CREATE TABLE IF NOT EXISTS kv (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS withdrawn (
  sample_id TEXT PRIMARY KEY,      -- 已撤回的委托样品号;错位返回号(如 MIX 钩子的 -X)也登记在此
  withdrawn_at REAL NOT NULL,
  alias_of TEXT                    -- 错位返回号指向其委托号;委托号本身为 NULL
);
CREATE TABLE IF NOT EXISTS withdrawn_requests (
  request_id TEXT PRIMARY KEY,     -- 撤回时该号全部在途/已完成调用的 request_id
  withdrawn_at REAL NOT NULL       -- 迟到服务端上报按 request_id 拦截(错位号证据兜底)
);
"""

with _lock:
    _conn.executescript(SCHEMA)
    _conn.commit()


def execute(sql, args=()):
    with _lock:
        cur = _conn.execute(sql, args)
        if not getattr(_local, "in_txn", False):
            _conn.commit()
        return cur


@contextlib.contextmanager
def atomic():
    """多语句事务：块内 execute 不逐句提交，整体成功才提交，异常即回滚。
    撤回等多表改动必须用它保证"清退状态与可查数据"在失败/重启后仍一致。"""
    with _lock:
        _local.in_txn = True
        try:
            yield
        except Exception:
            _conn.rollback()
            raise
        else:
            _conn.commit()
        finally:
            _local.in_txn = False


def query(sql, args=()):
    with _lock:
        return [dict(r) for r in _conn.execute(sql, args).fetchall()]


def query_one(sql, args=()):
    rows = query(sql, args)
    return rows[0] if rows else None


# ---------- 撤回名单 ----------

def is_withdrawn(sample_id):
    """样品号(含错位返回号)是否已撤回。已撤回号的一切新写入均被拦截。"""
    return query_one("SELECT 1 AS x FROM withdrawn WHERE sample_id=?",
                     (str(sample_id),)) is not None


def register_withdrawn(sample_id, alias_of=None):
    execute("INSERT INTO withdrawn(sample_id, withdrawn_at, alias_of) VALUES(?,?,?)"
            " ON CONFLICT(sample_id) DO NOTHING",
            (str(sample_id), time.time(), alias_of))


def is_withdrawn_request(request_id):
    """该 request_id 是否属于已撤回样品的调用(迟到服务端上报拦截用)。"""
    return bool(request_id) and query_one(
        "SELECT 1 AS x FROM withdrawn_requests WHERE request_id=?",
        (str(request_id),)) is not None


def register_withdrawn_request(request_id):
    if request_id:
        execute("INSERT INTO withdrawn_requests(request_id, withdrawn_at) VALUES(?,?)"
                " ON CONFLICT(request_id) DO NOTHING",
                (str(request_id), time.time()))


# ---------- 跨度 ----------

def insert_span(batch_id, sample_id, stage, side, service, attempt, request_id,
                status, start_ts, queue_ms=None, detail=None):
    with _lock:  # 检查与写入同锁:与撤回事务互斥,撤回后绝不重新写入
        if is_withdrawn(sample_id):
            return None  # 已撤回:不再产生新的调用记录
        cur = execute(
            "INSERT INTO spans(batch_id,sample_id,stage,side,service,attempt,request_id,"
            "status,start_ts,queue_ms,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (batch_id, sample_id, stage, side, service, attempt, request_id, status,
             start_ts, queue_ms,
             json.dumps(detail, ensure_ascii=False) if detail is not None else None))
        return cur.lastrowid


def finish_span(span_id, end_ts, status, detail=None, note=None):
    if span_id is None:
        return  # 撤回拦截后的空跨度
    sets, args = "end_ts=?, status=?", [end_ts, status]
    if detail is not None:
        sets += ", detail=?"
        args.append(json.dumps(detail, ensure_ascii=False))
    if note:
        sets += ", note=note||?"
        args.append(note)
    args.append(span_id)
    execute(f"UPDATE spans SET {sets} WHERE id=?", args)


def mark_status(span_id, status, note=None, only_if=None):
    if span_id is None:
        return  # 撤回拦截后的空跨度
    sql, args = "UPDATE spans SET status=?", [status]
    if note:
        sql += ", note=note||?"
        args.append(note)
    sql += " WHERE id=?"
    args.append(span_id)
    if only_if:
        sql += " AND status=?"
        args.append(only_if)
    execute(sql, args)


# ---------- 入仓 ----------

def warehouse_insert(batch_id, sample_id, concentration, request_id):
    """INSERT OR IGNORE + UNIQUE(batch_id,sample_id)：迟到结果不会重复入仓。
    已撤回号拒绝入仓(撤回与在途流程交错时兜底);检查与写入同锁,与撤回事务互斥。"""
    with _lock:
        if is_withdrawn(sample_id):
            return False
        cur = execute(
            "INSERT OR IGNORE INTO warehouse(batch_id,sample_id,concentration,stored_at,request_id)"
            " VALUES(?,?,?,?,?)",
            (batch_id, sample_id, concentration, time.time(), request_id))
        return cur.rowcount == 1


# ---------- 配置 ----------

CONFIG_DEFAULTS = {
    "source_delay_ms": 300,      # 源站单次执行延迟
    "source_slots": 3,           # 源站并行名额(槽位)，超出需排队等名额
    "source_fail_first": False,  # 源站每样品首次请求注入 500
    "qc_nonneg_delay_ms": 200,   # 质控①非负值 延迟
    "qc_nonneg_fail_first": False,
    "qc_match_delay_ms": 200,    # 质控②样品号对应 延迟
    "qc_match_fail_first": False,
    "client_timeout_ms": 2000,   # 编排侧取数逻辑超时(超时后重试，旧请求仍可能运行)
    "max_attempts": 3,           # 取数最大尝试次数
}


def get_config():
    row = query_one("SELECT v FROM kv WHERE k='config'")
    cfg = dict(CONFIG_DEFAULTS)
    if row:
        cfg.update(json.loads(row["v"]))
    return cfg


def save_config(cfg):
    execute("INSERT INTO kv(k,v) VALUES('config',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (json.dumps(cfg, ensure_ascii=False),))
