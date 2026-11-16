"""诊断网页与编排 API：提交批次、查询批次/时间线、运行时调参并下发到源站/质控。"""
import asyncio
import json
import os
import time

import httpx
from fastapi import Body, FastAPI
from fastapi.responses import FileResponse, JSONResponse

import analysis
import db
import pipeline

SOURCE_URL = os.environ.get("SOURCE_URL", "http://localhost:8001")
QC_URL = os.environ.get("QC_URL", "http://localhost:8002")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

app = FastAPI(title="样品入仓链路诊断")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.post("/api/batches")
async def create_batch(body: dict = Body(...)):
    raw = body.get("sample_ids") or []
    ids = list(dict.fromkeys(str(s).strip() for s in raw if str(s).strip()))
    if not ids:
        return JSONResponse({"error": "样品号清单为空"}, status_code=400)
    wd = [s for s in ids if db.is_withdrawn(s)]
    if wd:
        return JSONResponse(
            {"error": f"样品号已撤回,拒绝提交: {', '.join(wd)}", "withdrawn": wd},
            status_code=400)
    cur = db.execute(
        "INSERT INTO batches(created_at, sample_ids, status) VALUES(?,?,?)",
        (time.time(), json.dumps(ids, ensure_ascii=False), "running"))
    bid = cur.lastrowid
    asyncio.create_task(pipeline.run_batch(bid, ids))
    return {"id": bid, "samples": len(ids)}


@app.post("/api/withdraw")
async def withdraw_sample(body: dict = Body(...)):
    """按委托样品号撤回:单事务清除全部批次的入仓行、关联调用记录(含错位返回证据)
    与批次清单,按剩余样品重算结论;样品全部撤回的批次标记"已撤回"。
    撤回名单(含错位返回号)持久化:后续提交拒绝,在途/迟到证据不再写入。"""
    sid = str(body.get("sample_id", "")).strip()
    if not sid:
        return JSONResponse({"error": "样品号为空"}, status_code=400)
    with db.atomic():
        # 关联请求:该号全部调用记录与入仓记录对应的 request_id
        req_ids = {r["request_id"] for r in db.query(
            "SELECT DISTINCT request_id FROM spans WHERE sample_id=?", (sid,))}
        req_ids |= {r["request_id"] for r in db.query(
            "SELECT DISTINCT request_id FROM warehouse WHERE sample_id=?", (sid,))}
        # 错位返回证据:同一 request_id 下服务端可能以错位号(如 MIX 钩子的 -X 后缀)上报;
        # 质控在途时错位号尚未落库,再从取数响应(detail.response.sample_id)中收集
        aliases = {sid}
        if req_ids:
            ph = ",".join("?" * len(req_ids))
            aliases |= {r["sample_id"] for r in db.query(
                f"SELECT DISTINCT sample_id FROM spans WHERE request_id IN ({ph})",
                tuple(req_ids))}
        for r in db.query("SELECT detail FROM spans WHERE sample_id=? AND stage='fetch'",
                          (sid,)):
            try:
                ret = (json.loads(r["detail"] or "{}").get("response") or {}).get("sample_id")
                if ret:
                    aliases.add(str(ret))
            except Exception:
                pass
        if req_ids:
            ph = ",".join("?" * len(req_ids))
            n_sp = db.execute(
                f"DELETE FROM spans WHERE sample_id=? OR request_id IN ({ph})",
                (sid, *req_ids)).rowcount
        else:
            n_sp = db.execute("DELETE FROM spans WHERE sample_id=?", (sid,)).rowcount
        n_wh = db.execute("DELETE FROM warehouse WHERE sample_id=?", (sid,)).rowcount
        # 批次清单剔除该号;全部撤回的批次标记 withdrawn
        affected = []
        for b in db.query("SELECT id, sample_ids, status FROM batches"):
            ids = json.loads(b["sample_ids"])
            if sid not in ids:
                continue
            ids = [x for x in ids if x != sid]
            affected.append({"id": b["id"], "status": b["status"], "remaining": ids})
            if ids:
                db.execute("UPDATE batches SET sample_ids=? WHERE id=?",
                           (json.dumps(ids, ensure_ascii=False), b["id"]))
            else:
                db.execute(
                    "UPDATE batches SET sample_ids='[]', status='withdrawn',"
                    " finished_at=COALESCE(finished_at, ?), conclusion=? WHERE id=?",
                    (time.time(),
                     json.dumps({"status": "withdrawn",
                                 "text": f"批次#{b['id']} 的委托样品已全部撤回。"},
                                    ensure_ascii=False), b["id"]))
        # 撤回名单(含错位返回号)与在途 request_id:后续提交拒绝,迟到/在途证据拦截;
        # 均持久化于 SQLite,重启后仍生效
        for a in aliases:
            db.register_withdrawn(a, alias_of=None if a == sid else sid)
        for rid in req_ids:
            db.register_withdrawn_request(rid)
        # 已结束批次按剩余样品重算结论;running 批次由流水线定稿时按新清单实时重算
        for a in affected:
            if a["remaining"] and a["status"] != "running":
                concl = analysis.analyze(a["id"])
                db.execute("UPDATE batches SET status=?, conclusion=? WHERE id=?",
                           ("done" if concl.get("status") == "done" else "failed",
                            json.dumps(concl, ensure_ascii=False), a["id"]))
    return {"ok": True, "sample_id": sid, "aliases": sorted(aliases),
            "spans_deleted": n_sp, "warehouse_deleted": n_wh,
            "batches_affected": [a["id"] for a in affected],
            "batches_withdrawn": [a["id"] for a in affected if not a["remaining"]]}


@app.get("/api/batches")
async def list_batches():
    rows = db.query("SELECT * FROM batches ORDER BY id DESC LIMIT 100")
    out = []
    for b in rows:
        item = {"id": b["id"], "status": b["status"], "created_at": b["created_at"],
                "finished_at": b["finished_at"], "samples": len(json.loads(b["sample_ids"]))}
        if b["conclusion"]:
            c = json.loads(b["conclusion"])
            item.update({"bottleneck": c.get("bottleneck"), "wall_s": c.get("wall_s"),
                         "stored": c.get("samples_stored")})
        out.append(item)
    return out


@app.get("/api/batches/{bid}")
async def batch_detail(bid: int):
    b = db.query_one("SELECT * FROM batches WHERE id=?", (bid,))
    if not b:
        return JSONResponse({"error": "not found"}, status_code=404)
    spans = db.query("SELECT * FROM spans WHERE batch_id=? ORDER BY start_ts,id", (bid,))
    wh = db.query("SELECT * FROM warehouse WHERE batch_id=? ORDER BY stored_at", (bid,))
    # 结论按当前记录实时重算：迟到跨度随时可能补记，看到的一定与记录一致
    concl = analysis.analyze(bid) if (spans or b["status"] == "withdrawn") else None
    return {
        "batch": {"id": b["id"], "status": b["status"], "created_at": b["created_at"],
                  "finished_at": b["finished_at"], "sample_ids": json.loads(b["sample_ids"])},
        "spans": spans,
        "warehouse": wh,
        "conclusion": concl,
    }


@app.post("/api/ingest")
async def ingest(body: dict = Body(...)):
    """接收源站/质控的服务端跨度(含等名额 queue_ms)，按 request_id 与客户端跨度关联。
    已撤回号(含错位返回号及其在途 request_id)的迟到/在途上报直接丢弃,不再写入;
    检查与写入在同一事务内,与撤回事务互斥。"""
    sid = str(body.get("sample_id", ""))
    with db.atomic():
        if db.is_withdrawn(sid) or db.is_withdrawn_request(body.get("request_id", "")):
            return {"ok": True, "skipped": "withdrawn", "sample_id": sid}
        try:
            db.execute(
                "INSERT INTO spans(batch_id,sample_id,stage,side,service,attempt,request_id,"
                "status,start_ts,end_ts,queue_ms,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (int(body.get("batch_id", 0)), sid,
                 str(body.get("stage", "")), "server", str(body.get("service", "")),
                 int(body.get("attempt", 1)), str(body.get("request_id", "")),
                 str(body.get("status", "ok")), float(body.get("start_ts", 0)),
                 body.get("end_ts"), body.get("queue_ms"),
                 json.dumps(body.get("detail"), ensure_ascii=False)
                 if body.get("detail") is not None else None))
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    return {"ok": True}


@app.get("/api/config")
async def get_config():
    return db.get_config()


@app.post("/api/config")
async def set_config(body: dict = Body(...)):
    cfg = db.get_config()
    for k in db.CONFIG_DEFAULTS:
        if k in body:
            cfg[k] = body[k]
    db.save_config(cfg)
    push = await _push_config(cfg)
    return {"config": cfg, "push": push}


async def _push_config(cfg):
    """把运行时参数下发到源站与质控服务；服务不可达时如实返回错误。"""
    out = {}
    async with httpx.AsyncClient(timeout=3) as c:
        try:
            r = await c.post(f"{SOURCE_URL}/config", json={
                "delay_ms": cfg["source_delay_ms"],
                "slots": cfg["source_slots"],
                "fail_first": cfg["source_fail_first"]})
            out["source"] = "ok" if r.status_code == 200 else f"http {r.status_code}"
        except Exception as e:
            out["source"] = f"error: {e}"
        try:
            r = await c.post(f"{QC_URL}/config", json={"checks": {
                "nonneg": {"delay_ms": cfg["qc_nonneg_delay_ms"],
                           "fail_first": cfg["qc_nonneg_fail_first"]},
                "match": {"delay_ms": cfg["qc_match_delay_ms"],
                          "fail_first": cfg["qc_match_fail_first"]}}})
            out["qc"] = "ok" if r.status_code == 200 else f"http {r.status_code}"
        except Exception as e:
            out["qc"] = f"error: {e}"
    return out


@app.get("/api/health")
async def health():
    return {"ok": True}
