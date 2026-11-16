#!/usr/bin/env python3
"""端到端自测：需先启动三服务(默认编排端 http://localhost:8000)。
覆盖:正常入仓、源站慢(超时/迟到/不重入仓)、等名额、质控慢瓶颈迁移、
源站首次失败、质控失败阻断、NEG/MIX 钩子、多批交错不串记录、
撤回(跨批清除/结论重算/全部撤回标记/取数与质控在途交错/错位返回证据/
迟到上报拦截/重复撤回幂等/撤回后拒绝提交)。"""
import json
import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
FAILS = []


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))
    if not cond:
        FAILS.append(name)


def set_config(c, **kw):
    r = c.post(f"{BASE}/api/config", json=kw, timeout=10)
    r.raise_for_status()
    j = r.json()
    assert j["push"]["source"] == "ok" and j["push"]["qc"] == "ok", f"配置下发失败: {j['push']}"
    return j["config"]


def submit(c, ids):
    r = c.post(f"{BASE}/api/batches", json={"sample_ids": ids}, timeout=10)
    r.raise_for_status()
    return r.json()["id"]


def wait_batch(c, bid, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        d = c.get(f"{BASE}/api/batches/{bid}", timeout=10).json()
        if d["batch"]["status"] != "running":
            return d
        time.sleep(0.4)
    raise TimeoutError(f"batch {bid} still running")


def get_batch(c, bid):
    return c.get(f"{BASE}/api/batches/{bid}", timeout=10).json()


def withdraw(c, sid):
    r = c.post(f"{BASE}/api/withdraw", json={"sample_id": sid}, timeout=10)
    r.raise_for_status()
    return r.json()


def spans_of(d, stage=None, status=None, side=None):
    out = d["spans"]
    if stage:
        out = [s for s in out if s["stage"] == stage]
    if status:
        out = [s for s in out if s["status"] == status]
    if side:
        out = [s for s in out if s["side"] == side]
    return out


def main():
    c = httpx.Client()
    # 0. 服务可达
    for url in (f"{BASE}/api/health",):
        check("编排端健康", c.get(url, timeout=5).json().get("ok") is True)

    # 1. 基线：6 样品全部入仓，关键链存在
    set_config(c, source_delay_ms=300, source_slots=3, source_fail_first=False,
               qc_nonneg_delay_ms=200, qc_nonneg_fail_first=False,
               qc_match_delay_ms=200, qc_match_fail_first=False,
               client_timeout_ms=2000, max_attempts=3)
    ids = [f"T1-{i}" for i in range(6)]
    d = wait_batch(c, submit(c, ids))
    check("基线批次完成", d["batch"]["status"] == "done")
    check("基线全部入仓", len(d["warehouse"]) == 6, f"{len(d['warehouse'])}/6")
    check("基线结论含关键链", bool(d["conclusion"] and d["conclusion"]["chain"]))
    check("基线无迟到/失败", not spans_of(d, status="late") and not spans_of(d, status="failed"))

    # 2. 源站慢(延迟>超时)：出现超时与迟到尝试，迟到不重复入仓
    set_config(c, source_delay_ms=2600, client_timeout_ms=900, max_attempts=3)
    ids = [f"T2-{i}" for i in range(3)]
    d = wait_batch(c, submit(c, ids), timeout=90)
    fetch = spans_of(d, stage="fetch", side="client")
    check("慢源批次仍完成", d["batch"]["status"] == "done")
    check("慢源产生多次尝试", len(fetch) > 3, f"{len(fetch)} 次")
    check("存在迟到/曾被超时放弃的尝试",
          any(s["status"] in ("late", "timeout") or "超时" in (s["note"] or "") for s in fetch))
    check("迟到不重复入仓", len(d["warehouse"]) == 3, f"{len(d['warehouse'])}/3")
    eff = [s for s in fetch if s["status"] == "effective"]
    check("每样品恰一个有效尝试", len(eff) == 3, f"{len(eff)}/3")

    # 3. 等名额：名额=1、8 样品，源站服务端上报 queue_ms>0
    set_config(c, source_delay_ms=300, source_slots=1, client_timeout_ms=5000)
    ids = [f"T3-{i}" for i in range(8)]
    d = wait_batch(c, submit(c, ids), timeout=90)
    srv = spans_of(d, stage="fetch", side="server")
    queued = [s for s in srv if (s.get("queue_ms") or 0) > 50]
    check("等名额批次完成", d["batch"]["status"] == "done")
    check("服务端记录到等名额(queue_ms>50ms)", len(queued) > 0, f"{len(queued)} 条")
    check("瓶颈判为等名额", d["conclusion"]["bottleneck"] == "等名额(源站槽位)",
          str(d["conclusion"]["bottleneck"]))
    set_config(c, source_slots=3)

    # 4. 调慢质控②：瓶颈迁移到质控②
    set_config(c, source_delay_ms=150, qc_match_delay_ms=1500, client_timeout_ms=3000)
    ids = [f"T4-{i}" for i in range(4)]
    d = wait_batch(c, submit(c, ids), timeout=60)
    check("调慢质控②后瓶颈迁移", d["conclusion"]["bottleneck"] == "质控②样品号对应",
          str(d["conclusion"]["bottleneck"]))
    set_config(c, qc_match_delay_ms=200)

    # 5. 源站首次失败：批次完成，存在失败尝试且有效尝试为 #2
    set_config(c, source_fail_first=True)
    ids = [f"T5-{i}" for i in range(3)]
    d = wait_batch(c, submit(c, ids), timeout=60)
    failed_fetch = spans_of(d, stage="fetch", status="failed")
    eff2 = [s for s in spans_of(d, stage="fetch", status="effective") if s["attempt"] == 2]
    check("源站首次失败批次仍完成", d["batch"]["status"] == "done")
    check("记录了失败的首次尝试", len(failed_fetch) >= 3, f"{len(failed_fetch)}")
    check("重试#2成为有效尝试", len(eff2) == 3, f"{len(eff2)}/3")
    set_config(c, source_fail_first=False)

    # 6. 质控①首次失败：批次失败，阻断定位在质控①，不伪造成功链
    set_config(c, qc_nonneg_fail_first=True)
    ids = [f"T6-{i}" for i in range(3)]
    d = wait_batch(c, submit(c, ids), timeout=60)
    check("质控失败批次标记失败", d["batch"]["status"] == "failed")
    check("失败批次无成功链", d["conclusion"]["chain"] is None)
    bl = d["conclusion"]["blocking"] or []
    check("阻断定位在质控①", any(b["stage"] == "qc_nonneg" for b in bl),
          json.dumps(bl, ensure_ascii=False)[:120])
    check("失败批次零入仓", len(d["warehouse"]) == 0)
    set_config(c, qc_nonneg_fail_first=False)

    # 7. 演示钩子：NEG 负浓度 / MIX 错位样品号
    d = wait_batch(c, submit(c, ["HOOK-NEG-1", "HOOK-MIX-1", "HOOK-OK-1"]), timeout=60)
    bl = {b["sample_id"]: b["stage"] for b in (d["conclusion"]["blocking"] or [])}
    check("NEG 阻断于质控①", bl.get("HOOK-NEG-1") == "qc_nonneg", str(bl))
    check("MIX 阻断于质控②", bl.get("HOOK-MIX-1") == "qc_match", str(bl))
    check("正常样品照常入仓", any(w["sample_id"] == "HOOK-OK-1" for w in d["warehouse"]))

    # 8. 多批交错：两批并发，记录不串
    b1 = submit(c, [f"X1-{i}" for i in range(5)])
    b2 = submit(c, [f"X2-{i}" for i in range(5)])
    d1, d2 = wait_batch(c, b1, timeout=60), wait_batch(c, b2, timeout=60)
    check("交错批次均完成", d1["batch"]["status"] == "done" and d2["batch"]["status"] == "done")
    ok1 = all(s["batch_id"] == b1 for s in d1["spans"]) and \
          all(w["batch_id"] == b1 for w in d1["warehouse"])
    ok2 = all(s["batch_id"] == b2 for s in d2["spans"]) and \
          all(w["batch_id"] == b2 for w in d2["warehouse"])
    check("交错批次记录不串", ok1 and ok2)
    pref1 = {w["sample_id"] for w in d1["warehouse"]}
    pref2 = {w["sample_id"] for w in d2["warehouse"]}
    check("入仓样品归属正确", all(x.startswith("X1-") for x in pref1)
          and all(x.startswith("X2-") for x in pref2))

    # ---------- 撤回 ----------
    set_config(c, source_delay_ms=150, source_slots=3, source_fail_first=False,
               qc_nonneg_delay_ms=100, qc_nonneg_fail_first=False,
               qc_match_delay_ms=100, qc_match_fail_first=False,
               client_timeout_ms=3000, max_attempts=3)

    # 9. 跨多批撤回:入仓行/调用记录/批次清单清除,结论按剩余样品重算
    b1 = submit(c, ["W-A", "W-B", "W-C"])
    b2 = submit(c, ["W-B", "W-D"])
    d1, d2 = wait_batch(c, b1), wait_batch(c, b2)
    check("撤回前两批完成", d1["batch"]["status"] == "done" and d2["batch"]["status"] == "done")
    r = withdraw(c, "W-B")
    check("撤回返回影响统计", r["ok"] and r["warehouse_deleted"] == 2
          and set(r["batches_affected"]) == {b1, b2} and not r["batches_withdrawn"],
          json.dumps(r, ensure_ascii=False)[:140])
    d1, d2 = get_batch(c, b1), get_batch(c, b2)
    check("撤回后批次清单剔除该号",
          d1["batch"]["sample_ids"] == ["W-A", "W-C"] and d2["batch"]["sample_ids"] == ["W-D"])
    check("撤回后入仓行已清除",
          all(w["sample_id"] != "W-B" for w in d1["warehouse"] + d2["warehouse"]))
    check("撤回后调用记录已清除",
          all(s["sample_id"] != "W-B" for s in d1["spans"] + d2["spans"]))
    c1 = d1["conclusion"]
    check("撤回后结论按剩余样品重算", c1["status"] == "done" and c1["samples_total"] == 2
          and c1["samples_stored"] == 2, json.dumps(c1)[:100])
    check("撤回后批次仍为完成态",
          d1["batch"]["status"] == "done" and d2["batch"]["status"] == "done")

    # 10. 批次样品全部撤回 → 状态"已撤回",记录清空
    b3 = submit(c, ["W-E"])
    wait_batch(c, b3)
    r = withdraw(c, "W-E")
    d3 = get_batch(c, b3)
    check("全部撤回返回标记", r["batches_withdrawn"] == [b3], json.dumps(r)[:120])
    check("全部撤回批次状态为已撤回", d3["batch"]["status"] == "withdrawn")
    check("已撤回批次记录清空", not d3["spans"] and not d3["warehouse"]
          and d3["batch"]["sample_ids"] == [])
    check("已撤回批次给出撤回结论", bool(d3["conclusion"])
          and d3["conclusion"]["status"] == "withdrawn" and "撤回" in d3["conclusion"]["text"])

    # 11. MIX 错位返回:撤回后错位证据清除;失败批次撤回阻断样品后重算为完成
    b4 = submit(c, ["W-MIX-1", "W-OK-1"])
    d4 = wait_batch(c, b4)
    check("MIX 批次撤回前为失败", d4["batch"]["status"] == "failed")
    check("撤回前存在错位返回证据",
          any(s["sample_id"] == "W-MIX-1-X" for s in d4["spans"]))
    r = withdraw(c, "W-MIX-1")
    check("错位返回号列入撤回名单", "W-MIX-1-X" in r["aliases"], str(r["aliases"]))
    d4 = get_batch(c, b4)
    check("撤回后该号及错位证据清除",
          all("W-MIX-1" not in s["sample_id"] for s in d4["spans"])
          and all("W-MIX-1" not in w["sample_id"] for w in d4["warehouse"]))
    check("撤回后正常样品不受影响",
          any(w["sample_id"] == "W-OK-1" for w in d4["warehouse"]))
    check("失败批次撤回阻断样品后重算为完成", d4["batch"]["status"] == "done")

    # 12. 交错:取数在途时撤回 → 该号不再写入,其他样品继续完成
    set_config(c, source_delay_ms=1500, client_timeout_ms=5000)
    b5 = submit(c, ["W-F", "W-G"])
    time.sleep(0.5)  # 取数在途
    withdraw(c, "W-F")
    d5 = wait_batch(c, b5, timeout=60)
    check("取数在途撤回后批次完成", d5["batch"]["status"] == "done")
    check("取数在途撤回后该号未重新写入",
          all(s["sample_id"] != "W-F" for s in d5["spans"])
          and all(w["sample_id"] != "W-F" for w in d5["warehouse"]))
    check("取数在途撤回后其他样品照常入仓",
          any(w["sample_id"] == "W-G" for w in d5["warehouse"]))
    check("取数在途撤回后批次清单已剔除", d5["batch"]["sample_ids"] == ["W-G"])

    # 13. 交错:质控在途时撤回(含 MIX 错位) → 该号及错位证据不再写入
    set_config(c, source_delay_ms=100, qc_nonneg_delay_ms=2000, qc_match_delay_ms=2000)
    b6 = submit(c, ["W-MIX-2", "W-H"])
    time.sleep(0.8)  # 取数已完成,两项质控在途
    withdraw(c, "W-MIX-2")
    d6 = wait_batch(c, b6, timeout=60)
    check("质控在途撤回后批次完成", d6["batch"]["status"] == "done")
    check("质控在途撤回后该号及错位证据未写入",
          all("W-MIX-2" not in s["sample_id"] for s in d6["spans"])
          and all("W-MIX-2" not in w["sample_id"] for w in d6["warehouse"]))
    check("质控在途撤回后其他样品照常入仓",
          any(w["sample_id"] == "W-H" for w in d6["warehouse"]))
    set_config(c, qc_nonneg_delay_ms=100, qc_match_delay_ms=100)

    # 14. 迟到上报拦截:撤回后到达的服务端跨度(模拟重启后迟到到达)不再写入
    r = c.post(f"{BASE}/api/ingest", json={
        "batch_id": b5, "sample_id": "W-F", "stage": "fetch", "service": "source",
        "attempt": 1, "request_id": "late-req-wf", "status": "ok",
        "start_ts": time.time(), "end_ts": time.time(), "queue_ms": 0}, timeout=10)
    check("撤回后迟到上报被拦截", r.json().get("skipped") == "withdrawn", r.text[:80])
    r = c.post(f"{BASE}/api/ingest", json={
        "batch_id": b6, "sample_id": "W-MIX-2-X", "stage": "qc_match", "service": "qc",
        "attempt": 1, "request_id": "late-req-mix", "status": "ok",
        "start_ts": time.time(), "end_ts": time.time(), "queue_ms": 0}, timeout=10)
    check("撤回后错位号迟到上报被拦截", r.json().get("skipped") == "withdrawn", r.text[:80])
    d5, d6 = get_batch(c, b5), get_batch(c, b6)
    check("迟到证据未重新写入",
          all(s["request_id"] != "late-req-wf" for s in d5["spans"])
          and all(s["request_id"] != "late-req-mix" for s in d6["spans"]))

    # 15. 重复撤回幂等;撤回未知号不报错
    r = withdraw(c, "W-F")
    check("重复撤回幂等", r["ok"] and r["spans_deleted"] == 0 and r["warehouse_deleted"] == 0)
    r = withdraw(c, "W-NEVER-EXIST")
    check("撤回未知号不报错", r["ok"] and r["spans_deleted"] == 0
          and r["batches_affected"] == [])

    # 16. 已撤回号(含错位返回号)后续提交须拒绝
    r = c.post(f"{BASE}/api/batches", json={"sample_ids": ["W-B", "NEW-1"]}, timeout=10)
    check("已撤回号提交被拒", r.status_code == 400 and "W-B" in r.json().get("error", ""))
    r = c.post(f"{BASE}/api/batches", json={"sample_ids": ["W-MIX-1-X"]}, timeout=10)
    check("错位返回号提交亦被拒", r.status_code == 400)
    bid_ok = submit(c, ["NEW-1"])
    check("未撤回号照常提交", wait_batch(c, bid_ok)["batch"]["status"] == "done")

    print()
    if FAILS:
        print(f"共 {len(FAILS)} 项失败: {FAILS}")
        sys.exit(1)
    print("全部自测通过")


if __name__ == "__main__":
    main()
