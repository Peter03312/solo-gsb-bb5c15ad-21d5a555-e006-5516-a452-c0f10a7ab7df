#!/usr/bin/env python3
"""Stepwise inventory execution demo using only the standard library.

Creates an inventory plan with execution_mode="stepwise", then registers the
frozen transfers one by one: next-step-only, exact content, idempotent
retries, per-step stock accounting, completion, plus a second plan aborted
mid-way and a third plan recalled after its first step.

Usage:
    python examples/stepwise_demo.py [base_url]
Default base URL: http://localhost:8000
"""
import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"

with open("examples/inventory_stepwise.json", encoding="utf-8") as fh:
    PLAN_BODY = json.load(fh)


def call(path, body=None, request_id=None, method=None):
    headers = {"Content-Type": "application/json"}
    if request_id:
        headers["X-Request-ID"] = request_id
    data = json.dumps(body).encode() if body is not None else None
    if method is None:
        method = "POST" if body is not None else "GET"
    req = urllib.request.Request(BASE + path, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def register(rid, t):
    return call(f"/inventory/plans/{rid}/steps", {
        "step": t["step"], "source": t["source"],
        "destination": t["destination"], "volume_nl": t["volume_nl"]})


print("1) register batch:")
status, batch = call("/batches",
                     {"id": "S1", "concentration": "1000",
                      "initial_volume_nl": 1_000_000})
print("   HTTP", status)

print("2) create a stepwise plan (whole stock volume reserved up front):")
rid = "stepwise-py-1"
status, plan_doc = call("/inventory/plans", PLAN_BODY, request_id=rid)
print("   HTTP", status, "mode:", plan_doc["execution_mode"],
      "status:", plan_doc["reservation_status"])
transfers = plan_doc["plan"]["transfers"]
print("   progress:", plan_doc["progress"]["registered_count"], "/",
      plan_doc["progress"]["total_steps"], "steps")

print("3) identical create retry replays without a second reservation:")
status, again = call("/inventory/plans", PLAN_BODY, request_id=rid)
print("   HTTP", status, "same:", again == plan_doc)

print("4) register every frozen step in order:")
for t in transfers:
    status, body = register(rid, t)
    moved = body.get("stock_moved_nl")
    note = f"  (stock {moved} nL -> used)" if moved else ""
    print(f"   step {t['step']:2d}: HTTP {status} "
          f"{body['registered_count']}/{body['progress']['total_steps']}"
          f"{note}")

print("5) same step + same content replays (200, changed=false):")
status, body = register(rid, transfers[0])
print("   HTTP", status, "changed:", body["changed"])

print("6) same step with a different volume is refused:")
bad = dict(transfers[0], volume_nl=transfers[0]["volume_nl"] - 100)
status, body = register(rid, bad)
print("   HTTP", status, body["error"])

print("7) jumping a step is refused (shown on the fresh second plan):")
rid2 = "stepwise-py-2"
status, doc = call("/inventory/plans", PLAN_BODY, request_id=rid2)
jump = doc["plan"]["transfers"][1]  # step 2 while step 1 is still pending
status, body = register(rid2, jump)
print("   HTTP", status, body.get("error"),
      "(expected step", str(body.get("expected")) + ")")
# wrong content for the (still-unregistered) next step is refused too
wrong = dict(doc["plan"]["transfers"][0],
             volume_nl=doc["plan"]["transfers"][0]["volume_nl"] - 100)
status, body = register(rid2, wrong)
print("   wrong content -> HTTP", status, body.get("error"))

print("8) whole-order confirm/cancel are refused for stepwise plans:")
for action in ("confirm", "cancel"):
    status, body = call(f"/inventory/plans/{rid}/{action}", body={})
    print(f"   {action}: HTTP", status, body["error"])

print("9) complete only after all steps:")
status, body = call(f"/inventory/plans/{rid}/complete", body={})
print("   HTTP", status, body["status"])
status, body = call(f"/inventory/plans/{rid}/complete", body={})
print("   repeated complete: HTTP", status, "changed:", body["changed"])
status, batch = call("/batches/S1")
print("   batch: used", batch["used_nl"], "reserved", batch["reserved_nl"])

print("10) the second plan is aborted after step 1: used kept, pending "
      "released:")
status, body = register(rid2, doc["plan"]["transfers"][0])
print("    step 1 moved", body["stock_moved_nl"], "nL to used")
status, body = call(f"/inventory/plans/{rid2}/complete", body={})
print("    complete early -> HTTP", status, body["error"])
status, body = call(f"/inventory/plans/{rid2}/abort", body={})
print("    abort -> HTTP", status, body["status"],
      "released:", body["released"])
status, body = call(f"/inventory/plans/{rid2}/abort", body={})
print("    repeated abort -> HTTP", status, "changed:", body["changed"])
status, batch = call("/batches/S1")
print("    batch: used", batch["used_nl"], "reserved", batch["reserved_nl"],
      "available", batch["available_nl"])

print("11) recall blocks further steps but still allows abort:")
rid3 = "stepwise-py-3"
status, doc = call("/inventory/plans", PLAN_BODY, request_id=rid3)
register(rid3, doc["plan"]["transfers"][0])
status, rec = call("/batches/S1/recall",
                   {"reason": "QC aliquot tested positive for contamination"})
print("    recall -> HTTP", status)
entry = next(p for p in rec["report"]["plans"] if p["request_id"] == rid3)
print("    report:", entry["reservation_status"],
      "drawn_nl:", entry["drawn_nl"], "wells:", entry["wells"])
status, body = register(rid3, doc["plan"]["transfers"][1])
print("    next step -> HTTP", status, body["error"])
status, body = call(f"/inventory/plans/{rid3}/abort", body={})
print("    abort -> HTTP", status, body["status"],
      "released:", body["released"])
