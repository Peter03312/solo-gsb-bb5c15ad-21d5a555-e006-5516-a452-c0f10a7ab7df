#!/usr/bin/env bash
# Stepwise inventory execution demo against a locally running service
# (docker compose up).
#
# Creates an inventory plan with execution_mode="stepwise", then the
# experimenter registers each frozen transfer in order. Demonstrates:
#   * only the NEXT frozen step is accepted and content must match exactly;
#   * same step + same content retries replay the original record (200,
#     no double accounting); different content / a jumped step -> 409;
#   * stock-out steps move reserved -> used; diluent/intermediate steps do
#     not post stock accounting;
#   * progress + per-batch used/pending are visible on GET;
#   * whole-order confirm/cancel are explicitly refused (409);
#   * completion requires every step; abort keeps used volume and releases
#     the not-yet-taken reservation once;
#   * after a batch recall, further steps are blocked but abort still works,
#     and the recall report follows the executed path only.
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
RID="stepwise-demo-001"

post() { curl -sS -X POST "$BASE$1" "${@:2}"; }

echo "== register batch S1 (1000 uM, 1 mL) =="
post /batches -H "Content-Type: application/json" \
  -d '{"id":"S1","concentration":"1000","initial_volume_nl":1000000}' \
  | python3 -m json.tool

echo "== create a STEPWISE plan (whole amount reserved atomically) =="
post /inventory/plans -H "Content-Type: application/json" \
  -H "X-Request-ID: $RID" \
  --data @examples/inventory_stepwise.json | python3 -m json.tool

echo "== same request id + same body retries -> 200 (no double reserve) =="
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$BASE/inventory/plans" \
  -H "Content-Type: application/json" -H "X-Request-ID: $RID" \
  --data @examples/inventory_stepwise.json

# Pull the frozen transfers out of the frozen plan and register them one by
# one, in step order, with the exact source/destination/volume.
python3 - "$BASE" "$RID" <<'PY'
import json, sys, urllib.error, urllib.request

BASE, RID = sys.argv[1], sys.argv[2]


def call(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


_, plan_doc = call(f"/inventory/plans/{RID}")
transfers = plan_doc["plan"]["transfers"]
print(f"== frozen plan has {len(transfers)} steps; registering in order ==")
for t in transfers:
    code, body = call(f"/inventory/plans/{RID}/steps", {
        "step": t["step"], "source": t["source"],
        "destination": t["destination"], "volume_nl": t["volume_nl"]})
    moved = body.get("stock_moved_nl")
    tag = f" stock moved {moved} nL" if moved else ""
    print(f"   step {t['step']:2d}: HTTP {code} "
          f"registered={body['registered_count']}/{body['progress']['total_steps']}"
          f"{tag}")

print("== same step + same content again -> 200 replay, changed=false ==")
t = transfers[0]
code, body = call(f"/inventory/plans/{RID}/steps", {
    "step": t["step"], "source": t["source"],
    "destination": t["destination"], "volume_nl": t["volume_nl"]})
print("   HTTP", code, "changed:", body["changed"])

print("== same step with DIFFERENT volume -> 409 step_content_conflict ==")
code, body = call(f"/inventory/plans/{RID}/steps", {
    "step": t["step"], "source": t["source"],
    "destination": t["destination"], "volume_nl": t["volume_nl"] - 100})
print("   HTTP", code, body["error"])

print("== whole-order confirm/cancel are refused for a stepwise plan ==")
for action in ("confirm", "cancel"):
    code, body = call(f"/inventory/plans/{RID}/{action}", {}, method="POST")
    print(f"   {action}: HTTP {code} {body['error']}")

print("== live view: progress and per-batch used/pending ==")
_, body = call(f"/inventory/plans/{RID}")
print("   status:", body["reservation_status"])
print("   progress:", json.dumps(body["progress"]))

print("== complete the fully registered plan -> 201 completed ==")
code, body = call(f"/inventory/plans/{RID}/complete", {}, method="POST")
print("   HTTP", code, body["status"])
code, body = call(f"/inventory/plans/{RID}/complete", {}, method="POST")
print("   repeated complete -> HTTP", code, "changed:", body["changed"])
PY

echo
echo "== second stepwise plan: abort mid-way keeps used, releases pending =="
RID2="stepwise-demo-002"
post /inventory/plans -H "Content-Type: application/json" \
  -H "X-Request-ID: $RID2" \
  --data @examples/inventory_stepwise.json > /dev/null
python3 - "$BASE" "$RID2" <<'PY'
import json, sys, urllib.error, urllib.request

BASE, RID = sys.argv[1], sys.argv[2]


def call(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


_, doc = call(f"/inventory/plans/{RID}")
first = doc["plan"]["transfers"][0]  # a 600 nL stock-out step
code, body = call(f"/inventory/plans/{RID}/steps", {
    "step": first["step"], "source": first["source"],
    "destination": first["destination"],
    "volume_nl": first["volume_nl"]})
print("   registered step 1 ->", code, "stock_moved_nl:",
      body["stock_moved_nl"])

code, body = call(f"/inventory/plans/{RID}/complete", {}, method="POST")
print("   complete while steps remain ->", code, body["error"])

code, body = call(f"/inventory/plans/{RID}/abort", {}, method="POST")
print("   abort ->", code, body["status"], "released:",
      json.dumps(body["released"]))
code, body = call(f"/inventory/plans/{RID}/abort", {}, method="POST")
print("   repeated abort ->", code, "changed:", body["changed"])
_, batch = call("/batches/S1")
print("   batch balances: reserved", batch["reserved_nl"],
      "used", batch["used_nl"], "available", batch["available_nl"])
PY

echo
echo "== recall interaction: a third plan, steps run, then recalled =="
RID3="stepwise-demo-003"
post /inventory/plans -H "Content-Type: application/json" \
  -H "X-Request-ID: $RID3" \
  --data @examples/inventory_stepwise.json > /dev/null
python3 - "$BASE" "$RID3" <<'PY'
import json, sys, urllib.error, urllib.request

BASE, RID = sys.argv[1], sys.argv[2]


def call(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


_, doc = call(f"/inventory/plans/{RID}")
first = doc["plan"]["transfers"][0]
call(f"/inventory/plans/{RID}/steps", {
    "step": first["step"], "source": first["source"],
    "destination": first["destination"],
    "volume_nl": first["volume_nl"]})
code, rec = call("/batches/S1/recall",
                 {"reason": "QC aliquot tested positive for contamination"})
print("   recall ->", code)
for p in rec["report"]["plans"]:
    if p["request_id"] == RID:
        print("   report entry:", json.dumps(p))
code, body = call(f"/inventory/plans/{RID}/steps", {
    "step": doc["plan"]["transfers"][1]["step"],
    "source": doc["plan"]["transfers"][1]["source"],
    "destination": doc["plan"]["transfers"][1]["destination"],
    "volume_nl": doc["plan"]["transfers"][1]["volume_nl"]})
print("   next step after recall ->", code, body["error"])
code, body = call(f"/inventory/plans/{RID}/abort", {}, method="POST")
print("   abort still allowed ->", code, body["status"],
      "released:", json.dumps(body["released"]))
PY
