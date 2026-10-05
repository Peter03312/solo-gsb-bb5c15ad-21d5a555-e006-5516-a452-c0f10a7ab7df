"""Stepwise-execution mode tests.

A stepwise inventory plan keeps the whole frozen transfer plan but consumes
its stock reservation as the lab registers the transfers one at a time:

* only the next step is accepted and its content must equal the frozen step;
  same step + same content retries replay the original registration, while
  different content and step skipping are refused;
* a stock-drawing (mother-liquor) step moves that volume reserved -> used;
  intermediate-liquid steps debit nothing again;
* completion requires every step; abort keeps the used volume and releases
  the not-yet-drawn reservation once; the two are mutually exclusive;
* recalling a touched batch forbids further step registration (abort stays
  allowed), and the recall report follows the executed path (un-drawn mother
  liquor is absent);
* the old whole-order confirm/cancel is explicitly rejected for stepwise
  plans, while whole-mode plans keep their original lifecycle.
"""
import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from tests.test_inventory import BATCH, PLAN, FakePool, FakeStore
import app.db as db
import app.main as main


@pytest.fixture
def store(monkeypatch):
    st = FakeStore()

    async def fake_connect(dsn=None):
        return FakePool()

    pairs = [
        ("connect", fake_connect),
        ("get_frozen", st.get_frozen),
        ("freeze", st.freeze),
        ("create_batch", st.create_batch),
        ("get_batch", st.get_batch),
        ("get_batches", st.get_batches),
        ("list_batches", st.list_batches),
        ("get_reservation", st.get_reservation),
        ("reserve_and_freeze", st.reserve_and_freeze),
        ("transition_reservation", st.transition_reservation),
        ("register_step", st.register_step),
        ("complete_stepwise", st.complete_stepwise),
        ("abort_stepwise", st.abort_stepwise),
        ("recall_batch", st.recall_batch),
        ("get_recall", st.get_recall),
    ]
    for name, fn in pairs:
        monkeypatch.setattr(db, name, fn)
        monkeypatch.setattr(main, name, fn)
    main.app.state.db = FakePool()
    return st


@pytest.fixture
def client(store):
    with TestClient(main.app) as c:
        yield c


@pytest.fixture
def anyio_backend():
    return "asyncio"


SERIAL = {
    "batch_ids": ["S1"],
    "diluent": {"available_nl": 10_000_000},
    "wells": [
        {"id": "A1", "final_volume_nl": 1000, "final_concentration": "400"},
        {"id": "A2", "final_volume_nl": 1000, "final_concentration": "100"},
    ],
    "containers": [
        {"id": "tube-1", "capacity_nl": 10_000},
        {"id": "tube-2", "capacity_nl": 20_000},
    ],
    "pipette": {"min_nl": 500, "max_nl": 1000, "step_nl": 100},
    "default_well_capacity_nl": 2000,
    "execution_mode": "stepwise",
}
SERIAL_DRAWN = 1100  # steps 1 (600) and 3 (500) draw stock S1
RECALL_BODY = {"reason": "contamination"}


def _serial(client, rid, batch_conc="1000", batch_vol=1_000_000):
    client.post("/batches", json={"id": "S1", "concentration": batch_conc,
                                  "initial_volume_nl": batch_vol})
    r = client.post("/inventory/plans", json=SERIAL,
                    headers={"X-Request-ID": rid})
    assert r.status_code == 201, r.text
    return r.json()


def _reg(client, rid, steps, count, start=1):
    for i in range(start, count + 1):
        t = steps[i - 1]
        r = client.post(f"/inventory/plans/{rid}/steps/{i}", json={
            "step": i, "source": t["source"],
            "destination": t["destination"], "volume_nl": t["volume_nl"]})
        assert r.status_code == 201, (i, r.text)
    return r


# --------------------------------------------------------------------------- #
# Creation
# --------------------------------------------------------------------------- #


def test_stepwise_plan_created_with_progress(client):
    body = _serial(client, "sw-create")
    assert body["execution_mode"] == "stepwise"
    assert body["progress"] == {
        "status": "reserved", "executed_steps": 0,
        "total_steps": len(body["plan"]["transfers"]),
        "all_steps_registered": False}
    assert body["batch_balances"] == [
        {"batch_id": "S1", "reserved_nl": SERIAL_DRAWN,
         "used_nl": 0, "pending_nl": SERIAL_DRAWN}]
    # whole reservation is taken up front (same as whole mode)
    batch = client.get("/batches/S1").json()
    assert (batch["reserved_nl"], batch["used_nl"]) == (SERIAL_DRAWN, 0)


def test_default_execution_mode_is_whole(client):
    client.post("/batches", json=BATCH)
    r = client.post("/inventory/plans", json=PLAN,
                    headers={"X-Request-ID": "sw-default"})
    assert r.status_code == 201
    assert r.json()["execution_mode"] == "whole"
    assert "steps" not in r.json()
    # whole lifecycle still works
    assert client.post(
        "/inventory/plans/sw-default/confirm").status_code == 200


def test_execution_mode_participates_in_idempotency_conflict(client):
    client.post("/batches", json=BATCH)
    assert client.post("/inventory/plans", json=PLAN,
                       headers={"X-Request-ID": "sw-modex"}).status_code == 201
    changed = json.loads(json.dumps(PLAN))
    changed["execution_mode"] = "stepwise"
    r = client.post("/inventory/plans", json=changed,
                    headers={"X-Request-ID": "sw-modex"})
    assert r.status_code == 409
    assert r.json()["error"] == "idempotency_conflict"


def test_stepwise_same_id_replay_does_not_reregister(client):
    first = _serial(client, "sw-replay")
    r = client.post("/inventory/plans", json=SERIAL,
                    headers={"X-Request-ID": "sw-replay"})
    assert r.status_code == 200
    assert r.json() == first
    assert client.get("/batches/S1").json()["reserved_nl"] == SERIAL_DRAWN


# --------------------------------------------------------------------------- #
# Step registration: next step only, content must match
# --------------------------------------------------------------------------- #


def test_only_next_step_accepted(client):
    body = _serial(client, "sw-next")
    steps = body["plan"]["transfers"]
    t = steps[1]
    r = client.post("/inventory/plans/sw-next/steps/2", json={
        "step": 2, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"]})
    assert r.status_code == 409
    assert r.json()["error"] == "step_out_of_order"
    assert r.json()["expected_step"] == 1


def test_step_content_must_match_frozen_step(client):
    body = _serial(client, "sw-content")
    t = body["plan"]["transfers"][0]
    r = client.post("/inventory/plans/sw-content/steps/1", json={
        "step": 1, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"] + 100})
    assert r.status_code == 409
    assert r.json()["error"] == "step_transfer_mismatch"
    assert r.json()["expected"]["volume_nl"] == t["volume_nl"]
    # different source / destination are refused too
    wrong = {"step": 1, "source": {"kind": "diluent"},
             "destination": t["destination"], "volume_nl": t["volume_nl"]}
    assert client.post("/inventory/plans/sw-content/steps/1",
                       json=wrong).status_code == 409
    # nothing was registered or debited
    assert client.get("/batches/S1").json()["used_nl"] == 0
    live = client.get("/inventory/plans/sw-content").json()
    assert live["progress"]["executed_steps"] == 0


def test_path_step_must_match_body_step(client):
    body = _serial(client, "sw-path")
    t = body["plan"]["transfers"][0]
    r = client.post("/inventory/plans/sw-content/x/steps/1", json={
        "step": 2, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"]})
    assert r.status_code in (400, 404, 422)  # malformed path, never executes
    r = client.post("/inventory/plans/sw-path/steps/1", json={
        "step": 2, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"]})
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_request"


def test_step_number_out_of_range(client):
    body = _serial(client, "sw-range")
    n = len(body["plan"]["transfers"])
    t = body["plan"]["transfers"][0]
    r = client.post(f"/inventory/plans/sw-range/steps/{n + 1}", json={
        "step": n + 1, "source": t["source"],
        "destination": t["destination"], "volume_nl": t["volume_nl"]})
    assert r.status_code == 409
    assert r.json()["error"] == "step_out_of_range"


def test_same_step_same_content_replays_original(client):
    body = _serial(client, "sw-idem")
    steps = body["plan"]["transfers"]
    t = steps[0]
    payload = {"step": 1, "source": t["source"],
               "destination": t["destination"], "volume_nl": t["volume_nl"]}
    r1 = client.post("/inventory/plans/sw-idem/steps/1", json=payload)
    r2 = client.post("/inventory/plans/sw-idem/steps/1", json=payload)
    assert r1.status_code == 201
    assert r2.status_code == 200
    assert r2.json()["replayed"] is True
    assert r2.json()["registration"]["volume_nl"] == t["volume_nl"]
    # only one debit happened
    assert client.get("/batches/S1").json()["used_nl"] == 600


def test_same_step_different_content_after_registration_rejected(client):
    body = _serial(client, "sw-redo")
    steps = body["plan"]["transfers"]
    t = steps[0]
    client.post("/inventory/plans/sw-redo/steps/1", json={
        "step": 1, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"]})
    r = client.post("/inventory/plans/sw-redo/steps/1", json={
        "step": 1, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"] + 100})
    assert r.status_code == 409
    assert r.json()["error"] == "step_transfer_mismatch"
    assert client.get("/batches/S1").json()["used_nl"] == 600


def test_step_on_unknown_or_whole_plan(client):
    assert client.post("/inventory/plans/nope/steps/1", json={
        "step": 1, "source": {"kind": "stock", "id": "S1"},
        "destination": {"kind": "well", "id": "A1"},
        "volume_nl": 100}).status_code == 404

    client.post("/batches", json=BATCH)
    client.post("/inventory/plans", json=PLAN,
                headers={"X-Request-ID": "sw-whole"})
    r = client.post("/inventory/plans/sw-whole/steps/1", json={
        "step": 1, "source": {"kind": "stock", "id": "S1"},
        "destination": {"kind": "well", "id": "A1"}, "volume_nl": 600})
    assert r.status_code == 409
    assert r.json()["error"] == "execution_mode_conflict"


# --------------------------------------------------------------------------- #
# Accounting: stock steps debit, intermediate steps do not
# --------------------------------------------------------------------------- #


def test_stock_steps_move_reserved_to_used_intermediate_do_not(client):
    body = _serial(client, "sw-acct")
    steps = body["plan"]["transfers"]
    # step 1 draws 600 nL of stock
    _reg(client, "sw-acct", steps, 1)
    batch = client.get("/batches/S1").json()
    assert (batch["reserved_nl"], batch["used_nl"],
            batch["available_nl"]) == (500, 600, 1_000_000 - SERIAL_DRAWN)
    live = client.get("/inventory/plans/sw-acct").json()
    assert live["progress"]["executed_steps"] == 1
    assert live["batch_balances"] == [{
        "batch_id": "S1", "reserved_nl": 500, "used_nl": 600,
        "pending_nl": 500}]
    assert len(live["steps"]) == 1

    # step 2 is diluent -> intermediate: no batch debit at all
    _reg(client, "sw-acct", steps, 2, start=2)
    batch = client.get("/batches/S1").json()
    assert (batch["reserved_nl"], batch["used_nl"]) == (500, 600)

    # step 3 draws another 500 nL of stock
    _reg(client, "sw-acct", steps, 3, start=3)
    batch = client.get("/batches/S1").json()
    assert (batch["reserved_nl"], batch["used_nl"]) == (0, SERIAL_DRAWN)

    # remaining steps are diluent/intermediate transfers: no further debit
    _reg(client, "sw-acct", steps, len(steps), start=4)
    batch = client.get("/batches/S1").json()
    assert (batch["reserved_nl"], batch["used_nl"]) == (0, SERIAL_DRAWN)
    live = client.get("/inventory/plans/sw-acct").json()
    assert live["progress"] == {
        "status": "reserved", "executed_steps": len(steps),
        "total_steps": len(steps), "all_steps_registered": True}
    assert live["batch_balances"][0]["pending_nl"] == 0


# --------------------------------------------------------------------------- #
# Complete / abort
# --------------------------------------------------------------------------- #


def test_complete_requires_all_steps_and_is_idempotent(client):
    body = _serial(client, "sw-done")
    steps = body["plan"]["transfers"]
    _reg(client, "sw-done", steps, len(steps) - 1)
    r = client.post("/inventory/plans/sw-done/complete")
    assert r.status_code == 409
    assert r.json()["error"] == "steps_incomplete"
    assert r.json()["executed_steps"] == len(steps) - 1

    _reg(client, "sw-done", steps, len(steps), start=len(steps))
    r = client.post("/inventory/plans/sw-done/complete")
    assert r.status_code == 200 and r.json()["status"] == "completed"
    r = client.post("/inventory/plans/sw-done/complete")
    assert r.status_code == 200 and r.json()["changed"] is False
    # balances unchanged by completion
    assert client.get("/batches/S1").json()["used_nl"] == SERIAL_DRAWN
    # further registration is refused
    t = steps[0]
    r = client.post("/inventory/plans/sw-done/steps/1", json={
        "step": 1, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"]})
    assert r.status_code == 409
    assert r.json()["error"] == "stepwise_state_conflict"


def test_abort_keeps_used_and_releases_remaining_once(client):
    body = _serial(client, "sw-abort")
    steps = body["plan"]["transfers"]
    _reg(client, "sw-abort", steps, 1)  # drew 600; 500 still reserved
    r = client.post("/inventory/plans/sw-abort/abort")
    assert r.status_code == 200
    assert r.json()["released"] == [{"batch_id": "S1", "released_nl": 500}]
    batch = client.get("/batches/S1").json()
    assert (batch["reserved_nl"], batch["used_nl"],
            batch["available_nl"]) == (0, 600, 1_000_000 - 600)
    # idempotent: no second release
    r = client.post("/inventory/plans/sw-abort/abort")
    assert r.status_code == 200 and r.json()["changed"] is False
    assert client.get("/batches/S1").json()["available_nl"] == 1_000_000 - 600
    # abort after nothing-drawn releases the full reservation
    body2 = _serial(client, "sw-abort2")
    r = client.post("/inventory/plans/sw-abort2/abort")
    assert r.json()["released"] == [
        {"batch_id": "S1", "released_nl": SERIAL_DRAWN}]
    assert client.get("/batches/S1").json()["used_nl"] == 600


def test_complete_and_abort_are_mutually_exclusive(client):
    body = _serial(client, "sw-mut")
    steps = body["plan"]["transfers"]
    _reg(client, "sw-mut", steps, len(steps))
    client.post("/inventory/plans/sw-mut/complete")
    r = client.post("/inventory/plans/sw-mut/abort")
    assert r.status_code == 409
    assert r.json()["error"] == "stepwise_state_conflict"


def test_terminal_routes_on_unknown_and_whole_plans(client):
    assert client.post("/inventory/plans/nope/complete").status_code == 404
    assert client.post("/inventory/plans/nope/abort").status_code == 404
    client.post("/batches", json=BATCH)
    client.post("/inventory/plans", json=PLAN,
                headers={"X-Request-ID": "sw-whole2"})
    assert client.post(
        "/inventory/plans/sw-whole2/complete").status_code == 409
    assert client.post(
        "/inventory/plans/sw-whole2/abort").status_code == 409
    # whole lifecycle still intact
    assert client.post(
        "/inventory/plans/sw-whole2/confirm").status_code == 200


# --------------------------------------------------------------------------- #
# Old whole-order confirm/cancel explicitly refused on stepwise plans
# --------------------------------------------------------------------------- #


def test_whole_confirm_cancel_refused_in_every_state(client):
    body = _serial(client, "sw-oldapi")
    steps = body["plan"]["transfers"]
    _reg(client, "sw-oldapi", steps, len(steps))
    client.post("/inventory/plans/sw-oldapi/complete")
    for path in ("confirm", "cancel"):
        r = client.post(f"/inventory/plans/sw-oldapi/{path}")
        assert r.status_code == 409
        assert r.json()["error"] == "execution_mode_conflict"

    body2 = _serial(client, "sw-oldapi2")
    r = client.post("/inventory/plans/sw-oldapi2/cancel")
    assert r.status_code == 409
    assert r.json()["error"] == "execution_mode_conflict"
    # the reservation was NOT cancelled: its drawn volume is still reserved
    assert client.get("/batches/S1").json()["reserved_nl"] == SERIAL_DRAWN


# --------------------------------------------------------------------------- #
# Recall interactions
# --------------------------------------------------------------------------- #


def test_recall_blocks_steps_but_abort_still_allowed(client):
    body = _serial(client, "sw-rc")
    steps = body["plan"]["transfers"]
    _reg(client, "sw-rc", steps, 1)
    client.post("/batches/S1/recall", json=RECALL_BODY)
    t = steps[1]
    r = client.post("/inventory/plans/sw-rc/steps/2", json={
        "step": 2, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"]})
    assert r.status_code == 409
    assert r.json()["error"] == "batch_recalled"
    # used 600 stands; the remaining 500 reservation is releasable via abort
    r = client.post("/inventory/plans/sw-rc/abort")
    assert r.status_code == 200
    assert r.json()["released"] == [{"batch_id": "S1", "released_nl": 500}]
    batch = client.get("/batches/S1").json()
    assert (batch["reserved_nl"], batch["used_nl"]) == (0, 600)
    # steps stay blocked after abort (terminal)
    r = client.post("/inventory/plans/sw-rc/steps/2", json={
        "step": 2, "source": t["source"], "destination": t["destination"],
        "volume_nl": t["volume_nl"]})
    assert r.status_code == 409


def test_recall_report_follows_executed_path(client):
    body = _serial(client, "sw-rp")
    steps = body["plan"]["transfers"]
    # plan A: only stock drawn into the first intermediate -- no well reached
    client.post("/inventory/plans", json=SERIAL,
                headers={"X-Request-ID": "pa"})
    _reg(client, "pa", steps, 2)
    # plan B: executed through step 9 -> A1 reached, A2 not yet
    client.post("/inventory/plans", json=SERIAL,
                headers={"X-Request-ID": "pb"})
    _reg(client, "pb", steps, 9)
    # plan C: nothing executed -> entirely absent
    client.post("/inventory/plans", json=SERIAL,
                headers={"X-Request-ID": "pc"})
    # plan D: fully executed and completed -> both wells
    client.post("/inventory/plans", json=SERIAL,
                headers={"X-Request-ID": "pd"})
    _reg(client, "pd", steps, 10)
    client.post("/inventory/plans/pd/complete")
    # plan E: one stock step then aborted -> kept in report, no well
    client.post("/inventory/plans", json=SERIAL,
                headers={"X-Request-ID": "pe"})
    _reg(client, "pe", steps, 1)
    client.post("/inventory/plans/pe/abort")

    report = client.post("/batches/S1/recall",
                         json=RECALL_BODY).json()["report"]
    by_id = {p["request_id"]: p for p in report["plans"]}
    assert set(by_id) == {"pa", "pb", "pd", "pe"}
    assert by_id["pa"]["wells"] == []
    assert by_id["pa"]["drawn_nl"] == 600
    assert by_id["pb"]["wells"] == ["A1"]
    assert by_id["pb"]["drawn_nl"] == SERIAL_DRAWN
    assert by_id["pb"]["reservation_status"] == "reserved"
    assert by_id["pd"]["wells"] == ["A1", "A2"]
    assert by_id["pd"]["reservation_status"] == "completed"
    assert by_id["pe"]["wells"] == []
    assert by_id["pe"]["reservation_status"] == "aborted"
    assert all(p["execution_mode"] == "stepwise" for p in by_id.values())


def test_whole_mode_recall_report_unchanged(client):
    # regression: whole-mode report entry has no execution_mode field and
    # reserved/confirmed plans are listed with the full reservation amount
    client.post("/batches", json={"id": "S1", "concentration": "1000",
                                  "initial_volume_nl": 1_000_000})
    whole_body = dict(SERIAL)
    whole_body.pop("execution_mode")
    client.post("/inventory/plans", json=whole_body,
                headers={"X-Request-ID": "wa"})
    report = client.post("/batches/S1/recall",
                         json=RECALL_BODY).json()["report"]
    entry = report["plans"][0]
    assert entry["request_id"] == "wa"
    assert "execution_mode" not in entry
    assert entry["drawn_nl"] == SERIAL_DRAWN
    assert entry["wells"] == ["A1", "A2"]


# --------------------------------------------------------------------------- #
# Concurrency: same-step registrations and recall arbitration
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_concurrent_same_step_registers_once(store, anyio_backend):
    await store.create_batch(None, "S1", "1000", 1_000_000)
    transport = httpx.ASGITransport(app=main.app)

    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        body = await ac.post("/inventory/plans", json=SERIAL,
                             headers={"X-Request-ID": "cc-sw"})
        steps = body.json()["plan"]["transfers"]
        t = steps[0]
        payload = {"step": 1, "source": t["source"],
                   "destination": t["destination"],
                   "volume_nl": t["volume_nl"]}
        results = await asyncio.gather(*[
            ac.post("/inventory/plans/cc-sw/steps/1", json=payload)
            for _ in range(6)
        ])
        codes = sorted(r.status_code for r in results)
        assert codes == [200, 200, 200, 200, 200, 201]
        batch = (await ac.get("/batches/S1")).json()
        assert (batch["reserved_nl"], batch["used_nl"]) == (500, 600)
        live = (await ac.get("/inventory/plans/cc-sw")).json()
        assert live["progress"]["executed_steps"] == 1


@pytest.mark.anyio
async def test_concurrent_step_and_recall_linearize(store, anyio_backend):
    await store.create_batch(None, "S1", "1000", 1_000_000)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        body = await ac.post("/inventory/plans", json=SERIAL,
                             headers={"X-Request-ID": "rc-sw"})
        steps = body.json()["plan"]["transfers"]
        t = steps[0]
        payload = {"step": 1, "source": t["source"],
                   "destination": t["destination"],
                   "volume_nl": t["volume_nl"]}
        step_res, recall_res = await asyncio.gather(
            ac.post("/inventory/plans/rc-sw/steps/1", json=payload),
            ac.post("/batches/S1/recall", json=RECALL_BODY),
        )
        b = store._pub(store.batches["S1"])
        if step_res.status_code == 201:
            # step linearized first: it drew 600, and the frozen report
            # follows the executed path (drawn 600, no well reached yet)
            assert (b["used_nl"], b["reserved_nl"]) == (600, 500)
            entry = recall_res.json()["report"]["plans"][0]
            assert entry["drawn_nl"] == 600
            assert entry["wells"] == []
        else:
            # recall linearized first: the step was rejected on authoritative
            # batch state; abort remains the only way out
            assert step_res.status_code == 409
            assert step_res.json()["error"] == "batch_recalled"
            assert (b["used_nl"], b["reserved_nl"]) == (0, SERIAL_DRAWN)
            assert recall_res.json()["report"]["plans"] == []
        abort = await ac.post("/inventory/plans/rc-sw/abort")
        assert abort.status_code == 200
