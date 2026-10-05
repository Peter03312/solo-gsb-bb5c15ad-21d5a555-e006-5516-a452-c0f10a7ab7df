"""Stepwise inventory execution tests.

A stepwise plan is created with ``execution_mode: "stepwise"``; instead of a
whole-order confirm, the experimenter registers each frozen transfer in
order (request id + frozen step number + actual source/destination/volume).

Rules under test:

* only the next step is accepted, and its content must match the frozen step;
* same step + same content retries replay the original record (no double
  accounting); different content or a jumped step is rejected;
* a stock-source registration moves that step's volume reserved -> used;
  intermediate / diluent steps post no inventory movement;
* the plan can be completed only after every step is registered; abort
  keeps used volume and releases the not-yet-taken reservation exactly once;
  the two are mutually exclusive;
* a recalled related batch forbids further registration (but not abort);
  the recall race is adjudicated on authoritative batch state;
* the recall report for a partially executed plan follows the executed path
  and never counts not-yet-taken stock;
* whole-order confirm/cancel on stepwise plans is explicitly rejected;
* queries show progress plus per-batch used / pending volumes.
"""
import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from app.planning import (
    registered_stock_usage,
    stepwise_progress,
    transfers_match,
)
from tests.test_inventory import FakePool, FakeStore
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


BATCH = {"id": "S1", "concentration": "1000", "initial_volume_nl": 1_000_000}

# Two-well serial dilution: stock S1 -> intermediate -> wells A1/A2.
STEP_BODY = {
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

RECALL_BODY = {"reason": "QC aliquot tested positive for contamination"}


def _create(client, rid="sw-1", body=None):
    return client.post("/inventory/plans", json=body or STEP_BODY,
                       headers={"X-Request-ID": rid})


def _steps(client, rid):
    return client.get(f"/inventory/plans/{rid}").json()["plan"]["transfers"]


def _reg(client, rid, transfer):
    return client.post(f"/inventory/plans/{rid}/steps", json={
        "step": transfer["step"],
        "source": transfer["source"],
        "destination": transfer["destination"],
        "volume_nl": transfer["volume_nl"],
    })


def _register_all(client, rid, transfers):
    return [_reg(client, rid, t) for t in transfers]


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_transfers_match_requires_exact_content():
    t = {"step": 1,
         "source": {"kind": "stock", "id": "S1", "container": None},
         "destination": {"kind": "intermediate", "id": "I1",
                         "container": "tube-2"},
         "volume_nl": 600}
    assert transfers_match(t, t["source"], t["destination"], 600)
    assert not transfers_match(t, t["source"], t["destination"], 500)
    other_src = {"kind": "stock", "id": "S2", "container": None}
    assert not transfers_match(t, other_src, t["destination"], 600)
    other_dst = {"kind": "well", "id": "A1", "container": None}
    assert not transfers_match(t, t["source"], other_dst, 600)


def test_registered_stock_usage_counts_only_stock_steps_once():
    steps = [
        {"step": 1, "source": {"kind": "stock", "id": "S1"},
         "volume_nl": 600},
        {"step": 2, "source": {"kind": "diluent"}, "volume_nl": 900},
        {"step": 3, "source": {"kind": "stock", "id": "S1"},
         "volume_nl": 500},
        {"step": 4, "source": {"kind": "intermediate", "container": "C"},
         "volume_nl": 1000},
        # duplicate step number must not be counted twice
        {"step": 1, "source": {"kind": "stock", "id": "S1"},
         "volume_nl": 600},
    ]
    assert registered_stock_usage(steps) == {"S1": 1100}


# --------------------------------------------------------------------------- #
# Creation
# --------------------------------------------------------------------------- #


def test_stepwise_plan_starts_reserved_and_announces_mode(client):
    client.post("/batches", json=BATCH)
    r = _create(client)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["execution_mode"] == "stepwise"
    assert body["reservation_status"] == "stepwise"
    assert body["progress"]["registered_count"] == 0
    assert body["progress"]["next_step"] == 1
    assert body["progress"]["complete"] is False
    # the whole plan volume is reserved up front
    assert client.get("/batches/S1").json()["reserved_nl"] == 1100
    assert client.get("/batches/S1").json()["used_nl"] == 0


def test_default_mode_remains_whole(client):
    client.post("/batches", json=BATCH)
    body = json.loads(json.dumps(STEP_BODY))
    body.pop("execution_mode")
    r = _create(client, "whole-1", body)
    assert r.status_code == 201
    assert r.json()["reservation_status"] == "reserved"
    assert r.json()["execution_mode"] == "whole"
    # whole lifecycle still works
    assert client.post(
        "/inventory/plans/whole-1/confirm").status_code == 200


def test_hash_normalized_when_whole_is_explicit(client):
    # an explicit execution_mode:"whole" replays a request frozen before the
    # flag existed (and vice versa); stepwise is a distinct frozen content
    from app.models import InventoryPlanIn
    from app.planning import request_hash
    body = json.loads(json.dumps(STEP_BODY))
    body.pop("execution_mode")
    default = request_hash(InventoryPlanIn(**body))
    explicit = request_hash(
        InventoryPlanIn(**{**body, "execution_mode": "whole"}))
    stepwise = request_hash(
        InventoryPlanIn(**{**body, "execution_mode": "stepwise"}))
    assert default == explicit
    assert default != stepwise


def test_step_endpoint_on_whole_plan_rejected(client):
    client.post("/batches", json=BATCH)
    body = json.loads(json.dumps(STEP_BODY))
    body["execution_mode"] = "whole"
    _create(client, "w-2", body)
    t = _steps(client, "w-2")[0]
    r = _reg(client, "w-2", t)
    assert r.status_code == 409
    assert r.json()["error"] == "whole_execution_mode"


def test_complete_unknown_plan_is_404(client):
    assert client.post("/inventory/plans/nope/complete").status_code == 404
    assert client.post("/inventory/plans/nope/abort").status_code == 404


def test_invalid_execution_mode_is_400(client):
    client.post("/batches", json=BATCH)
    body = json.loads(json.dumps(STEP_BODY))
    body["execution_mode"] = "incremental"
    r = _create(client, "bad-mode", body)
    assert r.status_code == 400


def test_stepwise_same_id_same_body_replays_without_double_reserve(client):
    client.post("/batches", json=BATCH)
    r1 = _create(client, "sw-retry")
    r2 = _create(client, "sw-retry")
    assert r1.status_code == 201
    assert r2.status_code == 200
    assert r2.json() == r1.json()
    assert client.get("/batches/S1").json()["reserved_nl"] == 1100


def test_stepwise_same_id_different_body_conflicts(client):
    client.post("/batches", json=BATCH)
    _create(client, "sw-conf")
    changed = json.loads(json.dumps(STEP_BODY))
    changed["wells"][0]["final_volume_nl"] = 900
    r = _create(client, "sw-conf", changed)
    assert r.status_code == 409
    assert r.json()["error"] == "idempotency_conflict"


# --------------------------------------------------------------------------- #
# Step registration: ordering and content
# --------------------------------------------------------------------------- #


def test_register_steps_in_order_moves_stock_to_used(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    stock_steps = [t for t in transfers if t["source"]["kind"] == "stock"]
    assert [t["step"] for t in stock_steps] == [1, 3]

    r1 = _reg(client, "sw-1", transfers[0])  # 600 nL of S1
    assert r1.status_code == 201, r1.text
    assert r1.json()["registered_steps"] == [1]
    assert r1.json()["progress"]["next_step"] == 2
    assert r1.json()["stock_moved_nl"] == 600
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"]) == (500, 600)

    r2 = _reg(client, "sw-1", transfers[1])  # diluent step: no stock move
    assert r2.status_code == 201
    assert r2.json().get("stock_moved_nl", 0) == 0
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"]) == (500, 600)

    r3 = _reg(client, "sw-1", transfers[2])  # another 500 nL of S1
    assert r3.status_code == 201
    assert r3.json()["stock_moved_nl"] == 500
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"]) == (0, 1100)


def test_same_step_same_content_retries_replay_original(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    first = _reg(client, "sw-1", transfers[0])
    again = _reg(client, "sw-1", transfers[0])
    assert first.status_code == 201
    assert again.status_code == 200
    assert again.json()["changed"] is False
    # no second stock movement
    assert client.get("/batches/S1").json()["used_nl"] == 600
    assert again.json()["step"]["step"] == 1


def test_same_step_different_content_rejected(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    t = dict(transfers[0])
    wrong = dict(t, volume_nl=t["volume_nl"] - 100)
    r = _reg(client, "sw-1", wrong)
    assert r.status_code == 409
    assert r.json()["error"] == "step_content_conflict"
    assert r.json()["frozen"]["volume_nl"] == t["volume_nl"]
    # a wrong source/destination is equally rejected
    wrong2 = json.loads(json.dumps(t))
    wrong2["destination"]["container"] = "tube-1"
    assert _reg(client, "sw-1", wrong2).status_code == 409
    # nothing got registered
    g = client.get("/inventory/plans/sw-1").json()
    assert g["progress"]["registered_count"] == 0
    assert client.get("/batches/S1").json()["used_nl"] == 0


def test_jump_step_is_rejected(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    r = _reg(client, "sw-1", transfers[1])  # step 2 before step 1
    assert r.status_code == 409
    assert r.json()["error"] == "step_out_of_order"
    assert r.json()["expected"] == 1
    assert client.get("/inventory/plans/sw-1").json()[
        "progress"]["registered_count"] == 0


def test_unknown_and_oversized_step_number(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    r = client.post("/inventory/plans/sw-1/steps", json={
        "step": 999,
        "source": transfers[0]["source"],
        "destination": transfers[0]["destination"],
        "volume_nl": transfers[0]["volume_nl"],
    })
    assert r.status_code == 404
    assert r.json()["error"] == "step_not_found"


def test_step_endpoint_404_for_unknown_plan(client):
    r = client.post("/inventory/plans/nope/steps", json={
        "step": 1, "source": {"kind": "diluent"},
        "destination": {"kind": "well", "id": "A1"},
        "volume_nl": 100})
    assert r.status_code == 404


def test_step_body_validation(client):
    client.post("/batches", json=BATCH)
    _create(client)
    # zero/negative volume and bad node kind -> 400 (not 409)
    t = _steps(client, "sw-1")[0]
    bad = {"step": 1, "source": t["source"],
           "destination": t["destination"], "volume_nl": 0}
    assert client.post("/inventory/plans/sw-1/steps", json=bad) \
        .status_code == 400
    bad2 = {"step": 1, "source": {"kind": "ghost"},
            "destination": t["destination"], "volume_nl": 100}
    assert client.post("/inventory/plans/sw-1/steps", json=bad2) \
        .status_code == 400


# --------------------------------------------------------------------------- #
# Completion / abort
# --------------------------------------------------------------------------- #


def test_complete_only_after_all_steps(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    for t in transfers[:-1]:
        assert _reg(client, "sw-1", t).status_code == 201
    early = client.post("/inventory/plans/sw-1/complete")
    assert early.status_code == 409
    assert early.json()["error"] == "plan_incomplete"
    assert early.json()["total_steps"] == len(transfers)
    assert early.json()["registered_count"] == len(transfers) - 1

    last = _reg(client, "sw-1", transfers[-1])
    assert last.status_code == 201
    done = client.post("/inventory/plans/sw-1/complete")
    assert done.status_code == 201
    assert done.json()["status"] == "completed"
    g = client.get("/inventory/plans/sw-1").json()
    assert g["reservation_status"] == "completed"
    assert g["progress"]["complete"] is True
    # all stock is used, nothing pending
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"]) == (0, 1100)


def test_complete_is_idempotent_and_blocks_abort(client):
    client.post("/batches", json=BATCH)
    _create(client)
    _register_all(client, "sw-1", _steps(client, "sw-1"))
    assert client.post(
        "/inventory/plans/sw-1/complete").status_code == 201
    again = client.post("/inventory/plans/sw-1/complete")
    assert again.status_code == 200 and again.json()["changed"] is False
    r = client.post("/inventory/plans/sw-1/abort")
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_state_conflict"
    # no more steps after completion
    t = _steps(client, "sw-1")[0]
    assert _reg(client, "sw-1", t).status_code == 409


def test_abort_keeps_used_and_releases_pending_once(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    # register steps 1..3: stock draws of 600 + 500 = all 1100 reserved...
    # use a different plan where a later stock step remains pending instead
    # (here abort after step 1: 600 used, 500 still pending)
    _reg(client, "sw-1", transfers[0])
    r = client.post("/inventory/plans/sw-1/abort")
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "aborted"
    assert r.json()["released"] == [{"batch_id": "S1",
                                     "released_nl": 500}]
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"], b["available_nl"]) == \
        (0, 600, 1_000_000 - 600)
    # idempotent abort: no second release
    again = client.post("/inventory/plans/sw-1/abort")
    assert again.status_code == 200 and again.json()["changed"] is False
    assert again.json()["released"] == []
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"]) == (0, 600)
    # completion and further steps are rejected after abort
    assert client.post(
        "/inventory/plans/sw-1/complete").status_code == 409
    assert _reg(client, "sw-1", transfers[1]).status_code == 409


def test_abort_before_any_step_releases_everything(client):
    client.post("/batches", json=BATCH)
    _create(client)
    r = client.post("/inventory/plans/sw-1/abort")
    assert r.status_code == 201
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"], b["available_nl"]) == \
        (0, 0, 1_000_000)


def test_complete_abort_endpoints_on_whole_mode_rejected(client):
    client.post("/batches", json=BATCH)
    body = json.loads(json.dumps(STEP_BODY))
    body["execution_mode"] = "whole"
    _create(client, "w-1", body)
    r = client.post("/inventory/plans/w-1/complete")
    assert r.status_code == 409
    assert r.json()["error"] == "whole_execution_mode"
    r = client.post("/inventory/plans/w-1/abort")
    assert r.status_code == 409
    assert r.json()["error"] == "whole_execution_mode"
    # whole confirm still works
    assert client.post("/inventory/plans/w-1/confirm").status_code == 200


# --------------------------------------------------------------------------- #
# Whole-order confirm/cancel on stepwise plans are forbidden
# --------------------------------------------------------------------------- #


def test_stepwise_rejects_whole_order_confirm_and_cancel(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    _reg(client, "sw-1", transfers[0])
    for action in ("confirm", "cancel"):
        r = client.post(f"/inventory/plans/sw-1/{action}")
        assert r.status_code == 409
        assert r.json()["error"] == "stepwise_execution_mode"
        assert r.json()["status"] == "stepwise"
    # balances unchanged by the rejected calls
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"]) == (500, 600)


def test_whole_order_endpoints_still_rejected_after_completion(client):
    client.post("/batches", json=BATCH)
    _create(client)
    _register_all(client, "sw-1", _steps(client, "sw-1"))
    client.post("/inventory/plans/sw-1/complete")
    r = client.post("/inventory/plans/sw-1/confirm")
    assert r.status_code == 409
    assert r.json()["error"] == "stepwise_execution_mode"
    assert r.json()["status"] == "completed"
    r = client.post("/inventory/plans/sw-1/cancel")
    assert r.status_code == 409
    assert r.json()["error"] == "stepwise_execution_mode"


# --------------------------------------------------------------------------- #
# Query view
# --------------------------------------------------------------------------- #


def test_query_shows_progress_and_per_batch_used_pending(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    _reg(client, "sw-1", transfers[0])  # 600 used out of 1100
    g = client.get("/inventory/plans/sw-1").json()
    assert g["execution_mode"] == "stepwise"
    assert g["reservation_status"] == "stepwise"
    assert g["progress"]["total_steps"] == len(transfers)
    assert g["progress"]["registered_steps"] == [1]
    assert g["progress"]["next_step"] == 2
    bp = g["progress"]["batch_progress"]
    assert bp == [{"batch_id": "S1", "reserved_total_nl": 1100,
                   "used_nl": 600, "pending_nl": 500}]
    assert g["step_registrations"][0]["step"] == 1


# --------------------------------------------------------------------------- #
# Recall interaction
# --------------------------------------------------------------------------- #


def test_recall_blocks_further_steps_but_allows_abort(client):
    client.post("/batches", json=BATCH)
    _create(client)
    transfers = _steps(client, "sw-1")
    _reg(client, "sw-1", transfers[0])
    client.post("/batches/S1/recall", json=RECALL_BODY)

    r = _reg(client, "sw-1", transfers[1])
    assert r.status_code == 409
    assert r.json()["error"] == "batch_recalled"
    assert r.json()["recalled_batches"] == ["S1"]
    # used volume from the executed step is untouched
    assert client.get("/batches/S1").json()["used_nl"] == 600

    # abort still succeeds and releases the pending reservation once
    r = client.post("/inventory/plans/sw-1/abort")
    assert r.status_code == 201
    assert r.json()["released"] == [{"batch_id": "S1",
                                     "released_nl": 500}]
    b = client.get("/batches/S1").json()
    assert (b["reserved_nl"], b["used_nl"]) == (0, 600)
    # registering after abort is a state conflict
    assert _reg(client, "sw-1", transfers[1]).status_code == 409


def test_recall_report_follows_executed_path_only(client):
    client.post("/batches", json=BATCH)
    _create(client, "sw-path")
    transfers = _steps(client, "sw-path")
    # execute only the first stock + first diluent step (intermediate I1
    # half-made, nothing reached any well yet)
    _reg(client, "sw-path", transfers[0])
    _reg(client, "sw-path", transfers[1])
    body = client.post("/batches/S1/recall", json=RECALL_BODY).json()
    report = body["report"]
    # stock was taken (600 nL) so the plan is affected...
    assert report["affected_plan_count"] == 1
    entry = report["plans"][0]
    assert entry["request_id"] == "sw-path"
    assert entry["execution_mode"] == "stepwise"
    assert entry["reservation_status"] == "stepwise"
    assert entry["drawn_nl"] == 600
    assert entry["registered_steps"] == [1, 2]
    # ...but no well has been reached via the executed prefix
    assert entry["wells"] == []

    # a fresh plan with NO executed steps draws nothing: fully excluded
    _create(client, "sw-idle")
    body = client.post("/batches/S1/recall",
                       json=RECALL_BODY).json()  # same reason -> replay
    ids = {p["request_id"] for p in body["report"]["plans"]}
    assert "sw-idle" not in ids
    assert "sw-path" in ids


def test_recall_report_after_full_execution_traces_wells(client):
    client.post("/batches", json=BATCH)
    _create(client, "sw-done")
    for t in _steps(client, "sw-done"):
        _reg(client, "sw-done", t)
    client.post("/inventory/plans/sw-done/complete")
    report = client.post(
        "/batches/S1/recall", json=RECALL_BODY).json()["report"]
    entry = next(p for p in report["plans"]
                 if p["request_id"] == "sw-done")
    assert entry["reservation_status"] == "completed"
    assert entry["drawn_nl"] == 1100
    assert entry["wells"] == ["A1", "A2"]


def test_recall_report_partial_prefix_traces_reached_wells_only(client):
    # Execute steps 1..9: intermediate I1 is finished and A1 is filled, but
    # step 10 (I2 -> A2) never runs. The recall report reaches A1 only.
    client.post("/batches", json=BATCH)
    _create(client, "sw-partial")
    transfers = _steps(client, "sw-partial")
    assert transfers[8]["destination"]["id"] == "A1"
    assert transfers[9]["destination"]["id"] == "A2"
    for t in transfers[:9]:
        assert _reg(client, "sw-partial", t).status_code == 201
    report = client.post(
        "/batches/S1/recall", json=RECALL_BODY).json()["report"]
    entry = next(p for p in report["plans"]
                 if p["request_id"] == "sw-partial")
    assert entry["wells"] == ["A1"]
    assert entry["drawn_nl"] == 1100  # both stock-outs already happened
    assert entry["registered_steps"] == list(range(1, 10))


def test_aborted_stepwise_plan_excluded_from_recall_report(client):
    client.post("/batches", json=BATCH)
    _create(client, "sw-ab")
    _reg(client, "sw-ab", _steps(client, "sw-ab")[0])
    client.post("/inventory/plans/sw-ab/abort")
    report = client.post(
        "/batches/S1/recall", json=RECALL_BODY).json()["report"]
    assert report["affected_plan_count"] == 0


def test_new_stepwise_request_against_recalled_batch_rejected(client):
    client.post("/batches", json=BATCH)
    client.post("/batches/S1/recall", json=RECALL_BODY)
    r = _create(client, "sw-after")
    assert r.status_code == 409
    assert r.json()["error"] == "batch_recalled"
    assert client.get("/inventory/plans/sw-after").status_code == 404


# --------------------------------------------------------------------------- #
# Concurrency: recall vs step registration
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_concurrent_step_registration_and_recall(store, anyio_backend):
    await store.create_batch(None, "S1", "1000", 1_000_000)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        created = await ac.post("/inventory/plans", json=STEP_BODY,
                                headers={"X-Request-ID": "sw-race"})
        assert created.status_code == 201
        transfers = created.json()["plan"]["transfers"]
        step1, step2 = transfers[0], transfers[1]

        async def reg(t):
            return await ac.post("/inventory/plans/sw-race/steps", json={
                "step": t["step"], "source": t["source"],
                "destination": t["destination"],
                "volume_nl": t["volume_nl"]})

        results = await asyncio.gather(
            reg(step1),
            ac.post("/batches/S1/recall", json=RECALL_BODY),
        )
        step_res, recall_res = results
        b = store._pub(store.batches["S1"])
        if step_res.status_code == 201:
            # step linearized first: its stock moved to used; the recalled
            # plan can no longer run step 2, but can be aborted
            assert step_res.json()["stock_moved_nl"] == step1["volume_nl"]
            assert (b["used_nl"], b["reserved_nl"]) == (600, 500)
            blocked = await reg(step2)
            assert blocked.status_code == 409
            assert blocked.json()["error"] == "batch_recalled"
            entry = recall_res.json()["report"]["plans"][0]
            assert entry["drawn_nl"] == 600
            abort = await ac.post("/inventory/plans/sw-race/abort")
            assert abort.status_code == 201
            assert store._pub(store.batches["S1"])["reserved_nl"] == 0
        else:
            # recall linearized first: no step ran, nothing used
            assert step_res.status_code == 409
            assert step_res.json()["error"] == "batch_recalled"
            assert (b["used_nl"], b["reserved_nl"]) == (0, 1100)
            # the fully-pending plan drew nothing -> absent from report
            assert recall_res.json()["report"]["plans"] == []
            # abort remains possible and releases the whole reservation
            abort = await ac.post("/inventory/plans/sw-race/abort")
            assert abort.status_code == 201
            assert store._pub(store.batches["S1"])["available_nl"] \
                == 1_000_000


@pytest.mark.anyio
async def test_concurrent_same_step_registers_once(store, anyio_backend):
    await store.create_batch(None, "S1", "1000", 1_000_000)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as ac:
        await ac.post("/inventory/plans", json=STEP_BODY,
                      headers={"X-Request-ID": "sw-same"})
        t = (await ac.get("/inventory/plans/sw-same")).json()
        t = t["plan"]["transfers"][0]
        payload = {"step": t["step"], "source": t["source"],
                   "destination": t["destination"],
                   "volume_nl": t["volume_nl"]}
        results = await asyncio.gather(*[
            ac.post("/inventory/plans/sw-same/steps", json=payload)
            for _ in range(6)
        ])
        codes = sorted(r.status_code for r in results)
        assert codes == [200, 200, 200, 200, 200, 201]
        assert sum(r.json().get("changed") is True for r in results) == 1
        assert store._pub(store.batches["S1"])["used_nl"] == 600
