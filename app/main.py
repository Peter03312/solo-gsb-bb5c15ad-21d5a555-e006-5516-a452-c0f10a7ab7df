"""HTTP API for microplate dilution planning."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .db import (
    abort_stepwise,
    complete_stepwise,
    connect,
    create_batch,
    freeze,
    get_batch,
    get_batches,
    get_frozen,
    get_recall,
    get_reservation,
    list_batches,
    recall_batch,
    register_step,
    reserve_and_freeze,
    transition_reservation,
)
from .models import (
    BatchRegisterIn,
    InventoryPlanIn,
    PlanRequest,
    RecallIn,
    StepRegistrationIn,
)
from .planning import (
    build_inventory_problem,
    build_problem,
    container_ids,
    cross_validate_inventory,
    cross_validate_references,
    frozen_transfer,
    relabel_plan,
    request_hash,
    stepwise_progress,
    stock_usage_from_plan,
    traced_wells_for_batch,
    traced_wells_for_executed_steps,
    transfers_match,
)
from .solver import solve


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await connect()
    yield
    await app.state.db.close()


app = FastAPI(
    title="Microplate Dilution Planner",
    version="1.3.0",
    description=(
        "Computes an exact, executable pipetting plan for microplate wells. "
        "Volumes are integer nanoliters; concentrations are rationals. "
        "Inventory mode plans against registered stock batches with atomic, "
        "oversell-proof reservations and a confirm/cancel lifecycle. "
        "Inventory plans may alternatively run in stepwise execution mode: "
        "the experimenter registers each frozen transfer in order, stock is "
        "charged only as its stock-out steps are registered, and the plan is "
        "completed or aborted explicitly. Contaminated batches can be "
        "irrevocably recalled: new reservations and pending steps/confirms "
        "are rejected, and the frozen recall report traces affected plans "
        "and target wells (for stepwise plans, only the executed path) "
        "through intermediate liquids."
    ),
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    # jsonable_encoder also handles ValueError objects carried in the error
    # context of field validators (they are not JSON serializable otherwise).
    return JSONResponse(
        status_code=400,
        content={"error": "invalid_request",
                 "detail": jsonable_encoder(exc.errors())},
    )


def _frozen_reply(frozen, rhash: str, conflict_detail: str) -> JSONResponse:
    """Response for an already-frozen request id (200/422 replay or 409)."""
    old_hash, status, payload = frozen
    if old_hash != rhash:
        return JSONResponse(
            status_code=409,
            content={"error": "idempotency_conflict",
                     "detail": conflict_detail},
        )
    code = 200 if status == "feasible" else 422
    return JSONResponse(status_code=code, content=payload)


@app.get("/health")
async def health():
    row = await app.state.db.fetchval("SELECT 1")
    return {"status": "ok" if row == 1 else "degraded"}


@app.post("/plans")
async def create_plan(
    req: PlanRequest,
    request_id: str = Header(..., alias="X-Request-ID"),
):
    if not request_id or not request_id.strip():
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_request",
                     "detail": "X-Request-ID header is required"},
        )
    request_id = request_id.strip()

    try:
        cross_validate_references(req)
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_request", "detail": str(exc)},
        )

    rhash = request_hash(req)
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is not None:
        old_hash, status, payload = frozen
        if old_hash != rhash:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "idempotency_conflict",
                    "detail": (
                        "X-Request-ID already frozen with different content"),
                },
            )
        code = 200 if status == "feasible" else 422
        return JSONResponse(status_code=code, content=payload)

    prob = build_problem(req)
    result = solve(prob)

    if result.ok:
        plan = relabel_plan(result.plan, container_ids(req))
        payload = {"status": "feasible", "request_id": request_id, "plan": plan}
        await freeze(app.state.db, request_id, rhash, "feasible", payload)
        # A concurrent request with the same id may have inserted first.
        # Re-read the authoritative frozen row to honor conflict semantics.
        frozen = await get_frozen(app.state.db, request_id)
        if frozen is not None and frozen[0] != rhash:
            return JSONResponse(
                status_code=409,
                content={"error": "idempotency_conflict",
                         "detail": "X-Request-ID frozen concurrently with "
                                   "different content"},
            )
        return JSONResponse(status_code=201, content=payload)

    payload = {
        "status": "infeasible",
        "request_id": request_id,
        "blocked": result.failure.blocked,
        "reason": result.failure.reason,
        "constraints": result.failure.detail,
    }
    await freeze(app.state.db, request_id, rhash, "infeasible", payload)
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is not None and frozen[0] != rhash:
        return JSONResponse(
            status_code=409,
            content={"error": "idempotency_conflict",
                     "detail": "X-Request-ID frozen concurrently with "
                               "different content"},
        )
    return JSONResponse(status_code=422, content=payload)


@app.get("/plans/{request_id}")
async def read_plan(request_id: str):
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no frozen plan for request {request_id}"},
        )
    _, status, payload = frozen
    code = 200 if status == "feasible" else 422
    return JSONResponse(status_code=code, content=payload)


# --------------------------------------------------------------------------- #
# Batch registry
# --------------------------------------------------------------------------- #


@app.post("/batches")
async def register_batch(req: BatchRegisterIn):
    created, batch = await create_batch(
        app.state.db, req.id, req.concentration, req.initial_volume_nl)
    if created:
        return JSONResponse(status_code=201, content=batch)
    if (batch["concentration"] != req.concentration
            or batch["initial_volume_nl"] != req.initial_volume_nl):
        return JSONResponse(
            status_code=409,
            content={
                "error": "batch_conflict",
                "detail": f"batch {req.id!r} already registered with "
                          "different content",
                "batch": batch,
            },
        )
    return JSONResponse(status_code=200, content=batch)


@app.get("/batches")
async def batches_list():
    return {"batches": await list_batches(app.state.db)}


@app.get("/batches/{batch_id}")
async def batch_detail(batch_id: str):
    batch = await get_batch(app.state.db, batch_id)
    if batch is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no batch registered with id {batch_id!r}"},
        )
    return batch


# --------------------------------------------------------------------------- #
# Batch recall (irrevocable)
# --------------------------------------------------------------------------- #


def _build_recall_report(batch_id: str, candidates: list) -> dict:
    """Affected plans/wells for a batch, from live reservations + frozen plans.

    Only reservations in ``reserved`` / ``confirmed`` / ``stepwise`` /
    ``completed`` state that actually drew (or, for stepwise plans, have
    already drawn) from the batch are present (a ``reservation_items`` row
    exists iff the plan drew volume). Each target well reached from the
    batch through the transfer chain (possibly via intermediate liquids) is
    listed once per plan, annotated with that reservation's status.

    For a stepwise plan the impact is adjudicated along the **executed
    path**: the trace only crosses registered steps, and ``drawn_nl`` counts
    only stock-source steps already registered -- stock reserved for
    not-yet-taken steps has never left the batch and is excluded.
    """
    plans_out = []
    for c in candidates:
        payload = c["payload"]
        plan = payload.get("plan", {})
        if c.get("mode") == "stepwise":
            steps = c.get("steps", [])
            done = [s["step"] for s in steps]
            wells = traced_wells_for_executed_steps(plan, batch_id, done)
            drawn = sum(s["volume_nl"] for s in steps
                        if s.get("source", {}).get("kind") == "stock"
                        and s.get("source", {}).get("id") == batch_id)
            if drawn <= 0:
                # Stock reserved but never taken: nothing reached any well
                # from this batch.
                continue
            plans_out.append({
                "request_id": c["request_id"],
                "reservation_status": c["status"],
                "execution_mode": "stepwise",
                "drawn_nl": drawn,
                "registered_steps": done,
                "wells": wells,
            })
            continue
        wells = traced_wells_for_batch(plan, batch_id)
        plans_out.append({
            "request_id": c["request_id"],
            "reservation_status": c["status"],
            "drawn_nl": c["amount_nl"],
            "wells": wells,
        })
    return {
        "batch_id": batch_id,
        "affected_plan_count": len(plans_out),
        "plans": plans_out,
    }


@app.post("/batches/{batch_id}/recall")
async def recall_batch_route(batch_id: str, req: RecallIn):
    outcome, data = await recall_batch(
        app.state.db, batch_id, req.reason,
        lambda candidates: _build_recall_report(batch_id, candidates),
    )
    if outcome == "unknown":
        return JSONResponse(
            status_code=404,
            content={"error": "unknown_batch",
                     "detail": f"no batch registered with id {batch_id!r}"},
        )
    if outcome == "replay":
        return JSONResponse(status_code=200, content=data)
    if outcome == "conflict":
        return JSONResponse(
            status_code=409,
            content={"error": "recall_reason_conflict",
                     "detail": f"batch {batch_id!r} is already recalled with "
                               "a different reason; recalls are irrevocable",
                     "existing_reason": data["reason"],
                     "recalled_at": data["recalled_at"]},
        )
    return JSONResponse(status_code=201, content=data)


@app.get("/batches/{batch_id}/recall")
async def get_recall_route(batch_id: str):
    result = await get_recall(app.state.db, batch_id)
    if result == "missing":
        return JSONResponse(
            status_code=404,
            content={"error": "unknown_batch",
                     "detail": f"no batch registered with id {batch_id!r}"},
        )
    if result is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"batch {batch_id!r} has not been recalled"},
        )
    return result


# --------------------------------------------------------------------------- #
# Inventory-mode planning with atomic reservations
# --------------------------------------------------------------------------- #

# Solver failure codes that prove the registered batches are too small for
# the request (as opposed to a structural impossibility).
_STOCK_SHORTAGE_WELL_CODES = {"stock_overdraft"}
_STOCK_SHORTAGE_GLOBAL_CODES = {
    "insufficient_total_solute", "insufficient_total_volume"}


def _inventory_failure_reason(failure) -> str:
    well_codes = {c for b in failure.blocked
                  for c in b.get("constraints", [])}
    global_codes = set(failure.detail.get("global", []))
    if (well_codes & _STOCK_SHORTAGE_WELL_CODES
            or global_codes & _STOCK_SHORTAGE_GLOBAL_CODES):
        return "insufficient_inventory"
    return failure.reason


@app.post("/inventory/plans")
async def create_inventory_plan(
    req: InventoryPlanIn,
    request_id: str = Header(..., alias="X-Request-ID"),
):
    if not request_id or not request_id.strip():
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_request",
                     "detail": "X-Request-ID header is required"},
        )
    request_id = request_id.strip()

    try:
        cross_validate_inventory(req)
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_request", "detail": str(exc)},
        )

    rhash = request_hash(req)
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is not None:
        return _frozen_reply(
            frozen, rhash,
            "X-Request-ID already frozen with different content")

    batches = await get_batches(app.state.db, req.batch_ids)
    missing = [b for b in req.batch_ids if b not in batches]
    if missing:
        return JSONResponse(
            status_code=400,
            content={"error": "unknown_batch",
                     "detail": "unregistered batch id(s): "
                               + ", ".join(missing)},
        )

    recalled = [b for b in req.batch_ids if batches[b]["recalled"]]
    if recalled:
        # Explicit rejection, without freezing the request id: the same
        # X-Request-ID stays reusable later. The authoritative re-check
        # inside reserve_and_freeze() settles the race with a concurrent
        # first-time recall.
        return JSONResponse(
            status_code=409,
            content={"error": "batch_recalled",
                     "detail": "referenced batch(es) have been recalled: "
                               + ", ".join(sorted(recalled)),
                     "recalled_batches": sorted(recalled)},
        )

    prob = build_inventory_problem(req, batches)
    result = solve(prob)

    if not result.ok:
        payload = {
            "status": "infeasible",
            "request_id": request_id,
            "blocked": result.failure.blocked,
            "reason": _inventory_failure_reason(result.failure),
            "constraints": result.failure.detail,
            "inventory": [batches[b] for b in req.batch_ids],
        }
        await freeze(app.state.db, request_id, rhash, "infeasible", payload)
        # The frozen row is authoritative: a concurrent identical request
        # may have committed a feasible plan (inventory changed under us),
        # in which case its result is the correct replay for this retry.
        frozen = await get_frozen(app.state.db, request_id)
        if frozen is not None:
            return _frozen_reply(
                frozen, rhash,
                "X-Request-ID frozen concurrently with different content")
        return JSONResponse(status_code=422, content=payload)

    plan = relabel_plan(result.plan, container_ids(req))
    usage = stock_usage_from_plan(plan)
    stepwise = req.execution_mode == "stepwise"
    payload = {
        "status": "feasible",
        "request_id": request_id,
        "plan": plan,
        "reservations": [
            {"batch_id": bid, "reserved_nl": nl}
            for bid, nl in sorted(usage.items())
        ],
        "reservation_status": "stepwise" if stepwise else "reserved",
        "execution_mode": "stepwise" if stepwise else "whole",
        **({"progress": {
            "total_steps": len(plan["transfers"]),
            "registered_steps": [],
            "registered_count": 0,
            "next_step": 1,
            "complete": False,
            "batch_progress": [
                {"batch_id": bid, "reserved_total_nl": nl,
                 "used_nl": 0, "pending_nl": nl}
                for bid, nl in sorted(usage.items())
            ],
        }} if stepwise else {}),
    }
    outcome, detail = await reserve_and_freeze(
        app.state.db, request_id, rhash, payload, usage, req.execution_mode)
    if outcome == "ok":
        return JSONResponse(status_code=201, content=payload)
    if outcome == "exists":
        # A concurrent request with the same id committed first; replay its
        # frozen result (or conflict) instead of reserving a second time.
        frozen = await get_frozen(app.state.db, request_id)
        return _frozen_reply(
            frozen, rhash,
            "X-Request-ID frozen concurrently with different content")
    if outcome == "unknown":
        return JSONResponse(
            status_code=400,
            content={"error": "unknown_batch",
                     "detail": "batch disappeared while planning: "
                               + ", ".join(detail)},
        )
    if outcome == "recalled":
        # A concurrent recall committed while this request was planning; the
        # authoritative batch state wins. Nothing was frozen or reserved, so
        # the request id is not consumed.
        return JSONResponse(
            status_code=409,
            content={"error": "batch_recalled",
                     "detail": "referenced batch(es) were recalled "
                               "concurrently: " + ", ".join(detail),
                     "recalled_batches": detail},
        )
    # insufficient inventory: explain, freeze the negative result, and
    # reserve nothing.
    payload = {
        "status": "infeasible",
        "request_id": request_id,
        "blocked": [],
        "reason": "insufficient_inventory",
        "constraints": {"global": ["insufficient_inventory"]},
        "shortages": detail,
    }
    await freeze(app.state.db, request_id, rhash, "infeasible", payload)
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is not None:
        return _frozen_reply(
            frozen, rhash,
            "X-Request-ID frozen concurrently with different content")
    return JSONResponse(status_code=422, content=payload)


@app.get("/inventory/plans/{request_id}")
async def read_inventory_plan(request_id: str):
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no frozen plan for request {request_id}"},
        )
    _, status, payload = frozen
    body = dict(payload)
    reservation = await get_reservation(app.state.db, request_id)
    if reservation is not None:
        # live reservation state on top of the frozen plan payload
        body["reservation_status"] = reservation["status"]
        body["reservation_items"] = reservation["items"]
        body["execution_mode"] = reservation["mode"]
        if reservation["mode"] == "stepwise":
            body["progress"] = stepwise_progress(
                payload.get("plan", {}),
                reservation["steps"], reservation["items"])
            body["step_registrations"] = reservation["steps"]
    code = 200 if status == "feasible" else 422
    return JSONResponse(status_code=code, content=body)


async def _transition(request_id: str, action: str) -> JSONResponse:
    result = await transition_reservation(app.state.db, request_id, action)
    if result is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no reservation for request {request_id}"},
        )
    if result.get("stepwise_rejected"):
        # A stepwise plan must never flow through the whole-order
        # confirm/cancel endpoints; its lifecycle is registration +
        # complete/abort.
        return JSONResponse(
            status_code=409,
            content={
                "error": "stepwise_execution_mode",
                "detail": "this plan was created in stepwise execution mode; "
                          "register each frozen step via "
                          "POST /inventory/plans/{request_id}/steps and finish "
                          "via /complete or /abort -- whole-order "
                          f"{action} is not allowed",
                "request_id": request_id,
                "status": result["status"],
            },
        )
    target = "confirmed" if action == "confirm" else "cancelled"
    if action == "confirm" and result.get("recalled"):
        # A pending reservation that consumes a recalled batch may not be
        # confirmed; it can still be cancelled.
        return JSONResponse(
            status_code=409,
            content={
                "error": "batch_recalled",
                "detail": "cannot confirm a pending reservation that draws "
                          "from recalled batch(es): "
                          + ", ".join(result["recalled"])
                          + "; the reservation can still be cancelled",
                "request_id": request_id,
                "status": result["status"],
                "recalled_batches": result["recalled"],
            },
        )
    if not result["changed"] and result["status"] != target:
        return JSONResponse(
            status_code=409,
            content={
                "error": "reservation_state_conflict",
                "detail": f"reservation is already {result['status']}; "
                          f"it cannot be {target}",
                "request_id": request_id,
                "status": result["status"],
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "request_id": request_id,
            "status": result["status"],
            "changed": result["changed"],
            "items": result["items"],
        },
    )


@app.post("/inventory/plans/{request_id}/confirm")
async def confirm_inventory_plan(request_id: str):
    return await _transition(request_id, "confirm")


@app.post("/inventory/plans/{request_id}/cancel")
async def cancel_inventory_plan(request_id: str):
    return await _transition(request_id, "cancel")


# --------------------------------------------------------------------------- #
# Stepwise execution lifecycle
# --------------------------------------------------------------------------- #


def _stepwise_progress_body(request_id: str, frozen_payload: dict,
                            snapshot: dict) -> dict:
    """Merge a db snapshot with frozen-plan step progress for a response."""
    plan = frozen_payload.get("plan", {})
    steps = snapshot.get("steps")
    if steps is None:
        # Terminal snapshots carry only the registered step numbers.
        steps = [{"step": n, "source": {}}
                 for n in snapshot.get("registered_steps", [])]
    progress = stepwise_progress(plan, steps, snapshot["items"])
    body = {
        "request_id": request_id,
        "execution_mode": "stepwise",
        "reservation_status": snapshot.get("status", "stepwise"),
        "status": snapshot.get("status", "stepwise"),
        "step": snapshot.get("step"),
        "registered_steps": snapshot.get("registered_steps", []),
        "registered_count": snapshot.get("registered_count", 0),
        "items": snapshot.get("items", []),
        "progress": progress,
    }
    if "released" in snapshot:
        body["released"] = snapshot["released"]
    return body


@app.post("/inventory/plans/{request_id}/steps")
async def register_inventory_step(request_id: str, req: StepRegistrationIn):
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no frozen plan for request {request_id}"},
        )
    _h, fstatus, payload = frozen
    if fstatus != "feasible":
        return JSONResponse(
            status_code=409,
            content={"error": "reservation_state_conflict",
                     "detail": f"request {request_id} has no feasible frozen "
                               "plan; steps cannot be registered"},
        )
    plan = payload.get("plan", {})
    frozen_t = frozen_transfer(plan, req.step)
    if frozen_t is None:
        return JSONResponse(
            status_code=404,
            content={"error": "step_not_found",
                     "detail": f"frozen plan has no step {req.step}",
                     "total_steps": len(plan.get("transfers", []))},
        )
    source = req.source.model_dump(exclude_none=False)
    destination = req.destination.model_dump(exclude_none=False)
    if not transfers_match(frozen_t, source, destination, req.volume_nl):
        return JSONResponse(
            status_code=409,
            content={"error": "step_content_conflict",
                     "detail": "registered transfer does not match the frozen "
                               f"step {req.step}",
                     "step": req.step,
                     "submitted": {"source": source,
                                   "destination": destination,
                                   "volume_nl": req.volume_nl},
                     "frozen": frozen_t},
        )

    outcome, detail = await register_step(
        app.state.db, request_id, req.step, source, destination,
        req.volume_nl, frozen_t)

    if outcome == "unknown":
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no reservation for request {request_id}"},
        )
    if outcome == "not_stepwise":
        return JSONResponse(
            status_code=409,
            content={"error": "whole_execution_mode",
                     "detail": "this plan was created without stepwise "
                               "execution mode; use the whole-order "
                               "confirm/cancel endpoints instead",
                     "request_id": request_id,
                     "status": detail},
        )
    if outcome == "state":
        return JSONResponse(
            status_code=409,
            content={"error": "reservation_state_conflict",
                     "detail": f"stepwise plan is already {detail}; no more "
                               "steps can be registered",
                     "request_id": request_id,
                     "status": detail},
        )
    if outcome == "recalled":
        return JSONResponse(
            status_code=409,
            content={"error": "batch_recalled",
                     "detail": "a batch of this plan has been recalled; "
                               "further step execution is forbidden (the plan "
                               "can still be aborted): "
                               + ", ".join(detail),
                     "request_id": request_id,
                     "recalled_batches": detail},
        )
    if outcome == "content_conflict":
        return JSONResponse(
            status_code=409,
            content={"error": "step_content_conflict",
                     "detail": f"step {req.step} was already registered with "
                               "different content",
                     "step": req.step,
                     "registered": detail["registered"],
                     "submitted": detail["submitted"],
                     "frozen": detail["frozen"]},
        )
    if outcome == "step_conflict":
        return JSONResponse(
            status_code=409,
            content={"error": detail["error"],
                     "detail": f"step {detail['step']} is not the next "
                               f"expected step {detail['expected']}",
                     "step": detail["step"],
                     "expected": detail["expected"]},
        )
    # outcome in ("ok", "replay"): build a progress-rich body
    body = _stepwise_progress_body(request_id, payload, detail)
    body["changed"] = outcome == "ok"
    if outcome == "ok":
        body["stock_moved_nl"] = detail.get("stock_moved_nl", 0)
    return JSONResponse(status_code=201 if outcome == "ok" else 200,
                        content=body)


def _terminal_reply(request_id: str, outcome: str, detail,
                    payload) -> JSONResponse:
    if outcome == "unknown":
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no reservation for request {request_id}"},
        )
    if outcome == "not_stepwise":
        return JSONResponse(
            status_code=409,
            content={"error": "whole_execution_mode",
                     "detail": "this plan was created without stepwise "
                               "execution mode; use the whole-order "
                               "confirm/cancel endpoints instead",
                     "request_id": request_id,
                     "status": detail},
        )
    if outcome == "state":
        verb = {"aborted": "aborted", "completed": "completed"}[detail]
        return JSONResponse(
            status_code=409,
            content={"error": "reservation_state_conflict",
                     "detail": f"stepwise plan is already {verb}; completion "
                               "and abort are mutually exclusive",
                     "request_id": request_id,
                     "status": detail},
        )
    if outcome == "incomplete":
        return JSONResponse(
            status_code=409,
            content={"error": "plan_incomplete",
                     "detail": "not all frozen steps have been registered",
                     "request_id": request_id,
                     **detail},
        )
    # ok / replay
    body = _stepwise_progress_body(request_id, payload, detail)
    body["changed"] = outcome == "ok"
    if "released" in detail:
        body["released"] = detail["released"]
    return JSONResponse(status_code=200 if outcome == "replay" else 201,
                        content=body)


@app.post("/inventory/plans/{request_id}/complete")
async def complete_inventory_plan(request_id: str):
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no frozen plan for request {request_id}"},
        )
    outcome, detail = await complete_stepwise(app.state.db, request_id)
    return _terminal_reply(request_id, outcome, detail, frozen[2])


@app.post("/inventory/plans/{request_id}/abort")
async def abort_inventory_plan(request_id: str):
    frozen = await get_frozen(app.state.db, request_id)
    if frozen is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no frozen plan for request {request_id}"},
        )
    outcome, detail = await abort_stepwise(app.state.db, request_id)
    return _terminal_reply(request_id, outcome, detail, frozen[2])
