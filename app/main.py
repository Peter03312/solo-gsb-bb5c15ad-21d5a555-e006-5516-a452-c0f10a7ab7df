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
    frozen_step,
    relabel_plan,
    request_hash,
    stock_usage_for_steps,
    stock_usage_from_plan,
    total_steps,
    traced_wells_for_batch,
    transfer_matches,
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
        "oversell-proof reservations and a confirm/cancel lifecycle, or an "
        "optional stepwise mode where frozen transfers are registered one at "
        "a time (stock draws convert reservation to used as executed) and the "
        "plan is completed after all steps or aborted (keeping used volume "
        "and releasing the rest). "
        "Contaminated batches can be irrevocably recalled: new reservations "
        "and pending confirms/step executions are rejected, and the frozen "
        "recall report traces affected plans and target wells through "
        "intermediate liquids (the executed path for stepwise plans)."
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

    Whole-mode candidates are reservations in ``reserved`` / ``confirmed``
    state that actually drew from the batch (a ``reservation_items`` row
    exists iff the plan drew volume). Each target well reached from the batch
    through the whole frozen transfer chain is listed once per plan.

    Stepwise candidates are reserved / completed / aborted plans whose
    executed steps actually drew from the batch: the trace follows only the
    *executed path* (the first ``executed_steps`` frozen transfers), so
    not-yet-drawn mother liquor and pending transfers never contribute. The
    reported ``drawn_nl`` is the volume already drawn. Each target well is
    annotated with that reservation's status.
    """
    plans_out = []
    for c in candidates:
        payload = c["payload"]
        plan = payload.get("plan", {})
        if c.get("execution_mode") == "stepwise":
            wells = traced_wells_for_batch(
                plan, batch_id, upto_step=c["executed_steps"])
            drawn_nl = c["drawn_nl"]
        else:
            wells = traced_wells_for_batch(plan, batch_id)
            drawn_nl = c["amount_nl"]
        entry = {
            "request_id": c["request_id"],
            "reservation_status": c["status"],
            "drawn_nl": drawn_nl,
            "wells": wells,
        }
        if c.get("execution_mode") == "stepwise":
            entry["execution_mode"] = "stepwise"
        plans_out.append(entry)
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
    step_total = total_steps(plan)
    payload = {
        "status": "feasible",
        "request_id": request_id,
        "plan": plan,
        "reservations": [
            {"batch_id": bid, "reserved_nl": nl}
            for bid, nl in sorted(usage.items())
        ],
        "reservation_status": "reserved",
        "execution_mode": req.execution_mode,
    }
    if req.execution_mode == "stepwise":
        # Progress snapshot frozen into the create response; GET re-computes
        # the live values as steps are registered.
        payload["progress"] = {
            "status": "reserved",
            "executed_steps": 0,
            "total_steps": step_total,
            "all_steps_registered": False,
        }
        payload["batch_balances"] = [
            {"batch_id": bid, "reserved_nl": nl, "used_nl": 0,
             "pending_nl": nl}
            for bid, nl in sorted(usage.items())
        ]
    outcome, detail = await reserve_and_freeze(
        app.state.db, request_id, rhash, payload, usage,
        req.execution_mode)
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


def _progress_view(reservation: dict, payload: dict) -> dict:
    """Live progress + per-batch used/pending view for a reservation.

    For stepwise reservations the drawn volume migrates reserved -> used as
    stock-drawing steps are registered, so ``pending_nl = reserved - used``.
    Whole-mode reservations keep the single reserved-or-used item amounts.
    """
    items = {i["batch_id"]: i["amount_nl"] for i in reservation["items"]}
    if reservation.get("execution_mode") == "stepwise":
        plan = payload.get("plan", {})
        executed_count = len(reservation["steps"])
        used = stock_usage_for_steps(plan, executed_count)
        balances = [
            {"batch_id": bid,
             "reserved_nl": items[bid] - used.get(bid, 0),
             "used_nl": used.get(bid, 0),
             "pending_nl": items[bid] - used.get(bid, 0)}
            for bid in sorted(items)
        ]
        return {
            "status": reservation["status"],
            "executed_steps": executed_count,
            "total_steps": total_steps(plan),
            "all_steps_registered": executed_count >= total_steps(plan),
            "batch_balances": balances,
            "steps": reservation["steps"],
        }
    return {
        "status": reservation["status"],
        "total_steps": total_steps(payload.get("plan", {})),
        "batch_balances": [
            {"batch_id": bid,
             "reserved_nl": items[bid]
             if reservation["status"] == "reserved" else 0,
             "used_nl": items[bid]
             if reservation["status"] == "confirmed" else 0,
             "pending_nl": items[bid]
             if reservation["status"] == "reserved" else 0}
            for bid in sorted(items)
        ],
    }


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
        body["execution_mode"] = reservation["execution_mode"]
        body["reservation_items"] = reservation["items"]
        view = _progress_view(reservation, payload)
        body["progress"] = {
            "status": view["status"],
            "executed_steps": view.get("executed_steps", 0),
            "total_steps": view["total_steps"],
            "all_steps_registered": view.get(
                "all_steps_registered",
                reservation["status"] in ("confirmed", "cancelled")),
        }
        body["batch_balances"] = view["batch_balances"]
        if reservation["execution_mode"] == "stepwise":
            body["steps"] = reservation["steps"]
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
    if result.get("mode_conflict") == "stepwise":
        verb = "confirmed" if action == "confirm" else "cancelled"
        return JSONResponse(
            status_code=409,
            content={
                "error": "execution_mode_conflict",
                "detail": f"plan {request_id} is a stepwise plan; whole-order "
                          f"{verb} is not available. Register its frozen "
                          "steps via /steps/{n}, then /complete (or /abort)",
                "request_id": request_id,
                "execution_mode": "stepwise",
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
# Stepwise-mode execution: register frozen steps one at a time, then close
# --------------------------------------------------------------------------- #


def _stepwise_error(request_id: str, outcome: str, detail) -> JSONResponse:
    if outcome == "missing":
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no reservation for request {request_id}"})
    if outcome == "mode_conflict":
        return JSONResponse(
            status_code=409,
            content={"error": "execution_mode_conflict",
                     "detail": f"plan {request_id} is not a stepwise plan"})
    if outcome == "state_conflict":
        return JSONResponse(
            status_code=409,
            content={"error": "stepwise_state_conflict",
                     "detail": f"plan {request_id} is already "
                               f"{detail['status']}",
                     "status": detail["status"]})
    if outcome == "recalled":
        return JSONResponse(
            status_code=409,
            content={"error": "batch_recalled",
                     "detail": "cannot continue executing a stepwise plan "
                               "that draws from recalled batch(es): "
                               + ", ".join(detail)
                               + "; the plan can still be aborted",
                     "recalled_batches": detail})
    if outcome == "not_all_steps":
        return JSONResponse(
            status_code=409,
            content={"error": "steps_incomplete",
                     "detail": f"{detail['executed']} of {detail['total']} "
                               "steps registered; a stepwise plan can be "
                               "completed only after every step",
                     "executed_steps": detail["executed"],
                     "total_steps": detail["total"]})
    return JSONResponse(
        status_code=409,
        content={"error": outcome, "detail": detail})


@app.post("/inventory/plans/{request_id}/steps/{step_no}")
async def register_plan_step(request_id: str, step_no: int,
                             req: StepRegistrationIn):
    if req.step != step_no:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_request",
                     "detail": f"path step {step_no} does not match body step "
                               f"{req.step}"})

    frozen = await get_frozen(app.state.db, request_id)
    if frozen is None:
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"no frozen plan for request {request_id}"})
    _, fstatus, payload = frozen
    if fstatus != "feasible":
        return JSONResponse(
            status_code=404,
            content={"error": "not_found",
                     "detail": f"request {request_id} has no executable plan"})
    plan = payload.get("plan", {})
    frozen_t = frozen_step(plan, step_no)
    if frozen_t is None:
        return JSONResponse(
            status_code=409,
            content={"error": "step_out_of_range",
                     "detail": f"step {step_no} does not exist; the frozen "
                               f"plan has {total_steps(plan)} steps",
                     "total_steps": total_steps(plan)})

    registered = {"source": req.source.model_dump(exclude_none=True),
                  "destination": req.destination.model_dump(exclude_none=True),
                  "volume_nl": req.volume_nl}
    if not transfer_matches(registered, frozen_t):
        # Different content for the same step is refused (even before any
        # row is touched); the frozen step is the only legal execution.
        return JSONResponse(
            status_code=409,
            content={"error": "step_transfer_mismatch",
                     "detail": "registered transfer differs from the frozen "
                               f"step {step_no}",
                     "step": step_no,
                     "expected": {"source": frozen_t["source"],
                                  "destination": frozen_t["destination"],
                                  "volume_nl": frozen_t["volume_nl"]},
                     "received": registered})

    outcome, detail = await register_step(
        app.state.db, request_id, step_no, registered)
    if outcome == "replay":
        # Same step, same content retry: return the original registration.
        return JSONResponse(
            status_code=200,
            content={"request_id": request_id, "replayed": True,
                     "registration": detail})
    if outcome == "ok":
        step_total = total_steps(plan)
        return JSONResponse(
            status_code=201,
            content={
                "request_id": request_id,
                "replayed": False,
                "registration": {
                    "step": detail["step"],
                    "source": registered["source"],
                    "destination": registered["destination"],
                    "volume_nl": registered["volume_nl"],
                },
                "status": detail["status"],
                "progress": {
                    "status": detail["status"],
                    "executed_steps": detail["executed_count"],
                    "total_steps": step_total,
                    "all_steps_registered":
                        detail["executed_count"] >= step_total,
                },
                "used_nl": [{"batch_id": bid, "used_nl": nl}
                            for bid, nl in sorted(detail["used"].items())],
            })
    if outcome == "out_of_order":
        return JSONResponse(
            status_code=409,
            content={"error": "step_out_of_order",
                     "detail": f"only the next step is accepted; expected "
                               f"step {detail['expected']}, got "
                               f"{detail['received']}",
                     "expected_step": detail["expected"],
                     "received_step": detail["received"]})
    if outcome == "mismatch":
        return JSONResponse(
            status_code=409,
            content={"error": "step_transfer_mismatch",
                     "detail": f"step {step_no} was already registered with "
                               "different content",
                     "existing": detail["existing"]})
    return _stepwise_error(request_id, outcome, detail)


@app.post("/inventory/plans/{request_id}/complete")
async def complete_plan(request_id: str):
    outcome, detail = await complete_stepwise(app.state.db, request_id)
    if outcome == "ok":
        return JSONResponse(
            status_code=200,
            content={"request_id": request_id, "status": "completed",
                     "changed": True})
    if outcome == "replay":
        return JSONResponse(
            status_code=200,
            content={"request_id": request_id, "status": "completed",
                     "changed": False})
    return _stepwise_error(request_id, outcome, detail)


@app.post("/inventory/plans/{request_id}/abort")
async def abort_plan(request_id: str):
    outcome, detail = await abort_stepwise(app.state.db, request_id)
    if outcome == "ok":
        return JSONResponse(
            status_code=200,
            content={"request_id": request_id, "status": "aborted",
                     "changed": True,
                     "released": detail["released"]})
    if outcome == "replay":
        return JSONResponse(
            status_code=200,
            content={"request_id": request_id, "status": "aborted",
                     "changed": False})
    return _stepwise_error(request_id, outcome, detail)
