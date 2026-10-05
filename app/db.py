"""PostgreSQL persistence.

Two concerns share one database:

* frozen plans (idempotency support for both planning modes), and
* the stock-batch inventory: registered batches, per-plan reservations and
  their confirm/cancel lifecycle.

The application connects through a small asyncpg pool so that concurrent
requests serialize on row locks (``SELECT ... FOR UPDATE``) instead of on a
single shared connection.
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import Optional

import asyncpg

# Errors raised while the server is starting / accepting connections but the
# database / role is not ready yet.
_CONNECT_ERRORS = (OSError, asyncpg.PostgresError, ConnectionError)


SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    request_id      TEXT PRIMARY KEY,
    request_hash    TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('feasible', 'infeasible')),
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    concentration   TEXT NOT NULL,
    initial_nl      BIGINT NOT NULL CHECK (initial_nl > 0),
    reserved_nl     BIGINT NOT NULL DEFAULT 0 CHECK (reserved_nl >= 0),
    used_nl         BIGINT NOT NULL DEFAULT 0 CHECK (used_nl >= 0),
    recalled        BOOLEAN NOT NULL DEFAULT FALSE,
    recalled_reason TEXT,
    recalled_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (reserved_nl + used_nl <= initial_nl)
);

ALTER TABLE batches ADD COLUMN IF NOT EXISTS recalled
    BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE batches ADD COLUMN IF NOT EXISTS recalled_reason TEXT;
ALTER TABLE batches ADD COLUMN IF NOT EXISTS recalled_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS reservations (
    request_id      TEXT PRIMARY KEY,
    status          TEXT NOT NULL
                    CHECK (status IN ('reserved', 'confirmed', 'cancelled',
                                      'completed', 'aborted')),
    execution_mode  TEXT NOT NULL DEFAULT 'whole'
                    CHECK (execution_mode IN ('whole', 'stepwise')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE reservations ADD COLUMN IF NOT EXISTS execution_mode
    TEXT NOT NULL DEFAULT 'whole';
-- extend the status check when an older database restricted it to the
-- whole-mode lifecycle (reserved/confirmed/cancelled).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'reservations_status_check'
    ) THEN
        ALTER TABLE reservations DROP CONSTRAINT reservations_status_check;
    END IF;
    ALTER TABLE reservations
        ADD CONSTRAINT reservations_status_check
        CHECK (status IN ('reserved', 'confirmed', 'cancelled',
                          'completed', 'aborted'));
END$$;

CREATE TABLE IF NOT EXISTS reservation_items (
    request_id      TEXT NOT NULL REFERENCES reservations(request_id),
    batch_id        TEXT NOT NULL REFERENCES batches(batch_id),
    amount_nl       BIGINT NOT NULL CHECK (amount_nl > 0),
    PRIMARY KEY (request_id, batch_id)
);

-- Stepwise-mode registrations: one row per executed (frozen) transfer.
-- Rows are inserted strictly in step order, so COUNT(*) is the progress and
-- joining on stock-drawing steps gives the volume already moved to used.
CREATE TABLE IF NOT EXISTS step_executions (
    request_id      TEXT NOT NULL REFERENCES reservations(request_id),
    step            INTEGER NOT NULL CHECK (step > 0),
    source          JSONB NOT NULL,
    destination     JSONB NOT NULL,
    volume_nl       BIGINT NOT NULL CHECK (volume_nl > 0),
    batch_id        TEXT,
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (request_id, step)
);

CREATE TABLE IF NOT EXISTS recalls (
    batch_id        TEXT PRIMARY KEY REFERENCES batches(batch_id),
    reason          TEXT NOT NULL,
    report          JSONB NOT NULL,
    recalled_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def connect(dsn: Optional[str] = None) -> asyncpg.Pool:
    dsn = dsn or os.environ.get(
        "DATABASE_URL",
        "postgresql://planner:planner@db:5432/planner",
    )
    last_err = None
    for attempt in range(60):
        try:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
            break
        except _CONNECT_ERRORS as exc:  # database not ready yet
            last_err = exc
            await asyncio.sleep(1)
    else:
        raise RuntimeError(f"cannot reach database: {last_err}")
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
    return pool


# --------------------------------------------------------------------------- #
# Frozen plans (idempotency)
# --------------------------------------------------------------------------- #


async def get_frozen(pool, request_id: str):
    """Return (hash, status, payload dict) or None."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT request_hash, status, payload FROM plans "
            "WHERE request_id = $1",
            request_id,
        )
    if row is None:
        return None
    return row["request_hash"], row["status"], json.loads(row["payload"])


async def freeze(pool, request_id: str, request_hash: str,
                 status: str, payload: dict) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO plans (request_id, request_hash, status, payload)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (request_id) DO NOTHING
            """,
            request_id, request_hash, status, json.dumps(payload),
        )


# --------------------------------------------------------------------------- #
# Batch registry
# --------------------------------------------------------------------------- #


def _batch_dict(row) -> dict:
    available = row["initial_nl"] - row["reserved_nl"] - row["used_nl"]
    recalled_at = row["recalled_at"]
    return {
        "id": row["batch_id"],
        "concentration": row["concentration"],
        "initial_volume_nl": row["initial_nl"],
        "reserved_nl": row["reserved_nl"],
        "used_nl": row["used_nl"],
        "available_nl": available,
        "recalled": row["recalled"],
        "recalled_reason": row["recalled_reason"],
        "recalled_at": recalled_at.isoformat() if recalled_at else None,
    }


_BATCH_COLS = ("batch_id, concentration, initial_nl, reserved_nl, used_nl, "
               "recalled, recalled_reason, recalled_at")


async def create_batch(pool, batch_id: str, concentration: str,
                       initial_nl: int):
    """Insert a batch; return (created, batch dict).

    When the id already exists the existing row is returned unchanged so the
    caller can decide between an idempotent replay and a conflict.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO batches (batch_id, concentration, initial_nl)
            VALUES ($1, $2, $3)
            ON CONFLICT (batch_id) DO NOTHING
            RETURNING {_BATCH_COLS}
            """,
            batch_id, concentration, initial_nl,
        )
        if row is not None:
            return True, _batch_dict(row)
        row = await conn.fetchrow(
            f"SELECT {_BATCH_COLS} FROM batches WHERE batch_id = $1",
            batch_id,
        )
        return False, _batch_dict(row)


async def get_batch(pool, batch_id: str) -> Optional[dict]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {_BATCH_COLS} FROM batches WHERE batch_id = $1",
            batch_id,
        )
    return _batch_dict(row) if row is not None else None


async def get_batches(pool, batch_ids) -> dict:
    """Return {batch_id: batch dict} for the ids that exist."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT {_BATCH_COLS} FROM batches "
            "WHERE batch_id = ANY($1::text[])",
            list(batch_ids),
        )
    return {r["batch_id"]: _batch_dict(r) for r in rows}


async def list_batches(pool) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT {_BATCH_COLS} FROM batches ORDER BY batch_id"
        )
    return [_batch_dict(r) for r in rows]


# --------------------------------------------------------------------------- #
# Reservations
# --------------------------------------------------------------------------- #


class _Abort(Exception):
    """Rollback the open transaction and report this outcome instead."""

    def __init__(self, outcome: str, detail):
        super().__init__(outcome)
        self.outcome = outcome
        self.detail = detail


async def reserve_and_freeze(pool, request_id: str, request_hash: str,
                             payload: dict, usage: dict,
                             execution_mode: str = "whole"):
    """Atomically freeze a feasible inventory plan and reserve its stock.

    ``usage`` maps batch_id -> nanoliters drawn from that batch by the plan.
    Everything happens in one transaction:

    1. insert the frozen plan row (first writer wins);
    2. lock the touched batch rows ``FOR UPDATE`` (sorted, deadlock-free);
    3. verify every batch still has enough available volume;
    4. bump ``reserved_nl`` and record the reservation with its items.

    ``execution_mode`` is ``"whole"`` (whole-order confirm/cancel lifecycle)
    or ``"stepwise"`` (transfers registered one by one; stock is converted
    reserved -> used as the steps that draw it are registered).

    Returns ``(outcome, detail)`` where outcome is one of:

    * ``"ok"``           - plan frozen and stock reserved;
    * ``"exists"``       - request_id already frozen by a concurrent request;
                           nothing was reserved by this call;
    * ``"unknown"``      - a referenced batch no longer exists (defensive);
    * ``"recalled"``     - a referenced batch has been recalled (authoritative
                           batch state checked under the row lock);
    * ``"insufficient"`` - detail is a list of per-batch shortages; nothing
                           was reserved and nothing was frozen.
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO plans (request_id, request_hash, status,
                                       payload)
                    VALUES ($1, $2, 'feasible', $3::jsonb)
                    ON CONFLICT (request_id) DO NOTHING
                    RETURNING request_id
                    """,
                    request_id, request_hash, json.dumps(payload),
                )
                if row is None:
                    # Lost the insert race; the conflicting INSERT waits for
                    # the winner to commit, so its row is visible afterwards.
                    return "exists", None
                ids = sorted(usage)
                rows = await conn.fetch(
                    """
                    SELECT batch_id, initial_nl, reserved_nl, used_nl, recalled
                    FROM batches
                    WHERE batch_id = ANY($1::text[])
                    ORDER BY batch_id
                    FOR UPDATE
                    """,
                    ids,
                )
                found = {r["batch_id"]: r for r in rows}
                missing = [b for b in ids if b not in found]
                if missing:
                    raise _Abort("unknown", missing)
                recalled = [b for b in ids if found[b]["recalled"]]
                if recalled:
                    # Authoritative batch state wins: a recall committed (or
                    # committing) alongside this request forbids new reserves.
                    raise _Abort("recalled", recalled)
                shortages = []
                for bid in ids:
                    r = found[bid]
                    available = (r["initial_nl"] - r["reserved_nl"]
                                 - r["used_nl"])
                    if available < usage[bid]:
                        shortages.append({
                            "batch_id": bid,
                            "required_nl": usage[bid],
                            "available_nl": available,
                        })
                if shortages:
                    raise _Abort("insufficient", shortages)
                for bid in ids:
                    await conn.execute(
                        "UPDATE batches SET reserved_nl = reserved_nl + $2 "
                        "WHERE batch_id = $1",
                        bid, usage[bid],
                    )
                await conn.execute(
                    "INSERT INTO reservations (request_id, status, "
                    "execution_mode) VALUES ($1, 'reserved', $2)",
                    request_id, execution_mode,
                )
                if ids:
                    await conn.executemany(
                        "INSERT INTO reservation_items "
                        "(request_id, batch_id, amount_nl) "
                        "VALUES ($1, $2, $3)",
                        [(request_id, bid, usage[bid]) for bid in ids],
                    )
    except _Abort as abort:
        return abort.outcome, abort.detail
    return "ok", None


async def get_reservation(pool, request_id: str) -> Optional[dict]:
    """Return ``{request_id, status, execution_mode, items, steps}`` or None.

    ``steps`` lists the registered step executions in order (empty for
    whole-mode reservations and for fresh stepwise ones).
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, execution_mode FROM reservations "
            "WHERE request_id = $1",
            request_id,
        )
        if row is None:
            return None
        items = await conn.fetch(
            "SELECT batch_id, amount_nl FROM reservation_items "
            "WHERE request_id = $1 ORDER BY batch_id",
            request_id,
        )
        steps = await conn.fetch(
            "SELECT step, source, destination, volume_nl, batch_id "
            "FROM step_executions WHERE request_id = $1 ORDER BY step",
            request_id,
        )
    return {
        "request_id": request_id,
        "status": row["status"],
        "execution_mode": row["execution_mode"],
        "items": [{"batch_id": r["batch_id"], "amount_nl": r["amount_nl"]}
                  for r in items],
        "steps": [{
            "step": r["step"],
            "source": json.loads(r["source"]),
            "destination": json.loads(r["destination"]),
            "volume_nl": r["volume_nl"],
            "batch_id": r["batch_id"],
        } for r in steps],
    }


async def transition_reservation(pool, request_id: str, action: str):
    """Confirm or cancel a reservation exactly once.

    * confirm: reserved -> confirmed; each item's volume moves from
      ``reserved_nl`` to ``used_nl`` (it stays consumed forever);
    * cancel:  reserved -> cancelled; each item's ``reserved_nl`` is
      released back to the available pool exactly once.

    A pending (``reserved``) reservation that touches a recalled batch may
    still be cancelled but can no longer be confirmed; such a confirm returns
    a dict carrying ``"recalled": [batch_id, ...]`` (the caller answers 409).

    Stepwise reservations never take the whole-order transition: both actions
    return a dict carrying ``"mode_conflict": "stepwise"`` so the caller can
    answer 409 (their lifecycle is register_step / complete_stepwise /
    abort_stepwise instead).

    Lock order is always batch rows (sorted) first, then the reservation row,
    which matches :func:`recall_batch`; recalls and transitions therefore
    serialize on the authoritative batch state and cannot deadlock.  Repeated
    or concurrent calls converge on the same final state.  Returns ``None``
    when no reservation exists for ``request_id``; otherwise a dict with the
    (possibly unchanged) ``status``, whether this call performed the
    transition (``changed``), and the reservation ``items``.
    """
    assert action in ("confirm", "cancel")
    target = "confirmed" if action == "confirm" else "cancelled"
    async with pool.acquire() as conn:
        async with conn.transaction():
            pre = await conn.fetchrow(
                "SELECT status, execution_mode FROM reservations "
                "WHERE request_id = $1",
                request_id,
            )
            if pre is None:
                return None
            if pre["execution_mode"] == "stepwise":
                # Old whole-order confirm/cancel must be refused explicitly
                # for stepwise plans, regardless of their current status.
                return {"status": pre["status"], "changed": False,
                        "items": [], "mode_conflict": "stepwise"}
            items = await conn.fetch(
                "SELECT batch_id, amount_nl FROM reservation_items "
                "WHERE request_id = $1 ORDER BY batch_id",
                request_id,
            )
            item_list = [{"batch_id": r["batch_id"],
                          "amount_nl": r["amount_nl"]} for r in items]
            # Lock the touched batches first (sorted) -- the same global order
            # the recall transaction uses -- then re-lock the reservation.
            batch_ids = sorted(r["batch_id"] for r in items)
            if batch_ids:
                await conn.fetch(
                    "SELECT batch_id, recalled FROM batches "
                    "WHERE batch_id = ANY($1::text[]) "
                    "ORDER BY batch_id FOR UPDATE",
                    batch_ids,
                )
            row = await conn.fetchrow(
                "SELECT status FROM reservations WHERE request_id = $1 "
                "FOR UPDATE",
                request_id,
            )
            status = row["status"]
            result = {
                "status": status,
                "changed": False,
                "items": item_list,
            }
            if status != "reserved":
                return result  # idempotent replay: balances stay untouched
            if action == "confirm":
                recalled = await conn.fetch(
                    "SELECT batch_id FROM batches "
                    "WHERE batch_id = ANY($1::text[]) AND recalled",
                    batch_ids,
                )
                if recalled:
                    # Authoritative batch state forbids confirming a pending
                    # reservation that consumes a recalled batch.
                    result["recalled"] = [r["batch_id"] for r in recalled]
                    return result
            for r in items:
                if action == "confirm":
                    await conn.execute(
                        "UPDATE batches SET reserved_nl = reserved_nl - $2, "
                        "used_nl = used_nl + $2 WHERE batch_id = $1",
                        r["batch_id"], r["amount_nl"],
                    )
                else:
                    await conn.execute(
                        "UPDATE batches SET reserved_nl = reserved_nl - $2 "
                        "WHERE batch_id = $1",
                        r["batch_id"], r["amount_nl"],
                    )
            await conn.execute(
                "UPDATE reservations SET status = $2, updated_at = now() "
                "WHERE request_id = $1",
                request_id, target,
            )
            result["status"] = target
            result["changed"] = True
            return result


# --------------------------------------------------------------------------- #
# Stepwise-mode reservation lifecycle
# --------------------------------------------------------------------------- #


def _step_row(r) -> dict:
    return {
        "step": r["step"],
        "source": json.loads(r["source"]),
        "destination": json.loads(r["destination"]),
        "volume_nl": r["volume_nl"],
        "batch_id": r["batch_id"],
    }


async def _stepwise_state(conn, request_id: str, for_update: bool):
    """Reservation row (plus item batch ids / amounts) for a stepwise plan."""
    clause = " FOR UPDATE" if for_update else ""
    row = await conn.fetchrow(
        f"SELECT status, execution_mode FROM reservations "
        f"WHERE request_id = $1{clause}",
        request_id,
    )
    if row is None:
        return None
    items = await conn.fetch(
        "SELECT batch_id, amount_nl FROM reservation_items "
        "WHERE request_id = $1 ORDER BY batch_id",
        request_id,
    )
    steps = await conn.fetch(
        "SELECT step, source, destination, volume_nl, batch_id "
        "FROM step_executions WHERE request_id = $1 ORDER BY step",
        request_id,
    )
    return {
        "status": row["status"],
        "execution_mode": row["execution_mode"],
        "items": [dict(r) for r in items],
        "steps": [_step_row(r) for r in steps],
    }


async def register_step(pool, request_id: str, step_no: int, transfer: dict):
    """Register the execution of one frozen transfer in a stepwise plan.

    ``transfer`` is the lab-registered ``{source, destination, volume_nl}``;
    the caller has already proven it equals the frozen step and that
    ``step_no`` is a valid step number, but the authoritative same-content /
    next-step adjudication happens here under the locks, so concurrent calls
    converge.

    Lock order is the same global order every other write uses: touched batch
    rows (sorted) first, then the reservation row and the step rows. Returns
    ``(outcome, detail)``:

    * ``"missing"``       - no reservation for this request id;
    * ``"mode_conflict"``- the reservation is whole-mode;
    * ``"state_conflict"``- the reservation is already completed/aborted;
    * ``"out_of_order"`` - ``step_no`` is not the next step; detail gives
                           ``{"expected", "received"}``;
    * ``"mismatch"``     - the step exists with different content, or the
                           registered content differs from the frozen step
                           (detail);
    * ``"recalled"``     - a batch touched by the plan has been recalled
                           (detail: the recalled batch ids); no further step
                           may execute, but abort remains allowed;
    * ``"replay"``       - this exact step was already registered; detail is
                           the stored step row;
    * ``"ok"``           - registered; detail is ``{step, status, steps,
                           executed_count, used}`` where ``used`` maps
                           batch -> nL moved reserved -> used so far.
    """
    source = transfer["source"]
    destination = transfer["destination"]
    volume = transfer["volume_nl"]
    batch_id = source["id"] if source.get("kind") == "stock" else None
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Read the item batch ids up front purely to know which batch
                # rows to lock first (no locks taken by this read, so the
                # global batch-before-reservation lock order is preserved).
                ids = [r["batch_id"] for r in await conn.fetch(
                    "SELECT batch_id FROM reservation_items "
                    "WHERE request_id = $1 ORDER BY batch_id",
                    request_id,
                )]
                for bid in ids:
                    await conn.fetch(
                        "SELECT batch_id FROM batches WHERE batch_id = $1 "
                        "FOR UPDATE", bid)
                state = await _stepwise_state(conn, request_id, True)
                if state is None:
                    return "missing", None
                if state["execution_mode"] != "stepwise":
                    return "mode_conflict", None
                if state["status"] in ("completed", "aborted"):
                    return "state_conflict", {"status": state["status"]}

                existing = await conn.fetchrow(
                    "SELECT step, source, destination, volume_nl, batch_id "
                    "FROM step_executions WHERE request_id = $1 AND step = $2",
                    request_id, step_no,
                )
                if existing is not None:
                    stored = _step_row(existing)
                    same = (json.loads(existing["source"]) == source
                            and json.loads(existing["destination"])
                            == destination
                            and existing["volume_nl"] == volume)
                    if not same:
                        raise _Abort("mismatch",
                                     {"step": step_no, "existing": stored})
                    return "replay", stored
                next_step = len(state["steps"]) + 1
                if step_no != next_step:
                    raise _Abort("out_of_order",
                                 {"expected": next_step,
                                  "received": step_no})

                recalled = [r["batch_id"] for r in await conn.fetch(
                    "SELECT batch_id FROM batches "
                    "WHERE batch_id = ANY($1::text[]) AND recalled "
                    "ORDER BY batch_id", ids)]
                if recalled:
                    # Authoritative batch state wins: a recall committed (or
                    # committing) alongside this registration forbids any
                    # further execution; abort is the only remaining action.
                    raise _Abort("recalled", recalled)

                await conn.execute(
                    "INSERT INTO step_executions "
                    "(request_id, step, source, destination, volume_nl, "
                    "batch_id) VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)",
                    request_id, step_no, json.dumps(source),
                    json.dumps(destination), volume, batch_id,
                )
                if batch_id is not None:
                    # Mother-liquor draw: reservation becomes usage now.
                    # Intermediate-liquid / diluent steps debit nothing.
                    await conn.execute(
                        "UPDATE batches SET reserved_nl = reserved_nl - $2, "
                        "used_nl = used_nl + $2 WHERE batch_id = $1",
                        batch_id, volume,
                    )
                steps = [_step_row(r) for r in await conn.fetch(
                    "SELECT step, source, destination, volume_nl, batch_id "
                    "FROM step_executions WHERE request_id = $1 ORDER BY step",
                    request_id,
                )]
                status = await conn.fetchval(
                    "SELECT status FROM reservations WHERE request_id = $1",
                    request_id,
                )
                used: dict = {}
                for s in steps:
                    if s["batch_id"] is not None:
                        used[s["batch_id"]] = (
                            used.get(s["batch_id"], 0) + s["volume_nl"])
    except _Abort as abort:
        return abort.outcome, abort.detail
    return "ok", {"step": step_no, "status": status, "steps": steps,
                  "executed_count": len(steps), "used": used}


async def _finalize_stepwise(pool, request_id: str, action: str):
    """Complete (after all steps) or abort a stepwise reservation.

    The two are mutually exclusive terminal transitions from ``reserved``:

    * ``complete`` is only legal once every frozen step has been registered;
      it performs no batch accounting (stock already moved to used per step)
      and merely marks the reservation ``completed``;
    * ``abort`` keeps the used volume from registered stock steps and releases
      the not-yet-drawn reservation of every touched batch back to available
      in one shot, marking the reservation ``aborted``.

    Both follow the global lock order (batches sorted, then reservation).
    Returns ``(outcome, detail)`` with ``missing`` / ``mode_conflict`` /
    ``state_conflict`` (detail: current status) / ``not_all_steps`` (complete
    only, detail: executed/total) / ``replay`` (idempotent repeat) / ``ok``
    (detail: new status plus released amounts for abort).
    """
    assert action in ("complete", "abort")
    target = "completed" if action == "complete" else "aborted"
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                ids = [r["batch_id"] for r in await conn.fetch(
                    "SELECT batch_id FROM reservation_items "
                    "WHERE request_id = $1 ORDER BY batch_id",
                    request_id,
                )]
                for bid in ids:
                    await conn.fetch(
                        "SELECT batch_id FROM batches WHERE batch_id = $1 "
                        "FOR UPDATE", bid)
                state = await _stepwise_state(conn, request_id, True)
                if state is None:
                    return "missing", None
                if state["execution_mode"] != "stepwise":
                    return "mode_conflict", None
                if state["status"] == target:
                    return "replay", {"status": target}
                if state["status"] in ("completed", "aborted"):
                    raise _Abort("state_conflict",
                                 {"status": state["status"]})

                executed_count = await conn.fetchval(
                    "SELECT count(*) FROM step_executions "
                    "WHERE request_id = $1", request_id)
                if action == "complete":
                    total = await conn.fetchval(
                        "SELECT jsonb_array_length(payload->'plan'->'transfers')"
                        " FROM plans WHERE request_id = $1", request_id)
                    if executed_count < total:
                        raise _Abort("not_all_steps",
                                     {"executed": executed_count,
                                      "total": total})
                else:
                    used: dict = {}
                    for r in await conn.fetch(
                        "SELECT batch_id, volume_nl FROM step_executions "
                        "WHERE request_id = $1 AND batch_id IS NOT NULL",
                        request_id,
                    ):
                        used[r["batch_id"]] = (
                            used.get(r["batch_id"], 0) + r["volume_nl"])
                    released = []
                    for item in state["items"]:
                        rest = item["amount_nl"] - used.get(
                            item["batch_id"], 0)
                        if rest > 0:
                            await conn.execute(
                                "UPDATE batches SET reserved_nl = "
                                "reserved_nl - $2 WHERE batch_id = $1",
                                item["batch_id"], rest)
                            released.append(
                                {"batch_id": item["batch_id"],
                                 "released_nl": rest})
                await conn.execute(
                    "UPDATE reservations SET status = $2, updated_at = now() "
                    "WHERE request_id = $1", request_id, target)
    except _Abort as abort:
        return abort.outcome, abort.detail
    detail = {"status": target}
    if action == "abort":
        detail["released"] = released
    return "ok", detail


async def complete_stepwise(pool, request_id: str):
    return await _finalize_stepwise(pool, request_id, "complete")


async def abort_stepwise(pool, request_id: str):
    return await _finalize_stepwise(pool, request_id, "abort")


# --------------------------------------------------------------------------- #
# Batch recalls
# --------------------------------------------------------------------------- #


async def recall_batch(pool, batch_id: str, reason: str, make_report):
    """Irrevocably recall a batch and freeze its impact report atomically.

    Everything is one transaction, locking in the global order (batches
    before reservations) so the report is adjudicated against authoritative
    state even while plan creation / confirmation run concurrently:

    1. lock the batch row ``FOR UPDATE``; a missing batch ->
       ``("unknown", None)``;
    2. if it is already recalled, return ``("replay", existing)`` when the
       reason matches or ``("conflict", existing)`` otherwise -- the original
       result is never altered;
    3. lock every reservation touching this batch (sorted by request_id) so a
       concurrent confirm cannot flip a pending reservation under us, then
       read each live reservation's items together with its frozen plan;
    4. ``make_report(candidates)`` (pure, no I/O) builds the affected list;
    5. mark the batch recalled and insert the frozen ``recalls`` row.

    ``candidates`` is a list of
    ``{"request_id", "status", "execution_mode", "amount_nl", "drawn_nl",
    "executed_steps", "payload"}``. Whole-mode candidates are reservations in
    ``reserved`` / ``confirmed`` state that drew from the batch (``drawn_nl``
    is the reserved item amount). Stepwise candidates are reserved /
    completed / aborted plans whose *executed* steps actually drew from the
    batch; ``drawn_nl`` is the volume already drawn and ``executed_steps``
    bounds the executed path the impact report traces along (un-drawn mother
    liquor never counts).
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"SELECT {_BATCH_COLS} FROM batches WHERE batch_id = $1 "
                "FOR UPDATE",
                batch_id,
            )
            if row is None:
                return "unknown", None
            if row["recalled"]:
                existing = await conn.fetchrow(
                    "SELECT reason, report, recalled_at FROM recalls "
                    "WHERE batch_id = $1",
                    batch_id,
                )
                data = {
                    "batch_id": batch_id,
                    "reason": existing["reason"],
                    "recalled_at": existing["recalled_at"].isoformat(),
                    "report": json.loads(existing["report"]),
                }
                outcome = ("replay" if existing["reason"] == reason
                           else "conflict")
                return outcome, data

            # Serialize against concurrent confirm/cancel (which lock batches
            # first, then reservations).
            res_rows = await conn.fetch(
                """
                SELECT r.request_id AS request_id, r.status AS status,
                       r.execution_mode AS execution_mode,
                       i.amount_nl AS amount_nl, p.payload AS payload,
                       COALESCE((
                           SELECT sum(se.volume_nl)
                           FROM step_executions se
                           WHERE se.request_id = r.request_id
                             AND se.batch_id = $1), 0) AS drawn_nl,
                       COALESCE((
                           SELECT count(*) FROM step_executions se
                           WHERE se.request_id = r.request_id), 0)
                           AS executed_steps
                FROM reservation_items i
                JOIN reservations r ON r.request_id = i.request_id
                JOIN plans p ON p.request_id = i.request_id
                WHERE i.batch_id = $1
                  AND (
                    (r.execution_mode = 'whole'
                       AND r.status IN ('reserved', 'confirmed'))
                    OR
                    (r.execution_mode = 'stepwise'
                       AND r.status IN ('reserved', 'completed', 'aborted')
                       AND EXISTS (
                           SELECT 1 FROM step_executions se
                           WHERE se.request_id = r.request_id
                             AND se.batch_id = $1))
                  )
                ORDER BY r.request_id
                FOR UPDATE OF r
                """,
                batch_id,
            )
            candidates = [
                {
                    "request_id": r["request_id"],
                    "status": r["status"],
                    "execution_mode": r["execution_mode"],
                    "amount_nl": r["amount_nl"],
                    "drawn_nl": r["drawn_nl"],
                    "executed_steps": r["executed_steps"],
                    "payload": json.loads(r["payload"]),
                }
                for r in res_rows
            ]
            report = make_report(candidates)
            ts = await conn.fetchval(
                "UPDATE batches SET recalled = TRUE, recalled_reason = $2, "
                "recalled_at = now() WHERE batch_id = $1 RETURNING recalled_at",
                batch_id, reason,
            )
            await conn.execute(
                "INSERT INTO recalls (batch_id, reason, report, recalled_at) "
                "VALUES ($1, $2, $3::jsonb, $4)",
                batch_id, reason, json.dumps(report), ts,
            )
            data = {
                "batch_id": batch_id,
                "reason": reason,
                "recalled_at": ts.isoformat(),
                "report": report,
            }
            return "ok", data


async def get_recall(pool, batch_id: str):
    """Return the frozen recall record for a batch, or a sentinel.

    * ``"missing"``  - no such batch (404 unknown_batch);
    * ``None``       - batch exists but was never recalled (404);
    * dict           - the frozen recall (reason, recalled_at, report).
    """
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT TRUE FROM batches WHERE batch_id = $1", batch_id)
        if not exists:
            return "missing"
        row = await conn.fetchrow(
            "SELECT reason, report, recalled_at FROM recalls "
            "WHERE batch_id = $1",
            batch_id,
        )
        if row is None:
            return None
        return {
            "batch_id": batch_id,
            "reason": row["reason"],
            "recalled_at": row["recalled_at"].isoformat(),
            "report": json.loads(row["report"]),
        }
