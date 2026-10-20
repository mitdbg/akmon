"""Admission-envelope persistence helpers.

The admission layer records why a mutation batch was allowed to enter
the substrate. It is deliberately compact: source facts still live in
``okg.node_facts`` / ``okg.edge_facts``; admission rows hold lifecycle
state, idempotency, DBOS workflow references, and range pointers back
to those fact logs.
"""

from __future__ import annotations

import hashlib
import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from okg.substrate.errors import SubstrateError
from okg.substrate.producer_authority import PRODUCER_KINDS


ADMISSION_MODES = frozenset(
    {
        "direct",
        "fast_track",
        "review",
        "quarantine",
    }
)

ADMISSION_STATUSES = frozenset(
    {
        "pending",
        "accepted",
        "rejected",
        "published",
        "blocked",
        "failed",
        "quarantined",
    }
)

EVENT_TYPES = frozenset(
    {
        "created",
        "duplicate_coalesced",
        "submitted",
        "accepted",
        "rejected",
        "quarantined",
        "published",
        "blocked",
        "failed",
        "refs_attached",
    }
)

# Per `make-admission-review-load-bearing`: untrusted-origin sources
# (admission_mode `review` / `quarantine`) ingest to this shared
# Chronos branch instead of `default`. The publisher checkpoints
# `default` only, and the agent-facing MCP session pins the default
# branch, so quarantined facts are structurally invisible to agent
# reads until `okg admission decide` applies them onto `default`.
# The `quarantine:` prefix follows the okg.branches role convention.
QUARANTINE_BRANCH = "quarantine:review"

# The held admission modes. One set decides both where a run's rows land
# (`graph_branch_for_admission_mode`) and whether finalize holds the run
# `pending` for `okg admission decide` (`admission_mode_awaits_decision`),
# so the two cannot drift apart again (issue #1752: `quarantine` routed to
# the quarantine branch but finalized `accepted`, stranding its rows).
HELD_ADMISSION_MODES = frozenset({"review", "quarantine"})


def graph_branch_for_admission_mode(admission_mode: str | None) -> str:
    """Map a source's admission mode to its ingest target branch."""
    if admission_mode in HELD_ADMISSION_MODES:
        return QUARANTINE_BRANCH
    return "default"


def admission_mode_awaits_decision(admission_mode: str | None) -> bool:
    """True when a run in this mode is held `pending` for an operator decision.

    Exactly the modes whose rows land on the quarantine branch: nothing but
    `okg admission decide` applies those rows onto `default`.
    """
    return admission_mode in HELD_ADMISSION_MODES


@dataclass(frozen=True)
class AdmissionEnvelopeResult:
    admission_id: str
    status: str
    inserted: bool


@dataclass(frozen=True)
class FactRangeRef:
    ref_kind: str
    ref_key: str
    source: str
    run_id: str
    fact_id_min: int
    fact_id_max: int
    record_count: int


def stable_idempotency_key(*parts: Any) -> str:
    """Return a deterministic key for idempotent admission writes."""
    payload = json.dumps(
        parts,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def workflow_ref_from_dbos(workflow_name: str) -> dict[str, Any]:
    """Best-effort DBOS workflow reference for audit rows."""
    ref: dict[str, Any] = {"workflow_name": workflow_name}
    dbos_module = sys.modules.get("dbos")
    if dbos_module is None:
        return ref
    DBOS = getattr(dbos_module, "DBOS", None)
    if DBOS is None:
        return ref
    workflow_id = getattr(DBOS, "workflow_id", None)
    step_id = getattr(DBOS, "step_id", None)
    if workflow_id:
        ref["workflow_id"] = workflow_id
    if step_id:
        ref["step_id"] = step_id
    return ref



def source_mutation_transaction(conn, *, admission_id=None, producer_kind=None, branch_name=None):
    """Join the existing graph branch fence until the mutation transaction ends.

    Transaction locks also survive a nested helper's return when its caller owns
    the outer transaction. Lens envelopes are not source staging authority.
    """
    @contextmanager
    def guarded():
        with conn.transaction():
            observed = None
            if admission_id is not None:
                with conn.cursor() as cur:
                    cur.execute("SELECT producer_kind, COALESCE(target_branch_name, 'default') "
                                "FROM okg.admission_envelopes WHERE admission_id=%s::uuid",
                                (_uuid_text(admission_id),))
                    observed = cur.fetchone()
                if observed is None:
                    raise SubstrateError("source admission mutation target is missing")
                kind, branch = observed
            else:
                kind, branch = producer_kind, branch_name or "default"
            if kind == "source":
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                                ("okg.chronos_graph", branch))
                    if admission_id is not None:
                        cur.execute("SELECT producer_kind, COALESCE(target_branch_name, 'default') "
                                    "FROM okg.admission_envelopes WHERE admission_id=%s::uuid",
                                    (_uuid_text(admission_id),))
                        if cur.fetchone() != observed:
                            raise SubstrateError("source admission branch changed while acquiring its fence")
            yield
    return guarded()

def ensure_envelope(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    producer_id: str,
    producer_kind: str,
    intent: str,
    admission_mode: str,
    status: str,
    idempotency_key: str,
    catalog_version_id: int | None = None,
    source: str | None = None,
    run_id: str | UUID | None = None,
    authority: Mapping[str, Any] | None = None,
    check_summary: Mapping[str, Any] | None = None,
    verdict: Mapping[str, Any] | None = None,
    workflow_ref: Mapping[str, Any] | None = None,
    target_branch_name: str | None = None,
) -> AdmissionEnvelopeResult:
    """Create or coalesce an admission envelope.

    The uniqueness scope is ``(deployment_name, producer_id,
    idempotency_key)``. A duplicate write returns the existing envelope
    and appends ``duplicate_coalesced`` evidence.

    ``target_branch_name`` (AKMON Phase B.2) pins the envelope to a
    specific branch. NULL means the envelope targets the deployment's
    default branch. Non-default branches (e.g. ``quarantine:<reason>``)
    are picked up by a publisher cycle whose own ``branch_name``
    matches.
    """
    _validate_common(
        producer_kind=producer_kind,
        admission_mode=admission_mode,
        status=status,
    )
    with source_mutation_transaction(conn, producer_kind=producer_kind, branch_name=target_branch_name):
        run_uuid = _uuid_text(run_id) if run_id is not None else None
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH ins AS (
                  INSERT INTO okg.admission_envelopes
                    (deployment_name, producer_id, producer_kind, intent,
                     admission_mode, status, catalog_version_id, source,
                     run_id, idempotency_key, authority, check_summary,
                     verdict, workflow_ref, target_branch_name)
                  VALUES
                    (%(deployment_name)s, %(producer_id)s,
                     %(producer_kind)s, %(intent)s, %(admission_mode)s,
                     %(status)s, %(catalog_version_id)s, %(source)s,
                     %(run_id)s::uuid, %(idempotency_key)s,
                     %(authority)s, %(check_summary)s,
                     %(verdict)s, %(workflow_ref)s,
                     %(target_branch_name)s)
                  ON CONFLICT
                    (deployment_name, producer_id, idempotency_key)
                  DO NOTHING
                  RETURNING admission_id, status, true AS inserted
                )
                SELECT admission_id, status, inserted FROM ins
                UNION ALL
                SELECT admission_id, status, false AS inserted
                  FROM okg.admission_envelopes
                 WHERE deployment_name = %(deployment_name)s
                   AND producer_id = %(producer_id)s
                   AND idempotency_key = %(idempotency_key)s
                LIMIT 1
                """,
                {
                    "deployment_name": deployment_name,
                    "producer_id": producer_id,
                    "producer_kind": producer_kind,
                    "intent": intent,
                    "admission_mode": admission_mode,
                    "status": status,
                    "catalog_version_id": catalog_version_id,
                    "source": source,
                    "run_id": run_uuid,
                    "idempotency_key": idempotency_key,
                    "authority": Jsonb(dict(authority or {})),
                    "check_summary": Jsonb(dict(check_summary or {})),
                    "verdict": Jsonb(dict(verdict or {})),
                    "workflow_ref": Jsonb(dict(workflow_ref or {})),
                    "target_branch_name": target_branch_name,
                },
            )
            row = cur.fetchone()
        if row is None:
            raise SubstrateError(
                "admission envelope upsert returned no row",
                data={
                    "deployment_name": deployment_name,
                    "producer_id": producer_id,
                    "idempotency_key": idempotency_key,
                },
            )
        admission_id, current_status, inserted = row
        with conn.cursor() as cur:
            cur.execute(
                "SELECT producer_kind, COALESCE(target_branch_name, 'default') "
                "FROM okg.admission_envelopes WHERE admission_id=%s::uuid",
                (str(admission_id),),
            )
            if cur.fetchone() != (producer_kind, target_branch_name or "default"):
                raise SubstrateError("coalesced admission identity differs from requested branch or producer kind")
        result = AdmissionEnvelopeResult(
            admission_id=str(admission_id),
            status=str(current_status),
            inserted=bool(inserted),
        )
        if result.inserted:
            record_event(
                conn,
                admission_id=result.admission_id,
                event_type="created",
                actor=producer_id,
                payload={"status": status, "intent": intent},
                workflow_ref=workflow_ref,
            )
        else:
            record_event(
                conn,
                admission_id=result.admission_id,
                event_type="duplicate_coalesced",
                actor=producer_id,
                payload={"idempotency_key": idempotency_key},
                workflow_ref=workflow_ref,
            )
        return result


def record_event(
    conn: psycopg.Connection,
    *,
    admission_id: str | UUID,
    event_type: str,
    actor: str | None = None,
    reason: str | None = None,
    payload: Mapping[str, Any] | None = None,
    workflow_ref: Mapping[str, Any] | None = None,
) -> int:
    """Append one admission lifecycle event."""
    if event_type not in EVENT_TYPES:
        raise SubstrateError(
            f"unknown admission event_type {event_type!r}",
            data={"event_type": event_type},
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_events
              (admission_id, event_type, actor, reason, payload,
               workflow_ref)
            VALUES
              (%(admission_id)s::uuid, %(event_type)s, %(actor)s,
               %(reason)s, %(payload)s, %(workflow_ref)s)
            RETURNING event_id
            """,
            {
                "admission_id": _uuid_text(admission_id),
                "event_type": event_type,
                "actor": actor,
                "reason": reason,
                "payload": Jsonb(dict(payload or {})),
                "workflow_ref": Jsonb(dict(workflow_ref or {})),
            },
        )
        row = cur.fetchone()
    return int(row[0])


def record_held_run_submitted(
    conn: psycopg.Connection,
    *,
    admission_id: str,
    actor: str,
    verdict: Mapping[str, Any],
    workflow_ref: Mapping[str, Any],
) -> None:
    """Mark a held-mode (review or quarantine) run ready for decision.

    Stores the passing policy verdict on the (still `pending`)
    envelope and appends a `submitted` lifecycle event, so
    `okg admission list --status pending --held` is the review
    queue. No status transition happens here; only
    `okg admission decide` moves a held envelope forward (#1752).
    """
    with source_mutation_transaction(conn, admission_id=admission_id):
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.admission_envelopes
                   SET verdict = %(verdict)s::jsonb,
                       updated_at = now()
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {
                    "admission_id": admission_id,
                    "verdict": Jsonb(dict(verdict or {})),
                },
            )
        record_event(
            conn,
            admission_id=admission_id,
            event_type="submitted",
            actor=actor,
            reason="awaiting_review",
            payload={"verdict": dict(verdict or {})},
            workflow_ref=workflow_ref,
        )


def transition_envelope(
    conn: psycopg.Connection,
    *,
    admission_id: str | UUID,
    status: str,
    actor: str | None = None,
    reason: str | None = None,
    verdict: Mapping[str, Any] | None = None,
    target_generation_ref: str | None = None,
    workflow_ref: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Set envelope status and append the matching lifecycle event."""
    if status not in ADMISSION_STATUSES:
        raise SubstrateError(
            f"unknown admission status {status!r}",
            data={"status": status},
        )
    event_type = "quarantined" if status == "quarantined" else status
    if event_type not in EVENT_TYPES:
        raise SubstrateError(
            f"status {status!r} has no admission event mapping",
            data={"status": status},
        )
    if target_generation_ref is not None:
        target_generation_ref = str(target_generation_ref).strip()
        if not target_generation_ref:
            raise SubstrateError(
                "target_generation_ref must be non-empty",
                data={"admission_id": str(admission_id)},
            )
        _ensure_generation_ref_column(conn)
    with source_mutation_transaction(conn, admission_id=admission_id):
        with conn.transaction():
            _assert_fact_batch_disposition(
                conn,
                admission_id=admission_id,
                status=status,
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                UPDATE okg.admission_envelopes
                   SET status = %(status)s,
                       verdict = CASE
                         WHEN %(verdict)s::jsonb = '{}'::jsonb
                         THEN verdict
                         ELSE %(verdict)s::jsonb
                       END,
                       workflow_ref = CASE
                         WHEN %(workflow_ref)s::jsonb = '{}'::jsonb
                         THEN workflow_ref
                         ELSE %(workflow_ref)s::jsonb
                       END,
                       target_generation_ref = COALESCE(
                         %(target_generation_ref)s,
                         target_generation_ref
                       ),
                       decided_at = CASE
                         WHEN %(status)s IN (
                           'accepted', 'rejected', 'quarantined', 'failed'
                         )
                         THEN now()
                         ELSE decided_at
                       END,
                       published_at = CASE
                         WHEN %(status)s = 'published' THEN now()
                         ELSE published_at
                       END,
                       updated_at = now()
                 WHERE admission_id = %(admission_id)s::uuid
                 RETURNING admission_id::text, status, target_generation_ref
                """,
                    {
                        "admission_id": _uuid_text(admission_id),
                        "status": status,
                        "verdict": Jsonb(dict(verdict or {})),
                        "workflow_ref": Jsonb(dict(workflow_ref or {})),
                        "target_generation_ref": target_generation_ref,
                    },
                )
                row = cur.fetchone()
            if row is None:
                raise SubstrateError(
                    "admission envelope not found",
                    data={"admission_id": str(admission_id)},
                )
            record_event(
                conn,
                admission_id=str(row[0]),
                event_type=event_type,
                actor=actor,
                reason=reason,
                payload={
                    "status": status,
                    "target_generation_ref": target_generation_ref,
                    "verdict": dict(verdict or {}),
                },
                workflow_ref=workflow_ref,
            )
        return {
            "admission_id": str(row[0]),
            "status": str(row[1]),
            "target_generation_ref": row[2],
        }


def _assert_fact_batch_disposition(
    conn: psycopg.Connection,
    *,
    admission_id: str | UUID,
    status: str,
) -> None:
    """Fence terminal policy state against durable physical application state."""
    if status not in {"accepted", "rejected", "quarantined", "failed"}:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT state, target_branch
              FROM okg.fact_batch_receipts
             WHERE admission_id = %s::uuid
             FOR UPDATE
            """,
            (_uuid_text(admission_id),),
        )
        receipts = list(cur.fetchall())
    if not receipts:
        # Pre-application rejection and authorized zero-output acceptance have
        # no physical graph effects to fence.
        return
    states = {str(row[0]) for row in receipts}
    branches = {str(row[1]) for row in receipts}
    if status == "accepted" and states != {"applied"}:
        raise SubstrateError(
            "accepted admission requires every FactBatch receipt applied",
            data={
                "admission_id": str(admission_id),
                "receipt_states": sorted(states),
            },
        )
    if status in {"rejected", "failed"} and states != {"compensated"}:
        raise SubstrateError(
            f"{status} admission requires proven FactBatch compensation",
            data={
                "admission_id": str(admission_id),
                "receipt_states": sorted(states),
            },
        )
    if status == "quarantined":
        isolated = states == {"applied"} and "default" not in branches
        compensated = states == {"compensated"}
        if not isolated and not compensated:
            raise SubstrateError(
                "quarantined admission requires isolated or compensated facts",
                data={
                    "admission_id": str(admission_id),
                    "receipt_states": sorted(states),
                    "target_branches": sorted(branches),
                },
            )


def fact_range_refs_for_run(
    conn: psycopg.Connection,
    *,
    source: str,
    run_id: str | UUID,
) -> list[FactRangeRef]:
    """Reconstruct compact fact-range refs for a source run."""
    run_uuid = _uuid_text(run_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranges AS (
              SELECT
                'node_fact_range'::text AS ref_kind,
                source,
                run_id,
                min(fact_id)::bigint AS fact_id_min,
                max(fact_id)::bigint AS fact_id_max,
                count(*)::bigint AS record_count
              FROM okg.node_facts
              WHERE source = %(source)s
                AND run_id = %(run_id)s::uuid
              GROUP BY source, run_id
              UNION ALL
              SELECT
                'edge_fact_range'::text AS ref_kind,
                source,
                run_id,
                min(fact_id)::bigint AS fact_id_min,
                max(fact_id)::bigint AS fact_id_max,
                count(*)::bigint AS record_count
              FROM okg.edge_facts
              WHERE source = %(source)s
                AND run_id = %(run_id)s::uuid
              GROUP BY source, run_id
            )
            SELECT ref_kind, source, run_id::text, fact_id_min,
                   fact_id_max, record_count
              FROM ranges
             WHERE record_count > 0
             ORDER BY ref_kind
            """,
            {"source": source, "run_id": run_uuid},
        )
        rows = cur.fetchall()
    return [
        FactRangeRef(
            ref_kind=str(row[0]),
            ref_key=f"{row[0]}:{row[1]}:{row[2]}",
            source=str(row[1]),
            run_id=str(row[2]),
            fact_id_min=int(row[3]),
            fact_id_max=int(row[4]),
            record_count=int(row[5]),
        )
        for row in rows
    ]


def attach_fact_range_refs_for_run(
    conn: psycopg.Connection,
    *,
    admission_id: str | UUID,
    source: str,
    run_id: str | UUID,
    workflow_ref: Mapping[str, Any] | None = None,
) -> list[FactRangeRef]:
    """Attach reconstructed fact-range refs idempotently."""
    with source_mutation_transaction(conn, admission_id=admission_id):
        refs = fact_range_refs_for_run(conn, source=source, run_id=run_id)
        if not refs:
            chronos_ref = _chronos_graph_ref_for_run(
                conn,
                source=source,
                run_id=run_id,
            )
            if chronos_ref is not None:
                refs = [chronos_ref]
        inserted = 0
        with conn.cursor() as cur:
            for ref in refs:
                if ref.ref_kind == "payload" and ref.ref_key.startswith(
                    "chronos_graph_run:"
                ):
                    cur.execute(
                        """
                        INSERT INTO okg.admission_record_refs
                          (admission_id, ref_key, ref_kind, source, run_id,
                           record_count, payload)
                        VALUES
                          (%(admission_id)s::uuid, %(ref_key)s, 'payload',
                           %(source)s, %(run_id)s::uuid, %(record_count)s,
                           %(payload)s)
                        ON CONFLICT (admission_id, ref_key) DO NOTHING
                        RETURNING ref_id
                        """,
                        {
                            "admission_id": _uuid_text(admission_id),
                            "ref_key": ref.ref_key,
                            "source": ref.source,
                            "run_id": ref.run_id,
                            "record_count": ref.record_count,
                            "payload": Jsonb(
                                {
                                    "kind": "chronos_graph_run",
                                    "source": ref.source,
                                    "run_id": ref.run_id,
                                    "record_count": ref.record_count,
                                }
                            ),
                        },
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO okg.admission_record_refs
                          (admission_id, ref_key, ref_kind, source, run_id,
                           fact_id_min, fact_id_max, record_count, payload)
                        VALUES
                          (%(admission_id)s::uuid, %(ref_key)s, %(ref_kind)s,
                           %(source)s, %(run_id)s::uuid, %(fact_id_min)s,
                           %(fact_id_max)s, %(record_count)s, '{}'::jsonb)
                        ON CONFLICT (admission_id, ref_key) DO NOTHING
                        RETURNING ref_id
                        """,
                        {
                            "admission_id": _uuid_text(admission_id),
                            "ref_key": ref.ref_key,
                            "ref_kind": ref.ref_kind,
                            "source": ref.source,
                            "run_id": ref.run_id,
                            "fact_id_min": ref.fact_id_min,
                            "fact_id_max": ref.fact_id_max,
                            "record_count": ref.record_count,
                        },
                    )
                if cur.fetchone() is not None:
                    inserted += 1
        if inserted:
            record_event(
                conn,
                admission_id=admission_id,
                event_type="refs_attached",
                payload={
                    "source": source,
                    "run_id": _uuid_text(run_id),
                    "inserted_ref_count": inserted,
                    "refs": [ref.__dict__ for ref in refs],
                },
                workflow_ref=workflow_ref,
            )
        return refs


def _chronos_graph_ref_for_run(
    conn: psycopg.Connection,
    *,
    source: str,
    run_id: str | UUID,
) -> FactRangeRef | None:
    try:
        with conn.transaction():
            nodes = _chronos_interval_run_count(
                conn,
                table_name="okg.graph_nodes",
                source=source,
                run_id=_uuid_text(run_id),
            )
            edges = _chronos_interval_run_count(
                conn,
                table_name="okg.graph_edges",
                source=source,
                run_id=_uuid_text(run_id),
            )
    except Exception:  # noqa: BLE001 — best-effort interval-count read; any failure yields no fact-range ref
        return None
    count = nodes + edges
    if count <= 0:
        return None
    run_uuid = _uuid_text(run_id)
    return FactRangeRef(
        ref_kind="payload",
        ref_key=f"chronos_graph_run:{source}:{run_uuid}",
        source=source,
        run_id=run_uuid,
        fact_id_min=0,
        fact_id_max=0,
        record_count=count,
    )


def _chronos_interval_run_count(
    conn: psycopg.Connection,
    *,
    table_name: str,
    source: str,
    run_id: str,
    branch: str = "default",
) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
    ).checkout(branch)
    rows = graph.query(
        f"""
        SELECT count(*)::bigint AS record_count
          FROM {table_name}
         WHERE source = :source
           AND run_id = :run_id
        """,
        {"source": source, "run_id": run_id},
    )
    return int(rows[0]["record_count"] or 0) if rows else 0


def attach_fact_range_refs(
    conn: psycopg.Connection,
    *,
    admission_id: str | UUID,
    refs: list[FactRangeRef],
    workflow_ref: Mapping[str, Any] | None = None,
) -> list[FactRangeRef]:
    """Attach precomputed fact-range refs idempotently."""
    with source_mutation_transaction(conn, admission_id=admission_id):
        inserted = 0
        with conn.cursor() as cur:
            for ref in refs:
                cur.execute(
                    """
                    INSERT INTO okg.admission_record_refs
                      (admission_id, ref_key, ref_kind, source, run_id,
                       fact_id_min, fact_id_max, record_count, payload)
                    VALUES
                      (%(admission_id)s::uuid, %(ref_key)s, %(ref_kind)s,
                       %(source)s, %(run_id)s::uuid, %(fact_id_min)s,
                       %(fact_id_max)s, %(record_count)s, '{}'::jsonb)
                    ON CONFLICT (admission_id, ref_key) DO NOTHING
                    RETURNING ref_id
                    """,
                    {
                        "admission_id": _uuid_text(admission_id),
                        "ref_key": ref.ref_key,
                        "ref_kind": ref.ref_kind,
                        "source": ref.source,
                        "run_id": ref.run_id,
                        "fact_id_min": ref.fact_id_min,
                        "fact_id_max": ref.fact_id_max,
                        "record_count": ref.record_count,
                    },
                )
                if cur.fetchone() is not None:
                    inserted += 1
        if inserted:
            record_event(
                conn,
                admission_id=admission_id,
                event_type="refs_attached",
                payload={
                    "inserted_ref_count": inserted,
                    "refs": [ref.__dict__ for ref in refs],
                },
                workflow_ref=workflow_ref,
            )
        return refs


def attach_payload_ref(
    conn: psycopg.Connection,
    *,
    admission_id: str | UUID,
    payload: Mapping[str, Any],
    ref_key: str = "payload",
    workflow_ref: Mapping[str, Any] | None = None,
) -> bool:
    """Attach one review payload ref idempotently."""
    with source_mutation_transaction(conn, admission_id=admission_id):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.admission_record_refs
                  (admission_id, ref_key, ref_kind, record_count, payload)
                VALUES
                  (%(admission_id)s::uuid, %(ref_key)s, 'payload',
                   1, %(payload)s)
                ON CONFLICT (admission_id, ref_key) DO NOTHING
                RETURNING ref_id
                """,
                {
                    "admission_id": _uuid_text(admission_id),
                    "ref_key": ref_key,
                    "payload": Jsonb(dict(payload)),
                },
            )
            inserted = cur.fetchone() is not None
        if inserted:
            record_event(
                conn,
                admission_id=admission_id,
                event_type="refs_attached",
                payload={"ref_key": ref_key, "ref_kind": "payload"},
                workflow_ref=workflow_ref,
            )
        return inserted


def recovery_candidate_admission_ids(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    predecessor_generation_id: int | str,
) -> list[str]:
    """Snapshot the exact admissions one recovery freeze would bind.

    The guarded feedback path repeats this query while fenced and refuses if
    the set moved after the snapshot was stamped into checkpoint metadata.
    """
    _ensure_generation_ref_column(conn)
    return _read_recovery_candidate_admission_ids(
        conn,
        deployment_name=deployment_name,
        branch_name=branch_name,
        predecessor_generation_id=predecessor_generation_id,
    )


def _read_recovery_candidate_admission_ids(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    predecessor_generation_id: int | str,
) -> list[str]:
    """The candidate query alone, with no schema DDL (safe mid-publish)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.admission_id::text
              FROM okg.admission_envelopes e
             WHERE e.deployment_name = %(deployment_name)s
               AND COALESCE(e.target_branch_name, 'default') =
                   %(branch_name)s
               AND (
                 (
                   e.status = 'blocked'
                   AND e.target_generation_ref =
                       %(predecessor_generation_id)s::text
                 )
                 OR (
                   e.status IN ('pending', 'accepted')
                   AND (
                     e.admission_mode NOT IN ('review', 'quarantine')
                     OR e.status = 'accepted'
                   )
                   AND e.target_generation_ref IS NULL
                   AND EXISTS (
                     SELECT 1 FROM okg.admission_record_refs r
                      WHERE r.admission_id = e.admission_id
                        AND r.ref_kind IN (
                          'node_fact_range', 'edge_fact_range', 'payload'
                        )
                        AND (
                          r.ref_kind != 'payload'
                          OR r.payload->>'kind' = 'chronos_graph_run'
                        )
                   )
                 )
               )
             ORDER BY e.admission_id
            """,
            {
                "deployment_name": deployment_name,
                "branch_name": branch_name,
                "predecessor_generation_id": str(
                    predecessor_generation_id
                ),
            },
        )
        return [str(row[0]) for row in cur.fetchall()]


def apply_generation_feedback(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    generation_id: int | str,
    workflow_ref: Mapping[str, Any] | None = None,
    expected_admission_ids: Iterable[str | UUID] | None = None,
    recovery_from_generation_id: int | str | None = None,
) -> dict[str, Any]:
    """Update admission envelopes from a graph generation verdict.

    Published generations mark accepted/pending fact-range admissions on
    the same target branch published when every attached range is at or below
    that generation's fact watermark. Blocked generations mark currently-
    unpublished admissions on the same branch blocked; the deployment
    partition queue means these are the batches waiting on the failed publish
    attempt. Chronos feedback acquires the publisher and branch mutexes and
    refuses unless the checkpoint remains the unchanged branch head.

    Recovery callers pass both ``expected_admission_ids`` and
    ``recovery_from_generation_id``. The transaction then moves only that
    exact predecessor-bound set, so a frozen recovery checkpoint cannot sweep
    a concurrent or later admission.
    """
    expected_ids = (
        None
        if expected_admission_ids is None
        else sorted({_uuid_text(item) for item in expected_admission_ids})
    )
    if recovery_from_generation_id is not None and expected_ids is None:
        raise SubstrateError(
            "generation recovery requires an exact admission set",
            data={"reason": "recovery_admission_set_missing"},
        )
    tolerated_unbound: list[dict[str, str]] = []
    with _generation_feedback_scope(
        conn,
        deployment_name=deployment_name,
        generation_id=generation_id,
    ) as generation:
        status = generation["status"]
        if status == "published" and recovery_from_generation_id is not None:
            _verify_recovery_promotion_admission_set(
                conn,
                generation=generation,
                predecessor_generation_id=recovery_from_generation_id,
                expected_admission_ids=expected_ids or [],
            )
            changed = _mark_recovery_generation_published(
                conn,
                generation=generation,
                predecessor_generation_id=recovery_from_generation_id,
                expected_admission_ids=expected_ids or [],
                workflow_ref=workflow_ref,
            )
        elif status == "published":
            changed = _mark_generation_published(
                conn,
                generation=generation,
                workflow_ref=workflow_ref,
            )
        elif status == "blocked" and recovery_from_generation_id is not None:
            tolerated_unbound = _verify_recovery_freeze_admission_set(
                conn,
                generation=generation,
                predecessor_generation_id=recovery_from_generation_id,
                expected_admission_ids=expected_ids or [],
            )
            changed = _mark_recovery_generation_blocked(
                conn,
                generation=generation,
                predecessor_generation_id=recovery_from_generation_id,
                expected_admission_ids=expected_ids or [],
                workflow_ref=workflow_ref,
            )
        elif status == "blocked":
            changed = _mark_generation_blocked(
                conn,
                generation=generation,
                workflow_ref=workflow_ref,
            )
        else:
            return {
                "deployment": deployment_name,
                "generation_id": generation_id,
                "status": status,
                "updated": 0,
                "admission_ids": [],
            }

        event_type = "published" if status == "published" else "blocked"
        for row in changed:
            record_event(
                conn,
                admission_id=row["admission_id"],
                event_type=event_type,
                actor="okg_publisher_cycle",
                reason=(
                    "generation_published"
                    if status == "published"
                    else "generation_blocked"
                ),
                payload={
                    "generation_id": generation_id,
                    "generation_status": status,
                    "branch_name": generation["branch_name"],
                    "fact_watermark": generation.get("fact_watermark"),
                    "edge_fact_watermark": generation.get("edge_fact_watermark"),
                    "blocked_reason": generation.get("blocked_reason"),
                },
                workflow_ref=workflow_ref,
            )
        if expected_ids is not None:
            _verify_exact_generation_admission_set(
                conn,
                generation=generation,
                expected_admission_ids=expected_ids,
            )
        result = {
            "deployment": deployment_name,
            "generation_id": generation_id,
            "status": status,
            "branch_name": generation["branch_name"],
            "updated": len(changed),
            "admission_ids": [row["admission_id"] for row in changed],
        }
        if expected_ids is not None:
            result["verified_admission_ids"] = expected_ids
        if tolerated_unbound:
            # Non-source admissions the freeze left accepted and unbound; see
            # _verify_recovery_freeze_admission_set.
            result["tolerated_unbound_admissions"] = tolerated_unbound
        return result


@contextmanager
def _generation_feedback_scope(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    generation_id: int | str,
) -> Iterator[dict[str, Any]]:
    """Hold the safety boundary required by one feedback transaction."""
    generation = _load_generation_feedback(
        conn,
        deployment_name=deployment_name,
        generation_id=generation_id,
    )
    if generation["generation_int"] is not None:
        with conn.transaction():
            yield generation
        return

    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_lock_on_connection,
    )

    branch_name = generation["branch_name"]
    with publisher_publish_lock_on_connection(
        conn=conn,
        deployment=deployment_name,
        operation="generation admission feedback",
    ):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_lock(hashtext(%s), hashtext(%s))",
                ("okg.chronos_graph", branch_name),
            )
        try:
            with conn.transaction():
                locked_generation = _load_generation_feedback(
                    conn,
                    deployment_name=deployment_name,
                    generation_id=generation_id,
                )
                if locked_generation["branch_name"] != branch_name:
                    raise SubstrateError(
                        "generation branch changed while admission feedback was locking",
                        data={
                            "deployment": deployment_name,
                            "generation_id": str(generation_id),
                            "expected_branch": branch_name,
                            "actual_branch": locked_generation["branch_name"],
                        },
                    )
                _assert_chronos_feedback_head_unchanged(
                    conn,
                    generation=locked_generation,
                )
                yield locked_generation
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                    ("okg.chronos_graph", branch_name),
                )


# Longest lineage the feedback check walks: two segments per fork since the
# last checkpoint, so about 1,000 forks between publishes. A longer or cyclic
# parent chain is refused as a structure change instead of being walked.
_CHRONOS_FEEDBACK_LINEAGE_MAX_DEPTH = 2001


def _assert_chronos_feedback_head_unchanged(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
) -> None:
    """Refuse a delayed sweep once the checkpoint's graph head has moved."""
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL

    checkpoint_id = str(generation["checkpoint_id"])
    branch_name = str(generation["branch_name"])
    with conn.cursor() as cur:
        # Any checkpoint kind counts as "newer" except a source run's private
        # run base, which is never a generation (issue #2993).
        cur.execute(
            f"""
            SELECT checkpoint_id
              FROM _chronos_branch_interval_checkpoints
             WHERE branch_id = %s
               AND {EXCLUDE_RUN_BASE_SQL}
             ORDER BY created_at DESC, checkpoint_id DESC
             LIMIT 1
            """,
            (branch_name,),
        )
        latest_row = cur.fetchone()
        latest_checkpoint_id = str(latest_row[0]) if latest_row else None
        if latest_checkpoint_id != checkpoint_id:
            raise SubstrateError(
                "Chronos generation is no longer the branch's latest checkpoint; "
                "refusing delayed admission feedback",
                data={
                    "reason": "newer_checkpoint_exists",
                    "deployment": generation["deployment_name"],
                    "branch": branch_name,
                    "generation_id": generation["generation_id"],
                    "latest_checkpoint_id": latest_checkpoint_id,
                },
                fix_hint=(
                    "apply feedback to the current generation; never bind newer "
                    "admissions to an older checkpoint"
                ),
            )

        cur.execute(
            """
            SELECT b.current_segment_id,
                   snap.parent_segment_id, snap.owner_branch_id,
                   snap.segment_kind, snap.live_hi, snap.created_at
              FROM _chronos_branch_interval_branches b
              JOIN _chronos_branch_interval_segments snap
                ON snap.segment_id = %s
             WHERE b.branch_id = %s
            """,
            (generation["checkpoint_segment_id"], branch_name),
        )
        head_row = cur.fetchone()
        # The checkpoint split the branch head into the checkpoint and a
        # mutable continuation. Creating another branch from this one (#2966)
        # splits that continuation again: a one-unit fork base, the new
        # branch's head, and a new continuation for this branch. Those forks
        # do not change what this branch reads, so walk this branch's own
        # lineage back through fork bases to the checkpoint's continuation.
        # Anything else in the lineage (a rebase, compaction, rollover or a
        # merged staging segment) cannot be proven unchanged and is refused.
        lineage_segment_ids: list[int] = []
        lineage_stop: str | None = "branch_or_checkpoint_missing"
        structurally_unchanged = False
        if (
            head_row is not None
            and head_row[2] is None
            and str(head_row[3]) == "checkpoint"
        ):
            # One bounded query reads the head's parent chain up to the
            # segment whose parent is the checkpoint's parent. The depth bound
            # also ends a parent cycle. The shape is checked below, once.
            cur.execute(
                """
                WITH RECURSIVE lineage AS (
                    SELECT segment_id, parent_segment_id, owner_branch_id,
                           segment_kind, live_lo, created_at, 0 AS depth
                      FROM _chronos_branch_interval_segments
                     WHERE segment_id = %(head)s
                    UNION ALL
                    SELECT p.segment_id, p.parent_segment_id,
                           p.owner_branch_id, p.segment_kind, p.live_lo,
                           p.created_at, l.depth + 1
                      FROM lineage l
                      JOIN _chronos_branch_interval_segments p
                        ON p.segment_id = l.parent_segment_id
                     WHERE l.depth < %(max_depth)s
                       AND l.parent_segment_id IS DISTINCT FROM %(stop)s
                )
                SELECT segment_id, parent_segment_id, owner_branch_id,
                       segment_kind, live_lo, created_at
                  FROM lineage
                 ORDER BY depth
                """,
                {
                    "head": int(head_row[0]),
                    "stop": int(head_row[1]),
                    "max_depth": _CHRONOS_FEEDBACK_LINEAGE_MAX_DEPTH,
                },
            )
            lineage = cur.fetchall()
            lineage_segment_ids = [int(row[0]) for row in lineage]
            # Segments of this lineage that a checkpoint still sealed: one
            # with a checkpoint child named by any checkpoint row other than
            # a source run's run base (issue #2993).
            cur.execute(
                """
                SELECT DISTINCT snap.parent_segment_id
                  FROM _chronos_branch_interval_segments snap
                  JOIN _chronos_branch_interval_checkpoints c
                    ON c.segment_id = snap.segment_id
                 WHERE snap.parent_segment_id = ANY(%s)
                   AND snap.segment_kind = 'checkpoint'
                   AND c.checkpoint_id NOT LIKE 'okg-run-base:%%'
                """,
                (lineage_segment_ids,),
            )
            sealed = {int(row[0]) for row in cur.fetchall()}

            def own_mutable(row: Any) -> bool:
                return (
                    str(row[3]) == "mutable"
                    and row[2] is not None
                    and str(row[2]) == branch_name
                )

            def fork_base(row: Any) -> bool:
                return str(row[3]) == "fork_base" and row[2] is None

            # A fork puts an ownerless fork base between two of this branch's
            # own mutable segments. A source run's run-base checkpoint splits
            # one of them directly into the next; it seals nothing, so it is
            # allowed as long as no generation checkpoint names that split.
            fork_shape = bool(lineage) and own_mutable(lineage[0]) and all(
                (fork_base(parent) and own_mutable(child))
                if not own_mutable(parent)
                else (
                    fork_base(child)
                    or (own_mutable(child) and int(parent[0]) not in sealed)
                )
                for child, parent in zip(lineage, lineage[1:])
            )
            last = lineage[-1] if lineage else None
            if len(set(lineage_segment_ids)) != len(lineage_segment_ids):
                lineage_stop = "parent_cycle"
            elif not fork_shape:
                lineage_stop = "not_a_fork_lineage"
            elif len(lineage) > _CHRONOS_FEEDBACK_LINEAGE_MAX_DEPTH:
                lineage_stop = "depth_limit"
            elif (
                last is None
                or not own_mutable(last)
                or last[1] is None
                or int(last[1]) != int(head_row[1])
            ):
                lineage_stop = "not_a_fork_lineage"
            elif int(last[4]) != int(head_row[4]) or str(last[5]) != str(
                head_row[5]
            ):
                lineage_stop = "not_the_checkpoint_continuation"
            else:
                lineage_stop = None
                structurally_unchanged = True
        if not structurally_unchanged:
            raise SubstrateError(
                "Chronos branch structure changed after the generation; "
                "refusing delayed admission feedback",
                data={
                    "reason": "branch_structure_changed",
                    "deployment": generation["deployment_name"],
                    "branch": branch_name,
                    "generation_id": generation["generation_id"],
                    "current_segment_id": head_row[0] if head_row else None,
                    "checkpoint_segment_id": generation[
                        "checkpoint_segment_id"
                    ],
                    "lineage_segment_ids": lineage_segment_ids[:50],
                    "lineage_length": len(lineage_segment_ids),
                    "lineage_stop": lineage_stop,
                },
                fix_hint=(
                    "publish the current branch and apply feedback to that exact "
                    "generation"
                ),
            )

        current_segment_id = int(head_row[0])
        cur.execute(
            """
            SELECT table_name, physical_table
              FROM _chronos_branch_tables
             WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
               AND backend = 'interval'
             ORDER BY table_name
            """
        )
        graph_tables = [(str(name), str(table)) for name, table in cur]
    if [name for name, _table in graph_tables] != [
        "okg.graph_edges",
        "okg.graph_nodes",
    ]:
        raise SubstrateError(
            "cannot prove that the Chronos graph head is unchanged",
            data={
                "reason": "graph_interval_tables_missing",
                "deployment": generation["deployment_name"],
                "branch": branch_name,
                "generation_id": generation["generation_id"],
                "tables": [name for name, _table in graph_tables],
            },
        )
    for table_name, physical_table in graph_tables:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    SELECT EXISTS (
                      SELECT 1 FROM {physical}
                       WHERE writer_segment_id = ANY(%s)
                       LIMIT 1
                    )
                    """
                ).format(physical=sql.Identifier(physical_table)),
                (lineage_segment_ids,),
            )
            if bool(cur.fetchone()[0]):
                raise SubstrateError(
                    "Chronos graph head changed after the generation; "
                    "refusing delayed admission feedback",
                    data={
                        "reason": "intervening_graph_write",
                        "deployment": generation["deployment_name"],
                        "branch": branch_name,
                        "generation_id": generation["generation_id"],
                        "current_segment_id": current_segment_id,
                        "lineage_segment_ids": lineage_segment_ids,
                        "table": table_name,
                    },
                    fix_hint=(
                        "publish the intervening graph writes and apply feedback "
                        "to the new exact generation"
                    ),
                )


def _verify_exact_generation_admission_set(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
    expected_admission_ids: list[str],
) -> None:
    """Require an exact, retry-safe generation binding before commit."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_id::text, deployment_name,
                   COALESCE(target_branch_name, 'default'), status,
                   target_generation_ref
              FROM okg.admission_envelopes
             WHERE admission_id = ANY(%s::uuid[])
            """,
            (expected_admission_ids,),
        )
        expected_rows = {str(row[0]): row for row in cur.fetchall()}
        cur.execute(
            """
            SELECT admission_id::text
              FROM okg.admission_envelopes
             WHERE deployment_name = %s
               AND COALESCE(target_branch_name, 'default') = %s
               AND target_generation_ref = %s
             ORDER BY admission_id
            """,
            (
                generation["deployment_name"],
                generation["branch_name"],
                str(generation["generation_id"]),
            ),
        )
        bound_ids = [str(row[0]) for row in cur.fetchall()]
    expected_status = str(generation["status"])
    invalid_ids = [
        admission_id
        for admission_id in expected_admission_ids
        if admission_id not in expected_rows
        or str(expected_rows[admission_id][1]) != generation["deployment_name"]
        or str(expected_rows[admission_id][2]) != generation["branch_name"]
        or str(expected_rows[admission_id][3]) != expected_status
        or str(expected_rows[admission_id][4]) != str(generation["generation_id"])
    ]
    if bound_ids != expected_admission_ids or invalid_ids:
        raise SubstrateError(
            "generation feedback did not bind the exact expected admission set",
            data={
                "reason": "expected_admission_set_mismatch",
                "deployment": generation["deployment_name"],
                "branch": generation["branch_name"],
                "generation_id": generation["generation_id"],
                "expected_admission_ids": expected_admission_ids,
                "bound_admission_ids": bound_ids,
                "invalid_admission_ids": invalid_ids,
            },
            fix_hint=(
                "recheck the generation and admission IDs; this transaction "
                "was rolled back without a partial repair"
            ),
        )


def _load_generation_feedback(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    generation_id: int | str,
) -> dict[str, Any]:
    generation_text = str(generation_id)
    generation_int: int | None
    try:
        generation_int = int(generation_text)
    except (TypeError, ValueError):
        generation_int = None
    if generation_int is None:
        checkpoint_id = (
            generation_text
            if generation_text.startswith(("okg:", "generation:"))
            else f"generation:{generation_text}"
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT branch_id, segment_id, created_at, metadata
                  FROM _chronos_branch_interval_checkpoints
                 WHERE checkpoint_id = %s
                """,
                (checkpoint_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise SubstrateError(
                "generation not found for admission feedback",
                data={
                    "deployment_name": deployment_name,
                    "generation_id": generation_id,
                },
            )
        checkpoint_branch = str(row[0]).strip()
        metadata_raw = row[3] or "{}"
        metadata = (
            metadata_raw
            if isinstance(metadata_raw, dict)
            else json.loads(str(metadata_raw))
        )
        if metadata.get("deployment_name") != deployment_name:
            raise SubstrateError(
                "generation not found for admission feedback",
                data={
                    "deployment_name": deployment_name,
                    "generation_id": generation_id,
                    "checkpoint_metadata": metadata,
                },
            )
        # P5 (specify-publish-commit-protocol): the checkpoint status is an
        # explicit attestation, never inferred. A missing status means the
        # checkpoint was written by a path that did not record whether the
        # generation published or blocked -- a benign default of 'published'
        # would report an un-attested generation as a success (the exact
        # fail-open the umbrella forbids). Refuse loudly instead.
        raw_status = metadata.get("status")
        if raw_status is None or str(raw_status).strip() == "":
            raise SubstrateError(
                "checkpoint metadata is missing an explicit generation "
                "status; refusing to default to 'published'",
                data={
                    "deployment_name": deployment_name,
                    "generation_id": generation_id,
                    "checkpoint_metadata": metadata,
                },
                fix_hint=(
                    "the publisher must stamp an explicit status "
                    "('published' or 'blocked') on every checkpoint"
                ),
            )
        raw_branch = metadata.get("branch")
        if raw_branch is None or str(raw_branch).strip() == "":
            raise SubstrateError(
                "checkpoint metadata is missing an explicit branch; "
                "refusing branch-ambiguous admission feedback",
                data={
                    "deployment_name": deployment_name,
                    "generation_id": generation_id,
                    "checkpoint_metadata": metadata,
                },
                fix_hint=(
                    "the publisher must stamp the target branch on every checkpoint"
                ),
            )
        if str(raw_branch).strip() != checkpoint_branch:
            raise SubstrateError(
                "checkpoint branch identity does not match its metadata; "
                "refusing branch-ambiguous admission feedback",
                data={
                    "deployment_name": deployment_name,
                    "generation_id": generation_id,
                    "checkpoint_branch": checkpoint_branch,
                    "metadata_branch": str(raw_branch).strip(),
                },
                fix_hint=(
                    "repair the checkpoint branch attestation before "
                    "applying admission feedback"
                ),
            )
        return {
            "generation_id": generation_text,
            "generation_int": None,
            "checkpoint_id": checkpoint_id,
            "checkpoint_segment_id": int(row[1]),
            "checkpoint_created_at": str(row[2]),
            "deployment_name": deployment_name,
            "status": str(raw_status),
            "branch_name": checkpoint_branch,
            "fact_watermark": 9223372036854775807,
            "edge_fact_watermark": 9223372036854775807,
            "invariant_summary": metadata.get("invariant_summary") or {},
            "blocked_reason": metadata.get("blocked_reason") or {},
        }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT g.generation_id, g.deployment_name, g.status,
                   g.fact_watermark, g.edge_fact_watermark,
                   g.invariant_summary, g.blocked_reason, b.name
              FROM okg.graph_generations g
              JOIN okg.branches b
                ON b.branch_id = g.branch_id
               AND b.deployment_name = g.deployment_name
             WHERE g.generation_id = %s
               AND g.deployment_name = %s
            """,
            (generation_int, deployment_name),
        )
        row = cur.fetchone()
    if row is None:
        raise SubstrateError(
            "generation not found for admission feedback",
            data={
                "deployment_name": deployment_name,
                "generation_id": generation_id,
            },
        )
    return {
        "generation_id": int(row[0]),
        "generation_int": int(row[0]),
        "deployment_name": str(row[1]),
        "status": str(row[2]),
        "branch_name": str(row[7]),
        "fact_watermark": int(row[3] or 0),
        "edge_fact_watermark": int(row[4] or 0),
        "invariant_summary": row[5] or {},
        "blocked_reason": row[6] or {},
    }


#: Receipt key the publisher stamps once it has re-read the recorded
#: admission set under the publisher and branch locks, before the cycle.
LOCKED_RECOVERY_ADMISSIONS_KEY = "locked_admission_ids"


def stamp_locked_recovery_admission_set(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    blocker: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove a freeze's recorded admission set while the locks are held.

    The recovery snapshot is read before the publisher and branch locks are
    taken, so a source admission can land between the two. Re-read it on the
    locked connection before the cycle runs: any difference refuses before a
    checkpoint exists. On a match, stamp the set into the receipt; feedback
    then knows that every admission absent from it was made by this cycle,
    because sources are locked out until the cycle ends.
    """
    recovery = blocker.get("recovery")
    if not isinstance(recovery, Mapping) or recovery.get("admission_ids") is None:
        return dict(blocker)
    expected_ids = sorted({_uuid_text(item) for item in recovery["admission_ids"]})
    # No `_ensure_generation_ref_column` here: its ALTER TABLE would hold an
    # exclusive table lock on this connection for the rest of the publish.
    actual_ids = _read_recovery_candidate_admission_ids(
        conn,
        deployment_name=deployment_name,
        branch_name=branch_name,
        predecessor_generation_id=str(recovery["predecessor_checkpoint_id"]),
    )
    _require_exact_recovery_admission_ids(
        generation={
            "deployment_name": deployment_name,
            "branch_name": branch_name,
            "generation_id": None,
        },
        phase="lock",
        expected_admission_ids=expected_ids,
        actual_admission_ids=actual_ids,
    )
    stamped = dict(blocker)
    stamped["recovery"] = {
        **dict(recovery),
        LOCKED_RECOVERY_ADMISSIONS_KEY: expected_ids,
    }
    return stamped


def _locked_recovery_admission_ids(
    generation: Mapping[str, Any],
) -> list[str] | None:
    blocked_reason = generation.get("blocked_reason")
    recovery = (
        blocked_reason.get("recovery")
        if isinstance(blocked_reason, Mapping)
        else None
    )
    locked = (
        recovery.get(LOCKED_RECOVERY_ADMISSIONS_KEY)
        if isinstance(recovery, Mapping)
        else None
    )
    if not isinstance(locked, list):
        return None
    return sorted(str(item) for item in locked)


def _verify_recovery_freeze_admission_set(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
    predecessor_generation_id: int | str,
    expected_admission_ids: list[str],
) -> list[dict[str, str]]:
    """Refuse if the admission set moved between freeze probe and fence.

    When the publisher proved the recorded set under its locks (the receipt
    carries ``locked_admission_ids`` equal to the expected set), a non-source
    admission absent from that set is tolerated and left accepted and unbound
    for the next publish to mark. Most are this cycle's own enricher or
    extractor output, but not necessarily all: the branch fence holds back
    only ``source`` admissions (:func:`source_mutation_transaction`), so an
    agent or system admission from outside the cycle can also land while the
    locks are held, and on the crash-retry path
    (``incomplete_recovery.ensure_frozen_feedback``) the locks were already
    released. The tolerated ids and their ``producer_kind`` are returned so
    the caller reports them. Every source admission, and every admission of a
    checkpoint without that proof, still needs exact equality.
    """
    locked_ids = _locked_recovery_admission_ids(generation)
    tolerate_non_source = locked_ids is not None and locked_ids == sorted(
        expected_admission_ids
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.admission_id::text, e.producer_kind
              FROM okg.admission_envelopes e
             WHERE e.deployment_name = %(deployment_name)s
               AND COALESCE(e.target_branch_name, 'default') =
                   %(branch_name)s
               AND (
                 (
                   e.status = 'blocked'
                   AND e.target_generation_ref IN (
                     %(predecessor_generation_id)s::text,
                     %(generation_id)s::text
                   )
                 )
                 OR (
                   e.status IN ('pending', 'accepted')
                   AND (
                     e.admission_mode NOT IN ('review', 'quarantine')
                     OR e.status = 'accepted'
                   )
                   AND e.target_generation_ref IS NULL
                   AND EXISTS (
                     SELECT 1 FROM okg.admission_record_refs r
                      WHERE r.admission_id = e.admission_id
                        AND r.ref_kind IN (
                          'node_fact_range', 'edge_fact_range', 'payload'
                        )
                        AND (
                          r.ref_kind != 'payload'
                          OR r.payload->>'kind' = 'chronos_graph_run'
                        )
                   )
                 )
               )
             ORDER BY e.admission_id
            """,
            {
                "deployment_name": generation["deployment_name"],
                "branch_name": generation["branch_name"],
                "predecessor_generation_id": str(
                    predecessor_generation_id
                ),
                "generation_id": str(generation["generation_id"]),
            },
        )
        rows = [(str(row[0]), str(row[1])) for row in cur.fetchall()]
    expected_set = set(expected_admission_ids)
    tolerated = [
        {"admission_id": admission_id, "producer_kind": producer_kind}
        for admission_id, producer_kind in rows
        if tolerate_non_source
        and admission_id not in expected_set
        and producer_kind != "source"
    ]
    tolerated_ids = {item["admission_id"] for item in tolerated}
    actual_ids = [
        admission_id
        for admission_id, _producer_kind in rows
        if admission_id not in tolerated_ids
    ]
    _require_exact_recovery_admission_ids(
        generation=generation,
        phase="freeze",
        expected_admission_ids=expected_admission_ids,
        actual_admission_ids=actual_ids,
    )
    return tolerated


def _verify_recovery_promotion_admission_set(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
    predecessor_generation_id: int | str,
    expected_admission_ids: list[str],
) -> None:
    """Refuse unless the frozen checkpoint still owns the exact set."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_id::text
              FROM okg.admission_envelopes
             WHERE deployment_name = %s
               AND COALESCE(target_branch_name, 'default') = %s
               AND status = 'blocked'
               AND target_generation_ref = %s
             ORDER BY admission_id
            """,
            (
                generation["deployment_name"],
                generation["branch_name"],
                str(predecessor_generation_id),
            ),
        )
        actual_ids = [str(row[0]) for row in cur.fetchall()]
    _require_exact_recovery_admission_ids(
        generation=generation,
        phase="promotion",
        expected_admission_ids=expected_admission_ids,
        actual_admission_ids=actual_ids,
    )


def _require_exact_recovery_admission_ids(
    *,
    generation: Mapping[str, Any],
    phase: str,
    expected_admission_ids: list[str],
    actual_admission_ids: list[str],
) -> None:
    expected_ids = sorted(expected_admission_ids)
    if actual_admission_ids == expected_ids:
        return
    data: dict[str, Any] = {
        "reason": "recovery_admission_set_changed",
        "phase": phase,
        "deployment": generation["deployment_name"],
        "branch": generation["branch_name"],
        "generation_id": generation["generation_id"],
        "expected_admission_ids": expected_ids,
        "actual_admission_ids": actual_admission_ids,
    }
    fix_hint = (
        "retry recovery from the newest exact checkpoint; no admission "
        "transition from this transaction was committed"
    )
    if phase == "lock":
        data["checkpoint_committed"] = False
        fix_hint = (
            "an admission landed after the freeze recorded its set and before "
            "it took the publisher and branch locks; nothing was checkpointed "
            "and no admission changed, so rerun the same command"
        )
    elif phase == "freeze":
        from okg.substrate.publish_recovery import _supersede_command

        data["checkpoint_committed"] = True
        fix_hint = (
            f"the blocked checkpoint {generation['generation_id']} and its "
            "repair receipt WERE committed; only its admission feedback was "
            "refused, so no admission changed. The next run must repeat the "
            "sources the receipt records. To retire that requirement "
            "instead, first run the dry run: "
            f"{_supersede_command(str(generation['deployment_name']), apply=False)}"
        )
    raise SubstrateError(
        "incomplete recovery admission set changed",
        data=data,
        fix_hint=fix_hint,
    )


def _mark_recovery_generation_blocked(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
    predecessor_generation_id: int | str,
    expected_admission_ids: list[str],
    workflow_ref: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    _ensure_generation_ref_column(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.admission_envelopes e
               SET status = 'blocked',
                   target_generation_ref = %(generation_id)s::text,
                   verdict = e.verdict || jsonb_build_object(
                     'generation_id', %(generation_id)s::text,
                     'generation_status', 'blocked',
                     'blocked_reason', %(blocked_reason)s::jsonb,
                     'invariant_summary', %(invariant_summary)s::jsonb
                   ),
                   workflow_ref = CASE
                     WHEN %(workflow_ref)s::jsonb = '{}'::jsonb
                     THEN e.workflow_ref
                     ELSE %(workflow_ref)s::jsonb
                   END,
                   updated_at = now()
             WHERE e.admission_id = ANY(%(expected_admission_ids)s::uuid[])
               AND e.deployment_name = %(deployment_name)s
               AND COALESCE(e.target_branch_name, 'default') =
                   %(branch_name)s
               AND (
                 (
                   e.status = 'blocked'
                   AND e.target_generation_ref =
                       %(predecessor_generation_id)s::text
                 )
                 OR (
                   e.status IN ('pending', 'accepted')
                   AND e.target_generation_ref IS NULL
                 )
               )
            RETURNING e.admission_id::text
            """,
            {
                "expected_admission_ids": expected_admission_ids,
                "deployment_name": generation["deployment_name"],
                "branch_name": generation["branch_name"],
                "predecessor_generation_id": str(
                    predecessor_generation_id
                ),
                "generation_id": generation["generation_id"],
                "blocked_reason": Jsonb(
                    dict(generation.get("blocked_reason") or {})
                ),
                "invariant_summary": Jsonb(
                    dict(generation.get("invariant_summary") or {})
                ),
                "workflow_ref": Jsonb(dict(workflow_ref or {})),
            },
        )
        rows = cur.fetchall()
    return [{"admission_id": str(row[0])} for row in rows]


def _mark_recovery_generation_published(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
    predecessor_generation_id: int | str,
    expected_admission_ids: list[str],
    workflow_ref: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    _ensure_generation_ref_column(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.admission_envelopes e
               SET status = 'published',
                   target_generation_ref = %(generation_id)s::text,
                   verdict = e.verdict || jsonb_build_object(
                     'generation_id', %(generation_id)s::text,
                     'generation_status', 'published',
                     'fact_watermark', %(fact_watermark)s,
                     'edge_fact_watermark', %(edge_fact_watermark)s,
                     'invariant_summary', %(invariant_summary)s::jsonb
                   ),
                   workflow_ref = CASE
                     WHEN %(workflow_ref)s::jsonb = '{}'::jsonb
                     THEN e.workflow_ref
                     ELSE %(workflow_ref)s::jsonb
                   END,
                   published_at = now(),
                   updated_at = now()
             WHERE e.admission_id = ANY(%(expected_admission_ids)s::uuid[])
               AND e.deployment_name = %(deployment_name)s
               AND COALESCE(e.target_branch_name, 'default') =
                   %(branch_name)s
               AND e.status = 'blocked'
               AND e.target_generation_ref =
                   %(predecessor_generation_id)s::text
            RETURNING e.admission_id::text
            """,
            {
                "expected_admission_ids": expected_admission_ids,
                "deployment_name": generation["deployment_name"],
                "branch_name": generation["branch_name"],
                "predecessor_generation_id": str(
                    predecessor_generation_id
                ),
                "generation_id": generation["generation_id"],
                "fact_watermark": generation["fact_watermark"],
                "edge_fact_watermark": generation["edge_fact_watermark"],
                "invariant_summary": Jsonb(
                    dict(generation.get("invariant_summary") or {})
                ),
                "workflow_ref": Jsonb(dict(workflow_ref or {})),
            },
        )
        rows = cur.fetchall()
    return [{"admission_id": str(row[0])} for row in rows]


def _mark_generation_published(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
    workflow_ref: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    _ensure_generation_ref_column(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH candidate AS (
              SELECT e.admission_id
                FROM okg.admission_envelopes e
               WHERE e.deployment_name = %(deployment_name)s
                 AND COALESCE(e.target_branch_name, 'default') =
                     %(branch_name)s
                 AND e.status IN ('pending', 'accepted')
                 -- Review/quarantine envelopes require an explicit accepted
                 -- decision before publish eligibility (add-admission-envelope
                 -- design): refs attach for ANY mode with output, so without
                 -- this mode guard the sweep would auto-publish a pending
                 -- review envelope and strand `okg admission decide` (which
                 -- requires pending). Publish decisiveness for held reviews
                 -- is doctor-surfacing, not auto-publish.
                 AND (
                   e.admission_mode NOT IN ('review', 'quarantine')
                   OR e.status = 'accepted'
                 )
                 AND EXISTS (
                   SELECT 1 FROM okg.admission_record_refs r
                    WHERE r.admission_id = e.admission_id
                      AND r.ref_kind IN (
                        'node_fact_range', 'edge_fact_range', 'payload'
                      )
                      AND (
                        r.ref_kind != 'payload'
                        OR r.payload->>'kind' = 'chronos_graph_run'
                      )
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM okg.admission_record_refs r
                    WHERE r.admission_id = e.admission_id
                      AND (
                        (
                          r.ref_kind = 'node_fact_range'
                          AND r.fact_id_max > %(fact_watermark)s
                        )
                        OR (
                          r.ref_kind = 'edge_fact_range'
                          AND r.fact_id_max > %(edge_fact_watermark)s
                        )
                      )
                 )
            )
            UPDATE okg.admission_envelopes e
               SET status = 'published',
                   target_generation_ref = %(generation_id)s::text,
                   verdict = e.verdict || jsonb_build_object(
                     'generation_id', %(generation_id)s::text,
                     'generation_status', 'published',
                     'fact_watermark', %(fact_watermark)s,
                     'edge_fact_watermark', %(edge_fact_watermark)s,
                     'invariant_summary', %(invariant_summary)s::jsonb
                   ),
                   workflow_ref = CASE
                     WHEN %(workflow_ref)s::jsonb = '{}'::jsonb
                     THEN e.workflow_ref
                     ELSE %(workflow_ref)s::jsonb
                   END,
                   published_at = now(),
                   updated_at = now()
              FROM candidate c
             WHERE e.admission_id = c.admission_id
            RETURNING e.admission_id::text
            """,
            {
                "deployment_name": generation["deployment_name"],
                "branch_name": generation["branch_name"],
                "generation_id": generation["generation_id"],
                "fact_watermark": generation["fact_watermark"],
                "edge_fact_watermark": generation["edge_fact_watermark"],
                "invariant_summary": Jsonb(
                    dict(generation.get("invariant_summary") or {})
                ),
                "workflow_ref": Jsonb(dict(workflow_ref or {})),
            },
        )
        rows = cur.fetchall()
    return [{"admission_id": str(row[0])} for row in rows]


def _mark_generation_blocked(
    conn: psycopg.Connection,
    *,
    generation: Mapping[str, Any],
    workflow_ref: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    _ensure_generation_ref_column(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.admission_envelopes e
               SET status = 'blocked',
                   target_generation_ref = %(generation_id)s::text,
                   verdict = e.verdict || jsonb_build_object(
                     'generation_id', %(generation_id)s::text,
                     'generation_status', 'blocked',
                     'blocked_reason', %(blocked_reason)s::jsonb,
                     'invariant_summary', %(invariant_summary)s::jsonb
                   ),
                   workflow_ref = CASE
                     WHEN %(workflow_ref)s::jsonb = '{}'::jsonb
                     THEN e.workflow_ref
                     ELSE %(workflow_ref)s::jsonb
                   END,
                   updated_at = now()
             WHERE e.deployment_name = %(deployment_name)s
               AND COALESCE(e.target_branch_name, 'default') =
                   %(branch_name)s
               AND e.status IN ('pending', 'accepted')
               -- Same mode guard as the publish sweep: a pending review or
               -- quarantine envelope awaits an explicit decision; a blocked
               -- generation on the default branch must not consume it (its
               -- facts are isolated on the quarantine branch) or `okg
               -- admission decide` (which requires pending) is stranded.
               AND (
                 e.admission_mode NOT IN ('review', 'quarantine')
                 OR e.status = 'accepted'
               )
               AND e.target_generation_ref IS NULL
               AND EXISTS (
                 SELECT 1 FROM okg.admission_record_refs r
                  WHERE r.admission_id = e.admission_id
                    AND r.ref_kind IN (
                      'node_fact_range', 'edge_fact_range', 'payload'
                    )
                    AND (
                      r.ref_kind != 'payload'
                      OR r.payload->>'kind' = 'chronos_graph_run'
                    )
               )
            RETURNING e.admission_id::text
            """,
            {
                "deployment_name": generation["deployment_name"],
                "branch_name": generation["branch_name"],
                "generation_id": generation["generation_id"],
                "blocked_reason": Jsonb(dict(generation.get("blocked_reason") or {})),
                "invariant_summary": Jsonb(
                    dict(generation.get("invariant_summary") or {})
                ),
                "workflow_ref": Jsonb(dict(workflow_ref or {})),
            },
        )
        rows = cur.fetchall()
    return [{"admission_id": str(row[0])} for row in rows]


def _ensure_generation_ref_column(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE okg.admission_envelopes
              ADD COLUMN IF NOT EXISTS target_generation_ref TEXT
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS admission_envelopes_generation_ref_idx
              ON okg.admission_envelopes (target_generation_ref)
              WHERE target_generation_ref IS NOT NULL
            """
        )


def _validate_common(
    *,
    producer_kind: str,
    admission_mode: str,
    status: str,
) -> None:
    if producer_kind not in PRODUCER_KINDS:
        raise SubstrateError(
            f"unknown admission producer_kind {producer_kind!r}",
            data={"producer_kind": producer_kind},
        )
    if admission_mode not in ADMISSION_MODES:
        raise SubstrateError(
            f"unknown admission mode {admission_mode!r}",
            data={"admission_mode": admission_mode},
        )
    if status not in ADMISSION_STATUSES:
        raise SubstrateError(
            f"unknown admission status {status!r}",
            data={"status": status},
        )


def _uuid_text(value: str | UUID) -> str:
    try:
        return str(UUID(str(value)))
    except ValueError as exc:
        raise SubstrateError(
            f"invalid UUID value {value!r}",
            data={"value": str(value)},
        ) from exc
