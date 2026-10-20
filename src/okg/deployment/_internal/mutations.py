"""Mutation helpers (Phase 2 §3.1, §3.3).

Bridges the SDK's public mutation surface to the substrate's
``okg.node_facts`` / ``okg.edge_facts`` audit log and the Chronos
graph (``okg.graph_nodes`` / ``okg.graph_edges``). Two principles
guide the design:

  1. **Explicit provenance.** The SDK helpers stamp ``source`` as
     ``_deployment:<name>:<workflow>`` so audit trails distinguish
     SDK writes from substrate-internal writes.
  2. **Worker-write generation.** All SDK mutations land at the active
     worker-write generation_id (cached per-transaction via
     :mod:`okg.deployment._internal.context`). Dual-mutation within
     a single ``@deployment_transaction`` lands at one generation.

Each mutation records an audit fact (``okg.node_facts`` /
``okg.edge_facts``) and then applies the graph edit through
:meth:`ChronosGraphStore.apply_facts`, which upserts (op ``I``) or
retracts (op ``R``) the node/edge in the Chronos graph. Narrowing
is enforced by the Chronos publish gate, not inline at edge write.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from okg.substrate.images.values import check_incoming

from .context import current_context, current_worker_write_generation


__all__ = [
    "emit_edge_fact",
    "emit_node_fact",
    "set_progress",
    "update_node_attrs",
]


log = logging.getLogger(__name__)


# Source-string prefix stamped into okg.node_facts.source so audit
# queries (e.g. provenance-split audit, doctor checks) can distinguish
# SDK writes from substrate-internal writes.
_SDK_SOURCE_PREFIX = "_deployment"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sdk_source(deployment: str, workflow_id: str) -> str:
    """Build the canonical SDK source identifier."""
    return f"{_SDK_SOURCE_PREFIX}:{deployment}:{workflow_id}"


def _coerce_record_id(value: Any) -> Any:
    """Convert a user-supplied source_record_id into a JSON-safe value.

    okg.node_facts.source_record_id is JSONB. Common author inputs:

      * None — let the SDK derive from (workflow_id, step_id, dedupe_key)
      * str — wrapped as `{"id": value}`
      * int — wrapped as `{"id": value}`
      * dict — used as-is

    Anything else is rejected with a TypeError so authors don't get a
    silent JSONB serialization surprise.
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return {"id": value}
    if isinstance(value, dict):
        return value
    raise TypeError(
        f"source_record_id must be None, scalar, or dict; got "
        f"{type(value).__name__}"
    )


def _coerce_revision(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return {"revision": value}
    if isinstance(value, dict):
        return value
    raise TypeError(
        f"source_revision must be None, scalar, or dict; got "
        f"{type(value).__name__}"
    )


def _stable_edge_id(src: str, edge_type: str, dst: str) -> int:
    """Deterministic 63-bit edge id from the (src, edge_type, dst) triple.

    Mirrors the formula in `sdk-design.md` §2.2.2:
    ``uuid5(NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int & ((1<<63)-1)``.
    """
    edge_uuid = uuid.uuid5(
        uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}",
    )
    return edge_uuid.int & ((1 << 63) - 1)


# ---------------------------------------------------------------------------
# emit_node_fact
# ---------------------------------------------------------------------------


def emit_node_fact(
    node_id: str,
    subtype: str,
    attrs: dict,
    *,
    dedupe_key: str,
    op: str = "I",
    source_record_id: Any = None,
    source_revision: Any = None,
) -> int:
    """Insert a fact row and update the live projection at the active
    worker-write generation.

    Equivalent to a publisher-side projection step, but driven by the
    SDK from a deployment workflow context.

    Parameters mirror ``okg.node_facts`` column semantics; see
    ``src/okg/substrate/db/schema.sql`` for the constraint shapes.

    Returns
    -------
    int
        The newly inserted ``okg.node_facts.fact_id``.

    Raises
    ------
    MissingContextError
        Called outside a ``@deployment_transaction``.
    ValueError
        ``op`` is not 'I' or 'R'.
    ImageValueForged
        An image value in ``attrs`` carries ``png`` or ``error``, which only
        okg's ingest image step may add. Nothing is written.
    ImageRefInvalid
        A mapping tagged ``okg_image`` in ``attrs`` is not a valid image
        reference. Nothing is written.
    """
    if op not in ("I", "R"):
        raise ValueError(f"op must be 'I' or 'R', got {op!r}")
    check_incoming(attrs)
    return _emit_node_fact(
        node_id,
        subtype,
        attrs,
        dedupe_key=dedupe_key,
        op=op,
        source_record_id=source_record_id,
        source_revision=source_revision,
    )


def _emit_node_fact(
    node_id: str,
    subtype: str,
    attrs: dict,
    *,
    dedupe_key: str,
    op: str,
    source_record_id: Any = None,
    source_revision: Any = None,
) -> int:
    """Write one node fact and its graph edit. The caller has checked
    ``op`` and the caller-supplied part of ``attrs``."""
    ctx = current_context()
    gen_id = current_worker_write_generation()
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    coerced_record = _coerce_record_id(source_record_id) or {
        "workflow_id": ctx.workflow_id,
        "step_id": ctx.step_id,
        "dedupe_key": dedupe_key,
    }
    coerced_revision = _coerce_revision(source_revision) or {
        "observed_at": now.isoformat(),
    }

    with ctx.conn.cursor() as cur:
        # Insert fact.
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb,
                    %s, %s, %s::uuid, %s)
            RETURNING fact_id
            """,
            (
                node_id, subtype, json.dumps(attrs),
                _sdk_source(ctx.deployment, ctx.workflow_id),
                json.dumps(coerced_record),
                json.dumps(coerced_revision),
                now, dedupe_key, run_id, op,
            ),
        )
        fact_id = cur.fetchone()[0]

    # Apply the graph edit to the Chronos graph. op='I' upserts the
    # node; op='R' retracts it (removes it from the branch).
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    ChronosGraphStore.from_connection(
        ctx.conn, ensure_schema=False,
    ).apply_facts(
        [
            NodeFact(
                node_id,
                subtype,
                attrs,
                coerced_record,
                coerced_revision,
                op=op,
            )
        ],
        source=_sdk_source(ctx.deployment, ctx.workflow_id),
        run_id=run_id,
        observed_at=now,
        strict_endpoints=False,
    )

    log.debug(
        "emit_node_fact: node=%s subtype=%s op=%s fact_id=%s gen=%s",
        node_id, subtype, op, fact_id, gen_id,
    )
    return fact_id


# ---------------------------------------------------------------------------
# emit_edge_fact
# ---------------------------------------------------------------------------


def emit_edge_fact(
    src: str,
    dst: str,
    edge_type: str,
    attrs: dict,
    *,
    dedupe_key: str,
    provenance: str = "derived_deterministic",
    confidence: float = 1.0,
    op: str = "I",
    source_record_id: Any = None,
    source_revision: Any = None,
) -> int:
    """Insert an edge fact and apply the edge to the Chronos graph.

    Narrowing — refusing edges whose ``(src_subtype, edge_type,
    dst_subtype)`` is not in the active catalog narrowings — is no
    longer enforced inline here; the Chronos publish gate validates
    narrowing when the generation is checkpointed.

    Returns
    -------
    int
        The newly inserted ``okg.edge_facts.fact_id``.
    """
    if op not in ("I", "R"):
        raise ValueError(f"op must be 'I' or 'R', got {op!r}")
    if provenance not in (
        "explicit", "derived_deterministic", "linked_heuristic",
        "extracted_model", "derived_semantic",
    ):
        raise ValueError(
            f"provenance {provenance!r} not in the allowed set; see "
            "okg.edge_facts.provenance CHECK constraint"
        )
    if not (0.0 <= confidence <= 1.0):
        raise ValueError(
            f"confidence must be in [0, 1], got {confidence!r}"
        )
    check_incoming(attrs)

    ctx = current_context()
    gen_id = current_worker_write_generation()
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    edge_id = _stable_edge_id(src, edge_type, dst)

    coerced_record = _coerce_record_id(source_record_id) or {
        "workflow_id": ctx.workflow_id,
        "step_id": ctx.step_id,
        "dedupe_key": dedupe_key,
    }
    coerced_revision = _coerce_revision(source_revision) or {
        "observed_at": now.isoformat(),
    }

    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.edge_facts
              (edge_id, src, dst, edge_type, attrs, provenance,
               confidence, source, source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s, %s, %s::uuid, %s)
            RETURNING fact_id
            """,
            (
                edge_id, src, dst, edge_type, json.dumps(attrs),
                provenance, confidence,
                _sdk_source(ctx.deployment, ctx.workflow_id),
                json.dumps(coerced_record),
                json.dumps(coerced_revision),
                now, dedupe_key, run_id, op,
            ),
        )
        fact_id = cur.fetchone()[0]

    # Apply the graph edit to the Chronos graph. op='I' upserts the
    # edge; op='R' retracts it (removes it from the branch).
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact

    ChronosGraphStore.from_connection(
        ctx.conn, ensure_schema=False,
    ).apply_facts(
        [
            EdgeFact(
                src,
                dst,
                edge_type,
                attrs=attrs,
                provenance=provenance,
                confidence=confidence,
                source_record_id=coerced_record,
                source_revision=coerced_revision,
                op=op,
            )
        ],
        source=_sdk_source(ctx.deployment, ctx.workflow_id),
        run_id=run_id,
        observed_at=now,
        strict_endpoints=False,
    )

    log.debug(
        "emit_edge_fact: src=%s edge=%s dst=%s op=%s fact_id=%s gen=%s",
        src, edge_type, dst, op, fact_id, gen_id,
    )
    return fact_id


# ---------------------------------------------------------------------------
# update_node_attrs
# ---------------------------------------------------------------------------


def update_node_attrs(
    node_id: str,
    new_attrs: dict,
    *,
    dedupe_key: str | None = None,
) -> int:
    """Mutate an existing node's attrs.

    Reads the node's current attrs from the Chronos graph at the
    active worker-write generation, merges ``new_attrs`` over them
    (shallow), and writes the merged node back via
    :func:`emit_node_fact` (which records the fact and upserts the
    graph node).

    Parameters
    ----------
    node_id : str
        Node to update.
    new_attrs : dict
        Attrs to merge over the current node's attrs (shallow update).
    dedupe_key : str | None
        Optional dedupe key; auto-derived from
        ``(node_id, attrs hash)`` when omitted so the same logical
        update from the same step is naturally idempotent.

    Returns
    -------
    int
        The new ``okg.node_facts.fact_id``.

    Only ``new_attrs`` is checked for forged or invalid image values: the
    current attrs were written by okg and may legitimately hold image
    values ingest completed, which this update carries forward unchanged.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    check_incoming(new_attrs)
    ctx = current_context()

    # Read the current node from the default-branch HEAD -- NOT
    # checkout_generation(worker_write_gen). Worker-write generations are never
    # Chronos-checkpointed (only PUBLISHED generations get a checkpoint), so
    # checkout_generation(gen_id) raises KeyError. emit_node_fact's apply_facts
    # writes to the same HEAD, so the read-merge-upsert is consistent.
    graph = ChronosGraphStore.from_connection(
        ctx.conn, ensure_schema=False,
    ).checkout()
    rows = graph.query(
        """
        SELECT subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = :node_id
         LIMIT 1
        """,
        {"node_id": node_id},
    )
    if not rows:
        raise ValueError(
            f"update_node_attrs: node {node_id!r} not found in graph"
        )
    subtype = rows[0]["subtype"]
    current_attrs = rows[0]["attrs"] or {}

    merged = {**current_attrs, **new_attrs}
    if dedupe_key is None:
        key_input = (
            f"{node_id}|{json.dumps(merged, sort_keys=True)}"
        )
        dedupe_key = hashlib.sha256(
            key_input.encode("utf-8")
        ).hexdigest()

    return _emit_node_fact(
        node_id,
        subtype,
        merged,
        dedupe_key=dedupe_key,
        op="I",
    )


# ---------------------------------------------------------------------------
# set_progress (§3.3)
# ---------------------------------------------------------------------------


def set_progress(key: str, value: Any) -> None:
    """Publish a workflow event for operator visibility.

    Fire-and-forget by design (Phase 0 C2): NOT a durable counter.
    Operators read this via ``okg runtime get-event <workflow_id>
    <key>``. For durable accumulation, use :class:`cost_budget`
    or insert into a deployment-owned ledger table.

    No-op outside a DBOS workflow context (e.g. tests using
    :func:`bind_test_context` without a DBOS singleton).
    """
    try:
        from dbos import DBOS

        DBOS.set_event(key, value)
    except Exception:  # noqa: BLE001 — fire and forget
        log.debug("set_progress: no DBOS context; dropping key=%s", key)
