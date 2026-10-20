"""Enricher and derived-fact work computed before the publish lock.

Issue 2941 fix 1 (design: ``pact/changes/lane-h-2941-prelock-enrich/
design.md``). Before taking the deployment's publish lock the publisher may
compute, read-only:

* the dirty scope (the rows written since the last published generation);
* for each enricher that sets ``prelock_plannable``, the exact edge-fact rows
  it would insert (``EnricherPrelockPlan``);
* for those rows, the read-and-compute half of the derived-fact apply, one
  ``PreparedFactApply`` per FactBatch chunk.

Under the lock each piece is used only after a recheck proves its inputs did
not change; otherwise the cycle computes it under the lock, as before. This
module holds the state, the fingerprint and the report; the publisher wires
them in. Nothing here writes: the pass runs with
``default_transaction_read_only`` on.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping

import psycopg

from okg.substrate.library.sources.base import EdgeFact

PRELOCK_ENRICH_ENV = "OKG_PRELOCK_ENRICH"
_UNSET = object()


def prelock_enrich_disabled() -> bool:
    """The kill switch: ``OKG_PRELOCK_ENRICH=0`` turns the pass off."""
    return os.environ.get(PRELOCK_ENRICH_ENV, "1") == "0"


def rss_mb() -> float | None:
    """This process's resident set size in MiB, or None when unreadable."""
    try:
        with open("/proc/self/status", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024.0, 1)
    except OSError:
        pass
    try:
        import resource
        import sys

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports bytes, Linux KiB; this branch is the peak, not RSS.
        divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
        return round(peak / divisor, 1)
    except (ImportError, OSError, ValueError):
        # No ``resource`` module (Windows) or no usage figure: unreadable.
        return None


@contextmanager
def read_only_session(conn: psycopg.Connection) -> Iterator[None]:
    """Make every statement on ``conn`` read-only for the block.

    The connection is in autocommit mode, so each statement is its own
    transaction and ``default_transaction_read_only`` applies to all of them;
    any write raises. Temporary tables stay allowed. The previous setting is
    restored afterwards.
    """
    with conn.cursor() as cur:
        cur.execute("SHOW default_transaction_read_only")
        previous = str(cur.fetchone()[0])
        cur.execute("SET default_transaction_read_only = on")
    try:
        yield
    finally:
        if conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
            conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('default_transaction_read_only', %s, false)",
                (previous,),
            )


def graph_fingerprint(
    conn: psycopg.Connection,
    *,
    store: Any,
    deployment_name: str,
    branch: str,
    with_segment_signature: bool = True,
) -> dict[str, Any]:
    """What the pre-lock work read, other than enricher state.

    The branch's latest checkpoint (any status) and current segment, the
    current segment's write signature (``dirty_hint_coverage``), the applied
    catalog version and the active Nomos policy fingerprint.
    """
    from okg.substrate.nomos.store import load_active_policy_bundle
    # Imported under a name that says what it does here: it only reads.
    from okg.substrate.publisher.dirty_hint_coverage import (
        segment_write_signature as read_segment_signature,
    )

    latest = store.latest_generation(
        deployment_name=deployment_name, branch=branch, status=None,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT current_segment_id FROM _chronos_branch_interval_branches "
            "WHERE branch_id = %s",
            (branch,),
        )
        row = cur.fetchone()
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        )
        catalog = cur.fetchone()[0]
    policy = load_active_policy_bundle(
        conn,
        deployment_name=deployment_name,
        scope=f"deployment:{deployment_name}",
    )
    out: dict[str, Any] = {
        "latest_checkpoint_id": (
            str(latest.checkpoint_id) if latest is not None else None
        ),
        "current_segment_id": int(row[0]) if row is not None else None,
        "catalog_version_id": int(catalog) if catalog is not None else None,
        "policy_fingerprint": (
            policy.policy_fingerprint if policy is not None else None
        ),
    }
    if with_segment_signature:
        out["segment_signature"] = read_segment_signature(
            conn, branch_name=branch,
        )
    return out


def enricher_state_fingerprint(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> dict[str, Any]:
    """Fact-log watermarks and sequences, and every enricher progress row."""
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(max(fact_id), 0) FROM okg.node_facts")
        node_watermark = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COALESCE(max(fact_id), 0) FROM okg.edge_facts")
        edge_watermark = int(cur.fetchone()[0] or 0)
        # The fact-id sequences move on every insert, including facts a
        # writer inserts and deletes again before this reads (the merge-back
        # gate). An early change signal only: ids are reserved before their
        # transaction commits, and a deleted fact leaves no other trace, so
        # the linkers repeat their "already said this" check under the lock.
        cur.execute(
            """
            SELECT pg_sequence_last_value(
                       pg_get_serial_sequence('okg.node_facts', 'fact_id')),
                   pg_sequence_last_value(
                       pg_get_serial_sequence('okg.edge_facts', 'fact_id'))
            """
        )
        node_sequence, edge_sequence = cur.fetchone()
        cur.execute(
            """
            SELECT enricher_name, fact_watermark, edge_watermark, last_status,
                   last_run_at::text, catalog_version_id
              FROM okg.enricher_progress
             WHERE deployment_name = %s
             ORDER BY enricher_name
            """,
            (deployment_name,),
        )
        progress = [tuple(str(v) if v is not None else None for v in r)
                    for r in cur.fetchall()]
        # The linkers' "already said this" check reads prior facts; the
        # watermarks catch an append, and the chunk list catches the fact
        # retention policy dropping old facts (TimescaleDB drop_chunks).
        cur.execute("SELECT to_regclass('timescaledb_information.chunks')")
        chunks: Any = "timescaledb_absent"
        if cur.fetchone()[0] is not None:
            cur.execute(
                """
                SELECT hypertable_name, chunk_name
                  FROM timescaledb_information.chunks
                 WHERE hypertable_schema = 'okg'
                   AND hypertable_name IN ('node_facts', 'edge_facts')
                 ORDER BY hypertable_name, chunk_name
                """
            )
            chunks = [tuple(r) for r in cur.fetchall()]
    return {
        "node_fact_watermark": node_watermark,
        "edge_fact_watermark": edge_watermark,
        "node_fact_sequence": (
            int(node_sequence) if node_sequence is not None else None
        ),
        "edge_fact_sequence": (
            int(edge_sequence) if edge_sequence is not None else None
        ),
        "enricher_progress": progress,
        "fact_chunks": chunks,
    }


def differing_keys(
    before: Mapping[str, Any], after: Mapping[str, Any],
) -> list[str]:
    return sorted(
        key for key in set(before) | set(after)
        if before.get(key, _UNSET) != after.get(key, _UNSET)
    )


def predicted_edge_facts(
    conn: psycopg.Connection,
    plan: Any,
) -> list[EdgeFact]:
    """The facts the derived-fact loader will read back for ``plan``'s rows.

    The loader decodes ``attrs``, ``source_record_id`` and
    ``source_revision`` from ``jsonb`` and orders a run's facts by
    ``(src, edge_type, dst)``. Doing both in PostgreSQL here gives the same
    Python values and the same order; the apply under the lock still
    compares every chunk's facts with the prepared ones.
    """
    rows = list(plan.rows)
    if not rows:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.src, r.dst, r.edge_type, r.attrs::jsonb,
                   r.source_record_id::jsonb, r.source_revision::jsonb
              FROM unnest(%s::text[], %s::text[], %s::text[],
                          %s::text[], %s::text[], %s::text[])
                     AS r(src, dst, edge_type, attrs,
                          source_record_id, source_revision)
             ORDER BY r.src, r.edge_type, r.dst
            """,
            (
                [row[1] for row in rows],
                [row[2] for row in rows],
                [row[3] for row in rows],
                [row[4] for row in rows],
                [row[5] for row in rows],
                [row[6] for row in rows],
            ),
        )
        decoded = cur.fetchall()
    return [
        EdgeFact(
            src=str(src),
            dst=str(dst),
            edge_type=str(edge_type),
            attrs=dict(attrs or {}),
            provenance=str(plan.provenance),
            confidence=float(plan.confidence),
            source_record_id=dict(source_record_id or {}),
            source_revision=dict(source_revision or {}),
            op="I",
        )
        for src, dst, edge_type, attrs, source_record_id, source_revision
        in decoded
    ]


def report_counters(report: Mapping[str, Any]) -> dict[str, float]:
    """The report's numbers, flat, for ``timings_ms`` (``prelock_enrich.*``)."""
    out: dict[str, float] = {}
    if not report:
        return out
    out["prelock_enrich.planned"] = 1.0 if report.get("planned") else 0.0
    out["prelock_enrich.fallback"] = 1.0 if report.get("fallback") else 0.0
    out["prelock_enrich.failed"] = 1.0 if report.get("failed") else 0.0
    for key in ("rss_before_mb", "rss_after_mb", "rss_delta_mb"):
        value = report.get(key)
        if isinstance(value, (int, float)):
            out[f"prelock_enrich.{key}"] = float(value)
    scope = report.get("dirty_scope") or {}
    out["prelock_enrich.dirty_scope_seeded"] = float(scope.get("seeded", 0) or 0)
    if isinstance(scope.get("elapsed_ms"), (int, float)):
        out["prelock_enrich.dirty_scope_ms"] = float(scope["elapsed_ms"])
    enrichers = report.get("enrichers") or {}
    out["prelock_enrich.enrichers_planned"] = float(
        sum(1 for entry in enrichers.values() if entry.get("planned"))
    )
    out["prelock_enrich.enrichers_used"] = float(
        sum(1 for entry in enrichers.values() if entry.get("used"))
    )
    out["prelock_enrich.planned_rows"] = float(
        sum(int(entry.get("rows", 0) or 0) for entry in enrichers.values())
    )
    out["prelock_enrich.derived_chunks_prepared"] = float(
        sum(int(entry.get("derived_chunks", 0) or 0)
            for entry in enrichers.values())
    )
    derived = report.get("derived") or {}
    for key in (
        "chunks_reused",
        "chunks_recomputed",
        "chunk_lock_hold_ms_total",
        "chunk_lock_hold_ms_max",
        "chunk_check_ms_total",
    ):
        out[f"prelock_enrich.{key}"] = float(derived.get(key, 0) or 0)
    for label, value in (derived.get("chunk_sums") or {}).items():
        out[f"prelock_enrich.chunk_sum.{label}"] = float(value)
    return out


@dataclass
class PrelockEnrichState:
    """Everything the pre-lock pass computed, and what became of it."""

    report: dict[str, Any]
    previous_checkpoint_id: str | None = None
    graph: dict[str, Any] = field(default_factory=dict)
    enricher_state: dict[str, Any] = field(default_factory=dict)
    dirty_scope: Any = _UNSET
    dirty_scope_from_hint: bool = False
    plans: dict[str, Any] = field(default_factory=dict)
    # (producer_id, run uuid) -> {ordinal: PreparedFactApply}
    prepared: dict[tuple[str, uuid.UUID], dict[int, Any]] = field(
        default_factory=dict
    )
    enrichers_usable: bool = False
    derived_usable: bool = False

    def drop(self, reason: str, **detail: Any) -> None:
        """Discard every plan; the cycle computes everything under the lock."""
        self.plans = {}
        self.prepared = {}
        self.dirty_scope = _UNSET
        self.enrichers_usable = False
        self.derived_usable = False
        if self.report.get("fallback") is None:
            self.report["fallback"] = {"reason": reason, **detail}

    def release(self) -> None:
        """Drop everything once the derived apply has used it.

        That apply wrote the branch head, so the pre-lock dirty scope can
        no longer be current; keeping a reference would hold a second copy
        of the scope in memory while the cycle rescans (on okg-workspace,
        several GB at the peak).
        """
        self.plans = {}
        self.prepared = {}
        self.dirty_scope = _UNSET
        self.enrichers_usable = False
        self.derived_usable = False

    def seed_dirty_scope(
        self,
        *,
        segment_signature: Any,
        previous_checkpoint_id: str | None,
    ) -> tuple[bool, Any]:
        """``(True, scope)`` when the pre-lock dirty scope is still current.

        Current means the branch head's segment signature and the
        predecessor are the ones the scope was read under. ``(False, None)``
        otherwise; the cycle then scans as before.
        """
        entry = self.report.setdefault("dirty_scope", {})
        if self.dirty_scope is _UNSET or self.dirty_scope_from_hint:
            return False, None
        if previous_checkpoint_id != self.previous_checkpoint_id:
            entry["seed_refused"] = "predecessor_changed"
            return False, None
        if segment_signature != self.graph.get("segment_signature"):
            entry["seed_refused"] = "segment_signature_changed"
            return False, None
        entry["seeded"] = int(entry.get("seeded", 0)) + 1
        return True, self.dirty_scope

    def plan_for(self, enricher_name: str) -> Any | None:
        if not self.enrichers_usable:
            return None
        return self.plans.get(enricher_name)

    def note_enricher(self, enricher_name: str, **values: Any) -> None:
        entry = self.report.setdefault("enrichers", {}).setdefault(
            enricher_name, {},
        )
        entry.update(values)

    def prepared_for(
        self, producer_id: str, run_id: Any,
    ) -> dict[int, Any] | None:
        if not self.derived_usable or run_id is None:
            return None
        try:
            key = (str(producer_id), uuid.UUID(str(run_id)))
        except ValueError:
            return None
        return self.prepared.get(key)

    def note_derived_result(self, result: Any) -> None:
        """Fold one derived batch's per-chunk outcomes into the report."""
        derived = self.report.setdefault("derived", {})
        for primitive in getattr(result, "primitive_results", ()) or ():
            timings = dict(getattr(primitive, "timings_ms", {}) or {})
            if timings.get("prepared.reused"):
                derived["chunks_reused"] = int(
                    derived.get("chunks_reused", 0)
                ) + 1
            elif timings.get("prepared.recomputed"):
                derived["chunks_recomputed"] = int(
                    derived.get("chunks_recomputed", 0)
                ) + 1
                reasons = derived.setdefault("recompute_reasons", {})
                for label in timings:
                    if label.startswith("prepared.recomputed."):
                        reason = label[len("prepared.recomputed."):]
                        reasons[reason] = int(reasons.get(reason, 0)) + 1
            else:
                continue
            hold = float(timings.get("prepared.lock_hold", 0.0) or 0.0)
            derived["chunk_lock_hold_ms_total"] = round(
                float(derived.get("chunk_lock_hold_ms_total", 0.0)) + hold, 3,
            )
            derived["chunk_lock_hold_ms_max"] = round(
                max(float(derived.get("chunk_lock_hold_ms_max", 0.0)), hold), 3,
            )
            check = float(timings.get("prepared.check", 0.0) or 0.0)
            derived["chunk_check_ms_total"] = round(
                float(derived.get("chunk_check_ms_total", 0.0)) + check, 3,
            )
            # Issue #2941: where a chunk's lock hold goes, step by step. Each
            # numeric ``transaction.*`` entry is summed over the chunks as it
            # is: a step's milliseconds, or a count or flag the write records
            # beside them (``.recorded``, ``.handoffs``, ``.row_count``). A
            # configured threshold is not summed. ``transaction.total`` starts
            # after the lock wait and the check; ``prepared.lock_hold`` is the
            # whole hold.
            sums = derived.setdefault("chunk_sums", {})
            for label, value in timings.items():
                if (
                    label.startswith("transaction.")
                    and not label.endswith("threshold")
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                ):
                    sums[label] = round(
                        float(sums.get(label, 0.0)) + float(value), 3,
                    )


__all__ = [
    "PRELOCK_ENRICH_ENV",
    "PrelockEnrichState",
    "differing_keys",
    "enricher_state_fingerprint",
    "graph_fingerprint",
    "predicted_edge_facts",
    "prelock_enrich_disabled",
    "read_only_session",
    "report_counters",
    "rss_mb",
]
