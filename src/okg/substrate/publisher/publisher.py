"""Publisher: single-writer Chronos checkpoint publication.

@implements_concept: happens-before-ordering

The publisher's monotonically-advancing `generation_id` (held
under an advisory lock so only one writer ever assigns the next
id) is Lamport's happens-before ordering applied to substrate
state: every reader at gen G sees ALL writes whose
`valid_from_gen <= G`, and no writes from G+1 or later.
Concurrent agents' edits are linearized into a single total
order by this lock.

The live publish path no longer projects fact-log rows into live tables. It
applies source and derived facts to the Chronos graph, evaluates publish gates
against that Chronos checkout, and checkpoints the branch head as the published
generation.

See docs/develop/architecture/overview.md for the publication responsibility.
"""
from __future__ import annotations

import functools
import hashlib
import json
import logging
import math
import os
import re
import sys
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import (
    Any, Callable, Iterable, Iterator, Mapping, NamedTuple, Optional, Sequence,
)

import psycopg
from chronos_core.branching import BranchingError
from psycopg import sql

from okg.substrate.consistency import (
    ConsistencyPublishGate,
    ConsistencyPublishGateEvaluation,
    consistency_gate_summary,
    run_one_post_publish_check,
)
from okg.substrate.consistency.sql_gate import (
    evaluate_post_publish_check_reading_mirror,
)
from okg.substrate.consistency.chronos_sql_mirror import (
    chronos_sql_mirror_for_checks,
)
from okg.substrate.enrichers.base import IncrementalContext
from okg.substrate.enrichers.lineage import (
    UNKNOWN_DECLARED_SOURCE,
    WITHHELD_INPUT_SOURCE_HELD,
    build_enricher_run_lineage_envelope,
    declare_enricher_dependencies,
    invalidations_from_chronos_dirty_scope,
    plan_enricher_lineage,
    held_input_sources,
    resolve_declared_source_ids,
    source_alias_index,
    unknown_declared_source_status,
    withheld_input_source_status,
)
from okg.substrate.enrichers.governance import (
    enricher_governance_summary,
    merge_enricher_projection_deltas,
    record_enricher_governance_available,
    record_enricher_governance_failure,
    validate_enricher_judgment_registration,
)
from okg.substrate.errors import (
    PublisherLeaseHeld,
    SubstrateError,
)
from okg.substrate.admission import (
    FactRangeRef,
    attach_fact_range_refs,
    ensure_envelope,
    record_event,
    stable_idempotency_key,
    transition_envelope,
    workflow_ref_from_dbos,
)
from okg.substrate.producer_authority import (
    ENVELOPE_VIOLATION_CODES,
    ProducerPolicyViolation,
    evaluate_derived_output_trust,
    evaluate_edge_fact_range_signature,
    evaluate_node_fact_range_signature,
    load_profile_for_producer,
    materialize_producer_profile,
    merge_policy_evidence,
    record_envelope_violation,
    normalize_allowed_record_kinds,
    normalize_authority_scope,
    normalize_output_signature,
    normalize_policy_clauses,
    policy_from_authority,
    profile_fingerprint,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    normalize_source_lineage,
    source_ids_from_lineage,
)
from okg.substrate.projection import (
    AliasResolver,
    EntityExtractor,
    Projector,
)
from okg.substrate.catalog.linker_config import LINK_ENDPOINT_SOURCE
from okg.substrate.projection.link_endpoints import (
    endpoints_to_retract,
    plan_link_endpoint_facts,
)
from okg.substrate.projection.retry import (
    LinkRetryKey,
    acknowledge_extractor_queue,
    acknowledge_link_retries,
    clear_unobserved_unready_link_retries,
    queued_extractor_node_ids,
    ready_link_retry_keys_for_nodes,
    ready_link_retry_node_ids,
    record_link_retry_misses,
)
from okg.substrate.publisher.extractor_stage import ChronosExtractorStage
from okg.substrate.publisher.gates import (
    DEFAULT_CROSS_BUDGET_MS,
    evaluate_gates,
    preflight_invariants,
)
from okg.substrate.types import (
    PhaseAResult,
    PhaseBResult,
    ReservedGeneration,
    SourceProjectionStatus,
)
from okg.substrate.profiling import profile_stage
from okg.substrate.publish_lock_headroom import (
    DEFAULT_LOCK_HOLD_HEADROOM_FACTOR as _HEADROOM_FACTOR,
    DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS as _HARD_TIMEOUT_SECONDS,
    evaluate_lock_headroom,
)
from okg.substrate.runtime.source_sync_priority import (
    PublisherWaitIntent,
    publisher_publish_advisory_keys,
    own_run_guard_runtime_blocker,
    source_sync_admission_intent_workflow_id_on_connection,
    source_sync_run_guard_held,
)
from okg.substrate.telemetry import (
    SpanTimingRecorder,
    TelemetrySpan,
    child_span_identity,
    current_trace_identity,
    deep_span_capture_enabled,
    record_context_span_if_missing,
    record_graph_effect_manifest,
    record_span,
    trace_identity_from_latest_source_progress,
)
from okg.substrate.transform.commit import (
    commit_witness_session,
    persist_transform_program,
    pin_commit_witness,
)
from okg.substrate.transform.programs import enrichment_program


_COUNT_STAMP_RECOVERY_AUTHORIZATION_ENV = (
    "OKG_COUNT_STAMP_RECOVERY_AUTHORIZATION"
)


def _publish_telemetry_status_message(
    blocked_reason: Mapping[str, Any] | str | None,
) -> str | None:
    """Return a bounded scalar status message for psycopg telemetry writes."""
    if blocked_reason is None:
        return None
    if isinstance(blocked_reason, Mapping):
        reason = blocked_reason.get("reason")
        failed_gates = blocked_reason.get("failed_consistency_gates")
        if not reason and isinstance(failed_gates, list) and failed_gates:
            first_gate = failed_gates[0]
            if isinstance(first_gate, Mapping):
                reason = (
                    first_gate.get("reason")
                    or first_gate.get("gate_name")
                    or first_gate.get("check_id")
                )
        return str(reason or "publish_blocked")[:512]
    return str(blocked_reason)[:512]


_WRITE_RACE_ATTEMPTS_ENV = "OKG_PUBLISH_WRITE_RACE_ATTEMPTS"
_WRITE_RACE_ATTEMPTS_DEFAULT = 3


def _write_race_attempts_allowed() -> int:
    """Publish attempts per cycle() call when other writers keep racing it.

    ``OKG_PUBLISH_WRITE_RACE_ATTEMPTS`` must be a positive integer; anything
    else is refused rather than silently replaced by the default.
    """
    raw = os.environ.get(_WRITE_RACE_ATTEMPTS_ENV)
    if raw is None or raw == "":
        return _WRITE_RACE_ATTEMPTS_DEFAULT
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        raise SubstrateError(
            f"{_WRITE_RACE_ATTEMPTS_ENV} must be a positive integer",
            data={"value": raw},
        )
    return value


def _write_race_backoff_seconds(attempt: int) -> float:
    """Jittered, growing pause before re-running a refused publish."""
    import random

    return 0.5 * attempt * random.uniform(0.5, 1.5)


def _validate_count_stamp_recovery_authorization(
    raw: str | None,
    *,
    conn: psycopg.Connection,
    deployment: str,
    branch: str,
    predecessor_generation_id: str,
    recorded_counts: tuple[int, int],
    pinned_counts: tuple[int, int],
) -> dict[str, Any] | None:
    """Validate the one-predecessor recovery capability carried by deployment up."""
    if not raw:
        return None
    try:
        authorization = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise SubstrateError(
            "count-stamp recovery authorization is not valid JSON"
        ) from exc
    required_keys = {
        "schema", "deployment", "branch", "branch_id",
        "predecessor_generation_id", "checkpoint_counts", "pinned_counts",
        "classification_binding_digest", "prepared_binding_digest",
        "authorization_digest",
    }
    if not isinstance(authorization, dict) or set(authorization) != required_keys:
        raise SubstrateError(
            "count-stamp recovery authorization has an invalid schema",
            data={
                "observed_keys": (
                    sorted(authorization) if isinstance(authorization, dict) else []
                )
            },
        )
    unsigned = dict(authorization)
    observed_digest = str(unsigned.pop("authorization_digest"))
    expected_digest = "sha256:" + hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    expected_checkpoint = {"node": recorded_counts[0], "edge": recorded_counts[1]}
    expected_pinned = {"node": pinned_counts[0], "edge": pinned_counts[1]}
    checks = {
        "schema": authorization.get("schema")
        == "okg.count_stamp_recovery_authorization.v1",
        "digest": observed_digest == expected_digest,
        "deployment": authorization.get("deployment") == deployment,
        "branch": authorization.get("branch") == branch,
        "predecessor": authorization.get("predecessor_generation_id")
        == predecessor_generation_id,
        "checkpoint_counts": authorization.get("checkpoint_counts")
        == expected_checkpoint,
        "pinned_counts": authorization.get("pinned_counts") == expected_pinned,
        "prepared_binding_digest": str(
            authorization.get("prepared_binding_digest") or ""
        ).startswith("sha256:"),
        "classification_binding_digest": str(
            authorization.get("classification_binding_digest") or ""
        ).startswith("sha256:"),
    }
    if not all(checks.values()):
        raise SubstrateError(
            "count-stamp recovery authorization does not match the current predecessor",
            data={
                "checks": checks,
                "predecessor_generation_id": predecessor_generation_id,
            },
        )
    classification_digest = str(authorization["classification_binding_digest"])
    with conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_name, branch_name, predecessor_generation_id, "
            "stage, authorization_payload, consumed_by_generation_id "
            "FROM okg.effective_attribute_recovery_attempts "
            "WHERE classification_binding_digest = %s",
            (classification_digest,),
        )
        attempt = cur.fetchone()
    journal_checks = {
        "exists": attempt is not None,
        "deployment": attempt is not None and str(attempt[0]) == deployment,
        "branch": attempt is not None and str(attempt[1]) == branch,
        "predecessor": (
            attempt is not None and str(attempt[2]) == predecessor_generation_id
        ),
        "prepared": attempt is not None and str(attempt[3]) == "prepared",
        "authorization_exact": (
            attempt is not None
            and isinstance(attempt[4], dict)
            and dict(attempt[4]) == authorization
        ),
        "unconsumed": attempt is not None and attempt[5] is None,
    }
    if not all(journal_checks.values()):
        raise SubstrateError(
            "count-stamp recovery authorization has no matching prepared journal authority",
            data={"checks": journal_checks},
        )
    from okg.substrate.attributes import classify_effective_attribute_recovery

    current = classify_effective_attribute_recovery(
        conn,
        deployment_name=deployment,
        branch=branch,
    )
    if current.get("binding_digest") != authorization["prepared_binding_digest"]:
        raise SubstrateError(
            "count-stamp recovery prepared view changed before successor publication",
            data={
                "expected": authorization["prepared_binding_digest"],
                "observed": current.get("binding_digest"),
            },
        )
    return authorization


_COUNT_STAMP_RECOVERY_FAILPOINT: Callable[[str], None] | None = None

# How far behind the generation being published an attribute-integrity receipt
# may be and still hold closed interval rows open against the retention prune
# (okg#1603). Past this the sidecar bound wins: the prune reclaims, and the
# receipt refuses and re-anchors instead of subtracting deleted history.
_MAX_PROTECTED_RECEIPT_LAG = 32


def _consume_count_stamp_recovery_authorization(
    conn: psycopg.Connection,
    *,
    authorization: Mapping[str, Any],
    successor_generation_id: str,
) -> None:
    # The caller owns the transaction that also creates the successor checkpoint.
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.effective_attribute_recovery_attempts "
            "SET consumed_by_generation_id = %s, consumed_at = now(), updated_at = now() "
            "WHERE classification_binding_digest = %s AND stage = 'prepared' "
            "AND consumed_by_generation_id IS NULL AND authorization_payload = %s",
            (
                successor_generation_id,
                authorization["classification_binding_digest"],
                json.dumps(dict(authorization)),
            ),
        )
        if cur.rowcount != 1:
            raise SubstrateError(
                "count-stamp recovery journal authority was not consumed exactly once",
                data={"successor_generation_id": successor_generation_id},
            )


# Commit-witness (PACT route-all-writers): the enrichment stage stamps every fact
# its enrichers/linkers emit with this program's checksum, via the GUC-default on
# okg.{node,edge}_facts.source_program_checksum. Persisted once per cycle. The
# setting is pinned while the stage runs (issue #2965): for the session on the
# shipped autocommit connection, where a transaction-scoped value was gone
# before the first INSERT; per transaction on a caller's open transaction
# (transform.commit.pin_commit_witness).
_ENRICHMENT_PROGRAM = enrichment_program()
_ENRICHMENT_CHECKSUM = _ENRICHMENT_PROGRAM.checksum()
_PUBLISHER_TIMING_EVENTS_KEY = "__okg_telemetry_timing_events__"


def _within_enrichment_witness(method):
    """Run an enricher-stage method inside one commit-witness session.

    Every fact an enricher or linker writes on the method's connection without
    naming ``source_program_checksum`` is stamped with the enrichment
    program's checksum, and the setting is cleared when the stage ends, so
    later stages on the same connection are not stamped with it (issue
    #2965). A decorator rather than a renamed body, so the stage keeps its
    name in the formal boundary inventory.
    """

    @functools.wraps(method)
    def wrapper(self, conn, *args, **kwargs):
        with commit_witness_session(conn, _ENRICHMENT_CHECKSUM):
            return method(self, conn, *args, **kwargs)

    return wrapper

# Derived-verdict statuses whose producer output is discarded rather than
# projected. Kept in step with the derived-fact loader's exclusion predicate
# (``status IN ('rejected', 'failed', 'quarantined')``): if the loader drops a
# producer's facts, the operator-facing rejection census must count them.
_DISCARDED_DERIVED_STATUSES = frozenset({
    "rejected",
    "failed",
    "quarantined",
})


DEFAULT_DBOS_PUBLISH_LOCK_WAIT_SECONDS = 600.0
# harden-okg-ci-runtime req.publish-resilience: bound every statement executed
# under the publisher's session advisory lock so a single runaway query (e.g. a
# pathological chronos dirty-edge delta scan) cannot hold the lock for hours and
# wedge ALL publishing + starve the source-sync lane. 0 disables. The default is
# deliberately generous — a normal publish is seconds and even a large backlog
# drains in minutes; this only kills true runaways. Live on okg-ci 2026-06-16:
# one delta query held the lock CPU-bound for 2h26m with no bound, blocking
# publishing for ~5h. Issue #1479: a Publisher now takes this bound from the
# deployment manifest (`publisher.hard_statement_timeout_seconds`); the
# environment variable no longer reaches a constructed Publisher.
# Issue #1291: the 900s value itself now lives in publish_lock_headroom, so the
# receipt, the publish proof, and deployment readiness resolve one bound.
DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS = _HARD_TIMEOUT_SECONDS

# Issue #1479 moved these two knobs into the deployment manifest. A live
# deployment whose directory is outside this repository may still be driven by
# a runbook that exports them -- pact/changes/rebuild-af-pubs-interval-storage
# records exactly that for the af_pubs cold rebuild, which needs 7200s rather
# than the 900s default. Silently ignoring the export would hand that rebuild
# the default bound with nothing said, so say it once per process.
_IGNORED_PUBLISHER_ENV: tuple[tuple[str, str], ...] = (
    ("OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS",
     "publisher.hard_statement_timeout_seconds"),
    ("OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR",
     "publisher.lock_hold_headroom_factor"),
)
# Mutated in place rather than rebound: a module-level flag reassigned under
# `global` reads to CodeQL as py/unused-global-variable, and a set also lets
# each stranded variable be announced on its own.
_ignored_publisher_env_announced: set[str] = set()


def _warn_ignored_publisher_env() -> None:
    """Say once that a publisher-tuning variable no longer reaches a Publisher."""
    for name, key in _IGNORED_PUBLISHER_ENV:
        if os.environ.get(name) is None:
            continue
        if name in _ignored_publisher_env_announced:
            continue
        _ignored_publisher_env_announced.add(name)
        logging.getLogger(__name__).warning(
            "%s is set but no longer configures the publisher. "
            "Declare %s in the deployment manifest instead; this publisher is "
            "using its manifest or compatibility-default value.",
            name,
            key,
        )
# Soft watchdog: log a WARNING when the publisher holds the advisory lock longer
# than this — an early signal well before the hard statement_timeout fires.
# The warning is observability only; the blocking capacity bar is the
# headroom policy in publish_lock_headroom (issue #1291).
DEFAULT_PUBLISH_LOCK_HOLD_WARN_SECONDS = 120.0
_PHASE_EVENT_BATCH_CURSOR_KEY = "__publisher_phase_event_batch__"
_PHASE_EVENT_BATCH_EVENT_KEY = "publisher_phase_events"
_PHASE_EVENT_BATCH_SCHEMA = "okg.publisher.phase_events.v1"
_UNSET = object()

# ---------------------------------------------------------------------------
# Graph-count provenance and progress (issue #1456)
#
# `_chronos_current_graph_counts` has two implementations that return the SAME
# exact number by different routes, and a receipt that does not say which one
# ran cannot be audited. The static method has one positional parameter and is
# monkeypatched in several test suites, so widening its signature would break
# every double; a thread-local side channel carries the label and the optional
# per-table progress sink instead. The label is written on every route, and the
# reader clears it first, so a stale or missing label reads as "unknown" rather
# than silently attributing the wrong source.
#
# Thread-local rather than a ContextVar for one specific reason: the DBOS
# boundary scanner classifies a function by the names it calls, and a
# `ContextVar.set` call inside a read-only counting helper reclassifies it as a
# state mutation (the same trap #1479 hit with `dict.update`). Plain attribute
# assignment carries the same value and stays honest about what the function
# does. The publisher cycle is synchronous, so thread scoping is sufficient.
_GRAPH_COUNT_SOURCE_INTERVAL = "chronos_interval_visible_predicate"
_GRAPH_COUNT_SOURCE_BRANCH_QUERY = "branch_query_effective_projection"
_GRAPH_COUNT_SOURCE_PREVIOUS_PLUS_DELTA = "previous_counts_plus_delta"
_GRAPH_COUNT_SOURCE_UNKNOWN = "unknown"


class _GraphCountChannel(threading.local):
    """Per-thread side channel for one graph count."""

    source: str | None = None
    progress: Callable[[str, int, float], None] | None = None
    # Why the bounded route was not taken, if it was not. A route that
    # declines without saying why is the silent fallback this repository
    # refuses; the reason reaches the cycle receipt.
    declined: str | None = None


# The errors a Chronos branch read can raise that are a reason to count the
# other way rather than a defect: the database refusing or failing the
# statement, the branch layer refusing it, and a row that is not the shape the
# registry is documented to hold. Anything else -- an AttributeError, a
# TypeError from our own code -- is a bug in this file and must propagate,
# which is why `Exception` is NOT caught at these sites.
_GRAPH_COUNT_READ_ERRORS = (
    psycopg.Error,
    BranchingError,
    KeyError,
    IndexError,
)


_GRAPH_COUNT = _GraphCountChannel()
# A first-publish count whose backing tables together hold no more than this
# many heap bytes is not announced on stderr: a scan of 64 MiB is cheap, and
# every freshly created database falls under it. Measured on the #1456 branch,
# 2,000,000 rows filled 110,768 pages (about 865 MiB).
_FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES = 64 * 1024 * 1024
# A first-publish table count that takes longer than this is printed even on
# a small branch: something other than size (a lock wait, slow storage) made
# it slow, and the operator watching the terminal should see where it went.
_FIRST_PUBLISH_COUNT_SLOW_SECONDS = 5.0
# The heap-size read takes the same share lock as any read. A publish blocked
# there must not be silent, so the read gives up after this long and the count
# is announced as if its size were unknown.
_BACKING_HEAP_BYTES_LOCK_TIMEOUT = "2s"
# "No mapping was passed", distinct from a passed mapping of None (resolution
# declined), so a declined resolution is not attempted -- and warned -- twice.
_MAPPING_NOT_GIVEN: Any = object()


def _first_publish_count_is_announced(
    heap_bytes: Mapping[str, int] | None,
) -> bool:
    """True unless every backing table is measured and at most the threshold."""
    return heap_bytes is None or any(
        size > _FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES
        for size in heap_bytes.values()
    )
# Chronos generates its interval backing-table names; this refuses anything
# that is not a bare identifier rather than quoting an arbitrary string into
# DDL-shaped SQL.
_CHRONOS_PHYSICAL_TABLE_NAME = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")
_CHRONOS_GRAPH_TABLES = ("okg.graph_nodes", "okg.graph_edges")
# The visible-row predicate the Chronos interval backend itself applies when it
# rewrites a query against a registered table (`_visible_subquery`). Counting
# with the same predicate against the same physical table is the same number by
# construction; what it avoids is the backend's all-columns projection, which
# makes an index-only plan impossible and forces a heap scan.
_CHRONOS_VISIBLE_COUNT_SQL = (
    'SELECT count(*) AS n FROM "{physical}" '
    "WHERE live_lo <= :_chronos_branch_point "
    "AND :_chronos_branch_point < live_hi "
    "AND deleted = FALSE"
)
_ChronosDirtyScope = tuple[
    dict[str, list[str]],
    dict[str, list[int]],
    bool,
    dict[str, dict[str, list[str]]] | None,
    dict[str, list[dict[str, Any]]],
    dict[str, dict[int, list[str]]] | None,
]


def _merge_keyed_dirty_scope(
    scope: _ChronosDirtyScope,
    keyed: _ChronosDirtyScope,
    *,
    node_ids: Iterable[str],
    edge_ids: Iterable[int],
) -> _ChronosDirtyScope | None:
    """``scope`` with every key in ``node_ids`` / ``edge_ids`` replaced by
    what the keyed read ``keyed`` holds for it (issue #2941).

    Each key leaves every subtype and type list first, so a key the
    publisher's own write put back to its previous state leaves the scope,
    and one whose subtype changed moves. None when either scope carries
    the parts a segment scan does not fill (they are not merged; the
    caller rescans instead).
    """
    if (
        scope[4] or scope[5] is not None or keyed[4] or keyed[5] is not None
        or scope[3] is None or keyed[3] is None
        or scope[2] != keyed[2]
    ):
        return None
    drop_nodes = {str(i) for i in node_ids}
    drop_edges = {int(i) for i in edge_ids}
    nodes: dict[str, list[str]] = {}
    for subtype, ids in scope[0].items():
        kept = [i for i in ids if i not in drop_nodes]
        if kept:
            nodes[subtype] = kept
    for subtype, ids in keyed[0].items():
        nodes.setdefault(subtype, []).extend(ids)
    changes: dict[str, dict[str, list[str]]] = {}
    for subtype, by_node in scope[3].items():
        kept_changes = {
            node_id: fields for node_id, fields in by_node.items()
            if node_id not in drop_nodes
        }
        if kept_changes:
            changes[subtype] = kept_changes
    for subtype, by_node in keyed[3].items():
        merged_changes = changes.setdefault(subtype, {})
        for node_id, fields in by_node.items():
            merged_changes[node_id] = fields
    edges: dict[str, list[int]] = {}
    for edge_type, ids in scope[1].items():
        kept_edges = [i for i in ids if int(i) not in drop_edges]
        if kept_edges:
            edges[edge_type] = kept_edges
    for edge_type, ids in keyed[1].items():
        edges.setdefault(edge_type, []).extend(ids)
    return (nodes, edges, scope[2], changes, {}, None)


def _dirty_scope_normalized(scope: _ChronosDirtyScope) -> tuple[Any, ...]:
    """A dirty scope with its lists sorted, for comparing two reads."""
    return (
        {k: sorted(v) for k, v in scope[0].items() if v},
        {k: sorted(int(i) for i in v) for k, v in scope[1].items() if v},
        scope[2],
        {
            k: {node: sorted(fields) for node, fields in v.items()}
            for k, v in (scope[3] or {}).items() if v
        },
    )


_ChronosDeltaCounts = dict[str, int]
_CHRONOS_DELTA_COUNT_KEYS = (
    "nodes_added",
    "nodes_retracted",
    "nodes_updated",
    "edges_added",
    "edges_retracted",
    "edges_updated",
)


def _build_invariant_summary(phase_b: PhaseBResult) -> dict[str, Any]:
    """Produce the `graph_generations.invariant_summary` JSONB
    from a PhaseBResult.

    Shape:
      {
        "cross_source": {
            "total": int, "passed": int, "failed": int,
            "warns": int, "timed_out": int,
            "fired_warns": [{name, violator_count, sample}, ...],
        },
        "source_local": {
            "<source>": {total, passed, failed, warns, timed_out,
                         fired_warns},
            ...
        },
        "total_duration_ms": int,
        "total_budget_exceeded": bool,
      }

    Error-severity failures on the published path are impossible
    (they'd have blocked), so this summary is silent on them.
    Warn-severity fires + timeouts are recorded for operator
    visibility.
    """
    if phase_b.invariant_summary_override is not None:
        return dict(phase_b.invariant_summary_override)

    def _bucket(
        results: list,  # list[GateInvariantResult]
    ) -> dict[str, Any]:
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = sum(
            1 for r in results
            if r.severity != "warn" and not r.passed
        )
        warns = sum(
            1 for r in results
            if r.severity == "warn" and not r.passed
        )
        timed_out = sum(1 for r in results if r.timed_out)
        fired_warns = [
            {
                "name": r.name,
                "violator_count": r.violator_count,
                "sample": r.violators[:5],
            }
            for r in results
            if r.severity == "warn" and not r.passed
        ]
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "warns": warns,
            "timed_out": timed_out,
            "fired_warns": fired_warns,
        }

    summary = {
        "cross_source": _bucket(phase_b.cross_source),
        "source_local": {
            src: _bucket(results)
            for src, results in phase_b.source_local.items()
        },
        "total_duration_ms": phase_b.total_duration_ms,
        "total_budget_exceeded": phase_b.total_budget_exceeded,
    }
    if phase_b.substrate_integrity:
        substrate_integrity = phase_b.substrate_integrity
        failed = [
            r for r in substrate_integrity
            if r.severity == "error" and not r.passed
        ]
        narrowing_result = next(
            (
                r for r in substrate_integrity
                if r.name == "substrate_missing_active_narrowings"
            ),
            None,
        )
        summary["substrate_integrity"] = {
            "passed": not failed,
            "total": len(substrate_integrity),
            "failed": len(failed),
            "checks": {
                r.name: dict(r.metadata)
                for r in substrate_integrity
            },
            "narrowing_validation": (
                narrowing_result.metadata.get("narrowing_validation", {})
                if narrowing_result is not None
                else {}
            ),
        }
    if phase_b.skipped_invariants:
        # Issue #3624: a skipped invariant neither passed nor failed; say so.
        summary["skipped_invariants"] = [
            dict(item) for item in phase_b.skipped_invariants
        ]
    if phase_b.unchecked_source_requirements:
        summary["unchecked_source_requirements"] = list(
            phase_b.unchecked_source_requirements
        )
    if phase_b.gate_cache_key is not None:
        summary["gate_cache_key"] = phase_b.gate_cache_key
    if phase_b.consistency_gates:
        blocked = [
            item for item in phase_b.consistency_gates
            if item.get("blocks_publish")
        ]
        summary["consistency_gates"] = {
            "total": len(phase_b.consistency_gates),
            "passed": len(phase_b.consistency_gates) - len(blocked),
            "blocked": len(blocked),
            "results": phase_b.consistency_gates,
        }
    return summary


def _count_stamp_self_heal_every() -> int:
    """Every how many publishes the count-stamp self-heal forces a full cycle."""
    try:
        return max(
            1, int(os.environ.get("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "20"))
        )
    except (TypeError, ValueError):
        return 20


#: Issue 2941 fix 5: the fewest rows written since the last checkpoint for
#: which an incremental cycle hashes them before the publish lock. Below it
#: the materialize stages them in a small TEMP table and hashes them serially
#: (_PARALLEL_HASH_MIN_ROWS in attributes.py); a pre-lock pass would cost
#: more than it saves.
_PRELOCK_CHANGED_MIN_ROWS = 20_000


class _PrelockHashPlan(NamedTuple):
    """Whether, and over which rows, to hash attributes before the lock.

    ``code`` is a stable machine-readable reason; ``reason`` explains it.
    ``scope`` is "full" (every visible row) or "changed" (the rows written in
    ``segment_ids``, the writer segments since the last checkpoint).
    ``changed_rows`` is the bounded count of row versions written in those
    segments (at most _PRELOCK_CHANGED_MIN_ROWS), when it was counted.
    """

    run: bool
    reason: str
    code: str
    scope: str | None = None
    segment_ids: tuple[int, ...] = ()
    changed_rows: int | None = None


@contextmanager
def _publisher_timed_stage(
    timings_ms: dict[str, Any],
    name: str,
) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        ended = time.perf_counter()
        elapsed = max(0.0, (ended - started) * 1000.0)
        timings_ms[name] = round(timings_ms.get(name, 0.0) + elapsed, 3)
        timing_events = timings_ms.get(_PUBLISHER_TIMING_EVENTS_KEY)
        if isinstance(timing_events, list):
            timing_events.append({
                "name": name,
                "started_perf": started,
                "ended_perf": ended,
            })


@contextmanager
def _attribute_integrity_transition_guard(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
) -> Iterator[dict[str, Any]]:
    """Clear publisher-only transition authority on every cycle exit."""
    transition: dict[str, Any] = {}
    try:
        yield transition
    finally:
        transition_active = bool(transition.get("transition_backend_bound"))
        if transition_active:
            from okg.substrate.attribute_integrity import (
                clear_attribute_integrity_transition,
            )

            primary_error = sys.exc_info()[0] is not None
            try:
                clear_attribute_integrity_transition(
                    conn,
                    deployment_name=deployment_name,
                    branch=branch,
                    namespace="default",
                    transition_active=True,
                )
            except Exception:  # noqa: BLE001 - never replace the cycle failure.
                logging.getLogger(__name__).exception(
                    "attribute integrity transition cleanup failed"
                )
                if not primary_error:
                    raise


def _publisher_round_ms(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return None


def _publisher_slowest_phase(
    flat_ms: Mapping[str, float],
) -> dict[str, Any] | None:
    """Identify the single slowest recorded phase of a publish cycle.

    Issue #1291: a receipt that says only "the lock was held 881s" does not
    tell an operator what to fix. The lock bookkeeping itself (``lock.*``), the
    whole-cycle total, and ``stage.locked_connection`` -- the WAIT before the
    hold begins, already reported as the lock wait -- are excluded so the
    answer names a phase that ran inside the hold. So are the passes that
    run before the lock (issue 2941: ``stage.attribute_hash_prelock`` and
    ``stage.prelock_enrich`` with its ``prelock_enrich.*`` counters).
    """
    candidates = {
        name: value
        for name, value in flat_ms.items()
        if not name.startswith("lock.")
        and name not in {
            "stage.total",
            "stage.locked_connection",
            "stage.attribute_hash_prelock",
            "stage.prelock_enrich",
        }
        and not name.startswith("stage.prelock_enrich.")
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
    }
    if not candidates:
        return None
    name, value = max(candidates.items(), key=lambda item: (item[1], item[0]))
    return {"name": name, "elapsed_ms": round(float(value), 3)}


def _publisher_numeric_timings(value: Any) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, float] = {}
    for key, raw in value.items():
        number = _publisher_round_ms(raw)
        if number is not None:
            out[str(key)] = number
    return out


def _chronos_backend_timings(store: Any) -> tuple[dict[str, float], dict[str, float]]:
    backend = getattr(getattr(store, "context", None), "_backend", None)
    operation_timings: dict[str, float] = {}
    transaction_timings: dict[str, float] = {}
    pop_operation_timings = getattr(
        backend,
        "pop_last_operation_timings_ms",
        None,
    )
    if callable(pop_operation_timings):
        operation_timings = _publisher_numeric_timings(pop_operation_timings())
    pop_transaction_timings = getattr(
        backend,
        "pop_last_transaction_timings_ms",
        None,
    )
    if callable(pop_transaction_timings):
        transaction_timings = _publisher_numeric_timings(
            pop_transaction_timings()
        )
    return operation_timings, transaction_timings


def _add_prefixed_timings(
    timings_ms: dict[str, Any],
    prefix: str,
    values: Mapping[str, Any],
) -> None:
    timing_events = timings_ms.get(_PUBLISHER_TIMING_EVENTS_KEY)
    for key, value in _publisher_numeric_timings(values).items():
        timing_key = f"{prefix}.{key}"
        timing_value = round(float(value), 3)
        timings_ms[timing_key] = timing_value
        if isinstance(timing_events, list):
            ended = time.perf_counter()
            timing_events.append({
                "name": timing_key,
                "started_perf": max(0.0, ended - (timing_value / 1000.0)),
                "ended_perf": ended,
            })


def _timing_label_segment(value: Any) -> str:
    normalized = "".join(
        char if char.isalnum() or char in {"_", "-"} else "_"
        for char in str(value or "").strip()
    ).strip("_")
    return normalized or "unnamed"


def _add_named_timing_details(
    timings_ms: dict[str, Any],
    *,
    prefix: str,
    name: Any,
    values: Mapping[str, Any] | None,
) -> None:
    if not isinstance(values, Mapping):
        return
    duration_values = {
        key: value
        for key, value in _publisher_numeric_timings(values).items()
        if _publisher_timing_detail_is_duration(key)
    }
    if not duration_values:
        return
    _add_prefixed_timings(
        timings_ms,
        f"{prefix}.{_timing_label_segment(name)}",
        duration_values,
    )


def _publisher_timing_detail_is_duration(key: str) -> bool:
    normalized = str(key).strip().lower()
    segments = [part for part in normalized.split(".") if part]
    if (
        not normalized
        or normalized == "total_instrumented"
        or "total" in segments
    ):
        return False
    if normalized.endswith("_ms") or normalized.endswith(".ms"):
        return True
    flag_or_count_tokens = (
        "count",
        "skipped",
        "trusted",
        "failed",
        "llm_calls",
        "empty_carry_forward",
    )
    return not any(token in normalized for token in flag_or_count_tokens)


def _add_native_view_timing_details(
    timings_ms: dict[str, Any],
    view_outcomes: Any,
) -> None:
    for index, outcome in enumerate(view_outcomes or (), start=1):
        view_id = getattr(outcome, "view_id", f"native_view_{index}")
        _add_named_timing_details(
            timings_ms,
            prefix="native_views",
            name=view_id,
            values=getattr(outcome, "timings_ms", None),
        )


def _current_dbos_workflow_id() -> str | None:
    dbos_module = sys.modules.get("dbos")
    if dbos_module is None:
        return None
    DBOS = getattr(dbos_module, "DBOS", None)
    if DBOS is None:
        return None
    workflow_id = getattr(DBOS, "workflow_id", None)
    if callable(workflow_id):
        try:
            workflow_id = workflow_id()
        except Exception:
            logging.getLogger(__name__).exception(
                "failed to read current DBOS workflow id"
            )
            raise
    if isinstance(workflow_id, str) and workflow_id:
        return workflow_id
    return None


def generation_published_channel(deployment_name: str) -> str:
    """Return the Postgres NOTIFY channel for a deployment's
    `status='published'` transitions.

    Per ``publish-generation-events-via-dbos-streams``
    (deepen-dbos-integration Phase 1). Existing short deployment
    names keep the channel format
    ``okg_generation_<deployment_name>_published``. Long or
    path-style refs are mapped to a deterministic hash because
    Postgres rejects channel names beyond 63 bytes.

    Consumers ``LISTEN`` on this channel; Publisher.commit() emits
    the NOTIFY inside the same transaction as the
    ``status='published'`` UPDATE so the signal is atomic with the
    row's commit.
    """
    raw = f"okg_generation_{deployment_name}_published"
    if _notify_channel_safe(raw, deployment_name):
        return raw
    digest = hashlib.sha256(deployment_name.encode("utf-8")).hexdigest()[:16]
    return f"okg_generation_{digest}_published"


def _notify_channel_safe(channel: str, deployment_name: str) -> bool:
    if len(channel.encode("utf-8")) > 63:
        return False
    if any(ch in deployment_name for ch in "/\\\0\r\n\t "):
        return False
    return True


def _emit_phase_event(name: str, payload: dict[str, Any]) -> None:
    """Emit a DBOS workflow event marking a publisher-cycle phase
    boundary.

    Per ``decompose-publisher-cycle-into-dbos-steps``
    (deepen-dbos-integration Phase 1): operators read per-phase
    progress via ``okg runtime debug-workflow <id>`` which reads
    ``dbos.workflow_events`` rows emitted here.

    Safe to call outside a DBOS workflow context: the helper swallows
    every exception ``DBOS.set_event`` might raise (no workflow
    context, DBOS singleton not initialized, etc.) so the publisher
    remains usable from non-workflow callers (e.g. unit tests, the
    `--inline` CLI path).
    """
    dbos_module = sys.modules.get("dbos")
    if dbos_module is None:
        return
    try:
        DBOS = getattr(dbos_module, "DBOS", None)
        if DBOS is None:
            return
        # Emit only inside an active DBOS workflow context. Outside a
        # workflow ``set_event`` raises "no current workflow" (caught
        # below), so pre-checking ``workflow_id`` avoids a guaranteed
        # no-op call. This is the gating contract pinned by
        # tests/substrate/test_publisher_phase_events.py: skip when
        # ``workflow_id`` is None, emit when it is set.
        if getattr(DBOS, "workflow_id", None) is None:
            return
        DBOS.set_event(name, payload)
    except Exception as exc:  # noqa: BLE001
        # Don't let a missing DBOS context derail the cycle. The
        # publisher's correctness lives in node_facts / edge_facts /
        # graph_generations; observability events are best-effort.
        logging.getLogger(__name__).debug(
            "phase event %r not emitted (no workflow context?): %s",
            name, exc,
        )


def _emit_phase_event_with_timing(
    timings_ms: dict[str, float],
    name: str,
    payload: dict[str, Any],
) -> None:
    with _publisher_timed_stage(timings_ms, f"phase_event.{name}"):
        if _publisher_phase_events_batched():
            batch = timings_ms.setdefault(
                _PHASE_EVENT_BATCH_CURSOR_KEY,
                [],
            )
            if isinstance(batch, list):
                batch.append({"name": name, "payload": dict(payload)})
            else:
                batch = [{"name": name, "payload": dict(payload)}]
                timings_ms[_PHASE_EVENT_BATCH_CURSOR_KEY] = batch
            if name == "cycle_complete":
                _flush_phase_event_batch(timings_ms, batch)
            return
        _emit_phase_event(name, payload)


def _publisher_phase_events_batched() -> bool:
    mode = os.environ.get("OKG_PUBLISH_PHASE_EVENTS", "batch")
    return mode.strip().lower() not in {"immediate", "legacy", "direct"}


def _flush_pending_phase_events(timings_ms: dict[str, Any]) -> None:
    """Flush any still-batched phase events.

    P3 (specify-publish-commit-protocol): the per-cycle phase-event batch
    lives in ``timings_ms`` and is normally flushed only on
    ``cycle_complete``. A mid-cycle exception never reaches that flush, so
    the timeline of a FAILED cycle -- exactly when it is most needed for
    diagnosis -- is discarded. This helper drains whatever accumulated so
    far; it is idempotent (``_flush_phase_event_batch`` pops the cursor
    key) so a normal completion, which already flushed, makes it a no-op.
    """
    batch = timings_ms.get(_PHASE_EVENT_BATCH_CURSOR_KEY)
    if isinstance(batch, list) and batch:
        _flush_phase_event_batch(timings_ms, batch)
    else:
        timings_ms.pop(_PHASE_EVENT_BATCH_CURSOR_KEY, None)


@contextmanager
def _phase_event_failure_flush(
    timings_ms: dict[str, Any],
) -> Iterator[None]:
    """Guard the publish cycle so batched phase events survive a raise.

    On a normal exit the batch has already been flushed+popped at
    ``cycle_complete`` and this is a no-op. On ANY exception propagating
    through the guarded body it flushes the pending batch (best-effort,
    like every phase-event emit) before re-raising, so a failed cycle's
    partial timeline is durably emitted.
    """
    try:
        yield
    except BaseException:
        try:
            _flush_pending_phase_events(timings_ms)
        except Exception:  # noqa: BLE001 -- observability is best-effort
            logging.getLogger(__name__).debug(
                "phase-event failure flush did not complete",
                exc_info=True,
            )
        raise


def _announce_long_publish_phase(
    message: str,
    *,
    deployment_name: str,
    branch_name: str,
) -> None:
    """Tell the operator a multi-minute publish phase has started.

    Issue #1456: the durable phase-event stream is the right record, but
    `_emit_phase_event` is a no-op outside a DBOS workflow, which is exactly the
    `okg run --once --inline` path the incident used. `okg run` has no
    `--progress` flag either, so an INFO record is dropped by the default
    handler. A single stderr line is therefore the only thing that actually
    reaches an operator watching an hour-long first publish; stdout is left
    untouched so `--json` consumers are unaffected.
    """
    logging.getLogger(__name__).info(
        "publisher %s branch=%s: %s", deployment_name, branch_name, message,
    )
    try:
        print(
            f"okg publisher {deployment_name} branch={branch_name}: {message}",
            file=sys.stderr,
            flush=True,
        )
    except Exception:  # noqa: BLE001 - a closed stderr must never fail a publish
        pass


def _flush_phase_event_batch(
    timings_ms: dict[str, float],
    batch: list[dict[str, Any]],
) -> None:
    timings_ms.pop(_PHASE_EVENT_BATCH_CURSOR_KEY, None)
    if not batch:
        return
    with _publisher_timed_stage(timings_ms, "phase_event.batch_flush"):
        _emit_phase_event(
            _PHASE_EVENT_BATCH_EVENT_KEY,
            {
                "schema": _PHASE_EVENT_BATCH_SCHEMA,
                "events": list(batch),
                "event_count": len(batch),
            },
        )


def _advisory_keys(deployment_name: str) -> tuple[int, int]:
    """Derive a 64-bit advisory-lock key for a deployment.

    Uses MD5 of `okg.publish.<deployment>` and splits the high 64
    bits into two signed-int4 halves for `pg_try_advisory_lock(int,
    int)`. This widens the keyspace from 32 bits (Postgres `hashtext`)
    to 64 bits, which keeps collision risk negligible at any
    reasonable deployment count. Per the M1 design review.

    The docs/runbooks/troubleshooting/publisher-lock.md SQL needs to use the
    same derivation; updated there too.
    """
    return publisher_publish_advisory_keys(deployment_name)


def derived_admission_record_counts(
    status: str,
    emitted_record_count: int,
    *,
    record_kind: str,
    gating_mode: str,
) -> dict[str, Any]:
    """The admitted/rejected split for one derived emission (issue #1331).

    An admission envelope recorded WHAT was decided but never HOW MUCH, so a
    published enricher that derived nothing and one whose output was discarded
    read identically. Admission is whole-emission (the derived-fact loader
    gates edges by the envelope fact-range allow-list and nodes by a per-run
    deny-list), so a non-accepted verdict refuses every emitted record --
    stated here rather than left to be inferred.

    ``admitted``/``rejected`` describe the ADMISSION DECISION, which is not
    the same as what reaches the served graph: under Nomos ``audit`` or
    ``off`` the derived-fact loader projects a refused producer's output
    anyway (see ``_load_unapplied_derived_facts``). ``gating_mode`` records
    which of the two a reader is looking at, so a refusal that withheld
    nothing is never presented as output that was dropped.
    """
    emitted = int(emitted_record_count)
    admitted = emitted if status == "accepted" else 0
    return {
        "admission_record_counts": {
            "record_kind": record_kind,
            "emitted": emitted,
            "admitted": admitted,
            "rejected": emitted - admitted,
            "gating_mode": str(gating_mode),
            "refusal_withholds_output": str(gating_mode) == "enforce",
        },
    }


def derived_gating_mode(deployment_name: str) -> str:
    """The runtime enforcement mode governing derived-fact projection.

    A thin read-only lookup, deliberately its own function. The admission
    paths that record this on an envelope carry state-transition calls, and
    the DBOS boundary census classifies any function that both mentions the
    policy runtime and transitions state as a state-applying boundary. These
    callers apply nothing here -- they read a mode -- so the read lives out
    here rather than widening their declared boundary category with a lookup
    that changes nothing (fast-lane census
    `test_epoch_binding_registry_is_complete`).
    """
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    return deployment_nomos_enforcement_mode(deployment_name)


def _infer_derived_producer_kind(producer_id: str) -> str:
    """Derive the producer_kind from a derived producer-id prefix.

    Used as a fallback when ``authority.producer_kind`` is omitted from
    a deployment manifest. Lens-promotion (``_lens:``) was introduced
    by ``add-lens-promotion-admission``; the substrate keeps the
    fallback explicit so a misspelled prefix surfaces as an unknown
    producer_kind validation error in
    :func:`okg.substrate.producer_authority.materialize_producer_profile`
    rather than silently materializing the wrong kind.
    """

    if producer_id.startswith("_extractor:"):
        return "extractor"
    if producer_id.startswith("_lens:"):
        return "lens"
    return "enricher"


@dataclass(frozen=True)
class NativeViewRefreshPolicy:
    """Runtime knobs for coalescing native-view refreshes."""

    min_refresh_interval_ms: int = 0
    materialization_delay_ms: int = 0

    @property
    def enabled(self) -> bool:
        return (
            self.min_refresh_interval_ms > 0
            or self.materialization_delay_ms > 0
        )


@dataclass(frozen=True)
class _DerivedFactBatch:
    """One staged derived producer run awaiting FactBatch application."""

    producer_id: str
    producer_kind: str
    run_id: uuid.UUID
    facts: tuple[Any, ...]


@dataclass
class _CallerVerifiedPublishTransaction:
    """Own the one commit for an explicitly verified publish cycle."""

    conn: psycopg.Connection
    verifier: Callable[[psycopg.Connection, str], None]
    store: Any | None = None
    finished: bool = False

    def begin(self) -> None:
        # ``autocommit=False`` only configures the next statement; it does not
        # open a PostgreSQL transaction. Start it before any publisher helper
        # can create a psycopg transaction context. Otherwise the first such
        # context is top-level and commits on exit instead of being the nested
        # savepoint the atomic path requires.
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")

    def require_active(self, *, stage: str) -> None:
        if self.conn.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
            raise SubstrateError(
                "caller-owned publication transaction ended before commit",
                data={"stage": stage},
            )

    def bind_store(self, store: Any) -> None:
        self.store = store

    def verify_and_commit(self, generation_id: str) -> None:
        self.require_active(stage="precommit_verifier")
        self.verifier(self.conn, generation_id)
        self.conn.commit()
        self.finished = True
        if self.store is not None:
            try:
                self.store.context.finalize_external_transaction(committed=True)
            except Exception:  # pragma: no cover - maintenance hook is best-effort
                logging.getLogger(__name__).exception(
                    "Chronos post-commit finalization failed for generation %s",
                    generation_id,
                )

    def rollback(self) -> None:
        if self.finished:
            return
        try:
            self.conn.rollback()
        finally:
            self.finished = True
            if self.store is not None:
                try:
                    self.store.context.finalize_external_transaction(
                        committed=False,
                    )
                except Exception:  # pragma: no cover - preserve original failure
                    logging.getLogger(__name__).exception(
                        "Chronos rollback finalization failed"
                    )


# Issue #2865. A publish is gated against a base generation: its own latest
# published generation, or for a fork's first publish the fork's recorded base
# on default. Its dirty set is every row written SINCE that base, which is not
# always the branch's current segment:
#   * a branch made from another branch (the merge-back gate branch is made
#     from the staging branch) inherits rows its parent wrote after the base,
#     under the parent's writer_segment_id;
#   * creating a branch moves the source branch to a new segment, so rows the
#     source wrote before the fork (and after its base) sit in the old one;
#   * a BLOCKED generation cuts a checkpoint, which also moves the branch to a
#     new segment, so the rows it refused sit in the old one.
# The segments written since the base are the current segment's ancestors up
# to, not including, the first one that is an ancestor of the base. Chronos
# nests a child's interval inside its parent's and keeps siblings disjoint, so
# "ancestor of the base, or the base itself" is "interval contains the base's
# interval"; the walk stops there instead of walking both full chains, which
# on a long-lived branch are one level per checkpoint. With no fork and no
# blocked generation since the base the result is just the current segment.
# Parameters:
# (base_segment_id, branch_name).
_CHRONOS_SEGMENTS_WRITTEN_SINCE_BASE_SQL = """
    WITH RECURSIVE base AS (
      SELECT live_lo, live_hi
        FROM _chronos_branch_interval_segments
       WHERE segment_id = %s
    ), walk AS (
      SELECT s.segment_id, s.parent_segment_id,
             (s.live_lo <= base.live_lo AND base.live_hi <= s.live_hi) AS shared,
             0 AS depth
        FROM _chronos_branch_interval_branches b
        JOIN _chronos_branch_interval_segments s
          ON s.segment_id = b.current_segment_id
       CROSS JOIN base
       WHERE b.branch_id = %s
      UNION ALL
      SELECT p.segment_id, p.parent_segment_id,
             (p.live_lo <= base.live_lo AND base.live_hi <= p.live_hi) AS shared,
             w.depth + 1
        FROM walk w
        JOIN _chronos_branch_interval_segments p
          ON p.segment_id = w.parent_segment_id
       CROSS JOIN base
       WHERE NOT w.shared
    )
    SELECT segment_id, shared FROM walk ORDER BY depth
"""

# The segments a branch wrote since its last checkpoint of any status: the
# current segment and, while the walk climbs through a fork split (the child's
# parent is a fork_base segment), the segment that fork split. A checkpoint
# split makes the continuation a direct child of the sealed segment, which
# ends the walk. fork_base segments hold no rows and are left out.
# Parameters: (branch_name,).
_CHRONOS_SEGMENTS_SINCE_LAST_CHECKPOINT_SQL = """
    WITH RECURSIVE walk AS (
      SELECT s.segment_id, s.parent_segment_id, s.segment_kind
        FROM _chronos_branch_interval_branches b
        JOIN _chronos_branch_interval_segments s
          ON s.segment_id = b.current_segment_id
       WHERE b.branch_id = %s
      UNION ALL
      SELECT p.segment_id, p.parent_segment_id, p.segment_kind
        FROM walk w
        JOIN _chronos_branch_interval_segments p
          ON p.segment_id = w.parent_segment_id
       WHERE p.segment_kind = 'fork_base' OR w.segment_kind = 'fork_base'
          -- The run-base checkpoint of a source run (issue #2993) splits the
          -- segment too, but seals nothing: it is never a generation, and it
          -- is deleted when the run ends. Walk through any checkpoint split
          -- that no generation checkpoint row still names.
          OR (
            w.segment_kind = 'mutable' AND p.segment_kind = 'mutable'
            AND NOT EXISTS (
              SELECT 1
                FROM _chronos_branch_interval_segments snap
                JOIN _chronos_branch_interval_checkpoints c
                  ON c.segment_id = snap.segment_id
               WHERE snap.parent_segment_id = p.segment_id
                 AND snap.segment_kind = 'checkpoint'
                 AND c.checkpoint_id NOT LIKE 'okg-run-base:%%'
            )
          )
    )
    SELECT segment_id FROM walk WHERE segment_kind <> 'fork_base'
"""

# The segments reachable from the branch head through run-base splits only
# (issue #2993): the current segment and each segment a source run's run-base
# checkpoint split off, up to the first split a generation checkpoint row
# names, and never through a fork. Rows in them were written by this
# branch's own writers since that split and sealed by no generation, so a
# caller's changed-row hint may stand for them once it lists their keys.
# Parameters: (branch_name,).
_CHRONOS_SEGMENTS_THROUGH_RUN_BASES_SQL = """
    WITH RECURSIVE walk AS (
      SELECT s.segment_id, s.parent_segment_id, s.segment_kind
        FROM _chronos_branch_interval_branches b
        JOIN _chronos_branch_interval_segments s
          ON s.segment_id = b.current_segment_id
       WHERE b.branch_id = %s
      UNION ALL
      SELECT p.segment_id, p.parent_segment_id, p.segment_kind
        FROM walk w
        JOIN _chronos_branch_interval_segments p
          ON p.segment_id = w.parent_segment_id
       WHERE w.segment_kind = 'mutable' AND p.segment_kind = 'mutable'
         AND NOT EXISTS (
           SELECT 1
             FROM _chronos_branch_interval_segments snap
             JOIN _chronos_branch_interval_checkpoints c
               ON c.segment_id = snap.segment_id
            WHERE snap.parent_segment_id = p.segment_id
              AND snap.segment_kind = 'checkpoint'
              AND c.checkpoint_id NOT LIKE 'okg-run-base:%%'
         )
    )
    SELECT segment_id FROM walk
"""

# With the since-base seed, a key may have versions in several of those
# segments (a child's write splices its parent's version around the child's
# interval), so the seed keeps only the version visible at the current branch
# point. The segment list is passed as a constant array so the planner probes
# the (writer_segment_id, key) index once per segment.
_CHRONOS_WRITTEN_SINCE_BASE_CTES = """
            WITH current_branch AS (
              SELECT s.branch_point
                FROM _chronos_branch_interval_branches b
                JOIN _chronos_branch_interval_segments s
                  ON s.segment_id = b.current_segment_id
               WHERE b.branch_id = %s
            ), previous_point AS (
              SELECT branch_point
                FROM _chronos_branch_interval_segments
               WHERE segment_id = %s
            )"""

_CHRONOS_CURRENT_SEGMENT_CTES = """
            WITH current_branch AS (
              SELECT current_segment_id
                FROM _chronos_branch_interval_branches
               WHERE branch_id = %s
            ), previous_point AS (
              SELECT branch_point
                FROM _chronos_branch_interval_segments
               WHERE segment_id = %s
            )"""


def _chronos_dirty_seed_filter(alias: str, *, since_base: bool) -> str:
    if not since_base:
        return (
            f"JOIN current_branch b\n"
            f"                  ON {alias}.writer_segment_id = b.current_segment_id"
        )
    return (
        f"WHERE {alias}.writer_segment_id = ANY(%s)\n"
        f"                 AND {alias}.live_lo <= (SELECT branch_point FROM current_branch)\n"
        f"                 AND (SELECT branch_point FROM current_branch) < {alias}.live_hi"
    )


# Attributes that are not a JSON object read as an empty object, as
# `dict(attrs or {})` read null; the scan never fails on them.
_DIRTY_ATTRS = "CASE WHEN jsonb_typeof({side}.attrs) = 'object' THEN {side}.attrs ELSE '{{}}'::jsonb END"
_D_ATTRS = _DIRTY_ATTRS.format(side="d")
_P_ATTRS = _DIRTY_ATTRS.format(side="p")
_CHRONOS_DIRTY_NODE_FIELDS_SQL = (
    """
                   CASE
                     WHEN p.subtype IS NULL THEN
                       ARRAY['__new__'] || ARRAY(SELECT jsonb_object_keys("""
    + _D_ATTRS
    + """))
                     WHEN d.deleted IS DISTINCT FROM p.deleted THEN
                       CASE WHEN d.deleted THEN
                         ARRAY['__deleted__'] || ARRAY(SELECT jsonb_object_keys("""
    + _P_ATTRS
    + """))
                       ELSE
                         ARRAY['__new__'] || ARRAY(SELECT jsonb_object_keys("""
    + _D_ATTRS
    + """))
                       END
                     ELSE
                       (CASE WHEN d.subtype IS DISTINCT FROM p.subtype
                             THEN ARRAY['__subtype__'] ELSE ARRAY[]::text[] END)
                       || (CASE WHEN COALESCE(d.source, '')
                                     IS DISTINCT FROM COALESCE(p.source, '')
                                THEN ARRAY['__source__'] ELSE ARRAY[]::text[] END)
                       -- Each side read once (jsonb_each), not once per key:
                       -- a large attribute document is stored compressed.
                       || ARRAY(
                            SELECT COALESCE(a.key, b.key)
                              FROM jsonb_each("""
    + _D_ATTRS
    + """) a
                              FULL JOIN jsonb_each("""
    + _P_ATTRS
    + """) b
                                ON a.key = b.key
                             WHERE COALESCE(a.value, 'null'::jsonb)
                                   IS DISTINCT FROM COALESCE(b.value, 'null'::jsonb)
                          )
                   END AS changed_fields"""
)


def _chronos_dirty_nodes_since_previous_sql(
    node_table: str,
    *,
    since_base: bool = False,
    keyed: bool = False,
    fields_in_sql: bool = False,
) -> sql.Composed:
    """Nodes written in the current segment that differ from the previous point.

    Each row also carries ``payload_changed`` (subtype or attrs differ, as
    jsonb), the update test the delta counts use (issue #2941).

    ``fields_in_sql`` returns ``(node_id, subtype, deleted,
    previous_subtype, previous_deleted, payload_changed, changed_fields)``
    instead of the two attribute documents: the changed-field list is
    computed in the database by the same rules the scan applied in Python
    (``__new__``, ``__deleted__``, ``__subtype__``, ``__source__``, and the
    attribute keys whose values differ, a JSON null equal to a missing
    key), with values compared as jsonb and attributes that are not a JSON
    object read as empty (issue #2941). Shipping both documents of every
    dirty node to Python was most of a scan's time.
    Parameters: ``(branch_name, previous_segment_id)``. Kept separate so a
    test can EXPLAIN the exact statement (issue #2556). With
    ``since_base`` the seed is every row written since the previous
    point along the branch ancestry, and the parameters are
    ``(branch_name, previous_segment_id, segment_ids)`` where
    ``segment_ids`` comes from ``_CHRONOS_SEGMENTS_WRITTEN_SINCE_BASE_SQL``
    (issue #2865). With ``keyed`` one more parameter, a list of node ids,
    limits the seed to those ids (issue #2941: re-reading only the keys a
    write of the publisher's own touched).
    """
    ctes = (
        _CHRONOS_WRITTEN_SINCE_BASE_CTES
        if since_base
        else _CHRONOS_CURRENT_SEGMENT_CTES
    )
    seed = _chronos_dirty_seed_filter("n", since_base=since_base)
    if keyed:
        seed += "\n                 AND n.node_id = ANY(%s)"
    return (
        sql.SQL(
            ctes
            + """, dirty AS (
              SELECT DISTINCT ON (n.node_id)
                     n.node_id, n.subtype, n.attrs, n.source,
                     COALESCE(n.deleted, FALSE) AS deleted
                FROM {node_table} n
                """
            + seed
            + """
               ORDER BY n.node_id, n.live_lo DESC
            )
            SELECT {select_list}
              FROM dirty d
              -- The row visible at the previous branch point,
              -- tombstones included. It holds the prior state,
              -- not liveness: see _chronos_segment_table_delta,
              -- where reading it as liveness was a real defect.
              -- The predicate below compares `d.deleted IS
              -- DISTINCT FROM p.deleted` rather than testing
              -- p.node_id for NULL, so a resurrection (previous
              -- deleted, now live) is reported as changed.
              --
              -- One primary-key lookup per dirty row (issue
              -- #2556). A set-at-a-time join here let the
              -- planner, on never-analyzed tables that
              -- estimate ~100 dirty rows for ~6,000, re-run
              -- the whole previous-state scan per dirty row.
              -- A LATERAL ... LIMIT 1 has no such plan.
              LEFT JOIN LATERAL (
                SELECT p.node_id, p.subtype, p.attrs, p.source,
                       COALESCE(p.deleted, FALSE) AS deleted
                  FROM {node_table} p
                 WHERE p.node_id = d.node_id
                   AND p.live_lo <= (SELECT branch_point FROM previous_point)
                   AND (SELECT branch_point FROM previous_point) < p.live_hi
                 ORDER BY p.live_lo DESC
                 LIMIT 1
              ) p ON TRUE
             WHERE p.node_id IS NULL
                OR d.deleted IS DISTINCT FROM p.deleted
                OR (
                     d.deleted = FALSE
                 AND (
                      d.subtype IS DISTINCT FROM p.subtype
                   OR d.attrs IS DISTINCT FROM p.attrs
                   OR d.source IS DISTINCT FROM p.source
                 )
                )
            """
        ).format(
            node_table=sql.Identifier(node_table),
            select_list=sql.SQL(
                """d.node_id, d.subtype, d.deleted,
                   p.subtype AS previous_subtype,
                   p.deleted AS previous_deleted,
                   (d.subtype IS DISTINCT FROM p.subtype
                    OR d.attrs IS DISTINCT FROM p.attrs) AS payload_changed,"""
                + _CHRONOS_DIRTY_NODE_FIELDS_SQL
                if fields_in_sql
                else """d.node_id, d.subtype, d.deleted, d.attrs, d.source,
                   p.subtype AS previous_subtype,
                   p.deleted AS previous_deleted,
                   p.attrs AS previous_attrs,
                   p.source AS previous_source,
                   (d.subtype IS DISTINCT FROM p.subtype
                    OR d.attrs IS DISTINCT FROM p.attrs) AS payload_changed"""
            ),
        )
    )

def _chronos_dirty_edges_since_previous_sql(
    edge_table: str, *, since_base: bool = False, keyed: bool = False,
) -> sql.Composed:
    """Edges written in the current segment that differ from the previous point.

    Parameters: ``(branch_name, previous_segment_id)``; with
    ``since_base``, ``(branch_name, previous_segment_id, segment_ids)``
    and the ancestry seed (issue #2865). With ``keyed``, one more
    parameter, a list of edge ids, limits the seed to those ids.

    Each row also carries ``deleted``, ``was_live`` and
    ``payload_changed``, the classification the delta counts use
    (``_chronos_segment_table_delta``), so a scan can count its own rows
    (issue #2941).
    """
    ctes = (
        _CHRONOS_WRITTEN_SINCE_BASE_CTES
        if since_base
        else _CHRONOS_CURRENT_SEGMENT_CTES
    )
    seed = _chronos_dirty_seed_filter("e", since_base=since_base)
    if keyed:
        seed += "\n                 AND e.edge_id = ANY(%s)"
    return (
        sql.SQL(
            ctes
            + """, dirty AS (
              SELECT DISTINCT ON (e.edge_id)
                     e.edge_id, e.edge_type, e.attrs,
                     COALESCE(e.deleted, FALSE) AS deleted
                FROM {edge_table} e
                """
            + seed
            + """
               ORDER BY e.edge_id, e.live_lo DESC
            )
            SELECT d.edge_id, d.edge_type, d.deleted,
                   (p.edge_id IS NOT NULL AND NOT p.deleted) AS was_live,
                   (d.edge_type IS DISTINCT FROM p.edge_type
                    OR d.attrs IS DISTINCT FROM p.attrs) AS payload_changed
              FROM dirty d
              -- Prior state at the previous branch point,
              -- tombstones included, looked up once per
              -- dirty edge by primary key. Same reasons as
              -- the node query above (issue #2556): the
              -- predicate compares `d.deleted IS DISTINCT
              -- FROM p.deleted`, so a resurrected edge is
              -- reported as changed.
              LEFT JOIN LATERAL (
                SELECT p.edge_id, p.edge_type, p.attrs,
                       COALESCE(p.deleted, FALSE) AS deleted
                  FROM {edge_table} p
                 WHERE p.edge_id = d.edge_id
                   AND p.live_lo <= (SELECT branch_point FROM previous_point)
                   AND (SELECT branch_point FROM previous_point) < p.live_hi
                 ORDER BY p.live_lo DESC
                 LIMIT 1
              ) p ON TRUE
             WHERE p.edge_id IS NULL
                OR d.deleted IS DISTINCT FROM p.deleted
                OR (
                     d.deleted = FALSE
                 AND (
                      d.edge_type IS DISTINCT FROM p.edge_type
                   OR d.attrs IS DISTINCT FROM p.attrs
                 )
                )
            """
        ).format(edge_table=sql.Identifier(edge_table))
    )


def _chronos_segment_table_delta_sql(
    *,
    physical_table: str,
    key_column: str,
    compare_columns: tuple[str, ...],
    branch_name: str,
    previous_segment_id: str,
    key_values: list[Any] | tuple[Any, ...] | None = None,
    since_base_segments: list[int] | None = None,
    changed_requires_non_null: tuple[str, ...] = (),
) -> tuple[sql.Composed, tuple[Any, ...]]:
    """The delta-count statement of ``Publisher._chronos_segment_table_delta``
    and its parameters, separate so a test can EXPLAIN it.

    ``changed_requires_non_null`` narrows "updated" to rows whose new value of
    each named column is not NULL (PACT openrouter-embeddings-hnsw); empty by
    default, which leaves the statement unchanged.

    Each dirty key's previous state is one ``LATERAL ... LIMIT 1`` probe of
    the key's rows (issues #3593, #2941). The set-at-a-time join it replaces
    is the shape issue #2556 removed from the dirty-scope statements: on a
    cold build, whose tables carry no or stale statistics, the planner could
    re-run the whole previous-state scan once per dirty row, and the count
    hit the 900 s statement timeout. A probe per dirty row has no such plan.
    """
    multi_segment = bool(since_base_segments) and len(since_base_segments or ()) > 1
    key = sql.Identifier(key_column)
    compare_select = sql.SQL(", ").join(
        sql.SQL("t.{col} AS {col}").format(col=sql.Identifier(col))
        for col in compare_columns
    )
    update_predicate = (
        sql.SQL(" OR ").join(
            sql.SQL("d.{col} IS DISTINCT FROM p.{col}").format(
                col=sql.Identifier(col)
            )
            for col in compare_columns
        )
        if compare_columns else sql.SQL("FALSE")
    )
    if changed_requires_non_null:
        update_predicate = sql.SQL("({changed}) AND {non_null}").format(
            changed=update_predicate,
            non_null=sql.SQL(" AND ").join(
                sql.SQL("d.{col} IS NOT NULL").format(col=sql.Identifier(col))
                for col in changed_requires_non_null
            ),
        )
    compare_select_prefix = (
        sql.SQL(", ") + compare_select
        if compare_columns else sql.SQL("")
    )
    key_filter = (
        sql.SQL("AND t.{key} = ANY(%s)").format(key=key)
        if key_values is not None else sql.SQL("")
    )
    params: tuple[Any, ...] = (branch_name, previous_segment_id)
    if multi_segment:
        params = (*params, list(since_base_segments or ()))
    if key_values is not None:
        params = (*params, list(key_values))
    if multi_segment:
        # Rows the listed segments wrote, at the version the branch sees.
        current_seg_columns = sql.SQL("s.live_lo, s.live_hi, s.branch_point")
        dirty_source = sql.SQL(
            """FROM {physical} t
                    CROSS JOIN current_seg s
                   WHERE t.writer_segment_id = ANY(%s)
                     AND t.live_lo <= s.branch_point
                     AND s.branch_point < t.live_hi"""
        ).format(physical=sql.Identifier(physical_table))
    else:
        current_seg_columns = sql.SQL("s.live_lo, s.live_hi")
        dirty_source = sql.SQL(
            """FROM {physical} t
                    JOIN current_seg s
                      ON t.live_lo >= s.live_lo
                     AND t.live_hi <= s.live_hi
                   WHERE TRUE"""
        ).format(physical=sql.Identifier(physical_table))
    statement = sql.SQL(
        """
        WITH current_seg AS (
          SELECT {current_seg_columns}
            FROM _chronos_branch_interval_segments s
            JOIN _chronos_branch_interval_branches b
              ON b.current_segment_id = s.segment_id
           WHERE b.branch_id = %s
        ), previous_point AS (
          SELECT branch_point
            FROM _chronos_branch_interval_segments
           WHERE segment_id = %s
        ), dirty AS (
          SELECT DISTINCT ON (t.{key})
                 t.{key} AS graph_key,
                 COALESCE(t.deleted, FALSE) AS deleted
                 {compare_select_prefix}
            {dirty_source}
             {key_filter}
           ORDER BY t.{key}, t.live_lo DESC
        ), classified AS (
          -- One definition of "was live before", read by all three
          -- buckets. They partition a single space (was_live x
          -- is_live), so they MUST share it: repairing one predicate
          -- alone moves the miscount instead of removing it.
          SELECT
            d.deleted AS now_deleted,
            (p.graph_key IS NOT NULL AND NOT p.deleted) AS was_live,
            ({update_predicate}) AS payload_changed
            FROM dirty d
            -- The row visible at the previous branch point, which may
            -- be a TOMBSTONE: presence here is not liveness, and reading
            -- it as liveness is the defect this query was repaired for.
            -- One primary-key probe per dirty row (issues #2556, #3593).
            LEFT JOIN LATERAL (
              SELECT t.{key} AS graph_key,
                     COALESCE(t.deleted, FALSE) AS deleted
                     {compare_select_prefix}
                FROM {physical} t
               WHERE t.{key} = d.graph_key
                 AND t.live_lo <= (SELECT branch_point FROM previous_point)
                 AND (SELECT branch_point FROM previous_point) < t.live_hi
               ORDER BY t.live_lo DESC
               LIMIT 1
            ) p ON TRUE
        )
        SELECT
          COUNT(*) FILTER (
            WHERE NOT now_deleted AND NOT was_live
          )::bigint AS added,
          COUNT(*) FILTER (
            WHERE now_deleted AND was_live
          )::bigint AS retracted,
          COUNT(*) FILTER (
            WHERE NOT now_deleted AND was_live AND payload_changed
          )::bigint AS updated
          FROM classified
        """
    ).format(
        key=key,
        physical=sql.Identifier(physical_table),
        compare_select_prefix=compare_select_prefix,
        key_filter=key_filter,
        update_predicate=update_predicate,
        dirty_source=dirty_source,
        current_seg_columns=current_seg_columns,
    )
    return statement, params


class Publisher:
    """One per deployment, built with explicit catalog writer authority.

    Production callers use ``build_deployment_publisher(...)``. Direct test
    callers must pass a canonical ``catalog_identity`` explicitly.

    @implements_concept: happens-before-ordering

    The publisher's monotonically-advancing generation_id (under a
    single-writer advisory lock) is Lamport's happens-before
    ordering applied to substrate state: every reader at gen G
    sees ALL writes whose valid_from_gen <= G and none from G+1
    or later. Concurrent agents' writes are linearized into a
    single total order by the lock.
    """

    def __init__(
        self,
        dsn: str,
        deployment_name: str,
        *,
        extractor: Optional[EntityExtractor] = None,
        routes: Optional[list] = None,   # list[LinkerRoute]
        extractor_source_subtypes: Optional[list[str]] = None,
        resolver_factory: Optional[
            Callable[[psycopg.Connection], AliasResolver]
        ] = None,
        projector: Optional[Projector] = None,
        gate_total_timeout_ms: int = DEFAULT_CROSS_BUDGET_MS,
        hard_statement_timeout_ms: int | None = None,
        hard_statement_timeout_source: str | None = None,
        lock_hold_headroom_factor: float | None = None,
        lock_hold_headroom_factor_source: str | None = None,
        allow_raised_statement_timeout: bool | None = None,
        allow_raised_statement_timeout_source: str | None = None,
        first_publish_warning_budget: Mapping[str, Any] | None = None,
        first_publish_warning_budget_source: str | None = None,
        enrichers: Optional[list] = None,  # list[Enricher]
        derived_producer_policies: Optional[dict[str, dict[str, Any]]] = None,
        materializers: Optional[list] = None,  # list[ViewMaterializer]
        maintained_derivations: Any | None = None,
        lenses: Optional[list] = None,  # list[LensDefinition]
        consistency_gates: Optional[list[ConsistencyPublishGate]] = None,
        post_publish_consistency_checks: Optional[list] = None,
        enqueue_embeddings: bool = False,
        coordination_mode: str = "auto",
        branch_name: str = "default",
        catalog_identity: Any | None = None,
        sync_chronos_graph: Optional[bool] = None,
        chronos_interval_continuation_percent: Optional[int] = None,
        chronos_dirty_scope_hint: _ChronosDirtyScope | None = None,
        chronos_delta_counts_hint: Mapping[str, Any] | None = None,
        enricher_lineage_invalidations_hint: Any = _UNSET,
        enricher_projection_deltas_hint: Any = (),
        native_view_min_refresh_interval_ms: int = 0,
        native_view_materialization_delay_ms: int = 0,
        attribute_effective_view_empty_overlay_fast_path: bool = False,
        history_retention_policy: Mapping[str, Any] | None | object = _UNSET,
        checkpoint_attestors: Optional[Sequence[Any]] = None,
        source_sync_priority_probe: Callable[[], str | None] | None = None,
        source_sync_publish_context: bool = False,
        own_source_run_ids: Iterable[str] | None = None,
        repository_provenance: Any | None = None,
        packaged_release: Any | None = None,
        ingest_completeness: Mapping[str, Any] | None = None,
    ) -> None:
        self.dsn = dsn
        # Issue #3342: the ingest run this publish is for, as
        # `published_completeness.run_marker`. None for every other publish,
        # which carries the previous generation's marker forward.
        self.ingest_completeness = (
            dict(ingest_completeness) if ingest_completeness is not None else None
        )
        self.deployment_name = deployment_name
        self.source_sync_priority_probe = source_sync_priority_probe
        self.source_sync_publish_context = source_sync_publish_context
        # Issue #2993 round 6: the source runs whose own publish this is
        # (named by run id). None for every publisher that is not an ingest's
        # run-end publish (scheduled, `okg run --once`, merge-back): those are
        # refused at once while another run is in progress; an ingest's own
        # publish first waits, bounded, for it to finish.
        self.own_source_run_ids = (
            None if own_source_run_ids is None
            else frozenset(str(run_id) for run_id in own_source_run_ids)
        )
        self.catalog_identity = catalog_identity
        from okg.substrate.publisher.repository_provenance import (
            RepositoryProvenance,
        )
        self.repository_provenance = (
            repository_provenance
            if isinstance(repository_provenance, RepositoryProvenance)
            else RepositoryProvenance.unavailable()
        )
        self.packaged_release = None
        self._packaged_release_context = None
        if packaged_release is not None:
            if self.repository_provenance != RepositoryProvenance.unavailable():
                raise ValueError("packaged and Git repository identities cannot be combined")
            from okg.substrate.distributions.packaged_release import (
                _VerifiedRelease, current_verified_release,
            )
            if (not isinstance(packaged_release, _VerifiedRelease)
                    or current_verified_release(packaged_release.deployment_dir) is not packaged_release):
                raise ValueError("packaged publication requires active verified inputs")
            self.packaged_release = packaged_release.identity
            self._packaged_release_context = packaged_release
        from okg.substrate.publisher.checkpoint_attestations import (
            validate_loaded_checkpoint_attestors,
        )

        self.checkpoint_attestors = tuple(
            validate_loaded_checkpoint_attestors(checkpoint_attestors)
        )
        self.chronos_dirty_scope_hint = self._normalize_chronos_dirty_scope(
            chronos_dirty_scope_hint,
        )
        self.chronos_delta_counts_hint = self._normalize_chronos_delta_counts(
            chronos_delta_counts_hint,
        )
        self.enricher_lineage_invalidations_hint = (
            self._normalize_enricher_lineage_invalidations_hint(
                enricher_lineage_invalidations_hint,
            )
        )
        if isinstance(enricher_projection_deltas_hint, Mapping):
            self.enricher_projection_deltas_hint = (
                dict(enricher_projection_deltas_hint),
            )
        else:
            self.enricher_projection_deltas_hint = tuple(
                enricher_projection_deltas_hint or ()
            )
        # AKMON Phase A.3: every publisher cycle targets one branch.
        # Default is the deployment's `default` branch, materialized
        # lazily by the schema trigger if missing. Non-default branches
        # (quarantine:..., experiment:...) MUST be created explicitly
        # via `okg branch create` before the publisher will use them.
        self.branch_name = branch_name
        self.chronos_interval_continuation_percent = (
            chronos_interval_continuation_percent
        )
        self.sync_chronos_graph = (
            sync_chronos_graph
            if sync_chronos_graph is not None
            else os.environ.get("OKG_CHRONOS_GRAPH", "1")
            not in {"0", "false", "FALSE", "no", "NO"}
        )
        # Per `migrate-substrate-runtime-to-dbos` Phase 1: how the
        # Publisher achieves mutual exclusion against itself.
        #   * 'dbos'  — DBOS handles workflow durability/dispatch;
        #               a session advisory lock is still acquired so
        #               source-sync's separate lane cannot overlap a
        #               publisher-cycle publish.
        #   * 'auto'  — resolves to 'dbos'. Kept as the public default
        #               so older callers do not have to pass the mode
        #               explicitly.
        if coordination_mode not in ("auto", "dbos"):
            raise ValueError(
                f"coordination_mode must be 'auto' or 'dbos'; "
                f"got {coordination_mode!r}"
            )
        self.coordination_mode = coordination_mode
        self.gate_total_timeout_ms = gate_total_timeout_ms
        _warn_ignored_publisher_env()
        if hard_statement_timeout_ms is None:
            # Issue #1291: one home for the bound. Deriving it here keeps a
            # direct-constructed Publisher agreeing with the manifest default
            # and with the clamp inside headroom_policy.
            hard_statement_timeout_ms = int(_HARD_TIMEOUT_SECONDS * 1000)
            hard_statement_timeout_source = "direct_default"
        if (
            isinstance(hard_statement_timeout_ms, bool)
            or not isinstance(hard_statement_timeout_ms, int)
            or hard_statement_timeout_ms < 0
        ):
            raise ValueError("hard_statement_timeout_ms must be a nonnegative integer")
        self.hard_statement_timeout_ms = hard_statement_timeout_ms
        self.hard_statement_timeout_source = str(
            hard_statement_timeout_source or "direct_explicit"
        )

        if lock_hold_headroom_factor is None:
            lock_hold_headroom_factor = float(_HEADROOM_FACTOR)
            lock_hold_headroom_factor_source = "direct_default"
        if (
            isinstance(lock_hold_headroom_factor, bool)
            or not isinstance(lock_hold_headroom_factor, (int, float))
            or not math.isfinite(float(lock_hold_headroom_factor))
            or float(lock_hold_headroom_factor) < 1.0
        ):
            raise ValueError(
                "lock_hold_headroom_factor must be finite and >= 1.0"
            )
        self.lock_hold_headroom_factor = float(lock_hold_headroom_factor)
        self.lock_hold_headroom_factor_source = str(
            lock_hold_headroom_factor_source or "direct_explicit"
        )

        if allow_raised_statement_timeout is None:
            allow_raised_statement_timeout = False
            allow_raised_statement_timeout_source = "direct_default"
        if not isinstance(allow_raised_statement_timeout, bool):
            raise ValueError("allow_raised_statement_timeout must be a boolean")
        self.allow_raised_statement_timeout = allow_raised_statement_timeout
        self.allow_raised_statement_timeout_source = str(
            allow_raised_statement_timeout_source or "direct_explicit"
        )

        if first_publish_warning_budget is None:
            self.first_publish_warning_budget = None
            # Force the source, the way the timeout above does. Honouring a
            # caller-supplied source beside a missing budget would let a
            # receipt say `deployment_manifest` about a budget no manifest
            # declared.
            self.first_publish_warning_budget_source = "undeclared"
        else:
            if not isinstance(first_publish_warning_budget, Mapping):
                raise ValueError("first_publish_warning_budget must be a mapping")
            if set(first_publish_warning_budget) != {"node_count", "edge_count"}:
                raise ValueError(
                    "first_publish_warning_budget requires exactly node_count "
                    "and edge_count"
                )
            normalized_budget: dict[str, int] = {}
            for key in ("node_count", "edge_count"):
                value = first_publish_warning_budget[key]
                if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                    raise ValueError(
                        f"first_publish_warning_budget.{key} must be a positive integer"
                    )
                normalized_budget[key] = value
            self.first_publish_warning_budget = normalized_budget
            self.first_publish_warning_budget_source = str(
                first_publish_warning_budget_source or "direct_explicit"
            )
        self.enqueue_embeddings = bool(enqueue_embeddings)
        # Registered enrichers emit derived fact-log rows that the
        # Chronos-native publish path admits and applies to the graph
        # before checkpointing.
        self.enrichers: list = list(enrichers or [])
        self.derived_producer_policies: dict[str, dict[str, Any]] = {
            str(producer_id): dict(policy)
            for producer_id, policy in (
                derived_producer_policies or {}
            ).items()
        }
        self._derived_verdict_events: list[dict[str, Any]] = []
        # Per `add-enricher-required-narrowings` (2026-04-26):
        # validate every enricher's declared
        # `requires_narrowings` against the active catalog
        # version so a missing narrowing surfaces here, not
        # mid-projection.
        if self.catalog_identity is not None:
            from okg.substrate.db.connection import connect as db_connect
            from okg.substrate.catalog.ownership import (
                resolve_catalog_for_publish,
            )
            with db_connect(dsn) as _conn:
                resolve_catalog_for_publish(
                    _conn,
                    identity=self.catalog_identity,
                    deployment_name=self.deployment_name,
                )
        if self.enrichers:
            from okg.substrate.publisher.enricher_loader import (
                validate_enricher_envelopes,
                validate_enricher_narrowings,
            )
            # Envelope-or-refuse precedes catalog narrowing validation: an
            # enricher with no declared write surface (or a dishonest
            # deterministic claim on a model-calling operator) fails boot
            # loudly (spec §9.3).
            validate_enricher_envelopes(self.enrichers)
            validate_enricher_narrowings(dsn, self.enrichers)
        # Native V1 view publication: registered ViewMaterializer
        # adapters still drive the views framework's planning,
        # refresh, and assembly, but publication now goes through
        # derived-artifact output-set manifests. Native view reads
        # resolve completed manifests and typed slots directly.
        from okg.substrate.publisher.native_view_stage import (
            NativeViewStage,
        )
        self.native_view_stage = NativeViewStage(
            list(materializers or []),
            deployment_name=deployment_name,
        )
        self.maintained_derivations = maintained_derivations
        self.native_view_refresh_policy = NativeViewRefreshPolicy(
            min_refresh_interval_ms=max(
                0, int(native_view_min_refresh_interval_ms or 0),
            ),
            materialization_delay_ms=max(
                0, int(native_view_materialization_delay_ms or 0),
            ),
        )
        self.attribute_effective_view_empty_overlay_fast_path = bool(
            attribute_effective_view_empty_overlay_fast_path
        )
        if history_retention_policy is _UNSET:
            try:
                from okg.substrate.deployments import resolve_deployment

                resolved_manifest = resolve_deployment(deployment_name)
                resolved_chronos = getattr(resolved_manifest, "chronos", None)
                history_retention_policy = (
                    resolved_chronos.get("history_retention")
                    if isinstance(resolved_chronos, Mapping)
                    else None
                )
            except Exception:
                # An arbitrary test/library deployment name may have no manifest
                # and retains the historical two-generation behavior. A real
                # deployment directory that fails to load must stop startup:
                # silently choosing legacy pruning could delete `full` history.
                from okg.substrate.deployments import find_deployment_dir

                if find_deployment_dir(deployment_name) is None:
                    history_retention_policy = None
                else:
                    raise
        if history_retention_policy is not None and not isinstance(
            history_retention_policy, Mapping
        ):
            raise SubstrateError(
                "chronos.history_retention must be a mapping before publisher startup"
            )
        self.history_retention_policy = (
            dict(history_retention_policy)
            if history_retention_policy is not None
            else None
        )
        self.lenses: list[Any] = list(lenses or [])
        self._last_view_outcomes: list[Any] = []
        self._last_envelope_violations: list[dict[str, Any]] = []
        self._last_native_view_refresh: dict[str, Any] = {
            "status": "not_run",
        }
        self._last_lens_outcomes: list[dict[str, Any]] = []
        self._last_post_publish_consistency: list[dict[str, Any]] = []
        # Consistency publish gates are Phase-B participants. Each
        # registered gate owns candidate generation/evaluation for
        # the pending generation and returns a persisted
        # ConsistencyPublishGateEvaluation. Gate reuse is disabled
        # when these are present because observation state can move
        # independently of source fact watermarks.
        self.consistency_gates: list[ConsistencyPublishGate] = list(
            consistency_gates or []
        )
        # Post-publish checks are observation producers over already
        # published outputs, including native view relation slots. They
        # may fail or emit open observations, but they do not alter the
        # published generation's admission result.
        self.post_publish_consistency_checks: list = list(
            post_publish_consistency_checks or []
        )
        self._extractor_stage = ChronosExtractorStage.from_config(
            dsn,
            deployment_name=deployment_name,
            extractor=extractor,
            routes=routes,
            extractor_source_subtypes=extractor_source_subtypes,
            resolver_factory=resolver_factory,
            projector=projector,
        )
        # Compatibility for adjacent projection-watermark tests and callers that
        # still inject or inspect the private projector while the live stage
        # boundary moves to ChronosExtractorStage.
        self._projector = self._extractor_stage.projector
        # Issue #3666: endpoint ids this publish's extractor run routed to;
        # the endpoint sweep keeps them (their edges are applied after it).
        self._link_endpoints_cited: set[str] = set()
        # Advisory-lock diagnostics still use these owner fields in error
        # payloads. The old okg.publisher_leases heartbeat table is retired.
        self._owner_id: str | None = None
        self._owner_meta: dict | None = None
        # Set during _locked_connection's `with` body so callers
        # Long-running publish stages can poll `self.lease.assert_held()`.
        self.lease = None
        # okg#3112: True runs the post-publish index rebuild at the end of
        # cycle() (only when the cycle owns its lock). Off by default; the
        # publisher worker turns it on. See cycle().
        self.post_publish_index_rebuild_in_cycle = False

    def set_owner(
        self,
        owner_id: str | None = None,
        owner_meta: dict | None = None,
    ) -> None:
        """CLI shim: set the lease holder identity before
        `cycle()`. `owner_id` defaults to
        `default_holder_id()` at acquire time when None."""
        self._owner_id = owner_id
        self._owner_meta = dict(owner_meta) if owner_meta else None

    # ---------- lease + reservation ----------

    def _resolved_coordination_mode(self) -> str:
        """Resolve ``self.coordination_mode`` to a concrete mode.

        ``'auto'`` now resolves to ``'dbos'`` unconditionally. The old
        ``'lease'`` mode used ``okg.publisher_leases`` and was retired with
        the physical ``okg.jobs`` runtime.
        """
        if self.coordination_mode != "auto":
            return self.coordination_mode
        return "dbos"

    def _chronos_interval_continuation_percent(self) -> int | None:
        if self.chronos_interval_continuation_percent is not None:
            return self.chronos_interval_continuation_percent
        try:
            from okg.substrate.graph_store import (
                deployment_interval_continuation_percent,
            )

            return deployment_interval_continuation_percent(
                self.deployment_name
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "failed to resolve Chronos interval continuation percent for "
                "deployment=%s",
                self.deployment_name,
            )
            raise

    def _dbos_publish_lock_wait_seconds(self) -> float:
        raw = os.environ.get("OKG_DBOS_PUBLISH_LOCK_WAIT_SECONDS")
        if raw is None:
            return DEFAULT_DBOS_PUBLISH_LOCK_WAIT_SECONDS
        try:
            return max(0.0, float(raw))
        except ValueError:
            return DEFAULT_DBOS_PUBLISH_LOCK_WAIT_SECONDS

    def _publish_statement_timeout_ms(self) -> int:
        """Per-statement timeout (ms) for queries run under the publisher lock.

        Bounds a single runaway statement so it cannot hold the advisory lock
        indefinitely and wedge publishing. Product publishers receive this
        from the typed deployment manifest; direct callers use explicit values
        or the fixed compatibility defaults.
        """
        return self.hard_statement_timeout_ms

    def _publisher_configuration_receipt(self) -> dict[str, Any]:
        """Configuration plus provenance used by this exact Publisher."""
        return {
            "schema": "okg.publisher.configuration.v1",
            "hard_statement_timeout_ms": self.hard_statement_timeout_ms,
            "hard_statement_timeout_source": self.hard_statement_timeout_source,
            "lock_hold_headroom_factor": self.lock_hold_headroom_factor,
            "lock_hold_headroom_factor_source": (
                self.lock_hold_headroom_factor_source
            ),
            "allow_raised_statement_timeout": (
                self.allow_raised_statement_timeout
            ),
            "allow_raised_statement_timeout_source": (
                self.allow_raised_statement_timeout_source
            ),
            "first_publish_warning_budget": (
                dict(self.first_publish_warning_budget)
                if self.first_publish_warning_budget is not None
                else None
            ),
            "first_publish_warning_budget_source": (
                self.first_publish_warning_budget_source
            ),
        }

    def _initial_generation_budget_warning(
        self,
        *,
        first_publish: bool,
        node_count: int,
        edge_count: int,
    ) -> dict[str, Any]:
        """Return the deterministic first-publish warning receipt.

        Named away from a ``publish``/``receipt`` identifier and written with
        no ``update``-style call so the DBOS boundary inventory keeps reading
        this pure receipt builder -- and every function that calls it -- as
        the read-only computation it is, not as an authority transition.
        """
        budget = self.first_publish_warning_budget
        triggered_by: list[str] = []
        if not first_publish:
            status, reason = "not_applicable", "not_first_publish"
        elif budget is None:
            status, reason = "not_configured", "undeclared"
        else:
            triggered_by = [
                key
                for key, observed in (
                    ("node_count", int(node_count)),
                    ("edge_count", int(edge_count)),
                )
                if observed >= budget[key]
            ]
            if triggered_by:
                status = "warning"
                reason = "declared_first_publish_count_threshold_reached"
            else:
                status = "not_triggered"
                reason = "counts_below_declared_warning_thresholds"
        return {
            "schema": "okg.publisher.first_publish_warning.v1",
            "first_publish": first_publish,
            "source": self.first_publish_warning_budget_source,
            "node_count": int(node_count),
            "edge_count": int(edge_count),
            "thresholds": dict(budget) if budget is not None else None,
            "triggered": bool(triggered_by),
            "triggered_by": triggered_by,
            "status": status,
            "reason": reason,
        }

    def _publish_lock_hold_warn_seconds(self) -> float:
        """Soft-watchdog threshold (s) for logging a long lock hold. 0 disables."""
        raw = os.environ.get("OKG_PUBLISH_LOCK_HOLD_WARN_SECONDS")
        if raw is None:
            return DEFAULT_PUBLISH_LOCK_HOLD_WARN_SECONDS
        try:
            return max(0.0, float(raw))
        except ValueError:
            return DEFAULT_PUBLISH_LOCK_HOLD_WARN_SECONDS

    def _set_statement_timeout(
        self, conn: psycopg.Connection, timeout_ms: int,
    ) -> None:
        # SET does not accept bound parameters; timeout_ms is an int we control.
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(timeout_ms)}")

    def _reset_statement_timeout(self, conn: psycopg.Connection) -> None:
        """Best-effort reset before the connection returns to the shared pool,
        so the publish bound never leaks to another borrower of the pool."""
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 0")
        except psycopg.Error:
            try:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = 0")
            except psycopg.Error:
                pass  # broken (e.g. timed-out) connection — the pool recycles it

    def _note_publish_lock_hold(
        self, held_start: float, timeout_ms: int, *, mode: str,
    ) -> None:
        """Emit the publish-duration / watchdog signal: how long the publisher
        held the mutex this cycle, WARNing past the soft threshold so a slow
        publish is visible long before it can wedge the lane."""
        held_s = time.monotonic() - held_start
        log = logging.getLogger(__name__)
        warn_s = self._publish_lock_hold_warn_seconds()
        hard = f"{timeout_ms}ms" if timeout_ms > 0 else "off"
        if warn_s and held_s > warn_s:
            log.warning(
                "publisher held the %s publish lock %.1fs (> soft warn %.0fs; "
                "hard statement_timeout %s) deployment=%s — investigate a slow "
                "publish/delta query before it wedges publishing",
                mode, held_s, warn_s, hard, self.deployment_name,
            )
        else:
            log.info(
                "publisher %s lock-hold %.2fs (hard timeout %s) deployment=%s",
                mode, held_s, hard, self.deployment_name,
            )

    def _acquire_dbos_publish_lock(
        self,
        conn: psycopg.Connection,
        *,
        wait_seconds: float | None = None,
    ) -> tuple[int, int]:
        """Acquire the deployment publisher mutex used in DBOS mode.

        Source-sync intentionally runs on its own DBOS lane, so DBOS queue
        concurrency is not a complete publisher mutual-exclusion boundary.
        This session advisory lock restores the single-writer invariant across
        publisher workflows, source-sync inter-layer publishes, and manual
        inline publishes. A killed worker drops the DB session and releases the
        lock, which preserves DBOS recovery behavior.
        """
        classid, objid = _advisory_keys(self.deployment_name)
        wait_seconds = (
            self._dbos_publish_lock_wait_seconds()
            if wait_seconds is None
            else max(0.0, float(wait_seconds))
        )
        deadline = time.monotonic() + wait_seconds
        intent = PublisherWaitIntent(conn, self.deployment_name)
        try:
            while True:
                blocking_source_sync = self._blocking_source_sync_workflow_id(
                    conn=conn,
                )
                if blocking_source_sync is not None:
                    self._yield_to_source_sync(blocking_source_sync, deadline)
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_try_advisory_lock(%s, %s)",
                        (classid, objid),
                    )
                    acquired = bool(cur.fetchone()[0])
                if acquired:
                    # The lock is a SESSION lock: rollback and the pool's return do
                    # not release it. The probe below can raise (a connect timeout,
                    # too many connections), so release on every exit that does not
                    # hand the lock to the caller.
                    try:
                        blocking_source_sync = (
                            self._blocking_source_sync_workflow_id(conn=conn)
                        )
                    except BaseException:
                        self._release_dbos_publish_lock_after_failure(
                            conn, (classid, objid),
                        )
                        raise
                    if blocking_source_sync is not None:
                        self._release_dbos_publish_lock(conn, (classid, objid))
                        self._yield_to_source_sync(blocking_source_sync, deadline)
                        continue
                    return classid, objid
                if wait_seconds <= 0 or time.monotonic() >= deadline:
                    raise PublisherLeaseHeld(
                        "publisher lock is held for "
                        f"deployment={self.deployment_name!r}",
                        data={
                            "deployment_name": self.deployment_name,
                            "coordination_mode": "dbos",
                            "advisory_key": [classid, objid],
                            "wait_seconds": wait_seconds,
                        },
                        fix_hint=(
                            "wait for the active publisher/source-sync publish "
                            "to finish, then retry"
                        ),
                    )
                # Announce the wait (okg#3112): the post-publish index rebuild
                # yields to a waiting publish between its steps.
                intent.announce()
                time.sleep(min(1.0, max(0.05, deadline - time.monotonic())))
        finally:
            intent.release()

    def _blocking_source_sync_workflow_id(
        self,
        *,
        conn: psycopg.Connection | None = None,
    ) -> str | None:
        # Every caller honours the source-sync signal (#3003), including a
        # publish from inside a sync; the signal excludes this run's own hold.
        if conn is not None:
            blocking_intent = (
                source_sync_admission_intent_workflow_id_on_connection(
                    conn=conn,
                    deployment=self.deployment_name,
                )
            )
            if blocking_intent is not None:
                return blocking_intent
        if source_sync_run_guard_held(self.deployment_name):
            # A command-line run's own publish: a worker sync queued or
            # running beside it (e.g. enqueued before the run began) blocks.
            return own_run_guard_runtime_blocker(self.deployment_name)
        # The worker's probe would see the worker sync's own workflow.
        if self.source_sync_priority_probe is None or self._inside_source_sync():
            return None
        try:
            return self.source_sync_priority_probe()
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).exception(
                "publisher source-sync priority probe failed: "
                "deployment=%s error=%s",
                self.deployment_name,
                exc,
            )
            raise

    def _inside_source_sync(self) -> bool:
        return bool(self.source_sync_publish_context) or (
            source_sync_run_guard_held(self.deployment_name)
        )

    def _yield_to_source_sync(self, blocking: str, deadline: float) -> None:
        """A scheduled publish yields at once; a sync's own publish waits for
        the other sync until the publish-lock deadline, then refuses."""
        if not self._inside_source_sync() or time.monotonic() >= deadline:
            self._raise_source_sync_priority(blocking)
        time.sleep(min(1.0, max(0.05, deadline - time.monotonic())))

    def _raise_source_sync_priority(
        self,
        blocking_workflow_id: str,
    ) -> None:
        raise PublisherLeaseHeld(
            "source-sync publish work is active for "
            f"deployment={self.deployment_name!r}",
            data={
                "deployment_name": self.deployment_name,
                "coordination_mode": "dbos",
                "reason": "source_sync_active",
                "blocking_workflow_id": blocking_workflow_id,
            },
            fix_hint=(
                "let the active source-sync workflow publish first, "
                "then retry the scheduled publisher"
            ),
        )

    def _release_dbos_publish_lock(
        self,
        conn: psycopg.Connection,
        lock_key: tuple[int, int],
    ) -> None:
        """Release the publisher mutex, or close the session that holds it.

        A session lock that cannot be released must not stay on a live
        connection: returned to a pool, it blocks every later publish with
        PublisherLeaseHeld until the connection ages out. Closing the
        session is the one release PostgreSQL always honours. An unlock
        that raises is re-raised after the close; an unlock that returns
        false (this session did not hold the lock) is logged and the
        session closed, since its lock bookkeeping is no longer known.
        """
        classid, objid = lock_key

        def _unlock() -> bool:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (classid, objid),
                )
                row = cur.fetchone()
            return bool(row[0]) if row is not None else False

        try:
            try:
                released = _unlock()
            except psycopg.Error:
                conn.rollback()
                released = _unlock()
        except BaseException:
            logging.getLogger(__name__).exception(
                "publisher lock unlock failed; closing the connection so the "
                "session lock cannot outlive it: deployment=%s key=%s",
                self.deployment_name, [classid, objid],
            )
            self._close_lock_connection(conn)
            raise
        if not released:
            logging.getLogger(__name__).error(
                "publisher lock unlock returned false (the session did not "
                "hold it); closing the connection: deployment=%s key=%s",
                self.deployment_name, [classid, objid],
            )
            self._close_lock_connection(conn)

    def _release_dbos_publish_lock_after_failure(
        self,
        conn: psycopg.Connection,
        lock_key: tuple[int, int],
    ) -> None:
        """Release on an error path without masking the original error.

        The caller re-raises the error that got it here. If the release
        itself fails, that failure is logged with the lock key and the
        connection is closed, so the session lock dies with the session
        instead of riding the connection back into a pool.
        """
        try:
            self._release_dbos_publish_lock(conn, lock_key)
        except Exception as release_exc:
            classid, objid = lock_key
            logging.getLogger(__name__).error(
                "publisher lock release failed while handling an earlier "
                "error; closing the connection so the lock dies with the "
                "session: deployment=%s key=%s release_error=%r",
                self.deployment_name, [classid, objid], release_exc,
            )
            self._close_lock_connection(conn)

    @staticmethod
    def _close_lock_connection(conn: psycopg.Connection) -> None:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - closing is the last resort
            logging.getLogger(__name__).exception(
                "closing the publisher lock connection failed",
            )

    @contextmanager
    def _publish_lock_on_connection(
        self,
        conn: psycopg.Connection,
        *,
        mode: str,
        wait_seconds: float | None = None,
    ) -> Iterator[psycopg.Connection]:
        """Hold the deployment publisher mutex on an already-open connection."""

        if mode == "dbos":
            self.lease = None
            lock_key = self._acquire_dbos_publish_lock(
                conn,
                wait_seconds=wait_seconds,
            )
            held_start = time.monotonic()
            timeout_ms = 0
            try:
                # Inside the try: a failed SET must still release the lock.
                timeout_ms = self._publish_statement_timeout_ms()
                if timeout_ms > 0:
                    self._set_statement_timeout(conn, timeout_ms)
                yield conn
            finally:
                try:
                    self._note_publish_lock_hold(
                        held_start, timeout_ms, mode="dbos",
                    )
                    if timeout_ms > 0:
                        self._reset_statement_timeout(conn)
                finally:
                    self._release_dbos_publish_lock(conn, lock_key)
            return

        raise ValueError(f"unsupported publisher coordination mode: {mode!r}")

    @contextmanager
    def _locked_connection(
        self,
        conn: psycopg.Connection | None = None,
        *,
        before_lock: (
            Callable[[psycopg.Connection], Callable[[], None] | None] | None
        ) = None,
    ) -> Iterator[psycopg.Connection]:
        """Yield a connection scoped to one publish cycle.

        Mutual exclusion is selected by ``self.coordination_mode``:

          * ``'dbos'`` — acquire a session advisory lock. DBOS still
            handles workflow durability and queue dispatch, but the
            publisher mutex must span source-sync's separate lane.
          * ``'auto'`` — resolves to 'dbos'.

        DBOS mode uses a session advisory lock on the pooled connection; if
        the worker dies, Postgres releases it.

        ``before_lock`` runs on the cycle's connection before the lock is
        requested (issue 2941 fix 2: the pre-lock attribute hash). It may
        return a cleanup callable, which runs after the lock is released.
        """
        mode = self._resolved_coordination_mode()

        if conn is not None:
            cleanup = before_lock(conn) if before_lock is not None else None
            try:
                with self._publish_lock_on_connection(conn, mode=mode) as locked:
                    yield locked
            finally:
                if cleanup is not None:
                    cleanup()
            return

        # Borrow from the process-shared ConnectionPool so the
        # publisher cycle reuses a warm connection across cycles
        # instead of paying a fresh ~11 ms handshake each time.
        # The connection is auto-returned to the pool on the
        # outer `yield`'s exit.
        from okg.substrate.db.pool import RESERVED, pooled_connection

        with pooled_connection(
            self.dsn,
            purpose=RESERVED,
            application_name=f"okg-publisher-{mode}",
        ) as pooled:
            cleanup = before_lock(pooled) if before_lock is not None else None
            try:
                with self._publish_lock_on_connection(pooled, mode=mode) as locked:
                    yield locked
            finally:
                if cleanup is not None:
                    cleanup()

    def _prelock_attribute_hash_plan(
        self, conn: psycopg.Connection
    ) -> _PrelockHashPlan:
        """Whether this cycle should hash attributes before taking the lock.

        Issue 2941 fixes 2 and 5. The materialize hashes every row it merges.
        A first publish, and a full count-stamp cycle whose previous snapshot
        is partial, merge every row: the pass then hashes every visible row
        (scope "full"). Every other cycle merges only the rows changed since
        the last checkpoint (dirty-driven, or carry-forward over a full
        snapshot): the pass then hashes only the rows the branch wrote since
        its last checkpoint (scope "changed"), and runs only when at least
        _PRELOCK_CHANGED_MIN_ROWS row versions were written. Before fix 5
        such cycles always skipped the pass, so a new source that landed most
        of the graph in one incremental cycle had all of it hashed under the
        lock.

        It is a prediction made without the lock; a wrong one costs time,
        never correctness, because the materialize checks every reused hash
        against the rows it stages under the lock. It is skipped when the
        session already holds an advisory lock (a caller that took the
        publish or branch write lock would hold it through the pass) or is
        inside a transaction, and when another session holds the publish
        lock.
        """
        if os.environ.get("OKG_ATTR_HASH_PRELOCK", "1") == "0":
            return _PrelockHashPlan(
                False, "disabled by OKG_ATTR_HASH_PRELOCK=0", "disabled",
            )
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            return _PrelockHashPlan(
                False, "connection is inside a transaction", "in_transaction",
            )
        if not conn.autocommit:
            # The pass must leave the caller's connection as it found it; on a
            # non-autocommit connection its first read would open a transaction.
            return _PrelockHashPlan(
                False, "connection is not in autocommit mode", "not_autocommit",
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND pid = pg_backend_pid() AND granted"
            )
            held = int(cur.fetchone()[0])
        if held:
            return _PrelockHashPlan(
                False,
                f"the session already holds {held} advisory lock(s); the pass "
                "would run under them",
                "advisory_lock_held",
            )
        # A publisher queued behind another would run its parallel scan and
        # hash while the holder works, taking workers and CPU from the hold
        # it is waiting on. Read pg_locks only; this never takes the lock.
        # Advisory locks are per database, and pg_locks lists every database
        # on the server: a publish of the same deployment name in another
        # database holds an unrelated lock, so only this database counts.
        classid, objid = _advisory_keys(self.deployment_name)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND granted AND objsubid = 2 AND pid <> pg_backend_pid() "
                "AND database = (SELECT oid FROM pg_database "
                "WHERE datname = current_database()) "
                "AND classid = %s::bigint::oid AND objid = %s::bigint::oid",
                (classid & 0xFFFFFFFF, objid & 0xFFFFFFFF),
            )
            others = int(cur.fetchone()[0])
        if others:
            return _PrelockHashPlan(
                False,
                "another session holds the publish lock; hashing now would "
                "compete with its publish",
                "publish_lock_busy",
            )
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=True
        )
        previous = store.latest_generation(
            deployment_name=self.deployment_name,
            branch=self.branch_name,
            status="published",
        )
        if previous is None:
            return _PrelockHashPlan(
                True, "first publish of the branch", "first_publish", "full",
            )
        metadata = dict(getattr(previous, "metadata", None) or {})
        full_cycle = True
        if "count_stamp_cadence" in metadata:
            cadence = int(metadata.get("count_stamp_cadence", 0) or 0)
            full_cycle = (cadence + 1) % _count_stamp_self_heal_every() == 0
        has_writes = self._chronos_current_segment_has_graph_writes(
            conn, branch_name=self.branch_name,
        )
        if has_writes is False:
            return _PrelockHashPlan(
                False, "no graph writes since the last checkpoint",
                "no_graph_writes",
            )
        if full_cycle:
            # A full cycle copies every unchanged row that the previous
            # generation's snapshot holds (carry-forward) and hashes only the
            # rest. A partial snapshot (the usual changed-only one) leaves
            # most rows to hash: hash every row. A full one (a fork branch,
            # or right after a full cycle) leaves only the rows changed since
            # then: decide on those, as an incremental cycle does.
            from okg.substrate.attributes import prior_snapshot_coverage

            coverage = prior_snapshot_coverage(conn, str(previous.checkpoint_id))
            if not all(
                expected is not None and covered >= expected
                for covered, expected in coverage.values()
            ):
                return _PrelockHashPlan(
                    True, "a full count-stamp cycle is expected", "full_cycle",
                    "full",
                )
            basis = (
                "a full count-stamp cycle over a full previous snapshot ("
                f"{coverage['node'][0]} node rows, {coverage['edge'][0]} edge "
                "rows) is expected"
            )
        else:
            basis = "an incremental cycle is expected"
        changed = self._chronos_rows_written_since_checkpoint(
            conn, branch_name=self.branch_name, limit=_PRELOCK_CHANGED_MIN_ROWS,
        )
        if changed is None:
            return _PrelockHashPlan(
                False,
                f"{basis}, and the rows written since the last checkpoint "
                "cannot be listed (no interval storage or no writer segment)",
                "changed_rows_unknown",
            )
        segment_ids, rows = changed
        if rows < _PRELOCK_CHANGED_MIN_ROWS:
            return _PrelockHashPlan(
                False,
                f"{basis}, and only {rows} row version(s) were written since "
                f"the last checkpoint (below {_PRELOCK_CHANGED_MIN_ROWS}); "
                "hashing them under the lock costs less than a pass",
                "changed_rows_below_threshold",
                None, segment_ids, rows,
            )
        return _PrelockHashPlan(
            True,
            f"{basis}, and at least {rows} row versions were written since "
            "the last checkpoint; hashing those before the lock",
            "changed_rows", "changed", segment_ids, rows,
        )

    @staticmethod
    def _chronos_rows_written_since_checkpoint(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        limit: int,
    ) -> tuple[tuple[int, ...], int] | None:
        """The writer segments since the last checkpoint and a bounded count.

        Counts the node and edge row versions written in those segments (the
        segment walk of _chronos_current_segment_has_graph_writes), stopping
        at ``limit``; each count reads the (writer_segment_id, key) index.
        None when the branch has no interval storage or no such segment.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT physical_table
                  FROM _chronos_branch_tables
                 WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
                   AND backend = 'interval'
                 ORDER BY table_name DESC
                """
            )
            physical_tables = [str(row[0]) for row in cur.fetchall()]
            if not physical_tables:
                return None
            cur.execute(
                _CHRONOS_SEGMENTS_SINCE_LAST_CHECKPOINT_SQL, (branch_name,),
            )
            segment_ids = tuple(sorted({int(row[0]) for row in cur.fetchall()}))
            if not segment_ids:
                return None
            rows = 0
            for physical_table in physical_tables:
                if rows >= limit:
                    break
                cur.execute(
                    sql.SQL(
                        "SELECT count(*) FROM (SELECT 1 FROM {physical} "
                        "WHERE writer_segment_id = ANY(%s) LIMIT %s) s"
                    ).format(physical=sql.Identifier(physical_table)),
                    (list(segment_ids), limit - rows),
                )
                rows += int(cur.fetchone()[0])
        return segment_ids, rows

    def _prelock_attribute_hash(
        self,
        conn: psycopg.Connection,
        timings_ms: dict[str, Any],
        report: dict[str, Any],
    ) -> Any:
        """Run the pre-lock hash pass; return the cache or None.

        Never raises for an ordinary failure: the cycle then hashes every row
        under the lock, as before, and ``report`` plus one warning say why.
        """
        started = time.perf_counter()
        try:
            plan = self._prelock_attribute_hash_plan(conn)
            report["planned"] = plan.run
            report["reason"] = plan.reason
            report["reason_code"] = plan.code
            if plan.scope is not None:
                report["scope"] = plan.scope
            if plan.changed_rows is not None:
                report["changed_rows_counted"] = plan.changed_rows
            if not plan.run:
                return None
            from okg.substrate.attributes import (
                prepare_prelock_attribute_hash_cache,
            )

            cache = prepare_prelock_attribute_hash_cache(
                conn,
                branch=self.branch_name,
                statement_timeout_ms=self._publish_statement_timeout_ms() or None,
                writer_segment_ids=(
                    plan.segment_ids if plan.scope == "changed" else None
                ),
            )
            report["rows"] = {
                "node": cache.report.get("node"),
                "edge": cache.report.get("edge"),
            }
            report["parallel"] = cache.report.get("parallel")
            return cache
        except (psycopg.Error, SubstrateError, BranchingError) as exc:
            # The failures a database pass can have: a database error, a
            # refusal from the cache builder, or a Chronos branch error. The
            # cycle then hashes every row under the lock and says why; any
            # other exception is a bug and propagates.
            if conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
                conn.rollback()
            report["planned"] = report.get("planned", False)
            if "reason_code" not in report:
                # The plan itself raised: there is no plan reason to report.
                report["reason"] = "the pre-lock plan failed"
                report["reason_code"] = "plan_failed"
            report["failed"] = f"{type(exc).__name__}: {exc}"
            logging.getLogger(__name__).warning(
                "pre-lock attribute hash failed for deployment=%s (%s); the "
                "publish hashes every row under the lock",
                self.deployment_name,
                report["failed"],
            )
            return None
        finally:
            elapsed = round((time.perf_counter() - started) * 1000.0, 3)
            report["elapsed_ms"] = elapsed
            timings_ms["attribute_hash_prelock"] = elapsed
            # Issue 2941 fix 5: one structured record per cycle, written when
            # the pass is over, so a slow publish can say why the pass did or
            # did not run, how many rows it hashed, how long it took, and why
            # it failed. A bug that propagates is logged here too. Outcome
            # values come from cli/run_summary.STATUS_CLASSES.
            if sys.exc_info()[0] is not None:
                outcome = "error"
            elif "failed" in report:
                outcome = "failed"
            elif report.get("planned"):
                outcome = "completed"
            else:
                outcome = "skipped"
            event = {
                "event": "publisher.prelock_attribute_hash_plan",
                "deployment": self.deployment_name,
                "branch": self.branch_name,
                "outcome": outcome,
                **report,
            }
            logging.getLogger(__name__).info(
                "pre-lock attribute hash for deployment=%s: %s (%s) %s",
                self.deployment_name,
                outcome,
                report.get("reason", "the pass raised an unexpected error"),
                json.dumps(event, sort_keys=True, default=str),
                extra={"okg_event": event},
            )

    def _attach_prelock_enrich_report(
        self,
        timing_details: dict[str, Any],
        timings_ms: dict[str, Any],
    ) -> None:
        """Put this cycle's pre-lock report in the result and the logs.

        The full report goes to ``timing_details["prelock_enrich"]``; its
        counters also go into ``timings_ms`` (``prelock_enrich.*``), which is
        the part a persisted publish-timing row keeps.
        """
        from okg.substrate.publisher.prelock_enrich import report_counters

        report = dict(getattr(self, "_last_prelock_enrich_report", None) or {})
        timing_details["prelock_enrich"] = report
        timings_ms.update(report_counters(report))
        if report.get("planned"):
            logging.getLogger(__name__).info(
                "pre-lock enricher report for deployment=%s: %s",
                self.deployment_name,
                json.dumps(report, sort_keys=True, default=str),
            )

    def _prelock_enrich_plan(
        self, conn: psycopg.Connection,
    ) -> tuple[bool, str]:
        """Whether this cycle computes enricher work before the lock.

        Issue 2941 fix 1. Same session gates as the pre-lock attribute hash
        (an idle autocommit connection holding no advisory lock, and no
        other session holding this deployment's publish lock), plus at least
        one enricher that can be planned. Whether there is work is decided
        from the branch itself, inside the pass.
        """
        from okg.substrate.publisher.prelock_enrich import (
            PRELOCK_ENRICH_ENV,
            prelock_enrich_disabled,
        )

        if prelock_enrich_disabled():
            return False, f"disabled by {PRELOCK_ENRICH_ENV}=0"
        if not any(
            getattr(enricher, "prelock_plannable", False)
            for enricher in self.enrichers
        ):
            return False, "no configured enricher can be planned before the lock"
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            return False, "connection is inside a transaction"
        if not conn.autocommit:
            return False, "connection is not in autocommit mode"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND pid = pg_backend_pid() AND granted"
            )
            held = int(cur.fetchone()[0])
        if held:
            return False, (
                f"the session already holds {held} advisory lock(s); the pass "
                "would run under them"
            )
        # Advisory locks are per database, and pg_locks lists every database
        # on the server: a publish of the same deployment name in another
        # database holds an unrelated lock, so only this database counts.
        classid, objid = _advisory_keys(self.deployment_name)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND granted AND objsubid = 2 AND pid <> pg_backend_pid() "
                "AND database = (SELECT oid FROM pg_database "
                "WHERE datname = current_database()) "
                "AND classid = %s::bigint::oid AND objid = %s::bigint::oid",
                (classid & 0xFFFFFFFF, objid & 0xFFFFFFFF),
            )
            others = int(cur.fetchone()[0])
        if others:
            return False, (
                "another session holds the publish lock; its publish would "
                "change what the pass reads"
            )
        return True, "enricher work can be computed before the lock"

    def _prelock_enrich(
        self,
        conn: psycopg.Connection,
        timings_ms: dict[str, Any],
        report: dict[str, Any],
        *,
        force_enrichers: bool = False,
    ) -> Any:
        """Run the pre-lock enricher pass; return its state or None.

        Never raises for an ordinary database failure: the cycle then
        computes everything under the lock, as before, and ``report`` plus
        one warning say why.
        """
        started = time.perf_counter()
        try:
            run, reason = self._prelock_enrich_plan(conn)
            report["planned"] = run
            report["reason"] = reason
            logging.getLogger(__name__).info(
                "pre-lock enricher pass for deployment=%s: %s (%s)",
                self.deployment_name,
                "running" if run else "skipped",
                reason,
            )
            if not run:
                return None
            from okg.substrate.publisher.prelock_enrich import (
                read_only_session,
                rss_mb,
            )

            report["rss_before_mb"] = rss_mb()
            with read_only_session(conn):
                state = self._prelock_enrich_compute(
                    conn, report, force_enrichers=force_enrichers,
                )
            report["rss_after_mb"] = rss_mb()
            if (
                report.get("rss_before_mb") is not None
                and report.get("rss_after_mb") is not None
            ):
                report["rss_delta_mb"] = round(
                    float(report["rss_after_mb"])
                    - float(report["rss_before_mb"]),
                    1,
                )
            return state
        except (psycopg.Error, SubstrateError, BranchingError) as exc:
            if conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
                conn.rollback()
            report["failed"] = f"{type(exc).__name__}: {exc}"
            logging.getLogger(__name__).warning(
                "pre-lock enricher pass failed for deployment=%s (%s); the "
                "publish computes enricher output under the lock",
                self.deployment_name,
                report["failed"],
            )
            return None
        finally:
            elapsed = round((time.perf_counter() - started) * 1000.0, 3)
            report["elapsed_ms"] = elapsed
            timings_ms["prelock_enrich"] = elapsed

    def _prelock_enrich_compute(
        self,
        conn: psycopg.Connection,
        report: dict[str, Any],
        *,
        force_enrichers: bool,
    ) -> Any:
        """The read-only body of the pre-lock enricher pass."""
        from okg.substrate.fact_application import (
            prepare_bounded_fact_batch_chunks,
        )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.publisher.prelock_enrich import (
            PrelockEnrichState,
            differing_keys,
            enricher_state_fingerprint,
            graph_fingerprint,
            predicted_edge_facts,
        )

        log = logging.getLogger(__name__)
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=True,
        )
        previous = store.latest_generation(
            deployment_name=self.deployment_name,
            branch=self.branch_name,
            status="published",
        )
        if previous is None:
            report["planned"] = False
            report["reason"] = "first publish of the branch"
            return None
        if self._chronos_current_segment_has_graph_writes(
            conn, branch_name=self.branch_name,
        ) is False:
            report["planned"] = False
            report["reason"] = "no graph writes since the last checkpoint"
            return None
        # The linkers read the branch named by this session setting; the
        # cycle sets the same value under the lock.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('okg.chronos_branch', %s, false)",
                (self.branch_name,),
            )
        state = PrelockEnrichState(
            report=report,
            previous_checkpoint_id=str(previous.checkpoint_id),
        )
        state.graph = graph_fingerprint(
            conn,
            store=store,
            deployment_name=self.deployment_name,
            branch=self.branch_name,
        )
        state.enricher_state = enricher_state_fingerprint(
            conn, deployment_name=self.deployment_name,
        )

        scope_started = time.perf_counter()
        hint_decision = self._chronos_dirty_scope_hint_decision(
            conn, previous=previous,
        )
        if hint_decision.get("used") and self.chronos_dirty_scope_hint is not None:
            scope = self.chronos_dirty_scope_hint
            state.dirty_scope_from_hint = True
        else:
            scope = self._query_dirty_from_chronos_current_segment_by_type(
                conn,
                branch_name=self.branch_name,
                previous=previous,
                timing_sink={},
                timing_prefix="prelock_enrich.dirty_scope",
            )
        state.dirty_scope = scope
        report["dirty_scope"] = {
            "source": "hint" if state.dirty_scope_from_hint else "scan",
            "elapsed_ms": round(
                (time.perf_counter() - scope_started) * 1000.0, 3,
            ),
        }

        enricher_names = [
            str(getattr(enricher, "name", enricher.__class__.__name__))
            for enricher in self.enrichers
        ]
        progress_rows = self._load_enricher_progress_rows(conn, enricher_names)
        current_fact_watermark, current_edge_watermark = (
            self._current_fact_watermarks(conn)
        )
        current_catalog_version_id = self._current_catalog_version_id(conn)
        lineage_invalidations = self._enricher_lineage_invalidations(
            scope, self.enricher_lineage_invalidations_hint,
        )
        session = store.checkout(self.branch_name)
        for enricher, ename in zip(self.enrichers, enricher_names):
            if not getattr(enricher, "prelock_plannable", False):
                continue
            plan_started = time.perf_counter()
            try:
                ctx, _declaration, _lineage_plan = self._enricher_context(
                    conn,
                    enricher,
                    progress_row=progress_rows.get(ename),
                    current_fact_watermark=current_fact_watermark,
                    current_edge_watermark=current_edge_watermark,
                    current_catalog_version_id=current_catalog_version_id,
                    chronos_dirty_scope=scope,
                    enricher_lineage_invalidations=lineage_invalidations,
                    force_enrichers=force_enrichers,
                )
                if current_catalog_version_id is not None and (
                    self._should_skip_clean_enricher(
                        conn,
                        enricher,
                        ctx,
                        catalog_version_id=current_catalog_version_id,
                    )
                ):
                    state.note_enricher(
                        ename, planned=False, reason="expected_clean_skip",
                    )
                    continue
                plan = enricher.prelock_plan(
                    conn, generation_id=0, incremental=ctx,
                )
            except Exception as exc:  # noqa: BLE001 - planned again under the lock
                if conn.info.transaction_status == (
                    psycopg.pq.TransactionStatus.INERROR
                ):
                    conn.rollback()
                state.note_enricher(
                    ename,
                    planned=False,
                    reason=f"plan_failed: {type(exc).__name__}: {exc}",
                )
                log.warning(
                    "pre-lock plan of enricher %s failed (%s: %s); it runs "
                    "under the lock as before",
                    ename, type(exc).__name__, exc,
                )
                continue
            state.plans[ename] = plan
            state.note_enricher(
                ename,
                planned=True,
                rows=len(plan.rows),
                skipped=int(plan.n_skipped),
                plan_ms=round((time.perf_counter() - plan_started) * 1000.0, 3),
            )
            if not plan.rows:
                continue
            prepare_started = time.perf_counter()
            producer_id = f"_enricher:{ename}"
            try:
                facts = predicted_edge_facts(conn, plan)
                prepared = prepare_bounded_fact_batch_chunks(
                    store,
                    facts,
                    target_branch=self.branch_name,
                    run_id=str(plan.run_id),
                    source=producer_id,
                    policy_fingerprint=state.graph.get("policy_fingerprint"),
                    strict_endpoints=False,
                    derived_reapplication=True,
                    session=session,
                )
            except (psycopg.Error, SubstrateError, BranchingError) as exc:
                if conn.info.transaction_status == (
                    psycopg.pq.TransactionStatus.INERROR
                ):
                    conn.rollback()
                state.note_enricher(
                    ename,
                    derived_prepared=False,
                    derived_reason=f"{type(exc).__name__}: {exc}",
                )
                continue
            state.prepared[(producer_id, uuid.UUID(str(plan.run_id)))] = prepared
            state.note_enricher(
                ename,
                derived_prepared=True,
                derived_chunks=len(prepared),
                derived_prepare_ms=round(
                    (time.perf_counter() - prepare_started) * 1000.0, 3,
                ),
            )

        graph_after = graph_fingerprint(
            conn,
            store=store,
            deployment_name=self.deployment_name,
            branch=self.branch_name,
        )
        enricher_after = enricher_state_fingerprint(
            conn, deployment_name=self.deployment_name,
        )
        changed = differing_keys(state.graph, graph_after) + differing_keys(
            state.enricher_state, enricher_after,
        )
        if changed:
            state.drop("graph_changed_during_planning", changed=changed)
            return state
        state.enrichers_usable = True
        state.derived_usable = True
        return state

    def _prelock_enrich_recheck(
        self,
        conn: psycopg.Connection,
        store: Any,
        state: Any,
    ) -> None:
        """Recheck A: under the lock, before the enricher stage."""
        from okg.substrate.publisher.prelock_enrich import (
            differing_keys,
            enricher_state_fingerprint,
            graph_fingerprint,
        )

        graph_now = graph_fingerprint(
            conn,
            store=store,
            deployment_name=self.deployment_name,
            branch=self.branch_name,
        )
        enricher_now = enricher_state_fingerprint(
            conn, deployment_name=self.deployment_name,
        )
        changed = differing_keys(state.graph, graph_now) + differing_keys(
            state.enricher_state, enricher_now,
        )
        if changed:
            state.report["recheck_enrichers"] = {
                "result": "changed", "changed": changed,
            }
            state.drop("inputs_changed_before_enrichers", changed=changed)
        else:
            state.report["recheck_enrichers"] = {"result": "unchanged"}

    def _prelock_enrich_derived_recheck(
        self,
        conn: psycopg.Connection,
        store: Any,
        state: Any,
    ) -> None:
        """Recheck B: under the lock, before the derived-fact apply.

        The enricher stage appended facts and advanced progress, so only the
        graph part is compared, and not the segment signature: each prepared
        chunk proves its own rows unchanged under the branch write lock.
        """
        from okg.substrate.publisher.prelock_enrich import (
            differing_keys,
            graph_fingerprint,
        )

        graph_now = graph_fingerprint(
            conn,
            store=store,
            deployment_name=self.deployment_name,
            branch=self.branch_name,
            with_segment_signature=False,
        )
        changed = [
            key for key in differing_keys(state.graph, graph_now)
            if key != "segment_signature"
        ]
        derived = state.report.setdefault("derived", {})
        if changed:
            state.derived_usable = False
            derived["fallback"] = {
                "reason": "inputs_changed_before_derived_apply",
                "changed": changed,
            }
        else:
            derived["recheck"] = "unchanged"

    def reserve(self, conn: psycopg.Connection) -> ReservedGeneration:
        """Insert a pending graph_generations row in ONE transaction.

        Side effects (atomic — either all or none observable):

        - Any existing `pending` row FOR THIS DEPLOYMENT is
          transitioned to `blocked` with reason=`abandoned`. (Cleanup
          for crashed prior publisher.) The UPDATE is scoped by
          deployment_name; rows for other deployments are untouched.
        - The new row references the currently-applied
          catalog_version_id (the max applied one).
        - correlation_id is computed before INSERT and inserted
          with the row — no `__pending__` sentinel ever lands in the
          durable log.

        Raises SubstrateError if no applied catalog_version exists
        (catalog never loaded).
        """
        # Switch to explicit transaction mode for the reservation.
        # The advisory lock prevents concurrency, but the schema's
        # partial UNIQUE index `WHERE status='pending'` will reject
        # a second pending row for the same deployment — so the
        # whole reservation needs to be atomic.
        prior_autocommit = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # 0. Fail closed if this database is owned by a DIFFERENT
                #    deployment (P0 #419 cross-deployment contamination guard).
                #    Runs before ANY write in the reservation; on mismatch it
                #    raises and the except-clause rolls the whole reservation
                #    back, so no branch row, no pending generation, and no
                #    checkpoint can land in a foreign deployment's database.
                from okg.substrate.db.deployment_owner import assert_db_owner
                assert_db_owner(cur, self.deployment_name)

                # 1. The currently-applied catalog version. When the
                # caller supplied a deployment catalog identity, do not
                # blindly use max(applied): a shared dev DB may have a
                # newer catalog written by another worktree.
                from okg.substrate.catalog.ownership import (
                    resolve_catalog_for_publish,
                )
                cv = resolve_catalog_for_publish(
                    conn,
                    identity=self.catalog_identity,
                    deployment_name=self.deployment_name,
                )

                # 2. Resolve the target branch. The `default` branch
                #    is materialized lazily (idempotent ON CONFLICT) to
                #    keep first-publish on a fresh deployment working;
                #    non-default branches must be created explicitly
                #    via `okg branch create`.
                cur.execute(
                    """
                    SELECT branch_id FROM okg.branches
                     WHERE deployment_name = %s AND name = %s
                    """,
                    (self.deployment_name, self.branch_name),
                )
                row = cur.fetchone()
                if row is None:
                    # The `default` branch is materialized lazily
                    # so first-publish on a fresh deployment
                    # works without a bootstrap ritual. ALL other
                    # branches (quarantine:*, experiment:*,
                    # free-form) require an explicit
                    # `okg branch create` call. Per
                    # `fix-akmon-design-mistakes` task 1: the
                    # previous behavior auto-created
                    # `quarantine:*` branches on first publish,
                    # which silently absorbed operator typos
                    # (`--target-branch quarantine:misspelt-
                    # reason` created a stranded branch rather
                    # than surfacing the typo). The fail-fast
                    # shape requires intent at branch-creation
                    # time.
                    if self.branch_name == "default":
                        cur.execute(
                            """
                            INSERT INTO okg.branches
                              (deployment_name, name, role, status,
                               metadata)
                            VALUES
                              (%s, %s, 'default', 'active',
                               jsonb_build_object(
                                 'auto_materialized',
                                 'Publisher.reserve'
                               ))
                            ON CONFLICT (deployment_name, name)
                              DO NOTHING
                            RETURNING branch_id
                            """,
                            (self.deployment_name, self.branch_name),
                        )
                        row = cur.fetchone()
                        if row is None:
                            cur.execute(
                                """
                                SELECT branch_id FROM okg.branches
                                 WHERE deployment_name = %s
                                   AND name = %s
                                """,
                                (self.deployment_name,
                                 self.branch_name),
                            )
                            row = cur.fetchone()
                    else:
                        # Fetch close-edit-distance matches for
                        # the "did you mean..." hint.
                        cur.execute(
                            """
                            SELECT name FROM okg.branches
                             WHERE deployment_name = %s
                               AND status = 'active'
                            ORDER BY name
                            """,
                            (self.deployment_name,),
                        )
                        existing = [r[0] for r in cur.fetchall()]
                        import difflib
                        suggestions = difflib.get_close_matches(
                            self.branch_name, existing, n=3, cutoff=0.5
                        )
                        if suggestions:
                            suggest_phrase = (
                                "did you mean one of: "
                                + ", ".join(suggestions)
                                + "? "
                            )
                        else:
                            suggest_phrase = ""
                        suggest_phrase += (
                            f"create it first via `okg branch create "
                            f"{self.branch_name} --deployment "
                            f"{self.deployment_name} --role "
                            f"{'quarantine' if self.branch_name.startswith('quarantine:') else 'experiment'}`"
                        )
                        from okg.substrate.errors import (
                            BranchNotRegistered,
                        )

                        raise BranchNotRegistered(
                            f"branch {self.branch_name!r} not "
                            f"registered for deployment "
                            f"{self.deployment_name!r}",
                            fix_hint=suggest_phrase,
                            data={
                                "deployment_name": self.deployment_name,
                                "branch_name": self.branch_name,
                                "suggestions": suggestions,
                            },
                        )
                branch_id = int(row[0])

                # 3. Block any stranded pending row for THIS
                #    (deployment, branch) (crash recovery). Other
                #    branches' pending rows are untouched, and
                #    worker-write rows (allocated by
                #    `okg.substrate.runtime.generation.
                #    allocate_worker_write_generation` for out-of-cycle
                #    SDK mutations) are naturally skipped by the
                #    `status = 'pending'` filter per Phase 2 §1.2.
                cur.execute(
                    """
                    UPDATE okg.graph_generations
                       SET status = 'blocked',
                           blocked_reason = jsonb_build_object(
                             'reason','abandoned',
                             'detected_at', now())
                     WHERE deployment_name = %s
                       AND branch_id = %s
                       AND status = 'pending'
                    """,
                    (self.deployment_name, branch_id),
                )

                # 4. Compute correlation_id BEFORE INSERT so the
                #    durable row never carries a sentinel.
                #
                # The generation_id is allocated by BIGSERIAL on
                # INSERT; we don't know it until after. To keep
                # correlation_id deterministic-from-row, we INSERT
                # with a placeholder ID-bearing form and then UPDATE
                # — same as before — but the placeholder is a real
                # value, not a sentinel. To make it durable-safe:
                # use a `nextval` ahead of INSERT.
                cur.execute(
                    "SELECT nextval(pg_get_serial_sequence("
                    "  'okg.graph_generations', 'generation_id'))"
                )
                g_next = cur.fetchone()[0]
                now = datetime.now(timezone.utc)
                correlation_id = (
                    f"pub:{self.deployment_name}:"
                    f"{self.branch_name}:"
                    f"{now.strftime('%Y-%m-%dT%H:%M:%SZ')}:{g_next}"
                )
                trace_identity = current_trace_identity()
                if not trace_identity.is_contextual:
                    trace_identity = (
                        trace_identity_from_latest_source_progress(
                            conn,
                            deployment_name=self.deployment_name,
                        )
                        or trace_identity
                    )

                # 5. INSERT pending row with the resolved branch_id.
                cur.execute(
                    """
                    INSERT INTO okg.graph_generations
                      (generation_id, deployment_name, branch_id,
                       status, catalog_version_id, correlation_id, trace_id,
                       created_at)
                    VALUES (%s, %s, %s, 'pending', %s, %s, %s, %s)
                    """,
                    (g_next, self.deployment_name, branch_id, cv,
                     correlation_id, trace_identity.trace_id, now),
                )
            conn.commit()
        except Exception:  # noqa: BLE001 — rollback on any DB error then re-raise
            conn.rollback()
            raise
        finally:
            conn.autocommit = prior_autocommit

        return ReservedGeneration(
            generation_id=g_next,
            deployment_name=self.deployment_name,
            catalog_version_id=cv,
            correlation_id=correlation_id,
            _conn=conn,
            trace_id=trace_identity.trace_id,
        )

    def _admit_extractor_outputs(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        catalog_version_id: int,
        prev_edge_fact_watermark: int,
    ) -> None:
        self._admit_derived_edge_outputs(
            conn,
            generation_id=generation_id,
            catalog_version_id=catalog_version_id,
            prev_edge_fact_watermark=prev_edge_fact_watermark,
            source_prefix="_extractor:",
            producer_kind="extractor",
            default_policy_factory=self._extractor_policy,
        )

    def _materialize_declared_derived_profiles(
        self,
        conn: psycopg.Connection,
        *,
        catalog_version_id: int,
    ) -> None:
        for producer_id in sorted(self.derived_producer_policies):
            policy = self._declared_derived_policy(producer_id)
            materialize_producer_profile(
                conn,
                deployment_name=self.deployment_name,
                producer_id=str(policy["producer_id"]),
                producer_kind=str(policy["producer_kind"]),
                trust_label=str(policy["trust_label"]),
                admission_mode=str(policy["admission_mode"]),
                authority_scope=dict(policy["authority_scope"]),
                allowed_record_kinds=tuple(policy["allowed_record_kinds"]),
                output_signature=dict(policy["output_signature"]),
                policy_clauses=tuple(
                    dict(clause) for clause in policy["policy_clauses"]
                ),
                output_scope_summary=dict(policy["output_scope_summary"]),
                source_ref=dict(policy.get("source_ref") or {}),
                catalog_version_id=catalog_version_id,
            )

    def _extractor_policy(self, source: str) -> dict[str, Any]:
        if source in self.derived_producer_policies:
            return self._declared_derived_policy(source)
        return self._compat_derived_policy(source)

    def _declared_derived_policy(self, producer_id: str) -> dict[str, Any]:
        authority = self.derived_producer_policies[producer_id]
        producer_kind = str(
            authority.get("producer_kind")
            or _infer_derived_producer_kind(producer_id)
        )
        policy = policy_from_authority(
            authority,
            producer_id=producer_id,
            producer_kind=producer_kind,
        )
        signature = normalize_output_signature(
            policy.get("output_signature"),
            source_name=producer_id,
        )
        return {
            "producer_id": str(policy.get("producer_id") or producer_id),
            "producer_kind": str(policy.get("producer_kind") or producer_kind),
            "trust_label": str(
                policy.get("trust_label") or "derived-deterministic-trusted"
            ),
            "admission_mode": str(
                policy.get("admission_mode") or "fast_track"
            ),
            "authority_scope": normalize_authority_scope(
                policy.get("authority_scope"),
                source_name=producer_id,
            ),
            "allowed_record_kinds": normalize_allowed_record_kinds(
                policy.get("allowed_record_kinds"),
                output_signature=signature,
                source_name=producer_id,
            ),
            "output_signature": signature,
            "policy_clauses": normalize_policy_clauses(
                policy.get("policy_clauses"),
                source_name=producer_id,
            ),
            "output_scope_summary": dict(
                policy.get("output_scope_summary") or {}
            ),
            "source_ref": dict(authority.get("source_ref") or {}),
        }

    def _admit_enricher_outputs(
        self,
        conn: psycopg.Connection,
        *,
        enricher: Any,
        generation_id: int,
        catalog_version_id: int,
        prev_edge_fact_watermark: int,
        prev_node_fact_watermark: int | None = None,
        failure_reason: str | None = None,
        profile_already_materialized: bool = False,
    ) -> None:
        ename = getattr(enricher, "name", enricher.__class__.__name__)
        producer_id = f"_enricher:{ename}"
        if not profile_already_materialized:
            self._materialize_enricher_profile(
                conn,
                enricher=enricher,
                producer_id=producer_id,
                catalog_version_id=catalog_version_id,
            )
        def policy_factory(source: str) -> dict[str, Any]:
            del source
            return self._enricher_policy(enricher, producer_id=producer_id)
        self._admit_derived_edge_outputs(
            conn,
            generation_id=generation_id,
            catalog_version_id=catalog_version_id,
            prev_edge_fact_watermark=prev_edge_fact_watermark,
            prev_node_fact_watermark=prev_node_fact_watermark,
            source_prefix=producer_id,
            producer_kind="enricher",
            default_policy_factory=policy_factory,
            failure_reason=failure_reason,
        )
        # PACT admit-derived-node-emission: a producer emitting only nodes
        # was previously admitted by nothing at all.
        if prev_node_fact_watermark is not None:
            self._admit_derived_node_outputs(
                conn,
                generation_id=generation_id,
                catalog_version_id=catalog_version_id,
                prev_node_fact_watermark=prev_node_fact_watermark,
                source_prefix=producer_id,
                producer_kind="enricher",
                default_policy_factory=policy_factory,
                failure_reason=failure_reason,
            )

    def _admit_derived_node_outputs(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        catalog_version_id: int,
        prev_node_fact_watermark: int,
        source_prefix: str,
        producer_kind: str,
        default_policy_factory: Callable[[str], dict[str, Any]],
        failure_reason: str | None = None,
    ) -> None:
        """Admit a derived producer's NODE output against its profile.

        PACT ``admit-derived-node-emission``. Derived admission was
        edge-only: ``derived_edge_emission`` was the sole intent and the
        fact-range scan read ``okg.edge_facts`` exclusively, so a producer
        that emits only nodes was governed by nothing. It could emit any
        subtype, outside any declared output signature, and the loader's
        ``rejected``/``failed``/``quarantined`` deny-list had no envelope to
        act on — so its output projected unconditionally.

        Writing an envelope here is what gives that existing deny-list teeth
        for node-only producers. It deliberately does NOT change the loader
        into an allow-list: runs with no envelope stay loadable, which is a
        deliberate substrate rule ("governance is per-emission, not a
        precondition for projection"). Narrowing that is a separate,
        blast-radius-bearing decision.

        Scope note: this evaluates the OUTPUT SIGNATURE (is this producer
        permitted to emit this subtype?). It does not yet run the
        input-trust comparison the edge path runs, because a derived node's
        inputs are only resolvable from the ``derived_from`` declaration and
        the trust of the resulting row already travels through its inherited
        lineage envelope. That comparison stays tracked in the PACT.
        """
        current_node_fact_watermark, _edge_fw = self._current_fact_watermarks(
            conn
        )
        # The runtime enforcement mode that decides whether a refusal
        # withholds this output; read-only, and resolved through the helper
        # so this function stays a pure admission-envelope writer.
        gating_mode = derived_gating_mode(self.deployment_name)
        refs = self._derived_node_fact_ranges(
            conn,
            prev_node_fact_watermark=prev_node_fact_watermark,
            current_node_fact_watermark=current_node_fact_watermark,
            source_prefix=source_prefix,
        )
        if not refs:
            return
        workflow_ref = workflow_ref_from_dbos("okg_publisher_cycle")
        for ref in refs:
            profile, lookup = self._resolve_derived_profile(
                conn,
                producer_id=ref.source,
                producer_kind=producer_kind,
                catalog_version_id=catalog_version_id,
                default_policy=default_policy_factory(ref.source),
            )
            verdict_base = {
                "source": ref.source,
                "run_id": ref.run_id,
                "fact_id_min": ref.fact_id_min,
                "fact_id_max": ref.fact_id_max,
                "record_count": ref.record_count,
                "producer_lookup": lookup,
            }
            envelope = ensure_envelope(
                conn,
                deployment_name=self.deployment_name,
                producer_id=profile.producer_id,
                producer_kind=profile.producer_kind,
                intent="derived_node_emission",
                admission_mode=profile.admission_mode,
                status="pending",
                idempotency_key=stable_idempotency_key(
                    "derived_node_emission",
                    self.deployment_name,
                    profile.producer_id,
                    ref.source,
                    ref.run_id,
                    ref.fact_id_min,
                    ref.fact_id_max,
                ),
                catalog_version_id=catalog_version_id,
                source=ref.source,
                run_id=ref.run_id,
                authority={
                    "producer_policy": profile.evidence(),
                    "authority_scope": profile.authority_scope,
                    "output_scope_summary": profile.output_scope_summary,
                    "source_ref": profile.source_ref,
                },
                check_summary={
                    "admission_policy": {
                        "producer_id": profile.producer_id,
                        "producer_kind": profile.producer_kind,
                        "admission_mode": profile.admission_mode,
                    },
                    # What this producer EMITTED, recorded at emission time
                    # (issue #1331). Without it an envelope says only what was
                    # decided, so "derived nothing" and "output dropped" are
                    # indistinguishable from the record.
                    "emitted_record_counts": {
                        "node": int(ref.record_count),
                        "edge": 0,
                    },
                    "producer_policy": {
                        **profile.evidence(),
                        **(
                            {"warnings": ["producer_signature_missing"]}
                            if profile.status == "compat"
                            else {}
                        ),
                    },
                    "producer_lookup": lookup,
                },
                workflow_ref=workflow_ref,
                target_branch_name=(
                    self.branch_name
                    if self.branch_name != "default" else None
                ),
            )
            attach_fact_range_refs(
                conn,
                admission_id=envelope.admission_id,
                refs=[ref],
                workflow_ref=workflow_ref,
            )
            if envelope.status in {
                "published", "blocked", "rejected", "failed", "quarantined",
            }:
                continue
            if failure_reason is not None:
                status = "failed"
                reason = failure_reason
                verdict = merge_policy_evidence(
                    verdict_base,
                    {
                        "code": failure_reason,
                        "message": "derived producer failed during emit",
                        "producer_policy": {
                            **profile.evidence(),
                            "status": "failed",
                        },
                    },
                )
            else:
                try:
                    policy_evidence = evaluate_node_fact_range_signature(
                        conn,
                        profile=profile,
                        source=ref.source,
                        fact_id_min=ref.fact_id_min,
                        fact_id_max=ref.fact_id_max,
                    )
                    status = "accepted"
                    reason = "derived_producer_policy_matched"
                    verdict = merge_policy_evidence(
                        verdict_base,
                        policy_evidence,
                    )
                except ProducerPolicyViolation as exc:
                    status = "rejected"
                    reason = exc.code
                    verdict = merge_policy_evidence(verdict_base, exc.verdict)
                    if exc.code in ENVELOPE_VIOLATION_CODES:
                        record_envelope_violation(
                            conn,
                            deployment=self.deployment_name,
                            generation_id=generation_id,
                            producer_id=profile.producer_id,
                            code=exc.code,
                            record_kind="node",
                            detail=exc.verdict,
                        )
                        self._last_envelope_violations.append({
                            "producer_id": profile.producer_id,
                            "code": exc.code,
                            "record_kind": "node",
                            "source": ref.source,
                            "record_count": ref.record_count,
                            "poisons_reuse": True,
                        })
            verdict = merge_policy_evidence(
                verdict,
                derived_admission_record_counts(
                    status,
                    ref.record_count,
                    record_kind="node",
                    gating_mode=gating_mode,
                ),
            )
            transition_envelope(
                conn,
                admission_id=envelope.admission_id,
                status=status,
                actor=ref.source,
                reason=reason,
                verdict=verdict,
                workflow_ref=workflow_ref,
            )
            self._emit_derived_verdict_event({
                "generation_id": generation_id,
                "producer_id": profile.producer_id,
                "producer_kind": profile.producer_kind,
                "source": ref.source,
                "status": status,
                "reason": reason,
                "record_count": ref.record_count,
                "record_kind": "node",
                "profile_fingerprint": profile.profile_fingerprint,
                "profile_status": profile.status,
                "compatibility": profile.status == "compat",
                "producer_policy": verdict.get("producer_policy", {}),
            })

    def _admit_derived_edge_outputs(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        catalog_version_id: int,
        prev_edge_fact_watermark: int,
        source_prefix: str,
        producer_kind: str,
        default_policy_factory: Callable[[str], dict[str, Any]],
        prev_node_fact_watermark: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        current_node_fact_watermark, current_edge_fact_watermark = (
            self._current_fact_watermarks(
                conn
            )
        )
        # The runtime enforcement mode that decides whether a refusal
        # withholds this output; read-only, and resolved through the helper
        # so this function stays a pure admission-envelope writer.
        gating_mode = derived_gating_mode(self.deployment_name)
        refs = self._derived_edge_fact_ranges(
            conn,
            prev_edge_fact_watermark=prev_edge_fact_watermark,
            current_edge_fact_watermark=current_edge_fact_watermark,
            source_prefix=source_prefix,
        )
        if not refs:
            return
        workflow_ref = workflow_ref_from_dbos("okg_publisher_cycle")
        for ref in refs:
            profile, lookup = self._resolve_derived_profile(
                conn,
                producer_id=ref.source,
                producer_kind=producer_kind,
                catalog_version_id=catalog_version_id,
                default_policy=default_policy_factory(ref.source),
            )
            verdict_base = {
                "source": ref.source,
                "run_id": ref.run_id,
                "fact_id_min": ref.fact_id_min,
                "fact_id_max": ref.fact_id_max,
                "record_count": ref.record_count,
                "producer_lookup": lookup,
            }
            envelope = ensure_envelope(
                conn,
                deployment_name=self.deployment_name,
                producer_id=profile.producer_id,
                producer_kind=profile.producer_kind,
                intent="derived_edge_emission",
                admission_mode=profile.admission_mode,
                status="pending",
                idempotency_key=stable_idempotency_key(
                    "derived_edge_emission",
                    self.deployment_name,
                    profile.producer_id,
                    ref.source,
                    ref.run_id,
                    ref.fact_id_min,
                    ref.fact_id_max,
                ),
                catalog_version_id=catalog_version_id,
                source=ref.source,
                run_id=ref.run_id,
                authority={
                    "producer_policy": profile.evidence(),
                    "authority_scope": profile.authority_scope,
                    "output_scope_summary": profile.output_scope_summary,
                    "source_ref": profile.source_ref,
                },
                check_summary={
                    "admission_policy": {
                        "producer_id": profile.producer_id,
                        "producer_kind": profile.producer_kind,
                        "admission_mode": profile.admission_mode,
                    },
                    # What this producer EMITTED, recorded at emission time
                    # (issue #1331). Without it an envelope says only what was
                    # decided, so "derived nothing" and "output dropped" are
                    # indistinguishable from the record.
                    "emitted_record_counts": {
                        "node": 0,
                        "edge": int(ref.record_count),
                    },
                    "producer_policy": {
                        **profile.evidence(),
                        **(
                            {"warnings": ["producer_signature_missing"]}
                            if profile.status == "compat"
                            else {}
                        ),
                    },
                    "producer_lookup": lookup,
                },
                workflow_ref=workflow_ref,
                target_branch_name=(
                    self.branch_name
                    if self.branch_name != "default" else None
                ),
            )
            attach_fact_range_refs(
                conn,
                admission_id=envelope.admission_id,
                refs=[ref],
                workflow_ref=workflow_ref,
            )
            if envelope.status in {
                "published", "blocked", "rejected", "failed",
                "quarantined",
            }:
                continue
            if failure_reason is not None:
                status = "failed"
                reason = failure_reason
                verdict = merge_policy_evidence(
                    verdict_base,
                    {
                        "code": failure_reason,
                        "message": "derived producer failed during emit",
                        "producer_policy": {
                            **profile.evidence(),
                            "status": "failed",
                        },
                    },
                )
            else:
                try:
                    policy_evidence = evaluate_edge_fact_range_signature(
                        conn,
                        profile=profile,
                        source=ref.source,
                        fact_id_min=ref.fact_id_min,
                        fact_id_max=ref.fact_id_max,
                        node_fact_id_min=(
                            prev_node_fact_watermark + 1
                            if prev_node_fact_watermark is not None
                            else None
                        ),
                        node_fact_id_max=(
                            current_node_fact_watermark
                            if prev_node_fact_watermark is not None
                            else None
                        ),
                        generation_id=generation_id,
                    )
                    input_trust_sources = (
                        self._derived_edge_input_trust_sources(
                            conn,
                            source=ref.source,
                            fact_id_min=ref.fact_id_min,
                            fact_id_max=ref.fact_id_max,
                        )
                    )
                    trust_evidence = evaluate_derived_output_trust(
                        profile=profile,
                        input_trust_labels=[
                            str(item["trust_label"])
                            for item in input_trust_sources
                        ],
                        input_sources=input_trust_sources,
                    )
                    status = "accepted"
                    reason = "derived_producer_policy_matched"
                    verdict = merge_policy_evidence(
                        verdict_base,
                        policy_evidence,
                        trust_evidence,
                    )
                except ProducerPolicyViolation as exc:
                    status = "rejected"
                    reason = exc.code
                    verdict = merge_policy_evidence(verdict_base, exc.verdict)
                    if exc.code in ENVELOPE_VIOLATION_CODES:
                        # A well-typed output outside the producer's finite
                        # envelope (spec §9.3): persist the violation — the
                        # reuse-poisoning substrate — and surface it in the
                        # cycle outcome. The envelope is never widened.
                        record_envelope_violation(
                            conn,
                            deployment=self.deployment_name,
                            generation_id=generation_id,
                            producer_id=profile.producer_id,
                            code=exc.code,
                            record_kind="edge",
                            detail=exc.verdict,
                        )
                        self._last_envelope_violations.append({
                            "producer_id": profile.producer_id,
                            "code": exc.code,
                            "record_kind": "edge",
                            "source": ref.source,
                            "record_count": ref.record_count,
                            "poisons_reuse": True,
                        })
            verdict = merge_policy_evidence(
                verdict,
                derived_admission_record_counts(
                    status,
                    ref.record_count,
                    record_kind="edge",
                    gating_mode=gating_mode,
                ),
            )
            transition_envelope(
                conn,
                admission_id=envelope.admission_id,
                status=status,
                actor=ref.source,
                reason=reason,
                verdict=verdict,
                workflow_ref=workflow_ref,
            )
            self._emit_derived_verdict_event({
                "generation_id": generation_id,
                "producer_id": profile.producer_id,
                "producer_kind": profile.producer_kind,
                "source": ref.source,
                "status": status,
                "reason": reason,
                "record_count": ref.record_count,
                "profile_fingerprint": profile.profile_fingerprint,
                "profile_status": profile.status,
                "compatibility": profile.status == "compat",
                "producer_policy": verdict.get("producer_policy", {}),
            })

    def _derived_edge_input_trust_sources(
        self,
        conn: psycopg.Connection,
        *,
        source: str,
        fact_id_min: int,
        fact_id_max: int,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT attrs
                  FROM okg.edge_facts ef
                 WHERE ef.source = %s
                   AND ef.fact_id BETWEEN %s AND %s
                 ORDER BY fact_id
                """,
                (source, fact_id_min, fact_id_max),
            )
            source_ids: set[str] = set()
            malformed_lineage = False
            for (attrs,) in cur.fetchall():
                attrs = dict(attrs or {})
                if LINEAGE_ATTR in attrs:
                    lineage = normalize_source_lineage(attrs.get(LINEAGE_ATTR))
                    if lineage.get("status") != "ok":
                        malformed_lineage = True
                    source_ids.update(
                        self._lineage_source_ids_for_trust_check(lineage)
                    )
            if not source_ids:
                # The edge facts carry no lineage of their own, so fall back
                # to the ENDPOINT NODES' stamped lineage.
                #
                # This used to read `graph_nodes.source`, which names
                # whichever producer last WROTE the row -- for a derived
                # node, the emitting enricher. Resolving trust from that is
                # the same laundering shape as #1534, one layer down: the
                # enricher's own declared label comes back as the label of
                # its own input. The lineage envelope is the only field that
                # names the ROOT sources, so it is what this fallback reads
                # (issue #1545).
                (
                    endpoint_source_ids,
                    endpoint_lineage_malformed,
                ) = self._chronos_endpoint_source_ids_for_trust_check(
                    conn,
                    branch_name=self.branch_name,
                    source=source,
                    fact_id_min=fact_id_min,
                    fact_id_max=fact_id_max,
                )
                source_ids.update(endpoint_source_ids)
                if endpoint_lineage_malformed:
                    malformed_lineage = True
        profiled_sources: list[dict[str, Any]] = []
        if malformed_lineage:
            profiled_sources.append({
                "source": "__malformed_lineage__",
                "producer_id": "__malformed_lineage__",
                "profile_fingerprint": None,
                "trust_label": "untrusted",
                "producer_lookup": {
                    "status": "synthetic_untrusted_lineage",
                },
            })
        for input_source in sorted(source_ids):
            try:
                input_profile, lookup = load_profile_for_producer(
                    conn,
                    deployment_name=self.deployment_name,
                    producer_id=input_source,
                    allow_source_family_fallback=True,
                    # DECLARED authority only. A machine-minted row exists
                    # for any producer whose facts have been re-applied
                    # into a checkpoint, and reading one here would let a
                    # producer supply its own input trust label (#1545). A
                    # producer with nothing declared -- `_system:native_views`,
                    # a retired source -- resolves as missing and is treated
                    # untrusted below, which is the fail-closed answer.
                    #
                    # Prose warning for the next editor: this function is a
                    # READ, and its registered boundary categories say so.
                    # scripts/check_formal_dbos_epoch_supersession.py
                    # derives those categories from a text scan of the whole
                    # function source, COMMENTS INCLUDED, and its
                    # authority-transition vocabulary includes the ordinary
                    # words for making a generation visible and for the
                    # concurrency and durable-acknowledgement primitives
                    # around it. One such word anywhere in here -- even in a
                    # comment like this one -- reclassifies a read function as an
                    # authority transition and reds the inventory check. The
                    # word list lives in that script; consult it there
                    # rather than restating it here.
                    declared_only=True,
                )
            except ProducerPolicyViolation as exc:
                if exc.code == "producer_profile_missing":
                    profiled_sources.append({
                        "source": input_source,
                        "producer_id": input_source,
                        "profile_fingerprint": None,
                        "trust_label": "untrusted",
                        "producer_lookup": {
                            "status": "missing_profile_treated_untrusted",
                        },
                    })
                    continue
                raise
            profiled_sources.append({
                "source": input_source,
                "producer_id": input_profile.producer_id,
                "profile_fingerprint": input_profile.profile_fingerprint,
                "trust_label": input_profile.trust_label,
                "producer_lookup": lookup,
            })
        return profiled_sources

    @staticmethod
    def _chronos_endpoint_source_ids_for_trust_check(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        source: str,
        fact_id_min: int,
        fact_id_max: int,
    ) -> tuple[set[str], bool]:
        """Root source ids from the endpoint nodes' STAMPED LINEAGE.

        Returns ``(source_ids, lineage_unreadable)``. The name predates the
        second element and the switch from `graph_nodes.source` to lineage;
        it is kept because renaming it would drag
        `tests/substrate/extraction/test_enricher_input_quarantine.py` --
        a postgres-marked file with no per-PR blocking lane owner -- into
        this change. Rename both together when that file gains an owner.

        An UNREADABLE envelope --
        one normalization rejects as malformed -- fails closed: the caller
        turns it into a synthetic untrusted input, so an edge whose
        endpoints have broken provenance can never be admitted at a rank
        that provenance does not support (issue #1545).

        An endpoint with NO lineage at all is a different fact and keeps a
        different verdict: it contributes no source id, and an edge with no
        discoverable input trust is rejected as
        ``derived_output_trust_inputs_missing`` rather than called
        malformed.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ef.src, ef.dst
                  FROM okg.edge_facts ef
                 WHERE ef.source = %s
                   AND ef.fact_id BETWEEN %s AND %s
                """,
                (source, fact_id_min, fact_id_max),
            )
            node_ids = sorted({
                str(node_id)
                for row in cur.fetchall()
                for node_id in row
                if node_id is not None and str(node_id).strip()
            })
        if not node_ids:
            return set(), False

        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            # This fallback is reachable while a model-backed enricher's
            # reservation/output/progress transaction is open.  Its checkout
            # and query must join that transaction; Chronos's default
            # per-query commit would either split the atomic boundary or fail
            # a valid publish inside psycopg's Transaction context.
            autocommit=False,
        )
        try:
            checkout = store.checkout(branch_name)
            rows = checkout.query(
                """
                SELECT node_id, attrs
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                 ORDER BY node_id
                """,
                {"node_ids": node_ids},
            )
        finally:
            store.close()
        resolved: set[str] = set()
        malformed = False
        for row in rows:
            attrs = dict(row.get("attrs") or {})
            if LINEAGE_ATTR not in attrs:
                continue
            lineage = normalize_source_lineage(attrs.get(LINEAGE_ATTR))
            if lineage.get("status") != "ok":
                malformed = True
            # Collect whatever the envelope DOES name, malformed or not --
            # the same order the edge-fact arm above uses. A partly broken
            # envelope that still names a root must contribute that root:
            # dropping it would lose a real untrusted input and leave the
            # verdict resting only on the synthetic marker.
            resolved.update(
                Publisher._lineage_source_ids_for_trust_check(lineage)
            )
        return resolved, malformed

    @staticmethod
    def _lineage_source_ids_for_trust_check(
        lineage: Mapping[str, Any],
    ) -> set[str]:
        source_ids = source_ids_from_lineage(lineage)
        if source_ids:
            return set(source_ids)
        raw_source_ids = lineage.get("source_ids")
        if not isinstance(raw_source_ids, Sequence) or isinstance(
            raw_source_ids,
            (str, bytes),
        ):
            return set()
        return {
            str(source_id).strip()
            for source_id in raw_source_ids
            if str(source_id).strip()
        }

    @staticmethod
    def _derived_node_fact_ranges(
        conn: psycopg.Connection,
        *,
        prev_node_fact_watermark: int,
        current_node_fact_watermark: int,
        source_prefix: str,
    ) -> list[FactRangeRef]:
        """Node-fact ranges per derived producer run.

        PACT ``admit-derived-node-emission``. The edge equivalent below has
        always existed; this one did not, which is why a producer emitting
        only nodes was governed by nothing.
        """
        if current_node_fact_watermark <= prev_node_fact_watermark:
            return []
        with conn.cursor() as cur:
            if source_prefix.endswith(":"):
                predicate, params = "source LIKE %s", (f"{source_prefix}%",)
            else:
                predicate, params = (
                    "(source = %s OR source LIKE %s)",
                    (source_prefix, f"{source_prefix}:%"),
                )
            cur.execute(
                f"""
                SELECT source, run_id::text, min(fact_id)::bigint,
                       max(fact_id)::bigint, count(*)::bigint
                  FROM okg.node_facts
                 WHERE fact_id > %s
                   AND fact_id <= %s
                   AND {predicate}
                 GROUP BY source, run_id
                 ORDER BY source, run_id
                """,
                (
                    prev_node_fact_watermark,
                    current_node_fact_watermark,
                    *params,
                ),
            )
            rows = cur.fetchall()
        return [
            FactRangeRef(
                ref_kind="node_fact_range",
                ref_key=f"node_fact_range:{row[0]}:{row[1]}:{row[2]}:{row[3]}",
                source=str(row[0]),
                run_id=str(row[1]),
                fact_id_min=int(row[2]),
                fact_id_max=int(row[3]),
                record_count=int(row[4]),
            )
            for row in rows
        ]

    @staticmethod
    def _derived_edge_fact_ranges(
        conn: psycopg.Connection,
        *,
        prev_edge_fact_watermark: int,
        current_edge_fact_watermark: int,
        source_prefix: str,
    ) -> list[FactRangeRef]:
        if current_edge_fact_watermark <= prev_edge_fact_watermark:
            return []
        with conn.cursor() as cur:
            if source_prefix.endswith(":"):
                cur.execute(
                    """
                    SELECT source, run_id::text, min(fact_id)::bigint,
                           max(fact_id)::bigint, count(*)::bigint
                      FROM okg.edge_facts
                     WHERE fact_id > %s
                       AND fact_id <= %s
                       AND source LIKE %s
                     GROUP BY source, run_id
                     ORDER BY source, run_id
                    """,
                    (
                        prev_edge_fact_watermark,
                        current_edge_fact_watermark,
                        f"{source_prefix}%",
                    ),
                )
            else:
                cur.execute(
                    """
                    SELECT source, run_id::text, min(fact_id)::bigint,
                           max(fact_id)::bigint, count(*)::bigint
                      FROM okg.edge_facts
                     WHERE fact_id > %s
                       AND fact_id <= %s
                       AND (
                         source = %s
                         OR source LIKE %s
                       )
                     GROUP BY source, run_id
                     ORDER BY source, run_id
                    """,
                    (
                        prev_edge_fact_watermark,
                        current_edge_fact_watermark,
                        source_prefix,
                        f"{source_prefix}:%",
                    ),
                )
            rows = cur.fetchall()
        return [
            FactRangeRef(
                ref_kind="edge_fact_range",
                ref_key=f"edge_fact_range:{row[0]}:{row[1]}:{row[2]}:{row[3]}",
                source=str(row[0]),
                run_id=str(row[1]),
                fact_id_min=int(row[2]),
                fact_id_max=int(row[3]),
                record_count=int(row[4]),
            )
            for row in rows
        ]

    def _resolve_derived_profile(
        self,
        conn: psycopg.Connection,
        *,
        producer_id: str,
        producer_kind: str,
        catalog_version_id: int,
        default_policy: dict[str, Any],
    ) -> tuple[Any, dict[str, Any]]:
        try:
            return load_profile_for_producer(
                conn,
                deployment_name=self.deployment_name,
                producer_id=producer_id,
                allow_source_family_fallback=True,
            )
        except ProducerPolicyViolation as exc:
            if exc.code != "producer_profile_missing":
                raise
        profile = materialize_producer_profile(
            conn,
            deployment_name=self.deployment_name,
            producer_id=str(default_policy.get("producer_id") or producer_id),
            producer_kind=str(default_policy.get("producer_kind")
                              or producer_kind),
            trust_label=str(default_policy.get("trust_label")
                            or "derived-compat"),
            admission_mode=str(default_policy.get("admission_mode")
                               or "fast_track"),
            authority_scope=dict(default_policy.get("authority_scope") or {}),
            allowed_record_kinds=tuple(
                default_policy.get("allowed_record_kinds") or ()
            ),
            output_signature=dict(default_policy.get("output_signature") or {}),
            policy_clauses=tuple(
                dict(clause)
                for clause in default_policy.get("policy_clauses") or ()
            ),
            output_scope_summary=dict(
                default_policy.get("output_scope_summary") or {}
            ),
            source_ref=dict(default_policy.get("source_ref") or {}),
            catalog_version_id=catalog_version_id,
        )
        return profile, {
            "requested_producer_id": producer_id,
            "resolved_producer_id": profile.producer_id,
            "source_family_fallback": profile.producer_id != producer_id,
            "materialized_from_default": True,
        }

    def _materialize_enricher_profile(
        self,
        conn: psycopg.Connection,
        *,
        enricher: Any,
        producer_id: str,
        catalog_version_id: int,
    ) -> str:
        policy = self._enricher_policy(enricher, producer_id=producer_id)
        if policy.pop("_filter_unloaded_catalog_subtypes", False):
            policy = self._filter_policy_to_catalog_subtypes(
                conn,
                policy=policy,
                catalog_version_id=catalog_version_id,
            )
        profile = materialize_producer_profile(
            conn,
            deployment_name=self.deployment_name,
            producer_id=str(policy["producer_id"]),
            producer_kind=str(policy["producer_kind"]),
            trust_label=str(policy["trust_label"]),
            admission_mode=str(policy["admission_mode"]),
            authority_scope=dict(policy["authority_scope"]),
            allowed_record_kinds=tuple(policy["allowed_record_kinds"]),
            output_signature=dict(policy["output_signature"]),
            policy_clauses=tuple(
                dict(clause) for clause in policy["policy_clauses"]
            ),
            output_scope_summary=dict(policy["output_scope_summary"]),
            source_ref=dict(policy.get("source_ref") or {}),
            catalog_version_id=catalog_version_id,
        )
        return profile.profile_fingerprint

    def _expected_enricher_profile_fingerprint(
        self,
        conn: psycopg.Connection,
        *,
        enricher: Any,
        producer_id: str,
        catalog_version_id: int,
    ) -> str:
        policy = self._enricher_policy(enricher, producer_id=producer_id)
        if policy.pop("_filter_unloaded_catalog_subtypes", False):
            policy = self._filter_policy_to_catalog_subtypes(
                conn,
                policy=policy,
                catalog_version_id=catalog_version_id,
            )
        return profile_fingerprint(
            deployment_name=self.deployment_name,
            producer_id=str(policy["producer_id"]),
            producer_kind=str(policy["producer_kind"]),
            trust_label=str(policy["trust_label"]),
            admission_mode=str(policy["admission_mode"]),
            authority_scope=dict(policy["authority_scope"]),
            allowed_record_kinds=tuple(policy["allowed_record_kinds"]),
            output_signature=dict(policy["output_signature"]),
            policy_clauses=tuple(
                dict(clause) for clause in policy["policy_clauses"]
            ),
            catalog_version_id=catalog_version_id,
        )

    def _enricher_policy(
        self,
        enricher: Any,
        *,
        producer_id: str,
    ) -> dict[str, Any]:
        authority = getattr(enricher, "producer_policy", None)
        if authority is None:
            authority = getattr(enricher, "authority", None)
        if authority:
            policy = policy_from_authority(
                authority,
                producer_id=producer_id,
                producer_kind="enricher",
            )
        else:
            from okg.substrate.publisher.enricher_loader import (
                derive_write_envelope,
            )
            envelope = derive_write_envelope(
                enricher, producer_id=producer_id
            )
            signature = dict(envelope.signature)
            nodes = list(signature.get("nodes") or [])
            edges = list(signature.get("edges") or [])
            unbounded = set(getattr(envelope, "unbounded", ()) or ())
            kinds: list[str] = []
            # A dimension declared UNBOUNDED has no clauses to enumerate, so
            # it contributes no signature entries — but the producer still
            # writes that record kind. Without this, an enricher with an
            # open node surface is rejected as producer_record_kind_not_allowed
            # against its own declared envelope.
            if nodes or "nodes" in unbounded:
                kinds.append("node")
            if edges or "edges" in unbounded:
                kinds.append("edge")
            # Classification is DECLARED, never inferred (spec §9.3): a
            # model-backed producer is model-derived-trusted — same lattice
            # rank, honest label — and the former unconditional
            # deterministic stamp survives only as the deterministic
            # branch.
            trust = (
                "model-derived-trusted"
                if envelope.nondeterministic
                else "derived-deterministic-trusted"
            )
            policy = {
                "producer_id": producer_id,
                "producer_kind": "enricher",
                "trust_label": trust,
                "admission_mode": "fast_track",
                "authority_scope": {
                    "source_family": producer_id,
                    "enricher_name": getattr(
                        enricher, "name", enricher.__class__.__name__,
                    ),
                },
                "allowed_record_kinds": kinds,
                "output_signature": signature,
                "policy_clauses": [
                    *(
                        {
                            "id": str(node.get("clause_id")),
                            "effect": "allow",
                            "record_kind": "node",
                            "subtype": str(node.get("subtype")),
                        }
                        for node in nodes
                    ),
                    *(
                        {
                            "id": str(edge.get("clause_id")),
                            "effect": "allow",
                            "record_kind": "edge",
                            "triple": [
                                str(edge.get("src_subtype")),
                                str(edge.get("edge_type")),
                                str(edge.get("dst_subtype")),
                            ],
                        }
                        for edge in edges
                    ),
                ],
                "output_scope_summary": {
                    "summary": (
                        "model-backed enricher output"
                        if envelope.nondeterministic
                        else "deterministic enricher output"
                    ),
                    "edges": [
                        [
                            str(edge.get("src_subtype")),
                            str(edge.get("edge_type")),
                            str(edge.get("dst_subtype")),
                        ]
                        for edge in edges
                    ],
                    "write_envelope": envelope.summary(),
                },
                "_filter_unloaded_catalog_subtypes": True,
            }
        signature = normalize_output_signature(
            policy.get("output_signature"),
            source_name=producer_id,
        )
        return {
            "producer_id": str(policy.get("producer_id") or producer_id),
            "producer_kind": str(policy.get("producer_kind") or "enricher"),
            "trust_label": str(
                policy.get("trust_label") or "derived-deterministic-trusted"
            ),
            "admission_mode": str(policy.get("admission_mode")
                                  or "fast_track"),
            "authority_scope": normalize_authority_scope(
                policy.get("authority_scope"),
                source_name=producer_id,
            ),
            "allowed_record_kinds": normalize_allowed_record_kinds(
                policy.get("allowed_record_kinds"),
                output_signature=signature,
                source_name=producer_id,
            ),
            "output_signature": signature,
            "policy_clauses": normalize_policy_clauses(
                policy.get("policy_clauses"),
                source_name=producer_id,
            ),
            "output_scope_summary": dict(
                policy.get("output_scope_summary") or {}
            ),
            "source_ref": {
                "enricher_name": getattr(
                    enricher, "name", enricher.__class__.__name__,
                ),
                "class": (
                    f"{enricher.__class__.__module__}."
                    f"{enricher.__class__.__qualname__}"
                ),
                **dict(getattr(enricher, "producer_policy_source", {}) or {}),
            },
            "_filter_unloaded_catalog_subtypes": bool(
                policy.get("_filter_unloaded_catalog_subtypes")
            ),
        }

    @staticmethod
    def _filter_policy_to_catalog_subtypes(
        conn: psycopg.Connection,
        *,
        policy: dict[str, Any],
        catalog_version_id: int,
    ) -> dict[str, Any]:
        """Drop auto-derived enricher signature entries for modules
        absent from the active deployment catalog.

        DeclarativeLinker instances can aggregate substrate-shipped
        cross-link rules for many optional ontology modules. The
        runtime rule dispatcher already skips unloaded subtypes; the
        producer profile generated from ``requires_narrowings`` must
        do the same, while still letting catalog validation reject any
        loaded-subtype triple that lacks a narrowing.
        """

        with conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM okg.subtypes WHERE catalog_version_id = %s",
                (catalog_version_id,),
            )
            loaded = {str(row[0]) for row in cur.fetchall()}

        def _known_edge(edge: Mapping[str, Any]) -> bool:
            return (
                str(edge.get("src_subtype")) in loaded
                and str(edge.get("dst_subtype")) in loaded
            )

        def _known_triple(triple: Any) -> bool:
            if not isinstance(triple, (list, tuple)) or len(triple) != 3:
                return False
            return str(triple[0]) in loaded and str(triple[2]) in loaded

        filtered = dict(policy)
        signature = dict(filtered.get("output_signature") or {})
        edges = [
            dict(edge)
            for edge in signature.get("edges") or []
            if isinstance(edge, Mapping) and _known_edge(edge)
        ]
        # PACT admit-derived-node-emission: node clauses get the same
        # catalog filtering as edges, and both feed allowed_record_kinds.
        # This function previously kept node clauses in the signature while
        # hardcoding kinds to ["edge"] — harmless while node emission was
        # admitted by nothing, but now a node-emitting enricher would be
        # rejected as producer_record_kind_not_allowed against its OWN
        # declared signature.
        nodes = [
            dict(node)
            for node in signature.get("nodes") or []
            if isinstance(node, Mapping)
            and str(node.get("subtype")) in loaded
        ]
        signature["edges"] = edges
        signature["nodes"] = nodes
        # An UNBOUNDED dimension has no clauses to filter, so recomputing
        # kinds from clause counts alone would silently drop it here and undo
        # the same fix applied in _enricher_policy — leaving a structural
        # enricher rejected as producer_record_kind_not_allowed at admission
        # despite its envelope declaring the dimension open. The marker rides
        # output_scope_summary.write_envelope, which this function preserves.
        unbounded = set(
            (
                (filtered.get("output_scope_summary") or {})
                .get("write_envelope") or {}
            ).get("unbounded") or ()
        )
        # Collapsing to {} would be worse than dropping the kind: admission
        # refuses an empty signature as producer_signature_missing BEFORE it
        # ever consults the unbounded dimensions, so a producer whose only
        # declared surface is unbounded — or one whose every clause was
        # filtered out as not-in-this-catalog, which is the graceful
        # degradation this function exists to provide — would be rejected
        # outright with its declaration powerless to save it.
        filtered["output_signature"] = (
            signature if (edges or nodes or unbounded) else {}
        )
        kinds: list[str] = []
        if nodes or "nodes" in unbounded:
            kinds.append("node")
        if edges or "edges" in unbounded:
            kinds.append("edge")
        filtered["allowed_record_kinds"] = kinds
        filtered["policy_clauses"] = [
            dict(clause)
            for clause in filtered.get("policy_clauses") or []
            if (
                _known_triple((clause or {}).get("triple"))
                or (
                    (clause or {}).get("record_kind") == "node"
                    and str((clause or {}).get("subtype")) in loaded
                )
            )
        ]
        scope = dict(filtered.get("output_scope_summary") or {})
        if "edges" in scope:
            scope["edges"] = [
                list(edge)
                for edge in scope.get("edges") or []
                if _known_triple(edge)
            ]
        filtered["output_scope_summary"] = scope
        return filtered

    @staticmethod
    def _compat_derived_policy(source: str) -> dict[str, Any]:
        return {
            "producer_id": source,
            "producer_kind": (
                "extractor" if source.startswith("_extractor:") else
                "enricher"
            ),
            "trust_label": "derived-compat",
            "admission_mode": "fast_track",
            "authority_scope": {"source_family": source},
            "allowed_record_kinds": [],
            "output_signature": {},
            "policy_clauses": [],
            "output_scope_summary": {
                "summary": (
                    "compatibility derived profile; add authority "
                    "signature for enforceable derived output"
                ),
            },
            "source_ref": {"source": source},
        }

    def _emit_derived_verdict_event(self, payload: dict[str, Any]) -> None:
        event = dict(payload)
        self._derived_verdict_events.append(event)
        _emit_phase_event("derived_producer_verdict", event)

    def _emit_derived_verdict_rollup(self) -> None:
        if not self._derived_verdict_events:
            return
        _emit_phase_event("derived_producer_verdicts", {
            "count": len(self._derived_verdict_events),
            "events": [dict(item) for item in self._derived_verdict_events],
        })

    def _derived_rejection_summary(self) -> dict[str, Any]:
        """Counted census of derived producers rejected this cycle.

        A rejected derived producer does not change the cycle status — that
        contract is pinned by the authority suite — so without a counted key
        of its own the only trace in the outcome is an untyped element of
        ``enrichers``, byte-indistinguishable from a producer that had
        nothing to emit. This is the surface an operator reads when the row
        counts did not move (PACT surface-derived-admission-rejections).

        Deliberately NOT ``envelope_violations``: that list is gated on
        ``ENVELOPE_VIOLATION_CODES`` (a reuse-poisoning subset that excludes
        the trust-lattice codes), so it is not a rejection census.

        Counts ``failed`` and ``quarantined`` alongside ``rejected``: the
        derived-fact loader excludes all three the same way
        (``status IN ('rejected', 'failed', 'quarantined')``), so all three
        discard the producer's whole output. Reporting only ``rejected``
        would leave an enricher that threw mid-emit reporting zero
        discards — the same silent hole this summary exists to close.
        """
        rejected = [
            event for event in self._derived_verdict_events
            if str(event.get("status")) in _DISCARDED_DERIVED_STATUSES
        ]
        producers = [
            {
                "producer_id": str(event.get("producer_id") or ""),
                "producer_kind": str(event.get("producer_kind") or ""),
                "source": str(event.get("source") or ""),
                "status": str(event.get("status") or ""),
                "code": str(event.get("reason") or ""),
                "discarded_record_count": int(event.get("record_count") or 0),
            }
            for event in rejected
        ]
        return {
            "rejected_producer_count": len(producers),
            "discarded_record_count": sum(
                item["discarded_record_count"] for item in producers
            ),
            "producers": producers,
        }

    @staticmethod
    def _enricher_checkpoint_receipts(
        *,
        enricher_runs: list[dict[str, Any]],
        projection_deltas: Mapping[str, Any],
        projection_delta_hints: tuple[Mapping[str, Any], ...] = (),
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        governance = enricher_governance_summary(enricher_runs)
        merged_deltas = merge_enricher_projection_deltas(
            projection_deltas,
            *projection_delta_hints,
            enricher_runs=enricher_runs,
        )
        return governance, merged_deltas

    def _run_lens_promotion_phase(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> dict[str, Any]:
        """Drain pending/accepted lens-promotion envelopes.

        Runs between ``reserve()`` and Phase A projection. Each
        ``producer_kind='lens'`` envelope whose status is ``pending``
        or ``accepted`` is applied via ``apply_promoted_overlay``:
        derived ``node_facts`` land in the current generation's
        projection window, the overlay log flips to ``promoted``, and
        the envelope transitions to ``published`` with no generation
        reference yet. The Chronos checkpoint id is linked by
        ``_finalize_lens_promotion_generation_refs`` after the checkpoint
        has been minted. Idempotent: re-running a cycle
        that already drained a promotion is a no-op (overlay rows
        already ``promoted``; ``node_facts.dedupe_key`` collides).
        """

        from okg.substrate.lenses import (
            apply_promoted_overlay,
            list_pending_lens_envelopes,
        )

        workflow_ref = workflow_ref_from_dbos("okg_publisher_cycle")
        # AKMON Phase B.2 quarantine routing: only drain envelopes
        # whose target_branch_name matches this cycle's branch.
        # `default` accepts both NULL and explicit 'default'; non-
        # default branches match exactly. See
        # `okg.substrate.lenses.promote.list_pending_lens_envelopes`.
        pending = list_pending_lens_envelopes(
            conn,
            deployment_name=self.deployment_name,
            branch_name=self.branch_name,
        )
        applied = 0
        nodes_added = 0
        edges_added = 0
        for admission_id in pending:
            try:
                outcome = apply_promoted_overlay(
                    conn,
                    admission_id=admission_id,
                    cycle_token=str(generation_id),
                    workflow_ref=workflow_ref,
                )
            except Exception as exc:  # pragma: no cover - audit path
                logging.getLogger(__name__).exception(
                    "lens_promotion_apply_failed admission_id=%s: %s",
                    admission_id, exc,
                )
                continue
            applied += 1
            nodes_added += outcome.nodes_added
            edges_added += outcome.edges_added
            self._emit_derived_verdict_event({
                "generation_id": generation_id,
                "producer_id": outcome.producer_id,
                "producer_kind": "lens",
                "source": outcome.producer_id,
                "status": outcome.status,
                "reason": "lens_promotion_applied",
                "record_count": outcome.nodes_added + outcome.edges_added,
            })
        return {
            "applied": applied,
            "nodes_added": nodes_added,
            "edges_added": edges_added,
        }

    def _has_pending_lens_promotions(
        self,
        conn: psycopg.Connection,
    ) -> bool:
        """Return true when this branch has lens envelopes to drain."""

        from okg.substrate.lenses import list_pending_lens_envelopes

        return bool(list_pending_lens_envelopes(
            conn,
            deployment_name=self.deployment_name,
            branch_name=self.branch_name,
        ))

    def _finalize_lens_promotion_generation_refs(
        self,
        conn: psycopg.Connection,
        *,
        cycle_token: str,
        generation_ref: str,
        generation_status: str,
    ) -> int:
        """Link lens-promotion envelopes to the minted Chronos generation.

        The drain runs before the checkpoint is minted, so it leaves
        ``target_generation_ref`` NULL; this pass fills it in once the ref
        exists. Correlation used to run through the BIGINT compat token stored
        in a dedicated column; that column is gone in v1.0 (D2), so the drain
        now stamps the same token into the envelope's verdict JSONB and this
        pass matches on it.

        Matching the token rather than "any still-unlinked envelope on this
        deployment+branch" is load-bearing: a cycle that mints its checkpoint
        and then crashes before finalizing leaves its envelopes unlinked, and
        the broader predicate would let the NEXT cycle adopt them and attribute
        them to the wrong generation. The second arm only repairs status drift
        on envelopes already linked to *this* generation, so an envelope is
        never re-pointed from one generation to another.
        """

        branch_filter = (
            "AND (target_branch_name IS NULL OR target_branch_name = 'default')"
            if self.branch_name == "default"
            else "AND target_branch_name = %(branch_name)s"
        )
        workflow_ref = workflow_ref_from_dbos("okg_publisher_cycle")
        final_envelope_status = (
            "blocked" if generation_status == "blocked" else "published"
        )
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE okg.admission_envelopes e
                   SET status = %(final_envelope_status)s,
                       target_generation_ref = %(generation_ref)s,
                       verdict = COALESCE(e.verdict, '{{}}'::jsonb)
                       || jsonb_build_object(
                         'applied',
                         COALESCE(e.verdict->'applied', '{{}}'::jsonb)
                         || jsonb_build_object(
                           'target_generation_ref',
                           %(generation_ref)s::text,
                           'target_generation_status',
                           %(generation_status)s::text
                         )
                       ),
                       published_at = CASE
                         WHEN %(final_envelope_status)s = 'blocked'
                         THEN NULL
                         ELSE e.published_at
                       END,
                       updated_at = now()
                 WHERE e.deployment_name = %(deployment_name)s
                   AND e.producer_kind = 'lens'
                   AND e.status IN ('published', 'blocked')
                   AND e.verdict->'applied'->>'cycle_token'
                       = %(cycle_token)s
                   AND (
                     e.target_generation_ref IS NULL
                     OR (
                       e.target_generation_ref = %(generation_ref)s
                       AND e.status != %(final_envelope_status)s
                     )
                   )
                   {branch_filter}
                RETURNING e.admission_id::text
                """,
                {
                    "deployment_name": self.deployment_name,
                    "branch_name": self.branch_name,
                    "cycle_token": cycle_token,
                    "generation_ref": generation_ref,
                    "generation_status": generation_status,
                    "final_envelope_status": final_envelope_status,
                },
            )
            rows = [str(row[0]) for row in cur.fetchall()]
        for admission_id in rows:
            record_event(
                conn,
                admission_id=admission_id,
                event_type=(
                    "blocked"
                    if final_envelope_status == "blocked"
                    else "refs_attached"
                ),
                actor="okg_publisher_cycle",
                reason="lens_promotion_generation_ref_finalized",
                payload={
                    "status": final_envelope_status,
                    "generation_status": generation_status,
                    "target_generation_ref": generation_ref,
                    "producer_kind": "lens",
                },
                workflow_ref=workflow_ref,
            )
        return len(rows)

    @_within_enrichment_witness
    def _run_enrichers(
        self,
        conn: psycopg.Connection,
        generation_id: int,
        *,
        catalog_version_id: int,
        previous: Any | None = None,
        chronos_dirty_scope: _ChronosDirtyScope | None | object = _UNSET,
        lineage_invalidations: Any = _UNSET,
        publisher_timings_ms: dict[str, Any] | None = None,
        force_enrichers: bool = False,
        compat_generation_id: int | None = None,
        caller_owned_transaction: bool = False,
        prelock_state: Any | None = None,
    ) -> list[dict[str, Any]]:
        """Invoke each registered enricher in registration order
        with per-enricher `IncrementalContext`.

        Per `add-substrate-incremental-publish` (2026-04-28):
        before each enricher invocation the publisher reads
        (or initializes) the enricher's
        `okg.enricher_progress(deployment_name, enricher_name)` row,
        computes its `dirty_node_ids` per the spec contract
        (drawn from `node_facts` past the prior watermark on
        normal runs; from the graph checkout on first-run /
        forced-rescan / stale-watermark cases), and passes an
        `IncrementalContext` to `enrich(...)`. After a
        successful return the watermarks advance to current
        `max(fact_id)`; on exception they stay put and
        `last_status` is set to `'failed:<exc>'` so the next
        publish retries the enricher on the same delta.

        Each runs against the publisher's own connection (not
        a fresh one) so emitted edge_facts share the cycle's
        transactional view. A failing enricher's exception is
        logged + swallowed; subsequent enrichers still run
        and the failed enricher's watermark is NOT advanced.
        """
        log = logging.getLogger(__name__)
        progress_generation_id = (
            generation_id
            if compat_generation_id is None
            else compat_generation_id
        )
        if lineage_invalidations is _UNSET:
            lineage_invalidations = self.enricher_lineage_invalidations_hint
        # Commit-witness (PACT route-all-writers, task 2): ledger the enrichment
        # program once, then pin its checksum into the setting the fact-table
        # column default reads. Every node_facts/edge_facts INSERT an enricher or
        # linker emits on this connection (the dozen-plus inline sites) auto-stamps
        # source_program_checksum -- no per-site SQL surgery. The pin is
        # session-scoped on an autocommit connection and transaction-scoped
        # inside a caller's transaction (issue #2965). Re-pinned each iteration
        # because a commit or rollback inside an enricher ends a
        # transaction-scoped pin; `_run_enrichers` clears it when the stage
        # ends.
        runs: list[dict[str, Any]] = []
        setup_timings: dict[str, float] = {}
        with _publisher_timed_stage(
            setup_timings,
            "persist_transform_program",
        ):
            persist_transform_program(conn, _ENRICHMENT_PROGRAM)
        enricher_names = [
            str(getattr(enricher, "name", enricher.__class__.__name__))
            for enricher in self.enrichers
        ]
        producer_ids_by_name = {
            ename: f"_enricher:{ename}" for ename in enricher_names
        }
        with _publisher_timed_stage(
            setup_timings,
            "load_progress_rows",
        ):
            progress_rows = self._load_enricher_progress_rows(
                conn,
                enricher_names,
            )
        with _publisher_timed_stage(
            setup_timings,
            "cycle_fact_watermarks",
        ):
            current_fact_watermark, current_edge_watermark = (
                self._current_fact_watermarks(conn)
            )
        with _publisher_timed_stage(
            setup_timings,
            "cycle_catalog_version",
        ):
            current_catalog_version_id = self._current_catalog_version_id(
                conn
            )
        with _publisher_timed_stage(
            setup_timings,
            "load_profile_fingerprints",
        ):
            profile_fingerprints = (
                self._load_enricher_profile_fingerprints(
                    conn,
                    producer_ids_by_name.values(),
                )
            )
        if chronos_dirty_scope is _UNSET:
            with _publisher_timed_stage(
                setup_timings,
                "chronos_dirty_scope",
            ):
                chronos_dirty_scope = (
                    self._query_dirty_from_chronos_current_segment_by_type(
                        conn,
                        branch_name=self.branch_name,
                        previous=previous,
                        timing_sink=setup_timings,
                        timing_prefix="chronos_dirty_scope.query",
                    )
                )
        else:
            setup_timings["chronos_dirty_scope_reused"] = 0.0
        with _publisher_timed_stage(
            setup_timings,
            "enricher_lineage_frontier",
        ):
            enricher_lineage_invalidations = (
                self._enricher_lineage_invalidations(
                    chronos_dirty_scope, lineage_invalidations,
                )
            )
        for enricher in self.enrichers:
            ename = getattr(
                enricher, "name", enricher.__class__.__name__,
            )
            producer_id = producer_ids_by_name[str(ename)]
            timings: dict[str, float] = {}
            status = "unknown"
            reason: str | None = None
            dirty_count: int | None = None
            dirty_edge_count: int | None = None
            result = None
            failure_repr: Optional[str] = None
            exception_class: str | None = None
            consecutive_failure_count = 0
            # Set when an enricher returns normally but did NOT fully
            # process its inputs (e.g. a per-item LLM call failed). Kept
            # SEPARATE from failure_repr: the good outputs still admit
            # this cycle (admission uses failure_repr), but the watermark
            # must hold (progress uses failure_repr or incomplete_repr).
            incomplete_repr: Optional[str] = None
            profile_fingerprint = None
            lineage_plan = None
            lineage_status: str | None = None
            lineage_fallback_reason: str | None = None
            lineage_force_full = False
            lineage_envelope_payload: dict[str, Any] | None = None
            governance_record: dict[str, Any] | None = None
            governance_status: str | None = None
            registry_row_payload: dict[str, Any] | None = None
            try:
                with _publisher_timed_stage(timings, "set_program_checksum"):
                    pin_commit_witness(conn, _ENRICHMENT_CHECKSUM)
                with _publisher_timed_stage(
                        timings,
                        "incremental_context",
                    ):
                    ctx, declaration, lineage_plan = self._enricher_context(
                        conn,
                        enricher,
                        progress_row=progress_rows.get(str(ename)),
                        current_fact_watermark=current_fact_watermark,
                        current_edge_watermark=current_edge_watermark,
                        current_catalog_version_id=current_catalog_version_id,
                        chronos_dirty_scope=chronos_dirty_scope,
                        enricher_lineage_invalidations=(
                            enricher_lineage_invalidations
                        ),
                        force_enrichers=force_enrichers,
                    )
                    lineage_status = lineage_plan.lineage_status
                    lineage_fallback_reason = lineage_plan.fallback_reason
                    lineage_force_full = lineage_plan.force_full
                dirty_count = len(ctx.dirty_node_ids)
                dirty_edge_count = len(ctx.dirty_edge_ids)
                # Issue #1528 (follow-up to #1330): withhold an enricher whose
                # declared INPUT source run is still held off `default`. The
                # derived-fact loader already drops a quarantined producer's own
                # output; without this the transitive case survives -- the
                # enricher reads a source whose rows live only on the
                # `quarantine:*` branch and emits edges against nodes that were
                # never admitted (#1330: 111,940 records, one of them an edge to
                # a nonexistent `tool_use:codex:...` node).
                #
                # Checked BEFORE the clean-skip test so the quarantine reason is
                # the one recorded, and the watermark is HELD rather than
                # advanced. Held with the same discipline as a failed enricher:
                # the next publish re-runs it over the same delta once the held
                # run is approved. It is NOT counted as a failure -- nothing
                # went wrong, the input is simply not admitted yet.
                #
                # LIMIT, stated deliberately: only an enricher that declares
                # `reads_sources` / `reads_source_ids` is gated. An enricher with
                # no source declaration is invisible to this check and still
                # runs. Declaring inputs is what buys the protection.
                #
                # Issue #1528 F5: resolve the declaration against the source
                # registry FIRST. `reads_sources` is free-form text while
                # ingest writes `admission_envelopes.source =
                # entry.ownership_id` (`ingest/service.py:2914`), and registry
                # name != ownership_id for real sources (a `code_repos` block
                # generates `git-files-<slug>` owned by `<slug>.git-files`).
                # An id that resolves to nothing matches zero envelopes, so
                # the gate below would pass VACUOUSLY and the enricher would
                # ship ungated with no signal -- the exact failure mode this
                # change exists to close. Refuse instead.
                with _publisher_timed_stage(timings, "declared_source_check"):
                    alias_index = self._source_alias_index()
                    lookup_sources: list[str] | None = None
                    unknown_sources: tuple[str, ...] = ()
                    if declaration.source_ids and alias_index is not None:
                        resolved, unknown_sources = (
                            resolve_declared_source_ids(
                                declaration, alias_index,
                            )
                        )
                        lookup_sources = list(resolved)
                if unknown_sources:
                    log.warning(
                        "enricher %s withheld "
                        "[declares unregistered input source: %s]",
                        ename,
                        ", ".join(unknown_sources),
                    )
                    self._emit_derived_verdict_event({
                        "generation_id": generation_id,
                        "producer_id": producer_id,
                        "producer_kind": "enricher",
                        "source": producer_id,
                        "status": "withheld",
                        "reason": UNKNOWN_DECLARED_SOURCE,
                        "unknown_declared_sources": list(unknown_sources),
                        "record_count": 0,
                        "profile_fingerprint": profile_fingerprints.get(
                            producer_id
                        ),
                        "profile_status": "withheld",
                        "compatibility": False,
                        "producer_policy": {
                            "profile_fingerprint": profile_fingerprints.get(
                                producer_id
                            ),
                            "status": "withheld",
                        },
                    })
                    with _publisher_timed_stage(timings, "hold_progress"):
                        consecutive_failure_count = (
                            self._advance_enricher_progress(
                                conn, ename,
                                success=False,
                                failure_repr=unknown_declared_source_status(
                                    unknown_sources
                                ),
                                generation_id=progress_generation_id,
                                catalog_version_id=catalog_version_id,
                                emitted_count=0,
                                dormant_ok=bool(
                                    getattr(enricher, "dormant_ok", False)
                                ),
                                counts_as_failure=False,
                            ) or 0
                        )
                    status = "withheld"
                    reason = UNKNOWN_DECLARED_SOURCE
                    continue
                with _publisher_timed_stage(timings, "held_source_check"):
                    withheld_sources = held_input_sources(
                        conn,
                        declaration,
                        deployment_name=self.deployment_name,
                        source_ids=lookup_sources,
                    )
                if withheld_sources:
                    log.warning(
                        "enricher %s withheld "
                        "[input source held for admission: %s]",
                        ename,
                        ", ".join(withheld_sources),
                    )
                    self._emit_derived_verdict_event({
                        "generation_id": generation_id,
                        "producer_id": producer_id,
                        "producer_kind": "enricher",
                        "source": producer_id,
                        "status": "withheld",
                        "reason": WITHHELD_INPUT_SOURCE_HELD,
                        "withheld_input_sources": list(withheld_sources),
                        "record_count": 0,
                        "profile_fingerprint": profile_fingerprints.get(
                            producer_id
                        ),
                        "profile_status": "withheld",
                        "compatibility": False,
                        "producer_policy": {
                            "profile_fingerprint": profile_fingerprints.get(
                                producer_id
                            ),
                            "status": "withheld",
                        },
                    })
                    with _publisher_timed_stage(
                        timings,
                        "hold_progress",
                    ):
                        consecutive_failure_count = (
                            self._advance_enricher_progress(
                                conn, ename,
                                success=False,
                                failure_repr=withheld_input_source_status(
                                    withheld_sources
                                ),
                                generation_id=progress_generation_id,
                                catalog_version_id=catalog_version_id,
                                # Never ran: no new output to accumulate.
                                emitted_count=0,
                                dormant_ok=bool(
                                    getattr(enricher, "dormant_ok", False)
                                ),
                                counts_as_failure=False,
                            ) or 0
                        )
                    status = "withheld"
                    reason = WITHHELD_INPUT_SOURCE_HELD
                    continue
                with _publisher_timed_stage(timings, "skip_check"):
                    skip_clean = self._should_skip_clean_enricher(
                        conn,
                        enricher,
                        ctx,
                        catalog_version_id=catalog_version_id,
                        caller_owned_transaction=caller_owned_transaction,
                    )
                if skip_clean:
                    with _publisher_timed_stage(
                        timings,
                        "profile_lookup",
                    ):
                        profile_fingerprint = profile_fingerprints.get(
                            producer_id
                        )
                    log.info(
                        "enricher %s skipped "
                        "[clean incremental input, catalog=%s]",
                        ename,
                        catalog_version_id,
                    )
                    self._emit_derived_verdict_event({
                        "generation_id": generation_id,
                        "producer_id": producer_id,
                        "producer_kind": "enricher",
                        "source": producer_id,
                        "status": "skipped",
                        "reason": "enricher_skip_when_clean",
                        "record_count": 0,
                        "profile_fingerprint": profile_fingerprint,
                        "profile_status": "skipped",
                        "compatibility": False,
                        "producer_policy": {
                            "profile_fingerprint": profile_fingerprint,
                            "status": "skipped",
                        },
                    })
                    with _publisher_timed_stage(
                        timings,
                        "advance_progress",
                    ):
                        consecutive_failure_count = (
                            self._advance_enricher_progress(
                                conn, ename,
                                success=True,
                                failure_repr=None,
                                generation_id=progress_generation_id,
                                catalog_version_id=catalog_version_id,
                                current_fact_watermark=current_fact_watermark,
                                current_edge_watermark=current_edge_watermark,
                                # Clean skip: no run, no new emissions (adds 0).
                                emitted_count=0,
                                dormant_ok=bool(
                                    getattr(enricher, "dormant_ok", False)
                                ),
                            ) or 0
                        )
                    status = "skipped"
                    reason = "enricher_skip_when_clean"
                    continue
                with _publisher_timed_stage(
                    timings,
                    "fact_watermarks_before",
                ):
                    node_before, edge_before = self._current_fact_watermarks(conn)
                with _publisher_timed_stage(
                    timings,
                    "profile_materialize",
                ):
                    expected_fingerprint = (
                        self._expected_enricher_profile_fingerprint(
                            conn,
                            enricher=enricher,
                            producer_id=producer_id,
                            catalog_version_id=catalog_version_id,
                        )
                    )
                    cached_fingerprint = profile_fingerprints.get(producer_id)
                    if cached_fingerprint == expected_fingerprint:
                        profile_fingerprint = cached_fingerprint
                        timings["profile_reuse"] = 0.0
                    else:
                        profile_fingerprint = self._materialize_enricher_profile(
                            conn,
                            enricher=enricher,
                            producer_id=producer_id,
                            catalog_version_id=catalog_version_id,
                        )
                invocation_savepoint: str | None = None
                invocation_transaction = nullcontext()
                if caller_owned_transaction:
                    invocation_transaction = self._enricher_lifecycle_guard(
                        conn,
                        caller_owned_transaction=True,
                        enricher_name=str(ename),
                        hook="enrich",
                    )
                elif getattr(conn, "autocommit", True) is False:
                    # Use SQL savepoints instead of ``conn.transaction()``:
                    # existing enrichers may perform Chronos checkouts whose
                    # adapter calls ``commit()``, which psycopg forbids inside
                    # a Transaction context. On success the savepoint may be
                    # released by such a commit; otherwise the cycle's outer
                    # connection commit releases it.
                    invocation_savepoint = (
                        f"okg_enricher_invocation_{uuid.uuid4().hex}"
                    )
                    with conn.cursor() as cur:
                        cur.execute(
                            sql.SQL("SAVEPOINT {}").format(
                                sql.Identifier(invocation_savepoint)
                            )
                        )
                try:
                    stage_name = f"enricher.{ename}"
                    set_publisher_context = getattr(
                        enricher,
                        "_set_publisher_context",
                        None,
                    )
                    if callable(set_publisher_context):
                        with self._enricher_lifecycle_guard(
                            conn,
                            caller_owned_transaction=caller_owned_transaction,
                            enricher_name=str(ename),
                            hook="_set_publisher_context",
                        ):
                            set_publisher_context(
                                deployment_name=self.deployment_name,
                                branch_name=self.branch_name,
                            )
                    # Issue 2941 fix 1: a plan computed before the lock is
                    # used only for the very inputs it was computed from.
                    plan_kwargs: dict[str, Any] = {}
                    if prelock_state is not None:
                        plan = prelock_state.plan_for(str(ename))
                        if plan is not None:
                            if (
                                plan.incremental == ctx
                                and plan.generation_id == generation_id
                            ):
                                plan_kwargs["prelock_plan"] = plan
                                prelock_state.note_enricher(
                                    str(ename), used=True,
                                )
                            else:
                                prelock_state.note_enricher(
                                    str(ename),
                                    used=False,
                                    fallback="incremental_context_changed",
                                )
                    with _publisher_timed_stage(timings, "enrich"):
                        with invocation_transaction:
                            with profile_stage(stage_name):
                                result = enricher.enrich(
                                    conn,
                                    generation_id=generation_id,
                                    incremental=ctx,
                                    **plan_kwargs,
                                )
                    result_timings = _publisher_numeric_timings(
                        getattr(result, "timings_ms", None),
                    )
                    if plan_kwargs and prelock_state is not None:
                        # The plan's rows were a forecast; the linker ran
                        # its "already said this" check again under the
                        # lock and inserted what that check returned.
                        prelock_state.note_enricher(
                            str(ename),
                            dedupe_recheck=(
                                "changed"
                                if result_timings.get("prelock.dedupe_changed")
                                else "unchanged"
                            ),
                            inserted=int(result.n_edges_emitted),
                        )
                    for label, elapsed_ms in result_timings.items():
                        if _publisher_timing_detail_is_duration(label):
                            timings[f"enrich.{label}"] = round(
                                float(elapsed_ms),
                                3,
                            )
                    log.info(
                        "enricher %s emitted %d edges (skipped=%d) "
                        "[is_first_run=%s, dirty=%d]",
                        result.enricher_name,
                        result.n_edges_emitted,
                        result.n_edges_skipped,
                        ctx.is_first_run,
                        len(ctx.dirty_node_ids),
                    )
                    status = "ok"
                    reason = "enricher_completed"
                    # The enricher returned without raising, but may have
                    # left inputs unprocessed (a per-item LLM call failed).
                    # Its good outputs DID emit and must still admit this
                    # cycle, but the watermark must NOT advance past the
                    # failed items or their typed nodes are silently,
                    # permanently lost (P0). Hold the watermark by recording
                    # a non-success progress status; next cycle re-runs the
                    # enricher (last_status != 'success' => is_first_run)
                    # and retries them.
                    if getattr(result, "incomplete", False):
                        # `incomplete_repr` becomes enricher_progress.
                        # last_status, which is constrained to
                        # ('never','success') OR LIKE 'failed:%'
                        # (enricher_progress_last_status_chk). Use the
                        # 'failed:' namespace so the row is valid AND the
                        # existing retry machinery engages (is_first_run,
                        # and no clean-skip while last_status != 'success').
                        # The human-readable detail lives in `reason`,
                        # result.error, and the log line below.
                        incomplete_repr = "failed:incomplete_inputs"
                        exception_class = "incomplete_inputs"
                        detail = getattr(result, "error", None) or incomplete_repr
                        status = "partial"
                        reason = detail
                        log.warning(
                            "enricher %s completed but did NOT fully "
                            "process its inputs (%s); holding its watermark "
                            "so the unprocessed items retry next cycle "
                            "(no silent data loss)",
                            ename, detail,
                        )
                except Exception as exc:
                    finish_publisher_transaction = getattr(
                        enricher,
                        "_finish_publisher_transaction",
                        None,
                    )
                    if callable(finish_publisher_transaction):
                        with self._enricher_lifecycle_guard(
                            conn,
                            caller_owned_transaction=caller_owned_transaction,
                            enricher_name=str(ename),
                            hook="_finish_publisher_transaction(commit=False)",
                        ):
                            finish_publisher_transaction(conn, commit=False)
                    if invocation_savepoint is not None:
                        try:
                            with conn.cursor() as cur:
                                cur.execute(
                                    sql.SQL("ROLLBACK TO SAVEPOINT {}").format(
                                        sql.Identifier(invocation_savepoint)
                                    )
                                )
                                cur.execute(
                                    sql.SQL("RELEASE SAVEPOINT {}").format(
                                        sql.Identifier(invocation_savepoint)
                                    )
                                )
                        except psycopg.Error:
                            # Some graph-store reads intentionally commit their
                            # own checkout transaction, which also releases our
                            # savepoint. If a later statement then fails, only
                            # that post-checkout transaction can be rolled back.
                            conn.rollback()
                    exception_class = type(exc).__name__
                    failure_repr = f"failed:{exception_class}"
                    status = "failed"
                    reason = failure_repr
                    log.exception(
                        "enricher %r raised; continuing without "
                        "blocking the publish",
                        ename,
                    )
                with _publisher_timed_stage(timings, "admit_outputs"):
                    self._admit_enricher_outputs(
                        conn,
                        enricher=enricher,
                        generation_id=generation_id,
                        catalog_version_id=catalog_version_id,
                        prev_edge_fact_watermark=edge_before,
                        prev_node_fact_watermark=node_before,
                        # Only a raised enricher fails admission of its
                        # output range; an incomplete-but-returned run keeps
                        # failure_reason=None so its GOOD outputs still admit.
                        failure_reason=failure_repr,
                        profile_already_materialized=True,
                    )
                with _publisher_timed_stage(timings, "advance_progress"):
                    consecutive_failure_count = (
                        self._advance_enricher_progress(
                        conn, ename,
                        # Advance only for a fully-complete run. A raised
                        # (result is None) OR incomplete run holds the
                        # watermark so the next cycle retries.
                        success=self._enricher_run_is_complete(result),
                        failure_repr=failure_repr or incomplete_repr,
                        generation_id=progress_generation_id,
                        catalog_version_id=catalog_version_id,
                        # P4 (specify-publish-commit-protocol): pin the
                        # advance to the enricher-phase-start bounds
                        # captured before ANY enricher ran, exactly as the
                        # clean-skip path above already does. Passing no
                        # bounds lets `_advance_enricher_progress` read the
                        # LIVE max(fact_id) at advance time, which includes
                        # an earlier sibling enricher's same-cycle edge
                        # emissions -- facts this run's dirty scope never
                        # covered -- and silently drops them from next
                        # cycle's delta. The #679 completeness hold (above)
                        # decides WHETHER to advance; this bound decides
                        # WHERE TO.
                        current_fact_watermark=current_fact_watermark,
                        current_edge_watermark=current_edge_watermark,
                        # Feed this cycle's real edge output into the
                        # lifetime accumulator so a wired-but-silent
                        # enricher (#707) never leaves 0. A raised run
                        # (result is None) emitted nothing.
                        emitted_count=(
                            int(result.n_edges_emitted)
                            if result is not None else 0
                        ),
                        dormant_ok=bool(
                            getattr(enricher, "dormant_ok", False)
                        ),
                        ) or 0
                    )
                finish_publisher_transaction = getattr(
                    enricher,
                    "_finish_publisher_transaction",
                    None,
                )
                if callable(finish_publisher_transaction):
                    # For shared model-backed extractors this is the only
                    # commit from item reservation through output admission
                    # and progress. Terminal quarantine cannot survive
                    # without the matching successful watermark advance.
                    with self._enricher_lifecycle_guard(
                        conn,
                        caller_owned_transaction=caller_owned_transaction,
                        enricher_name=str(ename),
                        hook="_finish_publisher_transaction(commit=True)",
                    ):
                        finish_publisher_transaction(conn, commit=True)
            except Exception:  # noqa: BLE001 — profile materialization failure should still block
                finish_publisher_transaction = getattr(
                    enricher,
                    "_finish_publisher_transaction",
                    None,
                )
                if callable(finish_publisher_transaction):
                    with self._enricher_lifecycle_guard(
                        conn,
                        caller_owned_transaction=caller_owned_transaction,
                        enricher_name=str(ename),
                        hook="_finish_publisher_transaction(commit=False)",
                    ):
                        finish_publisher_transaction(conn, commit=False)
                log.exception(
                    "enricher %r producer profile materialization failed",
                    ename,
                )
                raise
            finally:
                if lineage_plan is not None:
                    registry_row_payload = (
                        validate_enricher_judgment_registration(enricher)
                    )
                    emitted = (
                        int(result.n_edges_emitted)
                        if result is not None else 0
                    )
                    skipped = (
                        int(result.n_edges_skipped)
                        if result is not None else 0
                    )
                    lineage_envelope_payload = (
                        build_enricher_run_lineage_envelope(
                            enricher,
                            generation_id=generation_id,
                            catalog_version_id=catalog_version_id,
                            plan=lineage_plan,
                            n_edges_emitted=emitted,
                            n_edges_skipped=skipped,
                        ).to_dict()
                    )
                    if (
                        result is not None
                        and getattr(result, "output_lineage", None) is None
                        and hasattr(result, "__dict__")
                    ):
                        result.output_lineage = lineage_envelope_payload
                    if failure_repr is not None:
                        governance_record = record_enricher_governance_failure(
                            enricher_name=str(ename),
                            generation_id=generation_id,
                            failure_reason=failure_repr,
                            lineage_envelope=lineage_envelope_payload,
                            registry_row=registry_row_payload,
                        )
                    elif result is not None:
                        governance_record = record_enricher_governance_available(
                            enricher_name=str(ename),
                            generation_id=generation_id,
                            n_edges_emitted=emitted,
                            n_edges_skipped=skipped,
                            lineage_envelope=lineage_envelope_payload,
                            registry_row=registry_row_payload,
                        )
                    if governance_record is not None:
                        governance_status = str(
                            governance_record.get("governance_status")
                        )
                timings["total"] = round(sum(timings.values()), 3)
                runs.append({
                    "name": ename,
                    "producer_id": producer_id,
                    "status": status,
                    "reason": reason,
                    "exception_class": exception_class,
                    "consecutive_failure_count": consecutive_failure_count,
                    "compat_generation_id": progress_generation_id,
                    "profile_fingerprint": profile_fingerprint,
                    "dirty_node_count": dirty_count,
                    "dirty_edge_count": dirty_edge_count,
                    "lineage_status": lineage_status,
                    "fallback_reason": lineage_fallback_reason,
                    "force_full": lineage_force_full,
                    "lineage_envelope": lineage_envelope_payload,
                    "governance_status": governance_status,
                    "governance_record": governance_record,
                    "n_edges_emitted": (
                        int(result.n_edges_emitted)
                        if result is not None else 0
                    ),
                    "n_edges_skipped": (
                        int(result.n_edges_skipped)
                        if result is not None else 0
                    ),
                    "timings_ms": timings,
                })
        if setup_timings:
            runs.insert(0, {
                "name": "_enricher_setup",
                "producer_id": "_enricher_setup",
                "status": "ok",
                "reason": "enricher_setup",
                "exception_class": None,
                "consecutive_failure_count": 0,
                "compat_generation_id": progress_generation_id,
                "profile_fingerprint": None,
                "dirty_node_count": None,
                "dirty_edge_count": None,
                "lineage_status": None,
                "fallback_reason": None,
                "force_full": False,
                "lineage_envelope": None,
                "governance_status": None,
                "governance_record": None,
                "n_edges_emitted": 0,
                "n_edges_skipped": 0,
                "timings_ms": {
                    **setup_timings,
                    "total": round(sum(setup_timings.values()), 3),
                },
            })
        if publisher_timings_ms is not None:
            for run in runs:
                _add_named_timing_details(
                    publisher_timings_ms,
                    prefix="enrichers",
                    name=run.get("name"),
                    values=run.get("timings_ms"),
                )
        return runs

    def _enricher_lineage_invalidations(
        self,
        chronos_dirty_scope: Any,
        lineage_invalidations: Any,
    ) -> Any:
        """The lineage invalidations every enricher's context is planned with."""
        graph_lineage_invalidations = invalidations_from_chronos_dirty_scope(
            chronos_dirty_scope if chronos_dirty_scope is not _UNSET else None
        )
        if lineage_invalidations is _UNSET:
            return graph_lineage_invalidations
        if graph_lineage_invalidations is None or lineage_invalidations is None:
            return None
        return (
            *graph_lineage_invalidations,
            *tuple(lineage_invalidations or ()),
        )

    def _enricher_context(
        self,
        conn: psycopg.Connection,
        enricher: Any,
        *,
        progress_row: Any,
        current_fact_watermark: int,
        current_edge_watermark: int,
        current_catalog_version_id: int | None,
        chronos_dirty_scope: Any,
        enricher_lineage_invalidations: Any,
        force_enrichers: bool,
    ) -> tuple[IncrementalContext, Any, Any]:
        """One enricher's ``IncrementalContext``, declaration and lineage plan.

        Shared by the enricher stage and the pre-lock pass (issue 2941 fix
        1), so a plan made before the lock was made from a context built the
        same way as the one it is compared with under the lock.
        """
        reads_subtypes: tuple[str, ...] = tuple(
            getattr(enricher, "reads_subtypes", ()) or ()
        )
        reads_edge_types: tuple[str, ...] = tuple(
            getattr(enricher, "reads_edge_types", ()) or ()
        )
        chronos_dirty = (
            self._filter_chronos_dirty_scope(
                chronos_dirty_scope,
                reads_subtypes,
                reads_edge_types,
            )
            if chronos_dirty_scope is not None else None
        )
        ctx = self._build_incremental_ctx(
            conn,
            enricher,
            progress_row=progress_row,
            current_fact_watermark=current_fact_watermark,
            current_edge_watermark=current_edge_watermark,
            current_catalog_version_id=current_catalog_version_id,
            chronos_dirty=chronos_dirty,
        )
        declaration = declare_enricher_dependencies(enricher)
        lineage_plan = plan_enricher_lineage(
            declaration,
            enricher_lineage_invalidations,
        )
        lineage_dependency_requires_full = any(
            invalidation.dependency_kind in {
                "source_record",
                "source_content_hash",
                "component_hash",
            }
            for invalidation in lineage_plan.matched_invalidations
        )
        if lineage_plan.force_full or lineage_dependency_requires_full:
            ctx = replace(
                ctx,
                is_first_run=True,
                change_basis=(
                    lineage_plan.fallback_reason or "lineage_dependency"
                ),
            )
        elif (
            lineage_plan.dirty
            and not ctx.dirty_node_ids
            and not ctx.dirty_edge_ids
        ):
            ctx = replace(
                ctx,
                is_first_run=True,
                change_basis="lineage_dependency",
            )
        if force_enrichers:
            ctx = replace(
                ctx,
                is_first_run=True,
                change_basis="operator_forced",
            )
        return ctx, declaration, lineage_plan

    def _lookup_enricher_profile_fingerprint(
        self,
        conn: psycopg.Connection,
        *,
        producer_id: str,
    ) -> str | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT profile_fingerprint
                  FROM okg.producer_profiles
                 WHERE deployment_name = %s
                   AND producer_id = %s
                 ORDER BY updated_at DESC
                 LIMIT 1
                """,
                (self.deployment_name, producer_id),
            )
            row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else None

    @staticmethod
    def _load_unapplied_derived_facts(
        conn: psycopg.Connection,
        *,
        generation_id: int | None = None,
        min_node_fact_id: int | None = None,
        min_edge_fact_id: int | None = None,
        timings_ms: dict[str, float] | None = None,
        timing_prefix: str = "derived_fact_load",
        enforcement_mode: str = "enforce",
        group_by_producer: bool = False,
    ) -> list[Any]:
        """Load derived fact-log rows for Chronos-native checkpointing.

        ``enforcement_mode`` is the deployment's Nomos runtime-enforcement
        tri-state. Under ``enforce`` a derived fact (NODE and EDGE alike) only
        projects when an ``accepted``/``published`` admission envelope covers
        its fact-id range and, for nodes, the producing run is not
        rejected/failed/quarantined. Under ``audit`` (log-and-allow) or ``off``
        (boundary skipped) those gates would silently DROP the derivation from
        the served graph — inconsistent with the audit contract. So in
        audit/off BOTH predicates relax (nodes and edges load unconditionally);
        the admission envelopes are still written, they just don't gate.
        """
        from okg.substrate.library.sources.base import EdgeFact, NodeFact

        def _record_timing(name: str, started: float) -> None:
            if timings_ms is None:
                return
            elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
            key = f"{timing_prefix}.{name}"
            timings_ms[key] = round(timings_ms.get(key, 0.0) + elapsed, 3)

        facts: list[Any] = []
        grouped: dict[tuple[str, uuid.UUID], list[Any]] = {}

        def _record_fact(
            fact: Any,
            *,
            source: Any,
            run_id: Any,
        ) -> None:
            if not group_by_producer:
                facts.append(fact)
                return
            producer_id = str(source)
            run_uuid = (
                uuid.UUID(str(run_id))
                if run_id is not None
                else uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"okg:legacy-derived:{producer_id}",
                )
            )
            grouped.setdefault((producer_id, run_uuid), []).append(fact)
        generation_filter = (
            "AND source_revision->>'generation_id' = %(generation_id)s"
            if generation_id is not None
            else ""
        )
        node_watermark_filter = (
            "AND nf.fact_id > %(min_node_fact_id)s"
            if min_node_fact_id is not None
            else ""
        )
        edge_watermark_filter = (
            "AND ef.fact_id > %(min_edge_fact_id)s"
            if min_edge_fact_id is not None
            else ""
        )
        # Rejection safety (add-admission-envelope design).
        # Under ``audit`` (log-and-allow) or ``off`` (boundary skipped),
        # admission still evaluates + writes its envelopes but MUST NOT gate the
        # served graph — for NODES as well as edges. #572 relaxed only the EDGE
        # allow-list, leaving the node deny-list in place on the premise that
        # "derived nodes always project". That premise is false for a REJECTED
        # producer (e.g. producer_edge_triple_not_allowed, which any enricher
        # that emits typed edges without a declared output-signature hits): its
        # NODES get dropped too, leaving its unconditionally-projected edges
        # dangling — so the served graph silently loses the whole derivation. So
        # under audit/off relax BOTH: derived nodes and edges project
        # unconditionally.
        if enforcement_mode in ("audit", "off"):
            derived_node_predicate = """
                   nf.source LIKE '_enricher:%%'
                OR nf.source LIKE '_extractor:%%'
                OR nf.source LIKE '_lens:%%'
                OR nf.source LIKE 'derived_artifact:%%'
            """
            # Log-and-allow: derived edges project unconditionally (same shape
            # as derived_node_predicate). Admission still evaluates + writes
            # its envelopes; it just does not gate the served graph. Without this
            # an audit-mode deployment silently loses every enricher-emitted edge
            # (nodes stay, edges vanish) because their admission verdicts are not
            # accepted/published.
            derived_edge_predicate = """
                   ef.source LIKE '_enricher:%%'
                OR ef.source LIKE '_extractor:%%'
                OR ef.source LIKE '_lens:%%'
                OR ef.source LIKE 'derived_artifact:%%'
            """
        else:
            # Enforce: a derived producer receives its admission verdict AFTER
            # its facts are committed, and rejection performs no fact cleanup —
            # so the loader must exclude a rejected/failed/quarantined producer
            # run's facts. Edges are gated by the fact-range allow-list; nodes
            # carry no fact-range refs, so they get a deny-list keyed by the
            # envelope's (source, run_id). Runs without any envelope stay
            # loadable (governance is per-emission, not a precondition for
            # projection).
            derived_node_predicate = """
                (
                   nf.source LIKE '_enricher:%%'
                OR nf.source LIKE '_extractor:%%'
                OR nf.source LIKE '_lens:%%'
                OR nf.source LIKE 'derived_artifact:%%'
                )
                AND NOT EXISTS (
                      SELECT 1
                        FROM okg.admission_envelopes e
                       WHERE e.source = nf.source
                         AND e.run_id = nf.run_id
                         AND e.status IN ('rejected', 'failed', 'quarantined')
                )
            """
            derived_edge_predicate = """
                   ef.source LIKE 'derived_artifact:%%'
                OR ((ef.source LIKE '_enricher:%%'
                  OR ef.source LIKE '_extractor:%%'
                  OR ef.source LIKE '_lens:%%')
               AND EXISTS (
                     SELECT 1
                       FROM okg.admission_envelopes e
                       JOIN okg.admission_record_refs r
                         ON r.admission_id = e.admission_id
                      WHERE r.ref_kind = 'edge_fact_range'
                        AND r.source = ef.source
                        AND r.run_id::text = ef.run_id::text
                        AND ef.fact_id
                            BETWEEN r.fact_id_min AND r.fact_id_max
                        AND e.status IN ('accepted', 'published')
               ))
            """
        # Each query has one fact-id watermark, allowing the planner to use the
        # source/fact-id indexes without a heterogeneous OR predicate.
        node_branch_cols = (
            "node_id, subtype, attrs, source_record_id, source_revision, op, "
            "source, run_id, observed_at, fact_id"
        )
        edge_branch_cols = (
            "src, dst, edge_type, attrs, provenance, confidence, "
            "source_record_id, source_revision, op, source, run_id, "
            "observed_at, fact_id"
        )
        node_query = (
            "SELECT DISTINCT ON (node_id) node_id, subtype, attrs, "
            "source_record_id, source_revision, op, source, run_id FROM ("
            f"SELECT {node_branch_cols} FROM okg.node_facts AS nf "
            f"WHERE ({derived_node_predicate}) "
            f"{node_watermark_filter} {generation_filter}"
            ") AS nf ORDER BY node_id, observed_at DESC, fact_id DESC"
        )
        edge_query = (
            "SELECT DISTINCT ON (src, edge_type, dst) src, dst, edge_type, "
            "attrs, provenance, confidence, source_record_id, source_revision, "
            "op, source, run_id FROM ("
            f"SELECT {edge_branch_cols} FROM okg.edge_facts AS ef "
            f"WHERE ({derived_edge_predicate}) "
            f"{edge_watermark_filter} {generation_filter}"
            ") AS ef ORDER BY src, edge_type, dst, observed_at DESC, "
            "fact_id DESC"
        )
        params: dict[str, object] = {}
        if generation_id is not None:
            params["generation_id"] = str(generation_id)
        if min_node_fact_id is not None:
            params["min_node_fact_id"] = int(min_node_fact_id)
        if min_edge_fact_id is not None:
            params["min_edge_fact_id"] = int(min_edge_fact_id)
        with conn.cursor() as cur:
            _t0 = time.perf_counter()
            cur.execute(node_query, params)
            _record_timing("node_query", _t0)
            _t0 = time.perf_counter()
            for (
                node_id,
                subtype,
                attrs,
                source_record_id,
                source_revision,
                op,
                source,
                run_id,
            ) in cur.fetchall():
                _record_fact(NodeFact(
                    node_id=str(node_id),
                    subtype=str(subtype),
                    attrs=dict(attrs or {}),
                    source_record_id=dict(source_record_id or {}),
                    source_revision=dict(source_revision or {}),
                    op=str(op),
                    # Issue #3666: a link endpoint fills an absent id only;
                    # it never takes a row a real record owns.
                    ownership_mode=(
                        "substitute"
                        if str(source) == LINK_ENDPOINT_SOURCE and str(op) == "I"
                        else "authoritative"
                    ),
                ), source=source, run_id=run_id)
            _record_timing("node_hydrate", _t0)

            _t0 = time.perf_counter()
            cur.execute(edge_query, params)
            _record_timing("edge_query", _t0)
            _t0 = time.perf_counter()
            for (
                src,
                dst,
                edge_type,
                attrs,
                provenance,
                confidence,
                source_record_id,
                source_revision,
                op,
                source,
                run_id,
            ) in cur.fetchall():
                _record_fact(EdgeFact(
                    src=str(src),
                    dst=str(dst),
                    edge_type=str(edge_type),
                    attrs=dict(attrs or {}),
                    provenance=str(provenance),
                    confidence=float(confidence),
                    source_record_id=dict(source_record_id or {}),
                    source_revision=dict(source_revision or {}),
                    op=str(op),
                ), source=source, run_id=run_id)
            _record_timing("edge_hydrate", _t0)
        if group_by_producer:
            return [
                _DerivedFactBatch(
                    producer_id=producer_id,
                    producer_kind=_infer_derived_producer_kind(producer_id),
                    run_id=run_id,
                    facts=tuple(batch_facts),
                )
                for (producer_id, run_id), batch_facts in sorted(
                    grouped.items(),
                    key=lambda item: (item[0][0], str(item[0][1])),
                )
            ]
        return facts

    @staticmethod
    def _semantic_dirty_nodes_by_subtype(
        chronos_dirty_scope: _ChronosDirtyScope,
    ) -> dict[str, list[str]]:
        """Return dirty nodes excluding source-lineage-only attr rewrites."""

        nodes_by_subtype = chronos_dirty_scope[0] or {}
        node_attr_changes_by_subtype = (
            chronos_dirty_scope[3] if len(chronos_dirty_scope) > 3 else None
        )
        if node_attr_changes_by_subtype is None:
            return {
                str(subtype): list(node_ids)
                for subtype, node_ids in nodes_by_subtype.items()
            }

        from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

        semantic: dict[str, list[str]] = {}
        for subtype, node_ids in nodes_by_subtype.items():
            subtype_changes = node_attr_changes_by_subtype.get(subtype, {})
            kept: list[str] = []
            for node_id in node_ids:
                changed_fields = {
                    str(field)
                    for field in subtype_changes.get(str(node_id), ())
                }
                if changed_fields and changed_fields <= {LINEAGE_ATTR}:
                    continue
                kept.append(str(node_id))
            if kept:
                semantic[str(subtype)] = kept
        return semantic

    @staticmethod
    def _semantic_dirty_edges_by_type(
        chronos_dirty_scope: _ChronosDirtyScope,
    ) -> dict[str, list[int]]:
        """Return dirty edges visible to view/read invalidation logic."""

        edges_by_type = chronos_dirty_scope[1] or {}
        return {
            str(edge_type): list(dict.fromkeys(int(edge_id) for edge_id in edge_ids))
            for edge_type, edge_ids in edges_by_type.items()
            if edge_ids
        }

    def _view_change_frontier(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        chronos_dirty_scope: _ChronosDirtyScope | None,
        overlay_completed_watermark_at: datetime | None,
    ) -> tuple[Any, ...]:
        """Convert this cycle's dirty sources into view invalidations.

        The write-set (graph nodes written since the prior checkpoint) is the
        authority on what changed this cycle. The native-view stage asks the
        reverse-dependency lineage index whether any view's outputs are
        reachable from these changed nodes. Lens overlays are committed outside
        Chronos graph writes, so active overlay projection deltas committed since
        the last completed view refresh are appended here as the same
        reverse-index keys. An empty/absent scope yields only overlay
        invalidations, if any.

        `unify-dirty-set-authority`: only graph_node invalidations are built.
        View envelopes declare cited nodes, not cited edges, so a graph_edge
        invalidation would never match an envelope and feeding it to
        lookup_dirty_outputs would force every view full (the kind would read
        as "unavailable"). Edge-level view dependency is a follow-up that
        requires envelopes to declare cited edges.
        """
        from okg.substrate.derived_artifacts.change_frontier import (
            build_change_frontier,
        )

        graph_node_ids: list[str] = []
        if chronos_dirty_scope is not None:
            nodes_by_subtype = Publisher._semantic_dirty_nodes_by_subtype(
                chronos_dirty_scope,
            )
            graph_node_ids = [
                node_id
                for node_ids in nodes_by_subtype.values()
                for node_id in node_ids
            ]
        overlay_invalidations = self._lens_overlay_projection_delta_invalidations(
            conn,
            generation_id=generation_id,
            since=overlay_completed_watermark_at,
        )
        if not graph_node_ids and not overlay_invalidations:
            return ()
        return (
            *build_change_frontier(graph_node_ids=graph_node_ids).invalidations,
            *overlay_invalidations,
        )

    @staticmethod
    def _lens_overlay_projection_delta_invalidations(
        conn: psycopg.Connection,
        *,
        generation_id: int,
        since: datetime | None,
    ) -> tuple[Any, ...]:
        from okg.substrate.derived_artifacts.change_frontier import (
            invalidations_from_projection_deltas,
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT metadata->'projection_deltas'
                  FROM okg.lens_overlay_log
                 WHERE status = 'active'
                   AND target_generation_id <= %(generation_id)s
                   AND metadata ? 'projection_deltas'
                   AND (
                        %(since)s::timestamptz IS NULL
                        OR created_at > %(since)s::timestamptz
                   )
                 ORDER BY created_at, overlay_id
                """,
                {
                    "generation_id": generation_id,
                    "since": since,
                },
            )
            payloads = [row[0] for row in cur.fetchall()]
        invalidations: list[Any] = []
        for payload in payloads:
            if isinstance(payload, Mapping):
                invalidations.extend(invalidations_from_projection_deltas(payload))
            else:
                raise TypeError(
                    "lens overlay projection_deltas payload must be a mapping"
                )
        return tuple(invalidations)

    def _native_view_coalesce_decision(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        final_view_refresh: bool,
        force_native_view_refresh: bool,
        dirty_scope_unavailable: bool,
        latest_completed_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        """Return a coalescing diagnostic when this refresh may be delayed."""

        policy = self.native_view_refresh_policy
        if (
            force_native_view_refresh
            or dirty_scope_unavailable
            or not policy.enabled
            or not self.native_view_stage.materializers
        ):
            return None

        latest_age_ms: float | None = None
        if latest_completed_at is not None:
            if latest_completed_at.tzinfo is None:
                latest_completed_at = latest_completed_at.replace(
                    tzinfo=timezone.utc,
                )
            latest_age_ms = max(
                0.0,
                (
                    datetime.now(timezone.utc)
                    - latest_completed_at.astimezone(timezone.utc)
                ).total_seconds()
                * 1000.0,
            )
        if (
            policy.min_refresh_interval_ms > 0
            and latest_age_ms is not None
            and latest_age_ms < policy.min_refresh_interval_ms
        ):
            reason = "min_refresh_interval"
        elif not final_view_refresh and policy.materialization_delay_ms > 0:
            reason = "materialization_delay"
        else:
            return None
        return {
            "status": "coalesced",
            "reason": reason,
            "generation_id": generation_id,
            "final_view_refresh": final_view_refresh,
            "force_native_view_refresh": False,
            "min_refresh_interval_ms": policy.min_refresh_interval_ms,
            "materialization_delay_ms": policy.materialization_delay_ms,
            "latest_completed_at": (
                latest_completed_at.isoformat()
                if latest_completed_at is not None else None
            ),
            "latest_completed_age_ms": (
                round(latest_age_ms, 3) if latest_age_ms is not None else None
            ),
        }

    def _console_count_rollup_enabled(self) -> bool:
        """True iff the console count rollup is enabled for this deployment.

        Gated by ``OKG_IVM_CONSOLE_COUNT_ROLLUP`` (the #433 ``OKG_IVM_*`` pattern),
        default ON after the operator UI latency incident. An explicit ``0`` is an
        opt-out for tests or emergency rollback; read paths no longer reopen raw
        graph scans when disabled.
        """
        return os.environ.get("OKG_IVM_CONSOLE_COUNT_ROLLUP", "1") == "1"

    def _run_post_publish_views(
        self,
        g: ReservedGeneration,
        *,
        published_at: datetime | None = None,
        chronos_dirty_scope: _ChronosDirtyScope | None = None,
        final_view_refresh: bool = True,
        force_native_view_refresh: bool = False,
        graph_store: Any | None = None,
        caller_owned_transaction: bool = False,
    ) -> None:
        """Materialize representational views after admission gates.

        Native views are derived artifacts, but they are not publish
        gates. They run only after Phase B has admitted the candidate, but
        before the Chronos checkpoint is inserted, so their own derived facts
        can be included in the published graph. Per-view failures are isolated
        and reported as degradation rather than changing graph admission.
        """

        if not self.native_view_stage.materializers:
            self._last_view_outcomes = []
            self._last_native_view_refresh = {
                "status": "disabled",
                "reason": "no_materializers",
                "generation_id": g.generation_id,
                "final_view_refresh": final_view_refresh,
                "force_native_view_refresh": force_native_view_refresh,
            }
            return

        # Native views emit their view_cites_node edges as edge_facts
        # (see NativeViewStage._project_view_cites). Under chronos-native
        # publishing those facts are applied to the Chronos graph
        # (okg.graph_edges) by the caller (_cycle_chronos_native loads the
        # unapplied derived facts + _chronos_view_cite_retractions and
        # store.apply_facts them). The legacy live-edge-table projection
        # (project_retractions / project_new_edges) was retired in the
        # chronos cutover (cascade step 5): the live tables are empty, so
        # those writes were dead.
        latest_view_completed_at = self.native_view_stage.latest_completed_at(g._conn)
        view_completed_watermark_at = self.native_view_stage.completed_watermark_at(
            g._conn,
        )
        lineage_invalidations = self._view_change_frontier(
            g._conn,
            generation_id=g.generation_id,
            chronos_dirty_scope=chronos_dirty_scope,
            overlay_completed_watermark_at=view_completed_watermark_at,
        )
        reads_deltas = self._view_reads_deltas(chronos_dirty_scope)
        reads_affected_view_ids = frozenset(reads_deltas)
        # `harden-deltaflow-frontier-blockers`: a None dirty scope means the
        # segment-delta query could not compute the cycle's change set (no
        # interval tables, or a swallowed query error in
        # `_query_dirty_from_chronos_current_segment`). Both the frontier and
        # the reads-delta signals are then empty, so without this flag every
        # view would carry forward — silently serving stale output under
        # chronos-native (the input-fact watermark is dead). None never occurs
        # on a baseline publish (that returns an all-`__new__` scope), so a None
        # here is a genuine UNAVAILABLE: force a full view rebuild and surface
        # the degradation loudly instead of degrading to stale reuse.
        dirty_scope_unavailable = chronos_dirty_scope is None
        if dirty_scope_unavailable:
            logging.getLogger(__name__).warning(
                "chronos dirty scope unavailable at gen=%s (segment-delta "
                "returned None); forcing a full rebuild of all %d native "
                "view(s) rather than carrying forward — cannot prove inputs "
                "unchanged",
                g.generation_id,
                len(self.native_view_stage.materializers),
            )
        coalesce = self._native_view_coalesce_decision(
            g._conn,
            generation_id=g.generation_id,
            final_view_refresh=final_view_refresh,
            force_native_view_refresh=force_native_view_refresh,
            dirty_scope_unavailable=dirty_scope_unavailable,
            latest_completed_at=latest_view_completed_at,
        )
        if coalesce is not None:
            self._last_view_outcomes = []
            self._last_native_view_refresh = coalesce
            return
        with profile_stage("native_views.post_publish"):
            self._last_view_outcomes = self.native_view_stage.run(
                g._conn,
                generation_id=g.generation_id,
                published_at=published_at,
                catalog_version_id=g.catalog_version_id,
                lineage_invalidations=lineage_invalidations,
                reads_affected_view_ids=reads_affected_view_ids,
                reads_deltas=reads_deltas,
                dirty_scope_unavailable=dirty_scope_unavailable,
                graph_store=graph_store,
                caller_owned_transaction=caller_owned_transaction,
            )
        # A failed view is NAMED here, not left to be counted out of the
        # per-view `views` list nobody reads. `status` itself changes, so a
        # publish result cannot report "materialized" while a view has been
        # raising on every cycle — the okg-workspace `today_brief` failure
        # mode after #1153. Views run after admission gates and their failures
        # are explicitly non-gating, so this degrades the report; it does not
        # block the graph checkpoint.
        view_failures = list(self.native_view_stage.last_failures)
        view_degradations = list(self.native_view_stage.last_degradations)
        self._last_native_view_refresh = {
            "status": (
                "materialized_with_failures"
                if view_failures
                else (
                    "materialized_with_degradation"
                    if view_degradations
                    else "materialized"
                )
            ),
            "generation_id": g.generation_id,
            "final_view_refresh": final_view_refresh,
            "force_native_view_refresh": force_native_view_refresh,
            "min_refresh_interval_ms": (
                self.native_view_refresh_policy.min_refresh_interval_ms
            ),
            "materialization_delay_ms": (
                self.native_view_refresh_policy.materialization_delay_ms
            ),
            "view_count": len(self._last_view_outcomes),
            "failed_view_count": len(view_failures),
            "failed_views": view_failures,
            "degraded_view_count": len(view_degradations),
            "degraded_views": view_degradations,
        }

    def _view_reads_deltas(
        self,
        chronos_dirty_scope: _ChronosDirtyScope | None,
    ) -> dict[
        str,
        tuple[
            dict[str, list[str]],
            dict[str, list[int]],
            bool,
            dict[str, list[dict[str, Any]]],
            dict[str, dict[str, list[str]]],
        ],
    ]:
        """Per-view changed-entity deltas for views whose declared reads moved.

        For each view that declares ``reads_subtypes`` / ``reads_edge_types``,
        returns ``(changed_nodes_by_subtype, changed_edges_by_type, can_scope,
        changed_edge_endpoints_by_type, changed_node_attr_fields_by_subtype)``
        IFF the cycle's Chronos write-set touched a declared type.
        """
        if chronos_dirty_scope is None:
            return {}
        nodes_by_subtype = Publisher._semantic_dirty_nodes_by_subtype(
            chronos_dirty_scope,
        )
        edges_by_type = Publisher._semantic_dirty_edges_by_type(
            chronos_dirty_scope,
        )
        edge_table_available = chronos_dirty_scope[2]
        node_attr_changes_by_subtype = (
            chronos_dirty_scope[3] if len(chronos_dirty_scope) > 3 else None
        )
        edge_endpoints_by_type = (
            chronos_dirty_scope[4] if len(chronos_dirty_scope) > 4 else {}
        )
        deltas: dict[
            str,
            tuple[
                dict[str, list[str]],
                dict[str, list[int]],
                bool,
                dict[str, list[dict[str, Any]]],
                dict[str, dict[str, list[str]]],
            ],
        ] = {}
        for materializer in self.native_view_stage.materializers:
            reads_subtypes = tuple(
                getattr(materializer, "reads_subtypes", ()) or ()
            )
            reads_edge_types = tuple(
                getattr(materializer, "reads_edge_types", ()) or ()
            )
            if not reads_subtypes and not reads_edge_types:
                continue
            changed_nodes = {
                st: list(nodes_by_subtype.get(st, ()))
                for st in reads_subtypes
                if nodes_by_subtype.get(st)
            }
            if reads_edge_types and not edge_table_available:
                # Edge reads declared but the changed edges can't be enumerated:
                # rebuild fully (conservative; never risk a stale carry-forward).
                deltas[materializer.name] = (changed_nodes, {}, False, {}, {})
                continue
            changed_edges = {
                et: list(edges_by_type.get(et, ()))
                for et in reads_edge_types
                if edges_by_type.get(et)
            }
            if changed_nodes or changed_edges:
                changed_edge_ids = {
                    int(edge_id)
                    for edge_ids in changed_edges.values()
                    for edge_id in edge_ids
                }
                changed_edge_endpoints = {
                    et: [
                        dict(endpoint)
                        for endpoint in edge_endpoints_by_type.get(et, ())
                        if int(endpoint.get("edge_id", -1)) in changed_edge_ids
                    ]
                    for et in reads_edge_types
                    if edge_endpoints_by_type.get(et)
                }
                changed_node_attr_fields: dict[str, dict[str, list[str]]] = {}
                if node_attr_changes_by_subtype is not None:
                    for subtype, node_ids in changed_nodes.items():
                        source = node_attr_changes_by_subtype.get(subtype, {})
                        scoped = {
                            node_id: list(source.get(node_id, ()))
                            for node_id in node_ids
                            if source.get(node_id)
                        }
                        if scoped:
                            changed_node_attr_fields[subtype] = scoped
                deltas[materializer.name] = (
                    changed_nodes,
                    changed_edges,
                    True,
                    changed_edge_endpoints,
                    changed_node_attr_fields,
                )
        return deltas

    def _record_post_publish_projection_status(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        error: Optional[str],
    ) -> None:
        """Upsert ``okg.post_publish_projection_status``.

        Pinned by ``surface-post-publish-projection-failures``.
        ``error`` is None on success, ``'<ExcType>: <message>'`` on
        failure (truncated to 4096 chars by the caller).
        """
        status = "failed" if error is not None else "success"
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.post_publish_projection_status
                  (deployment_name, generation_id, status, error)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (deployment_name, generation_id)
                  DO UPDATE SET
                    last_run_at = now(),
                    status      = EXCLUDED.status,
                    error       = EXCLUDED.error
                """,
                (self.deployment_name, generation_id, status, error),
            )

    def _native_view_refresh_report(self) -> dict[str, Any]:
        """``_last_native_view_refresh`` with the failure fields ALWAYS set.

        Several code paths assign that dict (not_run, disabled, coalesced,
        reused, materialized, publish-blocked) and only the materialized one knows
        about view failures. A consumer reading ``failed_view_count``
        directly would KeyError on the other four, and one reading
        ``.get("failed_view_count", 0)`` would read every coalesced cycle
        as healthy — absence dressed as agreement, which is the exact
        defect class this whole change exists to close.

        So the keys are always present, and ``view_stage_ran`` says which
        kind of zero it is: a stage that ran and found nothing wrong, or a
        stage that did not run this cycle and is making no claim.
        """
        report = dict(self._last_native_view_refresh)
        report.setdefault(
            "view_stage_ran",
            report.get("status")
            in {
                "materialized",
                "materialized_with_degradation",
                "materialized_with_failures",
            },
        )
        report.setdefault("failed_views", [])
        report.setdefault(
            "failed_view_count", len(report.get("failed_views") or [])
        )
        if report["view_stage_ran"]:
            report.setdefault("degraded_views", [])
            report.setdefault(
                "degraded_view_count", len(report.get("degraded_views") or [])
            )
        return report

    @staticmethod
    def _format_view_outcomes(outcomes: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "view_id": out.view_id,
                "asset_id": getattr(out, "asset_id", None),
                "asset_digest": getattr(out, "asset_digest", None),
                "freshness": out.freshness,
                "status": getattr(out, "status", "success"),
                "cause": getattr(out, "cause", None),
                "omitted_work": list(getattr(out, "omitted_work", [])),
                "scope": dict(getattr(out, "scope", {})),
                "retryable": getattr(out, "retryable", None),
                "fragments_total": out.fragments_total,
                "fragments_reused": out.fragments_reused,
                "fragments_refreshed": out.fragments_refreshed,
                "fragments_added": out.fragments_added,
                "fragments_dropped": out.fragments_dropped,
                "llm_calls": out.llm_calls,
                "error": out.error,
                "elapsed_ms": getattr(out, "elapsed_ms", None),
                "corpus_build_ms": getattr(out, "corpus_build_ms", None),
                "corpus_scan_scoped": getattr(
                    out, "corpus_scan_scoped", False,
                ),
                "dirty_scope_count": getattr(
                    out, "dirty_scope_count", None,
                ),
                "relation_rows": getattr(out, "relation_rows", None),
            }
            for out in outcomes
        ]

    @staticmethod
    def _chronos_timing_details(
        *,
        timings_ms: Mapping[str, Any],
        lock_acquire_ms: Any,
        lock_hold_ms: Any,
        coordination_mode: str,
        hard_timeout_ms: int | None = None,
        hard_timeout_source: str | None = None,
        lock_hold_headroom_factor: float | None = None,
        lock_hold_headroom_factor_source: str | None = None,
        allow_raised_statement_timeout: bool | None = None,
        allow_raised_statement_timeout_source: str | None = None,
        enricher_runs: list[dict[str, Any]],
        consistency_summaries: list[dict[str, Any]],
        view_outcomes: list[Any],
        native_view_refresh: Mapping[str, Any] | None,
        lens_outcomes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        top_level_ms = _publisher_numeric_timings(timings_ms)
        flat_ms = {
            f"stage.{name}": value
            for name, value in top_level_ms.items()
        }

        lock = {
            "coordination_mode": coordination_mode,
            "acquire_ms": _publisher_round_ms(lock_acquire_ms),
            "hold_ms": _publisher_round_ms(lock_hold_ms),
        }
        # Issue #1291: the receipt names the wait, the hold, the bound the hold
        # ran against, the slowest phase that produced it, and the verdict of
        # the configured headroom policy. A cycle with no measured hold reports
        # `unmeasured`, never a pass.
        lock["wait_ms"] = lock["acquire_ms"]
        lock["hard_timeout_ms"] = (
            None if hard_timeout_ms is None else int(hard_timeout_ms)
        )
        lock["hard_timeout_source"] = hard_timeout_source
        if lock["acquire_ms"] is not None:
            flat_ms["lock.acquire"] = lock["acquire_ms"]
        if lock["hold_ms"] is not None:
            flat_ms["lock.hold"] = lock["hold_ms"]

        enrichers: list[dict[str, Any]] = []
        for index, raw in enumerate(enricher_runs, start=1):
            if not isinstance(raw, Mapping):
                continue
            name = str(
                raw.get("producer_id")
                or raw.get("name")
                or f"enricher_{index}"
            )
            timings = _publisher_numeric_timings(raw.get("timings_ms"))
            item = {
                "name": str(raw.get("name") or name),
                "producer_id": name,
                "status": raw.get("status"),
                "reason": raw.get("reason"),
                "dirty_node_count": raw.get("dirty_node_count"),
                "dirty_edge_count": raw.get("dirty_edge_count"),
                "n_edges_emitted": raw.get("n_edges_emitted"),
                "n_edges_skipped": raw.get("n_edges_skipped"),
                "timings_ms": timings,
            }
            enrichers.append(item)
            for stage_name, value in timings.items():
                flat_ms[f"enricher.{name}.{stage_name}"] = value

        consistency_gates: list[dict[str, Any]] = []
        for index, raw in enumerate(consistency_summaries, start=1):
            if not isinstance(raw, Mapping):
                continue
            check_id = str(
                raw.get("check_id")
                or raw.get("gate_name")
                or f"consistency_gate_{index}"
            )
            elapsed_ms = _publisher_round_ms(raw.get("elapsed_ms"))
            item = {
                "check_id": check_id,
                "gate_name": raw.get("gate_name"),
                "result": raw.get("result"),
                "blocks_publish": raw.get("blocks_publish"),
                "elapsed_ms": elapsed_ms,
                "budget_ms": raw.get("budget_ms"),
            }
            consistency_gates.append(item)
            if elapsed_ms is not None:
                flat_ms[f"consistency_gate.{check_id}.elapsed"] = elapsed_ms

        native_views: list[dict[str, Any]] = []
        for index, out in enumerate(view_outcomes, start=1):
            view_id = str(getattr(out, "view_id", f"native_view_{index}"))
            elapsed_ms = _publisher_round_ms(getattr(out, "elapsed_ms", None))
            timings = _publisher_numeric_timings(
                getattr(out, "timings_ms", None),
            )
            item = {
                "view_id": view_id,
                "freshness": getattr(out, "freshness", None),
                "fragments_total": getattr(out, "fragments_total", None),
                "fragments_reused": getattr(out, "fragments_reused", None),
                "fragments_refreshed": getattr(
                    out, "fragments_refreshed", None,
                ),
                "llm_calls": getattr(out, "llm_calls", None),
                "error": getattr(out, "error", None),
                "elapsed_ms": elapsed_ms,
                "timings_ms": timings,
            }
            native_views.append(item)
            if elapsed_ms is not None:
                flat_ms[f"native_view.{view_id}.elapsed"] = elapsed_ms
            for stage_name, value in timings.items():
                flat_ms[f"native_view.{view_id}.{stage_name}"] = value

        lenses: list[dict[str, Any]] = []
        for index, raw in enumerate(lens_outcomes, start=1):
            if not isinstance(raw, Mapping):
                continue
            name = str(
                raw.get("lens_id")
                or raw.get("name")
                or f"lens_{index}"
            )
            timings = _publisher_numeric_timings(raw.get("timings_ms"))
            elapsed_ms = _publisher_round_ms(
                raw.get("elapsed_ms") or timings.get("total"),
            )
            lenses.append({
                "name": name,
                "status": raw.get("status"),
                "elapsed_ms": elapsed_ms,
                "timings_ms": timings,
            })
            if elapsed_ms is not None:
                flat_ms[f"lens.{name}.elapsed"] = elapsed_ms
            for stage_name, value in timings.items():
                flat_ms[f"lens.{name}.{stage_name}"] = value

        # Fill the lock's capacity verdict last, so the slowest phase is
        # chosen from every timing this cycle recorded, not only the stages
        # that happened to be flattened first.
        lock["slowest_phase"] = _publisher_slowest_phase(flat_ms)
        lock["headroom"] = evaluate_lock_headroom(
            hold_ms=lock["hold_ms"],
            hard_timeout_ms=hard_timeout_ms,
            hard_timeout_source=hard_timeout_source,
            required_factor=lock_hold_headroom_factor,
            factor_source=lock_hold_headroom_factor_source,
            allow_raised_timeout=allow_raised_statement_timeout,
            allow_raised_timeout_source=(
                allow_raised_statement_timeout_source
            ),
            wait_ms=lock["wait_ms"],
            slowest_phase=lock["slowest_phase"],
        )

        return {
            "schema": "okg.publisher.timing_details.v1",
            "top_level_ms": top_level_ms,
            "flat_ms": dict(sorted(flat_ms.items())),
            "lock": lock,
            "enrichers": enrichers,
            "consistency_gates": consistency_gates,
            "native_views": native_views,
            "native_view_refresh": dict(native_view_refresh or {}),
            "lenses": lenses,
        }

    def _run_post_publish_lenses(
        self,
        g: ReservedGeneration,
    ) -> None:
        """Register deployment-declared lenses after view publication."""

        self._last_lens_outcomes = []
        if not self.lenses:
            return

        from okg.substrate.lenses import register_lens_definition

        log = logging.getLogger(__name__)
        with profile_stage("lenses.post_publish"):
            for lens in self.lenses:
                lens_id = str(getattr(lens, "lens_id", ""))
                view_id = str(getattr(lens, "view_id", ""))
                outcome = {
                    "lens_id": lens_id,
                    "view_id": view_id,
                    "definition_id": str(
                        getattr(lens, "definition_id", "")
                    ),
                    "error": None,
                }
                try:
                    if not self._lens_base_manifest_completed(g, lens):
                        outcome["error"] = "view_manifest_not_completed"
                        self._last_lens_outcomes.append(outcome)
                        continue
                    register_lens_definition(g._conn, lens)
                except Exception as exc:  # noqa: BLE001
                    outcome["error"] = f"{type(exc).__name__}: {exc}"
                    log.exception(
                        "post-publish lens registration failed for "
                        "generation %s lens %r",
                        g.generation_id,
                        lens_id,
                    )
                self._last_lens_outcomes.append(outcome)

    def _enqueue_post_publish_hooks(
        self,
        *,
        generation_id: int,
        status: str,
    ) -> None:
        """Enqueue SDK-registered post-publish hooks for this generation.

        Phase 2 §1.3. The hooks module's :func:`enqueue_hooks` swallows
        per-hook failures internally; this wrapper just selects the
        event type based on the committed status and calls through.

        Skips silently when the runtime is disabled — Phase 1
        deployments without the DBOS runtime layer enabled have no
        hook contract.
        """
        from okg.substrate.runtime.hooks import (
            HOOK_EVENT_GENERATION_BLOCKED,
            HOOK_EVENT_GENERATION_PUBLISHED,
            enqueue_hooks,
        )

        # We need a DSN to construct the DBOSClient. Publisher exposes
        # it as self.dsn on substrates where the runtime layer is wired
        # in. If absent, skip — Phase 1 deployments without DBOS have
        # no hooks registered anyway.
        dsn = getattr(self, "dsn", None) or getattr(self, "_dsn", None)
        if not dsn:
            return

        if status == "published":
            event = HOOK_EVENT_GENERATION_PUBLISHED
        elif status == "blocked":
            event = HOOK_EVENT_GENERATION_BLOCKED
        else:
            # 'pending' or unexpected: hooks fire only on terminal
            # transitions.
            return

        enqueue_hooks(
            deployment=self.deployment_name,
            event_type=event,
            dsn=dsn,
            args=(generation_id,),
        )

    def _lens_base_manifest_completed(
        self,
        g: ReservedGeneration,
        lens: Any,
    ) -> bool:
        """Return true only when the lens base view completed this gen."""

        with g._conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM okg.derived_output_manifests
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(generation_id)s
                   AND status = 'completed'
                 LIMIT 1
                """,
                {
                    "definition_id": str(
                        getattr(lens, "view_definition_id", "")
                    ),
                    "generation_id": g.generation_id,
                },
            )
            return cur.fetchone() is not None

    def _refresh_published_generation_watermarks(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.graph_generations
                   SET fact_watermark = COALESCE((
                         SELECT max(fact_id) FROM okg.node_facts
                       ), 0),
                       edge_fact_watermark = COALESCE((
                         SELECT max(fact_id) FROM okg.edge_facts
                       ), 0)
                 WHERE generation_id = %s
                   AND status = 'published'
                """,
                (generation_id,),
            )

    # ---------- per-enricher progress helpers ----------

    _RETENTION_HORIZON_DAYS = 180  # matches schema.sql

    def _load_enricher_progress_rows(
        self,
        conn: psycopg.Connection,
        enricher_names: list[str],
    ) -> dict[str, tuple[Any, Any, Any, Any, Any]]:
        if not enricher_names:
            return {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.enricher_name, p.fact_watermark, p.edge_watermark,
                       p.last_status, p.last_run_at, p.catalog_version_id
                  FROM okg.enricher_progress p
                 WHERE p.deployment_name = %s
                   AND p.enricher_name = ANY(%s)
                """,
                (self.deployment_name, enricher_names),
            )
            return {
                str(row[0]): (row[1], row[2], row[3], row[4], row[5])
                for row in cur.fetchall()
            }

    def _enricher_retry_pending_name(
        self,
        conn: psycopg.Connection,
    ) -> str | None:
        """First wired enricher with a held (non-success) retry, if any.

        P6 (specify-publish-commit-protocol): the publisher's pre-enrichment
        noop-reuse probe must include enricher retry state in its input
        closure. The #679 watermark-hold records ``last_status`` as
        ``failed:...`` (e.g. ``failed:incomplete_inputs``) so the next cycle
        re-runs the enricher on the same delta. But if the deployment's
        graph delta is zero, the noop-reuse probe skips the enricher stage
        entirely -- so the held retry never runs and the enricher starves
        forever. The ingest-side skip already checks this
        (``ingest/service.py:_stack_enrichers_have_pending_work``); the
        publisher-side probe did not, so ingest saw pending work, invoked
        publish, and publish reused-noop past the very enricher ingest was
        trying to drive. An enricher with NO progress row (never run) is
        also pending -- it must run at least once. Returns the enricher
        name that invalidates reuse, or ``None`` when every wired enricher
        is ``success``.
        """
        enricher_names = [
            str(getattr(e, "name", e.__class__.__name__))
            for e in self.enrichers
        ]
        if not enricher_names:
            return None
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT enricher_name, last_status
                  FROM okg.enricher_progress
                 WHERE deployment_name = %s
                   AND enricher_name = ANY(%s)
                """,
                (self.deployment_name, enricher_names),
            )
            progress = {
                str(name): str(status or "")
                for name, status in cur.fetchall()
            }
        for name in enricher_names:
            if progress.get(name) != "success":
                return name
        return None

    def _has_pending_enricher_backlog(
        self,
        conn: psycopg.Connection,
    ) -> bool:
        """Whether a successful enricher still has eligible node-fact input.

        Enricher progress advances only to the fact frontier captured at phase
        start. An upstream enricher can emit a node fact after that snapshot,
        leaving a downstream enricher with ``last_status = 'success'`` but a
        fact watermark behind eligible input. A generation-level no-op reuse
        must not skip the next enricher phase in that state.

        Eligibility intentionally mirrors ``_query_dirty_from_node_facts``:
        any node-fact operation (including a retraction) above the successful
        watermark whose subtype is in ``reads_subtypes`` is pending, and an
        empty ``reads_subtypes`` selects every subtype rather than none.
        Historical
        edge-only backlog is not claimed here because the current incremental
        context cannot reconstruct prior-segment edge facts. Missing or failed
        progress rows remain the responsibility of
        ``_enricher_retry_pending_name``.
        """
        enricher_names = [
            str(getattr(enricher, "name", type(enricher).__name__))
            for enricher in self.enrichers
        ]
        progress = self._load_enricher_progress_rows(
            conn,
            enricher_names,
        )
        # None in the array slot means "every subtype", matching the empty-
        # selector branch of _query_dirty_from_node_facts.
        backlog_probes: list[tuple[int, list[str] | None]] = []
        for enricher in self.enrichers:
            name = str(getattr(enricher, "name", type(enricher).__name__))
            row = progress.get(name)
            if row is None:
                continue
            (
                fact_watermark,
                _edge_watermark,
                last_status,
                last_run_at,
                _catalog_version_id,
            ) = row
            if str(last_status or "") != "success":
                continue
            if self._is_stale_watermark(last_run_at):
                # Retention can have removed fact-log evidence. Force the
                # phase so _build_incremental_ctx performs its branch scan.
                return True
            reads_subtypes = tuple(
                str(subtype)
                for subtype in (
                    getattr(enricher, "reads_subtypes", ()) or ()
                )
                if str(subtype)
            )
            backlog_probes.append(
                (
                    int(fact_watermark or 0),
                    # An empty declaration means "every subtype" in
                    # _query_dirty_from_node_facts, so it must mean the same
                    # here. Treating it as "nothing" would fail open for an
                    # enricher that declares only edge selectors.
                    list(reads_subtypes) if reads_subtypes else None,
                )
            )
        if not backlog_probes:
            return False
        value_rows = sql.SQL(", ").join(
            sql.SQL("(%s::bigint, %s::text[])")
            for _ in backlog_probes
        )
        params: list[Any] = []
        for fact_watermark, reads_subtypes in backlog_probes:
            params.extend((fact_watermark, reads_subtypes))
        query = sql.SQL(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM okg.node_facts nf
                  JOIN (VALUES {value_rows})
                       AS backlog(fact_watermark, reads_subtypes)
                    ON nf.fact_id > backlog.fact_watermark
                   AND (
                        backlog.reads_subtypes IS NULL
                     OR nf.subtype = ANY(backlog.reads_subtypes)
                   )
            )
            """
        ).format(value_rows=value_rows)
        with conn.cursor() as cur:
            cur.execute(query, params)
            return bool(cur.fetchone()[0])

    def _noop_reuse_enricher_requires_run_name(self) -> str | None:
        """Return the first enricher that makes phase-level evaluation necessary.

        The pre-enrichment no-op probe has no ``IncrementalContext`` and must
        not outrank the per-enricher clean-skip contract. Missing/false opt-in
        means always-run. A custom ``can_skip_clean`` hook is likewise deferred
        to the enricher phase, where ``_should_skip_clean_enricher`` can call it
        with the complete context. This is intentionally conservative: a hook
        that ultimately permits skipping costs a phase-level check, never a
        silently starved enricher.
        """
        for enricher in self.enrichers:
            name = str(getattr(enricher, "name", type(enricher).__name__))
            if getattr(enricher, "publisher_skip_when_clean", False) is not True:
                return name
            reads_subtypes = tuple(
                getattr(enricher, "reads_subtypes", ()) or ()
            )
            reads_edge_types = tuple(
                getattr(enricher, "reads_edge_types", ()) or ()
            )
            if not reads_subtypes and not reads_edge_types:
                # No declared selector means the pre-phase probe cannot prove
                # cleanliness. Defer to lineage planning in _run_enrichers.
                return name
            if callable(getattr(enricher, "can_skip_clean", None)):
                return name
        return None

    def _load_enricher_profile_fingerprints(
        self,
        conn: psycopg.Connection,
        producer_ids: list[str] | tuple[str, ...] | Any,
    ) -> dict[str, str]:
        ids = [str(producer_id) for producer_id in producer_ids]
        if not ids:
            return {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (producer_id)
                       producer_id, profile_fingerprint
                  FROM okg.producer_profiles
                 WHERE deployment_name = %s
                   AND producer_id = ANY(%s)
                 ORDER BY producer_id, updated_at DESC
                """,
                (self.deployment_name, ids),
            )
            return {
                str(row[0]): str(row[1])
                for row in cur.fetchall()
                if row[1] is not None
            }

    def _build_incremental_ctx(
        self,
        conn: psycopg.Connection,
        enricher: Any,
        *,
        progress_row: tuple[Any, Any, Any, Any, Any] | object = _UNSET,
        current_fact_watermark: int | None = None,
        current_edge_watermark: int | None = None,
        current_catalog_version_id: int | None | object = _UNSET,
        chronos_dirty: (
            tuple[
                list[str],
                list[int],
                dict[str, dict[str, frozenset[str]]] | None,
            ]
            | None
            | object
        ) = _UNSET,
    ) -> IncrementalContext:
        """Read / initialize the enricher's progress row and
        compute its `dirty_node_ids` per the spec contract.

        - First run / forced rescan / stale watermark
          (`now() - last_run_at > 180 days`): dirty drawn
          from the graph checkout (sidesteps the 180-day
          `node_facts` retention boundary).
        - Normal incremental: dirty drawn from
          `node_facts WHERE fact_id > prev_fact_watermark`.
        - Optionally intersected with the enricher's
          `reads_subtypes` filter (cheaper for narrow-read
          enrichers).
        """
        ename = getattr(
            enricher, "name", enricher.__class__.__name__,
        )
        if progress_row is _UNSET:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.fact_watermark, p.edge_watermark,
                           p.last_status, p.last_run_at,
                           p.catalog_version_id
                      FROM okg.enricher_progress p
                     WHERE p.deployment_name = %s
                       AND p.enricher_name = %s
                    """,
                    (self.deployment_name, ename),
                )
                row = cur.fetchone()
        else:
            row = progress_row
        if row is None:
            prev_fw, prev_ew = 0, 0
            status, last_run_at, prev_catalog_version_id = (
                "never", None, None
            )
        else:
            (
                prev_fw, prev_ew, status, last_run_at,
                prev_catalog_version_id,
            ) = row

        reads_subtypes: tuple[str, ...] = tuple(
            getattr(enricher, "reads_subtypes", ()) or ()
        )
        reads_edge_types: tuple[str, ...] = tuple(
            getattr(enricher, "reads_edge_types", ()) or ()
        )
        if (
            current_fact_watermark is None
            or current_edge_watermark is None
        ):
            current_fact_watermark, current_edge_watermark = (
                self._current_fact_watermarks(conn)
            )
        if current_catalog_version_id is _UNSET:
            current_catalog_version_id = self._current_catalog_version_id(
                conn
            )
        if chronos_dirty is _UNSET:
            chronos_dirty = self._query_dirty_from_chronos_current_segment(
                conn,
                reads_subtypes,
                reads_edge_types,
                branch_name=self.branch_name,
            )
        dirty_edge_ids: frozenset[int] = frozenset()
        change_basis = "fact_watermark"
        if chronos_dirty is not None:
            is_first_run = (
                status != "success"
                or self._is_stale_watermark(last_run_at)
            )
            dirty, dirty_edges, dirty_node_attr_changes = chronos_dirty
            dirty_edge_ids = frozenset(dirty_edges)
            change_basis = "chronos_segment"
            if is_first_run:
                dirty = self._query_dirty_from_chronos_branch(
                    conn,
                    reads_subtypes,
                    branch_name=self.branch_name,
                )
            elif current_fact_watermark > int(prev_fw or 0):
                legacy_dirty = self._query_dirty_from_node_facts(
                    conn, int(prev_fw or 0), reads_subtypes,
                )
                if legacy_dirty:
                    dirty = sorted({*dirty, *legacy_dirty})
                    dirty_node_attr_changes = None
        else:
            dirty_node_attr_changes = None
            is_first_run = (
                status == "never"
                or prev_fw == 0
                or self._is_stale_watermark(last_run_at)
            )
            if is_first_run:
                dirty = self._query_dirty_from_chronos_branch(
                    conn,
                    reads_subtypes,
                    branch_name=self.branch_name,
                )
            else:
                dirty = self._query_dirty_from_node_facts(
                    conn, prev_fw, reads_subtypes,
                )
        return IncrementalContext(
            prev_fact_watermark=int(prev_fw or 0),
            prev_edge_watermark=int(prev_ew or 0),
            dirty_node_ids=frozenset(dirty),
            is_first_run=is_first_run,
            last_status=str(status or "never"),
            current_fact_watermark=current_fact_watermark,
            current_edge_watermark=current_edge_watermark,
            prev_catalog_version_id=(
                int(prev_catalog_version_id)
                if prev_catalog_version_id is not None else None
            ),
            current_catalog_version_id=current_catalog_version_id,
            dirty_edge_ids=dirty_edge_ids,
            dirty_node_attr_changes_by_subtype=dirty_node_attr_changes,
            change_basis=change_basis,
        )

    @staticmethod
    def _current_fact_watermarks(
        conn: psycopg.Connection,
    ) -> tuple[int, int]:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(max(fact_id), 0) FROM okg.node_facts")
            node_fact_watermark = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COALESCE(max(fact_id), 0) FROM okg.edge_facts")
            edge_fact_watermark = int(cur.fetchone()[0] or 0)
        return node_fact_watermark, edge_fact_watermark

    @staticmethod
    def _current_catalog_version_id(
        conn: psycopg.Connection,
    ) -> int | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(catalog_version_id)
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                """,
            )
            row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    @staticmethod
    @contextmanager
    def _enricher_lifecycle_guard(
        conn: psycopg.Connection,
        *,
        caller_owned_transaction: bool,
        enricher_name: str,
        hook: str,
    ) -> Iterator[None]:
        """Keep every enricher lifecycle hook inside the caller's transaction."""
        if not caller_owned_transaction:
            yield
            return
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
            raise SubstrateError(
                "caller-owned publication transaction ended before enricher hook",
                data={"enricher": enricher_name, "hook": hook},
            )
        # A nested psycopg transaction is a PostgreSQL savepoint. More
        # importantly, psycopg rejects conn.commit()/conn.rollback() while this
        # context is active, so compatibility is enforced at runtime for every
        # supplied-connection hook rather than only for enrich().
        with conn.transaction():
            yield

    def _should_skip_clean_enricher(
        self,
        conn: psycopg.Connection,
        enricher: Any,
        ctx: IncrementalContext,
        *,
        catalog_version_id: int,
        caller_owned_transaction: bool = False,
    ) -> bool:
        """Return True only when an enricher opts into a provable
        clean-input skip.

        The publisher skips the call only when the prior run succeeded,
        node inputs are clean, catalog identity is unchanged, and any
        declared edge input types have no new facts past the enricher's
        edge watermark. Enrichers with extra liveness requirements can
        expose ``can_skip_clean(conn, ctx)``.
        """
        if not getattr(enricher, "publisher_skip_when_clean", False):
            return False
        if ctx.last_status != "success":
            return False
        if ctx.is_first_run or ctx.dirty_node_ids:
            return False
        if ctx.dirty_edge_ids and tuple(
            getattr(enricher, "reads_edge_types", ()) or ()
        ):
            return False
        skip_clean_catalog_change = bool(getattr(
            enricher,
            "publisher_skip_when_catalog_changed_but_inputs_clean",
            False,
        ))
        if (
            ctx.prev_catalog_version_id != catalog_version_id
            and not (
                ctx.prev_catalog_version_id is None
                and ctx.last_status == "success"
            )
            and not skip_clean_catalog_change
        ):
            return False
        if (
            ctx.current_catalog_version_id is not None
            and ctx.current_catalog_version_id != catalog_version_id
        ):
            return False
        # (Chronos-native: edge-fact-watermark liveness was a legacy-projector
        # concern; the chronos read path has no edge_facts watermark gate here.)
        can_skip = getattr(enricher, "can_skip_clean", None)
        if can_skip is not None:
            with self._enricher_lifecycle_guard(
                conn,
                caller_owned_transaction=caller_owned_transaction,
                enricher_name=str(
                    getattr(enricher, "name", enricher.__class__.__name__)
                ),
                hook="can_skip_clean",
            ):
                if not can_skip(conn, ctx):
                    return False
        return True

    @classmethod
    def _is_stale_watermark(cls, last_run_at) -> bool:
        if last_run_at is None:
            return False
        from datetime import timedelta
        age = datetime.now(timezone.utc) - last_run_at
        return age > timedelta(days=cls._RETENTION_HORIZON_DAYS)

    @staticmethod
    def _query_dirty_from_node_facts(
        conn: psycopg.Connection,
        prev_fw: int,
        reads_subtypes: tuple[str, ...],
    ) -> list[str]:
        if reads_subtypes:
            sql = (
                "SELECT DISTINCT node_id FROM okg.node_facts "
                " WHERE fact_id > %s "
                "   AND subtype = ANY(%s)"
            )
            params: tuple = (prev_fw, list(reads_subtypes))
        else:
            sql = (
                "SELECT DISTINCT node_id FROM okg.node_facts "
                " WHERE fact_id > %s"
            )
            params = (prev_fw,)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [r[0] for r in cur.fetchall()]

    @staticmethod
    def _query_dirty_from_chronos_branch(
        conn: psycopg.Connection,
        reads_subtypes: tuple[str, ...],
        *,
        branch_name: str,
    ) -> list[str]:
        try:
            from okg.substrate.graph_store import ChronosGraphStore

            store = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
                # A first-run enricher may execute inside the caller-verified
                # publication transaction. Inherit ownership from the
                # supplied connection so BranchSession.query cannot commit
                # facts, checkpoint state, or the eventual pointer early.
                autocommit=bool(getattr(conn, "autocommit", True)),
            )
            if reads_subtypes:
                rows = store.checkout(branch_name).query(
                    """
                    SELECT node_id
                      FROM okg.graph_nodes
                     WHERE subtype = ANY(:subtypes)
                    """,
                    {"subtypes": list(reads_subtypes)},
                )
            else:
                rows = store.checkout(branch_name).query(
                    "SELECT node_id FROM okg.graph_nodes"
                )
            return [str(row["node_id"]) for row in rows]
        except Exception:
            logging.getLogger(__name__).exception(
                "chronos branch dirty-node read failed for branch=%s",
                branch_name,
            )
            raise

    @staticmethod
    def _query_dirty_from_chronos_current_segment(
        conn: psycopg.Connection,
        reads_subtypes: tuple[str, ...],
        reads_edge_types: tuple[str, ...],
        *,
        branch_name: str,
    ) -> tuple[
        list[str],
        list[int],
        dict[str, dict[str, frozenset[str]]] | None,
    ] | None:
        scope = Publisher._query_dirty_from_chronos_current_segment_by_type(
            conn,
            branch_name=branch_name,
        )
        if scope is None:
            return None
        return Publisher._filter_chronos_dirty_scope(
            scope,
            reads_subtypes,
            reads_edge_types,
        )

    @staticmethod
    def _normalize_chronos_dirty_scope(
        scope: _ChronosDirtyScope | None,
    ) -> _ChronosDirtyScope | None:
        if scope is None:
            return None
        if len(scope) == 3:
            nodes_by_subtype, edges_by_type, edge_table_available = scope
            node_attr_changes_by_subtype = None
            edge_endpoints_by_type = {}
            edge_attr_changes_by_type = None
        elif len(scope) == 4:
            (
                nodes_by_subtype,
                edges_by_type,
                edge_table_available,
                node_attr_changes_by_subtype,
            ) = scope
            edge_endpoints_by_type = {}
            edge_attr_changes_by_type = None
        else:
            (
                nodes_by_subtype,
                edges_by_type,
                edge_table_available,
                node_attr_changes_by_subtype,
                edge_endpoints_by_type,
                edge_attr_changes_by_type,
            ) = scope
        normalized_attr_changes: dict[str, dict[str, list[str]]] | None
        if node_attr_changes_by_subtype is None:
            normalized_attr_changes = None
        else:
            normalized_attr_changes = {
                str(subtype): {
                    str(node_id): list(dict.fromkeys(
                        str(field) for field in fields
                    ))
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (
                    node_attr_changes_by_subtype.items()
                )
            }
        normalized_edge_attr_changes: dict[str, dict[int, list[str]]] | None
        if edge_attr_changes_by_type is None:
            normalized_edge_attr_changes = None
        else:
            normalized_edge_attr_changes = {
                str(edge_type): {
                    int(edge_id): list(dict.fromkeys(
                        str(field) for field in fields
                    ))
                    for edge_id, fields in edge_changes.items()
                }
                for edge_type, edge_changes in (
                    edge_attr_changes_by_type.items()
                )
            }
        return (
            {
                str(subtype): list(dict.fromkeys(str(node_id) for node_id in node_ids))
                for subtype, node_ids in nodes_by_subtype.items()
            },
            {
                str(edge_type): list(dict.fromkeys(int(edge_id) for edge_id in edge_ids))
                for edge_type, edge_ids in edges_by_type.items()
            },
            bool(edge_table_available),
            normalized_attr_changes,
            {
                str(edge_type): [
                    {
                        "edge_id": int(endpoint["edge_id"]),
                        "src": str(endpoint["src"]),
                        "dst": str(endpoint["dst"]),
                    }
                    for endpoint in endpoints
                    if isinstance(endpoint, Mapping)
                    and {"edge_id", "src", "dst"} <= set(endpoint)
                ]
                for edge_type, endpoints in (
                    edge_endpoints_by_type or {}
                ).items()
            },
            normalized_edge_attr_changes,
        )

    @staticmethod
    def _normalize_chronos_delta_counts(
        counts: Mapping[str, Any] | None,
    ) -> _ChronosDeltaCounts | None:
        if counts is None:
            return None
        normalized: _ChronosDeltaCounts = {}
        for key in _CHRONOS_DELTA_COUNT_KEYS:
            try:
                normalized[key] = int(counts.get(key, 0) or 0)
            except (TypeError, ValueError):
                return None
        return normalized

    @staticmethod
    def _normalize_enricher_lineage_invalidations_hint(
        invalidations: Any,
    ) -> Any:
        if invalidations is _UNSET or invalidations is None:
            return invalidations
        if isinstance(invalidations, (str, bytes, Mapping)):
            return None
        try:
            return tuple(invalidations)
        except TypeError:
            return None

    @staticmethod
    def _chronos_dirty_scope_stats(
        scope: _ChronosDirtyScope | None,
    ) -> dict[str, Any]:
        if scope is None:
            return {
                "available": False,
                "node_count": 0,
                "edge_count": 0,
                "edge_table_available": False,
            }
        nodes_by_subtype, edges_by_type, edge_table_available = scope[:3]
        return {
            "available": True,
            "node_count": sum(len(node_ids) for node_ids in nodes_by_subtype.values()),
            "edge_count": sum(len(edge_ids) for edge_ids in edges_by_type.values()),
            "node_subtype_count": len(nodes_by_subtype),
            "edge_type_count": len(edges_by_type),
            "edge_table_available": edge_table_available,
        }

    def _deployment_maintained_derivation_rows(self) -> list[Any]:
        registry = self.maintained_derivations
        if registry is None:
            return []
        if hasattr(registry, "rows"):
            return list(registry.rows())
        return list(registry)

    @staticmethod
    def _changed_ids_from_chronos_dirty_scope(
        scope: _ChronosDirtyScope | None,
    ) -> tuple[tuple[str, ...] | None, tuple[int, ...] | None]:
        if scope is None:
            return None, None
        nodes_by_subtype, edges_by_type, edge_table_available = scope[:3]
        changed_node_ids = tuple(
            sorted(
                {
                    str(node_id)
                    for node_ids in nodes_by_subtype.values()
                    for node_id in node_ids
                }
            )
        )
        if not edge_table_available:
            return changed_node_ids, None
        changed_edge_ids = tuple(
            sorted(
                {
                    int(edge_id)
                    for edge_ids in edges_by_type.values()
                    for edge_id in edge_ids
                }
            )
        )
        return changed_node_ids, changed_edge_ids

    @staticmethod
    def _filter_chronos_dirty_scope(
        scope: _ChronosDirtyScope,
        reads_subtypes: tuple[str, ...],
        reads_edge_types: tuple[str, ...],
    ) -> tuple[
        list[str],
        list[int],
        dict[str, dict[str, frozenset[str]]] | None,
    ] | None:
        nodes_by_subtype, edges_by_type, edge_table_available = scope[:3]
        node_attr_changes_by_subtype = scope[3] if len(scope) > 3 else None
        if reads_subtypes:
            dirty_nodes = [
                node_id
                for subtype in reads_subtypes
                for node_id in nodes_by_subtype.get(subtype, ())
            ]
        else:
            dirty_nodes = [
                node_id
                for nodes in nodes_by_subtype.values()
                for node_id in nodes
            ]
        if reads_edge_types:
            if not edge_table_available:
                return None
            dirty_edges = [
                edge_id
                for edge_type in reads_edge_types
                for edge_id in edges_by_type.get(edge_type, ())
            ]
        else:
            dirty_edges = []
        if node_attr_changes_by_subtype is None:
            dirty_node_attr_changes = None
        elif reads_subtypes:
            dirty_node_attr_changes = {
                subtype: {
                    node_id: frozenset(fields)
                    for node_id, fields in (
                        node_attr_changes_by_subtype.get(subtype, {})
                    ).items()
                }
                for subtype in reads_subtypes
                if subtype in node_attr_changes_by_subtype
            }
        else:
            dirty_node_attr_changes = {
                subtype: {
                    node_id: frozenset(fields)
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (
                    node_attr_changes_by_subtype.items()
                )
            }
        return dirty_nodes, dirty_edges, dirty_node_attr_changes

    def _chronos_hint_segments(
        self, conn: psycopg.Connection, *, previous: Any | None,
    ) -> list[int] | None | bool:
        """The segments a caller's changed-row hint must cover, or False.

        None: the current segment alone (no previous generation, or nothing
        but the current segment written since it). A list: every segment
        written since ``previous`` when only source runs' run-base
        checkpoints split the head since then (issue #2993) -- the hint may
        stand for the scan once it lists every key those segments wrote.
        False: the branch moved to a new segment since ``previous`` for
        another reason -- a fork was taken from it, or a BLOCKED generation
        cut a checkpoint -- so the full since-previous scan must run (issue
        #2865).
        """
        previous_ref = getattr(previous, "ref", None)
        if previous_ref is None:
            return None
        since_previous = self._chronos_segments_written_since(
            conn,
            branch_name=self.branch_name,
            base_segment_id=str(previous_ref),
        )
        if len(since_previous) == 1:
            return None
        with conn.cursor() as cur:
            cur.execute(
                _CHRONOS_SEGMENTS_THROUGH_RUN_BASES_SQL, (self.branch_name,),
            )
            through_run_bases = {int(row[0]) for row in cur.fetchall()}
        if set(since_previous) <= through_run_bases:
            return [int(segment) for segment in since_previous]
        return False

    @staticmethod
    def _chronos_embedding_writes_since(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        previous: Any,
    ) -> int:
        """Nodes given a vector from a different model since ``previous``.

        PACT openrouter-embeddings-hnsw. Writing a vector (the embed worker or
        ``okg db embed-backfill``) makes a new row version whose subtype,
        attrs and source are unchanged, so the delta and the dirty scope,
        which compare only those columns, call the graph unchanged and the
        no-op path reuses the previous generation: the vectors are never
        readable at a pin. This count is used ONLY to refuse that reuse.
        It deliberately stays out of the dirty scope and the delta counts,
        so an embedding write invalidates no enricher, extractor, view or
        rollup input and is not reported as an updated node. A vector
        replaced under the same model has also changed text, which the
        attrs comparison already sees.
        """
        previous_ref = getattr(previous, "ref", None)
        if previous_ref is None:
            return 0
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT physical_table
                  FROM _chronos_branch_tables
                 WHERE table_name = 'okg.graph_nodes'
                   AND backend = 'interval'
                """
            )
            row = cur.fetchone()
        if row is None:
            return 0
        _added, _retracted, updated = Publisher._chronos_segment_table_delta(
            conn,
            physical_table=str(row[0]),
            key_column="node_id",
            compare_columns=("embedding_model",),
            # A vector the fact write path cleared (writes.py stores
            # embedding NULL on every node upsert) must not force a new
            # generation: that generation would only drop vectors.
            changed_requires_non_null=("embedding_model",),
            branch_name=branch_name,
            previous_segment_id=str(previous_ref),
            since_base_segments=Publisher._chronos_segments_written_since(
                conn,
                branch_name=branch_name,
                base_segment_id=str(previous_ref),
            ),
        )
        return int(updated)

    @staticmethod
    def _chronos_segments_written_since(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        base_segment_id: str,
    ) -> list[int]:
        """Segments whose rows the branch wrote or inherited since the base.

        Current segment first. The walk stops at the first segment whose
        interval contains the base's; it does not otherwise verify that the
        base is on the branch's lineage (every branch shares the root, so the
        walk always ends). It raises only when the base's segment row or the
        branch's current segment row is missing, rather than narrowing to the
        current segment and silently dropping rows from the gate (#2865).
        """
        with conn.cursor() as cur:
            cur.execute(
                _CHRONOS_SEGMENTS_WRITTEN_SINCE_BASE_SQL,
                (base_segment_id, branch_name),
            )
            rows = cur.fetchall()
        segments = [int(segment_id) for segment_id, shared in rows if not shared]
        if not rows or not any(bool(shared) for _segment_id, shared in rows):
            raise RuntimeError(
                "dirty scope: base segment is not on the branch lineage "
                f"(branch={branch_name}, base_segment={base_segment_id})"
            )
        if not segments:
            raise RuntimeError(
                "dirty scope: the branch's current segment contains its base "
                f"(branch={branch_name}, base_segment={base_segment_id})"
            )
        return segments

    @staticmethod
    def _query_dirty_from_chronos_current_segment_by_type(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        previous: Any | None = None,
        timing_sink: dict[str, Any] | None = None,
        timing_prefix: str = "chronos_dirty_scope.query",
        since_fork_base: bool = False,
        only_node_ids: Sequence[str] | None = None,
        only_edge_ids: Sequence[int] | None = None,
        counts_sink: dict[str, int] | None = None,
    ) -> _ChronosDirtyScope | None:
        """Return graph keys written since ``previous``.

        With ``previous`` the seed is every row written since it along the
        branch's segment chain: the current segment, plus older segments of
        this branch or its parents when a fork, or a BLOCKED generation's
        checkpoint, moved the branch off them after ``previous`` (issue
        #2865). With neither since ``previous`` that is the current segment
        alone and the single-segment statement runs. ``since_fork_base`` marks a fork's first publish, where
        ``previous`` is the fork's recorded base on default; it only makes a
        missing ``previous`` an error.

        Chronos-native ingestion writes directly to interval-backed
        ``okg.graph_nodes`` / ``okg.graph_edges`` and leaves legacy
        ``node_facts`` empty. After every checkpoint Chronos moves the
        mutable branch to a continuation segment. ``writer_segment_id`` marks
        the rows actually written in that continuation, so it is the bounded
        dirty-set seed; the previous branch point is still used to compare
        updates and tombstones against their prior live row.

        ``only_node_ids`` / ``only_edge_ids`` (both or neither, and only
        with ``previous``) read the same scope for just those keys: the
        result is what a full read would hold for them (issue #2941).

        ``counts_sink``: for a read since ``previous`` that is not keyed, it
        receives the six delta counts of the rows read, classified as
        ``_chronos_segment_table_delta`` classifies them (issue #2941). The
        rows are the ones that statement counts: in the single-segment form
        the current segment's, and since a segment split (a run-base
        checkpoint, a fork or a blocked generation) the same segments read
        at the same branch point. It is left empty otherwise.
        """
        keyed = only_node_ids is not None or only_edge_ids is not None
        if keyed and (
            only_node_ids is None or only_edge_ids is None or previous is None
        ):
            raise ValueError(
                "a keyed dirty-scope read needs node ids, edge ids and a "
                "previous generation"
            )
        def add_timing(label: str, started: float) -> None:
            if timing_sink is None:
                return
            ended = time.perf_counter()
            elapsed = max(0.0, (ended - started) * 1000.0)
            key = f"{timing_prefix}.{label}"
            timing_sink[key] = round(
                float(timing_sink.get(key, 0.0) or 0.0) + elapsed,
                3,
            )
            timing_events = timing_sink.get(_PUBLISHER_TIMING_EVENTS_KEY)
            if isinstance(timing_events, list):
                timing_events.append({
                    "name": key,
                    "started_perf": started,
                    "ended_perf": ended,
                })

        try:
            with conn.cursor() as cur:
                started = time.perf_counter()
                cur.execute(
                    """
                    SELECT table_name, physical_table
                      FROM _chronos_branch_tables
                     WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
                       AND backend = 'interval'
                    """
                )
                tables = {str(name): str(physical) for name, physical in cur}
                add_timing("table_lookup", started)
            node_table = tables.get("okg.graph_nodes")
            edge_table = tables.get("okg.graph_edges")
            if not node_table:
                return None

            previous_segment_id = None
            previous_ref = getattr(previous, "ref", None)
            if previous_ref is not None:
                previous_segment_id = str(previous_ref)
            if keyed and previous_segment_id is None:
                raise ValueError(
                    "a keyed dirty-scope read needs the previous generation's "
                    "segment"
                )
            if since_fork_base and previous_segment_id is None:
                # Without the base point there is no "since"; the single-segment
                # seed would silently drop the parent's rows (issue #2865).
                raise RuntimeError(
                    "fork-first dirty scope requires the fork base checkpoint "
                    f"segment (branch={branch_name})"
                )
            # Every row written since `previous`, along the branch's own
            # segment chain (issue #2865). With no fork and no blocked
            # generation since `previous` that is only the current segment,
            # and the original single-segment statement runs unchanged.
            seed_params: tuple[Any, ...] = (branch_name, previous_segment_id)
            seed_since_base = False
            if previous_segment_id is not None:
                started = time.perf_counter()
                since_base_segments = (
                    Publisher._chronos_segments_written_since(
                        conn,
                        branch_name=branch_name,
                        base_segment_id=previous_segment_id,
                    )
                )
                add_timing("segments_since_base", started)
                if len(since_base_segments) > 1:
                    seed_since_base = True
                    seed_params = (
                        branch_name, previous_segment_id, since_base_segments,
                    )
            nodes_by_subtype: dict[str, list[str]] = {}
            node_attr_changes_by_subtype: dict[str, dict[str, list[str]]] = {}
            edges_by_type: dict[str, list[int]] = {}
            edge_table_available = bool(edge_table)
            with conn.cursor() as cur:
                started = time.perf_counter()
                if previous_segment_id is None:
                    cur.execute(
                        sql.SQL(
                            """
                            WITH branch AS (
                              SELECT current_segment_id
                                FROM _chronos_branch_interval_branches
                               WHERE branch_id = %s
                            )
                            SELECT DISTINCT ON (n.node_id) n.node_id, n.subtype
                              FROM {node_table} n
                              JOIN branch b
                                ON n.writer_segment_id = b.current_segment_id
                             ORDER BY n.node_id, n.live_lo DESC
                            """
                        ).format(node_table=sql.Identifier(node_table)),
                        (branch_name,),
                    )
                elif keyed and not only_node_ids:
                    cur.execute("SELECT 1 WHERE FALSE")
                else:
                    cur.execute(
                        _chronos_dirty_nodes_since_previous_sql(
                            node_table,
                            since_base=seed_since_base,
                            keyed=keyed,
                            fields_in_sql=True,
                        ),
                        (
                            (*seed_params, sorted({str(i) for i in only_node_ids}))
                            if keyed and only_node_ids is not None
                            else seed_params
                        ),
                    )
                node_rows = cur.fetchall()
                add_timing("nodes_query", started)
                started = time.perf_counter()
                if previous_segment_id is None:
                    for node_id, subtype in node_rows:
                        node_id_text = str(node_id)
                        subtype_text = str(subtype)
                        nodes_by_subtype.setdefault(subtype_text, []).append(
                            node_id_text
                        )
                        node_attr_changes_by_subtype.setdefault(
                            subtype_text,
                            {},
                        )[node_id_text] = ["__new__"]
                else:
                    count_rows = counts_sink is not None and not keyed
                    counted = dict.fromkeys(_CHRONOS_DELTA_COUNT_KEYS, 0)
                    for (
                        node_id,
                        subtype,
                        deleted,
                        previous_subtype,
                        previous_deleted,
                        payload_changed,
                        changed_fields,
                    ) in node_rows:
                        node_id_text = str(node_id)
                        subtype_text = str(subtype)
                        if count_rows:
                            # previous_deleted is NULL only when no row was
                            # visible at the previous point.
                            now_deleted = bool(deleted)
                            was_live = (
                                previous_deleted is not None
                                and not bool(previous_deleted)
                            )
                            if not now_deleted and not was_live:
                                counted["nodes_added"] += 1
                            elif now_deleted and was_live:
                                counted["nodes_retracted"] += 1
                            elif not now_deleted and bool(payload_changed):
                                # Compared in SQL, as jsonb, the way the
                                # count statement compares: decoded Python
                                # values equate true with 1.
                                counted["nodes_updated"] += 1
                        nodes_by_subtype.setdefault(subtype_text, []).append(
                            node_id_text
                        )
                        # The changed-field list comes from the database
                        # (_CHRONOS_DIRTY_NODE_FIELDS_SQL). A SOURCE
                        # reattribution is marked `__source__`: it moves the
                        # node's by-source count bucket, and the console count
                        # rollup forces a full recount on it (console-count-
                        # rollup / #444 slice 2). Over-invalidation is
                        # correctness-safe (rebuild more, never serve stale).
                        fields = {str(field) for field in (changed_fields or ())}
                        node_attr_changes_by_subtype.setdefault(
                            subtype_text,
                            {},
                        )[node_id_text] = sorted(fields)
                add_timing("nodes_process", started)

                if edge_table_available:
                    started = time.perf_counter()
                    if previous_segment_id is None:
                        cur.execute(
                            sql.SQL(
                                """
                                WITH branch AS (
                                  SELECT current_segment_id
                                    FROM _chronos_branch_interval_branches
                                   WHERE branch_id = %s
                                )
                                SELECT DISTINCT ON (e.edge_id) e.edge_id, e.edge_type
                                  FROM {edge_table} e
                                  JOIN branch b
                                    ON e.writer_segment_id = b.current_segment_id
                                 ORDER BY e.edge_id, e.live_lo DESC
                                """
                            ).format(edge_table=sql.Identifier(edge_table)),
                            (branch_name,),
                        )
                    elif keyed and not only_edge_ids:
                        cur.execute("SELECT 1 WHERE FALSE")
                    else:
                        cur.execute(
                            _chronos_dirty_edges_since_previous_sql(
                                edge_table, since_base=seed_since_base, keyed=keyed,
                            ),
                            (
                                (
                                    *seed_params,
                                    sorted({int(i) for i in only_edge_ids}),
                                )
                                if keyed and only_edge_ids is not None
                                else seed_params
                            ),
                        )
                    edge_rows = cur.fetchall()
                    add_timing("edges_query", started)
                    started = time.perf_counter()
                    for edge_row in edge_rows:
                        edge_id, edge_type = edge_row[0], edge_row[1]
                        edges_by_type.setdefault(str(edge_type), []).append(
                            int(edge_id)
                        )
                        if previous_segment_id is not None and count_rows:
                            now_deleted = bool(edge_row[2])
                            was_live = bool(edge_row[3])
                            if not now_deleted and not was_live:
                                counted["edges_added"] += 1
                            elif now_deleted and was_live:
                                counted["edges_retracted"] += 1
                            elif not now_deleted and bool(edge_row[4]):
                                counted["edges_updated"] += 1
                    add_timing("edges_process", started)
                    if (
                        previous_segment_id is not None
                        and count_rows
                        and counts_sink is not None
                    ):
                        counts_sink.clear()
                        for count_key, count_value in counted.items():
                            counts_sink[count_key] = count_value
            return (
                nodes_by_subtype,
                edges_by_type,
                edge_table_available,
                node_attr_changes_by_subtype,
                {},
                None,
            )
        except Exception:
            # The clean non-interval case returns None above (no node_table)
            # without raising; reaching here means a real query/metadata error
            # while computing the dirty scope. Surface it instead of silently
            # downgrading to fact watermarks or full view rebuilds.
            logging.getLogger(__name__).exception(
                "chronos segment-delta query failed for branch=%s",
                branch_name,
            )
            raise

    @staticmethod
    def _enricher_run_is_complete(result: Any) -> bool:
        """True iff the enricher run fully processed its input delta.

        A run that RAISED (``result is None``) or completed but left
        inputs unprocessed (``result.incomplete`` — e.g. a per-item LLM
        call failed) is NOT complete. The publisher advances an
        enricher's fact/edge watermark only for a complete run; an
        incomplete run holds the watermark so the next cycle re-reads
        and retries the unprocessed inputs. Advancing past a failed
        item silently and permanently loses its typed nodes (a P0
        data-loss bug), so this predicate — not merely ``result is not
        None`` — gates the advance.
        """
        return result is not None and not getattr(result, "incomplete", False)

    _SOURCE_ALIAS_INDEX_UNSET = object()

    def _source_alias_index(self) -> dict[str, str] | None:
        """Every accepted alias for a registered source -> its ownership id.

        Issue #1528 F5. `None` means "no registry was loadable for this
        deployment", which is NOT the same as "the registry is empty": the
        publisher runs in test harnesses and ephemeral deployments that have
        no `source_registry.yaml` on disk, and refusing every
        source-declaring enricher there would wedge publishing over a missing
        file rather than a real declaration defect. Callers therefore SKIP
        validation on `None` and validate on a non-empty index. The gap is
        real and stated: a deployment whose registry cannot be read gets the
        old, ungated behaviour for its declared sources.

        Memoized for the life of the Publisher: the registry is a file read
        and the enricher loop asks once per enricher.
        """
        cached = getattr(
            self, "_source_alias_index_cache", self._SOURCE_ALIAS_INDEX_UNSET,
        )
        if cached is not self._SOURCE_ALIAS_INDEX_UNSET:
            return cached

        index: dict[str, str] | None = None
        try:
            from okg.substrate.deployments import find_deployment_dir
            from okg.substrate.sources.registry import load_source_registry

            dep_dir = find_deployment_dir(self.deployment_name)
            registry_path = (
                dep_dir / "source_registry.yaml" if dep_dir else None
            )
            if registry_path is not None and registry_path.is_file():
                entries = load_source_registry(registry_path)
                built = source_alias_index(entries.values())
                index = built or None
                if index is None:
                    # Parsed, but nothing usable came out of it. Same
                    # ungated outcome as an unreadable registry, so it
                    # gets the same signal.
                    logging.getLogger(__name__).warning(
                        "enricher declared-source validation skipped: "
                        "source registry for deployment %s has no usable "
                        "entries (%s); gating continues unnormalized",
                        self.deployment_name, registry_path,
                    )
            else:
                # No registry on disk is an ordinary state for a deployment
                # that never wired declared sources, so this is info rather
                # than a warning -- but it still means no validation ran.
                logging.getLogger(__name__).info(
                    "enricher declared-source validation skipped: no "
                    "source_registry.yaml on disk for deployment %s; "
                    "gating continues unnormalized",
                    self.deployment_name,
                )
        except Exception as exc:  # noqa: BLE001 - see below
            # A registry that does not parse is a real deployment fault, but
            # it is NOT this code path's fault to report: `okg doctor
            # --check wiring` and ingest both fail loudly on it already.
            # Logging and declining to validate keeps one broken file from
            # turning every publish into an enricher refusal storm.
            logging.getLogger(__name__).warning(
                "enricher input-source validation skipped: "
                "source registry unreadable for deployment %s (%s)",
                self.deployment_name, exc,
            )
            index = None
        self._source_alias_index_cache = index
        return index

    def _advance_enricher_progress(
        self,
        conn: psycopg.Connection,
        ename: str,
        *,
        success: bool,
        failure_repr: Optional[str],
        generation_id: int,
        catalog_version_id: int,
        current_fact_watermark: int | None = None,
        current_edge_watermark: int | None = None,
        emitted_count: int = 0,
        dormant_ok: bool = False,
        counts_as_failure: bool = True,
    ) -> int:
        """Upsert the progress row.

        On success: advance fact_watermark / edge_watermark to
        current `max(fact_id)`, set `last_status='success'`.
        On failure: leave watermarks at their prior values
        (the next publish retries the same delta), set
        `last_status='failed:<exc>'`. `last_run_at` advances on
        every invocation (success OR failure). Returns the unresolved
        consecutive failure count after the upsert.

        `emitted_count` (this cycle's ``EnrichResult.n_edges_emitted``)
        is ADDED to the row's lifetime accumulator on every invocation —
        success OR failure — because edges an incomplete run already
        admitted still count as real output. A wired enricher whose
        lifetime total never leaves 0 has produced nothing since it was
        wired; console diagnostics and `okg doctor --check
        enricher-liveness` surface that as ``wired-but-silent`` (#707)
        instead of the byte-identical "healthy". `dormant_ok` stamps the
        enricher's declared-liveness escape (design point (i)) so a
        legitimately-quiet enricher suppresses the flag by declaration.
        A negative `emitted_count` is a programming error and raises.

        `counts_as_failure` (issue #1528) separates "the watermark must
        hold" from "this producer is failing". A run withheld because its
        declared input source run is still held off `default` did not fail --
        nothing went wrong and there is nothing to retry differently -- so
        it holds the watermark like a failure but leaves
        `consecutive_failure_count` untouched, keeping the enricher out of
        the failure alarms. `last_status` still records the withholding
        verbatim, so the hold is observable rather than silent.
        """
        if emitted_count < 0:
            raise ValueError(
                "emitted_count must be non-negative; got "
                f"{emitted_count!r} for enricher {ename!r}"
            )
        with conn.cursor() as cur:
            if success:
                if (
                    current_fact_watermark is None
                    or current_edge_watermark is None
                ):
                    (
                        current_fact_watermark,
                        current_edge_watermark,
                    ) = self._current_fact_watermarks(conn)
                cur.execute(
                    """
                    INSERT INTO okg.enricher_progress
                      (deployment_name, enricher_name,
                       fact_watermark, edge_watermark,
                       generation_id, catalog_version_id,
                       last_run_at, last_status,
                       lifetime_edges_emitted, dormant_ok,
                       consecutive_failure_count)
                    VALUES (
                      %s, %s, %s, %s, %s, %s, now(), 'success', %s, %s, 0
                    )
                    ON CONFLICT (deployment_name, enricher_name)
                      DO UPDATE SET
                        fact_watermark = EXCLUDED.fact_watermark,
                        edge_watermark = EXCLUDED.edge_watermark,
                        generation_id = EXCLUDED.generation_id,
                        catalog_version_id = EXCLUDED.catalog_version_id,
                        last_run_at   = EXCLUDED.last_run_at,
                        last_status   = 'success',
                        -- Accumulate; never reset. A zero-emit cycle adds
                        -- 0, so a perpetually-silent enricher stays at 0.
                        lifetime_edges_emitted =
                          okg.enricher_progress.lifetime_edges_emitted
                          + EXCLUDED.lifetime_edges_emitted,
                        dormant_ok = EXCLUDED.dormant_ok,
                        consecutive_failure_count = 0
                    RETURNING consecutive_failure_count
                    """,
                    (
                        self.deployment_name,
                        ename,
                        current_fact_watermark,
                        current_edge_watermark,
                        generation_id,
                        catalog_version_id,
                        int(emitted_count),
                        bool(dormant_ok),
                    ),
                )
                return int(cur.fetchone()[0])
            else:
                cur.execute(
                    """
                    INSERT INTO okg.enricher_progress
                      (deployment_name, enricher_name,
                       fact_watermark, edge_watermark,
                       generation_id, catalog_version_id,
                       last_run_at, last_status,
                       lifetime_edges_emitted, dormant_ok,
                       consecutive_failure_count)
                    VALUES (%s, %s, 0, 0, %s, %s, now(), %s, %s, %s, {new})
                    ON CONFLICT (deployment_name, enricher_name)
                      DO UPDATE SET
                        last_run_at = EXCLUDED.last_run_at,
                        last_status = EXCLUDED.last_status,
                        generation_id = EXCLUDED.generation_id,
                        catalog_version_id = EXCLUDED.catalog_version_id,
                        -- Edges a partial/incomplete run already admitted
                        -- are real output and still accumulate; the
                        -- watermark is held elsewhere, the count is not.
                        lifetime_edges_emitted =
                          okg.enricher_progress.lifetime_edges_emitted
                          + EXCLUDED.lifetime_edges_emitted,
                        dormant_ok = EXCLUDED.dormant_ok,
                        consecutive_failure_count = {bump}
                    RETURNING consecutive_failure_count
                    """.format(
                        new="1" if counts_as_failure else "0",
                        bump=(
                            "okg.enricher_progress"
                            ".consecutive_failure_count + 1"
                            if counts_as_failure
                            else "okg.enricher_progress"
                                 ".consecutive_failure_count"
                        ),
                    ),
                    (
                        self.deployment_name, ename,
                        generation_id,
                        catalog_version_id,
                        failure_repr or "failed:unknown",
                        int(emitted_count),
                        bool(dormant_ok),
                    ),
                )
                return int(cur.fetchone()[0])

    # ---------- Phase B (stub at M1) ----------

    def run_gates(
        self,
        g: ReservedGeneration,
        phase_a: PhaseAResult,
    ) -> PhaseBResult:
        """Evaluate source-local + cross-source gates (M5.2).

        Delegates to `okg.substrate.publisher.gates.evaluate_gates`.
        Runs OUTSIDE the publish txn (each invariant opens its
        own read-only conn with per-invariant statement_timeout).
        """
        if self.consistency_gates:
            # This method cannot evaluate consistency gates, so it refuses
            # before doing any work. The live cycle evaluates them in
            # `_cycle_chronos_native`, which knows the pre_view/post_view pass
            # split (#1426) and prefers a gate's `evaluate_incremental` when it
            # exposes one; neither is reachable from here. A second, divergent
            # implementation used to sit below and was shadowed by the live one
            # from 2026-08-27, so every call raised TypeError instead. Refusing
            # loudly keeps that honest: a rewired caller gets a refusal, not
            # gates evaluated by the wrong rules.
            raise NotImplementedError(
                "Publisher.run_gates cannot evaluate consistency gates; the "
                "publish cycle evaluates them in _cycle_chronos_native"
            )

        cache_key = self._gate_cache_key(g)
        cached = self._try_reuse_gate_result(g, phase_a, cache_key)
        if cached is not None:
            return cached

        result = evaluate_gates(
            self.dsn, g, phase_a,
            total_timeout_ms=self.gate_total_timeout_ms,
            branch_name=self.branch_name,
            source_states=self._source_states,
        )
        return replace(result, gate_cache_key=cache_key)

    @staticmethod
    def _consistency_gate_summary(
        evaluation: ConsistencyPublishGateEvaluation,
    ) -> dict[str, Any]:
        return consistency_gate_summary(evaluation)

    def _evaluate_consistency_gates(
        self,
        conn: psycopg.Connection,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        previous_catalog_version_id: int | None,
        delta_counts: Mapping[str, Any] | None,
        dirty_scope: Any,
        gate_pass: str,
    ) -> list[dict[str, Any]]:
        """Run every configured consistency gate once and summarize each.

        ``gate_pass`` names which set of writes this run covers. The cycle runs
        the gates twice: ``pre_view`` over the source/enricher writes, and
        ``post_view`` over the branch after native views, lenses, and their
        derived facts have been applied (#1426). Those facts land AFTER the
        first pass and BEFORE checkpoint creation, so without the second pass
        nothing structural ever sees them.
        """
        summaries: list[dict[str, Any]] = []
        for gate in self.consistency_gates:
            gate_name = str(getattr(gate, "name", gate.__class__.__name__))
            t0 = time.monotonic()
            try:
                evaluate_incremental = getattr(
                    gate,
                    "evaluate_incremental",
                    None,
                )
                if callable(evaluate_incremental):
                    evaluation = evaluate_incremental(
                        conn,
                        target_generation_id=target_generation_id,
                        catalog_version_id=catalog_version_id,
                        deployment_name=self.deployment_name,
                        previous_catalog_version_id=previous_catalog_version_id,
                        delta_counts=delta_counts,
                        dirty_scope=dirty_scope,
                        branch_name=self.branch_name,
                    )
                else:
                    evaluation = gate.evaluate(
                        conn,
                        target_generation_id=target_generation_id,
                        catalog_version_id=catalog_version_id,
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                    )
                summary = self._consistency_gate_summary(evaluation)
                summary["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
            except Exception as exc:
                summary = {
                    "check_id": gate_name,
                    "gate_name": gate_name,
                    "target_generation_id": target_generation_id,
                    "result": "blocked_error",
                    "blocks_publish": True,
                    "reason": "gate_exception",
                    "open_observation_count": 0,
                    "newly_opened_count": 0,
                    "resolved_count": 0,
                    "coverage_complete": None,
                    "coverage_basis": None,
                    "budget_ms": None,
                    "elapsed_ms": int((time.monotonic() - t0) * 1000),
                    "details": {
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                }
            details = summary.get("details")
            if isinstance(details, dict):
                details["gate_pass"] = gate_pass
            summaries.append(summary)
        return summaries

    @staticmethod
    def _merge_consistency_blocked_reason(
        existing: dict[str, Any] | None,
        blocked_gates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if existing is None:
            return {
                "reason": "consistency_publish_gate_blocked",
                "failed_consistency_gates": blocked_gates,
            }
        return {
            "reason": "multiple_gate_failures",
            "phase_b": existing,
            "failed_consistency_gates": blocked_gates,
        }

    def _gate_cache_key(
        self,
        g: ReservedGeneration,
        *,
        ignore_derived_artifact_facts: bool = False,
    ) -> dict[str, Any]:
        """Build the conservative dependency key for gate reuse.

        The key intentionally includes the database evaluation date
        because several af_pubs invariants use ``CURRENT_DATE`` in
        their SQL. A graph no-op across midnight must run gates again.
        """
        conn = g._conn
        fact_filter = (
            "WHERE source NOT LIKE 'derived_artifact:%%'"
            if ignore_derived_artifact_facts else ""
        )
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COALESCE(max(fact_id), 0) FROM okg.node_facts "
                f"{fact_filter}"
            )
            fact_watermark = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"SELECT COALESCE(max(fact_id), 0) FROM okg.edge_facts "
                f"{fact_filter}"
            )
            edge_fact_watermark = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT CURRENT_DATE::text")
            evaluation_date = str(cur.fetchone()[0])
            cur.execute(
                """
                SELECT name, severity, scope, source, sql_body,
                       timeout_ms, kind, fire_on, concept_scope,
                       requires_sources
                  FROM okg.invariants
                 WHERE catalog_version_id = %s
                   AND kind = 'sql'
                 ORDER BY scope, source NULLS FIRST, name
                """,
                (g.catalog_version_id,),
            )
            invariant_rows = [
                {
                    "name": r[0],
                    "severity": r[1],
                    "scope": r[2],
                    "source": r[3],
                    "sql_body": r[4],
                    "timeout_ms": r[5],
                    "kind": r[6],
                    "fire_on": list(r[7]) if r[7] else None,
                    "concept_scope": r[8],
                    "requires_sources": list(r[9]) if r[9] else None,
                }
                for r in cur.fetchall()
            ]
        invariant_fingerprint = hashlib.sha256(
            json.dumps(
                invariant_rows,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:16]
        consistency_gate_identities: list[dict[str, Any]] = []
        for gate in self.consistency_gates:
            identity_fn = getattr(gate, "cache_identity", None)
            if callable(identity_fn):
                identity = dict(identity_fn())
            else:
                identity = {
                    "gate_name": str(getattr(gate, "name", gate.__class__.__name__)),
                    "gate_type": (
                        f"{gate.__class__.__module__}.{gate.__class__.__qualname__}"
                    ),
                }
            consistency_gate_identities.append(identity)
        consistency_gate_fingerprint = hashlib.sha256(
            json.dumps(
                consistency_gate_identities,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:16]
        return {
            "version": 2,
            "deployment": g.deployment_name,
            "catalog_version_id": g.catalog_version_id,
            "fact_watermark": fact_watermark,
            "edge_fact_watermark": edge_fact_watermark,
            "evaluation_date": evaluation_date,
            "invariant_fingerprint": invariant_fingerprint,
            "consistency_gate_fingerprint": consistency_gate_fingerprint,
            # Issue #3624: whether each required source is on decides which
            # invariants run, so a source switched on or off must not reuse
            # a result computed under the other setting.
            "required_source_states": self._required_source_states(
                invariant_rows,
            ),
        }

    _SOURCE_STATES_UNSET = object()

    def _source_states(self) -> dict[str, bool] | None:
        """Each registered source of this deployment -> whether it is on.

        Issue #3624: invariants with `requires_sources` are skipped while a
        named source is off. "On" follows the ingest planner's default rule:
        a source runs unless its registry entry says `enabled_by_default:
        false`. `None` means no registry could be read; the gate then
        evaluates those invariants anyway and reports that it could not
        check. Memoized for the life of the Publisher.
        """
        cached = getattr(self, "_source_states_cache", self._SOURCE_STATES_UNSET)
        if cached is not self._SOURCE_STATES_UNSET:
            return cached
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            find_deployment_dir,
        )
        from okg.substrate.sources.registry import load_source_registry
        import yaml

        states: dict[str, bool] | None = None
        try:
            dep_dir = find_deployment_dir(self.deployment_name)
        except DeploymentResolutionError as exc:
            logging.getLogger(__name__).warning(
                "deployment %s not resolvable for its source registry (%s); "
                "invariants with requires_sources are evaluated as usual",
                self.deployment_name, exc,
            )
            dep_dir = None
        registry_path = dep_dir / "source_registry.yaml" if dep_dir else None
        if registry_path is not None and registry_path.is_file():
            try:
                entries = load_source_registry(registry_path)
            except (OSError, ValueError, yaml.YAMLError, SubstrateError) as exc:
                # The gate lists every requires_sources invariant as
                # unchecked and evaluates it, so this cannot pass silently.
                logging.getLogger(__name__).warning(
                    "source registry unreadable for deployment %s (%s); "
                    "invariants with requires_sources are evaluated as usual",
                    self.deployment_name, exc,
                )
            else:
                states = {
                    name: entry.raw.get("enabled_by_default") is not False
                    for name, entry in entries.items()
                }
        self._source_states_cache = states
        return states

    def _required_source_states(
        self, invariant_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        required = sorted({
            name
            for row in invariant_rows
            for name in (row.get("requires_sources") or [])
        })
        if not required:
            return None
        states = self._source_states()
        if states is None:
            return {"registry": "unreadable"}
        return {name: states.get(name, False) for name in required}

    def _try_reuse_gate_result(
        self,
        g: ReservedGeneration,
        phase_a: PhaseAResult,
        cache_key: dict[str, Any],
    ) -> PhaseBResult | None:
        if not phase_a.converged:
            return None
        if any(
            status == SourceProjectionStatus.FAILED
            for status in phase_a.per_source_status.values()
        ):
            return None
        with g._conn.cursor() as cur:
            cur.execute(
                """
                SELECT generation_id, invariant_summary, excluded_sources
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND status = 'published'
                   AND generation_id < %s
                 ORDER BY generation_id DESC
                 LIMIT 1
                """,
                (g.deployment_name, g.generation_id),
            )
            row = cur.fetchone()
        if row is None:
            return None
        prev_generation_id, prev_summary, prev_excluded = row
        prev_summary = dict(prev_summary or {})
        if prev_summary.get("gate_cache_key") != cache_key:
            return None
        if prev_excluded not in ({}, [], None):
            return None

        summary = {
            "cross_source": prev_summary.get("cross_source") or {},
            "source_local": prev_summary.get("source_local") or {},
            "total_duration_ms": 0,
            "total_budget_exceeded": bool(
                prev_summary.get("total_budget_exceeded", False)
            ),
            "gate_cache_key": cache_key,
            "gate_reuse": {
                "reused": True,
                "from_generation_id": int(prev_generation_id),
            },
        }
        if prev_summary.get("substrate_integrity"):
            summary["substrate_integrity"] = prev_summary[
                "substrate_integrity"
            ]
        for key in ("skipped_invariants", "unchecked_source_requirements"):
            # Same key -> same source states -> the same skips still apply.
            if prev_summary.get(key):
                summary[key] = prev_summary[key]
        logging.getLogger(__name__).info(
            "reusing gate result from generation %s for generation %s",
            prev_generation_id,
            g.generation_id,
        )
        return PhaseBResult(
            source_local={},
            cross_source=[],
            excluded_sources={},
            blocked=False,
            blocked_reason=None,
            total_duration_ms=0,
            total_budget_exceeded=summary["total_budget_exceeded"],
            gate_cache_key=cache_key,
            invariant_summary_override=summary,
        )

    # ---------- Top-level orchestration ----------

    @staticmethod
    def _active_nomos_policy_pin(
        conn: psycopg.Connection, deployment_name: str
    ) -> tuple[list[str], list[str]]:
        """Resolve the deployment's active Nomos policy pin -- the distinct active
        ``policy_bundle_id``s and ``policy_fingerprint``s -- to record on the published
        generation's checkpoint metadata. (Pre-cutover this was an UPDATE of
        okg.graph_generations.nomos_policy_{bundle_ids,fingerprints}.)
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COALESCE(array_agg(
                    DISTINCT b.policy_bundle_id ORDER BY b.policy_bundle_id),
                    ARRAY[]::text[]),
                  COALESCE(array_agg(
                    DISTINCT a.policy_fingerprint ORDER BY a.policy_fingerprint),
                    ARRAY[]::text[])
                  FROM okg.nomos_policy_activations a
                  JOIN okg.nomos_policy_bundles b
                    ON b.deployment_name = a.deployment_name
                   AND b.policy_fingerprint = a.policy_fingerprint
                 WHERE a.deployment_name = %s
                   AND a.status = 'active'
                   AND b.status = 'active'
                """,
                (deployment_name,),
            )
            bundle_ids, fingerprints = cur.fetchone()
        return list(bundle_ids or []), list(fingerprints or [])

    def _nomos_generation_publish_blocker(
        self,
        conn: psycopg.Connection,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        node_count: int,
        edge_count: int,
        projection_deltas: Mapping[str, Any],
        nomos_bundle_ids: list[str],
        nomos_fingerprints: list[str],
        commit: bool = True,
    ) -> dict[str, Any] | None:
        from okg.substrate.nomos import (
            NomosRuntimeRequest,
            deployment_requires_nomos_runtime,
            evaluate_runtime_boundary_with_active_policy,
        )

        if not deployment_requires_nomos_runtime(self.deployment_name):
            return None
        scope = f"deployment:{self.deployment_name}"
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=self.deployment_name,
            scope=scope,
            request=NomosRuntimeRequest(
                surface="generation.publish",
                subject="okg_service:publisher",
                classification="internal",
                resource={
                    "target_generation_id": target_generation_id,
                    "catalog_version_id": catalog_version_id,
                    "branch_name": self.branch_name,
                    "node_count": node_count,
                    "edge_count": edge_count,
                    "projection_deltas": dict(projection_deltas),
                    "nomos_policy_bundle_ids": list(nomos_bundle_ids),
                    "nomos_policy_fingerprints": list(nomos_fingerprints),
                },
            ),
            target={
                "kind": "generation_publish",
                "target_generation_id": target_generation_id,
                "branch_name": self.branch_name,
            },
            performed_obligations=("audit",),
        )
        if commit:
            conn.commit()
        if outcome is None or outcome.allowed:
            return None
        return {
            "reason": "nomos_generation_publish_denied",
            "nomos_decision_id": outcome.decision.decision_id,
            "nomos_effect": outcome.decision.effect,
            "nomos_reason_code": outcome.decision.reason_code,
            "nomos_final_effect": outcome.final_effect,
        }

    def _chronos_generation_delta_counts(
        self,
        conn: psycopg.Connection,
        *,
        store: Any,
        branch_name: str,
        previous: Any,
    ) -> dict[str, int]:
        """Count cardinality deltas against the prior Chronos generation.

        The normal interval-backend path is proportional to writes confined to
        the branch's current continuation segment: rows written since the
        previous checkpoint are classified as additions, updates, or
        retractions by probing whether their key existed at the prior
        checkpoint. The full-set checkout diff remains as a correctness
        fallback for non-interval backends or missing Chronos metadata.
        """

        segment_delta = self._chronos_current_segment_delta_counts(
            conn,
            branch_name=branch_name,
            previous=previous,
        )
        if segment_delta is not None:
            return segment_delta

        branch = store.checkout(branch_name)
        current_node_ids = {
            row["node_id"]
            for row in branch.query("SELECT node_id FROM okg.graph_nodes")
        }
        current_edge_ids = {
            row["edge_id"]
            for row in branch.query("SELECT edge_id FROM okg.graph_edges")
        }
        previous_checkout = store.checkout_generation(
            previous.checkpoint_id,
            branch=branch_name,
        )
        previous_node_ids = {
            row["node_id"]
            for row in previous_checkout.query(
                "SELECT node_id FROM okg.graph_nodes"
            )
        }
        previous_edge_ids = {
            row["edge_id"]
            for row in previous_checkout.query(
                "SELECT edge_id FROM okg.graph_edges"
            )
        }
        return {
            "nodes_added": len(current_node_ids - previous_node_ids),
            "nodes_retracted": len(previous_node_ids - current_node_ids),
            "nodes_updated": 0,
            "edges_added": len(current_edge_ids - previous_edge_ids),
            "edges_retracted": len(previous_edge_ids - current_edge_ids),
            "edges_updated": 0,
        }

    @staticmethod
    def _chronos_current_segment_delta_counts(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        previous: Any,
    ) -> dict[str, int] | None:
        previous_ref = getattr(previous, "ref", None)
        if previous_ref is None:
            return None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, physical_table
                      FROM _chronos_branch_tables
                     WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
                       AND backend = 'interval'
                    """
                )
                tables = {str(name): str(physical) for name, physical in cur}
            node_table = tables.get("okg.graph_nodes")
            edge_table = tables.get("okg.graph_edges")
            if not node_table or not edge_table:
                return None
            since_base_segments = Publisher._chronos_segments_written_since(
                conn,
                branch_name=branch_name,
                base_segment_id=str(previous_ref),
            )
            nodes_added, nodes_retracted, nodes_updated = (
                Publisher._chronos_segment_table_delta(
                    conn,
                    physical_table=node_table,
                    key_column="node_id",
                    compare_columns=("subtype", "attrs"),
                    branch_name=branch_name,
                    previous_segment_id=str(previous_ref),
                    since_base_segments=since_base_segments,
                )
            )
            edges_added, edges_retracted, edges_updated = (
                Publisher._chronos_segment_table_delta(
                    conn,
                    physical_table=edge_table,
                    key_column="edge_id",
                    compare_columns=("edge_type", "attrs"),
                    branch_name=branch_name,
                    previous_segment_id=str(previous_ref),
                    since_base_segments=since_base_segments,
                )
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "chronos current-segment delta count failed for branch=%s",
                branch_name,
            )
            raise
        return {
            "nodes_added": nodes_added,
            "nodes_retracted": nodes_retracted,
            "nodes_updated": nodes_updated,
            "edges_added": edges_added,
            "edges_retracted": edges_retracted,
            "edges_updated": edges_updated,
        }

    @staticmethod
    def _chronos_current_segment_has_graph_writes(
        conn: psycopg.Connection,
        *,
        branch_name: str,
    ) -> bool | None:
        """Return whether the branch wrote graph rows since its last checkpoint.

        A checkpoint seals the writer segment. Creating a branch also moves
        the source branch to a new segment but seals nothing: rows written
        before the fork are still unpublished, in the pre-fork segment. The
        probe therefore reads the current segment and, through each fork
        split above it, the segment the fork split, stopping at the first
        checkpoint split (issue #2865). Without a fork since the last
        checkpoint that is the current segment alone.
        """

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, physical_table
                      FROM _chronos_branch_tables
                     WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
                       AND backend = 'interval'
                    """
                )
                tables = {str(name): str(physical) for name, physical in cur}
            physical_tables = [
                table
                for table in (
                    tables.get("okg.graph_nodes"),
                    tables.get("okg.graph_edges"),
                )
                if table
            ]
            if not physical_tables:
                return None
            with conn.cursor() as cur:
                cur.execute(
                    _CHRONOS_SEGMENTS_SINCE_LAST_CHECKPOINT_SQL, (branch_name,),
                )
                segment_ids = [int(row[0]) for row in cur.fetchall()]
                if not segment_ids:
                    return None
                for physical_table in physical_tables:
                    cur.execute(
                        sql.SQL(
                            """
                            SELECT EXISTS (
                              SELECT 1
                                FROM {physical}
                               WHERE writer_segment_id = ANY(%s)
                               LIMIT 1
                            )
                            """
                        ).format(physical=sql.Identifier(physical_table)),
                        (segment_ids,),
                    )
                    row = cur.fetchone()
                    if bool(row and row[0]):
                        return True
            return False
        except Exception:
            logging.getLogger(__name__).exception(
                "chronos current-segment write probe failed for branch=%s",
                branch_name,
            )
            raise

    def _chronos_segment_signature_now(
        self, conn: psycopg.Connection,
    ) -> dict[str, Any] | None:
        """Fingerprint of the branch head's current segment, or a refusal."""
        from okg.substrate.publisher.dirty_hint_coverage import (
            segment_write_signature,
        )

        try:
            return segment_write_signature(conn, branch_name=self.branch_name)
        except Exception as exc:
            raise SubstrateError(
                "cannot read the branch head's write segment",
                data={
                    "branch": self.branch_name,
                    "error_type": type(exc).__name__,
                },
                fix_hint="restore database access and re-run the publish",
            ) from exc

    def _chronos_refuse_writes_since_counted(
        self,
        conn: psycopg.Connection,
        signature_counted: dict[str, Any] | None,
        *,
        stage: str = "checkpoint_cut",
        lock: bool = True,
    ) -> None:
        """Refuse if anyone wrote the branch head since the counted state.

        At the checkpoint cut (``lock=True``) this runs inside the
        publication transaction and takes the transaction-scoped branch
        write lock -- the fence every graph writer takes -- which is held
        through the commit, so no write can land between this check and the
        cut. Before the publisher's own post-count writes the caller already
        holds the session-scoped form of the same lock (``lock=False``).
        """
        if lock:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock("
                    "hashtext('okg.chronos_graph'), hashtext(%s))",
                    (self.branch_name,),
                )
        signature_now = self._chronos_segment_signature_now(conn)
        if lock and stage == "checkpoint_cut":
            # Issue #2993 round 6: the guard at the top of the cycle ran
            # before the counts, so a run could have cut its run base and
            # written between the two. Every writer takes the branch write
            # lock held here through the commit, so checking again now leaves
            # no window: no run's unfinished writes enter this checkpoint.
            from okg.substrate.ingest.run_base import (
                refuse_publish_while_undo_unfinished,
            )

            refuse_publish_while_undo_unfinished(
                conn,
                deployment=self.deployment_name,
                own_run_ids=self.own_source_run_ids or (),
            )
        if signature_now == signature_counted:
            return
        err = SubstrateError(
            "graph written during publish; checkpoint refused",
            data={
                "branch": self.branch_name,
                "stage": stage,
                "segment_signature_counted": signature_counted,
                "segment_signature_now": signature_now,
            },
            fix_hint=(
                "re-run the publish: the rows written during this one are "
                "still unpublished and the next cycle counts them"
            ),
        )
        err.code = "graph_written_during_publish"
        raise err

    def _chronos_fence_protected_writes(
        self,
        store: Any,
        conn: psycopg.Connection,
        signature_counted: dict[str, Any] | None,
    ) -> bool:
        """Lock the branch before an incremental cycle's protected writes.

        Takes the session-scoped branch write lock, then refuses
        (``before_effective_view``) if anyone wrote since the counts. On
        success the caller owns the lock and releases it after the commit;
        on a refusal it is released here. False when the backend has no
        branch lock (not PostgreSQL).
        """
        if not store._lock_branch_write_session(self.branch_name):
            return False
        try:
            self._chronos_refuse_writes_since_counted(
                conn,
                signature_counted,
                stage="before_effective_view",
                lock=False,
            )
        except BaseException:
            store._unlock_branch_write_session(self.branch_name)
            raise
        return True

    def _chronos_dirty_scope_hint_decision(
        self, conn: psycopg.Connection, *, previous: Any | None = None,
    ) -> dict[str, Any]:
        """Decide whether the caller's dirty-scope hint may replace the scan.

        ``used`` is True only when a hint was given and every row written
        since ``previous`` has a key the hint lists: the branch head's current
        segment, or every segment since ``previous`` when only source runs'
        run-base checkpoints split the head since then (issue #2993). A fork
        or a blocked generation since ``previous`` refuses the hint
        (``segments_since_previous_exceed_hint``, issue #2865). Any database
        error raises a SubstrateError, failing the publish: trusting an
        unchecked hint is the silent undercount this decision exists to stop.
        """
        scope = self.chronos_dirty_scope_hint
        if scope is None and self.chronos_delta_counts_hint is None:
            return {"used": False, "reason": "no_hint"}
        if scope is None:
            # A counts-only hint names no keys, so nothing can be checked.
            return {"used": False, "reason": "hint_lists_no_keys"}
        from okg.substrate.publisher.dirty_hint_coverage import (
            segment_keys_outside_hint,
        )

        hint_segments = self._chronos_hint_segments(conn, previous=previous)
        nodes_by_subtype, edges_by_type = scope[0] or {}, scope[1] or {}
        try:
            outside = segment_keys_outside_hint(
                conn,
                branch_name=self.branch_name,
                node_ids=(n for ids in nodes_by_subtype.values() for n in ids),
                edge_ids=(e for ids in edges_by_type.values() for e in ids),
                segment_ids=(
                    hint_segments if isinstance(hint_segments, list) else None
                ),
            )
        except Exception as exc:
            raise SubstrateError(
                "cannot check the dirty-scope hint against the branch head",
                data={
                    "branch": self.branch_name,
                    "error_type": type(exc).__name__,
                },
                fix_hint=(
                    "restore database access and re-run the publish; it was "
                    "refused rather than trusting an unchecked hint"
                ),
            ) from exc
        if not outside["checked"]:
            return {"used": True, "reason": outside["reason"]}
        tables = [label for label in ("nodes", "edges") if outside[label]]
        if tables:
            return {
                "used": False,
                "reason": "segment_has_rows_outside_hint",
                "tables": tables,
                "segment_id": outside["segment_id"],
            }
        if hint_segments is False:
            return {"used": False, "reason": "segments_since_previous_exceed_hint"}
        decision: dict[str, Any] = {
            "used": True, "segment_id": outside["segment_id"],
        }
        if "segment_ids" in outside:
            decision["segment_ids"] = outside["segment_ids"]
        return decision

    @staticmethod
    def chronos_graph_branch_mutation_token(
        conn: psycopg.Connection,
        *,
        branch_name: str,
    ) -> str:
        """Hash every graph row written into the branch's mutable head.

        Chronos deliberately keeps ``current_segment_id`` stable across plain
        writes, so the segment id alone is not a branch-head identity.  This
        token binds a guarded staged-source redrive to the exact node/edge
        contents observed by its admission probe.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT current_segment_id
                  FROM _chronos_branch_interval_branches
                 WHERE branch_id = %s
                """,
                (branch_name,),
            )
            segment_row = cur.fetchone()
            if segment_row is None:
                raise SubstrateError(
                    "cannot identify Chronos branch mutation state",
                    data={"branch": branch_name, "reason": "branch_missing"},
                )
            current_segment_id = int(segment_row[0])
            cur.execute(
                """
                SELECT table_name, physical_table
                  FROM _chronos_branch_tables
                 WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
                   AND backend = 'interval'
                 ORDER BY table_name
                """
            )
            tables = [(str(name), str(physical)) for name, physical in cur]
        if [name for name, _physical in tables] != [
            "okg.graph_edges",
            "okg.graph_nodes",
        ]:
            raise SubstrateError(
                "cannot identify Chronos branch mutation state",
                data={
                    "branch": branch_name,
                    "reason": "graph_interval_tables_missing",
                    "tables": [name for name, _physical in tables],
                },
            )

        digest = hashlib.sha256()
        digest.update(f"segment:{current_segment_id}\n".encode())
        for table_name, physical_table in tables:
            digest.update(f"table:{table_name}\n".encode())
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT to_jsonb(row_data)::text
                          FROM {physical} AS row_data
                         WHERE writer_segment_id = %s
                         ORDER BY to_jsonb(row_data)::text
                        """
                    ).format(physical=sql.Identifier(physical_table)),
                    (current_segment_id,),
                )
                while rows := cur.fetchmany(1000):
                    for (row_text,) in rows:
                        encoded = str(row_text).encode("utf-8")
                        digest.update(len(encoded).to_bytes(8, "big"))
                        digest.update(encoded)
            digest.update(b"\n")
        return f"sha256:{digest.hexdigest()}"

    @staticmethod
    def _chronos_current_segment_delta_counts_for_keys(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        previous: Any,
        node_ids: list[str] | tuple[str, ...],
        edge_ids: list[int] | tuple[int, ...],
    ) -> dict[str, int] | None:
        previous_ref = getattr(previous, "ref", None)
        if previous_ref is None:
            return None
        if not node_ids and not edge_ids:
            return {
                "nodes_added": 0,
                "nodes_retracted": 0,
                "nodes_updated": 0,
                "edges_added": 0,
                "edges_retracted": 0,
                "edges_updated": 0,
            }
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, physical_table
                      FROM _chronos_branch_tables
                     WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
                       AND backend = 'interval'
                    """
                )
                tables = {str(name): str(physical) for name, physical in cur}
            node_table = tables.get("okg.graph_nodes")
            edge_table = tables.get("okg.graph_edges")
            if not node_table or not edge_table:
                return None
            since_base_segments = Publisher._chronos_segments_written_since(
                conn,
                branch_name=branch_name,
                base_segment_id=str(previous_ref),
            )
            nodes_added, nodes_retracted, nodes_updated = (
                Publisher._chronos_segment_table_delta(
                    conn,
                    physical_table=node_table,
                    key_column="node_id",
                    compare_columns=("subtype", "attrs"),
                    branch_name=branch_name,
                    previous_segment_id=str(previous_ref),
                    key_values=tuple(dict.fromkeys(node_ids)),
                    since_base_segments=since_base_segments,
                )
            )
            edges_added, edges_retracted, edges_updated = (
                Publisher._chronos_segment_table_delta(
                    conn,
                    physical_table=edge_table,
                    key_column="edge_id",
                    compare_columns=("edge_type", "attrs"),
                    branch_name=branch_name,
                    previous_segment_id=str(previous_ref),
                    key_values=tuple(dict.fromkeys(edge_ids)),
                    since_base_segments=since_base_segments,
                )
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "chronos keyed current-segment delta count failed for branch=%s",
                branch_name,
            )
            raise
        return {
            "nodes_added": nodes_added,
            "nodes_retracted": nodes_retracted,
            "nodes_updated": nodes_updated,
            "edges_added": edges_added,
            "edges_retracted": edges_retracted,
            "edges_updated": edges_updated,
        }

    @staticmethod
    def _chronos_fact_delta_keys(
        facts: list[Any] | tuple[Any, ...],
    ) -> tuple[list[str], list[int]]:
        from okg.substrate.graph_store import _edge_id
        from okg.substrate.library.sources.base import EdgeFact, NodeFact

        node_ids: list[str] = []
        edge_ids: list[int] = []
        for fact in facts:
            if isinstance(fact, NodeFact):
                node_ids.append(str(fact.node_id))
            elif isinstance(fact, EdgeFact):
                edge_ids.append(int(_edge_id(fact.src, fact.edge_type, fact.dst)))
        return (
            list(dict.fromkeys(node_ids)),
            list(dict.fromkeys(edge_ids)),
        )

    def _chronos_scope_with_view_keys(
        self,
        conn: psycopg.Connection,
        *,
        scope: Any,
        fenced: bool,
        previous: Any | None,
        view_facts: list[Any] | tuple[Any, ...],
        view_delta_keys: tuple[list[str], list[int]],
        timings_ms: dict[str, Any],
    ) -> _ChronosDirtyScope | None:
        """The dirty scope after the view writes, without a full rescan.

        Issue #2941. ``scope`` was read before the view writes; under the
        branch write fence nothing else wrote since, so after them the
        scope differs only at the view facts' own keys, which a keyed read
        returns exactly. None (the caller rescans) unless the writes were
        fenced, ``scope`` is a scan result (the caller passes None for the
        ingest hint, which may list keys a scan would not), and no view fact
        retracts a node
        (that also deletes the node's incident edges, outside the keys).

        ``OKG_DIRTY_SCOPE_MERGE_VERIFY=1`` also rescans and compares; on a
        difference it logs, counts ``chronos_dirty_scope.view_keys_mismatch``
        and returns None so the rescan is used. ``=strict`` raises instead
        (for test runs).
        """
        from okg.substrate.library.sources.base import NodeFact

        if not fenced or previous is None or not isinstance(scope, tuple):
            return None
        if any(
            isinstance(fact, NodeFact)
            and str(getattr(fact, "op", "I") or "I").upper() == "R"
            for fact in view_facts
        ):
            timings_ms["chronos_dirty_scope.view_keys_node_retraction"] = 1.0
            return None
        node_ids, edge_ids = view_delta_keys
        with _publisher_timed_stage(
            timings_ms, "chronos_dirty_scope.view_keys_read",
        ):
            keyed = self._query_dirty_from_chronos_current_segment_by_type(
                conn,
                branch_name=self.branch_name,
                previous=previous,
                timing_sink=timings_ms,
                timing_prefix="chronos_dirty_scope.view_keys_read",
                only_node_ids=node_ids,
                only_edge_ids=edge_ids,
            )
        if keyed is None:
            return None
        merged = _merge_keyed_dirty_scope(
            scope, keyed, node_ids=node_ids, edge_ids=edge_ids,
        )
        if merged is None:
            return None
        timings_ms["chronos_dirty_scope.view_keys_merged"] = 1.0
        verify_mode = os.environ.get("OKG_DIRTY_SCOPE_MERGE_VERIFY")
        if verify_mode in ("1", "strict"):
            with _publisher_timed_stage(
                timings_ms, "chronos_dirty_scope.view_keys_verify",
            ):
                full = self._query_dirty_from_chronos_current_segment_by_type(
                    conn, branch_name=self.branch_name, previous=previous,
                )
            if full is None or _dirty_scope_normalized(
                full,
            ) != _dirty_scope_normalized(merged):
                logging.getLogger(__name__).error(
                    "dirty scope merged with the view keys differs from a "
                    "rescan; using the rescan (deployment=%s branch=%s)",
                    self.deployment_name, self.branch_name,
                )
                timings_ms["chronos_dirty_scope.view_keys_mismatch"] = 1.0
                if verify_mode == "strict":
                    raise RuntimeError(
                        "dirty scope merged with the view keys differs from "
                        "a rescan"
                    )
                return None
        return merged

    @staticmethod
    def _chronos_adjust_delta_counts_for_scoped_change(
        base: Mapping[str, Any],
        before: Mapping[str, Any],
        after: Mapping[str, Any],
    ) -> dict[str, int]:
        keys = (
            "nodes_added",
            "nodes_retracted",
            "nodes_updated",
            "edges_added",
            "edges_retracted",
            "edges_updated",
        )
        adjusted: dict[str, int] = {}
        for key in keys:
            adjusted[key] = (
                int(base.get(key, 0) or 0)
                - int(before.get(key, 0) or 0)
                + int(after.get(key, 0) or 0)
            )
        return adjusted

    @staticmethod
    def _chronos_segment_table_delta(
        conn: psycopg.Connection,
        *,
        physical_table: str,
        key_column: str,
        compare_columns: tuple[str, ...],
        branch_name: str,
        previous_segment_id: str,
        key_values: list[Any] | tuple[Any, ...] | None = None,
        since_base_segments: list[int] | None = None,
        changed_requires_non_null: tuple[str, ...] = (),
    ) -> tuple[int, int, int]:
        """Classify the rows written since ``previous_segment_id``.

        ``changed_requires_non_null`` narrows "updated" to rows whose new value
        of each named column is not NULL (the embedding-writes probe counts a
        written vector, never a cleared one). Empty by default, which leaves
        every other caller's classification unchanged.

        By default the dirty set is the rows confined to the branch's current
        segment. ``since_base_segments`` (more than one segment, from
        ``_chronos_segments_written_since``) widens it to the rows those
        segments wrote that the branch still sees, for a branch a fork moved
        off an unpublished segment (issue #2865). The statement is
        ``_chronos_segment_table_delta_sql``.
        """
        if key_values is not None and not key_values:
            return 0, 0, 0
        statement, params = _chronos_segment_table_delta_sql(
            physical_table=physical_table,
            key_column=key_column,
            compare_columns=compare_columns,
            branch_name=branch_name,
            previous_segment_id=previous_segment_id,
            key_values=key_values,
            since_base_segments=since_base_segments,
            changed_requires_non_null=changed_requires_non_null,
        )
        with conn.cursor() as cur:
            cur.execute(statement, params)
            row = cur.fetchone()
        if row is None:
            return 0, 0, 0
        return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)

    @staticmethod
    def _chronos_graph_effect_manifest(
        conn: psycopg.Connection,
        *,
        branch_name: str,
        previous: Any,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Capture bounded changed-object identities before checkpoint rotation."""
        previous_ref = getattr(previous, "ref", None)
        bounded_limit = max(1, min(int(limit), 500))
        if previous_ref is None:
            return {"schema": "okg.graph_effect.v1", "coverage": "aggregate_only",
                    "reason": "previous_generation_not_available", "items": [],
                    "returned": 0, "limit": bounded_limit}
        with conn.cursor() as cur:
            cur.execute(
                """SELECT table_name, physical_table FROM _chronos_branch_tables
                   WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
                     AND backend = 'interval'"""
            )
            tables = {str(name): str(physical) for name, physical in cur}
        if not tables.get("okg.graph_nodes") or not tables.get("okg.graph_edges"):
            return {"schema": "okg.graph_effect.v1", "coverage": "aggregate_only",
                    "reason": "interval_graph_tables_not_available", "items": [],
                    "returned": 0, "limit": bounded_limit}

        items_by_kind: dict[str, list[dict[str, Any]]] = {}
        specs = (
            ("node", tables["okg.graph_nodes"], "node_id", "subtype", False),
            ("edge", tables["okg.graph_edges"], "edge_id", "edge_type", True),
        )
        # The retained identities are diagnostic samples, not the generation's
        # authoritative change count. Bound the current-writer candidate set
        # before previous-state comparison and change-kind ranking. Chronos
        # provisions (writer_segment_id, primary_key) specifically for this
        # changed-row access path; using the segment's live interval here made a
        # small result LIMIT scan and sort the complete historical relation.
        candidate_limit = min(2_000, bounded_limit * 4)
        # Every segment written since the previous generation: more than the
        # current one when a fork, a blocked generation or a source run's
        # run-base checkpoint (issue #2993) split the head since then.
        since_previous = Publisher._chronos_segments_written_since(
            conn, branch_name=branch_name, base_segment_id=str(previous_ref),
        )
        several = len(since_previous) > 1
        for object_kind, physical, key_column, type_column, is_edge in specs:
            with conn.cursor() as cur:
                cur.execute(
                    Publisher._chronos_graph_effect_identity_query(
                        physical_table=physical,
                        key_column=key_column,
                        type_column=type_column,
                        is_edge=is_edge,
                        several_segments=several,
                    ),
                    (
                        (branch_name, list(since_previous))
                        if several else (branch_name,)
                    ) + (
                        candidate_limit,
                        str(previous_ref),
                        bounded_limit,
                    ),
                )
                kind_items: list[dict[str, Any]] = []
                for object_id, change_kind, object_type, source_id, target_id in cur:
                    kind_items.append({
                        "object_kind": object_kind, "object_id": str(object_id),
                        "change_kind": str(change_kind),
                        "object_type": str(object_type or "") or None,
                        "source_id": str(source_id) if source_id is not None else None,
                        "target_id": str(target_id) if target_id is not None else None,
                    })
                items_by_kind[object_kind] = kind_items
        # Retention is a diagnostic sample, not a prefix. Interleave node and
        # edge identities so a node-heavy write cannot consume the entire bound;
        # each per-kind query is itself round-robin ordered by change kind.
        items: list[dict[str, Any]] = []
        positions = {object_kind: 0 for object_kind, *_ in specs}
        while len(items) < bounded_limit:
            advanced = False
            for object_kind, *_ in specs:
                kind_items = items_by_kind.get(object_kind, [])
                position = positions[object_kind]
                if position >= len(kind_items):
                    continue
                items.append(kind_items[position])
                positions[object_kind] = position + 1
                advanced = True
                if len(items) >= bounded_limit:
                    break
            if not advanced:
                break
        truncated = len(items) >= bounded_limit
        return {"schema": "okg.graph_effect.v1",
                "coverage": "sampled" if truncated else "exact",
                "reason": "identity_limit_reached" if truncated else None,
                "items": items, "returned": len(items), "limit": bounded_limit,
                "candidate_limit": candidate_limit}

    @staticmethod
    def _chronos_graph_effect_identity_query(
        *,
        physical_table: str,
        key_column: str,
        type_column: str,
        is_edge: bool,
        several_segments: bool = False,
    ) -> sql.Composed:
        """Build the bounded identity-sampling query for one graph object kind.

        Parameters: ``(branch, candidate_limit, previous_segment, limit)``;
        with ``several_segments`` the segment list follows ``branch``, and a
        key's state now is the version visible at the head's branch point
        (issue #2993: rows written before a run-base split)."""
        endpoint_cols = (
            sql.SQL(", t.src, t.dst") if is_edge else sql.SQL("")
        )
        # Endpoints are coalesced once, in `joined`, so `changed` can read the
        # already-resolved columns by name after the d./p. aliases go out of scope.
        endpoint_cols_joined = (
            sql.SQL(
                ", COALESCE(d.src, p.src) AS src, "
                "COALESCE(d.dst, p.dst) AS dst"
            )
            if is_edge
            else sql.SQL("")
        )
        endpoint_result = (
            sql.SQL(", src, dst")
            if is_edge
            else sql.SQL(", NULL::text AS src, NULL::text AS dst")
        )
        head = (
            """WITH current_seg AS (
                 SELECT s.branch_point
                   FROM _chronos_branch_interval_branches b
                   JOIN _chronos_branch_interval_segments s
                     ON s.segment_id = b.current_segment_id
                  WHERE b.branch_id = %s
               ), dirty_candidates AS MATERIALIZED (
                 SELECT DISTINCT t.{key} AS graph_key
                   FROM {physical} t
                  WHERE t.writer_segment_id = ANY(%s)
                  ORDER BY t.{key}
                  LIMIT %s
               ), previous_point AS (
                 SELECT branch_point FROM _chronos_branch_interval_segments
                  WHERE segment_id = %s
               ), dirty AS (
                 SELECT DISTINCT ON (t.{key}) t.{key} AS graph_key,
                        COALESCE(t.deleted, FALSE) AS deleted,
                        t.{type_col} AS object_type, t.attrs {endpoint_cols}
                   FROM {physical} t
                   JOIN dirty_candidates c ON c.graph_key = t.{key}
                  CROSS JOIN current_seg h
                  WHERE t.live_lo <= h.branch_point
                    AND h.branch_point < t.live_hi
                  ORDER BY t.{key}, t.live_lo DESC
               ), previous_state AS ("""
            if several_segments else
            """WITH current_seg AS (
                 SELECT b.current_segment_id
                   FROM _chronos_branch_interval_branches b
                  WHERE b.branch_id = %s
               ), dirty_candidates AS MATERIALIZED (
                 SELECT t.{key} AS graph_key
                   FROM {physical} t
                  WHERE t.writer_segment_id = (
                        SELECT current_segment_id FROM current_seg
                  )
                  ORDER BY t.writer_segment_id, t.{key}
                  LIMIT %s
               ), previous_point AS (
                 SELECT branch_point FROM _chronos_branch_interval_segments
                  WHERE segment_id = %s
               ), dirty AS (
                 SELECT DISTINCT ON (t.{key}) t.{key} AS graph_key,
                        COALESCE(t.deleted, FALSE) AS deleted,
                        t.{type_col} AS object_type, t.attrs {endpoint_cols}
                   FROM {physical} t
                   JOIN dirty_candidates c ON c.graph_key = t.{key}
                  WHERE t.writer_segment_id = (
                        SELECT current_segment_id FROM current_seg
                  )
                  ORDER BY t.{key}, t.live_lo DESC
               ), previous_state AS ("""
        )
        return sql.SQL(
            head + """
                 -- The row visible at the previous branch point, which may be a
                 -- TOMBSTONE. Presence is not liveness; see the matching note in
                 -- _chronos_segment_table_delta.
                 SELECT DISTINCT ON (t.{key}) t.{key} AS graph_key,
                        COALESCE(t.deleted, FALSE) AS deleted,
                        t.{type_col} AS object_type, t.attrs {endpoint_cols}
                   FROM {physical} t JOIN dirty d ON d.graph_key = t.{key}
                  CROSS JOIN previous_point p
                  WHERE t.live_lo <= p.branch_point
                    AND p.branch_point < t.live_hi
                  ORDER BY t.{key}, t.live_lo DESC
               ), joined AS (
                 SELECT d.graph_key, d.deleted AS now_deleted,
                        (p.graph_key IS NOT NULL AND NOT p.deleted) AS was_live,
                        d.object_type, d.attrs,
                        p.object_type AS previous_object_type,
                        p.attrs AS previous_attrs {endpoint_cols_joined}
                   FROM dirty d LEFT JOIN previous_state p
                     ON p.graph_key = d.graph_key
               ), changed AS (
                 -- Same change PREDICATE as the count classifier, so the two
                 -- cannot disagree about how to label a given dirty row. They do
                 -- NOT share a dirty set: this query selects candidates by
                 -- writer_segment_id and truncates them to candidate_limit, while
                 -- _chronos_segment_table_delta selects by interval containment
                 -- and does not truncate. The manifest is a bounded diagnostic
                 -- sample; the counts are authoritative.
                 SELECT graph_key::text AS graph_key,
                        CASE WHEN NOT now_deleted AND NOT was_live THEN 'added'
                             WHEN now_deleted AND was_live THEN 'retracted'
                             ELSE 'updated' END AS change_kind,
                        COALESCE(object_type, previous_object_type) AS object_type
                        {endpoint_result}
                   FROM joined
                  WHERE (NOT now_deleted AND NOT was_live)
                     OR (now_deleted AND was_live)
                     OR (NOT now_deleted AND was_live AND
                         (object_type IS DISTINCT FROM previous_object_type OR
                          attrs IS DISTINCT FROM previous_attrs))
               ), ranked AS (
                 SELECT *, row_number() OVER (
                          PARTITION BY change_kind ORDER BY graph_key
                        ) AS change_rank
                   FROM changed
               )
               SELECT graph_key, change_kind, object_type, src, dst
                 FROM ranked
                ORDER BY change_rank, change_kind, graph_key
                LIMIT %s"""
        ).format(
            key=sql.Identifier(key_column),
            type_col=sql.Identifier(type_column),
            physical=sql.Identifier(physical_table),
            endpoint_cols=endpoint_cols,
            endpoint_cols_joined=endpoint_cols_joined,
            endpoint_result=endpoint_result,
        )

    @staticmethod
    def _chronos_segment_table_cardinality_delta(
        conn: psycopg.Connection,
        *,
        physical_table: str,
        key_column: str,
        branch_name: str,
        previous_segment_id: str,
    ) -> tuple[int, int]:
        return Publisher._chronos_segment_table_delta(
            conn,
            physical_table=physical_table,
            key_column=key_column,
            compare_columns=(),
            branch_name=branch_name,
            previous_segment_id=previous_segment_id,
        )[:2]

    def _chronos_extractor_input_signature(self) -> dict[str, Any] | None:
        return self._extractor_stage.input_signature()

    def _resolve_fork_base_generation(self, store: Any) -> Any | None:
        """Resolve a fork branch's BASE generation as the consistency-gate baseline.

        A fresh fork has no own published generation; without a baseline the gate
        full-validates the entire inherited base (re-tripping its latent
        violations). The fork's lineage anchor is recorded as branch metadata
        ``base_generation_id``. Delegates to the SHARED authority gate
        ``ChronosGraphStore.resolve_fork_base_checkpoint`` (also used by the merge
        catalog precondition, so the two cannot drift): the base MUST be a published
        ``okg_generation`` on ``default`` for THIS deployment. Returns the base
        checkpoint, or None -- in which case the caller MUST block loudly (never
        silently full-validate or publish unvalidated). worktree-fork-writes:
        fork-publish-soundness.
        """
        return store.resolve_fork_base_checkpoint(
            self.branch_name, expected_deployment=self.deployment_name
        )

    @staticmethod
    def _fork_scoped_delta_counts(
        scope: _ChronosDirtyScope | None,
    ) -> dict[str, int] | None:
        """Delta counts for a fork's FIRST publish, derived from its segment-scan
        dirty scope (the fork's own continuation-segment writes).

        Used ONLY to satisfy the consistency gate's incremental preconditions: a
        non-None delta_counts whose changed-node/edge totals match the dirty scope,
        so ``_incremental_fallback_reason`` does not force a full re-validate. The
        PUBLISHED node/edge counts come from the full-graph count (copy-on-write
        inherited base + fork delta), NOT from these. All continuation-segment
        writes are counted as additions: the gate uses only the totals for its
        scope-mismatch check, so the added/retracted split is immaterial here, and
        the gate still validates every dirty edge regardless of these counts.
        """
        if scope is None:
            return None
        nodes_by_subtype, edges_by_type, edge_table_available = scope[:3]
        if not edge_table_available:
            return None
        n_nodes = sum(len(ids) for ids in nodes_by_subtype.values())
        n_edges = sum(len(ids) for ids in edges_by_type.values())
        return {
            "nodes_added": n_nodes,
            "nodes_retracted": 0,
            "nodes_updated": 0,
            "edges_added": n_edges,
            "edges_retracted": 0,
            "edges_updated": 0,
        }

    def _should_run_chronos_extractor(
        self,
        conn: psycopg.Connection,
        *,
        previous: Any | None,
        input_signature: dict[str, Any] | None,
        branch_name: str,
    ) -> bool:
        return self._extractor_stage.should_run(
            previous=previous,
            input_signature=input_signature,
            dirty_probe=lambda: (
                self._chronos_current_segment_has_extractor_input_changes(
                    conn,
                    branch_name=branch_name,
                )
            ),
        )

    def _chronos_current_segment_has_extractor_input_changes(
        self,
        conn: psycopg.Connection,
        *,
        branch_name: str,
    ) -> bool:
        """Return whether the current Chronos segment touched extractor input.

        The Chronos-native extractor still scans the graph when it runs. Keep
        that correctness fallback, but only invoke it when the extractor's
        configuration changed or the current continuation segment contains a
        text-bearing source node that could feed it.
        """
        source_subtypes = self._extractor_stage.source_subtypes
        all_subtypes = not bool(source_subtypes)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT physical_table
                      FROM _chronos_branch_tables
                     WHERE table_name = 'okg.graph_nodes'
                       AND backend = 'interval'
                    """
                )
                row = cur.fetchone()
                if row is None:
                    return True
                node_table = str(row[0])
                # Every segment the branch wrote since its last checkpoint,
                # not only the current one: a source run's run-base checkpoint
                # (issue #2993) splits the segment without sealing the writes
                # before it, and those must still wake the extractor.
                cur.execute(
                    _CHRONOS_SEGMENTS_SINCE_LAST_CHECKPOINT_SQL, (branch_name,),
                )
                segment_ids = [int(seg_row[0]) for seg_row in cur.fetchall()]
                if not segment_ids:
                    return True
                cur.execute(
                    sql.SQL(
                        """
                        SELECT 1
                          FROM {node_table} n
                         WHERE n.writer_segment_id = ANY(%s)
                           AND COALESCE(n.deleted, FALSE) = FALSE
                           AND n.attrs ? 'text'
                           AND (%s OR n.subtype = ANY(%s))
                         LIMIT 1
                        """
                    ).format(node_table=sql.Identifier(node_table)),
                    (segment_ids, all_subtypes, list(source_subtypes)),
                )
                return cur.fetchone() is not None
        except Exception:
            logging.getLogger(__name__).exception(
                "chronos extractor-input change probe failed for branch=%s",
                branch_name,
            )
            raise

    @staticmethod
    def _chronos_delta_is_noop(delta_counts: Mapping[str, Any]) -> bool:
        return (
            int(delta_counts["nodes_added"]) == 0
            and int(delta_counts["nodes_retracted"]) == 0
            and int(delta_counts.get("nodes_updated", 0)) == 0
            and int(delta_counts["edges_added"]) == 0
            and int(delta_counts["edges_retracted"]) == 0
            and int(delta_counts.get("edges_updated", 0)) == 0
        )

    @staticmethod
    def _chronos_policy_pin_matches(
        previous_metadata: Mapping[str, Any],
        *,
        nomos_bundle_ids: list[str],
        nomos_fingerprints: list[str],
    ) -> bool:
        return (
            list(previous_metadata.get("nomos_policy_bundle_ids") or [])
            == list(nomos_bundle_ids or [])
            and list(previous_metadata.get("nomos_policy_fingerprints") or [])
            == list(nomos_fingerprints or [])
        )

    @staticmethod
    def _chronos_gate_cache_key_matches(
        previous_metadata: Mapping[str, Any],
        *,
        gate_cache_key: Mapping[str, Any],
    ) -> bool:
        invariant_summary = dict(
            previous_metadata.get("invariant_summary") or {}
        )
        return invariant_summary.get("gate_cache_key") == dict(gate_cache_key)

    class _StagedAdmissionProbeError(RuntimeError):
        """The staged-source-admission reuse probe could not run (#828)."""

    def _chronos_staged_source_admissions_present(
        self,
        conn: psycopg.Connection,
    ) -> bool:
        """Sweepable source admissions whose facts never published (#828).

        Mirrors the feedback sweep's candidate set
        (``_mark_generation_published``): producer_kind='source',
        status pending/accepted (accepted-or-direct/fast_track, so
        review/quarantine pendings that only ``okg admission decide`` can
        clear never pin the probe), attached fact refs, target branch.
        The only reachable coexistence with reuse eligibility is a
        feedback-crash leftover (``_apply_admission_feedback_safely``
        swallows feedback errors, so an envelope can survive a published
        checkpoint un-swept); while one exists the cycle must checkpoint
        for real so the sweep clears it. Fail-closed: a probe error
        refuses reuse and proceeds with a real checkpoint -- an aborted
        cycle would heal nothing.
        """
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('okg.admission_envelopes')")
                if cur.fetchone()[0] is None:
                    # No admission schema: no staged admissions can exist.
                    return False
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                          FROM okg.admission_envelopes e
                         WHERE e.deployment_name = %s
                           AND e.producer_kind = 'source'
                           AND e.status IN ('pending', 'accepted')
                           AND COALESCE(e.target_branch_name, 'default') = %s
                           AND (
                             e.status = 'accepted'
                             OR e.admission_mode IN ('direct', 'fast_track')
                           )
                           AND EXISTS (
                             SELECT 1
                               FROM okg.admission_record_refs r
                              WHERE r.admission_id = e.admission_id
                                AND r.ref_kind IN (
                                  'node_fact_range',
                                  'edge_fact_range',
                                  'payload'
                                )
                                AND (
                                  r.ref_kind != 'payload'
                                  OR r.payload->>'kind' =
                                     'chronos_graph_run'
                                )
                           )
                         LIMIT 1
                    )
                    """,
                    (self.deployment_name, self.branch_name),
                )
                return bool(cur.fetchone()[0])
        except Exception as exc:
            # Typed re-raise, no default: the caller decides the
            # fail-closed posture (refuse reuse) via a NARROW except.
            raise Publisher._StagedAdmissionProbeError(str(exc)) from exc

    def _chronos_noop_reuse_result(
        self,
        conn: psycopg.Connection,
        *,
        previous: Any,
        catalog_version_id: int,
        node_count: int,
        edge_count: int,
        timings_ms: dict[str, Any],
        cycle_start: float,
        lock_acquired_at: float | None,
        enricher_results: list[dict[str, Any]],
        enricher_runs: list[dict[str, Any]],
        extractor_edges_emitted: int,
        skipped_stages: list[str],
        reuse_stage: str,
        reuse_checks: Mapping[str, Any],
        graph_counts_provenance: Mapping[str, Any],
        noop_reuse_invalidation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        # Final eligibility check lives in the single constructor both
        # reuse returns flow through, so no reuse path can bypass it
        # (#828 defense-in-depth): re-serving the previous generation
        # while a sweepable staged source admission exists would strand
        # its facts and false-sweep its envelope. None = reuse refused;
        # callers record the invalidation and run the real cycle.
        # Probe failure refuses reuse (fail closed) WITHOUT rolling back
        # the shared cycle connection: if the failed SELECT aborted the
        # transaction, the real cycle's next statement fails loudly and
        # the outer handler does the rollback-and-re-raise.
        try:
            staged_present = self._chronos_staged_source_admissions_present(
                conn,
            )
        except Publisher._StagedAdmissionProbeError:
            logging.getLogger(__name__).warning(
                "staged-source-admission probe failed for deployment=%s; "
                "refusing noop-reuse and proceeding with a real checkpoint",
                self.deployment_name,
                exc_info=True,
            )
            staged_present = True
        if staged_present:
            return None
        previous_metadata = dict(getattr(previous, "metadata", None) or {})
        generation_id = str(getattr(previous, "checkpoint_id"))
        total_duration_ms = int((time.monotonic() - cycle_start) * 1000)
        timings_ms["total"] = round(float(total_duration_ms), 3)
        lock_hold_ms = (
            round(
                max(0.0, (time.perf_counter() - lock_acquired_at) * 1000.0),
                3,
            )
            if lock_acquired_at is not None
            else None
        )
        native_view_refresh = (
            {
                "status": "reused",
                "reason": "chronos_noop_same_catalog",
                "generation_id": generation_id,
            }
            if self.native_view_stage.materializers
            else {
                "status": "disabled",
                "reason": "no_materializers",
                "generation_id": generation_id,
            }
        )
        self._last_native_view_refresh = native_view_refresh
        native_view_refresh = self._native_view_refresh_report()
        attribute_effective_view_refresh = {
            "status": "reused",
            "reason": "chronos_noop_same_catalog",
            "generation_id": generation_id,
        }
        timing_details = self._chronos_timing_details(
            timings_ms=timings_ms,
            lock_acquire_ms=timings_ms.get("locked_connection"),
            lock_hold_ms=lock_hold_ms,
            coordination_mode=self._resolved_coordination_mode(),
            # Read the resolved attribute directly: the DBOS boundary inventory
            # classifies a function by the NAMES it calls, and a `_publish_`-
            # prefixed call would reclassify this read-only reuse path as an
            # authority transition.
            hard_timeout_ms=self.hard_statement_timeout_ms,
            hard_timeout_source=self.hard_statement_timeout_source,
            lock_hold_headroom_factor=self.lock_hold_headroom_factor,
            lock_hold_headroom_factor_source=(
                self.lock_hold_headroom_factor_source
            ),
            allow_raised_statement_timeout=(
                self.allow_raised_statement_timeout
            ),
            allow_raised_statement_timeout_source=(
                self.allow_raised_statement_timeout_source
            ),
            enricher_runs=enricher_runs,
            consistency_summaries=[],
            view_outcomes=[],
            native_view_refresh=native_view_refresh,
            lens_outcomes=[],
        )
        timing_details["attribute_effective_view_refresh"] = (
            attribute_effective_view_refresh
        )
        self._attach_prelock_enrich_report(timing_details, timings_ms)
        timing_details["skipped_stages"] = list(skipped_stages)
        timing_details["noop_reuse"] = {
            "reused": True,
            "stage": reuse_stage,
            "reason": "chronos_noop_same_catalog",
            "checks": dict(reuse_checks),
        }
        _emit_phase_event_with_timing(timings_ms, "cycle_complete", {
            "generation_id": generation_id,
            "status": "published",
            "noop": True,
            "reused_generation": True,
            "total_duration_ms": total_duration_ms,
        })
        timings_ms.pop(_PUBLISHER_TIMING_EVENTS_KEY, None)
        result = {
            "generation_id": generation_id,
            "compat_generation_id": previous_metadata.get(
                "target_generation_id",
            ),
            "deployment_name": self.deployment_name,
            "status": "published",
            "catalog_version_id": catalog_version_id,
            "published_at": (
                previous_metadata.get("published_at")
                or previous_metadata.get("created_at")
                or datetime.now(timezone.utc).isoformat()
            ),
            "correlation_id": generation_id,
            "phase_a_converged": True,
            "phase_b_blocked": False,
            # Derived from the generation being reused, never asserted. A
            # generation published before issue #1427 landed can carry a real
            # exclusion, and claiming [] for it would repeat the bug this
            # change fixes on the one path that does not re-run the gate.
            "excluded_sources": list(
                previous_metadata.get("excluded_sources") or []
            ),
            "consistency_gates": [],
            "nodes_added": 0,
            "nodes_retracted": 0,
            "nodes_updated": 0,
            "edges_added": 0,
            "edges_retracted": 0,
            "edges_updated": 0,
            "node_count": node_count,
            "edge_count": edge_count,
            "views": [],
            "lenses": [],
            "post_publish_consistency_checks": [],
            "per_source_status": {},
            "enrichers": enricher_results,
            "enricher_runs": enricher_runs,
            "extractor_edges_emitted": extractor_edges_emitted,
            # Present on the reuse path too: "the publish was green and the
            # row counts did not move" is EXACTLY the noop-reuse cycle, so
            # this is the outcome an operator inspects most often. Omitting
            # it here would KeyError the one consumer that needs it.
            "derived_rejections": self._derived_rejection_summary(),
            "noop": True,
            "reused_generation": True,
            "native_views_reused": bool(self.native_view_stage.materializers),
            "native_view_refresh": native_view_refresh,
            "attribute_effective_view_refresh": attribute_effective_view_refresh,
            "reuse_reason": "chronos_noop_same_catalog",
            "noop_reuse_stage": reuse_stage,
            "noop_reuse_checks": dict(reuse_checks),
            "publish_skipped_stages": list(skipped_stages),
            "blocked_reason": None,
            "timings_ms": timings_ms,
            "timing_details": timing_details,
            "publisher_configuration": self._publisher_configuration_receipt(),
            "first_publish_budget_warning": self._initial_generation_budget_warning(
                first_publish=False,
                node_count=node_count,
                edge_count=edge_count,
            ),
            # A reuse cycle still counted (or carried) the numbers it reports,
            # so it must say where they came from. Omitting the key here would
            # KeyError every consumer that reads it on the ordinary path, and
            # a green cycle whose counts did not move is exactly the cycle an
            # operator inspects.
            "graph_counts_provenance": dict(graph_counts_provenance),
            "total_duration_ms": total_duration_ms,
        }
        if noop_reuse_invalidation is not None:
            result["noop_reuse_invalidation"] = dict(noop_reuse_invalidation)
            result["noop_reuse_invalidation_reason"] = (
                noop_reuse_invalidation.get("reason")
            )
        return result

    def _record_chronos_publish_telemetry(
        self,
        *,
        trace_identity: Any,
        context_identity: Any | None,
        generation_id: str,
        status: str,
        started_at: datetime,
        ended_at: datetime,
        total_duration_ms: int,
        timings_ms: Mapping[str, Any],
        node_count: int | None,
        edge_count: int | None,
        blocked_reason: Mapping[str, Any] | str | None,
        noop: bool,
        base_perf_counter: float,
        timing_events: list[Mapping[str, Any]],
        graph_effect: Mapping[str, Any],
    ) -> None:
        """Mirror one Chronos-native publish into unified telemetry."""
        try:
            workflow_id = _current_dbos_workflow_id()
            recorder = None
            if deep_span_capture_enabled():
                recorder = SpanTimingRecorder(
                    trace_id=trace_identity.trace_id,
                    root_span_id=trace_identity.span_id,
                    root_span_name="publisher.cycle",
                    root_started_at=started_at,
                    base_perf_counter=base_perf_counter,
                    deployment_name=self.deployment_name,
                    branch_name=self.branch_name,
                    component="publisher",
                    generation_id=generation_id,
                    workflow_id=workflow_id,
                    root_kind="publisher",
                )
                for event in timing_events:
                    try:
                        recorder.record(
                            str(event["name"]),
                            float(event["started_perf"]),
                            ended_perf=float(event["ended_perf"]),
                        )
                    except (KeyError, TypeError, ValueError):
                        continue
            # Named connection: this was one of the two <unnamed> backends in
            # the usage_by_application inventory (unify-connection-budget) —
            # the publisher's own telemetry-span write was invisible to
            # attribution.
            from okg.substrate.db.connection import connect as _named_connect

            with _named_connect(
                self.dsn, autocommit=True,
                application_name="okg-publisher-telemetry",
            ) as conn:
                if (
                    context_identity is not None
                    and getattr(context_identity, "is_contextual", False)
                    and trace_identity.parent_span_id
                    == context_identity.span_id
                ):
                    context_span_name = (
                        "ingest.run"
                        if context_identity.from_local_context
                        and not context_identity.from_otel_context
                        else "telemetry.context"
                    )
                    record_context_span_if_missing(
                        conn,
                        context_identity,
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                        span_name=context_span_name,
                        root_kind=(
                            "ingest"
                            if context_span_name == "ingest.run"
                            else "context"
                        ),
                        started_at=started_at,
                        ended_at=ended_at,
                        attributes={
                            "component": "publisher",
                            "okg.component": "publisher",
                        },
                    )
                record_span(
                    conn,
                    TelemetrySpan(
                        trace_id=trace_identity.trace_id,
                        span_id=trace_identity.span_id,
                        parent_span_id=trace_identity.parent_span_id,
                        span_name="publisher.cycle",
                        span_kind="internal",
                        status_code="error" if status == "blocked" else "ok",
                        status_message=_publish_telemetry_status_message(
                            blocked_reason,
                        ),
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                        generation_id=generation_id,
                        workflow_id=workflow_id,
                        started_at=started_at,
                        ended_at=ended_at,
                        attributes={
                            "component": "publisher",
                            "okg.component": "publisher",
                            "okg.deployment.name": self.deployment_name,
                            "okg.branch.name": self.branch_name,
                            "okg.generation.id": generation_id,
                            "runtime": "chronos_native",
                            "noop": noop,
                        },
                        payload_summary={
                            "status": status,
                            "node_count": node_count,
                            "edge_count": edge_count,
                            "total_duration_ms": total_duration_ms,
                            "timings_ms": dict(timings_ms),
                        },
                        root_name="publisher.cycle",
                        root_kind="publisher",
                    ),
                )
                record_graph_effect_manifest(
                    conn,
                    deployment_name=self.deployment_name,
                    trace_id=trace_identity.trace_id,
                    span_id=trace_identity.span_id,
                    branch_name=self.branch_name,
                    producer_key="system:publisher",
                    manifest=graph_effect,
                )
                if recorder is not None:
                    for child_span in recorder.spans():
                        record_span(conn, child_span)
        except Exception as exc:  # noqa: BLE001 - telemetry must not break publish.
            logging.getLogger(__name__).warning(
                "Chronos-native publish telemetry write failed for %s: %s",
                generation_id,
                exc,
            )

    def _reapplication_trust_label(
        self,
        conn: psycopg.Connection,
        *,
        producer_id: str,
    ) -> str:
        """Trust for a re-applied batch, taken from what was DECLARED.

        The publisher is not producing these facts; it is moving rows that
        were already admitted under the emitting producer's own authority
        into the checkpoint. Stamping the bounded-batch default
        ``derived-deterministic-trusted`` on the profile row it mints under
        THAT producer's id inverts the direction of trust: it writes a
        rank-3 label for a producer that may have been granted far less,
        and an unfingerprinted lookup could then read it back (issue #1545).

        So the label is read from the producer's declared profile, and a
        producer with nothing declared -- a system batch such as
        ``_system:native_views``, or an emitter whose profile is missing --
        fails CLOSED to ``untrusted`` rather than up to trusted.
        """
        from okg.substrate.producer_authority import (
            ProducerPolicyViolation,
            load_profile_for_producer,
        )

        try:
            profile, _lookup = load_profile_for_producer(
                conn,
                deployment_name=self.deployment_name,
                producer_id=producer_id,
                allow_source_family_fallback=True,
                declared_only=True,
            )
        except ProducerPolicyViolation as exc:
            if exc.code != "producer_profile_missing":
                raise
            return "untrusted"
        return profile.trust_label

    def _apply_publisher_derived_fact_batch(
        self,
        conn: psycopg.Connection,
        store: Any,
        facts: Iterable[Any],
        *,
        catalog_version_id: int,
        gate_target_id: int,
        stage: str,
        previous_generation_ref: str | None,
        producer_id: str = "_publisher_derived",
        producer_kind: str = "workflow",
        source: str | None = None,
        run_id: uuid.UUID | None = None,
        prepared_chunks: Mapping[int, Any] | None = None,
    ) -> Any:
        """Apply one accepted derived producer run through FactBatch.

        ``prepared_chunks`` are the chunks' graph-apply halves computed
        before the lock (issue 2941 fix 1); each is reused only after the
        graph store proves its inputs unchanged under the branch write lock.
        """
        from okg.substrate.admission import (
            attach_fact_range_refs_for_run,
            stable_idempotency_key,
        )
        from okg.substrate.fact_application import apply_bounded_fact_batch

        resolved_run_id = run_id or uuid.uuid5(
            uuid.NAMESPACE_URL,
            "|".join(
                (
                    "okg.publisher-derived.v2",
                    self.deployment_name,
                    self.branch_name,
                    str(gate_target_id),
                    stage,
                    producer_id,
                )
            ),
        )
        authority_scope = {
            "stage": stage,
            "dependency_generation": previous_generation_ref,
            "input_lineage": {
                "source": "accepted_derived_fact_log",
                "previous_generation": previous_generation_ref,
            },
            "invalidation_scope": {
                "branch": self.branch_name,
                "gate_target_id": gate_target_id,
            },
            "catalog_version_id": catalog_version_id,
            "determinism": "deterministic",
            "incomplete_result": False,
            "watermark": {"gate_target_id": gate_target_id},
        }
        result = apply_bounded_fact_batch(
            conn,
            store,
            facts,
            deployment_name=self.deployment_name,
            producer_id=producer_id,
            producer_kind=producer_kind,
            trust_label=self._reapplication_trust_label(
                conn,
                producer_id=producer_id,
            ),
            intent=f"publisher_derived:{stage}",
            catalog_version_id=catalog_version_id,
            authority_scope=authority_scope,
            source=source or producer_id,
            target_branch=self.branch_name,
            run_id=resolved_run_id,
            idempotency_key=stable_idempotency_key(
                "publisher_derived",
                self.deployment_name,
                self.branch_name,
                gate_target_id,
                stage,
                producer_id,
                str(resolved_run_id),
            ),
            completion="delta",
            retractions_allowed=True,
            strict_endpoints=False,
            # These rows were already admitted under their emitting
            # producer; the publisher is only moving them into the
            # checkpoint. Keep the lineage they carry and run the
            # derived-provenance stamps (#1534).
            derived_reapplication=True,
            prepared_chunks=prepared_chunks,
        )
        # Without a record ref the generation sweep cannot see this
        # envelope, so it sits at `accepted` forever and no reader can tell
        # a re-application that reached the checkpoint from one that was
        # abandoned mid-cycle (issue #1545). The refs name exactly the fact
        # range -- or, for a synthetic batch with no fact-log rows, the graph
        # run -- that this application wrote, so the same watermark test that
        # governs every other envelope governs this one.
        #
        # Wording note: say "graph run", not the graph store's product name.
        # The boundary inventory in
        # scripts/check_formal_dbos_epoch_supersession.py derives a
        # function's categories from a TEXT scan of its whole source,
        # comments included, so naming that store here would add a
        # graph-write category and drift the registry. Whether this function
        # warrants that category is a formal-model question, not something a
        # comment should decide.
        attached_refs = attach_fact_range_refs_for_run(
            conn,
            admission_id=result.admission_id,
            source=source or producer_id,
            run_id=resolved_run_id,
        )
        if not attached_refs:
            # Attaching nothing is the ONE way this envelope can still be
            # stranded at `accepted`, and it would otherwise be invisible.
            # It is not an error: a re-application that applied no facts has
            # no range to cite. Saying so keeps "applied nothing" and
            # "recorded nothing" distinguishable in the log instead of
            # collapsing them into the silence this issue was about.
            logging.getLogger(__name__).info(
                "publisher_derived:%s for %s attached no record refs "
                "(run %s, %d chunks); its envelope stays 'accepted' "
                "until a later run cites a range.",
                stage,
                producer_id,
                resolved_run_id,
                result.summary.terminal_chunk_count,
            )
        return result

    def _cycle_chronos_native(
        self,
        conn: psycopg.Connection | None = None,
        *,
        native_view_refresh_final: bool = True,
        force_native_view_refresh: bool = False,
        force_enrichers: bool = False,
        publish_blocker: Mapping[str, Any] | None = None,
        expected_latest_checkpoint_id: str | None = None,
        expected_branch_mutation_token: str | None = None,
        require_expected_latest_checkpoint: bool = False,
        caller_verified_transaction: _CallerVerifiedPublishTransaction | None = None,
    ) -> dict[str, Any]:
        """Publish the current Chronos branch head as an OKG generation.

        This is the Chronos-native publisher path: it does not project
        fact logs into live tables and it does not insert/update
        okg.graph_generations. The generation is the Chronos checkpoint.
        """
        publish_blocker_payload: dict[str, Any] | None = None
        if publish_blocker is not None:
            publish_blocker_payload = dict(publish_blocker)
            reason = publish_blocker_payload.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError("publish_blocker requires a non-empty reason")

        # Per-cycle envelope-violation ledger (spec §9.3): reset here so the
        # outcome surfaces exactly this cycle's violations.
        self._last_envelope_violations = []
        # Same reason, same lifetime: without this reset a Publisher reused
        # across cycles reports the PREVIOUS cycle's derived verdicts as this
        # cycle's. Worse, a terminal envelope is skipped on re-run
        # (see _admit_derived_edge_outputs), so cycle 2 can emit nothing and
        # still report a nonzero rejection count that is entirely historical.
        self._derived_verdict_events = []
        cycle_start = time.monotonic()
        cycle_perf_started = time.perf_counter()
        cycle_started_at = datetime.now(timezone.utc)
        context_trace_identity = current_trace_identity()
        publish_trace_identity = child_span_identity()
        timings_ms: dict[str, Any] = {
            _PUBLISHER_TIMING_EVENTS_KEY: [],
        }
        attribute_effective_view_refresh: dict[str, Any] = {
            "status": "not_run",
            "final_view_refresh": native_view_refresh_final,
            "force_native_view_refresh": force_native_view_refresh,
        }
        lock_started = time.perf_counter()
        lock_acquired_at: float | None = None
        lock_hold_ms: float | None = None
        # Issue #1427: the receipt and the checkpoint metadata must report the
        # gate's ACTUAL exclusion verdict. This used to be a hardcoded `[]`,
        # which told every reader -- `okg.substrate.qa.metrics`, the health
        # console, the QA contract -- that no source had been excluded even on
        # a cycle whose source-local error invariant had just named one.
        #
        # This binding is never the value that gets reported. The gate stage
        # assigns it from `phase_b` in the same statement group that assigns
        # `blocked_reason`, and `blocked_reason` is read unguarded below (the
        # nomos blocker check, the checkpoint status, the metadata), so every
        # path that reaches the receipt has run the gate. The declaration is
        # here only so the name has a type and a definition at function scope.
        gate_excluded_sources: list[str] = []
        # Issue 2941 fix 2: hash the attributes of a predicted full cycle
        # before the lock. Only a final (or forced) refresh materializes the
        # view, and a caller-verified publish already owns a transaction.
        prelock_hash_report: dict[str, Any] = {}
        prelock_hash_cache: Any = None
        # Issue 2941 fix 1: compute the dirty scope, enricher output and the
        # derived-fact apply's compute half before the lock; each is used
        # under the lock only after a recheck (see prelock_enrich.py).
        prelock_enrich_report: dict[str, Any] = {}
        prelock_enrich_state: Any = None
        self._last_prelock_enrich_report = prelock_enrich_report

        def _before_publish_lock(
            locked_conn: psycopg.Connection,
        ) -> Callable[[], None] | None:
            nonlocal lock_started, prelock_hash_cache, prelock_enrich_state
            if native_view_refresh_final or force_native_view_refresh:
                prelock_hash_cache = self._prelock_attribute_hash(
                    locked_conn, timings_ms, prelock_hash_report,
                )

            def _drop() -> None:
                if prelock_hash_cache is None:
                    return
                from okg.substrate.attributes import (
                    drop_prelock_attribute_hash_cache,
                )

                try:
                    drop_prelock_attribute_hash_cache(
                        locked_conn, prelock_hash_cache,
                    )
                except Exception:  # noqa: BLE001 - TEMP tables end with the session
                    logging.getLogger(__name__).warning(
                        "could not drop the pre-lock attribute hash cache",
                        exc_info=True,
                    )

            try:
                prelock_enrich_state = self._prelock_enrich(
                    locked_conn,
                    timings_ms,
                    prelock_enrich_report,
                    force_enrichers=force_enrichers,
                )
            except BaseException:
                # The cycle never gets the cleanup callback when this hook
                # raises; drop the hash cache's TEMP tables here so the
                # pooled connection does not keep them.
                if (
                    locked_conn.info.transaction_status
                    == psycopg.pq.TransactionStatus.INERROR
                ):
                    try:
                        locked_conn.rollback()
                    except Exception:  # noqa: BLE001 - the raise below wins
                        logging.getLogger(__name__).warning(
                            "could not roll back after the pre-lock "
                            "enricher pass raised",
                            exc_info=True,
                        )
                _drop()
                raise
            # The lock wait is measured from here, not across the passes.
            lock_started = time.perf_counter()
            if prelock_hash_cache is None:
                return None
            return _drop

        prelock_hook = (
            _before_publish_lock
            if caller_verified_transaction is None
            else None
        )
        # P3 (specify-publish-commit-protocol): guard the whole cycle so a
        # mid-cycle exception flushes the phase-event batch accumulated so
        # far, instead of discarding the timeline of a failed cycle. On a
        # normal exit the batch is already flushed+popped at cycle_complete,
        # so the guard is a no-op.
        with self._locked_connection(conn, before_lock=prelock_hook) as conn, \
                ExitStack() as guarded_branch_writes, \
                _phase_event_failure_flush(timings_ms), \
                _attribute_integrity_transition_guard(
                    conn,
                    deployment_name=self.deployment_name,
                    branch=self.branch_name,
                ) as _attribute_transition_guard:
            timings_ms["locked_connection"] = round(
                max(0.0, (time.perf_counter() - lock_started) * 1000.0),
                3,
            )
            # Issue #2993: a failed source run whose undo did not finish left
            # the working branch half-undone; publishing ANY source would show
            # readers that state. Refuse every publish until it is resolved.
            from okg.substrate.ingest.run_base import (
                refuse_publish_while_undo_unfinished,
            )

            refuse_publish_while_undo_unfinished(
                conn,
                deployment=self.deployment_name,
                own_run_ids=self.own_source_run_ids or (),
            )
            if not publish_trace_identity.is_contextual:
                publish_trace_identity = (
                    trace_identity_from_latest_source_progress(
                        conn,
                        deployment_name=self.deployment_name,
                    )
                    or publish_trace_identity
                )
            lock_acquired_at = time.perf_counter()
            with _publisher_timed_stage(timings_ms, "catalog_version"):
                # Catalog selection is an authority check, not a global
                # max(applied) lookup. This runs before branch creation or any
                # checkpoint mutation so missing, stale, or conflicting writer
                # scope fails closed on the live Chronos publish path.
                from okg.substrate.catalog.ownership import (
                    resolve_catalog_for_publish,
                )

                cv = resolve_catalog_for_publish(
                    conn,
                    identity=self.catalog_identity,
                    deployment_name=self.deployment_name,
                )
            with _publisher_timed_stage(timings_ms, "producer_profiles"):
                # Commit-constant: declared derived profiles come from the
                # commit-stable catalog/manifest and the materialization is
                # idempotent, so run it once per commit rather than once per
                # per-layer publish cycle.
                from okg.substrate.commit_scope import db_scope_key, memoized

                def _materialize_producer_profiles_once() -> bool:
                    self._materialize_declared_derived_profiles(
                        conn,
                        catalog_version_id=int(cv),
                    )
                    return True

                memoized(
                    (
                        "producer_profiles_materialized",
                        db_scope_key(conn),
                        int(cv),
                    ),
                    _materialize_producer_profiles_once,
                )

            from okg.substrate.graph_store import ChronosGraphStore

            with _publisher_timed_stage(timings_ms, "store_setup"):
                store = ChronosGraphStore.from_connection(
                    conn,
                    autocommit=caller_verified_transaction is None,
                    interval_continuation_percent=(
                        self._chronos_interval_continuation_percent()
                    ),
                )
                if caller_verified_transaction is not None:
                    caller_verified_transaction.bind_store(store)
                store.ensure_branch(
                    self.branch_name,
                    from_branch=(
                        "default" if self.branch_name != "default" else "main"
                    ),
                    metadata={
                        "kind": "okg_branch",
                        "deployment_name": self.deployment_name,
                        "branch_name": self.branch_name,
                        "role": (
                            "default"
                            if self.branch_name == "default"
                            else "experiment"
                        ),
                        "status": "active",
                    },
                )
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT set_config('okg.chronos_branch', %s, false)",
                        (self.branch_name,),
                    )
                if require_expected_latest_checkpoint:
                    if expected_branch_mutation_token is None:
                        raise ValueError(
                            "guarded redrive requires a branch mutation token"
                        )
                if require_expected_latest_checkpoint or self.checkpoint_attestors:
                    if not store._lock_branch_write_session(self.branch_name):
                        raise SubstrateError(
                            "checkpoint publication requires PostgreSQL branch fencing",
                            data={"branch": self.branch_name},
                        )
                    guarded_branch_writes.callback(
                        store._unlock_branch_write_session,
                        self.branch_name,
                    )

            with _publisher_timed_stage(timings_ms, "branch_checkout"):
                branch = store.checkout(self.branch_name)
                previous = store.latest_generation(
                    deployment_name=self.deployment_name,
                    branch=self.branch_name,
                    status="published",
                )
                # `previous_counts` baselines the RUNNING-COUNTER stamp path
                # (`_chronos_counts_after_delta` -> previous_counts + delta_counts),
                # used only under OKG_IVM_INTERVAL_VIEW=1 (the interval-tracked branch
                # path below). delta_counts is the continuation-segment delta, which
                # advances at every checkpoint (published OR blocked), so the running
                # baseline MUST be the last CHECKPOINT's counts -- NOT the last
                # *published* gen's; otherwise a blocked gen's delta is dropped. `previous`
                # stays the last PUBLISHED gen (the ontology / count rollup -1 baseline).
                # In the all-published case last-checkpoint == last-published, so this is
                # a no-op. NOTE: this is NOT what fixes the DEFAULT (flag-off) path -- there
                # the stamp is the effective-attribute-view materialize cardinality, fixed
                # by the `_block_intervened` full-scan force below.
                previous_checkpoint = store.latest_generation(
                    deployment_name=self.deployment_name,
                    branch=self.branch_name,
                    status=None,
                )
                previous_counts = self._chronos_checkpoint_graph_counts(
                    previous_checkpoint
                )
                previous_checkpoint_matches_published = (
                    previous is not None
                    and previous_checkpoint is not None
                    and previous_checkpoint.checkpoint_id == previous.checkpoint_id
                )
                if require_expected_latest_checkpoint:
                    actual_checkpoint_id = (
                        str(previous_checkpoint.checkpoint_id)
                        if previous_checkpoint is not None
                        else None
                    )
                    if actual_checkpoint_id != expected_latest_checkpoint_id:
                        raise SubstrateError(
                            "publisher predecessor changed before locked redrive",
                            data={
                                "deployment_name": self.deployment_name,
                                "branch": self.branch_name,
                                "expected_latest_checkpoint_id": (
                                    expected_latest_checkpoint_id
                                ),
                                "actual_latest_checkpoint_id": (
                                    actual_checkpoint_id
                                ),
                                "actual_latest_checkpoint_status": (
                                    dict(previous_checkpoint.metadata or {}).get(
                                        "status"
                                    )
                                    if previous_checkpoint is not None
                                    else None
                                ),
                            },
                            fix_hint=(
                                "retry after the competing publisher finishes; "
                                "the guarded redrive did not checkpoint"
                            ),
                        )
                    actual_branch_mutation_token = (
                        self.chronos_graph_branch_mutation_token(
                            conn,
                            branch_name=self.branch_name,
                        )
                    )
                    if (
                        actual_branch_mutation_token
                        != expected_branch_mutation_token
                    ):
                        raise SubstrateError(
                            "publisher branch changed before locked redrive",
                            data={
                                "deployment_name": self.deployment_name,
                                "branch": self.branch_name,
                                "expected_latest_checkpoint_id": (
                                    expected_latest_checkpoint_id
                                ),
                                "actual_latest_checkpoint_id": (
                                    actual_checkpoint_id
                                ),
                                "reason": "branch_mutation_token_changed",
                            },
                            fix_hint=(
                                "retry after the competing source ingest "
                                "finishes; the guarded redrive did not "
                                "checkpoint"
                            ),
                        )
                    if publish_blocker_payload is not None:
                        # An incomplete-recovery freeze: prove its recorded
                        # admission set under the locks, before this cycle
                        # admits its own derived output.
                        from okg.substrate.admission import (
                            stamp_locked_recovery_admission_set,
                        )

                        publish_blocker_payload = (
                            stamp_locked_recovery_admission_set(
                                conn,
                                deployment_name=self.deployment_name,
                                branch_name=self.branch_name,
                                blocker=publish_blocker_payload,
                            )
                        )
            # Capture the receipt epoch immediately after acquiring the
            # publisher lock and resolving the branch, before Layer-1 overlay
            # refresh can issue an UPDATE against the protected snapshot.  A
            # later capture would misclassify this cycle's own first write as
            # pre-existing corruption and would never advance the chain.
            # ONE predicate, shared with the effective-attribute materializer,
            # for "does the interval representation own this branch's publish".
            # It now also honours the OKG_IVM_GENERATION_ORDINAL=0 kill switch,
            # which previously gated only
            # ChronosGraphStore._maybe_mint_generation_ordinal and so did
            # nothing on this path (issue #1514).
            from okg.substrate.graph_store_internal.generation_ordinal import (
                interval_representation_enabled as _interval_arm_enabled,
            )

            _interval_branch_enabled_at_start = _interval_arm_enabled(
                store.context.db, self.branch_name
            )
            if _interval_branch_enabled_at_start and previous is not None:
                from okg.substrate.attributes import (
                    require_effective_interval_backfill_precondition,
                    resolve_interval_precondition_anchor,
                )

                # The precondition equates a generation's checkpoint counts with
                # the intervals open at its ordinal. A generation that
                # DELIBERATELY deferred its interval sync (an intermediate
                # publish) folded nothing, so its counts legitimately run ahead
                # of the interval rows; checking it refused a healthy deployment
                # and told the operator to run a repair that has nothing to fix
                # (issue #1927). Anchor on the last generation that actually
                # synced instead. Only an EXPLICIT deferral marker moves the
                # anchor, so every other state stays fail-closed.
                _interval_anchor = resolve_interval_precondition_anchor(
                    previous,
                    load_checkpoint=store.get_generation,
                )
                _anchor_checkpoint = _interval_anchor["checkpoint"]
                _published_counts = self._chronos_checkpoint_graph_counts(
                    _anchor_checkpoint
                )
                require_effective_interval_backfill_precondition(
                    conn,
                    store.context.db,
                    deployment_name=self.deployment_name,
                    branch=self.branch_name,
                    generation_id=str(_anchor_checkpoint.checkpoint_id),
                    checkpoint_counts=(
                        {
                            "node": _published_counts[0],
                            "edge": _published_counts[1],
                        }
                        if _published_counts is not None
                        else None
                    ),
                    anchor_note=(
                        {
                            "predecessor_generation_id": str(
                                previous.checkpoint_id
                            ),
                            "deferred_generations_skipped": (
                                _interval_anchor["steps"]
                            ),
                            "stop_reason": _interval_anchor["reason"],
                        }
                        if _interval_anchor["steps"]
                        else None
                    ),
                )

            _attribute_integrity_transition: dict[str, Any] = {
                "eligible": False,
                "reason": "interval_read_model_not_materialized",
            }
            if (
                _interval_branch_enabled_at_start
                and (native_view_refresh_final or force_native_view_refresh)
            ):
                from okg.substrate.attribute_integrity import (
                    clear_attribute_integrity_transition,
                    prepare_attribute_integrity_transition,
                )

                _attribute_integrity_transition = (
                    prepare_attribute_integrity_transition(
                        conn,
                        deployment_name=self.deployment_name,
                        branch=self.branch_name,
                        namespace="default",
                    )
                )
                if _attribute_integrity_transition.get("eligible"):
                    _attribute_transition_guard.update(
                        _attribute_integrity_transition
                    )
            _emit_phase_event_with_timing(timings_ms, "reserve_complete", {
                "deployment_name": self.deployment_name,
                "branch_name": self.branch_name,
                "catalog_version_id": int(cv),
                "duration_ms": int((time.monotonic() - cycle_start) * 1000),
            })
            # Issue #1456: an invariant the gate cannot run used to surface only
            # at `evaluate_gates`, which is after the first-publish counts, the
            # extractor, the enrichers and the derived-fact stages -- 3,437
            # seconds of held publisher lock in the reported incident, for a
            # defect the planner reports in milliseconds. Plan them here, the
            # first thing after the catalog version is resolved and the last
            # thing before any stage that costs real time. It runs on EVERY
            # cycle, not just the first publish: a delta publish pays the
            # extractor and the enrichers for the same nothing.
            with _publisher_timed_stage(timings_ms, "invariant_preflight"):
                _invariant_block = preflight_invariants(
                    self.dsn,
                    catalog_version_id=int(cv),
                    # Only bound as a plan-time parameter value; the gate mints
                    # its own token for the run itself.
                    generation_id=self._chronos_compat_generation_id(),
                )
            if _invariant_block is not None:
                # Raising is the same failure shape this path already had -- an
                # unrunnable invariant escaped `evaluate_gates` the same way --
                # so the runtime's disposition of a failed cycle is unchanged.
                # What changes is the price: the cycle now fails in milliseconds
                # instead of after the counts, the extractor and the enrichers,
                # and it fails before those stages write anything to the branch.
                raise SubstrateError(
                    f"invariant {_invariant_block['invariant']!r} cannot be "
                    f"evaluated by the publish gate: its "
                    f"{_invariant_block['statement']} statement failed to plan "
                    f"({_invariant_block['error_class']}). Publishing would "
                    f"pay for the full cycle and then abort on it.",
                    data={
                        "deployment": self.deployment_name,
                        "branch": self.branch_name,
                        "catalog_version_id": int(cv),
                        **_invariant_block,
                    },
                    fix_hint=(
                        "correct the invariant body, or remove it from the "
                        "deployment's invariants/ bundle and re-apply the "
                        "catalog"
                    ),
                )
            # One full count per branch state, not one per consumer.
            #
            # A first publish reaches `_chronos_current_graph_counts` four to six
            # times, and each call is two sequential scans of the effective
            # graph. Several of those calls provably observe the SAME graph: the
            # cycle re-checks out the branch after every write batch, so a
            # sequence of consumers separated by no write shares one session
            # object. Keying the memo on that object's IDENTITY makes
            # invalidation structural rather than a call someone can forget to
            # make: a new checkout is a new object and misses the memo.
            #
            # That is NOT sufficient on its own. A `BranchSession` re-prepares
            # itself when the metadata epoch moves, so a session that was never
            # rebound still reads the live head -- an unrebound write is
            # invisible to identity keying rather than self-evidently stale.
            # Every point that can mutate the branch without rebinding
            # therefore clears the memo explicitly (see the consistency-gate
            # phase), and the AST guard in
            # `tests/substrate/publisher/test_first_publish_count_cost.py`
            # refuses the direct write calls inside the shared span.
            #
            # The memo is held by reference, not by `id()`: holding it prevents
            # the object being collected and its id reused by a later checkout,
            # which would be a silent false hit.
            _counts_memo: list[Any] = []

            def _carried_graph_counts() -> tuple[int, int] | None:
                """Counts carried from the last checkpoint, or None to scan.

                Issue #1456: a first publish that fails after its counts still
                cuts a BLOCKED checkpoint, and that checkpoint stamps the very
                node/edge counts its retry would otherwise spend an hour
                recomputing. `previous_counts` is already baselined from the
                last checkpoint of ANY status for exactly that reason, but every
                count site branched on `previous` -- the last PUBLISHED
                generation -- so the retry rescanned regardless.

                Carrying forward is sound only if nothing has touched the graph
                since that checkpoint. Cutting a checkpoint seals the writer
                segment and starts a new one, so "the current segment has no
                graph writes" is exactly that proof, and it costs a primary-key
                read plus one indexed EXISTS per graph table rather than a scan.

                Fail-safe, never fail-open: absent counts, an unanswerable
                probe, or any probe failure returns None and the caller scans.
                The worst outcome of a wrong answer here is a repeated scan.
                """
                if previous_counts is None:
                    return None
                try:
                    has_writes = self._chronos_current_segment_has_graph_writes(
                        conn, branch_name=self.branch_name,
                    )
                except (psycopg.Error, SubstrateError) as exc:
                    # The probe re-raises what it hit. Declining to carry is the
                    # safe answer -- the cycle counts for real -- but it is
                    # ANNOUNCED, not swallowed: a warning plus a receipt key, so
                    # a deployment that silently stopped carrying is visible
                    # rather than merely slow. Anything that is not a database
                    # or substrate error is a defect here and still propagates.
                    logging.getLogger(__name__).warning(
                        "count carry-forward probe failed for branch=%s "
                        "(%s: %s); taking a full count instead",
                        self.branch_name, type(exc).__name__, exc,
                    )
                    timings_ms["graph_counts_carry_probe_failed"] = 1.0
                    return None
                if has_writes is not False:
                    # True (segment written this cycle) or None (cannot tell).
                    return None
                return int(previous_counts[0]), int(previous_counts[1])

            # Every count in this cycle records HOW it was obtained. A receipt
            # that reports a number without its source cannot be audited, and
            # the three sources here (carried stamp, bounded interval count,
            # projection scan) have different failure modes.
            graph_counts_provenance: dict[str, Any] = {
                "source": None,
                # Every route used this cycle, in order. `source` is the route
                # that produced the counts most recently, which is the pair the
                # receipt reports; a cycle that counts more than once by
                # different routes would otherwise leave the earlier route
                # unrecorded.
                "sources": [],
                "exact": True,
                "progress": [],
                "physical_row_versions_estimate": None,
                "access_path": None,
                "access_path_unavailable": None,
                # Why the bounded interval route was not used, when it was not.
                # None means it was used, or was never attempted because the
                # counts were carried or derived.
                "bounded_route_declined": None,
                "physical_row_versions_estimate_unavailable": None,
                # Heap bytes per backing table, measured before a first-publish
                # count, and whether that count was announced on stderr.
                "backing_heap_bytes": None,
                "backing_heap_bytes_unavailable": None,
                "long_phase_announced": None,
            }

            def _note_count_route(source: str) -> None:
                graph_counts_provenance["source"] = source
                graph_counts_provenance["sources"].append(source)

            def _counts_after_delta(
                previous_for_delta: tuple[int, int] | None,
                delta_counts: Mapping[str, Any],
                *,
                branch: Any,
            ) -> tuple[int, int]:
                _GRAPH_COUNT.source = None
                counted = self._chronos_counts_after_delta(
                    previous_for_delta, delta_counts, branch=branch,
                )
                _note_count_route(
                    _GRAPH_COUNT.source or _GRAPH_COUNT_SOURCE_UNKNOWN
                )
                return counted

            def _counted_graph(branch_session: Any) -> tuple[int, int]:
                if _counts_memo and _counts_memo[0] is branch_session:
                    return _counts_memo[1], _counts_memo[2]
                carried = _carried_graph_counts()
                if carried is not None:
                    timings_ms["graph_counts_carried_from_checkpoint"] = 1.0
                    _note_count_route("carried_from_checkpoint")
                    _counts_memo[:] = [branch_session, carried[0], carried[1]]
                    return carried
                # Clear first: a label left over from an earlier count must
                # never be reported for this one. A counting double that writes
                # nothing therefore reads as "unknown", not as the last source.
                _GRAPH_COUNT.source = None
                _GRAPH_COUNT.declined = None
                counted = self._chronos_current_graph_counts(branch_session)
                _note_count_route(
                    _GRAPH_COUNT.source or _GRAPH_COUNT_SOURCE_UNKNOWN
                )
                if _GRAPH_COUNT.declined is not None:
                    graph_counts_provenance["bounded_route_declined"] = (
                        _GRAPH_COUNT.declined
                    )
                _counts_memo[:] = [branch_session, counted[0], counted[1]]
                return counted

            n_nodes: int | None = None
            n_edges: int | None = None
            first_publish_budget_warning: dict[str, Any] | None = None
            if previous is None:
                # Whether this count is worth announcing on stderr at all.
                # The announcements below exist for an hour-long count on a
                # large branch (#1456). On a small branch -- every freshly
                # created database, whose tables have also never been
                # vacuumed -- the count takes milliseconds and the lines,
                # including the VACUUM advice, are noise. The measured heap
                # size of each backing table decides; when it cannot be read
                # (including a lock that outlasts the read's lock timeout),
                # announce as before. The table mapping is resolved once here
                # and shared with the row-version estimate, so this adds no
                # resolver call and no repeated "route declined" warning.
                _backing_tables = self._chronos_interval_physical_tables(branch)
                _heap_bytes, _heap_bytes_reason = (
                    self._chronos_backing_heap_bytes(conn, _backing_tables)
                )
                graph_counts_provenance["backing_heap_bytes"] = _heap_bytes
                graph_counts_provenance["backing_heap_bytes_unavailable"] = (
                    _heap_bytes_reason
                )
                _announce_count = _first_publish_count_is_announced(_heap_bytes)
                graph_counts_provenance["long_phase_announced"] = (
                    _announce_count
                )

                def _count_phase_note(
                    message: str, *, deployment_name: str, branch_name: str,
                ) -> None:
                    if _announce_count:
                        _announce_long_publish_phase(
                            message,
                            deployment_name=deployment_name,
                            branch_name=branch_name,
                        )
                    else:
                        logging.getLogger(__name__).info(
                            "publisher %s branch=%s: %s",
                            deployment_name, branch_name, message,
                        )

                # The full count is the longest unbroken stage of a first
                # publish. Announce it under its own event key before it starts:
                # the batched phase-event cursor is drained by its flush and the
                # runtime upserts on the key, so flushing the batch early would
                # leave the durable record holding only what came after.
                _emit_phase_event("publisher_progress", {
                    "phase": "current_graph_counts_initial",
                    "deployment_name": self.deployment_name,
                    "branch_name": self.branch_name,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                })
                _count_phase_note(
                    "first publish has no predecessor count to carry forward; "
                    "counting the full graph now. This phase scans every node "
                    "and edge and can run for a long time on a large branch.",
                    deployment_name=self.deployment_name,
                    branch_name=self.branch_name,
                )
                # Order of magnitude BEFORE the count starts, clearly labelled
                # as an estimate of stored row VERSIONS -- not of the effective
                # graph, which only the exact count below reports.
                _row_version_estimate, _row_version_reason = (
                    self._chronos_physical_row_version_estimate(
                        branch, mapping=_backing_tables,
                    )
                )
                graph_counts_provenance["physical_row_versions_estimate"] = (
                    _row_version_estimate
                )
                graph_counts_provenance[
                    "physical_row_versions_estimate_unavailable"
                ] = _row_version_reason
                if _row_version_estimate is not None:
                    _count_phase_note(
                        "before counting: the branch backing tables hold about "
                        + ", ".join(
                            f"{count:,} {table.split('.')[-1]} row versions"
                            for table, count in sorted(
                                _row_version_estimate.items()
                            )
                        )
                        + " (planner ESTIMATE of stored versions, not the "
                        "effective graph)",
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                    )
                # Which plan is about to run, before it runs. This is the one
                # thing that actually decides whether the phase takes seconds
                # or an hour, and nothing in the query text reveals it.
                _count_access_path, _access_path_reason = (
                    self._chronos_graph_count_access_path(conn, branch)
                )
                graph_counts_provenance["access_path"] = _count_access_path
                graph_counts_provenance["access_path_unavailable"] = (
                    _access_path_reason
                )
                if _count_access_path is None:
                    # An absent access path is the exact blindness #1456 was
                    # filed about, so it is announced rather than left as a
                    # null in a receipt nobody reads.
                    logging.getLogger(__name__).warning(
                        "first-publish count access path unavailable for "
                        "deployment=%s branch=%s: %s; the count still runs and "
                        "is still exact, but its cost cannot be predicted here",
                        self.deployment_name, self.branch_name,
                        _access_path_reason,
                    )
                    _announce_long_publish_phase(
                        "cannot report how this count will run "
                        f"({_access_path_reason}); it still runs and is still "
                        "exact",
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                    )
                else:
                    _heap_scanned = sorted(
                        table for table, path in _count_access_path.items()
                        if not path["index_only"]
                    )
                    for table, path in sorted(_count_access_path.items()):
                        _count_phase_note(
                            f"counting {table} by {path['scan']}"
                            + (
                                f" on {path['index']}" if path["index"] else ""
                            ),
                            deployment_name=self.deployment_name,
                            branch_name=self.branch_name,
                        )
                    # The advice names only heap-scanned tables that are
                    # themselves over the threshold (or every heap-scanned
                    # table when sizes are unknown): a heap scan of a small
                    # table is cheap, and on a database that was just created
                    # it is the normal plan because nothing has been vacuumed.
                    _heap_scanned_large = [
                        table for table in _heap_scanned
                        if _heap_bytes is None
                        or _heap_bytes.get(table, 0)
                        > _FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES
                    ]
                    _heap_scanned_small = [
                        table for table in _heap_scanned
                        if table not in _heap_scanned_large
                    ]
                    if _heap_scanned_small:
                        logging.getLogger(__name__).info(
                            "first-publish count reads the whole backing "
                            "table for %s (deployment=%s branch=%s); each is "
                            "at most %s bytes, so this is expected and cheap",
                            ", ".join(_heap_scanned_small),
                            self.deployment_name, self.branch_name,
                            _FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES,
                        )
                    if _heap_scanned_large:
                        logging.getLogger(__name__).warning(
                            "first-publish count cannot be served from an "
                            "index for %s (deployment=%s branch=%s); the whole "
                            "backing table is read to produce one number. "
                            "VACUUM ANALYZE the backing table so the "
                            "visibility map allows an index-only count.",
                            ", ".join(_heap_scanned_large),
                            self.deployment_name, self.branch_name,
                        )
                        _count_phase_note(
                            "this count reads every stored row of "
                            + ", ".join(_heap_scanned_large)
                            + " because no index-only plan is available; "
                            "VACUUM ANALYZE the backing tables to make the "
                            "next one an index-only count",
                            deployment_name=self.deployment_name,
                            branch_name=self.branch_name,
                        )
                # Per-table progress, following the history-compaction planner
                # (#1308/#1627): one durable record per table as it finishes,
                # so an hour-long phase is legible while it runs instead of
                # only in the terminal output at the end.
                _counts_started = time.monotonic()

                def _record_count_progress(
                    table: str, count: int, elapsed_seconds: float,
                ) -> None:
                    record = {
                        "phase": "current_graph_counts_initial",
                        "table": table,
                        "count": count,
                        "elapsed_seconds": round(elapsed_seconds, 3),
                        "since_start_seconds": round(
                            time.monotonic() - _counts_started, 3
                        ),
                    }
                    graph_counts_provenance["progress"].append(record)
                    timings_ms[
                        f"current_graph_counts_initial.{table}"
                    ] = round(elapsed_seconds * 1000, 3)
                    _emit_phase_event("publisher_progress", {
                        **record,
                        "deployment_name": self.deployment_name,
                        "branch_name": self.branch_name,
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    })
                    # A count that took long for any reason (a lock wait,
                    # slow or cold storage) is printed whatever the size:
                    # quiet is only for a count that was actually quick.
                    (
                        _announce_long_publish_phase
                        if elapsed_seconds > _FIRST_PUBLISH_COUNT_SLOW_SECONDS
                        else _count_phase_note
                    )(
                        f"counted {count:,} rows in {table} in "
                        f"{elapsed_seconds:.1f}s",
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                    )

                # Save and RESTORE, not clear: `_GRAPH_COUNT` is one
                # per-thread object shared by every Publisher on this thread,
                # and a cycle reached from inside this one would otherwise
                # switch off the outer cycle's progress on its way out.
                _outer_progress = _GRAPH_COUNT.progress
                _GRAPH_COUNT.progress = _record_count_progress
                try:
                    with _publisher_timed_stage(
                        timings_ms,
                        "current_graph_counts_initial",
                    ):
                        n_nodes, n_edges = _counted_graph(branch)
                finally:
                    _GRAPH_COUNT.progress = _outer_progress
                first_publish_budget_warning = (
                    self._initial_generation_budget_warning(
                        first_publish=True,
                        node_count=n_nodes,
                        edge_count=n_edges,
                    )
                )
                if first_publish_budget_warning["triggered"]:
                    logging.getLogger(__name__).warning(
                        "first publish reached its deployment warning budget "
                        "before cold publication stages: deployment=%s "
                        "nodes=%s/%s edges=%s/%s source=%s",
                        self.deployment_name,
                        n_nodes,
                        first_publish_budget_warning["thresholds"]["node_count"],
                        n_edges,
                        first_publish_budget_warning["thresholds"]["edge_count"],
                        first_publish_budget_warning["source"],
                    )
                    _emit_phase_event_with_timing(
                        timings_ms,
                        "first_publish_budget_warning",
                        first_publish_budget_warning,
                    )

            previous_metadata = (
                dict(getattr(previous, "metadata", None) or {})
                if previous is not None
                else {}
            )
            # Issue #3342: which sources' last ingest attempt failed, carried
            # from the previous published generation and updated by this run.
            from okg.substrate.ingest import published_completeness

            generation_ingest_completeness = published_completeness.merge_marker(
                previous_metadata.get(published_completeness.METADATA_KEY),
                getattr(self, "ingest_completeness", None),
            )
            # A changed marker must reach a new checkpoint: every no-op reuse
            # path below refuses to answer with the previous generation.
            ingest_completeness_changed = published_completeness.marker_changes(
                previous_metadata.get(published_completeness.METADATA_KEY),
                generation_ingest_completeness,
            )
            # Validate recovery authority before any no-op/early-return path.
            # A capability is one-predecessor scoped and reuse must fail loudly
            # even when the attempted successor would otherwise have no delta.
            _recovery_authorization_raw = os.environ.get(
                _COUNT_STAMP_RECOVERY_AUTHORIZATION_ENV
            )
            _authorized_count_stamp_recovery: dict[str, Any] | None = None
            _authorized_previous_pinned_counts: tuple[int, int] | None = None
            _authorized_previous_recorded_counts: tuple[int, int] | None = None
            if _recovery_authorization_raw:
                if previous is None:
                    raise SubstrateError(
                        "count-stamp recovery authorization requires a published predecessor"
                    )
                _authorized_previous_recorded_counts = (
                    self._chronos_checkpoint_graph_counts(previous)
                )
                if _authorized_previous_recorded_counts is None:
                    raise SubstrateError(
                        "recovery predecessor has no trustworthy count stamp"
                    )
                _authorization_checkout = store.checkout_generation(
                    previous.checkpoint_id,
                    branch=self.branch_name,
                )
                _authorized_previous_pinned_counts = (
                    self._chronos_current_graph_counts(_authorization_checkout)
                )
                _authorized_count_stamp_recovery = (
                    _validate_count_stamp_recovery_authorization(
                        _recovery_authorization_raw,
                        conn=conn,
                        deployment=self.deployment_name,
                        branch=self.branch_name,
                        predecessor_generation_id=str(previous.checkpoint_id),
                        recorded_counts=_authorized_previous_recorded_counts,
                        pinned_counts=_authorized_previous_pinned_counts,
                    )
                )
                if (
                    _authorized_previous_recorded_counts
                    == _authorized_previous_pinned_counts
                ):
                    raise SubstrateError(
                        "count-stamp recovery authorization is unnecessary for a truthful predecessor"
                    )
            previous_catalog_version_id = self._chronos_checkpoint_catalog_version(
                previous,
            )
            gate_target_id = self._chronos_compat_generation_id()
            # Reserve the generation's authoring timestamp before any native
            # view materializes. The checkpoint does not exist yet at this
            # point, so a view cannot discover its own publication time by
            # querying checkpoint metadata. Carry one timestamp through the
            # view stage and persist the exact same value in the eventual
            # checkpoint instead.
            generation_published_at = datetime.now(timezone.utc)
            # Fork-first publish (worktree-fork-writes: fork-publish-soundness): a
            # fresh fork has no own published generation, so `previous` is None and
            # the consistency gate would full-validate the ENTIRE inherited base
            # (re-tripping its latent violations). Resolve the fork's BASE
            # generation as the gate baseline so evaluate_incremental engages and
            # validates ONLY the fork's own delta. If the base cannot be resolved
            # under the read-through guard, block loudly -- never silently
            # full-validate or publish unvalidated. (The published node/edge counts
            # still come from the full-graph copy-on-write count below; only the
            # gate's baseline + delta_counts are redirected.)
            fork_gate_baseline: Any | None = None
            fork_gate_baseline_cv: int | None = None
            fork_block_reason: str | None = None
            if previous is None and self.branch_name != "default":
                fork_gate_baseline = self._resolve_fork_base_generation(store)
                if fork_gate_baseline is None:
                    fork_block_reason = "fork_base_generation_unresolved"
                else:
                    fork_gate_baseline_cv = (
                        self._chronos_checkpoint_catalog_version(fork_gate_baseline)
                    )
                    if fork_gate_baseline_cv is None:
                        fork_block_reason = "fork_base_generation_unresolved"
            active_nomos_pin: tuple[list[str], list[str]] | None = None
            policy_pin_reusable: bool | None = None
            current_gate_cache_key: dict[str, Any] | None = None
            gate_inputs_reusable: bool | None = None
            noop_reuse_invalidation: dict[str, Any] | None = None
            # A caller's changed-row hints (scope and counts) list only the
            # rows that caller wrote. They stand in for the scan only when
            # the branch head's current segment holds no other row: a write
            # made before the run (provision's views-registry projection) or
            # during it (a runtime admission, a record) would otherwise be
            # left out of the stamped counts (okg-workspace: 53,710 recorded
            # against 53,718 visible). Checked under the publish lock; a
            # check that cannot run fails the publish.
            #
            # Two fingerprints of the current segment (row count plus a hash
            # of every row version; see dirty_hint_coverage):
            # `segment_signature_at_scope_read` is taken just before the
            # cached changed-row scope was read (the hint check or a scan);
            # `segment_signature_counted` is the state the cycle's counts
            # describe. The latter advances ONLY when the counts are
            # recomputed from a read of the whole segment, or across the
            # publisher's own writes made under the branch write lock. The
            # checkpoint cut compares it again under that lock and refuses
            # if anyone else wrote in between: that row would be in the
            # checkpoint but in neither this cycle's counts nor the next
            # cycle's segment.
            segment_signature_at_scope_read = (
                self._chronos_segment_signature_now(conn)
            )
            segment_signature_counted = segment_signature_at_scope_read
            # The hints are also not used when the rows since the gate's base
            # span more than the current segment: on a fork's first publish
            # (rows the branch's ancestors wrote since the base), or when this
            # branch moved to a new segment since its last published
            # generation -- a fork was taken from it or a BLOCKED generation
            # cut a checkpoint (issue #2865). The full since-base scan runs.
            # Segments split only by source runs' run-base checkpoints (issue
            # #2993) are checked against the hint instead, inside the decision.
            dirty_scope_hint_decision = self._chronos_dirty_scope_hint_decision(
                conn, previous=previous,
            )
            if dirty_scope_hint_decision["used"]:
                if fork_gate_baseline is not None:
                    dirty_scope_hint_decision = {
                        "used": False, "reason": "fork_first_publish",
                    }
            caller_hints_usable = bool(dirty_scope_hint_decision["used"])
            delta_counts_cache: dict[str, int] | None = (
                dict(self.chronos_delta_counts_hint)
                if (
                    self.chronos_delta_counts_hint is not None
                    and caller_hints_usable
                )
                else None
            )
            dirty_scope_cache: _ChronosDirtyScope | None | object = (
                self.chronos_dirty_scope_hint
                if (
                    self.chronos_dirty_scope_hint is not None
                    and caller_hints_usable
                )
                else _UNSET
            )

            # Issue #2941: True while the cache holds a segment scan of the
            # head (taken under the lock or seeded from the pre-lock scan),
            # not the caller's hint. Only a scan's scope is merged with the
            # view keys after the view writes.
            dirty_scope_is_scan = False
            # Issue #2941: the delta counts of the rows the last segment scan
            # read, when that scan could classify them; None otherwise.
            # Cleared with the scope it came from.
            scope_scan_counts: dict[str, int] | None = None

            def invalidate_chronos_scope_cache() -> None:
                nonlocal delta_counts_cache, dirty_scope_cache
                nonlocal dirty_scope_is_scan, scope_scan_counts
                delta_counts_cache = None
                dirty_scope_cache = _UNSET
                dirty_scope_is_scan = False
                scope_scan_counts = None

            def chronos_generation_delta_counts() -> dict[str, int]:
                nonlocal delta_counts_cache, segment_signature_counted
                if previous is None:
                    raise RuntimeError(
                        "chronos delta counts require a previous generation"
                    )
                if delta_counts_cache is None:
                    # This read covers the whole segment, so the counts it
                    # yields describe the state fingerprinted just before it.
                    segment_signature_counted = (
                        self._chronos_segment_signature_now(conn)
                    )
                    with _publisher_timed_stage(
                        timings_ms,
                        "chronos_delta_counts.generation",
                    ):
                        delta_counts_cache = self._chronos_generation_delta_counts(
                            conn,
                            store=store,
                            branch_name=self.branch_name,
                            previous=previous,
                        )
                return dict(delta_counts_cache)

            def set_chronos_delta_counts_cache(
                counts: Mapping[str, Any],
            ) -> None:
                nonlocal delta_counts_cache
                delta_counts_cache = {
                    "nodes_added": int(counts.get("nodes_added", 0) or 0),
                    "nodes_retracted": int(
                        counts.get("nodes_retracted", 0) or 0,
                    ),
                    "nodes_updated": int(
                        counts.get("nodes_updated", 0) or 0,
                    ),
                    "edges_added": int(counts.get("edges_added", 0) or 0),
                    "edges_retracted": int(
                        counts.get("edges_retracted", 0) or 0,
                    ),
                    "edges_updated": int(
                        counts.get("edges_updated", 0) or 0,
                    ),
                }

            def chronos_dirty_scope() -> _ChronosDirtyScope | None:
                nonlocal dirty_scope_cache, segment_signature_at_scope_read
                nonlocal dirty_scope_is_scan, scope_scan_counts
                if dirty_scope_cache is _UNSET:
                    # Every branch below fills the cache from a scan: the
                    # pre-lock seed or the segment query.
                    dirty_scope_is_scan = True
                    scope_scan_counts = None
                    # A re-read does NOT advance the counted fingerprint: the
                    # counts may still come from an earlier read.
                    segment_signature_at_scope_read = (
                        self._chronos_segment_signature_now(conn)
                    )
                    # Issue 2941 fix 1: the pre-lock scan read the same
                    # segment for the same predecessor when the signature
                    # still matches; that is exactly the condition under
                    # which this cache holds a scan, so reuse it.
                    if (
                        prelock_enrich_state is not None
                        and previous is not None
                    ):
                        seeded, seeded_scope = (
                            prelock_enrich_state.seed_dirty_scope(
                                segment_signature=(
                                    segment_signature_at_scope_read
                                ),
                                previous_checkpoint_id=str(
                                    previous.checkpoint_id
                                ),
                            )
                        )
                        if seeded:
                            dirty_scope_cache = seeded_scope
                            timings_ms["chronos_dirty_scope.prelock_seeded"] = (
                                timings_ms.get(
                                    "chronos_dirty_scope.prelock_seeded", 0.0,
                                )
                                + 1.0
                            )
                            return dirty_scope_cache
                    # Fork-first publish: DIFF the fork's dirty scope against its
                    # resolved base, not previous=None. Otherwise the scan marks
                    # every dirty node __new__ and a copy-on-write RE-TYPED or
                    # DELETED inherited node is never flagged __subtype__/__deleted__
                    # -- so the inherited edge it invalidates is never pulled into
                    # the scoped gate and a real narrowing/dangling violation slips
                    # through. The diff branch stays O(fork delta) and is the SAME
                    # baseline the extractor decision uses. worktree-fork-writes:
                    # fork-publish-soundness.
                    #
                    # The fork delta is every row written since the base along the
                    # branch ancestry, not only the current segment: a branch made
                    # from another branch (the merge-back gate branch is made from
                    # staging) inherits rows its parent wrote after the base, and a
                    # current-segment seed never sees them (issue #2865).
                    scan_previous = (
                        previous if previous is not None else fork_gate_baseline
                    )
                    fork_first_scan = (
                        previous is None and fork_gate_baseline is not None
                    )
                    scan_counts: dict[str, int] = {}
                    with _publisher_timed_stage(
                        timings_ms,
                        "chronos_dirty_scope.query",
                    ):
                        dirty_scope_cache = (
                            self._query_dirty_from_chronos_current_segment_by_type(
                                conn,
                                branch_name=self.branch_name,
                                previous=scan_previous,
                                timing_sink=timings_ms,
                                timing_prefix="chronos_dirty_scope.query",
                                since_fork_base=fork_first_scan,
                                counts_sink=(
                                    scan_counts if not fork_first_scan else None
                                ),
                            )
                        )
                    if scan_counts:
                        scope_scan_counts = dict(scan_counts)
                if dirty_scope_cache is _UNSET:
                    return None
                return dirty_scope_cache

            def chronos_scoped_delta_counts_from_dirty_scope() -> dict[str, int] | None:
                nonlocal segment_signature_counted
                if delta_counts_cache is not None:
                    timings_ms["chronos_delta_counts.hint_used"] = (
                        timings_ms.get("chronos_delta_counts.hint_used", 0.0)
                        + 1.0
                    )
                    return dict(delta_counts_cache)
                if previous is None:
                    return None
                scope = chronos_dirty_scope()
                if scope is None:
                    return None
                nodes_by_subtype, edges_by_type, edge_table_available = scope[:3]
                if not edge_table_available:
                    return None
                node_ids = [
                    node_id
                    for nodes in nodes_by_subtype.values()
                    for node_id in nodes
                ]
                edge_ids = [
                    edge_id
                    for edges in edges_by_type.values()
                    for edge_id in edges
                ]

                def keyed_counts() -> dict[str, int] | None:
                    with _publisher_timed_stage(
                        timings_ms,
                        "chronos_delta_counts.scoped",
                    ):
                        return self._chronos_current_segment_delta_counts_for_keys(
                            conn,
                            branch_name=self.branch_name,
                            previous=previous,
                            node_ids=list(dict.fromkeys(node_ids)),
                            edge_ids=list(dict.fromkeys(edge_ids)),
                        )

                # Issue #2941: the scan that read this scope already
                # classified its rows; reading them again by key cost 73 s
                # on okg-workspace. OKG_SCOPE_COUNTS_VERIFY=1 also counts by
                # key and uses that count on a difference; =strict raises
                # instead (for test runs).
                counts: dict[str, int] | None
                if scope_scan_counts is not None:
                    counts = dict(scope_scan_counts)
                    timings_ms["chronos_delta_counts.from_scope_scan"] = 1.0
                    verify_mode = os.environ.get("OKG_SCOPE_COUNTS_VERIFY")
                    if verify_mode in ("1", "strict"):
                        verified = keyed_counts()
                        if verified is not None and {
                            key: int(verified.get(key, 0) or 0)
                            for key in _CHRONOS_DELTA_COUNT_KEYS
                        } != counts:
                            logging.getLogger(__name__).error(
                                "delta counts from the scope scan %s differ "
                                "from the keyed count %s; using the keyed "
                                "count (deployment=%s branch=%s)",
                                counts, verified,
                                self.deployment_name, self.branch_name,
                            )
                            timings_ms["chronos_delta_counts.from_scope_scan_mismatch"] = 1.0
                            if verify_mode == "strict":
                                raise RuntimeError(
                                    "delta counts from the scope scan differ "
                                    "from the keyed count"
                                )
                            counts = verified
                else:
                    counts = keyed_counts()
                if counts is None:
                    return None
                set_chronos_delta_counts_cache(counts)
                # Counted over the whole scope just read, so the counts
                # describe the state fingerprinted before that read.
                segment_signature_counted = segment_signature_at_scope_read
                return counts

            embedding_writes_cache: int | None = None

            def embedding_writes_pending() -> bool:
                """Embedding-only writes since ``previous`` (refuses reuse)."""
                nonlocal embedding_writes_cache
                if previous is None:
                    return False
                if embedding_writes_cache is None:
                    with _publisher_timed_stage(
                        timings_ms,
                        "embedding_writes_probe",
                    ):
                        embedding_writes_cache = (
                            self._chronos_embedding_writes_since(
                                conn,
                                branch_name=self.branch_name,
                                previous=previous,
                            )
                        )
                        # Vector sets: vectors or document links written
                        # since the previous generation's watermark.
                        from okg.substrate.embeddings.store import (
                            seq_advanced_since,
                        )

                        vector_sets_advanced = seq_advanced_since(
                            conn, str(getattr(previous, "checkpoint_id", "")) or None,
                        )
                    timings_ms["embedding_writes_probe.nodes"] = float(
                        embedding_writes_cache
                    )
                    timings_ms["embedding_writes_probe.vector_sets"] = float(
                        vector_sets_advanced
                    )
                    embedding_writes_cache += vector_sets_advanced
                return embedding_writes_cache > 0

            def fork_first_change_receipt(
                full_nodes: int, full_edges: int,
            ) -> tuple[int, int, int, int, int, int]:
                """Change-receipt counts for a fork's FIRST publish.

                A fork inherited its base via copy-on-write, so only its own segment
                writes (the post-enrichment dirty scope) are its contribution.
                Report THAT delta as the change receipt when a base baseline is
                resolved; otherwise fall back to the full count (a default-style
                first publish). node_count/edge_count stay the full copy-on-write
                size regardless -- only the added/retracted receipt is scoped.
                Returns (nodes_added, nodes_retracted, nodes_updated,
                edges_added, edges_retracted, edges_updated).
                """
                if fork_gate_baseline is not None:
                    fork_delta = self._fork_scoped_delta_counts(chronos_dirty_scope())
                    if fork_delta is not None:
                        return (
                            int(fork_delta["nodes_added"]), 0, 0,
                            int(fork_delta["edges_added"]), 0, 0,
                        )
                return full_nodes, 0, 0, full_edges, 0, 0

            def record_noop_reuse_invalidation(
                reason: str,
                *,
                owner: str,
                replace: bool = False,
            ) -> None:
                nonlocal noop_reuse_invalidation
                if noop_reuse_invalidation is None or replace:
                    noop_reuse_invalidation = {
                        "reason": reason,
                        "owner": owner,
                    }

            def reuse_policy_pin_matches() -> bool:
                nonlocal active_nomos_pin, policy_pin_reusable
                if policy_pin_reusable is not None:
                    return policy_pin_reusable
                if active_nomos_pin is None:
                    with _publisher_timed_stage(
                        timings_ms,
                        "nomos_policy_pin_reuse_check",
                    ):
                        active_nomos_pin = self._active_nomos_policy_pin(
                            conn,
                            self.deployment_name,
                        )
                nomos_bundle_ids, nomos_fingerprints = active_nomos_pin
                policy_pin_reusable = self._chronos_policy_pin_matches(
                    previous_metadata,
                    nomos_bundle_ids=nomos_bundle_ids,
                    nomos_fingerprints=nomos_fingerprints,
                )
                return policy_pin_reusable

            def reuse_gate_inputs_match() -> bool:
                nonlocal current_gate_cache_key, gate_inputs_reusable
                if gate_inputs_reusable is not None:
                    return gate_inputs_reusable
                if current_gate_cache_key is None:
                    with _publisher_timed_stage(
                        timings_ms,
                        "gate_input_reuse_check",
                    ):
                        current_gate_cache_key = self._gate_cache_key(
                            ReservedGeneration(
                                generation_id=gate_target_id,
                                deployment_name=self.deployment_name,
                                catalog_version_id=int(cv),
                                correlation_id=(
                                    f"chronos-native-gate:"
                                    f"{self.deployment_name}:{gate_target_id}"
                                ),
                                _conn=conn,
                            ),
                            ignore_derived_artifact_facts=True,
                        )
                gate_inputs_reusable = self._chronos_gate_cache_key_matches(
                    previous_metadata,
                    gate_cache_key=current_gate_cache_key,
                )
                return gate_inputs_reusable

            enricher_results: list[dict[str, Any]] = []
            enricher_runs: list[dict[str, Any]] = []
            with _publisher_timed_stage(timings_ms, "fact_watermarks"):
                pre_derived_node_fact_id, pre_derived_edge_fact_id = (
                    self._current_fact_watermarks(conn)
                )
            with _publisher_timed_stage(timings_ms, "lens_promotion_probe"):
                pending_lens_promotions = self._has_pending_lens_promotions(conn)
            with _publisher_timed_stage(timings_ms, "enricher_retry_probe"):
                # P6 closure: a held #679 enricher retry invalidates
                # noop-reuse so the retry actually runs on a zero-delta
                # cycle instead of starving forever.
                enricher_retry_pending = self._enricher_retry_pending_name(conn)
            delta_counts: dict[str, int] | None = None
            extractor_input_signature = self._chronos_extractor_input_signature()
            with _publisher_timed_stage(timings_ms, "link_retry_ready_probe"):
                ready_retry_node_ids = (
                    ready_link_retry_node_ids(
                        conn,
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                    )
                    if self._extractor_stage.tracks_extraction_cache
                    else []
                )
            with _publisher_timed_stage(timings_ms, "extractor_queue_probe"):
                extractor_queue_node_ids = (
                    queued_extractor_node_ids(
                        conn,
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                    )
                    if self._extractor_stage.tracks_extraction_cache
                    else []
                )
            link_retry_receipt: list[LinkRetryKey] = []
            extractor_queue_receipt: list[str] = []
            extractor_input_reusable = (
                extractor_input_signature is None
                or previous_metadata.get("extractor_input_signature")
                == extractor_input_signature
            )
            if self.checkpoint_attestors:
                # Attestations can change when semantic graph identity does not
                # (for example a source-occurrence receipt or policy pin changed).
                # The first protocol version therefore always evaluates the final
                # candidate and cuts a fresh checkpoint.
                record_noop_reuse_invalidation(
                    "checkpoint_attestors_configured",
                    owner="checkpoint_attestations",
                )
            elif force_enrichers:
                # P6 (specify-publish-commit-protocol): the sanctioned
                # operator force lever. Bypass the pre-enrichment noop-reuse
                # skip for exactly this invocation so a zero-delta,
                # unchanged-catalog deployment still runs enrichers, gates,
                # and cuts a checkpoint. Recorded as an auditable reuse
                # invalidation; one-shot-ness falls out of it being a call
                # argument, never persisted, so the next unforced cycle
                # resumes normal skip behavior with intact bookkeeping.
                record_noop_reuse_invalidation(
                    "operator_forced",
                    owner="operator",
                )
            elif publish_blocker_payload is not None:
                # A caller-supplied run blocker is a one-cycle refusal to advance
                # reader visibility. It must reach Phase B and cut a fresh blocked
                # checkpoint even when graph deltas are zero; reusing the previous
                # published checkpoint would erase the incomplete-run outcome.
                record_noop_reuse_invalidation(
                    "publish_blocker_present",
                    owner=str(publish_blocker_payload["reason"]),
                )
            elif ingest_completeness_changed:
                # A source failed or recovered: reusing the previous
                # generation would keep its stale completeness marker.
                record_noop_reuse_invalidation(
                    "ingest_completeness_changed",
                    owner="ingest",
                )
            elif previous is None:
                record_noop_reuse_invalidation(
                    "no_previous_generation",
                    owner="publisher.branch_checkout",
                )
            elif not previous_checkpoint_matches_published:
                record_noop_reuse_invalidation(
                    (
                        "latest_checkpoint_unresolved"
                        if previous_checkpoint is None
                        else "checkpoint_after_last_published_generation"
                    ),
                    owner="chronos_delta",
                )
            elif previous_catalog_version_id != int(cv):
                record_noop_reuse_invalidation(
                    "catalog_version_changed",
                    owner="catalog",
                )
            elif not extractor_input_reusable:
                record_noop_reuse_invalidation(
                    "extractor_input_signature_changed",
                    owner="extractor",
                )
            elif self.lenses:
                record_noop_reuse_invalidation(
                    "lenses_configured",
                    owner="lenses",
                )
            elif pending_lens_promotions:
                record_noop_reuse_invalidation(
                    "pending_lens_promotions",
                    owner="lenses",
                )
            elif enricher_retry_pending is not None:
                # P6 (specify-publish-commit-protocol): a wired enricher
                # whose last_status != 'success' has a held retry (#679).
                # Reusing the previous generation would skip the enricher
                # stage and the retry would never run on a zero-delta
                # deployment. Invalidate reuse so the enricher re-runs.
                record_noop_reuse_invalidation(
                    "enricher_retry_pending",
                    owner="enrichers",
                )
            elif ready_retry_node_ids or extractor_queue_node_ids:
                record_noop_reuse_invalidation(
                    (
                        "link_retry_ready"
                        if ready_retry_node_ids
                        else "extractor_retry_queued"
                    ),
                    owner="extractor",
                )
            else:
                early_delta_counts: dict[str, int] | None = None
                with _publisher_timed_stage(
                    timings_ms,
                    "current_segment_write_probe",
                ):
                    current_segment_has_writes = (
                        self._chronos_current_segment_has_graph_writes(
                            conn,
                            branch_name=self.branch_name,
                        )
                    )
                if current_segment_has_writes is False:
                    with _publisher_timed_stage(
                        timings_ms,
                        "delta_counts_noop_reuse_probe",
                    ):
                        early_delta_counts = {
                            "nodes_added": 0,
                            "nodes_retracted": 0,
                            "nodes_updated": 0,
                            "edges_added": 0,
                            "edges_retracted": 0,
                            "edges_updated": 0,
                        }
                        set_chronos_delta_counts_cache(early_delta_counts)
                    timings_ms["delta_counts_noop_reuse_probe.segment_empty"] = 1.0
                else:
                    dirty_scope_for_noop = chronos_dirty_scope()
                    dirty_scope_stats = self._chronos_dirty_scope_stats(
                        dirty_scope_for_noop,
                    )
                    if (
                        dirty_scope_stats["available"]
                        and (
                            int(dirty_scope_stats["node_count"])
                            + int(dirty_scope_stats["edge_count"])
                        )
                        > 0
                    ):
                        timings_ms["delta_counts_noop_reuse_probe"] = 0.0
                        timings_ms[
                            "delta_counts_noop_reuse_probe.skipped_dirty_scope"
                        ] = 1.0
                        record_noop_reuse_invalidation(
                            "graph_delta_present",
                            owner="chronos_delta",
                        )
                    else:
                        with _publisher_timed_stage(
                            timings_ms,
                            "delta_counts_noop_reuse_probe",
                        ):
                            early_delta_counts = (
                                chronos_scoped_delta_counts_from_dirty_scope()
                                or chronos_generation_delta_counts()
                            )
                if early_delta_counts is not None:
                    if self._chronos_delta_is_noop(early_delta_counts):
                        n_nodes, n_edges = _counts_after_delta(
                            previous_counts,
                            early_delta_counts,
                            branch=branch,
                        )
                        enricher_requires_run = (
                            self._noop_reuse_enricher_requires_run_name()
                        )
                        pending_enricher_backlog = False
                        if enricher_requires_run is not None:
                            record_noop_reuse_invalidation(
                                "enricher_requires_run",
                                owner=enricher_requires_run,
                            )
                        else:
                            with _publisher_timed_stage(
                                timings_ms,
                                "enricher_backlog_probe",
                            ):
                                pending_enricher_backlog = (
                                    self._has_pending_enricher_backlog(conn)
                                )
                            if pending_enricher_backlog:
                                record_noop_reuse_invalidation(
                                    "pending_enricher_backlog",
                                    owner="enrichers",
                                )
                        if (
                            enricher_requires_run is None
                            and not pending_enricher_backlog
                            and not reuse_policy_pin_matches()
                        ):
                            record_noop_reuse_invalidation(
                                "nomos_policy_pin_changed",
                                owner="nomos",
                            )
                        elif (
                            enricher_requires_run is None
                            and not pending_enricher_backlog
                            and not reuse_gate_inputs_match()
                        ):
                            record_noop_reuse_invalidation(
                                "gate_inputs_changed",
                                owner="consistency_gates",
                            )
                        elif (
                            enricher_requires_run is None
                            and not pending_enricher_backlog
                            and embedding_writes_pending()
                        ):
                            # Vectors written since the previous checkpoint
                            # are readable only in a new one (PACT
                            # openrouter-embeddings-hnsw).
                            record_noop_reuse_invalidation(
                                "embedding_writes",
                                owner="embeddings",
                            )
                        elif (
                            enricher_requires_run is None
                            and not pending_enricher_backlog
                        ):
                            with _publisher_timed_stage(
                                timings_ms,
                                "noop_reuse_check",
                            ):
                                native_views_reusable = (
                                    self.native_view_stage.can_reuse_noop_generation(
                                        conn,
                                        generation_id=gate_target_id,
                                    )
                                )
                            if (
                                native_views_reusable
                                and not self.checkpoint_attestors
                                and not force_native_view_refresh
                                and _authorized_count_stamp_recovery is None
                            ):
                                reuse_result = self._chronos_noop_reuse_result(
                                    conn,
                                    previous=previous,
                                    catalog_version_id=int(cv),
                                    node_count=n_nodes,
                                    edge_count=n_edges,
                                    timings_ms=timings_ms,
                                    cycle_start=cycle_start,
                                    lock_acquired_at=lock_acquired_at,
                                    enricher_results=enricher_results,
                                    enricher_runs=enricher_runs,
                                    extractor_edges_emitted=0,
                                    skipped_stages=[
                                        "extractor_dirty_input_check",
                                        "extractor",
                                        "lens_promotion",
                                        "enrichers",
                                        "derived_fact_load",
                                        "derived_fact_apply",
                                        "gates",
                                        "consistency_gates",
                                        "native_views",
                                        "lenses",
                                        "view_derived_fact_load",
                                        "view_cite_retractions",
                                        "view_derived_fact_apply",
                                        "nomos_policy_pin",
                                        "checkpoint_generation",
                                        "notify",
                                    ],
                                    reuse_stage="pre_enrichment_delta_probe",
                                    graph_counts_provenance=graph_counts_provenance,
                                    reuse_checks={
                                        "catalog_version": "same",
                                        "delta": "noop",
                                        "extractor_input_signature": "same",
                                        "gate_inputs": "same",
                                        "lenses": "absent",
                                        "native_views": "reusable",
                                        "nomos_policy_pin": "same",
                                    },
                                )
                                if reuse_result is not None:
                                    return reuse_result
                                record_noop_reuse_invalidation(
                                    "staged_source_admissions_present",
                                    owner="admission",
                                )
                        if force_native_view_refresh:
                            record_noop_reuse_invalidation(
                                "native_view_refresh_forced",
                                owner="native_views",
                            )
                        else:
                            record_noop_reuse_invalidation(
                                "native_views_not_reusable",
                                owner="native_views",
                            )
                    else:
                        record_noop_reuse_invalidation(
                            "graph_delta_present",
                            owner="chronos_delta",
                        )
            with _publisher_timed_stage(
                timings_ms,
                "extractor_dirty_input_check",
            ):
                # Fork-first publish: use the fork's BASE generation as the
                # extractor baseline so the decision is segment-scoped (a fork that
                # added no extractor input skips the full-graph scan) instead of
                # `previous is None` -> always-run. (The extractor's own scan is not
                # yet segment-scoped; a fork that DID add text-bearing input still
                # full-scans -- tracked as a follow-up perf item.)
                run_extractor_for_dirty_input = self._should_run_chronos_extractor(
                    conn,
                    previous=(
                        previous if previous is not None else fork_gate_baseline
                    ),
                    input_signature=extractor_input_signature,
                    branch_name=self.branch_name,
                )
                run_extractor = bool(
                    run_extractor_for_dirty_input
                    or ready_retry_node_ids
                    or extractor_queue_node_ids
                )
            self._link_endpoints_cited = set()
            if run_extractor:
                # The extractor writes `_extractor:*` mention edges that omit
                # source_program_checksum; witness them as enrichment (#2965).
                persist_transform_program(conn, _ENRICHMENT_PROGRAM)
                with _publisher_timed_stage(
                    timings_ms, "extractor",
                ), commit_witness_session(conn, _ENRICHMENT_CHECKSUM):
                    (
                        extractor_edges_emitted,
                        link_retry_receipt,
                        processed_extractor_node_ids,
                    ) = self._run_chronos_extractor(
                        conn,
                        store=store,
                        branch_name=self.branch_name,
                        generation_id=0,
                        catalog_version_id=int(cv),
                        target_node_ids=(
                            sorted(set(
                                ready_retry_node_ids
                                + extractor_queue_node_ids
                            ))
                            if not run_extractor_for_dirty_input
                            else None
                        ),
                    )
                    extractor_queue_receipt = sorted(
                        set(extractor_queue_node_ids)
                        & set(processed_extractor_node_ids)
                    )
            else:
                extractor_edges_emitted = 0
            if self._extractor_stage.endpoint_routes:
                # Issue #3666: withdraw link endpoints whose last citation or
                # whose missing record changed in this publish.
                persist_transform_program(conn, _ENRICHMENT_PROGRAM)
                with _publisher_timed_stage(
                    timings_ms, "link_endpoint_sweep",
                ), commit_witness_session(conn, _ENRICHMENT_CHECKSUM):
                    self._sweep_link_endpoints(
                        conn,
                        store=store,
                        branch_name=self.branch_name,
                        generation_id=0,
                    )
            with _publisher_timed_stage(timings_ms, "lens_promotion"):
                self._run_lens_promotion_phase(
                    conn,
                    generation_id=gate_target_id,
                )
            if self.enrichers:
                if prelock_enrich_state is not None and (
                    prelock_enrich_state.plans
                ):
                    with _publisher_timed_stage(
                        timings_ms, "prelock_enrich_recheck",
                    ):
                        self._prelock_enrich_recheck(
                            conn, store, prelock_enrich_state,
                        )
                with _publisher_timed_stage(timings_ms, "enrichers"):
                    enricher_runs = self._run_enrichers(
                        conn,
                        generation_id=0,
                        catalog_version_id=int(cv),
                        previous=previous,
                        chronos_dirty_scope=chronos_dirty_scope(),
                        publisher_timings_ms=timings_ms,
                        force_enrichers=force_enrichers,
                        compat_generation_id=gate_target_id,
                        caller_owned_transaction=(
                            caller_verified_transaction is not None
                        ),
                        prelock_state=prelock_enrich_state,
                    )
                # Legacy enrichers still emit fact-log rows. In
                # Chronos-native mode, ingest those audit facts into the
                # target graph before checkpointing so derived graph content
                # is captured without a live-table projector.
            with _publisher_timed_stage(timings_ms, "derived_verdict_rollup"):
                self._emit_derived_verdict_rollup()
            for event in self._derived_verdict_events:
                enricher_results.append(dict(event))
            derived_fact_summary: dict[str, int] = {
                "pre_view_loaded_count": 0,
                "pre_view_applied_count": 0,
                "view_loaded_count": 0,
                "view_synthetic_count": 0,
                "view_applied_count": 0,
                "total_loaded_count": 0,
                "total_applied_count": 0,
            }
            with _publisher_timed_stage(timings_ms, "derived_fact_load"):
                from okg.substrate.nomos.runtime_enforcement import (
                    deployment_nomos_enforcement_mode,
                )
                derived_batches = self._load_unapplied_derived_facts(
                    conn,
                    min_node_fact_id=pre_derived_node_fact_id,
                    min_edge_fact_id=pre_derived_edge_fact_id,
                    timings_ms=timings_ms,
                    timing_prefix="derived_fact_load",
                    enforcement_mode=deployment_nomos_enforcement_mode(
                        self.deployment_name
                    ),
                    group_by_producer=True,
                )
            derived_facts = [
                fact
                for batch in derived_batches
                for fact in batch.facts
            ]
            derived_fact_summary["pre_view_loaded_count"] = len(derived_facts)
            derived_fact_summary["total_loaded_count"] += len(derived_facts)
            if derived_batches:
                if prelock_enrich_state is not None and (
                    prelock_enrich_state.prepared
                ):
                    with _publisher_timed_stage(
                        timings_ms, "prelock_enrich_derived_recheck",
                    ):
                        self._prelock_enrich_derived_recheck(
                            conn, store, prelock_enrich_state,
                        )
                with _publisher_timed_stage(timings_ms, "derived_fact_apply"):
                    for batch in derived_batches:
                        prepared_chunks = (
                            prelock_enrich_state.prepared_for(
                                batch.producer_id, batch.run_id,
                            )
                            if prelock_enrich_state is not None
                            else None
                        )
                        derived_result = (
                            self._apply_publisher_derived_fact_batch(
                                conn,
                                store,
                                batch.facts,
                                catalog_version_id=int(cv),
                                gate_target_id=gate_target_id,
                                stage="pre_view",
                                previous_generation_ref=(
                                    str(previous.checkpoint_id)
                                    if previous is not None
                                    else None
                                ),
                                producer_id=batch.producer_id,
                                producer_kind=batch.producer_kind,
                                source=batch.producer_id,
                                run_id=batch.run_id,
                                prepared_chunks=prepared_chunks,
                            )
                        )
                        if prepared_chunks:
                            prelock_enrich_state.note_derived_result(
                                derived_result,
                            )
                    if prelock_enrich_state is not None:
                        # Done with the pre-lock results; free their memory
                        # for the rest of the cycle.
                        prelock_enrich_state.release()
                    branch = store.checkout(self.branch_name)
                    invalidate_chronos_scope_cache()
                derived_fact_summary["pre_view_applied_count"] = len(derived_facts)
                derived_fact_summary["total_applied_count"] += len(derived_facts)
                if previous is None:
                    with _publisher_timed_stage(
                        timings_ms,
                        "current_graph_counts_after_derived",
                    ):
                        n_nodes, n_edges = _counted_graph(branch)

            if previous is None:
                assert n_nodes is not None
                assert n_edges is not None
                (
                    nodes_added, nodes_retracted, nodes_updated,
                    edges_added, edges_retracted, edges_updated,
                ) = fork_first_change_receipt(n_nodes, n_edges)
            else:
                # Anything written to the branch head since the cycle last
                # read it -- an enricher writing the graph directly, another
                # process -- is not in the cached scope. Re-read before the
                # counts and the gates use it; after this point a new write
                # refuses the checkpoint instead (see the cut).
                signature_before_counting = self._chronos_segment_signature_now(
                    conn,
                )
                if (
                    signature_before_counting != segment_signature_at_scope_read
                    or signature_before_counting != segment_signature_counted
                ):
                    invalidate_chronos_scope_cache()
                    dirty_scope_hint_decision["rescanned_before_counting"] = True
                with _publisher_timed_stage(
                    timings_ms,
                    "delta_counts_before_views",
                ):
                    delta_counts = (
                        chronos_scoped_delta_counts_from_dirty_scope()
                        or chronos_generation_delta_counts()
                    )
                nodes_added = int(delta_counts["nodes_added"])
                nodes_retracted = int(delta_counts["nodes_retracted"])
                nodes_updated = int(delta_counts.get("nodes_updated", 0))
                edges_added = int(delta_counts["edges_added"])
                edges_retracted = int(delta_counts["edges_retracted"])
                edges_updated = int(delta_counts.get("edges_updated", 0))
                n_nodes, n_edges = _counts_after_delta(
                    previous_counts,
                    delta_counts,
                    branch=branch,
                )
            noop = (
                nodes_added == 0
                and nodes_retracted == 0
                and nodes_updated == 0
                and edges_added == 0
                and edges_retracted == 0
                and edges_updated == 0
            )
            _emit_phase_event_with_timing(timings_ms, "phase_a_complete", {
                "nodes_added": nodes_added,
                "nodes_retracted": nodes_retracted,
                "nodes_updated": nodes_updated,
                "edges_added": edges_added,
                "edges_retracted": edges_retracted,
                "edges_updated": edges_updated,
                "noop": noop,
                "duration_ms": int((time.monotonic() - cycle_start) * 1000),
            })
            native_views_reusable = False
            post_enrichment_pending_enricher_backlog = False
            if (
                noop
                and previous is not None
                and _authorized_count_stamp_recovery is None
                and previous_checkpoint_matches_published
                and previous_catalog_version_id == int(cv)
                and not self.lenses
                and not pending_lens_promotions
                and not self.post_publish_consistency_checks
                and not force_enrichers
                and not ready_retry_node_ids
                and not extractor_queue_node_ids
                and publish_blocker_payload is None
                and not ingest_completeness_changed
                and not self.checkpoint_attestors
            ):
                with _publisher_timed_stage(
                    timings_ms,
                    "enricher_backlog_probe.post_enrichment",
                ):
                    post_enrichment_pending_enricher_backlog = (
                        self._has_pending_enricher_backlog(conn)
                    )
                if post_enrichment_pending_enricher_backlog:
                    # The upstream stage may have created this lag after the
                    # phase-start snapshot. Preserve the current blocker in
                    # the result even when an earlier reason forced the phase.
                    record_noop_reuse_invalidation(
                        "pending_enricher_backlog",
                        owner="enrichers",
                        replace=True,
                    )
            if (
                noop
                and previous is not None
                and _authorized_count_stamp_recovery is None
                and previous_checkpoint_matches_published
                and previous_catalog_version_id == int(cv)
                and reuse_policy_pin_matches()
                and reuse_gate_inputs_match()
                and not self.lenses
                and not pending_lens_promotions
                and not self.post_publish_consistency_checks
                # P6: a forced cycle never reuses the noop generation (see the
                # post-enrichment reuse guard below), so the reuse probe is moot.
                and not force_enrichers
                and not ready_retry_node_ids
                and not extractor_queue_node_ids
                and not post_enrichment_pending_enricher_backlog
                and publish_blocker_payload is None
                and not ingest_completeness_changed
                and not self.checkpoint_attestors
            ):
                with _publisher_timed_stage(
                    timings_ms,
                    "noop_reuse_check",
                ):
                    native_views_reusable = (
                        self.native_view_stage.can_reuse_noop_generation(
                            conn,
                            generation_id=gate_target_id,
                        )
                    )
            if (
                noop
                and previous is not None
                and _authorized_count_stamp_recovery is None
                and previous_checkpoint_matches_published
                and previous_catalog_version_id == int(cv)
                # Native views are generation-scoped derived outputs. They
                # may participate in no-op generation reuse only when their
                # manifest signatures prove the prior artifacts can be
                # carried forward without rebuilding the corpus or relation
                # slots. Lenses still disable reuse because their putback
                # overlays can move independently of source fact watermarks.
                and reuse_policy_pin_matches()
                and reuse_gate_inputs_match()
                and native_views_reusable
                and not self.lenses
                and not pending_lens_promotions
                and not force_native_view_refresh
                and not post_enrichment_pending_enricher_backlog
                # P6 (specify-publish-commit-protocol): a forced cycle must
                # cut a checkpoint even when the post-enrichment recompute is
                # zero-delta (idempotent re-derive). Without this clause a
                # `--force-enrichers` run on a zero-delta deployment would
                # re-run enrichers but still short-circuit here, skipping
                # gates/checkpoint_generation and violating the force-lever
                # acceptance ("runs enrichers, gates, checkpoint").
                and not force_enrichers
                and publish_blocker_payload is None
                and not ingest_completeness_changed
                and not self.checkpoint_attestors
                # Embedding-only writes leave the delta at zero but are
                # readable only in a new checkpoint (PACT
                # openrouter-embeddings-hnsw).
                and not embedding_writes_pending()
            ):
                reuse_result = self._chronos_noop_reuse_result(
                    conn,
                    previous=previous,
                    catalog_version_id=int(cv),
                    node_count=n_nodes,
                    edge_count=n_edges,
                    timings_ms=timings_ms,
                    cycle_start=cycle_start,
                    lock_acquired_at=lock_acquired_at,
                    enricher_results=enricher_results,
                    enricher_runs=enricher_runs,
                    extractor_edges_emitted=extractor_edges_emitted,
                    skipped_stages=[
                        "gates",
                        "consistency_gates",
                        "native_views",
                        "lenses",
                        "view_derived_fact_load",
                        "view_cite_retractions",
                        "view_derived_fact_apply",
                        "nomos_policy_pin",
                        "checkpoint_generation",
                        "notify",
                    ],
                    reuse_stage="post_enrichment_delta_counts",
                    graph_counts_provenance=graph_counts_provenance,
                    reuse_checks={
                        "catalog_version": "same",
                        "delta": "noop",
                        "gate_inputs": "same",
                        "lenses": "absent",
                        "native_views": "reusable",
                        "nomos_policy_pin": "same",
                    },
                    noop_reuse_invalidation=noop_reuse_invalidation,
                )
                if reuse_result is not None:
                    return reuse_result
                record_noop_reuse_invalidation(
                    "staged_source_admissions_present",
                    owner="admission",
                )
            if (
                force_native_view_refresh
                and noop
                and previous is not None
                and previous_checkpoint_matches_published
                and previous_catalog_version_id == int(cv)
                and native_views_reusable
                and not self.lenses
            ):
                record_noop_reuse_invalidation(
                    "native_view_refresh_forced",
                    owner="native_views",
                )
            forced_noop_native_view_gate_reuse = (
                force_native_view_refresh
                and noop
                and previous is not None
                and previous_checkpoint_matches_published
                and previous_catalog_version_id == int(cv)
                and native_views_reusable
                and not self.lenses
            )

            gate_generation = ReservedGeneration(
                generation_id=gate_target_id,
                deployment_name=self.deployment_name,
                catalog_version_id=int(cv),
                correlation_id=(
                    f"chronos-native-gate:"
                    f"{self.deployment_name}:{gate_target_id}"
                ),
                _conn=conn,
            )
            phase_a_for_gates = PhaseAResult(
                converged=True,
                per_source_status={},
                projection_cursor_by_source={},
                timed_out=False,
            )
            if current_gate_cache_key is None:
                with _publisher_timed_stage(timings_ms, "gate_cache_key"):
                    current_gate_cache_key = self._gate_cache_key(
                        gate_generation,
                        ignore_derived_artifact_facts=True,
                    )
            with _publisher_timed_stage(timings_ms, "gates"):
                phase_b = evaluate_gates(
                    self.dsn,
                    gate_generation,
                    phase_a_for_gates,
                    total_timeout_ms=self.gate_total_timeout_ms,
                    branch_name=self.branch_name,
                    source_states=self._source_states,
                )
            phase_b = replace(phase_b, gate_cache_key=current_gate_cache_key)

            # Fork-first publish: redirect the gate's baseline + delta_counts to the
            # fork's BASE generation catalog + the fork's own segment delta, so
            # evaluate_incremental runs SCOPED over the fork's edges instead of
            # full-validating the inherited base. delta_counts is derived from the
            # POST-enrichment dirty scope (the same one the gate reads), so the
            # fork's derived edges are included; if the base catalog differs from
            # the active catalog the gate correctly falls back to full validation.
            gate_previous_catalog_version_id = previous_catalog_version_id
            gate_delta_counts = delta_counts
            if fork_gate_baseline_cv is not None:
                gate_previous_catalog_version_id = fork_gate_baseline_cv
                gate_delta_counts = self._fork_scoped_delta_counts(
                    chronos_dirty_scope()
                )
                if gate_delta_counts is None and fork_block_reason is None:
                    # Base resolved, but the fork's segment dirty-scope is
                    # unavailable (no interval edge table). Block LOUDLY with a
                    # distinct reason rather than passing delta_counts=None to the
                    # gate, which would silently full-validate the inherited base and
                    # re-trip its latent violations (a confusing base-violation block
                    # for what is really a fork-scoping failure).
                    fork_block_reason = "fork_delta_scope_unavailable"
            if fork_block_reason is not None:
                # A fork that cannot be delta-scoped MUST block loudly -- never
                # silently full-validate (re-trips the inherited base's latent
                # violations) or publish unvalidated.
                phase_b = replace(
                    phase_b,
                    blocked=True,
                    blocked_reason={
                        "reason": fork_block_reason,
                        "branch_name": self.branch_name,
                        "detail": {
                            "fork_base_generation_unresolved": (
                                "fork base generation is missing, unpublished, or "
                                "not a published okg_generation on default; cannot "
                                "scope the consistency gate to the fork delta"
                            ),
                            "fork_delta_scope_unavailable": (
                                "the fork's segment dirty-scope is unavailable (no "
                                "interval edge table); cannot scope the consistency "
                                "gate to the fork delta"
                            ),
                        }[fork_block_reason],
                    },
                )

            def recount_after_gate_removal(
                branch_session: Any,
                current_delta: dict[str, int] | None,
            ) -> tuple[
                dict[str, int] | None,
                tuple[int, int, int, int, int, int],
                tuple[int, int],
            ]:
                """Delta, change receipt and counts of the branch AFTER a gate removal.

                A gate that prunes rows invalidates the delta taken before it, and
                everything read from that delta afterwards -- the view-scoped
                adjustment and the checkpoint counts -- inherits the error (#2305).
                Both consistency passes call this; it takes the documented full
                recompute, since the scoped adjustment's key set does not cover
                gate-removed rows. Returns (delta_counts, the six receipt counts,
                (n_nodes, n_edges)).
                """
                if previous is None:
                    # Through the memo, so the final stamp that counts this same
                    # checkout reuses the result instead of scanning it again.
                    counted = _counted_graph(branch_session)
                    return (
                        current_delta,
                        fork_first_change_receipt(*counted),
                        counted,
                    )
                recounted = chronos_generation_delta_counts()
                return (
                    recounted,
                    (
                        int(recounted["nodes_added"]),
                        int(recounted["nodes_retracted"]),
                        int(recounted.get("nodes_updated", 0)),
                        int(recounted["edges_added"]),
                        int(recounted["edges_retracted"]),
                        int(recounted.get("edges_updated", 0)),
                    ),
                    _counts_after_delta(
                        previous_counts,
                        recounted,
                        branch=branch_session,
                    ),
                )

            consistency_summaries: list[dict[str, Any]] = []
            if (
                self.consistency_gates
                and fork_block_reason is None
                and forced_noop_native_view_gate_reuse
            ):
                timings_ms[
                    "consistency_gates."
                    "skipped_for_forced_native_view_refresh_noop"
                ] = 1.0
                consistency_summaries.append({
                    "check_id": "consistency_gates",
                    "gate_name": "consistency_gates",
                    "target_generation_id": gate_target_id,
                    "result": "skipped_reused_previous",
                    "blocks_publish": False,
                    "reason": "forced_native_view_refresh_noop",
                    "open_observation_count": 0,
                    "newly_opened_count": 0,
                    "resolved_count": 0,
                    "coverage_complete": True,
                    "coverage_basis": "previous_generation_same_gate_inputs",
                    "budget_ms": None,
                    "elapsed_ms": 0,
                    "details": {
                        "force_native_view_refresh": True,
                        "previous_generation_id": str(
                            getattr(previous, "checkpoint_id"),
                        ),
                    },
                })
            elif self.consistency_gates and fork_block_reason is None:
                with _publisher_timed_stage(timings_ms, "consistency_gates"):
                    consistency_summaries.extend(
                        self._evaluate_consistency_gates(
                            conn,
                            target_generation_id=gate_target_id,
                            catalog_version_id=int(cv),
                            previous_catalog_version_id=(
                                gate_previous_catalog_version_id
                            ),
                            delta_counts=gate_delta_counts,
                            dirty_scope=chronos_dirty_scope(),
                            gate_pass="pre_view",
                        )
                    )
                # Drop the shared measurement unconditionally, BEFORE asking how
                # much the gates removed. Consistency gates commit their
                # retractions in their own transactions and build their summary
                # afterwards, so a gate that mutated the branch and then RAISED
                # is recorded as `blocked_error` with no mutation details --
                # zero removals, no re-checkout below, and a memo that would
                # otherwise survive across writes that already landed. The
                # result would be a stamp describing the pre-mutation graph,
                # which this file treats as a P0. Counting again is the only
                # cost of being wrong here.
                _counts_memo.clear()
                gate_mutations = self._consistency_gate_edge_mutation_counts(
                    consistency_summaries,
                )
                if gate_mutations["edges_removed"] or gate_mutations["nodes_removed"]:
                    # Gates mutate the branch after the publisher first cached
                    # its delta and checkout. Views, maintained count rollups,
                    # and the final count stamp must read the post-gate branch.
                    invalidate_chronos_scope_cache()
                    branch = store.checkout(self.branch_name)
                    # The delta above was taken before the prune. Views adjust
                    # it in place, and a later post-view pass that removes
                    # nothing never recounts, so recount here (#2305).
                    with _publisher_timed_stage(
                        timings_ms,
                        "delta_counts_after_pre_view_gate",
                    ):
                        (
                            delta_counts,
                            (
                                nodes_added, nodes_retracted, nodes_updated,
                                edges_added, edges_retracted, edges_updated,
                            ),
                            (n_nodes, n_edges),
                        ) = recount_after_gate_removal(branch, delta_counts)
                    noop = (
                        nodes_added == 0
                        and nodes_retracted == 0
                        and nodes_updated == 0
                        and edges_added == 0
                        and edges_retracted == 0
                        and edges_updated == 0
                    )
                blocked_gates = [
                    summary for summary in consistency_summaries
                    if summary.get("blocks_publish")
                ]
                if blocked_gates:
                    phase_b = replace(
                        phase_b,
                        blocked=True,
                        blocked_reason=self._merge_consistency_blocked_reason(
                            phase_b.blocked_reason,
                            blocked_gates,
                        ),
                    )
                phase_b = replace(
                    phase_b,
                    consistency_gates=consistency_summaries,
                )
            if publish_blocker_payload is not None:
                combined_blocker = dict(publish_blocker_payload)
                if phase_b.blocked and phase_b.blocked_reason is not None:
                    combined_blocker["phase_b_blocked_reason"] = (
                        phase_b.blocked_reason
                    )
                phase_b = replace(
                    phase_b,
                    blocked=True,
                    blocked_reason=combined_blocker,
                )
            blocked_reason = phase_b.blocked_reason if phase_b.blocked else None
            gate_excluded_sources = sorted(phase_b.excluded_sources.keys())
            invariant_summary = _build_invariant_summary(phase_b)
            _emit_phase_event_with_timing(timings_ms, "phase_b_complete", {
                "blocked": phase_b.blocked,
                "blocked_reason": blocked_reason or {},
                "consistency_gate_count": len(consistency_summaries),
                "duration_ms": int((time.monotonic() - cycle_start) * 1000),
            })
            if not blocked_reason:
                view_generation = ReservedGeneration(
                    generation_id=gate_target_id,
                    deployment_name=self.deployment_name,
                    catalog_version_id=int(cv),
                    correlation_id=(
                        f"chronos-native-gate:"
                        f"{self.deployment_name}:{gate_target_id}"
                    ),
                    _conn=conn,
                )
                pre_view_node_fact_id, pre_view_edge_fact_id = (
                    self._current_fact_watermarks(conn)
                )
                with _publisher_timed_stage(timings_ms, "native_views"):
                    self._run_post_publish_views(
                        view_generation,
                        published_at=generation_published_at,
                        chronos_dirty_scope=chronos_dirty_scope(),
                        final_view_refresh=native_view_refresh_final,
                        force_native_view_refresh=force_native_view_refresh,
                        graph_store=store,
                        caller_owned_transaction=(
                            caller_verified_transaction is not None
                        ),
                    )
                    _add_native_view_timing_details(
                        timings_ms,
                        self._last_view_outcomes,
                    )
                _emit_phase_event_with_timing(
                    timings_ms,
                    "post_publish_views_complete",
                    {
                    "view_count": len(self._last_view_outcomes),
                    # Durable half of the same fact: the phase event is what
                    # an operator reads back after the cycle is gone, so a
                    # degraded view stage has to be visible there too. Read
                    # through the normalizing report so the keys are present
                    # on every refresh shape.
                    "failed_view_count": int(
                        self._native_view_refresh_report()["failed_view_count"]
                    ),
                    "failed_view_ids": [
                        str(failure.get("view_id"))
                        for failure in self._native_view_refresh_report()[
                            "failed_views"
                        ]
                    ],
                    "degraded_view_count": int(
                        self._native_view_refresh_report().get(
                            "degraded_view_count", 0
                        )
                    ),
                    "degraded_view_ids": [
                        str(degradation.get("view_id"))
                        for degradation in self._native_view_refresh_report().get(
                            "degraded_views", []
                        )
                    ],
                    "degraded_views": list(
                        self._native_view_refresh_report().get(
                            "degraded_views", []
                        )
                    ),
                    "duration_ms": int((time.monotonic() - cycle_start) * 1000),
                    },
                )
                with _publisher_timed_stage(timings_ms, "lenses"):
                    self._run_post_publish_lenses(view_generation)
                _emit_phase_event_with_timing(
                    timings_ms,
                    "post_publish_lenses_complete",
                    {
                    "lens_count": len(self._last_lens_outcomes),
                    "duration_ms": int((time.monotonic() - cycle_start) * 1000),
                    },
                )
                # Nomos decision-log retention (issue #635): the decision log
                # is bounded here, once per publish, on the privileged worker
                # connection (app_rw/okg_mcp deliberately lack DELETE). A
                # misconfigured bound raises — never a silent skip.
                from okg.substrate.nomos.retention import (
                    prune_nomos_decisions,
                )

                with _publisher_timed_stage(
                    timings_ms,
                    "nomos_decision_retention",
                ):
                    pruned_nomos_decisions = prune_nomos_decisions(
                        conn,
                        deployment_name=self.deployment_name,
                    )
                _emit_phase_event_with_timing(
                    timings_ms,
                    "nomos_decision_retention_complete",
                    {
                    "deleted_rows": pruned_nomos_decisions,
                    "duration_ms": int((time.monotonic() - cycle_start) * 1000),
                    },
                )
                # Chronos-native publishing runs consistency gates inline
                # in Phase B (no separate post-publish consistency stage);
                # emit the contract's phase boundary with the empty
                # post-publish set so operators' phase timelines stay
                # complete.
                _emit_phase_event_with_timing(
                    timings_ms,
                    "post_publish_consistency_complete",
                    {
                    "checks": [],
                    "duration_ms": int((time.monotonic() - cycle_start) * 1000),
                    },
                )
                with _publisher_timed_stage(
                    timings_ms,
                    "view_derived_fact_load",
                ):
                    from okg.substrate.nomos.runtime_enforcement import (
                        deployment_nomos_enforcement_mode,
                    )
                    view_derived_batches = self._load_unapplied_derived_facts(
                        conn,
                        generation_id=gate_target_id,
                        min_node_fact_id=pre_view_node_fact_id,
                        min_edge_fact_id=pre_view_edge_fact_id,
                        timings_ms=timings_ms,
                        timing_prefix="view_derived_fact_load",
                        enforcement_mode=deployment_nomos_enforcement_mode(
                            self.deployment_name
                        ),
                        group_by_producer=True,
                    )
                view_derived_facts = [
                    fact
                    for batch in view_derived_batches
                    for fact in batch.facts
                ]
                derived_fact_summary["view_loaded_count"] = len(
                    view_derived_facts
                )
                derived_fact_summary["total_loaded_count"] += len(
                    view_derived_facts
                )
                # View-cites retraction is owned by the native-view stage:
                # `_project_view_cites` diffs each changed fragment's desired
                # citations against the live branch head and emits BOTH the
                # op="I" inserts and the op="R" retractions (including for
                # dropped fragments), leaving reused fragments untouched. The
                # former publisher-side safety net rebuilt "desired" from only
                # the partial insert set and retracted every OTHER live citation
                # edge of the view -- silently deleting reused-but-valid
                # fragments' lineage on every warm publish (a fragment changes,
                # its insert is the lone op="I", and every reused fragment's
                # citation gets retracted). The stage's per-fragment diff is
                # complete, so that net is removed.
                synthetic_view_facts: list[Any] = []
                if self._last_view_outcomes:
                    from okg.substrate.library.sources.base import NodeFact
                    from okg.substrate.nomos.source_lineage import (
                        LINEAGE_ATTR,
                        normalize_source_lineage,
                    )

                    synthetic_view_facts = [
                        NodeFact(
                            node_id=f"view:{out.view_id}",
                            subtype="view",
                            attrs={
                                "view_id": out.view_id,
                                "canonical_asset_id": getattr(
                                    out, "asset_id", None,
                                ),
                                "canonical_asset_digest": getattr(
                                    out, "asset_digest", None,
                                ),
                                LINEAGE_ATTR: normalize_source_lineage(
                                    "catalog:views_registry"
                                ),
                            },
                            source_record_id={"view_id": out.view_id},
                            source_revision={
                                "generation_id": gate_target_id,
                            },
                        )
                        for out in self._last_view_outcomes
                        if getattr(out, "error", None) is None
                    ]
                    view_derived_facts = [
                        *synthetic_view_facts,
                        *view_derived_facts,
                    ]
                    if synthetic_view_facts:
                        view_derived_batches = [
                            _DerivedFactBatch(
                                producer_id="_system:native_views",
                                producer_kind="workflow",
                                run_id=uuid.uuid5(
                                    uuid.NAMESPACE_URL,
                                    "|".join((
                                        "okg.native-views.v1",
                                        self.deployment_name,
                                        self.branch_name,
                                        str(gate_target_id),
                                    )),
                                ),
                                facts=tuple(synthetic_view_facts),
                            ),
                            *view_derived_batches,
                        ]
                derived_fact_summary["view_synthetic_count"] = len(
                    synthetic_view_facts
                )
                derived_fact_summary["total_loaded_count"] += len(
                    synthetic_view_facts
                )
                if view_derived_facts:
                    view_delta_keys = self._chronos_fact_delta_keys(
                        view_derived_facts,
                    )
                    pre_view_delta_for_keys: dict[str, int] | None = None
                    if (
                        previous is not None
                        and delta_counts is not None
                        and (view_delta_keys[0] or view_delta_keys[1])
                    ):
                        with _publisher_timed_stage(
                            timings_ms,
                            "delta_counts_view_scope_before",
                        ):
                            pre_view_delta_for_keys = (
                                self._chronos_current_segment_delta_counts_for_keys(
                                    conn,
                                    branch_name=self.branch_name,
                                    previous=previous,
                                    node_ids=view_delta_keys[0],
                                    edge_ids=view_delta_keys[1],
                                )
                            )
                    # The view's rows are the publisher's own writes after
                    # the counts; their counts are adjusted for the view's
                    # keys only. Write them under the branch write lock:
                    # refuse first if anyone else wrote since the counts,
                    # then fingerprint the result before releasing, so the
                    # counted fingerprint covers these writes and nothing
                    # else.
                    view_writes_fenced = (
                        previous is not None
                        and store._lock_branch_write_session(self.branch_name)
                    )
                    with ExitStack() as view_write_fence, _publisher_timed_stage(
                        timings_ms,
                        "view_derived_fact_apply",
                    ):
                        if view_writes_fenced:
                            view_write_fence.callback(
                                store._unlock_branch_write_session,
                                self.branch_name,
                            )
                            self._chronos_refuse_writes_since_counted(
                                conn,
                                segment_signature_counted,
                                stage="before_view_writes",
                                lock=False,
                            )
                        for batch in view_derived_batches:
                            self._apply_publisher_derived_fact_batch(
                                conn,
                                store,
                                batch.facts,
                                catalog_version_id=int(cv),
                                gate_target_id=gate_target_id,
                                stage="view",
                                previous_generation_ref=(
                                    str(previous.checkpoint_id)
                                    if previous is not None
                                    else None
                                ),
                                producer_id=batch.producer_id,
                                producer_kind=batch.producer_kind,
                                source=batch.producer_id,
                                run_id=batch.run_id,
                            )
                        if view_writes_fenced:
                            segment_signature_counted = (
                                self._chronos_segment_signature_now(conn)
                            )
                        # Issue #2941: the view writes are fenced (nobody
                        # else wrote since the counts) and change only the
                        # view facts' own keys -- a node retraction would
                        # also delete incident edges, so it rescans. The
                        # scope is then the scope read before them with
                        # those keys read again, not a rescan of the
                        # whole segment (97 s on okg-workspace).
                        merged_scope = self._chronos_scope_with_view_keys(
                            conn,
                            scope=(
                                dirty_scope_cache if dirty_scope_is_scan
                                else None
                            ),
                            fenced=view_writes_fenced,
                            previous=previous,
                            view_facts=view_derived_facts,
                            view_delta_keys=view_delta_keys,
                            timings_ms=timings_ms,
                        )
                        branch = store.checkout(self.branch_name)
                        invalidate_chronos_scope_cache()
                        if merged_scope is not None:
                            dirty_scope_cache = merged_scope
                            dirty_scope_is_scan = True
                            segment_signature_at_scope_read = (
                                segment_signature_counted
                            )
                    derived_fact_summary["view_applied_count"] = len(
                        view_derived_facts
                    )
                    derived_fact_summary["total_applied_count"] += len(
                        view_derived_facts
                    )
                    if previous is None:
                        with _publisher_timed_stage(
                            timings_ms,
                            "current_graph_counts_after_views",
                        ):
                            n_nodes, n_edges = (
                                _counted_graph(branch)
                            )
                        # Fork-first: re-derive the scoped change receipt AFTER the
                        # view-derived facts landed (they invalidated the scope
                        # cache above) so view outputs count toward the fork delta.
                        (
                            nodes_added, nodes_retracted, nodes_updated,
                            edges_added, edges_retracted, edges_updated,
                        ) = fork_first_change_receipt(n_nodes, n_edges)
                    else:
                        scoped_delta_applied = False
                        if (
                            delta_counts is not None
                            and pre_view_delta_for_keys is not None
                            and (view_delta_keys[0] or view_delta_keys[1])
                        ):
                            with _publisher_timed_stage(
                                timings_ms,
                                "delta_counts_view_scope_after",
                            ):
                                post_view_delta_for_keys = (
                                    self._chronos_current_segment_delta_counts_for_keys(
                                        conn,
                                        branch_name=self.branch_name,
                                        previous=previous,
                                        node_ids=view_delta_keys[0],
                                        edge_ids=view_delta_keys[1],
                                    )
                                )
                            if post_view_delta_for_keys is not None:
                                delta_counts = (
                                    self._chronos_adjust_delta_counts_for_scoped_change(
                                        delta_counts,
                                        pre_view_delta_for_keys,
                                        post_view_delta_for_keys,
                                    )
                                )
                                set_chronos_delta_counts_cache(delta_counts)
                                scoped_delta_applied = True
                        if not scoped_delta_applied:
                            with _publisher_timed_stage(
                                timings_ms,
                                "delta_counts_after_views",
                            ):
                                delta_counts = chronos_generation_delta_counts()
                        nodes_added = int(delta_counts["nodes_added"])
                        nodes_retracted = int(delta_counts["nodes_retracted"])
                        nodes_updated = int(
                            delta_counts.get("nodes_updated", 0)
                        )
                        edges_added = int(delta_counts["edges_added"])
                        edges_retracted = int(delta_counts["edges_retracted"])
                        edges_updated = int(
                            delta_counts.get("edges_updated", 0)
                        )
                        n_nodes, n_edges = _counts_after_delta(
                            previous_counts,
                            delta_counts,
                            branch=branch,
                        )
                    noop = (
                        nodes_added == 0
                        and nodes_retracted == 0
                        and nodes_updated == 0
                        and edges_added == 0
                        and edges_retracted == 0
                        and edges_updated == 0
                    )
                    # #1426: native views, lenses and their derived facts are
                    # applied AFTER Phase B's consistency gate and BEFORE the
                    # checkpoint, and `_apply_publisher_derived_fact_batch`
                    # applies them with strict_endpoints=False. This branch is
                    # the ONLY post-gate path that writes graph rows, so it is
                    # exactly where uncovered facts enter a published
                    # generation. Re-run the structural gates over the
                    # recomputed segment scope -- a superset of the pre-view
                    # pass's -- so every fact in the checkpoint has been
                    # structurally checked. Post-publish checks cannot serve
                    # here: they start after the checkpoint exists, so they
                    # cannot stop reader visibility.
                    if self.consistency_gates and fork_block_reason is None:
                        with _publisher_timed_stage(
                            timings_ms,
                            "consistency_gates.post_view",
                        ):
                            post_view_summaries = (
                                self._evaluate_consistency_gates(
                                    conn,
                                    target_generation_id=gate_target_id,
                                    catalog_version_id=int(cv),
                                    previous_catalog_version_id=(
                                        gate_previous_catalog_version_id
                                    ),
                                    delta_counts=(
                                        self._fork_scoped_delta_counts(
                                            chronos_dirty_scope()
                                        )
                                        if fork_gate_baseline_cv is not None
                                        else delta_counts
                                    ),
                                    dirty_scope=chronos_dirty_scope(),
                                    gate_pass="post_view",
                                )
                            )
                        # The post-view verdict SUPERSEDES the pre-view one for
                        # the same gate: it covers everything the pre-view pass
                        # covered plus the view output, and the checkpoint's
                        # invariant_summary is what a later cycle reads back as
                        # its induction basis. Recording the narrower pass there
                        # would claim coverage the run does not have. The
                        # superseded verdict is kept inside the details.
                        post_view_by_gate = {
                            str(summary.get("gate_name")): summary
                            for summary in post_view_summaries
                        }
                        merged_summaries: list[dict[str, Any]] = []
                        for summary in consistency_summaries:
                            replacement = post_view_by_gate.pop(
                                str(summary.get("gate_name")),
                                None,
                            )
                            if replacement is None:
                                merged_summaries.append(summary)
                                continue
                            replacement_details = replacement.get("details")
                            if isinstance(replacement_details, dict):
                                replacement_details["pre_view_pass"] = summary
                            merged_summaries.append(replacement)
                        merged_summaries.extend(post_view_by_gate.values())
                        consistency_summaries = merged_summaries
                        post_view_mutations = (
                            self._consistency_gate_edge_mutation_counts(
                                post_view_summaries,
                            )
                        )
                        if (
                            post_view_mutations["edges_removed"]
                            or post_view_mutations["nodes_removed"]
                        ):
                            # The gate retracted view output (an orphaned
                            # derived edge, a pruned dangling edge). The
                            # checkpoint's counts and delta must describe the
                            # branch AFTER that removal, so take the documented
                            # full recompute rather than the scoped adjustment
                            # (whose key set does not cover gate-removed rows).
                            invalidate_chronos_scope_cache()
                            branch = store.checkout(self.branch_name)
                            with _publisher_timed_stage(
                                timings_ms,
                                "delta_counts_after_post_view_gate",
                            ):
                                (
                                    delta_counts,
                                    (
                                        nodes_added, nodes_retracted,
                                        nodes_updated, edges_added,
                                        edges_retracted, edges_updated,
                                    ),
                                    (n_nodes, n_edges),
                                ) = recount_after_gate_removal(
                                    branch, delta_counts,
                                )
                            noop = (
                                nodes_added == 0
                                and nodes_retracted == 0
                                and nodes_updated == 0
                                and edges_added == 0
                                and edges_retracted == 0
                                and edges_updated == 0
                            )
                        post_view_blocked_gates = [
                            summary for summary in consistency_summaries
                            if summary.get("blocks_publish")
                        ]
                        if post_view_blocked_gates:
                            phase_b = replace(
                                phase_b,
                                blocked=True,
                                blocked_reason=(
                                    self._merge_consistency_blocked_reason(
                                        phase_b.blocked_reason,
                                        post_view_blocked_gates,
                                    )
                                ),
                            )
                        phase_b = replace(
                            phase_b,
                            consistency_gates=consistency_summaries,
                        )
                        blocked_reason = (
                            phase_b.blocked_reason if phase_b.blocked else None
                        )
                        invariant_summary = _build_invariant_summary(phase_b)
                        _emit_phase_event_with_timing(
                            timings_ms,
                            "post_view_consistency_complete",
                            {
                                "blocked": phase_b.blocked,
                                "blocked_reason": blocked_reason or {},
                                "consistency_gate_count": len(
                                    post_view_summaries
                                ),
                                "duration_ms": int(
                                    (time.monotonic() - cycle_start) * 1000
                                ),
                            },
                        )
            else:
                self._last_view_outcomes = []
                self._last_native_view_refresh = {
                    "status": "skipped",
                    "reason": "publish_blocked",
                    "final_view_refresh": native_view_refresh_final,
                    "force_native_view_refresh": force_native_view_refresh,
                }
                self._last_lens_outcomes = []

            # Record the active Nomos policy pin on the generation. Pre-cutover this
            # was an UPDATE of graph_generations.nomos_policy_{bundle_ids,fingerprints};
            # the chronos publish records it in the generation CHECKPOINT metadata (the
            # generation boundary now).
            if active_nomos_pin is None:
                with _publisher_timed_stage(timings_ms, "nomos_policy_pin"):
                    active_nomos_pin = self._active_nomos_policy_pin(
                        conn,
                        self.deployment_name,
                    )
            nomos_bundle_ids, nomos_fingerprints = active_nomos_pin
            # Mint the new generation's checkpoint id up-front so the effective-attrs
            # materialization below can tag its snapshot with it AND so we can stamp
            # the snapshot's true cardinality into the gate + checkpoint (both read
            # n_nodes/n_edges below).
            generation_id = ChronosGraphStore.new_generation_id(
                deployment_name=self.deployment_name,
                branch=self.branch_name,
            )
            with _publisher_timed_stage(
                timings_ms,
                "attribute_effective_attrs_materialize",
            ):
                from okg.substrate.attributes import (
                    materialize_effective_attributes_for_generation,
                )

                materialize_effective_attributes_for_generation(
                    conn,
                    generation_id=generation_id,
                    previous_generation_id=(
                        previous.checkpoint_id if previous is not None else None
                    ),
                )
            # scale-operator-attr-reads / req.effective-attrs-view (Layer 2): build
            # the INDEXED effective-attrs snapshot the MCP attr operators read. Built
            # here — after every source + enricher-derived + view_cites edge has
            # landed on the branch, and BEFORE the nomos gate + checkpoint — so its
            # row count IS the true published node/edge cardinality. It commits
            # atomically with the generation below (operators never see a published
            # generation without its snapshot); the new generation's checkpoint does
            # not exist yet, so the raw base is the live-branch checkout.
            from okg.substrate.attributes import (
                DirtyTargets,
                materialize_effective_attribute_view_for_generation,
            )

            # ivm-effective-attribute-view / task.plumb-dirty-scope: hand the
            # materializer this cycle's raw changeset so it stages/merges/writes
            # only changed targets (O(changed)) instead of re-deriving the delta
            # from a full chronos-visible scan. The materializer unions this with
            # the per-generation overlay gen-diff (overlay-only changes are not in
            # the chronos scope) and falls back to the full-scan path on a None
            # scope (delta unavailable). OKG_IVM_DIRTY_MATERIALIZE=0 forces the
            # legacy full-scan path (operator kill-switch for the hot path).
            # Computed unconditionally — the interval-view sync below reuses this
            # scope even when an intermediate publish defers its own snapshot.
            _ivm_scope = chronos_dirty_scope() if previous is not None else None
            # If a BLOCKED generation intervened since the last published one, the
            # carry-forward base (the last published snapshot) and the dirty scope
            # (the segment delta since the last CHECKPOINT, i.e. the blocked gen) have
            # mismatched baselines -- an incremental materialize would carry forward
            # the last published snapshot and apply only the post-blocked-gen delta,
            # DROPPING the blocked gen's changes (a wrong snapshot + wrong stamped
            # node/edge count served as truth -> a P0). Force the documented full-scan
            # fallback (dirty_targets=None) for this one cycle so the snapshot rebuilds
            # from the live branch (correct; rare; resumes incremental next cycle).
            _block_intervened = (
                previous is not None
                and previous_checkpoint is not None
                and previous_checkpoint.checkpoint_id != previous.checkpoint_id
            )
            # The bounded graph-count self-heal is also the bounded proof/re-anchor
            # cadence for the effective-attribute read model. A dirty scope can miss
            # a graph mutation path; if the count scan discovers that drift AFTER an
            # incremental snapshot + interval sync, the checkpoint would stamp the
            # corrected graph count beside a stale interval front. Decide the cadence
            # before materialization and force the documented full path on the same
            # generation. This keeps steady state O(changed) while ensuring any
            # dirty-scope omission is repaired within the existing count-heal bound.
            _prev_cadence = int(
                previous_metadata.get("count_stamp_cadence", 0) or 0
            )
            _has_cadence_stamp = "count_stamp_cadence" in previous_metadata
            _self_heal_every = _count_stamp_self_heal_every()
            _integrity_forces_reanchor = (
                not bool(_attribute_integrity_transition.get("eligible"))
                and _attribute_integrity_transition.get("reason")
                in {
                    "prior_state_not_clean",
                    "prior_epoch_mismatch",
                    "transition_cas_failed",
                }
            )
            _count_self_heal = (
                previous is None
                or _block_intervened
                or not _has_cadence_stamp
                or _authorized_count_stamp_recovery is not None
                # A refused incremental publish can commit protected derived
                # writes before it fails its proof. Retrying as another
                # changed-only cycle would inherit a dirty receipt and a stale
                # running-count baseline. Make the immediate retry the recovery
                # boundary: full count, full front, and a fresh anchor.
                or _integrity_forces_reanchor
                or (_prev_cadence + 1) % _self_heal_every == 0
            )
            _dirty_targets = None
            if (
                _ivm_scope is not None
                and not _block_intervened
                and not _count_self_heal
                and os.environ.get("OKG_IVM_DIRTY_MATERIALIZE", "1") != "0"
            ):
                _nodes_by_subtype = _ivm_scope[0] or {}
                _edges_by_type = _ivm_scope[1] or {}
                _dirty_targets = DirtyTargets(
                    node_ids=frozenset(
                        str(nid)
                        for ids in _nodes_by_subtype.values()
                        for nid in ids
                    ),
                    edge_ids=frozenset(
                        str(eid)
                        for ids in _edges_by_type.values()
                        for eid in ids
                    ),
                )

            # Internal dependency-layer (intermediate) publishes are correctness
            # checkpoints; defer their O(live) read-optimized snapshot and let the
            # Layer-1 operator fallback serve reads until the final refresh. Under
            # the per-commit collapse the ingest path only ever issues a final
            # publish, but the direct Publisher.cycle API still drives intermediate
            # publishes (native_view_refresh_final=False); their deferred status is
            # part of that contract and the publish-time tests assert it.
            _did_materialize_effective_view = bool(
                native_view_refresh_final or force_native_view_refresh
            )
            # Computed BEFORE the materialize: the interval sync derives its
            # open intervals FROM the snapshot table, so the empty-overlay
            # fast path (which skips snapshot writes entirely) would starve
            # it into "0 intervals vs full graph" and fail the first-publish
            # reconcile on every fresh database (issue #647,
            # fix-ivm-fresh-db-publish). Under the interval representation
            # per-publish writes are already O(changed) — the property the
            # fast path approximates — so the two are redundant and mutually
            # exclusive by construction.
            # The SAME predicate the materialize below resolves internally and
            # the lock-time check above already used, so the ordinal kill switch
            # (issue #1514) cannot leave the sync block and the snapshot
            # carry-forward disagreeing about which arm owns this publish.
            from okg.substrate.graph_store_internal.generation_ordinal import (
                interval_representation_enabled as _interval_arm,
            )

            _interval_branch = _interval_arm(store.context.db, self.branch_name)
            if _count_self_heal and previous is not None:
                # A full-proof cycle (the count-stamp cadence, or the retry
                # after a refused publish, whose integrity state is dirty)
                # re-derives the whole read model from the branch below and
                # recounts the whole graph after it. Both describe the state
                # fingerprinted here; anything written later refuses the cut.
                segment_signature_counted = self._chronos_segment_signature_now(
                    conn,
                )
            # An incremental cycle takes the branch write lock here, before
            # its first write to the protected attribute tables, and holds it
            # through the commit. A write that raced the counts is refused
            # now, while the integrity state is still clean, so the retry
            # stays incremental; a refusal after the materialize would dirty
            # it and turn the retry into a full rebuild. Session locks nest on
            # this connection, so a caller already holding the lock (guarded
            # redrive, inline publish) re-enters. Full cycles keep the check
            # at the cut: their retry is a full rebuild either way.
            protected_write_fence = guarded_branch_writes.enter_context(
                ExitStack()
            )
            if (
                previous is not None
                and not _count_self_heal
                and self._chronos_fence_protected_writes(
                    store, conn, segment_signature_counted,
                )
            ):
                protected_write_fence.callback(
                    store._unlock_branch_write_session, self.branch_name,
                )
            if _did_materialize_effective_view:
                with _publisher_timed_stage(
                    timings_ms,
                    "attribute_effective_view_materialize",
                ):
                    effective_view_result = (
                        materialize_effective_attribute_view_for_generation(
                            conn,
                            generation_id=generation_id,
                            branch=self.branch_name,
                            previous_generation_id=(
                                previous.checkpoint_id
                                if previous is not None else None
                            ),
                            empty_overlay_fast_path=(
                                self.attribute_effective_view_empty_overlay_fast_path
                                and not _interval_branch
                            ),
                            dirty_targets=_dirty_targets,
                            graph_store=store,
                            prelock_hash_cache=prelock_hash_cache,
                        )
                    )
                attribute_effective_view_refresh = {
                    "status": (
                        "materialized"
                        if bool(effective_view_result.get("materialized"))
                        else "not_materialized"
                    ),
                    "final_view_refresh": native_view_refresh_final,
                    "force_native_view_refresh": force_native_view_refresh,
                    **effective_view_result,
                }
                if prelock_hash_report:
                    attribute_effective_view_refresh["prelock_hash_pass"] = dict(
                        prelock_hash_report
                    )
            else:
                # Deferred: no snapshot for this generation. Cardinality below falls
                # back to a direct count (effective_view_result is falsy), and the
                # interval-view sync is skipped (nothing to fold).
                effective_view_result = {}
                attribute_effective_view_refresh = {
                    "status": "deferred",
                    "reason": "intermediate_publish",
                    "generation_id": generation_id,
                    "final_view_refresh": native_view_refresh_final,
                    "force_native_view_refresh": force_native_view_refresh,
                }
            # node_count / edge_count are the TRUE published-graph totals — the
            # materialized snapshot's own cardinality (carried-forward + merged),
            # free from the scan the materialize already did. NOT prev+delta, which
            # compounds drift across generations with no self-heal and silently
            # undercounts the graph (~28% / ~17% low on okg-workspace), making every
            # status/map read display a wrong total. By construction this equals the
            # count(*) the read path runs over the same snapshot.
            # (accelerate-operator-read-path: req.totals-from-precomputed-metadata)
            # The interval path (running counter, sync, reconcile) OWNS this publish
            # only when the flag is on AND this is an interval-tracked (ROOT) branch.
            # A STRUCTURAL fork keeps the full-snapshot path (flag-off-like): correct,
            # since it has no ordinal and reads fall back to its full snapshot.
            # (_interval_branch is hoisted above the materialize so the
            # empty-overlay fast path is disabled under the interval arm.)
            if _interval_branch:
                # ivm-effective-attribute-view / task.cardinality-counter: the full
                # snapshot row count is retired as the cardinality source (its O(total)
                # carry-forward write is dropped). Maintain the published count as a
                # RUNNING COUNTER -- advance the prior published count by THIS cycle's
                # delta (O(1)), not a full scan. This is NOT "blind prev+delta" (the
                # drift F1 fixed): the periodic post-sync reconcile re-derives the full
                # graph count and FAILS LOUD on any divergence, and the cardinality
                # oracle asserts counter == full count across insert/update/retract.
                # The first publish (no previous) seeds from the full count.
                _delta = locals().get("delta_counts")
                _prev = locals().get("previous_counts")
                if previous is not None and _delta is not None:
                    n_nodes, n_edges = _counts_after_delta(
                        _prev, _delta, branch=branch
                    )
                else:
                    n_nodes, n_edges = _counted_graph(branch)
            elif effective_view_result.get("materialized"):
                # The effective-attribute-view snapshot row count (node_rows) is
                # NOT the live graph node count: it omits nodes with no effective
                # attributes (on okg-workspace ~28% fewer rows -> the stamped count
                # under-reported the graph). Maintain the count as a running counter
                # (prior published count + this cycle's delta), re-anchored by the
                # periodic full-scan self-heal below; first publish / missing delta
                # falls back to a full scan. (Mirrors the interval-view branch.)
                _eff_delta = locals().get("delta_counts")
                if previous is not None and _eff_delta is not None:
                    n_nodes, n_edges = _counts_after_delta(
                        previous_counts, _eff_delta, branch=branch
                    )
                else:
                    n_nodes, n_edges = _counted_graph(branch)
            else:
                # Snapshot already existed for this generation (idempotent re-run —
                # not expected on a fresh publish). Fall back to a direct count so a
                # stale/zero is NEVER stamped (the read path serves this verbatim, so
                # a wrong number here is a P0), never a default.
                n_nodes, n_edges = _counted_graph(branch)
            # Maintain the ontology-graph rollup (#304 running sum) BEFORE the
            # checkpoint is cut, after every cycle edge (source + enricher +
            # view_cites) has landed, so the checkpoint pins a rollup consistent
            # with the graph. Only for a generation that will PUBLISH; a None dirty
            # scope forces a full recount (loud), never a stale carry-forward.
            # Maintain the registered pre-checkpoint derivations -- the ontology rollup
            # (#304) and the console count rollup (#444 slice 2) -- in ONE iterated loop
            # over the maintained-derivation registry (#444 slice 3), replacing the two
            # hand-wired stages. Each row runs in its own timing stage (the bench reads
            # the keys back); enablement stays a PUBLISHER decision (the ontology rollup
            # is maintained on every deployment since #1843; the console rollup keys on
            # the OKG_IVM_CONSOLE_COUNT_ROLLUP flag), so only the publisher's enabled
            # rows are driven. A None dirty scope OR a blocked-gen intervention forces a
            # full recount (loud), never a stale carry-forward. The console summary is
            # persisted into checkpoint metadata (the marginal-cost bench reads it back).
            console_count_rollup_summary: dict[str, Any] | None = None
            ontology_rollup_summary: dict[str, Any] | None = None
            deployment_derivation_summaries: dict[str, dict[str, Any]] = {}
            if blocked_reason is None:
                from okg.substrate.derived_artifacts.derivation_registry import (
                    ExecutionPhase,
                    maintain_one_row,
                )
                from okg.substrate.publisher.console_count_rollup import (
                    console_count_rollup_row,
                )
                from okg.substrate.publisher.ontology_rollup import (
                    ontology_rollup_row,
                )

                prior_checkpoint = (
                    str(getattr(previous, "checkpoint_id", "") or "") or None
                )
                rollup_dirty_scope = chronos_dirty_scope()
                # The ontology rollup is maintained on EVERY deployment (#1843).
                # It was opt-in, keyed on a registered view whose id ends in
                # `ontology_graph_rollup` — but Health READS the rollup on every
                # deployment, only okg-workspace registered that view, and the
                # module it names is deployment-local Python, so every other
                # deployment published generations the Graph-ontology read then
                # refused with `no_sentinel`. Nothing was left for the switch to
                # select: the hook's full recount joins only
                # okg.graph_edges/graph_nodes, it reads the Chronos dirty scope
                # rather than the view's `reads_edge_types`, and its table is
                # ensured everywhere. Deliberately NOT an OKG_IVM_* flag like the
                # count rollup: a disabled window carries the sentinel forward, so
                # re-enabling would resume incremental folds on a stale base and
                # permanently under-count — the count rollup pays for its flag
                # with the rebuild guard below, and a second flag would need a
                # second guard -- so this change gives the ontology rollup the
                # SAME guard (the receipt below), rather than shipping an
                # unguarded one to every deployment. The row is NON_GATING, so a
                # maintain failure never reds a publish; the receipt is what makes
                # that failure visible to the NEXT cycle.
                enabled_rollup_rows = [ontology_rollup_row()]
                if self._console_count_rollup_enabled():
                    enabled_rollup_rows.append(console_count_rollup_row())
                deployment_rows = [
                    row for row in self._deployment_maintained_derivation_rows()
                    if row.execution_phase is ExecutionPhase.PRE_CHECKPOINT
                ]
                deployment_row_ids = {
                    row.spec.derivation_id for row in deployment_rows
                }
                changed_node_ids, changed_edge_ids = (
                    self._changed_ids_from_chronos_dirty_scope(rollup_dirty_scope)
                )
                # Checkpoint-receipt key per maintained rollup (derivation id ->
                # metadata key). The ontology rollup's differ because its timing
                # stage and table are named `ontology_rollup` while its derivation
                # id is `ontology_graph_rollup`.
                rollup_receipt_keys = {
                    "console_count_rollup": "console_count_rollup",
                    "ontology_graph_rollup": "ontology_rollup",
                }
                for rollup_row in [*enabled_rollup_rows, *deployment_rows]:
                    # A carried Chronos sentinel does not prove uninterrupted
                    # maintenance: a cycle that was disabled, OR whose maintain
                    # FAILED (these rows are NON_GATING, so the failure is
                    # swallowed), carries the table forward without writing this
                    # derivation's checkpoint receipt. The read then finds the
                    # PRIOR generation's sentinel and serves stale counts, and the
                    # next cycle folds incrementally over its own dirty scope
                    # alone -- permanently dropping the skipped cycle's deltas
                    # until the periodic self-heal. The receipt is the proof of
                    # maintenance: absent from the previous generation's metadata,
                    # rebuild from the current graph before resuming incremental
                    # folds.
                    receipt_key = rollup_receipt_keys.get(
                        rollup_row.spec.derivation_id
                    )
                    rollup_force_full = _block_intervened or (
                        receipt_key is not None
                        and previous is not None
                        and receipt_key not in previous_metadata
                    )
                    with _publisher_timed_stage(timings_ms, rollup_row.timing_stage):
                        rollup_summary = maintain_one_row(
                            conn,
                            rollup_row,
                            branch=self.branch_name,
                            generation_id=gate_target_id,
                            prior_generation_checkpoint=prior_checkpoint,
                            dirty_scope=rollup_dirty_scope,
                            changed_node_ids=changed_node_ids,
                            changed_edge_ids=changed_edge_ids,
                            force_full=rollup_force_full,
                            graph_store=store,
                        )
                        maintained = rollup_summary is None or (
                            rollup_summary.get("status") != "unavailable"
                        )
                        if receipt_key == "console_count_rollup" and maintained:
                            console_count_rollup_summary = rollup_summary
                        if receipt_key == "ontology_rollup" and maintained:
                            ontology_rollup_summary = rollup_summary
                        if rollup_row.spec.derivation_id in deployment_row_ids:
                            if (
                                rollup_summary is None
                                or rollup_summary.get("status") == "unavailable"
                            ):
                                deployment_derivation_summaries[
                                    rollup_row.spec.derivation_id
                                ] = {
                                    "status": "unavailable",
                                    "fallback_reason": (
                                        (rollup_summary or {}).get("reason")
                                        or "maintain_failed"
                                    ),
                                    "error": (
                                        (rollup_summary or {}).get("error")
                                    ),
                                }
                            else:
                                deployment_derivation_summaries[
                                    rollup_row.spec.derivation_id
                                ] = {
                                    "status": "maintained",
                                    "mode": rollup_summary.get("mode"),
                                    "forcing_reason": rollup_summary.get("reason"),
                                    "fallback_reason": rollup_summary.get("reason"),
                                    # The recorded cause of ANY full recompute
                                    # (kit-dispatched OR a callback fallback under
                                    # incremental dispatch); present iff this run
                                    # recomputed in full, None for a genuine
                                    # bucket-scoped incremental (spec §9.8).
                                    "full_reason": rollup_summary.get("full_reason"),
                                    "dirty_bucket_count": rollup_summary.get(
                                        "dirty_bucket_count"
                                    ),
                                    "rows_written": rollup_summary.get("rows_written"),
                                    "rows_deleted": rollup_summary.get("rows_deleted"),
                                }
            projection_deltas = {
                "nodes_added": nodes_added,
                "nodes_retracted": nodes_retracted,
                "nodes_updated": nodes_updated,
                "edges_added": edges_added,
                "edges_retracted": edges_retracted,
                "edges_updated": edges_updated,
                "noop": noop,
            }
            enricher_governance, projection_deltas = (
                self._enricher_checkpoint_receipts(
                    enricher_runs=enricher_runs,
                    projection_deltas=projection_deltas,
                    projection_delta_hints=self.enricher_projection_deltas_hint,
                )
            )
            # Consistency gates run before the checkpoint and may mutate the branch.
            # A prune-policy gate can remove part of what producers wrote before it
            # becomes checkpoint-visible. The six change-receipt counts describe the
            # post-gate graph; `*_projected_net` keeps the producers' raw write
            # (checkpoint-visible net plus gate removals). Preserve both truths
            # explicitly and immediately re-count a mutated branch: deferring correction to the
            # periodic count-stamp self-heal publishes a count for a graph that never
            # existed (close-publish-prune-count-gap).
            _gate_edge_mutations = self._consistency_gate_edge_mutation_counts(
                consistency_summaries,
                include_pre_view_pass=True,
            )
            # Both gate passes recount the delta after they remove rows (#2305),
            # so the delta already describes the checkpoint-visible branch. The
            # producers' raw write is that plus what the gates removed.
            _checkpoint_visible_node_net = (
                int(projection_deltas.get("nodes_added", 0) or 0)
                - int(projection_deltas.get("nodes_retracted", 0) or 0)
            )
            _checkpoint_visible_edge_net = (
                int(projection_deltas.get("edges_added", 0) or 0)
                - int(projection_deltas.get("edges_retracted", 0) or 0)
            )
            _projected_edge_net = (
                _checkpoint_visible_edge_net
                + _gate_edge_mutations["edges_removed"]
            )
            _projected_node_net = (
                _checkpoint_visible_node_net
                + _gate_edge_mutations["nodes_removed"]
            )
            # A gate-only mutation is observable work even when producers emitted
            # no delta. Keep the public no-op signal consistent with the final graph.
            noop = noop and not (
                _gate_edge_mutations["edges_removed"]
                or _gate_edge_mutations["nodes_removed"]
            )
            projection_deltas = {
                **dict(projection_deltas),
                "noop": noop,
                "nodes_projected_net": _projected_node_net,
                "nodes_checkpoint_visible_net": _checkpoint_visible_node_net,
                "edges_projected_net": _projected_edge_net,
                "edges_checkpoint_visible_net": _checkpoint_visible_edge_net,
                "consistency_gate_mutations": _gate_edge_mutations,
            }
            if (
                _gate_edge_mutations["edges_removed"]
                or _gate_edge_mutations["nodes_removed"]
            ):
                with _publisher_timed_stage(
                    timings_ms,
                    "count_stamp_post_consistency_gate_full_scan",
                ):
                    n_nodes, n_edges = _counted_graph(branch)
            # Count-stamp self-heal (fix-checkpoint-graph-count-stamp): the stamped
            # node_count/edge_count is a RUNNING COUNTER (previous checkpoint stamp +
            # this cycle's delta, _chronos_counts_after_delta). A baseline seeded too
            # low (e.g. from the effective-attribute-view materialize cardinality) or
            # a delta that missed a graph-growth path drifts forever, since the only
            # full-scan re-baseline fires on first publish or after a blocked gen.
            # Re-baseline from a full scan of the just-published branch on a bounded
            # cadence -- first publish / blocked-gen / a legacy checkpoint with no
            # cadence stamp (corrects existing drift on the first post-deploy publish)
            # / every Nth published generation -- so drift is bounded to N gens while
            # the cheap delta path is kept between self-heals.
            _recovery_authorization: dict[str, Any] | None = None
            _count_stamp_recovery: dict[str, Any] | None = None
            if _authorized_count_stamp_recovery is not None:
                _recovery_authorization = _authorized_count_stamp_recovery
                assert _authorized_previous_recorded_counts is not None
                assert _authorized_previous_pinned_counts is not None
                _previous_published_counts = _authorized_previous_recorded_counts
                _previous_pinned_counts = _authorized_previous_pinned_counts
                _count_stamp_recovery = {
                    "schema": "okg.count_stamp_recovery.v1",
                    "reason": "predecessor_checkpoint_count_mismatch",
                    "recovery_of": str(previous.checkpoint_id),
                    "authorization_digest": _recovery_authorization[
                        "authorization_digest"
                    ],
                    "prepared_binding_digest": _recovery_authorization[
                        "prepared_binding_digest"
                    ],
                    "classification_binding_digest": _recovery_authorization[
                        "classification_binding_digest"
                    ],
                    "recorded": {
                        "node_count": int(_previous_published_counts[0]),
                        "edge_count": int(_previous_published_counts[1]),
                    },
                    "pinned": {
                        "node_count": int(_previous_pinned_counts[0]),
                        "edge_count": int(_previous_pinned_counts[1]),
                    },
                }
            # `previous is None` already full-scanned n_nodes/n_edges above; only the
            # carried-delta paths need the corrective re-scan.
            if _count_self_heal and previous is not None:
                with _publisher_timed_stage(
                    timings_ms, "count_stamp_self_heal_full_scan"
                ):
                    n_nodes, n_edges = self._chronos_current_graph_counts(branch)
            _count_stamp_cadence = 0 if _count_self_heal else _prev_cadence + 1

            if blocked_reason is None:
                with _publisher_timed_stage(
                    timings_ms,
                    "nomos_generation_publish",
                ):
                    blocked_reason = self._nomos_generation_publish_blocker(
                        conn,
                        target_generation_id=gate_target_id,
                        catalog_version_id=int(cv),
                        node_count=n_nodes,
                        edge_count=n_edges,
                        projection_deltas=projection_deltas,
                        nomos_bundle_ids=nomos_bundle_ids,
                        nomos_fingerprints=nomos_fingerprints,
                        commit=caller_verified_transaction is None,
                    )
            try:
                graph_effect_manifest = self._chronos_graph_effect_manifest(
                    conn,
                    branch_name=self.branch_name,
                    previous=previous,
                )
            except Exception as exc:  # telemetry coverage must be loud, not fatal.
                logging.getLogger(__name__).exception(
                    "Chronos graph-effect identity capture failed"
                )
                graph_effect_manifest = {
                    "schema": "okg.graph_effect.v1",
                    "coverage": "aggregate_only",
                    "reason": f"identity_capture_failed:{type(exc).__name__}",
                    "items": [],
                    "returned": 0,
                    "limit": 100,
                }
            graph_effect_manifest.update({
                "input_generation_id": (
                    str(getattr(previous, "checkpoint_id", "") or "") or None
                ),
                "output_generation_id": generation_id,
                "counts": {
                    "nodes_added": nodes_added,
                    "nodes_updated": nodes_updated,
                    "nodes_retracted": nodes_retracted,
                    "edges_added": edges_added,
                    "edges_updated": edges_updated,
                    "edges_retracted": edges_retracted,
                },
            })
            effect_total = sum(
                max(0, int(value or 0))
                for value in graph_effect_manifest["counts"].values()
            )
            if graph_effect_manifest.get("coverage") in {"exact", "sampled"}:
                retained = len(graph_effect_manifest.get("items") or [])
                graph_effect_manifest["coverage"] = (
                    "exact" if retained >= effect_total else "sampled"
                )
                graph_effect_manifest["reason"] = (
                    None if retained >= effect_total else "identity_limit_reached"
                )
            if first_publish_budget_warning is None:
                first_publish_budget_warning = self._initial_generation_budget_warning(
                    first_publish=False,
                    node_count=n_nodes,
                    edge_count=n_edges,
                )
            publisher_configuration = self._publisher_configuration_receipt()
            checkpoint_metadata = {
                "publisher": "chronos_native",
                "repository_provenance": self.repository_provenance.to_dict(),
                **({"packaged_release": self.packaged_release.to_dict()}
                   if self.packaged_release is not None else {}),
                # Bind the generation to the exact published predecessor selected
                # while the publisher lock is held. Callers that validate
                # incremental-only effects must not infer this from a later
                # "latest generation" lookup, which can race another publisher.
                "predecessor_generation_id": (
                    str(previous.checkpoint_id)
                    if previous is not None
                    else None
                ),
                "node_count": n_nodes,
                "edge_count": n_edges,
                "publisher_configuration": publisher_configuration,
                "first_publish_budget_warning": first_publish_budget_warning,
                "count_stamp_cadence": _count_stamp_cadence,
                "target_generation_id": gate_target_id,
                # Reserved before native views run and handed to every corpus
                # builder / SQL-relation template that explicitly accepts it.
                # Keep it separate from the checkpoint row's creation time;
                # post-publish readers prefer this authored value and fall back
                # to the row timestamp for older checkpoints.
                "published_at": generation_published_at.isoformat(),
                "invariant_summary": invariant_summary,
                # The gate's exclusion verdict, durable beside the summary that
                # explains it (issue #1427). Readers key on this list -- an
                # empty one here is a claim that every source in the checkpoint
                # passed its own invariants.
                "excluded_sources": gate_excluded_sources,
                "blocked_reason": blocked_reason or {},
                "nomos_policy_bundle_ids": nomos_bundle_ids,
                "nomos_policy_fingerprints": nomos_fingerprints,
                # Delta-aware publish evidence (deltaflow-publish-projection /
                # req.projection-deltas): the per-generation "change receipt".
                # The publisher already computes these upsert/retraction/update
                # row deltas for the cycle; persist them into the checkpoint so
                # `okg generation describe` can explain what this generation
                # changed (and that a no-op published nothing) without
                # re-deriving the deltas from the interval store.
                "projection_deltas": projection_deltas,
                "enricher_governance": enricher_governance,
                "derived_fact_summary": dict(derived_fact_summary),
            }
            if generation_ingest_completeness is not None:
                checkpoint_metadata[published_completeness.METADATA_KEY] = (
                    generation_ingest_completeness
                )
            if _count_stamp_recovery is not None:
                checkpoint_metadata["count_stamp_recovery"] = _count_stamp_recovery
            checkpoint_metadata["dirty_scope_hint"] = dirty_scope_hint_decision
            if extractor_input_signature is not None:
                checkpoint_metadata["extractor_input_signature"] = (
                    extractor_input_signature
                )
            # Persist the console count rollup's maintain summary so the read path's
            # marginal-cost bench (maintained_derivation_scope) can read back the
            # mode / forcing reason / rows-touched for THIS generation without
            # re-deriving them. Only present when the rollup is enabled + maintained.
            if console_count_rollup_summary is not None:
                checkpoint_metadata["console_count_rollup"] = {
                    "mode": console_count_rollup_summary.get("mode"),
                    "forcing_reason": console_count_rollup_summary.get("reason"),
                    "dirty_bucket_count": console_count_rollup_summary.get(
                        "dirty_bucket_count"
                    ),
                    "rows_written": console_count_rollup_summary.get("rows_written"),
                    "rows_deleted": console_count_rollup_summary.get("rows_deleted"),
                }
            # The ontology rollup's receipt: written only when this cycle actually
            # maintained it, so its ABSENCE at generation N tells generation N+1 to
            # rebuild rather than fold incrementally onto a gap (#1843).
            if ontology_rollup_summary is not None:
                checkpoint_metadata["ontology_rollup"] = {
                    "mode": ontology_rollup_summary.get("mode"),
                    "forcing_reason": ontology_rollup_summary.get("reason"),
                    "dirty_bucket_count": ontology_rollup_summary.get(
                        "dirty_bucket_count"
                    ),
                    "rows_written": ontology_rollup_summary.get("rows_written"),
                    "rows_deleted": ontology_rollup_summary.get("rows_deleted"),
                }
            if deployment_derivation_summaries:
                checkpoint_metadata["maintained_derivations"] = (
                    deployment_derivation_summaries
                )
            # (generation id minted + effective-attrs/-view materialized above,
            # before the gate, so node_count/edge_count are the snapshot's true
            # cardinality.)
            checkpoint_status = "blocked" if blocked_reason else "published"
            # Attestation must decide BEFORE the interval branch below assigns
            # a generation ordinal (issue #1428). The ordinal sequence is
            # reserved for PUBLISHED generations; assigning one first and then
            # refusing left the blocked checkpoint holding a published ordinal
            # that nothing removed, so the next successful publish skipped that
            # slot. Attestors read only the candidate session's graph rows and
            # source watermarks -- never the interval tables the
            # block below writes -- so deciding here sees the same evidence.
            # The guard stays keyed on checkpoint_status, NOT on
            # `blocked_reason is None`: a falsy-but-not-None blocker (an empty
            # dict) would skip the attestors and still publish the checkpoint,
            # which is attestation fail-open -- the exact thing this subsystem
            # exists to prevent.
            deployment_attestations: dict[str, Any] | None = None
            attestation_failure: Any | None = None
            if checkpoint_status == "published" and self.checkpoint_attestors:
                from okg.substrate.publisher.checkpoint_attestations import (
                    CheckpointAttestationFailure,
                    checkpoint_attestation_context,
                    run_checkpoint_attestors,
                )

                with _publisher_timed_stage(
                    timings_ms,
                    "checkpoint_attestations",
                ):
                    with checkpoint_attestation_context(
                        conn=conn,
                        candidate_session=branch,
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                        native_generation_id=str(generation_id),
                        target_generation_token=int(gate_target_id),
                        catalog_version_id=int(cv),
                        node_count=int(n_nodes),
                        edge_count=int(n_edges),
                    ) as context:
                        try:
                            deployment_attestations = run_checkpoint_attestors(
                                self.checkpoint_attestors,
                                context=context,
                            )
                        except CheckpointAttestationFailure as exc:
                            attestation_failure = exc
                            blocked_reason = exc.blocked_reason()
                            checkpoint_status = "blocked"
                            checkpoint_metadata["blocked_reason"] = blocked_reason
                        else:
                            checkpoint_metadata["deployment_attestations"] = (
                                deployment_attestations
                            )
            _attribute_integrity_preflight: dict[str, Any] = {
                "advanced": False,
                "reason": "interval_sync_not_run",
            }
            # Ordinal this cycle protected from the retention prune, if any.
            # None means the incremental receipt must prove the old side of its
            # delta is intact before trusting it (okg#1603).
            _ivm_protect_ord: int | None = None
            # Durable, POSITIVE record of whether this generation folded itself
            # into the interval table. The NEXT publish's backfill precondition
            # reads it: an intermediate publish deliberately defers its
            # effective-attribute snapshot and therefore folds nothing, so its
            # checkpoint counts intentionally run ahead of the interval rows and
            # comparing the two refuses a healthy deployment (issue #1927). The
            # marker is written on EVERY cycle, so its ABSENCE means "published
            # before this marker existed" and keeps the old fail-closed path.
            from okg.substrate.attributes import (
                DEFERRED_INTERVAL_SYNC_REASON,
            )

            _interval_sync_marker: dict[str, Any]
            if blocked_reason is not None:
                _interval_sync_marker = {"ran": False, "reason": "blocked"}
            elif not _interval_branch:
                _interval_sync_marker = {
                    "ran": False,
                    "reason": "interval_representation_not_enabled",
                }
            elif not _did_materialize_effective_view:
                _interval_sync_marker = {
                    "ran": False,
                    "reason": DEFERRED_INTERVAL_SYNC_REASON,
                }
            else:
                _interval_sync_marker = {"ran": True, "reason": None}
            checkpoint_metadata["effective_interval_view_sync"] = (
                _interval_sync_marker
            )
            # The interval table is part of the published read contract. Prepare it
            # BEFORE exposing the Chronos checkpoint: the old ordering checkpointed
            # first, so a timeout left a published generation with no interval rows.
            #
            # ONE publication transaction (issue #1587). The ordinal, the interval
            # sync, the retention prune, the integrity preflight and the checkpoint
            # itself all commit together or not at all. Each used to commit on its
            # own, so a failure after the ordinal committed -- in the prune, the
            # integrity write, or checkpoint_generation -- left an ordinal with no
            # generation behind it, and the next publish skipped that position.
            # The inner `conn.transaction()` blocks below are now savepoints.
            # A cycle that neither enters the interval arm nor recovers a count
            # stamp opens no transaction here and checkpoints exactly as before.
            #
            # Under a caller-verified publish (#2268) the caller already owns
            # the one transaction for the whole cycle, with Chronos autocommit
            # off, and its verify_and_commit()/rollback() runs the Chronos
            # finalize hook after the real COMMIT or ROLLBACK. The publication
            # transaction then JOINS that transaction: nothing is opened,
            # committed, rolled back or finalized here, and a failure
            # propagates to the caller's rollback, which discards the ordinal,
            # interval rows, prune, preflight and checkpoint together.
            def _finalize_publication_transaction(
                exc_type: Any, _exc: Any, _tb: Any
            ) -> bool:
                store.context.finalize_external_transaction(
                    committed=exc_type is None
                )
                return False

            def _open_publication_transaction(scope: ExitStack) -> None:
                if caller_verified_transaction is not None:
                    caller_verified_transaction.require_active(
                        stage="publication_transaction"
                    )
                    return
                # Exit runs in reverse: the transaction ends (COMMIT or
                # ROLLBACK), then the Chronos runtime gets its own commit
                # control back, then its after-commit / after-rollback hook
                # runs. While the transaction is open the runtime must not
                # commit or roll back on its own: a Chronos failure would
                # otherwise call rollback() inside the transaction, and psycopg
                # refuses that with an error that hides the real failure.
                scope.push(_finalize_publication_transaction)
                chronos_autocommit = store.context.autocommit
                scope.callback(
                    setattr, store.context, "autocommit", chronos_autocommit
                )
                store.context.autocommit = False
                scope.enter_context(conn.transaction())

            _publication_transaction_open = False
            write_race_checked = False
            with ExitStack() as _publication_scope:
                if (
                    blocked_reason is None
                    and _interval_branch
                    and _did_materialize_effective_view
                ):
                    with _publisher_timed_stage(
                        timings_ms, "effective_interval_view_sync"
                    ):
                        from okg.substrate.attributes import (
                            reanchor_effective_interval_view_for_generation,
                            sync_effective_interval_view_for_generation,
                        )
                        from okg.substrate.graph_store_internal.generation_ordinal import (
                            assign_generation_ordinal,
                        )

                        with conn.cursor() as ordinal_cur:
                            ordinal_cur.execute(
                                "SELECT COALESCE(max(ordinal) + 1, 0) "
                                "FROM okg.generation_ordinals WHERE branch_id = %s",
                                (self.branch_name,),
                            )
                            expected_ordinal = int(ordinal_cur.fetchone()[0])
                        reconcile_every = int(
                            os.environ.get("OKG_IVM_INTERVAL_RECONCILE_EVERY", "100")
                            or "100"
                        )
                        # Read BEFORE the publication transaction opens: this
                        # checks out a Chronos branch, which commits on its own.
                        reconcile_full_counts = (
                            self._chronos_current_graph_counts(branch)
                            if reconcile_every > 0
                            and expected_ordinal % reconcile_every == 0
                            else None
                        )
                        _open_publication_transaction(_publication_scope)
                        _publication_transaction_open = True
                        with conn.transaction():
                            assigned_ordinal = assign_generation_ordinal(
                                store.context.db,
                                store.context,
                                checkpoint_id=generation_id,
                                branch=self.branch_name,
                                deployment_name=self.deployment_name,
                            )
                            if assigned_ordinal != expected_ordinal:
                                raise SubstrateError(
                                    "generation ordinal changed under the publisher lock",
                                    data={
                                        "branch": self.branch_name,
                                        "expected": expected_ordinal,
                                        "assigned": assigned_ordinal,
                                    },
                                )
                            if _count_self_heal:
                                reanchor_effective_interval_view_for_generation(
                                    conn,
                                    store.context.db,
                                    generation_id=generation_id,
                                )
                            else:
                                sync_effective_interval_view_for_generation(
                                    conn,
                                    store.context.db,
                                    generation_id=generation_id,
                                    # Delta mode uses the same changeset as the
                                    # materializer. None means a non-cadence full
                                    # fallback with payload-preserving interval
                                    # reconciliation.
                                    dirty_targets=_dirty_targets,
                                )
                            self._reconcile_effective_interval_view(
                                conn,
                                generation_id=generation_id,
                                node_count=n_nodes,
                                edge_count=n_edges,
                                full_counts=reconcile_full_counts,
                            )
                        # Bound the interval sidecar (PACT bound-okg-workspace-disk-growth,
                        # req.bound-disk.intervals-sidecar): prune CLOSED intervals that
                        # fell out of the retention window, CAPPED per cycle so the publish
                        # path never issues an unbounded DELETE over the backlog. Runs in
                        # its own savepoint inside the publication transaction; a prune
                        # failure fails the publish and rolls back the ordinal and the
                        # sync with it (issue #1587) rather than leaving a synced front
                        # at an ordinal no generation will ever own. LOUD: the
                        # per-cycle delete count is stamped into the checkpoint metadata.
                        # Deliberately NOT gated on the host-headroom blocker: this is a
                        # bounded reclaimer (<= EFFECTIVE_INTERVAL_PRUNE_BATCH rows, so its
                        # WAL is bounded) whose net effect is to RELEASE space. Refusing to
                        # prune under disk pressure would be backwards — it would let the
                        # backlog the sentinel is warning about keep growing. The sentinel
                        # gates unbounded WRITERS (backups, scratch), not bounded reclaimers.
                        if self.history_retention_policy is None:
                            from okg.substrate.attributes import (
                                prune_effective_interval_view_closed,
                            )
                            from okg.substrate.attribute_integrity import (
                                latest_receipt_ordinal,
                            )

                            # The next incremental receipt subtracts the interval
                            # STAB at the latest receipt's ordinal. Retention 1
                            # prunes exactly that stab on every publish, leaving the
                            # delta reading an empty old side -- the count then
                            # disagrees with the checkpoint and the publish fails
                            # even though the generation is sound (okg#1603).
                            _ivm_protect_request = latest_receipt_ordinal(
                                conn,
                                deployment_name=self.deployment_name,
                                branch=self.branch_name,
                            )
                            # Bound the protection: a receipt chain that has stopped
                            # advancing must not pin the closed-interval sidecar
                            # open forever (PACT bound-okg-workspace-disk-growth).
                            # Past the bound the receipt refuses and re-anchors.
                            if (
                                _ivm_protect_request is not None
                                and assigned_ordinal - _ivm_protect_request
                                > _MAX_PROTECTED_RECEIPT_LAG
                            ):
                                _ivm_protect_request = None
                            with _publisher_timed_stage(
                                timings_ms, "effective_interval_view_prune"
                            ), conn.transaction():
                                _ivm_prune = prune_effective_interval_view_closed(
                                    conn,
                                    branch=self.branch_name,
                                    protect_from_ord=_ivm_protect_request,
                                )
                            # What the prune REPORTS it protected -- never what this
                            # cycle asked for. The receipt skips its intactness
                            # probe only on protection that actually ran.
                            _ivm_protect_ord = _ivm_prune.get("protect_from_ord")
                        else:
                            # A declared classifier-specific policy makes the
                            # operator's confirmed compaction plan the sole deletion
                            # authority. The legacy two-generation publisher prune
                            # cannot distinguish full/horizon/latest-only classifiers,
                            # so running it here would silently defeat `full`.
                            _ivm_prune = {
                                "deleted": 0,
                                "capped": False,
                                "branch": self.branch_name,
                                "skipped": "explicit_history_retention_policy",
                            }
                        checkpoint_metadata["effective_interval_view_prune"] = _ivm_prune
                    # Check for writes since the counts BEFORE the attribute
                    # integrity proof below, which would otherwise refuse a
                    # racing write with a count or payload mismatch instead
                    # of naming it. The branch write lock taken here is held
                    # through the checkpoint cut and the commit.
                    if checkpoint_status == "published" and previous is not None:
                        self._chronos_refuse_writes_since_counted(
                            conn, segment_signature_counted,
                        )
                        write_race_checked = True
                    from okg.substrate.attribute_integrity import (
                        finalize_full_attribute_integrity_anchor,
                        finalize_incremental_attribute_integrity_receipt,
                    )

                    # Issue #2941: the receipt passes were the largest
                    # unlabeled part of the lock hold.
                    with _publisher_timed_stage(
                        timings_ms, "attribute_integrity_receipt_preflight",
                    ):
                        if _dirty_targets is None:
                            with conn.transaction():
                                _attribute_integrity_preflight = (
                                    finalize_full_attribute_integrity_anchor(
                                        conn,
                                        deployment_name=self.deployment_name,
                                        branch=self.branch_name,
                                        namespace="default",
                                        generation_id=str(generation_id),
                                        ordinal=assigned_ordinal,
                                        checkpoint_counts={
                                            "node": int(n_nodes),
                                            "edge": int(n_edges),
                                        },
                                        # Prove exact full parity before the generation
                                        # becomes visible. The post-checkpoint pass below
                                        # repeats the proof and is the only clean marker.
                                        persist=False,
                                    )
                                )
                        else:
                            with conn.transaction():
                                _attribute_integrity_preflight = (
                                    finalize_incremental_attribute_integrity_receipt(
                                        conn,
                                        deployment_name=self.deployment_name,
                                        branch=self.branch_name,
                                        namespace="default",
                                        generation_id=str(generation_id),
                                        previous_generation_id=(
                                            str(previous.checkpoint_id)
                                            if previous is not None
                                            else None
                                        ),
                                        ordinal=assigned_ordinal,
                                        checkpoint_counts={
                                            "node": int(n_nodes),
                                            "edge": int(n_edges),
                                        },
                                        dirty_node_ids=set(_dirty_targets.node_ids),
                                        dirty_edge_ids=set(_dirty_targets.edge_ids),
                                        expected_prior_mutation_epoch=(
                                            int(
                                                _attribute_integrity_transition[
                                                    "mutation_epoch"
                                                ]
                                            )
                                            if _attribute_integrity_transition.get(
                                                "eligible"
                                            )
                                            else None
                                        ),
                                        expected_transition_active=bool(
                                            _attribute_integrity_transition.get(
                                                "transition_backend_bound"
                                            )
                                        ),
                                        protected_from_ord=_ivm_protect_ord,
                                        # Validate before publication, but do not stamp
                                        # clean yet: checkpoint retention may still
                                        # delete an old protected snapshot and advance
                                        # the mutation epoch.
                                        persist=False,
                                    )
                                )
                    checkpoint_metadata["attribute_integrity_receipt_preflight"] = (
                        _attribute_integrity_preflight
                    )
                with _publisher_timed_stage(timings_ms, "checkpoint_generation"):
                    recovery_publication = (
                        checkpoint_status == "published"
                        and _authorized_count_stamp_recovery is not None
                    )
                    # A count-stamp recovery consumes its journal row in the
                    # same transaction as the successor checkpoint. When the
                    # interval arm already opened the publication transaction it
                    # joins that one; otherwise it opens it here.
                    if recovery_publication and not _publication_transaction_open:
                        _open_publication_transaction(_publication_scope)
                        _publication_transaction_open = True
                    # The cycle counted and materialized the rows it read; a
                    # graph write since then would enter this checkpoint
                    # uncounted, and the next cycle's segment would not hold
                    # it either. Hold the branch write lock from this check
                    # through the commit, and refuse if the segment moved
                    # (the rows stay for the next cycle). A first publish has
                    # no counted fingerprint and is not checked.
                    if (
                        checkpoint_status == "published"
                        and previous is not None
                        and not write_race_checked
                    ):
                        if not _publication_transaction_open:
                            _open_publication_transaction(_publication_scope)
                            _publication_transaction_open = True
                        self._chronos_refuse_writes_since_counted(
                            conn, segment_signature_counted,
                        )
                    # Decision 284a: stamp the lock hold so far into the
                    # checkpoint, so readiness has publish-capacity evidence
                    # from the deployment's own publishes. The full hold is
                    # measured after commit and lives only in the returned
                    # timing_details; this one stops before the checkpoint
                    # write, commit and notification.
                    checkpoint_metadata["publisher_lock"] = {
                        "schema": "okg.publisher.checkpoint_lock.v1",
                        "hold_ms": (
                            round(
                                max(
                                    0.0,
                                    (time.perf_counter() - lock_acquired_at)
                                    * 1000.0,
                                ),
                                3,
                            )
                            if lock_acquired_at is not None
                            else None
                        ),
                        "measured_until": "before_checkpoint_write",
                        "hard_timeout_ms": self._publish_statement_timeout_ms(),
                        "hard_timeout_source": self.hard_statement_timeout_source,
                    }
                    # PACT openrouter-embeddings-hnsw: checkpoint_generation
                    # records the vector-set watermarks on this connection
                    # just before the checkpoint (inside the publication
                    # transaction when one is open, after the write-race check
                    # took the branch write lock). Writers take seq under the
                    # set's row lock, so the committed value it reads is never
                    # undercut by a later commit.
                    try:
                        generation_id = store.checkpoint_generation(
                            generation_id,
                            branch=self.branch_name,
                            deployment_name=self.deployment_name,
                            catalog_version_id=int(cv),
                            status=checkpoint_status,
                            metadata=checkpoint_metadata,
                            commit=(
                                False
                                if (
                                    _publication_transaction_open
                                    or caller_verified_transaction is not None
                                )
                                else None
                            ),
                        )
                        if recovery_publication:
                            if _COUNT_STAMP_RECOVERY_FAILPOINT is not None:
                                _COUNT_STAMP_RECOVERY_FAILPOINT(
                                    "after_successor_checkpoint_before_journal_consumption"
                                )
                            assert _authorized_count_stamp_recovery is not None
                            _consume_count_stamp_recovery_authorization(
                                conn,
                                authorization=_authorized_count_stamp_recovery,
                                successor_generation_id=str(generation_id),
                            )
                    except Exception as checkpoint_exc:
                        if attestation_failure is not None:
                            raise attestation_failure from checkpoint_exc
                        raise
                    (
                        checkpoint_operation_timings,
                        checkpoint_transaction_timings,
                    ) = _chronos_backend_timings(store)
                    _add_prefixed_timings(
                        timings_ms,
                        "checkpoint_generation.operation",
                        checkpoint_operation_timings,
                    )
                    _add_prefixed_timings(
                        timings_ms,
                        "checkpoint_generation.runtime",
                        checkpoint_transaction_timings,
                    )
                if (
                    _publication_transaction_open
                    and caller_verified_transaction is None
                ):
                    # A caller-verified cycle commits later, in
                    # verify_and_commit(); closing the scope there is a no-op.
                    # Commit explicitly so its cost is timed and a commit-time
                    # failure surfaces here, before anything reads the
                    # generation as published. close() commits the transaction,
                    # then runs the backend's after-commit hook; a failed commit
                    # rolls back and runs the after-rollback hook instead.
                    with _publisher_timed_stage(timings_ms, "publication_commit"):
                        _publication_scope.close()
            protected_write_fence.close()
            _attribute_integrity_receipt = dict(_attribute_integrity_preflight)
            _attribute_transition_active = bool(
                _attribute_integrity_transition.get("transition_backend_bound")
            )
            try:
                with _publisher_timed_stage(
                    timings_ms, "attribute_integrity_receipt_final",
                ):
                    if _attribute_integrity_preflight.get("advanced") is True:
                        with conn.transaction():
                            if _dirty_targets is None:
                                _attribute_integrity_receipt = (
                                    finalize_full_attribute_integrity_anchor(
                                        conn,
                                        deployment_name=self.deployment_name,
                                        branch=self.branch_name,
                                        namespace="default",
                                        generation_id=str(generation_id),
                                        ordinal=assigned_ordinal,
                                        checkpoint_counts={
                                            "node": int(n_nodes),
                                            "edge": int(n_edges),
                                        },
                                        persist=True,
                                    )
                                )
                            else:
                                _attribute_integrity_receipt = (
                                    finalize_incremental_attribute_integrity_receipt(
                                        conn,
                                        deployment_name=self.deployment_name,
                                        branch=self.branch_name,
                                        namespace="default",
                                        generation_id=str(generation_id),
                                        previous_generation_id=(
                                            str(previous.checkpoint_id)
                                            if previous is not None
                                            else None
                                        ),
                                        ordinal=assigned_ordinal,
                                        checkpoint_counts={
                                            "node": int(n_nodes),
                                            "edge": int(n_edges),
                                        },
                                        dirty_node_ids=set(_dirty_targets.node_ids),
                                        dirty_edge_ids=set(_dirty_targets.edge_ids),
                                        expected_prior_mutation_epoch=int(
                                            _attribute_integrity_transition[
                                                "mutation_epoch"
                                            ]
                                        ),
                                        expected_transition_active=(
                                            _attribute_transition_active
                                        ),
                                        protected_from_ord=_ivm_protect_ord,
                                        persist=True,
                                    )
                                )
            except Exception as exc:  # noqa: BLE001 - proof cache stays dirty
                logging.getLogger(__name__).exception(
                    "attribute integrity receipt finalization failed after "
                    "a validated checkpoint"
                )
                _attribute_integrity_receipt = {
                    "advanced": False,
                    "reason": "post_checkpoint_finalization_failed",
                    "error_type": type(exc).__name__,
                }
            finally:
                if _attribute_transition_active:
                    clear_attribute_integrity_transition(
                        conn,
                        deployment_name=self.deployment_name,
                        branch=self.branch_name,
                        namespace="default",
                        transition_active=True,
                    )
                    _attribute_transition_guard.clear()
            attribute_effective_view_refresh["integrity_receipt"] = (
                _attribute_integrity_receipt
            )
            if checkpoint_status == "published" and link_retry_receipt:
                with _publisher_timed_stage(
                    timings_ms,
                    "link_retry_acknowledge",
                ):
                    acknowledge_link_retries(conn, link_retry_receipt)
            if checkpoint_status == "published" and extractor_queue_receipt:
                with _publisher_timed_stage(
                    timings_ms,
                    "extractor_queue_acknowledge",
                ):
                    acknowledge_extractor_queue(
                        conn,
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                        node_ids=extractor_queue_receipt,
                    )
            if checkpoint_status == "published":
                with _publisher_timed_stage(
                    timings_ms,
                    "source_retirement_receipt_binding",
                ):
                    from okg.substrate.source_retirement import (
                        bind_source_retirements_to_published_generation,
                    )

                    bind_source_retirements_to_published_generation(
                        conn,
                        deployment_name=self.deployment_name,
                        branch_name=self.branch_name,
                        generation_id=str(generation_id),
                    )
            with _publisher_timed_stage(
                timings_ms,
                "lens_promotion_generation_ref",
            ):
                self._finalize_lens_promotion_generation_refs(
                    conn,
                    cycle_token=str(gate_target_id),
                    generation_ref=generation_id,
                    generation_status=checkpoint_status,
                )
            _emit_phase_event_with_timing(timings_ms, "commit_complete", {
                "generation_id": generation_id,
                "status": checkpoint_status,
                "node_count": n_nodes,
                "edge_count": n_edges,
                "duration_ms": int((time.monotonic() - cycle_start) * 1000),
            })

            self._last_post_publish_consistency = []
            if not blocked_reason and self.post_publish_consistency_checks:
                # Every post-publish check reads the checkpoint just
                # published (``generation_id``), never the live branch head.
                # ``gate_target_id`` stays the compat id the run records key
                # on.
                # Each SQL check that reads the shared mirror takes its lock
                # (bounded wait), refreshes it from this checkpoint when
                # needed, and refuses a copy of another generation. Any of
                # those failures is that check's error, never the publish's.
                with _publisher_timed_stage(
                    timings_ms, "post_publish_consistency_checks",
                ), chronos_sql_mirror_for_checks(
                    conn,
                    self.post_publish_consistency_checks,
                    store=store,
                    generation_id=str(generation_id),
                    deployment_name=self.deployment_name,
                    branch=self.branch_name,
                    refresh=self.sync_chronos_graph,
                ) as mirror_guard:
                    for check in self.post_publish_consistency_checks:
                        # The whole per-check body (evaluate -> summarize -> honest
                        # crash summary -> runs block) is the shared runner
                        # (run_one_post_publish_check); this live path only supplies
                        # HOW to evaluate. IR checks derive their scope from the
                        # publish delta (never a declared frontier); absent delta
                        # evidence they run full with the reason recorded — never
                        # silently scoped. When a BLOCKED generation intervened, the
                        # current-segment dirty seed does not cover the blocked gen's
                        # writes (baseline = last PUBLISHED gen spans segments), so
                        # delta evidence is withheld — same guard the IVM/rollup paths
                        # apply via _block_intervened.
                        def _evaluate(check=check):
                            if getattr(check, "accepts_publish_delta", False):
                                return check.evaluate(
                                    conn,
                                    target_generation_id=gate_target_id,
                                    checkout_generation_id=str(generation_id),
                                    catalog_version_id=int(cv),
                                    deployment_name=self.deployment_name,
                                    branch_name=self.branch_name,
                                    dirty_scope=(
                                        None if _block_intervened
                                        else chronos_dirty_scope()
                                    ),
                                    previous_generation_id=(
                                        previous.checkpoint_id
                                        if previous is not None else None
                                    ),
                                )
                            # A mirror-reading SQL check holds the mirror
                            # lock only while it runs.
                            return evaluate_post_publish_check_reading_mirror(
                                conn,
                                check,
                                mirror_guard,
                                lambda: check.evaluate(
                                    conn,
                                    target_generation_id=gate_target_id,
                                    catalog_version_id=int(cv),
                                    deployment_name=self.deployment_name,
                                ),
                                target_generation_id=gate_target_id,
                                catalog_version_id=int(cv),
                                deployment_name=self.deployment_name,
                            )

                        self._last_post_publish_consistency.append(
                            run_one_post_publish_check(
                                conn,
                                check,
                                target_generation_id=gate_target_id,
                                evaluate=_evaluate,
                                logger=logging.getLogger(__name__),
                            )
                        )

            if caller_verified_transaction is not None:
                with _publisher_timed_stage(timings_ms, "notify"):
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_notify(%s, %s)",
                            (
                                generation_published_channel(self.deployment_name),
                                generation_id,
                            ),
                        )
                caller_verified_transaction.verify_and_commit(
                    str(generation_id)
                )
            else:
                try:
                    with _publisher_timed_stage(timings_ms, "notify"):
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT pg_notify(%s, %s)",
                                (
                                    generation_published_channel(
                                        self.deployment_name
                                    ),
                                    generation_id,
                                ),
                            )
                        conn.commit()
                except Exception:  # noqa: BLE001 — default notification remains best-effort
                    conn.rollback()
                    logging.getLogger(__name__).exception(
                        "Chronos-native publish notification failed"
                    )
            lock_hold_ms = round(
                max(0.0, (time.perf_counter() - lock_acquired_at) * 1000.0),
                3,
            ) if lock_acquired_at is not None else None

        total_duration_ms = int((time.monotonic() - cycle_start) * 1000)
        timings_ms["total"] = round(float(total_duration_ms), 3)
        timing_details = self._chronos_timing_details(
            timings_ms=timings_ms,
            lock_acquire_ms=timings_ms.get("locked_connection"),
            lock_hold_ms=lock_hold_ms,
            coordination_mode=self._resolved_coordination_mode(),
            hard_timeout_ms=self._publish_statement_timeout_ms(),
            hard_timeout_source=self.hard_statement_timeout_source,
            lock_hold_headroom_factor=self.lock_hold_headroom_factor,
            lock_hold_headroom_factor_source=(
                self.lock_hold_headroom_factor_source
            ),
            allow_raised_statement_timeout=(
                self.allow_raised_statement_timeout
            ),
            allow_raised_statement_timeout_source=(
                self.allow_raised_statement_timeout_source
            ),
            enricher_runs=enricher_runs,
            consistency_summaries=consistency_summaries,
            view_outcomes=self._last_view_outcomes,
            native_view_refresh=self._last_native_view_refresh,
            lens_outcomes=self._last_lens_outcomes,
        )
        timing_details["attribute_effective_view_refresh"] = (
            attribute_effective_view_refresh
        )
        self._attach_prelock_enrich_report(timing_details, timings_ms)
        if noop_reuse_invalidation is not None:
            timing_details["noop_reuse"] = {
                "reused": False,
                **noop_reuse_invalidation,
            }
        status = checkpoint_status
        _emit_phase_event_with_timing(timings_ms, "cycle_complete", {
            "generation_id": generation_id,
            "status": status,
            "noop": noop,
            "total_duration_ms": total_duration_ms,
        })
        timing_events = list(timings_ms.pop(_PUBLISHER_TIMING_EVENTS_KEY, []))
        self._record_chronos_publish_telemetry(
            trace_identity=publish_trace_identity,
            context_identity=(
                context_trace_identity
                if context_trace_identity.is_contextual else None
            ),
            generation_id=generation_id,
            status=status,
            started_at=cycle_started_at,
            ended_at=datetime.now(timezone.utc),
            total_duration_ms=total_duration_ms,
            timings_ms=timings_ms,
            node_count=n_nodes,
            edge_count=n_edges,
            blocked_reason=blocked_reason,
            noop=noop,
            base_perf_counter=cycle_perf_started,
            timing_events=timing_events,
            graph_effect=graph_effect_manifest,
        )
        result = {
            "generation_id": generation_id,
            "predecessor_generation_id": (
                str(previous.checkpoint_id)
                if previous is not None
                else None
            ),
            "compat_generation_id": gate_target_id,
            "deployment_name": self.deployment_name,
            "status": status,
            "catalog_version_id": int(cv),
            "published_at": generation_published_at.isoformat(),
            "correlation_id": generation_id,
            "phase_a_converged": True,
            "phase_b_blocked": blocked_reason is not None,
            "excluded_sources": gate_excluded_sources,
            "consistency_gates": consistency_summaries,
            "nodes_added": nodes_added,
            "nodes_retracted": nodes_retracted,
            "nodes_updated": nodes_updated,
            "edges_added": edges_added,
            "edges_retracted": edges_retracted,
            "edges_updated": edges_updated,
            "node_count": n_nodes,
            "edge_count": n_edges,
            "views": self._format_view_outcomes(self._last_view_outcomes),
            "native_view_refresh": self._native_view_refresh_report(),
            "attribute_effective_view_refresh": attribute_effective_view_refresh,
            "dirty_scope_hint": dirty_scope_hint_decision,
            "lenses": self._last_lens_outcomes,
            "post_publish_consistency_checks": self._last_post_publish_consistency,
            "per_source_status": {},
            "enrichers": enricher_results,
            "enricher_runs": enricher_runs,
            "enricher_governance": enricher_governance,
            "maintained_derivations": deployment_derivation_summaries,
            "extractor_edges_emitted": extractor_edges_emitted,
            "derived_fact_summary": dict(derived_fact_summary),
            "derived_rejections": self._derived_rejection_summary(),
            "envelope_violations": list(self._last_envelope_violations),
            "noop": noop,
            "noop_reuse_invalidation": noop_reuse_invalidation,
            "noop_reuse_invalidation_reason": (
                noop_reuse_invalidation.get("reason")
                if noop_reuse_invalidation else None
            ),
            "blocked_reason": blocked_reason,
            "timings_ms": timings_ms,
            "timing_details": timing_details,
            "publisher_configuration": publisher_configuration,
            "first_publish_budget_warning": first_publish_budget_warning,
            "graph_counts_provenance": graph_counts_provenance,
            "total_duration_ms": total_duration_ms,
        }
        if self.checkpoint_attestors:
            result["deployment_attestations"] = deployment_attestations
        return result

    @staticmethod
    def _chronos_checkpoint_catalog_version(checkpoint: Any | None) -> int | None:
        if checkpoint is None:
            return None
        metadata = getattr(checkpoint, "metadata", None) or {}
        catalog_version_id = metadata.get("catalog_version_id")
        if catalog_version_id is None:
            return None
        try:
            return int(catalog_version_id)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _chronos_checkpoint_graph_counts(
        checkpoint: Any | None,
    ) -> tuple[int, int] | None:
        if checkpoint is None:
            return None
        metadata = getattr(checkpoint, "metadata", None) or {}
        try:
            return (
                int(metadata["node_count"]),
                int(metadata["edge_count"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def _chronos_interval_physical_tables(branch: Any) -> dict[str, str] | None:
        """Map each graph table to its interval backing table, or None.

        `_chronos_branch_tables` is not a Chronos-registered logical table, so
        the backend's rewrite leaves this statement alone and it runs on the
        session's own connection. Anything unexpected returns None and the
        caller counts the way it always did. Refusing is never wrong; it only
        costs a scan.

        `_chronos_branch_tables` is the registry Chronos itself reads ONLY
        while schema branching is off. With schema branching on, the backend
        resolves the physical table per segment through
        `_chronos_branch_table_bindings`, honouring tombstones and the
        segment's branch point, and this registry holds just the base
        registration -- so after any schema-version change the two disagree and
        this would count the wrong table with no error to show for it. The
        precondition is therefore checked, and an unreadable backend is treated
        as schema-branching ON: declining costs a scan, guessing costs a wrong
        published count.
        """
        log = logging.getLogger(__name__)

        def _decline(reason: str, *, warn: bool = True) -> None:
            # Never a bare None: the reason reaches the receipt through
            # `_GRAPH_COUNT.declined`, so a deployment that stopped taking this
            # route is visible rather than merely slower.
            _GRAPH_COUNT.declined = reason
            if warn:
                log.warning(
                    "chronos interval count route declined (%s); counting the "
                    "effective graph through the branch query path", reason,
                )

        backend = getattr(getattr(branch, "_context", None), "_backend", None)
        if backend is None:
            _decline("branch session exposes no backend to check")
            return None
        if getattr(backend, "enable_schema_branching", True):
            _decline("schema branching is on, or could not be ruled out")
            return None
        if getattr(backend, "name", None) != "interval":
            # The registry rows below are filtered to the interval backend. If
            # the session is reading through a different one, those rows
            # describe a table it does not read.
            _decline(
                f"branch backend is {getattr(backend, 'name', None)!r}, "
                "not interval",
                warn=False,
            )
            return None
        try:
            rows = branch.query(
                "SELECT table_name, physical_table FROM _chronos_branch_tables "
                "WHERE backend = 'interval' "
                "AND table_name IN ('okg.graph_nodes', 'okg.graph_edges')"
            )
        except _GRAPH_COUNT_READ_ERRORS as exc:
            log.warning(
                "chronos interval table registry unreadable; "
                "counting the effective graph through the branch query path",
                exc_info=True,
            )
            _decline(f"registry unreadable ({type(exc).__name__})", warn=False)
            return None
        mapping: dict[str, str] = {}
        for row in rows:
            name = str(row["table_name"])
            physical = row["physical_table"]
            if physical is None:
                _decline(f"registry has no backing table for {name}")
                return None
            physical = str(physical)
            if not _CHRONOS_PHYSICAL_TABLE_NAME.match(physical):
                _decline(f"backing table for {name} is not a bare identifier")
                return None
            mapping[name] = physical
        if sorted(mapping) != sorted(_CHRONOS_GRAPH_TABLES):
            # Routine on a deployment whose graph tables are not interval
            # backed, so this one does not warn.
            _decline("graph tables are not interval backed", warn=False)
            return None
        if len({*mapping.values()}) != len(mapping):
            # The check that matters is INJECTIVITY, and in this direction:
            # `table_name` is the registry primary key, so one logical name can
            # never yield two backing tables, but two logical names sharing one
            # backing table would make this count the same rows twice and
            # report both the node and the edge count wrong, silently.
            _decline(
                "the graph tables share one backing table "
                f"({sorted(mapping.values())})"
            )
            return None
        return mapping

    @staticmethod
    def _chronos_bounded_graph_counts(branch: Any) -> tuple[int, int] | None:
        """Count the effective graph without projecting the stored rows.

        `SELECT count(*) FROM okg.graph_nodes` is rewritten by the interval
        backend into a count over a subquery selecting EVERY column. This
        issues the backend's own visibility predicate against the same physical
        table with no projection instead. Same table, same predicate, same
        branch point -- Chronos binds `_chronos_branch_point` for the session
        that runs the statement, so a checkpoint checkout is counted at its own
        branch point rather than the branch head -- and therefore the same
        EXACT count, never an estimate.

        NO SPEEDUP IS CLAIMED FOR THIS, and the measurement says none exists.
        At 2,000,000 interval rows the two forms get the same plan and the same
        page count, because PostgreSQL flattens the subquery and prunes the
        columns nobody reads. What this buys is that the count no longer
        DEPENDS on that flattening, and that the statement whose plan
        `_chronos_graph_count_access_path` reports is the statement that
        actually runs. The cost driver the measurement did find is recorded
        there, not here.

        Returns None when the shortcut cannot be proven applicable, and the
        caller falls back to the projection path.
        """
        mapping = Publisher._chronos_interval_physical_tables(branch)
        if mapping is None:
            return None
        counted: list[int] = []
        # Progress is BUFFERED, not emitted per table. A failure on the second
        # table sends the caller back to the projection path, which counts BOTH
        # tables again -- so emitting as we go would leave the receipt holding
        # two records for the first table and a since-start clock that
        # double-counts its scan.
        buffered: list[tuple[str, int, float]] = []
        for table in _CHRONOS_GRAPH_TABLES:
            started = time.monotonic()
            try:
                rows = branch.query(
                    _CHRONOS_VISIBLE_COUNT_SQL.format(physical=mapping[table])
                )
                value = int(rows[0]["n"])
            except _GRAPH_COUNT_READ_ERRORS as exc:
                logging.getLogger(__name__).warning(
                    "bounded interval count failed for %s; counting the "
                    "effective graph through the branch query path",
                    table, exc_info=True,
                )
                _GRAPH_COUNT.declined = (
                    f"count failed for {table} ({type(exc).__name__})"
                )
                return None
            counted.append(value)
            buffered.append((table, value, time.monotonic() - started))
        progress = _GRAPH_COUNT.progress
        if progress is not None:
            for record in buffered:
                progress(*record)
        return counted[0], counted[1]

    @staticmethod
    def _chronos_current_graph_counts(branch: Any) -> tuple[int, int]:
        """Exact node/edge counts of the graph this session sees.

        Both routes below return the same number; `_GRAPH_COUNT.source` records
        which one produced it so a receipt can name its source instead of
        implying one.
        """
        _GRAPH_COUNT.declined = None
        bounded = Publisher._chronos_bounded_graph_counts(branch)
        if bounded is not None:
            _GRAPH_COUNT.source = _GRAPH_COUNT_SOURCE_INTERVAL
            return bounded
        if _GRAPH_COUNT.declined is None:
            # Every decline names itself. Reaching here without one is a defect
            # in this file, not a condition to paper over, so it is recorded
            # under a name that reads as broken rather than as a reason.
            _GRAPH_COUNT.declined = "declined without recording a reason"
        _GRAPH_COUNT.source = _GRAPH_COUNT_SOURCE_BRANCH_QUERY
        progress = _GRAPH_COUNT.progress
        counted: list[int] = []
        for table in _CHRONOS_GRAPH_TABLES:
            started = time.monotonic()
            value = int(
                branch.query(f"SELECT count(*) AS n FROM {table}")[0]["n"]
            )
            counted.append(value)
            if progress is not None:
                progress(table, value, time.monotonic() - started)
        return counted[0], counted[1]

    @staticmethod
    def _chronos_graph_count_access_path(
        conn: psycopg.Connection,
        branch: Any,
    ) -> tuple[dict[str, dict[str, Any]] | None, str | None]:
        """How PostgreSQL will run the graph counts, before they run.

        Issue #1456 reported an hour-long first-publish count and attributed it
        to the query surface. Measured on this repository at 2,000,000 interval
        rows, that attribution is wrong: PostgreSQL prunes the interval
        backend's all-columns projection, so the projected and unprojected
        counts get the SAME plan and the same page count. What decides the cost
        is whether the backing table can be counted from an index:

          visibility map current -> Parallel Index Only Scan,  9,263 pages
          visibility map stale   -> Parallel Seq Scan,       110,768 pages

        A twelvefold difference in pages read, invisible from the query text
        and decided entirely by whether the table has been vacuumed. So report
        it: EXPLAIN (no ANALYZE, so nothing is executed) costs milliseconds and
        tells the operator which of those two runs is about to start.

        Returns `(paths, None)` on success and `(None, reason)` otherwise. A
        diagnostic must never be the reason a publish fails, but it must also
        never go missing in silence -- an absent access path is exactly the
        blindness #1456 was filed about -- so the reason is returned for the
        receipt and the caller warns on it.

        Every statement runs inside a SAVEPOINT when the caller's connection is
        not in autocommit. `_cycle_chronos_native` does not own that
        connection, and a failed statement inside somebody else's open
        transaction aborts it: without the savepoint a failing diagnostic would
        take down the rest of the cycle with `current transaction is aborted`,
        far from its cause.
        """
        mapping = Publisher._chronos_interval_physical_tables(branch)
        if mapping is None:
            return None, "chronos interval backing tables could not be resolved"
        try:
            branch_point = branch.query(
                "SELECT :_chronos_branch_point AS bp"
            )[0]["bp"]
        except Exception as exc:  # noqa: BLE001 - diagnostic only
            logging.getLogger(__name__).debug(
                "count access-path probe could not resolve the branch point",
                exc_info=True,
            )
            return None, f"branch point unavailable ({type(exc).__name__})"
        paths: dict[str, dict[str, Any]] = {}
        for table, physical in sorted(mapping.items()):
            statement = sql.SQL(
                "EXPLAIN (FORMAT JSON) SELECT count(*) FROM {physical} "
                "WHERE live_lo <= %s AND %s < live_hi AND deleted = FALSE"
            ).format(physical=sql.Identifier(physical))
            savepoint = not getattr(conn, "autocommit", False)
            try:
                with conn.cursor() as cur:
                    if savepoint:
                        cur.execute("SAVEPOINT okg_count_access_path")
                    try:
                        cur.execute(statement, (branch_point, branch_point))
                        row = cur.fetchone()
                    except Exception:
                        if savepoint:
                            cur.execute(
                                "ROLLBACK TO SAVEPOINT okg_count_access_path"
                            )
                        raise
                    if savepoint:
                        cur.execute("RELEASE SAVEPOINT okg_count_access_path")
                plan = row[0][0]["Plan"] if row else None
            except Exception as exc:  # noqa: BLE001 - diagnostic only
                logging.getLogger(__name__).debug(
                    "count access-path probe failed for %s", table,
                    exc_info=True,
                )
                return None, f"EXPLAIN failed for {table} ({type(exc).__name__})"
            if not isinstance(plan, Mapping):
                return None, f"EXPLAIN returned no plan for {table}"
            scan = Publisher._first_scan_node(plan)
            if scan is None:
                return None, f"plan for {table} contains no scan node"
            paths[table] = {
                "physical_table": physical,
                "scan": str(scan.get("Node Type")),
                "index": scan.get("Index Name"),
                "estimated_total_cost": plan.get("Total Cost"),
                # True when the count can be served from an index; False means
                # the whole backing table -- jsonb payloads and embedding
                # vectors included -- is read to produce one integer.
                "index_only": str(scan.get("Node Type")) == "Index Only Scan",
            }
        return paths, None

    @staticmethod
    def _first_scan_node(plan: Mapping[str, Any]) -> Mapping[str, Any] | None:
        """The first scan node found depth-first, or None if there is none.

        Deliberately named for what it does. A plan with several scans -- an
        Append over partitions, or a Bitmap Heap Scan whose child is a Bitmap
        Index Scan -- has no single answer, and this reports one of them. The
        count this probes is a single unpartitioned relation, so today there is
        exactly one; a shape that grows more would need a real summary rather
        than this.
        """
        children = plan.get("Plans")
        if isinstance(children, list):
            for child in children:
                if isinstance(child, Mapping):
                    found = Publisher._first_scan_node(child)
                    if found is not None:
                        return found
        if "Scan" in str(plan.get("Node Type", "")):
            return plan
        return None

    @staticmethod
    def _chronos_backing_heap_bytes(
        conn: psycopg.Connection,
        mapping: Mapping[str, str] | None,
    ) -> tuple[dict[str, int] | None, str | None]:
        """Current heap size in bytes per graph backing table, or None.

        `pg_relation_size` reads the main fork's size on disk, so unlike
        `reltuples` it is exact on a table that has never been analyzed. It
        is what a heap-scan count reads, which makes it the right measure of
        whether that count is worth announcing.

        `mapping` is the caller's already-resolved graph-table mapping, so
        this adds no registry read. The read runs in its own transaction or
        savepoint under a short `lock_timeout`, restored afterwards, so a
        table locked by someone else returns a reason instead of blocking
        before anything has been printed.
        """
        if mapping is None:
            return None, "chronos interval backing tables could not be resolved"
        sizes: dict[str, int] = {}
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT current_setting('lock_timeout')")
                    previous_timeout = cur.fetchone()[0]
                    cur.execute(
                        "SELECT set_config('lock_timeout', %s, true)",
                        (_BACKING_HEAP_BYTES_LOCK_TIMEOUT,),
                    )
                    for table, physical in sorted(mapping.items()):
                        cur.execute(
                            "SELECT pg_relation_size(to_regclass(%s))",
                            (physical,),
                        )
                        row = cur.fetchone()
                        if not row or row[0] is None:
                            missing = table
                            break
                        sizes[table] = int(row[0])
                    else:
                        missing = None
                    cur.execute(
                        "SELECT set_config('lock_timeout', %s, true)",
                        (previous_timeout,),
                    )
        except psycopg.Error as exc:
            logging.getLogger(__name__).debug(
                "heap size unavailable", exc_info=True,
            )
            return None, f"heap size read failed ({type(exc).__name__})"
        if missing is not None:
            return None, f"backing table for {missing} was not found"
        return sizes, None

    @staticmethod
    def _chronos_physical_row_version_estimate(
        branch: Any,
        *,
        mapping: Mapping[str, str] | None = _MAPPING_NOT_GIVEN,
    ) -> tuple[dict[str, int] | None, str | None]:
        """Planner row-version estimate per graph backing table, or None.

        This is an ESTIMATE and it does NOT estimate the effective graph: it is
        `pg_class.reltuples`, the planner's count of ALL row versions in the
        backing table, including superseded and tombstoned ones. It is reported
        under its own key, before the exact count starts, purely so an operator
        watching a long first publish knows the order of magnitude of what is
        being scanned. Nothing consumes it as a count.

        `mapping` is a graph-table mapping the caller already resolved (None
        when resolution declined); when omitted it is resolved here.
        """
        if mapping is _MAPPING_NOT_GIVEN:
            mapping = Publisher._chronos_interval_physical_tables(branch)
        if mapping is None:
            return None, "chronos interval backing tables could not be resolved"
        estimates: dict[str, int] = {}
        for table, physical in mapping.items():
            try:
                rows = branch.query(
                    "SELECT reltuples AS n FROM pg_class "
                    "WHERE oid = to_regclass(:relname)",
                    {"relname": physical},
                )
            except _GRAPH_COUNT_READ_ERRORS as exc:
                logging.getLogger(__name__).debug(
                    "row-version estimate unavailable for %s", table,
                    exc_info=True,
                )
                return None, (
                    f"reltuples read failed for {table} ({type(exc).__name__})"
                )
            if not rows or rows[0]["n"] is None:
                continue
            value = int(rows[0]["n"])
            if value < 0:
                # -1 is PostgreSQL's "never analyzed" sentinel, not a count.
                # Skip that table rather than discarding the other one: a
                # partial estimate is still an order of magnitude, and the key
                # is absent rather than zero so nobody reads it as "empty".
                continue
            estimates[table] = value
        if not estimates:
            return None, "no backing table has been analyzed yet"
        return estimates, None

    def _reconcile_effective_interval_view(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: str,
        node_count: int,
        edge_count: int,
        full_counts: tuple[int, int] | None,
    ) -> None:
        """Periodically prove the prepared interval front before publication."""
        if full_counts is None:
            return

        from okg.substrate.attributes import (
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
            EFFECTIVE_INTERVAL_OPEN_ORD,
        )

        full_nodes, full_edges = full_counts
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_kind, count(*) "
                f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s AND namespace = 'default' "
                "AND to_ord = %s GROUP BY target_kind",
                (self.branch_name, EFFECTIVE_INTERVAL_OPEN_ORD),
            )
            open_counts = {str(kind): int(count) for kind, count in cur.fetchall()}
        if (node_count, edge_count) != (full_nodes, full_edges):
            raise SubstrateError(
                "published cardinality running counter drifted from the full graph "
                "count -- the per-cycle delta is wrong",
                data={
                    "generation_id": generation_id,
                    "branch": self.branch_name,
                    "counter": [node_count, edge_count],
                    "full_count": [full_nodes, full_edges],
                },
            )
        if (open_counts.get("node", 0), open_counts.get("edge", 0)) != (
            full_nodes,
            full_edges,
        ):
            raise SubstrateError(
                "interval effective-view open-count disagrees with the full graph "
                "count -- a sync was missed/partial, a delta is wrong, or the flag "
                "was enabled before the interval table was backfilled from the snapshot",
                data={
                    "generation_id": generation_id,
                    "branch": self.branch_name,
                    "interval_open": [
                        open_counts.get("node", 0),
                        open_counts.get("edge", 0),
                    ],
                    "full_count": [full_nodes, full_edges],
                },
            )

    @staticmethod
    def _consistency_gate_edge_mutation_counts(
        summaries: list[Mapping[str, Any]],
        *,
        include_pre_view_pass: bool = False,
    ) -> dict[str, int]:
        """Return count-only receipt data for gate-induced graph removals.

        TransformConsistencyGate records the affected ids in its result details.
        Counting those ids here keeps the publisher decoupled from the concrete gate
        class while ensuring every pre-checkpoint removal is reflected in both the
        count stamp and the projection receipt.

        A post-view summary replaces its pre-view summary and keeps it under
        ``details["pre_view_pass"]``. With ``include_pre_view_pass`` the removals
        of that superseded pass count too (#2305): they happened, and a
        post-view pass that removed nothing must not hide them. The two passes
        never remove the same row, because the post-view pass runs over a branch
        the pre-view removals already left.
        """
        pruned_count = 0
        orphan_count = 0
        cleanup_node_count = 0
        cleanup_edge_count = 0
        passes: list[Mapping[str, Any]] = []
        for summary in summaries:
            passes.append(summary)
            details = summary.get("details")
            pre_view = (
                details.get("pre_view_pass")
                if include_pre_view_pass and isinstance(details, Mapping)
                else None
            )
            if isinstance(pre_view, Mapping):
                passes.append(pre_view)
        for summary in passes:
            details = summary.get("details")
            if not isinstance(details, Mapping):
                continue
            pruned_count += int(
                details.get("pruned_dangling_edge_count", len(
                    details.get("pruned_dangling_edge_ids", ()) or ()
                )) or 0
            )
            orphan_count += int(
                details.get("orphan_retracted_derived_edge_count", len(
                    details.get("orphan_retracted_derived_edge_ids", ()) or ()
                )) or 0
            )
            cleanup = details.get("derived_node_cleanup")
            if isinstance(cleanup, Mapping):
                cleanup_node_count += int(
                    cleanup.get("retracted_node_count", len(
                        cleanup.get("retracted_node_ids", ()) or ()
                    )) or 0
                )
                cleanup_edge_count += int(
                    cleanup.get("retracted_incident_edge_count", len(
                        cleanup.get("retracted_incident_edge_ids", ()) or ()
                    )) or 0
                )
        # TransformConsistencyGate makes these buckets disjoint by re-evaluating
        # after orphan retraction and before policy pruning.
        return {
            "dangling_edges_pruned": pruned_count,
            "orphaned_derived_edges_retracted": orphan_count,
            "unsupported_derived_nodes_retracted": cleanup_node_count,
            "unsupported_derived_incident_edges_retracted": cleanup_edge_count,
            "nodes_removed": cleanup_node_count,
            "edges_removed": pruned_count + orphan_count + cleanup_edge_count,
        }

    @staticmethod
    def _chronos_counts_after_delta(
        previous_counts: tuple[int, int] | None,
        delta_counts: Mapping[str, Any],
        *,
        branch: Any,
    ) -> tuple[int, int]:
        if previous_counts is None:
            # The delegate names itself.
            return Publisher._chronos_current_graph_counts(branch)
        # A fourth route, and the one a quiet cycle usually takes: no count at
        # all, the previous checkpoint's stamp advanced by this cycle's delta.
        # A receipt that left this unlabelled would imply a scan that never
        # happened.
        _GRAPH_COUNT.source = _GRAPH_COUNT_SOURCE_PREVIOUS_PLUS_DELTA
        previous_nodes, previous_edges = previous_counts
        return (
            previous_nodes
            + int(delta_counts["nodes_added"])
            - int(delta_counts["nodes_retracted"]),
            previous_edges
            + int(delta_counts["edges_added"])
            - int(delta_counts["edges_retracted"]),
        )

    @staticmethod
    def _chronos_compat_generation_id() -> int:
        """Return an integer generation token for legacy side tables.

        Chronos-native OKG generations are checkpoint ids. A few
        control-plane tables still store an integer
        ``target_generation_id`` for observation/view bookkeeping, so
        the native publisher carries this non-canonical token in
        checkpoint metadata instead of inserting into
        ``okg.graph_generations``.
        """

        return int(time.time_ns() // 1_000)

    def _run_chronos_extractor(
        self,
        conn: psycopg.Connection,
        *,
        store: Any,
        branch_name: str,
        generation_id: int,
        catalog_version_id: int,
        target_node_ids: Sequence[str] | None = None,
    ) -> tuple[int, list[LinkRetryKey], list[str]]:
        if not self._extractor_stage.tracks_extraction_cache:
            return 0, [], []
        prev_edge_watermark = self._current_fact_watermarks(conn)[1]
        checkout = store.checkout(branch_name)
        # Issue #3666: a link endpoint's text only restates its own key, so
        # it is never extracted (it would only record misses on itself).
        query = """
            SELECT node_id, subtype, attrs->>'text' AS text
              FROM okg.graph_nodes
             WHERE attrs ? 'text'
               AND NOT (coalesce(source, '') = :endpoint_source
                        AND coalesce(attrs->>'load_state', '') = 'not_loaded')
        """
        params: dict[str, Any] = {"endpoint_source": LINK_ENDPOINT_SOURCE}
        if target_node_ids is not None:
            query += " AND node_id = ANY(:node_ids)"
            params["node_ids"] = list(dict.fromkeys(target_node_ids))
        query += " ORDER BY node_id"
        rows = checkout.query(query, params)
        source_subtypes = self._extractor_stage.source_subtype_set
        miss_nodes = [
            (
                str(row["node_id"]),
                str(row["subtype"]),
                str(row["text"] or ""),
            )
            for row in rows
            if not source_subtypes or str(row["subtype"]) in source_subtypes
        ]
        processed_node_ids = sorted({
            node_id for node_id, _subtype, text in miss_nodes if text
        })
        # okg#3272: a targeted node that cannot be extracted (gone, no text,
        # or no longer a source subtype) has no mention left to retry. Its
        # rows join the receipt, acknowledged after a published checkpoint;
        # otherwise a ready one would be targeted, skipped, and invalidate
        # no-op reuse on every publish.
        unextractable: list[LinkRetryKey] = []
        if target_node_ids is not None:
            skipped = sorted(set(target_node_ids) - set(processed_node_ids))
            unextractable = ready_link_retry_keys_for_nodes(
                conn,
                deployment_name=self.deployment_name,
                branch_name=branch_name,
                node_ids=skipped,
                include_unready=True,
            )
        if not processed_node_ids:
            return 0, unextractable, []
        emitted, receipt = self._emit_chronos_extractor_edge_facts(
            conn,
            checkout=checkout,
            generation_id=generation_id,
            miss_nodes=miss_nodes,
        )
        receipt = sorted(set(receipt) | set(unextractable))
        if emitted:
            self._admit_extractor_outputs(
                conn,
                generation_id=generation_id,
                catalog_version_id=catalog_version_id,
                prev_edge_fact_watermark=prev_edge_watermark,
            )
        return int(emitted), receipt, processed_node_ids

    @staticmethod
    def _insert_link_endpoint_facts(
        conn: psycopg.Connection,
        facts: Sequence[tuple[str, str, Mapping[str, Any], str]],
        *,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> None:
        """Write link endpoint node facts (issue #3666): one fact per
        endpoint, ``I`` to declare it and ``R`` to withdraw it, never an
        alias. ``facts`` are ``(node_id, subtype, attrs, op)``."""
        if not facts:
            return
        with conn.cursor() as cur:
            for endpoint_id, endpoint_subtype, endpoint_attrs, op in facts:
                cur.execute(
                    """
                    INSERT INTO okg.node_facts
                      (node_id, subtype, attrs, source, source_record_id,
                       source_revision, observed_at, dedupe_key, run_id, op)
                    VALUES (%s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb,
                            %s, %s, %s, %s)
                    """,
                    (
                        endpoint_id,
                        endpoint_subtype,
                        json.dumps(dict(endpoint_attrs)),
                        LINK_ENDPOINT_SOURCE,
                        json.dumps({"node_id": endpoint_id}),
                        json.dumps({"generation_id": generation_id}),
                        observed_at,
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"{LINK_ENDPOINT_SOURCE}|{endpoint_id}|{op}|{run_id}",
                        ).hex,
                        run_id,
                        op,
                    ),
                )

    def _sweep_link_endpoints(
        self,
        conn: psycopg.Connection,
        *,
        store: Any,
        branch_name: str,
        generation_id: int,
    ) -> int:
        """Withdraw the endpoints no live record cites, or a real record now
        owns (issue #3666); return how many.

        Runs on every publish that reaches the derived-fact load when a route
        declares ``on_miss``, whether the extractor ran or not: removing a
        citing record wakes no extraction, and the endpoint must go in the
        same publish as its last citation. A deployment with no such route
        skips it and reads nothing.
        """
        cited = set(self._link_endpoints_cited)
        self._link_endpoints_cited = set()
        if not self._extractor_stage.endpoint_routes:
            return 0
        retractions = endpoints_to_retract(
            store.checkout(branch_name), keep=sorted(cited),
        )
        self._insert_link_endpoint_facts(
            conn,
            [(node_id, subtype, {}, "R") for node_id, subtype in retractions],
            generation_id=generation_id,
            run_id=str(uuid.uuid4()),
            observed_at=datetime.now(timezone.utc),
        )
        return len(retractions)

    def _emit_chronos_extractor_edge_facts(
        self,
        conn: psycopg.Connection,
        *,
        checkout: Any,
        generation_id: int,
        miss_nodes: list[tuple[str, str, str]],
    ) -> tuple[int, list[LinkRetryKey]]:
        """Emit extractor edge_facts without touching retired live tables.

        Chronos-native publishing dropped okg.nodes_live/okg.edges_live. Derived
        extractor edges still enter through edge_facts for admission/audit, then
        the publisher applies accepted derived facts to okg.graph_edges before
        checkpointing the branch.
        """
        # The catalog version the resolver will answer from, read before it
        # is built: an alias at this version was visible to the route, so a
        # miss on it is the resolver's answer, not a landing to wait for.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT coalesce(max(catalog_version_id), 0) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            route_catalog_version = int(cur.fetchone()[0])
        resolver = self._extractor_stage.resolver_factory(conn)
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)

        derived: list[Any] = []
        retry_misses: list[LinkRetryKey] = []
        processed_node_ids = [node_id for node_id, _subtype, text in miss_nodes if text]
        # Every earlier row of a node re-extracted now, ready or not: a row
        # this run does not record again is stale (okg#3272). An unready one
        # is deleted at once; a ready one joins the receipt, acknowledged only
        # after a published checkpoint.
        prior_rows = set(ready_link_retry_keys_for_nodes(
            conn,
            deployment_name=self.deployment_name,
            branch_name=self.branch_name,
            node_ids=processed_node_ids,
            include_unready=True,
        ))
        extracted = [
            (node_id, subtype, self._extractor_stage.extract(text))
            for node_id, subtype, text in miss_nodes
            if text
        ]
        # The alias rows present at the route's catalog version BEFORE any
        # route runs, by identity (okg#3272): a miss on one of these rows is
        # the resolver's own answer and must not mark the row ready on every
        # publish; any other row (new, re-added, or a newer version) landed
        # after this read and may wake it.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT alias, catalog_version_id, xmin::text FROM okg.aliases "
                "WHERE catalog_version_id = %s AND alias = ANY(%s)",
                (
                    route_catalog_version,
                    sorted({ent.value for _n, _s, ents in extracted for ent in ents}),
                ),
            )
            alias_rows_before_route = {
                (str(row[0]), (int(row[1]), str(row[2]))) for row in cur.fetchall()
            }
        # Issue #3666: endpoints a route's `on_miss` asks for, written below
        # as node facts of the core producer before the edges that reach them.
        # A deployment with no `on_miss` route routes exactly as before.
        endpoint_requests: list[Any] = []
        endpoint_kwargs: dict[str, Any] = (
            {"endpoint_requests": endpoint_requests}
            if self._extractor_stage.endpoint_routes
            else {}
        )
        for node_id, subtype, entities in extracted:
            for ent in entities:
                resolver_misses: list[tuple[str, str]] = []
                edge = self._extractor_stage.route(
                    ent,
                    src_node_id=node_id,
                    src_subtype=subtype,
                    resolver=resolver,
                    resolver_misses=resolver_misses,
                    **endpoint_kwargs,
                )
                retry_misses.extend(
                    LinkRetryKey(
                        self.deployment_name,
                        self.branch_name,
                        node_id,
                        entity_type,
                        entity_value,
                    )
                    for entity_type, entity_value in resolver_misses
                )
                if edge is not None:
                    derived.append(edge)
        record_link_retry_misses(
            conn,
            retry_misses,
            seen_by_route=lambda value, identity: (
                (value, identity) in alias_rows_before_route
            ),
        )
        unobserved = prior_rows - set(retry_misses)
        clear_unobserved_unready_link_retries(conn, sorted(unobserved))
        retry_receipt = sorted(unobserved)
        endpoint_writes = plan_link_endpoint_facts(
            conn,
            checkout=checkout,
            requests=endpoint_requests,
            catalog_version_id=route_catalog_version,
        )
        # Every endpoint this run routed to, written now or already live: the
        # publish's endpoint sweep keeps them, because the edges reaching
        # them are applied only after it runs.
        self._link_endpoints_cited.update(
            request.node_id for request in endpoint_requests
        )
        self._insert_link_endpoint_facts(
            conn,
            [
                (request.node_id, request.subtype, request.attrs, "I")
                for request in endpoint_writes
            ],
            generation_id=generation_id,
            run_id=run_id,
            observed_at=observed_at,
        )
        if not derived:
            return 0, retry_receipt

        unique: dict[tuple[str, str, str], Any] = {}
        for edge in derived:
            unique.setdefault((edge.src, edge.edge_type, edge.dst), edge)

        from okg.substrate.graph_store import _edge_id

        dedupe_rows = [
            (
                src,
                edge_type,
                dst,
                edge,
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"_extractor|{src}|{edge_type}|{dst}",
                ).hex,
                _edge_id(src, edge_type, dst),
            )
            for (src, edge_type, dst), edge in unique.items()
        ]
        dedupe_keys = [row[4] for row in dedupe_rows]
        already_present: set[str] = set()
        chunk_size = 1000
        with conn.cursor() as cur:
            for start in range(0, len(dedupe_keys), chunk_size):
                chunk = dedupe_keys[start:start + chunk_size]
                cur.execute(
                    "SELECT DISTINCT dedupe_key FROM okg.edge_facts "
                    "WHERE dedupe_key = ANY(%s)",
                    (chunk,),
                )
                already_present.update(str(row[0]) for row in cur.fetchall())

            edge_ids = [row[5] for row in dedupe_rows]
            already_live: set[int] = set()
            for start in range(0, len(edge_ids), chunk_size):
                chunk = edge_ids[start:start + chunk_size]
                rows = checkout.query(
                    """
                    SELECT edge_id
                      FROM okg.graph_edges
                     WHERE edge_id = ANY(:edge_ids)
                    """,
                    {"edge_ids": chunk},
                )
                already_live.update(int(row["edge_id"]) for row in rows)

            to_insert = [
                row for row in dedupe_rows
                if row[4] not in already_present
                and int(row[5]) not in already_live
            ]
            for (
                src,
                edge_type,
                dst,
                edge,
                dedupe_key,
                edge_id,
            ) in to_insert:
                cur.execute(
                    """
                    INSERT INTO okg.edge_facts
                      (edge_id, src, dst, edge_type, attrs, provenance,
                       confidence, source, source_record_id,
                       source_revision, observed_at, dedupe_key, run_id, op)
                    VALUES (%s, %s, %s, %s, %s::jsonb,
                            'linked_heuristic', %s, %s,
                            %s::jsonb, %s::jsonb,
                            %s, %s, %s, 'I')
                    """,
                    (
                        edge_id,
                        src,
                        dst,
                        edge_type,
                        json.dumps({
                            "match_type": f"{edge.entity_type}_mention",
                            "entity_value": edge.entity_value,
                        }),
                        edge.confidence,
                        f"_extractor:{edge.entity_type}",
                        json.dumps({
                            "src_node_id": src,
                            "entity_type": edge.entity_type,
                            "entity_value": edge.entity_value,
                        }),
                        json.dumps({"generation_id": generation_id}),
                        observed_at,
                        dedupe_key,
                        run_id,
                    ),
                )
        return len(to_insert), retry_receipt

    def cycle(
        self,
        *,
        conn: psycopg.Connection | None = None,
        atomic_precommit_verifier: (
            Callable[[psycopg.Connection, str], None] | None
        ) = None,
        native_view_refresh_final: bool = True,
        force_native_view_refresh: bool = False,
        force_enrichers: bool = False,
        publish_blocker: Mapping[str, Any] | None = None,
        expected_latest_checkpoint_id: str | None = None,
        expected_branch_mutation_token: str | None = None,
        require_expected_latest_checkpoint: bool = False,
    ) -> dict[str, Any]:
        """One full publish cycle: reserve → A → B → C.

        Returns a summary dict used by `okg run --once` and tests.
        Raises PublisherLeaseHeld if someone else is publishing.

        The publish path is Chronos-native (legacy projector path removed): one
        cycle checkpoints the Chronos branch head as a published generation.

        `force_enrichers` is the sanctioned P6 operator lever
        (specify-publish-commit-protocol): it bypasses the pre-enrichment
        noop-reuse skip for exactly this invocation so a zero-delta,
        unchanged-catalog deployment still runs every enricher, runs gates,
        and cuts a checkpoint. It is recorded as an ``operator_forced``
        noop-reuse invalidation; the next unforced cycle resumes normal
        skip behavior with intact watermarks and reuse pins.

        `publish_blocker` is a run-scoped, structured refusal to advance reader
        visibility. It invalidates no-op reuse and becomes the blocked
        checkpoint's durable `blocked_reason`; callers must provide a stable
        non-empty `reason`.

        `expected_latest_checkpoint_id` and `expected_branch_mutation_token`
        guard a staged-source redrive against both checkpoints and ordinary
        source writes between its admission probe and this locked cycle. The
        guarded cycle holds the graph store's session write fence through the
        checkpoint, so a source write cannot enter after validation.
        `require_expected_latest_checkpoint` enables the guard even when the
        expected state is no checkpoint.

        `atomic_precommit_verifier` selects the additive caller-verified
        publication mode. It requires a supplied non-autocommit connection.
        The verifier receives that same connection and the new generation id
        after checkpoint and pointer writes, but before their single commit.
        Raising rolls the entire publication transaction back. Omitting the
        verifier preserves the existing publisher-owned commit behavior.
        """
        packaged = getattr(self, "_packaged_release_context", None)
        if packaged is not None:
            from okg.substrate.distributions.packaged_release import current_verified_release
            if current_verified_release(packaged.deployment_dir) is not packaged:
                raise ValueError("packaged publication verification scope expired")
        caller_verified_transaction = None
        if atomic_precommit_verifier is not None:
            if conn is None:
                raise ValueError(
                    "atomic_precommit_verifier requires a supplied connection"
                )
            if conn.autocommit:
                raise ValueError(
                    "atomic_precommit_verifier requires autocommit=False"
                )
            incompatible_enrichers = [
                str(getattr(enricher, "name", type(enricher).__name__))
                for enricher in self.enrichers
                if getattr(
                    enricher,
                    "atomic_publish_transaction_compatible",
                    False,
                ) is not True
            ]
            if incompatible_enrichers:
                raise ValueError(
                    "caller-verified publication requires every enricher to "
                    "declare atomic_publish_transaction_compatible=True; "
                    "incompatible enrichers: "
                    + ", ".join(incompatible_enrichers)
                )
            caller_verified_transaction = _CallerVerifiedPublishTransaction(
                conn=conn,
                verifier=atomic_precommit_verifier,
            )
            caller_verified_transaction.begin()
        # A cycle refused because another writer changed the branch head
        # mid-publish (`graph_written_during_publish`) is re-run from the
        # start: every lock is released between attempts, the rows stay
        # unpublished and the next attempt counts them. Bounded, so a
        # continuous writer surfaces as a failure instead of a hang. A
        # caller-verified transaction cannot be re-run inside the caller's
        # transaction, so it gets one attempt.
        attempts_allowed = (
            1 if caller_verified_transaction is not None
            else _write_race_attempts_allowed()
        )
        source_run_wait: dict[str, Any] | None = None
        if conn is None and self.own_source_run_ids is not None:
            # An ingest's run-end publish waits (bounded) for other source
            # runs of the deployment to finish, before it takes any lock: a
            # run it waits for needs the branch write lock to finish, and
            # waits on nothing of this publish. The guard still decides.
            from okg.substrate.ingest.run_base import wait_for_source_runs

            source_run_wait = wait_for_source_runs(
                self.dsn,
                deployment=self.deployment_name,
                own_run_ids=self.own_source_run_ids,
            )
        write_race_refusals: list[dict[str, Any]] = []
        attempt = 0
        while True:
            attempt += 1
            try:
                result = self._cycle_chronos_native(
                    conn=conn,
                    native_view_refresh_final=native_view_refresh_final,
                    force_native_view_refresh=force_native_view_refresh,
                    force_enrichers=force_enrichers,
                    publish_blocker=publish_blocker,
                    expected_latest_checkpoint_id=expected_latest_checkpoint_id,
                    expected_branch_mutation_token=expected_branch_mutation_token,
                    require_expected_latest_checkpoint=(
                        require_expected_latest_checkpoint
                    ),
                    caller_verified_transaction=caller_verified_transaction,
                )
            except SubstrateError as exc:
                if caller_verified_transaction is not None:
                    caller_verified_transaction.rollback()
                if getattr(exc, "code", None) != "graph_written_during_publish":
                    if source_run_wait is not None and isinstance(exc.data, dict):
                        exc.data.setdefault("source_run_wait", source_run_wait)
                    raise
                write_race_refusals.append({
                    "attempt": attempt,
                    "stage": (exc.data or {}).get("stage"),
                })
                if attempt >= attempts_allowed:
                    if isinstance(exc.data, dict):
                        exc.data["write_race_refusals"] = write_race_refusals
                    raise
                logging.getLogger(__name__).warning(
                    "publish attempt %d of %d refused: the branch head was "
                    "written during the publish (%s); retrying",
                    attempt,
                    attempts_allowed,
                    write_race_refusals[-1]["stage"],
                )
                time.sleep(_write_race_backoff_seconds(attempt))
                continue
            except BaseException:
                if caller_verified_transaction is not None:
                    caller_verified_transaction.rollback()
                raise
            break
        if isinstance(result, dict):
            result["write_race_refusals"] = write_race_refusals
            if source_run_wait is not None:
                result["source_run_wait"] = source_run_wait
        if (
            caller_verified_transaction is not None
            and not caller_verified_transaction.finished
        ):
            caller_verified_transaction.rollback()
            raise RuntimeError(
                "caller-verified publisher returned without transaction outcome"
            )
        if (
            conn is None
            and isinstance(result, dict)
            and getattr(self, "post_publish_index_rebuild_in_cycle", False)
        ):
            # okg#3112: with no caller connection this cycle took and has
            # released the publisher lock. Opt-in per caller: the publisher
            # worker sets it (so does `okg run --once` through the worker).
            # Request paths (the MCP connector ingest, the exchange receiver)
            # leave it off so a request never waits for a rebuild; the next
            # worker or run cycle picks the work up. Callers that hold the
            # lock themselves (inline `okg run --once`, merge-back) and `okg
            # ingest` run `rebuild_indexes_after_publish` after releasing
            # it. Never raises; does nothing after a blocked or no-op
            # publish; bounded by its time budget.
            from okg.substrate.db.index_reclaim import rebuild_indexes_after_publish

            result["post_publish_index_rebuild"] = rebuild_indexes_after_publish(
                self.dsn, deployment=self.deployment_name, summary=result,
            )
        return result
