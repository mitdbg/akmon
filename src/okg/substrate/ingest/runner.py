"""SourceRunner — drives a SourceAdapter and writes fact tables.

Contract:
  - Opens one connection per run (autocommit=True per the M2.5
    queue-module invariant; individual INSERT/COPY batches open
    short txns via `with conn.transaction():`).
  - dedupe_key per node: `uuid5(source|node_id|revision_hex)`
    so arrival order is irrelevant and the same fact from the
    same source+revision collapses at the partial unique index.
  - dedupe_key per edge: `uuid5(source|src|edge_type|dst|revision)`.
  - run_id: one uuid4 per `run()` call, stamped on every fact so
    an operator can grep the fact log by run.
  - observed_at: single `now()` at run-start, so all facts from
    one run share a timestamp. Matches the bootstrap pattern.
    Callers that need DBOS replay idempotency may provide a
    SourceRunContext with stable run_id + observed_at.
  - op='R' retractions: consult the M3.5 retraction policy guard
    (`assert_retraction_permitted`) with the adapter's profile
    + the run's mode. Refused retractions raise BEFORE any fact
    is written for the run.
  - Watermark: standalone runs advance
    `okg.source_watermarks.last_applied_rev` on successful completion.
    Admission-orchestrated runs return the candidate cursor and defer its
    durable advancement until the admission transaction accepts the run.

NOT in scope for this stride:
  - Streaming adapters that yield 10^6+ facts. COPY batches in
    chunks of 10_000 today; scaling beyond that is a future
    follow-up.
  - Cron-driven runner invocation. The runner is a library today;
    `okg run --source <name>` CLI wrapping is a follow-up.
  - Embedding-job fan-out. The Projector already enqueues embed
    jobs for cache-miss nodes; runner writes fact rows, the
    projector's normal flow handles embedding via the publisher
    cycle.
"""
from __future__ import annotations

import functools
import hashlib
import inspect
import json
import logging
import os
import re
import time
import uuid
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
)

import psycopg

from okg.substrate.admission import QUARANTINE_BRANCH
from okg.substrate.errors import SubstrateError
from okg.substrate.images.config import ImagesConfig
from okg.substrate.images.render import ImageCompleter
from okg.substrate.images.values import check_incoming
from okg.substrate.ingest.full_retraction_guard import (
    SUB_SCOPE_ATTRIBUTE,
    FullRetractionGuard,
    FullRetractionOverride,
    LiveRowCount,
    count_live_source_rows,
    live_rows_from_record_set,
)
from okg.substrate.runtime.source_sync_priority import ensure_run_guard_held
from okg.substrate.ingest.record_authority import (
    PrivateAuthorityCandidate,
    ProductionAuthorityStore,
)
from okg.substrate.ingest.change_identity import (
    BOOTSTRAP_AUTHORITY_CURSOR_KEY,
    PipelineIdentitySnapshot,
    bootstrap_authority_from_cursor,
    build_bootstrap_authority,
    build_effective_token,
    effective_token_from_cursor,
    identity_fingerprint,
    module_code_identity,
    raw_token_cursor,
)
from okg.substrate.library.sources.base import (
    APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT,
    BOOTSTRAP_SNAPSHOT_MATERIALIZATION,
    BootstrapIdentityClaim,
    EdgeFact,
    EdgeKey,
    NodeFact,
    ProgressMarker,
    RecordEmission,
    RecordAuthorityStream,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourceAdapter,
    cursor_has_record_set_authority,
    record_key_is_runner_reserved,
    record_key_is_scope_independent,
)
from okg.substrate.ingest.record_set_pieces import (
    DEFAULT_PIECE_MAX_BYTES as _DEFAULT_RECORD_SET_PIECE_MAX_BYTES,
    PIECED_RECORD_SET_MARKER as _PIECED_RECORD_SET_MARKER,
    RECORD_SET_PIECES_DDL as _RECORD_SET_PIECES_DDL,
    RecordSetPiecesError,
    iter_pieces as _iter_record_set_pieces,
    iter_record_set_json as _iter_record_set_json,
    reassemble_pieces as _reassemble_record_set_pieces,
)
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ChangeProbeError,
    ProbeVerdict,
    cursor_has_outstanding_work,
    run_probe,
    tokens_equal,
    validate_token_composition,
)
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.policy.retraction import (
    RunMode as PolicyRunMode,
    assert_retraction_permitted,
)
from okg.substrate.source_run_modes import (
    PROFILE_RUN_MODES,
    profile_mode_fix_hint as _profile_mode_fix_hint,
    resolve_whole_scope_request,
)
from okg.substrate.fact_equivalence import (
    compare_attrs,
    edge_columns_differing,
    volatile_fields_for,
)
from okg.substrate.source_sync import (
    SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES,
    source_sync_scope_narrows,
)
from okg.substrate.sources.registry import SourceEntry
from okg.substrate.source_sync_contract import (
    SOURCE_CONTRACT_RUN_MODES,
    SourceDeltaAccumulator,
    SourceFallbackEvidence,
    SourceSyncContractError,
    scope_keys as contract_scope_keys,
    scope_widened as contract_scope_widened,
)
from okg.substrate.telemetry import (
    SpanTimingRecorder,
    TelemetrySpan,
    child_span_identity,
    current_trace_identity,
    deep_span_capture_enabled,
    record_context_span_if_missing,
    record_span,
    use_trace,
)
from okg.substrate.transform.commit import persist_transform_program
from okg.substrate.transform.programs import ingestion_program

if TYPE_CHECKING:  # pragma: no cover - typing only
    from okg.substrate.ingest.pii_posture import PiiPosture
    from okg.substrate.producer_authority import ProducerProfile


log = logging.getLogger(__name__)

_RECORD_SET_CHRONOS_PROOF_KEY = "record_set_chronos_proof"
_RECORD_SET_CHRONOS_PROOF_SCHEMA = (
    "okg.source_runner.record_set_chronos_proof.v1"
)
_RECORD_SET_SUMMARY_KEY = "record_set_summary"
_RECORD_SET_SUMMARY_SCHEMA = "okg.source_runner.record_set_summary.v1"
_RECORD_SET_SANITIZATION_SCHEMA = (
    "okg.source_runner.record_set_sanitization.v1"
)
_RECORD_SET_REF_KEY = "record_set_ref"
_BOOTSTRAP_GLOBAL_SCOPE = "global:none"
BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY = "sync.bootstrap_identity_authority"
_BOOTSTRAP_LEDGER_SCHEMA = "okg.source_runner.bootstrap_identity_ledger.v2"
_BOOTSTRAP_LEDGER_STATUSES = frozenset({"active", "exact", "revoked"})

# Issue #1539. A narrowed run may replace a record OUTSIDE its sync_scope
# only when the runner itself can see that the replacement touches nothing
# but resolution edges pointing directly into the scope: a file that imports
# a changed file must re-point its references, because symbol ids carry line
# numbers. Both constants are the runner's own and never an adapter's
# declaration, like SCOPE_INDEPENDENT_RECORD_KEY_PATTERN, so a source cannot
# widen what it may delete by describing its own records.
SCOPED_DEPENDENT_EDGE_TYPES = frozenset({"references", "calls"})
# symbol:<repo>:<path>:<qualified name>:<line>. The repo and path segments
# admit no colon, so the path is read unambiguously; an id this does not
# match names no path and never qualifies.
SCOPED_DEPENDENT_SYMBOL_ID_PATTERN = re.compile(
    r"^symbol:[^:]+:(?P<path>[^:]+):.+:\d+$"
)


def _scope_key_covers_path(scope_key: str, path: str) -> bool:
    """Report whether `scope_key` names `path` or a directory above it."""
    if not scope_key:
        return False
    if path == scope_key:
        return True
    return path.startswith(scope_key.rstrip("/") + "/")


def scoped_dependent_edge_qualifies(
    edge: EdgeKey,
    scope_keys: Sequence[str],
) -> bool:
    """Report whether one edge points directly into the declared scope.

    The edge must be a resolution edge (SCOPED_DEPENDENT_EDGE_TYPES) whose
    target is a code symbol living in a file the scope names. One hop only:
    an edge into a file that merely imports a scoped file does not qualify.
    """
    if edge.edge_type not in SCOPED_DEPENDENT_EDGE_TYPES:
        return False
    match = SCOPED_DEPENDENT_SYMBOL_ID_PATTERN.match(edge.dst)
    if match is None:
        return False
    path = match.group("path")
    return any(_scope_key_covers_path(key, path) for key in scope_keys)


def _record_nodes_fingerprint(emission: RecordEmission | None) -> str:
    if emission is None:
        emission = RecordEmission()
    return json.dumps(
        {
            "primary": (
                emission.primary.to_cursor()
                if emission.primary is not None else None
            ),
            "descendants": sorted(
                json.dumps(node.to_cursor(), sort_keys=True, default=str)
                for node in emission.descendants
            ),
        },
        sort_keys=True,
        default=str,
    )


@dataclass(frozen=True)
class ScopedDependentReplacementReview:
    """What the runner could and could not verify about out-of-scope keys.

    ``unverified`` maps each refused key to the reasons it was refused.
    ``dependents`` names the keys whose durable prior record held at least
    one qualifying edge into the scope, i.e. real dependents of a scoped
    file; a refused key outside it has no relation to the scope at all.
    """

    unverified: dict[str, list[str]]
    dependents: frozenset[str]


def review_scoped_dependent_replacements(
    *,
    prior: RecordSet,
    current: RecordSet,
    keys: Iterable[str],
    scope_keys: Sequence[str],
) -> ScopedDependentReplacementReview:
    """Verify out-of-scope replacement keys against the durable prior.

    Issue #1539. For each key, compare the prior record with the record the
    slice carries (an absent record is an empty one). The replacement is
    verified only when the nodes are identical and every edge identity it
    adds or removes satisfies ``scoped_dependent_edge_qualifies``. Edges
    kept under the same identity may change attributes, which deletes
    nothing. A key whose record did not change is a verified no-op.
    """
    unverified: dict[str, list[str]] = {}
    dependents: set[str] = set()
    for key in sorted(set(keys)):
        before = prior.records.get(key)
        after = current.records.get(key)
        before_edges = {
            edge.identity(): edge
            for edge in (before.edges if before is not None else [])
        }
        after_edges = {
            edge.identity(): edge
            for edge in (after.edges if after is not None else [])
        }
        if any(
            scoped_dependent_edge_qualifies(edge, scope_keys)
            for edge in before_edges.values()
        ):
            dependents.add(key)
        reasons: list[str] = []
        if _record_nodes_fingerprint(before) != _record_nodes_fingerprint(after):
            reasons.append("record nodes change")
        removed = sorted(set(before_edges) - set(after_edges))
        added = sorted(set(after_edges) - set(before_edges))
        for label, identities, edges in (
            ("removes", removed, before_edges),
            ("adds", added, after_edges),
        ):
            for identity in identities:
                if not scoped_dependent_edge_qualifies(
                    edges[identity],
                    scope_keys,
                ):
                    src, edge_type, dst = identity
                    reasons.append(
                        f"{label} edge {src} -{edge_type}-> {dst}, "
                        "which does not point into the scope"
                    )
        if reasons:
            unverified[key] = reasons
    return ScopedDependentReplacementReview(
        unverified=unverified,
        dependents=frozenset(dependents),
    )


def validate_adapter_source_progress(
    progress: Mapping[Any, Any],
    *,
    source_name: str,
) -> dict[str, str]:
    """Normalize adapter progress without surrendering runner-owned keys."""
    if not isinstance(progress, Mapping):
        raise SubstrateError(
            "source terminal progress evidence is malformed",
            data={
                "source_name": source_name,
                "reason_code": "source_progress_not_mapping",
            },
            fix_hint="return source progress as a key/value mapping",
        )
    normalized = {str(key): str(value) for key, value in progress.items()}
    if BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY in normalized:
        err = SubstrateError(
            "source adapter attempted to write runner-owned identity authority",
            data={
                "source_name": source_name,
                "reason_code": "runner_progress_key_reserved",
                "progress_key": BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
            },
            fix_hint=(
                "remove the runner-reserved bootstrap identity progress key "
                "from the adapter result before retrying"
            ),
        )
        err.code = "source_progress_reserved_key"
        raise err
    return normalized


def _parse_bootstrap_identity_ledger(value: Any) -> dict[str, Any] | None:
    """Parse the runner-owned monotonic authority ledger without defaults."""
    if value is None:
        return None
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("bootstrap authority ledger is not valid JSON") from exc
    if payload.get("schema") != _BOOTSTRAP_LEDGER_SCHEMA:
        raise ValueError("bootstrap authority ledger schema is invalid")
    if payload.get("status") not in _BOOTSTRAP_LEDGER_STATUSES:
        raise ValueError("bootstrap authority ledger status is invalid")
    generation = payload.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int):
        raise ValueError("bootstrap authority ledger generation is invalid")
    if generation < 1:
        raise ValueError("bootstrap authority ledger generation is invalid")
    for key in ("nonce", "authority_fingerprint", "binding_fingerprint"):
        if not isinstance(payload.get(key), str) or not payload[key]:
            raise ValueError(f"bootstrap authority ledger {key} is invalid")
    terminal_cursor_fingerprint = payload.get("terminal_cursor_fingerprint")
    if payload.get("status") == "exact":
        if (
            not isinstance(terminal_cursor_fingerprint, str)
            or not terminal_cursor_fingerprint
        ):
            raise ValueError(
                "exact bootstrap authority ledger terminal cursor fingerprint "
                "is invalid"
            )
    elif terminal_cursor_fingerprint is not None:
        raise ValueError(
            "active bootstrap authority ledger has terminal cursor fingerprint"
        )
    transition_identity = payload.get("transition_identity")
    required_transition_fields = {"admission_id", "run_id", "idempotency_key"}
    if not isinstance(transition_identity, Mapping):
        raise ValueError("bootstrap authority ledger transition identity is invalid")
    if set(transition_identity) != required_transition_fields or any(
        not isinstance(transition_identity.get(key), str)
        or not transition_identity[key]
        for key in required_transition_fields
    ):
        raise ValueError("bootstrap authority ledger transition identity is invalid")
    return payload


def _bootstrap_identity_ledger_entry(
    authority: Mapping[str, Any],
    *,
    status: str,
    transition_identity: Mapping[str, str] | None = None,
    terminal_cursor: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the independent high-water mark for one cursor authority."""
    if status not in _BOOTSTRAP_LEDGER_STATUSES:
        raise ValueError("bootstrap authority ledger status is invalid")
    entry = {
        "schema": _BOOTSTRAP_LEDGER_SCHEMA,
        "status": status,
        "generation": int(authority["generation"]),
        "nonce": str(authority["nonce"]),
        "authority_fingerprint": identity_fingerprint(authority),
        "binding_fingerprint": identity_fingerprint(
            authority.get("bindings") or {},
        ),
        "transition_identity": dict(
            transition_identity or authority.get("transition_identity") or {}
        ),
    }
    if status == "exact":
        if terminal_cursor is None:
            raise ValueError("exact bootstrap ledger requires terminal cursor")
        entry["terminal_cursor_fingerprint"] = identity_fingerprint(
            terminal_cursor
        )
    elif terminal_cursor is not None:
        raise ValueError("active bootstrap ledger cannot bind terminal cursor")
    return entry


def _bootstrap_transition_identity(
    context: "SourceRunContext | None",
) -> dict[str, str] | None:
    """Return the accepted-transition identity; never invent ambient values."""
    if context is None:
        return None
    values = {
        "admission_id": context.admission_id,
        "run_id": context.run_id,
        "idempotency_key": context.idempotency_key,
    }
    if any(not isinstance(value, str) or not value for value in values.values()):
        return None
    return {key: str(value) for key, value in values.items()}


def _timing_label_segment(value: Any) -> str:
    normalized = "".join(
        char if char.isalnum() or char in {"_", "-"} else "_"
        for char in str(value or "").strip()
    ).strip("_")
    return normalized or "unnamed"


def _timing_detail_is_duration(label: str) -> bool:
    normalized = str(label).strip().lower()
    segments = [part for part in normalized.split(".") if part]
    if not normalized or normalized == "total" or "total" in segments:
        return False

    if normalized.startswith("node_change_class.") or normalized.startswith(
        "node_change_fields."
    ):
        return False

    flag_or_count_segments = {
        "count",
        "deleted",
        "direct_count",
        "dropped",
        "external",
        "failed",
        "fast_path",
        "inserted",
        "left_count",
        "load_mode",
        "location_only",
        "metadata_only",
        "mixed_row_batch",
        "mixed_single_row_batch",
        "replacement_count",
        "reused",
        "right_count",
        "row_count",
        "semantic_or_identity",
        "single_row_fast_path",
        "skipped",
        "trusted",
    }
    if any(segment in flag_or_count_segments for segment in segments):
        return False

    flag_or_count_suffixes = (
        "_count",
        "_counts",
        "_dropped",
        "_fast_path",
        "_fast_path_miss",
        "_miss",
        "_reused",
        "_skip",
        "_skipped",
        "_threshold",
    )
    allowed_duration_labels = {
        "delta_counts",
        "load_incident_edges",
        "missing_edge_endpoints",
    }
    if normalized not in allowed_duration_labels and any(
        segment.endswith(flag_or_count_suffixes) for segment in segments
    ):
        return False

    flag_or_count_prefixes = (
        "chronos_delta_counts.",
        "noop_filter.",
        "transaction.delete_edges.skipped_",
        "transaction.delete_incident_edges.skipped_",
        "transaction.delete_nodes.skipped_",
    )
    if normalized.startswith(flag_or_count_prefixes):
        return False

    return True


_RECORD_SET_REF_SCHEMA = "okg.source_runner.record_set_ref.v1"
_CURSOR_BLOB_REF_SCHEMA = "okg.source_runner.cursor_blob_ref.v1"
_CURSOR_BLOB_STORAGE = "okg.source_cursor_blobs"
_DEFAULT_INLINE_RECORD_SET_MAX_BYTES = 1_000_000
# Fixed source-tail record fields; caller metadata may not overwrite them.
_TAIL_RESERVED_KEYS = frozenset({
    "schema", "correlation", "phase", "authority_ordinal", "verified",
    "status", "elapsed_ms", "exception_class", "sqlstate",
})
_DEFAULT_INLINE_CURSOR_FIELD_MAX_BYTES = 256_000
_DEFAULT_RECORD_SET_JSONB_MAX_BYTES = 64 * 1024 * 1024

# Commit-witness (PACT route-all-writers-through-transform-ir): the source runner
# stamps every fact it emits with the ingestion transform program's checksum, which
# resolves in okg.ir_programs. Content-addressed -> one shared witness.
_INGESTION_PROGRAM = ingestion_program()
_INGESTION_CHECKSUM = _INGESTION_PROGRAM.checksum()
# Position of `op` in the tuples `_node_row` / `_edge_row` build (the COPY
# column order of okg.node_facts / okg.edge_facts).
_NODE_ROW_OP = 9
_EDGE_ROW_OP = 13


# The (profile, mode) pairs the runner has a retraction policy for, and each
# profile's whole-scope mode, both derived from the one per-profile table in
# `okg.substrate.source_run_modes`. live_overlay never emits retractions.
_POLICY_MODE: dict[tuple[str, str], PolicyRunMode] = {
    (profile, mode): PolicyRunMode(mode)
    for profile, modes in PROFILE_RUN_MODES.items()
    for mode in sorted(modes.accepted)
}

_AUTHORITATIVE_REPLAY_MODE: dict[str, str] = {
    profile: modes.whole_scope for profile, modes in PROFILE_RUN_MODES.items()
}


def profile_native_mode(entry: SourceEntry, mode: str) -> str:
    """Translate a whole-scope request into the source profile's own mode.

    `scope_complete` is how `okg ingest` and `okg provision` ask for a
    source's whole scope, and it is the only whole-scope mode the
    `discovery_crawl` profile knows. The runner refuses a retraction under
    any (profile, mode) pair it has no policy for, so a `reference_catalog`,
    `mutable_api` or `append_feed` source handed `scope_complete` failed the
    first run that had to delete a row -- strict sources on every plain
    ingest (issue #2476), sources on the older registry contract whenever a
    record went missing (issues #2367, #2501). Any source that declares a
    profile (`source_class`) therefore gets that profile's whole-scope mode,
    whatever its admission contract. A mode the profile already accepts is
    never changed, and a source that declares no profile keeps the
    requested mode. An operator's explicit `--mode` is never translated
    (see `IngestOptions.run_mode_explicit`); an incompatible explicit mode
    reaches the runner's refusal, whose fix hint names the profile's mode.
    """
    return resolve_whole_scope_request(getattr(entry, "source_class", None), mode)


# COPY in chunks of this many rows so memory is bounded on
# large adapter runs. Tuned for typical COPY wire framing; not
# performance-critical below ~10K rows per chunk.
COPY_CHUNK_ROWS = 10_000

# A complete graph write-set is only an optimization hint for the downstream
# Chronos dirty-scope calculation.  Sources that opt into the narrowly audited
# adapter-guaranteed identity policy below may emit millions of facts; retaining
# every written node/edge identity would defeat the runner's chunked ingestion
# boundary.  Keep at most one COPY-sized hint, then discard it atomically so the
# publisher takes its existing conservative full-scope fallback rather than a
# misleading partial hint.
MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES = COPY_CHUNK_ROWS

_RUN_WIDE_IDENTITY_POLICY_DEFAULT = "runner_dedup"
_RUN_WIDE_IDENTITY_POLICY_UNIQUE = "adapter_guaranteed_unique"
_RUN_WIDE_IDENTITY_POLICIES = frozenset(
    {
        _RUN_WIDE_IDENTITY_POLICY_DEFAULT,
        _RUN_WIDE_IDENTITY_POLICY_UNIQUE,
    }
)

# Skipping cross-chunk runner dedupe is correctness-sensitive.  An adapter
# attribute alone is not authority: bind the exception to the exact registered
# source, executable profile, and machine-derived emission implementation that
# was reviewed for unique, role-aware IDs.
_ADAPTER_GUARANTEED_UNIQUE_BINDINGS = MappingProxyType(
    {
        (
            "ancient-greece.zenodo-10060888",
            "archaeos-tuff",
            "sources.adapters.science.archaeos_tuff",
            "ArchaeosTuffSource",
            "reference_catalog",
        ): "0b95d92eef51b61a9bc1f776abd7ead5b341c8e9806e12c9ce5e688514a02e0d",
    }
)


# Change-probe skip budget (issue #323): force a real read after this many
# consecutive probe short-circuits, as defense-in-depth against a probe
# false-negative (a collision the sound-token discipline is meant to prevent).
# This bounds the blast radius of any probe bug; it does NOT substitute for the
# delete-reconcile obligation (a cursor read still does not observe deletes —
# that is the readiness-side max-staleness degradation). Env-overridable.
def _probe_skip_budget() -> int:
    raw = os.environ.get("OKG_PROBE_SKIP_BUDGET")
    if raw is None:
        return 64
    try:
        value = int(raw)
    except ValueError:
        return 64
    return value if value > 0 else 64


def _change_probe_gate_enabled() -> bool:
    """The change-probe short-circuit is on by default. Set
    `OKG_DISABLE_CHANGE_PROBE_GATE=1` to force every source to read (the
    microbench's probe-forced-off discrimination arm, and an operator escape
    hatch). The probe still runs to keep the cursor token fresh; only the
    short-circuit is disabled."""
    return os.environ.get("OKG_DISABLE_CHANGE_PROBE_GATE") not in {
        "1", "true", "TRUE", "yes", "YES", "on", "ON",
    }


_PROBE_SKIP_COUNT_KEY = "change_probe_skips"

#: `applied_identity_acknowledgeable` verdicts that accept the applied token.
APPLIED_IDENTITY_POST_PROBE_EQUAL = "post_probe_equal"
APPLIED_IDENTITY_SEALED_INPUT_ADVANCED = "sealed_snapshot_input_advanced"


def applied_identity_acknowledgeable(
    *,
    applied_raw_token: Any,
    applied_token_authority: str,
    applied_token_scope: str,
    applied_token_binding: str,
    pre_token: Any,
    post_token: Any,
    pipeline_stable: bool,
    replay_proof: bool,
    normalizes_replay_probe: bool,
) -> tuple[bool, str]:
    """Decide whether a completed read may stamp its applied token.

    Returns ``(accepted, reason)``. The applied token must be exact and
    global, the post-run probe must have produced a token, the pipeline
    snapshot must still be current, and replay (when required) must be
    proven. Then:

    - a post-run token equal to the applied token is accepted, as before;
    - a different post-run token is accepted only when the run declared
      ``applied_token_binding == "sealed_snapshot"`` (the token was computed
      from a snapshot taken before reading, so it stays true for the facts
      emitted) AND the run is not normalizing an authoritative replay probe,
      whose durable token is re-read after the run and so must match. The
      moved input is then seen as a change on the next poll;
    - a different post-run token is refused when the pre-read probe token
      (``pre_token``, when one exists) equals the post-run token: the input
      did not move, so an applied token that differs from both was not
      computed from that input.

    Every other case is refused.
    """
    if not (
        applied_token_authority == "exact"
        and applied_token_scope == "global"
        and isinstance(applied_raw_token, Mapping)
        and bool(applied_raw_token)
    ):
        return False, "applied_token_not_exact_global"
    if not post_token:
        return False, "post_probe_unavailable"
    if not pipeline_stable:
        return False, "pipeline_snapshot_unstable"
    if not replay_proof:
        return False, "authoritative_replay_unproven"
    if tokens_equal(applied_raw_token, post_token):
        return True, APPLIED_IDENTITY_POST_PROBE_EQUAL
    if pre_token and tokens_equal(pre_token, post_token):
        return False, "applied_token_diverges_without_input_change"
    if normalizes_replay_probe:
        return False, "replay_normalization_requires_equal_post_probe"
    if applied_token_binding != APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT:
        return False, "post_probe_differs_without_sealed_snapshot"
    try:
        validate_token_composition(post_token)
    except ChangeProbeError:
        return False, "post_probe_token_malformed"
    return True, APPLIED_IDENTITY_SEALED_INPUT_ADVANCED


def applied_identity_refusal(
    *,
    source_name: str,
    reason: str,
    exact_applied_snapshot: bool,
    pipeline_snapshot_stable: bool,
    authoritative_replay_proof: bool,
    applied_token_binding: str,
    applied_raw_token: Any,
    post_token: Any,
) -> SubstrateError:
    """Build the refusal for a run whose applied identity was not accepted.

    ``SubstrateError.__str__`` prints only the message, so every input to
    the decision is in the message as well as in ``data``.
    """
    applied_fingerprint = (
        identity_fingerprint(applied_raw_token) if applied_raw_token else None
    )
    post_fingerprint = (
        identity_fingerprint(post_token) if post_token else None
    )
    return SubstrateError(
        "source run cannot acknowledge change identity: "
        f"source={source_name} "
        f"reason={reason} "
        f"exact_applied_snapshot={exact_applied_snapshot} "
        f"pipeline_snapshot_stable={pipeline_snapshot_stable} "
        f"authoritative_replay_proof={authoritative_replay_proof} "
        f"applied_token_binding={applied_token_binding} "
        f"applied_token={applied_fingerprint} "
        f"post_run_token={post_fingerprint}",
        data={
            "source_name": source_name,
            "reason": reason,
            "exact_applied_snapshot": exact_applied_snapshot,
            "pipeline_snapshot_stable": pipeline_snapshot_stable,
            "authoritative_replay_proof": authoritative_replay_proof,
            "applied_token_binding": applied_token_binding,
            "applied_token_fingerprint": applied_fingerprint,
            "post_run_token_fingerprint": post_fingerprint,
        },
        fix_hint=(
            "return an exact global applied_raw_token from the "
            "bytes/rows/objects emitted (declare applied_token_binding="
            "'sealed_snapshot' only when it was computed from a snapshot "
            "taken before reading) and, for replay, a completed "
            "record-set-proven global result"
        ),
    )


def _adapter_run_kwargs(
    adapter: SourceAdapter,
    *,
    mode: str,
    cursor: Optional[dict[str, Any]],
    since_progress: Optional[dict[str, str]],
    sync_scope: Optional[dict[str, Any]],
    change_probe_verdict: Optional[ProbeVerdict] = None,
    authoritative_replay: bool = False,
    source_has_published_facts: Optional[Callable[[], bool]] = None,
) -> dict[str, Any]:
    """Return only the runner kwargs supported by this adapter.

    Older adapters predate `cursor`, `since_progress`, or `sync_scope`.
    The runner used to discover support by calling `adapter.run(...)` and
    catching `TypeError`, but that also swallowed real adapter bugs and could
    silently retry without cursor state. Signature inspection preserves legacy
    adapter compatibility without turning internal exceptions into full
    re-emits.

    `source_has_published_facts` is stricter than the rest: it is passed
    only to an adapter whose `run` names it, never through `**kwargs` or
    to a `run` whose signature cannot be read. A wrapper that forwards
    `**kwargs` to a stricter inner `run` would otherwise hand the inner
    adapter a keyword it rejects. It is a callable, evaluated only when
    the adapter names the keyword, so adapters that never read it do not
    pay for its graph query.
    """

    available = {
        "mode": mode,
        "cursor": cursor,
        "since_progress": since_progress,
        "sync_scope": sync_scope,
        "change_probe_verdict": change_probe_verdict,
        "authoritative_replay": authoritative_replay,
    }
    try:
        signature = inspect.signature(adapter.run)
    except (TypeError, ValueError):
        return available
    parameters = signature.parameters
    if any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    ):
        selected = dict(available)
    else:
        selected = {
            name: value
            for name, value in available.items()
            if name in parameters
        }
    # Issue #2892 review: whether the live graph (or, with Chronos writes
    # off, the fact log) holds any row from this source. Unlike the
    # cursor, a reset or stripped cursor cannot hide it, so an adapter
    # can tell "never published" from "lost its record set" before it
    # reports an empty complete scope. Only an explicitly named keyword
    # parameter receives it (see the docstring).
    named = parameters.get("source_has_published_facts")
    if source_has_published_facts is not None and named is not None and (
        named.kind in (
            inspect.Parameter.KEYWORD_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ):
        selected["source_has_published_facts"] = source_has_published_facts()
    return selected


def _callable_accepts_kwarg(fn: Any, name: str) -> bool:
    """Whether `fn` takes keyword `name` (explicitly or through **kwargs)."""
    try:
        parameters = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return name in parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )


def _adapter_accepts_run_kwarg(adapter: SourceAdapter, name: str) -> bool:
    try:
        signature = inspect.signature(adapter.run)
    except (TypeError, ValueError):
        return True
    parameters = signature.parameters
    if any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    ):
        return True
    return name in parameters


def _adapter_requires_live_call_boundary(
    adapter: SourceAdapter,
    *,
    executable_profile: str | None = None,
) -> bool:
    explicit = getattr(adapter, "requires_live_call_authorization", None)
    if explicit is not None:
        return bool(explicit)
    profile = executable_profile
    if profile is None:
        profile = str(getattr(adapter, "profile", ""))
    return profile in {
        "live_overlay",
        "mutable_api",
    }


def upsert_source_progress(
    *,
    dsn: str,
    deployment: str | None,
    source_name: str,
    progress: Mapping[str, str],
) -> None:
    """Write diagnostic `okg.source_progress` rows for one source.

    Lives here, beside :meth:`SourceRunner._upsert_progress`, because this
    module is the registered direct writer of that table
    (`runtime_authority_manifest`); a second writer elsewhere in the
    substrate would put the ledger back out of one place.

    Unlike the method, this borrows and RETURNS a connection. Callers outside
    a source run used to build a throwaway `SourceRunner` and call the
    private method, which stashes its connection on the instance for `run()`
    to release in a `finally` -- and `run()` was never entered, so the
    connection came back only when the orphaned object was finalized. Once a
    borrow also holds a general admission slot, an unreleased one is a slot
    leaked out of the ingest budget on every source run.
    """
    if not deployment or not source_name or not progress:
        return
    from okg.substrate.db.pool import GENERAL, pooled_connection

    rows = [
        (deployment, source_name, key, value)
        for key, value in progress.items()
    ]
    with pooled_connection(dsn, purpose=GENERAL) as conn:
        with conn.transaction(), conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (deployment, source_name, key)
                  DO UPDATE SET
                    value      = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                rows,
            )


RECORD_SET_COVERAGE_MARKER = "record_set_coverage"
# `_record_set_stale_graph_nodes` / `_record_set_stale_graph_edges`
# read stale rows through this sample cap.
RECORD_SET_STALE_SAMPLE_LIMIT = 10


def record_set_coverage_diagnosis(
    *,
    missing_node_count: int,
    missing_edge_count: int,
    stale_node_count: int,
    stale_edge_count: int,
    stale_counts_exact: bool = True,
) -> dict[str, Any]:
    """Classify a record-set coverage failure and pick the repair advice.

    The coverage guards fail when EITHER side is non-empty, but the two
    sides call for opposite repairs:

    * ``missing_*`` — identities this run declared that the graph does not
      hold. The adapter under-emitted against its own cursor, so replaying
      the source from a reset cursor genuinely re-emits source truth.
    * ``stale_*`` — source-owned rows already in the graph that this run
      did NOT re-emit. The run's scope did not cover what is already
      there. A cursor reset cannot add coverage, and on a backlogged
      source it makes things worse by discarding backlog progress.

    Proven empirically on 2026-08-24 against a fresh database with the
    same command shape and opposite outcomes: ``--reset-cursor`` repaired
    ``git-history`` (git refs are a stable snapshot; missing-dominant) and
    changed nothing for ``agent-sessions`` (live agent transcripts are
    appended DURING the run, so the run's scope grows underneath it;
    stale-dominant — a live-data race, not a stale cursor).

    Returns the structured classification that both the ``data=`` payload
    and the ``fix_hint`` are built from, so the two never disagree.
    """
    missing_total = int(missing_node_count) + int(missing_edge_count)
    stale_total = int(stale_node_count) + int(stale_edge_count)
    # The chronos guard reads stale rows through a ``LIMIT 10`` sample, so
    # a full sample means the real count is only a lower bound. Never let
    # a capped lower bound read as "missing dominates" — that is exactly
    # the reading that sends an operator to a harmful cursor reset.
    stale_bounded = bool(stale_counts_exact) or stale_total == 0

    if missing_total and stale_total:
        dominant = (
            "missing"
            if stale_bounded and missing_total > stale_total
            # Tie goes to ``stale``: never lead an operator toward a
            # cursor reset that can discard backlog progress without
            # adding coverage.
            else "stale"
        )
        coverage_class = "mixed"
    elif missing_total:
        dominant = "missing"
        coverage_class = "missing"
    elif stale_total:
        dominant = "stale"
        coverage_class = "stale"
    else:
        dominant = "none"
        coverage_class = "none"

    missing_phrase = (
        f"{missing_total} declared identities "
        f"({missing_node_count} nodes, {missing_edge_count} edges) "
        "are absent from the graph"
    )
    stale_qualifier = "" if stale_bounded else "at least "
    stale_phrase = (
        f"{stale_qualifier}{stale_total} source-owned rows "
        f"({stale_qualifier}{stale_node_count} nodes, "
        f"{stale_qualifier}{stale_edge_count} edges) "
        "already in the graph were not re-emitted by this run"
    )

    if coverage_class == "missing":
        fix_hint = (
            f"missing coverage: {missing_phrase}. Rerun the source with an "
            "explicit cursor reset repair so the adapter re-emits source "
            "truth before the watermark advances."
        )
    elif coverage_class == "stale":
        fix_hint = (
            f"stale coverage: {stale_phrase}, so this run's scope did not "
            "cover the scope already in the graph. A cursor reset is NOT "
            "the fix here — it adds no coverage and discards backlog "
            "progress on a backlogged source. Find why the run's scope "
            "was narrower than the graph (a live-data race where the "
            "source's inputs are appended during the run, a narrowed path "
            "or record filter, or a run that ended early), then rerun over "
            "a scope that covers the existing rows."
        )
    elif dominant == "missing":
        fix_hint = (
            f"mixed coverage, missing-dominant: {missing_phrase}, and "
            f"{stale_phrase}. A cursor reset repairs only the missing "
            "half; it adds no coverage for the stale rows. Reset the "
            "cursor, then confirm the stale count reached zero before "
            "trusting the watermark."
        )
    elif dominant == "stale":
        fix_hint = (
            f"mixed coverage, stale-dominant: {stale_phrase}, and "
            f"{missing_phrase}. Do NOT start with a cursor reset — it "
            "adds no coverage for the stale majority and discards backlog "
            "progress. Fix the run's scope first, then reset the cursor "
            "only if identities are still missing."
        )
    else:
        fix_hint = (
            "coverage check reported no missing and no stale identities; "
            "re-run the guard and report the payload if it still fails."
        )

    return {
        "missing_node_count": int(missing_node_count),
        "missing_edge_count": int(missing_edge_count),
        "stale_node_count": int(stale_node_count),
        "stale_edge_count": int(stale_edge_count),
        "missing_total": missing_total,
        "stale_total": stale_total,
        "coverage_class": coverage_class,
        "dominant": dominant,
        "stale_counts_exact": stale_bounded,
        "fix_hint": fix_hint,
    }


OWNED_ELSEWHERE_SAMPLE_LIMIT = 10


def _coverage_fix_hint(
    diagnosis_hint: str,
    owned_elsewhere: list[dict[str, Any]],
    *,
    owned_elsewhere_capped: bool,
    only_cause: bool,
) -> str:
    """The coverage fix hint, led by missing nodes another source holds (#3591).

    A node held under another source is not repaired by a cursor reset: the
    rerun takes the row back and the other source then fails. When those
    nodes are the whole refusal, the generic hint is dropped.
    """
    if not owned_elsewhere:
        return diagnosis_hint
    named = ", ".join(
        f"{row['node_id']} (held by "
        f"{row['source'] if row['source'] is not None else 'no source'})"
        for row in owned_elsewhere[:3]
    )
    count = (
        f"at least {len(owned_elsewhere)}"
        if owned_elsewhere_capped
        else str(len(owned_elsewhere))
    )
    lead = (
        f"{count} missing node(s) are in the graph under another source, "
        f"e.g. {named}. If more than one source emits such a node and each "
        "declares it authoritative, the last one to write it owns the row "
        "and the others' coverage check fails, depending on run order. "
        "Declare ownership_mode='substitute' for that node in each source "
        "that is not its owner (docs/develop/architecture/pipeline.md, "
        "shared nodes); a cursor reset does not fix this."
    )
    if only_cause:
        return lead
    return f"{lead} For the other missing or stale identities: {diagnosis_hint}"


def record_set_coverage_marker(diagnosis: Mapping[str, Any]) -> str:
    """Render the machine-readable coverage tail carried in the message.

    The structured ``data=`` payload is flattened to ``str(exc)`` by the
    time the source-sync workflow wraps a source failure, so the counts
    ride inside the message text too. Keep this format stable —
    ``okg.substrate.runtime.source_sync_workflow`` parses it back into
    structured fields for the operator-facing workflow error.
    """
    return (
        f"[{RECORD_SET_COVERAGE_MARKER} "
        f"missing_nodes={diagnosis['missing_node_count']} "
        f"missing_edges={diagnosis['missing_edge_count']} "
        f"stale_nodes={diagnosis['stale_node_count']} "
        f"stale_edges={diagnosis['stale_edge_count']} "
        f"class={diagnosis['coverage_class']} "
        f"dominant={diagnosis['dominant']}]"
    )


@dataclass(frozen=True)
class SourceRunContext:
    """Caller-supplied identity for a replay-safe source run."""

    run_id: str
    observed_at: datetime
    admission_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    workflow_id: Optional[str] = None
    producer_profile: Optional["ProducerProfile"] = None

    def __post_init__(self) -> None:
        try:
            normalized = str(uuid.UUID(str(self.run_id)))
        except ValueError as exc:
            raise SubstrateError(
                f"SourceRunContext.run_id must be a UUID, got "
                f"{self.run_id!r}",
                data={"run_id": str(self.run_id)},
            ) from exc
        object.__setattr__(self, "run_id", normalized)
        tz = self.observed_at.tzinfo
        if tz is None or self.observed_at.utcoffset() is None:
            raise SubstrateError(
                "SourceRunContext.observed_at must be timezone-aware",
                data={"observed_at": str(self.observed_at)},
            )
        if self.workflow_id is not None:
            workflow_id = str(self.workflow_id).strip()
            if not workflow_id:
                raise SubstrateError(
                    "SourceRunContext.workflow_id must be non-empty when set",
                    data={"workflow_id": str(self.workflow_id)},
                )
            object.__setattr__(self, "workflow_id", workflow_id)


@dataclass
class RunResult:
    run_id: str
    source_name: str
    mode: str
    started_at: datetime
    finished_at: datetime
    nodes_appended: int
    edges_appended: int
    retractions_node: int
    retractions_edge: int
    next_cursor: Optional[dict[str, Any]] = None
    completed_scope: bool = False
    # Requested mode remains in ``mode`` for scheduling and telemetry. The
    # adapter's authorized structural outcome can differ during a
    # runner-forced replay, so admission must receive it separately.
    effective_mode: Optional[str] = None
    # True only when the runner, rather than the adapter, proved that an
    # append-feed read observed one exact global change identity and produced
    # no graph delta after persisted semantic dedupe. Admission may treat this
    # as ``no_change`` without trusting an adapter-authored mode escalation.
    runner_proven_no_change: bool = False
    # True when the runner acknowledged an adapter's sealed-snapshot applied
    # token although the post-run probe saw a newer input. The facts match
    # the snapshot; the next poll reads the newer input.
    input_advanced_during_run: bool = False
    # `add-source-runner-dedup` (2026-04-25) — counts facts the
    # runner-level dedup pass dropped (by `node_id` for nodes,
    # by `(src, dst, edge_type)` for edges). first_dropped_id
    # is the structural identity of the first dropped fact, so
    # operators can find the offending legacy emit.
    dedupe_drops_node: int = 0
    dedupe_drops_edge: int = 0
    dedupe_first_dropped_id: Optional[str] = None
    # Per `refine-doc-corpus-extraction` (2026-04-26) W7:
    # per-prefix breakdown of the drops above. NodeFact drops
    # count under their `node_id`'s prefix (substring before
    # `:`); EdgeFact drops count under `edge:<edge_type>`. The
    # values sum to `dedupe_drops_node + dedupe_drops_edge`.
    dedupe_drops_by_prefix: dict[str, int] = field(
        default_factory=dict,
    )
    # Persisted-dedupe detail: candidates dropped because only their source
    # revision changed while their content matched the stored row, and kept
    # candidates broken down by the attribute key that differed (keys only).
    dedupe_revision_only_node: int = 0
    dedupe_revision_only_edge: int = 0
    dedupe_kept_by_key: dict[str, int] = field(default_factory=dict)
    # What the persisted dedupe compared candidates against on this run:
    # ``live_branch`` (the current Chronos branch rows), ``fact_log`` (the
    # latest append-only fact rows) or ``none`` (nothing was compared, every
    # candidate was written). None when the run wrote no facts at all.
    dedupe_reference: Optional[str] = None
    # PACT `sanitize-fact-text-at-admission` (2026-07-09, issue
    # #638) — NUL (U+0000) admission normalization. Postgres
    # text/jsonb cannot store NUL, so admission scrubs attr values
    # (each occurrence → U+FFFD, counted in
    # `sanitized_value_count`) and REJECTS facts whose
    # identity-bearing fields carry NUL (counted in
    # `rejected_fact_count`; identity is never silently rewritten).
    # `sanitize_first_rejected_fact` is a printable identity of the
    # first rejected fact for operator triage.
    sanitized_value_count: int = 0
    rejected_fact_count: int = 0
    sanitize_first_rejected_fact: Optional[str] = None
    # ProgressMarkers skipped for NUL in record_id/fingerprint —
    # progress keys are resume identity (rejection rule, not scrub);
    # the marker's record simply re-parses next run.
    rejected_marker_count: int = 0
    sanitize_first_rejected_marker: Optional[str] = None
    # PACT first-class-images (#3046): what the run's image step did --
    # `images_rendered`, `images_reused`, `images_error` (by reason) and
    # `image_bytes_stored`. None when the run saw no file image value.
    image_metrics: Optional[dict[str, Any]] = None
    source_health: Optional[SourceHealth] = None
    admission_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    graph_nodes_by_subtype: dict[str, tuple[str, ...]] = field(
        default_factory=dict,
    )
    graph_node_attr_changes_by_subtype: dict[
        str, dict[str, tuple[str, ...]]
    ] = field(default_factory=dict)
    graph_edges_by_type: dict[str, tuple[int, ...]] = field(
        default_factory=dict,
    )
    graph_edge_table_available: bool = True
    timings_ms: dict[str, float] = field(default_factory=dict)
    trace_id: Optional[str] = None
    telemetry_span_id: Optional[str] = None
    # `deltaflow-source-contracts` (2026-06-13) — validated
    # SourceDeltaBatch evidence for readiness-critical sources:
    # changed/inserted/retracted record ids, affected scopes, and
    # frontiers for the no-change / small-change / delete cases. None
    # when the source is not readiness-critical or the contract could
    # not be formed (the dict then carries an explicit
    # `contract: unavailable` reason rather than silently vanishing).
    source_delta_batch: Optional[dict[str, Any]] = None
    fact_batch_summary: Optional[dict[str, Any]] = None
    terminal_progress: dict[str, str] = field(default_factory=dict)
    clear_replay_required: bool = False
    replay_target: Optional[dict[str, Any]] = None
    expected_last_applied_rev: Optional[dict[str, Any]] = None
    watermark_compare_and_swap: bool = False
    expected_bootstrap_ledger: Optional[dict[str, Any]] = None
    next_bootstrap_ledger: Optional[dict[str, Any]] = None
    # The exact admission/run/idempotency transition was already accepted and
    # persisted.  The reader and all write paths were skipped.
    idempotent_replay: bool = False
    # How a completed streaming run that inferred no positive footprint
    # treated the prior record-set authority (#1859 rule (b)). None when the
    # rule did not apply. ``preserved_empty_stream``: the stream was empty and
    # the prior state was kept. ``preserved_retraction_only_stream``: the
    # stream named only explicit retractions, so exactly those left the prior
    # authority and everything else was kept. ``empty_scope_assertion_ignored``:
    # the reader set ``asserts_empty_scope`` on a run with no whole-scope
    # deletion authority, so the assertion retracted nothing.
    record_set_authority: Optional[str] = None
    # Issue #2818 backstop: the operator's `--allow-full-retraction` record
    # for this run (who, when, which source, and what it let through).
    full_retraction_override: Optional[dict[str, Any]] = None
    # Private transaction-local staging state. It is deliberately omitted from
    # as_dict(), cursors, watermark state, summaries and admission payloads.
    private_authority_candidate: Optional[PrivateAuthorityCandidate] = field(
        default=None, repr=False, compare=False,
    )

    def as_dict(self) -> dict[str, Any]:
        out = {
            "run_id": self.run_id,
            "source_name": self.source_name,
            "mode": self.mode,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "nodes_appended": self.nodes_appended,
            "edges_appended": self.edges_appended,
            "retractions_node": self.retractions_node,
            "retractions_edge": self.retractions_edge,
            "completed_scope": self.completed_scope,
            "next_cursor": self.next_cursor,
            "dedupe_drops_node": self.dedupe_drops_node,
            "dedupe_drops_edge": self.dedupe_drops_edge,
            "dedupe_first_dropped_id": self.dedupe_first_dropped_id,
            "dedupe_drops_by_prefix": dict(self.dedupe_drops_by_prefix),
            "dedupe_revision_only_node": self.dedupe_revision_only_node,
            "dedupe_revision_only_edge": self.dedupe_revision_only_edge,
            "dedupe_kept_by_key": dict(self.dedupe_kept_by_key),
        }
        if self.dedupe_reference is not None:
            out["dedupe_reference"] = self.dedupe_reference
        if self.effective_mode and self.effective_mode != self.mode:
            out["effective_mode"] = self.effective_mode
        if self.runner_proven_no_change:
            out["runner_proven_no_change"] = True
        # Sanitize counts surface only when nonzero (zero-count runs
        # must not add noise — PACT sanitize-fact-text-at-admission).
        if self.sanitized_value_count:
            out["sanitized_value_count"] = self.sanitized_value_count
        if self.rejected_fact_count:
            out["rejected_fact_count"] = self.rejected_fact_count
            if self.sanitize_first_rejected_fact is not None:
                out["sanitize_first_rejected_fact"] = (
                    self.sanitize_first_rejected_fact
                )
        if self.rejected_marker_count:
            out["rejected_marker_count"] = self.rejected_marker_count
            if self.sanitize_first_rejected_marker is not None:
                out["sanitize_first_rejected_marker"] = (
                    self.sanitize_first_rejected_marker
                )
        if self.image_metrics is not None:
            out["images"] = dict(self.image_metrics)
        if self.timings_ms:
            out["timings_ms"] = dict(self.timings_ms)
        if self.trace_id is not None:
            out["trace_id"] = self.trace_id
        if self.telemetry_span_id is not None:
            out["telemetry_span_id"] = self.telemetry_span_id
        if self.source_health is not None:
            out["source_health"] = self.source_health.as_dict()
        if self.admission_id is not None:
            out["admission_id"] = self.admission_id
        if self.idempotency_key is not None:
            out["idempotency_key"] = self.idempotency_key
        graph_write_set = self.graph_write_set_as_dict()
        if graph_write_set is not None:
            out["graph_write_set"] = graph_write_set
        if self.source_delta_batch is not None:
            out["source_delta_batch"] = self.source_delta_batch
        if self.fact_batch_summary is not None:
            out["fact_batch_summary"] = self.fact_batch_summary
        if self.terminal_progress:
            out["terminal_progress"] = dict(self.terminal_progress)
        if self.replay_target is not None:
            out["replay_target"] = dict(self.replay_target)
        out["clear_replay_required"] = self.clear_replay_required
        if self.idempotent_replay:
            out["idempotent_replay"] = True
        if self.record_set_authority is not None:
            out["record_set_authority"] = self.record_set_authority
        if self.full_retraction_override is not None:
            out["full_retraction_override"] = self.full_retraction_override
        return out

    def graph_write_set_as_dict(self) -> dict[str, Any] | None:
        if not self.graph_nodes_by_subtype and not self.graph_edges_by_type:
            return None
        return {
            "schema": "okg.graph_write_set.v1",
            "nodes_by_subtype": {
                subtype: list(node_ids)
                for subtype, node_ids in self.graph_nodes_by_subtype.items()
            },
            "node_attr_changes_by_subtype": {
                subtype: {
                    node_id: list(fields)
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (
                    self.graph_node_attr_changes_by_subtype.items()
                )
            },
            "edges_by_type": {
                edge_type: list(edge_ids)
                for edge_type, edge_ids in self.graph_edges_by_type.items()
            },
            "edge_table_available": self.graph_edge_table_available,
        }


@dataclass(frozen=True)
class PollProbeDecision:
    """Non-acknowledging runner decision consumed by the poll producer."""

    enqueue: bool
    changed: bool
    reason: str
    revision_hint: str
    effective_token: Optional[dict[str, Any]] = None


def persist_source_watermark(
    conn: psycopg.Connection,
    *,
    source_name: str,
    deployment_name: str | None = None,
    next_cursor: Optional[Mapping[str, Any]],
    last_reconcile: Optional[datetime],
    clear_replay_required: bool = False,
    replay_target_fingerprint: str | None = None,
    replay_target_run_id: str | None = None,
    expected_last_applied_rev: Mapping[str, Any] | None = None,
    require_compare_and_swap: bool = False,
    expected_bootstrap_ledger: Mapping[str, Any] | None = None,
    next_bootstrap_ledger: Mapping[str, Any] | None = None,
) -> None:
    """Persist accepted terminal source state on the caller's transaction.

    Admission orchestration calls this immediately before the envelope moves
    to ``accepted`` so cursor/record-set/probe authority cannot become visible
    in the crash window between source execution and terminal admission.
    """
    last_applied_rev = (
        None
        if next_cursor is None
        else json.dumps(dict(next_cursor), default=str)
    )
    with conn.transaction():
        if require_compare_and_swap:
            if not deployment_name:
                raise SubstrateError(
                    "bootstrap identity authority has no deployment ledger scope",
                    data={
                        "source_name": source_name,
                        "reason_code": "authority_ledger_scope_missing",
                    },
                    fix_hint=(
                        "run the source through a deployment-bound runner; "
                        "bootstrap authority cannot use an ambient scope"
                    ),
                )
            if next_bootstrap_ledger is None:
                raise SubstrateError(
                    "bootstrap identity authority has no durable ledger transition",
                    data={
                        "source_name": source_name,
                        "reason_code": "authority_ledger_transition_missing",
                    },
                    fix_hint="repair the runner bootstrap transition before retrying",
                )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT last_applied_rev
                      FROM okg.source_watermarks
                     WHERE source = %s
                     FOR UPDATE
                    """,
                    (source_name,),
                )
                row = cur.fetchone()
                cur.execute(
                    """
                    SELECT value
                      FROM okg.source_progress
                     WHERE deployment = %s
                       AND source_name = %s
                       AND key = %s
                     FOR UPDATE
                    """,
                    (
                        deployment_name,
                        source_name,
                        BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                    ),
                )
                ledger_row = cur.fetchone()
            expected = dict(expected_last_applied_rev or {})
            observed = (
                dict(row[0])
                if row and isinstance(row[0], Mapping)
                else None
            )
            if observed != expected:
                err = SubstrateError(
                    "bootstrap identity authority was already consumed",
                    data={
                        "source_name": source_name,
                        "reason_code": "authority_generation_replayed",
                        "expected_frontier": identity_fingerprint(expected),
                        "observed_frontier": (
                            identity_fingerprint(observed)
                            if observed is not None else None
                        ),
                    },
                    fix_hint=(
                        "reload the current source watermark and retry; do not "
                        "reuse or copy an older bootstrap cursor"
                    ),
                )
                err.code = "bootstrap_identity_authority_replayed"
                raise err
            try:
                observed_ledger = _parse_bootstrap_identity_ledger(
                    ledger_row[0] if ledger_row else None,
                )
            except ValueError as exc:
                err = SubstrateError(
                    "bootstrap identity authority ledger is malformed",
                    data={
                        "source_name": source_name,
                        "reason_code": "authority_ledger_malformed",
                    },
                    fix_hint=(
                        "inspect the runner-owned source progress ledger and "
                        "restore it from trusted evidence; do not copy a cursor"
                    ),
                )
                err.code = "bootstrap_identity_authority_ledger_malformed"
                raise err from exc
            expected_ledger = (
                dict(expected_bootstrap_ledger)
                if expected_bootstrap_ledger is not None else None
            )
            if observed_ledger != expected_ledger:
                err = SubstrateError(
                    "bootstrap identity authority ledger was already consumed",
                    data={
                        "source_name": source_name,
                        "reason_code": "authority_ledger_replayed",
                        "expected_ledger": (
                            identity_fingerprint(expected_ledger)
                            if expected_ledger is not None else None
                        ),
                        "observed_ledger": (
                            identity_fingerprint(observed_ledger)
                            if observed_ledger is not None else None
                        ),
                    },
                    fix_hint=(
                        "reload both the current source watermark and its "
                        "runner-owned authority ledger; do not restore an older cursor"
                    ),
                )
                err.code = "bootstrap_identity_authority_ledger_replayed"
                raise err
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.source_progress
                      (deployment, source_name, key, value, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (deployment, source_name, key) DO UPDATE SET
                      value = EXCLUDED.value,
                      updated_at = EXCLUDED.updated_at
                    """,
                    (
                        deployment_name,
                        source_name,
                        BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                        json.dumps(dict(next_bootstrap_ledger), sort_keys=True),
                    ),
                )
        with conn.cursor() as cur:
            cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev, last_reconcile_at)
            VALUES (%s, COALESCE(%s::jsonb, '{}'::jsonb), %s)
            ON CONFLICT (source) DO UPDATE SET
              last_applied_rev = CASE
                WHEN %s
                 AND (
                   okg.source_watermarks.replay_required
                     ->>'target_fingerprint' IS DISTINCT FROM %s
                   OR okg.source_watermarks.replay_required
                     ->>'run_id' IS DISTINCT FROM %s
                 )
                  THEN okg.source_watermarks.last_applied_rev
                WHEN %s::jsonb IS NULL THEN
                  okg.source_watermarks.last_applied_rev
                ELSE
                  %s::jsonb
              END,
              last_reconcile_at = CASE
                WHEN %s
                 AND (
                   okg.source_watermarks.replay_required
                     ->>'target_fingerprint' IS DISTINCT FROM %s
                   OR okg.source_watermarks.replay_required
                     ->>'run_id' IS DISTINCT FROM %s
                 )
                  THEN okg.source_watermarks.last_reconcile_at
                ELSE COALESCE(
                  EXCLUDED.last_reconcile_at,
                  okg.source_watermarks.last_reconcile_at
                )
              END,
              replay_required = CASE
                WHEN %s
                 AND okg.source_watermarks.replay_required
                       ->>'target_fingerprint' = %s
                 AND okg.source_watermarks.replay_required
                       ->>'run_id' = %s
                  THEN '{}'::jsonb
                ELSE okg.source_watermarks.replay_required
              END,
              replay_required_at = CASE
                WHEN %s
                 AND okg.source_watermarks.replay_required
                       ->>'target_fingerprint' = %s
                 AND okg.source_watermarks.replay_required
                       ->>'run_id' = %s
                  THEN NULL
                ELSE okg.source_watermarks.replay_required_at
              END
            """,
                (
                source_name,
                last_applied_rev,
                last_reconcile,
                clear_replay_required,
                replay_target_fingerprint,
                replay_target_run_id,
                last_applied_rev,
                last_applied_rev,
                clear_replay_required,
                replay_target_fingerprint,
                replay_target_run_id,
                clear_replay_required,
                replay_target_fingerprint,
                replay_target_run_id,
                clear_replay_required,
                replay_target_fingerprint,
                replay_target_run_id,
                ),
            )


@dataclass
class RunPrelude:
    prior_cursor: Optional[dict[str, Any]]
    prior_progress: Optional[dict[str, str]]
    no_prior_facts: bool
    replay_required: Optional[dict[str, Any]] = None
    # Exact durable state observed before an explicit reset hides adapter
    # progress. Bootstrap transitions compare-and-swap against this value.
    persisted_cursor: Optional[dict[str, Any]] = None


@dataclass
class FactLogRows:
    node_rows: list[tuple] = field(default_factory=list)
    edge_rows: list[tuple] = field(default_factory=list)
    marker_rows: list[tuple] = field(default_factory=list)
    nodes_inserted: int = 0
    nodes_retracted: int = 0
    edges_inserted: int = 0
    edges_retracted: int = 0


@dataclass
class GraphWriteSet:
    nodes_by_subtype: dict[str, list[str]] = field(default_factory=dict)
    node_attr_changes_by_subtype: dict[
        str, dict[str, list[str]]
    ] = field(default_factory=dict)
    edges_by_type: dict[str, list[int]] = field(default_factory=dict)
    edge_table_available: bool = True
    timings_ms: dict[str, float] = field(default_factory=dict)


@dataclass
class GraphWriteSetAccumulator:
    """Aggregate graph-write hints, degrading atomically at a size boundary.

    ``max_entries=None`` preserves the historical exact aggregation contract.
    With a limit, crossing it clears every accumulated identity and ignores all
    later writes.  Callers therefore receive either a complete hint or no hint;
    a partial dirty scope must never escape.
    """

    max_entries: int | None = None
    available: bool = True
    retained_entries: int = 0
    nodes_by_subtype: dict[str, list[str]] = field(default_factory=dict)
    node_attr_changes_by_subtype: dict[
        str, dict[str, list[str]]
    ] = field(default_factory=dict)
    edges_by_type: dict[str, list[int]] = field(default_factory=dict)
    edge_table_available: bool = True

    @staticmethod
    def _entry_count(write_set: GraphWriteSet) -> int:
        return (
            sum(len(node_ids) for node_ids in write_set.nodes_by_subtype.values())
            + sum(
                len(node_changes)
                + sum(len(fields) for fields in node_changes.values())
                for node_changes in (
                    write_set.node_attr_changes_by_subtype.values()
                )
            )
            + sum(len(edge_ids) for edge_ids in write_set.edges_by_type.values())
        )

    def observe(self, write_set: GraphWriteSet) -> None:
        if not self.available:
            return
        incoming_entries = (
            self._entry_count(write_set)
            if self.max_entries is not None
            else 0
        )
        if (
            self.max_entries is not None
            and self.retained_entries + incoming_entries > self.max_entries
        ):
            self.available = False
            self.retained_entries = 0
            self.nodes_by_subtype.clear()
            self.node_attr_changes_by_subtype.clear()
            self.edges_by_type.clear()
            self.edge_table_available = False
            return
        self.retained_entries += incoming_entries
        for subtype, node_ids in write_set.nodes_by_subtype.items():
            self.nodes_by_subtype.setdefault(subtype, []).extend(node_ids)
        for subtype, node_changes in (
            write_set.node_attr_changes_by_subtype.items()
        ):
            target = self.node_attr_changes_by_subtype.setdefault(subtype, {})
            for node_id, fields in node_changes.items():
                target.setdefault(node_id, []).extend(fields)
        for edge_type, edge_ids in write_set.edges_by_type.items():
            self.edges_by_type.setdefault(edge_type, []).extend(edge_ids)
        self.edge_table_available = (
            self.edge_table_available and write_set.edge_table_available
        )


@dataclass
class FlushCounts:
    nodes_inserted: int = 0
    nodes_retracted: int = 0
    edges_inserted: int = 0
    edges_retracted: int = 0


# What the persisted dedupe compares a run's candidates against (reported as
# ``RunResult.dedupe_reference``).
DEDUPE_REFERENCE_LIVE_BRANCH = "live_branch"
DEDUPE_REFERENCE_FACT_LOG = "fact_log"
DEDUPE_REFERENCE_NONE = "none"

@dataclass
class PersistedDedupeDetail:
    """Why the persisted (chronos-graph) dedupe kept or dropped candidates.

    ``revision_only_nodes`` / ``revision_only_edges`` count candidates whose
    content equals the stored row and whose lineage differs only in its source
    revision; they are dropped as unchanged for every revision kind.
    ``kept_by_key`` counts kept candidates by the top-level attribute key that
    differed, with the lineage envelope reported as
    ``nomos_source_lineage.source_revision`` when only its revision differed
    and an edge column as ``edge.provenance`` / ``edge.confidence``. Keys
    only, never values.
    """
    revision_only_nodes: int = 0
    revision_only_edges: int = 0
    kept_by_key: dict[str, int] = field(default_factory=dict)


def sanitize_report_fields(result: Any) -> dict[str, Any]:
    """NUL-admission counts for the per-source ingest report.

    Per PACT `sanitize-fact-text-at-admission`: the runner's
    `RunResult` carries `sanitized_value_count` (attr-value NUL
    occurrences replaced with U+FFFD) and `rejected_fact_count`
    (facts skipped for NUL in identity-bearing fields). Surface
    them in the per-source report ONLY when nonzero so clean runs
    add no noise.
    """
    out: dict[str, Any] = {}
    sanitized = getattr(result, "sanitized_value_count", 0) or 0
    rejected = getattr(result, "rejected_fact_count", 0) or 0
    rejected_markers = getattr(result, "rejected_marker_count", 0) or 0
    if sanitized:
        out["sanitized_value_count"] = int(sanitized)
    if rejected:
        out["rejected_fact_count"] = int(rejected)
        first = getattr(result, "sanitize_first_rejected_fact", None)
        if first:
            out["sanitize_first_rejected_fact"] = str(first)
    if rejected_markers:
        out["rejected_marker_count"] = int(rejected_markers)
        first_marker = getattr(
            result,
            "sanitize_first_rejected_marker",
            None,
        )
        if first_marker:
            out["sanitize_first_rejected_marker"] = str(first_marker)
    return out


def dedupe_report_fields(result: Any) -> dict[str, Any]:
    """Persisted-dedupe detail for the per-source report, only when nonzero.

    ``dedupe_revision_only_*`` counts unchanged facts whose source revision
    alone changed; ``dedupe_kept_by_key`` names the attribute keys (never
    values) that kept a candidate out of the dedupe, so a zero-addition
    republish explains which attribute moved. ``dedupe_reference`` says what
    the candidates were compared against (``live_branch``, ``fact_log`` or
    ``none``); it is reported whenever the run set it.
    """
    out: dict[str, Any] = {}
    reference = getattr(result, "dedupe_reference", None)
    if isinstance(reference, str):
        out["dedupe_reference"] = reference
    nodes = int(getattr(result, "dedupe_revision_only_node", 0) or 0)
    edges = int(getattr(result, "dedupe_revision_only_edge", 0) or 0)
    if nodes or edges:
        out["dedupe_revision_only_node"] = nodes
        out["dedupe_revision_only_edge"] = edges
    kept = getattr(result, "dedupe_kept_by_key", None)
    if isinstance(kept, Mapping) and kept:
        out["dedupe_kept_by_key"] = {str(k): int(v) for k, v in kept.items()}
    return out


@dataclass
class DedupeDropSummary:
    in_chunk_nodes: int = 0
    in_chunk_edges: int = 0
    persisted_nodes: int = 0
    persisted_edges: int = 0
    first_dropped: Optional[str] = None
    by_prefix: dict[str, int] = field(default_factory=dict)
    persisted_detail: PersistedDedupeDetail = field(default_factory=PersistedDedupeDetail)


def _has_file_image_value(attrs: Any) -> bool:
    from okg.substrate.images.values import IMAGE_TAG, KIND_FILE, iter_image_values

    return any(
        site.value.get(IMAGE_TAG) == KIND_FILE
        for site in iter_image_values(attrs)
    )


def image_report_fields(result: Any) -> dict[str, Any]:
    """The run's image-step counts for the per-source report.

    PACT first-class-images (#3046). Present only when the run saw a file
    image value, so a source without images adds no key.
    """
    metrics = getattr(result, "image_metrics", None)
    if not isinstance(metrics, Mapping):
        return {}
    return {"images": dict(metrics)}


@dataclass
class SanitizeSummary:
    """Per-chunk NUL-admission normalization counts.

    Per PACT `sanitize-fact-text-at-admission` (issue #638):
    `sanitized_value_count` counts NUL→U+FFFD replacements applied
    to attr values; `rejected_fact_count` counts facts skipped
    because an identity-bearing field carried NUL (identity is
    never silently rewritten). `first_rejected` is a printable
    identity of the first rejected fact for operator triage.
    `rejected_marker_count` counts ProgressMarkers skipped for NUL
    in `record_id`/`fingerprint` — progress keys are resume
    identity, so they get the rejection rule; the only cost is a
    re-parse of that record next run.
    """

    sanitized_value_count: int = 0
    rejected_fact_count: int = 0
    first_rejected: Optional[str] = None
    rejected_marker_count: int = 0
    first_rejected_marker: Optional[str] = None


@dataclass
class _SanitizeWarnBudget:
    """Per-RUN cap on per-fact NUL-rejection warning detail.

    The cap previously lived inside `_sanitize_facts`, i.e. per
    CHUNK — a pathological many-chunk stream could still emit 5
    detail lines per chunk. One budget object is created per run and
    threaded through `_flush_chunk`; when it exhausts, a single
    notice marks the suppression and the end-of-run summary still
    carries the full counts.
    """

    remaining: int = 5
    notice_emitted: bool = False


@dataclass
class FlushResult:
    counts: FlushCounts = field(default_factory=FlushCounts)
    dedupe: DedupeDropSummary = field(default_factory=DedupeDropSummary)
    graph_write_set: GraphWriteSet = field(default_factory=GraphWriteSet)
    sanitize: SanitizeSummary = field(default_factory=SanitizeSummary)


@dataclass
class _StreamFootprintAccumulator:
    """Canonical last-write-wins identities from one completed fact stream."""

    source_name: str
    nodes: dict[str, RecordNode] = field(default_factory=dict)
    edges: dict[tuple[str, str, str], EdgeKey] = field(default_factory=dict)
    explicit_node_retractions: set[str] = field(default_factory=set)
    explicit_edge_retractions: set[tuple[str, str, str]] = field(
        default_factory=set,
    )

    def observe(self, fact: NodeFact | EdgeFact) -> None:
        op = str(getattr(fact, "op", "I")).upper()
        if isinstance(fact, NodeFact):
            node_id = fact.node_id
            if op == "R":
                self.nodes.pop(node_id, None)
                self.explicit_node_retractions.add(node_id)
                return
            prior = self.nodes.get(node_id)
            if (
                prior is not None
                and prior.ownership_mode != fact.ownership_mode
            ):
                raise SubstrateError(
                    "SourceRunner: completed source stream declares "
                    "conflicting node ownership modes",
                    data={
                        "source_name": self.source_name,
                        "node_id": node_id,
                        "first_ownership_mode": prior.ownership_mode,
                        "conflicting_ownership_mode": fact.ownership_mode,
                    },
                    fix_hint=(
                        "declare each canonical node identity as either "
                        "authoritative or substitute consistently"
                    ),
                )
            self.nodes[node_id] = RecordNode(
                node_id=node_id,
                subtype=fact.subtype,
                source_record_id=dict(fact.source_record_id),
                source_revision=dict(fact.source_revision),
                ownership_mode=fact.ownership_mode,
            )
            self.explicit_node_retractions.discard(node_id)
            return

        identity = (fact.src, fact.edge_type, fact.dst)
        if op == "R":
            self.edges.pop(identity, None)
            self.explicit_edge_retractions.add(identity)
            return
        self.edges[identity] = EdgeKey(
            src=fact.src,
            dst=fact.dst,
            edge_type=fact.edge_type,
            attrs=dict(fact.attrs),
            provenance=fact.provenance,
            confidence=fact.confidence,
            source_record_id=dict(fact.source_record_id),
            source_revision=dict(fact.source_revision),
        )
        self.explicit_edge_retractions.discard(identity)

    def record_set(self) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for node_id, node in sorted(self.nodes.items()):
            key = "__stream__:node:" + identity_fingerprint({
                "node_id": node_id,
            })
            records[key] = RecordEmission(primary=node)
        for identity, edge in sorted(self.edges.items()):
            key = "__stream__:edge:" + identity_fingerprint({
                "src": identity[0],
                "edge_type": identity[1],
                "dst": identity[2],
            })
            records[key] = RecordEmission(edges=[edge])
        return RecordSet(records=records)


def freeze_replacement_keys(
    keys: Iterable[str] | None,
) -> frozenset[str] | None:
    """Read an adapter's replacement keys once into a frozenset.

    Issue #2838 review. ``SourceRun.record_set_replacement_keys`` is typed
    ``Optional[Iterable[str]]``; a one-shot iterable read by the first check
    would leave every later check, and the prior merge, an empty set. None
    stays None: it means "no replacement slice", not "an empty one".
    """
    if keys is None:
        return None
    return frozenset(str(key) for key in keys)


def refuse_runner_reserved_replacement_keys(
    *,
    source_name: str,
    replacement_keys: Iterable[str],
) -> None:
    """Refuse a narrowed run that declares a runner-reserved record key.

    Issue #2838. ``__chronos_retained__`` and ``__stream__`` records are
    minted by the runner, never by an adapter: the retained-graph bootstrap
    adds its adopted live-graph rows to the PRIOR record set, never to the
    replacement keys, and ``_record_set_with_prior`` carries them forward. So
    no legitimate narrowed run lists one. Their fingerprints are
    deterministic, so an adapter that did could retract adopted records far
    outside its scope. Refuse before the slice is merged or anything is
    retracted.
    """
    reserved = sorted(
        str(key)
        for key in replacement_keys
        if record_key_is_runner_reserved(str(key))
    )
    if not reserved:
        return
    raise SubstrateError(
        "scoped run declares runner-reserved record keys as replacements",
        data={
            "source_name": source_name,
            "reason_code": "scoped_replacement_retained_key_forbidden",
            "reserved_replacement_keys": reserved[:20],
            "reserved_replacement_key_count": len(reserved),
        },
        fix_hint=(
            "declare only the adapter's own record keys; records under "
            "__chronos_retained__ or __stream__ belong to the runner and are "
            "replaced only by a whole-source run"
        ),
    )


def _coverage_edges(session: Any, columns: str | None = None) -> str:
    """Relation record-set coverage reads edges from: graph plus waiting rows."""
    from okg.substrate.graph_store_internal.edge_deferrals import (
        edges_relation_sql,
    )

    if columns is None:
        return edges_relation_sql(session)
    return edges_relation_sql(session, columns)


class SourceRunner:
    """Drives a SourceAdapter and persists its facts.

    Construct once per deployment + source. `run()` is the main
    entry point; subsequent calls reuse the same runner but
    produce fresh run_ids.
    """

    @contextmanager
    def _tail_phase(self, phase: str, /, **metadata: Any):
        """Small stderr records survive blocked/failed compensation, never payloads.

        `phase` is positional-only so a caller cannot collide with it, and
        caller metadata can neither overwrite a fixed field nor smuggle a
        non-scalar value into the record.
        """
        try:
            started = time.perf_counter()
        except Exception:
            started = None

        def emit(status: str, exc: Exception | None = None) -> None:
            # Allocation and sink failures are particularly plausible on the
            # failure path being diagnosed. Never replace the business outcome.
            try:
                if not hasattr(self, "_tail_diagnostic_id"):
                    self._tail_diagnostic_id = uuid.uuid4().hex
                # Caller metadata is bounded to short scalars and cannot
                # overwrite a fixed field, so the record stays forgery-free and
                # always JSON-serializable before it is stored or written.
                safe_metadata = {
                    key: value
                    for key, value in metadata.items()
                    if key not in _TAIL_RESERVED_KEYS
                    and isinstance(value, (bool, int, float, str, type(None)))
                    and not (isinstance(value, str) and len(value) > 128)
                }
                event = {
                    "schema": "okg.source_tail.v1",
                    "correlation": self._tail_diagnostic_id,
                    "phase": phase,
                    "authority_ordinal": min(getattr(self, "_tail_authority_ordinal", 0), 1000000),
                    "verified": bool(getattr(self, "_tail_verified", False)),
                    **safe_metadata,
                    "status": status,
                    "elapsed_ms": (round((time.perf_counter() - started) * 1000, 3)
                                   if started is not None else None),
                }
                if exc is not None:
                    sqlstate = getattr(exc, "sqlstate", None)
                    if not (isinstance(sqlstate, str) and len(sqlstate) == 5
                            and all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" for c in sqlstate)):
                        sqlstate = None
                    event.update(exception_class=type(exc).__name__[:64], sqlstate=sqlstate)
                    if getattr(self, "tail_failure_diagnostic", None) is None:
                        self.tail_failure_diagnostic = event
                # No log-level filtering; stdout remains the CLI JSON surface.
                sys.stderr.write(json.dumps(event, sort_keys=True) + "\n")
                sys.stderr.flush()
            except Exception:
                # Includes MemoryError; never swallow cancellation/BaseException.
                # A lost record is disclosed rather than hidden: the run result
                # carries `source_tail_observer_failures`. The primary evidence
                # (`tail_failure_diagnostic`) is assigned above, before any
                # formatting or I/O, so a sink failure cannot erase it.
                try:
                    self._tail_observer_failures = (
                        getattr(self, "_tail_observer_failures", 0) + 1
                    )
                except Exception:
                    # Last resort under memory exhaustion: the counter itself is
                    # unavailable. Raising here would replace the run outcome
                    # with an observer error, which this observer must never do.
                    pass

        emit("started")
        try:
            yield
        except Exception as exc:
            emit("failed", exc)
            raise
        else:
            emit("completed")

    def __init__(
        self,
        dsn: str,
        *,
        source_name: str,
        canonicalizers: Optional[list] = None,
        deployment_name: Optional[str] = None,
        graph_store: Any | None = None,
        graph_branch: str = "default",
        write_chronos_graph: Optional[bool] = None,
        write_fact_log: Optional[bool] = None,
        chronos_interval_continuation_percent: Optional[int] = None,
        pii_posture: Optional["PiiPosture"] = None,
        executable_profile: str | None = None,
        registry_name: str | None = None,
        ownership_id: str | None = None,
        registry_binding_fingerprint: str | None = None,
        deletion_semantics: str | None = None,
        source_revision_kind: str | None = None,
        record_authority_format: str = "legacy_v1",
        allow_unconsumed_indexed_authority: bool = False,
        defer_bootstrap_lock_release: bool = False,
        compensate_on_failure: bool = True,
        images_config: Optional[ImagesConfig] = None,
        full_retraction_override: Optional[FullRetractionOverride] = None,
    ) -> None:
        self.dsn = dsn
        # PACT first-class-images (#3046): the deployment's
        # `agent_surface.images` settings. Ingest reads `render` and
        # `file_max_bytes`; a caller that passes nothing gets the defaults a
        # deployment without the block gets.
        self.images_config: ImagesConfig = (
            images_config if images_config is not None
            else ImagesConfig.default()
        )
        # The run's image step; built per run in `_run_impl` once the
        # adapter's `SourceRun` names its `image_root`, closed with the
        # run's connection in `_release_conn`.
        self._image_completer: Optional[ImageCompleter] = None
        # Issue #2818 backstop: see `full_retraction_guard`.
        self.full_retraction_override = full_retraction_override
        self._full_retraction_guard = FullRetractionGuard(source_name)
        # Issue #1431: a run that fails after the streaming loop has already
        # flushed chunks must not leave those chunks applied. Callers that own
        # the compensation themselves — the ingest orchestrator, which also
        # restores watermarks and progress — pass False so the run is not
        # compensated twice.
        self.compensate_on_failure = compensate_on_failure
        self.source_name = source_name
        # `add-substrate-id-canonicalization` (2026-04-25):
        # rewrite raw `<family>:<repo>:<rest>` IDs to canonical
        # form before facts hit node_facts / edge_facts. Single
        # chokepoint; downstream readers never see raw IDs.
        # Empty list → no rewriting (every id passes through
        # unchanged).
        self.canonicalizers: list = list(canonicalizers or [])
        # `add-substrate-incremental-publish` (2026-04-28):
        # when set, the runner reads `okg.source_progress`
        # rows for (deployment_name, source_name) before each
        # adapter invocation and passes them as
        # `since_progress=` (with TypeError fallback for
        # adapters that don't accept the kwarg). After a
        # successful run, calls the adapter's optional
        # `progress_after_run()` and upserts the returned
        # dict. None disables the wiring (graceful for
        # callers that don't know their deployment name).
        self.deployment_name: Optional[str] = deployment_name
        # `bind-at-rest-anonymization`: the resolved at-rest privacy posture for
        # THIS source. None or a store_raw posture ⇒ the transform is a no-op
        # (every existing source, unchanged). A not-store-raw posture makes
        # `_apply_pii_posture` pseudonymize/mask each fact's PII attrs at
        # admission, before canonicalize/lineage/COPY. Resolve it with
        # `okg.substrate.ingest.pii_posture.resolve_source_pii_posture`.
        self.pii_posture: Optional["PiiPosture"] = pii_posture
        self._configured_pii_posture: Optional["PiiPosture"] = pii_posture
        authority_values = (
            executable_profile,
            registry_name,
            ownership_id,
        )
        if any(value is not None for value in authority_values) and not (
            all(
                isinstance(value, str) and bool(value)
                for value in authority_values
            )
            and isinstance(registry_binding_fingerprint, str)
            and bool(registry_binding_fingerprint)
        ):
            raise SubstrateError(
                "registered SourceRunner requires a complete frozen binding",
                data={
                    "source_name": source_name,
                    "has_executable_profile": bool(executable_profile),
                    "has_registry_name": bool(registry_name),
                    "has_ownership_id": bool(ownership_id),
                    "has_binding_fingerprint": bool(
                        registry_binding_fingerprint
                    ),
                },
            )
        if ownership_id is not None and ownership_id != source_name:
            raise SubstrateError(
                "frozen source ownership does not match runner source",
                data={
                    "source_name": source_name,
                    "ownership_id": ownership_id,
                    "registry_name": registry_name,
                },
            )
        self.executable_profile = executable_profile
        # The registry's declared ``source_revision_kind``. Only ``content_hash``
        # lets the persisted dedupe treat a revision that differs in
        # non-content fields (a per-run id) as unchanged content.
        self.source_revision_kind = source_revision_kind
        self._persisted_dedupe_detail = PersistedDedupeDetail()
        self.registry_name = registry_name
        self.ownership_id = ownership_id
        self.registry_binding_fingerprint = (
            registry_binding_fingerprint or ""
        )
        self.deletion_semantics = deletion_semantics
        if (
            not isinstance(record_authority_format, str)
            or record_authority_format not in {"legacy_v1", "indexed_v2"}
        ):
            raise SubstrateError(
                "unknown record authority format",
                data={
                    "source_name": source_name,
                    "record_authority_format": record_authority_format,
                },
            )
        self.record_authority_format = record_authority_format
        # Deliberately NOT reachable from the source registry or any
        # environment variable: an indexed_v2 run whose authority nothing reads
        # back loses deletion synthesis, so only a caller that knows it is
        # exercising storage alone can enable it.
        self.allow_unconsumed_indexed_authority = bool(
            allow_unconsumed_indexed_authority
        )
        # Admission finalization persists the authoritative watermark after
        # ``run()`` returns. Its caller can keep the source-scoped advisory
        # lease across that boundary so a second consumer cannot write facts
        # in the gap before compare-and-swap. Direct callers leave this false:
        # they persist the watermark inside ``run()`` and release normally.
        self.defer_bootstrap_lock_release = defer_bootstrap_lock_release
        self.graph_store = graph_store
        self.graph_branch = graph_branch
        self.chronos_interval_continuation_percent = (
            chronos_interval_continuation_percent
        )
        self.write_chronos_graph = (
            write_chronos_graph
            if write_chronos_graph is not None
            else os.environ.get("OKG_CHRONOS_GRAPH", "1")
            not in {"0", "false", "FALSE", "no", "NO"}
        )
        self.write_fact_log = (
            write_fact_log
            if write_fact_log is not None
            else os.environ.get("OKG_WRITE_FACT_LOG", "0")
            in {"1", "true", "TRUE", "yes", "YES"}
        )
        self._owned_graph_store: Any | None = None
        self._chronos_graph_unavailable_logged = False
        self._active_run_context: SourceRunContext | None = None
        self._active_run_mode = "cursor"
        # Issue #1431: the run's retraction verdict, resolved once at run
        # start. `None` means "not yet resolved for this run"; the FactBatch
        # validator is told the resolved value instead of a blanket `True`.
        self._active_retractions_allowed: bool | None = None
        # Prior `okg.source_progress` values for the marker keys this run has
        # written, captured before the first write of each key so a failed run
        # can be put back exactly (issue #1431). `None` means "absent before".
        # The source's watermark + progress as they stood before this run,
        # so a failed run can be put back (issue #1431). None when the runner
        # does not own its compensation or has no deployment scope.
        self._source_state_snapshot: Any | None = None
        # Issue #2993: this run's run-base checkpoint id, cut before its
        # first graph write (`_take_run_base`), or None.
        self._active_run_base: str | None = None
        self._run_base_attempted = False
        # The connection holding the run's liveness lock while a run base
        # exists (`_take_run_base`, `_release_run_liveness`).
        self._run_liveness_conn: Any | None = None
        self._run_applied_chunks = False
        self._run_marked_replay_fence = False
        # Set at the exact point this run aborts for a forbidden retraction,
        # or for a positive fact after `asserts_empty_scope` - the only
        # failures the runner compensates (issue #1431, #1859 rule (b)).
        self._run_refused_retraction = False
        self._active_run_id: str | None = None
        self._active_sync_scope: dict[str, Any] | None = None
        self._active_pipeline_snapshot: PipelineIdentitySnapshot | None = None
        self._active_replay_target: dict[str, Any] | None = None
        self._active_clear_replay_required = False
        self._active_watermark_cas_required = False
        self._active_watermark_cas_expected: dict[str, Any] | None = None
        self._active_bootstrap_ledger: dict[str, Any] | None = None
        self._active_bootstrap_ledger_expected: dict[str, Any] | None = None
        self._active_bootstrap_ledger_next: dict[str, Any] | None = None
        self._active_bootstrap_lock_key: str | None = None
        self._active_private_authority_candidate: PrivateAuthorityCandidate | None = None
        self._fact_batch_application: Any | None = None
        # ``run()`` resets this per run; a direct flush outside ``run()``
        # compares the way an ordinary (non-replay) run does.
        self._dedupe_reference: str = self._ordinary_dedupe_reference()
        # Shared connection across all helpers in one run(). Acquired
        # from the process-shared ConnectionPool at run() entry,
        # returned at exit. Each helper used to open its own
        # psycopg.connect — 6 fresh handshakes per run, ~66 ms of
        # pure TCP+TLS+auth overhead. With the pool, the first run
        # pays one handshake; subsequent runs in the same process
        # get a warm conn from the pool (sub-millisecond).
        self._shared_conn: Optional[psycopg.Connection] = None
        # Context manager for the pool acquisition — kept on the
        # instance so `_run_impl` can stash the conn and `run()`'s
        # `finally` releases it cleanly.
        self._pool_cm = None
        # `add-delta-proportional-substrate-work` follow-up: cache the
        # per-subtype volatile fields per SourceRunner instance. Without
        # this, `_dedupe_against_persisted` queries `okg.subtypes`
        # for every chunk (measured 35 ms p50 with 25 ms in the SQL
        # execute on the flamegraph). Catalog metadata doesn't change
        # within one ingest run; load once.
        self._volatile_keys_cache: Optional[dict[str, frozenset[str]]] = None

    def _policy_profile(self, adapter: SourceAdapter) -> str:
        """Return frozen registry authority or the legacy instance profile."""
        if self.executable_profile is not None:
            return self.executable_profile
        return str(getattr(adapter, "profile", ""))

    def _refuse_bootstrap_identity(
        self,
        reason_code: str,
        message: str,
        *,
        fix_hint: str,
        binding: str | None = None,
        expected: Any = None,
        observed: Any = None,
    ) -> None:
        data: dict[str, Any] = {
            "source_name": self.source_name,
            "reason_code": reason_code,
        }
        if binding is not None:
            data["binding"] = binding
            data["expected"] = expected
            data["observed"] = observed
        err = SubstrateError(message, data=data, fix_hint=fix_hint)
        err.code = f"bootstrap_identity_{reason_code}"
        raise err

    def _bootstrap_cursor_semantics(self, adapter: Any) -> str:
        value = getattr(adapter, "bootstrap_cursor_semantics", None)
        if not isinstance(value, str) or not value:
            self._refuse_bootstrap_identity(
                "cursor_semantics_missing",
                "bootstrap identity has no explicit cursor semantics",
                fix_hint=(
                    "declare a stable bootstrap_cursor_semantics contract on "
                    "the source adapter before retrying"
                ),
            )
        return value

    def _bootstrap_binding_snapshot(
        self,
        adapter: Any,
        *,
        requested_sync_scope: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        # A worker wakeup that names no scope key reads the whole source, so
        # it may drain the global backlog like an event-less run (#3314).
        if source_sync_scope_narrows(requested_sync_scope):
            self._refuse_bootstrap_identity(
                "scope_mismatch",
                "global bootstrap identity cannot be consumed by a scoped run",
                fix_hint=(
                    "drain the global bounded backlog before issuing scoped "
                    "source work"
                ),
                binding="source_scope",
                expected=_BOOTSTRAP_GLOBAL_SCOPE,
                observed="scoped_mapping",
            )
        required = {
            "deployment_name": self.deployment_name,
            "registry_name": self.registry_name,
            "ownership_id": self.ownership_id,
            "registry_binding_fingerprint": (
                self.registry_binding_fingerprint
            ),
            "executable_profile": self.executable_profile,
        }
        missing = sorted(
            key
            for key, value in required.items()
            if not isinstance(value, str) or not value
        )
        if missing:
            self._refuse_bootstrap_identity(
                "frozen_binding_missing",
                "bootstrap identity requires a complete frozen source binding",
                fix_hint=(
                    "construct the source through a frozen registry admission "
                    "binding; bootstrap authority cannot use runner defaults"
                ),
                binding="missing_fields",
                expected="resolved_non_empty",
                observed=missing,
            )
        if self.ownership_id != self.source_name:
            self._refuse_bootstrap_identity(
                "source_mismatch",
                "bootstrap source ownership does not match the runner source",
                fix_hint="repair the frozen source ownership binding",
                binding="ownership_id",
                expected=self.source_name,
                observed=self.ownership_id,
            )
        snapshot = self._active_pipeline_snapshot
        if snapshot is None or not snapshot.is_intact():
            self._refuse_bootstrap_identity(
                "pipeline_unstable",
                "bootstrap pipeline identity is unavailable or unstable",
                fix_hint=(
                    "restore the immutable pipeline collaborators and retry; "
                    "do not reset source progress for a transient pipeline failure"
                ),
            )
        branch_incarnation = (
            self._chronos_branch_incarnation()
            if self.write_chronos_graph else "graph_writes_disabled"
        )
        if not branch_incarnation:
            self._refuse_bootstrap_identity(
                "branch_incarnation_missing",
                "bootstrap graph branch incarnation is unavailable",
                fix_hint=(
                    "restore the target Chronos branch and retry; do not reuse "
                    "authority from a deleted branch"
                ),
            )
        return {
            "source_name": self.source_name,
            "deployment_name": self.deployment_name,
            "registry_name": self.registry_name,
            "ownership_id": self.ownership_id,
            "source_scope": _BOOTSTRAP_GLOBAL_SCOPE,
            "graph_branch": self.graph_branch,
            "graph_branch_incarnation": branch_incarnation,
            "graph_writes_enabled": bool(self.write_chronos_graph),
            "registry_binding_fingerprint": (
                self.registry_binding_fingerprint
            ),
            "executable_profile": self.executable_profile,
            "pipeline_fingerprint": snapshot.fingerprint,
            "cursor_semantics": self._bootstrap_cursor_semantics(adapter),
        }

    def _bootstrap_authority_from_cursor(
        self,
        cursor: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        try:
            authority = bootstrap_authority_from_cursor(cursor)
        except ValueError as exc:
            self._refuse_bootstrap_identity(
                "authority_malformed",
                "stored bootstrap identity authority is malformed or tampered",
                fix_hint=(
                    "inspect the stored watermark and deliberately use "
                    "--reset-cursor only when discarding incompatible progress"
                ),
                binding="authority_envelope",
                expected="valid_self_bound_frontier",
                observed=str(exc),
            )
        return authority

    def _validate_prior_bootstrap_authority(
        self,
        adapter: Any,
        *,
        prior_cursor: Mapping[str, Any] | None,
        requested_sync_scope: Mapping[str, Any] | None,
        bindings: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        authority = self._bootstrap_authority_from_cursor(prior_cursor)
        if authority is None:
            return None
        if not cursor_has_outstanding_work(prior_cursor):
            self._refuse_bootstrap_identity(
                "cursor_frontier_mismatch",
                "bootstrap authority is not attached to bounded resume work",
                fix_hint=(
                    "inspect the altered watermark and deliberately reset only "
                    "if its incompatible progress must be discarded"
                ),
            )
        if source_sync_scope_narrows(requested_sync_scope):
            self._bootstrap_binding_snapshot(
                adapter,
                requested_sync_scope=requested_sync_scope,
            )
        observed_bindings = authority.get("bindings")
        if observed_bindings != bindings:
            differing = sorted(
                key
                for key in set(bindings) | set(observed_bindings or {})
                if bindings.get(key) != (observed_bindings or {}).get(key)
            )
            self._refuse_bootstrap_identity(
                "binding_mismatch",
                "stored bootstrap identity does not match the active source binding",
                fix_hint=(
                    "review the changed binding and use --reset-cursor only if "
                    "the prior resume frontier is intentionally incompatible"
                ),
                binding="changed_fields",
                expected=sorted(bindings),
                observed=differing,
            )
        try:
            validate_token_composition(authority.get("raw_token"))
        except ChangeProbeError as exc:
            self._refuse_bootstrap_identity(
                "raw_token_malformed",
                "stored bootstrap raw snapshot identity is unsound",
                fix_hint=(
                    "repair or revoke the stored bootstrap watermark; an "
                    "under-specified token cannot authorize resume"
                ),
                binding="raw_token",
                expected="sound_change_probe_token",
                observed=str(exc),
            )
        return authority

    @staticmethod
    def _completed_scope_health_is_acceptable(
        *health_values: SourceHealth | None,
    ) -> bool:
        accepted = {"ok", "not_applicable", "skipped_optional"}
        return all(
            health is None or health.status in accepted
            for health in health_values
        )

    def _completed_scope_deletion_authorized(
        self,
        *,
        mode: str,
        source_run: Any,
        sync_scope: Mapping[str, Any] | None,
        acquire_health: SourceHealth | None,
    ) -> bool:
        return (
            mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
            and source_run.run_mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
            and source_run.completed_scope is True
            # Issue #3314: a worker wakeup (reconcile, poll) carries a
            # payload that names no scope key. It narrows nothing, so it
            # must not withhold the authority an event-less run gets.
            and not source_sync_scope_narrows(sync_scope)
            and self._completed_scope_health_is_acceptable(
                source_run.health,
                acquire_health,
            )
        )

    def _scoped_record_set_deletion_authority(
        self,
        *,
        mode: str,
        source_run: Any,
        sync_scope: Mapping[str, Any] | None,
        acquire_health: SourceHealth | None,
    ) -> str | None:
        """Name the deletion authority a NARROWED run carries, or None.

        Issue #2145. ``_completed_scope_deletion_authorized`` refuses every
        run the caller narrowed with a sync_scope. That term belongs to the
        streamed footprint below, which infers absence from the fact stream
        and would read a slice as if it were the whole source. An explicit
        RecordSet is not an inference, and a narrowed run can carry one of
        two exact authorities:

        * ``replacement_slice`` -- ``record_set_replacement_keys`` names the
          prior keys this run replaces, ``_record_set_with_prior`` carries
          every other prior record forward and refuses a slice that carries
          a record it never declared, so the diff can retract only the
          declared keys. The declaration itself is the adapter's, so the
          caller must first put it through
          ``_verify_scoped_replacement_keys_within_scope``: a narrowed run
          that declares the whole source and returns an empty slice would
          otherwise empty the source; or
        * ``merged_prior`` -- the adapter says it folded the durable prior
          into its slice and returns ``completed_scope=True``. That is a
          claim, not a proof. On a narrowed run ``completed_scope`` means
          "I walked my slice" just as readily as "I merged the prior", so
          this kind grants nothing on its own: the caller must first put it
          through ``_verify_scoped_merged_record_set_covers_prior``, which
          compares the declared set against the durable prior. Trusting the
          claim let a narrowed run that returned an empty set retract the
          whole source -- the issue #2122 failure mode reached through the
          scoped door.

        Both kinds are exceptions granted to a NARROWED run only. Without a
        sync_scope a run either proves complete authority through
        ``_completed_scope_deletion_authorized`` or carries none.

        Discarding both left a scoped re-ingest unable to retract the file it
        had just been told about: the retired nodes stayed readable and the
        graph kept serving records whose source file was gone.
        """
        if source_run.record_set is None or source_run.preserves_prior_record_set:
            return None
        if not source_sync_scope_narrows(sync_scope):
            return None
        if not self._completed_scope_health_is_acceptable(
            source_run.health,
            acquire_health,
        ):
            return None
        if source_run.record_set_replacement_keys is not None:
            return "replacement_slice"
        if (
            source_run.completed_scope is True
            and mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
            and source_run.run_mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
        ):
            return "merged_prior"
        return None

    @staticmethod
    def _scope_key_covers_record_key(scope_key: str, record_key: str) -> bool:
        """Report whether `scope_key` governs `record_key`.

        Scope keys are paths or record ids, so an exact match settles most
        of it; a directory scope key also governs everything beneath it.
        The comparison is deliberately narrow: under-matching only makes
        the coverage check below stricter, while over-matching would excuse
        a prior record the declared set really did drop.
        """
        return _scope_key_covers_path(scope_key, record_key)

    @staticmethod
    def _declared_scope_keys(
        sync_scope: Mapping[str, Any] | None,
    ) -> list[str]:
        """Read the scope keys out of a sync_scope the caller handed us.

        Issue #2145 review N4. Reading `scope_keys` straight off the mapping
        iterates a bare string character by character, which silently turns
        one path into a pile of one-letter scope keys. The source-sync
        contract already owns the canonical reading, including the `keys` and
        `affected_scopes` spellings, so use it.
        """
        return [str(key) for key in contract_scope_keys(sync_scope)]

    def _record_key_within_scope(
        self,
        record_key: str,
        scope_keys: Sequence[str],
    ) -> bool:
        """Report whether the declared scope governs `record_key`."""
        if record_key_is_scope_independent(record_key):
            return True
        return any(
            self._scope_key_covers_record_key(scope_key, record_key)
            for scope_key in scope_keys
        )

    @staticmethod
    def _record_emission_fingerprint(emission: RecordEmission) -> str:
        """Canonical identity of one record's declared footprint.

        Compared, not stored. Descendants and edges are order-insensitive
        sets of identities, so they are sorted before serialization; a
        reordered but otherwise identical emission must not read as altered.
        """
        cursor = emission.to_cursor()
        return json.dumps(
            {
                "primary": cursor.get("primary"),
                "descendants": sorted(
                    json.dumps(item, sort_keys=True, default=str)
                    for item in cursor.get("descendants") or []
                ),
                "edges": sorted(
                    json.dumps(item, sort_keys=True, default=str)
                    for item in cursor.get("edges") or []
                ),
            },
            sort_keys=True,
            default=str,
        )

    def _scoped_replacement_keys_outside_scope(
        self,
        *,
        sync_scope: Mapping[str, Any] | None,
        replacement_keys: set[str],
    ) -> list[str]:
        """Name the declared replacement keys the sync_scope does not govern.

        Issue #2145 review B1. `record_set_replacement_keys` is the exact
        list of prior keys the diff is allowed to retract, and it arrives
        from the adapter. Bounding retraction by a bound the adapter alone
        chose is no bound at all: a run narrowed to one file that declares
        the whole source and returns an empty slice retracts the whole
        source. The runner holds the sync_scope it handed out, so it checks
        the declaration against that. Every key returned here must pass
        ``_verify_scoped_replacement_keys_within_scope`` against the durable
        prior before the slice is merged, or the run is refused.

        The one allowance is the synthetic-key namespace documented on
        `SCOPE_INDEPENDENT_RECORD_KEY_PATTERN`, which the runner recognises
        by spelling rather than on an adapter's word.
        """
        scope_keys = self._declared_scope_keys(sync_scope)
        return sorted(
            key
            for key in replacement_keys
            if not self._record_key_within_scope(key, scope_keys)
        )

    def _verify_scoped_replacement_keys_within_scope(
        self,
        *,
        sync_scope: Mapping[str, Any] | None,
        outside_keys: Sequence[str],
        prior_record_set: RecordSet,
        declared_record_set: RecordSet,
    ) -> None:
        """Refuse an out-of-scope replacement key the runner cannot verify.

        Issue #1539. The 09-22 bound (issue #2145 review B1) refused every
        out-of-scope key, which also refused the one legitimate case: a
        file that imports a changed file must re-point its references,
        because symbol ids carry line numbers. An out-of-scope key is now
        accepted only when ``review_scoped_dependent_replacements`` shows,
        against the durable prior the merge is about to use, that its
        replacement leaves the record's nodes identical and adds or removes
        only resolution edges pointing directly into the scope. Anything
        else is refused before any retraction is computed.

        A refused key with no qualifying prior edge has no relation to the
        scope at all and keeps the 09-22 reason code; a real dependent whose
        replacement goes further gets ``scoped_dependent_replacement_
        unverified``.
        """
        if not outside_keys:
            return
        scope_keys = self._declared_scope_keys(sync_scope)
        review = review_scoped_dependent_replacements(
            prior=prior_record_set,
            current=declared_record_set,
            keys=outside_keys,
            scope_keys=scope_keys,
        )
        if not review.unverified:
            return
        refused = sorted(review.unverified)
        unverified_dependents = [
            key for key in refused if key in review.dependents
        ]
        reason_code = (
            "scoped_dependent_replacement_unverified"
            if unverified_dependents
            else "scoped_replacement_keys_outside_scope"
        )
        raise SubstrateError(
            (
                "scoped run replaces a dependent record outside its scope "
                "beyond the edges that point into the scope"
                if unverified_dependents
                else "scoped run declares replacement record keys outside "
                "the scope it was handed"
            ),
            data={
                "source_name": self.source_name,
                "reason_code": reason_code,
                "scope_keys": scope_keys,
                "out_of_scope_replacement_keys": refused[:20],
                "out_of_scope_replacement_key_count": len(refused),
                "unverified_dependent_keys": unverified_dependents[:20],
                "unverified_reasons": {
                    key: review.unverified[key][:5]
                    for key in refused[:20]
                },
            },
            fix_hint=(
                "declare only the record keys that lie inside the requested "
                "sync_scope, or replace an out-of-scope record only by "
                "re-pointing its references/calls edges into the scope; "
                "otherwise run the source without a sync_scope so it can "
                "prove whole-source authority"
            ),
        )

    def _verify_scoped_merged_record_set_covers_prior(
        self,
        *,
        sync_scope: Mapping[str, Any] | None,
        prior_record_set: RecordSet,
        declared_record_set: RecordSet,
    ) -> None:
        """Refuse a merged-prior claim the durable prior contradicts.

        Issue #2145 review B1. ``completed_scope=True`` on a narrowed run
        only becomes whole-source deletion authority once the declared set
        is shown to cover every prior record that lies OUTSIDE the declared
        scope. A genuinely merged set contains all of them by construction;
        a set that merely walked its own slice does not, and trusting it
        retracted records the run never looked at.

        Coverage alone is not enough. An out-of-scope prior key the declared
        set carries must also match the prior record exactly, or the merged
        claim rewrites a record the run never walked.
        """
        scope_keys = self._declared_scope_keys(sync_scope)
        out_of_scope_prior = [
            key
            for key in prior_record_set.records
            if not self._record_key_within_scope(key, scope_keys)
        ]
        uncovered = sorted(
            key
            for key in out_of_scope_prior
            if key not in declared_record_set.records
        )
        if uncovered:
            self._refuse_scoped_merged_record_set(
                message=(
                    "scoped run claims a merged record set that does not "
                    "cover the durable prior outside its scope"
                ),
                reason_code="scoped_merged_record_set_incomplete",
                scope_keys=scope_keys,
                detail_field="uncovered_prior_record_keys",
                detail_keys=uncovered,
                fix_hint=(
                    "either fold the durable prior record set into the "
                    "returned set, or declare the keys this run replaces "
                    "through record_set_replacement_keys instead of "
                    "completed_scope"
                ),
            )
        # Issue #2145 review B1. Presence is not preservation. A merged set
        # that CARRIES an out-of-scope prior key but rewrites its footprint
        # retracts nothing yet still replaces a record the run never walked
        # -- the same out-of-scope write the replacement-slice door refuses.
        # Compare the declared record against the durable prior and refuse
        # any difference.
        altered = sorted(
            key
            for key in out_of_scope_prior
            if key in declared_record_set.records
            and self._record_emission_fingerprint(
                declared_record_set.records[key]
            ) != self._record_emission_fingerprint(
                prior_record_set.records[key]
            )
        )
        if altered:
            self._refuse_scoped_merged_record_set(
                message=(
                    "scoped run claims a merged record set that alters a "
                    "durable prior record outside its scope"
                ),
                reason_code=(
                    "scoped_merged_record_set_altered_outside_scope"
                ),
                scope_keys=scope_keys,
                detail_field="altered_prior_record_keys",
                detail_keys=altered,
                fix_hint=(
                    "carry every out-of-scope prior record forward exactly "
                    "as the prior holds it, or declare the keys this run "
                    "replaces through record_set_replacement_keys"
                ),
            )

    def _refuse_scoped_merged_record_set(
        self,
        *,
        message: str,
        reason_code: str,
        scope_keys: list[str],
        detail_field: str,
        detail_keys: list[str],
        fix_hint: str,
    ) -> None:
        """Raise one shape of merged-prior refusal."""
        raise SubstrateError(
            message,
            data={
                "source_name": self.source_name,
                "reason_code": reason_code,
                "scope_keys": scope_keys,
                detail_field: detail_keys[:20],
                f"{detail_field}_count": len(detail_keys),
            },
            fix_hint=fix_hint,
        )

    def _acquire_conn(self) -> psycopg.Connection:
        """Return the shared connection for this run.

        Lazily acquires one from the pool on first call; subsequent
        calls in the same run() invocation reuse it.
        """
        if self._shared_conn is None or self._shared_conn.closed:
            from okg.substrate.db.pool import GENERAL, pooled_connection

            # GENERAL: a source holds this connection for its whole run, so
            # it is exactly the borrower the publish-cycle reservation exists
            # to bound.
            self._pool_cm = pooled_connection(self.dsn, purpose=GENERAL)
            self._shared_conn = self._pool_cm.__enter__()
        return self._shared_conn

    def _acquire_bootstrap_authority_lock(self, adapter: Any) -> None:
        """Serialize adapters that can consume bootstrap generations."""
        semantics = getattr(adapter, "bootstrap_cursor_semantics", None)
        if not isinstance(semantics, str) or not semantics:
            return
        lock_key = f"okg.source.bootstrap_identity:{self.source_name}"
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (lock_key,),
            )
            acquired = bool(cur.fetchone()[0])
        if not acquired:
            err = SubstrateError(
                "bootstrap identity authority is being consumed concurrently",
                data={
                    "source_name": self.source_name,
                    "reason_code": "authority_consumer_concurrent",
                },
                fix_hint=(
                    "wait for the active source run to finish, then reload the "
                    "current watermark before retrying"
                ),
            )
            err.code = "bootstrap_identity_authority_concurrent"
            raise err
        self._active_bootstrap_lock_key = lock_key

    def _load_bootstrap_identity_ledger(self) -> dict[str, Any] | None:
        """Load the monotonic authority high-water mark outside the cursor."""
        if not self.deployment_name:
            self._refuse_bootstrap_identity(
                "authority_ledger_scope_missing",
                "bootstrap identity authority has no deployment ledger scope",
                fix_hint=(
                    "run the source through a deployment-bound runner; "
                    "bootstrap authority cannot use an ambient scope"
                ),
            )
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                SELECT value
                  FROM okg.source_progress
                 WHERE deployment = %s
                   AND source_name = %s
                   AND key = %s
                """,
                (
                    self.deployment_name,
                    self.source_name,
                    BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                ),
            )
            row = cur.fetchone()
        try:
            return _parse_bootstrap_identity_ledger(row[0] if row else None)
        except ValueError as exc:
            self._refuse_bootstrap_identity(
                "authority_ledger_malformed",
                "stored bootstrap identity authority ledger is malformed",
                fix_hint=(
                    "restore the runner-owned ledger from trusted evidence; "
                    "do not copy or reuse a cursor"
                ),
                binding="authority_ledger",
                expected=_BOOTSTRAP_LEDGER_SCHEMA,
                observed=str(exc),
            )
        raise AssertionError("bootstrap ledger refusal must raise")

    def _revoke_bootstrap_identity_ledger(
        self,
        ledger: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Persist explicit reset revocation before a new claim is considered."""
        if ledger is None:
            return None
        revoked = {**dict(ledger), "status": "revoked"}
        with self._acquire_conn().transaction(), self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                UPDATE okg.source_progress
                   SET value = %s, updated_at = now()
                 WHERE deployment = %s
                   AND source_name = %s
                   AND key = %s
                   AND value = %s
                """,
                (
                    json.dumps(revoked, sort_keys=True),
                    self.deployment_name,
                    self.source_name,
                    BOOTSTRAP_IDENTITY_LEDGER_PROGRESS_KEY,
                    json.dumps(dict(ledger), sort_keys=True),
                ),
            )
            if cur.rowcount != 1:
                self._refuse_bootstrap_identity(
                    "authority_ledger_replayed",
                    "bootstrap identity authority changed during reset revocation",
                    fix_hint=(
                        "reload the current source state and retry the deliberate "
                        "reset; do not reuse the older cursor"
                    ),
                )
        return revoked

    def _validate_bootstrap_identity_ledger(
        self,
        *,
        authority: Mapping[str, Any] | None,
        ledger: Mapping[str, Any] | None,
    ) -> None:
        """Refuse cursor rollback/reuse before invoking the source reader."""
        if authority is None:
            if ledger is not None and ledger.get("status") == "active":
                self._refuse_bootstrap_identity(
                    "authority_ledger_cursor_missing",
                    "active bootstrap authority is missing from the source cursor",
                    fix_hint=(
                        "restore the current cursor paired with the authority "
                        "ledger, or deliberately revoke progress with --reset-cursor"
                    ),
                )
            return
        if ledger is None:
            self._refuse_bootstrap_identity(
                "authority_ledger_missing",
                "stored bootstrap cursor has no independent authority ledger",
                fix_hint=(
                    "restore the runner-owned ledger from trusted evidence, or "
                    "deliberately revoke incompatible progress with --reset-cursor"
                ),
            )
        expected = _bootstrap_identity_ledger_entry(authority, status="active")
        if dict(ledger) != expected:
            differing = sorted(
                key
                for key in set(expected) | set(ledger)
                if expected.get(key) != ledger.get(key)
            )
            self._refuse_bootstrap_identity(
                "authority_ledger_mismatch",
                "stored bootstrap cursor does not match its authority high-water mark",
                fix_hint=(
                    "reload the latest cursor and ledger; do not restore or copy "
                    "a previously consumed bootstrap generation"
                ),
                binding="authority_ledger_fields",
                expected=sorted(expected),
                observed=differing,
            )

    def _bootstrap_transition_replay(
        self,
        *,
        ledger: Mapping[str, Any] | None,
        prior_authority: Mapping[str, Any] | None,
        prior_cursor: Mapping[str, Any] | None,
    ) -> bool:
        """Recognize one already accepted transition and reject key aliasing."""
        if ledger is None:
            return False
        stored = ledger.get("transition_identity")
        if not isinstance(stored, Mapping):
            self._refuse_bootstrap_identity(
                "transition_identity_missing",
                "bootstrap authority ledger has no accepted transition identity",
                fix_hint="restore the runner-owned authority ledger from trusted evidence",
            )
        current = _bootstrap_transition_identity(self._active_run_context)
        if current is None:
            if ledger.get("status") == "active":
                self._refuse_bootstrap_identity(
                    "transition_identity_missing",
                    "bootstrap resume has no explicit admission transition identity",
                    fix_hint=(
                        "run the source through admission with stable admission_id, "
                        "run_id, and idempotency_key"
                    ),
                )
            return False
        stored_identity = {str(key): str(value) for key, value in stored.items()}
        if stored_identity == current:
            if ledger.get("status") == "exact":
                terminal_cursor_fingerprint = ledger.get(
                    "terminal_cursor_fingerprint"
                )
                if (
                    cursor_has_outstanding_work(prior_cursor)
                    or effective_token_from_cursor(prior_cursor) is None
                    or not isinstance(terminal_cursor_fingerprint, str)
                    or terminal_cursor_fingerprint
                    != identity_fingerprint(prior_cursor or {})
                ):
                    self._refuse_bootstrap_identity(
                        "exact_ledger_cursor_mismatch",
                        "exact bootstrap retry is not paired with its accepted "
                        "terminal cursor",
                        fix_hint=(
                            "restore the exact cursor and ledger from the same "
                            "accepted terminal transition"
                        ),
                    )
            if ledger.get("status") == "active" and prior_authority is None:
                self._refuse_bootstrap_identity(
                    "transition_cursor_missing",
                    "accepted bootstrap transition is missing its cursor authority",
                    fix_hint="restore the cursor and ledger from the same accepted transaction",
                )
            return True
        reused_fields = sorted(
            key
            for key in ("admission_id", "run_id", "idempotency_key")
            if stored_identity.get(key) == current.get(key)
        )
        if reused_fields:
            self._refuse_bootstrap_identity(
                "transition_identity_conflict",
                "bootstrap transition identity aliases a different accepted transition",
                fix_hint=(
                    "reuse all three accepted identity fields for a retry, or use "
                    "a completely new admission transition"
                ),
                binding="reused_transition_fields",
                expected=[],
                observed=reused_fields,
            )
        return False

    def _bootstrap_idempotent_replay_result(
        self,
        *,
        run_id: str,
        mode: str,
        runner_started_at: datetime,
        prior_cursor: Mapping[str, Any] | None,
        timings_ms: dict[str, float],
        timings_started: float,
    ) -> RunResult:
        """Return the stored transition without invoking or mutating the source."""
        timings_ms["total"] = round(
            (time.perf_counter() - timings_started) * 1000,
            3,
        )
        context = self._active_run_context
        return RunResult(
            run_id=run_id,
            source_name=self.source_name,
            mode=mode,
            started_at=runner_started_at,
            finished_at=datetime.now(timezone.utc),
            nodes_appended=0,
            edges_appended=0,
            retractions_node=0,
            retractions_edge=0,
            next_cursor=dict(prior_cursor or {}),
            completed_scope=not cursor_has_outstanding_work(prior_cursor),
            source_health=SourceHealth(
                status="ok",
                reason="accepted bootstrap transition already durable",
            ),
            admission_id=(context.admission_id if context is not None else None),
            idempotency_key=(
                context.idempotency_key if context is not None else None
            ),
            timings_ms=timings_ms,
            idempotent_replay=True,
        )

    def _release_conn(self) -> None:
        if (
            self._active_bootstrap_lock_key is not None
            and self._shared_conn is not None
            and not self._shared_conn.closed
        ):
            try:
                with self._shared_conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                        (self._active_bootstrap_lock_key,),
                    )
            except Exception:  # noqa: BLE001 - connection teardown follows.
                log.exception(
                    "SourceRunner: bootstrap authority unlock failed for "
                    "source=%s",
                    self.source_name,
                )
        self._active_bootstrap_lock_key = None
        if self._owned_graph_store is not None:
            try:
                self._owned_graph_store.close()
            except Exception:  # noqa: BLE001 — cleanup path; suppress close-time errors
                pass
            self._owned_graph_store = None
        if self._pool_cm is not None:
            try:
                self._pool_cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001 — cleanup path; suppress any close-time errors
                pass
        self._pool_cm = None
        self._shared_conn = None
        if self._image_completer is not None:
            try:
                self._image_completer.close()
            except Exception:  # noqa: BLE001 — cleanup path; suppress close-time errors
                pass

    def release_deferred_bootstrap_lock(self) -> None:
        """Release an admission-spanning bootstrap lease, if one is held.

        Also the run's liveness lock (issue #2993): the orchestrator calls
        this once the admission is final and the run base released or
        undone, so the run counts as ended only from here.
        """
        self._release_conn()
        self._release_run_liveness()

    def _chronos_graph_store(self):
        if not self.write_chronos_graph:
            return None
        if self.graph_store is not None:
            return self.graph_store
        if self._owned_graph_store is not None:
            return self._owned_graph_store
        try:
            from okg.substrate.graph_store import (
                ChronosGraphStore,
                deployment_interval_continuation_percent,
            )

            interval_percent = self.chronos_interval_continuation_percent
            if interval_percent is None:
                interval_percent = deployment_interval_continuation_percent(
                    self.deployment_name
                )
            self._owned_graph_store = ChronosGraphStore.connect(
                self.dsn,
                interval_continuation_percent=interval_percent,
            )
            return self._owned_graph_store
        except Exception:  # noqa: BLE001 — log once then re-raise (Chronos graph storage unavailable)
            if not self._chronos_graph_unavailable_logged:
                log.exception(
                    "SourceRunner: Chronos graph storage unavailable for "
                    "source=%s",
                    self.source_name,
                )
                self._chronos_graph_unavailable_logged = True
            raise

    def _active_nomos_policy_fingerprint(self) -> str | None:
        if self._active_pipeline_snapshot is not None:
            return self._active_pipeline_snapshot.nomos_policy_fingerprint
        if self.deployment_name is None:
            return None
        from okg.substrate.nomos import load_active_policy_bundle

        bundle = load_active_policy_bundle(
            self._acquire_conn(),
            deployment_name=self.deployment_name,
            scope=f"deployment:{self.deployment_name}",
        )
        return bundle.policy_fingerprint if bundle is not None else None

    def _checkout_graph_branch(self, graph_store):
        """Checkout this runner's target branch, materializing it on
        first use. Non-default branches (the admission-review
        quarantine branch) fork from `default` so held runs see the
        approved graph for read-side dedupe without writing to it."""
        if self.graph_branch != "default":
            graph_store.ensure_branch(
                self.graph_branch,
                from_branch="default",
            )
        return graph_store.checkout(self.graph_branch)

    def _source_has_published_facts(self, *, no_prior_facts: bool) -> bool:
        """Whether this source has rows a complete run could retract.

        Issue #2892 review. Read from the live graph branch the stray pass
        retracts from, or from the fact log when Chronos writes are off;
        never from the cursor, which `--reset-cursor` or an unverified
        record set can empty. With neither to ask, the answer is yes.
        """
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return not no_prior_facts if self.write_fact_log else True
        session = self._checkout_graph_branch(graph_store)
        rows = list(session.query(
            """
            SELECT EXISTS (
                     SELECT 1 FROM okg.graph_nodes WHERE source = :source
                   )
                OR EXISTS (
                     SELECT 1 FROM okg.graph_edges WHERE source = :source
                   ) AS present
            """,
            {"source": self.source_name},
        ))
        return bool(rows and rows[0]["present"])

    def _load_volatile_fields(self) -> dict[str, frozenset[str]]:
        """Return each node subtype's `okg_volatile_fields` in the applied
        catalog: ``{subtype: frozenset(field, ...)}``.

        The catalog's compose step stamps
        `okg.subtypes.metadata.volatile_fields` from each subtype's
        `okg_volatile_fields` annotation. The persisted dedupe ignores a
        subtype's own volatile fields only (`okg.substrate.fact_equivalence`);
        a union across subtypes would also hide a real change to a key
        another subtype happens to declare volatile (issue #2334). Edge types
        declare none.

        Reads the latest applied catalog version of each subtype, as the graph
        writer's per-subtype metadata reads do. Returns an empty mapping when
        the catalog hasn't been composed yet (test fixtures, first-time
        setup).

        Cached at the SourceRunner instance level -- catalog metadata does not
        change within one ingest run; the cache avoids a per-chunk SQL
        roundtrip.
        """
        if self._active_pipeline_snapshot is not None:
            return self._active_pipeline_snapshot.volatile_fields_by_subtype()
        if self._volatile_keys_cache is not None:
            return dict(self._volatile_keys_cache)
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (s.name)
                       s.name, s.metadata->'volatile_fields'
                  FROM okg.subtypes s
                  JOIN okg.catalog_versions cv
                    ON cv.catalog_version_id = s.catalog_version_id
                 WHERE cv.status = 'applied'
                 ORDER BY s.name, s.catalog_version_id DESC
                """,
            )
            fields: dict[str, frozenset[str]] = {}
            for name, declared in cur.fetchall():
                if not isinstance(declared, list):
                    continue
                keys = frozenset(
                    str(key) for key in declared if isinstance(key, str) and key
                )
                if keys:
                    fields[str(name)] = keys
            self._volatile_keys_cache = fields
            return dict(fields)

    def run(
        self,
        adapter: SourceAdapter,
        *,
        mode: str = "cursor",
        sync_scope: Optional[dict[str, Any]] = None,
        run_context: Optional[SourceRunContext] = None,
        reset_cursor: bool = False,
        emit_delta_contract: bool = False,
    ) -> RunResult:
        """Execute one adapter run. Returns counts + watermark.

        Raises on policy refusal (e.g. `mutable_api` in cursor
        mode that tries to emit retractions) BEFORE any fact
        is written.

        When `emit_delta_contract` is set (the caller marks
        readiness-critical sources), the runner observes the streamed
        fact sequence once and attaches a validated `SourceDeltaBatch`
        to `RunResult.source_delta_batch` — the no-change /
        small-change / delete-reconcile delta evidence downstream
        publish/projection will consume (`deltaflow-source-contracts`).

        Opens one shared Postgres connection at entry and closes it
        in `finally`. Helpers (cursor read, dedupe SELECT, COPY,
        watermark UPSERT, …) reuse it instead of each opening their
        own. Saves ~50–60 ms of handshake per source per commit.
        """
        self.tail_failure_diagnostic = None
        self._tail_observer_failures = 0
        self._tail_authority_ordinal = 0
        self._tail_verified = False
        try:
            self._tail_diagnostic_id = uuid.uuid4().hex
        except Exception:
            self._tail_diagnostic_id = None
        profile = self._policy_profile(adapter)
        if (
            self.executable_profile is not None
            and profile != "live_overlay"
            and (profile, mode) not in _POLICY_MODE
        ):
            raise SubstrateError(
                "source profile has no policy mapping for requested mode",
                data={
                    "source_name": self.source_name,
                    "profile": profile,
                    "mode": mode,
                },
                fix_hint=_profile_mode_fix_hint(profile),
            )
        telemetry_context = None
        owns_telemetry_context = False
        if not current_trace_identity().is_contextual:
            telemetry_context = use_trace()
            telemetry_context.__enter__()
            owns_telemetry_context = True
        try:
            self._active_run_context = run_context
            self._active_run_mode = mode
            self._active_sync_scope = sync_scope
            self._active_retractions_allowed = None
            self._source_state_snapshot = None
            self._active_run_base = None
            self._run_base_attempted = False
            self._run_applied_chunks = False
            self._run_marked_replay_fence = False
            self._run_refused_retraction = False
            self._active_run_id = None
            self._fact_batch_application = None
            self._active_replay_target = None
            self._active_clear_replay_required = False
            self._active_watermark_cas_required = False
            self._active_watermark_cas_expected = None
            self._active_bootstrap_ledger = None
            self._active_bootstrap_ledger_expected = None
            self._active_bootstrap_ledger_next = None
            self._active_private_authority_candidate = None
            self._active_pipeline_snapshot = (
                self._resolve_pipeline_identity_snapshot(adapter)
            )
            self._full_retraction_guard = FullRetractionGuard(
                self.source_name,
                registry_name=self.registry_name,
                override=self.full_retraction_override,
            )
            self._acquire_bootstrap_authority_lock(adapter)
            result = self._run_impl(
                adapter,
                mode=mode,
                sync_scope=sync_scope,
                run_context=run_context,
                reset_cursor=reset_cursor,
                emit_delta_contract=emit_delta_contract,
                owns_telemetry_context=owns_telemetry_context,
            )
            result.full_retraction_override = (
                self._full_retraction_guard.audit_record()
            )
            self._release_owned_run_base()
            return result
        except Exception as run_error:
            # Issue #1431: undo the chunks this run already applied before the
            # refusal surfaces. Scoped to the retraction refusal on purpose.
            # Other failures keep their existing contract: a run that wrote
            # facts and a progress marker and then failed to acknowledge its
            # change identity must LEAVE that evidence, with the replay fence
            # that forces the next run to reapply authoritatively
            # (test_source_runner_change_probe_gate_e2e.py::
            # test_facts_and_marker_before_failed_ack_require_authoritative_reapply).
            if self._run_refused_retraction:
                self._compensate_failed_run(self._active_run_id, run_error)
            self._release_owned_run_base()
            raise
        finally:
            if telemetry_context is not None:
                telemetry_context.__exit__(None, None, None)
            self._active_run_context = None
            self._active_sync_scope = None
            self._fact_batch_application = None
            self._active_pipeline_snapshot = None
            self._active_replay_target = None
            self._active_clear_replay_required = False
            self._active_watermark_cas_required = False
            self._active_watermark_cas_expected = None
            self._active_bootstrap_ledger = None
            self._active_bootstrap_ledger_expected = None
            self._active_bootstrap_ledger_next = None
            self._active_private_authority_candidate = None
            # A runner that owns its compensation is done with the run here
            # (its run base released or undone above); the orchestrator
            # releases the lock after the admission is final instead.
            if self.compensate_on_failure:
                self._release_run_liveness()
            if not (
                self.defer_bootstrap_lock_release
                and self._active_bootstrap_lock_key is not None
            ):
                self._release_conn()

    def _resolve_registered_pii_posture_if_available(
        self,
    ) -> Optional["PiiPosture"]:
        """Bind direct deployment-scoped callers to their audited source policy.

        The orchestration service passes an already-resolved posture. This lazy
        backstop covers callers that construct ``SourceRunner`` directly with a
        real deployment/source pair; test and standalone runners with no resolvable
        deployment retain only the explicitly constructed runner posture.
        """
        if not self.deployment_name:
            return self._configured_pii_posture
        from okg.substrate import fastyaml
        from okg.substrate.deployments import find_deployment_dir
        from okg.substrate.ingest.pii_posture import (
            resolve_deployment_source_pii_posture,
        )
        from okg.substrate.nomos.rollout import runtime_enforce_mode_dsn_keys

        deployment_dir = find_deployment_dir(self.deployment_name)
        if deployment_dir is None:
            return self._configured_pii_posture
        manifest_path = deployment_dir / "deployment.yaml"
        registry_path = deployment_dir / "source_registry.yaml"
        if not manifest_path.is_file() or not registry_path.is_file():
            return self._configured_pii_posture
        manifest = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8")
        ) or {}
        registry_doc = fastyaml.safe_load(
            registry_path.read_text(encoding="utf-8")
        ) or {}
        sources = registry_doc.get("sources") or registry_doc
        if not isinstance(sources, dict):
            raise SubstrateError("source_registry sources must be a mapping")
        selected_name = None
        selected_entry = None
        for registry_name, raw_entry in sources.items():
            if not isinstance(raw_entry, dict):
                continue
            if str(registry_name) == self.source_name or str(
                raw_entry.get("ownership_id") or ""
            ) == self.source_name:
                selected_name = str(registry_name)
                selected_entry = raw_entry
                break
        if selected_entry is None or selected_name is None:
            return self._configured_pii_posture
        resolved = resolve_deployment_source_pii_posture(
            manifest=manifest,
            source_name=selected_name,
            source_entry=selected_entry,
            enforce_dsn_keys=runtime_enforce_mode_dsn_keys(deployment_dir),
        )
        # Preserve the existing direct-call contract while also returning the
        # posture for immutable per-run pipeline snapshots.
        self.pii_posture = resolved
        return resolved

    def _resolve_pipeline_identity_snapshot(
        self,
        adapter: SourceAdapter,
    ) -> PipelineIdentitySnapshot:
        """Resolve all fact-determining runner state exactly once per run."""
        import importlib
        import sys

        import okg.substrate.alias.pseudonymize as pseudonymize_module
        import okg.substrate.canonicalize.base as canonicalize_module
        pii_posture_module = importlib.import_module(
            "okg.substrate.ingest.pii_posture"
        )
        pii_sanitize_module = importlib.import_module(
            "okg.substrate.ingest.pii_sanitize"
        )
        sanitize_module = importlib.import_module(
            "okg.substrate.ingest.sanitize"
        )
        import okg.substrate.nomos.pii_classification as pii_classification_module
        import okg.substrate.nomos.read_masking as read_masking_module
        import okg.substrate.nomos.source_lineage as source_lineage_module
        import okg.substrate.fact_equivalence as fact_equivalence_module

        self._volatile_keys_cache = None
        pii_posture = self._resolve_registered_pii_posture_if_available()
        canonicalizers = tuple(self.canonicalizers)
        volatile_fields = tuple(sorted(
            (subtype, tuple(sorted(keys)))
            for subtype, keys in self._load_volatile_fields().items()
        ))
        nomos_policy_fingerprint = self._active_nomos_policy_fingerprint()
        binding_fingerprint = (
            self.registry_binding_fingerprint
            or str(
                getattr(
                    adapter,
                    "__okg_registry_binding_fingerprint__",
                    "",
                )
            )
            or identity_fingerprint({
                "module": type(adapter).__module__,
                "class": type(adapter).__qualname__,
            })
        )
        snapshot = PipelineIdentitySnapshot.build(
            canonicalizers=canonicalizers,
            volatile_fields=volatile_fields,
            nomos_policy_fingerprint=nomos_policy_fingerprint,
            pii_posture=pii_posture,
            registry_binding_fingerprint=binding_fingerprint,
            transform_identity={
                # Hash the complete runner module so private helpers, the
                # chunk pipeline, record-set authority, and dedupe behavior
                # cannot change without changing the applied identity.
                "runner": module_code_identity(sys.modules[__name__]),
                # Hash every external module invoked by the fact-determining
                # transforms. Top-level method hashes alone miss helper edits.
                "sanitize": module_code_identity(sanitize_module),
                "pii_posture": module_code_identity(pii_posture_module),
                "pii_sanitize": module_code_identity(pii_sanitize_module),
                "pii_classification": module_code_identity(
                    pii_classification_module
                ),
                "read_masking": module_code_identity(read_masking_module),
                "pseudonymize": module_code_identity(pseudonymize_module),
                "canonicalize": module_code_identity(canonicalize_module),
                "source_lineage": module_code_identity(
                    source_lineage_module
                ),
                # The "is this fact unchanged?" rule the persisted dedupe
                # applies.
                "fact_equivalence": module_code_identity(
                    fact_equivalence_module
                ),
            },
        )
        # Legacy helpers inspect these attributes. Bind them to the immutable
        # per-run snapshot; every transform reads through the snapshot below.
        self.pii_posture = pii_posture
        return snapshot

    def _pipeline_snapshot_still_current(
        self,
        adapter: SourceAdapter,
    ) -> bool:
        """Prove the exact run-local transform snapshot retained its identity.

        Every fact-determining collaborator is detached from mutable runner
        configuration before the reader starts. A concurrent configuration
        replacement therefore cannot mix transform versions within this run:
        the current run finishes coherently on its captured identity, while the
        next run resolves the replacement and forces an authoritative replay.
        """
        del adapter
        applied = self._active_pipeline_snapshot
        return applied is not None and applied.is_intact()

    def evaluate_poll(
        self,
        adapter: SourceAdapter,
        *,
        mode: str = "cursor",
        reconcile_due: bool = False,
    ) -> PollProbeDecision:
        """Evaluate poll enqueue eligibility without acknowledging source state."""
        profile = self._policy_profile(adapter)
        if (
            self.executable_profile is not None
            and profile != "live_overlay"
            and (profile, mode) not in _POLICY_MODE
        ):
            raise SubstrateError(
                "source profile has no policy mapping for poll mode",
                data={
                    "source_name": self.source_name,
                    "profile": profile,
                    "mode": mode,
                },
            )
        try:
            self._active_run_mode = mode
            self._active_sync_scope = None
            self._active_pipeline_snapshot = (
                self._resolve_pipeline_identity_snapshot(adapter)
            )
            prelude = self._load_cursor_progress_boundary(
                adapter=adapter,
                reset_cursor=False,
                sync_scope=None,
            )
            prior_bootstrap_authority = self._bootstrap_authority_from_cursor(
                prelude.prior_cursor,
            )
            ensure_checkout = getattr(adapter, "ensure_checkout", None)
            if callable(ensure_checkout):
                health = ensure_checkout()
                if (
                    health is not None
                    and getattr(health, "status", "ok")
                    not in {"ok", "not_applicable", "skipped_optional"}
                ):
                    raise SubstrateError(
                        "poll checkout acquisition was not healthy"
                    )
            change_probe = getattr(adapter, "change_probe", None)
            if change_probe is None:
                raise SubstrateError("adapter has no runtime change probe")
            if not self._adapter_probe_short_circuit_safe(adapter):
                return PollProbeDecision(
                    enqueue=True,
                    changed=True,
                    reason="adapter_short_circuit_unsafe",
                    revision_hint=(
                        "change_probe:"
                        + identity_fingerprint({
                            "source": self.source_name,
                            "reason": "adapter_short_circuit_unsafe",
                        })
                    ),
                )
            verdict = run_probe(
                change_probe,
                cursor=raw_token_cursor(prelude.prior_cursor),
                sync_scope=None,
            )
            if not verdict.token:
                raise SubstrateError("runtime change probe returned no token")
            snapshot = self._active_pipeline_snapshot
            if snapshot is None:
                raise SubstrateError(
                    "pipeline identity snapshot is unavailable"
                )
            effective = build_effective_token(verdict.token, snapshot)
            prior_effective = effective_token_from_cursor(
                prelude.prior_cursor
            )
            effective_changed = (
                verdict.changed or effective != prior_effective
            )
            eligible_unchanged = (
                not reconcile_due
                and not verdict.changed
                and prior_bootstrap_authority is None
                and prelude.replay_required is None
                and effective == prior_effective
                and self._probe_skip_eligible(
                    mode=mode,
                    reset_cursor=False,
                    prior_cursor=prelude.prior_cursor,
                )
            )
            reason = (
                "bootstrap_identity_resume_required"
                if prior_bootstrap_authority is not None
                else (
                    "eligible_unchanged"
                    if eligible_unchanged
                    else (
                        "reconciliation_due"
                        if reconcile_due else verdict.reason or "changed"
                    )
                )
            )
            return PollProbeDecision(
                enqueue=not eligible_unchanged,
                changed=effective_changed,
                reason=reason,
                revision_hint=(
                    "change_probe:" + identity_fingerprint(effective)
                ),
                effective_token=effective,
            )
        finally:
            self._active_pipeline_snapshot = None
            self._active_sync_scope = None
            self._release_conn()

    def _run_impl(
        self,
        adapter: SourceAdapter,
        *,
        mode: str,
        sync_scope: Optional[dict[str, Any]],
        run_context: Optional[SourceRunContext],
        reset_cursor: bool,
        emit_delta_contract: bool = False,
        owns_telemetry_context: bool = False,
    ) -> RunResult:
        if not self.write_chronos_graph and not self.write_fact_log:
            raise SubstrateError(
                "SourceRunner has no enabled write sink",
                data={
                    "source_name": self.source_name,
                    "write_chronos_graph": self.write_chronos_graph,
                    "write_fact_log": self.write_fact_log,
                },
                fix_hint=(
                    "enable Chronos graph writes or fact-log writes before "
                    "advancing source watermarks"
                ),
            )

        timings_started = time.perf_counter()
        timings_ms: dict[str, float] = {}
        telemetry_recorder: SpanTimingRecorder | None = None

        def add_timing(
            label: str,
            started: float,
            *,
            record_span: bool = True,
        ) -> None:
            ended = time.perf_counter()
            elapsed = round((ended - started) * 1000, 3)
            timings_ms[label] = round(timings_ms.get(label, 0.0) + elapsed, 3)
            if record_span and telemetry_recorder is not None:
                telemetry_recorder.record(label, started, ended_perf=ended)

        runner_started_at = datetime.now(timezone.utc)
        if run_context is None:
            run_id = str(uuid.uuid4())
            observed_at = runner_started_at
            admission_id = None
            idempotency_key = None
            workflow_id = None
        else:
            run_id = run_context.run_id
            observed_at = run_context.observed_at
            admission_id = run_context.admission_id
            idempotency_key = run_context.idempotency_key
            workflow_id = run_context.workflow_id
        # Issue #1431: the failure path in `run()` needs the run identity to
        # compensate the chunks this run already applied, and the source's
        # pre-run watermark + progress to put the cursor back with them.
        self._active_run_id = run_id
        self._snapshot_source_state_for_compensation()
        active_identity = current_trace_identity()
        if (
            owns_telemetry_context
            and active_identity.from_local_context
            and not active_identity.from_otel_context
            and active_identity.parent_span_id is None
        ):
            telemetry_identity = active_identity
            context_identity = None
        else:
            telemetry_identity = child_span_identity()
            context_identity = (
                active_identity
                if active_identity.is_contextual else None
            )
        deployment_name = self.deployment_name or "unknown"
        telemetry_recorder = (
            SpanTimingRecorder(
                trace_id=telemetry_identity.trace_id,
                root_span_id=telemetry_identity.span_id,
                root_span_name=f"source.{self.source_name}",
                root_started_at=runner_started_at,
                base_perf_counter=timings_started,
                deployment_name=deployment_name,
                branch_name=self.graph_branch,
                component="source-runner",
                source_name=self.source_name,
                workflow_id=workflow_id,
                run_id=run_id,
                root_kind="source_run",
            )
            if deep_span_capture_enabled()
            else None
        )
        telemetry_unavailable: dict[str, str] = {}
        if admission_id is None:
            telemetry_unavailable["admission_id"] = (
                "source_run_context_missing_admission_id"
            )
        if idempotency_key is None:
            telemetry_unavailable["idempotency_key"] = (
                "source_run_context_missing_idempotency_key"
            )
        if workflow_id is None:
            telemetry_unavailable["workflow_id"] = (
                "source_run_context_missing_dbos_workflow_id"
            )

        requested_mode = mode
        requested_sync_scope = sync_scope
        self._authorize_source_ingest_boundaries(
            run_id=run_id,
            mode=mode,
            sync_scope=sync_scope,
            adapter=adapter,
        )
        self._refuse_rerun_while_undo_unfinished(run_id)

        # Per `add-doc-corpus-chunk-reconciliation` (2026-04-26):
        # load the persisted cursor and pass it to the adapter so
        # cursor-based optimizations (content-sha short-circuits,
        # per-record-set diffs for retraction) work. Adapters whose
        # signature doesn't accept `cursor=` are called as before
        # (graceful for older sources).
        #
        # Per `add-substrate-incremental-publish` (2026-04-28):
        # also load the persisted progress dict and pass it as
        # `since_progress=`. Distinct from `cursor=` — cursor
        # is one JSON blob ("where did I leave off"), progress
        # is keyed `(deployment, source, key)` so adapters
        # can persist per-record fingerprints. TypeError
        # fallback mirrors the cursor= precedent for adapters
        # whose signature doesn't accept the kwarg.
        #
        # The three startup reads (cursor / progress / no-prior-facts)
        # are coalesced into a single roundtrip; see
        # `_load_run_prelude`.
        step_started = time.perf_counter()
        prelude = self._load_cursor_progress_boundary(
            adapter=adapter,
            reset_cursor=reset_cursor,
            sync_scope=sync_scope,
        )
        add_timing("load_run_prelude", step_started)
        prior_cursor = prelude.prior_cursor
        prior_progress = prelude.prior_progress
        path_scoped = source_sync_scope_narrows(sync_scope)
        change_probe = getattr(adapter, "change_probe", None)
        managed_change_identity = (
            change_probe is not None
            and self._adapter_probe_short_circuit_safe(adapter)
        )
        prior_effective_token = effective_token_from_cursor(prior_cursor)
        prior_bootstrap_authority = self._bootstrap_authority_from_cursor(
            prior_cursor,
        )
        bootstrap_bindings_before: dict[str, Any] | None = None
        bootstrap_binding_error: SubstrateError | None = None
        bootstrap_semantics = getattr(
            adapter,
            "bootstrap_cursor_semantics",
            None,
        )
        if isinstance(bootstrap_semantics, str) and bootstrap_semantics:
            bootstrap_ledger = (
                self._load_bootstrap_identity_ledger()
                if self.deployment_name
                else None
            )
            if not self.deployment_name and (
                prior_bootstrap_authority is not None or reset_cursor
            ):
                self._refuse_bootstrap_identity(
                    "authority_ledger_scope_missing",
                    "bootstrap identity authority has no deployment ledger scope",
                    fix_hint=(
                        "run the source through a deployment-bound runner; "
                        "bootstrap authority cannot use an ambient scope"
                    ),
                )
            if reset_cursor:
                bootstrap_ledger = self._revoke_bootstrap_identity_ledger(
                    bootstrap_ledger,
                )
            self._active_bootstrap_ledger = (
                dict(bootstrap_ledger)
                if bootstrap_ledger is not None else None
            )
        if prior_bootstrap_authority is not None:
            bootstrap_bindings_before = self._bootstrap_binding_snapshot(
                adapter,
                requested_sync_scope=requested_sync_scope,
            )
            prior_bootstrap_authority = (
                self._validate_prior_bootstrap_authority(
                    adapter,
                    prior_cursor=prior_cursor,
                    requested_sync_scope=requested_sync_scope,
                    bindings=bootstrap_bindings_before,
                )
            )
        elif isinstance(bootstrap_semantics, str) and bootstrap_semantics:
            try:
                bootstrap_bindings_before = self._bootstrap_binding_snapshot(
                    adapter,
                    requested_sync_scope=requested_sync_scope,
                )
            except SubstrateError as exc:
                # A terminal exact run does not need bootstrap authority. Hold
                # this refusal unless the source actually returns backlog.
                bootstrap_binding_error = exc
        if isinstance(bootstrap_semantics, str) and bootstrap_semantics:
            self._validate_bootstrap_identity_ledger(
                authority=prior_bootstrap_authority,
                ledger=self._active_bootstrap_ledger,
            )
            if self._bootstrap_transition_replay(
                ledger=self._active_bootstrap_ledger,
                prior_authority=prior_bootstrap_authority,
                prior_cursor=prior_cursor,
            ):
                return self._bootstrap_idempotent_replay_result(
                    run_id=run_id,
                    mode=mode,
                    runner_started_at=runner_started_at,
                    prior_cursor=prior_cursor,
                    timings_ms=timings_ms,
                    timings_started=timings_started,
                )
        snapshot = self._active_pipeline_snapshot
        # Scoped watcher work may coexist with a pending global repair latch,
        # but can neither satisfy it nor be widened by it. The next unscoped
        # run consumes the obligation through authoritative replay.
        full_replay_required = (
            bool(prelude.replay_required) and not path_scoped
        )
        profile = self._policy_profile(adapter)
        if managed_change_identity and not path_scoped:
            if prior_effective_token is None:
                full_replay_required = True
            elif (
                snapshot is None
                or prior_effective_token.get("pipeline_fingerprint")
                != snapshot.fingerprint
                or prior_effective_token.get(
                    "registry_binding_fingerprint"
                )
                != snapshot.registry_binding_fingerprint
            ):
                full_replay_required = True
            elif (
                isinstance(prior_cursor, Mapping)
                and isinstance(
                    prior_cursor.get(_RECORD_SET_CHRONOS_PROOF_KEY),
                    Mapping,
                )
                and not self._prior_record_set_chronos_verified(prior_cursor)
            ):
                # Record-set authority is meaningful only for the exact
                # Chronos branch incarnation it was checked against. A branch
                # rollover can preserve the human branch name while replacing
                # its mutable lineage, so repair the graph before allowing the
                # source to short-circuit on an otherwise unchanged probe.
                full_replay_required = True
        if prior_bootstrap_authority is not None:
            # A validated bootstrap envelope is the explicit resume-only
            # authority. It never authorizes skipping, but it does prevent the
            # full-replay path from stripping the exact frontier it binds.
            full_replay_required = False
        if reset_cursor and not path_scoped:
            full_replay_required = True
        if full_replay_required:
            replay_mode = _AUTHORITATIVE_REPLAY_MODE.get(profile)
            if replay_mode is None:
                raise SubstrateError(
                    "source profile has no authoritative replay mode",
                    data={
                        "source_name": self.source_name,
                        "profile": profile,
                        "requested_mode": mode,
                    },
                    fix_hint=(
                        "declare a completed-scope replay mode for this "
                        "source profile or mark its probe short-circuit unsafe"
                    ),
                )
            mode = replay_mode
            sync_scope = None
            self._active_run_mode = mode
            self._active_sync_scope = None
        if mode != requested_mode or sync_scope != requested_sync_scope:
            # Replay selection can widen a bounded request into a complete
            # source read. Authorize that effective authority before adapter
            # preparation, checkout acquisition, probes, or live calls.
            self._authorize_source_ingest_boundaries(
                run_id=run_id,
                mode=mode,
                sync_scope=sync_scope,
                adapter=adapter,
            )
        if (
            os.environ.get("OKG_SOURCE_RUNNER_SKIP_PERSISTED_DEDUPE")
            in {"1", "true", "TRUE", "yes", "YES"}
            or prelude.no_prior_facts
        ):
            self._dedupe_reference = DEDUPE_REFERENCE_NONE
        elif full_replay_required:
            # Authoritative replay is also graph repair. A fact already in
            # the append-only fact log may be absent from the current Chronos
            # branch after rollover or recovery, so replay never compares
            # against the fact log. It compares against the live branch: a
            # row that is missing there, or differs, is still written, and a
            # row whose content already matches is not rewritten (#2076,
            # #1754, #1731). With Chronos writing off there is no branch to
            # compare against, so everything is written.
            self._dedupe_reference = (
                DEDUPE_REFERENCE_LIVE_BRANCH
                if self.write_chronos_graph
                else DEDUPE_REFERENCE_NONE
            )
        else:
            self._dedupe_reference = self._ordinary_dedupe_reference()

        adapter_cursor = self._prior_cursor_for_adapter(
            mode=mode,
            prior_cursor=self._cursor_without_bootstrap_authority(
                self._cursor_for_authoritative_replay(prior_cursor)
                if full_replay_required else prior_cursor
            ),
        )
        if full_replay_required:
            prior_progress = None
            prepare_authoritative_replay = getattr(
                adapter,
                "prepare_authoritative_replay",
                None,
            )
            if callable(prepare_authoritative_replay):
                step_started = time.perf_counter()
                replay_prior = self._prior_record_set(adapter_cursor)
                replay_kwargs: dict[str, Any] = {}
                if _callable_accepts_kwarg(
                    prepare_authoritative_replay, "prior_footprint_known",
                ):
                    # A `--reset-cursor` run (or an unverified prior) hands
                    # the adapter an empty record set while the source may
                    # still have rows the stray pass would retract. Say so,
                    # so an adapter that must keep earlier records refuses
                    # instead of dropping them (no-code-source-manifest-http).
                    replay_kwargs["prior_footprint_known"] = bool(
                        replay_prior.records
                    ) or not self._source_has_published_facts(
                        no_prior_facts=prelude.no_prior_facts,
                    )
                prepare_authoritative_replay(
                    record_set=replay_prior, **replay_kwargs,
                )
                add_timing(
                    "prepare_authoritative_replay",
                    step_started,
                )

        # ---- Checkout acquisition (code-repos-auto-acquire) ----
        # A source that manages its own checkout (e.g. git-files with
        # manage_checkout) materialises it HERE, before the change-probe reads
        # git HEAD (GitScopeProbe.build_token -> in_proc_head_sha would crash on
        # a missing .git) and before the short-circuit decision. Contract: the
        # optional `ensure_checkout` seam NEVER raises out — on failure it
        # returns a degraded SourceHealth and leaves run() to emit zero facts —
        # so the ingest layer degrades this one source instead of aborting. The
        # health is threaded onto both the short-circuit result and the normal
        # result so a repeatedly-failing acquire stays observable.
        acquire_health: Optional[SourceHealth] = None
        ensure_checkout = getattr(adapter, "ensure_checkout", None)
        if callable(ensure_checkout):
            step_started = time.perf_counter()
            try:
                acquire_health = ensure_checkout()
            except Exception:  # defensive: ensure_checkout must not abort the run
                log.exception(
                    "SourceRunner: ensure_checkout raised for source=%s",
                    self.source_name,
                )
                acquire_health = SourceHealth(
                    status="acquire_failed",
                    reason="ensure_checkout raised",
                )
            add_timing("ensure_checkout", step_started)

        # ---- Change-probe gate (issue #323) ----
        # Run the source-class-bound probe ONCE before the reader. Its token
        # both decides a short-circuit and is stamped on the normal-read cursor
        # (so the NEXT run has a prior token to compare — without this a normal
        # read leaves no token and the source can never short-circuit). The
        # probe is adapter-supplied; sources without one are never gated and
        # always read. `run_probe` enforces one-directional soundness: a probe
        # error or an unsound token resolves to changed=true.
        # A path-scoped sync is an explicit operator request to (re)read a
        # specific subset NOW; its global probe token is not aligned to that
        # scope, so it must neither short-circuit on the gate nor stamp the
        # global token (a partial read does not reconcile the global scope --
        # stamping would let a later full sync skip paths it never read).
        probe_verdict: Optional[ProbeVerdict] = None
        probe_token: Optional[dict[str, Any]] = None
        candidate_effective_token: Optional[dict[str, Any]] = None
        if change_probe is not None and not path_scoped:
            step_started = time.perf_counter()
            probe_verdict = run_probe(
                change_probe,
                cursor=raw_token_cursor(prior_cursor),
                sync_scope=sync_scope,
            )
            add_timing("change_probe", step_started)
            if probe_verdict.token:
                probe_token = probe_verdict.token
                if snapshot is None:
                    raise SubstrateError(
                        "pipeline identity snapshot is unavailable"
                    )
                candidate_effective_token = build_effective_token(
                    probe_token, snapshot
                )
            if prior_bootstrap_authority is not None:
                if not probe_token:
                    self._refuse_bootstrap_identity(
                        "probe_unavailable",
                        "bootstrap resume probe produced no snapshot identity",
                        fix_hint=(
                            "restore the source probe and retry against a stable "
                            "snapshot; do not reset resumable progress"
                        ),
                    )
                try:
                    validate_token_composition(probe_token)
                except ChangeProbeError as exc:
                    self._refuse_bootstrap_identity(
                        "raw_token_malformed",
                        "bootstrap resume probe returned an unsound identity",
                        fix_hint=(
                            "repair the probe token composition and retry; do "
                            "not reset resumable progress"
                        ),
                        binding="raw_token",
                        expected="sound_change_probe_token",
                        observed=str(exc),
                    )
            if (
                managed_change_identity
                and not probe_verdict.changed
                and not full_replay_required
                and not prelude.replay_required
                and candidate_effective_token == prior_effective_token
                and self._probe_skip_eligible(
                    mode=mode,
                    reset_cursor=reset_cursor,
                    prior_cursor=prior_cursor,
                )
            ):
                # Surface an explicit zero-work stats dict so the microbench /
                # optimization ledger reads files_scanned==0 (not an absent
                # source_stats) when the probe short-circuits the reader.
                if hasattr(adapter, "last_run_stats"):
                    adapter.last_run_stats = {
                        "scope_status": "probe_short_circuit",
                        "files_seen": 0,
                        "files_scanned": 0,
                        "files_skipped_unchanged": 0,
                        "emitted_nodes": 0,
                        "emitted_edges": 0,
                        "probe_skipped": True,
                        "probe_reason": probe_verdict.reason,
                    }
                return self._probe_short_circuit_result(
                    run_id=run_id,
                    mode=mode,
                    observed_at=observed_at,
                    runner_started_at=runner_started_at,
                    prior_cursor=prior_cursor,
                    probe_token=candidate_effective_token,
                    reason=probe_verdict.reason,
                    completed_scope=(
                        mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
                        or profile == "append_feed"
                    ),
                    timings_ms=timings_ms,
                    timings_started=timings_started,
                    source_health=acquire_health,
                )

        # This fence is durable before adapter invocation and therefore before
        # any fact/progress/cursor correctness mutation.
        if managed_change_identity:
            self._mark_replay_required(
                run_id=run_id,
                obligation=(
                    "bootstrap_identity_resume"
                    if prior_bootstrap_authority is not None
                    else (
                        "authoritative_full_replay"
                        if full_replay_required else "in_flight_delta"
                    )
                ),
                raw_token=probe_token,
            )

        step_started = time.perf_counter()
        source_run = adapter.run(
            run_id,
            **_adapter_run_kwargs(
                adapter,
                mode=mode,
                cursor=adapter_cursor,
                since_progress=prior_progress,
                sync_scope=sync_scope,
                change_probe_verdict=probe_verdict,
                authoritative_replay=full_replay_required,
                source_has_published_facts=functools.partial(
                    self._source_has_published_facts,
                    no_prior_facts=prelude.no_prior_facts,
                ),
            ),
        )
        add_timing("adapter_run", step_started)
        # PACT first-class-images (#3046): the image step resolves file
        # image values against the root this run names. The root stays on
        # the completer; no fact, row or report carries it.
        self._image_completer = ImageCompleter(
            image_root=getattr(source_run, "image_root", None),
            config=self.images_config,
            connect=self._connect_image_blob_store,
            source_name=self.source_name,
        )
        # A narrow path scope bounds what the runner ASKED for, not
        # necessarily what the adapter read. An adapter whose emitted
        # identities depend on content outside the requested paths -- a LaTeX
        # caption's `caption_of` target embeds the figure image's content
        # hash, so editing the image in place remints the target while the
        # citing document's bytes never change -- must widen to the complete
        # configured scope for its declared footprint to be sound, and says so
        # with `effective_scope_complete`. Both deletion-authority gates below
        # key on an unscoped request, so without honoring the widening here
        # the complete record set this run produced is discarded and the
        # superseded identity is never retracted (issue #1454). Widen the
        # EFFECTIVE scope, then re-authorize the ingest boundaries at that
        # broader authority exactly as the pre-run replay widening does.
        if (
            source_sync_scope_narrows(sync_scope)
            and source_run.effective_scope_complete is True
            and source_run.completed_scope is True
            and mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
            and source_run.run_mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
        ):
            sync_scope = None
            self._active_sync_scope = None
            self._authorize_source_ingest_boundaries(
                run_id=run_id,
                mode=mode,
                sync_scope=None,
                adapter=adapter,
            )
            # `path_scoped` (bound from the REQUESTED scope above) is
            # deliberately NOT recomputed here. Every consumer of it treats
            # "scoped" as the conservative value, so leaving it True keeps
            # this run at the narrower authority for change identity while
            # the widening grants deletion authority only:
            #   * bootstrap_resolution_pending stays False, so a discovery
            #     crawl still has to show complete deletion authority;
            #   * a bootstrap identity claim is REFUSED outright, and the
            #     bootstrap-identity validation path is skipped;
            #   * managed change identity carries the prior probe state
            #     forward instead of acknowledging a new one, so no ABA-
            #     unsafe token is minted from a run that began scoped;
            #   * runner_proven_no_change stays False, so nothing claims
            #     this run proved the source unchanged.
            # Each of those fails CLOSED. Recomputing `path_scoped` would
            # relax them on the strength of an adapter's own declaration,
            # which is exactly the authority this block is not granting.
        authority_stream = source_run.record_authority_stream
        if (
            authority_stream is None
            and self.record_authority_format == "indexed_v2"
            and source_run.record_set is not None
        ):
            # A source that declared indexed_v2 must not quietly write its
            # authority back through the legacy whole-field path: that is the
            # exact unbounded write this format exists to remove.
            raise SubstrateError(
                "indexed_v2 source returned a legacy record set",
                data={"source_name": self.source_name},
            )
        if authority_stream is not None:
            if self.record_authority_format != "indexed_v2":
                raise SubstrateError(
                    "source returned indexed authority without indexed_v2 opt-in",
                    data={"source_name": self.source_name},
                )
            if source_run.record_set is not None:
                raise SubstrateError(
                    "source returned both legacy and indexed record authority",
                    data={"source_name": self.source_name},
                )
            # A v2 run carries no RecordSet, so the record-set coverage,
            # ownership and retraction block below does not run for it. Until a
            # bounded consumer reads the sealed authority back, accepting such
            # a run would silently drop this source deletion synthesis. Refuse
            # by default; only an explicit constructor argument, which no
            # registry field can set, opts into the unconsumed path.
            if not self.allow_unconsumed_indexed_authority:
                raise SubstrateError(
                    "indexed_v2 authority has no wired record-set consumer yet",
                    data={
                        "source_name": self.source_name,
                        "blocked_on": "task.integrate-bounded-authority",
                    },
                )
            self._active_private_authority_candidate = (
                self._stage_record_authority_stream(authority_stream)
            )
        self._authorize_source_emit_boundary(
            run_id=run_id,
            mode=mode,
            sync_scope=sync_scope,
            adapter=adapter,
            source_run_mode=source_run.run_mode,
            completed_scope=source_run.completed_scope,
        )
        retain_run_seen_identities = self._retain_run_seen_identities(adapter)

        from okg.substrate.ingest.sanitize import (
            identity_nul_fields,
        )

        record_retractions: list[NodeFact | EdgeFact] = []
        next_cursor = source_run.next_cursor
        explicit_record_set = source_run.record_set
        deletion_authorized = self._completed_scope_deletion_authorized(
            mode=mode,
            source_run=source_run,
            sync_scope=sync_scope,
            acquire_health=acquire_health,
        )
        # A no-change run declares no record set because the PRIOR one is
        # still exact, not because the source is empty. It must carry that
        # prior authority forward, or the next run rebuilds from nothing.
        preserves_prior_record_set = source_run.preserves_prior_record_set
        if (
            preserves_prior_record_set
            and cursor_has_record_set_authority(adapter_cursor)
            and not cursor_has_record_set_authority(next_cursor)
        ):
            raise SubstrateError(
                "source run claims preserved record-set authority but its "
                "next cursor carries none",
                data={
                    "source_name": self.source_name,
                    "source_run_mode": source_run.run_mode,
                    "reason_code": "preserved_record_set_authority_dropped",
                },
                fix_hint=(
                    "return the no-change cursor through "
                    "preserve_cursor_record_set_authority so the prior "
                    "record_set or record_set_ref is carried forward"
                ),
            )
        infer_stream_footprint = (
            explicit_record_set is None
            # Issue #2122: a preserved-authority run emits no facts -- see
            # `_refuse_preserved_authority_fact`, which enforces that rather
            # than assume it -- so its stream is not a footprint. Inferring
            # one diffed the prior record set against nothing and retracted
            # every record the source had published.
            and not preserves_prior_record_set
            and self.deletion_semantics == "missing_from_completed_scope"
            and deletion_authorized
        )
        # Issue #2145: completed-scope authority covers the whole source;
        # replacement-slice authority covers exactly the declared keys. Either
        # one is enough to run the record-set diff -- the merge below bounds
        # a slice run to its own keys.
        scoped_record_set_authority = (
            self._scoped_record_set_deletion_authority(
                mode=mode,
                source_run=source_run,
                sync_scope=sync_scope,
                acquire_health=acquire_health,
            )
        )
        # Issue #2838 review. `record_set_replacement_keys` is typed
        # Iterable, so a generator would be used up by its first reader and
        # every later check would see nothing. Read it exactly once, here,
        # before any check, and let every later reader see the frozenset.
        source_run.record_set_replacement_keys = freeze_replacement_keys(
            source_run.record_set_replacement_keys,
        )
        if scoped_record_set_authority == "replacement_slice":
            # Issue #2838. Runner-reserved keys first: they must never reach
            # the scope bound below, which may otherwise accept them.
            with self._tail_phase("record_set.scoped_replacement_reserved"):
                refuse_runner_reserved_replacement_keys(
                    source_name=self.source_name,
                    replacement_keys=source_run.record_set_replacement_keys,
                )

        scoped_record_set_authorized = scoped_record_set_authority is not None
        scoped_outside_replacement_keys: list[str] = []
        if scoped_record_set_authority == "replacement_slice":
            # Issue #2145 review B1. Bound the declaration to the scope the
            # caller handed out. Issue #1539: a key outside it is verified
            # against the durable prior once that is loaded, below, and
            # refused there unless it only re-points edges into the scope.
            scoped_outside_replacement_keys = (
                self._scoped_replacement_keys_outside_scope(
                    sync_scope=sync_scope,
                    replacement_keys={
                        str(key)
                        for key in source_run.record_set_replacement_keys
                    },
                )
            )
        record_set = (
            explicit_record_set
            if deletion_authorized or scoped_record_set_authorized
            else None
        )
        if scoped_outside_replacement_keys and record_set is None:
            # The deferred verification below runs only on a record set.
            # Replacement-slice authority always carries one; refuse rather
            # than let an out-of-scope key through unchecked if that ever
            # stops holding.
            raise SubstrateError(
                "scoped run declares replacement record keys outside the "
                "scope it was handed",
                data={
                    "source_name": self.source_name,
                    "reason_code": "scoped_replacement_keys_outside_scope",
                    "out_of_scope_replacement_keys": (
                        scoped_outside_replacement_keys[:20]
                    ),
                },
            )
        bootstrap_resolution_pending = (
            managed_change_identity
            and not path_scoped
            and prior_effective_token is None
            and cursor_has_outstanding_work(next_cursor)
        )
        if (
            self.executable_profile is not None
            and profile == "discovery_crawl"
        ):
            if mode == "scope_complete":
                if (
                    source_run.run_mode != "scope_complete"
                    or not source_run.completed_scope
                    or (
                        explicit_record_set is None
                        and not infer_stream_footprint
                        # Preserved prior authority IS complete deletion
                        # authority, held by reference in the cursor.
                        and not preserves_prior_record_set
                    )
                ) and not (
                    bootstrap_resolution_pending
                    # Issue #2145: a run the caller narrowed with a sync_scope
                    # was never asked for the full scope. Refusing it for want
                    # of complete authority left the scoped delete path with
                    # no way to run at all under an executable profile.
                    or (
                        source_sync_scope_narrows(sync_scope)
                        and scoped_record_set_authorized
                    )
                ):
                    # A source that reported degraded health (a sub-source
                    # or part of its scope failed to read) did not walk its
                    # scope. That is an operator problem with the source's
                    # data, not an adapter defect, so the refusal names the
                    # health the source reported and says what to fix.
                    run_health = source_run.health
                    degraded = (
                        run_health is not None
                        and not self._completed_scope_health_is_acceptable(
                            run_health,
                        )
                    )
                    message = (
                        "discovery crawl forced-full run lacks complete "
                        "deletion authority"
                    )
                    if degraded:
                        message += (
                            f": source reported health {run_health.status!r}"
                            f" ({run_health.reason or 'no reason given'})"
                        )
                    raise SubstrateError(
                        message,
                        data={
                            "source_name": self.source_name,
                            "requested_mode": mode,
                            "source_run_mode": source_run.run_mode,
                            "completed_scope": source_run.completed_scope,
                            "has_record_set": explicit_record_set is not None,
                            "deletion_semantics": self.deletion_semantics,
                            "source_health_status": (
                                run_health.status
                                if run_health is not None else None
                            ),
                            "source_health_reason": (
                                run_health.reason
                                if run_health is not None else None
                            ),
                        },
                        fix_hint=(
                            "part of the source's scope failed to read (a "
                            "sub-source failed; see source_health_reason); "
                            "fix or remove the failing repository or input "
                            "named there and rerun, and nothing was written"
                            if degraded else
                            "enumerate the complete configured scope and either "
                            "return its RecordSet or register "
                            "missing_from_completed_scope for streamed authority"
                        ),
                    )
        prior_record_set = RecordSet()
        step_started = time.perf_counter()
        prior_record_set_chronos_verified = (
            self._prior_record_set_chronos_verified(adapter_cursor)
        )
        add_timing("record_set_prior_proof", step_started)
        record_set_covered_nodes: set[str] = set()
        record_set_node_ownership_modes: dict[str, str] = {}
        record_set_covered_edges: set[tuple[str, str, str]] = set()
        record_set_replacement_keys = (
            {
                str(key)
                for key in source_run.record_set_replacement_keys
            }
            if source_run.record_set_replacement_keys is not None else None
        )
        if record_set is not None:
            step_started = time.perf_counter()
            # DEFENSE IN DEPTH, not a live repair. A prior that is both
            # chronos-verified and NUL-bearing is unreachable in linear
            # history: write-path sanitization landed 2026-07-19
            # (ac42f2f9a6) and incarnation binding, which is what makes a
            # stored proof verifiable at all, landed 2026-08-01
            # (cd014cb751), so every verified prior was already sanitized
            # when it was written. An UNVERIFIED legacy prior is stripped
            # empty by _prior_cursor_for_adapter before it reaches here.
            # Only a hand-built fixture can produce the remaining state.
            # The call is kept anyway because the cost is one pass and the
            # failure it prevents is a run-aborting DataError from the
            # retired-identity query in delta coverage.
            prior_record_set = self._sanitize_record_set_authority(
                self._prior_record_set(adapter_cursor),
            )
            add_timing("record_set_load_prior", step_started)
            if scoped_record_set_authority == "merged_prior":
                # Issue #2145 review B1. The claim is checked against the
                # durable prior here, where the prior is first available,
                # and BEFORE any retraction is computed.
                with self._tail_phase("record_set.scoped_merged_prior"):
                    self._verify_scoped_merged_record_set_covers_prior(
                        sync_scope=sync_scope,
                        prior_record_set=prior_record_set,
                        declared_record_set=record_set,
                    )
            if (
                record_set_replacement_keys is not None
                and not prior_record_set_chronos_verified
                and bool(
                    getattr(
                        adapter,
                        "retain_existing_graph_on_record_set_bootstrap",
                        False,
                    )
                )
            ):
                step_started = time.perf_counter()
                retained_record_set = (
                    self._retained_record_set_from_chronos(record_set)
                )
                prior_record_set = RecordSet(
                    records={
                        **retained_record_set.records,
                        **prior_record_set.records,
                    }
                )
                add_timing(
                    "record_set_bootstrap_retained_graph",
                    step_started,
                )
            if scoped_outside_replacement_keys:
                # Issue #1539. Checked against the same prior the merge below
                # carries forward, and BEFORE any retraction is computed.
                with self._tail_phase("record_set.scoped_replacement_scope"):
                    self._verify_scoped_replacement_keys_within_scope(
                        sync_scope=sync_scope,
                        outside_keys=scoped_outside_replacement_keys,
                        prior_record_set=prior_record_set,
                        declared_record_set=record_set,
                    )
            if record_set_replacement_keys is not None:
                step_started = time.perf_counter()
                # Issue #2145 review S2. The undeclared-key refusal raised
                # in here is a record-set refusal like its neighbours, so it
                # leaves the same stderr breadcrumb.
                with self._tail_phase("record_set.replacement_merge"):
                    record_set = self._record_set_with_prior(
                        prior_record_set,
                        current=record_set,
                        replacement_keys=record_set_replacement_keys,
                    )
                add_timing("record_set_merge_prior", step_started)
            # Partial replacement merges durable prior authority into the
            # adapter's current slice.  Sanitize only after that merge so a
            # legacy external record-set blob cannot reintroduce an identity
            # containing U+0000 behind the admission boundary.  The prior is
            # now also sanitized on load above; this pass stays where it is
            # because it is the one that covers the MERGED result, including
            # the adapter's own slice.
            step_started = time.perf_counter()
            record_set = self._sanitize_record_set_authority(record_set)
            add_timing("record_set_sanitize", step_started)
            step_started = time.perf_counter()
            (
                record_set_covered_nodes,
                record_set_covered_edges,
            ) = self._record_set_identities(record_set)
            record_set_node_ownership_modes = (
                self._record_set_node_ownership_modes(record_set)
            )
            add_timing("record_set_identities", step_started)
            step_started = time.perf_counter()
            record_retractions = self._record_set_retractions(
                prior_record_set,
                record_set,
            )
            add_timing("record_set_retractions", step_started)
            if (
                full_replay_required or not prior_record_set_chronos_verified
            ) and scoped_record_set_authority is None:
                # Issue #2145 review B1. Stray retraction reads the WHOLE
                # live graph for this source and retires everything the
                # record set does not cover -- whole-source deletion
                # authority by another name. A narrowed run holds only the
                # bounded authority named above, so applying it here let a
                # slice wipe records published before the source ever
                # carried record-set authority (there is no durable prior
                # to check the slice against, so the coverage check above
                # passes vacuously). The narrowed run keeps its own diff;
                # the strays wait for a run that walked the whole scope.
                step_started = time.perf_counter()
                record_retractions.extend(
                    self._record_set_stray_retractions(record_set),
                )
                add_timing("record_set_stray_retractions", step_started)
            # Issue #2818 backstop: the whole plan is known and nothing of
            # this run is written yet.
            sub_scopes = getattr(adapter, SUB_SCOPE_ATTRIBUTE, False) is True
            self._full_retraction_guard.check_record_set(
                whole_scope=(
                    deletion_authorized
                    and not source_sync_scope_narrows(sync_scope)
                ),
                record_set=record_set,
                prior_record_set=(
                    self._full_retraction_sub_scope_prior(
                        prior_record_set,
                        persisted_cursor=prelude.persisted_cursor,
                    )
                    if sub_scopes else prior_record_set
                ),
                planned_retractions=len(record_retractions),
                sub_scopes=sub_scopes,
                live_rows=lambda: self._full_retraction_live_rows(
                    prior_record_set,
                ),
            )
            step_started = time.perf_counter()
            next_cursor = self._cursor_with_record_set(
                next_cursor,
                record_set,
                chronos_graph_verified=(
                    self.write_chronos_graph
                    and prior_record_set_chronos_verified
                ),
                covered_nodes=record_set_covered_nodes,
                covered_edges=record_set_covered_edges,
            )
            add_timing("record_set_cursor", step_started)

        bootstrap_identity_prevalidated = False
        if source_run.bootstrap_identity is not None and (
            not managed_change_identity or path_scoped
        ):
            self._refuse_bootstrap_identity(
                "claim_context_invalid",
                "bootstrap identity claim is outside a managed global source run",
                fix_hint=(
                    "use a sound managed change probe and an unscoped source "
                    "run before returning bootstrap authority"
                ),
            )
        if (
            managed_change_identity
            and not path_scoped
            and (
                prior_bootstrap_authority is not None
                or (
                    cursor_has_outstanding_work(next_cursor)
                    and prior_effective_token is None
                )
            )
        ):
            next_cursor = self._validate_bootstrap_identity_run(
                adapter=adapter,
                source_run=source_run,
                next_cursor=next_cursor,
                prior_cursor=prior_cursor,
                adapter_cursor=adapter_cursor,
                persisted_cursor=prelude.persisted_cursor,
                prior_authority=prior_bootstrap_authority,
                bindings_before=bootstrap_bindings_before,
                binding_error=bootstrap_binding_error,
                requested_sync_scope=requested_sync_scope,
                effective_sync_scope=sync_scope,
                change_probe=change_probe,
                pre_probe_token=probe_token,
                acquire_health=acquire_health,
                run_id=run_id,
            )
            bootstrap_identity_prevalidated = True

        stream_footprint = (
            _StreamFootprintAccumulator(source_name=self.source_name)
            if infer_stream_footprint else None
        )
        asserts_empty_scope = bool(
            getattr(source_run, "asserts_empty_scope", False)
        )
        record_set_authority_label: str | None = None
        if asserts_empty_scope and stream_footprint is None:
            # The run holds no whole-scope deletion authority (an incremental
            # or cursor run, a narrowed scope, a failed acquire, or a source
            # whose deletion semantics never retract), so the assertion cannot
            # retract anything. Say so rather than drop it silently; the run
            # itself proceeds. Positive facts are still refused below.
            log.warning(
                "source %s asserts an empty scope but this run holds no "
                "whole-scope deletion authority (mode=%s, deletion "
                "semantics=%s); the assertion retracts nothing",
                self.source_name,
                mode,
                self.deletion_semantics,
            )
            record_set_authority_label = "empty_scope_assertion_ignored"
        elif asserts_empty_scope:
            # Issue #2818 backstop: refused before the stream is walked.
            self._full_retraction_guard.check_empty_scope_assertion(
                live_rows=lambda: self._full_retraction_live_rows(
                    self._prior_record_set(adapter_cursor),
                ),
            )
        normalized_fact_observer: (
            Callable[[NodeFact | EdgeFact], None] | None
        ) = (
            # #1859 rule (b): a reader that asserts an empty scope streams
            # nothing; the runner itself retracts the prior footprint. Refuse
            # on the first fact of any kind, before its chunk persists,
            # exactly like the preserved-authority refusal.
            self._refuse_empty_scope_assertion_fact
            if asserts_empty_scope
            else stream_footprint.observe
            if stream_footprint is not None
            # Issue #2122, the other half of the contract: preserving the
            # prior footprint is only sound for a run that emits NOTHING.
            # A reader that claims preservation and still streams facts
            # would get its partial slice written on top of a footprint
            # nothing rechecked. Refuse on the first identity fact rather
            # than count them, so the stream stops before it persists.
            else (
                self._refuse_preserved_authority_fact
                if preserves_prior_record_set else None
            )
        )

        # Per `add-substrate-incremental-publish` (2026-04-28):
        # streaming consumer with per-chunk transactional flush.
        # The whole-stream materialization (`facts =
        # list(source_run.facts)`) is replaced with an
        # incremental consumer that buffers facts + markers and
        # flushes per chunk OR per ProgressMarker. Each flush
        # is one Postgres transaction wrapping
        # canonicalize → in-chunk dedupe → persisted dedupe →
        # COPY → upsert markers, so a `kill -9` mid-flush rolls
        # back BOTH the chunk's facts AND its markers
        # atomically; resume re-parses only un-committed
        # records (their fingerprint was never stamped, so
        # `since_progress` doesn't filter them out).
        # Issue #1431: resolve the retraction verdict HERE — before the walk
        # starts and therefore before any chunk of this run is durable. The
        # lazy "check on the first op='R' fact" it replaces fired after the
        # streaming loop had already committed every earlier chunk.
        retraction_refusal = self._resolve_retraction_authority(
            adapter,
            mode,
            source_run_mode=source_run.run_mode,
            completed_scope=source_run.completed_scope,
            sync_scope=sync_scope,
            health_acceptable=(
                self._completed_scope_health_is_acceptable(
                    source_run.health,
                    acquire_health,
                )
            ),
            # Issue #2145: a narrowed run walks its slice completely even
            # though it never completes the configured scope. Its retractions
            # come from the declared replacement keys, not from absence.
            bounded_record_set_authority=scoped_record_set_authorized,
        )
        self._active_retractions_allowed = retraction_refusal is None

        # Aggregate counters across flushes.
        n_nodes = n_node_retracts = 0
        n_edges = n_edge_retracts = 0
        dedupe_drops_node = dedupe_drops_edge = 0
        dedupe_drops_unchanged_node = dedupe_drops_unchanged_edge = 0
        dedupe_revision_only_node = dedupe_revision_only_edge = 0
        dedupe_kept_by_key: dict[str, int] = {}
        first_dropped: Optional[str] = None
        dedupe_drops_by_prefix: dict[str, int] = {}
        sanitized_value_count = rejected_fact_count = 0
        sanitize_first_rejected: Optional[str] = None
        rejected_marker_count = 0
        sanitize_first_rejected_marker: Optional[str] = None
        # One warning-detail budget for the WHOLE run (not per chunk).
        sanitize_warn_budget = _SanitizeWarnBudget()
        graph_write_sets = GraphWriteSetAccumulator(
            max_entries=(
                None
                if retain_run_seen_identities
                else MAX_BOUNDED_GRAPH_WRITE_SET_ENTRIES
            )
        )

        # `deltaflow-source-contracts` (2026-06-13): observe the streamed
        # fact sequence once (it is never materialized) to build
        # SourceDeltaBatch evidence for readiness-critical sources.
        # Contract construction is observability layered onto the hot
        # path — a malformed item degrades the contract loudly via
        # `delta_contract_error` but never fails the ingest run.
        delta_acc = (
            SourceDeltaAccumulator() if emit_delta_contract else None
        )
        delta_contract_error: Optional[str] = None

        def _observe_delta(item: Any) -> None:
            nonlocal delta_contract_error
            if delta_acc is None or delta_contract_error is not None:
                return
            try:
                delta_acc.observe(item)
            except SourceSyncContractError as exc:
                delta_contract_error = str(exc)

        fact_buffer: list = []
        marker_buffer: list[ProgressMarker] = []
        # Streaming flushes mean `_dedupe_facts()` only sees one
        # buffer at a time. Keep run-wide identities as a second
        # guard so duplicate facts emitted after an earlier flush
        # do not collide on `(dedupe_key, observed_at)` during COPY.
        seen_node_keys: set[tuple[str, str, str]] | None = (
            set() if retain_run_seen_identities else None
        )
        seen_edge_keys: set[tuple[str, str, str, str, str]] | None = (
            set() if retain_run_seen_identities else None
        )

        # Per-marker flushes batch up to MARKER_BATCH_SIZE
        # markers per transaction so the all-unchanged case
        # (5,318 markers, no facts) lands in ~50 transactions,
        # not 5,318. Smaller batches = finer resumability;
        # larger = fewer transactions.
        MARKER_BATCH_SIZE = 100

        def _do_flush() -> None:
            nonlocal n_nodes, n_node_retracts
            nonlocal n_edges, n_edge_retracts
            nonlocal dedupe_drops_node, dedupe_drops_edge
            nonlocal dedupe_drops_unchanged_node
            nonlocal dedupe_drops_unchanged_edge
            nonlocal dedupe_revision_only_node, dedupe_revision_only_edge
            nonlocal first_dropped
            nonlocal sanitized_value_count, rejected_fact_count
            nonlocal sanitize_first_rejected
            nonlocal rejected_marker_count
            nonlocal sanitize_first_rejected_marker
            if not fact_buffer and not marker_buffer:
                return
            # Issue #1431: from here on this run has (or is about to have)
            # durable chunks, so a later failure owes a compensation.
            self._run_applied_chunks = True
            flush_started = time.perf_counter()
            result = self._flush_chunk(
                fact_buffer, marker_buffer,
                run_id=run_id, observed_at=observed_at,
                seen_node_keys=seen_node_keys,
                seen_edge_keys=seen_edge_keys,
                timing_sink=timings_ms,
                timing_recorder=telemetry_recorder,
                sanitize_warn_budget=sanitize_warn_budget,
                normalized_fact_observer=normalized_fact_observer,
                image_completer=self._image_completer,
            )
            add_timing("flush_chunks", flush_started, record_span=False)
            n_nodes += result.counts.nodes_inserted
            n_node_retracts += result.counts.nodes_retracted
            n_edges += result.counts.edges_inserted
            n_edge_retracts += result.counts.edges_retracted
            sanitized_value_count += result.sanitize.sanitized_value_count
            rejected_fact_count += result.sanitize.rejected_fact_count
            if sanitize_first_rejected is None:
                sanitize_first_rejected = result.sanitize.first_rejected
            rejected_marker_count += result.sanitize.rejected_marker_count
            if sanitize_first_rejected_marker is None:
                sanitize_first_rejected_marker = (
                    result.sanitize.first_rejected_marker
                )
            dedupe_drops_node += result.dedupe.in_chunk_nodes
            dedupe_drops_edge += result.dedupe.in_chunk_edges
            dedupe_drops_unchanged_node += result.dedupe.persisted_nodes
            dedupe_drops_unchanged_edge += result.dedupe.persisted_edges
            if (
                result.dedupe.first_dropped is not None
                and first_dropped is None
            ):
                first_dropped = result.dedupe.first_dropped
            for k, v in result.dedupe.by_prefix.items():
                dedupe_drops_by_prefix[k] = (
                    dedupe_drops_by_prefix.get(k, 0) + v
                )
            detail = result.dedupe.persisted_detail
            dedupe_revision_only_node += detail.revision_only_nodes
            dedupe_revision_only_edge += detail.revision_only_edges
            for k, v in detail.kept_by_key.items():
                dedupe_kept_by_key[k] = dedupe_kept_by_key.get(k, 0) + v
            graph_write_sets.observe(result.graph_write_set)
            fact_buffer.clear()
            marker_buffer.clear()

        # Walk the generator. Each item is NodeFact, EdgeFact, or
        # ProgressMarker. Retraction policy is resolved lazily
        # on first op='R' fact; refusal raises BEFORE any
        # further work (in-progress buffer is discarded — never
        # reaches a flush).
        def _walk_items() -> Iterable:
            # Emit synthesized retractions before source facts so a
            # same-run replacement can land as R then I at the same
            # observed_at, leaving the insert as the latest fact.
            yield from record_retractions
            yield from source_run.facts

        step_started = time.perf_counter()
        for item in _walk_items():
            if isinstance(item, ProgressMarker):
                _observe_delta(item)
                marker_buffer.append(item)
                if (
                    len(marker_buffer) >= MARKER_BATCH_SIZE
                    or len(fact_buffer) >= COPY_CHUNK_ROWS
                ):
                    _do_flush()
                continue
            if isinstance(item, (NodeFact, EdgeFact)):
                _observe_delta(item)
                if (
                    getattr(item, "op", "I") == "R"
                    and retraction_refusal is not None
                ):
                    # The verdict was resolved at run start; name the fact
                    # that tried to use it so field triage does not have to
                    # replay the stream to find it.
                    self._name_refused_retraction(retraction_refusal, item)
                    self._run_refused_retraction = True
                    raise retraction_refusal
                if (
                    prior_record_set_chronos_verified
                    and record_set is not None
                    and not identity_nul_fields(item)
                    and getattr(item, "op", "I") == "I"
                ):
                    self._assert_record_set_covers_emitted_fact(
                        item,
                        covered_nodes=record_set_covered_nodes,
                        covered_node_ownership_modes=(
                            record_set_node_ownership_modes
                        ),
                        covered_edges=record_set_covered_edges,
                    )
                fact_buffer.append(item)
                if len(fact_buffer) >= COPY_CHUNK_ROWS:
                    # Chunk-size flush: facts only this round.
                    # The next marker will commit with whatever
                    # facts follow.
                    _do_flush()
                continue
            raise SubstrateError(
                f"SourceRunner: adapter yielded unknown stream "
                f"item type {type(item).__name__}; expected "
                f"NodeFact, EdgeFact, or ProgressMarker",
                data={"source_name": self.source_name},
            )
        add_timing("stream_walk", step_started)

        # End-of-stream: flush any remaining facts + markers.
        step_started = time.perf_counter()
        with self._tail_phase("stream.final_flush"):
            _do_flush()
        add_timing("flush_final", step_started, record_span=False)

        # A registry-declared completed scope may use the ordinary fact stream
        # as its deletion footprint.  Identity collection happens inside each
        # flush after admission sanitization and canonicalization, but before
        # persisted dedupe.  Only successful exhaustion with zero rejected
        # identity facts can replace durable authority.  Missing identities are
        # therefore known absent from the final canonical stream, so their
        # synthesized retractions are safe to emit at the tail.
        if stream_footprint is not None:
            empty_positive_footprint = (
                not stream_footprint.nodes and not stream_footprint.edges
            )
            if rejected_fact_count:
                deletion_authorized = False
                record_set = None
            elif empty_positive_footprint and not asserts_empty_scope:
                # #1859 rule (b), operator decision 2026-09-22. A completed
                # run with no record set and no positive fact is far more
                # often a reader that read nothing than a source that was
                # emptied, and inferring a footprint from it retracted every
                # record the source had published (the #2122 failure mode,
                # reached without the no-change flag). Withdraw deletion
                # authority so the block below carries the prior authority
                # forward, exactly as for `preserves_prior_record_set`. Only
                # `SourceRun.asserts_empty_scope` restores the old reading.
                deletion_authorized = False
                record_set = None
                record_set_authority_label = "preserved_empty_stream"
                normalized_fact_observer = None
                if (
                    stream_footprint.explicit_node_retractions
                    or stream_footprint.explicit_edge_retractions
                ):
                    # The stream named only explicit retractions. They were
                    # applied as ordinary facts; the carried-forward prior
                    # must lose exactly those identities and nothing else,
                    # or the next diff would retract them a second time from
                    # an authority that still lists them. No stray pass:
                    # that reads the whole live graph as the footprint.
                    step_started = time.perf_counter()
                    with self._tail_phase("stream_footprint.retraction_only"):
                        prior_record_set = self._sanitize_record_set_authority(
                            self._prior_record_set(adapter_cursor),
                        )
                        if prior_record_set.records:
                            record_set = self._record_set_without_identities(
                                prior_record_set,
                                node_ids={
                                    self._canonical_node_id(node_id)
                                    for node_id in (
                                        stream_footprint
                                        .explicit_node_retractions
                                    )
                                },
                                edge_identities={
                                    (
                                        self._canonical_node_id(src),
                                        edge_type,
                                        self._canonical_node_id(dst),
                                    )
                                    for src, edge_type, dst in (
                                        stream_footprint
                                        .explicit_edge_retractions
                                    )
                                },
                            )
                            (
                                record_set_covered_nodes,
                                record_set_covered_edges,
                            ) = self._record_set_identities(record_set)
                            next_cursor = self._cursor_with_record_set(
                                next_cursor,
                                record_set,
                                chronos_graph_verified=(
                                    self.write_chronos_graph
                                    and prior_record_set_chronos_verified
                                ),
                                covered_nodes=record_set_covered_nodes,
                                covered_edges=record_set_covered_edges,
                            )
                    add_timing("stream_footprint_retraction_only", step_started)
                    record_set_authority_label = (
                        "preserved_retraction_only_stream"
                    )
            else:
                step_started = time.perf_counter()
                with self._tail_phase("stream_footprint.assemble"):
                    record_set = self._sanitize_record_set_authority(
                        stream_footprint.record_set(),
                    )
                    # The stream footprint reaches the same retraction and
                    # delta-verification chain as the merged path above, so
                    # the prior gets the same treatment -- and for the same
                    # reason it is DEFENSE IN DEPTH rather than a live
                    # repair. A verified-and-dirty prior is unreachable in
                    # linear history here too: sanitization ac42f2f9a6
                    # (2026-07-19) precedes incarnation binding cd014cb751
                    # (2026-08-01). Kept so the two prior-load sites cannot
                    # drift apart, at the cost of one pass.
                    prior_record_set = self._sanitize_record_set_authority(
                        self._prior_record_set(adapter_cursor),
                    )
                    (
                        record_set_covered_nodes,
                        record_set_covered_edges,
                    ) = self._record_set_identities(record_set)
                add_timing("stream_footprint_finalize", step_started)

                step_started = time.perf_counter()
                with self._tail_phase("stream_footprint.stray_retractions"):
                    record_retractions = self._record_set_retractions(
                        prior_record_set,
                        record_set,
                    )
                    if full_replay_required or not prior_record_set_chronos_verified:
                        record_retractions.extend(
                            self._record_set_stray_retractions(record_set),
                        )
                    record_retractions = [
                        fact
                        for fact in record_retractions
                        if not (
                            isinstance(fact, NodeFact)
                            and self._canonical_node_id(fact.node_id)
                            in stream_footprint.explicit_node_retractions
                        )
                        and not (
                            isinstance(fact, EdgeFact)
                            and (
                                self._canonical_node_id(fact.src),
                                fact.edge_type,
                                self._canonical_node_id(fact.dst),
                            ) in stream_footprint.explicit_edge_retractions
                        )
                    ]
                add_timing("stream_footprint_retractions", step_started)

                normalized_fact_observer = None
                if record_retractions:
                    if retraction_refusal is not None:
                        self._run_refused_retraction = True
                        raise retraction_refusal
                    step_started = time.perf_counter()
                    with self._tail_phase("stream_footprint.tail_flush"):
                        # Issue #2988: flush at the same COPY_CHUNK_ROWS
                        # boundary as the streaming loop above. One flush of
                        # every retraction exceeded MAX_FACTS_PER_CHUNK and
                        # FactChunk refused it. Each slice is its own chunk
                        # and transaction, exactly like the loop's chunks.
                        for retraction in record_retractions:
                            fact_buffer.append(retraction)
                            if len(fact_buffer) >= COPY_CHUNK_ROWS:
                                _do_flush()
                        _do_flush()
                    add_timing(
                        "stream_footprint_tail_flush",
                        step_started,
                        record_span=False,
                    )
                next_cursor = self._cursor_with_record_set(
                    next_cursor,
                    record_set,
                    chronos_graph_verified=(
                        self.write_chronos_graph
                        and prior_record_set_chronos_verified
                    ),
                    covered_nodes=record_set_covered_nodes,
                    covered_edges=record_set_covered_edges,
                )

        if dedupe_drops_node or dedupe_drops_edge:
            log.info(
                "SourceRunner dedup dropped facts: "
                "source=%s drops_node=%d drops_edge=%d "
                "first_dropped_id=%r by_prefix=%r",
                self.source_name,
                dedupe_drops_node, dedupe_drops_edge,
                first_dropped,
                dedupe_drops_by_prefix,
            )

        # PACT sanitize-fact-text-at-admission: normalization and
        # rejection must be LOUD — one per-source warning when any
        # NUL was scrubbed or any fact rejected; silence otherwise.
        if (
            sanitized_value_count
            or rejected_fact_count
            or rejected_marker_count
        ):
            log.warning(
                "SourceRunner sanitized NUL (U+0000) at admission: "
                "source=%s sanitized_value_count=%d "
                "rejected_fact_count=%d first_rejected=%r "
                "rejected_marker_count=%d",
                self.source_name,
                sanitized_value_count,
                rejected_fact_count,
                sanitize_first_rejected,
                rejected_marker_count,
            )

        if record_set is not None and self.write_chronos_graph:
            step_started = time.perf_counter()
            with self._tail_phase("record_set.coverage"):
                if prior_record_set_chronos_verified and not full_replay_required:
                    self._verify_record_set_chronos_delta(
                        prior_record_set,
                        record_set,
                    )
                else:
                    self._verify_record_set_chronos_coverage(
                        record_set,
                    )
            add_timing("record_set_chronos_verify", step_started)
            step_started = time.perf_counter()
            next_cursor = self._cursor_with_record_set(
                next_cursor,
                record_set,
                chronos_graph_verified=True,
                covered_nodes=record_set_covered_nodes,
                covered_edges=record_set_covered_edges,
            )
            add_timing("record_set_verified_cursor", step_started)

        if (
            not source_run.completed_scope
            or (
                not deletion_authorized
                and (
                    explicit_record_set is not None
                    or self.deletion_semantics
                    == "missing_from_completed_scope"
                )
            )
        ):
            step_started = time.perf_counter()
            next_cursor = self._preserve_prior_record_set_authority(
                next_cursor,
                adapter_cursor,
            )
            add_timing("record_set_preserve_prior_authority", step_started)

        # A real read may acknowledge identity only when the adapter binds it
        # to the exact snapshot it consumed and the immutable pipeline snapshot
        # is still current. Pre/post probe equality alone is ABA-unsafe.
        # A sealed-snapshot adapter may be acknowledged although the live
        # input moved after its snapshot; that case is recorded here.
        input_advanced_during_run = False
        if managed_change_identity:
            if bootstrap_identity_prevalidated:
                # The sealed eager sequence and pre/claim/post identity were
                # validated before the first fact or marker was consumed.
                pass
            elif path_scoped:
                next_cursor = self._carry_forward_probe_state(
                    next_cursor, prior_cursor
                )
            else:
                applied_raw_token = source_run.applied_raw_token
                pipeline_stable = self._pipeline_snapshot_still_current(
                    adapter
                )
                terminal_health = source_run.health or acquire_health
                healthy_snapshot = (
                    terminal_health is None
                    or terminal_health.status in {
                        "ok",
                        "not_applicable",
                        "skipped_optional",
                    }
                )
                backlogged_progress = cursor_has_outstanding_work(
                    next_cursor
                )
                prior_durable_token = (
                    prior_cursor.get(CURSOR_TOKEN_KEY)
                    if isinstance(prior_cursor, Mapping)
                    else None
                )
                if backlogged_progress:
                    if (
                        not healthy_snapshot
                        or not pipeline_stable
                        or not isinstance(prior_durable_token, Mapping)
                        or not prior_durable_token
                    ):
                        raise SubstrateError(
                            "backlogged source run lacks durable identity authority",
                            data={
                                "source_name": self.source_name,
                                "healthy_snapshot": healthy_snapshot,
                                "pipeline_snapshot_stable": pipeline_stable,
                                "prior_durable_token": bool(
                                    prior_durable_token
                                ),
                            },
                            fix_hint=(
                                "establish one terminal exact source snapshot "
                                "before persisting bounded backlog progress"
                            ),
                        )
                    next_cursor = self._carry_forward_probe_state(
                        next_cursor,
                        prior_cursor,
                    )
                else:
                    post_verdict = run_probe(
                        change_probe,
                        cursor=raw_token_cursor(prior_cursor),
                        sync_scope=sync_scope,
                    )
                    exact_applied = (
                        source_run.applied_token_authority == "exact"
                        and source_run.applied_token_scope == "global"
                        and isinstance(applied_raw_token, Mapping)
                        and bool(applied_raw_token)
                        and bool(post_verdict.token)
                        and tokens_equal(
                            applied_raw_token,
                            post_verdict.token,
                        )
                    )
                    applied_token_binding = source_run.applied_token_binding
                    # An explicit RecordSet still proves that a fact-log-only
                    # replay enumerated the adapter's complete snapshot even
                    # when the run mode is not allowed to replace deletion
                    # authority (for example, append_feed's legacy append
                    # request reporting a completed scope).  Chronos replay
                    # remains stricter: its footprint must have passed the
                    # authorized persistence and graph-verification path.
                    replay_record_set_proof = (
                        record_set is not None
                        or (
                            explicit_record_set is not None
                            and not self.write_chronos_graph
                        )
                    )
                    replay_proof = (
                        healthy_snapshot
                        and (
                        not full_replay_required
                        or (
                            source_run.completed_scope
                            and replay_record_set_proof
                            and (
                                not self.write_chronos_graph
                                or self._prior_record_set_chronos_verified(
                                    next_cursor
                                )
                            )
                        )
                        )
                    )
                    normalize_authoritative_probe = getattr(
                        adapter,
                        "normalize_authoritative_replay_probe",
                        None,
                    )
                    normalizes_replay_probe = (
                        full_replay_required
                        and callable(normalize_authoritative_probe)
                    )
                    acknowledged, ack_reason = (
                        applied_identity_acknowledgeable(
                            applied_raw_token=applied_raw_token,
                            applied_token_authority=(
                                source_run.applied_token_authority
                            ),
                            applied_token_scope=source_run.applied_token_scope,
                            applied_token_binding=applied_token_binding,
                            pre_token=probe_token,
                            post_token=post_verdict.token,
                            pipeline_stable=pipeline_stable,
                            replay_proof=replay_proof,
                            normalizes_replay_probe=normalizes_replay_probe,
                        )
                    )
                    if not acknowledged:
                        raise applied_identity_refusal(
                            source_name=self.source_name,
                            reason=ack_reason,
                            exact_applied_snapshot=exact_applied,
                            pipeline_snapshot_stable=pipeline_stable,
                            authoritative_replay_proof=replay_proof,
                            applied_token_binding=applied_token_binding,
                            applied_raw_token=applied_raw_token,
                            post_token=post_verdict.token,
                        )
                    if ack_reason == APPLIED_IDENTITY_SEALED_INPUT_ADVANCED:
                        # The adapter read a sealed snapshot; its applied
                        # token is true for the facts emitted. The live input
                        # moved during the run, so the next poll will see a
                        # different token and read again.
                        input_advanced_during_run = True
                        log.warning(
                            "SourceRunner acknowledged a sealed snapshot while "
                            "its input advanced during the run: source=%s "
                            "applied_token=%s post_run_token=%s",
                            self.source_name,
                            identity_fingerprint(applied_raw_token),
                            identity_fingerprint(post_verdict.token),
                        )
                        adapter_stats = getattr(
                            adapter, "last_run_stats", None
                        )
                        if isinstance(adapter_stats, dict):
                            adapter_stats["input_advanced_during_run"] = True
                    if self._active_pipeline_snapshot is None:
                        raise SubstrateError(
                            "pipeline identity snapshot is unavailable"
                        )
                    durable_applied_raw_token = applied_raw_token
                    if normalizes_replay_probe:
                        # The exact snapshot above proves the temporary repair
                        # scope that was actually consumed. Some adapters
                        # extend that scope only to rebuild retained graph
                        # authority. Before stamping the durable cursor, let
                        # them return the probe and cursor to their bounded
                        # steady-state scope, then capture that scope's exact
                        # identity so the next poll does not interpret repair-
                        # only inputs as deletions.
                        normalize_authoritative_probe(
                            next_cursor=next_cursor,
                        )
                        durable_verdict = run_probe(
                            change_probe,
                            cursor=raw_token_cursor(prior_cursor),
                            sync_scope=None,
                        )
                        if not durable_verdict.token:
                            raise SubstrateError(
                                "authoritative replay probe normalization "
                                "did not produce a durable identity",
                                data={"source_name": self.source_name},
                                fix_hint=(
                                    "return the adapter probe to its bounded "
                                    "steady-state scope before replay completes"
                                ),
                            )
                        durable_applied_raw_token = durable_verdict.token
                    effective_applied_token = build_effective_token(
                        durable_applied_raw_token,
                        self._active_pipeline_snapshot,
                    )
                    next_cursor = self._stamp_probe_token(
                        next_cursor,
                        effective_applied_token,
                    )
                    self._active_clear_replay_required = True

        terminal_health = source_run.health or acquire_health
        healthy_terminal = (
            terminal_health is None
            or terminal_health.status in {
                "ok",
                "not_applicable",
                "skipped_optional",
            }
        )
        runner_proven_no_change = (
            profile == "append_feed"
            and not source_run.completed_scope
            and not path_scoped
            and managed_change_identity
            and self._active_clear_replay_required
            and healthy_terminal
            and n_nodes == 0
            and n_edges == 0
            and n_node_retracts == 0
            and n_edge_retracts == 0
            and rejected_fact_count == 0
        )
        effective_completed_scope = (
            source_run.completed_scope or runner_proven_no_change
        )
        effective_source_mode = (
            "no_change"
            if runner_proven_no_change
            else source_run.run_mode
        )

        # Per `add-substrate-incremental-publish` (2026-04-28):
        # if the adapter exposes `progress_after_run()`, upsert
        # its dict into `okg.source_progress`. The hook remains best-effort,
        # but a returned value is validated before the cursor boundary:
        # runner-owned authority keys are correctness state and may never be
        # supplied through this adapter-controlled channel.
        terminal_progress: dict[str, str] = {}
        pending_progress: dict[str, str] = {}
        progress_fn = getattr(adapter, "progress_after_run", None)
        admitted = (
            self._active_run_context is not None
            and self._active_run_context.admission_id is not None
        )
        if (
            self.deployment_name is not None
            and callable(progress_fn)
        ):
            step_started = time.perf_counter()
            try:
                new_progress = progress_fn()
            except Exception:  # noqa: BLE001 — best-effort progress hook; log + skip
                log.exception(
                    "SourceRunner: %s.progress_after_run raised",
                    self.source_name,
                )
                new_progress = None
            add_timing("adapter_progress_after_run", step_started)
            if new_progress:
                pending_progress = validate_adapter_source_progress(
                    new_progress,
                    source_name=self.source_name,
                )

        # Advance the source watermark only after adapter-controlled terminal
        # evidence has passed the runner-owned progress-key boundary.
        step_started = time.perf_counter()
        next_cursor = self._advance_cursor_watermark_boundary(
            next_cursor=next_cursor,
            observed_at=observed_at,
            completed_scope=effective_completed_scope,
        )
        add_timing("advance_watermark", step_started)

        if pending_progress:
            if admitted:
                terminal_progress = pending_progress
            else:
                step_started = time.perf_counter()
                try:
                    self._upsert_progress(pending_progress)
                except Exception:  # noqa: BLE001 — best-effort progress upsert; log + skip
                    log.exception(
                        "SourceRunner: failed to upsert source_progress "
                        "for source=%s",
                        self.source_name,
                    )
                add_timing("adapter_progress_upsert", step_started)

        finished_at = datetime.now(timezone.utc)

        if self.deployment_name is not None:
            step_started = time.perf_counter()
            try:
                last_run_payload = json.dumps(
                    {
                        "schema": "okg.source_runner.last_run.v1",
                        "run_id": run_id,
                        "trace_id": telemetry_identity.trace_id,
                        "span_id": telemetry_identity.span_id,
                        "parent_span_id": telemetry_identity.parent_span_id,
                        "workflow_id": workflow_id,
                        "source_name": self.source_name,
                        "mode": mode,
                        "observed_at": observed_at.isoformat(),
                        "admission_observed_at": observed_at.isoformat(),
                        "started_at": runner_started_at.isoformat(),
                        "finished_at": finished_at.isoformat(),
                        "source_runner_ms": round(
                            (
                                finished_at - runner_started_at
                            ).total_seconds() * 1000.0,
                            3,
                        ),
                        "nodes_appended": n_nodes,
                        "edges_appended": n_edges,
                        "nodes_retracted": n_node_retracts,
                        "edges_retracted": n_edge_retracts,
                        "dedupe_drops_node": dedupe_drops_node,
                        "dedupe_drops_edge": dedupe_drops_edge,
                        "completed_scope": effective_completed_scope,
                        "probe_skipped": 0,
                        "reader_invoked": 1,
                        "unavailable": telemetry_unavailable,
                        "full_retraction_override": (
                            self._full_retraction_guard.audit_record()
                        ),
                    },
                    sort_keys=True,
                )
                self._upsert_progress({
                    "sync.last_run": last_run_payload,
                    f"sync.run.{run_id}": last_run_payload,
                })
            except Exception:  # noqa: BLE001 - diagnostic progress is best effort.
                log.exception(
                    "SourceRunner: failed to upsert sync.last_run "
                    "for source=%s",
                    self.source_name,
                )
            add_timing("diagnostic_last_run_progress", step_started)

        timings_ms["total"] = round(
            (time.perf_counter() - timings_started) * 1000,
            3,
        )

        source_delta_batch: Optional[dict[str, Any]] = None
        if delta_acc is not None:
            source_delta_batch = self._delta_contract_dict(
                delta_acc,
                error=delta_contract_error,
                prior_cursor=adapter_cursor,
                next_cursor=next_cursor,
                run_mode=source_run.run_mode,
                # `sync_scope` is the EFFECTIVE scope: the adapter-widening
                # block above may have cleared it. `requested_sync_scope` is
                # what the caller asked for and never moves.
                sync_scope=sync_scope,
                requested_sync_scope=requested_sync_scope,
            )

        try:
            telemetry_finished_at = datetime.now(timezone.utc)
            conn = self._acquire_conn()
            if context_identity is not None:
                record_context_span_if_missing(
                    conn,
                    context_identity,
                    deployment_name=deployment_name,
                    branch_name=self.graph_branch,
                    span_name="ingest.run",
                    root_kind="ingest",
                    started_at=runner_started_at,
                    ended_at=telemetry_finished_at,
                    attributes={
                        "component": "ingest",
                        "okg.component": "ingest",
                    },
                )
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=telemetry_identity.trace_id,
                    span_id=telemetry_identity.span_id,
                    parent_span_id=telemetry_identity.parent_span_id,
                    span_name=f"source.{self.source_name}",
                    span_kind="internal",
                    status_code="ok",
                    deployment_name=deployment_name,
                    branch_name=self.graph_branch,
                    source_name=self.source_name,
                    workflow_id=workflow_id,
                    run_id=run_id,
                    started_at=runner_started_at,
                    ended_at=telemetry_finished_at,
                    attributes={
                        "component": "source-runner",
                        "mode": mode,
                        "completed_scope": effective_completed_scope,
                        "admission_id": admission_id,
                        "idempotency_key": idempotency_key,
                        "deployment_unavailable_reason": (
                            "source_runner.deployment_name_not_supplied"
                            if self.deployment_name is None else None
                        ),
                        "unavailable": telemetry_unavailable,
                    },
                    payload_summary={
                        "timings_ms": timings_ms,
                        "nodes_appended": n_nodes,
                        "edges_appended": n_edges,
                        "nodes_retracted": n_node_retracts,
                        "edges_retracted": n_edge_retracts,
                        "dedupe_drops_node": dedupe_drops_node,
                        "dedupe_drops_edge": dedupe_drops_edge,
                    },
                    unavailable_reason=(
                        "source_runner.partial_context"
                        if telemetry_unavailable else None
                    ),
                    root_name=f"source.{self.source_name}",
                    root_kind="source_run",
                ),
            )
            if telemetry_recorder is not None:
                for child_span in telemetry_recorder.spans():
                    record_span(conn, child_span)
        except Exception:  # noqa: BLE001 - telemetry must not fail the run.
            log.exception(
                "SourceRunner: failed to record telemetry span for source=%s",
                self.source_name,
            )

        self._ensure_zero_chunk_fact_batch_application(run_id=run_id)
        fact_batch_summary = None
        if self._fact_batch_application is not None:
            terminal_health = source_run.health or acquire_health
            healthy = terminal_health is None or terminal_health.status in {
                "ok",
                "not_applicable",
                "skipped_optional",
            }
            if not effective_completed_scope or not healthy:
                completion = "partial"
            elif mode in {
                "reconcile",
                "scope_complete",
                "full_rebuild",
            }:
                completion = "snapshot"
            else:
                completion = "delta"
            fact_batch_summary = self._fact_batch_application.finish_observation(
                completion=completion,
                observed_scope=sync_scope,
                source_delta=source_delta_batch,
            ).to_dict()

        if (
            self._active_bootstrap_ledger_next is not None
            and self._active_bootstrap_ledger_next.get("status") == "exact"
        ):
            self._active_bootstrap_ledger_next = {
                **self._active_bootstrap_ledger_next,
                "terminal_cursor_fingerprint": identity_fingerprint(
                    next_cursor or {}
                ),
            }

        return RunResult(
            run_id=run_id,
            source_name=self.source_name,
            mode=mode,
            started_at=runner_started_at,
            finished_at=finished_at,
            nodes_appended=n_nodes,
            edges_appended=n_edges,
            retractions_node=n_node_retracts,
            retractions_edge=n_edge_retracts,
            next_cursor=next_cursor,
            completed_scope=effective_completed_scope,
            effective_mode=effective_source_mode,
            runner_proven_no_change=runner_proven_no_change,
            record_set_authority=record_set_authority_label,
            input_advanced_during_run=input_advanced_during_run,
            dedupe_drops_node=dedupe_drops_node,
            dedupe_drops_edge=dedupe_drops_edge,
            dedupe_first_dropped_id=first_dropped,
            dedupe_drops_by_prefix=dedupe_drops_by_prefix,
            dedupe_revision_only_node=dedupe_revision_only_node,
            dedupe_revision_only_edge=dedupe_revision_only_edge,
            dedupe_kept_by_key=dedupe_kept_by_key,
            dedupe_reference=self._dedupe_reference,
            sanitized_value_count=sanitized_value_count,
            rejected_fact_count=rejected_fact_count,
            sanitize_first_rejected_fact=sanitize_first_rejected,
            rejected_marker_count=rejected_marker_count,
            sanitize_first_rejected_marker=sanitize_first_rejected_marker,
            image_metrics=(
                self._image_completer.metrics.as_dict()
                if self._image_completer is not None
                and self._image_completer.metrics.any()
                else None
            ),
            # A degraded acquisition (e.g. acquire_failed on the normal read
            # path) surfaces when the source's run() did not set its own health.
            source_health=source_run.health or acquire_health,
            admission_id=admission_id,
            idempotency_key=idempotency_key,
            graph_nodes_by_subtype={
                subtype: tuple(dict.fromkeys(node_ids))
                for subtype, node_ids in graph_write_sets.nodes_by_subtype.items()
            },
            graph_node_attr_changes_by_subtype={
                subtype: {
                    node_id: tuple(dict.fromkeys(fields))
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (
                    graph_write_sets.node_attr_changes_by_subtype.items()
                )
            },
            graph_edges_by_type={
                edge_type: tuple(dict.fromkeys(edge_ids))
                for edge_type, edge_ids in graph_write_sets.edges_by_type.items()
            },
            graph_edge_table_available=graph_write_sets.edge_table_available,
            timings_ms=timings_ms,
            source_delta_batch=source_delta_batch,
            fact_batch_summary=fact_batch_summary,
            terminal_progress=terminal_progress,
            clear_replay_required=self._active_clear_replay_required,
            replay_target=(
                dict(self._active_replay_target)
                if self._active_replay_target is not None else None
            ),
            expected_last_applied_rev=(
                dict(self._active_watermark_cas_expected or {})
                if self._active_watermark_cas_required else None
            ),
            watermark_compare_and_swap=(
                self._active_watermark_cas_required
            ),
            expected_bootstrap_ledger=(
                dict(self._active_bootstrap_ledger_expected)
                if self._active_bootstrap_ledger_expected is not None
                else None
            ),
            next_bootstrap_ledger=(
                dict(self._active_bootstrap_ledger_next)
                if self._active_bootstrap_ledger_next is not None
                else None
            ),
            trace_id=telemetry_identity.trace_id,
            telemetry_span_id=telemetry_identity.span_id,
            private_authority_candidate=self._active_private_authority_candidate,
        )

    def _ensure_zero_chunk_fact_batch_application(self, *, run_id: str) -> None:
        """Construct the admitted envelope for a source that emitted nothing.

        Non-empty runs construct their application during the first bounded
        flush. Empty runs still owe the shared header/terminal-summary
        lifecycle, but must not manufacture a chunk or receipt.
        """
        if self._fact_batch_application is not None:
            return
        context = self._active_run_context
        if (
            context is None
            or context.admission_id is None
            or context.idempotency_key is None
            or context.producer_profile is None
            or self.deployment_name is None
        ):
            return
        from okg.substrate.fact_application import FactBatchApplication
        from okg.substrate.fact_batch import (
            FactBatchHeader,
            PostgresApplicationReceiptStore,
        )

        profile = context.producer_profile
        self._fact_batch_application = FactBatchApplication(
            header=FactBatchHeader(
                schema_version=1,
                run_id=run_id,
                admission_id=context.admission_id,
                producer_profile_fingerprint=profile.profile_fingerprint,
                deployment=self.deployment_name,
                target_branch=self.graph_branch,
                catalog_version_id=profile.catalog_version_id or 0,
                intent="source_sync",
                idempotency_key=context.idempotency_key,
                requested_scope=profile.authority_scope or None,
            ),
            profile=profile,
            receipt_store=PostgresApplicationReceiptStore(self._acquire_conn()),
            apply_graph=lambda _facts, _header: {},
            validate_signature=False,
            # Issue #1431: a zero-chunk envelope applies no fact at all, so
            # nothing can be retracted through it. Still pass the resolved
            # verdict rather than a blanket `True`, so no construction site in
            # this file claims an authority it has not resolved.
            retractions_allowed=bool(self._active_retractions_allowed),
        )

    # ---------- internals ----------

    def _authorize_source_ingest_boundaries(
        self,
        *,
        run_id: str,
        mode: str,
        sync_scope: Mapping[str, Any] | None,
        adapter: SourceAdapter,
    ) -> None:
        self._authorize_nomos_source_boundary(
            surface="source.ingest",
            run_id=run_id,
            mode=mode,
            sync_scope=sync_scope,
            target_kind="source_ingest",
            adapter=adapter,
        )
        if _adapter_requires_live_call_boundary(
            adapter,
            executable_profile=self.executable_profile,
        ):
            self._authorize_nomos_source_boundary(
                surface="source.live_call",
                run_id=run_id,
                mode=mode,
                sync_scope=sync_scope,
                target_kind="source_live_call",
                adapter=adapter,
            )

    def _authorize_source_emit_boundary(
        self,
        *,
        run_id: str,
        mode: str,
        sync_scope: Mapping[str, Any] | None,
        adapter: SourceAdapter,
        source_run_mode: str | None,
        completed_scope: bool | None,
    ) -> None:
        self._authorize_nomos_source_boundary(
            surface="source.emit",
            run_id=run_id,
            mode=mode,
            sync_scope=sync_scope,
            target_kind="source_emit",
            adapter=adapter,
            source_run_mode=source_run_mode,
            completed_scope=completed_scope,
        )

    def _authorize_nomos_source_boundary(
        self,
        *,
        surface: str,
        run_id: str,
        mode: str,
        sync_scope: Mapping[str, Any] | None,
        target_kind: str,
        adapter: SourceAdapter | None = None,
        source_run_mode: str | None = None,
        completed_scope: bool | None = None,
    ) -> None:
        if self.deployment_name is None:
            return
        from okg.substrate.nomos import (
            NomosRuntimeRequest,
            deployment_requires_nomos_runtime,
            evaluate_runtime_boundary_with_active_policy,
        )

        if not deployment_requires_nomos_runtime(self.deployment_name):
            return
        scope = f"deployment:{self.deployment_name}"
        subject = (
            self.source_name
            if self.source_name.startswith("source:")
            else f"source:{self.source_name}"
        )
        conn = self._acquire_conn()
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=self.deployment_name,
            scope=scope,
            request=NomosRuntimeRequest(
                surface=surface,
                subject=subject,
                classification="internal",
                resource={
                    "deployment": self.deployment_name,
                    "scope": scope,
                    "source": self.source_name,
                    "run_id": run_id,
                    "mode": mode,
                    "source_run_mode": source_run_mode,
                    "completed_scope": completed_scope,
                    "source_profile": (
                        self._policy_profile(adapter)
                        if adapter is not None else None
                    ),
                    "adapter_class": (
                        adapter.__class__.__qualname__
                        if adapter is not None else None
                    ),
                    "sync_scope": dict(sync_scope or {}),
                    "graph_branch": self.graph_branch,
                },
            ),
            target={
                "kind": target_kind,
                "source": self.source_name,
                "run_id": run_id,
            },
            performed_obligations=(
                ("audit", "review")
                if surface == "source.live_call"
                else ("audit",)
            ),
        )
        conn.commit()
        if outcome is not None and not outcome.allowed:
            err = SubstrateError(
                "Nomos denied "
                f"{surface}: {outcome.decision.reason_code or outcome.final_effect}",
                data={
                    "deployment_name": self.deployment_name,
                    "source": self.source_name,
                    "run_id": run_id,
                    "surface": surface,
                    "reason_code": outcome.decision.reason_code,
                },
            )
            err.code = "nomos_runtime_denied"
            raise err

    def _delta_contract_dict(
        self,
        acc: SourceDeltaAccumulator,
        *,
        error: Optional[str],
        prior_cursor: Optional[dict[str, Any]],
        next_cursor: Optional[dict[str, Any]],
        run_mode: str,
        sync_scope: Optional[dict[str, Any]],
        requested_sync_scope: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Materialize the validated delta contract as a dict.

        Never raises: a degraded contract is reported with an explicit
        ``contract`` status (`invalid` / `unavailable`) plus the observed
        marker/unidentified counts, so a readiness gate can treat a
        missing or invalid contract as a signal rather than seeing a
        silently absent field.

        The runner reports the mode that actually ran as both requested
        and effective mode: it cannot prove the adapter narrowed work
        within a request, so adapter-level MODE widening evidence is
        still left to the per-adapter migration. Scope is different. The
        runner itself widens the effective scope when an adapter declares
        `SourceRun.effective_scope_complete` (issue #1454), so it KNOWS
        the two differ: ``requested_scope`` is the caller's scope and
        ``effective_scope`` is the scope the run actually claimed
        authority over. A widened run therefore also carries the
        ``fallback`` evidence the contract requires; when nothing widened
        the two scopes are identical and no fallback is emitted.
        """
        if error is not None:
            log.warning(
                "SourceRunner: delta contract degraded for source=%s: %s",
                self.source_name,
                error,
            )
            return {
                "contract": "invalid",
                "source_id": self.source_name,
                "reason": error,
                "observed_progress_markers": acc.progress_marker_count,
                "unidentified_facts": acc.unidentified_fact_count,
            }
        if next_cursor is None and acc.progress_marker_count == 0:
            return {
                "contract": "unavailable",
                "source_id": self.source_name,
                "reason": "no_next_frontier_or_progress",
            }
        contract_mode = (
            run_mode if run_mode in SOURCE_CONTRACT_RUN_MODES
            else "scope_complete"
        )
        requested_scope = (
            requested_sync_scope
            if requested_sync_scope is not None
            else sync_scope
        ) or None
        effective_scope = sync_scope or None
        fallback: Optional[SourceFallbackEvidence] = None
        if contract_scope_widened(requested_scope, effective_scope):
            # The runner cleared the requested scope after the adapter
            # declared it had read the complete configured scope. That is a
            # widening the contract must not report as an identity, and a
            # widened batch is invalid without fallback evidence.
            fallback = SourceFallbackEvidence(
                reason="adapter_declared_effective_scope_complete",
                requested_mode=contract_mode,
                effective_mode=contract_mode,
                measured_input_size=acc.changed_record_count,
                affected_scopes=contract_scope_keys(requested_scope),
                readiness_impact=(
                    "run claimed complete-scope deletion authority; "
                    "outputs keyed on any record of this source may change"
                ),
                requested_scope=requested_scope,
                effective_scope=effective_scope,
            )
        try:
            summary = acc.summary(
                self.source_name,
                previous_frontier=prior_cursor,
                next_frontier=next_cursor,
                run_mode=contract_mode,
                requested_scope=requested_scope,
                effective_scope=effective_scope,
                fallback=fallback,
            )
        except SourceSyncContractError as exc:
            log.warning(
                "SourceRunner: delta contract validation failed for "
                "source=%s: %s",
                self.source_name,
                exc,
            )
            return {
                "contract": "invalid",
                "source_id": self.source_name,
                "reason": str(exc),
                "observed_progress_markers": acc.progress_marker_count,
                "unidentified_facts": acc.unidentified_fact_count,
            }
        if acc.unidentified_fact_count:
            # A graph-writing fact with no stable source_record_id was dropped
            # from the change set (SourceDeltaAccumulator.observe), so it
            # produces NO invalidation key. A change frontier built from this
            # batch would then silently UNDER-COVER those records — a dependent
            # output keyed on the unnamed record would be wrongly reused (stale).
            # Report the contract invalid rather than labeling a batch that
            # dropped identities `valid`, so the frontier widens (force-full) and
            # a readiness gate sees a loud signal. (deltaflow review follow-up B)
            log.warning(
                "SourceRunner: %d graph-writing fact(s) from source=%s carry no "
                "stable source_record_id; reporting delta contract invalid (no "
                "invalidation key for those records)",
                acc.unidentified_fact_count,
                self.source_name,
            )
            summary["contract"] = "invalid"
            summary["reason"] = (
                f"{acc.unidentified_fact_count} graph-writing fact(s) without a "
                "stable source_record_id"
            )
            return summary
        summary["contract"] = "valid"
        return summary

    def _canonicalize_facts(
        self,
        facts: list,
    ) -> list:
        """Rewrite raw IDs on each fact through the registered
        canonicalizer chain. Returns a new list of facts with
        canonicalized identities; the original `source_record_id`
        provenance is preserved verbatim (it carries the
        adapter's notion of "where this fact came from", which
        stays raw for audit).

        NodeFact / EdgeFact are frozen dataclasses, so we
        construct fresh instances when an ID changes;
        unchanged facts are returned by reference.
        """
        from dataclasses import replace

        from okg.substrate.canonicalize import canonicalize_id

        snapshot = self._active_pipeline_snapshot
        chain = (
            snapshot.canonicalizers
            if snapshot is not None else tuple(self.canonicalizers)
        )
        out: list = []
        for f in facts:
            if isinstance(f, NodeFact):
                new_id = canonicalize_id(f.node_id, chain)
                if new_id == f.node_id:
                    out.append(f)
                else:
                    out.append(replace(f, node_id=new_id))
            elif isinstance(f, EdgeFact):
                new_src = canonicalize_id(f.src, chain)
                new_dst = canonicalize_id(f.dst, chain)
                if new_src == f.src and new_dst == f.dst:
                    out.append(f)
                else:
                    out.append(replace(f, src=new_src, dst=new_dst))
            else:
                out.append(f)
        return out

    def _sanitize_facts(
        self,
        facts: list,
        *,
        warn_budget: Optional[_SanitizeWarnBudget] = None,
    ) -> tuple[list, SanitizeSummary]:
        """Admission NUL normalization — PACT
        `sanitize-fact-text-at-admission` (issue #638).

        Postgres text/jsonb cannot store NUL (U+0000), and the
        chronos splice serializes attrs verbatim, so ONE
        pathological string used to abort the COPY for the whole
        source run. This shared-normalizer pass runs before
        canonicalize/lineage-stamp/COPY:

        - attr values (nested; string keys AND values): each NUL
          occurrence is replaced with U+FFFD and counted;
        - identity-bearing fields (node_id/src/dst/subtype/
          edge_type/source_record_id/source_revision) are NEVER
          silently rewritten — a NUL there rejects that fact loudly
          (skipped + counted + warned) while the rest of the run
          continues.
        """
        from okg.substrate.ingest.sanitize import (
            fact_identity_preview,
            sanitize_fact,
        )

        summary = SanitizeSummary()
        out: list = []
        # Cap the per-fact detail lines so a pathological stream
        # cannot flood the log; the end-of-run summary warning
        # always carries the full counts. The budget is per RUN when
        # the caller threads one through (`_flush_chunk` does);
        # standalone calls fall back to a per-call budget.
        if warn_budget is None:
            warn_budget = _SanitizeWarnBudget()
        for fact in facts:
            if not isinstance(fact, (NodeFact, EdgeFact)):
                out.append(fact)
                continue
            result = sanitize_fact(fact)
            if result.rejected:
                summary.rejected_fact_count += 1
                preview = fact_identity_preview(fact)
                if summary.first_rejected is None:
                    summary.first_rejected = preview
                if warn_budget.remaining > 0:
                    warn_budget.remaining -= 1
                    log.warning(
                        "SourceRunner: rejecting fact with NUL (U+0000) "
                        "in identity field(s) %s: source=%s fact=%s "
                        "(identity is never silently rewritten; see PACT "
                        "sanitize-fact-text-at-admission)",
                        ", ".join(result.rejected_fields),
                        self.source_name,
                        preview,
                    )
                elif not warn_budget.notice_emitted:
                    warn_budget.notice_emitted = True
                    log.warning(
                        "SourceRunner: further per-fact NUL-rejection "
                        "detail suppressed for this run from source=%s "
                        "(full counts in the end-of-run summary)",
                        self.source_name,
                    )
                continue
            summary.sanitized_value_count += result.sanitized_value_count
            out.append(result.fact)
        return out, summary

    def _apply_pii_posture(self, facts: list) -> list:
        """At-rest PII transform — PACT `bind-at-rest-anonymization`.

        For a not-store-raw source (``self.pii_posture.active``), pseudonymize /
        mask each fact's PII attributes (direct/quasi identifiers →
        vault pseudonyms so joins survive, other sensitive classes → mask token,
        detected free-text spans → mask token) BEFORE canonicalize / lineage /
        COPY, so no downstream stage ever sees raw PII. A store_raw source (the
        default, ``pii_posture`` None or ``store_raw=True``) is returned
        untouched — no existing source changes behavior.

        Fail-closed: a fact carrying free text with no detector installed is HELD
        (never stored raw); if ANY fact is held the run RAISES
        :class:`PiiFactsHeld`, so the source watermark stays unchanged and the
        held facts are re-processed once a detector is available — the run never
        silently stores raw or silently drops them.
        """
        snapshot = self._active_pipeline_snapshot
        posture = (
            snapshot.pii_posture
            if snapshot is not None else self.pii_posture
        )
        if posture is None or not posture.active:
            return facts
        from okg.substrate.ingest.pii_posture import PiiFactsHeld
        from okg.substrate.ingest.pii_sanitize import sanitize_fact_pii

        out: list = []
        held = 0
        held_reason: Optional[str] = None
        for fact in facts:
            if not isinstance(fact, (NodeFact, EdgeFact)):
                out.append(fact)
                continue
            result = sanitize_fact_pii(
                fact,
                classifier=posture.classifier,
                store_raw=False,
                pseudonym_service=posture.pseudonym_service,
                detector=posture.detector,
            )
            if result.held:
                held += 1
                held_reason = result.held_reason or held_reason
                continue
            out.append(result.fact)
        if held:
            raise PiiFactsHeld(
                f"SourceRunner: holding {held} fact(s) from not-store-raw "
                f"source={self.source_name!r}: {held_reason} — no raw PII was "
                "stored and the source watermark is unchanged; install the "
                "'privacy' extra so free-text PII can be detected and masked",
                held=held,
                reason=held_reason or "pii_held",
            )
        return out

    def _stamp_source_lineage_on_facts(
        self,
        facts: list,
        *,
        policy_fingerprint: str | None,
    ) -> list:
        from dataclasses import replace

        from okg.substrate.nomos.source_lineage import (
            stamp_attrs_with_source_lineage,
        )

        out: list = []
        for fact in facts:
            if isinstance(fact, NodeFact) and fact.op.upper() == "I":
                out.append(replace(
                    fact,
                    attrs=stamp_attrs_with_source_lineage(
                        fact.attrs,
                        source_id=self.source_name,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                        policy_fingerprint=policy_fingerprint,
                    ),
                ))
            elif isinstance(fact, EdgeFact) and fact.op.upper() == "I":
                out.append(replace(
                    fact,
                    attrs=stamp_attrs_with_source_lineage(
                        fact.attrs,
                        source_id=self.source_name,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                        policy_fingerprint=policy_fingerprint,
                    ),
                ))
            else:
                out.append(fact)
        return out

    def _prior_record_set(
        self,
        prior_cursor: Optional[dict[str, Any]],
    ) -> RecordSet:
        if not prior_cursor:
            return RecordSet()
        raw = prior_cursor.get("record_set")
        if raw is None and prior_cursor.get(_RECORD_SET_REF_KEY) is not None:
            raw = self._load_external_record_set(
                prior_cursor.get(_RECORD_SET_REF_KEY),
            )
        if raw is None:
            return RecordSet()
        try:
            return RecordSet.from_cursor(raw)
        except ValueError as exc:
            raise SubstrateError(
                "SourceRunner: stored record_set cursor is invalid",
                data={"record_set_error": str(exc)},
                fix_hint=(
                    "repair or clear okg.source_watermarks."
                    "last_applied_rev->'record_set' for this source"
                ),
            ) from exc

    def _prior_cursor_for_adapter(
        self,
        *,
        mode: str,
        prior_cursor: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if mode not in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES:
            return prior_cursor
        if not isinstance(prior_cursor, dict):
            return prior_cursor
        has_record_set_authority = (
            prior_cursor.get("record_set") is not None
            or prior_cursor.get(_RECORD_SET_REF_KEY) is not None
        )
        if not has_record_set_authority:
            return prior_cursor
        if not self._prior_record_set_chronos_verified(prior_cursor):
            cursor = dict(prior_cursor)
            cursor.pop("record_set", None)
            cursor.pop(_RECORD_SET_REF_KEY, None)
            cursor.pop(_RECORD_SET_SUMMARY_KEY, None)
            cursor.pop(_RECORD_SET_CHRONOS_PROOF_KEY, None)
            return cursor or None
        ref = prior_cursor.get(_RECORD_SET_REF_KEY)
        if ref is None:
            return prior_cursor
        if self._external_record_set_ref_reachable(ref):
            return prior_cursor
        cursor = dict(prior_cursor)
        cursor.pop(_RECORD_SET_REF_KEY, None)
        cursor.pop(_RECORD_SET_SUMMARY_KEY, None)
        cursor.pop(_RECORD_SET_CHRONOS_PROOF_KEY, None)
        return cursor

    @staticmethod
    def _cursor_without_bootstrap_authority(
        cursor: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        """Hide the runner-reserved authority envelope from adapters."""
        if not isinstance(cursor, dict):
            return cursor
        cleaned = dict(cursor)
        cleaned.pop(BOOTSTRAP_AUTHORITY_CURSOR_KEY, None)
        return cleaned or None

    @staticmethod
    def _cursor_for_authoritative_replay(
        prior_cursor: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        """Reset adapter resume state while retaining retraction authority."""
        if not isinstance(prior_cursor, dict):
            return None
        retained = {
            key: prior_cursor[key]
            for key in (
                "record_set",
                _RECORD_SET_REF_KEY,
                _RECORD_SET_SUMMARY_KEY,
                _RECORD_SET_CHRONOS_PROOF_KEY,
            )
            if key in prior_cursor
        }
        return retained or None

    @staticmethod
    def _record_set_with_prior(
        prior: RecordSet,
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> RecordSet:
        """Carry the prior forward around a declared replacement slice.

        Issue #2145 review S2. The bound this merge promises is that every
        prior record outside `replacement_keys` survives untouched. A slice
        that declares {b} and also carries a record for a breaks it in the
        direction the retraction diff cannot see: the entry for a is not
        retracted, it is REWRITTEN. Refuse the undeclared key rather than
        apply it.
        """
        undeclared = sorted(set(current.records) - set(replacement_keys))
        if undeclared:
            raise SubstrateError(
                "record-set replacement slice carries records it did not "
                "declare",
                data={
                    "reason_code": (
                        "record_set_replacement_slice_undeclared_key"
                    ),
                    "undeclared_record_keys": undeclared[:20],
                    "undeclared_record_key_count": len(undeclared),
                },
                fix_hint=(
                    "declare every record key the slice emits in "
                    "record_set_replacement_keys, or emit only the records "
                    "the declared keys name"
                ),
            )
        records = {
            key: value
            for key, value in prior.records.items()
            if key not in replacement_keys
        }
        records.update(current.records)
        return RecordSet(records=records)

    def _retained_record_set_from_chronos(
        self,
        current: RecordSet,
    ) -> RecordSet:
        """Adopt already-published source truth during an opt-in migration.

        Append-retention adapters may begin emitting record-set authority after
        source-owned graph history already exists. Treating an absent legacy
        cursor as an empty prior set would retract that retained history;
        refusing the coverage check would instead wedge every replay. This
        opt-in bootstrap converts only currently visible, source-owned Chronos
        rows that are not already declared by ``current`` into synthetic prior
        records. Later replacement slices carry those records forward.
        """
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return RecordSet()
        session = self._checkout_graph_branch(graph_store)
        current_nodes, current_edges = self._record_set_identities(current)
        records: dict[str, RecordEmission] = {}
        for row in session.query(
            """
            SELECT node_id, subtype
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": self.source_name},
        ):
            node_id = str(row["node_id"])
            if node_id in current_nodes:
                continue
            key = (
                "__chronos_retained__:node:"
                + identity_fingerprint({
                    "node_id": node_id,
                    "subtype": str(row["subtype"]),
                })
            )
            records[key] = RecordEmission(
                primary=RecordNode(
                    node_id=node_id,
                    subtype=str(row["subtype"]),
                )
            )
        # Ordered by edge_id (edge_source_edgeid_visible) since okg#3112: no
        # index orders a source's edges by (src, edge_type, dst) any more,
        # and the records are keyed by identity and sorted when serialized.
        for row in session.query(
            f"""
            SELECT src, edge_type, dst
              FROM {_coverage_edges(session)} AS owned
             WHERE source = :source
             ORDER BY edge_id
            """,
            {"source": self.source_name},
        ):
            identity = (
                str(row["src"]),
                str(row["edge_type"]),
                str(row["dst"]),
            )
            if identity in current_edges:
                continue
            key = (
                "__chronos_retained__:edge:"
                + identity_fingerprint({
                    "src": identity[0],
                    "edge_type": identity[1],
                    "dst": identity[2],
                })
            )
            records[key] = RecordEmission(
                edges=[EdgeKey(
                    src=identity[0],
                    edge_type=identity[1],
                    dst=identity[2],
                )]
            )
        if records:
            log.info(
                "SourceRunner adopted retained Chronos authority: "
                "source=%s record_count=%d",
                self.source_name,
                len(records),
            )
        return RecordSet(records=records)

    def _sanitize_record_set_authority(
        self,
        record_set: RecordSet,
    ) -> RecordSet:
        from okg.substrate.ingest.sanitize import sanitize_record_set

        outcome = sanitize_record_set(record_set)
        if outcome.changed:
            log.warning(
                "SourceRunner sanitized NUL (U+0000) from record-set "
                "authority: source=%s dropped_record_count=%d "
                "dropped_node_count=%d dropped_edge_count=%d "
                "sanitized_value_count=%d",
                self.source_name,
                outcome.dropped_record_count,
                outcome.dropped_node_count,
                outcome.dropped_edge_count,
                outcome.sanitized_value_count,
            )
        return outcome.record_set

    def _stage_record_authority_stream(
        self, stream: RecordAuthorityStream,
    ) -> PrivateAuthorityCandidate:
        context = self._active_run_context
        incarnation = self._chronos_branch_incarnation()
        if context is None or not incarnation:
            raise SubstrateError(
                "indexed authority staging requires run and graph incarnation",
                data={"source_name": self.source_name},
            )
        return ProductionAuthorityStore(self._acquire_conn()).stage_stream(
            self.source_name, incarnation, context.run_id, stream,
        )

    def _prior_record_set_chronos_verified(
        self,
        prior_cursor: Optional[dict[str, Any]],
    ) -> bool:
        if not isinstance(prior_cursor, dict):
            return False
        has_inline_record_set = prior_cursor.get("record_set") is not None
        ref = prior_cursor.get(_RECORD_SET_REF_KEY)
        has_external_record_set = ref is not None
        if not has_inline_record_set and not has_external_record_set:
            return False
        proof = prior_cursor.get(_RECORD_SET_CHRONOS_PROOF_KEY)
        if not isinstance(proof, dict):
            return False
        if not (
            proof.get("ok") is True
            and proof.get("source_name") == self.source_name
            and proof.get("graph_branch") == self.graph_branch
            and proof.get("schema") == _RECORD_SET_CHRONOS_PROOF_SCHEMA
        ):
            return False
        proof_incarnation = proof.get("graph_incarnation")
        if not isinstance(proof_incarnation, str) or not proof_incarnation:
            return False
        if proof_incarnation != self._chronos_branch_incarnation():
            return False
        summary = prior_cursor.get(_RECORD_SET_SUMMARY_KEY)
        if not self._record_set_summary_chronos_verified(summary):
            return False
        if has_external_record_set:
            return self._record_set_ref_matches_source(ref)
        return True

    def _record_set_summary_chronos_verified(self, summary: Any) -> bool:
        return (
            isinstance(summary, Mapping)
            and summary.get("schema") == _RECORD_SET_SUMMARY_SCHEMA
            and summary.get("source_name") == self.source_name
            and summary.get("graph_branch") == self.graph_branch
            and summary.get("chronos_graph_verified") is True
        )

    def _record_set_ref_matches_source(self, ref: Any) -> bool:
        if not isinstance(ref, Mapping):
            return False
        if ref.get("schema") != _RECORD_SET_REF_SCHEMA:
            return False
        if ref.get("source") != self.source_name:
            return False
        storage = ref.get("storage")
        if storage == "okg.source_record_set_blobs":
            return bool(str(ref.get("sha256") or ""))
        return storage == "okg.source_record_sets"

    def _external_record_set_ref_reachable(self, ref: Any) -> bool:
        """Report whether the referenced external record set exists.

        A missing header returns False, as before. A pieced header whose
        piece count does not match raises ``RecordSetPiecesError``: it must
        never read as "unreachable" and so as an empty prior (issue #3008).
        """
        if not self._record_set_ref_matches_source(ref):
            return False
        ref_payload = ref if isinstance(ref, Mapping) else {}
        storage = str(ref_payload.get("storage") or "")
        digest = str(ref_payload.get("sha256") or "")
        self._ensure_source_record_sets_table()
        with self._acquire_conn().cursor() as cur:
            if storage == "okg.source_record_sets":
                cur.execute(
                    """
                    SELECT 1
                      FROM okg.source_record_sets
                     WHERE source = %s
                     LIMIT 1
                    """,
                    (self.source_name,),
                )
            else:
                # A pieced header commits with its pieces in one transaction,
                # so a count that disagrees means pieces were removed or
                # moved afterwards. Refuse by name: reporting it unreachable
                # would let a scope-complete run drop the reference and read
                # an empty prior, which never retracts the stale records.
                cur.execute(
                    """
                    SELECT b.piece_count,
                           (SELECT count(*)
                              FROM okg.source_record_set_blob_pieces p
                             WHERE p.write_id = b.piece_write_id
                               AND p.source = b.source)
                      FROM okg.source_record_set_blobs b
                     WHERE b.source = %s
                       AND b.sha256 = %s
                    """,
                    (self.source_name, digest),
                )
                row = cur.fetchone()
                if row is None:
                    return False
                piece_count, found = row
                if piece_count is not None and int(found) != int(piece_count):
                    raise RecordSetPiecesError(
                        "SourceRunner: record-set authority has "
                        f"{int(found)} stored pieces, header records "
                        f"{int(piece_count)}",
                        data={
                            "source": self.source_name,
                            "sha256": digest,
                            "piece_count": int(piece_count),
                            "found_piece_count": int(found),
                        },
                        fix_hint=(
                            "the stored record set cannot be trusted; re-run "
                            "the source with a full read (reset its watermark)"
                        ),
                    )
                return True
            return cur.fetchone() is not None

    def _cursor_with_record_set(
        self,
        next_cursor: Optional[dict[str, Any]],
        record_set: RecordSet,
        *,
        chronos_graph_verified: bool = False,
        covered_nodes: set[str] | None = None,
        covered_edges: set[tuple[str, str, str]] | None = None,
    ) -> dict[str, Any]:
        self._tail_authority_ordinal = getattr(self, "_tail_authority_ordinal", 0) + 1
        self._tail_verified = bool(chronos_graph_verified)
        authority_ordinal = self._tail_authority_ordinal
        cursor = dict(next_cursor or {})
        with self._tail_phase("record_set.proof"):
            if covered_nodes is None or covered_edges is None:
                covered_nodes, covered_edges = self._record_set_identities(
                    record_set,
                )
            substitute_node_count = sum(
                mode == "substitute"
                for mode in self._record_set_node_ownership_modes(
                    record_set,
                ).values()
            )
            summary = {
                "schema": _RECORD_SET_SUMMARY_SCHEMA,
                "admission_sanitization": _RECORD_SET_SANITIZATION_SCHEMA,
                "source_name": self.source_name,
                "graph_branch": self.graph_branch,
                "record_count": len(record_set.records),
                "node_count": len(covered_nodes),
                "edge_count": len(covered_edges),
                "chronos_graph_verified": bool(chronos_graph_verified),
            }
            if substitute_node_count:
                summary["substitute_node_count"] = substitute_node_count
            proof = (
                {
                    "ok": True,
                    "source_name": self.source_name,
                    "graph_branch": self.graph_branch,
                    "schema": _RECORD_SET_CHRONOS_PROOF_SCHEMA,
                    **(
                        {"graph_incarnation": graph_incarnation}
                        if (
                            graph_incarnation
                            := self._chronos_branch_incarnation()
                        )
                        else {}
                    ),
                }
                if chronos_graph_verified else None
            )
        # The RecordSet itself, not its to_cursor() dict: the authority
        # writer streams it one record at a time (issue #3008).
        cursor.update(self._record_set_authority_cursor(
            record_set,
            summary=summary,
            proof=proof,
            authority_ordinal=authority_ordinal,
        ))
        if chronos_graph_verified:
            cursor[_RECORD_SET_CHRONOS_PROOF_KEY] = proof
        else:
            cursor.pop(_RECORD_SET_CHRONOS_PROOF_KEY, None)
        return cursor

    def _chronos_branch_incarnation(self) -> str | None:
        """Return the stable identity of the current mutable branch lineage.

        A rollover deletes and recreates the mutable branch under the same
        user-facing name. Chronos assigns that new row a fresh ``created_at``;
        ordinary fact application and checkpoints do not change it. Binding a
        record-set proof to this value prevents a proof from an older lineage
        from authorizing a no-op skip against the replacement branch.
        """
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return None
        self._checkout_graph_branch(graph_store)
        # Issue #2920: this raw read runs on the Chronos session, which is not
        # in autocommit mode, so it opens a transaction. The runner calls this
        # before the source adapter runs; leaving that transaction open kept the
        # session "idle in transaction" for the whole adapter run, and a run
        # longer than the idle bound was killed (SQLSTATE 25P03). End the
        # transaction this read opened. It held nothing the runner relies on:
        # one READ COMMITTED statement keeps no snapshot across statements, and
        # the runner re-reads the incarnation after the adapter returns and
        # again before it trusts a record-set proof. A transaction someone else
        # already opened on this session is left alone.
        db = graph_store.context.db
        opened_here = not db.in_transaction
        try:
            row = db.execute(
                """
                SELECT created_at
                  FROM _chronos_branch_interval_branches
                 WHERE branch_id = ?
                """,
                (self.graph_branch,),
            ).fetchone()
        except BaseException:
            if opened_here and db.in_transaction:
                db.rollback()
            raise
        if opened_here and db.in_transaction:
            db.commit()
        if row is None:
            return None
        try:
            value = row["created_at"]
        except (KeyError, TypeError):
            value = row[0]
        text = str(value or "")
        return text or None

    def _preserve_prior_record_set_authority(
        self,
        next_cursor: Optional[dict[str, Any]],
        prior_cursor: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not isinstance(prior_cursor, dict):
            return next_cursor
        if next_cursor is None:
            return None
        cursor = dict(next_cursor)
        authority_keys = ("record_set", _RECORD_SET_REF_KEY)
        cursor_has_authority = any(key in cursor for key in authority_keys)
        has_prior_authority = any(
            key in prior_cursor for key in authority_keys
        )
        carried_prior_authority = (
            cursor_has_authority
            and all(
                (key in cursor) == (key in prior_cursor)
                and cursor.get(key) == prior_cursor.get(key)
                for key in authority_keys
            )
        )
        prior_summary = prior_cursor.get(_RECORD_SET_SUMMARY_KEY)
        authority_is_sanitized = (
            isinstance(prior_summary, Mapping)
            and prior_summary.get("admission_sanitization")
            == _RECORD_SET_SANITIZATION_SCHEMA
        )
        if (
            has_prior_authority
            and not authority_is_sanitized
            and (not cursor_has_authority or carried_prior_authority)
        ):
            # Incomplete runs may omit durable authority, while probe-skipped
            # runs copy the prior cursor before preservation. Legacy cursors
            # predate the admission marker, so load and sanitize either form
            # once; this also covers escaped U+0000 in record_set_text blobs.
            prior_record_set = self._sanitize_record_set_authority(
                self._prior_record_set(prior_cursor),
            )
            for key in (
                *authority_keys,
                _RECORD_SET_SUMMARY_KEY,
                _RECORD_SET_CHRONOS_PROOF_KEY,
            ):
                cursor.pop(key, None)
            cursor.update(self._cursor_with_record_set(
                {},
                prior_record_set,
                chronos_graph_verified=(
                    self._prior_record_set_chronos_verified(prior_cursor)
                ),
            ))
        elif not cursor_has_authority:
            if _RECORD_SET_REF_KEY in prior_cursor:
                cursor[_RECORD_SET_REF_KEY] = prior_cursor[
                    _RECORD_SET_REF_KEY
                ]
            elif "record_set" in prior_cursor:
                cursor.update(self._record_set_authority_cursor(
                    prior_cursor["record_set"],
                    summary=prior_summary,
                    proof=prior_cursor.get(_RECORD_SET_CHRONOS_PROOF_KEY),
                ))
        for key in (_RECORD_SET_SUMMARY_KEY, _RECORD_SET_CHRONOS_PROOF_KEY):
            if key not in cursor and key in prior_cursor:
                cursor[key] = prior_cursor[key]
        return cursor

    def _record_set_authority_cursor(
        self,
        record_set_cursor: RecordSet | Mapping[str, Any],
        *,
        summary: Any,
        proof: Any,
        authority_ordinal: int | None = None,
    ) -> dict[str, Any]:
        """Return the cursor keys that carry a record set's authority.

        Small record sets stay inline in the cursor; larger ones are stored
        externally and referenced by digest. The canonical JSON text is
        streamed one record at a time and hashed as it goes, so the whole
        text is never held in memory: at most the larger of the inline and
        JSONB thresholds is buffered, and past that the text goes to the
        database in bounded pieces as it is produced (issue #3008).
        """
        self._tail_authority_ordinal = (
            authority_ordinal if authority_ordinal is not None
            else getattr(self, "_tail_authority_ordinal", 0) + 1
        )
        self._tail_verified = bool(isinstance(summary, dict) and summary.get("chronos_graph_verified"))
        if not isinstance(record_set_cursor, (RecordSet, Mapping)):
            # default=str would otherwise serialize an arbitrary object's
            # repr as if it were a record set.
            raise TypeError(
                "record-set authority must be a RecordSet or a cursor mapping, "
                f"not {type(record_set_cursor).__name__}"
            )
        summary_payload = summary if isinstance(summary, dict) else {}
        proof_payload = proof if isinstance(proof, dict) else None
        inline_max = self._inline_record_set_max_bytes()
        buffer_max = max(inline_max, self._record_set_jsonb_max_bytes())
        hasher = hashlib.sha256()
        buffered: list[tuple[str, int]] = []
        payload_bytes = 0
        overflow = False
        chunks = _iter_record_set_json(record_set_cursor)
        with self._tail_phase("record_set.serialize"):
            for chunk in chunks:
                # The one encode of this chunk: size and digest share it.
                encoded = chunk.encode("utf-8")
                hasher.update(encoded)
                chunk_bytes = len(encoded)
                del encoded
                buffered.append((chunk, chunk_bytes))
                payload_bytes += chunk_bytes
                if payload_bytes > buffer_max:
                    overflow = True
                    break
        if not overflow and payload_bytes <= inline_max:
            if isinstance(record_set_cursor, RecordSet):
                with self._tail_phase("record_set.to_cursor"):
                    inline_cursor: Any = record_set_cursor.to_cursor()
            else:
                inline_cursor = record_set_cursor
            out: dict[str, Any] = {
                "record_set": inline_cursor,
                _RECORD_SET_SUMMARY_KEY: summary_payload,
            }
            if proof_payload is not None:
                out[_RECORD_SET_CHRONOS_PROOF_KEY] = proof_payload
            return out
        if overflow:
            def remaining() -> Iterator[tuple[str, int]]:
                yield from buffered
                buffered.clear()
                with self._tail_phase("record_set.serialize_stream"):
                    for chunk in chunks:
                        encoded = chunk.encode("utf-8")
                        hasher.update(encoded)
                        yield chunk, len(encoded)

            digest = self._store_pieced_record_set(
                remaining(),
                digest_fn=hasher.hexdigest,
                summary=summary_payload,
                proof=proof_payload,
            )
        else:
            digest = hasher.hexdigest()
            payload = "".join(chunk for chunk, _ in buffered)
            buffered.clear()
            self._store_external_record_set(
                payload,
                summary=summary_payload,
                proof=proof_payload,
                digest=digest,
                payload_bytes=payload_bytes,
            )
            del payload
        out = {
            _RECORD_SET_REF_KEY: {
                "schema": _RECORD_SET_REF_SCHEMA,
                "storage": "okg.source_record_set_blobs",
                "source": self.source_name,
                "sha256": digest,
            },
            _RECORD_SET_SUMMARY_KEY: summary_payload,
        }
        if proof_payload is not None:
            out[_RECORD_SET_CHRONOS_PROOF_KEY] = proof_payload
        return out

    @staticmethod
    def _inline_record_set_max_bytes() -> int:
        raw = os.environ.get("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES")
        if raw is None:
            return _DEFAULT_INLINE_RECORD_SET_MAX_BYTES
        try:
            value = int(raw)
        except ValueError:
            return _DEFAULT_INLINE_RECORD_SET_MAX_BYTES
        return max(0, value)

    def _store_external_record_set(
        self,
        record_set_json: str,
        *,
        summary: dict[str, Any],
        proof: dict[str, Any] | None,
        digest: str,
        payload_bytes: int | None = None,
    ) -> None:
        """Store one already-serialized record set under ``(source, digest)``.

        Up to the JSONB threshold it is one header row, as before. Above it
        the text goes to ``okg.source_record_set_blob_pieces`` (issue #3008);
        ``record_set_text`` is no longer written, and is only read for rows
        stored before pieces existed.
        """
        self._ensure_source_record_sets_table()
        if payload_bytes is None:
            # No caller-supplied size: this is the only place the payload is
            # measured, so encoding here is the first and only encode.
            with self._tail_phase("record_set.storage_utf8_size"):
                json_bytes = len(record_set_json.encode("utf-8"))
        else:
            # payload_bytes must be derived from this same string by the
            # caller, which already encoded it. Re-encoding here to check that
            # would undo the whole point, so this guard uses the one bound a
            # caller cannot fake cheaply: UTF-8 never uses fewer bytes than the
            # string has code points.
            if payload_bytes < len(record_set_json):
                raise ValueError(
                    "payload_bytes is smaller than the payload's character "
                    f"count ({payload_bytes} < {len(record_set_json)}); it "
                    "must be len(record_set_json.encode('utf-8')) for this "
                    "same string"
                )
            json_bytes = payload_bytes
        if json_bytes > self._record_set_jsonb_max_bytes():
            self._store_pieced_record_set(
                iter(((record_set_json, json_bytes),)),
                digest_fn=lambda: digest,
                summary=summary,
                proof=proof,
            )
            return
        # Ordinals are 1-based to match the $N placeholders PostgreSQL names in
        # its own error: the payload is record_set ($2).
        with self._tail_phase(
            "record_set.execute", payload_bytes=json_bytes, parameter_count=5,
            payload_parameter_ordinal=2,
            payload_parameter_type="str", payload_characters=len(record_set_json),
        ):
            with self._acquire_conn().cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.source_record_set_blobs
                      (source, record_set, record_set_summary,
                       chronos_proof, sha256, record_set_text, updated_at)
                    VALUES (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s, NULL, now())
                    ON CONFLICT (source, sha256) DO UPDATE SET
                      record_set_summary = EXCLUDED.record_set_summary,
                      chronos_proof = EXCLUDED.chronos_proof,
                      updated_at = EXCLUDED.updated_at
                    """,
                    (
                        self.source_name,
                        record_set_json,
                        json.dumps(summary, default=str),
                        (
                            json.dumps(proof, default=str)
                            if proof is not None else None
                        ),
                        digest,
                    ),
                )

    def _store_pieced_record_set(
        self,
        chunks: Iterable[tuple[str, int]],
        *,
        digest_fn: Callable[[], str],
        summary: dict[str, Any],
        proof: dict[str, Any] | None,
    ) -> str:
        """Write a record set as ordered pieces, then its header; return the digest.

        ``chunks`` are ``(text, utf8_bytes)`` in order; they are consumed
        lazily and each piece is sent as soon as it fills, so memory holds at
        most one piece. ``digest_fn`` is called once the chunks are exhausted
        and must return the SHA-256 of their whole text.

        Pieces and header share one transaction (a savepoint when the caller
        already holds one), with the header last, and a deferred foreign key
        refuses a piece without a header at commit: a reader can never see a
        header whose pieces are still being written. When a header for this
        digest already exists, the content is identical by construction, so
        the new pieces are rolled back and only the summary, proof and
        timestamp are refreshed, as the single-row upsert did.
        """
        self._ensure_source_record_sets_table()
        piece_max = self._record_set_piece_max_bytes()
        write_id = uuid.uuid4()
        conn = self._acquire_conn()
        piece_count = 0
        piece_bytes = 0
        digest = ""
        inserted = False
        summary_json = json.dumps(summary, default=str)
        proof_json = json.dumps(proof, default=str) if proof is not None else None
        try:
            with conn.transaction() as tx:
                with conn.cursor() as cur:
                    with self._tail_phase(
                        "record_set.pieces", piece_max_bytes=piece_max,
                        parameter_count=4, payload_parameter_ordinal=4,
                        payload_parameter_type="str",
                    ):
                        for piece, nbytes in _iter_record_set_pieces(
                            chunks, piece_max,
                        ):
                            cur.execute(
                                """
                                INSERT INTO okg.source_record_set_blob_pieces
                                  (write_id, piece_no, source, piece)
                                VALUES (%s, %s, %s, %s)
                                """,
                                (write_id, piece_count, self.source_name, piece),
                            )
                            piece_count += 1
                            piece_bytes += nbytes
                        digest = digest_fn()
                    with self._tail_phase(
                        "record_set.pieces_header", payload_bytes=piece_bytes,
                        piece_count=piece_count,
                    ):
                        cur.execute(
                            """
                            INSERT INTO okg.source_record_set_blobs
                              (source, sha256, record_set, record_set_summary,
                               chronos_proof, record_set_text, piece_write_id,
                               piece_count, piece_bytes, updated_at)
                            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                                    NULL, %s, %s, %s, now())
                            ON CONFLICT (source, sha256) DO NOTHING
                            RETURNING 1
                            """,
                            (
                                self.source_name,
                                digest,
                                json.dumps(_PIECED_RECORD_SET_MARKER),
                                summary_json,
                                proof_json,
                                write_id,
                                piece_count,
                                piece_bytes,
                            ),
                        )
                        inserted = cur.fetchone() is not None
                if not inserted:
                    # Same digest already stored: drop this write's pieces.
                    raise psycopg.Rollback(tx)
        finally:
            # Release the rest of the stream (and its tail phase) promptly.
            close = getattr(chunks, "close", None)
            if callable(close):
                close()
        if not inserted:
            with self._tail_phase("record_set.pieces_reuse"):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE okg.source_record_set_blobs
                           SET record_set_summary = %s::jsonb,
                               chronos_proof = %s::jsonb,
                               updated_at = now()
                         WHERE source = %s
                           AND sha256 = %s
                        """,
                        (summary_json, proof_json, self.source_name, digest),
                    )
        return digest

    def _load_external_record_set(self, ref: Any) -> Any:
        self._ensure_source_record_sets_table()
        ref_payload = ref if isinstance(ref, Mapping) else {}
        digest = str(ref_payload.get("sha256") or "")
        storage = str(ref_payload.get("storage") or "")
        with self._acquire_conn().cursor() as cur:
            if storage == "okg.source_record_sets":
                cur.execute(
                    """
                    SELECT record_set
                      FROM okg.source_record_sets
                     WHERE source = %s
                    """,
                    (self.source_name,),
                )
                row = cur.fetchone()
            else:
                cur.execute(
                    """
                    SELECT record_set, record_set_text, piece_write_id,
                           piece_count, piece_bytes
                      FROM okg.source_record_set_blobs
                     WHERE source = %s
                       AND sha256 = %s
                    """,
                    (self.source_name, digest),
                )
                row = cur.fetchone()
        if not row:
            return None
        if storage == "okg.source_record_sets":
            return row[0]
        record_set, record_set_text, write_id, piece_count, piece_bytes = row
        if write_id is not None:
            with self._tail_phase(
                "record_set.pieces_load", piece_count=int(piece_count),
                payload_bytes=int(piece_bytes),
            ):
                text = self._read_record_set_pieces(
                    write_id,
                    digest=digest,
                    piece_count=int(piece_count),
                    piece_bytes=int(piece_bytes),
                )
                return json.loads(text)
        if record_set_text:
            return json.loads(str(record_set_text))
        return record_set

    def _read_record_set_pieces(
        self,
        write_id: Any,
        *,
        digest: str,
        piece_count: int,
        piece_bytes: int,
    ) -> bytearray:
        """Read a pieced record set one piece at a time and verify it.

        Fails closed (``RecordSetPiecesError``) on a missing, extra or altered
        piece, a size mismatch or a digest mismatch.
        """
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.source_record_set_blob_pieces
                 WHERE write_id = %s
                """,
                (write_id,),
            )
            stored = int(cur.fetchone()[0])
            if stored != piece_count:
                raise RecordSetPiecesError(
                    "SourceRunner: record-set authority has "
                    f"{stored} stored pieces, header records {piece_count}",
                    data={
                        "source": self.source_name,
                        "sha256": digest,
                        "piece_count": piece_count,
                        "found_piece_count": stored,
                    },
                    fix_hint=(
                        "the stored record set cannot be trusted; re-run the "
                        "source with a full read (reset its watermark)"
                    ),
                )

            def pieces() -> Iterator[tuple[int, str]]:
                for piece_no in range(piece_count):
                    cur.execute(
                        """
                        SELECT piece_no, piece
                          FROM okg.source_record_set_blob_pieces
                         WHERE write_id = %s
                           AND piece_no = %s
                           AND source = %s
                        """,
                        (write_id, piece_no, self.source_name),
                    )
                    found = cur.fetchone()
                    if found is None:
                        # reassemble_pieces reports the gap.
                        return
                    yield int(found[0]), str(found[1])

            return _reassemble_record_set_pieces(
                pieces(),
                source=self.source_name,
                sha256=digest,
                piece_count=piece_count,
                piece_bytes=piece_bytes,
            )

    def _ensure_source_record_sets_table(self) -> None:
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS okg.source_record_sets (
                  source TEXT PRIMARY KEY,
                  record_set JSONB NOT NULL,
                  record_set_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                  chronos_proof JSONB,
                  sha256 TEXT NOT NULL DEFAULT '',
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS okg.source_record_set_blobs (
                  source TEXT NOT NULL,
                  sha256 TEXT NOT NULL,
                  record_set JSONB NOT NULL,
                  record_set_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                  chronos_proof JSONB,
                  record_set_text TEXT,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (source, sha256)
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE okg.source_record_set_blobs
                  ADD COLUMN IF NOT EXISTS record_set_text TEXT
                """
            )
            # Pieced storage (issue #3008). Migration 0035 creates it; this
            # covers a database whose schema predates it.
            cur.execute(
                "SELECT to_regclass('okg.source_record_set_blob_pieces')"
            )
            if cur.fetchone()[0] is None:
                cur.execute(_RECORD_SET_PIECES_DDL)

    @staticmethod
    def _record_set_piece_max_bytes() -> int:
        raw = os.environ.get("OKG_SOURCE_RUNNER_RECORD_SET_PIECE_MAX_BYTES")
        if raw is None:
            return _DEFAULT_RECORD_SET_PIECE_MAX_BYTES
        try:
            value = int(raw)
        except ValueError:
            value = -1
        # Refuse rather than fall back: a piece must stay far below
        # PostgreSQL's 1 GB message and field limits.
        if not 4 <= value <= 256 * 1024 * 1024:
            raise SubstrateError(
                "OKG_SOURCE_RUNNER_RECORD_SET_PIECE_MAX_BYTES must be an "
                "integer between 4 and 268435456",
                data={"value": raw},
                fix_hint="unset it to use the 64 MB default",
            )
        return value

    @staticmethod
    def _record_set_jsonb_max_bytes() -> int:
        raw = os.environ.get("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES")
        if raw is None:
            return _DEFAULT_RECORD_SET_JSONB_MAX_BYTES
        try:
            value = int(raw)
        except ValueError:
            return _DEFAULT_RECORD_SET_JSONB_MAX_BYTES
        return max(0, value)

    def _cursor_with_externalized_fields(
        self,
        cursor: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if not isinstance(cursor, dict):
            return cursor
        threshold = self._inline_cursor_field_max_bytes()
        if threshold <= 0:
            return self._strip_cursor_blob_summary(dict(cursor))
        out = self._strip_cursor_blob_summary(dict(cursor))
        refs: dict[str, dict[str, Any]] = {}
        fields_to_store: list[tuple[str, bytes, str, int]] = []
        # One record for the whole loop: a per-field emit would flush twice per
        # cursor key, including the small keys that are never stored.
        with self._tail_phase("cursor_blob.serialize", field_count=len(out)):
            for key, value in list(out.items()):
                if key == "record_set":
                    continue
                if self._is_cursor_blob_ref(value):
                    refs[key] = dict(value)
                    continue
                encoded = json.dumps(
                    value,
                    sort_keys=True,
                    default=str,
                    separators=(",", ":"),
                ).encode("utf-8")
                size_bytes = len(encoded)
                if size_bytes <= threshold:
                    continue
                digest = hashlib.sha256(encoded).hexdigest()
                fields_to_store.append((key, encoded, digest, size_bytes))
                ref = {
                    "schema": _CURSOR_BLOB_REF_SCHEMA,
                    "storage": _CURSOR_BLOB_STORAGE,
                    "source": self.source_name,
                    "key": key,
                    "sha256": digest,
                    "size_bytes": size_bytes,
                }
                out[key] = ref
                refs[key] = ref
        self._store_external_cursor_fields(fields_to_store)
        if refs:
            out["cursor_blob_summary"] = {
                "schema": "okg.source_runner.cursor_blob_summary.v1",
                "field_count": len(refs),
                "total_size_bytes": sum(
                    int(ref["size_bytes"]) for ref in refs.values()
                ),
                "fields": refs,
            }
        return out

    @staticmethod
    def _strip_cursor_blob_summary(cursor: dict[str, Any]) -> dict[str, Any]:
        cursor.pop("cursor_blob_summary", None)
        return cursor

    @staticmethod
    def _inline_cursor_field_max_bytes() -> int:
        raw = os.environ.get("OKG_SOURCE_RUNNER_INLINE_CURSOR_FIELD_MAX_BYTES")
        if raw is None:
            return _DEFAULT_INLINE_CURSOR_FIELD_MAX_BYTES
        try:
            value = int(raw)
        except ValueError:
            return _DEFAULT_INLINE_CURSOR_FIELD_MAX_BYTES
        return max(0, value)

    @staticmethod
    def _is_cursor_blob_ref(value: Any) -> bool:
        return (
            isinstance(value, Mapping)
            and value.get("schema") == _CURSOR_BLOB_REF_SCHEMA
            and value.get("storage") == _CURSOR_BLOB_STORAGE
        )

    def _store_external_cursor_field(
        self,
        key: str,
        value_json: bytes,
        *,
        digest: str,
        size_bytes: int,
    ) -> None:
        self._store_external_cursor_fields([
            (key, value_json, digest, size_bytes),
        ])

    def _store_external_cursor_fields(
        self,
        fields: Iterable[tuple[str, bytes, str, int]],
    ) -> None:
        rows = list(fields)
        if not rows:
            return
        self._ensure_source_cursor_blobs_table()
        # Metadata is evaluated at the call site, outside the observer guard, so
        # it must not be able to raise into this write path.
        try:
            requested_bytes = sum(int(row[3]) for row in rows)
        except Exception:
            requested_bytes = None
        with self._acquire_conn().cursor() as cur:
            # The probe and the insert are separate statements; a shared phase
            # could not say which one failed.
            with self._tail_phase(
                "cursor_blob.probe", row_count=len(rows),
                payload_bytes=requested_bytes,
            ):
                cur.execute(
                    """
                    WITH requested(cursor_key, sha256) AS (
                      SELECT * FROM unnest(%s::text[], %s::text[])
                    )
                    SELECT blobs.cursor_key, blobs.sha256
                      FROM requested
                      JOIN okg.source_cursor_blobs AS blobs
                        ON blobs.source = %s
                       AND blobs.cursor_key = requested.cursor_key
                       AND blobs.sha256 = requested.sha256
                    """,
                    (
                        [key for key, _value, _digest, _size in rows],
                        [digest for _key, _value, digest, _size in rows],
                        self.source_name,
                    ),
                )
                existing = {
                    (str(key), str(digest))
                    for key, digest in cur.fetchall()
                }
                missing = [
                    row for row in rows
                    if (row[0], row[2]) not in existing
                ]
            if not missing:
                return
            try:
                missing_bytes = sum(int(row[3]) for row in missing)
            except Exception:
                missing_bytes = None
            with self._tail_phase(
                "cursor_blob.insert", row_count=len(missing),
                parameter_count_per_insert=5, payload_bytes=missing_bytes,
                payload_parameter_type="str",
            ):
                cur.executemany(
                    """
                    INSERT INTO okg.source_cursor_blobs
                      (source, cursor_key, sha256, value, size_bytes, updated_at)
                    VALUES (%s, %s, %s, %s::jsonb, %s, now())
                    ON CONFLICT (source, cursor_key, sha256) DO NOTHING
                    """,
                    [
                        (
                            self.source_name,
                            key,
                            digest,
                            value_json.decode("utf-8"),
                            size_bytes,
                        )
                        for key, value_json, digest, size_bytes in missing
                    ],
                )

    def _hydrate_cursor_blobs(
        self,
        cursor: Any,
        *,
        keys: set[str] | None = None,
    ) -> Any:
        if not isinstance(cursor, dict):
            return cursor
        hydrated = dict(cursor)
        refs: list[tuple[str, str, str, bool]] = []
        for key, value in list(hydrated.items()):
            if not self._is_cursor_blob_ref(value):
                continue
            refs.append(
                (
                    key,
                    str(value.get("key") or key),
                    str(value.get("sha256") or ""),
                    keys is None or key in keys,
                )
            )
        if not refs:
            return hydrated
        self._ensure_source_cursor_blobs_table()
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                WITH requested(cursor_key, sha256, hydrate) AS (
                  SELECT * FROM unnest(%s::text[], %s::text[], %s::boolean[])
                )
                SELECT requested.cursor_key, requested.sha256,
                       blobs.sha256 IS NOT NULL,
                       CASE WHEN requested.hydrate THEN blobs.value END
                  FROM requested
                  LEFT JOIN okg.source_cursor_blobs AS blobs
                    ON blobs.source = %s
                   AND blobs.cursor_key = requested.cursor_key
                   AND blobs.sha256 = requested.sha256
                """,
                (
                    [ref_key for _key, ref_key, _digest, _hydrate in refs],
                    [digest for _key, _ref_key, digest, _hydrate in refs],
                    [hydrate for _key, _ref_key, _digest, hydrate in refs],
                    self.source_name,
                ),
            )
            rows = {
                (str(ref_key), str(digest)): (bool(present), value)
                for ref_key, digest, present, value in cur.fetchall()
            }
        for key, ref_key, digest, hydrate in refs:
            lookup = (ref_key, digest)
            present, value = rows.get(lookup, (False, None))
            if not present:
                raise SubstrateError(
                    "SourceRunner: cursor blob ref is missing",
                    data={
                        "source_name": self.source_name,
                        "cursor_key": ref_key,
                        "sha256": digest,
                    },
                )
            if hydrate:
                hydrated[key] = value
        if keys is None:
            hydrated.pop("cursor_blob_summary", None)
        return hydrated

    def _load_external_cursor_field(self, key: str, ref: Mapping[str, Any]) -> Any:
        self._ensure_source_cursor_blobs_table()
        digest = str(ref.get("sha256") or "")
        ref_key = str(ref.get("key") or key)
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                SELECT value
                  FROM okg.source_cursor_blobs
                 WHERE source = %s
                   AND cursor_key = %s
                   AND sha256 = %s
                """,
                (self.source_name, ref_key, digest),
            )
            row = cur.fetchone()
        if row is None:
            raise SubstrateError(
                "SourceRunner: cursor blob ref is missing",
                data={
                    "source_name": self.source_name,
                    "cursor_key": ref_key,
                    "sha256": digest,
                },
            )
        return row[0]

    def _ensure_source_cursor_blobs_table(self) -> None:
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS okg.source_cursor_blobs (
                  source TEXT NOT NULL,
                  cursor_key TEXT NOT NULL,
                  sha256 TEXT NOT NULL,
                  value JSONB NOT NULL,
                  size_bytes BIGINT NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (source, cursor_key, sha256)
                )
                """
            )

    def _assert_record_set_covers_emitted_fact(
        self,
        fact: NodeFact | EdgeFact,
        *,
        covered_nodes: set[str],
        covered_node_ownership_modes: Mapping[str, str],
        covered_edges: set[tuple[str, str, str]],
    ) -> None:
        if isinstance(fact, NodeFact):
            node_id = self._canonical_node_id(fact.node_id)
            if (
                node_id in covered_nodes
                and covered_node_ownership_modes.get(node_id)
                == fact.ownership_mode
            ):
                return
            raise SubstrateError(
                "SourceRunner: record_set omitted a node or declared a "
                "different ownership mode; refusing verified delta fast path",
                data={
                    "source_name": self.source_name,
                    "node_id": node_id,
                    "subtype": fact.subtype,
                    "fact_ownership_mode": fact.ownership_mode,
                    "record_set_ownership_mode": (
                        covered_node_ownership_modes.get(node_id)
                    ),
                },
                fix_hint=(
                    "fix the source adapter to include every source-owned "
                    "emitted node in its record_set, or reset the source "
                    "cursor to force a full record-set repair"
                ),
            )

        identity = (
            self._canonical_node_id(fact.src),
            fact.edge_type,
            self._canonical_node_id(fact.dst),
        )
        if identity in covered_edges:
            return
        raise SubstrateError(
            "SourceRunner: record_set omitted an edge emitted by this "
            "run; refusing verified delta fast path",
            data={
                "source_name": self.source_name,
                "src": identity[0],
                "edge_type": identity[1],
                "dst": identity[2],
            },
            fix_hint=(
                "fix the source adapter to include every source-owned "
                "emitted edge in its record_set, or reset the source "
                "cursor to force a full record-set repair"
            ),
        )

    def _record_set_identities(
        self,
        record_set: RecordSet,
    ) -> tuple[set[str], set[tuple[str, str, str]]]:
        nodes: set[str] = set()
        edges: set[tuple[str, str, str]] = set()
        for emission in record_set.records.values():
            nodes.update(
                self._canonical_node_id(node_id)
                for node_id in emission.nodes_by_id()
            )
            edges.update(
                (
                    self._canonical_node_id(edge.src),
                    edge.edge_type,
                    self._canonical_node_id(edge.dst),
                )
                for edge in emission.edges_by_identity().values()
            )
        return nodes, edges

    def _record_set_node_ownership_modes(
        self,
        record_set: RecordSet,
    ) -> dict[str, str]:
        """Return one unambiguous ownership contract for each node identity."""
        modes: dict[str, str] = {}
        for record_id, emission in sorted(record_set.records.items()):
            emission_nodes = [
                *([emission.primary] if emission.primary is not None else []),
                *emission.descendants,
            ]
            for node in emission_nodes:
                canonical_id = self._canonical_node_id(node.node_id)
                prior = modes.get(canonical_id)
                if prior is not None and prior != node.ownership_mode:
                    raise SubstrateError(
                        "SourceRunner: record_set declares conflicting node "
                        "ownership modes",
                        data={
                            "source_name": self.source_name,
                            "record_id": record_id,
                            "node_id": canonical_id,
                            "first_ownership_mode": prior,
                            "conflicting_ownership_mode": node.ownership_mode,
                        },
                        fix_hint=(
                            "declare each node identity as either authoritative "
                            "or substitute consistently across the record set"
                        ),
                    )
                modes[canonical_id] = node.ownership_mode
        return modes

    def _verify_record_set_chronos_delta(
        self,
        prior: RecordSet,
        current: RecordSet,
    ) -> None:
        """Verify only changed record-set identities after prior proof.

        A full coverage sweep is required when a source first adopts a
        record-set cursor. Once that cursor is marked as Chronos-verified,
        unchanged records already have a persisted proof. Later runs only
        need to verify newly declared identities became visible and
        retired source-owned identities are no longer visible.
        """
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return

        prior_node_modes = self._record_set_node_ownership_modes(prior)
        current_node_modes = self._record_set_node_ownership_modes(current)
        prior_nodes, prior_edges = self._record_set_identities(prior)
        current_nodes, current_edges = self._record_set_identities(current)
        added_nodes = sorted(
            node_id
            for node_id, mode in current_node_modes.items()
            if prior_node_modes.get(node_id) != mode
        )
        added_edges = sorted(current_edges - prior_edges)
        retired_nodes = sorted(prior_nodes - current_nodes)
        retired_edges = sorted(prior_edges - current_edges)
        if (
            not added_nodes
            and not added_edges
            and not retired_nodes
            and not retired_edges
        ):
            return

        from okg.substrate.graph_store import _edge_id

        session = self._checkout_graph_branch(graph_store)
        chunk_size = 1000
        present_added_nodes: set[str] = set()
        added_authoritative_nodes = [
            node_id
            for node_id in added_nodes
            if current_node_modes[node_id] == "authoritative"
        ]
        added_substitute_nodes = [
            node_id
            for node_id in added_nodes
            if current_node_modes[node_id] == "substitute"
        ]
        for idx in range(0, len(added_authoritative_nodes), chunk_size):
            node_chunk = added_authoritative_nodes[idx : idx + chunk_size]
            rows = session.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE source = :source
                   AND node_id = ANY(:node_ids)
                """,
                {"source": self.source_name, "node_ids": node_chunk},
            )
            present_added_nodes.update(str(row["node_id"]) for row in rows)
        for idx in range(0, len(added_substitute_nodes), chunk_size):
            node_chunk = added_substitute_nodes[idx : idx + chunk_size]
            rows = session.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": node_chunk},
            )
            present_added_nodes.update(str(row["node_id"]) for row in rows)

        added_edge_ids = [
            _edge_id(src, edge_type, dst)
            for src, edge_type, dst in added_edges
        ]
        present_added_edges: set[tuple[str, str, str]] = set()
        for idx in range(0, len(added_edge_ids), chunk_size):
            edge_id_chunk = added_edge_ids[idx : idx + chunk_size]
            rows = session.query(
                f"""
                SELECT src, edge_type, dst
                  FROM {_coverage_edges(session)} AS present
                 WHERE edge_id = ANY(:edge_ids)
                """,
                {"edge_ids": edge_id_chunk},
            )
            present_added_edges.update(
                (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
                for row in rows
            )

        visible_retired_nodes: set[str] = set()
        for idx in range(0, len(retired_nodes), chunk_size):
            node_chunk = retired_nodes[idx : idx + chunk_size]
            rows = session.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE source = :source
                   AND node_id = ANY(:node_ids)
                """,
                {"source": self.source_name, "node_ids": node_chunk},
            )
            visible_retired_nodes.update(str(row["node_id"]) for row in rows)

        retired_edge_ids = [
            _edge_id(src, edge_type, dst)
            for src, edge_type, dst in retired_edges
        ]
        visible_retired_edges: set[tuple[str, str, str]] = set()
        for idx in range(0, len(retired_edge_ids), chunk_size):
            edge_id_chunk = retired_edge_ids[idx : idx + chunk_size]
            rows = session.query(
                """
                SELECT src, edge_type, dst
                  FROM okg.graph_edges
                 WHERE source = :source
                   AND edge_id = ANY(:edge_ids)
                """,
                {"source": self.source_name, "edge_ids": edge_id_chunk},
            )
            visible_retired_edges.update(
                (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
                for row in rows
            )

        missing_nodes = sorted(set(added_nodes) - present_added_nodes)
        missing_edges = sorted(set(added_edges) - present_added_edges)
        stale_nodes = sorted(visible_retired_nodes)
        stale_edges = sorted(visible_retired_edges)
        if not missing_nodes and not missing_edges and not stale_nodes and not stale_edges:
            return

        diagnosis = record_set_coverage_diagnosis(
            missing_node_count=len(missing_nodes),
            missing_edge_count=len(missing_edges),
            stale_node_count=len(stale_nodes),
            stale_edge_count=len(stale_edges),
        )
        owned_elsewhere, owned_elsewhere_capped = self._missing_nodes_owned_elsewhere(
            session, missing_nodes
        )
        raise SubstrateError(
            "SourceRunner: record_set delta coverage check failed; refusing "
            "to advance source watermark "
            + record_set_coverage_marker(diagnosis),
            data={
                "source_name": self.source_name,
                **{
                    key: value
                    for key, value in diagnosis.items()
                    if key != "fix_hint"
                },
                "missing_nodes_owned_by_other_source": owned_elsewhere,
                "missing_nodes_sample": missing_nodes[:10],
                "missing_edges_sample": [
                    {"src": src, "edge_type": edge_type, "dst": dst}
                    for src, edge_type, dst in missing_edges[:10]
                ],
                "stale_nodes_sample": stale_nodes[:10],
                "stale_edges_sample": [
                    {"src": src, "edge_type": edge_type, "dst": dst}
                    for src, edge_type, dst in stale_edges[:10]
                ],
            },
            fix_hint=_coverage_fix_hint(
                diagnosis["fix_hint"],
                owned_elsewhere,
                owned_elsewhere_capped=owned_elsewhere_capped,
                only_cause=(
                    len(owned_elsewhere) == len(missing_nodes)
                    and not missing_edges
                    and not stale_nodes
                    and not stale_edges
                ),
            ),
        )

    def _record_set_coverage_counts_agree(
        self,
        session: Any,
        *,
        node_ownership_modes: dict[str, str],
        expected_edge_list: list[tuple[str, str, str]],
    ) -> bool:
        """Decide record-set coverage without hydrating any graph row.

        Returns True only when the declared footprint and the graph agree on
        every side the hydrating proof checks:

        * every declared authoritative identity is present AND owned by this
          source, and every declared substitute identity is present under
          whatever source owns it (the same asymmetry the hydrating proof
          applies);
        * no source-owned identity sits outside the declaration.

        Returns False whenever the two sides disagree, or whenever this
        method cannot answer for the declaration it was given (an ownership
        mode outside the declared vocabulary). The caller then runs the
        unchanged hydrating proof, so a refusal keeps its exact counts,
        samples and fix hint. Every path out of here is therefore either
        "proved agreement" or "let the original proof decide" -- never
        "assume agreement".

        Two counting rules make this equivalent to the set comparison it
        replaces, WITHOUT assuming the graph holds exactly one live row per
        identity. That property is a Chronos write-path invariant, not a
        constraint on the rows this query reads, and a duplicate live row
        must not let a present identity pay for a missing one:

        * the presence counts are `count(DISTINCT expected.<key>)` over the
          STAGED side, whose keys are unique by construction, so a fanned-out
          join cannot inflate them;
        * the containment counts are plain `count(*)` on both sides of the
          same comparison, so a duplicated row is counted identically in the
          joined total and in the source-owned total and cancels out, while
          any source-owned identity outside the declaration still breaks the
          equality.

        Edges match on the full `(src, edge_type, dst)` tuple as well as on
        `edge_id`, because `edge_id` is a truncated uuid5 and the hydrating
        proof it replaces compared tuples. A hash collision therefore fails
        to match and falls through, rather than reading as coverage.
        """
        from okg.substrate.graph_store import _edge_id
        from okg.substrate.library.sources.base import NODE_OWNERSHIP_MODES

        if not set(node_ownership_modes.values()) <= NODE_OWNERSHIP_MODES:
            # An unrecognized ownership mode has no counting rule here. Let
            # the hydrating proof answer rather than guess.
            return False

        raw_connection = session._context._db.raw_connection
        expected_authoritative_count = sum(
            mode == "authoritative" for mode in node_ownership_modes.values()
        )
        expected_substitute_count = (
            len(node_ownership_modes) - expected_authoritative_count
        )
        # Stable names, not per-call unique ones: Chronos caches its SQL
        # rewrite keyed on the query text, and a fresh table name per call
        # would fill that cache with entries that can never be reused.
        nodes_table = "_okg_record_set_expected_nodes"
        edges_table = "_okg_record_set_expected_edges"

        def _counts(sql: str, params: dict[str, Any]) -> dict[str, int]:
            rows = session.query(sql, params)
            if len(rows) != 1:
                raise SubstrateError(
                    "SourceRunner: record_set coverage count returned an "
                    "invalid result",
                    data={
                        "source_name": self.source_name,
                        "row_count": len(rows),
                    },
                )
            try:
                return {str(key): int(value) for key, value in rows[0].items()}
            except (TypeError, ValueError) as exc:
                raise SubstrateError(
                    "SourceRunner: record_set coverage count returned an "
                    "invalid value",
                    data={"source_name": self.source_name},
                ) from exc

        # One transaction: the staged identity tables are ON COMMIT DROP and
        # must be visible to the counting queries, which ride the same
        # connection. `session.transaction()` also drains the Chronos
        # backend's deferred post-commit work (interval GC, deferred index
        # creation) at this point in the run rather than later.
        with session.transaction():
            try:
                with raw_connection.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {nodes_table}")
                    cur.execute(
                        f"CREATE TEMP TABLE {nodes_table} ("
                        "node_id text NOT NULL, ownership_mode text NOT NULL"
                        ") ON COMMIT DROP"
                    )
                    with cur.copy(
                        f"COPY {nodes_table} (node_id, ownership_mode) "
                        "FROM STDIN"
                    ) as copy:
                        for node_id, mode in node_ownership_modes.items():
                            copy.write_row((node_id, mode))
                    cur.execute(f"ANALYZE {nodes_table}")
                    cur.execute(f"DROP TABLE IF EXISTS {edges_table}")
                    cur.execute(
                        f"CREATE TEMP TABLE {edges_table} ("
                        "edge_id bigint NOT NULL, src text NOT NULL, "
                        "edge_type text NOT NULL, dst text NOT NULL"
                        ") ON COMMIT DROP"
                    )
                    if expected_edge_list:
                        with cur.copy(
                            f"COPY {edges_table} "
                            "(edge_id, src, edge_type, dst) FROM STDIN"
                        ) as copy:
                            for src, edge_type, dst in expected_edge_list:
                                copy.write_row((
                                    _edge_id(src, edge_type, dst),
                                    src,
                                    edge_type,
                                    dst,
                                ))
                    cur.execute(f"ANALYZE {edges_table}")
            except psycopg.Error as exc:
                raise SubstrateError(
                    "SourceRunner: record_set coverage check could not stage "
                    "its declared identities",
                    data={
                        "source_name": self.source_name,
                        "declared_nodes": len(node_ownership_modes),
                        "declared_edges": len(expected_edge_list),
                    },
                    fix_hint=(
                        "the coverage proof stages the run's declared node "
                        "and edge identities in transaction-local temporary "
                        "tables; grant the ingest role TEMPORARY on this "
                        "database, or investigate the reported error"
                    ),
                ) from exc

            node_membership = _counts(
                f"""
                SELECT
                  count(DISTINCT expected.node_id) FILTER (
                    WHERE expected.ownership_mode = :authoritative_mode
                      AND graph.source = :source
                  ) AS authoritative_present,
                  count(DISTINCT expected.node_id) FILTER (
                    WHERE expected.ownership_mode = :substitute_mode
                  ) AS substitute_present,
                  count(*) FILTER (
                    WHERE graph.source = :source
                  ) AS source_owned_declared
                  FROM okg.graph_nodes AS graph
                  JOIN {nodes_table} AS expected
                    ON expected.node_id = graph.node_id
                """,
                {
                    "source": self.source_name,
                    "authoritative_mode": "authoritative",
                    "substitute_mode": "substitute",
                },
            )
            source_owned_nodes = _counts(
                """
                SELECT count(*) AS n
                  FROM okg.graph_nodes
                 WHERE source = :source
                """,
                {"source": self.source_name},
            )["n"]
            from okg.substrate.graph_store_internal.edge_deferrals import (
                edges_relation_sql,
            )

            edges_relation = edges_relation_sql(session)
            edge_membership = _counts(
                f"""
                SELECT
                  count(DISTINCT expected.edge_id) AS declared_present,
                  count(*) FILTER (
                    WHERE graph.source = :source
                  ) AS source_owned_declared
                  FROM {edges_relation} AS graph
                  JOIN {edges_table} AS expected
                    ON expected.edge_id = graph.edge_id
                   AND expected.src = graph.src
                   AND expected.edge_type = graph.edge_type
                   AND expected.dst = graph.dst
                """,
                {"source": self.source_name},
            )
            source_owned_edges = _counts(
                f"""
                SELECT count(*) AS n
                  FROM {edges_relation} AS owned
                 WHERE source = :source
                """,
                {"source": self.source_name},
            )["n"]

        return (
            node_membership["authoritative_present"]
            == expected_authoritative_count
            and node_membership["substitute_present"]
            == expected_substitute_count
            and node_membership["source_owned_declared"] == source_owned_nodes
            and edge_membership["declared_present"] == len(expected_edge_list)
            and edge_membership["source_owned_declared"] == source_owned_edges
        )

    def _verify_record_set_chronos_coverage(
        self,
        record_set: RecordSet,
    ) -> None:
        """Fail closed before a record-set cursor advances past the graph.

        Record-set sources may skip unchanged records only when the prior
        cursor and the graph already agree. This check proves that the newly
        declared full-scope footprint is present in Chronos and that no
        source-owned stale rows remain outside that footprint.
        """
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return

        node_ownership_modes = self._record_set_node_ownership_modes(record_set)
        expected_nodes = set(node_ownership_modes)
        expected_edges: set[tuple[str, str, str]] = set()
        for emission in record_set.records.values():
            expected_edges.update(
                (
                    self._canonical_node_id(edge.src),
                    edge.edge_type,
                    self._canonical_node_id(edge.dst),
                )
                for edge in emission.edges_by_identity().values()
            )

        from okg.substrate.graph_store import _edge_id

        session = self._checkout_graph_branch(graph_store)

        expected_node_list = sorted(expected_nodes)
        expected_edge_list = sorted(expected_edges)
        expected_edge_ids = [
            _edge_id(src, edge_type, dst)
            for src, edge_type, dst in expected_edge_list
        ]

        # Bounded-memory fast path. The hydrating proof below reads every
        # source-owned graph row: `okg.graph_nodes` is a Chronos view, so
        # even a `SELECT node_id` projection is rewritten through the full
        # physical row shape and transfers the whole `attrs` payload. On the
        # 1.8M-record CMS reference catalog that retained >13 GiB in the
        # client for a run whose answer was "everything matches".
        #
        # Set equality on each side is decided by four counts: how many
        # declared identities the graph holds, and how many source-owned
        # identities the declaration holds. Compare them server-side
        # against COPY-staged identity tables (a multi-million element
        # `ANY(array)` parameter would just move the same bytes into
        # Postgres) and return without hydrating a single row when they
        # agree. A disagreement falls through to the hydrating proof
        # unchanged, so every refusal keeps its exact diagnostics; see
        # `_record_set_coverage_counts_agree` for why counting is
        # equivalent to comparing the sets.
        if self._record_set_coverage_counts_agree(
            session,
            node_ownership_modes=node_ownership_modes,
            expected_edge_list=expected_edge_list,
        ):
            return

        present_nodes: set[str] = set()
        present_edges: set[tuple[str, str, str]] = set()
        chunk_size = 1000
        for idx in range(0, len(expected_edge_ids), chunk_size):
            edge_id_chunk = expected_edge_ids[idx : idx + chunk_size]
            rows = session.query(
                f"""
                SELECT src, edge_type, dst
                  FROM {_coverage_edges(session)} AS present
                 WHERE edge_id = ANY(:edge_ids)
                """,
                {"edge_ids": edge_id_chunk},
            )
            present_edges.update(
                (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
                for row in rows
            )
        authoritative_nodes = sorted(
            node_id
            for node_id, mode in node_ownership_modes.items()
            if mode == "authoritative"
        )
        substitute_nodes = sorted(
            node_id
            for node_id, mode in node_ownership_modes.items()
            if mode == "substitute"
        )
        for idx in range(0, len(authoritative_nodes), chunk_size):
            node_chunk = authoritative_nodes[idx : idx + chunk_size]
            rows = session.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE source = :source
                   AND node_id = ANY(:node_ids)
                """,
                {"source": self.source_name, "node_ids": node_chunk},
            )
            present_nodes.update(str(row["node_id"]) for row in rows)
        for idx in range(0, len(substitute_nodes), chunk_size):
            node_chunk = substitute_nodes[idx : idx + chunk_size]
            rows = session.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": node_chunk},
            )
            present_nodes.update(str(row["node_id"]) for row in rows)

        missing_nodes = sorted(expected_nodes - present_nodes)
        missing_edges = sorted(expected_edges - present_edges)
        stale_nodes = self._record_set_stale_graph_nodes(
            session,
            expected_node_list,
        )
        stale_edges = self._record_set_stale_graph_edges(
            session,
            expected_edge_ids,
        )
        if (
            not missing_nodes
            and not missing_edges
            and not stale_nodes
            and not stale_edges
        ):
            return

        diagnosis = record_set_coverage_diagnosis(
            missing_node_count=len(missing_nodes),
            missing_edge_count=len(missing_edges),
            stale_node_count=len(stale_nodes),
            stale_edge_count=len(stale_edges),
            # `_record_set_stale_graph_*` read a LIMIT-10 sample, so a
            # full sample is a lower bound, not a count.
            stale_counts_exact=(
                len(stale_nodes) < RECORD_SET_STALE_SAMPLE_LIMIT
                and len(stale_edges) < RECORD_SET_STALE_SAMPLE_LIMIT
            ),
        )
        owned_elsewhere, owned_elsewhere_capped = self._missing_nodes_owned_elsewhere(
            session, missing_nodes
        )
        raise SubstrateError(
            "SourceRunner: record_set coverage check failed; refusing "
            "to advance source watermark "
            + record_set_coverage_marker(diagnosis),
            data={
                "source_name": self.source_name,
                **{
                    key: value
                    for key, value in diagnosis.items()
                    if key != "fix_hint"
                },
                "missing_nodes_owned_by_other_source": owned_elsewhere,
                "missing_nodes_sample": missing_nodes[:10],
                "missing_edges_sample": [
                    {"src": src, "edge_type": edge_type, "dst": dst}
                    for src, edge_type, dst in missing_edges[:10]
                ],
                "stale_nodes_sample": stale_nodes[:10],
                "stale_edges_sample": stale_edges[:10],
            },
            fix_hint=_coverage_fix_hint(
                diagnosis["fix_hint"],
                owned_elsewhere,
                owned_elsewhere_capped=owned_elsewhere_capped,
                only_cause=(
                    len(owned_elsewhere) == len(missing_nodes)
                    and not missing_edges
                    and not stale_nodes
                    and not stale_edges
                ),
            ),
        )

    def _missing_nodes_owned_elsewhere(
        self,
        session: Any,
        missing_nodes: list[str],
    ) -> tuple[list[dict[str, Any]], bool]:
        """Up to 10 missing nodes the graph holds under ANOTHER source.

        An authoritative node counts as present only under this source, so a
        row another source holds reads as missing (#3591). Returns the sample
        and whether more exist. Diagnostic only: a failed lookup is logged
        and yields no sample, never replacing the coverage refusal itself.
        """
        found: list[dict[str, Any]] = []
        limit = OWNED_ELSEWHERE_SAMPLE_LIMIT + 1
        try:
            for idx in range(0, len(missing_nodes), 1000):
                rows = session.query(
                    """
                    SELECT node_id, source
                      FROM okg.graph_nodes
                     WHERE node_id = ANY(:node_ids)
                       AND source IS DISTINCT FROM :source
                     ORDER BY node_id
                     LIMIT :limit
                    """,
                    {
                        "source": self.source_name,
                        "node_ids": missing_nodes[idx : idx + 1000],
                        "limit": limit - len(found),
                    },
                )
                found.extend(
                    {
                        "node_id": str(row["node_id"]),
                        "source": (
                            str(row["source"]) if row["source"] is not None else None
                        ),
                    }
                    for row in rows
                )
                if len(found) >= limit:
                    break
        except Exception as exc:  # noqa: BLE001 - diagnostic must not mask the refusal
            log.warning(
                "record_set coverage: could not look up other owners of missing "
                "nodes for %s: %s",
                self.source_name,
                exc,
            )
            return [], False
        capped = len(found) > OWNED_ELSEWHERE_SAMPLE_LIMIT
        return found[:OWNED_ELSEWHERE_SAMPLE_LIMIT], capped

    def _record_set_stale_graph_nodes(
        self,
        session: Any,
        expected_node_ids: list[str],
    ) -> list[str]:
        if expected_node_ids:
            rows = session.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE source = :source
                   AND NOT (node_id = ANY(:node_ids))
                 ORDER BY node_id
                 LIMIT 10
                """,
                {"source": self.source_name, "node_ids": expected_node_ids},
            )
        else:
            rows = session.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE source = :source
                 ORDER BY node_id
                 LIMIT 10
                """,
                {"source": self.source_name},
            )
        return [str(row["node_id"]) for row in rows]

    def _record_set_stale_graph_edges(
        self,
        session: Any,
        expected_edge_ids: list[int],
    ) -> list[dict[str, str]]:
        # A sample of ten, ordered by edge_id (edge_source_edgeid_visible)
        # since okg#3112: no index orders a source's edges by
        # (src, edge_type, dst) any more.
        if expected_edge_ids:
            rows = session.query(
                f"""
                SELECT src, edge_type, dst
                  FROM {_coverage_edges(session)} AS owned
                 WHERE source = :source
                   AND NOT (edge_id = ANY(:edge_ids))
                 ORDER BY edge_id
                 LIMIT 10
                """,
                {"source": self.source_name, "edge_ids": expected_edge_ids},
            )
        else:
            rows = session.query(
                f"""
                SELECT src, edge_type, dst
                  FROM {_coverage_edges(session)} AS owned
                 WHERE source = :source
                 ORDER BY edge_id
                 LIMIT 10
                """,
                {"source": self.source_name},
            )
        return [
            {
                "src": str(row["src"]),
                "edge_type": str(row["edge_type"]),
                "dst": str(row["dst"]),
            }
            for row in rows
        ]

    def _record_set_retractions(
        self,
        prior: RecordSet,
        current: RecordSet,
    ) -> list[NodeFact | EdgeFact]:
        """Diff source-owned logical records and synthesize retractions.

        Sources that opt into record-set reconciliation declare
        their current full-scope footprint. For disappeared records,
        the runner retracts the prior primary node, descendants, and
        explicit source-owned edges. For kept records, it retracts
        prior nodes/edges missing from the current footprint.
        """
        if not prior.records:
            return []

        out: list[NodeFact | EdgeFact] = []
        current_nodes: set[str] = set()
        current_edges: set[tuple[str, str, str]] = set()
        for emission in current.records.values():
            current_nodes.update(emission.nodes_by_id())
            current_edges.update(emission.edges_by_identity())

        for record_key in sorted(prior.records):
            prior_emission = prior.records[record_key]
            for node_id, node in sorted(
                prior_emission.nodes_by_id().items(),
            ):
                if node_id not in current_nodes:
                    out.append(self._node_retraction_fact(record_key, node))
            for identity, edge in sorted(
                prior_emission.edges_by_identity().items(),
            ):
                if identity not in current_edges:
                    out.append(self._edge_retraction_fact(record_key, edge))

        return out

    def _full_retraction_live_rows(
        self,
        prior_record_set: RecordSet,
    ) -> LiveRowCount:
        """Live rows for the full-retraction guard (issue #2818 backstop).

        The graph is the authority; a runner with no graph store counts the
        prior record-set footprint instead and says so in the refusal.
        """
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return live_rows_from_record_set(prior_record_set)
        return count_live_source_rows(
            self._checkout_graph_branch(graph_store),
            self.source_name,
        )

    def _full_retraction_sub_scope_prior(
        self,
        prior_record_set: RecordSet,
        *,
        persisted_cursor: Optional[dict[str, Any]],
    ) -> RecordSet:
        """Per-record prior footprint for the sub-scope guard.

        A `--reset-cursor` run or an unverified prior hands the diff an empty
        prior and retracts through the stray pass instead. The persisted
        record set, verified or not, still says what each record held.
        """
        if prior_record_set.records:
            return prior_record_set
        try:
            return self._prior_record_set(persisted_cursor)
        except SubstrateError:
            log.warning(
                "source %s: persisted record set is unreadable, so the "
                "sub-scope full-retraction guard has no prior to compare",
                self.source_name,
                exc_info=True,
            )
            return RecordSet()

    def _record_set_stray_retractions(
        self,
        current: RecordSet,
    ) -> list[NodeFact | EdgeFact]:
        """Retract latest source facts outside the current record set.

        Cursor diffs can only retract identities remembered in the prior
        cursor. This pass repairs legacy sources that started declaring a
        record set after stale facts already existed in the fact log, and
        catches future sources that accidentally omit an owned identity
        from their declared footprint.
        """
        current_nodes: set[str] = set()
        current_edges: set[tuple[str, str, str]] = set()
        for emission in current.records.values():
            current_nodes.update(
                self._canonical_node_id(node_id)
                for node_id in emission.nodes_by_id()
            )
            current_edges.update(
                (
                    self._canonical_node_id(edge.src),
                    edge.edge_type,
                    self._canonical_node_id(edge.dst),
                )
                for edge in emission.edges_by_identity().values()
            )

        out: list[NodeFact | EdgeFact] = []
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return out

        session = self._checkout_graph_branch(graph_store)
        for row in session.query(
            """
            SELECT node_id, subtype, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": self.source_name},
        ):
            node_id = str(row["node_id"])
            if node_id in current_nodes:
                continue
            out.append(NodeFact(
                node_id=node_id,
                subtype=str(row["subtype"]),
                attrs={},
                source_record_id=self._json_dict(row["source_record_id"]) or {
                    "node_id": node_id,
                    "retraction": "record_set_stray",
                },
                source_revision={
                    **self._json_dict(row["source_revision"]),
                    "record_set_stray_retraction": True,
                },
                op="R",
            ))

        # Ordered by edge_id, which edge_source_edgeid_visible serves: no
        # index orders a source's edges by (src, edge_type, dst) since
        # okg#3112. The strays (usually none) are sorted by identity below,
        # so the retraction order stays deterministic and identity-ordered.
        stray_edges: list[tuple[tuple[str, str, str], EdgeFact]] = []
        stray_columns = (
            "edge_id, src, dst, edge_type, attrs, provenance, confidence, "
            "source, source_record_id, source_revision"
        )
        for row in session.query(
            f"""
            SELECT src, dst, edge_type, attrs, provenance, confidence,
                   source_record_id, source_revision
              FROM {_coverage_edges(session, stray_columns)} AS owned
             WHERE source = :source
             ORDER BY edge_id
            """,
            {"source": self.source_name},
        ):
            identity = (
                str(row["src"]),
                str(row["edge_type"]),
                str(row["dst"]),
            )
            if identity in current_edges:
                continue
            stray_edges.append((identity, EdgeFact(
                src=identity[0],
                dst=identity[2],
                edge_type=identity[1],
                attrs=self._json_dict(row["attrs"]),
                provenance=str(row["provenance"]),
                confidence=float(row["confidence"]),
                source_record_id=self._json_dict(row["source_record_id"]) or {
                    "src": identity[0],
                    "edge_type": identity[1],
                    "dst": identity[2],
                    "retraction": "record_set_stray",
                },
                source_revision={
                    **self._json_dict(row["source_revision"]),
                    "record_set_stray_retraction": True,
                },
                op="R",
            )))
        stray_edges.sort(key=lambda item: item[0])
        out.extend(fact for _identity, fact in stray_edges)

        return out

    def _canonical_node_id(self, node_id: str) -> str:
        snapshot = self._active_pipeline_snapshot
        chain = (
            snapshot.canonicalizers
            if snapshot is not None else tuple(self.canonicalizers)
        )
        if not chain:
            return node_id
        from okg.substrate.canonicalize import canonicalize_id

        return canonicalize_id(node_id, chain)

    @staticmethod
    def _json_dict(raw: Any) -> dict[str, Any]:
        return dict(raw) if isinstance(raw, dict) else {}

    def _node_retraction_fact(
        self,
        record_key: str,
        node: RecordNode,
    ) -> NodeFact:
        return NodeFact(
            node_id=node.node_id,
            subtype=node.subtype,
            attrs={},
            source_record_id=node.source_record_id or {
                "record_key": record_key,
                "node_id": node.node_id,
                "retraction": "record_set",
            },
            source_revision={
                **node.source_revision,
                "record_set_retraction": True,
            },
            op="R",
            ownership_mode=node.ownership_mode,
        )

    def _edge_retraction_fact(
        self,
        record_key: str,
        edge: EdgeKey,
    ) -> EdgeFact:
        return EdgeFact(
            src=edge.src,
            dst=edge.dst,
            edge_type=edge.edge_type,
            attrs=edge.attrs,
            provenance=edge.provenance,
            confidence=edge.confidence,
            source_record_id=edge.source_record_id or {
                "record_key": record_key,
                "src": edge.src,
                "edge_type": edge.edge_type,
                "dst": edge.dst,
                "retraction": "record_set",
            },
            source_revision={
                **edge.source_revision,
                "record_set_retraction": True,
            },
            op="R",
        )

    def _ordinary_dedupe_reference(self) -> str:
        """The dedupe reference of a run that is not an authoritative replay.

        The live branch whenever Chronos writing is on, as for replay: it is
        what readers see, and the append-only fact log can lag it after a
        rollover or recovery (issue #2336). The fact log only when it is the
        one store written.
        """
        if self.write_chronos_graph:
            return DEDUPE_REFERENCE_LIVE_BRANCH
        if self.write_fact_log:
            return DEDUPE_REFERENCE_FACT_LOG
        return DEDUPE_REFERENCE_NONE

    def _dedupe_against_persisted(
        self,
        facts: list,
    ) -> tuple[list, int, int]:
        """Drop op='I' facts whose `(source, node_id, subtype, attrs)`
        already exists in `okg.node_facts` at the latest
        observed_at for that node. Retractions and
        genuinely-changed facts pass through.

        The hot case: a `discovery_crawl` source re-emits
        every NodeFact every cycle. Cycle 2 over unchanged
        files used to 2× the fact table; this pass collapses
        cycle-2's redundant emits to no-ops.

        Per `audit-doc-corpus-chunk-turnover` (2026-04-26).
        """
        # Only filter op='I'; retractions always pass through.
        candidates_nodes = [
            f for f in facts
            if isinstance(f, NodeFact) and f.op == "I"
        ]
        candidates_edges = [
            f for f in facts
            if isinstance(f, EdgeFact) and f.op == "I"
        ]
        reference = self._dedupe_reference
        if reference == DEDUPE_REFERENCE_NONE:
            return facts, 0, 0
        if not candidates_nodes and not candidates_edges:
            return facts, 0, 0

        if reference == DEDUPE_REFERENCE_LIVE_BRANCH:
            return self._dedupe_against_chronos_graph(
                facts,
                candidates_nodes=candidates_nodes,
                candidates_edges=candidates_edges,
            )
        if reference != DEDUPE_REFERENCE_FACT_LOG:
            raise SubstrateError(
                "unknown persisted dedupe reference",
                data={"source_name": self.source_name, "reference": reference},
            )

        # Legacy fact-log reference: only a run with Chronos writing off
        # reaches it (see `_ordinary_dedupe_reference`). It decides with the
        # same rule as the live-branch dedupe (`okg.substrate.fact_equivalence`,
        # issue #2336): every stored attr counts, `body_text` and `text`
        # included; a revision-only lineage change is unchanged; only the node
        # subtype's own volatile fields are ignored; edges also compare
        # `provenance` and `confidence`.
        #
        # Postgres' planner with `node_id = ANY($huge_array)` falls off the
        # index path around ~10k entries and into a seq scan over a
        # ~100M-row hypertable; chunked at 1000 IDs each the sweep stays on
        # node_facts_node_id_observed_idx.
        node_ids = list({f.node_id for f in candidates_nodes})
        edge_keys = list({
            (f.src, f.edge_type, f.dst) for f in candidates_edges
        })
        volatile = self._load_volatile_fields()
        detail = self._persisted_dedupe_detail
        _DEDUP_CHUNK = 1000
        latest_node_state: dict[str, tuple[str, str, dict[str, Any]]] = {}
        latest_edge_state: dict[
            tuple[str, str, str], tuple[str, dict[str, Any], str, Any]
        ] = {}
        if node_ids or edge_keys:
            with self._acquire_conn().cursor() as cur:
                for i in range(0, len(node_ids), _DEDUP_CHUNK):
                    chunk = node_ids[i : i + _DEDUP_CHUNK]
                    cur.execute(
                        """
                        SELECT DISTINCT ON (node_id) node_id,
                               op, subtype, attrs
                          FROM okg.node_facts
                         WHERE source = %s
                           AND node_id = ANY(%s)
                         ORDER BY node_id,
                                  observed_at DESC, fact_id DESC
                        """,
                        (self.source_name, chunk),
                    )
                    for nid, op, subtype, attrs in cur.fetchall():
                        latest_node_state[nid] = (
                            op, str(subtype), dict(attrs or {}),
                        )
                for i in range(0, len(edge_keys), _DEDUP_CHUNK):
                    chunk = edge_keys[i : i + _DEDUP_CHUNK]
                    cur.execute(
                        """
                        SELECT DISTINCT ON (src, edge_type, dst)
                               src, edge_type, dst, op, attrs,
                               provenance, confidence
                          FROM okg.edge_facts
                         WHERE source = %s
                           AND (src, edge_type, dst) IN (
                               SELECT * FROM unnest(
                                   %s::text[], %s::text[], %s::text[]
                               )
                           )
                         ORDER BY src, edge_type, dst,
                                  observed_at DESC, fact_id DESC
                        """,
                        (
                            self.source_name,
                            [k[0] for k in chunk],
                            [k[1] for k in chunk],
                            [k[2] for k in chunk],
                        ),
                    )
                    for src, edge_type, dst, op, attrs, provenance, confidence in cur.fetchall():
                        latest_edge_state[(src, edge_type, dst)] = (
                            op, dict(attrs or {}), str(provenance), confidence,
                        )

        def _record_kept(differing: Sequence[str]) -> None:
            for key in differing:
                detail.kept_by_key[key] = detail.kept_by_key.get(key, 0) + 1

        kept: list = []
        n_dropped_node = n_dropped_edge = 0
        for f in facts:
            if isinstance(f, NodeFact) and f.op == "I":
                prior = latest_node_state.get(f.node_id)
                if prior is not None and prior[0] == "I":
                    if prior[1] != f.subtype:
                        _record_kept(["subtype"])
                    else:
                        comparison = compare_attrs(
                            f.attrs, prior[2],
                            ignore=volatile_fields_for(volatile, f.subtype),
                        )
                        if comparison.unchanged:
                            n_dropped_node += 1
                            if comparison.revision_only:
                                detail.revision_only_nodes += 1
                            continue
                        _record_kept(comparison.differing)
            elif isinstance(f, EdgeFact) and f.op == "I":
                prior_edge = latest_edge_state.get((f.src, f.edge_type, f.dst))
                if prior_edge is not None and prior_edge[0] == "I":
                    _op, prior_attrs, prior_provenance, prior_confidence = prior_edge
                    column_differing = edge_columns_differing(
                        new_provenance=str(f.provenance),
                        new_confidence=f.confidence,
                        stored_provenance=prior_provenance,
                        stored_confidence=prior_confidence,
                    )
                    comparison = compare_attrs(f.attrs, prior_attrs)
                    if comparison.unchanged and not column_differing:
                        n_dropped_edge += 1
                        if comparison.revision_only:
                            detail.revision_only_edges += 1
                        continue
                    _record_kept(sorted(comparison.differing + column_differing))
            kept.append(f)
        return kept, n_dropped_node, n_dropped_edge

    def _dedupe_against_chronos_graph(
        self,
        facts: list,
        *,
        candidates_nodes: list[NodeFact],
        candidates_edges: list[EdgeFact],
    ) -> tuple[list, int, int]:
        graph_store = self._chronos_graph_store()
        if graph_store is None:
            return facts, 0, 0

        # Rule (issues #2076, #1754, #1731): a candidate whose content matches
        # the current live row on the branch is not written, on every run
        # including authoritative replay, whatever revision kind the source
        # declares. A row that is missing on the branch, or differs, is still
        # written, so replay keeps repairing the branch.
        #
        # "Content" is every attr the writer would store, with two
        # exclusions: the subtype-declared volatile keys (for example
        # ``Document.parsed_at``) and the source revision inside the lineage
        # envelope (record ids and the policy fingerprint still count).
        # ``body_text`` is compared. ``text`` is compared after
        # the candidate goes through the writer's own searchable-text rule,
        # because the stored row's ``text`` went through it on its write.
        # Edges also compare their ``provenance`` and ``confidence`` columns.
        # The rule lives in `okg.substrate.fact_equivalence`, shared with the
        # fact-log dedupe and the graph writer's no-op check; volatile fields
        # are the node subtype's own (issue #2334).
        volatile = self._load_volatile_fields()
        detail = self._persisted_dedupe_detail
        stamp_text: Any = None
        if candidates_nodes:
            stamp_text = getattr(
                graph_store, "searchable_text_for_comparison", None,
            )
            if not callable(stamp_text):
                raise SubstrateError(
                    "graph store cannot stamp searchable text for the "
                    "persisted dedupe",
                    data={
                        "source_name": self.source_name,
                        "graph_store": type(graph_store).__name__,
                    },
                    fix_hint=(
                        "use a graph store that exposes "
                        "searchable_text_for_comparison (ChronosGraphStore)"
                    ),
                )
        text_cache: dict[str, list[str]] = {}

        def _record_kept(differing: Sequence[str]) -> None:
            for key in differing:
                detail.kept_by_key[key] = detail.kept_by_key.get(key, 0) + 1

        node_ids = sorted({f.node_id for f in candidates_nodes})
        edge_keys = sorted({(f.src, f.edge_type, f.dst) for f in candidates_edges})
        session = self._checkout_graph_branch(graph_store)
        latest_node_state: dict[str, tuple[str, dict[str, Any]]] = {}
        latest_edge_state: dict[
            tuple[str, str, str], tuple[dict[str, Any], str, Any]
        ] = {}

        if node_ids:
            rows = session.query(
                """
                SELECT node_id, subtype, attrs
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                   AND source = :source
                """,
                {"node_ids": node_ids, "source": self.source_name},
            )
            latest_node_state = {
                row["node_id"]: (str(row["subtype"]), dict(row["attrs"]))
                for row in rows
            }
        # A substitute node this source already owns is unchanged content,
        # but the writer records the source's claim on a shared node
        # (okg.graph_node_declarers, issue #1901) only when the fact reaches
        # it. A row stored before the source declared it substitute has no
        # claim (issue #3382: doc-corpus chunks stored authoritative), and
        # dropping the fact here would leave the owner unclaimed, so a later
        # hand-off away from it could never come back. Keep such a fact; the
        # writer stores no new row version for it, only the claim.
        unclaimed_substitutes = self._unclaimed_owned_substitutes(
            session,
            graph_store,
            sorted({
                f.node_id
                for f in candidates_nodes
                if f.ownership_mode == "substitute"
                and f.node_id in latest_node_state
            }),
        )
        if edge_keys:
            from okg.substrate.graph_store import _edge_id

            edge_ids = [
                _edge_id(src, edge_type, dst)
                for src, edge_type, dst in edge_keys
            ]
            rows = session.query(
                """
                SELECT edge_id, src, edge_type, dst, attrs,
                       provenance, confidence
                  FROM okg.graph_edges
                 WHERE edge_id = ANY(:edge_ids)
                   AND source = :source
                """,
                {"edge_ids": edge_ids, "source": self.source_name},
            )
            wanted = set(edge_keys)
            latest_edge_state = {
                (row["src"], row["edge_type"], row["dst"]): (
                    dict(row["attrs"]),
                    str(row["provenance"]),
                    row["confidence"],
                )
                for row in rows
                if (row["src"], row["edge_type"], row["dst"]) in wanted
            }

        kept: list = []
        n_dropped_node = n_dropped_edge = 0
        for f in facts:
            if isinstance(f, NodeFact) and f.op == "I":
                prior = latest_node_state.get(f.node_id)
                if f.node_id in unclaimed_substitutes:
                    _record_kept(["ownership_claim"])
                elif prior is not None:
                    prior_subtype, prior_attrs = prior
                    if prior_subtype != f.subtype:
                        _record_kept(["subtype"])
                    else:
                        new_attrs = stamp_text(
                            f.subtype, f.attrs, cache=text_cache,
                        )
                        comparison = compare_attrs(
                            new_attrs, prior_attrs,
                            ignore=volatile_fields_for(volatile, f.subtype),
                        )
                        if comparison.unchanged:
                            n_dropped_node += 1
                            if comparison.revision_only:
                                detail.revision_only_nodes += 1
                            continue
                        _record_kept(comparison.differing)
            elif isinstance(f, EdgeFact) and f.op == "I":
                prior_edge = latest_edge_state.get((f.src, f.edge_type, f.dst))
                if prior_edge is not None:
                    prior_attrs, prior_provenance, prior_confidence = prior_edge
                    column_differing = edge_columns_differing(
                        new_provenance=str(f.provenance),
                        new_confidence=f.confidence,
                        stored_provenance=prior_provenance,
                        stored_confidence=prior_confidence,
                    )
                    comparison = compare_attrs(f.attrs, prior_attrs)
                    if comparison.unchanged and not column_differing:
                        n_dropped_edge += 1
                        if comparison.revision_only:
                            detail.revision_only_edges += 1
                        continue
                    _record_kept(sorted(comparison.differing + column_differing))
            kept.append(f)
        return kept, n_dropped_node, n_dropped_edge

    def _unclaimed_owned_substitutes(
        self,
        session: Any,
        graph_store: Any,
        node_ids: list[str],
    ) -> set[str]:
        """Substitute nodes this source owns but has recorded no claim on.

        Read on the run's branch. With no registered declarer table every
        id is unclaimed: the graph writer registers it on its first write
        that tracks claims, then records them.
        """
        if not node_ids:
            return set()
        from okg.substrate.graph_store_internal.node_declarers import (
            NODE_DECLARERS_TABLE,
        )

        backend = graph_store.context._backend
        if NODE_DECLARERS_TABLE not in backend.tables:
            backend.refresh_registries()
        if NODE_DECLARERS_TABLE not in backend.tables:
            return set(node_ids)
        claimed: set[str] = set()
        chunk = 10_000
        for start in range(0, len(node_ids), chunk):
            rows = session.query(
                f"""
                SELECT node_id
                  FROM {NODE_DECLARERS_TABLE}
                 WHERE source = :source
                   AND node_id = ANY(:node_ids)
                """,
                {
                    "source": self.source_name,
                    "node_ids": node_ids[start:start + chunk],
                },
            )
            claimed.update(str(row["node_id"]) for row in rows)
        return set(node_ids) - claimed

    @staticmethod
    def _dedupe_facts(
        facts: list,
    ) -> tuple[list, int, int, Optional[str], dict[str, int]]:
        """In-memory dedup pass: collapse facts to their structural
        identity (`node_id` for NodeFact;
        `(src, dst, edge_type)` for EdgeFact), keeping
        last-write-wins.

        Returns `(deduped, n_node_drops, n_edge_drops,
        first_dropped_id, drops_by_prefix)`. The first-dropped
        id is the identity (string `node_id`, or
        `"<src>|<edge_type>|<dst>"`) of the FIRST duplicate
        encountered. `drops_by_prefix` (per
        `refine-doc-corpus-extraction` W7) breaks down the
        drops by NodeFact node-id prefix or
        `edge:<edge_type>` so operators see whether the drop
        is healthy chunk dedup or two adapters colliding.

        Order preservation: deduped output preserves the order
        of LAST emit per identity (so consumers that care about
        ordering observe the latest write at the position
        where the latest write was emitted).
        """
        nodes_by_id: dict[str, NodeFact] = {}
        edges_by_triple: dict[tuple[str, str, str], EdgeFact] = {}
        n_node_drops = 0
        n_edge_drops = 0
        first_dropped: Optional[str] = None
        drops_by_prefix: dict[str, int] = {}

        for f in facts:
            if isinstance(f, NodeFact):
                if f.node_id in nodes_by_id:
                    n_node_drops += 1
                    prefix = f.node_id.split(":", 1)[0] or "_"
                    drops_by_prefix[prefix] = (
                        drops_by_prefix.get(prefix, 0) + 1
                    )
                    if first_dropped is None:
                        first_dropped = f.node_id
                nodes_by_id[f.node_id] = f
            elif isinstance(f, EdgeFact):
                key = (f.src, f.dst, f.edge_type)
                if key in edges_by_triple:
                    n_edge_drops += 1
                    prefix = f"edge:{f.edge_type}"
                    drops_by_prefix[prefix] = (
                        drops_by_prefix.get(prefix, 0) + 1
                    )
                    if first_dropped is None:
                        first_dropped = (
                            f"{f.src}|{f.edge_type}|{f.dst}"
                        )
                edges_by_triple[key] = f
            else:
                # Pass-through unknown fact types untouched —
                # the downstream writer raises a structured
                # error.
                pass

        # Build the output list in node-then-edge order;
        # within each, preserve insertion order from the dict
        # (Python 3.7+ guarantees this).
        deduped = list(nodes_by_id.values()) + list(
            edges_by_triple.values()
        )
        return (
            deduped, n_node_drops, n_edge_drops,
            first_dropped, drops_by_prefix,
        )

    def _refuse_empty_scope_assertion_fact(
        self,
        fact: NodeFact | EdgeFact,
    ) -> None:
        """Refuse a run that sets `SourceRun.asserts_empty_scope` and emits.

        The assertion says the reader walked its whole scope and found it
        empty; the runner then retracts the prior footprint itself, so the
        stream has nothing to carry. Any fact, retraction included, is
        refused on the first one, inside the flush and before its chunk
        persists. Retractions are refused too because letting them through
        is not recoverable: a positive fact in a later chunk would refuse
        the run after the retraction chunks were saved, and compensation
        only withdraws rows a run inserted. It cannot restore rows the run
        retracted, so those records stayed deleted while the carried
        authority still listed them.
        """
        # Progress-marker-only chunks may precede the first fact; mark the
        # run for the same compensation as a refused retraction so `run()`
        # puts the source's progress back.
        self._run_refused_retraction = True
        raise SubstrateError(
            "source run asserts an empty scope but emitted facts",
            data={
                "source_name": self.source_name,
                "fact_kind": (
                    "node" if isinstance(fact, NodeFact) else "edge"
                ),
                "fact_op": str(getattr(fact, "op", "I")).upper(),
                "fact_identity": (
                    fact.node_id
                    if isinstance(fact, NodeFact)
                    else f"{fact.src}|{fact.edge_type}|{fact.dst}"
                ),
                "reason_code": "empty_scope_assertion_stream_not_empty",
            },
            fix_hint=(
                "set asserts_empty_scope only on a completed run that emits "
                "no facts; the runner retracts the prior footprint itself"
            ),
        )

    def _record_set_without_identities(
        self,
        record_set: RecordSet,
        *,
        node_ids: set[str],
        edge_identities: set[tuple[str, str, str]],
    ) -> RecordSet:
        """Return `record_set` minus the named canonical identities.

        Used for a completed stream that carried ONLY explicit retractions:
        the prior authority is kept except for exactly what the stream
        retracted. A record left with no node and no edge is dropped.
        """
        records: dict[str, RecordEmission] = {}
        for key, emission in record_set.records.items():
            primary = emission.primary
            if (
                primary is not None
                and self._canonical_node_id(primary.node_id) in node_ids
            ):
                primary = None
            descendants = [
                node for node in emission.descendants
                if self._canonical_node_id(node.node_id) not in node_ids
            ]
            edges = [
                edge for edge in emission.edges
                if (
                    self._canonical_node_id(edge.src),
                    edge.edge_type,
                    self._canonical_node_id(edge.dst),
                ) not in edge_identities
            ]
            if primary is None and not descendants and not edges:
                continue
            records[key] = RecordEmission(
                primary=primary,
                descendants=descendants,
                edges=edges,
            )
        return RecordSet(records=records)

    def _connect_image_blob_store(self) -> psycopg.Connection:
        """A separate autocommit connection for the image step's blob rows.

        Blob rows are content-addressed and idempotent, so they commit on
        their own instead of inside the chunk's fact transaction; a row from
        a run that later rolls back is reclaimed by blob cleanup.
        """
        from okg.substrate.db.connection import connect

        return connect(
            self.dsn,
            autocommit=True,
            application_name="okg-image-render",
            deployment=self.deployment_name,
        )

    def _check_incoming_image_values(self, facts: list) -> None:
        """Refuse the run when an adapter fact's image value is forged or
        invalid (PACT first-class-images, req no-forgery).

        Only the ingest image step may add ``png`` or ``error``; a connector
        emits the reference alone. Retractions are skipped: they write no
        value, and a runner-synthesized edge retraction carries the stored
        attrs back. The error keeps its code (`image_value_forged` or
        `image_ref_invalid`) and gains the source and the fact's identity.
        """
        from okg.substrate.images.values import ImageRefInvalid, ImageValueForged

        for fact in facts:
            if not isinstance(fact, (NodeFact, EdgeFact)) or fact.op == "R":
                continue
            try:
                check_incoming(fact.attrs)
            except (ImageValueForged, ImageRefInvalid) as exc:
                kind = "node" if isinstance(fact, NodeFact) else "edge"
                identity = (
                    fact.node_id if isinstance(fact, NodeFact)
                    else f"{fact.src}|{fact.edge_type}|{fact.dst}"
                )
                raise type(exc)(
                    f"source {self.source_name!r} {kind} {identity!r}: "
                    f"{exc.message}",
                    data={
                        **exc.data,
                        "source_name": self.source_name,
                        "fact_kind": kind,
                        "fact_identity": identity,
                    },
                    fix_hint=exc.fix_hint,
                ) from None

    def _refuse_preserved_authority_fact(
        self,
        fact: NodeFact | EdgeFact,
    ) -> None:
        """Refuse a run that claims a preserved footprint and still emits.

        `SourceRun.preserves_prior_record_set` means one thing only: this run
        emitted no facts because the PRIOR record-set authority is still
        exact. The runner acts on that by skipping footprint inference, so a
        reader that sets the flag and then streams a partial slice would have
        its facts written against a footprint nothing rechecked -- exactly
        the silent divergence issue #2122 produced from the other side. The
        comment at the inference site used to assert emptiness; this enforces
        it, on the first identity fact, before the chunk persists.
        """
        raise SubstrateError(
            "source run claims preserved record-set authority but emitted "
            "identity facts",
            data={
                "source_name": self.source_name,
                "fact_kind": (
                    "node" if isinstance(fact, NodeFact) else "edge"
                ),
                "fact_identity": (
                    fact.node_id
                    if isinstance(fact, NodeFact)
                    else f"{fact.src}|{fact.edge_type}|{fact.dst}"
                ),
                "reason_code": (
                    "preserved_record_set_authority_stream_not_empty"
                ),
            },
            fix_hint=(
                "set preserves_prior_record_set only on a run that emits no "
                "facts; a run with changes must declare its own record set "
                "or stream the complete footprint"
            ),
        )

    def _retain_run_seen_identities(self, adapter: SourceAdapter) -> bool:
        """Return whether the runner must retain cross-chunk dedupe keys.

        The default remains exact runner-side dedupe.  The sole exception is
        bound to the reviewed registry/source/implementation tuple whose IDs
        include disjoint record roles and the grid physical line number.
        """
        policy = getattr(
            adapter,
            "run_wide_identity_policy",
            _RUN_WIDE_IDENTITY_POLICY_DEFAULT,
        )
        if type(policy) is not str:
            raise SubstrateError(
                "source adapter run-wide identity policy must be a string",
                data={
                    "source_name": self.source_name,
                    "policy_type": type(policy).__name__,
                },
                fix_hint=(
                    "use the exact runner_dedup or "
                    "adapter_guaranteed_unique string"
                ),
            )
        if policy not in _RUN_WIDE_IDENTITY_POLICIES:
            raise SubstrateError(
                "source adapter declares an unknown run-wide identity policy",
                data={"source_name": self.source_name, "policy": policy},
                fix_hint=(
                    "use runner_dedup or register an audited "
                    "adapter_guaranteed_unique binding"
                ),
            )
        if policy == _RUN_WIDE_IDENTITY_POLICY_DEFAULT:
            return True
        binding = (
            self.ownership_id or "",
            self.registry_name or "",
            type(adapter).__module__,
            type(adapter).__qualname__,
            self.executable_profile or "",
        )
        expected_emit_version = _ADAPTER_GUARANTEED_UNIQUE_BINDINGS.get(binding)
        if expected_emit_version is None:
            raise SubstrateError(
                "adapter-guaranteed run-wide identity policy is not registered",
                data={
                    "source_name": self.source_name,
                    "registry_name": self.registry_name,
                    "adapter_module": type(adapter).__module__,
                    "adapter_class": type(adapter).__qualname__,
                    "executable_profile": self.executable_profile,
                },
                fix_hint="retain runner_dedup until the exact binding is audited",
            )

        registry_binding_fingerprint = self.registry_binding_fingerprint
        if not (
            isinstance(registry_binding_fingerprint, str)
            and bool(registry_binding_fingerprint)
            and registry_binding_fingerprint == registry_binding_fingerprint.strip()
        ):
            raise SubstrateError(
                "adapter-guaranteed run-wide identity policy requires a "
                "canonical nonempty registry binding fingerprint",
                data={
                    "source_name": self.source_name,
                    "has_binding_fingerprint": bool(registry_binding_fingerprint),
                    "binding_fingerprint_is_string": isinstance(
                        registry_binding_fingerprint,
                        str,
                    ),
                },
                fix_hint="retain runner_dedup until registry authority is canonical",
            )

        try:
            change_probe = adapter.change_probe
        except Exception as exc:  # noqa: BLE001 - authority failure is fail-closed
            raise SubstrateError(
                "adapter-guaranteed run-wide identity policy requires the "
                "public change-probe emit-version authority",
                data={
                    "source_name": self.source_name,
                    "access_error": type(exc).__name__,
                },
                fix_hint=(
                    "expose the audited ContentHashProbe.emit_version property "
                    "or retain runner_dedup"
                ),
            ) from exc
        if type(change_probe) is not ContentHashProbe:
            raise SubstrateError(
                "adapter-guaranteed run-wide identity policy requires an exact "
                "ContentHashProbe authority",
                data={
                    "source_name": self.source_name,
                    "change_probe_type": type(change_probe).__name__,
                },
                fix_hint="retain runner_dedup until probe authority is audited",
            )
        try:
            emit_version = change_probe.emit_version
        except Exception as exc:  # noqa: BLE001 - authority failure is fail-closed
            raise SubstrateError(
                "adapter-guaranteed run-wide identity policy requires the "
                "public change-probe emit-version authority",
                data={
                    "source_name": self.source_name,
                    "access_error": type(exc).__name__,
                },
                fix_hint=(
                    "expose the audited ContentHashProbe.emit_version property "
                    "or retain runner_dedup"
                ),
            ) from exc

        if not (
            isinstance(emit_version, str)
            and len(emit_version) == 64
            and all(char in "0123456789abcdef" for char in emit_version)
        ):
            raise SubstrateError(
                "adapter-guaranteed run-wide identity policy received a "
                "malformed emit-version authority",
                data={
                    "source_name": self.source_name,
                    "emit_version_is_string": isinstance(emit_version, str),
                },
                fix_hint="retain runner_dedup until emit authority is valid",
            )
        if emit_version != expected_emit_version:
            raise SubstrateError(
                "adapter-guaranteed run-wide identity policy emit version is "
                "not the audited fingerprint",
                data={"source_name": self.source_name},
                fix_hint="retain runner_dedup until the changed adapter is audited",
            )
        return False

    @staticmethod
    def _node_run_key(f: NodeFact) -> tuple[str, str, str]:
        revision = json.dumps(
            f.source_revision, sort_keys=True, default=str,
        )
        return (f.node_id, revision, f.op)

    @staticmethod
    def _edge_run_key(f: EdgeFact) -> tuple[str, str, str, str, str]:
        revision = json.dumps(
            f.source_revision, sort_keys=True, default=str,
        )
        return (f.src, f.edge_type, f.dst, revision, f.op)

    @staticmethod
    def _dedupe_against_run_seen(
        facts: list,
        *,
        seen_node_keys: set[tuple[str, str, str]],
        seen_edge_keys: set[tuple[str, str, str, str, str]],
    ) -> tuple[list, int, int, Optional[str], dict[str, int]]:
        """Drop duplicates already flushed earlier in this run.

        `_dedupe_facts()` handles duplicates inside the current
        buffer. This pass covers duplicates that land after a
        prior buffer committed, where COPY would otherwise raise
        because all flushes in one run share `observed_at`.
        """
        kept: list = []
        n_node_drops = n_edge_drops = 0
        first_dropped: Optional[str] = None
        drops_by_prefix: dict[str, int] = {}

        for f in facts:
            if isinstance(f, NodeFact):
                key = SourceRunner._node_run_key(f)
                if key in seen_node_keys:
                    n_node_drops += 1
                    prefix = f.node_id.split(":", 1)[0] or "_"
                    drops_by_prefix[prefix] = (
                        drops_by_prefix.get(prefix, 0) + 1
                    )
                    if first_dropped is None:
                        first_dropped = f.node_id
                    continue
                seen_node_keys.add(key)
            elif isinstance(f, EdgeFact):
                key = SourceRunner._edge_run_key(f)
                if key in seen_edge_keys:
                    n_edge_drops += 1
                    prefix = f"edge:{f.edge_type}"
                    drops_by_prefix[prefix] = (
                        drops_by_prefix.get(prefix, 0) + 1
                    )
                    if first_dropped is None:
                        first_dropped = (
                            f"{f.src}|{f.edge_type}|{f.dst}"
                        )
                    continue
                seen_edge_keys.add(key)
            kept.append(f)

        return (
            kept, n_node_drops, n_edge_drops,
            first_dropped, drops_by_prefix,
        )

    def _load_prior_cursor(self) -> Optional[dict[str, Any]]:
        """Read the source's persisted `last_applied_rev` JSONB.

        Returns the dict (parsed JSONB) or None when the source has
        no prior watermark row. The runner passes this to the
        adapter's `run(cursor=...)` so the adapter can implement
        cursor-based short-circuits (skip-on-unchanged, diff-and-
        retract, etc.) without each adapter re-implementing the
        watermark read.

        Per `add-doc-corpus-chunk-reconciliation` (2026-04-26).
        """
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                "SELECT last_applied_rev FROM okg.source_watermarks "
                " WHERE source = %s",
                (self.source_name,),
            )
            row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        # psycopg returns JSONB as native Python (dict/list).
        return self._hydrate_cursor_blobs(row[0])

    def _load_cursor_progress_boundary(
        self,
        *,
        adapter: SourceAdapter,
        reset_cursor: bool,
        sync_scope: Optional[dict[str, Any]],
    ) -> RunPrelude:
        include_progress = _adapter_accepts_run_kwarg(
            adapter,
            "since_progress",
        )
        cursor_blob_keys: set[str] | None = None
        scoped_cursor_fields = getattr(
            adapter,
            "cursor_blob_keys_for_scope",
            None,
        )
        if callable(scoped_cursor_fields):
            requested = scoped_cursor_fields(sync_scope)
            if requested is not None:
                cursor_blob_keys = {str(key) for key in requested}
        (
            prior_cursor,
            prior_progress,
            no_prior_facts,
            replay_required,
            persisted_cursor,
        ) = (
            self._load_run_prelude(
                include_progress=include_progress,
                cursor_blob_keys=cursor_blob_keys,
            )
        )
        if reset_cursor:
            prior_cursor = None
            prior_progress = None
        return RunPrelude(
            prior_cursor=prior_cursor,
            prior_progress=prior_progress,
            no_prior_facts=no_prior_facts,
            replay_required=replay_required,
            persisted_cursor=persisted_cursor,
        )

    def _load_run_prelude(
        self,
        *,
        include_progress: bool = True,
        cursor_blob_keys: set[str] | None = None,
    ) -> tuple[
        Optional[dict[str, Any]],
        Optional[dict[str, str]],
        bool,
        Optional[dict[str, Any]],
        Optional[dict[str, Any]],
    ]:
        """Coalesced single-roundtrip read of the three pieces of
        per-source state that `_run_impl` needs at start:

          1. `last_applied_rev` from `okg.source_watermarks`
             (=> what `_load_prior_cursor` used to return)
          2. `okg.source_progress` rows for this (deployment, source)
             (=> what `_load_prior_progress` used to return)
          3. whether the source has no prior facts at all
             (=> what `_source_has_no_prior_facts` used to return)

        Each of the three used to be a separate SELECT, costing
        ~3 client roundtrips per source per run. On parallel ingest
        with 6 sources, that's 18 roundtrips on the critical path.
        Coalescing them into one CTE-driven SELECT cuts startup
        bookkeeping to a single roundtrip per source.

        The progress semantics are preserved:
          - `deployment_name is None` → returns None (callers see
            "no deployment configured, treat like first run")
          - deployment set but no rows → returns {} (callers see
            "first run for this deployment")
          - deployment set with rows → returns the {key: value, ...}
            dict.
        """
        progress_expr = (
            """
                  CASE
                    WHEN %(dep)s::text IS NULL THEN NULL
                    WHEN NOT %(include_progress)s::boolean THEN NULL
                    ELSE COALESCE(
                      (
                        SELECT jsonb_object_agg(key, value)
                          FROM okg.source_progress
                         WHERE deployment = %(dep)s
                           AND source_name = %(src)s
                           AND key NOT LIKE 'sync.%%'
                      ),
                      '{}'::jsonb
                    )
                  END
            """
        )
        no_prior_expr = (
            """
                  (
                    NOT EXISTS (
                      SELECT 1 FROM okg.node_facts
                       WHERE source = %(src)s LIMIT 1
                    )
                    AND NOT EXISTS (
                      SELECT 1 FROM okg.edge_facts
                       WHERE source = %(src)s LIMIT 1
                    )
                  )
            """
            if self.write_fact_log
            else "false"
        )
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  (
                    SELECT last_applied_rev
                      FROM okg.source_watermarks
                     WHERE source = %(src)s
                  ) AS cursor,
                  {progress_expr} AS progress,
                  {no_prior_expr} AS no_prior_facts,
                  (
                    SELECT replay_required
                      FROM okg.source_watermarks
                     WHERE source = %(src)s
                  ) AS replay_required
                """,
                {
                    "src": self.source_name,
                    "dep": self.deployment_name,
                    "include_progress": include_progress,
                },
            )
            row = cur.fetchone()
        persisted_cursor = (
            dict(row[0])
            if row and isinstance(row[0], Mapping) else None
        )
        cursor_val = self._hydrate_cursor_blobs(
            persisted_cursor,
            keys=cursor_blob_keys,
        )
        progress_val = row[1] if row else None
        no_prior = bool(row[2]) if row else False
        replay_value = row[3] if row and len(row) > 3 else None
        replay_required = (
            dict(replay_value)
            if isinstance(replay_value, Mapping) and replay_value
            else None
        )
        return (
            cursor_val,
            progress_val,
            no_prior,
            replay_required,
            persisted_cursor,
        )

    def _source_has_no_prior_facts(self) -> bool:
        """Return true when this source has no persisted facts yet.

        The vs-persisted no-op filter prevents full refreshes from
        appending unchanged facts. On a clean first import it can only
        find rows written earlier in the same run, so it adds repeated
        index probes without dropping useful facts.
        """
        if not self.write_fact_log:
            return False
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                SELECT NOT EXISTS (
                    SELECT 1 FROM okg.node_facts
                     WHERE source = %s LIMIT 1
                )
                AND NOT EXISTS (
                    SELECT 1 FROM okg.edge_facts
                     WHERE source = %s LIMIT 1
                )
                """,
                (self.source_name, self.source_name),
            )
            row = cur.fetchone()
        return bool(row and row[0])

    def _load_prior_progress(self) -> Optional[dict[str, str]]:
        """Read every `okg.source_progress` row for
        (deployment_name, source_name) into a dict.

        Returns None when `deployment_name` is not set (the
        runner is being driven by a caller that doesn't know
        its deployment) so the adapter sees the same
        no-progress shape as a first run. Returns {} when the
        deployment is set but the source has no progress rows
        yet — the adapter should treat that as "first run, no
        prior fingerprints."

        Per `add-substrate-incremental-publish` (2026-04-28).
        """
        if self.deployment_name is None:
            return None
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                "SELECT key, value FROM okg.source_progress "
                " WHERE deployment = %s AND source_name = %s",
                (self.deployment_name, self.source_name),
            )
            return {k: v for k, v in cur.fetchall()}

    def _upsert_progress(self, progress: dict[str, str]) -> None:
        """Upsert the adapter's `progress_after_run()` dict into
        `okg.source_progress` for (deployment_name, source_name).

        Per-row UPSERT semantics: keys present in `progress`
        replace prior values; keys absent from `progress` are
        LEFT IN PLACE (the adapter is the authority on what
        keys it owns; missing-from-the-return means "I didn't
        observe this key this run", not "delete it"). If an
        adapter wants to retract a key it must omit it AND the
        operator must `DELETE FROM okg.source_progress …` —
        the same operator-escape the proposal documents.

        Per `add-substrate-incremental-publish` (2026-04-28).
        """
        if self.deployment_name is None or not progress:
            return
        rows = [
            (self.deployment_name, self.source_name, k, v)
            for k, v in progress.items()
        ]
        conn = self._acquire_conn()
        with conn.transaction(), conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (deployment, source_name, key)
                  DO UPDATE SET
                    value      = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                rows,
            )

    def _resolve_retraction_authority(
        self,
        adapter: SourceAdapter,
        mode: str,
        *,
        source_run_mode: str | None = None,
        completed_scope: bool | None = None,
        sync_scope: Mapping[str, Any] | None = None,
        health_acceptable: bool = True,
        bounded_record_set_authority: bool = False,
    ) -> SubstrateError | None:
        """Resolve this run's retraction verdict; return the refusal, if any.

        The verdict is a pure function of the registered profile, the run
        mode, and the run's completed-scope/health evidence — all known
        before the first fact is consumed. `add-substrate-incremental-publish`
        Decision 12 requires it to be resolved ONCE, at run start, because the
        streaming loop starts committing chunks long before a late `op='R'`
        fact could raise it (issue #1431). The refusal is RETURNED rather than
        raised: a run that never emits a retraction must still succeed under a
        profile/mode pair that forbids one.
        """
        try:
            self._assert_retraction_permitted(
                adapter,
                mode,
                source_run_mode=source_run_mode,
                completed_scope=completed_scope,
                sync_scope=sync_scope,
                health_acceptable=health_acceptable,
                bounded_record_set_authority=bounded_record_set_authority,
            )
        except SubstrateError as refusal:
            return refusal
        return None

    def _assert_retraction_permitted(
        self,
        adapter: SourceAdapter,
        mode: str,
        *,
        source_run_mode: str | None = None,
        completed_scope: bool | None = None,
        sync_scope: Mapping[str, Any] | None = None,
        health_acceptable: bool = True,
        bounded_record_set_authority: bool = False,
    ) -> None:
        profile = self._policy_profile(adapter)
        policy_mode = _POLICY_MODE.get((profile, mode))
        complete_discovery_scope = (
            completed_scope is True
            and source_run_mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES
            and not source_sync_scope_narrows(sync_scope)
            and health_acceptable
        )
        if profile == "discovery_crawl" and mode == "reconcile":
            # Reconcile is the operator-facing request; discovery crawls use
            # the profile-native SCOPE_COMPLETE policy only after the adapter
            # proves the same completed, unscoped, healthy authority.
            if complete_discovery_scope:
                policy_mode = PolicyRunMode.SCOPE_COMPLETE
        elif (
            profile == "discovery_crawl"
            and policy_mode == PolicyRunMode.SCOPE_COMPLETE
            and not complete_discovery_scope
        ):
            policy_mode = PolicyRunMode.SCOPE_INCOMPLETE
        if policy_mode is None:
            raise SubstrateError(
                f"SourceRunner: adapter "
                f"{(self.registry_name or self.source_name)!r} with "
                f"profile={profile!r} has no policy mapping for "
                f"mode={mode!r}; cannot evaluate retraction safety",
                data={
                    "source_name": self.source_name,
                    "profile": profile,
                    "mode": mode,
                },
                fix_hint="pick a mode supported by the profile; "
                         "see okg.substrate.policy.retraction",
            )
        assert_retraction_permitted(
            source_class=profile,
            run_mode=policy_mode,
            source_name=self.source_name,
            bounded_record_set_authority=bounded_record_set_authority,
        )

    def _flush_chunk(
        self,
        facts: list,
        markers: list[ProgressMarker],
        *,
        run_id: str,
        observed_at: datetime,
        seen_node_keys: Optional[set[tuple[str, str, str]]] = None,
        seen_edge_keys: Optional[
            set[tuple[str, str, str, str, str]]
        ] = None,
        timing_sink: Optional[dict[str, float]] = None,
        timing_recorder: SpanTimingRecorder | None = None,
        sanitize_warn_budget: Optional[_SanitizeWarnBudget] = None,
        normalized_fact_observer: (
            Callable[[NodeFact | EdgeFact], None] | None
        ) = None,
        image_completer: Optional[ImageCompleter] = None,
    ) -> FlushResult:
        """Flush ONE chunk of (facts + markers) atomically.

        Per `add-substrate-incremental-publish` (2026-04-28):
        the chunk's facts AND its markers commit in ONE
        Postgres transaction. A `kill -9` between chunks
        leaves committed chunks durable (facts ↔ markers
        consistent); a kill mid-transaction rolls back BOTH
        sides.

        Pipeline (per chunk):
          -1. images (PACT first-class-images, #3046) — refuse a fact whose
             image values are forged or invalid, then complete every file
             image value with its stored PNG or a named error;
          0. sanitize (`_sanitize_facts`) — NUL admission
             normalization per PACT
             sanitize-fact-text-at-admission;
          1. canonicalize facts;
          2. in-chunk dedupe (`_dedupe_facts`) — defense vs
             buggy adapters yielding the same node_id twice
             within a chunk;
          3. vs-persisted dedupe (`_dedupe_against_persisted`)
             scoped to the chunk's IDs;
          4. run-wide dedupe against identities already flushed
             earlier in this run;
          5. open one transaction, COPY surviving facts,
             upsert markers, commit.
        """
        def add_flush_timing(label: str, started: float) -> None:
            ended = time.perf_counter()
            key = f"flush.{label}"
            if timing_recorder is not None:
                timing_recorder.record(key, started, ended_perf=ended)
            if timing_sink is None:
                return
            elapsed = round((ended - started) * 1000, 3)
            timing_sink[key] = round(timing_sink.get(key, 0.0) + elapsed, 3)

        def add_chronos_backend_timings(
            *,
            prefix: str,
            values: Mapping[str, Any] | None,
            ended: float,
        ) -> None:
            if not isinstance(values, Mapping):
                return
            for raw_label, raw_value in values.items():
                if not _timing_detail_is_duration(str(raw_label)):
                    continue
                try:
                    elapsed_ms = float(raw_value)
                except (TypeError, ValueError):
                    continue
                if elapsed_ms <= 0:
                    continue
                label = f"{prefix}.{_timing_label_segment(raw_label)}"
                started = max(0.0, ended - (elapsed_ms / 1000.0))
                if timing_recorder is not None:
                    timing_recorder.record(label, started, ended_perf=ended)
                if timing_sink is not None:
                    timing_sink[label] = round(
                        timing_sink.get(label, 0.0) + elapsed_ms,
                        3,
                    )

        # Step -1 — images (PACT first-class-images, #3046). The check runs
        # on the adapter's raw facts, every one of them, before anything in
        # this chunk is completed or written; a refusal fails the run.
        step_started = time.perf_counter()
        self._check_incoming_image_values(facts)
        if image_completer is not None:
            facts = image_completer.complete_facts(facts)
        elif any(
            isinstance(fact, NodeFact) and fact.op != "R"
            and _has_file_image_value(fact.attrs)
            for fact in facts
        ):
            # Every flush inside `run()` has a completer. Refuse a direct
            # flush rather than store a file value ingest never completed.
            raise SubstrateError(
                "SourceRunner: a file image value reached a flush with no "
                "image step",
                data={"source_name": self.source_name},
            )
        add_flush_timing("images", step_started)

        # Step 0 — sanitize (PACT sanitize-fact-text-at-admission):
        # scrub NUL from attr values (→ U+FFFD, counted) and reject
        # facts whose identity fields carry NUL. MUST run before
        # lineage stamping / COPY so Postgres never sees U+0000.
        step_started = time.perf_counter()
        facts, sanitize_summary = self._sanitize_facts(
            facts, warn_budget=sanitize_warn_budget,
        )
        add_flush_timing("sanitize", step_started)

        # Step 0.5 — at-rest PII transform (PACT bind-at-rest-anonymization):
        # for a not-store-raw source, pseudonymize/mask direct-identifier and
        # sensitive attrs and mask detected free-text PII spans. Runs right after
        # NUL scrub and BEFORE canonicalize/lineage/COPY, so no downstream stage
        # ever sees raw PII. A store_raw source (the default) is untouched.
        step_started = time.perf_counter()
        facts = self._apply_pii_posture(facts)
        add_flush_timing("pii_transform", step_started)

        # Step 1 — canonicalize.
        step_started = time.perf_counter()
        snapshot = self._active_pipeline_snapshot
        if (
            snapshot.canonicalizers
            if snapshot is not None else self.canonicalizers
        ):
            facts = self._canonicalize_facts(facts)
        policy_fingerprint = self._active_nomos_policy_fingerprint()
        facts = self._stamp_source_lineage_on_facts(
            facts,
            policy_fingerprint=policy_fingerprint,
        )
        if normalized_fact_observer is not None:
            for fact in facts:
                if isinstance(fact, (NodeFact, EdgeFact)):
                    normalized_fact_observer(fact)
        add_flush_timing("canonicalize", step_started)

        # Step 2 — in-chunk dedupe.
        step_started = time.perf_counter()
        (
            facts,
            in_chunk_drops_node,
            in_chunk_drops_edge,
            first_dropped,
            drops_by_prefix,
        ) = self._dedupe_facts(facts)
        add_flush_timing("dedupe_in_chunk", step_started)

        # Step 3 — vs-persisted dedupe (per chunk).
        step_started = time.perf_counter()
        self._persisted_dedupe_detail = PersistedDedupeDetail()
        (
            facts,
            vs_persisted_drops_node,
            vs_persisted_drops_edge,
        ) = self._dedupe_against_persisted(facts)
        persisted_detail = self._persisted_dedupe_detail
        add_flush_timing("dedupe_persisted", step_started)

        # Step 4 — run-wide dedupe across already-flushed chunks.
        step_started = time.perf_counter()
        if seen_node_keys is not None and seen_edge_keys is not None:
            (
                facts,
                run_drops_node,
                run_drops_edge,
                run_first_dropped,
                run_drops_by_prefix,
            ) = self._dedupe_against_run_seen(
                facts,
                seen_node_keys=seen_node_keys,
                seen_edge_keys=seen_edge_keys,
            )
            in_chunk_drops_node += run_drops_node
            in_chunk_drops_edge += run_drops_edge
            if first_dropped is None:
                first_dropped = run_first_dropped
            for k, v in run_drops_by_prefix.items():
                drops_by_prefix[k] = drops_by_prefix.get(k, 0) + v
        add_flush_timing("dedupe_run_seen", step_started)

        # Step 5 — transactional COPY + UPSERT markers.
        step_started = time.perf_counter()
        fact_log_rows = self._build_fact_log_rows(
            facts,
            run_id=run_id,
            observed_at=observed_at,
        )
        add_flush_timing("build_rows", step_started)

        # Build the marker upsert rows once; emit inside the
        # transaction so atomicity holds between facts and
        # markers.
        step_started = time.perf_counter()
        fact_log_rows.marker_rows = self._build_progress_marker_rows(
            markers,
            sanitize_summary=sanitize_summary,
        )
        add_flush_timing("build_markers", step_started)

        # Issue #3003: a command-line run holds its deployment's source-sync
        # signal; if that lock's session died, stop before this chunk writes.
        ensure_run_guard_held(self.deployment_name)
        conn = self._acquire_conn()
        admitted_fact_batch = (
            self._active_run_context is not None
            and self._active_run_context.admission_id is not None
            and self._active_run_context.idempotency_key is not None
            and self._active_run_context.producer_profile is not None
            and self.deployment_name is not None
        )
        if not admitted_fact_batch:
            step_started = time.perf_counter()
            self._persist_fact_log_chunk(conn, fact_log_rows)
            add_flush_timing("fact_log_tx", step_started)
        # Admitted chunks persist their optional audit rows and progress
        # markers only after graph application, in the same transaction as
        # the durable applied receipt. Compatibility callers retain the
        # pre-FactBatch transaction above.
        step_started = time.perf_counter()
        if facts or (
            markers
            and self._active_run_context is not None
            and self._active_run_context.producer_profile is not None
        ):
            graph_store = self._chronos_graph_store()
        else:
            graph_store = None
        add_flush_timing("graph_store", step_started)
        step_started = time.perf_counter()
        graph_write_set = self._apply_chronos_facts(
            facts,
            markers=markers,
            fact_log_rows=(fact_log_rows if admitted_fact_batch else None),
            graph_store=graph_store,
            policy_fingerprint=policy_fingerprint,
            run_id=run_id,
            observed_at=observed_at,
        )
        chronos_apply_ended = time.perf_counter()
        add_chronos_backend_timings(
            prefix="flush.chronos_apply",
            values=graph_write_set.timings_ms,
            ended=chronos_apply_ended,
        )
        add_flush_timing("chronos_apply", step_started)

        return FlushResult(
            counts=FlushCounts(
                nodes_inserted=fact_log_rows.nodes_inserted,
                nodes_retracted=fact_log_rows.nodes_retracted,
                edges_inserted=fact_log_rows.edges_inserted,
                edges_retracted=fact_log_rows.edges_retracted,
            ),
            dedupe=DedupeDropSummary(
                in_chunk_nodes=in_chunk_drops_node,
                in_chunk_edges=in_chunk_drops_edge,
                persisted_nodes=vs_persisted_drops_node,
                persisted_edges=vs_persisted_drops_edge,
                first_dropped=first_dropped,
                by_prefix=drops_by_prefix,
                persisted_detail=persisted_detail,
            ),
            graph_write_set=graph_write_set,
            sanitize=sanitize_summary,
        )

    def _build_fact_log_rows(
        self,
        facts: list,
        *,
        run_id: str,
        observed_at: datetime,
    ) -> FactLogRows:
        rows = FactLogRows()
        for fact in facts:
            if isinstance(fact, NodeFact):
                rows.node_rows.append(self._node_row(
                    fact,
                    run_id=run_id,
                    observed_at=observed_at,
                ))
                if fact.op == "I":
                    rows.nodes_inserted += 1
                else:
                    rows.nodes_retracted += 1
            elif isinstance(fact, EdgeFact):
                rows.edge_rows.append(self._edge_row(
                    fact,
                    run_id=run_id,
                    observed_at=observed_at,
                ))
                if fact.op == "I":
                    rows.edges_inserted += 1
                else:
                    rows.edges_retracted += 1
            else:
                raise SubstrateError(
                    f"SourceRunner: post-canonicalize stream "
                    f"contains item of unknown type "
                    f"{type(fact).__name__}",
                    data={"source_name": self.source_name},
                )
        return rows

    def _build_progress_marker_rows(
        self,
        markers: list[ProgressMarker],
        *,
        sanitize_summary: Optional[SanitizeSummary] = None,
    ) -> list[tuple]:
        if self.deployment_name is None or not markers:
            return []
        # PACT sanitize-fact-text-at-admission: `okg.source_progress`
        # is text columns, so a NUL (U+0000) in record_id/fingerprint
        # would abort the whole chunk transaction. Progress keys are
        # resume IDENTITY — never silently rewritten — so a
        # NUL-bearing marker is rejected loudly; its record simply
        # re-parses next run.
        from okg.substrate.ingest.sanitize import NUL

        rows: list[tuple] = []
        rejected_here = 0
        first_here: Optional[str] = None
        for marker in markers:
            if NUL in marker.record_id or NUL in marker.fingerprint:
                rejected_here += 1
                if first_here is None:
                    first_here = (
                        f"record:{marker.record_id}"
                        .replace(NUL, "\\x00")[:200]
                    )
                continue
            rows.append((
                self.deployment_name,
                self.source_name,
                f"record:{marker.record_id}",
                marker.fingerprint,
            ))
        if rejected_here:
            if sanitize_summary is not None:
                sanitize_summary.rejected_marker_count += rejected_here
                if sanitize_summary.first_rejected_marker is None:
                    sanitize_summary.first_rejected_marker = first_here
            log.warning(
                "SourceRunner: rejected %d progress marker(s) with NUL "
                "(U+0000) in record_id/fingerprint from source=%s "
                "(first=%r); progress identity is never rewritten — "
                "those records re-parse next run (PACT "
                "sanitize-fact-text-at-admission)",
                rejected_here,
                self.source_name,
                first_here,
            )
        return rows

    def _fact_log_rows_to_persist(
        self,
        rows: FactLogRows,
    ) -> tuple[list[tuple], list[tuple]]:
        """The fact-log rows this chunk writes: ``(node_rows, edge_rows)``.

        Every row when the fact log is on. Otherwise only a held run's
        retractions (issue #2999): a review-mode run writes the quarantine
        branch, where a retracted row is simply gone, and approving the run
        rebuilds its deletions for `default` from these op='R' rows
        (``admission_workflows._load_run_retraction_facts``). Without them an
        approved re-sync never deletes what the source removed. Upserts need
        no log: the approval reads them from the quarantine branch.
        """
        if self.write_fact_log:
            return rows.node_rows, rows.edge_rows
        if self.graph_branch != QUARANTINE_BRANCH:
            return [], []
        return (
            [row for row in rows.node_rows if row[_NODE_ROW_OP] == "R"],
            [row for row in rows.edge_rows if row[_EDGE_ROW_OP] == "R"],
        )

    def _persist_fact_log_chunk(
        self,
        conn: psycopg.Connection,
        rows: FactLogRows,
    ) -> None:
        node_rows, edge_rows = self._fact_log_rows_to_persist(rows)
        with conn.transaction():  # one tx for optional audit facts + markers
            # Ledger the ingestion program so the witness stamped on the audit
            # fact-log resolves in okg.ir_programs (commit-witness; route-all-writers).
            # Chronos-native publishes set write_fact_log=False; the canonical graph
            # write then goes to graph_nodes/graph_edges and is witnessed there.
            if node_rows or edge_rows:
                persist_transform_program(conn, _INGESTION_PROGRAM)
            if node_rows:
                self._copy_in_one_tx(
                    conn,
                    sql=(
                        "COPY okg.node_facts ("
                        "  node_id, subtype, attrs, source, "
                        "  source_record_id, source_revision, "
                        "  observed_at, dedupe_key, run_id, op, "
                        "  source_program_checksum"
                        ") FROM STDIN"
                    ),
                    rows=node_rows,
                )
            if edge_rows:
                self._copy_in_one_tx(
                    conn,
                    sql=(
                        "COPY okg.edge_facts ("
                        "  edge_id, src, dst, edge_type, attrs, "
                        "  provenance, confidence, source, "
                        "  source_record_id, source_revision, "
                        "  observed_at, dedupe_key, run_id, op, "
                        "  source_program_checksum"
                        ") FROM STDIN"
                    ),
                    rows=edge_rows,
                )
            if rows.marker_rows:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO okg.source_progress
                          (deployment, source_name, key,
                           value, updated_at)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (deployment, source_name, key)
                          DO UPDATE SET
                            value      = EXCLUDED.value,
                            updated_at = EXCLUDED.updated_at
                        """,
                        rows.marker_rows,
                    )

    @staticmethod
    def _name_refused_retraction(refusal: SubstrateError, fact: Any) -> None:
        """Record which fact the resolved refusal was raised for."""
        data = getattr(refusal, "data", None)
        if not isinstance(data, dict):
            return
        if isinstance(fact, NodeFact):
            data["refused_fact"] = {"node_id": fact.node_id}
        elif isinstance(fact, EdgeFact):
            data["refused_fact"] = {
                "src": fact.src,
                "edge_type": fact.edge_type,
                "dst": fact.dst,
            }

    def _snapshot_source_state_for_compensation(self) -> None:
        """Record the source's watermark and progress before this run writes.

        Issue #1431. Only runners that own their own compensation take this
        snapshot; the orchestrator takes the same one before it calls `run()`
        and restores it itself. Two indexed reads per run, not per flush.

        A runner with no deployment scope has no `okg.source_progress` /
        `okg.source_watermarks` identity to snapshot, so a failed run there is
        compensated in the graph only - stated here because a silent partial
        restore would be worse than a named one.
        """
        if not self.compensate_on_failure or self.deployment_name is None:
            self._source_state_snapshot = None
            return
        from okg.substrate.ingest.compensation import snapshot_source_state

        self._source_state_snapshot = snapshot_source_state(
            self.dsn,
            deployment=self.deployment_name,
            source=self.source_name,
        )

    def _refuse_rerun_while_undo_unfinished(self, run_id: str) -> None:
        """Refuse before reading while an earlier run of this source has an
        unfinished undo (issue #3592; see
        ``run_base.refuse_rerun_while_undo_unfinished``)."""
        self._run_started_db_at = None
        if self.deployment_name is None or not self.write_chronos_graph:
            return
        from okg.substrate.ingest.run_base import (
            refuse_rerun_while_undo_unfinished,
        )

        conn = self._acquire_conn()
        # Every progress marker this run saves is stamped after this time;
        # the run base records it so an undo forgets them all.
        self._run_started_db_at = str(
            conn.execute("SELECT now()::text").fetchone()[0]
        )
        refuse_rerun_while_undo_unfinished(
            conn,
            deployment=self.deployment_name,
            source=self.source_name,
            run_id=run_id,
        )

    def _take_run_base(self, run_id: str, graph_store: Any) -> None:
        """Cut this run's run-base checkpoint before its first write (#2993).

        Compensation undoes a failed run by writing every key it touched back
        to this checkpoint's version. Taken for every run that writes the
        Chronos graph, whoever compensates it: the orchestrator deletes it
        after the run's admission is final, and a runner that owns its
        compensation deletes it when the run ends. One checkpoint row and a
        segment split; no graph row is copied.
        """
        from okg.substrate.ingest.run_base import (
            hold_run_liveness,
            run_base_checkpoint_id,
            take_run_base,
        )

        # The liveness lock is taken before the cut and held until the run's
        # admission is final (see `_release_run_liveness`): a run base whose
        # lock nobody holds belongs to a run that ended, and without an
        # accepted admission that is an unfinished undo (issue #2993 review).
        liveness = hold_run_liveness(
            self.dsn, run_base_checkpoint_id(self.graph_branch, run_id),
        )
        context = self._active_run_context
        try:
            self._active_run_base = take_run_base(
                graph_store,
                deployment=self.deployment_name,
                branch=self.graph_branch,
                source=self.source_name,
                run_id=run_id,
                admission_id=(
                    context.admission_id if context is not None else None
                ),
                progress_since=getattr(self, "_run_started_db_at", None),
            )
        except BaseException:
            liveness.close()
            raise
        if self._active_run_base is None:
            liveness.close()
            return
        self._run_liveness_conn = liveness

    def _release_run_liveness(self) -> None:
        """Release the run's liveness lock (closing its connection).

        A runner that owns its compensation releases it when `run()` ends;
        the ingest orchestrator releases it through
        `release_deferred_bootstrap_lock`, after the admission is final.
        """
        liveness = getattr(self, "_run_liveness_conn", None)
        self._run_liveness_conn = None
        if liveness is not None:
            try:
                liveness.close()
            except Exception:  # noqa: BLE001 - the lock goes with the session
                log.warning(
                    "SourceRunner: closing the run liveness connection failed "
                    "for source=%s", self.source_name, exc_info=True,
                )

    def _release_owned_run_base(self) -> None:
        """Delete the run base when this runner owns the run's compensation.

        The orchestrator (``compensate_on_failure=False``) keeps it until the
        admission is final, since a rejected admission still needs the undo.
        A run base whose undo started and did not finish is kept either way.
        """
        if not self.compensate_on_failure or self._active_run_base is None:
            return
        from okg.substrate.ingest.run_base import release_run_base

        release_run_base(
            self.dsn,
            branch=self.graph_branch,
            run_id=str(self._active_run_id),
        )
        self._active_run_base = None

    def _compensate_failed_run(
        self,
        run_id: str | None,
        original: BaseException | None = None,
    ) -> None:
        """Undo everything this run made durable before it was refused.

        Issue #1431: the streaming loop commits a chunk every 100 progress
        markers or 10,000 facts, so by the time a run raises a forbidden
        retraction, earlier chunks are already applied. Only that refusal
        reaches here - other failures deliberately keep the durable evidence
        and the replay fence that make the next run reapply. Callers that own
        compensation themselves construct the runner with
        `compensate_on_failure=False`.

        A compensation failure is raised, never swallowed: a half-undone run
        must not look like a clean refusal. The raised error names BOTH what
        failed and what could not be undone, so the original diagnosis is not
        lost behind the recovery error.
        """
        if not self.compensate_on_failure or not self._run_applied_chunks:
            return
        if run_id is None:
            return
        from okg.substrate.ingest.compensation import retract_chronos_run

        context = self._active_run_context
        # The runner-owned graph store and the run's pooled connection both
        # touch the rows the compensation is about to rewrite. Release them
        # first so the compensation cannot wait on this run's own session.
        if self._owned_graph_store is not None:
            self._owned_graph_store.close()
            self._owned_graph_store = None
        self._release_conn()
        try:
            retract_chronos_run(
                self.dsn,
                admission_id=(
                    context.admission_id if context is not None else None
                ),
                source=self.source_name,
                run_id=run_id,
                branch=self.graph_branch,
            )
            snapshot = self._source_state_snapshot
            if snapshot is not None:
                from okg.substrate.ingest.compensation import (
                    restore_source_state,
                )

                restore_source_state(
                    self.dsn,
                    snapshot,
                    # Only rewind a replay fence this run itself set; a fence
                    # another run owns is not ours to clear.
                    expected_replay_run_id=(
                        run_id if self._run_marked_replay_fence else None
                    ),
                )
        except Exception as compensation_error:
            err = SubstrateError(
                "source run failure compensation is unresolved; chunks this "
                "run applied may still be durable",
                data={
                    "source_name": self.source_name,
                    "run_id": run_id,
                    "original_error": (
                        f"{type(original).__name__}: {original}"
                        if original is not None
                        else None
                    ),
                    "compensation_error": (
                        f"{type(compensation_error).__name__}: "
                        f"{compensation_error}"
                    ),
                },
                fix_hint=(
                    "resolve the compensation by hand (see the receipt states "
                    "in okg.fact_batch_receipts for this run) before rerunning "
                    "the source"
                ),
            )
            err.code = "source_run_compensation_unresolved"
            raise err from compensation_error

    def _apply_chronos_facts(
        self,
        facts: list,
        *,
        markers: list[ProgressMarker],
        fact_log_rows: FactLogRows | None,
        graph_store: Any | None,
        policy_fingerprint: str | None,
        run_id: str,
        observed_at: datetime,
    ) -> GraphWriteSet:
        if graph_store is None:
            return GraphWriteSet()
        if self.graph_branch != "default":
            graph_store.ensure_branch(
                self.graph_branch,
                from_branch="default",
            )
        if facts and not self._run_base_attempted:
            # Issue #2993: the run base is cut just before the run's first
            # graph write, so a run that writes nothing splits no segment.
            self._run_base_attempted = True
            self._take_run_base(run_id, graph_store)
        graph_result = None
        context = self._active_run_context
        if (
            context is not None
            and context.admission_id is not None
            and context.idempotency_key is not None
            and context.producer_profile is not None
            and self.deployment_name is not None
        ):
            from okg.substrate.fact_application import (
                FactBatchApplication,
                chronos_apply_callback,
            )
            from okg.substrate.fact_batch import (
                FactBatchHeader,
                FactChunk,
                PostgresApplicationReceiptStore,
            )

            if self._fact_batch_application is None:
                profile = context.producer_profile
                header = FactBatchHeader(
                    schema_version=1,
                    run_id=run_id,
                    admission_id=context.admission_id,
                    producer_profile_fingerprint=(profile.profile_fingerprint),
                    deployment=self.deployment_name,
                    target_branch=self.graph_branch,
                    catalog_version_id=profile.catalog_version_id or 0,
                    intent="source_sync",
                    idempotency_key=context.idempotency_key,
                    # Source sync scope is server-resolved invocation evidence,
                    # not producer-authored write authority. It is preserved in
                    # SourceDelta/terminal evidence; binding it as a requested
                    # authority scope would make every scoped compatibility
                    # source exceed its (intentionally empty) profile scope.
                    requested_scope=(profile.authority_scope or None),
                )
                graph_apply = chronos_apply_callback(
                    graph_store,
                    source=self.source_name,
                    observed_at=observed_at,
                    policy_fingerprint=policy_fingerprint,
                    strict_endpoints=False,
                )

                def _capture_graph_result(validated_facts, batch_header):
                    nonlocal graph_result
                    if not validated_facts:
                        return {}
                    graph_result = graph_apply(
                        validated_facts,
                        batch_header,
                    )
                    return graph_result

                self._fact_batch_application = FactBatchApplication(
                    header=header,
                    profile=profile,
                    receipt_store=PostgresApplicationReceiptStore(self._acquire_conn()),
                    apply_graph=_capture_graph_result,
                    # Source signatures are range- and catalog-aware and are
                    # evaluated at terminal admission after the durable fact
                    # rows exist. Chunk application still enforces profile,
                    # record-kind, retraction, digest, and receipt authority;
                    # duplicating signature evaluation here would turn an
                    # admission rejection into an earlier runner exception.
                    validate_signature=False,
                    write_chunk_evidence=(
                        (
                            lambda _chunk, _receipt: (
                                self._persist_fact_log_chunk(
                                    self._acquire_conn(),
                                    fact_log_rows,
                                )
                                or ()
                            )
                        )
                        if fact_log_rows is not None
                        else None
                    ),
                    # SourceRunner resolves retraction rights from the
                    # registered source profile and run mode before streaming
                    # (`_resolve_retraction_authority`). Issue #1431: this used
                    # to pass a blanket `True`, which made the chunk-level
                    # authority check vacuous; pass the resolved verdict so a
                    # forbidden retraction is refused at the chunk boundary
                    # too. `None` means the resolution has not run for this
                    # code path, which is not authority to retract.
                    retractions_allowed=bool(self._active_retractions_allowed),
                )
            else:
                # The first callback owns the capture closure. Replace it so
                # this flush returns its own primitive graph result.
                graph_apply = chronos_apply_callback(
                    graph_store,
                    source=self.source_name,
                    observed_at=observed_at,
                    policy_fingerprint=policy_fingerprint,
                    strict_endpoints=False,
                )

                def _capture_graph_result(validated_facts, batch_header):
                    nonlocal graph_result
                    if not validated_facts:
                        return {}
                    graph_result = graph_apply(
                        validated_facts,
                        batch_header,
                    )
                    return graph_result

                self._fact_batch_application.apply_graph = _capture_graph_result
                self._fact_batch_application.write_chunk_evidence = (
                    (
                        lambda _chunk, _receipt: (
                            self._persist_fact_log_chunk(
                                self._acquire_conn(),
                                fact_log_rows,
                            )
                            or ()
                        )
                    )
                    if fact_log_rows is not None
                    else None
                )
            ordinal = self._fact_batch_application.validator.accepted_chunk_count
            accepted_markers = [
                marker
                for marker in markers
                if "\x00" not in marker.record_id and "\x00" not in marker.fingerprint
            ]
            self._fact_batch_application.apply_chunk(
                FactChunk.create(
                    run_id=run_id,
                    ordinal=ordinal,
                    facts=facts,
                    progress_markers=accepted_markers,
                )
            )
        elif facts:
            # Explicit compatibility path for direct/standalone SourceRunner
            # callers that have no admission context. The orchestrated source
            # path always supplies a server-resolved profile above.
            graph_result = graph_store.apply_facts(
                facts,
                branch=self.graph_branch,
                source=self.source_name,
                policy_fingerprint=policy_fingerprint,
                run_id=run_id,
                observed_at=observed_at,
                strict_endpoints=False,
            )
        if graph_result is None:
            return GraphWriteSet()
        return GraphWriteSet(
            nodes_by_subtype={
                subtype: list(node_ids)
                for subtype, node_ids in graph_result.nodes_by_subtype.items()
            },
            node_attr_changes_by_subtype={
                subtype: {
                    node_id: list(fields)
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (
                    graph_result.node_attr_changes_by_subtype.items()
                )
            },
            edges_by_type={
                edge_type: list(edge_ids)
                for edge_type, edge_ids in graph_result.edges_by_type.items()
            },
            edge_table_available=graph_result.edge_table_available,
            timings_ms={
                str(label): float(value)
                for label, value in (
                    getattr(graph_result, "timings_ms", {}) or {}
                ).items()
                if isinstance(value, (int, float))
            },
        )

    @staticmethod
    def _copy_in_one_tx(
        conn: psycopg.Connection, *, sql: str, rows: list[tuple],
    ) -> None:
        """COPY all `rows` inside the CALLER's open transaction.

        Differs from `_copy_in_chunks` (which opens its own
        per-batch sub-transaction) in that the caller is
        expected to hold an outer `conn.transaction()` so the
        COPY commits atomically with whatever the caller
        appends afterwards (e.g., a marker upsert).
        """
        with conn.cursor() as cur:
            with cur.copy(sql) as cp:
                for r in rows:
                    cp.write_row(r)

    def _write_facts(
        self,
        facts: list[NodeFact | EdgeFact],
        *,
        run_id: str,
        observed_at: datetime,
    ) -> tuple[int, int, int, int]:
        """LEGACY: COPY node_facts + edge_facts in chunks. Returns:
          (n_nodes_op_I, n_nodes_op_R, n_edges_op_I, n_edges_op_R).

        Kept for any caller that still drives the old
        materialize-then-write path. New code SHOULD use
        `_flush_chunk` via the streaming `run()` loop.
        """
        node_rows: list[tuple] = []
        edge_rows: list[tuple] = []
        n_node_i = n_node_r = n_edge_i = n_edge_r = 0

        for f in facts:
            if isinstance(f, NodeFact):
                node_rows.append(self._node_row(
                    f, run_id=run_id, observed_at=observed_at,
                ))
                if f.op == "I":
                    n_node_i += 1
                else:
                    n_node_r += 1
            elif isinstance(f, EdgeFact):
                edge_rows.append(self._edge_row(
                    f, run_id=run_id, observed_at=observed_at,
                ))
                if f.op == "I":
                    n_edge_i += 1
                else:
                    n_edge_r += 1
            else:
                raise SubstrateError(
                    f"SourceRunner: adapter yielded unknown fact "
                    f"type {type(f).__name__}; expected NodeFact "
                    f"or EdgeFact",
                    data={"source_name": self.source_name},
                )

        conn = self._acquire_conn()
        # Ledger the ingestion program so the witness stamped on facts
        # resolves in okg.ir_programs (commit-witness; route-all-writers).
        persist_transform_program(conn, _INGESTION_PROGRAM)
        if node_rows:
            self._copy_in_chunks(
                conn,
                sql=(
                    "COPY okg.node_facts ("
                    "  node_id, subtype, attrs, source, "
                    "  source_record_id, source_revision, "
                    "  observed_at, dedupe_key, run_id, op, "
                    "  source_program_checksum"
                    ") FROM STDIN"
                ),
                rows=node_rows,
            )
        if edge_rows:
            # Mint edge_ids deterministically; bootstrap
            # uses a counter. These two spaces must not
            # collide at envelope scale (10^8 edges);
            # uuid5-derived ids have ~0 collision risk.
            self._copy_in_chunks(
                conn,
                sql=(
                    "COPY okg.edge_facts ("
                    "  edge_id, src, dst, edge_type, attrs, "
                    "  provenance, confidence, source, "
                    "  source_record_id, source_revision, "
                    "  observed_at, dedupe_key, run_id, op, "
                    "  source_program_checksum"
                    ") FROM STDIN"
                ),
                rows=edge_rows,
            )

        return n_node_i, n_node_r, n_edge_i, n_edge_r

    def _node_row(
        self, f: NodeFact, *, run_id: str, observed_at: datetime,
    ) -> tuple:
        # dedupe_key collapses the same (source, node_id, revision)
        # across arrival order. The adapter's source_revision
        # carries the caller's idea of "this is a new version";
        # we hash the whole dict for stability.
        revision_bytes = json.dumps(
            f.source_revision, sort_keys=True, default=str,
        ).encode("utf-8")
        dedupe = (
            f"{self.source_name}:n:"
            f"{uuid.uuid5(uuid.NAMESPACE_URL, f.node_id + '|' + revision_bytes.hex()).hex}"
            f":{f.op}"
        )
        return (
            f.node_id,
            f.subtype,
            json.dumps(f.attrs, default=str),
            self.source_name,
            json.dumps(f.source_record_id, default=str),
            json.dumps(f.source_revision, default=str),
            observed_at,
            dedupe,
            run_id,
            f.op,
            _INGESTION_CHECKSUM,
        )

    def _edge_row(
        self, f: EdgeFact, *, run_id: str, observed_at: datetime,
    ) -> tuple:
        edge_id = _mint_edge_id(f.src, f.edge_type, f.dst)
        revision_bytes = json.dumps(
            f.source_revision, sort_keys=True, default=str,
        ).encode("utf-8")
        dedupe = (
            f"{self.source_name}:e:"
            f"{uuid.uuid5(uuid.NAMESPACE_URL, f'{f.src}|{f.edge_type}|{f.dst}|' + revision_bytes.hex()).hex}"
            f":{f.op}"
        )
        return (
            edge_id,
            f.src,
            f.dst,
            f.edge_type,
            json.dumps(f.attrs, default=str),
            f.provenance,
            f.confidence,
            self.source_name,
            json.dumps(f.source_record_id, default=str),
            json.dumps(f.source_revision, default=str),
            observed_at,
            dedupe,
            run_id,
            f.op,
            _INGESTION_CHECKSUM,
        )

    @staticmethod
    def _copy_in_chunks(
        conn: psycopg.Connection, *, sql: str, rows: list[tuple],
    ) -> None:
        """COPY `rows` in batches of COPY_CHUNK_ROWS so the
        wire framing + memory footprint stay bounded."""
        for start in range(0, len(rows), COPY_CHUNK_ROWS):
            chunk = rows[start:start + COPY_CHUNK_ROWS]
            with conn.transaction():
                with conn.cursor() as cur:
                    with cur.copy(sql) as cp:
                        for r in chunk:
                            cp.write_row(r)

    def _advance_cursor_watermark_boundary(
        self,
        *,
        next_cursor: Optional[dict[str, Any]],
        observed_at: datetime,
        completed_scope: bool,
    ) -> Optional[dict[str, Any]]:
        cursor = self._cursor_with_externalized_fields(next_cursor)
        context = self._active_run_context
        admitted = context is not None and context.admission_id is not None
        if self._active_private_authority_candidate is not None and not admitted:
            raise SubstrateError(
                "indexed authority requires terminal seal-and-watermark binding",
                data={"source_name": self.source_name},
            )
        if not admitted:
            self._advance_watermark(
                next_cursor=cursor,
                last_reconcile=(observed_at if completed_scope else None),
                clear_replay_required=self._active_clear_replay_required,
                replay_target_fingerprint=(
                    str(self._active_replay_target["target_fingerprint"])
                    if self._active_replay_target is not None else None
                ),
                replay_target_run_id=(
                    str(self._active_replay_target["run_id"])
                    if self._active_replay_target is not None else None
                ),
            )
        return cursor

    def _advance_watermark(
        self,
        *,
        next_cursor: Optional[dict[str, Any]],
        last_reconcile: Optional[datetime],
        clear_replay_required: bool = False,
        replay_target_fingerprint: str | None = None,
        replay_target_run_id: str | None = None,
    ) -> None:
        """UPSERT source_watermarks.

        `next_cursor` becomes `last_applied_rev`. If the run
        completed a scope walk, `last_reconcile_at` is updated
        too.
        """
        persist_source_watermark(
            self._acquire_conn(),
            source_name=self.source_name,
            deployment_name=self.deployment_name,
            next_cursor=next_cursor,
            last_reconcile=last_reconcile,
            clear_replay_required=clear_replay_required,
            replay_target_fingerprint=replay_target_fingerprint,
            replay_target_run_id=replay_target_run_id,
            expected_last_applied_rev=(
                self._active_watermark_cas_expected
            ),
            require_compare_and_swap=(
                self._active_watermark_cas_required
            ),
            expected_bootstrap_ledger=(
                self._active_bootstrap_ledger_expected
            ),
            next_bootstrap_ledger=self._active_bootstrap_ledger_next,
        )

    def _mark_replay_required(
        self,
        *,
        run_id: str,
        obligation: str,
        raw_token: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Durably fence a source before invoking its reader."""
        # Issue #1431: a compensation may only rewind a fence THIS run set.
        self._run_marked_replay_fence = True
        snapshot = self._active_pipeline_snapshot
        if snapshot is None:
            raise SubstrateError("pipeline identity snapshot is unavailable")
        target_fingerprint = identity_fingerprint({
            "pipeline": snapshot.fingerprint,
            "raw_token": dict(raw_token or {}),
            "obligation": obligation,
        })
        payload = {
            "schema": "okg.source_runner.replay_required.v1",
            "target_fingerprint": target_fingerprint,
            "pipeline_fingerprint": snapshot.fingerprint,
            "obligation": obligation,
            "run_id": run_id,
        }
        with self._acquire_conn().cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, replay_required, replay_required_at)
                VALUES (%s, %s::jsonb, now())
                ON CONFLICT (source) DO UPDATE SET
                  replay_required = CASE
                    WHEN okg.source_watermarks.replay_required
                           ->> 'obligation' IN (
                             'authoritative_full_replay',
                             'bootstrap_identity_resume'
                           )
                     AND EXCLUDED.replay_required
                           ->> 'obligation' = 'in_flight_delta'
                    THEN okg.source_watermarks.replay_required
                    ELSE EXCLUDED.replay_required
                  END,
                  replay_required_at = CASE
                    WHEN okg.source_watermarks.replay_required
                           ->> 'obligation' IN (
                             'authoritative_full_replay',
                             'bootstrap_identity_resume'
                           )
                     AND EXCLUDED.replay_required
                           ->> 'obligation' = 'in_flight_delta'
                    THEN okg.source_watermarks.replay_required_at
                    ELSE EXCLUDED.replay_required_at
                  END
                RETURNING replay_required
                """,
                (self.source_name, json.dumps(payload, sort_keys=True)),
            )
            row = cur.fetchone()
        persisted = (
            dict(row[0])
            if row and isinstance(row[0], Mapping)
            else payload
        )
        self._active_replay_target = persisted
        return persisted

    # ---- Change-probe gate helpers (issue #323) ----

    def _validate_bootstrap_identity_run(
        self,
        *,
        adapter: Any,
        source_run: Any,
        next_cursor: Optional[dict[str, Any]],
        prior_cursor: Optional[dict[str, Any]],
        adapter_cursor: Optional[dict[str, Any]],
        persisted_cursor: Optional[dict[str, Any]],
        prior_authority: Mapping[str, Any] | None,
        bindings_before: Mapping[str, Any] | None,
        binding_error: SubstrateError | None,
        requested_sync_scope: Mapping[str, Any] | None,
        effective_sync_scope: Mapping[str, Any] | None,
        change_probe: Any,
        pre_probe_token: Mapping[str, Any] | None,
        acquire_health: SourceHealth | None,
        run_id: str,
    ) -> dict[str, Any]:
        """Validate and resolve a bootstrap mint/advance before fact writes."""
        backlogged = cursor_has_outstanding_work(next_cursor)
        if binding_error is not None:
            raise binding_error
        if bindings_before is None:
            bindings_before = self._bootstrap_binding_snapshot(
                adapter,
                requested_sync_scope=requested_sync_scope,
            )
        if type(source_run.facts) is not tuple:  # noqa: E721 - exact seal type.
            self._refuse_bootstrap_identity(
                "facts_not_sealed",
                "bootstrap facts are not an eagerly sealed snapshot",
                fix_hint=(
                    "materialize the bounded fact sequence from the pinned "
                    "snapshot before returning SourceRun"
                ),
                binding="facts_materialization",
                expected=BOOTSTRAP_SNAPSHOT_MATERIALIZATION,
                observed=type(source_run.facts).__name__,
            )
        health = source_run.health
        if health is None or health.status != "ok":
            self._refuse_bootstrap_identity(
                "health_not_affirmative",
                "bootstrap snapshot health was not explicitly healthy",
                fix_hint=(
                    "restore a fully readable source and retry; do not reset "
                    "progress for missing or transient health evidence"
                ),
                binding="source_health",
                expected="ok",
                observed=(health.status if health is not None else "unknown"),
            )
        if acquire_health is not None and acquire_health.status != "ok":
            self._refuse_bootstrap_identity(
                "acquisition_unhealthy",
                "bootstrap source acquisition was not explicitly healthy",
                fix_hint=(
                    "restore a current source checkout and retry; do not reset "
                    "progress for an acquisition failure"
                ),
                binding="acquire_health",
                expected="ok",
                observed=acquire_health.status,
            )
        if not pre_probe_token:
            self._refuse_bootstrap_identity(
                "probe_unavailable",
                "bootstrap source probe produced no pre-read identity",
                fix_hint=(
                    "restore the source probe and retry against a stable "
                    "snapshot; do not reset resumable progress"
                ),
            )
        try:
            validate_token_composition(pre_probe_token)
        except ChangeProbeError as exc:
            self._refuse_bootstrap_identity(
                "raw_token_malformed",
                "bootstrap pre-read identity is unsound",
                fix_hint="repair the probe token composition before retrying",
                binding="raw_token",
                expected="sound_change_probe_token",
                observed=str(exc),
            )
        bindings_after = self._bootstrap_binding_snapshot(
            adapter,
            requested_sync_scope=requested_sync_scope,
        )
        if bindings_after != bindings_before:
            self._refuse_bootstrap_identity(
                "binding_unstable",
                "bootstrap source binding changed while the snapshot was read",
                fix_hint=(
                    "restore the original source configuration and branch "
                    "incarnation, then retry"
                ),
                binding="binding_snapshot",
                expected=identity_fingerprint(bindings_before),
                observed=identity_fingerprint(bindings_after),
            )
        cursor = dict(next_cursor or {})
        injected_keys = sorted(
            key
            for key in (BOOTSTRAP_AUTHORITY_CURSOR_KEY, CURSOR_TOKEN_KEY)
            if key in cursor
        )
        if injected_keys:
            self._refuse_bootstrap_identity(
                "reserved_key_injection",
                "source adapter wrote runner-reserved identity cursor keys",
                fix_hint=(
                    "remove runner-reserved keys from the adapter cursor and "
                    "return identity through typed SourceRun fields"
                ),
                binding="reserved_cursor_keys",
                expected=[],
                observed=injected_keys,
            )
        if backlogged:
            claim = source_run.bootstrap_identity
            if not isinstance(claim, BootstrapIdentityClaim):
                self._refuse_bootstrap_identity(
                    "claim_missing",
                    "backlogged source run has no explicit bootstrap identity claim",
                    fix_hint=(
                        "return a sealed BootstrapIdentityClaim from the exact "
                        "bounded source snapshot"
                    ),
                )
            if (
                source_run.completed_scope
                or source_run.record_set is not None
                or source_run.record_authority_stream is not None
                or source_run.record_set_replacement_keys is not None
            ):
                self._refuse_bootstrap_identity(
                    "partial_completion_authority",
                    "backlogged bootstrap attempted completed-scope authority",
                    fix_hint=(
                        "return incomplete scope with no deletion RecordSet until "
                        "the bounded backlog reaches exact terminal completion"
                    ),
                )
            expected_semantics = self._bootstrap_cursor_semantics(adapter)
            claim_fields = {
                "scope": (claim.scope, "consumed_slices"),
                "cursor_semantics": (
                    claim.cursor_semantics,
                    expected_semantics,
                ),
                "snapshot_materialization": (
                    claim.snapshot_materialization,
                    BOOTSTRAP_SNAPSHOT_MATERIALIZATION,
                ),
            }
            mismatched_claim_fields = sorted(
                key
                for key, (observed, expected) in claim_fields.items()
                if observed != expected
            )
            if mismatched_claim_fields:
                self._refuse_bootstrap_identity(
                    "claim_mismatch",
                    "bootstrap identity claim does not match runner authority",
                    fix_hint="repair the adapter's explicit bootstrap contract",
                    binding="claim_fields",
                    expected=sorted(claim_fields),
                    observed=mismatched_claim_fields,
                )
            expected_input_cursor = dict(adapter_cursor or {})
            if dict(claim.input_cursor) != expected_input_cursor:
                self._refuse_bootstrap_identity(
                    "input_frontier_mismatch",
                    "bootstrap claim was not produced from the admitted cursor frontier",
                    fix_hint=(
                        "return the exact input cursor consumed by the bounded "
                        "adapter run"
                    ),
                    binding="input_cursor",
                    expected=identity_fingerprint(expected_input_cursor),
                    observed=identity_fingerprint(claim.input_cursor),
                )
            try:
                validate_token_composition(claim.raw_token)
            except ChangeProbeError as exc:
                self._refuse_bootstrap_identity(
                    "raw_token_malformed",
                    "bootstrap claim raw snapshot identity is unsound",
                    fix_hint="repair the claim token composition before retrying",
                    binding="raw_token",
                    expected="sound_change_probe_token",
                    observed=str(exc),
                )
            transition_identity = _bootstrap_transition_identity(
                self._active_run_context
            )
            if transition_identity is None:
                self._refuse_bootstrap_identity(
                    "transition_identity_missing",
                    "bootstrap transition has no explicit admission identity",
                    fix_hint=(
                        "run through admission with stable admission_id, run_id, "
                        "and idempotency_key"
                    ),
                )
            ledger_generation = (
                int(self._active_bootstrap_ledger["generation"])
                if self._active_bootstrap_ledger is not None else 0
            )
            generation = ledger_generation + 1
            authority = build_bootstrap_authority(
                raw_token=claim.raw_token,
                bindings=bindings_before,
                cursor_semantics=expected_semantics,
                resume_cursor=cursor,
                input_frontier_fingerprint=identity_fingerprint(
                    {"source_cursor": expected_input_cursor}
                ),
                transition_identity=transition_identity,
                generation=generation,
                nonce=str(uuid.uuid4()),
            )
            cursor[BOOTSTRAP_AUTHORITY_CURSOR_KEY] = authority
            self._active_bootstrap_ledger_expected = (
                dict(self._active_bootstrap_ledger)
                if self._active_bootstrap_ledger is not None else None
            )
            self._active_bootstrap_ledger_next = (
                _bootstrap_identity_ledger_entry(authority, status="active")
            )
            self._mark_replay_required(
                run_id=run_id,
                obligation="bootstrap_identity_resume",
                raw_token=claim.raw_token,
            )
        else:
            if prior_authority is None:
                raise AssertionError(
                    "bootstrap resolution requires backlog or prior authority"
                )
            post_verdict = run_probe(
                change_probe,
                cursor=raw_token_cursor(prior_cursor),
                sync_scope=effective_sync_scope,
            )
            post_token = post_verdict.token
            if not post_token:
                self._refuse_bootstrap_identity(
                    "post_probe_unavailable",
                    "terminal bootstrap probe produced no post-read identity",
                    fix_hint="restore the source probe and retry",
                )
            try:
                validate_token_composition(post_token)
            except ChangeProbeError as exc:
                self._refuse_bootstrap_identity(
                    "raw_token_malformed",
                    "terminal bootstrap post-read identity is unsound",
                    fix_hint="repair the probe token composition before retrying",
                    observed=str(exc),
                )
            if not tokens_equal(pre_probe_token, post_token):
                self._refuse_bootstrap_identity(
                    "snapshot_unstable",
                    "source changed during terminal bootstrap promotion",
                    fix_hint="retry after the source reaches a stable exact frontier",
                    binding="raw_snapshot",
                    expected=identity_fingerprint(pre_probe_token),
                    observed=identity_fingerprint(post_token),
                )
            applied_raw_token = source_run.applied_raw_token
            exact_applied = (
                source_run.applied_token_authority == "exact"
                and source_run.applied_token_scope == "global"
                and isinstance(applied_raw_token, Mapping)
                and bool(applied_raw_token)
                and tokens_equal(applied_raw_token, post_token)
            )
            if not exact_applied:
                self._refuse_bootstrap_identity(
                    "terminal_exact_missing",
                    "bootstrap resume did not reach an exact terminal identity",
                    fix_hint=(
                        "return an exact global applied_raw_token for the sealed "
                        "terminal snapshot before clearing bootstrap authority"
                    ),
                )
            snapshot = self._active_pipeline_snapshot
            if snapshot is None or not snapshot.is_intact():
                self._refuse_bootstrap_identity(
                    "pipeline_unstable",
                    "terminal bootstrap promotion lost pipeline identity",
                    fix_hint="restore the immutable pipeline and retry",
                )
            cursor = self._stamp_probe_token(
                cursor,
                build_effective_token(applied_raw_token, snapshot),
            )
            self._active_bootstrap_ledger_expected = (
                dict(self._active_bootstrap_ledger)
                if self._active_bootstrap_ledger is not None else None
            )
            terminal_transition_identity = _bootstrap_transition_identity(
                self._active_run_context
            )
            if terminal_transition_identity is None:
                self._refuse_bootstrap_identity(
                    "transition_identity_missing",
                    "terminal bootstrap transition has no explicit admission identity",
                    fix_hint=(
                        "run through admission with stable admission_id, run_id, "
                        "and idempotency_key"
                    ),
                )
            self._active_bootstrap_ledger_next = (
                _bootstrap_identity_ledger_entry(
                    prior_authority,
                    status="exact",
                    transition_identity=terminal_transition_identity,
                    terminal_cursor=cursor,
                )
            )
            self._active_clear_replay_required = True
        self._active_watermark_cas_required = True
        self._active_watermark_cas_expected = dict(persisted_cursor or {})
        return cursor

    @staticmethod
    def _adapter_probe_short_circuit_safe(adapter: Any) -> bool:
        """Whether the adapter's change-probe captures every input that
        determines its output.

        The change-probe hashes only the source's OWN inputs (file content /
        git frontier). A source that resolves edges to CROSS-SOURCE targets
        (e.g. `mentions` into another source's nodes) has an input the probe
        cannot see: a target retracted elsewhere changes the source's correct
        output without changing its files. Such a source reports
        ``probe_short_circuit_safe = False`` so the runner declines the
        short-circuit and lets ``run()`` reach its own freshness gate. Exact
        acknowledgement is fail-closed: a source must explicitly declare the
        attribute true after proving its reader returns snapshot-bound applied
        identity. Legacy probes with no declaration keep reading.
        """
        return bool(getattr(adapter, "probe_short_circuit_safe", False))

    def _probe_skip_eligible(
        self,
        *,
        mode: str,
        reset_cursor: bool,
        prior_cursor: Optional[dict[str, Any]],
    ) -> bool:
        """Whether a changed=false verdict may short-circuit the reader.

        Never short-circuits: a reset; a completed-scope / reconcile mode
        without a chronos-verified record-set proof (those carry the deletion
        proof, so the reader MUST run unless the prior completed footprint is
        already verified); a prior cursor whose record-set authority is NOT
        chronos-verified (a real read would run the stray-retraction sweep,
        which a skip would strand); or once the consecutive-skip budget is
        exhausted (defense-in-depth against a probe false-negative).
        """
        if not _change_probe_gate_enabled():
            return False
        if reset_cursor:
            return False
        active_scope = self._active_sync_scope or {}
        if "reconcile" in set(active_scope.get("trigger_kinds") or ()):
            return False
        if cursor_has_outstanding_work(prior_cursor):
            return False
        if mode in SOURCE_SYNC_COMPLETED_SCOPE_RUN_MODES:
            if not cursor_has_record_set_authority(prior_cursor):
                return False
            if not self._probe_skip_authority_clean(
                prior_cursor,
                require_external_reachability=True,
            ):
                return False
        elif not self._probe_skip_authority_clean(prior_cursor):
            return False
        if self._probe_skips(prior_cursor) >= _probe_skip_budget():
            return False
        return True

    def _probe_skip_authority_clean(
        self,
        prior_cursor: Optional[dict[str, Any]],
        *,
        require_external_reachability: bool = False,
    ) -> bool:
        # Nothing to strand when the prior cursor carries no record-set
        # authority. When it does, it must be chronos-verified: an un-verified
        # prior is exactly where a real read runs the stray-retraction sweep,
        # which a skip would silently skip.
        if not cursor_has_record_set_authority(prior_cursor):
            return True
        if not self.write_chronos_graph:
            return True
        if not self._prior_record_set_chronos_verified(prior_cursor):
            return False
        if (
            require_external_reachability
            and isinstance(prior_cursor, dict)
            and prior_cursor.get(_RECORD_SET_REF_KEY) is not None
        ):
            return self._external_record_set_ref_reachable(
                prior_cursor.get(_RECORD_SET_REF_KEY),
            )
        return True

    @staticmethod
    def _probe_skips(prior_cursor: Optional[dict[str, Any]]) -> int:
        if not isinstance(prior_cursor, dict):
            return 0
        value = prior_cursor.get(_PROBE_SKIP_COUNT_KEY)
        return value if isinstance(value, int) and value >= 0 else 0

    def _stamp_probe_token(
        self,
        next_cursor: Optional[dict[str, Any]],
        probe_token: dict[str, Any],
    ) -> dict[str, Any]:
        cursor = dict(next_cursor or {})
        cursor.pop(BOOTSTRAP_AUTHORITY_CURSOR_KEY, None)
        cursor[CURSOR_TOKEN_KEY] = probe_token
        cursor[_PROBE_SKIP_COUNT_KEY] = 0
        return cursor

    def _carry_forward_probe_state(
        self,
        next_cursor: Optional[dict[str, Any]],
        prior_cursor: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """Carry the prior global probe token + skip count forward unchanged.

        Used for a path-scoped run, which read only a subset and so is invisible
        to the global change-probe gate: the prior token must neither advance nor
        drop. If there is no prior token (a scoped run before any full sync), the
        cursor is left tokenless so the next full sync reads (no_prior_token).
        """
        cursor = dict(next_cursor or {})
        prior = prior_cursor if isinstance(prior_cursor, dict) else {}
        if CURSOR_TOKEN_KEY in prior:
            cursor[CURSOR_TOKEN_KEY] = prior[CURSOR_TOKEN_KEY]
            cursor[_PROBE_SKIP_COUNT_KEY] = self._probe_skips(prior)
        return cursor

    def _probe_short_circuit_result(
        self,
        *,
        run_id: str,
        mode: str,
        observed_at: datetime,
        runner_started_at: datetime,
        prior_cursor: Optional[dict[str, Any]],
        probe_token: Optional[dict[str, Any]],
        reason: str,
        completed_scope: bool,
        timings_ms: dict[str, float],
        timings_started: float,
        source_health: Optional[SourceHealth] = None,
    ) -> RunResult:
        """Return a no-op RunResult for a sound changed=false verdict.

        ``source_health`` carries a degraded acquisition outcome (e.g. a failed
        fast-forward that left the checkout stale) through the short-circuit so a
        repeatedly-failing pull stays observable even when the probe skips the read.

        Emits nothing; carries the prior record-set authority forward; advances
        the liveness watermark; bumps the completed-scope watermark only when
        the caller already proved a verified completed-scope footprint; bumps
        the consecutive-skip counter; writes the sync.last_run diagnostic
        (probe_skipped=1) so liveness sees the source as draining, not stalled.
        """
        next_cursor = dict(prior_cursor or {})
        if probe_token:
            next_cursor[CURSOR_TOKEN_KEY] = probe_token
        next_cursor[_PROBE_SKIP_COUNT_KEY] = self._probe_skips(prior_cursor) + 1
        next_cursor = self._preserve_prior_record_set_authority(
            next_cursor, prior_cursor,
        )
        next_cursor = self._advance_cursor_watermark_boundary(
            next_cursor=next_cursor,
            observed_at=observed_at,
            completed_scope=completed_scope,
        )
        finished_at = datetime.now(timezone.utc)
        self._write_probe_skip_diagnostics(
            run_id=run_id,
            mode=mode,
            observed_at=observed_at,
            runner_started_at=runner_started_at,
            finished_at=finished_at,
            reason=reason,
            completed_scope=completed_scope,
        )
        timings_ms["total"] = round(
            (time.perf_counter() - timings_started) * 1000, 3,
        )
        self._ensure_zero_chunk_fact_batch_application(run_id=run_id)
        fact_batch_summary = None
        if self._fact_batch_application is not None:
            completion = (
                "snapshot"
                if mode in {"reconcile", "scope_complete", "full_rebuild"}
                else "delta"
            )
            fact_batch_summary = self._fact_batch_application.finish_observation(
                completion=completion,
                observed_scope=self._active_sync_scope,
            ).to_dict()
        context = self._active_run_context
        return RunResult(
            run_id=run_id,
            source_name=self.source_name,
            # This value is runner-owned (the adapter was not invoked) and
            # lets admission distinguish a proven no-op from an adapter that
            # merely returned zero facts in a completed-scope mode.
            mode="no_change",
            started_at=runner_started_at,
            finished_at=finished_at,
            nodes_appended=0,
            edges_appended=0,
            retractions_node=0,
            retractions_edge=0,
            next_cursor=next_cursor,
            completed_scope=completed_scope,
            timings_ms=timings_ms,
            trace_id=child_span_identity().trace_id,
            source_health=source_health,
            admission_id=(context.admission_id if context is not None else None),
            idempotency_key=(
                context.idempotency_key if context is not None else None
            ),
            fact_batch_summary=fact_batch_summary,
        )

    def _write_probe_skip_diagnostics(
        self,
        *,
        run_id: str,
        mode: str,
        observed_at: datetime,
        runner_started_at: datetime,
        finished_at: datetime,
        reason: str,
        completed_scope: bool,
    ) -> None:
        if self.deployment_name is None:
            return
        try:
            payload = json.dumps(
                {
                    "schema": "okg.source_runner.last_run.v1",
                    "run_id": run_id,
                    "source_name": self.source_name,
                    "mode": mode,
                    "observed_at": observed_at.isoformat(),
                    "admission_observed_at": observed_at.isoformat(),
                    "started_at": runner_started_at.isoformat(),
                    "finished_at": finished_at.isoformat(),
                    "source_runner_ms": round(
                        (finished_at - runner_started_at).total_seconds() * 1000.0,
                        3,
                    ),
                    "nodes_appended": 0,
                    "edges_appended": 0,
                    "nodes_retracted": 0,
                    "edges_retracted": 0,
                    "dedupe_drops_node": 0,
                    "dedupe_drops_edge": 0,
                    "completed_scope": completed_scope,
                    "probe_skipped": 1,
                    "reader_invoked": 0,
                    "probe_reason": reason,
                },
                sort_keys=True,
            )
            self._upsert_progress({
                "sync.last_run": payload,
                f"sync.run.{run_id}": payload,
            })
        except Exception:  # noqa: BLE001 — diagnostic progress is best effort
            log.exception(
                "SourceRunner: failed to upsert probe-skip diagnostics for "
                "source=%s",
                self.source_name,
            )


# Edge_id minting — matches the extractor's convention in
# `projector._mint_edge_id`. Two adapters producing the same
# (src, edge_type, dst) get the same edge_id, which is the
# desired dedupe behavior at the edges_live layer.
def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(
        uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}",
    ).int
    return h & ((1 << 63) - 1)
