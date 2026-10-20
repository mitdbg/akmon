"""Source adapter protocol + shared fact types (M5b.6.1).

A source adapter produces a stream of facts per run. The
substrate runner consumes the stream and writes `okg.node_facts`
/ `okg.edge_facts`. Adapters do NOT talk to Postgres; they
produce plain Python dicts/dataclasses.

Run model — every adapter honors:

    run(run_id: str, *, mode: RunMode = CURSOR) -> SourceRun

where `SourceRun` is an iterable of (NodeFact|EdgeFact) plus the
run's completion disposition (completed scope walk vs cursor
tail vs aborted mid-fetch). The `mode` determines whether the
run is authorized to emit retractions — see
`okg.substrate.policy.retraction`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Protocol


__all__ = [
    "APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT",
    "APPLIED_TOKEN_BINDING_UNVERIFIED",
    "APPLIED_TOKEN_BINDINGS",
    "BOOTSTRAP_SNAPSHOT_MATERIALIZATION",
    "RUNNER_RESERVED_RECORD_KEY_NAMESPACES",
    "SCOPE_INDEPENDENT_RECORD_KEY_PATTERN",
    "record_key_is_runner_reserved",
    "record_key_is_scope_independent",
    "BootstrapIdentityClaim",
    "EdgeFact",
    "EdgeKey",
    "NodeFact",
    "NODE_OWNERSHIP_MODES",
    "ProgressMarker",
    "RecordEmission",
    "RecordAuthorityOperation",
    "RecordAuthorityStream",
    "RecordNode",
    "RecordSet",
    "cursor_has_record_set_authority",
    "cursor_record_set_summary",
    "cursor_without_inline_record_set",
    "preserve_cursor_record_set_authority",
    "source_state_without_runner_keys",
    "SourceAdapter",
    "SourceHealth",
    "SourcePreflightResult",
    "SourceRun",
    "available_adapters",
]


NODE_OWNERSHIP_MODES = frozenset({"authoritative", "substitute"})
BOOTSTRAP_SNAPSHOT_MATERIALIZATION = "sealed_eager_consumed_slices.v1"
#: How a ``SourceRun.applied_raw_token`` is bound to what the adapter read.
#: See the field comment on ``SourceRun.applied_token_binding``.
APPLIED_TOKEN_BINDING_UNVERIFIED = "unverified"
APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT = "sealed_snapshot"
APPLIED_TOKEN_BINDINGS = frozenset({
    APPLIED_TOKEN_BINDING_UNVERIFIED,
    APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT,
})


def _require_node_ownership_mode(value: str) -> None:
    if value not in NODE_OWNERSHIP_MODES:
        raise ValueError(
            "node ownership_mode must be one of "
            f"{sorted(NODE_OWNERSHIP_MODES)}; got {value!r}"
        )


@dataclass(frozen=True)
class NodeFact:
    """Shape the adapter yields; runner converts to node_facts row."""

    node_id: str
    subtype: str
    attrs: dict[str, Any]
    source_record_id: dict[str, Any]
    source_revision: dict[str, Any]
    op: str = "I"  # 'I' or 'R'
    ownership_mode: str = "authoritative"

    def __post_init__(self) -> None:
        _require_node_ownership_mode(self.ownership_mode)


@dataclass(frozen=True)
class EdgeFact:
    """Shape for edge_facts row."""

    src: str
    dst: str
    edge_type: str
    attrs: dict[str, Any] = field(default_factory=dict)
    provenance: str = "explicit"
    # An INTEGER 1, and the reason is now historical. The conformance kit
    # used to digest facts with a serializer that refused floating point, so
    # a float default made every default-constructed edge fail `determinism`
    # and `checkpoint_resume` (issue #1922). #1970 moved the kit onto the
    # publish path's own encoder, which accepts any finite float, so `1.0`
    # would be safe again: the publisher coerces confidence to float either
    # way, and `1` and `1.0` digest to identical bytes. The value is left at
    # `1` because changing a shipped default buys nothing now. `int` is a
    # valid `float` under the numeric tower, so a real float still works.
    confidence: float = 1
    source_record_id: dict[str, Any] = field(default_factory=dict)
    source_revision: dict[str, Any] = field(default_factory=dict)
    op: str = "I"


@dataclass(frozen=True)
class ProgressMarker:
    """Per-record commit boundary in a source's fact stream.

    A `ProgressMarker` is the source's claim that all facts
    it intends to emit for `record_id` have been yielded by
    preceding `yield` statements. The substrate's
    `SourceRunner` commits the fingerprint into
    `okg.source_progress` (key `record:<record_id>`,
    value `<fingerprint>`) atomically with the chunk's facts
    that include those preceding emissions — so a `kill -9`
    at any point loses at most the in-flight chunk.

    Per `add-substrate-incremental-publish` (2026-04-28).
    Sources that don't care about per-record progress
    simply never yield a marker; the runner falls back to
    end-of-run `progress_after_run()` upsert (legacy path).
    """

    record_id: str
    fingerprint: str


@dataclass(frozen=True)
class RecordNode:
    """Node identity captured in a source-declared record set.

    Generic retraction synthesis needs the subtype in addition to
    the node id because `okg.node_facts.subtype` is non-null even
    for `op='R'` facts. Source/revision payloads are optional; the
    runner fills deterministic record-set provenance when omitted.
    """

    node_id: str
    subtype: str
    source_record_id: dict[str, Any] = field(default_factory=dict)
    source_revision: dict[str, Any] = field(default_factory=dict)
    ownership_mode: str = "authoritative"

    def __post_init__(self) -> None:
        _require_node_ownership_mode(self.ownership_mode)

    def to_cursor(self) -> dict[str, Any]:
        payload = {
            "node_id": self.node_id,
            "subtype": self.subtype,
            "source_record_id": self.source_record_id,
            "source_revision": self.source_revision,
        }
        if self.ownership_mode != "authoritative":
            payload["ownership_mode"] = self.ownership_mode
        return payload

    @classmethod
    def from_cursor(cls, raw: Any) -> "RecordNode":
        if not isinstance(raw, dict):
            raise ValueError("record-set node must be a dict")
        node_id = raw.get("node_id")
        subtype = raw.get("subtype")
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("record-set node requires node_id")
        if not isinstance(subtype, str) or not subtype:
            raise ValueError("record-set node requires subtype")
        source_record_id = raw.get("source_record_id") or {}
        source_revision = raw.get("source_revision") or {}
        if not isinstance(source_record_id, dict):
            raise ValueError("record-set node source_record_id must be a dict")
        if not isinstance(source_revision, dict):
            raise ValueError("record-set node source_revision must be a dict")
        return cls(
            node_id=node_id,
            subtype=subtype,
            source_record_id=source_record_id,
            source_revision=source_revision,
            ownership_mode=raw.get("ownership_mode", "authoritative"),
        )


@dataclass(frozen=True)
class EdgeKey:
    """Edge identity captured in a source-declared record set."""

    src: str
    dst: str
    edge_type: str
    attrs: dict[str, Any] = field(default_factory=dict)
    provenance: str = "explicit"
    confidence: float = 1.0
    source_record_id: dict[str, Any] = field(default_factory=dict)
    source_revision: dict[str, Any] = field(default_factory=dict)

    def identity(self) -> tuple[str, str, str]:
        return (self.src, self.edge_type, self.dst)

    def to_cursor(self) -> dict[str, Any]:
        return {
            "src": self.src,
            "dst": self.dst,
            "edge_type": self.edge_type,
            "attrs": self.attrs,
            "provenance": self.provenance,
            "confidence": self.confidence,
            "source_record_id": self.source_record_id,
            "source_revision": self.source_revision,
        }

    @classmethod
    def from_cursor(cls, raw: Any) -> "EdgeKey":
        if not isinstance(raw, dict):
            raise ValueError("record-set edge must be a dict")
        src = raw.get("src")
        dst = raw.get("dst")
        edge_type = raw.get("edge_type")
        if not isinstance(src, str) or not src:
            raise ValueError("record-set edge requires src")
        if not isinstance(dst, str) or not dst:
            raise ValueError("record-set edge requires dst")
        if not isinstance(edge_type, str) or not edge_type:
            raise ValueError("record-set edge requires edge_type")
        attrs = raw.get("attrs") or {}
        source_record_id = raw.get("source_record_id") or {}
        source_revision = raw.get("source_revision") or {}
        if not isinstance(attrs, dict):
            raise ValueError("record-set edge attrs must be a dict")
        if not isinstance(source_record_id, dict):
            raise ValueError("record-set edge source_record_id must be a dict")
        if not isinstance(source_revision, dict):
            raise ValueError("record-set edge source_revision must be a dict")
        return cls(
            src=src,
            dst=dst,
            edge_type=edge_type,
            attrs=attrs,
            provenance=str(raw.get("provenance") or "explicit"),
            confidence=float(raw.get("confidence", 1.0)),
            source_record_id=source_record_id,
            source_revision=source_revision,
        )


@dataclass(frozen=True)
class RecordEmission:
    """One logical record's current graph footprint.

    `primary` is the main node for the logical record, if the
    source owns one. `descendants` are nodes owned under that
    record, such as document chunks, proposal tasks, or comments.
    `edges` are explicit edge identities the source emitted for the
    record and wants the runner to retract if they disappear.
    """

    primary: Optional[RecordNode] = None
    descendants: list[RecordNode] = field(default_factory=list)
    edges: list[EdgeKey] = field(default_factory=list)

    def nodes_by_id(self) -> dict[str, RecordNode]:
        nodes: dict[str, RecordNode] = {}
        if self.primary is not None:
            nodes[self.primary.node_id] = self.primary
        for node in self.descendants:
            nodes[node.node_id] = node
        return nodes

    def edges_by_identity(self) -> dict[tuple[str, str, str], EdgeKey]:
        return {edge.identity(): edge for edge in self.edges}

    def to_cursor(self) -> dict[str, Any]:
        return {
            "primary": (
                self.primary.to_cursor()
                if self.primary is not None else None
            ),
            "descendants": [
                node.to_cursor() for node in self.descendants
            ],
            "edges": [edge.to_cursor() for edge in self.edges],
        }

    @classmethod
    def from_cursor(cls, raw: Any) -> "RecordEmission":
        if not isinstance(raw, dict):
            raise ValueError("record emission must be a dict")
        primary_raw = raw.get("primary")
        if primary_raw is None and raw.get("primary_node_id"):
            primary_raw = {
                "node_id": raw.get("primary_node_id"),
                "subtype": raw.get("primary_subtype"),
                "source_record_id": raw.get("primary_source_record_id") or {},
                "source_revision": raw.get("primary_source_revision") or {},
            }
        primary = (
            RecordNode.from_cursor(primary_raw)
            if primary_raw is not None else None
        )
        descendants = [
            RecordNode.from_cursor(item)
            for item in raw.get("descendants") or []
        ]
        edges = [
            EdgeKey.from_cursor(item)
            for item in raw.get("edges") or []
        ]
        return cls(primary=primary, descendants=descendants, edges=edges)


@dataclass(frozen=True)
class RecordSet:
    """Full logical-record footprint for a source run."""

    records: dict[str, RecordEmission] = field(default_factory=dict)

    def to_cursor(self) -> dict[str, Any]:
        return {
            "records": {
                key: emission.to_cursor()
                for key, emission in sorted(self.records.items())
            }
        }

    @classmethod
    def from_cursor(cls, raw: Any) -> "RecordSet":
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError("record_set cursor must be a dict")
        records_raw = raw.get("records", raw)
        if not isinstance(records_raw, dict):
            raise ValueError("record_set.records must be a dict")
        return cls(records={
            str(key): RecordEmission.from_cursor(value)
            for key, value in records_raw.items()
        })


#: Issue #2145 review B1. A narrowed run declares the prior keys it replaces
#: through ``record_set_replacement_keys``, and the runner refuses a narrowed
#: run whose declared keys reach outside the sync_scope it was handed: a
#: declared key the caller never asked about is whole-source deletion
#: authority arriving through a slice.
#:
#: One exception is legitimate. Some adapters own SYNTHETIC records that
#: belong to no path at all, so they lie under no scope key by construction --
#: git_graph_files keys its branch record ``__branch__:<branch>``. Such a key
#: is recognised by its SPELLING, which the runner classifies for itself: a
#: double-underscore-delimited namespace token, optionally followed by ``:``
#: and a value. The allowance is deliberately never an adapter declaration,
#: because an adapter that could declare its own keys scope-independent would
#: reopen the hole this check closes.
#:
#: Real record keys are paths or record ids and do not match: ``__init__.py``
#: fails because ``.py`` is not ``:`` or the end of the key.
SCOPE_INDEPENDENT_RECORD_KEY_PATTERN = re.compile(
    r"^__[A-Za-z0-9_]+__(?::.*)?$"
)


#: Issue #2838. Namespaces the RUNNER mints for records it holds on a
#: source's behalf: ``__chronos_retained__`` for live-graph rows adopted by
#: the retained-graph bootstrap, and ``__stream__`` for a streamed footprint.
#: No adapter emits either, and their fingerprints are deterministic, so an
#: adapter could compute one. A key in these namespaces is therefore never
#: scope-independent: a narrowed run may not declare one as a replacement
#: key, and a narrowed merged-prior claim must carry each one forward
#: unchanged. Otherwise a slice could retract records the source adopted from
#: the live graph, outside any scope it was handed.
RUNNER_RESERVED_RECORD_KEY_NAMESPACES = frozenset({
    "__chronos_retained__",
    "__stream__",
})


def record_key_is_runner_reserved(key: str) -> bool:
    """Report whether `key` lies in a namespace only the runner may mint."""
    return key.split(":", 1)[0] in RUNNER_RESERVED_RECORD_KEY_NAMESPACES


def record_key_is_scope_independent(key: str) -> bool:
    """Report whether `key` is a synthetic record key bound to no scope.

    Runner-reserved keys (issue #2838) match the spelling but are excluded.
    """
    return bool(
        SCOPE_INDEPENDENT_RECORD_KEY_PATTERN.match(key)
    ) and not record_key_is_runner_reserved(key)


RECORD_SET_CURSOR_KEYS = frozenset({
    "record_set",
    "record_set_ref",
    "record_set_summary",
    "record_set_chronos_proof",
})


def cursor_has_record_set_authority(cursor: Any) -> bool:
    """Return true when the cursor carries inline or external record-set authority."""
    return (
        isinstance(cursor, Mapping)
        and (
            cursor.get("record_set") is not None
            or cursor.get("record_set_ref") is not None
        )
    )


def cursor_record_set_summary(cursor: Any) -> dict[str, Any]:
    if not isinstance(cursor, Mapping):
        return {}
    raw = cursor.get("record_set_summary")
    return dict(raw) if isinstance(raw, Mapping) else {}


def cursor_without_inline_record_set(cursor: Any) -> dict[str, Any]:
    if not isinstance(cursor, Mapping):
        return {}
    return {str(key): value for key, value in cursor.items() if key != "record_set"}


def source_state_without_runner_keys(cursor: Any) -> dict[str, Any]:
    if not isinstance(cursor, Mapping):
        return {}
    return {
        str(key): value
        for key, value in cursor.items()
        if key not in RECORD_SET_CURSOR_KEYS
    }


def preserve_cursor_record_set_authority(
    next_cursor: Mapping[str, Any] | None,
    prior_cursor: Any,
) -> dict[str, Any]:
    """Copy prior record-set authority into a source-owned next cursor.

    Source adapters can safely use this for no-change runs where the
    watermark/content hash proves the prior full-source footprint is still
    authoritative. External record sets remain by reference, so large sources
    do not have to materialize the blob just to report a no-op.
    """
    cursor = dict(next_cursor or {})
    if not isinstance(prior_cursor, Mapping):
        return cursor
    if "record_set" not in cursor and "record_set_ref" not in cursor:
        if prior_cursor.get("record_set_ref") is not None:
            cursor["record_set_ref"] = prior_cursor["record_set_ref"]
        elif prior_cursor.get("record_set") is not None:
            cursor["record_set"] = prior_cursor["record_set"]
    for key in ("record_set_summary", "record_set_chronos_proof"):
        if key not in cursor and key in prior_cursor:
            cursor[key] = prior_cursor[key]
    return cursor


PREFLIGHT_STATUSES = frozenset({
    "ok",
    "skipped_optional",
    "missing_credential",
    "auth_failed",
    "tls_failed",
    "endpoint_failed",
    "cache_missing",
    "not_applicable",
    # Acquisition outcomes for sources that manage their own checkout
    # (``ensure_checkout``): the checkout could not be fast-forwarded so the
    # existing tree was indexed (``acquire_stale``), or it could not be
    # materialised at all so zero facts were emitted (``acquire_failed``).
    "acquire_stale",
    "acquire_failed",
})

#: Run-completion status for a source run that produced usable facts but did
#: not observe its whole scope (a truncated crawl, a page-limited API sweep, a
#: cancelled walk). It is deliberately NOT a member of ``PREFLIGHT_STATUSES``:
#: a preflight answers "can this source run at all?", which has no partial
#: answer, and reusing the frozenset would let a preflight report it.
#: ``SourceRunner`` needs no change to honor it — deletion authority is already
#: withheld from every status outside ``{ok, not_applicable, skipped_optional}``
#: (``ingest/runner.py``, ``_completed_scope_health_is_acceptable``), so this
#: adds a truthful label rather than a new refusal. Per issue #1284.
SOURCE_RUN_PARTIAL_STATUS = "partial"

#: Statuses a ``SourceHealth`` may carry: the preflight vocabulary plus the
#: run-only partial status.
SOURCE_HEALTH_STATUSES = PREFLIGHT_STATUSES | {SOURCE_RUN_PARTIAL_STATUS}


@dataclass(frozen=True)
class SourceHealth:
    """Compact, redacted source health metadata.

    Values in ``credential_refs`` and ``alias_refs`` are environment
    variable names such as ``EXAMPLE_API_TOKEN`` rather than secret
    values. ``endpoint`` should already be redacted by callers when it
    may contain query-string secrets.
    """

    status: str = "not_applicable"
    mode: str = "live"  # 'cache' | 'fixture' | 'live' | 'mixed' | ...
    credential_refs: tuple[str, ...] = ()
    alias_refs: dict[str, tuple[str, ...]] = field(default_factory=dict)
    record_count: Optional[int] = None
    cache_path: Optional[str] = None
    content_hash: Optional[str] = None
    endpoint: Optional[str] = None
    reason: Optional[str] = None
    checked_at: Optional[str] = None

    def __post_init__(self) -> None:
        if self.status not in SOURCE_HEALTH_STATUSES:
            raise ValueError(
                f"unknown source health status {self.status!r}; "
                f"valid statuses: {sorted(SOURCE_HEALTH_STATUSES)}"
            )

    def as_dict(self) -> dict[str, Any]:
        from okg.substrate.sources.redaction import (
            redact_endpoint,
            redact_text,
        )

        out: dict[str, Any] = {
            "status": self.status,
            "mode": self.mode,
            "credential_refs": list(self.credential_refs),
            "alias_refs": {
                k: list(v) for k, v in self.alias_refs.items()
            },
        }
        if self.record_count is not None:
            out["record_count"] = self.record_count
        if self.cache_path is not None:
            out["cache_path"] = redact_text(self.cache_path)
        if self.content_hash is not None:
            out["content_hash"] = self.content_hash
        if self.endpoint is not None:
            out["endpoint"] = redact_endpoint(self.endpoint)
        if self.reason is not None:
            out["reason"] = redact_text(self.reason)
        if self.checked_at is not None:
            out["checked_at"] = self.checked_at
        return out


def acquire_outcome_health(outcome: Any) -> Optional[SourceHealth]:
    """Map a ``_git_helpers.clone_or_pull`` outcome to source health.

    Returns None for a clean acquisition (``cloned``/``updated``) so the source's
    own read health is used; ``stale`` -> ``acquire_stale`` (existing tree was
    indexed); anything else -> ``acquire_failed`` (checkout unavailable).
    """
    status = getattr(outcome, "status", None)
    if status in ("cloned", "updated"):
        return None
    if status == "stale":
        return SourceHealth(status="acquire_stale", reason=getattr(outcome, "reason", None))
    return SourceHealth(status="acquire_failed", reason=getattr(outcome, "reason", None))


@dataclass(frozen=True)
class SourcePreflightResult(SourceHealth):
    """Result of an optional source auth/cache preflight check."""

    source_name: str = ""
    required: bool = False

    def __post_init__(self) -> None:
        # Narrower than SourceHealth: a preflight answers whether the source
        # can run, so the run-only partial status is not a legal answer here.
        if self.status not in PREFLIGHT_STATUSES:
            raise ValueError(
                f"unknown source preflight status {self.status!r}; "
                f"valid statuses: {sorted(PREFLIGHT_STATUSES)}"
            )

    @classmethod
    def not_applicable(
        cls,
        source_name: str,
        *,
        required: bool = False,
        reason: str = "source does not implement preflight",
    ) -> "SourcePreflightResult":
        return cls(
            source_name=source_name,
            status="not_applicable",
            required=required,
            reason=reason,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )

    def as_dict(self) -> dict[str, Any]:
        out = super().as_dict()
        out["source_name"] = self.source_name
        out["required"] = self.required
        return out


@dataclass(frozen=True)
class BootstrapIdentityClaim:
    """Explicit authority request for persisting one bounded source slice.

    The raw token is exact for the bytes consumed to produce the eagerly
    materialized facts, rather than for an ambient whole-source snapshot.
    ``input_cursor`` is the exact frontier the adapter consumed.  The claim is
    *not* graph-completion authority: it cannot authorize deletion, completed
    scope, or a probe short-circuit. ``cursor_semantics`` names the adapter's
    concrete resume-state contract; ``snapshot_materialization`` must use the
    runner-recognized sealed form.
    """

    raw_token: Mapping[str, Any]
    scope: str
    cursor_semantics: str
    snapshot_materialization: str
    input_cursor: Mapping[str, Any]


@dataclass(frozen=True)
class RecordAuthorityOperation:
    """One ordered v2 authority operation without RecordEmission materialization."""

    ordinal: int
    record_id: str
    primary: Mapping[str, Any] | None = None
    descendants: Iterable[Mapping[str, Any]] = ()
    edges: Iterable[Mapping[str, Any]] = ()
    retract: bool = False


@dataclass(frozen=True)
class RecordAuthorityStream:
    """Private, expected-value-bound stream for an indexed-v2 candidate."""

    operations: Iterable[RecordAuthorityOperation]
    logical_sha256: str
    record_count: int
    operation_count: int
    component_count: int


@dataclass
class SourceRun:
    """Result of one adapter run. `facts` is the stream;
    `completed_scope` tells the runner whether this was a
    full-scope walk (retractions can be computed from absence).

    `record_set`, when present, normally declares the complete set
    of logical records the source owns at the end of this run. When
    `record_set_replacement_keys` is set, `record_set` instead
    declares only the changed/deleted record slice; SourceRunner
    merges that slice into the prior inline or external record-set
    authority before computing retractions and persisting the cursor.

    `record_set=None` on its own says only that this run declared no
    record set. For a `missing_from_completed_scope` source the runner
    reads that as "infer the whole footprint from the fact stream", which
    is right for a streaming-authority run and wrong for a no-change
    short-circuit whose stream is empty by construction. A no-change run
    states the difference with `preserves_prior_record_set=True`. An EMPTY
    stream infers no footprint at all: the runner keeps the prior state
    unless the reader sets `asserts_empty_scope=True`.
    """

    # NodeFact | EdgeFact | ProgressMarker
    facts: Iterable[Any]
    completed_scope: bool = False
    # Opaque per-adapter state to persist in
    # source_watermarks.last_applied_rev.
    next_cursor: Optional[dict[str, Any]] = None
    # Exact identity of the bytes/rows/objects consumed to produce `facts`.
    # A pre-read probe token is not sufficient: adapters opt into
    # short-circuit acknowledgement only by returning an exact applied token.
    applied_raw_token: Optional[dict[str, Any]] = None
    applied_token_scope: str = "unavailable"  # global | scoped | unavailable
    applied_token_authority: str = "unavailable"  # exact | unavailable
    # How `applied_raw_token` is bound to what the adapter read. One of
    # APPLIED_TOKEN_BINDINGS:
    #   - "unverified" (default): the runner accepts the applied token only
    #     when the post-run probe returns the same token.
    #   - "sealed_snapshot": the adapter promises that the applied token was
    #     computed from exactly the bytes/objects it read, taken from a
    #     snapshot captured BEFORE reading (copied bytes, pinned ref tips), so
    #     the token stays true for the facts emitted even if the live input
    #     moves while the run is in flight. The runner may then acknowledge the
    #     applied token when the post-run probe differs; the next poll sees
    #     the moved input as a change. Declare it only when that promise holds.
    applied_token_binding: str = "unverified"
    # Explicit non-terminal authority for a bounded run whose facts were
    # detached from one sealed source snapshot before this object returned.
    # The runner validates this independently and never treats it as an exact
    # applied token.
    bootstrap_identity: Optional[BootstrapIdentityClaim] = None
    run_mode: str = "cursor"  # 'cursor' | 'reconcile' | ...
    record_set: Optional[RecordSet] = None
    record_authority_stream: Optional[RecordAuthorityStream] = None
    # Every key here must lie inside the sync_scope the caller handed this
    # run, or be scope-independent by the spelling rule in
    # SCOPE_INDEPENDENT_RECORD_KEY_PATTERN above. The runner refuses a
    # narrowed run that declares anything else (issue #2145 review B1), and
    # any runner-reserved key (RUNNER_RESERVED_RECORD_KEY_NAMESPACES, #2838).
    record_set_replacement_keys: Optional[Iterable[str]] = None
    # Set only when the adapter was handed a narrow path scope and
    # deliberately read the COMPLETE configured scope anyway, so `record_set`
    # is a whole-source footprint rather than a slice. An adapter must widen
    # like this when a record's emitted identities depend on content outside
    # the requested paths -- otherwise the footprint it declares is unsound.
    # The runner treats a run flagged here as unscoped for deletion authority
    # (and re-authorizes its ingest boundaries at that broader authority), so
    # never set it on a run that actually stayed inside the requested scope.
    effective_scope_complete: bool = False
    # Set this when, and ONLY when, the run emits NO facts because the prior
    # record-set authority is still exact. That is the whole rule, and it is
    # narrower than it sounds. `record_set=None` on a completed scope is also
    # how a streaming-authority reader says "my fact stream IS the
    # footprint", and for such a run the runner must keep inferring a
    # footprint and retracting whatever the stream no longer names. What
    # separates the two is the STREAM, not how much the reader read:
    #   - empty stream + prior authority still exact -> set this flag;
    #   - complete re-emission of the footprint -> do NOT set it, and nothing
    #     is needed, because the inferred footprint equals the true one;
    #   - partial stream -> never set it. The runner refuses such a run
    #     (reason_code `preserved_record_set_authority_stream_not_empty`)
    #     rather than write partial facts on top of a preserved footprint.
    # The prior authority must be carried forward in `next_cursor` (see
    # `preserve_cursor_record_set_authority`); the runner refuses a run that
    # claims this and drops it. Inferring a footprint from an empty stream is
    # what read a no-change run as an emptied source and retracted everything
    # the source had published (issue #2122).
    preserves_prior_record_set: bool = False
    # Set this when, and ONLY when, the reader walked its whole configured
    # scope, found it genuinely empty, and wants the runner to retract every
    # record the source published before. Operator decision on #1859 (rule
    # (b), 2026-09-22): a completed run that declares no record set and
    # streams NO positive fact keeps the prior state, because an empty
    # stream is far more often a reader that silently read nothing than a
    # source that was emptied. This flag is the only way to say "empty on
    # purpose". No reader sets it by default. It needs `completed_scope=True`
    # and no record-set authority of its own. The runner retracts the prior
    # footprint itself, so the run streams nothing: the runner refuses a run
    # that sets it and still streams any fact, retraction included. On a run
    # with no whole-scope deletion authority (an incremental or narrowed
    # run) the runner logs the assertion and reports it ignored.
    asserts_empty_scope: bool = False
    health: Optional[SourceHealth] = None
    # The directory this run's file image values (`{"okg_image": "file",
    # "path": ...}`, #3046) resolve against. The runner's image step opens
    # files only under it; with no root, every file value is completed with
    # the error `image_root_undeclared`. It is never written to a fact or to
    # the graph -- only the relative path in the value is.
    image_root: Optional[str] = None

    def __post_init__(self) -> None:
        if self.applied_token_binding not in APPLIED_TOKEN_BINDINGS:
            raise ValueError(
                "applied_token_binding must be one of "
                f"{sorted(APPLIED_TOKEN_BINDINGS)!r}, got "
                f"{self.applied_token_binding!r}"
            )
        partial = (
            self.health is not None
            and self.health.status == SOURCE_RUN_PARTIAL_STATUS
        )
        if partial and self.completed_scope:
            raise ValueError(
                "a source run reporting health status "
                f"{SOURCE_RUN_PARTIAL_STATUS!r} did not observe its whole "
                "scope, so it cannot also set completed_scope=True; "
                "absence in a partial run is not evidence of deletion"
            )
        if self.preserves_prior_record_set and (
            self.record_set is not None
            or self.record_set_replacement_keys is not None
            or self.record_authority_stream is not None
        ):
            raise ValueError(
                "a source run that preserves the prior record-set authority "
                "declares no authority of its own, so it cannot also carry a "
                "record_set, a record_set_replacement_keys slice, or a "
                "record_authority_stream"
            )
        if self.preserves_prior_record_set and not self.completed_scope:
            raise ValueError(
                "a source run cannot preserve the prior whole-source "
                "record-set authority without reporting completed_scope=True; "
                "a partial read is no proof the prior footprint is current"
            )
        if self.asserts_empty_scope and not self.completed_scope:
            raise ValueError(
                "a source run cannot assert its scope is empty without "
                "reporting completed_scope=True; a partial read is no proof "
                "the source was emptied"
            )
        if self.asserts_empty_scope and self.preserves_prior_record_set:
            raise ValueError(
                "a source run cannot both assert an empty scope and preserve "
                "the prior record-set authority; the two claims contradict "
                "each other"
            )
        if self.asserts_empty_scope and (
            self.record_set is not None
            or self.record_set_replacement_keys is not None
            or self.record_authority_stream is not None
        ):
            raise ValueError(
                "a source run that asserts an empty scope declares its "
                "footprint through the empty fact stream, so it cannot also "
                "carry a record_set, a record_set_replacement_keys slice, or "
                "a record_authority_stream; an explicit empty record_set "
                "already says the same thing"
            )


class SourceAdapter(Protocol):
    """Every library adapter honors this contract.

    Deployment authors construct an adapter with source-specific
    kwargs (auth, fixture_path, etc.) and hand it to the
    substrate runner. The runner calls `.name`, `.profile`, and
    `.run()`.

    Sources whose underlying records can disappear should return a
    `SourceRun(record_set=RecordSet(...))` during full-scope or
    reconcile runs. The runner loads the prior cursor's record set,
    diffs it against the new one, and emits deterministic
    retractions for disappeared records. Append-only sources can
    leave `record_set=None`.
    """

    name: str
    profile: str  # one of the 5 source profiles
                  # ('reference_catalog' | 'mutable_api' |
                  #  'discovery_crawl' | 'append_feed' |
                  #  'live_overlay')

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        sync_scope: Optional[Mapping[str, Any]] = None,
    ) -> SourceRun:
        raise NotImplementedError

    # Optional. Callers use getattr(adapter, "preflight", None) so
    # existing adapters do not need to implement this method.


def available_adapters() -> list[str]:
    """Return the list of adapter names shipped in this package.

    Used by `okg source list`."""
    import pkgutil
    import okg.substrate.library.sources as pkg
    return sorted(
        m.name for m in pkgutil.iter_modules(pkg.__path__)
        if not m.name.startswith("_") and m.name != "base"
    )
