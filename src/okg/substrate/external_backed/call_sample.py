"""Per-relation call-sample policy: one sample per node-targeted operator call.

Issue #2011, PACT ``call-sample-policy``, Sprint 20 charter #2008.

WHAT THIS MODULE IS
-------------------
Every configured operator call that names a target node leaves exactly one
sample, stored under the policy its relation declares. An undeclared relation
uses ``none`` and writes nothing. This module owns three things and nothing
else:

* the **policy model** -- ``RelationSamplePolicy`` and ``RelationPolicyRegistry``;
* the **writers** -- ``record_call_sample``, ``record_tool_call_sample`` and
  ``record_live_relation_call_sample``;
* the **windowed reader** -- ``read_sample_window`` -- and the live call
  count, ``live_call_counts``.

Live-relation dispatch and Nomos gating live in the sibling PACT
``live-relations-on-expand`` (issue #2013). This module provides what that
dispatcher calls; it never decides whether a call is allowed and never invokes
an adapter. Per-deployment policy loading is likewise follow-up integration,
not part of this storage foundation.

THE FOUR POLICIES
-----------------
``none``      write nothing. The default for graph reads.
``metadata``  write the identity and outcome of the call; no payload bytes.
``inline``    write the payload when it serializes to at most
              ``INLINE_MAX_BYTES`` (16 KiB); above the cap it DEGRADES to
              ``digest``. The degradation is recorded, not hidden: the row
              keeps ``declared_policy='inline'`` beside ``policy='digest'``.
``digest``    write a sha256 of the payload, a bounded summary, and a re-fetch
              pointer holding the query and the source.

A call with **no target node** -- ``search`` is the canonical case -- writes no
sample at all. It stays telemetry-only in ``okg.mcp_trace_events`` and
``okg.telemetry_spans``.

RETENTION CLASSES
-----------------
Three named classes, one hypertable each, with distinct retention and
compression policies (schema.sql): ``hot`` (7 days), ``warm`` (90 days),
``cold`` (730 days). A relation declares its class. ``read_sample_window``
returns ``data``, ``aged_out`` or ``no_data`` per class and consults
``okg.call_sample_watermark`` before answering; an empty window is never
coerced to ``no_data`` when the watermark shows samples existed.

THE IDENTITY CONTRACT -- READ THIS BEFORE WIRING A LIVE PATH
------------------------------------------------------------
Every sample carries ``principal``, ``decision``, ``trace_id`` and
``session_id``. ``SampleIdentity`` refuses a blank or missing value for any of
them, and the database columns are ``NOT NULL`` with non-blank CHECKs. There is
deliberately **no default that papers over a missing value**:

* ``graph_read_identity()`` is the ONLY constructor that supplies defaults. It
  is for the existing graph-read paths, where the principal is the runtime
  principal and the authorization decision is ``allow`` because the read
  already passed the graph's own gate. It stamps
  ``attrs['decision_source'] = 'graph_read_default'`` so a reader can always
  tell a defaulted decision from a gated one.
* ``live_identity()`` is for a live relation. It takes all four values and
  raises ``CallSampleError`` if any is missing. A live caller that does not yet
  know its principal or its Nomos decision must fail, not sample an ``allow``.

Consequently a sample row whose ``attrs->>'decision_source'`` is absent was
produced by a real authorization decision, and one that reads
``graph_read_default`` was not. Nothing silently in between.

WRITE-FAILURE CONTRACT
----------------------
``record_call_sample`` raises. It is the authoritative storage path;
``record_live_relation_call_sample`` validates the dispatcher's explicit
identity and calls it inside the call's own transaction, so a sample that
cannot be stored fails the call rather than vanishing.

``record_tool_call_sample`` is the convenience arm used by the MCP tracing
wrapper. It likewise raises; the tracing wrapper's own swallow-and-log policy
(tracing must never break a tool call) applies only because the default
registry gives graph reads ``none``, so that arm writes nothing today. A
relation that declares a writing policy must go through the dispatcher's
``record_call_sample`` call, not rely on the tracer.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

import psycopg

#: The four declarable policies. ``none`` is never a stored row.
SAMPLE_POLICIES = frozenset({"none", "metadata", "inline", "digest"})

#: The policies that produce a stored row.
STORED_POLICIES = frozenset({"metadata", "inline", "digest"})

#: The three named retention classes, hottest first.
RETENTION_CLASSES = ("hot", "warm", "cold")

#: Inline payload cap. Above it, inline degrades to digest (decision 5).
INLINE_MAX_BYTES = 16 * 1024

#: Bound on the stored digest summary.
SUMMARY_MAX_CHARS = 512

_SAMPLE_STATUSES = frozenset({"ok", "unavailable", "denied", "error", "timeout"})
_DECISIONS = frozenset({"allow", "deny"})

_TABLE_BY_CLASS = {
    "hot": "okg.call_sample_hot",
    "warm": "okg.call_sample_warm",
    "cold": "okg.call_sample_cold",
}

#: The ONLY tools that target a node. Everything else -- ``search``,
#: ``aggregate``, ``query``, ``filter``, ``map``, every source and admin tool --
#: targets no single node, so it samples nothing and reports nothing.
#:
#: This is an allowlist on purpose. As a denylist it would have to name every
#: tool that exists now and every tool added later, and a tool nobody
#: remembered to list would be treated as node-targeted and warn on every call
#: about an anchor it was never supposed to have.
NODE_TARGETED_TOOLS = frozenset({"inspect", "expand"})


class CallSampleError(ValueError):
    """A sample is malformed or its identity is incomplete. Never a fallback."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CallSampleError(
            f"{field_name} must be a non-empty string; got {value!r}. "
            "A sample without it cannot be attributed, and defaulting it "
            "would record a call as someone it was not."
        )
    return value.strip()


def table_for_class(retention_class: str) -> str:
    """Return the hypertable backing a retention class."""
    try:
        return _TABLE_BY_CLASS[retention_class]
    except KeyError:
        raise CallSampleError(
            f"retention_class {retention_class!r} not in {list(RETENTION_CLASSES)}"
        ) from None


@dataclass(frozen=True)
class RelationSamplePolicy:
    """What one relation samples, where the samples are kept, and who writes.

    ``refetch_query_keys`` names the call arguments whose VALUES may be copied
    into a digest sample's re-fetch pointer. It is empty by default, so a
    pointer built by ``refetch_pointer_for_call`` carries only the argument
    SHAPE. A sample can live for 730 days; copying a whole kwargs mapping in
    would persist whatever a caller happened to pass, including a credential
    or a person's name, for that long. A relation opts each key in by name.

    ``graph_read`` decides WHICH writer is allowed to sample this relation.
    Only a graph-read relation may be sampled by the MCP tracing wrapper,
    because that wrapper has no authorization decision of its own and would
    have to default one (see ``graph_read_identity``). A live relation sets it
    False, so a Nomos deny can never be recorded by the tracer as an ``allow``:
    the tracer skips the relation with a warning and the dispatcher, which
    holds the real decision, writes the only sample.
    """

    relation: str
    policy: str = "none"
    retention_class: str = "warm"
    graph_read: bool = False
    refetch_query_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.relation, "relation")
        if not isinstance(self.refetch_query_keys, tuple) or any(
            not isinstance(key, str) or not key.strip()
            for key in self.refetch_query_keys
        ):
            raise CallSampleError(
                "refetch_query_keys must be a tuple of non-empty strings"
            )
        if self.policy not in SAMPLE_POLICIES:
            raise CallSampleError(
                f"policy {self.policy!r} not in {sorted(SAMPLE_POLICIES)}"
            )
        if self.retention_class not in RETENTION_CLASSES:
            raise CallSampleError(
                f"retention_class {self.retention_class!r} not in "
                f"{list(RETENTION_CLASSES)}"
            )

    @property
    def writes(self) -> bool:
        """True when a call under this policy produces a stored row."""
        return self.policy in STORED_POLICIES

    @property
    def table(self) -> str:
        return table_for_class(self.retention_class)


def graph_read_policy(
    relation: str, *, policy: str = "none", retention_class: str = "warm"
) -> RelationSamplePolicy:
    """A graph-read relation's policy. ``none`` by default: sample nothing.

    Graph-read policies are the only ones the tracing wrapper may sample.
    """
    return RelationSamplePolicy(
        relation=relation,
        policy=policy,
        retention_class=retention_class,
        graph_read=True,
    )


def live_relation_policy(
    relation: str, *, retention_class: str = "warm"
) -> RelationSamplePolicy:
    """A live relation's policy: inline, degrading at the cap.

    ``graph_read`` is False, so the tracing wrapper will NOT sample it. The
    live dispatcher writes the sample with the principal and Nomos decision it
    actually obtained.
    """
    return RelationSamplePolicy(
        relation=relation,
        policy="inline",
        retention_class=retention_class,
        graph_read=False,
    )


class RelationPolicyRegistry:
    """Per-relation policies, with a graph-read default of ``none``.

    A relation that has not declared a policy reads back as ``none``. That is a
    deliberate default for a READ that stores nothing, not a fallback that
    hides a value: it can only ever cause less to be written, never a sample
    with an invented principal or decision.
    """

    def __init__(
        self,
        policies: Iterable[RelationSamplePolicy] = (),
        *,
        default_policy: str = "none",
        default_retention_class: str = "warm",
    ) -> None:
        if default_policy not in SAMPLE_POLICIES:
            raise CallSampleError(
                f"default_policy {default_policy!r} not in {sorted(SAMPLE_POLICIES)}"
            )
        self._default_policy = default_policy
        self._default_retention_class = default_retention_class
        self._by_relation: dict[str, RelationSamplePolicy] = {}
        for entry in policies:
            self.declare(entry)

    def declare(self, policy: RelationSamplePolicy) -> RelationSamplePolicy:
        """Register (or replace) one relation's policy."""
        if not isinstance(policy, RelationSamplePolicy):
            raise CallSampleError("declare() takes a RelationSamplePolicy")
        self._by_relation[policy.relation] = policy
        return policy

    def policy_for(self, relation: str) -> RelationSamplePolicy:
        declared = self._by_relation.get(relation)
        if declared is not None:
            return declared
        # An undeclared relation is a graph read ONLY while the default stores
        # nothing. A registry built with a writing default would otherwise hand
        # the tracing wrapper a live relation it has never seen and let it
        # stamp the graph-read `allow` on it -- the exact defect graph_read
        # exists to prevent. Tying the flag to the default keeps the two from
        # drifting apart.
        return RelationSamplePolicy(
            relation=relation,
            policy=self._default_policy,
            retention_class=self._default_retention_class,
            graph_read=self._default_policy == "none",
        )

    @property
    def declared_relations(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_relation))


_DEFAULT_REGISTRY = RelationPolicyRegistry()


def default_registry() -> RelationPolicyRegistry:
    """The process-wide registry the tracing wrapper consults.

    Empty by design: until a relation declares otherwise every operator call
    is a graph read and samples nothing.
    """
    return _DEFAULT_REGISTRY


def register_relation_sample_policy(
    policy: RelationSamplePolicy,
) -> RelationSamplePolicy:
    """Register one policy in the process-wide production registry.

    The MCP tracing wrapper consults this registry when no registry override is
    supplied. Registration is process-scoped: it affects every deployment
    served by this Python process and is intended to happen during startup,
    before calls are served. This module does not load per-deployment policy or
    define a live-reload lifecycle; that wiring belongs to issues #2012 and
    #2013.
    """
    return _DEFAULT_REGISTRY.declare(policy)


@dataclass(frozen=True)
class SampleIdentity:
    """Who made the call, what the gate decided, and under which trace."""

    principal: str
    decision: str
    trace_id: str
    session_id: str
    decision_source: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "principal", _require_text(self.principal, "principal")
        )
        object.__setattr__(self, "trace_id", _require_text(self.trace_id, "trace_id"))
        object.__setattr__(
            self, "session_id", _require_text(self.session_id, "session_id")
        )
        decision = _require_text(self.decision, "decision")
        if decision not in _DECISIONS:
            raise CallSampleError(f"decision {decision!r} not in {sorted(_DECISIONS)}")
        object.__setattr__(self, "decision", decision)


def live_identity(
    *,
    principal: Optional[str],
    decision: Optional[str],
    trace_id: Optional[str],
    session_id: Optional[str],
) -> SampleIdentity:
    """Identity for a live call. All four values are required; no defaults."""
    return SampleIdentity(
        principal=principal,  # type: ignore[arg-type]
        decision=decision,  # type: ignore[arg-type]
        trace_id=trace_id,  # type: ignore[arg-type]
        session_id=session_id,  # type: ignore[arg-type]
    )


def runtime_principal() -> str:
    """The principal the runtime itself acts as on a graph-read path."""
    for name in ("OKG_PRINCIPAL", "OKG_MCP_PRINCIPAL"):
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return "runtime"


def graph_read_identity(
    *,
    trace_id: Optional[str],
    session_id: Optional[str],
    principal: Optional[str] = None,
) -> SampleIdentity:
    """Identity for an existing graph-read path.

    The principal defaults to the runtime principal and the decision to
    ``allow`` -- the read already passed the graph's own gate. The defaulting
    is disclosed on the row via ``decision_source='graph_read_default'``.
    ``trace_id`` and ``session_id`` are still required: they exist on every
    traced call, and inventing them would break the join back to the session.
    """
    return SampleIdentity(
        principal=principal or runtime_principal(),
        decision="allow",
        trace_id=trace_id,  # type: ignore[arg-type]
        session_id=session_id,  # type: ignore[arg-type]
        decision_source="graph_read_default",
    )


@dataclass(frozen=True)
class RefetchPointer:
    """Where a digested payload can be fetched again."""

    query: Any
    source: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _require_text(self.source, "refetch.source"))
        if self.query is None:
            raise CallSampleError(
                "refetch.query is required on a digest sample; without it the "
                "digest names a payload nobody can fetch again"
            )

    def as_dict(self) -> dict[str, Any]:
        return {"query": self.query, "source": self.source}


@dataclass(frozen=True)
class StoredSample:
    """One sample row, after the policy has been applied."""

    deployment_name: str
    node_id: str
    relation: str
    retention_class: str
    observed_at: datetime
    status: str
    identity: SampleIdentity
    policy: str
    declared_policy: str
    payload: Any = None
    payload_sha256: Optional[str] = None
    payload_bytes: Optional[int] = None
    summary: Optional[str] = None
    refetch: Optional[Mapping[str, Any]] = None
    tool_name: Optional[str] = None
    attrs: Mapping[str, Any] = dataclass_field(default_factory=dict)

    @property
    def table(self) -> str:
        return table_for_class(self.retention_class)

    @property
    def degraded(self) -> bool:
        """True when an over-cap inline payload was stored as a digest."""
        return self.policy != self.declared_policy


def encode_payload(payload: Any) -> bytes:
    """Canonical JSON encoding used for both the cap and the sha256."""
    return json.dumps(payload, sort_keys=True, default=str).encode("utf-8")


def digest_of(payload: Any) -> tuple[str, int]:
    """Return ``(sha256_hex, byte_length)`` of the canonical encoding."""
    encoded = encode_payload(payload)
    return hashlib.sha256(encoded).hexdigest(), len(encoded)


def summarize(payload: Any, *, limit: int = SUMMARY_MAX_CHARS) -> str:
    """A bounded, readable stand-in for a payload that was not stored."""
    if limit <= 0:
        raise CallSampleError("summary limit must be positive")
    text = encode_payload(payload).decode("utf-8", "replace")
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def build_sample(
    *,
    deployment_name: str,
    node_id: Optional[str],
    relation: str,
    policy: RelationSamplePolicy,
    identity: SampleIdentity,
    status: str,
    payload: Any = None,
    refetch: Optional[RefetchPointer] = None,
    observed_at: Optional[datetime] = None,
    tool_name: Optional[str] = None,
    attrs: Optional[Mapping[str, Any]] = None,
    store_inline: bool = True,
) -> Optional[StoredSample]:
    """Apply a relation's policy to one call.

    Returns ``None`` when nothing is to be stored -- the policy is ``none``, or
    the call named no target node. Returns a ``StoredSample`` otherwise, with
    the inline cap already applied.

    The cap is measured HERE, by this module's own encoding, and nowhere
    else (issue #2171): it is a storage rule and says nothing about what the
    caller received. ``store_inline=False`` degrades an inline policy to
    digest whatever the size -- a live call passes it when the caller
    received only a digest, because the sample holds what the caller
    received, and storing rows the caller never saw would not be that.
    """
    if node_id is None or not str(node_id).strip():
        return None
    if not policy.writes:
        return None
    if status not in _SAMPLE_STATUSES:
        raise CallSampleError(f"status {status!r} not in {sorted(_SAMPLE_STATUSES)}")
    if policy.relation != relation:
        raise CallSampleError(
            f"policy is declared for relation {policy.relation!r}, not {relation!r}"
        )

    declared = policy.policy
    effective = declared
    sha256: Optional[str] = None
    byte_count: Optional[int] = None
    summary: Optional[str] = None
    stored_payload: Any = None
    pointer: Optional[dict[str, Any]] = None

    has_payload = payload is not None
    if has_payload:
        sha256, byte_count = digest_of(payload)

    if declared == "inline":
        if not has_payload:
            stored_payload = None
        elif (
            store_inline
            and byte_count is not None
            and byte_count <= INLINE_MAX_BYTES
        ):
            stored_payload = payload
        else:
            # Over the cap: degrade to digest. Recorded, not hidden -- the row
            # keeps declared_policy='inline'.
            effective = "digest"
    if effective == "digest":
        stored_payload = None
        if has_payload:
            summary = summarize(payload)
            if refetch is None:
                raise CallSampleError(
                    "a digest sample of a payload requires a re-fetch pointer "
                    "(query and source); storing a hash nobody can resolve is "
                    "not a sample"
                )
            pointer = refetch.as_dict()
    elif effective == "metadata":
        stored_payload = None
        sha256 = None
        byte_count = None
        summary = None
        pointer = refetch.as_dict() if refetch is not None else None

    row_attrs = dict(attrs or {})
    if identity.decision_source is not None:
        row_attrs.setdefault("decision_source", identity.decision_source)
    if effective != declared:
        row_attrs.setdefault("degraded_from", declared)
        row_attrs.setdefault("inline_max_bytes", INLINE_MAX_BYTES)
        if not store_inline:
            row_attrs.setdefault("degraded_because", "caller_received_digest")

    return StoredSample(
        deployment_name=deployment_name,
        node_id=str(node_id),
        relation=relation,
        retention_class=policy.retention_class,
        observed_at=observed_at or _now(),
        status=status,
        identity=identity,
        policy=effective,
        declared_policy=declared,
        payload=stored_payload,
        payload_sha256=sha256,
        payload_bytes=byte_count,
        summary=summary,
        refetch=pointer,
        tool_name=tool_name,
        attrs=row_attrs,
    )


def record_call_sample(conn: psycopg.Connection, sample: StoredSample) -> StoredSample:
    """Insert one sample and maintain its monotonic-min watermark.

    Raises on failure. A live dispatcher calls this inside the call's own
    transaction so an unstorable sample fails the call.
    """
    table = sample.table
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {table} "  # noqa: S608 - table name from a closed map
            "(deployment_name, node_id, relation, observed_at, status, "
            " principal, decision, trace_id, session_id, tool_name, "
            " policy, declared_policy, payload, payload_sha256, payload_bytes, "
            " summary, refetch, attrs) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,"
            "%s::jsonb,%s::jsonb)",
            (
                sample.deployment_name,
                sample.node_id,
                sample.relation,
                sample.observed_at,
                sample.status,
                sample.identity.principal,
                sample.identity.decision,
                sample.identity.trace_id,
                sample.identity.session_id,
                sample.tool_name,
                sample.policy,
                sample.declared_policy,
                # The stored `payload` column is ordinary JSON, and jsonb
                # discards key order and duplicate keys. `payload_sha256` and
                # `payload_bytes` come from `encode_payload` (sorted keys),
                # NOT from this column, so the digest is a fingerprint of the
                # payload as it arrived and is deliberately NOT recomputable
                # from what Postgres stores. Verify against the re-fetched
                # payload, never against `payload`.
                (
                    None
                    if sample.payload is None
                    else json.dumps(sample.payload, default=str)
                ),
                sample.payload_sha256,
                sample.payload_bytes,
                sample.summary,
                (
                    None
                    if sample.refetch is None
                    else json.dumps(dict(sample.refetch), default=str)
                ),
                json.dumps(dict(sample.attrs), default=str),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.call_sample_watermark
              (deployment_name, node_id, relation, retention_class,
               first_observed_at)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (deployment_name, node_id, relation, retention_class)
            DO UPDATE SET
              first_observed_at = LEAST(
                okg.call_sample_watermark.first_observed_at,
                EXCLUDED.first_observed_at),
              updated_at = now()
            """,
            (
                sample.deployment_name,
                sample.node_id,
                sample.relation,
                sample.retention_class,
                sample.observed_at,
            ),
        )
    return sample


def record_tool_call_sample(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    tool_name: str,
    relation: str,
    node_id: Optional[str],
    identity: SampleIdentity,
    status: str,
    payload: Any = None,
    refetch: Optional[RefetchPointer] = None,
    registry: Optional[RelationPolicyRegistry] = None,
    observed_at: Optional[datetime] = None,
    attrs: Optional[Mapping[str, Any]] = None,
) -> Optional[StoredSample]:
    """Look up the relation's policy, build the sample, store it if any.

    Returns ``None`` when the policy is ``none`` or the call named no node.
    """
    policy = (registry or default_registry()).policy_for(relation)
    sample = build_sample(
        deployment_name=deployment_name,
        node_id=node_id,
        relation=relation,
        policy=policy,
        identity=identity,
        status=status,
        payload=payload,
        refetch=refetch,
        observed_at=observed_at,
        tool_name=tool_name,
        attrs=attrs,
    )
    if sample is None:
        return None
    return record_call_sample(conn, sample)


def record_live_relation_call_sample(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    tool_name: str,
    relation: str,
    node_id: Optional[str],
    principal: Optional[str],
    decision: Optional[str],
    trace_id: Optional[str],
    session_id: Optional[str],
    status: str,
    payload: Any = None,
    refetch: Optional[RefetchPointer] = None,
    registry: Optional[RelationPolicyRegistry] = None,
    observed_at: Optional[datetime] = None,
    attrs: Optional[Mapping[str, Any]] = None,
    store_inline: bool = True,
) -> Optional[StoredSample]:
    """Write a live-relation sample with the dispatcher's real identity.

    This is the integration point for issue #2013. The caller supplies the
    principal and Nomos decision it actually obtained; all four identity
    values are required and no authorization value is invented. The relation
    must be explicitly registered as live. An undeclared relation or a
    graph-read declaration raises instead of silently claiming that live
    sampling succeeded.

    An explicitly registered live ``none`` policy writes zero rows and returns
    ``None``. Other write failures raise so the dispatcher can fail the call in
    the same transaction.
    """
    selected_registry = registry or default_registry()
    if relation not in selected_registry.declared_relations:
        raise CallSampleError(
            f"live relation {relation!r} has no registered sample policy; "
            "register it before dispatch"
        )
    policy = selected_registry.policy_for(relation)
    if policy.graph_read:
        raise CallSampleError(
            f"relation {relation!r} is registered as a graph-read policy, not "
            "a live relation"
        )
    identity = live_identity(
        principal=principal,
        decision=decision,
        trace_id=trace_id,
        session_id=session_id,
    )
    sample = build_sample(
        deployment_name=deployment_name,
        node_id=node_id,
        relation=relation,
        policy=policy,
        identity=identity,
        status=status,
        payload=payload,
        refetch=refetch,
        observed_at=observed_at,
        tool_name=tool_name,
        attrs=attrs,
        store_inline=store_inline,
    )
    if sample is None:
        return None
    return record_call_sample(conn, sample)


@dataclass(frozen=True)
class CallTarget:
    """What node(s) a call named, and whether it was supposed to name any.

    ``node_targeted`` is about the CALL, not about the outcome: an ``inspect``
    of ``{"kind": "graph"}`` is not node-targeted and samples nothing, quietly
    and correctly. An ``expand`` always anchors on a node, so an ``expand``
    whose anchor cannot be read IS node-targeted with no ids -- a gap the
    caller must log rather than skip in silence.

    ``arg_shape`` describes the shape only (types and mapping keys), never the
    argument values, so it is safe to log.
    """

    node_ids: tuple[str, ...]
    node_targeted: bool
    arg_shape: str


def _describe_shape(label: str, value: Any) -> str:
    if isinstance(value, Mapping):
        return f"{label}=mapping(keys={sorted(str(k) for k in value)})"
    if isinstance(value, (list, tuple)):
        kinds = sorted({type(item).__name__ for item in value})
        return f"{label}=sequence(len={len(value)}, item_types={kinds})"
    return f"{label}={type(value).__name__}"


def _node_ids_from(value: Any) -> tuple[str, ...]:
    """Pull node ids out of an anchor argument.

    Accepts a bare id, a mapping carrying ``node_id`` or ``id``, and a list of
    either. A list anchor produces ONE SAMPLE PER NODE: an ``expand`` over
    three nodes is three calls' worth of state, and collapsing it to one row
    would lose which node each answer belonged to.
    """
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    if isinstance(value, Mapping):
        candidate = value.get("node_id") or value.get("id")
        return _node_ids_from(candidate) if candidate is not None else ()
    if isinstance(value, (list, tuple)):
        found: list[str] = []
        for item in value:
            found.extend(_node_ids_from(item))
        return tuple(dict.fromkeys(found))
    return ()


def resolve_call_target(
    tool_name: str,
    *,
    args: tuple[Any, ...] = (),
    kwargs: Optional[Mapping[str, Any]] = None,
) -> CallTarget:
    """Resolve the node(s) a call targets, and say when it named none.

    Only the tools in ``NODE_TARGETED_TOOLS`` can target a node. Every other
    tool -- ``search``, ``filter``, ``map``, ``list_sources``, the admin tools --
    is not node-targeted, writes nothing, and reports nothing: it was never
    supposed to name a node. A tool that IS node-targeted but whose anchor
    cannot be read comes back with no ids and an ``arg_shape`` the caller logs.
    """
    kwargs = dict(kwargs or {})
    if tool_name not in NODE_TARGETED_TOOLS:
        return CallTarget(node_ids=(), node_targeted=False, arg_shape="untargeted")

    target = kwargs.get("target")
    if target is None and args and isinstance(args[0], Mapping):
        target = args[0]
    if isinstance(target, Mapping):
        # `inspect` dispatches on target.kind; only kind='node' names a node.
        if target.get("kind") != "node":
            return CallTarget(
                node_ids=(),
                node_targeted=False,
                arg_shape=f"target.kind={target.get('kind')!r}",
            )
        found = _node_ids_from(target)
        return CallTarget(
            node_ids=found,
            node_targeted=True,
            arg_shape=_describe_shape("target", target),
        )

    anchor = kwargs.get("input")
    if anchor is None:
        anchor = kwargs.get("node_id")
    if anchor is None and args:
        anchor = args[0]
    found = _node_ids_from(anchor)
    return CallTarget(
        node_ids=found,
        node_targeted=True,
        arg_shape=_describe_shape("input", anchor),
    )


def refetch_pointer_for_call(
    policy: RelationSamplePolicy,
    *,
    kwargs: Optional[Mapping[str, Any]],
    arg_shape: str,
    source: str,
) -> RefetchPointer:
    """Build a digest sample's re-fetch pointer under the relation's allowlist.

    The stored ``query`` always carries the argument SHAPE. It carries a
    call argument's VALUE only when the relation named that key in
    ``refetch_query_keys``. Nothing else from kwargs is copied, because a
    sample outlives the call by up to 730 days and a caller's arguments are
    not this module's to keep.
    """
    kwargs = dict(kwargs or {})
    query: dict[str, Any] = {"arg_shape": arg_shape}
    allowed = {
        key: kwargs[key] for key in policy.refetch_query_keys if key in kwargs
    }
    if allowed:
        query["args"] = allowed
    return RefetchPointer(query=query, source=source)


def target_nodes_for_call(
    tool_name: str,
    *,
    args: tuple[Any, ...] = (),
    kwargs: Optional[Mapping[str, Any]] = None,
) -> tuple[str, ...]:
    """Every node a call targets. One sample is written per node."""
    return resolve_call_target(tool_name, args=args, kwargs=kwargs).node_ids


def target_node_for_call(
    tool_name: str,
    *,
    args: tuple[Any, ...] = (),
    kwargs: Optional[Mapping[str, Any]] = None,
) -> Optional[str]:
    """The first node a call targets, or ``None`` when it targets none.

    Use ``target_nodes_for_call`` when a multi-node anchor matters.
    """
    found = target_nodes_for_call(tool_name, args=args, kwargs=kwargs)
    return found[0] if found else None


def relation_for_call(
    tool_name: str, *, kwargs: Optional[Mapping[str, Any]] = None
) -> str:
    """The relation name a call is sampled under.

    ``expand`` is sampled per edge type -- that is the relation an agent
    follows, and the unit a live relation declares a policy for. Every other
    operator is sampled under ``graph.<tool>``.
    """
    kwargs = dict(kwargs or {})
    if tool_name == "expand":
        edge_type = kwargs.get("edge_type")
        if isinstance(edge_type, str) and edge_type.strip():
            return edge_type.strip()
    return f"graph.{tool_name}"


#: `attrs.served_from` on the sample row of a read of a retained live answer
#: (filter, map or aggregate over `input={"ref": "l1..."}`). Such a read did
#: not call the external system, so call counts leave it out.
RETAINED_ANSWER_READ = "retained_answer"


def live_call_counts(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    relation: str,
    retention_class: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> dict[str, int]:
    """Live CALLS of one relation by status: what health and metrics count.

    One row per call the dispatcher made (or refused), from the relation's
    retention-class table, optionally inside ``[start, end]``. Rows written
    by reads of a retained answer (``attrs.served_from = "retained_answer"``)
    are excluded: they never reached the external system. ``total`` is the
    sum.
    """
    table = table_for_class(retention_class)
    clauses = ["deployment_name = %s", "relation = %s",
               "coalesce(attrs->>'served_from', '') <> %s"]
    params: list[Any] = [deployment_name, relation, RETAINED_ANSWER_READ]
    if start is not None:
        clauses.append("observed_at >= %s")
        params.append(start)
    if end is not None:
        clauses.append("observed_at <= %s")
        params.append(end)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT status, count(*) FROM {table} "  # noqa: S608 - closed table map
            f"WHERE {' AND '.join(clauses)} GROUP BY status",
            params,
        )
        counts = {str(status): int(n) for status, n in cur.fetchall()}
    counts["total"] = sum(counts.values())
    return counts


def read_sample_window(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    node_id: str,
    relation: str,
    retention_class: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    """Read one class's samples for a window, with a retention-aware state.

    Rows written by reads of a retained live answer
    (``attrs.served_from = "retained_answer"``) are left out, as
    :func:`live_call_counts` leaves them out: they are not calls.

    ``data`` -- rows are present. ``aged_out`` -- the range existed and was
    evicted. ``no_data`` -- the range never existed, or is a genuine gap inside
    the retained window. An empty window is NEVER coerced to ``no_data`` when
    the watermark shows samples existed there.

    A ``data`` result whose window BEGINS BEFORE the earliest surviving
    sample, while the watermark shows earlier samples existed, carries
    ``truncated_by_retention: True`` with ``first_observed_at`` and
    ``earliest_retained_at``. Rows plus silence would otherwise read as a
    complete answer for a window whose early samples retention removed.
    """
    table = table_for_class(retention_class)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT observed_at, policy, status, payload, payload_sha256, "
            f"summary, refetch FROM {table} "  # noqa: S608 - closed table map
            "WHERE deployment_name=%s AND node_id=%s AND relation=%s "
            "AND observed_at >= %s AND observed_at <= %s "
            "AND coalesce(attrs->>'served_from', '') <> %s ORDER BY observed_at",
            (deployment_name, node_id, relation, start, end, RETAINED_ANSWER_READ),
        )
        rows = [
            {
                "observed_at": r[0].isoformat(),
                "policy": r[1],
                "status": r[2],
                "payload": r[3],
                "payload_sha256": r[4],
                "summary": r[5],
                "refetch": r[6],
            }
            for r in cur.fetchall()
        ]
        # The window's own earliest surviving sample tells nothing about what
        # retention removed; the watermark and the relation's global minimum
        # do. Read them even on the `data` path, so a window that lost its
        # early samples says so instead of looking complete.
        cur.execute(
            "SELECT first_observed_at FROM okg.call_sample_watermark "
            "WHERE deployment_name=%s AND node_id=%s AND relation=%s "
            "AND retention_class=%s",
            (deployment_name, node_id, relation, retention_class),
        )
        watermark = cur.fetchone()
        first_seen = watermark[0] if watermark else None

        cur.execute(
            f"SELECT min(observed_at) FROM {table} "  # noqa: S608 - closed map
            "WHERE deployment_name=%s AND node_id=%s AND relation=%s "
            "AND coalesce(attrs->>'served_from', '') <> %s",
            (deployment_name, node_id, relation, RETAINED_ANSWER_READ),
        )
        current_min = cur.fetchone()[0]

        if rows:
            result: dict[str, Any] = {
                "state": "data",
                "retention_class": retention_class,
                "rows": rows,
            }
            # Retention has evicted something (the watermark predates the
            # oldest surviving row) AND the caller asked for a window that
            # begins before that oldest surviving row. Whether a sample
            # actually sat in the evicted part of THIS window is unknowable --
            # the rows are gone -- so the answer is marked partial rather than
            # returned as if it were the whole window. A window beginning at or
            # after the earliest retained sample lost nothing and is unmarked.
            if (
                first_seen is not None
                and current_min is not None
                and first_seen < current_min
                and start < current_min
            ):
                result["truncated_by_retention"] = True
                result["first_observed_at"] = first_seen.isoformat()
                result["earliest_retained_at"] = current_min.isoformat()
            return result

    base: dict[str, Any] = {"retention_class": retention_class, "rows": []}
    if first_seen is None:
        return {**base, "state": "no_data", "reason": "relation_never_sampled"}
    if end < first_seen:
        return {**base, "state": "no_data", "reason": "before_first_sample"}
    if current_min is None:
        return {
            **base,
            "state": "aged_out",
            "first_observed_at": first_seen.isoformat(),
        }
    if end < current_min:
        return {
            **base,
            "state": "aged_out",
            "first_observed_at": first_seen.isoformat(),
            "earliest_retained_at": current_min.isoformat(),
        }
    return {**base, "state": "no_data", "reason": "gap_within_retained_window"}
