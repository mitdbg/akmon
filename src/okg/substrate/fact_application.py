"""Shared admission-aware application chokepoint for FactBatch chunks."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Iterator, Mapping

from okg.substrate.admission import FactRangeRef
from okg.substrate.fact_batch import (
    ApplicationReceipt,
    ApplicationReceiptStore,
    FactBatchError,
    FactBatchHeader,
    FactBatchSummary,
    FactBatchValidator,
    FactChunk,
    MAX_FACTS_PER_CHUNK,
    PostgresApplicationReceiptStore,
    validate_chunk_authority,
    validate_header_authority,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.producer_authority import ProducerProfile


FACT_BATCH_ADMISSION_STATES = frozenset({"prepared", "applying", "finalized"})


class FactBatchApplicationError(FactBatchError):
    """Raised when shared application lifecycle invariants are violated."""


class FactBatchRecoveryBlocked(FactBatchApplicationError):
    """Raised when recovery or compensation lacks verifiable evidence."""


@dataclass(frozen=True)
class ZeroOutputEvidence:
    """Server-resolved evidence that may support a zero-output acceptance.

    This object is not a caller claim and does not itself accept an admission.
    The caller must supply the admission decision separately to ``finalize``.
    """

    kind: str
    health_status: str
    completed_scope: bool
    terminal_state_authorized: bool
    reason: str

    def __post_init__(self) -> None:
        if self.kind not in {"unchanged", "authoritative_empty_snapshot"}:
            raise FactBatchApplicationError(
                "zero-output evidence must be unchanged or an "
                "authoritative empty snapshot"
            )
        if not self.health_status:
            raise FactBatchApplicationError("zero-output health_status is required")
        if not self.reason:
            raise FactBatchApplicationError("zero-output reason is required")


@dataclass(frozen=True)
class AppliedChunk:
    chunk: FactChunk
    receipt: ApplicationReceipt
    graph_result: Mapping[str, int]


@dataclass(frozen=True)
class BoundedFactBatchResult:
    """Result of a server-owned derived/system FactBatch application."""

    admission_id: str
    run_id: str
    summary: FactBatchSummary
    primitive_results: tuple[Any, ...]

    @property
    def last_primitive_result(self) -> Any | None:
        return self.primitive_results[-1] if self.primitive_results else None

    def primitive_total(self, attribute: str) -> int:
        return sum(
            int(getattr(result, attribute, 0) or 0) for result in self.primitive_results
        )


GraphApply = Callable[
    [Iterable[NodeFact | EdgeFact], FactBatchHeader],
    Any,
]
ChunkEvidenceWriter = Callable[
    [FactChunk, ApplicationReceipt],
    Iterable[FactRangeRef] | None,
]


class FactBatchApplication:
    """Prepare, apply, and finish observing one logical FactBatch.

    Domain paths retain their identity/auth/policy checks. They enter this
    object only after resolving a server-owned ``ProducerProfile`` and the
    admission envelope represented by ``FactBatchHeader``.
    """

    def __init__(
        self,
        *,
        header: FactBatchHeader,
        profile: ProducerProfile,
        receipt_store: ApplicationReceiptStore,
        apply_graph: GraphApply,
        write_chunk_evidence: ChunkEvidenceWriter | None = None,
        retractions_allowed: bool = False,
        validate_signature: bool = True,
    ) -> None:
        self.header = header
        self.profile = profile
        self.receipt_store = receipt_store
        self.apply_graph = apply_graph
        self.write_chunk_evidence = write_chunk_evidence
        self.validator = FactBatchValidator(
            header,
            profile=profile,
            retractions_allowed=retractions_allowed,
            validate_signature=validate_signature,
        )
        self.state = "prepared"
        self._receipts: list[ApplicationReceipt] = []

    @property
    def receipts(self) -> tuple[ApplicationReceipt, ...]:
        return tuple(self._receipts)

    def apply_chunk(self, chunk: FactChunk) -> AppliedChunk:
        if self.state == "finalized":
            raise FactBatchApplicationError("admission is already finalized")
        self.validator.accept(chunk)
        prepared = self.receipt_store.prepare(
            ApplicationReceipt.prepared(self.header, chunk)
        )
        if prepared.state == "applied":
            self._receipts.append(prepared)
            self.state = "applying"
            return AppliedChunk(
                chunk=chunk,
                receipt=prepared,
                graph_result=prepared.graph_result,
            )
        if prepared.state != "prepared":
            raise FactBatchApplicationError(
                f"cannot apply receipt in state {prepared.state!r}"
            )
        self.state = "applying"
        graph_result_raw = self.apply_graph(chunk.facts, self.header)
        graph_result, branch_token = _normalize_graph_result(graph_result_raw)
        if self.write_chunk_evidence is not None:
            applied = self.receipt_store.transition_with_evidence(
                prepared.key,
                "applied",
                graph_result=graph_result,
                branch_token=branch_token,
                evidence_writer=lambda: self.write_chunk_evidence(
                    chunk,
                    prepared,
                ),
            )
        else:
            applied = self.receipt_store.transition(
                prepared.key,
                "applied",
                graph_result=graph_result,
                branch_token=branch_token,
            )
        self._receipts.append(applied)
        return AppliedChunk(
            chunk=chunk,
            receipt=applied,
            graph_result=applied.graph_result,
        )

    def finish_observation(
        self,
        *,
        completion: str,
        observed_scope: Mapping[str, Any] | None = None,
        source_delta: Mapping[str, Any] | None = None,
    ) -> FactBatchSummary:
        if self.state == "finalized":
            raise FactBatchApplicationError("admission is already finalized")
        summary = FactBatchSummary.from_receipts(
            run_id=self.header.run_id,
            receipts=self._receipts,
            observed_scope=observed_scope,
            completion=completion,
            source_delta=source_delta,
        )
        self.validator.finish(summary)
        self.state = "finalized"
        return summary


def assert_zero_output_acceptance_allowed(evidence: ZeroOutputEvidence | None) -> None:
    """Validate an admission decision for an observed zero-output batch."""
    if evidence is None:
        raise FactBatchApplicationError(
            "zero-output acceptance requires server-resolved evidence"
        )
    if (
        not evidence.completed_scope
        or not evidence.terminal_state_authorized
        or evidence.health_status not in {"ok", "not_applicable", "skipped_optional"}
    ):
        raise FactBatchApplicationError(
            "zero-output evidence does not authorize terminal state"
        )


class FactBatchRecoveryCoordinator:
    """Recover prepared chunks and prove compensation before terminal failure."""

    def __init__(
        self,
        *,
        header: FactBatchHeader,
        profile: ProducerProfile,
        receipt_store: ApplicationReceiptStore,
        retractions_allowed: bool = False,
        validate_signature: bool = True,
    ) -> None:
        validate_header_authority(header, profile)
        self.header = header
        self.profile = profile
        self.receipt_store = receipt_store
        self.retractions_allowed = retractions_allowed
        self.validate_signature = validate_signature

    def recover_prepared_chunk(
        self,
        chunk: FactChunk,
        *,
        apply_graph: GraphApply,
        write_chunk_evidence: ChunkEvidenceWriter | None = None,
    ) -> ApplicationReceipt:
        receipt = self.receipt_store.get(
            self.header.admission_id,
            self.header.run_id,
            chunk.ordinal,
        )
        if receipt is None:
            raise FactBatchRecoveryBlocked("prepared recovery receipt is missing")
        receipt.verify_chunk(chunk)
        if receipt.target_branch != self.header.target_branch:
            raise FactBatchRecoveryBlocked(
                "prepared receipt target branch does not match authority"
            )
        if receipt.state == "applied":
            if not receipt.graph_result and receipt.branch_token is None:
                raise FactBatchRecoveryBlocked(
                    "applied receipt has no verifiable graph result"
                )
            return receipt
        if receipt.state != "prepared":
            raise FactBatchRecoveryBlocked(
                f"cannot recover receipt in state {receipt.state!r}"
            )
        validate_chunk_authority(
            chunk,
            profile=self.profile,
            retractions_allowed=self.retractions_allowed,
            validate_signature=self.validate_signature,
        )
        graph_result_raw = apply_graph(chunk.facts, self.header)
        graph_result, branch_token = _normalize_graph_result(graph_result_raw)
        if not graph_result and branch_token is None and chunk.facts:
            raise FactBatchRecoveryBlocked(
                "reapplied chunk returned no verifiable graph result"
            )
        if write_chunk_evidence is not None:
            return self.receipt_store.transition_with_evidence(
                receipt.key,
                "applied",
                graph_result=graph_result,
                branch_token=branch_token,
                evidence_writer=lambda: write_chunk_evidence(chunk, receipt),
            )
        return self.receipt_store.transition(
            receipt.key,
            "applied",
            graph_result=graph_result,
            branch_token=branch_token,
        )

    def compensate(
        self,
        receipts: Iterable[ApplicationReceipt],
        *,
        compensate_graph: Callable[[ApplicationReceipt], None],
    ) -> tuple[ApplicationReceipt, ...]:
        compensated: list[ApplicationReceipt] = []
        for supplied in receipts:
            receipt = self.receipt_store.get(*supplied.key)
            if receipt is None:
                raise FactBatchRecoveryBlocked("compensation receipt is missing")
            if receipt.canonical_digest != supplied.canonical_digest:
                raise FactBatchRecoveryBlocked("compensation receipt digest changed")
            if receipt.state == "prepared":
                raise FactBatchRecoveryBlocked(
                    "prepared receipt has unverifiable graph outcome"
                )
            if receipt.state == "compensated":
                compensated.append(receipt)
                continue
            if receipt.state == "applied":
                receipt = self.receipt_store.transition(
                    receipt.key,
                    "compensating",
                )
            if receipt.state != "compensating":
                raise FactBatchRecoveryBlocked(
                    f"cannot compensate receipt in state {receipt.state!r}"
                )
            try:
                compensate_graph(receipt)
            except Exception as exc:
                raise FactBatchRecoveryBlocked(
                    "graph compensation failed; receipt remains compensating"
                ) from exc
            receipt = self.receipt_store.transition(
                receipt.key,
                "compensated",
            )
            compensated.append(receipt)
        return tuple(compensated)

    @staticmethod
    def assert_terminal_disposition_allowed(
        disposition: str,
        receipts: Iterable[ApplicationReceipt],
    ) -> None:
        states = {receipt.state for receipt in receipts}
        if disposition == "accepted" and states - {"applied"}:
            raise FactBatchRecoveryBlocked(
                "accepted disposition requires every receipt applied"
            )
        if disposition in {"rejected", "failed", "quarantined"} and states - {
            "compensated"
        }:
            raise FactBatchRecoveryBlocked(
                "nonaccepted terminal disposition requires proven compensation"
            )


def chronos_apply_callback(
    store: Any,
    *,
    source: str | None,
    observed_at: datetime | None = None,
    policy_fingerprint: str | None = None,
    strict_endpoints: bool = True,
    session: Any | None = None,
    derived_reapplication: bool = False,
    node_retraction: str = "handoff",
    prepared_for: Callable[[], Any | None] | None = None,
    row_provenance: Mapping[str, Mapping[str, Any]] | None = None,
) -> GraphApply:
    """Bind the primitive graph store to a pinned FactBatch header.

    ``derived_reapplication`` is forwarded to the graph store: it marks a
    batch whose facts were already admitted under their emitting
    producer and keeps the lineage they already carry (#1534).

    ``prepared_for`` returns the read-and-compute half prepared ahead for
    the chunk being applied, or None (issue 2941 fix 1). The graph store
    reuses it only after proving under the branch write lock that its
    inputs are unchanged, and recomputes otherwise.

    ``row_provenance`` is forwarded only when given: the graph store then
    rewrites existing nodes keeping each row's own provenance (#3318).
    """

    def apply(
        facts: Iterable[NodeFact | EdgeFact],
        header: FactBatchHeader,
    ) -> Any:
        kwargs = {
            "branch": header.target_branch,
            "source": source,
            "policy_fingerprint": policy_fingerprint,
            "run_id": str(header.run_id),
            "observed_at": observed_at,
            "strict_endpoints": strict_endpoints,
            "derived_reapplication": derived_reapplication,
        }
        if node_retraction != "handoff":
            # Only a caller that asked for another mode passes it, so a
            # store without the parameter keeps working for everyone else.
            kwargs["node_retraction"] = node_retraction
        if row_provenance is not None:
            kwargs["row_provenance"] = row_provenance
        if session is not None:
            kwargs["session"] = session
        prepared = prepared_for() if prepared_for is not None else None
        if prepared is not None:
            kwargs["prepared"] = prepared
        return store.apply_facts(
            [_mutable_graph_fact(fact) for fact in facts],
            **kwargs,
        )

    return apply


def apply_bounded_fact_batch(
    conn: Any,
    store: Any,
    facts: Iterable[NodeFact | EdgeFact],
    *,
    deployment_name: str,
    producer_id: str,
    producer_kind: str,
    intent: str,
    catalog_version_id: int | None,
    authority_scope: Mapping[str, Any],
    source: str | None = None,
    target_branch: str = "default",
    run_id: str | uuid.UUID | None = None,
    idempotency_key: str | None = None,
    completion: str = "delta",
    trust_label: str = "derived-deterministic-trusted",
    retractions_allowed: bool = False,
    strict_endpoints: bool = True,
    observed_at: datetime | None = None,
    policy_fingerprint: str | None = None,
    session: Any | None = None,
    chunk_size: int = MAX_FACTS_PER_CHUNK,
    zero_output_kind: str = "unchanged",
    zero_output_reason: str = "bounded workflow emitted no graph facts",
    cross_source_retractions: bool = False,
    derived_reapplication: bool = False,
    node_retraction: str = "handoff",
    prepared_chunks: Mapping[int, Any] | None = None,
    row_provenance: Mapping[str, Mapping[str, Any]] | None = None,
) -> BoundedFactBatchResult:
    """Apply a bounded derived/system stream under explicit server authority.

    Domain invocations stay unchanged. This helper supplies the common admission,
    ProducerProfile, chunk validation, durable receipt, and graph-application
    lifecycle for paths that do not have SourceRunner's stateful protocol.

    ``prepared_chunks`` maps a chunk ordinal to that chunk's graph-apply
    half prepared ahead (``ChronosGraphStore.prepare_apply_facts``); see
    ``chronos_apply_callback``.

    ``row_provenance`` (issue #3318) makes this a rewrite of existing nodes
    that keeps every row's own source, record id, revision, run id,
    observed time and lineage. The producer still owns the admission
    envelope and receipt, which record that it made the rewrite; the graph
    rows are written with no batch source. See ``apply_facts``.
    """
    from okg.substrate.admission import (
        ensure_envelope,
        stable_idempotency_key,
        transition_envelope,
    )
    from okg.substrate.producer_authority import (
        SYNTHETIC_PROFILE_ORIGIN,
        ProducerPolicyViolation,
        load_profile_by_fingerprint,
        materialize_producer_profile,
        profile_fingerprint,
    )

    if producer_kind not in {"extractor", "enricher", "lens", "workflow", "system"}:
        raise FactBatchApplicationError(
            "bounded FactBatch producer_kind must be derived or system authority"
        )
    if chunk_size <= 0 or chunk_size > MAX_FACTS_PER_CHUNK:
        raise FactBatchApplicationError(
            f"chunk_size must be between 1 and {MAX_FACTS_PER_CHUNK}"
        )
    if row_provenance is not None and (
        prepared_chunks or cross_source_retractions or derived_reapplication
    ):
        raise FactBatchApplicationError(
            "row_provenance rewrites cannot be prepared ahead, retract across "
            "sources, or re-apply derived facts"
        )
    run_uuid = uuid.UUID(str(run_id)) if run_id is not None else uuid.uuid4()
    scope = dict(authority_scope)
    if producer_kind in {"extractor", "enricher", "lens"}:
        required_derived_scope = {
            "dependency_generation",
            "input_lineage",
            "invalidation_scope",
            "determinism",
            "incomplete_result",
            "watermark",
        }
        missing = required_derived_scope - scope.keys()
        if missing:
            raise FactBatchApplicationError(
                "derived FactBatch authority is missing " + ", ".join(sorted(missing))
            )
        if scope["determinism"] not in {
            "deterministic",
            "model_nondeterministic",
        }:
            raise FactBatchApplicationError(
                "derived FactBatch determinism classification is invalid"
            )
        if scope["incomplete_result"] is not False:
            raise FactBatchApplicationError(
                "incomplete derived output cannot reach graph application"
            )
    replay_key = idempotency_key or stable_idempotency_key(
        "bounded_fact_batch",
        deployment_name,
        producer_id,
        intent,
        str(run_uuid),
        scope,
    )
    fingerprint = profile_fingerprint(
        deployment_name=deployment_name,
        producer_id=producer_id,
        producer_kind=producer_kind,
        trust_label=trust_label,
        admission_mode="fast_track",
        authority_scope=scope,
        allowed_record_kinds=("node", "edge"),
        output_signature={},
        policy_clauses=(),
        catalog_version_id=catalog_version_id,
    )
    try:
        profile = load_profile_by_fingerprint(
            conn,
            deployment_name=deployment_name,
            producer_id=producer_id,
            profile_fingerprint=fingerprint,
        )
    except ProducerPolicyViolation as exc:
        if exc.code != "producer_profile_missing":
            raise
        profile = materialize_producer_profile(
            conn,
            deployment_name=deployment_name,
            producer_id=producer_id,
            producer_kind=producer_kind,
            trust_label=trust_label,
            admission_mode="fast_track",
            authority_scope=scope,
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            output_scope_summary=scope,
            source_ref={"workflow": intent, "producer_id": producer_id},
            catalog_version_id=catalog_version_id,
            # Bookkeeping, not a declaration: this row exists so the write
            # below has a FactBatch header to cite. Marking it keeps an
            # unfingerprinted trust lookup from resolving to it ahead of
            # what the deployment declared (issue #1545).
            profile_origin=SYNTHETIC_PROFILE_ORIGIN,
        )
    else:
        # Active/compat authority may be refreshed against a newer catalog, but
        # an explicitly disabled/revoked profile is never silently resurrected.
        if profile.status in {"active", "compat"}:
            profile = materialize_producer_profile(
                conn,
                deployment_name=deployment_name,
                producer_id=producer_id,
                producer_kind=producer_kind,
                trust_label=trust_label,
                admission_mode="fast_track",
                authority_scope=scope,
                allowed_record_kinds=("node", "edge"),
                output_signature={},
                policy_clauses=(),
                output_scope_summary=scope,
                source_ref={"workflow": intent, "producer_id": producer_id},
                catalog_version_id=catalog_version_id,
                profile_origin=SYNTHETIC_PROFILE_ORIGIN,
            )
    envelope = ensure_envelope(
        conn,
        deployment_name=deployment_name,
        producer_id=producer_id,
        producer_kind=producer_kind,
        intent=intent,
        admission_mode="fast_track",
        status="pending",
        idempotency_key=replay_key,
        catalog_version_id=catalog_version_id,
        source=source or producer_id,
        run_id=run_uuid,
        authority={**profile.evidence(), **scope},
        # A fast-track envelope must RECORD what was checked before it was
        # let through, not just that it was. Without these two keys the
        # envelope is indistinguishable from an unchecked one, and
        # admission_conformance reports it as a violation (issue #1545).
        check_summary={
            "admission_policy": {
                "producer_id": producer_id,
                "producer_kind": producer_kind,
                "admission_mode": "fast_track",
                "intent": intent,
                "completion": completion,
                "retractions_allowed": bool(retractions_allowed),
                "strict_endpoints": bool(strict_endpoints),
                "cross_source_retractions": bool(cross_source_retractions),
                "derived_reapplication": bool(derived_reapplication),
                "row_provenance_preserved": row_provenance is not None,
                "authority_scope_checked": sorted(scope),
            },
            "producer_policy": {
                **profile.evidence(),
                "trust_label": profile.trust_label,
                # Read back from the row that was actually resolved, never
                # asserted: the summary must describe the profile this
                # envelope cites, not the one this call expected to mint.
                "profile_origin": (
                    dict(profile.source_ref or {}).get("profile_origin")
                ),
            },
        },
        target_branch_name=(target_branch if target_branch != "default" else None),
    )
    header = FactBatchHeader(
        schema_version=1,
        run_id=run_uuid,
        admission_id=envelope.admission_id,
        producer_profile_fingerprint=fingerprint,
        deployment=deployment_name,
        target_branch=target_branch,
        catalog_version_id=catalog_version_id or 0,
        intent=intent,
        idempotency_key=replay_key,
        requested_scope=scope,
    )
    if policy_fingerprint is None:
        from okg.substrate.nomos.store import load_active_policy_bundle

        active_policy = load_active_policy_bundle(
            conn,
            deployment_name=deployment_name,
            scope=f"deployment:{deployment_name}",
        )
        if active_policy is not None:
            policy_fingerprint = active_policy.policy_fingerprint
    primitive_results: list[Any] = []
    applying_ordinal: list[int] = [-1]

    def _prepared_for_chunk() -> Any | None:
        if not prepared_chunks:
            return None
        return prepared_chunks.get(applying_ordinal[0])

    graph_apply = chronos_apply_callback(
        store,
        source=(
            None
            if cross_source_retractions or row_provenance is not None
            else source or producer_id
        ),
        observed_at=observed_at,
        policy_fingerprint=policy_fingerprint,
        strict_endpoints=strict_endpoints,
        session=session,
        derived_reapplication=derived_reapplication,
        node_retraction=node_retraction,
        prepared_for=_prepared_for_chunk if prepared_chunks else None,
        row_provenance=row_provenance,
    )

    def _capture(
        values: Iterable[NodeFact | EdgeFact], batch_header: FactBatchHeader
    ) -> Any:
        result = graph_apply(values, batch_header)
        primitive_results.append(result)
        return result

    application = FactBatchApplication(
        header=header,
        profile=profile,
        receipt_store=PostgresApplicationReceiptStore(conn),
        apply_graph=_capture,
        retractions_allowed=retractions_allowed,
        validate_signature=False,
    )
    fact_count = 0
    for ordinal, chunk_facts in enumerate(_bounded_chunks(facts, chunk_size)):
        fact_count += len(chunk_facts)
        applying_ordinal[0] = ordinal
        application.apply_chunk(
            FactChunk.create(
                run_id=run_uuid,
                ordinal=ordinal,
                facts=chunk_facts,
            )
        )
    zero_output_evidence = None
    if fact_count == 0:
        zero_output_evidence = ZeroOutputEvidence(
            kind=zero_output_kind,
            health_status="ok",
            completed_scope=True,
            terminal_state_authorized=True,
            reason=zero_output_reason,
        )
    if fact_count == 0:
        assert_zero_output_acceptance_allowed(zero_output_evidence)
    summary = application.finish_observation(
        completion=completion,
        observed_scope=scope,
    )
    transition_envelope(
        conn,
        admission_id=envelope.admission_id,
        status="accepted",
        actor=producer_id,
        reason="bounded_fact_batch_applied",
        verdict={
            "fact_count": fact_count,
            "chunk_count": summary.terminal_chunk_count,
            "producer_profile_fingerprint": fingerprint,
            "publisher_required": True,
        },
    )
    return BoundedFactBatchResult(
        admission_id=envelope.admission_id,
        run_id=str(run_uuid),
        summary=summary,
        primitive_results=tuple(primitive_results),
    )


def prepare_bounded_fact_batch_chunks(
    store: Any,
    facts: Iterable[NodeFact | EdgeFact],
    *,
    target_branch: str,
    run_id: str | uuid.UUID,
    source: str,
    policy_fingerprint: str | None,
    strict_endpoints: bool,
    derived_reapplication: bool,
    observed_at: datetime | None = None,
    chunk_size: int = MAX_FACTS_PER_CHUNK,
    session: Any | None = None,
) -> dict[int, Any]:
    """Prepare each chunk's graph-apply half ahead (issue 2941 fix 1).

    Chunks the facts and freezes each chunk exactly as
    :func:`apply_bounded_fact_batch` will, then runs the graph store's
    read-and-compute half on the facts the apply callback would pass, with
    the same options, and fingerprints the rows it read. Writes nothing.
    Returns ``{ordinal: prepared}`` for ``prepared_chunks``.
    """
    run_uuid = uuid.UUID(str(run_id))
    out: dict[int, Any] = {}
    for ordinal, chunk_facts in enumerate(_bounded_chunks(facts, chunk_size)):
        chunk = FactChunk.create(
            run_id=run_uuid,
            ordinal=ordinal,
            facts=chunk_facts,
        )
        out[ordinal] = store.prepare_apply_facts(
            [_mutable_graph_fact(fact) for fact in chunk.facts],
            branch=target_branch,
            source=source,
            policy_fingerprint=policy_fingerprint,
            run_id=str(run_uuid),
            observed_at=observed_at,
            strict_endpoints=strict_endpoints,
            session=session,
            derived_reapplication=derived_reapplication,
            key_fingerprint=True,
        )
    return out


def _bounded_chunks(
    facts: Iterable[NodeFact | EdgeFact],
    chunk_size: int,
) -> Iterator[tuple[NodeFact | EdgeFact, ...]]:
    chunk: list[NodeFact | EdgeFact] = []
    for fact in facts:
        chunk.append(fact)
        if len(chunk) == chunk_size:
            yield tuple(chunk)
            chunk = []
    if chunk:
        yield tuple(chunk)


def _mutable_graph_fact(fact: NodeFact | EdgeFact) -> NodeFact | EdgeFact:
    if isinstance(fact, NodeFact):
        return NodeFact(
            node_id=fact.node_id,
            subtype=fact.subtype,
            attrs=_mutable_json(fact.attrs),
            source_record_id=_mutable_json(fact.source_record_id),
            source_revision=_mutable_json(fact.source_revision),
            op=fact.op,
            ownership_mode=fact.ownership_mode,
        )
    return EdgeFact(
        src=fact.src,
        dst=fact.dst,
        edge_type=fact.edge_type,
        attrs=_mutable_json(fact.attrs),
        provenance=fact.provenance,
        confidence=fact.confidence,
        source_record_id=_mutable_json(fact.source_record_id),
        source_revision=_mutable_json(fact.source_revision),
        op=fact.op,
    )


def _mutable_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _mutable_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_mutable_json(item) for item in value]
    return value


def _normalize_graph_result(value: Any) -> tuple[dict[str, int], str | None]:
    if value is None:
        return {}, None
    if isinstance(value, Mapping):
        counts = {
            str(name): int(count)
            for name, count in value.items()
            if isinstance(count, int) and not isinstance(count, bool) and count >= 0
        }
        token = value.get("branch_token")
        return counts, str(token) if token else None
    counts: dict[str, int] = {}
    for result_name, attribute in (
        ("node_subtypes_touched", "nodes_by_subtype"),
        ("edge_types_touched", "edges_by_type"),
    ):
        observed = getattr(value, attribute, None)
        if isinstance(observed, Mapping):
            counts[result_name] = len(observed)
            counts[f"{result_name}_items"] = sum(
                len(items) for items in observed.values()
            )
    token = getattr(value, "branch_token", None)
    return counts, str(token) if token else None


__all__ = [
    "FACT_BATCH_ADMISSION_STATES",
    "AppliedChunk",
    "BoundedFactBatchResult",
    "FactBatchApplication",
    "FactBatchApplicationError",
    "FactBatchRecoveryBlocked",
    "FactBatchRecoveryCoordinator",
    "ZeroOutputEvidence",
    "apply_bounded_fact_batch",
    "assert_zero_output_acceptance_allowed",
    "prepare_bounded_fact_batch_chunks",
    "chronos_apply_callback",
]
