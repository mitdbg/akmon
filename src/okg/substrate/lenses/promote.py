"""Lens-overlay promotion to base-graph facts.

Two entry points:

* :func:`promote_lens_overlay` opens a lens-promotion admission
  envelope for a previously committed overlay request. The envelope's
  verdict carries the planned graph mutations (computed from the
  lens definition's ``lens_promotion_mapping``).

* :func:`apply_promoted_overlay` is called by the publisher cycle's
  lens-promotion phase: it consumes an accepted (or pending) envelope,
  writes the derived ``node_facts`` / ``edge_facts`` to the deployment's
  default branch, flips overlay + request status, and transitions the
  envelope to ``published``.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.admission import (
    ensure_envelope,
    record_event,
    stable_idempotency_key,
    transition_envelope,
)
from okg.substrate.images.values import (
    ImageRefInvalid,
    ImageValueForged,
    check_incoming,
)
from okg.substrate.producer_authority import (
    ProducerPolicyViolation,
    load_profile_for_producer,
)

from .promotion_mapping import (
    LensPromotionMapping,
    LensPromotionMappingError,
    PromotionRule,
)
from .putback import (
    LensPutbackError,
    LensPutbackRequest,
    _load_request_by_id,
)


class LensPromotionError(LensPutbackError):
    """Base class for lens-promotion failures.

    Note: :class:`LensPromotionMappingError` lives in
    :mod:`promotion_mapping` and subclasses :class:`LensPutbackError`
    directly so the same validation can run at register-time. CLI
    callers that previously caught only ``LensPromotionError`` now
    also catch ``LensPromotionMappingError`` explicitly.
    """


class LensPromotionNotSupported(LensPromotionError):
    """Raised when a lens definition has no ``lens_promotion_mapping``."""

    def __init__(self, *, definition_id: str, fix_hint: str) -> None:
        self.definition_id = definition_id
        self.fix_hint = fix_hint
        super().__init__(
            f"lens definition {definition_id!r} does not declare a "
            f"lens_promotion_mapping; {fix_hint}"
        )


class LensPromotionStateError(LensPromotionError):
    """Raised when a request cannot transition to promoted."""


@dataclass(frozen=True)
class PlannedMutation:
    """One graph mutation planned by a lens-promotion mapping."""

    kind: str  # "node_attr" (first cut)
    subtype: str
    node_id: str
    attrs: Mapping[str, Any]
    overlay_id: UUID
    delta_id: UUID
    field_path: Sequence[str]
    new_value: Any
    rule_id: str

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "subtype": self.subtype,
            "node_id": self.node_id,
            "attrs": dict(self.attrs),
            "overlay_id": str(self.overlay_id),
            "delta_id": str(self.delta_id),
            "field_path": list(self.field_path),
            "new_value": self.new_value,
            "rule_id": self.rule_id,
        }


@dataclass(frozen=True)
class LensPromotionResult:
    """Outcome of ``promote_lens_overlay``."""

    admission_id: str
    status: str
    inserted: bool
    request_id: UUID
    definition_id: str
    deployment_name: str
    producer_id: str
    planned_mutations: tuple[PlannedMutation, ...] = field(default_factory=tuple)

    def to_json(self) -> dict[str, Any]:
        return {
            "admission_id": self.admission_id,
            "status": self.status,
            "inserted": self.inserted,
            "request_id": str(self.request_id),
            "definition_id": self.definition_id,
            "deployment_name": self.deployment_name,
            "producer_id": self.producer_id,
            "planned_mutations": [m.to_json() for m in self.planned_mutations],
        }


def lens_producer_id(definition_id: str) -> str:
    """Return the canonical lens producer id."""

    return f"_lens:{definition_id}"


def lens_promotion_mapping_checksum(mapping: Mapping[str, Any]) -> str:
    """Stable checksum of a lens promotion mapping for fingerprinting."""

    payload = json.dumps(
        dict(mapping),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def promote_lens_overlay(
    conn: psycopg.Connection[Any],
    *,
    request_id: UUID,
    actor_id: str,
    deployment_name: str | None = None,
    workflow_ref: Mapping[str, Any] | None = None,
    target_branch_name: str | None = None,
) -> LensPromotionResult:
    """Open a lens-promotion admission envelope for a committed overlay.

    Reads the lens definition's ``lens_promotion_mapping``, applies it
    to the overlay rows attached to ``request_id``, and opens a
    ``producer_kind='lens'`` admission envelope whose verdict carries
    the planned graph mutations. The publisher's lens-promotion phase
    is the consumer.

    ``target_branch_name`` (AKMON Phase B.2 quarantine routing) pins
    the envelope to a specific branch. NULL means the default branch.
    A publisher cycle running on the matching branch picks up the
    envelope; other branches' cycles skip it.
    """

    with conn.transaction():
        with conn.cursor() as cur:
            request = _load_request_by_id(cur, request_id, for_update=True)
            if request.status == "promoted":
                # Idempotent: return the existing envelope evidence.
                return _result_from_existing(cur, request, deployment_name)
            if request.status != "committed_overlay":
                raise LensPromotionStateError(
                    "lens putback request must be in 'committed_overlay' "
                    f"to promote: request_id={request_id} "
                    f"status={request.status}"
                )

            if deployment_name and deployment_name != request.deployment_name:
                raise LensPromotionStateError(
                    "deployment mismatch: request "
                    f"{request.deployment_name!r} != arg {deployment_name!r}"
                )

            loaded = _load_promotion_mapping(cur, request.lens_definition_id)
            if loaded is None:
                raise LensPromotionNotSupported(
                    definition_id=request.lens_definition_id,
                    fix_hint=(
                        "declare lens_promotion_mapping on "
                        "okg.lens_definitions to enable promotion"
                    ),
                )
            mapping_raw, mapping_typed = loaded

            overlay_rows = _load_active_overlays_for_request(cur, request_id)
            if not overlay_rows:
                raise LensPromotionStateError(
                    "no active overlay rows for request "
                    f"{request_id}; nothing to promote"
                )

            planned = _plan_mutations(
                mapping=mapping_typed,
                overlay_rows=overlay_rows,
                definition_id=request.lens_definition_id,
            )
            if not planned:
                raise LensPromotionMappingError(
                    "lens_promotion_mapping produced no mutations for any "
                    f"overlay row (request_id={request_id}); check that "
                    "rule slot_name / field_path match overlay deltas"
                )

            producer_id = lens_producer_id(request.lens_definition_id)
            idempotency_key = stable_idempotency_key(
                "lens_promotion",
                request.deployment_name,
                producer_id,
                str(request_id),
            )

            verdict = {
                "kind": "lens_promotion_plan",
                "lens_request_id": str(request_id),
                "lens_definition_id": request.lens_definition_id,
                "mapping_checksum": lens_promotion_mapping_checksum(mapping_raw),
                "planned_mutations": [m.to_json() for m in planned],
            }
            check_summary = {
                "overlay_count": len(overlay_rows),
                "mutation_count": len(planned),
            }
            authority = {
                "producer_kind": "lens",
                "producer_id": producer_id,
                "request_id": str(request_id),
            }

            envelope = ensure_envelope(
                conn,
                deployment_name=request.deployment_name,
                producer_id=producer_id,
                producer_kind="lens",
                intent="promote_lens_overlay",
                admission_mode="review",
                status="pending",
                idempotency_key=idempotency_key,
                authority=authority,
                check_summary=check_summary,
                verdict=verdict,
                workflow_ref=workflow_ref,
                target_branch_name=target_branch_name,
            )
            if envelope.inserted:
                record_event(
                    conn,
                    admission_id=envelope.admission_id,
                    event_type="refs_attached",
                    actor=actor_id,
                    payload={
                        "lens_request_id": str(request_id),
                        "overlay_ids": [
                            str(row["overlay_id"]) for row in overlay_rows
                        ],
                    },
                    workflow_ref=workflow_ref,
                )

            return LensPromotionResult(
                admission_id=envelope.admission_id,
                status=envelope.status,
                inserted=envelope.inserted,
                request_id=request_id,
                definition_id=request.lens_definition_id,
                deployment_name=request.deployment_name,
                producer_id=producer_id,
                planned_mutations=tuple(planned),
            )


def _load_promotion_mapping(
    cur: psycopg.Cursor[Any],
    definition_id: str,
) -> tuple[Mapping[str, Any], LensPromotionMapping] | None:
    """Load and validate the mapping. Returns (raw_dict, typed) or None.

    The raw dict is retained for checksum stability (existing
    envelopes carry checksums over the JSONB field order); the
    typed form drives planning.
    """

    cur.execute(
        """
        SELECT lens_promotion_mapping
          FROM okg.lens_definitions
         WHERE definition_id = %(definition_id)s
        """,
        {"definition_id": definition_id},
    )
    row = cur.fetchone()
    if row is None:
        raise LensPromotionError(
            f"lens definition not found: {definition_id}"
        )
    raw = row[0]
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise LensPromotionMappingError(
            f"lens_promotion_mapping for {definition_id!r} is not an "
            f"object: got {type(raw).__name__}"
        )
    raw_dict = dict(raw)
    typed = LensPromotionMapping.from_dict(raw_dict, definition_id=definition_id)
    return raw_dict, typed


def _load_active_overlays_for_request(
    cur: psycopg.Cursor[Any],
    request_id: UUID,
) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT overlay_id, delta_id, lens_definition_id,
               target_generation_id, source_authority, source_record_id,
               expected_source_revision, field_path, old_value, new_value,
               source_delta, status, metadata, created_at
          FROM okg.lens_overlay_log
         WHERE request_id = %(request_id)s
           AND status = 'active'
         ORDER BY created_at, overlay_id
        """,
        {"request_id": request_id},
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            {
                "overlay_id": row[0],
                "delta_id": row[1],
                "lens_definition_id": row[2],
                "target_generation_id": int(row[3]),
                "source_authority": row[4],
                "source_record_id": dict(row[5] or {}),
                "expected_source_revision": (
                    None if row[6] is None else dict(row[6])
                ),
                "field_path": list(row[7] or []),
                "old_value": row[8],
                "new_value": row[9],
                "source_delta": dict(row[10] or {}),
                "status": row[11],
                "metadata": dict(row[12] or {}),
                "created_at": row[13],
            }
        )
    return rows


def _plan_mutations(
    *,
    mapping: LensPromotionMapping,
    overlay_rows: Sequence[Mapping[str, Any]],
    definition_id: str,
) -> list[PlannedMutation]:
    """Plan mutations from a pre-validated typed mapping."""

    planned: list[PlannedMutation] = []
    unmapped: list[dict[str, Any]] = []
    for overlay in overlay_rows:
        rule = _match_rule(mapping.rules, overlay)
        if rule is None:
            source_delta = overlay.get("source_delta") or {}
            unmapped.append({
                "overlay_id": str(overlay.get("overlay_id") or ""),
                "delta_id": str(overlay.get("delta_id") or ""),
                "slot_name": str(source_delta.get("slot_name") or ""),
                "field_path": list(overlay.get("field_path") or []),
                "row_key": str(source_delta.get("row_key") or ""),
            })
            continue
        mutation = _materialize_rule(rule, overlay, definition_id)
        planned.append(mutation)
    if unmapped:
        raise LensPromotionMappingError(
            f"lens_promotion_mapping for {definition_id!r}: "
            "unmapped overlay rows are not promotable without an "
            f"approved skip record: {unmapped!r}"
        )
    return planned


def _match_rule(
    rules: Sequence[PromotionRule],
    overlay: Mapping[str, Any],
) -> PromotionRule | None:
    source_delta = overlay.get("source_delta") or {}
    overlay_slot = source_delta.get("slot_name")
    overlay_path = tuple(overlay.get("field_path") or [])
    for rule in rules:
        if rule.slot_name is not None and rule.slot_name != overlay_slot:
            continue
        if rule.field_path is not None and rule.field_path != overlay_path:
            continue
        return rule
    return None


def _materialize_rule(
    rule: PromotionRule,
    overlay: Mapping[str, Any],
    definition_id: str,
) -> PlannedMutation:
    mutation = rule.mutation
    source_delta = overlay.get("source_delta") or {}
    bindings = {
        "row_key": str(source_delta.get("row_key") or ""),
        "scope_key": str(source_delta.get("scope_key") or ""),
        "slot_name": str(source_delta.get("slot_name") or ""),
    }
    try:
        node_id = mutation.node_id_template.format(**bindings)
    except KeyError as exc:  # template referenced an unknown binding
        raise LensPromotionMappingError(
            f"lens_promotion_mapping for {definition_id!r}: "
            f"node_id_template references unknown binding {exc!s}"
        ) from exc
    node_id = node_id.strip()
    if not node_id:
        raise LensPromotionMappingError(
            f"lens_promotion_mapping for {definition_id!r}: "
            "node_id_template produced an empty node_id"
        )

    new_value = overlay.get("new_value")
    attrs: dict[str, Any] = {mutation.attr_field: new_value}
    # Image values (#3046): an overlay value cannot carry `png` or `error`,
    # which only okg's ingest image step adds. Raised inside the promote
    # transaction, so no envelope is opened.
    check_incoming(attrs)

    return PlannedMutation(
        kind=mutation.kind,
        subtype=mutation.subtype,
        node_id=node_id,
        attrs=attrs,
        overlay_id=overlay["overlay_id"],
        delta_id=overlay["delta_id"],
        field_path=tuple(overlay.get("field_path") or []),
        new_value=new_value,
        rule_id=rule.id,
    )


@dataclass(frozen=True)
class AppliedPromotion:
    """Outcome of ``apply_promoted_overlay`` for one envelope."""

    admission_id: str
    request_id: UUID
    definition_id: str
    deployment_name: str
    producer_id: str
    nodes_added: int
    edges_added: int
    overlay_ids: tuple[UUID, ...]
    target_generation_ref: str | None
    status: str  # final envelope status after this call

    def to_json(self) -> dict[str, Any]:
        return {
            "admission_id": self.admission_id,
            "request_id": str(self.request_id),
            "definition_id": self.definition_id,
            "deployment_name": self.deployment_name,
            "producer_id": self.producer_id,
            "nodes_added": self.nodes_added,
            "edges_added": self.edges_added,
            "overlay_ids": [str(o) for o in self.overlay_ids],
            "target_generation_ref": self.target_generation_ref,
            "status": self.status,
        }


def apply_promoted_overlay(
    conn: psycopg.Connection[Any],
    *,
    admission_id: UUID | str,
    cycle_token: str | None = None,
    target_generation_ref: str | None = None,
    workflow_ref: Mapping[str, Any] | None = None,
) -> AppliedPromotion:
    """Apply an accepted lens-promotion envelope.

    The publisher cycle's lens-promotion phase calls this once per
    accepted-or-pending ``producer_kind='lens'`` envelope. It writes the
    derived ``node_facts`` (one per ``planned_mutations[*]``) using the
    envelope's ``admission_id`` as the ``run_id``, flips the matching
    ``okg.lens_overlay_log`` rows to ``status='promoted'``, updates the
    ``okg.lens_putback_requests`` row to ``status='promoted'``, and
    transitions the envelope to ``status='published'``. The Chronos
    generation ref is attached later, by the publisher's
    ``_finalize_lens_promotion_generation_refs`` pass, because the checkpoint
    does not exist yet when this runs. ``cycle_token`` is the publisher cycle's
    own token, recorded in the verdict so that pass can identify exactly the
    envelopes THIS cycle drained --- without it, a cycle that crashed after
    minting its checkpoint would have its envelopes adopted by the next cycle
    and attributed to the wrong generation.

    Idempotent: re-applying an already-published envelope is a no-op
    (overlay rows are already ``promoted``; the ``ON CONFLICT`` on
    ``node_facts.dedupe_key`` avoids duplicate facts; the publisher
    phase still calls this so the per-cycle counters remain stable).
    """

    admission_uuid = UUID(str(admission_id))
    with conn.transaction():
        with conn.cursor() as cur:
            envelope = _load_lens_envelope(cur, admission_uuid)

            verdict = envelope["verdict"] or {}
            planned_raw = verdict.get("planned_mutations") or []
            if not isinstance(planned_raw, list):
                raise LensPromotionMappingError(
                    "lens envelope verdict.planned_mutations must be a list"
                )

            request_id = UUID(str(verdict.get("lens_request_id")))
            request = _load_request_by_id(cur, request_id, for_update=True)

            already_promoted = envelope["status"] == "published" or (
                request.status == "promoted"
            )

        # Profile check happens outside the cursor's locking scope:
        # rejecting writes through transition_envelope which has its
        # own cursor.
        producer_id = envelope["producer_id"]
        if not already_promoted:
            try:
                profile, _lookup = load_profile_for_producer(
                    conn,
                    deployment_name=envelope["deployment_name"],
                    producer_id=producer_id,
                )
                if profile.status != "active":
                    raise ProducerPolicyViolation(
                        code="producer_profile_inactive",
                        message=(
                            "lens producer profile is not active: "
                            f"status={profile.status!r}"
                        ),
                        verdict={
                            "code": "producer_profile_inactive",
                            "producer_id": producer_id,
                            "profile_status": profile.status,
                        },
                    )
            except ProducerPolicyViolation as exc:
                transition = transition_envelope(
                    conn,
                    admission_id=str(admission_uuid),
                    status="rejected",
                    actor=producer_id,
                    reason=exc.code,
                    verdict={
                        **dict(verdict),
                        "rejection": {
                            "code": exc.code,
                            "message": exc.message,
                            "fix_hint": (
                                f"declare authority.producers['{producer_id}']"
                                " in deployment.yaml with producer_kind='lens'"
                                if exc.code == "producer_profile_missing"
                                else (
                                    f"reactivate profile for {producer_id}"
                                )
                            ),
                        },
                    },
                    workflow_ref=workflow_ref,
                )
                return AppliedPromotion(
                    admission_id=str(admission_uuid),
                    request_id=request_id,
                    definition_id=request.lens_definition_id,
                    deployment_name=request.deployment_name,
                    producer_id=producer_id,
                    nodes_added=0,
                    edges_added=0,
                    overlay_ids=(),
                    target_generation_ref=target_generation_ref,
                    status=transition["status"],
                )

        with conn.cursor() as cur:

            nodes_added = 0
            edges_added = 0
            overlay_ids: list[UUID] = []
            planned_mutations: list[Mapping[str, Any]] = []
            for raw in planned_raw:
                if not isinstance(raw, Mapping):
                    raise LensPromotionMappingError(
                        "verdict.planned_mutations entries must be objects; "
                        f"got {type(raw).__name__}"
                    )
                kind = str(raw.get("kind") or "")
                if kind != "node_attr":
                    raise LensPromotionMappingError(
                        f"unsupported promoted mutation kind: {kind!r}"
                    )
                overlay_ids.append(UUID(str(raw["overlay_id"])))
                planned_mutations.append(raw)
            if not already_promoted:
                coverage_failure = _planned_overlay_coverage_failure(
                    cur,
                    request_id=request_id,
                    planned_overlay_ids=overlay_ids,
                )
                if coverage_failure is not None:
                    transition = transition_envelope(
                        conn,
                        admission_id=str(admission_uuid),
                        status="failed",
                        actor=producer_id,
                        reason=coverage_failure["code"],
                        verdict={
                            **dict(verdict),
                            "failure": coverage_failure,
                        },
                        workflow_ref=workflow_ref,
                    )
                    return AppliedPromotion(
                        admission_id=str(admission_uuid),
                        request_id=request_id,
                        definition_id=request.lens_definition_id,
                        deployment_name=request.deployment_name,
                        producer_id=producer_id,
                        nodes_added=0,
                        edges_added=0,
                        overlay_ids=tuple(overlay_ids),
                        target_generation_ref=target_generation_ref,
                        status=transition["status"],
                    )
            if not already_promoted:
                image_failure = _planned_image_value_failure(planned_mutations)
                if image_failure is not None:
                    transition = transition_envelope(
                        conn,
                        admission_id=str(admission_uuid),
                        status="rejected",
                        actor=producer_id,
                        reason=image_failure["code"],
                        verdict={
                            **dict(verdict),
                            "rejection": image_failure,
                        },
                        workflow_ref=workflow_ref,
                    )
                    return AppliedPromotion(
                        admission_id=str(admission_uuid),
                        request_id=request_id,
                        definition_id=request.lens_definition_id,
                        deployment_name=request.deployment_name,
                        producer_id=producer_id,
                        nodes_added=0,
                        edges_added=0,
                        overlay_ids=tuple(overlay_ids),
                        target_generation_ref=target_generation_ref,
                        status=transition["status"],
                    )
            for raw in planned_mutations:
                if already_promoted:
                    continue
                inserted = _insert_promoted_node_fact(
                    cur,
                    mutation=raw,
                    admission_id=admission_uuid,
                    definition_id=request.lens_definition_id,
                    request_id=request_id,
                )
                if inserted:
                    nodes_added += 1

            if not already_promoted:
                _flip_overlay_rows_to_promoted(
                    cur,
                    overlay_ids=overlay_ids,
                    admission_id=admission_uuid,
                )
                _mark_request_promoted(cur, request_id=request_id)

        status = envelope["status"]
        if status not in {"published", "rejected", "failed"}:
            transition = transition_envelope(
                conn,
                admission_id=str(admission_uuid),
                status="published",
                actor=lens_producer_id(request.lens_definition_id),
                reason="lens_promotion_applied",
                target_generation_ref=target_generation_ref,
                workflow_ref=workflow_ref,
                verdict={
                    **dict(verdict),
                    "applied": {
                        "nodes_added": nodes_added,
                        "edges_added": edges_added,
                        "cycle_token": cycle_token,
                        "target_generation_ref": target_generation_ref,
                    },
                },
            )
            status = transition["status"]
            target_generation_ref = transition.get("target_generation_ref")

    return AppliedPromotion(
        admission_id=str(admission_uuid),
        request_id=request_id,
        definition_id=request.lens_definition_id,
        deployment_name=request.deployment_name,
        producer_id=lens_producer_id(request.lens_definition_id),
        nodes_added=nodes_added,
        edges_added=edges_added,
        overlay_ids=tuple(overlay_ids),
        target_generation_ref=target_generation_ref,
        status=status,
    )


def _planned_image_value_failure(
    planned_mutations: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """The first planned mutation whose attrs carry a forged or malformed
    image value (#3046), as a rejection record, or None.

    `_materialize_rule` refuses these when the plan is made; this covers an
    envelope planned before that check existed. Every mutation is checked
    before any is written.
    """
    for index, raw in enumerate(planned_mutations):
        try:
            check_incoming(raw.get("attrs") or {})
        except (ImageRefInvalid, ImageValueForged) as exc:
            return {
                "code": exc.code,
                "message": exc.message,
                "mutation_index": index,
                "data": dict(exc.data),
            }
    return None


def _planned_overlay_coverage_failure(
    cur: psycopg.Cursor[Any],
    *,
    request_id: UUID,
    planned_overlay_ids: Sequence[UUID],
) -> dict[str, Any] | None:
    """Return a fail-closed verdict when a plan omits active overlay rows."""

    cur.execute(
        """
        SELECT overlay_id
          FROM okg.lens_overlay_log
         WHERE request_id = %(request_id)s::uuid
           AND status = 'active'
         ORDER BY overlay_id
         FOR UPDATE
        """,
        {"request_id": str(request_id)},
    )
    active_ids = {UUID(str(row[0])) for row in cur.fetchall()}
    planned_ids = set(planned_overlay_ids)
    duplicates: list[str] = []
    seen: set[UUID] = set()
    for overlay_id in planned_overlay_ids:
        if overlay_id in seen:
            duplicates.append(str(overlay_id))
        seen.add(overlay_id)
    missing = active_ids - planned_ids
    unexpected = planned_ids - active_ids
    if not missing and not unexpected and not duplicates:
        return None
    return {
        "code": "lens_promotion_overlay_plan_mismatch",
        "message": (
            "lens promotion plan must cover exactly the active overlay rows"
        ),
        "request_id": str(request_id),
        "active_overlay_ids": sorted(str(item) for item in active_ids),
        "planned_overlay_ids": sorted(str(item) for item in planned_ids),
        "missing_overlay_ids": sorted(str(item) for item in missing),
        "unexpected_overlay_ids": sorted(str(item) for item in unexpected),
        "duplicate_overlay_ids": sorted(duplicates),
    }


def list_pending_lens_envelopes(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    statuses: Sequence[str] = ("pending", "accepted"),
    branch_name: str | None = None,
) -> list[UUID]:
    """Return lens-promotion envelope ids awaiting application.

    ``branch_name=None`` returns every awaiting envelope across all
    branches (legacy caller behavior). A specific branch name returns
    only envelopes routed to that branch:

      * ``branch_name='default'`` matches both NULL and explicit
        ``'default'`` target_branch_name (the deployment-default
        semantic).
      * Any other ``branch_name`` matches exactly.

    This is the admission-side half of AKMON Phase B.2 quarantine
    routing: a publisher cycle running on ``branch_name=X`` only
    drains envelopes pinned to ``X``.
    """

    sql = """
        SELECT admission_id
          FROM okg.admission_envelopes
         WHERE deployment_name = %(deployment_name)s
           AND producer_kind = 'lens'
           AND status = ANY(%(statuses)s::text[])
    """
    params: dict[str, Any] = {
        "deployment_name": deployment_name,
        "statuses": list(statuses),
    }
    if branch_name is not None:
        if branch_name == "default":
            sql += (
                " AND (target_branch_name IS NULL "
                "OR target_branch_name = 'default')"
            )
        else:
            sql += " AND target_branch_name = %(branch_name)s"
            params["branch_name"] = branch_name
    sql += " ORDER BY created_at"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [row[0] for row in cur.fetchall()]


def _load_lens_envelope(
    cur: psycopg.Cursor[Any],
    admission_id: UUID,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT admission_id, deployment_name, producer_id, producer_kind,
               status, verdict
          FROM okg.admission_envelopes
         WHERE admission_id = %(admission_id)s::uuid
         FOR UPDATE
        """,
        {"admission_id": str(admission_id)},
    )
    row = cur.fetchone()
    if row is None:
        raise LensPromotionStateError(
            f"admission envelope not found: {admission_id}"
        )
    if row[3] != "lens":
        raise LensPromotionStateError(
            "envelope is not a lens-promotion envelope: "
            f"admission_id={admission_id} producer_kind={row[3]!r}"
        )
    return {
        "admission_id": str(row[0]),
        "deployment_name": row[1],
        "producer_id": row[2],
        "producer_kind": row[3],
        "status": row[4],
        "verdict": None if row[5] is None else dict(row[5]),
    }


def _insert_promoted_node_fact(
    cur: psycopg.Cursor[Any],
    *,
    mutation: Mapping[str, Any],
    admission_id: UUID,
    definition_id: str,
    request_id: UUID,
) -> bool:
    """Insert one promoted node_fact; return True iff a row was inserted."""

    subtype = str(mutation["subtype"])
    node_id = str(mutation["node_id"])
    attrs = dict(mutation.get("attrs") or {})
    overlay_id = str(mutation["overlay_id"])
    delta_id = str(mutation["delta_id"])
    field_path = list(mutation.get("field_path") or [])
    rule_id = str(mutation.get("rule_id") or "")

    source_record_id = {
        "lens_definition_id": definition_id,
        "lens_request_id": str(request_id),
        "overlay_id": overlay_id,
        "delta_id": delta_id,
        "field_path": field_path,
        "rule_id": rule_id,
    }
    source_revision = {
        "admission_id": str(admission_id),
        "kind": "lens_promotion",
    }
    dedupe_key = hashlib.sha256(
        json.dumps(
            {
                "source": lens_producer_id(definition_id),
                "source_record_id": source_record_id,
                "source_revision": source_revision,
                "op": "I",
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()

    cur.execute(
        """
        INSERT INTO okg.node_facts
          (node_id, subtype, attrs, source, source_record_id,
           source_revision, observed_at, dedupe_key, run_id, op)
        VALUES
          (%(node_id)s, %(subtype)s, %(attrs)s::jsonb,
           %(source)s, %(source_record_id)s::jsonb,
           %(source_revision)s::jsonb, now(), %(dedupe_key)s,
           %(run_id)s::uuid, 'I')
        ON CONFLICT (dedupe_key, observed_at) DO NOTHING
        RETURNING fact_id
        """,
        {
            "node_id": node_id,
            "subtype": subtype,
            "attrs": Jsonb(attrs),
            "source": lens_producer_id(definition_id),
            "source_record_id": Jsonb(source_record_id),
            "source_revision": Jsonb(source_revision),
            "dedupe_key": dedupe_key,
            "run_id": str(admission_id),
        },
    )
    row = cur.fetchone()
    return row is not None


def _flip_overlay_rows_to_promoted(
    cur: psycopg.Cursor[Any],
    *,
    overlay_ids: Sequence[UUID],
    admission_id: UUID,
) -> None:
    if not overlay_ids:
        return
    cur.execute(
        """
        UPDATE okg.lens_overlay_log
           SET status = 'promoted',
               promoted_admission_id = %(admission_id)s::uuid,
               promoted_at = now()
         WHERE overlay_id = ANY(%(overlay_ids)s::uuid[])
           AND status = 'active'
        """,
        {
            "admission_id": str(admission_id),
            "overlay_ids": [str(o) for o in overlay_ids],
        },
    )


def _mark_request_promoted(
    cur: psycopg.Cursor[Any],
    *,
    request_id: UUID,
) -> None:
    cur.execute(
        """
        UPDATE okg.lens_putback_requests
           SET status = 'promoted'
         WHERE request_id = %(request_id)s::uuid
           AND status IN ('committed_overlay', 'promoted')
        """,
        {"request_id": str(request_id)},
    )


def _result_from_existing(
    cur: psycopg.Cursor[Any],
    request: LensPutbackRequest,
    deployment_name: str | None,
) -> LensPromotionResult:
    """Return the previously-opened admission envelope for an idempotent retry."""

    producer_id = lens_producer_id(request.lens_definition_id)
    idempotency_key = stable_idempotency_key(
        "lens_promotion",
        request.deployment_name,
        producer_id,
        str(request.request_id),
    )
    cur.execute(
        """
        SELECT admission_id, status
          FROM okg.admission_envelopes
         WHERE deployment_name = %(deployment_name)s
           AND producer_id = %(producer_id)s
           AND idempotency_key = %(idempotency_key)s
        """,
        {
            "deployment_name": request.deployment_name,
            "producer_id": producer_id,
            "idempotency_key": idempotency_key,
        },
    )
    row = cur.fetchone()
    if row is None:
        raise LensPromotionStateError(
            f"request {request.request_id} is 'promoted' but no admission "
            "envelope was found — substrate is inconsistent"
        )
    return LensPromotionResult(
        admission_id=str(row[0]),
        status=str(row[1]),
        inserted=False,
        request_id=request.request_id,
        definition_id=request.lens_definition_id,
        deployment_name=request.deployment_name,
        producer_id=producer_id,
        planned_mutations=(),
    )
