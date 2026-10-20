"""Producer authority profiles and signature evaluation.

The producer profile is the normalized authority record used by source
admission and review-mode admission. Source registries and ad-hoc review
authorities can both produce the same profile shape; admission verdicts
then cite the profile fingerprint and the concrete policy/catalog clauses
that allowed or rejected the payload.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Sequence

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.errors import (
    SignatureGroupingInconsistent,
    SourceRunGroupingTimeout,
    SubstrateError,
)


PRODUCER_KINDS = frozenset({
    "source",
    "extractor",
    "enricher",
    "agent",
    "human",
    "workflow",
    "system",
    "lens",
})

ALLOWED_RECORD_KINDS = frozenset({"node", "edge"})

#: ``source_ref.profile_origin`` marking a profile row that no operator
#: declared -- one MINTED by a machine path (``apply_bounded_fact_batch``)
#: purely so a bounded write has a FactBatch header to cite.
#:
#: Such a row is authority BOOKKEEPING, never an authority DECLARATION, and
#: an unfingerprinted trust lookup must never resolve to it in preference to
#: what the deployment actually declared (issue #1545).
SYNTHETIC_PROFILE_ORIGIN = "bounded_fact_batch"

#: SQL predicate naming a synthetic row, for the ORDER BY and the
#: ``declared_only`` filter below. ONE definition, two readers.
#:
#: The second arm recognises rows minted BEFORE ``profile_origin`` existed.
#: ``apply_bounded_fact_batch`` wrote them with a ``source_ref`` of EXACTLY
#: ``{"workflow": ..., "producer_id": ...}``, so the arm requires exactly
#: those two keys and nothing else. An exact shape, not a containment test:
#: a declared profile that merely happens to carry both keys alongside its
#: own (a ``deployment_manifest`` reference, a registry name) is a real
#: declaration and must keep outranking machine rows. Without this arm a
#: deployment that has already minted a laundered row would keep resolving
#: to it forever.
_SYNTHETIC_PROFILE_SQL = """(
        COALESCE(source_ref->>'profile_origin', '') = 'bounded_fact_batch'
        OR (
          NOT source_ref ? 'profile_origin'
          AND source_ref ? 'workflow'
          AND source_ref ? 'producer_id'
          AND (SELECT count(*) FROM jsonb_object_keys(source_ref)) = 2
        )
      )"""

TRUST_LABEL_RANKS = {
    "untrusted": 0,
    "less-trusted": 1,
    "derived-compat": 1,
    "review-trusted": 2,
    "trusted": 3,
    "agent-derived-trusted": 3,
    "curated": 3,
    "external-source-trusted": 3,
    "fast-track-trusted": 3,
    "repo-local-trusted": 3,
    "fixture-trusted": 3,
    "poros-trusted": 3,
    "implicit_legacy_trusted": 3,
    "derived-deterministic-trusted": 3,
    # Honest classification for producers whose outputs depend on a model
    # call (spec §9.3, consistency-output-envelopes). Same lattice rank as
    # derived-deterministic-trusted — the relabel changes what admission
    # RECORDS, not what it permits; reuse decisions are what consume the
    # distinction (a nondeterministic producer's step is never skippable
    # without a recorded outcome).
    "model-derived-trusted": 3,
}

# NOTE (issue #1316): adding a member here is NOT a local change. The lattice
# is hashed by trust_lattice_policy_fingerprint() into EVERY producer's
# profile_fingerprint, so a purely additive label -- one that changes no
# existing producer's rank -- still invalidates every producer profile and
# forces a global re-materialization. Prefer reusing an accurate existing
# member over minting a new one unless the provenance distinction is worth
# that cost.

TRUST_LABELS = frozenset(TRUST_LABEL_RANKS)
TRUST_LATTICE_POLICY_VERSION = "okg.trust-label-lattice.v1"

NODE_SIGNATURE_KEYS = frozenset({
    "subtype",
    "id_prefix",
    "attrs",
    "attrs_exact",
    "attr_keys",
    "clause_id",
})

EDGE_SIGNATURE_KEYS = frozenset({
    "src_subtype",
    "edge_type",
    "dst_subtype",
    "src_id_prefix",
    "dst_id_prefix",
    "attrs",
    "attrs_exact",
    "attr_keys",
    "clause_id",
    "src_subtype_optional",
    "dst_subtype_optional",
})


def _json_detached_value(value: Any) -> Any:
    """Detach immutable registry values into JSON-native containers."""
    if isinstance(value, Mapping):
        return {
            key: _json_detached_value(item) for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [_json_detached_value(item) for item in value]
    if isinstance(value, set | frozenset):
        return [
            _json_detached_value(item)
            for item in sorted(value, key=repr)
        ]
    return value


@dataclass(frozen=True)
class ProducerProfile:
    deployment_name: str
    producer_id: str
    producer_kind: str
    trust_label: str
    admission_mode: str
    authority_scope: dict[str, Any]
    allowed_record_kinds: tuple[str, ...]
    output_signature: dict[str, Any]
    policy_clauses: tuple[dict[str, Any], ...]
    output_scope_summary: dict[str, Any]
    source_ref: dict[str, Any]
    catalog_version_id: int | None
    profile_fingerprint: str
    status: str

    def evidence(self) -> dict[str, Any]:
        return {
            "producer_id": self.producer_id,
            "profile_fingerprint": self.profile_fingerprint,
            "status": self.status,
            "catalog_version_id": self.catalog_version_id,
        }


class ProducerPolicyViolation(Exception):
    """Raised when a payload or fact range violates its profile."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        verdict: Mapping[str, Any],
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.verdict = dict(verdict)


def normalize_trust_label(value: str, *, field: str = "trust_label") -> str:
    label = str(value or "").strip().lower()
    if label not in TRUST_LABEL_RANKS:
        raise ValueError(
            f"{field} must be one of {sorted(TRUST_LABELS)}, got {value!r}"
        )
    return label


def trust_lattice_policy_fingerprint(
    trust_label_ranks: Mapping[str, int] | None = None,
) -> str:
    ranks = trust_label_ranks or TRUST_LABEL_RANKS
    payload = {
        "version": TRUST_LATTICE_POLICY_VERSION,
        "trust_label_ranks": {
            str(label): int(rank)
            for label, rank in sorted(ranks.items())
        },
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def compare_trust_labels(left: str, right: str) -> int:
    left_rank = TRUST_LABEL_RANKS[normalize_trust_label(left, field="left")]
    right_rank = TRUST_LABEL_RANKS[normalize_trust_label(right, field="right")]
    return (left_rank > right_rank) - (left_rank < right_rank)


def least_trusted_label(labels: Sequence[str]) -> str:
    if not labels:
        raise ValueError("at least one trust label is required")
    normalized = [
        normalize_trust_label(label, field="trust_label") for label in labels
    ]
    return min(normalized, key=lambda label: TRUST_LABEL_RANKS[label])


def trust_label_allows_output(
    *,
    output_label: str,
    input_labels: Sequence[str],
) -> bool:
    least_input = least_trusted_label(input_labels)
    return compare_trust_labels(output_label, least_input) <= 0


def evaluate_derived_output_trust(
    *,
    profile: ProducerProfile,
    input_trust_labels: Sequence[str],
    input_sources: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    try:
        output_label = normalize_trust_label(
            profile.trust_label,
            field="profile.trust_label",
        )
        least_input = (
            least_trusted_label(input_trust_labels)
            if input_trust_labels else None
        )
    except ValueError as exc:
        raise ProducerPolicyViolation(
            code="unknown_trust_label",
            message=str(exc),
            verdict={
                "code": "unknown_trust_label",
                "message": str(exc),
                "producer_policy": {
                    **profile.evidence(),
                    "status": "rejected",
                    "trust_lattice": {
                        "status": "rejected",
                        "reason": "unknown_trust_label",
                        "output_trust_label": profile.trust_label,
                        "input_sources": [
                            dict(source) for source in input_sources
                        ],
                    },
                },
            },
        ) from exc

    trust_lattice = {
        "status": "rejected",
        "reason": "no_profiled_inputs",
        "output_trust_label": output_label,
        "input_sources": [dict(source) for source in input_sources],
    }
    if least_input is None:
        raise ProducerPolicyViolation(
            code="derived_output_trust_inputs_missing",
            message="derived output has no discoverable input trust",
            verdict={
                "code": "derived_output_trust_inputs_missing",
                "message": "derived output has no discoverable input trust",
                "producer_policy": {
                    **profile.evidence(),
                    "status": "rejected",
                    "trust_lattice": trust_lattice,
                },
            },
        )

    trust_lattice = {
        **trust_lattice,
        "status": "matched",
        "reason": "output_not_more_trusted_than_inputs",
        "input_trust_labels": list(input_trust_labels),
        "least_input_trust_label": least_input,
    }
    if compare_trust_labels(output_label, least_input) > 0:
        trust_lattice = {
            **trust_lattice,
            "status": "rejected",
            "reason": "output_trust_exceeds_input",
        }
        raise ProducerPolicyViolation(
            code="derived_output_trust_exceeds_input",
            message=(
                "derived output trust label is higher than the least "
                "trusted input label"
            ),
            verdict={
                "code": "derived_output_trust_exceeds_input",
                "message": (
                    "derived output trust label is higher than the least "
                    "trusted input label"
                ),
                "producer_policy": {
                    **profile.evidence(),
                    "status": "rejected",
                    "trust_lattice": trust_lattice,
                },
            },
        )
    return {
        "producer_policy": {
            **profile.evidence(),
            "trust_lattice": trust_lattice,
        },
    }


def normalize_authority_scope(
    value: Any,
    *,
    source_name: str,
) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(
            f"source {source_name!r}: "
            "admission_policy.authority_scope must be a mapping, "
            f"got {type(value).__name__}"
        )
    return dict(value)


def normalize_output_signature(
    value: Any,
    *,
    source_name: str,
    strict: bool = False,
) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(
            f"source {source_name!r}: "
            "admission_policy.output_signature must be a mapping, "
            f"got {type(value).__name__}"
        )
    allowed = {"nodes", "edges"}
    extras = sorted(set(value) - allowed)
    if extras:
        raise ValueError(
            f"source {source_name!r}: "
            "admission_policy.output_signature has unknown keys "
            f"{extras!r}"
        )
    for field in allowed:
        if strict and field in value and value[field] is None:
            raise ValueError(
                f"source {source_name!r}: admission_policy."
                f"output_signature.{field} must not be null"
            )
    nodes = _normalize_signature_list(
        value.get("nodes"),
        source_name=source_name,
        field="nodes",
        required_keys=("subtype",),
        allowed_keys=NODE_SIGNATURE_KEYS,
        strict=strict,
    )
    edges = _normalize_signature_list(
        value.get("edges"),
        source_name=source_name,
        field="edges",
        required_keys=("src_subtype", "edge_type", "dst_subtype"),
        allowed_keys=EDGE_SIGNATURE_KEYS,
        strict=strict,
    )
    out: dict[str, Any] = {}
    if nodes:
        out["nodes"] = nodes
    if edges:
        out["edges"] = edges
    return out


ENVELOPE_UNBOUNDED_DIMENSIONS = frozenset({
    "nodes", "edges", "claim_keys", "values",
})

_ENVELOPE_KEYS = frozenset({
    "nodes", "edges", "claim_keys", "value_vocabularies",
    "unbounded", "nondeterministic",
})


@dataclass(frozen=True)
class WriteEnvelope:
    """A producer's declared write surface (spec §9.3; pinned name).

    Extends the admission ``output_signature`` (nodes + edge triples) with
    the dimensions reuse analysis needs: the claim keys / correction forms
    the producer may write, closed value vocabularies where they exist, an
    explicit ``unbounded`` marker per dimension (an unbounded dimension can
    never be certified skippable — the step runs), and the declared
    ``nondeterministic`` classification (outputs depend on a model call).
    Declaration is mandatory where enforced; observed history never
    substitutes for it.
    """

    signature: dict[str, Any]              # {"nodes": [...], "edges": [...]}
    claim_keys: tuple[str, ...]
    value_vocabularies: dict[str, tuple[str, ...]]
    unbounded: tuple[str, ...]             # subset of ENVELOPE_UNBOUNDED_DIMENSIONS
    nondeterministic: bool

    def summary(self) -> dict[str, Any]:
        """JSON-shaped form for profiles, evidence, and scope summaries."""
        return {
            "nodes": list(self.signature.get("nodes") or []),
            "edges": list(self.signature.get("edges") or []),
            "claim_keys": list(self.claim_keys),
            "value_vocabularies": {
                k: list(v) for k, v in sorted(self.value_vocabularies.items())
            },
            "unbounded": list(self.unbounded),
            "nondeterministic": self.nondeterministic,
        }


def normalize_write_envelope(
    value: Any,
    *,
    source_name: str,
    strict: bool = False,
) -> WriteEnvelope:
    """Normalize a declared write envelope, loudly.

    ``value`` is the raw mapping a producer declares. Unknown keys refuse;
    an ``unbounded`` entry outside the known dimensions refuses; vocabulary
    values must be non-empty lists of strings. ``None`` is NOT a valid
    envelope — a producer with no declaration fails at its boot validator,
    never silently here.
    """
    if not isinstance(value, Mapping):
        raise ValueError(
            f"producer {source_name!r}: write_envelope must be a mapping "
            f"(got {type(value).__name__}); a producer without an envelope "
            f"declaration cannot be admitted"
        )
    extras = sorted(set(value) - _ENVELOPE_KEYS)
    if extras:
        raise ValueError(
            f"producer {source_name!r}: write_envelope has unknown keys "
            f"{extras!r} (allowed: {sorted(_ENVELOPE_KEYS)})"
        )
    for key in _ENVELOPE_KEYS:
        if strict and key in value and value[key] is None:
            raise ValueError(
                f"producer {source_name!r}: write_envelope.{key} "
                "must not be null"
            )
    signature_values = (
        {
            key: value[key]
            for key in ("nodes", "edges")
            if key in value
        }
        if strict
        else {
            key: item
            for key, item in (
                ("nodes", value.get("nodes")),
                ("edges", value.get("edges")),
            )
            if item
        }
    )
    signature = normalize_output_signature(
        signature_values,
        source_name=source_name,
        strict=strict,
    )
    raw_claim_keys = (
        value.get("claim_keys", [])
        if strict else value.get("claim_keys") or []
    )
    if not isinstance(raw_claim_keys, (list, tuple)) or not all(
        isinstance(x, str) and x for x in raw_claim_keys
    ):
        raise ValueError(
            f"producer {source_name!r}: write_envelope.claim_keys must be "
            f"a list of non-empty strings, got {raw_claim_keys!r}"
        )
    raw_vocab = (
        value.get("value_vocabularies", {})
        if strict else value.get("value_vocabularies") or {}
    )
    if not isinstance(raw_vocab, Mapping):
        raise ValueError(
            f"producer {source_name!r}: write_envelope.value_vocabularies "
            f"must be a mapping of field -> closed value list"
        )
    vocabularies: dict[str, tuple[str, ...]] = {}
    for field, values in raw_vocab.items():
        if not isinstance(values, (list, tuple, set, frozenset)) or not values \
                or not all(isinstance(x, str) for x in values):
            raise ValueError(
                f"producer {source_name!r}: write_envelope."
                f"value_vocabularies[{field!r}] must be a non-empty "
                f"list of strings"
            )
        vocabularies[str(field)] = tuple(sorted(values))
    raw_unbounded = (
        value.get("unbounded", [])
        if strict else value.get("unbounded") or []
    )
    if not isinstance(raw_unbounded, (list, tuple)) or (
        strict and not all(isinstance(item, str) for item in raw_unbounded)
    ):
        raise ValueError(
            f"producer {source_name!r}: write_envelope.unbounded must be a "
            f"list of dimension names"
        )
    invalid = sorted(set(raw_unbounded) - ENVELOPE_UNBOUNDED_DIMENSIONS)
    if invalid:
        raise ValueError(
            f"producer {source_name!r}: write_envelope.unbounded names "
            f"unknown dimensions {invalid!r} "
            f"(allowed: {sorted(ENVELOPE_UNBOUNDED_DIMENSIONS)})"
        )
    nondeterministic = value.get("nondeterministic", False)
    if not isinstance(nondeterministic, bool):
        raise ValueError(
            f"producer {source_name!r}: write_envelope.nondeterministic "
            f"must be a bool, got {nondeterministic!r}"
        )
    return WriteEnvelope(
        signature=signature,
        claim_keys=tuple(raw_claim_keys),
        value_vocabularies=vocabularies,
        unbounded=tuple(dict.fromkeys(str(d) for d in raw_unbounded)),
        nondeterministic=nondeterministic,
    )


# Envelope-class rejection codes: a well-typed output outside the producer's
# finite envelope (spec §9.3). Distinct from operator failures (type-invalid
# output) and from catalog narrowing violations (graph-shape law).
ENVELOPE_VIOLATION_CODES = frozenset({
    "producer_record_kind_not_allowed",
    "producer_node_subtype_not_allowed",
    "producer_edge_triple_not_allowed",
    "producer_edge_endpoint_missing",
    "envelope_vocabulary_violation",
})

# An edge whose endpoint node does not exist is a DIFFERENT defect from an
# edge whose declared triple is outside the producer's signature, and issue
# #1330 is what a shared code costs: one dangling edge refused a 111,940-record
# batch under the message "edge record does not match producer signature",
# sending the operator to audit a signature that was correct. The endpoint
# subtype resolves to None only when the node is absent from the graph (and
# from the same-cycle node-fact window), so the two are separable at the point
# of refusal. The CONSEQUENCE is deliberately unchanged: this code stays in
# ENVELOPE_VIOLATION_CODES, so the run is still deny-listed and the violation
# still recorded. Only the diagnosis is renamed.
EDGE_ENDPOINT_MISSING_CODE = "producer_edge_endpoint_missing"


def profile_write_envelope(
    profile: "ProducerProfile",
) -> Mapping[str, Any] | None:
    """The persisted WriteEnvelope summary a profile carries, if any.

    Wave-1 profiles persist the envelope under
    ``output_scope_summary["write_envelope"]`` (the summary block is the
    profile's free-form audit surface); absence means the producer predates
    envelope declarations — evaluation then covers only the signature
    dimensions.
    """
    summary = profile.output_scope_summary or {}
    envelope = summary.get("write_envelope")
    return envelope if isinstance(envelope, Mapping) else None


def record_envelope_violation(
    conn: psycopg.Connection,
    *,
    deployment: str,
    generation_id: int | str | None,
    producer_id: str,
    code: str,
    record_kind: str | None,
    detail: Mapping[str, Any],
) -> None:
    """Persist one envelope violation (spec §9.3).

    The row is the poisoning substrate: ``poisons_reuse`` is TRUE by
    construction — certificate validation (consistency-resolution-aware-reuse)
    must refuse any certificate whose producer has a violation row at or
    after the certificate's recorded generation. Never widens anything;
    recording is additive audit state.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.envelope_violations
              (deployment_name, generation_id, producer_id, code, record_kind,
               detail, poisons_reuse)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE)
            """,
            (deployment,
             None if generation_id is None else str(generation_id),
             producer_id, code, record_kind, Jsonb(dict(detail))),
        )


def summarize_envelope_violations(
    conn: psycopg.Connection,
    *,
    deployment: str,
    generation_id: int | str | None = None,
) -> dict[str, Any]:
    """Queryable per-generation / per-producer violation summary."""
    clauses = ["deployment_name = %s"]
    params: list[Any] = [deployment]
    if generation_id is not None:
        clauses.append("generation_id = %s")
        params.append(str(generation_id))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT producer_id, code, count(*), bool_or(poisons_reuse)
              FROM okg.envelope_violations
             WHERE {' AND '.join(clauses)}
             GROUP BY producer_id, code
             ORDER BY producer_id, code
            """,
            params,
        )
        rows = cur.fetchall()
    return {
        "count": sum(int(row[2]) for row in rows),
        "poisons_reuse": any(bool(row[3]) for row in rows),
        "by_producer": [
            {
                "producer_id": str(row[0]),
                "code": str(row[1]),
                "count": int(row[2]),
                "poisons_reuse": bool(row[3]),
            }
            for row in rows
        ],
    }


def normalize_allowed_record_kinds(
    value: Any,
    *,
    output_signature: Mapping[str, Any],
    source_name: str,
) -> tuple[str, ...]:
    if value is None:
        inferred: list[str] = []
        if output_signature.get("nodes"):
            inferred.append("node")
        if output_signature.get("edges"):
            inferred.append("edge")
        return tuple(inferred)
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(item, str) for item in value
    ):
        raise ValueError(
            f"source {source_name!r}: "
            "admission_policy.allowed_record_kinds must be a list of "
            f"strings, got {value!r}"
        )
    normalized = tuple(dict.fromkeys(str(item) for item in value))
    invalid = sorted(set(normalized) - ALLOWED_RECORD_KINDS)
    if invalid:
        raise ValueError(
            f"source {source_name!r}: "
            "admission_policy.allowed_record_kinds contains unsupported "
            f"kinds {invalid!r}"
        )
    return normalized


def normalize_policy_clauses(
    value: Any,
    *,
    source_name: str,
    strict: bool = False,
) -> tuple[dict[str, Any], ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ValueError(
            f"source {source_name!r}: "
            "admission_policy.policy_clauses must be a list of mappings, "
            f"got {type(value).__name__}"
        )
    clauses: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.policy_clauses[{index}] must be a "
                f"mapping, got {type(raw).__name__}"
            )
        clause = dict(raw)
        clause_id_raw = clause.get("id")
        if strict and (
            not isinstance(clause_id_raw, str) or not clause_id_raw.strip()
        ):
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.policy_clauses[{index}].id is required"
            )
        clause_id = (
            clause_id_raw.strip()
            if strict else str(clause_id_raw or "").strip()
        )
        if not clause_id:
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.policy_clauses[{index}].id is required"
            )
        if clause_id in seen:
            raise ValueError(
                f"source {source_name!r}: duplicate policy clause id "
                f"{clause_id!r}"
            )
        seen.add(clause_id)
        effect = (
            clause.get("effect", "allow")
            if strict else str(clause.get("effect") or "allow")
        )
        if effect != "allow":
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.policy_clauses[{index}].effect must be "
                f"'allow', got {effect!r}"
            )
        record_kind = (
            clause.get("record_kind")
            if strict else str(clause.get("record_kind") or "")
        )
        if record_kind not in ALLOWED_RECORD_KINDS:
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.policy_clauses[{index}].record_kind "
                f"must be one of {sorted(ALLOWED_RECORD_KINDS)}, got "
                f"{record_kind!r}"
            )
        if strict and "subtype" in clause and (
            not isinstance(clause["subtype"], str)
            or not clause["subtype"].strip()
        ):
            raise ValueError(
                f"source {source_name!r}: admission_policy."
                f"policy_clauses[{index}].subtype must be a non-empty string"
            )
        if strict and "triple" in clause:
            triple = clause["triple"]
            if not isinstance(triple, list) or len(triple) != 3 or not all(
                isinstance(item, str) and bool(item) for item in triple
            ):
                raise ValueError(
                    f"source {source_name!r}: admission_policy."
                    f"policy_clauses[{index}].triple must be "
                    "[src_subtype, edge_type, dst_subtype]"
                )
        if record_kind == "node" and (
            (
                not isinstance(clause.get("subtype"), str)
                or not clause["subtype"].strip()
            )
            if strict else not clause.get("subtype")
        ):
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.policy_clauses[{index}].subtype is "
                "required for node clauses"
            )
        if record_kind == "edge":
            triple = clause.get("triple")
            if not isinstance(triple, list) or len(triple) != 3 or not all(
                isinstance(item, str) and (bool(item) if strict else True)
                for item in triple
            ):
                raise ValueError(
                    f"source {source_name!r}: "
                    f"admission_policy.policy_clauses[{index}].triple "
                    "must be [src_subtype, edge_type, dst_subtype]"
                )
        clauses.append(clause)
    return tuple(clauses)


def _signature_subtype_keys(
    output_signature: Mapping[str, Any],
) -> set[str]:
    """The node-subtype + edge-triple identity keys an output_signature
    declares, in the same `"<a> <edge_type> <b>"` form
    output_scope_summary uses for edges and bare subtype names for nodes."""
    keys: set[str] = set()
    for node in output_signature.get("nodes") or []:
        if isinstance(node, Mapping):
            subtype = node.get("subtype")
            if subtype:
                keys.add(f"node:{subtype}")
    for edge in output_signature.get("edges") or []:
        if isinstance(edge, Mapping):
            src = edge.get("src_subtype")
            etype = edge.get("edge_type")
            dst = edge.get("dst_subtype")
            if src and etype and dst:
                keys.add(f"edge:{src} {etype} {dst}")
    return keys


def _scope_summary_subtype_keys(
    output_scope_summary: Mapping[str, Any],
) -> set[str]:
    """The same identity keys, parsed from the human-readable
    output_scope_summary block (node names + `"<a> <edge_type> <b>"`
    edge phrases)."""
    keys: set[str] = set()
    for node in output_scope_summary.get("nodes") or []:
        if isinstance(node, str) and node.strip():
            keys.add(f"node:{node.strip()}")
    for edge in output_scope_summary.get("edges") or []:
        if isinstance(edge, str):
            parts = edge.split()
            if len(parts) == 3:
                keys.add(f"edge:{parts[0]} {parts[1]} {parts[2]}")
    return keys


def _signature_clause_ids(output_signature: Mapping[str, Any]) -> set[str]:
    """Every clause_id referenced by an output_signature's nodes/edges."""
    ids: set[str] = set()
    for record in (
        *(output_signature.get("nodes") or []),
        *(output_signature.get("edges") or []),
    ):
        if isinstance(record, Mapping):
            clause_id = record.get("clause_id")
            if clause_id:
                ids.add(str(clause_id))
    return ids


def check_admission_policy_block_consistency(
    admission_policy: Mapping[str, Any],
    *,
    source_name: str,
) -> list[str]:
    """Cross-block consistency for one source's admission_policy.

    The three blocks declare the same subtype/triple set three times:
      - output_signature.nodes/edges carries clause_id (source of truth),
      - policy_clauses (id/effect/triple) drives admission,
      - output_scope_summary.nodes/edges is the human-readable copy.
    They drift silently. This returns a list of human-readable problems
    when, for the source:
      (a) the output_signature clause_ids != policy_clauses ids, OR
      (b) the output_scope_summary subtype/triple set !=
          the output_signature subtype/triple set.
    An empty list means the blocks agree."""
    if not isinstance(admission_policy, Mapping):
        return []
    output_signature = admission_policy.get("output_signature") or {}
    policy_clauses = admission_policy.get("policy_clauses") or []
    output_scope_summary = admission_policy.get("output_scope_summary") or {}
    if not isinstance(output_signature, Mapping):
        output_signature = {}
    if not isinstance(output_scope_summary, Mapping):
        output_scope_summary = {}

    problems: list[str] = []

    sig_clause_ids = _signature_clause_ids(output_signature)
    clause_ids = {
        str(clause.get("id"))
        for clause in policy_clauses
        if isinstance(clause, Mapping) and clause.get("id")
    }
    if sig_clause_ids or clause_ids:
        only_signature = sorted(sig_clause_ids - clause_ids)
        only_clauses = sorted(clause_ids - sig_clause_ids)
        if only_signature or only_clauses:
            problems.append(
                f"source {source_name!r}: output_signature clause_ids "
                f"disagree with policy_clauses ids "
                f"(only in output_signature: {only_signature}; "
                f"only in policy_clauses: {only_clauses})"
            )

    sig_keys = _signature_subtype_keys(output_signature)
    summary_keys = _scope_summary_subtype_keys(output_scope_summary)
    if sig_keys or summary_keys:
        only_sig = sorted(sig_keys - summary_keys)
        only_summary = sorted(summary_keys - sig_keys)
        if only_sig or only_summary:
            problems.append(
                f"source {source_name!r}: output_scope_summary subtype/triple "
                f"set disagrees with output_signature "
                f"(only in output_signature: {only_sig}; "
                f"only in output_scope_summary: {only_summary})"
            )
    return problems


def profile_fingerprint(
    *,
    deployment_name: str,
    producer_id: str,
    producer_kind: str,
    trust_label: str,
    admission_mode: str,
    authority_scope: Mapping[str, Any],
    allowed_record_kinds: Sequence[str],
    output_signature: Mapping[str, Any],
    policy_clauses: Sequence[Mapping[str, Any]],
    catalog_version_id: int | None = None,
    trust_policy: Mapping[str, Any] | None = None,
) -> str:
    """Compute a stable fingerprint for a producer's authority.

    Per `fix-akmon-design-mistakes` task 2: the fingerprint
    covers the producer's authority **signature** —
    `(deployment_name, producer_id, producer_kind, trust_label,
    admission_mode, authority_scope, allowed_record_kinds,
    output_signature, policy_clauses)`. It deliberately does NOT
    incorporate `catalog_version_id`, so the same producer's
    signature produces the same fingerprint across catalog
    evolutions that don't touch any clause this producer cares
    about. The current catalog version lives as a separate
    column on `okg.producer_profiles` and is updated in place
    when the producer is re-materialized against a new catalog.

    The `catalog_version_id` parameter is preserved for
    backward-compat with existing callers but is no longer
    hashed.
    """

    del catalog_version_id  # intentionally unhashed; see docstring
    normalized_trust_label = normalize_trust_label(trust_label)
    payload = {
        "deployment_name": deployment_name,
        "producer_id": producer_id,
        "producer_kind": producer_kind,
        "trust_label": normalized_trust_label,
        "admission_mode": admission_mode,
        "authority_scope": _json_detached_value(authority_scope),
        "allowed_record_kinds": list(allowed_record_kinds),
        "output_signature": _json_detached_value(output_signature),
        "policy_clauses": _json_detached_value(policy_clauses),
        "trust_lattice_policy": _json_detached_value(trust_policy or {
            "version": TRUST_LATTICE_POLICY_VERSION,
            "fingerprint": trust_lattice_policy_fingerprint(),
        }),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def materialize_producer_profile(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    producer_id: str,
    producer_kind: str,
    trust_label: str,
    admission_mode: str,
    authority_scope: Mapping[str, Any],
    allowed_record_kinds: Sequence[str],
    output_signature: Mapping[str, Any],
    policy_clauses: Sequence[Mapping[str, Any]],
    output_scope_summary: Mapping[str, Any],
    source_ref: Mapping[str, Any],
    catalog_version_id: int | None,
    profile_origin: str | None = None,
) -> ProducerProfile:
    """Upsert one producer profile row.

    ``profile_origin`` stamps ``source_ref.profile_origin``. Pass
    :data:`SYNTHETIC_PROFILE_ORIGIN` when the caller is minting a row for its
    own bookkeeping rather than recording a declared authority; such a row is
    ranked BELOW every declared row by :func:`load_profile_by_fingerprint`
    and is excluded outright under ``declared_only`` (issue #1545).
    """
    normalized_authority_scope = _json_detached_value(authority_scope)
    normalized_signature = _json_detached_value(output_signature)
    normalized_policy_clauses = _json_detached_value(policy_clauses)
    normalized_output_scope_summary = _json_detached_value(
        output_scope_summary
    )
    normalized_source_ref = _json_detached_value(source_ref)
    if profile_origin is not None:
        normalized_source_ref = {
            **dict(normalized_source_ref or {}),
            "profile_origin": str(profile_origin),
        }
    normalized_trust_label = normalize_trust_label(trust_label)
    status = "active" if normalized_signature else "compat"
    catalog_evidence = validate_signature_catalog(
        conn,
        catalog_version_id=catalog_version_id,
        output_signature=normalized_signature,
    )
    fingerprint = profile_fingerprint(
        deployment_name=deployment_name,
        producer_id=producer_id,
        producer_kind=producer_kind,
        trust_label=normalized_trust_label,
        admission_mode=admission_mode,
        authority_scope=normalized_authority_scope,
        allowed_record_kinds=allowed_record_kinds,
        output_signature=normalized_signature,
        policy_clauses=normalized_policy_clauses,
        catalog_version_id=catalog_version_id,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.producer_profiles
              (deployment_name, producer_id, producer_kind, trust_label,
               admission_mode, authority_scope, allowed_record_kinds,
               output_signature, policy_clauses, output_scope_summary,
               source_ref, catalog_version_id, profile_fingerprint,
               status, catalog_evidence)
            VALUES
              (%(deployment_name)s, %(producer_id)s, %(producer_kind)s,
               %(trust_label)s, %(admission_mode)s,
               %(authority_scope)s, %(allowed_record_kinds)s,
               %(output_signature)s, %(policy_clauses)s,
               %(output_scope_summary)s, %(source_ref)s,
               %(catalog_version_id)s, %(profile_fingerprint)s,
               %(status)s, %(catalog_evidence)s)
            ON CONFLICT (
              deployment_name, producer_id, profile_fingerprint
            ) DO UPDATE
               SET trust_label = EXCLUDED.trust_label,
                   admission_mode = EXCLUDED.admission_mode,
                   authority_scope = EXCLUDED.authority_scope,
                   allowed_record_kinds = EXCLUDED.allowed_record_kinds,
                   output_signature = EXCLUDED.output_signature,
                   policy_clauses = EXCLUDED.policy_clauses,
                   output_scope_summary = EXCLUDED.output_scope_summary,
                   source_ref = EXCLUDED.source_ref,
                   catalog_version_id = EXCLUDED.catalog_version_id,
                   status = EXCLUDED.status,
                   catalog_evidence = EXCLUDED.catalog_evidence,
                   updated_at = now()
            """,
            {
                "deployment_name": deployment_name,
                "producer_id": producer_id,
                "producer_kind": producer_kind,
                "trust_label": normalized_trust_label,
                "admission_mode": admission_mode,
                "authority_scope": Jsonb(normalized_authority_scope),
                "allowed_record_kinds": list(allowed_record_kinds),
                "output_signature": Jsonb(normalized_signature),
                "policy_clauses": Jsonb(normalized_policy_clauses),
                "output_scope_summary": Jsonb(
                    normalized_output_scope_summary
                ),
                "source_ref": Jsonb(normalized_source_ref),
                "catalog_version_id": catalog_version_id,
                "profile_fingerprint": fingerprint,
                "status": status,
                "catalog_evidence": Jsonb(catalog_evidence),
            },
        )
    return ProducerProfile(
        deployment_name=deployment_name,
        producer_id=producer_id,
        producer_kind=producer_kind,
        trust_label=normalized_trust_label,
        admission_mode=admission_mode,
        authority_scope=normalized_authority_scope,
        allowed_record_kinds=tuple(allowed_record_kinds),
        output_signature=normalized_signature,
        policy_clauses=tuple(normalized_policy_clauses),
        output_scope_summary=normalized_output_scope_summary,
        source_ref=normalized_source_ref,
        catalog_version_id=catalog_version_id,
        profile_fingerprint=fingerprint,
        status=status,
    )


def load_profile_by_fingerprint(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    producer_id: str,
    profile_fingerprint: str | None,
    declared_only: bool = False,
) -> ProducerProfile:
    """Resolve one producer profile, by fingerprint or by ranking.

    The unfingerprinted arm answers "what authority does this producer
    have?", so it must not answer with a row the deployment never
    declared. Machine-minted rows (see :data:`SYNTHETIC_PROFILE_ORIGIN`)
    are ranked last, ahead of the old ``active``-then-``updated_at DESC``
    ordering: a producer whose declared profile is ``compat`` used to be
    overtaken by whichever synthetic row was written most recently, which
    let a bounded write mint a trust label its producer never earned and
    then read it back as if it had (issue #1545).

    ``declared_only`` drops synthetic rows from the candidate set
    altogether and raises ``producer_profile_missing`` when nothing
    declared remains -- the fail-closed answer for a caller asking what
    trust an operator granted.
    """
    with conn.cursor() as cur:
        if profile_fingerprint:
            # declared_only FILTERS here too. Accepting the argument and then
            # ignoring it on this arm would make a caller asking for declared
            # authority silently receive a machine-minted row whenever it
            # happened to know the fingerprint -- the exact confusion this
            # argument exists to remove.
            cur.execute(
                f"""
                SELECT deployment_name, producer_id, producer_kind,
                       trust_label, admission_mode, authority_scope,
                       allowed_record_kinds, output_signature,
                       policy_clauses, output_scope_summary, source_ref,
                       catalog_version_id, profile_fingerprint, status
                  FROM okg.producer_profiles
                 WHERE deployment_name = %s
                   AND producer_id = %s
                   AND profile_fingerprint = %s
                   AND (
                     NOT %s::boolean
                     OR NOT {_SYNTHETIC_PROFILE_SQL}
                   )
                """,
                (
                    deployment_name,
                    producer_id,
                    profile_fingerprint,
                    bool(declared_only),
                ),
            )
        else:
            cur.execute(
                f"""
                SELECT deployment_name, producer_id, producer_kind,
                       trust_label, admission_mode, authority_scope,
                       allowed_record_kinds, output_signature,
                       policy_clauses, output_scope_summary, source_ref,
                       catalog_version_id, profile_fingerprint, status
                 FROM okg.producer_profiles
                 WHERE deployment_name = %s
                   AND producer_id = %s
                   AND status IN ('active', 'compat')
                   AND (
                     NOT %s::boolean
                     OR NOT {_SYNTHETIC_PROFILE_SQL}
                   )
                 ORDER BY
                   CASE WHEN {_SYNTHETIC_PROFILE_SQL} THEN 1 ELSE 0 END,
                   CASE status WHEN 'active' THEN 0 ELSE 1 END,
                   updated_at DESC
                 LIMIT 1
                """,
                (deployment_name, producer_id, bool(declared_only)),
            )
        row = cur.fetchone()
    if row is None:
        raise ProducerPolicyViolation(
            code="producer_profile_missing",
            message="producer profile not found",
            verdict={
                "code": "producer_profile_missing",
                "producer_policy": {
                    "producer_id": producer_id,
                    "profile_fingerprint": profile_fingerprint,
                    "status": "missing",
                },
            },
        )
    try:
        trust_label = normalize_trust_label(
            str(row[3]),
            field="profile.trust_label",
        )
    except ValueError as exc:
        raise ProducerPolicyViolation(
            code="unknown_trust_label",
            message=str(exc),
            verdict={
                "code": "unknown_trust_label",
                "message": str(exc),
                "producer_policy": {
                    "producer_id": producer_id,
                    "profile_fingerprint": profile_fingerprint,
                    "status": "rejected",
                    "trust_label": str(row[3]),
                },
            },
        ) from exc
    return ProducerProfile(
        deployment_name=str(row[0]),
        producer_id=str(row[1]),
        producer_kind=str(row[2]),
        trust_label=trust_label,
        admission_mode=str(row[4]),
        authority_scope=dict(row[5] or {}),
        allowed_record_kinds=tuple(row[6] or ()),
        output_signature=dict(row[7] or {}),
        policy_clauses=tuple(dict(clause) for clause in (row[8] or ())),
        output_scope_summary=dict(row[9] or {}),
        source_ref=dict(row[10] or {}),
        catalog_version_id=int(row[11]) if row[11] is not None else None,
        profile_fingerprint=str(row[12]),
        status=str(row[13]),
    )


def producer_id_candidates(producer_id: str) -> tuple[str, ...]:
    """Return exact/family producer ids for authority lookup.

    Declarative enrichers emit fact sources such as
    ``_enricher:<name>:<rule_id>``. A deployment may declare either a
    rule-level profile or a parent ``_enricher:<name>`` profile. The
    exact id always wins.
    """
    normalized = str(producer_id)
    candidates = [normalized]
    if normalized.startswith("_enricher:") and normalized.count(":") >= 2:
        parts = normalized.split(":")
        parent = ":".join(parts[:2])
        if parent != normalized:
            candidates.append(parent)
    return tuple(dict.fromkeys(candidates))


def load_profile_for_producer(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    producer_id: str,
    profile_fingerprint: str | None = None,
    allow_source_family_fallback: bool = False,
    declared_only: bool = False,
) -> tuple[ProducerProfile, dict[str, Any]]:
    """Load a producer profile with optional source-family fallback.

    ``declared_only`` is forwarded to :func:`load_profile_by_fingerprint`.
    """
    candidates = (
        producer_id_candidates(producer_id)
        if allow_source_family_fallback
        else (str(producer_id),)
    )
    last_missing: ProducerPolicyViolation | None = None
    for candidate in candidates:
        try:
            profile = load_profile_by_fingerprint(
                conn,
                deployment_name=deployment_name,
                producer_id=candidate,
                profile_fingerprint=(
                    profile_fingerprint if candidate == producer_id else None
                ),
                declared_only=declared_only,
            )
            return profile, {
                "requested_producer_id": str(producer_id),
                "resolved_producer_id": profile.producer_id,
                "source_family_fallback": candidate != str(producer_id),
                "declared_only": bool(declared_only),
            }
        except ProducerPolicyViolation as exc:
            if exc.code != "producer_profile_missing":
                raise
            last_missing = exc
            continue
    if last_missing is not None:
        raise last_missing
    raise ProducerPolicyViolation(
        code="producer_profile_missing",
        message="producer profile not found",
        verdict={
            "code": "producer_profile_missing",
            "producer_policy": {
                "producer_id": producer_id,
                "profile_fingerprint": profile_fingerprint,
                "status": "missing",
            },
        },
    )


def validate_signature_catalog(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int | None,
    output_signature: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = {
        "catalog_version_id": catalog_version_id,
        "narrowings": [],
    }
    edges = output_signature.get("edges") or []
    if not edges or catalog_version_id is None:
        return evidence
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM okg.subtypes WHERE catalog_version_id = %s",
            (catalog_version_id,),
        )
        present_subtypes = {str(row[0]) for row in cur.fetchall()}
    for rule in edges:
        src_subtype = str(rule["src_subtype"])
        edge_type = str(rule["edge_type"])
        dst_subtype = str(rule["dst_subtype"])
        missing_subtypes = sorted(
            subtype
            for subtype in (src_subtype, dst_subtype)
            if subtype not in present_subtypes
        )
        if missing_subtypes:
            evidence["narrowings"].append({
                "src_subtype": src_subtype,
                "edge_type": edge_type,
                "dst_subtype": dst_subtype,
                "status": "skipped_missing_subtype",
                "missing_subtypes": missing_subtypes,
            })
            continue
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM okg.narrowings
                 WHERE catalog_version_id = %s
                   AND parent_subtype = %s
                   AND edge_type = %s
                   AND child_subtype = %s
                 LIMIT 1
                """,
                (catalog_version_id, src_subtype, edge_type, dst_subtype),
            )
            matched = cur.fetchone() is not None
        narrowing = {
            "src_subtype": src_subtype,
            "edge_type": edge_type,
            "dst_subtype": dst_subtype,
            "status": "matched" if matched else "missing",
        }
        evidence["narrowings"].append(narrowing)
        if not matched:
            raise ProducerPolicyViolation(
                code="producer_catalog_narrowing_missing",
                message="producer edge signature is not allowed by catalog",
                verdict={
                    "code": "producer_catalog_narrowing_missing",
                    "producer_policy": {
                        "status": "rejected",
                    },
                    "catalog": {
                        "catalog_version_id": catalog_version_id,
                        "narrowing": narrowing,
                    },
                },
            )
    return evidence


def evaluate_payload_signature(
    *,
    profile: ProducerProfile,
    nodes: Sequence[Mapping[str, Any]],
    edges: Sequence[Mapping[str, Any]],
    conn: psycopg.Connection | None = None,
) -> dict[str, Any]:
    """Evaluate a payload against a producer profile.

    `conn` is optional: when supplied, the subtype-refinement
    closure for the profile's catalog version is loaded and used
    for refinement-aware matching (a profile declaring an abstract
    parent matches emissions of any concrete refinement). When
    omitted (legacy callers), matching is exact — strict superset
    of pre-refinement behavior.
    """
    endpoint_subtypes = {
        str(node["node_id"]): str(node["subtype"])
        for node in nodes
        if str(node.get("op") or "I") == "I"
    }
    ancestors = (
        _load_subtype_ancestors(conn, profile.catalog_version_id)
        if conn is not None
        else None
    )
    return _evaluate_records(
        profile=profile,
        # Endpoints here resolve from the nodes carried in THIS payload and
        # nowhere else, so absence means absence from the batch.
        endpoint_scope="the nodes declared in this payload",
        node_records=[
            {
                "node_id": str(node["node_id"]),
                "subtype": str(node["subtype"]),
                "attrs": dict(node.get("attrs") or {}),
                "op": str(node.get("op") or "I"),
            }
            for node in nodes
        ],
        edge_records=[
            {
                "src": str(edge["src"]),
                "src_subtype": (
                    str(edge["src_subtype"])
                    if edge.get("src_subtype")
                    else endpoint_subtypes.get(str(edge["src"]))
                ),
                "edge_type": str(edge["edge_type"]),
                "dst": str(edge["dst"]),
                "dst_subtype": (
                    str(edge["dst_subtype"])
                    if edge.get("dst_subtype")
                    else endpoint_subtypes.get(str(edge["dst"]))
                ),
                "attrs": dict(edge.get("attrs") or {}),
                "op": str(edge.get("op") or "I"),
            }
            for edge in edges
        ],
        ancestors=ancestors,
    )


def evaluate_source_run_signature(
    conn: psycopg.Connection,
    *,
    profile: ProducerProfile,
    source: str,
    run_id: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_id, node_id, subtype, attrs, op
              FROM okg.node_facts
             WHERE source = %s
               AND run_id = %s::uuid
             ORDER BY fact_id
            """,
            (source, run_id),
        )
        node_rows = cur.fetchall()
        cur.execute(
            """
            SELECT fact_id, src, edge_type, dst, attrs, op
              FROM okg.edge_facts
             WHERE source = %s
               AND run_id = %s::uuid
             ORDER BY fact_id
            """,
            (source, run_id),
        )
        edge_rows = cur.fetchall()

    if not node_rows and not edge_rows:
        # The run wrote straight to the graph. Its rows are evaluated in
        # groups read by aggregate statements, never loaded whole: a large run
        # once held gigabytes here and left the admission transaction idle
        # long enough for the idle-in-transaction timeout to kill it. A read
        # failure is raised, not treated as an empty run.
        run_nodes, run_edges = _source_run_record_sources(
            conn,
            profile=profile,
            source=source,
            run_id=run_id,
        )
        try:
            return _evaluate_record_sources(
                profile=profile,
                nodes=run_nodes,
                edges=run_edges,
                # This path reads the projected graph for the run; it does
                # NOT consult the node-fact log, so it cannot speak to a
                # node that exists only as an unprojected fact.
                endpoint_scope=(
                    "the default-branch graph nodes for this source run"
                ),
                ancestors=_load_subtype_ancestors(
                    conn, profile.catalog_version_id,
                ),
            )
        except SignatureGroupingInconsistent as exc:
            exc.data.update({"source": source, "run_id": run_id})
            raise

    node_records = [
        {
            "fact_id": int(row[0]),
            "node_id": str(row[1]),
            "subtype": str(row[2]),
            "attrs": dict(row[3] or {}),
            "op": str(row[4]),
        }
        for row in node_rows
    ]
    endpoint_subtypes = {
        record["node_id"]: record["subtype"]
        for record in node_records
        if record["op"] == "I"
    }
    endpoint_ids = {
        str(row[1]) for row in edge_rows
    } | {
        str(row[3]) for row in edge_rows
    }
    missing = sorted(endpoint_ids - set(endpoint_subtypes))
    if missing:
        endpoint_subtypes.update(
            _chronos_endpoint_subtypes(conn, missing),
        )
        # Cross-source endpoints: a source emitting an edge may target a
        # node created by another source whose facts are still pending
        # admission/projection in the same publish cycle, or a node that
        # was retracted in some past generation but whose I-fact is still
        # the canonical subtype record.  Fall back to the most-recent
        # I-fact in node_facts so the signature can match either case.
        # The narrowing trigger at projection still enforces catalog
        # constraints, so admission can be permissive about endpoint
        # discoverability without losing safety.
        still_missing = sorted(endpoint_ids - set(endpoint_subtypes))
        if still_missing:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (node_id) node_id, subtype
                      FROM okg.node_facts
                     WHERE node_id = ANY(%s)
                       AND op = 'I'
                     ORDER BY node_id, observed_at DESC
                    """,
                    (still_missing,),
                )
                endpoint_subtypes.update({
                    str(node_id): str(subtype)
                    for node_id, subtype in cur.fetchall()
                })
    edge_records = [
        {
            "fact_id": int(row[0]),
            "src": str(row[1]),
            "src_subtype": endpoint_subtypes.get(str(row[1])),
            "edge_type": str(row[2]),
            "dst": str(row[3]),
            "dst_subtype": endpoint_subtypes.get(str(row[3])),
            "attrs": dict(row[4] or {}),
            "op": str(row[5]),
        }
        for row in edge_rows
    ]
    return _evaluate_records(
        profile=profile,
        node_records=node_records,
        edge_records=edge_records,
        endpoint_scope=(
            "this run's node facts, the branch graph nodes, and the "
            "most recent insertion for the node in the fact log"
        ),
        ancestors=_load_subtype_ancestors(
            conn, profile.catalog_version_id,
        ),
    )


#: Representative records fetched per statement while walking a source run's
#: record groups. Bounds the rows held in Python between two statements.
_SOURCE_RUN_FETCH_CHUNK = 1000


def _signature_group_plan(
    profile: ProducerProfile,
) -> tuple[list[str], list[str], bool]:
    """Every record property a signature check reads, as SQL grouping inputs.

    Returns the id prefixes the rules test, the attribute keys the rules and
    the closed vocabularies read, and whether any rule compares the exact
    attribute key set. Two records that agree on all of these (plus subtype,
    or edge type and resolved endpoint subtypes) get the same verdict from
    ``_evaluate_record_sources``. Taking every prefix and key from both rule
    kinds only makes the groups finer, never coarser.
    """
    signature = profile.output_signature or {}
    rules = [
        rule
        for kind in ("nodes", "edges")
        for rule in (signature.get(kind) or [])
        if isinstance(rule, Mapping)
    ]
    prefixes: set[str] = set()
    attr_keys: set[str] = set()
    exact = False
    for rule in rules:
        for key in ("id_prefix", "src_id_prefix", "dst_id_prefix"):
            if rule.get(key):
                prefixes.add(str(rule[key]))
        if isinstance(rule.get("attrs"), Mapping):
            attr_keys.update(str(key) for key in rule["attrs"])
        if rule.get("attrs_exact") is True:
            exact = True
    envelope = profile_write_envelope(profile) or {}
    attr_keys.update(
        str(field) for field in (envelope.get("value_vocabularies") or {})
    )
    return sorted(prefixes), sorted(attr_keys), exact


def _source_run_read_error(
    exc: BaseException, *, source: str, run_id: str, kind: str, stage: str,
) -> SubstrateError:
    return SubstrateError(
        (
            f"admission could not read the {kind} records of source run "
            f"{source!r} / {run_id!r} from the default-branch graph to check "
            f"them against the producer signature ({stage}): "
            f"{type(exc).__name__}: {exc}"
        ),
        data={
            "source": source,
            "run_id": run_id,
            "record_kind": kind,
            "stage": stage,
            "cause": type(exc).__name__,
        },
        fix_hint=(
            "the run was not admitted; fix the read failure named in the "
            "message (a lost connection, an idle-in-transaction timeout, or "
            "an unreadable graph table) and rerun the source"
        ),
    )


#: Time limit for one grouping statement of the source-run signature check.
#: Admission holds a transaction open while it runs, and a running statement
#: is not bounded by the idle-in-transaction timeout. The 1.55-million-record
#: code-structure run grouped in 13 s (nodes) and 21 s (edges); one lane C
#: review run on a 40,000-record run took 21 minutes before it was
#: cancelled, and its reruns took 6-8.5 s. The cause of that plan was not
#: reproduced.
_SOURCE_RUN_GROUPING_TIMEOUT_MS = 600_000


def _signature_grouping_error(
    kind: str,
    first: Mapping[str, Any],
    other: Mapping[str, Any] | None,
    first_verdict: Any,
    other_verdict: Any,
    *,
    stage: str,
) -> SignatureGroupingInconsistent:
    """Two records the grouping called equivalent got different verdicts.

    Admission refuses to decide rather than apply one record's verdict to
    records it does not describe; the run is not admitted. The source-run
    caller adds ``source`` and ``run_id`` to ``data``. ``other`` is None
    when a single record's own verdict disagrees with its group's.
    """
    return SignatureGroupingInconsistent(
        (
            f"signature check grouped {kind} records that it judges "
            f"differently ({first_verdict!r} vs {other_verdict!r}); the "
            f"run was not admitted"
        ),
        data={
            "stage": stage,
            "record_kind": kind,
            "records": [
                *_sample_records(kind, [first]),
                *(_sample_records(kind, [other]) if other is not None else []),
            ],
            "verdicts": [repr(first_verdict), repr(other_verdict)],
        },
        fix_hint=(
            "a defect in the admission grouping "
            "(producer_authority._signature_group_plan): a rule reads a "
            "record property the grouping does not key on"
        ),
    )


def _is_query_canceled(exc: BaseException) -> bool:
    """True when PostgreSQL cancelled the statement (SQLSTATE 57014)."""
    seen: BaseException | None = exc
    while seen is not None:
        if getattr(seen, "sqlstate", None) == "57014":
            return True
        seen = seen.__cause__ or seen.__context__
    return False


class _ChronosRunRecords:
    """One source run's node or edge rows on the default branch, grouped.

    The rows are never loaded into Python whole. One aggregate statement
    groups the run's rows by every property a signature check reads (see
    ``_signature_group_plan``) and returns, per group, its row count and the
    positions and ids of its earliest rows, in the order the old full read
    used (nodes by ``node_id, subtype``; edges by ``src, edge_type, dst``,
    ties broken by ``edge_id``). Positions start at 1, like the old
    ``fact_id`` numbering. Each group's first row is then fetched, a chunk of
    groups per statement, and offending samples are fetched by id.

    Edge endpoint subtypes come from a join to the same default-branch
    ``okg.graph_nodes`` the old path read twice (once for the run's nodes,
    once for other endpoints); ``node_id`` is that table's key, so the join
    returns the same subtype for every endpoint.

    Every read failure is raised as a ``SubstrateError`` naming the run and
    the cause. There is no fallback: a run that cannot be read is not
    admitted.
    """

    def __init__(
        self,
        graph: Any,
        conn: psycopg.Connection,
        *,
        kind: str,
        source: str,
        run_id: str,
        plan: tuple[list[str], list[str], bool],
    ) -> None:
        self._graph = graph
        self._kind = kind
        self._source = source
        self._run_id = run_id
        prefixes, attr_keys, exact = plan
        params: dict[str, Any] = {"source": source, "run_id": run_id}
        for index, prefix in enumerate(prefixes):
            params[f"prefix_{index}"] = prefix
        for index, key in enumerate(attr_keys):
            params[f"attr_{index}"] = key
        if kind == "node":
            alias = "n"
            ref = "n.node_id"
            order = ["n.node_id", "n.subtype"]
            keys = ["n.subtype"]
            keys += [
                f"STARTS_WITH(n.node_id, CAST(:prefix_{index} AS TEXT))"
                for index in range(len(prefixes))
            ]
            relation = "okg.graph_nodes AS n"
        else:
            alias = "e"
            ref = "e.edge_id"
            order = ["e.src", "e.edge_type", "e.dst", "e.edge_id"]
            keys = ["e.edge_type", "s.subtype", "d.subtype"]
            keys += [
                f"STARTS_WITH(e.{column}, CAST(:prefix_{index} AS TEXT))"
                for index in range(len(prefixes))
                for column in ("src", "dst")
            ]
            relation = (
                "okg.graph_edges AS e "
                "LEFT JOIN okg.graph_nodes AS s ON s.node_id = e.src "
                "LEFT JOIN okg.graph_nodes AS d ON d.node_id = e.dst"
            )
        # The shape of attrs: an object, empty (SQL NULL or JSON null, which
        # the check reads as {}), or another JSON type, which is refused when
        # its group's record is read.
        keys.append(
            f"CASE WHEN {alias}.attrs IS NULL "
            f"OR JSONB_TYPEOF({alias}.attrs) = 'null' THEN 'empty' "
            f"ELSE JSONB_TYPEOF({alias}.attrs) END"
        )
        keys += [
            f"CAST(({alias}.attrs -> CAST(:attr_{index} AS TEXT)) AS TEXT)"
            for index in range(len(attr_keys))
        ]
        if exact:
            # JSONB_OBJECT_KEYS refuses anything but an object, so only an
            # object's keys are listed; every other shape is told apart by
            # the shape key above.
            keys.append(
                f"CASE WHEN JSONB_TYPEOF({alias}.attrs) = 'object' "
                f"THEN ARRAY(SELECT JSONB_OBJECT_KEYS({alias}.attrs) "
                f"AS attr_key ORDER BY attr_key) END"
            )
        group_columns = [f"g_{index}" for index in range(len(keys))]
        order_columns = [f"o_{index}" for index in range(len(order))]
        # MATERIALIZED keeps the attrs column out of the window sorts: the
        # CTE projects only the order columns and the (small) group keys.
        sql = f"""
            WITH keyed AS MATERIALIZED (
              SELECT {ref} AS ref,
                     {", ".join(
                         f"{expr} AS {name}"
                         for expr, name in zip(order, order_columns)
                     )},
                     {", ".join(
                         f"{expr} AS {name}"
                         for expr, name in zip(keys, group_columns)
                     )}
                FROM {relation}
               WHERE {alias}.source = :source
                 AND {alias}.run_id = :run_id
            ), ordered AS (
              SELECT ref, {", ".join(group_columns)},
                     ROW_NUMBER() OVER (
                       ORDER BY {", ".join(order_columns)}
                     ) AS pos
                FROM keyed
            ), ranked AS (
              SELECT ref, pos, {", ".join(group_columns)},
                     ROW_NUMBER() OVER (
                       PARTITION BY {", ".join(group_columns)}
                       ORDER BY pos
                     ) AS rank_in_group,
                     COUNT(*) OVER (
                       PARTITION BY {", ".join(group_columns)}
                     ) AS group_size
                FROM ordered
            )
            SELECT COUNT(*) AS record_count,
                   MIN(pos) AS first_pos,
                   ARRAY_AGG(ref ORDER BY pos)
                     FILTER (WHERE rank_in_group <= {_OFFENDING_SAMPLE_LIMIT})
                     AS head_refs,
                   ARRAY_AGG(pos ORDER BY pos)
                     FILTER (WHERE rank_in_group <= {_OFFENDING_SAMPLE_LIMIT})
                     AS head_positions,
                   MAX(ref) FILTER (
                     WHERE rank_in_group = group_size AND group_size > 1
                   ) AS last_ref,
                   MAX(pos) FILTER (
                     WHERE rank_in_group = group_size AND group_size > 1
                   ) AS last_pos
              FROM ranked
             GROUP BY {", ".join(group_columns)}
             ORDER BY first_pos
        """
        # The stricter limit wins: the caller's own statement_timeout when
        # it is set and shorter, otherwise admission's. ``graph.query`` runs
        # on this same ``conn``: ChronosGraphStore.from_connection wraps it in
        # a PostgresDatabaseAdapter whose execute() calls conn.execute, so
        # the setting applies to the grouping statement.
        timeout_ms = _SOURCE_RUN_GROUPING_TIMEOUT_MS
        limit_source = "admission"
        started = time.monotonic()
        try:
            # Any change is made inside a savepoint (or a transaction on an
            # autocommit connection) and put back before it is released. A
            # failed statement rolls the savepoint back, which also undoes
            # the setting.
            with conn.transaction():
                prior, prior_ms = conn.execute(
                    "SELECT current_setting('statement_timeout'), "
                    "setting::bigint FROM pg_settings "
                    "WHERE name = 'statement_timeout'"
                ).fetchone()
                if 0 < int(prior_ms) <= timeout_ms:
                    timeout_ms = int(prior_ms)
                    limit_source = "caller"
                    rows = graph.query(sql, params)
                else:
                    conn.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (f"{int(timeout_ms)}ms",),
                    )
                    rows = graph.query(sql, params)
                    conn.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (prior,),
                    )
        except Exception as exc:
            if _is_query_canceled(exc):
                raise SourceRunGroupingTimeout(
                    (
                        f"admission's grouping of the {kind} records of "
                        f"source run {source!r} / {run_id!r} ran past the "
                        f"{timeout_ms} ms {limit_source} statement_timeout "
                        f"and was cancelled; the run was not admitted"
                    ),
                    data={
                        "source": source,
                        "run_id": run_id,
                        "record_kind": kind,
                        "stage": "grouping the run's records",
                        "timeout_ms": int(timeout_ms),
                        "limit_source": limit_source,
                        "elapsed_ms": int(
                            (time.monotonic() - started) * 1000
                        ),
                    },
                    fix_hint=(
                        "rerun the source; if it recurs, capture the "
                        "statement's plan (EXPLAIN) and the graph tables' "
                        "statistics (pg_stat_user_tables) for the run"
                    ),
                ) from exc
            raise _source_run_read_error(
                exc, source=source, run_id=run_id, kind=kind,
                stage="grouping the run's records",
            ) from exc
        self._groups: list[tuple[int, tuple[tuple[int, Any], ...]]] = []
        #: The last row of each group of two or more rows, re-evaluated by
        #: the admit-path guard (``_RecordGroup.check_record``).
        self._last: list[tuple[int, Any] | None] = []
        for row in rows:
            self._groups.append((
                int(row["record_count"]),
                tuple(
                    (int(position), reference)
                    for position, reference in zip(
                        row["head_positions"], row["head_refs"],
                    )
                ),
            ))
            self._last.append(
                None if row["last_pos"] is None
                else (int(row["last_pos"]), row["last_ref"])
            )
        self.total = sum(count for count, _ in self._groups)
        self.insert_count = self.total
        #: Rows each statement returned, for the bounded-read tests.
        self.statement_row_counts: list[int] = [len(rows)]

    def groups(self) -> Iterator[_RecordGroup]:
        for start in range(0, len(self._groups), _SOURCE_RUN_FETCH_CHUNK):
            chunk = self._groups[start:start + _SOURCE_RUN_FETCH_CHUNK]
            lasts = self._last[start:start + _SOURCE_RUN_FETCH_CHUNK]
            wanted = [head[0] for _, head in chunk] + [
                last for last in lasts if last is not None
            ]
            resolved = iter(self.resolve(wanted))
            records = [next(resolved) for _ in chunk]
            for (count, head), record, last in zip(chunk, records, lasts):
                yield _RecordGroup(
                    record=record,
                    count=count,
                    head=head,
                    check_record=None if last is None else next(resolved),
                )

    def first(self, limit: int) -> list[Mapping[str, Any]]:
        head = sorted(
            (entry for _, group_head in self._groups for entry in group_head),
            key=lambda entry: entry[0],
        )[:limit]
        return self.resolve(head)

    def resolve(
        self, head: Sequence[tuple[int, Any]],
    ) -> list[Mapping[str, Any]]:
        if not head:
            return []
        references = [reference for _, reference in head]
        params = {
            "refs": references,
            "source": self._source,
            "run_id": self._run_id,
        }
        if self._kind == "node":
            sql = """
                SELECT n.node_id AS ref, n.node_id, n.subtype, n.attrs
                  FROM okg.graph_nodes AS n
                 WHERE n.node_id = ANY(:refs)
                   AND n.source = :source
                   AND n.run_id = :run_id
            """
        else:
            sql = """
                SELECT e.edge_id AS ref, e.src, e.edge_type, e.dst, e.attrs,
                       s.subtype AS src_subtype, d.subtype AS dst_subtype
                  FROM okg.graph_edges AS e
                  LEFT JOIN okg.graph_nodes AS s ON s.node_id = e.src
                  LEFT JOIN okg.graph_nodes AS d ON d.node_id = e.dst
                 WHERE e.edge_id = ANY(:refs)
                   AND e.source = :source
                   AND e.run_id = :run_id
            """
        try:
            rows = self._graph.query(sql, params)
            by_ref = {row["ref"]: row for row in rows}
            missing = [ref for ref in references if ref not in by_ref]
            if missing:
                raise LookupError(
                    f"{len(missing)} {self._kind} row(s) of the run vanished "
                    f"between the grouping read and the record read, e.g. "
                    f"{missing[:3]!r}"
                )
            self.statement_row_counts.append(len(rows))
            return [
                self._record(position, by_ref[reference])
                for position, reference in head
            ]
        except Exception as exc:
            raise _source_run_read_error(
                exc, source=self._source, run_id=self._run_id,
                kind=self._kind, stage="reading the run's records",
            ) from exc

    def _record(self, position: int, row: Mapping[str, Any]) -> dict[str, Any]:
        if row["attrs"] is not None and not isinstance(row["attrs"], Mapping):
            raise TypeError(
                f"{self._kind} attrs at position {position} is a JSON "
                f"{type(row['attrs']).__name__}, not an object"
            )
        if self._kind == "node":
            return {
                "fact_id": position,
                "node_id": str(row["node_id"]),
                "subtype": str(row["subtype"]),
                "attrs": dict(row["attrs"] or {}),
                "op": "I",
            }
        return {
            "fact_id": position,
            "src": str(row["src"]),
            "src_subtype": (
                None if row["src_subtype"] is None
                else str(row["src_subtype"])
            ),
            "edge_type": str(row["edge_type"]),
            "dst": str(row["dst"]),
            "dst_subtype": (
                None if row["dst_subtype"] is None
                else str(row["dst_subtype"])
            ),
            "attrs": dict(row["attrs"] or {}),
            "op": "I",
        }


def _source_run_record_sources(
    conn: psycopg.Connection,
    *,
    profile: ProducerProfile,
    source: str,
    run_id: str,
    branch: str = "default",
) -> tuple[_ChronosRunRecords, _ChronosRunRecords]:
    from okg.substrate.graph_store import ChronosGraphStore

    try:
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            # Admission owns this transaction; a Chronos per-query commit
            # would end it early.
            autocommit=False,
        ).checkout(branch)
    except Exception as exc:
        raise _source_run_read_error(
            exc, source=source, run_id=run_id, kind="node and edge",
            stage=f"checking out branch {branch!r}",
        ) from exc
    plan = _signature_group_plan(profile)
    return (
        _ChronosRunRecords(
            graph, conn, kind="node", source=source, run_id=run_id,
            plan=plan,
        ),
        _ChronosRunRecords(
            graph, conn, kind="edge", source=source, run_id=run_id,
            plan=plan,
        ),
    )


def _chronos_interval_physical_table(
    conn: psycopg.Connection,
    *,
    table_name: str,
    branch: str,
) -> tuple[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bt.physical_table, seg.branch_point
              FROM _chronos_branch_tables bt
              JOIN _chronos_branch_interval_branches b
                ON b.branch_id = %s
              JOIN _chronos_branch_interval_segments seg
                ON seg.segment_id = b.current_segment_id
             WHERE bt.table_name = %s
               AND bt.backend = 'interval'
            """,
            (branch, table_name),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Chronos interval table not registered: {table_name}")
    return str(row[0]), row[1]


def evaluate_edge_fact_range_signature(
    conn: psycopg.Connection,
    *,
    profile: ProducerProfile,
    source: str,
    fact_id_min: int,
    fact_id_max: int,
    node_fact_id_min: int | None = None,
    node_fact_id_max: int | None = None,
    generation_id: int | None = None,
) -> dict[str, Any]:
    """Evaluate a derived edge fact range against a producer profile.

    Endpoint subtypes are resolved from the Chronos graph
    (`okg.graph_nodes`) via the current branch checkout. `generation_id`
    is retained for caller compatibility but is no longer used: the
    legacy bitemporal `nodes_live` pin it gated was removed by the
    Chronos cutover.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_id, src, edge_type, dst, attrs, op
              FROM okg.edge_facts
             WHERE source = %s
               AND fact_id BETWEEN %s AND %s
             ORDER BY fact_id
            """,
            (source, fact_id_min, fact_id_max),
        )
        edge_rows = cur.fetchall()

    endpoint_ids = {
        str(row[1]) for row in edge_rows
    } | {
        str(row[3]) for row in edge_rows
    }
    endpoint_subtypes: dict[str, str] = {}
    if endpoint_ids:
        endpoint_subtypes.update(
            _chronos_endpoint_subtypes(conn, sorted(endpoint_ids)),
        )
        # Derived enrichers may emit a node and an edge to that node in the
        # same publisher transaction. Admission runs before those node facts
        # are applied to the Chronos working head, so resolve still-missing
        # endpoints from the node-fact window captured immediately around that
        # producer invocation. Do not consult older fact history: a historical
        # insertion may since have been retracted, and must not authorize a new
        # edge merely because its subtype is still discoverable in the ledger.
        missing = sorted(endpoint_ids - set(endpoint_subtypes))
        if (
            missing
            and node_fact_id_min is not None
            and node_fact_id_max is not None
            and node_fact_id_min <= node_fact_id_max
        ):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT node_id, subtype
                      FROM (
                        SELECT DISTINCT ON (node_id)
                               node_id, subtype, op
                          FROM okg.node_facts
                         WHERE node_id = ANY(%s)
                           AND fact_id BETWEEN %s AND %s
                         ORDER BY node_id, observed_at DESC, fact_id DESC
                      ) latest
                     WHERE op = 'I'
                    """,
                    (missing, node_fact_id_min, node_fact_id_max),
                )
                endpoint_subtypes.update({
                    str(node_id): str(subtype)
                    for node_id, subtype in cur.fetchall()
                })

    edge_records = [
        {
            "fact_id": int(row[0]),
            "src": str(row[1]),
            "src_subtype": endpoint_subtypes.get(str(row[1])),
            "edge_type": str(row[2]),
            "dst": str(row[3]),
            "dst_subtype": endpoint_subtypes.get(str(row[3])),
            "attrs": dict(row[4] or {}),
            "op": str(row[5]),
        }
        for row in edge_rows
    ]
    return _evaluate_records(
        profile=profile,
        node_records=[],
        edge_records=edge_records,
        endpoint_scope=(
            "the branch graph nodes and this cycle's node-fact window"
        ),
        ancestors=_load_subtype_ancestors(
            conn, profile.catalog_version_id,
        ),
    )


def evaluate_node_fact_range_signature(
    conn: psycopg.Connection,
    *,
    profile: ProducerProfile,
    source: str,
    fact_id_min: int,
    fact_id_max: int,
) -> dict[str, Any]:
    """Evaluate a derived NODE fact range against a producer profile.

    PACT ``admit-derived-node-emission``. The record-evaluation machinery
    has always accepted ``node_records``; nothing ever fed it for a derived
    producer, because derived admission scanned ``okg.edge_facts`` only. A
    producer that emits only nodes was therefore admitted by nothing at all
    — it could emit any subtype, outside any declared output signature,
    and the loader's rejected-run deny-list had no envelope to act on.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_id, node_id, subtype, attrs, op
              FROM okg.node_facts
             WHERE source = %s
               AND fact_id BETWEEN %s AND %s
             ORDER BY fact_id
            """,
            (source, fact_id_min, fact_id_max),
        )
        node_rows = cur.fetchall()

    node_records = [
        {
            "fact_id": int(row[0]),
            "node_id": str(row[1]),
            "subtype": str(row[2]),
            "attrs": dict(row[3] or {}),
            "op": str(row[4]),
        }
        for row in node_rows
    ]
    return _evaluate_records(
        profile=profile,
        node_records=node_records,
        edge_records=[],
        # Node-only path: no edge record is evaluated, so no endpoint is ever
        # resolved here. The scope is stated rather than left to a default.
        endpoint_scope="no endpoint lookup (node records only)",
        ancestors=_load_subtype_ancestors(
            conn, profile.catalog_version_id,
        ),
    )


def _chronos_endpoint_subtypes(
    conn: psycopg.Connection,
    node_ids: list[str],
) -> dict[str, str]:
    """Map each endpoint id present in the branch graph to its subtype.

    An id absent from the graph is simply absent from the result: that is
    the legitimate "not in the graph yet" answer, and the callers resolve it
    from the node-fact log next. A failed read is different and propagates.
    It used to be swallowed into ``{}``, which made every endpoint look
    unresolvable and let admission judge edges on a lookup that never ran.
    Inside a caller's open transaction a swallowed database error also left
    that transaction aborted, so the callers' next query failed with an
    unrelated message.
    """
    if not node_ids:
        return {}
    from okg.substrate.graph_store import (
        DEFAULT_BRANCH,
        ChronosGraphStore,
    )

    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('okg.chronos_branch', true)")
        branch = cur.fetchone()[0] or DEFAULT_BRANCH
    store = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        # Signature evaluation runs during output admission and may share
        # the Publisher transaction that owns retry reservation, output,
        # terminal quarantine, and progress.  A caller-owned connection
        # must never let this read invoke Chronos's per-query commit.
        autocommit=False,
    )
    rows = store.checkout(str(branch)).query(
        """
        SELECT node_id, subtype
          FROM okg.graph_nodes
         WHERE node_id = ANY(:node_ids)
        """,
        {"node_ids": node_ids},
    )
    return {
        str(row["node_id"]): str(row["subtype"])
        for row in rows
    }


def merge_policy_evidence(
    *evidence_maps: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for evidence in evidence_maps:
        if not evidence:
            continue
        for key, value in evidence.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(value, Mapping)
            ):
                merged[key] = {**merged[key], **dict(value)}
            else:
                merged[key] = value
    return merged


# The agent write-back is the one genuinely free-form producer (spec §9.3):
# an agent may propose any catalog-legal node/edge, so no finite signature
# can bound it. Its envelope is declared explicitly unbounded on every
# dimension and nondeterministic — proving the marker is USED, not dodged.
AGENT_UNBOUNDED_DIMENSIONS = ("nodes", "edges", "claim_keys", "values")


def _apply_agent_write_envelope(
    output_scope_summary: dict[str, Any],
    *,
    producer_kind: str,
    producer_id: str,
    output_signature: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Stamp the unbounded agent envelope on the scope summary (spec §9.3).

    The default agent write-back (MCP record/propose) declares no finite
    signature, so no finite envelope can bound it: its envelope is declared
    explicitly unbounded on every dimension and nondeterministic (the marker
    is USED, not dodged). The marker rides
    ``output_scope_summary["write_envelope"]`` exactly like the source-adapter
    and enricher paths, so :func:`profile_write_envelope` and the runtime
    evaluator pick it up with ZERO evaluator changes: the evaluator exempts
    unbounded dimensions from producer-signature matching while catalog
    narrowing law still applies.

    An agent that VOLUNTARILY declares a finite ``output_signature`` (or an
    explicit ``write_envelope``) has a tighter, explicit contract that
    admission still enforces — that loud-failure path is never weakened, so the
    unbounded default applies only when no finite declaration is present.
    Non-agent producers are returned unchanged."""
    if producer_kind != "agent":
        return output_scope_summary
    if output_signature:
        # An explicit finite signature is a tighter contract; honor it and do
        # not widen it to unbounded (do not weaken the rejection path).
        return output_scope_summary
    if "write_envelope" in output_scope_summary:
        # An explicit envelope was already declared; never override it.
        return output_scope_summary
    envelope = normalize_write_envelope(
        {
            "unbounded": list(AGENT_UNBOUNDED_DIMENSIONS),
            "nondeterministic": True,
        },
        source_name=producer_id,
    )
    return {**output_scope_summary, "write_envelope": envelope.summary()}


def policy_from_authority(
    authority: Mapping[str, Any] | None,
    *,
    producer_id: str,
    producer_kind: str,
) -> dict[str, Any]:
    raw = dict(authority or {})
    policy = raw.get("producer_policy")
    if policy is None:
        policy = raw.get("admission_policy")
    if policy is None and (
        "output_signature" in raw
        or "allowed_record_kinds" in raw
        or "policy_clauses" in raw
        or "authority_scope" in raw
    ):
        policy = raw
    if policy is None:
        return {
            "producer_id": producer_id,
            "producer_kind": producer_kind,
            "trust_label": normalize_trust_label(
                str(raw.get("trust_label") or "review-trusted")
            ),
            "admission_mode": "review",
            "authority_scope": {},
            # Compatibility review submissions historically accepted either
            # graph record kind after the explicit human review decision.
            # Keep that bounded compatibility behavior visible in the profile
            # so FactBatch authority validation and payload-signature
            # validation agree instead of the latter allowing a proposal that
            # the shared application chokepoint then rejects.
            "allowed_record_kinds": ["node", "edge"],
            "output_signature": {},
            "policy_clauses": [],
            "output_scope_summary": _apply_agent_write_envelope(
                {
                    "summary": (
                        "compatibility review profile; add authority."
                        "producer_policy for enforceable review signatures"
                    )
                },
                producer_kind=producer_kind,
                producer_id=producer_id,
                output_signature={},
            ),
        }
    if not isinstance(policy, Mapping):
        raise ProducerPolicyViolation(
            code="producer_policy_invalid",
            message="authority.producer_policy must be a mapping",
            verdict={"code": "producer_policy_invalid"},
        )
    source_name = producer_id
    output_signature = normalize_output_signature(
        policy.get("output_signature"),
        source_name=source_name,
    )
    resolved_producer_id = str(policy.get("producer_id") or producer_id)
    resolved_producer_kind = str(policy.get("producer_kind") or producer_kind)
    return {
        "producer_id": resolved_producer_id,
        "producer_kind": resolved_producer_kind,
        "trust_label": normalize_trust_label(
            str(policy.get("trust_label") or "review-trusted")
        ),
        "admission_mode": str(policy.get("admission_mode") or "review"),
        "authority_scope": normalize_authority_scope(
            policy.get("authority_scope"),
            source_name=source_name,
        ),
        "allowed_record_kinds": normalize_allowed_record_kinds(
            policy.get("allowed_record_kinds"),
            output_signature=output_signature,
            source_name=source_name,
        ),
        "output_signature": output_signature,
        "policy_clauses": normalize_policy_clauses(
            policy.get("policy_clauses"),
            source_name=source_name,
        ),
        "output_scope_summary": _apply_agent_write_envelope(
            _normalize_output_scope_summary(
                policy.get("output_scope_summary"),
            ),
            producer_kind=resolved_producer_kind,
            producer_id=resolved_producer_id,
            output_signature=output_signature,
        ),
    }


@dataclass(frozen=True)
class _RecordGroup:
    """Insert records that every signature check treats identically.

    ``record`` is the group's earliest record. ``head`` holds the
    ``(position, reference)`` pairs of the group's earliest records, at most
    ``_OFFENDING_SAMPLE_LIMIT`` of them, in record order; a reference is
    whatever the owning record source's ``resolve`` turns back into records.
    """

    record: Mapping[str, Any]
    count: int
    head: tuple[tuple[int, Any], ...]
    #: Another record of the same group (its last), evaluated as a guard:
    #: it must get the same verdict as ``record``. None for a one-record
    #: group.
    check_record: Mapping[str, Any] | None = None


class _ListRecordSource:
    """Records already in memory: every insert record is its own group.

    Used by every path that is handed a list (payloads, fact ranges, the
    node-fact source-run path). Evaluating singleton groups is record-by-
    record evaluation, so these paths behave exactly as before.
    """

    def __init__(self, records: Sequence[Mapping[str, Any]]) -> None:
        self._records = records
        self.total = len(records)
        self.insert_count = sum(
            1 for record in records if record.get("op") == "I"
        )

    def groups(self) -> Iterator[_RecordGroup]:
        for position, record in enumerate(self._records):
            if record.get("op") == "I":
                yield _RecordGroup(
                    record=record, count=1, head=((position, record),),
                )

    def first(self, limit: int) -> list[Mapping[str, Any]]:
        return list(self._records[:limit])

    def resolve(
        self, head: Sequence[tuple[int, Any]],
    ) -> list[Mapping[str, Any]]:
        return [record for _, record in head]


def _evaluate_records(
    *,
    profile: ProducerProfile,
    node_records: Sequence[Mapping[str, Any]],
    edge_records: Sequence[Mapping[str, Any]],
    endpoint_scope: str,
    ancestors: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    """Evaluate one in-memory emission against a producer profile.

    ``endpoint_scope`` names WHERE this caller looked for endpoint subtypes,
    in plain words. It is required rather than defaulted: an unresolved
    endpoint means only "not found in the place this path searched", and the
    paths search very different places -- one reads the nodes declared in the
    payload itself, another the default-branch graph, another the full
    node-fact history. A message that says the node does not exist would be
    a claim the caller cannot support (issue #1323 review of #1330).
    """
    return _evaluate_record_sources(
        profile=profile,
        nodes=_ListRecordSource(node_records),
        edges=_ListRecordSource(edge_records),
        endpoint_scope=endpoint_scope,
        ancestors=ancestors,
    )


def _verdict_shape(outcome: Mapping[str, Any] | None) -> tuple | None:
    """The part of one record's verdict that must agree across its group.

    Messages and endpoint node ids name the record itself and legitimately
    differ; the code, clause ids, unresolved endpoint roles, matched clause
    and allowed-relationship triple may not.
    """
    if outcome is None:
        return None
    failure = outcome.get("failure")
    if failure is not None:
        return (
            "failure",
            str(failure["code"]),
            tuple(failure["clause_ids"]),
            tuple(
                str(endpoint["role"])
                for endpoint in failure.get("missing_endpoints", ())
            ),
        )
    return (
        "matched",
        str(outcome["clause_id"]),
        json.dumps(outcome.get("narrowing"), sort_keys=True, default=str),
    )


def _evaluate_record_sources(
    *,
    profile: ProducerProfile,
    nodes: Any,
    edges: Any,
    endpoint_scope: str,
    ancestors: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    """Evaluate one emission, given as groups of equivalent records.

    ``nodes`` and ``edges`` are record sources (``_ListRecordSource`` or
    ``_ChronosRunRecords``). A group's records share every property a check
    below reads, so one evaluation of the group's first record stands for all
    of them; counts are summed over groups, and offending samples are
    resolved back to the earliest concrete records. Groups arrive in the
    order of their first record, nodes before edges, which is the order the
    record-by-record loop visited them.
    """
    if profile.status == "compat":
        return {
            "producer_policy": {
                **profile.evidence(),
                "status": "compat",
                "matched_clause_ids": [],
                "failed_clause_ids": [],
                "record_counts": {
                    "node": nodes.insert_count,
                    "edge": edges.insert_count,
                },
                "warnings": ["producer_signature_missing"],
            },
            "catalog": {
                "catalog_version_id": profile.catalog_version_id,
                "narrowings": [],
            },
        }
    if not profile.output_signature:
        raise _violation(
            profile=profile,
            code="producer_signature_missing",
            message="producer profile has no output_signature",
            failed_clause_ids=[],
            offending_records=[],
        )

    allowed_kinds = set(profile.allowed_record_kinds)
    if nodes.total and "node" not in allowed_kinds:
        raise _violation(
            profile=profile,
            code="producer_record_kind_not_allowed",
            message="producer is not allowed to emit node records",
            failed_clause_ids=[],
            offending_records=_sample_records(
                "node", nodes.first(_OFFENDING_SAMPLE_LIMIT),
            ),
        )
    if edges.total and "edge" not in allowed_kinds:
        raise _violation(
            profile=profile,
            code="producer_record_kind_not_allowed",
            message="producer is not allowed to emit edge records",
            failed_clause_ids=[],
            offending_records=_sample_records(
                "edge", edges.first(_OFFENDING_SAMPLE_LIMIT),
            ),
        )

    matched_clause_ids: set[str] = set()
    failed_clause_ids: set[str] = set()
    catalog_narrowings: list[dict[str, Any]] = []
    node_rules = list(profile.output_signature.get("nodes") or [])
    edge_rules = list(profile.output_signature.get("edges") or [])

    # Envelope dimensions beyond the signature (spec §9.3): an unbounded
    # dimension is exempt from producer-signature matching (the catalog's
    # graph-shape law still applies downstream — unbounded bounds the
    # PRODUCER's declaration, and forfeits skippability, not admission);
    # closed value vocabularies reject well-typed out-of-vocabulary values
    # as envelope violations, never widening the vocabulary at runtime.
    envelope = profile_write_envelope(profile) or {}
    unbounded_dims = set(envelope.get("unbounded") or ())
    vocabularies = {
        str(field): {str(v) for v in values}
        for field, values in (envelope.get("value_vocabularies") or {}).items()
        if isinstance(values, (list, tuple, set, frozenset))
    }

    def _vocabulary_failure(
        record: Mapping[str, Any], kind: str,
    ) -> tuple[str, str] | None:
        """The closed-vocabulary check as a verdict, not an exception.

        Returning the failure instead of raising is what lets the caller
        finish the batch and report an exact offending count (issue #1330).
        """
        if not vocabularies or "values" in unbounded_dims:
            return None
        attrs = record.get("attrs") or {}
        if not isinstance(attrs, Mapping):
            return None
        for field, allowed_values in vocabularies.items():
            if field not in attrs:
                continue
            value = attrs.get(field)
            if str(value) not in allowed_values:
                return (
                    "envelope_vocabulary_violation",
                    (
                        f"{kind} record carries {field}={value!r}, outside "
                        f"the producer's declared closed vocabulary "
                        f"(spec §9.3: a well-typed out-of-envelope value "
                        f"is recorded and rejected, never widened into the "
                        f"envelope)"
                    ),
                )
        return None

    def _node_outcome(record: Mapping[str, Any]) -> dict[str, Any] | None:
        """One node record's verdict: a failure, a matched clause, or None.

        None means the record is exempt (unbounded node dimension).
        """
        vocabulary_failure = _vocabulary_failure(record, "node")
        if vocabulary_failure is not None:
            return {"failure": {
                "code": vocabulary_failure[0],
                "message": vocabulary_failure[1],
                "kind": "node",
                "clause_ids": (),
            }}
        if "nodes" in unbounded_dims:
            return None
        rule_index, rule = _match_node_rule(
            node_rules, record, ancestors=ancestors,
        )
        if rule is None:
            return {"failure": {
                "code": "producer_node_subtype_not_allowed",
                "message": "node record does not match producer signature",
                "kind": "node",
                "clause_ids": tuple(_candidate_node_clause_ids(
                    profile, str(record["subtype"]),
                )),
            }}
        return {"clause_id": _rule_clause_id(
            rule,
            fallback=f"signature:nodes:{rule_index}",
        )}

    def _edge_outcome(record: Mapping[str, Any]) -> dict[str, Any] | None:
        """One edge record's verdict: a failure, a matched clause, or None.

        None means the record is exempt (unbounded edge dimension).
        """
        vocabulary_failure = _vocabulary_failure(record, "edge")
        if vocabulary_failure is not None:
            return {"failure": {
                "code": vocabulary_failure[0],
                "message": vocabulary_failure[1],
                "kind": "edge",
                "clause_ids": (),
            }}
        if "edges" in unbounded_dims:
            return None
        rule_index, rule = _match_edge_rule(
            edge_rules, record, ancestors=ancestors,
        )
        if rule is None:
            # Which defect is this? An endpoint whose subtype did not resolve
            # is a node that does not exist, not a signature the producer got
            # wrong. The endpoint diagnosis is claimed ONLY when the missing
            # endpoint is the sole blocker: re-match with the unresolved
            # endpoint treated as a wildcard, and if the record would then
            # satisfy a declared rule, the signature is genuinely not at
            # issue. An edge that ALSO carries an undeclared edge_type or a
            # wrong resolved endpoint keeps the signature code -- reporting
            # that one as a missing endpoint would be the mirror image of the
            # defect this change exists to fix. The record is refused either
            # way; only the reported cause differs.
            missing = _missing_endpoints(record)
            _, wildcard_rule = (
                _match_edge_rule(
                    edge_rules,
                    record,
                    ancestors=ancestors,
                    unresolved_endpoint_wildcard=True,
                )
                if missing else (-1, None)
            )
            if missing and wildcard_rule is not None:
                return {"failure": {
                    "code": EDGE_ENDPOINT_MISSING_CODE,
                    "message": _missing_endpoint_message(
                        record, missing, endpoint_scope=endpoint_scope,
                    ),
                    "kind": "edge",
                    "clause_ids": (),
                    "missing_endpoints": missing,
                }}
            return {"failure": {
                "code": "producer_edge_triple_not_allowed",
                "message": "edge record does not match producer signature",
                "kind": "edge",
                "clause_ids": tuple(_candidate_edge_clause_ids(
                    profile,
                    str(record.get("src_subtype") or ""),
                    str(record["edge_type"]),
                    str(record.get("dst_subtype") or ""),
                )),
            }}
        return {
            "clause_id": _rule_clause_id(
                rule,
                fallback=f"signature:edges:{rule_index}",
            ),
            "narrowing": {
                "src_subtype": record.get("src_subtype"),
                "edge_type": record["edge_type"],
                "dst_subtype": record.get("dst_subtype"),
                "status": "matched",
            },
        }

    # Failures are COLLECTED, not raised on sight. The refusal is still
    # whole-batch and fail-closed (the derived-fact loader gates edges by the
    # envelope's fact-range allow-list and nodes by a per-run deny-list, so the
    # envelope is the unit of admission), but a batch refused whole must be
    # able to say how much it discarded and how much of that was admissible.
    # Raising on the first offender made both counts unknowable (issue #1330).
    #
    # Only a bounded summary per failure code is kept: its record count, its
    # clause ids, and the earliest few record references for the sample.
    # Codes are recorded in the order they are first met.
    failure_codes: dict[str, dict[str, Any]] = {}
    narrowing_seen: set[str] = set()
    kind_order = {"node": 0, "edge": 1}

    for kind, record_source, outcome_of in (
        ("node", nodes, _node_outcome),
        ("edge", edges, _edge_outcome),
    ):
        for group in record_source.groups():
            outcome = outcome_of(group.record)
            if group.check_record is not None:
                # Admit-path guard: the grouping claims every record of the
                # group gets this verdict. Check it on the group's last
                # record too. This catches a grouping defect only when the
                # first and last records differ in the missed property; the
                # records between them are not re-evaluated, so the guard is
                # a backstop and the key coverage is pinned by tests
                # (test_group_plan_covers_every_rule_and_envelope_key).
                check_verdict = _verdict_shape(
                    outcome_of(group.check_record),
                )
                if check_verdict != _verdict_shape(outcome):
                    raise _signature_grouping_error(
                        kind, group.record, group.check_record,
                        _verdict_shape(outcome), check_verdict,
                        stage="group guard (first and last record)",
                    )
            if outcome is None:
                continue
            failure = outcome.get("failure")
            if failure is not None:
                summary = failure_codes.setdefault(str(failure["code"]), {
                    "count": 0,
                    "clause_ids": set(),
                    "head": [],
                })
                summary["count"] += group.count
                summary["clause_ids"].update(failure["clause_ids"])
                summary["head"] = sorted(
                    [
                        *summary["head"],
                        *(
                            (kind_order[kind], position, kind, reference)
                            for position, reference in group.head
                        ),
                    ],
                    key=lambda entry: (entry[0], entry[1]),
                )[:_OFFENDING_SAMPLE_LIMIT]
                continue
            matched_clause_ids.add(str(outcome["clause_id"]))
            narrowing = outcome.get("narrowing")
            if narrowing is not None:
                encoded = json.dumps(narrowing, sort_keys=True, default=str)
                if encoded not in narrowing_seen:
                    narrowing_seen.add(encoded)
                    catalog_narrowings.append(dict(narrowing))

    node_insert_count = nodes.insert_count
    edge_insert_count = edges.insert_count
    if failure_codes:
        # The first failure decides the code, exactly as the raise-on-sight
        # order did; every other record failing the SAME way is then counted,
        # so `offending_record_count` is measured rather than assumed.
        code = next(iter(failure_codes))
        summary = failure_codes[code]
        failed_clause_ids.update(summary["clause_ids"])
        sampled: list[tuple[str, Mapping[str, Any]]] = []
        for kind, record_source in (("node", nodes), ("edge", edges)):
            head = [
                (position, reference)
                for _, position, entry_kind, reference in summary["head"]
                if entry_kind == kind
            ]
            if head:
                sampled.extend(
                    (kind, record) for record in record_source.resolve(head)
                )
        details: list[dict[str, Any]] = []
        for kind, record in sampled:
            outcome = (
                _node_outcome(record) if kind == "node"
                else _edge_outcome(record)
            )
            failure = (outcome or {}).get("failure")
            if failure is None or failure["code"] != code:
                # A sampled record must fail exactly as its group did. If it
                # does not, the grouping missed a property a check reads.
                raise _signature_grouping_error(
                    kind, record, None,
                    code, (failure or {}).get("code"),
                    stage="offending sample check",
                )
            details.append({**failure, "record": record})
        offending_records: list[dict[str, Any]] = []
        for detail in details:
            offending_records.extend(
                _sample_records(str(detail["kind"]), [detail["record"]]),
            )
        missing_endpoints = [
            endpoint
            for detail in details
            for endpoint in detail.get("missing_endpoints", ())
        ]
        by_code = {
            failure_code: int(failure_summary["count"])
            for failure_code, failure_summary in failure_codes.items()
        }
        failing = sum(by_code.values())
        offending = int(summary["count"])
        evaluated = node_insert_count + edge_insert_count
        # Collateral excludes EVERY failing record, not merely the ones
        # sharing the reported code: a record refused under a second code was
        # never admissible, and counting it as collateral would overstate what
        # the refusal cost.
        raise _violation(
            profile=profile,
            code=code,
            message=_batch_violation_message(
                str(details[0]["message"]),
                offending,
                evaluated,
                collateral=evaluated - failing,
            ),
            failed_clause_ids=sorted(failed_clause_ids),
            offending_records=offending_records,
            record_counts={
                "node": node_insert_count,
                "edge": edge_insert_count,
                # Retractions travel in the same emission and are discarded
                # with it, but they are not signature-evaluated; name them
                # rather than let the totals quietly disagree.
                "non_insert": (nodes.total + edges.total - evaluated),
            },
            offending_record_count=offending,
            offending_record_counts_by_code=by_code,
            collateral_record_count=evaluated - failing,
            missing_endpoints=missing_endpoints or None,
            sample_truncated=offending > _OFFENDING_SAMPLE_LIMIT,
        )

    return {
        "producer_policy": {
            **profile.evidence(),
            "status": "matched",
            "matched_clause_ids": sorted(matched_clause_ids),
            "failed_clause_ids": sorted(failed_clause_ids),
            "record_counts": {
                "node": node_insert_count,
                "edge": edge_insert_count,
            },
        },
        "catalog": {
            "catalog_version_id": profile.catalog_version_id,
            "narrowings": catalog_narrowings,
        },
    }


def _normalize_signature_list(
    value: Any,
    *,
    source_name: str,
    field: str,
    required_keys: Sequence[str],
    allowed_keys: frozenset[str],
    strict: bool = False,
) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(
            f"source {source_name!r}: "
            f"admission_policy.output_signature.{field} must be a list, "
            f"got {type(value).__name__}"
        )
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.output_signature.{field}[{index}] "
                f"must be a mapping, got {type(raw).__name__}"
            )
        extras = sorted(set(raw) - allowed_keys)
        if extras:
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.output_signature.{field}[{index}] "
                "uses unsupported predicate keys "
                f"{extras!r}; code=producer_policy_unsupported_predicate"
            )
        for required in required_keys:
            if not isinstance(raw.get(required), str) or not raw[required]:
                raise ValueError(
                    f"source {source_name!r}: "
                    f"admission_policy.output_signature.{field}[{index}]."
                    f"{required} is required"
                )
        attrs = raw.get("attrs")
        if (
            ("attrs" in raw if strict else attrs is not None)
            and not isinstance(attrs, dict)
        ):
            raise ValueError(
                f"source {source_name!r}: "
                f"admission_policy.output_signature.{field}[{index}]."
                "attrs must be a mapping"
            )
        attr_keys = raw.get("attr_keys")
        if "attrs_exact" in raw and not isinstance(raw["attrs_exact"], bool):
            raise ValueError(
                f"source {source_name!r}: admission_policy."
                f"output_signature.{field}[{index}].attrs_exact "
                "must be a bool"
            )
        if attr_keys is not None:
            if (
                not isinstance(attr_keys, list)
                or not all(isinstance(key, str) and key for key in attr_keys)
                or len(set(attr_keys)) != len(attr_keys)
            ):
                raise ValueError(
                    f"source {source_name!r}: admission_policy."
                    f"output_signature.{field}[{index}].attr_keys "
                    "must be a list of unique non-empty strings"
                )
            if raw.get("attrs_exact") is not True:
                raise ValueError(
                    f"source {source_name!r}: admission_policy."
                    f"output_signature.{field}[{index}].attr_keys "
                    "requires attrs_exact=true"
                )
            if isinstance(attrs, dict) and not set(attrs).issubset(attr_keys):
                raise ValueError(
                    f"source {source_name!r}: admission_policy."
                    f"output_signature.{field}[{index}].attrs keys "
                    "must be included in attr_keys"
                )
        for optional_string in (
            "id_prefix",
            "src_id_prefix",
            "dst_id_prefix",
            "clause_id",
        ):
            if strict and optional_string in raw and not isinstance(
                raw[optional_string], str
            ):
                raise ValueError(
                    f"source {source_name!r}: admission_policy."
                    f"output_signature.{field}[{index}].{optional_string} "
                    "must be a string"
                )
        for optional_bool in (
            "src_subtype_optional",
            "dst_subtype_optional",
        ):
            if strict and optional_bool in raw and not isinstance(
                raw[optional_bool], bool
            ):
                raise ValueError(
                    f"source {source_name!r}: admission_policy."
                    f"output_signature.{field}[{index}].{optional_bool} "
                    "must be a bool"
                )
        normalized_rule = dict(raw)
        if attrs is not None:
            normalized_rule["attrs"] = dict(attrs)
        if attr_keys is not None:
            normalized_rule["attr_keys"] = list(attr_keys)
        normalized.append(normalized_rule)
    return normalized


def _normalize_output_scope_summary(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        return {"summary": value}
    if isinstance(value, Mapping):
        return dict(value)
    raise ProducerPolicyViolation(
        code="producer_policy_invalid",
        message="output_scope_summary must be a mapping or string",
        verdict={"code": "producer_policy_invalid"},
    )


def _subtype_matches(
    rule_subtype: str,
    record_subtype: str,
    ancestors: Mapping[str, set[str]] | None,
) -> bool:
    """Subtype match with optional refinement closure.

    Per `add-subtype-refines-to-catalog`: a producer profile
    declaring an abstract parent subtype matches emissions whose
    concrete subtypes refine that parent. When `ancestors` is
    None (legacy callers, pre-refinement tests), falls back to
    exact-match — strict superset of pre-refinement behavior.
    """
    if rule_subtype == record_subtype:
        return True
    if ancestors is None:
        return False
    # `ancestors[record_subtype]` is the set of names (parents
    # + reflexive self) the record's subtype is_a. Rule matches
    # iff its declared subtype is in that set.
    return rule_subtype in ancestors.get(record_subtype, set())


def _match_node_rule(
    rules: Sequence[Mapping[str, Any]],
    record: Mapping[str, Any],
    ancestors: Mapping[str, set[str]] | None = None,
) -> tuple[int, Mapping[str, Any] | None]:
    record_subtype = str(record["subtype"])
    for index, rule in enumerate(rules):
        if not _subtype_matches(
            str(rule["subtype"]), record_subtype, ancestors,
        ):
            continue
        prefix = rule.get("id_prefix")
        if prefix and not str(record["node_id"]).startswith(str(prefix)):
            continue
        if not _attrs_match(
            record.get("attrs") or {},
            rule.get("attrs"),
            exact=rule.get("attrs_exact") is True,
            exact_keys=rule.get("attr_keys"),
        ):
            continue
        return index, rule
    return -1, None


def _match_edge_rule(
    rules: Sequence[Mapping[str, Any]],
    record: Mapping[str, Any],
    ancestors: Mapping[str, set[str]] | None = None,
    *,
    unresolved_endpoint_wildcard: bool = False,
) -> tuple[int, Mapping[str, Any] | None]:
    """Find the signature rule this edge record satisfies.

    ``unresolved_endpoint_wildcard`` is a DIAGNOSTIC mode, never an
    admission mode: it asks whether an unresolved endpoint is the ONLY thing
    standing between the record and a declared rule, which is what separates
    "the endpoint node does not exist" from "this triple was never declared".
    Admission always calls it with the default, where an unresolved endpoint
    matches only a rule that opted in via src_subtype_optional /
    dst_subtype_optional.
    """
    record_src_subtype = str(record.get("src_subtype") or "")
    record_dst_subtype = str(record.get("dst_subtype") or "")
    for index, rule in enumerate(rules):
        if str(rule["edge_type"]) != str(record["edge_type"]):
            continue
        # src_subtype: mirror dst — a resolvable src must match the declared
        # subtype (refinement-aware); an unresolvable src (subtype lookup
        # returned None) only matches rules that opt in via src_subtype_optional.
        # Needed by pure edge-producers (e.g. code-lsp resolution) whose BOTH
        # endpoints are nodes owned by another source, so either may be
        # unresolvable at admission. The flag relaxes discoverability, not
        # subtype identity; the narrowing trigger at projection still enforces
        # the catalog against the actual endpoint nodes.
        if record_src_subtype:
            if not _subtype_matches(
                str(rule["src_subtype"]), record_src_subtype, ancestors,
            ):
                continue
        elif not (
            unresolved_endpoint_wildcard
            or rule.get("src_subtype_optional")
        ):
            continue
        # dst_subtype: resolvable dsts must match the declared subtype
        # (refinement-aware); unresolvable dsts (subtype lookup
        # returned None) only match rules that opt in via
        # dst_subtype_optional. The flag relaxes discoverability,
        # not subtype identity.
        if record_dst_subtype:
            if not _subtype_matches(
                str(rule["dst_subtype"]), record_dst_subtype, ancestors,
            ):
                continue
        elif not (
            unresolved_endpoint_wildcard
            or rule.get("dst_subtype_optional")
        ):
            continue
        src_prefix = rule.get("src_id_prefix")
        if src_prefix and not str(record["src"]).startswith(str(src_prefix)):
            continue
        dst_prefix = rule.get("dst_id_prefix")
        if dst_prefix and not str(record["dst"]).startswith(str(dst_prefix)):
            continue
        if not _attrs_match(
            record.get("attrs") or {},
            rule.get("attrs"),
            exact=rule.get("attrs_exact") is True,
            exact_keys=rule.get("attr_keys"),
        ):
            continue
        return index, rule
    return -1, None


def _load_subtype_ancestors(
    conn: psycopg.Connection,
    cv: int | None,
) -> dict[str, set[str]] | None:
    """Load the subtype-refinement closure for `cv`.

    Returns `{subtype: {ancestor_a, ancestor_b, ...}}` including
    each subtype's reflexive depth-0 self-ancestor. Returns None
    when `cv` is None or the closure table has no rows for cv
    (treated as pre-refinement state; callers fall back to
    exact-match). Cheap query — closure is small relative to the
    catalog.
    """
    if cv is None:
        return None
    out: dict[str, set[str]] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT subtype_name, ancestor_name "
            "  FROM okg.subtype_ancestors "
            " WHERE catalog_version_id = %s",
            (cv,),
        )
        for subtype_name, ancestor_name in cur.fetchall():
            out.setdefault(str(subtype_name), set()).add(
                str(ancestor_name)
            )
    return out if out else None


def _attrs_match(
    attrs: Mapping[str, Any],
    expected: Mapping[str, Any] | None,
    *,
    exact: bool = False,
    exact_keys: Sequence[str] | None = None,
) -> bool:
    if exact and set(attrs) != set(exact_keys or (expected or {})):
        return False
    if not expected:
        return True
    for key, value in expected.items():
        if key not in attrs or attrs[key] != value:
            return False
    return True


def _candidate_node_clause_ids(
    profile: ProducerProfile,
    subtype: str,
) -> list[str]:
    return [
        str(clause["id"])
        for clause in profile.policy_clauses
        if clause.get("record_kind") == "node"
        and str(clause.get("subtype")) == subtype
    ]


def _candidate_edge_clause_ids(
    profile: ProducerProfile,
    src_subtype: str,
    edge_type: str,
    dst_subtype: str,
) -> list[str]:
    out = []
    expected = [src_subtype, edge_type, dst_subtype]
    for clause in profile.policy_clauses:
        if clause.get("record_kind") == "edge" and clause.get("triple") == expected:
            out.append(str(clause["id"]))
    return out


def _rule_clause_id(rule: Mapping[str, Any], *, fallback: str) -> str:
    return str(rule.get("clause_id") or rule.get("id") or fallback)


def _sample_records(
    record_kind: str,
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for record in records[:5]:
        sample = {"record_kind": record_kind}
        for key in (
            "fact_id", "node_id", "subtype", "src", "src_subtype",
            "edge_type", "dst", "dst_subtype", "attrs",
        ):
            if key in record:
                sample[key] = record[key]
        out.append(sample)
    return out


def _missing_endpoints(
    record: Mapping[str, Any],
) -> list[dict[str, str]]:
    """The endpoint roles of ``record`` whose subtype did not resolve.

    An unresolved subtype means the endpoint node is absent from the graph
    checkout and from the same-cycle node-fact window; it is never a
    signature question.
    """
    missing: list[dict[str, str]] = []
    for role in ("src", "dst"):
        if not record.get(f"{role}_subtype"):
            missing.append({
                "role": role,
                "node_id": str(record.get(role) or ""),
            })
    return missing


def _missing_endpoint_message(
    record: Mapping[str, Any],
    missing: Sequence[Mapping[str, str]],
    *,
    endpoint_scope: str,
) -> str:
    """Report the unresolved endpoint WITHOUT overclaiming its absence.

    Only the scope this evaluation actually searched can be spoken for. The
    payload path sees the nodes in the batch; the chronos source-run path sees
    the default-branch graph; the fact-range paths add the node-fact window.
    Saying "the node does not exist" would repeat, in the other direction, the
    misdirection issue #1330 is about.
    """
    endpoints = ", ".join(
        f"{item['role']} node {item['node_id']!r}" for item in missing
    )
    edge_type = str(record.get("edge_type") or "")
    return (
        f"edge record references an endpoint whose subtype is not resolvable "
        f"in {endpoint_scope}: {endpoints} (edge_type {edge_type!r}). The "
        f"rest of the record satisfies a declared rule, so what refused it is "
        f"the unresolvable endpoint and not the producer signature"
    )


def _batch_violation_message(
    message: str,
    offending: int,
    evaluated: int,
    *,
    collateral: int,
) -> str:
    """Append the blast radius to a per-record refusal message.

    Admission refuses the whole emission, so the operator needs both numbers
    to judge the cost: how many records offended under the reported code, and
    how many records that broke no rule went with them (issue #1330).
    """
    if evaluated <= 1 and offending <= 1:
        return message
    return (
        f"{message} [{offending} of {evaluated} evaluated insert record(s) "
        f"offended under this code; the whole emission is refused, "
        f"discarding {collateral} admissible record(s)]"
    )


_OFFENDING_SAMPLE_LIMIT = 5


def _violation(
    *,
    profile: ProducerProfile,
    code: str,
    message: str,
    failed_clause_ids: Sequence[str],
    offending_records: Sequence[Mapping[str, Any]],
    record_counts: Mapping[str, int] | None = None,
    offending_record_count: int | None = None,
    offending_record_counts_by_code: Mapping[str, int] | None = None,
    collateral_record_count: int | None = None,
    missing_endpoints: Sequence[Mapping[str, str]] | None = None,
    sample_truncated: bool = False,
) -> ProducerPolicyViolation:
    producer_policy: dict[str, Any] = {
        **profile.evidence(),
        "status": "rejected",
        "matched_clause_ids": [],
        "failed_clause_ids": list(failed_clause_ids),
        "offending_records": [
            dict(record) for record in offending_records
        ],
    }
    if record_counts is not None:
        producer_policy["record_counts"] = dict(record_counts)
    if missing_endpoints:
        producer_policy["missing_endpoints"] = [
            dict(item) for item in missing_endpoints
        ]
    verdict: dict[str, Any] = {
        "code": code,
        "message": message,
        "producer_policy": producer_policy,
        "catalog": {
            "catalog_version_id": profile.catalog_version_id,
        },
    }
    if sample_truncated:
        # offending_records and missing_endpoints are SAMPLES. Say so, so a
        # reader never mistakes len(offending_records) for the count.
        producer_policy["offending_records_truncated"] = True
        producer_policy["offending_record_sample_limit"] = (
            _OFFENDING_SAMPLE_LIMIT
        )
    if offending_record_count is not None:
        verdict["offending_record_count"] = int(offending_record_count)
    if offending_record_counts_by_code:
        verdict["offending_record_counts_by_code"] = {
            str(key): int(value)
            for key, value in offending_record_counts_by_code.items()
        }
    if collateral_record_count is not None:
        # The admissible records this whole-emission refusal discarded. Named
        # at the top of the verdict because that is where a health reader can
        # find it without parsing the policy block (issue #1330).
        verdict["collateral_record_count"] = int(collateral_record_count)
    return ProducerPolicyViolation(
        code=code,
        message=message,
        verdict=verdict,
    )
