"""Reuse certificates — the persisted justification behind every cone skip.

PACT ``consistency-resolution-aware-reuse``, task.reuse.certificate-store
(req.reuse.certificates). Normative semantics in
docs/develop/architecture/consistency.md §9.5; identifiers pinned by the ratified
notation map (:class:`ReuseCertificate`, store table ``okg.reuse_certificates``).

What a certificate is
---------------------
A step inside the resolved state's dependency cone may be *skipped* (its prior
result kept) ONLY behind a persisted :class:`ReuseCertificate` that records two
things (§9.5):

1. **the comparison that justified reuse** — the incumbent decision's worst
   outcome against competitors' best under every permitted output, tie-break
   included where score alone does not decide. The v1 shape carries the
   incumbent outcome + basis, the effects-summary digest, and the competitor
   bound statement; the scheduler wave populates richer bounds into the SAME
   ``comparison`` object without a redesign. The comparison is a JSON object
   validated loudly against its declared ``kind``: the effects-backed
   ``resolution_dominance`` schema (:data:`REQUIRED_COMPARISON_KEYS`) or the
   ``degenerate_unchanged_inputs`` schema (:data:`DEGENERATE_COMPARISON_KEYS`)
   for the unchanged-inputs carry-forward case (§9.5). The degenerate schema is
   a distinct, fully specified contract — never a loosened resolution one — so
   exactly one skip mechanism exists on the cone with no un-validated shape.

2. **every versioned fact that comparison consumed** — a typed, per-class
   mapping (:class:`CertificateDependencies`): input digests, operator
   versions, model/prompt keys (OutcomeKey digests), the write-envelope
   fingerprint, the constraint set (check id -> checksum), generator versions,
   the policy and precedence fingerprints, competitor bounds, and the catalog
   version id.

Validity is EVALUATED, never STORED
-----------------------------------
The row carries NO ``status`` / ``invalidated_at`` flag. A later change to any
recorded dependency invalidates the certificate — but that fact is COMPUTED at
decision time by :func:`validate_certificate`, which compares each recorded
dependency class against the current facts and returns EXACTLY the invalidated
classes (an empty set means valid). A stored validity flag would be a second
source of truth that could drift from the facts; §9.5 forbids it.

Recording a skip WITHOUT a certificate is structurally impossible: the store's
:meth:`ReuseCertificateStore.record_skip` takes a :class:`ReuseCertificate`
(not an optional) — that is the load-bearing shape behind
``test_every_cone_skip_has_certificate`` at this layer (the scheduler wave
re-proves it end-to-end). The store is append-only: rows are keyed
``(deployment_name, generation_id, step_id)`` and only ever inserted.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError

logger = logging.getLogger(__name__)

REUSE_CERTIFICATES_TABLE = "okg.reuse_certificates"

# The dependency classes a certificate records, in a fixed order. Each name is
# both an attribute of :class:`CertificateDependencies` and the token
# :func:`validate_certificate` returns when that class has changed. The set is
# closed: a dependencies payload missing any class is a loud defect.
DEPENDENCY_CLASSES: tuple[str, ...] = (
    "input_digests",         # input key -> content digest
    "operator_versions",     # operator id -> declared version
    "model_prompt_keys",     # step id -> OutcomeKey digest (omega, §9.2)
    "envelope_fingerprint",  # producer write-envelope fingerprint (§9.3)
    "constraint_set",        # check id -> definition/structural checksum
    "generator_versions",    # repair generator id -> version
    "policy_fingerprint",    # predicate-policy fingerprint
    "precedence_fingerprint",  # claim-precedent fingerprint
    "competitor_bounds",     # per-competitor bound statement(s)
    "catalog_version_id",    # active catalog version id
)

# The comparison object's required keys (§9.5 part (a), v1 shape). Extra keys
# are permitted so the scheduler wave can carry richer bounds without a
# redesign; a MISSING required key is a loud refusal.
REQUIRED_COMPARISON_KEYS: frozenset[str] = frozenset({
    "incumbent_outcome",       # the incumbent decision's resolved outcome
    "incumbent_basis",         # the deciding basis (Resolution basis class)
    "effects_summary_digest",  # digest of the possible-effects summary (§9.4)
    "competitor_bound",        # the competitors'-best bound statement
    "tie_break",               # tie-break fields (may be null when score decides)
    "fact_coverage",           # per-fact premise map (§9.5, four-fact contract)
})

# The four harmlessness facts (§9.5; adjudication v2, verdict F3). A skip is
# harmless only when every state change the effects summary allows preserves
# all four. Every RESOLUTION comparison must declare, per fact, which of its
# premises support it — a certificate that cannot say why the optimum set is
# preserved is not a certificate. The DEGENERATE kind is exempt: byte-identical
# inputs trivially preserve all four (its `unchanged_inputs` statement IS the
# coverage).
HARMLESSNESS_FACTS: frozenset[str] = frozenset({
    "qualified_exists",
    "optimum_set_unchanged",
    "canonical_selection_unchanged",
    "selected_outputs_unchanged",
})

# The v1 premise map every resolution-kind builder stamps: the honest
# statement of which existing premise argues each fact. Builders import this
# so writers and the validator share one source.
RESOLUTION_V1_FACT_COVERAGE: dict[str, list[str]] = {
    "qualified_exists": ["incumbent_outcome", "incumbent_basis"],
    "optimum_set_unchanged": ["competitor_bound", "effects_summary_digest"],
    "canonical_selection_unchanged": ["incumbent_basis", "tie_break"],
    "selected_outputs_unchanged": ["effects_summary_digest"],
}

# The comparison "kind" discriminator (§9.5). Every comparison declares which
# justification shape it carries; validation dispatches on it so the DEGENERATE
# (unchanged-inputs carry-forward) case is accepted through its OWN required-key
# contract — never by loosening the resolution schema. ``kind`` is MANDATORY:
# every certificate writer stamps it (``build_skip_comparison`` /
# ``build_dominance_comparison`` -> ``resolution_dominance``,
# ``build_degenerate_comparison`` -> ``degenerate_unchanged_inputs``), so the
# original kind-less tolerance is retired (PACT ``consistency-single-engine``,
# comparison-kind tightening). A missing OR unknown ``kind`` is a loud refusal —
# never read as a default shape.
COMPARISON_KIND_FIELD = "kind"
COMPARISON_KIND_RESOLUTION = "resolution_dominance"          # effects-backed / valid-cert reuse
COMPARISON_KIND_DEGENERATE = "degenerate_unchanged_inputs"   # unchanged-inputs carry-forward

# The DEGENERATE comparison's required keys (§9.5, the degenerate certificate).
# Its justification is TRIVIAL — every recorded dependency is byte-identical to
# the prior generation's — so it carries no effects summary or competitor bound;
# instead it declares the unchanged-inputs justification explicitly. This is a
# DISTINCT, fully specified schema, not a subset of the resolution schema: a
# degenerate comparison missing any of these keys is refused as loudly as a
# resolution one missing its own.
DEGENERATE_COMPARISON_KEYS: frozenset[str] = frozenset({
    COMPARISON_KIND_FIELD,   # must equal COMPARISON_KIND_DEGENERATE
    "incumbent_outcome",     # the carried-forward decision's outcome
    "incumbent_basis",       # its deciding basis
    "unchanged_inputs",      # the trivial justification (statement + matched classes)
})

# The scalar dependency classes (single fingerprints); the rest are structured
# (mappings / bound statements). Both are compared byte-stably by canonical JSON,
# so the split is only for construction-time type validation.
_SCALAR_CLASSES: frozenset[str] = frozenset({
    "envelope_fingerprint",
    "policy_fingerprint",
    "precedence_fingerprint",
    "catalog_version_id",
})


def _canonical_json(obj: Any) -> str:
    """sha-stable canonical JSON: sorted keys, compact separators.

    The single serialization discipline for the module — every digest,
    byte-stability, and per-class comparison rests on it. ``sort_keys`` makes
    dict insertion order irrelevant; lists keep their order."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def validate_comparison_shape(comparison: Mapping[str, Any]) -> str:
    """Validate a comparison payload against its declared kind; return the kind.

    Dispatches on the ``kind`` discriminator (§9.5): the DEGENERATE case is held
    to :data:`DEGENERATE_COMPARISON_KEYS`, the RESOLUTION case to
    :data:`REQUIRED_COMPARISON_KEYS`. ``kind`` is MANDATORY — an ABSENT kind is
    refused as loudly as an UNKNOWN one (the kind-less tolerance is retired now
    that every writer stamps a kind; PACT ``consistency-single-engine``). The
    degenerate schema is a distinct, fully specified contract, never a loosened
    resolution one. Extra keys are permitted; a MISSING required key is a loud
    refusal. Returns the resolved kind so callers can record it."""
    kind = comparison.get(COMPARISON_KIND_FIELD)
    if kind is None:
        raise SubstrateError(
            "ReuseCertificate.comparison is missing its `kind` discriminator — "
            "every comparison must declare its justification shape explicitly "
            "(spec §9.5); the kind-less tolerance is retired now that every "
            "certificate writer stamps a kind. A comparison without a kind is "
            "refused, never read as a default shape.",
            data={"present": sorted(comparison),
                  "allowed": [COMPARISON_KIND_RESOLUTION, COMPARISON_KIND_DEGENERATE]},
        )
    if kind == COMPARISON_KIND_DEGENERATE:
        required = DEGENERATE_COMPARISON_KEYS
    elif kind == COMPARISON_KIND_RESOLUTION:
        required = REQUIRED_COMPARISON_KEYS
    else:
        raise SubstrateError(
            "ReuseCertificate.comparison declares an unknown kind — a comparison "
            "is validated against its declared schema, never a looser one (spec "
            "§9.5); an unrecognized kind is refused, not silently accepted",
            data={"kind": kind,
                  "allowed": [COMPARISON_KIND_RESOLUTION, COMPARISON_KIND_DEGENERATE]},
        )
    missing = sorted(required - set(comparison))
    if missing:
        raise SubstrateError(
            "ReuseCertificate.comparison is missing required keys — the "
            "justifying comparison must state, for its declared kind, the full "
            "required-key set (spec §9.5); a malformed comparison is refused, "
            "never patched with defaults",
            data={"kind": kind, "missing": missing,
                  "present": sorted(comparison)},
        )
    if kind == COMPARISON_KIND_RESOLUTION:
        _validate_fact_coverage(comparison)
    return kind


def _validate_fact_coverage(comparison: Mapping[str, Any]) -> None:
    """Enforce the four-fact contract (§9.5; adjudication v2, F3).

    A resolution comparison's ``fact_coverage`` must name EXACTLY the four
    harmlessness facts, each supported by a non-empty list of premises, and
    every named premise must be a key actually present on the comparison —
    coverage citing a phantom premise is as refusable as no coverage at all.
    """
    coverage = comparison["fact_coverage"]
    if not isinstance(coverage, Mapping):
        raise SubstrateError(
            "ReuseCertificate.comparison.fact_coverage must be a mapping from "
            "harmlessness fact to its supporting premises (spec §9.5, four-fact "
            "contract); a malformed coverage is refused, never defaulted",
            data={"got_type": type(coverage).__name__},
        )
    declared = set(coverage)
    if declared != HARMLESSNESS_FACTS:
        raise SubstrateError(
            "ReuseCertificate.comparison.fact_coverage must cover EXACTLY the "
            "four harmlessness facts — a certificate that cannot say why the "
            "optimum set, selection, outputs, and qualification are preserved "
            "is not a certificate (spec §9.5; adjudication v2 F3)",
            data={
                "missing": sorted(HARMLESSNESS_FACTS - declared),
                "unknown": sorted(declared - HARMLESSNESS_FACTS),
            },
        )
    premise_universe = set(comparison) - {COMPARISON_KIND_FIELD, "fact_coverage"}
    for fact in sorted(HARMLESSNESS_FACTS):
        premises = coverage[fact]
        if not isinstance(premises, (list, tuple)) or not premises:
            raise SubstrateError(
                "ReuseCertificate.comparison.fact_coverage entry must be a "
                "non-empty list of premise keys — an unsupported fact means the "
                "skip is unjustified for that fact (spec §9.5)",
                data={"fact": fact, "got": premises},
            )
        phantom = sorted(set(premises) - premise_universe)
        if phantom:
            raise SubstrateError(
                "ReuseCertificate.comparison.fact_coverage cites premises that "
                "are not present on the comparison — coverage must reference "
                "real recorded premises, never phantom ones (spec §9.5)",
                data={"fact": fact, "phantom": phantom,
                      "present": sorted(premise_universe)},
            )


# --------------------------------------------------------------------------- #
# Dependencies — the typed, per-class record of consumed versioned facts.      #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CertificateDependencies:
    """Every versioned fact one reuse comparison consumed, by class (§9.5).

    Scalars are single fingerprints; the mappings key their versioned facts
    (operator id -> version, check id -> checksum, ...). ``competitor_bounds``
    is a JSON-shaped bound statement (a scalar summary in v1, a richer
    structure once the scheduler wave lands) — its shape is not constrained
    here beyond JSON-serializability, so the record carries the future bounds
    without a redesign."""

    input_digests: Mapping[str, str]
    operator_versions: Mapping[str, str]
    model_prompt_keys: Mapping[str, str]
    envelope_fingerprint: str
    constraint_set: Mapping[str, str]
    generator_versions: Mapping[str, str]
    policy_fingerprint: str
    precedence_fingerprint: str
    competitor_bounds: Any
    catalog_version_id: str

    def __post_init__(self) -> None:
        for name in _SCALAR_CLASSES:
            value = getattr(self, name)
            if not isinstance(value, str):
                raise SubstrateError(
                    f"CertificateDependencies.{name} must be a string "
                    f"fingerprint (a dependency class is never omitted or "
                    f"defaulted; spec §9.5)",
                    data={"class": name, "got_type": type(value).__name__},
                )
        for name in (
            "input_digests", "operator_versions", "model_prompt_keys",
            "constraint_set", "generator_versions",
        ):
            value = getattr(self, name)
            if not isinstance(value, Mapping):
                raise SubstrateError(
                    f"CertificateDependencies.{name} must be a mapping of "
                    f"versioned facts (spec §9.5)",
                    data={"class": name, "got_type": type(value).__name__},
                )
        # competitor_bounds must be JSON-serializable so it round-trips and
        # compares byte-stably — a non-serializable bound is a loud defect.
        try:
            _canonical_json(self.competitor_bounds)
        except (TypeError, ValueError) as exc:
            raise SubstrateError(
                "CertificateDependencies.competitor_bounds must be "
                "JSON-serializable (spec §9.5)",
                data={"error": str(exc)},
            ) from exc

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "input_digests": dict(self.input_digests),
            "operator_versions": dict(self.operator_versions),
            "model_prompt_keys": dict(self.model_prompt_keys),
            "envelope_fingerprint": self.envelope_fingerprint,
            "constraint_set": dict(self.constraint_set),
            "generator_versions": dict(self.generator_versions),
            "policy_fingerprint": self.policy_fingerprint,
            "precedence_fingerprint": self.precedence_fingerprint,
            "competitor_bounds": self.competitor_bounds,
            "catalog_version_id": self.catalog_version_id,
        }

    def canonical_json(self) -> str:
        return _canonical_json(self.to_json_dict())

    @classmethod
    def from_json_dict(
        cls, payload: Mapping[str, Any]
    ) -> "CertificateDependencies":
        missing = [c for c in DEPENDENCY_CLASSES if c not in payload]
        if missing:
            raise SubstrateError(
                "CertificateDependencies payload is missing dependency "
                "classes — a certificate never omits a class (spec §9.5)",
                data={"missing": missing},
            )
        return cls(
            input_digests=dict(payload["input_digests"]),
            operator_versions=dict(payload["operator_versions"]),
            model_prompt_keys=dict(payload["model_prompt_keys"]),
            envelope_fingerprint=str(payload["envelope_fingerprint"]),
            constraint_set=dict(payload["constraint_set"]),
            generator_versions=dict(payload["generator_versions"]),
            policy_fingerprint=str(payload["policy_fingerprint"]),
            precedence_fingerprint=str(payload["precedence_fingerprint"]),
            competitor_bounds=payload["competitor_bounds"],
            catalog_version_id=str(payload["catalog_version_id"]),
        )


# --------------------------------------------------------------------------- #
# The certificate.                                                             #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReuseCertificate:
    """One skip justification: the comparison + the dependencies it consumed.

    ``comparison`` is a JSON object carrying at least
    :data:`REQUIRED_COMPARISON_KEYS`; ``dependencies`` is the typed per-class
    record. :attr:`digest` is the byte-stable content fingerprint over
    ``(step_id, comparison, dependencies)`` — the deployment / generation
    labels are execution identity (the row key), not justification content, so
    two byte-identical justifications share a digest (the degenerate
    carry-forward certificate is byte-stable)."""

    deployment: str
    generation_id: str
    step_id: str
    comparison: Mapping[str, Any]
    dependencies: CertificateDependencies

    def __post_init__(self) -> None:
        for name in ("deployment", "generation_id", "step_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise SubstrateError(
                    f"ReuseCertificate.{name} must be a non-empty string",
                    data={"field": name, "got": repr(value)},
                )
        if not isinstance(self.comparison, Mapping):
            raise SubstrateError(
                "ReuseCertificate.comparison must be a JSON object (spec §9.5)",
                data={"got_type": type(self.comparison).__name__},
            )
        # Validate against the comparison's DECLARED kind (resolution or the
        # degenerate unchanged-inputs case) — each schema is enforced loudly, and
        # the degenerate case never rides a loosened resolution schema (§9.5).
        validate_comparison_shape(self.comparison)
        # The whole comparison must be JSON-serializable so it round-trips and
        # compares byte-stably.
        try:
            _canonical_json(dict(self.comparison))
        except (TypeError, ValueError) as exc:
            raise SubstrateError(
                "ReuseCertificate.comparison must be JSON-serializable "
                "(spec §9.5)",
                data={"error": str(exc)},
            ) from exc
        if not isinstance(self.dependencies, CertificateDependencies):
            raise SubstrateError(
                "ReuseCertificate.dependencies must be a "
                "CertificateDependencies (spec §9.5)",
                data={"got_type": type(self.dependencies).__name__},
            )

    def _digest_payload(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "comparison": dict(self.comparison),
            "dependencies": self.dependencies.to_json_dict(),
        }

    @property
    def digest(self) -> str:
        return _sha256(_canonical_json(self._digest_payload()))

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "generation_id": self.generation_id,
            "step_id": self.step_id,
            "comparison": dict(self.comparison),
            "dependencies": self.dependencies.to_json_dict(),
            "digest": self.digest,
        }

    def canonical_json(self) -> str:
        return _canonical_json(self.to_json_dict())

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "ReuseCertificate":
        cert = cls(
            deployment=str(payload["deployment"]),
            generation_id=str(payload["generation_id"]),
            step_id=str(payload["step_id"]),
            comparison=dict(payload["comparison"]),
            dependencies=CertificateDependencies.from_json_dict(
                payload["dependencies"]
            ),
        )
        recorded_digest = payload.get("digest")
        if recorded_digest is not None and recorded_digest != cert.digest:
            raise SubstrateError(
                "ReuseCertificate digest does not match the recomputed digest "
                "— the certificate content was altered after signing (spec "
                "§9.5); a mismatched certificate is refused, never trusted",
                data={"recorded": recorded_digest, "recomputed": cert.digest},
            )
        return cert

    @classmethod
    def from_canonical_json(cls, text: str) -> "ReuseCertificate":
        return cls.from_json_dict(json.loads(text))


# --------------------------------------------------------------------------- #
# Validation — validity is evaluated against the current facts.                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ValidationResult:
    """The evaluated validity of one certificate against the current facts.

    ``invalidated_classes`` names EXACTLY the dependency classes whose current
    fact diverges from the recorded one; an empty set means the certificate is
    valid and the skip may stand. There is no stored flag — this is computed
    fresh at every decision (§9.5)."""

    invalidated_classes: frozenset[str]

    @property
    def valid(self) -> bool:
        return not self.invalidated_classes

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "invalidated_classes": sorted(self.invalidated_classes),
        }


def _coerce_current_facts(
    conn_or_current_facts: Any,
) -> CertificateDependencies:
    """Resolve the ``conn_or_current_facts`` argument to the current facts.

    Accepts a :class:`CertificateDependencies` (the snapshot of current facts
    the caller assembled) or its mapping form. A live DB connection is NOT
    accepted here: gathering the current per-class facts from a connection is
    the reuse scheduler wave's job (it holds the per-step context — which
    operators, checks, generators the step consumed). Passing a connection
    raises loudly rather than silently degrading."""
    if isinstance(conn_or_current_facts, CertificateDependencies):
        return conn_or_current_facts
    if isinstance(conn_or_current_facts, Mapping):
        return CertificateDependencies.from_json_dict(conn_or_current_facts)
    raise SubstrateError(
        "validate_certificate needs the current dependency facts as a "
        "CertificateDependencies (or its mapping form). A connection-driven "
        "current-fact gather is wired by the reuse scheduler wave; pass the "
        "snapshot it assembles (spec §9.5 — validity is EVALUATED, never a "
        "silent fallback)",
        data={"got_type": type(conn_or_current_facts).__name__},
    )


def dependency_classes_differ(
    recorded: CertificateDependencies,
    current: CertificateDependencies,
) -> frozenset[str]:
    """The dependency classes whose byte-serialization differs between two
    snapshots (§9.5).

    The single per-class byte comparison for the module: both
    :func:`validate_certificate` (recorded certificate vs current facts) and the
    reuse scheduler's degenerate-certificate test (current facts vs the prior
    generation's facts) rest on it, so "byte-identical" means EXACTLY the same
    thing in both places. Returns the differing classes; empty means the two
    snapshots are byte-identical across all ten classes."""
    return frozenset(
        cls
        for cls in DEPENDENCY_CLASSES
        if _canonical_json(getattr(recorded, cls))
        != _canonical_json(getattr(current, cls))
    )


def validate_certificate(
    conn_or_current_facts: Any,
    certificate: ReuseCertificate,
) -> ValidationResult:
    """Evaluate a certificate against the current facts (§9.5).

    Compares each recorded dependency class byte-stably against the current
    fact and returns EXACTLY the invalidated classes (empty = valid). A change
    to any single class invalidates the certificate before the next reuse
    decision; the comparison is per-class so the caller learns precisely which
    fact moved. A degenerate (unchanged-inputs) certificate invalidates through
    this exact path — validity is a function of the DEPENDENCIES, never of the
    comparison kind."""
    if not isinstance(certificate, ReuseCertificate):
        raise SubstrateError(
            "validate_certificate requires a ReuseCertificate",
            data={"got_type": type(certificate).__name__},
        )
    current = _coerce_current_facts(conn_or_current_facts)
    invalidated = dependency_classes_differ(certificate.dependencies, current)
    return ValidationResult(invalidated_classes=invalidated)


# --------------------------------------------------------------------------- #
# The append-only store.                                                       #
# --------------------------------------------------------------------------- #
_INSERT_SQL = f"""
INSERT INTO {REUSE_CERTIFICATES_TABLE}
  (deployment_name, generation_id, step_id, certificate_digest, comparison,
   dependencies)
VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
"""

_SELECT_COLS = (
    "deployment_name, generation_id, step_id, certificate_digest, comparison, "
    "dependencies"
)


class ReuseCertificateStore:
    """Append-only per-deployment store of reuse certificates (§9.5).

    Operates on a caller-supplied psycopg connection (the scheduler records a
    skip inside the publish cycle's transaction, so the certificate commits
    atomically with the maintenance state it justifies — spec §9.6). The API
    makes a certificate-less skip structurally impossible: :meth:`record_skip`
    takes a :class:`ReuseCertificate`, never an optional."""

    def __init__(self, *, deployment: str) -> None:
        if not deployment:
            raise SubstrateError(
                "ReuseCertificateStore requires a deployment label"
            )
        self.deployment = deployment

    def record_skip(self, conn: Any, certificate: ReuseCertificate) -> str:
        """Record one skip behind its certificate; return the digest.

        A skip cannot be recorded without a certificate — the type is enforced,
        so ``None`` or a bare dict is refused loudly. Append-only: the
        ``(deployment, generation_id, step_id)`` primary key means a second,
        divergent skip for the same step in one generation raises rather than
        silently overwriting."""
        if not isinstance(certificate, ReuseCertificate):
            raise SubstrateError(
                "record_skip requires a ReuseCertificate — a skip without a "
                "certificate is structurally impossible (spec §9.5). No "
                "step inside the resolved cone is skipped un-justified.",
                data={"got_type": type(certificate).__name__},
            )
        if certificate.deployment != self.deployment:
            raise SubstrateError(
                "record_skip: certificate deployment does not match the store",
                data={"store": self.deployment,
                      "certificate": certificate.deployment},
            )
        digest = certificate.digest
        with conn.cursor() as cur:
            cur.execute(
                _INSERT_SQL,
                (
                    certificate.deployment,
                    certificate.generation_id,
                    certificate.step_id,
                    digest,
                    _canonical_json(dict(certificate.comparison)),
                    certificate.dependencies.canonical_json(),
                ),
            )
        logger.info(
            "reuse_certificate recorded: deployment=%s gen=%s step=%s "
            "digest=%s",
            certificate.deployment, certificate.generation_id,
            certificate.step_id, digest[:12],
        )
        return digest

    def _row_to_certificate(self, row: tuple[Any, ...]) -> ReuseCertificate:
        deployment, generation_id, step_id, digest, comparison, dependencies = row
        cert = ReuseCertificate(
            deployment=str(deployment),
            generation_id=str(generation_id),
            step_id=str(step_id),
            comparison=dict(comparison),
            dependencies=CertificateDependencies.from_json_dict(dependencies),
        )
        if cert.digest != digest:
            raise SubstrateError(
                "stored reuse certificate digest does not match the recomputed "
                "digest — the row was corrupted after write (spec §9.5)",
                data={"deployment": str(deployment),
                      "generation_id": str(generation_id),
                      "step_id": str(step_id),
                      "stored": digest, "recomputed": cert.digest},
            )
        return cert

    def get(
        self, conn: Any, *, generation_id: int | str, step_id: str
    ) -> ReuseCertificate | None:
        """The certificate for one (generation, step), or ``None`` — never a
        fuzzy match."""
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_SELECT_COLS} FROM {REUSE_CERTIFICATES_TABLE} "
                f"WHERE deployment_name = %s AND generation_id = %s AND step_id = %s",
                (self.deployment, str(generation_id), step_id),
            )
            row = cur.fetchone()
        return None if row is None else self._row_to_certificate(row)

    def for_generation(
        self, conn: Any, generation_id: int | str
    ) -> list[ReuseCertificate]:
        """Every certificate for one generation, ordered by step (queryable per
        generation, §9.5)."""
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_SELECT_COLS} FROM {REUSE_CERTIFICATES_TABLE} "
                f"WHERE deployment_name = %s AND generation_id = %s "
                f"ORDER BY step_id",
                (self.deployment, str(generation_id)),
            )
            rows = cur.fetchall()
        return [self._row_to_certificate(r) for r in rows]

    def for_step(self, conn: Any, step_id: str) -> list[ReuseCertificate]:
        """Every certificate for one step across generations, ordered by
        generation (queryable per step, §9.5)."""
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_SELECT_COLS} FROM {REUSE_CERTIFICATES_TABLE} "
                f"WHERE deployment_name = %s AND step_id = %s "
                f"ORDER BY generation_id",
                (self.deployment, step_id),
            )
            rows = cur.fetchall()
        return [self._row_to_certificate(r) for r in rows]

    def quotable_rows(
        self,
        conn: Any,
        *,
        generation_id: int | str | None = None,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Supervisor-quotable one-line summaries of the matching certificates.

        Reads by generation and/or step and renders a compact row per
        certificate — the fields a supervisor quotes when auditing a skip: the
        identity, the short digest, the incumbent outcome/basis, the recorded
        catalog version, and the dependency-class count. Pure read; no
        mutation."""
        if generation_id is not None and step_id is not None:
            cert = self.get(conn, generation_id=generation_id, step_id=step_id)
            certs = [cert] if cert is not None else []
        elif generation_id is not None:
            certs = self.for_generation(conn, generation_id)
        elif step_id is not None:
            certs = self.for_step(conn, step_id)
        else:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_SELECT_COLS} FROM {REUSE_CERTIFICATES_TABLE} "
                    f"WHERE deployment_name = %s "
                    f"ORDER BY generation_id, step_id",
                    (self.deployment,),
                )
                rows = cur.fetchall()
            certs = [self._row_to_certificate(r) for r in rows]
        out: list[dict[str, Any]] = []
        for cert in certs:
            out.append({
                "deployment": cert.deployment,
                "generation_id": cert.generation_id,
                "step_id": cert.step_id,
                "digest": cert.digest[:16],
                "incumbent_outcome": cert.comparison.get("incumbent_outcome"),
                "incumbent_basis": cert.comparison.get("incumbent_basis"),
                "catalog_version_id": cert.dependencies.catalog_version_id,
                "dependency_classes": len(DEPENDENCY_CLASSES),
            })
        return out


__all__ = [
    "REUSE_CERTIFICATES_TABLE",
    "DEPENDENCY_CLASSES",
    "REQUIRED_COMPARISON_KEYS",
    "DEGENERATE_COMPARISON_KEYS",
    "COMPARISON_KIND_FIELD",
    "COMPARISON_KIND_RESOLUTION",
    "COMPARISON_KIND_DEGENERATE",
    "HARMLESSNESS_FACTS",
    "RESOLUTION_V1_FACT_COVERAGE",
    "CertificateDependencies",
    "ReuseCertificate",
    "ReuseCertificateStore",
    "ValidationResult",
    "validate_comparison_shape",
    "dependency_classes_differ",
    "validate_certificate",
]
