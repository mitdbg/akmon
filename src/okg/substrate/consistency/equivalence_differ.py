"""Exact three-way equivalence differ over one update's resolved state.

PACT ``consistency-complete-equivalence``, task.harness.differ
(req.harness.exact-diff). Normative semantics in
docs/develop/architecture/consistency.md §9.1 (the same-result contract:
equality is EXACT, there is no tolerance tier) and §9.7 (an update that
cannot replay every key is EXCLUDED with the missing keys named, reported
separately, NEVER counted as agreement).

What this module is
-------------------
Two things, and nothing else:

1. :class:`ResolvedStateSnapshot` — a canonical, deterministic extraction of
   ONE deployment-generation's resolved state from a psycopg connection, over
   the four equality families the contract fixes:

   * **effective claims** — ``okg.claim_effective`` kept rows
     (id, predicate, key, value, scope, provenance, applied correction, stale
     flag, element handle);
   * **resolution outcomes** — ``okg.claim_projection`` ``bucket_resolution``
     kept rows (bucket, outcome, winner, basis, ranked, superseded);
   * **selections** — ``okg.repair_selections`` bucket rows plus the
     whole-selection digest;
   * **open findings** — ``okg.observation_current`` open rows by the
     per-assignment finding identity (input_scope, normalized_predicate,
     canonical normalized_values) with severity carried as a field.

   The extraction is *pinned-honest*: the two Chronos interval kept tables
   (``claim_effective`` / ``claim_projection``) are read THROUGH a checkout
   pinned to the generation — exactly the discipline the claim-state IR check
   surface uses (tests/substrate/consistency/test_claim_state_surface.py), so
   interval visibility scopes the read to the one generation;
   ``repair_selections`` filters by its exact generation column, and
   ``observation_current`` filters AS-OF the generation (``latest_generation_id
   <= gen``, ``finding_status = 'open'`` — the same active-set resolution
   gates.py uses) so a finding carried forward unchanged from an earlier
   generation is captured, not dropped. The snapshot serializes to canonical
   JSON that is
   byte-stable across extraction order and across two extractions of the same
   generation (witnessed).

2. :func:`diff_resolved_states` — renders EXACTLY ONE of ``EQUAL``,
   ``MISMATCH``, or ``EXCLUDED``:

   * ``EXCLUDED`` DOMINATES: whenever ``exclusions`` is non-empty (a replay
     could not resolve every key) the verdict is EXCLUDED, it NAMES every
     missing key, and it carries ``counted_as_agreement=False`` structurally —
     it is never counted as agreement even if the two states would otherwise
     be equal (§9.7).
   * ``MISMATCH`` carries a machine-readable :class:`EquivalenceArtifact`
     naming EVERY divergent object: per family, ``added`` / ``removed`` object
     identities and per-field diffs for ``changed`` objects (plus the
     selection-digest scalar).
   * ``EQUAL`` otherwise.

There is NO tolerance tier — no epsilon, no ignore-list, no severity filter.
The function signature accepts NO knob that would loosen the exact comparison; a
caller cannot ask for a lenient one.

The ONE §9.7 licence: certified differences
-------------------------------------------
:func:`diff_resolved_states` takes an OPTIONAL ``certified_differences`` input —
NOT a tolerance, a PROOF-backed allowance. §9.7 is exact about what it licenses:

    "the materialized state a differential run maintains may differ from the
    reference state only behind valid certificates; the equality the contract
    demands (9.1) is on the resolved state, not on raw extraction."

The CENTREPIECE changed-inputs skip is exactly this case: a bounded-dominance
certificate (:mod:`okg.substrate.consistency.reuse_scheduler`) proves a producer
step's every permitted output loses to the decisive incumbent, so the
differential run never extracts the claim. Its EFFECTIVE set (and the loser
membership the projection derives — ranked / superseded / distrusted lists) then
lacks a claim the reference contains as a superseded loser. That is a difference
in *raw extraction*, not in the *resolved decision*. Given the generation's VALID
certificates and their bucket reach (a :class:`CertifiedDifferences`),
:func:`diff_resolved_states` renders EQUAL-with-certified-differences — a distinct
recorded facet on the verdict, NEVER a silent plain-equal — iff EVERY divergent
object is attributable to a certificate-covered bucket AND the resolved DECISION
is strictly equal: same canonical winner / outcome / basis per bucket, same
selection winner / generator / basis, byte-identical open findings, and every
surviving (non-covered) claim byte-identical. The ONLY licensed divergences are
the covered claims' presence in the effective set and their membership in the
covered buckets' loser lists (ranked / superseded / distrusted) plus the derived
whole-selection digest that reflects them. Anything else — an uncovered claim, a
changed winner / outcome / basis, a moved finding, a changed surviving claim —
stays MISMATCH exactly as before. Absent the input the differ behaves
BYTE-IDENTICALLY to today (no facet, exact comparison), so every existing witness
passes unchanged. A certified difference is a proof, so the no-tolerance rule is
intact: the certificate — invalidated by any recorded-dependency change — is the
allowance, not an epsilon; a divergence with NO covering valid certificate is a
MISMATCH, witnessed both ways.

This is the repo's only equivalence surface going forward
(req.harness.live-series): it absorbs the identity-diff idiom of the legacy
SQL-vs-IR migration harness (``consistency/equivalence.py``).

The verdict, its artifact, and its certified-differences facet round-trip to
canonical JSON so the live-series task can store and re-read them per generation.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from okg.substrate.consistency.claim_model.views import CLAIM_SUBTYPE
from okg.substrate.derived_artifacts.claim_effective_derivation import (
    CLAIM_EFFECTIVE_TABLE,
    ROW_KIND_EFFECTIVE,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    CLAIM_PROJECTION_TABLE,
    ROW_KIND_RESOLUTION,
    bucket_row_key,
)
from okg.substrate.derived_artifacts.repair_materialization import (
    REPAIR_SELECTIONS_TABLE,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.llm.outcomes import ReplayExclusion

# --------------------------------------------------------------------------- #
# Verdict + family vocabulary (fixed by the ratified adjudication; §9.1).      #
# --------------------------------------------------------------------------- #
VERDICT_EQUAL = "EQUAL"
VERDICT_MISMATCH = "MISMATCH"
VERDICT_EXCLUDED = "EXCLUDED"
_VERDICTS = frozenset({VERDICT_EQUAL, VERDICT_MISMATCH, VERDICT_EXCLUDED})

FAMILY_EFFECTIVE = "effective_claims"
FAMILY_RESOLUTION = "resolution_outcomes"
FAMILY_SELECTION = "selections"
FAMILY_FINDING = "open_findings"
# Deterministic family order for artifact rendering + iteration.
FAMILIES: tuple[str, ...] = (
    FAMILY_EFFECTIVE,
    FAMILY_RESOLUTION,
    FAMILY_SELECTION,
    FAMILY_FINDING,
)


def _canonical_json(obj: Any) -> str:
    """sha-stable canonical JSON: sorted keys, compact separators.

    The single serialization discipline for the whole module — every byte-
    stability and round-trip guarantee rests on it. ``sort_keys`` makes dict
    insertion order irrelevant; lists keep their order (``ranked_claim_ids`` is
    ≻-descending and load-bearing, so it must NOT be re-sorted)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parse_json_list(raw: Any) -> list[Any]:
    """Normalize a JSON-array column to a list, order preserved.

    Chronos kept-table columns (``ranked_claim_ids`` / ``superseded_claim_ids``)
    are TEXT holding a JSON array; JSONB columns come back already parsed. A
    NULL is the empty list. Order is preserved (never sorted — see
    :func:`_canonical_json`)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return list(raw)
    return list(json.loads(raw))


def _canonical_element_ref(raw: Any) -> Any:
    """Canonicalize a claim's ``element_ref`` handle for byte-stable compare.

    Read as ``attrs->>'element_ref'`` (text). If it parses as JSON, keep the
    parsed value so serialization re-canonicalizes its key order; otherwise
    keep the raw text. ``None`` (a corpus-level claim, or an operator-asserted
    row with no source node) stays ``None`` — the honest absence, never a
    fabricated default."""
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


# --------------------------------------------------------------------------- #
# The snapshot                                                                 #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ResolvedStateSnapshot:
    """One deployment-generation's resolved state, canonical + deterministic.

    Each family maps a stable object identity to a field dict. ``deployment`` /
    ``generation_id`` / ``chronos_generation_id`` are provenance metadata: they
    identify WHICH execution this is, and are part of :meth:`canonical_json` (so
    two extractions of the same generation are byte-equal) but are DELIBERATELY
    excluded from :meth:`state_canonical_json` and from
    :func:`diff_resolved_states` — the same-result contract is on the resolved
    state, not on the generation label two equal executions never share."""

    deployment: str
    # Compat integer generation id (as text): the recording-table + finding
    # generation column. Distinct from the Chronos checkpoint.
    generation_id: str
    # Chronos checkpoint id: pins the interval-visible kept-table read.
    chronos_generation_id: str
    effective_claims: Mapping[str, Mapping[str, Any]]
    resolution_outcomes: Mapping[str, Mapping[str, Any]]
    selections: Mapping[str, Mapping[str, Any]]
    selection_digest: str | None
    open_findings: Mapping[str, Mapping[str, Any]]

    # -- family access ----------------------------------------------------- #
    def family(self, name: str) -> Mapping[str, Mapping[str, Any]]:
        if name == FAMILY_EFFECTIVE:
            return self.effective_claims
        if name == FAMILY_RESOLUTION:
            return self.resolution_outcomes
        if name == FAMILY_SELECTION:
            return self.selections
        if name == FAMILY_FINDING:
            return self.open_findings
        raise KeyError(f"unknown equality family {name!r}")

    # -- serialization ----------------------------------------------------- #
    def state_json_dict(self) -> dict[str, Any]:
        """The RESOLVED STATE only — the four families + the selection digest.

        No generation/deployment metadata: this is the byte-level identity two
        equal executions share regardless of their generation labels."""
        return {
            FAMILY_EFFECTIVE: {k: dict(v) for k, v in self.effective_claims.items()},
            FAMILY_RESOLUTION: {
                k: dict(v) for k, v in self.resolution_outcomes.items()
            },
            FAMILY_SELECTION: {k: dict(v) for k, v in self.selections.items()},
            "selection_digest": self.selection_digest,
            FAMILY_FINDING: {k: dict(v) for k, v in self.open_findings.items()},
        }

    def to_json_dict(self) -> dict[str, Any]:
        """Full snapshot including generation/deployment provenance."""
        return {
            "deployment": self.deployment,
            "generation_id": self.generation_id,
            "chronos_generation_id": self.chronos_generation_id,
            **self.state_json_dict(),
        }

    def state_canonical_json(self) -> str:
        return _canonical_json(self.state_json_dict())

    def canonical_json(self) -> str:
        return _canonical_json(self.to_json_dict())

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "ResolvedStateSnapshot":
        return cls(
            deployment=str(payload["deployment"]),
            generation_id=str(payload["generation_id"]),
            chronos_generation_id=str(payload["chronos_generation_id"]),
            effective_claims=dict(payload.get(FAMILY_EFFECTIVE) or {}),
            resolution_outcomes=dict(payload.get(FAMILY_RESOLUTION) or {}),
            selections=dict(payload.get(FAMILY_SELECTION) or {}),
            selection_digest=payload.get("selection_digest"),
            open_findings=dict(payload.get(FAMILY_FINDING) or {}),
        )

    @classmethod
    def from_canonical_json(cls, text: str) -> "ResolvedStateSnapshot":
        return cls.from_json_dict(json.loads(text))

    # -- extraction -------------------------------------------------------- #
    @classmethod
    def extract(
        cls,
        conn: Any,
        *,
        deployment: str,
        generation_id: int | str,
        chronos_generation_id: int | str,
    ) -> "ResolvedStateSnapshot":
        """Extract one deployment-generation's resolved state, canonically.

        ``generation_id`` is the compat integer generation id (the recording
        tables' ``generation_id`` column and ``observation_current``'s
        ``latest_generation_id``); ``chronos_generation_id`` is the Chronos
        checkpoint id that pins the interval-visible kept-table read. Both come
        straight from the publish output (``out["compat_generation_id"]`` and
        ``out["generation_id"]`` respectively) — no defaulting, so the pin and
        the recording-table filter are never silently conflated.

        The two interval kept tables are read through a pinned checkout;
        ``repair_selections`` and ``observation_current`` filter by their
        generation column on the raw connection.
        """
        if not deployment:
            raise SubstrateError(
                "equivalence_differ: a deployment label is required to extract a "
                "resolved-state snapshot"
            )
        # Local import: the diff/verdict half of this module is pure and must
        # not drag the graph store into pure-logic importers.
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(chronos_generation_id)

        effective = cls._extract_effective(checkout)
        resolution = cls._extract_resolution(checkout)
        selections, digest = cls._extract_selections(conn, deployment, generation_id)
        findings = cls._extract_open_findings(conn, deployment, generation_id)

        return cls(
            deployment=str(deployment),
            generation_id=str(generation_id),
            chronos_generation_id=str(chronos_generation_id),
            effective_claims=effective,
            resolution_outcomes=resolution,
            selections=selections,
            selection_digest=digest,
            open_findings=findings,
        )

    # -- per-family readers (each returns identity -> field dict) ----------- #
    @staticmethod
    def _extract_effective(checkout: Any) -> dict[str, dict[str, Any]]:
        rows = checkout.query(
            f"SELECT row_key, predicate, normalized_key, value, scope, "
            f"scope_state, claim_semantics, source_id, provenance, "
            f"applied_correction_id, "
            f"stale_correction_review "
            f"FROM {CLAIM_EFFECTIVE_TABLE} WHERE row_kind = :k",
            {"k": ROW_KIND_EFFECTIVE},
        )
        # element_ref is not (yet) persisted on the kept table; read the handle
        # from the source claim node through the SAME pinned checkout so the
        # snapshot carries the field the contract lists. Asserted claims have no
        # source node -> absent -> None (honest, never defaulted).
        elem_rows = checkout.query(
            "SELECT node_id, attrs->>'element_ref' AS element_ref "
            "FROM okg.graph_nodes WHERE subtype = :s",
            {"s": CLAIM_SUBTYPE},
        )
        elem_by_id = {
            str(r["node_id"]): _canonical_element_ref(r["element_ref"])
            for r in elem_rows
        }
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            claim_id = str(r["row_key"])
            if claim_id in out:
                raise SubstrateError(
                    "equivalence_differ: duplicate effective_claim row_key in the "
                    "pinned kept table",
                    data={"claim_id": claim_id},
                )
            out[claim_id] = {
                "predicate": r["predicate"],
                "normalized_key": r["normalized_key"],
                "value": r["value"],
                "scope": r["scope"],
                "scope_state": r["scope_state"],
                "claim_semantics": r["claim_semantics"],
                "source_id": r["source_id"],
                "provenance": r["provenance"],
                "applied_correction_id": r["applied_correction_id"],
                "stale_correction_review": r["stale_correction_review"],
                "element_ref": elem_by_id.get(claim_id),
            }
        return out

    @staticmethod
    def _extract_resolution(checkout: Any) -> dict[str, dict[str, Any]]:
        rows = checkout.query(
            f"SELECT row_key, predicate, bucket_key, outcome, winner_claim_id, "
            f"basis, ranked_claim_ids, superseded_claim_ids "
            f"FROM {CLAIM_PROJECTION_TABLE} WHERE row_kind = :k",
            {"k": ROW_KIND_RESOLUTION},
        )
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            row_key = str(r["row_key"])
            if row_key in out:
                raise SubstrateError(
                    "equivalence_differ: duplicate bucket_resolution row_key in "
                    "the pinned kept table",
                    data={"row_key": row_key},
                )
            out[row_key] = {
                "predicate": r["predicate"],
                "bucket_key": r["bucket_key"],
                "outcome": r["outcome"],
                "winner_claim_id": r["winner_claim_id"],
                "basis": r["basis"],
                "ranked_claim_ids": _parse_json_list(r["ranked_claim_ids"]),
                "superseded_claim_ids": _parse_json_list(r["superseded_claim_ids"]),
            }
        return out

    @staticmethod
    def _extract_selections(
        conn: Any, deployment: str, generation_id: int | str
    ) -> tuple[dict[str, dict[str, Any]], str | None]:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT bucket_key, outcome, winner_claim_id, generator_id, "
                f"generator_version, basis, distrusted_claim_ids, "
                f"asserted_claim_ids, override_claim_ids, unresolved_reason, "
                f"selection_digest "
                f"FROM {REPAIR_SELECTIONS_TABLE} "
                f"WHERE deployment_name = %s AND generation_id = %s",
                (deployment, str(generation_id)),
            )
            rows = cur.fetchall()
        out: dict[str, dict[str, Any]] = {}
        digests: set[str] = set()
        for (
            bucket_key,
            outcome,
            winner,
            generator_id,
            generator_version,
            basis,
            distrusted,
            asserted,
            overrides,
            unresolved_reason,
            digest,
        ) in rows:
            key = str(bucket_key)
            if key in out:
                raise SubstrateError(
                    "equivalence_differ: duplicate repair_selections bucket_key "
                    "for one generation",
                    data={"bucket_key": key},
                )
            out[key] = {
                "outcome": outcome,
                "winner_claim_id": winner,
                "generator_id": generator_id,
                "generator_version": generator_version,
                "basis": basis,
                "distrusted_claim_ids": list(distrusted or []),
                "asserted_claim_ids": list(asserted or []),
                # candidate-set content: the second repair the bucket had
                # available. Compared like any other selection column — a
                # generation that enumerated a different repair set is NOT
                # equivalent to one that did not.
                "override_claim_ids": list(overrides or []),
                "unresolved_reason": unresolved_reason,
            }
            if digest is not None:
                digests.add(str(digest))
        if len(digests) > 1:
            # repair_materialization writes ONE whole-selection digest per
            # generation on every row; more than one is a real defect, surfaced
            # loudly rather than papered over with a pick-one fallback.
            raise SubstrateError(
                "equivalence_differ: repair_selections carries multiple "
                "whole-selection digests for one generation",
                data={"deployment": deployment,
                      "generation_id": str(generation_id),
                      "digests": sorted(digests)},
            )
        return out, (next(iter(digests)) if digests else None)

    @staticmethod
    def _extract_open_findings(
        conn: Any, deployment: str, generation_id: int | str
    ) -> dict[str, dict[str, Any]]:
        with conn.cursor() as cur:
            # As-of semantics (``<=``), mirroring gates.py's active-set
            # resolution (``_open_observation_sample``): the active set at a
            # generation is every finding OPEN with ``latest_generation_id`` at
            # or below it — NOT stamped exactly at it. Bounded event growth
            # suppresses the re-``observed`` event for a still-open, unchanged
            # finding (observations.py), so a finding carried forward unchanged
            # from an earlier generation keeps its earlier
            # ``latest_generation_id``. An ``= %s`` filter would silently drop
            # such a carried finding, making a steady-state series compare
            # dishonestly (a resolved state missing a finding that is genuinely
            # open). ``observation_current`` holds the CURRENT status, so the
            # ``finding_status = 'open'`` filter scopes the read to findings
            # still open as of this snapshot.
            cur.execute(
                """
                SELECT oc.input_scope, oc.normalized_predicate,
                       oc.normalized_values, oc.severity
                  FROM okg.observation_current oc
                  JOIN okg.consistency_check_definitions d
                    ON d.check_id = oc.check_id
                 WHERE d.deployment_name = %s
                   AND oc.finding_status = 'open'
                   AND oc.latest_generation_id <= %s
                """,
                (deployment, int(generation_id)),
            )
            rows = cur.fetchall()
        out: dict[str, dict[str, Any]] = {}
        for input_scope, normalized_predicate, normalized_values, severity in rows:
            nv = dict(normalized_values or {})
            # Per-assignment finding identity (PACT consistency-finding-identity):
            # (input_scope, normalized_predicate, canonical normalized_values).
            # Severity is a LABEL, not identity — a relabel never re-keys a
            # finding (test_finding_identity), so it rides as a field.
            identity = _canonical_json(
                [str(input_scope), str(normalized_predicate), nv]
            )
            if identity in out:
                raise SubstrateError(
                    "equivalence_differ: two open findings share a per-assignment "
                    "identity (input_scope, normalized_predicate, "
                    "normalized_values) — a duplicate the observation store "
                    "should never hold",
                    data={"identity": identity},
                )
            out[identity] = {
                "input_scope": input_scope,
                "normalized_predicate": normalized_predicate,
                "normalized_values": nv,
                "severity": severity,
            }
        return out


# --------------------------------------------------------------------------- #
# The diff artifact                                                            #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FamilyDiff:
    """Per-family divergence. ``added`` / ``removed`` name object identities;
    ``changed`` maps a shared identity to ``{field: {reference, candidate}}``;
    ``scalar_changed`` carries family-level scalars (the selection digest).

    Convention: ``reference`` is the ground truth (the complete-execution
    reference); ``candidate`` is the execution under test. ``removed`` is an
    object the candidate is MISSING; ``added`` is an object the candidate has
    that the reference does not."""

    family: str
    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    changed: Mapping[str, Mapping[str, Mapping[str, Any]]] = field(
        default_factory=dict
    )
    scalar_changed: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return not (
            self.added or self.removed or self.changed or self.scalar_changed
        )

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "added": list(self.added),
            "removed": list(self.removed),
            "changed": {k: dict(v) for k, v in self.changed.items()},
            "scalar_changed": {k: dict(v) for k, v in self.scalar_changed.items()},
        }

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "FamilyDiff":
        return cls(
            family=str(payload["family"]),
            added=tuple(payload.get("added") or ()),
            removed=tuple(payload.get("removed") or ()),
            changed=dict(payload.get("changed") or {}),
            scalar_changed=dict(payload.get("scalar_changed") or {}),
        )


@dataclass(frozen=True)
class EquivalenceArtifact:
    """The MISMATCH artifact: every family with a divergence, named."""

    families: Mapping[str, FamilyDiff]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "families": {
                name: fd.to_json_dict() for name, fd in self.families.items()
            }
        }

    def canonical_json(self) -> str:
        return _canonical_json(self.to_json_dict())

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "EquivalenceArtifact":
        return cls(
            families={
                name: FamilyDiff.from_json_dict(fd)
                for name, fd in (payload.get("families") or {}).items()
            }
        )


@dataclass(frozen=True)
class EquivalenceVerdict:
    """Exactly one of EQUAL / MISMATCH / EXCLUDED, with its evidence.

    ``counted_as_agreement`` is STRUCTURAL, not prose: it is ``True`` iff the
    verdict is EQUAL. An EXCLUDED update carries ``False`` and its named missing
    keys — it is never counted as agreement (§9.7). A MISMATCH carries the
    naming artifact.

    ``certified_differences`` is the §9.7 facet: NON-EMPTY iff the verdict is
    EQUAL-with-certified-differences — a plain resolved-state divergence that a
    valid reuse certificate LICENSES (the covered claims' absence from the
    differential's raw extraction). It records the accepted divergences PER OBJECT
    and the covering certificate digests, so a certified-equal is NEVER silently
    indistinguishable from a plain-equal (:meth:`is_certified_equal`). A
    certified-equal is still EQUAL and still counts as agreement — the resolved
    DECISION is exactly equal; the certificate is the proof that the raw-extraction
    difference does not touch it. The constructor enforces every invariant, so a
    malformed verdict cannot be built."""

    verdict: str
    counted_as_agreement: bool
    artifact: EquivalenceArtifact | None = None
    excluded_keys: tuple[Mapping[str, Any], ...] = ()
    certified_differences: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.verdict not in _VERDICTS:
            raise ValueError(
                f"verdict must be one of {sorted(_VERDICTS)}, got {self.verdict!r}"
            )
        if self.counted_as_agreement != (self.verdict == VERDICT_EQUAL):
            raise ValueError(
                "counted_as_agreement must be True iff the verdict is EQUAL "
                "(an EXCLUDED or MISMATCH update is never agreement)"
            )
        if self.verdict == VERDICT_MISMATCH and self.artifact is None:
            raise ValueError("a MISMATCH verdict requires a naming artifact")
        if self.verdict != VERDICT_MISMATCH and self.artifact is not None:
            raise ValueError("only a MISMATCH verdict carries an artifact")
        if self.verdict == VERDICT_EXCLUDED and not self.excluded_keys:
            raise ValueError(
                "an EXCLUDED verdict must name every missing key (§9.7)"
            )
        if self.verdict != VERDICT_EXCLUDED and self.excluded_keys:
            raise ValueError("only an EXCLUDED verdict carries excluded_keys")
        if self.certified_differences and self.verdict != VERDICT_EQUAL:
            raise ValueError(
                "certified_differences is the EQUAL-with-certified-differences "
                "facet (§9.7); a MISMATCH or EXCLUDED verdict carries none"
            )

    @property
    def is_certified_equal(self) -> bool:
        """EQUAL behind a valid certificate — distinct from a plain EQUAL (§9.7).

        NEVER silently plain-equal: a certified-equal is exactly the EQUAL verdict
        whose ``certified_differences`` facet is non-empty."""
        return self.verdict == VERDICT_EQUAL and bool(self.certified_differences)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "counted_as_agreement": self.counted_as_agreement,
            "artifact": self.artifact.to_json_dict() if self.artifact else None,
            "excluded_keys": [dict(k) for k in self.excluded_keys],
            "certified_differences": [dict(d) for d in self.certified_differences],
        }

    def canonical_json(self) -> str:
        return _canonical_json(self.to_json_dict())

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, Any]) -> "EquivalenceVerdict":
        artifact_payload = payload.get("artifact")
        return cls(
            verdict=str(payload["verdict"]),
            counted_as_agreement=bool(payload["counted_as_agreement"]),
            artifact=(
                EquivalenceArtifact.from_json_dict(artifact_payload)
                if artifact_payload is not None
                else None
            ),
            excluded_keys=tuple(
                dict(k) for k in (payload.get("excluded_keys") or ())
            ),
            certified_differences=tuple(
                dict(d) for d in (payload.get("certified_differences") or ())
            ),
        )

    @classmethod
    def from_canonical_json(cls, text: str) -> "EquivalenceVerdict":
        return cls.from_json_dict(json.loads(text))


# --------------------------------------------------------------------------- #
# The diff                                                                     #
# --------------------------------------------------------------------------- #
def _diff_family(
    family: str,
    reference: Mapping[str, Mapping[str, Any]],
    candidate: Mapping[str, Mapping[str, Any]],
    *,
    scalar_changed: Mapping[str, Mapping[str, Any]] | None = None,
) -> FamilyDiff:
    ref_keys = set(reference)
    cand_keys = set(candidate)
    added = tuple(sorted(cand_keys - ref_keys))
    removed = tuple(sorted(ref_keys - cand_keys))
    changed: dict[str, dict[str, dict[str, Any]]] = {}
    for key in sorted(ref_keys & cand_keys):
        ref_fields = reference[key]
        cand_fields = candidate[key]
        field_diffs: dict[str, dict[str, Any]] = {}
        for fname in sorted(set(ref_fields) | set(cand_fields)):
            rv = ref_fields.get(fname)
            cv = cand_fields.get(fname)
            if _canonical_json(rv) != _canonical_json(cv):
                field_diffs[fname] = {"reference": rv, "candidate": cv}
        if field_diffs:
            changed[key] = field_diffs
    return FamilyDiff(
        family=family,
        added=added,
        removed=removed,
        changed=changed,
        scalar_changed=dict(scalar_changed or {}),
    )


def _name_exclusions(
    exclusions: Sequence[ReplayExclusion | str],
) -> tuple[dict[str, Any], ...]:
    """Name every missing replay key (§9.7). Accepts :class:`ReplayExclusion`
    records or bare key digests; renders a sorted, deterministic list."""
    named: list[dict[str, Any]] = []
    for item in exclusions:
        if isinstance(item, ReplayExclusion):
            named.append({
                "key_digest": item.key_digest,
                "step_id": item.step_id,
                "fragment_id": item.fragment_id,
            })
        else:
            named.append({
                "key_digest": str(item),
                "step_id": None,
                "fragment_id": None,
            })
    named.sort(
        key=lambda d: (
            str(d["key_digest"]),
            str(d["step_id"] or ""),
            str(d["fragment_id"] or ""),
        )
    )
    return tuple(named)


# --------------------------------------------------------------------------- #
# The §9.7 certified-differences licence.                                      #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CertifiedDifferences:
    """The generation's VALID reuse certificates and the bucket reach they cover
    — the ONLY input that lets :func:`diff_resolved_states` license a
    materialized/reference divergence without a MISMATCH (§9.7).

    ``covered_buckets`` are the bucket ROW-KEYS (``bucket_row_key(predicate,
    normalized_key)``) the valid certificates' effects reach — the buckets in
    which a certificate-backed skip could have left the differential run's raw
    extraction short of a superseded loser. ``certificate_digests`` are the
    covering certificates' digests, recorded on the verdict facet for
    provenance. This is a PROOF the caller assembles from the generation's
    certificate store (only VALID certificates — an invalidated certificate
    licenses nothing), never a tolerance the differ invents."""

    covered_buckets: frozenset[str]
    certificate_digests: tuple[str, ...] = ()

    def covers_bucket(self, bucket_key: str) -> bool:
        return bucket_key in self.covered_buckets


# The resolution / selection fields whose LIST membership a certified difference
# may touch (the loser lists — "raw extraction", §9.7). Every OTHER field (the
# decision: winner / outcome / basis / generator) must be byte-identical.
_RESOLUTION_MEMBERSHIP_FIELDS: frozenset[str] = frozenset({
    "ranked_claim_ids", "superseded_claim_ids",
})
_SELECTION_MEMBERSHIP_FIELDS: frozenset[str] = frozenset({
    "distrusted_claim_ids",
    # The override candidate's targets are loser membership, not a decision:
    # which generator was SELECTED stays in the strictly-compared decision
    # fields. Do NOT justify this entry by equating the list with
    # distrusted_claim_ids — the two are NOT the same set. The override
    # generator refuses on a contested bucket, a value-less bucket, and a
    # typed-surrogate bucket, all of which record distrusted = the losers while
    # override_claim_ids stays empty (witness:
    # test_override_refuses_when_the_evidence_names_no_value).
    # The guarantee is `_list_delta_is_covered` alone. Listing a field here
    # licenses NOTHING by itself; the helper independently requires that
    # deleting the certificate-covered claims from both the reference and the
    # candidate list leaves the surviving members EQUAL, IN ORDER. A divergence
    # that is not exactly a set of covered claims still fails, whatever the
    # relationship between this list and the distrusted one. Anyone relaxing
    # that helper reopens the hole this comment does not close.
    "override_claim_ids",
})


def _bucket_of_claim(fields: Mapping[str, Any]) -> str | None:
    """The bucket row-key an effective-claim row belongs to, or ``None``.

    A corpus-level or malformed claim without both a predicate and a normalized
    key has no bucket and can never be certificate-covered (it refuses)."""
    predicate = fields.get("predicate")
    key = fields.get("normalized_key")
    if predicate is None or key is None:
        return None
    return bucket_row_key(str(predicate), str(key))


def _list_delta_is_covered(
    ref_list: Any, cand_list: Any, covered_claims: frozenset[str]
) -> bool:
    """True iff removing the covered claims from BOTH lists leaves them EQUAL.

    Order-preserving: the surviving (non-covered) claims must appear in the same
    order in reference and candidate — the ≻ ranking of the survivors is
    unchanged, and the ONLY difference is covered claims (§9.7: the licensed
    divergence is exactly the never-extracted losers, nothing reordered)."""
    ref_survivors = [c for c in _parse_json_list(ref_list) if c not in covered_claims]
    cand_survivors = [c for c in _parse_json_list(cand_list) if c not in covered_claims]
    return ref_survivors == cand_survivors


def _certify_divergences(
    family_diffs: Mapping[str, FamilyDiff],
    reference: ResolvedStateSnapshot,
    certified: CertifiedDifferences,
) -> list[dict[str, Any]] | None:
    """Attempt to license every divergence with the valid certificates (§9.7).

    Returns the accepted per-object certified-difference facet when EVERY
    divergence is attributable to a certificate-covered bucket AND the resolved
    DECISION is strictly equal; returns ``None`` (→ MISMATCH) on anything the
    certificates do not license. The licensed divergences are exactly:

    * effective_claims: covered claims REMOVED from the candidate (never extracted
      by the skipped step) — no added, no changed surviving claim;
    * resolution_outcomes / selections: covered buckets whose ONLY changed fields
      are the loser-membership lists (ranked / superseded / distrusted), with the
      covered claims removed leaving the surviving members equal — the decision
      fields (outcome / winner / basis / generator) byte-identical, no bucket
      added or removed;
    * the whole-selection ``selection_digest`` scalar, which reflects the covered
      distrusted deltas.

    open_findings MUST be strictly equal (a finding divergence is a decision
    change — never licensed)."""
    # A finding divergence is never a raw-extraction difference — refuse.
    if FAMILY_FINDING in family_diffs:
        return None

    # The covered claims: effective rows REMOVED from the candidate whose bucket a
    # valid certificate covers. Everything else the certificates cannot license.
    accepted: list[dict[str, Any]] = []
    covered_removed: set[str] = set()
    eff = family_diffs.get(FAMILY_EFFECTIVE)
    if eff is not None:
        if eff.added or eff.changed or eff.scalar_changed:
            # A candidate with an EXTRA claim, a changed surviving claim, or a
            # scalar move is not a "never-extracted loser" difference — refuse.
            return None
        for claim_id in eff.removed:
            fields = reference.effective_claims.get(claim_id)
            bucket = _bucket_of_claim(fields) if fields is not None else None
            if bucket is None or not certified.covers_bucket(bucket):
                return None  # an uncovered removed claim — refuse.
            covered_removed.add(claim_id)
            accepted.append({
                "family": FAMILY_EFFECTIVE, "kind": "never_extracted_claim",
                "object": claim_id, "bucket": bucket,
            })
    covered_claims = frozenset(covered_removed)

    # resolution + selection: only covered buckets, only loser-membership deltas,
    # every decision field byte-identical.
    for family, membership_fields in (
        (FAMILY_RESOLUTION, _RESOLUTION_MEMBERSHIP_FIELDS),
        (FAMILY_SELECTION, _SELECTION_MEMBERSHIP_FIELDS),
    ):
        fd = family_diffs.get(family)
        if fd is None:
            continue
        if fd.added or fd.removed:
            # A bucket appearing / disappearing is a decision change — refuse.
            return None
        for bucket_key, field_diffs in fd.changed.items():
            if not certified.covers_bucket(bucket_key):
                return None
            changed_fields = set(field_diffs)
            if not changed_fields <= membership_fields:
                # A decision field (winner / outcome / basis / generator) moved.
                return None
            for fname in changed_fields:
                pair = field_diffs[fname]
                if not _list_delta_is_covered(
                    pair["reference"], pair["candidate"], covered_claims
                ):
                    return None
                accepted.append({
                    "family": family, "kind": "loser_membership",
                    "object": bucket_key, "field": fname,
                })
        # The whole-selection digest scalar reflects the covered distrusted
        # deltas: licensed ONLY when at least one covered bucket actually changed
        # (a digest move with no covered bucket change is unexplained — refuse).
        if fd.scalar_changed:
            if set(fd.scalar_changed) != {"selection_digest"} or not fd.changed:
                return None
            accepted.append({
                "family": family, "kind": "derived_selection_digest",
                "object": "selection_digest",
            })

    # Something diverged but nothing was licensed (e.g. only a finding diff, caught
    # above) — with no accepted object there is nothing to certify.
    if not accepted:
        return None
    return accepted


def diff_resolved_states(
    reference: ResolvedStateSnapshot,
    candidate: ResolvedStateSnapshot,
    *,
    exclusions: Sequence[ReplayExclusion | str] = (),
    certified_differences: CertifiedDifferences | None = None,
) -> EquivalenceVerdict:
    """Render the exact per-update verdict: EQUAL, MISMATCH, or EXCLUDED.

    ``exclusions`` (non-empty) DOMINATES: an update whose replay could not
    resolve every key is EXCLUDED with the missing keys named, and never
    counted as agreement — even against an otherwise byte-identical pair
    (§9.7). Otherwise the four resolved-state families are compared EXACTLY:
    no divergence is EQUAL; any divergence is a MISMATCH whose artifact names
    every divergent object.

    ``certified_differences`` (optional) is the §9.7 licence, NOT a tolerance:
    the generation's VALID reuse certificates and their covered buckets. When
    present AND every divergence is attributable to a certificate-covered bucket
    AND the resolved DECISION is strictly equal (see :func:`_certify_divergences`),
    the verdict is EQUAL-with-certified-differences — verdict EQUAL, still
    agreement, with a distinct ``certified_differences`` facet recording the
    accepted divergences per object (NEVER a silent plain-equal). A divergence the
    certificates do not license stays MISMATCH exactly as before. Absent the input
    the differ is BYTE-IDENTICAL to today — the parameter only ever RESCUES a
    would-be MISMATCH, never turns an EQUAL into anything else, and never loosens
    the exact comparison (a certificate is a proof; §9.1's no-tolerance rule is
    intact). Equality is on the resolved STATE only; the generation/deployment
    labels two equal executions never share are ignored."""
    if exclusions:
        return EquivalenceVerdict(
            verdict=VERDICT_EXCLUDED,
            counted_as_agreement=False,
            excluded_keys=_name_exclusions(exclusions),
        )

    family_diffs: dict[str, FamilyDiff] = {}
    for name in FAMILIES:
        scalar_changed: dict[str, dict[str, Any]] = {}
        if name == FAMILY_SELECTION and (
            _canonical_json(reference.selection_digest)
            != _canonical_json(candidate.selection_digest)
        ):
            scalar_changed["selection_digest"] = {
                "reference": reference.selection_digest,
                "candidate": candidate.selection_digest,
            }
        fd = _diff_family(
            name,
            reference.family(name),
            candidate.family(name),
            scalar_changed=scalar_changed,
        )
        if not fd.is_empty:
            family_diffs[name] = fd

    if not family_diffs:
        return EquivalenceVerdict(
            verdict=VERDICT_EQUAL, counted_as_agreement=True
        )

    # §9.7: a divergence a VALID certificate licenses is EQUAL-with-certified-
    # differences, not a MISMATCH. Only ever consulted when there IS a divergence,
    # so absent the input the behaviour is byte-identical to before.
    if certified_differences is not None:
        accepted = _certify_divergences(
            family_diffs, reference, certified_differences
        )
        if accepted is not None:
            recorded = list(accepted)
            for digest in certified_differences.certificate_digests:
                recorded.append({"kind": "covering_certificate", "digest": digest})
            return EquivalenceVerdict(
                verdict=VERDICT_EQUAL,
                counted_as_agreement=True,
                certified_differences=tuple(recorded),
            )

    return EquivalenceVerdict(
        verdict=VERDICT_MISMATCH,
        counted_as_agreement=False,
        artifact=EquivalenceArtifact(families=family_diffs),
    )


__all__ = [
    "VERDICT_EQUAL",
    "VERDICT_MISMATCH",
    "VERDICT_EXCLUDED",
    "FAMILY_EFFECTIVE",
    "FAMILY_RESOLUTION",
    "FAMILY_SELECTION",
    "FAMILY_FINDING",
    "FAMILIES",
    "ResolvedStateSnapshot",
    "FamilyDiff",
    "EquivalenceArtifact",
    "EquivalenceVerdict",
    "CertifiedDifferences",
    "diff_resolved_states",
]
