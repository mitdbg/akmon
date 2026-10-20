"""Operator corrections over the extracted claims — the C_eff tier.

``C_eff = derived ⊕ corrections``. The benchmark models the OPERATOR, not just
the corpus: a correction is minted at the ingest seam as a ``claim_correction``
node (the engine's real shape), exactly as the coverage policies, the arm
stamps, and the operator precedents are. Three shapes, deterministically
planned (PRF over the sealed gold), each with its expected effect recorded:

* ``retract`` — drops a CORRUPTED claim from C_eff. The corruption's conflict
  leaves the engine's view; the funnel attributes it to ``removed_from_scope``,
  an operator action, never a detection miss.
* ``override`` — rewrites a CORRUPTED claim's value back to its clean surface.
  The conflict is gone with the claim still present — also ``removed_from_scope``
  (an override that removes the conflict is the same operator action).
* ``assert`` — authors a claim absent from extraction. A pure C_eff addition,
  observed as a new effective-claim row (provenance ``operator_asserted``).

The synth CORE stays substrate-free (this module imports no substrate); the
runner builds the ``NodeFact``\\ s from these specs at the ingest seam.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from okg.bench.synth.prf import SynthPrf

CORRECTION_SUBTYPE = "claim_correction"

ACTION_RETRACT = "retract"
ACTION_OVERRIDE = "override"
ACTION_ASSERT = "assert"


class CorrectionError(ValueError):
    """Typed rejection: the requested corrections are inexpressible."""


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


@dataclass(frozen=True)
class CorrectionSpec:
    """One planned operator correction + its expected effect on C_eff."""

    correction_id: str
    action: str
    target_claim_id: str
    sequence: int
    value: str | None = None
    predicate: str | None = None
    normalized_key: str | None = None
    source_id: str | None = None
    # The corruption whose conflict this correction removes (retract/override
    # only); None for assert. Drives the funnel's removed_from_scope attribution.
    targets_corruption: str | None = None
    # The pre-correction (gen A) value of the target claim, for the delta witness.
    before_value: str | None = None


@dataclass(frozen=True)
class CorrectionsPlan:
    specs: tuple[CorrectionSpec, ...]

    @property
    def removed_corruption_ids(self) -> frozenset[str]:
        """Corruptions whose conflict a retract/override removed — the funnel's
        removed_from_scope set."""
        return frozenset(
            spec.targets_corruption
            for spec in self.specs
            if spec.action in (ACTION_RETRACT, ACTION_OVERRIDE)
            and spec.targets_corruption is not None
        )

    def count_by_action(self) -> dict[str, int]:
        return {
            action: sum(1 for spec in self.specs if spec.action == action)
            for action in (ACTION_RETRACT, ACTION_OVERRIDE, ACTION_ASSERT)
        }


def corruption_claim_id(
    site: Mapping[str, Any], manifest: Mapping[str, Any]
) -> str:
    """The value-free claim id the corrupted rendering of ``site`` extracts to
    (``claim:{doc}:{predicate}:{entity}:0``) — the id a correction targets."""
    pred_by_fact = {
        row["fact_id"]: _slug(row["predicate_label"]) for row in manifest["trace"]
    }
    entity = manifest["patients"][site["fact_id"].split(":")[1]]["entity"]
    predicate = pred_by_fact[site["fact_id"]]
    return f"claim:{site['doc']}:{predicate}:{entity}:0"


def plan_corrections(
    manifest: Mapping[str, Any],
    prf: SynthPrf,
    avoid_claims: frozenset[str] = frozenset(),
    present_claims: frozenset[str] | None = None,
) -> CorrectionsPlan:
    """Deterministically choose the retract / override / assert targets.

    Retract and override each target a distinct CORRUPTED claim (so the funnel
    can attribute both halves of removed_from_scope); the override rewrites the
    value back to the corruption's clean surface (``pre_text``), removing the
    conflict without removing the claim. Assert authors a fresh operator claim.
    Fail-closed: fewer than two corruptions cannot seat both a retract and an
    override, so the plan raises rather than silently under-delivering.

    ``avoid_claims`` are claim ids a correction must NOT target — the composition
    seam (task.complete.capstone): a claim the precedence arm stamped is skipped
    so the corrections' C_eff delta stays a clean read of the correction alone,
    never entangled with an arm stamp on the same claim. Default empty leaves the
    single-axis corrections run byte-identical.

    ``present_claims``, when given, are the claim ids that ACTUALLY SURVIVED
    extraction (the second seam): on a lossy LLM lane a corruption's occurrence-0
    claim may never have landed, so retract/override it and the C_eff delta reads
    nothing. Restricting targets to survivors keeps requirement (d) — a corruption
    whose C_eff membership changed — observable rather than a coin flip. ``None``
    (the deterministic lane is lossless) imposes no filter.
    """
    # Dedupe by corruption_id first: a majority-visibility corruption emits one
    # gold SITE ROW per corrupted rendering, so the raw list repeats a
    # corruption_id, and two rows of the SAME corruption would be a retract and an
    # override of one claim (the distinctness assert below would then fire). One
    # row per corruption keeps retract and override on genuinely distinct claims.
    seen_ids: set[str] = set()
    corruptions = []
    for site in sorted(
        manifest["gold"]["corruptions"], key=lambda s: s["corruption_id"]
    ):
        if site["corruption_id"] in seen_ids:
            continue
        seen_ids.add(site["corruption_id"])
        corruptions.append(site)
    eligible = [
        s
        for s in corruptions
        if corruption_claim_id(s, manifest) not in avoid_claims
        and (present_claims is None or corruption_claim_id(s, manifest) in present_claims)
    ]
    if len(eligible) < 2:
        raise CorrectionError(
            f"corrections need at least two corruptions to seat a distinct "
            f"retract and override target; the corpus has {len(corruptions)} "
            f"({len(eligible)} after excluding {len(avoid_claims)} arm-stamped "
            f"claim(s)"
            + (
                " and keeping only extraction survivors"
                if present_claims is not None
                else ""
            )
            + ")"
        )
    ordered = prf.shuffled(eligible, "corrections", "targets")
    retract_site, override_site = ordered[0], ordered[1]

    retract_claim = corruption_claim_id(retract_site, manifest)
    override_claim = corruption_claim_id(override_site, manifest)
    assert override_claim != retract_claim  # distinct corruptions -> distinct claims

    entity = manifest["patients"][override_site["fact_id"].split(":")[1]]["entity"]
    assert_claim = f"claim:operator-assert:operator_flag:{entity}:0"

    specs = (
        CorrectionSpec(
            correction_id=f"correction:retract:{retract_site['corruption_id']}",
            action=ACTION_RETRACT,
            target_claim_id=retract_claim,
            sequence=1,
            targets_corruption=retract_site["corruption_id"],
            before_value=retract_site["post_text"],
        ),
        CorrectionSpec(
            correction_id=f"correction:override:{override_site['corruption_id']}",
            action=ACTION_OVERRIDE,
            target_claim_id=override_claim,
            sequence=2,
            # rewrite the corrupted value back to the clean surface: the conflict
            # is removed while the claim stays in C_eff.
            value=override_site["pre_text"],
            targets_corruption=override_site["corruption_id"],
            before_value=override_site["post_text"],
        ),
        CorrectionSpec(
            correction_id=f"correction:assert:{entity}",
            action=ACTION_ASSERT,
            target_claim_id=assert_claim,
            sequence=3,
            value="operator asserted status",
            predicate="operator_flag",
            normalized_key=entity,
            source_id="operator",
        ),
    )
    return CorrectionsPlan(specs=specs)


def corrections_gold(plan: CorrectionsPlan) -> list[dict[str, Any]]:
    """The sealed gold rows: each correction and its expected C_eff effect."""
    rows: list[dict[str, Any]] = []
    for spec in plan.specs:
        row: dict[str, Any] = {
            "correction_id": spec.correction_id,
            "action": spec.action,
            "target_claim_id": spec.target_claim_id,
            "sequence": spec.sequence,
            "targets_corruption": spec.targets_corruption,
        }
        if spec.action == ACTION_RETRACT:
            row["expected"] = "target absent from C_eff at the corrected generation"
        elif spec.action == ACTION_OVERRIDE:
            row["expected_value"] = spec.value
            row["before_value"] = spec.before_value
            row["expected"] = "target value rewritten to the clean surface"
        else:
            row["expected_value"] = spec.value
            row["predicate"] = spec.predicate
            row["normalized_key"] = spec.normalized_key
            row["expected"] = "operator-asserted claim present in C_eff"
        rows.append(row)
    return rows
