"""Sealed corpus manifest: digests, trace, gold, canaries, comparability.

The manifest (``okg.synth.manifest.v1``) is the benchmark artifact: given
the manifest plus the corpus directories, a scorer needs nothing else.
Comparability is enforced through ``config_digest``: scorecards produced
under different digests refuse comparison unless explicitly overridden
with a recorded reason (:func:`assert_comparable`).

Leakage keys: every synthetic patient name token and record identifier is
a forbidden token outside sanctioned corpus bytes; the runner merges these
into the #754 leakage scanner's token set. Canary tokens are planted in
the manifest only (never rendered), so any canary appearing in pipeline
output is a leak by construction.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

from okg.bench.synth.arms import build_arm_plan
from okg.bench.synth.config import MANDATORY_COST, SynthConfig
from okg.bench.synth.corruption import CorruptionPlan
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.provenance import build_provenance_plan
from okg.bench.synth.render import RenderResult, pack_bytes
from okg.bench.synth.worldschema import GroundTruthWorld

MANIFEST_SCHEMA = "okg.synth.manifest.v1"


class SealError(ValueError):
    """Typed manifest/comparability failure."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _entity_slug(full_name: str) -> str:
    """The H1 entity slug the deterministic claim grammar derives — kept
    byte-compatible with claim_corpus_source._slug (same regex, no import
    per the purity boundary)."""
    import re

    return re.sub(r"[^a-z0-9]+", "_", f"patient {full_name}".lower()).strip("_")


def corpus_digests(docs: Mapping[str, str]) -> dict[str, str]:
    return {doc: _sha256(text.encode("utf-8")) for doc, text in sorted(docs.items())}


def leakage_tokens(world: GroundTruthWorld) -> tuple[str, ...]:
    """Forbidden-outside-sanctioned-surface tokens — HIGH-ENTROPY only.

    Record identifiers (UUIDs) and digit-suffixed Synthea name tokens
    ('Abram53') qualify; bare dictionary names never do (low entropy,
    collision-prone — review finding 11). A name token without a digit is
    excluded rather than trusted.
    """
    tokens: set[str] = set()
    for patient in world.patients:
        for name_token in (*patient.given.split(), *patient.family.split()):
            if any(c.isdigit() for c in name_token):
                tokens.add(name_token)
        tokens.add(patient.patient_id)
    return tuple(sorted(tokens))


def clone_rate(trace_rows: Any) -> dict[str, Any]:
    """Fraction of k>1 facts whose surface forms are all identical clones.

    Reported in the manifest (never silently): at benign-variation rate 0,
    template renderings collapse to string-diffable clones (design §8).
    """
    surfaces: dict[str, set[str]] = {}
    for row in trace_rows:
        surfaces.setdefault(row.fact_id, set()).add(row.surface_value)
    counts: dict[str, int] = {}
    for row in trace_rows:
        counts[row.fact_id] = counts.get(row.fact_id, 0) + 1
    multi = [fact_id for fact_id, n in counts.items() if n > 1]
    identical = [fact_id for fact_id in multi if len(surfaces[fact_id]) == 1]
    return {
        "facts_k_gt_1": len(multi),
        "all_identical": len(identical),
        "rate": (len(identical) / len(multi)) if multi else 0.0,
    }


def _arm_excluded_buckets(manifest: Mapping[str, Any]) -> frozenset[str]:
    """Conflicted buckets the precedence arm must not target (task.complete.capstone).

    A bucket is excluded when its corruption is either relational (carries a
    ``constraint_kind`` in ``params`` — the arm's ingest stamp would flip its
    finding channel and silently zero that Σ-kind's recall) or majority-visibility
    (``visibility_corrupted > 1`` — the run's realized-majority mis-repair evidence,
    which must be settled by corroboration alone, not arm-forced). Derived from the
    sealed gold + trace, so a single-axis run (no such corruptions) yields the empty
    set and its arm plan is byte-identical to before.
    """
    trace = manifest["trace"]
    patients = manifest["patients"]
    predicate_by_fact: dict[str, str] = {}
    for row in trace:
        predicate_by_fact.setdefault(row["fact_id"], _slug(row["predicate_label"]))
    excluded: set[str] = set()
    for site in manifest["gold"]["corruptions"]:
        params = dict(site.get("params", {}))
        relational = bool(params.get("constraint_kind")) and (
            params["constraint_kind"] != "functional"
        )
        majority = int(site.get("visibility_corrupted", 1)) > 1
        if not (relational or majority):
            continue
        predicate = predicate_by_fact.get(site["fact_id"])
        entity = patients[site["fact_id"].split(":")[1]]["entity"]
        if predicate is not None:
            excluded.add(f"{predicate}:{entity}")
    return frozenset(excluded)


def _slug(text: str) -> str:
    """The one slug rule (equals claim_corpus_source._slug / arms._slug)."""
    import re

    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def build_manifest(
    *,
    config: SynthConfig,
    world: GroundTruthWorld,
    plan: CorruptionPlan,
    rendered: RenderResult,
    pack_name: str = "clinical_v1",
) -> dict[str, Any]:
    prf = SynthPrf(config.seed)
    canaries = tuple(
        f"okgsynthcanary_{prf.token('canary', str(i), length=16)}" for i in range(3)
    )
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "config": json.loads(config.canonical_json_bytes().decode("utf-8")),
        "config_digest": config.config_digest(),
        "world_digest": _sha256(world.to_json_bytes()),
        "world_provenance": dict(world.provenance),
        "pack_name": pack_name,
        "pack_digest": _sha256(pack_bytes(pack_name)),
        "clean_corpus": corpus_digests(rendered.clean_docs),
        "corrupted_corpus": corpus_digests(rendered.corrupted_docs),
        "trace": [asdict(row) for row in rendered.trace],
        "gold": {
            # Each planted corruption DECLARES its dollar error cost, drawn from
            # the preset's per-class table (synth-cost-weighted-gold). A MANDATORY
            # violation carries the string sentinel MANDATORY_COST rather than a
            # float — JSON has no Infinity literal, and a manifest no conformant
            # parser could read would make manifest_digest unverifiable outside
            # Python. ``mandatory`` and ``error_cost == MANDATORY_COST`` are the
            # same statement, sealed together so neither can be read alone.
            "corruptions": [
                {
                    **asdict(site),
                    "params": dict(site.params),
                    "error_cost": (
                        MANDATORY_COST
                        if config.cost.is_mandatory(site.corruption_class)
                        else config.cost.error_cost_of(site.corruption_class)
                    ),
                    "mandatory": config.cost.is_mandatory(site.corruption_class),
                }
                for site in rendered.sites
            ],
            "twins": [
                {**asdict(twin), "detail": dict(twin.detail)} for twin in rendered.twins
            ],
            "planned": len(plan.rows),
            "partitions": dict(rendered.finalized_partitions),
        },
        "patients": {
            p.patient_id: {"entity": _entity_slug(p.full_name)}
            for p in world.patients
        },
        "effective_k": dict(rendered.effective_k),
        "clone_rate": clone_rate(rendered.trace),
        "canary_tokens": list(canaries),
        "leakage_tokens": list(leakage_tokens(world)),
        # THE SEALED COST TABLES (synth-cost-weighted-gold): the per-class error
        # costs (with the MANDATORY set marked) and the per-action-class prices.
        # Sealed BEFORE manifest_digest, so a run's economics are covered by the
        # digest and a re-score that reached for a different config could not
        # silently re-price the run — every scorer reads the tables from HERE.
        "cost_model": {
            "error_costs": config.cost.sealed_error_costs(),
            "default_error_cost": config.cost.default_error_cost,
            "mandatory_classes": sorted(config.cost.mandatory_classes),
            "action_prices": config.cost.action_prices.as_table(),
        },
    }
    # The precedence arm: the admitted-signal set, per-signal surface, and the
    # sealed PLAN (which extracted claim gets which stamp, and the expected winner
    # per targeted bucket). Recorded so a run's arm is auditable after the fact and
    # every witness checks engine output against SEALED gold, never re-derived
    # ranking logic (synth-precedence-arms). Default arm → empty targets. The plan
    # ALSO seals the POISONING selection (synth-authority-poisoning): the rate r, the
    # eligible conflicted-bucket pool, and the PRF-chosen poisoned buckets whose
    # corrupted claim carries a wrong-authority stamp — the sealed basis for the flip
    # report and the poisoned-bucket denominator exclusion.
    arm = config.precedence_arm
    manifest["precedence_arm"] = {
        "admits": list(arm.admits),
        "surface": arm.resolved_surface(),
        "plan": build_arm_plan(
            admits=arm.admits,
            surface=arm.resolved_surface(),
            trace=manifest["trace"],
            patients=manifest["patients"],
            seed=config.seed,
            exclude_buckets=_arm_excluded_buckets(manifest),
            role_redundancy=arm.role_redundancy,
            poison_rate=arm.poison_rate,
            rescue_rate=arm.rescue_rate,
        ),
    }
    # The provenance-group / majority-flavor plan (synth-provenance-diversity):
    # which majority site carries which flavor, and which corrupted-rendering
    # claim gets which provenance group. Sealed here (before the digest) so a
    # run's flavor labelling is auditable after the fact and every witness checks
    # engine output against SEALED gold. Default (disabled) arm → empty plan, so a
    # non-provenance run's manifest gains only this small always-present block.
    prov = config.provenance
    manifest["provenance"] = build_provenance_plan(
        enabled=prov.enabled,
        flavor_split=prov.flavor_split,
        gold_corruptions=manifest["gold"]["corruptions"],
        trace=manifest["trace"],
        patients=manifest["patients"],
    )
    manifest["manifest_digest"] = _sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    return manifest


def write_manifest(manifest: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(manifest, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise SealError(f"manifest schema {manifest.get('schema')!r} != {MANIFEST_SCHEMA!r}")
    return manifest


def scan_unsanctioned_surface(
    manifest: Mapping[str, Any], surfaces: Mapping[str, str]
) -> list[str]:
    """Fail-closed leakage scan over UNSANCTIONED surfaces.

    Sanctioned: corpus bytes and corpus-derived pipeline surfaces (published
    claim content, findings over it). Unsanctioned: configs, templates,
    code, logs, prompt scaffolding — pass those here. Any leakage token or
    canary appearing in them is a violation.
    """
    forbidden = list(manifest.get("leakage_tokens", ())) + list(
        manifest.get("canary_tokens", ())
    )
    violations = []
    for surface_name, text in sorted(surfaces.items()):
        for token in forbidden:
            if token in text:
                violations.append(f"{surface_name}: contains {token!r}")
    return violations


def assert_comparable(
    a: Mapping[str, Any],
    b: Mapping[str, Any],
    *,
    override_reason: str = "",
) -> None:
    """Refuse cross-digest scorecard/manifest comparison without a reason."""
    da, db = a.get("config_digest"), b.get("config_digest")
    if da != db:
        if not override_reason:
            raise SealError(
                f"refusing to compare artifacts with different config digests "
                f"({str(da)[:12]} vs {str(db)[:12]}); pass an explicit override "
                f"reason to force"
            )
    return None
