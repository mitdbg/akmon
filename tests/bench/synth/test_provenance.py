"""synth-provenance-diversity witnesses.

req.prov.axis — a ``provenance_group`` attr enters at the INGEST seam (never
rendered into a document, so the corpus is byte-identical between flavors),
declared per-claim in sealed gold. The engine reads no such attr today: a stamped
and an unstamped publish of the same corpus yield row-wise identical findings and
resolutions. The per-arm scope condition admits the provenance attr EXPLICITLY as
a declared axis — an undeclared ranking-signal stamp still fails loudly.

req.prov.flavors — majority arms come in two flavors: COPY (the corrupted
renderings share one provenance group) and INDEPENDENT (distinct groups). Gold
labels the flavor per site; the scorecard reports mis_repaired per flavor against
its own preserved-majority denominator. A real deterministic-lane run carries BOTH
flavors with non-zero preserved majorities, both mis-repair today (the both-fail
baseline the future fix must split), and the gradability witness is an
expected-fail naming the independence-weighting engine fix.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from okg.bench.synth.config import (
    ConfigError,
    ProvenanceAxis,
    load_config,
    load_preset,
)
from okg.bench.synth.corruption import Planner
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import build_manifest
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.provenance import (
    FLAVOR_COPY,
    FLAVOR_INDEPENDENT,
    PROVENANCE_GROUP_ATTR,
    SPLIT_MODES,
    build_provenance_plan,
)
from okg.bench.synth.render import render_corpus
from okg.bench.synth.resolution import (
    STATE_DETECTED_ONLY,
    STATE_MIS_REPAIRED,
    STATE_REPAIRED,
    ArmScopeError,
    assert_arm_signal_scope,
    per_flavor_mis_repair,
)
from okg.bench.synth.runner import _subset_world, apply_provenance_stamps

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SYNTH = _REPO_ROOT / "src/okg/bench/synth"
_WORLD = _SYNTH / "world_smoke"
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"

_PRESET = "provenance_flavors"


# --------------------------------------------------------------------------- #
# offline: manifest builder                                                    #
# --------------------------------------------------------------------------- #
def _manifest(update: dict[str, Any] | None = None) -> dict[str, Any]:
    config = load_preset(_PRESET)
    if update:
        config = config.model_copy(update=update)
    prov = json.loads((_WORLD / "provenance.json").read_text())
    world = world_from_synthea_dir(
        _WORLD, reference_date=prov["reference_date"], provenance=prov
    )
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    return build_manifest(config=config, world=world, plan=plan, rendered=rendered)


# --------------------------------------------------------------------------- #
# config: the axis is accepted, its split modes pin to the provenance module   #
# --------------------------------------------------------------------------- #
def test_config_split_modes_pin_to_the_provenance_module() -> None:
    """The config regex and the provenance module's SPLIT_MODES must agree, or a
    mode one accepts and the other cannot dispatch would silently mis-flavor."""
    for mode in SPLIT_MODES:
        assert ProvenanceAxis(enabled=True, flavor_split=mode).flavor_split == mode
    with pytest.raises(ConfigError):
        load_config({
            "seed": "x", "world_path": "world_smoke",
            "provenance": {"enabled": True, "flavor_split": "nonsense"},
        })


def test_the_stamped_attr_is_the_attr_the_engine_reads() -> None:
    """The declared axis only grades the engine if both sides spell it the same.

    The benchmark stamps ``PROVENANCE_GROUP_ATTR`` at ingest; the engine's
    support binding reads ``DECLARED_ORIGIN_ATTR`` off the claim node. A drift
    between them is SILENT in the worst way — every origin would read as
    undeclared, the engine would fall back to document identity, and the whole
    arm would report the pre-fix numbers while looking like it graded the fix.
    """
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        DECLARED_ORIGIN_ATTR,
    )

    assert PROVENANCE_GROUP_ATTR == DECLARED_ORIGIN_ATTR


def test_default_config_disables_provenance() -> None:
    """Every existing preset keeps provenance OFF by default (the ingest is
    byte-for-byte unchanged; only the config_digest moves)."""
    assert load_preset("smoke").provenance.enabled is False
    assert load_preset("capstone_smoke").provenance.enabled is False


# --------------------------------------------------------------------------- #
# req.prov.axis: corpus byte-identity between flavors                          #
# --------------------------------------------------------------------------- #
def test_corpus_byte_identical_across_flavors_and_disabled() -> None:
    """Same world, same seed: COPY, INDEPENDENT, and provenance-DISABLED all render
    the SAME corpus bytes — the group is stamped at ingest, never into a document.
    The config_digest DOES differ (the axis is part of the canonical dump), which
    is what forces the golden regen and proves the runs are not trivially equal."""
    base = load_preset(_PRESET)
    copy = _manifest(
        {"provenance": base.provenance.model_copy(update={"flavor_split": "copy"})}
    )
    indep = _manifest(
        {"provenance": base.provenance.model_copy(update={"flavor_split": "independent"})}
    )
    disabled = _manifest({"provenance": ProvenanceAxis(enabled=False)})

    assert copy["clean_corpus"] == indep["clean_corpus"] == disabled["clean_corpus"]
    assert (
        copy["corrupted_corpus"]
        == indep["corrupted_corpus"]
        == disabled["corrupted_corpus"]
    )
    # the change is real: three distinct config digests, so byte-identity is not
    # the trivial "same run" case
    assert len({
        copy["config_digest"], indep["config_digest"], disabled["config_digest"]
    }) == 3


# --------------------------------------------------------------------------- #
# req.prov.axis: the sealed plan structure (copy shares, independent distinct) #
# --------------------------------------------------------------------------- #
def test_plan_copy_shares_one_group_independent_distinct() -> None:
    """COPY: a site's corrupted renderings share ONE group. INDEPENDENT: each is a
    DISTINCT group. Only corrupted renderings are stamped (the clean counterpart is
    left ungrouped), and both flavors label the SAME set of majority sites."""
    copy = _manifest(
        {"provenance": load_preset(_PRESET).provenance.model_copy(
            update={"flavor_split": "copy"})}
    )["provenance"]
    indep = _manifest(
        {"provenance": load_preset(_PRESET).provenance.model_copy(
            update={"flavor_split": "independent"})}
    )["provenance"]

    assert copy["enabled"] and indep["enabled"]
    # same majority sites, opposite flavor
    assert set(copy["flavor_by_corruption"]) == set(indep["flavor_by_corruption"])
    assert set(copy["flavor_by_corruption"].values()) == {FLAVOR_COPY}
    assert set(indep["flavor_by_corruption"].values()) == {FLAVOR_INDEPENDENT}

    # non-vacuity: there ARE multi-rendering (majority) sites, each with >= 2
    # corrupted claims
    assert copy["flavor_by_corruption"], "the preset must realize majority sites"
    cg = copy["group_by_claim"]
    ig = indep["group_by_claim"]
    assert cg and set(cg) == set(ig)
    # copy collapses claims to fewer groups; independent is 1-group-per-claim
    assert len(set(cg.values())) < len(cg), "copy flavor must share groups"
    assert len(set(ig.values())) == len(ig), "independent flavor must be distinct"


def test_alternate_split_carries_both_flavors() -> None:
    """The shipped preset uses ``alternate`` so ONE run carries BOTH flavors."""
    flavors = _manifest()["provenance"]["flavor_by_corruption"]
    assert set(flavors.values()) == {FLAVOR_COPY, FLAVOR_INDEPENDENT}, (
        f"alternate split must seat both flavors: {flavors}"
    )


def test_stamps_apply_only_when_the_plan_is_enabled() -> None:
    """The SAME fact is stamped under the shipped enabled plan and passes through
    untouched under a disabled one — the non-provenance ingest is byte-for-byte
    unchanged.

    The disabled half on its own cannot fail: it compares a constant against
    itself and survives ``apply_provenance_stamps`` being stubbed to
    ``return list(facts)``. The enabled half is what gives the pair a defect to
    catch, and the disabled half then pins the short-circuit.
    """
    enabled_plan = _manifest()["provenance"]
    assert enabled_plan["group_by_claim"], "the preset must seat stamped claims"
    node_id, group = sorted(enabled_plan["group_by_claim"].items())[0]

    disabled_plan = build_provenance_plan(
        enabled=False, flavor_split="", gold_corruptions=[], trace=[], patients={}
    )
    assert disabled_plan["group_by_claim"] == {}

    @dataclass
    class _Fact:
        node_id: str
        subtype: str
        attrs: dict[str, str]

    facts = [_Fact(node_id, "consistency_claim", {"value": "1"})]

    stamped = apply_provenance_stamps(list(facts), enabled_plan)
    assert stamped[0].attrs == {"value": "1", PROVENANCE_GROUP_ATTR: group}, (
        "an enabled plan must stamp the sealed group onto its claim"
    )

    passthrough = apply_provenance_stamps(list(facts), disabled_plan)
    assert passthrough[0].attrs == {"value": "1"}, "disabled plan must not stamp"


# --------------------------------------------------------------------------- #
# req.prov.axis: the scope condition ADMITS provenance explicitly              #
# --------------------------------------------------------------------------- #
@dataclass
class _Claim:
    node_id: str
    attrs: dict[str, Any] = field(default_factory=dict)


def test_scope_admits_provenance_only_when_declared() -> None:
    """A ``provenance_group`` stamp is admitted ONLY when the run declares the
    provenance axis; otherwise it fails loudly naming the axis — the same
    silent-realism guard, applied to the declared provenance axis."""
    stamped = [_Claim("claim:a", {"value": "1", PROVENANCE_GROUP_ATTR: "pg:x"})]
    # declared → silent
    assert_arm_signal_scope(stamped, admits=[], provenance_admitted=True)
    # undeclared → loud, names the axis attr and the claim
    with pytest.raises(ArmScopeError) as exc:
        assert_arm_signal_scope(stamped, admits=[], provenance_admitted=False)
    assert PROVENANCE_GROUP_ATTR in str(exc.value)
    assert "claim:a" in str(exc.value)


def test_undeclared_ranking_signal_still_fails_with_provenance_admitted() -> None:
    """Admitting the provenance axis must NOT relax the ranking-signal guard: an
    undeclared ranking-signal stamp still fails loudly (that witness keeps firing),
    because the provenance axis is checked separately from ranking signals."""
    leaked = [
        _Claim("claim:a", {"value": "1", PROVENANCE_GROUP_ATTR: "pg:x"}),
        _Claim("claim:b", {"value": "2", "is_superseded": "true"}),
    ]
    with pytest.raises(ArmScopeError, match="explicit_supersession"):
        assert_arm_signal_scope(leaked, admits=[], provenance_admitted=True)


# --------------------------------------------------------------------------- #
# req.prov.flavors: the per-flavor scorer (pure)                               #
# --------------------------------------------------------------------------- #
def test_per_flavor_mis_repair_gates_on_preserved_majority() -> None:
    """mis_repaired is counted per flavor ONLY over PRESERVED majorities (corrupted
    strict-majority AND a surviving clean counterpart), and a preserved-but-repaired
    site does not count. A tie (not preserved) is excluded from the denominator."""
    manifest = {
        "provenance": {
            "flavor_by_corruption": {"c1": "copy", "c2": "copy", "c3": "independent"}
        }
    }
    stages = {
        "rows": [
            # copy, preserved + mis-repaired
            {"corruption_id": "c1", "state": STATE_MIS_REPAIRED,
             "realized_vote": {"corrupted": 2, "clean": 1, "other": 0}},
            # copy, preserved but REPAIRED (settled correctly) → not mis_repaired
            {"corruption_id": "c2", "state": STATE_REPAIRED,
             "realized_vote": {"corrupted": 2, "clean": 1, "other": 0}},
            # independent, a TIE → not a preserved majority → excluded entirely
            {"corruption_id": "c3", "state": STATE_DETECTED_ONLY,
             "realized_vote": {"corrupted": 1, "clean": 1, "other": 0}},
        ]
    }
    out = per_flavor_mis_repair(manifest, stages)
    assert out[FLAVOR_COPY] == {"preserved": 2, "mis_repaired": 1}
    assert out[FLAVOR_INDEPENDENT] == {"preserved": 0, "mis_repaired": 0}


def test_per_flavor_zero_when_no_provenance() -> None:
    """A run with no provenance plan reports both flavors zero (never omitted)."""
    out = per_flavor_mis_repair({}, {"rows": []})
    assert out == {
        FLAVOR_COPY: {"preserved": 0, "mis_repaired": 0},
        FLAVOR_INDEPENDENT: {"preserved": 0, "mis_repaired": 0},
    }


# --------------------------------------------------------------------------- #
# DB-backed witnesses                                                          #
# --------------------------------------------------------------------------- #
psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.runner import (  # noqa: E402
    apply_arm_stamps,
    constraint_policy_facts,
    detection_state,
    extract_facts,
    functional_policies,
    publish_fact_list,
    resolutions_at,
    run_synth_benchmark,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "catalog-scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, compose_catalog(d))
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


def _finding_rows(state: Any) -> list[tuple]:
    return sorted(
        (f.bucket_key, f.channel, tuple(sorted(f.claim_ids))) for f in state.findings
    )


def _resolution_rows(resolutions: Any) -> list[tuple]:
    return sorted(
        (
            r.predicate, r.bucket_key, r.outcome, r.basis,
            r.winner_claim_id, tuple(r.ranked_claim_ids), r.winner_value,
        )
        for r in resolutions
    )


@pytest.mark.postgres
def test_stamped_publish_moves_resolutions_but_not_detection(
    clean_runtime: str, tmp_path: Path
) -> None:
    """req.prov.axis, now that the engine READS the axis (PACT
    consistency-independent-support).

    This was the inertness witness: the engine read no provenance attr, so a
    stamped and an unstamped publish of the same corpus were row-wise identical
    everywhere. The origin fix is exactly the change that retires that, so the
    witness becomes the SHARP statement of what the fix does and does not touch.
    Publish the SAME corrupted corpus twice — once WITHOUT the stamp, once WITH
    it. The only delta between the two pinned generations is the
    provenance_group attr on the majority claims, so:

    * FINDINGS stay row-wise identical — corroboration is a tier-3 ranking
      signal and the declaration must not move tier-2 conflict DETECTION. A
      diff here would mean the fix leaked into which buckets are conflicted.
    * RESOLUTIONS differ — the copy-majority buckets that settled on the
      corrupted value now see one origin against one origin.
    """
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    from okg.bench.synth.runner import _write_docs

    manifest = _manifest()  # provenance ENABLED (alternate flavors)
    assert manifest["provenance"]["group_by_claim"], "the corpus must stamp claims"

    # render the corpus to disk (structured deterministic lane)
    config = load_preset(_PRESET)
    prov = json.loads((_WORLD / "provenance.json").read_text())
    world = world_from_synthea_dir(
        _WORLD, reference_date=prov["reference_date"], provenance=prov
    )
    world = _subset_world(world, config.scale.patients)
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    corrupted_dir = tmp_path / "corpus_corrupted"
    _write_docs(corrupted_dir, rendered.corrupted_docs)

    extra_policies, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)
    prov_plan = manifest["provenance"]

    def _facts(stamped: bool) -> list[Any]:
        facts = extract_facts(corrupted_dir, policies)
        if stamped:
            facts = apply_provenance_stamps(facts, prov_plan)
        facts = apply_arm_stamps(
            facts, None, (), provenance_admitted=stamped
        )
        return list(facts) + list(extra_policies)

    # gen_u: unstamped baseline
    out_u, ids_u = publish_fact_list(dsn, _DEP_DIR, _facts(stamped=False))
    state_u = detection_state(dsn, out_u["generation_id"])
    res_u = resolutions_at(dsn, out_u["generation_id"], state_u.claims)

    # gen_s: the SAME corpus, now stamped (same claim ids → an update, no retraction)
    out_s, _ids = publish_fact_list(dsn, _DEP_DIR, _facts(stamped=True), ids_u)
    state_s = detection_state(dsn, out_s["generation_id"])
    res_s = resolutions_at(dsn, out_s["generation_id"], state_s.claims)

    # non-vacuity: the corpus actually produced a conflict + a resolution
    assert _finding_rows(state_u), "no findings — the parity check would be vacuous"
    assert _resolution_rows(res_u), "no resolutions — the parity check would be vacuous"

    # DETECTION is untouched: the declaration is a ranking input, not a
    # conflict predicate. Row-wise identical findings.
    assert _finding_rows(state_u) == _finding_rows(state_s)

    # RESOLUTION moves: the copy-majority buckets no longer settle on the
    # corrupted value.
    assert _resolution_rows(res_u) != _resolution_rows(res_s), (
        "the declared origin changed no resolution — the engine is not reading "
        "the declaration, or no copy-majority bucket seated in this corpus"
    )
    # and it moves in ONE direction only: buckets that were canonical become
    # contested, never the reverse (a copy-majority collapses to a tie; nothing
    # gains support from a declaration).
    outcome_u = {(r.predicate, r.bucket_key): r.outcome for r in res_u}
    outcome_s = {(r.predicate, r.bucket_key): r.outcome for r in res_s}
    assert set(outcome_u) == set(outcome_s), "the bucket set must not move"
    moved = {b for b in outcome_u if outcome_u[b] != outcome_s[b]}
    assert moved, "no bucket outcome moved"
    assert all(
        outcome_u[b] == "canonical" and outcome_s[b] == "contested" for b in moved
    ), (
        f"declaring a shared origin may only UN-settle a bucket: "
        f"{[(b, outcome_u[b], outcome_s[b]) for b in sorted(moved)]}"
    )

    # and the stamped generation really did carry the attr (guard against a
    # silent no-op that would make the parity trivially true)
    stamped_present = any(
        PROVENANCE_GROUP_ATTR in (row.attrs if hasattr(row, "attrs") else {})
        for row in _facts(stamped=True)
        if getattr(row, "subtype", "") == "consistency_claim"
    )
    assert stamped_present, "the stamped fact set must carry provenance_group"


@pytest.mark.postgres
def test_provenance_flavors_split_on_a_balanced_denominator(
    clean_runtime: str, tmp_path: Path
) -> None:
    """req.prov.flavors: a real deterministic-lane run carries BOTH flavors with
    non-zero preserved majorities, and the origin fix SPLITS them. Read entirely
    from run output (the scorecard's majority_flavor block).

    This recorded the both-fail BASELINE — copy and independent mis-repairing
    alike, because copies counted as independent support. PACT
    consistency-independent-support retired that baseline, so the test now pins
    the split it was written to make measurable, and pins the two halves of it
    that the gradability witness does not:

    * the DENOMINATORS are balanced (both flavors seat the same number of
      preserved majorities), so copy's zero is a real split and not an empty
      denominator dressed up as a result;
    * EVERY preserved independent majority still mis-repairs — the strong form
      of "independent-flavor unchanged", stronger than the gradability
      witness's ``>= 1``.
    """
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run", preset=_PRESET
    )
    assert result["clean"]["status"] == "published"
    assert result["corrupted"]["status"] == "published"

    flavor = result["scorecard"]["majority_flavor"]
    copy = flavor[FLAVOR_COPY]
    indep = flavor[FLAVOR_INDEPENDENT]

    # BOTH flavors carry non-zero preserved majorities, in equal number
    assert copy["preserved"] >= 1, f"copy flavor seated no preserved majority: {flavor}"
    assert indep["preserved"] >= 1, (
        f"independent flavor seated no preserved majority: {flavor}"
    )
    assert copy["preserved"] == indep["preserved"], (
        f"the alternate split must give the two flavors the same denominator, "
        f"or the per-flavor rates are not comparable: {flavor}"
    )
    # the split: copies corroborate nothing, independent sources still settle
    assert copy["mis_repaired"] == 0, (
        f"a copy-majority still mis-repaired — echoed copies are being counted "
        f"as independent support again: {flavor}"
    )
    assert indep["mis_repaired"] == indep["preserved"], (
        f"an independent majority stopped mis-repairing — a majority of "
        f"genuinely independent sources is better evidence and MUST still "
        f"settle; the fix has over-reached: {flavor}"
    )


@pytest.mark.postgres
def test_provenance_fix_gradability(clean_runtime: str, tmp_path: Path) -> None:
    """The witness the engine fix FLIPPED — now a standing regression test.

    It was a strict-xfail naming the unlanded independence-weighting fix: copies
    counted as independent support, so COPY-flavor majorities mis-repaired
    exactly like INDEPENDENT ones. PACT consistency-independent-support landed
    that fix (support counts distinct declared ORIGIN; a claim declaring none is
    its own origin), the test flipped to a loud strict XPASS, and the marker
    comes off in the same change so the assertion becomes standing: COPY-flavor
    mis_repaired must stay 0 and INDEPENDENT-flavor must keep mis-repairing —
    a majority of genuinely independent sources is better evidence and must
    still settle."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run", preset=_PRESET
    )
    flavor = result["scorecard"]["majority_flavor"]
    copy = flavor[FLAVOR_COPY]
    indep = flavor[FLAVOR_INDEPENDENT]

    # non-vacuity: both flavors seated preserved majorities to grade over
    assert copy["preserved"] >= 1 and indep["preserved"] >= 1, flavor

    # THE FLIP: copy repaired away (0), independent still mis-repairing (>= 1).
    assert copy["mis_repaired"] == 0, (
        f"copy-flavor majorities still mis-repair — the independence-weighting "
        f"fix has not landed: {flavor}"
    )
    assert indep["mis_repaired"] >= 1, (
        f"independent-flavor majorities must keep mis-repairing (the majority is "
        f"genuinely better evidence): {flavor}"
    )


@pytest.mark.postgres
def test_declaring_copy_origins_lowers_the_run_s_mis_repair_count(
    clean_runtime: str, tmp_path: Path
) -> None:
    """req.support.graded ACCEPTANCE: the END-TO-END effect, measured rather
    than assumed — a composed run whose preset declares copy origins on its
    majority sites reports a LOWER mis-repair count than the SAME preset
    without the declarations, both numbers from run output.

    ``provenance_copy`` declares one shared origin on every majority site's
    corrupted renderings. The undeclared arm is that preset with the provenance
    axis switched off — the same seed, the same world, a byte-identical corpus
    (test_corpus_byte_identical_across_flavors_and_disabled), so the ONLY delta
    is the declaration and the whole-run delta is attributable to it.

    The comparable number is the run funnel's ``mis_repaired`` state count over
    all corruptions, not the per-flavor slice: the per-flavor block reads zero
    on a non-provenance run (there are no flavors to slice by), so it cannot
    express "the same preset without the declarations" at all.
    """
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    declared_config = load_preset("provenance_copy")
    assert declared_config.provenance.enabled
    assert declared_config.provenance.flavor_split == "copy"
    # the same preset WITHOUT the declarations
    undeclared_config = declared_config.model_copy(
        update={"provenance": ProvenanceAxis()}
    )
    assert not undeclared_config.provenance.enabled

    def _run(config: Any, name: str) -> dict[str, Any]:
        result = run_synth_benchmark(
            dsn=dsn,
            deployment_dir=_DEP_DIR,
            out_dir=tmp_path / name,
            preset="provenance_copy",
            config=config,
        )
        assert result["clean"]["status"] == "published"
        assert result["corrupted"]["status"] == "published"
        return result

    # the undeclared arm runs FIRST so a declared-arm failure cannot be blamed
    # on a dirty graph the undeclared arm left behind.
    undeclared = _run(undeclared_config, "undeclared")
    declared = _run(declared_config, "declared")

    mis_u = undeclared["scorecard"]["funnel"]["states"][STATE_MIS_REPAIRED]
    mis_d = declared["scorecard"]["funnel"]["states"][STATE_MIS_REPAIRED]

    # non-vacuity: the undeclared arm really does mis-repair, and both runs
    # graded the same number of corruptions.
    assert mis_u >= 1, (
        f"the undeclared arm mis-repaired nothing, so there is no reduction to "
        f"measure: {undeclared['scorecard']['funnel']['states']}"
    )
    assert (
        undeclared["scorecard"]["funnel"]["total"]
        == declared["scorecard"]["funnel"]["total"]
    ), "the two arms did not grade the same corpus"

    assert mis_d < mis_u, (
        f"declaring copy origins on every majority site did not lower the run's "
        f"mis-repair count: undeclared={mis_u} declared={mis_d}"
    )
    # every majority site is a copy here, so the whole flavor collapses
    assert declared["scorecard"]["majority_flavor"][FLAVOR_COPY][
        "mis_repaired"
    ] == 0
