"""task.complete.capstone — the COMPOSITION PROOF.

A single deterministic (LLM-free, structured) run that stamps the precedence arm,
the coverage Σ policies, the corrections, and a majority-visibility class into ONE
manifest across three pinned generations, and proves the four axes compose without
destroying each other. This is the cheap twin of the real prose+LLM capstone: it
runs in normal suite time against a fresh dedicated DB and asserts the capstone
gate (a)-(h) mechanically, so a composition collision is caught on every run.

The real prose capstone (test_capstone_real_run.py, SYNTH_CAPSTONE_REAL=1) shares
the flow and asserts the same gate on the LLM lane.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.capstone import CAPSTONE_FILENAME, run_synth_capstone  # noqa: E402
from okg.bench.synth.config import ADMITTABLE_SIGNALS, load_preset  # noqa: E402
from okg.bench.synth.corruption import CONSTRAINT_KINDS  # noqa: E402
from okg.bench.synth.resolution import (  # noqa: E402
    STATE_MIS_REPAIRED,
    STATE_REMOVED_FROM_SCOPE,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "catalog-scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


@pytest.mark.postgres
def test_capstone_smoke_composes_all_four_axes(
    clean_runtime: str, tmp_path: Path
) -> None:
    """One deterministic run carries every axis, checked HERE from the run output
    by the assertions the GATE DOES NOT MAKE.

    ``run_synth_capstone`` raises ``CapstoneGateError`` before returning unless
    every required axis passed, so ``result`` existing already proves
    ``all_required_axes_passed``, ``axes[x]["passed"]`` for every required x, and
    every expression those flags are computed from. Re-asserting them here reads
    like coverage and cannot fail. What is asserted below is the complement: the
    facts outside the gate's own predicate (denominators actually non-zero, the
    corrections population check, agreement between two independently computed
    tallies, the lane identity) plus the pinned golden, which is what would
    actually catch a composition collision that moves a number.

    The gate's own predicate is falsified separately, against the report written
    to disk BEFORE the raise, in
    ``test_capstone_gate_report_localizes_the_failing_axis``.
    """
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_capstone(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run",
        preset="capstone_smoke",
    )
    report = result["capstone"]
    core = result["scorecard"]

    # three real generations published
    assert result["clean"]["status"] == "published"
    assert result["corrupted"]["status"] == "published"
    assert result["corrected"]["status"] == "published"

    axes = report["axes"]

    # (c) the kind histogram covers EXACTLY the Σ vocabulary. The gate only
    # checks that no required kind is missing, so an extra kind — a Σ vocabulary
    # drift between corruption.py and the finding stream — passes the gate and
    # fails here.
    by_kind = axes["c_kind_histogram"]["by_kind"]
    assert set(by_kind) == set(CONSTRAINT_KINDS)

    # (d) the corrections population check is NOT part of axis d's passed
    # predicate (which reads retracted_absent and removed_from_scope only).
    assert axes["d_corrections_delta"]["population_ok"]
    # the two-generation corrections funnel independently reaches the reserved
    # state — a second tally of the same event, from result["corrections"].
    assert result["corrections"]["funnel_states"][STATE_REMOVED_FROM_SCOPE] >= 1

    # (e) this is the deterministic control lane, so the prose+LLM axis is NOT
    # required — the gate passing tells us nothing here, since an unrequired
    # axis is skipped entirely.
    assert axes["e_prose_llm_lane"]["required"] is False
    assert report["extractor"] == "deterministic_labels"

    # (g) the gate's rates check is KEY PRESENCE plus the surviving-conflict
    # floor; it never looks at the denominators. A zero denominator would report
    # a clean-looking rate over an empty set and pass the gate.
    g = axes["g_detection_repair_rates"]
    assert g["detection_recall"]["conditional"]["denominator"] > 0
    assert g["detection_recall"]["end_to_end"]["denominator"] > 0
    assert g["repair_rate"]["conditional"]["denominator"] > 0
    assert g["repair_rate"]["end_to_end"]["denominator"] > 0

    # (h) the two independent tallies of mis-repair agree: the capstone's
    # per-site walk of realized_vote, and the scorecard funnel's state counts.
    h = axes["h_majority_mis_repair"]
    assert core["funnel"]["states"][STATE_MIS_REPAIRED] >= len(h["mis_repaired_preserved"])
    # every basis the histogram reports is a real one — the gate checks only
    # that none of ADMITTABLE_SIGNALS is absent, never that nothing else crept in
    assert set(axes["b_basis_histogram"]["histogram"]) <= set(ADMITTABLE_SIGNALS)

    # the capstone report is on disk
    assert (tmp_path / "run" / CAPSTONE_FILENAME).exists()

    # the golden pin: the deterministic gen2 scorecard (funnel / basis-driving
    # resolutions / per-kind / survival) is byte-comparable across runs, so a
    # composition collision that changes any number is caught, not just a missing
    # axis. Config/gen ids stay out of the core, exactly as test_run_e2e's golden.
    golden_path = Path(__file__).parent / "golden_capstone_smoke_scorecard.json"
    if golden_path.exists():
        golden = json.loads(golden_path.read_text())
        assert core == golden, (
            "capstone_smoke scorecard drifted from the pinned golden; if the change "
            "is intentional, regenerate "
            "tests/bench/synth/golden_capstone_smoke_scorecard.json"
        )
    else:  # first run mints the golden (committed with the change)
        assert not os.environ.get("CI"), (
            "golden_capstone_smoke_scorecard.json is missing under CI"
        )
        golden_path.write_text(json.dumps(core, sort_keys=True, indent=1) + "\n")


def test_capstone_smoke_preset_declares_the_floors() -> None:
    """The per-kind, per-basis and majority floors are DECLARED in the preset, so a
    kind/basis zeroing out by draw, or a mis-repair read over an empty denominator,
    fails the run rather than passing quietly."""
    cfg = load_preset("capstone_smoke")
    assert set(cfg.require_constraint_kinds) == set(CONSTRAINT_KINDS)
    assert set(cfg.precedence_arm.admits) == set(ADMITTABLE_SIGNALS)
    assert cfg.majority_wrong_floor >= 1
    assert cfg.surviving_conflict_floor >= 1
    # the majority axis needs a visibility>1 class to be expressible at all
    assert any(
        spec.visibility_corrupted > 1
        for spec in cfg.corruption.classes.values()
        if spec.count
    )


@pytest.mark.postgres
def test_capstone_gate_report_localizes_the_failing_axis(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Drop the majority-visibility class: axis (h) can no longer be carried and the
    capstone gate FAILS naming the missing axis — a silent pass is the thing the
    whole task guards against."""
    from okg.bench.synth.capstone import CapstoneGateError

    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    cfg = load_preset("capstone_smoke")
    classes = {
        name: spec
        for name, spec in cfg.corruption.classes.items()
        if name != "cardinality_violation"
    }
    broken = cfg.model_copy(update={
        "corruption": cfg.corruption.model_copy(update={"classes": classes}),
    })
    out_dir = tmp_path / "broken"
    with pytest.raises(CapstoneGateError, match="h_majority_mis_repair"):
        run_synth_capstone(
            dsn=dsn, deployment_dir=_DEP_DIR, out_dir=out_dir, config=broken,
        )

    # The report is written BEFORE the gate raises, which is the only place the
    # axis flags can be read while a required axis is FALSE. Asserting them in
    # the passing test is unfalsifiable — the gate raised on exactly those
    # expressions — so the flags are pinned here instead, and the failure is
    # shown to be LOCALIZED: h is false, every other required axis still true.
    report = json.loads((out_dir / CAPSTONE_FILENAME).read_text())
    assert report["all_required_axes_passed"] is False
    axes = report["axes"]
    assert axes["h_majority_mis_repair"]["passed"] is False
    assert axes["h_majority_mis_repair"]["mis_repaired_preserved"] == []
    still_passing = {
        name: row["passed"]
        for name, row in axes.items()
        if row["required"] and name != "h_majority_mis_repair"
    }
    assert all(still_passing.values()), (
        f"dropping the majority class must fail ONLY axis h: {still_passing}"
    )
