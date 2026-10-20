"""PACT ``concord-bench-e2e``, ``req.e2e.capstone`` — the completion gate.

ONE seeded 22-update history, replayed through complete, dependency-only and
exact (scheduler-governed differential) execution, filling the paper's Table
injection-results "Controlled fixture" row entirely from run output.

The postgres witness at the bottom is the row itself: it drives the three modes,
grades every update, runs the five safety mutations, and asserts the row's
acceptance clauses —

* every cell is read from published run output or the work ledger (asserted by
  cross-checking the row's cells against the ``ModeRun`` objects the replay
  returned, and by the provenance every cell carries),
* the exact-mode row reports ZERO unsound reuses, with the safety-mutation
  witnesses attached as the reason that zero is evidence rather than an absence
  of testing,
* the deferred cells (budget, disclosure) carry the ``DEFERRED`` marker and name
  their gating surface, and are never a number.

The non-postgres tests above it are the ROW CONTRACT's own witnesses: each
drives one build-time invariant against a deliberately holed row and shows it
FIRE. A row builder whose refusals are never exercised would let a blank cell
through as a zero, which is the failure this row exists to rule out.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from okg.bench.synth.controlled_fixture import (
    BUDGET_GATE,
    CLASS_ENGINE_FP,
    CLASS_EXTRACTOR_FP,
    CLASS_ROSTER_ARTIFACT,
    CLASS_TRUE_POSITIVE,
    CONTROLLED_FIXTURE_SCHEMA,
    DEFERRED,
    MODE_EXACT,
    ControlledFixtureError,
    assert_row_invariants,
    bucket_conflict_causes,
    cell,
    classify_flagged_buckets,
    deferred_cell,
    detection_precision,
    is_deferred,
    live_manifest,
    render_controlled_fixture,
)
from okg.bench.synth.parity import (
    MODE_COMPLETE,
    MODE_DEPENDENCY,
    MODES,
    VERDICT_CERTIFIED,
    VERDICT_EQUAL,
)

#: The durable-store labels the two rows are filed under. Declared in
#: ``durable.REQUIRED_ANALYSIS_LABELS`` too, so a row that is computed here and
#: never stored is a `durable verify` failure rather than a silent absence.
ROW_LABEL_SMOKE = "controlled-fixture-row"
ROW_LABEL_PRICED = "priced-history-row"


def _record_row(row: dict, rendered: str, *, label: str, note: str) -> None:
    """Store a row in the durable record store, and PROVE it reads back.

    Off unless ``OKG_SYNTH_RECORD`` names a records root, so an ordinary test run
    writes nothing into the repository. When it IS on, the round-trip is asserted
    here rather than assumed: a store that wrote an unreadable artifact would
    leave the same citation-into-nothing this whole change exists to end.
    """
    from okg.bench.synth.durable import read_analysis, record_analysis_if_enabled

    stored = record_analysis_if_enabled(
        row, label=label, rendered=rendered, note=note
    )
    if stored is None:
        return
    assert read_analysis(str(stored)) == json.loads(json.dumps(row)), (
        f"{stored} did not read back as the row it was stored from"
    )
    print(f"[{label}] row recorded durably at {stored}")

# =========================================================================== #
# The cell contract                                                           #
# =========================================================================== #
def test_a_cell_must_name_where_it_was_read_from() -> None:
    """An untraceable number is exactly what the row exists to rule out."""
    with pytest.raises(ControlledFixtureError, match="artifact it was read from"):
        cell(3, source="")


def test_a_cell_with_no_value_is_refused_rather_than_left_blank() -> None:
    with pytest.raises(ControlledFixtureError, match="must be DEFERRED"):
        cell(None, source="a mode's work ledger")


def test_a_deferred_cell_carries_the_marker_and_its_gating_surface() -> None:
    entry = deferred_cell(gating_surface=BUDGET_GATE, reason="the scheduler is absent")
    assert entry["value"] == DEFERRED
    assert entry["value"] != 0 and entry["value"] is not None
    assert is_deferred(entry)
    # It must not be summable as a zero: json round-trip keeps it a STRING.
    assert json.loads(json.dumps(entry))["value"] == DEFERRED
    with pytest.raises(TypeError):
        _ = entry["value"] + 0


def test_a_deferral_with_no_named_gate_is_refused() -> None:
    with pytest.raises(ControlledFixtureError, match="gating surface"):
        deferred_cell(gating_surface="", reason="because")
    with pytest.raises(ControlledFixtureError, match="gating surface"):
        deferred_cell(gating_surface=BUDGET_GATE, reason="")


# =========================================================================== #
# Precision, from the finding rows                                            #
# =========================================================================== #
class _Finding:
    def __init__(
        self, bucket_key: str, channel: str, claim_ids: frozenset[str] = frozenset()
    ) -> None:
        self.bucket_key = bucket_key
        self.channel = channel
        self.claim_ids = claim_ids


def test_precision_charges_a_flag_on_a_clean_bucket_as_a_false_positive() -> None:
    block = detection_precision(
        findings=[
            _Finding("weight:p1", "definite"),
            _Finding("weight:p2", "possible"),
            _Finding("clean:p9", "definite"),
            _Finding("noise:p8", "informational"),   # not a conflict channel
        ],
        gold_buckets=frozenset({"weight:p1", "weight:p2"}),
        surviving_gold_buckets=frozenset({"weight:p1"}),
    )
    assert block["flagged_buckets"] == 3, "the informational channel is not a flag"
    assert block["end_to_end"] == {
        "denominator": 3, "true_positive": 2, "false_positive": 1,
        "rate": pytest.approx(2 / 3),
    }
    # The conditional reading is strictly harsher: a flag on a bucket whose
    # conflict extraction destroyed is not evidence of detection.
    assert block["conditional"]["true_positive"] == 1
    assert block["conditional"]["rate"] == pytest.approx(1 / 3)
    assert block["false_positive_buckets"] == ["clean:p9"]


# --------------------------------------------------------------------------- #
# req.row.precision — every flagged non-gold bucket is attributed              #
# --------------------------------------------------------------------------- #
_SEALED = {
    ("doc_a.md", "weight", "p1"): frozenset({"70 kg"}),
    ("doc_b.md", "weight", "p1"): frozenset({"90 kg"}),
    ("doc_a.md", "height", "p2"): frozenset({"170 cm"}),
    ("update_03_fresh.md", "height", "p2"): frozenset({"180 cm"}),
    ("doc_a.md", "pulse", "p3"): frozenset({"60 bpm"}),
    ("doc_b.md", "pulse", "p3"): frozenset({"60 bpm"}),
}


def _classify(findings, claims, **overrides):
    # A real finding names the claims it joined; fill that in from the bucket so
    # each case below states only the claim population, which is what it is about.
    for finding in findings:
        if not finding.claim_ids:
            finding.claim_ids = frozenset(
                cid for cid in claims
                if ":".join(cid.split(":")[2:4]) == finding.bucket_key
            )
    kwargs = dict(
        findings=findings,
        claims=claims,
        gold_buckets=frozenset({"weight:p1"}),
        surviving_gold_buckets=frozenset({"weight:p1"}),
        gold_conflicted=frozenset({"weight:p1", "height:p2"}),
        sealed_keys=_SEALED,
        causes={
            "height:p2": [
                {"cause": "history_update_conflict", "delta": "fresh_conflict",
                 "update": 3, "doc": "update_03_fresh.md", "value": "180 cm"}
            ]
        },
    )
    kwargs.update(overrides)
    return classify_flagged_buckets(**kwargs)


def _by_bucket(block):
    return {row["bucket"]: row for row in block["table"]}


def test_a_flag_on_a_bucket_gold_declares_conflicted_is_a_roster_artifact() -> None:
    """The bucket carries a real contradiction the CORRUPTION roster omits."""
    block = _classify(
        [
            _Finding("weight:p1", "definite"),
            _Finding("height:p2", "definite"),
        ],
        {
            "claim:doc_a.md:weight:p1:0": "70 kg",
            "claim:doc_b.md:weight:p1:0": "90 kg",
            "claim:doc_a.md:height:p2:0": "170 cm",
            "claim:update_03_fresh.md:height:p2:0": "180 cm",
        },
    )
    rows = _by_bucket(block)
    assert rows["weight:p1"]["classification"] == CLASS_TRUE_POSITIVE
    assert rows["height:p2"]["classification"] == CLASS_ROSTER_ARTIFACT
    assert rows["height:p2"]["sealed_causes"][0]["cause"] == "history_update_conflict"
    # RAW precision would read 1/2; the corrected figure drops the artifact from
    # BOTH sides and reports the flag the engine is answerable for.
    assert block["engine"]["end_to_end"] == {
        "denominator": 1, "true_positive": 1, "false_positive": 0, "rate": 1.0,
    }
    assert block["excluded_from_engine_precision"] == ["height:p2"]


def test_a_constructed_spurious_flag_on_a_clean_bucket_is_an_engine_false_positive() -> None:
    """ACCEPTANCE (non-vacuity): the classification can charge the engine.

    ``pulse:p3`` is unanimous at the pin — both sealed surfaces read 60 bpm, no
    sealed delta plants a contradiction in it, and the per-step gold does not
    declare it conflicted. A flag there is spurious, and it must land in the one
    class that debits the engine.
    """
    block = _classify(
        [
            _Finding("weight:p1", "definite"),
            _Finding("height:p2", "definite"),
            _Finding("pulse:p3", "definite"),   # the constructed spurious flag
        ],
        {
            "claim:doc_a.md:weight:p1:0": "70 kg",
            "claim:doc_b.md:weight:p1:0": "90 kg",
            "claim:doc_a.md:height:p2:0": "170 cm",
            "claim:update_03_fresh.md:height:p2:0": "180 cm",
            "claim:doc_a.md:pulse:p3:0": "60 bpm",
            "claim:doc_b.md:pulse:p3:0": "60 bpm",
        },
    )
    rows = _by_bucket(block)
    assert rows["pulse:p3"]["classification"] == CLASS_ENGINE_FP
    assert rows["pulse:p3"]["gold_declares_conflicted"] is False
    assert rows["pulse:p3"]["sealed_causes"] == []
    assert rows["pulse:p3"]["unsealed_claims"] == []
    assert block["by_class"][CLASS_ENGINE_FP] == ["pulse:p3"]
    # The engine figure MOVES when the engine is charged: 1/1 above, 1/2 here.
    assert block["engine"]["end_to_end"] == {
        "denominator": 2, "true_positive": 1, "false_positive": 1, "rate": 0.5,
    }
    assert block["gold_conflict_agreement"]["flagged_not_conflicted"] == ["pulse:p3"]


def test_a_flag_over_an_invented_claim_is_charged_to_the_extractor() -> None:
    block = _classify(
        [_Finding("weight:p1", "definite"), _Finding("pulse:p3", "definite")],
        {
            "claim:doc_a.md:weight:p1:0": "70 kg",
            "claim:doc_b.md:weight:p1:0": "90 kg",
            "claim:doc_a.md:pulse:p3:0": "60 bpm",
            "claim:doc_b.md:pulse:p3:0": "999 bpm",   # no sealed surface says this
        },
    )
    rows = _by_bucket(block)
    assert rows["pulse:p3"]["classification"] == CLASS_EXTRACTOR_FP
    assert rows["pulse:p3"]["unsealed_claims"][0]["value"] == "999 bpm"
    # An extraction defect never debits the engine.
    assert block["by_class"][CLASS_ENGINE_FP] == []
    assert block["engine"]["end_to_end"]["denominator"] == 1


def test_a_dominated_update_is_not_a_cause_so_it_cannot_excuse_a_flag() -> None:
    """A restatement of the standing value plants no disagreement."""

    class _Delta:
        def __init__(self, kind, bucket):
            self.kind = kind
            self.bucket = bucket
            self.corruption_id = ""
            self.detail = {"doc": "update_04_dominated.md", "value": "170 cm"}

    class _Step:
        def __init__(self, index, delta):
            self.index = index
            self.delta = delta

    class _History:
        steps = (
            _Step(0, _Delta("dominated_claim", "height:p2")),
            _Step(1, _Delta("fresh_conflict", "weight:p1")),
        )

    causes = bucket_conflict_causes(_History(), 1)
    assert "height:p2" not in causes
    assert causes["weight:p1"][0]["cause"] == "history_update_conflict"


def test_an_unexplained_gold_conflict_is_refused_rather_than_filed_as_an_artifact() -> None:
    """The roster class needs BOTH gold's verdict and a sealed cause."""
    with pytest.raises(ControlledFixtureError, match="no sealed delta plants"):
        _classify(
            [_Finding("height:p2", "definite")],
            {
                "claim:doc_a.md:height:p2:0": "170 cm",
                "claim:update_03_fresh.md:height:p2:0": "180 cm",
            },
            causes={},
        )


def test_live_manifest_refuses_a_corruption_the_gold_roster_does_not_carry() -> None:
    manifest = {"gold": {"corruptions": [{"corruption_id": "c1"}]}}
    assert len(live_manifest(manifest, ["c1"])["gold"]["corruptions"]) == 1
    assert live_manifest(manifest, [])["gold"]["corruptions"] == []
    with pytest.raises(ControlledFixtureError, match="no such gold site"):
        live_manifest(manifest, ["c1", "ghost"])


# =========================================================================== #
# The row's build-time invariants, each shown to FIRE                          #
# =========================================================================== #
def _stub_row() -> dict:
    """A minimal WELL-FORMED row, so each hole below is the only defect."""
    modes = {}
    for mode in MODES:
        modes[mode] = None
    detection = {
        mode: {
            "recall_conditional": cell(
                {"detected": 2, "denominator": 3, "rate": 2 / 3},
                source="funnel", pin="g1", field="detection_recall.conditional",
            ),
            "recall_end_to_end": cell(
                {"detected": 2, "denominator": 4, "rate": 0.5},
                source="funnel", pin="g1", field="detection_recall.end_to_end",
            ),
        }
        for mode in MODES
    }
    restoration = {
        mode: {
            "repaired": cell(1, source="funnel", pin="g1", field="states.repaired"),
            "mis_repaired": cell(
                0, source="funnel", pin="g1", field="states.mis_repaired"
            ),
        }
        for mode in MODES
    }
    # A well-formed PRICED row bills the three modes DIFFERENTLY — the modes do
    # different amounts of work, and a priced row whose totals tie is refused. The
    # stub varies the class the modes really differ on (checks evaluated), so the
    # totals separate 8 > 7 > 6 with the exact mode cheapest.
    checks_per_mode = {MODE_COMPLETE: 3, MODE_DEPENDENCY: 2, MODE_EXACT: 1}

    def _priced(n_checks: int) -> dict:
        return {
            "total": 5.0 + n_checks,
            "by_class": {
                "parse": {"count": 1, "unit_price": 1.0, "cost": 1.0},
                "extract_document": {"count": 1, "unit_price": 1.0, "cost": 1.0},
                "check": {"count": n_checks, "unit_price": 1.0, "cost": float(n_checks)},
                "correction": {"count": 1, "unit_price": 1.0, "cost": 1.0},
                "publish": {"count": 2, "unit_price": 1.0, "cost": 2.0},
            },
        }

    action_cost = {
        mode: {
            "spent_action_cost": cell(
                _priced(checks_per_mode[mode]), source="ledger",
                field="ledger x prices",
                priced_corpus=True, price_table={"parse": 1.0}, note=None,
            ),
            "action_counts": cell(
                {"parse": 1, "extract_document": 1, "check": checks_per_mode[mode],
                 "correction": 1, "publish": 2},
                source="ledger", field="action_counts_from_step_ledger",
            ),
            "steps_run": cell(10, source="ledger", field="steps_run"),
            "steps_skipped": cell(0, source="ledger", field="steps_skipped"),
            "publishes": cell(2, source="ledger", field="publishes"),
            "skips_by_cause": cell({}, source="ledger", field="skips_by_cause"),
        }
        for mode in MODES
    }
    del modes
    return {
        "schema": CONTROLLED_FIXTURE_SCHEMA,
        "row": "Controlled fixture",
        "history": {"updates": cell(2, source="history", field="steps")},
        "detection": detection,
        "restoration": restoration,
        "action_cost": action_cost,
        "parity": {
            "verdicts": cell(
                {VERDICT_EQUAL: 1, VERDICT_CERTIFIED: 1},
                source="verdicts", field="verdict", updates=2,
            ),
            "silent_divergences": cell(
                0, source="verdicts", field="non-equal", offending_updates=[]
            ),
            "certified_updates": cell([1], source="verdicts", field="certified"),
            "qualified": cell(True, source="funnel", field="qualification", reason="ok"),
        },
        "safety": {
            "unsound_reuses": cell(0, source="verdicts", field="unsound"),
            "why_the_zero_is_evidence": cell(
                [{"mutation": "m", "update": 1, "fired": "it fired"}],
                source="mutations", field="witness", mutations=1,
            ),
        },
        "error_cost": {
            "remaining_unbudgeted": cell(
                {"total": 0.0}, source="funnel", pin="g1", field="remaining"
            ),
            "remaining_at_declared_budgets": deferred_cell(
                gating_surface=BUDGET_GATE, reason="no approximate scheduler"
            ),
            "disclosure_misses": deferred_cell(
                gating_surface=BUDGET_GATE, reason="nothing is deferred, so nothing "
                "is disclosed"
            ),
        },
    }


def test_the_stub_row_passes_so_every_hole_below_is_the_only_defect() -> None:
    assert_row_invariants(_stub_row())


def test_an_ungraded_update_is_refused() -> None:
    row = _stub_row()
    row["history"]["updates"]["value"] = 3
    with pytest.raises(ControlledFixtureError, match="silent divergence the row"):
        assert_row_invariants(row)


def test_a_mismatch_verdict_is_refused() -> None:
    row = _stub_row()
    row["parity"]["verdicts"]["value"] = {VERDICT_EQUAL: 1, "MISMATCH": 1}
    with pytest.raises(ControlledFixtureError, match="MISMATCH"):
        assert_row_invariants(row)


def test_a_reported_silent_divergence_is_refused() -> None:
    row = _stub_row()
    row["parity"]["silent_divergences"]["value"] = 1
    row["parity"]["silent_divergences"]["provenance"]["offending_updates"] = [7]
    with pytest.raises(ControlledFixtureError, match="update\\(s\\) \\[7\\]"):
        assert_row_invariants(row)


def test_a_zero_with_no_safety_witness_is_refused() -> None:
    """The acceptance in one test: an untested zero is not evidence."""
    row = _stub_row()
    row["safety"]["why_the_zero_is_evidence"]["value"] = []
    with pytest.raises(ControlledFixtureError, match="untested zero"):
        assert_row_invariants(row)


def test_a_missing_execution_mode_is_refused() -> None:
    for block in ("detection", "restoration", "action_cost"):
        row = _stub_row()
        del row[block][MODE_DEPENDENCY]
        with pytest.raises(ControlledFixtureError, match="missing mode"):
            assert_row_invariants(row)


def test_an_unpriced_action_class_is_refused() -> None:
    row = _stub_row()
    del row["action_cost"][MODE_EXACT]["spent_action_cost"]["value"]["by_class"]["check"]
    with pytest.raises(ControlledFixtureError, match="unpriced class costs nothing"):
        assert_row_invariants(row)


def test_costs_that_do_not_reconstruct_the_total_are_refused() -> None:
    row = _stub_row()
    row["action_cost"][MODE_COMPLETE]["spent_action_cost"]["value"]["total"] = 99.0
    with pytest.raises(ControlledFixtureError, match="per-class costs sum to"):
        assert_row_invariants(row)


def test_an_unpriced_corpus_must_say_so_rather_than_show_a_bare_zero() -> None:
    """A $0.00 total on a corpus that prices nothing is not a measured saving."""
    row = _stub_row()
    for mode in MODES:
        spent = row["action_cost"][mode]["spent_action_cost"]
        spent["provenance"]["priced_corpus"] = False
        spent["provenance"]["note"] = None
    with pytest.raises(ControlledFixtureError, match="reads as a measured saving"):
        assert_row_invariants(row)
    # With the note present the row builds — and the total must then really be 0.
    for mode in MODES:
        row["action_cost"][mode]["spent_action_cost"]["provenance"]["note"] = "unpriced"
    with pytest.raises(ControlledFixtureError, match="marked unpriced but the total"):
        assert_row_invariants(row)


def test_a_priced_corpus_whose_modes_bill_the_same_is_refused() -> None:
    """The unpriced zero's counterpart: a priced TIE is not a measured tie.

    Three identical dollar totals read as "the execution modes cost the same"
    however they arose. On an unpriced corpus the row says so in the cell; on a
    PRICED corpus there is no such excuse, so the row refuses."""
    row = _stub_row()
    tied = row["action_cost"][MODE_COMPLETE]["spent_action_cost"]["value"]
    row["action_cost"][MODE_EXACT]["spent_action_cost"]["value"] = json.loads(
        json.dumps(tied)
    )
    with pytest.raises(ControlledFixtureError, match="bill the same dollar total"):
        assert_row_invariants(row)


def test_a_priced_corpus_that_bills_a_mode_nothing_is_refused() -> None:
    """A $0.00 mode under a sealed price table is work that did not happen."""
    row = _stub_row()
    spent = row["action_cost"][MODE_EXACT]["spent_action_cost"]["value"]
    spent["total"] = 0.0
    for entry in spent["by_class"].values():
        entry["cost"] = 0.0
    with pytest.raises(ControlledFixtureError, match="total is 0.00"):
        assert_row_invariants(row)


def test_a_missing_action_count_leaves_nothing_to_compare() -> None:
    row = _stub_row()
    row["action_cost"][MODE_EXACT]["action_counts"]["value"] = {}
    with pytest.raises(ControlledFixtureError, match="no action counts"):
        assert_row_invariants(row)


def test_a_vacuous_detection_denominator_is_refused() -> None:
    row = _stub_row()
    row["detection"][MODE_EXACT]["recall_conditional"]["value"]["denominator"] = 0
    with pytest.raises(ControlledFixtureError, match="ZERO denominator"):
        assert_row_invariants(row)


def test_a_deferred_cell_filled_with_a_number_is_refused() -> None:
    """A proxy number in a deferred cell is the failure the acceptance names."""
    row = _stub_row()
    row["error_cost"]["disclosure_misses"]["value"] = 0
    with pytest.raises(ControlledFixtureError, match="could be read as a measured"):
        assert_row_invariants(row)


# =========================================================================== #
# THE ROW — one replayed history, three modes, every cell from run output      #
# =========================================================================== #
_UPDATES = 22


@pytest.mark.postgres
def test_one_replayed_history_fills_the_controlled_fixture_row(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    psycopg = pytest.importorskip("psycopg")
    assert psycopg is not None

    from okg.bench.synth.controlled_fixture import (
        run_controlled_fixture,
        write_controlled_fixture,
    )
    from tests.bench.synth.test_parity_replay import (
        _DEP_DIR,
        _install_composed_catalog,
        _reset_between_modes,
        _seal_recorded_history,
    )

    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _UPDATES
    )
    assert len(parity.updates) >= 10, "the verification is stated over >= 10 updates"

    # The sealed manifest carries the gold roster and the cost tables. It is
    # re-derived from the same (config, seed, world) the seal used and PINNED to
    # the history's own manifest digest, so the row is priced against the gold
    # the run was actually graded on rather than a look-alike.
    from okg.bench.synth.corruption import Planner
    from okg.bench.synth.goldseal import build_manifest
    from okg.bench.synth.prf import SynthPrf
    from okg.bench.synth.render import render_corpus

    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, smoke_config, planner, plan, prf)
    sealed_manifest = build_manifest(
        config=smoke_config, world=world, plan=plan, rendered=rendered
    )
    assert sealed_manifest["manifest_digest"] == parity.history.manifest_digest, (
        "the manifest re-sealed here is not the one the history was built from"
    )

    row, runs, verdicts = run_controlled_fixture(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_root=tmp_path,
        parity=parity, manifest=sealed_manifest, policies=policies, checks=checks,
        direct_report=direct, reset=lambda: _reset_between_modes(dsn),
    )

    rendered_row = render_controlled_fixture(row)
    print("\n" + rendered_row)
    out = write_controlled_fixture(row, tmp_path / "controlled-fixture-row.json")
    (tmp_path / "controlled-fixture-row.txt").write_text(
        rendered_row + "\n", encoding="utf-8"
    )
    print(f"\n[capstone] artifact written to {out}")
    # The row is a paper deliverable, so an operator can ask for it somewhere it
    # survives the test's temporary directory. Off by default; the artifact under
    # tmp_path is always written and is what the assertions below read back.
    keep = os.environ.get("OKG_SYNTH_CAPSTONE_OUT")
    if keep:
        write_controlled_fixture(row, Path(keep) / "controlled-fixture-row.json")
        (Path(keep) / "controlled-fixture-row.txt").write_text(
            rendered_row + "\n", encoding="utf-8"
        )
        print(f"[capstone] artifact also kept under {keep}")

    # A run an operator asked to RECORD survives the worktree: the row goes into
    # the durable record store as a CONTENT-ADDRESSED analysis record, indexed
    # beside the scored bundles and covered by `durable verify`. It used to be
    # copied to a fixed filename under the records root instead, which meant the
    # next recorded run silently replaced the draw the precision evidence was
    # read from — the same overwrite req.row.record-identity removed one level
    # down. Content addressing keeps every draw.
    _record_row(row, rendered_row, label=ROW_LABEL_SMOKE, note=(
        "the unpriced controlled-fixture row: one 22-update history, three "
        "execution modes, on the smoke corpus (action prices all zero, so the "
        "cost cells take the honest-zero path)."
    ))

    # ---- ACCEPTANCE (req.row.precision): every flagged bucket attributed ---- #
    # A per-bucket table for EVERY flagged bucket, the corrected engine figure
    # printed BESIDE the raw one, and the classification resting on sealed gold.
    from okg.bench.synth.controlled_fixture import (
        BUCKET_CLASSES,
        CLASS_ENGINE_FP,
        CLASS_ROSTER_ARTIFACT,
        CLASS_TRUE_POSITIVE,
    )

    for mode in MODES:
        det = row["detection"][mode]
        table = det["precision_attribution"]["value"]["table"]
        raw = det["precision_end_to_end"]["value"]
        engine = det["precision_engine"]["value"]["end_to_end"]
        assert len(table) == raw["denominator"], (
            f"mode {mode}: {len(table)} attributed vs {raw['denominator']} flagged"
        )
        for entry in table:
            assert entry["classification"] in BUCKET_CLASSES, entry
            assert entry["evidence"], entry
            assert entry["claims"], entry
            if entry["classification"] == CLASS_ROSTER_ARTIFACT:
                assert entry["gold_declares_conflicted"] is True, entry
                assert entry["sealed_causes"], entry
                assert entry["in_live_gold_roster"] is False, entry
            if entry["classification"] == CLASS_TRUE_POSITIVE:
                assert entry["in_live_gold_roster"] is True, entry
        # The corrected figure drops the artifacts from BOTH sides, and the raw
        # figure is still there beside it.
        excluded = det["precision_engine"]["provenance"]["excluded_buckets"]
        assert engine["denominator"] == raw["denominator"] - len(excluded), (
            engine, raw, excluded
        )
        assert det["precision_engine"]["provenance"]["raw_figure"] == raw

    exact_det = row["detection"][MODE_EXACT]
    exact_table = exact_det["precision_attribution"]["value"]
    print("\n[capstone] req.row.precision — every flagged bucket, attributed:")
    for entry in exact_table["table"]:
        print(f"    {entry['classification']:<26} {entry['bucket']}")
        print(f"      channels {','.join(entry['channels'])}  "
              f"gold_conflicted={entry['gold_declares_conflicted']}  "
              f"live_roster={entry['in_live_gold_roster']}")
        for cid, value in sorted(entry["claims"].items()):
            print(f"        {cid} = {value}")
        for cause in entry["sealed_causes"]:
            print(f"        sealed cause: {cause}")
        print(f"      {entry['evidence']}")
    raw = exact_det["precision_end_to_end"]["value"]
    eng = exact_det["precision_engine"]["value"]["end_to_end"]
    print(
        f"\n[capstone] RAW precision {raw['true_positive']}/{raw['denominator']} "
        f"(graded against the live CORRUPTION roster) | ENGINE precision "
        f"{eng['true_positive']}/{eng['denominator']} with "
        f"{len(exact_table['by_class'][CLASS_ROSTER_ARTIFACT])} roster "
        f"artifact(s) separated | engine false positives "
        f"{len(exact_table['by_class'][CLASS_ENGINE_FP])}"
    )
    print(f"[capstone] gold-conflict agreement: "
          f"{exact_table['gold_conflict_agreement']}")
    assert "ENGINE precision" in rendered_row and "RAW" in rendered_row
    assert "FLAGGED-BUCKET ATTRIBUTION" in rendered_row

    # ---- ACCEPTANCE: the classification is FALSIFIABLE on the real run ------ #
    # A spurious flag is constructed on a bucket this run left CLEAN — unanimous
    # at the pin, absent from the sealed per-step gold's conflicted set, and
    # named by no delta — and pushed through the SAME classifier with the SAME
    # sealed inputs the row used. It must come back charged to the engine, and
    # the engine precision must move. Without this the corrected figure would be
    # a rule that can only ever exonerate.
    from okg.bench.synth.controlled_fixture import sealed_surface_keys
    from okg.bench.synth.resolution import corruption_bucket_map

    exact_run = runs[MODE_EXACT].executions[-1]
    gold_conflicted = frozenset(parity.gold_step(exact_run.index).expected_state)
    claims_by_bucket: dict[str, dict[str, str]] = {}
    for claim_id, value in exact_run.detection.claims.items():
        parts = claim_id.split(":")
        if len(parts) == 5 and parts[0] == "claim":
            claims_by_bucket.setdefault(f"{parts[2]}:{parts[3]}", {})[claim_id] = value
    clean_buckets = sorted(
        bucket
        for bucket, members in claims_by_bucket.items()
        if bucket not in gold_conflicted
        and len(members) >= 2
        and len(set(members.values())) == 1
    )
    assert clean_buckets, "the pin holds no unanimous non-conflicted bucket to plant on"
    victim = clean_buckets[0]

    class _SpuriousFinding:
        bucket_key = victim
        channel = "definite"
        claim_ids = frozenset(claims_by_bucket[victim])

    forged = classify_flagged_buckets(
        findings=[*exact_run.detection.findings, _SpuriousFinding()],
        claims=exact_run.detection.claims,
        gold_buckets=frozenset(
            corruption_bucket_map(
                live_manifest(
                    sealed_manifest,
                    tuple(parity.gold_step(exact_run.index).live_corruptions),
                )
            ).values()
        ),
        surviving_gold_buckets=frozenset(),
        gold_conflicted=gold_conflicted,
        sealed_keys=sealed_surface_keys(
            sealed_manifest, parity.history, exact_run.index
        ),
        causes=bucket_conflict_causes(parity.history, exact_run.index),
    )
    charged = {e["bucket"]: e for e in forged["table"]}[victim]
    assert charged["classification"] == CLASS_ENGINE_FP, charged
    assert forged["by_class"][CLASS_ENGINE_FP] == [victim]
    assert forged["engine"]["end_to_end"]["false_positive"] == 1
    assert forged["engine"]["end_to_end"]["rate"] < eng["rate"], (
        "a spurious flag left the engine precision unchanged; the corrected "
        "figure would be a rule that can only exonerate"
    )
    print(
        f"\n[capstone] NON-VACUITY: a constructed flag on the clean bucket "
        f"{victim!r} (claims all {sorted(set(claims_by_bucket[victim].values()))}) "
        f"classifies as {charged['classification']}, and engine precision moves "
        f"{eng['true_positive']}/{eng['denominator']} -> "
        f"{forged['engine']['end_to_end']['true_positive']}/"
        f"{forged['engine']['end_to_end']['denominator']}"
    )
    print(f"[capstone]   evidence: {charged['evidence']}")

    # ---- ACCEPTANCE: every cell is READ from run output --------------------- #
    # Checked structurally (every cell names its source) AND by value: the cost
    # and ledger cells are re-derived from the ModeRun objects the replay
    # returned, so a cell that had drifted to a configured constant would differ.
    for block in ("detection", "restoration", "action_cost"):
        for mode in MODES:
            for name, entry in row[block][mode].items():
                assert entry["provenance"]["source"], (block, mode, name)
    for mode in MODES:
        totals = runs[mode].totals()
        assert row["action_cost"][mode]["steps_run"]["value"] == totals["steps_run"]
        assert row["action_cost"][mode]["publishes"]["value"] == totals["publishes"]
        assert (
            row["action_cost"][mode]["skips_by_cause"]["value"]
            == totals["skips_by_cause"]
        )

    # ---- ACCEPTANCE: parity over EVERY update, never a silent divergence ---- #
    assert len(verdicts) == _UPDATES
    assert set(row["parity"]["verdicts"]["value"]) <= {VERDICT_EQUAL, VERDICT_CERTIFIED}
    assert sum(row["parity"]["verdicts"]["value"].values()) == _UPDATES
    assert row["parity"]["silent_divergences"]["value"] == 0
    assert row["parity"]["certified_updates"]["value"], (
        "no EQUAL-with-certified-differences update: the certified arm of the "
        "verdict is untested by this history"
    )

    # ---- ACCEPTANCE: zero unsound reuses, and WHY that zero is evidence ----- #
    assert row["safety"]["unsound_reuses"]["value"] == 0
    witnesses = row["safety"]["why_the_zero_is_evidence"]["value"]
    assert len(witnesses) == 5, [w["mutation"] for w in witnesses]
    for witness in witnesses:
        assert witness["fired"], witness
        assert witness["parity_restored"] is True, (
            f"{witness['mutation']}: safety cost correctness, not just work"
        )
    print("\n[capstone] the zero is evidence — the mutations that fired:")
    for witness in witnesses:
        print(f"    {witness['mutation']:<28} update {witness['update']}: "
              f"{witness['fired']}")

    # ---- ACCEPTANCE: deferred cells, never a proxy number ------------------- #
    for name in ("remaining_at_declared_budgets", "disclosure_misses"):
        entry = row["error_cost"][name]
        assert entry["value"] == DEFERRED and entry["value"] != 0
        assert "concord-disclosure-budget" in entry["gating_surface"]
        assert f"{name:<24} {DEFERRED}" in rendered_row
    # and the UNBUDGETED figure is a real measurement, not the deferred one.
    assert not is_deferred(row["error_cost"]["remaining_unbudgeted"])

    # ---- the three-way cost comparison is real ----------------------------- #
    # In the units the ledger charges. The dollar column is asserted ONLY when the
    # corpus seals prices: the smoke corpus declares none, so its dollar totals are
    # 0.00 by construction and asserting an ordering over them would be vacuous.
    counts = {
        mode: row["action_cost"][mode]["action_counts"]["value"] for mode in MODES
    }
    billed = {mode: sum(counts[mode].values()) for mode in MODES}
    assert billed[MODE_EXACT] < billed[MODE_DEPENDENCY] < billed[MODE_COMPLETE], billed
    for mode in MODES:
        assert counts[mode]["extract_document"] <= counts[mode]["parse"], (
            "a document that delivered claims but was never parsed is impossible"
        )
    assert (
        counts[MODE_EXACT]["extract_document"]
        < counts[MODE_DEPENDENCY]["extract_document"]
        < counts[MODE_COMPLETE]["extract_document"]
    ), counts
    print(
        f"\n[capstone] priced actions billed: complete={billed[MODE_COMPLETE]} > "
        f"dependency_only={billed[MODE_DEPENDENCY]} > exact={billed[MODE_EXACT]}"
    )
    spent = {
        mode: row["action_cost"][mode]["spent_action_cost"]["value"]["total"]
        for mode in MODES
    }
    priced = row["action_cost"][MODE_COMPLETE]["spent_action_cost"]["provenance"][
        "priced_corpus"
    ]
    # The smoke corpus is THE honest-zero witness and must stay one: asserted
    # rather than branched on, so this arm cannot go quietly unexercised.
    assert priced is False, (
        "the smoke corpus now seals action prices; the unpriced-corpus marker "
        "and its refusing invariant would no longer be witnessed by any run "
        "(the PRICED corpus is presets/priced_history — see test_priced_history)"
    )
    if priced:
        assert spent[MODE_EXACT] < spent[MODE_DEPENDENCY] < spent[MODE_COMPLETE], spent
    else:
        assert set(spent.values()) == {0.0}
        assert "UNPRICED CORPUS" in rendered_row
        print(
            "[capstone] FINDING: this corpus seals no action_prices "
            f"({row['action_cost'][MODE_COMPLETE]['spent_action_cost']['provenance']['price_table']}),"
            " so every mode's dollar total is 0.00 BY CONSTRUCTION and the row "
            "states the comparison in action counts"
        )

    # ---- detection agrees across the three modes, which is the parity claim -- #
    baseline = row["detection"][MODE_COMPLETE]["recall_end_to_end"]["value"]
    for mode in MODES:
        assert row["detection"][mode]["recall_end_to_end"]["value"] == baseline, (
            f"mode {mode} reports a different end-to-end detection recall than the "
            "complete reference — the modes did not agree on the answer"
        )
        assert (
            row["restoration"][mode]["repaired"]["value"]
            == row["restoration"][MODE_COMPLETE]["repaired"]["value"]
        )

    # ---- the artifact on disk IS the row ----------------------------------- #
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded == json.loads(json.dumps(row))
    assert_row_invariants(reloaded)


# =========================================================================== #
# THE PRICED ROW — same history, sealed prices, three DIFFERENT dollar totals   #
# =========================================================================== #
@pytest.mark.postgres
def test_a_priced_corpus_fills_the_cost_cells_with_distinct_dollars(
    clean_runtime: str, tmp_path: Path, world
) -> None:
    """PACT ``concord-row-publication-ready``, ``req.row.priced``.

    The same 22-update history, generated from a preset that SEALS action prices
    (``presets/priced_history``: smoke's corpus, plus the price table the cost
    presets already seal). Three acceptances:

    * the row's dollar totals are non-zero, DIFFERENT across the three modes, and
      the exact mode is strictly cheapest — the E2 claim in money rather than in
      action counts;
    * the live-corruption count at the final update matches what the preset
      declares, so a corpus that silently stopped corrupting fails the run rather
      than producing another vacuous comparison;
    * the unpriced-corpus marker is untouched — the smoke row above still takes
      the honest-zero path, and this row takes the priced one.
    """
    psycopg = pytest.importorskip("psycopg")
    assert psycopg is not None

    from okg.bench.synth.config import load_preset
    from okg.bench.synth.controlled_fixture import (
        run_controlled_fixture,
        write_controlled_fixture,
    )
    from okg.bench.synth.corruption import Planner
    from okg.bench.synth.goldseal import build_manifest
    from okg.bench.synth.prf import SynthPrf
    from okg.bench.synth.priced_history import (
        DECLARED_SIZING,
        PRICED_PRESET,
        assert_declared_sizing,
    )
    from okg.bench.synth.render import render_corpus
    from tests.bench.synth.test_parity_replay import (
        _DEP_DIR,
        _install_composed_catalog,
        _reset_between_modes,
        _seal_recorded_history,
    )

    dsn = clean_runtime
    config = load_preset(PRICED_PRESET)
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, direct = _seal_recorded_history(
        dsn, tmp_path, world, config, _UPDATES
    )

    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    sealed_manifest = build_manifest(
        config=config, world=world, plan=plan, rendered=rendered
    )
    assert sealed_manifest["manifest_digest"] == parity.history.manifest_digest, (
        "the manifest re-sealed here is not the one the history was built from"
    )
    sealed_prices = sealed_manifest["cost_model"]["action_prices"]
    assert all(value > 0.0 for value in sealed_prices.values()), sealed_prices

    # ---- ACCEPTANCE: the declared sizing is what the history realized ------- #
    # Checked BEFORE the three modes run: an empty (or drifted) final roster is
    # a corpus failure, and there is no point paying for three replays to grade
    # nothing.
    declared = DECLARED_SIZING[PRICED_PRESET]
    realized = assert_declared_sizing(
        preset_name=PRICED_PRESET, manifest=sealed_manifest,
        history=parity.history,
    )
    assert realized == declared, realized.as_json()
    assert len(parity.gold_step(_UPDATES - 1).live_corruptions) == declared.live_at_final
    print(f"\n[priced] declared history sizing REALIZED: {realized.as_json()}")

    row, runs, verdicts = run_controlled_fixture(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_root=tmp_path,
        parity=parity, manifest=sealed_manifest, policies=policies, checks=checks,
        direct_report=direct, reset=lambda: _reset_between_modes(dsn),
    )
    rendered_row = render_controlled_fixture(row)
    print("\n" + rendered_row)
    out = write_controlled_fixture(row, tmp_path / "priced-controlled-fixture-row.json")
    (tmp_path / "priced-controlled-fixture-row.txt").write_text(
        rendered_row + "\n", encoding="utf-8"
    )
    print(f"[priced] artifact written to {out}")

    # The priced row is the E2 cost claim in dollars, and until now it landed
    # ONLY in pytest's tmp_path — so the one row a paper would quote money out of
    # was the one row a fresh checkout could not read. Recorded durably, on the
    # same hook the smoke row uses.
    _record_row(row, rendered_row, label=ROW_LABEL_PRICED, note=(
        f"the priced controlled-fixture row on preset {PRICED_PRESET}: same "
        "22-update history as the smoke row, with the sealed action price table, "
        "so the three modes' cost cells carry real dollars."
    ))

    # The row reads the same live roster the sizing check did.
    for mode in MODES:
        assert (
            row["detection"][mode]["live_corruptions"]["value"]
            == declared.live_at_final
        ), row["detection"][mode]["live_corruptions"]

    # ---- ACCEPTANCE: non-zero, DIFFERENT dollars, exact strictly cheapest --- #
    spent = {
        mode: row["action_cost"][mode]["spent_action_cost"]["value"] for mode in MODES
    }
    totals = {mode: spent[mode]["total"] for mode in MODES}
    for mode in MODES:
        provenance = row["action_cost"][mode]["spent_action_cost"]["provenance"]
        assert provenance["priced_corpus"] is True, provenance
        assert provenance["note"] is None, provenance
        assert provenance["price_table"] == sealed_prices, provenance
        assert totals[mode] > 0.0, totals
        # Every dollar is a count times the SEALED unit price, not a figure the
        # row composed for itself.
        counts = row["action_cost"][mode]["action_counts"]["value"]
        for name, entry in spent[mode]["by_class"].items():
            assert entry["count"] == counts[name], (mode, name, entry)
            assert entry["unit_price"] == sealed_prices[name], (mode, name, entry)
            assert entry["cost"] == pytest.approx(
                counts[name] * sealed_prices[name]
            ), (mode, name, entry)
    assert len(set(totals.values())) == 3, totals
    assert totals[MODE_EXACT] < totals[MODE_DEPENDENCY] < totals[MODE_COMPLETE], totals
    assert "UNPRICED CORPUS" not in rendered_row
    print(
        f"\n[priced] ACCEPTANCE — three-mode cost in DOLLARS: "
        f"complete ${totals[MODE_COMPLETE]:,.2f} > dependency_only "
        f"${totals[MODE_DEPENDENCY]:,.2f} > exact ${totals[MODE_EXACT]:,.2f} "
        f"(prices {sealed_prices}, pinned at "
        f"{row['detection'][MODE_EXACT]['recall_end_to_end']['provenance']['pin']})"
    )

    # ---- the row still passes its own contract off disk --------------------- #
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded == json.loads(json.dumps(row))
    assert_row_invariants(reloaded)

    # ---- NON-VACUITY: the priced-tie refusal FIRES on this real row --------- #
    # A row whose modes bill the same dollars is exactly the misreading the
    # unpriced note prevents on the other side, so the refusal is driven here
    # against the row this run produced rather than only against a stub.
    tied = json.loads(json.dumps(row))
    tied["action_cost"][MODE_EXACT]["spent_action_cost"]["value"] = json.loads(
        json.dumps(spent[MODE_COMPLETE])
    )
    with pytest.raises(ControlledFixtureError, match="bill the same dollar total"):
        assert_row_invariants(tied)
    print("[priced] NON-VACUITY: forcing exact to bill complete's total is REFUSED")
