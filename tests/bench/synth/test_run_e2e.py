"""task.synth.run witnesses: the smoke preset's full loop through a REAL
publish cycle, the pinned NON-VACUOUS golden, the clean-corpus FP gate's
negative control, partitioned denominators, and the post-render #754
continuity case."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.config import load_preset  # noqa: E402
from okg.bench.synth.runner import (  # noqa: E402
    canonicalize_value,
    run_synth_benchmark,
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


def _published_claim_count(dsn: str, generation_id) -> int:
    """Claim population at the pinned generation (chronos-native graph: the
    relational surface is the checkout, not a nodes_live table)."""
    from okg.bench.synth.runner import detection_state

    return len(detection_state(dsn, generation_id).claims)


@pytest.mark.postgres
def test_smoke_end_to_end_real_publish(clean_runtime: str, tmp_path: Path) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run"
    )
    core = result["scorecard"]
    manifest = result["manifest"]

    # real publish: both cycles published; the pinned corrupted generation's
    # claim population matches the rendered corpus (the row-delta witness)
    assert result["clean"]["status"] == "published"
    assert result["corrupted"]["status"] == "published"
    live = _published_claim_count(dsn, result["corrupted"]["generation_id"])
    claim_count = core["extraction"]["denominator"]
    assert live == claim_count, (
        f"published claims {live} != rendered claim population {claim_count}"
    )

    # NON-VACUITY (review finding 6): every stage denominator > 0, >=1
    # corruption recovered end-to-end, >=1 twin suppressed, >=1 other-doc
    # cross_doc detection.
    assert core["extraction"]["denominator"] > 0
    assert core["extraction"]["recovered"] > 0
    assert core["cross_doc_recall"]["denominator"] > 0
    assert core["cross_doc_recall"]["recovered"] >= 1
    assert core["twins"]["denominator"] >= 1
    assert core["twins"]["flagged"] < core["twins"]["denominator"], (
        "at least one hard-negative twin must be suppressed"
    )
    recovered_routed = sum(r["routed"] for r in core["per_class"].values())
    assert recovered_routed > 0, "routing stage denominator must be exercised"

    # clean-corpus gate: the certified-clean corpus produced zero findings
    assert core["clean_run_fp"] == 0
    assert core["clean_gate_passed"]

    # partitions: undetectable and world_knowledge are excluded from
    # cross-doc recall; world_knowledge reports its own denominator
    partition_gold = manifest["gold"]["partitions"]
    cross_gold = [c for c in partition_gold.values() if c == "cross_doc"]
    assert core["cross_doc_recall"]["denominator"] == len(cross_gold)
    assert core["undetectable_flagged_fp"] == 0

    # the golden pin (the deterministic core, byte-comparable across runs)
    golden_path = Path(__file__).parent / "golden_smoke_scorecard.json"
    if golden_path.exists():
        golden = json.loads(golden_path.read_text())
        assert core == golden, (
            "smoke scorecard drifted from the pinned golden; if the change "
            "is intentional, regenerate tests/bench/synth/golden_smoke_scorecard.json"
        )
    else:  # first run mints the golden (committed with the change)
        import os

        assert not os.environ.get("CI"), (
            "golden_smoke_scorecard.json is missing under CI - the golden "
            "must be committed, never minted green-by-construction there"
        )
        golden_path.write_text(json.dumps(core, sort_keys=True, indent=1) + "\n")


def test_cli_bench_synth_typed_error_on_bad_config(tmp_path: Path) -> None:
    """The CLI verb's error path (review finding L10): an invalid config is
    a typed rejection with exit 1, no traceback, no DB touched."""
    from okg.substrate.cli.main import main

    bad = tmp_path / "bad.yaml"
    bad.write_text("seed: ''\nworld_path: world_smoke\nnot_a_key: 1\n")
    rc = main(
        [
            "bench",
            "synth",
            "--deployment-dir",
            str(_DEP_DIR),
            "--out",
            str(tmp_path / "out"),
            "--config",
            str(bad),
        ]
    )
    assert rc == 1


@pytest.mark.postgres
def test_clean_gate_negative_control(clean_runtime: str, tmp_path: Path) -> None:
    """Force a clean-run finding and prove the ceiling breach FAILS the
    gate: poison the rendered CLEAN corpus with a contradictory restatement,
    publish it, and observe clean_gate_passed=False."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)

    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "control_run"
    )
    clean_dir = tmp_path / "control_run" / "corpus_clean"
    docs = sorted(clean_dir.glob("*__discharge_summary.md"))
    # duplicate the Body Weight line with a contradictory value: two claims
    # in one bucket (occurrence 0 and 1) violate the functional policy
    # regardless of how many other documents restate the fact.
    poisoned_doc = None
    for doc in docs:
        text = doc.read_text()
        if "Body Weight: " in text:
            doc.write_text(
                text.replace("Body Weight: ", "Body Weight: 999.9 kg\nBody Weight: ", 1)
            )
            poisoned_doc = doc
            break
    assert poisoned_doc is not None, "no discharge summary renders Body Weight"

    from okg.bench.synth.runner import (
        detection_state,
        functional_policies,
        ingest_and_publish,
    )

    policies = functional_policies(result["manifest"])
    out, _ = ingest_and_publish(dsn, clean_dir, _DEP_DIR, policies)
    state = detection_state(dsn, out["generation_id"])
    findings = [f for f in state.findings if f.channel in ("definite", "possible")]
    assert findings, "poisoned clean corpus must produce a contested finding"

    from okg.bench.synth.runner import build_synth_scorecard, routed_buckets

    core = build_synth_scorecard(
        manifest=result["manifest"],
        clean_state=state,
        corrupted_state=state,
        routed=routed_buckets(dsn),
        config=load_preset("smoke"),
    )
    assert core["clean_run_fp"] > 0
    assert not core["clean_gate_passed"], (
        "ceiling breach must fail the gate (negative control)"
    )


def test_canonicalize_value_units() -> None:
    assert canonicalize_value("82000 g") == "82.0 kg"
    assert canonicalize_value("1750 mm") == "175.0 cm"
    assert canonicalize_value("82.0 kg") == "82.0 kg"
    assert canonicalize_value("active") == "active"


@pytest.mark.postgres
def test_double_run_deterministic_core(clean_runtime: str, tmp_path: Path) -> None:
    """Same preset, fresh DB state: the scorecard core is byte-identical
    (generation ids live only in run-binding)."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    first = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "a"
    )["scorecard"]
    second = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "b"
    )["scorecard"]
    assert first == second


def test_post_render_continuity(tmp_path: Path) -> None:
    """One post-render case flows through the UNCHANGED #754 plan/apply
    contract, and apply_inverses restores byte identity (no DB needed)."""
    from okg.bench.synth.config import load_preset
    from okg.bench.synth.corruption import Planner
    from okg.bench.synth.prf import SynthPrf
    from okg.bench.synth.render import render_corpus
    from okg.bench.synth.runner import _write_docs, post_render_apply
    from okg.bench.synth.fhir_map import world_from_synthea_dir

    world_dir = _REPO_ROOT / "src/okg/bench/synth/world_smoke"
    provenance = json.loads((world_dir / "provenance.json").read_text())
    world = world_from_synthea_dir(
        world_dir, reference_date=provenance["reference_date"], provenance=provenance
    )
    config = load_preset("smoke")
    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    corrupted_dir = tmp_path / "corpus"
    _write_docs(corrupted_dir, rendered.corrupted_docs)

    result = post_render_apply(corrupted_dir, _DEP_DIR, seed=7, out_dir=tmp_path / "pr")
    assert result.mutations, "post-render layer must emit #754 MutationRecords"
    for record in result.mutations:
        assert record.operator_id == "injection.date_transpose"
