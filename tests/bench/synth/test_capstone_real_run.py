"""THE capstone: the real majority-wrong PROSE run that carries all four axes.

render_mode=prose + extractor=llm, published clean -> corrupted -> corrected across
three pinned generations against a fresh dedicated DB, scored, gated on (a)-(h),
and re-scored from the recorded artifacts alone. This is the completion gate of the
synth-benchmark-complete umbrella (task.complete.capstone).

It makes ~2 x (patients x genres) real OpenRouter calls (gen3 reuses gen2's
extracted claims, so it never re-calls the model) and runs well past ten minutes,
so it is GUARDED behind SYNTH_CAPSTONE_REAL=1 and never runs in the normal suite.
Launch it detached against a DEDICATED DB:

    SYNTH_CAPSTONE_REAL=1 \
      OKG_TEST_DSN=postgres://postgres:okg@127.0.0.1:5433/okg_test_synthcapstone \
      OKG_TEST_DSN_TRUST_AS_EPHEMERAL=1 \
      uv run pytest tests/bench/synth/test_capstone_real_run.py -s

The majority-CLEAN twin (majority_clean.yaml) and the structured control
(capstone_smoke.yaml, proven by test_capstone_compose.py) are SEPARATELY-landed
paired runs REFERENCED by this evidence, never folded into this run.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.capstone import (  # noqa: E402
    CAPSTONE_FILENAME,
    CapstoneGateError,
    run_synth_capstone,
)
from okg.bench.synth.config import ADMITTABLE_SIGNALS  # noqa: E402
from okg.bench.synth.corruption import CONSTRAINT_KINDS  # noqa: E402
from okg.bench.synth.resolution import STATE_REMOVED_FROM_SCOPE  # noqa: E402
from okg.bench.synth.runner import (  # noqa: E402
    SCORECARD_FILENAME,
    rescore_from_artifacts,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"
_OUT_DIR = _REPO_ROOT / ".scratch" / "capstone_run"


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
@pytest.mark.skipif(
    not os.environ.get("SYNTH_CAPSTONE_REAL"),
    reason="real LLM capstone run; set SYNTH_CAPSTONE_REAL=1 to run",
)
def test_capstone_prose_real_run(clean_runtime: str, tmp_path: Path) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    breach: CapstoneGateError | None = None
    try:
        result = run_synth_capstone(
            dsn=dsn, deployment_dir=_DEP_DIR, out_dir=_OUT_DIR, preset="capstone"
        )
    except CapstoneGateError as exc:
        breach = exc
    wall_s = time.monotonic() - started

    # Artifacts are written BEFORE the gate raises, so read them either way.
    report = json.loads((_OUT_DIR / CAPSTONE_FILENAME).read_text())
    core = json.loads((_OUT_DIR / SCORECARD_FILENAME).read_text())

    print("\n================ CAPSTONE PROSE REAL RUN ================")
    print(f"wall_clock_s = {wall_s:.1f}")
    print("generations:", json.dumps(report["generations"]))
    for axis, row in sorted(report["axes"].items()):
        flag = "PASS" if row["passed"] else ("----" if not row["required"] else "FAIL")
        print(f"  [{flag}] {axis}: {row['detail']}")
    print("---- funnel states ----")
    print(json.dumps(core["funnel"]["states"], indent=1))
    print("---- basis histogram ----")
    print(json.dumps(report["axes"]["b_basis_histogram"]["histogram"], indent=1))
    print("---- kind histogram ----")
    print(json.dumps(report["axes"]["c_kind_histogram"]["by_kind"], indent=1))
    print("---- corrections ----")
    print(json.dumps({k: report["corrections"].get(k) for k in (
        "count_by_action", "retracted_absent", "population_ok",
    )}, indent=1, default=str))
    print("---- (h) majority mis-repair ----")
    print(json.dumps(report["axes"]["h_majority_mis_repair"], indent=1))
    if breach is not None:
        print(f"---- CAPSTONE GATE RAISED: {breach} ----")

    # Re-score from artifacts alone reproduces the gen2 scorecard (no model, no live
    # table). This holds whether or not the gate raised.
    rescored = rescore_from_artifacts(_OUT_DIR)
    assert rescored == core, "re-score from artifacts must reproduce the scorecard"
    print("re-score == scorecard: OK")
    print("========================================================")

    # A genuine gate breach is the honest test outcome.
    if breach is not None:
        raise breach

    # THE CAPSTONE GATE (a)-(h), asserted here too so a green run is unambiguous.
    assert result["clean"]["status"] == "published"
    assert result["corrupted"]["status"] == "published"
    assert result["corrected"]["status"] == "published"
    axes = report["axes"]
    assert report["all_required_axes_passed"], axes

    # (a) funnel exhaustive
    assert sum(core["funnel"]["states"].values()) == core["funnel"]["total"]
    # (b) five decisive bases
    assert set(ADMITTABLE_SIGNALS) <= set(axes["b_basis_histogram"]["histogram"])
    # (c) four Σ kinds (loudness on the end-to-end denominator)
    for kind in CONSTRAINT_KINDS:
        assert axes["c_kind_histogram"]["by_kind"][kind]["end_to_end"]["detected"] >= 1
    # (d) a corruption left C_eff via a correction
    assert axes["d_corrections_delta"]["retracted_absent"]
    assert result["corrections"]["funnel_states"][STATE_REMOVED_FROM_SCOPE] >= 1
    # (e) prose through the LLM lane
    assert axes["e_prose_llm_lane"]["passed"]
    assert report["extractor"] == "llm" and report["render_mode"] == "prose"
    # (f) per-class survival
    assert axes["f_per_class_survival"]["by_class"]
    # (g) detection + repair conditional and end-to-end; floor met
    assert axes["g_detection_repair_rates"]["surviving_conflict_floor_passed"]
    # (h) mis_repaired > 0 over realized-majority-preserved sites at/above the floor
    assert len(axes["h_majority_mis_repair"]["mis_repaired_preserved"]) >= 1
    assert (
        len(axes["h_majority_mis_repair"]["realized_majority_preserved"])
        >= axes["h_majority_mis_repair"]["majority_wrong_floor"]
    )
