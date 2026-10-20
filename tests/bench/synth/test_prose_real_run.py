"""The real prose capstone: render_mode=prose + extractor=llm, published clean
and corrupted against a fresh DB, scored, floor-asserted, and re-scored from the
recorded artifacts alone.

This makes ~2 x (patients x genres) real OpenRouter calls and can run well past
ten minutes, so it is GUARDED behind SYNTH_PROSE_REAL=1 and never runs in the
normal suite. Launch it in the background:

    SYNTH_PROSE_REAL=1 OKG_TEST_DSN=...okg_test_synthwire \
      uv run pytest tests/bench/synth/test_prose_real_run.py -s
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.runner import (  # noqa: E402
    RUN_BINDING_FILENAME,
    SCORECARD_FILENAME,
    RunnerError,
    render_synth_scorecard,
    rescore_from_artifacts,
    run_synth_benchmark,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"
_OUT_DIR = _REPO_ROOT / ".scratch" / "prose_run"


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
    not os.environ.get("SYNTH_PROSE_REAL"),
    reason="real LLM prose run; set SYNTH_PROSE_REAL=1 to run",
)
def test_prose_llm_real_run(clean_runtime: str, tmp_path: Path) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    breach: RunnerError | None = None
    try:
        result = run_synth_benchmark(
            dsn=dsn, deployment_dir=_DEP_DIR, out_dir=_OUT_DIR, preset="prose_llm"
        )
    except RunnerError as exc:
        breach = exc
    wall_s = time.monotonic() - started

    # Artifacts are written BEFORE any gate raises, so read them either way.
    core = json.loads((_OUT_DIR / SCORECARD_FILENAME).read_text())
    binding = json.loads((_OUT_DIR / RUN_BINDING_FILENAME).read_text())

    print("\n================ PROSE REAL RUN ================")
    print(f"wall_clock_s = {wall_s:.1f}")
    print(render_synth_scorecard(core), end="")
    print("---- run binding ----")
    print(json.dumps({k: binding[k] for k in (
        "extractor", "surviving_conflict_floor", "surviving_conflict_realized",
        "surviving_conflict_floor_passed", "clean_generation_id",
        "corrupted_generation_id",
    )}, indent=1))
    print("---- extraction cost ----")
    print(json.dumps(binding["extraction_cost"], indent=1))
    print("---- extraction failures ----")
    print(json.dumps(binding["extraction_failed_documents"], indent=1))
    print("---- funnel states ----")
    print(json.dumps(core["funnel"], indent=1))
    if breach is not None:
        print(f"---- FLOOR/CLEAN GATE RAISED: {breach} ----")

    # Re-score from artifacts alone reproduces the scorecard (no model, no live
    # table). This holds whether or not the floor gate raised.
    rescored = rescore_from_artifacts(_OUT_DIR)
    assert rescored == core, "re-score from artifacts must reproduce the scorecard"
    print("re-score == scorecard: OK")
    print("===============================================")

    # Surface a genuine floor/clean breach as the honest test outcome.
    if breach is not None:
        raise breach

    assert result["clean"]["status"] == "published"
    assert result["corrupted"]["status"] == "published"
    assert binding["surviving_conflict_floor_passed"]
