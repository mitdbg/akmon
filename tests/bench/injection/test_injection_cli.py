"""`okg bench inject` witnesses (PACT ship-okg-bench, task.ship.cli).

- the verb registers on the standard CLI surface (`--help` smoke via the
  real console entry point);
- the rendered scorecard is pinned against a fixed deterministic core
  (golden text — raw denominators, no volatile fields);
- the live handler run publishes the clean + mutated generations, writes
  the sealed artifacts, prints numbers equal to scorecard-core.json, and a
  repeat invocation is byte-identical over the deterministic surface;
- the fail-closed path holds: the scorer (and therefore the verb) refuses
  without a leakage pass — pinned in test_injection_leakage.py; here we pin
  the renderer reads the core only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pytest

from okg.bench.injection import render_scorecard

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "consistency-injection-fixture"

_GOLDEN_CORE = {
    "gold_count": 3,
    "stages": {
        "extraction": {"denominator": 2, "recovered": 2, "missed": []},
        "candidate": {"denominator": 1, "recovered": 1, "missed": []},
        "verdict": {"denominator": 1, "recovered": 0, "missed": ["okgmut_x"]},
        "routing": {"denominator": 0, "recovered": 0, "missed": []},
    },
    "modes": {
        "graph": {"denominator": 2, "recovered": 1, "missed": ["okgmut_x"]},
        "independent": {"denominator": 1, "recovered": 1, "missed": []},
    },
    "tiers": {"t1_literal": {"denominator": 3, "recovered": 2, "missed": ["okgmut_x"]}},
    "hard_negatives": {
        "denominator": 1,
        "recovered": 0,
        "missed": ["okgmut_y"],
        "fp_by_mechanism": {"value_compatibility": ["conflict:functional_u:abc"]},
    },
    "false_positives": {"count": 0, "finding_ids": [], "baseline_excluded": 1},
    "end_to_end": {
        "graph_rate": 0.5,
        "independent_rate": 1.0,
        "visibility_gap": -0.5,
    },
    "families": {"okgfam_a": {"members": 3, "recovered": 2, "matched_finding_ids": []}},
}

_GOLDEN_TEXT = """injection benchmark scorecard  (gold=3)

stage chain (each conditioned on the previous stage's survivors):
  extraction  2/2
  candidate   1/1
  verdict     0/1  missed: okgmut_x
  routing     0/0

end-to-end by targeting mode:
  graph        1/2
  independent  1/1
  visibility gap: -0.500

recall by hardness tier:
  t1_literal         2/3

hard negatives: 0/1 correct (era-conditional)
  leak via value_compatibility: conflict:functional_u:abc
false positives: 0 (baseline findings excluded: 1)

incident families: 1 (members recovering as units; matched-finding counts expose over-pairing)
"""


def test_rendered_scorecard_is_golden():
    assert render_scorecard(_GOLDEN_CORE) == _GOLDEN_TEXT


def test_bench_inject_registers_on_the_cli_surface():
    out = subprocess.run(
        ["uv", "run", "okg", "bench", "inject", "--help"],
        capture_output=True, text=True, cwd=_REPO_ROOT, timeout=180,
    )
    assert out.returncode == 0, out.stderr[-1500:]
    for flag in ("--deployment-dir", "--corpus", "--out", "--work-dir", "--seed"):
        assert flag in out.stdout


@pytest.mark.postgres
def test_bench_inject_handler_runs_and_repeats_byte_identical(
    clean_runtime: str, tmp_path: Path, capsys
) -> None:
    from okg.substrate.cli.main import _cmd_bench
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog

    apply_explicit_test_catalog(clean_runtime, _DEP_DIR)

    def run(tag: str) -> Path:
        out_dir = tmp_path / tag
        args = argparse.Namespace(
            bench_cmd="inject",
            deployment_dir=str(_DEP_DIR),
            corpus=None,
            out=str(out_dir),
            work_dir=str(tmp_path / f"{tag}-working"),
            seed=101,
            dsn=clean_runtime,
        )
        assert _cmd_bench(args, as_json=False) == 0
        return out_dir

    out1 = run("run1")
    rendered = capsys.readouterr().out
    core = json.loads((out1 / "scorecard-core.json").read_text(encoding="utf-8"))
    # The rendering equals the deterministic core's numbers, and only them.
    assert f"(gold={core['gold_count']})" in rendered
    extraction = core["stages"]["extraction"]
    assert f"extraction  {extraction['recovered']}/{extraction['denominator']}" in rendered
    assert "run-binding.json" in rendered
    assert (out1 / "run-binding.json").is_file()
    assert (out1 / "leakage-check.json").is_file()

    out2 = run("run2")
    for name in ("mutations.jsonl", "benchmark-manifest.json", "scorecard-core.json"):
        assert (out1 / name).read_bytes() == (out2 / name).read_bytes(), name
