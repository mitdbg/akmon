from __future__ import annotations

from pathlib import Path


README = Path(__file__).resolve().parents[3] / "bench" / "README.md"


def test_readme_documents_sensitivity_config_and_commands():
    text = README.read_text(encoding="utf-8")

    assert "bench/engine_sensitivity.yaml" in text
    assert "python bench/orchestrator.py sensitivity" in text
    assert "python bench/orchestrator.py sensitivity --json" in text
    assert "python bench/orchestrator.py sensitivity --run" in text
    assert "--run-branching-report" in text


def test_readme_documents_workload_and_engine_axes():
    text = README.read_text(encoding="utf-8")

    for token in (
        "one_factor_at_a_time",
        "mode: cartesian",
        "axes.workload",
        "source_set",
        "replay",
        "axes.engine",
        "enrichers",
        "views",
        "chronos_continuation_pct",
        "embedding",
        "unavailable_axes",
        "projection_batch",
        "reports.branching",
    ):
        assert token in text
