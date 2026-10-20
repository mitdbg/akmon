"""Fixtures for the PACT friction benchmark lane.

The lane APPLIES the harness isolation contract (it removes the env the
harness refuses to run under) so the numbers are the same on a developer
machine with a live DSN exported and on a clean CI runner. The refusal itself
is exercised by ``test_isolation_refuses_unexpected_env``.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from okg.substrate.pact.bench import scrub_environment
from okg.substrate.pact.bench.runner import BenchResult, repo_root_from, run_benchmark

BASELINE = Path(__file__).parent / "baseline" / "metrics.json"


@pytest.fixture(scope="session", autouse=True)
def bench_env() -> None:
    saved = dict(os.environ)
    scrub_environment()
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


@pytest.fixture(scope="session")
def bench_repo_root() -> Path | None:
    return repo_root_from()


@pytest.fixture(scope="session")
def bench_result(
    bench_repo_root: Path | None,
    tmp_path_factory: pytest.TempPathFactory,
) -> BenchResult:
    # An explicit workdir, so the fixture repo survives the session and the
    # executor tests can copy it.
    workdir = tmp_path_factory.mktemp("pact-bench")
    return run_benchmark(workdir=workdir, repo_root=bench_repo_root)


@pytest.fixture(scope="session")
def metrics(bench_result: BenchResult, bench_repo_root: Path | None) -> dict:
    if bench_repo_root is not None:
        # The run emits its machine-readable report and human table as
        # inspectable artifacts. `.scratch/` is gitignored, so this pollutes
        # nothing; failure to write is reported, never swallowed as success.
        artifacts = bench_repo_root / ".scratch" / "pact-bench"
        try:
            artifacts.mkdir(parents=True, exist_ok=True)
            (artifacts / "metrics.json").write_text(
                bench_result.to_json(), encoding="utf-8"
            )
            (artifacts / "report.txt").write_text(
                bench_result.to_table(), encoding="utf-8"
            )
        except OSError as exc:  # pragma: no cover - environment-dependent
            pytest.fail(f"could not emit benchmark artifacts: {exc}")
    return bench_result.metrics
