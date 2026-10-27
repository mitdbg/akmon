"""Rebuild-equivalence proof alignment (issue #1539, decision 92a).

Two bench-side defects made the tinydb-lsp-enr-10 proof fail on a correct
incremental run:

* The forced rebuild ran under its own deployment name, so deployment-scoped
  values (a view node's ``canonical_asset_id``, ``<deployment>.<source>``
  ownership ids) could never match. It now shares the incremental
  experiment's deployment name while keeping its own database and files.
* The attribute fingerprint names the generation it read on each side. (It
  reads the per-generation overlay rows since PACT lane-s-storage-drop-copies
  dropped the snapshot and interval tables.)

No database: the worker pattern and the rendered deployment files are pure.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

import okg.bench.orchestrator as orchestrator


def _experiment(monkeypatch, tmp_path: Path) -> orchestrator.Experiment:
    monkeypatch.setattr(orchestrator, "BENCH_REPOS_DIR", tmp_path / "repos")
    monkeypatch.setattr(orchestrator, "GENERATED_DIR", tmp_path / "generated")
    monkeypatch.setattr(orchestrator, "RESULTS_DIR", tmp_path / "results")
    monkeypatch.setenv("OKG_BENCH_DSN_TEMPLATE", "postgresql://u:p@127.0.0.1:5545/{db}")
    return orchestrator.Experiment(
        id="tinydb-lsp-enr-10",
        repo=orchestrator.Repo(name="tinydb", url="https://example.invalid/t.git"),
        source_set="files+code+lsp",
        replay=orchestrator.Replay(strategy="head_only", last_n=10),
        config=orchestrator.BenchConfig(enrichers="code", views=True),
    )


def test_manifest_experiment_runs_under_its_own_id(monkeypatch, tmp_path) -> None:
    exp = _experiment(monkeypatch, tmp_path)

    assert exp.deployment == exp.id
    assert exp.deployments_root == tmp_path / "generated"
    assert exp.deployment_dir == tmp_path / "generated" / exp.id


def test_rebuild_shares_the_deployment_name_but_not_the_database(
    monkeypatch, tmp_path
) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    rebuild = orchestrator._rebuild_experiment(exp)

    assert rebuild.deployment == exp.deployment == "tinydb-lsp-enr-10"
    assert rebuild.id != exp.id
    assert rebuild.db_name != exp.db_name
    assert rebuild.dsn != exp.dsn
    assert rebuild.worker_log != exp.worker_log
    assert rebuild.rebuild_equivalence_json != exp.rebuild_equivalence_json
    # Its generated deployment never overwrites the incremental one.
    assert rebuild.deployment_dir != exp.deployment_dir
    assert rebuild.deployment_dir.name == exp.deployment
    assert rebuild.deployments_root == tmp_path / "generated" / ".isolated" / rebuild.id


def test_rebuild_renders_the_incremental_deployment_name(monkeypatch, tmp_path) -> None:
    exp = _experiment(monkeypatch, tmp_path)
    rebuild = orchestrator._rebuild_experiment(exp)

    orchestrator._write_deployment_files(exp)
    orchestrator._write_deployment_files(rebuild)

    for side in (exp, rebuild):
        deployment = yaml.safe_load(
            (side.deployment_dir / "deployment.yaml").read_text()
        )
        assert deployment["name"] == "tinydb-lsp-enr-10"
    # Same deployment name => the same source ownership ids on both sides, so
    # the rebuild's terminal wait reads the rebuild's own sources by the names
    # its registry declares.
    assert orchestrator._deployment_source_names(rebuild) == (
        orchestrator._deployment_source_names(exp)
    )
    assert all(
        name.startswith("tinydb-lsp-enr-10.")
        for name in orchestrator._deployment_source_names(rebuild)
    )


def test_worker_pattern_tells_the_two_sides_apart(monkeypatch, tmp_path) -> None:
    """stop_worker() pkills by pattern. With a shared deployment name, a
    name-only pattern would stop the other side's worker too."""
    exp = _experiment(monkeypatch, tmp_path)
    rebuild = orchestrator._rebuild_experiment(exp)

    def argv(side: orchestrator.Experiment) -> str:
        return (
            "/usr/bin/python -P -m okg.substrate.cli.main runtime worker "
            f"--deployment {side.deployment} --dsn {side.dsn}"
        )

    for side, other in ((exp, rebuild), (rebuild, exp)):
        pattern = re.compile(orchestrator._worker_process_pattern(side))
        assert pattern.search(argv(side))
        assert not pattern.search(argv(other))

    # A database whose name extends this one's is not this worker.
    longer = orchestrator.Experiment(
        id=f"{exp.id}0",
        repo=exp.repo,
        source_set=exp.source_set,
        replay=exp.replay,
        deployment_name=exp.deployment,
    )
    assert not re.search(orchestrator._worker_process_pattern(exp), argv(longer))


_DSN_TEMPLATES = (
    "postgresql://u:p@127.0.0.1:5545/{db}",
    "postgresql://u:p@127.0.0.1:5545/{db}?sslmode=disable&application_name=b.x",
    "host=127.0.0.1 port=5545 user=u dbname={db} sslmode=disable",
)


@pytest.mark.parametrize("template", _DSN_TEMPLATES)
def test_worker_pattern_matches_every_dsn_shape(
    monkeypatch, tmp_path, template
) -> None:
    """Review finding 1: a `[^ ]*/<db>` pattern matched nothing for a DSN with a
    query string or in key=value form, so stop_worker() silently stopped
    nothing. The pattern now matches the exact DSN literally, and still tells
    the incremental and rebuild workers apart."""
    exp = _experiment(monkeypatch, tmp_path)
    monkeypatch.setenv("OKG_BENCH_DSN_TEMPLATE", template)
    rebuild = orchestrator._rebuild_experiment(exp)

    def argv(side: orchestrator.Experiment) -> str:
        return (
            "/usr/bin/python -P -m okg.substrate.cli.main runtime worker "
            f"--deployment {side.deployment} --dsn {side.dsn}"
        )

    for side, other in ((exp, rebuild), (rebuild, exp)):
        pattern = re.compile(orchestrator._worker_process_pattern(side))
        assert pattern.search(argv(side)), (template, pattern.pattern)
        assert not pattern.search(argv(other)), (template, pattern.pattern)


@pytest.mark.skipif(shutil.which("pgrep") is None, reason="pgrep not installed")
@pytest.mark.parametrize("template", _DSN_TEMPLATES)
def test_real_pgrep_finds_the_worker_by_pattern(
    monkeypatch, tmp_path, template
) -> None:
    """The same patterns through the real `pgrep -f` (POSIX ERE), against a
    live process whose argv has the worker's shape."""
    exp = _experiment(monkeypatch, tmp_path)
    monkeypatch.setenv("OKG_BENCH_DSN_TEMPLATE", template)
    rebuild = orchestrator._rebuild_experiment(exp)
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
            "okg.substrate.cli.main",
            "runtime",
            "worker",
            "--deployment",
            exp.deployment,
            "--dsn",
            exp.dsn,
        ],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:

        def pids(pattern: str) -> set[int]:
            out = subprocess.run(
                ["pgrep", "-f", pattern], capture_output=True, text=True, check=False
            )
            return {int(p) for p in out.stdout.split()}

        deadline = time.monotonic() + 10
        while proc.pid not in pids(orchestrator._worker_process_pattern(exp)):
            assert time.monotonic() < deadline, (template, "pgrep never matched")
            time.sleep(0.1)
        assert proc.pid not in pids(orchestrator._worker_process_pattern(rebuild))
    finally:
        proc.kill()
        proc.wait()


def test_report_names_the_attribute_surface_read_on_each_side() -> None:
    """Review finding 2: the equivalence report carries, for each side,
    which generation its attribute fingerprint read."""
    graph = {
        "generation_id": "g",
        "nodes": {"count": 1, "digest": "d"},
        "edges": {"count": 1, "digest": "d"},
        "view_cites": {"count": 0, "digest": "d"},
    }
    outputs = {"count": 0, "digest": "d"}

    def attrs(gen: str) -> dict:
        return {
            "count": 3,
            "digest": "a",
            "present": True,
            "generation_id": gen,
        }

    report = orchestrator._build_rebuild_equivalence_report(
        exp=orchestrator.Experiment(
            id="x",
            repo=orchestrator.Repo(name="r", url="u"),
            source_set="files-only",
            replay=orchestrator.Replay(strategy="head_only", last_n=1),
        ),
        incremental_graph=graph,
        rebuild_graph=graph,
        incremental_outputs=outputs,
        rebuild_outputs=outputs,
        incremental_attrs=attrs("okg:inc"),
        rebuild_attrs=attrs("okg:reb"),
        delta_proof={"rows": []},
        rebuild_experiment_id="rebuild-x",
        rebuild_generation_id="okg:reb",
    )

    assert report["incremental"]["attribute_effective"]["generation_id"] == "okg:inc"
    assert report["forced_rebuild"]["attribute_effective"]["generation_id"] == "okg:reb"
