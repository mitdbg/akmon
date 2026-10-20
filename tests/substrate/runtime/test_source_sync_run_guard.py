"""Command-line source runs hold the deployment's source-sync signal (#3003).

No-database cases: the worker path keeps the guard off, the wait setting,
and which deployments probe the runtime queue. The PostgreSQL cases (two
concurrent ingests, the bounded wait, a publish from inside a sync, a killed
holder) live at the end of tests/substrate/test_publisher.py, which a
blocking per-PR database lane already owns.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest import service
from okg.substrate.runtime.source_sync_priority import (
    DEFAULT_SOURCE_SYNC_RUN_WAIT_SECONDS,
    SOURCE_SYNC_RUN_WAIT_ENV,
    ingest_run_guard,
    runtime_source_sync_probe,
    source_sync_run_wait_seconds,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def _deployment_dir(tmp_path: Path, name: str, *, runtime: bool = False) -> Path:
    directory = tmp_path / name
    directory.mkdir()
    runtime_block = "runtime:\n  enabled: true\n" if runtime else ""
    (directory / "deployment.yaml").write_text(
        f"name: {name}\n{runtime_block}", encoding="utf-8",
    )
    return directory


# --- configuration and the unchanged worker path (no database) ------------


def test_worker_source_sync_does_not_take_the_run_guard() -> None:
    """The worker path is unchanged: the DBOS source-sync workflow builds its
    ingest options with the guard turned off."""
    source = (
        REPO_ROOT / "src/okg/substrate/runtime/source_sync_workflow.py"
    ).read_text(encoding="utf-8")
    calls = [
        node for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "IngestOptions"
    ]
    assert len(calls) == 1
    flags = {
        kw.arg: kw.value for kw in calls[0].keywords
        if kw.arg == "hold_source_sync_signal"
    }
    assert isinstance(flags.get("hold_source_sync_signal"), ast.Constant)
    assert flags["hold_source_sync_signal"].value is False


def test_guard_off_opens_no_connection() -> None:
    with ingest_run_guard(
        dsn="postgresql://nobody@127.0.0.1:1/none",
        deployment="d",
        deployment_dir=None,
        hold=False,
        wait_seconds=None,
    ):
        pass


def test_command_line_default_holds_the_guard() -> None:
    options = service.IngestOptions(
        dsn="postgresql://x", deployment="d", deployment_dir=Path("."),
    )
    assert options.hold_source_sync_signal is True
    assert options.sync_wait_seconds is None


def test_runtime_probe_only_for_runtime_enabled_deployments(
    tmp_path: Path,
) -> None:
    disabled = _deployment_dir(tmp_path, "off")
    enabled = _deployment_dir(tmp_path, "on", runtime=True)
    unreadable = tmp_path / "missing"
    kwargs = {"dsn": "postgresql://x", "deployment": "d"}
    assert runtime_source_sync_probe(deployment_dir=disabled, **kwargs) is None
    assert callable(runtime_source_sync_probe(deployment_dir=enabled, **kwargs))
    # Fails closed: an unreadable manifest still checks the worker queue.
    assert callable(
        runtime_source_sync_probe(deployment_dir=unreadable, **kwargs),
    )


def test_wait_seconds_flag_env_and_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(SOURCE_SYNC_RUN_WAIT_ENV, raising=False)
    assert source_sync_run_wait_seconds() == DEFAULT_SOURCE_SYNC_RUN_WAIT_SECONDS
    assert DEFAULT_SOURCE_SYNC_RUN_WAIT_SECONDS == 600.0
    monkeypatch.setenv(SOURCE_SYNC_RUN_WAIT_ENV, "42")
    assert source_sync_run_wait_seconds() == 42.0
    assert source_sync_run_wait_seconds(7) == 7.0
    monkeypatch.setenv(SOURCE_SYNC_RUN_WAIT_ENV, "soon")
    with pytest.raises(SubstrateError, match=SOURCE_SYNC_RUN_WAIT_ENV):
        source_sync_run_wait_seconds()
    with pytest.raises(SubstrateError, match=">= 0"):
        source_sync_run_wait_seconds(-1)


def test_ingest_cli_accepts_sync_wait_seconds() -> None:
    from okg.substrate.cli.main import _build_parser

    args = _build_parser().parse_args(
        ["ingest", "--deployment", "d", "--sync-wait-seconds", "12.5"],
    )
    assert args.sync_wait_seconds == 12.5


def test_chunk_check_is_a_no_op_outside_a_run_guard() -> None:
    """The worker and standalone runners hold no guard: the per-chunk check
    must not touch the database or refuse for them."""
    from okg.substrate.runtime.source_sync_priority import ensure_run_guard_held

    ensure_run_guard_held(None)
    ensure_run_guard_held("no-guard-held-here")


def test_a_child_without_a_handoff_skips_the_check_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Only a run outside any guard (the runtime worker, serialised by its
    own queue, or a standalone run) sends no handoff; its child logs that it
    does no per-chunk check rather than skipping silently."""
    from okg.substrate.runtime.source_sync_priority import (
        adopt_run_guard,
        source_sync_run_guard_held,
    )

    with caplog.at_level("DEBUG", logger="okg.substrate.runtime.source_sync_priority"):
        adopt_run_guard(None, dsn="postgres://unused")
    assert "no source-sync run guard handed over" in caplog.text
    assert "runtime worker" in caplog.text
    assert not source_sync_run_guard_held("svc")
