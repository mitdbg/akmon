"""Guard regression tests for destructive `okg` CLI verbs (#682, #694).

Convention point (d) (destructive verbs default to dry-run and require an
explicit guard flag) and point (g) (strict config schemas: unknown/malformed
input is a load-time error, not a benign default) from the
``remediate-audit-2026-07`` design.

Each test is written to FAIL on the pre-fix code:

- ``runtime reset`` passed a hardcoded ``environment="development"`` so the
  ``DevResetRefused`` guard never saw a manifest ``runtime.environment:
  production`` (#682). ``RuntimeConfig`` had no ``environment`` field and
  silently dropped an unknown ``runtime:`` key.
- ``nomos decisions-prune`` DELETE-d by default (dry-run was opt-in) (#694).
- ``branch compact`` neither refused the default branch nor dry-ran (#694).
- ``db drop`` ignored its declared ``--apply`` so ``--confirm`` alone dropped
  (#694).
- ``ingest --owner-meta`` silently substituted ``{}`` on invalid JSON (#694).

The tests drive the real argument parsers (so defaults are authentic) or the
handlers directly with the destructive backend spied/blocked, so no Postgres
fixture is required.
"""
from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.cli import branch as branch_cli
from okg.substrate.cli import main as cli_main
from okg.substrate.cli import nomos as nomos_cli
from okg.substrate.cli import runtime as runtime_cli
from okg.substrate.cli.parser_groups import register_branch_parser
from okg.substrate.db import management as db_management
from okg.substrate.runtime.config import RuntimeConfig
from okg.substrate.runtime.dev import DevResetResult, reset_dbos


def _add_json_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")


# ---------------------------------------------------------------------------
# #682 — runtime reset resolves environment from the manifest
# ---------------------------------------------------------------------------


def test_runtime_config_carries_environment_field() -> None:
    """A `runtime.environment` marker is parsed and surfaced (was dropped)."""
    cfg = RuntimeConfig.from_manifest_block({"environment": "production"})
    assert cfg.environment == "production"
    assert cfg.as_dict()["environment"] == "production"
    # Absent -> defaults to development so the local dev loop still works.
    assert RuntimeConfig.from_manifest_block(None).environment == "development"
    assert RuntimeConfig.from_manifest_block({}).environment == "development"


def test_runtime_block_rejects_unknown_key() -> None:
    """A typo'd runtime key is a load-time error, not a silent no-op (g)."""
    with pytest.raises(ValueError) as excinfo:
        RuntimeConfig.from_manifest_block({"enviroment": "production"})
    assert "enviroment" in str(excinfo.value)


def test_runtime_reset_passes_manifest_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reset handler forwards `manifest.runtime.environment`, not a
    hardcoded 'development' (#682)."""
    captured: dict[str, Any] = {}

    def _spy_reset(**kwargs: Any) -> DevResetResult:
        captured.update(kwargs)
        return DevResetResult(
            deployment=kwargs["deployment"], cleared=True, schema_name="dbos"
        )

    manifest = SimpleNamespace(
        runtime=SimpleNamespace(enabled=True, environment="production")
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda name: manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.dev.reset_dbos", _spy_reset
    )

    args = argparse.Namespace(
        rt_subcmd="reset",
        deployment="fake-deployment",
        dsn="postgresql://ignored/x",
        confirm=True,
    )
    rc = runtime_cli._cmd_runtime(args, as_json=True)

    assert rc == 0
    assert captured.get("environment") == "production", (
        "reset handler must forward the manifest environment; a hardcoded "
        "'development' would let a production reset through"
    )


def test_reset_dbos_refuses_non_development_environment() -> None:
    """The underlying guard raises for a production environment."""
    from okg.substrate.runtime.dev import DevResetRefused

    with pytest.raises(DevResetRefused):
        reset_dbos(
            deployment="prod",
            dsn="postgresql://ignored/x",
            runtime=RuntimeConfig(enabled=True, environment="production"),
            confirm=True,
            environment="production",
        )


# ---------------------------------------------------------------------------
# #694 — nomos decisions-prune dry-runs by default
# ---------------------------------------------------------------------------


class _FakeConn:
    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def commit(self) -> None:
        return None


def _build_nomos_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    nomos_cli.register_subparsers(sub, add_json_flag=_add_json_flag)
    return parser.parse_args(argv)


def test_decisions_prune_dry_runs_without_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No --apply => prune is invoked with dry_run=True and deletes nothing."""
    seen: dict[str, Any] = {}

    def _spy_prune(conn: object, **kwargs: Any) -> int:
        seen.update(kwargs)
        return 0

    monkeypatch.setattr(
        "okg.substrate.nomos.retention.prune_nomos_decisions", _spy_prune
    )
    monkeypatch.setattr(
        "psycopg.connect", lambda *a, **k: _FakeConn()
    )

    args = _build_nomos_args(
        ["nomos", "decisions-prune", "--dsn", "postgresql://ignored/x"]
    )
    rc = nomos_cli.cmd_nomos(args, as_json=True)

    assert rc == 0
    assert seen.get("dry_run") is True, (
        "decisions-prune must default to dry-run; pre-fix it DELETE-d unless "
        "--dry-run was passed (#694)"
    )


def test_decisions_prune_deletes_only_with_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--apply flips dry_run off so the DELETE actually runs."""
    seen: dict[str, Any] = {}

    def _spy_prune(conn: object, **kwargs: Any) -> int:
        seen.update(kwargs)
        return 3

    monkeypatch.setattr(
        "okg.substrate.nomos.retention.prune_nomos_decisions", _spy_prune
    )
    monkeypatch.setattr(
        "psycopg.connect", lambda *a, **k: _FakeConn()
    )

    args = _build_nomos_args(
        ["nomos", "decisions-prune", "--dsn", "postgresql://ignored/x",
         "--apply"]
    )
    rc = nomos_cli.cmd_nomos(args, as_json=True)

    assert rc == 0
    assert seen.get("dry_run") is False


# ---------------------------------------------------------------------------
# #694 — branch compact refuses default + dry-runs
# ---------------------------------------------------------------------------


def _build_branch_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    register_branch_parser(sub, add_json_flag=_add_json_flag)
    return parser.parse_args(argv)


def _no_db(*_a: object, **_k: object) -> None:
    raise AssertionError("compact must not open a DB connection in this path")


def test_branch_compact_refuses_default_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgresql://ignored/x",
    )
    monkeypatch.setattr("psycopg.connect", _no_db)

    args = _build_branch_args(
        ["branch", "compact", "default", "--deployment", "d", "--confirm"]
    )
    rc = branch_cli._cmd_branch(args, as_json=True)
    assert rc == 1, "compacting the default branch must be refused (#694)"


def test_branch_compact_dry_runs_without_confirm(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgresql://ignored/x",
    )
    monkeypatch.setattr("psycopg.connect", _no_db)

    args = _build_branch_args(
        ["branch", "compact", "experiment-x", "--deployment", "d"]
    )
    rc = branch_cli._cmd_branch(args, as_json=True)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload.get("dry_run") is True, (
        "compact must dry-run without --confirm; pre-fix it rebased "
        "immediately (#694)"
    )
    assert payload.get("applied") is False


def test_branch_delete_refuses_protected_branches_without_touching_the_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg branch delete main|default` is refused before any connection (#1306)."""
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgresql://ignored/x",
    )
    monkeypatch.setattr(
        "psycopg.connect",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("delete must refuse protected branches before connecting")
        ),
    )
    for protected in ("main", "default"):
        args = _build_branch_args(
            ["branch", "delete", protected, "--deployment", "d", "--confirm"]
        )
        assert branch_cli._cmd_branch(args, as_json=True) == 1


def test_branch_rollover_dry_runs_without_confirm(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgresql://ignored/x",
    )
    monkeypatch.setattr("psycopg.connect", _no_db)

    args = _build_branch_args(
        ["branch", "rollover", "default", "--deployment", "d"]
    )
    rc = branch_cli._cmd_branch(args, as_json=True)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["applied"] is False
    assert payload["history_policy"] == "preserve_all_generation_checkpoints"


def test_branch_rollover_holds_publisher_mutex_through_commit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    events: list[str] = []
    catalog_identity = object()

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def commit(self) -> None:
            events.append("commit")

        def rollback(self) -> None:
            events.append("rollback")

    class Store:
        context = SimpleNamespace(
            finalize_external_transaction=lambda *, committed: events.append(
                f"finalize:{committed}"
            )
        )

        def rollover_branch_from_generation(self, *args: object, **kwargs: object):
            del args, kwargs
            assert events[-1] == "lock:enter"
            events.append("rollover")
            return {"new_generation_id": "generation:new"}

    class Guard:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args
            assert kwargs["catalog_identity"] is catalog_identity
            self.probe = kwargs["source_sync_priority_probe"]

        @contextmanager
        def _publish_lock_on_connection(
            self, conn: object, *, mode: str, wait_seconds: float
        ):
            del conn
            assert mode == "dbos"
            assert wait_seconds == 0.0
            assert self.probe() is None
            events.append("lock:enter")
            try:
                yield
            finally:
                events.append("lock:exit")

    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgresql://ignored/x",
    )
    monkeypatch.setattr("psycopg.connect", lambda *a, **k: Conn())
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.Publisher",
        Guard,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack.build_runtime_catalog_identity",
        lambda **_kwargs: catalog_identity,
    )
    monkeypatch.setattr(
        "okg.substrate.graph_store.ChronosGraphStore.from_connection",
        lambda *a, **k: Store(),
    )
    monkeypatch.setattr(
        branch_cli,
        "_active_source_sync_workflow_id_for_rollover",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: SimpleNamespace(deployment_dir=tmp_path),
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.tracking_repository_provenance",
        lambda _path: SimpleNamespace(status="exact"),
    )

    args = _build_branch_args([
        "branch",
        "rollover",
        "default",
        "--deployment",
        "d",
        "--generation-id",
        "generation:old",
        "--confirm",
    ])
    assert branch_cli._cmd_branch(args, as_json=True) == 0
    assert events == [
        "lock:enter",
        "rollover",
        "commit",
        "finalize:True",
        "lock:exit",
    ]
    assert json.loads(capsys.readouterr().out)["applied"] is True


def test_branch_rollover_refuses_active_source_sync_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from okg.substrate.errors import PublisherLeaseHeld

    catalog_identity = object()

    class Guard:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args
            assert kwargs["catalog_identity"] is catalog_identity
            self.probe = kwargs["source_sync_priority_probe"]

        @contextmanager
        def _publish_lock_on_connection(self, *args: object, **kwargs: object):
            del args, kwargs
            workflow_id = self.probe()
            assert workflow_id == "source-sync-active"
            raise PublisherLeaseHeld(
                "source-sync publish work is active",
                data={"blocking_workflow_id": workflow_id},
            )
            yield

    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn",
        lambda args: "postgresql://ignored/x",
    )
    monkeypatch.setattr("psycopg.connect", lambda *a, **k: _FakeConn())
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.Publisher",
        Guard,
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.deployment_stack.build_runtime_catalog_identity",
        lambda **_kwargs: catalog_identity,
    )
    monkeypatch.setattr(
        "okg.substrate.graph_store.ChronosGraphStore.from_connection",
        lambda *a, **k: pytest.fail("rollover mutated while source sync was active"),
    )
    monkeypatch.setattr(
        branch_cli,
        "_active_source_sync_workflow_id_for_rollover",
        lambda **kwargs: "source-sync-active",
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _name: SimpleNamespace(deployment_dir=tmp_path),
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.tracking_repository_provenance",
        lambda _path: SimpleNamespace(status="exact"),
    )

    args = _build_branch_args([
        "branch",
        "rollover",
        "default",
        "--deployment",
        "d",
        "--generation-id",
        "generation:old",
        "--confirm",
    ])
    assert branch_cli._cmd_branch(args, as_json=True) == 1


# ---------------------------------------------------------------------------
# #694 — db drop honors --apply (confirm alone no longer drops)
# ---------------------------------------------------------------------------


def test_db_drop_handler_forwards_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The db-drop handler passes --apply through; pre-fix it dropped a
    call site into drop_database that ignored --apply and dropped on
    --confirm alone (#694)."""
    seen: dict[str, Any] = {}

    def _spy_drop(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return {"dry_run": True, "applied": False}

    monkeypatch.setattr(db_management, "drop_database", _spy_drop)

    args = argparse.Namespace(
        db_subcmd="drop",
        name="scratch_guard_test_db",
        cluster_dsn="postgresql://ignored/x",
        confirm=True,
        apply=False,
        override_deployment=None,
        json=True,
    )
    rc = cli_main._cmd_db(args, as_json=True)
    assert rc == 0
    assert "apply" in seen, (
        "db drop handler must forward apply; pre-fix it never passed it, so "
        "--confirm alone executed the DROP (#694)"
    )
    assert seen["apply"] is False


def test_drop_database_dry_runs_without_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """drop_database is a dry-run without apply and never opens a connection."""
    def _boom(*_a: object, **_k: object) -> None:
        raise AssertionError("drop_database dry-run must not connect")

    monkeypatch.setattr(db_management.psycopg, "connect", _boom)

    result = db_management.drop_database(
        cluster_dsn="postgresql://ignored/x",
        database="scratch_guard_test_db_zzz",
        confirm=True,
        apply=False,
    )
    assert result["dry_run"] is True
    assert result["applied"] is False


# ---------------------------------------------------------------------------
# #694 — ingest --owner-meta raises on invalid JSON (no silent {})
# ---------------------------------------------------------------------------


def test_ingest_owner_meta_invalid_json_is_a_loud_error() -> None:
    """The ingest handler must not swallow invalid --owner-meta into {}.

    The `okg ingest` path silently substituted `{}` on a JSONDecodeError,
    discarding operator provenance and reporting success (#694). Reaching
    that parse end-to-end needs a fully-resolved deployment, so this pins the
    handler-source contract: the benign-default pattern is gone and the
    typed error message is present.
    """
    with open(cli_main.__file__, encoding="utf-8") as fh:
        text = fh.read()
    assert "owner_meta = {}" not in text, (
        "ingest --owner-meta must not silently substitute {} on invalid "
        "JSON (#694)"
    )
    assert "--owner-meta must be valid JSON" in text, (
        "the fixed ingest handler should emit a typed error for malformed "
        "--owner-meta"
    )
