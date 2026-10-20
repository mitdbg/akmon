"""Focused coverage for large CLI command functions.

The command implementations in ``okg.substrate.cli.main`` mostly glue
argparse, dependency resolution, and JSON envelopes around behavior that
has deeper tests elsewhere. These tests keep that boundary explicit:
replace external services with small fakes and assert the dispatcher
returns the expected envelope shape.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli import catalog as cli_catalog
from okg.substrate.errors import SubstrateError


def _args(**kwargs):
    kwargs.setdefault("apply", True)
    return SimpleNamespace(**kwargs)


def _captured_error(capsys) -> dict:
    captured = capsys.readouterr()
    raw = (captured.err or captured.out).strip().splitlines()[-1]
    return json.loads(raw)


def _write_minimal_registry(root) -> None:
    (root / "source_registry.yaml").write_text(
        """
sources:
  pact:
    ownership_id: okg-workspace.pact
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [repo, change_id]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
""".lstrip(),
        encoding="utf-8",
    )



def _stub_database_readiness(monkeypatch) -> None:
    """Stub the readiness precondition these mocked happy paths now have.

    `_cmd_run` and `_cmd_mcp_serve` answer "is this database ready" before
    any graph work (issue #2128). That is a real collaborator, and these
    tests drive the commands against the fake DSN `postgres://test`, so it
    is stubbed to its success answer here alongside the other seams. The
    stub returns a real `DatabaseReadiness` rather than None so a caller
    that reads the result keeps working.
    """
    import okg.substrate.db.readiness as readiness_mod

    monkeypatch.setattr(
        readiness_mod,
        "require_database_ready",
        lambda dsn, **kwargs: readiness_mod.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        ),
    )
    # `okg mcp-serve` also proves its role credentials before it starts
    # (issue #3273): a remote host needs OKG_MCP_RO_DSN, a local one a
    # provisioned password. Another real collaborator, stubbed to success
    # for the fake DSN; its refusals are tested in test_local_database_roles.
    import okg.substrate.local_database_roles as roles_mod

    monkeypatch.setattr(
        roles_mod, "require_role_dsn_usable", lambda dsn, **kwargs: None,
    )

def test_deployment_runtime_enabled_fails_loudly_on_resolution_error(
    monkeypatch,
) -> None:
    import okg.substrate.deployments as deployments

    def _raise_resolution_error(_deployment):
        raise ValueError("bad runtime block")

    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        _raise_resolution_error,
    )

    with pytest.raises(SubstrateError) as exc:
        cli._deployment_runtime_enabled("broken")

    assert exc.value.code == "deployment_runtime_config_unresolved"
    assert exc.value.data["deployment"] == "broken"
    assert "bad runtime block" in exc.value.data["error"]


def test_cmd_run_surfaces_runtime_resolution_error(monkeypatch, capsys) -> None:
    err = SubstrateError("could not resolve deployment runtime configuration")
    err.code = "deployment_runtime_config_unresolved"

    def _raise_runtime_error(_args):
        raise err

    monkeypatch.setattr(cli, "_resolve_run_context", _raise_runtime_error)

    rc = cli._cmd_run(
        _args(once=True, deployment="broken", inline=False),
        as_json=True,
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["code"] == "deployment_runtime_config_unresolved"


def test_cmd_ingest_surfaces_runtime_resolution_error(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    import okg.substrate.deployments as deployments

    err = SubstrateError("could not resolve deployment runtime configuration")
    err.code = "deployment_runtime_config_unresolved"
    _write_minimal_registry(tmp_path)
    monkeypatch.setattr(cli, "_resolve_dsn", lambda args: "postgres://test")
    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda _deployment: tmp_path,
    )
    monkeypatch.setattr(deployments, "inject_deployment_path", lambda _path: None)

    def _raise_runtime_error(_deployment):
        raise err

    monkeypatch.setattr(cli, "_deployment_runtime_enabled", _raise_runtime_error)

    rc = cli._cmd_ingest(
        _args(deployment="broken", inline=False, progress=False),
        as_json=True,
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["code"] == "deployment_runtime_config_unresolved"


def test_ingest_publish_scope_classifies_event_append_sources() -> None:
    event_source = SimpleNamespace(
        source_class="append_feed",
        sync=SimpleNamespace(watch=None),
        admission_policy=SimpleNamespace(
            authority_scope={
                "source_family": "okg_workspace_pact_coordination_events",
            },
        ),
    )
    watched_append_source = SimpleNamespace(
        source_class="append_feed",
        sync=SimpleNamespace(watch=object()),
        admission_policy=SimpleNamespace(authority_scope={}),
    )
    path_append_source = SimpleNamespace(
        source_class="append_feed",
        sync=SimpleNamespace(watch=None),
        admission_policy=SimpleNamespace(
            authority_scope={"path_globs": ["pact/**/*"]},
        ),
    )
    discovery_source = SimpleNamespace(
        source_class="discovery_crawl",
        sync=SimpleNamespace(watch=None),
        admission_policy=SimpleNamespace(authority_scope={}),
    )

    assert cli._source_entry_requires_sync_root_for_publish(event_source) is False
    assert (
        cli._source_entry_requires_sync_root_for_publish(watched_append_source)
        is True
    )
    assert (
        cli._source_entry_requires_sync_root_for_publish(path_append_source)
        is True
    )
    assert cli._source_entry_requires_sync_root_for_publish(discovery_source) is True
    assert cli._ingest_plan_is_file_derived({"event": event_source}, ["event"]) is False
    assert cli._ingest_plan_is_file_derived(
        {"event": event_source, "pact": discovery_source},
        ["event", "pact"],
    ) is True


def test_error_emitters_agent_mode_and_env_interpolation(
    monkeypatch, capsys,
) -> None:
    err = SubstrateError(
        "extension missing",
        data={"extension": "vector"},
        fix_hint="install vector",
    )
    err.code = "extension_missing"

    monkeypatch.setenv("OKG_AGENT", "1")
    monkeypatch.setenv("OKG_TEST_INTERP", "resolved")

    assert cli._is_agent_mode() is True
    assert cli._json_error(err) == 2
    assert cli._text_error(err) == 2
    cli._emit_ok({"ok": True, "count": 2}, as_json=False)
    assert (
        cli._interpolate_env("${OKG_TEST_INTERP}/${UNSET_OKG_VAR}")
        == "resolved/${UNSET_OKG_VAR}"
    )

    out = capsys.readouterr()
    assert '"error"' in out.err
    assert "fix_hint: install vector" in out.err
    assert "ok: True" in out.out


def test_json_error_redacts_and_serializes_nested_authority_diagnostics(
    capsys,
) -> None:
    class Stage(Enum):
        WORKER_RELOAD = "worker_reload"

    err = SubstrateError(
        "replacement worker acknowledgment timed out",
        data={
            "stage": Stage.WORKER_RELOAD,
            "observed_at": datetime(2026, 7, 27, 3, 0, tzinfo=timezone.utc),
            "deployment_dir": Path("/tmp/okg-workspace"),
            "nested": {
                "dsn": "postgres://operator:top-secret@db/okg_workspace",
                "attempts": {1, 2},
            },
        },
        fix_hint="restart the canonical worker and retry deployment apply",
    )
    err.code = "worker_ack_timeout"

    assert cli._json_error(err) == 1

    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["code"] == "worker_ack_timeout"
    assert payload["error"]["message"] == (
        "replacement worker acknowledgment timed out"
    )
    assert payload["error"]["fix_hint"].startswith("restart the canonical")
    assert payload["error"]["data"]["stage"] == "worker_reload"
    assert payload["error"]["data"]["observed_at"] == (
        "2026-07-27T03:00:00+00:00"
    )
    assert payload["error"]["data"]["deployment_dir"] == (
        "/tmp/okg-workspace"
    )
    assert sorted(payload["error"]["data"]["nested"]["attempts"]) == [1, 2]
    encoded = json.dumps(payload)
    assert "top-secret" not in encoded
    assert "operator:***@db" in encoded


def test_cmd_run_success_sets_owner_and_emits_cycle(
    monkeypatch, capsys,
) -> None:
    import okg.substrate.publisher.factory as publisher_factory_mod
    import okg.substrate.profiling as profiling_mod

    seen: dict = {}

    class FakePublisher:
        def __init__(self, dsn, deployment, **kwargs):
            seen["publisher"] = {
                "dsn": dsn,
                "deployment": deployment,
                "kwargs": kwargs,
            }

        def set_owner(self, *, owner_id, owner_meta):
            seen["owner"] = {"owner_id": owner_id, "owner_meta": owner_meta}

        def cycle(self, *, force_enrichers):
            seen["force_enrichers"] = force_enrichers
            seen["count_reconcile"] = os.environ.get(
                "OKG_FORCE_COUNT_STAMP_RECONCILE"
            )
            return {"status": "published", "generation_id": 7}

    class FakeProfile:
        generation_id = None

        def __enter__(self):
            seen["profile"] = self
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    _stub_database_readiness(monkeypatch)
    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda args: ("postgres://test", "test", None, False),
    )
    monkeypatch.setattr(cli, "_publisher_kwargs_from_env", lambda: {})
    monkeypatch.setattr(cli, "_profiling_policy_for_args", lambda args: None)
    monkeypatch.setattr(cli, "_profile_force_deep", lambda args: False)

    def fake_inline_cycle(**kwargs):
        seen["inline_cycle"] = {
            "publisher": kwargs["publisher"],
            "dsn": kwargs["dsn"],
            "deployment": kwargs["deployment"],
            "force_enrichers": kwargs["force_enrichers"],
        }
        seen["force_enrichers"] = kwargs["force_enrichers"]
        seen["count_reconcile"] = os.environ.get(
            "OKG_FORCE_COUNT_STAMP_RECONCILE"
        )
        return {
            "status": "published",
            "generation_id": 7,
            "admission_feedback": {
                "status": "published",
                "generation_id": 7,
                "branch_name": "default",
                "updated": 2,
                "admission_ids": ["admission-a", "admission-b"],
            },
        }

    monkeypatch.setattr(
        cli,
        "_run_inline_publish_cycle",
        fake_inline_cycle,
    )
    def fake_build_deployment_publisher(**kwargs):
        seen["factory"] = kwargs
        return FakePublisher(
            kwargs["dsn"],
            kwargs["deployment_name"],
            catalog_identity=object(),
        )

    monkeypatch.setattr(
        publisher_factory_mod,
        "build_deployment_publisher",
        fake_build_deployment_publisher,
    )
    monkeypatch.setattr(
        profiling_mod,
        "profiling_context",
        lambda **kwargs: FakeProfile(),
    )

    rc = cli._cmd_run(
        _args(
            once=True,
            deployment="test",
            owner_id="operator",
            owner_meta='{"ticket": "T-1"}',
            include_llm_invariants=False,
            llm_invariant=None,
            profile=None,
            profile_collector=(),
            inline=True,
            reconcile_count_stamp=True,
        ),
        as_json=True,
    )

    assert rc == 0
    assert seen["publisher"]["deployment"] == "test"
    assert seen["factory"]["command"] == "okg run"
    assert seen["owner"]["owner_meta"]["command"] == "okg run"
    assert seen["profile"].generation_id == 7
    assert seen["force_enrichers"] is False
    assert seen["count_reconcile"] == "1"
    assert isinstance(seen["inline_cycle"]["publisher"], FakePublisher)
    assert seen["inline_cycle"]["dsn"] == "postgres://test"
    assert seen["inline_cycle"]["deployment"] == "test"
    assert seen["inline_cycle"]["force_enrichers"] is False
    assert "OKG_FORCE_COUNT_STAMP_RECONCILE" not in os.environ
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "published"
    assert payload["admission_feedback"]["updated"] == 2


def test_inline_publish_feedback_skips_nonterminal_cycle() -> None:
    assert cli._apply_inline_publish_admission_feedback(
        dsn="postgres://unused",
        deployment="test",
        summary={"status": "skipped", "skipped": "no_source_work"},
    ) is None


def test_inline_publish_cycle_holds_branch_fence_through_feedback(
    monkeypatch,
) -> None:
    from contextlib import contextmanager

    import okg.substrate.db.pool as pool_mod
    import okg.substrate.graph_store as graph_store_mod

    events: list[str] = []
    connection = object()

    @contextmanager
    def checked_out_connection():
        events.append("connection_enter")
        try:
            yield connection
        finally:
            events.append("connection_exit")

    class FakePool:
        # `pooled_connection` sizes the general-admission bound from the
        # pool's width, so a stub pool has to declare one.
        max_size = 8
        name = "stub-pool"

        def connection(self):
            return checked_out_connection()

    class FakeGraph:
        def _lock_branch_write_session(self, branch):
            assert branch == "experiment"
            events.append("branch_lock")
            return True

        def _unlock_branch_write_session(self, branch):
            assert branch == "experiment"
            events.append("branch_unlock")

    class FakePublisher:
        branch_name = "experiment"

        def _resolved_coordination_mode(self):
            return "dbos"

        @contextmanager
        def _publish_lock_on_connection(self, conn, *, mode):
            assert conn is connection
            assert mode == "dbos"
            events.append("publisher_lock")
            try:
                yield conn
            finally:
                events.append("publisher_unlock")

        def cycle(self, *, conn, force_enrichers):
            assert conn is connection
            assert force_enrichers is True
            events.append("cycle")
            return {
                "status": "published",
                "generation_id": "okg:test:branch:experiment:gen:1",
            }

    graph = FakeGraph()
    monkeypatch.setattr(pool_mod, "get_pool", lambda dsn: FakePool())
    monkeypatch.setattr(
        graph_store_mod.ChronosGraphStore,
        "from_connection",
        lambda conn, *, ensure_schema: graph,
    )

    def fake_feedback(**kwargs):
        assert kwargs["conn"] is connection
        events.append("feedback")
        return {
            "status": "published",
            "generation_id": kwargs["summary"]["generation_id"],
            "branch_name": "experiment",
            "updated": 1,
            "admission_ids": ["admission-1"],
        }

    monkeypatch.setattr(
        cli,
        "_apply_inline_publish_admission_feedback",
        fake_feedback,
    )

    # okg#3112: the post-publish index rebuild runs once the fence's
    # connection, and with it the publisher lock, is released.
    import okg.substrate.db.index_reclaim as index_reclaim

    def fake_rebuild(dsn, *, deployment, summary):
        events.append("index_rebuild")
        assert summary["status"] == "published"
        return {"status": "nothing_to_rebuild"}

    monkeypatch.setattr(index_reclaim, "rebuild_indexes_after_publish", fake_rebuild)

    out = cli._run_inline_publish_cycle(
        publisher=FakePublisher(),
        dsn="postgres://test",
        deployment="test",
        force_enrichers=True,
    )

    assert out["admission_feedback"]["updated"] == 1
    assert out["post_publish_index_rebuild"] == {"status": "nothing_to_rebuild"}
    assert events == [
        "connection_enter",
        "publisher_lock",
        "branch_lock",
        "cycle",
        "feedback",
        "branch_unlock",
        "publisher_unlock",
        "connection_exit",
        "index_rebuild",
    ]


def test_inline_publish_feedback_applies_blocked_generation(
    monkeypatch,
) -> None:
    import psycopg

    import okg.substrate.admission as admission

    seen: dict[str, object] = {}

    class FakeTransaction:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def transaction(self):
            return FakeTransaction()

    def fake_connect(dsn, *, autocommit, **kwargs):
        # `**kwargs` absorbs the settings the substrate connection factory
        # applies to every connection; the assertion below is about the DSN
        # and the autocommit mode this command asks for.
        seen["connect"] = (dsn, autocommit)
        return FakeConnection()

    def fake_feedback(conn, **kwargs):
        seen["feedback"] = kwargs
        return {
            "status": "blocked",
            "generation_id": "okg:test:branch:default:gen:blocked",
            "branch_name": "default",
            "updated": 1,
            "admission_ids": ["admission-blocked"],
        }

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    monkeypatch.setattr(admission, "apply_generation_feedback", fake_feedback)

    result = cli._apply_inline_publish_admission_feedback(
        dsn="postgres://test",
        deployment="test",
        summary={
            "status": "blocked",
            "generation_id": "okg:test:branch:default:gen:blocked",
        },
    )

    assert seen["connect"] == ("postgres://test", True)
    assert seen["feedback"] == {
        "deployment_name": "test",
        "generation_id": "okg:test:branch:default:gen:blocked",
        "workflow_ref": {"workflow_name": "okg_run_once_inline"},
    }
    assert result is not None
    assert result["updated"] == 1


def test_cmd_run_reports_inline_feedback_failure_with_generation(
    monkeypatch,
    capsys,
) -> None:
    import psycopg

    import okg.substrate.publisher.factory as publisher_factory_mod
    import okg.substrate.profiling as profiling_mod

    class FakePublisher:
        def __init__(self, dsn, deployment, **kwargs):
            pass

        def set_owner(self, *, owner_id, owner_meta):
            pass

        def cycle(self, *, force_enrichers):
            return {
                "status": "published",
                "generation_id": "okg:test:branch:default:gen:1",
            }

    class FakeProfile:
        generation_id = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    _stub_database_readiness(monkeypatch)
    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda args: ("postgres://test", "test", None, False),
    )
    monkeypatch.setattr(cli, "_profiling_policy_for_args", lambda args: None)
    monkeypatch.setattr(cli, "_profile_force_deep", lambda args: False)
    monkeypatch.setattr(
        publisher_factory_mod,
        "build_deployment_publisher",
        lambda **kwargs: FakePublisher(
            kwargs["dsn"],
            kwargs["deployment_name"],
            catalog_identity=object(),
        ),
    )
    monkeypatch.setattr(
        profiling_mod,
        "profiling_context",
        lambda **kwargs: FakeProfile(),
    )

    def fail_connect(*args, **kwargs):
        raise RuntimeError("feedback database unavailable")

    monkeypatch.setattr(psycopg, "connect", fail_connect)
    monkeypatch.setattr(
        cli,
        "_run_inline_publish_cycle",
        lambda **kwargs: cli._apply_inline_publish_admission_feedback(
            dsn=kwargs["dsn"],
            deployment=kwargs["deployment"],
            summary={
                "status": "published",
                "generation_id": "okg:test:branch:default:gen:1",
            },
        ),
    )

    rc = cli._cmd_run(
        _args(
            once=True,
            deployment="test",
            owner_id=None,
            owner_meta="{}",
            include_llm_invariants=False,
            llm_invariant=None,
            profile=None,
            profile_collector=(),
            inline=True,
            reconcile_count_stamp=False,
            force_enrichers=False,
            branch=None,
        ),
        as_json=True,
    )

    assert rc == 1
    error = _captured_error(capsys)["error"]
    assert error["code"] == "substrate_error"
    assert error["data"] == {
        "deployment": "test",
        "generation_id": "okg:test:branch:default:gen:1",
        "generation_status": "published",
        "generation_published": True,
        "stage": "admission_feedback",
        "feedback_error": (
            "RuntimeError: feedback database unavailable"
        ),
    }
    assert "do not reingest" in error["fix_hint"]


def test_cmd_run_rejects_non_json_owner_meta(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_resolve_dsn", lambda args: "postgres://test")

    rc = cli._cmd_run(
        _args(
            once=True,
            deployment="test",
            owner_id=None,
            owner_meta="{not-json",
            include_llm_invariants=False,
            profile=None,
            profile_collector=(),
        ),
        as_json=True,
    )

    assert rc == 1


def test_cmd_mcp_serve_delegates_to_stdio(monkeypatch) -> None:
    seen: dict = {}
    server_mod = ModuleType("okg.substrate.mcp.server")
    # _cmd_mcp_serve dispatches through the transport-agnostic run_server;
    # run_stdio remains for other callers (deployment_entry) but is unused here.
    server_mod.run_server = lambda **kwargs: seen.update(kwargs)
    server_mod.run_stdio = lambda **kwargs: seen.update(kwargs)
    # _cmd_mcp_serve also imports the write/transport guard from this module.
    server_mod._write_transport_guard = lambda **kwargs: None
    monkeypatch.setitem(
        sys.modules,
        "okg.substrate.mcp.server",
        server_mod,
    )
    import okg.substrate.cli.dsn_authority as dsn_authority
    import okg.substrate.deployment_release as deployment_release

    _stub_database_readiness(monkeypatch)
    monkeypatch.setattr(
        dsn_authority,
        "resolve_read_dsn",
        lambda args, *, surface: SimpleNamespace(
            dsn="postgres://test",
            authority={"mode": "test"},
            target="test",
        ),
    )
    monkeypatch.setattr(
        deployment_release,
        "enforce_release_contract_if_declared",
        lambda **kwargs: None,
    )
    rc = cli._cmd_mcp_serve(
        _args(deployment="test", embedder="hash", branch=None),
        as_json=True,
    )

    assert rc == 0
    embedder = seen.pop("embedder")
    assert embedder.enabled is True
    assert embedder.model_id == "hash:2560"
    assert embedder.fingerprint.startswith("sha256:")
    assert seen == {
        "transport": "stdio",
        "host": "127.0.0.1",
        "port": 8100,
        "dsn": "postgres://test",
        "dsn_authority": {
            "dsn": "postgres://test",
            "authority": {"mode": "test"},
            "target": "test",
        },
        "deployment_name": "test",
        "branch_name": "default",
        "allow_writes": False,
        "allowed_hosts": (),
        # PACT mcp-http-auth: the CLI forwards the VARIABLE NAME (never the
        # token value); None here means no --auth-token-env was given.
        "auth_token_env": None,
        # PACT mcp-principal-mapping: the multi-user posture is forwarded
        # EXPLICITLY, including when off. Asserting the False case matters --
        # a posture that reached run_server only when enabled would leave the
        # default arm depending on run_server's own default rather than on
        # what the CLI decided, and the two could drift apart unnoticed.
        "multi_user_principals": False,
        # PACT mcp-oidc-auth: same discipline. Off is forwarded as False, so
        # the no-auth default stays a CLI decision, never run_server's.
        "oidc_auth": False,
        # PACT mcp-tool-surface-profiles: same discipline for the tool
        # profile. None means no --tools was given, and forwarding it
        # explicitly keeps the unnarrowed surface a CLI decision rather than
        # run_server's default.
        "tool_profile": None,
        # Issue #1458: and again for the in-session ceiling. None means no
        # --tools-ceiling was given, so the tool list is fixed for a
        # session's lifetime -- a CLI decision, not run_server's default.
        "tool_profile_ceiling": None,
        # PACT unknown-s8-mcp-readonly: None unless the release verdict is
        # `read_only`; this deployment's database is not behind.
        "read_only_release": None,
    }


def test_cmd_mcp_serve_passes_streamable_http_flags(monkeypatch) -> None:
    seen: dict = {}
    server_mod = ModuleType("okg.substrate.mcp.server")
    server_mod.run_server = lambda **kwargs: seen.update(kwargs)
    server_mod.run_stdio = lambda **kwargs: seen.update(kwargs)
    server_mod._write_transport_guard = lambda **kwargs: None
    monkeypatch.setitem(sys.modules, "okg.substrate.mcp.server", server_mod)
    import okg.substrate.cli.dsn_authority as dsn_authority
    import okg.substrate.deployment_release as deployment_release

    _stub_database_readiness(monkeypatch)
    monkeypatch.setattr(
        dsn_authority,
        "resolve_read_dsn",
        lambda args, *, surface: SimpleNamespace(
            dsn="postgres://test",
            authority={"mode": "test"},
            target="test",
        ),
    )
    monkeypatch.setattr(
        deployment_release,
        "enforce_release_contract_if_declared",
        lambda **kwargs: None,
    )

    rc = cli._cmd_mcp_serve(
        _args(
            deployment="test",
            embedder=None,
            branch=None,
            transport="streamable-http",
            host="0.0.0.0",
            port=9001,
            allowed_host=[
                "verification-okg-mcp:*",
                "verification-okg-mcp.default.svc:*",
            ],
        ),
        as_json=True,
    )

    assert rc == 0
    # the CLI must forward the transport/host/port selection to run_server.
    assert seen["transport"] == "streamable-http"
    assert seen["host"] == "0.0.0.0"
    assert seen["port"] == 9001
    assert seen["deployment_name"] == "test"
    assert seen["allowed_hosts"] == (
        "verification-okg-mcp:*",
        "verification-okg-mcp.default.svc:*",
    )


def test_cmd_mcp_serve_refuses_unsafe_write_transport(monkeypatch) -> None:
    seen: dict = {}
    server_mod = ModuleType("okg.substrate.mcp.server")
    server_mod.run_server = lambda **kwargs: seen.update(kwargs)
    server_mod.run_stdio = lambda **kwargs: seen.update(kwargs)
    # guard reports the unsafe combo -> the CLI must refuse before serving.
    server_mod._write_transport_guard = lambda **kwargs: ("refuse", "unauthenticated write surface")
    monkeypatch.setitem(sys.modules, "okg.substrate.mcp.server", server_mod)
    import okg.substrate.cli.dsn_authority as dsn_authority
    import okg.substrate.deployment_release as deployment_release

    monkeypatch.setattr(
        dsn_authority,
        "resolve_read_dsn",
        lambda args, *, surface: SimpleNamespace(
            dsn="postgres://test",
            authority={"mode": "test"},
            target="test",
        ),
    )
    monkeypatch.setattr(
        deployment_release,
        "enforce_release_contract_if_declared",
        lambda **kwargs: None,
    )

    rc = cli._cmd_mcp_serve(
        _args(
            deployment="test",
            embedder=None,
            branch=None,
            transport="streamable-http",
            host="0.0.0.0",
            allow_writes=True,
        ),
        as_json=True,
    )

    assert rc != 0
    # refused before serving -- run_server must not have been reached.
    assert seen == {}


def test_cmd_profile_list_show_and_compare(monkeypatch, capsys) -> None:
    import okg.substrate.profiling.artifacts as artifacts

    left = {
        "profile_run_id": "left",
        "deployment": "test",
        "target_kind": "publish",
        "stages": [{"name": "projection", "duration_ms": 10}],
    }
    right = {
        "profile_run_id": "right",
        "deployment": "test",
        "target_kind": "publish",
        "stages": [{"name": "projection", "duration_ms": 13}],
    }
    profiles = {
        "left": left,
        "right": right,
        "/profiles/left.json": left,
        "/profiles/right.json": right,
    }
    monkeypatch.setattr(
        artifacts,
        "list_profiles",
        lambda deployment: [
            {
                "profile_run_id": "left",
                "deployment": deployment,
                "target_kind": "publish",
                "command": "okg run",
                "started_at": "2026-05-04T00:00:00Z",
                "duration_ms": 10,
                "summary": {
                    "bottleneck_class": "projection",
                    "top_stage": "projection",
                },
            }
        ],
    )
    monkeypatch.setattr(artifacts, "profile_root", lambda: "/profiles")
    monkeypatch.setattr(
        artifacts,
        "find_profile",
        lambda profile_id, deployment: f"/profiles/{profile_id}.json",
    )
    monkeypatch.setattr(artifacts, "load_profile", lambda path: profiles[path])

    assert cli._cmd_profile(
        _args(subcmd="list", deployment="test", limit=10),
        as_json=True,
    ) == 0
    assert cli._cmd_profile(
        _args(subcmd="show", deployment="test", profile_id="left"),
        as_json=True,
    ) == 0
    assert cli._cmd_profile(
        _args(subcmd="compare", deployment="test", left="left", right="right"),
        as_json=True,
    ) == 0

    out = capsys.readouterr().out
    assert '"profiles"' in out
    assert '"profile_run_id": "left"' in out
    assert '"delta_ms": 3.0' in out


@pytest.mark.parametrize("views_only", [False, True])
def test_cmd_migrate_reports_recreated_views(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    views_only: bool,
) -> None:
    import okg.substrate.db.migrate as migrate_module
    result = migrate_module.MigrateResult(
        statements_executed=7,
        statements_skipped=0,
        extension_versions={},
        idempotent_second_run=True,
        converged_views=("okg.v_issue1169_probe",),
    )
    monkeypatch.setattr(cli, "_resolve_dsn", lambda _args: "postgresql://probe")
    monkeypatch.setattr(migrate_module, "migrate", lambda *_args, **_kwargs: result)
    monkeypatch.setattr(
        migrate_module, "install_views", lambda *_args, **_kwargs: result
    )

    # `--apply` is required for every caller now (#1288); without it the
    # command refuses before it would reach either branch.
    rc = cli._cmd_migrate(
        _args(deployment=None, dsn=None, views_only=views_only, apply=True),
        as_json=True,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["views_only"] is views_only
    assert payload["mutation_requested"] is True
    assert payload["mutation_started"] is True
    assert payload["converged_views"] == ["okg.v_issue1169_probe"]


def test_public_cli_command_handlers_have_boundary_contracts(
    tmp_path,
    monkeypatch,
) -> None:
    import okg.substrate.deployments as deployments

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.setattr(deployments, "find_deployment_dir", lambda name: None)

    missing_dep = tmp_path / "missing-deployment"

    assert cli._cmd_migrate(
        _args(deployment=None, dsn=None),
        as_json=True,
    ) != 0
    assert cli._cmd_ingest(
        _args(deployment=None, dsn=None, progress=False),
        as_json=True,
    ) != 0
    assert cli._cmd_sync(
        _args(deployment="missing-deployment", sync_subcmd="health"),
        as_json=True,
    ) != 0
    assert cli._cmd_status(
        _args(deployment=None, dsn=None, watch=False, interval=1),
        as_json=True,
    ) != 0
    assert cli._cmd_init(
        _args(template="okg-workspace", repo_root=None),
        as_json=True,
    ) != 0
    assert cli._cmd_library(
        _args(subcmd="list", kind=None),
        as_json=True,
    ) == 0
    assert cli._cmd_trace(
        _args(dsn=None, node_id="node:missing"),
        as_json=True,
    ) != 0
    assert cli._cmd_schema(
        _args(file="missing.yaml"),
        as_json=True,
    ) != 0
    assert cli._cmd_catalog(
        _args(subcmd="missing"),
        as_json=True,
    ) != 0
    # `_cmd_catalog_load` was extracted to `cli/catalog.py` by
    # `extract-cli-catalog`. (`_cmd_catalog_reconcile` — the legacy
    # live-vs-superseded scan — was removed by the chronos cutover.)
    assert cli_catalog._cmd_catalog_load(
        _args(deployment=str(missing_dep), apply=False),
        as_json=True,
    ) != 0
    assert cli._cmd_metrics(
        _args(
            qa_snapshot=False,
            llm=False,
            source_sync=False,
        ),
        as_json=True,
    ) != 0
    assert cli._cmd_alias(
        _args(dsn=None),
        as_json=True,
    ) != 0


def test_ingest_reset_cursor_requires_inline_when_runtime_enabled(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    import okg.substrate.deployments as deployments

    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda deployment: tmp_path,
    )
    monkeypatch.setattr(deployments, "inject_deployment_path", lambda path: None)
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda deployment: True)

    rc = cli._cmd_ingest(
        _args(
            deployment="test",
            dsn="postgres://postgres:okg@localhost:5433/okg_test",
            inline=False,
            reset_cursor=True,
            progress=False,
        ),
        as_json=True,
    )

    assert rc != 0
    assert "--reset-cursor repair requires --inline" in capsys.readouterr().err


def test_ingest_allow_full_retraction_requires_inline_when_runtime_enabled(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    import okg.substrate.deployments as deployments

    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda deployment: tmp_path,
    )
    monkeypatch.setattr(deployments, "inject_deployment_path", lambda path: None)
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda deployment: True)

    rc = cli._cmd_ingest(
        _args(
            deployment="test",
            dsn="postgres://postgres:okg@localhost:5433/okg_test",
            inline=False,
            reset_cursor=False,
            allow_full_retraction=["external_repos"],
            progress=False,
        ),
        as_json=True,
    )

    assert rc != 0
    assert "--allow-full-retraction requires --inline" in capsys.readouterr().err


def test_ingest_inline_publish_refuses_active_source_sync_work(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    import okg.substrate.deployments as deployments
    import okg.substrate.ingest.service as ingest_service
    import okg.substrate.runtime.admin as runtime_admin

    def fail_if_called(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("guard should fire before execution path")

    _write_minimal_registry(tmp_path)
    monkeypatch.setattr(cli, "_resolve_dsn", lambda args: "postgres://unused")
    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda deployment: tmp_path,
    )
    monkeypatch.setattr(deployments, "inject_deployment_path", lambda path: None)
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda deployment: True)
    monkeypatch.setattr(
        cli,
        "_active_source_sync_runtime_work",
        lambda *, deployment, dsn: {
            "available": True,
            "queue_name": "okg-okg-workspace-source-sync",
            "active_count": 1,
            "active_rows": [{
                "queue_name": "okg-okg-workspace-source-sync",
                "status": "PENDING",
                "count": 1,
            }],
        },
    )
    monkeypatch.setattr(ingest_service, "run_ingest", fail_if_called)
    monkeypatch.setattr(runtime_admin, "client_enqueue_source_sync", fail_if_called)

    rc = cli._cmd_ingest(
        _args(
            deployment="okg-workspace",
            dsn=None,
            include="pact",
            exclude=None,
            inline=True,
            mode="scope_complete",
            no_predecessors=False,
            include_dependents=False,
            dependent_fanout=None,
            enqueue=False,
            no_publish=False,
            reset_cursor=False,
            progress=False,
            profile=None,
            profile_collector=(),
            owner_id=None,
            owner_meta=None,
        ),
        as_json=True,
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    err = payload["error"]
    assert err["code"] == "runtime_source_sync_active"
    assert "--inline" in err["message"]
    assert err["data"]["deployment"] == "okg-workspace"
    assert err["data"]["queue_name"] == "okg-okg-workspace-source-sync"
    assert err["data"]["active_count"] == 1
    assert "fix_hint" in err


def test_ingest_dbos_enqueue_only_does_not_wait(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    import okg.substrate.deployments as deployments
    import okg.substrate.runtime.admin as runtime_admin

    captured: dict[str, object] = {}

    _write_minimal_registry(tmp_path)
    monkeypatch.setattr(cli, "_resolve_dsn", lambda args: "postgres://unused")
    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda deployment: tmp_path,
    )
    monkeypatch.setattr(deployments, "inject_deployment_path", lambda path: None)
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda deployment: True)

    def fake_enqueue_source_sync(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            workflow_id="wf-profile-enqueued",
            status="ENQUEUED",
            any_failed=False,
            elapsed_seconds=0.0,
            report=None,
            error=None,
        )

    monkeypatch.setattr(
        runtime_admin,
        "client_enqueue_source_sync",
        fake_enqueue_source_sync,
    )

    rc = cli._cmd_ingest(
        _args(
            deployment="okg-workspace",
            dsn=None,
            include="pact",
            exclude=None,
            inline=False,
            mode="scope_complete",
            no_predecessors=True,
            include_dependents=False,
            dependent_fanout=None,
            enqueue=True,
            no_publish=False,
            reset_cursor=False,
            progress=False,
            profile="deep",
            profile_collector=("rootless-speedscope",),
            owner_id=None,
            owner_meta=None,
        ),
        as_json=True,
    )

    assert rc == 0
    assert captured["wait_for_result"] is False
    assert captured["sources"] == ["pact"]
    assert captured["include_predecessors"] is False
    assert captured["profile_force_deep"] is True
    assert captured["profile_collectors"] == ("rootless-speedscope",)
    out = capsys.readouterr().out
    assert '"workflow_id": "wf-profile-enqueued"' in out
    assert '"job_kind": "ingest"' not in out
