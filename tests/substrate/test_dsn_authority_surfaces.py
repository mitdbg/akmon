from __future__ import annotations

import json
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.cli import _read_side
from okg.substrate.cli import main as cli_main
from okg.substrate.cli.dsn_authority import resolve_read_dsn
from okg.substrate.errors import SubstrateError


def _write_deployment(tmp_path: Path, name: str = "okg-workspace") -> Path:
    deployment_dir = tmp_path / "deployments" / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": name,
            "postgres": {
                "dsn": "${OKG_DSN}",
                "database": "okg_workspace",
            },
        }),
        encoding="utf-8",
    )
    return deployment_dir


def test_search_uses_worktree_main_env_from_cold_worktree(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deployment_dir = _write_deployment(tmp_path)
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: deployment_dir if deployment == "okg-workspace" else None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.worktree_main_local_env_dsn",
        lambda: "postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
    )

    args = SimpleNamespace(deployment="okg-workspace", dsn=None)

    assert _read_side._resolve_dsn(args) == (
        "postgres://postgres:okg@127.0.0.1:55434/okg_workspace"
    )
    resolution = resolve_read_dsn(args, surface="okg search")
    assert resolution.authority["mode"] == "worktree_main_local_env"
    assert resolution.target == {
        "authority_mode": "worktree_main_local_env",
        "database": "okg_workspace",
        "deployment": "okg-workspace",
        "expected_database": "okg_workspace",
        "host": "127.0.0.1",
        "identity": "ok",
        "port": 55434,
    }


def test_sealed_search_uses_launcher_dsn_and_ignores_caller_override(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setenv("OKG_DEPLOYMENT", "okg-ci-dev")
    monkeypatch.setenv("OKG_DSN", "postgres://sealed-authority")

    resolution = resolve_read_dsn(
        SimpleNamespace(
            deployment="okg-ci-dev",
            dsn="postgres://caller-override",
        ),
        surface="okg search",
    )

    assert resolution.dsn == "postgres://sealed-authority"
    assert resolution.authority == {
        "mode": "sealed_environment",
        "deployment": "okg-ci-dev",
    }

    with pytest.raises(SubstrateError, match="outside its active environment"):
        resolve_read_dsn(
            SimpleNamespace(deployment="foreign-deployment", dsn=None),
            surface="okg search",
        )


def test_sealed_search_refuses_missing_launcher_dsn(monkeypatch) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setenv("OKG_DEPLOYMENT", "okg-ci-dev")
    monkeypatch.delenv("OKG_DSN", raising=False)

    with pytest.raises(SubstrateError, match="authoritative OKG_DSN"):
        resolve_read_dsn(
            SimpleNamespace(deployment="okg-ci-dev", dsn=None),
            surface="okg search",
        )


def test_search_json_reports_authority_and_target(capsys, monkeypatch) -> None:
    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    def fake_connect(dsn: str, **_kwargs):
        assert dsn == "postgres://postgres:okg@127.0.0.1:55434/okg_workspace"
        return FakeConnection()

    monkeypatch.setattr(
        "okg.substrate.cli._read_side.resolve_read_dsn",
        lambda _args, *, surface: SimpleNamespace(
            dsn="postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
            deployment="okg-workspace",
            authority={
                "mode": "worktree_main_local_env",
                "deployment": "okg-workspace",
            },
            target={
                "deployment": "okg-workspace",
                "database": "okg_workspace",
                "identity": "ok",
            },
        ),
    )
    monkeypatch.setattr("psycopg.connect", fake_connect)
    # `okg search` resolves the enforcement mode once and reports it (#2279).
    monkeypatch.setattr(
        "okg.substrate.nomos.deployment_nomos_enforcement_mode",
        lambda _deployment: "off",
    )
    monkeypatch.setattr(
        "okg.substrate.cli._read_side._open_session",
        lambda *_args, **_kwargs: SimpleNamespace(generation_id="gen:test", catalog_version_id=1),
    )
    search_module = importlib.import_module("okg.substrate.mcp.search")
    monkeypatch.setattr(
        search_module,
        "search",
        lambda *_args, **_kwargs: {
            "query": "worktree_main_local_env",
            "results": [],
        },
    )
    # Resolving a deployment search profile reads the pinned catalog, which
    # this DSN-authority fixture has no database for — same reason `search`
    # itself is stubbed above. Profile behaviour is covered by
    # tests/substrate/mcp/test_search_profiles.py.
    monkeypatch.setattr(
        "okg.substrate.cli._read_side._resolve_search_scope",
        lambda *_args, **_kwargs: (
            importlib.import_module(
                "okg.substrate.mcp.search_profiles"
            ).UNSCOPED
        ),
    )

    rc = _read_side.cmd_search(
        SimpleNamespace(
            deployment="okg-workspace",
            dsn=None,
            method="lexical",
            subtype=None,
            attr_filter=None,
            query="worktree_main_local_env",
            limit=1,
            offset=0,
            select=None,
            generation_id=None,
        ),
        as_json=True,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["authority"]["mode"] == "worktree_main_local_env"
    assert payload["target"]["identity"] == "ok"
    assert payload["deployment"] == "okg-workspace"


def test_search_nomos_denial_blocks_search_before_effect(
    capsys,
    monkeypatch,
) -> None:
    calls: list[str] = []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    def fake_connect(_dsn: str, **_kwargs):
        return FakeConnection()

    def fake_authorize(*_args, **_kwargs):
        calls.append("nomos")
        err = SubstrateError(
            "Nomos denied graph.read.cli_search: nomos.test.denied",
            data={"surface": "graph.read.cli_search"},
        )
        err.code = "nomos_runtime_denied"
        raise err

    def forbidden_search(*_args, **_kwargs):
        raise AssertionError("search should not run after Nomos denial")

    monkeypatch.setattr(
        "okg.substrate.cli._read_side.resolve_read_dsn",
        lambda _args, *, surface: SimpleNamespace(
            dsn="postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
            deployment="okg-workspace",
            authority={"mode": "test"},
            target={"identity": "ok"},
        ),
    )
    monkeypatch.setattr("psycopg.connect", fake_connect)
    monkeypatch.setattr(
        "okg.substrate.cli._read_side._open_session",
        lambda *_args, **_kwargs: SimpleNamespace(generation_id="gen:test", catalog_version_id=1),
    )
    monkeypatch.setattr(
        "okg.substrate.cli._read_side._authorize_search_with_nomos",
        fake_authorize,
    )
    search_module = importlib.import_module("okg.substrate.mcp.search")
    monkeypatch.setattr(search_module, "search", forbidden_search)

    rc = _read_side.cmd_search(
        SimpleNamespace(
            deployment="okg-workspace",
            dsn=None,
            method="lexical",
            subtype=None,
            attr_filter=None,
            query="blocked",
            limit=1,
            offset=0,
            select=None,
            generation_id=None,
        ),
        as_json=True,
    )

    assert rc == 1
    assert calls == ["nomos"]
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["code"] == "nomos_runtime_denied"


def test_trace_accepts_deployment_and_uses_shared_authority(
    capsys,
    monkeypatch,
    tmp_path,
) -> None:
    # A synthetic self-hosted deployment (the flag that makes the local
    # profile authoritative), so the public copy runs this too; this
    # repository's own workspace deployment declares the same flag.
    deployments = tmp_path / "deployments"
    (deployments / "self-hosted-demo").mkdir(parents=True)
    (deployments / "self-hosted-demo" / "deployment.yaml").write_text(
        "name: self-hosted-demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: self_hosted_demo\n"
        "  self_hosted_authoritative: true\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments))
    calls: dict[str, object] = {}

    def fake_resolve(deployment: str, **kwargs):
        calls["deployment"] = deployment
        calls["kwargs"] = kwargs
        return {
            "dsn": "postgres://postgres:okg@127.0.0.1:55434/self_hosted_demo",
            "authority": {
                "mode": "worktree_main_local_env",
                "deployment": deployment,
            },
            "target": {
                "authority_mode": "worktree_main_local_env",
                "deployment": deployment,
                "database": "self_hosted_demo",
                "identity": "ok",
            },
        }

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def cursor(self):
            return FakeCursor()

    def fake_connect(dsn: str, **kwargs):
        calls["dsn"] = dsn
        calls["connect_kwargs"] = kwargs
        return FakeConnection()

    def fake_preview(_conn, node_id: str, *, generation_id=None):
        calls["node_id"] = node_id
        calls["generation_id"] = generation_id
        return {
            "present": False,
            "diagnostic": "chronos_generation_missing",
            "generation_id": generation_id,
        }

    monkeypatch.setattr(
        "okg.substrate.cli.dsn_authority.resolve_deployment_dsn_authority",
        fake_resolve,
    )
    monkeypatch.setattr("psycopg.connect", fake_connect)
    monkeypatch.setattr(cli_main, "_trace_graph_node_preview", fake_preview)

    rc = cli_main.main([
        "--json",
        "trace",
        "node",
        "node:demo",
        "--deployment",
        "self-hosted-demo",
        "--generation-id",
        "okg:self-hosted-demo:branch:default:gen:test",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert calls["deployment"] == "self-hosted-demo"
    assert calls["kwargs"] == {
        "explicit_dsn": None,
        "local_profile_authoritative": True,
    }
    assert calls["dsn"] == (
        "postgres://postgres:okg@127.0.0.1:55434/self_hosted_demo"
    )
    assert calls["generation_id"] == "okg:self-hosted-demo:branch:default:gen:test"
    assert payload["authority"]["mode"] == "worktree_main_local_env"
    assert payload["deployment"] == "self-hosted-demo"
    assert payload["graph_node"]["diagnostic"] == "chronos_generation_missing"


def test_stale_okg_entrypoint_is_refused_or_delegated(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    checkout = tmp_path / "okg"
    checkout.mkdir()
    package = tmp_path / "tool" / "okg"
    package.mkdir(parents=True)
    monkeypatch.setattr(cli_main, "_cwd_okg_checkout_root", lambda: checkout)
    monkeypatch.setattr(cli_main, "_entrypoint_package_root", lambda: package)
    monkeypatch.setattr(sys, "argv", ["/Users/example/.local/bin/okg"])

    err = cli_main._stale_entrypoint_error([
        "deployment",
        "env",
        "okg-workspace",
        "--json",
    ])

    assert err is not None
    assert err.message == "bare okg is not running this checkout's code"
    assert err.data["repo_root"] == str(checkout)
    assert err.data["package_root"] == str(package)
    assert err.data["executable_path"] == "/Users/example/.local/bin/okg"
    assert err.data["expected_command"].endswith(
        "uv run okg deployment env okg-workspace --json"
    )
    assert err.data["repair_command"].endswith(str(checkout))

    rc = cli_main.main(["--json", "future-command", "--new-flag"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["message"] == (
        "bare okg is not running this checkout's code"
    )
    assert payload["error"]["data"]["repo_root"] == str(checkout)


def _authorize_search_2279(monkeypatch, *, mode, outcome):
    import okg.substrate.nomos as nomos

    calls = []
    monkeypatch.setattr(nomos, "deployment_nomos_enforcement_mode", lambda _d: mode)

    def evaluate(_conn, **kwargs):
        calls.append(kwargs)
        return outcome

    monkeypatch.setattr(nomos, "evaluate_runtime_boundary_with_active_policy", evaluate)
    monkeypatch.setattr(_read_side, "_nomos_profile_record", lambda *_a, **_k: {})
    result = _read_side._authorize_search_with_nomos(
        SimpleNamespace(commit=lambda: None),
        deployment="fixture",
        session=SimpleNamespace(generation_id=1, branch_name="default", catalog_version_id=1),
        query="q",
        method="lexical",
        limit=10,
        offset=0,
        filter_obj=None,
        select=None,
        access_context=None,
        subject="subject:test",
    )
    return result, calls


@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_search_refuses_an_evaluated_mode_without_a_decision(monkeypatch, mode):
    # Issue #2279 review N1: `nomos_effect: null` means mode off. An enforce or
    # audit evaluation that yields no outcome must refuse, never report null.
    with pytest.raises(SubstrateError) as exc:
        _authorize_search_2279(monkeypatch, mode=mode, outcome=None)
    assert exc.value.code == "nomos_search_decision_missing"
    assert exc.value.data["enforcement_mode"] == mode


def test_search_off_mode_evaluates_nothing(monkeypatch):
    (mode, outcome), calls = _authorize_search_2279(
        monkeypatch, mode="off", outcome=None,
    )
    assert (mode, outcome, calls) == ("off", None, [])
    assert _read_side._search_nomos_fields(mode, outcome) == {
        "nomos_enforcement_mode": "off",
        "nomos_effect": None,
        "nomos_policy_effect": None,
        "nomos_decision_id": None,
    }


def test_search_passes_the_resolved_mode_to_the_evaluation(monkeypatch):
    outcome = SimpleNamespace(allowed=True)
    (mode, got), calls = _authorize_search_2279(
        monkeypatch, mode="audit", outcome=outcome,
    )
    assert (mode, got) == ("audit", outcome)
    assert calls[0]["enforcement_mode"] == "audit"
