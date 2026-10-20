from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import decisions
from okg.substrate.pact import operations
from okg.substrate.pact import review
from tests.pact.test_cli import _seed_root


def _set_graph_deployment(root: Path, deployment: str) -> None:
    path = root / "pact" / "project.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data["graph_projection"] = {"deployment": deployment}
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _capture_authority(monkeypatch, *, dsn: str | None = "postgres://selected/db"):
    calls: list[dict[str, object]] = []

    def resolve(deployment, *, explicit_dsn=None, local_profile_authoritative=False):
        calls.append({
            "deployment": deployment,
            "explicit_dsn": explicit_dsn,
            "local_profile_authoritative": local_profile_authoritative,
        })
        return {"dsn": dsn, "authority": {"mode": "test"}} if dsn else {
            "dsn": None,
            "reason": f"deployment {deployment!r} not found",
            "next_action": "configure_okg_dsn",
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_authority.resolve_deployment_dsn_authority",
        resolve,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.deployment_self_hosted_authoritative",
        lambda deployment: False,
    )
    return calls


def test_ledger_uses_downstream_project_graph_deployment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _set_graph_deployment(root, "cooper-self")
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    calls = _capture_authority(monkeypatch)

    result = decisions.resolve_ledger_dsn(repo_root=root)

    assert result["deployment"] == "cooper-self"
    assert result["deployment_source"] == "pact_project"
    assert calls == [{
        "deployment": "cooper-self",
        "explicit_dsn": None,
        "local_profile_authoritative": False,
    }]


def test_ledger_deployment_precedence_is_explicit_then_env_then_project(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _set_graph_deployment(root, "from-project")
    monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "from-env")
    calls = _capture_authority(monkeypatch)

    env_result = decisions.resolve_ledger_dsn(repo_root=root)
    explicit_result = decisions.resolve_ledger_dsn(
        repo_root=root,
        deployment="from-explicit",
    )

    assert env_result["deployment"] == "from-env"
    assert env_result["deployment_source"] == "process_env"
    assert explicit_result["deployment"] == "from-explicit"
    assert explicit_result["deployment_source"] == "explicit"
    assert [row["deployment"] for row in calls] == ["from-env", "from-explicit"]


def test_explicit_dsn_is_identity_checked_against_selected_project_deployment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _set_graph_deployment(root, "cooper-self")
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    calls = _capture_authority(monkeypatch)
    explicit_dsn = "postgres://operator@localhost/cooper"

    decisions.resolve_ledger_dsn(repo_root=root, explicit_dsn=explicit_dsn)

    assert calls[0]["deployment"] == "cooper-self"
    assert calls[0]["explicit_dsn"] == explicit_dsn


def test_refused_explicit_dsn_never_falls_back_to_process_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _set_graph_deployment(root, "cooper-self")
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    monkeypatch.setenv("OKG_DSN", "postgres://process/fallback-must-not-win")
    calls = _capture_authority(monkeypatch, dsn=None)

    result = decisions.resolve_ledger_dsn(
        repo_root=root,
        explicit_dsn="postgres://explicit/wrong-database",
    )

    assert calls[0]["explicit_dsn"] == "postgres://explicit/wrong-database"
    assert result["dsn"] is None
    assert result["next_action"] == "configure_okg_dsn"


@pytest.mark.parametrize(
    "project_patch",
    [
        {},
        {"graph_projection": {}},
        {"graph_projection": "not-a-mapping"},
    ],
)
def test_missing_or_malformed_project_policy_never_defaults_to_workspace(
    tmp_path: Path,
    monkeypatch,
    project_patch: dict,
) -> None:
    root = _seed_root(tmp_path)
    project_path = root / "pact" / "project.yaml"
    project = yaml.safe_load(project_path.read_text(encoding="utf-8"))
    project.update(project_patch)
    project_path.write_text(
        yaml.safe_dump(project, sort_keys=False),
        encoding="utf-8",
    )
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.resolve_deployment_dsn_authority",
        lambda *args, **kwargs: pytest.fail("authority must not receive a fallback"),
    )

    result = decisions.resolve_ledger_dsn(repo_root=root)

    assert result["dsn"] is None
    assert result["deployment"] is None
    assert "graph_projection" in result["reason"]
    assert "okg-workspace" not in result["reason"]


def test_selected_unknown_deployment_remains_selected_in_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _set_graph_deployment(root, "cooper-missing")
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    monkeypatch.setenv("OKG_DSN", "postgres://generic/must-not-win")
    monkeypatch.setattr(
        decisions,
        "worktree_local_env_dsn",
        lambda: "postgres://worktree/must-not-win",
    )
    calls = _capture_authority(monkeypatch, dsn=None)

    result = decisions.resolve_ledger_dsn(repo_root=root)

    assert calls[0]["deployment"] == "cooper-missing"
    assert result["dsn"] is None
    assert result["deployment"] == "cooper-missing"
    assert "cooper-missing" in result["reason"]


def test_nested_cwd_discovers_project_policy(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _set_graph_deployment(root, "cooper-nested")
    nested = root / "analysis" / "work"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    calls = _capture_authority(monkeypatch)

    result = decisions.resolve_ledger_dsn()

    assert result["deployment"] == "cooper-nested"
    assert calls[0]["deployment"] == "cooper-nested"


def test_public_ledger_consumers_propagate_repo_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    observed: list[Path | None] = []

    def unavailable(*, repo_root=None, **_kwargs):
        observed.append(repo_root)
        raise SubstrateError("ledger unavailable", data={"reason": "test"})

    monkeypatch.setattr(decisions, "require_ledger_connection", unavailable)

    review.build_queue(root)
    operations._workflow_ledger_authority(root)

    assert observed == [root, root]


def test_deferred_coordination_replay_keeps_repository_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    observed: list[Path | None] = []
    monkeypatch.setattr(
        decisions,
        "current_worktree_identity",
        lambda: {
            "suffix": "fixture",
            "path": str(root),
            "branch": "test",
            "head_sha": "deadbeef",
            "session_id": None,
        },
    )

    def unavailable(*, repo_root=None, **_kwargs):
        observed.append(repo_root)
        raise SubstrateError("ledger unavailable")

    monkeypatch.setattr(decisions, "require_ledger_connection", unavailable)
    deferred = operations._record_coordination_event(
        "demo",
        "proof",
        payload={"command": "test"},
        repo_root=root,
    )
    assert deferred["status"] == "deferred"

    class Connection:
        def close(self) -> None:
            pass

    def available(*, repo_root=None, **_kwargs):
        observed.append(repo_root)
        return Connection()

    monkeypatch.setattr(decisions, "require_ledger_connection", available)
    monkeypatch.setattr(
        decisions,
        "record_coordination_event",
        lambda conn, **kwargs: {"event_id": 1, "occurred_at": "now"},
    )

    replayed = operations.sync_closure_intents(root, "demo")

    assert replayed["ok"] is True
    assert replayed["pending"] == 0
    assert observed == [root, root]
