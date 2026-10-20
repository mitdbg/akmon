"""Commands that act on a deployment never fall back to a built-in name.

They used to default to the development repository's own workspace graph. A
public user has no such deployment, so each now takes, in order, the
deployment passed, ``OKG_DEPLOYMENT``, or the checkout's configured
``graph_projection.deployment`` -- and refuses with
``deployment_not_specified`` naming all three when none is set (#2823).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.deployment_default import (
    DeploymentNotSpecified,
    configured_default_deployment,
    resolve_command_deployment,
)
from okg.substrate.errors import SubstrateError

BUILT_IN = "okg-" + "workspace"


def _configure(root: Path, deployment: str) -> Path:
    config = root / ".okg" / "coordination.yaml"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        f"graph_projection:\n  deployment: {deployment}\n", encoding="utf-8"
    )
    return root


@pytest.fixture
def bare_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A working directory with no deployment named anywhere."""
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_the_order_is_explicit_then_environment_then_configured(
    bare_checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nested = _configure(bare_checkout, "configured") / "a" / "b"
    nested.mkdir(parents=True)

    assert resolve_command_deployment("given", command="c", start=nested) == "given"
    assert resolve_command_deployment(None, command="c", start=nested) == "configured"
    monkeypatch.setenv("OKG_DEPLOYMENT", "from-env")
    assert resolve_command_deployment(None, command="c", start=nested) == "from-env"
    assert resolve_command_deployment("given", command="c", start=nested) == "given"


def test_nothing_configured_refuses_and_names_every_way_to_set_one(
    bare_checkout: Path,
) -> None:
    assert configured_default_deployment(bare_checkout) is None
    with pytest.raises(DeploymentNotSpecified) as raised:
        resolve_command_deployment(None, command="okg example", start=bare_checkout)

    error = raised.value
    assert error.code == "deployment_not_specified"
    assert error.data == {"command": "okg example"}
    assert str(error) == "okg example needs a deployment and none was given or configured"
    for way in ("--deployment", "OKG_DEPLOYMENT", "graph_projection.deployment"):
        assert way in (error.fix_hint or "")


def test_a_malformed_configured_default_is_loud_not_skipped(bare_checkout: Path) -> None:
    config = bare_checkout / ".okg" / "coordination.yaml"
    config.parent.mkdir()
    config.write_text("graph_projection:\n  deployment: ''\n", encoding="utf-8")

    with pytest.raises(SubstrateError) as raised:
        resolve_command_deployment(None, command="c", start=bare_checkout)
    assert not isinstance(raised.value, DeploymentNotSpecified)
    assert "graph_projection.deployment" in str(raised.value)


@pytest.mark.parametrize(
    ("argv", "command"),
    [
        (["dogfood", "status"], "okg dogfood status"),
        (["dogfood", "up"], "okg dogfood up"),
        (
            ["dogfood", "service", "render", "--adapter", "docker-compose", "--repo-root", "."],
            "okg dogfood service",
        ),
        (["deployment", "fork-worktree"], "okg deployment fork-worktree"),
        (["deployment", "gc-forks"], "okg deployment gc-forks"),
        (["doctor", "--check", "dsn-agreement"], "okg doctor --check dsn-agreement"),
        (
            ["jobs", "find-source-run", "--dsn", "postgresql://u@127.0.0.1:1/okg_x",
             "--run-key", "k"],
            "okg jobs find-source-run",
        ),
        (
            ["jobs", "enqueue-source", "--apply", "--dsn",
             "postgresql://u@127.0.0.1:1/okg_x", "--source", "s"],
            "okg jobs enqueue-source",
        ),
        (["dogfood", "down"], "okg dogfood down"),
        (["dogfood", "restart"], "okg dogfood restart"),
        (["dogfood", "doctor"], "okg dogfood doctor"),
    ],
)
def test_each_command_refuses_without_a_deployment(
    bare_checkout: Path,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    command: str,
) -> None:
    rc = main([*argv, "--json"])

    captured = capsys.readouterr()
    assert rc != 0
    error = json.loads(captured.err or captured.out)["error"]
    assert error["code"] == "deployment_not_specified"
    assert error["data"] == {"command": command}
    assert BUILT_IN not in captured.out + captured.err


def test_pact_coordination_setup_plan_refuses_without_a_deployment(
    bare_checkout: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(["pact", "--root", str(bare_checkout), "coord", "setup", "plan", "--json"])

    captured = capsys.readouterr()
    assert rc != 0
    error = json.loads(captured.out or captured.err)["error"]
    assert error["code"] == "deployment_not_specified"
    assert error["data"]["command"] == "okg pact coord setup plan"
    assert "OKG_PACT_GRAPH_DEPLOYMENT" in error["fix_hint"]
    assert BUILT_IN not in captured.out + captured.err


def test_pact_coordination_setup_plan_ignores_the_agent_target_deployment(
    bare_checkout: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Launchers set OKG_DEPLOYMENT to an agent's target; coordination is the
    repository's own PACT graph deployment and must not follow it."""
    monkeypatch.setenv("OKG_DEPLOYMENT", "agent-target")

    rc = main(["pact", "--root", str(bare_checkout), "coord", "setup", "plan", "--json"])

    captured = capsys.readouterr()
    assert rc != 0
    assert json.loads(captured.out or captured.err)["error"]["code"] == (
        "deployment_not_specified"
    )


def test_the_configured_default_reaches_the_command(
    bare_checkout: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Internal users keep working through their checkout's configuration."""
    _configure(bare_checkout, "configured-graph")
    seen: list[str] = []

    def agreement(deployment: str) -> dict:
        seen.append(deployment)
        return {"ok": True, "deployment": deployment}

    monkeypatch.setattr(
        "okg.substrate.deployment_agreement.run_agreement_check", agreement
    )

    assert main(["doctor", "--check", "dsn-agreement", "--json"]) == 0
    assert seen == ["configured-graph"]
