"""Newcomer papercuts in `okg install` and `okg deployment bootstrap`.

PACT newcomer-papercuts. Each test failed on dev before the fix:

* #2608 -- the academic-researcher installer ignored `--deployment-name` and
  picked a random roster name.
* #2277 -- unset init answers were stored as the string "None", and the
  human-readable dry-run plan printed `orcid: None`.
* #2338 -- `--embed-worker` resolved the deployment without the install's
  `--deployments-root` and crashed with an uncaught FileNotFoundError.
* #2562 -- `deployment bootstrap --deployment` help said "name" while every
  step reads the value as a directory path.

None of these touch Docker, Postgres or the network.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli.profile_cli_loader import get_profile_cli
from okg.substrate.deployment_bootstrap.profile_init import (
    discover_profile,
    redacted_init_answers,
)

pi_cli = get_profile_cli("academic-researcher").install


@pytest.fixture
def _reachable_docker(monkeypatch) -> None:
    monkeypatch.setattr(
        pi_cli,
        "_docker_state",
        lambda: {
            "docker_on_path": True,
            "daemon_reachable": True,
            "image_present": True,
            "image_build_needed": False,
        },
    )


def _install_args(argv: list[str]) -> argparse.Namespace:
    return cli._build_parser(["install"]).parse_args(argv)


# ---------- #2608: --deployment-name names the deployment ----------


@pytest.mark.usefixtures("_reachable_docker")
def test_deployment_name_names_the_academic_researcher_deployment(
    tmp_path: Path, capsys,
) -> None:
    args = _install_args([
        "install", "--profile", "academic-researcher",
        "--deployment-name", "demo-name",
        "--deployments-root", str(tmp_path / "deps"),
        "--dry-run", "--no-claude-config",
    ])
    rc = pi_cli._cmd_install(args, as_json=True)
    captured = capsys.readouterr()
    plan = json.loads(captured.out)

    assert rc == 0, plan
    assert plan["deployment_name"] == "demo-name"
    assert "randomly picked" not in captured.err


@pytest.mark.usefixtures("_reachable_docker")
def test_conflicting_name_and_deployment_name_are_refused(
    tmp_path: Path, capsys,
) -> None:
    args = _install_args([
        "install", "--profile", "academic-researcher",
        "--name", "one", "--deployment-name", "two",
        "--deployments-root", str(tmp_path / "deps"),
        "--dry-run", "--no-claude-config",
    ])
    rc = pi_cli._cmd_install(args, as_json=True)
    captured = capsys.readouterr()

    assert rc != 0
    # The refusal is the coded `--json` envelope, on stderr.
    error = json.loads(captured.err)["error"]
    assert error["code"] == "substrate_error"
    assert error["data"] == {"name": "one", "deployment_name": "two"}
    assert "--deployment-name" in error["fix_hint"]
    assert captured.out == ""
    assert not (tmp_path / "deps").exists()


# ---------- #2277: unset answers stay unset ----------


def test_unset_init_answers_are_stored_as_null_not_the_string_none() -> None:
    profile = discover_profile("academic-researcher")
    answers = {
        "deployment_name": "t1",
        "orcid": None,
        "pi_name": None,
        "dblp_pid": None,
        "group_webpage": None,
        "slack_channels": None,
        "lab_talks_url": "https://lab.example.org/talks",
        "calendar_ics": (
            "https://calendar.example.org/lab.ics?token=fake-calendar-secret-0001"
        ),
    }

    stored = redacted_init_answers(profile, answers)

    for key in ("orcid", "pi_name", "dblp_pid", "group_webpage", "slack_channels"):
        assert stored[key] is None, (key, stored[key])
    # A value that is set still passes through.
    assert stored["deployment_name"] == "t1"
    assert stored["lab_talks_url"] == "https://lab.example.org/talks"
    # A set value carrying a secret is still redacted.
    assert "fake-calendar-secret-0001" not in stored["calendar_ics"]
    assert stored["calendar_ics"].endswith("token=<redacted>")


def test_text_plan_prints_unset_values_as_not_set(capsys) -> None:
    pi_cli._emit({"orcid": None, "deployment_name": "t1"}, as_json=False)
    out = capsys.readouterr().out

    assert "orcid: (not set)" in out
    assert "None" not in out


# ---------- #2338: --embed-worker honours --deployments-root ----------


def _scaffold(root: Path, name: str) -> Path:
    dep_dir = root / name
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(f"name: {name}\n")
    return dep_dir


def test_embed_worker_finds_the_deployment_under_the_named_root(
    tmp_path: Path, monkeypatch,
) -> None:
    root = tmp_path / "deps"
    dep_dir = _scaffold(root, "t1")
    spawned: list[dict] = []

    class _Proc:
        pid = 4242

    def _popen(argv, **kwargs):
        spawned.append({"argv": argv, "env": kwargs.get("env")})
        return _Proc()

    monkeypatch.setattr(pi_cli.subprocess, "Popen", _popen)

    outcome = pi_cli._maybe_start_embed_worker(
        deployment_name="t1", repo_root=None, enabled=True,
        deployments_root=root,
    )

    assert outcome["started"] is True, outcome
    assert outcome["logfile"] == str(dep_dir / "logs" / "runtime-worker.log")
    # The worker resolves `--deployment t1` itself, under the same root.
    assert spawned[0]["argv"][-2:] == ["--deployment", "t1"]
    assert spawned[0]["env"]["OKG_DEPLOYMENTS_DIR"] == str(root)


def test_embed_worker_missing_deployment_is_a_typed_refusal(
    tmp_path: Path, monkeypatch,
) -> None:
    root = tmp_path / "deps"
    root.mkdir()

    def _popen(*_args, **_kwargs):
        raise AssertionError("no worker may start for a missing deployment")

    monkeypatch.setattr(pi_cli.subprocess, "Popen", _popen)

    outcome = pi_cli._maybe_start_embed_worker(
        deployment_name="absent", repo_root=None, enabled=True,
        deployments_root=root,
    )

    assert outcome["started"] is False
    assert outcome["reason"] == "deployment_not_found"
    assert "absent" in outcome["error"]


def test_install_with_embed_worker_starts_the_worker_under_the_named_root(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """The whole install path, not the helper alone.

    Only the scaffold child, the database step and the process start are
    stubbed. On dev the worker lookup ignored `--deployments-root` and this
    install died with FileNotFoundError before any worker started.
    """
    root = (tmp_path / "owned").resolve()
    user_home = tmp_path / "user-home"
    user_home.mkdir()
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: None)
    monkeypatch.setattr(pi_cli, "_derive_pi_name_from_git", lambda: "Test PI")
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli, "_write_deployment_docker_block", lambda *a, **k: None,
    )
    monkeypatch.setattr(
        pi_cli,
        "_retire_legacy_storage_and_migrate",
        lambda *_a, **_kw: {"ran": True, "status": "ok", "retirement_plan": {}},
    )

    def _scaffold(bootstrap_args, *, as_json):  # noqa: ANN001
        target = bootstrap_args.deployments_root_resolved / "curie"
        target.mkdir(parents=True)
        (target / "deployment.yaml").write_text("name: curie\n", encoding="utf-8")
        (target / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
        bootstrap_args.postgres_port = 15435
        return 0

    monkeypatch.setattr(pi_cli, "_provision_postgres_and_scaffold", _scaffold)

    spawned: list[dict] = []

    class _Proc:
        pid = 4242

    def _popen(argv, **kwargs):
        spawned.append({"argv": list(argv), "env": kwargs.get("env")})
        return _Proc()

    monkeypatch.setattr(pi_cli.subprocess, "Popen", _popen)

    args = _install_args([
        "install", "--profile", "academic-researcher", "--name", "curie",
        "--deployments-root", str(root), "--embed-worker",
        "--no-claude-config", "--mcp-port", "15431",
    ])
    rc = pi_cli._cmd_install(args, as_json=True)
    captured = capsys.readouterr()

    assert rc == 0, captured.err
    payload = json.loads(captured.out)
    assert payload["embed_worker"]["started"] is True, payload["embed_worker"]
    workers = [s for s in spawned if "runtime" in s["argv"]]
    assert len(workers) == 1, spawned
    assert workers[0]["argv"][-2:] == ["--deployment", "curie"]
    assert workers[0]["env"]["OKG_DEPLOYMENTS_DIR"] == str(root)
    assert (root / "curie" / "logs").is_dir()


# ---------- #2562: bootstrap --deployment help says directory ----------


@pytest.mark.parametrize(
    "step", ["plan", "inspect-source", "draft", "verify", "apply", "status"],
)
def test_bootstrap_deployment_help_says_directory(step: str, capsys) -> None:
    parser = cli._build_parser(["deployment", "bootstrap", step])
    with pytest.raises(SystemExit):
        parser.parse_args(["deployment", "bootstrap", step, "--help"])
    help_text = " ".join(capsys.readouterr().out.split())

    assert "Deployment directory" in help_text
    assert "Deployment name (manifest-resolved DSN)" not in help_text
