"""`okg install` honours the deployments root it was given, and says so.

Issue #2127.

* **F9.** `--deployments-root <dir>` is declared on the `install` parser and
  the generic scaffold path honours it, but the academic-researcher installer derived
  every path from the user-state XDG root instead. The dry-run plan therefore
  reported `scaffold_target: ~/.local/share/okg/deployments/<name>` while the
  operator had named another directory, and nothing warned. In the same area,
  `OKG_DEPLOYMENTS_DIR` pointing at a directory that does not exist escaped as
  an unhandled `DeploymentResolutionError` traceback -- under `--json` too,
  which `docs/operate/deployments/external.md` tells operators to parse.
* **F21.** The dry run printed the operator's Claude Desktop configuration
  path and the server keys it would touch without saying that step can be
  declined with `--no-claude-config`.

Most of these run against the plan emitter, which by contract writes nothing,
starts no container and opens no database connection. Docker probing is
stubbed because the plan's Docker findings are not what is under test here
and a laptop without a running daemon would otherwise turn every assertion
into a blocker report.

Two do NOT: a plan is not an install, and the claim under test is that the
deployment LANDS in the named root. `test_a_real_install_lands_in_the_named
_root` drives the scaffold itself with the container and publish stages
stubbed, and `test_reset_finds_a_deployment_in_the_named_root` proves the
teardown path resolves the same root -- it was the one default-root read
left in the install path.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli.profile_cli_loader import get_profile_cli

pi_cli = get_profile_cli("academic-researcher").install


@pytest.fixture(autouse=True)
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


def _plan(argv: list[str], capsys) -> tuple[int, dict]:
    args = cli._build_parser(["install"]).parse_args(argv)
    rc = pi_cli._cmd_install(args, as_json=True)
    return rc, json.loads(capsys.readouterr().out)


def test_dry_run_plans_into_the_named_deployments_root(
    tmp_path: Path, capsys,
) -> None:
    root = tmp_path / "owned-portfolio"

    rc, plan = _plan([
        "install", "--profile", "academic-researcher", "--name", "namedroot",
        "--deployments-root", str(root), "--dry-run", "--no-claude-config",
    ], capsys)

    assert rc == 0, plan
    # The regression itself: this used to report the user-state default.
    assert plan["scaffold_target"] == str(root / "namedroot")
    assert plan["deployments_root"] == str(root)
    assert plan["deployments_root_source"] == "explicit"
    # The plan's own next step must reproduce the plan, root included.
    assert str(root) in plan["next_step"]
    # A plan writes nothing, including the root it would create.
    assert not root.exists()


def test_dry_run_plans_into_the_environment_root(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """`OKG_DEPLOYMENTS_DIR` is the documented external-portfolio route."""
    root = tmp_path / "external"
    root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    rc, plan = _plan([
        "install", "--profile", "academic-researcher", "--name", "envroot",
        "--dry-run", "--no-claude-config",
    ], capsys)

    assert rc == 0, plan
    assert plan["scaffold_target"] == str(root / "envroot")
    assert plan["deployments_root_source"] == "environment"


def test_an_existing_deployment_elsewhere_is_not_a_collision(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """With a root named, only that root decides whether the name is taken.

    Reporting a same-named user-state deployment as a collision would block
    an install that is going somewhere else entirely.
    """
    user_state = tmp_path / "user-state"
    (user_state / "twin").mkdir(parents=True)
    (user_state / "twin" / "deployment.yaml").write_text("name: twin\n")
    monkeypatch.setattr(
        "okg.substrate.deployments.user_state_deployments_root",
        lambda: user_state,
    )
    root = tmp_path / "owned"

    rc, plan = _plan([
        "install", "--profile", "academic-researcher", "--name", "twin",
        "--deployments-root", str(root), "--dry-run", "--no-claude-config",
    ], capsys)

    assert rc == 0, plan
    assert plan["scaffold_collision"] is False
    assert plan["scaffold_target"] == str(root / "twin")


def test_dry_run_says_the_claude_desktop_step_is_optional(
    tmp_path: Path, capsys,
) -> None:
    rc, plan = _plan([
        "install", "--profile", "academic-researcher", "--name", "optional",
        "--deployments-root", str(tmp_path / "root"), "--dry-run",
    ], capsys)

    assert rc == 0, plan
    desktop = plan["claude_desktop"]
    assert desktop["enabled"] is True
    assert desktop["optional"] is True
    assert desktop["skip_with"] == "--no-claude-config"
    assert "--no-claude-config" in desktop["note"]


def test_missing_environment_root_refuses_in_the_json_envelope(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """The refusal stays; only its shape changes.

    A misspelled `OKG_DEPLOYMENTS_DIR` must still stop the install -- silently
    creating a second deployments root would be worse than refusing. What it
    must not do is leave the `--json` caller with a traceback and no code.
    """
    missing = tmp_path / "not-created-yet"
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(missing))

    args = cli._build_parser(["install"]).parse_args([
        "install", "--profile", "academic-researcher", "--name", "envmissing",
        "--dry-run", "--no-claude-config",
    ])
    rc = pi_cli._cmd_install(args, as_json=True)
    captured = capsys.readouterr()

    assert rc == 1
    # The CANONICAL `--json` contract, not a second one invented here:
    # `cli/main.py` puts the error envelope on STDERR, sorts its keys, and
    # runs it through `redact_value`. A consumer parsing stderr used to get
    # nothing, and one parsing stdout found an error where the success
    # payload belongs.
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"]["code"] == "deployment_not_found"
    assert str(missing) in payload["error"]["message"]
    assert "OKG_DEPLOYMENTS_DIR" in payload["error"]["fix_hint"]
    assert payload["error"]["data"]["env_var"] == "OKG_DEPLOYMENTS_DIR"
    assert captured.err == json.dumps(payload, sort_keys=True) + "\n"
    assert not missing.exists()


def test_the_refusal_text_mode_keeps_the_code_and_the_data(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Text mode is the other half of the same contract.

    `_text_error` prints `error [<code>]: <message>`, the fix hint and the
    `data` block. The first version of this refusal printed a bare
    `error: <message>` with no code, so an operator reading the terminal
    could not name what refused them.
    """
    missing = tmp_path / "still-not-created"
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(missing))

    args = cli._build_parser(["install"]).parse_args([
        "install", "--profile", "academic-researcher", "--name", "envtext",
        "--dry-run", "--no-claude-config",
    ])
    rc = pi_cli._cmd_install(args, as_json=False)
    captured = capsys.readouterr()

    assert rc == 1
    assert captured.out == ""
    assert "error [deployment_not_found]:" in captured.err
    assert "OKG_DEPLOYMENTS_DIR" in captured.err
    assert "data:" in captured.err


def test_explicit_root_beats_the_environment_root(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Same precedence as the generic scaffold path in `cli/init.py`."""
    env_root = tmp_path / "env"
    env_root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(env_root))
    flag_root = tmp_path / "flag"

    rc, plan = _plan([
        "install", "--profile", "academic-researcher", "--name", "precedence",
        "--deployments-root", str(flag_root), "--dry-run",
        "--no-claude-config",
    ], capsys)

    assert rc == 0, plan
    assert plan["scaffold_target"] == str(flag_root / "precedence")


# ---------------------------------------------------------------------------
# Beyond the plan: the deployment has to LAND in the named root
# ---------------------------------------------------------------------------


def _install_args(**overrides):
    from types import SimpleNamespace

    defaults = {
        "orcid": None,
        "name": None,
        "pi_name": "Test PI",
        "postgres_port": None,
        "mcp_port": 15431,
        "no_claude_config": True,
        "claude_config_only": False,
        "dry_run": False,
        "git_repos": None,
        "dblp_pid": None,
        "embed_worker": False,
        "deployments_root": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_the_scaffold_child_is_told_the_named_root(
    tmp_path: Path, monkeypatch,
) -> None:
    """The one hop a stubbed provision step cannot prove.

    `_provision_postgres_and_scaffold` spawns the generic scaffold as a
    child process. If `--deployments-root` is not in that argv, the child
    resolves the default cascade and the deployment lands in user state no
    matter what the plan said. `--use-local-postgres` is what keeps this a
    unit test: it skips every Docker step, leaving the scaffold child as the
    only subprocess.
    """
    import argparse

    root = (tmp_path / "owned").resolve()
    recorded: list[list[str]] = []

    class _Done:
        returncode = 0
        stdout = ""
        stderr = ""

    def _record(cmd, **_kw):  # noqa: ANN001
        recorded.append(list(cmd))
        return _Done()

    monkeypatch.setattr(pi_cli.subprocess, "run", _record)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)

    bootstrap = argparse.Namespace(
        deployment_name="landing",
        orcid=None,
        postgres_dsn="postgresql://okg@127.0.0.1:59999/unreachable",
        postgres_port=15435,
        use_local_postgres=True,
        mcp_port=15431,
        papers_dir=None,
        cv_path=None,
        group_webpage=None,
        pi_name=None,
        dblp_pid=None,
        git_repos=None,
        reset=False,
        yes=True,
        force=False,
        deployments_root_resolved=root,
        _invoked_from_install=True,
    )
    rc = pi_cli._provision_postgres_and_scaffold(bootstrap, as_json=True)

    assert rc == 0
    scaffold = [c for c in recorded if "--scaffold-only" in c]
    assert len(scaffold) == 1, recorded
    argv = scaffold[0]
    assert "--deployments-root" in argv
    assert argv[argv.index("--deployments-root") + 1] == str(root)


def test_a_real_install_lands_in_the_named_root(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Not a plan: `--dry-run` is off and the install runs to completion.

    The scaffold step itself is replaced by a stub that writes what the
    child would write -- into the root it was HANDED, and it asserts it was
    handed the operator's root rather than deriving one. Everything after
    that is the real install path: the existing-deployment lookup, the
    `docker:` block write, the aggregate-count read and the final payload.
    Each of those used to resolve `user_state_deployments_root()` directly,
    which is why the user-state root here is a separate empty directory the
    test asserts nothing ever reached.
    """
    root = (tmp_path / "owned-portfolio").resolve()
    user_home = tmp_path / "user-home"
    user_home.mkdir()
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli, "_derive_name_from_random", lambda: "curie")
    monkeypatch.setattr(pi_cli, "_derive_pi_name_from_git", lambda: "Test PI")
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli, "_write_deployment_docker_block", lambda *a, **k: None,
    )
    monkeypatch.setattr(
        pi_cli,
        "_maybe_start_embed_worker",
        lambda **_kw: {"started": False, "reason": "not_requested"},
    )
    # The database step is not under test here; it has its own tests.
    monkeypatch.setattr(
        pi_cli,
        "_retire_legacy_storage_and_migrate",
        lambda *_a, **_kw: {"ran": True, "status": "ok", "retirement_plan": {}},
    )

    def _scaffold(bootstrap_args, *, as_json):  # noqa: ANN001
        assert bootstrap_args.deployments_root_resolved == root
        target = bootstrap_args.deployments_root_resolved / "curie"
        target.mkdir(parents=True)
        (target / "deployment.yaml").write_text(
            "name: curie\nrelease:\n  channel: dev\n  kind: standalone\n",
            encoding="utf-8",
        )
        (target / "source_registry.yaml").write_text(
            "sources: {}\n", encoding="utf-8",
        )
        bootstrap_args.postgres_port = 15435
        return 0

    monkeypatch.setattr(
        pi_cli, "_provision_postgres_and_scaffold", _scaffold,
    )

    rc = pi_cli._cmd_install(
        _install_args(deployments_root=str(root)), as_json=True,
    )

    assert rc == 0, capsys.readouterr()
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "install_complete"
    assert payload["deployment_dir"] == str(root / "curie")
    assert (root / "curie" / "deployment.yaml").is_file()
    # The default root never saw this install.
    assert list(user_home.rglob("deployment.yaml")) == []
    # A deployment outside the default root is unreachable by name until the
    # resolver is told where to look, so the export leads the handoff.
    assert payload["next_steps"][0] == f"export OKG_DEPLOYMENTS_DIR={root}"
    # Issue #2331: the install publishes an empty graph, so the next step
    # names the exact command that adds a first source.
    add_source = (
        "okg deployment add-source curie notes --type local-folder "
        "--setting root=/absolute/path/to/folder --apply --yes"
    )
    assert payload["next_steps"][1] == add_source
    assert add_source in payload["setup_message"]


def test_reset_destroys_the_deployment_in_the_named_root_only(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """The teardown path reads the same root as the install path.

    `okg install --reset --deployments-root <dir>` resolved the deployment
    through the DEFAULT cascade while every other install path honoured the
    operator's root. With a same-named deployment in user state -- the
    ordinary case for an operator trying a second portfolio -- that is a
    destructive misresolution: `shutil.rmtree` on the wrong directory. The
    two twins here are what make the difference observable.
    """
    import argparse

    from okg.substrate.library.agent_runtimes import registry

    root = (tmp_path / "owned").resolve()
    named = root / "twin"
    named.mkdir(parents=True)
    (named / "deployment.yaml").write_text("name: twin\n", encoding="utf-8")

    user_home = tmp_path / "user-home"
    default_twin = user_home / "deployments" / "twin"
    default_twin.mkdir(parents=True)
    (default_twin / "deployment.yaml").write_text(
        "name: twin\n", encoding="utf-8",
    )
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pi_cli.shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        registry, "unregister_from_all", lambda *, deployment: [],
    )
    # `_provision_postgres_and_scaffold` does not forward `--yes` to the
    # reset helper, so the interactive confirmation still runs here. That
    # gap is pre-existing and reported separately; answering it keeps this
    # test on the real call path instead of reaching past it.
    monkeypatch.setattr("builtins.input", lambda *_a: "destroy")

    bootstrap = argparse.Namespace(
        deployment_name="twin",
        orcid=None,
        postgres_dsn=None,
        postgres_port=None,
        use_local_postgres=False,
        mcp_port=15431,
        papers_dir=None,
        cv_path=None,
        group_webpage=None,
        pi_name=None,
        dblp_pid=None,
        git_repos=None,
        reset=True,
        yes=True,
        force=False,
        deployments_root_resolved=root,
        _invoked_from_install=True,
    )
    rc = pi_cli._provision_postgres_and_scaffold(bootstrap, as_json=True)

    assert rc == 0, capsys.readouterr()
    assert not named.exists()
    assert default_twin.is_dir()
