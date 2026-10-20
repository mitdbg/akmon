"""Tests for `okg install` helpers + parser wiring.

Per `streamline-academic-pi-install` Phase 4.
"""
from __future__ import annotations

import socket
from pathlib import Path

from okg.substrate.cli.profile_cli_loader import get_profile_cli
from okg.substrate.library.agent_runtimes import (
    RegisterReport, RegisterResult,
)

_pi_cli_pkg = get_profile_cli("academic-researcher")
pi_cli = _pi_cli_pkg.install
_derive_name_from_orcid = pi_cli._derive_name_from_orcid
_pick_free_port = pi_cli._pick_free_port
DISPATCH = _pi_cli_pkg.DISPATCH
#: `install` moved out of `build_parsers` when the substrate took ownership
#: of the verb (PACT define-newcomer-entry-path). The profile keeps a
#: standalone builder for direct callers like these tests, which exercise
#: academic-researcher's own contribution rather than the merged substrate parser.
build_parsers = pi_cli._build_install_parser


def test_derive_name_uses_last_8_orcid_digits() -> None:
    assert _derive_name_from_orcid(
        "0000-0002-1234-5678",
    ) == "pi_1234_5678"
    assert _derive_name_from_orcid(
        "https://orcid.org/0000-0001-9876-5432",
    ) == "pi_9876_5432"


def test_derive_name_falls_back_when_orcid_missing() -> None:
    assert _derive_name_from_orcid(None) == "pi_default"
    assert _derive_name_from_orcid("") == "pi_default"
    assert _derive_name_from_orcid("not-an-orcid") == "pi_default"


def test_pick_free_port_returns_starting_port_when_free() -> None:
    # Use a high port unlikely to be in use.
    port = _pick_free_port(starting_at=49152)
    # Bind to verify it really was free.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", port))


def test_pick_free_port_skips_bound_port() -> None:
    # Bind a port, then ask for that port and confirm we get the
    # next one.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(("127.0.0.1", 0))
        port = occupied.getsockname()[1]
        result = _pick_free_port(starting_at=port)
        assert result > port


def test_install_is_reachable_from_the_substrate() -> None:
    """`install` is substrate-owned and routes back to this profile.

    It used to live in academic-researcher's DISPATCH. Per PACT
    `define-newcomer-entry-path` the substrate owns the one documented way
    in, so the profile now opts in through `INSTALL_EXTENSION` instead —
    and it must still be the profile's own installer that runs.
    """
    from okg.substrate.cli.profile_cli_loader import (
        discover_install_extensions,
    )

    assert "install" not in DISPATCH
    extensions = discover_install_extensions()
    assert "academic-researcher" in extensions
    assert extensions["academic-researcher"] is _pi_cli_pkg.INSTALL_EXTENSION


def test_install_parser_is_buildable() -> None:
    """The install subparser SHALL register without crashing and
    accept all args including the optional --orcid.

    Per `dead-simple-install` (2026-05-21): --orcid is OPTIONAL.
    Without it, the deployment name derives from hostname and
    the dblp_by_orcid source falls back to name-based search.
    """
    import argparse
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    build_parsers(sub)

    # No args required — install parses cleanly with no flags.
    args = parser.parse_args(["install"])
    assert args.cmd == "install"
    assert args.orcid is None
    # With --orcid → parses.
    args = parser.parse_args([
        "install", "--orcid", "0000-0002-1234-5678",
    ])
    assert args.orcid == "0000-0002-1234-5678"
    assert args.name is None
    assert args.postgres_port is None
    assert args.mcp_port is None
    assert args.no_claude_config is False
    assert args.dry_run is False


def test_agent_runtime_registration_extra_env_includes_role_dsns(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_MCP_RO_DSN", "postgres://managed/ro")
    monkeypatch.setenv("OKG_MCP_RW_DSN", "postgres://managed/rw")

    assert pi_cli._agent_runtime_registration_extra_env() == {
        "OKG_MCP_ALLOW_WRITES": "1",
        "OKG_MCP_RO_DSN": "postgres://managed/ro",
        "OKG_MCP_RW_DSN": "postgres://managed/rw",
    }


def test_install_dry_run_flag_parses() -> None:
    """--dry-run is a `store_true` flag (no value)."""
    import argparse
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    build_parsers(sub)
    args = parser.parse_args([
        "install", "--orcid", "0000-0002-1825-0097", "--dry-run",
    ])
    assert args.dry_run is True


def test_install_dry_run_emits_plan_on_fresh_target(
    capsys, tmp_path, monkeypatch,
) -> None:
    """Dry-run on a non-colliding target: status=dry_run_ok, blockers
    empty, sources list non-empty, no files written, no docker
    state mutated.

    Re-points _find_repo_root() at tmp_path/repo so the scaffold
    collision check tests `tmp_path/repo/deployments/<name>/` (which
    doesn't exist) rather than the real worktree.
    """
    fake_repo = tmp_path / "repo"
    (fake_repo / "src" / "okg").mkdir(parents=True)
    (fake_repo / "deployments").mkdir()

    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: fake_repo)
    # Stub Docker probe to avoid the real shell-out + report-state.
    monkeypatch.setattr(
        pi_cli, "_docker_state",
        lambda: {
            "docker_on_path": True,
            "daemon_reachable": True,
            "image_present": True,
            "image_build_needed": False,
        },
    )

    # SimpleNamespace, not MagicMock — MagicMock special-cases the
    # `name` kwarg (sets the mock's own name) and would leak the
    # MagicMock object through instead of our deployment slug.
    from types import SimpleNamespace
    args = SimpleNamespace(
        orcid="0000-0002-1825-0097",
        name="dryrun_smoke",
        postgres_port=15435,
        mcp_port=15431,
        no_claude_config=True,  # don't read Claude config in the test
        claude_config_only=False,
        dry_run=True,
    )
    rc = pi_cli._cmd_install(args, as_json=True)
    assert rc == 0

    out = capsys.readouterr().out
    import json as _json
    plan = _json.loads(out.strip())
    assert plan["status"] == "dry_run_ok"
    assert plan["deployment_name"] == "dryrun_smoke"
    assert plan["scaffold_collision"] is False
    assert plan["ports"] == {"postgres": 15435, "mcp": 15431}
    assert plan["sources"]["total"] > 0
    assert plan["blockers"] == []
    assert "next_step" in plan
    # No scaffold should have been created.
    assert not (fake_repo / "deployments" / "dryrun_smoke").exists()


def test_resolve_git_repos_returns_explicit_when_provided() -> None:
    """If the operator passes --git-repos, the helper SHALL return
    it verbatim."""
    out = pi_cli._resolve_git_repos(explicit="/some/operator/path")
    assert out == "/some/operator/path"


def test_resolve_git_repos_does_not_infer_cwd_when_implicit(
    monkeypatch,
) -> None:
    """Wizard-first install SHALL NOT infer the OKG framework
    checkout as the operator's project repo."""
    def _fake_run(*_a, **_kw):  # noqa: ANN001
        raise AssertionError("git rev-parse should not be called")

    monkeypatch.setattr(pi_cli.subprocess, "run", _fake_run)
    out = pi_cli._resolve_git_repos(explicit=None)
    assert out is None


def test_install_argv_includes_git_repos_when_resolved() -> None:
    """The init-argv builder SHALL pass --git-repos to `okg init`
    when bootstrap_args.git_repos is set (the install code threads
    the resolved value through bootstrap_args)."""
    import argparse
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    build_parsers(sub)
    # The install parser SHALL expose --git-repos so tests like
    # test_install_dry_run_emits_plan_on_fresh_target can probe
    # the SimpleNamespace without AttributeError.
    args = parser.parse_args([
        "install", "--orcid", "0000-0002-1234-5678",
        "--git-repos", "/explicit/repo",
    ])
    assert args.git_repos == "/explicit/repo"


def test_install_bootstrap_does_not_run_filtered_ingest(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Wizard-first install SHALL bootstrap and report handoff without
    running the old filtered first-ingest subprocess."""
    fake_repo = tmp_path / "repo"
    (fake_repo / "src" / "okg").mkdir(parents=True)
    (fake_repo / "deployments").mkdir()
    user_home = tmp_path / "user-home"
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: fake_repo)
    monkeypatch.setattr(pi_cli, "_derive_name_from_random", lambda: "curie")
    monkeypatch.setattr(pi_cli, "_derive_pi_name_from_git", lambda: "Test PI")
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        pi_cli,
        "_write_deployment_docker_block",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        pi_cli,
        "_maybe_start_embed_worker",
        lambda **_kw: {"started": False, "reason": "not_requested"},
    )

    def _fake_provision(bootstrap_args, *, as_json):  # noqa: ANN001
        bootstrap_args.postgres_port = 15435
        dep_dir = user_home / "deployments" / "curie"
        dep_dir.mkdir(parents=True)
        (dep_dir / "deployment.yaml").write_text(
            "name: curie\n"
            "profile:\n"
            "  name: academic-researcher\n"
            "  init_answers:\n"
            "    postgres_dsn: postgres://postgres:okg@localhost:15435/okg_curie\n",
            encoding="utf-8",
        )
        (dep_dir / "source_registry.yaml").write_text(
            "sources:\n"
            "  git_files:\n"
            "    ownership_id: curie.git_files\n"
            "    enabled_by_default: false\n"
            "  dblp_by_orcid:\n"
            "    ownership_id: curie.dblp_by_orcid\n"
            "    enabled_by_default: false\n",
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(
        pi_cli, "_provision_postgres_and_scaffold", _fake_provision,
    )

    cleanup_calls: list[object] = []
    cleanup_plan = {
        "mode": "legacy_live_table_retirement",
        "destructive_targets": [
            {
                "relation": "okg.nodes_live",
                "action": "drop_table_restrict",
                "exists": True,
                "row_count": 2,
            },
            {
                "relation": "okg.edges_live",
                "action": "drop_table_restrict",
                "exists": True,
                "row_count": 3,
            },
        ],
        "preserved_relations": [
            {"relation": "okg.node_facts", "exists": True},
        ],
    }

    class _FakeConnection:
        def __enter__(self):  # noqa: ANN204
            cleanup_calls.append("connect_enter")
            return self

        def __exit__(self, *_exc):  # noqa: ANN204, ANN002
            cleanup_calls.append("connect_exit")
            return False

        def commit(self) -> None:
            cleanup_calls.append("commit")

    def _fake_connect(dsn: str, **_kwargs: object):  # noqa: ANN202
        cleanup_calls.append(("connect", dsn))
        return _FakeConnection()

    def _fake_retirement_plan(_conn):  # noqa: ANN001, ANN202
        cleanup_calls.append("plan")
        return cleanup_plan

    def _fake_retire(_conn) -> None:  # noqa: ANN001
        cleanup_calls.append("retire")

    def _fake_migrate(dsn: str, *, deployment_name: str | None = None):  # noqa: ANN202
        cleanup_calls.append(("migrate", dsn, deployment_name))

    import psycopg
    from okg.substrate.db import management as db_management
    from okg.substrate.db import migrate as db_migrate

    monkeypatch.setattr(psycopg, "connect", _fake_connect)
    monkeypatch.setattr(
        db_management,
        "legacy_graph_storage_retirement_plan",
        _fake_retirement_plan,
    )
    monkeypatch.setattr(
        db_management,
        "retire_legacy_graph_storage",
        _fake_retire,
    )
    monkeypatch.setattr(db_migrate, "migrate", _fake_migrate)

    def _no_subprocess(*_a, **_kw):  # noqa: ANN001
        raise AssertionError("install should not run ingest/run subprocesses")

    monkeypatch.setattr(pi_cli.subprocess, "run", _no_subprocess)

    args = _minimal_install_args(
        name=None,
        no_claude_config=True,
        mcp_port=15431,
    )
    rc = pi_cli._cmd_install(args, as_json=True)

    assert rc == 0
    payload = __import__("json").loads(capsys.readouterr().out)
    assert payload["status"] == "install_complete"
    assert payload["data_sources_ingested"] is False
    assert payload["setup_status"] == "awaiting_operator_approval"
    assert payload["enabled_sources"] == []
    assert payload["first_publish_filtered_ingest"]["ran"] is False
    assert payload["chronos_native_graph_cleanup"] == {
        "ran": True,
        "status": "ok",
        "retirement_plan": cleanup_plan,
    }
    assert cleanup_calls[:7] == [
        ("connect", "postgres://postgres:okg@localhost:15435/okg_curie"),
        "connect_enter",
        "plan",
        "retire",
        "commit",
        "connect_exit",
        (
            "migrate",
            "postgres://postgres:okg@localhost:15435/okg_curie",
            "curie",
        ),
    ]


def test_install_exits_1_when_its_migrate_step_fails(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Install used to report `install_complete` and exit 0 when the
    database step under it raised, so a script saw success on a deployment
    that could not publish."""
    fake_repo = tmp_path / "repo"
    (fake_repo / "src" / "okg").mkdir(parents=True)
    (fake_repo / "deployments").mkdir()
    user_home = tmp_path / "user-home"
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: fake_repo)
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

    def _fake_provision(bootstrap_args, *, as_json):  # noqa: ANN001
        bootstrap_args.postgres_port = 15435
        dep_dir = user_home / "deployments" / "curie"
        dep_dir.mkdir(parents=True)
        (dep_dir / "deployment.yaml").write_text(
            "name: curie\n"
            "profile:\n"
            "  name: academic-researcher\n"
            "  init_answers:\n"
            "    postgres_dsn: postgres://postgres:okg@localhost:15435/okg_curie\n",
            encoding="utf-8",
        )
        (dep_dir / "source_registry.yaml").write_text(
            "sources: {}\n", encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(
        pi_cli, "_provision_postgres_and_scaffold", _fake_provision,
    )

    def _refusing_connect(dsn: str, **_kwargs: object):  # noqa: ANN202
        raise RuntimeError("connection refused by the test")

    import psycopg

    monkeypatch.setattr(psycopg, "connect", _refusing_connect)

    def _no_subprocess(*_a, **_kw):  # noqa: ANN001
        raise AssertionError("install should not run ingest/run subprocesses")

    monkeypatch.setattr(pi_cli.subprocess, "run", _no_subprocess)

    rc = pi_cli._cmd_install(
        _minimal_install_args(
            name=None, no_claude_config=True, mcp_port=15431,
        ),
        as_json=True,
    )

    assert rc == 1
    out = capsys.readouterr()
    payload = __import__("json").loads(out.out)
    assert payload["status"] == "install_failed"
    assert payload["failed_stage"] == "chronos_native_graph_cleanup"
    assert "connection refused by the test" in payload["error"]
    assert payload["chronos_native_graph_cleanup"]["status"] == "failed"
    assert "error: install_failed:" in out.err


def test_reset_yes_skips_confirm_prompt(
    tmp_path, monkeypatch, capsys,
) -> None:
    """`okg reset --yes` SHALL bypass the interactive `Type 'destroy'`
    prompt and proceed straight to teardown.

    Reset's argparse already exposes `--yes` (per
    `_build_reset_parser`); this regression-tests that the handler
    actually consults it. Without this fix the flag is dead and
    reset blocks forever in any non-interactive context.
    """
    fake_repo = tmp_path / "repo"
    dep_dir = fake_repo / "deployments" / "yes_smoke"
    dep_dir.mkdir(parents=True)

    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: fake_repo)
    # Stub docker compose teardown so the test doesn't shell out to
    # a real daemon. shutil.which("docker") returns the real path,
    # but subprocess.run is the actual call site we care about.
    captured: list[list[str]] = []
    import subprocess

    def _fake_run(cmd, *a, **kw):  # noqa: ANN001
        captured.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(pi_cli.subprocess, "run", _fake_run)
    # And `input` raises if called — proves `--yes` short-circuited it.
    def _no_input(_prompt=""):  # noqa: ANN001
        raise AssertionError(
            "input() called despite --yes; --yes did not bypass confirm"
        )
    monkeypatch.setattr("builtins.input", _no_input)

    from types import SimpleNamespace
    args = SimpleNamespace(deployment="yes_smoke", yes=True)
    rc = pi_cli._cmd_reset(args, as_json=True)
    assert rc == 0

    # Scaffold dir should be gone.
    assert not dep_dir.exists()
    # docker compose down should have been called.
    assert any(
        cmd[:3] == ["docker", "compose", "-f"] for cmd in captured
    )


def test_reset_without_yes_still_prompts(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Without `--yes`, the confirm prompt SHALL still run.

    Guards against an over-correction that would silently skip
    confirm in interactive contexts.
    """
    fake_repo = tmp_path / "repo"
    dep_dir = fake_repo / "deployments" / "prompt_smoke"
    dep_dir.mkdir(parents=True)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: fake_repo)
    # `input` returns something other than 'destroy' → reset aborts.
    monkeypatch.setattr("builtins.input", lambda _prompt="": "no")

    from types import SimpleNamespace
    args = SimpleNamespace(deployment="prompt_smoke", yes=False)
    rc = pi_cli._cmd_reset(args, as_json=True)
    assert rc == 1
    # Scaffold dir SHALL remain because reset aborted.
    assert dep_dir.exists()


def test_install_dry_run_blocks_on_existing_scaffold(
    capsys, tmp_path, monkeypatch,
) -> None:
    """If the scaffold dir already exists, dry-run returns rc=1 and
    cites the collision in `blockers`."""
    fake_repo = tmp_path / "repo"
    (fake_repo / "src" / "okg").mkdir(parents=True)
    existing = fake_repo / "deployments" / "collide_test"
    existing.mkdir(parents=True)

    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: fake_repo)
    monkeypatch.setattr(
        pi_cli, "_docker_state",
        lambda: {
            "docker_on_path": True,
            "daemon_reachable": True,
            "image_present": True,
            "image_build_needed": False,
        },
    )

    from types import SimpleNamespace
    args = SimpleNamespace(
        orcid="0000-0002-1825-0097",
        name="collide_test",
        postgres_port=15435,
        mcp_port=15431,
        no_claude_config=True,
        claude_config_only=False,
        dry_run=True,
    )
    rc = pi_cli._cmd_install(args, as_json=True)
    assert rc == 1

    out = capsys.readouterr().out
    import json as _json
    plan = _json.loads(out.strip())
    assert plan["status"] == "dry_run_blocked"
    assert plan["scaffold_collision"] is True
    assert any(
        "scaffold target already exists" in b for b in plan["blockers"]
    )


def _write_academic_researcher_deployment(
    user_home: Path, name: str, *, dsn: str | None = None,
    profile: str = "academic-researcher",
) -> Path:
    dep_dir = user_home / "deployments" / name
    dep_dir.mkdir(parents=True, exist_ok=True)
    (dep_dir / "deployment.yaml").write_text(
        "name: {name}\n"
        "profile:\n"
        "  name: {profile}\n"
        "  init_answers:\n"
        "    postgres_dsn: {dsn}\n".format(
            name=name,
            profile=profile,
            dsn=dsn or (
                f"postgres://postgres:okg@localhost:5432/okg_{name}"
            ),
        ),
        encoding="utf-8",
    )
    return dep_dir


def _write_pre_rename_academic_pi_deployment(user_home: Path, name: str) -> Path:
    """A deployment scaffolded before 2026-09-23: it records the retired
    profile name, the old ontology module and the old source package."""
    dep_dir = _write_academic_researcher_deployment(
        user_home, name, profile="academic-pi",
    )
    with (dep_dir / "deployment.yaml").open("a", encoding="utf-8") as fh:
        fh.write(
            "modules:\n"
            "- person\n"
            "- academic_pi\n"
            "enrichers:\n"
            "- class: okg.substrate.library.linkers."
            "academic_pi_collaboration.AcademicPiCollaborationLinker\n"
        )
    (dep_dir / "source_registry.yaml").write_text(
        "sources:\n"
        "  doe_osti:\n"
        "    module: okg.substrate.library.sources.academic_pi.doe_osti\n"
        "    class: DoeOstiSource\n",
        encoding="utf-8",
    )
    (dep_dir / "invariants.yaml").write_text(
        "invariants:\n"
        "  - name: academic_pi_advisee_floor\n"
        "    sql: SELECT 1 FROM okg.v_nodes WHERE subtype = 'advisee'\n",
        encoding="utf-8",
    )
    (dep_dir / "dashboards").mkdir()
    (dep_dir / "dashboards" / "pi-overview.yaml").write_text(
        "title: academic-pi overview\n"
        "panels: [academic_pi_grants]\n",
        encoding="utf-8",
    )
    (dep_dir / "skills" / "nested").mkdir(parents=True)
    (dep_dir / "skills" / "nested" / "okg-setup.md").write_text(
        "Run okg install --profile academic-pi.\n", encoding="utf-8",
    )
    return dep_dir


def _refuse_random_name(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        pi_cli, "_derive_name_from_random",
        lambda: (_ for _ in ()).throw(
            AssertionError("a new deployment must not be created")
        ),
    )


def test_install_without_name_refuses_a_pre_rename_academic_pi_deployment(
    tmp_path, monkeypatch, capsys,
) -> None:
    """A no-name rerun over a deployment still recorded as `academic-pi`
    SHALL fail with `profile_retired` and the one command that rewrites
    the record -- not skip it and scaffold a second deployment beside it.
    Applying that command SHALL make the same rerun reuse the deployment.
    """
    import json as _json
    import shlex
    import subprocess

    user_home = tmp_path / "user-home"
    dep_dir = _write_pre_rename_academic_pi_deployment(user_home, "mendel")
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    _refuse_random_name(monkeypatch)
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)

    rc = pi_cli._cmd_install(_minimal_install_args(), as_json=True)

    assert rc == 1
    envelope = _json.loads(capsys.readouterr().err)["error"]
    assert envelope["code"] == "profile_retired"
    assert envelope["data"]["profile_name"] == "academic-pi"
    assert envelope["data"]["replacement"] == "academic-researcher"
    assert envelope["data"]["deployment_dir"] == str(dep_dir)
    data = envelope["data"]
    rewrite = data["rewrite_command"]
    # The fix is the rewrite, then re-claiming catalog ownership and
    # reloading the catalog (both carry the old catalog fingerprint), then
    # the same install again. `okg migrate` is not a step: it changes
    # neither.
    assert data["claim_command"] == (
        "okg catalog ownership claim --deployment mendel"
    )
    assert data["catalog_load_command"] == (
        "okg catalog load --deployment mendel --apply"
    )
    assert data["reinstall_command"] == (
        "okg install --profile academic-researcher --name mendel"
    )
    assert "migrate" not in envelope["fix_hint"]
    assert envelope["fix_hint"] == (
        f"{rewrite}"
        " && okg catalog ownership claim --deployment mendel"
        " && okg catalog load --deployment mendel --apply"
        " && okg install --profile academic-researcher --name mendel"
    )
    steps = (
        rewrite, data["claim_command"], data["catalog_load_command"],
        data["reinstall_command"],
    )
    positions = [envelope["message"].index(step) for step in steps]
    assert positions == sorted(positions)
    for path in (
        "deployment.yaml", "source_registry.yaml", "invariants.yaml",
        "dashboards/pi-overview.yaml", "skills/nested/okg-setup.md",
    ):
        assert str(dep_dir / path) in rewrite, path
    assert sorted(p.name for p in (user_home / "deployments").iterdir()) == [
        "mendel",
    ]

    # The rewrite is one runnable command, and it is sufficient for install
    # to recognise the deployment again (the other steps need a database).
    subprocess.run(shlex.split(rewrite), check=True)
    for path in dep_dir.rglob("*"):
        if path.is_file():
            text = path.read_text()
            assert "academic-pi" not in text, path
            assert "academic_pi" not in text, path
    assert "name: academic_researcher_advisee_floor" in (
        dep_dir / "invariants.yaml"
    ).read_text()
    rewritten = (dep_dir / "deployment.yaml").read_text()
    assert "name: academic-researcher" in rewritten
    assert "- academic_researcher" in rewritten
    assert (
        "linkers.academic_researcher_collaboration."
        "AcademicResearcherCollaborationLinker"
    ) in rewritten
    assert "sources.academic_researcher.doe_osti" in (
        dep_dir / "source_registry.yaml"
    ).read_text()
    assert pi_cli._user_state_academic_researcher_deployments(
        deployments_root=None,
    ) == [dep_dir]


def test_install_with_name_refuses_a_cs_researcher_deployment(
    tmp_path, monkeypatch, capsys,
) -> None:
    """`--name` naming an existing deployment recorded as the merged
    `cs-researcher` profile SHALL fail the same way, before the
    registration refresh, with a fix that rewrites only the name."""
    import json as _json

    user_home = tmp_path / "user-home"
    dep_dir = _write_academic_researcher_deployment(
        user_home, "hopper", profile="cs-researcher",
    )
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    monkeypatch.setattr(
        "okg.substrate.library.agent_runtimes.registry."
        "register_with_all_detected",
        lambda **_kw: (_ for _ in ()).throw(
            AssertionError("registrations must not be refreshed")
        ),
    )

    rc = pi_cli._cmd_install(
        _minimal_install_args(name="hopper"), as_json=True,
    )

    assert rc == 1
    envelope = _json.loads(capsys.readouterr().err)["error"]
    assert envelope["code"] == "profile_retired"
    assert envelope["data"]["profile_name"] == "cs-researcher"
    assert envelope["fix_hint"] == (
        "perl -pi -e 's/cs-researcher/academic-researcher/g' "
        f"{dep_dir / 'deployment.yaml'}"
        " && okg catalog ownership claim --deployment hopper"
        " && okg catalog load --deployment hopper --apply"
        " && okg install --profile academic-researcher --name hopper"
    )


def test_academic_pi_rewrite_keeps_the_three_stored_identifiers(
    tmp_path,
) -> None:
    """The rewrite renames every `academic_pi` module, path and check name
    but SHALL leave the three identifiers published rows carry: the linker
    name, the review source and the candidate-link schema strings."""
    import shlex
    import subprocess

    sample = tmp_path / "invariants.yaml"
    sample.write_text(
        "modules: [academic_pi]\n"
        "check: academic_pi_advisee_floor\n"
        "linker_class: okg.substrate.library.linkers."
        "academic_pi_collaboration.AcademicPiCollaborationLinker\n"
        "source: okg.substrate.library.sources.academic_pi.candidate_review\n"
        "sql: >-\n"
        "  WHERE linker = 'academic_pi_collaboration'\n"
        "  AND source = '_review:academic_pi_candidate_review'\n"
        "  AND schema IN ('okg.academic_pi.candidate_link.v1',\n"
        "  'okg.academic_pi.candidate_link_decision.v1')\n",
        encoding="utf-8",
    )

    subprocess.run(
        ["perl", "-pi", "-e", pi_cli.ACADEMIC_PI_REWRITE, str(sample)],
        check=True,
    )

    text = sample.read_text()
    assert "modules: [academic_researcher]" in text
    assert "check: academic_researcher_advisee_floor" in text
    assert (
        "linkers.academic_researcher_collaboration."
        "AcademicResearcherCollaborationLinker"
    ) in text
    assert "sources.academic_researcher.candidate_review" in text
    assert "linker = 'academic_pi_collaboration'" in text
    assert "'_review:academic_pi_candidate_review'" in text
    assert "'okg.academic_pi.candidate_link.v1'" in text
    assert "'okg.academic_pi.candidate_link_decision.v1'" in text
    # Nothing else keeps the old spelling.
    for keep in (
        "academic_pi_collaboration'", "academic_pi_candidate_review",
        "okg.academic_pi.candidate_link",
    ):
        text = text.replace(keep, "")
    assert "academic_pi" not in text
    assert shlex.quote(pi_cli.ACADEMIC_PI_REWRITE) == (
        f"'{pi_cli.ACADEMIC_PI_REWRITE}'"
    )


def _minimal_install_args(**overrides):
    from types import SimpleNamespace

    defaults = {
        "orcid": None,
        "name": None,
        "pi_name": "Test PI",
        "postgres_port": None,
        "mcp_port": 15431,
        "no_claude_config": False,
        "claude_config_only": False,
        "dry_run": False,
        "git_repos": None,
        "dblp_pid": None,
        "embed_worker": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_install_without_name_reuses_single_user_state_deployment(
    tmp_path, monkeypatch, capsys,
) -> None:
    """A no-name rerun SHALL refresh the one academic-researcher deployment
    in user state instead of choosing a new random scientist name."""
    user_home = tmp_path / "user-home"
    _write_academic_researcher_deployment(user_home, "mendel")
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.setattr(
        pi_cli, "_derive_name_from_random",
        lambda: (_ for _ in ()).throw(
            AssertionError("random slug should not be chosen")
        ),
    )
    monkeypatch.setattr(pi_cli, "_resolve_git_repos", lambda explicit: None)
    report = RegisterReport(results=[
        RegisterResult(
            runtime="codex",
            status="registered",
            config_path=tmp_path / "config.toml",
            config_mtime=1.0,
        ),
    ])
    monkeypatch.setattr(
        "okg.substrate.library.agent_runtimes.registry."
        "register_with_all_detected",
        # `extra_env` (PACT least-privilege-mcp) is now passed by the
        # install path so academic-researcher launches its MCP write-enabled;
        # accept+ignore it so the mock matches the real signature.
        lambda *, deployment, dsn, extra_env=None: report,
    )

    args = _minimal_install_args()
    rc = pi_cli._cmd_install(args, as_json=True)

    assert rc == 0
    assert args.claude_config_only is True
    out = capsys.readouterr().out
    import json as _json
    payload = _json.loads(out)
    assert payload["status"] == "agent_runtimes_re_registered"
    assert payload["deployment_name"] == "mendel"
    assert payload["primary_agent"] == "codex"


def test_install_without_name_refuses_multiple_user_state_deployments(
    tmp_path, monkeypatch, capsys,
) -> None:
    user_home = tmp_path / "user-home"
    _write_academic_researcher_deployment(user_home, "curie")
    _write_academic_researcher_deployment(user_home, "turing")
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))

    args = _minimal_install_args()
    rc = pi_cli._cmd_install(args, as_json=False)

    assert rc == 1
    err = capsys.readouterr().err
    assert "multiple academic-researcher deployments" in err
    assert "curie" in err
    assert "turing" in err
    assert "--name <deployment>" in err


def test_install_docker_block_loads_through_the_strict_manifest_loader(
    tmp_path, monkeypatch,
) -> None:
    """A fresh Docker install stamps deployment.yaml with a `docker:` block
    (install's own writer, not a copied dict, so a new field is caught here).
    The strict loader used to refuse it as an unknown top-level key, which
    failed the install's migrate step and `okg mcp-serve --deployment`."""
    import pytest
    import yaml

    from okg.substrate.cli import lifecycle
    from okg.substrate.deployments import (
        DeploymentConfigError,
        DeploymentManifest,
    )

    user_home = tmp_path / "user-home"
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    dep_dir = user_home / "deployments" / "curie"
    (dep_dir / "schemas").mkdir(parents=True)
    (dep_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    manifest = dep_dir / "deployment.yaml"
    manifest.write_text(
        "name: curie\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    compose_path = tmp_path / "compose.yaml"

    pi_cli._write_deployment_docker_block(
        dep_dir,
        compose_path=compose_path,
        project_name="okg-curie",
        host_port=15435,
    )

    written = yaml.safe_load(manifest.read_text(encoding="utf-8"))["docker"]
    assert written == {
        "compose_path": str(compose_path),
        "project_name": "okg-curie",
        "host_port": 15435,
    }
    assert DeploymentManifest.from_yaml(manifest).name == "curie"
    assert DeploymentManifest.from_yaml_lenient(manifest).name == "curie"
    # The lifecycle verbs read back the block the loader accepted.
    assert lifecycle._read_docker_block("curie") == written

    # The census checks the block's type: a scalar is still refused.
    broken = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    broken["docker"] = "okg-curie"
    manifest.write_text(yaml.safe_dump(broken, sort_keys=False), encoding="utf-8")
    with pytest.raises(DeploymentConfigError, match="docker"):
        DeploymentManifest.from_yaml(manifest)
