"""`okg install` with no `--profile` builds the minimal starter (issue #3414).

Jason, 2026-10-05 (`default A`): the primary user is a team building its own
domain graph, so a bare `okg install` renders, migrates and publishes an
empty `minimal` starter the team grows with `okg deployment add-source`. The
academic-researcher profile now needs `--profile academic-researcher`.

Jason, 2026-10-05 (`nomosdefault A`): a fresh install leaves Nomos runtime
checks off when the deployment declares no access rules; audit and enforce
are opt-in. Every starter and profile the library ships is checked here.

Folded in from the same issue: `okg install --help` lists every installable
profile, a flag that belongs to another profile's installer is refused
instead of dropped, and `okg reset` removes any installed deployment but
tears down only a Postgres the academic-researcher installer started.

The PostgreSQL test runs the real entry point against a fresh, empty
database: install, then add a local folder, then search it.
"""
from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote, urlencode

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from okg.substrate import fastyaml
from okg.substrate.cli import main as cli
from okg.substrate.cli.profile_cli_loader import get_profile_cli
from okg.substrate.deployment_bootstrap.profile_init import discover_profile
from okg.substrate.nomos.runtime_enforcement import (
    _clear_nomos_enforcement_mode_cache,
    deployment_nomos_enforcement_mode,
    manifest_nomos_enforcement_mode,
    manifest_nomos_enforcement_mode_source,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
LIBRARY = REPO_ROOT / "src" / "okg" / "substrate" / "library"


def _installable_library_profiles():
    # Imported per call so the base commit, which lacks it, fails test by
    # test instead of at collection.
    from okg.substrate.deployment_bootstrap.profile_init import (
        installable_library_profiles,
    )

    return installable_library_profiles()


def _install_parser() -> argparse.ArgumentParser:
    parser = cli._build_parser()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action.choices["install"]
    raise AssertionError("no subparsers action on the root parser")


def _parse_install(argv: list[str]) -> argparse.Namespace:
    return cli._build_parser(["install"]).parse_args(["install", *argv])


# -- The default -------------------------------------------------------------


def test_default_install_profile_is_the_minimal_starter() -> None:
    assert cli.DEFAULT_INSTALL_PROFILE == "minimal"
    profile = discover_profile("minimal")
    assert profile.classification == "reusable"
    # No source defaults: the team adds its own sources.
    assert profile.source_defaults_dir is None
    modules = fastyaml.safe_load(
        (profile.modules_file).read_text(encoding="utf-8"),
    )["modules"]
    assert modules == ["document_starter"]
    assert {q.id for q in profile.init_questions} == {
        "deployment_name", "postgres_dsn",
    }


def test_bare_install_routes_to_the_minimal_scaffold(monkeypatch) -> None:
    routed: list[str | None] = []
    monkeypatch.setattr(
        cli, "_install_via_scaffold",
        lambda args, profile_name, *, as_json: routed.append(profile_name) or 0,
    )
    args = _parse_install([
        "--deployment-name", "teamgraph",
        "--postgres-dsn", "postgresql://u@127.0.0.1:1/teamgraph",
    ])
    assert cli._cmd_install(args, as_json=True) == 0
    assert routed == ["minimal"]


def test_academic_researcher_still_installs_with_its_profile(monkeypatch) -> None:
    import okg.substrate.cli.profile_cli_loader as loader

    ran: list[str] = []

    class _Recorder:
        def __init__(self, name: str) -> None:
            self.name = name

        def run(self, args, *, as_json: bool) -> int:  # noqa: ANN001
            ran.append(self.name)
            return 0

    real = loader.discover_install_extensions()
    assert "academic-researcher" in real
    args = _parse_install(["--profile", "academic-researcher", "--orcid", "0000-0001"])
    monkeypatch.setattr(
        loader, "discover_install_extensions",
        lambda: {name: _Recorder(name) for name in real},
    )
    monkeypatch.setattr(
        cli, "_install_via_scaffold",
        lambda *a, **k: pytest.fail("academic-researcher must use its installer"),
    )
    assert cli._cmd_install(args, as_json=True) == 0
    assert ran == ["academic-researcher"]


def test_installer_flag_without_its_profile_is_refused(monkeypatch, capsys) -> None:
    """`okg install --orcid X` used to run academic-researcher. Now the
    minimal starter runs, and dropping --orcid without a word would be a
    silent fallback; the install refuses and names the profile instead."""
    monkeypatch.setattr(
        cli, "_install_via_scaffold",
        lambda *a, **k: pytest.fail("a refused install must not scaffold"),
    )
    args = _parse_install([
        "--orcid", "0000-0001-2345-6789",
        "--deployment-name", "teamgraph",
        "--postgres-dsn", "postgresql://u@127.0.0.1:1/teamgraph",
    ])
    rc = cli._cmd_install(args, as_json=True)
    assert rc != 0
    err = json.loads(capsys.readouterr().err.strip().splitlines()[-1])["error"]
    assert "--orcid" in err["message"]
    assert "academic-researcher" in err["message"]
    assert err["data"]["profile"] == "minimal"
    assert err["data"]["profile_defaulted"] is True
    assert err["data"]["foreign_flags"] == [
        {"flag": "--orcid", "profile": "academic-researcher"},
    ]
    assert "--profile academic-researcher" in err["fix_hint"]


def test_shared_flags_are_not_foreign() -> None:
    """--postgres-dsn and --dry-run serve the scaffold path too."""
    from okg.substrate.cli.profile_cli_loader import discover_install_extensions

    args = _parse_install([
        "--postgres-dsn", "postgresql://u@127.0.0.1:1/x", "--dry-run",
        "--deployment-name", "x", "--force", "--yes",
    ])
    assert cli._foreign_installer_flags(
        args, routed_profile="minimal",
        extensions=discover_install_extensions(),
    ) == []


def test_routed_profiles_own_answer_flag_is_not_foreign(monkeypatch, capsys) -> None:
    """Review H1: workspace-graph asks `mcp_port`, and the injected answer
    flag lands on the `--mcp-port` slot academic-researcher's installer
    declared. Through the real entry point the install must reach the
    scaffold with the port, not be refused as an academic-researcher flag."""
    routed: list[tuple[str | None, object]] = []
    monkeypatch.setattr(
        cli, "_install_via_scaffold",
        lambda args, profile_name, *, as_json: routed.append(
            (profile_name, args.mcp_port),
        ) or 0,
    )
    rc = cli.main([
        "install", "--profile", "workspace-graph", "--mcp-port", "5440",
        "--deployment-name", "wg",
        "--postgres-dsn", "postgresql://u@127.0.0.1:1/wg", "--json",
    ])
    assert rc == 0, capsys.readouterr()
    assert routed == [("workspace-graph", 5440)]

    # Without the injected flag map (a direct caller), the routed
    # profile's question ids still mark the slot as its own.
    from okg.substrate.cli.profile_cli_loader import discover_install_extensions

    args = _parse_install([
        "--mcp-port", "5440", "--postgres-dsn", "postgresql://u@127.0.0.1:1/x",
    ])
    extensions = discover_install_extensions()
    assert cli._foreign_installer_flags(
        args, routed_profile="workspace-graph", extensions=extensions,
        routed_question_ids=[
            q.id for q in discover_profile("workspace-graph").init_questions
        ],
    ) == []
    # The minimal starter asks no `mcp_port`, so there it stays foreign.
    assert cli._foreign_installer_flags(
        args, routed_profile="minimal", extensions=extensions,
        routed_question_ids=[
            q.id for q in discover_profile("minimal").init_questions
        ],
    ) == [{"flag": "--mcp-port", "profile": "academic-researcher"}]


def test_truly_foreign_flag_is_still_refused_for_workspace_graph(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setattr(
        cli, "_install_via_scaffold",
        lambda *a, **k: pytest.fail("a refused install must not scaffold"),
    )
    rc = cli.main([
        "install", "--profile", "workspace-graph", "--mcp-port", "5440",
        "--orcid", "0000-0001-2345-6789", "--deployment-name", "wg",
        "--postgres-dsn", "postgresql://u@127.0.0.1:1/wg", "--json",
    ])
    assert rc != 0
    err = json.loads(capsys.readouterr().err.strip().splitlines()[-1])["error"]
    assert err["data"]["foreign_flags"] == [
        {"flag": "--orcid", "profile": "academic-researcher"},
    ]
    assert err["data"]["profile_defaulted"] is False


def test_refusal_names_the_general_installer_spelling(monkeypatch, capsys) -> None:
    """Review M2: `okg install --name foo --postgres-dsn ...` on the default
    profile meant the minimal starter's --deployment-name. The refusal names
    both ways out: that spelling, and --profile academic-researcher."""
    monkeypatch.setattr(
        cli, "_install_via_scaffold",
        lambda *a, **k: pytest.fail("a refused install must not scaffold"),
    )
    args = _parse_install([
        "--name", "foo", "--postgres-port", "5999",
        "--postgres-dsn", "postgresql://u@127.0.0.1:1/foo",
    ])
    rc = cli._cmd_install(args, as_json=True)
    assert rc != 0
    err = json.loads(capsys.readouterr().err.strip().splitlines()[-1])["error"]
    assert err["data"]["profile_defaulted"] is True
    flags = {item["flag"]: item for item in err["data"]["foreign_flags"]}
    assert flags["--name"]["general_installer"] == "--deployment-name"
    assert flags["--postgres-port"]["general_installer"].startswith("--postgres-dsn")
    assert "--profile academic-researcher" in err["fix_hint"]
    assert "replace --name with --deployment-name" in err["fix_hint"]
    assert "profile 'minimal'" in err["fix_hint"]
    # A flag with no general-installer spelling is told to be dropped.
    args = _parse_install(["--orcid", "1", "--name", "foo"])
    assert cli._cmd_install(args, as_json=True) != 0
    hint = json.loads(capsys.readouterr().err.strip().splitlines()[-1])["error"]["fix_hint"]
    assert hint.endswith("and drop --orcid"), hint


# -- install --help ------------------------------------------------------------


def test_install_help_lists_every_installable_profile() -> None:
    shipped = sorted(
        path.name
        for path in (LIBRARY / "profiles").iterdir()
        if (path / "profile.yaml").is_file() and path.name != "_test"
    )
    assert {"academic-researcher", "minimal", "workspace-graph"} <= set(shipped)
    assert [p.name for p in _installable_library_profiles()] == shipped

    text = _install_parser().format_help()
    for name in shipped:
        assert f"\n  {name}" in text, f"`okg install --help` omits {name}:\n{text}"
    assert "  minimal (default):" in text
    assert "  academic-researcher (own installer):" in text
    assert "_test" not in text


# -- Nomos defaults --------------------------------------------------------------


def _shipped_nomos_manifests() -> list[tuple[str, dict]]:
    rows: list[tuple[str, dict]] = []
    for path in sorted((LIBRARY / "profiles").glob("*/deployment-defaults.yaml")):
        rows.append((f"profile:{path.parent.name}", yaml.safe_load(path.read_text())))
    for path in sorted((LIBRARY / "templates").glob("*/deployment.yaml")):
        rows.append((f"template:{path.parent.name}", yaml.safe_load(path.read_text())))
    return rows


@pytest.mark.parametrize(
    ("starter", "manifest"),
    _shipped_nomos_manifests(),
    ids=[row[0] for row in _shipped_nomos_manifests()],
)
def test_every_shipped_starter_defaults_nomos_off(
    starter: str, manifest: dict, monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    assert manifest_nomos_enforcement_mode(manifest) == "off", starter
    nomos = manifest.get("nomos")
    if nomos is not None:
        # A starter that ships a Nomos block says `off` itself rather than
        # leaning on the absent-field default, so opting in is a one-line edit.
        assert manifest_nomos_enforcement_mode_source(manifest) == "manifest", starter


def test_every_shipped_profile_is_covered_by_the_nomos_check() -> None:
    covered = {name for name, _ in _shipped_nomos_manifests()}
    for profile in _installable_library_profiles():
        assert f"profile:{profile.name}" in covered, profile.name


# -- okg reset -------------------------------------------------------------------


def _pi_cli():
    return get_profile_cli("academic-researcher").install


def _deployment(root: Path, name: str, manifest: dict) -> Path:
    dep = root / name
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")
    return dep


def _reset(monkeypatch, capsys, root: Path, name: str) -> tuple[int, dict, list]:
    pi_cli = _pi_cli()
    calls: list[list[str]] = []
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: None)
    def _run(cmd, *a, **k):  # noqa: ANN001
        calls.append(list(cmd))
        return pi_cli.subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(pi_cli.subprocess, "run", _run)
    monkeypatch.setattr(
        "okg.substrate.library.agent_runtimes.registry.unregister_from_all",
        lambda **kwargs: [],
    )
    rc = pi_cli._cmd_reset(SimpleNamespace(deployment=name, yes=True), as_json=True)
    out = capsys.readouterr().out
    return rc, json.loads(out), calls


def test_reset_of_a_minimal_deployment_leaves_its_database(
    tmp_path, monkeypatch, capsys,
) -> None:
    dep = _deployment(tmp_path, "teamgraph", {
        "name": "teamgraph",
        "profile": {"name": "minimal"},
        "postgres": {"dsn": "${OKG_DSN}"},
    })
    rc, payload, calls = _reset(monkeypatch, capsys, tmp_path, "teamgraph")
    assert rc == 0
    assert not dep.exists()
    assert calls == [], "no Docker teardown for a database the operator owns"
    assert payload["database"] == "not_touched"
    assert payload["docker_project_removed"] is None
    assert "not touched" in payload["database_note"]


def test_reset_tears_down_the_docker_project_the_installer_recorded(
    tmp_path, monkeypatch, capsys,
) -> None:
    compose = tmp_path / "recorded-compose.yaml"
    compose.write_text("services: {}\n", encoding="utf-8")
    dep = _deployment(tmp_path, "labgraph", {
        "name": "labgraph",
        "profile": {"name": "academic-researcher"},
        "docker": {"compose_path": str(compose), "project_name": "okg-labgraph"},
    })
    monkeypatch.setattr(_pi_cli().shutil, "which", lambda _name: "/usr/bin/docker")
    rc, payload, calls = _reset(monkeypatch, capsys, tmp_path, "labgraph")
    assert rc == 0
    assert not dep.exists()
    assert calls == [[
        "docker", "compose", "-f", str(compose), "-p", "okg-labgraph",
        "down", "-v",
    ]]
    assert payload["database"] == "removed_with_docker_project"
    assert payload["docker_teardown"] == "removed"
    assert payload["docker_project_removed"] == "okg-labgraph"


def test_reset_reports_a_failed_docker_teardown_and_keeps_the_record(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Review M1: `docker compose down -v` failing used to read as
    `removed_with_docker_project`. Now reset exits non-zero, says the
    teardown failed, and keeps deployment.yaml so a retry still knows the
    project."""
    pi_cli = _pi_cli()
    dep = _deployment(tmp_path, "labgraph", {
        "name": "labgraph",
        "profile": {"name": "academic-researcher"},
        "docker": {"compose_path": "/gone/compose.yaml", "project_name": "okg-labgraph"},
    })
    calls: list[list[str]] = []

    def _down_fails(cmd, *a, **k):  # noqa: ANN001
        calls.append(list(cmd))
        return pi_cli.subprocess.CompletedProcess(cmd, 1, "", "daemon not running")

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: None)
    monkeypatch.setattr(pi_cli.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(pi_cli.subprocess, "run", _down_fails)
    monkeypatch.setattr(
        "okg.substrate.library.agent_runtimes.registry.unregister_from_all",
        lambda **kwargs: pytest.fail("a failed reset must not unregister"),
    )
    rc = pi_cli._cmd_reset(SimpleNamespace(deployment="labgraph", yes=True), as_json=True)
    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert dep.is_dir(), "the record of the Docker project must survive a failed teardown"
    assert payload["status"] == "reset_failed"
    assert payload["docker_teardown"] == "failed"
    assert "daemon not running" in payload["docker_teardown_detail"]
    # The recorded compose file is gone, so reset used the shipped one.
    assert calls[0][3] == str(pi_cli._find_compose_yaml())
    assert calls[0][3] != "/gone/compose.yaml"


def test_reset_without_docker_fails_for_a_recorded_project(
    tmp_path, monkeypatch, capsys,
) -> None:
    dep = _deployment(tmp_path, "labgraph", {
        "name": "labgraph",
        "profile": {"name": "academic-researcher"},
        "docker": {"compose_path": "/x/compose.yaml", "project_name": "okg-labgraph"},
    })
    monkeypatch.setattr(_pi_cli().shutil, "which", lambda _name: None)
    rc, payload, calls = _reset(monkeypatch, capsys, tmp_path, "labgraph")
    assert rc == 1
    assert calls == []
    assert dep.is_dir()
    assert payload["docker_teardown"] == "failed"
    assert "not on PATH" in payload["docker_teardown_detail"]


def test_reset_without_docker_skips_a_guessed_project_and_says_so(
    tmp_path, monkeypatch, capsys,
) -> None:
    """A half-written install (no deployment.yaml) only guesses
    `okg-<name>`; with no Docker there is nothing to tear down, and reset
    says it skipped rather than claiming a removal."""
    pi_cli = _pi_cli()
    # A directory with no deployment.yaml is found only at the legacy
    # in-repo path, as in test_install_command's reset tests.
    fake_repo = tmp_path / "repo"
    dep = fake_repo / "deployments" / "halfgraph"
    dep.mkdir(parents=True)
    calls: list[list[str]] = []
    (tmp_path / "empty-root").mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "empty-root"))
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: fake_repo)
    monkeypatch.setattr(pi_cli.shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        pi_cli.subprocess, "run", lambda cmd, *a, **k: calls.append(list(cmd)),
    )
    monkeypatch.setattr(
        "okg.substrate.library.agent_runtimes.registry.unregister_from_all",
        lambda **kwargs: [],
    )
    rc = pi_cli._cmd_reset(SimpleNamespace(deployment="halfgraph", yes=True), as_json=True)
    out = capsys.readouterr()
    assert rc == 0, out
    payload = json.loads(out.out)
    assert calls == []
    assert not dep.exists()
    assert payload["docker_teardown"] == "skipped_no_docker"
    assert payload["database"] == "not_removed_docker_missing"


def test_failed_academic_scaffold_records_its_docker_project_for_reset(
    tmp_path, monkeypatch, capsys,
) -> None:
    """Review H2: `compose up` started okg-<name>, then the scaffold wrote
    deployment.yaml and failed. The installer now stamps the `docker:`
    block on that failure, so `okg reset` tears the project down instead of
    reporting `database: not_touched` and stranding the volume."""
    pi_cli = _pi_cli()
    name = "labgraph"
    commands: list[list[str]] = []

    def _fake_run(cmd, *a, **k):  # noqa: ANN001
        cmd = [str(part) for part in cmd]
        commands.append(cmd)
        if "--scaffold-only" in cmd:
            # The scaffold renders deployment.yaml, then its publish fails.
            _deployment(tmp_path, name, {
                "name": name,
                "profile": {"name": "academic-researcher"},
                "postgres": {"dsn": "postgres://postgres:pw@localhost:55432/okg_labgraph"},
            })
            raise pi_cli.subprocess.CalledProcessError(
                1, cmd, output="first publish failed", stderr="",
            )
        return pi_cli.subprocess.CompletedProcess(cmd, 0, b"", b"")

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: None)
    monkeypatch.setattr(pi_cli.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(pi_cli.subprocess, "run", _fake_run)
    args = SimpleNamespace(
        deployment_name=name, orcid=None, deployments_root_resolved=tmp_path,
        use_local_postgres=False, postgres_port=55432, postgres_dsn=None,
        reset=False, papers_dir=None, cv_path=None, group_webpage=None,
        pi_name=None, dblp_pid=None, mcp_port=None, git_repos=None,
        yes=True, force=False,
    )
    rc = pi_cli._provision_postgres_and_scaffold(args, as_json=False)
    assert rc == 1
    assert any(cmd[:2] == ["docker", "compose"] and "up" in cmd for cmd in commands)
    assert "recorded Docker project okg-labgraph" in capsys.readouterr().err
    manifest = yaml.safe_load((tmp_path / name / "deployment.yaml").read_text())
    assert manifest["docker"]["project_name"] == "okg-labgraph"
    assert manifest["docker"]["host_port"] == 55432

    rc, payload, calls = _reset(monkeypatch, capsys, tmp_path, name)
    assert rc == 0
    assert payload["database"] == "removed_with_docker_project"
    assert payload["docker_teardown"] == "removed"
    assert [c[-4:] for c in calls] == [["-p", "okg-labgraph", "down", "-v"]]
    assert not (tmp_path / name).exists()


# -- End to end on PostgreSQL ----------------------------------------------------


@pytest.fixture
def empty_database(okg_dsn: str):
    """A fresh, unmigrated database: `okg install` must migrate it itself."""
    options = conninfo_to_dict(okg_dsn)
    options.pop("dbname", None)

    def target(name: str) -> str:
        uri = "postgresql:///" + quote(name, safe="")
        if options:
            uri += "?" + urlencode(options, quote_via=quote)
        return uri

    name = "okg_test_mininstall_" + uuid.uuid4().hex[:12]
    admin, dsn = target("postgres"), target(name)
    with psycopg.connect(admin, autocommit=True, connect_timeout=15) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    from okg.substrate.db import migrate as migration

    # pg_cron lives in one database per cluster (cron.database_name); a
    # second database cannot create it, as in the add-source journeys.
    original = dict(migration.REQUIRED_EXTENSIONS)
    migration.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        migration.REQUIRED_EXTENSIONS.clear()
        migration.REQUIRED_EXTENSIONS.update(original)
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)),
            )


def _main_json(capsys, argv: list[str]) -> tuple[int, dict]:
    rc = cli.main(argv)
    out = capsys.readouterr()
    text = out.out.strip() or out.err.strip()
    if not text:
        return rc, {}
    try:
        # Read-side commands print one indented document.
        return rc, json.loads(text)
    except json.JSONDecodeError:
        # Others print their envelope as the last line.
        return rc, json.loads(text.splitlines()[-1])


@pytest.mark.postgres
def test_no_profile_install_publishes_the_minimal_starter_and_grows(
    tmp_path: Path,
    empty_database: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    name = "teamgraph"
    root = tmp_path / "deployments"
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    monkeypatch.setenv("OKG_DSN", empty_database)

    # 1. No --profile: render, migrate and publish the minimal starter.
    rc, install = _main_json(capsys, [
        "install",
        "--deployment-name", name,
        "--deployments-root", str(root),
        "--postgres-dsn", empty_database,
        "--non-interactive",
        "--json",
    ])
    assert rc == 0, install
    assert install["profile"]["name"] == "minimal"
    publish = install["publish"]
    assert publish["ok"] is True, publish
    assert publish["no_runnable_sources"] is True
    proof = publish["details"]["publish_proof"]
    assert proof["rule"] == "no_runnable_sources_empty_generation"
    first_generation = proof["generation_id"]
    assert first_generation.startswith(f"okg:{name}:"), proof
    assert (proof["node_count"], proof["edge_count"]) == (0, 0)
    assert "nomos_baseline" not in publish["details"]
    assert any("add-source" in step for step in install["next"]), install["next"]

    dep = root / name
    manifest = fastyaml.safe_load((dep / "deployment.yaml").read_text())
    assert manifest["profile"]["name"] == "minimal"
    assert manifest["modules"] == ["document_starter"]
    assert manifest["nomos"]["runtime_enforcement"] == "off"
    registry = fastyaml.safe_load((dep / "source_registry.yaml").read_text())
    assert registry["sources"] == {}

    # 2. Nomos is off for the deployment as every runtime caller resolves it.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    _clear_nomos_enforcement_mode_cache()
    assert deployment_nomos_enforcement_mode(name) == "off"

    # 3. The team grows it with its own source.
    folder = tmp_path / "notes"
    folder.mkdir()
    (folder / "charter.md").write_text(
        "# Team charter\n\nThe quokkaledger tracks every shipment.\n",
        encoding="utf-8",
    )
    rc, added = _main_json(capsys, [
        "deployment", "add-source", str(dep), "notes",
        "--type", "local-folder", "--setting", f"root={folder}",
        "--apply", "--yes", "--json",
    ])
    assert rc == 0, added
    assert added["status"] == "published", added
    grown_generation = added["generation_id"]
    assert grown_generation != first_generation

    # 4. Search finds it, and no Nomos decision was evaluated for the call.
    rc, found = _main_json(capsys, [
        "search", "--deployment", name, "--query", "quokkaledger", "--json",
    ])
    assert rc == 0, found
    assert found["nomos_enforcement_mode"] == "off"
    assert found["nomos_decision_id"] is None
    hits = found.get("results") or found.get("hits") or []
    assert hits, found

    with psycopg.connect(empty_database, autocommit=True) as conn:
        decisions = conn.execute(
            "SELECT count(*) FROM okg.nomos_decisions WHERE deployment_name = %s",
            (name,),
        ).fetchone()[0]
        activations = conn.execute(
            "SELECT count(*) FROM okg.nomos_policy_activations",
        ).fetchone()[0]
    assert decisions == 0
    assert activations == 0
