"""workspace-graph profile + okg init coverage."""
from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path

import yaml

import okg.substrate.cli.main as cli
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.profile import load_profile, load_profile_modules
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    discover_profile,
    scaffold_deployment,
)
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.sources.registry import load_source_registry


PROFILE_NAME = "workspace-graph"
PROFILE_DIR = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/profiles" / PROFILE_NAME
)
REPO_ROOT = Path(__file__).resolve().parents[3]


def _okg_child_args(cmd: list[str]) -> list[str]:
    """The okg subcommand args of a child argv, after the shared prefix.

    Every okg CLI child starts with ``running_okg_argv()`` (issue #2313).
    Asserting that prefix and slicing past it keeps this test independent
    of how many interpreter flags the prefix carries.
    """
    prefix = running_okg_argv()
    assert cmd[:len(prefix)] == prefix, cmd
    return cmd[len(prefix):]


def _sample_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "sample-repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text(
        "[project]\nname = 'sample-repo'\n",
        encoding="utf-8",
    )
    (repo / "src").mkdir()
    (repo / "src" / "app.py").write_text(
        "def main():\n    return 'ok'\n",
        encoding="utf-8",
    )
    (repo / "openspec").mkdir()
    (repo / "pact" / "changes").mkdir(parents=True)
    return repo


def _init_git_repo(repo: Path) -> None:
    subprocess.run(
        ["git", "init"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )


def _answers(repo: Path) -> dict[str, object]:
    return {
        "deployment_name": "demo_workspace",
        "postgres_dsn": "postgres://postgres:okg@localhost:5433/demo_workspace",
        "repo_root": str(repo),
        "since": "30d",
        "mcp_port": "18888",
        "codex_sessions_root": str(repo / ".codex" / "sessions"),
        "codex_archived_sessions_root": str(
            repo / ".codex" / "archived_sessions"
        ),
    }


def test_workspace_graph_profile_discoverable() -> None:
    assert PROFILE_DIR.is_dir()
    assert (PROFILE_DIR / "profile.yaml").is_file()
    assert (PROFILE_DIR / "modules.yaml").is_file()
    assert (PROFILE_DIR / "source-defaults").is_dir()
    assert (PROFILE_DIR / "invariants.yaml").is_file()
    assert (PROFILE_DIR / "dashboards" / "workspace-overview.yaml").is_file()
    assert (PROFILE_DIR / "deployment-defaults.yaml").is_file()

    profile = discover_profile(PROFILE_NAME)
    assert profile.name == PROFILE_NAME
    assert profile.deployment_defaults_file is not None
    qids = {q.id for q in profile.init_questions}
    expected = {
        "deployment_name",
        "postgres_dsn",
        "repo_root",
        "since",
        "mcp_port",
    }
    assert expected <= qids
    since_question = next(q for q in profile.init_questions if q.id == "since")
    assert since_question.default == "all"

    dashboard = yaml.safe_load(
        (
            PROFILE_DIR / "dashboards" / "workspace-overview.yaml"
        ).read_text(encoding="utf-8")
    )
    panels = {panel["id"]: panel for panel in dashboard["panels"]}
    assert "okg.v_nodes" in panels["live_counts_by_layer"]["sql"]
    assert "okg.v_generations" in panels["latest_generations"]["sql"]
    assert "okg.graph_generations" not in panels["latest_generations"]["sql"]

    modules = load_profile_modules(profile)
    for required in (
        "git_graph",
        "openspec",
        "agent_sessions",
        "compute_env",
        "code_graph",
        "ci",
        "quality_evidence",
        "views",
    ):
        assert required in modules


def test_workspace_graph_profile_source_defaults_are_deployment_scoped(
    tmp_path: Path,
) -> None:
    repo = _sample_repo(tmp_path)
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="demo_workspace",
        deployments_root=tmp_path / "deployments",
        raw_answers=_answers(repo),
    )
    result = scaffold_deployment(plan)

    raw_registry = (result.deployment_dir / "source_registry.yaml").read_text(
        encoding="utf-8"
    )
    assert "${" not in raw_registry
    assert "okg-workspace" not in raw_registry
    assert "demo_workspace.git_files" in raw_registry
    assert str(repo) in raw_registry

    parsed = yaml.safe_load(raw_registry)
    sources = parsed["sources"]
    ownership_ids = {
        source["ownership_id"] for source in sources.values()
    }
    assert ownership_ids
    assert all(oid.startswith("demo_workspace.") for oid in ownership_ids)
    assert sources["pact"]["params"]["pact_root"] == f"{repo}/pact"
    assert sources["openspec"]["params"]["openspec_root"] == (
        f"{repo}/openspec"
    )

    entries = load_source_registry(result.deployment_dir / "source_registry.yaml")
    for expected in (
        "git_files",
        "git_history",
        "code_structure",
        "package_manifests",
        "openspec",
        "pact",
        "codex_sessions",
        "test_reports",
        "security_analysis",
        "compute_env_local",
        "interfaces",
        "code_data_flow",
        "codeql_data_flow",
        "profile_measurements",
    ):
        assert expected in entries

    assert entries["git_history"].raw["predecessor_source"] == "git_files"


def test_profile_scaffold_carries_deployment_defaults(tmp_path: Path) -> None:
    repo = _sample_repo(tmp_path)
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="demo_workspace",
        deployments_root=tmp_path / "deployments",
        raw_answers=_answers(repo),
    )
    result = scaffold_deployment(plan)
    deployment = yaml.safe_load(
        (result.deployment_dir / "deployment.yaml").read_text(
            encoding="utf-8"
        )
    )

    assert deployment["name"] == "demo_workspace"
    assert deployment["profile"]["name"] == "workspace-graph"
    assert deployment["embeddings"]["enabled"] is False
    assert deployment["viz"]["port"] == 18888
    source_policy_defaults = deployment["nomos"]["source_policy_defaults"][
        "by_source_class"
    ]
    assert source_policy_defaults["discovery_crawl"]["store_raw"] is True
    assert source_policy_defaults["reference_catalog"]["store_raw"] is True
    # Issue #3414: no access rules declared, so runtime checks start off.
    assert deployment["nomos"]["runtime_enforcement"] == "off"
    enrichers = {item["class"] for item in deployment["enrichers"]}
    assert (
        "okg.substrate.library.linkers.code_reference.CodeReferenceResolver"
        in enrichers
    )
    assert (
        "okg.substrate.library.linkers.test_symbol.TestSymbolLinker"
        in enrichers
    )

    composed = compose_catalog(result.deployment_dir)
    for subtype in ("source_file", "code_symbol", "open_spec_change"):
        assert subtype in composed.subtypes


def test_okg_init_workspace_graph_profile_scaffolds_deployment(
    tmp_path: Path,
    capsys,
) -> None:
    repo = _sample_repo(tmp_path)
    _init_git_repo(repo)
    deployments_root = tmp_path / "deployments root"

    rc = cli.main([
        "init",
        "--profile",
        "workspace-graph",
        "--deployment-name",
        "demo_workspace",
        "--deployments-root",
        str(deployments_root),
        "--postgres-dsn",
        "postgres://postgres:okg@localhost:5433/demo_workspace",
        "--repo-root",
        str(repo),
        "--mcp-port",
        "18888",
        "--no-publish",
        "--non-interactive",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["profile"]["name"] == "workspace-graph"
    assert payload["deployment"]["name"] == "demo_workspace"
    assert payload["next"][0] == "export OKG_DEPLOYMENTS_DIR=" + shlex.quote(
        str(deployments_root.resolve()),
    )

    dep_dir = deployments_root / "demo_workspace"
    assert (dep_dir / "deployment.yaml").is_file()
    assert (dep_dir / "source_registry.yaml").is_file()
    assert (dep_dir / "schemas").is_dir()
    assert (dep_dir / "invariants.yaml").is_file()
    assert (dep_dir / "dashboards" / "workspace-overview.yaml").is_file()
    manifest = DeploymentManifest.from_yaml(dep_dir / "deployment.yaml")
    assert manifest.schema_dir == dep_dir / "schemas"

    deployment = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert deployment["profile"]["init_answers"]["repo_root"] == str(repo)
    assert deployment["viz"]["port"] == 18888


def test_okg_init_workspace_graph_profile_rejects_non_git_repo(
    tmp_path: Path,
    capsys,
) -> None:
    repo = _sample_repo(tmp_path)
    deployments_root = tmp_path / "deployments"

    rc = cli.main([
        "init",
        "--profile",
        "workspace-graph",
        "--deployment-name",
        "demo_workspace",
        "--deployments-root",
        str(deployments_root),
        "--postgres-dsn",
        "postgres://postgres:okg@localhost:5433/demo_workspace",
        "--repo-root",
        str(repo),
        "--mcp-port",
        "18888",
        "--no-publish",
        "--non-interactive",
        "--json",
    ])

    assert rc != 0
    err = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert err["error"]["code"] == "substrate_error"
    assert "not a git repository" in err["error"]["message"]
    assert not (deployments_root / "demo_workspace").exists()


def test_okg_init_workspace_graph_profile_runs_first_publish_for_custom_root(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    repo = _sample_repo(tmp_path)
    _init_git_repo(repo)
    deployments_root = tmp_path / "deployments root"
    dsn = "postgres://postgres:okg@localhost:5433/demo_workspace"
    migrate_calls: list[dict[str, str | None]] = []
    subprocess_calls: list[dict[str, object]] = []
    baseline_calls: list[dict[str, object]] = []
    baseline_commits: list[bool] = []
    operation_order: list[str] = []

    def fake_migrate(got_dsn: str, *, deployment_name: str | None = None):
        migrate_calls.append({
            "dsn": got_dsn,
            "deployment_name": deployment_name,
            "deployments_root": os.environ.get("OKG_DEPLOYMENTS_DIR"),
        })

    real_completed_process = subprocess.CompletedProcess

    def fake_run(*popenargs, **kwargs):
        cmd = list(popenargs[0] if popenargs else kwargs["args"])
        if cmd and cmd[0] == "git" and "rev-parse" in cmd:
            return real_completed_process(
                cmd,
                0,
                stdout=f"{repo}\n",
                stderr="",
            )
        operation_order.append(str(_okg_child_args(cmd)[0]))
        subprocess_calls.append({
            "cmd": cmd,
            "env": dict(kwargs.get("env") or {}),
            "capture_output": kwargs.get("capture_output"),
            "text": kwargs.get("text"),
        })
        stdout = ""
        if _okg_child_args(cmd)[:2] == ["run", "--once"]:
            # init reads the version its own run reports (#2422).
            stdout = json.dumps({"run_summary": {
                "outcome": "published", "generation_id": "gen-first",
            }})
        return real_completed_process(cmd, 0, stdout=stdout, stderr="")

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def commit(self) -> None:
            baseline_commits.append(True)

    def fake_connect(got_dsn: str, **kwargs):
        # `**kwargs` absorbs what the substrate connection factory adds on
        # every call (keepalives, connect_timeout, the idle-in-transaction
        # `options`). This test is about the ORDER of the init operations,
        # not about those connection settings.
        assert got_dsn == dsn
        return FakeConnection()

    def fake_seed_baseline(conn, **kwargs):
        operation_order.append("nomos_baseline")
        baseline_calls.append(kwargs)
        return {
            "ok": True,
            "source_names": list(kwargs["source_names"]),
            "runtime_source_names": list(kwargs["runtime_source_names"]),
        }

    def fake_publish_proof(
        got_dsn, last_publish, *, deployment_name, source_failure_reasons=None,
        expected_generation_id=None,
    ):
        # `fix-init-scaffold-provability`: init applies provision's
        # real-publish proof after `run --once`; this orchestration test
        # fakes the DB, so fake the proof and record its position.
        assert got_dsn == dsn
        assert deployment_name == "demo_workspace"
        assert expected_generation_id == "gen-first"
        operation_order.append("publish_proof")
        return {"ok": True, "status": "published", "nodes_added": 1}

    def fake_ensure_extensions(got_dsn: str):
        # init now provisions extensions before migrate (provision parity,
        # fix-init-scaffold-provability review H1); fake it like migrate.
        assert got_dsn == dsn
        operation_order.append("extensions")
        return {"created": [], "already_present": 6}

    monkeypatch.setattr("okg.substrate.db.migrate.migrate", fake_migrate)
    monkeypatch.setattr(
        "okg.substrate.db.migrate.ensure_required_extensions",
        fake_ensure_extensions,
    )
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("psycopg.connect", fake_connect)
    monkeypatch.setattr(
        "okg.substrate.nomos.seed_deployment_baseline_nomos_state",
        fake_seed_baseline,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.provision._provision_publish_proof",
        fake_publish_proof,
    )

    rc = cli.main([
        "init",
        "--profile",
        "workspace-graph",
        "--deployment-name",
        "demo_workspace",
        "--deployments-root",
        str(deployments_root),
        "--postgres-dsn",
        dsn,
        "--repo-root",
        str(repo),
        "--mcp-port",
        "18888",
        "--non-interactive",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["publish"]["ok"] is True
    assert payload["publish"]["details"]["deployments_root"] == str(
        deployments_root.resolve(),
    )
    # The profile ships `nomos.runtime_enforcement: off` (issue #3414), so no
    # Nomos baseline is required and init seeds none.
    assert "nomos_baseline" not in payload["publish"]["details"]
    assert payload["next"][0] == "export OKG_DEPLOYMENTS_DIR=" + shlex.quote(
        str(deployments_root.resolve()),
    )
    assert migrate_calls == [{
        "dsn": dsn,
        "deployment_name": "demo_workspace",
        "deployments_root": str(deployments_root.resolve()),
    }]
    assert baseline_commits == []
    assert baseline_calls == []
    assert operation_order == [
        "extensions",
        "catalog",
        "ingest",
        "run",
        "publish_proof",
    ]
    assert len(subprocess_calls) == 3
    load_call, ingest_call, run_call = subprocess_calls
    load_args = _okg_child_args(load_call["cmd"])
    ingest_args = _okg_child_args(ingest_call["cmd"])
    run_args = _okg_child_args(run_call["cmd"])
    assert load_args[0:3] == ["catalog", "load", "--deployment"]
    assert load_args[3] == str(
        deployments_root / "demo_workspace",
    )
    assert ingest_args[0:3] == [
        "ingest",
        "--deployment",
        "demo_workspace",
    ]
    assert "--progress" in ingest_args
    assert run_args[0:4] == [
        "run",
        "--once",
        "--deployment",
        "demo_workspace",
    ]
    for call in subprocess_calls:
        env = call["env"]
        assert env["OKG_DSN"] == dsn
        assert env["OKG_DEPLOYMENTS_DIR"] == str(
            deployments_root.resolve(),
        )


def test_workspace_graph_docs_prefer_profile_setup() -> None:
    workspace_doc = (REPO_ROOT / "docs/runbooks/workspace-graph/setup.md").read_text(
        encoding="utf-8"
    )
    profiles_readme = (
        REPO_ROOT / "src/okg/substrate/library/profiles/README.md"
    ).read_text(encoding="utf-8")
    combined = "\n".join([workspace_doc, profiles_readme])

    # The profile path over the legacy template path, named with the
    # declared door: `okg init` still works as an alias, but guidance names
    # `okg install` (define-newcomer-entry-path).
    assert "okg install --profile workspace-graph" in combined
    assert "okg init --profile workspace-graph" not in combined
    assert "legacy `workspace-graph` template alias" in combined
