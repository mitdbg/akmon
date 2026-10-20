"""Real-history execution proof for the generated workspace-graph profile.

This lane is intentionally deterministic and offline.  It clones the checked-in
TinyDB Git bundle, publishes v4.8.1, advances the same worktree to v4.8.2,
publishes again, and finally proves a no-change replay.  Evidence families that
do not exist upstream in TinyDB are generated under ``.okg-test-overlay`` and
remain visibly untracked so graph assertions cannot mistake them for upstream
repository history.

Run locally with a test-owned Postgres database:

    OKG_WORKSPACE_PROFILE_DSN=postgresql://postgres:okg@127.0.0.1:5433/okg_workspace_profile \
      uv run pytest tests/substrate/e2e/test_workspace_profile_real_repository.py -q --no-cov
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "workspaces" / "tinydb"
FIXTURE_MANIFEST = FIXTURE_ROOT / "fixture.yaml"
DEPLOYMENT = "workspace-profile-real-repository"
DSN_ENV = "OKG_WORKSPACE_PROFILE_DSN"
REPORT_ENV = "OKG_WORKSPACE_PROFILE_REPORT"
REQUIRED_EXTENSIONS = (
    "timescaledb",
    "vector",
    "pg_textsearch",
    "pg_trgm",
    "fuzzystrmatch",
    "btree_gist",
)

SOURCE_NAMES = {
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
}

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not os.environ.get(DSN_ENV),
        reason=f"{DSN_ENV} is required for the real workspace-profile lane",
    ),
]


def _dsn_parts(dsn: str) -> dict[str, str]:
    from psycopg import conninfo

    parts = {
        key: str(value)
        for key, value in conninfo.conninfo_to_dict(dsn).items()
        if value is not None
    }
    dbname = parts.get("dbname")
    if not dbname or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dbname):
        pytest.fail(
            f"{DSN_ENV} must name a plain-identifier disposable database; "
            f"got dbname={dbname!r}"
        )
    return parts


def _create_disposable_database(dsn: str) -> str:
    parts = _dsn_parts(dsn)
    base_name = parts["dbname"]
    suffix = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"
    dbname = f"{base_name[: 62 - len(suffix)]}_{suffix}"
    server = {**parts, "dbname": "postgres"}
    with psycopg.connect(**server, autocommit=True) as conn:
        conn.execute(
            psycopg.sql.SQL("CREATE DATABASE {}").format(
                psycopg.sql.Identifier(dbname)
            )
        )
    parsed_dsn = urlsplit(dsn)
    if parsed_dsn.scheme not in {"postgres", "postgresql"}:
        pytest.fail(
            f"{DSN_ENV} must use a postgres:// or postgresql:// URL"
        )
    child_dsn = urlunsplit(
        (
            parsed_dsn.scheme,
            parsed_dsn.netloc,
            f"/{dbname}",
            parsed_dsn.query,
            parsed_dsn.fragment,
        )
    )
    with psycopg.connect(child_dsn, autocommit=True) as conn:
        for extension in REQUIRED_EXTENSIONS:
            conn.execute(
                psycopg.sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                    psycopg.sql.Identifier(extension)
                )
            )
    return child_dsn


def _run(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: int = 1800,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        errors="replace",
        timeout=timeout,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"command failed ({proc.returncode}): {' '.join(args)}\n"
            f"stdout tail:\n{proc.stdout[-8000:]}\n"
            f"stderr tail:\n{proc.stderr[-8000:]}"
        )
    return proc


def _run_okg(
    args: list[str],
    *,
    env: dict[str, str],
    timeout: int = 1800,
) -> dict[str, Any]:
    proc = _run(
        [sys.executable, "-m", "okg.substrate.cli.main", *args, "--json"],
        cwd=REPO_ROOT,
        env=env,
        timeout=timeout,
    )
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        pytest.fail(
            f"okg command did not emit one JSON document: {' '.join(args)}\n"
            f"stdout tail:\n{proc.stdout[-8000:]}\n"
            f"stderr tail:\n{proc.stderr[-8000:]}"
        )
    assert isinstance(payload, dict)
    return payload


def _fixture() -> dict[str, Any]:
    payload = yaml.safe_load(FIXTURE_MANIFEST.read_text(encoding="utf-8"))
    assert payload["schema"] == "okg.workspace-fixture.v1"
    return payload


def _materialize_tinydb(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    fixture = _fixture()
    bundle = FIXTURE_ROOT / fixture["bundle"]["file"]
    assert hashlib.sha256(bundle.read_bytes()).hexdigest() == (
        fixture["bundle"]["sha256"]
    )
    repo = tmp_path / "tinydb"
    _run(["git", "clone", str(bundle), str(repo)], cwd=tmp_path)
    for revision in fixture["revisions"].values():
        resolved = _run(
            ["git", "rev-parse", f"{revision['ref']}^{{commit}}"],
            cwd=repo,
        ).stdout.strip()
        assert resolved == revision["commit"]
    _run(
        ["git", "checkout", "--detach", fixture["revisions"]["initial"]["commit"]],
        cwd=repo,
    )
    return repo, fixture


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _write_json(path: Path, payload: Any) -> Path:
    return _write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_overlay(repo: Path, fixture: dict[str, Any]) -> dict[str, Path]:
    overlay = repo / ".okg-test-overlay"
    overlay.mkdir()
    _write_json(
        overlay / "manifest.json",
        {
            "schema": "okg.workspace-test-overlay.v1",
            "ownership": "test-owned",
            "upstream": fixture["upstream"],
            "upstream_initial_commit": fixture["revisions"]["initial"]["commit"],
            "upstream_updated_commit": fixture["revisions"]["updated"]["commit"],
            "note": (
                "These artifacts exercise profile-only sources and are not "
                "TinyDB upstream content."
            ),
        },
    )
    interface_fixture = _write_text(
        overlay / "fixture_api.py",
        '''"""Test-owned interface and data-flow evidence for profile coverage."""
from pathlib import Path


def tool(name):
    def decorate(function):
        return function
    return decorate


@tool("exercise-tinydb-cache")
def exercise_cache(cache, output_path: Path):
    """Route an interface to a symbol and one local data asset."""
    output_path.write_text("cache exercised", encoding="utf-8")
    return cache.set("fixture", "value")
''',
    )

    pact_path = overlay / "pact" / "changes" / "workspace-profile-fixture"
    _write_text(
        pact_path / "pact.yaml",
        yaml.safe_dump(
            {
                "pact_version": 4,
                "target_pact_version": 4,
                "change": {
                    "id": "workspace-profile-fixture",
                    "title": "Workspace profile fixture",
                    "rationale": "Exercise the PACT source in the real-repo lane.",
                    "root_cause": (
                        "The deterministic fixture needs separately owned "
                        "change-management evidence."
                    ),
                },
                "requirements": [
                    {
                        "id": "req.workspace-profile-fixture",
                        "statement": "The TinyDB profile publish is observable.",
                        "evidence_policy": {"required": ["executed_test"]},
                    }
                ],
                "tasks": [
                    {
                        "id": "task.workspace-profile-fixture",
                        "title": "Publish the fixture",
                        "status": "todo",
                        "verification": "pytest -q",
                        "requirements": ["req.workspace-profile-fixture"],
                    }
                ],
            },
            sort_keys=False,
        ),
    )

    openspec = (
        overlay
        / "openspec"
        / "changes"
        / "workspace-profile-fixture"
    )
    _write_text(
        openspec / "proposal.md",
        """# Workspace Profile Fixture

## Why

Exercise generated-profile acceptance evidence.

## What Changes

- Adds one test-owned acceptance criterion.
""",
    )
    _write_text(
        openspec / "specs" / "workspace-profile" / "spec.md",
        """## ADDED Requirements

### Requirement: TinyDB release updates remain visible

The graph SHALL expose the changed TinyDB release paths.

#### Scenario: JUnit cites the stable criterion

- **GIVEN** the generated workspace profile
- **WHEN** the TinyDB fixture publishes
- **THEN** the test evidence verifies this criterion
""",
    )
    _write_text(
        overlay / "openspec" / "specs" / "workspace-profile" / "spec.md",
        """# Workspace Profile Specification

## Purpose

Exercise the generated workspace profile against pinned repository history.

## Requirements

### Requirement: TinyDB release updates remain visible

The graph SHALL expose the changed TinyDB release paths.
""",
    )

    junit = _write_text(
        overlay / "reports" / "junit.xml",
        f"""<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="tinydb-fixture" tests="1" failures="1" errors="0" skipped="0">
    <testcase classname="tests.test_utils" name="test_lru_cache" file="tests/test_utils.py">
      <properties>
        <property name="openspec_criterion" value="crit:workspace-profile-fixture:0"/>
      </properties>
      <failure message="fixture traceback">Traceback (most recent call last):
  File "{repo / 'tinydb' / 'utils.py'}", line 90, in get
    value = self.cache.get(key)
RuntimeError: deterministic fixture traceback</failure>
    </testcase>
  </testsuite>
</testsuites>
""",
    )
    coverage = _write_text(
        overlay / "reports" / "coverage.xml",
        """<?xml version="1.0"?>
<coverage line-rate="0.9" branch-rate="0.8">
  <packages>
    <package name="tinydb">
      <classes>
        <class name="utils.py" filename="tinydb/utils.py">
          <lines>
            <line number="90" hits="1"/>
            <line number="91" hits="1"/>
            <line number="98" hits="0"/>
          </lines>
        </class>
      </classes>
    </package>
  </packages>
</coverage>
""",
    )
    codeql = _write_json(
        overlay / "reports" / "codeql-data-flow.sarif",
        {
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "CodeQL",
                            "semanticVersion": "2.17.0",
                            "rules": [
                                {
                                    "id": "py/cache-flow",
                                    "version": "1.0",
                                    "properties": {
                                        "tags": ["security", "test-fixture"],
                                        "security-severity": "5.0",
                                    },
                                }
                            ],
                        }
                    },
                    "results": [
                        {
                            "ruleId": "py/cache-flow",
                            "level": "warning",
                            "message": {
                                "text": "Test-owned cache flow reaches LRUCache.get"
                            },
                            "locations": [
                                {
                                    "physicalLocation": {
                                        "artifactLocation": {
                                            "uri": "tinydb/utils.py"
                                        },
                                        "region": {
                                            "startLine": 90,
                                            "endLine": 98,
                                        },
                                    }
                                }
                            ],
                            "codeFlows": [
                                {
                                    "threadFlows": [
                                        {
                                            "locations": [
                                                {
                                                    "location": {
                                                        "physicalLocation": {
                                                            "artifactLocation": {
                                                                "uri": "tinydb/utils.py"
                                                            },
                                                            "region": {
                                                                "startLine": 90,
                                                                "endLine": 91,
                                                            },
                                                        },
                                                        "message": {
                                                            "text": "cache lookup"
                                                        },
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ],
                        }
                    ],
                }
            ],
        },
    )
    profile = _write_json(
        overlay / "profiles" / DEPLOYMENT / "fixture" / "profile.json",
        {
            "profile_run_id": "tinydb-fixture",
            "deployment": DEPLOYMENT,
            "target_kind": "publish",
            "command": f"okg ingest --deployment {DEPLOYMENT}",
            "started_at": "2026-07-26T00:00:00Z",
            "duration_ms": 25,
            "stages": [
                {
                    "name": "get",
                    "duration_ms": 20,
                    "path": "tinydb/utils.py",
                    "line": 90,
                }
            ],
        },
    )
    sessions = overlay / "codex" / "sessions"
    archived = overlay / "codex" / "archived"
    archived.mkdir(parents=True)
    transcript = sessions / "workspace-profile.jsonl"
    rows = [
        {
            "type": "session_meta",
            "timestamp": "2026-07-26T00:00:00Z",
            "payload": {
                "id": "workspace-profile-fixture",
                "cwd": str(repo),
                "model": "gpt-5",
                "timestamp": "2026-07-26T00:00:00Z",
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-07-26T00:00:01Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": (
                    "Inspect TinyDB cache behavior and the separately owned "
                    "workspace-profile fixture."
                ),
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-07-26T00:00:02Z",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps(
                    {
                        "cmd": (
                            "sed -n '90,98p' tinydb/utils.py && "
                            "okg pact status workspace-profile-fixture && "
                            "openspec show workspace-profile-fixture"
                        )
                    }
                ),
            },
        },
    ]
    _write_text(
        transcript,
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
    )
    return {
        "root": overlay,
        "interface_fixture": interface_fixture,
        "pact_root": overlay / "pact",
        "openspec_root": overlay / "openspec",
        "junit": junit,
        "coverage": coverage,
        "codeql": codeql,
        "profile": profile,
        "sessions": sessions,
        "archived": archived,
    }


def _command_env(
    *,
    deployments_root: Path,
    dsn: str,
) -> dict[str, str]:
    env = os.environ.copy()
    for key in (
        "OKG_DEPLOYMENT",
        "PZ_OKG_DEPLOYMENT",
        "OKG_DSN",
        "OKG_DEPLOYMENTS_DIR",
        "OKG_PACT_DECISION_PROJECTION",
        "OKG_PACT_GRAPH_DEPLOYMENT",
    ):
        env.pop(key, None)
    env.update(
        {
            "OKG_DEPLOYMENT": DEPLOYMENT,
            "OKG_DEPLOYMENTS_DIR": str(deployments_root),
            "OKG_DSN": dsn,
            "OKG_PACT_GRAPH_DEPLOYMENT": DEPLOYMENT,
            "OKG_NOMOS_SUBJECT": "human_operator",
        }
    )
    return env


def _scaffold(
    *,
    repo: Path,
    overlay: dict[str, Path],
    deployments_root: Path,
    dsn: str,
    env: dict[str, str],
) -> Path:
    _run_okg(
        [
            "install",
            "--profile",
            "workspace-graph",
            "--deployment-name",
            DEPLOYMENT,
            "--deployments-root",
            str(deployments_root),
            "--postgres-dsn",
            dsn,
            "--repo-root",
            str(repo),
            "--codex-sessions-root",
            str(overlay["sessions"]),
            "--codex-archived-sessions-root",
            str(overlay["archived"]),
            "--since",
            "all",
            "--no-publish",
            "--non-interactive",
        ],
        env=env,
    )
    deployment_dir = deployments_root / DEPLOYMENT
    registry_path = deployment_dir / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    sources = registry["sources"]
    assert set(sources) == SOURCE_NAMES

    sources["git_files"]["params"]["include_untracked"] = True
    sources["openspec"]["params"]["openspec_root"] = str(
        overlay["openspec_root"]
    )
    sources["pact"]["params"]["pact_root"] = str(overlay["pact_root"])
    sources["codex_sessions"]["params"].update(
        {
            "codex_sessions_root": str(overlay["sessions"]),
            "archived_sessions_root": str(overlay["archived"]),
            "cwd_filter": [str(repo)],
            "since": "all",
        }
    )
    sources["test_reports"]["params"].update(
        {
            "artifact_paths": [str(overlay["junit"])],
            "artifact_globs": [],
            "coverage_paths": [str(overlay["coverage"])],
            "coverage_globs": [],
            "infer_sha": False,
        }
    )
    sources["security_analysis"]["params"].update(
        {
            "artifact_paths": [str(overlay["codeql"])],
            "artifact_globs": [],
        }
    )
    sources["codeql_data_flow"]["params"].update(
        {
            "artifact_paths": [str(overlay["codeql"])],
            "artifact_globs": [],
        }
    )
    sources["profile_measurements"]["params"].update(
        {
            "artifact_paths": [str(overlay["profile"])],
            "artifact_globs": [],
            "benchmark_paths": [],
            "benchmark_globs": [],
            "openspec_roots": [str(overlay["openspec_root"])],
        }
    )
    registry_path.write_text(
        yaml.safe_dump(registry, sort_keys=False),
        encoding="utf-8",
    )
    _run_okg(
        [
            "migrate",
            "--deployment",
            DEPLOYMENT,
            "--dsn",
            dsn,
            "--apply",
        ],
        env=env,
    )
    # okg_mcp is one cluster-wide role: alter it under the role lock every
    # other writer takes, or a concurrent ALTER fails one of them (#1985).
    from tests.substrate.conftest import enable_okg_mcp_login

    enable_okg_mcp_login(dsn)
    from okg.substrate.cli.init import _seed_init_nomos_baseline_if_required

    prior_root = os.environ.get("OKG_DEPLOYMENTS_DIR")
    os.environ["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
    try:
        baseline = _seed_init_nomos_baseline_if_required(
            deployment_name=DEPLOYMENT,
            deployment_dir=deployment_dir,
            dsn=dsn,
        )
    finally:
        if prior_root is None:
            os.environ.pop("OKG_DEPLOYMENTS_DIR", None)
        else:
            os.environ["OKG_DEPLOYMENTS_DIR"] = prior_root
    assert baseline is not None
    _run_okg(
        [
            "catalog",
            "load",
            "--deployment",
            str(deployment_dir),
            "--dsn",
            dsn,
            "--apply",
        ],
        env=env,
    )
    return deployment_dir


def _ingest(*, dsn: str, env: dict[str, str]) -> dict[str, Any]:
    return _run_okg(
        [
            "ingest",
            "--deployment",
            DEPLOYMENT,
            "--dsn",
            dsn,
            "--inline",
            "--mode",
            "scope_complete",
        ],
        env=env,
    )


def _publish(report: dict[str, Any]) -> dict[str, Any]:
    publishes = report.get("publishes") or []
    assert publishes, report
    publish = publishes[-1]
    assert publish["status"] == "published", publish
    return publish


def _assert_source_report(report: dict[str, Any], *, require_output: bool) -> None:
    assert set(report["ordered"]) == SOURCE_NAMES
    results = {row["name"]: row for row in report["results"]}
    assert set(results) == SOURCE_NAMES
    assert all(row["status"] == "OK" for row in results.values()), results
    if require_output:
        silent = {
            name: row
            for name, row in results.items()
            if int(row.get("nodes_appended") or 0)
            + int(row.get("edges_appended") or 0)
            <= 0
        }
        assert not silent, silent


def _graph_counts(graph: Any) -> dict[str, int]:
    nodes = graph.query("SELECT count(*) AS n FROM okg.graph_nodes")
    edges = graph.query("SELECT count(*) AS n FROM okg.graph_edges")
    return {"nodes": int(nodes[0]["n"]), "edges": int(edges[0]["n"])}


def _generation_ids(dsn: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                  FROM okg.v_generations
                 WHERE deployment_name = %s
                   AND status = 'published'
                 ORDER BY published_at DESC
                """,
                (DEPLOYMENT,),
            )
            return [str(row[0]) for row in cur.fetchall()]


def _source_files(graph: Any, paths: list[str]) -> dict[str, dict[str, Any]]:
    rows = graph.query(
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'source_file'
           AND attrs->>'path' = ANY(:paths)
         ORDER BY node_id
        """,
        {"paths": paths},
    )
    return {
        str(row["attrs"]["path"]): dict(row["attrs"])
        for row in rows
    }


def _assert_enrichers(
    *,
    dsn: str,
    publish: dict[str, Any],
) -> dict[str, Any]:
    from okg.substrate.library.linkers.acceptance_criteria import (
        AcceptanceCriteriaLinker,
    )
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )
    from okg.substrate.library.linkers.coverage_symbol import CoverageSymbolLinker
    from okg.substrate.library.linkers.data_asset import DataAssetLinker
    from okg.substrate.library.linkers.data_flow_evidence import (
        DataFlowEvidenceLinker,
    )
    from okg.substrate.library.linkers.interface_handler import (
        InterfaceHandlerLinker,
    )
    from okg.substrate.library.linkers.profile_hotspot import ProfileHotspotLinker
    from okg.substrate.library.linkers.security_finding import SecurityFindingLinker
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker
    from okg.substrate.library.linkers.tool_use_symbol import ToolUseSymbolLinker

    run_by_name = {
        row["name"]: row
        for row in publish["enricher_runs"]
        if not row["name"].startswith("_")
    }
    assert set(run_by_name) == {
        "declarative",
        "acceptance_criteria",
        "tool_use_symbol",
        "code_reference",
        "test_symbol",
        "coverage_symbol",
        "interface_handler",
        "data_asset",
        "data_flow_evidence",
        "security_finding",
        "profile_hotspot",
    }
    assert all(row["status"] == "ok" for row in run_by_name.values()), run_by_name

    diagnostic_specs = {
        "acceptance_criteria": (
            AcceptanceCriteriaLinker(),
            ("eligible_criteria",),
            ("matched_criteria",),
        ),
        "tool_use_symbol": (
            ToolUseSymbolLinker(),
            ("eligible_events",),
            ("matched_events",),
        ),
        "code_reference": (
            CodeReferenceResolver(),
            ("eligible_imports", "eligible_references"),
            ("matched_imports", "matched_references"),
        ),
        "test_symbol": (
            TestSymbolLinker(),
            ("eligible_test_cases", "eligible_stack_frames"),
            ("matched_test_cases", "matched_stack_frames"),
        ),
        "coverage_symbol": (
            CoverageSymbolLinker(),
            ("coverage_reports",),
            ("matched_symbols",),
        ),
        "interface_handler": (
            InterfaceHandlerLinker(),
            ("eligible_endpoints",),
            ("matched_endpoints",),
        ),
        "data_asset": (
            DataAssetLinker(),
            ("eligible_touchpoints",),
            ("matched_touchpoints",),
        ),
        "data_flow_evidence": (
            DataFlowEvidenceLinker(),
            ("eligible_evidence_nodes",),
            ("matched_evidence_nodes",),
        ),
        "security_finding": (
            SecurityFindingLinker(),
            ("eligible_findings",),
            ("matched_findings",),
        ),
        "profile_hotspot": (
            ProfileHotspotLinker(),
            ("eligible_nodes",),
            ("matched_nodes",),
        ),
    }
    report: dict[str, Any] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        for name, (linker, eligible_fields, matched_fields) in diagnostic_specs.items():
            diagnostics = asdict(linker.diagnostics(conn))
            eligible = sum(int(diagnostics[field]) for field in eligible_fields)
            matched = sum(int(diagnostics[field]) for field in matched_fields)
            assert eligible > 0, (name, diagnostics)
            assert matched > 0, (name, diagnostics)
            emitted = int(run_by_name[name]["n_edges_emitted"])
            report[name] = {
                "eligible": eligible,
                "matched": matched,
                "cycle_edges_emitted": emitted,
                "attribution": "emitted" if emitted > 0 else "existing",
                "diagnostics": diagnostics,
            }
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.edge_facts
                 WHERE source LIKE '_enricher:declarative:%'
                """
            )
            declarative_edges = int(cur.fetchone()[0])
    assert declarative_edges > 0
    assert int(run_by_name["declarative"]["n_edges_emitted"]) > 0
    report["declarative"] = {
        "eligible": 1,
        "matched": 1,
        "cycle_edges_emitted": int(
            run_by_name["declarative"]["n_edges_emitted"]
        ),
        "attribution": "emitted",
        "persisted_edge_facts": declarative_edges,
    }
    return report


def _assert_mcp_operators(
    *,
    dsn: str,
    generation_id: str,
    endpoint_id: str,
) -> dict[str, int]:
    from okg.substrate.mcp.server import build_server

    server = build_server(
        dsn=dsn,
        deployment_name=DEPLOYMENT,
        dsn_authority={
            "dsn": dsn,
            "authority": {
                "mode": "test_fixture",
                "deployment": DEPLOYMENT,
            },
            "target": None,
        },
    )
    tools = server._tool_manager._tools
    try:
        inspected = tools["inspect"].fn(
            target={"kind": "graph"},
            generation_id=generation_id,
        )
        assert inspected["node_count"] > 0
        assert inspected["edge_count"] > 0

        searched = tools["search"].fn(
            query="LRUCache",
            method="lexical",
            generation_id=generation_id,
        )
        assert searched["results"]

        expanded = tools["expand"].fn(
            input={"node_id": endpoint_id},
            generation_id=generation_id,
        )
        assert expanded["neighbors"]

        filtered = tools["filter"].fn(
            input={"kind": "nodes", "subtype": "code_symbol"},
            predicate={},
            select=["node_id", "subtype"],
            generation_id=generation_id,
        )
        assert filtered["items"]

        mapped = tools["map"].fn(
            input={"kind": "nodes", "subtype": "source_file"},
            projection=["node_id", "subtype"],
            generation_id=generation_id,
        )
        assert mapped["items"]

        aggregated = tools["aggregate"].fn(
            input={"kind": "edges"},
            by=["src_subtype", "edge_type", "dst_subtype"],
            metrics=["count"],
            generation_id=generation_id,
        )
        assert aggregated["routes"]

        queried = tools["query"].fn(
            sql=(
                "SELECT node_id FROM okg.v_nodes "
                "WHERE subtype = 'source_file' ORDER BY node_id LIMIT 5"
            ),
            generation_id=generation_id,
        )
        assert queried["rows"]
        return {
            "inspect_nodes": int(inspected["node_count"]),
            "inspect_edges": int(inspected["edge_count"]),
            "search_results": len(searched["results"]),
            "expanded_neighbors": len(expanded["neighbors"]),
            "filtered_items": len(filtered["items"]),
            "mapped_rows": len(mapped["items"]),
            "aggregate_routes": len(aggregated["routes"]),
            "query_rows": len(queried["rows"]),
        }
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def _assert_dashboard_panels(
    *,
    deployment_dir: Path,
    dsn: str,
    generation_id: str,
) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp.run_sql import (
        _expand_composite_views,
        _rewrite_graph_views,
    )

    dashboard = yaml.safe_load(
        (
            deployment_dir
            / "dashboards"
            / "workspace-overview.yaml"
        ).read_text(encoding="utf-8")
    )
    panels = {panel["id"]: panel for panel in dashboard["panels"]}
    assert set(panels) == {"live_counts_by_layer", "latest_generations"}
    with psycopg.connect(dsn, autocommit=True) as conn:
        live_sql = _rewrite_graph_views(
            _expand_composite_views(
                panels["live_counts_by_layer"]["sql"],
                conn,
            )
        )
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = store.checkout_generation(generation_id)
        live_rows = graph.query(live_sql)
    finally:
        store.close()
    assert live_rows
    assert sum(int(row["live_count"]) for row in live_rows) > 0
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(panels["latest_generations"]["sql"])
            generation_rows = [dict(row) for row in cur.fetchall()]
    assert generation_rows
    assert any(
        str(row.get("generation_ref") or row.get("generation_id"))
        == generation_id
        for row in generation_rows
    )
    return {
        "live_count_rows": len(live_rows),
        "generation_rows": len(generation_rows),
    }


def _assert_runtime_health(
    *,
    dsn: str,
    env: dict[str, str],
    publish: dict[str, Any],
) -> dict[str, Any]:
    assert publish["phase_a_converged"] is True
    assert publish["phase_b_blocked"] is False
    assert publish["blocked_reason"] is None
    assert all(
        row.get("status") not in {"failed", "blocked", "timeout"}
        for row in publish.get("consistency_gates", [])
    )
    doctor = _run_okg(
        ["doctor", "--deployment", DEPLOYMENT, "--dsn", dsn],
        env=env,
        timeout=600,
    )
    assert doctor.get("ok") is True
    assert doctor.get("errors") == []
    serialized = json.dumps(doctor, sort_keys=True).lower()
    assert '"status": "fail"' not in serialized
    assert '"status": "error"' not in serialized
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT queue_name, workflow_name, availability, dlq_count
                  FROM okg.v_runtime_queue_health
                """
            )
            queues = [dict(row) for row in cur.fetchall()]
    assert all(int(row.get("dlq_count") or 0) == 0 for row in queues)
    assert all(row.get("availability") != "failed" for row in queues)
    return {
        "doctor_status": "ok",
        "consistency_gate_count": len(publish.get("consistency_gates", [])),
        "queue_rows": queues,
    }


def test_generated_workspace_profile_indexes_real_tinydb_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = _create_disposable_database(os.environ[DSN_ENV])
    repo, fixture = _materialize_tinydb(tmp_path)
    overlay = _write_overlay(repo, fixture)
    assert _run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=repo,
    ).stdout.splitlines()
    assert all(
        line[0:2] == "??" and ".okg-test-overlay/" in line
        for line in _run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=repo,
        ).stdout.splitlines()
    )

    deployments_root = tmp_path / "deployments"
    env = _command_env(deployments_root=deployments_root, dsn=dsn)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "human_operator")
    deployment_dir = _scaffold(
        repo=repo,
        overlay=overlay,
        deployments_root=deployments_root,
        dsn=dsn,
        env=env,
    )

    initial_report = _ingest(dsn=dsn, env=env)
    _assert_source_report(initial_report, require_output=True)
    initial_publish = _publish(initial_report)
    initial_generation = str(initial_publish["generation_id"])
    assert initial_publish["node_count"] > 0
    assert initial_publish["edge_count"] > 0
    enricher_report = _assert_enrichers(
        dsn=dsn,
        publish=initial_publish,
    )

    from okg.substrate.graph_store import ChronosGraphStore

    changed_paths = list(fixture["expected_changed_paths"])
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        initial_reader = store.checkout_generation(initial_generation)
        initial_counts = _graph_counts(initial_reader)
        initial_files = _source_files(initial_reader, changed_paths)
        assert set(initial_files) == set(changed_paths)
        overlay_rows = initial_reader.query(
            """
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = 'source_file'
               AND attrs->>'path' LIKE '.okg-test-overlay/%'
            """
        )
        assert overlay_rows
        assert all(bool(row["attrs"]["is_dirty"]) for row in overlay_rows)

        endpoint_rows = initial_reader.query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE subtype = 'interface_endpoint'
               AND attrs->>'path' = '.okg-test-overlay/fixture_api.py'
             ORDER BY node_id
            """
        )
        assert endpoint_rows
        endpoint_id = str(endpoint_rows[0]["node_id"])

        _run(
            [
                "git",
                "checkout",
                "--detach",
                "--force",
                fixture["revisions"]["updated"]["commit"],
            ],
            cwd=repo,
        )
        assert _run(["git", "rev-parse", "HEAD"], cwd=repo).stdout.strip() == (
            fixture["revisions"]["updated"]["commit"]
        )
        assert _run(
            [
                "git",
                "diff",
                "--name-only",
                fixture["revisions"]["initial"]["commit"],
                fixture["revisions"]["updated"]["commit"],
            ],
            cwd=repo,
        ).stdout.splitlines() == changed_paths

        updated_report = _ingest(dsn=dsn, env=env)
        _assert_source_report(updated_report, require_output=False)
        updated_publish = _publish(updated_report)
        updated_generation = str(updated_publish["generation_id"])
        assert updated_generation != initial_generation
        assert updated_publish["nodes_updated"] > 0

        updated_reader = store.checkout_generation(updated_generation)
        updated_counts = _graph_counts(updated_reader)
        updated_files = _source_files(updated_reader, changed_paths)
        assert set(updated_files) == set(changed_paths)
        assert (
            initial_files["tinydb/utils.py"]["git_blob_sha"]
            != updated_files["tinydb/utils.py"]["git_blob_sha"]
        )
        assert (
            initial_files["tests/test_utils.py"]["git_blob_sha"]
            != updated_files["tests/test_utils.py"]["git_blob_sha"]
        )
        assert (
            initial_files["README.rst"]["git_blob_sha"]
            != updated_files["README.rst"]["git_blob_sha"]
        )
        assert (
            initial_files["pyproject.toml"]["git_blob_sha"]
            != updated_files["pyproject.toml"]["git_blob_sha"]
        )
        duplicates = updated_reader.query(
            """
            SELECT attrs->>'path' AS path, count(*) AS n
              FROM okg.graph_nodes
             WHERE subtype = 'source_file'
               AND attrs->>'path' = ANY(:paths)
             GROUP BY attrs->>'path'
            HAVING count(*) <> 1
            """,
            {"paths": changed_paths},
        )
        assert duplicates == []

        assert _graph_counts(initial_reader) == initial_counts
        assert _source_files(initial_reader, changed_paths) == initial_files

        operator_report = _assert_mcp_operators(
            dsn=dsn,
            generation_id=updated_generation,
            endpoint_id=endpoint_id,
        )
        dashboard_report = _assert_dashboard_panels(
            deployment_dir=deployment_dir,
            dsn=dsn,
            generation_id=updated_generation,
        )
        health_report = _assert_runtime_health(
            dsn=dsn,
            env=env,
            publish=updated_publish,
        )

        generations_before_replay = _generation_ids(dsn)
        assert generations_before_replay[0] == updated_generation
        replay_report = _ingest(dsn=dsn, env=env)
        _assert_source_report(replay_report, require_output=False)
        replay_publish = replay_report["publishes"][-1]
        assert replay_publish == {
            "label": "commit",
            "status": "skipped",
            "publish_skipped": True,
            "reason": "no_source_work",
        }
        assert _generation_ids(dsn) == generations_before_replay
        assert _graph_counts(updated_reader) == updated_counts
    finally:
        store.close()

    execution_report = {
        "schema": "okg.workspace-profile-real-repository.v1",
        "deployment": DEPLOYMENT,
        "upstream": fixture["upstream"],
        "overlay": {
            "ownership": "test-owned",
            "path": ".okg-test-overlay",
            "source_files_are_dirty": True,
        },
        "sources": {
            "expected": sorted(SOURCE_NAMES),
            "initial_outcomes": initial_report["results"],
            "updated_outcomes": updated_report["results"],
            "replay_outcomes": replay_report["results"],
        },
        "revisions": {
            "initial": {
                **fixture["revisions"]["initial"],
                "generation_id": initial_generation,
                "counts": initial_counts,
            },
            "updated": {
                **fixture["revisions"]["updated"],
                "generation_id": updated_generation,
                "counts": updated_counts,
                "changed_paths": changed_paths,
            },
        },
        "enrichers": enricher_report,
        "operators": operator_report,
        "dashboards": dashboard_report,
        "health": health_report,
        "replay": {
            "generation_id": updated_generation,
            "noop": True,
            "publish_status": replay_publish["status"],
            "reason": replay_publish["reason"],
            "counts": updated_counts,
        },
    }
    report_path = Path(
        os.environ.get(REPORT_ENV)
        or tmp_path / "workspace-profile-real-repository.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(execution_report, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "report": str(report_path),
                "initial_generation": initial_generation,
                "updated_generation": updated_generation,
                "nodes": updated_counts["nodes"],
                "edges": updated_counts["edges"],
                "sources": len(SOURCE_NAMES),
                "enrichers": len(enricher_report),
            },
            sort_keys=True,
        )
    )
