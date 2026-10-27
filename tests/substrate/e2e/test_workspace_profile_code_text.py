"""Live proof for the opt-in `code_text` source of the workspace-graph profile.

A profile install stores each function's signature and docstring and never the
function itself. `code_text` adds the text, costs about 20 times the code's
bytes in the database, and so ships switched off. This lane walks the whole
operator story on a disposable database with the real command line:

1. install the profile over a small git repository and publish;
   no document or chunk exists, because `code_text` is dormant;
2. a corpus over the source's size cap is refused and stores nothing;
3. switch the source on, ingest and publish: documents and chunks appear,
   no source reports an error and the doctor passes (the workflow queue
   tables do not exist for an inline ingest, so queue health is only checked
   when they do);
4. a search for a word found only inside a function body returns that
   function's chunk with its symbol and lines, and reading the function's
   `symbol:` node names the document that holds its text.

It runs in the `e2e-publish-gate` CI lane, on the lane's test PostgreSQL
server. NEVER ON A LIVE GRAPH: the test connects to the server's maintenance
database only to create ONE new, uniquely named database derived from the given
DSN, builds the deployment in that, and drops exactly that database at the end.
It never connects to, migrates or writes the database the DSN names. The DSN is
`OKG_WORKSPACE_PROFILE_DSN` when set, else the lane's test DSN
(`OKG_TEST_DSN` or `OKG_TEST_DSN_TEMPLATE`, see `tests/substrate/conftest.py`);
with neither the test skips.

    OKG_WORKSPACE_PROFILE_DSN=postgresql://.../okg_test_code_text_live \\
      pytest tests/substrate/e2e/test_workspace_profile_code_text.py -q --no-cov
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from tests.substrate.e2e.test_workspace_profile_real_repository import (
    DSN_ENV,
    REPO_ROOT,
    _create_disposable_database,
    _dsn_parts,
    _run,
    _run_okg,
)

DEPLOYMENT = "code-text-live"
REPORT_ENV = "OKG_CODE_TEXT_LIVE_REPORT"
BODY_ONLY_WORD = "zanzibarquux"

pytestmark = [pytest.mark.postgres]


def _base_dsn() -> str:
    """The DSN a disposable database is derived from; skips when there is none.

    The explicit variable wins. Otherwise the lane's test DSN, resolved by the
    one function every test under `tests/` uses for it (it skips when neither
    `OKG_TEST_DSN` nor `OKG_TEST_DSN_TEMPLATE` is set).
    """
    explicit = os.environ.get(DSN_ENV)
    if explicit:
        return explicit
    from tests.substrate.conftest import (
        _assert_test_db_target_is_safe_for_mutation,
        require_okg_test_dsn,
    )

    dsn = require_okg_test_dsn()
    # The test DSN names a database the fixtures own (`okg_test*`); refuse to
    # derive from anything else.
    _assert_test_db_target_is_safe_for_mutation(
        dsn, operation="derive a disposable database for the code_text lane",
    )
    return dsn


@pytest.fixture
def live_dsn():
    base_dsn = _base_dsn()
    base_name = _dsn_parts(base_dsn)["dbname"]
    dsn = _create_disposable_database(base_dsn)
    child_name = _dsn_parts(dsn)["dbname"]
    # Never the given database: a new name that extends it.
    assert child_name != base_name and child_name.startswith(base_name[:20])
    try:
        yield dsn
    finally:
        parts = _dsn_parts(dsn)
        assert parts["dbname"] == child_name
        server = {**parts, "dbname": "postgres"}
        with psycopg.connect(**server, autocommit=True) as conn:
            conn.execute(
                psycopg.sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    psycopg.sql.Identifier(parts["dbname"])
                )
            )


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    root.mkdir()
    _write(root, "README.md", "# Workspace\n\nA small repository.\n")
    _write(root, "pyproject.toml", "[project]\nname = 'workspace'\n")
    _write(
        root, "src/app.py",
        "def greet(name):\n"
        '    """Say hello."""\n'
        "    return 'hello ' + name\n"
        "\n"
        "\n"
        "def compute_total(values):\n"
        '    """Add the values."""\n'
        f"    marker = '{BODY_ONLY_WORD}'\n"
        "    return sum(values), marker\n",
    )
    _write(root, "tests/test_app.py", "def test_greet():\n    assert 'testonlybody'\n")
    _write(root, "vendor/lib.py", "VENDORED = 'vendoredonlybody'\n")
    for args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.email=f@example.test", "-c", "user.name=Fixture",
         "-c", "commit.gpgsign=false", "commit", "-q", "-m", "fixture"],
    ):
        _run(["git", *args], cwd=root)
    return root


def _command_env(deployments_root: Path, dsn: str) -> dict[str, str]:
    env = os.environ.copy()
    for key in (
        "OKG_DEPLOYMENT", "PZ_OKG_DEPLOYMENT", "OKG_DSN", "OKG_DEPLOYMENTS_DIR",
        "OKG_PACT_DECISION_PROJECTION", "OKG_PACT_GRAPH_DEPLOYMENT",
    ):
        env.pop(key, None)
    env.update({
        "OKG_DEPLOYMENT": DEPLOYMENT,
        "OKG_DEPLOYMENTS_DIR": str(deployments_root),
        "OKG_DSN": dsn,
        "OKG_PACT_GRAPH_DEPLOYMENT": DEPLOYMENT,
        "OKG_NOMOS_SUBJECT": "human_operator",
    })
    return env


def _install(repo: Path, tmp_path: Path, dsn: str, env: dict[str, str]) -> Path:
    deployments_root = Path(env["OKG_DEPLOYMENTS_DIR"])
    _run_okg(
        [
            "install", "--profile", "workspace-graph",
            "--deployment-name", DEPLOYMENT,
            "--deployments-root", str(deployments_root),
            "--postgres-dsn", dsn,
            "--repo-root", str(repo),
            "--codex-sessions-root", str(tmp_path / "sessions"),
            "--codex-archived-sessions-root", str(tmp_path / "archived"),
            "--since", "all",
            "--no-publish", "--non-interactive",
        ],
        env=env,
    )
    deployment_dir = deployments_root / DEPLOYMENT
    _run_okg(
        ["migrate", "--deployment", DEPLOYMENT, "--dsn", dsn, "--apply"], env=env,
    )
    from tests.substrate.conftest import enable_okg_mcp_login

    enable_okg_mcp_login(dsn)
    from okg.substrate.cli.init import _seed_init_nomos_baseline_if_required

    prior_root = os.environ.get("OKG_DEPLOYMENTS_DIR")
    os.environ["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
    try:
        _seed_init_nomos_baseline_if_required(
            deployment_name=DEPLOYMENT, deployment_dir=deployment_dir, dsn=dsn,
        )
    finally:
        if prior_root is None:
            os.environ.pop("OKG_DEPLOYMENTS_DIR", None)
        else:
            os.environ["OKG_DEPLOYMENTS_DIR"] = prior_root
    _run_okg(
        ["catalog", "load", "--deployment", str(deployment_dir), "--dsn", dsn,
         "--apply"],
        env=env,
    )
    return deployment_dir


def _ingest(
    include: list[str], dsn: str, env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """`okg ingest --include ...`, returning the process whatever its exit code."""
    args = [
        sys.executable, "-m", "okg.substrate.cli.main", "ingest",
        "--deployment", DEPLOYMENT, "--dsn", dsn, "--inline",
        "--mode", "scope_complete", "--json",
    ]
    for name in include:
        args += ["--include", name]
    return subprocess.run(
        args, cwd=REPO_ROOT, env=env, capture_output=True, text=True,
        errors="replace", timeout=1800,
    )


def _tail(proc: subprocess.CompletedProcess[str]) -> str:
    """What an ingest said, minus the per-phase timing telemetry lines."""
    lines = [
        line for line in (proc.stdout + "\n" + proc.stderr).splitlines()
        if "okg.source_tail" not in line
    ]
    summary = ""
    try:
        report = json.loads(proc.stdout)
        summary = json.dumps(
            {
                "keys": sorted(report),
                "results": [
                    {key: row.get(key) for key in ("name", "status", "error", "reason")}
                    for row in report.get("results") or []
                ],
                "publishes": [
                    {
                        key: value for key, value in row.items()
                        if key in {"status", "label", "reason", "code"}
                        or any(
                            word in key
                            for word in ("block", "refus", "gate", "fail", "error")
                        )
                    }
                    for row in report.get("publishes") or []
                ],
                "warnings": report.get("warnings"),
                "errors": report.get("errors"),
            },
            default=str,
        )
    except (json.JSONDecodeError, AttributeError):
        pass
    return "\n".join(lines)[-2500:] + "\n" + summary[-3500:]


def _report(proc: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        pytest.fail(
            "okg ingest did not emit one JSON document\n"
            f"stdout tail:\n{proc.stdout[-4000:]}\nstderr tail:\n{proc.stderr[-4000:]}"
        )
    assert isinstance(payload, dict)
    return payload


def _graph_counts(dsn: str, generation_id: str) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        reader = store.checkout_generation(generation_id)
        rows = reader.query(
            "SELECT subtype, count(*) AS n FROM okg.graph_nodes "
            "WHERE subtype IN ('document', 'document_chunk') GROUP BY subtype"
        )
    finally:
        store.close()
    counts = {"document": 0, "document_chunk": 0}
    counts.update({str(row["subtype"]): int(row["n"]) for row in rows})
    return counts


def _document_paths(dsn: str, generation_id: str) -> set[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        reader = store.checkout_generation(generation_id)
        rows = reader.query(
            "SELECT attrs->>'path' AS path FROM okg.graph_nodes "
            "WHERE subtype = 'document'"
        )
    finally:
        store.close()
    return {str(row["path"]) for row in rows}


def _latest_published_generation(dsn: str) -> str:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        latest = store.latest_generation(deployment_name=DEPLOYMENT)
    finally:
        store.close()
    assert latest is not None
    return str(latest.checkpoint_id)


def _published_generation(report: dict[str, Any]) -> str:
    publishes = report.get("publishes") or []
    assert publishes, report
    assert publishes[-1]["status"] == "published", publishes[-1]
    return str(publishes[-1]["generation_id"])


def _health(dsn: str, env: dict[str, str], publish_report: dict[str, Any]) -> dict[str, Any]:
    publish = (publish_report.get("publishes") or [])[-1]
    assert publish["phase_a_converged"] is True
    assert publish["phase_b_blocked"] is False
    doctor = _run_okg(
        ["doctor", "--deployment", DEPLOYMENT, "--dsn", dsn], env=env, timeout=600,
    )
    assert doctor.get("ok") is True
    assert doctor.get("errors") == []
    # An ingest run with --inline never creates the workflow queue tables.
    # When they are absent the queue-health view returns a placeholder row
    # ("unavailable", zero counts), which is not a measurement; this check
    # says so and asserts only that placeholder. When the tables exist, the
    # failure-queue and failed-workflow counts are real and must be zero.
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT to_regclass('dbos.workflow_status') IS NOT NULL AS present"
            )
            queue_tables_present = bool(cur.fetchone()["present"])
            cur.execute(
                "SELECT queue_name, workflow_name, availability, dlq_count "
                "FROM okg.v_runtime_queue_health"
            )
            queues = [dict(row) for row in cur.fetchall()]
    if queue_tables_present:
        assert all(int(row.get("dlq_count") or 0) == 0 for row in queues)
        assert all(row.get("availability") != "failed" for row in queues)
    else:
        assert queues and all(row["availability"] == "unavailable" for row in queues)
    # What the inline ingest itself can show: no source reported an error.
    results = publish_report.get("results") or []
    assert results, publish_report
    assert not [row for row in results if row.get("error")], results
    return {
        "doctor_ok": True,
        "queue_tables_present": queue_tables_present,
        "queue_measured": queue_tables_present,
        "queue_rows": queues,
        "source_results": [
            {key: row.get(key) for key in ("name", "status")} for row in results
        ],
    }


def _read_it(dsn: str, generation_id: str) -> dict[str, Any]:
    """One search read and one `inspect` read through the real MCP tools."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp.server import build_server

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        reader = store.checkout_generation(generation_id)
        symbols = reader.query(
            "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'code_symbol' "
            "AND attrs->>'name' = 'compute_total'"
        )
    finally:
        store.close()
    assert symbols, "code_structure stored no compute_total symbol"
    symbol_id = str(symbols[0]["node_id"])

    server = build_server(
        dsn=dsn,
        deployment_name=DEPLOYMENT,
        dsn_authority={
            "dsn": dsn,
            "authority": {"mode": "test_fixture", "deployment": DEPLOYMENT},
            "target": None,
        },
    )
    tools = server._tool_manager._tools
    try:
        searched = tools["search"].fn(
            query=BODY_ONLY_WORD, method="grep", generation_id=generation_id,
        )
        inspected = tools["inspect"].fn(
            target={"kind": "text", "node_id": symbol_id},
            generation_id=generation_id,
        )
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()
    return {"symbol_id": symbol_id, "search": searched, "inspect": inspected}


def test_code_text_is_off_until_switched_on_then_stores_function_text(
    tmp_path: Path, live_dsn: str,
) -> None:
    dsn = live_dsn
    repo = _fixture_repo(tmp_path)
    env = _command_env(tmp_path / "deployments", dsn)
    deployment_dir = _install(repo, tmp_path, dsn, env)
    registry_path = deployment_dir / "source_registry.yaml"

    # 1. Dormant: the profile ships the source switched off.
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    assert registry["sources"]["code_text"]["enabled_by_default"] is False
    before_proc = _ingest(["code_structure"], dsn, env)
    assert before_proc.returncode == 0, _tail(before_proc)
    before = _report(before_proc)
    assert "code_text" not in before["ordered"]
    before_generation = _published_generation(before)
    before_counts = _graph_counts(dsn, before_generation)
    assert before_counts == {"document": 0, "document_chunk": 0}

    # 2. Over the cap: the run is refused and the graph is unchanged.
    registry["sources"]["code_text"]["enabled_by_default"] = True
    registry["sources"]["code_text"]["params"]["max_total_bytes"] = 100
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    refused_proc = _ingest(["code_text"], dsn, env)
    refused_text = refused_proc.stdout + refused_proc.stderr
    assert refused_proc.returncode != 0
    assert "doc_corpus_oversize" in refused_text or "max_total_bytes" in refused_text
    assert "largest top-level folders" in refused_text
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        stored_after_refusal = store.checkout_generation(before_generation).query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            "WHERE subtype IN ('document', 'document_chunk')"
        )
    finally:
        store.close()
    assert int(stored_after_refusal[0]["n"]) == 0
    # The refusal published nothing: the newest generation is still the one
    # from before it (a count in the old generation alone could not show that).
    assert _latest_published_generation(dsn) == before_generation

    # 3. On, with the shipped cap: ingest and publish.
    registry["sources"]["code_text"]["params"]["max_total_bytes"] = 64 * 1024 * 1024
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    # The registry edit changes what the deployment publishes, so the next
    # publish is refused until the operator claims the new manifest.
    _run_okg(
        ["catalog", "ownership", "claim", "--deployment", str(deployment_dir)],
        env=env,
    )
    after_proc = _ingest(["code_text"], dsn, env)
    assert after_proc.returncode == 0, _tail(after_proc)
    after = _report(after_proc)
    after_generation = _published_generation(after)
    assert after_generation != before_generation
    assert _latest_published_generation(dsn) == after_generation
    after_counts = _graph_counts(dsn, after_generation)
    assert after_counts["document"] > 0 and after_counts["document_chunk"] > 0
    paths = _document_paths(dsn, after_generation)
    assert "src/app.py" in paths
    # Test code and vendored code are left out by default.
    assert "tests/test_app.py" not in paths
    assert "vendor/lib.py" not in paths
    health = _health(dsn, env, after)

    # 4. Read: the body-only word finds its function; inspect names the document.
    read = _read_it(dsn, after_generation)
    hits = [
        item for item in (read["search"].get("results") or [])
        if BODY_ONLY_WORD in json.dumps(item)
    ]
    assert hits, read["search"]
    # The exact-text search answers with the document and the matching line.
    assert hits[0]["node_id"] == f"doc:{DEPLOYMENT}:src/app.py", hits[0]
    assert f"marker = '{BODY_ONLY_WORD}'" in json.dumps(hits[0]), hits[0]
    outcome = json.dumps(read["inspect"].get("outcome") or read["inspect"])
    assert f"doc:{DEPLOYMENT}:src/app.py" in outcome

    report = {
        "schema": "okg.code-text-live.v1",
        "before": {"generation_id": before_generation, "counts": before_counts},
        "after": {"generation_id": after_generation, "counts": after_counts,
                  "document_paths": sorted(paths)},
        "health": health,
        "search_hit": hits[0],
        "inspect_outcome": read["inspect"].get("outcome"),
    }
    report_path = Path(os.environ.get(REPORT_ENV) or tmp_path / "code-text-live.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"report": str(report_path), "before": before_counts,
                      "after": after_counts}, sort_keys=True))
