"""Substrate-native viz smoke tests (M6.7.4.2).

Boots viz against a substrate test DSN seeded with the
`devops` template fixture, then exercises the core HTTP
endpoints via Starlette's TestClient.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.github import GitHubSource
from okg.substrate.library.sources.jira import JiraSource
from okg.substrate.publisher import Publisher
from okg.substrate.db.pool import close_all
from tests.substrate.conftest import recreate_okg_test_database
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def test_type_filter_parser_accepts_frontend_and_predicate() -> None:
    from okg.viz.endpoints import _parse_type_filter

    predicate = json.dumps({
        "kind": "and",
        "terms": [
            {"kind": "type_not_in", "values": ["document_chunk"]},
            {"kind": "type_in", "values": ["ticket", "jira_issue"]},
        ],
    })
    excluded, included = _parse_type_filter(predicate)
    assert excluded == {"document_chunk"}
    assert included == {"ticket", "jira_issue"}


def _seed_devops(dsn: str, tmp_path: Path) -> None:
    """Bootstrap a `devops`-shaped substrate generation."""
    # Scaffold the template into tmp_path (the runner reads
    # fixture files from the scaffolded dir).
    init = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "init", "--template", "devops",
         "--target", str(tmp_path / "work")],
        capture_output=True, text=True, timeout=30,
    )
    assert init.returncode == 0, init.stderr
    scaffold = tmp_path / "work" / "devops"
    fixtures = scaffold / "fixtures"

    composed = compose_catalog(scaffold)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)

    jira_adapter = JiraSource(fixture_path=str(fixtures / "jira.json"))
    gh_adapter = GitHubSource(fixture_path=str(fixtures / "github.json"))
    SourceRunner(dsn, source_name="devops.jira").run(
        jira_adapter, mode="cursor",
    )
    SourceRunner(dsn, source_name="devops.github").run(
        gh_adapter, mode="cursor",
    )
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out


def _make_ctx(dsn: str, tmp_path: Path):
    """Build a VizContext over a seeded substrate. Skips the
    deployment-dir lookup and constructs the context directly."""
    from okg.viz.context import VizContext
    from okg.viz.config import load_default_config
    from okg.substrate.mcp.session import open_session
    from psycopg_pool import ConnectionPool

    # We need a deployments/devops/deployment.yaml to anchor the
    # manifest read. The substrate template scaffolds one — but
    # the legacy file lives at deployments/devops/. Use whatever
    # exists; the viz tests don't depend on its content beyond
    # `name:`.
    manifest = {"name": "default", "description": ""}
    config = load_default_config()
    pool = ConnectionPool(
        conninfo=dsn, min_size=1, max_size=4,
        kwargs={"autocommit": True}, open=True, name="okg-viz-test",
    )
    with psycopg.connect(dsn, autocommit=True) as seed:
        session = open_session(seed, deployment_name="default")
    return VizContext(
        dsn=dsn, deployment_name="default", manifest=manifest,
        config=config, pool=pool, session=session,
        deployment_dir=tmp_path,
    )


@pytest.fixture(scope="module")
def seeded_app(okg_migrated: str, tmp_path_factory):
    """A FastAPI TestClient against a viz wired to a seeded
    substrate.

    Module-scoped (per the post-bootstrap test-speed audit) so
    the `okg init` subprocess + adapter runs + publish cycle
    happen ONCE per test module instead of once per test. All
    viz tests in this file are read-only against the seeded
    state, so sharing is safe. Wall-clock savings: ~6s on this
    module.
    """
    tmp_path = tmp_path_factory.mktemp("viz")
    # One fresh clone of the migrated template per module (issue #2271),
    # the module-scoped form of clean_runtime.
    recreate_okg_test_database(okg_migrated)
    _seed_devops(okg_migrated, tmp_path)
    ctx = _make_ctx(okg_migrated, tmp_path)
    from okg.viz.app import build_app
    app = build_app(ctx_provider=lambda: ctx, is_ready=lambda: True)
    client = TestClient(app)
    yield client, ctx
    ctx.close()
    close_all()


def test_health_returns_ready(seeded_app) -> None:
    client, ctx = seeded_app
    r = client.get("/viz/api/health")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["ready"] is True
    assert body["node_count"] > 0
    assert body["edge_count"] > 0
    assert body["deployment_name"] == "default"


def test_stats(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/stats")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["node_count"] > 0
    assert body["edge_count"] > 0
    assert sum(body["node_types"].values()) == body["node_count"]
    assert sum(body["edge_types"].values()) == body["edge_count"]


def test_types(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/types")
    assert r.status_code == 200, r.text
    body = r.json()
    # devops fixture has multiple subtypes — at least one with count > 0
    assert sum(body.values()) > 0
    assert all(c > 0 for c in body.values())


def test_node_lookup(seeded_app) -> None:
    client, _ = seeded_app
    # Use a known fixture node from the devops template.
    r = client.get("/viz/api/node/person:alice")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["node_id"] == "person:alice"
    assert "edge_counts" in body


def test_node_404(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/node/does:not:exist")
    assert r.status_code == 404


def test_ontology(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/ontology")
    assert r.status_code == 200
    body = r.json()
    # 0 because the test catalog_version row is empty (no
    # subtypes registered) — the response shape is what matters
    # here, not the content.
    assert "subtypes" in body
    assert "edges" in body


def test_ontology_graph_msgpack(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/ontology_graph")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/msgpack"
    import msgpack
    body = msgpack.unpackb(r.content, raw=False)
    assert "nodes" in body and "edges" in body


def test_expand_msgpack(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/expand/person:alice")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/msgpack"
    import msgpack
    body = msgpack.unpackb(r.content, raw=False)
    assert body["meta"]["request_kind"] == "expand"
    assert body["meta"]["source"] == "person:alice"


def test_expand_prefilters_frontend_and_predicate_before_limit(
    seeded_app,
) -> None:
    client, _ = seeded_app
    # Alice has two inbound neighbors in the devops fixture:
    # pull_request via authored_by and ticket via reported_by. With
    # limit=1, post-filtering samples the pull_request first and the
    # client-side ticket filter hides it. The server must push the
    # frontend's `{kind: "and"}` predicate into SQL before LIMIT.
    predicate = json.dumps({
        "kind": "and",
        "terms": [{"kind": "type_in", "values": ["ticket"]}],
    })
    r = client.get(
        "/viz/api/expand/person:alice",
        params={"direction": "in", "limit": 1, "predicate": predicate},
    )
    assert r.status_code == 200
    import msgpack
    body = msgpack.unpackb(r.content, raw=False)
    assert [n["id"] for n in body["nodes"]] == ["jira:PROJ-1"]
    assert body["nodes"][0]["type"] == "ticket"
    assert body["meta"]["returned_nodes"] == 1


def test_khop_msgpack(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/khop/person:alice?depth=2")
    assert r.status_code == 200
    import msgpack
    body = msgpack.unpackb(r.content, raw=False)
    assert body["meta"]["request_kind"] == "khop"


def test_config_endpoints(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/config")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, dict)


def test_traces_view_uses_substrate_hypertable(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/traces")
    assert r.status_code == 200
    body = r.json()
    assert body["meta"]["request_kind"] == "traces"
    # No tool calls have been made against this substrate yet,
    # so the trace_sessions list is empty.
    assert body["items"] == []


def test_tools_describe(seeded_app) -> None:
    client, _ = seeded_app
    r = client.get("/viz/api/tools")
    assert r.status_code == 200
    body = r.json()
    assert "tools" in body
    # Substrate-shaped allowlist — one entry per tool.
    assert "search_by_type" in body["tools"]


def test_tools_dispatch_search_by_type(seeded_app) -> None:
    client, _ = seeded_app
    r = client.post(
        "/viz/api/tools/search_by_type",
        json={"subtype": "person", "page_size": 10},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["name"] == "search_by_type"
    assert isinstance(body["result"]["items"], list)


def test_tools_dispatch_resolve_alias(seeded_app) -> None:
    client, _ = seeded_app
    r = client.post(
        "/viz/api/tools/resolve_alias",
        json={"name": "alice", "limit": 3},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["name"] == "resolve_alias"
    assert body["result"]["method"] == "alias"
    assert isinstance(body["result"].get("results"), list)


def test_no_legacy_imports() -> None:
    """The new viz tree must not import any legacy-runtime
    module."""
    import sys
    import okg.viz  # noqa: F401
    legacy = (
        "okg.backbone", "okg.core", "okg.persistence",
        "okg.mcp_server", "okg.tools",
        "okg.observability", "okg.live", "okg.audit",
    )
    for mod in list(sys.modules):
        if mod.startswith("okg.viz") or mod == "okg.viz":
            for forbidden in legacy:
                assert not mod.startswith(forbidden), (
                    f"{mod}: viz must not transitively import "
                    f"legacy module {forbidden}"
                )
