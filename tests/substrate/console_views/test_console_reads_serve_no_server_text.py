"""Every console read, on a real publication seeded with leaks, serves none.

Issue #2130, PACT console-reads-mask-paths-and-exception-text. A graph is
published through the publisher with the values the live sweep of 2026-09-22
found leaving the server: a record attribute holding an absolute path inside a
NESTED object, an agent session's working directory, a blocked attempt whose
recorded blocker is exception text, and a failed view run whose error carries a
traceback. The deployment directory is itself an absolute path under the test's
temporary root. Then every console GET route is called -- the recipes are the
database-free file's, which fails when a route has none -- and every body is
swept for a path, a connection string, a host, an address, a password or
exception text.

Record CONTENT is outside the rule (#2179): the document's body text below is
plain prose, so a hit can only come from an identity or provenance field the
console composed.
"""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp.session import open_session  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.viz.app import build_app  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate._db_helpers import seed_catalog_version  # noqa: E402
from tests.substrate.console_views.test_served_text import (  # noqa: E402
    LEAK_EXCEPTION,
    LEAK_HOME,
    LEAK_TRACEBACK,
    RECIPES,
    hits,
)

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"
SOURCE = "fixture"
VIEW = "leak_probe"
DOC = "document:leak-probe"
SESSION = "agent_session:leak-probe"


class _Pool:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def connection(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn)


class _Ctx:
    """The slice of VizContext the console routes read, on a real database."""

    def __init__(self, dsn: str, deployment_dir: Path) -> None:
        self.dsn = dsn
        self.deployment_name = DEPLOYMENT
        self.deployment_dir = deployment_dir
        self.manifest: dict[str, Any] = {}
        self.config: dict[str, Any] = {}
        self.pool = _Pool(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn:
            self.session = open_session(conn, deployment_name=DEPLOYMENT)

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            yield conn


def _publish(dsn: str, facts: list[Any]) -> dict[str, Any]:
    if facts:
        with psycopg.connect(dsn, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(facts, source=SOURCE)
    return Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()


def _insert_failed_view_run(dsn: str, *, target_generation_id: int) -> None:
    """A failed view run, written the way the view runner writes one."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.derived_artifact_definitions (
              definition_id, artifact_kind, deployment_name, logical_key,
              compute_hash, output_schema_hash, renderer_hash, dirty_rule_hash,
              renderer_ref
            ) VALUES (
              %(view)s, 'view', %(deployment)s, %(view)s,
              'h', 'h', 'h', 'h', 'tests.renderer'
            ) ON CONFLICT (definition_id) DO NOTHING
            """,
            {"view": VIEW, "deployment": DEPLOYMENT},
        )
        cur.execute(
            """
            INSERT INTO okg.derived_artifact_runs (
              run_id, definition_id, target_generation_id, status, trigger_kind,
              compute_hash, output_schema_hash, renderer_hash, dirty_rule_hash,
              error, completed_at
            ) VALUES (
              %(run_id)s, %(view)s, %(target)s, 'failed', 'full',
              'h', 'h', 'h', 'h', %(error)s, now()
            )
            """,
            {
                "run_id": str(uuid.uuid4()),
                "view": VIEW,
                "target": target_generation_id,
                "error": psycopg.types.json.Jsonb({
                    "error_type": "ValueError",
                    "reason": f"{LEAK_HOME}/inbox/secret.yaml",
                    "message": LEAK_EXCEPTION,
                    "traceback": LEAK_TRACEBACK,
                }),
            },
        )


@pytest.fixture
def seeded(clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    published = _publish(clean_runtime, [
        NodeFact(
            node_id=DOC, subtype="document",
            attrs={
                "title": "Leak probe document",
                "text": "A plain paragraph about a leak probe.",
                # Nested: the formatter flattens this to one line that does
                # not START with `/`, which is how it slipped past masking.
                "fsm": {"path": f"{LEAK_HOME}/deployments/x/coordination_fsm.yaml"},
            },
            source_record_id={"id": DOC}, source_revision={"v": 1},
        ),
        NodeFact(
            node_id=SESSION, subtype="agent_session",
            attrs={
                "title": "leak probe session",
                "cwd": f"{LEAK_HOME}/projects/okg",
                "git_branch": "claude/leak-probe",
            },
            source_record_id={"id": SESSION}, source_revision={"v": 1},
        ),
        # So the Graph record read of DOC has a neighbour group to serve,
        # labelled from a record whose attributes hold a server path.
        EdgeFact(src=DOC, dst=SESSION, edge_type="mentions"),
    ])
    assert published["status"] == "published", published
    version = str(published["checkpoint_id"]) if "checkpoint_id" in published else None

    # A blocked attempt whose recorded blocker is exception text and a path.
    monkeypatch.setattr(
        Publisher, "_nomos_generation_publish_blocker",
        lambda self, conn, **_: {"message": LEAK_EXCEPTION, "path": f"{LEAK_HOME}/x"},
    )
    blocked = _publish(clean_runtime, [
        NodeFact(
            node_id="document:leak-probe-2", subtype="document",
            attrs={"title": "second"},
            source_record_id={"id": "document:leak-probe-2"}, source_revision={"v": 1},
        ),
    ])
    monkeypatch.undo()
    assert blocked["status"] == "blocked", blocked

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generations = store.list_generations(
            deployment_name=DEPLOYMENT, branch="default", status=None,
        )
    published_gen = next(g for g in generations if (g.metadata or {}).get("status") == "published")
    blocked_gen = next(g for g in generations if (g.metadata or {}).get("status") == "blocked")
    target = int((published_gen.metadata or {})["target_generation_id"])
    _insert_failed_view_run(clean_runtime, target_generation_id=target)

    # The deployment directory is an absolute path, as on every server.
    deployment_dir = tmp_path / LEAK_HOME.lstrip("/") / "deployments" / DEPLOYMENT
    deployment_dir.mkdir(parents=True)
    # The Graph ontology read composes the deployment's declared schema; give
    # it one, so the sweep reads its real answer rather than a refusal.
    (deployment_dir / "deployment.yaml").write_text(
        f"name: {DEPLOYMENT}\nmodules: []\n", encoding="utf-8",
    )
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "schemas" / "leak_probe.yaml").write_text(
        "id: https://example.org/leak-probe\nname: leak_probe\ndefault_range: string\n"
        "classes:\n  Document:\n    attributes:\n      title: {}\n      text: {}\n      fsm: {}\n"
        "  AgentSession:\n    attributes:\n      title: {}\n      cwd: {}\n",
        encoding="utf-8",
    )
    (deployment_dir / "views_registry.yaml").write_text(
        f"deployment: {DEPLOYMENT}\nviews:\n"
        f"  - view_def_id: {VIEW}\n"
        "    view_name: Leak probe\n"
        "    view_def_factory: views.leak_probe:view_def\n"
        "    build_corpus: views.leak_probe:build_corpus\n"
        "    schedule: \"\"\n    params: {}\n    llm_client: stub\n",
        encoding="utf-8",
    )
    return {
        "dsn": clean_runtime,
        "deployment_dir": deployment_dir,
        "state": {
            "attempt": blocked_gen.checkpoint_id,
            "source": SOURCE,
            "view": VIEW,
            "record": DOC,
            "version": version or published_gen.checkpoint_id,
        },
    }


def test_no_console_read_serves_a_server_path_or_exception_text(
    seeded: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn, deployment_dir, state = seeded["dsn"], seeded["deployment_dir"], seeded["state"]
    # The readiness report's live checks open their own connections and read
    # the host's disk; they run for real against this database.
    ctx = _Ctx(dsn, deployment_dir)
    client = TestClient(
        build_app(ctx_provider=lambda: ctx, is_ready=lambda: True),
        raise_server_exceptions=False,
    )
    leaked: dict[str, list[str]] = {}
    statuses: dict[str, int] = {}
    bodies: dict[str, Any] = {}
    for route, recipe in sorted(RECIPES.items()):
        response = client.get(route, params=recipe(state))
        statuses[route] = response.status_code
        try:
            body: Any = response.json()
        except ValueError:
            body = response.text
        bodies[route] = body
        # The deployment's own temporary root is a path too.
        found = hits(body) + [
            hit for hit in hits(body, pattern=_literal(str(deployment_dir.parents[3])))
        ]
        if found:
            leaked[route] = found
    assert leaked == {}, leaked

    # The seeded values reached the reads that serve them, so the sweep above
    # is not vacuous.
    detail = bodies["/viz/api/console/records/detail"]["payload"]
    assert any(key.startswith("attrs.fsm") for key in detail["masked_paths"]), detail["masked_paths"]
    worktrees = bodies["/viz/api/console/worktrees"]["payload"]["worktrees"]
    assert [row["label"] for row in worktrees] == ["okg"]
    assert "cwd" not in worktrees[0]
    views = bodies["/viz/api/console/view-diagnostics"]["payload"]
    assert views["read_authority"]["registry"] == "views_registry.yaml"
    assert views["views"][0]["latest_run"]["error"]["error_text_withheld"] is True
    rows = bodies["/viz/api/console/generations"]["payload"]["generations"]
    blocked = [row for row in rows if row["status"] == "blocked"]
    assert blocked and blocked[0]["blocked_reason"] is None
    assert blocked[0]["blocked_reason_unavailable_reason"] == "blocked_reason_not_code_shaped"
    # The Graph reads answered with real content, so their sweep counts.
    graph_types = bodies["/viz/api/console/graph/types"]["payload"]
    assert {row["type"] for row in graph_types["types"]} >= {"document", "agent_session"}
    graph_record = bodies["/viz/api/console/graph/record"]["payload"]
    assert graph_record["record"]["id"] == DOC
    assert [(g["relationship"], g["type"], g["count"]) for g in graph_record["groups"]] == [
        ("mentions", "agent_session", 1),
    ]
    assert graph_record["groups"][0]["items"][0]["id"] == SESSION
    ontology = bodies["/viz/api/console/graph/ontology"]["payload"]
    assert {a["name"] for a in ontology["fill"]["agent_session"]["attributes"]} == {"title", "cwd"}
    source_links = bodies["/viz/api/console/graph/source-links"]["payload"]
    assert source_links["rows"], source_links
    # Every route answered; a 5xx would mean its success path was not swept.
    assert {route: code for route, code in statuses.items() if code >= 500} == {}, {
        route: bodies[route] for route, code in statuses.items() if code >= 500
    }


def _literal(text: str) -> Any:
    import re

    return re.compile(re.escape(text))
