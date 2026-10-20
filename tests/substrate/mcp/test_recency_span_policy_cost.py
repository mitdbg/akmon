"""The recency span is not measured under Nomos runtime enforcement (issue #2760).

On okg-workspace a console search for a common word spent 52 s of 64 s in
`_corpus_span`, which sent its whole sample (up to 5,000 rows) through the
per-record Nomos read filter on every annotated search. The operator decided
that under enforcement the span is not measured at all: no read-filter call,
no endpoints, no count, and no signal about how many rows are hidden. With
enforcement off the filter is a pass-through, so the span is measured over the
whole sample exactly as before.

The cost tests drive both entry points that reach the span: the MCP `search`
entry and the console route `/viz/api/console/records/search`.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Iterator

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp import read_affordances as affordances
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"
SUBJECT = "subject:operator"
TERM = "alpha"
ROWS = 150
#: Rows at EACH end of the instant order that the policy hides.
HIDDEN_AT_EACH_END = 10
BASE = datetime(2024, 1, 1, tzinfo=timezone.utc)
#: Written out rather than imported, so this file also runs against dev.
NOT_MEASURED = "policy_enforced_not_measured"


def _stamp(index: int) -> str:
    return (BASE + timedelta(days=index)).strftime("%Y-%m-%dT%H:%M:%SZ")


class _Pool:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def connection(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn)


class _Ctx:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.deployment_name = DEPLOYMENT
        self.session = SimpleNamespace(branch_name="default")
        self.pool = _Pool(dsn)

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            yield conn


def _rule(rule_id: str, action: str, match: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "effect": "allow",
        "actions": [action],
        "subjects": [SUBJECT],
        "match": match,
        "priority": 100,
        "reason_code": f"nomos.test.{rule_id}",
        **extra,
    }


def publish_dated(dsn: str, *, rows: int, hidden_at_each_end: int) -> str:
    """`rows` dated pages; the `hidden_at_each_end` earliest and latest are
    classified restricted, which the policy below hides."""
    nodes = []
    for index in range(rows):
        restricted = index < hidden_at_each_end or index >= rows - hidden_at_each_end
        node_id = f"page:{index:05d}"
        nodes.append(NodeFact(
            node_id=node_id,
            subtype="page",
            attrs={
                "text": f"{TERM} note {index}",
                "authored_at": _stamp(index),
                "classification": "restricted" if restricted else "internal",
            },
            source_record_id={"id": node_id},
            source_revision={"v": 1},
        ))
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(nodes, source="fixture")
    result = Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published"
    return str(result["generation_id"])


def enforce_policy(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    rules = [
        _rule("ui_boundary", "graph.read.local_operator", {"resource_kind": "local_operator_boundary"}),
        _rule("ui_rows", "graph.read.local_operator", {}),
        _rule("mcp_rows", "graph.read.mcp", {}),
    ]
    for action in ("graph.read.local_operator", "graph.read.mcp"):
        rules.append(_rule(
            f"deny_restricted_{action}", action, {"classification": "restricted"},
            effect="deny", priority=200,
        ))
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:recency-span-cost",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=f"deployment:{DEPLOYMENT}",
            activated_by="test:operator",
        )
        conn.commit()
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)


@pytest.fixture
def filtered_rows(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Rows the recency span sends to the read filter, one entry per call."""
    real_filter = affordances.filter_node_rows_for_session
    sizes: list[int] = []

    def counting(conn, session, rows, **kwargs):
        rows = list(rows)
        sizes.append(len(rows))
        return real_filter(conn, session, rows, **kwargs)

    monkeypatch.setattr(affordances, "filter_node_rows_for_session", counting)
    return sizes


def _mcp_search(dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=DEPLOYMENT)
        return search(conn, session, TERM, method="lexical", limit=10)


def _assert_not_measured(payload: dict[str, Any]) -> None:
    block = payload["recency"]
    assert block["annotated"] is True
    assert block["corpus_span_ordering"] == NOT_MEASURED
    assert block["corpus_span_scanned"] == NOT_MEASURED
    assert (block["corpus_span_lo"], block["corpus_span_hi"]) == (None, None)
    assert block["corpus_span_rows"] is None
    rows = [row for row in payload["results"] if "recency" in row]
    assert rows
    for row in rows:
        assert row["recency"]["corpus_span_position"] is None
        assert row["recency"]["corpus_span_position_basis"] == NOT_MEASURED
        assert row["recency"]["outside_corpus_span"] is False


# ---------- under enforcement: no filter call, nothing disclosed ----------


@pytest.mark.parametrize("hidden", [HIDDEN_AT_EACH_END, 0], ids=["some-hidden", "all-readable"])
def test_mcp_search_does_not_measure_the_span_under_enforcement(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    filtered_rows: list[int],
    hidden: int,
) -> None:
    publish_dated(clean_runtime, rows=ROWS, hidden_at_each_end=hidden)
    enforce_policy(clean_runtime, monkeypatch)

    payload = _mcp_search(clean_runtime)

    assert filtered_rows == [], "the span sent rows through the read filter"
    _assert_not_measured(payload)


def test_console_search_route_does_not_measure_the_span_under_enforcement(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    filtered_rows: list[int],
) -> None:
    pytest.importorskip("fastapi")
    from fastapi import APIRouter, FastAPI
    from fastapi.testclient import TestClient

    from okg.viz.record_endpoints import register_operator_record_routes

    version = publish_dated(clean_runtime, rows=ROWS, hidden_at_each_end=HIDDEN_AT_EACH_END)
    enforce_policy(clean_runtime, monkeypatch)
    # Confirm the console route reaches the recency step at all: count the
    # calls into it, so a green result cannot come from a route that skips it.
    reached: list[int] = []
    search_module = __import__("okg.substrate.mcp.search", fromlist=["search"])
    real_disclosure = search_module.recency_disclosure

    def counting_disclosure(*args, **kwargs):
        reached.append(1)
        return real_disclosure(*args, **kwargs)

    monkeypatch.setattr(search_module, "recency_disclosure", counting_disclosure)
    app = FastAPI()
    router = APIRouter(prefix="/viz/api")
    ctx = _Ctx(clean_runtime)
    register_operator_record_routes(router, ctx_provider=lambda: ctx)
    app.include_router(router)
    response = TestClient(app).get(
        "/viz/api/console/records/search", params={"version": version, "q": TERM},
    )

    assert response.status_code == 200, response.text
    results = response.json()["payload"]["results"]
    assert results
    assert all(
        HIDDEN_AT_EACH_END <= int(row["node_id"].split(":")[1]) < ROWS - HIDDEN_AT_EACH_END
        for row in results
    )
    assert reached, "the console route no longer reaches the recency step"
    assert filtered_rows == [], "the span sent rows through the read filter"


# ---------- enforcement off: the whole sample, measured exactly ----------


def test_span_without_enforcement_is_the_whole_sample_answer(
    clean_runtime: str, filtered_rows: list[int],
) -> None:
    publish_dated(clean_runtime, rows=ROWS, hidden_at_each_end=0)

    payload = _mcp_search(clean_runtime)

    block = payload["recency"]
    assert filtered_rows == [ROWS]
    assert block["corpus_span_ordering"] == "measured_instants"
    assert (block["corpus_span_lo"], block["corpus_span_hi"]) == (_stamp(0), _stamp(ROWS - 1))
    assert block["corpus_span_rows"] == ROWS
    assert block["corpus_span_scanned"] == "all"
    for row in payload["results"]:
        if "recency" in row:
            assert row["recency"]["corpus_span_position_basis"] == "measured"
            assert 0.0 <= row["recency"]["corpus_span_position"] <= 1.0
