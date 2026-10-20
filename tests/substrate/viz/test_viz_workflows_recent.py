from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from okg.viz.endpoints import build_router


class _Cursor:
    description = [
        SimpleNamespace(name=name)
        for name in (
            "trace_id",
            "root_name",
            "root_kind",
            "status_code",
            "duration_ms",
            "started_at",
            "generation_id",
            "workflow_id",
            "span_count",
        )
    ]

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.sql = ""
        self.params: tuple[Any, ...] = ()
        self.rows = rows or []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: Any) -> bool:
        return False

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor


class _Context:
    deployment_name = "okg-test"

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.cursor = _Cursor(rows)

    @contextmanager
    def conn(self):
        yield _Connection(self.cursor)


def _client(ctx: _Context) -> TestClient:
    app = FastAPI()
    app.include_router(build_router(lambda: ctx))
    return TestClient(app)


def test_workflows_recent_orders_by_start_time_and_honors_limit() -> None:
    started_at = datetime(2026, 7, 19, 12, 30, tzinfo=timezone.utc)
    ctx = _Context(rows=[(
        "trace-running",
        "dbos.workflow.okg_source_sync",
        "dbos_workflow",
        "running",
        0,
        started_at,
        "gen-1",
        "workflow-1",
        4,
    )])
    response = _client(ctx).get("/viz/api/workflows?order=recent&limit=20")

    assert response.status_code == 200, response.text
    assert "ORDER BY t.started_at DESC, span_count DESC LIMIT %s" in ctx.cursor.sql
    assert "THEN 'running' ELSE t.status_code END AS status_code" in ctx.cursor.sql
    assert "dbos_workflow" in ctx.cursor.params[1]
    assert ctx.cursor.params[-1] == 20
    assert response.json()["meta"] == {
        "request_kind": "workflows",
        "order": "recent",
        "limit": 20,
        "count": 1,
        "workflow_tables_present": True,
        "root_kinds": response.json()["meta"]["root_kinds"],
    }
    assert response.json()["items"] == [{
        "trace_id": "trace-running",
        "root_name": "dbos.workflow.okg_source_sync",
        "root_kind": "dbos_workflow",
        "status": "running",
        "duration_ms": 0,
        "started_at": started_at.isoformat(),
        "generation_id": "gen-1",
        "workflow_id": "workflow-1",
        "span_count": 4,
    }]


def test_workflows_default_keeps_trace_richness_order() -> None:
    ctx = _Context()
    response = _client(ctx).get("/viz/api/workflows")

    assert response.status_code == 200, response.text
    assert "ORDER BY span_count DESC, t.started_at DESC LIMIT %s" in ctx.cursor.sql
    assert "dbos_workflow" not in ctx.cursor.params[1]
    assert ctx.cursor.params[-1] == 50


def test_workflows_recent_rejects_unbounded_or_unknown_reads() -> None:
    client = _client(_Context())

    assert client.get("/viz/api/workflows?order=unknown").status_code == 422
    assert client.get("/viz/api/workflows?order=recent&limit=101").status_code == 422
