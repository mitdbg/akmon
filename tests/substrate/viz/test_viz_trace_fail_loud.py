"""Fail-loud contract for the MCP trace listing endpoint.

Regression guard for the silent-fallback P0: ``/viz/api/traces`` used to
swallow *any* exception into an empty 200 response, so a broken deployment
looked identical to one that simply had no traces. These tests pin the
corrected contract:

- a real backend error (e.g. a dropped connection or a query failure)
  surfaces as a loud 5xx, never a 200 empty;
- a genuinely-absent trace table is a 200 empty that is *signaled*
  (``trace_tables_present: False`` / a ``warnings`` entry), not a bare
  empty list indistinguishable from "queried fine, zero rows";
- a successful query that returns zero rows stays a 200 empty
  (covered by the postgres-backed tests in
  ``test_viz_trace_detail.py``).

The per-session ``/viz/api/trace`` detail route these cases also covered was
removed with the Trace view in issue #1924; only Home's listing read remains.

The router is mounted with a fake ``VizContext``, so these tests need
no database and run in the fast lane.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import psycopg
import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from okg.viz.endpoints import build_router


class _FakeCursor:
    """Cursor whose ``execute`` raises a configured exception."""

    def __init__(self, query_exc: Exception | None) -> None:
        self._query_exc = query_exc

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_a: Any) -> bool:
        return False

    def execute(self, *_a: Any, **_k: Any) -> None:
        if self._query_exc is not None:
            raise self._query_exc

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


class _FakeConn:
    def __init__(self, query_exc: Exception | None) -> None:
        self._query_exc = query_exc

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._query_exc)

    def rollback(self) -> None:
        pass


class _FakeSession:
    generation_id = "okg:fake:gen:0"


class _FakeCtx:
    """Minimal ``VizContext`` stand-in: only what the trace endpoints touch.

    ``conn_exc`` simulates a pool/connection failure (raised on
    ``conn()`` entry). ``query_exc`` simulates a query-time failure
    (raised by ``cursor.execute``); use ``UndefinedTable`` to model an
    absent trace table.
    """

    deployment_name = "fake-deployment"
    session = _FakeSession()

    def __init__(
        self,
        *,
        conn_exc: Exception | None = None,
        query_exc: Exception | None = None,
    ) -> None:
        self._conn_exc = conn_exc
        self._query_exc = query_exc

    @contextmanager
    def conn(self):
        if self._conn_exc is not None:
            raise self._conn_exc
        yield _FakeConn(self._query_exc)


def _client(ctx: _FakeCtx) -> TestClient:
    app = FastAPI()
    app.include_router(build_router(lambda: ctx))
    return TestClient(app)


# --------------------------------------------------------------------
# /viz/api/traces (trace listing)
# --------------------------------------------------------------------

def test_traces_fails_loud_on_connection_error() -> None:
    ctx = _FakeCtx(conn_exc=psycopg.OperationalError("simulated connection loss"))
    r = _client(ctx).get("/viz/api/traces")
    assert r.status_code == 500, r.text


def test_traces_fails_loud_on_query_error() -> None:
    ctx = _FakeCtx(query_exc=psycopg.OperationalError("simulated query failure"))
    r = _client(ctx).get("/viz/api/traces")
    assert r.status_code == 500, r.text


def test_traces_absent_table_is_signaled_empty() -> None:
    # Both the labeled and legacy trace tables missing -> 200 empty, but
    # explicitly flagged so the UI can say "this deployment has no trace
    # tables" rather than "no sessions".
    ctx = _FakeCtx(
        query_exc=psycopg.errors.UndefinedTable("relation okg.trace_sessions_labeled does not exist"),
    )
    r = _client(ctx).get("/viz/api/traces")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["items"] == []
    assert body["meta"]["trace_tables_present"] is False
