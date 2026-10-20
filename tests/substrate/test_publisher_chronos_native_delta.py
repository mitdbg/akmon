from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.publisher.publisher import Publisher


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def execute(self, sql: str, params: dict[str, object] | None = None) -> None:
        self.calls.append((sql, dict(params) if params else {}))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def rollback(self) -> None:
        return None


def test_chronos_derived_fact_loader_applies_current_cycle_bounds() -> None:
    conn = _FakeConnection()

    facts = Publisher._load_unapplied_derived_facts(
        conn,
        generation_id=17,
        min_node_fact_id=101,
        min_edge_fact_id=202,
    )

    assert facts == []
    node_sql, node_params = conn.cursor_obj.calls[0]
    edge_sql, edge_params = conn.cursor_obj.calls[1]
    assert "source_revision->>'generation_id' = %(generation_id)s" in node_sql
    assert "fact_id > %(min_node_fact_id)s" in node_sql
    assert "fact_id > %(min_edge_fact_id)s" in edge_sql
    assert "okg.job_" not in node_sql
    assert "okg.job_" not in edge_sql
    assert node_params == {
        "generation_id": "17",
        "min_node_fact_id": 101,
        "min_edge_fact_id": 202,
    }
    assert edge_params == node_params


def test_enforce_mode_gates_derived_edges_on_admission() -> None:
    conn = _FakeConnection()
    Publisher._load_unapplied_derived_facts(conn, enforcement_mode="enforce")
    edge_sql = conn.cursor_obj.calls[1][0]
    assert "edge_fact_range" in edge_sql
    assert "'accepted', 'published'" in edge_sql


def test_audit_and_off_modes_project_derived_edges_without_admission_gate() -> None:
    for mode in ("audit", "off"):
        conn = _FakeConnection()
        Publisher._load_unapplied_derived_facts(conn, enforcement_mode=mode)
        edge_sql = conn.cursor_obj.calls[1][0]
        assert "'accepted', 'published'" not in edge_sql, mode
        assert "edge_fact_range" not in edge_sql, mode
        assert "'_enricher:%%'" in edge_sql, mode
        assert "'_extractor:%%'" in edge_sql, mode


def test_derived_fact_loader_fails_loudly_when_core_fact_tables_are_missing() -> None:
    conn = _FakeConnection()

    def raise_undefined_table(
        sql: str,
        params: dict[str, object] | None = None,
    ) -> None:
        raise psycopg.errors.UndefinedTable("okg.node_facts is missing")

    conn.cursor_obj.execute = raise_undefined_table  # type: ignore[method-assign]

    with pytest.raises(psycopg.errors.UndefinedTable):
        Publisher._load_unapplied_derived_facts(conn)
