"""The legacy live-table deletion is NARROW: it took the live complex only.

This file used to assert that `schema_without_legacy_live_table_compatibility`
— a Python filter that stripped the live-table DDL out of schema.sql on the way
to the database — removed the live tables without taking the fact log or the
generation ledger with them.

`schema-canonical-read-path` deleted the filter by applying it: schema.sql no
longer contains the live complex, so there is nothing left to strip and
migrate() executes the file as written. The property the filter had to be
trusted for is now a property of the file, and it is still worth pinning, so
the assertions move onto schema.sql itself. An over-broad deletion — one that
also took `okg.node_facts`, `okg.edge_facts` or `okg.graph_generations`, which
current writers and audit readers still depend on — reds here.
"""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCHEMA_SQL = ROOT / "src" / "okg" / "substrate" / "db" / "schema.sql"


def _sql_without_line_comments(path: Path) -> str:
    return "\n".join(
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )


def test_legacy_live_table_deletion_is_narrow_and_named() -> None:
    schema = _sql_without_line_comments(SCHEMA_SQL)

    # Kept: the append-only fact log and the generation ledger.
    assert "CREATE TABLE IF NOT EXISTS okg.node_facts" in schema
    assert "CREATE TABLE IF NOT EXISTS okg.edge_facts" in schema
    assert "CREATE TABLE IF NOT EXISTS okg.graph_generations" in schema

    # Gone: the live projection complex.
    assert "CREATE TABLE IF NOT EXISTS okg.nodes_live" not in schema
    assert "CREATE TABLE IF NOT EXISTS okg.edges_live" not in schema
    assert "nodes_live" not in schema
    assert "edges_live" not in schema
