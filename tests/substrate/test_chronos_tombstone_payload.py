"""Delete markers keep only the columns a reader uses (okg#3284).

A delete in the Chronos interval backend writes a marker: a physical row with
``deleted = TRUE``. Upstream Chronos copies every column of the deleted row
into it. The OKG-local option (``set_tombstone_keep``) makes a marker keep the
primary key, the interval columns and a per-table keep set, and NULL the rest.

These tests drive every marker write path on real PostgreSQL and check, per
path, that a marker's kept columns equal the deleted row's and every other
column is NULL; that live rows (including live remainders) keep every column;
that a re-insert after a delete carries the inserted row; that the option
unset gives the upstream full copy; and that the backfill script clears only
non-kept columns of markers outside any branch's current segment, and only
when psql is given a true confirm_clear.
"""
from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest

from chronos_core.branching import BranchingError, ChronosBranchContext
from chronos_core.branching import _interval_backend as interval_backend
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.schema import (
    TOMBSTONE_KEEP_COLUMNS,
    configure_tombstone_payload,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher

pytestmark = pytest.mark.postgres

REPO = Path(__file__).resolve().parents[2]
BACKFILL_SQL = REPO / "scripts" / "storage" / "clear_tombstone_payloads.sql"

INTERVAL_COLUMNS = frozenset({"live_lo", "live_hi", "writer_segment_id", "deleted"})

TABLE = "tomb_t"
COLUMNS = ("k", "kind", "payload", "blob", "n")
KEEP = ("kind",)
DROPPED = ("payload", "blob", "n")


@pytest.fixture
def keep_settings() -> Iterator[None]:
    """Restore the process-wide keep sets after a test changes them."""
    saved, saved_default = interval_backend.tombstone_keep_settings()
    try:
        yield
    finally:
        interval_backend._TOMBSTONE_KEEP.clear()
        interval_backend._TOMBSTONE_KEEP.update(saved)
        interval_backend.set_tombstone_keep_default(saved_default)


def _upstream_settings() -> None:
    """The state of a process that never applied OKG's keep sets."""
    interval_backend._TOMBSTONE_KEEP.clear()
    interval_backend.set_tombstone_keep_default(None)


def _row(key: str, version: int = 1) -> dict[str, Any]:
    return {
        "k": key,
        "kind": f"kind-{key}",
        "payload": {"key": key, "version": version, "pad": "x" * 64},
        "blob": f"blob-{key}-{version}",
        "n": version,
    }


@pytest.fixture
def generic(clean_runtime: str, keep_settings: None) -> Iterator[Any]:
    """A Chronos context with a registered test table and its own branch."""
    ctx = ChronosBranchContext.connect(clean_runtime, ensure_metadata=True)
    ctx.db.execute(
        f"CREATE TABLE {TABLE} (k TEXT PRIMARY KEY, kind TEXT NOT NULL, "
        "payload JSONB, blob TEXT, n INTEGER)"
    )
    ctx.db.commit()
    ctx.register_table(TABLE, ["k"])
    branch = f"tomb_{uuid.uuid4().hex[:8]}"
    ctx.create_branch(branch, "main")
    ctx.test_branch = branch  # type: ignore[attr-defined]
    try:
        yield ctx
    finally:
        ctx.close()


def _physical(ctx: Any, table: str = TABLE) -> list[dict[str, Any]]:
    meta = ctx._backend.tables[table]
    rows = ctx.db.execute(
        f'SELECT * FROM "{meta.physical_name}" ORDER BY 1, live_lo'
    ).fetchall()
    ctx.db.commit()
    return [dict(row) for row in rows]


def _assert_markers(
    rows: list[dict[str, Any]],
    originals: dict[str, dict[str, Any]],
    *,
    keep_only: bool,
) -> int:
    """Check every marker against the row it deleted; return how many."""
    markers = 0
    for row in rows:
        if not row["deleted"]:
            continue
        markers += 1
        original = originals[row["k"]]
        assert row["kind"] == original["kind"], row
        for column in DROPPED:
            if keep_only:
                assert row[column] is None, (column, row)
            else:
                assert row[column] == original[column], (column, row)
    return markers


def _assert_live(rows: list[dict[str, Any]], expected: dict[str, dict[str, Any]]) -> None:
    for row in rows:
        if row["deleted"]:
            continue
        want = expected[row["k"]]
        assert {c: row[c] for c in COLUMNS} == want, row


def _timings(ctx: Any) -> set[str]:
    return set(ctx._backend._last_operation_timings_ms)


# --------------------------------------------------------------- write paths


@pytest.mark.parametrize("keep_only", [True, False], ids=["option_on", "option_off"])
def test_single_row_exact_and_inherited_paths(generic: Any, keep_only: bool) -> None:
    ctx = generic
    branch = ctx.test_branch
    if keep_only:
        interval_backend.set_tombstone_keep(TABLE, KEEP)
    else:
        _upstream_settings()
    rows = {key: _row(key) for key in ("a", "b")}
    session = ctx.checkout(branch)
    session.upsert_rows(TABLE, [rows["a"]])
    session.delete_keys(TABLE, [{"k": "a"}])
    assert "splice.single_row_exact_direct_fast_path" in _timings(ctx)

    session.upsert_rows(TABLE, [rows["b"]])
    ctx.create_checkpoint(f"{branch}_cp1", branch)
    session = ctx.checkout(branch)
    session.delete_keys(TABLE, [{"k": "b"}])
    assert "splice.single_row_inherited_direct_fast_path" in _timings(ctx)

    physical = _physical(ctx)
    assert _assert_markers(physical, rows, keep_only=keep_only) == 2
    _assert_live(physical, rows)


@pytest.mark.parametrize("keep_only", [True, False], ids=["option_on", "option_off"])
def test_batch_inherited_tail_fast_path(generic: Any, keep_only: bool) -> None:
    ctx = generic
    branch = ctx.test_branch
    if keep_only:
        interval_backend.set_tombstone_keep(TABLE, KEEP)
    else:
        _upstream_settings()
    rows = {key: _row(key) for key in ("a", "b", "c")}
    session = ctx.checkout(branch)
    session.upsert_rows(TABLE, list(rows.values()))
    ctx.create_checkpoint(f"{branch}_cp1", branch)
    # A child branch carves its segment out of the middle of the parent's
    # rows, so the deletes there leave right-hand live remainders too.
    child = f"{branch}_child"
    ctx.create_branch(child, branch)
    ctx.checkout(child).delete_keys(TABLE, [{"k": k} for k in rows])
    timings = _timings(ctx)
    assert "splice.inherited_tail_batch_fast_path" in timings, timings
    physical = _physical(ctx)
    assert _assert_markers(physical, rows, keep_only=keep_only) == 3
    _assert_live(physical, rows)
    assert sum(1 for row in physical if not row["deleted"]) >= 3


@pytest.mark.parametrize("keep_only", [True, False], ids=["option_on", "option_off"])
def test_batch_general_path_with_remainders(generic: Any, keep_only: bool) -> None:
    ctx = generic
    branch = ctx.test_branch
    if keep_only:
        interval_backend.set_tombstone_keep(TABLE, KEEP)
    else:
        _upstream_settings()
    rows = {key: _row(key) for key in ("a", "b", "c")}
    ctx.checkout(branch).upsert_rows(TABLE, [rows["a"], rows["b"]])
    ctx.create_checkpoint(f"{branch}_cp1", branch)
    child = f"{branch}_child"
    ctx.create_branch(child, branch)
    session = ctx.checkout(child)
    # "c" is written in the child's own segment, so it is not an inherited
    # tail and the batch falls back to the general path.
    session.upsert_rows(TABLE, [rows["c"]])
    session.delete_keys(TABLE, [{"k": k} for k in rows])
    timings = _timings(ctx)
    assert "splice.inherited_tail_batch_fast_path_miss" in timings, timings
    assert "splice.insert_right_segments" in timings, timings
    physical = _physical(ctx)
    assert _assert_markers(physical, rows, keep_only=keep_only) == 3
    _assert_live(physical, rows)


@pytest.mark.parametrize("keep_only", [True, False], ids=["option_on", "option_off"])
def test_mixed_batch_path(generic: Any, keep_only: bool) -> None:
    ctx = generic
    branch = ctx.test_branch
    if keep_only:
        interval_backend.set_tombstone_keep(TABLE, KEEP)
    else:
        _upstream_settings()
    rows = {key: _row(key) for key in ("a", "b", "c")}
    ctx.checkout(branch).upsert_rows(TABLE, [rows["a"], rows["b"]])
    ctx.create_checkpoint(f"{branch}_cp1", branch)
    session = ctx.checkout(branch)
    session.apply_table_edits(
        TABLE,
        upserts=[rows["c"]],
        deletes=[{"k": "a"}, {"k": "b"}],
    )
    assert "splice.mixed_jsonb_statement" in _timings(ctx), _timings(ctx)
    physical = _physical(ctx)
    assert _assert_markers(physical, rows, keep_only=keep_only) == 2
    _assert_live(physical, rows)


@pytest.mark.parametrize("keep_only", [True, False], ids=["option_on", "option_off"])
def test_sql_delete_splice_row_paths(
    generic: Any, keep_only: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SQL DELETE goes through _splice_row: its exact, inherited and general
    (remainder-splitting) branches."""
    ctx = generic
    backend = ctx._backend
    branch = ctx.test_branch
    if keep_only:
        interval_backend.set_tombstone_keep(TABLE, KEEP)
    else:
        _upstream_settings()
    seen: list[str | None] = []
    original = backend._splice_row

    def spy(*args: Any, **kwargs: Any) -> str | None:
        result = original(*args, **kwargs)
        seen.append(result)
        return result

    monkeypatch.setattr(backend, "_splice_row", spy)
    rows = {key: _row(key) for key in ("a", "b", "c")}
    session = ctx.checkout(branch)
    session.upsert_rows(TABLE, [rows["a"]])
    session.execute(f"DELETE FROM {TABLE} WHERE k = 'a'")
    assert seen[-1] == "splice.single_row_exact_fast_path"

    session.upsert_rows(TABLE, [rows["b"], rows["c"]])
    ctx.create_checkpoint(f"{branch}_cp1", branch)
    session = ctx.checkout(branch)
    session.execute(f"DELETE FROM {TABLE} WHERE k = 'b'")
    assert seen[-1] == "splice.single_row_inherited_fast_path"

    child = f"{branch}_child"
    ctx.create_branch(child, branch)
    ctx.checkout(child).execute(f"DELETE FROM {TABLE} WHERE k = 'c'")
    assert seen[-1] is None  # the general loop: left/right remainders
    physical = _physical(ctx)
    assert _assert_markers(physical, rows, keep_only=keep_only) == 3
    _assert_live(physical, rows)
    assert sum(1 for row in physical if row["k"] == "c" and not row["deleted"]) == 2


def test_redelete_and_split_old_full_copy_marker(generic: Any) -> None:
    """A marker written before the option (full copy) is split by later
    writes: a marker remainder drops the payload, a live row keeps all of it;
    a delete over a marker writes a key-plus-keep-set marker."""
    ctx = generic
    branch = ctx.test_branch
    _upstream_settings()
    rows = {key: _row(key) for key in ("a", "b")}
    ctx.checkout(branch).upsert_rows(TABLE, list(rows.values()))
    ctx.create_checkpoint(f"{branch}_cp1", branch)
    ctx.checkout(branch).delete_keys(TABLE, [{"k": "a"}, {"k": "b"}])
    full = [row for row in _physical(ctx) if row["deleted"]]
    assert all(row["blob"] is not None for row in full)  # upstream full copy

    interval_backend.set_tombstone_keep(TABLE, KEEP)
    ctx.create_checkpoint(f"{branch}_cp2", branch)
    child = f"{branch}_child"
    ctx.create_branch(child, branch)
    reinserted = _row("a", version=2)
    session = ctx.checkout(child)
    session.upsert_rows(TABLE, [reinserted])  # splits a's old marker
    session.delete_keys(TABLE, [{"k": "b"}])  # a delete over a marker
    physical = _physical(ctx)
    a_rows = [row for row in physical if row["k"] == "a"]
    child_segment = ctx._backend._current_segment(child).segment_id
    live_a = [
        row for row in a_rows
        if not row["deleted"] and row["writer_segment_id"] == child_segment
    ]
    assert [{c: row[c] for c in COLUMNS} for row in live_a] == [reinserted]
    # The old marker's remainders on both sides of the child are rewritten
    # key-plus-keep-set; only the parent's untouched marker rows are not.
    split_markers = [
        row for row in a_rows
        if row["deleted"] and row["live_hi"] != full[0]["live_hi"]
    ]
    assert split_markers, a_rows
    for row in split_markers:
        assert row["kind"] == rows["a"]["kind"]
        assert all(row[c] is None for c in DROPPED), row
    b_child = [
        row for row in physical
        if row["k"] == "b" and row["deleted"]
        and row["writer_segment_id"] == ctx._backend._current_segment(child).segment_id
    ]
    assert len(b_child) == 1
    assert b_child[0]["kind"] == rows["b"]["kind"]
    assert all(b_child[0][c] is None for c in DROPPED)


def test_delete_then_reinsert_carries_the_inserted_row(generic: Any) -> None:
    ctx = generic
    branch = ctx.test_branch
    interval_backend.set_tombstone_keep(TABLE, KEEP)
    first = _row("a", version=1)
    second = _row("a", version=2)
    ctx.checkout(branch).upsert_rows(TABLE, [first])
    ctx.create_checkpoint(f"{branch}_cp1", branch)
    ctx.checkout(branch).delete_keys(TABLE, [{"k": "a"}])
    ctx.checkout(branch).upsert_rows(TABLE, [second])
    visible = ctx.checkout(branch).query(f"SELECT * FROM {TABLE}")
    assert [dict(row) for row in visible] == [second]
    live = [row for row in _physical(ctx) if not row["deleted"]]
    assert {c: live[-1][c] for c in COLUMNS} == second


def test_delete_with_no_prior_row_is_key_only(generic: Any) -> None:
    """A delete of a key with no physical row writes an all-NULL marker,
    as upstream does; nothing assumes such a marker carries a keep column."""
    ctx = generic
    interval_backend.set_tombstone_keep(TABLE, KEEP)
    ctx.checkout(ctx.test_branch).delete_keys(TABLE, [{"k": "ghost"}])
    (marker,) = _physical(ctx)
    assert marker["deleted"] is True
    assert all(marker[c] is None for c in ("kind", *DROPPED))


def test_option_refuses_schema_branching(generic: Any) -> None:
    """Every marker write asks _tombstone_keep first; with schema branching
    on and a keep set configured it refuses instead of writing a key-only
    marker the private-schema path could revive as a row of NULLs."""
    ctx = generic
    backend = ctx._backend
    meta = backend.tables[TABLE]
    interval_backend.set_tombstone_keep(TABLE, KEEP)
    backend.enable_schema_branching = True
    try:
        with pytest.raises(BranchingError, match="okg#3284"):
            backend._tombstone_keep(meta)
        _upstream_settings()
        assert backend._tombstone_keep(meta) is None  # upstream: allowed
    finally:
        backend.enable_schema_branching = False


# ------------------------------------------------------------- OKG graph store


def _graph_physical(store: Any, logical: str) -> list[dict[str, Any]]:
    meta = store.context._backend.tables[logical]
    rows = store.context.db.execute(
        f'SELECT * FROM "{meta.physical_name}" ORDER BY 1, live_lo'
    ).fetchall()
    store.context.db.commit()
    return [dict(row) for row in rows]


def _graph_facts(prefix: str, count: int, *, pad: int = 64) -> list[Any]:
    facts: list[Any] = []
    for index in range(count):
        facts.append(
            NodeFact(
                f"{prefix}:{index}",
                "claim",
                {"text": f"{prefix} {index} " + os.urandom(pad).hex()},
                {"id": index},
                {"rev": 1},
            )
        )
    for index in range(1, count):
        facts.append(EdgeFact(f"{prefix}:0", f"{prefix}:{index}", "supported_by"))
    return facts


def _assert_graph_markers(
    store: Any, *, live_before: dict[str, dict[Any, dict[str, Any]]]
) -> tuple[int, int]:
    counts = []
    for logical, id_column in (("okg.graph_nodes", "node_id"), ("okg.graph_edges", "edge_id")):
        keep = {id_column, *TOMBSTONE_KEEP_COLUMNS[logical]}
        markers = [row for row in _graph_physical(store, logical) if row["deleted"]]
        for row in markers:
            original = live_before[logical][row[id_column]]
            for column, value in row.items():
                if column in {"live_lo", "live_hi", "writer_segment_id", "deleted"}:
                    continue
                if column in keep:
                    assert value == original[column], (logical, column, row)
                else:
                    assert value is None, (logical, column, row)
            type_column = "subtype" if logical == "okg.graph_nodes" else "edge_type"
            assert row[type_column] is not None, row
        counts.append(len(markers))
    return counts[0], counts[1]


def test_graph_store_markers_keep_reader_columns_after_fresh_process(
    clean_runtime: str, keep_settings: None
) -> None:
    """Failure case for a warm process: the graph is registered and the
    bootstrap is skipped (ensure_schema=False), so no register_table runs.
    The keep sets must still apply."""
    bootstrap = ChronosGraphStore.connect(clean_runtime)
    bootstrap.close()
    _upstream_settings()  # a process that never ran OKG's registration
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        assert interval_backend.tombstone_keep_settings()[0] == {
            table: frozenset(columns)
            for table, columns in TOMBSTONE_KEEP_COLUMNS.items()
        }
        store.apply_facts(_graph_facts("tp", 4), branch="default", source="tomb-src")
        live_before = {
            "okg.graph_nodes": {
                row["node_id"]: row
                for row in _graph_physical(store, "okg.graph_nodes")
                if not row["deleted"]
            },
            "okg.graph_edges": {
                row["edge_id"]: row
                for row in _graph_physical(store, "okg.graph_edges")
                if not row["deleted"]
            },
        }
        result = store.apply_facts(
            [NodeFact("tp:0", "claim", {}, {}, {}, op="R")],
            branch="default",
            source="tomb-src",
        )
        assert result.nodes_deleted == 1
        assert result.edges_deleted == 3
        nodes, edges = _assert_graph_markers(store, live_before=live_before)
        assert (nodes, edges) == (1, 3)
        # Re-insert after the delete carries the incoming row.
        store.apply_facts(
            [NodeFact("tp:0", "claim", {"text": "back"}, {}, {})],
            branch="default",
            source="tomb-src",
        )
        (back,) = store.query(
            "SELECT subtype, attrs FROM okg.graph_nodes WHERE node_id = 'tp:0'",
            branch="default",
        )
        assert back["subtype"] == "claim"
        assert back["attrs"]["text"] == "back"
    finally:
        store.close()


def _node_table_growth(store: Any, prefix: str, count: int) -> dict[str, Any]:
    store.apply_facts(
        [
            NodeFact(f"{prefix}:{i}", "claim", {"text": os.urandom(1024).hex()}, {"id": i}, {})
            for i in range(count)
        ],
        branch="default",
        source=f"{prefix}-src",
    )
    store.checkpoint_generation(branch="default")
    meta = store.context._backend.tables["okg.graph_nodes"]
    db = store.context.db
    before = db.execute(
        "SELECT pg_total_relation_size(?::regclass) AS size",
        (f'"{meta.physical_name}"',),
    ).fetchone()["size"]
    db.commit()
    store.apply_facts(
        [NodeFact(f"{prefix}:{i}", "claim", {}, {}, {}, op="R") for i in range(count)],
        branch="default",
        source=f"{prefix}-src",
    )
    after = db.execute(
        "SELECT pg_total_relation_size(?::regclass) AS size",
        (f'"{meta.physical_name}"',),
    ).fetchone()["size"]
    markers = db.execute(
        "SELECT count(*) AS n, avg(pg_column_size(p.*))::bigint AS width, "
        "sum(pg_column_size(p.*))::bigint AS bytes "
        f'FROM "{meta.physical_name}" AS p WHERE p.deleted AND p.node_id LIKE ?',
        (f"{prefix}:%",),
    ).fetchone()
    db.commit()
    return {
        "before": before,
        "after": after,
        "growth": after - before,
        "markers": markers["n"],
        "marker_width": markers["width"],
        "marker_bytes": markers["bytes"],
    }


def test_marker_size_option_on_against_off(clean_runtime: str, keep_settings: None) -> None:
    count = int(os.environ.get("OKG_TOMBSTONE_SIZE_ROWS", "400"))
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _upstream_settings()
        off = _node_table_growth(store, "size-off", count)
        configure_tombstone_payload()
        on = _node_table_growth(store, "size-on", count)
    finally:
        store.close()
    print(f"\nTOMBSTONE-SIZE rows={count} off={off} on={on}")
    assert off["markers"] == on["markers"] == count, (off, on)
    assert on["marker_width"] < off["marker_width"] / 4, (off, on)
    # The markers' own bytes, not the table's growth: pg_total_relation_size
    # also counts index pages and free-space reuse, and the option-on run
    # follows the option-off run on the same table, so its growth depends on
    # page layout (on grew 4.7 MB against off's 2.2 MB in one CI run). The
    # growth stays in the printed report the size requirement asks for.
    assert on["marker_bytes"] < off["marker_bytes"] / 4, (off, on)


# ------------------------------------------------------------------- backfill


def _script_statements(*, confirm: bool) -> list[str]:
    """Split the psql script on its "-- >>>" lines and drop psql meta-commands
    (\\if, \\gexec, ...). With ``confirm`` the confirm-only steps are kept."""
    text = BACKFILL_SQL.read_text(encoding="utf-8")
    blocks = text.split("\n-- >>> ")[1:]
    statements: list[str] = []
    for block in blocks:
        title, _, body = block.partition("\n")
        if title.startswith("step 2") or title.startswith("step 3") or title.startswith("step 4"):
            if not confirm:
                continue
        if title.startswith("step 3"):
            continue  # \gexec VACUUM: run by the test itself
        sql_lines = [line for line in body.splitlines() if not line.startswith("\\")]
        statement = "\n".join(sql_lines).strip()
        if statement:
            statements.append(statement)
    return statements


def _run_script(dsn: str, *, confirm: bool) -> list[list[dict[str, Any]]]:
    results: list[list[dict[str, Any]]] = []
    with psycopg.connect(dsn, autocommit=True, row_factory=psycopg.rows.dict_row) as conn:
        for statement in _script_statements(confirm=confirm):
            cur = conn.execute(statement)
            results.append(cur.fetchall() if cur.description else [])
    return results


def _report(results: list[list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    report = [rows for rows in results if rows and "markers_to_clear" in rows[0]]
    return {row["table_name"]: row for row in report[-1]}


def _digests(dsn: str) -> dict[str, str]:
    out: dict[str, str] = {}
    with psycopg.connect(dsn, row_factory=psycopg.rows.dict_row) as conn:
        tables = conn.execute(
            "SELECT table_name, physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' ORDER BY table_name"
        ).fetchall()
        for table in tables:
            physical = psycopg.sql.Identifier(table["physical_table"])
            for label, where in (("live", "NOT p.deleted"), ("all", "TRUE")):
                row = conn.execute(
                    psycopg.sql.SQL(
                        "SELECT md5(coalesce(string_agg(to_jsonb(p)::text, '|' "
                        "ORDER BY to_jsonb(p)::text), '')) AS d FROM {} AS p WHERE "
                    ).format(physical)
                    + psycopg.sql.SQL(where),
                ).fetchone()
                out[f"{table['table_name']}:{label}"] = row["d"]
    return out


def test_backfill_clears_only_marker_payloads(clean_runtime: str, keep_settings: None) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _upstream_settings()
        store.apply_facts(_graph_facts("bf", 6), branch="default", source="bf-src")
        store.checkpoint_generation(branch="default")
        store.apply_facts(
            [NodeFact("bf:1", "claim", {}, {}, {}, op="R"),
             NodeFact("bf:2", "claim", {}, {}, {}, op="R")],
            branch="default",
            source="bf-src",
        )
        store.checkpoint_generation(branch="default")
        # One more full-copy marker written into the branch's CURRENT segment:
        # the mutation token hashes it, so the backfill must leave it alone.
        store.apply_facts(
            [NodeFact("bf:3", "claim", {}, {}, {}, op="R")],
            branch="default",
            source="bf-src",
        )
        physical_tables = {
            logical: (store.context._backend.tables[logical].physical_name, id_column)
            for logical, id_column in (
                ("okg.graph_nodes", "node_id"),
                ("okg.graph_edges", "edge_id"),
            )
        }
        current_segment = store.context._backend._current_segment("default").segment_id
    finally:
        store.close()
        configure_tombstone_payload()

    def markers() -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {}
        with psycopg.connect(clean_runtime, row_factory=psycopg.rows.dict_row) as conn:
            for logical, (physical, id_column) in physical_tables.items():
                out[logical] = conn.execute(
                    psycopg.sql.SQL("SELECT * FROM {} WHERE deleted ORDER BY {}, live_lo").format(
                        psycopg.sql.Identifier(physical), psycopg.sql.Identifier(id_column)
                    )
                ).fetchall()
        return out

    before_markers = markers()
    for logical in physical_tables:
        assert any(
            row["writer_segment_id"] == current_segment for row in before_markers[logical]
        ), logical
    assert all(row["attrs"] is not None for row in before_markers["okg.graph_nodes"])
    with psycopg.connect(clean_runtime) as conn:
        token_before = Publisher.chronos_graph_branch_mutation_token(conn, branch_name="default")
    digests_before = _digests(clean_runtime)

    report_only = _report(_run_script(clean_runtime, confirm=False))
    assert _digests(clean_runtime) == digests_before  # report-only changes nothing
    planned = report_only["okg.graph_nodes"]
    assert planned["markers_to_clear"] >= 2
    assert planned["markers_skipped_current_segment"] >= 1
    assert planned["payload_bytes_to_clear"] > 0
    assert report_only["okg.graph_edges"]["markers_to_clear"] >= 2

    results = _run_script(clean_runtime, confirm=True)
    after_report = _report(results)
    for row in after_report.values():
        assert row["markers_to_clear"] == 0, row

    after_markers = markers()
    for logical, (_, id_column) in physical_tables.items():
        keep = {id_column, *TOMBSTONE_KEEP_COLUMNS[logical]}
        type_column = "subtype" if logical == "okg.graph_nodes" else "edge_type"
        old_rows, new_rows = before_markers[logical], after_markers[logical]
        assert len(new_rows) == len(old_rows), logical
        cleared = 0
        for old, new in zip(old_rows, new_rows, strict=True):
            assert (old[id_column], old["live_lo"]) == (new[id_column], new["live_lo"])
            assert new[type_column] is not None, (logical, new)
            if old["writer_segment_id"] == current_segment:
                assert new == old  # current-segment marker untouched
                continue
            cleared += 1
            for column, value in new.items():
                if column in INTERVAL_COLUMNS:
                    assert value == old[column], (logical, column, new)
                elif column in keep:
                    assert value == old[column], (logical, column, new)
                else:
                    assert value is None, (logical, column, new)
        assert cleared == report_only[logical]["markers_to_clear"], logical
        assert cleared >= 2, logical

    digests_after = _digests(clean_runtime)
    for name, digest in digests_before.items():
        if name.endswith(":live"):
            assert digests_after[name] == digest, name
    with psycopg.connect(clean_runtime) as conn:
        token_after = Publisher.chronos_graph_branch_mutation_token(conn, branch_name="default")
    assert token_after == token_before

    # A second run changes nothing.
    _run_script(clean_runtime, confirm=True)
    assert _digests(clean_runtime) == digests_after


def test_backfill_waits_for_the_graph_write_lock_and_gives_up(
    clean_runtime: str, keep_settings: None
) -> None:
    """Failure case for the lock: while a writer holds the branch write lock
    (the okg.chronos_graph advisory lock every graph write takes), a batch
    fails on its lock timeout and clears nothing; once the lock is free the
    same call clears the markers."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _upstream_settings()
        store.apply_facts(_graph_facts("lk", 4), branch="default", source="lk-src")
        store.checkpoint_generation(branch="default")
        store.apply_facts(
            [NodeFact("lk:1", "claim", {}, {}, {}, op="R")],
            branch="default",
            source="lk-src",
        )
        store.checkpoint_generation(branch="default")
    finally:
        store.close()
        configure_tombstone_payload()
    digests_before = _digests(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as writer, psycopg.connect(
        clean_runtime, autocommit=True, row_factory=psycopg.rows.dict_row
    ) as runner:
        for statement in _script_statements(confirm=False):
            runner.execute(statement)
        writer.execute(
            "SELECT pg_advisory_lock(hashtext('okg.chronos_graph'), hashtext('default'))"
        )
        with pytest.raises(psycopg.errors.LockNotAvailable):
            runner.execute("CALL pg_temp.okg_clear_tombstone_payloads(5000, '200ms')")
        assert _digests(clean_runtime) == digests_before
        writer.execute(
            "SELECT pg_advisory_unlock(hashtext('okg.chronos_graph'), hashtext('default'))"
        )
        runner.execute("CALL pg_temp.okg_clear_tombstone_payloads(5000, '200ms')")
        (report,) = [
            row
            for row in runner.execute("SELECT * FROM pg_temp.okg_tombstone_report()").fetchall()
            if row["table_name"] == "okg.graph_nodes"
        ]
    assert report["markers_to_clear"] == 0
    after = _digests(clean_runtime)
    assert after["okg.graph_nodes:all"] != digests_before["okg.graph_nodes:all"]
    assert after["okg.graph_nodes:live"] == digests_before["okg.graph_nodes:live"]


def test_backfill_keep_sets_match_the_store_keep_sets(clean_runtime: str) -> None:
    """The script's keep sets (pg_temp.okg_tombstone_keep) are a hand copy of
    TOMBSTONE_KEEP_COLUMNS. Failure case: a column added to one and not the
    other, or a table named in only one, fails here."""
    text = BACKFILL_SQL.read_text(encoding="utf-8")
    sql_tables = set(re.findall(r"WHEN '([^']+)' THEN ARRAY\[", text))
    assert sql_tables == set(TOMBSTONE_KEEP_COLUMNS)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for statement in _script_statements(confirm=False):
            conn.execute(statement)
        for table, columns in TOMBSTONE_KEEP_COLUMNS.items():
            (keep,) = conn.execute("SELECT pg_temp.okg_tombstone_keep(%s)", (table,)).fetchone()
            assert tuple(keep) == tuple(columns), table
        (other,) = conn.execute(
            "SELECT pg_temp.okg_tombstone_keep('okg.some_other_table')"
        ).fetchone()
        assert other == []  # every other table: primary key only, as the store's default


def _psql_command() -> list[str] | None:
    """The psql to run the script with: OKG_TEST_PSQL (a command line) when
    set, else psql on PATH."""
    override = os.environ.get("OKG_TEST_PSQL")
    if override:
        return shlex.split(override)
    found = shutil.which("psql")
    return [found] if found else None


@pytest.mark.parametrize(
    ("confirm", "clears"),
    [
        (None, False),
        ("0", False),
        ("off", False),
        ("not-a-boolean", False),
        ("1", True),
    ],
    ids=["unset", "zero", "off", "not-a-boolean", "one"],
)
def test_backfill_through_psql_clears_only_on_a_true_confirm(
    clean_runtime: str, keep_settings: None, confirm: str | None, clears: bool
) -> None:
    """Runs the script through the real psql, so its \\if gate is what runs.
    Failure case: -v confirm_clear=0 (or off, or a typo) must change nothing;
    only a true value clears."""
    psql = _psql_command()
    if psql is None:
        pytest.skip(
            "psql is not on PATH and OKG_TEST_PSQL is not set; this test runs the real psql"
        )
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _upstream_settings()
        store.apply_facts(_graph_facts("ps", 4), branch="default", source="ps-src")
        store.checkpoint_generation(branch="default")
        store.apply_facts(
            [NodeFact("ps:1", "claim", {}, {}, {}, op="R")],
            branch="default",
            source="ps-src",
        )
        store.checkpoint_generation(branch="default")
    finally:
        store.close()
        configure_tombstone_payload()
    digests_before = _digests(clean_runtime)

    command = [*psql, clean_runtime, "-X", "-v", "ON_ERROR_STOP=1"]
    if confirm is not None:
        command += ["-v", f"confirm_clear={confirm}"]
    command += ["-f", str(BACKFILL_SQL)]
    done = subprocess.run(command, capture_output=True, text=True, timeout=300, check=False)
    assert done.returncode == 0, (done.stdout, done.stderr)

    after = _digests(clean_runtime)
    for name, digest in digests_before.items():
        if name.endswith(":live"):
            assert after[name] == digest, name
    if clears:
        assert after["okg.graph_nodes:all"] != digests_before["okg.graph_nodes:all"]
        assert after["okg.graph_edges:all"] != digests_before["okg.graph_edges:all"]
        assert "Report only" not in done.stdout
        assert "cleared" in done.stderr  # the per-table NOTICE
        with psycopg.connect(
            clean_runtime, autocommit=True, row_factory=psycopg.rows.dict_row
        ) as conn:
            for statement in _script_statements(confirm=False):
                conn.execute(statement)
            for row in conn.execute("SELECT * FROM pg_temp.okg_tombstone_report()").fetchall():
                assert row["markers_to_clear"] == 0, row
    else:
        assert after == digests_before
        assert "Report only; nothing was changed." in done.stdout
