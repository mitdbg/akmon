"""PACT lane-h-nomos-read-class-sql (slice 2) on PostgreSQL.

- the class catalog: written by every writer through the stamping helper,
  insert-once under concurrent writers, a fresh install carries it, a database
  without it still writes and reads;
- T1-style randomized equivalence through the REAL SQL predicate plus
  ``finish`` against the per-row filter (nodes and edges, audit and enforce,
  hidden and redacted classes, overlays, NULL, other-version and uncatalogued
  rows, unusual classifications), and the mutations that need SQL;
- the view on a real published generation: every field, ``sql_exact``
  (true only with Nomos off; the property and its mutation), ``finish``
  removing no row when every row is placed, ``open_many``, shadow mode passing
  and raising on an injected difference, the fingerprint, and the decision
  trail (today's per-row reads for served rows, nothing else).
"""
from __future__ import annotations

import contextlib
import json
import random
import threading
import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.common import EDGE_TABLE, NODE_TABLE
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos import read_class as rc
from okg.substrate.nomos import read_filter, reader_view, store as nomos_store
from okg.substrate.nomos.core import compile_policy_graph
from okg.substrate.nomos.read_class import (
    READ_CLASS_CATALOG_TABLE,
    READ_CLASS_COLUMN,
    READ_CLASS_VERSION_COLUMN,
    class_facts,
    record_read_classes,
    stamp_rows,
)
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)
from okg.substrate.nomos.read_masking import MASK_TOKEN, ReadRedactedAttrs
from okg.substrate.nomos.reader_view import ReaderViewShadowMismatch
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, normalize_source_lineage
from tests.substrate._db_helpers import seed_catalog_version
from tests.substrate.test_chronos_graph_store import _reset_chronos_graph

import test_read_filter_decision_class as _decision_class
from test_read_filter_decision_class import _Conn, _Engine, _rows, _session
from test_read_filter_read_class import _canon, _redacting_bundle, _with_payload

pytestmark = pytest.mark.postgres

engine = _decision_class.engine
SUBJECT = "subject:reader"

_SUBTYPE_PII = {
    "doc": {"name": "direct_identifier", "body": "free_text", "title": "non_pii",
            "classification": "non_pii"},
    "page": {},
}


def _per_row(conn: Any, kind: str, session: Any, rows: list[dict[str, Any]]) -> list[dict]:
    exact = filter_node_rows_for_session if kind == "node" else filter_edge_rows_for_session
    return exact(conn, session, [dict(r) for r in rows])


# --------------------------------------------------------------------------- #
# Catalog                                                                      #
# --------------------------------------------------------------------------- #
def _catalog(conn: psycopg.Connection) -> dict[uuid.UUID, dict[str, Any]]:
    return {
        uuid.UUID(str(row[0])): row[1]
        for row in conn.execute(f"SELECT read_class, class_facts FROM {READ_CLASS_CATALOG_TABLE}")
    }


def _graph_classes(store: ChronosGraphStore) -> list[tuple[str, dict[str, Any]]]:
    graph = store.checkout()
    out = []
    for kind, table, id_key in (("node", NODE_TABLE, "node_id"), ("edge", EDGE_TABLE, "edge_id")):
        for row in graph.query(
            f"SELECT {id_key}, attrs, source, source_record_id, source_revision,"
            f" {READ_CLASS_COLUMN}, {READ_CLASS_VERSION_COLUMN} FROM {table}"
        ):
            out.append((kind, dict(row)))
    return out


def _assert_every_stamped_class_is_catalogued(dsn: str) -> int:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = _graph_classes(store)
    finally:
        store.close()
    with psycopg.connect(dsn) as conn:
        catalog = _catalog(conn)
    stamped = 0
    for kind, row in rows:
        value = row[READ_CLASS_COLUMN]
        if value is None:
            continue
        stamped += 1
        assert value in catalog, (kind, row)
        assert catalog[value] == class_facts(row, kind)
        assert rc.class_id_of_facts(catalog[value]) == value
    return stamped


def _nodes(source: str, n: int, *, prefix: str | None = None, subtype: str = "doc") -> list[NodeFact]:
    prefix = prefix or source
    return [
        NodeFact(f"{prefix}:{i}", subtype,
                 {"classification": ("internal", "restricted", "internal")[i % 3],
                  "name": f"Person {i}", "body": f"body {i}", "title": f"T{i}",
                  "text": f"text {i}"},
                 {"key": "ab"[i % 2]}, {"v": 1})
        for i in range(n)
    ]


def test_a_fresh_install_carries_the_catalog(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime) as conn:
        cols = {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns"
                " WHERE table_schema = 'okg' AND table_name = 'nomos_read_classes'"
            )
        }
        assert cols == {
            "read_class": ("uuid", "NO"), "class_facts": ("jsonb", "NO"),
            "vocabulary_version": ("integer", "NO"),
            "first_written_at": ("timestamp with time zone", "NO"),
        }
        assert conn.execute(
            "SELECT 1 FROM okg.schema_migrations WHERE migration_id = '0040_nomos_read_classes'"
        ).fetchone() is not None
        # The MCP read pool may read the catalog (and only read it).
        if conn.execute("SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp'").fetchone():
            assert conn.execute(
                "SELECT has_table_privilege('okg_mcp', 'okg.nomos_read_classes', 'SELECT'),"
                " has_table_privilege('okg_mcp', 'okg.nomos_read_classes', 'INSERT')"
            ).fetchone() == (True, False)
        # Re-running the migration is a no-op.
        from importlib import resources

        text = resources.files("okg.substrate.db").joinpath(
            "migrations/0040_nomos_read_classes.sql").read_text()
        with conn.transaction():
            conn.execute(text)


def test_fact_apply_writes_a_catalog_row_for_every_stamped_class(clean_runtime: str) -> None:
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([*_nodes("git", 6), EdgeFact("git:1", "git:0", "member_of",
                           attrs={}, source_record_id={"key": "a"}, source_revision={"v": 1})],
                          source="git")
        store.apply_facts(_nodes("jira", 3), source="jira")
        # Publisher-derived rows take their lineage from their endpoints.
        store.apply_facts(
            [EdgeFact("git:1", "jira:2", "relates_to", attrs={}, source_record_id={},
                      source_revision={})],
            source="_publisher_derived",
        )
    finally:
        store.close()
    assert _assert_every_stamped_class_is_catalogued(clean_runtime) == 11
    with psycopg.connect(clean_runtime) as conn:
        # git nodes, jira nodes, the git edge, the derived git+jira edge.
        assert len(_catalog(conn)) == 4


def test_the_embedding_write_and_the_lineage_repair_write_catalog_rows(
    clean_runtime: str,
) -> None:
    from okg.substrate.embeddings import EmbeddingConfig, build_embedding_execution_plan
    from okg.substrate.nomos.source_lineage_repair import (
        _update_single_attrs,
        _upsert_logical_rows_with_retry,
    )
    from okg.substrate.runtime.embed_workflow import _embed_and_write_one

    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(_nodes("git", 3), source="git")
        text_hash = store.checkout().query(
            "SELECT text_hash FROM okg.graph_nodes WHERE node_id = 'git:0'"
        )[0]["text_hash"]
        assert text_hash is not None
    finally:
        store.close()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f"DELETE FROM {READ_CLASS_CATALOG_TABLE}")
    # The embedding write: the catalog row comes back with the new version.
    _embed_and_write_one(
        dsn=clean_runtime, node_id="git:0", text="text 0", text_hash=text_hash,
        embedding_plan=build_embedding_execution_plan(EmbeddingConfig(enabled=True, backend="hash")),
    )
    with psycopg.connect(clean_runtime) as conn:
        assert len(_catalog(conn)) == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = [dict(r) for r in store.checkout().query(
            "SELECT node_id, subtype, attrs, text_hash, embedding_model, source,"
            " source_record_id, source_revision, run_id, observed_at"
            " FROM okg.graph_nodes ORDER BY node_id"
        )]
        for target, source in ((1, "repaired-a"), (2, "repaired-b")):
            lineage = normalize_source_lineage(
                source_id=source, source_record_id={"key": "z"}, source_revision={"v": 9},
            )
            row = {**rows[target], "attrs": {**rows[target]["attrs"], LINEAGE_ATTR: lineage}}
            if target == 1:
                _upsert_logical_rows_with_retry(
                    store, connection=conn, branch="default", table=NODE_TABLE,
                    key_column="node_id", rows=[row],
                )
            else:
                _update_single_attrs(
                    store, branch="default", table=NODE_TABLE, key_column="node_id", row=row,
                )
    assert _assert_every_stamped_class_is_catalogued(clean_runtime) == 3
    with psycopg.connect(clean_runtime) as conn:
        sources = {tuple(f["source_ids"]) for f in _catalog(conn).values()}
    assert sources == {("git",), ("repaired-a",), ("repaired-b",)}


def test_catalog_rows_are_insert_once_under_concurrent_writers(clean_runtime: str) -> None:
    rows = [{"node_id": f"n:{i}", "attrs": {}, "source": f"src-{i % 5}",
             "source_record_id": {"key": "a"}, "source_revision": {"v": 1}} for i in range(20)]
    classes = stamp_rows([dict(r) for r in rows], "node")
    assert len(classes) == 5
    ids = sorted(classes, key=str)
    first_half = {c: classes[c] for c in ids[:4]}
    second_half = {c: classes[c] for c in ids[1:]}
    errors: list[BaseException] = []
    inserted: list[int] = []
    started = threading.Barrier(2)

    def writer(part: dict, hold: float) -> None:
        try:
            with psycopg.connect(clean_runtime) as conn:
                started.wait(timeout=30)
                inserted.append(record_read_classes(conn, part))
                # Hold the transaction open: the other writer's overlapping
                # inserts wait on these keys, then succeed with DO NOTHING.
                threading.Event().wait(hold)
                conn.commit()
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(first_half, 0.5)),
               threading.Thread(target=writer, args=(second_half, 0.0))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not errors, errors
    with psycopg.connect(clean_runtime) as conn:
        catalog = _catalog(conn)
        stamps = dict(conn.execute(
            f"SELECT read_class, first_written_at FROM {READ_CLASS_CATALOG_TABLE}"
        ).fetchall())
        assert set(catalog) == set(classes)
        assert all(catalog[c] == classes[c] for c in classes)
        # A later writer of the same classes changes nothing.
        assert record_read_classes(conn, classes) == 0
        conn.commit()
        assert dict(conn.execute(
            f"SELECT read_class, first_written_at FROM {READ_CLASS_CATALOG_TABLE}"
        ).fetchall()) == stamps
    # Each class was inserted by exactly one of the two writers.
    assert sum(inserted) == 5 and len(inserted) == 2


def test_a_catalog_row_whose_id_is_not_its_digest_is_refused(clean_runtime: str) -> None:
    facts = class_facts({"node_id": "n", "attrs": {}, "source": "git",
                         "source_record_id": {}, "source_revision": {}}, "node")
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(ValueError, match="not the digest"):
            record_read_classes(conn, {uuid.UUID(int=1): facts})
        conn.rollback()
        conn.execute(
            f"INSERT INTO {READ_CLASS_CATALOG_TABLE} (read_class, class_facts, vocabulary_version)"
            " VALUES (%s, %s::jsonb, %s)",
            (uuid.UUID(int=2), json.dumps(facts), rc.CLASS_VOCABULARY_VERSION),
        )
        conn.commit()
        with pytest.raises(Exception, match="does not match its facts"):
            reader_view._load_catalog(conn)


def test_a_database_without_the_catalog_writes_and_reads_per_row(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_chronos_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f"DROP TABLE {READ_CLASS_CATALOG_TABLE}")
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(_nodes("git", 3), source="git")
        assert {r[READ_CLASS_COLUMN] is not None for _k, r in _graph_classes(store)} == {True}
    finally:
        store.close()
    deployment = _activate(clean_runtime, _BASE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime) as conn:
        view = reader_view.open(conn, _real_session(deployment, 0), row_kind="node",
                                action=None, surface=None)
    assert (view.mode, view.reason) == ("per_row", "catalog_absent")


# --------------------------------------------------------------------------- #
# T1 through the real SQL predicate (in-memory policy engine)                  #
# --------------------------------------------------------------------------- #
#: Classifications the SQL label must handle: falsy JSON (internal), blank and
#: unusual strings, non-string values, unicode, and plain named/unnamed ones.
_ODD_CLASSIFICATIONS = (
    True, False, 5, -2.5, 0, 0.0, "", [], {}, [1], {"a": 1}, None,
    "  ", "\u2003", "\t", "a b", "p:q", "caf\u00e9", "x%y", "'quoted'",
    "public", "restricted", "internal", "Restricted",
)


@contextlib.contextmanager
def _offline_view(monkeypatch: pytest.MonkeyPatch, catalog: dict, *, overlay: bool) -> Iterator[None]:
    monkeypatch.setattr(read_filter, "_load_subtype_pii_classes", lambda conn, cv: _SUBTYPE_PII)
    monkeypatch.setattr(reader_view, "_catalog_present", lambda conn: True)
    monkeypatch.setattr(reader_view, "_columns_present", lambda conn, kind: True)
    monkeypatch.setattr(reader_view, "_load_catalog", lambda conn: catalog)
    monkeypatch.setattr(reader_view, "_overlay_present", lambda conn, session, kind: overlay)
    yield


def _stored_rows(rng: random.Random, kind: str, rows: list[dict]) -> tuple[list[dict], dict, list]:
    """Rows as the database holds them (stamp states vary) and the catalog;
    the third value lists each row's stamp state."""
    stored, states = [], []
    catalog: dict[uuid.UUID, dict[str, Any]] = {}
    for row in rows:
        row = dict(row)
        if rng.random() < 0.35:
            attrs = dict(row["attrs"])
            attrs["classification"] = rng.choice(_ODD_CLASSIFICATIONS)
            row["attrs"] = attrs
        state = rng.random()
        if state < 0.65:
            catalog.update(stamp_rows([row], kind))
            states.append("stamped")
        elif state < 0.75:
            stamp_rows([row], kind)  # stamped, never catalogued
            states.append("uncatalogued")
        elif state < 0.9:
            row[READ_CLASS_COLUMN] = None
            row[READ_CLASS_VERSION_COLUMN] = None
            states.append("null")
        else:
            stamp_rows([row], kind)
            row[READ_CLASS_VERSION_COLUMN] = rc.CLASS_VOCABULARY_VERSION - 1
            states.append("old_version")
        stored.append(row)
    return stored, catalog, states


def _sql_positions(pg: psycopg.Connection, view: Any, kind: str, rows: list[dict]) -> list[int]:
    """Positions of the rows the view's SQL predicate keeps, evaluated by
    PostgreSQL over a table holding the stored rows."""
    pg.execute("DROP TABLE IF EXISTS rv_rows")
    pg.execute(
        "CREATE TEMP TABLE rv_rows (pos int, subtype text, attrs jsonb,"
        " nomos_read_class uuid, nomos_read_class_version smallint)"
    )
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO rv_rows VALUES (%s, %s, %s::jsonb, %s, %s)",
            [
                (
                    i, row.get("subtype"),
                    json.dumps({"classification": row["attrs"]["classification"]}
                               if "classification" in row["attrs"] else {}),
                    row.get(READ_CLASS_COLUMN), row.get(READ_CLASS_VERSION_COLUMN),
                )
                for i, row in enumerate(rows)
            ],
        )
    text = f"SELECT pos FROM rv_rows r WHERE {view.predicate('r').as_string(pg)} ORDER BY pos"
    return [row[0] for row in pg.execute(text)]


def _sql_readable(pg: psycopg.Connection, view: Any, keys: list[str]) -> dict[int, dict[str, Any]]:
    """``readable(r, key)`` for every row of ``rv_rows`` and every key."""
    if not keys:
        return {}
    cols = ", ".join(f"({view.readable('r', key).as_string(pg)})" for key in keys)
    return {
        row[0]: dict(zip(keys, row[1:]))
        for row in pg.execute(f"SELECT pos, {cols} FROM rv_rows r")
    }


def _masked_keys(served: dict[str, Any], given: dict[str, Any]) -> set[str]:
    """Keys of ``given`` attrs the per-row filter withheld (dropped or tokened)."""
    out = served["attrs"]
    if not isinstance(out, ReadRedactedAttrs):
        return set()
    return {
        key for key, value in given.items()
        if key not in out or (out[key] == MASK_TOKEN and value != MASK_TOKEN)
    }


def _overlaid(rng: random.Random, rows: list[dict]) -> list[dict]:
    """The rows a reader is served from: an accepted overlay changed some
    rows' classification after they were stored."""
    out = []
    for row in rows:
        row = dict(row)
        if rng.random() < 0.3:
            row["attrs"] = {**row["attrs"], "classification": rng.choice(
                ("internal", "restricted", "public", *_ODD_CLASSIFICATIONS))}
        out.append(row)
    return out


def _sql_sweep(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch, *,
    kind: str, mode: str, seeds: int, overlay: bool, view_sees_overlay: bool | None = None,
    exact_only: bool = False,
) -> dict[str, int]:
    engine.mode = mode
    session = _session()
    stats = {"class_mode": 0, "mismatch": 0, "dropped_rows": 0, "rows": 0, "kept": 0,
             "hidden": 0, "redacted": 0, "removed_when_exact": 0, "readable_checked": 0,
             "readable_false": 0, "readable_unknown": 0, "readable_mismatch": 0,
             "exact_claimed": 0, "exact_violation": 0, "exact_vs_per_row_mismatch": 0}
    for seed in range(seeds):
        rng = random.Random(f"sql:{kind}:{mode}:{overlay}:{exact_only}:{seed}")
        rows = [_with_payload(rng, r) for r in _rows(rng, kind, rng.randint(1, 30))]
        engine.bundle = _redacting_bundle(rng, kind, rows)
        if exact_only:
            stored = []
            catalog: dict[uuid.UUID, dict[str, Any]] = {}
            for row in rows:
                row = dict(row)
                attrs = dict(row["attrs"])
                if str(attrs.get("classification") or "internal").strip() == "":
                    attrs["classification"] = "internal"
                row["attrs"] = attrs
                found = stamp_rows([row], kind)
                if not found:
                    continue  # no lineage: no class, never in an exact scope
                catalog.update(found)
                stored.append(row)
        else:
            stored, catalog, _states = _stored_rows(rng, kind, rows)
        served_from = _overlaid(rng, stored) if overlay else stored
        sees = overlay if view_sees_overlay is None else view_sees_overlay
        with _offline_view(monkeypatch, {"node": {}, "edge": {}, kind: catalog}, overlay=sees):
            view = reader_view.open(_Conn(), session, row_kind=kind, action=None, surface=None)
        if view.sql_exact:
            # The sql_exact contract on these rows (NULL, other-version and
            # uncatalogued classes, odd classifications, overlays included):
            # the predicate keeps them and finish returns them untouched.
            stats["exact_claimed"] += 1
            kept_all = [served_from[p] for p in _sql_positions(pg, view, kind, stored)]
            finished = _canon(view.finish(kept_all))
            if finished != _canon(kept_all):
                stats["exact_violation"] += 1
            # And it is what the per-row filter returns for the same rows.
            if finished != _canon(_per_row(_Conn(), kind, session, kept_all)):
                stats["exact_vs_per_row_mismatch"] += 1
        if view.mode != "class":
            continue
        stats["class_mode"] += 1
        expected = _per_row(_Conn(), kind, session, served_from)
        kept = [served_from[p] for p in _sql_positions(pg, view, kind, stored)]
        got = view.finish(kept)
        # readable() against the per-row masks, every attribute key of every
        # row the per-row filter serves. TRUE where it masked, or FALSE where
        # it did not, fails the sweep; NULL (unknown) is allowed.
        keys = sorted({str(k) for r in served_from for k in r["attrs"]})
        readable = _sql_readable(pg, view, keys)
        for position, row in enumerate(served_from):
            out = _per_row(_Conn(), kind, session, [row])
            if not out:
                continue
            masked = _masked_keys(out[0], row["attrs"])
            for key in row["attrs"]:
                value = readable[position][str(key)]
                if value is None:
                    stats["readable_unknown"] += 1
                    continue
                stats["readable_checked"] += 1
                stats["readable_false"] += value is False
                if value is (key in masked):
                    stats["readable_mismatch"] += 1
        stats["rows"] += len(stored)
        stats["kept"] += len(kept)
        stats["hidden"] += len(stored) - len(expected)
        stats["redacted"] += sum(isinstance(r["attrs"], ReadRedactedAttrs) for r in expected)
        if _canon(got) != _canon(expected):
            stats["mismatch"] += 1
            stats["dropped_rows"] += len(expected) - len(got)
        if exact_only and len(got) != len(kept):
            stats["removed_when_exact"] += 1
    return stats


@pytest.fixture
def pg(clean_runtime: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        yield conn


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("overlay", [False, True])
def test_t1_predicate_plus_finish_equals_the_per_row_filter(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
    kind: str, mode: str, overlay: bool,
) -> None:
    stats = _sql_sweep(pg, engine, monkeypatch, kind=kind, mode=mode, seeds=120,
                       overlay=overlay)
    assert stats["mismatch"] == 0, stats
    assert stats["readable_mismatch"] == 0, stats
    assert stats["readable_unknown"] > 0, stats
    if overlay:
        # Class-only: a key is decided only for a class every probed
        # classification serves (a masked key: serves unredacted), so most
        # answers are unknown (read-model slice 7a, review round 2: fail
        # closed). Observed on these seeds: enforce decides 142 node / 87 edge
        # answers, every one FALSE (an always-redacted class: the blank probe
        # hides every class under enforce, so none is served under every
        # classification); audit decides 1570 / 1838, none FALSE (audit hides
        # no class, and no class here redacts under every classification).
        if mode == "enforce":
            assert stats["readable_checked"] > 70, stats
            assert stats["readable_false"] > 0, stats
        else:
            assert stats["readable_checked"] > 1200, stats
    else:
        assert stats["readable_checked"] > 500 and stats["readable_false"] > 20, stats
    assert stats["class_mode"] > 40, stats
    assert stats["redacted"] > 20, stats
    if mode == "enforce":
        # The predicate dropped rows of hidden classes; finish removed others.
        assert stats["kept"] < stats["rows"], stats
        assert stats["hidden"] > 50, stats


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
def test_t1_with_every_row_placed_finish_removes_no_row(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
    kind: str, mode: str,
) -> None:
    """When every row is placed (stamped, catalogued, current, with lineage, a
    classification the SQL maps exactly, no overlay) the predicate alone is
    per-row visibility, so finish only masks. sql_exact does not claim this:
    it cannot know, without reading the rows, that every row is placed."""
    stats = _sql_sweep(pg, engine, monkeypatch, kind=kind, mode=mode, seeds=120,
                       overlay=False, exact_only=True)
    assert stats["mismatch"] == 0, stats
    assert stats["readable_mismatch"] == 0, stats
    assert stats["removed_when_exact"] == 0, stats
    assert stats["class_mode"] > 40 and stats["redacted"] > 20, stats


def test_mutation_a_predicate_without_the_unplaced_row_branch_is_caught(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from psycopg import sql

    full = reader_view.ReaderView.predicate

    def no_branch(self: Any, alias: Any) -> Any:
        text = full(self, alias).as_string(None)
        # Drop "NOT <known> OR": NULL, other-version and uncatalogued rows
        # are then decided only by the visible-class term.
        return sql.SQL(text.replace("COALESCE(NOT COALESCE(", "COALESCE(FALSE AND COALESCE(", 1))

    stats_before = _sql_sweep(pg, engine, monkeypatch, kind="node", mode="enforce", seeds=60,
                              overlay=False)
    assert stats_before["mismatch"] == 0
    monkeypatch.setattr(reader_view.ReaderView, "predicate", no_branch)
    stats = _sql_sweep(pg, engine, monkeypatch, kind="node", mode="enforce", seeds=60,
                       overlay=False)
    assert stats["mismatch"] > 0 and stats["dropped_rows"] > 0, stats


def test_mutation_trusting_stored_classification_under_an_overlay_is_caught(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The overlay check is what makes sql_exact false and the predicate key
    on the class alone; a view that ignores the overlay drops rows an overlay
    made visible."""
    stats = _sql_sweep(pg, engine, monkeypatch, kind="node", mode="enforce", seeds=120,
                       overlay=True, view_sees_overlay=False)
    assert stats["mismatch"] > 0 and stats["dropped_rows"] > 0, stats


def test_mutation_finish_skipping_masks_is_caught(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = reader_view.ReaderView._per_row

    def unmasked(self: Any, rows: list[dict]) -> list[dict]:
        by_id = {str(r.get(f"{self.row_kind}_id")): r for r in rows}
        out = []
        for row in real(self, rows):
            if isinstance(row["attrs"], ReadRedactedAttrs):
                row = {**row, "attrs": dict(by_id[str(row.get(f"{self.row_kind}_id"))]["attrs"])}
            out.append(row)
        return out

    monkeypatch.setattr(reader_view.ReaderView, "_per_row", unmasked)
    stats = _sql_sweep(pg, engine, monkeypatch, kind="node", mode="enforce", seeds=60,
                       overlay=False)
    assert stats["mismatch"] > 0, stats


# --------------------------------------------------------------------------- #
# The view on a real published generation                                      #
# --------------------------------------------------------------------------- #
_BASE_RULES = [
    {"rule_id": "deny_jira", "effect": "deny", "match": {"source_id": "jira"}},
    {"rule_id": "deny_restricted", "effect": "deny", "match": {"classification": "restricted"}},
    {"rule_id": "redact_docs", "effect": "redact", "match": {"source_ids": "docs"},
     "obligations": [{"type": "redaction"}]},
]


def _bundle(rules: list[dict[str, Any]], version: str = "v1") -> Any:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:reader-view-db", "policy_version": version,
        "default_effect": "allow",
        "rules": [{"actions": ["*"], "subjects": ["*"], "priority": 10 - i, **rule}
                  for i, rule in enumerate(rules)],
    })


def _activate(dsn: str, rules: list[dict[str, Any]], *, deployment: str | None = None,
              version: str = "v1") -> str:
    from okg.substrate.nomos import activate_policy_bundle

    deployment = deployment or f"reader-view-{uuid.uuid4().hex[:10]}"
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name=deployment, bundle=_bundle(rules, version),
            scope=f"deployment:{deployment}", activated_by="test:operator", bootstrap=True,
        )
        conn.commit()
    return deployment


def _real_session(deployment: str, generation: Any) -> SimpleNamespace:
    return SimpleNamespace(
        deployment_name=deployment, generation_id=generation, branch_name="default",
        catalog_version_id=None, nomos_subject=SUBJECT, nomos_read_action=None,
        nomos_read_surface=None, nomos_multi_user=False, nomos_identity_claim=None,
        nomos_access_context=None,
    )


def _edges() -> list[EdgeFact]:
    return [EdgeFact(f"{s}:{i}", f"{s}:0", "member_of", attrs={"chunk_ordinal": i},
                     source_record_id={"key": "ab"[i % 2]}, source_revision={"v": 1})
            for s in ("git", "docs") for i in range(1, 4)]


def _publish_generation_1(dsn: str) -> None:
    _reset_chronos_graph(dsn)
    store = ChronosGraphStore.connect(dsn)
    try:
        for source, n in (("git", 9), ("jira", 4), ("docs", 6)):
            facts: list[Any] = _nodes(source, n)
            facts += [e for e in _edges() if e.src.startswith(source)]
            store.apply_facts(facts, source=source)
        store.checkpoint_generation(1)
    finally:
        store.close()


_CLASS_COLUMNS = "nomos_read_class, nomos_read_class_version"
_SELECT = {
    "node": (
        "SELECT node_id, subtype, attrs, source, source_record_id, source_revision, "
        + _CLASS_COLUMNS + " FROM okg.graph_nodes r"
    ),
    "edge": (
        "SELECT edge_id, src, dst, edge_type, attrs, source, source_record_id, source_revision, "
        + _CLASS_COLUMNS + " FROM okg.graph_edges r"
    ),
}
_ORDER = {"node": " ORDER BY node_id", "edge": " ORDER BY edge_id"}


def _read(conn: psycopg.Connection, generation: Any, kind: str, where: str = "TRUE") -> list[dict]:
    return [dict(r) for r in ChronosGraphStore.from_connection(conn, ensure_schema=False)
            .query_generation_once(generation, f"{_SELECT[kind]} WHERE {where}{_ORDER[kind]}", {})]


def _through_view(conn: psycopg.Connection, view: Any, generation: Any, kind: str) -> tuple[list, list]:
    kept = _read(conn, generation, kind, view.predicate("r").as_string(conn))
    return kept, view.finish(kept)


@pytest.fixture
def published(clean_runtime: str, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    monkeypatch.setattr(read_filter, "_load_subtype_pii_classes", lambda conn, cv: _SUBTYPE_PII)
    reader_view.clear_reader_view_cache()
    _publish_generation_1(clean_runtime)
    return {"dsn": clean_runtime, "deployment": _activate(clean_runtime, _BASE_RULES)}


@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("kind", ["node", "edge"])
def test_on_a_published_generation_the_view_serves_the_per_row_answer(
    published: dict, monkeypatch: pytest.MonkeyPatch, mode: str, kind: str,
) -> None:
    monkeypatch.setattr(nomos_store, "deployment_nomos_enforcement_mode", lambda name: mode)
    session = _real_session(published["deployment"], 1)
    with psycopg.connect(published["dsn"]) as conn:
        view = reader_view.open(conn, session, row_kind=kind, action=None, surface=None)
        assert (view.mode, view.reason) == ("class", "read_class_catalog")
        assert view.sql_exact is False  # deny and redact rules exist
        all_rows = _read(conn, 1, kind)
        expected = _per_row(conn, kind, session, all_rows)
        kept, got = _through_view(conn, view, 1, kind)
        assert _canon(got) == _canon(expected)
        # Every row is placed: the predicate alone is per-row visibility and
        # finish only masks.
        assert len(got) == len(kept)
        if mode == "enforce" and kind == "node":
            # jira and restricted rows never leave the database.
            assert len(kept) < len(all_rows)
        assert any(isinstance(r["attrs"], ReadRedactedAttrs) for r in got) is (True)
        # Shadow mode agrees on the same rows (and checks readable()).
        shadow = reader_view.open(conn, session, row_kind=kind, action=None, surface=None,
                                  force="shadow")
        assert _canon(shadow.finish(all_rows)) == _canon(expected)
        # force=per_row: the exact path, predicate TRUE.
        forced = reader_view.open(conn, session, row_kind=kind, action=None, surface=None,
                                  force="per_row")
        assert (forced.mode, forced.reason) == ("per_row", "forced_per_row")
        assert _canon(forced.finish(all_rows)) == _canon(expected)


def test_readable_follows_the_class_masks_per_subtype(published: dict) -> None:
    session = _real_session(published["deployment"], 1)
    with psycopg.connect(published["dsn"]) as conn:
        view = reader_view.open(conn, session, row_kind="node", action=None, surface=None)
        keys = ("body", "name", "title", "classification", "text")
        cols = ", ".join(
            f"({view.readable('r', key).as_string(conn)}) AS k_{key}" for key in keys
        )
        rows = {
            r["node_id"]: r
            for r in ChronosGraphStore.from_connection(conn, ensure_schema=False)
            .query_generation_once(1, f"SELECT node_id, {cols} FROM okg.graph_nodes r", {})
        }
    # docs rows are redacted: doc's body is free_text (dropped), name a direct
    # identifier (masked, not pseudonymized on reads), title non_pii.
    docs = rows["docs:0"]
    assert (docs["k_body"], docs["k_name"], docs["k_title"], docs["k_classification"]) == (
        False, False, True, True)
    # text is unlisted for the declared doc subtype: fail-safe masked.
    assert docs["k_text"] is False
    # git rows are not redacted: everything readable.
    assert all(rows["git:0"][f"k_{k}"] is True for k in keys)
    assert view.pseudonymized("doc") == frozenset()
    assert view.masked_attrs("doc").action("name") == "mask"


def _write_rows(dsn: str, rows: list[dict[str, Any]]) -> None:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        session = store.checkout()
        with session.transaction():
            session.upsert_rows(NODE_TABLE, rows)
    finally:
        store.close()


def _checkpoint(dsn: str, generation: int) -> None:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        store.checkpoint_generation(generation)
    finally:
        store.close()


def _raw_node(node_id: str, source: str, classification: str = "internal") -> dict[str, Any]:
    return {"node_id": node_id, "subtype": "doc",
            "attrs": {"classification": classification, "name": "x", "body": "y"},
            "text_hash": None, "embedding_model": None, "source": source,
            "source_record_id": {"key": "a"}, "source_revision": {"v": 1},
            "run_id": None, "observed_at": None}


def _assert_exact_means_raw_rows(conn: psycopg.Connection, view: Any, generation: Any) -> None:
    """The sql_exact contract: when true, finish removes no row the predicate
    kept and masks nothing; its output is the raw rows."""
    if not view.sql_exact:
        return
    kept = _read(conn, generation, "node", view.predicate("r").as_string(conn))
    assert _canon(view.finish(kept)) == _canon(kept), (
        "sql_exact was true but finish removed or masked a row"
    )


def _claim_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutation: sql_exact true whatever the policy says."""
    monkeypatch.setattr(reader_view.ReaderView, "sql_exact", property(lambda self: True))


def test_a_null_class_row_is_kept_by_the_predicate_and_removed_by_finish(
    published: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = published["dsn"]
    # A writer that does not stamp: a hidden (jira) row with a NULL class.
    _write_rows(dsn, [_raw_node("jira:unstamped", "jira")])
    _checkpoint(dsn, 2)
    session = _real_session(published["deployment"], 2)
    with psycopg.connect(dsn) as conn:
        view = reader_view.open(conn, session, row_kind="node", action=None, surface=None)
        assert view.exactness() == {"sql_exact": False, "reason": "nomos_on"}
        assert reader_view.exactness_cause(view) == "rule_can_hide_or_mask"
        kept, got = _through_view(conn, view, 2, "node")
        assert "jira:unstamped" in {r["node_id"] for r in kept}
        assert "jira:unstamped" not in {r["node_id"] for r in got}
        assert _canon(got) == _canon(_per_row(conn, "node", session, _read(conn, 2, "node")))
        _assert_exact_means_raw_rows(conn, view, 2)

        # Mutation: sql_exact true while deny and redact rules exist.
        _claim_exact(monkeypatch)
        lying = reader_view.open(conn, session, row_kind="node", action=None, surface=None)
        assert lying.sql_exact is True
        with pytest.raises(AssertionError, match="removed or masked"):
            _assert_exact_means_raw_rows(conn, lying, 2)


def test_an_old_version_row_is_kept_by_the_predicate_and_decided_by_finish(
    published: dict,
) -> None:
    dsn = published["dsn"]
    row = _raw_node("git:old", "git")
    stamp_rows([row], "node")
    row[READ_CLASS_VERSION_COLUMN] = rc.CLASS_VOCABULARY_VERSION - 1
    _write_rows(dsn, [row])
    _checkpoint(dsn, 2)
    session = _real_session(published["deployment"], 2)
    with psycopg.connect(dsn) as conn:
        view = reader_view.open(conn, session, row_kind="node", action=None, surface=None)
        assert view.sql_exact is False
        kept, got = _through_view(conn, view, 2, "node")
        assert "git:old" in {r["node_id"] for r in got}
        assert _canon(got) == _canon(_per_row(conn, "node", session, _read(conn, 2, "node")))


def test_a_classification_overlay_switches_the_predicate_to_the_class_alone(
    published: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = published["dsn"]
    # git:1 is stored "restricted" (hidden); an accepted overlay at
    # generation 1 makes it "internal" (visible).
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id,"
            " generation_id, attr_key, observation_id, value, value_hash, producer)"
            " VALUES ('node', 'git:1', '1', 'classification', 'obs:1', '\"internal\"'::jsonb,"
            " 'h', 'test')"
        )
        conn.commit()
    session = _real_session(published["deployment"], 1)
    with psycopg.connect(dsn) as conn:
        view = reader_view.open(conn, session, row_kind="node", action=None, surface=None)
        assert view.sql_exact is False and view._state.overlay is True
        expected = _per_row(conn, "node", session, _read(conn, 1, "node"))
        assert "git:1" in {r["node_id"] for r in expected}
        kept, got = _through_view(conn, view, 1, "node")
        assert _canon(got) == _canon(expected)
        # Mutation: the predicate ignoring the overlay drops the row.
        monkeypatch.setattr(reader_view, "_overlay_present", lambda conn, session, kind: False)
        blind = reader_view.open(conn, session, row_kind="node", action=None, surface=None)
        _kept, blind_got = _through_view(conn, blind, 1, "node")
        assert "git:1" not in {r["node_id"] for r in blind_got}


def test_open_many_one_pass_one_fingerprint_and_fingerprint_changes(
    published: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = published["dsn"]
    loads: list[Any] = []
    real = read_filter._load_read_bundle

    def counting(conn: Any, session: Any, *, scope: str) -> Any:
        loads.append(scope)
        return real(conn, session, scope=scope)

    monkeypatch.setattr(read_filter, "_load_read_bundle", counting)
    session = _real_session(published["deployment"], 1)
    with psycopg.connect(dsn) as conn:
        views = reader_view.open_many(conn, session, action=None, surface=None)
        assert len(loads) == 1
        assert views["node"].mode == views["edge"].mode == "class"
        assert views["node"].fingerprint == views["edge"].fingerprint == views.fingerprint
        again = reader_view.open_many(conn, session, action=None, surface=None)
        assert again.fingerprint == views.fingerprint
        _checkpoint(dsn, 2)
        assert reader_view.open_many(
            conn, _real_session(published["deployment"], 2), action=None, surface=None,
        ).fingerprint != views.fingerprint
    # (A bundle change is covered in memory: test_reader_view.py.)
    with psycopg.connect(dsn) as conn:
        monkeypatch.setattr(rc, "CLASS_VOCABULARY_VERSION", rc.CLASS_VOCABULARY_VERSION + 1)
        bumped = reader_view.open_many(conn, session, action=None, surface=None)
        # No catalogued class is of the new version: nothing decided, a new
        # fingerprint.
        assert bumped.fingerprint != views.fingerprint
        assert bumped["node"].decided_classes == frozenset()


@pytest.mark.parametrize("injection", ["hide_a_visible_class", "flip_a_redaction"])
def test_shadow_raises_on_an_injected_difference(
    published: dict, monkeypatch: pytest.MonkeyPatch, injection: str,
) -> None:
    real = reader_view.decide_classes

    def injected(conn: Any, session: Any, catalog: Any, **kw: Any) -> Any:
        out = real(conn, session, catalog, **kw)
        internal = reader_view._md5("internal")
        for class_id, facts in catalog.items():
            source = facts["source_ids"][0]
            outcome = out[class_id][internal]
            if injection == "hide_a_visible_class" and source == "git":
                out[class_id][internal] = reader_view.ClassOutcome(False, False)
            if injection == "flip_a_redaction" and source == "docs":
                out[class_id][internal] = reader_view.ClassOutcome(outcome.visible, False)
        return out

    monkeypatch.setattr(reader_view, "decide_classes", injected)
    session = _real_session(published["deployment"], 1)
    with psycopg.connect(published["dsn"]) as conn:
        rows = _read(conn, 1, "node")
        view = reader_view.open(conn, session, row_kind="node", action=None, surface=None,
                                force="shadow")
        with pytest.raises(ReaderViewShadowMismatch):
            view.finish(rows)


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_the_trail_holds_only_per_row_reads_for_served_rows(
    published: dict, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    monkeypatch.setattr(nomos_store, "deployment_nomos_enforcement_mode", lambda name: mode)
    deployment = published["deployment"]
    session = _real_session(deployment, 1)

    def trail(conn: psycopg.Connection) -> list[dict]:
        return [r[0] for r in conn.execute(
            "SELECT target FROM okg.nomos_decisions WHERE deployment_name = %s"
            " ORDER BY decision_row_id", (deployment,),
        )]

    with psycopg.connect(published["dsn"]) as conn:
        before = trail(conn)
        view = reader_view.open_many(conn, session, action=None, surface=None)["node"]
        conn.commit()
        # Every class decided, nothing recorded.
        assert view.mode == "class" and view.decided_classes
        assert trail(conn) == before
        kept = _read(conn, 1, "node", view.predicate("r").as_string(conn))
        page = view.finish(kept[:2])
        conn.commit()
        added = trail(conn)[len(before):]
        assert added and all(t["kind"].endswith("_read") for t in added)
        # The rows finish was given (the per-row filter records each row it
        # decides); every served row is among them.
        assert {t["node_id"] for t in added if "node_id" in t} == {r["node_id"] for r in kept[:2]}
        assert {r["node_id"] for r in page} <= {r["node_id"] for r in kept[:2]}
        assert not [t for t in added if t.get("decided_by") == "read_class_catalog"]
        # The same rows through the per-row filter record nothing new: the
        # view's finish wrote exactly the per-row filter's decisions.
        _per_row(conn, "node", session, kept[:2])
        conn.commit()
        assert trail(conn)[len(before):] == added


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("overlay", [False, True])
def test_sql_exact_true_means_finish_returns_the_raw_rows(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
    kind: str, overlay: bool,
) -> None:
    """With Nomos off (the one case sql_exact is true), across the randomized
    rows: the predicate keeps every row and finish returns them unchanged."""
    monkeypatch.setattr(read_filter, "deployment_requires_nomos_runtime", lambda name: False)
    stats = _sql_sweep(pg, engine, monkeypatch, kind=kind, mode="enforce", seeds=60,
                       overlay=overlay)
    assert stats["exact_claimed"] == 60 and stats["exact_violation"] == 0, stats
    assert stats["exact_vs_per_row_mismatch"] == 0, stats


def test_mutation_sql_exact_true_while_deny_or_mask_rules_exist_is_caught(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _claim_exact(monkeypatch)
    stats = _sql_sweep(pg, engine, monkeypatch, kind="node", mode="enforce", seeds=60,
                       overlay=False)
    assert stats["exact_claimed"] > 0 and stats["exact_violation"] > 0, stats


@pytest.mark.parametrize("mutation", ["always_true", "ignores_redaction"])
def test_mutation_a_readable_that_says_true_where_the_per_row_filter_masks_is_caught(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch, mutation: str,
) -> None:
    from psycopg import sql

    if mutation == "always_true":
        monkeypatch.setattr(reader_view.ReaderView, "readable",
                            lambda self, alias, key: sql.SQL("TRUE"))
    else:
        real_init = reader_view.ReaderView.__init__

        def init(self: Any, **kw: Any) -> None:
            real_init(self, **kw)
            self._redacted_pairs = []
            self._always_redacted = frozenset()

        monkeypatch.setattr(reader_view.ReaderView, "__init__", init)
    stats = _sql_sweep(pg, engine, monkeypatch, kind="node", mode="enforce", seeds=60,
                       overlay=False)
    assert stats["readable_mismatch"] > 0, stats


# --------------------------------------------------------------------------- #
# admits_every_row: leaving the clause out reads the same rows                 #
# (PACT reader-view-decision-cache-3560)                                       #
# --------------------------------------------------------------------------- #
_HIDES_NOTHING_RULES = [
    {"rule_id": "deny_nobody", "effect": "deny", "match": {"source_id": "nowhere"}},
]
_MASKS_EVERYTHING_RULES = [
    {"rule_id": "redact_all", "effect": "redact", "match": {"resource_kind": "source_record"},
     "obligations": [{"type": "redaction"}]},
]


def _admits_sweep(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch, *,
    kind: str, mode: str, overlay: bool, seeds: int = 60,
) -> dict[str, int]:
    """For each random policy and stored rows: whether the view admits every
    row, and, when it does, how many stored rows the SQL clause drops and how
    many ``readable`` answers are FALSE (both must be zero)."""
    engine.mode = mode
    session = _session()
    stats = {"claimed": 0, "refused": 0, "rows_dropped": 0, "readable_false": 0}
    for seed in range(seeds):
        rng = random.Random(f"admits:{kind}:{mode}:{overlay}:{seed}")
        rows = [_with_payload(rng, r) for r in _rows(rng, kind, rng.randint(1, 30))]
        stored, catalog, _states = _stored_rows(rng, kind, rows)
        # A policy that hides nothing every third time, one that masks every
        # source record the time after, a random one (most hide or mask
        # something) otherwise, so both answers are exercised.
        if seed % 3 == 0:
            engine.bundle = _bundle(_HIDES_NOTHING_RULES)
        elif seed % 3 == 1:
            engine.bundle = _bundle(_MASKS_EVERYTHING_RULES)
        else:
            engine.bundle = _redacting_bundle(rng, kind, rows)
        catalogs = {"node": {}, "edge": {}}
        catalogs[kind] = catalog
        with _offline_view(monkeypatch, catalogs, overlay=overlay):
            view = reader_view.open(_Conn(), session, row_kind=kind, action=None, surface=None)
        if not view.admits_every_row:
            stats["refused"] += 1
            continue
        stats["claimed"] += 1
        stats["rows_dropped"] += len(stored) - len(_sql_positions(pg, view, kind, stored))
        keys = sorted({str(k) for r in stored for k in r["attrs"]} | {"text"})
        for answers in _sql_readable(pg, view, keys).values():
            stats["readable_false"] += sum(value is False for value in answers.values())
    return stats


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("overlay", [False, True])
def test_a_view_that_admits_every_row_keeps_every_row_in_sql(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
    kind: str, mode: str, overlay: bool,
) -> None:
    """Whenever the view says the clause may be left out, PostgreSQL keeps
    every stored row (NULL, other-version, uncatalogued and unusual-
    classification rows included) and ``readable`` is never FALSE for any
    key."""
    stats = _admits_sweep(pg, engine, monkeypatch, kind=kind, mode=mode, overlay=overlay)
    assert stats["rows_dropped"] == 0 and stats["readable_false"] == 0, stats
    # Audit lets denials through, so fewer random policies still hide or mask.
    assert stats["claimed"] >= 15 and stats["refused"] >= 5, stats


#: One class denied outright (jira), one denied for a classification only
#: (so its pairs are listed one by one), one masked for a classification only.
_PARTIAL_RULES = [
    {"rule_id": "deny_jira", "effect": "deny", "match": {"source_id": "jira"}},
    {"rule_id": "deny_restricted", "effect": "deny", "match": {"classification": "restricted"}},
    {"rule_id": "redact_docs_internal", "effect": "redact",
     "match": {"source_ids": "docs", "classification": "internal"},
     "obligations": [{"type": "redaction"}]},
]

#: Keys the keep_sql sweep asks about: `text` (masked for every declared
#: subtype here) and keys some declared subtypes reveal.
_KEEP_KEYS = ("text", "title", "name", "body")


def _keep_sweep(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch, *,
    kind: str, mode: str, overlay: bool, seeds: int = 40,
) -> dict[str, int]:
    """``keep_sql`` against ``predicate AND readable IS NOT FALSE``, both
    evaluated by PostgreSQL over the same stored rows, for every key."""
    engine.mode = mode
    session = _session()
    stats = {"views": 0, "class_views": 0, "compared": 0, "mismatch": 0, "dropped": 0, "kept": 0}
    for seed in range(seeds):
        rng = random.Random(f"keep:{kind}:{mode}:{overlay}:{seed}")
        rows = [_with_payload(rng, r) for r in _rows(rng, kind, rng.randint(1, 30))]
        stored, catalog, _states = _stored_rows(rng, kind, rows)
        if seed % 5 == 0:
            engine.bundle = _bundle(_HIDES_NOTHING_RULES)
        elif seed % 5 == 1:
            engine.bundle = _bundle(_MASKS_EVERYTHING_RULES)
        elif seed % 5 == 2:
            engine.bundle = _bundle(_PARTIAL_RULES)
        else:
            engine.bundle = _redacting_bundle(rng, kind, rows)
        catalogs = {"node": {}, "edge": {}}
        catalogs[kind] = catalog
        with _offline_view(monkeypatch, catalogs, overlay=overlay):
            view = reader_view.open(_Conn(), session, row_kind=kind, action=None, surface=None)
        stats["views"] += 1
        stats["class_views"] += view.mode == "class"
        _sql_positions(pg, view, kind, stored)  # loads the rows into rv_rows
        for key in _KEEP_KEYS:
            old = (
                f"({view.predicate('r').as_string(pg)}) AND"
                f" (({view.readable('r', key).as_string(pg)}) IS NOT FALSE)"
            )
            want = [r[0] for r in pg.execute(f"SELECT pos FROM rv_rows r WHERE {old} ORDER BY pos")]
            text = view.keep_sql("r", key).as_string(pg)
            got = [r[0] for r in pg.execute(f"SELECT pos FROM rv_rows r WHERE {text} ORDER BY pos")]
            stats["compared"] += 1
            stats["mismatch"] += got != want
            stats["dropped"] += len(stored) - len(want)
            stats["kept"] += len(want)
    return stats


@pytest.mark.parametrize("kind", ["node", "edge"])
@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("overlay", [False, True])
def test_keep_sql_keeps_the_rows_predicate_and_readable_keep(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
    kind: str, mode: str, overlay: bool,
) -> None:
    stats = _keep_sweep(pg, engine, monkeypatch, kind=kind, mode=mode, overlay=overlay)
    assert stats["mismatch"] == 0, stats
    assert stats["class_views"] >= 20 and stats["kept"] > 100, stats
    if mode == "enforce" or not overlay:
        assert stats["dropped"] > 20, stats  # the sweep does drop rows


def _synthetic_view(classes: list[uuid.UUID], hidden: set[uuid.UUID]) -> reader_view.ReaderView:
    internal = reader_view._md5("internal")
    decisions = {
        c: {
            internal: reader_view.ClassOutcome(c not in hidden, False),
            reader_view.OTHER_LABEL: reader_view.ClassOutcome(c not in hidden, False),
            reader_view.BLANK_LABEL: reader_view.ClassOutcome(False, False),
        }
        for c in classes
    }
    state = reader_view._KindState("node", "class", "read_class_catalog", decisions, overlay=False)
    return reader_view.ReaderView(
        conn=None, session=SimpleNamespace(), state=state, action="graph.read.mcp", surface="mcp",
        force=None, bundle_fingerprint="x", named=frozenset({"internal"}), subtype_pii={},
        fingerprint_inputs={},
    )


def test_the_planner_expects_most_rows_to_pass_when_few_classes_are_hidden(
    pg: psycopg.Connection,
) -> None:
    """PR 3863's clause made the planner expect a quarter of the rows (it
    guesses 0.5 for each COALESCE/CASE it cannot estimate), which priced the
    BM25 ordered scan out of the filtered passage read on the large copy. The
    negated form is built from class tests the planner can estimate from
    column statistics: about 10 percent of the rows are hidden here, and the
    estimate follows."""
    rng = random.Random(11)
    classes = [uuid.UUID(int=rng.getrandbits(128)) for _ in range(60)]
    freq_order = list(classes)
    hidden = set(freq_order[10:16])  # six mid-sized classes
    rows = 30_000
    array = "ARRAY[" + ",".join(f"'{c}'::uuid" for c in classes) + "]"
    pg.execute("DROP TABLE IF EXISTS pg_temp.plan_rows")
    pg.execute(
        "CREATE TEMP TABLE plan_rows (node_id text, subtype text, attrs jsonb,"
        " nomos_read_class uuid, nomos_read_class_version smallint)"
    )
    pg.execute(
        "INSERT INTO plan_rows SELECT 'n' || g, 'doc', jsonb_build_object('text', 'x'),"
        f" ({array})[1 + floor(power(random(), 2) * 60)::int], 1 FROM generate_series(1, {rows}) g"
    )
    pg.execute("ANALYZE plan_rows")
    view = _synthetic_view(classes, hidden)
    old = (
        f"({view.predicate('n').as_string(pg)}) AND"
        f" (({view.readable('n', 'text').as_string(pg)}) IS NOT FALSE)"
    )
    new = view.keep_sql("n", "text").as_string(pg)

    def estimate(where: str) -> tuple[int, int]:
        plan = pg.execute(
            f"EXPLAIN (FORMAT JSON) SELECT * FROM plan_rows n WHERE {where}"
        ).fetchone()[0][0]["Plan"]
        real = pg.execute(f"SELECT count(*) FROM plan_rows n WHERE {where}").fetchone()[0]
        return plan["Plan Rows"], real

    est_old, real_old = estimate(old)
    est_new, real_new = estimate(new)
    assert real_old == real_new and 0.5 * rows < real_new < rows  # the same rows pass
    assert est_old < 0.4 * rows, "the old clause was expected to be badly underestimated"
    assert est_new > 0.7 * real_new, (est_new, real_new)
    assert len(new) * 3 < len(old)


@pytest.mark.parametrize("omit", ["masked", "masked_pairs", "hidden_pairs"])
def test_mutation_a_keep_sql_that_forgets_a_drop_set_is_caught(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch, omit: str,
) -> None:
    real = reader_view.ReaderView._drop_sets

    def forgetful(self: Any) -> dict[str, Any]:
        sets = real(self)
        sets[omit] = []
        return sets

    monkeypatch.setattr(reader_view.ReaderView, "_drop_sets", forgetful)
    # Audit hides no class, so only an enforce sweep can notice a forgotten hidden pair.
    mode = "enforce" if omit == "hidden_pairs" else "audit"
    stats = _keep_sweep(pg, engine, monkeypatch, kind="node", mode=mode, overlay=False)
    assert stats["mismatch"] > 0, stats


def test_mutation_a_view_that_always_admits_every_row_is_caught(
    pg: psycopg.Connection, engine: _Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If every view said "leave the clause out", hidden rows would spend the
    caller's cap: the sweep finds stored rows the clause would drop."""
    monkeypatch.setattr(
        reader_view.ReaderView, "admits_every_row", property(lambda self: True), raising=False,
    )
    stats = _admits_sweep(pg, engine, monkeypatch, kind="node", mode="enforce", overlay=False)
    assert stats["rows_dropped"] > 0, stats


# --------------------------------------------------------------------------- #
# The content-addressed memo of the catalog and the subtype map                #
# (PACT reader-view-decision-cache-3560)                                       #
# --------------------------------------------------------------------------- #
def _statement_counter(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    count = {"n": 0}
    real = psycopg.Cursor.execute

    def counting(self: Any, *args: Any, **kwargs: Any) -> Any:
        count["n"] += 1
        return real(self, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", counting)
    return count


def _node_class(source: str) -> dict[uuid.UUID, dict[str, Any]]:
    facts = class_facts({"node_id": f"n:{source}", "attrs": {}, "source": source,
                         "source_record_id": {}, "source_revision": {}}, "node")
    return {rc.class_id_of_facts(facts): facts}


def test_the_catalog_read_is_memoized_by_content_and_follows_every_change(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    git, docs = _node_class("git"), _node_class("docs")
    with psycopg.connect(clean_runtime) as conn:
        record_read_classes(conn, git)
        conn.commit()
        statements = _statement_counter(monkeypatch)

        first = reader_view._load_catalog(conn)
        after_first = statements["n"]
        again = reader_view._load_catalog(conn)
        assert again == first and set(first["node"]) == set(git)
        assert statements["n"] - after_first == 1, "a repeat read more than the digest"
        # The caller owns what it gets: editing it never reaches the memo.
        again["node"].clear()
        assert set(reader_view._load_catalog(conn)["node"]) == set(git)

        # A new class is a new digest: it is read, and the memo does not answer.
        record_read_classes(conn, docs)
        conn.commit()
        assert set(reader_view._load_catalog(conn)["node"]) == set(git) | set(docs)

        # A rewritten row (its id is no longer the digest of its facts) is
        # refused even though the catalog was memoized a moment ago.
        conn.execute(
            f"UPDATE {READ_CLASS_CATALOG_TABLE} SET class_facts = class_facts || '{{\"x\": 1}}'"
            " WHERE read_class = %s",
            (next(iter(docs)),),
        )
        conn.commit()
        with pytest.raises(SubstrateError, match="does not match its facts"):
            reader_view._load_catalog(conn)
        with pytest.raises(SubstrateError, match="does not match its facts"):
            reader_view._load_catalog(conn)  # a refusal is never memoized


def test_the_subtype_pii_map_is_memoized_by_content_and_follows_every_change(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog_version(conn)
        conn.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) VALUES (%s, %s, %s)",
            ("doc", Jsonb({"pii_classes": {"name": "direct_identifier"}}), cv),
        )
        statements = _statement_counter(monkeypatch)

        first = reader_view._subtype_pii_classes(conn, cv)
        after_first = statements["n"]
        again = reader_view._subtype_pii_classes(conn, cv)
        assert first == again == {"doc": {"name": "direct_identifier"}}
        assert statements["n"] - after_first == 1, "a repeat read more than the digest"
        again["doc"]["name"] = "edited by the caller"
        assert reader_view._subtype_pii_classes(conn, cv) == first

        # Changed metadata under the same catalog version is read again.
        conn.execute(
            "UPDATE okg.subtypes SET metadata = %s WHERE name = 'doc' AND catalog_version_id = %s",
            (Jsonb({"pii_classes": {"name": "non_pii"}}), cv),
        )
        assert reader_view._subtype_pii_classes(conn, cv) == {"doc": {"name": "non_pii"}}

        # Malformed metadata is refused, never memoized.
        conn.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) VALUES (%s, %s, %s)",
            ("broken", Jsonb({"pii_classes": ["email"]}), cv),
        )
        for _ in range(2):
            with pytest.raises(SubstrateError, match="pii_classes must be a mapping"):
                reader_view._subtype_pii_classes(conn, cv)


def test_a_caller_editing_a_nested_class_fact_never_reaches_the_memo(
    clean_runtime: str,
) -> None:
    """Must fail if the memo hands out the stored per-class fact dicts: a caller
    that edits one (a nested list included) would change what every later read
    is told about the class."""
    git = _node_class("git")
    (class_id,) = git
    with psycopg.connect(clean_runtime) as conn:
        record_read_classes(conn, git)
        conn.commit()
        first = reader_view._load_catalog(conn)
        pristine = json.dumps(first["node"][class_id], sort_keys=True, default=str)
        first["node"][class_id]["source_ids"].append("injected")
        first["node"][class_id]["extra"] = "injected"
        again = reader_view._load_catalog(conn)  # from the memo
        assert json.dumps(again["node"][class_id], sort_keys=True, default=str) == pristine
        again["node"][class_id]["source_ids"].append("injected")
        third = reader_view._load_catalog(conn)
        assert json.dumps(third["node"][class_id], sort_keys=True, default=str) == pristine


def test_the_subtype_digest_tells_apart_names_that_look_like_its_separators(
    clean_runtime: str,
) -> None:
    """Must fail if the digest joins the fields with plain separators: the
    subtypes ``p`` and ``q`` and the single subtype named ``p:object:-,q`` would
    produce the same text."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        two = seed_catalog_version(conn)
        one = seed_catalog_version(conn)
        for name in ("p", "q"):
            conn.execute(
                "INSERT INTO okg.subtypes (name, metadata, catalog_version_id)"
                " VALUES (%s, %s, %s)", (name, Jsonb({}), two),
            )
        conn.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) VALUES (%s, %s, %s)",
            ("p:object:-,q", Jsonb({}), one),
        )
        assert two != one
        assert reader_view._subtype_pii_digest(conn, two) != reader_view._subtype_pii_digest(
            conn, one,
        )
