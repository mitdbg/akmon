"""PACT lane-h-nomos-read-class-stored (slice 1) on PostgreSQL.

- the two class columns on a fresh install and after an upgrade, added
  without rewriting either table; reads and writes on a database without them;
- every writer stores the class of the row version's own content;
- the Chronos questions of design Q5: (a) an interval upsert that omits the
  column stores NULL, never the previous version's value, while an SQL UPDATE
  keeps every column it does not SET; (b) no Chronos path rewrites a stamped
  version's content apart from its class: retraction, reopen, branch reset,
  fork and whole-row copies through every splice load mode keep each
  version's class equal to its content;
- the read filter keyed by stored classes on real rows, and the doctor's
  verification sample.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.common import EDGE_TABLE, NODE_TABLE
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos.read_class import (
    CLASS_VOCABULARY_VERSION,
    READ_CLASS_COLUMN,
    READ_CLASS_COLUMNS,
    READ_CLASS_VERSION_COLUMN,
    graph_carries_read_class,
    read_class,
    read_class_select_sql,
    stamp_rows,
    stored_read_class_report,
)
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, normalize_source_lineage
from tests.substrate.test_chronos_graph_store import _reset_chronos_graph

pytestmark = pytest.mark.postgres

_NODE_SELECT = (
    "SELECT node_id, subtype, attrs, source, source_record_id, source_revision, "
    "nomos_read_class, nomos_read_class_version FROM okg.graph_nodes ORDER BY node_id"
)
_EDGE_SELECT = (
    "SELECT edge_id, src, dst, edge_type, attrs, source, source_record_id, "
    "source_revision, nomos_read_class, nomos_read_class_version "
    "FROM okg.graph_edges ORDER BY edge_id"
)


def _nodes(n: int = 6, *, rev: int = 1, text: str = "t") -> list[NodeFact]:
    return [
        NodeFact(f"n:{i}", "doc", {"text": f"{text} {i}", "name": f"Person {i}"},
                 {"key": "ab"[i % 2]}, {"v": rev})
        for i in range(n)
    ]


def _edges(n: int = 6) -> list[EdgeFact]:
    return [
        EdgeFact(f"n:{i}", "n:0", "member_of", attrs={"chunk_ordinal": i},
                 source_record_id={"key": "ab"[i % 2]}, source_revision={"v": 1})
        for i in range(1, n)
    ]


def _assert_classes_match_content(rows: list[dict[str, Any]], kind: str) -> int:
    """Every row's stored class is its own content's class; returns how many
    rows carry one."""
    stamped = 0
    for row in rows:
        expected = read_class(row, kind)
        assert row[READ_CLASS_COLUMN] == expected, (kind, row.get(f"{kind}_id"))
        assert row[READ_CLASS_VERSION_COLUMN] == (
            None if expected is None else CLASS_VOCABULARY_VERSION
        )
        stamped += expected is not None
    return stamped


def _physical(conn: psycopg.Connection, table: str) -> str:
    return conn.execute(
        "SELECT physical_table FROM _chronos_branch_tables WHERE table_name = %s",
        (table,),
    ).fetchone()[0]


def _columns(conn: psycopg.Connection, schema: str, table: str) -> dict[str, tuple[str, str, Any]]:
    return {
        row[0]: (row[1], row[2], row[3])
        for row in conn.execute(
            "SELECT column_name, data_type, is_nullable, column_default"
            " FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
    }


def _relfilenode(conn: psycopg.Connection, qualified: str) -> int:
    return conn.execute(
        "SELECT relfilenode FROM pg_class WHERE oid = %s::regclass", (qualified,)
    ).fetchone()[0]


# --------------------------------------------------------------------------- #
# Columns                                                                      #
# --------------------------------------------------------------------------- #
def test_a_fresh_install_carries_nullable_class_columns(clean_runtime: str) -> None:
    _reset_chronos_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for table in (NODE_TABLE, EDGE_TABLE):
            physical = _physical(conn, table)
            for schema, name in (("okg", table.split(".")[1]), ("public", physical)):
                cols = _columns(conn, schema, name)
                assert cols[READ_CLASS_COLUMN] == ("uuid", "YES", None), (schema, name)
                assert cols[READ_CLASS_VERSION_COLUMN] == ("smallint", "YES", None), (schema, name)
            registered = json.loads(conn.execute(
                "SELECT columns FROM _chronos_branch_tables WHERE table_name = %s", (table,),
            ).fetchone()[0])
            assert set(READ_CLASS_COLUMNS) <= set(registered)


def _drop_class_columns(conn: psycopg.Connection) -> None:
    """Make the database look like one provisioned before slice 1."""
    from chronos_core.branching._common import clear_all_registry_caches

    for table in (NODE_TABLE, EDGE_TABLE):
        physical = _physical(conn, table)
        for column in READ_CLASS_COLUMNS:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
            conn.execute(f'ALTER TABLE public."{physical}" DROP COLUMN {column}')
        row = conn.execute(
            "SELECT columns, column_defs FROM _chronos_branch_tables WHERE table_name = %s",
            (table,),
        ).fetchone()
        keep = [
            (c, d) for c, d in zip(json.loads(row[0]), json.loads(row[1]))
            if c not in READ_CLASS_COLUMNS
        ]
        conn.execute(
            "UPDATE _chronos_branch_tables SET columns = %s, column_defs = %s WHERE table_name = %s",
            (json.dumps([c for c, _ in keep]), json.dumps([d for _, d in keep]), table),
        )
    clear_all_registry_caches()


def test_an_upgrade_adds_the_columns_without_rewriting_and_old_databases_keep_working(
    clean_runtime: str,
) -> None:
    from okg.substrate.nomos import read_filter

    _reset_chronos_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _drop_class_columns(conn)
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        # A writer on the old database: the stamped keys are ignored.
        store.apply_facts([*_nodes(), *_edges()], source="git")
        graph = store.checkout()
        assert not graph_carries_read_class(graph, NODE_TABLE)
        assert read_class_select_sql(graph, NODE_TABLE) == ""
        rows = graph.query(
            "SELECT node_id, attrs, source, source_record_id, source_revision "
            "FROM okg.graph_nodes ORDER BY node_id"
        )
        assert len(rows) == 6 and READ_CLASS_COLUMN not in rows[0]
        # A reader on the old database: no stored class, every row projected.
        assert all(read_filter.stored_read_class(r) is None for r in rows)
        before = {
            t: (_relfilenode(conn, t), _relfilenode(conn, f'public."{_physical(conn, t)}"'))
            for t in (NODE_TABLE, EDGE_TABLE)
        }

        ChronosGraphStore.from_connection(conn, ensure_schema=False).ensure_graph_column_upgrade()

        after = {
            t: (_relfilenode(conn, t), _relfilenode(conn, f'public."{_physical(conn, t)}"'))
            for t in (NODE_TABLE, EDGE_TABLE)
        }
        assert after == before, "adding the class columns rewrote a table"
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph = store.checkout()
        assert graph_carries_read_class(graph, NODE_TABLE)
        assert graph_carries_read_class(graph, EDGE_TABLE)
        old = graph.query(_NODE_SELECT)
        assert {r[READ_CLASS_COLUMN] for r in old} == {None}
        # Writes after the upgrade stamp the new versions.
        store.apply_facts(_nodes(rev=2, text="u"), source="git")
        assert _assert_classes_match_content(store.checkout().query(_NODE_SELECT), "node") == 6


# --------------------------------------------------------------------------- #
# Writers                                                                      #
# --------------------------------------------------------------------------- #
def test_fact_apply_stamps_nodes_and_edges(clean_runtime: str) -> None:
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([*_nodes(), *_edges()], source="git")
        graph = store.checkout()
        assert _assert_classes_match_content(graph.query(_NODE_SELECT), "node") == 6
        assert _assert_classes_match_content(graph.query(_EDGE_SELECT), "edge") == 5
        # One source: the rows share a class (the record key is not a class
        # fact: no class-safe rule can read it).
        assert len({r[READ_CLASS_COLUMN] for r in graph.query(_NODE_SELECT)}) == 1
    finally:
        store.close()


def test_a_write_prepared_ahead_stamps_the_classes_it_computed_before_the_lock(
    clean_runtime: str,
) -> None:
    """Issue #2941: an apply prepared before the publish lock computes its
    rows' classes then; the write under the lock reuses them and stores the
    same classes a direct write stores."""
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(_nodes(), source="git")
        edges = _edges()
        prepared = store.prepare_apply_facts(
            edges, branch="default", source="git", key_fingerprint=True,
        )
        assert len(prepared.read_classes) == len(edges)
        result = store.apply_facts(edges, source="git", prepared=prepared)
        assert result.timings_ms.get("prepared.reused") == 1.0, result.timings_ms
        assert "prelock.prepare.read_classes" in result.timings_ms
        assert _assert_classes_match_content(store.checkout().query(_EDGE_SELECT), "edge") == 5
    finally:
        store.close()


def test_a_ranked_edge_prepared_ahead_reuses_its_class_under_the_lock(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2941: a ranked edge type's upserts are held apart for the
    declarer step. Their classes are computed before the lock too, so the
    write under the lock computes no class for a winning upsert. (Every
    derived ``calls``/``references`` edge of okg-workspace takes this path.)"""
    from okg.substrate.graph_store_internal.edge_declarers import (
        sync_edge_precedence,
    )
    from okg.substrate.nomos import read_class

    _reset_chronos_graph(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        sync_edge_precedence(conn, {"member_of": ["git"]})
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(_nodes(), source="git")
        edges = _edges()
        prepared = store.prepare_apply_facts(
            edges, branch="default", source="git", key_fingerprint=True,
        )
        assert len(prepared.ranked_edge_upserts) == len(edges)
        assert not prepared.edge_upserts
        assert len(prepared.read_classes) == len(edges)

        computed: list[Any] = []
        compute = read_class._facts_of_stored

        def counting(stored: Any, kind: str) -> Any:
            computed.append(stored)
            return compute(stored, kind)

        monkeypatch.setattr(read_class, "_facts_of_stored", counting)
        result = store.apply_facts(edges, source="git", prepared=prepared)
        assert result.timings_ms.get("prepared.reused") == 1.0, result.timings_ms
        assert computed == []
        assert _assert_classes_match_content(store.checkout().query(_EDGE_SELECT), "edge") == 5
    finally:
        store.close()
        with psycopg.connect(clean_runtime) as conn:
            sync_edge_precedence(conn, {})


def test_publisher_derived_rows_are_stamped_after_their_lineage(clean_runtime: str) -> None:
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(_nodes(), source="git")
        store.apply_facts(
            [EdgeFact("n:1", "n:2", "relates_to", attrs={}, source_record_id={},
                      source_revision={})],
            source="_publisher_derived",
        )
        edges = store.checkout().query(_EDGE_SELECT)
        assert len(edges) == 1
        assert edges[0]["attrs"][LINEAGE_ATTR]["source_ids"] == ["git"]
        assert _assert_classes_match_content(edges, "edge") == 1
    finally:
        store.close()


def test_the_embedding_write_stamps_its_new_version(clean_runtime: str) -> None:
    from okg.substrate.embeddings import EmbeddingConfig, build_embedding_execution_plan
    from okg.substrate.runtime.embed_workflow import _embed_and_write_one

    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(_nodes(1), source="fixture")
        row = store.checkout().query(_NODE_SELECT)[0]
        text_hash = store.checkout().query(
            "SELECT text_hash FROM okg.graph_nodes WHERE node_id = 'n:0'"
        )[0]["text_hash"]
    finally:
        store.close()
    _embed_and_write_one(
        dsn=clean_runtime, node_id="n:0", text=row["attrs"]["text"], text_hash=text_hash,
        embedding_plan=build_embedding_execution_plan(EmbeddingConfig(enabled=True, backend="hash")),
    )
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        after = store.checkout().query(
            _NODE_SELECT.replace("FROM", ", embedding IS NOT NULL AS embedded FROM")
        )[0]
    finally:
        store.close()
    assert after["embedded"] is True
    assert _assert_classes_match_content([after], "node") == 1
    assert after[READ_CLASS_COLUMN] == row[READ_CLASS_COLUMN]


def test_lineage_repair_stamps_both_its_upsert_and_its_single_row_update(
    clean_runtime: str,
) -> None:
    from okg.substrate.nomos.source_lineage_repair import (
        _update_single_attrs,
        _upsert_logical_rows_with_retry,
    )

    _reset_chronos_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.apply_facts(_nodes(2), source="git")
        rows = [dict(r) for r in store.checkout().query(
            "SELECT node_id, subtype, attrs, text_hash, embedding_model, source,"
            " source_record_id, source_revision, run_id, observed_at"
            " FROM okg.graph_nodes ORDER BY node_id"
        )]
        before = {r["node_id"]: r[READ_CLASS_COLUMN] for r in store.checkout().query(_NODE_SELECT)}
        relineaged = normalize_source_lineage(
            source_id="jira", source_record_id={"key": "z"}, source_revision={"v": 9},
        )
        batch = [{**rows[0], "attrs": {**rows[0]["attrs"], LINEAGE_ATTR: relineaged}}]
        _upsert_logical_rows_with_retry(
            store, connection=conn, branch="default", table=NODE_TABLE,
            key_column="node_id", rows=batch,
        )
        _update_single_attrs(
            store, branch="default", table=NODE_TABLE, key_column="node_id",
            row={**rows[1], "attrs": {**rows[1]["attrs"], LINEAGE_ATTR: relineaged}},
        )
        after = store.checkout().query(_NODE_SELECT)
        assert _assert_classes_match_content(after, "node") == 2
        assert all(r[READ_CLASS_COLUMN] != before[r["node_id"]] for r in after)


# --------------------------------------------------------------------------- #
# Chronos (design Q5)                                                          #
# --------------------------------------------------------------------------- #
def _node_rows(n: int, *, source: str = "git", rev: int = 1) -> list[dict[str, Any]]:
    return [
        {
            "node_id": f"n:{i}", "subtype": "doc", "attrs": {"text": f"t{i} r{rev}"},
            "text_hash": None, "embedding_model": None, "source": source,
            "source_record_id": {"key": f"k{i % 3}"}, "source_revision": {"v": rev},
            "run_id": None, "observed_at": None,
        }
        for i in range(n)
    ]


@pytest.mark.parametrize("load_mode", ["jsonb", "copy"])
@pytest.mark.parametrize("rows", [1, 40])
def test_q5a_an_upsert_that_omits_the_class_stores_null_not_the_previous_value(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, load_mode: str, rows: int,
) -> None:
    monkeypatch.setenv("CHRONOS_INTERVAL_SPLICE_LOAD_MODE", load_mode)
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        session = store.checkout()
        stamped = _node_rows(rows)
        stamp_rows(stamped, "node")
        with session.transaction():
            session.upsert_rows(NODE_TABLE, stamped)
        store.checkpoint_generation(1)
        # Same writer, new content, no class keys at all.
        with session.transaction():
            session.upsert_rows(NODE_TABLE, _node_rows(rows, rev=2))
        head = store.checkout().query(_NODE_SELECT)
        assert {r[READ_CLASS_COLUMN] for r in head} == {None}
        # The earlier version still carries the class of its own content.
        assert _assert_classes_match_content(
            store.checkout_generation(1).query(_NODE_SELECT), "node"
        ) == rows
    finally:
        store.close()


def test_q5a_an_sql_update_keeps_every_column_it_does_not_set(clean_runtime: str) -> None:
    """Why the lineage repair's UPDATE sets the class: a Chronos UPDATE copies
    the previous version's other columns, the stored class included."""
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(_nodes(1), source="git")
        before = store.checkout().query(_NODE_SELECT)[0]
        relineaged = normalize_source_lineage(source_id="jira", source_record_id={"key": "z"})
        store.checkout().execute(
            "UPDATE okg.graph_nodes SET attrs = :attrs WHERE node_id = 'n:0'",
            {"attrs": {LINEAGE_ATTR: relineaged}},
        )
        after = store.checkout().query(_NODE_SELECT)[0]
        assert after[READ_CLASS_COLUMN] == before[READ_CLASS_COLUMN]
        assert read_class(after, "node") != after[READ_CLASS_COLUMN]
    finally:
        store.close()


@pytest.mark.parametrize("load_mode", ["jsonb", "copy"])
@pytest.mark.parametrize("shape", ["single", "batch", "mixed"])
def test_q5b_whole_rows_read_back_and_rewritten_keep_their_class(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, load_mode: str, shape: str,
) -> None:
    """Run undo, branch restore and merge-back write rows they read from the
    graph (every column, the class as a UUID) through upsert_rows /
    apply_table_edits. Every splice path must store the class unchanged."""
    monkeypatch.setenv("CHRONOS_INTERVAL_SPLICE_LOAD_MODE", load_mode)
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        session = store.checkout()
        n = 1 if shape == "single" else 30
        first = _node_rows(n + 2)
        stamp_rows(first, "node")
        with session.transaction():
            session.upsert_rows(NODE_TABLE, first)
        store.checkpoint_generation(1)
        copied = [dict(r) for r in store.checkout_generation(1).query(
            "SELECT * FROM okg.graph_nodes ORDER BY node_id"
        )][:n]
        assert all(isinstance(r[READ_CLASS_COLUMN], uuid.UUID) for r in copied)
        changed = _node_rows(n + 2, source="jira", rev=3)
        stamp_rows(changed, "node")
        with session.transaction():
            session.upsert_rows(NODE_TABLE, changed)
        with session.transaction():
            if shape == "mixed":
                session.apply_table_edits(
                    NODE_TABLE, upserts=copied, deletes=[{"node_id": f"n:{n + 1}"}],
                )
            else:
                session.upsert_rows(NODE_TABLE, copied)
        head = {r["node_id"]: r for r in store.checkout().query(_NODE_SELECT)}
        for row in copied:
            assert head[row["node_id"]][READ_CLASS_COLUMN] == row[READ_CLASS_COLUMN]
        assert _assert_classes_match_content(list(head.values()), "node") == len(head)
    finally:
        store.close()


def test_q5b_retraction_reopen_restore_and_fork_keep_each_version_true(
    clean_runtime: str,
) -> None:
    _reset_chronos_graph(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([*_nodes(), *_edges()], source="git")
        store.checkpoint_generation(1)
        gen1 = store.checkout_generation(1).query(_NODE_SELECT)
        # Retraction, then reopen with other content.
        store.apply_facts([NodeFact("n:3", "doc", {}, {"key": "b"}, {"v": 1}, op="R")],
                          source="git")
        store.apply_facts([NodeFact("n:3", "doc", {"text": "back"}, {"key": "q"}, {"v": 5})],
                          source="jira")
        store.checkpoint_generation(2)
        # Fork from generation 2 and change rows only there.
        store.create_branch("fork", from_branch="default")
        store.apply_facts(_nodes(rev=7, text="fork"), source="docs", branch="fork")
        # Change the default branch, then reset it to generation 1.
        store.apply_facts(_nodes(rev=9, text="later"), source="jira")
        store.restore_branch_to_generation(1, branch="default", deployment_name="default")

        for view, kind_select in (
            (store.checkout_generation(1), _NODE_SELECT),
            (store.checkout_generation(2), _NODE_SELECT),
            (store.checkout("fork"), _NODE_SELECT),
            (store.checkout("default"), _NODE_SELECT),
            (store.checkout("default"), _EDGE_SELECT),
        ):
            kind = "edge" if kind_select is _EDGE_SELECT else "node"
            assert _assert_classes_match_content(view.query(kind_select), kind) > 0
        assert store.checkout_generation(1).query(_NODE_SELECT) == gen1
        assert store.checkout("default").query(_NODE_SELECT) == gen1
        reopened = {r["node_id"]: r for r in store.checkout_generation(2).query(_NODE_SELECT)}
        assert reopened["n:3"]["source"] == "jira"
        assert {r["source"] for r in store.checkout("fork").query(_NODE_SELECT)} == {"docs"}
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Read and doctor                                                              #
# --------------------------------------------------------------------------- #
def test_the_read_filter_decides_real_rows_by_their_stored_classes(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from okg.substrate.nomos import activate_policy_bundle, read_filter, store as nomos_store
    from okg.substrate.nomos.core import compile_policy_graph

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:reader")
    _reset_chronos_graph(clean_runtime)
    deployment = f"read-class-stored-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime) as conn:
        graph_store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph_store.apply_facts(_nodes(12), source="git")
        graph_store.apply_facts(
            [NodeFact(f"j:{i}", "doc", {"text": "j"}, {"key": "a"}, {"v": 1}) for i in range(4)],
            source="jira",
        )
        conn.commit()
        activate_policy_bundle(
            conn, deployment_name=deployment,
            bundle=compile_policy_graph({
                "policy_bundle_id": "nomos:test:stored", "policy_version": "v1",
                "default_effect": "allow",
                "rules": [
                    {"rule_id": "deny_jira", "effect": "deny", "actions": ["*"],
                     "subjects": ["*"], "match": {"source_id": "jira"}, "priority": 2},
                    {"rule_id": "redact_git", "effect": "redact", "actions": ["*"],
                     "subjects": ["*"], "match": {"source_ids": "git"}, "priority": 1,
                     "obligations": [{"type": "redaction"}]},
                ],
            }),
            scope=f"deployment:{deployment}", activated_by="test:operator", bootstrap=True,
        )
        conn.commit()
        graph = graph_store.checkout()
        rows = graph.query(
            "SELECT node_id, subtype, attrs, source, source_record_id, source_revision"
            f"{read_class_select_sql(graph, NODE_TABLE)} FROM okg.graph_nodes ORDER BY node_id"
        )
        session = SimpleNamespace(
            deployment_name=deployment, generation_id=0, branch_name="default",
            catalog_version_id=None, nomos_subject="subject:reader",
            nomos_read_action=None, nomos_read_surface=None, nomos_multi_user=False,
            nomos_identity_claim=None, nomos_access_context=None,
        )
        for mode in ("enforce", "audit"):
            monkeypatch.setattr(nomos_store, "deployment_nomos_enforcement_mode",
                                lambda name, m=mode: m)
            got, path = read_filter.read_class_filter(
                conn, session, [dict(r) for r in rows], row_kind="node", verify_sample=len(rows),
            )
            want = read_filter.filter_node_rows_for_session(conn, session, [dict(r) for r in rows])
            assert [(r["node_id"], dict(r["attrs"])) for r in got] == [
                (r["node_id"], dict(r["attrs"])) for r in want
            ], mode
            assert path.path == "read_class" and path.stored_class_rows == len(rows)
            assert path.classes == 2  # git, jira
            assert path.verified_rows == len(got)
        assert len(got) == 16 and len(want) == 16  # audit returns every row


def test_the_doctor_reports_counts_and_refuses_a_tampered_class(clean_runtime: str) -> None:
    _reset_chronos_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.apply_facts([*_nodes(), *_edges()], source="git")
        store.apply_facts([NodeFact("bare", "doc", {}, {}, {})], source=None)
        report = stored_read_class_report(store.checkout())
        nodes = report["tables"][NODE_TABLE]
        assert nodes["rows"] == 7 and nodes["current_class"] == 6 and nodes["null_class"] == 1
        assert nodes["distinct_classes"] == 1 and nodes["verified"] == 6
        assert report["tables"][EDGE_TABLE]["current_class"] == 5
        assert report["mismatches"] == []

        physical = _physical(conn, NODE_TABLE)
        conn.execute(
            f'UPDATE public."{physical}" SET nomos_read_class = %s WHERE node_id = %s',
            (uuid.UUID(int=1), "n:2"),
        )
        tampered = stored_read_class_report(ChronosGraphStore.from_connection(
            conn, ensure_schema=False).checkout())
        assert [m["node_id"] for m in tampered["mismatches"]] == ["n:2"]


def _ranked_calls_seed(dsn: str) -> None:
    """Item 92a: code-structure (s1) and the enricher (e1) both declare the
    calls edge a->b; the enricher ranks first, so its row is stored."""
    from tests.substrate.test_edge_declarers_db import RANKING, _rank
    from tests.substrate.test_edge_declarers_lifecycle_db import _seed

    _reset_chronos_graph(dsn)
    _rank(dsn, RANKING)
    store = ChronosGraphStore.connect(dsn)
    try:
        _seed(store)
    finally:
        store.close()
    assert _assert_classes_match_content(_stored_calls(dsn), "edge") == 1


def _stored_calls(dsn: str) -> list[dict[str, Any]]:
    store = ChronosGraphStore.connect(dsn)
    try:
        return [
            row for row in store.checkout().query(_EDGE_SELECT)
            if row["edge_type"] == "calls"
        ]
    finally:
        store.close()


def test_a_withdrawn_declaration_stamps_the_next_ranked_row(clean_runtime: str) -> None:
    """Item 92a's withdrawal writes the next-ranked declarer's row
    (withdraw_edge_declarations); it carries its own content's class."""
    from tests.substrate.test_edge_declarers_db import ENRICHER, STRUCTURE

    _ranked_calls_seed(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.withdraw_edge_declarations(branch="default", sources=[ENRICHER])
    finally:
        store.close()
    rows = _stored_calls(clean_runtime)
    assert [row["source"] for row in rows] == [STRUCTURE]
    assert _assert_classes_match_content(rows, "edge") == 1


def test_a_ranking_change_stamps_the_settled_row(clean_runtime: str) -> None:
    """Item 92a's settling (_settle_ranked_edges, here after a ranking drops
    the enricher) writes the new winner's row; it carries its own class."""
    from tests.substrate.test_edge_declarers_db import STRUCTURE, _rank

    _ranked_calls_seed(clean_runtime)
    _rank(clean_runtime, {"calls": [STRUCTURE, "code-lsp-resolution"]})
    rows = _stored_calls(clean_runtime)
    assert [row["source"] for row in rows] == [STRUCTURE]
    assert _assert_classes_match_content(rows, "edge") == 1


def test_a_ranked_retraction_stamps_the_handed_on_row(clean_runtime: str) -> None:
    """Item 92a through the fact apply: the enricher retracts its edge and
    the row passes to code-structure's declaration; the stored row carries
    its own content's class."""
    from tests.substrate.test_edge_declarers_db import ENRICHER, STRUCTURE, _edge

    _ranked_calls_seed(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1", op="R")],
            source=ENRICHER, strict_endpoints=False,
        )
    finally:
        store.close()
    rows = _stored_calls(clean_runtime)
    assert [row["source"] for row in rows] == [STRUCTURE]
    assert _assert_classes_match_content(rows, "edge") == 1
