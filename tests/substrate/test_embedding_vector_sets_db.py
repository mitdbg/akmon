"""Vector-set storage on PostgreSQL (PACT openrouter-embeddings-hnsw, v3.2).

The tables ``okg migrate`` creates, the document links the graph writer adds
in its own transaction, the watermark the publisher writes in the publish
transaction, and the pinned read's row-content check. Free ``hash`` backend
only; no paid call.
"""
from __future__ import annotations

import json
import re
import threading
import time
from pathlib import Path

import psycopg
import pytest

from okg.substrate.embeddings.config import parse_embedding_config
from okg.substrate.embeddings.store import (
    node_document_at_pin,
    record_watermarks,
    register_sets,
    seq_advanced_since,
    set_index_name,
    take_seq,
    watermark_for,
    write_vectors,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp.search_fixture import make_search_deployment


pytestmark = pytest.mark.postgres

SOURCE = "system:analyst_search_fixture"
EMBEDDINGS = {
    "enabled": True,
    "sets": {
        "main": {
            "backend": "hash",
            "document": {
                "default": {"header": ["subtype", "title"], "body": "text"},
                "types": {
                    "jira_issue": {"header": ["subtype", "title", "status"]},
                },
            },
        }
    },
}


def _deployment(tmp_path, dsn: str, block: dict | None = EMBEDDINGS) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(path.read_text())
    if block is not None:
        manifest["embeddings"] = block
    path.write_text(json.dumps(manifest))
    return deployment


def _vector_set(block: dict = EMBEDDINGS):
    return parse_embedding_config(plural=block).search_vector_set


def _seed(dsn: str, subtypes=("jira_issue",)) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=list(subtypes))


def _write(dsn: str, *nodes: NodeFact) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            list(nodes), source=SOURCE, strict_endpoints=True,
        )


def _issue(node_id: str = "jira:1", **attrs) -> NodeFact:
    values = {"title": "Build breaks", "status": "open", "owner": "ana",
              "text": "The nightly build fails on main."}
    values.update(attrs)
    return NodeFact(
        node_id=node_id, subtype="jira_issue", attrs=values,
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _publish(dsn: str, deployment: str) -> dict:
    result = Publisher(
        dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment),
    ).cycle()
    assert result["status"] == "published", result
    return result


def _register(dsn: str, vector_set) -> None:
    with psycopg.connect(dsn) as conn:
        register_sets(conn, [vector_set])
        conn.commit()


def _at(dsn: str, vector_set, generation_id: str, node_id: str = "jira:1"):
    with psycopg.connect(dsn, autocommit=True) as conn:
        return node_document_at_pin(
            conn, vector_set=vector_set, generation_id=generation_id, node_id=node_id,
        )


def _store_vector(dsn: str, vector_set, doc: str) -> None:
    with psycopg.connect(dsn) as conn:
        assert write_vectors(conn, vector_set.set_id, [(doc, [0.5] * vector_set.width)]) == 1
        conn.commit()


def _links(dsn: str, node_id: str = "jira:1") -> list[tuple[str, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return [
            (str(r[0]), str(r[1])) for r in conn.execute(
                "SELECT row_hash, doc_hash FROM okg.node_documents "
                "WHERE node_id = %s ORDER BY seq", (node_id,),
            ).fetchall()
        ]


def test_migrate_registers_the_set_records_a_watermark_and_builds_its_index(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.db.migrate import _ensure_vector_sets

    deployment = _deployment(tmp_path, clean_runtime)
    _seed(clean_runtime)
    _write(clean_runtime, _issue())
    before = _publish(clean_runtime, deployment)["generation_id"]
    vector_set = _vector_set()

    (report,) = _ensure_vector_sets(clean_runtime, deployment)
    assert report["set_id"] == vector_set.set_id and report["created"] is True
    assert report["watermark_generations"] == [before]
    assert report["index"]["status"] == "present"
    assert report["index"]["index"] == set_index_name(vector_set)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        indexdef = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'okg' "
            "AND indexname = %s", (set_index_name(vector_set),),
        ).fetchone()[0]
        assert "hnsw" in indexdef and vector_set.set_id in indexdef
        assert "halfvec(2560)" in indexdef
        # The set's watermark for the generation current at migration.
        assert watermark_for(conn, before, vector_set.set_id) == (0, 0)
        row = conn.execute(
            "SELECT recorded_by FROM okg.embedding_watermarks WHERE generation_id = %s",
            (before,),
        ).fetchone()
        assert row[0] == "migrate"
        # The old node columns are still there, untouched.
        cols = {
            r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'okg' AND table_name = 'graph_nodes'"
            ).fetchall()
        }
        assert {"embedding", "embedding_model"} <= cols
    # A rerun keeps the counter and the index.
    (again,) = _ensure_vector_sets(clean_runtime, deployment)
    assert again["created"] is False and again["index"]["action"] == "none"


def test_migrate_refuses_a_type_header_field_some_reader_cannot_see(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.db.migrate import _ensure_vector_sets

    deployment = _deployment(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec(
            "jira_issue",
            metadata={"pii_classes": {"title": "non_pii", "status": "direct_identifier",
                                      "text": "free_text"}},
        )])
    with pytest.raises(SubstrateError) as caught:
        _ensure_vector_sets(clean_runtime, deployment)
    assert caught.value.code == "embedding_header_field_masked"
    assert caught.value.data["schema_migrated"] is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute("SELECT count(*) FROM okg.embedding_sets").fetchone()[0] == 0


def test_an_edit_outside_the_header_keeps_the_vector_and_a_header_edit_does_not(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _deployment(tmp_path, clean_runtime)
    vector_set = _vector_set()
    _seed(clean_runtime)
    _register(clean_runtime, vector_set)
    _write(clean_runtime, _issue())
    g1 = _publish(clean_runtime, deployment)["generation_id"]
    first = _at(clean_runtime, vector_set, g1)
    assert first["doc_hash"] is not None and first["has_vector"] is False
    doc = first["doc_hash"]

    # A backfill stores the vector; the pinned page does not change ...
    _store_vector(clean_runtime, vector_set, doc)
    assert _at(clean_runtime, vector_set, g1) == {"doc_hash": doc, "has_vector": False}
    # ... and the next plain publish is not a reuse: it shows the vector.
    second = _publish(clean_runtime, deployment)
    assert not second.get("reused_generation"), second
    assert second["timings_ms"]["embedding_writes_probe.vector_sets"] == 1.0
    g2 = second["generation_id"]
    assert _at(clean_runtime, vector_set, g2) == {"doc_hash": doc, "has_vector": True}
    assert _publish(clean_runtime, deployment)["reused_generation"] is True

    # `owner` is not in the header: a new row content, the same document.
    _write(clean_runtime, _issue(owner="ben"))
    g3 = _publish(clean_runtime, deployment)["generation_id"]
    assert _at(clean_runtime, vector_set, g3) == {"doc_hash": doc, "has_vector": True}
    links = _links(clean_runtime)
    assert len(links) == 2 and links[0][0] != links[1][0]
    assert {d for _r, d in links} == {doc}

    # `status` is in the header: a new document, no vector until refilled.
    _write(clean_runtime, _issue(owner="ben", status="closed"))
    g4 = _publish(clean_runtime, deployment)["generation_id"]
    closed = _at(clean_runtime, vector_set, g4)
    assert closed["doc_hash"] not in (None, doc) and closed["has_vector"] is False
    # A body edit is a new document too.
    _write(clean_runtime, _issue(owner="ben", status="closed", text="Fixed."))
    g5 = _publish(clean_runtime, deployment)["generation_id"]
    body = _at(clean_runtime, vector_set, g5)
    assert body["doc_hash"] not in (None, doc, closed["doc_hash"])
    # Reverting to embedded content reuses its vector for free.
    _write(clean_runtime, _issue(owner="carl"))
    g6 = _publish(clean_runtime, deployment)["generation_id"]
    assert _at(clean_runtime, vector_set, g6) == {"doc_hash": doc, "has_vector": True}
    # The earlier pins still read what they read.
    assert _at(clean_runtime, vector_set, g1) == {"doc_hash": doc, "has_vector": False}
    assert _at(clean_runtime, vector_set, g4)["has_vector"] is False


def test_an_in_place_chronos_rewrite_cannot_serve_the_old_vector(
    clean_runtime: str, tmp_path,
) -> None:
    """A second write inside one Chronos segment updates the row in place:
    same live_lo, new content. The link names content, so the old document's
    vector is not served for the new text."""
    deployment = _deployment(tmp_path, clean_runtime)
    vector_set = _vector_set()
    _seed(clean_runtime)
    _register(clean_runtime, vector_set)
    _write(clean_runtime, _issue(text="alpha body"))
    ((_row, alpha_doc),) = _links(clean_runtime)
    _store_vector(clean_runtime, vector_set, alpha_doc)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
        ).fetchone()[0]
        live_lo_sql = (
            f'SELECT live_lo FROM "{physical}" WHERE node_id = %s AND deleted = FALSE '
            "ORDER BY live_lo DESC LIMIT 1"
        )
        live_lo_before = conn.execute(live_lo_sql, ("jira:1",)).fetchone()[0]
        graph = ChronosGraphStore.from_connection(conn).checkout()
        attrs = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": "jira:1"},
        )[0]["attrs"]
        attrs = dict(attrs, text="beta body")
        graph.execute(
            "UPDATE okg.graph_nodes SET attrs = :attrs WHERE node_id = :node_id",
            {"attrs": attrs, "node_id": "jira:1"},
        )
        live_lo_after = conn.execute(live_lo_sql, ("jira:1",)).fetchone()[0]
    assert live_lo_after == live_lo_before, "the rewrite must be in place"

    generation = _publish(clean_runtime, deployment)["generation_id"]
    at_pin = _at(clean_runtime, vector_set, generation)
    assert at_pin == {"doc_hash": None, "has_vector": False}
    # Must-fail: a link found by node id alone (no row-content check) would
    # hand this pin the alpha vector for the beta text.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        naive = conn.execute(
            "SELECT d.doc_hash FROM okg.node_documents d "
            "JOIN okg.text_embeddings e USING (set_id, doc_hash) "
            "WHERE d.node_id = %s AND d.set_id = %s",
            ("jira:1", vector_set.set_id),
        ).fetchall()
    assert [str(r[0]) for r in naive] == [alpha_doc]


def _watermark(dsn: str, generation_id: str, vector_set) -> tuple[int, int, str] | None:
    """``(links bound, vectors bound, recorded_by)``, or None."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT max_seq, max_vector_seq, recorded_by FROM okg.embedding_watermarks "
            "WHERE generation_id = %s AND set_id = %s",
            (str(generation_id), vector_set.set_id),
        ).fetchone()
    return None if row is None else (int(row[0]), int(row[1]), str(row[2]))


def test_compaction_rollover_and_fast_forward_record_watermarks(
    clean_runtime: str, monkeypatch,
) -> None:
    """Every way a generation is minted records the set's watermark, so a
    pin on it reads instead of refusing `embedding_watermark_missing`:
    a checkpoint, a compaction, a rollover and a fast-forward."""
    deployment = "watermark-paths"
    vector_set = _vector_set()
    _register(clean_runtime, vector_set)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts([NodeFact("n:1", "claim", {"text": "first body"}, {}, {})])
        first = store.checkpoint_generation("wm-first", deployment_name=deployment)
        _store_vector(clean_runtime, vector_set, _links(clean_runtime, "n:1")[0][1])
        compacted = store.compact_branch_from_generation(
            first, deployment_name=deployment,
        )["new_generation_id"]
        rolled = store.rollover_branch_from_generation(
            compacted, deployment_name=deployment,
        )["new_generation_id"]
        store.create_branch("staging:wm", from_branch="default")
        store.apply_facts(
            [NodeFact("n:2", "claim", {"text": "staged body"}, {}, {})],
            branch="staging:wm",
        )
        forwarded = store.fast_forward(
            "staging:wm", target="default", deployment_name=deployment,
        )
    finally:
        store.close()
    assert _watermark(clean_runtime, first, vector_set) == (1, 0, "publisher")
    for generation in (compacted, rolled, forwarded):
        watermark = _watermark(clean_runtime, generation, vector_set)
        assert watermark is not None, generation
        assert watermark[1] >= 1, "the vector written after the first checkpoint"
        pinned = _at(clean_runtime, vector_set, generation, "n:1")
        assert pinned is not None and pinned["doc_hash"] is not None, generation
    assert _at(clean_runtime, vector_set, forwarded, "n:2")["doc_hash"] is not None


def test_a_set_from_the_shared_counter_keeps_its_older_pins(
    clean_runtime: str, monkeypatch,
) -> None:
    """A set registered when links and vectors shared one counter: migrate
    starts its vector counter at the shared counter's value, and a watermark
    recorded then (no max_vector_seq) bounds both tables, so a vector
    written after the split is not under it. (The schema body is replayed:
    an upgrade from that build has a different schema checksum.)"""
    import okg.substrate.db.management as management
    from okg.substrate.db.migrate import migrate

    monkeypatch.setattr(management, "applied_executable_schema_checksum", lambda conn: None)
    vector_set = _vector_set()
    _register(clean_runtime, vector_set)
    with psycopg.connect(clean_runtime) as conn:
        assert take_seq(conn, vector_set.set_id, 4) == 1  # the shared counter at 5
        conn.execute(
            "INSERT INTO okg.embedding_watermarks "
            "(generation_id, set_id, max_seq, recorded_by) "
            "VALUES ('gen-shared', %s, 4, 'publisher')",
            (vector_set.set_id,),
        )
        conn.execute(
            "DELETE FROM okg.embedding_vector_counters WHERE set_id = %s",
            (vector_set.set_id,),
        )
        conn.commit()
    migrate(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        assert watermark_for(conn, "gen-shared", vector_set.set_id) == (4, 4)
        assert take_seq(conn, vector_set.set_id, 1, counter="vectors") == 5
        conn.rollback()


def test_migrate_records_a_watermark_for_every_branch(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.db.migrate import _ensure_vector_sets

    deployment = _deployment(tmp_path, clean_runtime)
    _seed(clean_runtime)
    _write(clean_runtime, _issue())
    default_generation = _publish(clean_runtime, deployment)["generation_id"]
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        name = store.get_generation(default_generation).metadata["deployment_name"]
        store.create_branch("experiment", from_branch="default")
        branch_generation = store.checkpoint_generation(
            "wm-experiment", branch="experiment", deployment_name=name,
        )
    finally:
        store.close()

    (report,) = _ensure_vector_sets(clean_runtime, deployment)
    assert sorted(report["watermark_generations"]) == sorted(
        [default_generation, branch_generation]
    )
    vector_set = _vector_set()
    for generation in (default_generation, branch_generation):
        assert _watermark(clean_runtime, generation, vector_set) == (0, 0, "migrate")
        assert _at(clean_runtime, vector_set, generation)["has_vector"] is False


def test_the_runtime_roles_write_publish_and_read_a_vector_set(
    clean_runtime: str, tmp_path,
) -> None:
    """The grants end to end, with the real code under the real roles.

    The publisher and the backfill connect as the `postgres.dsn` user, which
    must own the substrate's tables (docs/reference/security.md: as `app_rw`
    the publisher stops at its readiness check, and `app_rw` holds no
    Chronos table). `app_rw` is the MCP write role: as `app_rw`, the
    vector-set writer API links documents and inserts vectors (counter row
    UPDATE, inserts) inside one transaction. Then, as `okg_mcp`, a semantic
    read of the set (registry rows, watermark, index state)."""
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.embeddings.backfill import run_set_backfill
    from okg.substrate.embeddings.store import write_node_documents
    from okg.substrate.mcp import open_session, search

    block = _with_header([], _search_block())
    deployment = _deployment(tmp_path, clean_runtime, block)
    _seed(clean_runtime, subtypes=("jira_issue", "note"))
    (report,) = _ensure_vector_sets(clean_runtime, deployment)
    assert report["index"]["status"] == "present"
    config = parse_embedding_config(plural=block)
    vector_set = config.search_vector_set
    _write(clean_runtime, _issue("jira:1"), _note("note:1", "a note body"))
    _publish(clean_runtime, deployment)

    # As app_rw: a new row's link, and a vector for its document.
    with psycopg.connect(clean_runtime) as conn:
        conn.execute("SET ROLE app_rw")
        assert conn.execute("SELECT current_user").fetchone()[0] == "app_rw"
        row = {"node_id": "note:2", "subtype": "note",
               "attrs": {"title": "t", "text": "another note body"}}
        assert write_node_documents(conn, [row]) == 1
        doc = conn.execute(
            "SELECT doc_hash FROM okg.node_documents WHERE node_id = 'note:2'"
        ).fetchone()[0]
        assert write_vectors(conn, vector_set.set_id, [
            (doc, HashEmbedder().embed("another note body")),
        ]) == 1
        conn.commit()
        conn.execute("RESET ROLE")

    ran = run_set_backfill(
        clean_runtime, deployment=deployment, config=config, set_name="main",
        max_usd=None,
    )
    assert ran["documents_embedded"] == 2 and ran["links_written"] == 0
    second = _publish(clean_runtime, deployment)
    assert not second.get("reused_generation")

    plan = _plan(block)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        conn.execute("SET ROLE okg_mcp")
        try:
            page = search(conn, session, "a note body", method="semantic",
                          embedder=plan, limit=5)
        finally:
            conn.execute("RESET ROLE")
    assert page["vector_source"] == "vector_set"
    assert page["vector_set"]["index"]["status"] == "present"
    assert page["results"][0]["node_id"] == "note:1"


def test_a_pin_without_a_watermark_refuses(clean_runtime: str, tmp_path) -> None:
    deployment = _deployment(tmp_path, clean_runtime)
    vector_set = _vector_set()
    _seed(clean_runtime)
    _write(clean_runtime, _issue())
    before = _publish(clean_runtime, deployment)["generation_id"]
    _register(clean_runtime, vector_set)  # after the publish, without migrate
    with pytest.raises(SubstrateError) as caught:
        _at(clean_runtime, vector_set, before)
    assert caught.value.code == "embedding_watermark_missing"


@pytest.mark.parametrize("counter", ["vectors", "links"])
def test_interleaved_writers_take_seq_in_commit_order(
    clean_runtime: str, counter: str,
) -> None:
    """Two writers of one set's counter: the second waits on the counter row
    until the first commits, so a watermark read between them never sits
    above an uncommitted number. Links and vectors each have a counter."""
    vector_set = _vector_set()
    _register(clean_runtime, vector_set)
    width = vector_set.width
    first = psycopg.connect(clean_runtime)
    second = psycopg.connect(clean_runtime)
    reader = psycopg.connect(clean_runtime, autocommit=True)
    table = "text_embeddings" if counter == "vectors" else "node_documents"

    def mark(generation_id: str) -> int:
        return getattr(watermark_for(reader, generation_id, vector_set.set_id), counter)

    try:
        a_seq = take_seq(first, vector_set.set_id, 1, counter=counter)
        if counter == "vectors":
            first.execute(
                "INSERT INTO okg.text_embeddings (set_id, doc_hash, embedding, seq) "
                "VALUES (%s, %s, CAST(%s AS halfvec), %s)",
                (vector_set.set_id, "a" * 64, "[" + ",".join(["0.1"] * width) + "]",
                 a_seq),
            )
        else:
            first.execute(
                "INSERT INTO okg.node_documents "
                "(set_id, template_hash, node_id, row_hash, doc_hash, seq) "
                "VALUES (%s, %s, 'n:1', %s, %s, %s)",
                (vector_set.set_id, "b" * 64, "c" * 64, "a" * 64, a_seq),
            )
        got: dict[str, int] = {}
        second_pid = second.info.backend_pid

        def run_second() -> None:
            got["seq"] = take_seq(second, vector_set.set_id, 1, counter=counter)

        thread = threading.Thread(target=run_second)
        thread.start()
        deadline = time.monotonic() + 10
        waiting = False
        while time.monotonic() < deadline and not waiting:
            waiting = bool(reader.execute(
                "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = %s",
                (second_pid,),
            ).fetchone()[0])
            time.sleep(0.05)
        assert waiting, "the second writer must wait on the set's counter row"
        # A watermark read now sees nothing uncommitted.
        assert record_watermarks(reader, "gen-before", recorded_by="publisher") == 1
        assert mark("gen-before") == a_seq - 1
        first.commit()
        thread.join(10)
        assert got["seq"] == a_seq + 1
        record_watermarks(reader, "gen-between", recorded_by="publisher")
        assert mark("gen-between") == a_seq
        assert seq_advanced_since(reader, "gen-between") == 0
        second.commit()
        assert seq_advanced_since(reader, "gen-between") == 1
        # Every committed row at or below a watermark committed before it.
        rows = reader.execute(
            f"SELECT seq FROM okg.{table} WHERE set_id = %s ORDER BY seq",
            (vector_set.set_id,),
        ).fetchall()
        assert [r[0] for r in rows] == [a_seq]
    finally:
        first.close()
        second.close()
        reader.close()


def test_must_fail_a_graph_write_does_not_wait_for_an_open_vector_batch(
    clean_runtime: str, tmp_path,
) -> None:
    """A backfill holds its vector batch open through the HNSW inserts
    (about 10 ms a vector on the C4 copy). A graph write in that window
    writes its links without waiting: links take their own counter. With
    one shared counter row the write waits for the batch's commit, and the
    1 s lock_timeout below fails it. A watermark taken while the batch is
    open does not cover its vectors; one taken after its commit does."""
    deployment = _deployment(tmp_path, clean_runtime)
    vector_set = _vector_set()
    _seed(clean_runtime)
    _register(clean_runtime, vector_set)
    _write(clean_runtime, _issue("jira:1"))
    published = _publish(clean_runtime, deployment)["generation_id"]
    batch = psycopg.connect(clean_runtime)
    writer = psycopg.connect(clean_runtime, autocommit=True, options="-c lock_timeout=1000")
    reader = psycopg.connect(clean_runtime, autocommit=True)
    try:
        before = watermark_for(reader, published, vector_set.set_id)
        assert write_vectors(batch, vector_set.set_id, [
            (f"{i:064x}", [0.5] * vector_set.width) for i in range(3)
        ]) == 3  # open: not committed
        started = time.monotonic()
        ChronosGraphStore.from_connection(writer).apply_facts(
            [_issue("jira:2", text="written while the batch is open")],
            source=SOURCE, strict_endpoints=True,
        )
        assert time.monotonic() - started < 1.0
        assert [doc for _row, doc in _links(clean_runtime, "jira:2")], "jira:2 was linked"
        assert record_watermarks(reader, "gen-open", recorded_by="publisher") == 1
        open_mark = watermark_for(reader, "gen-open", vector_set.set_id)
        assert open_mark.vectors == before.vectors
        assert open_mark.links > before.links
        batch.commit()
        record_watermarks(reader, "gen-after", recorded_by="publisher")
        assert watermark_for(reader, "gen-after", vector_set.set_id).vectors == (
            before.vectors + 3
        )
    finally:
        batch.close()
        writer.close()
        reader.close()


def test_must_fail_a_plain_sequence_lets_a_later_commit_slip_under_the_watermark(
    clean_runtime: str,
) -> None:
    """Why the counter is a locked row: with nextval(), the first writer's
    number commits AFTER a watermark that already covers it."""
    first = psycopg.connect(clean_runtime)
    second = psycopg.connect(clean_runtime)
    reader = psycopg.connect(clean_runtime, autocommit=True)
    try:
        reader.execute("CREATE TABLE okg_seq_race (seq BIGINT)")
        reader.execute("CREATE SEQUENCE okg_seq_race_seq")
        first.execute("INSERT INTO okg_seq_race VALUES (nextval('okg_seq_race_seq'))")
        second.execute("INSERT INTO okg_seq_race VALUES (nextval('okg_seq_race_seq'))")
        second.commit()
        watermark = reader.execute("SELECT max(seq) FROM okg_seq_race").fetchone()[0]
        visible_then = reader.execute(
            "SELECT count(*) FROM okg_seq_race WHERE seq <= %s", (watermark,),
        ).fetchone()[0]
        first.commit()
        visible_now = reader.execute(
            "SELECT count(*) FROM okg_seq_race WHERE seq <= %s", (watermark,),
        ).fetchone()[0]
        assert (watermark, visible_then, visible_now) == (2, 1, 2)
    finally:
        first.close()
        second.close()
        reader.close()


def test_must_fail_a_missing_template_table_refuses_instead_of_skipping(
    clean_runtime: str,
) -> None:
    """A database migrated by an earlier build has the vector tables but not
    okg.embedding_templates. The writer and the reader refuse with a reason;
    before, both returned quietly, so links were written that a template
    change could never serve again."""
    from okg.substrate.embeddings.store import load_template_records, record_templates

    vector_set = _vector_set()
    _seed(clean_runtime)
    _register(clean_runtime, vector_set)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DROP TABLE okg.embedding_templates")
        for call in (
            lambda: record_templates(conn, vector_set.set_id, {"a" * 64: {"subtype": "note"}}),
            lambda: load_template_records(conn, vector_set.set_id, ["note"]),
        ):
            with pytest.raises(SubstrateError) as caught:
                call()
            assert caught.value.code == "embedding_schema_outdated"
            assert caught.value.data == {"table": "okg.embedding_templates"}
        # A graph write with an active set refuses too, and writes nothing.
        with pytest.raises(SubstrateError) as caught:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [_issue("jira:9")], source=SOURCE, strict_endpoints=True,
            )
        assert caught.value.code == "embedding_schema_outdated"
        assert conn.execute("SELECT count(*) FROM okg.node_documents").fetchone()[0] == 0


def test_vectors_must_have_the_sets_width(clean_runtime: str) -> None:
    vector_set = _vector_set()
    _register(clean_runtime, vector_set)
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(SubstrateError) as caught:
            write_vectors(conn, vector_set.set_id, [("b" * 64, [0.1] * 768)])
    assert caught.value.code == "embedding_vector_width_mismatch"


# --------------------------------------------------------------------------- #
# Semantic search over a vector set (the read path)                           #
# --------------------------------------------------------------------------- #

QUERY = "where does the nightly build break"


def _search_block(*, kind: str = "hnsw", search: dict | None = None) -> dict:
    index: dict = {"kind": kind}
    if search is not None:
        index["search"] = search
    return {
        "enabled": True,
        "sets": {
            "main": {
                "backend": "hash",
                "subtypes": ["jira_issue", "note"],
                "document": {"default": {"header": ["subtype", "title"], "body": "text"}},
                "index": index,
            }
        },
    }


def _plan(block: dict):
    from okg.substrate.embeddings.config import build_embedding_execution_plan

    return build_embedding_execution_plan(parse_embedding_config(plural=block))


def _unit(values: list[float]) -> list[float]:
    norm = sum(v * v for v in values) ** 0.5
    return [v / norm for v in values]


def _toward(query_vector: list[float], node_id: str, sign: float) -> list[float]:
    """A vector near the query (sign 1) or near its opposite (sign -1), made
    distinct per node by a little hashed noise."""
    from okg.substrate.embeddings import HashEmbedder

    noise = HashEmbedder().embed(f"noise:{node_id}")
    return _unit([sign * q + 0.3 * n for q, n in zip(query_vector, noise)])


def _note(node_id: str, text: str) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype="note",
        attrs={"title": f"note {node_id}", "text": text},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _store_vectors_for(
    dsn: str, vector_set, vectors: dict[str, list[float]], *, complete: bool = True,
) -> None:
    """Store a vector for the document each named node links to now and,
    with ``complete``, record the fill as complete (every node has one), as
    a completed backfill does; search routes to the set only after that."""
    from okg.substrate.embeddings.store import record_fill_completion

    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT DISTINCT ON (node_id) node_id, doc_hash FROM okg.node_documents "
            "WHERE set_id = %s AND node_id = ANY(%s) ORDER BY node_id, seq DESC",
            (vector_set.set_id, list(vectors)),
        ).fetchall()
        assert len(rows) == len(vectors), rows
        write_vectors(conn, vector_set.set_id, [
            (str(doc), vectors[str(node_id)]) for node_id, doc in rows
        ])
        if complete:
            record_fill_completion(conn, vector_set.set_id, "fixture")
        conn.commit()


def _semantic_fixture(tmp_path, dsn: str, block: dict, *, issues: int, notes: int):
    """A published graph whose set holds `issues` issue vectors near the query
    and `notes` note vectors far from it. Returns (deployment, vector set,
    plan)."""
    from okg.substrate.embeddings.store import ensure_set_index

    deployment = _deployment(tmp_path, dsn, block)
    vector_set = _vector_set(block)
    plan = _plan(block)
    _seed(dsn, subtypes=("jira_issue", "note"))
    _register(dsn, vector_set)
    ensure_set_index(dsn, vector_set)
    nodes = [_issue(f"jira:{i}", text=f"issue body {i}") for i in range(issues)]
    nodes += [_note(f"note:{i}", f"note body {i}") for i in range(notes)]
    _write(dsn, *nodes)
    q = list(plan.embed(QUERY))
    vectors = {f"jira:{i}": _toward(q, f"jira:{i}", 1.0) for i in range(issues)}
    vectors.update({f"note:{i}": _toward(q, f"note:{i}", -1.0) for i in range(notes)})
    _store_vectors_for(dsn, vector_set, vectors)
    _publish(dsn, deployment)
    return deployment, vector_set, plan


def _semantic(dsn: str, deployment: str, plan, **kwargs) -> dict:
    from okg.substrate.mcp import open_session, search

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, QUERY, method="semantic", embedder=plan, **kwargs)


def test_semantic_search_ranks_the_registered_set_and_says_so(
    clean_runtime: str, tmp_path,
) -> None:
    block = _search_block()
    deployment, vector_set, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=3, notes=2,
    )
    page = _semantic(clean_runtime, deployment, plan, limit=10)
    ids = [row["node_id"] for row in page["results"]]
    assert sorted(ids[:3]) == ["jira:0", "jira:1", "jira:2"]
    assert sorted(ids[3:]) == ["note:0", "note:1"]
    # Every vector of the set is of its registered model: never "unknown",
    # which means "admitted under the unverified-provenance opt-in".
    assert {row["embedding_provenance"] for row in page["results"]} == {
        vector_set.model_id
    }
    assert page["vector_source"] == "vector_set"
    assert page["vector_set"]["set_id"] == vector_set.set_id
    assert page["vector_set"]["status"] == "active"
    assert page["vector_set"]["config_drift"] == []
    assert page["vector_set"]["index"]["status"] == "present"
    assert page["ranking"] == "exact"
    assert page["embedding_coverage"]["text_nodes"] == 5
    assert page["embedding_coverage"]["embedded_nodes"] == 5
    assert page["truncated"] is False and page["total_count_basis"] == "exact"
    assert "rank_capped" not in page and "listing" not in page


def test_must_fail_search_coverage_does_not_count_blank_text_as_missing(
    clean_runtime: str, tmp_path,
) -> None:
    """Lane EH: the search page's embedding_coverage counts nodes the fill
    embeds. A node with empty or blank text gets no document, so a fully
    filled set reports as many text nodes as embedded ones. Must-fail: the
    scope counted every node with a `text` attribute (one above embedded
    per blank node on the C4S copy)."""
    deployment, _vector_set, plan = _semantic_fixture(
        tmp_path, clean_runtime, _search_block(), issues=2, notes=1,
    )
    _write(clean_runtime, _issue("jira:blank", text=""), _note("note:blank", " 　\n"))
    _publish(clean_runtime, deployment)
    page = _semantic(clean_runtime, deployment, plan, limit=10)
    assert page["vector_source"] == "vector_set"
    assert page["embedding_coverage"]["text_nodes"] == 3, page["embedding_coverage"]
    assert page["embedding_coverage"]["embedded_nodes"] == 3
    assert {row["node_id"] for row in page["results"]} == {
        "jira:0", "jira:1", "note:0",
    }


def _plan_nodes(plan: dict):
    yield plan
    for child in plan.get("Plans", ()):
        yield from _plan_nodes(child)


def test_must_fail_a_filled_pin_runs_no_vector_count(
    clean_runtime: str, tmp_path,
) -> None:
    """DIAG-SMALL-EXACT fix 1: the route check of a pin whose watermark
    carries a completed fill does not count the set's vectors (the count is
    used only for a "filling" page, and its bitmap scan cannot stop at its
    LIMIT: about 80 ms per search on the C4 copy). Must-fail: the count
    subplan ran on every search."""
    from okg.substrate.mcp.search import _set_fill_at_pin

    block = _search_block()
    deployment, vector_set, _plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=3, notes=2,
    )
    seen: list[tuple[str, tuple]] = []

    class Recording(psycopg.Cursor):
        def execute(self, query, params=None, **kwargs):
            seen.append((str(query), tuple(params or ())))
            return super().execute(query, params, **kwargs)

    from okg.substrate.mcp import open_session

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation_id = str(open_session(conn, deployment_name=deployment).generation_id)
    with psycopg.connect(
        clean_runtime, autocommit=True, cursor_factory=Recording,
    ) as conn:
        state = _set_fill_at_pin(conn, vector_set.set_id, generation_id)
    assert state is not None and state[0] is True
    (query, params) = [s for s in seen if "embedding_watermarks" in s[0]][-1]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        (plan,) = conn.execute(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + query, params,
        ).fetchone()[0]
    subplans = [
        node for node in _plan_nodes(plan["Plan"])
        if node.get("Parent Relationship") in ("SubPlan", "InitPlan")
    ]
    assert subplans, plan
    assert all(node["Actual Loops"] == 0 for node in subplans), plan


def test_must_fail_the_exact_join_hashes_each_row_once(
    clean_runtime: str, tmp_path,
) -> None:
    """DIAG-SMALL-EXACT fix 2: the exact statement computes the row hash
    (sha256 of `attrs::text`) once, in the node scan's output, and the link
    join and newer-link check compare against that value. Must-fail: the
    planner flattened the scope subquery and hashed every row again inside
    each lookup (the scan output carried no hash). The ranked rows are
    unchanged (the exact-rank tests in this file)."""
    from okg.substrate.mcp import open_session
    from okg.substrate.semantic_ann import _exact_join, prepare_set_read

    block = _search_block()
    deployment, vector_set, _plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=3, notes=2,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        read = prepare_set_read(conn, session, vector_set)
        body, params = _exact_join(read)
        (plan,) = conn.execute(
            "EXPLAIN (VERBOSE, FORMAT JSON) SELECT v.node_id" + body, params,
        ).fetchone()[0]
    relation = read.physical_table.split(".")[-1].strip('"')
    scans = [
        node for node in _plan_nodes(plan["Plan"])
        if node.get("Relation Name") == relation
    ]
    assert scans, plan
    assert any(
        any("sha256(" in str(item) for item in node.get("Output", ()))
        for node in scans
    ), scans


class _RecordingConnection:
    """A psycopg connection whose cursors record each statement they run."""

    def __init__(self, conn) -> None:
        self._conn = conn
        self.statements: list[tuple[str, dict]] = []

    def cursor(self, *args, **kwargs):
        recorder = self

        class _Cursor:
            def __init__(self, cur) -> None:
                self._cur = cur

            def __enter__(self):
                self._cur.__enter__()
                return self

            def __exit__(self, *exc):
                return self._cur.__exit__(*exc)

            def execute(self, statement, params=None, **kw):
                recorder.statements.append((statement, params))
                return self._cur.execute(statement, params, **kw)

            def __getattr__(self, name):
                return getattr(self._cur, name)

        return _Cursor(self._conn.cursor(*args, **kwargs))

    def __getattr__(self, name):
        return getattr(self._conn, name)


_TEXT_EXTRACTION = re.compile(r"attrs ->>? 'text'")


def _text_extractions(plan: dict) -> tuple[int, int]:
    """(extractions of `attrs -> 'text'` in the table scans' outputs, those
    in any filter or join condition) of an EXPLAIN (VERBOSE, FORMAT JSON)
    plan. An upper node's output repeats its child's expression, so only
    a table scan counts as computing it."""
    computed = tested = 0
    for node in _plan_nodes(plan["Plan"]):
        if "Relation Name" in node:
            computed += len(_TEXT_EXTRACTION.findall(str(node.get("Output", ()))))
        for key in ("Filter", "Join Filter", "Index Cond", "Hash Cond", "Recheck Cond"):
            tested += len(_TEXT_EXTRACTION.findall(str(node.get(key, ""))))
    return computed, tested


def test_must_fail_the_non_blank_test_reads_the_text_once(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Lane EH C4 measurement (MEASURE-C4.md, fix variant c): every
    statement with the non-blank test reads `attrs -> 'text'` once, in the
    node scan's output, and tests that value one level up, so `attrs` is
    not read again per test. Must-fail: the filter extracted the text twice
    (`jsonb_typeof(attrs -> 'text')` and `attrs ->> 'text' ~ ...`), twice
    the buffers on the C4 copy. Covers the three doctor coverage counts and
    the read's scope count, exact join and walked-candidate check."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.embeddings.doctor import vector_set_report
    from okg.substrate.mcp import open_session

    block = _search_block()
    deployment, vector_set, _plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=3, notes=2,
    )
    config = parse_embedding_config(plural=block)
    recorded: list[tuple[str, dict]] = []

    def recording(real):
        def call(conn, *args, **kwargs):
            proxy = _RecordingConnection(conn)
            try:
                return real(proxy, *args, **kwargs)
            finally:
                recorded.extend(proxy.statements)
        return call

    for name in ("_coverage_text_nodes", "_coverage_exact", "_coverage_sample"):
        monkeypatch.setattr(semantic_ann, name, recording(getattr(semantic_ann, name)))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        exact = vector_set_report(conn, deployment=deployment, config=config)
        monkeypatch.setattr(semantic_ann, "COVERAGE_EXACT_MAX", 1)
        sampled = vector_set_report(conn, deployment=deployment, config=config)
        assert exact["sets"][0]["coverage"]["method"] == "exact", exact
        assert sampled["sets"][0]["coverage"]["method"] == "sample", sampled
        # Plain doctor counts text nodes first, then counts exactly or samples.
        assert len(recorded) == 4, [s for s, _ in recorded]

        session = open_session(conn, deployment_name=deployment)
        read = semantic_ann.prepare_set_read(conn, session, vector_set)
        proxy = _RecordingConnection(conn)
        semantic_ann._bounded_count(proxy, read, bound=10, with_vectors=False)
        recorded.extend(proxy.statements)
        body, params = semantic_ann._exact_join(read)
        recorded.append(("SELECT v.node_id" + body, params))
        statement, params = semantic_ann._scope_check_statement(read)
        recorded.append((statement, {**params, "docs": ["d"], "dists": [0.5]}))

        for statement, params in recorded:
            (plan,) = conn.execute(
                "EXPLAIN (VERBOSE, FORMAT JSON) " + statement, params,
            ).fetchone()[0]
            assert _text_extractions(plan) == (1, 0), (statement, plan)


def test_the_route_needs_a_registered_active_set_and_a_plan(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.embeddings.store import deactivate_other_sets
    from okg.substrate.mcp.search import _vector_set_route

    block = _search_block()
    plan = _plan(block)
    vector_set = _vector_set(block)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _vector_set_route(conn, embedder=HashEmbedder(), embedder_spec=None) is None
        assert _vector_set_route(conn, embedder=plan, embedder_spec=None).status == (
            "unregistered"
        )
    _register(clean_runtime, vector_set)
    with psycopg.connect(clean_runtime) as conn:
        assert _vector_set_route(conn, embedder=plan, embedder_spec=None).status == "active"
        # The spec form names the backend's default set. Its identity leaves
        # out subtypes and templates, so `hash` is this very set; another
        # backend is not registered.
        assert _vector_set_route(conn, embedder=None, embedder_spec="hash").status == (
            "active"
        )
        other = _vector_set_route(
            conn, embedder=None,
            embedder_spec="openrouter:google/gemini-embedding-2@768",
        )
        assert other.status == "unregistered"
        assert other.describe(withhold_counts=False) == {
            "name": "main", "set_id": other.configured.set_id, "status": "unregistered",
        }
        deactivate_other_sets(conn, [])
        conn.commit()
        assert _vector_set_route(conn, embedder=plan, embedder_spec=None).status == (
            "inactive"
        )


def test_migrate_before_any_backfill_keeps_the_node_column_search(
    clean_runtime: str, tmp_path,
) -> None:
    """Must-fail for review item B1: migrate registers and activates the set
    of the old single-backend form before anything fills it. Routing to that
    empty set turned a working semantic search into empty pages. The read
    now keeps the node columns until the set holds a vector at the pin, and
    says so; after a backfill and a publish the set serves it."""
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.embeddings.backfill import run_set_backfill
    from okg.substrate.embeddings.config import build_embedding_execution_plan
    from tests.substrate.mcp.semantic_fixture import node, publish_with_vectors

    block = {"enabled": True, "backend": "hash"}
    deployment = _deployment(tmp_path, clean_runtime, block)
    texts = {"chunk:a": "background estimation", "chunk:b": "trigger efficiency"}
    hashed = HashEmbedder()
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"],
        nodes=[node(nid, "chunk", text) for nid, text in texts.items()],
        vectors={nid: hashed.embed(text) for nid, text in texts.items()},
        model_id=hashed.model_id,
    )
    (report,) = _ensure_vector_sets(clean_runtime, deployment)
    assert report["created"] is True
    config = parse_embedding_config(plural=block)
    plan = build_embedding_execution_plan(config)

    def query(text: str) -> dict:
        from okg.substrate.mcp import open_session, search

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment)
            return search(conn, session, text, method="semantic", embedder=plan, limit=5)

    page = query("trigger efficiency")
    assert page["results"], "the empty set must not empty the page"
    assert page["results"][0]["node_id"] == "chunk:b"
    assert page["vector_source"] == "node_columns"
    assert page["vector_set"]["status"] == "no_vectors"

    run_set_backfill(
        clean_runtime, deployment=deployment, config=config, set_name=None,
        max_usd=None,
    )
    _publish(clean_runtime, deployment)
    served = query("trigger efficiency")
    assert served["vector_source"] == "vector_set"
    assert served["vector_set"]["status"] == "active"
    # The documents carry a `Type:` header, so the hash vectors differ from
    # the query's: only membership is asserted here.
    assert sorted(r["node_id"] for r in served["results"]) == ["chunk:a", "chunk:b"]


def test_a_pin_without_a_watermark_refuses_before_the_query_is_embedded(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    from okg.substrate.embeddings.config import EmbeddingExecutionPlan

    block = _search_block()
    deployment = _deployment(tmp_path, clean_runtime, block)
    _seed(clean_runtime, subtypes=("jira_issue", "note"))
    _write(clean_runtime, _issue())
    _publish(clean_runtime, deployment)
    _register(clean_runtime, _vector_set(block))  # after the publish, no migrate
    plan = _plan(block)

    def no_embedding(self, text):
        raise AssertionError("the query was embedded before the refusal")

    monkeypatch.setattr(EmbeddingExecutionPlan, "embed", no_embedding)
    with pytest.raises(SubstrateError) as caught:
        _semantic(clean_runtime, deployment, plan, limit=5)
    assert caught.value.code == "embedding_watermark_missing"


def test_a_vector_for_changed_content_is_never_served(
    clean_runtime: str, tmp_path,
) -> None:
    block = _search_block()
    deployment, _vector_set_, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=1, notes=0,
    )
    assert [r["node_id"] for r in _semantic(clean_runtime, deployment, plan)["results"]] == [
        "jira:0"
    ]
    # New body text: a new document with no vector yet. The old vector is
    # still stored and still linked to the old row content.
    _write(clean_runtime, _issue("jira:0", text="a rewritten body"))
    _publish(clean_runtime, deployment)
    page = _semantic(clean_runtime, deployment, plan)
    assert page["results"] == []
    assert page["embedding_coverage"]["text_nodes"] == 1
    assert page["embedding_coverage"]["embedded_nodes"] == 0


@pytest.mark.parametrize("exact_max, ranking", [(5_000, "exact"), (2, "approximate")])
def test_must_fail_a_narrow_scope_inside_a_large_set_fills_its_page(
    clean_runtime: str, tmp_path, monkeypatch, exact_max: int, ranking: str,
) -> None:
    """Prefilter. 40 issue vectors sit near the query and 6 note vectors far
    from it. A note-only search must return 5 notes. Filtering after taking
    the nearest `limit` candidates (the earlier read) returns none: the test
    shows that set below. `ef_search: 1` makes the approximate walk's first
    index search return one candidate, so only the iterative scan reaches the
    notes. EXACT_SCOPE_MAX is lowered to 2 for the approximate case."""
    import okg.substrate.semantic_ann as semantic_ann

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", exact_max)
    block = _search_block(search={"ef_search": 1})
    deployment, vector_set, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=40, notes=6,
    )
    page = _semantic(clean_runtime, deployment, plan, limit=5, filter={"subtype": "note"})
    ids = [row["node_id"] for row in page["results"]]
    assert page["ranking"] == ranking
    assert len(ids) == 5 and all(i.startswith("note:") for i in ids), ids
    assert page["truncated"] is True and page["next_offset"] == 5
    if ranking == "approximate":
        assert page["embedding_coverage"]["embedded_nodes"] is None
    # The post-filter the prefilter replaced: the nearest `limit` vectors of
    # the set, then the scope. It finds no note at all.
    q = list(plan.embed(QUERY))
    literal = "[" + ",".join(f"{v:.7g}" for v in q) + "]"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        nearest = conn.execute(
            "SELECT d.node_id FROM okg.text_embeddings e "
            "JOIN okg.node_documents d USING (set_id, doc_hash) "
            "WHERE e.set_id = %s "
            "ORDER BY CAST(e.embedding AS halfvec(2560)) <=> CAST(%s AS halfvec(2560)) "
            "LIMIT 5",
            (vector_set.set_id, literal),
        ).fetchall()
    assert [str(r[0]) for r in nearest if str(r[0]).startswith("note:")] == []


def test_an_empty_scope_returns_its_empty_page_without_embedding_the_query(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """No node in scope has a vector at the pin: the page is empty whatever
    the query, so the query (a paid call with a paid backend) is not
    embedded."""
    from okg.substrate.embeddings.config import EmbeddingExecutionPlan

    block = _search_block()
    deployment, _vs, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=2, notes=0,
    )
    _write(clean_runtime, _note("note:0", "a note with no vector yet"))
    _publish(clean_runtime, deployment)

    def no_embedding(self, text):
        raise AssertionError("the query was embedded for an empty scope")

    monkeypatch.setattr(EmbeddingExecutionPlan, "embed", no_embedding)
    page = _semantic(clean_runtime, deployment, plan, limit=5, filter={"subtype": "note"})
    assert page["results"] == [] and page["vector_source"] == "vector_set"
    assert page["embedding_coverage"]["text_nodes"] == 1
    assert page["embedding_coverage"]["embedded_nodes"] == 0


def test_must_fail_a_large_scope_with_few_vectors_ranks_approximately(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Exact ranking reads every row of the scope, with or without a vector,
    so the choice follows the scope's visible text nodes. Five issues, one
    with a vector, bound 3: the walk. The earlier rule counted vectors (1)
    and ranked exactly, which on the C4 copy cost about 21 s for the whole
    graph. Also checks the counting stays bounded: an approximate scope runs
    only the text count, to bound + 1, and no eligible count."""
    import okg.substrate.semantic_ann as semantic_ann

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", 3)
    block = _search_block()
    deployment, _vs, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=1, notes=0,
    )
    _write(clean_runtime, *[_issue(f"jira:{i}", text=f"no vector {i}") for i in range(1, 5)])
    _publish(clean_runtime, deployment)
    counts: list[tuple[int, bool, int]] = []
    real_count = semantic_ann._bounded_count

    def spy(conn, read, *, bound, with_vectors):
        got = real_count(conn, read, bound=bound, with_vectors=with_vectors)
        counts.append((bound, with_vectors, got))
        return got

    monkeypatch.setattr(semantic_ann, "_bounded_count", spy)
    page = _semantic(clean_runtime, deployment, plan, limit=5)
    assert page["ranking"] == "approximate"
    assert [row["node_id"] for row in page["results"]] == ["jira:0"]
    assert counts == [(3, False, 4)]
    assert page["embedding_coverage"]["text_nodes"] is None
    assert page["embedding_coverage"]["embedded_nodes"] is None
    # A scope at the bound ranks exactly and counts its eligible nodes.
    counts.clear()
    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", 5)
    page = _semantic(clean_runtime, deployment, plan, limit=5)
    assert page["ranking"] == "exact"
    assert counts == [(5, False, 5), (5, True, 1)]
    assert page["embedding_coverage"]["embedded_nodes"] == 1


def test_an_approximate_scope_without_vectors_does_not_embed_the_query(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Above the exact bound the eligible nodes are not counted; whether any
    link of the scope's templates has a vector still decides that the page
    is empty, so the query is not embedded."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.embeddings.config import EmbeddingExecutionPlan

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", 2)
    block = _search_block()
    deployment, _vs, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=2, notes=0,
    )
    _write(clean_runtime, *[_note(f"note:{i}", f"note {i}, no vector") for i in range(4)])
    _publish(clean_runtime, deployment)

    def no_embedding(self, text):
        raise AssertionError("the query was embedded for a scope with no vector")

    monkeypatch.setattr(EmbeddingExecutionPlan, "embed", no_embedding)
    page = _semantic(clean_runtime, deployment, plan, limit=5, filter={"subtype": "note"})
    assert page["ranking"] == "approximate"
    assert page["results"] == []
    assert page["embedding_coverage"]["embedded_nodes"] == 0


def test_a_source_filter_is_refused_before_the_set_read(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Review item 9 (source filtered after ranking) cannot happen:
    `filter.source` is refused for method="semantic" before any read or
    query embedding. Only a profile's source families narrow a semantic
    read, and those go into the set read's scope before ranking."""
    from okg.substrate.embeddings.config import EmbeddingExecutionPlan

    block = _search_block()
    deployment, _vs, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=1, notes=0,
    )

    def no_embedding(self, text):
        raise AssertionError("the query was embedded before the refusal")

    monkeypatch.setattr(EmbeddingExecutionPlan, "embed", no_embedding)
    with pytest.raises(SubstrateError, match="filter\\['source'\\] is not supported"):
        _semantic(clean_runtime, deployment, plan, limit=3, filter={"source": SOURCE})


def test_a_walk_stopped_at_its_tuple_bound_says_so(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """pgvector documents `hnsw.max_scan_tuples` as approximate: the walk
    still returns every vector its first scan visited. So the page is made
    short by construction, not by trusting the bound: 4 notes for a limit
    of 5. The notes lie far from the query, behind 200 issues, so a walk
    bounded at one tuple visits fewer vectors than the set stores and must
    say it stopped at its bound. (With 6 notes and 40 issues the first scan
    sometimes reached 5 notes and filled the page: 1 run in 60 over rebuilt
    indexes on a lab host, and once in the nightly.)"""
    import okg.substrate.semantic_ann as semantic_ann

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", 2)
    block = _search_block(search={"ef_search": 1, "max_scan_tuples": 1})
    deployment, _vs, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=200, notes=4,
    )
    page = _semantic(clean_runtime, deployment, plan, limit=5, filter={"subtype": "note"})
    assert page["ranking"] == "approximate"
    assert {row["node_id"] for row in page["results"]} <= {f"note:{i}" for i in range(4)}
    assert page["listing"] == {"complete": False, "reason": "scan_bound"}
    assert page["truncated"] is True and page["next_offset"] is None

    # Failure case: the same read with a bound past the set's size (the
    # registration's search settings, updated in place) walks to the end,
    # finds all 4 notes and does not claim it stopped at a bound. Its
    # ef_search is 200, not 1: at 1 the index walk itself sometimes ends
    # before it has visited every vector (10 of 60 index builds on
    # a lab host), which is the walk ending early, not the bound.
    unbounded = _search_block(search={"ef_search": 200, "max_scan_tuples": 20_000})
    _register(clean_runtime, _vector_set(unbounded))
    page = _semantic(clean_runtime, deployment, _plan(unbounded), limit=5,
                     filter={"subtype": "note"})
    assert page["ranking"] == "approximate"
    assert sorted(row["node_id"] for row in page["results"]) == [f"note:{i}" for i in range(4)]
    assert page.get("listing") != {"complete": False, "reason": "scan_bound"}, page.get("listing")


def test_an_unindexed_set_ranks_exactly_and_refuses_a_large_scope(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    import okg.substrate.semantic_ann as semantic_ann

    block = _search_block(kind="none")
    deployment, _vs, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=3, notes=1,
    )
    page = _semantic(clean_runtime, deployment, plan, limit=10)
    assert page["ranking"] == "exact" and len(page["results"]) == 4
    assert page["vector_set"]["index"]["status"] == "not_indexed"
    monkeypatch.setattr(semantic_ann, "UNINDEXED_SCOPE_MAX", 3)
    with pytest.raises(SubstrateError) as caught:
        _semantic(clean_runtime, deployment, plan, limit=10)
    assert caught.value.code == "semantic_scope_too_large"


def test_a_missing_index_refuses(clean_runtime: str, tmp_path) -> None:
    block = _search_block()
    deployment = _deployment(tmp_path, clean_runtime, block)
    _seed(clean_runtime, subtypes=("jira_issue", "note"))
    vector_set = _vector_set(block)
    _register(clean_runtime, vector_set)  # no index built
    _write(clean_runtime, _issue())
    _store_vectors_for(clean_runtime, vector_set, {"jira:1": [0.5] * vector_set.width})
    _publish(clean_runtime, deployment)  # the set serves the read
    with pytest.raises(SubstrateError) as caught:
        _semantic(clean_runtime, deployment, _plan(block), limit=5)
    assert caught.value.code == "semantic_index_unavailable"
    assert caught.value.data["status"] == "missing"


def test_pgvector_without_iterative_scan_refuses(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """No silent fallback to filtering a fixed candidate list: an HNSW set on
    pgvector older than 0.8 refuses with the reason."""
    import okg.substrate.semantic_ann as semantic_ann

    block = _search_block()
    deployment, _vs, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=1, notes=0,
    )
    monkeypatch.setattr(semantic_ann, "_pgvector_version", lambda conn: "0.7.4")
    with pytest.raises(SubstrateError) as caught:
        _semantic(clean_runtime, deployment, plan, limit=5)
    assert caught.value.code == "semantic_index_unavailable"
    assert caught.value.data["status"] == "unsupported"
    assert "0.8" in str(caught.value) and "0.7.4" in str(caught.value)


# --------------------------------------------------------------------------- #
# Backfill, dry run, embed-show, CLI search (free hash backend)                #
# --------------------------------------------------------------------------- #

BF_BLOCK = {
    "enabled": True,
    "sets": {
        "main": {
            "backend": "hash",
            "subtypes": ["jira_issue", "note"],
            "document": {"default": {"header": ["subtype", "title"], "body": "text"}},
            "index": {"kind": "none"},
        }
    },
}
ISSUE_DOC = "Type: jira_issue\nTitle: Build breaks\n\nThe nightly build fails on main."
NOTE_DOC = "Type: note\nTitle: note note:1\n\na note body"


def _with_header(header: list[str], block: dict = BF_BLOCK) -> dict:
    changed = json.loads(json.dumps(block))
    changed["sets"]["main"]["document"]["default"]["header"] = header
    return changed


class _CountingHash:
    """The free hash backend, recording every text it embeds in a batch."""

    def __init__(self) -> None:
        from okg.substrate.embeddings import HashEmbedder

        self._inner = HashEmbedder()
        self.texts: list[str] = []

    dimension = property(lambda self: self._inner.dimension)
    model_id = property(lambda self: self._inner.model_id)

    def embed(self, text: str) -> list[float]:
        return self._inner.embed(text)

    def embed_batch(self, texts):
        self.texts.extend(texts)
        return self._inner.embed_batch(texts)


def _counting_builder():
    made: list[_CountingHash] = []

    def build(_spec):
        made.append(_CountingHash())
        return made[-1]

    return build, made


def _never_built(_spec):
    raise AssertionError("the backend was built although nothing needed a vector")


def _node_versions(dsn: str) -> tuple:
    """Every stored node row version: count and newest live_lo."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
        ).fetchone()[0]
        return tuple(conn.execute(
            f'SELECT count(*), max(live_lo) FROM "{physical}"'
        ).fetchone())


def _backfill_fixture(tmp_path, dsn: str, block: dict = BF_BLOCK):
    """Two issues with one document, a third issue and a note, published
    under a registered hash set. Returns (deployment, config, vector set)."""
    deployment = _deployment(tmp_path, dsn, block)
    config = parse_embedding_config(plural=block)
    vector_set = config.search_vector_set
    _seed(dsn, subtypes=("jira_issue", "note"))
    _register(dsn, vector_set)
    _write(
        dsn, _issue("jira:1"), _issue("jira:2"),
        _issue("jira:3", text="Another body."), _note("note:1", "a note body"),
    )
    _publish(dsn, deployment)
    return deployment, config, vector_set


def _backfill(dsn: str, deployment: str, config, builder, **kwargs) -> dict:
    from okg.substrate.embeddings.backfill import run_set_backfill

    return run_set_backfill(
        dsn, deployment=deployment, config=config, set_name="main",
        max_usd=None, builder=builder, **kwargs,
    )


def _set_rows(dsn: str, vector_set) -> tuple[int, int]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return tuple(conn.execute(
            "SELECT (SELECT count(*) FROM okg.text_embeddings WHERE set_id = %(s)s), "
            "(SELECT count(*) FROM okg.node_documents WHERE set_id = %(s)s)",
            {"s": vector_set.set_id},
        ).fetchone())


def test_must_fail_a_partial_backfill_does_not_route_search_to_the_set(
    clean_runtime: str, tmp_path,
) -> None:
    """Jason, lane chat 2026-10-14, "ehroute yes": search ranks a set only
    from a generation whose watermark carries a completed fill. A --limit
    run plus a publish leaves the page on the node columns, saying
    "filling" with the vectors at the pin; the completed run plus a publish
    moves it; the earlier pin keeps the columns. The earlier rule (any
    vector at the pin) routed to the thin set after the partial run. The
    node columns hold vectors here (the old single-backend form), so they
    can answer."""
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.embeddings.backfill import run_set_backfill
    from okg.substrate.embeddings.config import build_embedding_execution_plan
    from tests.substrate.mcp.semantic_fixture import node, publish_with_vectors

    block = {"enabled": True, "backend": "hash"}
    deployment = _deployment(tmp_path, clean_runtime, block)
    texts = {"chunk:a": "background estimation", "chunk:b": "trigger efficiency",
             "chunk:c": "luminosity calibration"}
    hashed = HashEmbedder()
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"],
        nodes=[node(nid, "chunk", text) for nid, text in texts.items()],
        vectors={nid: hashed.embed(text) for nid, text in texts.items()},
        model_id=hashed.model_id,
    )
    _ensure_vector_sets(clean_runtime, deployment)
    config = parse_embedding_config(plural=block)
    plan = build_embedding_execution_plan(config)

    def backfill(**kwargs) -> dict:
        return run_set_backfill(
            clean_runtime, deployment=deployment, config=config, set_name=None,
            max_usd=None, **kwargs,
        )

    def query(generation: str | None = None) -> dict:
        from okg.substrate.mcp import open_session, search

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(
                conn, deployment_name=deployment, requested_generation=generation,
            )
            return search(conn, session, "trigger efficiency", method="semantic",
                          embedder=plan, limit=5)

    partial = backfill(limit=1)
    assert partial["status"] == "limit_reached" and partial["fill_completed"] is False
    g_partial = _publish(clean_runtime, deployment)["generation_id"]
    page = query()
    assert page["vector_source"] == "node_columns"
    assert page["vector_set"]["status"] == "filling"
    assert page["vector_set"]["coverage"] == {"vectors_at_pin": 1, "count_bound": 10_000}
    assert len(page["results"]) == 3

    full = backfill()
    assert full["fill_completed"] is True and full["fill_completion_recorded"] is True
    _publish(clean_runtime, deployment)
    page = query()
    assert page["vector_source"] == "vector_set"
    assert page["vector_set"]["status"] == "active"
    assert query(g_partial)["vector_set"]["status"] == "filling"


def test_a_clone_keeps_the_paid_vectors_and_a_backfill_rebuilds_the_rest_for_free(
    clean_runtime: str, tmp_path,
) -> None:
    """Environment-clone dispositions of the vector-set tables: the sets,
    vectors and vector counter are preserved; links, template records and
    watermarks are purged. The purge is simulated in place (TRUNCATE, as the
    clone tests do) with the graph kept, so this checks the vector-set
    tables only: after a publish and a backfill that embeds nothing, the
    set serves every node from the preserved vectors."""
    from okg.substrate.environment_clone import clone_table_disposition

    tables = (
        "embedding_sets", "embedding_vector_counters", "text_embeddings",
        "node_documents", "embedding_templates", "embedding_watermarks",
        "embedding_fill_completions",
    )
    dispositions = {t: clone_table_disposition("okg", t) for t in tables}
    assert dispositions == {
        "embedding_sets": "preserve", "embedding_vector_counters": "preserve",
        "text_embeddings": "preserve", "node_documents": "purge",
        "embedding_templates": "purge", "embedding_watermarks": "purge",
        "embedding_fill_completions": "purge",
    }
    deployment, config, vector_set = _backfill_fixture(tmp_path, clean_runtime)
    first, _made = _counting_builder()
    assert _backfill(clean_runtime, deployment, config, first)["documents_embedded"] == 3
    _publish(clean_runtime, deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for table in tables:
            if dispositions[table] == "purge":
                conn.execute(f"TRUNCATE okg.{table}")
    assert _set_rows(clean_runtime, vector_set) == (3, 0)
    _publish(clean_runtime, deployment)
    report = _backfill(clean_runtime, deployment, config, _never_built)
    assert report["links_written"] == 4
    assert report["documents_embedded"] == 0 and report["documents_missing"] == 0
    assert report["fill_completed"] is True
    _publish(clean_runtime, deployment)
    page = _semantic(clean_runtime, deployment, _plan(BF_BLOCK), limit=10)
    assert page["vector_source"] == "vector_set"
    assert sorted(r["node_id"] for r in page["results"]) == [
        "jira:1", "jira:2", "jira:3", "note:1",
    ]


def test_backfill_embeds_only_missing_distinct_documents_and_writes_no_node_rows(
    clean_runtime: str, tmp_path,
) -> None:
    deployment, config, vector_set = _backfill_fixture(tmp_path, clean_runtime)
    g1 = _publish(clean_runtime, deployment)["generation_id"]
    # jira:3's document already has a vector: the backfill reuses it.
    ((_row, third_doc),) = _links(clean_runtime, "jira:3")
    _store_vector(clean_runtime, vector_set, third_doc)
    before = _node_versions(clean_runtime)

    build, made = _counting_builder()
    report = _backfill(clean_runtime, deployment, config, build)
    assert report["status"] == "completed", report
    assert report["nodes_scanned"] == 4
    assert report["links_written"] == 0, "the graph writer already linked them"
    # jira:1 and jira:2 render one document; jira:3's is stored already.
    assert report["documents_missing"] == 2
    assert report["documents_embedded"] == 2
    assert report["documents_reused"] == 1
    assert report["node_rows_written"] == 0
    assert sorted(made[0].texts) == sorted([ISSUE_DOC, NOTE_DOC])
    assert _node_versions(clean_runtime) == before, "no node row was written"
    assert _set_rows(clean_runtime, vector_set)[0] == 3

    # Resume: nothing is missing, so the backend is never even built.
    again = _backfill(clean_runtime, deployment, config, _never_built)
    assert again["documents_embedded"] == 0 and again["documents_missing"] == 0

    # The pinned generation does not change; the next publish shows them.
    assert _at(clean_runtime, vector_set, g1, "jira:2")["has_vector"] is False
    published = _publish(clean_runtime, deployment)
    assert not published.get("reused_generation"), published
    for node_id in ("jira:1", "jira:2", "jira:3", "note:1"):
        assert _at(
            clean_runtime, vector_set, published["generation_id"], node_id,
        )["has_vector"] is True, node_id


def test_backfill_links_documents_after_a_template_change_from_the_registered_row(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.embeddings.backfill import show_node_document

    deployment, config, vector_set = _backfill_fixture(tmp_path, clean_runtime)
    _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    _publish(clean_runtime, deployment)

    # A template change writes no node row, so no new link is made by the
    # graph writer. Same identity: the same set, re-registered.
    changed = _with_header(["subtype"])
    changed_set = parse_embedding_config(plural=changed).search_vector_set
    assert changed_set.set_id == vector_set.set_id
    _register(clean_runtime, changed_set)
    before = _node_versions(clean_runtime)

    # deployment.yaml still has the old template: documents come from the
    # REGISTERED row, and the difference is reported as drift.
    build, made = _counting_builder()
    report = _backfill(clean_runtime, deployment, config, build)
    assert report["config_drift"] == ["document"]
    assert report["links_written"] == 4
    assert report["documents_embedded"] == 3
    assert sorted(made[0].texts) == sorted([
        "Type: jira_issue\n\nThe nightly build fails on main.",
        "Type: jira_issue\n\nAnother body.",
        "Type: note\n\na note body",
    ])
    assert _node_versions(clean_runtime) == before
    generation = _publish(clean_runtime, deployment)["generation_id"]
    assert _at(clean_runtime, changed_set, generation, "jira:1")["has_vector"] is True

    shown = show_node_document(
        clean_runtime, deployment=deployment, config=config, set_name="main",
        node_id="jira:1",
    )
    assert shown["document"] == "Type: jira_issue\n\nThe nightly build fails on main."
    assert shown["linked"] is True and shown["has_vector"] is True
    assert shown["config_drift"] == ["document"]


def test_a_second_backfill_of_the_same_set_refuses_while_one_runs(
    clean_runtime: str, tmp_path,
) -> None:
    deployment, config, _vs = _backfill_fixture(tmp_path, clean_runtime)
    entered = threading.Event()
    release = threading.Event()

    class _Blocking(_CountingHash):
        def embed_batch(self, texts):
            entered.set()
            assert release.wait(30)
            return super().embed_batch(texts)

    results: dict = {}

    def first() -> None:
        try:
            results["first"] = _backfill(
                clean_runtime, deployment, config, lambda _spec: _Blocking(),
            )
        except BaseException as exc:  # reported below
            results["first"] = exc

    thread = threading.Thread(target=first)
    thread.start()
    try:
        assert entered.wait(30), "the first backfill never reached its embedding"
        with pytest.raises(SubstrateError) as caught:
            _backfill(clean_runtime, deployment, config, _never_built)
        assert caught.value.code == "embedding_backfill_in_progress"
    finally:
        release.set()
        thread.join(30)
    assert isinstance(results["first"], dict), results["first"]
    assert results["first"]["status"] == "completed"
    # The lock went with the first run: a third run proceeds (nothing left).
    assert _backfill(clean_runtime, deployment, config, _never_built)["status"] == (
        "completed"
    )


def test_dry_run_counts_per_set_and_subtype_and_writes_nothing(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    import okg.substrate.embeddings.identity as identity
    from okg.substrate.embeddings.backfill import dry_run_backfill

    block = json.loads(json.dumps(BF_BLOCK))
    # A second configured set that `okg migrate` never registered.
    block["sets"]["alt"] = dict(block["sets"]["main"], max_chars=1000)
    block["search_set"] = "main"
    deployment, config, vector_set = _backfill_fixture(tmp_path, clean_runtime, block)
    monkeypatch.setattr(identity, "build_embedder", _never_built)

    def state() -> tuple:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return tuple(conn.execute(
                "SELECT (SELECT count(*) FROM okg.text_embeddings), "
                "(SELECT count(*) FROM okg.node_documents), "
                "(SELECT count(*) FROM okg.llm_calls), "
                "(SELECT sum(next_seq) FROM okg.embedding_sets)"
            ).fetchone())

    before = state()
    report = dry_run_backfill(
        clean_runtime, deployment=deployment, config=config, set_name=None,
    )
    assert state() == before
    by_name = {entry["set"]: entry for entry in report["sets"]}
    assert by_name["alt"]["status"] == "embedding_set_unregistered"
    main = by_name["main"]
    assert main["nodes"] == 4 and main["documents"] == 3
    assert main["documents_missing"] == 3 and main["links_missing"] == 0
    issue_chars = len(ISSUE_DOC) + len(
        "Type: jira_issue\nTitle: Build breaks\n\nAnother body."
    )
    assert main["by_subtype"] == {
        "jira_issue": {"nodes": 3, "links_missing": 0, "documents_missing": 2,
                       "characters_missing": issue_chars},
        "note": {"nodes": 1, "links_missing": 0, "documents_missing": 1,
                 "characters_missing": len(NOTE_DOC)},
    }
    assert main["characters_missing"] == issue_chars + len(NOTE_DOC)
    assert main["estimated_usd"] == 0.0 and "free backend" in main["price_source"]
    # Review P10 F1: a free set's rates are never looked up; the basis says
    # so instead of claiming the ledger has no call of the model.
    assert "rates are measured only for paid models" in main["token_basis"]
    assert "no priced backfill call" not in main["token_basis"]

    # After a template change every node lacks a link to its new document.
    _register(clean_runtime, parse_embedding_config(
        plural=_with_header(["subtype"]),
    ).search_vector_set)
    (changed,) = dry_run_backfill(
        clean_runtime, deployment=deployment, config=config, set_name="main",
    )["sets"]
    assert changed["links_missing"] == 4


def test_embed_show_prints_the_registered_document_and_refuses_an_unknown_node(
    clean_runtime: str, tmp_path, capsys,
) -> None:
    import hashlib

    from okg.substrate.cli.main import main as okg_main

    deployment, config, _vs = _backfill_fixture(tmp_path, clean_runtime)
    base = ["db", "embed-show", "--deployment", deployment, "--dsn", clean_runtime,
            "--set", "main", "--json"]

    def show(node_id: str) -> dict:
        assert okg_main(base + ["--node", node_id]) == 0
        out = json.loads(capsys.readouterr().out)
        return out.get("data", out)

    shown = show("jira:1")
    assert shown["document"] == ISSUE_DOC
    assert shown["doc_hash"] == hashlib.sha256(ISSUE_DOC.encode()).hexdigest()
    assert shown["linked"] is True and shown["has_vector"] is False
    assert shown["covered"] is True and shown["omitted_fields"] == []
    assert shown["readable_at_pin"] is False
    _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    # Stored, but the pinned generation's watermark is below it until the
    # next publish.
    stored = show("jira:1")
    assert stored["has_vector"] is True and stored["readable_at_pin"] is False
    _publish(clean_runtime, deployment)
    assert show("jira:1")["readable_at_pin"] is True

    assert okg_main(base + ["--node", "jira:missing"]) == 1
    assert "node_not_found" in capsys.readouterr().err


def test_embed_backfill_cli_runs_dry_runs_and_refuses_an_unknown_or_unregistered_set(
    clean_runtime: str, tmp_path, capsys,
) -> None:
    from okg.substrate.cli.main import main as okg_main

    deployment, _config, vector_set = _backfill_fixture(tmp_path, clean_runtime)
    base = ["db", "embed-backfill", "--deployment", deployment, "--dsn", clean_runtime,
            "--json"]

    assert okg_main(base + ["--set", "nope"]) == 1
    assert "embedding_set_not_configured" in capsys.readouterr().err

    assert okg_main(base + ["--dry-run"]) == 0
    dry = json.loads(capsys.readouterr().out)
    dry = dry.get("data", dry)
    assert dry["dry_run"] is True and dry["sets"][0]["documents_missing"] == 3
    assert _set_rows(clean_runtime, vector_set)[0] == 0

    assert okg_main(base + ["--set", "main"]) == 0
    ran = json.loads(capsys.readouterr().out)
    ran = ran.get("data", ran)
    assert ran["documents_embedded"] == 3 and ran["node_rows_written"] == 0

    # A set in deployment.yaml that migrate never registered.
    other = _with_header(["subtype"])
    other["sets"]["main"]["max_chars"] = 999
    path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(path.read_text())
    manifest["embeddings"] = other
    path.write_text(json.dumps(manifest))
    assert okg_main(base) == 1
    assert "embedding_set_unregistered" in capsys.readouterr().err


def test_embed_index_cli_rebuilds_beside_the_old_index_then_drops_it(
    clean_runtime: str, tmp_path, capsys,
) -> None:
    from okg.substrate.cli.main import main as okg_main
    from okg.substrate.embeddings.store import ensure_set_index, index_lock_key
    from okg.substrate.semantic_ann import inspect_set_index

    old_block = _search_block()
    new_block = json.loads(json.dumps(old_block))
    new_block["sets"]["main"]["index"]["build"] = {"m": 8, "ef_construction": 32}
    none_block = _search_block(kind="none")
    deployment = _deployment(tmp_path, clean_runtime, old_block)
    old_set, new_set = _vector_set(old_block), _vector_set(new_block)
    assert old_set.set_id == new_set.set_id
    old_name, new_name = set_index_name(old_set), set_index_name(new_set)
    assert old_name != new_name
    _register(clean_runtime, old_set)
    ensure_set_index(clean_runtime, old_set)

    def set_block(block: dict) -> None:
        path = Path(deployment) / "deployment.yaml"
        manifest = json.loads(path.read_text())
        manifest["embeddings"] = block
        path.write_text(json.dumps(manifest))

    def run(*extra: str) -> tuple[int, dict]:
        code = okg_main(["db", "embed-index", "--deployment", deployment,
                         "--dsn", clean_runtime, "--json", *extra])
        captured = capsys.readouterr()
        if code != 0:
            return code, {"err": captured.err}
        out = json.loads(captured.out)
        return code, out.get("data", out)

    def indexes() -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return [str(r[0]) for r in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'okg' "
                "AND indexname LIKE %s ORDER BY 1",
                (f"text_embeddings_{old_set.set_id[3:]}%",),
            ).fetchall()]

    # deployment.yaml names new settings that migrate has not registered.
    set_block(new_block)
    code, out = run()
    assert code == 1 and "embedding_index_config_drift" in out["err"]
    _register(clean_runtime, new_set)  # what `okg migrate` does

    code, dry = run()
    assert code == 0 and dry["status"] == "dry_run"
    assert dry["actions"] == [
        {"action": "build", "index": new_name}, {"action": "drop", "index": old_name},
    ]
    assert indexes() == [old_name]

    # Another rebuild of the set holds its lock: refuse, change nothing.
    with psycopg.connect(clean_runtime, autocommit=True) as holder:
        holder.execute(
            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
            (index_lock_key(old_set.set_id),),
        )
        code, out = run("--apply")
    assert code == 1 and "embedding_index_build_running" in out["err"]
    assert indexes() == [old_name]

    code, applied = run("--apply")
    assert code == 0 and applied["status"] == "present"
    assert applied["dropped"] == [old_name]
    assert indexes() == [new_name]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert inspect_set_index(conn, new_set)["status"] == "present"
    code, again = run()
    assert again["actions"] == [{"action": "keep", "index": new_name}]

    # kind none: every index of the set goes.
    set_block(none_block)
    _register(clean_runtime, _vector_set(none_block))
    code, unindexed = run("--apply")
    assert code == 0 and unindexed["status"] == "not_indexed"
    assert unindexed["dropped"] == [new_name] and indexes() == []


def test_must_fail_doctor_flags_a_pending_index_rebuild_that_is_not_running(
    clean_runtime: str, tmp_path,
) -> None:
    """Review P8 S1. New build settings are registered but their index was
    never built (a killed `okg migrate`), while the older index serves
    reads. Search stays up, so the index state is `present`; doctor still
    flags the pending rebuild, and stops once the new index is valid.
    Must-fail: doctor flagged only an index state a read refuses on, so a
    rebuild that never ran looked healthy."""
    from okg.substrate.embeddings.store import ensure_set_index

    old_block = _search_block()
    new_block = json.loads(json.dumps(old_block))
    new_block["sets"]["main"]["index"]["build"] = {"m": 8, "ef_construction": 32}
    deployment = _deployment(tmp_path, clean_runtime, new_block)
    old_set, new_set = _vector_set(old_block), _vector_set(new_block)
    old_name, new_name = set_index_name(old_set), set_index_name(new_set)
    _register(clean_runtime, old_set)
    ensure_set_index(clean_runtime, old_set)
    _register(clean_runtime, new_set)  # what `okg migrate` commits first
    config = parse_embedding_config(plural=new_block)

    report = _doctor(clean_runtime, deployment, config)
    (entry,) = report["sets"]
    assert entry["index"]["status"] == "present"
    assert entry["index"]["rebuild"] == {"index_name": new_name, "status": "missing"}
    codes = _codes(report)
    assert "vector_set_index_unavailable" not in codes
    pending = codes["vector_set_index_rebuild_pending"]
    assert pending["status"] == "missing"
    assert pending["index_name"] == new_name
    assert pending["serving_index"] == old_name
    assert report["ok"] is False

    ensure_set_index(clean_runtime, new_set)
    assert "vector_set_index_rebuild_pending" not in _codes(
        _doctor(clean_runtime, deployment, config)
    )


def test_must_fail_search_stays_up_while_migrate_builds_new_index_settings(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Review P7 S1. New build settings name a new index. `okg migrate`
    registers them, then builds that index concurrently; the build waits here
    behind an open write transaction on okg.text_embeddings, as it would
    behind live writers. Meanwhile an index-walk search still ranks the set,
    through the old valid index, and the page names the pending one. Once
    the build returns the new index serves. Must-fail: the read looked only
    for the index the registered settings name and refused
    (`semantic_index_unavailable`) for the whole build."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.db.migrate import _ensure_vector_sets

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", 2)
    old_block = _search_block()
    new_block = json.loads(json.dumps(old_block))
    new_block["sets"]["main"]["index"]["build"] = {"m": 8, "ef_construction": 32}
    deployment, old_set, _ = _semantic_fixture(
        tmp_path, clean_runtime, old_block, issues=3, notes=2,
    )
    new_set = _vector_set(new_block)
    old_name, new_name = set_index_name(old_set), set_index_name(new_set)
    assert old_set.set_id == new_set.set_id and old_name != new_name
    path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(path.read_text())
    manifest["embeddings"] = new_block
    path.write_text(json.dumps(manifest))
    plan = _plan(new_block)

    outcome: dict = {}

    def migrate() -> None:
        try:
            outcome["reports"] = _ensure_vector_sets(clean_runtime, deployment)
        except BaseException as exc:  # reported by the assertion below
            outcome["error"] = exc

    thread = threading.Thread(target=migrate, daemon=True)
    # CREATE INDEX CONCURRENTLY waits for every transaction holding a write
    # lock on the table, with its index already in the catalog as invalid.
    writer = psycopg.connect(clean_runtime)
    try:
        writer.execute("LOCK TABLE okg.text_embeddings IN ROW EXCLUSIVE MODE")
        thread.start()
        deadline = time.monotonic() + 60
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            while True:
                row = conn.execute(
                    "SELECT indisvalid FROM pg_index "
                    "WHERE indexrelid = to_regclass(%s)",
                    (f"okg.{new_name}",),
                ).fetchone()
                if row is not None:
                    break
                assert thread.is_alive(), outcome
                assert time.monotonic() < deadline, "the new build never started"
                time.sleep(0.05)
            assert row[0] is False
            registered = conn.execute(
                "SELECT index_json->>'m' FROM okg.embedding_sets WHERE set_id = %s",
                (new_set.set_id,),
            ).fetchone()[0]
            assert registered == "8", "migrate registered the new settings first"
        during = _semantic(clean_runtime, deployment, plan, limit=10)
    finally:
        writer.rollback()
        writer.close()
    thread.join(timeout=120)
    assert not thread.is_alive() and "error" not in outcome, outcome
    assert during["vector_source"] == "vector_set"
    assert during["ranking"] == "approximate"
    assert sorted(r["node_id"] for r in during["results"][:3]) == [
        "jira:0", "jira:1", "jira:2",
    ]
    index = during["vector_set"]["index"]
    assert index["status"] == "present" and index["index_name"] == old_name
    assert index["rebuild"]["index_name"] == new_name
    assert index["rebuild"]["status"] in ("building", "invalid")

    (report,) = outcome["reports"]
    assert report["index"]["action"] == "build"
    assert report["index"]["other_indexes"] == [old_name]
    after = _semantic(clean_runtime, deployment, plan, limit=10)
    assert after["vector_set"]["index"]["index_name"] == new_name
    assert "rebuild" not in after["vector_set"]["index"]


def test_cli_semantic_search_ranks_the_vector_set(
    clean_runtime: str, tmp_path, capsys,
) -> None:
    """`okg search --method semantic` serves the registered set, as the MCP
    path does. Must-fail: the old-column readiness check the command ran
    first refuses this graph (no node-column vectors at all)."""
    from okg.substrate.cli.main import main as okg_main
    from okg.substrate.mcp import open_session
    from okg.substrate.mcp.semantic_readiness import (
        assert_rankable,
        probe_semantic_readiness,
    )

    block = _with_header([])  # the document is the body: a query can equal it
    deployment, config, vector_set = _backfill_fixture(tmp_path, clean_runtime, block)
    _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    _publish(clean_runtime, deployment)

    assert okg_main([
        "search", "a note body", "--method", "semantic",
        "--deployment", deployment, "--json",
    ]) == 0
    out = json.loads(capsys.readouterr().out)
    page = out.get("data", out)
    assert page["vector_source"] == "vector_set"
    assert page["vector_set"]["set_id"] == vector_set.set_id
    assert page["results"][0]["node_id"] == "note:1"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        readiness = probe_semantic_readiness(conn, session, scoped_subtypes=None)
        with pytest.raises(SubstrateError) as caught:
            assert_rankable(
                readiness, query_model_id=vector_set.model_id,
                allow_unverified_provenance=False, scoped_subtypes=None,
            )
    assert caught.value.code == "semantic_index_empty"


# --------------------------------------------------------------------------- #
# okg doctor: coverage, index state, spend, drift, old-column vectors          #
# --------------------------------------------------------------------------- #

HNSW_BLOCK = json.loads(json.dumps(BF_BLOCK))
HNSW_BLOCK["sets"]["main"]["index"] = {"kind": "hnsw"}


def _doctor(dsn: str, deployment: str, config) -> dict:
    from okg.substrate.embeddings.doctor import vector_set_report

    with psycopg.connect(dsn, autocommit=True) as conn:
        return vector_set_report(conn, deployment=deployment, config=config)


def _codes(report: dict) -> dict[str, dict]:
    return {f["code"]: f for f in report["findings"]}


def _old_column_vector(dsn: str, node_id: str, model_id: str) -> None:
    """Write a vector into the old node column, as the embed worker does."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).checkout().execute(
            "UPDATE okg.graph_nodes SET embedding = :embedding::vector, "
            "       embedding_model = :model_id WHERE node_id = :node_id",
            {"embedding": "[" + ",".join(["0.01"] * 2560) + "]",
             "model_id": model_id, "node_id": node_id},
        )


def _ledger_call(dsn: str, deployment: str, set_id: str, cost: float | None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.llm_calls (deployment_name, caller, model, cost_usd, "
            "success, extras) VALUES (%s, 'embeddings.backfill', 'm', %s, TRUE, "
            "CAST(%s AS jsonb))",
            (deployment, cost, json.dumps({"set_id": set_id})),
        )


def test_doctor_reports_coverage_index_spend_and_unowned_old_column_vectors(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.embeddings.store import ensure_set_index

    deployment, config, vector_set = _backfill_fixture(
        tmp_path, clean_runtime, HNSW_BLOCK,
    )
    # One old-column vector of a model no set uses, one of the set's own model.
    _old_column_vector(clean_runtime, "jira:1", "legacy-model")
    _old_column_vector(clean_runtime, "jira:2", vector_set.model_id)
    g1 = _publish(clean_runtime, deployment)["generation_id"]

    report = _doctor(clean_runtime, deployment, config)
    (entry,) = report["sets"]
    assert entry["set_id"] == vector_set.set_id and entry["active"] is True
    assert entry["config_drift"] == []
    # The index is not built yet: a semantic read refuses, doctor says so.
    assert entry["index"]["status"] == "missing"
    codes = _codes(report)
    assert codes["vector_set_index_unavailable"]["status"] == "missing"
    # Coverage before the backfill: the writer linked all four, none has a vector.
    assert entry["coverage"]["generation_id"] == g1
    assert entry["coverage"]["text_nodes"] == 4
    assert entry["coverage"]["linked_nodes"] == 4
    assert entry["coverage"]["embedded_nodes"] == 0
    assert entry["coverage"]["subtypes"]["note"] == {
        "text_nodes": 1, "linked_nodes": 1, "embedded_nodes": 0,
    }
    assert codes["vector_set_coverage_incomplete"]["nodes"] == 4
    assert entry["spend"] == {
        "calls": 0, "usd": 0.0, "basis": "no_prior_calls",
        "answered_uncosted_calls": 0, "failed_calls": 0,
    }
    # The old column: the set's own model is owned; the other is flagged.
    assert report["old_columns"]["nodes_by_model"] == {
        "legacy-model": 1, vector_set.model_id: 1,
    }
    assert report["old_columns"]["unowned_nodes"] == 1
    assert codes["vector_old_column_unowned"]["model"] == "legacy-model"
    assert codes["vector_old_column_unowned"]["nodes"] == 1
    assert report["ok"] is False

    # Backfill: the vectors are written but the published generation does not
    # show them until the next publish.
    _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    ensure_set_index(clean_runtime, vector_set)
    _ledger_call(clean_runtime, deployment, vector_set.set_id, 0.25)
    report = _doctor(clean_runtime, deployment, config)
    (entry,) = report["sets"]
    codes = _codes(report)
    assert entry["index"]["status"] == "present"
    assert "vector_set_index_unavailable" not in codes
    assert entry["coverage"]["embedded_nodes"] == 0
    coverage = entry["coverage"]
    assert coverage["unpublished_writes"] == (
        entry["max_seq"] - coverage["watermark"]
        + entry["max_vector_seq"] - coverage["vector_watermark"]
    )
    assert entry["max_vector_seq"] - coverage["vector_watermark"] == 3
    assert codes["vector_set_unpublished_writes"]["rows"] > 0
    assert entry["vectors"] == 3  # jira:1 and jira:2 share a document
    assert entry["spend"]["calls"] == 1 and entry["spend"]["usd"] == 0.25

    g2 = _publish(clean_runtime, deployment)["generation_id"]
    report = _doctor(clean_runtime, deployment, config)
    (entry,) = report["sets"]
    codes = _codes(report)
    assert entry["coverage"]["generation_id"] == g2
    assert entry["coverage"]["embedded_nodes"] == 4
    assert entry["coverage"]["unpublished_writes"] == 0
    assert set(codes) == {"vector_old_column_unowned"}

    # An answered call with no cost makes the spend unknown.
    _ledger_call(clean_runtime, deployment, vector_set.set_id, None)
    codes = _codes(_doctor(clean_runtime, deployment, config))
    assert codes["vector_set_spend_unknown"]["set_id"] == vector_set.set_id

    # A published generation without the set's watermark: reads refuse.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM okg.embedding_watermarks WHERE generation_id = %s", (g2,),
        )
    report = _doctor(clean_runtime, deployment, config)
    assert report["sets"][0]["coverage"]["checked"] is False
    assert _codes(report)["vector_set_watermark_missing"]["generation_id"] == g2


def test_must_fail_plain_doctor_samples_coverage_above_its_exact_bound(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Lane EH DIAG-DOCTOR-COVERAGE. Exact coverage hashes every visible
    text node, which on a large graph outlasts plain doctor's 15 s timeout.
    Above COVERAGE_EXACT_MAX text nodes plain doctor counts text nodes
    exactly and estimates the rest from a BERNOULLI row sample, labelled
    `method: sample`; a gap in the sample is still a finding, marked as an
    estimate that names the exact check. Below the bound, and in the named
    check, the count stays exact. Here the sample is forced to 100%, so it
    must equal the exact count. Must-fail: plain doctor always ran the
    exact count and its coverage had no `method`."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.embeddings.doctor import vector_set_report

    deployment, config, _vector_set = _backfill_fixture(tmp_path, clean_runtime)
    _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    _write(clean_runtime, _issue("jira:9", text="A body written after the backfill."))
    _publish(clean_runtime, deployment)

    exact = _doctor(clean_runtime, deployment, config)
    coverage = exact["sets"][0]["coverage"]
    assert coverage["method"] == "exact"
    assert (coverage["text_nodes"], coverage["linked_nodes"], coverage["embedded_nodes"]) == (5, 5, 4)
    incomplete = _codes(exact)["vector_set_coverage_incomplete"]
    assert incomplete["nodes"] == 1 and "estimated" not in incomplete

    monkeypatch.setattr(semantic_ann, "COVERAGE_EXACT_MAX", 1)
    monkeypatch.setattr(semantic_ann, "COVERAGE_SAMPLE_ROWS", 1_000_000)
    sampled = _doctor(clean_runtime, deployment, config)
    coverage = sampled["sets"][0]["coverage"]
    assert coverage["method"] == "sample"
    assert coverage["sample_percent"] == 100.0
    assert coverage["text_nodes"] == coverage["sampled_nodes"] == 5
    assert coverage["linked_nodes"] is None and coverage["embedded_nodes"] is None
    assert coverage["sampled_linked"] == coverage["estimated_linked_nodes"] == 5
    assert coverage["sampled_embedded"] == coverage["estimated_embedded_nodes"] == 4
    assert coverage["missing_upper_95"] >= 1
    assert coverage["subtypes"]["jira_issue"]["estimated_embedded_nodes"] == 3
    assert coverage["subtypes"]["note"]["estimated_embedded_nodes"] == 1
    assert "--check vector-sets" in coverage["note"]
    estimate = _codes(sampled)["vector_set_coverage_incomplete"]
    assert estimate["estimated"] is True and estimate["nodes"] == 1
    assert estimate["sampled_missing"] == 1
    assert "okg doctor --check vector-sets" in estimate["message"]

    # The named check (exact_coverage) counts exactly whatever the size.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        named = vector_set_report(
            conn, deployment=deployment, config=config, exact_coverage=True,
        )
    coverage = named["sets"][0]["coverage"]
    assert coverage["method"] == "exact" and coverage["embedded_nodes"] == 4


def test_must_fail_coverage_does_not_count_a_node_with_blank_text_as_missing(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Lane EH (C4S copy: 629,970 of 629,971, the one "missing" node a
    document_chunk with text ''). The fill gives a node with empty or
    blank text no document (render_document: no non-empty body), so
    coverage, exact and sampled, must not count it as a text node the set
    is missing. Must-fail: coverage counted every node with a `text`
    attribute and doctor raised vector_set_coverage_incomplete for them."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.embeddings.documents import NONBLANK_BODY_REGEX

    deployment, config, _vector_set = _backfill_fixture(tmp_path, clean_runtime)
    _write(
        clean_runtime, _issue("jira:8", text=""),
        _note("note:9", " 　\n\t "),
    )
    _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    _publish(clean_runtime, deployment)

    exact = _doctor(clean_runtime, deployment, config)
    coverage = exact["sets"][0]["coverage"]
    assert coverage["method"] == "exact"
    assert (
        coverage["text_nodes"], coverage["linked_nodes"], coverage["embedded_nodes"],
    ) == (4, 4, 4), coverage
    assert "vector_set_coverage_incomplete" not in _codes(exact)

    monkeypatch.setattr(semantic_ann, "COVERAGE_EXACT_MAX", 1)
    monkeypatch.setattr(semantic_ann, "COVERAGE_SAMPLE_ROWS", 1_000_000)
    sampled = _doctor(clean_runtime, deployment, config)
    coverage = sampled["sets"][0]["coverage"]
    assert coverage["method"] == "sample"
    assert coverage["text_nodes"] == coverage["sampled_nodes"] == 4, coverage
    assert coverage["sampled_embedded"] == 4
    assert "vector_set_coverage_incomplete" not in _codes(sampled)

    # Review P12 S2: the pattern is a bound parameter, so it must encode
    # under any client encoding. Must-fail: a raw U+3000 in it raised
    # UnicodeEncodeError on LATIN1 and the coverage count failed. (Not
    # SQL_ASCII: psycopg returns text as bytes there, so doctor cannot read
    # the set registry whatever the pattern.)
    from okg.substrate.embeddings.doctor import vector_set_report

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("SET client_encoding = LATIN1")
        assert conn.info.encoding == "iso8859-1"
        latin1_report = vector_set_report(conn, deployment=deployment, config=config)
    entry = latin1_report["sets"][0]
    coverage = entry.get("coverage") or {}
    assert (coverage.get("method"), coverage.get("text_nodes")) == ("sample", 4), (
        entry, latin1_report["findings"],
    )
    assert "vector_set_probe_failed" not in _codes(latin1_report)

    # The database's blank test is Python's str.strip() test, character by
    # character: every str.isspace character, and look-alikes it keeps.
    samples = [chr(c) for c in range(0x10000) if chr(c).isspace()]
    samples += ["​", "﻿", "᠎", "x", " x ", ""]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        got = conn.execute(
            "SELECT array_agg(s ~ %s ORDER BY o) "
            "  FROM unnest(CAST(%s AS text[])) WITH ORDINALITY AS t(s, o)",
            (NONBLANK_BODY_REGEX, samples),
        ).fetchone()[0]
    assert got == [bool(s.strip()) for s in samples]


def test_doctor_flags_drift_unregistered_and_unconfigured_sets(
    clean_runtime: str, tmp_path,
) -> None:
    deployment, _config, vector_set = _backfill_fixture(tmp_path, clean_runtime)

    # A header change is not part of the identity: the same set, drifted.
    drifted = parse_embedding_config(plural=_with_header(["subtype"]))
    assert drifted.search_vector_set.set_id == vector_set.set_id
    report = _doctor(clean_runtime, deployment, drifted)
    assert report["sets"][0]["config_drift"] == ["document"]
    assert _codes(report)["vector_set_config_drift"]["drift"] == ["document"]

    # A cut change is: a new set migrate never registered, and the registered
    # one is active but no longer in the file.
    changed = _with_header(["subtype", "title"])
    changed["sets"]["main"]["max_chars"] = 999
    other = parse_embedding_config(plural=changed)
    report = _doctor(clean_runtime, deployment, other)
    by_id = {entry["set_id"]: entry for entry in report["sets"]}
    assert by_id[other.search_vector_set.set_id]["registered"] is False
    assert by_id[vector_set.set_id]["configured"] is False
    codes = _codes(report)
    assert codes["vector_set_unregistered"]["set_id"] == other.search_vector_set.set_id
    assert codes["vector_set_not_configured"]["set_id"] == vector_set.set_id

    # Embeddings disabled and nothing registered beyond the old set: still
    # reported, not skipped.
    report = _doctor(clean_runtime, deployment, None)
    assert [e["set_id"] for e in report["sets"]] == [vector_set.set_id]


def test_must_fail_doctor_without_an_active_set_does_not_flag_old_column_vectors(
    clean_runtime: str, tmp_path, capsys,
) -> None:
    """Review P5 F3. With no active vector set, semantic search reads the
    old node columns, so their vectors are in use: doctor reports no
    finding and `--check vector-sets` exits 0. Must-fail: counting them as
    unowned flags every deployment that has no vector set."""
    from okg.substrate.cli.main import main as okg_main

    deployment = _deployment(tmp_path, clean_runtime, None)
    _seed(clean_runtime)
    _write(clean_runtime, _issue("jira:1"))
    _old_column_vector(clean_runtime, "jira:1", "legacy-model")
    _publish(clean_runtime, deployment)

    report = _doctor(clean_runtime, deployment, None)
    assert report["sets"] == [] and report["findings"] == []
    assert report["old_columns"]["checked"] is False
    assert "no active vector set" in report["old_columns"]["reason"]
    assert report["ok"] is True
    assert okg_main([
        "doctor", "--deployment", deployment, "--check", "vector-sets", "--json",
    ]) == 0
    capsys.readouterr()


def test_doctor_flags_a_set_never_filled_or_filled_too_many_publishes_ago(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Jason, lane chat 2026-10-14, "ehfill yes": doctor flags stale
    coverage. A set no backfill has completed is flagged; a completed fill
    clears it; more publishes since the fill than FILL_STALE_PUBLISHES
    (lowered to 1 here) flags it again."""
    from okg.substrate.embeddings import doctor as set_doctor

    deployment, config, _vs = _backfill_fixture(tmp_path, clean_runtime)
    stale = _codes(_doctor(clean_runtime, deployment, config))["vector_set_fill_stale"]
    assert stale["reason"] == "never_completed"

    assert _backfill(clean_runtime, deployment, config, _counting_builder()[0])[
        "fill_completed"] is True
    _publish(clean_runtime, deployment)
    report = _doctor(clean_runtime, deployment, config)
    assert "vector_set_fill_stale" not in _codes(report)
    assert report["sets"][0]["fill"]["publishes_since"] == 1

    monkeypatch.setattr(set_doctor, "FILL_STALE_PUBLISHES", 1)
    _write(clean_runtime, _issue("jira:4", text="written after the fill"))
    _publish(clean_runtime, deployment)
    stale = _codes(_doctor(clean_runtime, deployment, config))["vector_set_fill_stale"]
    assert stale["reason"] == "publishes_since_fill" and stale["publishes_since"] == 2


def test_plain_doctor_carries_vector_set_findings_as_warnings(
    clean_runtime: str, tmp_path, monkeypatch, capsys,
) -> None:
    """Plain doctor copies the findings into its warnings and stays ok; a
    probe that fails is a warning naming it, never a clean result; the named
    check exits 1 on a finding."""
    from okg.substrate import semantic_ann
    from okg.substrate.cli import doctor as doctor_cli
    from okg.substrate.cli.main import main as okg_main
    from okg.substrate.deployments import resolve_deployment

    deployment, _config, _vs = _backfill_fixture(tmp_path, clean_runtime)
    manifest = resolve_deployment(deployment)
    report: dict = {"ok": True, "errors": []}
    result = doctor_cli._default_doctor_vector_sets(
        clean_runtime, deployment, manifest, report, timeout_ms=15_000,
    )
    assert result["ok"] is False
    assert {w["code"] for w in report["warnings"]} == {
        "vector_set_coverage_incomplete", "vector_set_fill_stale",
    }
    assert report["ok"] is True and report["errors"] == []

    assert okg_main([
        "doctor", "--deployment", deployment, "--check", "vector-sets", "--json",
    ]) == 1
    out = json.loads(capsys.readouterr().out)
    payload = out.get("data", out)
    assert payload["check"] == "vector-sets"
    assert payload["vector_sets"]["sets"][0]["coverage"]["text_nodes"] == 4

    def broken(*_args, **_kwargs):
        raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

    monkeypatch.setattr(semantic_ann, "set_coverage", broken)
    report = {"ok": True, "errors": []}
    result = doctor_cli._default_doctor_vector_sets(
        clean_runtime, deployment, manifest, report, timeout_ms=15_000,
    )
    assert result["sets"][0]["coverage"]["checked"] is False
    (failed,) = [w for w in report["warnings"] if w["code"] == "vector_set_probe_failed"]
    assert "statement timeout" in failed["message"]
    assert "--check vector-sets" in failed["fix_hint"]


def _worker_fill_findings(summary: dict) -> tuple[dict, list[dict]]:
    """The embed outcome's details as the workflow writes them (the fill
    details through the outcome writer's 8 KiB bound), read back the way
    doctor reads the newest outcome row."""
    from okg.substrate.embeddings.doctor import (
        _fill_failure_findings,
        _worker_fill_from_details,
    )
    from okg.substrate.runtime.embed_workflow import _vector_set_fill_details
    from okg.substrate.runtime.workflow_operational_outcomes import _bounded_details

    details = _bounded_details({"embedder": "hash", **_vector_set_fill_details(summary)})
    last = _worker_fill_from_details("wf-1", "2026-10-14T00:00:00+00:00", details)
    return details, _fill_failure_findings(last)


def test_doctor_flags_each_failed_set_in_the_worker_fill_outcome() -> None:
    """Review P9 SF4. A set whose worker fill raised is a
    `vector_set_fill_failed` finding naming the set, its error and code,
    read from the per-set status in the outcome details; a completed set
    beside it is not flagged."""
    _details, findings = _worker_fill_findings({"vector_sets": [
        {"set": "main", "status": "completed"},
        {"set": "paid", "status": "error", "error": "SubstrateError",
         "code": "embedding_cost_unreported", "reason": "prior spend unknown"},
    ]})
    (finding,) = findings
    assert finding["code"] == "vector_set_fill_failed"
    assert finding["set"] == "paid" and finding["error"] == "SubstrateError"
    assert finding["error_code"] == "embedding_cost_unreported"
    assert "prior spend unknown" in finding["message"]
    assert finding["workflow"] == "wf-1"


def test_must_fail_doctor_flags_a_truncated_worker_fill_outcome() -> None:
    """Review P9 SF3. Enough failed sets push the fill statuses past the
    outcome row's 8 KiB bound, so the writer drops the whole key and marks
    the row `_truncated`. Doctor must not read that as no failures.
    Must-fail: doctor read the missing key as an empty list and reported
    nothing."""
    failing = [
        {"set": f"set{i:02d}", "status": "error", "error": "RuntimeError",
         "code": None, "reason": "x" * 400}
        for i in range(30)
    ]
    details, findings = _worker_fill_findings({"vector_sets": failing})
    assert "vector_sets" not in details and details["_truncated"] is True
    (finding,) = findings
    assert finding["code"] == "vector_set_fill_report_truncated"
    assert finding["workflow"] == "wf-1"
    # Below the bound nothing is cut and nothing extra is flagged.
    _details, few = _worker_fill_findings({"vector_sets": failing[:2]})
    assert [f["code"] for f in few] == ["vector_set_fill_failed"] * 2


# --------------------------------------------------------------------------- #
# Template and classification changes (review P4 S1)                          #
# --------------------------------------------------------------------------- #

def _semantic_at(dsn: str, deployment: str, plan, generation: str, **kwargs) -> dict:
    from okg.substrate.mcp import open_session, search

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment, requested_generation=generation,
        )
        return search(conn, session, QUERY, method="semantic", embedder=plan, **kwargs)


def _ranked(page: dict) -> list[tuple[str, float]]:
    return [(row["node_id"], round(float(row["distance"]), 6)) for row in page["results"]]


@pytest.mark.parametrize(
    "kind, exact_max, ranking",
    [("none", 5_000, "exact"), ("hnsw", 1, "approximate")],
)
def test_must_fail_a_template_change_keeps_serving_the_older_documents(
    clean_runtime: str, tmp_path, monkeypatch, kind: str, exact_max: int,
    ranking: str,
) -> None:
    """A template is not part of a set's identity, so `okg migrate` applies a
    new one to the same set. Until the backfill embeds the new documents, the
    read keeps serving each node's older link, and a pinned page never
    changes. Must-fail: a read that accepts only the current template's links
    returns an empty page right after the change, at the old pin too. Run on
    both the exact scan and the HNSW walk (EXACT_SCOPE_MAX lowered to 1),
    where a node's older and newer documents are both in the index and must
    still rank the node once (review P5 F4)."""
    import okg.substrate.semantic_ann as semantic_ann

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", exact_max)
    block = _search_block(kind=kind)
    deployment, vector_set, _plan_ = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=3, notes=2,
    )
    plan = _plan(block)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.mcp import open_session

        g1 = str(open_session(conn, deployment_name=deployment).generation_id)
    first = _semantic(clean_runtime, deployment, plan, limit=10)
    assert first["ranking"] == ranking
    before = _ranked(first)
    assert len(before) == 5

    changed = _search_block(kind=kind)
    changed["sets"]["main"]["document"]["default"]["header"] = ["subtype"]
    new_set = _vector_set(changed)
    assert new_set.set_id == vector_set.set_id
    _register(clean_runtime, new_set)
    new_plan = _plan(changed)
    # The same pin, the new registered template, no backfill yet.
    assert _ranked(_semantic(clean_runtime, deployment, new_plan, limit=10)) == before

    # The backfill links and embeds the new documents; after a publish each
    # node ranks once, by its newest document.
    config = parse_embedding_config(plural=changed)
    report = _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    assert report["links_written"] == 5 and report["documents_embedded"] == 5
    g2 = _publish(clean_runtime, deployment)["generation_id"]
    after = _semantic_at(clean_runtime, deployment, new_plan, g2, limit=10)
    assert after["ranking"] == ranking
    ids = [row["node_id"] for row in after["results"]]
    assert sorted(ids) == sorted(node for node, _d in before)
    assert len(ids) == len(set(ids)), "a node with two servable links ranked twice"
    assert _ranked(after) != before, "the new documents' vectors were not used"
    # The earlier pin still reads exactly what it read.
    assert _ranked(_semantic_at(clean_runtime, deployment, new_plan, g1, limit=10)) == before
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.embedding_templates WHERE set_id = %s",
            (vector_set.set_id,),
        ).fetchone()[0] == 4  # two subtypes, two templates each


def test_must_fail_a_reverted_template_serves_its_own_documents_again(
    clean_runtime: str, tmp_path,
) -> None:
    """Template T1, then T2, then T1 again (review P5 F2). Each unchanged
    node keeps its T1 link at an old seq, and its T2 link is newer. After
    the revert, the backfill must add a newer copy of each T1 link so the
    newest-link rule serves T1's documents again; older pins keep reading
    what they read. Must-fail: with links that are never re-added, the read
    keeps serving T2's documents after the backfill and the publish."""
    block = _search_block(kind="none")
    deployment, vector_set, plan = _semantic_fixture(
        tmp_path, clean_runtime, block, issues=3, notes=2,
    )
    t1 = _ranked(_semantic(clean_runtime, deployment, plan, limit=10))
    assert len(t1) == 5

    changed = _search_block(kind="none")
    changed["sets"]["main"]["document"]["default"]["header"] = ["subtype"]
    _register(clean_runtime, _vector_set(changed))
    report = _backfill(
        clean_runtime, deployment, parse_embedding_config(plural=changed),
        _counting_builder()[0],
    )
    assert report["links_written"] == 5 and report["documents_embedded"] == 5
    g2 = _publish(clean_runtime, deployment)["generation_id"]
    t2 = _ranked(_semantic_at(clean_runtime, deployment, _plan(changed), g2, limit=10))
    assert t2 != t1

    # Back to T1: its documents and vectors are stored; only links move.
    _register(clean_runtime, vector_set)
    report = _backfill(
        clean_runtime, deployment, parse_embedding_config(plural=block),
        _never_built,
    )
    assert report["documents_embedded"] == 0
    g3 = _publish(clean_runtime, deployment)["generation_id"]
    page = _semantic_at(clean_runtime, deployment, plan, g3, limit=10)
    ids = [row["node_id"] for row in page["results"]]
    assert len(ids) == len(set(ids)) == 5
    assert _ranked(page) == t1, "the reverted template's documents were not served"
    assert report["links_written"] == 5, report
    # Re-running finds nothing to add.
    again = _backfill(
        clean_runtime, deployment, parse_embedding_config(plural=block),
        _never_built,
    )
    assert again["links_written"] == 0
    # The T2 pin still reads exactly what it read.
    assert _ranked(_semantic_at(clean_runtime, deployment, plan, g2, limit=10)) == t2


def test_must_fail_a_header_field_that_becomes_maskable_stops_its_documents(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Documents built while `status` was shown to every reader hold its
    value. Once the latest catalog classifies `status` as maskable, no read
    serves them, at any pin, until documents without it are embedded.
    Must-fail: accepting every stored template serves them again."""
    from okg.substrate.embeddings import documents

    block = {
        "enabled": True,
        "sets": {"main": {
            "backend": "hash",
            "subtypes": ["jira_issue"],
            "document": {
                "default": {"header": ["subtype", "title"], "body": "text"},
                "types": {"jira_issue": {"header": ["subtype", "title", "status"]}},
            },
            "index": {"kind": "none"},
        }},
    }
    classes = {"title": "non_pii", "status": "non_pii", "owner": "non_pii",
               "text": "free_text"}
    deployment = _deployment(tmp_path, clean_runtime, block)
    config = parse_embedding_config(plural=block)
    vector_set = config.search_vector_set
    plan = _plan(block)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec("jira_issue", metadata={"pii_classes": classes})])
    _register(clean_runtime, vector_set)
    _write(clean_runtime, _issue("jira:1"), _issue("jira:2", text="Another body."))
    _publish(clean_runtime, deployment)
    _backfill(clean_runtime, deployment, config, _counting_builder()[0])
    g2 = _publish(clean_runtime, deployment)["generation_id"]
    page = _semantic_at(clean_runtime, deployment, plan, g2, limit=10)
    assert sorted(row["node_id"] for row in page["results"]) == ["jira:1", "jira:2"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec(
            "jira_issue", metadata={"pii_classes": {**classes, "status": "direct_identifier"}},
        )])
    masked = _semantic_at(clean_runtime, deployment, plan, g2, limit=10)
    assert masked["results"] == []
    assert masked["embedding_coverage"]["text_nodes"] == 2

    monkeypatch.setattr(documents, "record_still_safe", lambda *_a, **_k: True)
    unsafe = _semantic_at(clean_runtime, deployment, plan, g2, limit=10)
    assert sorted(row["node_id"] for row in unsafe["results"]) == ["jira:1", "jira:2"]
