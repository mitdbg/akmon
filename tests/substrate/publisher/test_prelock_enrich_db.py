"""Issue 2941 fix 1: enricher and derived-fact work computed before the lock.

PostgreSQL proofs:

* a retraction or rewrite of a row from an older segment changes the
  current segment's write signature (the precondition the fingerprint rests
  on);
* a publish that uses the pre-lock pass produces the same graph, facts,
  receipts, progress and counts as one computed under the lock
  (``OKG_PRELOCK_ENRICH=0``) from the same starting database;
* a graph write between the pre-lock pass and the lock is detected and the
  cycle recomputes under the lock, matching a cold run on the final state;
* a write to a prepared chunk's rows after the enricher stage makes that
  chunk recompute under the branch write lock, never apply stale rows;
* an enricher whose plan tries to write is refused by the read-only guard and
  still runs under the lock;
* facts another writer inserts and deletes again while the pass plans (the
  merge-back gate) do not make a linker drop rows: the "already said this"
  check runs again under the lock;
* the fact-id sequences are part of the enricher-state fingerprint;
* an uncaught error in the enricher pass still drops the pre-lock attribute
  hash cache;
* the publish-lock probe counts only this database's lock.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

DEPLOYMENT = "test-prelock-enrich"
FILES = 12
REFS_PER_FILE = 8


def _code_facts(*, files: int, refs_per_file: int, rev: int = 1) -> list[Any]:
    from okg.substrate.library.sources.base import NodeFact

    out: list[Any] = []
    for f in range(files):
        path = f"pkg/mod_{f}.py"
        caller = f"symbol:demo:{path}:caller_{f}:1"
        out.append(NodeFact(
            node_id=caller, subtype="code_symbol",
            attrs={"repo": "demo", "path": path, "kind": "function",
                   "name": f"caller_{f}", "qualified_name": f"caller_{f}",
                   "start_line": 1, "end_line": 900},
            source_record_id={"node_id": caller},
            source_revision={"rev": rev},
        ))
        for r in range(refs_per_file):
            target = (f + r + 1) % files
            tpath = f"pkg/mod_{target}.py"
            name = f"fn_{target}_{r}"
            sym = f"symbol:demo:{tpath}:{name}:{10 + r}"
            out.append(NodeFact(
                node_id=sym, subtype="code_symbol",
                attrs={"repo": "demo", "path": tpath, "kind": "function",
                       "name": name, "qualified_name": name,
                       "start_line": 10 + r, "end_line": 12 + r},
                source_record_id={"node_id": sym},
                source_revision={"rev": rev},
            ))
            ref = f"reference:demo:{path}:{name}:{20 + r}"
            out.append(NodeFact(
                node_id=ref, subtype="code_reference",
                attrs={"repo": "demo", "path": path, "language": "python",
                       "name": name, "qualified_name": name,
                       "reference_kind": "call",
                       "enclosing_symbol_id": caller,
                       "start_line": 20 + r, "end_line": 20 + r},
                source_record_id={"node_id": ref},
                source_revision={"rev": rev},
            ))
    return out


TEST_FILES = 3


def _test_facts(*, files: int = TEST_FILES) -> list[Any]:
    """Tests that test_symbol links to code symbols (references, exercises,
    points_to), so a second planned linker produces derived edges."""
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    out: list[Any] = []
    for f in range(files):
        app_path = f"src/app_{f}.py"
        test_path = f"tests/test_app_{f}.py"
        app_symbol = f"symbol:demo:{app_path}:handle_{f}:30"
        test_symbol = f"symbol:demo:{test_path}:test_handle_{f}:10"
        case = f"test-case:fixture:test_handle_{f}"
        event = f"runtime-event:fixture:test_handle_{f}"
        frame = f"stack-frame:fixture:test_handle_{f}:0"
        for node_id, subtype, attrs in (
            (app_symbol, "code_symbol", {
                "repo": "demo", "path": app_path, "kind": "function",
                "name": f"handle_{f}", "qualified_name": f"app_{f}.handle_{f}",
                "start_line": 30, "end_line": 60}),
            (test_symbol, "code_symbol", {
                "repo": "demo", "path": test_path, "kind": "function",
                "name": f"test_handle_{f}",
                "qualified_name": f"tests.test_app_{f}.test_handle_{f}",
                "start_line": 10, "end_line": 20}),
            (case, "test_case", {
                "classname": f"tests.test_app_{f}", "name": f"test_handle_{f}",
                "status": "failed"}),
            (event, "runtime_event", {
                "event_id": event, "repo": "demo", "kind": "test_failure",
                "status": "failed", "message": "assertion failed"}),
            (frame, "stack_frame", {
                "frame_id": frame, "repo": "demo", "path": app_path,
                "line": 42, "function": f"handle_{f}"}),
        ):
            out.append(NodeFact(
                node_id=node_id, subtype=subtype, attrs=attrs,
                source_record_id={"node_id": node_id},
                source_revision={"rev": 1},
            ))
        for src, edge_type, dst in (
            (event, "references", case),
            (event, "contains", frame),
            (test_symbol, "calls", app_symbol),
        ):
            out.append(EdgeFact(
                src=src, dst=dst, edge_type=edge_type, attrs={},
                source_record_id={"edge": f"{src}|{edge_type}|{dst}"},
                source_revision={"rev": 1},
            ))
    return out


class _ListSource:
    profile = "discovery_crawl"

    def __init__(self, name: str, facts: list[Any]) -> None:
        self.name = name
        self.facts = facts

    def run(self, run_id: str, *, mode: str,
            cursor: dict[str, Any] | None = None) -> Any:
        from okg.substrate.library.sources.base import SourceRun

        del run_id, mode, cursor
        return SourceRun(facts=iter(self.facts), completed_scope=True,
                         run_mode="scope_complete")


def _ingest(dsn: str, source_name: str, facts: list[Any]) -> None:
    from okg.substrate.ingest import SourceRunner

    SourceRunner(dsn, source_name=source_name).run(
        _ListSource(source_name.split(".")[0], facts), mode="scope_complete",
    )


def _enrichers() -> list[Any]:
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker

    return [CodeReferenceResolver(), TestSymbolLinker()]


def _publish(dsn: str, enrichers: list[Any] | None = None) -> dict[str, Any]:
    from okg.substrate.publisher import Publisher

    out = Publisher(
        dsn,
        DEPLOYMENT,
        enrichers=enrichers if enrichers is not None else _enrichers(),
        catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert out["status"] == "published", out
    return out


def _setup_deployment(dsn: str, tmp_path: Path) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": DEPLOYMENT,
        "description": "pre-lock enricher fixture",
        "modules": ["git_graph", "openspec", "agent_sessions",
                    "compute_env", "ci", "code_graph"],
    }))
    (dep / "schemas").mkdir()
    with psycopg.connect(dsn) as conn:
        apply_composed_catalog(conn, compose_catalog(dep))


def _prepared_state(dsn: str, tmp_path: Path) -> None:
    """A published predecessor, then a source run the enrichers must read."""
    _setup_deployment(dsn, tmp_path)
    _ingest(dsn, "seed.code", _code_facts(files=2, refs_per_file=1))
    _publish(dsn)
    _ingest(dsn, "fixture.code", _code_facts(
        files=FILES, refs_per_file=REFS_PER_FILE,
    ) + _test_facts())


def _clone(dsn: str) -> str:
    """Copy the worker database; return the copy's DSN."""
    from okg.substrate.db.pool import close_all
    from tests.substrate.conftest import (
        _drop_database_force,
        _maintenance_dsn,
        _target_dbname,
        create_database_from_template,
    )

    close_all()
    source = _target_dbname(dsn)
    assert source
    copy = f"{source}_prelock_b"
    # The worker database allows connections, so TimescaleDB runs a
    # scheduler in it; create_database_from_template ends that scheduler and
    # fails, naming the session, if anything else is still connected.
    with psycopg.connect(_maintenance_dsn(dsn), autocommit=True) as conn, \
            conn.cursor() as cur:
        _drop_database_force(cur, copy)
        create_database_from_template(cur, copy, source)
    parts = urlsplit(dsn)
    return urlunsplit(parts._replace(path=f"/{copy}"))


def _drop(dsn: str) -> None:
    from okg.substrate.db.pool import close_all
    from tests.substrate.conftest import (
        _drop_database_force,
        _maintenance_dsn,
        _target_dbname,
    )

    close_all()
    with psycopg.connect(_maintenance_dsn(dsn), autocommit=True) as conn, \
            conn.cursor() as cur:
        _drop_database_force(cur, _target_dbname(dsn))


def _snapshot(dsn: str, out: dict[str, Any]) -> dict[str, Any]:
    """Everything the publish produced that must not depend on the pass."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        nodes = sorted(
            (r["node_id"], r["subtype"], json.dumps(r["attrs"], sort_keys=True),
             r.get("source"),
             json.dumps(r.get("source_record_id"), sort_keys=True),
             json.dumps(r.get("source_revision"), sort_keys=True))
            for r in checkout.query(
                "SELECT node_id, subtype, attrs, source, source_record_id, "
                "source_revision FROM okg.graph_nodes"
            )
        )
        edges = sorted(
            (int(r["edge_id"]), r["src"], r["dst"], r["edge_type"],
             json.dumps(r["attrs"], sort_keys=True), r.get("source"),
             r.get("provenance"),
             json.dumps(r.get("source_record_id"), sort_keys=True),
             json.dumps(r.get("source_revision"), sort_keys=True))
            for r in checkout.query(
                "SELECT edge_id, src, dst, edge_type, attrs, source, provenance, "
                "source_record_id, source_revision FROM okg.graph_edges"
            )
        )
    finally:
        store.close()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT src, dst, edge_type, attrs::text, provenance, confidence,
                   source, source_record_id::text, source_revision::text,
                   dedupe_key, op, fact_id
              FROM okg.edge_facts ORDER BY fact_id
            """
        )
        edge_facts = [tuple(r) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT enricher_name, fact_watermark, edge_watermark, last_status,
                   catalog_version_id
              FROM okg.enricher_progress ORDER BY enricher_name
            """
        )
        progress = [tuple(r) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT producer_id, intent, status, admission_mode, count(*)
              FROM okg.admission_envelopes
             GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4
            """
        )
        envelopes = [tuple(r) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT chunk_ordinal, state, graph_result::text, target_branch
              FROM okg.fact_batch_receipts
             ORDER BY chunk_ordinal, state, graph_result::text
            """
        )
        receipts = [tuple(r) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT target_kind, target_id, namespace, attr_key, value::text,
                   value_hash
              FROM okg.attribute_effective_attrs
             ORDER BY 1, 2, 3, 4
            """
        )
        attribute_overlays = [tuple(r) for r in cur.fetchall()]
    counts = {
        key: out.get(key)
        for key in ("node_count", "edge_count", "nodes_added", "edges_added",
                    "nodes_retracted", "edges_retracted", "nodes_updated",
                    "edges_updated", "status", "blocked_reason")
    }
    return {
        "nodes": nodes, "edges": edges, "edge_facts": edge_facts,
        "progress": progress, "envelopes": envelopes, "receipts": receipts,
        "attribute_overlays": attribute_overlays, "counts": counts,
        "enrichers": sorted(
            (r.get("name"), r.get("status"), r.get("n_edges_emitted"),
             r.get("n_edges_skipped"))
            for r in out.get("enricher_runs") or []
        ),
    }


def _report(out: dict[str, Any]) -> dict[str, Any]:
    return dict((out.get("timing_details") or {}).get("prelock_enrich") or {})


def _assert_same(a: dict[str, Any], b: dict[str, Any]) -> None:
    for key in a:
        assert a[key] == b[key], f"{key} differs"


def _write_node(dsn: str, node_id: str, attrs: dict[str, Any]) -> None:
    """A graph write by another session, as a concurrent ingest would."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        store.apply_facts(
            [NodeFact(node_id=node_id, subtype="code_symbol", attrs=attrs,
                      source_record_id={"node_id": node_id},
                      source_revision={"rev": 9})],
            branch="default",
            source="fixture.concurrent",
        )
    finally:
        store.close()


def _signature(dsn: str) -> Any:
    from okg.substrate.publisher.dirty_hint_coverage import (
        segment_write_signature,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        return segment_write_signature(conn, branch_name="default")


def test_older_segment_retraction_and_rewrite_change_segment_signature(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """The fingerprint's precondition: a write to a row that an earlier
    checkpoint wrote lands in the current segment, so the signature moves."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    _setup_deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, "seed.code", _code_facts(files=2, refs_per_file=1))
    _publish(clean_runtime, enrichers=[])
    target = "symbol:demo:pkg/mod_0.py:caller_0:1"
    empty = _signature(clean_runtime)

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        store.apply_facts(
            [NodeFact(node_id=target, subtype="code_symbol", attrs={},
                      source_record_id={"node_id": target},
                      source_revision={"rev": 1}, op="R")],
            branch="default",
            source="seed.code",
        )
    finally:
        store.close()
    retracted = _signature(clean_runtime)
    assert retracted != empty, "a retraction of an older row must move it"

    _write_node(clean_runtime, "symbol:demo:pkg/mod_1.py:caller_1:1",
                {"repo": "demo", "path": "pkg/mod_1.py", "name": "caller_1",
                 "kind": "function", "qualified_name": "caller_1",
                 "start_line": 2, "end_line": 900})
    rewritten = _signature(clean_runtime)
    assert rewritten != retracted, "a rewrite of an older row must move it"


def test_prelock_publish_matches_under_lock_publish(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prepared_state(clean_runtime, tmp_path)
    cold_dsn = _clone(clean_runtime)
    try:
        monkeypatch.delenv("OKG_PRELOCK_ENRICH", raising=False)
        hot = _publish(clean_runtime)
        report = _report(hot)
        assert report.get("planned") is True, report
        assert report.get("fallback") is None, report
        assert report["dirty_scope"]["seeded"] >= 1, report
        assert report["enrichers"]["code_reference"]["used"] is True, report
        assert report["enrichers"]["code_reference"]["rows"] > 0, report
        # A second planned linker produces derived edges too.
        assert report["enrichers"]["test_symbol"]["used"] is True, report
        assert report["enrichers"]["test_symbol"]["rows"] > 0, report
        assert report["enrichers"]["test_symbol"]["derived_chunks"] >= 1, (
            report
        )
        assert report["derived"]["chunks_reused"] >= 1, report
        assert not report["derived"].get("chunks_recomputed"), report
        assert report["derived"]["chunk_lock_hold_ms_max"] > 0, report
        # The counters also reach timings_ms, which persisted rows keep.
        timings = hot["timings_ms"]
        assert timings["prelock_enrich.chunks_reused"] >= 1
        assert timings["prelock_enrich.enrichers_used"] >= 1
        assert timings["prelock_enrich.fallback"] == 0.0

        monkeypatch.setenv("OKG_PRELOCK_ENRICH", "0")
        cold = _publish(cold_dsn)
        assert _report(cold).get("planned") is False
        hot_snapshot = _snapshot(clean_runtime, hot)
        _assert_same(hot_snapshot, _snapshot(cold_dsn, cold))
        linked = {
            (edge[3], edge[1].split(":", 1)[0])
            for edge in hot_snapshot["edges"]
            if edge[5] == "_enricher:test_symbol"
        }
        assert ("references", "test-case") in linked, linked
        assert ("points_to", "stack-frame") in linked, linked
    finally:
        _drop(cold_dsn)


def _inject_after_prelock(
    monkeypatch: pytest.MonkeyPatch, action: Callable[[], None],
) -> None:
    from okg.substrate.publisher import Publisher

    original = Publisher._prelock_enrich

    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        state = original(self, *args, **kwargs)
        action()
        return state

    monkeypatch.setattr(Publisher, "_prelock_enrich", wrapped)


def test_graph_write_between_prelock_pass_and_lock_recomputes(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _prepared_state(clean_runtime, tmp_path)
    cold_dsn = _clone(clean_runtime)
    extra = "symbol:demo:pkg/late.py:fn_late:1"
    attrs = {"repo": "demo", "path": "pkg/late.py", "kind": "function",
             "name": "fn_late", "qualified_name": "fn_late",
             "start_line": 1, "end_line": 3}
    try:
        monkeypatch.delenv("OKG_PRELOCK_ENRICH", raising=False)
        _inject_after_prelock(
            monkeypatch, lambda: _write_node(clean_runtime, extra, attrs),
        )
        hot = _publish(clean_runtime)
        report = _report(hot)
        assert report.get("planned") is True, report
        assert report["fallback"]["reason"] == (
            "inputs_changed_before_enrichers"
        ), report
        assert "segment_signature" in report["fallback"]["changed"], report
        assert not report["dirty_scope"].get("seeded"), report
        assert not (report.get("derived") or {}).get("chunks_reused"), report

        monkeypatch.undo()
        monkeypatch.setenv("OKG_PRELOCK_ENRICH", "0")
        _write_node(cold_dsn, extra, attrs)
        cold = _publish(cold_dsn)
        _assert_same(_snapshot(clean_runtime, hot), _snapshot(cold_dsn, cold))
    finally:
        _drop(cold_dsn)


def test_chunk_row_written_after_enrichers_is_recomputed_under_lock(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A write to an endpoint of a prepared chunk after the enricher stage:
    the chunk's key fingerprint moves, so the chunk is prepared again under
    the branch write lock instead of applying its stale lineage."""
    from okg.substrate.publisher import Publisher

    _prepared_state(clean_runtime, tmp_path)
    cold_dsn = _clone(clean_runtime)
    # Not in the seed run (files 0-1), so its edge is new this cycle and sits
    # in a prepared chunk.
    endpoint = "reference:demo:pkg/mod_5.py:fn_7_1:21"

    def rewrite(dsn: str) -> None:
        # The same reference node at a new source revision with one more
        # attribute: a new row version of an endpoint the chunk read.
        before = _signature(dsn)
        _write_node_ref(dsn, endpoint)
        assert _signature(dsn) != before, "the injected write must land"

    def install(dsn: str) -> None:
        original = Publisher._apply_publisher_derived_fact_batch

        def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
            if not getattr(self, "_test_rewrote", False):
                self._test_rewrote = True
                rewrite(dsn)
            return original(self, *args, **kwargs)

        monkeypatch.setattr(
            Publisher, "_apply_publisher_derived_fact_batch", wrapped,
        )

    try:
        monkeypatch.delenv("OKG_PRELOCK_ENRICH", raising=False)
        install(clean_runtime)
        hot = _publish(clean_runtime)
        report = _report(hot)
        assert report["enrichers"]["code_reference"]["used"] is True, report
        derived = report["derived"]
        assert derived.get("chunks_recomputed", 0) >= 1, report
        assert derived["recompute_reasons"].get("key_rows_changed", 0) >= 1, (
            report
        )

        monkeypatch.undo()
        monkeypatch.setenv("OKG_PRELOCK_ENRICH", "0")
        install(cold_dsn)
        cold = _publish(cold_dsn)
        _assert_same(_snapshot(clean_runtime, hot), _snapshot(cold_dsn, cold))
    finally:
        _drop(cold_dsn)


def _write_node_ref(dsn: str, node_id: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    _kind, _repo, path, name, line_text = node_id.split(":")
    line = int(line_text)
    module = path.rsplit("/", 1)[-1].removesuffix(".py").removeprefix("mod_")
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        store.apply_facts(
            [NodeFact(node_id=node_id, subtype="code_reference",
                      attrs={"repo": "demo", "path": path,
                             "language": "python", "name": name,
                             "qualified_name": name, "reference_kind": "call",
                             "enclosing_symbol_id":
                                 f"symbol:demo:{path}:caller_{module}:1",
                             "start_line": line, "end_line": line,
                             "end_column": 40},
                      source_record_id={"node_id": node_id},
                      source_revision={"rev": 7})],
            branch="default",
            source="fixture.code",
        )
    finally:
        store.close()


def test_plan_that_writes_is_refused_and_runs_under_lock(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )

    class WritingPlanner(CodeReferenceResolver):
        # The pre-lock pass calls _prelock_resolve (code_reference keeps its
        # resolution for the stale-declaration step, item 92a).
        def _prelock_resolve(self, cur, incremental, timings_ms):  # type: ignore[override]
            cur.execute(
                "UPDATE okg.enricher_progress SET last_status = last_status"
            )
            return super()._prelock_resolve(cur, incremental, timings_ms)

    _prepared_state(clean_runtime, tmp_path)
    monkeypatch.delenv("OKG_PRELOCK_ENRICH", raising=False)
    out = _publish(clean_runtime, enrichers=[WritingPlanner()])
    report = _report(out)
    entry = report["enrichers"]["code_reference"]
    assert entry["planned"] is False, report
    assert "ReadOnlySqlTransaction" in entry["reason"], report
    runs = {r["name"]: r for r in out["enricher_runs"]}
    assert runs["code_reference"]["status"] == "ok"
    assert runs["code_reference"]["n_edges_emitted"] > 0


_EDGE_FACT_SEQUENCE = "pg_get_serial_sequence('okg.edge_facts', 'fact_id')"


def _reserve_fact_ids(conn: psycopg.Connection, count: int) -> list[int]:
    return [
        int(row[0]) for row in conn.execute(
            f"SELECT nextval({_EDGE_FACT_SEQUENCE}) "
            "FROM generate_series(1, %s)",
            (count,),
        ).fetchall()
    ]


def _insert_linker_rows(
    conn: psycopg.Connection,
    rows: list[tuple[Any, ...]],
    fact_ids: list[int],
    *,
    source: str,
) -> None:
    """Commit ``rows`` (the linker emit layout) as facts with given ids."""
    from okg.substrate.library.linkers.code_reference import (
        _ENRICHMENT_WITNESS,
    )

    conn.execute(
        """
        INSERT INTO okg.edge_facts
          (fact_id, edge_id, src, dst, edge_type, attrs, provenance,
           confidence, source, source_record_id, source_revision,
           observed_at, dedupe_key, run_id, op, source_program_checksum)
        SELECT r.fact_id, r.edge_id, r.src, r.dst, r.edge_type,
               r.attrs::jsonb, 'derived_deterministic', 1.0, %s,
               r.source_record_id::jsonb, r.source_revision::jsonb, now(),
               r.dedupe_key, gen_random_uuid(), 'I', %s
          FROM unnest(%s::bigint[], %s::bigint[], %s::text[], %s::text[],
                      %s::text[], %s::text[], %s::text[], %s::text[],
                      %s::text[])
            AS r(fact_id, edge_id, src, dst, edge_type, attrs,
                 source_record_id, source_revision, dedupe_key)
        """,
        (
            source,
            _ENRICHMENT_WITNESS,
            fact_ids,
            [row[0] for row in rows],
            [row[1] for row in rows],
            [row[2] for row in rows],
            [row[3] for row in rows],
            [row[4] for row in rows],
            [row[5] for row in rows],
            [row[6] for row in rows],
            [row[8] for row in rows],
        ),
    )


def test_facts_inserted_and_deleted_during_the_pass_are_still_written(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The merge-back gate case. Another writer commits the linker's own
    rows as facts while the pass plans, and deletes them before the pass
    ends (the gate restores its fact log exactly). Its fact ids were
    reserved before the pass's first fingerprint, so the watermarks, the
    sequences, the chunks and the progress rows all read the same before and
    after: the plan is used. Its "already said this" answer, taken while
    those facts existed, said every row was written; the linker must ask
    again under the lock and write them, as the in-lock path does."""
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )
    from okg.substrate.publisher import Publisher

    reserve = 4000
    _prepared_state(clean_runtime, tmp_path)
    cold_dsn = _clone(clean_runtime)
    other = psycopg.connect(clean_runtime, autocommit=True)
    try:
        monkeypatch.delenv("OKG_PRELOCK_ENRICH", raising=False)
        reserved: list[int] = []
        injected: list[int] = []
        in_pass = [False]
        original_pass = Publisher._prelock_enrich
        original_check = CodeReferenceResolver._rows_to_insert

        def pass_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # A writer whose transaction reserved its ids before the pass.
            reserved.extend(_reserve_fact_ids(other, reserve))
            in_pass[0] = True
            try:
                return original_pass(self, *args, **kwargs)
            finally:
                in_pass[0] = False

        def check_wrapper(self: Any, cur: Any, rows: list[Any]) -> Any:
            if not in_pass[0] or injected:
                return original_check(self, cur, rows)
            assert len(rows) <= len(reserved), "reserve more fact ids"
            ids = reserved[:len(rows)]
            _insert_linker_rows(
                other, rows, ids, source=f"_enricher:{self.name}",
            )
            injected.extend(ids)
            try:
                return original_check(self, cur, rows)
            finally:
                other.execute(
                    "DELETE FROM okg.edge_facts WHERE fact_id = ANY(%s)",
                    (ids,),
                )

        monkeypatch.setattr(Publisher, "_prelock_enrich", pass_wrapper)
        monkeypatch.setattr(
            CodeReferenceResolver, "_rows_to_insert", check_wrapper,
        )
        hot = _publish(clean_runtime)
        report = _report(hot)
        assert injected, "the injected facts must land during planning"
        assert report.get("planned") is True, report
        assert report.get("fallback") is None, report
        entry = report["enrichers"]["code_reference"]
        # The pre-lock answer dropped every row; the plan was still used.
        assert entry["rows"] == 0 and entry["skipped"] == len(injected), report
        assert entry["used"] is True, report

        monkeypatch.undo()
        monkeypatch.setenv("OKG_PRELOCK_ENRICH", "0")
        with psycopg.connect(cold_dsn, autocommit=True) as cold_conn:
            _reserve_fact_ids(cold_conn, reserve)
        cold = _publish(cold_dsn)
        assert hot["edge_count"] == cold["edge_count"]
        _assert_same(_snapshot(clean_runtime, hot), _snapshot(cold_dsn, cold))
        # The linker asked again under the lock and wrote every row.
        assert entry["dedupe_recheck"] == "changed", report
        assert entry["inserted"] == len(injected), report
    finally:
        other.close()
        _drop(cold_dsn)


def test_enricher_state_fingerprint_sees_a_fact_inserted_and_deleted(
    clean_runtime: str,
) -> None:
    """max(fact_id) cannot see a fact that came and went; the sequence can."""
    from okg.substrate.publisher.prelock_enrich import (
        differing_keys,
        enricher_state_fingerprint,
    )

    row = (1, "a", "b", "references", "{}", "{}", "{}", None, "k-1", None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # One fact that stays, so the fact chunk exists before the probe.
        _insert_linker_rows(
            conn, [row], _reserve_fact_ids(conn, 1), source="_enricher:probe",
        )
        before = enricher_state_fingerprint(conn, deployment_name=DEPLOYMENT)
        ids = _reserve_fact_ids(conn, 1)
        _insert_linker_rows(conn, [row], ids, source="_enricher:probe")
        conn.execute(
            "DELETE FROM okg.edge_facts WHERE fact_id = ANY(%s)", (ids,),
        )
        after = enricher_state_fingerprint(conn, deployment_name=DEPLOYMENT)
    assert differing_keys(before, after) == ["edge_fact_sequence"]


def test_publish_lock_held_in_another_database_does_not_skip_the_pass(
    clean_runtime: str,
) -> None:
    """Advisory locks are per database, but pg_locks lists every database.
    The same deployment's publish lock held in another database is not
    this deployment's lock; held in this database, it is."""
    from okg.substrate.dsn_form import dsn_with_dbname
    from okg.substrate.publisher import Publisher
    from okg.substrate.publisher.publisher import _advisory_keys

    publisher = Publisher(clean_runtime, DEPLOYMENT, enrichers=_enrichers())
    classid, objid = _advisory_keys(DEPLOYMENT)
    for holder_dsn, expected in (
        (dsn_with_dbname(clean_runtime, "postgres"), True),
        (clean_runtime, False),
    ):
        with psycopg.connect(holder_dsn, autocommit=True) as holder, \
                psycopg.connect(clean_runtime, autocommit=True) as waiter:
            holder.execute("SELECT pg_advisory_lock(%s, %s)", (classid, objid))
            try:
                run, reason = publisher._prelock_enrich_plan(waiter)
            finally:
                holder.execute(
                    "SELECT pg_advisory_unlock(%s, %s)", (classid, objid),
                )
        assert run is expected, (holder_dsn, reason)
