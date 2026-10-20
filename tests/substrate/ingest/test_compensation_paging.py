"""Compensation retracts a failed run a bounded page at a time.

A refused code-structure run of about 1.5 million rows made the old
compensation load every row into one list: the process grew to 28.7 GB and
its transaction sat idle for up to 93 seconds between statements. The change
reads and retracts the run in keyset pages inside the same one transaction.

Lane C's constraint: WHAT gets retracted must not change, only how it is
loaded. ``_legacy_retract_chronos_run`` at the bottom of this file is the
replaced implementation, verbatim, kept only as the oracle. Each equality test
seeds one graph, copies it to two branches, compensates one branch with the
oracle and the other with the paged code, then compares what each retracted
(every fact's retraction-relevant fields: an edge's src, edge_type and dst, a
node's node_id and subtype -- the only fields apply_facts reads from an "R"
fact) and every visible row the two branches hold afterwards.
"""
from __future__ import annotations

import math
import tracemalloc
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
import pytest

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.compensation import (
    iter_run_retraction_pages,
    retract_chronos_run,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact


pytestmark = pytest.mark.postgres

SOURCE = "repo.gamma"
BAD_RUN = "run-gamma-bad"
GOOD_RUN = "run-gamma-good"
OTHER = "repo.alpha"
SHARED_OWNED = "author:owned-by-bad-run@example.org"
SHARED_FOREIGN = "author:owned-by-alpha@example.org"


def _node(
    node_id: str, subtype: str = "claim", *, op: str = "I", **attrs: Any,
) -> NodeFact:
    return NodeFact(
        node_id,
        subtype,
        {} if op == "R" else {"text": f"text of {node_id}", **attrs},
        {"id": node_id},
        {"rev": 1},
        op=op,
    )


def _author(node_id: str, source: str) -> NodeFact:
    return NodeFact(
        node_id,
        "git_author",
        {"label": node_id, "email": node_id, "seen_in": source},
        {"author": node_id, "repo": source},
        {"commit": f"{source}-head"},
        ownership_mode="substitute",
    )


def _edge(
    src: str, dst: str, edge_type: str = "supported_by", **attrs: Any,
) -> EdgeFact:
    return EdgeFact(
        src,
        dst,
        edge_type,
        attrs={"note": f"{src}->{dst}", **attrs},
        confidence=0.75,
        source_record_id={"id": f"{src}->{dst}"},
        source_revision={"rev": 1},
    )


def _apply(
    store: ChronosGraphStore, facts: list, *, source: str, run_id: str,
) -> None:
    store.apply_facts(
        facts, source=source, run_id=run_id, strict_endpoints=False,
    )


def _seed_mixed_run(store: ChronosGraphStore, *, bad_nodes: int) -> None:
    """One source with a good run and a failed run that touches everything.

    The failed run writes nodes and edges, owns a node another source also
    declares (compensation hands it off), declares a node another source
    owns (compensation withdraws the declaration), retracts a node of the
    good run and one of its own (deletions compensation must not undo), and
    has edges of the good run and of another source incident to its nodes.
    """
    _apply(
        store,
        [_node(f"a:{i}") for i in range(5)] + [_author(SHARED_FOREIGN, OTHER)],
        source=OTHER, run_id=OTHER,
    )
    good: list = [_node(f"g:{i}", "evidence") for i in range(6)]
    good += [_edge(f"g:{i}", f"g:{i + 1}") for i in range(5)]
    _apply(store, good, source=SOURCE, run_id=GOOD_RUN)

    bad: list = [_node(f"b:{i:03d}") for i in range(bad_nodes)]
    bad += [_node("b:tmp")]
    bad += [
        _edge(f"b:{i:03d}", f"b:{i + 1:03d}") for i in range(bad_nodes - 1)
    ]
    bad += [
        _edge(f"b:{i:03d}", f"a:{i % 5}", "cites") for i in range(bad_nodes)
    ]
    bad += [
        _edge(f"b:{i:03d}", f"g:{i % 5}", "cites")
        for i in range(0, bad_nodes, 2)
    ]
    bad += [_author(SHARED_OWNED, SOURCE), _author(SHARED_FOREIGN, SOURCE)]
    # The failed run arrives in chunks, as the runner commits them.
    for start in range(0, len(bad), 7):
        _apply(store, bad[start:start + 7], source=SOURCE, run_id=BAD_RUN)
    _apply(
        store,
        [_node("g:5", "evidence", op="R"), _node("b:tmp", op="R")],
        source=SOURCE, run_id=BAD_RUN,
    )
    # Edges of the good run and of another source ending at failed-run nodes.
    _apply(
        store,
        [
            _edge(f"g:{i % 5}", f"b:{i:03d}", "mentions")
            for i in range(1, bad_nodes, 3)
        ],
        source=SOURCE, run_id=GOOD_RUN,
    )
    _apply(
        store,
        [
            _edge(f"a:{i % 5}", f"b:{i:03d}", "mentions")
            for i in range(0, bad_nodes, 4)
        ]
        + [_author(SHARED_OWNED, OTHER)],
        source=OTHER, run_id=OTHER,
    )


def _branch_state(
    store: ChronosGraphStore, branch: str,
) -> dict[str, list[dict]]:
    return {
        "nodes": [
            dict(row) for row in store.query(
                "SELECT * FROM okg.graph_nodes ORDER BY node_id",
                branch=branch,
            )
        ],
        "edges": [
            dict(row) for row in store.query(
                "SELECT * FROM okg.graph_edges ORDER BY edge_id",
                branch=branch,
            )
        ],
        "declarers": [
            dict(row) for row in store.query(
                "SELECT * FROM okg.graph_node_declarers "
                "ORDER BY node_id, source",
                branch=branch,
            )
        ],
    }


@contextmanager
def _recorded_apply_calls(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sizes_only: bool = False,
) -> Iterator[list]:
    """Record each apply_facts call: its facts, or only its size when the
    record must not keep the facts alive (the memory test)."""
    calls: list = []
    original = ChronosGraphStore.apply_facts

    def recording(self, facts, **kwargs):
        facts = list(facts)
        calls.append(len(facts) if sizes_only else facts)
        return original(self, facts, **kwargs)

    monkeypatch.setattr(ChronosGraphStore, "apply_facts", recording)
    try:
        yield calls
    finally:
        monkeypatch.setattr(ChronosGraphStore, "apply_facts", original)


def _retracted(facts: list) -> list[tuple]:
    """What each fact retracts: the fields apply_facts reads from an "R"
    fact (graph_store_internal/writes.py, the NodeFact and EdgeFact branches
    of its fact loop). The paged code no longer reads the rest."""
    out: list[tuple] = []
    for fact in facts:
        if isinstance(fact, NodeFact):
            out.append(("node", fact.op, fact.node_id, fact.subtype))
        else:
            out.append(("edge", fact.op, fact.src, fact.edge_type, fact.dst))
    return sorted(out)


def _two_branches(store: ChronosGraphStore) -> None:
    store.create_branch("oracle", from_branch="default")
    store.create_branch("paged", from_branch="default")


def _legacy(dsn: str) -> None:
    _legacy_retract_chronos_run(
        dsn, admission_id=None, source=SOURCE, run_id=BAD_RUN,
        branch="oracle",
    )


def _paged(dsn: str, page_rows: int) -> None:
    retract_chronos_run(
        dsn, admission_id=None, source=SOURCE, run_id=BAD_RUN,
        branch="paged", page_rows=page_rows,
    )


@pytest.mark.parametrize("page_rows", [1, 4, 10_000])
def test_paged_compensation_retracts_exactly_what_the_single_list_did(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    page_rows: int,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_mixed_run(store, bad_nodes=23)
        before = _branch_state(store, "default")
        _two_branches(store)
        with _recorded_apply_calls(monkeypatch) as legacy_calls:
            _legacy(clean_runtime)
        with _recorded_apply_calls(monkeypatch) as paged_calls:
            _paged(clean_runtime, page_rows)

        # The same retractions, each once.
        assert len(legacy_calls) == 1
        legacy_facts = legacy_calls[0]
        paged_facts = [fact for call in paged_calls for fact in call]
        assert _retracted(paged_facts) == _retracted(legacy_facts)
        assert len(paged_facts) == len(legacy_facts)
        assert all(fact.op == "R" for fact in paged_facts)
        # Every edge retraction still precedes every node retraction.
        kinds = [isinstance(fact, NodeFact) for fact in paged_facts]
        assert kinds == sorted(kinds)
        # Each call holds at most one page, and no call is wasted.
        edges = sum(isinstance(fact, EdgeFact) for fact in legacy_facts)
        nodes = len(legacy_facts) - edges
        assert max(len(call) for call in paged_calls) <= page_rows
        assert len(paged_calls) == (
            math.ceil(edges / page_rows) + math.ceil(nodes / page_rows)
        )

        # The resulting graph: every visible row of both branches is equal.
        oracle = _branch_state(store, "oracle")
        paged = _branch_state(store, "paged")
        assert paged == oracle
        # The comparison is not vacuous: the failed run's rows are gone, the
        # shared node passed to the other source, the other source's edges
        # into the failed run survive, and the default branch is untouched.
        assert _branch_state(store, "default") == before
        assert edges >= 40 and nodes >= 24
        assert not [r for r in paged["nodes"] if r.get("run_id") == BAD_RUN]
        assert not [r for r in paged["edges"] if r.get("run_id") == BAD_RUN]
        handed = [r for r in paged["nodes"] if r["node_id"] == SHARED_OWNED]
        assert [row["source"] for row in handed] == [OTHER]
        assert [
            r for r in paged["edges"]
            if r["source"] == OTHER and str(r["dst"]).startswith("b:")
        ]
        assert not [
            r for r in paged["edges"]
            if r["source"] == SOURCE and str(r["dst"]).startswith("b:")
        ]
        assert not [
            r for r in paged["declarers"]
            if r["source"] == SOURCE and r["node_id"] == SHARED_FOREIGN
        ]
    finally:
        store.close()


def test_paging_rejects_a_non_positive_page() -> None:
    with pytest.raises(ValueError, match="page_rows must be positive"):
        next(iter_run_retraction_pages(
            object(), source=SOURCE, run_id=BAD_RUN, page_rows=0,
        ))


@pytest.mark.parametrize("fail_on", ["third_page", "second_node_page"])
def test_a_failed_page_rolls_back_every_page_before_it(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    fail_on: str,
) -> None:
    """All pages share one transaction: a failure on a later page leaves the
    graph exactly as it was, not half retracted -- including after node
    pages that deleted nodes, deleted incident edges, withdrew declarations
    and handed the shared node to the other source."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_mixed_run(store, bad_nodes=23)
        before = _branch_state(store, "default")
        original = ChronosGraphStore.apply_facts
        seen: list[str] = []

        def fail_on_page(self, facts, **kwargs):
            facts = list(facts)
            kind = "node" if isinstance(facts[0], NodeFact) else "edge"
            seen.append(kind)
            if (
                fail_on == "third_page" and len(seen) == 3
            ) or (
                fail_on == "second_node_page" and seen.count("node") == 2
            ):
                raise RuntimeError("page failed")
            return original(self, facts, **kwargs)

        monkeypatch.setattr(ChronosGraphStore, "apply_facts", fail_on_page)
        with pytest.raises(RuntimeError, match="page failed"):
            retract_chronos_run(
                clean_runtime,
                admission_id=None,
                source=SOURCE,
                run_id=BAD_RUN,
                page_rows=5,
            )
        monkeypatch.setattr(ChronosGraphStore, "apply_facts", original)
        if fail_on == "third_page":
            assert seen == ["edge", "edge", "edge"]
        else:
            # Every edge page and one whole node page were applied first.
            assert seen[-2:] == ["node", "node"]
            assert seen.count("edge") >= 8
        assert _branch_state(store, "default") == before
    finally:
        store.close()


def test_paged_compensation_on_a_caller_connection_is_one_savepoint(
    clean_runtime: str,
) -> None:
    """With ``graph_conn`` the pages run in a savepoint inside the caller's
    open transaction: nothing is visible or durable until the caller
    commits, a caller rollback undoes every page, and a caller commit leaves
    the same graph as the single-list implementation."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_mixed_run(store, bad_nodes=23)
        _two_branches(store)
        before = _branch_state(store, "paged")
        _legacy(clean_runtime)
        oracle = _branch_state(store, "oracle")
        assert oracle != before

        with db_connect(clean_runtime) as caller:
            caller.execute("SELECT 1")  # the caller's transaction is open
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN, branch="paged", graph_conn=caller,
                page_rows=4,
            )
            assert caller.info.transaction_status == (
                psycopg.pq.TransactionStatus.INTRANS
            )
            # Other sessions still see nothing of the retraction.
            assert _branch_state(store, "paged") == before
            caller.rollback()
        assert _branch_state(store, "paged") == before

        with db_connect(clean_runtime) as caller:
            caller.execute("SELECT 1")
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN, branch="paged", graph_conn=caller,
                page_rows=4,
            )
            caller.commit()
        assert _branch_state(store, "paged") == oracle
    finally:
        store.close()


@pytest.mark.parametrize(
    "change", ["node_re_attributed", "edge_re_attributed", "node_deleted"],
)
def test_a_row_changed_after_the_key_read_is_refused_or_skipped(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    """Another writer changes one of the run's rows after the keys are read
    and before its page is fetched.

    A row re-attributed to a later run of the same source keeps its key but
    is no longer the failed run's: the compensation raises
    CompensationRowSetChanged and rolls back instead of retracting a partial
    set. Dropping the source/run_id filter from the page fetch would retract
    the re-attributed row, and these cases would fail.

    A row that is simply gone (as when a racing compensation of the same run
    retracted it first, #2244) is skipped: the single list's apply found it
    gone too, and the compensation completes.
    """
    import okg.substrate.ingest.compensation as compensation
    from okg.substrate.errors import CompensationRowSetChanged

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_mixed_run(store, bad_nodes=23)
        writer = ChronosGraphStore.connect(clean_runtime)
        injected: dict[str, Any] = {}

        def inject() -> None:
            if change == "node_re_attributed":
                fact: Any = _node("b:010", note="rewritten")
            elif change == "edge_re_attributed":
                fact = _edge("b:003", "b:004", note="rewritten")
            else:
                fact = _node("b:010", op="R")
            _apply(writer, [fact], source=SOURCE, run_id=GOOD_RUN)
            injected["state"] = _branch_state(writer, "default")

        class ChangingGraph:
            def __init__(self, graph: Any) -> None:
                self._graph = graph

            def query(self, sql: str, params: dict) -> list:
                if "ids" in params and "state" not in injected:
                    inject()
                return self._graph.query(sql, params)

        original_pages = compensation.iter_run_retraction_pages
        monkeypatch.setattr(
            compensation, "iter_run_retraction_pages",
            lambda graph, **kwargs: original_pages(
                ChangingGraph(graph), **kwargs,
            ),
        )

        def compensate() -> None:
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN, page_rows=4,
            )

        try:
            if change == "node_deleted":
                compensate()
            else:
                with pytest.raises(CompensationRowSetChanged) as raised:
                    compensate()
        finally:
            writer.close()
        after = _branch_state(store, "default")
        if change == "node_deleted":
            assert not [r for r in after["nodes"] if r["run_id"] == BAD_RUN]
            assert not [r for r in after["edges"] if r["run_id"] == BAD_RUN]
            assert "b:010" not in {r["node_id"] for r in after["nodes"]}
            return
        table = (
            "okg.graph_nodes" if change == "node_re_attributed"
            else "okg.graph_edges"
        )
        assert raised.value.code == "compensation_row_set_changed"
        assert raised.value.data["table"] == table
        assert raised.value.data["missing_count"] == 1
        # Rolled back: the graph is exactly what the other writer left.
        assert after == injected["state"]
        assert [r for r in after["nodes"] if r["run_id"] == BAD_RUN]
    finally:
        store.close()


LARGE_RUN_NODES = 3_000
LARGE_RUN_PAGE = 100


def _seed_large_run(store: ChronosGraphStore) -> None:
    facts: list = [
        _node(f"n:{i:06d}", payload="x" * 200)
        for i in range(LARGE_RUN_NODES)
    ]
    facts += [
        _edge(f"n:{i:06d}", f"n:{i + 1:06d}", payload="y" * 200)
        for i in range(LARGE_RUN_NODES - 1)
    ]
    for start in range(0, len(facts), 2_000):
        _apply(
            store, facts[start:start + 2_000], source=SOURCE, run_id=BAD_RUN,
        )
    _two_branches(store)


def test_paged_compensation_peak_memory_is_bounded_by_the_page(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_large_run(store)

        def peak(run) -> int:
            tracemalloc.start()
            try:
                run()
                return tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()

        # Warm both paths on a run with no rows, so one-time imports and
        # caches count against neither peak.
        for warm in (_legacy_retract_chronos_run, retract_chronos_run):
            warm(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id="run-with-no-rows", branch="default",
            )
        legacy_peak = peak(lambda: _legacy(clean_runtime))
        with _recorded_apply_calls(monkeypatch, sizes_only=True) as sizes:
            paged_peak = peak(lambda: _paged(clean_runtime, LARGE_RUN_PAGE))
        print(f"tracemalloc peak: single list {legacy_peak:,} B, "
              f"paged {paged_peak:,} B")
        assert max(sizes) == LARGE_RUN_PAGE
        assert len(sizes) == (
            math.ceil((LARGE_RUN_NODES - 1) / LARGE_RUN_PAGE)
            + math.ceil(LARGE_RUN_NODES / LARGE_RUN_PAGE)
        )
        # Sixty pages instead of one call of 5,999 facts: the peak Python
        # allocation is about one page's worth, far below the single list.
        assert paged_peak * 10 < legacy_peak, (paged_peak, legacy_peak)
        assert _branch_state(store, "paged") == _branch_state(store, "oracle")
    finally:
        store.close()


_BRANCH_LOCKS_SQL = """
    SELECT count(*)
      FROM pg_locks
     WHERE locktype = 'advisory'
       AND granted
       AND database = (SELECT oid FROM pg_database
                        WHERE datname = current_database())
       AND objsubid = 2
       AND classid::bigint = (hashtext('okg.chronos_graph')::bigint & 4294967295)
       AND objid::bigint = (hashtext(%s)::bigint & 4294967295)
"""


def test_keys_are_read_before_the_lock_and_each_statement_holds_one_page(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deterministic shape of the paged read, independent of host speed.

    The run's keys are read before the first page is applied, while no
    session holds the branch write lock; after that, reads and applies
    alternate one page at a time, and no read returns more than one page of
    rows. The lock, once the first page takes it, is held until commit.
    """
    import okg.substrate.ingest.compensation as compensation

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_large_run(store)
        events: list[tuple[str, int, int]] = []
        watcher = psycopg.connect(clean_runtime, autocommit=True)

        def branch_locks() -> int:
            return int(watcher.execute(_BRANCH_LOCKS_SQL, ("paged",)).fetchone()[0])

        class RecordingGraph:
            def __init__(self, graph: Any) -> None:
                self._graph = graph

            def query(self, sql: str, params: dict) -> list:
                rows = self._graph.query(sql, params)
                kind = "page" if "ids" in params else "keys"
                events.append((kind, len(rows), branch_locks()))
                return rows

        original_pages = compensation.iter_run_retraction_pages

        def recording_pages(graph, **kwargs):
            return original_pages(RecordingGraph(graph), **kwargs)

        original_apply = ChronosGraphStore.apply_facts

        def recording_apply(self, facts, **kwargs):
            facts = list(facts)
            events.append(("apply", len(facts), branch_locks()))
            return original_apply(self, facts, **kwargs)

        monkeypatch.setattr(
            compensation, "iter_run_retraction_pages", recording_pages,
        )
        monkeypatch.setattr(ChronosGraphStore, "apply_facts", recording_apply)
        try:
            _paged(clean_runtime, LARGE_RUN_PAGE)
        finally:
            watcher.close()

        edge_pages = math.ceil((LARGE_RUN_NODES - 1) / LARGE_RUN_PAGE)
        node_pages = math.ceil(LARGE_RUN_NODES / LARGE_RUN_PAGE)
        pages = edge_pages + node_pages
        # Both key reads come first, with the branch lock free.
        assert [kind for kind, _, _ in events[:2]] == ["keys", "keys"]
        assert [n for _, n, _ in events[:2]] == [
            LARGE_RUN_NODES - 1, LARGE_RUN_NODES,
        ]
        assert [locks for _, _, locks in events[:3]] == [0, 0, 0]
        # Then page read, apply, page read, apply ... one page each.
        body = events[2:]
        assert [kind for kind, _, _ in body] == ["page", "apply"] * pages
        assert max(n for _, n, _ in body) == LARGE_RUN_PAGE
        # The first apply takes the lock; it stays held to the commit.
        assert all(locks == 1 for _, _, locks in body[2:])
        assert _branch_state(store, "paged")["nodes"] == [
            row for row in _branch_state(store, "default")["nodes"]
            if row["run_id"] != BAD_RUN
        ]
    finally:
        store.close()


# --- The oracle: the implementation this change replaced, verbatim. -------

def _legacy_retract_chronos_run(
    dsn: str,
    *,
    admission_id: str | None,
    source: str,
    run_id: str,
    branch: str = "default",
    graph_conn: Any | None = None,
) -> None:
    """The implementation before paging, verbatim; the oracle only."""
    from okg.substrate.fact_application import FactBatchRecoveryBlocked
    from okg.substrate.fact_batch import PostgresApplicationReceiptStore
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    with db_connect(dsn) as conn:
        # This runs on a failure path while the caller may still hold rows in
        # the tables below. A bounded wait turns a lock conflict into a named
        # compensation failure instead of an unbounded hang (#802's lesson).
        # This statement bounds this routine's own connection only. The effect
        # transaction below repeats the bound with SET LOCAL, since a
        # caller-supplied graph_conn is pooled and carries no lock_timeout.
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '30s'")
        conn.commit()
        receipt_store = PostgresApplicationReceiptStore(conn)
        keys: list[tuple[Any, Any]] = []
        if admission_id is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, chunk_ordinal
                      FROM okg.fact_batch_receipts
                     WHERE admission_id = %s::uuid
                     ORDER BY chunk_ordinal
                    """,
                    (admission_id,),
                )
                keys = list(cur.fetchall())
        receipts = [
            receipt_store.get(admission_id, receipt_run_id, ordinal)
            for receipt_run_id, ordinal in keys
        ]
        if any(receipt is None for receipt in receipts):
            raise FactBatchRecoveryBlocked("compensation receipt disappeared")
        if any(receipt.state == "prepared" for receipt in receipts if receipt):
            raise FactBatchRecoveryBlocked(
                "prepared receipt must be recovered before terminal rejection"
            )
        for receipt in receipts:
            if receipt is not None and receipt.state == "applied":
                receipt_store.transition(receipt.key, "compensating")
        # The compensating state must survive a graph-retraction failure.
        conn.commit()

        # One transaction owner for the retraction and the receipt state it
        # attests (#2152). With the caller's connection that owner is the
        # caller's open transaction and `transaction()` below is a savepoint
        # inside it; without one this routine owns the single transaction.
        effect_conn = conn if graph_conn is None else graph_conn
        store = ChronosGraphStore.from_connection(
            effect_conn, ensure_schema=False, autocommit=False,
        )
        effect_receipts = PostgresApplicationReceiptStore(effect_conn)
        graph = store.checkout(branch)
        facts: list[NodeFact | EdgeFact] = []
        for row in graph.query(
            """
            SELECT src, dst, edge_type, attrs, provenance, confidence,
                   source_record_id, source_revision
              FROM okg.graph_edges
             WHERE source = :source
               AND run_id = :run_id
            """,
            {"source": source, "run_id": run_id},
        ):
            facts.append(
                EdgeFact(
                    src=str(row["src"]),
                    dst=str(row["dst"]),
                    edge_type=str(row["edge_type"]),
                    attrs=dict(row["attrs"] or {}),
                    provenance=str(row["provenance"] or "explicit"),
                    confidence=float(row["confidence"] or 1.0),
                    source_record_id=dict(row["source_record_id"] or {}),
                    source_revision=dict(row["source_revision"] or {}),
                    op="R",
                )
            )
        for row in graph.query(
            """
            SELECT node_id, subtype, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE source = :source
               AND run_id = :run_id
            """,
            {"source": source, "run_id": run_id},
        ):
            facts.append(
                NodeFact(
                    node_id=str(row["node_id"]),
                    subtype=str(row["subtype"]),
                    attrs={},
                    source_record_id=dict(row["source_record_id"] or {}),
                    source_revision=dict(row["source_revision"] or {}),
                    op="R",
                )
            )
        with effect_conn.transaction():
            # effect_conn may be the caller's pooled connection, which has no
            # lock_timeout of its own; bound the FOR UPDATE that
            # transition() takes below the same way (#802).
            with effect_conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '30s'")
            if facts:
                store.apply_facts(
                    facts,
                    branch=branch,
                    source=source,
                    run_id=run_id,
                )
            # The run's declarations of shared nodes it did not own have no
            # graph row to retract; delete them, or a later hand-off would
            # write the failed run's content (issue #1901).
            store.withdraw_node_declarations(
                branch=branch,
                sources=[source],
                run_id=run_id,
            )
            for receipt in receipts:
                if receipt is None:
                    continue
                current = effect_receipts.get(*receipt.key)
                if current is not None and current.state == "compensating":
                    effect_receipts.transition(current.key, "compensated")
