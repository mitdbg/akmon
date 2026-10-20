"""The record-set coverage proof must not scale with the source's graph (#1479).

A scope-complete record-set run declares its whole footprint every time, so
the proof that the declaration matches the graph is paid on every run, not
only on runs that changed something. These tests pin three things:

* a PASSING proof reads no graph identity rows at all;
* a FAILING proof still refuses with the exact counts, samples and repair
  advice it produced before the bounded path existed;
* the bounded path and the hydrating path reach the SAME verdict on every
  shape the ownership contract distinguishes -- that equivalence is the whole
  change, so it is tested differentially rather than argued.

Kept out of `test_source_runner_record_set.py` deliberately: three rows of
that file fail in the nightly `ci-substrate-db-full` run on dev, so it cannot
be enrolled in a blocking lane yet.
"""
from __future__ import annotations

import hashlib
import tracemalloc
import uuid
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.ingest import record_authority_coverage as private_coverage
from okg.substrate.ingest.change_identity import PipelineIdentitySnapshot
from okg.substrate.ingest.record_authority import (
    AuthorityError,
    ProductionAuthorityStore,
    canonical,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordAuthorityOperation,
    RecordAuthorityStream,
    RecordNode,
    RecordSet,
)
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

# Columns that only a query hydrating graph identities returns. A counting
# query answers with an aggregate column instead, so classifying on the ROW
# SHAPE -- not on the SQL text -- keeps the counter honest if the queries are
# ever reworded.
_IDENTITY_COLUMNS = frozenset({"node_id", "src", "edge_type", "dst"})


class _RowCountingSession:
    """Proxy that records how many graph identity rows a check hydrates.

    `hydrated_rows` must scale with the number of MISMATCHES, not with the
    number of rows the source already owns.
    """

    def __init__(self, session: Any) -> None:
        self._session = session
        self.hydrated_rows = 0
        self.hydrating_queries = 0
        self.aggregate_queries = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    def query(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        rows = self._session.query(sql, params)
        if rows and _IDENTITY_COLUMNS & set(rows[0]):
            self.hydrating_queries += 1
            self.hydrated_rows += len(rows)
        else:
            self.aggregate_queries += 1
        return rows


def _counting_runner(
    dsn: str,
    source: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[SourceRunner, list[_RowCountingSession]]:
    runner = SourceRunner(dsn, source_name=source)
    real_checkout = runner._checkout_graph_branch
    sessions: list[_RowCountingSession] = []

    def _counting_checkout(graph_store: Any) -> Any:
        session = _RowCountingSession(real_checkout(graph_store))
        sessions.append(session)
        return session

    monkeypatch.setattr(runner, "_checkout_graph_branch", _counting_checkout)
    return runner, sessions


def _node(node_id: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype="concept",
        attrs={"text": node_id},
        source_record_id={"id": node_id},
        source_revision={"rev": "stable"},
    )


def _edge(src: str, edge_type: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        source_record_id={"src": src, "edge_type": edge_type, "dst": dst},
        source_revision={"rev": "stable"},
    )


def _apply(
    dsn: str,
    source: str,
    node_ids: list[str] = [],
    edges: list[tuple[str, str, str]] = [],
) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        facts: list[Any] = [_node(node_id) for node_id in node_ids]
        facts.extend(_edge(src, edge_type, dst) for src, edge_type, dst in edges)
        ChronosGraphStore.from_connection(conn).apply_facts(facts, source=source)


def _record_set(
    node_ids: list[str],
    *,
    ownership_mode: str = "authoritative",
    edges: list[tuple[str, str, str]] = [],
) -> RecordSet:
    records = {
        node_id: RecordEmission(
            primary=RecordNode(node_id, "concept", ownership_mode=ownership_mode)
        )
        for node_id in node_ids
    }
    for src, edge_type, dst in edges:
        records[src] = RecordEmission(
            primary=records[src].primary,
            edges=[
                *records[src].edges,
                EdgeKey(src=src, edge_type=edge_type, dst=dst),
            ],
        )
    return RecordSet(records=records)


@pytest.mark.parametrize("visible_nodes", [40, 400])
def test_record_set_coverage_hydrates_no_rows_when_graph_agrees(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    visible_nodes: int,
) -> None:
    """A passing coverage proof must not read the source's visible rows.

    Before this change the proof hydrated every declared node and every
    source-owned node, so the client retained O(visible rows) -- it read
    exactly `visible_nodes` rows here. The assertion is on rows actually
    returned by the real Chronos session, at two graph sizes an order of
    magnitude apart, so an implementation that reads a CONSTANT sample would
    also be caught: the required count is zero, not "the same at both sizes".
    """
    source = f"test.record-set-bounded-{visible_nodes}"
    node_ids = [f"doc:{index:06d}" for index in range(visible_nodes)]
    edges = [("doc:000000", "relates_to", "doc:000001")]
    _apply(clean_runtime, source, node_ids, edges)

    runner, sessions = _counting_runner(clean_runtime, source, monkeypatch)
    runner._verify_record_set_chronos_coverage(
        _record_set(node_ids, edges=edges)
    )

    assert len(sessions) == 1
    assert sessions[0].hydrated_rows == 0
    assert sessions[0].hydrating_queries == 0
    assert sessions[0].aggregate_queries == 4


def test_record_set_coverage_stale_refusal_keeps_exact_diagnostics(
    clean_runtime: str,
) -> None:
    """A mismatch still refuses with the same counts, samples and hint."""
    source = "test.record-set-bounded-stale"
    declared = [f"doc:{index:06d}" for index in range(5)]
    stale = [f"doc:stale-{index:03d}" for index in range(12)]
    _apply(clean_runtime, source, declared + stale)

    runner = SourceRunner(clean_runtime, source_name=source)
    with pytest.raises(SubstrateError) as excinfo:
        runner._verify_record_set_chronos_coverage(_record_set(declared))

    error = excinfo.value
    assert "record_set coverage check failed" in str(error)
    data = error.data
    assert data["source_name"] == source
    assert data["missing_node_count"] == 0
    assert data["missing_edge_count"] == 0
    # `_record_set_stale_graph_nodes` reads a LIMIT-10 sample, so the
    # reported stale count stays the historical capped lower bound.
    assert data["stale_node_count"] == 10
    assert data["stale_edge_count"] == 0
    assert data["stale_counts_exact"] is False
    assert data["coverage_class"] == "stale"
    assert data["stale_nodes_sample"] == sorted(stale)[:10]
    assert "cursor reset" in error.fix_hint


def test_record_set_coverage_missing_node_refusal_is_unchanged(
    clean_runtime: str,
) -> None:
    source = "test.record-set-bounded-missing"
    declared = [f"doc:{index:06d}" for index in range(4)]
    _apply(clean_runtime, source, declared[:2])

    runner = SourceRunner(clean_runtime, source_name=source)
    with pytest.raises(SubstrateError) as excinfo:
        runner._verify_record_set_chronos_coverage(_record_set(declared))

    data = excinfo.value.data
    assert data["missing_node_count"] == 2
    assert data["stale_node_count"] == 0
    assert data["coverage_class"] == "missing"
    assert data["missing_nodes_sample"] == declared[2:]


def test_record_set_coverage_stale_edge_refusal_hydrates_only_on_mismatch(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A declared edge the source owns but did not re-emit still refuses.

    Also pins the OTHER half of the bounded-memory claim: hydration happens
    on the mismatch path, so the row count there is not required to be zero.
    """
    source = "test.record-set-bounded-stale-edge"
    nodes = ["doc:a", "doc:b", "doc:c"]
    _apply(
        clean_runtime,
        source,
        nodes,
        [("doc:a", "relates_to", "doc:b"), ("doc:a", "relates_to", "doc:c")],
    )

    runner, sessions = _counting_runner(clean_runtime, source, monkeypatch)
    with pytest.raises(SubstrateError) as excinfo:
        runner._verify_record_set_chronos_coverage(
            _record_set(nodes, edges=[("doc:a", "relates_to", "doc:b")])
        )

    data = excinfo.value.data
    assert data["missing_node_count"] == 0
    assert data["missing_edge_count"] == 0
    assert data["stale_edge_count"] == 1
    assert data["stale_edges_sample"] == [
        {"src": "doc:a", "edge_type": "relates_to", "dst": "doc:c"}
    ]
    assert sessions[0].hydrating_queries > 0


# Each case is (label, graph nodes, graph edges, declaration, refuses?).
_EQUIVALENCE_CASES = [
    ("clean-nodes-and-edges", ["n:1", "n:2"], [("n:1", "rel", "n:2")],
     (["n:1", "n:2"], "authoritative", [("n:1", "rel", "n:2")]), False),
    ("missing-edge", ["n:1", "n:2"], [],
     (["n:1", "n:2"], "authoritative", [("n:1", "rel", "n:2")]), True),
    ("stale-node", ["n:1", "n:2"], [],
     (["n:1"], "authoritative", []), True),
    ("empty-declaration-with-owned-rows", ["n:1"], [],
     ([], "authoritative", []), True),
    ("substitute-present", ["n:1"], [],
     (["n:1"], "substitute", []), False),
    ("substitute-absent", [], [],
     (["n:1"], "substitute", []), True),
]


@pytest.mark.parametrize(
    "label,graph_nodes,graph_edges,declaration,refuses",
    _EQUIVALENCE_CASES,
    ids=[case[0] for case in _EQUIVALENCE_CASES],
)
def test_bounded_and_hydrating_coverage_proofs_agree(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
    graph_nodes: list[str],
    graph_edges: list[tuple[str, str, str]],
    declaration: tuple[list[str], str, list[tuple[str, str, str]]],
    refuses: bool,
) -> None:
    """The two proofs must reach the same verdict on the same graph.

    The bounded path is only sound if it accepts exactly what the hydrating
    path accepts. Forcing the fast path to decline turns the SAME fixture
    into a run of the original code, so this compares implementations rather
    than restating one of them.
    """
    source = f"test.record-set-equivalence-{label}"
    node_ids, ownership_mode, edges = declaration
    _apply(clean_runtime, source, graph_nodes, graph_edges)
    record_set = _record_set(
        node_ids,
        ownership_mode=ownership_mode,
        edges=edges,
    )

    def _verdict(force_hydrating: bool) -> str | None:
        runner = SourceRunner(clean_runtime, source_name=source)
        if force_hydrating:
            monkeypatch.setattr(
                runner,
                "_record_set_coverage_counts_agree",
                lambda *args, **kwargs: False,
            )
        try:
            runner._verify_record_set_chronos_coverage(record_set)
        except SubstrateError as exc:
            return str(exc.data["coverage_class"])
        return None

    bounded = _verdict(force_hydrating=False)
    hydrating = _verdict(force_hydrating=True)

    assert bounded == hydrating
    assert (bounded is not None) is refuses


def test_substitute_node_owned_by_another_source_is_covered(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A substitute identity counts as present under any owner.

    The same identity declared as authoritative must NOT, because this source
    does not own the row. This asymmetry is the reason the bounded path needs
    two separate presence counts rather than one.
    """
    source = "test.record-set-bounded-substitute"
    node_id = "shared:node"
    _apply(clean_runtime, "test.record-set-bounded-other-owner", [node_id])

    runner, sessions = _counting_runner(clean_runtime, source, monkeypatch)
    runner._verify_record_set_chronos_coverage(
        _record_set([node_id], ownership_mode="substitute")
    )
    assert sessions[0].hydrated_rows == 0

    with pytest.raises(SubstrateError, match="record_set coverage"):
        SourceRunner(
            clean_runtime,
            source_name=source,
        )._verify_record_set_chronos_coverage(_record_set([node_id]))


# Final private coverage has a fresh post-apply snapshot and cannot be reused
# as acceptance, deletion or cursor authority.


def _pipeline(*canonicalizers):
    return PipelineIdentitySnapshot.build(
        canonicalizers=tuple(canonicalizers), volatile_fields=(),
        nomos_policy_fingerprint=None, pii_posture=None,
        registry_binding_fingerprint="test-registry", transform_identity={},
    )


@pytest.fixture
def coverage_context(clean_runtime):
    source = "test.private-coverage"
    _apply(clean_runtime, source)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        incarnation = str(conn.execute(
            "SELECT created_at FROM _chronos_branch_interval_branches "
            "WHERE branch_id='default'"
        ).fetchone()[0])
        yield conn, ProductionAuthorityStore(conn), {
            "expected_source": source, "expected_incarnation": incarnation,
            "expected_attempt": uuid.uuid4().hex, "graph_branch": "default",
            "normalization_context": _pipeline(),
        }


def _private_candidate(store, bindings, record_set):
    operations = []
    components = 0
    for ordinal, (record_id, record) in enumerate(sorted(record_set.records.items())):
        row = record.to_cursor()
        operations.append(RecordAuthorityOperation(
            ordinal=ordinal, record_id=record_id, primary=row["primary"],
            descendants=tuple(row["descendants"]), edges=tuple(row["edges"]),
        ))
        components += int(row["primary"] is not None) + len(row["descendants"]) + len(row["edges"])
    stream = RecordAuthorityStream(
        operations=operations,
        logical_sha256=hashlib.sha256(canonical(record_set.to_cursor()).encode()).hexdigest(),
        record_count=len(record_set.records), operation_count=len(operations),
        component_count=components,
    )
    return store.stage_stream(bindings["expected_source"], bindings["expected_incarnation"],
                              bindings["expected_attempt"], stream)


def _assert_coverage_clean(conn):
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    assert conn.execute(
        "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
        "AND relname LIKE '_okg_private_coverage_%'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT count(*) FROM pg_cursors WHERE name LIKE 'authority_identity_rows_%'"
    ).fetchone()[0] == 0


def test_private_coverage_observes_committed_graph_and_returns_no_proof(coverage_context, clean_runtime):
    conn, store, bindings = coverage_context
    records = _record_set(["doc:a", "doc:b"], edges=[("doc:a", "relates_to", "doc:b")])
    candidate = _private_candidate(store, bindings, records)
    _apply(clean_runtime, bindings["expected_source"], ["doc:a", "doc:b"],
           [("doc:a", "relates_to", "doc:b")])
    assert private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings) is None
    _assert_coverage_clean(conn)
    assert conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_refs WHERE authority_id=%s",
        (candidate.authority,),
    ).fetchone()[0] == 0
    assert store.seal_and_bind(candidate).sha256 == candidate.expected_sha256


def test_private_coverage_empty_authority_and_graph(coverage_context):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, RecordSet(records={}))
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
def test_private_coverage_refuses_caller_transaction_without_touching_it(
    coverage_context, clean_runtime, isolation,
):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, _record_set(["doc:new"]))
    with conn.transaction():
        conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}")
        transaction = conn.execute("SELECT txid_current()").fetchone()[0]
        conn.execute("CREATE TEMP TABLE caller_owned(value int) ON COMMIT DROP")
        conn.execute("INSERT INTO caller_owned VALUES (17)")
        _apply(clean_runtime, bindings["expected_source"], ["doc:new"])
        with pytest.raises(AuthorityError, match="idle"):
            private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS
        assert conn.execute("SELECT txid_current()").fetchone()[0] == transaction
        assert conn.execute("SELECT value FROM caller_owned").fetchone() == (17,)
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)


@pytest.mark.parametrize("field", ["expected_source", "expected_incarnation", "expected_attempt"])
def test_private_coverage_independent_bindings_refuse_before_graph_reads(coverage_context, field):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, RecordSet(records={}))
    with pytest.raises(AuthorityError, match="binding"):
        private_coverage.assert_private_post_apply_coverage(
            store, candidate, **{**bindings, field: "wrong-independent-value"},
        )
    _assert_coverage_clean(conn)


def test_private_coverage_checks_actual_branch_incarnation_even_when_candidate_matches(coverage_context):
    conn, store, bindings = coverage_context
    stale = {**bindings, "expected_incarnation": "old-incarnation"}
    candidate = _private_candidate(store, stale, RecordSet(records={}))
    with pytest.raises(AuthorityError, match="incarnation"):
        private_coverage.assert_private_post_apply_coverage(store, candidate, **stale)
    _assert_coverage_clean(conn)


def test_private_coverage_missing_and_wrong_existing_branch_refuse(coverage_context):
    from chronos_core.branching import BranchNotFoundError

    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, RecordSet(records={}))
    graph = ChronosGraphStore.from_connection(conn)
    graph.ensure_branch("another-coverage-branch", from_branch="default")
    for branch, error in (("absent-coverage-branch", BranchNotFoundError),
                          ("another-coverage-branch", AuthorityError)):
        with pytest.raises(error):
            private_coverage.assert_private_post_apply_coverage(
                store, candidate, **{**bindings, "graph_branch": branch},
            )
        _assert_coverage_clean(conn)
    assert conn.execute(
        "SELECT count(*) FROM _chronos_branch_interval_branches WHERE branch_id=%s",
        ("absent-coverage-branch",),
    ).fetchone()[0] == 0


def test_private_coverage_revalidates_candidate_after_prior_read(coverage_context):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, _record_set(["doc:a"]))
    expected = {k: v for k, v in bindings.items() if k.startswith("expected_")}
    with store.private_identity_reader(candidate, **expected):
        pass
    conn.execute("SET session_replication_role='replica'")
    try:
        conn.execute("DELETE FROM okg.source_record_authority_identities WHERE authority_id=%s",
                     (candidate.authority,))
    finally:
        conn.execute("SET session_replication_role='origin'")
    with pytest.raises(AuthorityError, match="identity index"):
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)


def test_private_coverage_substitute_and_cross_source_edge_are_covered(coverage_context, clean_runtime):
    conn, store, bindings = coverage_context
    records = _record_set(["shared:a", "shared:b"], ownership_mode="substitute",
                          edges=[("shared:a", "relates_to", "shared:b")])
    candidate = _private_candidate(store, bindings, records)
    _apply(clean_runtime, "other-owner", ["shared:a", "shared:b"],
           [("shared:a", "relates_to", "shared:b")])
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)


def test_private_coverage_exact_counts_and_bounded_sorted_samples(coverage_context, clean_runtime):
    conn, store, bindings = coverage_context
    missing = [f"doc:missing-{i:03d}" for i in range(13)]
    stale = [f"doc:stale-{i:03d}" for i in range(14)]
    declared = ["doc:a", "doc:b", *missing]
    candidate = _private_candidate(store, bindings, _record_set(
        declared, edges=[("doc:a", "relates_to", "doc:b")],
    ))
    _apply(clean_runtime, bindings["expected_source"], ["doc:a", *stale],
           [("doc:stale-000", "relates_to", "doc:stale-001")])
    _apply(clean_runtime, "other-owner", ["doc:b"])
    with pytest.raises(SubstrateError, match="coverage check failed") as error:
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    data = error.value.data
    assert {k: data[k] for k in ("missing_node_count", "missing_edge_count", "stale_node_count", "stale_edge_count")} == {
        "missing_node_count": 14, "missing_edge_count": 1,
        "stale_node_count": 14, "stale_edge_count": 1,
    }
    assert data["stale_counts_exact"] is True
    assert data["missing_nodes_sample"] == sorted(["doc:b", *missing])[:10]
    assert data["stale_nodes_sample"] == stale[:10]
    assert data["missing_edges_sample"] == [{"src": "doc:a", "edge_type": "relates_to", "dst": "doc:b"}]
    _assert_coverage_clean(conn)


def test_private_coverage_success_hydrates_no_graph_identity(coverage_context, clean_runtime, monkeypatch):
    _conn, store, bindings = coverage_context
    nodes = [f"doc:{i:03d}" for i in range(150)]
    candidate = _private_candidate(store, bindings, _record_set(nodes))
    _apply(clean_runtime, bindings["expected_source"], nodes)
    original = ChronosGraphStore.checkout
    observed = []

    def checkout(graph, branch):
        session = _RowCountingSession(original(graph, branch))
        observed.append(session)
        return session

    monkeypatch.setattr(ChronosGraphStore, "checkout", checkout)
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    assert len(observed) == 1
    assert observed[0].hydrated_rows == 0
    assert observed[0].aggregate_queries == 6  # branch binding + conflict count + four comparisons


class _MapCanonicalizer:
    id_family = "raw"

    def __init__(self, mapping):
        self.mapping = mapping

    def canonicalize(self, raw_id):
        return self.mapping.get(raw_id, raw_id)


def test_private_coverage_uses_captured_chain_and_normalizes_edge_endpoints(
    coverage_context, clean_runtime,
):
    conn, store, bindings = coverage_context
    mapping = {"raw:a": "doc:α", "raw:b": "doc:β"}
    first = _MapCanonicalizer(mapping)
    bindings["normalization_context"] = _pipeline(first, _MapCanonicalizer({"raw:a": "doc:wrong"}))
    mapping["raw:a"] = "doc:mutated-after-capture"
    candidate = _private_candidate(store, bindings, _record_set(
        ["raw:a", "raw:b"], edges=[("raw:a", "relates_to", "raw:b")],
    ))
    _apply(clean_runtime, bindings["expected_source"], ["doc:α", "doc:β"],
           [("doc:α", "relates_to", "doc:β")])
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)


@pytest.mark.parametrize("conflict", [False, True])
def test_private_coverage_canonical_ownership_is_checked_across_batches(
    coverage_context, clean_runtime, monkeypatch, conflict,
):
    conn, store, bindings = coverage_context
    bindings["normalization_context"] = _pipeline(_MapCanonicalizer({"raw:a": "doc:same", "raw:b": "doc:same"}))
    records = RecordSet(records={
        "a": RecordEmission(primary=RecordNode("raw:a", "concept")),
        "b": RecordEmission(primary=RecordNode(
            "raw:b", "concept", ownership_mode="substitute" if conflict else "authoritative",
        )),
    })
    candidate = _private_candidate(store, bindings, records)
    _apply(clean_runtime, bindings["expected_source"], ["doc:same"])
    monkeypatch.setattr(private_coverage, "READ_BATCH", 1)
    if conflict:
        with pytest.raises(SubstrateError, match="conflicting normalized") as error:
            private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
        assert error.value.data["ownership_conflict_count"] == 1
        assert error.value.data["nodes_sample"] == ["doc:same"]
    else:
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)


def test_private_coverage_does_not_add_a_text_index_identity_limit(
    coverage_context, clean_runtime, monkeypatch,
):
    conn, store, bindings = coverage_context
    normalized = "doc:" + "x" * 4096
    bindings["normalization_context"] = _pipeline(_MapCanonicalizer({"raw:a": normalized}))
    candidate = _private_candidate(store, bindings, _record_set(["raw:a"]))
    _apply(clean_runtime, bindings["expected_source"], [normalized])
    probe = _CoverageConnectionProbe(conn)
    monkeypatch.setattr(private_coverage, "STAGE_BYTES", 1024)
    private_coverage.assert_private_post_apply_coverage(ProductionAuthorityStore(probe), candidate, **bindings)
    assert any(total > private_coverage.STAGE_BYTES for total, _widest in probe.coverage_copy_bytes)
    assert all(total <= private_coverage.STAGE_BYTES + widest
               for total, widest in probe.coverage_copy_bytes)
    _assert_coverage_clean(conn)


@pytest.mark.parametrize("invalid", [None, "", 42])
def test_private_coverage_rejects_malformed_canonicalizer_output(coverage_context, invalid):
    conn, store, bindings = coverage_context
    bindings["normalization_context"] = _pipeline(_MapCanonicalizer({"raw:a": invalid}))
    candidate = _private_candidate(store, bindings, _record_set(["raw:a"]))
    with pytest.raises(AuthorityError, match="canonicalizer"):
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)
    assert store.seal_and_bind(candidate).count == 1


def test_private_coverage_requires_pipeline_snapshot_integrity_at_entry_and_exit(
    coverage_context, monkeypatch,
):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, RecordSet(records={}))
    with pytest.raises(AuthorityError, match="pipeline snapshot"):
        private_coverage.assert_private_post_apply_coverage(
            store, candidate, **{**bindings, "normalization_context": replace(
                bindings["normalization_context"], fingerprint="wrong",
            )},
        )
    with monkeypatch.context() as patch:
        intact_answers = iter((True, False))
        patch.setattr(PipelineIdentitySnapshot, "is_intact", lambda _self: next(intact_answers))
        with pytest.raises(AuthorityError, match="pipeline snapshot"):
            private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)


def test_private_coverage_retains_one_snapshot_when_other_connection_commits(
    coverage_context, clean_runtime, monkeypatch,
):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, _record_set(["doc:before"]))
    _apply(clean_runtime, bindings["expected_source"], ["doc:before"])
    original = private_coverage._stage

    def stage_then_commit(*args):
        original(*args)
        # Another connection's commit occurs after verification established
        # the snapshot, but before graph comparison. It must remain invisible.
        _apply(clean_runtime, bindings["expected_source"], ["doc:after"])

    with monkeypatch.context() as patch:
        patch.setattr(private_coverage, "_stage", stage_then_commit)
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)
    with pytest.raises(SubstrateError, match="coverage check failed") as error:
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    assert error.value.data["stale_node_count"] == 1
    assert error.value.data["stale_nodes_sample"] == ["doc:after"]


class _CoverageCursorProbe:
    def __init__(self, cursor, owner, *, name=""):
        self.cursor, self.owner, self.name = cursor, owner, name

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    @property
    def itersize(self):
        return self.cursor.itersize

    @itersize.setter
    def itersize(self, value):
        self.cursor.itersize = value

    def __enter__(self):
        self.cursor.__enter__()
        return self

    def __exit__(self, *args):
        return self.cursor.__exit__(*args)

    def __iter__(self):
        return self

    def __next__(self):
        assert not self.owner.copy_active, "server cursor advanced during COPY"
        row = next(self.cursor)
        if self.name:
            self.owner.named_advances += 1
        if self.name.startswith("authority_identity_rows_"):
            self.owner.pending_identities += 1
            self.owner.max_pending_identities = max(
                self.owner.max_pending_identities, self.owner.pending_identities,
            )
        return row

    @contextmanager
    def copy(self, sql, *args, **kwargs):
        assert not self.owner.copy_active
        self.owner.copy_active = True
        self.owner.copy_calls += 1
        coverage_copy = "_okg_private_coverage_" in str(sql)
        measure = [0, 0]  # total encoded bytes, widest encoded row
        try:
            with self.cursor.copy(sql, *args, **kwargs) as copier:
                if coverage_copy:
                    owner = self.owner

                    class MeasuredCopy:
                        def write_row(self, row):
                            size = sum(len(v.encode("utf-8")) if isinstance(v, str) else 8 for v in row)
                            measure[0] += size
                            measure[1] = max(measure[1], size)
                            copier.write_row(row)
                            owner.pending_identities -= 1

                    yield MeasuredCopy()
                else:
                    yield copier
        finally:
            self.owner.copy_active = False
            if coverage_copy:
                self.owner.coverage_copy_bytes.append(tuple(measure))


class _CoverageConnectionProbe:
    """Real connection probe: deny implicit transaction control and COPY overlap."""
    def __init__(self, conn):
        self.conn = conn
        self.copy_active = False
        self.copy_calls = 0
        self.named_advances = 0
        self.explicit_commits = self.explicit_rollbacks = 0
        self.pending_identities = self.max_pending_identities = 0
        self.coverage_copy_bytes = []

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def cursor(self, *args, **kwargs):
        return _CoverageCursorProbe(self.conn.cursor(*args, **kwargs), self,
                                    name=kwargs.get("name", ""))

    def commit(self):
        self.explicit_commits += 1
        raise AssertionError("borrowed graph session committed the authority transaction")

    def rollback(self):
        self.explicit_rollbacks += 1
        raise AssertionError("borrowed graph session rolled back the authority transaction")


def test_private_coverage_bounds_memory_and_never_fetches_during_copy(
    coverage_context, clean_runtime, monkeypatch,
):
    conn, store, bindings = coverage_context
    nodes = [f"doc:{i:04d}:" + "z" * 120 for i in range(600)]
    candidate = _private_candidate(store, bindings, _record_set(nodes))
    _apply(clean_runtime, bindings["expected_source"], nodes)
    probe = _CoverageConnectionProbe(conn)
    measured = ProductionAuthorityStore(probe)
    monkeypatch.setattr(private_coverage, "READ_BATCH", 16)
    monkeypatch.setattr(private_coverage, "STAGE_BYTES", 1024)
    tracemalloc.start()
    try:
        private_coverage.assert_private_post_apply_coverage(measured, candidate, **bindings)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 4 * 1024 * 1024, peak
    assert probe.named_advances > 600
    assert probe.copy_calls > 30
    assert probe.max_pending_identities <= private_coverage.READ_BATCH
    assert probe.pending_identities == 0
    assert probe.coverage_copy_bytes
    assert all(total <= private_coverage.STAGE_BYTES + widest
               for total, widest in probe.coverage_copy_bytes)
    assert probe.explicit_commits == probe.explicit_rollbacks == 0
    assert not probe.copy_active
    _assert_coverage_clean(conn)


@pytest.mark.parametrize("failure", ["normalization", "copy", "query"])
def test_private_coverage_failure_cleanup_releases_exclusion_and_preserves_transaction_owner(
    coverage_context, clean_runtime, monkeypatch, failure,
):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, _record_set(["doc:a"]))
    _apply(clean_runtime, bindings["expected_source"], ["doc:a"])
    probe = _CoverageConnectionProbe(conn)
    measured = ProductionAuthorityStore(probe)

    def reject_normalization(*_args):
        raise RuntimeError("normalization failed")

    def reject_copy(connection, _nodes, _edges):
        with connection.cursor() as cursor:
            with cursor.copy(f"COPY {private_coverage._NODES_RAW} FROM STDIN") as copier:
                copier.write_row(("doc:a", "authoritative"))
                raise RuntimeError("copy consumer failed")

    def reject_query(*_args):
        conn.execute("SELECT 1 / 0")

    targets = {
        "normalization": ("_normalized_id", reject_normalization),
        "copy": ("_copy_batch", reject_copy),
        "query": ("_compare", reject_query),
    }
    with monkeypatch.context() as patch:
        patch.setattr(private_coverage, *targets[failure])
        with pytest.raises((RuntimeError, SubstrateError)):
            private_coverage.assert_private_post_apply_coverage(measured, candidate, **bindings)
    assert probe.explicit_commits == probe.explicit_rollbacks == 0
    assert not probe.copy_active
    _assert_coverage_clean(conn)
    private_coverage.assert_private_post_apply_coverage(measured, candidate, **bindings)
    assert measured.seal_and_bind(candidate).count == 1


def test_private_coverage_excludes_other_store_during_verification_and_comparison(
    coverage_context, monkeypatch,
):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, RecordSet(records={}))
    second = ProductionAuthorityStore(conn)
    seen = []

    def check_exclusion(phase):
        with pytest.raises(AuthorityError, match="active verified reader"):
            second.seal_and_bind(candidate)
        with pytest.raises(AuthorityError, match="active verified reader"):
            second.begin("other", "incarnation", "attempt")
        seen.append(phase)

    verify, compare = store._verify_candidate, private_coverage._compare

    def checked_verify(candidate):
        check_exclusion("verification")
        return verify(candidate)

    def checked_compare(*args):
        check_exclusion("comparison")
        return compare(*args)

    monkeypatch.setattr(store, "_verify_candidate", checked_verify)
    monkeypatch.setattr(private_coverage, "_compare", checked_compare)
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    assert seen == ["verification", "comparison"]
    _assert_coverage_clean(conn)
    assert second.seal_and_bind(candidate).count == 0


@pytest.mark.parametrize("answer", [[], [{"count": True}], [{"count": -1}], [{"count": "0"}], [{"other": 0}]])
def test_private_coverage_malformed_graph_count_refuses_and_cleans_up(
    coverage_context, monkeypatch, answer,
):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, RecordSet(records={}))
    checkout = ChronosGraphStore.checkout

    def bad_checkout(graph, branch):
        session = checkout(graph, branch)
        query = session.query
        session.query = lambda sql, params=None: answer if "AS count" in sql else query(sql, params)
        return session

    with monkeypatch.context() as patch:
        patch.setattr(ChronosGraphStore, "checkout", bad_checkout)
        with pytest.raises(SubstrateError, match="invalid count"):
            private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)
    private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)


def test_private_coverage_full_edge_tuples_survive_forced_hash_collisions(
    coverage_context, clean_runtime, monkeypatch,
):
    import okg.substrate.graph_store_internal.writes as graph_writes

    conn, store, bindings = coverage_context
    monkeypatch.setattr(private_coverage, "_edge_id", lambda *_args: 77)
    monkeypatch.setattr(graph_writes, "edge_id", lambda *_args: 77)
    nodes = ["doc:a", "doc:b", "doc:c"]
    edges = [("doc:a", "relates_to", "doc:b"), ("doc:a", "relates_to", "doc:c"),
             ("doc:b", "relates_to", "doc:c")]
    candidate = _private_candidate(store, bindings, _record_set(nodes, edges=edges))
    _apply(clean_runtime, bindings["expected_source"], nodes, edges[:1])
    with pytest.raises(SubstrateError, match="coverage check failed") as error:
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    assert error.value.data["missing_edge_count"] == 2
    assert error.value.data["stale_edge_count"] == 0
    assert error.value.data["missing_edges_sample"] == [
        {"src": src, "edge_type": kind, "dst": dst} for src, kind, dst in edges[1:]
    ]
    _assert_coverage_clean(conn)


def test_private_coverage_duplicate_visible_rows_cannot_compensate_for_missing_nodes(
    coverage_context, clean_runtime,
):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, _record_set(["doc:a", "doc:missing"]))
    _apply(clean_runtime, bindings["expected_source"], ["doc:a", "doc:stale"])
    physical = conn.execute(
        "SELECT physical_table FROM _chronos_branch_tables WHERE table_name='okg.graph_nodes'"
    ).fetchone()[0]
    table = psycopg.sql.Identifier(*physical.split("."))
    columns = [column.name for column in conn.execute(
        psycopg.sql.SQL("SELECT * FROM {} LIMIT 0").format(table)
    ).description]
    # Deliberate physical corruption creates overlapping intervals. Witness
    # the duplicated branch-visible rows before asking coverage to judge them.
    conn.execute(psycopg.sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {} "
                                "WHERE node_id IN ('doc:a', 'doc:stale')").format(
        table, psycopg.sql.SQL(", ").join(map(psycopg.sql.Identifier, columns)),
        psycopg.sql.SQL(", ").join(
            psycopg.sql.SQL("live_lo - 1") if column == "live_lo"
            else psycopg.sql.Identifier(column) for column in columns
        ), table,
    ))
    session = ChronosGraphStore.from_connection(conn).checkout("default")
    rows = session.query("SELECT node_id FROM okg.graph_nodes WHERE source=:source",
                         {"source": bindings["expected_source"]})
    assert sorted(row["node_id"] for row in rows) == ["doc:a", "doc:a", "doc:stale", "doc:stale"]
    with pytest.raises(SubstrateError, match="coverage check failed") as error:
        private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    assert error.value.data["missing_node_count"] == 1
    assert error.value.data["stale_node_count"] == 1
    _assert_coverage_clean(conn)


def test_private_coverage_accepts_idle_non_autocommit_connection(coverage_context):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, RecordSet(records={}))
    conn.autocommit = False
    probe = _CoverageConnectionProbe(conn)
    private_coverage.assert_private_post_apply_coverage(ProductionAuthorityStore(probe), candidate, **bindings)
    assert probe.explicit_commits == probe.explicit_rollbacks == 0
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    conn.autocommit = True
    _assert_coverage_clean(conn)


def test_private_coverage_malformed_diagnostic_sample_refuses(coverage_context, monkeypatch):
    conn, store, bindings = coverage_context
    candidate = _private_candidate(store, bindings, _record_set(["doc:missing"]))
    checkout = ChronosGraphStore.checkout

    def bad_checkout(graph, branch):
        session = checkout(graph, branch)
        query = session.query
        session.query = lambda sql, params=None: [{"node_id": 7}] if "LIMIT 10" in sql else query(sql, params)
        return session

    with monkeypatch.context() as patch:
        patch.setattr(ChronosGraphStore, "checkout", bad_checkout)
        with pytest.raises(SubstrateError, match="invalid sample"):
            private_coverage.assert_private_post_apply_coverage(store, candidate, **bindings)
    _assert_coverage_clean(conn)
