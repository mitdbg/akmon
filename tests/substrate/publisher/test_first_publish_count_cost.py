"""Issue #1456 — a first publish must not recount an unchanged graph.

`_chronos_current_graph_counts` is two sequential scans of the effective graph,
and a first publish reached it four to six times. Several of those calls
provably observe the same graph, and a retry after a failed attempt recomputed
counts a blocked checkpoint was already carrying.
"""
from __future__ import annotations

import ast
import copy
import pathlib
import re
from typing import Any

import psycopg
import pytest

from okg.substrate.catalog import InvariantSpec
from okg.substrate.catalog.invariants import install_invariants
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher import publisher as publisher_module
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer


pytestmark = pytest.mark.postgres

PUBLISHER_SOURCE = (
    pathlib.Path(__file__).resolve().parents[3]
    / "src/okg/substrate/publisher/publisher.py"
)

# Calls that mutate the Chronos branch. If one of these ever appears between the
# first shared count and the checkpoint stamp, the shared measurement below stops
# being sound and this file must be revisited.
BRANCH_WRITE_CALLS = frozenset({
    "apply_facts",
    "_apply_publisher_derived_fact_batch",
    "prune_edges",
})


def _seed_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def _seed_graph(conn: psycopg.Connection, node_ids: list[str]) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(nid, "page", {"text": nid}, {"id": nid}, {"v": 1})
            for nid in node_ids
        ],
        source="default",
        strict_endpoints=True,
    )
    if len(node_ids) >= 2:
        store.apply_facts(
            [EdgeFact(
                node_ids[0], node_ids[1], "links_to",
                source_record_id={"id": "e0"}, source_revision={"v": 1},
            )],
            source="default",
            strict_endpoints=True,
        )


def _publish(dsn: str) -> dict:
    return Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()


def _spy_counts(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    original = Publisher._chronos_current_graph_counts

    def _recording(branch: Any) -> tuple[int, int]:
        calls.append("scan")
        return original(branch)

    monkeypatch.setattr(
        Publisher, "_chronos_current_graph_counts", staticmethod(_recording),
    )
    return calls


def _independent_count(dsn: str) -> tuple[int, int]:
    """Count the published graph without going through the publisher."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        checkout = store.checkout("default")
        nodes = int(
            checkout.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        edges = int(
            checkout.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    return nodes, edges


def _stamped_counts(dsn: str, generation_id: str) -> tuple[int, int]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        metadata = dict(store.get_generation(generation_id).metadata or {})
    return int(metadata["node_count"]), int(metadata["edge_count"])


# --------------------------------------------- the span must stay write-free

def test_no_branch_write_between_the_shared_count_and_the_stamp() -> None:
    """The mechanism that keeps the shared measurement sound.

    The counts are shared across consumers that observe the same graph. That is
    true today because the cycle performs no branch write between the
    post-view-fact count and the checkpoint stamp. This test fails the moment
    someone adds one, rather than letting a stale count be stamped silently.
    """
    tree = ast.parse(PUBLISHER_SOURCE.read_text())
    cycle = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and node.name == "_cycle_chronos_native"
    )
    shared_start = min(
        node.lineno for node in ast.walk(cycle)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_counted_graph"
        and node.lineno > _first_checkout_line(cycle)
    )
    stamp_end = max(
        node.lineno for node in ast.walk(cycle)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_counted_graph"
    )
    offenders = [
        f"line {node.lineno}: {node.func.attr}"
        for node in ast.walk(cycle)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in BRANCH_WRITE_CALLS
        and shared_start < node.lineno <= stamp_end
    ]
    assert not offenders, (
        "a branch write appeared between the shared count and the checkpoint "
        "stamp, so the shared measurement is no longer sound: "
        + "; ".join(offenders)
    )


def _first_checkout_line(cycle: ast.FunctionDef) -> int:
    """Line of the LAST `store.checkout(...)` re-checkout in the cycle."""
    return max(
        node.lineno for node in ast.walk(cycle)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "checkout"
    )


# ------------------------------------------------------- one count per state

def test_first_publish_shares_one_count_across_the_write_free_span(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])

    calls = _spy_counts(monkeypatch)
    out = _publish(clean_runtime)
    assert out["status"] == "published"

    # The exact figure depends on which stages a given deployment runs; what
    # this pins is that the cycle no longer scans once per consumer.
    assert len(calls) <= 2, (
        f"a first publish should share its measurement; scanned {len(calls)}x"
    )
    assert _stamped_counts(clean_runtime, out["generation_id"]) == (
        _independent_count(clean_runtime)
    ), "the stamp must still equal an independent count of the published graph"


def test_a_write_between_uses_is_not_carried_forward(
    clean_runtime: str,
) -> None:
    """The memo is keyed on the branch session, which a write invalidates.

    Every write in the cycle is followed by a re-checkout, so a written-to
    branch is a different session object and cannot hit the memo. Publishing
    twice with a write in between must therefore scan again rather than carry
    the first cycle's numbers forward.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    first = _publish(clean_runtime)
    assert first["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, ["n:c", "n:d"])

    second = _publish(clean_runtime)
    assert second["status"] == "published"
    # With a published predecessor and a real delta the publisher maintains a
    # running counter and legitimately does not scan at all. What must NOT
    # happen is carrying the checkpoint's counts forward as if nothing changed.
    assert "graph_counts_carried_from_checkpoint" not in second["timings_ms"], (
        "a branch written since the checkpoint must not carry counts forward"
    )
    assert _stamped_counts(clean_runtime, second["generation_id"]) == (
        _independent_count(clean_runtime)
    ), "the stamp must reflect the writes made since the previous generation"


# ------------------------------------------- carry forward across a failure

def test_retry_after_a_blocked_first_publish_does_not_rescan(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reported incident: a first publish fails after its counts.

    It still cuts a blocked checkpoint carrying them. With the branch untouched,
    the retry must take those counts forward instead of scanning again.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "blocks_the_first_attempt", "error", "cross", None,
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])

    blocked = _publish(clean_runtime)
    assert blocked["status"] == "blocked"
    stamped = _stamped_counts(clean_runtime, blocked["generation_id"])
    assert stamped == _independent_count(clean_runtime), (
        "the blocked checkpoint must carry trustworthy counts"
    )

    # Remove the blocking invariant; touch nothing else about the graph.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.invariants WHERE name = %s",
                ("blocks_the_first_attempt",),
            )

    calls = _spy_counts(monkeypatch)
    retry = _publish(clean_runtime)
    assert retry["status"] == "published"
    assert retry["timings_ms"].get("graph_counts_carried_from_checkpoint") == 1.0, (
        "the retry must carry the blocked checkpoint's counts forward"
    )
    # One scan remains and MUST remain: the periodic reconcile is an independent
    # oracle for the stamp, and serving it from the carried value would make the
    # drift guard compare a number with itself.
    assert len(calls) <= 1, (
        f"only the independent reconcile may scan; scanned {len(calls)}x"
    )
    assert _stamped_counts(clean_runtime, retry["generation_id"]) == stamped


def test_a_write_after_the_blocked_attempt_forces_a_real_count(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "blocks_the_first_attempt", "error", "cross", None,
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _seed_graph(conn, ["n:a", "n:b"])
    assert _publish(clean_runtime)["status"] == "blocked"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.invariants WHERE name = %s",
                ("blocks_the_first_attempt",),
            )
        _seed_graph(conn, ["n:c"])

    calls = _spy_counts(monkeypatch)
    retry = _publish(clean_runtime)
    assert retry["status"] == "published"
    assert calls, "a graph write since the checkpoint must force a real count"
    assert _stamped_counts(clean_runtime, retry["generation_id"]) == (
        _independent_count(clean_runtime)
    )


def _blocked_first_publish(dsn: str, node_ids: list[str]) -> None:
    """Leave a blocked checkpoint carrying counts, so a carry is POSSIBLE.

    Without this the fail-safe tests below would be vacuous: on a virgin branch
    `previous_counts` is None and the carry short-circuits before the probe is
    ever consulted, so they would pass with the probe handling deleted.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "blocks_the_first_attempt", "error", "cross", None,
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _seed_graph(conn, node_ids)
    assert _publish(dsn)["status"] == "blocked"
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.invariants WHERE name = %s",
                ("blocks_the_first_attempt",),
            )


def test_the_carry_is_reachable_in_the_failsafe_setup(
    clean_runtime: str,
) -> None:
    """Guards the three tests below from going vacuous.

    Each asserts that a broken probe forces a scan. That only means anything if
    an unbroken probe would have carried instead, so prove the carry fires here.
    """
    _blocked_first_publish(clean_runtime, ["n:a", "n:b"])
    retry = _publish(clean_runtime)
    assert retry["status"] == "published"
    assert retry["timings_ms"].get(
        "graph_counts_carried_from_checkpoint"
    ) == 1.0


def test_an_unanswerable_write_probe_forces_a_real_count(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail-safe, not fail-open: no answer means scan."""
    _blocked_first_publish(clean_runtime, ["n:a", "n:b"])
    monkeypatch.setattr(
        Publisher, "_chronos_current_segment_has_graph_writes",
        staticmethod(lambda *a, **k: None),
    )
    calls = _spy_counts(monkeypatch)
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert "graph_counts_carried_from_checkpoint" not in out["timings_ms"]
    assert calls, "an unanswerable probe must fall back to a real count"
    assert _stamped_counts(clean_runtime, out["generation_id"]) == (
        _independent_count(clean_runtime)
    )


def test_a_failing_write_probe_forces_a_real_count(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _blocked_first_publish(clean_runtime, ["n:a", "n:b"])

    def _boom(*args: Any, **kwargs: Any) -> bool:
        # The probe re-raises whatever it hit, which in practice is a database
        # error; the handler is narrowed to those so a genuine defect still
        # propagates rather than being read as "just take a full count".
        raise psycopg.OperationalError("probe unavailable")

    monkeypatch.setattr(
        Publisher, "_chronos_current_segment_has_graph_writes",
        staticmethod(_boom),
    )
    calls = _spy_counts(monkeypatch)
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert "graph_counts_carried_from_checkpoint" not in out["timings_ms"]
    assert out["timings_ms"].get("graph_counts_carry_probe_failed") == 1.0, (
        "declining to carry must be announced, not silent"
    )
    assert calls, "a probe failure must fall back to a real count"
    assert _stamped_counts(clean_runtime, out["generation_id"]) == (
        _independent_count(clean_runtime)
    )


def test_a_missing_count_stamp_forces_a_real_count(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The third fail-safe arm the PACT promises: no trustworthy stamp, no carry."""
    _blocked_first_publish(clean_runtime, ["n:a", "n:b"])
    monkeypatch.setattr(
        Publisher, "_chronos_checkpoint_graph_counts",
        staticmethod(lambda checkpoint: None),
    )
    calls = _spy_counts(monkeypatch)
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert "graph_counts_carried_from_checkpoint" not in out["timings_ms"]
    assert calls, "an absent count stamp must fall back to a real count"
    assert _stamped_counts(clean_runtime, out["generation_id"]) == (
        _independent_count(clean_runtime)
    )


# ------------------------------------- the count must not scan the stored rows


def _physical_graph_tables(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name, physical_table FROM _chronos_branch_tables "
                "WHERE backend = 'interval' AND table_name IN "
                "('okg.graph_nodes', 'okg.graph_edges')"
            )
            return {str(name): str(physical) for name, physical in cur.fetchall()}


def _record_chronos_statements(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every SQL text Chronos sends to PostgreSQL, after its own rewrite.

    `_execute_with_dict_rows` is the single chokepoint the Postgres adapter
    funnels every branch statement through, so recording there captures the
    statement inventory for the publisher's own pooled connections without
    threading a probe through production code. Recording the text Chronos
    RECEIVES instead would prove nothing: the defect is what the backend's
    rewrite turns that text into.
    """
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    statements: list[str] = []
    original = PostgresDatabaseAdapter._execute_with_dict_rows

    def _recording(self: Any, sql: str, params: Any = ()) -> Any:
        statements.append(sql)
        return original(self, sql, params)

    monkeypatch.setattr(
        PostgresDatabaseAdapter, "_execute_with_dict_rows", _recording,
    )
    return statements


# Columns that only the STORED row carries. A count statement that names one
# has to read the heap -- the jsonb payload and the embedding vector live
# there -- which is precisely the plan that held the publish lock for 3,437s.
_STORED_ROW_COLUMNS = ("attrs", "embedding", "source_record_id")

# `SELECT count(*) AS n FROM ...` -- the shape both graph-count routes use.
_SCALAR_GRAPH_COUNT = re.compile(
    r"\A\s*SELECT\s+COUNT\(\*\)\s+AS\s+n\s+FROM\b", re.IGNORECASE,
)


def test_the_first_publish_count_reads_no_stored_graph_row(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1456(a): count through the visibility predicate, not a projection.

    Before this change the publisher asked the branch session for
    `SELECT count(*) FROM okg.graph_nodes`, which the interval backend rewrites
    into a count over a subquery selecting EVERY column. After it, the same
    exact number comes from the backend's own visibility predicate applied to
    the same physical table with no projection.

    This pins the statement shape, NOT a speedup: measured at 2,000,000
    interval rows the two forms get the same plan, because PostgreSQL flattens
    the subquery and prunes the unread columns. What it buys is that the count
    no longer depends on that flattening, and that the plan reported by the
    access-path probe is the plan that runs.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])
    physical = _physical_graph_tables(clean_runtime)
    assert set(physical) == {"okg.graph_nodes", "okg.graph_edges"}, physical

    statements = _record_chronos_statements(monkeypatch)
    out = _publish(clean_runtime)
    assert out["status"] == "published"

    # Scalar graph counts only -- `SELECT count(*) AS n FROM ...`. The cycle
    # also runs GROUPED counts for the ontology rollup, which legitimately read
    # `subtype` and `source`; those are a separate consumer with a separate
    # cost, and folding them in here would make this assertion about something
    # this change does not touch.
    graph_counts = [
        text for text in statements
        if _SCALAR_GRAPH_COUNT.match(text)
        and any(table in text for table in physical.values())
    ]
    assert graph_counts, "the cycle took no scalar graph count at all"
    projecting = [
        text for text in graph_counts
        if any(f'"{column}"' in text for column in _STORED_ROW_COLUMNS)
    ]
    assert not projecting, (
        "a graph count projected the stored row, so PostgreSQL must read the "
        "heap for every node/edge:\n" + "\n".join(projecting)
    )
    # Not vacuous: the bounded form really did run.
    bounded = [
        text for text in graph_counts
        if "live_lo" in text and "live_hi" in text
    ]
    assert bounded, (
        "no bounded interval count ran, so the assertion above proves nothing; "
        f"graph count statements were: {graph_counts}"
    )
    assert out["graph_counts_provenance"]["source"] == (
        "chronos_interval_visible_predicate"
    )
    assert out["graph_counts_provenance"]["exact"] is True
    assert _stamped_counts(clean_runtime, out["generation_id"]) == (
        _independent_count(clean_runtime)
    )


def test_the_bounded_count_equals_the_projection_count_on_a_live_branch(
    clean_runtime: str,
) -> None:
    """Same table, same predicate, same branch point -- so the same number.

    Seeded with superseded revisions, because a shortcut that counted stored
    row versions instead of visible ones would disagree on exactly those.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])
    # A checkpoint seals the writer segment, so the next revision cannot
    # overwrite the stored row in place: it closes that interval and inserts a
    # new one. Without the seal Chronos splices within the open segment and the
    # backing table would hold exactly the visible rows, making the
    # non-vacuity check below untrue rather than merely unproven.
    assert _publish(clean_runtime)["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:a", "page", {"text": "n:a v2"}, {"id": "n:a"}, {"v": 2})],
            source="default", strict_endpoints=True,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        branch = store.checkout("default")
        bounded = Publisher._chronos_bounded_graph_counts(branch)
        projected = (
            int(branch.query(
                "SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]),
            int(branch.query(
                "SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]),
        )
    assert bounded is not None, "the bounded path must be available here"
    assert bounded == projected
    # Not vacuous: the stored table really does hold more versions than the
    # visible graph, so a version count would have given a different answer.
    physical = _physical_graph_tables(clean_runtime)["okg.graph_nodes"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{physical}"')
            stored = int(cur.fetchone()[0])
    assert stored > bounded[0], (stored, bounded)


def test_an_unresolvable_interval_registry_falls_back_and_says_so(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail-safe, and labelled: the slower route is still the exact route."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    monkeypatch.setattr(
        Publisher, "_chronos_interval_physical_tables",
        staticmethod(lambda branch: None),
    )
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    provenance = out["graph_counts_provenance"]
    assert provenance["source"] == "branch_query_effective_projection"
    # Falling back must never be a bare None. The receipt says WHY the bounded
    # route was not taken, which is the difference between an announced
    # fallback and the silent one this repository refuses.
    assert provenance["bounded_route_declined"], provenance
    assert _stamped_counts(clean_runtime, out["generation_id"]) == (
        _independent_count(clean_runtime)
    )


def test_a_carried_count_is_labelled_as_carried_not_as_a_count(
    clean_runtime: str,
) -> None:
    """Issue #1456(b), stated in the receipt rather than inferred from timings."""
    _blocked_first_publish(clean_runtime, ["n:a", "n:b"])
    retry = _publish(clean_runtime)
    assert retry["status"] == "published"
    assert retry["graph_counts_provenance"]["source"] == "carried_from_checkpoint"


# ------------------------------------------------- progress while it is slow


def test_a_first_publish_reports_per_table_count_progress(
    clean_runtime: str,
) -> None:
    """Issue #1456(c): the long phase is legible before the terminal output.

    One durable record per graph table, each carrying its own elapsed seconds
    and the seconds since the phase began, following the history-compaction
    planner's `planner_progress` (#1308/#1627).
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])
    out = _publish(clean_runtime)
    assert out["status"] == "published"

    provenance = out["graph_counts_provenance"]
    progress = provenance["progress"]
    assert [record["table"] for record in progress] == [
        "okg.graph_nodes", "okg.graph_edges",
    ], progress
    for record in progress:
        assert record["phase"] == "current_graph_counts_initial"
        assert record["elapsed_seconds"] >= 0.0
        assert record["since_start_seconds"] >= record["elapsed_seconds"]
        assert f"current_graph_counts_initial.{record['table']}" in (
            out["timings_ms"]
        )
    counted = {record["table"]: record["count"] for record in progress}
    assert (
        counted["okg.graph_nodes"], counted["okg.graph_edges"],
    ) == _independent_count(clean_runtime)

    # The pre-count estimate is reported under its own name and is never used
    # as a count: it measures stored row VERSIONS in the backing table.
    estimate = provenance["physical_row_versions_estimate"]
    unavailable = provenance["physical_row_versions_estimate_unavailable"]
    assert (estimate is None) != (unavailable is None), provenance
    assert estimate is None or (
        estimate and set(estimate) <= {"okg.graph_nodes", "okg.graph_edges"}
        and all(value >= 0 for value in estimate.values())
    ), estimate
    # The bounded route was taken here, so nothing declined it.
    assert provenance["bounded_route_declined"] is None, provenance


def test_a_later_cycle_reports_no_first_publish_count_progress(
    clean_runtime: str,
) -> None:
    """Guards the test above from passing on a phase that always runs.

    The per-table progress belongs to the first-publish count. A cycle with a
    published predecessor maintains a running counter and must not manufacture
    progress records for a count it never took.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    assert _publish(clean_runtime)["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, ["n:c", "n:d"])
    second = _publish(clean_runtime)
    assert second["status"] == "published"
    assert second["graph_counts_provenance"]["progress"] == []


def _branch_point(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        return int(
            store.checkout("default").query(
                "SELECT :_chronos_branch_point AS bp"
            )[0]["bp"]
        )


def _plan_node_types(plan: dict) -> set[str]:
    types = {str(plan.get("Node Type"))}
    for child in plan.get("Plans") or ():
        types |= _plan_node_types(child)
    return types


# ---------------------------------------- which plan is about to run, and why


def test_a_first_publish_reports_the_count_access_path(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1456(a), as the measurement rewrote it.

    The reported hour was not caused by the query surface: at 2,000,000
    interval rows the projected and unprojected counts get the same plan and
    the same page count. The cost is decided by whether the backing table can
    be counted from an index -- 9,263 pages when the visibility map is current,
    110,768 when it is not. That is invisible in the query text, so the first
    publish now reports it before the count starts.
    """
    captured: list[tuple[str, Any, Any]] = []
    real_probe = Publisher._chronos_graph_count_access_path

    class RecordingCursor:
        def __init__(self, cursor: Any, connection: psycopg.Connection) -> None:
            self.cursor = cursor
            self.connection = connection
            self.explain: tuple[str, Any] | None = None

        def __enter__(self) -> RecordingCursor:
            self.cursor.__enter__()
            return self

        def __exit__(self, *args: Any) -> Any:
            return self.cursor.__exit__(*args)

        def execute(self, statement: Any, parameters: Any = None) -> Any:
            text = (statement.as_string(self.connection)
                    if isinstance(statement, psycopg.sql.Composable) else statement)
            self.explain = (text, parameters) if text.startswith("EXPLAIN ") else None
            return self.cursor.execute(statement, parameters)

        def fetchone(self) -> Any:
            row = self.cursor.fetchone()
            if self.explain is not None:
                captured.append((*self.explain, copy.deepcopy(row)))
            return row

    class RecordingConnection:
        def __init__(self, connection: psycopg.Connection) -> None:
            self.connection = connection

        @property
        def autocommit(self) -> bool:
            return self.connection.autocommit

        def cursor(self) -> RecordingCursor:
            return RecordingCursor(self.connection.cursor(), self.connection)

    def record_probe(conn: psycopg.Connection, branch: Any) -> Any:
        # Execute the real probe unchanged. Only tee its actual database rows:
        # a later EXPLAIN may observe new statistics or a new branch point.
        return real_probe(RecordingConnection(conn), branch)

    monkeypatch.setattr(Publisher, "_chronos_graph_count_access_path", staticmethod(record_probe))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])
    out = _publish(clean_runtime)
    assert out["status"] == "published"

    provenance = out["graph_counts_provenance"]
    access_path = provenance["access_path"]
    assert access_path is not None, provenance["access_path_unavailable"]
    assert provenance["access_path_unavailable"] is None
    assert set(access_path) == {"okg.graph_nodes", "okg.graph_edges"}
    physical = _physical_graph_tables(clean_runtime)
    assert len(captured) == 2
    assert captured[0][1] == captured[1][1]
    for table, path in access_path.items():
        assert path["physical_table"] == physical[table]
        expected_statement = (
            'EXPLAIN (FORMAT JSON) SELECT count(*) FROM '
            f'"{physical[table]}" WHERE live_lo <= %s '
            'AND %s < live_hi AND deleted = FALSE'
        )
        matches = [(parameters, row) for statement, parameters, row in captured
                   if statement == expected_statement]
        assert len(matches) == 1, captured
        parameters, row = matches[0]
        assert len(parameters) == 2 and parameters[0] == parameters[1]
        plan = row[0][0]["Plan"]
        pending = [plan]
        scans = []
        relations = set()
        while pending:
            node = pending.pop()
            if "Relation Name" in node:
                relations.add(node["Relation Name"])
            if "Scan" in node["Node Type"]:
                scans.append(node)
            pending.extend(node.get("Plans", []))
        # A bitmap plan can contain both heap and index scan nodes. Require
        # the actual reported pair in the raw plan, without fixing its shape.
        assert relations == {physical[table]}, plan
        reported = [node for node in scans
                    if (node["Node Type"], node.get("Index Name"))
                    == (path["scan"], path["index"])]
        assert len(reported) == 1, (path, plan)
        scan = reported[0]
        assert isinstance(path["estimated_total_cost"], (int, float))
        assert path["estimated_total_cost"] == plan["Total Cost"]
        assert path["index_only"] is (scan["Node Type"] == "Index Only Scan")


def test_the_access_path_probe_distinguishes_the_two_plans(
    clean_runtime: str,
) -> None:
    """Not a constant: the probe reports whichever plan PostgreSQL picks.

    A probe that always answered the same thing would satisfy the test above
    while telling an operator nothing. So drive the SAME table into both of
    the plans the publisher's warning is written about -- a count served from
    an index, and a count that reads every heap page -- and require the probe
    to report each one, including the index name and the `index_only` flag the
    receipt and the warning are built from.

    Which plan wins is forced here by the planner settings on the probe's own
    session, not by table size. Issue #1986: the earlier version of this test
    bulk-loaded 200,000 rows, ran VACUUM ANALYZE and asserted the result was
    an `Index Only Scan`. That assertion is not a property of the probe. An
    index-only count is only the cheaper plan while the visibility map says
    those pages are all-visible, and VACUUM cannot mark a page all-visible
    while any other backend still holds an older snapshot. In CI this file
    runs in one job and one database with the whole substrate-db shard, so a
    snapshot held anywhere else in that database flipped the planner to a
    sequential scan and reddened unrelated pull requests (#1978, #1957 and
    dev bf23098784). Reproduced deterministically by holding one REPEATABLE
    READ snapshot open across the load and the VACUUM. Forcing the choice
    with `enable_*` instead makes the losing plans cost `disable_cost`, so
    each side is decided by a rule rather than by a cost comparison against
    whatever statistics and visibility map the shard left behind.

    This still fails if the probe stops distinguishing the plans: a probe that
    answered a constant would be wrong on one of the two sides. It also holds
    the schema to having an index that can serve this count at all, which is
    what the publisher's "VACUUM ANALYZE the backing tables" advice assumes.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    physical = _physical_graph_tables(clean_runtime)["okg.graph_nodes"]

    # Statistics and a visibility map that describe the two rows actually
    # seeded. PostgreSQL does not reset `pg_class.reltuples` / `relpages` on
    # TRUNCATE and `clean_runtime` resets the graph by truncating, so without
    # this the table carries whatever estimate last analyzed it, across tests
    # and across files.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'VACUUM ANALYZE "{physical}"')

    def _node_path(settings: tuple[str, ...]) -> dict[str, Any]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                for setting in settings:
                    cur.execute(f"SET {setting} = off")
            store = ChronosGraphStore.from_connection(conn)
            branch = store.checkout("default")
            paths, reason = Publisher._chronos_graph_count_access_path(
                conn, branch,
            )
        assert paths is not None, reason
        return paths["okg.graph_nodes"]

    # No sequential path available, so the count must come off an index.
    # Bitmap scans are off as well: a bitmap plan re-reads the heap, so it is
    # neither of the two plans this is about.
    from_index = _node_path(("enable_seqscan", "enable_bitmapscan"))
    # No index path available, so the count must read the heap.
    from_heap = _node_path(
        ("enable_indexscan", "enable_indexonlyscan", "enable_bitmapscan")
    )

    # An index path of some kind on one side, a heap read on the other. Which
    # index path is NOT asserted: a plain `Index Scan` beats an `Index Only
    # Scan` whenever the visibility map is stale, and that is the same
    # snapshot-dependent comparison this test exists to stop depending on. The
    # `index_only` flag is checked for agreement with the node the probe
    # reported, and `test_a_first_publish_reports_the_count_access_path` checks
    # both against the raw EXPLAIN output.
    assert "Index" in from_index["scan"], from_index
    assert from_index["index"] is not None, from_index
    assert from_index["index_only"] is (
        from_index["scan"] == "Index Only Scan"
    ), from_index
    assert from_heap["scan"] == "Seq Scan", from_heap
    assert from_heap["index"] is None, from_heap
    assert from_heap["index_only"] is False, from_heap
    assert from_index["scan"] != from_heap["scan"], (
        "the probe reported the same plan for a forced index count and a "
        f"forced heap count ({from_index['scan']!r}), so it is a constant"
    )
    for path in (from_index, from_heap):
        assert isinstance(path["estimated_total_cost"], (int, float)), path
        assert path["physical_table"] == physical, path


# ------------------------------------------- the guards on the bounded route


class _StubBackend:
    name = "interval"
    enable_schema_branching = False


class _StubContext:
    def __init__(self, backend: Any) -> None:
        self._backend = backend


class _StubBranch:
    """A branch session whose registry answer the test chooses."""

    def __init__(self, rows: list[dict[str, Any]], backend: Any = None) -> None:
        self._rows = rows
        self._context = _StubContext(backend or _StubBackend())

    def query(self, sql_text: str, params: Any = None) -> list[dict[str, Any]]:
        assert "_chronos_branch_tables" in sql_text, sql_text
        return self._rows


def _backend(*, name: str = "interval", schema: bool = False) -> _StubBackend:
    backend = _StubBackend()
    backend.name = name
    backend.enable_schema_branching = schema
    return backend


_GOOD_ROWS = [
    {"table_name": "okg.graph_nodes", "physical_table": "phys_nodes"},
    {"table_name": "okg.graph_edges", "physical_table": "phys_edges"},
]


def test_the_registry_guard_accepts_the_shape_it_is_meant_to_accept() -> None:
    """Guards the refusals below from being vacuous."""
    assert Publisher._chronos_interval_physical_tables(
        _StubBranch(_GOOD_ROWS)
    ) == {"okg.graph_nodes": "phys_nodes", "okg.graph_edges": "phys_edges"}


def test_two_graph_tables_sharing_one_backing_table_are_refused() -> None:
    """Identity must be injective, not merely single-sited.

    `table_name` is the registry primary key, so one logical name can never
    yield two backing tables. The failure that IS possible is the other
    direction: if both graph tables resolved to one backing table the counts
    would count the same rows twice and both reported numbers would be wrong,
    with no error anywhere.
    """
    rows = [
        {"table_name": "okg.graph_nodes", "physical_table": "same_table"},
        {"table_name": "okg.graph_edges", "physical_table": "same_table"},
    ]
    assert Publisher._chronos_interval_physical_tables(_StubBranch(rows)) is None


def test_schema_branching_refuses_the_bounded_route() -> None:
    """The registry this route reads is only Chronos's answer when it is off.

    With schema branching on, Chronos resolves the backing table per segment
    through the table bindings, honouring tombstones and the segment's branch
    point. `_chronos_branch_tables` then holds only the base registration, so
    counting through it would count a table the session does not read.
    """
    backend = _StubBackend()
    backend.enable_schema_branching = True
    assert Publisher._chronos_interval_physical_tables(
        _StubBranch(_GOOD_ROWS, backend)
    ) is None


def test_a_non_interval_backend_refuses_the_bounded_route() -> None:
    backend = _StubBackend()
    backend.name = "copy"
    assert Publisher._chronos_interval_physical_tables(
        _StubBranch(_GOOD_ROWS, backend)
    ) is None


def test_an_unreadable_backend_refuses_rather_than_assumes() -> None:
    """No backend to ask means no proof the precondition holds."""
    branch = _StubBranch(_GOOD_ROWS)
    branch._context = None
    assert Publisher._chronos_interval_physical_tables(branch) is None


def test_a_null_backing_table_name_refuses() -> None:
    rows = [
        {"table_name": "okg.graph_nodes", "physical_table": None},
        {"table_name": "okg.graph_edges", "physical_table": "phys_edges"},
    ]
    assert Publisher._chronos_interval_physical_tables(_StubBranch(rows)) is None


def test_a_backing_table_name_that_is_not_an_identifier_refuses() -> None:
    rows = [
        {"table_name": "okg.graph_nodes", "physical_table": 'x"; DROP TABLE y--'},
        {"table_name": "okg.graph_edges", "physical_table": "phys_edges"},
    ]
    assert Publisher._chronos_interval_physical_tables(_StubBranch(rows)) is None


def test_every_refusal_of_the_bounded_route_records_a_reason() -> None:
    """No route may decline in silence.

    Each refusal below returns None to the caller, which then counts the slower
    way. That is correct behavior; leaving no reason behind is not, because a
    deployment that quietly stopped taking the fast route would look identical
    to one that never could.
    """
    cases = {
        "no backend": _StubBranch(_GOOD_ROWS),
        "schema branching": _StubBranch(_GOOD_ROWS, _backend(schema=True)),
        "other backend": _StubBranch(_GOOD_ROWS, _backend(name="copy")),
        "null backing table": _StubBranch([
            {"table_name": "okg.graph_nodes", "physical_table": None},
            {"table_name": "okg.graph_edges", "physical_table": "phys_edges"},
        ]),
        "unquotable name": _StubBranch([
            {"table_name": "okg.graph_nodes", "physical_table": "a b"},
            {"table_name": "okg.graph_edges", "physical_table": "phys_edges"},
        ]),
        "shared backing table": _StubBranch([
            {"table_name": "okg.graph_nodes", "physical_table": "same"},
            {"table_name": "okg.graph_edges", "physical_table": "same"},
        ]),
        "missing rows": _StubBranch([]),
    }
    cases["no backend"]._context = None
    for label, branch in cases.items():
        publisher_module._GRAPH_COUNT.declined = None
        assert Publisher._chronos_interval_physical_tables(branch) is None, label
        assert publisher_module._GRAPH_COUNT.declined, (
            f"{label} declined the bounded route without recording a reason"
        )
    publisher_module._GRAPH_COUNT.declined = None


def test_a_database_error_in_the_registry_read_is_caught_and_named() -> None:
    """Narrow, not broad: a database refusal falls back and says so."""

    class _Failing(_StubBranch):
        def query(self, sql_text: str, params: Any = None) -> list[dict[str, Any]]:
            raise psycopg.OperationalError("registry unavailable")

    publisher_module._GRAPH_COUNT.declined = None
    assert Publisher._chronos_interval_physical_tables(_Failing([])) is None
    assert "OperationalError" in (publisher_module._GRAPH_COUNT.declined or "")
    publisher_module._GRAPH_COUNT.declined = None


def test_a_programming_error_in_the_registry_read_still_propagates() -> None:
    """The handler is narrowed so a defect here is not read as a slow path.

    Catching `Exception` would have turned an `AttributeError` in this file
    into "take a full count instead" -- a real bug, permanently invisible,
    paid for on every publish.
    """

    class _Broken(_StubBranch):
        def query(self, sql_text: str, params: Any = None) -> list[dict[str, Any]]:
            raise AttributeError("defect in this file")

    with pytest.raises(AttributeError):
        Publisher._chronos_interval_physical_tables(_Broken([]))


# ------------------------------------- an absent diagnostic is not a silence


def test_an_unavailable_access_path_is_reported_with_its_reason(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An access path nobody can compute must not read as one nobody needed.

    Reporting the plan is the part of this change that addresses the measured
    cost driver. If the probe cannot run, the operator is back to the blindness
    issue #1456 was filed about, so the receipt carries why.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    monkeypatch.setattr(
        Publisher, "_chronos_interval_physical_tables",
        staticmethod(lambda branch: None),
    )
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    provenance = out["graph_counts_provenance"]
    assert provenance["access_path"] is None
    assert provenance["access_path_unavailable"], provenance
    # And the count still happened, exactly, by the other route.
    assert provenance["source"] == "branch_query_effective_projection"
    assert _stamped_counts(clean_runtime, out["generation_id"]) == (
        _independent_count(clean_runtime)
    )


def test_a_reuse_cycle_still_says_where_its_counts_came_from(
    clean_runtime: str,
) -> None:
    """A green cycle whose counts did not move is the one most often read.

    The no-op reuse path builds its own result dict. Before this change that
    dict had no provenance key at all, so a consumer that read it on the
    ordinary path raised KeyError on exactly the quiet cycle.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    assert _publish(clean_runtime)["status"] == "published"

    # Nothing changes between the two cycles, so the second is a no-op.
    second = _publish(clean_runtime)
    provenance = second["graph_counts_provenance"]
    assert provenance["source"] in {
        "carried_from_checkpoint",
        "chronos_interval_visible_predicate",
        "branch_query_effective_projection",
        "previous_counts_plus_delta",
    }, provenance
    assert provenance["sources"], provenance
    assert provenance["source"] == provenance["sources"][-1]


# ------------------------------- a small first publish is quiet on stderr


_COUNT_ANNOUNCEMENT = "okg publisher default branch=default:"


def _plan_every_count_as_a_heap_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the real access-path probe, then report every table as heap-scanned.

    A never-vacuumed table is normally planned as a heap scan, but autovacuum
    can mark a two-row table all-visible between the seed and the publish; the
    planner then picks an index-only scan and a test about the heap-scan
    advice depends on autovacuum timing (seen on a shared test database).
    Pinning the plan keeps these tests about what the publisher says for a
    heap-scan count, which is the plan of the hour-long count in #1456.
    """
    probe = Publisher._chronos_graph_count_access_path

    def heap_scan_probe(conn: Any, branch: Any) -> Any:
        paths, reason = probe(conn, branch)
        if paths is None:
            return paths, reason
        return {
            table: {**path, "scan": "Seq Scan", "index": None,
                    "index_only": False}
            for table, path in paths.items()
        }, reason

    monkeypatch.setattr(
        Publisher, "_chronos_graph_count_access_path",
        staticmethod(heap_scan_probe),
    )


def test_a_small_first_publish_prints_no_count_announcement(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A freshly created database must not tell the operator to VACUUM.

    Its backing tables have never been vacuumed, so the count is planned as
    a heap scan and the old code printed seven stderr lines, one of them a
    WARNING with VACUUM advice, for a count of a few rows. The heap size is
    measured and recorded; the lines are not printed.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])
    capsys.readouterr()
    with caplog.at_level("INFO", logger=publisher_module.__name__):
        out = _publish(clean_runtime)
    assert out["status"] == "published"
    err = capsys.readouterr().err
    assert _COUNT_ANNOUNCEMENT not in err, err
    assert "VACUUM" not in err, err
    assert not [
        r for r in caplog.records
        if r.levelname == "WARNING" and "first-publish count" in r.getMessage()
    ], caplog.text
    provenance = out["graph_counts_provenance"]
    assert provenance["long_phase_announced"] is False, provenance
    heap = provenance["backing_heap_bytes"]
    assert set(heap) == {"okg.graph_nodes", "okg.graph_edges"}, heap
    assert 0 < sum(heap.values()) <= (
        publisher_module._FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES
    ), heap
    # The diagnostics are still recorded, only not printed.
    assert provenance["access_path"], provenance
    assert [record["table"] for record in provenance["progress"]] == [
        "okg.graph_nodes", "okg.graph_edges",
    ]


def test_a_first_publish_over_the_threshold_still_announces_and_warns(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard for the test above: a large branch keeps every line.

    Lowering the threshold below the measured size stands in for a large
    branch, whose never-vacuumed tables are the hour-long count of #1456.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])
    monkeypatch.setattr(
        publisher_module, "_FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES", -1,
    )
    _plan_every_count_as_a_heap_scan(monkeypatch)
    capsys.readouterr()
    with caplog.at_level("INFO", logger=publisher_module.__name__):
        out = _publish(clean_runtime)
    assert out["status"] == "published"
    err = capsys.readouterr().err
    provenance = out["graph_counts_provenance"]
    assert provenance["long_phase_announced"] is True, provenance
    assert "first publish has no predecessor count" in err, err
    assert f"{_COUNT_ANNOUNCEMENT} counting okg.graph_nodes by" in err, err
    assert f"{_COUNT_ANNOUNCEMENT} counted " in err, err
    heap_scanned = [
        table for table, path in provenance["access_path"].items()
        if not path["index_only"]
    ]
    # A never-vacuumed table is planned as a heap scan (measured on a fresh
    # PostgreSQL 17 database); the warning must then name it.
    assert heap_scanned, provenance["access_path"]
    warnings = [
        r.getMessage() for r in caplog.records
        if r.levelname == "WARNING"
        and "cannot be served from an index" in r.getMessage()
    ]
    assert len(warnings) == 1 and "VACUUM ANALYZE" in warnings[0], caplog.text
    assert "VACUUM ANALYZE the backing tables" in err, err


def test_an_unreadable_heap_size_announces_as_before(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Quiet needs a measurement. Without one the count is announced."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    monkeypatch.setattr(
        Publisher, "_chronos_backing_heap_bytes",
        staticmethod(lambda conn, mapping: (None, "heap size probe unavailable")),
    )
    capsys.readouterr()
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    provenance = out["graph_counts_provenance"]
    assert provenance["backing_heap_bytes"] is None
    assert provenance["backing_heap_bytes_unavailable"] == (
        "heap size probe unavailable"
    )
    assert provenance["long_phase_announced"] is True
    assert "first publish has no predecessor count" in capsys.readouterr().err


def test_a_small_first_publish_still_warns_when_the_plan_is_unknown(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken diagnostic is a real problem at any size and still warns."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    monkeypatch.setattr(
        Publisher, "_chronos_graph_count_access_path",
        staticmethod(lambda conn, branch: (None, "EXPLAIN failed for test")),
    )
    capsys.readouterr()
    with caplog.at_level("INFO", logger=publisher_module.__name__):
        out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert out["graph_counts_provenance"]["long_phase_announced"] is False
    err = capsys.readouterr().err
    assert "cannot report how this count will run (EXPLAIN failed for test)" in (
        err
    ), err
    assert any(
        r.levelname == "WARNING"
        and "count access path unavailable" in r.getMessage()
        for r in caplog.records
    ), caplog.text


def test_the_quiet_threshold_is_64_mib_per_table_and_inclusive() -> None:
    """Pin the value and the boundary: at the threshold is quiet, one over is not."""
    limit = publisher_module._FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES
    assert limit == 64 * 1024 * 1024
    decide = publisher_module._first_publish_count_is_announced
    assert decide(None) is True
    assert decide({"okg.graph_nodes": 0, "okg.graph_edges": 0}) is False
    assert decide({"okg.graph_nodes": limit, "okg.graph_edges": limit}) is False
    assert decide({"okg.graph_nodes": limit + 1, "okg.graph_edges": 0}) is True
    assert decide({"okg.graph_nodes": 0, "okg.graph_edges": limit + 1}) is True
    assert publisher_module._FIRST_PUBLISH_COUNT_SLOW_SECONDS == 5.0


def _hold_exclusive_lock(dsn: str, physical: str, seconds: float) -> Any:
    """Hold ACCESS EXCLUSIVE on one table from another session; return when held."""
    import threading
    import time

    ready = threading.Event()

    def hold() -> None:
        with psycopg.connect(dsn) as other:
            other.execute(f'LOCK TABLE "{physical}" IN ACCESS EXCLUSIVE MODE')
            ready.set()
            time.sleep(seconds)

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert ready.wait(10), "the lock holder never got its lock"
    return thread


def test_a_slow_small_count_is_still_printed(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review F1: quiet is for a quick count, not for a small one that waited.

    A lock held by another session makes a three-node count wait six seconds.
    The size read ran before the lock, so the phase is not announced; the
    count that waited must still print how long it took.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b", "n:c"])
    physical = _physical_graph_tables(clean_runtime)["okg.graph_nodes"]
    original = Publisher._chronos_current_graph_counts
    holders: list[Any] = []

    def locked(branch: Any) -> tuple[int, int]:
        if not holders:
            holders.append(_hold_exclusive_lock(clean_runtime, physical, 6))
        return original(branch)

    monkeypatch.setattr(
        Publisher, "_chronos_current_graph_counts", staticmethod(locked),
    )
    capsys.readouterr()
    out = _publish(clean_runtime)
    holders[0].join(15)
    assert out["status"] == "published"
    provenance = out["graph_counts_provenance"]
    assert provenance["long_phase_announced"] is False, provenance
    slow = [p for p in provenance["progress"] if p["elapsed_seconds"] > 5.0]
    assert slow, provenance["progress"]
    err = capsys.readouterr().err
    for record in slow:
        assert (
            f"{_COUNT_ANNOUNCEMENT} counted {record['count']:,} rows in "
            f"{record['table']} in "
        ) in err, err
    # The quick table is still quiet.
    for record in provenance["progress"]:
        if record not in slow:
            assert f"rows in {record['table']} in" not in err, err


def test_a_size_read_blocked_by_a_lock_gives_up_and_announces(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review F1: a publish blocked before its first line must not be silent.

    The size read runs under a short lock timeout. When another session holds
    the table, the read gives up, the size is unknown, and the phase is
    announced before the count waits on the same lock.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    physical = _physical_graph_tables(clean_runtime)["okg.graph_nodes"]
    monkeypatch.setattr(
        publisher_module, "_BACKING_HEAP_BYTES_LOCK_TIMEOUT", "200ms",
    )
    original = Publisher._chronos_backing_heap_bytes
    holders: list[Any] = []

    def locked(conn: Any, mapping: Any) -> Any:
        holders.append(_hold_exclusive_lock(clean_runtime, physical, 2))
        return original(conn, mapping)

    monkeypatch.setattr(
        Publisher, "_chronos_backing_heap_bytes", staticmethod(locked),
    )
    capsys.readouterr()
    out = _publish(clean_runtime)
    holders[0].join(15)
    assert out["status"] == "published"
    provenance = out["graph_counts_provenance"]
    assert provenance["backing_heap_bytes"] is None, provenance
    assert provenance["backing_heap_bytes_unavailable"] == (
        "heap size read failed (LockNotAvailable)"
    ), provenance
    assert provenance["long_phase_announced"] is True
    assert "first publish has no predecessor count" in capsys.readouterr().err


def test_the_heap_size_read_restores_the_lock_timeout(
    clean_runtime: str,
) -> None:
    """The short lock timeout must not leak into the caller's transaction."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    mapping = _physical_graph_tables(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:  # not autocommit
        conn.execute("SET LOCAL lock_timeout = '7s'")
        sizes, reason = Publisher._chronos_backing_heap_bytes(conn, mapping)
        assert reason is None and set(sizes) == set(mapping), (sizes, reason)
        assert all(value > 0 for value in sizes.values()), sizes
        assert conn.execute("SHOW lock_timeout").fetchone()[0] == "7s"


@pytest.mark.parametrize("case", ["unresolved", "missing", "read_error"])
def test_every_heap_size_fallback_returns_a_reason_and_announces(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch, case: str,
) -> None:
    """Review F2: each way the size read can fail leaves the phase announced."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    mapping: dict[str, str] | None = _physical_graph_tables(clean_runtime)
    expected = {
        "unresolved": "chronos interval backing tables could not be resolved",
        "missing": "backing table for okg.graph_edges was not found",
        # An invalid lock_timeout value is a real server-side error raised
        # inside the read, after the transaction has started.
        "read_error": "heap size read failed (InvalidParameterValue)",
    }[case]
    if case == "unresolved":
        mapping = None
    elif case == "missing":
        mapping = {**mapping, "okg.graph_edges": "no_such_backing_table"}
    elif case == "read_error":
        monkeypatch.setattr(
            publisher_module, "_BACKING_HEAP_BYTES_LOCK_TIMEOUT", "not a time",
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        sizes, reason = Publisher._chronos_backing_heap_bytes(conn, mapping)
        # The connection is still usable after the failed read.
        assert conn.execute("SELECT 1").fetchone()[0] == 1
    assert sizes is None
    assert reason == expected

    # And through a real publish: the same fallback prints the phase.
    monkeypatch.setattr(
        Publisher, "_chronos_backing_heap_bytes",
        staticmethod(lambda conn, given: (None, expected)),
    )
    capsys.readouterr()
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert out["graph_counts_provenance"]["long_phase_announced"] is True
    assert "first publish has no predecessor count" in capsys.readouterr().err


@pytest.mark.parametrize("over", [False, True])
def test_the_threshold_boundary_through_a_publish(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch, over: bool,
) -> None:
    """Review F3: exactly the threshold is quiet; one byte more announces."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    limit = publisher_module._FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES
    sizes = {"okg.graph_nodes": limit + (1 if over else 0), "okg.graph_edges": 0}
    monkeypatch.setattr(
        Publisher, "_chronos_backing_heap_bytes",
        staticmethod(lambda conn, mapping: (dict(sizes), None)),
    )
    capsys.readouterr()
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert out["graph_counts_provenance"]["long_phase_announced"] is over
    err = capsys.readouterr().err
    assert ("first publish has no predecessor count" in err) is over, err


def test_the_vacuum_advice_names_only_large_tables(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review nit: a 0-byte edge table is not worth a VACUUM warning."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    limit = publisher_module._FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES
    monkeypatch.setattr(
        Publisher, "_chronos_backing_heap_bytes",
        staticmethod(lambda conn, mapping: (
            {"okg.graph_nodes": limit + 1, "okg.graph_edges": 0}, None,
        )),
    )
    _plan_every_count_as_a_heap_scan(monkeypatch)
    capsys.readouterr()
    with caplog.at_level("INFO", logger=publisher_module.__name__):
        out = _publish(clean_runtime)
    assert out["status"] == "published"
    paths = out["graph_counts_provenance"]["access_path"]
    # Both tables are heap-scanned (pinned above); only the large one is named.
    assert not paths["okg.graph_nodes"]["index_only"], paths
    assert not paths["okg.graph_edges"]["index_only"], paths
    warnings = [
        r.getMessage() for r in caplog.records
        if r.levelname == "WARNING"
        and "cannot be served from an index" in r.getMessage()
    ]
    assert len(warnings) == 1, caplog.text
    assert "okg.graph_nodes" in warnings[0], warnings
    assert "okg.graph_edges" not in warnings[0], warnings
    err = capsys.readouterr().err
    advice = [line for line in err.splitlines() if "VACUUM ANALYZE" in line]
    assert len(advice) == 1 and "okg.graph_edges" not in advice[0], err


def test_the_first_publish_resolves_the_table_mapping_no_more_than_before(
    clean_runtime: str, caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review F4: the size read reuses the mapping; no extra declined warning.

    Before this change a first publish resolved the mapping four times (row
    estimate, access path, and the two graph counts). The size read shares
    the row estimate's resolution, so the total stays four.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])
    calls: list[int] = []

    def declined(branch: Any) -> None:
        calls.append(1)
        return None

    monkeypatch.setattr(
        Publisher, "_chronos_interval_physical_tables",
        staticmethod(declined),
    )
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert len(calls) == 4, len(calls)
