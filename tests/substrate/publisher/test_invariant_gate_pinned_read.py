"""Issue #1456 — the publish gate reads the graph through a pinned checkout.

The gate used to execute deployment-authored invariant SQL on a bare pooled
connection. That connection cannot read the Chronos graph at all, and it fails
in two opposite ways: ``okg.v_nodes`` / ``okg.v_edges`` raise the honest-read
guard and abort the whole cycle, while ``okg.graph_nodes`` / ``okg.graph_edges``
are logical stubs that report a confident zero, so the invariant passes
vacuously. ``test_bare_connection_cannot_read_the_graph`` pins both halves of
that defect; every other test here proves the gate no longer inherits it.
"""
from __future__ import annotations

import time
from typing import Any

import psycopg
import pytest

from okg.substrate.catalog import InvariantSpec
from okg.substrate.catalog.invariants import install_invariants
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher import publisher as publisher_module
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer


pytestmark = pytest.mark.postgres


def _seed_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def _seed_graph(conn: psycopg.Connection, node_ids: list[str]) -> None:
    """Write nodes plus one edge between the first two into the Chronos branch."""
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
            [EdgeFact(node_ids[0], node_ids[1], "links_to", {}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )


def _publish(dsn: str) -> dict:
    return Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()


def _gen_metadata(dsn: str, generation_id: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        checkpoint = store.get_generation(generation_id)
    return dict(checkpoint.metadata or {})


def _install(dsn: str, spec: InvariantSpec, node_ids: list[str]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [spec], catalog_version_id=cv)
        _seed_graph(conn, node_ids)


def _failed_invariants(dsn: str, out: dict) -> list[dict[str, Any]]:
    reason = _gen_metadata(dsn, out["generation_id"])["blocked_reason"]
    return list(reason.get("failed_invariants", []))


# ---------------------------------------------------------------- the defect

def test_bare_connection_cannot_read_the_graph(clean_runtime: str) -> None:
    """Both halves of the defect the gate used to inherit, on one connection.

    This is the reason a pinned checkout is required rather than optional: there
    is no spelling of a graph read that a bare session answers correctly.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, ["n:a", "n:b"])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                cur.execute("SELECT node_id FROM okg.v_nodes")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.graph_nodes")
            assert cur.fetchone()[0] == 0, (
                "okg.graph_nodes must be an empty stub for a bare session; if "
                "this ever returns rows the vacuous-pass premise has changed"
            )


# ------------------------------------------------- graph surfaces read truly

@pytest.mark.parametrize(
    ("relation", "expected_violators"),
    [
        ("okg.v_nodes", 2),
        ("okg.graph_nodes", 2),
    ],
)
def test_node_invariant_sees_the_pinned_graph(
    clean_runtime: str, relation: str, expected_violators: int,
) -> None:
    """An invariant selecting every node must find exactly the seeded nodes.

    The count is the whole proof: a raise would abort the cycle with no
    generation, and the old vacuous read would report zero violators and
    publish. Only a real pinned read reports two.
    """
    _install(
        clean_runtime,
        InvariantSpec(
            "all_nodes_are_violators", "error", "cross", None,
            f"SELECT node_id FROM {relation}", None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    assert [f["violator_count"] for f in failed
            if f["name"] == "all_nodes_are_violators"] == [expected_violators]


@pytest.mark.parametrize("relation", ["okg.v_edges", "okg.graph_edges"])
def test_edge_invariant_sees_the_pinned_graph(
    clean_runtime: str, relation: str,
) -> None:
    _install(
        clean_runtime,
        InvariantSpec(
            "all_edges_are_violators", "error", "cross", None,
            f"SELECT edge_id FROM {relation}", None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    assert [f["violator_count"] for f in failed
            if f["name"] == "all_edges_are_violators"] == [1]


def test_clean_graph_invariant_publishes(clean_runtime: str) -> None:
    """The same surface, with a predicate nothing matches, must publish.

    Guards against the inverse error: a rewrite that made every graph invariant
    report violators would block every publish and still pass the tests above.
    """
    _install(
        clean_runtime,
        InvariantSpec(
            "no_page_is_named_zzz", "error", "cross", None,
            "SELECT node_id FROM okg.v_nodes WHERE node_id = 'n:zzz'", None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    summary = _gen_metadata(
        clean_runtime, out["generation_id"],
    )["invariant_summary"]
    assert summary["cross_source"]["passed"] == 1
    assert summary["cross_source"]["failed"] == 0


def test_violator_sample_is_returned_from_the_pinned_graph(
    clean_runtime: str,
) -> None:
    """The sample statement is rewritten too, not just the count statement.

    The sample runs only when the count finds violators, so a rewrite applied to
    the count alone would leave this path broken until an invariant first fired.
    """
    _install(
        clean_runtime,
        InvariantSpec(
            "sample_me", "error", "cross", None,
            "SELECT node_id FROM okg.v_nodes WHERE node_id = 'n:a'", None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    sample = next(f for f in failed if f["name"] == "sample_me")["sample"]
    assert [row["node_id"] for row in sample] == ["n:a"]


def test_quoted_relation_and_string_literal_are_handled(
    clean_runtime: str,
) -> None:
    """The rewrite is parser-based, so a quoted spelling is caught and a string
    literal naming the same relation is left alone."""
    _install(
        clean_runtime,
        InvariantSpec(
            "quoted_surface", "error", "cross", None,
            "SELECT node_id, 'okg.v_nodes' AS mentioned "
            'FROM "okg"."v_nodes"',
            None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    entry = next(f for f in failed if f["name"] == "quoted_surface")
    assert entry["violator_count"] == 2
    assert {row["mentioned"] for row in entry["sample"]} == {"okg.v_nodes"}


# ------------------------------------------------------- preserved contracts

def test_non_graph_invariant_is_unchanged(clean_runtime: str) -> None:
    _install(
        clean_runtime,
        InvariantSpec(
            "constant_violator", "error", "cross", None,
            "SELECT 1 AS x, 2 AS y LIMIT 1", None,
        ),
        ["n:a"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    assert [f["violator_count"] for f in failed
            if f["name"] == "constant_violator"] == [1]


def test_graph_invariant_statement_timeout_still_fires(
    clean_runtime: str,
) -> None:
    """The checkout runs on the caller's connection, so its SET LOCAL still binds."""
    _install(
        clean_runtime,
        InvariantSpec(
            "slow_graph_read", "error", "cross", None,
            "SELECT node_id FROM okg.v_nodes WHERE pg_sleep(2) IS NULL",
            50,
        ),
        ["n:a", "n:b"],
    )
    started = time.monotonic()
    out = _publish(clean_runtime)
    elapsed = time.monotonic() - started
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    entry = next(f for f in failed if f["name"] == "slow_graph_read")
    assert entry["timed_out"] is True
    assert elapsed < 60, "the per-invariant timeout must still bound the read"


# ------------------------------------ preflight precedes the expensive stages

def _spy_counts(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every first-publish full-count call without changing its result."""
    calls: list[str] = []
    original = Publisher._chronos_current_graph_counts

    def _recording(branch: Any) -> tuple[int, int]:
        calls.append("counted")
        return original(branch)

    monkeypatch.setattr(
        Publisher, "_chronos_current_graph_counts", staticmethod(_recording),
    )
    return calls


def test_unrunnable_invariant_refuses_before_the_full_count(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole point of issue #1456: pay milliseconds, not the count.

    The reported incident spent 3,437 seconds counting before `evaluate_gates`
    reached the invariant. Asserting the count never ran is what proves the
    preflight is actually upstream of it, rather than merely present.
    """
    _install(
        clean_runtime,
        InvariantSpec(
            "reads_missing_column", "error", "cross", None,
            "SELECT no_such_column FROM okg.v_nodes", None,
        ),
        ["n:a", "n:b"],
    )
    calls = _spy_counts(monkeypatch)
    with pytest.raises(SubstrateError) as exc:
        _publish(clean_runtime)
    assert calls == [], "the full graph count must not run for a doomed cycle"
    assert exc.value.data["reason"] == "invariant_preflight_failed"
    assert exc.value.data["invariant"] == "reads_missing_column"
    assert "reads_missing_column" in str(exc.value)


def test_preflight_also_runs_with_a_published_predecessor(
    clean_runtime: str,
) -> None:
    """Not first-publish-only: a delta cycle pays the extractor and the
    enrichers for the same nothing."""
    _install(
        clean_runtime,
        InvariantSpec(
            "always_passes", "error", "cross", None,
            "SELECT 1 WHERE FALSE", None,
        ),
        ["n:a"],
    )
    assert _publish(clean_runtime)["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "now_broken", "error", "cross", None,
                "SELECT * FROM okg.no_such_relation", None,
            ),
        ], catalog_version_id=cv)
        _seed_graph(conn, ["n:c"])

    with pytest.raises(SubstrateError) as exc:
        _publish(clean_runtime)
    assert exc.value.data["invariant"] == "now_broken"


def test_valid_invariant_lets_the_cycle_count_and_publish(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(
        clean_runtime,
        InvariantSpec(
            "fine", "error", "cross", None,
            "SELECT node_id FROM okg.v_nodes WHERE FALSE", None,
        ),
        ["n:a", "n:b"],
    )
    calls = _spy_counts(monkeypatch)
    assert _publish(clean_runtime)["status"] == "published"
    # A first publish runs the full count more than once (the initial count, the
    # post-derived-fact recount, and the pre-checkpoint reconcile). The
    # assertion that matters here is that a valid invariant lets the cycle reach
    # them at all -- the doomed-cycle test asserts the exact opposite.
    assert calls, "a valid invariant must not suppress the counts"


def test_preflight_plans_without_executing(clean_runtime: str) -> None:
    """A VOLATILE function that raises when executed must still plan clean.

    `okg.require_graph_checkout` is itself VOLATILE, so this is the property the
    preflight depends on: EXPLAIN resolves relations, columns and syntax without
    running anything.
    """
    from okg.substrate.publisher.gates import preflight_invariants

    # A session-local `pg_temp` function would be invisible to the preflight,
    # which deliberately runs on its own pooled connection, so the probe is
    # created in a shared schema.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE FUNCTION public.okg_probe_always_raises() "
                "RETURNS boolean AS "
                "$$ BEGIN RAISE EXCEPTION 'executed'; END $$ "
                "LANGUAGE plpgsql VOLATILE"
            )
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "never_executed", "error", "cross", None,
                "SELECT node_id FROM okg.v_nodes "
                "WHERE public.okg_probe_always_raises()", None,
            ),
        ], catalog_version_id=cv)
    assert preflight_invariants(
        clean_runtime, catalog_version_id=cv, generation_id=1,
    ) is None


def test_preflight_plans_the_sample_wrapper_too(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sample statement runs only once an invariant fires, so planning only
    the count wrapper would leave it unchecked until that day."""
    from okg.substrate.publisher import gates as gates_module

    monkeypatch.setattr(
        gates_module, "_invariant_statements",
        lambda inv: (
            "SELECT count(*) AS violator_count FROM (SELECT 1) _inv",
            "SELECT * FROM okg.no_such_relation",
        ),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "sample_only_break", "error", "cross", None,
                "SELECT 1 WHERE FALSE", None,
            ),
        ], catalog_version_id=cv)
    block = gates_module.preflight_invariants(
        clean_runtime, catalog_version_id=cv, generation_id=1,
    )
    assert block is not None
    assert block["statement"] == "sample"
    assert block["invariant"] == "sample_only_break"


def test_llm_judge_row_source_cannot_block_the_publish(
    clean_runtime: str, tmp_path: Any,
) -> None:
    """LLM-judge invariants run outside the publish cycle, where a failure is
    advisory. The preflight must not hand them the power to block."""
    prompt = tmp_path / "judge.md"
    prompt.write_text("judge it")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.invariants "
                "(name, severity, scope, kind, candidates_sql, judge_prompt, "
                " judge_model, sample_cap, fire_on, catalog_version_id) "
                "VALUES (%s, 'warn', 'cross', 'llm_judge', %s, %s, 'stub', 5, "
                "        ARRAY['inconsistent'], %s)",
                (
                    "judge_broken",
                    "SELECT * FROM okg.no_such_relation",
                    str(prompt),
                    cv,
                ),
            )
        _seed_graph(conn, ["n:a"])

    assert _publish(clean_runtime)["status"] == "published"


# --------------------------------------------- durable first-publish progress

def test_first_publish_announces_the_count_before_running_it(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The count is the longest unbroken stage of a first publish.

    `_emit_phase_event` is a no-op without a DBOS workflow, which is exactly the
    `okg run --once --inline` path in the report, so the announcement is
    asserted at the emit boundary rather than in the runtime event store.
    """
    from okg.substrate.publisher import publisher as publisher_module

    timeline: list[str] = []
    emitted: list[tuple[str, dict]] = []

    original_emit = publisher_module._emit_phase_event

    def _recording_emit(name: str, payload: dict) -> None:
        emitted.append((name, dict(payload)))
        timeline.append(f"emit:{name}")
        return original_emit(name, payload)

    original_counts = Publisher._chronos_current_graph_counts
    batch_at_count: list[list[Any]] = []

    def _recording_counts(branch: Any) -> tuple[int, int]:
        timeline.append("count")
        return original_counts(branch)

    original_flush = publisher_module._flush_phase_event_batch

    def _recording_flush(timings_ms: dict, batch: list) -> None:
        timeline.append("batch_flush")
        return original_flush(timings_ms, batch)

    original_stage = publisher_module._publisher_timed_stage

    def _recording_stage(timings_ms: dict, name: str):
        if name == "current_graph_counts_initial":
            batch_at_count.append(list(
                timings_ms.get(
                    publisher_module._PHASE_EVENT_BATCH_CURSOR_KEY, [],
                )
            ))
        return original_stage(timings_ms, name)

    monkeypatch.setattr(publisher_module, "_emit_phase_event", _recording_emit)
    monkeypatch.setattr(
        publisher_module, "_flush_phase_event_batch", _recording_flush,
    )
    monkeypatch.setattr(
        publisher_module, "_publisher_timed_stage", _recording_stage,
    )
    monkeypatch.setattr(
        Publisher, "_chronos_current_graph_counts",
        staticmethod(_recording_counts),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, ["n:a", "n:b"])
    assert _publish(clean_runtime)["status"] == "published"

    assert "emit:publisher_progress" in timeline
    assert timeline.index("emit:publisher_progress") < timeline.index("count")
    progress = next(p for name, p in emitted if name == "publisher_progress")
    assert progress["phase"] == "current_graph_counts_initial"
    assert progress["deployment_name"] == "default"
    assert progress["branch_name"] == "default"
    # The batched timeline is untouched: its single flush still happens at the
    # end of the cycle, so the runtime's upsert-on-key store keeps the whole
    # cycle rather than only the events after an early drain.
    assert timeline.count("batch_flush") == 1
    assert timeline.index("batch_flush") > timeline.index("count")
    # The batch is still accumulating when the count starts -- the announcement
    # must not have drained the cursor to make itself durable.
    assert batch_at_count and len(batch_at_count[0]) > 0


def test_first_publish_progress_reaches_an_inline_operator(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg run --once --inline` has no DBOS workflow and no --progress flag.

    `_emit_phase_event` no-ops without a workflow and an INFO record is dropped
    by the default handler, so neither would reach the operator who is actually
    watching the hour-long count. stderr does.

    The announcement is only made for a branch whose backing tables are over
    the quiet-size threshold (64 MiB each). A threshold of 0 bytes makes this
    two-node branch "large" through the real heap-size measurement -- each
    non-empty table has at least one 8 KiB page -- so the announcement fires
    for the same reason it fires on the hour-long branch of #1456.
    """
    monkeypatch.setattr(
        publisher_module, "_FIRST_PUBLISH_COUNT_QUIET_HEAP_BYTES", 0,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, ["n:a", "n:b"])
    capsys.readouterr()
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    provenance = out["graph_counts_provenance"]
    assert provenance["long_phase_announced"] is True, provenance
    assert all(size > 0 for size in provenance["backing_heap_bytes"].values()), (
        provenance
    )
    captured = capsys.readouterr()
    assert "counting the full graph now" in captured.err
    assert "branch=default" in captured.err
    assert "counting the full graph now" not in captured.out, (
        "progress must not contaminate stdout, which carries --json output"
    )


def test_a_small_first_publish_is_quiet_for_an_inline_operator(
    clean_runtime: str, capsys: pytest.CaptureFixture[str],
) -> None:
    """The counterpart: at the real threshold a two-node branch prints nothing.

    A fresh branch of a few kilobytes counts in milliseconds; announcing it
    on stderr (with VACUUM advice) is noise. Neither stream carries it.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, ["n:a", "n:b"])
    capsys.readouterr()
    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert out["graph_counts_provenance"]["long_phase_announced"] is False
    captured = capsys.readouterr()
    assert "counting the full graph now" not in captured.err, captured.err
    assert "VACUUM" not in captured.err, captured.err
    assert "counting the full graph now" not in captured.out


# ---------------------------------- psycopg placeholders survive the rewrite

def test_graph_invariant_binding_pin_is_evaluated(clean_runtime: str) -> None:
    """A graph body that binds `%(pin)s` must still run.

    Invariant bodies are psycopg SQL and the gate documents `pin` and
    `excluded_sources` as bindable. PostgreSQL's own parser has no `%(name)s`
    syntax, so the rewrite has to mask them; without that, every invariant using
    a documented bind became a permanent publish failure.
    """
    _install(
        clean_runtime,
        InvariantSpec(
            "binds_pin", "error", "cross", None,
            "SELECT node_id FROM okg.v_nodes WHERE %(pin)s > 0", None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    assert [f["violator_count"] for f in failed
            if f["name"] == "binds_pin"] == [2]


def test_non_graph_invariant_binding_pin_is_unaffected(
    clean_runtime: str,
) -> None:
    """The same bind on a body that reads no graph relation must keep its
    original execution path -- it must never reach the parser at all."""
    _install(
        clean_runtime,
        InvariantSpec(
            "non_graph_binds_pin", "error", "cross", None,
            "SELECT 1 AS x WHERE %(pin)s > 0", None,
        ),
        ["n:a"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    assert [f["violator_count"] for f in failed
            if f["name"] == "non_graph_binds_pin"] == [1]


def test_excluded_sources_bind_survives_the_rewrite(clean_runtime: str) -> None:
    _install(
        clean_runtime,
        InvariantSpec(
            "binds_excluded", "error", "cross", None,
            "SELECT node_id FROM okg.v_nodes "
            "WHERE node_id <> ALL(%(excluded_sources)s)", None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    assert [f["violator_count"] for f in failed
            if f["name"] == "binds_excluded"] == [2]


def test_judge_candidate_sql_reads_the_pinned_graph(clean_runtime: str) -> None:
    """The LLM-judge row source is the second executable invariant surface.

    Its wrapper always binds `%(seed)s` and `%(cap)s`, so it exercises the
    placeholder masking unconditionally, and its body reads the graph through
    the same pinned checkout as the gate.
    """
    from okg.substrate.catalog.invariants_llm import LLMJudgeInvariantRunner

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        _seed_graph(conn, ["n:a", "n:b"])

    runner = LLMJudgeInvariantRunner(
        clean_runtime,
        "default",
        {
            "name": "judge_reads_graph",
            "severity": "warn",
            "scope": "cross",
            "source": None,
            "kind": "llm_judge",
            "candidates_sql": (
                "SELECT node_id AS candidate_id, attrs AS payload "
                "FROM okg.v_nodes"
            ),
            "judge_prompt": "unused",
            "judge_model": "stub",
            "sample_cap": 5,
            "fire_on": ["inconsistent"],
            "timeout_ms": None,
            "concept_scope": None,
            "judge_spend_cap_usd": None,
            "judge_spend_estimate_usd": None,
        },
        branch_name="default",
    )
    sampled, total, reason = runner._fetch_candidates(1)
    assert reason is None, f"candidate fetch failed: {reason}"
    assert total == 2, (
        "the row source must see the pinned graph, not an empty stub"
    )
    assert {candidate_id for candidate_id, _payload in sampled} == {
        "n:a", "n:b",
    }


# ------------------------------------------------ caller-supplied connections

def test_cycle_on_a_caller_transaction_does_not_leak_preflight_settings(
    clean_runtime: str,
) -> None:
    """`Publisher.cycle(conn=...)` is called with a NON-autocommit connection by
    merge-back and by ingest. On such a connection `conn.transaction()` opens a
    savepoint, and PostgreSQL keeps `SET LOCAL` when a savepoint is RELEASED --
    they revert only on rollback. A preflight that borrowed the caller's
    connection would therefore leave READ ONLY and its short planner timeout in
    force for the rest of that transaction, and the next thing a first publish
    does is the full graph count.
    """
    _install(
        clean_runtime,
        InvariantSpec(
            "fine_for_caller_txn", "error", "cross", None,
            "SELECT node_id FROM okg.v_nodes WHERE FALSE", None,
        ),
        ["n:a", "n:b"],
    )
    with psycopg.connect(clean_runtime) as conn:
        assert conn.autocommit is False
        out = Publisher(
            clean_runtime, "default",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle(conn=conn)
        assert out["status"] == "published"
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            assert cur.fetchone()[0] == "off", (
                "the caller's transaction must not be left read-only"
            )
            cur.execute("SHOW statement_timeout")
            assert cur.fetchone()[0] != "5s", (
                "the preflight's planner timeout must not be left in force"
            )


def test_exotically_spelled_graph_relation_is_still_read_pinned(
    clean_runtime: str,
) -> None:
    """PostgreSQL resolves a Unicode-escaped identifier to the same relation.

    `okg.U&"graph_node\\0073"` is `okg.graph_nodes`, but the string
    "graph_nodes" appears nowhere in the text. A textual test for graph reads
    would call this a non-graph body and route it to the bare connection, which
    answers with the confident zero this whole change exists to remove.
    """
    _install(
        clean_runtime,
        InvariantSpec(
            "escaped_surface", "error", "cross", None,
            'SELECT node_id FROM okg.U&"graph_node\\0073"', None,
        ),
        ["n:a", "n:b"],
    )
    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    failed = _failed_invariants(clean_runtime, out)
    assert [f["violator_count"] for f in failed
            if f["name"] == "escaped_surface"] == [2], (
        "an escaped spelling must read the pinned graph, not an empty stub"
    )
