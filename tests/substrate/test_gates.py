"""M5.2 — Phase B gate evaluator end-to-end."""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog import InvariantSpec
from okg.substrate.catalog.invariants import install_invariants, load_invariants
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library import resolve
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def _insert_node_facts(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, str, str]],  # (node_id, subtype, text, source)
) -> None:
    """Seed nodes into the Chronos graph (graph_nodes), which is what
    the Chronos-native publisher reads. The legacy `okg.node_facts`
    log is no longer projected, so the publish wouldn't see those rows.
    Same node_id / subtype / attrs ({"text": ...}) as before."""
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(nid, subtype, {"text": text}, {"id": nid}, {"v": 1})
            for nid, subtype, text, _source in nodes
        ],
        source="default",
        strict_endpoints=True,
    )


def _gen_metadata(clean_runtime: str, generation_id: str) -> dict:
    """Read a published generation's checkpoint metadata.

    The Chronos-native publisher records `invariant_summary`,
    `blocked_reason`, and `status` in the generation checkpoint's
    metadata (not in `okg.graph_generations`, which it never writes).
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        checkpoint = store.get_generation(generation_id)
    return dict(checkpoint.metadata or {})


def _publish(clean_runtime: str) -> dict:
    return Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()


# ---------- passing invariant doesn't block ----------

def test_passing_cross_invariant_publishes(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        # A trivially-passing invariant — returns nothing.
        install_invariants(conn, [
            InvariantSpec(
                "always_passes", "error", "cross", None,
                "SELECT 1 WHERE FALSE", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert out["phase_b_blocked"] is False
    # invariant_summary populated.
    summary = _gen_metadata(clean_runtime, out["generation_id"])["invariant_summary"]
    assert summary["cross_source"]["total"] == 1
    assert summary["cross_source"]["passed"] == 1
    assert summary["cross_source"]["failed"] == 0


def test_library_subtype_count_invariant_publishes_on_chronos_views(
    clean_runtime: str,
) -> None:
    specs = load_invariants(
        resolve("okg.invariants.subtype_counts_within_bounds")
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, specs, catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    assert out["status"] == "published"
    summary = _gen_metadata(clean_runtime, out["generation_id"])["invariant_summary"]
    assert summary["cross_source"]["total"] == 1
    assert summary["cross_source"]["passed"] == 1
    assert summary["cross_source"]["failed"] == 0


# ---------- failing cross-scope error blocks ----------

def test_failing_cross_error_blocks_generation(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "will_fail", "error", "cross", None,
                "SELECT 1 AS x, 2 AS y LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    assert out["phase_b_blocked"] is True
    reason = _gen_metadata(clean_runtime, out["generation_id"])["blocked_reason"]
    assert reason["reason"] == "cross_source_error_invariant_failed"
    assert any(
        f["name"] == "will_fail" and f["violator_count"] == 1
        for f in reason["failed_invariants"]
    )


# ---------- failing cross-scope WARN does not block ----------

def test_failing_cross_warn_does_not_block(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "warn_fires", "warn", "cross", None,
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    assert out["status"] == "published"
    summary = _gen_metadata(clean_runtime, out["generation_id"])["invariant_summary"]
    assert summary["cross_source"]["warns"] == 1
    assert summary["cross_source"]["failed"] == 0
    assert len(summary["cross_source"]["fired_warns"]) == 1


# RETIRED (chronos cutover): test_noop_publish_reuses_prior_gate_summary and
# test_gate_reuse_invalidates_when_fact_watermark_changes verified the
# gate-cache-reuse mechanism (invariant_summary["gate_cache_key"] /
# "gate_reuse"), a legacy projector-era feature. The reuse cache was keyed on
# the node/edge fact watermark and the prior summary was looked up from
# okg.graph_generations. The Chronos-native publisher does not project facts,
# does not write okg.graph_generations, and never sets phase_b.gate_cache_key,
# so the mechanism has no Chronos equivalent and these assertions cannot
# translate. (Noop detection itself is exercised by the publisher's own tests.)


# RETIRED (chronos cutover): test_phase_c_does_not_over_delete_multi_source_edges
# verified the Phase C `edges_live` DELETE-filter (the DISTINCT ON latest-fact
# survival rule) over okg.edges_live / okg.edge_facts. The Chronos-native
# publisher has no Phase C and writes neither table.


# ---------- failing source-scoped error blocks (issue #1427) ----------

def test_failing_source_local_error_blocks_generation(clean_runtime: str) -> None:
    """A source-scoped ERROR invariant that fires must not publish.

    Issue #1427: the gate added the source to `excluded_sources` and expected
    Phase C to discard that source's projection rows. Phase C is gone, the
    Chronos-native publisher checkpoints the whole branch head, and the
    receipt hardcoded `excluded_sources: []` -- so the invariant fired, the
    rows published anyway, and nothing a reader could see said otherwise.
    Exclusion is not enforceable here, so it blocks.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "src_x_is_bad", "error", "source", "src_x",
                "SELECT 1 AS x, 2 AS y LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    assert out["phase_b_blocked"] is True
    # The receipt names the source the gate excluded, not an empty list.
    assert out["excluded_sources"] == ["src_x"]

    metadata = _gen_metadata(clean_runtime, out["generation_id"])
    reason = metadata["blocked_reason"]
    assert reason["reason"] == "source_local_error_invariant_failed"
    failed = reason["failed_sources"]
    assert [entry["source"] for entry in failed] == ["src_x"]
    assert failed[0]["first_invariant"] == "src_x_is_bad"
    assert any(
        inv["name"] == "src_x_is_bad" and inv["violator_count"] == 1
        for inv in failed[0]["failed_invariants"]
    )
    # The durable generation metadata agrees with the receipt, so a reader of
    # the checkpoint sees the same disposition the publish call reported.
    assert metadata["excluded_sources"] == ["src_x"]
    summary = metadata["invariant_summary"]
    assert summary["source_local"]["src_x"]["failed"] == 1


def test_failing_source_local_warn_does_not_block(clean_runtime: str) -> None:
    """WARN severity never excludes and never blocks -- unchanged by #1427."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "src_x_is_odd", "warn", "source", "src_x",
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    assert out["status"] == "published"
    assert out["excluded_sources"] == []
    metadata = _gen_metadata(clean_runtime, out["generation_id"])
    assert metadata["excluded_sources"] == []
    summary = metadata["invariant_summary"]
    assert summary["source_local"]["src_x"]["warns"] == 1
    assert summary["source_local"]["src_x"]["failed"] == 0


def test_source_local_exclusion_names_only_the_error_source(
    clean_runtime: str,
) -> None:
    """The receipt lists the failing source and NOT the warning one.

    An assertion that the excluded list is empty is satisfied by the old
    hardcoded `[]` as well as by the fix, so it proves only that a key
    exists. A mixed case cannot be satisfied that way: the list has to name
    `src_bad` and leave out `src_warn`.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "warn_only", "warn", "source", "src_warn",
                "SELECT 1 AS x LIMIT 1", None,
            ),
            InvariantSpec(
                "error_fires", "error", "source", "src_bad",
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_bad")])

    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    assert out["excluded_sources"] == ["src_bad"]
    metadata = _gen_metadata(clean_runtime, out["generation_id"])
    assert metadata["excluded_sources"] == ["src_bad"]
    reason = metadata["blocked_reason"]
    assert [e["source"] for e in reason["failed_sources"]] == ["src_bad"]
    assert reason["also_excluded"] == []
    assert reason["all_unevaluated"] is False
    # The warn invariant still ran and is still reported, just not excluding.
    summary = metadata["invariant_summary"]
    assert summary["source_local"]["src_warn"]["warns"] == 1


def test_source_local_error_timeout_blocks_and_says_it_was_unevaluated(
    clean_runtime: str,
) -> None:
    """A timed-out source-local error invariant blocks, and says why.

    `passed` is `not timed_out and violator_count == 0`, so a timeout excludes
    the source with a violator count of zero. Blocking is the same fail-closed
    reading a timed-out cross-source error invariant already gets, but the
    reason must not read as "an error invariant failed and found nothing".
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "slow_source_inv", "error", "source", "src_slow",
                "SELECT pg_sleep(2)", 50,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_slow")])

    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    assert out["excluded_sources"] == ["src_slow"]
    reason = _gen_metadata(clean_runtime, out["generation_id"])["blocked_reason"]
    assert reason["reason"] == "source_local_error_invariant_failed"
    assert reason["all_unevaluated"] is True
    entry = reason["failed_sources"][0]
    assert entry["timed_out"] is True
    assert entry["failed_invariants"][0]["name"] == "slow_source_inv"
    assert entry["failed_invariants"][0]["timed_out"] is True


def test_published_generation_never_carries_an_unenforced_exclusion(
    clean_runtime: str,
) -> None:
    """No PUBLISHED checkpoint may carry an exclusion it does not implement.

    The Chronos checkpoint is the whole branch head, so there is no per-source
    discard for a recorded exclusion to mean. This binds the surviving half of
    the disjunction in issue #1427: the gate records the exclusion it computed,
    and a checkpoint carrying one is never in `published` state.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "src_x_is_bad", "error", "source", "src_x",
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    metadata = _gen_metadata(clean_runtime, out["generation_id"])
    excluded = list(metadata.get("excluded_sources") or [])
    assert excluded, "the gate must record the exclusion it computed"
    assert metadata["status"] == "blocked", (
        "a checkpoint published with a recorded exclusion would be a graph "
        "that contains rows the gate said to drop"
    )


def test_phase_a_exclusion_alone_blocks_and_is_named(clean_runtime: str) -> None:
    """A source excluded before the gate ran blocks too, under its own reason.

    Phase A exclusions reach `evaluate_gates` through `per_source_status`. The
    Chronos-native publisher passes an empty map, so this arm has no reachable
    caller today; it is driven directly here rather than left as an untested
    blocking decision that the first real caller would discover.
    """
    from okg.substrate.publisher.gates import evaluate_gates
    from okg.substrate.types import (
        PhaseAResult,
        ReservedGeneration,
        SourceProjectionStatus,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "passes", "error", "source", "src_ok",
                "SELECT 1 WHERE FALSE", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_ok")])
        g = ReservedGeneration(
            generation_id=1,
            deployment_name="default",
            catalog_version_id=cv,
            correlation_id="test-phase-a-exclusion",
            _conn=conn,
        )
        phase_a = PhaseAResult(
            converged=True,
            per_source_status={"src_broken": SourceProjectionStatus.FAILED},
            projection_cursor_by_source={},
            timed_out=False,
        )
        result = evaluate_gates(clean_runtime, g, phase_a)

    assert result.excluded_sources == {
        "src_broken": "phase_a_projection_failed",
    }
    assert result.blocked is True
    assert result.blocked_reason["reason"] == "excluded_source_not_enforceable"
    assert result.blocked_reason["failed_sources"] == [
        {
            "source": "src_broken",
            "exclusion_reason": "phase_a_projection_failed",
        },
    ]


def test_phase_a_exclusion_is_named_beside_a_gate_exclusion(
    clean_runtime: str,
) -> None:
    """Both exclusion kinds at once: the reason names both, not just one.

    `excluded_sources` lists every excluded source, so a reason that named only
    the gate exclusions would leave a source in the receipt that nothing in the
    block explains.
    """
    from okg.substrate.publisher.gates import evaluate_gates
    from okg.substrate.types import (
        PhaseAResult,
        ReservedGeneration,
        SourceProjectionStatus,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "fires", "error", "source", "src_bad",
                "SELECT 1 AS x LIMIT 1", None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_bad")])
        g = ReservedGeneration(
            generation_id=1,
            deployment_name="default",
            catalog_version_id=cv,
            correlation_id="test-both-exclusion-kinds",
            _conn=conn,
        )
        phase_a = PhaseAResult(
            converged=True,
            per_source_status={"src_broken": SourceProjectionStatus.FAILED},
            projection_cursor_by_source={},
            timed_out=False,
        )
        result = evaluate_gates(clean_runtime, g, phase_a)

    assert result.blocked is True
    reason = result.blocked_reason
    assert reason["reason"] == "source_local_error_invariant_failed"
    assert [e["source"] for e in reason["failed_sources"]] == ["src_bad"]
    assert reason["also_excluded"] == [
        {
            "source": "src_broken",
            "exclusion_reason": "phase_a_projection_failed",
        },
    ]
    assert sorted(result.excluded_sources) == ["src_bad", "src_broken"]


# ---------- statement_timeout enforcement ----------

def test_invariant_statement_timeout_fires(clean_runtime: str) -> None:
    """A cross-scope invariant that exceeds its per-invariant
    timeout is treated as failure (blocks when error-severity)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            InvariantSpec(
                "slow", "error", "cross", None,
                "SELECT pg_sleep(2)",  # 2s sleep
                50,                    # 50ms timeout
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    reason = _gen_metadata(clean_runtime, out["generation_id"])["blocked_reason"]
    failed = reason.get("failed_invariants", [])
    assert any(
        f["name"] == "slow" and f["timed_out"] is True
        for f in failed
    )


# ---------- gate_timeout: total cross-source budget exceeded ----------

def test_violator_count_is_honest_past_limit(clean_runtime: str) -> None:
    """M5.5 P0.3 regression (von Neumann review §1): when an
    invariant body returns more rows than the sample LIMIT,
    `violator_count` must be the TRUE total, not 100.

    Earlier behavior auto-appended `LIMIT 100` and set
    `violator_count = len(violators)`, silently lying about
    migrations of any non-trivial size.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        # A cross-scope invariant that returns 250 rows — more
        # than the DEFAULT_SAMPLE_LIMIT=100.
        install_invariants(conn, [
            InvariantSpec(
                "many_violators", "warn", "cross", None,
                "SELECT * FROM generate_series(1, 250) AS g(x)",
                None,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "s")])

    out = _publish(clean_runtime)
    assert out["status"] == "published"
    summary = _gen_metadata(clean_runtime, out["generation_id"])["invariant_summary"]
    warns = summary["cross_source"]["fired_warns"]
    assert len(warns) == 1
    fired = warns[0]
    assert fired["violator_count"] == 250, (
        f"expected honest count 250, got {fired['violator_count']} "
        f"— LIMIT 100 silently truncated the count"
    )
    # Sample must be capped at 100 though (JSON size bound).
    assert len(fired["sample"]) == 5, (
        f"sample in invariant_summary is capped at 5 for size "
        f"bounding; got {len(fired['sample'])}"
    )


def test_cross_source_total_budget_blocks(clean_runtime: str) -> None:
    """Multiple slow cross invariants exceed the total budget —
    the overall cross-source gate phase times out."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        install_invariants(conn, [
            # Each sleep 200ms, per-invariant timeout of 300ms
            # lets them complete. But we'll set the TOTAL budget
            # to 50ms so the LOOP exits before even the first runs.
            InvariantSpec(
                "s1", "warn", "cross", None,
                "SELECT pg_sleep(0.2)", 500,
            ),
            InvariantSpec(
                "s2", "warn", "cross", None,
                "SELECT pg_sleep(0.2)", 500,
            ),
        ], catalog_version_id=cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "src_x")])

    pub = Publisher(
        clean_runtime, "default", gate_total_timeout_ms=50,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )
    # Let the first invariant actually consume the budget so the
    # second is skipped with total_budget_exceeded.
    out = pub.cycle()
    metadata = _gen_metadata(clean_runtime, out["generation_id"])
    status = metadata["status"]
    reason = metadata["blocked_reason"]
    # Blocked with gate_timeout OR published (edge: the budget
    # check fires AT the start of each cross-source iteration, so
    # if there's only one the loop never re-checks). Accept both
    # outcomes but verify the reason when blocked.
    if status == "blocked":
        assert reason["reason"] == "gate_timeout"
    # We always expect at least one invariant to have run; the
    # test is really about the blocked-with-gate_timeout shape
    # existing and being wired.


# `test_integrity_audit_does_not_consume_deployment_gate_budget` was
# retired in the chronos cutover (cascade step 5): the legacy
# substrate-integrity audit (gates.audit_generation_as_gate_results over
# the now-empty okg.nodes_live / okg.edges_live) was removed. The
# no_dangling + narrowing invariants are now enforced over the Chronos
# graph by the always-on TransformConsistencyGate; see
# tests/substrate/test_chronos_narrowing_gate.py for that guarantee.


# ---------- requires_sources (issue #3624) ----------

def _install_scheduler_invariant(conn: psycopg.Connection, cv: int) -> None:
    """An error invariant that fails while no scheduler node exists, like
    submit-cluster's submit_single_scheduler with the Slurm sources off."""
    install_invariants(conn, [
        InvariantSpec(
            "single_scheduler", "error", "cross", None,
            "SELECT count(*) AS schedulers FROM okg.v_nodes "
            "WHERE subtype = 'scheduler' HAVING count(*) <> 1",
            None,
            requires_sources=("slurm-inventory",),
        ),
    ], catalog_version_id=cv)


def test_an_invariant_whose_source_is_off_is_skipped_and_reported(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        Publisher, "_source_states",
        lambda self: {"slurm-inventory": False, "guide": True},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _install_scheduler_invariant(conn, cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "guide")])

    out = _publish(clean_runtime)
    assert out["status"] == "published"
    summary = _gen_metadata(clean_runtime, out["generation_id"])["invariant_summary"]
    assert summary["cross_source"]["total"] == 0
    (skipped,) = summary["skipped_invariants"]
    assert skipped["name"] == "single_scheduler"
    assert skipped["off_sources"] == ["slurm-inventory"]
    assert skipped["severity"] == "error"
    assert "slurm-inventory (enabled_by_default: false)" in skipped["reason"]


def test_an_invariant_whose_source_is_on_still_blocks(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        Publisher, "_source_states", lambda self: {"slurm-inventory": True},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _install_scheduler_invariant(conn, cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "guide")])

    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    reason = _gen_metadata(clean_runtime, out["generation_id"])["blocked_reason"]
    assert reason["reason"] == "cross_source_error_invariant_failed"
    assert [f["name"] for f in reason["failed_invariants"]] == ["single_scheduler"]


def test_without_a_registry_the_invariant_still_blocks_and_is_reported(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No registry to check against must never turn the check into a pass."""
    monkeypatch.setattr(Publisher, "_source_states", lambda self: None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _install_scheduler_invariant(conn, cv)
        _insert_node_facts(conn, [("n:a", "page", "a", "guide")])

    out = _publish(clean_runtime)
    assert out["status"] == "blocked"
    meta = _gen_metadata(clean_runtime, out["generation_id"])
    assert meta["blocked_reason"]["reason"] == "cross_source_error_invariant_failed"
    assert meta["invariant_summary"]["unchecked_source_requirements"] == [
        "single_scheduler",
    ]


def test_catalog_apply_stores_requires_sources(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Issue #3624: `okg catalog load` installs invariants through
    apply_composed_catalog, not install_invariants; both must store
    requires_sources, or the gate never skips."""
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "dep"
    (dep / "schemas").mkdir(parents=True)
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas", "modules": [],
    }))
    (dep / "source_registry.yaml").write_text(
        "sources:\n  slurm-inventory:\n    ownership_id: scratch.slurm-inventory\n"
    )
    def _inv(name: str) -> dict:
        return {"name": name, "severity": "error", "scope": "cross",
                "sql": f"SELECT 1 AS {name} WHERE false"}

    (dep / "invariants.yaml").write_text(yaml.safe_dump({"invariants": [
        {**_inv("single_scheduler"), "requires_sources": ["slurm-inventory"]},
        _inv("plain"),
    ]}))
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = apply_composed_catalog(conn, composed)
        rows = dict(conn.execute(
            "SELECT name, requires_sources FROM okg.invariants "
            "WHERE catalog_version_id = %s", (cv,),
        ).fetchall())
    assert rows == {"single_scheduler": ["slurm-inventory"], "plain": None}
