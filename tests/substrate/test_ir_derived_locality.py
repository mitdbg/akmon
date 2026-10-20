"""Derived locality + session execution for IR consistency checks.

PACT consistency-checks-as-ir, task.ir.derived-locality +
task.ir.session-executor. Pure plan tests pin the conservative locality
analysis (recognized shapes scope; everything else runs full with a named
reason). The live tests drive a real Chronos store on the ephemeral test DB
through the exact case the declared-frontier design got wrong: page B's edit
removing its link to page A pulls A into the derived scope, A's orphan finding
observes, and the reverse edit resolves it — all through the pinned checkout
session with NO chronos_graph_*_current mirror and NO pg_temp scope staging,
with engine-certified coverage rows and durable gate results.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import (  # noqa: E402
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
    derive_frontier_plan,
)
from okg.substrate.consistency.ir_locality import (  # noqa: E402
    SCOPE_PARAM,
    scoped_program,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.transform import (  # noqa: E402
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import (  # noqa: E402
    and_,
    col,
    eq,
    is_null,
    lit,
)
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


# --------------------------------------------------------------------------- #
# Fixture programs                                                            #
# --------------------------------------------------------------------------- #

CHECK_ATTRS = {
    "predicate_key": "page_has_inbound_link",
    "normalized_predicate": "orphan_page",
    "participant_roles": {"page": "node_id"},
    "severity": "medium",
    "normalized_value_columns": ["node_id"],
}


def _entry(*bindings, returns=("offending",)):
    return Block(
        block_id="entry",
        bindings=tuple(bindings),
        terminator=Terminator(TerminatorKind.RETURN, {"values": list(returns)}),
    )


def orphan_program() -> TransformProgram:
    """Nodes of subtype 'page' with NO inbound links_to edge (anti-join) —
    the non-local shape: a page's result changes when OTHER pages' edges do."""
    return TransformProgram(
        program_id="fixture.locality.orphan_page",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": dict(CHECK_ATTRS)},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
                Binding(
                    Op(OpKind.JOIN, {"how": "left", "on": and_(
                        eq(col("e.dst"), col("n.node_id")),
                        eq(col("e.edge_type"), lit("links_to")),
                    )}),
                    output="lj", inputs=("n", "e"),
                ),
                Binding(
                    Op(OpKind.WHERE, {"expr": and_(
                        is_null(col("e.edge_id")),
                        eq(col("n.subtype"), lit("page")),
                    )}),
                    output="orphans", inputs=("lj",),
                ),
                Binding(
                    Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id")}}),
                    output="offending", inputs=("orphans",),
                ),
                Binding(Op(OpKind.EMIT, {"target": "assertion_input"}), inputs=("offending",)),
            ),
        ),
    )


def subject_local_program() -> TransformProgram:
    return TransformProgram(
        program_id="fixture.locality.subject_local",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": dict(CHECK_ATTRS)},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                Binding(
                    Op(OpKind.WHERE, {"expr": eq(col("n.subtype"), lit("page"))}),
                    output="bad", inputs=("n",),
                ),
                Binding(
                    Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id")}}),
                    output="offending", inputs=("bad",),
                ),
                Binding(Op(OpKind.EMIT, {"target": "assertion_input"}), inputs=("offending",)),
            ),
        ),
    )


def _variant(program: TransformProgram, mutate) -> TransformProgram:
    data = program.to_dict()
    mutate(data)
    return TransformProgram.from_dict(data)


# --------------------------------------------------------------------------- #
# Pure plan analysis                                                          #
# --------------------------------------------------------------------------- #

def test_single_node_surface_is_subject_local() -> None:
    plan = derive_frontier_plan(subject_local_program(), check_attrs=CHECK_ATTRS)
    assert plan.mode == "subject_local"
    assert plan.subject_expr == {"col": "n.node_id"}


def test_edge_join_widens_to_incident_edges() -> None:
    plan = derive_frontier_plan(orphan_program(), check_attrs=CHECK_ATTRS)
    assert plan.mode == "node_with_incident_edges"


def test_node_node_join_falls_back_to_full() -> None:
    def mutate(data):
        data["blocks"][0]["bindings"][1]["op"]["args"]["surface"] = "nodes_live"
    plan = derive_frontier_plan(
        _variant(orphan_program(), mutate), check_attrs=CHECK_ATTRS
    )
    assert plan.mode == "full"
    assert "non_local_join" in plan.reason


def test_deployment_surface_base_falls_back_to_full() -> None:
    def mutate(data):
        data["blocks"][0]["bindings"][0]["op"]["args"]["surface"] = "my_claims"
    plan = derive_frontier_plan(
        _variant(subject_local_program(), mutate), check_attrs=CHECK_ATTRS
    )
    assert plan.mode == "full"
    assert "base_surface_not_nodes_live" in plan.reason


def test_group_and_limit_fall_back_to_full() -> None:
    def add_group(data):
        data["blocks"][0]["bindings"].insert(2, {
            "output": "g",
            "op": {"kind": "group", "args": {
                "key": {"k": {"col": "n.subtype"}},
                "agg": {"n": {"fn": "count"}},
            }},
            "inputs": ["bad"],
        })
    assert derive_frontier_plan(
        _variant(subject_local_program(), add_group), check_attrs=CHECK_ATTRS
    ).mode == "full"

    def add_limit(data):
        data["blocks"][0]["bindings"].insert(2, {
            "output": None,
            "op": {"kind": "limit", "args": {"n": 5}},
            "inputs": ["bad"],
        })
    plan = derive_frontier_plan(
        _variant(subject_local_program(), add_limit), check_attrs=CHECK_ATTRS
    )
    assert plan.mode == "full"
    assert plan.reason == "limit_changes_result_set"


def test_multiple_where_bindings_fall_back_to_full() -> None:
    # The canonical lowering keeps only the LAST WHERE; injecting a scope
    # conjunct anywhere else would be silently dropped (run executes full
    # while recorded scoped). The analyzer must refuse to scope instead.
    def add_second_where(data):
        data["blocks"][0]["bindings"].insert(2, {
            "output": "bad2",
            "op": {"kind": "where", "args": {"expr": {"is_null": {"col": "n.text_hash"}}}},
            "inputs": ["bad"],
        })
    plan = derive_frontier_plan(
        _variant(subject_local_program(), add_second_where),
        check_attrs=CHECK_ATTRS,
    )
    assert plan.mode == "full"
    assert plan.reason == "multiple_where_bindings"


def test_subject_must_be_a_base_relation_column() -> None:
    def mutate(data):
        # project the subject through a function -> cannot inject a scope predicate
        data["blocks"][0]["bindings"][2]["op"]["args"]["project"]["node_id"] = {
            "func": ["lower", [{"col": "n.node_id"}]]
        }
    plan = derive_frontier_plan(
        _variant(subject_local_program(), mutate), check_attrs=CHECK_ATTRS
    )
    assert plan.mode == "full"
    assert plan.reason == "subject_not_a_base_relation_column"


def test_scoped_program_is_execution_artifact_not_identity() -> None:
    program = subject_local_program()
    plan = derive_frontier_plan(program, check_attrs=CHECK_ATTRS)
    rewritten = scoped_program(program, plan)
    assert SCOPE_PARAM in rewritten.params
    assert rewritten.checksum() != program.checksum()  # rewrite changes content
    # ... but the ORIGINAL program is untouched (identity source of truth)
    assert SCOPE_PARAM not in program.params


# --------------------------------------------------------------------------- #
# Live: the B->A case through a real Chronos session                          #
# --------------------------------------------------------------------------- #

def _spec() -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="orphan_page_ir",
        check_id=f"fixture.orphan_page.{uuid.uuid4().hex[:8]}",
        predicate_family="fixture_structure",
        confidence_type="deterministic_boolean",
        program=orphan_program(),
        program_ref="tests.fixture.orphan_program",
    )


def _publish(dsn: str) -> dict:
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out
    return out


def _open_subjects(conn, check_id: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT participant_roles->>'page'
              FROM okg.observation_current
             WHERE check_id = %s AND finding_status = 'open'
            """,
            (check_id,),
        )
        return {row[0] for row in cur.fetchall()}


def _mirror_row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.chronos_graph_nodes_current')")
        if cur.fetchone()[0] is None:
            return 0
        cur.execute("SELECT count(*) FROM okg.chronos_graph_nodes_current")
        return int(cur.fetchone()[0])


def _coverage_payload(conn, check_id: str, generation_id) -> dict:
    """The persisted coverage payload jsonb for one check run at a generation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload
              FROM okg.consistency_candidate_coverage
             WHERE check_id = %s AND target_generation_id = %s
             ORDER BY created_at DESC LIMIT 1
            """,
            (check_id, generation_id),
        )
        row = cur.fetchone()
    assert row is not None, (check_id, generation_id)
    return dict(row[0])


@pytest.mark.postgres
def test_removed_edge_pulls_endpoint_into_scope_and_resolves(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    a, b, c = f"page:{sfx}:A", f"page:{sfx}:B", f"page:{sfx}:C"
    spec = _spec()
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        mirror_rows_before = _mirror_row_count(conn)
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(a, "page", {}, {"id": a}, {"v": 1}),
                NodeFact(b, "page", {}, {"id": b}, {"v": 1}),
                NodeFact(c, "page", {}, {"id": c}, {"v": 1}),
                EdgeFact(b, a, "links_to",
                         source_record_id={"id": "e-ba"}, source_revision={"v": 1}),
                EdgeFact(a, b, "links_to",
                         source_record_id={"id": "e-ab"}, source_revision={"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    gen1 = _publish(clean_runtime)["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # FULL run at gen1: A and B have inbound links; C is the only orphan.
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen1,
            catalog_version_id=1,
            deployment_name="default",
        )
        assert evaluation.result == "evaluated"
        assert _open_subjects(conn, spec.check_id) == {c}

        # B's edit removes its link to A (A itself untouched by the change) —
        # a retraction fact, the same shape a source re-ingest emits.
        store = ChronosGraphStore.from_connection(conn)
        session = store.checkout(branch="default")
        edge_rows = list(session.query(
            "SELECT edge_id FROM okg.graph_edges WHERE src=%(s)s AND dst=%(d)s",
            {"s": b, "d": a},
        ))
        edge_id = int(edge_rows[0]["edge_id"])
        store.apply_facts(
            [EdgeFact(b, a, "links_to", op="R",
                      source_record_id={"id": "e-ba"}, source_revision={"v": 2})],
            source="default",
            strict_endpoints=True,
        )
        gone = list(store.checkout(branch="default").query(
            "SELECT edge_id FROM okg.graph_edges WHERE edge_id=%(e)s",
            {"e": edge_id},
        ))
        assert gone == [], "retraction must remove the edge at head"
    gen2 = _publish(clean_runtime)["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # SCOPED run: the dirty delta is the removed edge only. Its endpoints
        # (fetched from the PREVIOUS checkpoint — the edge no longer exists at
        # head) pull A into scope; A now offends.
        dirty_scope = ({}, {"links_to": [edge_id]}, True)
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen2,
            catalog_version_id=1,
            deployment_name="default",
            dirty_scope=dirty_scope,
            previous_generation_id=gen1,
        )
        scope = dict(evaluation.preflight["scope"])
        assert scope["scope_mode"] == "node_with_incident_edges", scope
        assert _open_subjects(conn, spec.check_id) == {a, c}

        # Reverse edit: B links to A again -> scoped run resolves A.
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [EdgeFact(b, a, "links_to",
                      source_record_id={"id": "e-ba2"}, source_revision={"v": 2})],
            source="default",
            strict_endpoints=True,
        )
    gen3 = _publish(clean_runtime)["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        session = store.checkout(branch="default")
        new_edge = list(session.query(
            "SELECT edge_id FROM okg.graph_edges WHERE src=%(s)s AND dst=%(d)s",
            {"s": b, "d": a},
        ))
        dirty_scope = ({}, {"links_to": [int(new_edge[0]["edge_id"])]}, True)
        check.evaluate(
            conn,
            target_generation_id=gen3,
            catalog_version_id=1,
            deployment_name="default",
            dirty_scope=dirty_scope,
            previous_generation_id=gen2,
        )
        assert _open_subjects(conn, spec.check_id) == {c}  # A resolved, C untouched

        # And through it all: NO mirror rows were built for the IR check
        # (delta-based — the shared test DB may hold stale mirror rows from
        # legacy SQL-check tests, which the IR path must not add to).
        assert _mirror_row_count(conn) == mirror_rows_before


@pytest.mark.postgres
def test_no_delta_evidence_runs_full_with_recorded_reason(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    lone = f"page:{sfx}:lone"
    spec = _spec()
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(lone, "page", {}, {"id": lone}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    gen1 = _publish(clean_runtime)["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen1,
            catalog_version_id=1,
            deployment_name="default",
            dirty_scope=None,  # no delta evidence
        )
        scope = dict(evaluation.preflight["scope"])
        assert scope["scope_mode"] == "full"
        assert scope["scope_reason"] == "dirty_scope_unavailable"
        # engine-certified coverage: full run is universe-complete
        assert evaluation.coverage_complete is True
        assert _open_subjects(conn, spec.check_id) == {lone}

        # v_consistency_observations serves the finding (authoring-agnostic)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM okg.v_consistency_observations
                 WHERE check_id = %s AND finding_status = 'open'
                """,
                (spec.check_id,),
            )
            assert int(cur.fetchone()[0]) == 1


@pytest.mark.postgres
def test_publisher_loop_runs_ir_check_end_to_end(clean_runtime: str) -> None:
    """The REAL post-publish loop: an IR check registered on the Publisher runs
    through the loop's delta wiring (the publisher's own dirty-scope shape, not
    a hand-built 3-tuple), persists a durable gate result, and builds no mirror."""
    sfx = uuid.uuid4().hex[:8]
    lone = f"page:{sfx}:solo"
    spec = _spec()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        mirror_before = _mirror_row_count(conn)
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(lone, "page", {}, {"id": lone}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )

    publisher = Publisher(
        clean_runtime, "default",
        post_publish_consistency_checks=[IrPostPublishCheck(spec)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )
    out = publisher.cycle()
    assert out["status"] == "published", out
    summaries = publisher._last_post_publish_consistency
    assert [s["gate_name"] for s in summaries] == [spec.name]
    assert summaries[0]["result"] != "blocked_error", summaries[0]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _open_subjects(conn, spec.check_id) == {lone}
        assert _mirror_row_count(conn) == mirror_before
        # durable gate result persisted (transform-IR runtime)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.consistency_gate_results "
                "WHERE check_id = %s",
                (spec.check_id,),
            )
            assert int(cur.fetchone()[0]) == 1

    # Second cycle with a fresh node exercises the loop's OWN dirty scope
    # (the publisher's full tuple shape, not the tests' hand-built 3-tuple).
    other = f"page:{sfx}:other"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(other, "page", {}, {"id": other}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    out2 = publisher.cycle()
    assert out2["status"] == "published", out2
    summaries2 = publisher._last_post_publish_consistency
    assert summaries2 and summaries2[0]["result"] != "blocked_error", summaries2
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _open_subjects(conn, spec.check_id) == {lone, other}


@pytest.mark.postgres
def test_scoped_run_never_claims_universe_complete(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    x, y = f"page:{sfx}:X", f"page:{sfx}:Y"
    spec = _spec()
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(x, "page", {}, {"id": x}, {"v": 1}),
                NodeFact(y, "page", {}, {"id": y}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    gen1 = _publish(clean_runtime)["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen1,
            catalog_version_id=1,
            deployment_name="default",
            dirty_scope=({"page": [x]}, {}, True),  # only X changed
            previous_generation_id=None,
        )
        # scoped run: exact over its scope, NOT universe-complete, but HEALTHY --
        # an engine-certified scoped-exact run maps to result='evaluated', not the
        # old 'blocked_incomplete' (task.scope.first-class-basis-and-health).
        assert evaluation.coverage_complete is False
        assert evaluation.result == "evaluated"
        scope = dict(evaluation.preflight["scope"])
        assert scope["scope_subject_count"] == 1
        # Y was not evaluated: no observation for it, and X observed
        assert _open_subjects(conn, spec.check_id) == {x}
        # The scoped basis is auditable as FIRST-CLASS COLUMNS the gate algebra /
        # doctor / operator read directly (scoped / scope_subject_count) -- not
        # only the payload convention (which stays for backward-compat).
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT coverage_complete, scoped, scope_subject_count,
                       payload->>'scope_restricted'
                  FROM okg.consistency_candidate_coverage
                 WHERE check_id = %s AND target_generation_id = %s
                 ORDER BY created_at DESC LIMIT 1
                """,
                (spec.check_id, evaluation.target_generation_id),
            )
            assert cur.fetchone() == (False, True, 1, "true")


# --------------------------------------------------------------------------- #
# Anti-entropy oracle: a scoped run widens to a full sweep after an error or   #
# on the periodic cadence, and a full sweep reconciles stranded opens +        #
# records the observed drift. task.antientropy.oracle.                         #
# --------------------------------------------------------------------------- #


def _bump(conn_dsn: str, node_id: str) -> None:
    """Apply one fresh page node so the next publish cycle has a delta."""
    with psycopg.connect(conn_dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(node_id, "page", {}, {"id": node_id}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )


@pytest.mark.postgres
def test_forced_full_after_a_scoped_run_errors(
    clean_runtime: str, monkeypatch
) -> None:
    """A scoped run whose execution raises persists a durable blocked_error;
    the NEXT scope-eligible run of that check widens to a full sweep with
    scope_reason 'antientropy_after_error' — unverified findings get swept."""
    sfx = uuid.uuid4().hex[:8]
    x, y = f"page:{sfx}:X", f"page:{sfx}:Y"
    spec = _spec()
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(x, "page", {}, {"id": x}, {"v": 1}),
                NodeFact(y, "page", {}, {"id": y}, {"v": 1}),
                # mutual links -> neither is an orphan (findings are irrelevant
                # to this test; we only care about the error->sweep transition)
                EdgeFact(x, y, "links_to",
                         source_record_id={"id": "e-xy"}, source_revision={"v": 1}),
                EdgeFact(y, x, "links_to",
                         source_record_id={"id": "e-yx"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )
    gen1 = _publish(clean_runtime)["generation_id"]

    # Patch _execute to raise on its FIRST invocation only, then delegate to
    # the real method — run 1 errors, run 2 (and beyond) execute normally.
    real_execute = IrPostPublishCheck._execute
    state = {"raised": False}

    def flaky_execute(self, session, subjects):
        if not state["raised"]:
            state["raised"] = True
            raise RuntimeError("injected scoped-run failure")
        return real_execute(self, session, subjects)

    monkeypatch.setattr(IrPostPublishCheck, "_execute", flaky_execute)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen1,
            catalog_version_id=1,
            deployment_name="default",
            dirty_scope=({"page": [x]}, {}, True),  # scope-eligible
            previous_generation_id=None,
        )
        assert evaluation.result == "blocked_error", evaluation.result
        # durable blocked_error row exists for this check at gen1
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM okg.consistency_gate_results
                 WHERE check_id = %s AND target_generation_id = %s
                   AND result = 'blocked_error'
                """,
                (spec.check_id, evaluation.target_generation_id),
            )
            assert int(cur.fetchone()[0]) == 1

    _bump(clean_runtime, f"page:{sfx}:d2")
    gen2 = _publish(clean_runtime)["generation_id"]
    assert gen2 != gen1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation2 = check.evaluate(
            conn,
            target_generation_id=gen2,
            catalog_version_id=1,
            deployment_name="default",
            dirty_scope=({"page": [x]}, {}, True),  # WOULD be scoped
            previous_generation_id=None,
        )
        # prior run errored -> this run widened to a full sweep
        scope = dict(evaluation2.preflight["scope"])
        assert scope["scope_mode"] == "full", scope
        assert scope["scope_reason"] == "antientropy_after_error", scope
        payload = _coverage_payload(
            conn, spec.check_id, evaluation2.target_generation_id
        )
        assert payload["scope_mode"] == "full"
        assert payload["scope_reason"] == "antientropy_after_error"


@pytest.mark.postgres
def test_cadence_forces_universe_exact_sweep(
    clean_runtime: str, monkeypatch
) -> None:
    """With OKG_ANTIENTROPY_SWEEP_EVERY=2, two consecutive scoped runs are
    followed by a forced full sweep (scope_reason 'antientropy_cadence_sweep',
    universe-exact coverage_complete=True)."""
    monkeypatch.setenv("OKG_ANTIENTROPY_SWEEP_EVERY", "2")
    sfx = uuid.uuid4().hex[:8]
    x, y = f"page:{sfx}:X", f"page:{sfx}:Y"
    spec = _spec()
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(x, "page", {}, {"id": x}, {"v": 1}),
                NodeFact(y, "page", {}, {"id": y}, {"v": 1}),
                EdgeFact(x, y, "links_to",
                         source_record_id={"id": "e-xy"}, source_revision={"v": 1}),
                EdgeFact(y, x, "links_to",
                         source_record_id={"id": "e-yx"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )
    gen1 = _publish(clean_runtime)["generation_id"]

    def _scoped_eval(conn, gen):
        return check.evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=1,
            deployment_name="default",
            dirty_scope=({"page": [x]}, {}, True),
            previous_generation_id=None,
        )

    # Two scoped runs at distinct generations.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev1 = _scoped_eval(conn, gen1)
        assert dict(ev1.preflight["scope"])["scope_mode"] == "node_with_incident_edges"

    _bump(clean_runtime, f"page:{sfx}:d2")
    gen2 = _publish(clean_runtime)["generation_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev2 = _scoped_eval(conn, gen2)
        assert dict(ev2.preflight["scope"])["scope_mode"] == "node_with_incident_edges"

    # Third scope-eligible run: cadence (2 consecutive scoped) forces full.
    _bump(clean_runtime, f"page:{sfx}:d3")
    gen3 = _publish(clean_runtime)["generation_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev3 = _scoped_eval(conn, gen3)
        assert ev3.coverage_complete is True  # universe-exact sweep
        payload = _coverage_payload(conn, spec.check_id, ev3.target_generation_id)
        assert payload["scope_mode"] == "full", payload
        assert payload["scope_reason"] == "antientropy_cadence_sweep", payload
        assert payload["sweep"] == "true" or payload["sweep"] is True, payload


@pytest.mark.postgres
def test_full_sweep_resolves_stranded_open_and_records_drift(
    clean_runtime: str, monkeypatch
) -> None:
    """A scoped run can strand an open finding whose subject fell out of scope;
    the next full sweep resolves it and records the drift (stale_open >= 1)."""
    monkeypatch.setenv("OKG_ANTIENTROPY_SWEEP_EVERY", "1")
    sfx = uuid.uuid4().hex[:8]
    x, y = f"page:{sfx}:X", f"page:{sfx}:Y"
    spec = _spec()
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(x, "page", {}, {"id": x}, {"v": 1}),
                NodeFact(y, "page", {}, {"id": y}, {"v": 1}),
                # X links to Y so Y is not an orphan; NOTHING links to X, so X
                # is the orphan at gen1.
                EdgeFact(x, y, "links_to",
                         source_record_id={"id": "e-xy"}, source_revision={"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )
    gen1 = _publish(clean_runtime)["generation_id"]

    # gen1: FULL run -> X is open (orphan).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev1 = check.evaluate(
            conn, target_generation_id=gen1, catalog_version_id=1,
            deployment_name="default", dirty_scope=None,
        )
        assert dict(ev1.preflight["scope"])["scope_mode"] == "full"
        assert x in _open_subjects(conn, spec.check_id)

    # Change the graph so X no longer offends (Y -> X gives X an inbound link).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [EdgeFact(y, x, "links_to",
                      source_record_id={"id": "e-yx"}, source_revision={"v": 1})],
            source="default", strict_endpoints=True,
        )
    gen2 = _publish(clean_runtime)["generation_id"]

    # gen2: SCOPED run that MISSES X (dirty scope is Y only) -> X stays open,
    # stranded, because subjects outside the scope are never resolved. (One
    # prior full run precedes this, so the cadence streak is 0 -> stays scoped.)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev2 = check.evaluate(
            conn, target_generation_id=gen2, catalog_version_id=1,
            deployment_name="default",
            dirty_scope=({"page": [y]}, {}, True),
            previous_generation_id=None,
        )
        assert dict(ev2.preflight["scope"])["scope_mode"] == "node_with_incident_edges"
        assert x in _open_subjects(conn, spec.check_id), "X must be stranded open"

    # gen3: cadence (1 scoped run since the last full) forces a full sweep,
    # which sees X is no longer an orphan and resolves it.
    _bump(clean_runtime, f"page:{sfx}:d3")
    gen3 = _publish(clean_runtime)["generation_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ev3 = check.evaluate(
            conn, target_generation_id=gen3, catalog_version_id=1,
            deployment_name="default",
            dirty_scope=({"page": [y]}, {}, True),  # scope-eligible; forced full
            previous_generation_id=None,
        )
        scope = dict(ev3.preflight["scope"])
        assert scope["scope_mode"] == "full", scope
        assert scope["scope_reason"] == "antientropy_cadence_sweep", scope
        # X resolved by the sweep
        assert x not in _open_subjects(conn, spec.check_id)
        # drift recorded: at least one open (X) no longer offended
        payload = _coverage_payload(conn, spec.check_id, ev3.target_generation_id)
        assert int(payload["stale_open"]) >= 1, payload


@pytest.mark.postgres
def test_cadence_counts_prior_runs_sharing_a_compat_generation(
    clean_runtime: str, monkeypatch
) -> None:
    """Regression: consecutive / no-op publishes resolve to the SAME compat
    generation id, so the cadence decision must count prior scoped runs by
    RECENCY, not by ``target_generation_id < current`` (which would drop a
    prior run sharing this run's generation and never fire the sweep). Three
    scoped evaluations against the SAME generation must still trip a cadence of
    2 on the third."""
    monkeypatch.setenv("OKG_ANTIENTROPY_SWEEP_EVERY", "2")
    sfx = uuid.uuid4().hex[:8]
    x = f"page:{sfx}:X"
    spec = _spec()
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn, subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(x, "page", {}, {"id": x}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
    gen = _publish(clean_runtime)["generation_id"]  # ONE generation, reused

    reasons = []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for _ in range(3):
            ev = check.evaluate(
                conn, target_generation_id=gen, catalog_version_id=1,
                deployment_name="default",
                dirty_scope=({"page": [x]}, {}, True),  # scope-eligible
                previous_generation_id=None,
            )
            reasons.append(dict(ev.preflight["scope"])["scope_reason"])
    # First two stay scoped; the third widens despite all three sharing the
    # same compat generation id.
    assert reasons[0] != "antientropy_cadence_sweep"
    assert reasons[1] != "antientropy_cadence_sweep"
    assert reasons[2] == "antientropy_cadence_sweep", reasons
