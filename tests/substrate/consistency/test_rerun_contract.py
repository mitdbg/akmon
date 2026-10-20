"""consistency-rerun run-record + scope-override contract, IR-path end to end.

PACT ``consistency-engine-hardening``, task.impl.rerun-contract /
req.impl.rerun-integrity. The multiplicity contract is *documented
multiplicity, not uniqueness*: each ``run_post_publish_consistency_checks``
invocation creates exactly one new terminal ``derived_artifact_runs`` row per
check; N reruns leave N terminal runs, each with its own coverage + gate rows
(history preserved — no upsert / uniqueness collapse). "No unbounded orphans"
is met by an operator-visible count
(:func:`summarize_consistency_run_multiplicity`) that exposes multiplicity and
any non-terminal (still-``running``) orphan.

These tests drive the REAL rerun flow (``run_post_publish_consistency_checks``
with an ``IrPostPublishCheck`` against a real published generation) TWICE
against the SAME generation and pin: two distinct terminal run ids; the
``scope_subject_ids`` override recording ``explicit_scope_subject_ids`` with
``scoped``/``coverage_complete=False``; gate rows for both runs present and
consistent; and the count surface reporting the two runs with zero
non-terminal. A focused test proves the count surface flags a non-terminal
(orphan) run so accumulation is detectable, not silent.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import (  # noqa: E402
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
    complete_consistency_run,
    run_post_publish_consistency_checks,
    start_consistency_run,
    summarize_consistency_run_multiplicity,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
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
from okg.substrate.transform.expr import and_, col, eq, is_null, lit  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


CHECK_ATTRS = {
    "predicate_key": "page_has_inbound_link",
    "normalized_predicate": "orphan_page",
    "participant_roles": {"page": "node_id"},
    "severity": "medium",
    "normalized_value_columns": ["node_id"],
}


def _orphan_program() -> TransformProgram:
    """Pages with NO inbound links_to edge (anti-join) — the node-with-
    incident-edges shape the scope analyzer can scope by subject."""
    return TransformProgram(
        program_id="fixture.rerun.orphan_page",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": dict(CHECK_ATTRS)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
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
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["offending"]}),
            ),
        ),
    )


def _spec() -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="orphan_page_ir",
        check_id=f"fixture.rerun.orphan.{uuid.uuid4().hex[:8]}",
        predicate_family="fixture_structure",
        confidence_type="deterministic_boolean",
        program=_orphan_program(),
        program_ref="tests.fixture.rerun.orphan_program",
    )


def _seed_lone_orphan(dsn: str, node_id: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(node_id, "page", {}, {"id": node_id}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )


def _publish(dsn: str) -> None:
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out


def _run_status(conn, run_id: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.derived_artifact_runs WHERE run_id = %s",
            (uuid.UUID(run_id),),
        )
        row = cur.fetchone()
    assert row is not None, f"no run row for {run_id}"
    return str(row[0])


def _coverage_by_run(conn, run_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT coverage_complete, scoped, scope_subject_count, payload
              FROM okg.consistency_candidate_coverage
             WHERE run_id = %s
            """,
            (uuid.UUID(run_id),),
        )
        row = cur.fetchone()
    assert row is not None, f"no coverage row for {run_id}"
    return {
        "coverage_complete": row[0],
        "scoped": row[1],
        "scope_subject_count": row[2],
        "payload": dict(row[3] or {}),
    }


def _gate_results_by_run(conn, run_id: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT result, blocks_publish
              FROM okg.consistency_gate_results
             WHERE run_id = %s
            """,
            (uuid.UUID(run_id),),
        )
        return [(str(r[0]), bool(r[1])) for r in cur.fetchall()]


def test_scoped_ir_run_persists_subject_identity_so_complement_is_computable(
    clean_runtime: str,
) -> None:
    """A scoped IR run must persist WHICH subjects it evaluated.

    Pre-change the IR executor was the best-behaved scoped path -- honest that
    it trimmed (scoped=true, coverage_complete=false, scope_reason recorded) --
    but COUNT-ONLY: scope_subject_count=1 and no identifiers, so the
    unevaluated complement was not recoverable from the published generation.
    Two orphan pages are seeded and exactly ONE is scoped, so a count of 1
    cannot distinguish which; only identity can.
    """
    sfx = uuid.uuid4().hex[:8]
    scoped_page, unevaluated_page = f"page:{sfx}:a", f"page:{sfx}:b"
    spec = _spec()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "links_to", "page")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(scoped_page, "page", {}, {"id": scoped_page}, {"v": 1}),
                NodeFact(
                    unevaluated_page, "page", {},
                    {"id": unevaluated_page}, {"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    _publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        payload = run_post_publish_consistency_checks(
            conn,
            [IrPostPublishCheck(spec)],
            deployment_name="default",
            generation_id=None,
            scope_subject_ids=[scoped_page],
        )
    run_id = payload["checks"][0]["run_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        coverage = _coverage_by_run(conn, run_id)
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout(branch="default")
        universe = {
            str(r["node_id"])
            for r in session.query(
                "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'page'", {}
            )
        }

    cov_payload = coverage["payload"]
    assert coverage["scoped"] is True, coverage
    assert coverage["coverage_complete"] is False, coverage

    # IDENTITY. Unsatisfiable by any count: it names the subject evaluated.
    assert cov_payload["scope_subject_ids"] == [scoped_page], cov_payload
    assert cov_payload["scope_subject_ids_truncated"] is False, cov_payload
    assert cov_payload["scope_subject_digest_sha256"], cov_payload

    # THE COMPLEMENT a reader computes: subject universe MINUS the scope.
    complement = universe - set(cov_payload["scope_subject_ids"])
    assert complement == {unevaluated_page}, {
        "universe": universe, "payload": cov_payload,
    }
    assert "MINUS" in cov_payload["scope_complement_recipe"]


def test_rerun_twice_yields_two_terminal_runs_scope_override_and_count(
    clean_runtime: str,
) -> None:
    """The full rerun flow twice against the SAME published generation:
    (a) two distinct run ids, both terminal (never left 'running');
    (b) the scope_subject_ids run records reason=explicit_scope_subject_ids,
        scoped + coverage_complete=False; the non-scoped run is universe
        complete; (c) gate rows for BOTH runs present and consistent;
    (d) the operator-visible count reports 2 runs, 0 non-terminal."""
    sfx = uuid.uuid4().hex[:8]
    lone = f"page:{sfx}:lone"
    spec = _spec()

    _seed_lone_orphan(clean_runtime, lone)
    _publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Invocation #1: no explicit scope -> FULL run (universe-complete).
        payload1 = run_post_publish_consistency_checks(
            conn,
            [IrPostPublishCheck(spec)],
            deployment_name="default",
            generation_id=None,
        )
        # Invocation #2: explicit operator scope -> scoped override.
        payload2 = run_post_publish_consistency_checks(
            conn,
            [IrPostPublishCheck(spec)],
            deployment_name="default",
            generation_id=None,
            scope_subject_ids=[lone],
        )

    # Both invocations targeted the SAME published generation.
    assert payload1["generation_id"] == payload2["generation_id"]

    summary1 = payload1["checks"][0]
    summary2 = payload2["checks"][0]
    run_id1 = summary1["run_id"]
    run_id2 = summary2["run_id"]

    # (a) two DISTINCT runs, BOTH terminal (neither stuck 'running').
    assert run_id1 != run_id2
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        status1 = _run_status(conn, run_id1)
        status2 = _run_status(conn, run_id2)
    assert status1 != "running" and status2 != "running", (status1, status2)
    assert status1 == "completed" and status2 == "completed", (status1, status2)

    # (b) scope-override contract vs universe-complete full run.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cov1 = _coverage_by_run(conn, run_id1)
        cov2 = _coverage_by_run(conn, run_id2)
    # non-scoped full run: universe complete, not scope-restricted.
    assert cov1["coverage_complete"] is True
    assert cov1["scoped"] is False
    # scoped override run: exact over its subject scope, NEVER universe complete.
    assert cov2["coverage_complete"] is False
    assert cov2["scoped"] is True
    assert cov2["scope_subject_count"] == 1
    assert cov2["payload"]["scope_reason"] == "explicit_scope_subject_ids"
    assert cov2["payload"]["scope_restricted"] is True

    # (c) gate rows for BOTH runs present and consistent.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gates1 = _gate_results_by_run(conn, run_id1)
        gates2 = _gate_results_by_run(conn, run_id2)
    assert len(gates1) == 1 and len(gates2) == 1, (gates1, gates2)
    (result1, blocks1) = gates1[0]
    (result2, blocks2) = gates2[0]
    # non-blocking observation gates: neither errored, both agree.
    assert blocks1 is False and blocks2 is False
    assert result1 != "blocked_error" and result2 != "blocked_error"
    assert result1 == result2, (result1, result2)
    # both summaries echo the persisted gate result.
    assert summary1["result"] == result1
    assert summary2["result"] == result2

    # (d) operator-visible count: #1 sees 1 run; #2 sees 2 runs, 0 orphaned.
    runs1 = summary1["runs"]
    runs2 = summary2["runs"]
    assert runs1["runs_total"] == 1
    assert runs1["runs_non_terminal"] == 0
    assert runs2["runs_total"] == 2
    assert runs2["runs_terminal"] == 2
    assert runs2["runs_non_terminal"] == 0
    assert runs2["by_status"] == {"completed": 2}

    # The count surface, queried directly, agrees for this (check, generation).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        direct = summarize_consistency_run_multiplicity(
            conn,
            definition_id=spec.check_id,
            target_generation_id=int(payload2["generation_id"]),
        )
    assert direct["runs_total"] == 2
    assert direct["runs_non_terminal"] == 0


class _RaisingIrCheck:
    """A check whose evaluate() crashes, with ``name`` (the display gate_name)
    distinct from ``spec.check_id`` (the durable definition_id) — the mismatch
    the crash-path count must key correctly through."""

    def __init__(self, spec: IrConsistencyCheckSpec) -> None:
        self.spec = spec
        self.name = spec.name

    def evaluate(self, conn, **_kw):  # noqa: ANN001 - test stub
        raise RuntimeError("mid-evaluate crash")


def test_rerun_crash_path_count_keys_off_definition_id_not_gate_name(
    clean_runtime: str,
) -> None:
    """Regression (adversarial review 2026-07-07): on the CRASH path the
    summary check_id is the display gate_name (spec.name != spec.check_id). The
    multiplicity count must key off the DURABLE definition id (spec.check_id),
    or it queries a non-existent id and wrong-zeros the orphan gauge EXACTLY on
    the error path — the silent-wrong-default class. A real prior run PLUS a
    genuine 'running' orphan at (spec.check_id, gen) must both surface in the
    crashed check's runs block."""
    sfx = uuid.uuid4().hex[:8]
    lone = f"page:{sfx}:lone"
    spec = _spec()
    assert spec.name != spec.check_id  # the whole point of the finding

    _seed_lone_orphan(clean_runtime, lone)
    _publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # A real successful run establishes a terminal run at the durable id via
        # the same resolution path (so its generation matches the crash count's).
        good = run_post_publish_consistency_checks(
            conn, [IrPostPublishCheck(spec)],
            deployment_name="default", generation_id=None,
        )
        gen = good["generation_id"]
        # A genuine non-terminal orphan at the durable id + this generation.
        start_consistency_run(
            conn, spec.definition("default"),
            target_generation_id=int(gen), trigger_kind="post_publish",
        )
        # The raising check (same spec) crashes -> crash summary keyed on the
        # display gate_name.
        crash = run_post_publish_consistency_checks(
            conn, [_RaisingIrCheck(spec)],
            deployment_name="default", generation_id=gen,
        )

    csum = crash["checks"][0]
    assert csum["result"] == "blocked_error"            # the crash path
    assert csum["check_id"] == spec.name                # display gate_name...
    assert csum["check_id"] != spec.check_id            # ...not the durable id
    runs = csum["runs"]
    # Keyed off spec.check_id: sees the prior run AND the orphan. The old
    # gate_name key would query a non-existent definition_id -> runs_total 0.
    assert runs["runs_total"] == 2, runs
    assert runs["runs_non_terminal"] == 1, runs
    assert runs["by_status"].get("running") == 1, runs


def test_rerun_count_surface_flags_a_non_terminal_run(
    clean_runtime: str,
) -> None:
    """The multiplicity count is the orphan gauge: a run left in 'running'
    (a crashed/killed or in-flight rerun) is reported as non-terminal, so
    accumulation is DETECTABLE — not silently swallowed. Completing it flips
    the gauge to zero."""
    spec = _spec()
    definition = spec.definition("default")
    target_gen = 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # A run that never reaches a terminal status (simulated orphan).
        orphan_run = start_consistency_run(
            conn,
            definition,
            target_generation_id=target_gen,
            trigger_kind="post_publish",
        )
        summary = summarize_consistency_run_multiplicity(
            conn,
            definition_id=spec.check_id,
            target_generation_id=target_gen,
        )
        assert summary["runs_total"] == 1
        assert summary["runs_non_terminal"] == 1
        assert summary["runs_terminal"] == 0
        assert summary["by_status"] == {"running": 1}

        # Once it reaches a terminal status the gauge clears.
        complete_consistency_run(conn, run_id=orphan_run)
        after = summarize_consistency_run_multiplicity(
            conn,
            definition_id=spec.check_id,
            target_generation_id=target_gen,
        )
        assert after["runs_total"] == 1
        assert after["runs_non_terminal"] == 0
        assert after["runs_terminal"] == 1
        assert after["by_status"] == {"completed": 1}
