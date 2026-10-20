"""Per-check scope-decision surface — the locality observability deliverable.

PACT ``consistency-engine-hardening``, task.impl.locality-surface /
req.impl.locality-observability. For every IR check run, the engine records a
scope decision (scoped incremental subset vs. a full universe sweep, the scope
MODE, and WHY the analyzer chose it) in ``okg.consistency_candidate_coverage``.
:func:`summarize_consistency_scope_decisions` reads that decision back,
aggregated per check across recent runs, so "which checks never run
incrementally and why" is a MEASUREMENT rather than archaeology. The surface is
the deliverable — no analyzer fallback decision changes here.

These tests stand up a real test deployment with BOTH a scopeable IR check (the
orphan-page anti-join, ``node_with_incident_edges``, driven once full and once
scoped) AND a full-fallback IR check (a GROUP program the analyzer refuses to
scope). They then pin the surface's per-check report — scoped-vs-full counts,
scope-mode distribution, and the full-fallback reason (the REAL analyzer reason,
read back from the coverage row) — and prove those numbers EQUAL a direct query
over the coverage rows (the "manual read matches the coverage rows" clause), on
both the helper API and the ``consistency-scope-report`` CLI verb.
"""

from __future__ import annotations

import json
import uuid
from argparse import Namespace

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.cli.main import (  # noqa: E402
    _cmd_consistency_scope_report,
)
from okg.substrate.consistency import (  # noqa: E402
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
    run_post_publish_consistency_checks,
    summarize_consistency_scope_decisions,
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

DEPLOYMENT = "default"

# The scopeable check: pages with NO inbound links_to edge (anti-join). This is
# the node-with-incident-edges shape ir_locality CAN scope by subject.
SCOPED_CHECK_ATTRS = {
    "predicate_key": "page_has_inbound_link",
    "normalized_predicate": "orphan_page",
    "participant_roles": {"page": "node_id"},
    "severity": "medium",
    "normalized_value_columns": ["node_id"],
}

# The full-fallback check: a GROUP program. ir_locality refuses to scope any
# aggregation (a row's group can be changed by any OTHER row), so this ALWAYS
# runs full with reason `aggregation_over_other_rows`.
GROUP_CHECK_ATTRS = {
    "predicate_key": "node_group_probe",
    "normalized_predicate": "group_full_fallback",
    "participant_roles": {"node": "node_id"},
    "severity": "low",
    "normalized_value_columns": ["node_id"],
}


def _orphan_program() -> TransformProgram:
    return TransformProgram(
        program_id="fixture.locality_surface.orphan_page",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": dict(SCOPED_CHECK_ATTRS)},
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


def _group_program() -> TransformProgram:
    """FROM nodes_live, GROUP BY node_id with a COUNT — the aggregation the
    analyzer refuses to scope. A zero-matching WHERE keeps the run finding-free
    (the coverage row, the surface's subject, is written regardless), while the
    node_id group key means even a non-empty result stays emit-safe."""
    return TransformProgram(
        program_id="fixture.locality_surface.group_full",
        effect=Effect.READ_ONLY,
        outputs=("agg",),
        entry_block="entry",
        attrs={"check": dict(GROUP_CHECK_ATTRS)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": eq(
                            col("n.subtype"), lit("__okg_no_such_subtype__"),
                        )}),
                        output="filtered", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.GROUP, {
                            "key": {"node_id": col("n.node_id")},
                            "agg": {"cnt": {"fn": "count"}},
                        }),
                        output="agg", inputs=("filtered",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "assertion_input"}), inputs=("agg",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["agg"]}),
            ),
        ),
    )


def _scoped_spec() -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="orphan_page_ir",
        check_id=f"fixture.locality_surface.orphan.{uuid.uuid4().hex[:8]}",
        predicate_family="fixture_structure",
        confidence_type="deterministic_boolean",
        program=_orphan_program(),
        program_ref="tests.fixture.locality_surface.orphan_program",
    )


def _full_spec() -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="group_full_ir",
        check_id=f"fixture.locality_surface.group.{uuid.uuid4().hex[:8]}",
        predicate_family="fixture_aggregate",
        confidence_type="deterministic_boolean",
        program=_group_program(),
        program_ref="tests.fixture.locality_surface.group_program",
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
    out = Publisher(dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT)).cycle()
    assert out["status"] == "published", out


def _run_both_checks(dsn: str) -> tuple[IrConsistencyCheckSpec, IrConsistencyCheckSpec, int]:
    """Publish one lone orphan page, then run the scopeable check FULL and
    SCOPED (same check, two runs) plus the GROUP check FULL. Returns the two
    specs and the shared published generation id."""
    sfx = uuid.uuid4().hex[:8]
    lone = f"page:{sfx}:lone"
    scoped_spec = _scoped_spec()
    full_spec = _full_spec()

    _seed_lone_orphan(dsn, lone)
    _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        # Scopeable check, run #1: no explicit scope + no publish delta context
        # => derived scope has no delta to diff => FULL sweep.
        run_post_publish_consistency_checks(
            conn, [IrPostPublishCheck(scoped_spec)],
            deployment_name=DEPLOYMENT, generation_id=None,
        )
        # Scopeable check, run #2: explicit operator scope => SCOPED override.
        run_post_publish_consistency_checks(
            conn, [IrPostPublishCheck(scoped_spec)],
            deployment_name=DEPLOYMENT, generation_id=None,
            scope_subject_ids=[lone],
        )
        # Full-fallback check: the analyzer forces FULL regardless of scope.
        payload = run_post_publish_consistency_checks(
            conn, [IrPostPublishCheck(full_spec)],
            deployment_name=DEPLOYMENT, generation_id=None,
        )
    return scoped_spec, full_spec, int(payload["generation_id"])


def _raw_report_for_check(conn, check_id: str, lookback: int = 20) -> dict:
    """Recompute the per-check report DIRECTLY from the coverage rows — the
    independent oracle for the "manual read matches the coverage rows" clause."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT scoped,
                   payload->>'scope_mode'        AS scope_mode,
                   payload->>'scope_plan_reason' AS scope_plan_reason
              FROM okg.consistency_candidate_coverage
             WHERE check_id = %s
             ORDER BY created_at DESC, run_id DESC
             LIMIT %s
            """,
            (check_id, lookback),
        )
        rows = cur.fetchall()
    by_mode: dict[str, int] = {}
    by_full_reason: dict[str, int] = {}
    scoped_n = full_n = 0
    for (scoped, mode, plan_reason) in rows:
        is_scoped = bool(scoped)
        if is_scoped:
            scoped_n += 1
        else:
            full_n += 1
        label = mode if mode is not None else ("scoped" if is_scoped else "full")
        by_mode[label] = by_mode.get(label, 0) + 1
        if not is_scoped:
            rl = plan_reason if plan_reason is not None else "unrecorded"
            by_full_reason[rl] = by_full_reason.get(rl, 0) + 1
    return {
        "total_runs": len(rows),
        "scoped_run_count": scoped_n,
        "full_run_count": full_n,
        "by_mode": dict(sorted(by_mode.items())),
        "by_full_reason": dict(sorted(by_full_reason.items())),
    }


def test_locality_surface_reports_scoped_and_full_per_check(
    clean_runtime: str,
) -> None:
    """Per check, the surface reports scoped-vs-full counts, the scope-mode
    distribution, and the full-fallback reasons — with the GROUP check's reason
    CONFIRMED from the coverage row (not assumed)."""
    scoped_spec, full_spec, _gen = _run_both_checks(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = summarize_consistency_scope_decisions(
            conn, deployment_name=DEPLOYMENT,
        )

    checks = report["checks"]
    # The deployment-scoped query finds BOTH registered checks.
    assert scoped_spec.check_id in checks, sorted(checks)
    assert full_spec.check_id in checks, sorted(checks)

    # --- scopeable check: one full run + one scoped run --------------------
    scoped_rep = checks[scoped_spec.check_id]
    assert scoped_rep["total_runs"] == 2
    assert scoped_rep["scoped_run_count"] == 1
    assert scoped_rep["full_run_count"] == 1
    assert scoped_rep["by_mode"] == {"full": 1, "node_with_incident_edges": 1}
    # The scopeable check ran full for a RUNTIME reason, so its full-run
    # scope_plan_reason is the mode's analyzer plan reason; the runtime cause
    # lives per-run in recent[].scope_reason (asserted below).
    assert scoped_rep["by_full_reason"] == {"base_nodes+edges_live_joins": 1}

    recent = scoped_rep["recent"]
    assert len(recent) == 2
    scoped_run = next(r for r in recent if r["scoped"])
    full_run = next(r for r in recent if not r["scoped"])
    assert scoped_run["scope_mode"] == "node_with_incident_edges"
    assert scoped_run["scope_reason"] == "explicit_scope_subject_ids"
    assert full_run["scope_mode"] == "full"
    assert full_run["scope_reason"] == "dirty_scope_unavailable"
    assert full_run["scope_plan_reason"] == "base_nodes+edges_live_joins"

    # --- full-fallback check: analyzer forces full -------------------------
    full_rep = checks[full_spec.check_id]
    assert full_rep["total_runs"] == 1
    assert full_rep["scoped_run_count"] == 0
    assert full_rep["full_run_count"] == 1
    assert full_rep["by_mode"] == {"full": 1}
    # CONFIRMED from the coverage row, not assumed: the GROUP program is refused
    # by ir_locality with mode=full, reason=aggregation_over_other_rows.
    assert full_rep["by_full_reason"] == {"aggregation_over_other_rows": 1}
    group_run = full_rep["recent"][0]
    assert group_run["scope_mode"] == "full"
    assert group_run["scope_plan_reason"] == "aggregation_over_other_rows"
    assert group_run["scope_reason"] == "aggregation_over_other_rows"

    # The single-check filter narrows to just that check.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        only_group = summarize_consistency_scope_decisions(
            conn, deployment_name=DEPLOYMENT, check_id=full_spec.check_id,
        )
    assert set(only_group["checks"]) == {full_spec.check_id}


def test_locality_surface_counts_equal_raw_coverage_rows(
    clean_runtime: str,
) -> None:
    """The manual-read clause: the surface's per-check numbers EQUAL a direct
    query over okg.consistency_candidate_coverage for that check — asserted
    against the raw rows, not merely internal consistency."""
    scoped_spec, full_spec, _gen = _run_both_checks(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = summarize_consistency_scope_decisions(
            conn, deployment_name=DEPLOYMENT,
        )
        for spec in (scoped_spec, full_spec):
            raw = _raw_report_for_check(conn, spec.check_id)
            surfaced = report["checks"][spec.check_id]
            for field in (
                "total_runs",
                "scoped_run_count",
                "full_run_count",
                "by_mode",
                "by_full_reason",
            ):
                assert surfaced[field] == raw[field], (spec.check_id, field)


def test_locality_surface_last_n_window_is_per_check(
    clean_runtime: str,
) -> None:
    """The last-N window is PER CHECK, not a global LIMIT (adversarial-review
    F1). The scopeable check has 2 coverage rows and the GROUP check 1, all
    created in sequence (GROUP most recent). With lookback=1, a per-check window
    returns BOTH checks (each capped at its single most-recent run); a GLOBAL
    limit would return only the one most-recent row across all checks and evict
    the scopeable check entirely."""
    scoped_spec, full_spec, _gen = _run_both_checks(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = summarize_consistency_scope_decisions(
            conn, deployment_name=DEPLOYMENT, lookback=1,
        )
    checks = report["checks"]
    # Both checks survive lookback=1 — the window did not evict one globally.
    assert scoped_spec.check_id in checks, checks
    assert full_spec.check_id in checks, checks
    # Each capped at its single most-recent run.
    assert checks[scoped_spec.check_id]["total_runs"] == 1, checks
    assert checks[full_spec.check_id]["total_runs"] == 1, checks


def test_locality_surface_cli_payload_carries_per_check_report(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The `consistency-scope-report` CLI verb emits the per-check report. DSN
    comes from --dsn (first in the deployment-DSN precedence), so no manifest is
    needed — --deployment is purely the query filter here."""
    scoped_spec, full_spec, _gen = _run_both_checks(clean_runtime)

    args = Namespace(
        deployment=DEPLOYMENT,
        dsn=clean_runtime,
        check_id=None,
        lookback=20,
        json=True,
    )
    rc = _cmd_consistency_scope_report(args, as_json=True)
    assert rc == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["deployment_name"] == DEPLOYMENT
    checks = payload["checks"]
    assert scoped_spec.check_id in checks
    assert full_spec.check_id in checks
    assert checks[scoped_spec.check_id]["scoped_run_count"] == 1
    assert checks[scoped_spec.check_id]["full_run_count"] == 1
    assert checks[full_spec.check_id]["by_full_reason"] == {
        "aggregation_over_other_rows": 1
    }
