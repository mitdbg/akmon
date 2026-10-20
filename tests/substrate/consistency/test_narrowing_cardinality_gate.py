"""fix-projection-alias-identity #710: functional edges cannot silently
accumulate multi-target states.

Pre-fix, narrowings were a bare (parent_subtype, edge_type, child_subtype)
triple with no cardinality annotation, the transform-consistency gate checked
only dangling/narrowing/subtype conformance, and no storage layer enforced
uniqueness on (src, edge_type). So a semantically one-per-source edge
(bib_reference resolves_to paper) could accumulate twin targets while the gate
stayed green.

The fix (ruling 3) adds a `cardinality: at_most_one_target` attr_rules
annotation on the narrowing and a fourth transform-gate program that reports a
violation when two live edges share (src, edge_type) under such a narrowing.
Un-annotated narrowings keep today's semantics.
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.consistency import TransformConsistencyGate
from okg.substrate.consistency.observations import (
    complete_consistency_run,
    start_consistency_run,
)
from okg.substrate.consistency.transform_gate import (
    _legacy_cardinality_transform_gate_definition,
    _legacy_endpoint_cardinality_transform_gate_definition,
    _legacy_transform_gate_definition,
    _transform_gate_definition,
)
from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _publisher(dsn: str) -> Publisher:
    return Publisher(dsn, "default", consistency_gates=[TransformConsistencyGate()], catalog_identity=bind_test_catalog_writer(dsn, "default"))


def _annotate_cardinality(
    conn: psycopg.Connection, cv: int, parent: str, edge_type: str
) -> None:
    """Stamp `cardinality: at_most_one_target` onto an existing narrowing row
    (models what modules_apply now writes from the schema annotation)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.narrowings
               SET attr_rules = %s::jsonb
             WHERE catalog_version_id = %s
               AND parent_subtype = %s
               AND edge_type = %s
            """,
            (
                json.dumps({"cardinality": "at_most_one_target"}),
                cv, parent, edge_type,
            ),
        )


def test_legacy_and_cardinality_gate_runs_keep_distinct_durable_identity(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        definitions = [
            _legacy_transform_gate_definition("default"),
            _legacy_cardinality_transform_gate_definition("default"),
            _legacy_endpoint_cardinality_transform_gate_definition("default"),
            _transform_gate_definition("default"),
        ]
        run_ids = []
        for target, definition in enumerate(definitions, start=101):
            run_id = start_consistency_run(
                conn,
                definition,
                target_generation_id=target,
                trigger_kind="full",
            )
            complete_consistency_run(conn, run_id=run_id, metrics={})
            run_ids.append(run_id)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT definition_id, compute_hash FROM "
                "okg.derived_artifact_runs WHERE run_id = ANY(%s) "
                "ORDER BY definition_id",
                (run_ids,),
            )
            rows = cur.fetchall()

    assert {row[0] for row in rows} == {
        "transform.consistency.no_dangling+narrowing",
        "transform.consistency.no_dangling+narrowing+cardinality.v2",
        "transform.consistency.no_dangling+narrowing+cardinality.v3",
        "transform.consistency.no_dangling+narrowing+cardinality+derived_node_liveness.v4",
    }
    assert len({row[1] for row in rows}) == 4


def test_twin_resolves_to_blocks_when_annotated(clean_runtime: str) -> None:
    """The #710 scenario: two producers emit `resolves_to` edges to DIFFERENT
    targets from one bib_reference source. With the at-most-one-target
    annotation, the gate reports the offending edges and BLOCKS."""
    sfx = uuid.uuid4().hex[:8]
    bib = f"bib:{sfx}"
    paper_a, paper_b = f"paper:{sfx}:a", f"paper:{sfx}:b"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
        _annotate_cardinality(conn, cv, "bib_reference", "resolves_to")
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(bib, "bib_reference", {}, {"id": bib}, {"v": 1}),
                NodeFact(paper_a, "paper", {}, {"id": paper_a}, {"v": 1}),
                NodeFact(paper_b, "paper", {}, {"id": paper_b}, {"v": 1}),
                # Two live resolves_to edges from the SAME bib to DIFFERENT
                # papers -- the twin-target state #710 describes.
                EdgeFact(
                    bib, paper_a, "resolves_to",
                    source_record_id={"id": "e1"}, source_revision={"v": 1},
                ),
                EdgeFact(
                    bib, paper_b, "resolves_to",
                    source_record_id={"id": "e2"}, source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out
    details = out["consistency_gates"][0]["details"]
    offending = set(details["cardinality_violation_edge_ids"])
    assert offending == {
        _edge_id(bib, "resolves_to", paper_a),
        _edge_id(bib, "resolves_to", paper_b),
    }, details


def test_twin_resolves_to_passes_when_unannotated(clean_runtime: str) -> None:
    """Control: the SAME twin-target state without the annotation keeps today's
    semantics -- the gate stays green (un-annotated narrowings are unaffected).
    This is exactly the pre-fix silent-wrongness the annotation closes."""
    sfx = uuid.uuid4().hex[:8]
    bib = f"bib:{sfx}"
    paper_a, paper_b = f"paper:{sfx}:a", f"paper:{sfx}:b"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
        # NO cardinality annotation.
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(bib, "bib_reference", {}, {"id": bib}, {"v": 1}),
                NodeFact(paper_a, "paper", {}, {"id": paper_a}, {"v": 1}),
                NodeFact(paper_b, "paper", {}, {"id": paper_b}, {"v": 1}),
                EdgeFact(
                    bib, paper_a, "resolves_to",
                    source_record_id={"id": "e1"}, source_revision={"v": 1},
                ),
                EdgeFact(
                    bib, paper_b, "resolves_to",
                    source_record_id={"id": "e2"}, source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out
    details = out["consistency_gates"][0]["details"]
    assert details["cardinality_violation_edge_ids"] == []


def test_single_target_passes_when_annotated(clean_runtime: str) -> None:
    """A well-formed one-target resolves_to under the annotation publishes."""
    sfx = uuid.uuid4().hex[:8]
    bib, paper = f"bib:{sfx}", f"paper:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
        _annotate_cardinality(conn, cv, "bib_reference", "resolves_to")
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(bib, "bib_reference", {}, {"id": bib}, {"v": 1}),
                NodeFact(paper, "paper", {}, {"id": paper}, {"v": 1}),
                EdgeFact(
                    bib, paper, "resolves_to",
                    source_record_id={"id": "e1"}, source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out
    assert out["consistency_gates"][0]["details"][
        "cardinality_violation_edge_ids"
    ] == []


def _twin_corpus_gate_row(dsn: str, *, annotate: bool, twin: bool = True) -> dict:
    """Publish the SAME twin-target corpus, differing ONLY in whether the
    cardinality family is active, and return the DURABLE gate row.

    Reads okg.consistency_gate_results rather than the in-memory cycle result:
    the requirement is that a reader of the PUBLISHED generation can tell the
    two runs apart, so the persisted row is the only admissible witness.
    """
    sfx = uuid.uuid4().hex[:8]
    bib = f"bib:{sfx}"
    paper_a, paper_b = f"paper:{sfx}:a", f"paper:{sfx}:b"
    with psycopg.connect(dsn, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
        if annotate:
            _annotate_cardinality(conn, cv, "bib_reference", "resolves_to")
        facts = [
            NodeFact(bib, "bib_reference", {}, {"id": bib}, {"v": 1}),
            NodeFact(paper_a, "paper", {}, {"id": paper_a}, {"v": 1}),
            NodeFact(paper_b, "paper", {}, {"id": paper_b}, {"v": 1}),
            EdgeFact(
                bib, paper_a, "resolves_to",
                source_record_id={"id": "e1"}, source_revision={"v": 1},
            ),
        ]
        if twin:
            # The second, violating target. Omitted for the CLEAN control.
            facts.append(
                EdgeFact(
                    bib, paper_b, "resolves_to",
                    source_record_id={"id": "e2"}, source_revision={"v": 1},
                )
            )
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts,
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(dsn).cycle()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT result, reason, coverage_complete, coverage_basis, details
                  FROM okg.consistency_gate_results
                 WHERE gate_name = 'transform_consistency_gate'
                 ORDER BY created_at DESC
                 LIMIT 1
                """
            )
            row = cur.fetchone()
    assert row is not None, "gate row was not persisted"
    result, reason, coverage_complete, coverage_basis, details = row
    return {
        "status": out["status"],
        "result": result,
        "reason": reason,
        "coverage_complete": coverage_complete,
        "coverage_basis": coverage_basis,
        "evaluated_families": details.get("evaluated_families"),
        "skipped_families": details.get("skipped_families"),
        "runtime": details.get("runtime"),
        "cardinality_violation_edge_ids": details.get(
            "cardinality_violation_edge_ids"
        ),
    }


def test_inactive_family_is_distinguishable_from_a_clean_complete_run(
    clean_runtime: str,
) -> None:
    """THE SILENT PATH (PACT finding iii). Two runs over the SAME corpus, one
    with the cardinality family active and one without, must not be
    indistinguishable in the published output.

    Pre-change the unannotated run reported result=evaluated,
    reason=no_violations, coverage_complete=true,
    coverage_basis=chronos_transform_ir+cardinality_sql and an empty violation
    list -- byte-identical to a clean complete evaluation -- while the
    annotated twin blocked on the exact violating edge ids. So a reader saw a
    clean bill of health for a check that never ran.
    """
    from tests.substrate.conftest import reset_okg_test_runtime

    # (a) family ACTIVE on the violating corpus: blocks, naming the edge ids.
    #     This is what proves the violation in (b) is REAL, not absent.
    blocked = _twin_corpus_gate_row(clean_runtime, annotate=True)
    reset_okg_test_runtime(clean_runtime)
    # (b) family INACTIVE on the SAME violating corpus: looks clean.
    skipped = _twin_corpus_gate_row(clean_runtime, annotate=False)
    reset_okg_test_runtime(clean_runtime)
    # (c) family ACTIVE on a NON-violating corpus: genuinely clean.
    #     (b) vs (c) is the pair an operator must be able to tell apart --
    #     "cardinality: never ran" vs "cardinality: clean".
    clean = _twin_corpus_gate_row(clean_runtime, annotate=True, twin=False)

    assert blocked["status"] == "blocked", blocked
    assert len(blocked["cardinality_violation_edge_ids"]) == 2, blocked
    assert skipped["status"] == "published", skipped
    assert skipped["cardinality_violation_edge_ids"] == [], skipped
    assert clean["status"] == "published", clean
    assert clean["cardinality_violation_edge_ids"] == [], clean

    # THE DECISIVE ASSERTION. Pre-change this tuple was IDENTICAL for (b) and
    # (c) -- ('evaluated', 'no_violations', True,
    # 'chronos_transform_ir+cardinality_sql', None, ()) -- so a skipped family
    # was byte-indistinguishable from a clean complete evaluation.
    def disclosure(row: dict) -> tuple:
        return (
            row["result"],
            row["reason"],
            row["coverage_complete"],
            row["coverage_basis"],
            tuple(row["evaluated_families"] or ()),
            tuple(sorted(
                entry["family"] for entry in (row["skipped_families"] or [])
            )),
        )

    assert disclosure(skipped) != disclosure(clean), (
        "a run whose cardinality family never activated is indistinguishable "
        f"from a clean complete run: both report {disclosure(clean)}"
    )

    # ... and it differs by NAMING the skipped family, not by some incidental
    # count. The name and the failed activation predicate are both present.
    assert "cardinality" not in (skipped["evaluated_families"] or [])
    assert [
        entry["family"] for entry in skipped["skipped_families"]
    ] == ["cardinality"], skipped
    entry = skipped["skipped_families"][0]
    assert entry["skip_reason"] == "activation_predicate_false", entry
    assert "at_most_one_target" in entry["activation_predicate"], entry
    assert entry["activation_source"] == "okg.narrowings", entry
    assert "cardinality" in (clean["evaluated_families"] or []), clean
    assert clean["skipped_families"] == [], clean

    # The first-class column carries it too, so a reader who never opens the
    # details JSON still cannot mistake a partial constraint set for a total
    # one -- and the basis no longer NAMES a check that did not run.
    assert skipped["coverage_basis"] == (
        "chronos_transform_ir+skipped_families:cardinality"
    ), skipped
    assert clean["coverage_basis"] == (
        "chronos_transform_ir+cardinality_sql"
    ), clean
    assert blocked["coverage_basis"] == (
        "chronos_transform_ir+cardinality_sql"
    ), blocked

    # details.runtime carried the SAME false claim as coverage_basis: it named
    # cardinality_sql unconditionally, so fixing only the basis would have left
    # the identical lie standing one field over. A reader who consults runtime
    # instead of the basis must reach the same conclusion, not the opposite one.
    assert skipped["runtime"] == (
        "chronos_transform_ir+skipped_families:cardinality"
    ), skipped
    assert clean["runtime"] == "chronos_transform_ir+cardinality_sql", clean
    assert blocked["runtime"] == "chronos_transform_ir+cardinality_sql", blocked


def _seed_incremental_scoped_run(dsn: str) -> dict:
    """Publish a clean full generation, then a second cycle whose delta is ONE
    new edge, so the second run takes the incremental SCOPED path. Returns the
    ids involved plus the durable gate + coverage rows for that scoped run."""
    sfx = uuid.uuid4().hex[:8]
    bib_a, bib_b = f"bib:{sfx}:a", f"bib:{sfx}:b"
    paper = f"paper:{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
        _annotate_cardinality(conn, cv, "bib_reference", "resolves_to")
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(bib_a, "bib_reference", {}, {"id": bib_a}, {"v": 1}),
                NodeFact(paper, "paper", {}, {"id": paper}, {"v": 1}),
                EdgeFact(
                    bib_a, paper, "resolves_to",
                    source_record_id={"id": "e1"}, source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    assert _publisher(dsn).cycle()["status"] == "published"

    # Second cycle: ONE new node + ONE new edge. Everything else is untouched
    # and therefore UNEVALUATED by the scoped run -- the set a reader must be
    # able to recover.
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(bib_b, "bib_reference", {}, {"id": bib_b}, {"v": 1}),
                EdgeFact(
                    bib_b, paper, "resolves_to",
                    source_record_id={"id": "e2"}, source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(dsn).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, coverage_complete, coverage_basis,
                       preflight, details
                  FROM okg.consistency_gate_results
                 WHERE gate_name = 'transform_consistency_gate'
                 ORDER BY created_at DESC
                 LIMIT 1
                """
            )
            gate = cur.fetchone()
            cur.execute(
                """
                SELECT run_id, scoped, coverage_complete, coverage_basis,
                       scope_subject_count, payload
                  FROM okg.consistency_candidate_coverage
                 WHERE run_id = %s
                """,
                (gate[0],),
            )
            coverage = cur.fetchone()
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout(branch="default")
        live_edge_ids = {
            int(r["edge_id"])
            for r in session.query("SELECT edge_id FROM okg.graph_edges", {})
        }
    return {
        "edge_pre_existing": _edge_id(bib_a, "resolves_to", paper),
        "edge_new": _edge_id(bib_b, "resolves_to", paper),
        "node_new": bib_b,
        "live_edge_ids": live_edge_ids,
        "gate": gate,
        "coverage": coverage,
    }


def test_scoped_run_persists_scope_identity_not_only_counts(
    clean_runtime: str,
) -> None:
    """A scoped run must persist WHICH subjects it evaluated, so the reader can
    compute the complement -- the UNEVALUATED set.

    Pre-change the incremental transform gate persisted dirty_edge_count=1 /
    scoped_edge_count=1 and no identifiers at all, so the complement was not
    recoverable from the published generation by any means.
    """
    seeded = _seed_incremental_scoped_run(clean_runtime)
    _, _, _, preflight, details = seeded["gate"]

    identity = details["scope_identity"]
    scoped_edge_ids = {int(value) for value in identity["scoped_edge_ids"]}

    # IDENTITY, not cardinality. This assertion is unsatisfiable by any count:
    # it names the exact edge the scoped run evaluated.
    assert scoped_edge_ids == {seeded["edge_new"]}, identity
    assert identity["dirty_node_ids"] == [seeded["node_new"]], identity
    assert identity["scoped_edge_ids_truncated"] is False, identity
    assert identity["scoped_edge_digest_sha256"], identity

    # THE COMPLEMENT, computed by a reader from the published generation alone:
    # live edges MINUS the evaluated scope = what went unevaluated.
    complement = seeded["live_edge_ids"] - scoped_edge_ids
    assert complement == {seeded["edge_pre_existing"]}, {
        "live": seeded["live_edge_ids"],
        "scoped": scoped_edge_ids,
    }
    assert "MINUS" in identity["scope_complement_recipe"]["edge_families"]

    # Identity also rides on the gate row's preflight, because
    # consistency_candidate_coverage is read by no CLI surface (audit finding
    # v) while consistency_gate_results is what `okg generation describe` reads.
    assert preflight["scope_identity"]["scoped_edge_ids"] == identity[
        "scoped_edge_ids"
    ]


def test_scoped_run_no_longer_bypasses_the_coverage_check(
    clean_runtime: str,
) -> None:
    """The incremental gate wrote coverage_complete=TRUE while scoped AND wrote
    ZERO rows to okg.consistency_candidate_coverage, so the schema's one
    structural honesty constraint -- CHECK (NOT (coverage_complete AND scoped))
    -- never applied to it. The scoped run now lands a real coverage row that
    the CHECK governs, and the gate row stops claiming universe-completeness.
    """
    seeded = _seed_incremental_scoped_run(clean_runtime)
    _, gate_coverage_complete, gate_basis, _, details = seeded["gate"]
    coverage = seeded["coverage"]

    # A row EXISTS for the scoped run (it used to write none at all).
    assert coverage is not None, "scoped run wrote no coverage row"
    run_id, scoped, coverage_complete, basis, subject_count, payload = coverage
    assert scoped is True, coverage
    assert coverage_complete is False, coverage
    assert basis == "exact_postgres", coverage
    assert subject_count > 0, coverage
    assert payload["scope_identity"]["scoped_edge_ids"], payload

    # The gate row agrees: scoped is not universe-complete.
    assert gate_coverage_complete is False, seeded["gate"]
    assert gate_basis.startswith("chronos_dirty_edges"), gate_basis

    # The honest claim SURVIVES: the verdict is still universe-valid, by the
    # guarded inductive delta argument, recorded where it can be checked
    # instead of being smuggled in as coverage_complete.
    assert details["verdict_universe_valid_by_induction"] is True, details
    assert details["induction_guard"] == (
        "previous and current catalogs have the same applied content identity"
    ), details

    # And the CHECK now actually governs this path: flipping the persisted row
    # to the pre-change claim is REJECTED by the database.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE okg.consistency_candidate_coverage "
                    "SET coverage_complete = true WHERE run_id = %s",
                    (run_id,),
                )


def test_cardinality_violation_blocks_on_incremental_delta(
    clean_runtime: str,
) -> None:
    """After a clean publish, adding a SECOND resolves_to target to a
    pre-existing bib in a later cycle is caught by the incremental scoped
    cardinality check (the added edge's src is a non-dirty pre-existing node)."""
    sfx = uuid.uuid4().hex[:8]
    bib = f"bib:{sfx}"
    paper_a, paper_b = f"paper:{sfx}:a", f"paper:{sfx}:b"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
        _annotate_cardinality(conn, cv, "bib_reference", "resolves_to")
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(bib, "bib_reference", {}, {"id": bib}, {"v": 1}),
                NodeFact(paper_a, "paper", {}, {"id": paper_a}, {"v": 1}),
                NodeFact(paper_b, "paper", {}, {"id": paper_b}, {"v": 1}),
                EdgeFact(
                    bib, paper_a, "resolves_to",
                    source_record_id={"id": "e1"}, source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    assert _publisher(clean_runtime).cycle()["status"] == "published"

    # Second cycle: add the twin target. The bib node is NOT dirty; only the
    # new edge is. The scoped cardinality check resolves the edge's src and
    # detects the >1 group.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                EdgeFact(
                    bib, paper_b, "resolves_to",
                    source_record_id={"id": "e2"}, source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )
    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out
    details = out["consistency_gates"][0]["details"]
    offending = set(details["cardinality_violation_edge_ids"])
    assert offending == {
        _edge_id(bib, "resolves_to", paper_a),
        _edge_id(bib, "resolves_to", paper_b),
    }, details
