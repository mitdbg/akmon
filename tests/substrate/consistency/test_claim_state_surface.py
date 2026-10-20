"""The effective/canonical claim state as a declared IR check surface.

PACT ``consistency-claim-graph-binding``, task.binding.surface
(req.binding.resolution-visible). An IR consistency check declares the
claim-state projection (``okg.claim_projection``) as a read surface and runs
through the pinned post-publish executor. Proven here:

* the check LOADS as an ``IrConsistencyCheckSpec`` and is recognized as reading
  the claim-state surface (by kept table, not surface name);
* derived locality falls back to FULL with the CLAIM-SPECIFIC reason
  ``claim_state_surface_bucket_locality_pending`` (never the generic
  ``base_surface_not_nodes_live``), while the graph-surface scoped path is
  UNAFFECTED — witnessed both ways;
* it RUNS PINNED and EMITS FINDINGS: seeded across two generations, a run pinned
  to gen1 emits findings for gen1's claim state ONLY. The pin is load-bearing —
  the physical (unpinned) kept table holds BOTH generations' rows, so a read
  that skipped the pin would mix them; the gen1 assertion excludes gen2's bucket
  exactly because the checkout's interval visibility scopes the read;
* the coverage row records the run HONESTLY: scoped=false, universe-complete,
  full mode with the claim reason, and the claim-state surfaces it pinned;
* the raw-connection (unpinned) ``run_rows`` path is REFUSED for a claim-state
  check — reads are constrained to the pinned executor.

Seeding + publish wiring follow tests/substrate/test_claim_projection_derivation.py
(the derivation harness); the executor idiom follows
tests/substrate/test_ir_derived_locality.py.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency import (  # noqa: E402
    CLAIM_STATE_FULL_REASON,
    IrCheckLoadError,
    IrCheckSurface,
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
    TransformConsistencyGate,
    derive_frontier_plan,
)
from okg.substrate.consistency.claim_model.precedence import (  # noqa: E402
    OUTCOME_CONTESTED,
)
from okg.substrate.derived_artifacts.claim_effective_derivation import (  # noqa: E402
    claim_effective_derivation_row,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    CLAIM_PROJECTION_TABLE,
    ROW_KIND_RESOLUTION,
    bucket_row_key,
    claim_projection_derivation_row,
)
from okg.substrate.derived_artifacts.derivation_registry import (  # noqa: E402
    DerivationRegistry,
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
from okg.substrate.transform.expr import and_, col, eq, lit  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_DEPLOYMENT = "default"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"

# The claim-state surface: a deployment-chosen name bound to the projection kept
# table. Recognition is by the binding's kept table, so the name is arbitrary.
_PROJECTION_SURFACE = IrCheckSurface(
    name="claim_projection_rows", binding=CLAIM_PROJECTION_TABLE
)


# --------------------------------------------------------------------------- #
# Fixture check: contested (unresolved) bucket resolutions — the af_pubs /     #
# okg-workspace IR-check authoring idiom, over the claim-state surface.        #
# --------------------------------------------------------------------------- #
def contested_bucket_program() -> TransformProgram:
    """A bucket whose ≻ resolution is CONTESTED (an unresolved conflict the
    operator must adjudicate). Subject: the bucket (its (predicate,key) row_key).
    Reads the claim-state projection surface, never the raw claim nodes."""
    return TransformProgram(
        program_id="fixture.claim_state.contested_bucket",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "claim_bucket_resolved",
            "normalized_predicate": "claim_bucket_contested",
            "participant_roles": {"bucket": "row_key"},
            "severity": "medium",
            # Per-assignment finding identity (PACT consistency-finding-identity):
            # the stored assignment is (row_key, outcome); the scope suffix keys
            # each bucket so resolutions stay independently resolvable.
            "normalized_value_columns": ["row_key", "outcome"],
            "finding_scope_columns": ["row_key"],
        }},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {
                            "surface": _PROJECTION_SURFACE.name, "alias": "r",
                        }),
                        output="r",
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": and_(
                            eq(col("r.row_kind"), lit(ROW_KIND_RESOLUTION)),
                            eq(col("r.outcome"), lit(OUTCOME_CONTESTED)),
                        )}),
                        output="flagged", inputs=("r",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {
                            "row_key": col("r.row_key"),
                            "outcome": col("r.outcome"),
                            "predicate": col("r.predicate"),
                            "bucket_key": col("r.bucket_key"),
                        }}),
                        output="offending", inputs=("flagged",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "offending"}),
                            inputs=("offending",)),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN, {"values": ["offending"]}
                ),
            ),
        ),
    )


def _spec() -> IrConsistencyCheckSpec:
    program = contested_bucket_program()
    return IrConsistencyCheckSpec(
        name="claim_bucket_contested_ir",
        check_id=f"fixture.claim_state.contested_bucket.{uuid.uuid4().hex[:8]}",
        predicate_family="claim_state_resolution_consistency",
        confidence_type="deterministic_boolean",
        program=program,
        program_ref=program.program_id,
        surfaces=(_PROJECTION_SURFACE,),
    )


# --------------------------------------------------------------------------- #
# A tiny graph-surface (nodes_live) check — to witness the scoped path is      #
# UNAFFECTED by the claim-state recognition.                                   #
# --------------------------------------------------------------------------- #
_GRAPH_CHECK_ATTRS = {
    "predicate_key": "node_present",
    "normalized_predicate": "graph_subject_local",
    "participant_roles": {"node": "node_id"},
    "severity": "low",
    "normalized_value_columns": ["node_id"],
}


def _graph_subject_local_program() -> TransformProgram:
    return TransformProgram(
        program_id="fixture.claim_state.graph_subject_local",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": dict(_GRAPH_CHECK_ATTRS)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                            output="n"),
                    Binding(
                        Op(OpKind.WHERE, {"expr": eq(col("n.subtype"), lit("page"))}),
                        output="bad", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id")}}),
                        output="offending", inputs=("bad",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "offending"}),
                            inputs=("offending",)),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN, {"values": ["offending"]}
                ),
            ),
        ),
    )


# --------------------------------------------------------------------------- #
# Pure tests (no DB): declaration + derived locality both ways.               #
# --------------------------------------------------------------------------- #
def test_check_declares_and_is_recognized_as_claim_state_surface() -> None:
    spec = _spec()
    # loads: structural identity derives from the program (sha-shaped hashes).
    definition = spec.definition(_DEPLOYMENT)
    assert isinstance(definition.candidate_rule_hash, str)
    assert len(definition.candidate_rule_hash) >= 32
    # recognized by the kept table the binding reads, not the surface name.
    assert spec.reads_claim_state is True
    assert spec.claim_state_binding_tables() == frozenset({CLAIM_PROJECTION_TABLE})
    assert spec.claim_state_surface_names() == frozenset({_PROJECTION_SURFACE.name})


def test_frontier_plan_records_claim_specific_full_reason() -> None:
    spec = _spec()
    plan = derive_frontier_plan(
        spec.program,
        check_attrs=spec.program.attrs["check"],
        claim_state_surfaces=spec.claim_state_surface_names(),
    )
    assert plan.mode == "full"
    assert plan.scoped is False
    assert plan.reason == CLAIM_STATE_FULL_REASON

    # Without the recognition input the SAME program still falls back to full,
    # but with the GENERIC reason — so the specific reason is exactly what the
    # claim-state recognition contributes.
    generic = derive_frontier_plan(
        spec.program, check_attrs=spec.program.attrs["check"]
    )
    assert generic.mode == "full"
    assert generic.reason.startswith("base_surface_not_nodes_live")
    assert generic.reason != CLAIM_STATE_FULL_REASON


def test_graph_surface_scoped_path_is_unaffected() -> None:
    # A nodes_live check still classifies subject_local; passing an (empty, or
    # unrelated) claim_state_surfaces set does not perturb it.
    program = _graph_subject_local_program()
    plan = derive_frontier_plan(program, check_attrs=_GRAPH_CHECK_ATTRS)
    assert plan.mode == "subject_local"
    plan2 = derive_frontier_plan(
        program,
        check_attrs=_GRAPH_CHECK_ATTRS,
        claim_state_surfaces=frozenset({"some_other_surface"}),
    )
    assert plan2.mode == "subject_local"
    assert plan2.subject_expr == {"col": "n.node_id"}


def test_run_rows_refused_for_claim_state_surface() -> None:
    # The raw-connection (UNPINNED) path is refused: it would read the kept
    # table across all generations. Raises before touching the connection.
    spec = _spec()
    with pytest.raises(IrCheckLoadError, match="claim-state surface"):
        spec.run_rows(None)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Live tests (postgres): the check runs pinned and emits generation-scoped     #
# findings; the pin is load-bearing.                                           #
# --------------------------------------------------------------------------- #
def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)


def _claim_fact(node_id: str, value: str, *, key: str, source: str,
                source_class: str) -> NodeFact:
    return NodeFact(
        node_id, "consistency_claim",
        {
            "predicate": _PREDICATE, "normalized_key": key, "value": value,
            "scope": _SCOPE, "scope_state": "known",
            "claim_provenance": "derived", "source_id": source,
            "source_class": source_class,
        },
        {"id": node_id}, {"v": 1},
    )


def _functional_policy(node_id: str) -> NodeFact:
    return NodeFact(
        node_id, "predicate_policy",
        {"enabled": "true", "policy_id": _CONSTRAINT,
         "policy_kind": "functional", "predicate": _PREDICATE},
        {"id": node_id}, {"v": 1},
    )


def _precedent_fact(node_id: str, prefer_source: str) -> NodeFact:
    return NodeFact(
        node_id, "claim_precedent",
        {"precedent_id": f"pid:{node_id}", "prefer_source": prefer_source,
         "scope": _SCOPE, "sequence": 1, "decided_by": "operator:riley"},
        {"id": node_id}, {"v": 1},
    )


def _registry() -> DerivationRegistry:
    registry = DerivationRegistry()
    registry.register(claim_effective_derivation_row())
    registry.register(claim_projection_derivation_row())
    return registry


def _cycle(dsn: str, registry: DerivationRegistry) -> Any:
    out = Publisher(
        dsn, _DEPLOYMENT,
        consistency_gates=[TransformConsistencyGate()],
        maintained_derivations=registry,
        catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT),
    ).cycle()
    assert out["status"] == "published", out
    return out["generation_id"]


def _open_buckets(conn: Any, check_id: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT participant_roles->>'bucket'
              FROM okg.observation_current
             WHERE check_id = %s AND finding_status = 'open'
            """,
            (check_id,),
        )
        return {row[0] for row in cur.fetchall()}


def _coverage_row(conn: Any, check_id: str, generation_id: Any) -> tuple[Any, dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT coverage_complete, scoped, payload
              FROM okg.consistency_candidate_coverage
             WHERE check_id = %s AND target_generation_id = %s
             ORDER BY created_at DESC LIMIT 1
            """,
            (check_id, generation_id),
        )
        row = cur.fetchone()
    assert row is not None, (check_id, generation_id)
    return (row[0], row[1]), dict(row[2])


def _physical_contested_buckets(conn: Any) -> set[str]:
    """UNPINNED read of the physical interval kept table: every non-deleted
    contested resolution across ALL generations (no checkout, no interval
    visibility filter). This is the generation-mixed set the pin excludes."""
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    backend = store.context._backend
    backend.refresh_registries()
    physical = backend.tables[CLAIM_PROJECTION_TABLE].physical_name
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT row_key FROM "{physical}" '
            "WHERE row_kind = %s AND outcome = %s AND deleted = FALSE",
            (ROW_KIND_RESOLUTION, OUTCOME_CONTESTED),
        )
        return {row[0] for row in cur.fetchall()}


@pytest.mark.postgres
def test_claim_state_check_runs_pinned_and_emits_generation_scoped_findings(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    c, d = f"claim:c:{sfx}", f"claim:d:{sfx}"
    key1, key2 = f"key|one:{sfx}", f"key|two:{sfx}"
    rk1, rk2 = bucket_row_key(_PREDICATE, key1), bucket_row_key(_PREDICATE, key2)
    registry = _registry()
    spec = _spec()
    check = IrPostPublishCheck(spec)

    # gen1: a,b conflict on key1 with equal signals -> CONTESTED bucket key1.
    _apply(clean_runtime, [
        _claim_fact(a, "vX", key=key1, source="page:1", source_class="release_notes"),
        _claim_fact(b, "vY", key=key1, source="page:2", source_class="wiki"),
        _functional_policy(f"pol:{sfx}"),
    ])
    gen1 = _cycle(clean_runtime, registry)

    # gen2: a precedent resolves key1 -> CANONICAL; new c,d conflict on key2
    # (both wiki, no applicable precedent winner) -> CONTESTED bucket key2.
    _apply(clean_runtime, [
        _precedent_fact(f"prec:{sfx}", prefer_source="release_notes"),
        _claim_fact(c, "vP", key=key2, source="page:3", source_class="wiki"),
        _claim_fact(d, "vQ", key=key2, source="page:4", source_class="wiki"),
    ])
    gen2 = _cycle(clean_runtime, registry)

    # ---- runs pinned to gen1: findings reflect gen1's claim state ONLY ------ #
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Both generations' rows now physically coexist in the kept table: an
        # UNPINNED read mixes them (gen1 key1-contested + gen2 key2-contested).
        unpinned = _physical_contested_buckets(conn)
        assert rk1 in unpinned and rk2 in unpinned, unpinned

        evaluation = check.evaluate(
            conn,
            target_generation_id=gen1,
            catalog_version_id=1,
            deployment_name=_DEPLOYMENT,
            dirty_scope=None,
        )
        assert evaluation.result == "evaluated", evaluation.result
        # Load-bearing: the pin excludes gen2's key2 (which physically exists) —
        # this assertion FAILS if the read is not pinned to gen1.
        pinned = _open_buckets(conn, spec.check_id)
        assert pinned == {rk1}
        # The leak the pin prevents, made explicit: an UNPINNED read would have
        # returned strictly more (gen2's key2 too); the pin dropped exactly it.
        assert pinned < unpinned
        assert unpinned - pinned == {rk2}

        # Coverage recorded honestly: universe-complete, NOT scoped, full mode
        # with the claim-specific reason, and the pinned claim-state surface.
        (coverage_complete, scoped), payload = _coverage_row(
            conn, spec.check_id, evaluation.target_generation_id
        )
        assert (coverage_complete, scoped) == (True, False)
        assert payload["scope_mode"] == "full"
        assert payload["scope_reason"] == CLAIM_STATE_FULL_REASON
        assert payload["scope_plan_reason"] == CLAIM_STATE_FULL_REASON
        assert payload["scope_restricted"] is False
        assert payload["claim_state_surfaces"] == [_PROJECTION_SURFACE.name]
        assert payload["claim_state_pinned_generation"] == str(gen1)

    # ---- the pin SELECTS the generation: gen2 sees key2, resolves key1 ------ #
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        check.evaluate(
            conn,
            target_generation_id=gen2,
            catalog_version_id=1,
            deployment_name=_DEPLOYMENT,
            dirty_scope=None,
        )
        assert _open_buckets(conn, spec.check_id) == {rk2}


# --------------------------------------------------------------------------- #
# The explicit pin's precondition (unit): the executor verifies each claim-    #
# state kept table is Chronos-registered at the checkout, so the interval      #
# visibility actually scopes it — refusing LOUDLY otherwise rather than        #
# reading raw rows across generations.                                         #
# --------------------------------------------------------------------------- #
class _FakeBackend:
    def __init__(self, tables: dict[str, Any]) -> None:
        self.tables = dict(tables)
        self.refreshed = False

    def refresh_registries(self) -> None:
        self.refreshed = True


class _FakeStore:
    def __init__(self, tables: dict[str, Any]) -> None:
        self.context = type("Ctx", (), {"_backend": _FakeBackend(tables)})()


def test_pin_guard_raises_when_claim_table_unregistered() -> None:
    from okg.substrate.errors import SubstrateError

    spec = _spec()
    store = _FakeStore({})  # no registered tables -> pin cannot apply
    with pytest.raises(SubstrateError, match="Chronos-registered"):
        IrPostPublishCheck._assert_claim_state_pinned(store, spec)
    assert store.context._backend.refreshed is True  # refreshed before deciding


def test_pin_guard_passes_when_claim_table_registered() -> None:
    spec = _spec()
    store = _FakeStore({CLAIM_PROJECTION_TABLE: object()})
    # registered -> no raise (the checkout's interval visibility will scope it).
    IrPostPublishCheck._assert_claim_state_pinned(store, spec)
