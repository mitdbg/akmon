"""End-to-end decision-consumption proof for the tier-1 effective-claims derivation.

task.core.decision-consumption (consistency-claim-model-core), the publish
half: on a scratch deployment composing the REAL ["extraction",
"consistency"] library modules, operator decisions recorded as graph nodes
(claim corrections + a precedent) are consumed by the NEXT publish cycle
through the #444 maintained-derivation registry — the kept table
``okg.claim_effective`` changes (row deltas asserted per pinned generation),
decisions stick across re-ingest of an unchanged corpus WITH the
``stale_correction_review`` flag firing when the target re-derives
identically, a vanished claim's effective row disappears and its decision
expires visibly, and every generation passes the CallableCorrectnessOracle
parity check against the pure library (``merge_effective_claims``).

Follows tests/substrate/test_deployment_maintained_derivation_consumers.py
(publisher-wired registry rows) + the scratch-deployment catalog install
from tests/substrate/test_catalog_consistency_module_publish.py.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
import yaml

psycopg = pytest.importorskip("psycopg")

from chronos_core.branching._common import invalidate_registry_cache  # noqa: E402
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.consistency.claim_model.applicability import (  # noqa: E402
    Applicability,
    TimeBounds,
)
from okg.substrate.consistency.claim_model.views import (  # noqa: E402
    ClaimSemantics,
)
from okg.substrate.derived_artifacts.claim_effective_derivation import (  # noqa: E402
    CLAIM_EFFECTIVE_TABLE,
    DERIVATION_ID,
    REASON_NEVER_EXISTED,
    REASON_VANISHED,
    ROW_KIND_EFFECTIVE,
    ROW_KIND_EXPIRED,
    ROW_KIND_PRECEDENT,
    VALUE_COLUMNS,
    claim_effective_derivation_row,
    compute_rows,
    compute_rows_interpreted,
    library_truth_rows,
    rows_as_truth,
)
from okg.substrate.derived_artifacts.maintained_derivation import (  # noqa: E402
    FULL_REASON_DELETION_SHAPE,
    FULL_REASON_FIRST_PUBLISH,
    FULL_RECOMPUTE_CAUSES,
)
from okg.substrate.derived_artifacts.derivation_registry import (  # noqa: E402
    DerivationRegistry,
    FailureMode,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.derived_artifacts.maintained_derivation import (  # noqa: E402
    parity_check,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (  # noqa: E402
    SENTINEL_TOKEN,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "scratch",
            "schema_dir": "schemas",
            "modules": ["extraction", "consistency"],
        })
    )
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)


def _claim_fact(
    node_id: str, value: str, rev: int = 1, *, key: str | None = None,
    **extra_attrs: Any,
) -> NodeFact:
    attrs = {
            "predicate": "preferred_target",
            # Default: each claim its own bucket. Pass ``key`` to place several
            # claims in ONE (predicate, normalized_key) bucket (the §9.8 witness
            # needs a bucket whose recompute re-derives MORE than one row).
            "normalized_key": key if key is not None else f"key|{node_id}",
            "value": value,
            "scope": "HCAL/Run3",
            "scope_state": "known",
            "claim_provenance": "derived",
            "source_id": "page:1",
        }
    attrs.update(extra_attrs)
    return NodeFact(
        node_id,
        "consistency_claim",
        attrs,
        {"id": node_id},
        {"v": rev},
    )


def _typed_semantics(activity: str = "installation") -> str:
    return ClaimSemantics(
        logical_subject="entity:shared",
        activity=activity,
        applicability=Applicability(
            heading_path="HCAL/Run3",
            heading_state="known",
            valid_time=TimeBounds.known("2025-01-01", "2026-01-01"),
        ),
    ).to_attr()


def _correction_fact(
    node_id: str, target: str, action: str, sequence: int, **payload: Any
) -> NodeFact:
    attrs = {
        "correction_id": f"cid:{node_id}",
        "target_claim_id": target,
        "action": action,
        "sequence": sequence,
        "decided_by": "operator:riley",
    }
    attrs.update(payload)
    return NodeFact(node_id, "claim_correction", attrs, {"id": node_id}, {"v": 1})


def _observed(predicate: str, normalized_key: str, value: str, scope: str) -> str:
    return json.dumps({
        "predicate": predicate,
        "normalized_key": normalized_key,
        "value": value,
        "scope": scope,
    })


def _publisher(dsn: str, registry: DerivationRegistry) -> Publisher:
    return Publisher(
        dsn,
        _DEPLOYMENT,
        consistency_gates=[TransformConsistencyGate()],
        maintained_derivations=registry,
        catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT),
    )


def _kept_rows(checkout: Any) -> dict[tuple[str, str], tuple[Any, ...]]:
    columns = ", ".join(("row_kind", "row_key", *VALUE_COLUMNS))
    rows = checkout.query(
        f"SELECT {columns} FROM {CLAIM_EFFECTIVE_TABLE} "
        f"WHERE NOT (row_kind = :s AND row_key = :s)",
        {"s": SENTINEL_TOKEN},
    )
    return {
        (str(r["row_kind"]), str(r["row_key"])): tuple(
            r[column] for column in VALUE_COLUMNS
        )
        for r in rows
    }


def _col(row: tuple, name: str):
    return row[VALUE_COLUMNS.index(name)]


def _cycle(
    dsn: str, registry: DerivationRegistry, *, expect_noop: bool = False
) -> dict[str, Any]:
    out = _publisher(dsn, registry).cycle()
    assert out["status"] == "published", out
    # A step that re-ingests an unchanged corpus writes nothing (the store does
    # not rewrite an equal payload), so the publisher reuses the previous
    # generation and never reaches derivation maintenance. Every other step
    # must publish and maintain: a change the store wrongly treated as
    # unchanged would otherwise pass as a reuse.
    assert bool(out.get("noop")) is expect_noop, out
    if expect_noop:
        assert out.get("reused_generation") is True, out
        assert "maintained_derivations" not in out, out
        return out
    summary = out["maintained_derivations"][DERIVATION_ID]
    assert summary["status"] == "maintained", summary
    return out


def _assert_parity(dsn: str, row: Any, generation_id: Any) -> None:
    """Oracle parity for the PINNED generation: kept rows == library recompute."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        kept = _kept_rows(checkout)
        result = parity_check(row.spec.oracle, checkout, kept)
        assert result.ok, {
            "generation_id": generation_id,
            "missing_in_kept": result.missing_in_kept,
            "extra_in_kept": result.extra_in_kept,
            "mismatched": result.mismatched,
        }


def _kept_at(dsn: str, generation_id: Any) -> dict[tuple[str, str], tuple[Any, ...]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return _kept_rows(store.checkout_generation(generation_id))


def test_typed_override_assert_and_stale_snapshot_roundtrip(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)
    derived = "claim:typed:derived"
    asserted = "claim:typed:asserted"
    original = _typed_semantics("installation")
    replacement = _typed_semantics("retirement")
    observed = json.dumps({
        "predicate": "preferred_target",
        "normalized_key": f"key|{derived}",
        "value": "blue",
        "scope": "HCAL/Run3",
        "claim_semantics": json.loads(original),
    })
    _apply(clean_runtime, [
        _claim_fact(derived, "blue", claim_semantics=original),
        _correction_fact(
            "corr:typed:override",
            derived,
            "override",
            1,
            claim_semantics=replacement,
            observed_content=observed,
        ),
        _correction_fact(
            "corr:typed:assert",
            asserted,
            "assert",
            2,
            source_id="operator",
            predicate="preferred_target",
            normalized_key="asserted:key",
            value="green",
            scope="HCAL/Run3",
            scope_state="known",
            claim_semantics=original,
        ),
    ])
    published = _cycle(clean_runtime, registry)
    kept = _kept_at(clean_runtime, published["generation_id"])
    derived_row = kept[(ROW_KIND_EFFECTIVE, derived)]
    asserted_row = kept[(ROW_KIND_EFFECTIVE, asserted)]
    assert _col(derived_row, "claim_semantics") == replacement
    assert _col(derived_row, "stale_correction_review") is True
    assert _col(asserted_row, "claim_semantics") == original
    assert _col(asserted_row, "provenance") == "operator_asserted"
    _assert_parity(clean_runtime, row, published["generation_id"])


def test_registered_v1_upgrade_is_idempotent_and_publishes_typed_rows(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)
    v1_claim = "claim:v1:preserved"
    _apply(clean_runtime, [_claim_fact(v1_claim, "legacy")])
    prior = _cycle(clean_runtime, registry)
    prior_generation = prior["generation_id"]
    prior_rows = _kept_at(clean_runtime, prior_generation)
    assert _col(prior_rows[(ROW_KIND_EFFECTIVE, v1_claim)], "claim_semantics") is None

    # Recreate the exact already-registered v1 table shape: both the logical
    # relation and its Chronos physical storage lack the additive v2 column,
    # while the prior generation/checkpoint already exists.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        meta = store.context._backend.tables[CLAIM_EFFECTIVE_TABLE]
        conn.execute("DROP VIEW IF EXISTS okg.v_current_claim_effective")
        conn.execute(
            f"ALTER TABLE {CLAIM_EFFECTIVE_TABLE} "
            "DROP COLUMN IF EXISTS claim_semantics"
        )
        conn.execute(
            f'ALTER TABLE "{meta.physical_name}" '
            "DROP COLUMN IF EXISTS claim_semantics"
        )
        registry_row = conn.execute(
            "SELECT columns, column_defs FROM _chronos_branch_tables "
            "WHERE table_name = %s",
            (CLAIM_EFFECTIVE_TABLE,),
        ).fetchone()
        assert registry_row is not None
        registry_columns = json.loads(registry_row[0])
        registry_defs = json.loads(registry_row[1])
        column_index = registry_columns.index("claim_semantics")
        registry_columns.pop(column_index)
        registry_defs.pop(column_index)
        conn.execute(
            "UPDATE _chronos_branch_tables SET columns = %s, column_defs = %s "
            "WHERE table_name = %s",
            (
                json.dumps(registry_columns),
                json.dumps(registry_defs),
                CLAIM_EFFECTIVE_TABLE,
            ),
        )
        invalidate_registry_cache(
            store.context.db, store.context._backend.name
        )
        store.context._backend.refresh_registries()
        assert "claim_semantics" not in store.context._backend.tables[
            CLAIM_EFFECTIVE_TABLE
        ].columns

    claim = "claim:typed:upgrade"
    semantics = _typed_semantics()
    _apply(clean_runtime, [_claim_fact(claim, "blue", claim_semantics=semantics)])
    first = _cycle(clean_runtime, registry)
    first_rows = _kept_at(clean_runtime, first["generation_id"])
    assert _col(first_rows[(ROW_KIND_EFFECTIVE, claim)], "claim_semantics") == semantics
    preserved_rows = _kept_at(clean_runtime, prior_generation)
    assert (ROW_KIND_EFFECTIVE, v1_claim) in preserved_rows
    assert _col(
        preserved_rows[(ROW_KIND_EFFECTIVE, v1_claim)], "claim_semantics"
    ) is None

    _apply(clean_runtime, [_claim_fact(claim, "blue", rev=2, claim_semantics=semantics)])
    second = _cycle(clean_runtime, registry, expect_noop=True)
    second_rows = _kept_at(clean_runtime, second["generation_id"])
    assert second_rows[(ROW_KIND_EFFECTIVE, claim)] == first_rows[
        (ROW_KIND_EFFECTIVE, claim)
    ]
    _assert_parity(clean_runtime, row, second["generation_id"])


def test_decision_consumption_full_arc(clean_runtime: str, tmp_path: Path) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim_a, claim_b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    # ---- (a) publish claims -> effective rows, provenance=derived --------- #
    _apply(clean_runtime, [_claim_fact(claim_a, "v-a"), _claim_fact(claim_b, "v-b")])
    r1 = _cycle(clean_runtime, registry)
    gen1 = r1["generation_id"]
    kept1 = _kept_at(clean_runtime, gen1)
    assert set(kept1) == {
        (ROW_KIND_EFFECTIVE, claim_a),
        (ROW_KIND_EFFECTIVE, claim_b),
    }, gen1
    assert _col(kept1[(ROW_KIND_EFFECTIVE, claim_a)], "provenance") == "derived"
    assert _col(kept1[(ROW_KIND_EFFECTIVE, claim_a)], "value") == "v-a"
    _assert_parity(clean_runtime, row, gen1)

    # ---- (b) record decisions -> next cycle consumes them ----------------- #
    corr_a, corr_b = f"corr:a:{sfx}", f"corr:b:{sfx}"
    corr_ghost, corr_never = f"corr:ghost:{sfx}", f"corr:never:{sfx}"
    precedent = f"prec:{sfx}"
    _apply(clean_runtime, [
        # Override on A whose snapshot EQUALS A's current derived content —
        # the constructed stale-flag case (operator decision 2026-07-06).
        _correction_fact(
            corr_a, claim_a, "override", 1,
            value="v-a-fixed",
            observed_content=_observed(
                "preferred_target", f"key|{claim_a}", "v-a", "HCAL/Run3"),
        ),
        # Override on B whose snapshot names OLDER content -> no flag.
        _correction_fact(
            corr_b, claim_b, "override", 2,
            value="v-b-fixed",
            observed_content=_observed(
                "preferred_target", f"key|{claim_b}", "v-b-older", "HCAL/Run3"),
        ),
        # Override naming a claim that VANISHED (snapshot present, node absent).
        _correction_fact(
            corr_ghost, f"claim:ghost:{sfx}", "override", 3,
            value="v-x",
            observed_content=_observed(
                "preferred_target", "key|ghost", "v-old", ""),
        ),
        # Retract naming a claim never observed (no snapshot).
        _correction_fact(corr_never, f"claim:never:{sfx}", "retract", 4),
        # A precedent — read + surfaced (≻ consumption is tier-3 work).
        NodeFact(
            precedent,
            "claim_precedent",
            {
                "precedent_id": f"pid:{sfx}",
                "prefer_source": "release_notes",
                "scope": "HCAL/Run3",
                "sequence": 1,
                "decided_by": "operator:riley",
            },
            {"id": precedent},
            {"v": 1},
        ),
    ])
    r2 = _cycle(clean_runtime, registry)
    gen2 = r2["generation_id"]
    assert r2["maintained_derivations"][DERIVATION_ID]["mode"] == "incremental"
    kept2 = _kept_at(clean_runtime, gen2)

    # Row DELTA between the pinned generations: the effective value CHANGED.
    a1, a2 = kept1[(ROW_KIND_EFFECTIVE, claim_a)], kept2[(ROW_KIND_EFFECTIVE, claim_a)]
    assert (gen1, _col(a1, "value"), _col(a1, "provenance")) == (gen1, "v-a", "derived")
    assert (gen2, _col(a2, "value"), _col(a2, "provenance")) == (
        gen2, "v-a-fixed", "corrected")
    assert _col(a2, "applied_correction_id") == corr_a
    # observed == current derived content -> flagged for operator review.
    assert _col(a2, "stale_correction_review") is True

    b2 = kept2[(ROW_KIND_EFFECTIVE, claim_b)]
    assert _col(b2, "value") == "v-b-fixed"
    assert _col(b2, "provenance") == "corrected"
    assert _col(b2, "stale_correction_review") is False

    # Decisions naming absent claims expire VISIBLY, never silently apply.
    assert _col(kept2[(ROW_KIND_EXPIRED, corr_ghost)], "reason") == REASON_VANISHED
    assert _col(
        kept2[(ROW_KIND_EXPIRED, corr_never)], "reason") == REASON_NEVER_EXISTED

    # The precedent is read and surfaced (row presence).
    prec2 = kept2[(ROW_KIND_PRECEDENT, precedent)]
    assert _col(prec2, "value") == "release_notes"
    assert _col(prec2, "scope") == "HCAL/Run3"

    _assert_parity(clean_runtime, row, gen2)

    # ---- (c) re-ingest the unchanged corpus: decision STICKS + flag stays -- #
    _apply(clean_runtime, [
        _claim_fact(claim_a, "v-a", rev=2), _claim_fact(claim_b, "v-b", rev=2),
    ])
    r3 = _cycle(clean_runtime, registry, expect_noop=True)
    gen3 = r3["generation_id"]
    kept3 = _kept_at(clean_runtime, gen3)
    # The decision stayed applied across re-ingest; the target re-derived
    # identically, so the stale flag KEEPS firing (never silently permanent).
    assert kept3 == kept2, (gen2, gen3)
    a3 = kept3[(ROW_KIND_EFFECTIVE, claim_a)]
    assert _col(a3, "value") == "v-a-fixed"
    assert _col(a3, "stale_correction_review") is True
    _assert_parity(clean_runtime, row, gen3)

    # ---- (d) delete the claim's source -> its decision expires visibly ---- #
    _apply(clean_runtime, [
        NodeFact(claim_b, "consistency_claim", {}, {"id": claim_b}, {"v": 3}, op="R"),
    ])
    r4 = _cycle(clean_runtime, registry)
    gen4 = r4["generation_id"]
    kept4 = _kept_at(clean_runtime, gen4)
    # The vanished claim's effective row DISAPPEARS (row delta vs gen3)...
    assert (ROW_KIND_EFFECTIVE, claim_b) in kept3
    assert (ROW_KIND_EFFECTIVE, claim_b) not in kept4, (gen3, gen4)
    # ...and its correction expires visibly (snapshot present -> vanished).
    b_expired = kept4[(ROW_KIND_EXPIRED, corr_b)]
    assert _col(b_expired, "reason") == REASON_VANISHED
    # The untouched claim's decision is unaffected.
    assert kept4[(ROW_KIND_EFFECTIVE, claim_a)] == a3
    _assert_parity(clean_runtime, row, gen4)


def test_superseded_override_snapshot_never_flags_stale_e2e(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Review F1 counterexample, through a real publish: ov1 (seq 1, value,
    observed == derived) superseded by ov2 (seq 2, value, no snapshot). Spec
    §2's flag reads the per-field WINNERS only — the superseded override no
    longer applies, so its matching snapshot must NOT flag the row; kept row
    stale is False and the oracle parity check agrees."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim = f"claim:a:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    _apply(clean_runtime, [
        _claim_fact(claim, "v0"),
        _correction_fact(
            f"corr:1:{sfx}", claim, "override", 1,
            value="x1",
            observed_content=_observed(
                "preferred_target", f"key|{claim}", "v0", "HCAL/Run3"),
        ),
        _correction_fact(f"corr:2:{sfx}", claim, "override", 2, value="x2"),
    ])
    r = _cycle(clean_runtime, registry)
    gen = r["generation_id"]
    kept = _kept_at(clean_runtime, gen)
    eff = kept[(ROW_KIND_EFFECTIVE, claim)]
    assert _col(eff, "value") == "x2"
    assert _col(eff, "applied_correction_id") == f"corr:2:{sfx}"
    assert _col(eff, "stale_correction_review") is False
    _assert_parity(clean_runtime, row, gen)


def test_equal_sequence_node_id_tiebreak_matches_oracle_on_postgres(
    clean_runtime: str,
) -> None:
    """Review F2: equal-(sequence, action) overrides tie-broken on node_id —
    'corr:B...' vs 'corr:a...'. Python compares ordinal ('B' < 'a'); the SQL
    executor pins node_id to COLLATE "C" (byte order) so the postgres path,
    the reference interpreter, and the library oracle pick the SAME winner.
    (The en_US-collation database proof is .scratch/collation_divergence_e2e.py;
    this pins the lowered-SQL path against the oracle on the test database.)"""
    sfx = uuid.uuid4().hex[:8]
    claim_nodes = [{
        "node_id": f"claim:x:{sfx}",
        "attrs": {
            "predicate": "p", "normalized_key": "k", "value": "v0",
            "scope": "S", "scope_state": "known", "source_id": "s",
        },
    }]
    corr_nodes = [
        {"node_id": f"corr:B:{sfx}", "attrs": {
            "correction_id": "cid:B", "target_claim_id": f"claim:x:{sfx}",
            "action": "override", "sequence": 5, "value": "FROM-B"}},
        {"node_id": f"corr:a:{sfx}", "attrs": {
            "correction_id": "cid:a", "target_claim_id": f"claim:x:{sfx}",
            "action": "override", "sequence": 5, "value": "FROM-a"}},
    ]
    facts = [
        NodeFact(n["node_id"], subtype, n["attrs"], {"id": n["node_id"]}, {"v": 1})
        for n, subtype in (
            [(claim_nodes[0], "consistency_claim")]
            + [(c, "claim_correction") for c in corr_nodes]
        )
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)
        pg_rows = rows_as_truth(compute_rows(store.checkout(_DEPLOYMENT)))

    interp_rows = rows_as_truth(
        compute_rows_interpreted(claim_nodes, corr_nodes, ())
    )
    oracle_rows = library_truth_rows(claim_nodes, corr_nodes, ())
    key = (ROW_KIND_EFFECTIVE, f"claim:x:{sfx}")
    # 'corr:a' > 'corr:B' ordinal -> corr:a wins on ALL THREE paths.
    assert _col(oracle_rows[key], "value") == "FROM-a"
    assert _col(oracle_rows[key], "applied_correction_id") == f"corr:a:{sfx}"
    assert pg_rows[key] == oracle_rows[key]
    assert interp_rows[key] == oracle_rows[key]


def test_malformed_typed_correction_names_node_and_nested_field() -> None:
    node_id = "corr:typed:bad"
    with pytest.raises(SubstrateError) as exc_info:
        library_truth_rows(
            [],
            [{
                "node_id": node_id,
                "attrs": {
                    "correction_id": "cid:typed:bad",
                    "target_claim_id": "claim:typed",
                    "action": "assert",
                    "sequence": 1,
                    "claim_semantics": {
                        "schema_version": "okg.claim_model.semantics.v2",
                        "logical_subject": "entity:one",
                        "activity": "installation",
                        "polairty": "negated",
                    },
                },
            }],
            (),
        )
    message = str(exc_info.value)
    assert node_id in message
    assert "polairty" in message


def test_malformed_decision_blocks_the_publish_gating(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Review F4 + F5: the tier-1 row is FailureMode.GATING and the derivation
    validates correction rows itself — a malformed decision (OVERRIDE changing
    no field) RAISES out of the publish cycle, naming the correction node id,
    instead of silently mis-applying. Removing the malformed node recovers."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim, bad_corr = f"claim:a:{sfx}", f"corr:bad:{sfx}"
    row = claim_effective_derivation_row()
    assert row.failure_mode is FailureMode.GATING
    registry = DerivationRegistry()
    registry.register(row)

    _apply(clean_runtime, [
        _claim_fact(claim, "v-a"),
        _correction_fact(bad_corr, claim, "override", 1),  # changes NO field
    ])
    with pytest.raises(SubstrateError, match="OVERRIDE changes no field") as ei:
        _publisher(clean_runtime, registry).cycle()
    assert bad_corr in str(ei.value)

    # Remove the malformed decision -> the next cycle publishes cleanly.
    _apply(clean_runtime, [
        NodeFact(bad_corr, "claim_correction", {}, {"id": bad_corr},
                 {"v": 2}, op="R"),
    ])
    r = _cycle(clean_runtime, registry)
    kept = _kept_at(clean_runtime, r["generation_id"])
    eff = kept[(ROW_KIND_EFFECTIVE, claim)]
    assert _col(eff, "value") == "v-a"
    assert _col(eff, "provenance") == "derived"
    _assert_parity(clean_runtime, row, r["generation_id"])


def test_irrelevant_change_is_a_noop_incremental(
    clean_runtime: str, tmp_path: Path
) -> None:
    """A cycle touching none of claims/corrections/precedents is a no-op
    incremental: zero dirty buckets, prior rows carried forward as-of the new
    generation, oracle parity intact. (predicate_policy is Σ — a tier-2 input,
    deliberately NOT a tier-1 dirty subtype.)"""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim = f"claim:a:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    _apply(clean_runtime, [_claim_fact(claim, "v-a")])
    r1 = _cycle(clean_runtime, registry)
    kept1 = _kept_at(clean_runtime, r1["generation_id"])

    _apply(clean_runtime, [
        NodeFact(
            f"policy:{sfx}",
            "predicate_policy",
            {
                "policy_id": f"pol:{sfx}",
                "policy_kind": "functional",
                "predicate": "preferred_target",
                "enabled": "true",
            },
            {"id": f"policy:{sfx}"},
            {"v": 1},
        ),
    ])
    r2 = _cycle(clean_runtime, registry)
    summary = r2["maintained_derivations"][DERIVATION_ID]
    assert summary["mode"] == "incremental"
    assert summary["dirty_bucket_count"] == 0
    assert summary["rows_written"] == 0

    gen2 = r2["generation_id"]
    kept2 = _kept_at(clean_runtime, gen2)
    assert kept2 == kept1, (r1["generation_id"], gen2)
    _assert_parity(clean_runtime, row, gen2)


# --------------------------------------------------------------------------- #
# Differential maintenance witnesses (PACT consistency-claim-graph-binding,    #
# task.binding.differential; req.binding.differential-maintenance; spec §9.8). #
# The effective tier no longer escalates every touched input to a full         #
# recompute — it recomputes only the dirty (predicate, normalized_key) buckets #
# and carries every untouched bucket forward. Recompute COUNTS are the         #
# witness; oracle parity is the correctness arbiter.                           #
# --------------------------------------------------------------------------- #
def _precedent_fact(node_id: str, sfx: str, prefer: str = "release_notes") -> NodeFact:
    return NodeFact(
        node_id, "claim_precedent",
        {"precedent_id": f"pid:{node_id}", "prefer_source": prefer,
         "scope": "HCAL/Run3", "sequence": 1, "decided_by": "operator:riley"},
        {"id": node_id}, {"v": 1},
    )


def _summary(out: dict[str, Any]) -> dict[str, Any]:
    return out["maintained_derivations"][DERIVATION_ID]


def test_touch_one_bucket_recomputes_one_bucket(
    clean_runtime: str, tmp_path: Path
) -> None:
    """The §9.8-named witness. TWO buckets, TWO claims each; touching ONE claim
    recomputes EXACTLY its bucket's rows (rows_written == that bucket's claim
    count, dirty_bucket_count == 1), the untouched bucket's rows are byte-
    identical carried-forwards (never re-written), and the incremental result
    passes oracle parity against a from-scratch recompute."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    key1, key2 = f"k|one:{sfx}", f"k|two:{sfx}"
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    c, d = f"claim:c:{sfx}", f"claim:d:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    # ---- first publish (full) seeds N=2 buckets, 2 claims each --------------- #
    _apply(clean_runtime, [
        _claim_fact(a, "vA", key=key1), _claim_fact(b, "vB", key=key1),
        _claim_fact(c, "vC", key=key2), _claim_fact(d, "vD", key=key2),
    ])
    r1 = _cycle(clean_runtime, registry)
    gen1 = r1["generation_id"]
    assert _summary(r1)["full_reason"] == FULL_REASON_FIRST_PUBLISH
    kept1 = _kept_at(clean_runtime, gen1)
    bucket1 = {(ROW_KIND_EFFECTIVE, a), (ROW_KIND_EFFECTIVE, b)}
    bucket2 = {(ROW_KIND_EFFECTIVE, c), (ROW_KIND_EFFECTIVE, d)}
    assert set(kept1) == bucket1 | bucket2, gen1
    _assert_parity(clean_runtime, row, gen1)

    # ---- touch ONE claim in bucket1 ----------------------------------------- #
    _apply(clean_runtime, [_claim_fact(a, "vA2", key=key1, rev=2)])
    r2 = _cycle(clean_runtime, registry)
    gen2 = r2["generation_id"]
    summary = _summary(r2)
    # RECOMPUTE COUNTS: exactly bucket1 (2 rows) re-derived; ONE dirty bucket;
    # no deletes; and this is NOT a full recompute (no recorded cause).
    assert summary["mode"] == "incremental"
    assert summary["dirty_bucket_count"] == 1
    assert summary["rows_written"] == len(bucket1)   # == 2, the whole bucket
    assert summary["rows_deleted"] == 0
    assert summary.get("full_reason") is None

    kept2 = _kept_at(clean_runtime, gen2)
    assert set(kept2) == set(kept1), (gen1, gen2)
    # Only claim a's CONTENT changed; b re-derived to identical content...
    changed = {k for k in kept2 if kept2[k] != kept1[k]}
    assert changed == {(ROW_KIND_EFFECTIVE, a)}, changed
    assert _col(kept2[(ROW_KIND_EFFECTIVE, a)], "value") == "vA2"
    # ...and the UNTOUCHED bucket's rows are byte-identical carried-forwards.
    for k in bucket2:
        assert kept2[k] == kept1[k], k
    _assert_parity(clean_runtime, row, gen2)


def test_touch_one_correction_recomputes_one_bucket(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Same shape as §9.8, but the dirty input is a CORRECTION: an override on a
    claim in bucket1 recomputes only bucket1 (dirty_bucket_count == 1), the
    correction applies (provenance corrected), bucket2 is carried forward, and
    oracle parity holds on the incremental result."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    key1, key2 = f"k|one:{sfx}", f"k|two:{sfx}"
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    c, d = f"claim:c:{sfx}", f"claim:d:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    _apply(clean_runtime, [
        _claim_fact(a, "vA", key=key1), _claim_fact(b, "vB", key=key1),
        _claim_fact(c, "vC", key=key2), _claim_fact(d, "vD", key=key2),
    ])
    r1 = _cycle(clean_runtime, registry)
    gen1 = r1["generation_id"]
    kept1 = _kept_at(clean_runtime, gen1)
    bucket2 = {(ROW_KIND_EFFECTIVE, c), (ROW_KIND_EFFECTIVE, d)}

    # Override claim a (in bucket1) -> only bucket1 recomputes.
    corr = f"corr:a:{sfx}"
    _apply(clean_runtime, [
        _correction_fact(corr, a, "override", 1, value="vA-fixed"),
    ])
    r2 = _cycle(clean_runtime, registry)
    gen2 = r2["generation_id"]
    summary = _summary(r2)
    assert summary["mode"] == "incremental"
    assert summary["dirty_bucket_count"] == 1          # bucket1 only
    assert summary["rows_written"] == 2                # a + b (the whole bucket)
    assert summary.get("full_reason") is None

    kept2 = _kept_at(clean_runtime, gen2)
    a2 = kept2[(ROW_KIND_EFFECTIVE, a)]
    assert _col(a2, "value") == "vA-fixed"
    assert _col(a2, "provenance") == "corrected"
    assert _col(a2, "applied_correction_id") == corr
    # bucket2 carried forward byte-identical.
    for k in bucket2:
        assert kept2[k] == kept1[k], k
    _assert_parity(clean_runtime, row, gen2)


def test_touch_one_precedent_recomputes_only_pass_through_rows(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Same shape for a bucket-scoped precedent: in tier 1 a precedent is a
    pass-through row (keyed by precedent node_id), so its bucket reach is
    provably EMPTY — adding one recomputes ZERO effective buckets
    (dirty_bucket_count == 0), surfaces the precedent row, carries every
    effective_claim row forward byte-identical, and is NEVER a full recompute
    (the old any-precedent-forces-full behavior is gone). Oracle parity holds."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    key1, key2 = f"k|one:{sfx}", f"k|two:{sfx}"
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    _apply(clean_runtime, [
        _claim_fact(a, "vA", key=key1), _claim_fact(b, "vB", key=key2),
    ])
    r1 = _cycle(clean_runtime, registry)
    gen1 = r1["generation_id"]
    kept1 = _kept_at(clean_runtime, gen1)
    eff_keys = {(ROW_KIND_EFFECTIVE, a), (ROW_KIND_EFFECTIVE, b)}
    assert set(kept1) == eff_keys, gen1

    prec = f"prec:{sfx}"
    _apply(clean_runtime, [_precedent_fact(prec, sfx)])
    r2 = _cycle(clean_runtime, registry)
    gen2 = r2["generation_id"]
    summary = _summary(r2)
    assert summary["mode"] == "incremental"
    # A precedent governs NO effective bucket in tier 1: zero dirty buckets, but
    # the pass-through precedent row is re-derived (rows_written == 1). NOT full.
    assert summary["dirty_bucket_count"] == 0
    assert summary["rows_written"] == 1
    assert summary.get("full_reason") is None

    kept2 = _kept_at(clean_runtime, gen2)
    assert (ROW_KIND_PRECEDENT, prec) in kept2
    assert _col(kept2[(ROW_KIND_PRECEDENT, prec)], "value") == "release_notes"
    # Every effective_claim row carried forward byte-identical.
    for k in eff_keys:
        assert kept2[k] == kept1[k], k
    _assert_parity(clean_runtime, row, gen2)


def test_full_recompute_paths_record_their_cause(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Every full-recompute path records a cause from the taxonomy, queryable in
    the maintenance summary (summary['full_reason']). Witnessed here: the kit-
    dispatched FIRST publish (first_publish) and the callback DELETION fallback
    (deletion_shape). A full recompute with no recorded cause is a defect."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    # (1) first publish -> kit dispatch full, cause first_publish.
    _apply(clean_runtime, [_claim_fact(a, "vA"), _claim_fact(b, "vB")])
    r1 = _cycle(clean_runtime, registry)
    s1 = _summary(r1)
    assert s1["mode"] == "full_recount"
    assert s1["full_reason"] == FULL_REASON_FIRST_PUBLISH
    assert s1["full_reason"] in FULL_RECOMPUTE_CAUSES

    # (2) delete a claim -> callback deletion-shape fallback (mode stays
    # incremental, cause deletion_shape).
    _apply(clean_runtime, [
        NodeFact(b, "consistency_claim", {}, {"id": b}, {"v": 2}, op="R"),
    ])
    r2 = _cycle(clean_runtime, registry)
    s2 = _summary(r2)
    assert s2["mode"] == "incremental"
    assert s2["full_reason"] == FULL_REASON_DELETION_SHAPE
    assert s2["full_reason"] in FULL_RECOMPUTE_CAUSES
    # The vanished claim's row is gone; the survivor is intact.
    kept2 = _kept_at(clean_runtime, r2["generation_id"])
    assert (ROW_KIND_EFFECTIVE, b) not in kept2
    assert (ROW_KIND_EFFECTIVE, a) in kept2
    _assert_parity(clean_runtime, row, r2["generation_id"])


def test_incremental_matches_from_scratch_on_mixed_sequence(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Oracle parity green over a MIXED incremental sequence (claim edits in
    different buckets, an override, a bucket-moving key override, a precedent,
    and a re-ingest). The incremental (bucket-scoped) result must equal the
    from-scratch library recompute at every pinned generation — the same-result
    contract (spec §9.1) over the differential path (§9.8)."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    key1, key2 = f"k|one:{sfx}", f"k|two:{sfx}"
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    c = f"claim:c:{sfx}"
    row = claim_effective_derivation_row()
    registry = DerivationRegistry()
    registry.register(row)

    def _cycle_and_check(*, expect_noop: bool = False) -> None:
        out = _cycle(clean_runtime, registry, expect_noop=expect_noop)
        _assert_parity(clean_runtime, row, out["generation_id"])

    # seed two buckets
    _apply(clean_runtime, [
        _claim_fact(a, "vA", key=key1), _claim_fact(b, "vB", key=key1),
        _claim_fact(c, "vC", key=key2),
    ])
    _cycle_and_check()
    # edit one bucket
    _apply(clean_runtime, [_claim_fact(a, "vA2", key=key1, rev=2)])
    _cycle_and_check()
    # override in the other bucket
    _apply(clean_runtime, [
        _correction_fact(f"corr:c:{sfx}", c, "override", 1, value="vC-fixed"),
    ])
    _cycle_and_check()
    # bucket-MOVING key override: move a from key1 to key2
    _apply(clean_runtime, [
        _correction_fact(
            f"corr:a-move:{sfx}", a, "override", 2, normalized_key=key2
        ),
    ])
    _cycle_and_check()
    # add a precedent (pass-through)
    _apply(clean_runtime, [_precedent_fact(f"prec:{sfx}", sfx)])
    _cycle_and_check()
    # re-ingest unchanged corpus
    _apply(clean_runtime, [_claim_fact(b, "vB", key=key1, rev=2)])
    _cycle_and_check(expect_noop=True)
