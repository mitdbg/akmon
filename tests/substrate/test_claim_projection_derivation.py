"""End-to-end proof for the tier-2/3 claim-projection maintained derivation.

task.core.projection-derivation (consistency-claim-model-core), the publish
half: on a scratch deployment composing the REAL ["extraction",
"consistency"] library modules, BOTH claim-model derivations register on the
#444 registry — the sorted-``derivation_id`` iteration runs tier 1
(``claim_effective``) before tier 2/3 (``claim_projection``) — and a real
``Publisher.cycle()`` materializes per-pair conflict findings and per-bucket
canonical/contested resolutions with the deciding signal class into
``okg.claim_projection``, generation-pinned. Proven here:

* the full arc: claims + Σ policy publish -> finding rows + a canonical
  resolution with basis provenance (case-mixed claim ids in the bucket — the
  collation regression surface);
* tier-1 -> tier-2 chaining in ONE cycle: an operator correction recorded at
  generation g re-derives C_eff AND re-projects at g+1 — findings involving
  the retracted claim disappear and the bucket re-resolves;
* a precedent flips a contested bucket to canonical with
  basis=operator_precedent;
* bucket-scoped incremental maintenance: dirtying ONE claim updates only its
  bucket's rows (row-delta asserted between pinned generations +
  dirty_bucket_count metadata);
* CallableCorrectnessOracle parity (library recompute over plain-SQL reads)
  at every pinned generation;
* a malformed Σ policy BLOCKS the publish (FailureMode.GATING) and recovery;
* the tier-1 kept-table dependency is mandatory: without the
  ``claim_effective`` row registered, the publish fails LOUD naming the
  absent sentinel — no fallback to raw claim nodes.

Follows tests/substrate/test_claim_effective_derivation.py (the tier-1
template) for the scratch-deployment catalog install + publisher wiring.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

psycopg = pytest.importorskip("psycopg")

from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.consistency.claim_model.precedence import (  # noqa: E402
    SIGNAL_PRECEDENT,
    SIGNAL_SOURCE_AUTHORITY,
)
from okg.substrate.consistency.claim_model.applicability import (  # noqa: E402
    Applicability,
    TimeBounds,
)
from okg.substrate.consistency.claim_model.views import (  # noqa: E402
    MODALITY_CURRENT,
    POLARITY_NEGATED,
    ClaimSemantics,
)
from okg.substrate.derived_artifacts.claim_effective_derivation import (  # noqa: E402
    claim_effective_derivation_row,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    CLAIM_PROJECTION_TABLE,
    DERIVATION_ID,
    ROW_KIND_FINDING,
    ROW_KIND_RESOLUTION,
    VALUE_COLUMNS,
    _effective_binding,
    build_conflict_buckets,
    bucket_row_key,
    claim_projection_derivation_row,
    compute_rows_interpreted,
    finding_row_key,
    library_truth_rows,
    rows_as_truth,
)
from okg.substrate.derived_artifacts.derivation_registry import (  # noqa: E402
    DerivationRegistry,
    FailureMode,
)
from okg.substrate.derived_artifacts.maintained_derivation import (  # noqa: E402
    FULL_REASON_DELETION_SHAPE,
    FULL_REASON_FIRST_PUBLISH,
    FULL_REASON_POLICY_CHANGE,
    FULL_REASON_PRECEDENT_SCOPE_UNPROVABLE,
    FULL_RECOMPUTE_CAUSES,
    parity_check,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (  # noqa: E402
    SENTINEL_TOKEN,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.exchange.harness import (  # noqa: E402
    database_doctor,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"


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
    node_id: str, value: str, *, key: str, source: str = "page:1",
    rev: int = 1, **signals: Any,
) -> NodeFact:
    attrs: dict[str, Any] = {
        "predicate": _PREDICATE,
        "normalized_key": key,
        "value": value,
        "scope": "HCAL/Run3",
        "scope_state": "known",
        "claim_provenance": "derived",
        "source_id": source,
    }
    attrs.update(signals)
    return NodeFact(
        node_id, "consistency_claim", attrs, {"id": node_id}, {"v": rev}
    )


def _policy_fact(node_id: str, *, rev: int = 1, **attrs: Any) -> NodeFact:
    payload = {"enabled": "true"}
    payload.update(attrs)
    return NodeFact(
        node_id, "predicate_policy", payload, {"id": node_id}, {"v": rev}
    )


def _functional_policy(node_id: str) -> NodeFact:
    return _policy_fact(
        node_id, policy_id=_CONSTRAINT, policy_kind="functional",
        predicate=_PREDICATE,
    )


def _typed_semantics(
    *,
    activity: str = "installation",
    modality: str = MODALITY_CURRENT,
    start: str = "2025-01-01",
    end: str = "2026-01-01",
) -> str:
    return ClaimSemantics(
        logical_subject="entity:shared",
        activity=activity,
        modality=modality,
        applicability=Applicability(
            heading_path="HCAL/Run3",
            heading_state="known",
            valid_time=TimeBounds.known(start, end),
        ),
    ).to_attr()


def _effective_typed(
    claim_id: str,
    value: str,
    semantics: str,
    *,
    key: str = "typed:shared",
) -> dict[str, Any]:
    return {
        "claim_id": claim_id,
        "predicate": _PREDICATE,
        "normalized_key": key,
        "value": value,
        "scope": "HCAL/Run3",
        "scope_state": "known",
        "claim_semantics": semantics,
        "source_id": f"page:{claim_id}",
    }


def _policy_node() -> dict[str, Any]:
    return {
        "node_id": "policy:typed",
        "attrs": {
            "enabled": "true",
            "policy_id": _CONSTRAINT,
            "policy_kind": "functional",
            "predicate": _PREDICATE,
        },
    }


def _retract_fact(node_id: str, target: str, sequence: int,
                  observed_value: str, observed_key: str) -> NodeFact:
    return NodeFact(
        node_id,
        "claim_correction",
        {
            "correction_id": f"cid:{node_id}",
            "target_claim_id": target,
            "action": "retract",
            "sequence": sequence,
            "decided_by": "operator:riley",
            "observed_content": json.dumps({
                "predicate": _PREDICATE, "normalized_key": observed_key,
                "value": observed_value, "scope": "HCAL/Run3",
            }),
        },
        {"id": node_id},
        {"v": 1},
    )


def _registry(
    typed_incompatible: Callable[[Any, Any], bool] | None = None,
) -> tuple[DerivationRegistry, Any]:
    registry = DerivationRegistry()
    registry.register(claim_effective_derivation_row())
    row = claim_projection_derivation_row(
        typed_incompatible=typed_incompatible
    )
    registry.register(row)
    return registry, row


def _publisher(dsn: str, registry: DerivationRegistry) -> Publisher:
    return Publisher(
        dsn,
        _DEPLOYMENT,
        consistency_gates=[TransformConsistencyGate()],
        maintained_derivations=registry,
        catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT),
    )


def _cycle(dsn: str, registry: DerivationRegistry) -> dict[str, Any]:
    out = _publisher(dsn, registry).cycle()
    assert out["status"] == "published", out
    for derivation_id in ("claim_effective", DERIVATION_ID):
        summary = out["maintained_derivations"][derivation_id]
        assert summary["status"] == "maintained", (derivation_id, summary)
    return out


def _kept_rows(checkout: Any) -> dict[tuple[str, str], tuple[Any, ...]]:
    columns = ", ".join(("row_kind", "row_key", *VALUE_COLUMNS))
    rows = checkout.query(
        f"SELECT {columns} FROM {CLAIM_PROJECTION_TABLE} "
        f"WHERE NOT (row_kind = :s AND row_key = :s)",
        {"s": SENTINEL_TOKEN},
    )
    return {
        (str(r["row_kind"]), str(r["row_key"])): tuple(
            r[column] for column in VALUE_COLUMNS
        )
        for r in rows
    }


def _kept_at(dsn: str, generation_id: Any) -> dict[tuple[str, str], tuple[Any, ...]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return _kept_rows(store.checkout_generation(generation_id))


def _col(row: tuple, name: str):
    return row[VALUE_COLUMNS.index(name)]


def _assert_parity(dsn: str, row: Any, generation_id: Any) -> None:
    """Oracle parity for the PINNED generation: kept rows == the pure-library
    recompute (derive_conflict_findings + resolve_bucket over plain-SQL reads)."""
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


def test_typed_projection_filters_disjoint_members_from_resolution_component() -> None:
    current = _typed_semantics()
    historical = _typed_semantics(start="2024-01-01", end="2025-01-01")
    effective = [
        _effective_typed("claim:a", "blue", current),
        _effective_typed("claim:b", "green", current),
        _effective_typed("claim:historical", "red", historical),
    ]
    rows = compute_rows_interpreted(effective, [], [_policy_node()])
    resolution = rows[(
        ROW_KIND_RESOLUTION,
        bucket_row_key(_PREDICATE, "typed:shared"),
    )]
    assert json.loads(resolution["ranked_claim_ids"]) == ["claim:b", "claim:a"]
    assert "claim:historical" not in json.loads(resolution["superseded_claim_ids"])
    findings = [
        row for (kind, _key), row in rows.items() if kind == ROW_KIND_FINDING
    ]
    assert len(findings) == 1
    assert json.loads(findings[0]["member_claim_ids"]) == ["claim:a", "claim:b"]


def test_typed_projection_refuses_disconnected_components_pending_745() -> None:
    old = _typed_semantics(start="2024-01-01", end="2025-01-01")
    new = _typed_semantics(start="2025-01-01", end="2026-01-01")
    effective = [
        _effective_typed("claim:a", "blue", old),
        _effective_typed("claim:b", "green", old),
        _effective_typed("claim:c", "red", new),
        _effective_typed("claim:d", "yellow", new),
    ]
    with pytest.raises(SubstrateError, match="disconnected conflict components"):
        compute_rows_interpreted(effective, [], [_policy_node()])


def test_typed_projection_uses_registered_incompatibility_callback() -> None:
    callback_owned = ClaimSemantics(
        logical_subject="entity:shared",
        activity="installation",
        polarity=POLARITY_NEGATED,
        applicability=Applicability(
            heading_path="HCAL/Run3",
            heading_state="known",
            valid_time=TimeBounds.known("2025-01-01", "2026-01-01"),
        ),
    ).to_attr()
    effective = [
        _effective_typed("claim:a", "same", callback_owned),
        _effective_typed("claim:b", "same", _typed_semantics()),
    ]
    with pytest.raises(SubstrateError, match="typed incompatibility callback"):
        compute_rows_interpreted(effective, [], [_policy_node()])

    calls: list[tuple[str, str]] = []

    def incompatible(left, right):
        calls.append((left.node_id, right.node_id))
        return True

    rows = compute_rows_interpreted(
        effective,
        [],
        [_policy_node()],
        typed_incompatible=incompatible,
    )
    truth = library_truth_rows(
        effective,
        [],
        [_policy_node()],
        typed_incompatible=incompatible,
    )
    assert rows_as_truth(rows) == truth
    buckets, _precedents = build_conflict_buckets(
        effective,
        [],
        [_policy_node()],
        typed_incompatible=incompatible,
    )
    assert len(buckets) == 1
    assert calls == [("claim:a", "claim:b")] * 3
    findings = [row for (kind, _), row in rows.items() if kind == ROW_KIND_FINDING]
    assert len(findings) == 1


def test_typed_callback_owns_linked_pair_incompatibility() -> None:
    source_semantics = ClaimSemantics(
        logical_subject="entity:shared",
        activity="status",
        polarity=POLARITY_NEGATED,
        applicability=Applicability(
            heading_path="HCAL/Run3",
            heading_state="known",
            valid_time=TimeBounds.known("2025-01-01", "2026-01-01")
        ),
    ).to_attr()
    target_semantics = ClaimSemantics(
        logical_subject="entity:shared",
        activity="status",
        applicability=Applicability(
            heading_path="HCAL/Run3",
            heading_state="known",
            valid_time=TimeBounds.known("2025-01-01", "2026-01-01")
        ),
    ).to_attr()
    effective = [
        {
            **_effective_typed(
                "claim:source", "target:key", source_semantics, key="source:key"
            ),
            "predicate": "recommends",
        },
        {
            **_effective_typed(
                "claim:target", "bad", target_semantics, key="target:key"
            ),
            "predicate": "deprecated",
        },
    ]
    policies = [{
        "node_id": "policy:linked",
        "attrs": {
            "enabled": "true",
            "policy_id": "never_deprecated",
            "policy_kind": "linked_pair",
            "predicate": "recommends",
            "target_predicate": "deprecated",
            "link_key_fn": "normalize_identity",
            "violates_values": json.dumps(["bad"]),
        },
    }]
    calls: list[tuple[str, str]] = []

    def compatible(left, right):
        calls.append((left.node_id, right.node_id))
        return False

    rows = compute_rows_interpreted(
        effective, [], policies, typed_incompatible=compatible
    )
    truth = library_truth_rows(
        effective, [], policies, typed_incompatible=compatible
    )
    assert rows == {}
    assert truth == {}
    assert calls == [("claim:source", "claim:target")] * 2


def test_registered_typed_callback_publishes_and_matches_oracle(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    calls: list[tuple[str, str]] = []

    def incompatible(left, right):
        calls.append((left.node_id, right.node_id))
        return True

    registry, row = _registry(incompatible)
    callback_owned = ClaimSemantics(
        logical_subject="entity:shared",
        activity="installation",
        polarity=POLARITY_NEGATED,
        applicability=Applicability(
            heading_path="HCAL/Run3",
            heading_state="known",
            valid_time=TimeBounds.known("2025-01-01", "2026-01-01"),
        ),
    ).to_attr()
    _apply(clean_runtime, [
        _functional_policy("policy:typed-callback"),
        _claim_fact(
            "claim:typed-callback:a",
            "same",
            key="typed:callback",
            source="page:a",
            claim_semantics=callback_owned,
        ),
        _claim_fact(
            "claim:typed-callback:b",
            "same",
            key="typed:callback",
            source="page:b",
            claim_semantics=_typed_semantics(),
        ),
    ])
    published = _cycle(clean_runtime, registry)
    kept = _kept_at(clean_runtime, published["generation_id"])
    findings = [key for key in kept if key[0] == ROW_KIND_FINDING]
    assert len(findings) == 1
    resolution = kept[(
        ROW_KIND_RESOLUTION,
        bucket_row_key(_PREDICATE, "typed:callback"),
    )]
    assert json.loads(_col(resolution, "ranked_claim_ids")) == [
        "claim:typed-callback:b",
        "claim:typed-callback:a",
    ]
    _assert_parity(clean_runtime, row, published["generation_id"])
    assert calls == [
        ("claim:typed-callback:a", "claim:typed-callback:b"),
    ] * 2


def test_typed_publish_exercises_active_time_activity_and_conflict_path(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    registry, row = _registry()
    facts = [_functional_policy("policy:typed")]
    scenarios = (
        ("pending", _typed_semantics(modality="pending"), _typed_semantics()),
        (
            "historical",
            _typed_semantics(start="2024-01-01", end="2025-01-01"),
            _typed_semantics(),
        ),
        (
            "activity",
            _typed_semantics(activity="installation"),
            _typed_semantics(activity="retirement"),
        ),
        ("concurrent", _typed_semantics(), _typed_semantics()),
    )
    for index, (name, left_semantics, right_semantics) in enumerate(scenarios):
        key = f"typed:{name}"
        facts.extend([
            _claim_fact(
                f"claim:{name}:a", "blue", key=key,
                claim_semantics=left_semantics,
            ),
            _claim_fact(
                f"claim:{name}:b", "green", key=key,
                claim_semantics=right_semantics,
                rev=index + 1,
            ),
        ])
    _apply(clean_runtime, facts)
    published = _cycle(clean_runtime, registry)
    generation = published["generation_id"]
    kept = _kept_at(clean_runtime, generation)
    finding_rows = [
        row for (kind, _key), row in kept.items() if kind == ROW_KIND_FINDING
    ]
    assert len(finding_rows) == 1
    assert _col(finding_rows[0], "bucket_key") == "typed:concurrent"
    assert _col(finding_rows[0], "outcome") == "definite"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation)
        typed_rows = checkout.query(
            "SELECT row_key, claim_semantics FROM okg.claim_effective "
            "WHERE row_kind = 'effective_claim' AND claim_semantics IS NOT NULL"
        )
        assert len(typed_rows) == 8
        assert all(
            json.loads(r["claim_semantics"])["schema_version"]
            == "okg.claim_model.semantics.v2"
            for r in typed_rows
        )
    _assert_parity(clean_runtime, row, generation)
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.projection_pending")
        projection_queue = int(cur.fetchone()[0])
        cur.execute(
            "SELECT coalesce(sum(dlq_count), 0)::int "
            "FROM okg.v_runtime_queue_health"
        )
        dlq_count = int(cur.fetchone()[0])
    assert projection_queue == 0
    assert dlq_count == 0
    doctor = database_doctor(clean_runtime)
    assert doctor["ok"] is True, doctor
    artifact = Path(".scratch/claimview-v2-publish.json")
    artifact.parent.mkdir(exist_ok=True)
    artifact.write_text(json.dumps({
        "deployment": "isolated-test:default",
        "generation_id": generation,
        "generation_status": published["status"],
        "typed_claim_rows": len(typed_rows),
        "finding_rows": len(finding_rows),
        "finding_bucket": _col(finding_rows[0], "bucket_key"),
        "projection_queue": projection_queue,
        "dlq_count": dlq_count,
        "doctor": doctor,
    }, indent=2, sort_keys=True) + "\n")


def test_effective_binding_sql_coerces_null_scope_state(clean_runtime: str) -> None:
    """Adversarial-review regression (SQL side): the effective-claims binding
    must coerce scope/scope_state so a NULL scope_state does not make the
    downstream not_(eq(scope_state,'known')) evaluate to NULL and SILENTLY DROP
    a real conflict. Structural guard on the exact expressions + a semantic
    check that they equal the oracle's `x or default` on degenerate inputs.
    Tier-1 guarantees non-null today, but the projection must not rely on an
    invariant it does not enforce."""
    binding = _effective_binding("ea")
    assert "COALESCE(scope, '')" in binding
    assert "COALESCE(NULLIF(scope_state, ''), 'unknown')" in binding
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(scope, '') AS s, "
            "COALESCE(NULLIF(scope_state, ''), 'unknown') AS ss "
            "FROM (VALUES (NULL::text, NULL::text), (''::text, ''::text), "
            "('A > B'::text, 'known'::text)) v(scope, scope_state) ORDER BY 1, 2"
        )
        got = cur.fetchall()
    # None->'' , ''->'' , scope kept; None/''->'unknown', 'known' kept — exactly
    # the oracle's `scope or ""` / `scope_state or "unknown"`.
    assert got == [("", "unknown"), ("", "unknown"), ("A > B", "known")]


def test_projection_full_arc_with_one_cycle_chaining(
    clean_runtime: str, tmp_path: Path
) -> None:
    """(a) publish -> findings + canonical resolution with basis provenance;
    (b) a correction recorded as a graph decision re-projects in ONE cycle:
    tier 1 consumes it, tier 2 re-buckets, tier 3 re-resolves."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    # Case-mixed claim ids in ONE bucket (collation regression surface):
    # ordinal order is A1 < C3 < b2.
    claim_a, claim_b, claim_c = (
        f"claim:A1:{sfx}", f"claim:b2:{sfx}", f"claim:C3:{sfx}"
    )
    key = f"key|{sfx}"
    registry, row = _registry()

    # ---- (a) claims + Σ policy -> findings + canonical resolution --------- #
    _apply(clean_runtime, [
        _claim_fact(claim_a, "vX", key=key, source="page:1", source_authority=5),
        _claim_fact(claim_b, "vY", key=key, source="page:2", source_authority=3),
        _claim_fact(claim_c, "vZ", key=key, source="page:3"),
        _functional_policy(f"pol:{sfx}"),
    ])
    r1 = _cycle(clean_runtime, registry)
    gen1 = r1["generation_id"]
    kept1 = _kept_at(clean_runtime, gen1)

    pair_ab = finding_row_key(_CONSTRAINT, [claim_a, claim_b])
    pair_ac = finding_row_key(_CONSTRAINT, [claim_a, claim_c])
    pair_bc = finding_row_key(_CONSTRAINT, [claim_b, claim_c])
    res_key = bucket_row_key(_PREDICATE, key)
    assert set(kept1) == {
        (ROW_KIND_FINDING, pair_ab),
        (ROW_KIND_FINDING, pair_ac),
        (ROW_KIND_FINDING, pair_bc),
        (ROW_KIND_RESOLUTION, res_key),
    }, gen1
    ab = kept1[(ROW_KIND_FINDING, pair_ab)]
    assert _col(ab, "outcome") == "definite"          # known scopes, equal
    assert _col(ab, "locality") == "cross_document"   # distinct source pages
    assert _col(ab, "constraint_id") == _CONSTRAINT
    assert json.loads(_col(ab, "member_claim_ids")) == sorted([claim_a, claim_b])
    res1 = kept1[(ROW_KIND_RESOLUTION, res_key)]
    assert _col(res1, "outcome") == "canonical"
    assert _col(res1, "winner_claim_id") == claim_a
    assert _col(res1, "basis") == SIGNAL_SOURCE_AUTHORITY
    # ≻-descending ranked order across the case-mixed ids (authority 5, 3, 0).
    assert json.loads(_col(res1, "ranked_claim_ids")) == [
        claim_a, claim_b, claim_c,
    ]
    assert json.loads(_col(res1, "superseded_claim_ids")) == [claim_b, claim_c]
    _assert_parity(clean_runtime, row, gen1)

    # ---- (b) operator retracts the winner -> ONE cycle re-projects -------- #
    _apply(clean_runtime, [
        _retract_fact(f"corr:{sfx}", claim_a, 1, "vX", key),
    ])
    r2 = _cycle(clean_runtime, registry)
    gen2 = r2["generation_id"]
    assert r2["maintained_derivations"][DERIVATION_ID]["mode"] == "incremental"
    kept2 = _kept_at(clean_runtime, gen2)
    # Findings involving the retracted claim DISAPPEAR (row delta vs gen1)...
    assert set(kept2) == {
        (ROW_KIND_FINDING, pair_bc),
        (ROW_KIND_RESOLUTION, res_key),
    }, (gen1, gen2)
    # ...and the bucket RE-RESOLVES through tier-1 -> tier-2 in one cycle.
    res2 = kept2[(ROW_KIND_RESOLUTION, res_key)]
    assert _col(res2, "outcome") == "canonical"
    assert _col(res2, "winner_claim_id") == claim_b     # authority 3 beats 0
    assert _col(res2, "basis") == SIGNAL_SOURCE_AUTHORITY
    assert json.loads(_col(res2, "ranked_claim_ids")) == [claim_b, claim_c]
    _assert_parity(clean_runtime, row, gen2)


def test_precedent_flips_contested_bucket_to_canonical(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim_a, claim_b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    key = f"key|{sfx}"
    registry, row = _registry()

    _apply(clean_runtime, [
        _claim_fact(claim_a, "vX", key=key, source_class="release_notes"),
        _claim_fact(claim_b, "vY", key=key, source_class="wiki"),
        _functional_policy(f"pol:{sfx}"),
    ])
    r1 = _cycle(clean_runtime, registry)
    gen1 = r1["generation_id"]
    res_key = bucket_row_key(_PREDICATE, key)
    res1 = _kept_at(clean_runtime, gen1)[(ROW_KIND_RESOLUTION, res_key)]
    # Equal signals: a full tie is CONTESTED, never a guess.
    assert _col(res1, "outcome") == "contested"
    assert _col(res1, "winner_claim_id") == ""
    assert _col(res1, "basis") == "tie"
    _assert_parity(clean_runtime, row, gen1)

    # An operator precedent (a ≻ decision recorded as a graph node) flips it.
    _apply(clean_runtime, [
        NodeFact(
            f"prec:{sfx}",
            "claim_precedent",
            {
                "precedent_id": f"pid:{sfx}",
                "prefer_source": "release_notes",
                "scope": "HCAL/Run3",
                "sequence": 1,
                "decided_by": "operator:riley",
            },
            {"id": f"prec:{sfx}"},
            {"v": 1},
        ),
    ])
    r2 = _cycle(clean_runtime, registry)
    gen2 = r2["generation_id"]
    summary = r2["maintained_derivations"][DERIVATION_ID]
    assert summary["mode"] == "incremental"  # precedent dirty -> full under it
    res2 = _kept_at(clean_runtime, gen2)[(ROW_KIND_RESOLUTION, res_key)]
    assert _col(res2, "outcome") == "canonical", (gen1, gen2)
    assert _col(res2, "winner_claim_id") == claim_a
    assert _col(res2, "basis") == SIGNAL_PRECEDENT
    _assert_parity(clean_runtime, row, gen2)


def test_incremental_updates_only_the_dirty_bucket(
    clean_runtime: str, tmp_path: Path
) -> None:
    """The task's incremental verification: on a functional-only corpus with
    two buckets, dirtying ONE claim recomputes ONLY its bucket — row-delta
    asserted between pinned generations, dirty_bucket_count == 1, the other
    bucket's rows byte-identical (carried forward)."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    key1, key2 = f"key|one:{sfx}", f"key|two:{sfx}"
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    c, d = f"claim:c:{sfx}", f"claim:d:{sfx}"
    registry, row = _registry()

    _apply(clean_runtime, [
        _claim_fact(a, "vX", key=key1), _claim_fact(b, "vY", key=key1),
        _claim_fact(c, "vX", key=key2), _claim_fact(d, "vY", key=key2),
        _functional_policy(f"pol:{sfx}"),
    ])
    r1 = _cycle(clean_runtime, registry)
    gen1 = r1["generation_id"]
    kept1 = _kept_at(clean_runtime, gen1)
    bucket1_keys = {
        (ROW_KIND_FINDING, finding_row_key(_CONSTRAINT, [a, b])),
        (ROW_KIND_RESOLUTION, bucket_row_key(_PREDICATE, key1)),
    }
    bucket2_keys = {
        (ROW_KIND_FINDING, finding_row_key(_CONSTRAINT, [c, d])),
        (ROW_KIND_RESOLUTION, bucket_row_key(_PREDICATE, key2)),
    }
    assert set(kept1) == bucket1_keys | bucket2_keys, gen1
    _assert_parity(clean_runtime, row, gen1)

    # Dirty ONE claim (value change in bucket 1; still conflicting).
    _apply(clean_runtime, [_claim_fact(a, "vX2", key=key1, rev=2)])
    r2 = _cycle(clean_runtime, registry)
    gen2 = r2["generation_id"]
    summary = r2["maintained_derivations"][DERIVATION_ID]
    assert summary["mode"] == "incremental"
    assert summary["dirty_bucket_count"] == 1          # bucket-scoped, not full
    assert summary["rows_written"] == len(bucket1_keys)
    assert summary["rows_deleted"] == 0

    kept2 = _kept_at(clean_runtime, gen2)
    assert set(kept2) == set(kept1), (gen1, gen2)
    # Row delta is confined to the dirty bucket...
    changed = {k for k in kept2 if kept2[k] != kept1[k]}
    assert changed == {(ROW_KIND_FINDING, finding_row_key(_CONSTRAINT, [a, b]))}
    assert "vX2" in _col(
        kept2[(ROW_KIND_FINDING, finding_row_key(_CONSTRAINT, [a, b]))], "detail"
    )
    # ...and the untouched bucket's rows are byte-identical carried-forwards.
    for k in bucket2_keys:
        assert kept2[k] == kept1[k], k
    _assert_parity(clean_runtime, row, gen2)


def test_malformed_policy_blocks_the_publish_gating(
    clean_runtime: str, tmp_path: Path
) -> None:
    """A malformed Σ row (incompatible_pair missing value_b) RAISES out of the
    publish cycle naming the policy node (FailureMode.GATING); removing it
    recovers cleanly."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim, bad_pol = f"claim:a:{sfx}", f"pol:bad:{sfx}"
    registry, row = _registry()
    assert row.failure_mode is FailureMode.GATING

    _apply(clean_runtime, [
        _claim_fact(claim, "vX", key=f"key|{sfx}"),
        _policy_fact(
            bad_pol, policy_id=f"cid:{sfx}", policy_kind="incompatible_pair",
            predicate=_PREDICATE, value_a="current",  # value_b MISSING
        ),
    ])
    with pytest.raises(SubstrateError, match="missing value_a/value_b") as ei:
        _publisher(clean_runtime, registry).cycle()
    assert bad_pol in str(ei.value)

    _apply(clean_runtime, [
        NodeFact(bad_pol, "predicate_policy", {}, {"id": bad_pol},
                 {"v": 2}, op="R"),
    ])
    r = _cycle(clean_runtime, registry)
    kept = _kept_at(clean_runtime, r["generation_id"])
    assert kept == {}  # sole claim, no enabled policy -> no findings
    _assert_parity(clean_runtime, row, r["generation_id"])


def test_tier1_sentinel_required_no_fallback(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Registering the projection WITHOUT the tier-1 row is a loud failure:
    the effective-claims kept table is the only admissible input surface."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    registry = DerivationRegistry()
    registry.register(claim_projection_derivation_row())  # tier 2 ONLY

    _apply(clean_runtime, [
        _claim_fact(f"claim:a:{sfx}", "vX", key=f"key|{sfx}"),
        _functional_policy(f"pol:{sfx}"),
    ])
    with pytest.raises(SubstrateError, match="tier-1 sentinel absent"):
        _publisher(clean_runtime, registry).cycle()


def test_full_recompute_paths_record_their_cause(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Every tier-2/3 full-recompute path records a cause from the taxonomy,
    queryable in the maintenance summary (summary['full_reason']). Witnessed:
    first publish (first_publish), a Σ policy edit (policy_change), a precedent
    edit whose governed bucket set v1 does not prove (precedent_scope_unprovable),
    and a deletion (deletion_shape). A full recompute with no recorded cause is a
    defect (spec §9.8)."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    key = f"key|{sfx}"
    registry, row = _registry()

    def _proj_summary(out: dict[str, Any]) -> dict[str, Any]:
        return out["maintained_derivations"][DERIVATION_ID]

    # (1) first publish -> kit dispatch full, cause first_publish.
    _apply(clean_runtime, [
        _claim_fact(a, "vX", key=key, source_class="release_notes",
                    source_authority="10"),
        _claim_fact(b, "vY", key=key, source_class="wiki", source_authority="1"),
        _functional_policy(f"pol:{sfx}"),
    ])
    s1 = _proj_summary(_cycle(clean_runtime, registry))
    assert s1["mode"] == "full_recount"
    assert s1["full_reason"] == FULL_REASON_FIRST_PUBLISH
    assert s1["full_reason"] in FULL_RECOMPUTE_CAUSES

    # (2) edit the Σ policy -> policy_change (a Σ edit re-classifies every bucket).
    _apply(clean_runtime, [
        _policy_fact(f"pol:{sfx}", rev=2, policy_id=_CONSTRAINT,
                     policy_kind="functional", predicate=_PREDICATE,
                     enabled="true", note="touched"),
    ])
    s2 = _proj_summary(_cycle(clean_runtime, registry))
    assert s2["mode"] == "incremental"
    assert s2["full_reason"] == FULL_REASON_POLICY_CHANGE

    # (3) edit a precedent -> precedent_scope_unprovable (v1 does not prove the
    # governed bucket set; the FULL fallback carries the recorded cause).
    _apply(clean_runtime, [
        NodeFact(
            f"prec:{sfx}", "claim_precedent",
            {"precedent_id": f"pid:{sfx}", "prefer_source": "release_notes",
             "scope": "HCAL/Run3", "sequence": 1, "decided_by": "operator:riley"},
            {"id": f"prec:{sfx}"}, {"v": 1},
        ),
    ])
    s3 = _proj_summary(_cycle(clean_runtime, registry))
    assert s3["mode"] == "incremental"
    assert s3["full_reason"] == FULL_REASON_PRECEDENT_SCOPE_UNPROVABLE

    # (4) delete a claim -> deletion_shape.
    _apply(clean_runtime, [
        NodeFact(b, "consistency_claim", {}, {"id": b}, {"v": 2}, op="R"),
    ])
    r4 = _cycle(clean_runtime, registry)
    s4 = _proj_summary(r4)
    assert s4["mode"] == "incremental"
    assert s4["full_reason"] == FULL_REASON_DELETION_SHAPE
    _assert_parity(clean_runtime, row, r4["generation_id"])
