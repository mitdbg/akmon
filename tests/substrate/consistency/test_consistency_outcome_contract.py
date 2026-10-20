"""Focused typed-outcome witnesses for issue #746."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import yaml

try:
    import psycopg
except ImportError:  # pragma: no cover - exercised only without dev extras
    psycopg = None  # type: ignore[assignment]

from okg.substrate.consistency.claim_model import (
    Applicability,
    ClaimSemantics,
    ClaimView,
    ConflictFinding,
    ConflictRules,
    OUTCOME_ALIGNMENT_UNCERTAIN,
    OUTCOME_APPLICABILITY_UNCERTAIN,
    OUTCOME_CONFLICT,
    OUTCOME_CATEGORIES,
    OUTCOME_DEFINITE,
    OUTCOME_NOT_COMPARABLE,
    OUTCOME_NO_CONFLICT,
    OUTCOME_POSSIBLE,
    ScopeBounds,
    TimeBounds,
    derive_conflict_findings,
    classify_outcome,
    legacy_outcome,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.exchange.harness import database_doctor
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.deployment_stack import build_deployment_publisher_stack
from okg.substrate.derived_artifacts.claim_projection_derivation import ROW_KIND_FINDING
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_DEPLOYMENT = "consistency-fixture"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / _DEPLOYMENT


def _install_catalog(dsn: str, tmp_path: Path) -> None:
    root = tmp_path / "catalog"
    root.mkdir()
    (root / "schemas").mkdir()
    (root / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    composed = compose_catalog(root)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _runtime_claim(node_id: str, value: str, *, key: str, scope: str,
                   scope_state: str, semantics: ClaimSemantics) -> NodeFact:
    attrs = {
        "predicate": "preferred_target",
        "normalized_key": key,
        "value": value,
        "scope": scope,
        "scope_state": scope_state,
        "claim_provenance": "derived",
        "source_id": f"doc:{node_id}",
        "claim_semantics": semantics.to_attr(),
    }
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1})


def _runtime_policy() -> NodeFact:
    return NodeFact(
        "policy:746", "predicate_policy",
        {"enabled": "true", "policy_id": "unique_preferred",
         "policy_kind": "functional", "predicate": "preferred_target"},
        {"id": "policy:746"}, {"v": 1},
    )


@pytest.mark.postgres
def test_published_multigeneration_counts_and_health(clean_runtime: str, tmp_path: Path) -> None:
    """Real two-generation fixture proof for category counts and health."""
    if psycopg is None:
        pytest.skip("psycopg is required for the PostgreSQL publication proof")
    _install_catalog(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts([_runtime_policy()], source=_DEPLOYMENT, strict_endpoints=False)
    stack = build_deployment_publisher_stack(
        dsn=clean_runtime, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    first = Publisher(clean_runtime, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    assert first["status"] == "published", first
    claims = [
        _runtime_claim(
            "claim:746:conflict:a", "blue", key="conflict", scope="Run 3",
            scope_state="known", semantics=ClaimSemantics(
                logical_subject="entity:conflict", activity="install",
                applicability=Applicability.from_v1("Run 3", "known"),
            ),
        ),
        _runtime_claim(
            "claim:746:conflict:b", "green", key="conflict", scope="Run 3",
            scope_state="known", semantics=ClaimSemantics(
                logical_subject="entity:conflict", activity="install",
                applicability=Applicability.from_v1("Run 3", "known"),
            ),
        ),
        _runtime_claim(
            "claim:746:heading:a", "blue", key="heading", scope="Run 3",
            scope_state="known", semantics=ClaimSemantics(
                logical_subject="entity:heading", activity="install",
                applicability=Applicability.from_v1("Run 3", "known"),
            ),
        ),
        _runtime_claim(
            "claim:746:heading:b", "green", key="heading", scope="",
            scope_state="unknown", semantics=ClaimSemantics(
                logical_subject="entity:heading", activity="install",
                applicability=Applicability.from_v1("", "unknown"),
            ),
        ),
        _runtime_claim(
            "claim:746:activity:a", "blue", key="activity", scope="Run 3",
            scope_state="known", semantics=ClaimSemantics(
                logical_subject="entity:activity", activity="install",
                applicability=Applicability.from_v1("Run 3", "known"),
            ),
        ),
        _runtime_claim(
            "claim:746:activity:b", "green", key="activity", scope="Run 3",
            scope_state="known", semantics=ClaimSemantics(
                logical_subject="entity:activity", activity="unknown",
                applicability=Applicability.from_v1("Run 3", "known"),
            ),
        ),
        _runtime_claim(
            "claim:746:key:a", "blue", key="", scope="Run 3",
            scope_state="known", semantics=ClaimSemantics(
                logical_subject="entity:key", activity="install",
                applicability=Applicability.from_v1("Run 3", "known"),
            ),
        ),
        _runtime_claim(
            "claim:746:key:b", "green", key="", scope="Run 3",
            scope_state="known", semantics=ClaimSemantics(
                logical_subject="entity:key", activity="install",
                applicability=Applicability.from_v1("Run 3", "known"),
            ),
        ),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True).apply_facts(
            claims, source=_DEPLOYMENT, strict_endpoints=False
        )
    second = Publisher(clean_runtime, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    assert second["status"] == "published", second
    generation = second["generation_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = store.checkout_generation(generation).query(
            "SELECT typed_category, actionable_conflict, alignment_established, "
            "alignment_unknown, applicability_established, applicability_unknown, "
            "legacy_marker FROM okg.claim_projection WHERE row_kind = :kind",
            {"kind": ROW_KIND_FINDING},
        )
        counts = {}
        for row in rows:
            counts[row["typed_category"]] = counts.get(row["typed_category"], 0) + 1
        counts.setdefault("no_conflict", 0)
        assert counts == {
            "conflict": 1,
            "alignment_uncertain": 1,
            "applicability_uncertain": 1,
            "not_comparable": 1,
            "no_conflict": 0,
        }, counts
        assert sum(1 for row in rows if row["actionable_conflict"]) == 1
        actionable = store.checkout_generation(generation).query(
            "SELECT typed_category, actionable_conflict FROM okg.claim_projection "
            "WHERE row_kind = :kind AND actionable_conflict = true",
            {"kind": ROW_KIND_FINDING},
        )
        assert actionable == [{"typed_category": "conflict", "actionable_conflict": True}]
        by_category = {row["typed_category"]: row for row in rows}
        assert json.loads(by_category["alignment_uncertain"]["alignment_unknown"]) == ["activity"]
        assert json.loads(by_category["applicability_uncertain"]["applicability_unknown"]) == ["heading"]
        assert by_category["alignment_uncertain"]["legacy_marker"] == "legacy_possible"
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.projection_pending")
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT coalesce(sum(active_count), 0), coalesce(sum(pending_count), 0), "
                "coalesce(sum(waiting_count), 0), coalesce(sum(delayed_count), 0), "
                "coalesce(sum(running_count), 0), coalesce(sum(dlq_count), 0) "
                "FROM okg.v_runtime_queue_health"
            )
            assert cur.fetchone() == (0, 0, 0, 0, 0, 0)
    assert database_doctor(clean_runtime)["ok"] is True


RULES = ConflictRules(
    conflict_predicates=frozenset({"exclusive_target"}),
    incompatible=lambda _predicate, left, right: left != right,
    constraint_for=lambda _predicate: "exclusive_target",
)


def _claim(
    node_id: str,
    value: str,
    *,
    subject: str = "entity:one",
    activity: str = "install",
    heading: str = "Run 3",
    heading_state: str = "known",
    start: str = "2025-01-01",
    end: str = "2026-01-01",
    time_state: str = "known",
    key: str = "shared",
) -> ClaimView:
    if time_state == "known":
        valid_time = TimeBounds.known(start, end)
    else:
        valid_time = TimeBounds.unknown()
    semantics = ClaimSemantics(
        logical_subject=subject,
        activity=activity,
        applicability=Applicability(
            heading_path=heading if heading_state == "known" else "",
            heading_state=heading_state,
            valid_time=valid_time,
        ),
    )
    return ClaimView(
        node_id=node_id,
        source_id="doc:one",
        predicate="exclusive_target",
        normalized_key=key,
        value=value,
        scope=heading if heading_state == "known" else "",
        scope_state=heading_state,
        semantics=semantics,
    )


def _one(left: ClaimView, right: ClaimView):
    findings = derive_conflict_findings([left, right], RULES)
    assert len(findings) == 1
    return findings[0]


def test_unknown_heading_is_applicability_uncertain_not_actionable() -> None:
    finding = _one(_claim("a", "blue"), _claim("b", "green", heading_state="unknown"))
    assert finding.typed_category == OUTCOME_APPLICABILITY_UNCERTAIN
    assert finding.actionable_conflict is False
    assert finding.alignment_unknown == ()
    assert finding.applicability_unknown == ("heading",)
    assert finding.outcome == OUTCOME_POSSIBLE


def test_unknown_time_is_applicability_uncertain_and_distinct() -> None:
    finding = _one(_claim("a", "blue"), _claim("b", "green", time_state="unknown"))
    assert finding.typed_category == OUTCOME_APPLICABILITY_UNCERTAIN
    assert finding.applicability_unknown == ("time",)
    assert finding.alignment_unknown == ()
    assert finding.actionable_conflict is False


def test_unknown_scope_conditions_are_recorded() -> None:
    left = _claim("a", "blue")
    semantics = ClaimSemantics(
        logical_subject="entity:one", activity="install",
        applicability=Applicability(
            heading_path="Run 3", heading_state="known",
            valid_time=TimeBounds.known("2025-01-01", "2026-01-01"),
            scope_bounds=ScopeBounds.partial({}, {"region": frozenset({"us", "eu"})}),
        ),
    )
    right = ClaimView(
        node_id="b", source_id="doc:one", predicate="exclusive_target",
        normalized_key="shared", value="green", scope="Run 3", scope_state="known",
        semantics=semantics,
    )
    finding = _one(left, right)
    assert finding.typed_category == OUTCOME_APPLICABILITY_UNCERTAIN
    assert finding.applicability_unknown == ("scope/conditions",)
    assert finding.actionable_conflict is False


def test_unknown_activity_is_alignment_uncertain_not_conflict() -> None:
    finding = _one(_claim("a", "blue"), _claim("b", "green", activity="unknown"))
    assert finding.typed_category == OUTCOME_ALIGNMENT_UNCERTAIN
    assert finding.alignment_unknown == ("activity",)
    assert finding.applicability_unknown == ()
    assert finding.actionable_conflict is False


def test_unresolved_key_is_not_comparable() -> None:
    finding = _one(_claim("a", "blue", key=""), _claim("b", "green", key=""))
    assert finding.typed_category == OUTCOME_NOT_COMPARABLE
    assert finding.actionable_conflict is False
    assert "subject" in finding.alignment_unknown


def test_known_different_activity_is_no_conflict() -> None:
    left = _claim("a", "blue", activity="install")
    right = _claim("b", "green", activity="retire")
    assert derive_conflict_findings([left, right], RULES) == []
    assert legacy_outcome(OUTCOME_NO_CONFLICT) == OUTCOME_POSSIBLE


def test_known_disjoint_incompatible_pair_has_typed_no_conflict() -> None:
    left = _claim("a", "blue", heading="A")
    right = _claim("b", "green", heading="B")
    typed = classify_outcome(
        left,
        right,
        incompatible=True,
        applicability="disjoint",
    )
    assert typed.category == OUTCOME_NO_CONFLICT
    assert typed.actionable_conflict is False


def test_known_overlapping_exclusive_claims_are_actionable_conflict() -> None:
    finding = _one(_claim("a", "blue"), _claim("b", "green"))
    assert finding.typed_category == OUTCOME_CONFLICT
    assert finding.actionable_conflict is True
    assert set(finding.alignment_established) == {"subject", "activity", "value"}
    assert set(finding.applicability_established) == {"heading", "time", "scope/conditions"}
    assert finding.alignment_unknown == ()
    assert finding.applicability_unknown == ()
    assert finding.outcome == OUTCOME_DEFINITE


@pytest.mark.parametrize(
    ("category", "legacy"),
    [
        (OUTCOME_CONFLICT, OUTCOME_DEFINITE),
        (OUTCOME_ALIGNMENT_UNCERTAIN, OUTCOME_POSSIBLE),
        (OUTCOME_APPLICABILITY_UNCERTAIN, OUTCOME_POSSIBLE),
        (OUTCOME_NOT_COMPARABLE, OUTCOME_NOT_COMPARABLE),
        (OUTCOME_NO_CONFLICT, OUTCOME_POSSIBLE),
    ],
)
def test_explicit_legacy_adapter(category: str, legacy: str) -> None:
    assert legacy_outcome(category) == legacy


def test_legacy_adapter_rejects_unknown_and_mismatched_stored_values() -> None:
    with pytest.raises(ValueError, match="unknown legacy outcome"):
        ConflictFinding(
            node_id="f", constraint="c", logical_key="k", member_claim_ids=(),
            outcome="bogus", locality="internal", detail="", schema_version="v1",
        )
    with pytest.raises(ValueError, match="disagrees"):
        ConflictFinding(
            node_id="f", constraint="c", logical_key="k", member_claim_ids=(),
            outcome=OUTCOME_POSSIBLE, locality="internal", detail="", schema_version="v1",
            typed_category=OUTCOME_CONFLICT,
        )


@pytest.mark.parametrize(
    ("field", "dimension"),
    [("subject", "subject"), ("activity", "activity"), ("value", "value")],
)
def test_unknown_semantic_dimension_never_becomes_actionable(
    field: str, dimension: str,
) -> None:
    kwargs = {} if field == "value" else {field: "unknown"}
    right_value = "unknown" if field == "value" else "green"
    unknown = _one(_claim("a", "blue"), _claim("b", right_value, **kwargs))
    assert unknown.typed_category == OUTCOME_ALIGNMENT_UNCERTAIN
    assert unknown.actionable_conflict is False
    assert unknown.alignment_unknown == (dimension,)


def test_state_table_has_exact_five_categories() -> None:
    from okg.substrate.consistency.claim_model import OUTCOME_CATEGORIES

    assert OUTCOME_CATEGORIES == {
        OUTCOME_CONFLICT,
        OUTCOME_ALIGNMENT_UNCERTAIN,
        OUTCOME_APPLICABILITY_UNCERTAIN,
        OUTCOME_NOT_COMPARABLE,
        OUTCOME_NO_CONFLICT,
    }


def test_state_table_refines_all_finite_inputs() -> None:
    semantic_states = ("established", "unknown")
    key_states = ("comparable", "not_comparable")
    applicability_states = ("overlap", "unknown_heading", "unknown_time", "unknown_scope", "disjoint")
    value_states = ("incompatible", "compatible")
    observed = set()
    for semantic in semantic_states:
        for key in key_states:
            for applicability in applicability_states:
                for value in value_states:
                    left = _claim("a", "blue", subject="entity:one")
                    right = _claim("b", "green", subject=("unknown" if semantic == "unknown" else "entity:one"))
                    typed = classify_outcome(
                        left, right, incompatible=value == "incompatible",
                        applicability=("unknown" if applicability.startswith("unknown") else applicability),
                        unresolved_key=key == "not_comparable",
                    )
                    observed.add(typed.category)
    assert observed == OUTCOME_CATEGORIES
