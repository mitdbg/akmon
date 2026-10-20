from __future__ import annotations

import importlib
import inspect
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock

import psycopg
import pytest

from okg.substrate.entity_resolution import (
    EntityResolutionRejected,
    EntityResolutionAmbiguous,
    EntityResolutionStage,
    EntityResolutionUnavailable,
    ResolutionDecision,
    ResolutionStatus,
    mint_entity_id,
    normalize_entity_name,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.enrichers.extraction.base import (
    ExtractionCandidate,
    LLMExtractor,
    make_dedupe_key,
)
from okg.substrate.library.sources.base import NodeFact


pytestmark = pytest.mark.postgres


_ESCALATORS = "tests.pytest.entity_resolution_escalators"


def _slot_config(
    escalation_class: str | None, escalation_config: dict | None = None
) -> dict:
    """Policy for a *fact*-shaped subtype: identity is slots, not the name.

    `alias_first` and `live_exact` are both off because every one of those
    tiers keys on the name, which for this shape is model prose that differs
    on every extraction run. That leaves the escalator as the only tier that
    can decide identity -- the situation issue 1004 reports.
    """
    return {
        "deployment_name": "entity-resolution-test",
        "branch_name": "default",
        "algorithm_version": "unicode-escape-v1",
        "policies": {
            "background_estimate": {
                "name_field": "name",
                "id_prefix": "background_estimate",
                "identity_fields": [],
                "alias_first": False,
                "live_exact": False,
                **(
                    {
                        "escalation": {
                            "class": escalation_class,
                            "config": escalation_config or {},
                        }
                    }
                    if escalation_class
                    else {}
                ),
            }
        },
    }


def _seed_estimate(
    conn: psycopg.Connection, node_id: str, attrs: dict[str, object]
) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="background_estimate",
                attrs=attrs,
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
        ],
        branch="default",
        source="fixture",
    )


def _config(
    *,
    deny_exact: list[str] | None = None,
    escalation_class: str | None = None,
) -> dict:
    return {
        "deployment_name": "entity-resolution-test",
        "branch_name": "default",
        "algorithm_version": "unicode-escape-v1",
        "policies": {
            "concept": {
                "name_field": "name",
                "id_prefix": "concept",
                "identity_fields": ["concept_id"],
                "alias_first": True,
                "live_exact": True,
                "post_filter": {"deny_exact": deny_exact or []},
                **(
                    {"escalation": {"class": escalation_class}}
                    if escalation_class
                    else {}
                ),
            }
        },
    }


def _seed_catalog(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions "
            "(status, applied_at, deployment_name) "
            "VALUES ('applied', now(), 'entity-resolution-test') "
            "RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
    return cv


def _seed_nodes(conn: psycopg.Connection, rows: list[tuple[str, str]]) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="concept",
                attrs={"name": name},
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
            for node_id, name in rows
        ],
        branch="default",
        source="fixture",
    )


def test_identity_fields_must_be_an_explicit_policy_decision() -> None:
    config = _config()
    del config["policies"]["concept"]["identity_fields"]

    with pytest.raises(ValueError, match="requires explicit identity_fields"):
        EntityResolutionStage(config)


def test_exact_curated_alias_precedes_disagreeing_live_name(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _seed_nodes(
            conn,
            [
                ("concept:curated", "Canonical Signal Region"),
                ("concept:shadow", "signal region"),
            ],
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES ('Signal Region', 'signal region', 'concept:curated', "
                "'concept', %s)",
                (cv,),
            )
        decision = EntityResolutionStage(_config()).resolve(
            conn,
            proposed_node_id="concept:batch-hash",
            subtype="concept",
            attrs={"name": "Signal Region"},
        )
    assert decision.status is ResolutionStatus.MATCHED
    assert decision.canonical_node_id == "concept:curated"
    assert decision.method == "curated_alias_exact"
    assert decision.materialize is False


def test_normalized_curated_alias_prevents_case_and_punctuation_shadow(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _seed_nodes(conn, [("concept:curated", "Different canonical display name")])
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES ('Signal–Region', 'signal region', 'concept:curated', "
                "'concept', %s)",
                (cv,),
            )
        decision = EntityResolutionStage(_config()).resolve(
            conn,
            proposed_node_id="concept:must-not-shadow",
            subtype="concept",
            attrs={"name": "signal region"},
        )

    assert decision.status is ResolutionStatus.MATCHED
    assert decision.canonical_node_id == "concept:curated"
    assert decision.method == "curated_alias_exact"
    assert decision.materialize is False


def test_live_normalized_exact_then_atomic_deterministic_mint(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        _seed_nodes(conn, [("concept:extended", "Extended–ABCD")])
        stage = EntityResolutionStage(_config())
        existing = stage.resolve(
            conn,
            proposed_node_id="concept:shadow",
            subtype="concept",
            attrs={"name": "extended abcd"},
        )
        first = stage.resolve(
            conn,
            proposed_node_id="concept:first-random",
            subtype="concept",
            attrs={"name": "γ Signal Region"},
        )
        second = stage.resolve(
            conn,
            proposed_node_id="concept:second-random",
            subtype="concept",
            attrs={"name": "γ  Signal—Region"},
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE subtype = 'concept' AND normalized_key = 'γ signal region'"
            )
            claim_count = int(cur.fetchone()[0])
    assert existing.canonical_node_id == "concept:extended"
    assert existing.method == "live_normalized_exact"
    assert first.status is ResolutionStatus.NOVEL
    assert second.canonical_node_id == first.canonical_node_id
    assert "u03b3" in str(first.canonical_node_id)
    assert claim_count == 1


def test_missing_catalog_is_unavailable_not_novel(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catalog rows are intentionally static across the shared test database;
    # simulate the typed unavailable outcome without mutating that seed state.
    def missing_catalog(self, conn, *, required: bool):
        if required:
            raise EntityResolutionUnavailable(
                "curated alias resolution requires an applied catalog version"
            )
        return None

    monkeypatch.setattr(
        EntityResolutionStage,
        "_active_catalog_version",
        missing_catalog,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(EntityResolutionUnavailable):
            EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id="concept:unsafe",
                subtype="concept",
                attrs={"name": "Signal Region"},
            )
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.entity_identity_claims")
            assert int(cur.fetchone()[0]) == 0


def test_post_filter_rejection_is_audited(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        with pytest.raises(EntityResolutionRejected):
            EntityResolutionStage(_config(deny_exact=["Other"])).resolve(
                conn,
                proposed_node_id="concept:other",
                subtype="concept",
                attrs={"name": "Other"},
                source_node_ref="paper:1",
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT reason, source_node_ref, occurrences "
                "FROM okg.entity_resolution_rejections"
            )
            assert cur.fetchone() == ("deny_exact", "paper:1", 1)


def test_post_filter_regex_timeout_is_unavailable_not_novel(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    config["policies"]["concept"]["post_filter"]["deny_patterns"] = ["a+"]
    monkeypatch.setattr(
        "okg.substrate.entity_resolution.bounded_regex.search",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError()),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        with pytest.raises(EntityResolutionUnavailable, match="execution budget"):
            EntityResolutionStage(config).resolve(
                conn,
                proposed_node_id="concept:must-not-mint",
                subtype="concept",
                attrs={"name": "Regex Timeout Identity"},
            )
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.entity_identity_claims")
            assert int(cur.fetchone()[0]) == 0


def test_post_filter_shapes_and_total_regex_budget_are_bounded(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for invalid in ("", "a+", {}, None):
        malformed = _config()
        malformed["policies"]["concept"]["post_filter"]["deny_patterns"] = invalid
        with pytest.raises(ValueError, match="deny_patterns must be a list"):
            EntityResolutionStage(malformed)

    for invalid in ("", "Other", b"", {}, None):
        malformed = _config()
        malformed["policies"]["concept"]["post_filter"]["deny_exact"] = invalid
        with pytest.raises(ValueError, match="deny_exact must be a list"):
            EntityResolutionStage(malformed)

    config = _config()
    config["policies"]["concept"]["post_filter"]["deny_patterns"] = ["a", "b"]
    monotonic_values = iter([0.0, 0.0, 0.06])
    monkeypatch.setattr(
        "okg.substrate.entity_resolution.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        "okg.substrate.entity_resolution.bounded_regex.search",
        lambda *_args, **_kwargs: None,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        with pytest.raises(EntityResolutionUnavailable, match="total budget"):
            EntityResolutionStage(config).resolve(
                conn,
                proposed_node_id="concept:must-not-mint",
                subtype="concept",
                attrs={"name": "Regex Budget Identity"},
            )


def test_stale_alias_ambiguous_live_match_and_broken_escalation_fail_closed(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES ('Stale Concept', 'stale concept', 'concept:missing', "
                "'concept', %s)",
                (cv,),
            )
        with pytest.raises(EntityResolutionUnavailable, match="missing or has the wrong"):
            EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id="concept:stale",
                subtype="concept",
                attrs={"name": "Stale Concept"},
            )

        _seed_nodes(
            conn,
            [
                ("concept:ambiguous-a", "Ambiguous Concept"),
                ("concept:ambiguous-b", "ambiguous concept"),
            ],
        )
        with pytest.raises(EntityResolutionAmbiguous, match="multiple live nodes"):
            EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id="concept:ambiguous",
                subtype="concept",
                attrs={"name": "AMBIGUOUS CONCEPT"},
            )

        with pytest.raises(EntityResolutionUnavailable, match="could not load escalation"):
            EntityResolutionStage(
                _config(escalation_class="does.not.Exist")
            ).resolve(
                conn,
                proposed_node_id="concept:novel",
                subtype="concept",
                attrs={"name": "Novel Escalated Concept"},
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE normalized_key IN "
                "('stale concept', 'ambiguous concept', 'novel escalated concept')"
            )
            assert int(cur.fetchone()[0]) == 0


class _TypedOutcomeEscalator:
    def __init__(self, status: ResolutionStatus):
        self.status = status

    def resolve(self, *, normalized_key: str, graph_token: str, **_kwargs):
        return ResolutionDecision(
            self.status,
            None,
            "typed-fixture",
            normalized_key,
            None,
            graph_token,
        )


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (ResolutionStatus.REJECTED, EntityResolutionRejected),
        (ResolutionStatus.AMBIGUOUS, EntityResolutionAmbiguous),
        (ResolutionStatus.UNAVAILABLE, EntityResolutionUnavailable),
    ],
)
def test_escalation_typed_failures_remain_distinct(
    clean_runtime: str,
    status: ResolutionStatus,
    error_type: type[Exception],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        stage = EntityResolutionStage(_config(escalation_class="fixture.Typed"))
        stage._escalators["concept"] = _TypedOutcomeEscalator(status)
        with pytest.raises(error_type, match=status.value):
            stage.resolve(
                conn,
                proposed_node_id="concept:typed-outcome",
                subtype="concept",
                attrs={"name": "Typed Escalation Outcome"},
            )


def test_escalator_matches_on_slots_using_the_checkout_it_is_handed(
    clean_runtime: str,
) -> None:
    """The escalator resolves a fact whose name it never looks at.

    Also pins the measurement that justifies passing the checkout: inside the
    escalator, the identical query over the bare connection reads 0 rows
    because `okg.graph_nodes` is a Chronos-managed base table. An escalator
    given only `conn` would silently match nothing and mint a duplicate.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_estimate(
            conn,
            "background_estimate:abcd-sr",
            {
                "name": "ABCD-SR prediction",
                "target_region": "Signal Region",
                "uses_method": "ABCD method",
                "for_process": "W and Z channels",
            },
        )
        stage = EntityResolutionStage(
            _slot_config(f"{_ESCALATORS}.SlotKeyedEscalator")
        )
        decision = stage.resolve(
            conn,
            proposed_node_id="background_estimate:run-2-batch-hash",
            subtype="background_estimate",
            # Different prose name, different unstable slot, same identity.
            attrs={
                "name": "Predicted background",
                "target_region": "signal region",
                "uses_method": "abcd method",
                "for_process": "nSUEP",
            },
        )
        escalator = stage._escalators["background_estimate"]
        with conn.cursor() as cur:
            cur.execute(
                "SELECT canonical_node_id, resolution_method "
                "FROM okg.entity_identity_claims "
                "WHERE subtype = 'background_estimate'"
            )
            claims = cur.fetchall()

    assert decision.status is ResolutionStatus.MATCHED
    assert decision.canonical_node_id == "background_estimate:abcd-sr"
    assert decision.materialize is False
    assert escalator.seen_attrs[0]["for_process"] == "nSUEP"
    assert escalator.raw_connection_row_counts == [0]
    assert escalator.checkout_row_counts == [1]
    assert claims == [("background_estimate:abcd-sr", "escalation")]


@pytest.mark.parametrize("materialize", [False, True])
def test_escalation_match_refreshes_the_canonical_only_when_asked(
    clean_runtime: str, materialize: bool
) -> None:
    """A matched fact either keeps or refreshes the canonical's attributes.

    Default False is the established behaviour: the canonical's attrs freeze
    at the extraction that created it. For fact-shaped subtypes the attrs are
    the content -- a corrected number in extraction 2 would be dropped with no
    record -- so the deployment can ask for the refresh instead.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_estimate(
            conn,
            "background_estimate:refresh-probe",
            {
                "name": "ABCD-SR prediction",
                "target_region": "Signal Region",
                "uses_method": "ABCD method",
                "value": 11.0,
            },
        )
        decision = EntityResolutionStage(
            _slot_config(
                f"{_ESCALATORS}.SlotKeyedEscalator", {"materialize": materialize}
            )
        ).resolve(
            conn,
            proposed_node_id="background_estimate:corrected-batch",
            subtype="background_estimate",
            attrs={
                "name": "Corrected ABCD prediction",
                "target_region": "Signal Region",
                "uses_method": "ABCD method",
                "value": 12.4,
            },
        )

    assert decision.status is ResolutionStatus.MATCHED
    assert decision.canonical_node_id == "background_estimate:refresh-probe"
    assert decision.materialize is materialize


def test_refresh_rewrites_identity_attrs_onto_the_canonical(
    clean_runtime: str,
) -> None:
    """`canonicalize_identity_attrs` under an escalation match, in isolation.

    Before this change a matched identity always returned early, so this
    function was unreachable on the escalation path and a non-empty
    `identity_fields` was never exercised against a match. Covers the rewrite
    and the disagreement refusal directly; the emission this newly enables is
    a separate test
    (`test_refresh_emits_the_later_extraction_against_the_canonical`), which
    drives the real emitter rather than calling this by hand.
    """
    config = _slot_config(
        f"{_ESCALATORS}.SlotKeyedEscalator", {"materialize": True}
    )
    config["policies"]["background_estimate"]["identity_fields"] = ["estimate_id"]
    canonical = "background_estimate:refresh-emitted"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_estimate(
            conn,
            canonical,
            {
                "name": "ABCD-SR prediction",
                "target_region": "Signal Region",
                "uses_method": "ABCD method",
                "estimate_id": canonical,
                "value": 11.0,
            },
        )
        stage = EntityResolutionStage(config)
        proposed = "background_estimate:corrected-batch"
        decision = stage.resolve(
            conn,
            proposed_node_id=proposed,
            subtype="background_estimate",
            attrs={
                "name": "Corrected ABCD prediction",
                "target_region": "Signal Region",
                "uses_method": "ABCD method",
                "estimate_id": proposed,
                "value": 12.4,
            },
        )
        assert decision.materialize is True
        rewritten = stage.canonicalize_identity_attrs(
            subtype="background_estimate",
            attrs={"estimate_id": proposed, "value": 12.4},
            proposed_node_id=proposed,
            canonical_node_id=str(decision.canonical_node_id),
        )
        # An identity attr naming neither id is the disagreement this path
        # can now hit, and it must refuse rather than silently rewrite.
        with pytest.raises(EntityResolutionUnavailable, match="disagrees"):
            stage.canonicalize_identity_attrs(
                subtype="background_estimate",
                attrs={"estimate_id": "background_estimate:someone-else"},
                proposed_node_id=proposed,
                canonical_node_id=str(decision.canonical_node_id),
            )

    assert decision.canonical_node_id == canonical
    assert rewritten["estimate_id"] == canonical
    assert rewritten["value"] == 12.4


def test_escalator_cannot_mutate_the_attrs_the_extractor_emits(
    clean_runtime: str,
) -> None:
    proposed_attrs: dict[str, object] = {
        "name": "Original name",
        "tags": ["original"],
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stage = EntityResolutionStage(
            _slot_config(f"{_ESCALATORS}.MutatingEscalator")
        )
        decision = stage.resolve(
            conn,
            proposed_node_id="background_estimate:mutation-probe",
            subtype="background_estimate",
            attrs=proposed_attrs,
        )
        escalator = stage._escalators["background_estimate"]

    assert decision.status is ResolutionStatus.NOVEL
    # The read-only view refuses the rebind outright.
    assert escalator.top_level_error == "TypeError"
    # The nested append is not refused -- it lands on the deep copy.
    assert escalator.nested_error is None
    # Neither write reached the caller's attrs.
    assert proposed_attrs == {"name": "Original name", "tags": ["original"]}


def test_escalator_signature_without_the_new_parameters_is_refused_at_load(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(
            EntityResolutionUnavailable, match="cannot accept attrs, graph"
        ) as raised:
            EntityResolutionStage(
                _slot_config(f"{_ESCALATORS}.LegacySignatureEscalator")
            ).resolve(
                conn,
                proposed_node_id="background_estimate:legacy",
                subtype="background_estimate",
                attrs={"name": "Legacy Escalator Identity"},
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE subtype = 'background_estimate'"
            )
            claim_count = int(cur.fetchone()[0])

    assert raised.value.data["missing_parameters"] == ["attrs", "graph"]
    assert "LegacySignatureEscalator" in raised.value.data["escalation_class"]
    assert raised.value.fix_hint
    assert claim_count == 0


def test_escalator_that_cannot_be_introspected_still_fails_closed(
    clean_runtime: str,
) -> None:
    """The load-time check is belt; the call-site guard is the braces.

    `_require_escalator_parameters` proceeds when a signature cannot be read.
    This drives exactly that branch with a plugin that is genuinely
    unintrospectable AND genuinely incompatible, and asserts the independent
    call-site guard still converts the resulting TypeError into a typed
    unavailable outcome that claims nothing.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The load check really is bypassed rather than doing the refusing.
        with pytest.raises(ValueError):
            inspect.signature(
                importlib.import_module(_ESCALATORS).UnintrospectableEscalator().resolve
            )
        with pytest.raises(
            EntityResolutionUnavailable, match="escalation failed"
        ) as raised:
            EntityResolutionStage(
                _slot_config(f"{_ESCALATORS}.UnintrospectableEscalator")
            ).resolve(
                conn,
                proposed_node_id="background_estimate:unintrospectable",
                subtype="background_estimate",
                attrs={"name": "Unintrospectable Escalator Identity"},
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE subtype = 'background_estimate'"
            )
            claim_count = int(cur.fetchone()[0])

    assert isinstance(raised.value.__cause__, TypeError)
    assert raised.value.data["subtype"] == "background_estimate"
    assert "UnintrospectableEscalator" in raised.value.data["escalation_class"]
    assert claim_count == 0


def test_escalator_raising_internally_names_the_plugin_and_mints_nothing(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(
            EntityResolutionUnavailable, match="escalation failed"
        ) as raised:
            EntityResolutionStage(
                _slot_config(f"{_ESCALATORS}.RaisingEscalator")
            ).resolve(
                conn,
                proposed_node_id="background_estimate:raising",
                subtype="background_estimate",
                attrs={"name": "Raising Escalator Identity"},
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE subtype = 'background_estimate'"
            )
            claim_count = int(cur.fetchone()[0])

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "RaisingEscalator" in raised.value.data["escalation_class"]
    assert claim_count == 0


def test_deployment_catalog_precedes_newer_global_catalog(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_nodes(conn, [("concept:deployment-curated", "Deployment Curated")])
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions "
                "(status, applied_at, deployment_name) "
                "VALUES ('applied', now(), 'entity-resolution-test') "
                "RETURNING catalog_version_id"
            )
            deployment_cv = int(cur.fetchone()[0])
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES ('Deployment Alias', 'deployment alias', "
                "'concept:deployment-curated', 'concept', %s)",
                (deployment_cv,),
            )
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now())"
            )
        decision = EntityResolutionStage(_config()).resolve(
            conn,
            proposed_node_id="concept:must-not-mint",
            subtype="concept",
            attrs={"name": "Deployment Alias"},
        )
    assert decision.catalog_version_id == deployment_cv
    assert decision.canonical_node_id == "concept:deployment-curated"
    assert decision.method == "curated_alias_exact"


def test_claim_level_mint_collision_is_rejected(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.entity_resolution.mint_entity_id",
        lambda _prefix, _name: "concept:forced-mint-collision",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        stage = EntityResolutionStage(_config())
        first = stage.resolve(
            conn,
            proposed_node_id="concept:first",
            subtype="concept",
            attrs={"name": "First Novel Identity"},
        )
        with pytest.raises(EntityResolutionAmbiguous, match="another claimed identity"):
            stage.resolve(
                conn,
                proposed_node_id="concept:second",
                subtype="concept",
                attrs={"name": "Second Novel Identity"},
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT normalized_key FROM okg.entity_identity_claims "
                "WHERE canonical_node_id = 'concept:forced-mint-collision'"
            )
            claimed_keys = [str(row[0]) for row in cur.fetchall()]
    assert first.canonical_node_id == "concept:forced-mint-collision"
    assert claimed_keys == ["first novel identity"]


def test_minted_id_collision_with_wrong_live_subtype_is_rejected(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    forced_id = "concept:forced-cross-subtype"
    monkeypatch.setattr(
        "okg.substrate.entity_resolution.mint_entity_id",
        lambda _prefix, _name: forced_id,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id=forced_id,
                    subtype="paper",
                    attrs={"name": "Cross Subtype Identity"},
                    source_record_id={"id": forced_id},
                    source_revision={"v": 1},
                )
            ],
            branch="default",
            source="fixture",
        )
        with pytest.raises(EntityResolutionAmbiguous, match="different live identity"):
            EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id="concept:proposed",
                subtype="concept",
                attrs={"name": "Cross Subtype Identity"},
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE canonical_node_id = %s",
                (forced_id,),
            )
            assert int(cur.fetchone()[0]) == 0


def test_concurrent_compare_and_mint_creates_one_claim(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)

    def resolve_once(proposed: str) -> str:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            decision = EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id=proposed,
                subtype="concept",
                attrs={"name": "Concurrent γ Region"},
            )
            return str(decision.canonical_node_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        canonicals = list(pool.map(resolve_once, ["concept:a", "concept:b"]))

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), min(canonical_node_id) "
            "FROM okg.entity_identity_claims "
            "WHERE subtype = 'concept' AND normalized_key = 'concurrent γ region'"
        )
        claim_count, claimed = cur.fetchone()
    assert canonicals[0] == canonicals[1] == claimed
    assert int(claim_count) == 1


def test_concurrent_first_claims_with_different_policies_fail_closed(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        # Use a shared live match so the primary identity-claim conflict, not
        # the separate deterministic-mint unique fence, arbitrates the race.
        _seed_nodes(conn, [("concept:policy-race", "Policy Race Identity")])

    barrier = Barrier(2)
    original_claim = EntityResolutionStage._claim

    def synchronized_claim(self, conn, **kwargs):
        barrier.wait(timeout=10)
        return original_claim(self, conn, **kwargs)

    monkeypatch.setattr(EntityResolutionStage, "_claim", synchronized_claim)
    configs = [_config(), _config(deny_exact=["Unrelated Denied Identity"])]

    def resolve_once(config: dict) -> str:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            try:
                decision = EntityResolutionStage(config).resolve(
                    conn,
                    proposed_node_id="concept:proposed",
                    subtype="concept",
                    attrs={"name": "Policy Race Identity"},
                )
                return decision.status.value
            except EntityResolutionUnavailable as exc:
                assert "different policy fingerprint" in str(exc)
                return "unavailable"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(resolve_once, configs))

    assert sorted(outcomes) == ["matched", "unavailable"]


def test_concurrent_mint_fence_converges_on_one_claim(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two first writers for one identity converge on the mint fence too.

    Deliberately no live match: with nothing to match, both threads reach the
    deterministic mint, compute the same canonical, and the loser can trip the
    partial unique mint index instead of the identity-claim primary key. Which
    fence arbitrates is a sub-millisecond ordering -- whether the loser's ON
    CONFLICT arbiter pre-check sees the winner's still-uncommitted row -- so
    the barrier alone does not pin it. Rounds repeat until the extra claim
    re-read witnesses that the mint fence really arbitrated one of them; every
    round asserts convergence, so a resolver that fails closed there reds on
    the FIRST round that reaches the fence rather than intermittently in CI.

    Exhausting the round budget is a skip, not a failure. A loaded host can
    starve the losing thread past the pre-check window for every round, which
    says nothing about the resolver; asserting there would red correct code
    under CPU contention. Failing closed at the fence still reds this test the
    moment a round reaches the fence, and that is the property under test.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)

    original_claim = EntityResolutionStage._claim
    original_existing_claim = EntityResolutionStage._existing_claim
    read_lock = Lock()
    claim_reads = 0
    barrier = Barrier(2)

    def counting_existing_claim(self, conn, subtype, normalized_key):
        nonlocal claim_reads
        with read_lock:
            claim_reads += 1
        return original_existing_claim(self, conn, subtype, normalized_key)

    def synchronized_claim(self, conn, **kwargs):
        barrier.wait(timeout=10)
        return original_claim(self, conn, **kwargs)

    monkeypatch.setattr(
        EntityResolutionStage, "_existing_claim", counting_existing_claim
    )
    monkeypatch.setattr(EntityResolutionStage, "_claim", synchronized_claim)

    def resolve_once(name: str, proposed: str) -> str:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            decision = EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id=proposed,
                subtype="concept",
                attrs={"name": name},
            )
            return str(decision.canonical_node_id)

    fence_arbitrated = False
    rounds = 40
    for round_index in range(rounds):
        name = f"Mint Fence Identity {round_index}"
        normalized_key = normalize_entity_name(name)
        barrier = Barrier(2)
        reads_before = claim_reads

        with ThreadPoolExecutor(max_workers=2) as pool:
            canonicals = list(
                pool.map(
                    lambda proposed: resolve_once(name, proposed),
                    ["concept:a", "concept:b"],
                )
            )

        with psycopg.connect(clean_runtime, autocommit=True) as conn, (
            conn.cursor()
        ) as cur:
            cur.execute(
                "SELECT count(*), min(canonical_node_id) "
                "FROM okg.entity_identity_claims "
                "WHERE subtype = 'concept' AND normalized_key = %s",
                (normalized_key,),
            )
            claim_count, claimed = cur.fetchone()
        assert canonicals[0] == canonicals[1] == claimed
        assert int(claim_count) == 1

        # One read per resolve is the pre-mint lookup. A third read in a round
        # is the mint fence's recovery re-read, and only the fence path takes
        # it.
        if claim_reads - reads_before > 2:
            fence_arbitrated = True
            break

    if not fence_arbitrated:
        pytest.skip(
            f"the deterministic-mint fence never arbitrated within {rounds} "
            "rounds; convergence not witnessed on this host"
        )


def test_mint_fence_fails_closed_for_a_different_normalized_key(
    clean_runtime: str,
) -> None:
    """The fence keeps its real job: one canonical, two normalized identities.

    Convergence on the fence is only correct when the claim being re-read is
    this key's own. A canonical already minted under a DIFFERENT normalized
    key leaves nothing to converge on and must still fail closed.
    """
    canonical = mint_entity_id("concept", "Fence Guard Identity")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.entity_identity_claims "
                "(deployment_name, branch_name, subtype, normalized_key, "
                "canonical_node_id, raw_name, resolution_method, "
                "policy_fingerprint, catalog_version_id, graph_token) "
                "VALUES ('entity-resolution-test', 'default', 'concept', "
                "'a different claimed identity', %s, "
                "'A Different Claimed Identity', 'mint', "
                "'fingerprint-fixture', NULL, 'token-fixture')",
                (canonical,),
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(EntityResolutionAmbiguous) as excinfo:
            EntityResolutionStage(_config()).resolve(
                conn,
                proposed_node_id="concept:proposed",
                subtype="concept",
                attrs={"name": "Fence Guard Identity"},
            )
    assert "deterministic mint collided" in str(excinfo.value)


def test_mint_fence_converges_inside_an_outer_transaction(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fence is a SAVEPOINT in production, not a top-level transaction.

    `_claim` wraps its insert in `conn.transaction()`. The other concurrency
    tests here resolve on autocommit connections, where that is a real BEGIN
    and a rollback costs nothing. The publisher calls the resolver from inside
    its own already-open transaction (`library/enrichers/extraction/base.py`
    `emit_node`), where the same statement is a SAVEPOINT, and a rollback
    there must not cost the publisher its work. So this drives the same race
    with an outer transaction open and asserts what autocommit cannot: the
    nested transaction really is a savepoint (the connection is already
    INTRANS when it opens), the loser converges, the outer transaction is
    still usable rather than aborted afterwards, and the uncommitted row
    written before the resolve survives the savepoint rollback and commits.

    Budget exhaustion is a skip for the same reason as the autocommit race:
    a loaded host can miss the fence window every round, which is not a
    verdict on the resolver.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)

    original_claim = EntityResolutionStage._claim
    original_existing_claim = EntityResolutionStage._existing_claim
    read_lock = Lock()
    claim_reads = 0
    barrier = Barrier(2)

    def counting_existing_claim(self, conn, subtype, normalized_key):
        nonlocal claim_reads
        with read_lock:
            claim_reads += 1
        return original_existing_claim(self, conn, subtype, normalized_key)

    def synchronized_claim(self, conn, **kwargs):
        barrier.wait(timeout=30)
        return original_claim(self, conn, **kwargs)

    monkeypatch.setattr(
        EntityResolutionStage, "_existing_claim", counting_existing_claim
    )
    monkeypatch.setattr(EntityResolutionStage, "_claim", synchronized_claim)

    def resolve_in_transaction(
        name: str, proposed: str, sentinel_key: str
    ) -> tuple[str, str, str, int]:
        # autocommit is off: the INSERT below opens the transaction that the
        # publisher would already own, so `conn.transaction()` inside `_claim`
        # is a savepoint rather than a BEGIN.
        with psycopg.connect(clean_runtime) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO okg.entity_identity_claims "
                    "(deployment_name, branch_name, subtype, normalized_key, "
                    "canonical_node_id, raw_name, resolution_method, "
                    "policy_fingerprint, catalog_version_id, graph_token) "
                    "VALUES ('entity-resolution-test', 'default', 'concept', "
                    "%s, %s, 'Sentinel', 'mint', 'fingerprint-fixture', NULL, "
                    "'token-fixture')",
                    (sentinel_key, mint_entity_id("concept", sentinel_key)),
                )
            status_before = conn.info.transaction_status.name
            with conn.transaction():
                decision = EntityResolutionStage(_config()).resolve(
                    conn,
                    proposed_node_id=proposed,
                    subtype="concept",
                    attrs={"name": name},
                )
            status_after = conn.info.transaction_status.name
            # Reading at all proves the outer transaction is usable and not
            # INERROR; the count proves the pre-resolve write is still there.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM okg.entity_identity_claims "
                    "WHERE normalized_key = %s",
                    (sentinel_key,),
                )
                sentinel_rows = int(cur.fetchone()[0])
        return (
            str(decision.canonical_node_id),
            status_before,
            status_after,
            sentinel_rows,
        )

    fence_arbitrated = False
    rounds = 40
    for round_index in range(rounds):
        name = f"Outer Txn Mint Fence Identity {round_index}"
        normalized_key = normalize_entity_name(name)
        sentinels = [
            f"outer txn sentinel {round_index} a",
            f"outer txn sentinel {round_index} b",
        ]
        barrier = Barrier(2)
        reads_before = claim_reads

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda args: resolve_in_transaction(name, *args),
                    list(zip(["concept:a", "concept:b"], sentinels)),
                )
            )

        assert [row[1] for row in results] == ["INTRANS", "INTRANS"]
        assert [row[2] for row in results] == ["INTRANS", "INTRANS"]
        assert [row[3] for row in results] == [1, 1]

        with psycopg.connect(clean_runtime, autocommit=True) as conn, (
            conn.cursor()
        ) as cur:
            cur.execute(
                "SELECT count(*), min(canonical_node_id) "
                "FROM okg.entity_identity_claims "
                "WHERE subtype = 'concept' AND normalized_key = %s",
                (normalized_key,),
            )
            claim_count, claimed = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM okg.entity_identity_claims "
                "WHERE normalized_key = ANY(%s)",
                (sentinels,),
            )
            sentinels_committed = int(cur.fetchone()[0])
        assert results[0][0] == results[1][0] == claimed
        assert int(claim_count) == 1
        # The outer transactions committed for real, savepoint rollback and
        # all: both pre-resolve rows are durable.
        assert sentinels_committed == 2

        if claim_reads - reads_before > 2:
            fence_arbitrated = True
            break

    if not fence_arbitrated:
        pytest.skip(
            f"the deterministic-mint fence never arbitrated within {rounds} "
            "rounds; convergence not witnessed on this host"
        )


class _ResolvedExtractor(LLMExtractor):
    name = "resolved_fixture"
    emits_typed_subtype = "concept"
    support_policy: DerivedSupportPolicy = "all_members"
    emits_node_subtypes = ("concept", "extraction_dropped")
    requires_narrowings = (("paper", "mentions", "concept"),)

    def extract_candidates(self, conn, generation_id):
        yield ExtractionCandidate(
            "Signal Region", 1.0, "paper:1", {"name": "Signal Region"}
        )

    def emit_typed_node(
        self,
        conn,
        candidate,
        *,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> int:
        proposed = "concept:batch-hash"
        self._emit_node_fact(
            conn,
            node_id=proposed,
            subtype="concept",
            attrs={"name": candidate.extra["name"]},
            dedupe_key=make_dedupe_key("node", proposed),
            source_record_id={"candidate": candidate.candidate_text},
            source_revision={"generation": generation_id},
            run_id=run_id,
            observed_at=observed_at,
            derived_from=[candidate.source_node_ref],
        )
        self._emit_edge_fact(
            conn,
            src=candidate.source_node_ref,
            dst=proposed,
            edge_type="mentions",
            attrs={},
            provenance="extracted_model",
            confidence=1.0,
            dedupe_key=make_dedupe_key("edge", proposed),
            source_record_id={},
            source_revision={},
            run_id=run_id,
            observed_at=observed_at,
        )
        return 1


def test_llm_fact_emission_remaps_edges_without_minting_alias_shadow(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _seed_nodes(conn, [("concept:curated", "Canonical Signal Region")])
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES ('Signal Region', 'signal region', 'concept:curated', "
                "'concept', %s)",
                (cv,),
            )
        result = _ResolvedExtractor(
            confidence_floor=0.5,
            entity_resolution=_config(),
        ).enrich(conn, generation_id=1)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE node_id = 'concept:batch-hash'"
            )
            minted = int(cur.fetchone()[0])
            cur.execute("SELECT dst FROM okg.edge_facts WHERE edge_type = 'mentions'")
            dst = str(cur.fetchone()[0])
    assert result.incomplete is False
    assert minted == 0
    assert dst == "concept:curated"


class _RefreshingEstimateExtractor(LLMExtractor):
    """Re-extracts one fact under a new name with a corrected value."""

    name = "refreshing_estimate_fixture"
    emits_typed_subtype = "background_estimate"
    support_policy: DerivedSupportPolicy = "all_members"
    emits_node_subtypes = ("background_estimate", "extraction_dropped")
    requires_narrowings = (("paper", "reports", "background_estimate"),)

    def extract_candidates(self, conn, generation_id):
        del conn, generation_id
        yield ExtractionCandidate(
            "Corrected ABCD prediction", 1.0, "paper:1", {"value": 12.4}
        )

    def emit_typed_node(
        self,
        conn,
        candidate,
        *,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> int:
        proposed = "background_estimate:proposed-batch"
        self._emit_node_fact(
            conn,
            node_id=proposed,
            subtype="background_estimate",
            attrs={
                "name": candidate.candidate_text,
                "target_region": "Signal Region",
                "uses_method": "ABCD-SR",
                "estimate_id": proposed,
                "value": candidate.extra["value"],
            },
            dedupe_key=make_dedupe_key("estimate-node", proposed),
            source_record_id={"candidate": candidate.candidate_text},
            source_revision={"generation": generation_id},
            run_id=run_id,
            observed_at=observed_at,
            derived_from=[candidate.source_node_ref],
        )
        self._emit_edge_fact(
            conn,
            src=candidate.source_node_ref,
            dst=proposed,
            edge_type="reports",
            attrs={},
            provenance="extracted_model",
            confidence=1.0,
            dedupe_key=make_dedupe_key("estimate-edge", proposed),
            source_record_id={},
            source_revision={},
            run_id=run_id,
            observed_at=observed_at,
        )
        return 1


def test_refresh_emits_the_later_extraction_against_the_canonical(
    clean_runtime: str,
) -> None:
    """The refresh path end to end, through the real emitter.

    A matched identity used to return before emitting anything, so
    `_emit_node_fact`'s post-resolution branch -- canonicalize the identity
    attrs, then emit against the canonical id -- was unreachable on this path.
    Drive it: the emitted node fact must carry the CANONICAL id, the LATER
    extraction's corrected value, and an `estimate_id` rewritten from the
    proposed id to the canonical. The proposed id must not appear at all.
    """
    canonical = "background_estimate:abcd-sr"
    config = _slot_config(
        f"{_ESCALATORS}.SlotKeyedEscalator",
        {"synonyms": {"abcd-sr": "abcd method"}, "materialize": True},
    )
    config["policies"]["background_estimate"]["identity_fields"] = ["estimate_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        _seed_estimate(
            conn,
            canonical,
            {
                "name": "ABCD-SR prediction",
                "target_region": "Signal Region",
                "uses_method": "ABCD method",
                "estimate_id": canonical,
                "value": 11.0,
            },
        )
        result = _RefreshingEstimateExtractor(
            confidence_floor=0.5, entity_resolution=config
        ).enrich(conn, generation_id=1)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT node_id, attrs FROM okg.node_facts "
                "WHERE subtype = 'background_estimate'"
            )
            emitted = cur.fetchall()
            cur.execute("SELECT dst FROM okg.edge_facts WHERE edge_type = 'reports'")
            dst = str(cur.fetchone()[0])

    assert result.incomplete is False
    assert len(emitted) == 1, emitted
    node_id, attrs = emitted[0]
    assert str(node_id) == canonical
    # The later extraction's content actually landed, rather than being dropped.
    assert attrs["value"] == 12.4
    # The identity attr was rewritten off the proposed id onto the canonical.
    assert attrs["estimate_id"] == canonical
    assert dst == canonical
