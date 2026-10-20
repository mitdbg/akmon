from __future__ import annotations

import psycopg
import pytest

from okg.substrate.entity_resolution import (
    EntityResolutionStage,
    normalize_entity_name,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.enrichers.extraction.base import (
    ExtractionCandidate,
    LLMExtractor,
    make_dedupe_key,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.projection.remediation import (
    mark_entity_merge_published,
    remediate_entity_merge,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _policy() -> dict:
    return {
        "deployment_name": "cooper-identity-fixture",
        "branch_name": "default",
        "algorithm_version": "unicode-escape-v1",
        "policies": {
            "region": {
                "name_field": "name",
                "id_prefix": "region",
                "identity_fields": ["region_id"],
                "alias_first": True,
                "live_exact": True,
                "post_filter": {"deny_exact": ["other", "selection"]},
            }
        },
    }


def _published_policy(deployment: str) -> dict:
    policy = _policy()
    policy["deployment_name"] = deployment
    return policy


class _PublishedResolutionExtractor(LLMExtractor):
    name = "published_resolution_fixture"
    declares_node_provenance = True
    emits_typed_subtype = "region"
    support_policy: DerivedSupportPolicy = "all_members"
    emits_node_subtypes = ("region", "extraction_dropped")
    requires_narrowings = (("paper", "mentions", "region"),)

    def __init__(self, deployment: str, paper: str):
        self.paper = paper
        super().__init__(
            confidence_floor=0.5,
            entity_resolution=_published_policy(deployment),
        )

    def extract_candidates(self, conn, generation_id):
        del conn, generation_id
        yield ExtractionCandidate(
            "Signal Region", 1.0, self.paper, {"name": "Signal Region"}
        )
        yield ExtractionCandidate(
            "γ Published Region", 1.0, self.paper, {"name": "γ Published Region"}
        )

    def emit_typed_node(
        self,
        conn,
        candidate,
        *,
        generation_id: int,
        run_id: str,
        observed_at,
    ) -> int:
        token = "signal" if candidate.candidate_text == "Signal Region" else "gamma"
        proposed = f"region:proposed-{token}-batch"
        self._emit_node_fact(
            conn,
            node_id=proposed,
            subtype="region",
            attrs={
                "name": candidate.extra["name"],
                "region_id": proposed,
                "extracted": True,
            },
            dedupe_key=make_dedupe_key("published-node", proposed),
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
            attrs={"candidate": candidate.candidate_text},
            provenance="extracted_model",
            confidence=1.0,
            dedupe_key=make_dedupe_key("published-edge", proposed),
            source_record_id={"candidate": candidate.candidate_text},
            source_revision={"generation": generation_id},
            run_id=run_id,
            observed_at=observed_at,
        )
        return 1


#: Two extraction runs over one physics fact -- the ABCD background prediction
#: in the signal region -- named and slotted differently each time, matching the
#: variation measured in issue 1004. `for_process` is unstable prose and is
#: deliberately NOT part of identity; `uses_method` needs a deployment synonym
#: to fold, which is why a declarative hash of the slots cannot do this job.
_ESTIMATE_RUNS = (
    {
        "token": "run1",
        "name": "ABCD-SR prediction",
        "target_region": "Signal Region",
        "uses_method": "ABCD method",
        "for_process": "W and Z channels",
    },
    {
        "token": "run2",
        "name": "Predicted background",
        "target_region": "signal region",
        "uses_method": "ABCD-SR",
        "for_process": "nSUEP",
    },
)


def _estimate_policy(deployment: str, *, escalation: bool) -> dict:
    """A fact-shaped policy: every name-keyed tier is off, because the name is prose."""
    policy: dict = {
        "name_field": "name",
        "id_prefix": "background_estimate",
        "identity_fields": [],
        "alias_first": False,
        "live_exact": False,
    }
    if escalation:
        policy["escalation"] = {
            "class": "tests.pytest.entity_resolution_escalators.SlotKeyedEscalator",
            "config": {
                "slots": ["target_region", "uses_method"],
                "synonyms": {"abcd-sr": "abcd method"},
            },
        }
    return {
        "deployment_name": deployment,
        "branch_name": "default",
        "algorithm_version": "unicode-escape-v1",
        "policies": {"background_estimate": policy},
    }


class _EstimateExtractor(LLMExtractor):
    name = "background_estimate_fixture"
    declares_node_provenance = True
    emits_typed_subtype = "background_estimate"
    support_policy: DerivedSupportPolicy = "all_members"
    emits_node_subtypes = ("background_estimate", "extraction_dropped")
    requires_narrowings = (("paper", "reports", "background_estimate"),)

    def __init__(self, deployment: str, paper: str, run: dict, *, escalation: bool):
        self.paper = paper
        self.run = run
        super().__init__(
            confidence_floor=0.5,
            entity_resolution=_estimate_policy(deployment, escalation=escalation),
        )

    def extract_candidates(self, conn, generation_id):
        del conn, generation_id
        yield ExtractionCandidate(self.run["name"], 1.0, self.paper, self.run)

    def emit_typed_node(
        self,
        conn,
        candidate,
        *,
        generation_id: int,
        run_id: str,
        observed_at,
    ) -> int:
        proposed = f"background_estimate:proposed-{self.run['token']}"
        self._emit_node_fact(
            conn,
            node_id=proposed,
            subtype="background_estimate",
            attrs={
                key: candidate.extra[key]
                for key in ("name", "target_region", "uses_method", "for_process")
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
            attrs={"candidate": candidate.candidate_text},
            provenance="extracted_model",
            confidence=1.0,
            dedupe_key=make_dedupe_key("estimate-edge", proposed),
            source_record_id={"candidate": candidate.candidate_text},
            source_revision={"generation": generation_id},
            run_id=run_id,
            observed_at=observed_at,
        )
        return 1


def _publish_two_estimate_runs(
    clean_runtime: str, deployment: str, *, escalation: bool
) -> tuple[list[str], list[tuple[str, str]]]:
    """Run the same fact through two publisher cycles; return what survived."""
    paper = f"paper:{deployment}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = seed_catalog(
            conn,
            subtypes=["paper", "background_estimate", "extraction_dropped"],
            narrowings=[("paper", "reports", "background_estimate")],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.producer_profiles
                  (deployment_name, producer_id, producer_kind, trust_label,
                   admission_mode, catalog_version_id, profile_fingerprint, status)
                VALUES (%s, 'curated-estimate-fixture', 'source', 'curated',
                        'fast_track', %s, 'fixture:curated-estimate', 'active')
                """,
                (deployment, catalog_version_id),
            )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    paper,
                    "paper",
                    {"title": "Search for soft unclustered energy patterns"},
                    {"id": paper},
                    {"v": 1},
                )
            ],
            branch="default",
            source="curated-estimate-fixture",
        )

    generation_id = None
    for run in _ESTIMATE_RUNS:
        result = Publisher(
            clean_runtime,
            deployment,
            enrichers=[
                _EstimateExtractor(deployment, paper, run, escalation=escalation)
            ],
            catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
        ).cycle()
        assert result["status"] == "published", result
        generation_id = result["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        live = [
            str(row["node_id"])
            for row in graph.query(
                "SELECT node_id FROM okg.graph_nodes "
                "WHERE subtype = 'background_estimate' ORDER BY node_id"
            )
        ]
        with conn.cursor() as cur:
            cur.execute(
                "SELECT normalized_key, canonical_node_id "
                "FROM okg.entity_identity_claims "
                "WHERE deployment_name = %s AND subtype = 'background_estimate' "
                "ORDER BY normalized_key",
                (deployment,),
            )
            claims = [(str(key), str(canonical)) for key, canonical in cur.fetchall()]
    return live, claims


@pytest.mark.parametrize(
    ("escalation", "expected_live_nodes", "expected_canonicals"),
    [
        # The measured defect: one node per extraction run.
        pytest.param(False, 2, 2, id="control-no-escalator"),
        # The fix: both runs resolve onto one canonical node.
        pytest.param(True, 1, 1, id="slot-keyed-escalator"),
    ],
)
def test_slot_keyed_escalation_collapses_re_extraction_to_one_live_node(
    clean_runtime: str,
    escalation: bool,
    expected_live_nodes: int,
    expected_canonicals: int,
) -> None:
    """The measured defect, and its absence.

    Issue 1004 measured one physics fact stored as one node per extraction run.
    Two publisher cycles reproduce that shape; with the escalator able to see
    the proposed attrs and the checkout, the second run resolves onto the first
    run's node instead of minting a second one.

    The two parametrizations are the control pair, and they are parametrized
    rather than run back-to-back inside one test so each arm gets its own
    `clean_runtime`. Both arms publish onto the same Chronos branch, so sharing
    a database between them would let the control arm count the escalated arm's
    node -- and it would read 2 anyway, because `mint_entity_id` takes no
    deployment input and both arms mint the same id for run 1. That is a
    coincidence propping up an assertion, which is not a control.
    """
    deployment = f"estimate-identity-{'escalated' if escalation else 'control'}"
    live, claims = _publish_two_estimate_runs(
        clean_runtime, deployment, escalation=escalation
    )

    assert len(live) == expected_live_nodes, live
    # One claim row per distinct prose name in both arms; what differs is how
    # many distinct canonicals those rows point at.
    assert len(claims) == 2, claims
    assert len({canonical for _key, canonical in claims}) == expected_canonicals
    assert {canonical for _key, canonical in claims} == set(live)
    # No proposed batch id ever survives; ids are minted or matched.
    assert not any("proposed-run" in node_id for node_id in live), live


def test_llm_resolution_seam_publishes_canonical_facts_and_receipts(
    clean_runtime: str,
) -> None:
    deployment = "entity-resolution-llm-publish"
    paper = "paper:resolution-publish"
    curated = "region:signal-published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = seed_catalog(
            conn,
            subtypes=["paper", "region", "extraction_dropped"],
            narrowings=[("paper", "mentions", "region")],
            aliases=[("Signal Region", curated, "region")],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.producer_profiles
                  (deployment_name, producer_id, producer_kind, trust_label,
                   admission_mode, catalog_version_id, profile_fingerprint, status)
                VALUES (%s, 'curated-resolution-fixture', 'source', 'curated',
                        'fast_track', %s, 'fixture:curated-resolution', 'active')
                """,
                (deployment, catalog_version_id),
            )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    curated,
                    "region",
                    {"name": "Canonical Signal Region", "curated": True},
                    {"id": curated},
                    {"v": 1},
                ),
                NodeFact(
                    paper,
                    "paper",
                    {"title": "Resolution publish fixture"},
                    {"id": paper},
                    {"v": 1},
                ),
            ],
            source="curated-resolution-fixture",
        )

    result = Publisher(
        clean_runtime,
        deployment,
        enrichers=[_PublishedResolutionExtractor(deployment, paper)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert result["status"] == "published", result
    assert result["derived_rejections"]["rejected_producer_count"] == 0, result[
        "derived_rejections"
    ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(result["generation_id"])
        regions = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE subtype = 'region' ORDER BY node_id"
        )
        region_ids = {str(row["node_id"]) for row in regions}
        novel = next(
            row for row in regions if row["attrs"].get("name") == "γ Published Region"
        )
        edges = graph.query(
            "SELECT src, dst, edge_type FROM okg.graph_edges "
            "WHERE src = :paper AND edge_type = 'mentions' ORDER BY dst",
            {"paper": paper},
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT a.status, r.state FROM okg.admission_envelopes a "
                "JOIN okg.fact_batch_receipts r USING (admission_id) "
                "WHERE a.deployment_name = %s AND a.producer_id = %s",
                (deployment, "_enricher:published_resolution_fixture"),
            )
            receipt_rows = cur.fetchall()

    assert curated in region_ids
    assert "region:proposed-signal-batch" not in region_ids
    assert "region:proposed-gamma-batch" not in region_ids
    assert novel["attrs"]["region_id"] == novel["node_id"]
    assert "u03b3" in str(novel["node_id"])
    assert {str(row["dst"]) for row in edges} == {curated, str(novel["node_id"])}
    assert receipt_rows
    assert all(status in {"accepted", "published"} for status, _state in receipt_rows)
    assert all(state == "applied" for _status, state in receipt_rows)


def test_published_legacy_rehome_fixture_is_stable_and_repairs_duplicate(
    clean_runtime: str,
) -> None:
    deployment = "cooper-identity-fixture"
    curated_signal = "region:signal"
    curated_extended = "region:extended-abcd"
    historical_duplicate = "region:signal-batch-20260719"
    paper = "paper:cooper-fixture"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv = int(cur.fetchone()[0])
            cur.executemany(
                "INSERT INTO okg.aliases "
                "(alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, %s, %s, 'region', %s)",
                [
                    (alias, normalize_entity_name(alias), canonical, cv)
                    for alias, canonical in (
                        ("Signal Region", curated_signal),
                        ("extended-ABCD", curated_extended),
                    )
                ],
            )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    curated_signal,
                    "region",
                    {"name": "Canonical Signal Region", "curated": True},
                    {"id": curated_signal},
                    {"v": 1},
                ),
                NodeFact(
                    curated_extended,
                    "region",
                    {"name": "extended-ABCD", "curated": True},
                    {"id": curated_extended},
                    {"v": 1},
                ),
                NodeFact(
                    paper,
                    "paper",
                    {"title": "Cooper operational fixture"},
                    {"id": paper},
                    {"v": 1},
                ),
            ],
            branch="default",
            source="curated-fixture",
        )
        initial_generation = store.checkpoint_generation(
            deployment_name=deployment,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture_phase": "curated"},
        )

        stage = EntityResolutionStage(_policy())
        signal = stage.resolve(
            conn,
            proposed_node_id="region:signal-random-batch-a",
            subtype="region",
            attrs={"name": "Signal Region"},
        )
        extended = stage.resolve(
            conn,
            proposed_node_id="region:extended-random-batch-a",
            subtype="region",
            attrs={"name": "extended abcd"},
        )
        novel = stage.resolve(
            conn,
            proposed_node_id="region:gamma-random-batch-a",
            subtype="region",
            attrs={"name": "γ Transition Region"},
        )
        assert signal.canonical_node_id == curated_signal
        assert extended.canonical_node_id == curated_extended
        assert novel.materialize is True
        store.apply_facts(
            [
                NodeFact(
                    str(novel.canonical_node_id),
                    "region",
                    {"name": "γ Transition Region", "extracted": True},
                    {"paper": paper},
                    {"run": 1},
                )
            ],
            branch="default",
            source="extracted-fixture",
        )
        resolved_generation = store.checkpoint_generation(
            deployment_name=deployment,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture_phase": "resolved"},
        )
        replay = stage.resolve(
            conn,
            proposed_node_id="region:gamma-random-batch-b",
            subtype="region",
            attrs={"name": "γ  Transition—Region"},
        )
        assert replay.canonical_node_id == novel.canonical_node_id
        assert replay.materialize is False

        # Historical debt predates the stage. Publish it, then repair it with
        # the explicit pinned merge primitive.
        store.apply_facts(
            [
                NodeFact(
                    historical_duplicate,
                    "region",
                    {"name": "Signal Region", "batch_hash": "20260719"},
                    {"paper": paper},
                    {"run": "legacy"},
                )
            ],
            branch="default",
            source="legacy-extractor",
        )
        store.apply_facts(
            [
                EdgeFact(
                    paper,
                    historical_duplicate,
                    "grounds",
                    attrs={"section": 2},
                    provenance="extracted_model",
                    confidence=0.93,
                    source_record_id={"paper": paper},
                    source_revision={"run": "legacy"},
                )
            ],
            branch="default",
            source="legacy-extractor",
        )
        debt_generation = store.checkpoint_generation(
            deployment_name=deployment,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture_phase": "historical_debt"},
        )
        dry = remediate_entity_merge(
            conn,
            survivor_node_id=curated_signal,
            retired_node_id=historical_duplicate,
            deployment_name=deployment,
            accept_survivor_attributes=True,
            apply=False,
        )
        digest = dry.details["plan"]["plan_digest"]
        applied = remediate_entity_merge(
            conn,
            survivor_node_id=curated_signal,
            retired_node_id=historical_duplicate,
            deployment_name=deployment,
            expected_plan_digest=digest,
            accept_survivor_attributes=True,
            actor="published-fixture",
            apply=True,
        )
        repaired_generation = store.checkpoint_generation(
            deployment_name=deployment,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture_phase": "repaired"},
        )
        mark_entity_merge_published(
            conn,
            merge_id=applied.details["receipt"]["merge_id"],
            repair_generation_id=str(repaired_generation),
        )

        graph = store.checkout_generation(
            repaired_generation,
            branch="default",
        )
        region_rows = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes "
            "WHERE subtype = 'region' ORDER BY node_id"
        )
        ids = {str(row["node_id"]) for row in region_rows}
        assert curated_signal in ids
        assert curated_extended in ids
        assert str(novel.canonical_node_id) in ids
        assert historical_duplicate not in ids
        assert not any("random-batch" in node_id for node_id in ids)
        assert len(ids) == 3
        grounding = graph.query(
            "SELECT src, dst, edge_type, attrs, provenance, confidence "
            "FROM okg.graph_edges WHERE src = :paper AND edge_type = 'grounds'",
            {"paper": paper},
        )
        assert len(grounding) == 1
        assert grounding[0]["dst"] == curated_signal
        assert grounding[0]["attrs"]["section"] == 2
        assert grounding[0]["provenance"] == "extracted_model"
        assert grounding[0]["confidence"] == pytest.approx(0.93)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, repair_generation_id FROM okg.entity_merge_receipts "
                "WHERE retired_node_id = %s",
                (historical_duplicate,),
            )
            assert cur.fetchone() == ("published", str(repaired_generation))
            cur.execute(
                "SELECT count(*) FROM okg.extractor_retry_queue "
                "WHERE deployment_name = %s",
                (deployment,),
            )
            assert int(cur.fetchone()[0]) == 0

        assert (
            len(
                {
                    str(initial_generation),
                    str(resolved_generation),
                    str(debt_generation),
                    str(repaired_generation),
                }
            )
            == 4
        )


def test_published_cooper_shaped_fixture_is_stable_and_repairs_duplicate(
    clean_runtime: str,
) -> None:
    """Issue #1084's declared producer pair survives a real publish repair."""
    from tests.substrate.entity_resolution.publish_fixture_driver import prove

    report = prove(
        clean_runtime,
        deployment="issue-1084-published-fixture",
        branch="default",
    )
    assert report["ok"] is True
    assert report["published_generation_count"] == 4
    assert all(
        generation["status"] == "published"
        for generation in report["generations"]
    )
    assert report["row_counts"] == {
        "before": {"target_nodes_live": 2, "target_edges_live": 2},
        "after": {"target_nodes_live": 1, "target_edges_live": 1},
    }
    merge = report["merge"]
    assert merge["deduplicated_edge_count"] == 1
    assert merge["survivor_edge_unchanged"] is True
    assert merge["receipt_status"] == "published"
    assert merge["receipt_plan_digest"] == merge["plan_digest"]
    assert merge["repair_generation_id"] == report["final_generation_id"]
    assert merge["equivalence_evidence"]["mode"] == (
        "declared_catalog_equivalence"
    )
    assert report["queues"]["extractor_retry_depth"] == 0
    assert report["queues"]["dlq_count"] == 0
