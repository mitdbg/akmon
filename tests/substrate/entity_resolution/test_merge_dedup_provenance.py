"""Issue #1042 end to end: a duplicate co-referenced by one chunk merges.

The shape here is the one measured on a live deployment — two enricher runs over
the same chunk each assert `chunk --references--> entity`, one onto the curated
node and one onto a minted duplicate. Before this change the pair compared
unequal on `run_id` alone and the merge refused.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from threading import Barrier

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.projection.remediation import (
    plan_entity_merge,
    remediate_entity_merge,
)


pytestmark = pytest.mark.postgres

DEPLOYMENT = "entity-merge-dedup-test"
SURVIVOR = "concept:w-jets"
RETIRED = "concept:w-jets-minted"
CHUNK = "paper:chunk-03f02937"
SOURCE = "extracted-source"
RECORD_ID = {"chunk": CHUNK, "span": 12}
SURVIVOR_SOURCE = "_enricher:cooper_mention_grounding"
RETIREE_SOURCE = "_enricher:cooper_physics_extraction"
EDGE_EQUIVALENCE_POLICY = {
    "schema_version": "okg.entity_merge.edge_equivalence.v1",
    "survivor": {
        "producer": SURVIVOR_SOURCE,
        "provenance": "derived_deterministic",
    },
    "retiree": {
        "producer": RETIREE_SOURCE,
        "provenance": "explicit",
    },
    "audit_attrs": ["evidence_basis", "provenance_kind", "src_kind"],
    "confidence_rule": "survivor_gte",
}


def _seed(
    conn: psycopg.Connection,
    *,
    survivor_record_id: dict | None = None,
    retired_record_id: dict | None = None,
    survivor_confidence: float = 0.91,
    retired_confidence: float = 0.91,
) -> tuple[int, str]:
    """Two nodes and, on each, the same assertion written by a different run."""
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions "
            "(status, applied_at, deployment_name) "
            "VALUES ('applied', now(), %s) RETURNING catalog_version_id",
            (DEPLOYMENT,),
        )
        cv = int(cur.fetchone()[0])
    store = ChronosGraphStore.from_connection(conn)
    store.context.ensure_metadata()
    store.ensure_schema()
    store.ensure_branch("default", from_branch="main")
    store.apply_facts(
        [
            NodeFact(
                SURVIVOR, "concept", {"name": "W+jets"}, {"id": SURVIVOR}, {"v": 1}
            ),
            NodeFact(RETIRED, "concept", {"name": "W+jets"}, {"id": RETIRED}, {"v": 1}),
        ],
        branch="default",
        source="concept-source",
    )
    store.apply_facts(
        [NodeFact(CHUNK, "paper", {"title": "chunk"}, {"id": CHUNK}, {"v": 1})],
        branch="default",
        source="paper-source",
    )

    def _reference(
        dst: str,
        run_id: str,
        *,
        record_id: dict | None = None,
        confidence: float = 0.91,
    ) -> EdgeFact:
        return EdgeFact(
            CHUNK,
            dst,
            "references",
            attrs={"grounding": "section-2"},
            provenance="extracted_model",
            confidence=confidence,
            source_record_id=dict(record_id or RECORD_ID),
            # The enricher stamps its own run id here, exactly as issue #1042
            # observed. It lands in the edge's source_revision column and is
            # copied into the lineage envelope's source_revisions list, so it
            # used to differ in two places at once.
            source_revision={"run_id": run_id, "generation_id": 0},
        )

    # Two runs over the same chunk. The run id is the ONLY thing distinguishing
    # these two edges; everything they assert is identical, unless the caller
    # asks for a different source record on the retiree's edge.
    store.apply_facts(
        [
            _reference(
                SURVIVOR,
                "run-a",
                record_id=survivor_record_id,
                confidence=survivor_confidence,
            )
        ],
        branch="default",
        source=SOURCE,
    )
    store.apply_facts(
        [
            _reference(
                RETIRED,
                "run-b",
                record_id=retired_record_id,
                confidence=retired_confidence,
            )
        ],
        branch="default",
        source=SOURCE,
    )

    checkpoint = store.checkpoint_generation(
        branch="default",
        deployment_name=DEPLOYMENT,
        catalog_version_id=cv,
        status="published",
        metadata={"fixture": "entity-merge-dedup"},
    )
    return cv, str(checkpoint)


def _seed_cross_producer(
    conn: psycopg.Connection,
    *,
    policy: dict | None = EDGE_EQUIVALENCE_POLICY,
    variation: str | None = None,
) -> tuple[int, str]:
    """Redacted persisted Cooper pair from issue #1084.

    The writing producers differ, while the Nomos envelope identifies the same
    upstream record and policy. Producer-owned diagnostic attrs differ exactly
    in the fields the deployment rule declares audit-only.
    """
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store import _edge_id
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.nomos.source_lineage import (
        LINEAGE_ATTR,
        RECORD_DETAIL_KEY,
        RECORD_DETAIL_TRUNCATED,
        stamp_attrs_with_source_lineage,
    )

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions "
            "(status, applied_at, deployment_name) "
            "VALUES ('applied', now(), %s) RETURNING catalog_version_id",
            (DEPLOYMENT,),
        )
        cv = int(cur.fetchone()[0])
        if policy is not None:
            cur.execute(
                "INSERT INTO okg.narrowings "
                "(parent_subtype, edge_type, child_subtype, attr_rules, "
                "catalog_version_id) VALUES ('paper', 'references', 'concept', "
                "%s::jsonb, %s)",
                (json.dumps({"entity_merge_equivalence": policy}), cv),
            )
    store = ChronosGraphStore.from_connection(conn)
    store.context.ensure_metadata()
    store.ensure_schema()
    store.ensure_branch("default", from_branch="main")
    store.apply_facts(
        [
            NodeFact(
                SURVIVOR,
                "concept",
                {"name": "W+jets"},
                {"id": SURVIVOR},
                {"v": 1},
            ),
            NodeFact(
                RETIRED,
                "concept",
                {"name": "W+jets"},
                {"id": RETIRED},
                {"v": 1},
            ),
            NodeFact(
                CHUNK,
                "paper",
                {"title": "chunk"},
                {"id": CHUNK},
                {"v": 1},
            ),
        ],
        branch="default",
        source="cooper_paper_fixture",
    )
    existing_record_id = RECORD_ID
    retired_record_id = RECORD_ID
    if variation == "candidate_alias":
        existing_record_id = {
            "chunk_node_id": CHUNK,
            "enricher": "candidate-extractor",
            "node_id": SURVIVOR,
            "span": 12,
        }
        retired_record_id = {
            "chunk_node_id": CHUNK,
            "enricher": "candidate-extractor",
            "node_id": RETIRED,
            "span": 12,
        }
    elif variation == "scalar_record":
        existing_record_id = "candidate-record:shared"
        retired_record_id = "candidate-record:shared"
    shared_lineage = stamp_attrs_with_source_lineage(
        {},
        source_id="cooper_paper_fixture",
        source_record_id=existing_record_id,
        source_revision={"content_hash": "redacted-shared-record"},
        policy_fingerprint="sha256:redacted-cooper-policy",
    )[LINEAGE_ATTR]
    retired_lineage = deepcopy(shared_lineage)
    if variation == "candidate_alias":
        retired_lineage = stamp_attrs_with_source_lineage(
            {},
            source_id="cooper_paper_fixture",
            source_record_id=retired_record_id,
            source_revision={"content_hash": "redacted-shared-record"},
            policy_fingerprint="sha256:redacted-cooper-policy",
        )[LINEAGE_ATTR]
    elif variation == "lineage":
        retired_lineage = stamp_attrs_with_source_lineage(
            {},
            source_id="cooper_paper_fixture",
            source_record_id={"chunk": CHUNK, "span": 99},
            source_revision={"content_hash": "redacted-shared-record"},
            policy_fingerprint="sha256:redacted-cooper-policy",
        )[LINEAGE_ATTR]
    elif variation == "malformed_lineage":
        retired_lineage = {"status": "not-a-lineage"}
    elif variation == "truncated_lineage":
        retired_lineage[RECORD_DETAIL_KEY] = RECORD_DETAIL_TRUNCATED
    elif variation in {"absent_lineage", "both_absent_lineage"}:
        retired_lineage = None

    existing_attrs = {
        "grounding": "section-2",
        "provenance_kind": "name_mention",
        "evidence_basis": "verbatim_chunk_mention",
        LINEAGE_ATTR: shared_lineage,
    }
    if variation == "both_absent_lineage":
        existing_attrs.pop(LINEAGE_ATTR)
    if variation == "governance_attr":
        existing_attrs["classification"] = "public"
    retired_attrs = {
        "grounding": ("different-section" if variation == "semantic" else "section-2"),
        "provenance_kind": "model_extraction",
        "src_kind": "document_chunk",
    }
    if retired_lineage is not None:
        retired_attrs[LINEAGE_ATTR] = retired_lineage
    if variation == "governance_attr":
        retired_attrs["classification"] = "restricted"
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    if variation == "source_record":
        retired_record_id = {"chunk": CHUNK, "span": 99}
    session = store.checkout("default")
    with session.transaction():
        session.upsert_rows(
            "okg.graph_edges",
            [
                {
                    "edge_id": _edge_id(CHUNK, "references", SURVIVOR),
                    "src": CHUNK,
                    "dst": SURVIVOR,
                    "edge_type": "references",
                    "attrs": existing_attrs,
                    "provenance": "derived_deterministic",
                    "confidence": 0.95,
                    "source": SURVIVOR_SOURCE,
                    "source_record_id": existing_record_id,
                    "source_revision": {"run_id": "mention-grounding-7"},
                    "run_id": "mention-grounding-7",
                    "observed_at": observed_at,
                },
                {
                    "edge_id": _edge_id(CHUNK, "references", RETIRED),
                    "src": CHUNK,
                    "dst": RETIRED,
                    "edge_type": "references",
                    "attrs": retired_attrs,
                    "provenance": "explicit",
                    "confidence": 0.99 if variation == "confidence" else 0.90,
                    "source": RETIREE_SOURCE,
                    "source_record_id": retired_record_id,
                    "source_revision": {"run_id": "physics-extraction-4"},
                    "run_id": "physics-extraction-4",
                    "observed_at": observed_at,
                },
            ],
        )
    if variation == "read_trust":
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.producer_profiles "
                "(deployment_name, producer_id, producer_kind, trust_label, "
                "admission_mode, catalog_version_id, profile_fingerprint, status) "
                "VALUES (%s, %s, 'enricher', 'untrusted', 'review', %s, "
                "'fixture:retiree-untrusted', 'active')",
                (DEPLOYMENT, RETIREE_SOURCE, cv),
            )
    checkpoint = store.checkpoint_generation(
        branch="default",
        deployment_name=DEPLOYMENT,
        catalog_version_id=cv,
        status="published",
        metadata={"fixture": "issue-1084-redacted-cooper-pair"},
    )
    return cv, str(checkpoint)


def _edges(conn: psycopg.Connection) -> list[dict]:
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default"
    )
    return [
        dict(row)
        for row in graph.query(
            "SELECT src, dst, edge_type, source_revision FROM okg.graph_edges "
            "ORDER BY src, edge_type, dst"
        )
    ]


def _live_node_ids(conn: psycopg.Connection) -> set[str]:
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default"
    )
    return {
        str(row["node_id"])
        for row in graph.query("SELECT node_id FROM okg.graph_nodes")
    }


def _survivor_edge(conn: psycopg.Connection) -> dict:
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default"
    )
    rows = graph.query(
        "SELECT edge_id, src, dst, edge_type, attrs, provenance, confidence, "
        "source, source_record_id, source_revision, run_id, observed_at "
        "FROM okg.graph_edges WHERE src = :src AND dst = :dst "
        "AND edge_type = 'references'",
        {"src": CHUNK, "dst": SURVIVOR},
    )
    assert len(rows) == 1
    return dict(rows[0])


def test_cross_run_duplicate_deduplicates_instead_of_blocking(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.blocking_conflicts == ()
        assert len(plan.deduplicated_edges) == 1
        entry = plan.deduplicated_edges[0]
        assert str(entry["old"]["dst"]) == RETIRED
        assert str(entry["existing"]["dst"]) == SURVIVOR


def test_candidate_record_variant_deduplicates_and_receipt_keeps_raw_assertions(
    clean_runtime: str,
) -> None:
    survivor_record = {
        "chunk_node_id": CHUNK,
        "enricher": "cooper_physics_extraction",
        "node_id": SURVIVOR,
        "span": 12,
    }
    retired_record = {**survivor_record, "node_id": RETIRED}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            survivor_record_id=survivor_record,
            retired_record_id=retired_record,
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.blocking_conflicts == ()
        assert len(plan.deduplicated_edges) == 1
        entry = plan.deduplicated_edges[0]
        assert entry["equivalence_evidence"]["candidate_record_alias"] == {
            "schema_version": "okg.entity_merge.candidate_record_alias.v1",
            "required_fields": ["chunk_node_id", "enricher", "node_id"],
            "candidate_field": "node_id",
            "survivor_node_id": SURVIVOR,
            "retired_node_id": RETIRED,
            "normalized_node_id": SURVIVOR,
            "nomos_enforcement_mode": "off",
        }
        differences = entry["assertion_variance"]["differences"]
        assert set(differences) >= {
            "attrs",
            "source_record_id",
            "source_revision",
        }
        assert differences["source_record_id"] == {
            "existing": survivor_record,
            "retired": retired_record,
        }
        existing_lineage = differences["attrs"]["existing"][LINEAGE_ATTR]
        retired_lineage = differences["attrs"]["retired"][LINEAGE_ATTR]
        assert existing_lineage["sources"][0]["source_record_ids"] == [survivor_record]
        assert retired_lineage["sources"][0]["source_record_ids"] == [retired_record]
        provenance_union = entry["provenance_union"]
        assert provenance_union["schema_version"] == (
            "okg.entity_merge.provenance_union.v1"
        )
        assert {
            json.dumps(value, sort_keys=True)
            for value in provenance_union["source_record_ids"]
        } == {
            json.dumps(survivor_record, sort_keys=True),
            json.dumps(retired_record, sort_keys=True),
        }
        assert {
            json.dumps(value, sort_keys=True)
            for value in provenance_union["lineage_envelopes"]
        } == {
            json.dumps(existing_lineage, sort_keys=True),
            json.dumps(retired_lineage, sort_keys=True),
        }
        assert provenance_union["source_revisions"] == [
            {"generation_id": 0, "run_id": "run-a"},
            {"generation_id": 0, "run_id": "run-b"},
        ]
        assert provenance_union["sources"] == [SOURCE]
        assert provenance_union["provenance_classes"] == ["extracted_model"]

        result = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        assert result.applied
        live = _survivor_edge(conn)
        assert live["source_record_id"] == survivor_record
        assert live["attrs"][LINEAGE_ATTR] == entry["existing"]["attrs"][LINEAGE_ATTR]

        with conn.cursor() as cur:
            cur.execute(
                "SELECT plan FROM okg.entity_merge_receipts WHERE retired_node_id = %s",
                (RETIRED,),
            )
            stored = cur.fetchone()[0]
        if isinstance(stored, str):
            stored = json.loads(stored)
        stored_entry = stored["deduplicated_edges"][0]
        assert stored_entry["existing"]["source_record_id"] == survivor_record
        assert stored_entry["old"]["source_record_id"] == retired_record
        assert stored_entry["assertion_variance"] == entry["assertion_variance"]
        assert stored_entry["provenance_union"] == provenance_union
        stored_differences = stored_entry["assertion_variance"]["differences"]
        assert stored_differences["attrs"]["existing"][LINEAGE_ATTR] == (
            existing_lineage
        )
        assert stored_differences["attrs"]["retired"][LINEAGE_ATTR] == (retired_lineage)


def test_candidate_record_confidence_divergence_still_blocks(
    clean_runtime: str,
) -> None:
    survivor_record = {
        "chunk_node_id": CHUNK,
        "enricher": "cooper_physics_extraction",
        "node_id": SURVIVOR,
        "span": 12,
    }
    retired_record = {**survivor_record, "node_id": RETIRED}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            survivor_record_id=survivor_record,
            retired_record_id=retired_record,
            retired_confidence=0.99,
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert plan.blocking_conflicts[0]["reason"] == "identity_differs"


def test_candidate_record_variant_refuses_under_nomos_enforcement_without_proof(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    survivor_record = {
        "chunk_node_id": CHUNK,
        "enricher": "cooper_physics_extraction",
        "node_id": SURVIVOR,
        "span": 12,
    }
    retired_record = {**survivor_record, "node_id": RETIRED}
    monkeypatch.setattr(
        "okg.substrate.nomos.runtime_enforcement.deployment_nomos_enforcement_mode",
        lambda _deployment_name: "enforce",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            survivor_record_id=survivor_record,
            retired_record_id=retired_record,
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert plan.blocking_conflicts[0]["reason"] == (
            "candidate_record_alias_unproved"
        )
        before = _edges(conn)
        with pytest.raises(SubstrateError, match="blocking edge conflicts"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )
        assert _edges(conn) == before
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.entity_merge_receipts")
            assert cur.fetchone()[0] == 0


def test_cross_producer_duplicate_deduplicates_only_under_declared_policy(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.blocking_conflicts == ()
        assert len(plan.deduplicated_edges) == 1
        entry = plan.deduplicated_edges[0]
        evidence = entry["equivalence_evidence"]
        assert evidence["mode"] == "declared_catalog_equivalence"
        assert evidence["catalog_rule"] == EDGE_EQUIVALENCE_POLICY
        assert evidence["read_trust"]["existing"] == {
            "producer_id": SURVIVOR_SOURCE,
            "effective_classification": "trusted",
            "classification_basis": "system_prefix_without_profile",
            "trust_label": None,
            "admission_mode": None,
            "profile_fingerprint": None,
            "catalog_version_id": None,
            "profile_status": None,
        }
        variance = entry["assertion_variance"]
        assert variance["schema_version"] == (
            "okg.entity_merge.edge_assertion_variance.v1"
        )
        assert set(variance["differences"]) == {
            "attrs",
            "provenance",
            "confidence",
            "source",
            "source_revision",
            "run_id",
        }


def test_cross_producer_candidate_record_alias_uses_declared_policy(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(conn, variation="candidate_alias")
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.blocking_conflicts == ()
        assert len(plan.deduplicated_edges) == 1
        entry = plan.deduplicated_edges[0]
        evidence = entry["equivalence_evidence"]
        assert evidence["mode"] == "declared_catalog_equivalence"
        assert evidence["candidate_record_alias"]["survivor_node_id"] == SURVIVOR
        assert evidence["candidate_record_alias"]["retired_node_id"] == RETIRED
        assert {
            record["node_id"]
            for record in entry["provenance_union"]["source_record_ids"]
        } == {
            SURVIVOR,
            RETIRED,
        }


def test_scalar_source_record_remains_literal_through_dedup_and_apply(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(conn, variation="scalar_record")
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.blocking_conflicts == ()
        assert plan.deduplicated_edges[0]["provenance_union"]["source_record_ids"] == [
            "candidate-record:shared"
        ]
        result = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        assert result.applied
        assert _survivor_edge(conn)["source_record_id"] == ("candidate-record:shared")


@pytest.mark.parametrize(
    ("policy", "reason"),
    [
        pytest.param(None, "equivalence_policy_missing", id="missing"),
        pytest.param(
            {
                **EDGE_EQUIVALENCE_POLICY,
                "survivor": EDGE_EQUIVALENCE_POLICY["retiree"],
                "retiree": EDGE_EQUIVALENCE_POLICY["survivor"],
            },
            "equivalence_policy_party_mismatch",
            id="reversed",
        ),
    ],
)
def test_cross_producer_pair_without_or_against_declared_policy_blocks(
    clean_runtime: str,
    policy: dict | None,
    reason: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(conn, policy=policy)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert plan.blocking_conflicts[0]["reason"] == reason


def test_runtime_refuses_governance_attrs_even_in_direct_catalog_rows(
    clean_runtime: str,
) -> None:
    policy = deepcopy(EDGE_EQUIVALENCE_POLICY)
    policy["audit_attrs"] = ["classification"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(
            conn,
            policy=policy,
            variation="governance_attr",
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert plan.blocking_conflicts[0]["reason"] == ("equivalence_policy_invalid")


def test_edge_id_hash_collision_blocks_on_the_exact_structural_tuple(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import _edge_id

    other_paper = "paper:hash-collision-fixture"
    other_concept = "concept:hash-collision-fixture"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, _checkpoint = _seed_cross_producer(conn)
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.apply_facts(
            [
                NodeFact(
                    other_paper,
                    "paper",
                    {"title": "collision fixture"},
                    {"id": other_paper},
                    {"v": 1},
                ),
                NodeFact(
                    other_concept,
                    "concept",
                    {"name": "collision fixture"},
                    {"id": other_concept},
                    {"v": 1},
                ),
            ],
            branch="default",
            source="collision-fixture",
        )
        store.apply_facts(
            [
                EdgeFact(
                    other_paper,
                    other_concept,
                    "references",
                    attrs={"fixture": True},
                )
            ],
            branch="default",
            source="collision-fixture",
        )
        collision_id = _edge_id(other_paper, "references", other_concept)
        store.checkpoint_generation(
            branch="default",
            deployment_name=DEPLOYMENT,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture": "forced-edge-id-hash-collision"},
        )
        monkeypatch.setattr(
            "okg.substrate.graph_store._edge_id",
            lambda _src, _edge_type, _dst: collision_id,
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert plan.blocking_conflicts[0]["reason"] == "edge_id_hash_collision"


def test_cross_producer_apply_keeps_survivor_assertion_and_retires_duplicate(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(conn)
        before = _survivor_edge(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        result = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        assert result.applied
        assert _survivor_edge(conn) == before
        assert RETIRED not in _live_node_ids(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT plan FROM okg.entity_merge_receipts WHERE retired_node_id = %s",
                (RETIRED,),
            )
            stored = cur.fetchone()[0]
        if isinstance(stored, str):
            stored = json.loads(stored)
        stored_entry = stored["deduplicated_edges"][0]
        assert (
            stored_entry["assertion_variance"]
            == (plan.deduplicated_edges[0]["assertion_variance"])
        )
        assert (
            stored_entry["equivalence_evidence"]
            == (plan.deduplicated_edges[0]["equivalence_evidence"])
        )


@pytest.mark.parametrize(
    ("variation", "reason"),
    [
        ("semantic", "semantic_attrs_differ"),
        ("source_record", "source_record_differs"),
        ("lineage", "lineage_differs"),
        ("read_trust", "read_trust_differs"),
        ("confidence", "survivor_confidence_lower"),
    ],
)
def test_policy_semantic_or_read_scope_variance_still_blocks(
    clean_runtime: str,
    variation: str,
    reason: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(conn, variation=variation)
        before = _edges(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert plan.blocking_conflicts[0]["reason"] == reason
        with pytest.raises(SubstrateError, match="blocking edge conflicts"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )
        assert _edges(conn) == before
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.entity_merge_receipts")
            assert cur.fetchone()[0] == 0


@pytest.mark.parametrize(
    ("variation", "reason"),
    [
        ("malformed_lineage", "lineage_malformed"),
        ("truncated_lineage", "lineage_truncated"),
        ("absent_lineage", "lineage_differs"),
        ("both_absent_lineage", "lineage_absent"),
    ],
)
def test_invalid_lineage_still_refuses_cross_producer_dedup(
    clean_runtime: str,
    variation: str,
    reason: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cross_producer(conn, variation=variation)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert plan.blocking_conflicts[0]["reason"] == reason


@pytest.mark.parametrize("drift", ["profile", "catalog_rule", "audit_value"])
def test_assertion_policy_and_trust_evidence_are_digest_bound(
    clean_runtime: str,
    drift: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, _checkpoint = _seed_cross_producer(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        if drift == "profile":
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO okg.producer_profiles "
                    "(deployment_name, producer_id, producer_kind, trust_label, "
                    "admission_mode, catalog_version_id, profile_fingerprint, "
                    "status) VALUES (%s, %s, 'enricher', 'untrusted', 'review', "
                    "%s, 'fixture:survivor-now-untrusted', 'active')",
                    (DEPLOYMENT, SURVIVOR_SOURCE, cv),
                )
        elif drift == "catalog_rule":
            changed_policy = deepcopy(EDGE_EQUIVALENCE_POLICY)
            changed_policy["audit_attrs"] = sorted(
                [*changed_policy["audit_attrs"], "additional_audit_field"]
            )
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE okg.narrowings SET attr_rules = %s::jsonb "
                    "WHERE catalog_version_id = %s AND parent_subtype = 'paper' "
                    "AND edge_type = 'references' AND child_subtype = 'concept'",
                    (
                        json.dumps({"entity_merge_equivalence": changed_policy}),
                        cv,
                    ),
                )
        else:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            session = store.checkout("default")
            retired_edge = dict(
                session.query(
                    "SELECT edge_id, src, dst, edge_type, attrs, provenance, "
                    "confidence, source, source_record_id, source_revision, "
                    "run_id, observed_at FROM okg.graph_edges "
                    "WHERE src = :src AND dst = :dst AND edge_type = 'references'",
                    {"src": CHUNK, "dst": RETIRED},
                )[0]
            )
            retired_edge["attrs"] = {
                **dict(retired_edge["attrs"]),
                "src_kind": "document_chunk_changed",
            }
            with session.transaction():
                session.upsert_rows("okg.graph_edges", [retired_edge])
            store.checkpoint_generation(
                branch="default",
                deployment_name=DEPLOYMENT,
                catalog_version_id=cv,
                status="published",
                metadata={"fixture": "audit-value-drift"},
            )
        changed = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert changed.plan_digest != plan.plan_digest
        if drift == "catalog_rule":
            assert (
                changed.deduplicated_edges[0]["equivalence_evidence"]["catalog_rule"]
                != plan.deduplicated_edges[0]["equivalence_evidence"]["catalog_rule"]
            )
        if drift == "audit_value":
            assert (
                changed.deduplicated_edges[0]["assertion_variance"]
                != plan.deduplicated_edges[0]["assertion_variance"]
            )
        before = _edges(conn)
        with pytest.raises(ValueError, match="plan digest changed"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )
        assert _edges(conn) == before
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.entity_merge_receipts")
            assert cur.fetchone()[0] == 0


def test_apply_locks_digest_bound_policy_and_profile_evidence(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Policy writers cannot cross the apply-time replan/commit boundary."""
    import okg.substrate.entity_merge as entity_merge

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, _checkpoint = _seed_cross_producer(conn)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.producer_profiles "
                "(deployment_name, producer_id, producer_kind, trust_label, "
                "admission_mode, catalog_version_id, profile_fingerprint, status) "
                "VALUES (%s, %s, 'enricher', 'trusted', 'fast_track', %s, %s, "
                "'active')",
                [
                    (DEPLOYMENT, SURVIVOR_SOURCE, cv, "fixture:survivor-trusted"),
                    (DEPLOYMENT, RETIREE_SOURCE, cv, "fixture:retiree-trusted"),
                ],
            )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )

        real_plan_from_graph = entity_merge._plan_from_graph
        calls = 0
        blocked_writes: list[str] = []

        def _observe_locked_replan(*args, **kwargs):
            nonlocal calls
            current = real_plan_from_graph(*args, **kwargs)
            calls += 1
            if calls != 2:
                return current
            attempts = {
                "producer_profiles": (
                    "UPDATE okg.producer_profiles SET trust_label = 'untrusted' "
                    "WHERE deployment_name = %s AND producer_id = %s",
                    (DEPLOYMENT, SURVIVOR_SOURCE),
                ),
                "narrowings": (
                    "UPDATE okg.narrowings SET attr_rules = '{}'::jsonb "
                    "WHERE catalog_version_id = %s AND parent_subtype = 'paper' "
                    "AND edge_type = 'references' AND child_subtype = 'concept'",
                    (cv,),
                ),
            }
            for table, (statement, params) in attempts.items():
                with psycopg.connect(clean_runtime, autocommit=True) as writer:
                    with writer.cursor() as writer_cur:
                        writer_cur.execute("SET lock_timeout = '100ms'")
                        with pytest.raises(psycopg.errors.LockNotAvailable):
                            writer_cur.execute(statement, params)
                blocked_writes.append(table)
            return current

        monkeypatch.setattr(
            entity_merge,
            "_plan_from_graph",
            _observe_locked_replan,
        )
        result = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        assert result.applied
        assert blocked_writes == ["producer_profiles", "narrowings"]


def test_parallel_branch_merges_serialize_without_lock_upgrade_deadlock(
    clean_runtime: str,
) -> None:
    """Different branches serialize at the shared profile-write boundary."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, _checkpoint = _seed_cross_producer(conn)
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.ensure_branch("parallel", from_branch="default")
        store.checkpoint_generation(
            branch="parallel",
            deployment_name=DEPLOYMENT,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture": "issue-1084-parallel-branch"},
        )

    ready = Barrier(2)

    def _merge(branch_name: str):
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '5s'")
            plan = plan_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                branch_name=branch_name,
                accept_survivor_attributes=True,
            )
            ready.wait(timeout=5)
            return remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                branch_name=branch_name,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(_merge, ("default", "parallel")))
    assert all(result.applied for result in results)


def test_a_different_source_record_still_blocks_the_merge(
    clean_runtime: str,
) -> None:
    """Deduplicating these would drop one of two distinct claims, narrowing
    what a record-level read policy can see."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, retired_record_id={"chunk": CHUNK, "span": 99})
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.deduplicated_edges == ()
        assert [item["kind"] for item in plan.blocking_conflicts] == [
            "edge_identity_conflict"
        ]
        assert plan.blocking_conflicts[0]["reason"] == "identity_differs"
        before = _edges(conn)
        with pytest.raises(SubstrateError, match="blocking edge conflicts"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )
        assert _edges(conn) == before


def test_deduplicated_entry_records_the_provenance_it_does_not_carry_over(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        variance = plan.deduplicated_edges[0]["provenance_variance"]
        # The two runs differ, so the block is non-empty and names both sides.
        assert variance, "cross-run duplicate recorded no provenance variance"
        assert set(variance) <= {"source_revision", "lineage_records"}
        for field in variance.values():
            assert field["kept"] != field["not_carried_over"]


def test_apply_retires_the_duplicate_and_records_variance_in_the_receipt(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        result = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        assert result.applied

        edges = _edges(conn)
        # The duplicate assertion is gone; the survivor's own edge is untouched.
        assert [(e["src"], e["dst"]) for e in edges] == [(CHUNK, SURVIVOR)]
        # The retiree is no longer live on the working branch. (A replan still
        # reads the pinned published checkpoint, which predates the merge, so
        # node liveness is checked on the branch rather than through the plan.)
        assert RETIRED not in _live_node_ids(conn)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT plan FROM okg.entity_merge_receipts WHERE retired_node_id = %s",
                (RETIRED,),
            )
            stored = cur.fetchone()[0]
        if isinstance(stored, str):
            stored = json.loads(stored)
        assert stored["deduplicated_edges"][0]["provenance_variance"]
