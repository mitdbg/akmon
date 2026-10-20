"""Published-generation proof for the consistency ontology module.

task.core.ontology-module (consistency-claim-model-core), the publish half:
the five (C, Σ, ≻) subtypes land as ROWS in a published generation when a
scratch deployment composes ["extraction", "consistency"] against a clean
Postgres runtime, a correction links to its target claim, a precedent
carries its scope, and the narrowing gate rejects a mis-typed edge.

Enforcement reality (discovered, not invented — cite before extending):

* Narrowing gating IS mechanical. TransformConsistencyGate
  (src/okg/substrate/consistency/transform_gate.py) lowers the narrowing +
  node-subtype + no-dangling programs over okg.graph_nodes/graph_edges at
  publish; an edge whose (parent_subtype, edge_type, child_subtype) triple
  is absent from the applied catalog BLOCKS the publish and is named in
  details["narrowing_violation_edge_ids"].
* Node-subtype gating IS mechanical: a node whose subtype is not in the
  applied catalog blocks the same gate
  (details["node_subtype_violation_node_ids"]).
* Required-attr gating is NOT mechanical today. LinkML ``required: true``
  never leaves the schema file: ComposedCatalog
  (src/okg/substrate/catalog/modules_compose.py) carries subtypes, parents,
  narrowings, aliases, identifier patterns, and searchable-text metadata
  only, and apply_composed_catalog persists exactly those — so no
  projection or publish layer can see (let alone reject) a missing
  required attribute. ``test_missing_required_attr_publishes_today`` is
  the honest witness: it pins the current pass-through and FAILS the day
  enforcement lands (then invert the assertion; do not delete the test).
  If enforcement is built, it belongs in compose (harvest requiredness) →
  apply (persist per-subtype required attrs) → TransformConsistencyGate
  (attrs antijoin), not in a bespoke checker.
* Enum gating is NOT mechanical either. The library convention
  (academic_researcher, methodology) records enums as description PROSE —
  consistency/schema.yaml does the same for scope_state / action /
  policy_kind / outcome / locality — there is no LinkML enum range and
  nothing at insertion or publish validates values against the prose sets.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
import yaml

psycopg = pytest.importorskip("psycopg")

from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore, _edge_id  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _install_composed_catalog(dsn: str, tmp_path: Path) -> int:
    """Compose the REAL library modules ["extraction", "consistency"] for a
    scratch deployment and apply the result to the runtime DB."""
    d = tmp_path / "scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "scratch",
                "schema_dir": "schemas",
                "modules": ["extraction", "consistency"],
            }
        )
    )
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        return apply_composed_catalog(conn, composed)


def _publisher(dsn: str) -> Publisher:
    return Publisher(dsn, "default", consistency_gates=[TransformConsistencyGate()], catalog_identity=bind_test_catalog_writer(dsn, "default"))


def test_consistency_rows_publish_in_generation(
    clean_runtime: str, tmp_path: Path
) -> None:
    """All five subtypes + all three narrowed edges land in a PUBLISHED
    generation; the correction links to its target claim; the precedent
    carries its scope."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim = f"claim:{sfx}"
    correction = f"correction:{sfx}"
    precedent = f"precedent:{sfx}"
    policy = f"policy:{sfx}"
    finding = f"finding:{sfx}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                # ConsistencyClaim with ALL required attrs (predicate,
                # normalized_key, value, scope_state, claim_provenance).
                NodeFact(
                    claim,
                    "consistency_claim",
                    {
                        "predicate": "preferred_target",
                        "normalized_key": "hcal|preferred_target",
                        "value": "CMSSW_14_0_7",
                        "scope": "HCAL/Run3",
                        "scope_state": "known",
                        "claim_provenance": "derived",
                        "source_id": f"page:{sfx}",
                    },
                    {"id": claim},
                    {"v": 1},
                ),
                # ClaimCorrection with the review-F1 observed-content
                # snapshot (baseline for the stale_correction_review flag).
                NodeFact(
                    correction,
                    "claim_correction",
                    {
                        "correction_id": f"corr:{sfx}",
                        "target_claim_id": claim,
                        "action": "override",
                        "sequence": 1,
                        "observed_content": json.dumps(
                            {
                                "predicate": "preferred_target",
                                "normalized_key": "hcal|preferred_target",
                                "value": "CMSSW_14_0_7",
                                "scope": "HCAL/Run3",
                            }
                        ),
                        "value": "CMSSW_14_0_9",
                        "decided_by": "operator:riley",
                    },
                    {"id": correction},
                    {"v": 1},
                ),
                # ClaimPrecedent carrying its scope.
                NodeFact(
                    precedent,
                    "claim_precedent",
                    {
                        "precedent_id": f"prec:{sfx}",
                        "prefer_source": "release_notes",
                        "scope": "HCAL/Run3",
                        "sequence": 1,
                        "decided_by": "operator:riley",
                    },
                    {"id": precedent},
                    {"v": 1},
                ),
                # PredicatePolicy — ALL FOUR Σ row kinds publish (review
                # L3; the shapes are review-F6 + H2's unresolved_key attr).
                NodeFact(
                    policy,
                    "predicate_policy",
                    {
                        "policy_id": f"pol:{sfx}",
                        "policy_kind": "functional",
                        "predicate": "preferred_target",
                        "enabled": "true",
                    },
                    {"id": policy},
                    {"v": 1},
                ),
                NodeFact(
                    f"{policy}:pair",
                    "predicate_policy",
                    {
                        "policy_id": f"pol:{sfx}:pair",
                        "policy_kind": "incompatible_pair",
                        "predicate": "lifecycle_status",
                        "value_a": "current",
                        "value_b": "retired",
                        "enabled": "true",
                    },
                    {"id": f"{policy}:pair"},
                    {"v": 1},
                ),
                NodeFact(
                    f"{policy}:link",
                    "predicate_policy",
                    {
                        "policy_id": f"pol:{sfx}:link",
                        "policy_kind": "linked_pair",
                        "predicate": "preferred_target",
                        "target_predicate": "lifecycle_status",
                        "violates_values": ["deprecated", "retired"],
                        "link_key_fn": "normalize_entity_value",
                        "enabled": "true",
                    },
                    {"id": f"{policy}:link"},
                    {"v": 1},
                ),
                NodeFact(
                    f"{policy}:unres",
                    "predicate_policy",
                    {
                        "policy_id": f"pol:{sfx}:unres",
                        "policy_kind": "unresolved_key",
                        "predicate": "preferred_target",
                        "unresolved_key": "ACTIVITY_UNKNOWN",
                        "enabled": "true",
                    },
                    {"id": f"{policy}:unres"},
                    {"v": 1},
                ),
                # ConflictFinding — tier-2 derived finding node.
                NodeFact(
                    finding,
                    "conflict_finding",
                    {
                        "finding_id": f"find:{sfx}",
                        "constraint_id": f"pol:{sfx}",
                        "predicate": "preferred_target",
                        "bucket_key": "hcal|preferred_target",
                        "outcome": "definite",
                        "locality": "internal",
                        "finding_status": "open",
                    },
                    {"id": finding},
                    {"v": 1},
                ),
                # The three narrowed edges.
                EdgeFact(
                    correction,
                    claim,
                    "corrects",
                    source_record_id={"id": f"e1:{sfx}"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    finding,
                    claim,
                    "involves_claim",
                    source_record_id={"id": f"e2:{sfx}"},
                    source_revision={"v": 1},
                ),
                EdgeFact(
                    finding,
                    policy,
                    "constrained_by",
                    source_record_id={"id": f"e3:{sfx}"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "published", out
    gen = out["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)

        # Generation-pinned: the chronos generation checkpoint this cycle
        # produced is the latest PUBLISHED generation for the deployment
        # (under chronos_native, cycle()["generation_id"] is the generation
        # token, which is the checkpoint id verbatim).
        latest = store.latest_generation(
            deployment_name="default", status="published"
        )
        assert latest is not None
        assert latest.checkpoint_id == gen, (latest.checkpoint_id, gen)
        assert dict(latest.metadata or {}).get("status") == "published"
        nodes = {
            r["node_id"]: r
            for r in store.query(
                "SELECT node_id, subtype, attrs FROM okg.graph_nodes"
            )
        }
        # Row presence for every subtype.
        assert nodes[claim]["subtype"] == "consistency_claim"
        assert nodes[correction]["subtype"] == "claim_correction"
        assert nodes[precedent]["subtype"] == "claim_precedent"
        assert nodes[policy]["subtype"] == "predicate_policy"
        assert nodes[finding]["subtype"] == "conflict_finding"

        # The claim row carries the required attrs it was inserted with.
        claim_attrs = nodes[claim]["attrs"]
        assert claim_attrs["normalized_key"] == "hcal|preferred_target"
        assert claim_attrs["scope_state"] == "known"
        assert claim_attrs["claim_provenance"] == "derived"

        # The correction carries its observed-content snapshot (review F1)
        # and names its target claim as an attr (review H1).
        assert json.loads(nodes[correction]["attrs"]["observed_content"])[
            "value"
        ] == "CMSSW_14_0_7"
        assert nodes[correction]["attrs"]["target_claim_id"] == claim

        # All FOUR Σ row kinds published (review L3/H2).
        kinds = {
            nodes[n]["attrs"]["policy_kind"]
            for n in (policy, f"{policy}:pair", f"{policy}:link", f"{policy}:unres")
        }
        assert kinds == {
            "functional", "incompatible_pair", "linked_pair", "unresolved_key"
        }
        assert nodes[f"{policy}:unres"]["attrs"]["unresolved_key"] == "ACTIVITY_UNKNOWN"

        # The finding carries its lifecycle attr (review M2 / decision D2).
        assert nodes[finding]["attrs"]["finding_status"] == "open"

        # The precedent carries its scope.
        assert nodes[precedent]["attrs"]["scope"] == "HCAL/Run3"

        edges = {
            (r["src"], r["edge_type"], r["dst"])
            for r in store.query(
                "SELECT src, edge_type, dst FROM okg.graph_edges"
            )
        }
        # The correction LINKS to its target claim.
        assert (correction, "corrects", claim) in edges
        assert (finding, "involves_claim", claim) in edges
        assert (finding, "constrained_by", policy) in edges


def test_missing_required_attr_publishes_today(
    clean_runtime: str, tmp_path: Path
) -> None:
    """WITNESS OF NON-ENFORCEMENT (see module docstring): a consistency_claim
    missing the LinkML-required ``normalized_key`` attr publishes anyway,
    because ComposedCatalog / apply_composed_catalog never persist attribute
    requiredness, so no publish-gate program can antijoin on it. This test
    pins that reality; when required-attr gating lands in the substrate,
    this test MUST fail — invert it into a blocked-publish assertion."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    claim = f"claim:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    claim,
                    "consistency_claim",
                    {
                        # normalized_key (required: true) deliberately absent.
                        "predicate": "preferred_target",
                        "value": "CMSSW_14_0_7",
                        "scope_state": "known",
                        "claim_provenance": "derived",
                    },
                    {"id": claim},
                    {"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    out = _publisher(clean_runtime).cycle()
    # NOT a silent drop — the row lands, attr genuinely absent. The gap is
    # the absence of a rejection layer, and this assertion is its witness.
    assert out["status"] == "published", out
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
            f"SELECT attrs FROM okg.graph_nodes WHERE node_id = '{claim}'"
        )
    assert len(rows) == 1
    assert "normalized_key" not in rows[0]["attrs"]


def test_narrowing_gate_rejects_mistyped_corrects_edge(
    clean_runtime: str, tmp_path: Path
) -> None:
    """A ``corrects`` edge aimed at a predicate_policy — the triple
    (claim_correction, corrects, predicate_policy) is NOT in the composed
    catalog (only claim_correction --corrects--> consistency_claim is) —
    BLOCKS the publish and is named in narrowing_violation_edge_ids."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    correction = f"correction:{sfx}"
    policy = f"policy:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    correction,
                    "claim_correction",
                    {
                        "correction_id": f"corr:{sfx}",
                        "target_claim_id": f"claim:{sfx}",
                        "action": "retract",
                        "sequence": 1,
                        # Review L2: retract carries the observed-content
                        # baseline per the schema's prose contract.
                        "observed_content": json.dumps(
                            {"predicate": "preferred_target", "value": "x"}
                        ),
                        "decided_by": "operator:riley",
                    },
                    {"id": correction},
                    {"v": 1},
                ),
                NodeFact(
                    policy,
                    "predicate_policy",
                    {
                        "policy_id": f"pol:{sfx}",
                        "policy_kind": "functional",
                        "predicate": "preferred_target",
                        "enabled": "true",
                    },
                    {"id": policy},
                    {"v": 1},
                ),
                EdgeFact(
                    correction,
                    policy,
                    "corrects",
                    source_record_id={"id": f"e:{sfx}"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

    out = _publisher(clean_runtime).cycle()
    assert out["status"] == "blocked", out
    gate = out["consistency_gates"][0]
    assert _edge_id(correction, "corrects", policy) in (
        gate.get("details") or {}
    ).get("narrowing_violation_edge_ids", []), gate
