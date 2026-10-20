"""Catalog contract for deployment-owned entity-merge edge equivalence."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import (
    ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION,
    ENTITY_MERGE_RESERVED_AUDIT_ATTRS,
    compose_catalog,
)
from okg.substrate.errors import CatalogModuleMissing


POLICY = {
    "schema_version": ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION,
    "survivor": {
        "producer": "_enricher:cooper_mention_grounding",
        "provenance": "derived_deterministic",
    },
    "retiree": {
        "producer": "_enricher:cooper_physics_extraction",
        "provenance": "explicit",
    },
    "audit_attrs": ["provenance_kind", "evidence_basis", "src_kind"],
    "confidence_rule": "survivor_gte",
}


def _schema(policy=POLICY, *, duplicate_policy=None) -> dict:
    attrs = {
        "paper_references_uncertainty": {
            "annotations": {
                "edge_archetype": "references",
                "src_subtypes": "DocumentChunk",
                "dst_subtypes": "ExperimentalUncertainty",
                "entity_merge_equivalence": policy,
            }
        }
    }
    if duplicate_policy is not None:
        attrs["paper_references_uncertainty_duplicate"] = {
            "annotations": {
                "edge_archetype": "references",
                "src_subtypes": "DocumentChunk",
                "dst_subtypes": "ExperimentalUncertainty",
                "entity_merge_equivalence": duplicate_policy,
            }
        }
    return {
        "id": "https://example.com/okg/modules/cooper",
        "name": "cooper",
        "classes": {
            "DocumentChunk": {
                "annotations": {"archetype": "artifact"},
                "attributes": {"text": {"required": True}},
            },
            "ExperimentalUncertainty": {
                "annotations": {"archetype": "concept"},
                "attributes": {"name": {"required": True}},
            },
            "EdgeNarrowings": {
                "annotations": {"okg_edge_narrowings": "true"},
                "attributes": attrs,
            },
        },
    }


def _compose(tmp_path: Path, policy=POLICY, *, duplicate_policy=None):
    modules_root = tmp_path / "ontologies"
    module_root = modules_root / "cooper"
    module_root.mkdir(parents=True)
    (module_root / "schema.yaml").write_text(
        yaml.safe_dump(_schema(policy, duplicate_policy=duplicate_policy)),
        encoding="utf-8",
    )
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "schemas").mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "cooper", "modules": ["cooper"]}),
        encoding="utf-8",
    )
    cross_links = tmp_path / "cross-links"
    cross_links.mkdir()
    return compose_catalog(
        deployment,
        modules_root=modules_root,
        cross_links_root=cross_links,
    )


def test_equivalence_policy_round_trips_through_compose(tmp_path: Path) -> None:
    composed = _compose(tmp_path)
    triple = ("document_chunk", "references", "experimental_uncertainty")
    assert composed.narrowing_attr_rules()[triple] == {
        "entity_merge_equivalence": {
            **POLICY,
            "audit_attrs": sorted(POLICY["audit_attrs"]),
        }
    }


def test_invalid_equivalence_policy_refuses(tmp_path: Path) -> None:
    mutations = [
        lambda p: p.update({"unknown": True}),
        lambda p: p.update({"schema_version": "v2-ish"}),
        lambda p: p.update({"confidence_rule": "highest_wins"}),
        lambda p: p["survivor"].update({"producer": ""}),
        lambda p: p["survivor"].update({"producer": 123}),
        lambda p: p["retiree"].update({"provenance": ""}),
        lambda p: p["retiree"].update({"provenance": False}),
        lambda p: p.update({"audit_attrs": []}),
        lambda p: p.update({"audit_attrs": ["evidence_basis", 123]}),
        lambda p: p.update({"audit_attrs": ["evidence_basis", "evidence_basis"]}),
        *[
            lambda p, key=key: p.update({"audit_attrs": [key]})
            for key in sorted(ENTITY_MERGE_RESERVED_AUDIT_ATTRS)
        ],
        lambda p: p.update({"survivor": deepcopy(p["retiree"])}),
    ]
    for index, mutate in enumerate(mutations):
        policy = deepcopy(POLICY)
        mutate(policy)
        with pytest.raises(CatalogModuleMissing, match="entity_merge_equivalence"):
            _compose(tmp_path / f"invalid-{index}", policy)


def test_conflicting_duplicate_policy_refuses(tmp_path: Path) -> None:
    conflicting = deepcopy(POLICY)
    conflicting["audit_attrs"] = ["different_audit_field"]
    with pytest.raises(
        CatalogModuleMissing,
        match="conflicting entity_merge_equivalence",
    ):
        _compose(tmp_path, duplicate_policy=conflicting)


@pytest.mark.postgres
def test_equivalence_policy_round_trips_into_narrowing_attr_rules(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    import psycopg

    from okg.substrate.catalog.modules_apply import apply_composed_catalog

    composed = _compose(tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = apply_composed_catalog(conn, composed)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT attr_rules->'entity_merge_equivalence' "
                "FROM okg.narrowings WHERE catalog_version_id = %s "
                "AND parent_subtype = 'document_chunk' "
                "AND edge_type = 'references' "
                "AND child_subtype = 'experimental_uncertainty'",
                (catalog_version_id,),
            )
            assert cur.fetchone()[0] == {
                **POLICY,
                "audit_attrs": sorted(POLICY["audit_attrs"]),
            }
