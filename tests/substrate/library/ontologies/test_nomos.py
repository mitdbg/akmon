from __future__ import annotations

import json
import uuid
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.nomos import (
    ACTIONS,
    DATA_CLASSIFICATIONS,
    DEPLOYMENT_CLASSES,
    EFFECTS,
    OBLIGATION_TYPES,
    PRINCIPAL_KINDS,
    REASON_CODE_PATTERN,
    RESOURCE_KINDS,
    SOURCE_SENSITIVITIES,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.module_manifest import load_manifest
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


NOMOS_DIR = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/nomos"
)


def test_nomos_module_manifest_validates() -> None:
    manifest = load_manifest(NOMOS_DIR / "module.yaml")

    assert manifest.name == "nomos"
    assert manifest.version == "1.0.0"
    assert manifest.provides.subtypes_file == "schema.yaml"


def test_nomos_schema_declares_policy_config_and_identity_classes() -> None:
    classes = yaml.safe_load((NOMOS_DIR / "schema.yaml").read_text())["classes"]

    for class_name in {
        "PolicyBundle",
        "PolicyRule",
        "PolicyScope",
        "PolicySubject",
        "PolicyAction",
        "PolicyResource",
        "PolicyDecision",
        "NomosConfig",
        "NomosConfigScope",
        "SubjectBinding",
        "PermissionGrant",
        "IdentityProvider",
        "ExternalPrincipal",
        "ExternalGroupOrRole",
        "AuthClaimAssertion",
    }:
        assert class_name in classes


def test_nomos_schema_vocabulary_enums_match_python_contract() -> None:
    schema = yaml.safe_load((NOMOS_DIR / "schema.yaml").read_text())

    assert _enum_values(schema, "NomosAction") == set(ACTIONS)
    assert _enum_values(schema, "NomosDataClassification") == set(
        DATA_CLASSIFICATIONS
    )
    assert _enum_values(schema, "NomosDeploymentClass") == set(DEPLOYMENT_CLASSES)
    assert _enum_values(schema, "NomosEffect") == set(EFFECTS)
    assert _enum_values(schema, "NomosObligationType") == set(OBLIGATION_TYPES)
    assert _enum_values(schema, "NomosPrincipalKind") == set(PRINCIPAL_KINDS)
    assert _enum_values(schema, "NomosResourceKind") == set(RESOURCE_KINDS)
    assert _enum_values(schema, "NomosSourceSensitivity") == set(
        SOURCE_SENSITIVITIES
    )
    assert _attr_range(schema, "PolicyRule", "actions") == "NomosAction"
    assert _attr_is_multivalued(schema, "PolicyRule", "actions") is True
    assert (
        _attr_range(schema, "PolicyRule", "obligation_types")
        == "NomosObligationType"
    )
    assert (
        _attr_pattern(schema, "PolicyRule", "reason_code")
        == REASON_CODE_PATTERN
    )
    assert _attr_range(schema, "PolicyScope", "deployment_class") == (
        "NomosDeploymentClass"
    )
    assert _attr_range(schema, "PolicySubject", "subject_kind") == (
        "NomosPrincipalKind"
    )
    assert _attr_range(schema, "PolicyAction", "action_id") == "NomosAction"
    assert _attr_range(schema, "PolicyResource", "source_sensitivity") == (
        "NomosSourceSensitivity"
    )
    assert _attr_range(schema, "PolicyDecision", "action") == "NomosAction"
    assert (
        _attr_pattern(schema, "PolicyDecision", "reason_code")
        == REASON_CODE_PATTERN
    )
    assert _attr_range(schema, "PermissionGrant", "actions") == "NomosAction"
    assert _attr_is_multivalued(schema, "PermissionGrant", "actions") is True
    assert (
        _attr_range(schema, "PermissionGrant", "classifications")
        == "NomosDataClassification"
    )
    assert (
        _attr_is_multivalued(schema, "PermissionGrant", "classifications")
        is True
    )


def test_nomos_composes_into_catalog(tmp_path: Path) -> None:
    deployment = tmp_path / "nomos-deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "nomos-deployment",
            "modules": ["nomos"],
        }),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()

    composed = compose_catalog(deployment)

    assert "policy_bundle" in composed.subtypes
    assert "identity_provider" in composed.subtypes
    assert (
        "policy_bundle",
        "contains",
        "policy_rule",
    ) in composed.narrowing_triples()


@pytest.mark.postgres
def test_nomos_catalog_publishes_policy_bundle_and_rule_nodes(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    deployment = tmp_path / "nomos-publish"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "nomos-publish",
            "modules": ["nomos"],
        }),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)
        _insert_nomos_policy_facts(conn)
        conn.commit()

    published = Publisher(clean_runtime, "nomos-publish", catalog_identity=bind_test_catalog_writer(clean_runtime, "nomos-publish")).cycle()

    assert published["status"] == "published"
    # The chronos-native cutover dropped okg.nodes_live / okg.edges_live; the
    # published policy bundle + rule nodes and their edge are read from the
    # Chronos graph at the published generation through a checkout.
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(published["generation_id"])
        subtypes = sorted(
            str(r["subtype"])
            for r in graph.query("SELECT subtype FROM okg.graph_nodes")
        )
        edge_types = sorted(
            str(r["edge_type"])
            for r in graph.query("SELECT edge_type FROM okg.graph_edges")
        )
    finally:
        store.close()

    assert subtypes == ["policy_bundle", "policy_rule"]
    assert edge_types == ["contains"]


def _insert_nomos_policy_facts(conn: psycopg.Connection) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    run_id = str(uuid.uuid4())
    nodes = [
        (
            "nomos:policy:test",
            "policy_bundle",
            {
                "policy_bundle_id": "nomos:test:ontology",
                "policy_version": "v1",
                "default_effect": "deny",
                "text": "Nomos ontology publish test policy.",
            },
        ),
        (
            "nomos:rule:test:allow",
            "policy_rule",
            {
                "rule_id": "allow_test",
                "policy_bundle_id": "nomos:test:ontology",
                "effect": "allow",
                "actions": ["source.emit"],
                "subjects": ["subject:test"],
                "reason_code": "nomos.test.allow_source_emit",
                "obligation_types": ["audit"],
                "text": "Allow test subject.",
            },
        ),
    ]
    with conn.cursor() as cur:
        for node_id, subtype, attrs in nodes:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s::jsonb, 'nomos-test', %s::jsonb,
                        %s::jsonb, now(), %s, %s, 'I')
                """,
                (
                    node_id,
                    subtype,
                    json.dumps(attrs),
                    json.dumps({"id": node_id}),
                    json.dumps({"v": 1}),
                    f"{run_id}:{node_id}:{uuid.uuid4().hex[:8]}",
                    run_id,
                ),
            )
        cur.execute(
            """
            INSERT INTO okg.edge_facts
              (edge_id, src, dst, edge_type, attrs, provenance,
               confidence, source, source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (%s, 'nomos:policy:test', 'nomos:rule:test:allow',
                    'contains', '{}'::jsonb, 'explicit', 1.0,
                    'nomos-test', %s::jsonb, %s::jsonb,
                    now(), %s, %s, 'I')
            """,
            (
                uuid.uuid4().int & ((1 << 63) - 1),
                json.dumps({
                    "src": "nomos:policy:test",
                    "dst": "nomos:rule:test:allow",
                }),
                json.dumps({"v": 1}),
                f"{run_id}:contains:{uuid.uuid4().hex[:8]}",
                run_id,
            ),
        )
    # Post chronos-native cutover the publisher checkpoints the Chronos branch
    # head; it no longer projects node_facts/edge_facts. Write the same nodes
    # + edge to the Chronos graph so the published generation contains them.
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(node_id, subtype, dict(attrs), {"id": node_id}, {"v": 1})
            for node_id, subtype, attrs in nodes
        ]
        + [EdgeFact(
            "nomos:policy:test", "nomos:rule:test:allow", "contains",
            source_record_id={
                "src": "nomos:policy:test",
                "dst": "nomos:rule:test:allow",
            },
            source_revision={"v": 1},
        )],
        source="nomos-test",
        strict_endpoints=True,
    )


def _enum_values(schema: dict[str, object], name: str) -> set[str]:
    enum = schema["enums"][name]  # type: ignore[index]
    return set(enum["permissible_values"])  # type: ignore[index]


def _attr_range(
    schema: dict[str, object],
    class_name: str,
    attr_name: str,
) -> str | None:
    attr = schema["classes"][class_name]["attributes"][attr_name]  # type: ignore[index]
    return attr.get("range")  # type: ignore[no-any-return]


def _attr_pattern(
    schema: dict[str, object],
    class_name: str,
    attr_name: str,
) -> str | None:
    attr = schema["classes"][class_name]["attributes"][attr_name]  # type: ignore[index]
    return attr.get("pattern")  # type: ignore[no-any-return]


def _attr_is_multivalued(
    schema: dict[str, object],
    class_name: str,
    attr_name: str,
) -> bool | None:
    attr = schema["classes"][class_name]["attributes"][attr_name]  # type: ignore[index]
    return attr.get("multivalued")  # type: ignore[no-any-return]
