"""PACT read-masking-from-catalog-pii-class.

The Nomos read filter's redact transform classified by legacy_attr_classifier,
which fail-OPENS (reveals every attribute outside an 8-key legacy blocklist). This
proves the transform now classifies a node row by its subtype's declared
pii_class (okg.subtypes.metadata), so an ANNOTATED subtype gets real masking while
an UNANNOTATED subtype keeps today's legacy behavior -- the contrast over the SAME
attribute is the whole point.
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog, load_modules_root
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import open_session
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.pii_classification import resolve_pii_class
from okg.substrate.nomos.read_filter import (
    _load_subtype_pii_classes,
    filter_node_rows_for_session,
)
from okg.substrate.nomos.read_masking import (
    MASK_TOKEN,
    legacy_attr_classifier,
    subtype_attr_classifier,
)
from tests.substrate._db_helpers import (
    ensure_published_generation,
    seed_catalog_version,
)

pytestmark = pytest.mark.postgres


# --- unit: the subtype-aware classifier factory -----------------------------


def test_subtype_classifier_annotated_classifies_and_failsafes() -> None:
    classify = subtype_attr_classifier(
        {"email": "direct_identifier", "kind": "non_pii"}
    )
    assert classify("email") == "direct_identifier"
    assert classify("kind") == "non_pii"
    # an attribute the annotated map does not list fails SAFE (mask), never reveal.
    assert resolve_pii_class(classify("unlisted")) == "sensitive_attribute"


def test_subtype_classifier_unannotated_falls_back_to_legacy() -> None:
    # Only an absent declaration means unannotated and gets legacy behavior.
    assert subtype_attr_classifier(None) is legacy_attr_classifier
    empty = subtype_attr_classifier({})
    assert empty is not legacy_attr_classifier
    assert resolve_pii_class(empty("anything")) == "sensitive_attribute"


# --- integration: two principals over annotated vs unannotated subtypes ------


def _activate_policy(dsn: str) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:read-masking-subtype",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_privileged",
                "effect": "allow",
                "actions": ["graph.read.mcp"],
                "subjects": ["subject:privileged"],
                "match": {"deployment": "default", "scope": "deployment:default", "source_id": "pii_fixture"},
                "priority": 100,
            },
            {
                "rule_id": "redact_unprivileged",
                "effect": "redact",
                "actions": ["graph.read.mcp"],
                "subjects": ["subject:unprivileged"],
                "match": {"deployment": "default", "scope": "deployment:default", "source_id": "pii_fixture"},
                "priority": 100,
                "reason_code": "nomos.test.redact_unprivileged",
                "obligations": [{"type": "redaction"}, {"type": "audit"}],
            },
        ],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


def _row(subtype: str, attrs: dict) -> dict:
    return {
        "node_id": f"node:{subtype}-a",
        "subtype": subtype,
        "attrs": dict(attrs),
        "source": "pii_fixture",
        "source_record_id": {"id": "a"},
        "source_revision": {"updated_at": "2026-05-01T00:00:00Z"},
    }


def _apply_composed_privacy_catalog(
    clean_runtime: str,
    tmp_path: Path,
) -> int:
    deployment = tmp_path / "privacy_deployment"
    schemas = deployment / "schemas"
    schemas.mkdir(parents=True)
    (deployment / "deployment.yaml").write_text(
        "name: default\nschema_dir: schemas\nmodules:\n- person\n"
    )
    # Deployment-private controls: one has no declaration (legacy), and one is
    # changed below to a present empty declaration (fail-safe opt-in).
    (schemas / "nodes.yaml").write_text(
        """
classes:
  Page:
    annotations:
      archetype: artifact
    attributes:
      email: {}
  EmptyOptIn:
    annotations:
      archetype: artifact
    attributes:
      email: {}
"""
    )
    empty_cross_links = tmp_path / "cross_links"
    empty_cross_links.mkdir()
    composed = compose_catalog(
        deployment,
        modules_root=load_modules_root(),
        cross_links_root=empty_cross_links,
    )
    assert composed.subtype_metadata["person"]["pii_classes"] == {
        "person_id": "direct_identifier",
        "display_name": "quasi_identifier",
        "email": "direct_identifier",
        "orcid": "direct_identifier",
        "inspire_bai": "direct_identifier",
    }

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = apply_composed_catalog(conn, composed)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.subtypes SET metadata = %s "
                "WHERE name = %s AND catalog_version_id = %s",
                (
                    Jsonb({"pii_classes": {}}),
                    "empty_opt_in",
                    cv,
                ),
            )
            cur.execute(
                "SELECT metadata FROM okg.subtypes "
                "WHERE name = 'person' AND catalog_version_id = %s",
                (cv,),
            )
            assert cur.fetchone()[0]["pii_classes"]["email"] == "direct_identifier"
        ensure_published_generation(
            conn, catalog_version_id=cv, deployment_name="default"
        )
    return int(cv)


def test_composed_subtype_masks_empty_opt_in_and_legacy_control(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _apply_composed_privacy_catalog(clean_runtime, tmp_path)

    _activate_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    attrs = {
        "classification": "internal",
        "email": "alice@example.com",
        "display_name": "Alice",
        "secret_note": "confidential",
    }
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        person_unpriv = filter_node_rows_for_session(
            conn, session, [_row("person", attrs)], subject="subject:unprivileged"
        )
        person_priv = filter_node_rows_for_session(
            conn, session, [_row("person", attrs)], subject="subject:privileged"
        )
        page_unpriv = filter_node_rows_for_session(
            conn, session, [_row("page", attrs)], subject="subject:unprivileged"
        )
        empty_unpriv = filter_node_rows_for_session(
            conn,
            session,
            [_row("empty_opt_in", attrs)],
            subject="subject:unprivileged",
        )

    pu = person_unpriv[0]["attrs"]
    pp = person_priv[0]["attrs"]
    gu = page_unpriv[0]["attrs"]
    eu = empty_unpriv[0]["attrs"]

    # The production person LinkML annotations reached the applied catalog:
    # identifiers and unlisted attributes are masked for the redact principal.
    assert pu["email"] == MASK_TOKEN
    assert pu["display_name"] == MASK_TOKEN
    assert pu["secret_note"] == MASK_TOKEN
    assert "alice@example.com" not in json.dumps(person_unpriv)

    # Privileged principal (allow): everything revealed.
    assert pp["email"] == "alice@example.com"

    # The SAME email attribute on the
    # UNANNOTATED 'page' subtype is revealed to the redact principal (legacy
    # fail-open) -- exactly the leak the annotated path closes.
    assert gu["email"] == "alice@example.com"

    # A PRESENT empty map is an explicit opt-in, not an absent declaration.
    assert eu["email"] == MASK_TOKEN
    assert "alice@example.com" not in json.dumps(empty_unpriv)


def test_malformed_present_pii_metadata_fails_closed(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog_version(conn)
        conn.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "VALUES (%s, %s, %s)",
            ("broken", Jsonb({"pii_classes": ["email"]}), cv),
        )
        with pytest.raises(SubstrateError, match="pii_classes must be a mapping"):
            _load_subtype_pii_classes(conn, cv)
