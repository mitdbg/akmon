"""The SQL read-model check refuses secret-shaped string literals (#2782).

`validate_sql_read_model` parses the statement and inspects its literals in
Python; it opens no database connection. These tests therefore carry no
`postgres` marker and run in the fast lane, unlike the rest of the read
surface policy suite in test_read_surface_policy.py.
"""

from __future__ import annotations

import json

import pytest

from okg.substrate.contracts.deployment_product import build_read_asset
from okg.substrate.views.read_policy import (
    ReadPolicyRequirements,
    ReadSurfacePolicyError,
    build_sql_read_model_asset,
    validate_sql_read_model,
)

RELATION = "wave4_dist.source_rows"


def _policy() -> ReadPolicyRequirements:
    return ReadPolicyRequirements(
        audiences=frozenset({"operator", "agent", "wave4_app"}),
        tenants=frozenset({"tenant-a"}),
        visible_fields=frozenset({"tenant_id", "public_value", "row_count"}),
        decision_ids=frozenset({"nomos:decision:source"}),
    )


def _sql_contract(statement: str):
    policy = _policy()
    source = build_read_asset(
        owner="wave4",
        asset_id=RELATION.replace(".", "-"),
        kind="read-model",
        config_schema="okg.deployment_product.asset.read-model.config/v1",
        result_schema="okg.deployment_product.asset.read-model.result/v1",
        capability_schema="okg.deployment_product.asset.read-model.capability/v1",
        output_identity=f"source:{RELATION}",
        database_object=RELATION,
        refresh_policy={"mode": "generation"},
        policy_requirements=policy.to_dict(),
        lineage_sources=[f"fixture:{RELATION}"],
        config={"source_relation": RELATION},
        provenance={"source": "distribution"},
    )
    contract = build_sql_read_model_asset(
        owner="wave4",
        asset_id="tenant-summary",
        sql=statement,
        distribution_namespace="wave4_dist",
        object_name="tenant_summary",
        source_assets={RELATION: source},
        source_policies={RELATION: policy},
        source_policy_enforcements={RELATION: "row-security"},
        parameters=(),
        result_columns={"tenant_id": "text", "row_count": "bigint"},
        policy=policy,
        maintenance_role="wave4_maint",
        application_role="wave4_app",
        provenance={"source": "distribution"},
    )
    return source, contract


# One row per branch and class member of the check's secret-literal pattern.
# Each literal matches only its own branch. The pattern ignores case, so the
# A-Z and a-z members of the key class stand in for each other and need no row
# of their own. Values are assembled at runtime so the source holds no
# credential-shaped literal, and the ids name the shape, never the value.
_SECRET_SQL_LITERALS = [
    ("dsn", "postgres" + "://db.example.test/okg"),
    ("openai-style-key", "sk" + "-" + "FAKE0sentinel0NOTAKEY"),
    ("password-assignment", "password" + "=fake-value"),
    ("secret-assignment", "secret" + "=fake-value"),
    ("token-assignment", "token" + "=fake-value"),
    ("api-underscore-key-assignment", "api_key" + "=fake-value"),
    ("api-hyphen-key-assignment", "api-key" + "=fake-value"),
]


@pytest.mark.parametrize(
    "literal",
    [literal for _, literal in _SECRET_SQL_LITERALS],
    ids=[name for name, _ in _SECRET_SQL_LITERALS],
)
def test_sql_literal_shaped_like_a_secret_is_refused(literal: str) -> None:
    source, contract = _sql_contract(
        f"SELECT '{literal}' AS tenant_id, pg_catalog.count(*) AS row_count FROM {RELATION}"
    )

    with pytest.raises(ReadSurfacePolicyError) as caught:
        validate_sql_read_model(contract, source_assets={RELATION: source})

    assert caught.value.code == "sql_literal_secret"
    assert literal not in json.dumps(caught.value.data, sort_keys=True, default=str)


def test_sql_literal_without_a_secret_shape_passes_the_literal_check() -> None:
    """The same statement with a plain literal is not refused as a secret."""
    source, contract = _sql_contract(
        f"SELECT 'tenant-a' AS tenant_id, pg_catalog.count(*) AS row_count FROM {RELATION}"
    )

    try:
        validate_sql_read_model(contract, source_assets={RELATION: source})
    except ReadSurfacePolicyError as exc:
        assert exc.code != "sql_literal_secret"
