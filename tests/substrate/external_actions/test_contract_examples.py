from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.external_actions.action_contracts import (
    ActionContract,
    digest_value,
    provider_idempotency_key,
    provider_idempotency_key_digest,
)


ROOT = Path(__file__).resolve().parents[3]
VECTORS = ROOT / "tests/fixtures/external_actions/hash-vectors.yaml"
MIGRATION = ROOT / "src/okg/substrate/db/migrations/0030_external_action_review.sql"
SCHEMA = ROOT / "src/okg/substrate/db/schema.sql"


def test_hash_vectors_match_contract() -> None:
    document = yaml.safe_load(VECTORS.read_text())
    checked = 0

    def walk(value):
        nonlocal checked
        if isinstance(value, dict):
            if {"domain", "value", "digest"}.issubset(value):
                assert digest_value(value["domain"], value["value"]) == value["digest"]
                checked += 1
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(document)
    assert checked >= 10


def test_action_contract_and_provider_key_vectors_match() -> None:
    document = yaml.safe_load(VECTORS.read_text())
    values = {row["id"]: row for row in document["vectors"]}
    contract = ActionContract.from_mapping(values["action_contract"]["value"])
    assert contract.action_contract_digest == values["action_contract"]["digest"]
    provider = document["provider_idempotency_key_vector"]
    assert provider_idempotency_key(provider["value"]) == provider["value_rendered"]
    assert provider_idempotency_key_digest(provider["value"]) == digest_value(
        provider["domain"], provider["value"]
    )


def test_restore_sql_uses_the_approved_nomos_operation() -> None:
    for path in (MIGRATION, SCHEMA):
        sql = path.read_text(encoding="utf-8")
        assert "'external_action.restore'" not in sql
        assert "'external_action.restore_reconcile'" in sql
