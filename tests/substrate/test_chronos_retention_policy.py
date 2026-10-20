from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from okg.substrate.errors import SubstrateError
from okg.substrate.history_compaction import (
    _Checkpoint,
    _selector_floors,
    parse_history_retention,
)
from okg.substrate.publisher import Publisher


ROOT = Path(__file__).resolve().parents[2]


def test_mixed_retention_policy_is_strict_and_classifier_specific() -> None:
    policy = parse_history_retention(
        {
            "default": "full",
            "audit_tail_generations": 3,
            "nodes": {
                "durable_claim": "full",
                "workflow_event": "latest-only",
            },
            "edges": {"observed_in": {"horizon": "14d"}},
        },
        catalog_node_subtypes={"durable_claim", "workflow_event"},
        observed_edge_types={"observed_in"},
    )

    assert policy.rule_for("node", "durable_claim").mode == "full"
    assert policy.rule_for("node", "workflow_event").mode == "latest-only"
    assert policy.rule_for("edge", "observed_in").horizon_seconds == 14 * 86400
    assert policy.audit_tail_generations == 3
    assert policy.digest.startswith("sha256:")


@pytest.mark.parametrize(
    "raw, message",
    [
        ({"default": "latest-only", "audit_tail_generations": 0}, "positive integer"),
        ({"default": {"horizon": "0d"}, "audit_tail_generations": 1}, "positive duration"),
        ({"default": "latest-only", "audit_tail_generations": 1, "surprise": True}, "invalid"),
        ({"default": "sometimes", "audit_tail_generations": 1}, "must be full"),
    ],
)
def test_invalid_retention_policy_fails_closed(raw: dict, message: str) -> None:
    with pytest.raises(SubstrateError, match=message):
        parse_history_retention(
            raw,
            catalog_node_subtypes=set(),
            observed_edge_types=set(),
        )


def test_unknown_policy_classifier_fails_instead_of_defaulting() -> None:
    with pytest.raises(SubstrateError, match="unknown classifiers") as excinfo:
        parse_history_retention(
            {
                "default": "full",
                "audit_tail_generations": 1,
                "nodes": {"typo": "latest-only"},
            },
            catalog_node_subtypes={"claim"},
            observed_edge_types=set(),
        )

    assert excinfo.value.data["unknown_node_subtypes"] == ["typo"]


def test_horizon_and_latest_floors_map_to_checkpoints_and_extend_to_audit_tail() -> None:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    checkpoints = [
        _Checkpoint(
            checkpoint_id=f"g{index}",
            target_generation_id=index,
            point=index * 10,
            ordinal=index,
            created_at=start + timedelta(days=index),
            catalog_version_id=1,
        )
        for index in range(5)
    ]
    policy = parse_history_retention(
        {
            "default": "full",
            "audit_tail_generations": 2,
            "nodes": {
                "bounded": {"horizon": "2d"},
                "ephemeral": "latest-only",
                "durable": "full",
            },
            "edges": {},
        },
        catalog_node_subtypes={"bounded", "ephemeral", "durable"},
        observed_edge_types=set(),
    )
    floors = _selector_floors(
        policy=policy,
        kind="node",
        classifiers={"bounded", "ephemeral", "durable"},
        checkpoints=checkpoints,
        now=start + timedelta(days=5),
    )
    assert floors["bounded"]["checkpoint_id"] == "g3"
    assert floors["ephemeral"]["checkpoint_id"] == "g3"
    assert floors["durable"]["checkpoint_id"] is None


def test_direct_publisher_construction_resolves_manifest_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.deployments as deployments

    policy = {"default": "full", "audit_tail_generations": 2}
    monkeypatch.setattr(
        deployments,
        "resolve_deployment",
        lambda _name: SimpleNamespace(chronos={"history_retention": policy}),
    )
    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda _name: Path("/declared/deployment"),
    )

    publisher = Publisher("postgresql://unused", "declared-deployment")
    assert publisher.history_retention_policy == policy


def test_direct_publisher_refuses_real_manifest_loader_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.deployments as deployments

    def fail(_name: str) -> None:
        raise RuntimeError("malformed deployment manifest")

    monkeypatch.setattr(deployments, "resolve_deployment", fail)
    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda _name: Path("/declared/deployment"),
    )

    with pytest.raises(RuntimeError, match="malformed deployment manifest"):
        Publisher("postgresql://unused", "declared-deployment")


def test_deployment_schema_accepts_exact_history_retention_shape() -> None:
    schema = json.loads((ROOT / "schemas/deployment.schema.json").read_text())
    validator = Draft202012Validator(schema)
    deployment = {
        "name": "history-demo",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "postgres": {"dsn": "postgresql://example.invalid/demo"},
        "chronos": {
            "interval_continuation_percent": 95,
            "history_retention": {
                "default": "full",
                "audit_tail_generations": 2,
                "nodes": {"workflow_event": "latest-only"},
                "edges": {"observed_in": {"horizon": "30d"}},
            },
        },
    }
    assert list(validator.iter_errors(deployment)) == []

    deployment["chronos"]["history_retention"]["surprise"] = True
    assert any("Additional properties" in error.message for error in validator.iter_errors(deployment))


def test_deployment_schema_accepts_the_idle_in_transaction_bound() -> None:
    """The published schema and the manifest loader must agree (issue #1310).

    `postgres` is `additionalProperties: false`, so a knob the loader accepts
    but the schema rejects fails validation everywhere the schema is enforced
    (deployment bootstrap, `okg schema show`) while passing every loader test.
    """
    schema = json.loads((ROOT / "schemas/deployment.schema.json").read_text())
    validator = Draft202012Validator(schema)
    deployment = {
        "name": "idle-bound-demo",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "postgres": {
            "dsn": "postgresql://example.invalid/demo",
            "idle_in_transaction_session_timeout_ms": 900000,
        },
    }
    assert list(validator.iter_errors(deployment)) == []

    deployment["postgres"]["idle_in_transaction_session_timeout_ms"] = 0
    assert list(validator.iter_errors(deployment)) == []

    deployment["postgres"]["idle_in_transaction_session_timeout_ms"] = -1
    assert list(validator.iter_errors(deployment)) != []

    deployment["postgres"]["idle_in_transaction_session_timeout_ms"] = "5min"
    assert list(validator.iter_errors(deployment)) != []
