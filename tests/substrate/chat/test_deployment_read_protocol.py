from __future__ import annotations

import builtins
import importlib
import json
from pathlib import Path

import pytest

from okg.substrate.chat.deployment_read_protocol import DeploymentReadProtocol
from okg.substrate.contracts.deployment_product.projections import (
    DeploymentReadStatus,
    DeploymentReadStatusV1,
    ReadEvidenceReference,
    ReadExtensionReference,
    ReadMaterializationStatus,
    ReadPolicySummary,
    ReadProvenanceReference,
    assert_projection_conformance,
    build_readback_projection_authority,
    public_extension_references,
    upgrade_deployment_read_status,
)
from okg.substrate.extension_broker import plan_extension_package


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "projections"
EXTENSION_FIXTURE = FIXTURES.parent / "external_extension"


def _journal(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "deployment": "acme-prod",
                "operation_id": "up-existing-operation",
                "lifecycle_plan_admission": {"plan_digest": "sha256:" + "1" * 64},
                "active_phase": None,
                "next_phase": "readiness",
                "completed": False,
                "phase_statuses": {
                    "preflight": "completed",
                    "readiness": "not_started",
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _authority():
    asset_digest = "sha256:" + "2" * 64
    return build_readback_projection_authority(
        operation_id="up-existing-operation",
        deployment_id="acme-prod",
        plan_digest="sha256:" + "1" * 64,
        evidence=(
            ReadEvidenceReference(
                evidence_kind="graph",
                reference="generation-41",
                status="stale",
                generation_id="generation-41",
            ),
            ReadEvidenceReference(
                evidence_kind="runtime",
                reference="up-existing-operation",
                status="current",
            ),
            ReadEvidenceReference(
                evidence_kind="materialization",
                reference="view-customer-summary",
                status="published",
                generation_id="generation-40",
            ),
        ),
        materializations=(
            ReadMaterializationStatus(
                asset_id="view-customer-summary",
                asset_digest=asset_digest,
                generation_id="generation-40",
                status="published",
                lineage_reference="lineage-customer-summary",
            ),
        ),
        provenance=(
            ReadProvenanceReference(
                owner="distribution",
                asset_id="view-customer-summary",
                asset_digest=asset_digest,
                source_reference="bundle-analytics",
            ),
        ),
        policy=ReadPolicySummary(
            audience_ids=("operator",),
            tenant_ids=("tenant-a",),
            visible_fields=("display-name",),
            masked_fields=("contact-email",),
            decision_ids=("policy-7",),
        ),
    )


def test_discover_inspect_read_poll_reconnects_without_prompt_or_new_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = _journal(tmp_path / "upgrade-state.json")
    before = state_file.read_bytes()

    def refuse_prompt(*_args, **_kwargs):
        raise AssertionError("typed read protocol attempted to prompt")

    monkeypatch.setattr(builtins, "input", refuse_prompt)
    protocol = DeploymentReadProtocol(state_file, authority=_authority())
    record = protocol.self_play()
    reconnected = DeploymentReadProtocol(state_file, authority=_authority()).read()

    assert [event.step for event in record.events] == [
        "discover",
        "inspect",
        "read",
        "poll",
    ]
    assert {event.operation_id for event in record.events} == {"up-existing-operation"}
    assert {event.plan_digest for event in record.events} == {"sha256:" + "1" * 64}
    assert reconnected.operation_id == "up-existing-operation"
    assert reconnected.plan_digest == "sha256:" + "1" * 64
    assert state_file.read_bytes() == before


def test_public_extension_discovery_is_metadata_only_and_never_imports_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse_import(*_args, **_kwargs):
        raise AssertionError("extension discovery imported executable code")

    monkeypatch.setattr(importlib, "import_module", refuse_import)
    plan = plan_extension_package(EXTENSION_FIXTURE)

    references = public_extension_references(plan)
    assert references[0].extension_ref.endswith("#fixture.tool")
    assert "fixture_tool:run" not in json.dumps(references[0].model_dump(mode="json"))
    with pytest.raises(ValueError):
        ReadExtensionReference(
            extension_ref="customer-tools@1.0.0#customer-resolver",
            kind="resolver",
            capabilities=("graph-read",),
            requirements_digest="sha256:" + "3" * 64,
            package_digest="sha256:" + "4" * 64,
            entrypoint="hostile.module:execute",
        )
    with pytest.raises(ValueError, match="stable reference"):
        ReadProvenanceReference(
            owner="distribution",
            asset_id="view-customer-summary",
            asset_digest="sha256:" + "2" * 64,
            source_reference="postgres://user:pass@host/db",
        )


def test_uncertain_effectful_journal_fails_closed_and_is_not_retryable(tmp_path: Path) -> None:
    state_file = _journal(tmp_path / "upgrade-state.json")
    journal = json.loads(state_file.read_text(encoding="utf-8"))
    journal["status"] = "uncertain"
    journal["active_phase"] = None
    journal["phase_attempts"] = {
        "attempt-1": {
            "status": "uncertain",
            "effect_may_have_happened": True,
        }
    }
    state_file.write_text(json.dumps(journal), encoding="utf-8")

    status = DeploymentReadProtocol(state_file).read()

    assert status.state == "blocked"
    assert status.safe_to_retry is False
    assert status.blockers[0].code == "operation-uncertain"
    assert {item.evidence_kind for item in status.evidence} == {
        "graph",
        "materialization",
        "runtime",
    }


def test_completed_journal_cannot_override_uncertain_effect_or_failure(tmp_path: Path) -> None:
    state_file = _journal(tmp_path / "upgrade-state.json")
    journal = json.loads(state_file.read_text(encoding="utf-8"))
    journal.update({
        "completed": True,
        "status": "failed",
        "failed_phase": "publish",
        "phase_attempts": {
            "attempt-1": {
                "status": "uncertain",
                "effect_may_have_happened": True,
            }
        },
    })
    state_file.write_text(json.dumps(journal), encoding="utf-8")

    status = DeploymentReadProtocol(state_file).read()

    assert status.state == "blocked"
    assert status.safe_to_retry is False
    assert status.blockers[0].code == "operation-uncertain"


def test_readback_authority_cannot_replay_across_operation_or_plan(tmp_path: Path) -> None:
    state_file = _journal(tmp_path / "upgrade-state.json")
    replayed = build_readback_projection_authority(
        operation_id="up-different-operation",
        deployment_id="acme-prod",
        plan_digest="sha256:" + "9" * 64,
    )

    with pytest.raises(ValueError, match="does not match"):
        DeploymentReadProtocol(state_file, authority=replayed).read()


def test_current_and_immediately_prior_read_status_fixtures_conform() -> None:
    prior = DeploymentReadStatusV1.model_validate_json(
        (FIXTURES / "read_status_v1.json").read_text(encoding="utf-8")
    )
    current = DeploymentReadStatus.model_validate_json(
        (FIXTURES / "read_status_v2.json").read_text(encoding="utf-8")
    )
    upgraded = upgrade_deployment_read_status(prior)

    assert upgraded == current
    assert upgraded.schema_version == "okg.deployment_product.read_status/v2"
    assert_projection_conformance(FIXTURES / "conformance.json")
