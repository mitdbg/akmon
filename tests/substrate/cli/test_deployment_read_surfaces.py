from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli.deployment import _cmd_deployment
from okg.substrate.cli.deployment_read_surfaces import (
    deployment_read_agent_payload,
    deployment_read_human_text,
)
from okg.substrate.cli.main import _build_parser
from okg.substrate.contracts.deployment_product.projections import (
    ReadPolicySummary,
    ReadProvenanceReference,
    build_readback_projection_authority,
)


def test_human_and_agent_surfaces_derive_from_one_model_and_keep_advanced_ids(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "upgrade-state.json"
    state_file.write_text(
        json.dumps(
            {
                "deployment": "acme-prod",
                "operation_id": "up-existing-operation",
                "lifecycle_plan_admission": {"plan_digest": "sha256:" + "1" * 64},
                "active_phase": "readiness",
                "completed": False,
                "phase_statuses": {"preflight": "completed", "readiness": "running"},
            }
        ),
        encoding="utf-8",
    )
    provenance = (
        ReadProvenanceReference(
            owner="distribution",
            asset_id="view-customer-summary",
            asset_digest="sha256:" + "2" * 64,
            source_reference="bundle-analytics",
        ),
    )
    policy = ReadPolicySummary(
        audience_ids=("operator",),
        tenant_ids=("tenant-a",),
        visible_fields=("display-name",),
        masked_fields=("contact-email",),
        decision_ids=("policy-7",),
    )

    authority = build_readback_projection_authority(
        operation_id="up-existing-operation",
        deployment_id="acme-prod",
        plan_digest="sha256:" + "1" * 64,
        provenance=provenance,
        policy=policy,
    )
    agent = deployment_read_agent_payload(state_file, authority=authority)
    human = deployment_read_human_text(state_file, authority=authority)

    assert agent["operation_id"] == "up-existing-operation"
    assert agent["plan_digest"] == "sha256:" + "1" * 64
    assert agent["state"] == "running"
    assert agent["provenance"][0]["asset_digest"] in human
    assert "policy-7" in human
    assert "Plan digest: " + agent["plan_digest"] in human
    assert "next action poll-operation-status" in human


def test_registered_cli_command_loads_digest_bound_readback_sidecar(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_file = tmp_path / "acme-prod-prod.json"
    state_file.write_text(
        json.dumps({
            "deployment": "acme-prod",
            "operation_id": "up-public-command",
            "lifecycle_plan_admission": {"plan_digest": "sha256:" + "4" * 64},
            "active_phase": None,
            "completed": False,
            "phase_statuses": {},
        }),
        encoding="utf-8",
    )
    authority = build_readback_projection_authority(
        operation_id="up-public-command",
        deployment_id="acme-prod",
        plan_digest="sha256:" + "4" * 64,
        provenance=(ReadProvenanceReference(
            owner="distribution",
            asset_id="view-public",
            asset_digest="sha256:" + "5" * 64,
            source_reference="bundle-public",
        ),),
        policy=ReadPolicySummary(
            audience_ids=("operator",),
            tenant_ids=("tenant-a",),
            visible_fields=("display-name",),
            masked_fields=("contact-email",),
            decision_ids=("policy-public",),
        ),
    )
    state_file.with_suffix(".readback.json").write_text(
        authority.model_dump_json(),
        encoding="utf-8",
    )
    args = _build_parser().parse_args([
        "deployment", "read-status", "acme-prod", "--target", "prod",
        "--state-file", str(state_file), "--json",
    ])

    assert _cmd_deployment(args, as_json=True) == 0
    output = json.loads(capsys.readouterr().out)
    payload = output.get("data", output)
    assert payload["operation_id"] == "up-public-command"
    assert payload["provenance"][0]["asset_id"] == "view-public"
    assert payload["policy"]["masked_fields"] == ["contact-email"]
