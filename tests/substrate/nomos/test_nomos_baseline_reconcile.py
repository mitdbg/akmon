from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.baseline import (
    deployment_baseline_spec,
    reconcile_deployment_baseline,
)
from okg.substrate.nomos.core import DecisionInput, compile_policy_graph, evaluate
from okg.substrate.nomos.store import deployment_baseline_policy_graph


def _disabled_deployment(tmp_path: Path) -> Path:
    root = tmp_path / "nomos-disabled"
    root.mkdir()
    (root / "deployment.yaml").write_text(
        "name: nomos-disabled\nmodules: []\nsource_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    (root / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    return root


def _source_bound_deployment(tmp_path: Path) -> Path:
    root = tmp_path / "source-bound"
    root.mkdir()
    policy_path = root / "source-bindings.policy.json"
    manifest = {
        "name": "source-bound",
        "modules": [],
        "source_registry": "source_registry.yaml",
        "nomos": {
            "runtime_enforcement": "audit",
            "source_binding_policy": policy_path.name,
            "deployment_posture": {
                "deployment_class": "org_operational_private",
                "read_principals": ["human_operator"],
                "write_principals": ["source_adapter"],
                "mcp_write_default": "deny",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "deny",
                "allowed_source_sensitivities": ["private"],
                "allowed_data_classifications": ["private"],
                "allowed_exportability": ["internal"],
                "privacy_obligations": ["audit"],
                "readiness_profiles": ["org-private"],
            },
        },
    }
    source_policy = {
        "sensitivity": "private",
        "data_classification": "private",
        "credential_ref_policy": "none",
        "live_call_allowed": False,
        "exportability": "internal",
        "retention": "source_defined",
        "provenance": "local_filesystem",
        "privacy_obligations": ["audit"],
    }
    registry = {
        "sources": {
            "alpha": {
                "ownership_id": "source-bound.alpha",
                "admission_policy": {
                    "producer_id": "producer-profile.alpha",
                },
                "source_policy": source_policy,
            },
            "beta": {
                "ownership_id": "source-bound.beta",
                "admission_policy": {
                    "producer_id": "producer-profile.beta",
                },
                "source_policy": source_policy,
            },
        },
    }
    scope = "deployment:source-bound"
    rules = []
    for source in ("source-bound.alpha", "source-bound.beta"):
        for action, resource_kind in (
            ("source.ingest", "source"),
            ("source.emit", "node"),
        ):
            rules.append({
                "rule_id": f"allow_{source.replace('.', '_')}_{action.split('.')[1]}",
                "effect": "allow",
                "priority": 150,
                "subjects": [f"source:{source}"],
                "actions": [action],
                "match": {
                    "deployment": "source-bound",
                    "scope": scope,
                    "source": source,
                    "graph_branch": "default",
                    "resource_kind": resource_kind,
                    "classification": "internal",
                    "principal_kind": "source_adapter",
                },
                "reason_code": "nomos.deployment.source_binding_allow",
                "obligations": [{"type": "audit"}],
            })
    artifact = {
        "policy_bundle_id": "nomos:test:source-bindings",
        "policy_version": "v1",
        "default_effect": "deny",
        "vocabulary_strict": True,
        "scope": {"deployment": "source-bound", "scope": scope},
        "rules": rules,
    }
    (root / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    (root / "source_registry.yaml").write_text(yaml.safe_dump(registry))
    policy_path.write_text(json.dumps(artifact))
    return root


def _declare_source_binding_policy(root: Path, declared: str) -> None:
    manifest_path = root / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["nomos"]["source_binding_policy"] = declared
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")


def test_public_baseline_status_is_write_free_when_nomos_is_disabled(
    tmp_path: Path,
) -> None:
    root = _disabled_deployment(tmp_path)
    spec = deployment_baseline_spec(
        deployment="nomos-disabled", deployment_dir=root,
    )
    assert spec == {
        "ok": True,
        "required": False,
        "deployment": "nomos-disabled",
        "status": "skipped",
    }


def test_public_reconcile_does_not_connect_when_nomos_is_disabled(
    tmp_path: Path,
) -> None:
    root = _disabled_deployment(tmp_path)
    result = reconcile_deployment_baseline(
        deployment="nomos-disabled",
        deployment_dir=root,
        dsn="this is intentionally not a DSN",
        apply=True,
    )
    assert result["status"] == "skipped"


def test_workspace_baseline_covers_declared_derived_source_families() -> None:
    spec = deployment_baseline_spec(
        deployment="okg-workspace",
        deployment_dir=Path("deployments/okg-workspace"),
    )

    expected = {
        "_extractor:change_slug",
        "_enricher:code_reference",
        "_enricher:interface_handler",
    }
    assert expected.issubset(set(spec["source_names"]))

    policy = deployment_baseline_policy_graph(
        spec["manifest"],
        deployment_name="okg-workspace",
        scope="deployment:okg-workspace",
        activated_by="subject:test",
        source_names=spec["source_names"],
        source_policies=spec["source_policies"],
        runtime_source_names=spec["runtime_source_names"],
    )
    lineage_rule = next(
        rule
        for rule in policy["rules"]
        if rule["rule_id"] == "rollout_allow_source_lineage_filtered_reads"
    )
    assert expected.issubset(set(lineage_rule["match"]["source_ids"]))


def test_deployment_source_bindings_replace_generic_cross_source_allow(
    tmp_path: Path,
) -> None:
    root = _source_bound_deployment(tmp_path)
    spec = deployment_baseline_spec(
        deployment="source-bound",
        deployment_dir=root,
    )
    graph = spec["policy_graph"]
    rule_ids = {rule["rule_id"] for rule in graph["rules"]}
    assert "rollout_allow_registered_source_runs" not in rule_ids
    assert {
        "allow_source-bound_alpha_ingest",
        "allow_source-bound_alpha_emit",
        "allow_source-bound_beta_ingest",
        "allow_source-bound_beta_emit",
    } <= rule_ids

    bundle = compile_policy_graph(graph)

    def decide(*, subject: str, source: str, branch: str) -> str:
        return evaluate(
            bundle,
            DecisionInput(
                subject=subject,
                action="source.emit",
                resource={
                    "deployment": "source-bound",
                    "scope": "deployment:source-bound",
                    "source": source,
                    "graph_branch": branch,
                    "resource_kind": "node",
                },
                classification="internal",
                context={"principal_kind": "source_adapter"},
            ),
        ).effect

    assert decide(
        subject="source:source-bound.alpha",
        source="source-bound.alpha",
        branch="default",
    ) == "allow"
    assert decide(
        subject="source:producer-profile.alpha",
        source="source-bound.alpha",
        branch="default",
    ) == "deny"
    assert decide(
        subject="source:source-bound.alpha",
        source="source-bound.beta",
        branch="default",
    ) == "deny"
    assert decide(
        subject="source:source-bound.alpha",
        source="source-bound.alpha",
        branch="quarantine:review",
    ) == "deny"


@pytest.mark.parametrize(
    "unsafe_kind",
    ["absolute", "parent", "file_symlink", "directory_symlink", "directory"],
)
def test_deployment_source_binding_policy_refuses_unsafe_paths(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    root = _source_bound_deployment(tmp_path)
    original = root / "source-bindings.policy.json"
    policy_text = original.read_text(encoding="utf-8")
    outside = tmp_path / "outside.policy.json"
    outside.write_text(policy_text, encoding="utf-8")

    if unsafe_kind == "absolute":
        declared = str(outside.resolve())
    elif unsafe_kind == "parent":
        declared = "../outside.policy.json"
    elif unsafe_kind == "file_symlink":
        original.unlink()
        original.symlink_to(outside)
        declared = original.name
    elif unsafe_kind == "directory_symlink":
        outside_dir = tmp_path / "outside-policies"
        outside_dir.mkdir()
        (outside_dir / "bindings.json").write_text(policy_text, encoding="utf-8")
        (root / "linked-policies").symlink_to(outside_dir, target_is_directory=True)
        declared = "linked-policies/bindings.json"
    else:
        policy_dir = root / "policy-directory"
        policy_dir.mkdir()
        declared = policy_dir.name
    _declare_source_binding_policy(root, declared)

    with pytest.raises(SubstrateError, match="source.binding.policy|source-binding"):
        deployment_baseline_spec(
            deployment="source-bound",
            deployment_dir=root,
        )


@pytest.mark.parametrize(
    "invalid_kind",
    ["malformed_json", "wrong_default", "wrong_scope", "incomplete_coverage"],
)
def test_deployment_source_binding_policy_refuses_invalid_contracts(
    tmp_path: Path,
    invalid_kind: str,
) -> None:
    root = _source_bound_deployment(tmp_path)
    policy_path = root / "source-bindings.policy.json"
    if invalid_kind == "malformed_json":
        policy_path.write_text("{not-json", encoding="utf-8")
    else:
        artifact = json.loads(policy_path.read_text(encoding="utf-8"))
        if invalid_kind == "wrong_default":
            artifact["default_effect"] = "allow"
        elif invalid_kind == "wrong_scope":
            artifact["scope"]["scope"] = "deployment:other"
        else:
            artifact["rules"].pop()
        policy_path.write_text(json.dumps(artifact), encoding="utf-8")

    with pytest.raises(SubstrateError):
        deployment_baseline_spec(
            deployment="source-bound",
            deployment_dir=root,
        )
