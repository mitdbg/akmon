from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.deployment import (
    _nomos_required_deployment_names,
    _nomos_source_names,
    _nomos_source_lineage_rollout_entry,
    _resolve_external_deployment_dir,
)
from okg.substrate.deployment_upgrade import (
    _nomos_policy_source_ids,
    _nomos_source_policies,
)
from okg.substrate.cli.main import main
from okg.substrate.errors import ExternalDeploymentsDirNotConfigured
from tests.substrate.cli.test_static_deployment_readiness_contract import (
    _write_exact_policy_deployment,
)


def test_rollout_audit_uses_composed_exact_policy_fingerprint(
    tmp_path: Path,
) -> None:
    from okg.substrate.nomos.baseline import deployment_baseline_spec

    deployment_dir = _write_exact_policy_deployment(tmp_path)
    manifest_path = deployment_dir / "deployment.yaml"
    registry_path = deployment_dir / "source_registry.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    spec = deployment_baseline_spec(
        deployment="exact-policy",
        deployment_dir=deployment_dir,
    )

    entry = _nomos_source_lineage_rollout_entry(
        name="exact-policy",
        kind="deployment",
        origin="test",
        path=manifest_path,
        manifest=manifest,
        source_registry=registry,
        source_registry_path=registry_path,
    )

    assert entry["status"] == "pass"
    assert entry["lineage_policy"]["expected_policy_fingerprint"] == (
        spec["policy_fingerprint"]
    )
    assert entry["lineage_policy"]["baseline_error"] is None
    rules = spec["policy_graph"]["rules"]
    assert len(rules) == 30
    assert sum(rule["rule_id"].startswith("exact_") for rule in rules) == 18
    rule_ids = {rule["rule_id"] for rule in rules}
    assert "rollout_allow_registered_source_runs" not in rule_ids
    assert "rollout_allow_manifest_local_operator_boundary" in rule_ids


def _write_manifest(root: Path, name: str, manifest: dict) -> Path:
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": name, **manifest}),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    return deployment_dir


def _posture(deployment_class: str) -> dict:
    class_overrides = {
        "ci_authority": {
            "read_principals": ["ci_runner", "okg_service"],
            "write_principals": ["ci_runner", "okg_service"],
        },
        "dogfood_workspace": {
            "allowed_source_sensitivities": ["internal", "local_private"],
        },
        "personal_okg": {
            "export_policy": "no_export",
            "allowed_source_sensitivities": ["private", "local_private"],
            "allowed_data_classifications": ["private"],
            "allowed_exportability": ["no_export"],
            "privacy_obligations": ["audit", "no_export", "secret_ref_required"],
        },
        "public_reference_demo": {
            "export_policy": "public_allowed",
            "secret_handling": "no_secrets",
            "live_call_default": "deny",
            "allowed_source_sensitivities": ["public"],
            "allowed_data_classifications": ["public"],
            "allowed_exportability": ["public"],
            "privacy_obligations": ["audit"],
        },
        "synthetic_healthcare_demo": {
            "allowed_source_sensitivities": ["public", "internal", "phi_synthetic"],
            "allowed_data_classifications": ["public", "internal", "synthetic_phi"],
            "privacy_obligations": ["audit", "redaction"],
        },
        "governed_research_metadata": {
            "allowed_source_sensitivities": ["internal", "regulated_metadata"],
            "allowed_data_classifications": ["internal", "regulated_metadata"],
            "privacy_obligations": ["audit", "source_retention"],
        },
        "org_operational_private": {
            "allowed_source_sensitivities": ["internal", "private"],
            "allowed_data_classifications": ["internal", "private"],
        },
        "private_product_engineering": {
            "allowed_source_sensitivities": ["credentialed_external"],
            "allowed_data_classifications": ["restricted"],
            "privacy_obligations": ["audit", "secret_ref_required"],
        },
    }
    posture = {
        "deployment_class": deployment_class,
        "read_principals": ["human_operator", "okg_service"],
        "write_principals": ["okg_service"],
        "mcp_write_default": "deny",
        "export_policy": "internal_only",
        "secret_handling": "secret_ref_required",
        "live_call_default": "review",
        "allowed_source_sensitivities": ["internal", "private"],
        "allowed_data_classifications": ["internal", "private"],
        "allowed_exportability": ["internal", "no_export"],
        "privacy_obligations": ["audit", "secret_ref_required"],
        "readiness_profiles": ["dogfood"],
    }
    posture.update(class_overrides.get(deployment_class, {}))
    return {
        "runtime_enforcement": "enforce",
        "deployment_posture": posture,
        "source_policy_defaults": {
            "by_source_class": {
                "discovery_crawl": {
                    "sensitivity": posture["allowed_source_sensitivities"][0],
                    "data_classification": (
                        posture["allowed_data_classifications"][0]
                    ),
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": posture["allowed_exportability"][0],
                    "retention": "normal",
                    "provenance": "local_filesystem",
                    "privacy_obligations": posture["privacy_obligations"],
                },
            },
        },
    }


def test_nomos_source_names_include_registry_key_and_ownership_id() -> None:
    assert _nomos_source_names({
        "sources": {
            "github-actions": {"ownership_id": "okg-ci.github-actions"},
            "test-reports": {"ownership_id": "okg-ci.test-reports"},
            "registry-only": {},
        },
    }) == [
        "github-actions",
        "okg-ci.github-actions",
        "okg-ci.test-reports",
        "registry-only",
        "test-reports",
    ]


def test_nomos_policy_source_ids_match_runtime_lineage_aliases() -> None:
    assert _nomos_policy_source_ids(
        {
            "sources": {
                "pact": {"ownership_id": "okg-ci.pact"},
                "github-issues": {
                    "ownership_id": "okg-ci.github-issues",
                    "admission_policy": {
                        "producer_id": "producer:github-issues",
                    },
                },
            },
        },
        deployment="okg-ci-dev",
    ) == [
        "_catalog:restamp",
        "github-issues",
        "okg-ci-dev.github-issues",
        "okg-ci-dev.pact",
        "okg-ci.github-issues",
        "okg-ci.pact",
        "pact",
        "producer:github-issues",
    ]


def test_nomos_policy_source_ids_include_external_backed_collector_for_monitored_hosts(
    tmp_path: Path,
) -> None:
    (tmp_path / "monitored_hosts.yaml").write_text("hosts: []\n", encoding="utf-8")

    assert "system:external_backed_collector" in _nomos_policy_source_ids(
        {"sources": {}},
        deployment="okg-workspace",
        deployment_dir=tmp_path,
    )


def test_nomos_policy_source_ids_omit_external_backed_collector_without_monitored_hosts(
    tmp_path: Path,
) -> None:
    assert "system:external_backed_collector" not in _nomos_policy_source_ids(
        {"sources": {}},
        deployment="okg-workspace",
        deployment_dir=tmp_path,
    )


def test_nomos_policy_source_ids_include_native_view_producer_for_views_registry(
    tmp_path: Path,
) -> None:
    (tmp_path / "views_registry.yaml").write_text("views: []\n", encoding="utf-8")

    source_ids = _nomos_policy_source_ids(
        {"sources": {}},
        deployment="okg-workspace",
        deployment_dir=tmp_path,
    )

    assert "catalog:views_registry" in source_ids
    assert "_system:native_views" in source_ids


def test_nomos_policy_source_ids_include_configured_derived_producers(
    tmp_path: Path,
) -> None:
    (tmp_path / "deployment.yaml").write_text(
        """
enrichers:
  - class: okg.substrate.library.linkers.declarative.DeclarativeLinker
    config:
      name: release_links
      include_rule_ids:
        - agent_message_mentions_pact_change_dir
  - class: okg.substrate.library.linkers.tool_use_symbol.ToolUseSymbolLinker
  - class: okg.substrate.library.linkers.code_reference.CodeReferenceResolver
derived_producers:
  extractors:
    change_slug:
      trust_label: derived-deterministic-trusted
""".lstrip(),
        encoding="utf-8",
    )

    source_ids = _nomos_policy_source_ids(
        {"sources": {}},
        deployment="okg-workspace",
        deployment_dir=tmp_path,
    )

    assert "_enricher:code_reference" in source_ids
    assert "_enricher:tool_use_symbol" in source_ids
    assert "_enricher:release_links" in source_ids
    assert (
        "_enricher:release_links:agent_message_mentions_pact_change_dir"
        in source_ids
    )
    assert "_extractor:change_slug" in source_ids


def test_nomos_policy_source_ids_omit_unconfigured_derived_producers(
    tmp_path: Path,
) -> None:
    (tmp_path / "deployment.yaml").write_text("{}\n", encoding="utf-8")

    source_ids = _nomos_policy_source_ids(
        {"sources": {}},
        deployment="okg-workspace",
        deployment_dir=tmp_path,
    )

    assert not any(
        source_id.startswith(("_enricher:", "_extractor:"))
        for source_id in source_ids
    )


def test_nomos_source_policies_include_ownership_alias() -> None:
    policies = _nomos_source_policies(
        {
            "sources": {
                "github-actions": {"ownership_id": "okg-ci.github-actions"},
                "test-reports": {"ownership_id": "okg-ci.test-reports"},
            },
        },
        {
            "github-actions": {"live_call_allowed": True},
            "test-reports": {"live_call_allowed": False},
        },
    )

    assert policies["okg-ci.github-actions"] == {"live_call_allowed": True}
    assert policies["okg-ci.test-reports"] == {"live_call_allowed": False}


def test_nomos_rollout_audit_reports_missing_external_posture(
    tmp_path: Path,
    capsys,
) -> None:
    repo = tmp_path / "repo-deployments"
    external = tmp_path / "okg-deployments"
    profiles = tmp_path / "profiles"
    _write_manifest(repo, "okg-workspace", {"nomos": _posture("dogfood_workspace")})
    _write_manifest(repo, "okg-ci", {"nomos": _posture("ci_authority")})
    _write_manifest(external, "cms", {})
    profile_dir = profiles / "academic-researcher"
    profile_dir.mkdir(parents=True)
    (profile_dir / "deployment-defaults.yaml").write_text(
        yaml.safe_dump({"nomos": _posture("personal_okg")}),
        encoding="utf-8",
    )

    rc = main([
        "deployment",
        "nomos-rollout-audit",
        "--repo-deployment-dir",
        str(repo),
        "--deployment-dir",
        str(external),
        "--profile-dir",
        str(profiles),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    cms = next(entry for entry in payload["entries"] if entry["name"] == "cms")
    assert cms["status"] == "failed"
    assert cms["findings"][0]["reason_code"] == (
        "nomos.rollout.deployment_posture_missing"
    )


def test_nomos_rollout_audit_writes_complete_inventory(
    tmp_path: Path,
    capsys,
) -> None:
    repo = tmp_path / "repo-deployments"
    external = tmp_path / "okg-deployments"
    profiles = tmp_path / "profiles"
    _write_manifest(repo, "okg-workspace", {"nomos": _posture("dogfood_workspace")})
    _write_manifest(repo, "okg-ci", {"nomos": _posture("ci_authority")})
    _write_manifest(external, "af_pubs", {"nomos": _posture("public_reference_demo")})
    _write_manifest(
        external,
        "intersystems",
        {"nomos": _posture("synthetic_healthcare_demo")},
    )
    _write_manifest(
        external,
        "statgen-analysis",
        {"nomos": _posture("governed_research_metadata")},
    )
    _write_manifest(external, "cms", {"nomos": _posture("org_operational_private")})
    _write_manifest(external, "memex", {"nomos": _posture("personal_okg")})
    _write_manifest(
        external,
        "kdflex",
        {"nomos": _posture("private_product_engineering")},
    )
    profile_dir = profiles / "academic-researcher"
    profile_dir.mkdir(parents=True)
    (profile_dir / "deployment-defaults.yaml").write_text(
        yaml.safe_dump({"nomos": _posture("personal_okg")}),
        encoding="utf-8",
    )
    out = tmp_path / "inventory.json"

    rc = main([
        "deployment",
        "nomos-rollout-audit",
        "--repo-deployment-dir",
        str(repo),
        "--deployment-dir",
        str(external),
        "--profile-dir",
        str(profiles),
        "--out",
        str(out),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    written = json.loads(out.read_text(encoding="utf-8"))
    assert rc == 0
    assert payload["ok"] is True
    assert written["ok"] is True
    assert written["required_missing"] == []
    classes = {entry["deployment_class"] for entry in written["entries"]}
    assert {
        "ci_authority",
        "dogfood_workspace",
        "personal_okg",
        "public_reference_demo",
        "synthetic_healthcare_demo",
        "governed_research_metadata",
        "org_operational_private",
        "private_product_engineering",
    }.issubset(classes)


def test_nomos_rollout_audit_allows_external_main_without_kdflex(
    tmp_path: Path,
    capsys,
) -> None:
    repo = tmp_path / "repo-deployments"
    external = tmp_path / "okg-deployments"
    profiles = tmp_path / "profiles"
    _write_manifest(repo, "okg-workspace", {"nomos": _posture("dogfood_workspace")})
    _write_manifest(repo, "okg-ci", {"nomos": _posture("ci_authority")})
    _write_manifest(external, "af_pubs", {"nomos": _posture("public_reference_demo")})
    _write_manifest(
        external,
        "intersystems",
        {"nomos": _posture("synthetic_healthcare_demo")},
    )
    _write_manifest(
        external,
        "statgen-analysis",
        {"nomos": _posture("governed_research_metadata")},
    )
    _write_manifest(external, "cms", {"nomos": _posture("org_operational_private")})
    _write_manifest(external, "memex", {"nomos": _posture("personal_okg")})
    profile_dir = profiles / "academic-researcher"
    profile_dir.mkdir(parents=True)
    (profile_dir / "deployment-defaults.yaml").write_text(
        yaml.safe_dump({"nomos": _posture("personal_okg")}),
        encoding="utf-8",
    )

    rc = main([
        "deployment",
        "nomos-rollout-audit",
        "--repo-deployment-dir",
        str(repo),
        "--deployment-dir",
        str(external),
        "--profile-dir",
        str(profiles),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["ok"] is True
    assert payload["required_missing"] == []
    assert {entry["name"] for entry in payload["entries"]} >= {
        "okg-ci",
        "okg-workspace",
        "af_pubs",
        "intersystems",
        "statgen-analysis",
        "cms",
        "memex",
    }


def test_resolve_external_dir_fails_loudly_when_unconfigured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No operator-specific default: with no --deployment-dir, no
    OKG_DEPLOYMENTS_DIR, and no fetched cache, resolution must raise a
    structured error rather than silently pointing at a per-machine path."""
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    # Point the cache root at an empty temp dir so no cache slot exists.
    monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg-cache"))
    with pytest.raises(ExternalDeploymentsDirNotConfigured) as exc:
        _resolve_external_deployment_dir(None)
    assert exc.value.code == "external_deployments_dir_not_configured"
    assert "checked" in exc.value.data


def test_resolve_external_dir_prefers_explicit_then_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    explicit = tmp_path / "explicit"
    env_dir = tmp_path / "env"
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(env_dir))
    assert _resolve_external_deployment_dir(str(explicit)) == explicit
    assert _resolve_external_deployment_dir(None) == env_dir


def test_nomos_audit_reports_loud_error_when_external_unconfigured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The CLI surfaces the loud unconfigured-external-repo error instead of
    falling back to a hardcoded default path."""
    repo = tmp_path / "repo-deployments"
    profiles = tmp_path / "profiles"
    _write_manifest(repo, "okg-workspace", {"nomos": _posture("dogfood_workspace")})
    _write_manifest(repo, "okg-ci", {"nomos": _posture("ci_authority")})
    profiles.mkdir(parents=True)
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.setenv("OKG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg-cache"))

    rc = main([
        "deployment",
        "nomos-rollout-audit",
        "--repo-deployment-dir",
        str(repo),
        "--profile-dir",
        str(profiles),
        "--json",
    ])

    # Structured error envelopes are printed to stderr.
    payload = json.loads(capsys.readouterr().err)
    assert rc != 0
    assert payload["error"]["code"] == "external_deployments_dir_not_configured"


def _fleet_config(root: Path, body: str) -> Path:
    (root / ".okg").mkdir(parents=True, exist_ok=True)
    (root / ".okg" / "coordination.yaml").write_text(body, encoding="utf-8")
    return root


def test_required_deployment_set_is_single_source_of_truth(tmp_path: Path) -> None:
    """Both audits derive their required set from one helper, so they agree.

    `_nomos_required_deployment_names` is the single source. The names are
    repository configuration (`deployment_fleet`), not package constants
    (#2823); a `..._when_present` name is required only when the scanned
    inventory has it."""
    root = _fleet_config(
        tmp_path,
        "deployment_fleet:\n"
        "  nomos_required: [alpha, beta]\n"
        "  nomos_required_when_present: [gamma]\n",
    )
    base = _nomos_required_deployment_names([], start=root)
    assert base == {"alpha", "beta"}
    with_gamma = _nomos_required_deployment_names([{"name": "gamma"}], start=root)
    assert with_gamma == base | {"gamma"}


def test_required_deployment_set_is_empty_and_says_so_without_configuration(
    tmp_path: Path,
) -> None:
    """A checkout that configures no fleet (the public copy) requires none."""
    from okg.substrate.cli.nomos_rollout_audit import _nomos_required_source

    assert _nomos_required_deployment_names([{"name": "gamma"}], start=tmp_path) == set()
    assert _nomos_required_source(tmp_path).startswith("none configured")
    root = _fleet_config(tmp_path, "deployment_fleet:\n  nomos_required: [alpha]\n")
    assert _nomos_required_source(root) == str(root / ".okg" / "coordination.yaml")
