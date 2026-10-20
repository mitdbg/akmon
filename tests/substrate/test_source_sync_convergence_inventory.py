from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.deployments import resolve_source_registry_path
from okg.substrate.source_sync_convergence import (
    SOURCE_SYNC_INVENTORY_SCHEMA,
    build_convergence_inventory,
    readiness_source_groups_for_deployment_dir,
)
from tests.mirror_scope import is_public_copy
from tests.private_deployments import name


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_inventory_enumerates_checked_in_deployments_and_shared_registries(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.inventory "
        "test.source-sync-convergence.inventory",
    )
    report = build_convergence_inventory(
        repo_root=REPO_ROOT,
        all_deployments=True,
    )

    deployment_names = {
        path.parent.name
        for path in (REPO_ROOT / "deployments").glob("*/deployment.yaml")
    }
    by_name = {row["deployment"]: row for row in report["deployments"]}

    assert report["schema"] == SOURCE_SYNC_INVENTORY_SCHEMA
    assert set(by_name) == deployment_names
    assert report["deployment_count"] == len(deployment_names)
    assert report["source_count"] == sum(
        row["source_count"] for row in report["deployments"]
    )

    # okg-workspace is this repository's own deployment. In the public copy it
    # is absent; there must still be something to enumerate.
    if is_public_copy():
        assert deployment_names, "the public copy ships no deployment to enumerate"
        return
    workspace = by_name["okg-workspace"]
    workspace_qa = by_name.get("okg-workspace-qa")
    if workspace_qa is not None:
        assert workspace_qa["source_registry_path"] == workspace[
            "source_registry_path"
        ]
        assert workspace_qa["source_count"] == workspace["source_count"]


def test_inventory_classifies_runtime_exposure_and_manual_gates(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.source-sync-convergence.inventory "
        "test.source-sync-convergence.inventory",
    )
    report = build_convergence_inventory(
        repo_root=REPO_ROOT,
        all_deployments=True,
    )
    by_name = {row["deployment"]: row for row in report["deployments"]}

    expected_exposed = {
        "okg-workspace",
        "synth-bench",
    }
    if "okg-workspace-qa" in by_name:
        expected_exposed.add("okg-workspace-qa")
    assert set(report["exposed_deployments"]) >= expected_exposed
    workspace = by_name["okg-workspace"]
    assert workspace["runtime"]["enabled"] is True
    assert set(workspace["exposure"]["sources"]) >= {
        "agent-sessions",
        "codex-sessions",
        "git-files",
        "pact",
    }
    pact = next(
        source for source in workspace["sources"]
        if source["source_name"] == "pact"
    )
    assert pact["freshness_policy"] == {
        "policy": "completed_scope_watermark",
        "readiness_role": "agent_context_required",
        "readiness_profiles": ["dogfood"],
        "readiness_required": True,
        "completed_scope_required_for_readiness": True,
        "progress_freshness_required_for_readiness": False,
        "completed_scope_capable": True,
    }
    assert pact["sync"]["default_event_mode"] == "scope_incomplete"
    assert pact["sync"]["reconcile_run_mode"] == "scope_complete"
    assert pact["exposed_to_stale_watermark_gap"] is True

    commons = by_name[name("retired_prototype")]
    gated = [
        source for source in commons["sources"]
        if source["source_class"] == "registry_only"
    ]
    assert gated
    assert all(source["constraints"]["manual_required"] for source in gated)
    assert commons["exposure"]["reason"] == "runtime_disabled"


def test_okg_ci_inventory_uses_ci_readiness_profile(
    record_property,
) -> None:
    record_property(
        "requirements",
        "req.repair-okg-ci-source-health.profile-authority "
        "test.repair-okg-ci-source-health.profile-authority",
    )
    report = build_convergence_inventory(
        repo_root=REPO_ROOT,
        deployment="okg-ci",
    )
    okg_ci = report["deployments"][0]
    by_source = {row["source_name"]: row for row in okg_ci["sources"]}

    assert okg_ci["readiness_profiles"] == ["ci"]
    assert okg_ci["readiness"]["groups"] == {
        "repo": ["git-history", "github-issues", "pact"],
        "test_artifacts": ["test-reports"],
        "dependencies": ["package-manifests"],
    }
    assert set(okg_ci["readiness"]["present_required_sources"]) == {
        "okg-ci.git-history",
        "okg-ci.github-issues",
        "okg-ci.pact",
        "okg-ci.test-reports",
        "okg-ci.package-manifests",
    }
    test_reports_policy = by_source["test-reports"]["freshness_policy"]
    assert test_reports_policy["policy"] == "source_progress"
    assert test_reports_policy["readiness_profiles"] == ["ci"]
    assert test_reports_policy["readiness_required"] is True
    assert (
        test_reports_policy["completed_scope_required_for_readiness"]
        is False
    )
    assert (
        test_reports_policy["progress_freshness_required_for_readiness"]
        is True
    )
    assert by_source["test-reports"]["exposed_to_stale_watermark_gap"] is False


def test_okg_ci_dev_inventory_honors_fail_closed_source_override() -> None:
    report = build_convergence_inventory(
        repo_root=REPO_ROOT,
        deployment="okg-ci-dev",
    )
    staging = report["deployments"][0]
    by_source = {row["source_name"]: row for row in staging["sources"]}

    assert staging["readiness"]["groups"] == {
        "repo": ["git-history", "pact"],
        "test_artifacts": ["test-reports"],
        "dependencies": ["package-manifests"],
    }
    assert (
        by_source["github-issues"]["freshness_policy"]["readiness_required"]
        is False
    )
    assert (
        by_source["git-history"]["freshness_policy"]["readiness_required"]
        is True
    )
    assert by_source["pact"]["freshness_policy"]["readiness_required"] is True


def test_readiness_override_cannot_weaken_production(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        """name: prod
release: {channel: dev}
readiness:
  source_groups:
    ci:
      repo: [git-history, pact]
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="staging_fail_closed"):
        readiness_source_groups_for_deployment_dir(deployment, ("ci",))


def test_readiness_override_cannot_omit_local_source_on_dev(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        """name: dev
release: {channel: dev}
source_registry: source_registry.yaml
nomos:
  source_policy_defaults:
    by_source_class:
      mutable_api: {live_call_allowed: false}
readiness:
  override_authority: staging_fail_closed
  source_groups:
    ci:
      repo: [github-issues, pact]
      test_artifacts: [test-reports]
      dependencies: [package-manifests]
""",
        encoding="utf-8",
    )
    (deployment / "source_registry.yaml").write_text(
        """sources:
  git-history: {source_class: discovery_crawl}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid omissions: git-history"):
        readiness_source_groups_for_deployment_dir(deployment, ("ci",))


def test_manifest_relative_source_registry_resolves_for_thin_overlay(
    tmp_path: Path,
) -> None:
    shared = tmp_path / "shared"
    overlay = tmp_path / "overlay"
    shared.mkdir()
    overlay.mkdir()
    registry = shared / "source_registry.yaml"
    registry.write_text("sources: {}\n", encoding="utf-8")
    (overlay / "deployment.yaml").write_text(
        "source_registry: ../shared/source_registry.yaml\n",
        encoding="utf-8",
    )

    assert resolve_source_registry_path(overlay).resolve() == registry.resolve()


def test_source_registry_resolution_preserves_manifestless_default(
    tmp_path: Path,
) -> None:
    deployment = tmp_path / "deployment"
    deployment.mkdir()

    assert resolve_source_registry_path(deployment) == (
        deployment / "source_registry.yaml"
    )
