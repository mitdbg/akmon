from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.deployment import (
    _apply_nomos_rollout_baseline,
    _nomos_audited_source_policies,
    _nomos_source_lineage_read_policy_source_ids,
    _nomos_source_lineage_rollout_state_from_runtime_reports,
)
from okg.substrate.cli.main import main
from okg.substrate.nomos import (
    NomosRuntimeRequest,
    compile_policy_graph,
    deployment_baseline_policy_graph,
    evaluate_runtime_boundary,
)
from tests.substrate.cli.test_nomos_rollout_audit import _posture


def test_rollout_apply_uses_public_deployment_policy_graph(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import okg.substrate.nomos as nomos
    import okg.substrate.nomos.baseline as baseline
    import psycopg

    deployment_dir = tmp_path / "source-bound"
    deployment_dir.mkdir()
    manifest_path = deployment_dir / "deployment.yaml"
    manifest_path.write_text("name: source-bound\n", encoding="utf-8")
    policy_graph = {
        "policy_bundle_id": "nomos:rollout:baseline:source-bound",
        "rules": [{"rule_id": "exact-source-bindings"}],
    }
    spec = {
        "required": True,
        "manifest": {"name": "source-bound"},
        "source_names": ["source-bound.alpha"],
        "source_policies": {"source-bound.alpha": {}},
        "runtime_source_names": ["source-bound.alpha"],
        "policy_graph": policy_graph,
    }
    captured: dict[str, object] = {}

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

        def commit(self) -> None:
            pass

    monkeypatch.setattr(baseline, "deployment_baseline_spec", lambda **kwargs: spec)
    monkeypatch.setattr(
        nomos,
        "seed_deployment_baseline_nomos_state",
        lambda _conn, **kwargs: captured.update(kwargs) or {"ok": True},
    )
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: Connection())

    result = _apply_nomos_rollout_baseline(
        deployment="source-bound",
        dsn="postgres://unused",
        inventory={
            "entries": [{
                "name": "source-bound",
                "kind": "deployment",
                "path": str(manifest_path),
            }],
        },
    )

    assert result == {"ok": True}
    assert captured["policy_graph"] is policy_graph


def test_baseline_policy_graph_scopes_read_rules_by_source() -> None:
    manifest = {"nomos": _posture("dogfood_workspace")}
    manifest["nomos"]["deployment_posture"]["read_principals"].append("mcp_client")
    bundle = compile_policy_graph(
        deployment_baseline_policy_graph(
            manifest,
            deployment_name="okg-workspace",
            scope="deployment:okg-workspace",
            activated_by="subject:rollout",
            source_names=["git-files", "google-drive"],
            source_policies={
                "git-files": {"live_call_allowed": False},
                "google-drive": {"live_call_allowed": False},
            },
            runtime_source_names=["git-files", "google-drive"],
        ),
    )

    allowed_source = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.read.mcp",
            subject="subject:mcp_client",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
                "source_id": "git-files",
                "source_ids": ["git-files"],
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    unlisted_source = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.read.mcp",
            subject="subject:mcp_client",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
                "source_id": "private-drive",
                "source_ids": ["private-drive"],
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    source_missing = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.read.mcp",
            subject="subject:mcp_client",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    boundary = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.read.mcp",
            subject="subject:mcp_client",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
                "resource_kind": "mcp_graph_read_boundary",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )

    assert allowed_source.allowed is True
    assert unlisted_source.allowed is False
    assert source_missing.allowed is False
    assert boundary.allowed is True


def test_source_lineage_rollout_audit_reports_missing_source_policy(
    tmp_path: Path,
    capsys,
) -> None:
    repo = tmp_path / "repo-deployments"
    external = tmp_path / "okg-deployments"
    profiles = tmp_path / "profiles"
    _write_deployment(repo, "okg-workspace", _posture("dogfood_workspace"))
    _write_deployment(repo, "okg-ci", _posture("ci_authority"))
    _write_deployment(external, "af_pubs", _posture("public_reference_demo"))
    _write_deployment(
        external,
        "intersystems",
        _posture("synthetic_healthcare_demo"),
    )
    _write_deployment(
        external,
        "statgen-analysis",
        _posture("governed_research_metadata"),
    )
    _write_deployment(
        external,
        "cms",
        _posture("org_operational_private"),
        source_class="unknown_class",
    )
    _write_profile(profiles, "academic-researcher", _posture("personal_okg"))

    rc = main([
        "deployment",
        "nomos-source-lineage-rollout-audit",
        "--repo-deployment-dir",
        str(repo),
        "--deployment-dir",
        str(external),
        "--profile-dir",
        str(profiles),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    cms = next(entry for entry in payload["entries"] if entry["name"] == "cms")
    codes = {finding["reason_code"] for finding in cms["findings"]}
    assert rc == 1
    assert cms["status"] == "failed"
    assert "nomos.source_lineage_rollout.source_policy_missing" in codes


def test_source_lineage_rollout_audit_writes_complete_inventory(
    tmp_path: Path,
    capsys,
) -> None:
    repo = tmp_path / "repo-deployments"
    external = tmp_path / "okg-deployments"
    profiles = tmp_path / "profiles"
    _write_deployment(repo, "okg-workspace", _posture("dogfood_workspace"))
    _write_deployment(repo, "okg-ci", _posture("ci_authority"))
    _write_deployment(external, "af_pubs", _posture("public_reference_demo"))
    _write_deployment(
        external,
        "intersystems",
        _posture("synthetic_healthcare_demo"),
    )
    _write_deployment(
        external,
        "statgen-analysis",
        _posture("governed_research_metadata"),
    )
    _write_deployment(external, "cms", _posture("org_operational_private"))
    _write_deployment(external, "memex", _posture("personal_okg"))
    _write_profile(profiles, "academic-researcher", _posture("personal_okg"))
    out = tmp_path / "inventory.json"

    rc = main([
        "deployment",
        "nomos-source-lineage-rollout-audit",
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
    workspace = next(
        entry for entry in written["entries"] if entry["name"] == "okg-workspace"
    )
    assert rc == 0
    assert payload["ok"] is True
    assert written["ok"] is True
    assert written["rollout_report"]["schema"] == (
        "okg.nomos.source_lineage_rollout_report.v1"
    )
    assert written["rollout_report"]["runtime_report"] is False
    assert written["rollout_report"]["state_counts"]["unapplied"] >= 1
    assert workspace["source_entries"][0]["source_policy_fingerprint"].startswith(
        "sha256:"
    )
    assert workspace["rollout_state"]["status"] == "unapplied"
    assert workspace["rollout_state"]["owner"] == "deployment-owner:okg-workspace"
    assert "apply-baseline okg-workspace" in workspace["rollout_state"][
        "recovery_command"
    ]
    assert (
        workspace["rollout_state"]["rollback"]["agent_visible_bypass_allowed"]
        is False
    )
    assert workspace["lineage_policy"]["coverage"]["covered_source_ids"] == [
        "_catalog:restamp",
        "docs",
        "okg-workspace.docs",
    ]
    assert workspace["source_entries"][0]["lineage_source_ids"] == [
        "docs",
        "okg-workspace.docs",
    ]
    assert (
        workspace["lineage_policy"]["coverage"]["generic_read_policy_count"] == 0
    )
    assert (
        workspace["lineage_policy"]["coverage"]["boundary_read_policy_count"] >= 1
    )


def test_source_lineage_rollout_audit_normalizes_ownership_policy_ids(
    tmp_path: Path,
    capsys,
) -> None:
    repo = tmp_path / "repo-deployments"
    external = tmp_path / "okg-deployments"
    profiles = tmp_path / "profiles"
    _write_deployment(
        repo,
        "okg-workspace",
        _posture("dogfood_workspace"),
        ownership_id="okg-workspace.docs",
    )
    _write_deployment(repo, "okg-ci", _posture("ci_authority"))
    _write_deployment(external, "af_pubs", _posture("public_reference_demo"))
    _write_deployment(
        external,
        "intersystems",
        _posture("synthetic_healthcare_demo"),
    )
    _write_deployment(
        external,
        "statgen-analysis",
        _posture("governed_research_metadata"),
    )
    _write_deployment(external, "cms", _posture("org_operational_private"))
    _write_deployment(external, "memex", _posture("personal_okg"))
    _write_profile(profiles, "academic-researcher", _posture("personal_okg"))

    rc = main([
        "deployment",
        "nomos-source-lineage-rollout-audit",
        "--repo-deployment-dir",
        str(repo),
        "--deployment-dir",
        str(external),
        "--profile-dir",
        str(profiles),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    workspace = next(
        entry for entry in payload["entries"] if entry["name"] == "okg-workspace"
    )
    manifest = yaml.safe_load(
        (repo / "okg-workspace" / "deployment.yaml").read_text(encoding="utf-8")
    )
    registry = yaml.safe_load(
        (repo / "okg-workspace" / "source_registry.yaml").read_text(
            encoding="utf-8"
        )
    )
    expected_policy = compile_policy_graph(
        deployment_baseline_policy_graph(
            manifest,
            deployment_name="okg-workspace",
            scope="deployment:okg-workspace",
            activated_by="subject:nomos-rollout",
            source_names=_nomos_source_lineage_read_policy_source_ids(
                registry,
                deployment="okg-workspace",
                deployment_dir=repo / "okg-workspace",
            ),
            source_policies=_nomos_audited_source_policies(
                manifest=manifest,
                source_registry=registry,
                deployment="okg-workspace",
            ),
            runtime_source_names=["docs"],
        ),
    )
    runtime_source_allowed = evaluate_runtime_boundary(
        expected_policy,
        NomosRuntimeRequest(
            surface="source.emit",
            subject="source:docs",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=expected_policy.policy_fingerprint,
        performed_obligations=("audit",),
    )
    runtime_alias_allowed = evaluate_runtime_boundary(
        expected_policy,
        NomosRuntimeRequest(
            surface="source.emit",
            subject="source:okg-workspace.docs",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=expected_policy.policy_fingerprint,
        performed_obligations=("audit",),
    )

    assert rc == 0
    assert workspace["status"] == "pass"
    assert workspace["source_policy_missing"] == []
    assert workspace["source_count"] == 1
    assert [entry["source_name"] for entry in workspace["source_entries"]] == ["docs"]
    assert workspace["source_entries"][0]["lineage_source_ids"] == [
        "docs",
        "okg-workspace.docs",
    ]
    assert workspace["lineage_policy"]["coverage"]["covered_source_ids"] == [
        "_catalog:restamp",
        "docs",
        "okg-workspace.docs",
    ]
    assert (
        workspace["lineage_policy"]["expected_policy_fingerprint"]
        == expected_policy.policy_fingerprint
    )
    assert runtime_source_allowed.allowed is True
    # The deployment-qualified source subject (source:okg-workspace.docs) is now
    # an accepted variant of the registered source, so production adapters that
    # emit under the qualified subject are not default-denied
    # (PACT repair-okg-ci-source-health).
    assert runtime_alias_allowed.allowed is True
    assert runtime_alias_allowed.decision.reason_code == (
        "nomos.rollout.baseline_registered_source_allow"
    )


def test_source_lineage_rollout_operator_report_names_blocked_entries(
    tmp_path: Path,
    capsys,
) -> None:
    repo = tmp_path / "repo-deployments"
    external = tmp_path / "okg-deployments"
    profiles = tmp_path / "profiles"
    _write_deployment(repo, "okg-workspace", _posture("dogfood_workspace"))
    _write_deployment(repo, "okg-ci", _posture("ci_authority"))
    _write_deployment(external, "af_pubs", _posture("public_reference_demo"))
    _write_deployment(
        external,
        "intersystems",
        _posture("synthetic_healthcare_demo"),
    )
    _write_deployment(
        external,
        "statgen-analysis",
        _posture("governed_research_metadata"),
    )
    _write_deployment(
        external,
        "cms",
        _posture("org_operational_private"),
        source_class="unknown_class",
    )
    _write_deployment(external, "memex", _posture("personal_okg"))
    _write_profile(profiles, "academic-researcher", _posture("personal_okg"))

    rc = main([
        "deployment",
        "nomos-source-lineage-rollout-audit",
        "--repo-deployment-dir",
        str(repo),
        "--deployment-dir",
        str(external),
        "--profile-dir",
        str(profiles),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    cms = next(entry for entry in payload["entries"] if entry["name"] == "cms")
    blocked = {
        entry["name"]: entry for entry in payload["rollout_report"][
            "blocked_entries"
        ]
    }
    assert rc == 1
    assert cms["rollout_state"]["status"] == "blocked"
    assert cms["rollout_state"]["owner"] == "okg-deployments:cms"
    assert "edit " in cms["rollout_state"]["recovery_command"]
    assert blocked["cms"]["owner"] == "okg-deployments:cms"
    assert blocked["cms"]["recovery_command"] == cms["rollout_state"][
        "recovery_command"
    ]


def test_source_lineage_rollout_runtime_state_classifies_operator_modes() -> None:
    entry = {
        "name": "okg-workspace",
        "kind": "deployment",
        "origin": "repo",
        "status": "pass",
        "path": "deployments/okg-workspace/deployment.yaml",
        "posture": {"readiness_profiles": ["dogfood"]},
        "lineage_policy": {
            "expected_policy_bundle_id": "nomos:rollout:baseline:okg-workspace",
            "expected_policy_fingerprint": "sha256:expected",
        },
    }

    assert _runtime_state(entry, active_ok=True, lineage_mode="active") == "active"
    assert _runtime_state(
        entry,
        active_ok=False,
        lineage_mode="missing_policy",
        finding="nomos.runtime.no_active_policy_bundle",
    ) == "missing_policy"
    assert _runtime_state(
        entry,
        active_ok=False,
        lineage_mode="active",
        finding="nomos.runtime.active_policy_mismatch",
    ) == "stale_policy"
    assert _runtime_state(entry, active_ok=False, lineage_mode="bypassed") == "bypass"
    assert _runtime_state(
        entry,
        active_ok=False,
        lineage_mode="stale_fingerprint",
    ) == "stale_policy"
    stale_audit_state = _nomos_source_lineage_rollout_state_from_runtime_reports(
        entry,
        active_state={
            "ok": True,
            "status": "pass",
            "effective_mode": "audit",
            "mode_source": "manifest",
            "mode_source_detail": "deployment.yaml",
            "blocks_actions": False,
            "rollout_exception": {
                "allowed": True,
                "readiness_profile": "dogfood",
                "expires_at": "2999-01-01T00:00:00Z",
            },
            "findings": [],
        },
        lineage_report={
            "ok": True,
            "mode": "active",
            "runtime_enforcement_mode": "audit",
            "runtime_enforcement_mode_source": "manifest",
            "blocks_actions": False,
            "rollout_exception": {
                "allowed": True,
                "readiness_profile": "dogfood",
                "expires_at": "2999-01-01T00:00:00Z",
            },
            "read_surfaces": {"mcp": "source_lineage_filtered"},
            "sources_missing_policy": [],
            "stale_policy_sources": ["docs"],
            "missing_lineage": {"node_count": 0, "edge_count": 0},
            "errors": [],
        },
        readiness_profile="dogfood",
    )
    assert stale_audit_state["status"] == "active"
    assert stale_audit_state["active_state"]["effective_mode"] == "audit"
    assert stale_audit_state["active_state"]["mode_source"] == "manifest"
    assert (
        stale_audit_state["active_state"]["mode_source_detail"] == "deployment.yaml"
    )
    assert stale_audit_state["active_state"]["blocks_actions"] is False
    assert stale_audit_state["active_state"]["rollout_exception"] == {
        "allowed": True,
        "readiness_profile": "dogfood",
        "expires_at": "2999-01-01T00:00:00Z",
    }
    assert stale_audit_state["source_lineage"]["runtime_enforcement_mode"] == "audit"
    assert (
        stale_audit_state["source_lineage"]["runtime_enforcement_mode_source"]
        == "manifest"
    )
    assert stale_audit_state["source_lineage"]["blocks_actions"] is False
    assert stale_audit_state["source_lineage"]["rollout_exception"] == {
        "allowed": True,
        "readiness_profile": "dogfood",
        "expires_at": "2999-01-01T00:00:00Z",
    }
    assert stale_audit_state["source_lineage"]["stale_policy_sources"] == ["docs"]
    stale_fingerprint_state = _nomos_source_lineage_rollout_state_from_runtime_reports(
        entry,
        active_state={"ok": True, "status": "pass", "findings": []},
        lineage_report={
            "ok": False,
            "mode": "stale_fingerprint",
            "read_surfaces": {"mcp": "source_lineage_filtered"},
            "sources_missing_policy": [],
            "stale_policy_sources": ["docs"],
            "missing_lineage": {"node_count": 0, "edge_count": 0},
            "errors": [],
        },
        readiness_profile="dogfood",
    )
    assert stale_fingerprint_state["status"] == "stale_policy"
    assert (
        "--restamp-active-policy-fingerprints okg-workspace"
        in stale_fingerprint_state["recovery_command"]
    )
    assert _runtime_state(
        entry,
        active_ok=False,
        lineage_mode="quarantine",
    ) == "blocked"


def _write_deployment(
    root: Path,
    name: str,
    nomos: dict,
    *,
    source_class: str = "discovery_crawl",
    ownership_id: str | None = None,
) -> None:
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": name,
            "source_registry": "source_registry.yaml",
            "nomos": nomos,
        }),
        encoding="utf-8",
    )
    source_entry = {
        "source_class": source_class,
        "record_identity_kind": "scoped_locator",
        "source_revision_kind": "version_token",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    if ownership_id is not None:
        source_entry["ownership_id"] = ownership_id
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"docs": source_entry}}),
        encoding="utf-8",
    )


def _write_profile(root: Path, name: str, nomos: dict) -> None:
    profile_dir = root / name
    profile_dir.mkdir(parents=True)
    (profile_dir / "deployment-defaults.yaml").write_text(
        yaml.safe_dump({"nomos": nomos}),
        encoding="utf-8",
    )


def _runtime_state(
    entry: dict,
    *,
    active_ok: bool,
    lineage_mode: str,
    finding: str | None = None,
) -> str:
    state = _nomos_source_lineage_rollout_state_from_runtime_reports(
        entry,
        active_state={
            "ok": active_ok,
            "status": "pass" if active_ok else "failed",
            "findings": (
                [{"reason_code": finding, "severity": "error"}]
                if finding else []
            ),
        },
        lineage_report={
            "ok": lineage_mode == "active",
            "mode": lineage_mode,
            "read_surfaces": {"mcp": "source_lineage_filtered"},
            "sources_missing_policy": ["docs"] if lineage_mode == "missing_policy" else [],
            "stale_policy_sources": (
                ["docs"] if lineage_mode == "stale_fingerprint" else []
            ),
            "missing_lineage": {"node_count": 0, "edge_count": 0},
            "errors": [],
        },
        readiness_profile="dogfood",
    )
    assert state["owner"] == "deployment-owner:okg-workspace"
    assert state["rollback"]["agent_visible_bypass_allowed"] is False
    assert state["recovery_command"]
    return str(state["status"])
