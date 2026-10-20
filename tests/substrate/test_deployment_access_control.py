from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.nomos import (
    IdentityProvider,
    NomosRuntimeRequest,
    PermissionGrant,
    SubjectBinding,
    activate_policy_bundle,
    compile_policy_graph,
    deployment_baseline_policy_graph,
    evaluate_runtime_boundary_with_active_policy,
    load_active_config_set,
    load_active_policy_bundle,
    load_permission_grants,
    load_subject_bindings,
    seed_deployment_baseline_nomos_state,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.cli.deployment import (
    _ready_nomos_active_state_status,
    _ready_nomos_blocking_required,
)
from okg.substrate.cli.main import main


def _write_deployment(
    tmp_path: Path,
    *,
    name: str,
    manifest: dict,
    registry: dict,
) -> Path:
    deployment_dir = tmp_path / name
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump(registry),
        encoding="utf-8",
    )
    return deployment_dir


def _valid_ci_nomos() -> dict:
    return {
        "deployment_posture": {
            "deployment_class": "ci_authority",
            "read_principals": ["ci_runner", "okg_service"],
            "write_principals": ["ci_runner", "okg_service"],
            "mcp_write_default": "deny",
            "export_policy": "internal_only",
            "secret_handling": "secret_ref_required",
            "live_call_default": "review",
            "allowed_source_sensitivities": ["internal"],
            "allowed_data_classifications": ["internal"],
            "allowed_exportability": ["internal"],
            "privacy_obligations": ["audit", "secret_ref_required"],
            "readiness_profiles": ["ci"],
        },
        "source_policy_defaults": {
            "by_source_class": {
                "discovery_crawl": {
                    "sensitivity": "internal",
                    "data_classification": "internal",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "internal",
                    "retention": "normal",
                    "provenance": "local_filesystem",
                    "privacy_obligations": ["audit"],
                },
            }
        },
    }


def _patch_deployments(monkeypatch, deployments: dict[str, Path]) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: deployments.get(name),
    )


def _patch_live_checks(monkeypatch) -> None:
    def _fake_live_checks(**_kwargs):
        return {
            "checks": [],
            "blockers": [],
            "warnings": [],
            "generation": {
                "generation_id": "okg:test:branch:default:gen:test",
                "status": "published",
            },
            "source_freshness": {"ok": True, "missing": [], "stale": []},
            "source_sync_status": {"ok": True, "status": "healthy"},
            "mcp": {"ok": True},
        }

    monkeypatch.setattr(
        "okg.substrate.cli.deployment._deployment_ready_live_checks",
        _fake_live_checks,
    )


def _seed_manifest_baseline(conn, deployment_dir: Path) -> dict:
    """Seed the baseline the way `okg deployment nomos-seed` does.

    Readiness derives the expected policy from the deployment directory
    (`deployment_baseline_spec`), which composes the source-binding policy
    into the rollout baseline. Seeding from the manifest alone activates a
    policy without that composition, which readiness correctly reports as
    `active Nomos policy does not match the deployment rollout baseline`.
    """
    from okg.substrate.nomos.baseline import deployment_baseline_spec

    spec = deployment_baseline_spec(
        deployment="okg-ci",
        deployment_dir=deployment_dir,
    )
    assert spec["required"] is True
    return seed_deployment_baseline_nomos_state(
        conn,
        deployment_name="okg-ci",
        manifest=spec["manifest"],
        source_names=spec["source_names"],
        source_policies=spec["source_policies"],
        runtime_source_names=spec["runtime_source_names"],
        policy_graph=spec["policy_graph"],
    )


def _patch_converged_apply(monkeypatch) -> None:
    """Report the deployment's apply state as converged.

    `deployment ready` also blocks on `okg deployment apply` convergence.
    These fixture deployments have no catalog, so the catalog ownership and
    catalog apply stages can never converge here. The tests prove only the
    Nomos readiness checks for a seeded baseline, the same way
    `_patch_live_checks` stands in for the live graph checks; the apply
    stages have their own tests.
    """

    def _converged_apply_health(**kwargs):
        return {
            "ok": True,
            "status": "ready",
            "warnings": [],
            "managed": True,
            "deployment": kwargs["deployment"],
            "problems": [],
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        _converged_apply_health,
    )


def test_baseline_policy_allows_qualified_runtime_source_subjects() -> None:
    manifest = {"name": "okg-ci", "nomos": _valid_ci_nomos()}
    graph = deployment_baseline_policy_graph(
        manifest,
        deployment_name="okg-ci",
        scope="deployment:okg-ci",
        activated_by="subject:nomos-rollout",
        source_names=["git-history", "github-issues"],
        source_policies={
            "github-issues": {
                "live_call_allowed": True,
            },
        },
        runtime_source_names=["git-history", "github-issues"],
    )
    source_run_rule = next(
        rule for rule in graph["rules"]
        if rule["rule_id"] == "rollout_allow_registered_source_runs"
    )
    live_call_rule = next(
        rule for rule in graph["rules"]
        if rule["rule_id"] == "rollout_live_call_for_explicit_sources"
    )

    assert "source:git-history" in source_run_rule["subjects"]
    assert "source:okg-ci.git-history" in source_run_rule["subjects"]
    assert "source:github-issues" in live_call_rule["subjects"]
    assert "source:okg-ci.github-issues" in live_call_rule["subjects"]


def test_protected_readiness_blocks_missing_nomos_policy(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", "postgres://test/ci")
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "source_registry": "source_registry.yaml",
        },
        registry={
            "sources": {
                "git-history": {
                    "ownership_id": "okg-ci.git-history",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["repo", "node_id"],
                    "source_revision_kind": "version_token",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                },
            }
        },
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)

    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["nomos_audit"]["ok"] is False
    assert any(
        blocker["id"] == "nomos.deployment_policy"
        for blocker in payload["blockers"]
    )
    codes = {
        finding["reason_code"]
        for finding in payload["nomos_audit"]["findings"]
    }
    assert "nomos.audit.deployment_posture_missing" in codes


def test_protected_readiness_accepts_policy_but_requires_runtime_mode(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", "postgres://test/ci")
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "source_registry": "source_registry.yaml",
            "nomos": _valid_ci_nomos(),
        },
        registry={
            "sources": {
                "git-history": {
                    "ownership_id": "okg-ci.git-history",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["repo", "node_id"],
                    "source_revision_kind": "version_token",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                },
            }
        },
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)

    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["nomos_audit"]["ok"] is True
    assert payload["nomos_audit"]["source_policy_count"] == 1
    assert any(
        check["id"] == "nomos.deployment_policy"
        and check["status"] == "pass"
        for check in payload["checks"]
    )
    assert payload["nomos_runtime"]["effective_mode"] == "off"
    assert any(
        blocker["id"] == "nomos.active_state"
        for blocker in payload["blockers"]
    )


def test_nomos_blocking_required_recognizes_production_and_posture_profiles() -> None:
    manifest = {
        "nomos": {
            "deployment_posture": {
                "readiness_profiles": ["custom-production"],
            },
        },
    }

    for profile in ("ci", "dogfood", "prod", "production", "custom-production"):
        assert _ready_nomos_blocking_required(
            manifest,
            readiness_profile=profile,
        ) is True
    assert _ready_nomos_blocking_required(
        manifest,
        readiness_profile="general",
    ) is False


def test_nomos_blocking_required_keeps_production_profiles_when_nomos_missing() -> None:
    manifest = {"name": "local-dev"}

    for profile in ("ci", "dogfood", "prod", "production"):
        assert _ready_nomos_blocking_required(
            manifest,
            readiness_profile=profile,
        ) is True
    assert _ready_nomos_blocking_required(
        manifest,
        readiness_profile="general",
    ) is False


def test_nomos_active_state_blocks_production_profile_without_nomos(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)

    payload = _ready_nomos_active_state_status(
        deployment="prod-demo",
        dsn=None,
        manifest={"name": "prod-demo"},
        deployment_dir=tmp_path,
        readiness_profile="production",
    )

    assert payload["ok"] is False
    assert payload["status"] == "failed"
    assert payload["blocking_required"] is True
    assert payload["effective_mode"] == "off"
    assert payload["mode_source"] == "default"
    assert payload["mode_source_detail"] == "default_off"
    assert {
        finding["reason_code"]
        for finding in payload["findings"]
    } == {"nomos.runtime.mode_not_blocking"}


def test_nomos_readiness_reports_environment_mode_source(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")

    payload = _ready_nomos_active_state_status(
        deployment="prod-demo",
        dsn=None,
        manifest={"name": "prod-demo"},
        deployment_dir=tmp_path,
        readiness_profile="production",
    )

    assert payload["ok"] is False
    assert payload["effective_mode"] == "enforce"
    assert payload["mode_source"] == "environment"
    assert payload["mode_source_detail"] == "OKG_NOMOS_RUNTIME_ENFORCEMENT"


def test_protected_readiness_rejects_nomos_off_mode(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", "postgres://test/ci")
    nomos = _valid_ci_nomos()
    nomos["runtime_enforcement"] = "off"
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "source_registry": "source_registry.yaml",
            "nomos": nomos,
        },
        registry={"sources": {}},
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)

    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["nomos_runtime"]["effective_mode"] == "off"
    assert payload["nomos_runtime"]["blocks_actions"] is False
    assert payload["nomos_runtime"]["rollout_exception"] is None
    assert {
        finding["reason_code"]
        for finding in payload["nomos_runtime"]["findings"]
    } == {"nomos.runtime.mode_not_blocking"}


def test_protected_readiness_rejects_audit_without_rollout_exception(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", "postgres://test/ci")
    nomos = _valid_ci_nomos()
    nomos["runtime_enforcement"] = "audit"
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "source_registry": "source_registry.yaml",
            "nomos": nomos,
        },
        registry={"sources": {}},
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)

    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["nomos_runtime"]["effective_mode"] == "audit"
    assert payload["nomos_runtime"]["blocks_actions"] is False
    assert payload["nomos_runtime"]["rollout_exception"] is None
    assert {
        finding["reason_code"]
        for finding in payload["nomos_runtime"]["findings"]
    } == {"nomos.runtime.mode_not_blocking"}


@pytest.mark.postgres
def test_enforce_readiness_blocks_missing_active_nomos_state(
    tmp_path: Path,
    monkeypatch,
    capsys,
    clean_runtime: str,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", clean_runtime)
    nomos = _valid_ci_nomos()
    nomos["runtime_enforcement"] = "enforce"
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "source_registry": "source_registry.yaml",
            "nomos": nomos,
        },
        registry={"sources": {}},
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)

    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["nomos_runtime"]["ok"] is False
    assert any(
        blocker["id"] == "nomos.active_state"
        for blocker in payload["blockers"]
    )
    codes = {
        finding["reason_code"]
        for finding in payload["nomos_runtime"]["findings"]
    }
    assert "nomos.runtime.no_active_policy_bundle" in codes
    assert "nomos.runtime.no_active_config_set" in codes


@pytest.mark.postgres
def test_protected_readiness_allows_audit_with_explicit_rollout_exception(
    tmp_path: Path,
    monkeypatch,
    capsys,
    clean_runtime: str,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", clean_runtime)
    nomos = _valid_ci_nomos()
    nomos["runtime_enforcement"] = "audit"
    nomos["rollout"] = {
        "audit_readiness_exception": {
            "allowed": True,
            "readiness_profiles": ["ci"],
            "reason": "temporary production rollout",
            "owner": "nomos-team",
            "expires_at": "2999-01-01T00:00:00Z",
        },
    }
    manifest = {
        "name": "okg-ci",
        "profile": {"name": "okg-ci"},
        "postgres": {"dsn": "${OKG_CI_DSN}"},
        "source_registry": "source_registry.yaml",
        "nomos": nomos,
    }
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest=manifest,
        registry={"sources": {}},
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)
    _patch_converged_apply(monkeypatch)
    with psycopg.connect(clean_runtime) as conn:
        seeded = _seed_manifest_baseline(conn, deployment_dir)
        conn.commit()

    assert seeded["ok"] is True
    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["nomos_runtime"]["effective_mode"] == "audit"
    assert payload["nomos_runtime"]["blocks_actions"] is False
    assert payload["nomos_runtime"]["rollout_exception"] == {
        "allowed": True,
        "readiness_profile": "ci",
        "readiness_profiles": ["ci"],
        "reason": "temporary production rollout",
        "owner": "nomos-team",
        "expires_at": "2999-01-01T00:00:00Z",
    }


@pytest.mark.postgres
def test_enforce_readiness_accepts_seeded_baseline_nomos_state(
    tmp_path: Path,
    monkeypatch,
    capsys,
    clean_runtime: str,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", clean_runtime)
    nomos = _valid_ci_nomos()
    nomos["runtime_enforcement"] = "enforce"
    manifest = {
        "name": "okg-ci",
        "profile": {"name": "okg-ci"},
        "postgres": {"dsn": "${OKG_CI_DSN}"},
        "source_registry": "source_registry.yaml",
        "nomos": nomos,
    }
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest=manifest,
        registry={"sources": {}},
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)
    _patch_converged_apply(monkeypatch)
    with psycopg.connect(clean_runtime) as conn:
        seeded = _seed_manifest_baseline(conn, deployment_dir)
        conn.commit()

    assert seeded["ok"] is True
    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["nomos_runtime"]["ok"] is True
    assert payload["nomos_runtime"]["effective_mode"] == "enforce"
    assert payload["nomos_runtime"]["blocks_actions"] is True
    assert payload["nomos_runtime"]["rollout_exception"] is None
    assert payload["nomos_runtime"]["active_policy"]["policy_bundle_id"] == (
        "nomos:rollout:baseline:okg-ci"
    )
    assert payload["nomos_runtime"]["active_config"]["config_set_id"] == (
        "nomos:rollout:baseline-config:okg-ci"
    )


@pytest.mark.postgres
def test_baseline_reseed_reconciles_mcp_record_grants(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "record-reseed-test"
    scope = f"deployment:{deployment}"
    nomos = _valid_ci_nomos()
    posture = nomos["deployment_posture"]
    posture["deployment_class"] = "dogfood_workspace"
    posture["write_principals"] = ["okg_service", "mcp_client"]
    posture["mcp_write_default"] = "review"
    manifest = {"name": deployment, "nomos": nomos}

    def _copy_manifest() -> dict:
        return json.loads(json.dumps(manifest))

    def _record_allowed(conn: psycopg.Connection, subject: str) -> bool:
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=deployment,
            scope=scope,
            request=NomosRuntimeRequest(
                surface="mcp.record",
                subject=subject,
                resource={
                    "deployment": deployment,
                    "scope": scope,
                    "surface": "mcp",
                    "tool": "record",
                    "record_kind": "observation",
                },
            ),
            performed_obligations=("audit",),
        )
        assert outcome is not None
        return outcome.allowed

    with psycopg.connect(clean_runtime) as conn:
        first = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=manifest,
        )
        first_policy = load_active_policy_bundle(
            conn,
            deployment_name=deployment,
            scope=scope,
        )
        first_config = load_active_config_set(
            conn,
            deployment_name=deployment,
            scope=scope,
        )
        assert first["ok"] is True
        assert first_policy is not None
        assert first_config is not None
        assert _record_allowed(conn, "subject:mcp_client") is True

        provider = IdentityProvider(
            provider_id="record-reseed-sso",
            provider_type="oidc",
            issuer="https://auth.example.test/record-reseed",
            audiences=("record-reseed",),
            key_reference="secret://record-reseed/jwks",
            freshness_seconds=3600,
        )
        binding = SubjectBinding(
            binding_id="binding:record-reseed:writer",
            provider_id=provider.provider_id,
            issuer=provider.issuer,
            claim_path="groups",
            claim_value="writers",
            subject="subject:record-reseed:writer",
            scope=scope,
        )
        grant = PermissionGrant(
            grant_id="grant:record-reseed:writer",
            subject=binding.subject,
            actions=("mcp.record",),
            scope=scope,
        )
        upsert_identity_provider(
            conn,
            deployment_name=deployment,
            provider=provider,
        )
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=binding,
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=grant,
        )
        iam_before = (
            [
                row.to_dict()
                for row in load_subject_bindings(
                    conn,
                    deployment_name=deployment,
                    scope=scope,
                )
            ],
            [
                row.to_dict()
                for row in load_permission_grants(
                    conn,
                    deployment_name=deployment,
                    scope=scope,
                )
            ],
        )

        added = _copy_manifest()
        added["nomos"]["deployment_posture"]["write_principals"].append("agent")
        seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=added,
        )
        added_policy = load_active_policy_bundle(
            conn,
            deployment_name=deployment,
            scope=scope,
        )
        assert added_policy is not None
        assert added_policy.policy_fingerprint != first_policy.policy_fingerprint
        assert _record_allowed(conn, "subject:mcp_client") is True
        assert _record_allowed(conn, "subject:agent") is True

        removed = _copy_manifest()
        removed["nomos"]["deployment_posture"]["write_principals"] = [
            "okg_service",
            "agent",
        ]
        seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=removed,
        )
        assert _record_allowed(conn, "subject:mcp_client") is False
        assert _record_allowed(conn, "subject:agent") is True

        denied = _copy_manifest()
        denied["nomos"]["deployment_posture"]["mcp_write_default"] = "deny"
        seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=denied,
        )
        denied_policy = load_active_policy_bundle(
            conn,
            deployment_name=deployment,
            scope=scope,
        )
        denied_config = load_active_config_set(
            conn,
            deployment_name=deployment,
            scope=scope,
        )
        assert denied_policy is not None
        assert denied_config is not None
        assert denied_policy.policy_fingerprint != added_policy.policy_fingerprint
        assert denied_config.config_fingerprint != first_config.config_fingerprint
        assert _record_allowed(conn, "subject:mcp_client") is False
        assert _record_allowed(conn, "subject:agent") is False

        iam_after = (
            [
                row.to_dict()
                for row in load_subject_bindings(
                    conn,
                    deployment_name=deployment,
                    scope=scope,
                )
            ],
            [
                row.to_dict()
                for row in load_permission_grants(
                    conn,
                    deployment_name=deployment,
                    scope=scope,
                )
            ],
        )
        assert iam_after == iam_before

        custom = compile_policy_graph(
            {
                "policy_bundle_id": "nomos:custom:record-reseed-test",
                "policy_version": "v1",
                "default_effect": "deny",
                "rules": [
                    {
                        "rule_id": "custom_record_rule",
                        "effect": "allow",
                        "actions": ["mcp.record"],
                        "subjects": ["subject:custom-writer"],
                        "match": {"deployment": deployment, "scope": scope},
                    }
                ],
            }
        )
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=custom,
            scope=scope,
            activated_by="subject:nomos-rollout",
        )
        seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=manifest,
        )
        active_custom = load_active_policy_bundle(
            conn,
            deployment_name=deployment,
            scope=scope,
        )
        assert active_custom is not None
        assert active_custom.policy_fingerprint == custom.policy_fingerprint
        active_config = load_active_config_set(
            conn,
            deployment_name=deployment,
            scope=scope,
        )
        assert active_config is not None
        assert active_config.config_fingerprint == denied_config.config_fingerprint
        conn.commit()


@pytest.mark.postgres
def test_enforce_readiness_rejects_active_nomos_state_mismatch(
    tmp_path: Path,
    monkeypatch,
    capsys,
    clean_runtime: str,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", clean_runtime)
    nomos = _valid_ci_nomos()
    nomos["runtime_enforcement"] = "enforce"
    manifest = {
        "name": "okg-ci",
        "profile": {"name": "okg-ci"},
        "postgres": {"dsn": "${OKG_CI_DSN}"},
        "source_registry": "source_registry.yaml",
        "nomos": nomos,
    }
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest=manifest,
        registry={
            "sources": {
                "git-history": {
                    "ownership_id": "okg-ci.git-history",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["repo", "node_id"],
                    "source_revision_kind": "version_token",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                },
            },
        },
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)
    with psycopg.connect(clean_runtime) as conn:
        seeded = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name="okg-ci",
            manifest=manifest,
            source_names=[],
        )
        conn.commit()

    assert seeded["ok"] is True
    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    codes = {
        finding["reason_code"]
        for finding in payload["nomos_runtime"]["findings"]
    }
    assert "nomos.runtime.active_policy_mismatch" in codes


def test_protected_nomos_readiness_blocks_missing_source_registry(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("OKG_CI_DSN", "postgres://test/ci")
    deployment_dir = tmp_path / "okg-ci"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "source_registry": "missing.yaml",
            "nomos": _valid_ci_nomos(),
        }),
        encoding="utf-8",
    )
    _patch_deployments(monkeypatch, {"okg-ci": deployment_dir})
    _patch_live_checks(monkeypatch)

    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["nomos_audit"]["ok"] is False
    assert payload["nomos_audit"]["findings"][0]["reason_code"] == (
        "nomos.audit.source_registry_missing"
    )
