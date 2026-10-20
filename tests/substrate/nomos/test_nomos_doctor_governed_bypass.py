"""req.gov.lint-and-doctor — doctor keys the runtime-bypass error off
deployment_class severity, so a governed-private deployment running ungoverned
is flagged rather than reported healthy."""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.nomos.doctor import (
    GOVERNED_DEPLOYMENT_CLASSES,
    source_lineage_readiness_report,
)


pytestmark = pytest.mark.postgres


def _write_deployment(
    root,
    name,
    *,
    deployment_class,
    enforcement,
    audit_exception_profile=None,
):
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    enforcement_line = (
        f"  runtime_enforcement: {enforcement}\n" if enforcement is not None else ""
    )
    rollout_block = ""
    if audit_exception_profile:
        rollout_block = (
            "  rollout:\n"
            "    audit_readiness_exception:\n"
            "      allowed: true\n"
            "      readiness_profiles:\n"
            f"        - {audit_exception_profile}\n"
            "      reason: governed audit rollout\n"
            "      owner: nomos-team\n"
            "      expires_at: 2999-01-01T00:00:00Z\n"
        )
    (d / "deployment.yaml").write_text(
        f"name: {name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "nomos:\n"
        f"{enforcement_line}"
        f"{rollout_block}"
        "  deployment_posture:\n"
        f"    deployment_class: {deployment_class}\n",
        encoding="utf-8",
    )
    return d


def test_governed_classes_set_is_nonempty():
    assert "org_operational_private" in GOVERNED_DEPLOYMENT_CLASSES
    assert "public_reference_demo" not in GOVERNED_DEPLOYMENT_CLASSES


def test_doctor_flags_governed_private_bypass_under_general_profile(
    clean_runtime: str,
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    root = tmp_path / "deployments"
    # Governed-private class, but runtime enforcement is OFF -> ungoverned.
    _write_deployment(
        root,
        "org-private",
        deployment_class="org_operational_private",
        enforcement="off",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name="org-private",
            # NOT a ci/dogfood profile: the bypass must still flag because the
            # class is governed-private.
            readiness_profile="general",
    )

    assert report["runtime_enforcement"] is False
    assert report["runtime_enforcement_mode"] == "off"
    assert report["runtime_enforcement_mode_source"] == "manifest"
    assert report["blocks_actions"] is False
    assert report["ok"] is False
    codes = {e["code"] for e in report["errors"]}
    assert "nomos_source_lineage_runtime_bypassed" in codes


def test_doctor_does_not_flag_public_demo_bypass_under_general_profile(
    clean_runtime: str,
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    root = tmp_path / "deployments"
    # A genuinely-public demo is not a governed class: bypass under a
    # non-protected profile is not an error (no regression for public graphs).
    _write_deployment(
        root,
        "public-demo",
        deployment_class="public_reference_demo",
        enforcement="off",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name="public-demo",
            readiness_profile="general",
    )

    assert report["runtime_enforcement"] is False
    assert report["runtime_enforcement_mode"] == "off"
    assert report["runtime_enforcement_mode_source"] == "manifest"
    assert report["blocks_actions"] is False
    codes = {e["code"] for e in report["errors"]}
    assert "nomos_source_lineage_runtime_bypassed" not in codes


def test_doctor_audit_mode_governed_private_is_not_bypassed(
    clean_runtime: str,
    tmp_path,
    monkeypatch,
) -> None:
    # audit mode EVALUATES the boundary, so runtime_enforcement reads True and
    # the deployment is NOT reported as bypassed.
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    root = tmp_path / "deployments"
    _write_deployment(
        root,
        "org-private-audit",
        deployment_class="org_operational_private",
        enforcement="audit",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name="org-private-audit",
            readiness_profile="general",
    )

    assert report["runtime_enforcement"] is True
    assert report["runtime_enforcement_mode"] == "audit"
    assert report["runtime_enforcement_mode_source"] == "manifest"
    assert report["blocks_actions"] is False
    assert report["rollout_exception"] is None
    codes = {e["code"] for e in report["errors"]}
    assert "nomos_source_lineage_runtime_bypassed" not in codes


def test_doctor_output_names_audit_rollout_exception(
    clean_runtime: str,
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    root = tmp_path / "deployments"
    _write_deployment(
        root,
        "org-private-audit-exception",
        deployment_class="org_operational_private",
        enforcement="audit",
        audit_exception_profile="general",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name="org-private-audit-exception",
            readiness_profile="general",
        )

    assert report["runtime_enforcement"] is True
    assert report["runtime_enforcement_mode"] == "audit"
    assert report["runtime_enforcement_mode_source"] == "manifest"
    assert report["blocks_actions"] is False
    assert report["rollout_exception"] == {
        "allowed": True,
        "readiness_profile": "general",
        "readiness_profiles": ["general"],
        "reason": "governed audit rollout",
        "owner": "nomos-team",
        "expires_at": "2999-01-01T00:00:00Z",
    }
