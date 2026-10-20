from __future__ import annotations

import pytest

from okg.substrate.nomos import (
    audit_deployment_policy,
    audit_source_privacy_policy,
)


def _manifest(*, live_call_default: str = "review") -> dict:
    return {
        "name": "test-deployment",
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "agent"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": live_call_default,
                "allowed_source_sensitivities": [
                    "public",
                    "internal",
                    "local_private",
                ],
                "allowed_data_classifications": [
                    "public",
                    "internal",
                    "private",
                ],
                "allowed_exportability": ["internal", "no_export"],
                "privacy_obligations": ["audit", "review"],
                "readiness_profiles": ["dogfood"],
            },
            "source_policy_defaults": {
                "by_source_class": {
                    "discovery_crawl": {
                        "sensitivity": "local_private",
                        "data_classification": "private",
                        "credential_ref_policy": "none",
                        "live_call_allowed": False,
                        "exportability": "no_export",
                        "retention": "source_defined",
                        "provenance": "local_filesystem",
                        "privacy_obligations": ["audit", "no_export"],
                    },
                },
            },
        },
    }


def _codes(report: dict) -> set[str]:
    return {finding["reason_code"] for finding in report["findings"]}


def _case_manifest(
    deployment_class: str,
    *,
    sensitivity: str,
    classification: str,
    exportability: str = "internal",
    export_policy: str = "internal_only",
    secret_handling: str = "secret_ref_required",
    live_call_default: str = "review",
    obligations: list[str] | None = None,
) -> dict:
    obligations = obligations or ["audit", "review"]
    return {
        "name": deployment_class,
        "nomos": {
            "deployment_posture": {
                "deployment_class": deployment_class,
                "read_principals": ["human_operator", "okg_service"],
                "write_principals": ["human_operator", "source_adapter"],
                "mcp_write_default": "review",
                "export_policy": export_policy,
                "secret_handling": secret_handling,
                "live_call_default": live_call_default,
                "allowed_source_sensitivities": [sensitivity],
                "allowed_data_classifications": [classification],
                "allowed_exportability": [exportability],
                "privacy_obligations": obligations,
                "readiness_profiles": ["test"],
            },
        },
    }


@pytest.mark.parametrize(
    ("case_name", "manifest", "source_entry"),
    [
        (
            "public-demo",
            _case_manifest(
                "public_reference_demo",
                sensitivity="public",
                classification="public",
                exportability="public",
                export_policy="public_allowed",
                secret_handling="no_secrets",
                live_call_default="deny",
                obligations=["audit"],
            ),
            {
                "source_policy": {
                    "sensitivity": "public",
                    "data_classification": "public",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "public",
                    "retention": "normal",
                    "provenance": "declared",
                    "privacy_obligations": ["audit"],
                },
            },
        ),
        (
            "synthetic-healthcare",
            _case_manifest(
                "synthetic_healthcare_demo",
                sensitivity="phi_synthetic",
                classification="synthetic_phi",
                exportability="no_export",
                obligations=["audit", "review", "redaction"],
            ),
            {
                "source_policy": {
                    "sensitivity": "phi_synthetic",
                    "data_classification": "synthetic_phi",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "no_export",
                    "retention": "normal",
                    "provenance": "declared",
                    "privacy_obligations": ["audit", "redaction"],
                },
            },
        ),
        (
            "research-metadata",
            _case_manifest(
                "governed_research_metadata",
                sensitivity="regulated_metadata",
                classification="regulated_metadata",
                obligations=["audit", "review", "source_retention"],
            ),
            {
                "source_policy": {
                    "sensitivity": "regulated_metadata",
                    "data_classification": "regulated_metadata",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "internal",
                    "retention": "source_defined",
                    "provenance": "declared",
                    "privacy_obligations": ["audit", "source_retention"],
                },
            },
        ),
        (
            "org-private",
            _case_manifest(
                "org_operational_private",
                sensitivity="private",
                classification="private",
                exportability="no_export",
                obligations=["audit", "review"],
            ),
            {
                "source_policy": {
                    "sensitivity": "private",
                    "data_classification": "private",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "no_export",
                    "retention": "source_defined",
                    "provenance": "operator_declared",
                    "privacy_obligations": ["audit", "no_export"],
                },
            },
        ),
        (
            "product-engineering-credentialed",
            _case_manifest(
                "private_product_engineering",
                sensitivity="credentialed_external",
                classification="restricted",
                obligations=["audit", "review", "secret_ref_required"],
            ),
            {
                "credential_refs": ["secretref:github-app"],
                "source_policy": {
                    "sensitivity": "credentialed_external",
                    "data_classification": "restricted",
                    "credential_ref_policy": "required",
                    "live_call_allowed": True,
                    "exportability": "internal",
                    "retention": "normal",
                    "provenance": "remote_api",
                    "privacy_obligations": [
                        "audit",
                        "review",
                        "secret_ref_required",
                    ],
                },
            },
        ),
        (
            "personal-private-local",
            _case_manifest(
                "personal_okg",
                sensitivity="local_private",
                classification="private",
                exportability="no_export",
                export_policy="no_export",
                obligations=["audit", "review", "no_export"],
            ),
            {
                "source_policy": {
                    "sensitivity": "local_private",
                    "data_classification": "private",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "no_export",
                    "retention": "source_defined",
                    "provenance": "local_filesystem",
                    "privacy_obligations": ["audit", "no_export"],
                },
            },
        ),
        (
            "remote-ci-live-call",
            {
                **_case_manifest(
                    "ci_authority",
                    sensitivity="credentialed_external",
                    classification="internal",
                    obligations=["audit", "review", "secret_ref_required"],
                ),
                "nomos": {
                    **_case_manifest(
                        "ci_authority",
                        sensitivity="credentialed_external",
                        classification="internal",
                        obligations=[
                            "audit",
                            "review",
                            "secret_ref_required",
                        ],
                    )["nomos"],
                    "deployment_posture": {
                        **_case_manifest(
                            "ci_authority",
                            sensitivity="credentialed_external",
                            classification="internal",
                            obligations=[
                                "audit",
                                "review",
                                "secret_ref_required",
                            ],
                        )["nomos"]["deployment_posture"],
                        "mcp_write_default": "deny",
                        "read_principals": ["ci_runner", "okg_service"],
                        "write_principals": ["ci_runner", "okg_service"],
                    },
                },
            },
            {
                "source_policy": {
                    "sensitivity": "credentialed_external",
                    "data_classification": "internal",
                    "credential_ref_policy": "optional",
                    "live_call_allowed": True,
                    "exportability": "internal",
                    "retention": "normal",
                    "provenance": "remote_api",
                    "privacy_obligations": ["audit", "review"],
                },
            },
        ),
    ],
)
def test_source_policy_allowed_combinations(
    case_name: str,
    manifest: dict,
    source_entry: dict,
) -> None:
    report = audit_source_privacy_policy(
        case_name,
        source_entry,
        manifest=manifest,
    )

    assert report["ok"] is True, report["findings"]


def test_source_without_policy_or_explicit_default_fails() -> None:
    report = audit_deployment_policy(
        _manifest(),
        source_registry={
            "sources": {
                "unknown": {"source_class": "custom_source"},
            }
        },
    )

    assert report["ok"] is False
    assert "nomos.audit.source_policy_missing" in _codes(report)


def test_non_mapping_source_policy_is_not_silently_defaulted() -> None:
    report = audit_source_privacy_policy(
        "bad-source",
        {"source_class": "discovery_crawl", "source_policy": "private"},
        manifest=_manifest(),
    )

    assert report["ok"] is False
    assert "nomos.audit.source_policy_invalid" in _codes(report)


def test_source_sensitivity_cannot_exceed_deployment_posture() -> None:
    report = audit_source_privacy_policy(
        "secrets",
        {
            "source_policy": {
                "sensitivity": "secret",
                "data_classification": "private",
                "credential_ref_policy": "none",
                "live_call_allowed": False,
                "exportability": "no_export",
                "retention": "normal",
                "provenance": "declared",
                "privacy_obligations": ["audit"],
            }
        },
        manifest=_manifest(),
    )

    assert report["ok"] is False
    assert "nomos.audit.source_sensitivity_not_allowed" in _codes(report)


def test_secret_source_requires_secret_ref_policy_refs_and_obligation() -> None:
    report = audit_source_privacy_policy(
        "secrets",
        {
            "source_policy": {
                "sensitivity": "secret",
                "data_classification": "secret",
                "credential_ref_policy": "none",
                "live_call_allowed": False,
                "exportability": "no_export",
                "retention": "normal",
                "provenance": "declared",
                "privacy_obligations": ["audit"],
            }
        },
        manifest={
            "name": "secret-test",
            "nomos": {
                "deployment_posture": {
                    **_manifest()["nomos"]["deployment_posture"],
                    "allowed_source_sensitivities": ["secret"],
                    "allowed_data_classifications": ["secret"],
                    "allowed_exportability": ["no_export"],
                    "secret_handling": "secret_ref_required",
                },
            },
        },
    )

    assert report["ok"] is False
    codes = _codes(report)
    assert "nomos.audit.secret_ref_required" in codes
    assert "nomos.audit.credential_ref_required" in codes
    assert "nomos.audit.secret_ref_obligation_missing" in codes


def test_no_export_deployment_rejects_internal_source_exportability() -> None:
    report = audit_source_privacy_policy(
        "personal-notes",
        {
            "source_policy": {
                "sensitivity": "local_private",
                "data_classification": "private",
                "credential_ref_policy": "none",
                "live_call_allowed": False,
                "exportability": "internal",
                "retention": "source_defined",
                "provenance": "local_filesystem",
                "privacy_obligations": ["audit", "no_export"],
            }
        },
        manifest={
            "name": "personal",
            "nomos": {
                "deployment_posture": {
                    **_manifest()["nomos"]["deployment_posture"],
                    "deployment_class": "personal_okg",
                    "export_policy": "no_export",
                    "allowed_exportability": ["internal", "no_export"],
                    "privacy_obligations": ["audit", "review", "no_export"],
                },
            },
        },
    )

    assert report["ok"] is False
    assert "nomos.audit.exportability_not_allowed" in _codes(report)


def test_secret_reference_policy_requires_declared_credential_ref() -> None:
    report = audit_source_privacy_policy(
        "github-private",
        {
            "source_policy": {
                "sensitivity": "credentialed_external",
                "data_classification": "internal",
                "credential_ref_policy": "required",
                "live_call_allowed": True,
                "exportability": "internal",
                "retention": "normal",
                "provenance": "remote_api",
                "privacy_obligations": ["audit", "review"],
            }
        },
        manifest={
            **_manifest(),
            "nomos": {
                **_manifest()["nomos"],
                "deployment_posture": {
                    **_manifest()["nomos"]["deployment_posture"],
                    "allowed_source_sensitivities": [
                        "credentialed_external",
                        "internal",
                    ],
                },
            },
        },
    )

    assert report["ok"] is False
    assert "nomos.audit.credential_ref_required" in _codes(report)


def test_live_call_policy_cannot_override_deny_posture() -> None:
    report = audit_source_privacy_policy(
        "remote",
        {
            "source_policy": {
                "sensitivity": "internal",
                "data_classification": "internal",
                "credential_ref_policy": "optional",
                "live_call_allowed": True,
                "exportability": "internal",
                "retention": "normal",
                "provenance": "remote_api",
                "privacy_obligations": ["audit"],
            }
        },
        manifest=_manifest(live_call_default="deny"),
    )

    assert report["ok"] is False
    assert "nomos.audit.live_call_not_allowed" in _codes(report)
