from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
import pytest

from okg.substrate.catalog.profile import load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    scaffold_deployment,
)
from okg.substrate.nomos import audit_deployment_policy


REPO_ROOT = Path(__file__).resolve().parents[3]
PROFILES_DIR = REPO_ROOT / "src/okg/substrate/library/profiles"


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _posture(deployment_class: str = "personal_okg") -> dict:
    return {
        "deployment_class": deployment_class,
        "read_principals": ["human_operator", "okg_service"],
        "write_principals": ["human_operator", "source_adapter"],
        "mcp_write_default": "review",
        "export_policy": "no_export",
        "secret_handling": "secret_ref_required",
        "live_call_default": "review",
        "allowed_source_sensitivities": ["public", "local_private"],
        "allowed_data_classifications": ["public", "private"],
        "allowed_exportability": ["internal", "no_export"],
        "privacy_obligations": ["audit", "review", "no_export"],
        "readiness_profiles": ["personal"],
    }


def _complete_posture_for_class(deployment_class: str) -> dict:
    posture = _posture(deployment_class)
    posture.update({
        "export_policy": "internal_only",
        "secret_handling": "secret_ref_required",
        "allowed_source_sensitivities": [
            "public",
            "internal",
            "local_private",
            "credentialed_external",
            "private",
            "restricted",
            "regulated_metadata",
            "phi_synthetic",
        ],
        "allowed_data_classifications": [
            "public",
            "internal",
            "private",
            "restricted",
            "confidential",
            "regulated_metadata",
            "synthetic_phi",
        ],
        "allowed_exportability": ["internal", "no_export"],
        "privacy_obligations": [
            "audit",
            "review",
            "no_export",
            "redaction",
            "secret_ref_required",
            "source_retention",
        ],
        "readiness_profiles": ["dogfood"],
    })
    if deployment_class == "ci_authority":
        posture.update({
            "mcp_write_default": "deny",
            "read_principals": ["ci_runner", "okg_service"],
            "write_principals": ["ci_runner", "okg_service"],
            "readiness_profiles": ["ci"],
        })
    elif deployment_class == "personal_okg":
        posture["export_policy"] = "no_export"
        posture["allowed_exportability"] = ["no_export"]
        posture["readiness_profiles"] = ["personal"]
    elif deployment_class == "public_reference_demo":
        posture.update({
            "export_policy": "public_allowed",
            "secret_handling": "no_secrets",
            "live_call_default": "deny",
            "allowed_source_sensitivities": ["public"],
            "allowed_data_classifications": ["public"],
            "allowed_exportability": ["public"],
            "privacy_obligations": ["audit"],
            "readiness_profiles": ["demo"],
        })
    elif deployment_class == "synthetic_healthcare_demo":
        posture.update({
            "allowed_source_sensitivities": [
                "public",
                "internal",
                "phi_synthetic",
            ],
            "allowed_data_classifications": [
                "public",
                "internal",
                "synthetic_phi",
            ],
            "readiness_profiles": ["demo"],
        })
    elif deployment_class == "governed_research_metadata":
        posture["readiness_profiles"] = ["research"]
    elif deployment_class == "org_operational_private":
        posture["readiness_profiles"] = ["org-private"]
    elif deployment_class == "private_product_engineering":
        posture["readiness_profiles"] = ["product-engineering"]
    return posture


@pytest.mark.parametrize("deployment_class", [
    "ci_authority",
    "dogfood_workspace",
    "personal_okg",
    "public_reference_demo",
    "synthetic_healthcare_demo",
    "governed_research_metadata",
    "org_operational_private",
    "private_product_engineering",
])
def test_all_deployment_classes_accept_complete_postures(
    deployment_class: str,
) -> None:
    report = audit_deployment_policy({
        "name": deployment_class,
        "nomos": {
            "deployment_posture": _complete_posture_for_class(
                deployment_class
            ),
        },
    })

    assert report["ok"] is True, report["findings"]
    assert report["deployment_posture"]["deployment_class"] == (
        deployment_class
    )


def test_real_okg_ci_and_workspace_manifests_have_nomos_posture() -> None:
    cases = {
        "okg-ci": "ci_authority",
        "okg-workspace": "dogfood_workspace",
    }
    for deployment, deployment_class in cases.items():
        deployment_dir = REPO_ROOT / "deployments" / deployment
        report = audit_deployment_policy(
            _load_yaml(deployment_dir / "deployment.yaml"),
            source_registry=_load_yaml(deployment_dir / "source_registry.yaml"),
            deployment=deployment,
        )

        assert report["ok"] is True, report["findings"]
        assert report["deployment_posture"]["deployment_class"] == (
            deployment_class
        )
        assert report["source_policy_count"] > 0


def test_personal_okg_profile_defaults_audit_cleanly() -> None:
    defaults = _load_yaml(
        REPO_ROOT
        / "src/okg/substrate/library/profiles/academic-researcher"
        / "deployment-defaults.yaml"
    )
    registry = {
        "sources": {
            "paper_pdf_inbox": {"source_class": "discovery_crawl"},
            "agent_sessions": {"source_class": "discovery_crawl"},
        }
    }

    report = audit_deployment_policy(
        {"name": "personal-okg", **defaults},
        source_registry=registry,
        deployment="personal-okg",
    )

    assert report["ok"] is True, report["findings"]
    assert report["deployment_posture"]["deployment_class"] == "personal_okg"


def test_missing_deployment_posture_fails_with_nomos_reason_code() -> None:
    report = audit_deployment_policy({"name": "missing-posture"})

    assert report["ok"] is False
    assert report["findings"][0]["reason_code"] == (
        "nomos.audit.deployment_posture_missing"
    )


def test_malformed_posture_lists_fail() -> None:
    posture = _complete_posture_for_class("dogfood_workspace")
    posture["read_principals"] = "human_operator"
    posture["allowed_source_sensitivities"] = [
        "local_private",
        7,
    ]

    report = audit_deployment_policy({
        "name": "bad-lists",
        "nomos": {"deployment_posture": posture},
    })

    codes = {finding["reason_code"] for finding in report["findings"]}
    assert "nomos.audit.invalid_list" in codes


def test_unknown_deployment_class_is_rejected() -> None:
    report = audit_deployment_policy({
        "name": "bad-class",
        "nomos": {
            "deployment_posture": _posture("one_big_shared_graph"),
        },
    })

    codes = {finding["reason_code"] for finding in report["findings"]}
    assert "nomos.audit.unknown_deployment_class" in codes


def test_public_demo_rejects_private_source_allowance() -> None:
    posture = _complete_posture_for_class("public_reference_demo")
    posture["allowed_source_sensitivities"] = ["public", "private"]
    report = audit_deployment_policy({
        "name": "bad-public-demo",
        "nomos": {"deployment_posture": posture},
    })

    codes = {finding["reason_code"] for finding in report["findings"]}
    assert "nomos.audit.deployment_posture_weaker_than_class" in codes


# ---------------------------------------------------------------------------
# Every library profile that ships a deployment_posture must also ship
# source_policy_defaults that cover its bundled sources. A governed-class posture
# turns ON the source-registry audit, so a missing default surfaces as
# `nomos.audit.source_policy_missing` at deploy/readiness time. This sweep
# scaffolds each profile and audits the REAL bundled source registry, so the gap
# fails in CI rather than at deploy time.
# ---------------------------------------------------------------------------


def _library_profiles_with_defaults() -> list[Path]:
    return sorted(
        p
        for p in PROFILES_DIR.iterdir()
        if p.is_dir()
        and not p.name.startswith("_")
        and (p / "deployment-defaults.yaml").is_file()
    )


def _probe_answers(profile, name: str) -> dict:
    """Generic answers satisfying each profile's required init questions."""
    out: dict = {"deployment_name": name}
    for q in profile.init_questions:
        if q.id in out or (not q.required and q.default is not None):
            continue
        if q.type == "integer":
            out[q.id] = q.default or 5430
        elif q.type == "path":
            out[q.id] = "/tmp/probe_path"
        elif q.type == "path_list":
            out[q.id] = ["/tmp/probe_repo"]
        elif q.type == "orcid":
            out[q.id] = "0000-0001-2345-678X"
        elif "dsn" in q.id:
            out[q.id] = f"postgres://x@localhost:5433/{name}"
        else:
            out[q.id] = f"probe_{q.id}"
    return out


@pytest.mark.parametrize(
    "profile_dir",
    _library_profiles_with_defaults(),
    ids=lambda p: p.name,
)
def test_library_profile_scaffold_audits_cleanly(profile_dir: Path) -> None:
    """A deployment scaffolded from each library profile must pass the Nomos
    policy audit with every bundled source resolving a source_policy (no
    nomos.audit.source_policy_missing)."""
    profile = load_profile(profile_dir / "profile.yaml")
    slug = profile_dir.name.replace("-", "_")
    with tempfile.TemporaryDirectory() as td:
        plan = build_plan(
            profile=profile,
            deployment_name=slug,
            deployments_root=Path(td),
            raw_answers=_probe_answers(profile, slug),
        )
        result = scaffold_deployment(plan)
        manifest = _load_yaml(result.deployment_dir / "deployment.yaml")
        registry = _load_yaml(result.deployment_dir / "source_registry.yaml")

    report = audit_deployment_policy(
        manifest, source_registry=registry, deployment=profile_dir.name
    )

    missing = [
        f
        for f in report["findings"]
        if f["reason_code"] == "nomos.audit.source_policy_missing"
    ]
    assert not missing, (
        f"{profile_dir.name}: {len(missing)} bundled source(s) lack a "
        "source_policy default: "
        + ", ".join(sorted(f.get("source_name", "?") for f in missing))
    )
    # Every scaffolded source resolved a policy, and the audit is fully clean.
    n_sources = len((registry or {}).get("sources") or {})
    assert report["source_policy_count"] == n_sources
    assert report["ok"] is True, report["findings"]
