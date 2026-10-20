"""PACT bind-at-rest-anonymization -- req.source-posture.

The at-rest posture rides the per-source ``source_policy``: an explicit
``store_raw`` boolean plus an optional ``pii_classes`` classifier map. This proves
the Nomos deployment audit validates both when present, that implicit raw storage
is limited to a bounded valid deferral, and that the runtime resolver
(:func:`resolve_source_pii_posture`) turns the policy into a :class:`PiiPosture`
fail-safe and fail-loud.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.ingest.pii_posture import (
    PiiPosture,
    resolve_deployment_source_pii_posture,
    resolve_source_pii_posture,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.nomos import audit_source_privacy_policy
from okg.substrate.nomos.pii_classification import resolve_pii_class
from okg.substrate.nomos.rollout import (
    deployment_dsn_key,
    enforce_mode_dsn_keys,
    runtime_enforce_mode_dsn_keys,
)

_KEY = "0123456789abcdef0123456789abcdef"  # 32 bytes >= MIN_KEY_BYTES


def _manifest() -> dict:
    """A permissive deployment posture so only store_raw/pii_classes findings surface."""
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
                "live_call_default": "review",
                "allowed_source_sensitivities": ["local_private", "restricted"],
                "allowed_data_classifications": ["private", "restricted"],
                "allowed_exportability": ["internal", "no_export"],
                "privacy_obligations": ["audit", "review"],
                "readiness_profiles": ["dogfood"],
            },
        },
    }


def _entry(**policy_extra) -> dict:
    """A source entry whose source_policy carries the required fields + extras."""
    policy = {
        "sensitivity": "restricted",
        "data_classification": "restricted",
        "credential_ref_policy": "none",
        "live_call_allowed": False,
        "exportability": "no_export",
        "retention": "normal",
        "provenance": "declared",
        "privacy_obligations": ["audit"],
    }
    policy.update(policy_extra)
    return {"source_policy": policy}


def _codes(report: dict) -> set[str]:
    return {finding["reason_code"] for finding in report["findings"]}


def _deferred_manifest(**rollout_overrides) -> dict:
    rollout = {
        "status": "deferred",
        "owner": {"name": "bench-owner", "contact": "bench@okg.example"},
        "review_by": "2099-01-01",
    }
    rollout.update(rollout_overrides)
    return {
        "name": "bench",
        "postgres": {"dsn": "${OKG_DSN}", "database": "bench"},
        "nomos": {"rollout": rollout},
    }


# --- Nomos audit validation --------------------------------------------------


def test_audit_preserves_omitted_store_raw_for_runtime_decision() -> None:
    report = audit_source_privacy_policy("src", _entry(), manifest=_manifest())
    assert "nomos.audit.invalid_store_raw" not in _codes(report)
    # Omission remains visible so the runtime can require a decision or validate
    # the deployment's bounded compatibility deferral.
    assert "store_raw" not in report["source_policy"]


def test_valid_store_raw_false_normalizes() -> None:
    report = audit_source_privacy_policy(
        "src", _entry(store_raw=False), manifest=_manifest()
    )
    assert "nomos.audit.invalid_store_raw" not in _codes(report)
    assert report["source_policy"]["store_raw"] is False


def test_non_bool_store_raw_is_flagged() -> None:
    report = audit_source_privacy_policy(
        "src", _entry(store_raw="yes"), manifest=_manifest()
    )
    assert "nomos.audit.invalid_store_raw" in _codes(report)


def test_valid_pii_classes_normalizes() -> None:
    report = audit_source_privacy_policy(
        "src",
        _entry(
            store_raw=False,
            pii_classes={"email": "direct_identifier", "kind": "non_pii"},
        ),
        manifest=_manifest(),
    )
    assert "nomos.audit.unknown_pii_class" not in _codes(report)
    assert report["source_policy"]["pii_classes"]["email"] == "direct_identifier"


def test_unknown_pii_class_is_flagged() -> None:
    report = audit_source_privacy_policy(
        "src",
        _entry(store_raw=False, pii_classes={"email": "not_a_class"}),
        manifest=_manifest(),
    )
    assert "nomos.audit.unknown_pii_class" in _codes(report)


def test_non_mapping_pii_classes_is_flagged() -> None:
    report = audit_source_privacy_policy(
        "src",
        _entry(store_raw=False, pii_classes=["email"]),
        manifest=_manifest(),
    )
    assert "nomos.audit.invalid_pii_classes" in _codes(report)


# --- Runtime posture resolution ---------------------------------------------


def test_direct_resolution_without_explicit_store_raw_fails_closed() -> None:
    with pytest.raises(SubstrateError, match="explicitly declare store_raw"):
        resolve_source_pii_posture(source_policy=None)


def test_nondeferred_deployment_without_policy_fails_closed() -> None:
    with pytest.raises(SubstrateError, match="failed Nomos audit"):
        resolve_deployment_source_pii_posture(
            manifest=_manifest(),
            source_name="src",
            source_entry={"source_class": "discovery_crawl"},
            env={},
            build_detector=False,
        )


def test_valid_deferred_deployment_without_posture_resolves_store_raw() -> None:
    posture = resolve_deployment_source_pii_posture(
        manifest=_deferred_manifest(),
        source_name="src",
        source_entry={"source_class": "discovery_crawl"},
        env={},
        build_detector=False,
        enforce_dsn_keys={},
    )
    assert posture.active is False


def test_deferred_deployment_without_portfolio_authority_fails_closed() -> None:
    with pytest.raises(SubstrateError, match="failed Nomos audit"):
        resolve_deployment_source_pii_posture(
            manifest=_deferred_manifest(),
            source_name="src",
            source_entry={"source_class": "discovery_crawl"},
            env={},
            build_detector=False,
        )


def test_deferred_deployment_sharing_enforced_database_fails_closed() -> None:
    manifest = _deferred_manifest()
    key = deployment_dsn_key(manifest)
    assert key is not None
    with pytest.raises(SubstrateError, match="failed Nomos audit"):
        resolve_deployment_source_pii_posture(
            manifest=manifest,
            source_name="src",
            source_entry={"source_class": "discovery_crawl"},
            env={},
            build_detector=False,
            enforce_dsn_keys={"enforced-deployment": key},
        )


@pytest.mark.parametrize("incomplete_kind", ["malformed", "enforce_without_dsn"])
def test_incomplete_runtime_portfolio_authority_fails_closed(
    tmp_path: Path,
    incomplete_kind: str,
) -> None:
    root = tmp_path / "deployments"
    deferred_dir = root / "bench"
    deferred_dir.mkdir(parents=True)
    (deferred_dir / "deployment.yaml").write_text(
        yaml.safe_dump(_deferred_manifest()),
        encoding="utf-8",
    )
    uncertain_dir = root / "uncertain"
    uncertain_dir.mkdir()
    uncertain_manifest = uncertain_dir / "deployment.yaml"
    if incomplete_kind == "malformed":
        uncertain_manifest.write_text("nomos: [", encoding="utf-8")
    else:
        uncertain_manifest.write_text(
            yaml.safe_dump({
                "name": "uncertain",
                "nomos": {"runtime_enforcement": "enforce"},
            }),
            encoding="utf-8",
        )

    authority = runtime_enforce_mode_dsn_keys(deferred_dir)
    assert authority is None
    with pytest.raises(SubstrateError, match="failed Nomos audit"):
        resolve_deployment_source_pii_posture(
            manifest=_deferred_manifest(),
            source_name="src",
            source_entry={"source_class": "discovery_crawl"},
            env={},
            build_detector=False,
            enforce_dsn_keys=authority,
        )


@pytest.mark.parametrize(
    "rollout",
    [
        {"status": "deferred"},
        {
            "status": "deferred",
            "owner": {"name": "bench-owner", "contact": "bench@okg.example"},
            "review_by": "2000-01-01",
        },
    ],
)
def test_invalid_or_expired_deferred_rollout_cannot_authorize_raw(rollout) -> None:
    with pytest.raises(SubstrateError, match="failed Nomos audit"):
        resolve_deployment_source_pii_posture(
            manifest={
                "name": "bench",
                "postgres": {"dsn": "${OKG_DSN}", "database": "bench"},
                "nomos": {"rollout": rollout},
            },
            source_name="src",
            source_entry={"source_class": "discovery_crawl"},
            env={},
            build_detector=False,
            enforce_dsn_keys={},
        )


def test_nondeferred_complete_policy_must_explicitly_decide_store_raw() -> None:
    with pytest.raises(SubstrateError, match="explicitly declare store_raw"):
        resolve_deployment_source_pii_posture(
            manifest=_manifest(),
            source_name="src",
            source_entry=_entry(),
            env={},
            build_detector=False,
        )


def test_store_raw_true_resolves_noop_without_vault() -> None:
    # store_raw=True must NOT require a vault key (the default path stays cheap).
    posture = resolve_source_pii_posture(
        source_policy={"store_raw": True}, env={}
    )
    assert posture.active is False
    assert posture.pseudonym_service is None


def test_not_store_raw_resolves_active_posture() -> None:
    posture = resolve_source_pii_posture(
        source_policy={
            "store_raw": False,
            "pii_classes": {"email": "direct_identifier", "kind": "non_pii"},
        },
        env={"OKG_PSEUDONYM_HMAC_KEY": _KEY},
        build_detector=False,
    )
    assert posture.active is True
    assert posture.pseudonym_service is not None
    # classifier reflects the declared map; an undeclared key fails safe.
    assert posture.classifier("email") == "direct_identifier"
    assert posture.classifier("kind") == "non_pii"
    assert resolve_pii_class(posture.classifier("undeclared")) == "sensitive_attribute"


def test_not_store_raw_without_vault_key_fails_loud() -> None:
    from okg.substrate.alias.pseudonymize import PseudonymKeyUnavailable

    # No silent store-raw fallback: a not-store-raw source with no key MUST raise.
    with pytest.raises(PseudonymKeyUnavailable):
        resolve_source_pii_posture(
            source_policy={"store_raw": False}, env={}, build_detector=False
        )


def test_malformed_store_raw_fails_loud() -> None:
    for malformed in ("sometimes", "true", "false"):
        with pytest.raises(ValueError, match="store_raw must be a boolean"):
            resolve_source_pii_posture(
                source_policy={"store_raw": malformed},
                env={"OKG_PSEUDONYM_HMAC_KEY": _KEY},
                build_detector=False,
            )


@pytest.mark.parametrize(
    ("defaults_key", "defaults_name"),
    [("by_source_name", "src"), ("by_source_class", "discovery_crawl")],
)
def test_runtime_uses_same_deployment_default_as_audit(
    defaults_key: str, defaults_name: str
) -> None:
    manifest = _manifest()
    manifest["nomos"]["source_policy_defaults"] = {
        defaults_key: {defaults_name: _entry(store_raw=False)["source_policy"]}
    }
    posture = resolve_deployment_source_pii_posture(
        manifest=manifest,
        source_name="src",
        source_entry={"source_class": "discovery_crawl"},
        env={"OKG_PSEUDONYM_HMAC_KEY": _KEY},
        build_detector=False,
    )
    assert posture.active is True


def test_runtime_policy_precedence_inline_then_name_then_class() -> None:
    manifest = _manifest()
    manifest["nomos"]["source_policy_defaults"] = {
        "by_source_name": {"src": _entry(store_raw=True)["source_policy"]},
        "by_source_class": {
            "discovery_crawl": _entry(store_raw=False)["source_policy"]
        },
    }
    named = resolve_deployment_source_pii_posture(
        manifest=manifest,
        source_name="src",
        source_entry={"source_class": "discovery_crawl"},
        env={},
        build_detector=False,
    )
    assert named.active is False

    inline_entry = _entry(store_raw=False)
    inline_entry["source_class"] = "discovery_crawl"
    inline = resolve_deployment_source_pii_posture(
        manifest=manifest,
        source_name="src",
        source_entry=inline_entry,
        env={"OKG_PSEUDONYM_HMAC_KEY": _KEY},
        build_detector=False,
    )
    assert inline.active is True


def test_repository_deployments_resolve_every_registered_source_explicitly() -> None:
    repo = Path(__file__).resolve().parents[3]
    enforce_keys = enforce_mode_dsn_keys(repo / "deployments")
    resolved: list[tuple[str, str]] = []
    for deployment_dir in sorted((repo / "deployments").iterdir()):
        manifest_path = deployment_dir / "deployment.yaml"
        if not manifest_path.is_file():
            continue
        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        registry_path = deployment_dir / str(
            manifest.get("source_registry") or "source_registry.yaml"
        )
        registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
        for source_name, source_entry in (registry.get("sources") or {}).items():
            resolve_deployment_source_pii_posture(
                manifest=manifest,
                source_name=str(source_name),
                source_entry=source_entry,
                env={"OKG_PSEUDONYM_HMAC_KEY": _KEY},
                build_detector=False,
                enforce_dsn_keys=enforce_keys,
            )
            resolved.append((str(manifest["name"]), str(source_name)))
    assert resolved


def test_runtime_rejects_policy_the_audit_rejects() -> None:
    with pytest.raises(Exception, match="failed Nomos audit"):
        resolve_deployment_source_pii_posture(
            manifest=_manifest(),
            source_name="src",
            source_entry=_entry(store_raw="true"),
            env={"OKG_PSEUDONYM_HMAC_KEY": _KEY},
            build_detector=False,
        )


def test_direct_deployment_runner_resolves_registered_default(
    tmp_path, monkeypatch
) -> None:
    import yaml

    from okg.substrate.ingest.runner import SourceRunner

    manifest = _manifest()
    manifest["nomos"]["source_policy_defaults"] = {
        "by_source_name": {
            "src": _entry(store_raw=False)["source_policy"],
        }
    }
    (tmp_path / "deployment.yaml").write_text(
        yaml.safe_dump(manifest), encoding="utf-8"
    )
    (tmp_path / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                "src": {
                    "ownership_id": "runtime.src",
                    "source_class": "discovery_crawl",
                }
            }
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_PSEUDONYM_HMAC_KEY", _KEY)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _name: tmp_path,
    )
    runner = SourceRunner(
        "postgresql://unused/unused",
        source_name="runtime.src",
        deployment_name="test-deployment",
    )
    runner._resolve_registered_pii_posture_if_available()
    assert runner.pii_posture is not None and runner.pii_posture.active


def test_posture_default_classifier_fails_safe() -> None:
    # A not-store-raw posture constructed WITHOUT an explicit classifier must mask
    # undeclared attributes (fail-safe), never reveal them via the fail-open legacy
    # classifier. Guards against a mis-constructed active posture leaking raw.
    posture = PiiPosture(store_raw=False)
    assert posture.active is True
    assert resolve_pii_class(posture.classifier("owner_email")) == "sensitive_attribute"
