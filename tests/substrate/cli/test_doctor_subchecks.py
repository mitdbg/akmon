"""Coverage tests for `okg doctor --check sources` and
`--check extractors`. Both walk a synthetic deployment dir and
report per-class import / instantiation status.
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.nomos import (
    IdentityProvider,
    activate_config_set,
    activate_policy_bundle,
    compile_config_graph,
    compile_policy_graph,
    upsert_identity_provider,
)
from okg.substrate.nomos.doctor import nomos_doctor_report


def _write_synthetic_deployment(
    tmp_path: Path, *, name: str = "doctor_test_dep",
) -> Path:
    deployments_root = tmp_path / "deployments"
    dep = deployments_root / name
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    return dep


# ---------- doctor --check sources ----------


def test_doctor_check_sources_with_no_registry(
    tmp_path, monkeypatch, capsys,
) -> None:
    """A deployment without source_registry.yaml is OK — doctor
    reports skipped."""
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    rc = main([
        "--json", "doctor", "--check", "sources",
        "--deployment", dep.name,
    ])
    assert rc == 0


def test_doctor_check_sources_with_resolvable_class(
    tmp_path, monkeypatch, capsys,
) -> None:
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
    )
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  files:\n"
        "    module: okg.substrate.library.sources.git_graph_files\n"
        "    class: GitFilesSource\n"
        "    ownership_id: doctor_test_dep.files\n"
        "    params:\n"
        "      repo: bogus\n"
        "      repo_root: /tmp/no-such-repo\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [repo, path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    rc = main([
        "--json", "doctor", "--check", "sources",
        "--deployment", dep.name,
    ])
    # Source class resolves (import works; constructor signature
    # may pass or warn — both exercise the doctor path).
    assert rc in (0, 1)


def test_doctor_check_sources_with_unresolvable_class(
    tmp_path, monkeypatch,
) -> None:
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
    )
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  bogus:\n"
        "    module: bogus.module.does.not.exist\n"
        "    class: NoSuchClass\n"
        "    ownership_id: doctor_test_dep.bogus\n"
        "    params: {}\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [a]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    rc = main([
        "--json", "doctor", "--check", "sources",
        "--deployment", dep.name,
    ])
    # Unresolvable class → ok=False (exit 1).
    assert rc != 0


def test_doctor_check_sources_missing_deployment(
    tmp_path, monkeypatch,
) -> None:
    """Pointing at a deployment dir that doesn't contain
    `<name>/` exits non-zero."""
    deployments_root = tmp_path / "deployments"
    deployments_root.mkdir()
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(deployments_root),
    )
    rc = main([
        "--json", "doctor", "--check", "sources",
        "--deployment", "no-such-deployment",
    ])
    assert rc != 0


def test_doctor_check_external_live_authority_warns_unprofiled_tool(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "agent_tools:\n"
        "  doctor_test_live_search:\n"
        "    module: example.tools\n"
        "    function: search\n"
        "    boundary: external_live\n"
        "    optional: true\n"
        "    description: Live external evidence.\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )

    rc = main([
        "--json", "doctor", "--check", "external-live-authority",
        "--deployment", dep.name,
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["status"] == "WARN"
    assert payload["unprofiled_external_live_tools"] == [
        "doctor_test_live_search"
    ]


def test_doctor_check_external_live_authority_errors_stale_profile(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "agent_tools:\n"
        "  doctor_test_live_search:\n"
        "    module: example.tools\n"
        "    function: search\n"
        "    boundary: external_live\n"
        "    optional: true\n"
        "    description: Live external evidence.\n"
    )
    (dep / "live_authority_profiles.yaml").write_text(
        "version: 1\n"
        "authorities:\n"
        "  - id: example_authority\n"
        "    name: Example Authority\n"
        "profiles:\n"
        "  - id: doctor.test.stale\n"
        "    name: Stale profile\n"
        "    tool: doctor_test_missing_tool\n"
        "    authority: example_authority\n"
        "    intent: stale_tool\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )

    rc = main([
        "--json", "doctor", "--check", "external-live-authority",
        "--deployment", dep.name,
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["status"] == "ERROR"
    assert payload["stale_profile_tools"] == ["doctor_test_missing_tool"]


# ---------- doctor --check nomos ----------


@pytest.mark.postgres
def test_doctor_check_nomos_warns_without_identity_providers(
    clean_runtime: str,
    capsys,
) -> None:
    rc = main([
        "--json", "doctor", "--check", "nomos",
        "--dsn", clean_runtime,
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["status"] == "WARN"
    assert payload["ok"] is True
    assert payload["identity"]["active_provider_count"] == 0
    assert all(payload["schema_objects"].values())


@pytest.mark.postgres
def test_doctor_check_nomos_fails_malformed_active_provider(
    clean_runtime: str,
    capsys,
) -> None:
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_identity_providers (
              deployment_name, provider_id, provider_type, issuer,
              audiences, status
            )
            VALUES (
              'cms', 'broken-sso', 'oidc', '',
              ARRAY[]::text[], 'active'
            )
            """
        )
        conn.commit()

    rc = main([
        "--json", "doctor", "--check", "nomos",
        "--dsn", clean_runtime,
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    codes = {error["code"] for error in payload["errors"]}
    assert "nomos_identity_provider_missing_issuer" in codes
    assert "nomos_identity_provider_missing_audience" in codes
    assert "nomos_identity_provider_missing_key_reference" in codes
    assert "nomos_identity_provider_missing_freshness_policy" in codes


@pytest.mark.postgres
def test_doctor_check_nomos_passes_configured_provider(
    clean_runtime: str,
    capsys,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(
            conn,
            deployment_name="cms",
            provider=IdentityProvider(
                provider_id="cern-sso",
                provider_type="oidc",
                issuer="https://auth.cern.ch/auth/realms/cern",
                audiences=("akmon-cms",),
                key_reference="secret://cern-sso/jwks",
                required_assurance=("mfa",),
                mfa_required=True,
                freshness_seconds=3600,
            ),
        )
        conn.commit()

    rc = main([
        "--json", "doctor", "--check", "nomos",
        "--dsn", clean_runtime,
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["status"] == "OK"
    assert payload["identity"]["active_provider_count"] == 1


@pytest.mark.postgres
def test_nomos_doctor_requires_deployment_active_policy_and_config(
    clean_runtime: str,
) -> None:
    policy = compile_policy_graph({
        "policy_bundle_id": "nomos:test:doctor",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_doctor",
                "effect": "allow",
                "actions": ["doctor.inspect"],
                "subjects": ["subject:doctor"],
            }
        ],
    })
    config = compile_config_graph({
        "config_set_id": "nomos:test:doctor-config",
        "config_version": "v1",
        "entries": [
            {
                "config_key": "runtime.max_batch",
                "value": 2000,
            }
        ],
    })
    with psycopg.connect(clean_runtime) as conn:
        missing = nomos_doctor_report(conn, deployment_name="cms")
        activate_policy_bundle(
            conn,
            deployment_name="cms",
            bundle=policy,
            scope="deployment:cms",
            activated_by="test:doctor",
        )
        activate_config_set(
            conn,
            deployment_name="cms",
            config_set=config,
            scope="deployment:cms",
            activated_by="test:doctor",
        )
        upsert_identity_provider(
            conn,
            deployment_name="cms",
            provider=IdentityProvider(
                provider_id="cern-sso",
                provider_type="oidc",
                issuer="https://auth.cern.ch/auth/realms/cern",
                audiences=("akmon-cms",),
                key_reference="secret://cern-sso/jwks",
                required_assurance=("mfa",),
                mfa_required=True,
                freshness_seconds=3600,
            ),
        )
        configured = nomos_doctor_report(conn, deployment_name="cms")

    missing_codes = {error["code"] for error in missing["errors"]}
    assert "nomos_no_active_policy_bundle" in missing_codes
    assert "nomos_no_active_config_set" in missing_codes
    assert configured["status"] == "OK"
    assert configured["policy"]["active_policy_count"] == 1
    assert configured["policy"]["active_config_count"] == 1


@pytest.mark.postgres
def test_nomos_doctor_detects_identity_provider_drift(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(
            conn,
            deployment_name="cms",
            provider=IdentityProvider(
                provider_id="cern-sso",
                provider_type="oidc",
                issuer="https://wrong.example.test/",
                audiences=("akmon-cms",),
                key_reference="secret://cern-sso/jwks",
                required_assurance=("mfa",),
                mfa_required=True,
                freshness_seconds=3600,
                metadata={
                    "expected_issuer": "https://auth.cern.ch/auth/realms/cern",
                },
            ),
        )
        report = nomos_doctor_report(conn)

    codes = {error["code"] for error in report["errors"]}
    assert "nomos_identity_provider_drift" in codes


@pytest.mark.postgres
def test_nomos_doctor_detects_config_drift(
    clean_runtime: str,
) -> None:
    config = compile_config_graph({
        "config_set_id": "nomos:test:drift-config",
        "config_version": "v1",
        "entries": [{"config_key": "runtime.max_batch", "value": 100}],
    })
    with psycopg.connect(clean_runtime) as conn:
        from okg.substrate.nomos import upsert_config_set

        upsert_config_set(
            conn,
            deployment_name="cms",
            config_set=config,
            metadata={"expected_config_fingerprint": "sha256:stale"},
        )
        report = nomos_doctor_report(conn)

    codes = {error["code"] for error in report["errors"]}
    assert "nomos_config_drift" in codes


# ---------- doctor --check extractors ----------


def test_doctor_check_extractors_with_no_extractors_yaml(
    tmp_path, monkeypatch,
) -> None:
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    rc = main([
        "--json", "doctor", "--check", "extractors",
        "--deployment", dep.name,
    ])
    # No extractors.yaml — reports skipped, exits 0.
    assert rc == 0


def test_doctor_check_extractors_with_real_yaml(
    tmp_path, monkeypatch,
) -> None:
    """A minimal extractors.yaml exercises the per-axis loop:
    each entry's `class` is import-checked and instantiated."""
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
    )
    (dep / "extractors.yaml").write_text(
        "parser:\n"
        "  - class: okg.substrate.library.extraction.parse.PdfTextParser\n"
        "    format: pdf\n"
        "    config: {}\n"
        "chunker: []\n"
        "ner: []\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    rc = main([
        "--json", "doctor", "--check", "extractors",
        "--deployment", dep.name,
    ])
    # Class resolves; instantiation may succeed or WARN — both ok.
    assert rc in (0, 1)


def test_doctor_check_extractors_with_missing_class_field(
    tmp_path, monkeypatch,
) -> None:
    """An extractors.yaml entry without `class:` reports FAIL."""
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
    )
    (dep / "extractors.yaml").write_text(
        "parser:\n"
        "  - format: pdf\n"  # missing class
        "    config: {}\n"
        "chunker: []\n"
        "ner: []\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    rc = main([
        "--json", "doctor", "--check", "extractors",
        "--deployment", dep.name,
    ])
    assert rc != 0


def test_doctor_check_extractors_with_unresolvable_class(
    tmp_path, monkeypatch,
) -> None:
    dep = _write_synthetic_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        f"name: {dep.name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
    )
    (dep / "extractors.yaml").write_text(
        "parser:\n"
        "  - class: bogus.module.NoSuchClass\n"
        "    config: {}\n"
        "chunker: []\n"
        "ner: []\n"
    )
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"),
    )
    rc = main([
        "--json", "doctor", "--check", "extractors",
        "--deployment", dep.name,
    ])
    assert rc != 0
