from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.runtime.openshell_policy import (
    OpenShellPolicyError,
    generate_openshell_policy,
    render_openshell_policy_yaml,
)


def _write_deployment(
    tmp_path: Path,
    *,
    registry_body: str,
    dsn: str = "postgres://okg:pw@db.example:5432/okg",
) -> Path:
    deployments = tmp_path / "deployments"
    deployment = deployments / "demo"
    deployment.mkdir(parents=True)
    (tmp_path / ".venv").mkdir()
    (deployment / "schema").mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "demo",
            "schema_dir": "schema",
            "source_registry": "source_registry.yaml",
            "postgres": {"dsn": dsn},
        }),
        encoding="utf-8",
    )
    (deployment / "source_registry.yaml").write_text(
        registry_body,
        encoding="utf-8",
    )
    return deployment


def _registry(*, endpoint: str = "https://api.example.test/v1") -> str:
    return f"""
sources:
  api:
    ownership_id: demo.api
    source_class: mutable_api
    record_identity_kind: remote_id
    record_identity_fields: [id]
    source_revision_kind: updated_at
    deletion_semantics: explicit_delete
    publication_mode: published_generation
    params:
      endpoint: {endpoint}
  feed:
    ownership_id: demo.feed
    source_class: append_feed
    record_identity_kind: event_id
    record_identity_fields: [url]
    source_revision_kind: updated_at
    deletion_semantics: append_only
    publication_mode: published_generation
    params:
      rss_url: https://feed.example.test/rss
  hosted:
    ownership_id: demo.hosted
    source_class: mutable_api
    record_identity_kind: remote_id
    record_identity_fields: [id]
    source_revision_kind: updated_at
    deletion_semantics: explicit_delete
    publication_mode: published_generation
    params:
      host: data.example.test
      port: 8443
  local:
    ownership_id: demo.local
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [path]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    params:
      root: ${{deployment_dir}}/fixtures
"""


def _registry_with_metadata() -> str:
    """A source carrying non-runtime `*_url` keys outside `params`, in
    `configuration_hints` (a legacy registry refuses an unknown key such as
    `metadata`)."""
    return """
sources:
  api:
    ownership_id: demo.api
    source_class: mutable_api
    record_identity_kind: remote_id
    record_identity_fields: [id]
    source_revision_kind: updated_at
    deletion_semantics: explicit_delete
    publication_mode: published_generation
    configuration_hints:
      documentation_url: https://docs.example.test/guide
      homepage_url: https://www.example.test
    params:
      endpoint: https://api.example.test/v1
"""


def test_openshell_policy_ignores_non_params_metadata_urls(tmp_path: Path):
    """Egress is derived only from runtime `params` endpoints; nested non-runtime
    metadata `*_url` keys must NOT become allowed sandbox egress."""
    deployment = _write_deployment(
        tmp_path, registry_body=_registry_with_metadata(),
    )
    manifest = DeploymentManifest.from_yaml(deployment / "deployment.yaml")
    env = {"VIRTUAL_ENV": str(tmp_path / ".venv")}

    payload = generate_openshell_policy(manifest, env=env)
    hosts = {
        entry["host"]
        for entry in payload["network_policies"]["okg_runtime_worker"]["endpoints"]
    }
    assert "api.example.test" in hosts        # runtime params endpoint -> allowed
    assert "docs.example.test" not in hosts   # configuration_hints.documentation_url -> excluded
    assert "www.example.test" not in hosts    # configuration_hints.homepage_url -> excluded
    assert "db.example" in hosts             # deployment Postgres DSN -> still allowed


def test_openshell_policy_contains_exact_registry_and_postgres_hosts(
    tmp_path: Path,
):
    deployment = _write_deployment(tmp_path, registry_body=_registry())
    manifest = DeploymentManifest.from_yaml(deployment / "deployment.yaml")
    env = {"VIRTUAL_ENV": str(tmp_path / ".venv")}

    first = generate_openshell_policy(manifest, env=env)
    second = generate_openshell_policy(manifest, env=env)

    assert first == second
    endpoints = first["network_policies"]["okg_runtime_worker"]["endpoints"]
    allow = {
        (entry["host"], entry["port"])
        for entry in endpoints
    }
    assert allow == {
        ("api.example.test", 443),
        ("data.example.test", 8443),
        ("db.example", 5432),
        ("feed.example.test", 443),
    }
    assert all(entry["host"] != "*" for entry in endpoints)
    rendered = render_openshell_policy_yaml(first)
    assert "host: '*'" not in rendered
    assert 'host: "*"' not in rendered
    assert first["filesystem_policy"] == {
        "read_only": [str((tmp_path / ".venv").resolve())],
        "read_write": [str(deployment.resolve())],
    }
    assert first["landlock"] == {"compatibility": "hard_requirement"}
    assert first["process"] == {
        "run_as_user": "sandbox",
        "run_as_group": "sandbox",
    }
    assert first["network_policies"]["okg_runtime_worker"]["binaries"] == [
        {"path": f"{(tmp_path / '.venv').resolve()}/bin/**"},
    ]


def test_openshell_policy_rejects_underivable_endpoint(tmp_path: Path):
    deployment = _write_deployment(
        tmp_path,
        registry_body=_registry(endpoint="${MISSING_ENDPOINT}"),
    )
    manifest = DeploymentManifest.from_yaml(deployment / "deployment.yaml")

    with pytest.raises(OpenShellPolicyError) as exc:
        generate_openshell_policy(
            manifest,
            env={"VIRTUAL_ENV": str(tmp_path / ".venv")},
        )

    assert "MISSING_ENDPOINT" in str(exc.value)
    assert "wildcard egress is never generated" in (
        exc.value.fix_hint or ""
    )


def test_runtime_openshell_policy_cli_outputs_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    _write_deployment(
        tmp_path,
        registry_body=_registry(endpoint="https://api.example.test/v1"),
        dsn="${OKG_DSN}",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / ".venv"))
    monkeypatch.delenv("OKG_DSN", raising=False)

    rc = main([
        "runtime",
        "openshell-policy",
        "--deployment",
        "demo",
        "--dsn",
        "postgres://okg:pw@override-db.example:6543/okg",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["version"] == 1
    allow = {
        (entry["host"], entry["port"])
        for entry in (
            payload["network_policies"]["okg_runtime_worker"]["endpoints"]
        )
    }
    assert ("override-db.example", 6543) in allow
    assert ("api.example.test", 443) in allow


def test_runtime_openshell_policy_cli_writes_yaml_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    _write_deployment(
        tmp_path,
        registry_body=_registry(endpoint="${OKG_DSN}"),
        dsn="${OKG_DSN}",
    )
    output = tmp_path / "policy.yaml"
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / ".venv"))
    monkeypatch.setenv(
        "OKG_DSN",
        "postgres://okg:pw@ambient-db.example:5432/okg",
    )

    rc = main([
        "runtime",
        "openshell-policy",
        "--deployment",
        "demo",
        "--dsn",
        "postgres://okg:pw@override-db.example:6543/okg",
        "--output",
        str(output),
    ])

    assert rc == 0
    stdout_payload = yaml.safe_load(capsys.readouterr().out)
    file_payload = yaml.safe_load(output.read_text(encoding="utf-8"))
    assert file_payload == stdout_payload
    endpoints = file_payload["network_policies"]["okg_runtime_worker"][
        "endpoints"
    ]
    allow = {
        (entry["host"], entry["port"])
        for entry in endpoints
    }
    assert ("override-db.example", 6543) in allow
    assert ("ambient-db.example", 5432) not in allow
    rendered = output.read_text(encoding="utf-8")
    assert "host: '*'" not in rendered
    assert 'host: "*"' not in rendered


def test_runtime_openshell_policy_resolves_registry_okg_dsn_placeholder(
    tmp_path: Path,
):
    deployment = _write_deployment(
        tmp_path,
        registry_body=_registry(endpoint="${OKG_DSN}"),
        dsn="${OKG_DSN}",
    )
    manifest = DeploymentManifest.from_yaml(deployment / "deployment.yaml")

    policy = generate_openshell_policy(
        manifest,
        env={"VIRTUAL_ENV": str(tmp_path / ".venv")},
        postgres_dsn="postgres://okg:pw@resolved-db.example:6543/okg",
    )

    endpoints = policy["network_policies"]["okg_runtime_worker"]["endpoints"]
    allow = {
        (entry["host"], entry["port"])
        for entry in endpoints
    }
    assert ("resolved-db.example", 6543) in allow


def test_runtime_openshell_policy_explicit_dsn_overrides_ambient_okg_dsn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    _write_deployment(
        tmp_path,
        registry_body=_registry(endpoint="${OKG_DSN}"),
        dsn="${OKG_DSN}",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / ".venv"))
    monkeypatch.setenv(
        "OKG_DSN",
        "postgres://okg:pw@ambient-db.example:5432/okg",
    )

    rc = main([
        "runtime",
        "openshell-policy",
        "--deployment",
        "demo",
        "--dsn",
        "postgres://okg:pw@override-db.example:6543/okg",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    endpoints = payload["network_policies"]["okg_runtime_worker"]["endpoints"]
    allow = {
        (entry["host"], entry["port"])
        for entry in endpoints
    }
    assert ("override-db.example", 6543) in allow
    assert ("ambient-db.example", 5432) not in allow


def test_openshell_policy_rejects_host_value_with_invalid_port(
    tmp_path: Path,
):
    deployment = _write_deployment(
        tmp_path,
        registry_body="""
sources:
  hosted:
    ownership_id: demo.hosted
    source_class: mutable_api
    record_identity_kind: remote_id
    record_identity_fields: [id]
    source_revision_kind: updated_at
    deletion_semantics: explicit_delete
    publication_mode: published_generation
    params:
      host: data.example.test:notaport
""",
    )
    manifest = DeploymentManifest.from_yaml(deployment / "deployment.yaml")

    with pytest.raises(OpenShellPolicyError) as exc:
        generate_openshell_policy(
            manifest,
            env={"VIRTUAL_ENV": str(tmp_path / ".venv")},
        )

    assert "Port could not be cast" in str(exc.value)
    assert "wildcard egress is never generated" in (
        exc.value.fix_hint or ""
    )
