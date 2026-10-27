from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.deployments.environment import (
    definition_substance,
    load_environment,
    load_environment_fleet,
    require_resource_isolation,
    resource_isolation_report,
    shared_definition_substance_problems,
)
from tests.mirror_scope import private_path


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "deployment_environments"


def _environment_values(state_root: Path) -> dict[str, str]:
    return {
        "TEST_STATE_ROOT": str(state_root),
        "TEST_STAGING_DSN": "postgresql://postgres:test@127.0.0.1:55435/fixture_staging",
        "TEST_PRODUCTION_DSN": "postgresql://postgres:test@127.0.0.1:55434/fixture_production",
    }


def _copy_fixtures(tmp_path: Path) -> Path:
    target = tmp_path / "environments"
    shutil.copytree(FIXTURES, target)
    return target


def _load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_yaml(path: Path, value: dict) -> None:
    path.write_text(
        yaml.safe_dump(value, sort_keys=False),
        encoding="utf-8",
    )


def test_composition_shares_one_definition_and_keeps_environment_values(
    tmp_path: Path,
) -> None:
    values = _environment_values(tmp_path / "state")

    staging = load_environment(
        "fixture-staging",
        environments_dir=FIXTURES,
        environ=values,
    )
    production = load_environment(
        "fixture-production",
        environments_dir=FIXTURES,
        environ=values,
    )

    assert staging.definition_digest == production.definition_digest
    assert staging.definition_path == production.definition_path
    assert staging.descriptor_digest != production.descriptor_digest
    assert staging.manifest.name == "fixture-graph-staging"
    assert production.manifest.name == "fixture-graph"
    assert staging.manifest.postgres_dsn == values["TEST_STAGING_DSN"]
    assert production.manifest.postgres_dsn == values["TEST_PRODUCTION_DSN"]
    assert staging.database.name == "fixture_staging"
    assert production.database.name == "fixture_production"


def test_composition_arms_reset_guard_from_environment_tier(
    tmp_path: Path,
) -> None:
    # #682 regression: the environment tier must map onto the composed
    # manifest's `runtime.environment`, so a staging/production environment
    # self-arms the `okg runtime reset` refusal (which rejects any
    # non-"development" environment) without a per-manifest edit, while a
    # development environment stays resettable.
    values = _environment_values(tmp_path / "state")

    staging = load_environment(
        "fixture-staging",
        environments_dir=FIXTURES,
        environ=values,
    )
    production = load_environment(
        "fixture-production",
        environments_dir=FIXTURES,
        environ=values,
    )

    assert staging.tier == "staging"
    assert staging.manifest.runtime.environment == "staging"
    assert production.tier == "production"
    assert production.manifest.runtime.environment == "production"


def test_composition_anchors_definition_assets_to_definition_directory(
    tmp_path: Path,
) -> None:
    environment = load_environment(
        FIXTURES / "fixture-staging" / "environment.yaml",
        environ=_environment_values(tmp_path / "state"),
    )
    definition_dir = FIXTURES / "shared-definition"

    assert environment.manifest.deployment_dir == definition_dir
    assert environment.manifest.schema_dir == definition_dir / "schemas"
    assert environment.manifest.source_registry == (
        definition_dir / "source_registry.yaml"
    )
    assert environment.runtime.state_root == (
        tmp_path / "state" / "environments" / "fixture-staging"
    )
    assert environment.readiness.evidence_path == (
        environment.runtime.state_root / "readiness" / "current.json"
    )
    assert environment.path_environment == {
        "TEST_STATE_ROOT": str(tmp_path / "state")
    }


def test_composition_carries_checkpoint_attestor_authority(
    tmp_path: Path,
) -> None:
    environments = _copy_fixtures(tmp_path)
    definition = environments / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw["checkpoint_attestor_root"] = "."
    raw["checkpoint_attestors"] = [
        {
            "id": "tests.sealed-receipt",
            "class": "checkpoint_attestors.sample.SampleAttestor",
            "config": {"label": "sealed"},
        }
    ]
    _write_yaml(definition, raw)

    environment = load_environment(
        environments / "fixture-staging" / "environment.yaml",
        environ=_environment_values(tmp_path / "state"),
    )

    assert environment.manifest.checkpoint_attestors == raw["checkpoint_attestors"]
    assert environment.manifest.checkpoint_attestor_root == definition.parent
    assert environment.manifest.effective_raw["checkpoint_attestors"] == raw[
        "checkpoint_attestors"
    ]


def test_checkpoint_attestor_implementation_changes_definition_digest(
    tmp_path: Path,
) -> None:
    environments = _copy_fixtures(tmp_path)
    definition_root = environments / "shared-definition"
    definition = definition_root / "definition.yaml"
    package = definition_root / "checkpoint_attestors"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    implementation = package / "sample.py"
    implementation.write_text("VERSION = 1\n", encoding="utf-8")
    raw = _load_yaml(definition)
    raw["checkpoint_attestor_root"] = "."
    raw["checkpoint_attestors"] = [
        {
            "id": "tests.sealed-receipt",
            "class": "checkpoint_attestors.sample.SampleAttestor",
            "config": {},
        }
    ]
    _write_yaml(definition, raw)
    values = _environment_values(tmp_path / "state")

    before = load_environment(
        "fixture-staging",
        environments_dir=environments,
        environ=values,
    )
    implementation.write_text("VERSION = 2\n", encoding="utf-8")
    after = load_environment(
        "fixture-staging",
        environments_dir=environments,
        environ=values,
    )

    assert before.definition_digest != after.definition_digest


def test_loader_rejects_attestor_import_from_outside_implementation_root(
    tmp_path: Path,
) -> None:
    from okg.substrate.publisher.checkpoint_attestations import (
        CheckpointAttestationConfigurationError,
        load_checkpoint_attestors,
    )

    implementation_root = tmp_path / "selected-implementation"
    implementation_root.mkdir()
    side_effect = tmp_path / "outside-module-executed"
    sibling_package = tmp_path / "checkpoint_attestors"
    sibling_package.mkdir()
    (sibling_package / "__init__.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(side_effect)!r}).write_text('executed', encoding='utf-8')\n",
        encoding="utf-8",
    )
    (sibling_package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'tests.outside-root'\n"
        "    config_schema = {'type': 'object', 'properties': {}, "
        "'required': [], 'additionalProperties': False}\n"
        "    def __init__(self, *, deployment_root): pass\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )

    with pytest.raises(
        CheckpointAttestationConfigurationError,
        match="has no regular local module beneath",
    ):
        load_checkpoint_attestors(
            implementation_root,
            deployment_name="sample",
            declarations=[
                {
                    "id": "tests.outside-root",
                    "class": "checkpoint_attestors.sample.SampleAttestor",
                    "config": {},
                }
            ],
            implementation_root=implementation_root,
        )
    assert not side_effect.exists()


def test_loader_requires_existing_attestor_implementation_root(
    tmp_path: Path,
) -> None:
    from okg.substrate.publisher.checkpoint_attestations import (
        CheckpointAttestationConfigurationError,
        load_checkpoint_attestors,
    )

    authority_root = tmp_path / "definition"
    authority_root.mkdir()

    with pytest.raises(
        CheckpointAttestationConfigurationError,
        match="implementation root is missing or not a directory",
    ):
        load_checkpoint_attestors(
            authority_root,
            deployment_name="sample",
            declarations=[],
            implementation_root=tmp_path / "missing",
        )


def test_sealed_stack_uses_definition_attestors_without_legacy_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import DeploymentResolutionError
    from okg.substrate.publisher.deployment_stack import _load_checkpoint_attestors

    environments = _copy_fixtures(tmp_path)
    definition_root = environments / "shared-definition"
    implementation_root = environments / "attestor-implementation"
    package = implementation_root / "checkpoint_attestors"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sample.py").write_text(
        "class SampleAttestor:\n"
        "    attestor_id = 'tests.sealed'\n"
        "    config_schema = {\n"
        "        'type': 'object',\n"
        "        'properties': {'label': {'type': 'string'}},\n"
        "        'required': ['label'],\n"
        "        'additionalProperties': False,\n"
        "    }\n"
        "    def __init__(self, *, deployment_root, label):\n"
        "        self.deployment_root = deployment_root\n"
        "        self.label = label\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    definition = definition_root / "definition.yaml"
    base_definition = definition.read_text(encoding="utf-8")
    definition.write_text(
        base_definition
        + "checkpoint_attestor_root: ../attestor-implementation\n"
        + "checkpoint_attestors:\n"
        + "  - id: tests.sealed\n"
        + "    class: checkpoint_attestors.sample.SampleAttestor\n"
        + "    config:\n"
        + "      label: sealed-definition\n",
        encoding="utf-8",
    )
    (definition_root / "deployment.yaml").write_text(
        "name: fixture-graph-staging\n"
        "checkpoint_attestors:\n"
        "  - id: tests.legacy\n"
        "    class: checkpoint_attestors.sample.SampleAttestor\n"
        "    config:\n"
        "      label: legacy-manifest\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TEST_STATE_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv(
        "TEST_STAGING_DSN",
        "postgresql://postgres:test@127.0.0.1:55435/fixture_staging",
    )
    monkeypatch.setenv(
        "OKG_ENVIRONMENT_DESCRIPTOR",
        str(environments / "fixture-staging" / "environment.yaml"),
    )
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(tmp_path / "sealed-release"))
    monkeypatch.setenv("OKG_DEPLOYMENT", "fixture-graph-staging")

    loaded = _load_checkpoint_attestors(
        definition_root,
        deployment_name="fixture-graph-staging",
    )

    assert [item.manifest_id for item in loaded] == ["tests.sealed"]
    assert loaded[0].attestor.label == "sealed-definition"
    assert loaded[0].attestor.deployment_root == implementation_root.resolve()

    definition.write_text(base_definition, encoding="utf-8")
    assert _load_checkpoint_attestors(
        definition_root,
        deployment_name="fixture-graph-staging",
    ) == []

    sealed_markers = (
        "OKG_ACTIVE_RELEASE_POINTER",
        "OKG_ENVIRONMENT_DESCRIPTOR",
        "OKG_ENVIRONMENT_ID",
        "OKG_RELEASE_ARTIFACT_ID",
        "OKG_RELEASE_INPUT_ID",
        "OKG_RELEASE_ROOT",
    )
    for marker in sealed_markers:
        for sealed_name in sealed_markers:
            monkeypatch.delenv(sealed_name, raising=False)
        monkeypatch.setenv(marker, "partial-sealed-authority")
        with pytest.raises(DeploymentResolutionError, match="incomplete"):
            _load_checkpoint_attestors(
                definition_root,
                deployment_name="fixture-graph-staging",
            )


@pytest.mark.parametrize(
    "declarations, match",
    [
        ({}, "must be a list"),
        (
            [
                {"id": "tests.duplicate", "class": "attestors.one.A", "config": {}},
                {"id": "tests.duplicate", "class": "attestors.two.B", "config": {}},
            ],
            "duplicate checkpoint attestor id",
        ),
        (
            [
                {
                    "id": "tests.reserved",
                    "class": "attestors.one.A",
                    "config": {"dsn": "forbidden"},
                }
            ],
            "publisher authority",
        ),
    ],
)
def test_definition_rejects_invalid_checkpoint_attestor_authority(
    declarations: object,
    match: str,
) -> None:
    definition = FIXTURES / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw["checkpoint_attestor_root"] = "."
    raw["checkpoint_attestors"] = declarations

    from okg.substrate.deployments.environment import _validate_definition

    with pytest.raises(DeploymentConfigError, match=match):
        _validate_definition(raw, definition_path=definition)


def test_definition_requires_explicit_checkpoint_attestor_root() -> None:
    definition = FIXTURES / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw["checkpoint_attestors"] = [
        {
            "id": "tests.sealed-receipt",
            "class": "attestors.sample.SampleAttestor",
            "config": {},
        }
    ]

    from okg.substrate.deployments.environment import _validate_definition

    with pytest.raises(
        DeploymentConfigError,
        match="require an explicit checkpoint_attestor_root",
    ):
        _validate_definition(raw, definition_path=definition)


def test_definition_rejects_duplicate_attestor_config_keys(tmp_path: Path) -> None:
    environments = _copy_fixtures(tmp_path)
    definition = environments / "shared-definition" / "definition.yaml"
    definition.write_text(
        definition.read_text(encoding="utf-8")
        + "checkpoint_attestor_root: .\n"
        + "checkpoint_attestors:\n"
        + "  - id: tests.duplicate-config\n"
        + "    class: attestors.sample.SampleAttestor\n"
        + "    config:\n"
        + "      label: first\n"
        + "      label: second\n",
        encoding="utf-8",
    )

    with pytest.raises(DeploymentConfigError, match="duplicate key 'label'"):
        load_environment(
            environments / "fixture-staging" / "environment.yaml",
            environ=_environment_values(tmp_path / "state"),
        )


def test_okg_ci_staging_composes_fail_closed_definition_and_readiness(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    environment = load_environment(
        private_path("ops/environments/okg-ci-staging/environment.yaml"),
        environ={
            "OKG_CI_STAGING_SYNC_ROOT": str(repo_root),
            "OKG_CI_STAGING_STATE_ROOT": str(tmp_path / "state"),
            "OKG_CI_DEV_DSN": (
                "postgresql://postgres:test@127.0.0.1:55437/okg_ci_dev"
            ),
        },
    )

    posture = environment.manifest.nomos["deployment_posture"]
    assert environment.definition_path == (
        repo_root / "deployments/okg-ci-dev/definition.yaml"
    )
    assert posture["live_call_default"] == "deny"
    assert posture["export_policy"] == "no_export"
    assert environment.database.name == "okg_ci_dev"
    assert environment.database.host_port == 55437
    assert (
        environment.database.container_uid,
        environment.database.container_gid,
    ) == (1000, 1000)
    assert environment.readiness.override_authority == "staging_fail_closed"
    assert environment.manifest.effective_raw["readiness"] == {
        "profile": "ci",
        "environment_policy": "ci",
        "override_authority": "staging_fail_closed",
        "source_groups": {
            "ci": {
                "repo": ["git-history", "pact"],
                "test_artifacts": ["test-reports"],
                "dependencies": ["package-manifests"],
            }
        },
    }


@pytest.mark.parametrize(
    "dsn,match",
    [
        (
            "postgresql://postgres:test@127.0.0.1:55437/okg_ci_dev_pr702",
            "database.name.*does not match",
        ),
        (
            "postgresql://postgres:test@127.0.0.1:55438/okg_ci_dev",
            "host_port.*does not match",
        ),
    ],
)
def test_okg_ci_staging_refuses_nonexact_database_authority(
    tmp_path: Path,
    dsn: str,
    match: str,
) -> None:
    repo_root = Path(__file__).resolve().parents[2]

    with pytest.raises(DeploymentConfigError, match=match):
        load_environment(
            private_path("ops/environments/okg-ci-staging/environment.yaml"),
            environ={
                "OKG_CI_STAGING_SYNC_ROOT": str(repo_root),
                "OKG_CI_STAGING_STATE_ROOT": str(tmp_path / "state"),
                "OKG_CI_DEV_DSN": dsn,
            },
        )


def test_okg_ci_staging_refuses_missing_database_authority(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[2]

    with pytest.raises(DeploymentConfigError, match="OKG_CI_DEV_DSN"):
        load_environment(
            private_path("ops/environments/okg-ci-staging/environment.yaml"),
            environ={
                "OKG_CI_STAGING_SYNC_ROOT": str(repo_root),
                "OKG_CI_STAGING_STATE_ROOT": str(tmp_path / "state"),
            },
        )


def test_composition_status_payload_redacts_dsn(tmp_path: Path) -> None:
    environment = load_environment(
        "fixture-staging",
        environments_dir=FIXTURES,
        environ=_environment_values(tmp_path / "state"),
    )

    payload = environment.to_dict()

    assert payload["database"]["dsn"] == "<redacted>"
    assert "postgresql://" not in repr(payload)
    assert payload["definition"]["digest"].startswith("sha256:")
    assert payload["descriptor"]["digest"].startswith("sha256:")


def test_composition_descriptor_digest_does_not_include_secret_value(
    tmp_path: Path,
) -> None:
    first_values = _environment_values(tmp_path / "state")
    second_values = dict(first_values)
    second_values["TEST_STAGING_DSN"] = (
        "postgresql://postgres:other@127.0.0.1:55435/fixture_staging"
    )

    first = load_environment(
        "fixture-staging",
        environments_dir=FIXTURES,
        environ=first_values,
    )
    second = load_environment(
        "fixture-staging",
        environments_dir=FIXTURES,
        environ=second_values,
    )

    assert first.descriptor_digest == second.descriptor_digest
    assert first.manifest.postgres_dsn != second.manifest.postgres_dsn


def test_composition_definition_digest_changes_with_declared_asset(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    values = _environment_values(tmp_path / "state")
    before = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=values,
    )
    registry = root / "shared-definition" / "source_registry.yaml"
    registry.write_text("sources: []\n# changed\n", encoding="utf-8")

    after = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=values,
    )

    assert before.definition_digest != after.definition_digest


def test_composition_definition_digest_is_independent_of_checkout_path(
    tmp_path: Path,
) -> None:
    first_root = _copy_fixtures(tmp_path / "first")
    second_root = _copy_fixtures(tmp_path / "second")
    values = _environment_values(tmp_path / "state")

    first = load_environment(
        "fixture-staging",
        environments_dir=first_root,
        environ=values,
    )
    second = load_environment(
        "fixture-staging",
        environments_dir=second_root,
        environ=values,
    )

    assert first.definition_path != second.definition_path
    assert first.definition_digest == second.definition_digest


def test_composition_rejects_environment_override_of_definition_field(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    descriptor = root / "fixture-staging" / "environment.yaml"
    raw = _load_yaml(descriptor)
    raw["modules"] = ["git_graph"]
    _write_yaml(descriptor, raw)

    with pytest.raises(DeploymentConfigError, match="unknown field.*modules"):
        load_environment(
            descriptor,
            environ=_environment_values(tmp_path / "state"),
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "wrong-owner"),
        ("postgres", {"dsn": "postgresql://wrong"}),
        ("release", {"channel": "dev"}),
        ("runtime", {"enabled": True}),
        ("sync_root", "/tmp/editable"),
    ],
)
def test_composition_rejects_environment_owned_definition_fields(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    root = _copy_fixtures(tmp_path)
    definition = root / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw[field] = value
    _write_yaml(definition, raw)

    with pytest.raises(
        DeploymentConfigError,
        match=f"environment-owned field.*{field}",
    ):
        load_environment(
            "fixture-staging",
            environments_dir=root,
            environ=_environment_values(tmp_path / "state"),
        )


@pytest.mark.parametrize("field", ["extends", "imports"])
def test_composition_rejects_second_level_definition_imports(
    tmp_path: Path,
    field: str,
) -> None:
    root = _copy_fixtures(tmp_path)
    definition = root / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw[field] = {"path": "definition.yaml"}
    _write_yaml(definition, raw)

    with pytest.raises(DeploymentConfigError, match="nested definition import"):
        load_environment(
            "fixture-staging",
            environments_dir=root,
            environ=_environment_values(tmp_path / "state"),
        )


def test_composition_rejects_definition_self_cycle_before_following_it(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    definition = root / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw["definition"] = {"path": "definition.yaml"}
    _write_yaml(definition, raw)

    with pytest.raises(DeploymentConfigError, match="nested definition import"):
        load_environment(
            "fixture-staging",
            environments_dir=root,
            environ=_environment_values(tmp_path / "state"),
        )


def test_composition_legal_boundary_is_one_environment_to_one_definition(
    tmp_path: Path,
) -> None:
    environment = load_environment(
        "fixture-staging",
        environments_dir=FIXTURES,
        environ=_environment_values(tmp_path / "state"),
    )

    assert environment.descriptor_path.parent.name == environment.id
    assert environment.definition_id == "shared-repository-graph"
    assert environment.definition_path.name == "definition.yaml"


def test_composition_rejects_endpoint_fields_inside_definition_viz(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    definition = root / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw["viz"]["port"] = 18770
    _write_yaml(definition, raw)

    with pytest.raises(DeploymentConfigError, match="viz declares.*port"):
        load_environment(
            "fixture-production",
            environments_dir=root,
            environ=_environment_values(tmp_path / "state"),
        )


def test_composition_rejects_missing_secret_without_local_env_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    local_env = tmp_path / ".okg" / "local.env"
    local_env.parent.mkdir()
    local_env.write_text("TEST_STAGING_DSN=postgresql://must-not-load\n")

    with pytest.raises(DeploymentConfigError, match="no cwd or .okg/local.env"):
        load_environment(
            "fixture-staging",
            environments_dir=FIXTURES,
            environ={"TEST_STATE_ROOT": str(tmp_path / "state")},
        )


def test_composition_bare_id_cannot_be_shadowed_from_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shadow = tmp_path / "fixture-staging"
    shadow.mkdir()
    shadow_descriptor = _load_yaml(FIXTURES / "fixture-staging" / "environment.yaml")
    shadow_descriptor["graph_deployment_name"] = "shadow-graph"
    _write_yaml(shadow / "environment.yaml", shadow_descriptor)
    monkeypatch.chdir(tmp_path)

    environment = load_environment(
        "fixture-staging",
        environments_dir=FIXTURES,
        environ=_environment_values(tmp_path / "state"),
    )

    assert environment.graph_deployment_name == "fixture-graph-staging"


def test_composition_rejects_tracking_mode_for_production_like_tier(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    descriptor = root / "fixture-staging" / "environment.yaml"
    raw = _load_yaml(descriptor)
    raw["release"]["mode"] = "tracking"
    _write_yaml(descriptor, raw)

    with pytest.raises(DeploymentConfigError, match="staging.*require.*sealed"):
        load_environment(
            descriptor,
            environ=_environment_values(tmp_path / "state"),
        )


def test_composition_rejects_definition_path_environment_placeholder(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    descriptor = root / "fixture-staging" / "environment.yaml"
    raw = _load_yaml(descriptor)
    raw["definition"]["path"] = "${DEFINITION_ROOT}/definition.yaml"
    _write_yaml(descriptor, raw)

    with pytest.raises(DeploymentConfigError, match="definition.path cannot"):
        load_environment(
            descriptor,
            environ={
                **_environment_values(tmp_path / "state"),
                "DEFINITION_ROOT": str(root / "shared-definition"),
            },
        )


def test_composition_rejects_mcp_registration_for_another_environment(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    descriptor = root / "fixture-staging" / "environment.yaml"
    raw = _load_yaml(descriptor)
    raw["mcp"]["launcher_environment"] = "fixture-production"
    _write_yaml(descriptor, raw)

    with pytest.raises(DeploymentConfigError, match="must equal environment id"):
        load_environment(
            descriptor,
            environ=_environment_values(tmp_path / "state"),
        )


def test_resource_isolation_allows_shared_definition_and_artifact_store(
    tmp_path: Path,
) -> None:
    fleet = load_environment_fleet(
        FIXTURES,
        environ=_environment_values(tmp_path / "state"),
    )

    report = resource_isolation_report(fleet)

    assert report["ok"] is True
    assert report["collision_count"] == 0
    shared = [
        row
        for row in report["resources"]
        if row["collision_domain"] in {"definition_digest", "immutable_artifact_store"}
    ]
    assert len(shared) == 4
    assert all(row["shareable"] for row in shared)


@pytest.mark.parametrize(
    "field_path,collision_domain",
    [
        (("database", "volume"), "volume_name"),
        (("runtime", "worker_service"), "service_id"),
        (("release", "active_pointer"), "mutable_path"),
        (("mcp", "registration_id"), "mcp_registration"),
    ],
)
def test_resource_isolation_reports_cross_environment_collision(
    tmp_path: Path,
    field_path: tuple[str, str],
    collision_domain: str,
) -> None:
    root = _copy_fixtures(tmp_path)
    staging_path = root / "fixture-staging" / "environment.yaml"
    production_path = root / "fixture-production" / "environment.yaml"
    staging_raw = _load_yaml(staging_path)
    production_raw = _load_yaml(production_path)
    block, field = field_path
    production_raw[block][field] = staging_raw[block][field]
    _write_yaml(production_path, production_raw)

    fleet = load_environment_fleet(
        root,
        environ=_environment_values(tmp_path / "state"),
    )
    report = resource_isolation_report(fleet)

    assert report["ok"] is False
    collision = next(
        row
        for row in report["collisions"]
        if row["collision_domain"] == collision_domain
    )
    assert {owner["environment_id"] for owner in collision["owners"]} == {
        "fixture-staging",
        "fixture-production",
    }


def test_resource_isolation_detects_endpoint_collision_across_service_roles(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    production_path = root / "fixture-production" / "environment.yaml"
    production_raw = _load_yaml(production_path)
    production_raw["webhook"]["port"] = 18771
    _write_yaml(production_path, production_raw)

    report = resource_isolation_report(
        load_environment_fleet(
            root,
            environ=_environment_values(tmp_path / "state"),
        )
    )

    collision = next(
        row
        for row in report["collisions"]
        if row["collision_domain"] == "tcp_endpoint"
        and row["value"] == "loopback:18771"
    )
    assert {owner["resource"] for owner in collision["owners"]} == {
        "ui.endpoint",
        "webhook.endpoint",
    }


def test_resource_isolation_detects_internal_active_previous_pointer_collision(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    descriptor = root / "fixture-staging" / "environment.yaml"
    raw = _load_yaml(descriptor)
    raw["release"]["previous_pointer"] = raw["release"]["active_pointer"]
    _write_yaml(descriptor, raw)
    environment = load_environment(
        descriptor,
        environ=_environment_values(tmp_path / "state"),
    )

    report = resource_isolation_report([environment])

    assert report["ok"] is False
    collision = report["collisions"][0]
    assert collision["collision_domain"] == "mutable_path"
    assert {owner["resource"] for owner in collision["owners"]} == {
        "release.active_pointer",
        "release.previous_pointer",
    }


def test_resource_isolation_detects_nested_paths_across_environments(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    production_path = root / "fixture-production" / "environment.yaml"
    production_raw = _load_yaml(production_path)
    production_raw["runtime"]["state_root"] = (
        "${TEST_STATE_ROOT}/environments/fixture-staging/foreign-production"
    )
    _write_yaml(production_path, production_raw)

    report = resource_isolation_report(
        load_environment_fleet(
            root,
            environ=_environment_values(tmp_path / "state"),
        )
    )

    collision = next(
        row
        for row in report["collisions"]
        if row.get("relationship") == "ancestor_descendant"
        and {owner["environment_id"] for owner in row["owners"]}
        == {"fixture-staging", "fixture-production"}
    )
    assert collision["collision_domain"] == "mutable_path"
    assert "fixture-staging" in collision["value"]


def test_resource_isolation_normalizes_loopback_database_target(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    production_path = root / "fixture-production" / "environment.yaml"
    production_raw = _load_yaml(production_path)
    production_raw["database"]["name"] = "fixture_staging"
    production_raw["database"]["host_port"] = 55435
    _write_yaml(production_path, production_raw)
    values = _environment_values(tmp_path / "state")
    values["TEST_PRODUCTION_DSN"] = (
        "postgresql://other:secret@localhost:55435/fixture_staging"
    )

    report = resource_isolation_report(load_environment_fleet(root, environ=values))

    domains = {row["collision_domain"] for row in report["collisions"]}
    assert "database_target" in domains
    assert "tcp_endpoint" in domains
    assert "secret" not in repr(report)


@pytest.mark.parametrize(
    "dsn,match",
    [
        (
            "postgresql://postgres:test@127.0.0.1:55435/wrong_database",
            "database.name.*does not match",
        ),
        (
            "postgresql://postgres:test@127.0.0.1:55436/fixture_staging",
            "host_port.*does not match",
        ),
        (
            "sqlite:///fixture_staging",
            "must use postgres",
        ),
    ],
)
def test_resource_identity_rejects_ambiguous_database_target(
    tmp_path: Path,
    dsn: str,
    match: str,
) -> None:
    values = _environment_values(tmp_path / "state")
    values["TEST_STAGING_DSN"] = dsn

    with pytest.raises(DeploymentConfigError, match=match):
        load_environment(
            "fixture-staging",
            environments_dir=FIXTURES,
            environ=values,
        )


def test_resource_identity_refuses_a_missing_database_authority(tmp_path: Path) -> None:
    """The shipped fixture's form of the okg-ci-staging refusal above."""
    values = _environment_values(tmp_path / "state")
    del values["TEST_STAGING_DSN"]

    with pytest.raises(DeploymentConfigError, match="TEST_STAGING_DSN"):
        load_environment(
            "fixture-staging",
            environments_dir=FIXTURES,
            environ=values,
        )


def test_resource_isolation_guard_names_both_owners(tmp_path: Path) -> None:
    root = _copy_fixtures(tmp_path)
    production_path = root / "fixture-production" / "environment.yaml"
    production_raw = _load_yaml(production_path)
    production_raw["database"]["volume"] = "fixture-staging-postgres-data"
    _write_yaml(production_path, production_raw)
    fleet = load_environment_fleet(
        root,
        environ=_environment_values(tmp_path / "state"),
    )

    with pytest.raises(DeploymentConfigError) as exc:
        require_resource_isolation(fleet)

    message = str(exc.value)
    assert "volume_name" in message
    assert "fixture-staging:database.volume" in message
    assert "fixture-production:database.volume" in message


def test_resource_status_rejects_secret_like_path_template(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    descriptor = root / "fixture-staging" / "environment.yaml"
    raw = _load_yaml(descriptor)
    raw["runtime"]["log_root"] = "${DATABASE_PASSWORD}/logs"
    _write_yaml(descriptor, raw)
    values = _environment_values(tmp_path / "state")
    values["DATABASE_PASSWORD"] = "must-not-appear"

    with pytest.raises(DeploymentConfigError, match="secret-like environment"):
        load_environment(descriptor, environ=values)


# --- Declared module provider bytes (#1967) ------------------------------
#
# `module_providers:` names directories, not files, and its value is a
# mapping. Before this block the digest folded in only the literal path
# string, so two tiers declaring the same relative path over different
# module contents compared as one foundation.


def _write_module_provider_definition(
    root: Path,
    *,
    definition_id: str,
    module_body: str,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    provider = root / "modules" / "neutral-lab"
    provider.mkdir(parents=True, exist_ok=True)
    (provider / "records.yaml").write_text(module_body, encoding="utf-8")
    definition = root / "definition.yaml"
    _write_yaml(
        definition,
        {
            "api_version": "okg.deployment-definition/v1",
            "id": definition_id,
            "description": "Module provider fixture",
            "schema_dir": "schemas",
            "source_registry": "source_registry.yaml",
            "module_providers": {"neutral-lab": "modules/neutral-lab"},
        },
    )
    (root / "schemas").mkdir(exist_ok=True)
    (root / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    return definition


def test_declared_module_bytes_separate_two_tiers_of_one_foundation(
    tmp_path: Path,
) -> None:
    staging = _write_module_provider_definition(
        tmp_path / "staging",
        definition_id="shared-repository-graph",
        module_body="records:\n- id: staging-only\n",
    )
    production = _write_module_provider_definition(
        tmp_path / "production",
        definition_id="shared-repository-graph",
        module_body="records:\n- id: production-only\n",
    )

    staging_substance = definition_substance(
        _load_yaml(staging),
        definition_path=staging,
    )
    production_substance = definition_substance(
        _load_yaml(production),
        definition_path=production,
    )

    problems = shared_definition_substance_problems(
        staging_substance,
        production_substance,
    )

    assert problems, (
        "identical declared module_providers paths over different module "
        "bytes compared as the same foundation"
    )
    assert any("module_providers" in problem for problem in problems), problems


def test_identical_module_bytes_under_one_declared_path_share_substance(
    tmp_path: Path,
) -> None:
    body = "records:\n- id: shared\n"
    staging = _write_module_provider_definition(
        tmp_path / "staging",
        definition_id="shared-repository-graph",
        module_body=body,
    )
    production = _write_module_provider_definition(
        tmp_path / "production",
        definition_id="shared-repository-graph",
        module_body=body,
    )

    problems = shared_definition_substance_problems(
        definition_substance(_load_yaml(staging), definition_path=staging),
        definition_substance(_load_yaml(production), definition_path=production),
    )

    assert problems == ()


def test_declared_module_bytes_move_the_definition_digest(
    tmp_path: Path,
) -> None:
    root = _copy_fixtures(tmp_path)
    shared = root / "shared-definition"
    provider = shared / "modules" / "neutral-lab"
    provider.mkdir(parents=True)
    (provider / "records.yaml").write_text("records: []\n", encoding="utf-8")
    definition_path = shared / "definition.yaml"
    raw = _load_yaml(definition_path)
    raw["module_providers"] = {"neutral-lab": "modules/neutral-lab"}
    _write_yaml(definition_path, raw)
    values = _environment_values(tmp_path / "state")

    before = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=values,
    )
    (provider / "records.yaml").write_text(
        "records:\n- id: added\n",
        encoding="utf-8",
    )
    after = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=values,
    )

    assert before.definition_digest != after.definition_digest


def test_undeclared_module_directory_leaves_the_definition_digest_alone(
    tmp_path: Path,
) -> None:
    # A definition without `module_providers:` must digest exactly as it did
    # before #1967 -- bytes sitting in the tree that nothing declares are not
    # part of the foundation.
    root = _copy_fixtures(tmp_path)
    values = _environment_values(tmp_path / "state")
    before = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=values,
    )
    provider = root / "shared-definition" / "modules" / "neutral-lab"
    provider.mkdir(parents=True)
    (provider / "records.yaml").write_text("records: []\n", encoding="utf-8")

    after = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=values,
    )

    assert before.definition_digest == after.definition_digest
    assert before.definition_substance == after.definition_substance


def test_declared_module_provider_crossing_a_symlink_is_refused(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    (outside / "neutral-lab").mkdir(parents=True)
    (outside / "neutral-lab" / "records.yaml").write_text(
        "records: []\n",
        encoding="utf-8",
    )
    root = tmp_path / "definition"
    definition = _write_module_provider_definition(
        root,
        definition_id="shared-repository-graph",
        module_body="records: []\n",
    )
    shutil.rmtree(root / "modules" / "neutral-lab")
    (root / "modules" / "neutral-lab").symlink_to(
        outside / "neutral-lab",
        target_is_directory=True,
    )

    with pytest.raises(DeploymentConfigError, match="crosses a symlink"):
        definition_substance(_load_yaml(definition), definition_path=definition)


def test_declared_module_provider_missing_target_is_not_shared_substance(
    tmp_path: Path,
) -> None:
    staging = _write_module_provider_definition(
        tmp_path / "staging",
        definition_id="shared-repository-graph",
        module_body="records: []\n",
    )
    production = _write_module_provider_definition(
        tmp_path / "production",
        definition_id="shared-repository-graph",
        module_body="records: []\n",
    )
    shutil.rmtree(tmp_path / "production" / "modules" / "neutral-lab")

    problems = shared_definition_substance_problems(
        definition_substance(_load_yaml(staging), definition_path=staging),
        definition_substance(_load_yaml(production), definition_path=production),
    )

    assert any(
        "module_providers:neutral-lab" in problem and "missing" in problem
        for problem in problems
    ), problems


# ── agent_surface in a definition (issue #2067, PACT agent-surface-migrate) ──


def _surface_definition(
    root: Path,
    *,
    instructions_path: str = "mcp-instructions.md",
    instructions_body: str = "Use `search` first.\n",
    surface: dict | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "schemas").mkdir(exist_ok=True)
    (root / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    target = root / instructions_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(instructions_body, encoding="utf-8")
    definition = root / "definition.yaml"
    _write_yaml(
        definition,
        {
            "api_version": "okg.deployment-definition/v1",
            "id": "shared-repository-graph",
            "description": "Agent surface fixture",
            "schema_dir": "schemas",
            "source_registry": "source_registry.yaml",
            "agent_surface": surface if surface is not None else {
                "served": ["search", "inspect"],
                "instructions": instructions_path,
            },
        },
    )
    return definition


def _fixture_definition_with_surface(tmp_path: Path, surface: dict) -> Path:
    root = _copy_fixtures(tmp_path)
    definition = root / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw["agent_surface"] = surface
    _write_yaml(definition, raw)
    (definition.parent / "mcp-instructions.md").write_text(
        "Use `search` first.\n", encoding="utf-8",
    )
    return root


def test_a_definition_may_declare_the_agent_surface_block(
    tmp_path: Path,
) -> None:
    """`agent_surface` was not a definition key, so an environment-composed
    deployment could only run the frozen legacy surface."""
    root = _fixture_definition_with_surface(tmp_path, {
        "served": ["search", "inspect"],
        "instructions": "mcp-instructions.md",
    })
    environment = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=_environment_values(tmp_path / "state"),
    )
    surface = environment.manifest.agent_surface
    assert surface is not None and surface.served == ("search", "inspect")
    # The declared instructions are the ones the server sends.
    assert environment.manifest.mcp_instructions == (
        root / "shared-definition" / "mcp-instructions.md"
    )


def test_a_definition_agent_surface_gets_the_manifest_validation(
    tmp_path: Path,
) -> None:
    from okg.substrate.mcp.effective_surface import AgentSurfaceInvalid

    root = _fixture_definition_with_surface(tmp_path, {
        "served": ["search"],
        "bogus": 1,
    })
    with pytest.raises(AgentSurfaceInvalid, match="bogus"):
        load_environment(
            "fixture-staging",
            environments_dir=root,
            environ=_environment_values(tmp_path / "state"),
        )


def test_a_definition_agent_surface_refuses_the_legacy_key_beside_it(
    tmp_path: Path,
) -> None:
    from okg.substrate.mcp.effective_surface import AgentSurfaceLegacyKeyPresent

    root = _fixture_definition_with_surface(tmp_path, {"served": ["search"]})
    definition = root / "shared-definition" / "definition.yaml"
    raw = _load_yaml(definition)
    raw["mcp_instructions"] = "mcp-instructions.md"
    _write_yaml(definition, raw)
    with pytest.raises(AgentSurfaceLegacyKeyPresent):
        load_environment(
            "fixture-staging",
            environments_dir=root,
            environ=_environment_values(tmp_path / "state"),
        )


def test_agent_surface_instruction_bytes_move_the_definition_digest(
    tmp_path: Path,
) -> None:
    """A nested path is still a path: the digest binds the BYTES the server
    sends, not just the path string, exactly as it did for the top-level
    `mcp_instructions` the block replaces."""
    root = _fixture_definition_with_surface(tmp_path, {
        "served": ["search"],
        "instructions": "mcp-instructions.md",
    })
    values = _environment_values(tmp_path / "state")
    before = load_environment(
        "fixture-staging", environments_dir=root, environ=values,
    )
    (root / "shared-definition" / "mcp-instructions.md").write_text(
        "Use `inspect` first.\n", encoding="utf-8",
    )
    after = load_environment(
        "fixture-staging", environments_dir=root, environ=values,
    )
    assert before.definition_digest != after.definition_digest


def test_equal_instruction_bytes_at_different_paths_share_substance(
    tmp_path: Path,
) -> None:
    """The okg-ci shape: production names `mcp-instructions.md`, staging
    names `../okg-ci/mcp-instructions.md`. Same bytes, same foundation."""
    production = _surface_definition(tmp_path / "production")
    staging = _surface_definition(
        tmp_path / "staging", instructions_path="docs/instructions.md",
    )
    assert shared_definition_substance_problems(
        definition_substance(_load_yaml(staging), definition_path=staging),
        definition_substance(_load_yaml(production), definition_path=production),
    ) == ()


def test_different_instruction_bytes_are_not_shared_substance(
    tmp_path: Path,
) -> None:
    production = _surface_definition(tmp_path / "production")
    staging = _surface_definition(
        tmp_path / "staging", instructions_body="Use `inspect` first.\n",
    )
    problems = shared_definition_substance_problems(
        definition_substance(_load_yaml(staging), definition_path=staging),
        definition_substance(_load_yaml(production), definition_path=production),
    )
    assert any("agent_surface.instructions" in p for p in problems), problems
