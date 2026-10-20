from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.cli.environment import _emit
from okg.substrate.cli.main import main
from okg.substrate.cli.deployment import _deployment_ready_env_status
from okg.substrate.cli.deployment_readiness import _ready_load_yaml_mapping
from okg.substrate.deployments.environment import load_environment
from okg.substrate.environment_services import render_environment_services


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "deployment_environments"


def _set_environment(monkeypatch: pytest.MonkeyPatch, state_root: Path) -> None:
    monkeypatch.setenv("TEST_STATE_ROOT", str(state_root))
    monkeypatch.setenv(
        "TEST_STAGING_DSN",
        "postgresql://postgres:test@127.0.0.1:55435/fixture_staging",
    )
    monkeypatch.setenv(
        "TEST_PRODUCTION_DSN",
        "postgresql://postgres:test@127.0.0.1:55434/fixture_production",
    )


def _json_output(capsys: pytest.CaptureFixture[str]) -> dict:
    captured = capsys.readouterr()
    return json.loads((captured.out or captured.err).strip())


def _copy_fixtures(tmp_path: Path) -> Path:
    root = tmp_path / "environments"
    shutil.copytree(FIXTURES, root)
    return root


def _load_yaml(path: Path) -> dict:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_sealed_readiness_uses_composed_environment_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_fixtures(tmp_path)
    state_root = tmp_path / "state"
    _set_environment(monkeypatch, state_root)
    descriptor = root / "fixture-staging/environment.yaml"
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(tmp_path / "release"))
    monkeypatch.setenv("OKG_DEPLOYMENT", "fixture-graph-staging")

    payload, dsn = _deployment_ready_env_status(
        SimpleNamespace(deployment="fixture-graph-staging", local_env=None),
        show_values=False,
    )
    environment = load_environment(descriptor, environ=dict(__import__("os").environ))
    raw = _ready_load_yaml_mapping(
        environment.manifest.deployment_dir / "deployment.yaml"
    )

    assert payload["authority"]["mode"] == "sealed_environment"
    assert payload["manifest_path"] == str(environment.definition_path)
    assert dsn == environment.manifest.postgres_dsn
    assert raw == environment.manifest.effective_raw
    assert raw["name"] == "fixture-graph-staging"
    assert raw["postgres"]["database"] == "fixture_staging"


def test_environment_promote_plans_exact_artifact_without_mutating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _copy_fixtures(tmp_path)
    definition = root / "shared-definition/definition.yaml"
    definition_raw = _load_yaml(definition)
    definition_raw["nomos"] = {
        "deployment_posture": {
            "export_policy": "no_export",
            "live_call_default": "deny",
            "allowed_exportability": ["no_export"],
        }
    }
    definition.write_text(
        yaml.safe_dump(definition_raw, sort_keys=False),
        encoding="utf-8",
    )
    for descriptor in root.glob("*/environment.yaml"):
        raw = _load_yaml(descriptor)
        raw["runtime"]["config"]["enabled"] = True
        descriptor.write_text(
            yaml.safe_dump(raw, sort_keys=False),
            encoding="utf-8",
        )
    state_root = tmp_path / "state"
    _set_environment(monkeypatch, state_root)
    values = dict(__import__("os").environ)
    staging = load_environment("fixture-staging", environments_dir=root, environ=values)
    production = load_environment(
        "fixture-production", environments_dir=root, environ=values
    )
    artifact_id = "sha256:" + "a" * 64
    artifact_root = production.release.artifact_store / ("a" * 64)
    release = SimpleNamespace(
        artifact_id=artifact_id,
        artifact_root=artifact_root,
        inputs=SimpleNamespace(
            input_id="sha256:input",
            definition_digest=production.definition_digest,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.sealed_release.verify_sealed_release",
        lambda *_args, **_kwargs: release,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_promotion.verify_sealed_release",
        lambda *_args, **_kwargs: release,
    )
    staging_plan = render_environment_services(
        staging,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    captured_at = datetime.now(timezone.utc).isoformat()
    readiness_path = staging.readiness.evidence_path
    supervision_path = (
        staging.runtime.state_root / "supervisor/promotion-supervision.json"
    )
    readiness_path.parent.mkdir(parents=True, exist_ok=True)
    supervision_path.parent.mkdir(parents=True, exist_ok=True)
    readiness_path.write_text(
        json.dumps(
            {
                "schema": "okg.environment_readiness/v1",
                "ok": True,
                "status": "ready",
                "environment_id": staging.id,
                "artifact_id": artifact_id,
                "captured_at": captured_at,
                "evidence_path": str(readiness_path),
                "generation": {
                    "status": "published",
                    "deployment_name": staging.graph_deployment_name,
                    "generation_id": "generation:staging-ready",
                    "node_count": 1,
                    "edge_count": 1,
                },
                "checks": [
                    {"id": check_id, "status": "pass", "ok": True}
                    for check_id in (
                        "release.active",
                        "processes.authority",
                        "services.installation",
                        "database.migrations",
                        "generation.latest_published",
                        "queues.bounds",
                        "sources.freshness",
                        "doctor.okg_workspace",
                        "pact.prerequisites",
                        "ui.http",
                        "webhook.owner",
                        "nomos.read_path",
                        "mcp.registration",
                        "mcp.release_authority",
                        "mcp.inspect",
                        "mcp.search",
                        "mcp.generation_alignment",
                    )
                ],
                "queues": {
                    "ok": True,
                    "active_queue_depth": 0,
                    "dlq_count": 0,
                },
                "source_freshness": {"ok": True, "missing": [], "stale": []},
                "doctor": {"ok": True, "status": "pass"},
                "prerequisite_pacts": {"ok": True, "status": "complete"},
            }
        ),
        encoding="utf-8",
    )
    supervision_path.write_text(
        json.dumps(
            {
                "schema": "okg.environment_service_supervision_evidence/v1",
                "ok": True,
                "status": "completed",
                "environment_id": staging.id,
                "artifact_id": artifact_id,
                "plan_digest": staging_plan["plan_digest"],
                "captured_at": captured_at,
                "action": "start",
                "evidence_path": str(supervision_path),
                "results": [{"returncode": 0}],
                "process_authority": {"ok": True, "status": "healthy"},
                "service_installation": {"ok": True, "status": "current"},
            }
        ),
        encoding="utf-8",
    )
    readiness_path.chmod(0o600)
    supervision_path.chmod(0o600)

    rc = main(
        [
            "--json",
            "environment",
            "promote",
            "fixture-staging",
            "fixture-production",
            "--environments-root",
            str(root),
            "--artifact",
            artifact_id,
            "--promotion-id",
            "promotion-test",
            "--approved-by",
            "operator@example.test",
            "--staging-evidence",
            str(readiness_path),
            "--supervision-evidence",
            str(supervision_path),
            "--adapter",
            "systemd",
            "--systemd-scope",
            "user",
        ]
    )

    assert rc == 0
    payload = _json_output(capsys)
    assert payload["ok"] is True
    assert payload["apply"] is False
    assert payload["approval"]["artifact_id"] == artifact_id
    assert payload["approval"]["promotion_id"] == "promotion-test"
    assert not Path(payload["approval_path"]).exists()


def test_environment_prepare_release_uses_exact_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")
    observed: dict[str, object] = {}

    class Release:
        def to_dict(self) -> dict:
            return {"artifact_id": "sha256:" + "a" * 64, "code_revision": "abc123"}

    def prepare(**kwargs):
        observed.update(kwargs)
        return Release()

    monkeypatch.setattr(
        "okg.substrate.sealed_release.prepare_environment_release",
        prepare,
    )

    rc = main(
        [
            "--json",
            "environment",
            "prepare-release",
            "fixture-staging",
            "--environments-root",
            str(FIXTURES),
            "--source-repo",
            str(tmp_path),
            "--revision",
            "abc123",
        ]
    )

    assert rc == 0
    payload = _json_output(capsys)
    assert payload["release"]["code_revision"] == "abc123"
    assert observed["revision"] == "abc123"
    assert observed["source_repo"] == tmp_path


def test_environment_nomos_baseline_uses_environment_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")
    observed: dict[str, object] = {}

    def seed(deployment, deployment_dir, *, dsn, raw_manifest):
        observed.update(
            deployment=deployment,
            deployment_dir=deployment_dir,
            dsn=dsn,
            raw_manifest=raw_manifest,
        )
        return {"ok": True, "active_policy": {"policy_bundle_id": "baseline"}}

    monkeypatch.setattr(
        "okg.substrate.cli.provision._seed_nomos_baseline",
        seed,
    )

    rc = main(
        [
            "--json",
            "environment",
            "seed-nomos-baseline",
            "fixture-staging",
            "--environments-root",
            str(FIXTURES),
            "--apply",
        ]
    )

    assert rc == 0
    payload = _json_output(capsys)
    assert payload["graph_deployment_name"] == "fixture-graph-staging"
    assert payload["baseline"]["ok"] is True
    assert observed["deployment"] == "fixture-graph-staging"
    assert observed["raw_manifest"]["name"] == "fixture-graph-staging"
    assert str(observed["dsn"]).endswith("/fixture_staging")


def test_environment_command_uses_active_sealed_descriptor_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")
    monkeypatch.setenv(
        "OKG_ENVIRONMENT_DESCRIPTOR",
        str(FIXTURES / "fixture-staging/environment.yaml"),
    )

    rc = main(["--json", "environment", "show", "fixture-staging"])

    assert rc == 1
    payload = _json_output(capsys)
    assert payload["environment"]["id"] == "fixture-staging"
    assert payload["release_status"]["status"] == "missing"


def test_environment_activate_release_is_plan_only_without_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")
    artifact_id = "sha256:" + "b" * 64
    release = SimpleNamespace(
        artifact_id=artifact_id,
        inputs=SimpleNamespace(
            code_revision="abc123",
            definition_digest="sha256:definition",
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.sealed_release.verify_sealed_release",
        lambda *_args, **_kwargs: release,
    )
    monkeypatch.setattr(
        "okg.substrate.release_activation.activate_environment_release",
        lambda **_kwargs: pytest.fail("dry-run activation mutated the pointer"),
    )

    rc = main(
        [
            "--json",
            "environment",
            "activate-release",
            "fixture-staging",
            "--environments-root",
            str(FIXTURES),
            "--artifact",
            artifact_id,
            "--activated-by",
            "operator@example.test",
            "--adapter",
            "systemd",
        ]
    )

    assert rc == 0
    payload = _json_output(capsys)
    assert payload["apply"] is False
    assert payload["artifact_id"] == artifact_id


def test_environment_activate_release_apply_is_incomplete_until_handoff_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")
    artifact_id = "sha256:" + "b" * 64
    release = SimpleNamespace(
        artifact_id=artifact_id,
        inputs=SimpleNamespace(
            code_revision="abc123",
            definition_digest="sha256:" + "c" * 64,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.sealed_release.verify_sealed_release",
        lambda *_args, **_kwargs: release,
    )
    service_handoff = {
        "schema": "okg.environment_release_handoff/v1",
        "ok": False,
        "status": "incomplete",
        "activation": {"artifact_id": artifact_id, "changed": True},
        "handoff": {"ok": False, "status": "incomplete"},
    }
    monkeypatch.setattr(
        "okg.substrate.environment_services.activate_environment_release_with_handoff",
        lambda *_args, **_kwargs: dict(service_handoff),
    )
    argv = [
        "--json",
        "environment",
        "activate-release",
        "fixture-staging",
        "--environments-root",
        str(FIXTURES),
        "--artifact",
        artifact_id,
        "--activated-by",
        "operator@example.test",
        "--adapter",
        "systemd",
        "--apply",
    ]

    assert main(argv) == 1
    first = _json_output(capsys)
    assert first["ok"] is False
    assert first["activation"]["changed"] is True
    assert first["handoff"]["status"] == "incomplete"

    service_handoff["activation"] = {"artifact_id": artifact_id, "changed": False}
    assert main(argv) == 1
    repeated = _json_output(capsys)
    assert repeated["activation"]["changed"] is False
    assert repeated["handoff"]["status"] == "incomplete"

    service_handoff.update(
        {
            "ok": True,
            "status": "complete",
            "handoff": {"ok": True, "status": "complete"},
        }
    )
    assert main(argv) == 0
    complete = _json_output(capsys)
    assert complete["ok"] is True
    assert complete["handoff"]["status"] == "complete"


@pytest.mark.parametrize(
    ("command", "patch_target", "arguments"),
    [
        (
            "clone-backup",
            "okg.substrate.environment_clone.create_environment_clone_backup",
            [
                "fixture-production",
                "fixture-staging",
                "--output",
                "/tmp/clone.dump",
                "--report",
                "/tmp/clone.json",
            ],
        ),
        (
            "clone-restore",
            "okg.substrate.environment_clone.restore_environment_clone",
            ["fixture-staging", "--report", "/tmp/clone.json"],
        ),
        (
            "clone-rebind",
            "okg.substrate.environment_clone.rebind_environment_clone",
            ["fixture-staging", "--report", "/tmp/clone.json"],
        ),
        (
            "claim-owner",
            "okg.substrate.environment_clone.claim_environment_database_owner",
            ["fixture-staging"],
        ),
    ],
)
def test_environment_mutation_commands_are_plan_only_without_apply(
    command: str,
    patch_target: str,
    arguments: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")
    observed: dict[str, object] = {}

    def operation(*args, **kwargs):
        observed.update(args=args, kwargs=kwargs)
        return {"ok": True, "apply": kwargs.get("apply")}

    monkeypatch.setattr(patch_target, operation)

    rc = main(
        [
            "--json",
            "environment",
            command,
            *arguments,
            "--environments-root",
            str(FIXTURES),
        ]
    )

    assert rc == 0
    assert _json_output(capsys) == {"ok": True, "apply": False}
    assert observed["kwargs"]["apply"] is False


def test_environment_show_is_resolved_and_secret_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")

    rc = main(
        [
            "--json",
            "environment",
            "show",
            "fixture-staging",
            "--environments-root",
            str(FIXTURES),
        ]
    )

    assert rc == 1
    payload = _json_output(capsys)
    assert payload["ok"] is False
    assert payload["environment"]["id"] == "fixture-staging"
    assert payload["environment"]["database"]["dsn"] == "<redacted>"
    assert payload["release_status"]["ok"] is False
    assert payload["release_status"]["status"] == "missing"
    assert "postgresql://" not in repr(payload)


def test_environment_plan_proves_complete_non_overlapping_fleet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")

    rc = main(
        [
            "--json",
            "environment",
            "plan",
            "fixture-production",
            "--environments-root",
            str(FIXTURES),
        ]
    )

    assert rc == 0
    payload = _json_output(capsys)
    isolation = payload["resource_isolation"]
    assert payload["ok"] is True
    assert isolation["environment_ids"] == [
        "fixture-production",
        "fixture-staging",
    ]
    assert isolation["collision_count"] == 0
    assert isolation["resource_count"] == 42


def test_environment_plan_blocks_and_names_resource_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _copy_fixtures(tmp_path)
    production_path = root / "fixture-production" / "environment.yaml"
    production = _load_yaml(production_path)
    production["database"]["volume"] = "fixture-staging-postgres-data"
    production_path.write_text(
        yaml.safe_dump(production, sort_keys=False),
        encoding="utf-8",
    )
    _set_environment(monkeypatch, tmp_path / "state")

    rc = main(
        [
            "--json",
            "environment",
            "plan",
            "fixture-staging",
            "--environments-root",
            str(root),
        ]
    )

    assert rc == 1
    payload = _json_output(capsys)
    assert payload["ok"] is False
    collision = payload["resource_isolation"]["collisions"][0]
    assert collision["collision_domain"] == "volume_name"
    assert {owner["environment_id"] for owner in collision["owners"]} == {
        "fixture-staging",
        "fixture-production",
    }


def test_environment_plan_blocks_nested_mutable_path_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _copy_fixtures(tmp_path)
    production_path = root / "fixture-production" / "environment.yaml"
    production = _load_yaml(production_path)
    production["runtime"]["log_root"] = (
        "${TEST_STATE_ROOT}/environments/fixture-staging/logs/production"
    )
    production_path.write_text(
        yaml.safe_dump(production, sort_keys=False),
        encoding="utf-8",
    )
    _set_environment(monkeypatch, tmp_path / "state")

    rc = main(
        [
            "--json",
            "environment",
            "plan",
            "fixture-production",
            "--environments-root",
            str(root),
        ]
    )

    assert rc == 1
    payload = _json_output(capsys)
    assert any(
        collision.get("relationship") == "ancestor_descendant"
        for collision in payload["resource_isolation"]["collisions"]
    )


def test_environment_command_does_not_load_cwd_local_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    local_env = tmp_path / ".okg" / "local.env"
    local_env.parent.mkdir()
    local_env.write_text(
        "TEST_STATE_ROOT=/tmp/must-not-load\n"
        "TEST_STAGING_DSN=postgresql://must-not-load\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TEST_STATE_ROOT", raising=False)
    monkeypatch.delenv("TEST_STAGING_DSN", raising=False)
    monkeypatch.delenv("TEST_PRODUCTION_DSN", raising=False)

    rc = main(
        [
            "--json",
            "environment",
            "show",
            "fixture-staging",
            "--environments-root",
            str(FIXTURES),
        ]
    )

    assert rc == 2
    payload = _json_output(capsys)
    assert payload["error"]["code"] == "deployment_environment_invalid"
    assert "env:TEST_STAGING_DSN is unavailable" in payload["error"]["message"]


def test_environment_plan_accepts_explicit_descriptor_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_environment(monkeypatch, tmp_path / "state")
    descriptor = FIXTURES / "fixture-staging" / "environment.yaml"

    rc = main(["--json", "environment", "plan", str(descriptor)])

    assert rc == 0
    payload = _json_output(capsys)
    assert payload["environment"]["descriptor"]["path"] == str(descriptor)
    assert payload["resource_isolation"]["collision_count"] == 0


def test_environment_nomos_baseline_renders_without_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The human-readable path must render the applied baseline, not crash.

    Regression: `_emit` indexed `payload["environment"]`, which this payload
    never carries, so `--apply` mutated the environment and then raised
    KeyError on the way out.
    """
    _set_environment(monkeypatch, tmp_path / "state")

    def seed(deployment, deployment_dir, *, dsn, raw_manifest):
        return {"ok": True, "active_policy": {"policy_bundle_id": "baseline"}}

    monkeypatch.setattr("okg.substrate.cli.provision._seed_nomos_baseline", seed)

    rc = main(
        [
            "environment",
            "seed-nomos-baseline",
            "fixture-staging",
            "--environments-root",
            str(FIXTURES),
            "--apply",
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert "environment: fixture-staging" in out
    assert "graph_deployment_name: fixture-graph-staging" in out
    assert "apply: True" in out


@pytest.mark.parametrize(
    ("label", "payload"),
    [
        (
            "seed-nomos-baseline",
            {
                "schema": "okg.environment_nomos_baseline/v1",
                "ok": True,
                "apply": False,
                "environment_id": "fixture-staging",
                "graph_deployment_name": "fixture-graph-staging",
                "definition_digest": "sha256:" + "b" * 64,
            },
        ),
        (
            "prepare-release",
            {
                "schema": "okg.environment_prepare_release/v1",
                "ok": True,
                "environment_id": "fixture-staging",
                "release": {"artifact_id": "sha256:" + "a" * 64},
            },
        ),
        (
            "activate-release",
            {
                "schema": "okg.environment_activate_release/v1",
                "ok": True,
                "apply": False,
                "environment_id": "fixture-staging",
                "artifact_id": "sha256:" + "a" * 64,
                "code_revision": "abc123",
                "definition_digest": "sha256:" + "b" * 64,
            },
        ),
        (
            "clone-backup",
            {
                "schema": "okg.environment_clone_backup/v1",
                "ok": True,
                "apply": False,
                "source": {"environment_id": "fixture-production"},
                "target": {
                    "environment_id": "fixture-staging",
                    "graph_deployment_name": "fixture-graph-staging",
                    "database": "fixture_staging",
                },
                "definition_digest": "sha256:" + "b" * 64,
            },
        ),
        (
            "clone-restore",
            {
                "schema": "okg.environment_clone_restore/v1",
                "ok": True,
                "apply": False,
                "environment_id": "fixture-staging",
                "database": "fixture_staging",
                "dump_digest": "sha256:" + "c" * 64,
            },
        ),
        (
            "clone-rebind",
            {
                "schema": "okg.environment_clone_rebind/v1",
                "ok": True,
                "apply": False,
                "target": {
                    "environment_id": "fixture-staging",
                    "graph_deployment_name": "fixture-graph-staging",
                    "database": "fixture_staging",
                },
            },
        ),
        (
            "claim-owner",
            {
                "schema": "okg.environment_database_owner_claim/v1",
                "ok": True,
                "apply": False,
                "environment_id": "fixture-staging",
                "database": "fixture_staging",
                "expected_owner": "fixture-graph-staging",
            },
        ),
    ],
)
def test_emit_renders_every_environment_identity_payload(
    label: str,
    payload: dict,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _emit(payload, as_json=False)

    out = capsys.readouterr().out
    assert "environment: fixture-staging" in out, label
    assert payload["schema"] in out, label


def test_environment_service_scope_allows_unresolved_sibling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _copy_fixtures(tmp_path)
    _set_environment(monkeypatch, tmp_path / "state")
    staging_path = root / "fixture-staging" / "environment.yaml"
    staging = _load_yaml(staging_path)
    staging["runtime"]["config"]["enabled"] = True
    staging_path.write_text(yaml.safe_dump(staging, sort_keys=False), encoding="utf-8")
    production_path = root / "fixture-production" / "environment.yaml"
    production = _load_yaml(production_path)
    production["sync_root"] = "${MISSING_SIBLING_SYNC_ROOT}"
    production_path.write_text(
        yaml.safe_dump(production, sort_keys=False), encoding="utf-8"
    )
    for action in ("render", "status", "install", "start", "restart", "stop"):
        rc = main(
            [
                "--json",
                "environment",
                "service",
                action,
                "fixture-staging",
                "--environments-root",
                str(root),
                "--adapter",
                "systemd",
            ]
        )
        payload = _json_output(capsys)
        assert payload["resource_isolation"]["complete"] is False
        unresolved = payload["resource_isolation"]["unresolved_environments"]
        assert unresolved and unresolved[0]["descriptor_path"].endswith(
            "fixture-production/environment.yaml"
        )
        assert unresolved[0]["reason"] == "deployment_config_error"
        assert "postgresql://" not in repr(payload)
        assert "fleet proof is incomplete" in payload["resource_isolation"]["warning"]
        assert rc in (0, 1)


def test_environment_service_scope_preserves_collision_blocking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _copy_fixtures(tmp_path)
    staging_path = root / "fixture-staging" / "environment.yaml"
    staging = _load_yaml(staging_path)
    staging["runtime"]["config"]["enabled"] = True
    staging_path.write_text(yaml.safe_dump(staging, sort_keys=False), encoding="utf-8")
    production_path = root / "fixture-production" / "environment.yaml"
    production = _load_yaml(production_path)
    production["database"]["volume"] = "fixture-staging-postgres-data"
    production_path.write_text(
        yaml.safe_dump(production, sort_keys=False), encoding="utf-8"
    )
    _set_environment(monkeypatch, tmp_path / "state")
    rc = main(
        [
            "--json",
            "environment",
            "service",
            "render",
            "fixture-staging",
            "--environments-root",
            str(root),
            "--adapter",
            "systemd",
        ]
    )
    payload = _json_output(capsys)
    assert rc == 1
    assert payload["ok"] is False
    collision = payload["resource_isolation"]["collisions"][0]
    assert {owner["environment_id"] for owner in collision["owners"]} == {
        "fixture-staging",
        "fixture-production",
    }


def test_environment_service_scope_reports_missing_sibling_database_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _copy_fixtures(tmp_path)
    _set_environment(monkeypatch, tmp_path / "state")
    staging_path = root / "fixture-staging" / "environment.yaml"
    staging = _load_yaml(staging_path)
    staging["runtime"]["config"]["enabled"] = True
    staging_path.write_text(yaml.safe_dump(staging, sort_keys=False), encoding="utf-8")
    production_path = root / "fixture-production" / "environment.yaml"
    production = _load_yaml(production_path)
    production["database"]["dsn_secret_ref"] = "env:MISSING_SIBLING_DSN"
    production_path.write_text(yaml.safe_dump(production, sort_keys=False), encoding="utf-8")
    for action in ("render", "status", "install", "start", "restart", "stop"):
        rc = main([
            "--json", "environment", "service", action, "fixture-staging",
            "--environments-root", str(root), "--adapter", "systemd",
        ])
        payload = _json_output(capsys)
        assert payload["environment"]["id"] == "fixture-staging"
        assert payload["resource_isolation"]["complete"] is False
        unresolved = payload["resource_isolation"]["unresolved_environments"]
        assert unresolved[0]["descriptor_path"].endswith(
            "fixture-production/environment.yaml"
        )
        assert unresolved[0]["reason"] == "missing_environment_variables:MISSING_SIBLING_DSN"
        assert "postgresql://" not in repr(payload)
        assert rc in (0, 1)


def test_environment_service_scope_rejects_duplicate_target_id_outside_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _copy_fixtures(tmp_path)
    _set_environment(monkeypatch, tmp_path / "state")
    target_path = tmp_path / "external" / "fixture-staging" / "environment.yaml"
    target_path.parent.parent.mkdir(parents=True)
    shutil.copytree(root / "fixture-staging", target_path.parent)
    shutil.copytree(root / "shared-definition", tmp_path / "external" / "shared-definition")
    duplicate_path = root / "fixture-production" / "environment.yaml"
    duplicate = _load_yaml(duplicate_path)
    duplicate["id"] = "fixture-staging"
    duplicate_path.write_text(yaml.safe_dump(duplicate, sort_keys=False), encoding="utf-8")
    rc = main([
        "--json", "environment", "service", "render", str(target_path),
        "--environments-root", str(root), "--adapter", "systemd",
    ])
    payload = _json_output(capsys)
    assert rc == 2
    assert payload["error"]["code"] == "deployment_environment_invalid"
    assert "duplicate id" in payload["error"]["message"]
