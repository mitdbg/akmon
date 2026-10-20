"""Archived admission must precede external effects; fixtures never start services."""
from argparse import Namespace

import pytest


@pytest.fixture
def archived(tmp_path, monkeypatch):
    deployment = tmp_path / "retained"
    deployment.mkdir()
    (deployment / "schemas").mkdir()
    (deployment / "source_registry.yaml").write_text("{}\n")
    (deployment / "deployment.yaml").write_text(
        "name: retained\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "contract:\n"
        "  lifecycle: archived\n"
        "  archive_reason: preservation pending\n"
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    return deployment


@pytest.mark.parametrize("verb", ["cmd_start", "cmd_restart"])
def test_lifecycle_refuses_before_docker(archived, monkeypatch, capsys, verb):
    from okg.substrate.cli import lifecycle

    attempts = []

    def docker(*args, **kwargs):
        attempts.append("docker")
        raise AssertionError("Docker boundary reached")

    monkeypatch.setattr(lifecycle, "_ensure_docker_on_path", docker)
    assert getattr(lifecycle, verb)(Namespace(deployment=str(archived)), as_json=True) == 1
    assert "deployment_archived" in capsys.readouterr().out
    assert attempts == []


def test_mcp_refuses_before_dsn(archived, monkeypatch):
    from okg.substrate.mcp import deployment_entry
    from okg.substrate.errors import SubstrateError

    attempts = []

    def dsn(*args, **kwargs):
        attempts.append("dsn")
        raise AssertionError("DSN boundary reached")

    monkeypatch.setattr(deployment_entry, "_deployment_entry_dsn_resolution", dsn)
    # run_for_deployment is a process entry point; keep its bytecode-cache
    # setting out of the test process.
    monkeypatch.setattr(
        "okg.substrate.bytecode_cache.enter_user_bytecode_cache", lambda: "",
    )
    with pytest.raises(SubstrateError) as caught:
        deployment_entry.run_for_deployment(str(archived))
    assert caught.value.code == "deployment_archived"
    assert attempts == []


def test_discovery_excludes_archived(archived):
    from okg.substrate.cli.fetch import list_profiles

    (archived / "profile.yaml").write_text("name: retained\n")
    assert list_profiles(archived.parent) == []


def test_ingest_refuses_before_telemetry(archived, monkeypatch):
    from okg.substrate.ingest import service
    from okg.substrate.errors import SubstrateError

    def forbidden():
        raise AssertionError("telemetry reached")

    monkeypatch.setattr(service, "current_trace_identity", forbidden)
    with pytest.raises(SubstrateError, match="archived"):
        service.run_ingest(service.IngestOptions(dsn="", deployment="other-name", deployment_dir=archived))


@pytest.mark.parametrize("plan_only", [False, True])
def test_upgrade_refuses_before_context(archived, monkeypatch, plan_only):
    from okg.substrate import deployment_upgrade as upgrade
    from okg.substrate.errors import SubstrateError

    options = upgrade.UpgradeOptions(deployment=str(archived), plan_only=plan_only)
    # An empty DSN is intentional: admission must occur before any resolution.
    if plan_only:
        result = upgrade.run_deployment_upgrade(options)
        assert result["executable"] is False
        assert result["error"]["code"] == "deployment_archived"
    else:
        with pytest.raises(SubstrateError, match="archived"):
            upgrade.run_deployment_upgrade(options)


def test_apply_direct_uses_exact_path(archived):
    from okg.substrate.deployment_apply import apply_deployment_plan, plan_deployment_apply
    from okg.substrate.errors import SubstrateError

    assert plan_deployment_apply(deployment="different", deployment_dir=archived, dsn="")["executable"] is False
    with pytest.raises(SubstrateError, match="archived"):
        apply_deployment_plan(deployment="different", deployment_dir=archived, dsn="", confirm="")


def test_profile_selection_does_not_fall_through(archived, monkeypatch):
    from okg.substrate.deployment_bootstrap.profile_init import discover_profile
    from okg.substrate.errors import SubstrateError

    (archived / "profile.yaml").write_text("name: retained\n")
    monkeypatch.setenv("OKG_PROFILES_DIR", str(archived.parent))
    cache = archived.parent / "cache"
    active = cache / "deployments" / "source" / "retained"
    active.mkdir(parents=True)
    (active / "profile.yaml").write_text("name: retained\n")
    monkeypatch.setenv("OKG_CACHE_HOME", str(cache))
    with pytest.raises(SubstrateError, match="archived"):
        discover_profile("retained")


@pytest.mark.parametrize("lifecycle", [None, "active", "demo"])
def test_nonarchived_discovery_preserved(archived, lifecycle):
    from okg.substrate.cli.fetch import list_profiles
    from okg.substrate.deployments.admission import require_active_deployment

    (archived / "deployment.yaml").write_text("name: retained\n" + (f"contract:\n  lifecycle: {lifecycle}\n" if lifecycle else ""))
    (archived / "profile.yaml").write_text("name: retained\n")
    require_active_deployment(archived)
    assert list_profiles(archived.parent) == ["retained"]


def test_stop_remains_available(archived, monkeypatch):
    from okg.substrate.cli import lifecycle

    calls = []
    monkeypatch.setattr(lifecycle, "_ensure_docker_on_path", lambda *_: calls.append("stop Docker check") or 73)
    assert lifecycle.cmd_stop(Namespace(deployment=str(archived)), as_json=True) == 73
    assert calls == ["stop Docker check"]


def test_scaffold_direct_refuses_before_render(archived, monkeypatch):
    from okg.substrate.deployment_bootstrap import profile_init
    from okg.substrate.errors import SubstrateError

    profile = Namespace(path=archived / "profile.yaml")
    plan = Namespace(profile=profile, deployment_dir=archived.parent / "new")
    monkeypatch.setattr(profile_init, "planned_writes", lambda *_: pytest.fail("render reached"))
    with pytest.raises(SubstrateError, match="archived"):
        profile_init.scaffold_deployment(plan)
    assert not plan.deployment_dir.exists()
    with pytest.raises(SubstrateError, match="archived"):
        profile_init.build_plan(profile=profile, deployment_name="new", deployments_root=archived.parent, raw_answers={})


def test_run_refuses_before_resolver(archived, monkeypatch):
    from okg.substrate.cli import main
    from okg.substrate import deployments
    from okg.substrate.errors import SubstrateError

    monkeypatch.setattr(deployments, "resolve_deployment", lambda *_: pytest.fail("resolver reached"))
    with pytest.raises(SubstrateError, match="archived"):
        main._resolve_run_context(Namespace(deployment=str(archived)))


def test_worker_cli_refuses_before_dsn(archived, monkeypatch, capsys):
    from okg.substrate.cli.runtime import _cmd_runtime
    from okg.substrate import deployments

    monkeypatch.setattr(deployments, "resolve_deployment", lambda *_: pytest.fail("resolver reached"))
    assert _cmd_runtime(Namespace(deployment=str(archived), rt_subcmd="worker"), as_json=True) == 1
    assert "deployment_archived" in capsys.readouterr().err


def test_worker_service_refuses_before_start(archived):
    from okg.substrate.runtime.worker import run_worker
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError, match="archived"):
        run_worker(deployment=str(archived), dsn="", runtime=None)


def test_fleet_refuses_before_snapshot(archived, monkeypatch):
    from okg.substrate import deployment_upgrade as upgrade
    from okg.substrate.errors import SubstrateError

    monkeypatch.setattr(upgrade, "_fleet_release_snapshot", lambda *a, **k: pytest.fail("snapshot reached"))
    with pytest.raises(SubstrateError, match="archived"):
        upgrade.run_deployment_fleet_upgrade(upgrade.FleetUpgradeOptions(deployments=(str(archived),)))


def test_plan_marks_inactive_without_imports(archived):
    from okg.substrate.deployment_wiring import resolve_deployment_wiring_plan

    plan = resolve_deployment_wiring_plan(archived, check_imports=False).as_dict()
    assert plan["active"] is False
    assert plan["executable"] is False


@pytest.mark.parametrize("reference", ["name", "directory", "manifest"])
def test_error_binds_exact_manifest(archived, reference):
    from okg.substrate.deployments.admission import require_active_deployment, DeploymentArchived

    value = {"name": "retained", "directory": archived, "manifest": archived / "deployment.yaml"}[reference]
    with pytest.raises(DeploymentArchived) as caught:
        require_active_deployment(value)
    assert caught.value.data == {"deployment": "retained", "manifest_path": str(archived / "deployment.yaml"), "lifecycle": "archived", "archive_reason": "preservation pending"}
    assert caught.value.fix_hint


def test_install_refuses_before_extension_import(archived, monkeypatch, capsys):
    from okg.substrate.cli import main, profile_cli_loader

    (archived / "profile.yaml").write_text("name: retained\n")
    monkeypatch.setenv("OKG_PROFILES_DIR", str(archived.parent))
    monkeypatch.setattr(profile_cli_loader, "discover_install_extensions", lambda: pytest.fail("extension import reached"))
    assert main._cmd_install(Namespace(profile="retained"), as_json=True) == 1
    assert "deployment_archived" in capsys.readouterr().err


def test_ingest_cli_refuses_before_dsn(archived, monkeypatch, capsys):
    from okg.substrate.cli import main

    monkeypatch.setattr(main, "_resolve_dsn", lambda *_: pytest.fail("DSN reached"))
    assert main._cmd_ingest(Namespace(deployment=str(archived)), as_json=True) == 1
    assert "deployment_archived" in capsys.readouterr().err


def test_bare_name_does_not_use_unrelated_cwd_directory(archived, monkeypatch):
    from okg.substrate.deployments.admission import DeploymentArchived, require_active_deployment

    elsewhere = archived.parent / "elsewhere"
    (elsewhere / "retained").mkdir(parents=True)
    (elsewhere / "retained" / "deployment.yaml").write_text("name: retained\n")
    monkeypatch.chdir(elsewhere)
    with pytest.raises(DeploymentArchived):
        require_active_deployment("retained")


def test_parser_does_not_execute_archived_profile_cli(archived, monkeypatch):
    from okg.substrate.cli import main, profile_cli_loader

    cli = archived / "cli"
    cli.mkdir()
    marker = archived / "executed"
    (cli / "__init__.py").write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
    monkeypatch.setattr(profile_cli_loader, "_PROFILE_ROOT", archived.parent)
    main._build_parser()
    assert not marker.exists()


def test_direct_loader_refuses_actual_archived_profile(archived, monkeypatch):
    from okg.substrate.cli import profile_cli_loader
    from okg.substrate.errors import SubstrateError

    (archived / "cli").mkdir()
    (archived / "cli" / "__init__.py").write_text("raise AssertionError('archived code executed')\n")
    # An active same-name local profile must not admit the bundled code.
    local = archived.parent / "local"
    (local / "retained").mkdir(parents=True)
    (local / "retained" / "profile.yaml").write_text("name: retained\n")
    monkeypatch.setenv("OKG_PROFILES_DIR", str(local))
    with pytest.raises(SubstrateError, match="archived"):
        profile_cli_loader._load_profile_cli(archived)


def test_sealed_archive_refuses_before_secret_resolution(archived, monkeypatch):
    from okg.substrate.deployments import environment
    from okg.substrate.deployments.admission import DeploymentArchived, require_active_deployment

    descriptor = archived.parent / "sealed" / "environment.yaml"
    descriptor.parent.mkdir()
    descriptor.write_text(f"id: sealed\ngraph_deployment_name: retained\ndefinition:\n  path: {archived / 'deployment.yaml'}\n")
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(descriptor.parent))
    monkeypatch.setattr(environment, "_resolve_secret_ref", lambda *a, **k: pytest.fail("secret resolution reached"))
    with pytest.raises(DeploymentArchived):
        require_active_deployment("retained")
    with pytest.raises(DeploymentArchived):
        require_active_deployment(archived)
    from okg.substrate.deployments import DeploymentResolutionError
    with pytest.raises(DeploymentResolutionError, match="outside"):
        require_active_deployment("different")


def test_real_sealed_fixture_refuses_before_missing_secret(tmp_path, monkeypatch):
    import shutil
    # Reuse the established valid descriptor, including its relative definition.
    from pathlib import Path
    import yaml
    from okg.substrate.deployments import environment
    from okg.substrate.deployments.admission import DeploymentArchived, require_active_deployment

    fixture = Path(__file__).parent / "fixtures" / "deployment_environments"
    copied = tmp_path / "environments"
    shutil.copytree(fixture, copied)
    descriptor = copied / "fixture-staging" / "environment.yaml"
    definition = copied / "shared-definition" / "definition.yaml"
    raw = yaml.safe_load(definition.read_text())
    attempts = []

    def secret(*args, **kwargs):
        attempts.append("secret")
        raise AssertionError("secret boundary reached")

    monkeypatch.setattr(environment, "_resolve_secret_ref", secret)
    monkeypatch.delenv("TEST_STAGING_DSN", raising=False)
    monkeypatch.setenv("TEST_STATE_ROOT", str(tmp_path / "state"))
    # Show this structurally valid fixture reaches the real secret boundary.
    with pytest.raises(AssertionError, match="secret boundary"):
        environment.load_environment(descriptor)
    assert attempts == ["secret"]
    attempts.clear()
    # contract is not valid in today's sealed definition schema. This check
    # only refuses marked input early; it does not add sealed lifecycle support.
    raw["contract"] = {"lifecycle": "archived", "archive_reason": "preserve fixture"}
    definition.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(copied))
    with pytest.raises(DeploymentArchived) as caught:
        require_active_deployment("fixture-graph-staging")
    assert caught.value.data["manifest_path"] == str(definition)
    assert attempts == []


@pytest.mark.parametrize("command", ["run", "ingest", "worker"])
def test_partial_sealed_authority_keeps_structured_refusal(monkeypatch, capsys, command):
    from okg.substrate.cli import main
    from okg.substrate.errors import SubstrateError
    from okg.substrate.runtime.worker import run_worker

    monkeypatch.setenv("OKG_RELEASE_ROOT", "/partial/release")
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    if command == "run":
        with pytest.raises(SubstrateError, match="incomplete"):
            main._resolve_run_context(Namespace(deployment="fixture"))
    elif command == "ingest":
        assert main._cmd_ingest(Namespace(deployment="fixture"), as_json=True) == 1
        assert "incomplete" in capsys.readouterr().err
    else:
        # No runtime, embedding, reservation or database collaborator may run.
        assert run_worker(deployment="fixture", dsn="", runtime=None) == 2



def test_archived_read_surfaces_and_global_resolution(archived, tmp_path, monkeypatch, capsys):
    import json

    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.cli import deployment as deployment_cli

    # The global resolver remains an inspection primitive for an archived
    # manifest; active callers apply require_active_deployment separately.
    manifest = resolve_deployment("retained")
    assert manifest.name == "retained"
    assert manifest.contract == {
        "lifecycle": "archived",
        "archive_reason": "preservation pending",
    }

    assert deployment_cli._cmd_deployment_show(
        Namespace(deployment="retained"), as_json=True,
    ) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["name"] == "retained"

    assert deployment_cli._cmd_deployment_lint(
        Namespace(deployment=str(archived), deployments_root=None, live=False),
        as_json=True,
    ) == 0
    linted = json.loads(capsys.readouterr().out)
    assert linted["deployment"] == "retained"

    state_file = tmp_path / "retained-dev.json"
    state_file.write_text(json.dumps({
        "deployment": "retained",
        "operation_id": "archived-read",
        "lifecycle_plan_admission": {"plan_digest": "sha256:" + "1" * 64},
        "active_phase": None,
        "completed": True,
        "phase_statuses": {},
    }))
    assert deployment_cli._cmd_deployment_read_status(
        Namespace(deployment="retained", profile="dev", state_file=str(state_file)),
        as_json=True,
    ) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["operation_id"] == "archived-read"

    doctor_module = __import__(
        "okg.substrate.deployment_env_doctor",
        fromlist=["build_env_doctor_report"],
    )
    monkeypatch.setattr(
        doctor_module,
        "build_env_doctor_report",
        lambda *args, **kwargs: {"ok": True, "deployment": "retained"},
    )
    assert deployment_cli._cmd_deployment_env_doctor(
        Namespace(
            deployment="retained", dsn=None, local_env=None,
            for_command="read", show_values=False,
            write_local=False, env_name=None,
        ),
        as_json=True, deployment="retained",
    ) == 0
    doctor = json.loads(capsys.readouterr().out)
    assert doctor["deployment"] == "retained"

    assert deployment_cli._cmd_deployments(
        Namespace(
            deployments_subcmd="audit", root=str(archived.parent),
            contract_level="current", include_archived=True,
        ),
        as_json=True,
    ) in (0, 1)
    portfolio = json.loads(capsys.readouterr().out)
    assert portfolio["deployments"]
    assert any(item.get("name") == "retained" for item in portfolio["deployments"])
