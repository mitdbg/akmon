from __future__ import annotations

from dataclasses import replace
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from okg.substrate.deployment_promotion import (
    ProductPromotionApprovalV3,
    PromotionApproval,
    PromotionOperations,
    _digest,
    admit_product_promotion_journal,
    create_product_promotion_approval,
    create_promotion_approval,
    load_promotion_approval,
    run_deployment_promotion,
)
from okg.substrate.deployments.environment import (
    DatabaseResource,
    DeploymentEnvironment,
    EndpointResource,
    McpResource,
    ReadinessResource,
    ReleaseResource,
    RuntimeResource,
    load_environment,
)
from okg.substrate.environment_services import render_environment_services
from okg.substrate.errors import (
    DeploymentEnvironmentInvalid,
    SealedReleaseInvalid,
    SubstrateError,
)
from okg.substrate.sealed_release import SealedRelease, SealedReleaseInputs
from okg.substrate.release_activation import _validate_candidate_binding


NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures" / "deployment_environments"


def _environment(tmp_path: Path, tier: str) -> DeploymentEnvironment:
    environment_id = f"okg-workspace-{tier}"
    port = 5541 if tier == "staging" else 5542
    root = tmp_path / environment_id
    return DeploymentEnvironment(
        id=environment_id,
        tier=tier,
        graph_deployment_name="okg-workspace",
        descriptor_path=root / "environment.yaml",
        descriptor_digest=f"sha256:{tier}-descriptor",
        definition_id="okg-workspace",
        definition_path=tmp_path / "definition.yaml",
        definition_digest="sha256:definition",
        definition_substance={
            "mapping": "sha256:substance-mapping",
            "schema_dir": "sha256:substance-schemas",
            "source_registry": "sha256:substance-sources",
        },
        database=DatabaseResource(
            dsn_secret_ref=f"{tier.upper()}_DSN",
            dsn=f"postgresql://okg@127.0.0.1:{port}/okg_{tier}",
            name=f"okg_{tier}",
            container=f"okg-{tier}-postgres",
            volume=f"okg-{tier}-pgdata",
            host_port=port,
            dbos_identity=f"okg-{tier}-dbos",
            container_uid=1000,
            container_gid=1000,
        ),
        release=ReleaseResource(
            mode="sealed",
            artifact_store=root / "artifacts",
            active_pointer=root / "state/active.json",
            previous_pointer=root / "state/previous.json",
        ),
        runtime=RuntimeResource(
            worker_service=f"okg-{tier}-worker",
            state_root=root / "state",
            local_env_file=root / "state/local.env",
            log_root=root / "logs",
            config={},
        ),
        ui=EndpointResource(
            service=f"okg-{tier}-ui",
            host="127.0.0.1",
            port=port + 100,
        ),
        webhook=EndpointResource(
            service=f"okg-{tier}-worker",
            host="127.0.0.1",
            port=port + 200,
        ),
        mcp=McpResource(
            transport="stdio",
            registration_id=f"okg-{tier}-mcp",
            launcher_environment=environment_id,
        ),
        readiness=ReadinessResource(
            policy="dogfood",
            evidence_store="file",
            evidence_path=root / "state/readiness.json",
            max_age_seconds=300,
            prerequisite_pacts=(),
            requires_clone_rebind=False,
        ),
        path_environment={},
        manifest=SimpleNamespace(),
    )


def _release(production: DeploymentEnvironment) -> SealedRelease:
    inputs = SealedReleaseInputs(
        code_revision="a" * 40,
        dependency_lock_digest="sha256:lock",
        schema_checksum="sha256:schema",
        catalog_fingerprint="sha256:catalog",
        definition_digest=production.definition_digest,
        skill_bundle_digest=None,
    )
    return SealedRelease(
        artifact_id="sha256:candidate",
        artifact_root=production.release.artifact_store / "candidate",
        inputs=inputs,
        executable_tree_digest="sha256:tree",
        prepared_at=NOW.isoformat(),
    )


def _plan(environment: DeploymentEnvironment) -> dict[str, Any]:
    launcher = environment.runtime.state_root / "supervisor/release_launcher.py"
    return {
        "ok": True,
        "environment_id": environment.id,
        "plan_digest": f"sha256:{environment.tier}-plan",
        "topology": [{"kind": "worker", "owner": environment.id}],
        "launcher": {"path": str(launcher)},
        "artifacts": [
            {
                "kind": "launcher",
                "path": str(launcher),
                "mode": "0555",
                "digest": "sha256:launcher",
            },
            {
                "kind": "worker_service",
                "path": str(environment.runtime.state_root / "worker.service"),
                "mode": "0644",
                "content": f"ExecStart=/usr/bin/python3 {launcher} --descriptor active",
            },
        ],
    }


def _evidence(
    environment: DeploymentEnvironment,
    release: SealedRelease,
    *,
    status: str,
    plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema": (
            "okg.environment_readiness/v1"
            if status == "ready"
            else "okg.environment_service_supervision_evidence/v1"
        ),
        "ok": True,
        "status": status,
        "environment_id": environment.id,
        "artifact_id": release.artifact_id,
        "captured_at": NOW.isoformat(),
    }
    if plan is not None:
        payload["plan_digest"] = plan["plan_digest"]
    if status == "ready":
        check_ids = {
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
        }
        payload.update(
            evidence_path=str(environment.readiness.evidence_path),
            generation={
                "status": "published",
                "deployment_name": environment.graph_deployment_name,
                "generation_id": "generation:staging-ready",
                "node_count": 1,
                "edge_count": 1,
            },
            checks=[
                {"id": check_id, "status": "pass", "ok": True}
                for check_id in sorted(check_ids)
            ],
            queues={"ok": True, "active_queue_depth": 0, "dlq_count": 0},
            source_freshness={"ok": True, "missing": [], "stale": []},
            doctor={"ok": True, "status": "pass"},
            prerequisite_pacts={"ok": True, "status": "complete"},
        )
    else:
        payload.update(
            action="start",
            evidence_path=str(
                environment.runtime.state_root
                / "supervisor/promotion-supervision.json"
            ),
            results=[{"returncode": 0}],
            process_authority={"ok": True, "status": "healthy"},
            service_installation={"ok": True, "status": "current"},
        )
    return payload


class _Recorder:
    def __init__(
        self,
        release: SealedRelease,
        *,
        fail_once: tuple[str, ...] = (),
        crash_once: tuple[str, ...] = (),
        raise_once: tuple[str, ...] = (),
    ) -> None:
        self.release = release
        self.calls: list[str] = []
        self.receipts: dict[str, dict[str, Any]] = {}
        self.fail_once = set(fail_once)
        self.crash_once = set(crash_once)
        self.raise_once = set(raise_once)

    def step(self, name: str):
        def invoke(context):
            self.calls.append(name)
            if name in self.crash_once:
                self.crash_once.remove(name)
                raise _SimulatedCrash(name)
            if name in self.raise_once:
                self.raise_once.remove(name)
                raise DeploymentEnvironmentInvalid(
                    f"forced {name} exception",
                    data={"operation": name},
                )
            active = context.state["active_operation"]
            result: dict[str, Any] = {
                "ok": True,
                "operation": name,
                "operation_id": active["operation_id"],
                "authoritative": True,
            }
            if name == "fence_writers":
                result.update(
                    {
                        "deployment_name": context.production.graph_deployment_name,
                        "promotion_id": context.approval.promotion_id,
                        "status": "fenced",
                        "schedules_removed": 2,
                    }
                )
            if name == "drain_work":
                result.update({"drained": True, "pending_workflows": 0})
            if name == "capture_previous_release":
                result["artifact_id"] = "sha256:previous"
            if name == "capture_previous_health":
                result.update({
                    "artifact_id": "sha256:previous",
                    "generation_id": "generation:previous",
                    "health_ok": True,
                })
            if name == "create_backup":
                result.update(
                    {
                        "backup_id": "backup-1",
                        "backup_digest": "sha256:backup",
                        "backup_path": "/backups/backup-1.dump",
                    }
                )
            if name == "verify_backup":
                result.update(
                    {
                        "backup_id": "backup-1",
                        "restore_verified": True,
                        "schema_ok": True,
                        "ownership_ok": True,
                    }
                )
            if name == "restore_backup":
                result["backup_id"] = "backup-1"
            if name in {"activate_release", "prove_production_ready"}:
                result["artifact_id"] = self.release.artifact_id
            if name in {"activate_previous_release", "prove_previous_ready"}:
                result["artifact_id"] = "sha256:previous"
            if name == "reopen_writers":
                result["released"] = True
            if name in self.fail_once:
                self.fail_once.remove(name)
                result.update({"ok": False, "error": f"forced {name} failure"})
            result["receipt_digest"] = _digest(result)
            self.receipts[active["operation_id"]] = dict(result)
            return result

        return invoke

    def inspect(self, context, name: str, operation_id: str):
        receipt = self.receipts.get(operation_id)
        if receipt is not None:
            return receipt
        if name in {
            "restore_backup",
            "activate_previous_release",
            "restart_previous_services",
            "prove_previous_ready",
            "reopen_writers",
        }:
            return self.step(name)(context)
        return None

    def operations(self) -> PromotionOperations:
        return PromotionOperations(
            **{
                field: self.step(field)
                for field in PromotionOperations.__dataclass_fields__
                if field != "inspect_operation"
            },
            inspect_operation=self.inspect,
        )


class _SimulatedCrash(BaseException):
    pass


@pytest.fixture
def promotion_inputs(tmp_path: Path, monkeypatch):
    root = tmp_path / "environments"
    shutil.copytree(FIXTURES, root)
    for descriptor in root.glob("*/environment.yaml"):
        raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
        raw["runtime"]["config"]["enabled"] = True
        descriptor.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    values = {
        "TEST_STATE_ROOT": str(tmp_path / "state"),
        "TEST_STAGING_DSN": (
            "postgresql://postgres:staging-secret@127.0.0.1:55435/fixture_staging"
        ),
        "TEST_PRODUCTION_DSN": (
            "postgresql://postgres:production-secret@127.0.0.1:55434/fixture_production"
        ),
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    staging = load_environment(
        "fixture-staging",
        environments_dir=root,
        environ=values,
    )
    production = load_environment(
        "fixture-production",
        environments_dir=root,
        environ=values,
    )
    staging = replace(
        staging,
        manifest=replace(
            staging.manifest,
            nomos={
                "deployment_posture": {
                    "export_policy": "no_export",
                    "live_call_default": "deny",
                    "allowed_exportability": ["no_export"],
                }
            },
        ),
    )
    release = _release(production)
    staging_plan = render_environment_services(
        staging,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    production_plan = render_environment_services(
        production,
        adapter="systemd",
        systemd_scope="user",
        environ=values,
    )
    readiness = _evidence(staging, release, status="ready")
    supervision = _evidence(
        staging,
        release,
        status="completed",
        plan=staging_plan,
    )

    def verify(_root, *, expected_inputs=None, expected_artifact_id=None, **_kwargs):
        if expected_inputs is not None and expected_inputs != release.inputs:
            raise SealedReleaseInvalid("input mismatch")
        if expected_artifact_id is not None and expected_artifact_id != release.artifact_id:
            raise SealedReleaseInvalid("artifact mismatch")
        return release

    monkeypatch.setattr(
        "okg.substrate.deployment_promotion.verify_sealed_release",
        verify,
    )
    return staging, production, release, staging_plan, production_plan, readiness, supervision


def _approval(inputs):
    staging, production, release, staging_plan, production_plan, readiness, supervision = inputs
    return create_promotion_approval(
        staging=staging,
        production=production,
        release=release,
        staging_readiness=readiness,
        supervision_evidence=supervision,
        staging_service_plan=staging_plan,
        production_service_plan=production_plan,
        approved_by="operator@example.test",
        approved_at=NOW,
        max_age_seconds=300,
        promotion_id="promotion-1",
    )


def test_promotion_accepts_split_definitions_with_shared_substance(
    promotion_inputs, monkeypatch
) -> None:
    """Post-97b96750a staging seals a DIFFERENT definition file than
    production; approval must bind the release to the staging digest it was
    sealed from and compare cross-tier substance, not whole-file digests."""
    staging, production, release, staging_plan, production_plan, readiness, supervision = (
        promotion_inputs
    )
    split_staging = replace(staging, definition_digest="sha256:staging-definition")
    split_release = replace(
        release,
        inputs=replace(
            release.inputs,
            definition_digest="sha256:staging-definition",
            postgres_capacity_digest=production.postgres_capacity_digest,
        ),
    )

    def verify(_root, *, expected_inputs=None, expected_artifact_id=None, **_kwargs):
        if expected_inputs is not None and expected_inputs != split_release.inputs:
            raise SealedReleaseInvalid("input mismatch")
        return split_release

    monkeypatch.setattr(
        "okg.substrate.deployment_promotion.verify_sealed_release",
        verify,
    )
    approval = create_promotion_approval(
        staging=split_staging,
        production=production,
        release=split_release,
        staging_readiness=readiness,
        supervision_evidence=supervision,
        staging_service_plan=staging_plan,
        production_service_plan=production_plan,
        approved_by="operator@example.test",
        approved_at=NOW,
        max_age_seconds=300,
        promotion_id="promotion-split",
    )
    assert approval.definition_digest == split_staging.definition_digest
    assert approval.production_definition_digest == production.definition_digest


def test_promotion_refuses_substance_divergence_and_wrong_seal_source(
    promotion_inputs, monkeypatch
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = (
        promotion_inputs
    )

    def approve(staging_env, release_candidate):
        return create_promotion_approval(
            staging=staging_env,
            production=production,
            release=release_candidate,
            staging_readiness=readiness,
            supervision_evidence=supervision,
            staging_service_plan=staging_plan,
            production_service_plan=production_plan,
            approved_by="operator@example.test",
            approved_at=NOW,
            max_age_seconds=300,
        )

    diverged = replace(
        staging,
        definition_substance={
            **staging.definition_substance,
            "schema_dir": "sha256:forked-schemas",
        },
    )
    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="shared foundation substance differs",
    ):
        approve(diverged, release)

    sealed_elsewhere = replace(staging, definition_digest="sha256:not-what-was-sealed")
    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="staging environment it was sealed from",
    ):
        approve(sealed_elsewhere, release)


def test_activation_accepts_split_definition_with_staging_approval_digest(
    promotion_inputs, monkeypatch
) -> None:
    staging, production, release, _staging_plan, _production_plan, _readiness, _supervision = (
        promotion_inputs
    )
    split_staging = replace(staging, definition_digest="sha256:staging-definition")
    split_release = replace(
        release,
        inputs=replace(
            release.inputs,
            definition_digest="sha256:staging-definition",
            postgres_capacity_digest=production.postgres_capacity_digest,
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.release_activation.validate_environment_release_paths",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda *_args, **_kwargs: object(),
    )
    catalog = type("Catalog", (), {"catalog_fingerprint": release.inputs.catalog_fingerprint})()
    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.catalog_identity_for",
        lambda *_args, **_kwargs: catalog,
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.environment_bound_catalog_fingerprint_for",
        lambda *_args, **_kwargs: "sha256:not-bound",
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.legacy_catalog_fingerprint_for",
        lambda *_args, **_kwargs: "sha256:not-legacy",
    )

    _validate_candidate_binding(
        production,
        production,
        split_release,
        staging_environment=split_staging,
        approved_definition_digest=split_staging.definition_digest,
    )

    diverged = replace(
        split_staging,
        definition_substance={
            **split_staging.definition_substance,
            "schema_dir": "sha256:diverged",
        },
    )
    with pytest.raises(SealedReleaseInvalid, match="definition_substance"):
        _validate_candidate_binding(
            production,
            production,
            split_release,
            staging_environment=diverged,
            approved_definition_digest=split_staging.definition_digest,
        )


def test_approval_refuses_softened_staging_posture(promotion_inputs) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = (
        promotion_inputs
    )
    softened = replace(
        staging,
        manifest=replace(
            staging.manifest,
            nomos={
                "deployment_posture": {
                    "export_policy": "internal_only",
                    "live_call_default": "deny",
                    "allowed_exportability": ["internal", "no_export"],
                }
            },
        ),
    )
    with pytest.raises(DeploymentEnvironmentInvalid, match="posture is not hardened"):
        create_promotion_approval(
            staging=softened,
            production=production,
            release=release,
            staging_readiness=readiness,
            supervision_evidence=supervision,
            staging_service_plan=staging_plan,
            production_service_plan=production_plan,
            approved_by="operator@example.test",
            approved_at=NOW,
            max_age_seconds=300,
        )


def test_legacy_v1_approval_requires_explicit_reapproval(
    promotion_inputs, tmp_path: Path
) -> None:
    approval = _approval(promotion_inputs)
    legacy = approval.to_dict()
    legacy["schema"] = "okg.deployment_promotion_approval/v1"
    legacy.pop("production_definition_digest")
    path = tmp_path / "legacy-approval.json"
    path.write_text(json.dumps(legacy), encoding="utf-8")

    with pytest.raises(SubstrateError, match="wrong schema"):
        load_promotion_approval(path)


def test_promotion_rejects_schema_only_staging_evidence(
    promotion_inputs,
) -> None:
    (
        staging,
        production,
        release,
        staging_plan,
        production_plan,
        readiness,
        supervision,
    ) = promotion_inputs
    readiness = {
        key: value
        for key, value in readiness.items()
        if key
        in {
            "schema",
            "ok",
            "status",
            "environment_id",
            "artifact_id",
            "captured_at",
        }
    }

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="check inventory",
    ):
        create_promotion_approval(
            staging=staging,
            production=production,
            release=release,
            staging_readiness=readiness,
            supervision_evidence=supervision,
            staging_service_plan=staging_plan,
            production_service_plan=production_plan,
            approved_by="operator@example.test",
            approved_at=NOW,
            max_age_seconds=300,
        )


def _bounded_product_target(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        id="target",
        descriptor_digest="sha256:" + "0" * 64,
        definition_digest="sha256:" + "0" * 64,
        runtime=SimpleNamespace(state_root=tmp_path / "product-state"),
        release=SimpleNamespace(
            active_pointer=tmp_path / "product-state/active.json",
            previous_pointer=tmp_path / "product-state/previous.json",
        ),
    )


def test_product_first_install_admission_holds_pointer_lock_and_runs_no_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.substrate.test_deployment_product_lifecycle import _admission_case
    from okg.substrate.deployment_product_lifecycle import read_product_apply_status

    case = _admission_case(tmp_path)
    production = _bounded_product_target(tmp_path)
    pointer_lock_held = False
    pointer_reads: list[Path] = []

    @contextmanager
    def pointer_lock(_path, *, exclusive):
        nonlocal pointer_lock_held
        assert exclusive is False
        pointer_lock_held = True
        try:
            yield
        finally:
            pointer_lock_held = False

    def load_pointer(path: Path, *, required: bool):
        assert required is False
        assert pointer_lock_held
        pointer_reads.append(path)
        return None

    monkeypatch.setattr(
        "okg.substrate.release_activation.release_activation_lock",
        pointer_lock,
    )
    monkeypatch.setattr(
        "okg.substrate.release_activation.load_active_release",
        load_pointer,
    )

    result = admit_product_promotion_journal(
        request=case.request,
        production=production,
        now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
    )

    assert result["ok"] is False
    assert result["status"] == "product_admitted"
    assert result["controller_status"] == "unavailable"
    assert pointer_reads == [
        production.release.active_pointer,
        production.release.previous_pointer,
    ]
    state = json.loads(Path(result["state_path"]).read_text(encoding="utf-8"))
    assert state["product_lifecycle_mode"] == (
        "okg.deployment_product.admission_only/v1"
    )
    assert state["status"] == "product_admitted"
    assert state["completed_transitions"] == []
    assert state["operation_intents"] == {}
    assert state["transition_results"] == {}
    assert state["mutation_boundary"] == "none"
    assert state["writers_fenced"] is False
    assert state["database_changed"] is False
    snapshot = read_product_apply_status(
        production=production,
        promotion_id=case.request.idempotency_key,
    )
    assert snapshot["status"]["state"] == "planned"
    assert snapshot["journal"]["controller_status"] == "unavailable"
    assert snapshot["journal"]["release_observation"] == state[
        "release_observation"
    ]
    assert snapshot["receipts"] == {
        "verification": "deferred",
        "verified": [],
    }


def test_product_status_refuses_journal_from_another_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.substrate.test_deployment_product_lifecycle import _admission_case
    from okg.substrate.deployment_product_lifecycle import read_product_apply_status

    case = _admission_case(tmp_path)
    production = _bounded_product_target(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.release_activation.load_active_release",
        lambda _path, *, required: None,
    )
    admit_product_promotion_journal(
        request=case.request,
        production=production,
        now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
    )
    other_production = SimpleNamespace(
        id="other-target",
        runtime=production.runtime,
        release=production.release,
    )

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="different production environment",
    ):
        read_product_apply_status(
            production=other_production,
            promotion_id=case.request.idempotency_key,
        )


def test_product_journal_reopen_refuses_completed_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.substrate.test_deployment_product_lifecycle import _admission_case

    case = _admission_case(tmp_path)
    production = _bounded_product_target(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.release_activation.load_active_release",
        lambda _path, *, required: None,
    )
    result = admit_product_promotion_journal(
        request=case.request,
        production=production,
        now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
    )
    state_path = Path(result["state_path"])
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["status"] = "completed"
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="admission-only product journal authority changed",
    ):
        admit_product_promotion_journal(
            request=case.request,
            production=production,
            now=datetime(2026, 8, 30, 7, 31, tzinfo=timezone.utc),
        )


def test_product_v3_cannot_execute_through_legacy_controller(tmp_path: Path) -> None:
    from tests.substrate.test_deployment_product_lifecycle import _admission_case
    from okg.substrate.deployment_product_lifecycle import (
        load_product_promotion_approval,
    )

    case = _admission_case(tmp_path)
    approval = load_product_promotion_approval(
        case.request.promotion_approval_path
    )
    assert isinstance(approval, ProductPromotionApprovalV3)

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="requires the production apply/resume controller",
    ):
        run_deployment_promotion(
            approval=approval,
            staging=None,  # type: ignore[arg-type]
            production=None,  # type: ignore[arg-type]
            release=None,  # type: ignore[arg-type]
            staging_readiness={},
            supervision_evidence={},
            staging_service_plan={},
            production_service_plan={},
            operations=None,  # type: ignore[arg-type]
            now=NOW,
        )


def test_bounded_product_journal_refuses_upgrade_before_pointer_or_effect_work(
    tmp_path: Path,
) -> None:
    from tests.substrate.test_deployment_product_lifecycle import _admission_case
    from okg.substrate.deployment_product_lifecycle import (
        approval_digest,
        load_product_promotion_approval,
        save_product_promotion_approval,
    )

    case = _admission_case(tmp_path)
    original = load_product_promotion_approval(
        case.request.promotion_approval_path
    )
    base = PromotionApproval(
        **{
            name: getattr(original, name)
            for name in PromotionApproval.__dataclass_fields__
        }
    )
    request = replace(case.request, requested_transition="upgrade")
    approval = create_product_promotion_approval(
        base=base,
        transition_kind="upgrade",
        expected_previous_artifact_id="sha256:" + "9" * 64,
        request_digest=request.request_digest,
        product_plan_digest=case.plan.plan_digest,
        lock_digest=case.plan.lock_digest,
        composition_digest=case.plan.lock.composition_digest,
    )
    approval_path = tmp_path / "generic-one/upgrade-approval.json"
    save_product_promotion_approval(approval_path, approval)
    request = replace(
        request,
        promotion_approval_path=approval_path,
        promotion_approval_digest=approval_digest(approval),
    )

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="supports first install only",
    ):
        admit_product_promotion_journal(
            request=request,
            production=_bounded_product_target(tmp_path),
            now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
        )


def test_promotion_requires_observed_clone_regeneration(
    promotion_inputs,
) -> None:
    (
        staging,
        production,
        release,
        staging_plan,
        production_plan,
        readiness,
        supervision,
    ) = promotion_inputs
    staging = replace(
        staging,
        readiness=replace(staging.readiness, requires_clone_rebind=True),
    )

    with pytest.raises(
        DeploymentEnvironmentInvalid,
        match="staging readiness is incomplete",
    ):
        create_promotion_approval(
            staging=staging,
            production=production,
            release=release,
            staging_readiness=readiness,
            supervision_evidence=supervision,
            staging_service_plan=staging_plan,
            production_service_plan=production_plan,
            approved_by="operator@example.test",
            approved_at=NOW,
            max_age_seconds=300,
        )

    readiness["checks"].append(
        {"id": "clone.regeneration", "status": "pass", "ok": True}
    )
    readiness["clone_regeneration"] = {
        "ok": True,
        "status": "completed",
        "path": str(staging.runtime.state_root / "clone-rebind.json"),
        "regenerate": [
            {"class": name, "status": "completed", "rows": 1}
            for name in (
                "database_credentials",
                "nomos_baseline",
                "source_progress",
                "dbos_schedules",
                "mcp_registration",
                "readiness_evidence",
            )
        ],
    }

    approval = create_promotion_approval(
        staging=staging,
        production=production,
        release=release,
        staging_readiness=readiness,
        supervision_evidence=supervision,
        staging_service_plan=staging_plan,
        production_service_plan=production_plan,
        approved_by="operator@example.test",
        approved_at=NOW,
        max_age_seconds=300,
    )

    assert approval.staging_environment_id == staging.id


def test_artifact_promotion_binds_bytes_topology_and_orders_mutation(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    approval = _approval(promotion_inputs)
    recorder = _Recorder(release)
    state_path = tmp_path / "promotion.json"

    result = run_deployment_promotion(
        approval=approval,
        staging=staging,
        production=production,
        release=release,
        staging_readiness=readiness,
        supervision_evidence=supervision,
        staging_service_plan=staging_plan,
        production_service_plan=production_plan,
        operations=recorder.operations(),
        now=NOW + timedelta(seconds=30),
        state_path=state_path,
    )

    assert result["ok"] is True
    assert result["status"] == "completed"
    assert result["artifact_id"] == release.artifact_id
    assert result["writers_fenced"] is False
    assert recorder.calls == [
        "capture_previous_release",
        "capture_previous_health",
        "fence_writers",
        "drain_work",
        "create_backup",
        "verify_backup",
        "migrate_database",
        "activate_policy",
        "activate_release",
        "restart_services",
        "publish_generation",
        "prove_production_ready",
        "reopen_writers",
    ]
    assert not {"build", "dependency_sync", "install"} & set(recorder.calls)

    state = __import__("json").loads(state_path.read_text())
    assert state["promotion_id"] == approval.promotion_id
    assert state["artifact_id"] == approval.artifact_id
    assert state["staging_descriptor_digest"] == staging.descriptor_digest
    assert state["production_descriptor_digest"] == production.descriptor_digest
    assert state["supervision_evidence_digest"] == approval.supervision_evidence_digest
    assert state["mutation_boundary"] == "readiness"


def test_artifact_promotion_refuses_stale_or_changed_approval(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    approval = _approval(promotion_inputs)
    kwargs = {
        "approval": approval,
        "staging": staging,
        "production": production,
        "release": release,
        "staging_readiness": readiness,
        "supervision_evidence": supervision,
        "staging_service_plan": staging_plan,
        "production_service_plan": production_plan,
        "operations": _Recorder(release).operations(),
        "state_path": tmp_path / "promotion.json",
    }
    with pytest.raises(DeploymentEnvironmentInvalid, match="stale"):
        run_deployment_promotion(**kwargs, now=NOW + timedelta(seconds=301))

    changed_plan = {**production_plan, "topology": [{"kind": "foreign"}]}
    with pytest.raises(DeploymentEnvironmentInvalid, match="canonical validation"):
        run_deployment_promotion(
            **{**kwargs, "production_service_plan": changed_plan},
            now=NOW + timedelta(seconds=10),
        )

    untrusted = {
        **production_plan,
        "artifacts": [
            item
            for item in production_plan["artifacts"]
            if item["kind"] != "launcher"
        ],
    }
    with pytest.raises(DeploymentEnvironmentInvalid, match="canonical validation"):
        create_promotion_approval(
            staging=staging,
            production=production,
            release=release,
            staging_readiness=readiness,
            supervision_evidence=supervision,
            staging_service_plan=staging_plan,
            production_service_plan=untrusted,
            approved_by="operator",
            approved_at=NOW,
            max_age_seconds=300,
        )


def test_artifact_promotion_resume_requires_identical_identity(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    approval = _approval(promotion_inputs)
    state_path = tmp_path / "promotion.json"
    recorder = _Recorder(release)
    kwargs = {
        "approval": approval,
        "staging": staging,
        "production": production,
        "release": release,
        "staging_readiness": readiness,
        "supervision_evidence": supervision,
        "staging_service_plan": staging_plan,
        "production_service_plan": production_plan,
        "operations": recorder.operations(),
        "now": NOW + timedelta(seconds=10),
        "state_path": state_path,
    }
    assert run_deployment_promotion(**kwargs)["ok"] is True
    first_calls = list(recorder.calls)
    assert run_deployment_promotion(**kwargs)["ok"] is True
    assert recorder.calls == first_calls

    changed_approval = replace(approval, approved_by="different-operator")
    with pytest.raises(DeploymentEnvironmentInvalid, match="resume identity changed"):
        run_deployment_promotion(**{**kwargs, "approval": changed_approval})


def test_artifact_promotion_lock_refuses_concurrent_owner(tmp_path: Path) -> None:
    from okg.substrate.deployment_promotion import _promotion_lock

    lock_path = tmp_path / "promotion.lock"
    with _promotion_lock(lock_path):
        with pytest.raises(DeploymentEnvironmentInvalid, match="another promotion"):
            with _promotion_lock(lock_path):
                pass


@pytest.mark.parametrize(
    ("boundary", "step"),
    [
        ("release", "activate_release"),
        ("database", "migrate_database"),
        ("policy", "activate_policy"),
        ("services", "restart_services"),
        ("publish", "publish_generation"),
        ("readiness", "prove_production_ready"),
    ],
)
def test_artifact_promotion_rollback_at_each_mutation_boundary(
    promotion_inputs,
    tmp_path: Path,
    boundary: str,
    step: str,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    recorder = _Recorder(release, fail_once=(step,))

    result = run_deployment_promotion(
        approval=_approval(promotion_inputs),
        staging=staging,
        production=production,
        release=release,
        staging_readiness=readiness,
        supervision_evidence=supervision,
        staging_service_plan=staging_plan,
        production_service_plan=production_plan,
        operations=recorder.operations(),
        now=NOW + timedelta(seconds=10),
        state_path=tmp_path / f"{boundary}.json",
    )

    assert result["status"] == "rolled_back"
    assert result["mutation_boundary"] == boundary
    assert result["writers_fenced"] is False
    assert "activate_previous_release" in recorder.calls
    assert "prove_previous_ready" in recorder.calls
    assert "reopen_writers" in recorder.calls
    # Release activation now owns the service restart and therefore runs only
    # after database and policy mutation. Every mutation boundary can have
    # changed database state and must restore the verified backup on rollback.
    assert "restore_backup" in recorder.calls


def test_artifact_promotion_retries_blocked_recovery_after_approval_expires(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    recorder = _Recorder(
        release,
        fail_once=("migrate_database", "restore_backup"),
    )
    kwargs = {
        "approval": _approval(promotion_inputs),
        "staging": staging,
        "production": production,
        "release": release,
        "staging_readiness": readiness,
        "supervision_evidence": supervision,
        "staging_service_plan": staging_plan,
        "production_service_plan": production_plan,
        "operations": recorder.operations(),
        "state_path": tmp_path / "blocked-recovery.json",
    }

    first = run_deployment_promotion(
        **kwargs,
        now=NOW + timedelta(seconds=10),
    )
    assert first["status"] == "recovery_blocked"
    assert first["writers_fenced"] is True

    resumed = run_deployment_promotion(
        **kwargs,
        now=NOW + timedelta(hours=1),
    )
    assert resumed["status"] == "rolled_back"
    assert resumed["writers_fenced"] is False
    assert recorder.calls.count("restore_backup") == 2


def test_artifact_promotion_retries_recovery_operation_that_raised(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    recorder = _Recorder(
        release,
        fail_once=("verify_backup",),
        raise_once=("restart_previous_services",),
    )
    state_path = tmp_path / "raised-recovery-operation.json"
    kwargs = {
        "approval": _approval(promotion_inputs),
        "staging": staging,
        "production": production,
        "release": release,
        "staging_readiness": readiness,
        "supervision_evidence": supervision,
        "staging_service_plan": staging_plan,
        "production_service_plan": production_plan,
        "operations": recorder.operations(),
        "state_path": state_path,
    }

    first = run_deployment_promotion(
        **kwargs,
        now=NOW + timedelta(seconds=10),
    )
    assert first["status"] == "recovery_blocked"
    state = yaml.safe_load(state_path.read_text(encoding="utf-8"))
    intent = state["operation_intents"]["restart_previous_services"]
    assert intent["status"] == "failed"
    assert intent["last_error"]["code"] == "deployment_environment_invalid"
    assert intent["last_error"]["data"] == {
        "operation": "restart_previous_services"
    }

    resumed = run_deployment_promotion(
        **kwargs,
        now=NOW + timedelta(hours=1),
    )
    assert resumed["status"] == "aborted"
    assert resumed["writers_fenced"] is False
    assert recorder.calls.count("restart_previous_services") == 2


def test_previous_health_failure_preserves_successful_restore_dispositions(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    recorder = _Recorder(
        release,
        fail_once=("migrate_database", "prove_previous_ready"),
    )
    result = run_deployment_promotion(
        approval=_approval(promotion_inputs),
        staging=staging,
        production=production,
        release=release,
        staging_readiness=readiness,
        supervision_evidence=supervision,
        staging_service_plan=staging_plan,
        production_service_plan=production_plan,
        operations=recorder.operations(),
        state_path=tmp_path / "health-blocked-recovery.json",
        now=NOW + timedelta(seconds=10),
    )

    assert result["status"] == "recovery_blocked"
    assert result["recovery_dispositions"] == {
        "database": "restored",
        "release": "restored",
        "services": "restored",
        "persistent_health": "failed",
        "writers": "fenced",
    }


def test_artifact_promotion_crash_after_database_intent_resumes_with_rollback_only(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    recorder = _Recorder(release, crash_once=("migrate_database",))
    kwargs = {
        "approval": _approval(promotion_inputs),
        "staging": staging,
        "production": production,
        "release": release,
        "staging_readiness": readiness,
        "supervision_evidence": supervision,
        "staging_service_plan": staging_plan,
        "production_service_plan": production_plan,
        "operations": recorder.operations(),
        "state_path": tmp_path / "crashed.json",
    }

    with pytest.raises(_SimulatedCrash):
        run_deployment_promotion(
            **kwargs,
            now=NOW + timedelta(seconds=10),
        )

    resumed = run_deployment_promotion(
        **kwargs,
        now=NOW + timedelta(hours=1),
    )

    assert resumed["status"] == "rolled_back"
    assert recorder.calls.count("migrate_database") == 1
    assert "activate_policy" not in recorder.calls
    assert "restore_backup" in recorder.calls


def test_artifact_promotion_invalid_backup_rollback_restores_schedules(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    recorder = _Recorder(release, fail_once=("verify_backup",))

    result = run_deployment_promotion(
        approval=_approval(promotion_inputs),
        staging=staging,
        production=production,
        release=release,
        staging_readiness=readiness,
        supervision_evidence=supervision,
        staging_service_plan=staging_plan,
        production_service_plan=production_plan,
        operations=recorder.operations(),
        now=NOW + timedelta(seconds=10),
        state_path=tmp_path / "invalid-backup.json",
    )

    assert result["status"] == "aborted"
    assert result["mutation_boundary"] == "none"
    assert "activate_release" not in recorder.calls
    assert "restart_previous_services" in recorder.calls
    assert recorder.calls[-1] == "reopen_writers"


def test_artifact_promotion_rollback_reexecutes_ambiguous_restore(
    promotion_inputs,
    tmp_path: Path,
) -> None:
    staging, production, release, staging_plan, production_plan, readiness, supervision = promotion_inputs
    recorder = _Recorder(
        release,
        fail_once=("migrate_database",),
        crash_once=("restore_backup",),
    )
    kwargs = {
        "approval": _approval(promotion_inputs),
        "staging": staging,
        "production": production,
        "release": release,
        "staging_readiness": readiness,
        "supervision_evidence": supervision,
        "staging_service_plan": staging_plan,
        "production_service_plan": production_plan,
        "operations": recorder.operations(),
        "state_path": tmp_path / "ambiguous-restore.json",
    }

    with pytest.raises(_SimulatedCrash):
        run_deployment_promotion(
            **kwargs,
            now=NOW + timedelta(seconds=10),
        )
    resumed = run_deployment_promotion(
        **kwargs,
        now=NOW + timedelta(hours=1),
    )

    assert resumed["status"] == "rolled_back"
    assert recorder.calls.count("restore_backup") == 2
    assert resumed["writers_fenced"] is False


def test_activation_refuses_a_release_whose_module_provenance_moved(
    promotion_inputs, monkeypatch
) -> None:
    """#1179 slice 6: activation re-resolves the providers, never re-records.

    Wired through the real `_validate_candidate_binding` call site, so this
    proves the refusal fires where activation actually reaches it rather
    than only in the helper it delegates to.
    """
    from types import SimpleNamespace

    from okg.substrate.catalog.module_provenance import (
        ReleaseModuleProvenanceMismatch,
    )

    _staging, production, release, *_rest = promotion_inputs
    sealed = replace(
        release,
        inputs=replace(
            release.inputs,
            postgres_capacity_digest=production.postgres_capacity_digest,
            module_provenance=(
                {
                    "module_id": "archi:lab_units",
                    "owner": "archi",
                    "provider_kind": "package",
                    "package_release_digest": "sha256:sealed-from-this",
                    "object_digests": ["sha256:one"],
                },
            ),
        ),
    )
    installed = SimpleNamespace(
        module_origins={
            "archi:lab_units": {
                "owner": "archi",
                "provider_kind": "package",
                "package_release_digest": "sha256:installed-instead",
                "object_digests": ["sha256:two"],
            }
        }
    )
    monkeypatch.setattr(
        "okg.substrate.release_activation.validate_environment_release_paths",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda *_args, **_kwargs: installed,
    )

    with pytest.raises(ReleaseModuleProvenanceMismatch) as excinfo:
        _validate_candidate_binding(production, production, sealed)

    assert excinfo.value.data["module_id"] == "archi:lab_units"
    assert excinfo.value.data["recorded_release_digest"] == (
        "sha256:sealed-from-this"
    )
    assert excinfo.value.data["resolved_release_digest"] == (
        "sha256:installed-instead"
    )
