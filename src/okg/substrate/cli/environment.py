"""Read-only CLI for long-lived deployment environments."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.deployments.environment import (
    default_environments_dir,
    find_environment_path,
    load_environment,
    load_environment_fleet,
    resource_isolation_report,
)
from okg.substrate.errors import DeploymentEnvironmentInvalid, SubstrateError


def _cmd_environment(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.environment_subcmd == "seed-nomos-baseline":
        return _cmd_environment_seed_nomos_baseline(args, as_json=as_json)
    if args.environment_subcmd == "claim-owner":
        return _cmd_environment_claim_owner(args, as_json=as_json)
    if args.environment_subcmd == "clone-backup":
        return _cmd_environment_clone_backup(args, as_json=as_json)
    if args.environment_subcmd == "clone-restore":
        return _cmd_environment_clone_restore(args, as_json=as_json)
    if args.environment_subcmd == "clone-rebind":
        return _cmd_environment_clone_rebind(args, as_json=as_json)
    if args.environment_subcmd == "prepare-release":
        return _cmd_environment_prepare_release(args, as_json=as_json)
    if args.environment_subcmd == "activate-release":
        return _cmd_environment_activate_release(args, as_json=as_json)
    if args.environment_subcmd == "promote":
        return _cmd_environment_promote(args, as_json=as_json)
    if args.environment_subcmd == "product-status":
        return _cmd_environment_product_status(args, as_json=as_json)
    environment_ref = str(args.environment)
    root = _environment_root(
        environment_ref,
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(
            environment_ref,
            environments_dir=root,
        )
        if args.environment_subcmd == "show":
            from okg.substrate.release_activation import (
                environment_release_status,
            )

            release_status = environment_release_status(target)
            payload = {
                "schema": "okg.environment.show.v1",
                "ok": bool(release_status["ok"]),
                "environment": target.to_dict(),
                "release_status": release_status,
            }
            _emit(payload, as_json=as_json)
            return 0 if payload["ok"] else 1

        if args.environment_subcmd == "service":
            return _cmd_environment_service(
                args,
                target=target,
                root=root,
                as_json=as_json,
            )
        if args.environment_subcmd == "ready":
            return _cmd_environment_ready(
                args,
                target=target,
                root=root,
                as_json=as_json,
            )

        fleet = list(load_environment_fleet(root))
        if all(item.descriptor_path != target.descriptor_path for item in fleet):
            fleet.append(target)
        isolation = resource_isolation_report(fleet)
        payload = {
            "schema": "okg.environment.plan.v1",
            "ok": isolation["ok"],
            "environment": target.to_dict(),
            "environments_root": str(root),
            "resource_isolation": isolation,
        }
        _emit(payload, as_json=as_json)
        return 0 if isolation["ok"] else 1
    except DeploymentConfigError as exc:
        error = DeploymentEnvironmentInvalid(
            str(exc),
            data={
                "environment": environment_ref,
                "environments_root": str(root),
            },
            fix_hint=(
                "fix the environment descriptor, required env: secret, "
                "database identity, or colliding resolved resource"
            ),
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_product_status(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """Render one locked product-journal snapshot without mutation."""

    environment_ref = str(args.environment)
    root = _environment_root(
        environment_ref,
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(environment_ref, environments_dir=root)
        from okg.substrate.deployment_product_lifecycle import (
            read_product_apply_status,
            render_product_operation_status,
        )

        readback = read_product_apply_status(
            production=target,
            promotion_id=str(args.operation_id),
        )
        if as_json:
            print(json.dumps(readback, indent=2, default=str))
        else:
            print(render_product_operation_status(readback), end="")
        status = readback.get("status")
        state = status.get("state") if isinstance(status, dict) else None
        # The bounded slice cannot return success until the paired receipt
        # verifier has produced a completed live readback.
        return 0 if state == "succeeded" else 1
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(
                str(exc),
                data={
                    "environment": environment_ref,
                    "environments_root": str(root),
                },
            )
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_prepare_release(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from okg.substrate.sealed_release import prepare_environment_release

    root = _environment_root(
        str(args.environment),
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(str(args.environment), environments_dir=root)
        release = prepare_environment_release(
            environment=target,
            source_repo=Path(args.source_repo).expanduser().resolve(),
            revision=str(args.revision),
        )
        payload = {
            "schema": "okg.environment_prepare_release/v1",
            "ok": True,
            "environment_id": target.id,
            "release": release.to_dict(),
        }
        _emit(payload, as_json=as_json)
        return 0
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_activate_release(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from okg.substrate.sealed_release import verify_sealed_release

    root = _environment_root(
        str(args.environment),
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(str(args.environment), environments_dir=root)
        artifact_id = str(args.artifact)
        if not artifact_id.startswith("sha256:"):
            raise DeploymentEnvironmentInvalid("--artifact must be a sha256 identity")
        release = verify_sealed_release(
            target.release.artifact_store / artifact_id.removeprefix("sha256:"),
            expected_artifact_id=artifact_id,
        )
        payload: dict[str, Any] = {
            "schema": "okg.environment_activate_release/v1",
            "ok": True,
            "apply": bool(args.apply),
            "environment_id": target.id,
            "artifact_id": release.artifact_id,
            "code_revision": release.inputs.code_revision,
            "definition_digest": release.inputs.definition_digest,
        }
        if args.apply:
            from okg.substrate.environment_services import (
                activate_environment_release_with_handoff,
            )

            fleet = list(load_environment_fleet(root))
            if all(item.descriptor_path != target.descriptor_path for item in fleet):
                fleet.append(target)
            isolation = resource_isolation_report(fleet)
            payload["resource_isolation"] = isolation
            if not isolation["ok"]:
                payload.update(
                    {
                        "ok": False,
                        "status": "blocked",
                        "error": "environment resource isolation failed",
                    }
                )
                _emit(payload, as_json=as_json)
                return 1
            handoff = activate_environment_release_with_handoff(
                target,
                release,
                source_repo=Path(args.source_repo).expanduser().resolve(),
                activated_by=str(args.activated_by),
                adapter=str(args.adapter),
                launcher_python=str(args.launcher_python),
                systemd_scope=str(args.systemd_scope),
                postgres_image=str(args.postgres_image),
            )
            payload["activation"] = handoff["activation"]
            payload["handoff"] = handoff["handoff"]
            payload["service_handoff"] = handoff
            payload["ok"] = bool(handoff["ok"])
        _emit(payload, as_json=as_json)
        return 0 if payload["ok"] else 1
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_clone_backup(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from okg.substrate.environment_clone import create_environment_clone_backup

    root = _environment_root(
        str(args.target),
        configured=getattr(args, "environments_root", None),
    )
    try:
        source = load_environment(str(args.source), environments_dir=root)
        target = load_environment(str(args.target), environments_dir=root)
        payload = create_environment_clone_backup(
            source=source,
            target=target,
            dump_path=args.output,
            report_path=args.report,
            apply=bool(args.apply),
        )
        _emit(payload, as_json=as_json)
        return 0
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_clone_restore(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from okg.substrate.environment_clone import restore_environment_clone

    root = _environment_root(
        str(args.environment),
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(str(args.environment), environments_dir=root)
        isolation_evidence = None
        if args.apply:
            from okg.substrate.environment_services import (
                fence_environment_host_services,
                render_environment_services,
            )

            plan = render_environment_services(
                target,
                adapter=str(args.adapter),
                launcher_python=str(args.launcher_python),
                systemd_scope=str(args.systemd_scope),
                postgres_image=str(args.postgres_image),
            )
            isolation_evidence = fence_environment_host_services(
                plan,
                environment=target,
            )
        payload = restore_environment_clone(
            target=target,
            report_path=args.report,
            apply=bool(args.apply),
            isolation_evidence=isolation_evidence,
        )
        if isolation_evidence is not None:
            payload["host_service_fence"] = isolation_evidence
        _emit(payload, as_json=as_json)
        return 0
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_clone_rebind(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from okg.substrate.environment_clone import rebind_environment_clone

    root = _environment_root(
        str(args.environment),
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(str(args.environment), environments_dir=root)
        output = args.output or target.runtime.state_root / "clone-rebind.json"
        payload = rebind_environment_clone(
            target=target,
            report_path=args.report,
            output_path=output,
            apply=bool(args.apply),
        )
        _emit(payload, as_json=as_json)
        return 0
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_claim_owner(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from okg.substrate.environment_clone import claim_environment_database_owner

    root = _environment_root(
        str(args.environment),
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(str(args.environment), environments_dir=root)
        payload = claim_environment_database_owner(target, apply=bool(args.apply))
        _emit(payload, as_json=as_json)
        return 0 if payload["ok"] else 1
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_seed_nomos_baseline(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    from okg.substrate import fastyaml
    from okg.substrate.cli.provision import _seed_nomos_baseline

    root = _environment_root(
        str(args.environment),
        configured=getattr(args, "environments_root", None),
    )
    try:
        target = load_environment(str(args.environment), environments_dir=root)
        payload: dict[str, Any] = {
            "schema": "okg.environment_nomos_baseline/v1",
            "ok": True,
            "apply": bool(args.apply),
            "environment_id": target.id,
            "graph_deployment_name": target.graph_deployment_name,
            "definition_digest": target.definition_digest,
        }
        if args.apply:
            raw_manifest = fastyaml.safe_load(
                target.definition_path.read_text(encoding="utf-8")
            ) or {}
            raw_manifest = {
                **raw_manifest,
                "name": target.graph_deployment_name,
                "runtime": dict(target.runtime.config),
            }
            baseline = _seed_nomos_baseline(
                target.graph_deployment_name,
                target.definition_path.parent,
                dsn=target.database.dsn,
                raw_manifest=raw_manifest,
            )
            if baseline is None:
                raise DeploymentEnvironmentInvalid(
                    "environment does not require a Nomos runtime baseline"
                )
            payload["baseline"] = baseline
        _emit(payload, as_json=as_json)
        return 0
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _cmd_environment_promote(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.deployment_promotion import (
        create_promotion_approval,
        load_promotion_approval,
        run_deployment_promotion,
        save_promotion_approval,
        validate_staging_promotion_posture,
    )
    from okg.substrate.deployment_promotion_operations import (
        ProductionPromotionOperations,
        PromotionOperationConfig,
    )
    from okg.substrate.environment_services import render_environment_services
    from okg.substrate.sealed_release import verify_sealed_release

    root = _environment_root(
        str(args.production),
        configured=getattr(args, "environments_root", None),
    )
    try:
        staging = load_environment(str(args.staging), environments_dir=root)
        production = load_environment(str(args.production), environments_dir=root)
        artifact_id = str(args.artifact)
        if not artifact_id.startswith("sha256:"):
            raise DeploymentEnvironmentInvalid("--artifact must be a sha256 identity")
        artifact_root = (
            production.release.artifact_store
            / artifact_id.removeprefix("sha256:")
        )
        transfer_receipt: dict[str, Any] | None = None
        transfer_archive = getattr(args, "transfer_archive", None)
        transfer_envelope = getattr(args, "transfer_envelope", None)
        candidate_revision = getattr(args, "candidate_revision", None)
        if any((transfer_archive, transfer_envelope, candidate_revision)):
            if not all((transfer_archive, transfer_envelope, candidate_revision)):
                raise DeploymentEnvironmentInvalid(
                    "promotion transfer requires archive, envelope, and candidate revision"
                )
            from okg.substrate.deployment_promotion_transfer import (
                import_staged_release,
            )

            validate_staging_promotion_posture(staging)
            secret_name = str(getattr(args, "transfer_secret_env", ""))
            secret = os.environ.get(secret_name, "").encode()
            imported, transfer_receipt = import_staged_release(
                archive_path=Path(str(transfer_archive)).expanduser().resolve(),
                envelope_path=Path(str(transfer_envelope)).expanduser().resolve(),
                production_artifact_store=production.release.artifact_store,
                replay_ledger=(
                    production.runtime.state_root / "promotions/transfers.json"
                ),
                expected_promotion_id=str(args.promotion_id),
                expected_staging_environment_id=staging.id,
                expected_candidate_revision=str(candidate_revision),
                expected_definition_digest=staging.definition_digest,
                secret=secret,
                expected_staging_posture=(
                    staging.manifest.nomos or {}
                ).get("deployment_posture", {}),
                expected_production_definition_substance=(
                    production.definition_substance
                ),
                max_age_seconds=staging.readiness.max_age_seconds,
            )
            if imported.artifact_id != artifact_id:
                raise DeploymentEnvironmentInvalid(
                    "imported transfer artifact differs from --artifact"
                )
        release = verify_sealed_release(
            artifact_root,
            expected_artifact_id=artifact_id,
        )
        if transfer_receipt is not None:
            imported_evidence = transfer_receipt["evidence"]
            staging_evidence = dict(imported_evidence["readiness.json"])
            supervision_evidence = dict(imported_evidence["supervision.json"])
        else:
            if not args.supervision_evidence:
                raise DeploymentEnvironmentInvalid(
                    "local promotion requires --supervision-evidence"
                )
            staging_evidence_path = Path(
                args.staging_evidence or staging.readiness.evidence_path
            ).expanduser().resolve()
            supervision_evidence_path = Path(
                args.supervision_evidence
            ).expanduser().resolve()
            expected_readiness_path = staging.readiness.evidence_path.resolve()
            expected_supervision_path = (
                staging.runtime.state_root
                / "supervisor/promotion-supervision.json"
            ).resolve()
            if staging_evidence_path != expected_readiness_path:
                raise DeploymentEnvironmentInvalid(
                    "staging readiness evidence is outside environment authority",
                    data={
                        "expected": str(expected_readiness_path),
                        "observed": str(staging_evidence_path),
                    },
                )
            if supervision_evidence_path != expected_supervision_path:
                raise DeploymentEnvironmentInvalid(
                    "service supervision evidence is outside environment authority",
                    data={
                        "expected": str(expected_supervision_path),
                        "observed": str(supervision_evidence_path),
                    },
                )
            staging_evidence = _read_json_evidence(
                staging_evidence_path, label="staging readiness evidence"
            )
            supervision_evidence = _read_json_evidence(
                supervision_evidence_path, label="service supervision evidence"
            )
        plan_options = {
            "adapter": str(args.adapter),
            "launcher_python": str(args.launcher_python),
            "systemd_scope": str(args.systemd_scope),
            "postgres_image": str(args.postgres_image),
        }
        staging_plan = render_environment_services(staging, **plan_options)
        production_plan = render_environment_services(production, **plan_options)
        approval_path = (
            production.runtime.state_root
            / "promotions"
            / f"{args.promotion_id}.approval.json"
        )
        if approval_path.exists():
            approval = load_promotion_approval(approval_path)
        else:
            approval = create_promotion_approval(
                staging=staging,
                production=production,
                release=release,
                staging_readiness=staging_evidence,
                supervision_evidence=supervision_evidence,
                staging_service_plan=staging_plan,
                production_service_plan=production_plan,
                approved_by=str(args.approved_by),
                approved_at=datetime.now(timezone.utc),
                max_age_seconds=staging.readiness.max_age_seconds,
                promotion_id=str(args.promotion_id),
            )
        payload: dict[str, Any] = {
            "schema": "okg.environment_promotion_plan/v1",
            "ok": True,
            "apply": bool(args.apply),
            "approval": approval.to_dict(),
            "approval_digest": approval.digest,
            "approval_path": str(approval_path),
            "state_path": str(
                production.runtime.state_root
                / "promotions"
                / f"{approval.promotion_id}.json"
            ),
        }
        if not args.apply:
            _emit_promotion(payload, as_json=as_json)
            return 0
        save_promotion_approval(approval_path, approval)
        concrete = ProductionPromotionOperations(
            PromotionOperationConfig(
                source_repo=Path(args.source_repo).expanduser().resolve(),
                approved_service_plan=production_plan,
                adapter=str(args.adapter),
                launcher_python=str(args.launcher_python),
                systemd_scope=str(args.systemd_scope),
                postgres_image=str(args.postgres_image),
                backup_root=Path(args.backup_root).expanduser().resolve(),
                drain_timeout_seconds=float(args.drain_timeout_seconds),
                readiness_timeout_seconds=float(args.readiness_timeout_seconds),
            )
        )
        result = run_deployment_promotion(
            approval=approval,
            staging=staging,
            production=production,
            release=release,
            staging_readiness=staging_evidence,
            supervision_evidence=supervision_evidence,
            staging_service_plan=staging_plan,
            production_service_plan=production_plan,
            operations=concrete.operations(),
            now=datetime.now(timezone.utc),
        )
        payload.update(result)
        _emit_promotion(payload, as_json=as_json)
        return 0 if result["ok"] else 1
    except (DeploymentConfigError, SubstrateError) as exc:
        error = (
            exc
            if isinstance(exc, SubstrateError)
            else DeploymentEnvironmentInvalid(str(exc))
        )
        _emit_error(error, as_json=as_json)
        return 2


def _load_environment_service_fleet(target: Any, root: Path) -> tuple[list[Any], list[dict[str, str]]]:
    """Load the named target and best-effort siblings for service isolation."""
    fleet: list[Any] = [target]
    unresolved: list[dict[str, str]] = []
    for descriptor_path in sorted(root.glob("*/environment.yaml")):
        if descriptor_path == target.descriptor_path:
            continue
        try:
            sibling = load_environment(descriptor_path)
        except DeploymentConfigError as exc:
            message = str(exc)
            variables = sorted(set(re.findall(r"(?:env:|\$\{)([A-Z][A-Z0-9_]*)", message)))
            reason = "deployment_config_error"
            if variables:
                reason = "missing_environment_variables:" + ",".join(variables)
            unresolved.append({"descriptor_path": str(descriptor_path), "reason": reason})
            continue
        fleet.append(sibling)
    ids = [environment.id for environment in fleet]
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    if duplicates:
        raise DeploymentConfigError(f"environment root {root} contains duplicate id(s): {duplicates}")
    return fleet, unresolved


def _cmd_environment_service(
    args: argparse.Namespace,
    *,
    target: Any,
    root: Path,
    as_json: bool,
) -> int:
    from okg.substrate.environment_services import (
        environment_service_lifecycle_transaction,
        execute_environment_service_action,
        install_environment_services,
        reconcile_environment_database_credentials,
        render_environment_services,
        require_ui_token_for_sensitive_deployment,
        write_environment_service_supervision_evidence,
    )

    fleet, unresolved = _load_environment_service_fleet(target, root)
    isolation = dict(resource_isolation_report(fleet))
    isolation["complete"] = not unresolved
    isolation["unresolved_environments"] = unresolved
    if unresolved:
        isolation["warning"] = "resource isolation covers resolved environments only; fleet proof is incomplete"
    action = str(args.environment_service_subcmd)
    if not isolation["ok"]:
        payload = {
            "schema": "okg.environment.service.v1",
            "ok": False,
            "action": action,
            "environment": target.to_dict(),
            "resource_isolation": isolation,
            "error": "environment resource isolation failed",
        }
        _emit_service(payload, as_json=as_json)
        return 1

    if action in {"install", "start", "restart"}:
        # Only the verbs that install or start the UI need its read token.
        # render, stop, logs and status stay usable on a misconfigured
        # environment so an operator can inspect and stop it.
        require_ui_token_for_sensitive_deployment(target)
    plan = render_environment_services(
        target,
        adapter=str(args.adapter),
        launcher_python=str(args.launcher_python),
        systemd_scope=str(args.systemd_scope),
        postgres_image=str(args.postgres_image),
    )
    payload: dict[str, Any] = {
        "schema": "okg.environment.service.v1",
        "ok": True,
        "action": action,
        "apply": bool(getattr(args, "apply", False)),
        "environment": target.to_dict(),
        "resource_isolation": isolation,
        "service_plan": plan,
        "worker_authority": plan["worker_authority"],
        "service_authority": plan["service_authority"],
        "installed": None,
        "database_credentials": None,
        "results": [],
    }
    if action == "render":
        _emit_service(payload, as_json=as_json)
        return 0
    if not payload["apply"]:
        if action == "status":
            payload["ok"] = bool(
                plan["installation"]["ok"]
                and payload["worker_authority"]["ok"]
                and all(
                    row["ok"] for row in payload["service_authority"].values()
                )
            )
        elif action in {"start", "restart"}:
            payload["ok"] = bool(plan["startable"])
        _emit_service(payload, as_json=as_json)
        return 0 if payload["ok"] else 1
    if action == "install":
        with environment_service_lifecycle_transaction(target):
            payload["installed"] = install_environment_services(
                target,
                plan,
            ).to_dict()
            payload["database_credentials"] = (
                reconcile_environment_database_credentials(target)
            )
            results = execute_environment_service_action(
                plan,
                action=action,
                environment=target,
            )
    else:
        results = execute_environment_service_action(
            plan,
            action=action,
            environment=target,
        )
    payload["results"] = results
    payload["ok"] = all(row["returncode"] == 0 for row in results)
    if payload["ok"] and action in {"start", "restart"}:
        payload["supervision_evidence"] = (
            write_environment_service_supervision_evidence(
                target,
                plan,
                action=action,
                results=results,
            )
        )
    _emit_service(payload, as_json=as_json)
    return 0 if payload["ok"] else 1


def _cmd_environment_ready(
    args: argparse.Namespace,
    *,
    target: Any,
    root: Path,
    as_json: bool,
) -> int:
    from okg.substrate.environment_clone import (
        finalize_environment_clone_rebind_report,
    )
    from okg.substrate.environment_readiness import environment_readiness_report
    from okg.substrate.environment_services import render_environment_services

    fleet = list(load_environment_fleet(root))
    if all(item.descriptor_path != target.descriptor_path for item in fleet):
        fleet.append(target)
    isolation = resource_isolation_report(fleet)
    if not isolation["ok"]:
        payload = {
            "schema": "okg.environment_readiness/v1",
            "ok": False,
            "status": "blocked",
            "environment_id": target.id,
            "resource_isolation": isolation,
            "checks": [],
            "blockers": [{
                "id": "environment.resource_isolation",
                "status": "blocker",
                "message": "environment resource isolation failed",
                "details": isolation,
            }],
        }
    else:
        plan = render_environment_services(
            target,
            adapter=str(args.adapter),
            launcher_python=str(args.launcher_python),
            systemd_scope=str(args.systemd_scope),
            postgres_image=str(args.postgres_image),
        )
        payload = environment_readiness_report(
            target,
            plan,
            timeout_seconds=float(args.timeout_seconds),
        )
        payload["resource_isolation"] = isolation
    payload.setdefault("captured_at", datetime.now(timezone.utc).isoformat())
    payload["evidence_path"] = str(target.readiness.evidence_path)
    _write_runtime_evidence(target.readiness.evidence_path, payload)
    if target.readiness.requires_clone_rebind:
        regeneration = finalize_environment_clone_rebind_report(target)
        check = {
            "id": "clone.regeneration",
            "ok": regeneration.get("ok") is True,
            "status": (
                "pass" if regeneration.get("ok") is True else "blocker"
            ),
            "message": (
                "clone-local state was regenerated and counted"
                if regeneration.get("ok") is True
                else "clone-local state regeneration is incomplete"
            ),
            "details": regeneration,
        }
        payload.setdefault("checks", []).append(check)
        payload["clone_regeneration"] = regeneration
        if regeneration.get("ok") is not True:
            payload["ok"] = False
            payload["status"] = "blocked"
            payload.setdefault("blockers", []).append(check)
        _write_runtime_evidence(target.readiness.evidence_path, payload)
    if as_json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(f"environment: {target.id}")
        print(f"  readiness: {payload['status']}")
        for check in payload.get("checks") or []:
            print(f"  {check['id']}: {check['status']}")
    return 0 if payload["ok"] else 1


def _write_runtime_evidence(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _environment_root(environment: str, *, configured: str | None) -> Path:
    if configured:
        return Path(configured).expanduser().resolve()
    active_descriptor = os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR")
    if active_descriptor:
        descriptor = find_environment_path(active_descriptor)
        return descriptor.parent.parent
    candidate = Path(environment).expanduser()
    if candidate.is_absolute() or len(candidate.parts) > 1:
        descriptor = find_environment_path(candidate)
        return descriptor.parent.parent
    return default_environments_dir()


_ENVIRONMENT_IDENTITY_FIELDS = (
    "graph_deployment_name",
    "database",
    "definition_digest",
    "artifact_id",
    "code_revision",
)


def _print_environment_descriptor(environment: dict[str, Any]) -> None:
    """Render a full environment descriptor (``show`` and ``plan``)."""
    print(f"environment: {environment['id']}")
    print(f"  tier: {environment['tier']}")
    print(f"  graph_deployment_name: {environment['graph_deployment_name']}")
    print(f"  definition_digest: {environment['definition']['digest']}")
    print(f"  database: {environment['database']['name']}")
    print(f"  worker_service: {environment['runtime']['worker_service']}")


def _print_environment_identity(payload: dict[str, Any]) -> None:
    """Render an action payload that carries scalar identity keys.

    ``prepare-release``, ``activate-release``, ``clone-backup``,
    ``clone-restore``, ``clone-rebind``, ``claim-owner``, and
    ``seed-nomos-baseline`` report ``environment_id`` -- sometimes only
    nested under ``target`` -- instead of a resolved descriptor under
    ``environment``.
    """
    nested = payload.get("target")
    if not isinstance(nested, dict):
        nested = {}
    environment_id = payload.get("environment_id") or nested.get("environment_id")
    if environment_id:
        print(f"environment: {environment_id}")
    if payload.get("schema"):
        print(f"  schema: {payload['schema']}")
    if "ok" in payload:
        print(f"  status: {'ok' if payload['ok'] else 'blocked'}")
    if "apply" in payload:
        print(f"  apply: {payload['apply']}")
    for field in _ENVIRONMENT_IDENTITY_FIELDS:
        value = payload.get(field)
        if value is None:
            value = nested.get(field)
        if value is not None:
            print(f"  {field}: {value}")


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, default=str))
        return
    environment = payload.get("environment")
    if isinstance(environment, dict):
        _print_environment_descriptor(environment)
    else:
        _print_environment_identity(payload)
    if "release_status" in payload:
        release = payload["release_status"]
        print(f"  release_status: {release['status']}")
        if release.get("artifact_id"):
            print(f"  artifact_id: {release['artifact_id']}")
    if "resource_isolation" in payload:
        isolation = payload["resource_isolation"]
        print(f"  resource_isolation: {'pass' if isolation['ok'] else 'blocked'}")
        for collision in isolation["collisions"]:
            print(
                f"    collision {collision['collision_domain']}: {collision['value']}"
            )


def _emit_error(error: SubstrateError, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(error.to_json(), indent=2), file=sys.stderr)
        return
    print(f"error: {error.message}", file=sys.stderr)
    if error.fix_hint:
        print(f"fix: {error.fix_hint}", file=sys.stderr)


def _emit_service(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, default=str))
        return
    environment = payload["environment"]
    print(f"environment: {environment['id']}")
    print(f"  action: {payload['action']}")
    print(f"  status: {'ok' if payload['ok'] else 'blocked'}")
    plan = payload.get("service_plan") or {}
    if plan:
        print(f"  adapter: {plan['adapter']}")
        print(f"  launchable: {plan['launchable']}")
        for row in (plan.get("commands") or {}).get(payload["action"], []):
            print(f"  command: {row['display']}")


def _read_json_evidence(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise DeploymentEnvironmentInvalid(
            f"{label} is not a regular file",
            data={"path": str(path)},
        )
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise DeploymentEnvironmentInvalid(
            f"{label} mode must be 0600",
            data={"path": str(path)},
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DeploymentEnvironmentInvalid(
            f"{label} is unreadable",
            data={"path": str(path)},
        ) from exc
    if not isinstance(raw, dict):
        raise DeploymentEnvironmentInvalid(f"{label} must contain a JSON object")
    return raw


def _emit_promotion(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, default=str))
        return
    print(f"promotion: {payload['approval']['promotion_id']}")
    print(f"  artifact: {payload['approval']['artifact_id']}")
    print(f"  apply: {payload['apply']}")
    if payload.get("status"):
        print(f"  status: {payload['status']}")
