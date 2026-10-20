"""Canonical, versioned deployment-up phase contract.

The JSON artifact is inert data so the installed okg-ci baseline planner can
inspect a candidate without importing or executing candidate Python.  The
runtime loader is deliberately strict: unknown fields, phases, duplicates, or
schema versions refuse before lifecycle mutation.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError


CONTRACT_SCHEMA = "okg.deployment_up.phase_contract.v1"
CONTRACT_RESOURCE = "contracts/deployment_up_v1.json"
CONTRACT_REPO_PATH = "src/okg/substrate/contracts/deployment_up_v1.json"
#: The lifecycle planner that emits ``--lifecycle-plan`` receipts. It is the
#: okg-ci deployment's autoupdate gate, so a checkout without that deployment
#: (the public copy) has no planner and refuses a lifecycle plan by name.
PLANNER_REPO_PATH = "deployments/okg-ci/autoupdate_gate.py"
PLAN_SCHEMA = "okg.okg_ci_lifecycle_plan.v2"
LEGACY_PLAN_SCHEMA = "okg.okg_ci_lifecycle_plan.v1"
LEGACY_FULL_PHASES = tuple(phase for phase in (
    "preflight", "backup", "stop_services", "sync_code", "dependency_sync",
    "migrate", "catalog_apply", "nomos_baseline", "generation_recovery",
    "attribute_read_model", "restart_services", "smoke_publish", "readiness",
))
CONTRACT_FIELDS = frozenset({"schema", "contract_version", "phases"})
KNOWN_PHASES = frozenset({
    "preflight",
    "backup",
    "stop_services",
    "sync_code",
    "dependency_sync",
    "migrate",
    "catalog_apply",
    "nomos_baseline",
    "generation_recovery",
    "attribute_read_model",
    "restart_services",
    "source_convergence",
    "smoke_publish",
    "readiness",
})


@dataclass(frozen=True)
class DeploymentLifecycleContract:
    schema: str
    contract_version: int
    phases: tuple[str, ...]
    digest: str

    def receipt(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "contract_version": self.contract_version,
            "phases": list(self.phases),
            "digest": self.digest,
        }


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def contract_digest(payload: Mapping[str, Any]) -> str:
    semantic = {key: payload[key] for key in sorted(CONTRACT_FIELDS)}
    return "sha256:" + hashlib.sha256(_canonical_bytes(semantic)).hexdigest()


def validate_contract_payload(payload: Any) -> DeploymentLifecycleContract:
    if not isinstance(payload, Mapping):
        raise SubstrateError("deployment lifecycle contract must be a JSON object")
    unknown = sorted(set(payload) - CONTRACT_FIELDS)
    missing = sorted(CONTRACT_FIELDS - set(payload))
    if unknown or missing:
        raise SubstrateError(
            "deployment lifecycle contract has an invalid shape",
            data={"unknown_fields": unknown, "missing_fields": missing},
        )
    if payload.get("schema") != CONTRACT_SCHEMA:
        raise SubstrateError(
            "deployment lifecycle contract schema is unsupported",
            data={"schema": payload.get("schema"), "expected": CONTRACT_SCHEMA},
        )
    if payload.get("contract_version") != 1:
        raise SubstrateError(
            "deployment lifecycle contract version is unsupported",
            data={"contract_version": payload.get("contract_version")},
        )
    raw_phases = payload.get("phases")
    if (
        not isinstance(raw_phases, Sequence)
        or isinstance(raw_phases, (str, bytes))
        or not raw_phases
        or any(not isinstance(phase, str) or not phase for phase in raw_phases)
    ):
        raise SubstrateError("deployment lifecycle phases must be a non-empty string list")
    phases = tuple(raw_phases)
    duplicates = sorted({phase for phase in phases if phases.count(phase) > 1})
    unknown_phases = sorted(set(phases) - KNOWN_PHASES)
    if duplicates or unknown_phases or set(phases) != KNOWN_PHASES:
        raise SubstrateError(
            "deployment lifecycle phase set is invalid",
            data={
                "duplicates": duplicates,
                "unknown_phases": unknown_phases,
                "missing_phases": sorted(KNOWN_PHASES - set(phases)),
            },
        )
    return DeploymentLifecycleContract(
        schema=CONTRACT_SCHEMA,
        contract_version=1,
        phases=phases,
        digest=contract_digest(payload),
    )


def load_contract_path(path: Path) -> DeploymentLifecycleContract:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SubstrateError(
            "deployment lifecycle contract is unavailable or invalid JSON",
            data={"path": str(path)},
        ) from exc
    return validate_contract_payload(payload)


def canonical_deployment_lifecycle_contract() -> DeploymentLifecycleContract:
    resource = files("okg.substrate").joinpath(CONTRACT_RESOURCE)
    try:
        payload = json.loads(resource.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SubstrateError(
            "packaged deployment lifecycle contract is unavailable or invalid"
        ) from exc
    return validate_contract_payload(payload)


CANONICAL_CONTRACT = canonical_deployment_lifecycle_contract()
PHASES = CANONICAL_CONTRACT.phases


def validate_lifecycle_plan_receipt(
    path: Path,
    *,
    candidate_revision: str | None,
    planner_path: Path,
    planner_bytes: bytes | None = None,
    authorized_baseline_revision: str | None = None,
    bridge_state_file: Path | None = None,
) -> dict[str, Any]:
    """Bind a trusted-baseline plan to this candidate executor.

    The v1 branch is an explicit one-release bridge for the already-installed
    planner: it bound the immutable candidate planner bytes but predated the
    inert contract field.  It is accepted only for a full-lifecycle plan whose
    candidate revision and planner digest match this checkout exactly.  All
    plans emitted after this release use v2 and bind the inert contract itself.
    """
    try:
        raw = path.read_bytes()
        receipt = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise SubstrateError(
            "deployment lifecycle plan receipt is unavailable or invalid",
            data={"path": str(path)},
        ) from exc
    if not isinstance(receipt, Mapping):
        raise SubstrateError("deployment lifecycle plan receipt must be an object")
    if receipt.get("action") != "full_lifecycle":
        raise SubstrateError("deployment lifecycle plan did not authorize full lifecycle")
    if not candidate_revision or receipt.get("candidate_revision") != candidate_revision:
        raise SubstrateError(
            "deployment lifecycle plan candidate does not match this checkout",
            data={
                "planned": receipt.get("candidate_revision"),
                "observed": candidate_revision,
            },
        )
    if planner_bytes is None:
        try:
            planner_bytes = planner_path.read_bytes()
        except OSError as exc:
            raise SubstrateError(
                "candidate lifecycle planner bytes are unavailable",
                data={"path": str(planner_path)},
                fix_hint=(
                    "a lifecycle plan binds the okg-ci autoupdate planner "
                    f"({PLANNER_REPO_PATH}); this checkout does not have it, so "
                    "run the upgrade without --lifecycle-plan"
                ),
            ) from exc
    planner_digest = "sha256:" + hashlib.sha256(planner_bytes).hexdigest()
    if receipt.get("candidate_planner_digest") != planner_digest:
        raise SubstrateError("deployment lifecycle plan does not bind this candidate planner")

    schema = receipt.get("schema")
    selected = tuple(receipt.get("selected_phases") or ())
    admission: str
    if schema == PLAN_SCHEMA:
        if selected != PHASES:
            raise SubstrateError("deployment lifecycle plan phase order differs from contract")
        candidate_contract = receipt.get("candidate_contract")
        contract = validate_contract_payload(candidate_contract)
        if contract.digest != CANONICAL_CONTRACT.digest:
            raise SubstrateError("deployment lifecycle plan contract digest differs from executor")
        if receipt.get("candidate_contract_digest") != contract_digest(candidate_contract):
            raise SubstrateError("deployment lifecycle plan contract digest is invalid")
        admission = "contract_v2"
    elif schema == LEGACY_PLAN_SCHEMA:
        if selected != LEGACY_FULL_PHASES:
            raise SubstrateError("legacy lifecycle plan does not authorize the known full phase set")
        if (
            not authorized_baseline_revision
            or receipt.get("baseline_revision") != authorized_baseline_revision
            or not str(receipt.get("baseline_artifact") or "").startswith("sha256:")
            or bridge_state_file is None
        ):
            raise SubstrateError(
                "legacy lifecycle plan is not bound to its authorized baseline transition"
            )
        marker = path.with_name(f"{path.name}.legacy-bridge-consumed.json")
        marker_payload = {
            "schema": "okg.legacy_lifecycle_bridge_consumption.v1",
            "plan_digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
            "baseline_revision": authorized_baseline_revision,
            "candidate_revision": candidate_revision,
            "state_file": str(bridge_state_file.resolve()),
        }
        if marker.exists():
            try:
                observed_marker = json.loads(marker.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise SubstrateError("legacy lifecycle bridge marker is unreadable") from exc
            if observed_marker != marker_payload:
                raise SubstrateError(
                    "legacy lifecycle bridge was already consumed by another transition"
                )
        else:
            marker.parent.mkdir(parents=True, exist_ok=True)
            try:
                with marker.open("x", encoding="utf-8") as stream:
                    json.dump(marker_payload, stream, sort_keys=True)
                    stream.write("\n")
                os.chmod(marker, 0o600)
            except FileExistsError:
                raise SubstrateError(
                    "legacy lifecycle bridge consumption raced another transition"
                ) from None
        admission = "legacy_v1_candidate_planner_bridge"
    else:
        raise SubstrateError(
            "deployment lifecycle plan schema is unsupported",
            data={"schema": schema},
        )
    return {
        "schema": "okg.deployment_up.plan_admission.v1",
        "admission": admission,
        "plan_path": str(path),
        "plan_digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "candidate_revision": candidate_revision,
        "candidate_planner_digest": planner_digest,
        "contract": CANONICAL_CONTRACT.receipt(),
    }
