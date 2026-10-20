"""Atomic environment activation for sealed deployment releases."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from okg.substrate.deployments.environment import (
    DeploymentEnvironment,
    load_environment,
    shared_definition_substance_problems,
)
from okg.substrate.catalog.module_provenance import (
    module_provenance_from_composed,
    refuse_release_provenance_mismatch,
)
from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.errors import SealedReleaseInvalid
from okg.substrate.sealed_release import (
    SealedRelease,
    validate_environment_release_paths,
    verify_sealed_release,
)


ACTIVE_RELEASE_SCHEMA = "okg.active_release/v1"
LAUNCH_DESCRIPTOR_SCHEMA = "okg.release_launcher/v1"
RELEASE_PREDECESSOR_SCHEMA = "okg.release_predecessor/v1"
RELEASE_ACTIVATION_TRANSACTION_SCHEMA = "okg.release_activation_transaction/v1"


@dataclass(frozen=True)
class ActivationResult:
    environment_id: str
    artifact_id: str
    definition_digest: str
    active_pointer_digest: str
    active_pointer: Path
    previous_pointer: Path
    launcher_descriptor: Path
    changed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.release_activation/v1",
            "environment_id": self.environment_id,
            "artifact_id": self.artifact_id,
            "definition_digest": self.definition_digest,
            "active_pointer_digest": self.active_pointer_digest,
            "active_pointer": str(self.active_pointer),
            "previous_pointer": str(self.previous_pointer),
            "launcher_descriptor": str(self.launcher_descriptor),
            "changed": self.changed,
        }


def launcher_descriptor_path(environment: DeploymentEnvironment) -> Path:
    return environment.runtime.state_root / "release-launcher.json"


def activate_environment_release(
    *,
    environment: DeploymentEnvironment,
    release: SealedRelease,
    source_repo: str | Path,
    activated_by: str,
    environ: Mapping[str, str] | None = None,
    staging_environment: DeploymentEnvironment | None = None,
    approved_definition_digest: str | None = None,
) -> ActivationResult:
    """Atomically select a verified release and retain the prior pointer."""
    actor = activated_by.strip()
    if not actor:
        raise SealedReleaseInvalid("sealed release activation requires an actor")
    values = dict(os.environ if environ is None else environ)
    verified = verify_sealed_release(
        release.artifact_root,
        expected_inputs=release.inputs,
        expected_artifact_id=release.artifact_id,
    )
    candidate, descriptor_relative = _candidate_environment(
        environment=environment,
        release=verified,
        source_repo=Path(source_repo),
        environ=values,
    )
    _validate_candidate_binding(
        environment,
        candidate,
        verified,
        staging_environment=staging_environment,
        approved_definition_digest=approved_definition_digest,
    )

    artifact_store = environment.release.artifact_store.resolve()
    if verified.artifact_root.parent != artifact_store:
        raise SealedReleaseInvalid(
            "sealed release is outside the environment artifact store",
            data={
                "expected": str(artifact_store),
                "observed": str(verified.artifact_root),
            },
        )

    descriptor_path = verified.repo_root / descriptor_relative
    active_payload = _active_payload(
        environment=environment,
        release=verified,
        descriptor_relative=descriptor_relative,
        descriptor_path=descriptor_path,
        activated_by=actor,
        release_definition_digest=(
            approved_definition_digest
            or getattr(verified.inputs, "definition_digest", None)
            or environment.definition_digest
        ),
    )
    launcher_path = launcher_descriptor_path(environment)
    launcher_payload = _launcher_payload(environment)
    active_path = environment.release.active_pointer
    previous_path = environment.release.previous_pointer

    with release_activation_lock(active_path):
        _recover_release_activation_transaction(
            environment,
            environ=values,
        )
        current = load_active_release(active_path, required=False)
        previous = load_active_release(previous_path, required=False)
        if current is None and previous is not None:
            raise SealedReleaseInvalid(
                "release activation found a previous pointer without an active pointer",
                data={"observed": str(previous_path)},
            )
        if current is not None:
            _validate_historical_pointer_release(
                environment,
                current,
                environ=values,
            )
        changed = not (
            current is not None and _same_active_binding(current, active_payload)
        )
        duplicate_root_recovery = bool(
            changed
            and current is not None
            and _is_duplicate_root_pointer_state(current, previous)
        )
        if current is not None and not duplicate_root_recovery:
            # Activation is itself an authority read.  Even a no-op request
            # must not report success for a bound pointer whose predecessor is
            # absent or contradictory.
            _validate_predecessor_binding(
                active=current,
                previous=previous,
            )
        if changed and current is not None:
            if previous is not None and not _same_active_binding(
                previous,
                active_payload,
            ):
                _require_current_runtime_handoff_before_rotation(
                    environment,
                    current=current,
                    previous=previous,
                    environ=values,
                )
        _atomic_write_json(launcher_path, launcher_payload)
        if changed:
            active_payload["predecessor"] = (
                _predecessor_binding(current) if current is not None else None
            )
            if current is not None:
                transaction_path = _release_activation_transaction_path(environment)
                _atomic_write_json(
                    transaction_path,
                    {
                        "schema": RELEASE_ACTIVATION_TRANSACTION_SCHEMA,
                        "environment_id": environment.id,
                        "active_before": current,
                        "previous_before": previous,
                        "active_after": active_payload,
                    },
                )
                _atomic_write_json(previous_path, current)
            _atomic_write_json(active_path, active_payload)
            if current is not None:
                _durable_unlink(transaction_path)
            selected_pointer = active_payload
        else:
            assert current is not None
            selected_pointer = current

        selected_definition_digest = _required_sha256(
            selected_pointer,
            "definition_digest",
        )
        selected_pointer_digest = active_release_pointer_digest(selected_pointer)

    return ActivationResult(
        environment_id=environment.id,
        artifact_id=verified.artifact_id,
        definition_digest=selected_definition_digest,
        active_pointer_digest=selected_pointer_digest,
        active_pointer=active_path,
        previous_pointer=previous_path,
        launcher_descriptor=launcher_path,
        changed=changed,
    )


def _require_current_runtime_handoff_before_rotation(
    environment: DeploymentEnvironment,
    *,
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
    environ: Mapping[str, str],
) -> None:
    """Preserve the only pointer that can authorize a still-live generation."""
    if _is_duplicate_root_pointer_state(current, previous):
        # A failed first active-pointer replace can leave the root pointer
        # duplicated in both files.  Retrying A -> B does not discard a unique
        # historical authority; validate both copies before admitting recovery.
        _validate_historical_pointer_release(environment, current, environ=environ)
        _validate_historical_pointer_release(environment, previous, environ=environ)
        return

    ancestry = _validate_predecessor_binding(active=current, previous=previous)
    if ancestry != "bound":
        raise SealedReleaseInvalid(
            "release activation cannot rotate an unbound live predecessor",
            data={"code": "release_handoff_incomplete"},
        )
    current_environment = historical_release_environment(
        environment,
        current,
        environ=environ,
    )
    previous_environment = historical_release_environment(
        environment,
        previous,
        environ=environ,
    )
    current_release = _validate_historical_pointer_release(
        environment,
        current,
        environ=environ,
    )
    previous_release = _validate_historical_pointer_release(
        environment,
        previous,
        environ=environ,
    )
    # Local imports avoid the release-activation/environment-services import
    # cycle.  Missing authority files are not proof that no manager process
    # remains: first preserve broken/symlinked paths as evidence, then query
    # every current and predecessor service target directly from the manager.
    from okg.substrate.environment_services import (  # noqa: PLC0415
        _environment_release_handoff_status_locked,
        _supervised_service_ids,
        environment_manager_process_census,
    )

    authority_paths = (
        environment.runtime.state_root / "supervisor",
        environment.runtime.state_root / "worker-authority.json",
        environment.runtime.state_root / "service-authority",
    )
    runtime_contract_present = any(
        path.exists() or path.is_symlink() for path in authority_paths
    )
    manager_census: Mapping[str, Any] | None = None
    if not runtime_contract_present:
        service_ids = tuple(dict.fromkeys(
            (
                *_supervised_service_ids(current_environment),
                *_supervised_service_ids(previous_environment),
            )
        ))
        manager_census = environment_manager_process_census(
            current_environment,
            service_ids=service_ids,
        )
        if manager_census.get("ok") is not True:
            raise SealedReleaseInvalid(
                "release activation cannot prove that receipt-less service "
                "managers are stopped",
                data={
                    "code": "release_handoff_incomplete",
                    "manager_census_status": manager_census.get("status"),
                },
            )
        runtime_contract_present = bool(
            manager_census.get("process_present") is True
        )
    if not runtime_contract_present:
        return

    release_status = {
        "schema": "okg.environment_release_status/v1",
        "ok": True,
        "status": "active",
        "environment_id": environment.id,
        "artifact_id": current_release.artifact_id,
        "artifact_root": str(current_release.artifact_root),
        "definition_digest": _required_sha256(current, "definition_digest"),
        "active_pointer_digest": active_release_pointer_digest(current),
        "active_pointer": str(environment.release.active_pointer),
        "previous_artifact_id": previous_release.artifact_id,
        "previous_artifact_root": str(previous_release.artifact_root),
        "previous_definition_digest": _required_sha256(
            previous, "definition_digest"
        ),
        "previous_pointer_digest": active_release_pointer_digest(previous),
        "ancestry_status": ancestry,
    }
    handoff = _environment_release_handoff_status_locked(
        current_environment,
        release_status=release_status,
        environ=environ,
    )
    if handoff.get("ok") is not True:
        raise SealedReleaseInvalid(
            "release activation requires the current runtime handoff before "
            "rotating its predecessor authority",
            data={
                "code": "release_handoff_incomplete",
                "artifact_id": current_release.artifact_id,
                "handoff_status": handoff.get("status"),
                "process_status": (
                    (handoff.get("process_authority") or {}).get("status")
                ),
                "predecessor_stop_status": (
                    (handoff.get("predecessor_stop") or {}).get("status")
                ),
                "manager_census_status": (
                    manager_census.get("status")
                    if manager_census is not None
                    else None
                ),
            },
        )


def load_active_release(
    path: str | Path, *, required: bool = True
) -> dict[str, Any] | None:
    target = Path(os.path.abspath(Path(path).expanduser()))
    if not target.exists() and not target.is_symlink() and not required:
        return None
    raw = _read_regular_json(target, label="active release pointer")
    if raw.get("schema") != ACTIVE_RELEASE_SCHEMA:
        raise SealedReleaseInvalid(
            "active release pointer has the wrong schema",
            data={"observed": raw.get("schema"), "artifact_root": str(target)},
        )
    return raw


def _release_activation_transaction_path(
    environment: DeploymentEnvironment,
) -> Path:
    active_path = environment.release.active_pointer
    return active_path.with_suffix(active_path.suffix + ".transaction.json")


def _recover_release_activation_transaction(
    environment: DeploymentEnvironment,
    *,
    environ: Mapping[str, str],
) -> None:
    """Recover only an exact, validated interrupted pointer replacement."""
    transaction_path = _release_activation_transaction_path(environment)
    if not transaction_path.exists() and not transaction_path.is_symlink():
        return
    transaction = _read_private_regular_json(
        transaction_path,
        label="release activation transaction",
    )
    expected_fields = {
        "schema",
        "environment_id",
        "active_before",
        "previous_before",
        "active_after",
    }
    if set(transaction) != expected_fields:
        _raise_invalid_activation_transaction(
            transaction_path,
            "release activation transaction fields are incomplete",
        )
    if transaction.get("schema") != RELEASE_ACTIVATION_TRANSACTION_SCHEMA:
        _raise_invalid_activation_transaction(
            transaction_path,
            "release activation transaction has the wrong schema",
        )
    if transaction.get("environment_id") != environment.id:
        _raise_invalid_activation_transaction(
            transaction_path,
            "release activation transaction names another environment",
        )

    active_before = transaction.get("active_before")
    previous_before = transaction.get("previous_before")
    active_after = transaction.get("active_after")
    if not isinstance(active_before, dict) or not isinstance(active_after, dict):
        _raise_invalid_activation_transaction(
            transaction_path,
            "release activation transaction pointers must be JSON objects",
        )
    if previous_before is not None and not isinstance(previous_before, dict):
        _raise_invalid_activation_transaction(
            transaction_path,
            "release activation transaction previous pointer is invalid",
        )
    for label, pointer in (
        ("active_before", active_before),
        ("active_after", active_after),
        ("previous_before", previous_before),
    ):
        if pointer is None:
            continue
        if pointer.get("schema") != ACTIVE_RELEASE_SCHEMA:
            _raise_invalid_activation_transaction(
                transaction_path,
                f"release activation transaction {label} has the wrong schema",
            )
        _validate_historical_pointer_release(
            environment,
            pointer,
            environ=environ,
        )

    duplicate_root = _is_duplicate_root_pointer_state(
        active_before,
        previous_before,
    )
    if not duplicate_root:
        ancestry = _validate_predecessor_binding(
            active=active_before,
            previous=previous_before,
        )
        if ancestry == "legacy_unbound" and previous_before is not None:
            _raise_invalid_activation_transaction(
                transaction_path,
                "release activation transaction has unbound prior ancestry",
            )
    if _same_active_binding(active_before, active_after):
        _raise_invalid_activation_transaction(
            transaction_path,
            "release activation transaction does not change the active release",
        )
    if active_after.get("predecessor") != _predecessor_binding(active_before):
        _raise_invalid_activation_transaction(
            transaction_path,
            "release activation transaction successor ancestry is invalid",
        )
    _validate_predecessor_binding(
        active=active_after,
        previous=active_before,
    )

    active_path = environment.release.active_pointer
    previous_path = environment.release.previous_pointer
    observed_active = load_active_release(active_path, required=False)
    observed_previous = load_active_release(previous_path, required=False)
    journal_only = (
        observed_active == active_before
        and observed_previous == previous_before
    )
    previous_replaced = (
        observed_active == active_before
        and observed_previous == active_before
    )
    pointers_replaced = (
        observed_active == active_after
        and observed_previous == active_before
    )
    if journal_only:
        _durable_unlink(transaction_path)
        return
    if previous_replaced:
        if previous_before is None:
            _durable_unlink(previous_path)
        else:
            _atomic_write_json(previous_path, previous_before)
        _durable_unlink(transaction_path)
        return
    if pointers_replaced:
        _durable_unlink(transaction_path)
        return
    _raise_invalid_activation_transaction(
        transaction_path,
        "release activation transaction contradicts the pointer state",
    )


def _raise_invalid_activation_transaction(path: Path, message: str) -> None:
    raise SealedReleaseInvalid(
        message,
        data={
            "code": "release_activation_transaction_invalid",
            "observed": str(path),
        },
    )


def environment_release_status(
    environment: DeploymentEnvironment,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return fail-closed active/previous release provenance for readiness."""
    active_path = environment.release.active_pointer
    if environment.release.mode == "tracking":
        return {
            "schema": "okg.environment_release_status/v1",
            "ok": True,
            "status": "tracking",
            "environment_id": environment.id,
            "active_pointer": str(active_path),
            "artifact_id": None,
        }
    if not active_path.exists():
        return {
            "schema": "okg.environment_release_status/v1",
            "ok": False,
            "status": "missing",
            "environment_id": environment.id,
            "active_pointer": str(active_path),
            "artifact_id": None,
        }
    try:
        with release_activation_lock(active_path, exclusive=False):
            return _environment_release_status_locked(
                environment,
                environ=environ,
            )
    except Exception as exc:  # noqa: BLE001 - status reports invalid authority.
        return {
            "schema": "okg.environment_release_status/v1",
            "ok": False,
            "status": "invalid",
            "environment_id": environment.id,
            "active_pointer": str(active_path),
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _environment_release_status_locked(
    environment: DeploymentEnvironment,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Validate one stable active/previous pointer snapshot under the lock."""
    active_path = environment.release.active_pointer
    try:
        active = load_active_release(active_path)
        assert active is not None
        values = dict(os.environ if environ is None else environ)
        release = _validate_pointer_release(
            environment,
            active,
            environ=values,
        )
        previous = load_active_release(
            environment.release.previous_pointer,
            required=False,
        )
        previous_release = (
            _validate_historical_pointer_release(
                environment,
                previous,
                environ=values,
            )
            if previous is not None
            else None
        )
        ancestry_status = _validate_predecessor_binding(
            active=active,
            previous=previous,
        )
        previous_definition_digest = (
            _required_sha256(previous, "definition_digest")
            if previous is not None and ancestry_status == "bound"
            else None
        )
        previous_pointer_digest = (
            active_release_pointer_digest(previous)
            if previous is not None and ancestry_status == "bound"
            else None
        )
        return {
            "schema": "okg.environment_release_status/v1",
            "ok": True,
            "status": "active",
            "environment_id": environment.id,
            "artifact_id": release.artifact_id,
            "artifact_root": str(release.artifact_root),
            "code_revision": release.inputs.code_revision,
            "input_id": release.inputs.input_id,
            "executable_tree_digest": release.executable_tree_digest,
            "definition_digest": _required_sha256(active, "definition_digest"),
            "active_pointer_digest": active_release_pointer_digest(active),
            "active_pointer": str(active_path),
            "previous_artifact_id": (
                previous_release.artifact_id
                if previous_release is not None
                else None
            ),
            "previous_artifact_root": (
                str(previous_release.artifact_root)
                if previous_release is not None and ancestry_status == "bound"
                else None
            ),
            "previous_definition_digest": previous_definition_digest,
            "previous_pointer_digest": previous_pointer_digest,
            "ancestry_status": ancestry_status,
            "launcher_descriptor": str(launcher_descriptor_path(environment)),
        }
    except Exception as exc:  # noqa: BLE001 - status reports invalid authority.
        return {
            "schema": "okg.environment_release_status/v1",
            "ok": False,
            "status": "invalid",
            "environment_id": environment.id,
            "active_pointer": str(active_path),
            "error": type(exc).__name__,
            "message": str(exc),
        }


def active_release_pointer_digest(pointer: Mapping[str, Any]) -> str:
    """Return the canonical identity of one immutable release pointer."""
    encoded = json.dumps(
        dict(pointer),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _predecessor_binding(pointer: Mapping[str, Any]) -> dict[str, str]:
    return {
        "schema": RELEASE_PREDECESSOR_SCHEMA,
        "artifact_id": _required_sha256(pointer, "artifact_id"),
        "definition_digest": _required_sha256(pointer, "definition_digest"),
        "pointer_digest": active_release_pointer_digest(pointer),
    }


def _validate_predecessor_binding(
    *,
    active: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
) -> str:
    """Prove that `previous` is the exact pointer displaced by `active`."""
    if "predecessor" not in active:
        # Existing v1 pointers remain readable so their running services can be
        # inspected and safely replaced. They do not carry enough evidence to
        # authorize a previous-release handoff; callers receive no predecessor
        # digest or definition from this legacy state.
        return "legacy_unbound"
    predecessor = active.get("predecessor")
    if predecessor is None:
        if previous is not None:
            raise SealedReleaseInvalid(
                "root release pointer has an unexpected previous release",
            )
        return "root"
    if not isinstance(predecessor, Mapping):
        raise SealedReleaseInvalid("release predecessor binding is invalid")
    expected_fields = {
        "schema",
        "artifact_id",
        "definition_digest",
        "pointer_digest",
    }
    if set(predecessor) != expected_fields:
        raise SealedReleaseInvalid("release predecessor binding is incomplete")
    if predecessor.get("schema") != RELEASE_PREDECESSOR_SCHEMA:
        raise SealedReleaseInvalid("release predecessor binding has the wrong schema")
    if previous is None:
        raise SealedReleaseInvalid("release predecessor pointer is missing")
    expected = _predecessor_binding(previous)
    mismatches = {
        field: {"expected": wanted, "observed": predecessor.get(field)}
        for field, wanted in expected.items()
        if predecessor.get(field) != wanted
    }
    if mismatches:
        raise SealedReleaseInvalid(
            "release predecessor binding does not match the previous pointer",
            data={"observed": mismatches},
        )
    return "bound"


def _is_duplicate_root_pointer_state(
    active: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
) -> bool:
    return bool(
        previous is not None
        and active.get("predecessor") is None
        and previous.get("predecessor") is None
        and active == previous
    )


def _validate_pointer_release(
    environment: DeploymentEnvironment,
    pointer: Mapping[str, Any],
    *,
    environ: Mapping[str, str],
) -> SealedRelease:
    _validate_active_environment_fields(
        environment,
        pointer,
        validate_postgres_capacity=False,
    )
    artifact_id = _required_sha256(pointer, "artifact_id")
    artifact_input = Path(
        os.path.abspath(Path(_required_string(pointer, "artifact_root")).expanduser())
    )
    expected_root = (
        environment.release.artifact_store.resolve()
        / artifact_id.removeprefix("sha256:")
    )
    if artifact_input != expected_root or artifact_input.is_symlink():
        raise SealedReleaseInvalid(
            "release pointer root does not match the environment artifact store",
            data={"expected": str(expected_root), "observed": str(artifact_input)},
        )
    release = verify_sealed_release(
        artifact_input,
        expected_artifact_id=artifact_id,
    )
    candidate_descriptor = _candidate_descriptor_path(release, pointer)
    candidate = load_environment(
        candidate_descriptor,
        environ=environ,
        _allow_legacy_container_owner=True,
    )
    _validate_active_environment_fields(candidate, pointer)
    _validate_candidate_binding(
        environment,
        candidate,
        release,
        allow_legacy_container_owner=True,
        allow_legacy_postgres_capacity=True,
        approved_definition_digest=pointer.get("release_definition_digest"),
    )
    _validate_descriptor_file(pointer, candidate_descriptor)
    return release


def _validate_historical_pointer_release(
    environment: DeploymentEnvironment,
    pointer: Mapping[str, Any],
    *,
    environ: Mapping[str, str],
) -> SealedRelease:
    """Validate a prior pointer against its own sealed descriptor.

    A definition update legitimately makes the prior artifact differ from the
    current environment. Rotation and status still require that pointer to be
    internally consistent, immutable, and stored under the same artifact root.
    """
    artifact_id = _required_sha256(pointer, "artifact_id")
    artifact_root = Path(
        os.path.abspath(Path(_required_string(pointer, "artifact_root")).expanduser())
    )
    expected_root = (
        environment.release.artifact_store.resolve()
        / artifact_id.removeprefix("sha256:")
    )
    if artifact_root != expected_root or artifact_root.is_symlink():
        raise SealedReleaseInvalid(
            "historical release pointer root does not match the artifact store",
            data={"expected": str(expected_root), "observed": str(artifact_root)},
        )
    release = verify_sealed_release(
        artifact_root,
        expected_artifact_id=artifact_id,
    )
    descriptor = _candidate_descriptor_path(release, pointer)
    try:
        candidate = load_environment(
            descriptor,
            environ=environ,
            _allow_legacy_container_owner=True,
        )
    except DeploymentConfigError:
        # The prior manifest is SEMANTICALLY invalid under today's rules.
        #
        # This function's contract, per its docstring, is that the pointer be
        # "internally consistent, immutable, and stored under the same
        # artifact root". Loading the environment re-parses an already-sealed,
        # already-superseded manifest through the CURRENT validation set, which
        # is a different and stricter thing — and it deadlocks: once any
        # manifest rule tightens, the outgoing release can no longer be loaded,
        # so it can never be replaced. The release that most needs rotating is
        # exactly the one that fails.
        #
        # okg-workspace-production hit this. Its 2026-07-12 release declares a
        # dark `viz.theme.background` with no `viz.theme.surface`, which the
        # 2026-08-18 theme-coherence rule rejects. Every attempt to activate a
        # newer release refused with `viz_theme_incoherent`, naming the OLD
        # artifact's manifest — a deployment locked to a release by a rule
        # written after it was sealed.
        #
        # Immutability and identity are NOT relaxed: `verify_sealed_release`
        # above already proved the artifact matches its id and is unmodified,
        # and the artifact-root check proved it lives in this environment's
        # store. Only the semantic re-validation is skipped, and only for a
        # pointer being rotated OUT. A missing or unreadable descriptor still
        # raises, because that is not a DeploymentConfigError.
        _validate_descriptor_file(pointer, descriptor)
        return release
    _validate_active_environment_fields(candidate, pointer)
    _validate_candidate_binding(
        candidate,
        candidate,
        release,
        approved_definition_digest=pointer.get("release_definition_digest"),
    )
    _validate_descriptor_file(pointer, descriptor)
    return release


def historical_release_environment(
    environment: DeploymentEnvironment,
    pointer: Mapping[str, Any],
    *,
    environ: Mapping[str, str] | None = None,
) -> DeploymentEnvironment:
    """Load a historical pointer's own topology under immutable authority.

    Receipt validation during a release handoff must use the definition that
    launched the predecessor process.  Unlike pointer rotation, process
    handoff cannot safely fall back when today's loader cannot reconstruct that
    topology: without the old role names and argv, the supervisor inventory is
    unknowable.  This helper therefore keeps the historical pointer's strict
    artifact/file validation and fails closed on semantic load errors.
    """
    values = dict(os.environ if environ is None else environ)
    release = _validate_historical_pointer_release(
        environment,
        pointer,
        environ=values,
    )
    descriptor = _candidate_descriptor_path(release, pointer)
    candidate = load_environment(
        descriptor,
        environ=values,
        _allow_legacy_container_owner=True,
    )
    _validate_active_environment_fields(candidate, pointer)
    _validate_candidate_binding(
        candidate,
        candidate,
        release,
        approved_definition_digest=pointer.get("release_definition_digest"),
    )
    _validate_descriptor_file(pointer, descriptor)
    anchors = {
        "environment_id": (environment.id, candidate.id),
        "state_root": (
            str(environment.runtime.state_root),
            str(candidate.runtime.state_root),
        ),
        "artifact_store": (
            str(environment.release.artifact_store),
            str(candidate.release.artifact_store),
        ),
        "active_pointer": (
            str(environment.release.active_pointer),
            str(candidate.release.active_pointer),
        ),
        "previous_pointer": (
            str(environment.release.previous_pointer),
            str(candidate.release.previous_pointer),
        ),
    }
    mismatches = [
        {"field": field, "expected": wanted, "observed": observed}
        for field, (wanted, observed) in anchors.items()
        if wanted != observed
    ]
    if mismatches:
        raise SealedReleaseInvalid(
            "historical release topology changed operational authority roots",
            data={"observed": mismatches},
        )
    return candidate


def _candidate_environment(
    *,
    environment: DeploymentEnvironment,
    release: SealedRelease,
    source_repo: Path,
    environ: Mapping[str, str],
) -> tuple[DeploymentEnvironment, Path]:
    source = source_repo.expanduser().resolve()
    try:
        relative = environment.descriptor_path.relative_to(source)
    except ValueError as exc:
        raise SealedReleaseInvalid(
            "environment descriptor is outside the release source repository",
            data={
                "expected": str(source),
                "observed": str(environment.descriptor_path),
            },
        ) from exc
    descriptor = release.deployment_repo_root / relative
    if not descriptor.is_file() or descriptor.is_symlink():
        raise SealedReleaseInvalid(
            "sealed release environment descriptor is missing or not regular",
            data={"observed": str(descriptor)},
        )
    stored_relative = Path(release.inputs.deployment_repo_path or ".") / relative
    return load_environment(descriptor, environ=environ), stored_relative


def _validate_candidate_binding(
    expected: DeploymentEnvironment,
    candidate: DeploymentEnvironment,
    release: SealedRelease,
    *,
    allow_legacy_container_owner: bool = False,
    allow_legacy_postgres_capacity: bool = False,
    staging_environment: DeploymentEnvironment | None = None,
    approved_definition_digest: str | None = None,
    module_providers: Mapping[str, Any] | None = None,
) -> None:
    validate_environment_release_paths(candidate, release.deployment_repo_root)
    fields = {
        "environment_id": (expected.id, candidate.id),
        "tier": (expected.tier, candidate.tier),
        "graph_deployment_name": (
            expected.graph_deployment_name,
            candidate.graph_deployment_name,
        ),
        "descriptor_digest": (
            expected.descriptor_digest,
            candidate.descriptor_digest,
        ),
        "definition_digest": (
            expected.definition_digest,
            candidate.definition_digest,
        ),
        "database_name": (expected.database.name, candidate.database.name),
        "dbos_identity": (
            expected.database.dbos_identity,
            candidate.database.dbos_identity,
        ),
        "postgres_container_uid": (
            expected.database.container_uid,
            candidate.database.container_uid,
        ),
        "postgres_container_gid": (
            expected.database.container_gid,
            candidate.database.container_gid,
        ),
        "postgres_capacity_digest": (
            expected.postgres_capacity_digest,
            candidate.postgres_capacity_digest,
        ),
        "max_worker_processes": (
            expected.database.max_worker_processes,
            candidate.database.max_worker_processes,
        ),
        "max_parallel_workers": (
            expected.database.max_parallel_workers,
            candidate.database.max_parallel_workers,
        ),
        "timescaledb_max_background_workers": (
            expected.database.timescaledb_max_background_workers,
            candidate.database.timescaledb_max_background_workers,
        ),
        "timescaledb_job_worker_reserve": (
            expected.database.timescaledb_job_worker_reserve,
            candidate.database.timescaledb_job_worker_reserve,
        ),
        "artifact_store": (
            str(expected.release.artifact_store),
            str(candidate.release.artifact_store),
        ),
        "active_pointer": (
            str(expected.release.active_pointer),
            str(candidate.release.active_pointer),
        ),
        "previous_pointer": (
            str(expected.release.previous_pointer),
            str(candidate.release.previous_pointer),
        ),
    }
    if approved_definition_digest is not None:
        fields.pop("definition_digest", None)
    mismatches = [
        {"field": field, "expected": wanted, "observed": observed}
        for field, (wanted, observed) in fields.items()
        if wanted != observed
        and not (
            allow_legacy_container_owner
            and field in {"postgres_container_uid", "postgres_container_gid"}
            and wanted is not None
            and observed is None
            and candidate.database.container_uid is None
            and candidate.database.container_gid is None
        )
        and not (
            allow_legacy_postgres_capacity
            and field
            in {
                "postgres_capacity_digest",
                "max_worker_processes",
                "max_parallel_workers",
                "timescaledb_max_background_workers",
                "timescaledb_job_worker_reserve",
            }
            and observed is None
            and candidate.postgres_capacity_digest is None
        )
    ]
    if staging_environment is None:
        if (
            approved_definition_digest is None
            and release.inputs.definition_digest != candidate.definition_digest
        ):
            mismatches.append(
                {
                    "field": "release.definition_digest",
                    "expected": candidate.definition_digest,
                    "observed": release.inputs.definition_digest,
                }
            )
        if approved_definition_digest is not None and (
            release.inputs.definition_digest != approved_definition_digest
        ):
            mismatches.append(
                {
                    "field": "pointer.release_definition_digest",
                    "expected": approved_definition_digest,
                    "observed": release.inputs.definition_digest,
                }
            )
    else:
        from okg.substrate.deployment_promotion import (
            validate_staging_promotion_posture,
        )

        validate_staging_promotion_posture(staging_environment)
        bound_digest = (
            approved_definition_digest or staging_environment.definition_digest
        )
        if staging_environment.definition_digest != bound_digest:
            mismatches.append(
                {
                    "field": "approval.staging_definition_digest",
                    "expected": staging_environment.definition_digest,
                    "observed": bound_digest,
                }
            )
        if release.inputs.definition_digest != bound_digest:
            mismatches.append(
                {
                    "field": "release.staging_definition_digest",
                    "expected": bound_digest,
                    "observed": release.inputs.definition_digest,
                }
            )
        substance_problems = shared_definition_substance_problems(
            staging_environment.definition_substance,
            candidate.definition_substance,
        )
        if substance_problems:
            mismatches.append(
                {
                    "field": "definition_substance",
                    "expected": "shared staging/production substance",
                    "observed": list(substance_problems),
                }
            )
    if (
        release.inputs.postgres_capacity_digest
        != candidate.postgres_capacity_digest
    ):
        mismatches.append(
            {
                "field": "release.postgres_capacity_digest",
                "expected": candidate.postgres_capacity_digest,
                "observed": release.inputs.postgres_capacity_digest,
            }
        )
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        providers_for_declared_deployment,
    )
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        environment_bound_catalog_fingerprint_for,
        legacy_catalog_fingerprint_for,
    )

    # Same resolution the seal performed: the candidate's declared owners are
    # added to the release library, so the provenance comparison below sees
    # the catalog the seal recorded rather than a refusal (issue #1933).
    composed = compose_catalog(
        candidate.manifest,
        providers=providers_for_declared_deployment(
            candidate.manifest,
            **(
                {"providers": dict(module_providers)}
                if module_providers is not None
                else {
                    "modules_root": (
                        release.repo_root / "src/okg/substrate/library/ontologies"
                    )
                }
            ),
        ),
    )
    # Re-resolve, then compare: a release records which package supplied each
    # non-built-in module, and activation refuses when that is no longer what
    # the providers hold. Adopting the newly resolved digests instead would
    # make the sealed record decorative. Raised on its own code rather than
    # folded into `mismatches`, so an operator reads the module and both
    # digests without parsing a list.
    refuse_release_provenance_mismatch(
        release.inputs.module_provenance,
        module_provenance_from_composed(composed),
        command="okg environment activate-release",
    )
    catalog = catalog_identity_for(
        composed=composed,
        deployment_dir=candidate.manifest.deployment_dir,
        command="okg environment activate-release",
    )
    legacy_catalog_fingerprint = legacy_catalog_fingerprint_for(composed)
    environment_bound_catalog_fingerprint = (
        environment_bound_catalog_fingerprint_for(composed)
    )
    if release.inputs.catalog_fingerprint not in {
        catalog.catalog_fingerprint,
        environment_bound_catalog_fingerprint,
        legacy_catalog_fingerprint,
    }:
        mismatches.append(
            {
                "field": "release.catalog_fingerprint",
                "expected": catalog.catalog_fingerprint,
                "observed": release.inputs.catalog_fingerprint,
            }
        )
    if mismatches:
        raise SealedReleaseInvalid(
            "sealed release does not match the environment binding: "
            + ", ".join(str(item["field"]) for item in mismatches),
            data={"observed": mismatches},
        )


def _active_payload(
    *,
    environment: DeploymentEnvironment,
    release: SealedRelease,
    descriptor_relative: Path,
    descriptor_path: Path,
    activated_by: str,
    release_definition_digest: str | None = None,
) -> dict[str, Any]:
    secret_ref = environment.database.dsn_secret_ref
    if not secret_ref.startswith("env:"):
        raise SealedReleaseInvalid(
            "environment database secret reference is not launchable",
            data={"observed": secret_ref},
        )
    payload = {
        "schema": ACTIVE_RELEASE_SCHEMA,
        "environment_id": environment.id,
        "tier": environment.tier,
        "graph_deployment_name": environment.graph_deployment_name,
        "descriptor_relative": descriptor_relative.as_posix(),
        "descriptor_digest": environment.descriptor_digest,
        "descriptor_file_digest": _file_digest(descriptor_path),
        "definition_digest": environment.definition_digest,
        "release_definition_digest": (
            release_definition_digest or release.inputs.definition_digest
        ),
        "data_root": str(environment.runtime.data_root),
        "database": {
            "name": environment.database.name,
            "dbos_identity": environment.database.dbos_identity,
            "dsn_env": secret_ref.removeprefix("env:"),
        },
        "credential_refs": {
            service: list(refs)
            for service, refs in sorted(environment.runtime.credential_refs.items())
        },
        "path_environment": dict(sorted(environment.path_environment.items())),
        "artifact_id": release.artifact_id,
        "artifact_root": str(release.artifact_root),
        "input_id": release.inputs.input_id,
        "executable_tree_digest": release.executable_tree_digest,
        "code_revision": release.inputs.code_revision,
        "activated_at": datetime.now(timezone.utc).isoformat(),
        "activated_by": activated_by,
    }
    if environment.postgres_capacity_digest is not None:
        payload["postgres_capacity"] = {
            "schema": "okg.postgres-capacity/v1",
            "digest": environment.postgres_capacity_digest,
            "max_worker_processes": environment.database.max_worker_processes,
            "max_parallel_workers": environment.database.max_parallel_workers,
            "timescaledb_max_background_workers": (
                environment.database.timescaledb_max_background_workers
            ),
            "timescaledb_job_worker_reserve": (
                environment.database.timescaledb_job_worker_reserve
            ),
        }
    return payload


def _launcher_payload(environment: DeploymentEnvironment) -> dict[str, Any]:
    return {
        "schema": LAUNCH_DESCRIPTOR_SCHEMA,
        "environment_id": environment.id,
        "artifact_store": str(environment.release.artifact_store),
        "active_pointer": str(environment.release.active_pointer),
        "previous_pointer": str(environment.release.previous_pointer),
        "service_authority_root": str(
            environment.runtime.state_root / "service-authority"
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _validate_active_environment_fields(
    environment: DeploymentEnvironment,
    active: Mapping[str, Any],
    *,
    validate_postgres_capacity: bool = True,
) -> None:
    expected = {
        "environment_id": environment.id,
        "tier": environment.tier,
        "graph_deployment_name": environment.graph_deployment_name,
        "descriptor_digest": environment.descriptor_digest,
        "definition_digest": environment.definition_digest,
    }
    mismatches = [
        {"field": field, "expected": wanted, "observed": active.get(field)}
        for field, wanted in expected.items()
        if active.get(field) != wanted
    ]
    database = active.get("database")
    if not isinstance(database, Mapping):
        mismatches.append({"field": "database", "expected": "mapping"})
    else:
        for field, wanted in (
            ("name", environment.database.name),
            ("dbos_identity", environment.database.dbos_identity),
        ):
            if database.get(field) != wanted:
                mismatches.append(
                    {
                        "field": f"database.{field}",
                        "expected": wanted,
                        "observed": database.get(field),
                    }
                )
    capacity = active.get("postgres_capacity")
    if (
        validate_postgres_capacity
        and environment.postgres_capacity_digest is not None
    ):
        expected_capacity = {
            "schema": "okg.postgres-capacity/v1",
            "digest": environment.postgres_capacity_digest,
            "max_worker_processes": environment.database.max_worker_processes,
            "max_parallel_workers": environment.database.max_parallel_workers,
            "timescaledb_max_background_workers": (
                environment.database.timescaledb_max_background_workers
            ),
            "timescaledb_job_worker_reserve": (
                environment.database.timescaledb_job_worker_reserve
            ),
        }
        if capacity != expected_capacity:
            mismatches.append(
                {
                    "field": "postgres_capacity",
                    "expected": expected_capacity,
                    "observed": capacity,
                }
            )
    if mismatches:
        raise SealedReleaseInvalid(
            "active release pointer does not match the environment",
            data={"observed": mismatches},
        )


def _candidate_descriptor_path(
    release: SealedRelease,
    active: Mapping[str, Any],
) -> Path:
    relative = Path(_required_string(active, "descriptor_relative"))
    if relative.is_absolute() or ".." in relative.parts:
        raise SealedReleaseInvalid(
            "active release descriptor path escapes the artifact",
            data={"observed": str(relative)},
        )
    descriptor_input = release.repo_root / relative
    if descriptor_input.is_symlink():
        raise SealedReleaseInvalid(
            "active environment descriptor must not be a symlink",
            data={"observed": str(descriptor_input)},
        )
    descriptor = descriptor_input.resolve()
    try:
        descriptor.relative_to(release.repo_root)
    except ValueError as exc:
        raise SealedReleaseInvalid(
            "active release descriptor path escapes the artifact",
            data={"observed": str(descriptor)},
        ) from exc
    return descriptor


def _validate_descriptor_file(active: Mapping[str, Any], descriptor: Path) -> None:
    if descriptor.is_symlink() or not descriptor.is_file():
        raise SealedReleaseInvalid(
            "active environment descriptor is missing or not regular",
            data={"observed": str(descriptor)},
        )
    observed = _file_digest(descriptor)
    expected = _required_string(active, "descriptor_file_digest")
    if observed != expected:
        raise SealedReleaseInvalid(
            "active environment descriptor bytes changed after activation",
            data={"expected": expected, "observed": observed},
        )


def _file_digest(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def _read_regular_json(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise SealedReleaseInvalid(
            f"{label} is missing or not a regular file",
            data={"observed": str(path)},
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SealedReleaseInvalid(
            f"{label} is invalid: {exc}",
            data={"observed": str(path)},
        ) from exc
    if not isinstance(raw, dict):
        raise SealedReleaseInvalid(
            f"{label} must contain a JSON object",
            data={"observed": str(path)},
        )
    return raw


def _read_private_regular_json(path: Path, *, label: str) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SealedReleaseInvalid(
            f"{label} is missing or not a regular file",
            data={"observed": str(path)},
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise SealedReleaseInvalid(
                f"{label} must be a private, singly linked regular file",
                data={"observed": str(path)},
            )
        try:
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                descriptor = -1
                raw = json.load(handle)
        except (OSError, ValueError) as exc:
            raise SealedReleaseInvalid(
                f"{label} is invalid: {exc}",
                data={"observed": str(path)},
            ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not isinstance(raw, dict):
        raise SealedReleaseInvalid(
            f"{label} must contain a JSON object",
            data={"observed": str(path)},
        )
    return raw


def _required_string(raw: Mapping[str, Any], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise SealedReleaseInvalid(
            f"active release pointer field {field!r} is missing",
            data={"observed": value},
        )
    return value


def _required_sha256(raw: Mapping[str, Any], field: str) -> str:
    value = _required_string(raw, field)
    prefix, separator, digest = value.partition(":")
    if (
        prefix != "sha256"
        or separator != ":"
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise SealedReleaseInvalid(
            f"active release pointer field {field!r} is not a sha256 digest",
            data={"observed": value},
        )
    return value


def _same_active_binding(
    current: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> bool:
    ignored = {"activated_at", "activated_by", "predecessor"}
    return {key: value for key, value in current.items() if key not in ignored} == {
        key: value for key, value in candidate.items() if key not in ignored
    }


@contextmanager
def release_activation_lock(
    active_pointer: Path,
    *,
    exclusive: bool = True,
) -> Iterator[None]:
    """Serialize activation with service start/restart verification."""
    lock_path = active_pointer.with_suffix(active_pointer.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(handle.fileno(), operation)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _durable_unlink(path: Path) -> None:
    path.unlink()
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
