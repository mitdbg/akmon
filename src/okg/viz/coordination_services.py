"""Deployment adapter factories for Viz coordination control surfaces.

The Viz transport owns authentication.  This module only loads a deployment's
native coordination implementation and supplies short-lived database-backed
callbacks to the already-authenticated routes.  If the deployment cannot be
resolved or does not provide the native implementation, callbacks stay
``None`` and the routes return a redacted 503.  Setup planning/apply
additionally require explicitly injected, server-owned local authority
adapters; absent those adapters setup remains unavailable.
"""

from __future__ import annotations

import importlib
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.pact.coordination_operations import build_setup_plan
from okg.viz.approval_control import (
    ApprovalControlRequest,
    ApprovalControlService,
)
from okg.viz.auth import OPERATOR_PRINCIPAL, OPERATOR_SESSION_ID
from okg.viz.operator_control import (
    OperatorControlRequest,
    OperatorControlService,
)


class SetupPlanRequest(BaseModel):
    """Authenticated setup-planner input.

    Deployment is the only caller-selected value.  It is checked against the
    deployment bound to this Viz service before any authority is read, so the
    request cannot make one service plan a different deployment.
    """

    model_config = ConfigDict(extra="forbid")

    deployment: str | None = None


@runtime_checkable
class SetupPlanService(Protocol):
    """Read-only setup planning callback installed by a deployment."""

    def __call__(
        self,
        *,
        principal: str,
        request: SetupPlanRequest,
    ) -> Mapping[str, Any]: ...


def invoke_setup_plan(
    service: SetupPlanService,
    *,
    principal: str,
    request: SetupPlanRequest,
) -> Mapping[str, Any]:
    receipt = service(principal=principal, request=request)
    if not isinstance(receipt, Mapping):
        raise TypeError("setup-plan service must return a mapping")
    return receipt


_SETUP_PROBE_NAMES = (
    "git",
    "pact",
    "deployment",
    "graph",
    "native_control",
    "capacity",
    "runtime",
    "route",
)

_RECOVERABLE_PAIR_COLLISIONS = frozenset(
    {
        "git_target_branch_exists",
        "git_target_worktree_exists",
        "graph_target_branch_exists",
    }
)


class SetupAuthorityProbes(Protocol):
    """Read-only probes for the authorities needed by local setup."""

    def git(self) -> Mapping[str, Any]: ...
    def pact(self) -> Mapping[str, Any]: ...
    def deployment(self) -> Mapping[str, Any]: ...
    def graph(self) -> Mapping[str, Any]: ...
    def native_control(self) -> Mapping[str, Any]: ...
    def capacity(self) -> Mapping[str, Any]: ...
    def runtime(self) -> Mapping[str, Any]: ...
    def route(self) -> Mapping[str, Any]: ...


@runtime_checkable
class SetupChiefResolver(Protocol):
    """Resolve server-owned local chief identity; never HTTP body data."""

    def __call__(
        self,
        *,
        plan: Mapping[str, Any],
        snapshot: Mapping[str, Mapping[str, Any]],
        deployment_name: str,
    ) -> Any: ...


@runtime_checkable
class SetupPairRequestFactory(Protocol):
    """Build one local-only paired-workspace request from server authorities."""

    def __call__(
        self,
        *,
        chief_request: Any,
        snapshot: Mapping[str, Mapping[str, Any]],
        deployment_name: str,
    ) -> Any: ...


@runtime_checkable
class SetupLifecycleFactory(Protocol):
    """Create a PairedWorkspaceLifecycle for one already-bound request."""

    def __call__(self, request: Any) -> Any: ...


class SetupAuthorityUnavailable(RuntimeError):
    """A required standard authority or read-only probe is unavailable."""


@dataclass(frozen=True)
class NativeCoordinationServices:
    """Optional callbacks installed into one Viz app."""

    operator_control_service: OperatorControlService | None = None
    approval_control_service: ApprovalControlService | None = None
    setup_plan_service: SetupPlanService | None = None


_OPERATOR_CONTROL_RECEIPT_FIELDS = frozenset(
    {
        "ok",
        "action",
        "deployment_name",
        "control_mode",
        "control_generation",
        "assignment_id",
        "assignment_generation",
        "plan_digest",
        "policy_digest",
        "project_digest",
        "audit_event_id",
        "complete",
        "pair",
    }
)
_APPROVAL_RECEIPT_FIELDS = frozenset(
    {
        "ok",
        "status",
        "disposition",
        "request_digest",
        "approval_id",
        "duplicate",
        "route_state",
        "blocker_code",
        "reason",
        "notification_count",
        "consumed",
    }
)


def _redacted_receipt(
    value: Any,
    *,
    allowed_fields: frozenset[str],
) -> Mapping[str, Any]:
    """Convert a native result through the surface's explicit allowlist.

    Native results currently expose small ``as_dict`` views, but the adapter
    must not turn that implementation detail into a transport contract.  A
    newly-added native field (including a path, DSN, token, or private row
    value) is therefore dropped until it is deliberately added to the
    relevant surface allowlist.
    """

    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        value = as_dict()
    if not isinstance(value, Mapping):
        raise TypeError("native coordination service returned no receipt")
    return {
        key: item
        for key, item in value.items()
        if isinstance(key, str) and key in allowed_fields
    }


def _probe_snapshot(probes: SetupAuthorityProbes) -> dict[str, Mapping[str, Any]]:
    """Read every setup authority once and reject partial snapshots."""

    snapshot: dict[str, Mapping[str, Any]] = {}
    for name in _SETUP_PROBE_NAMES:
        probe = getattr(probes, name, None)
        if not callable(probe):
            raise SetupAuthorityUnavailable(f"missing setup {name} probe")
        try:
            value = probe()
        except Exception as exc:  # noqa: BLE001 - fail closed at the boundary.
            raise SetupAuthorityUnavailable(f"setup {name} probe failed") from exc
        if not isinstance(value, Mapping) or not value:
            raise SetupAuthorityUnavailable(f"setup {name} probe returned no authority")
        snapshot[name] = dict(value)
    return snapshot


_SETUP_FORBIDDEN_KEYS = frozenset(
    {
        "password",
        "private_key",
        "secret",
        "token",
        "dsn",
        "credential",
        "principal",
        "session",
        "owner",
        "host",
    }
)


def _safe_setup_value(value: Any, *, path: str = "plan") -> Any:
    """Keep the approved plan metadata identity/credential free."""

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            normalized = key_text.strip().lower().replace("-", "_")
            # Do not reject harmless canonical metadata such as
            # credential_references or identity. Sensitive identity claims
            # remain rejected by exact field name and explicit suffixes.
            if (
                normalized in _SETUP_FORBIDDEN_KEYS
                or normalized.endswith("_principal")
                or normalized.endswith("_session")
                or normalized.endswith("_owner")
            ):
                raise SetupAuthorityUnavailable(
                    f"setup plan contains a server-owned field at {path}"
                )
            result[key_text] = _safe_setup_value(item, path=f"{path}.{key_text}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _safe_setup_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, str):
        lowered = value.lower()
        if "postgres://" in lowered or "postgresql://" in lowered:
            raise SetupAuthorityUnavailable("setup plan contains credential material")
    return value


def _canonical_plan(plan: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    clean = _safe_setup_value(plan)
    if not isinstance(clean, dict) or not clean:
        raise SetupAuthorityUnavailable("setup authority returned no plan")
    encoded = json.dumps(
        clean,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return clean, "sha256:" + hashlib.sha256(encoded).hexdigest()


def _compose_setup_state(
    snapshot: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Compose the eight authority reads into the one planner input.

    Each probe contributes state fields understood by
    ``build_setup_plan``.  There is deliberately no ``chief``/``plan``
    shortcut here: assignment writes are produced by the canonical planner
    and the native writer receives its unsigned payload later.
    """

    state: dict[str, Any] = {}
    for authority in _SETUP_PROBE_NAMES:
        fragment = snapshot[authority]
        for key, value in fragment.items():
            if key in {"plan", "structured_plan", "chief_plan"}:
                raise SetupAuthorityUnavailable(
                    "PACT setup probe must provide bindings, not a loose plan"
                )
            if key in state and state[key] != value:
                raise SetupAuthorityUnavailable(f"setup authorities disagree on {key}")
            state[key] = value
    return state


def _plan_from_snapshot(
    snapshot: Mapping[str, Mapping[str, Any]],
    *,
    repo_root: Path,
    deployment_name: str,
) -> tuple[Any, dict[str, Any], str, str, str]:
    """Build the sole canonical setup plan from current authority reads."""

    state = _compose_setup_state(snapshot)
    plan = build_setup_plan(repo_root, deployment_name, probe=state)
    return (
        plan,
        plan._unsigned_payload(),
        plan.policy_digest,
        plan.project_digest,
        plan.human_policy_digest,
    )


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _setup_plan_receipt(
    *,
    deployment_name: str,
    repo_root: Path,
    snapshot: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any]:
    plan, _unsigned, _policy_digest, _project_digest, _human_policy_digest = (
        _plan_from_snapshot(
            snapshot,
            repo_root=repo_root,
            deployment_name=deployment_name,
        )
    )
    # The wire response is the exact canonical SetupPlan document.  Keeping
    # status/check metadata in a second wrapper made the CLI accept a shape
    # that SetupPlan.from_json could not verify and created a competing plan
    # protocol.
    return plan.to_json()


def _require_mutation_ready_setup_plan(plan: Any) -> None:
    """Reject a canonical plan unless every readiness invariant permits writes."""

    if getattr(plan, "read_ok", None) is not True:
        raise SetupAuthorityUnavailable("current setup plan is not readable")
    if getattr(plan, "ready", None) is not True:
        raise SetupAuthorityUnavailable("current setup plan is not ready")
    blocked_by = getattr(plan, "blocked_by", None)
    if not isinstance(blocked_by, (tuple, list)) or blocked_by:
        raise SetupAuthorityUnavailable("current setup plan has active blockers")
    checks = getattr(plan, "checks", None)
    if (
        not isinstance(checks, Mapping)
        or not checks
        or any(value is not True for value in checks.values())
    ):
        raise SetupAuthorityUnavailable("current setup plan has failed checks")


def _pair_summary(value: Any) -> Mapping[str, Any]:
    """Return the private pair proof needed to prove pair activation.

    This representation never crosses the operator-control response boundary.
    In particular, its worktree identity and authenticated host are retained
    only so the native completion validator can match the server-owned pair.
    """

    source = value if isinstance(value, Mapping) else vars(value)
    git = source.get("git") if isinstance(source.get("git"), Mapping) else {}
    graph = source.get("graph") if isinstance(source.get("graph"), Mapping) else {}
    worktree_path = source.get("worktree_path") or git.get("worktree_path")
    graph_branch = graph.get("branch", source.get("graph_branch"))
    summary: dict[str, Any] = {
        "assignment_id": source.get("assignment_id"),
        "assignment_generation": source.get("assignment_generation"),
        "lifecycle_status": source.get("lifecycle_status", source.get("status")),
        "git_branch": git.get("branch", source.get("git_branch")),
        "graph_branch": graph_branch,
        "graph_deployment": graph.get("deployment", source.get("graph_deployment")),
    }
    # These are stable bindings used by the native completion validator.  Do
    # not copy arbitrary lifecycle fields: paths, credentials, and transport
    # evidence stay inside the server-owned adapter.
    for field in (
        "project_id",
        "owner_principal",
        "owner_session_id",
        "git_worktree_id",
        "okg_branch_id",
        "authenticated_host",
    ):
        if source.get(field) is not None:
            summary[field] = source[field]
    # PairedWorkspaceReceipt uses the concrete worktree path and nested graph
    # branch as its authoritative fields.  Bind those values into the native
    # assignment columns when an adapter has not provided separate aliases.
    if "git_worktree_id" not in summary and worktree_path is not None:
        summary["git_worktree_id"] = worktree_path
    if "okg_branch_id" not in summary and graph_branch is not None:
        summary["okg_branch_id"] = graph_branch
    return summary


def _opaque_worktree_reference(value: Any) -> str | None:
    """Return a stable, path-free reference for a private worktree identity."""

    if isinstance(value, Path):
        reference = str(value)
    elif isinstance(value, str) and value.strip():
        reference = value.strip()
    else:
        return None
    # Preserve an already-opaque reference, but never trust a slash-bearing
    # spelling: a concrete absolute or relative path must be transformed.
    if (
        reference.startswith(("opaque:", "worktree:"))
        and "/" not in reference
        and "\\" not in reference
    ):
        return reference
    digest = hashlib.sha256(reference.encode("utf-8")).hexdigest()
    return f"opaque:worktree:{digest}"


def _public_pair_summary(value: Any) -> Mapping[str, Any]:
    """Project private pair proof to the path- and host-free service receipt."""

    private = _pair_summary(value)
    public: dict[str, Any] = {}
    for field in (
        "assignment_id",
        "assignment_generation",
        "lifecycle_status",
        "git_branch",
        "graph_branch",
        "graph_deployment",
        "okg_branch_id",
    ):
        if private.get(field) is not None:
            public[field] = private[field]
    worktree_reference = _opaque_worktree_reference(private.get("git_worktree_id"))
    if worktree_reference is not None:
        public["git_worktree_id"] = worktree_reference
    return public


def _setup_receipt(
    native_receipt: Any,
    *,
    pair_receipt: Any,
) -> Mapping[str, Any]:
    pair_proof = dict(_pair_summary(pair_receipt))
    if pair_proof.get("lifecycle_status") != "active":
        raise SetupAuthorityUnavailable("paired workspace did not activate")
    if (
        not isinstance(pair_proof.get("assignment_id"), str)
        or not pair_proof["assignment_id"].strip()
        or isinstance(pair_proof.get("assignment_generation"), bool)
        or not isinstance(pair_proof.get("assignment_generation"), int)
        or pair_proof["assignment_generation"] < 1
    ):
        raise SetupAuthorityUnavailable("paired workspace receipt is incomplete")
    result = dict(
        _redacted_receipt(
            native_receipt,
            allowed_fields=_OPERATOR_CONTROL_RECEIPT_FIELDS,
        )
    )
    native_assignment_id = result.get("assignment_id")
    native_generation = result.get("assignment_generation")
    if native_assignment_id != pair_proof["assignment_id"]:
        raise SetupAuthorityUnavailable(
            "native and paired workspace assignment ids do not match"
        )
    if (
        isinstance(native_generation, bool)
        or not isinstance(native_generation, int)
        or native_generation != pair_proof["assignment_generation"]
    ):
        raise SetupAuthorityUnavailable(
            "native and paired workspace assignment generations do not match"
        )
    result["complete"] = bool(result.get("ok"))
    # Keep the complete proof private for native matching/completion.  Only
    # opaque operator-safe references are returned from this service.
    result["pair"] = dict(_public_pair_summary(pair_proof))
    return result


def _load_native_modules(deployment_name: str) -> tuple[Any, Any] | None:
    """Load the native state/approval modules for one resolved deployment."""

    from okg.substrate.deployments import find_deployment_dir, inject_deployment_path

    deployment_dir = find_deployment_dir(deployment_name)
    if deployment_dir is None:
        return None
    inject_deployment_path(deployment_dir)
    try:
        state = importlib.import_module("coordination.native_state")
        approvals = importlib.import_module("coordination.native_approvals")
    except Exception:  # noqa: BLE001 - unavailable means the route stays 503.
        return None

    # A stale module from another deployment must never silently become the
    # implementation for this app.  Keep the check local to the factory; the
    # deployment loader remains the source of path resolution policy.
    origin = getattr(state, "__file__", None)
    if not origin:
        return None
    try:
        resolved_origin = Path(origin).resolve()
        resolved_dir = deployment_dir.resolve()
    except OSError:
        return None
    if resolved_dir not in resolved_origin.parents:
        return None
    required_state = (
        "ensure_native_coordination_schema",
        "prepare_bootstrap_control",
        "record_bootstrap_pair_proof",
        "complete_bootstrap_control",
        "fallback_control",
        "reactivate_control",
        "OPERATOR_CONTROL_ACTOR",
        "OPERATOR_CONTROL_SESSION",
    )
    required_approvals = (
        "decide_native_approval",
        "revoke_native_approval",
    )
    if not all(hasattr(state, name) for name in required_state):
        return None
    if not all(hasattr(approvals, name) for name in required_approvals):
        return None
    return state, approvals


def build_native_coordination_services(
    *,
    dsn: str,
    deployment_name: str,
    repo_root: Path | None = None,
    setup_probes: SetupAuthorityProbes | None = None,
    chief_resolver: SetupChiefResolver | None = None,
    pair_request_factory: SetupPairRequestFactory | None = None,
    lifecycle_factory: SetupLifecycleFactory | None = None,
) -> NativeCoordinationServices:
    """Create native callbacks when the named deployment supports them.

    Import/loading failures are deliberately represented as unavailable
    callbacks.  The HTTP layer can then report one stable 503 without exposing
    a DSN, token, import path, or database exception.
    """

    if not isinstance(dsn, str) or not dsn.strip():
        return NativeCoordinationServices()
    loaded = _load_native_modules(deployment_name)
    if loaded is None:
        return NativeCoordinationServices()
    native, approvals = loaded
    setup_repo_root = (
        Path.cwd().resolve() if repo_root is None else Path(repo_root).resolve()
    )

    setup_apply_ready = all(
        component is not None
        for component in (
            setup_probes,
            chief_resolver,
            pair_request_factory,
            lifecycle_factory,
        )
    )
    # A partial setup adapter is more dangerous than an unavailable one: it
    # could create the native chief without the exact local Git/graph pair.
    if (
        any(
            component is not None
            for component in (
                setup_probes,
                chief_resolver,
                pair_request_factory,
                lifecycle_factory,
            )
        )
        and not setup_apply_ready
    ):
        setup_apply_ready = False

    def _current_native_control() -> Mapping[str, Any] | None:
        reader = getattr(native, "read_control_state", None)
        if not callable(reader):
            return None
        # The planner/apply preflight is intentionally read-only.  In
        # particular, do not call ensure_native_coordination_schema here: a
        # missing standard authority must stop setup before any staging.
        with db_connect(dsn) as conn:
            state = reader(conn, deployment_name=deployment_name)
        if not isinstance(state, Mapping):
            raise SetupAuthorityUnavailable("native control probe returned no state")
        return dict(state)

    def _setup_snapshot_for(
        request: OperatorControlRequest,
        *,
        current_control: Mapping[str, Any] | None,
    ) -> tuple[
        dict[str, Any],
        dict[str, Mapping[str, Any]],
        Mapping[str, Any] | None,
    ]:
        if setup_probes is None:
            raise SetupAuthorityUnavailable("setup authority probes are unavailable")
        snapshot = _probe_snapshot(setup_probes)
        if current_control is not None and current_control.get("control_mode") in {
            "preparing",
            "open",
            "frozen",
        }:
            # A durable exact attempt may already own the target Git/graph pair.
            # Ignore only the three resources that this attempt can have
            # created; stage_or_adopt below proves the attempt nonce and every
            # immutable binding before reuse.  Uninitialized setup and all
            # unrelated collision codes remain blocked.
            _require_request_binding(current_control, request=request)
            git_fragment = dict(snapshot["git"])
            collisions = git_fragment.get("configuration_collisions")
            if isinstance(collisions, (list, tuple)) and all(
                isinstance(item, str) for item in collisions
            ):
                git_fragment["configuration_collisions"] = [
                    item
                    for item in collisions
                    if item not in _RECOVERABLE_PAIR_COLLISIONS
                ]
                snapshot = dict(snapshot)
                snapshot["git"] = git_fragment
        (
            current_plan,
            current_unsigned,
            current_policy,
            current_project,
            current_human,
        ) = _plan_from_snapshot(
            snapshot,
            repo_root=setup_repo_root,
            deployment_name=deployment_name,
        )
        submitted_plan, submitted_digest = _canonical_plan(request.structured_plan)
        if (
            submitted_digest != request.claimed_plan_digest
            or submitted_plan != current_unsigned
        ):
            raise SetupAuthorityUnavailable("submitted setup plan is stale")
        if (
            current_policy is not None
            and current_policy != request.expected_policy_digest
        ):
            raise SetupAuthorityUnavailable("submitted execution policy is stale")
        if (
            current_project is not None
            and current_project != request.expected_project_digest
        ):
            raise SetupAuthorityUnavailable("submitted project authority is stale")
        if (
            current_human is not None
            and current_plan.human_policy_digest != current_human
        ):
            raise SetupAuthorityUnavailable("submitted human policy is stale")
        if current_plan.plan_digest != request.claimed_plan_digest:
            raise SetupAuthorityUnavailable("submitted setup digest is stale")
        # The exact digest match above is not a readiness approval.  A caller
        # may submit a canonical but intentionally blocked plan, so enforce
        # every SetupPlan status/check invariant before any native write.
        _require_mutation_ready_setup_plan(current_plan)
        route = snapshot["route"]
        if route.get("local_only") is not True:
            raise SetupAuthorityUnavailable(
                "local setup route is not explicitly configured"
            )
        return current_unsigned, snapshot, current_control

    def _cleanup_owned_pair(lifecycle: Any, receipt: Any) -> None:
        """Fence and remove only the pair created by this setup attempt."""
        try:
            retired = lifecycle.retire(receipt, disposition="abandoned")
            report = lifecycle.cleanup(retired)
        except Exception as exc:  # noqa: BLE001 - preserve the original failure.
            raise SetupAuthorityUnavailable("owned setup pair cleanup failed") from exc
        if not bool(getattr(report, "complete", False)):
            raise SetupAuthorityUnavailable("owned setup pair cleanup was incomplete")

    def _native_write(name: str, **kwargs: Any) -> Mapping[str, Any]:
        """Invoke one phased native write through a fresh DB connection."""

        action = getattr(native, name, None)
        if not callable(action):
            raise SetupAuthorityUnavailable(f"native {name} is unavailable")
        with db_connect(dsn) as conn:
            native.ensure_native_coordination_schema(conn)
            result = action(conn, **kwargs)
        if not isinstance(result, Mapping):
            raise SetupAuthorityUnavailable(f"native {name} returned no receipt")
        return dict(result)

    def _require_request_binding(
        value: Mapping[str, Any],
        *,
        request: OperatorControlRequest,
    ) -> None:
        """Reject a native receipt/state that belongs to another attempt."""

        requires_binding = value.get("control_mode") in {
            "preparing",
            "open",
            "frozen",
        }
        for field, expected in (
            ("plan_digest", request.claimed_plan_digest),
            ("policy_digest", request.expected_policy_digest),
            ("project_digest", request.expected_project_digest),
        ):
            actual = value.get(field)
            if field == "plan_digest" and actual is None:
                actual = value.get("approved_plan_digest")
            if (requires_binding and actual is None) or (
                actual is not None and actual != expected
            ):
                raise SetupAuthorityUnavailable(
                    f"native bootstrap {field} does not match submitted plan"
                )
        actual_key = value.get("last_idempotency_key")
        # Public phase receipts intentionally omit the idempotency key.  The
        # durable control row carries it and is checked immediately after
        # prepare and during recovery.  If a value does expose the binding,
        # it must still match exactly.
        if actual_key is not None and actual_key != request.idempotency_key:
            raise SetupAuthorityUnavailable(
                "native bootstrap idempotency key does not match submitted plan"
            )

    def _pair_proof_from_control(
        state: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if not isinstance(state, Mapping):
            return None
        proof = state.get("bootstrap_pair_proof")
        if isinstance(proof, Mapping):
            return dict(proof)
        receipt = state.get("last_receipt")
        if isinstance(receipt, Mapping) and isinstance(
            receipt.get("pair_proof"), Mapping
        ):
            return dict(receipt["pair_proof"])
        return None

    def _pair_proof_from_receipt(
        receipt: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        proof = receipt.get("pair_proof")
        return dict(proof) if isinstance(proof, Mapping) else None

    def _require_pair_matches_chief(
        chief_request: Any,
        pair_proof: Mapping[str, Any],
    ) -> None:
        """Keep the server-resolved chief and pair on one fenced generation."""

        pair_id = pair_proof.get("assignment_id")
        pair_generation = pair_proof.get("assignment_generation")
        chief_id = getattr(chief_request, "assignment_id", None)
        chief_generation = getattr(chief_request, "assignment_generation", None)
        if chief_id != pair_id:
            raise SetupAuthorityUnavailable(
                "chief and paired workspace assignment ids do not match"
            )
        if (
            isinstance(chief_generation, bool)
            or not isinstance(chief_generation, int)
            or chief_generation != pair_generation
        ):
            raise SetupAuthorityUnavailable(
                "chief and paired workspace assignment generations do not match"
            )
        for field in (
            "git_worktree_id",
            "okg_branch_id",
        ):
            expected = pair_proof.get(field)
            if expected is not None and getattr(chief_request, field, None) != expected:
                raise SetupAuthorityUnavailable(
                    f"chief and paired workspace {field} do not match"
                )

    def _operator_bootstrap_with_pair(
        *,
        principal: str,
        request: OperatorControlRequest,
    ) -> Mapping[str, Any]:
        if not setup_apply_ready:
            raise SetupAuthorityUnavailable(
                "local setup requires server-owned chief, route, and lifecycle adapters"
            )
        if principal != OPERATOR_PRINCIPAL:
            raise PermissionError("operator identity is not server-derived")
        current_native = _current_native_control()
        if current_native is not None and current_native.get("control_mode") in {
            "open",
            "frozen",
        }:
            # Successful bootstrap makes the ledger non-empty and consumes
            # capacity by design, so it can no longer reproduce the initial
            # readiness plan.  An exact same-key retry is a read-only replay
            # of durable native state: bind every submitted authority before
            # returning the stored, redacted receipt.  A new key or plan is
            # rejected by the durable binding check below.
            _submitted_plan, submitted_digest = _canonical_plan(request.structured_plan)
            if submitted_digest != request.claimed_plan_digest:
                raise SetupAuthorityUnavailable("submitted setup digest is stale")
            _require_request_binding(current_native, request=request)
            completed = current_native.get("last_receipt")
            if not isinstance(completed, Mapping) or not completed.get("complete"):
                raise SetupAuthorityUnavailable(
                    "native bootstrap is open without a complete receipt"
                )
            _require_request_binding(completed, request=request)
            proof = _pair_proof_from_control(current_native)
            if proof is None:
                proof = _pair_proof_from_receipt(completed)
            if proof is None:
                raise SetupAuthorityUnavailable(
                    "native bootstrap receipt has no paired-workspace proof"
                )
            return _setup_receipt(completed, pair_receipt=proof)
        # Re-read all standard authorities before every apply.  This supplies
        # the canonical plan used by the phased native API and prevents a
        # restart from silently reusing stale process-local state.
        plan, snapshot, current_native = _setup_snapshot_for(
            request,
            current_control=current_native,
        )
        if current_native is not None:
            _require_request_binding(current_native, request=request)
        if not setup_apply_ready:
            raise SetupAuthorityUnavailable(
                "local setup requires server-owned chief, route, and lifecycle adapters"
            )
        # The readiness guard above is deliberately repeated as explicit
        # checks rather than relying on assertions, which may be optimized
        # away in a production interpreter.
        if (
            chief_resolver is None
            or pair_request_factory is None
            or lifecycle_factory is None
        ):
            raise SetupAuthorityUnavailable(
                "local setup requires server-owned chief, route, and lifecycle adapters"
            )
        # ``prepare`` is the durable handoff.  A process can crash after this
        # write and a fresh service instance will see exactly the same plan,
        # policy, project, and idempotency binding.
        prepared = _native_write(
            "prepare_bootstrap_control",
            deployment_name=deployment_name,
            plan=plan,
            plan_digest=request.claimed_plan_digest,
            policy_digest=request.expected_policy_digest,
            project_digest=request.expected_project_digest,
            idempotency_key=request.idempotency_key,
        )
        _require_request_binding(prepared, request=request)
        current_native = _current_native_control()
        if current_native is None:
            raise SetupAuthorityUnavailable("native bootstrap state is unavailable")
        _require_request_binding(current_native, request=request)

        proof = _pair_proof_from_control(current_native)
        mode = current_native.get("control_mode")
        completed = current_native.get("last_receipt")
        if mode in {"open", "frozen"}:
            if not isinstance(completed, Mapping) or not completed.get("complete"):
                raise SetupAuthorityUnavailable(
                    "native bootstrap is open without a complete receipt"
                )
            _require_request_binding(completed, request=request)
            proof = proof or _pair_proof_from_receipt(completed)
            if proof is None:
                raise SetupAuthorityUnavailable(
                    "native bootstrap receipt has no paired-workspace proof"
                )
            return _setup_receipt(completed, pair_receipt=proof)
        if mode != "preparing":
            raise SetupAuthorityUnavailable(
                "native bootstrap did not enter preparation"
            )

        # Resolve the server-owned chief on every completion attempt.  If a
        # prior process already recorded the pair proof, no lifecycle method is
        # called: the proof, not a local cache, is the restart authority.
        chief_request = chief_resolver(
            plan=plan,
            snapshot=snapshot,
            deployment_name=deployment_name,
        )
        if proof is not None:
            _require_pair_matches_chief(chief_request, proof)
            native_receipt = _native_write(
                "complete_bootstrap_control",
                deployment_name=deployment_name,
                plan=plan,
                plan_digest=request.claimed_plan_digest,
                policy_digest=request.expected_policy_digest,
                project_digest=request.expected_project_digest,
                idempotency_key=request.idempotency_key,
                chief_request=chief_request,
                pair_proof=proof,
            )
            _require_request_binding(native_receipt, request=request)
            result = _setup_receipt(native_receipt, pair_receipt=proof)
            if not result.get("complete"):
                raise SetupAuthorityUnavailable("native bootstrap did not complete")
            return result

        pair_request = pair_request_factory(
            chief_request=chief_request,
            snapshot=snapshot,
            deployment_name=deployment_name,
        )
        host = str(getattr(pair_request, "authenticated_host", "") or "").lower()
        if host not in {
            "local",
            "logical:local",
            "localhost",
            "127.0.0.1",
            "::1",
        }:
            raise SetupAuthorityUnavailable("setup currently supports local hosts only")
        lifecycle = lifecycle_factory(pair_request)
        staged: Any | None = None
        active: Any | None = None
        proof_handoff_started = False
        try:
            try:
                stage_or_adopt = getattr(lifecycle, "stage_or_adopt", None)
                staged = (
                    stage_or_adopt(pair_request)
                    if callable(stage_or_adopt)
                    else lifecycle.stage(pair_request)
                )
                active = lifecycle.activate(staged)
                active_summary = _pair_summary(active)
                if (
                    active_summary.get("lifecycle_status") != "active"
                    or not active_summary.get("assignment_id")
                    or not active_summary.get("assignment_generation")
                ):
                    raise SetupAuthorityUnavailable(
                        "paired workspace receipt is incomplete"
                    )
            except Exception as exc:  # noqa: BLE001 - compensate before proof.
                if staged is not None:
                    _cleanup_owned_pair(
                        lifecycle, active if active is not None else staged
                    )
                if isinstance(exc, SetupAuthorityUnavailable):
                    raise
                raise SetupAuthorityUnavailable(
                    "local setup pair staging failed"
                ) from exc

            pair_proof = _pair_summary(active)
            _require_pair_matches_chief(chief_request, pair_proof)
            # From this call onward a transport/commit error is ambiguous: the
            # proof may already be durable even if this process receives no
            # receipt. Preserve the exact pair for retry rather than deleting
            # an object the ledger may now reference.
            proof_handoff_started = True
            recorded = _native_write(
                "record_bootstrap_pair_proof",
                deployment_name=deployment_name,
                plan=plan,
                plan_digest=request.claimed_plan_digest,
                policy_digest=request.expected_policy_digest,
                project_digest=request.expected_project_digest,
                idempotency_key=request.idempotency_key,
                pair_proof=pair_proof,
            )
            _require_request_binding(recorded, request=request)
            native_receipt = _native_write(
                "complete_bootstrap_control",
                deployment_name=deployment_name,
                plan=plan,
                plan_digest=request.claimed_plan_digest,
                policy_digest=request.expected_policy_digest,
                project_digest=request.expected_project_digest,
                idempotency_key=request.idempotency_key,
                chief_request=chief_request,
                pair_proof=pair_proof,
            )
            _require_request_binding(native_receipt, request=request)
            result = _setup_receipt(native_receipt, pair_receipt=pair_proof)
            if not result.get("complete"):
                raise SetupAuthorityUnavailable("native bootstrap did not complete")
            return result
        except Exception as exc:
            # Once the pair proof is durable, keep the pair recoverable for a
            # later completion attempt.  Deleting it would leave the ledger
            # claiming a proof that no longer exists.
            if not proof_handoff_started and active is not None:
                try:
                    _cleanup_owned_pair(lifecycle, active)
                except SetupAuthorityUnavailable as cleanup_exc:
                    raise cleanup_exc from exc
            raise
        finally:
            close = getattr(lifecycle, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:  # noqa: BLE001 - redact adapter details.
                    raise SetupAuthorityUnavailable(
                        "local setup lifecycle cleanup failed"
                    ) from exc

    def operator_control(
        *,
        principal: str,
        request: OperatorControlRequest,
    ) -> Mapping[str, Any]:
        if principal != OPERATOR_PRINCIPAL:
            raise PermissionError("operator identity is not server-derived")
        operation = request.operation
        if operation == "bootstrap":
            # Bootstrap is the one transition that creates a live chief. It
            # must never use the legacy native-only path: without all
            # server-owned setup adapters there is no proof of a complete
            # Git/graph/runtime pair.
            if not setup_apply_ready:
                raise SetupAuthorityUnavailable(
                    "local setup requires server-owned chief, route, and lifecycle adapters"
                )
            try:
                return _operator_bootstrap_with_pair(
                    principal=principal,
                    request=request,
                )
            except SetupAuthorityUnavailable:
                raise
            except Exception as exc:  # noqa: BLE001 - redact production details.
                raise SetupAuthorityUnavailable(
                    "local setup apply is unavailable"
                ) from exc
        action = getattr(native, f"{operation}_control", None)
        if not callable(action):
            raise RuntimeError("native operator control is unavailable")
        with db_connect(dsn) as conn:
            native.ensure_native_coordination_schema(conn)
            result = action(
                conn,
                deployment_name=deployment_name,
                plan=request.structured_plan,
                plan_digest=request.claimed_plan_digest,
                policy_digest=request.expected_policy_digest,
                project_digest=request.expected_project_digest,
                idempotency_key=request.idempotency_key,
            )
        return _redacted_receipt(
            result,
            allowed_fields=_OPERATOR_CONTROL_RECEIPT_FIELDS,
        )

    def approval_control(
        *,
        principal: str,
        session_id: str,
        request: ApprovalControlRequest,
    ) -> Mapping[str, Any]:
        # The transport stamps both values.  Validate them again at the native
        # adapter boundary so a future caller cannot launder a session by
        # invoking this callback directly.
        if principal != OPERATOR_PRINCIPAL or session_id != OPERATOR_SESSION_ID:
            raise PermissionError("operator identity is not server-derived")
        with db_connect(dsn) as conn:
            native.ensure_native_coordination_schema(conn)
            decision = getattr(request, "decision", None)
            if decision is None:
                result = approvals.revoke_native_approval(
                    conn,
                    request_digest=request.request_digest,
                    deployment_name=deployment_name,
                    operator_principal=principal,
                    operator_session_id=session_id,
                )
            else:
                result = approvals.decide_native_approval(
                    conn,
                    request_digest=request.request_digest,
                    decision=decision,
                    deployment_name=deployment_name,
                    operator_principal=principal,
                    operator_session_id=session_id,
                )
        return _redacted_receipt(
            result,
            allowed_fields=_APPROVAL_RECEIPT_FIELDS,
        )

    setup_plan_service: SetupPlanService | None = None
    if setup_probes is not None:

        def setup_plan(
            *,
            principal: str,
            request: SetupPlanRequest,
        ) -> Mapping[str, Any]:
            if principal != OPERATOR_PRINCIPAL:
                raise PermissionError("operator identity is not server-derived")
            if request.deployment not in (None, deployment_name):
                raise SetupAuthorityUnavailable(
                    "requested deployment does not match this Viz service"
                )
            snapshot = _probe_snapshot(setup_probes)
            return _setup_plan_receipt(
                deployment_name=deployment_name,
                repo_root=setup_repo_root,
                snapshot=snapshot,
            )

        setup_plan_service = setup_plan

    return NativeCoordinationServices(
        operator_control_service=operator_control,
        approval_control_service=approval_control,
        setup_plan_service=setup_plan_service,
    )


__all__ = [
    "NativeCoordinationServices",
    "SetupAuthorityUnavailable",
    "SetupAuthorityProbes",
    "SetupChiefResolver",
    "SetupLifecycleFactory",
    "SetupPairRequestFactory",
    "SetupPlanRequest",
    "SetupPlanService",
    "build_native_coordination_services",
    "invoke_setup_plan",
]
