"""Target-local activation for an authenticated native assignment reservation.

The native coordination ledger authenticates the parent and target, reserves
capacity, and returns one closed start plan.  This module executes only the
local paired-workspace portion of that plan.  It has no database access and
cannot choose identity, assignment generation, route, grant, branch, or base.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from okg.substrate.library.agent_runtimes.paired_workspace import (
    PairedWorkspaceLifecycle,
    PairedWorkspaceReceipt,
    PairedWorkspaceRequest,
)


_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,511}$")
_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")
START_PLAN_FIELDS = frozenset(
    {
        "assignment_id",
        "assignment_generation",
        "authenticated_host",
        "repo_path",
        "worktree_path",
        "git_branch",
        "git_base_commit",
        "graph_deployment",
        "graph_base_branch",
        "graph_base_generation",
        "graph_base_checkpoint",
        "graph_branch",
        "mcp_route",
        "grant_id",
        "config_path",
        "provision_attempt_id",
    }
)
PAIR_PROOF_FIELDS = frozenset(
    {
        "assignment_id",
        "assignment_generation",
        "lifecycle_status",
        "provision_attempt_id",
        "git_worktree_id",
        "git_branch",
        "git_base_commit",
        "git_head_commit",
        "git_dirty",
        "okg_branch_id",
        "graph_deployment",
        "graph_base_branch",
        "graph_base_generation",
        "graph_base_checkpoint",
        "graph_current_ref",
        "mcp_route",
        "grant_id",
    }
)


class AssignmentHandshakeError(RuntimeError):
    """A fail-closed local assignment-start refusal."""


def _token(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not _TOKEN_RE.fullmatch(value.strip()):
        raise AssignmentHandshakeError(f"invalid {field}")
    return value.strip()


def _name(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise AssignmentHandshakeError(f"invalid {field}")
    result = value.strip()
    if (
        not result
        or len(result) > 512
        or result.startswith("-")
        or any(char.isspace() or char == "\x00" for char in result)
    ):
        raise AssignmentHandshakeError(f"invalid {field}")
    return result


def _optional_token(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    return _name(value, field=field)


def _absolute_path(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AssignmentHandshakeError(f"invalid {field}")
    path = Path(value.strip())
    if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise AssignmentHandshakeError(f"invalid {field}")
    return str(path)


def _generation(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise AssignmentHandshakeError("invalid assignment_generation")
    return value


@dataclass(frozen=True, slots=True)
class AssignmentStartPlan:
    """Closed server-derived input for one target-local pair."""

    assignment_id: str
    assignment_generation: int
    authenticated_host: str
    repo_path: str
    worktree_path: str
    git_branch: str
    git_base_commit: str
    graph_deployment: str
    graph_base_branch: str
    graph_base_generation: str | None
    graph_base_checkpoint: str | None
    graph_branch: str
    mcp_route: str
    grant_id: str
    config_path: str | None
    provision_attempt_id: str

    @classmethod
    def from_mapping(cls, value: Any) -> "AssignmentStartPlan":
        if not isinstance(value, Mapping) or set(value) != START_PLAN_FIELDS:
            raise AssignmentHandshakeError("invalid start_plan")
        assignment_id = _token(value.get("assignment_id"), field="assignment_id")
        generation = _generation(value.get("assignment_generation"))
        repo_path = _absolute_path(value.get("repo_path"), field="repo_path")
        worktree_path = _absolute_path(
            value.get("worktree_path"), field="worktree_path"
        )
        if repo_path == worktree_path:
            raise AssignmentHandshakeError("invalid worktree_path")
        git_branch = _name(value.get("git_branch"), field="git_branch")
        graph_branch = _name(value.get("graph_branch"), field="graph_branch")
        if git_branch != graph_branch:
            raise AssignmentHandshakeError("Git and graph branches must match")
        git_base_commit = _token(value.get("git_base_commit"), field="git_base_commit")
        if not _COMMIT_RE.fullmatch(git_base_commit):
            raise AssignmentHandshakeError("invalid git_base_commit")
        config_path = value.get("config_path")
        if config_path is not None:
            config_path = _absolute_path(config_path, field="config_path")
            try:
                Path(config_path).relative_to(Path(worktree_path))
            except ValueError as exc:
                raise AssignmentHandshakeError(
                    "config_path is outside the worktree"
                ) from exc
        return cls(
            assignment_id=assignment_id,
            assignment_generation=generation,
            authenticated_host=_token(
                value.get("authenticated_host"), field="authenticated_host"
            ),
            repo_path=repo_path,
            worktree_path=worktree_path,
            git_branch=git_branch,
            git_base_commit=git_base_commit,
            graph_deployment=_token(
                value.get("graph_deployment"), field="graph_deployment"
            ),
            graph_base_branch=_name(
                value.get("graph_base_branch"), field="graph_base_branch"
            ),
            graph_base_generation=_optional_token(
                value.get("graph_base_generation"), field="graph_base_generation"
            ),
            graph_base_checkpoint=_optional_token(
                value.get("graph_base_checkpoint"), field="graph_base_checkpoint"
            ),
            graph_branch=graph_branch,
            mcp_route=_token(value.get("mcp_route"), field="mcp_route"),
            grant_id=_token(value.get("grant_id"), field="grant_id"),
            config_path=config_path,
            provision_attempt_id=_token(
                value.get("provision_attempt_id"), field="provision_attempt_id"
            ),
        )

    def paired_request(self) -> PairedWorkspaceRequest:
        return PairedWorkspaceRequest(
            assignment_id=self.assignment_id,
            assignment_generation=self.assignment_generation,
            authenticated_host=self.authenticated_host,
            repo_path=self.repo_path,
            worktree_path=self.worktree_path,
            git_branch=self.git_branch,
            git_base_commit=self.git_base_commit,
            graph_deployment=self.graph_deployment,
            graph_base_branch=self.graph_base_branch,
            graph_base_generation=self.graph_base_generation,
            graph_base_checkpoint=self.graph_base_checkpoint,
            graph_branch=self.graph_branch,
            mcp_route=self.mcp_route,
            grant_id=self.grant_id,
            config_path=self.config_path,
            provision_attempt_id=self.provision_attempt_id,
        )

    def as_dict(self) -> dict[str, Any]:
        return {field: getattr(self, field) for field in START_PLAN_FIELDS}


def pair_proof(receipt: PairedWorkspaceReceipt) -> dict[str, Any]:
    """Return the closed pair proof accepted by native finalization."""

    if receipt.lifecycle_status != "active" or receipt.git.dirty:
        raise AssignmentHandshakeError("paired workspace is not clean and active")
    proof = {
        "assignment_id": receipt.assignment_id,
        "assignment_generation": receipt.assignment_generation,
        "lifecycle_status": receipt.lifecycle_status,
        "provision_attempt_id": receipt.graph.metadata.get("provision_attempt_id"),
        "git_worktree_id": receipt.worktree_path,
        "git_branch": receipt.git.branch,
        "git_base_commit": receipt.git.base_commit,
        "git_head_commit": receipt.git.head_commit,
        "git_dirty": receipt.git.dirty,
        "okg_branch_id": receipt.graph.branch,
        "graph_deployment": receipt.graph.deployment,
        "graph_base_branch": receipt.graph.base_branch,
        "graph_base_generation": receipt.graph.base_generation,
        "graph_base_checkpoint": receipt.graph.metadata.get("base_checkpoint"),
        "graph_current_ref": receipt.graph.current_ref,
        "mcp_route": receipt.mcp_route,
        "grant_id": receipt.grant_id,
    }
    if set(proof) != PAIR_PROOF_FIELDS:
        raise AssertionError("pair proof schema drift")
    _token(proof["provision_attempt_id"], field="provision_attempt_id")
    return proof


def pair_proof_digest(proof: Mapping[str, Any]) -> str:
    """Hash the closed proof bytes used by native finalization."""

    if not isinstance(proof, Mapping) or set(proof) != PAIR_PROOF_FIELDS:
        raise AssignmentHandshakeError("invalid pair proof")
    try:
        encoded = json.dumps(
            dict(proof),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AssignmentHandshakeError("invalid pair proof") from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class AssignmentStartOrchestrator:
    """Prepare remotely, stage/adopt locally, then finalize remotely."""

    def __init__(
        self,
        *,
        prepare: Callable[..., Mapping[str, Any]],
        finalize: Callable[..., Mapping[str, Any]],
        lifecycle_factory: Callable[[AssignmentStartPlan], PairedWorkspaceLifecycle],
    ) -> None:
        self.prepare = prepare
        self.finalize = finalize
        self.lifecycle_factory = lifecycle_factory

    def start(self, *, assignment_id: str, idempotency_key: str) -> dict[str, Any]:
        assignment_id = _token(assignment_id, field="assignment_id")
        idempotency_key = _token(idempotency_key, field="idempotency_key")
        prepared = dict(
            self.prepare(
                assignment_id=assignment_id,
                idempotency_key=idempotency_key,
            )
        )
        if prepared.get("ok") is not True:
            return prepared
        if prepared.get("status") == "active":
            if prepared.get("assignment_id") != assignment_id:
                raise AssignmentHandshakeError(
                    "active retry returned another assignment"
                )
            return prepared
        plan = AssignmentStartPlan.from_mapping(prepared.get("start_plan"))
        if (
            plan.assignment_id != assignment_id
            or prepared.get("assignment_generation") != plan.assignment_generation
            or prepared.get("start_attempt_id") != plan.provision_attempt_id
        ):
            raise AssignmentHandshakeError("start plan does not match reservation")

        lifecycle = self.lifecycle_factory(plan)
        active = lifecycle.activate(lifecycle.stage_or_adopt(plan.paired_request()))
        proof = pair_proof(active)
        digest = pair_proof_digest(proof)
        try:
            finalized = dict(
                self.finalize(
                    assignment_id=assignment_id,
                    start_attempt_id=plan.provision_attempt_id,
                    pair_digest=digest,
                    pair_proof=proof,
                    idempotency_key=idempotency_key,
                )
            )
        except Exception as exc:
            # The server may have committed before a transport response was
            # lost.  Preserve the exact active pair so the caller can repeat
            # this idempotent finalize instead of guessing and deleting it.
            raise AssignmentHandshakeError(
                "assignment finalize outcome is unknown; preserve the local "
                "pair and retry the exact finalize"
            ) from exc
        if finalized.get("ok") is not True:
            # An idempotency conflict that echoes this exact pair can be an
            # accepted finalize hidden by a later control-state change.  Keep
            # the pair intact for exact retry instead of destroying workspace
            # state that the server may already own.
            if (
                finalized.get("pair_digest") == digest
                and finalized.get("assignment_id") == assignment_id
                and finalized.get("assignment_generation") == plan.assignment_generation
                and finalized.get("start_attempt_id") == plan.provision_attempt_id
            ):
                raise AssignmentHandshakeError(
                    "assignment finalize outcome requires exact retry"
                )
            if finalized.get("rejection_code") in {
                "assignment_already_started",
                "coordination_control_not_open",
                "coordination_policy_stale",
                "coordination_project_stale",
                "idempotency_conflict",
                "target_route_stale",
            }:
                raise AssignmentHandshakeError(
                    "assignment finalize outcome requires reconciliation; "
                    "preserve the local pair"
                )
            try:
                retired = lifecycle.retire(active, disposition="abandoned")
                cleanup = lifecycle.cleanup(retired)
            except Exception as exc:
                raise AssignmentHandshakeError(
                    "assignment finalize was rejected and local compensation "
                    "requires recovery"
                ) from exc
            if not cleanup.complete:
                raise AssignmentHandshakeError(
                    "assignment finalize was rejected and local compensation "
                    "requires recovery: " + "; ".join(cleanup.errors)
                )
            return finalized
        if (
            finalized.get("status") != "active"
            or finalized.get("assignment_id") != assignment_id
            or finalized.get("assignment_generation") != plan.assignment_generation
            or finalized.get("pair_digest") != digest
        ):
            raise AssignmentHandshakeError("finalized assignment does not match pair")
        return finalized


__all__ = [
    "AssignmentHandshakeError",
    "AssignmentStartOrchestrator",
    "AssignmentStartPlan",
    "PAIR_PROOF_FIELDS",
    "START_PLAN_FIELDS",
    "pair_proof",
    "pair_proof_digest",
]
