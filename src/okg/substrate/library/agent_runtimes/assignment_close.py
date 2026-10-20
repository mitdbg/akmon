"""Strict local orchestration for two-phase assignment close.

The native ledger owns close authority.  This module owns only the target-local
part of the close saga: adopt the exact pair named by the prepared plan, prove
that the pair digest is unchanged, retire and clean it through the existing
``PairedWorkspaceLifecycle``, then submit the exact cleanup proof.  It never
chooses a route or deletes an unadoptable pair.
"""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .assignment_handshake import (
    AssignmentStartPlan,
)
from .paired_workspace import (
    CleanupReport,
    PairMismatchError,
    PairStaleError,
    PairedWorkspaceLifecycle,
    PairedWorkspaceReceipt,
)


_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,511}$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_CLOSE_PLAN_FIELDS = frozenset(
    {
        "assignment_id",
        "assignment_generation",
        "close_attempt_id",
        "readiness_digest",
        "pair_digest",
        "start_plan",
    }
)
_CLEANUP_PROOF_FIELDS = frozenset(
    {
        "assignment_id",
        "assignment_generation",
        "close_attempt_id",
        "readiness_digest",
        "git_removed",
        "graph_removed",
        "config_removed",
        "complete",
        "disposition",
        "errors",
    }
)
CLOSE_PAIR_PROOF_FIELDS = frozenset(
    {
        "assignment_id",
        "assignment_generation",
        "lifecycle_status",
        "provision_attempt_id",
        "git_worktree_id",
        "git_branch",
        "git_base_commit",
        "git_head_commit",
        "git_tree",
        "git_dirty",
        "okg_branch_id",
        "graph_deployment",
        "graph_base_branch",
        "graph_base_generation",
        "graph_base_checkpoint",
        "graph_current_ref",
        "graph_checkpoint",
        "graph_checkpoint_ref",
        "graph_checkpoint_ids",
        "predecessor_digest",
        "mcp_route",
        "grant_id",
        "config_path",
    }
)
_CLOSE_PAIR_STATUSES = frozenset({"active", "checkpointed", "handed_off"})


class AssignmentCloseError(RuntimeError):
    """A close plan or cleanup proof cannot be accepted safely."""


def _token(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _TOKEN_RE.fullmatch(value.strip()):
        raise AssignmentCloseError(f"invalid {field}")
    return value.strip()


def _digest(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value.strip()):
        raise AssignmentCloseError(f"invalid {field}")
    return value.strip()


def _generation(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise AssignmentCloseError("invalid assignment_generation")
    return value


def _canonical(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            dict(value),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AssignmentCloseError("close value is not canonical JSON") from exc


def close_pair_proof(receipt: PairedWorkspaceReceipt) -> dict[str, Any]:
    """Return the immutable pair state that a close operation is fencing.

    The start proof only describes an active pair.  A close may arrive after a
    checkpoint or handoff, so its proof includes the complete checkpoint
    lineage and both the checkpoint ref and current graph ref.  The proof is
    deliberately a fixed schema: adding an unrelated metadata key cannot
    silently change the authority boundary.
    """

    if not isinstance(receipt, PairedWorkspaceReceipt):
        raise AssignmentCloseError("close pair has an invalid type")
    status = receipt.lifecycle_status
    if status not in _CLOSE_PAIR_STATUSES or receipt.graph.lifecycle_status != status:
        raise AssignmentCloseError("paired workspace is not closable")
    if receipt.git.dirty:
        raise AssignmentCloseError("paired Git worktree is dirty")
    if not receipt.graph.current_ref:
        raise AssignmentCloseError("paired graph has no current ref")
    checkpoint = receipt.graph.checkpoint
    checkpoint_ref = receipt.graph.checkpoint_ref
    checkpoint_ids = list(receipt.graph.checkpoint_ids)
    if status in {"checkpointed", "handed_off"}:
        if not checkpoint or not checkpoint_ref or checkpoint not in checkpoint_ids:
            raise AssignmentCloseError(
                "close pair has an incomplete checkpoint binding"
            )
    elif checkpoint is not None or checkpoint_ref is not None:
        # An active receipt may retain checkpoint history, but the current
        # checkpoint fields must be empty unless the graph itself is at that
        # lifecycle state.  This prevents an old checkpoint from being
        # mistaken for the state being closed.
        raise AssignmentCloseError("active close pair has stale checkpoint state")
    attempt = receipt.graph.metadata.get("provision_attempt_id")
    _token(attempt, "provision_attempt_id")
    proof: dict[str, Any] = {
        "assignment_id": receipt.assignment_id,
        "assignment_generation": receipt.assignment_generation,
        "lifecycle_status": status,
        "provision_attempt_id": attempt,
        "git_worktree_id": receipt.git.worktree_path,
        "git_branch": receipt.git.branch,
        "git_base_commit": receipt.git.base_commit,
        "git_head_commit": receipt.git.head_commit,
        "git_tree": receipt.git.tree,
        "git_dirty": False,
        "okg_branch_id": receipt.graph.branch,
        "graph_deployment": receipt.graph.deployment,
        "graph_base_branch": receipt.graph.base_branch,
        "graph_base_generation": receipt.graph.base_generation,
        "graph_base_checkpoint": receipt.graph.metadata.get("base_checkpoint"),
        "graph_current_ref": receipt.graph.current_ref,
        "graph_checkpoint": checkpoint,
        "graph_checkpoint_ref": checkpoint_ref,
        "graph_checkpoint_ids": checkpoint_ids,
        # The graph branch is authoritative.  A handoff receipt created by
        # the lifecycle intentionally retains the predecessor on graph state;
        # using the outer receipt field here would make the same pair hash
        # differently before and after read-only close adoption.
        "predecessor_digest": receipt.graph.predecessor_digest,
        "mcp_route": receipt.mcp_route,
        "grant_id": receipt.grant_id,
        "config_path": receipt.config_path,
    }
    if set(proof) != CLOSE_PAIR_PROOF_FIELDS:
        raise AssertionError("close pair proof schema drift")
    return proof


def close_pair_proof_digest(
    value: PairedWorkspaceReceipt | Mapping[str, Any],
) -> str:
    """Hash a canonical close pair proof, including checkpoint/ref state."""

    proof = (
        close_pair_proof(value) if isinstance(value, PairedWorkspaceReceipt) else value
    )
    if not isinstance(proof, Mapping) or set(proof) != CLOSE_PAIR_PROOF_FIELDS:
        raise AssignmentCloseError("invalid close pair proof")
    if proof.get("lifecycle_status") not in _CLOSE_PAIR_STATUSES:
        raise AssignmentCloseError("invalid close pair lifecycle status")
    if proof.get("git_dirty") is not False:
        raise AssignmentCloseError("close pair proof is dirty")
    if not proof.get("graph_current_ref"):
        raise AssignmentCloseError("close pair proof has no graph current ref")
    status = proof["lifecycle_status"]
    checkpoint = proof.get("graph_checkpoint")
    checkpoint_ref = proof.get("graph_checkpoint_ref")
    checkpoint_ids = proof.get("graph_checkpoint_ids")
    if not isinstance(checkpoint_ids, list) or any(
        not isinstance(item, str) or not item for item in checkpoint_ids
    ):
        raise AssignmentCloseError("invalid close pair checkpoint lineage")
    if status in {"checkpointed", "handed_off"}:
        if not isinstance(checkpoint, str) or not checkpoint:
            raise AssignmentCloseError("close pair proof has no checkpoint")
        if not isinstance(checkpoint_ref, str) or not checkpoint_ref:
            raise AssignmentCloseError("close pair proof has no checkpoint ref")
        if checkpoint not in checkpoint_ids:
            raise AssignmentCloseError("close pair proof checkpoint is not in lineage")
    elif checkpoint is not None or checkpoint_ref is not None:
        raise AssignmentCloseError("active close pair has stale checkpoint state")
    _token(proof.get("provision_attempt_id"), "provision_attempt_id")
    return "sha256:" + hashlib.sha256(_canonical(proof)).hexdigest()


@dataclass(frozen=True, slots=True)
class AssignmentClosePlan:
    """Closed server-derived authority for one assignment close attempt."""

    assignment_id: str
    assignment_generation: int
    close_attempt_id: str
    readiness_digest: str
    pair_digest: str
    start_plan: AssignmentStartPlan

    @classmethod
    def from_mapping(cls, value: Any) -> "AssignmentClosePlan":
        if not isinstance(value, Mapping) or set(value) != _CLOSE_PLAN_FIELDS:
            raise AssignmentCloseError("invalid close_plan")
        start_plan = AssignmentStartPlan.from_mapping(value.get("start_plan"))
        assignment_id = _token(value.get("assignment_id"), "assignment_id")
        generation = _generation(value.get("assignment_generation"))
        if (
            start_plan.assignment_id != assignment_id
            or start_plan.assignment_generation != generation
        ):
            raise AssignmentCloseError("close plan and start plan identity differ")
        return cls(
            assignment_id=assignment_id,
            assignment_generation=generation,
            close_attempt_id=_token(value.get("close_attempt_id"), "close_attempt_id"),
            readiness_digest=_digest(value.get("readiness_digest"), "readiness_digest"),
            pair_digest=_digest(value.get("pair_digest"), "pair_digest"),
            start_plan=start_plan,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "assignment_id": self.assignment_id,
            "assignment_generation": self.assignment_generation,
            "close_attempt_id": self.close_attempt_id,
            "readiness_digest": self.readiness_digest,
            "pair_digest": self.pair_digest,
            "start_plan": self.start_plan.as_dict(),
        }

    def digest(self) -> str:
        return "sha256:" + hashlib.sha256(_canonical(self.as_dict())).hexdigest()


@dataclass(frozen=True, slots=True)
class CleanupProof:
    """Strict target-local absence proof accepted by native finalization."""

    assignment_id: str
    assignment_generation: int
    close_attempt_id: str
    readiness_digest: str
    git_removed: bool
    graph_removed: bool
    config_removed: bool
    complete: bool
    disposition: str
    errors: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Any) -> "CleanupProof":
        if not isinstance(value, Mapping) or set(value) != _CLEANUP_PROOF_FIELDS:
            raise AssignmentCloseError("invalid cleanup_proof")
        errors = value.get("errors")
        if errors != [] and errors != ():
            raise AssignmentCloseError("invalid cleanup proof errors")
        return cls(
            assignment_id=_token(value.get("assignment_id"), "assignment_id"),
            assignment_generation=_generation(value.get("assignment_generation")),
            close_attempt_id=_token(value.get("close_attempt_id"), "close_attempt_id"),
            readiness_digest=_digest(value.get("readiness_digest"), "readiness_digest"),
            git_removed=_strict_bool(value.get("git_removed"), "git_removed"),
            graph_removed=_strict_bool(value.get("graph_removed"), "graph_removed"),
            config_removed=_strict_bool(value.get("config_removed"), "config_removed"),
            complete=_strict_bool(value.get("complete"), "complete"),
            disposition=_token(value.get("disposition"), "disposition"),
            errors=tuple(item.strip() for item in errors),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "assignment_id": self.assignment_id,
            "assignment_generation": self.assignment_generation,
            "close_attempt_id": self.close_attempt_id,
            "readiness_digest": self.readiness_digest,
            "git_removed": self.git_removed,
            "graph_removed": self.graph_removed,
            "config_removed": self.config_removed,
            "complete": self.complete,
            "disposition": self.disposition,
            "errors": list(self.errors),
        }

    def validate_complete(self) -> None:
        if not (
            self.git_removed
            and self.graph_removed
            and self.config_removed
            and self.complete
        ):
            raise AssignmentCloseError("cleanup proof is incomplete")
        if self.disposition != "complete":
            raise AssignmentCloseError("cleanup disposition is not complete")
        if self.errors:
            raise AssignmentCloseError("cleanup proof reports errors")

    def digest(self) -> str:
        self.validate_complete()
        return cleanup_proof_digest(self)


def _strict_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise AssignmentCloseError(f"invalid {field}")
    return value


def cleanup_proof_digest(value: CleanupProof | Mapping[str, Any]) -> str:
    """Return the closed digest of a complete cleanup proof."""

    proof = (
        value if isinstance(value, CleanupProof) else CleanupProof.from_mapping(value)
    )
    proof.validate_complete()
    return "sha256:" + hashlib.sha256(_canonical(proof.as_dict())).hexdigest()


def cleanup_proof(report: CleanupReport, plan: AssignmentClosePlan) -> CleanupProof:
    """Convert one complete lifecycle report into the exact close proof."""

    if not isinstance(report, CleanupReport):
        raise AssignmentCloseError("cleanup report has an invalid type")
    if (
        report.assignment_id != plan.assignment_id
        or report.assignment_generation != plan.assignment_generation
    ):
        raise AssignmentCloseError("cleanup report identity differs from plan")
    proof = CleanupProof(
        assignment_id=report.assignment_id,
        assignment_generation=report.assignment_generation,
        close_attempt_id=plan.close_attempt_id,
        readiness_digest=plan.readiness_digest,
        git_removed=report.git_removed,
        graph_removed=report.graph_removed,
        config_removed=report.config_removed,
        complete=report.complete,
        disposition=report.disposition,
        errors=(),
    )
    if report.errors:
        raise AssignmentCloseError("cleanup report contains errors")
    proof.validate_complete()
    return proof


class AssignmentCloseOrchestrator:
    """Adopt, verify, clean, and finalize one exact prepared pair."""

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

    def close(self, *, idempotency_key: str) -> dict[str, Any]:
        prepared = dict(self.prepare(idempotency_key=idempotency_key))
        if prepared.get("ok") is not True:
            return prepared
        if prepared.get("status") == "retired":
            return prepared
        plan = AssignmentClosePlan.from_mapping(prepared.get("close_plan"))
        if (
            prepared.get("assignment_id") != plan.assignment_id
            or prepared.get("assignment_generation") != plan.assignment_generation
            or prepared.get("close_attempt_id") != plan.close_attempt_id
            or prepared.get("readiness_digest") != plan.readiness_digest
        ):
            raise AssignmentCloseError("close plan identity does not match receipt")

        # adopt is the authority boundary.  If it raises, the pair may be
        # colliding or unadoptable; never guess and never delete it.  A prior
        # complete cleanup proof can be retried without adoption after a lost
        # finalize response; the trusted server cleanup verifier decides
        # whether the target is actually absent.
        lifecycle = self.lifecycle_factory(plan.start_plan)
        request = plan.start_plan.paired_request()
        # The graph branch/provision lock must cover the complete close
        # decision.  Otherwise a graph writer can advance the ref after the
        # digest fence and before retire, leaving a receipt that names a state
        # which was never actually retired.
        close_lock = getattr(lifecycle, "close_lock", None)
        if callable(close_lock):
            lock_context = close_lock(request)
        else:
            graph = getattr(lifecycle, "graph", None)
            provision_lock = getattr(graph, "provision_lock", None)
            lock_context = (
                provision_lock(request.graph_deployment, request.resolved_graph_branch)
                if callable(provision_lock)
                else nullcontext()
            )
        with lock_context:
            try:
                active = lifecycle.adopt_for_close(request)
            except (PairMismatchError, PairStaleError) as exc:
                # A close retry may recover only the exact durable close
                # marker written by ``retire``.  An adoption error by itself
                # is not evidence of absence and must never become a success
                # proof (it may be a collision or a stale pair).
                recover = getattr(lifecycle, "_recover_close_locked", None)
                if not callable(recover):
                    raise AssignmentCloseError(
                        "close pair could not be adopted; recovery authority is unavailable"
                    ) from exc
                try:
                    cleanup = recover(
                        request,
                        close_attempt_id=plan.close_attempt_id,
                    )
                except Exception as recovery_exc:  # noqa: BLE001 - sanitize boundary
                    raise AssignmentCloseError(
                        "close pair could not be adopted; exact recovery was refused"
                    ) from recovery_exc
                if not isinstance(cleanup, CleanupReport) or not cleanup.complete:
                    raise AssignmentCloseError(
                        "close pair could not be adopted; preserve state and recover"
                    ) from exc
                proof = cleanup_proof(cleanup, plan)
                cleanup_digest = proof.digest()
            except Exception as exc:  # noqa: BLE001 - local adapter boundary
                # Unknown adapter failures are not proof of absence and do
                # not enter marker-based recovery.  Keep the public failure
                # deterministic while preserving the target unchanged.
                raise AssignmentCloseError(
                    "close pair adoption failed; preserve state and recover"
                ) from exc
            else:
                current_pair = close_pair_proof(active)
                if close_pair_proof_digest(current_pair) != plan.pair_digest:
                    raise AssignmentCloseError("current pair differs from close plan")
                retired = lifecycle.retire(
                    active,
                    disposition="retained",
                    close_attempt_id=plan.close_attempt_id,
                )
                cleanup = lifecycle.cleanup(retired)
                if not isinstance(cleanup, CleanupReport) or not cleanup.complete:
                    raise AssignmentCloseError(
                        "local cleanup is incomplete; preserve state and recover"
                    )
                proof = cleanup_proof(cleanup, plan)
                cleanup_digest = proof.digest()
        if (
            proof.assignment_id != plan.assignment_id
            or proof.assignment_generation != plan.assignment_generation
        ):
            raise AssignmentCloseError("cleanup report identity differs from plan")
        try:
            result = dict(
                self.finalize(
                    close_attempt_id=plan.close_attempt_id,
                    readiness_digest=plan.readiness_digest,
                    cleanup_digest=cleanup_digest,
                    cleanup_proof=proof.as_dict(),
                    idempotency_key=idempotency_key,
                )
            )
        except Exception as exc:
            raise AssignmentCloseError(
                "close finalize outcome is unknown; preserve the exact cleanup "
                "proof and retry"
            ) from exc
        if result.get("ok") is not True:
            raise AssignmentCloseError(
                "close finalize was rejected; preserve the exact cleanup proof "
                "for reconciliation"
            )
        if (
            result.get("status") != "retired"
            or result.get("assignment_id") != plan.assignment_id
            or result.get("assignment_generation") != plan.assignment_generation
            or result.get("close_attempt_id") != plan.close_attempt_id
            or result.get("readiness_digest") != plan.readiness_digest
            or result.get("cleanup_digest") != cleanup_digest
        ):
            raise AssignmentCloseError("finalized close does not match plan")
        return result


__all__ = [
    "AssignmentCloseError",
    "AssignmentCloseOrchestrator",
    "AssignmentClosePlan",
    "CleanupProof",
    "CLOSE_PAIR_PROOF_FIELDS",
    "close_pair_proof",
    "close_pair_proof_digest",
    "cleanup_proof",
    "cleanup_proof_digest",
]
