"""Transport seam for human approval decisions in coordination.

Approval decisions deliberately have their own transport contract.  The
request identifies only the already-created native approval by digest; the
native ledger remains the authority for the action, resource, scope, and
requesting assignment.  The authenticated operator identity is supplied by
the Viz middleware and is never accepted from the request body.
"""

from __future__ import annotations

from typing import Any, Literal, Mapping, Protocol, TypeAlias, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


ApprovalDecision: TypeAlias = Literal["approve", "deny"]


class ApprovalControlRequest(BaseModel):
    """Common body accepted by the dedicated approval service.

    ``extra='forbid'`` is intentional.  In particular, principal/session,
    action/resource/scope, assignment, and credential fields are not part of
    this API.  They are read from the digest-bound native ledger instead.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    request_digest: str = Field(
        min_length=1,
        description="Digest of an existing native coordination approval request.",
    )


class ApprovalDecisionRequest(ApprovalControlRequest):
    """Input to the ``decide`` route."""

    decision: ApprovalDecision


class ApprovalRevokeRequest(ApprovalControlRequest):
    """Input to the ``revoke`` route."""


RedactedReceipt: TypeAlias = Mapping[str, Any]


@runtime_checkable
class ApprovalControlService(Protocol):
    """Callable implemented by a deployment's native approval adapter."""

    def __call__(
        self,
        principal: str,
        session_id: str,
        request: ApprovalControlRequest,
    ) -> RedactedReceipt:
        """Apply one authenticated decision or revocation."""
        raise NotImplementedError


def invoke_approval_control(
    service: ApprovalControlService,
    *,
    principal: str,
    session_id: str,
    request: ApprovalControlRequest,
) -> RedactedReceipt:
    """Invoke the service with only server-derived identity and safe input."""

    receipt = service(
        principal=principal,
        session_id=session_id,
        request=request,
    )
    if not isinstance(receipt, Mapping):
        raise TypeError(
            "approval-control service must return a redacted mapping receipt"
        )
    return receipt


__all__ = [
    "ApprovalControlRequest",
    "ApprovalControlService",
    "ApprovalDecision",
    "ApprovalDecisionRequest",
    "ApprovalRevokeRequest",
    "RedactedReceipt",
    "invoke_approval_control",
]
