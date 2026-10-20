"""Strict Nomos authorization boundary for external-action routes.

Only an active-policy decision with ``allowed=True`` and effect ``allow`` is
accepted.  Audit-mode, review-mode, unavailable, or partially evaluated
results are never translated into permission.  The adapter is intentionally
callback based so the owned module can be used with a transaction supplied by
the PostgreSQL implementation without opening a second connection.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol


class AuthorizationError(PermissionError):
    status_code = 403

    def __init__(self, code: str = "review_authority_unavailable", message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


class AuthorizationUnavailable(AuthorizationError):
    code = "review_authority_unavailable"


class SeparationError(AuthorizationError):
    code = "proposer_approver_separation_required"


@dataclass(frozen=True)
class NomosDecision:
    allowed: bool
    effect: str
    decision_id: str | None = None
    policy_digest: str | None = None
    obligations_performed: bool = False
    provider_id: str | None = None
    subject_binding_id: str | None = None


class NomosEvaluator(Protocol):
    def evaluate_runtime_boundary_with_active_policy(self, **kwargs: Any) -> Any: ...


@dataclass
class ExternalActionNomosStorageAdapter:
    """Transaction-local storage adapter expected by the Nomos store hook."""

    transaction: Any = None
    load_authority: Callable[..., Any] | None = None
    persist_decision: Callable[..., Any] | None = None

    def load(self, **kwargs: Any) -> Any:
        if self.load_authority is None:
            raise AuthorizationUnavailable()
        return self.load_authority(transaction=self.transaction, **kwargs)

    def persist(self, **kwargs: Any) -> Any:
        if self.persist_decision is None:
            raise AuthorizationUnavailable()
        return self.persist_decision(transaction=self.transaction, **kwargs)


def _call_evaluator(evaluator: Any, *, operation: str, principal: Any, deployment: str, adapter: ExternalActionNomosStorageAdapter, context: Mapping[str, Any] | None) -> Any:
    fn = getattr(evaluator, "evaluate_runtime_boundary_with_active_policy", None)
    if fn is None and callable(evaluator):
        fn = evaluator
    if fn is None:
        raise AuthorizationUnavailable()
    kwargs = {
        "deployment": deployment,
        "operation": operation,
        "subject": getattr(principal, "separation_subject", None),
        "principal": getattr(principal, "principal", None),
        "storage": adapter,
        "require_active": True,
        "enforcement_mode": "enforce",
    }
    if context:
        kwargs["context"] = dict(context)
    try:
        result = fn(**kwargs)
    except TypeError:
        # A narrow test double may use ``action`` instead of ``operation``;
        # retry only with the documented security arguments, never by
        # dropping enforcement or the same-transaction adapter.
        kwargs.pop("operation", None)
        kwargs["action"] = operation
        try:
            result = fn(**kwargs)
        except Exception as exc:
            raise AuthorizationUnavailable() from exc
    except Exception as exc:
        raise AuthorizationUnavailable() from exc
    if inspect.isawaitable(result):
        raise AuthorizationUnavailable("review_authority_unavailable", "async Nomos evaluators require an async route")
    return result


def _normalize_decision(value: Any) -> NomosDecision:
    if isinstance(value, NomosDecision):
        return value
    if value is None:
        raise AuthorizationUnavailable()
    if isinstance(value, Mapping):
        allowed = value.get("allowed")
        effect = value.get("effect", value.get("decision_effect"))
        obligations = value.get("obligations_performed", value.get("obligations", False))
        if isinstance(obligations, (list, tuple, set)):
            obligations = bool(obligations)
        return NomosDecision(
            allowed=allowed is True,
            effect=effect if isinstance(effect, str) else "",
            decision_id=value.get("decision_id", value.get("id")),
            policy_digest=value.get("policy_digest"),
            obligations_performed=obligations is True,
            provider_id=value.get("provider_id"),
            subject_binding_id=value.get("subject_binding_id"),
        )
    return NomosDecision(
        allowed=getattr(value, "allowed", False) is True,
        effect=getattr(value, "effect", ""),
        decision_id=getattr(value, "decision_id", None),
        policy_digest=getattr(value, "policy_digest", None),
        obligations_performed=getattr(value, "obligations_performed", False) is True,
        provider_id=getattr(value, "provider_id", None),
        subject_binding_id=getattr(value, "subject_binding_id", None),
    )


def authorize(
    evaluator: Any,
    *,
    operation: str,
    principal: Any,
    deployment: str,
    transaction: Any = None,
    storage_adapter: ExternalActionNomosStorageAdapter | None = None,
    context: Mapping[str, Any] | None = None,
) -> NomosDecision:
    """Authorize an operation through active Nomos enforcement."""

    adapter = storage_adapter or ExternalActionNomosStorageAdapter(transaction=transaction)
    result = _call_evaluator(
        evaluator,
        operation=operation,
        principal=principal,
        deployment=deployment,
        adapter=adapter,
        context=context,
    )
    decision = _normalize_decision(result)
    if not (decision.allowed is True and decision.effect == "allow" and decision.obligations_performed is True):
        raise AuthorizationError("review_authority_unavailable")
    return decision


def authorize_operation(*args: Any, **kwargs: Any) -> NomosDecision:
    return authorize(*args, **kwargs)


def require_separation(proposer_subject: str, approver_subject: str) -> None:
    if not isinstance(proposer_subject, str) or not isinstance(approver_subject, str) or proposer_subject == approver_subject:
        raise SeparationError()


def require_distinct_subjects(*subjects: str) -> None:
    if len(subjects) != len(set(subjects)):
        raise SeparationError()


__all__ = [
    "AuthorizationError",
    "AuthorizationUnavailable",
    "SeparationError",
    "NomosDecision",
    "ExternalActionNomosStorageAdapter",
    "authorize",
    "authorize_operation",
    "require_separation",
    "require_distinct_subjects",
]
