"""Core domain for reviewed external actions.

This package deliberately contains no HTTP client, provider adapter, queue, or
database connection.  It is the small, deterministic state machine shared by
the SQL and HTTP integrations.
"""

from .action_contracts import (
    ActionContract,
    ActionContractError,
    canonical_json,
    canonical_json_bytes,
    digest_value,
    effect_instance_key,
    idempotency_key_hash,
    provider_idempotency_key,
    provider_idempotency_key_digest,
    submission_content_hash,
    target_matches,
)
from .models import (
    Actor,
    AuditEvent,
    Decision,
    ExecutionClaim,
    Outcome,
    PayloadVersion,
    Proposal,
    ServiceResult,
)
from .service import ExternalActionError, ExternalActionService
from .store import ExternalActionStore, InMemoryExternalActionStore

__all__ = [
    "ActionContract",
    "ActionContractError",
    "Actor",
    "AuditEvent",
    "Decision",
    "ExecutionClaim",
    "Outcome",
    "PayloadVersion",
    "Proposal",
    "ServiceResult",
    "ExternalActionError",
    "ExternalActionService",
    "ExternalActionStore",
    "InMemoryExternalActionStore",
    "canonical_json",
    "canonical_json_bytes",
    "digest_value",
    "effect_instance_key",
    "idempotency_key_hash",
    "provider_idempotency_key",
    "provider_idempotency_key_digest",
    "submission_content_hash",
    "target_matches",
]
