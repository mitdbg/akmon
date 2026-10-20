"""Storage boundaries for the external-action review lifecycle.

The in-memory implementation is deliberately a test double.  Production uses
``PostgresExternalActionStore``.  The latter never receives raw-table grants:
all reads and writes go through fixed-signature security-definer functions
owned by the deployment's no-login function owner.  Construction fails closed
unless the complete fixed function contract is installed.
"""

from __future__ import annotations

import secrets
import threading
import time
import base64
import hashlib
import hmac
import json
import re
from contextlib import contextmanager
from collections import defaultdict
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from collections.abc import Iterator, Mapping, MutableMapping, MutableSequence
from typing import Any, Callable, Protocol

from okg.substrate.db.unit_of_work import NestedTransactionRefused, UnitOfWork

from .models import (
    AuditEvent,
    AuthorizationRefs,
    ClaimCommand,
    Decision,
    ExecutionClaim,
    Outcome,
    OutcomeCommand,
    OwnerUnavailabilityCommand,
    PayloadVersion,
    Proposal,
    ReviewTransitionCommand,
    ServiceResult,
    SubmitCommand,
)


def new_ulid() -> str:
    """Generate a valid uppercase Crockford ULID without an external package."""

    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    timestamp = int(time.time() * 1000) & ((1 << 48) - 1)
    value = (timestamp << 80) | secrets.randbits(80)
    chars = []
    for shift in range(125, -1, -5):
        chars.append(alphabet[(value >> shift) & 31])
    return "".join(chars[-26:])


class ExternalActionStore(Protocol):
    """Minimal storage boundary consumed by :class:`ExternalActionService`."""

    lock: threading.RLock


class ExternalActionStoreError(RuntimeError):
    """A storage or deployment-authority failure safe to expose at startup."""

    code = "review_authority_unavailable"

    def __init__(self, message: str, *, code: str | None = None, sqlstate: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.sqlstate = sqlstate


def _raise_closed_claim_refusal(value: Any) -> None:
    """Turn the claim RPCs' closed expiry refusal into a typed error.

    ``external_action_prepare_claim`` and ``external_action_claim`` record an
    elapsed execution deadline and then RETURN this refusal instead of
    raising, so the ``approval_expired`` event commits with the caller's
    transaction.  Only the exact two-field ``proposal_expired`` shape is typed;
    any other refusal shape is the generic unavailable authority.
    """

    if not isinstance(value, Mapping) or "refusal" not in value:
        return
    if (
        set(value) == {"refusal", "proposal_id"}
        and value.get("refusal") == "proposal_expired"
        and isinstance(value.get("proposal_id"), str)
    ):
        raise ExternalActionStoreError(
            "external action proposal has expired", code="proposal_expired"
        )
    raise ExternalActionStoreError("external-action database authority unavailable")


def _is_explicit_policy_denial(outcome: Any, fingerprint: Any) -> bool:
    """Distinguish a verified policy refusal from runtime authority failure."""
    from okg.substrate.nomos.core import PolicyDecision
    from okg.substrate.nomos.runtime_enforcement import NomosRuntimeOutcome

    if not isinstance(outcome, NomosRuntimeOutcome) or not isinstance(outcome.decision, PolicyDecision):
        return False
    decision = outcome.decision
    if (not isinstance(fingerprint, str) or not fingerprint
        or decision.policy_fingerprint != fingerprint
        or outcome.allowed is not False or outcome.final_effect != "deny"
        or decision.effect != "deny" or outcome.required_obligations != ()
        or outcome.missing_obligations != () or not isinstance(decision.reason_code, str)
        or not isinstance(decision.matched_rule_ids, tuple)):
        return False
    reason = decision.reason_code
    if decision.matched_rule_ids:
        return (all(isinstance(item, str) and item for item in decision.matched_rule_ids)
                and bool(reason) and not reason.startswith("nomos.runtime.")
                and reason != "nomos.invalid_identity_fact")
    return reason == "nomos.default_deny"


# These are deliberately fixed identifiers, not relation names supplied by a
# caller.  Each SQL function must use static statements for its operation and
# must assert the deployment resolved from ``session_user`` inside the same
# transaction.  A separate entry point exists for each semantic transition;
# the production service never uses a generic ``kind``/record dispatcher.
REQUIRED_SECURITY_DEFINER_FUNCTIONS: tuple[tuple[str, str], ...] = (
    ("external_automation_transaction_context", ""),
    ("external_automation_register", "jsonb"),
    ("external_automation_read", "jsonb"),
    ("external_automation_link", "jsonb"),
    ("external_action_store_snapshot", ""),
    ("external_action_authorized_read", "jsonb"),
    ("external_action_startup_login_binding", ""),
    ("external_action_claim_owner_unavailability", "text"),
    ("external_action_store_resolve_principal", "text, text"),
    (
        "external_action_insert_dpop_replay_nonce",
        "text, text, text, text, text, text, text, timestamp with time zone, timestamp with time zone",
    ),
    ("external_action_store_startup", "jsonb"),
    ("external_action_verifier_capability_status", ""),
    ("external_action_record_auth_audit", "jsonb"),
    # Authority-reference verification validates the persisted pair on every
    # lifecycle transition.  Policy loading and persistence carry the locked
    # provider/binding barrier through their fixed SQL functions.
    ("external_action_nomos_policy_bundle", "text, text, text"),
    ("external_action_nomos_authority_snapshot", "text, text, text, text, text"),
    ("external_action_nomos_persist_decision", "jsonb"),
    ("external_action_submit", "jsonb"),
    ("external_action_review_transition", "jsonb"),
    ("external_action_prepare_claim", "jsonb"),
    ("external_action_claim", "jsonb"),
    ("external_action_report_outcome", "jsonb"),
    ("external_action_owner_unavailability", "jsonb"),
    ("external_action_restore_create", "jsonb"),
    # The restore source manifest is deployment-owned.  It is not read from
    # arbitrary request paths and is distinct from the SQL finalize union of
    # surviving database keys plus verified positive ledger proofs.
    ("external_action_restore_inventory", "text"),
    ("external_action_restore_batch", "jsonb"),
    # Finalization is a two-call prepare/sign/commit protocol on one active
    # connection.  The prepare result is database-owned (post-event root,
    # recovery-set digest/count, and completed_at), so Python can sign it
    # without accepting caller-supplied timestamps or roots.
    ("external_action_restore_prepare", "jsonb"),
    ("external_action_restore_finalize", "jsonb"),
    ("external_action_store_insert_lease", "jsonb"),
    ("external_action_materialize_expiry", "text"),
)

_DEPLOYMENT_FUNCTIONS: tuple[tuple[str, str], ...] = (
    ("external_action_session_deployment", ""),
    ("external_action_assert_deployment", "text"),
)

_SECURITY_BARRIER_VIEWS: tuple[str, ...] = (
    "v_external_action_decisions",
    "v_external_action_events",
    "v_external_action_executions",
    "v_external_action_outcomes",
    "v_external_action_payload_versions",
    "v_external_action_proposals",
    "v_external_action_restore_fences",
    "v_external_action_restore_recoveries",
    "v_external_action_restore_session_terminal",
    "v_external_action_restore_sessions",
)


# Keep SQL call construction closed.  These statements are authored by this
# module and cannot be selected by request data.  The server role receives
# EXECUTE on the matching signatures only.
_RPC_STATEMENTS: dict[str, str] = {
    name: f"SELECT okg.{name}({', '.join('%s' for _ in args.split(', '))})"
    for name, args in REQUIRED_SECURITY_DEFINER_FUNCTIONS
    if args
}
_RPC_STATEMENTS.update({name: f"SELECT okg.{name}()" for name, args in REQUIRED_SECURITY_DEFINER_FUNCTIONS if not args})
_RPC_STATEMENTS.update({name: f"SELECT okg.{name}({', '.join('%s' for _ in args.split(', '))})" for name, args in _DEPLOYMENT_FUNCTIONS if args})
_RPC_STATEMENTS.update({name: f"SELECT okg.{name}()" for name, args in _DEPLOYMENT_FUNCTIONS if not args})


def _function_label(name: str, args: str) -> str:
    return f"okg.{name}({args})"


def _jsonable(value: Any) -> Any:
    """Encode dataclass records without allowing bytes or secrets into SQL."""

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, bytes):
        return {"__okg_bytes_b64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key): _jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(child) for child in value]
    if hasattr(value, "to_mapping"):
        return _jsonable(value.to_mapping())
    if hasattr(value, "__dataclass_fields__"):
        from dataclasses import fields

        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    raise TypeError(f"unsupported external-action record value: {type(value).__name__}")


def _jsonb_parameter(value: Any) -> Any:
    """Adapt one JSON command for psycopg without importing it for tests.

    Psycopg deliberately does not guess whether a Python mapping is JSON or a
    composite value.  The production RPCs all take JSONB, so passing a raw
    dict would fail at execute time.  Keep the fallback for lightweight
    connection doubles used by deterministic unit tests.
    """

    try:
        from psycopg.types.json import Jsonb
    except ImportError:  # pragma: no cover - only minimal test environments
        return value
    return Jsonb(value)


def _unjsonable(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {"__okg_bytes_b64"}:
            try:
                return base64.b64decode(value["__okg_bytes_b64"], validate=True)
            except (ValueError, TypeError) as exc:
                raise ExternalActionStoreError("durable snapshot contains invalid bytes") from exc
        return {key: _unjsonable(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_unjsonable(child) for child in value]
    return value


def _as_datetime(value: Any) -> datetime | str | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    return value


@dataclass(frozen=True, slots=True)
class TransitionRecord:
    deployment: str
    proposal_id: str
    operation: str
    actor_separation_subject: str
    idempotency_key_hash: str
    transition_request_hash: str
    result: ServiceResult


class InMemoryExternalActionStore:
    """Thread-safe append-only store with proposal projections."""

    def __init__(self, *, now: datetime | None = None, execution_epoch: int = 1, execution_epoch_lease_digest: str | None = None, policy_digest: str | None = None) -> None:
        self.lock = threading.RLock()
        self.proposals: dict[str, Proposal] = {}
        self.payload_versions: dict[str, list[PayloadVersion]] = defaultdict(list)
        self.decisions: dict[str, list[Decision]] = defaultdict(list)
        self.executions: dict[str, ExecutionClaim] = {}
        self.outcomes: dict[str, Outcome] = {}
        self.events: dict[str, list[AuditEvent]] = defaultdict(list)
        self.transition_idempotency: dict[tuple[str, str, str], TransitionRecord] = {}
        self.submit_idempotency: dict[tuple[str, str, str], tuple[str, str]] = {}
        self.attestations: dict[str, dict[str, Any]] = {}
        self.recovery_tombstones: dict[tuple[str, str], dict[str, Any]] = {}
        self.contracts: dict[str, Any] = {}
        self.leases: dict[str, list[Any]] = defaultdict(list)
        self.restore_fences: dict[str, dict[str, Any]] = {}
        self.execution_epoch = execution_epoch
        self.execution_epoch_lease_digest = execution_epoch_lease_digest
        self.policy_digest = policy_digest
        self._now = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)

    def now(self) -> datetime:
        with self.lock:
            return self._now

    def set_now(self, value: datetime) -> None:
        with self.lock:
            if value.tzinfo is None:
                raise ValueError("store clock must be timezone-aware")
            self._now = value.astimezone(timezone.utc)

    def advance(self, **kwargs: Any) -> datetime:
        from datetime import timedelta

        with self.lock:
            self._now += timedelta(**kwargs)
            return self._now

    def next_id(self) -> str:
        return new_ulid()

    def get(self, proposal_id: str) -> Proposal | None:
        return self.proposals.get(proposal_id)

    def get_payload(self, proposal_id: str, version: int | None = None) -> PayloadVersion | None:
        versions = self.payload_versions.get(proposal_id, ())
        if version is None:
            return versions[-1] if versions else None
        return next((item for item in versions if item.version == version), None)

    def get_decision(self, proposal_id: str, decision_id: str | None = None) -> Decision | None:
        rows = self.decisions.get(proposal_id, ())
        if decision_id is None:
            return rows[-1] if rows else None
        return next((item for item in rows if item.decision_id == decision_id), None)

    def get_execution(self, proposal_id: str) -> ExecutionClaim | None:
        value = self.executions.get(proposal_id)
        if isinstance(value, ExecutionClaim):
            return value
        return next(
            (item for item in self.executions.values()
             if isinstance(item, ExecutionClaim) and item.proposal_id == proposal_id),
            None,
        )

    def get_outcome(self, execution_id: str) -> Outcome | None:
        value = self.outcomes.get(execution_id)
        if isinstance(value, Outcome):
            return value
        return next(
            (item for item in self.outcomes.values()
             if isinstance(item, Outcome) and item.execution_id == execution_id),
            None,
        )

    def get_events(self, effect_key: str) -> tuple[AuditEvent, ...]:
        return tuple(self.events.get(effect_key, ()))

    def all_proposals(self, *, deployment: str | None = None) -> tuple[Proposal, ...]:
        values = self.proposals.values()
        if deployment is not None:
            values = (value for value in values if value.deployment == deployment)
        return tuple(values)

    def counts(self) -> dict[str, int]:
        return {
            "proposals": len(self.proposals),
            "payload_versions": sum(len(items) for items in self.payload_versions.values()),
            "decisions": sum(len(items) for items in self.decisions.values()),
            "executions": len(self.executions), "outcomes": len(self.outcomes),
            "events": sum(len(items) for items in self.events.values()),
            "transition_idempotency": len(self.transition_idempotency),
        }

    @contextmanager
    def transaction(self):
        """Provide the transaction-shaped hook used by the startup gate."""

        with self.lock:
            yield self

    def retained_leases(self, *, deployment: str | None = None) -> tuple[Any, ...]:
        if deployment is None:
            return tuple(item for rows in self.leases.values() for item in rows)
        return tuple(self.leases.get(deployment, ()))

    def insert_lease(self, *, lease: Any, **metadata: Any) -> Any:
        deployment = getattr(lease, "payload", {}).get("deployment") or getattr(lease, "deployment", None)
        if deployment is None:
            raise ValueError("lease has no deployment")
        if any(getattr(item, "envelope_digest", None) == getattr(lease, "envelope_digest", None) for item in self.leases[deployment]):
            return lease
        self.leases[deployment].append(lease)
        return lease

    def restore_fence(self, *, deployment: str | None = None) -> dict[str, Any] | None:
        return self.restore_fences.get(deployment or "")

    def set_restore_fence(self, *, deployment: str, **fields: Any) -> None:
        self.restore_fences[deployment] = dict(fields)


class _DurableDict(MutableMapping[Any, Any]):
    """Dict-shaped compatibility surface for the existing service.

    Reads are hydrated from the fixed complete snapshot RPC.  Assignments are
    rejected so an accidental legacy projection write cannot bypass a typed
    lifecycle transition.
    """

    def __init__(self, store: "PostgresExternalActionStore", kind: str) -> None:
        self._store = store
        self._kind = kind
        self._data: dict[Any, Any] = {}

    def __getitem__(self, key: Any) -> Any:
        return self._data[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[key] = value
        self._store._append(self._kind, {"key": key, "value": value})

    def __delitem__(self, key: Any) -> None:
        # No production lifecycle operation deletes an evidence row.  A
        # deletion on this compatibility surface is therefore a programming
        # error, rather than a tempting raw-table DELETE fallback.
        raise ExternalActionStoreError("external-action durable records are append-only")

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def _load(self, key: Any, value: Any) -> None:
        self._data[key] = value


class _DurableList(MutableSequence[Any]):
    def __init__(self, store: "PostgresExternalActionStore", kind: str, bucket: Any) -> None:
        self._store = store
        self._kind = kind
        self._bucket = bucket
        self._data: list[Any] = []

    def __getitem__(self, index: int | slice) -> Any:
        return self._data[index]

    def __setitem__(self, index: int | slice, value: Any) -> None:
        raise ExternalActionStoreError("external-action durable records are append-only")

    def __delitem__(self, index: int | slice) -> None:
        raise ExternalActionStoreError("external-action durable records are append-only")

    def __len__(self) -> int:
        return len(self._data)

    def insert(self, index: int, value: Any) -> None:
        if index != len(self._data):
            raise ExternalActionStoreError("external-action durable records are append-only")
        self._data.append(value)
        self._store._append(self._kind, {"bucket": self._bucket, "value": value})

    def _load(self, value: Any) -> None:
        self._data.append(value)


class _DurableBuckets:
    """Small ``defaultdict(list)`` equivalent used by service projections."""

    def __init__(self, store: "PostgresExternalActionStore", kind: str) -> None:
        self._store = store
        self._kind = kind
        self._data: dict[Any, _DurableList] = {}

    def __bool__(self) -> bool:
        # Merely accessing a bucket does not create retained durable facts.
        return any(self._data.values())

    def __getitem__(self, bucket: Any) -> _DurableList:
        if bucket not in self._data:
            self._data[bucket] = _DurableList(self._store, self._kind, bucket)
        return self._data[bucket]

    def get(self, bucket: Any, default: Any = None) -> Any:
        return self._data.get(bucket, default)

    def items(self):
        return self._data.items()

    def values(self):
        return self._data.values()

    def _load(self, bucket: Any, value: Any) -> None:
        self[bucket]._load(value)


def _enter_store_unit(unit: UnitOfWork) -> None:
    """Enter ``unit``; report a refused open as the store's closed error."""

    try:
        UnitOfWork.__enter__(unit)
    except NestedTransactionRefused as exc:
        raise ExternalActionStoreError(str(exc)) from exc


class _DatabaseLock(UnitOfWork):
    """RLock plus one PostgreSQL transaction for service transitions.

    The shared unit of work owns the transaction rules (issue #2574): it
    refuses to open on a connection a caller left mid-transaction, commits a
    refusal marked with ``committed_refusal`` -- such as the expiry event
    behind a late claim (#1925) or a late review decision (#2342) -- keeps the
    original error when the rollback fails, and releases the lock on every
    path, including a rollback that raises (#2347).
    """

    def __init__(self, store: "PostgresExternalActionStore") -> None:
        self._store = store
        super().__init__(
            lambda: store._conn,
            label="external-action service lock",
            lock=threading.RLock(),
            on_open=self._opened,
            on_close=self._closed,
        )

    def _opened(self) -> None:
        self._store._active_transaction = True
        self._store._refresh_snapshot()

    def _closed(self) -> None:
        self._store._active_transaction = False

    def __enter__(self) -> "PostgresExternalActionStore":
        _enter_store_unit(self)
        return self._store


class _StoreTransaction(UnitOfWork):
    """One store transaction outside the service lock, on the shared unit of work."""

    def __init__(self, store: "PostgresExternalActionStore") -> None:
        self.store = store
        self._committed = False
        super().__init__(
            lambda: store._conn,
            label="external-action store transaction",
            on_open=self._opened,
            on_close=self._closed,
        )

    def _opened(self) -> None:
        self.store._active_transaction = True

    def _closed(self) -> None:
        self.store._active_transaction = False

    def __enter__(self) -> "_StoreTransaction":
        _enter_store_unit(self)
        return self

    def commit(self) -> None:
        # psycopg commits when the transaction context exits.  This method is
        # present for ``pre_bind_restore_fence``'s transaction-shaped API.
        self._committed = True


class PostgresExternalActionStore:
    """Durable store backed solely by fixed, security-definer SQL functions.

    This adapter keeps a complete transaction-scoped projection cache for
    reads, while every mutation uses a fixed semantic command RPC.  It never
    selects or mutates an ``external_action_*`` table directly.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        deployment: str | None = None,
        verifier_capability: str | None = None,
        connection: Any = None,
        connection_factory: Callable[..., Any] | None = None,
        check_contract: bool = True,
    ) -> None:
        if connection is not None and dsn is not None:
            raise ValueError("pass either dsn or connection, not both")
        if connection is None:
            if not dsn:
                raise ValueError("a PostgreSQL DSN is required")
            if connection_factory is None:
                try:
                    from okg.substrate.db.connection import connect as _okg_connect
                except ImportError as exc:  # pragma: no cover - packaging failure
                    raise ExternalActionStoreError("PostgreSQL driver is unavailable") from exc
                # okg's factory, not psycopg's: it carries the session
                # search_path pin (issue #1968) along with the TCP keepalives,
                # connect timeout and idle-in-transaction bound. The
                # security-definer functions this store calls keep their own
                # `SET search_path = pg_catalog, okg`, which replaces the
                # session's for the duration of each call, so the audit in
                # `_assert_function_search_paths` is unaffected.
                connection_factory = _okg_connect
            connection = connection_factory(dsn)
        self._conn = connection
        self._dsn = dsn  # never included in an exception or projection
        self.deployment = deployment
        self._verifier_capability = self._validate_verifier_capability(
            verifier_capability
        )
        self._verifier_capability_generation: int | None = None
        self._pending_verifier_assertion: dict[str, Any] | None = None
        self._active_transaction = False
        self.lock = _DatabaseLock(self)
        self.proposals = _DurableDict(self, "proposal")
        self.payload_versions = _DurableBuckets(self, "payload")
        self.decisions = _DurableBuckets(self, "decision")
        self.executions = _DurableDict(self, "execution")
        self.outcomes = _DurableDict(self, "outcome")
        self.events = _DurableBuckets(self, "event")
        self.auth_audits = _DurableDict(self, "auth_audit")
        self.transition_idempotency = _DurableDict(self, "transition_idempotency")
        self.submit_idempotency = _DurableDict(self, "submit_idempotency")
        self.attestations = _DurableDict(self, "attestation")
        self.contracts = _DurableDict(self, "contract")
        self.leases = _DurableBuckets(self, "lease")
        self.restore_fences = _DurableDict(self, "restore_fence")
        self.restore_attestations = _DurableDict(self, "restore_attestation")
        self.restore_sessions = _DurableDict(self, "restore_session")
        self.restore_checkpoint_staging = _DurableDict(self, "restore_checkpoint_staging")
        # Newer snapshot RPCs expose one row per immutable checkpoint as well
        # as the batch header.  Keep this optional for compatibility with a
        # snapshot produced before the per-entry projection was added; the
        # restore service falls back to the header projection in that case.
        self.restore_checkpoint_entries = _DurableDict(self, "restore_checkpoint_entries")
        self.restore_recoveries = _DurableDict(self, "restore_recovery")
        self.dpop_replay_nonces = _DurableDict(self, "dpop_replay_nonce")
        self.execution_epoch = 0
        self.execution_epoch_lease_digest: str | None = None
        self.policy_digest: str | None = None
        # The active-policy SQL boundary requires the verified provider and
        # subject-binding ids.  They are populated by the service from the
        # authentication result while the store lock is held; they are never
        # accepted from a request body.
        self._nomos_provider_id: str | None = None
        self._nomos_subject_binding_id: str | None = None
        self._nomos_authority: dict[str, Any] | None = None
        if check_contract:
            # One store transaction ends before callers enter the service
            # transaction, so the startup fence cannot become an accidental
            # nested savepoint that never commits the durable state.  A failed
            # rollback no longer replaces the error that caused it.
            with _StoreTransaction(self):
                self._require_runtime_contract()
                self._refresh_snapshot()

    @property
    def connection(self) -> Any:
        return self._conn

    @staticmethod
    def _validate_verifier_capability(value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            )
        return value

    def _set_verifier_capability(self, value: str) -> None:
        """Set the server-only capability transaction-locally.

        The parameter is never interpolated into SQL, returned, persisted, or
        included in an error. PostgreSQL clears the setting at transaction
        end; the explicit empty value after audit insertion shortens exposure
        further.
        """

        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    "SELECT set_config('okg.external_action_verifier_capability', %s, true)",
                    (value,),
                )
        except Exception as exc:
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            ) from exc

    @staticmethod
    def _credential_evidence_projection(command: Mapping[str, Any]) -> dict[str, Any]:
        fields = (
            "principal",
            "separation_subject",
            "nomos_provider_id",
            "nomos_subject_binding_id",
            "authentication_method_code",
            "issuer_or_trust_domain_digest",
            "sender_key_or_certificate_thumbprint",
            "token_or_certificate_id_digest",
            "dpop_jti_digest",
            "auth_config_digest",
            "result_code",
            "credential_expires_at",
            "provider_status",
            "binding_status",
            "provider_updated_at",
            "binding_updated_at",
            "provider_credential_generation_digest",
            "binding_credential_generation_digest",
            "credential_generation_digest",
            "presented_credential_generation_digest",
            "token_issued_at",
            "certificate_thumbprint",
            "certificate_not_before",
            "certificate_not_after",
        )
        return {field: command.get(field) for field in fields}

    @staticmethod
    def _verifier_digest(domain: str, value: Any) -> str:
        from .signing import digest_value

        return digest_value(domain, _jsonable(value))

    @staticmethod
    def _canonical_verifier_timestamp(value: Any) -> str:
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif isinstance(value, datetime):
            parsed = value
        else:
            raise ExternalActionStoreError(
                "Nomos authority barrier timestamp is unavailable"
            )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")

    def _verifier_tag(self, assertion: Mapping[str, Any]) -> str:
        from .signing import canonical_json_bytes

        capability = self._verifier_capability
        if capability is None:
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            )
        message = (
            b"okg-external-action-verifier-assertion-v1\x00"
            + canonical_json_bytes(dict(assertion))
        )
        return "sha256:" + hmac.new(
            bytes.fromhex(capability), message, hashlib.sha256
        ).hexdigest()

    def begin_verifier_authorization(
        self,
        *,
        deployment: str,
        operation: str,
        transition_digest: str,
        actor: Any,
        authentication_evidence: Mapping[str, Any],
    ) -> None:
        """Create one short-lived verifier assertion before Nomos evaluation."""

        if self._verifier_capability is None or self._verifier_capability_generation is None:
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            )
        if deployment != self.deployment:
            raise ExternalActionStoreError(
                "external-action verifier deployment is unavailable"
            )
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", transition_digest or ""):
            raise ExternalActionStoreError(
                "external-action verifier transition digest is unavailable"
            )
        evidence = dict(authentication_evidence)
        evidence.update(
            {
                "principal": getattr(actor, "principal", None),
                "separation_subject": getattr(actor, "separation_subject", None),
                "nomos_provider_id": getattr(actor, "nomos_provider_id", None),
                "nomos_subject_binding_id": getattr(
                    actor, "nomos_subject_binding_id", None
                ),
                "result_code": evidence.get("result_code") or "authenticated",
            }
        )
        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=30)
        nonce = secrets.token_bytes(32)
        self._pending_verifier_assertion = {
            "schema": "okg.external_action.verifier_assertion/v1",
            "deployment": deployment,
            "capability_generation": self._verifier_capability_generation,
            "authentication_audit_id": new_ulid(),
            "operation": operation,
            "transition_digest": transition_digest,
            "credential_evidence_digest": self._verifier_digest(
                "okg-external-action-credential-evidence-v1",
                self._credential_evidence_projection(evidence),
            ),
            "nonce_digest": "sha256:" + hashlib.sha256(nonce).hexdigest(),
            "issued_at": now.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "expires_at": expires.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
        }

    def _query(self, statement: str, params: tuple[Any, ...] = ()) -> list[Any]:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(statement, params)
                return list(cursor.fetchall())
        except ExternalActionStoreError:
            raise
        except Exception as exc:
            # PostgreSQL exception text is intentionally not returned to the
            # client, but its closed SQLSTATE/message class lets the service
            # preserve the contract's typed restore coverage refusal.  All
            # other failures remain the generic unavailable authority.
            primary = getattr(getattr(exc, "diag", None), "message_primary", None)
            description = str(primary or exc).lower()
            if primary in {"run_identity_conflict", "run_not_found", "run_generation_invalid", "run_proposal_mismatch", "run_link_limit", "run_restore_quarantined", "run_authority_invalid", "invalid_run_record"}:
                code = primary
            elif "coverage" in description and (
                "incomplete" in description or "extra" in description
            ):
                code = "incomplete_restore_coverage"
            elif "restore checkpoint conflicts with existing terminal outcome" in description:
                code = "restore_terminal_conflict"
            elif any(
                marker in description
                for marker in (
                    "restore id was reused",
                    "restore attestation envelope digest was already used",
                    "restore epochs are not bound",
                    "restore session is terminal",
                    "restore reconciliation is terminal",
                    "restore batch replay conflicts",
                    "restore finalize replay conflicts",
                    "restore session is already complete",
                    "restore final epoch is stale",
                    "restore session is not accepting checkpoint batches",
                    "restore session is not finalizable",
                    "restore checkpoint effect key is duplicated",
                )
            ):
                code = "restore_reconciliation_conflict"
            else:
                code = None
            raise ExternalActionStoreError(
                "external-action database authority unavailable",
                code=code,
                sqlstate=getattr(exc, "sqlstate", None),
            ) from exc

    def require_runtime_contract(self) -> None:
        """Check entry points and hardening before serving or mutating.

        The check runs in its own store transaction unless one is active, so
        it leaves the connection idle.  On a non-autocommit connection its
        reads used to open a transaction nothing committed, and every later
        store transaction nested inside it as a savepoint (issue #2574).
        """

        if self._active_transaction:
            self._require_runtime_contract()
            return
        with _StoreTransaction(self):
            self._require_runtime_contract()

    def _require_runtime_contract(self) -> None:
        capability = self._verifier_capability
        if capability is None:
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            )
        self._set_verifier_capability(capability)
        expected = dict(REQUIRED_SECURITY_DEFINER_FUNCTIONS + _DEPLOYMENT_FUNCTIONS)
        names = list(expected)
        rows = self._query(
            """
            SELECT p.proname, pg_catalog.oidvectortypes(p.proargtypes),
                   p.prosecdef, pg_get_userbyid(p.proowner), p.proconfig,
                   EXISTS (
                     SELECT 1
                       FROM aclexplode(
                              COALESCE(p.proacl, acldefault('f', p.proowner))
                            ) AS privilege
                      WHERE privilege.grantee = 0
                        AND privilege.privilege_type = 'EXECUTE'
                   ) AS public_execute,
                   p.prosrc
              FROM pg_catalog.pg_proc AS p
              JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg' AND p.proname = ANY(%s)
            """,
            (names,),
        )
        found: dict[tuple[str, str], tuple[Any, ...]] = {
            (str(row[0]), str(row[1])): tuple(row) for row in rows
        }
        missing = [_function_label(name, args) for name, args in expected.items() if (name, args) not in found]
        insecure: list[str] = []
        for (name, args), row in found.items():
            if expected.get(name) != args:
                insecure.append(_function_label(name, args) + " is an unexpected overload")
                continue
            if row[2] is not True:
                insecure.append(_function_label(name, args) + " is not SECURITY DEFINER")
            owner = row[3]
            if owner != "okg_external_action_function_owner":
                insecure.append(_function_label(name, args) + " has the wrong function owner")
            config = row[4] or ()
            normalized_config = {
                str(item).replace(" ", "")
                for item in config
            }
            if "search_path=pg_catalog,okg" not in normalized_config:
                insecure.append(_function_label(name, args) + " has an unpinned search_path")
            if row[5] is True:
                insecure.append(_function_label(name, args) + " grants EXECUTE to PUBLIC")
            if re.search(r"^\s*EXECUTE\s", str(row[6] or ""), flags=re.IGNORECASE | re.MULTILINE):
                insecure.append(_function_label(name, args) + " contains dynamic SQL")

        role_rows = self._query(
            """
            SELECT rolcanlogin, rolsuper, rolbypassrls, rolcreatedb, rolcreaterole,
                   rolinherit
              FROM pg_catalog.pg_roles
             WHERE rolname = 'okg_external_action_api'
            """
        )
        if role_rows != [(False, False, False, False, False, True)]:
            insecure.append("okg_external_action_api is not an exact NOLOGIN NOBYPASSRLS unprivileged role")
        owner_rows = self._query(
            """
            SELECT rolcanlogin, rolsuper, rolbypassrls, rolcreatedb, rolcreaterole,
                   rolinherit
              FROM pg_catalog.pg_roles
             WHERE rolname = 'okg_external_action_function_owner'
            """
        )
        if owner_rows != [(False, False, False, False, False, False)]:
            insecure.append("okg_external_action_function_owner is not an exact NOLOGIN NOINHERIT NOBYPASSRLS unprivileged role")
        owner_memberships = self._query(
            """
            SELECT granted.rolname
              FROM pg_catalog.pg_auth_members AS membership
              JOIN pg_catalog.pg_roles AS owner
                ON owner.oid = membership.member
              JOIN pg_catalog.pg_roles AS granted
                ON granted.oid = membership.roleid
             WHERE owner.rolname = 'okg_external_action_function_owner'
             ORDER BY granted.rolname
            """
        )
        if owner_memberships:
            insecure.append("okg_external_action_function_owner inherits unexpected roles")

        # The DSN must name the dedicated deployment login, not a migration
        # owner, superuser, or broad application role.  PostgreSQL evaluates
        # these predicates for the live connection, so membership and
        # effective EXECUTE/schema grants cannot be faked by a configuration
        # projection.  The login may inherit exactly one role: the closed API
        # role.  It must not be able to bypass row-level security or own raw
        # external-action tables.
        runtime_rows = self._query(
            """
            SELECT session_user::text, current_user::text,
                   r.rolcanlogin, r.rolsuper, r.rolbypassrls,
                   r.rolinherit, r.rolcreatedb, r.rolcreaterole,
                   (SELECT count(*)::integer
                      FROM pg_catalog.pg_auth_members AS m
                     WHERE m.member = r.oid) AS membership_count,
                   EXISTS (
                     SELECT 1
                       FROM pg_catalog.pg_auth_members AS m
                       JOIN pg_catalog.pg_roles AS granted ON granted.oid = m.roleid
                      WHERE m.member = r.oid
                        AND granted.rolname = 'okg_external_action_api'
                   ) AS inherits_api,
                   has_schema_privilege(session_user, 'okg', 'USAGE') AS schema_usage,
                   (SELECT count(*)::integer
                      FROM pg_catalog.pg_proc AS fp
                      JOIN pg_catalog.pg_namespace AS fn ON fn.oid = fp.pronamespace
                     WHERE fn.nspname = 'okg'
                       AND fp.proname = ANY(%s)
                       AND has_function_privilege(session_user, fp.oid, 'EXECUTE')
                   ) AS effective_rpc_count,
                   (SELECT count(*)::integer
                      FROM pg_catalog.pg_class AS raw
                      JOIN pg_catalog.pg_namespace AS raw_ns ON raw_ns.oid = raw.relnamespace
                     WHERE raw_ns.nspname = 'okg'
                       AND raw.relkind IN ('r', 'p')
                       AND (raw.relname LIKE 'external_action_%%' OR raw.relname LIKE 'external_automation_%%')
                       AND has_table_privilege(session_user, raw.oid,
                           'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
                   ) AS raw_relation_count
              FROM pg_catalog.pg_roles AS r
             WHERE r.rolname = session_user
            """,
            (
                list(expected),
            ),
        )
        if len(runtime_rows) != 1:
            insecure.append("the PostgreSQL login is not a dedicated runtime role")
        else:
            runtime = runtime_rows[0]
            # session_user/current_user equality rules out an injected SET
            # ROLE.  ``rolinherit`` is required because the API privileges
            # are inherited, while all broad role capabilities are denied.
            if runtime[0] != runtime[1]:
                insecure.append("runtime login changed current_user via SET ROLE")
            if runtime[2:8] != (True, False, False, True, False, False):
                insecure.append("runtime login is not NOSUPERUSER NOBYPASSRLS with restricted inheritance")
            if runtime[8] != 1 or runtime[9] is not True:
                insecure.append("runtime login must inherit exactly okg_external_action_api")
            if runtime[10] is not True:
                insecure.append("runtime login lacks effective okg schema USAGE")
            if runtime[11] != len(expected):
                insecure.append("runtime login lacks effective EXECUTE on the fixed RPC set")
            if runtime[12] != 0:
                insecure.append("runtime login has raw external-action table privileges")

        # Direct membership counts are not sufficient: PostgreSQL role
        # inheritance may grant a broad role through an intermediate group.
        # Resolve the complete inherited-role closure for the actual session
        # login and permit exactly the one closed API role.  The query is
        # intentionally based on the live session_user, never a DSN/config
        # projection supplied by an embedder.
        inherited_roles = self._query(
            """
            WITH RECURSIVE inherited(role_oid) AS (
                SELECT m.roleid
                  FROM pg_catalog.pg_auth_members AS m
                  JOIN pg_catalog.pg_roles AS login ON login.oid = m.member
                 WHERE login.rolname = session_user
                UNION
                SELECT m.roleid
                  FROM pg_catalog.pg_auth_members AS m
                  JOIN inherited AS parent ON parent.role_oid = m.member
            )
            SELECT r.rolname
              FROM inherited
              JOIN pg_catalog.pg_roles AS r ON r.oid = inherited.role_oid
             ORDER BY r.rolname
            """
        )
        inherited_names = {str(row[0]) for row in inherited_roles if row}
        if inherited_names != {"okg_external_action_api"}:
            insecure.append("runtime login inherits roles outside okg_external_action_api")

        # CREATE on the Nomos/API schema is enough to replace a function,
        # create a shadow object, or otherwise escape the fixed RPC surface.
        # Check the effective privilege of the live login, including
        # inherited grants.  Raw base-table privileges are forbidden for the
        # runtime login; security-definer functions are the only data path.
        schema_create = self._query(
            """
            SELECT n.nspname
              FROM pg_catalog.pg_namespace AS n
             WHERE n.nspname = 'okg'
               AND has_schema_privilege(session_user, n.oid, 'CREATE')
            """
        )
        if schema_create:
            insecure.append("runtime login has effective CREATE on the okg schema")
        effective_raw_tables = self._query(
            """
            SELECT n.nspname, c.relname
              FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relkind IN ('r', 'p')
               AND has_table_privilege(
                     session_user, c.oid,
                     'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
                   )
             ORDER BY n.nspname, c.relname
            """
        )
        if effective_raw_tables:
            insecure.append("runtime login has effective raw Nomos/table privileges")

        # Registration can inspect two published-ledger metadata columns;
        # neither the transport login nor function owner gets graph contents.
        generation_grants = self._query(
            """
            SELECT
              has_column_privilege('okg_external_action_function_owner', 'okg.v_generations', 'id', 'SELECT'),
              has_column_privilege('okg_external_action_function_owner', 'okg.v_generations', 'deployment_name', 'SELECT'),
              has_table_privilege('okg_external_action_function_owner', 'okg.v_generations', 'SELECT'),
              has_any_column_privilege(session_user, 'okg.v_generations', 'SELECT'),
              has_any_column_privilege('okg_external_action_function_owner', 'public._chronos_branch_interval_checkpoints', 'SELECT')
            """
        )
        if generation_grants != [(True, True, False, False, False)]:
            insecure.append("run generation lookup must use only the two published-ledger view columns")

        # The API role's ACL can be exact while the concrete LOGIN named by
        # the DSN has a broader direct (or inherited) function grant.  Probe
        # the effective privilege of ``session_user`` as well as the group
        # role so an operator cannot accidentally turn an internal helper
        # into a second, unaudited command surface.
        runtime_execute = self._query(
            """
            SELECT p.proname, pg_catalog.oidvectortypes(p.proargtypes)
              FROM pg_catalog.pg_proc AS p
              JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg'
               AND (p.proname LIKE 'external_action_%%' OR p.proname LIKE 'external_automation_%%')
               AND has_function_privilege(session_user, p.oid, 'EXECUTE')
             ORDER BY p.proname, pg_catalog.oidvectortypes(p.proargtypes)
            """
        )
        runtime_extra_execute = [
            _function_label(str(row[0]), str(row[1]))
            for row in runtime_execute
            if expected.get(str(row[0])) != str(row[1])
        ]
        if runtime_extra_execute:
            insecure.append(
                "runtime login can execute unexpected functions: "
                + ", ".join(runtime_extra_execute)
            )

        # An unrelated SECURITY DEFINER function is another privileged RPC
        # even when its name is outside this subsystem.  PostgreSQL's default
        # PUBLIC EXECUTE grant otherwise makes it reachable by every mapped
        # login.  Permit only the fixed contract above; ordinary invoker
        # helpers are not privileged and remain governed by their own schema.
        runtime_security_definer_execute = self._query(
            """
            SELECT p.proname, pg_catalog.oidvectortypes(p.proargtypes)
              FROM pg_catalog.pg_proc AS p
              JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg'
               AND p.prosecdef
               AND has_function_privilege(session_user, p.oid, 'EXECUTE')
             ORDER BY p.proname, pg_catalog.oidvectortypes(p.proargtypes)
            """
        )
        runtime_extra_security_definer = [
            _function_label(str(row[0]), str(row[1]))
            for row in runtime_security_definer_execute
            if expected.get(str(row[0])) != str(row[1])
        ]
        if runtime_extra_security_definer:
            insecure.append(
                "runtime login can execute unrelated SECURITY DEFINER functions: "
                + ", ".join(runtime_extra_security_definer)
            )

        # Reads are intentionally limited to the deployment-filtered,
        # security-barrier projections declared by this lifecycle.  Reject an
        # omitted grant, a view recreated without its barrier, or any extra
        # table/view read granted directly, through PUBLIC, or by membership.
        runtime_relation_reads = self._query(
            """
            SELECT c.relname, c.relkind,
                   COALESCE(c.reloptions, ARRAY[]::text[])
              FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
               AND has_table_privilege(session_user, c.oid, 'SELECT')
             ORDER BY c.relname
            """
        )
        allowed_views = set(_SECURITY_BARRIER_VIEWS)
        readable_views = {
            str(row[0])
            for row in runtime_relation_reads
            if str(row[1]) == "v"
        }
        if readable_views != allowed_views:
            missing_views = sorted(allowed_views - readable_views)
            extra_views = sorted(readable_views - allowed_views)
            if missing_views:
                insecure.append(
                    "runtime login lacks declared review views: "
                    + ", ".join(missing_views)
                )
            if extra_views:
                insecure.append(
                    "runtime login can read unrelated views: "
                    + ", ".join(extra_views)
                )
        non_view_reads = sorted(
            str(row[0])
            for row in runtime_relation_reads
            if str(row[1]) != "v"
        )
        if non_view_reads:
            insecure.append(
                "runtime login can read non-view relations: "
                + ", ".join(non_view_reads)
            )
        missing_barriers = sorted(
            str(row[0])
            for row in runtime_relation_reads
            if str(row[0]) in allowed_views
            and "security_barrier=true" not in {str(item) for item in (row[2] or ())}
        )
        if missing_barriers:
            insecure.append(
                "runtime review views lack security barriers: "
                + ", ".join(missing_barriers)
            )

        raw_relation_grants = self._query(
            """
            SELECT c.relname
              FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relkind IN ('r', 'p')
               AND (c.relname LIKE 'external_action_%%' OR c.relname LIKE 'external_automation_%%')
               AND has_table_privilege(
                     'okg_external_action_api', c.oid,
                     'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
                   )
             ORDER BY c.relname
            """
        )
        if raw_relation_grants:
            insecure.append(
                "okg_external_action_api has raw-table privileges: "
                + ", ".join(str(row[0]) for row in raw_relation_grants)
            )

        allowed_execute = set(expected.items())
        unexpected_execute = self._query(
            """
            SELECT p.proname, pg_catalog.oidvectortypes(p.proargtypes)
              FROM pg_catalog.pg_proc AS p
              JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg'
               AND (p.proname LIKE 'external_action_%%' OR p.proname LIKE 'external_automation_%%')
               AND has_function_privilege(
                     'okg_external_action_api', p.oid, 'EXECUTE'
                   )
             ORDER BY p.proname, pg_catalog.oidvectortypes(p.proargtypes)
            """
        )
        extra_execute = [
            _function_label(str(row[0]), str(row[1]))
            for row in unexpected_execute
            if (str(row[0]), str(row[1])) not in allowed_execute
        ]
        if extra_execute:
            insecure.append(
                "okg_external_action_api can execute unexpected functions: "
                + ", ".join(extra_execute)
            )
        if not missing and not insecure:
            try:
                capability_status = self._json_call(
                    "external_action_verifier_capability_status"
                )
            except Exception as exc:
                raise ExternalActionStoreError(
                    "external-action verifier capability is unavailable"
                ) from exc
            if (
                not isinstance(capability_status, Mapping)
                or capability_status.get("deployment") != self.deployment
                or not isinstance(
                    capability_status.get("capability_generation"), int
                )
                or isinstance(
                    capability_status.get("capability_generation"), bool
                )
                or capability_status["capability_generation"] <= 0
            ):
                insecure.append(
                    "runtime verifier capability is not deployment-bound"
                )
            else:
                self._verifier_capability_generation = capability_status[
                    "capability_generation"
                ]
        if missing or insecure:
            details = []
            if missing:
                details.append("missing fixed entry points: " + ", ".join(missing))
            if insecure:
                details.append("insecure entry points: " + ", ".join(insecure))
            raise ExternalActionStoreError(
                "external-action PostgreSQL contract unavailable; "
                + "; ".join(details)
                + ". Add the fixed SECURITY DEFINER functions to the schema/migration; "
                "raw-table or privileged-connection fallback is forbidden."
            )

    def _json_call(self, function: str, params: tuple[Any, ...] = ()) -> Any:
        statement = _RPC_STATEMENTS.get(function)
        if statement is None:
            raise ExternalActionStoreError("unknown external-action RPC")
        rows = self._query(statement, params)
        if not rows:
            return None
        value = rows[0][0]
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value

    def _call_in_transaction(self, function: str, params: tuple[Any, ...] = ()) -> Any:
        if self._active_transaction:
            return self._json_call(function, params)
        with _StoreTransaction(self):
            return self._json_call(function, params)

    def _verifier_call_in_transaction(
        self, function: str, params: tuple[Any, ...] = ()
    ) -> Any:
        """Call an RPC only after presenting the server-held capability.

        The mapped PostgreSQL login is a transport identity, not verifier
        authority.  Re-present the deployment capability immediately before
        each semantic RPC because authentication-audit persistence clears the
        transaction-local setting to minimize its exposure.
        """

        capability = self._verifier_capability
        if capability is None:
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            )
        if self._active_transaction:
            self._set_verifier_capability(capability)
            return self._json_call(function, params)
        with _StoreTransaction(self):
            self._set_verifier_capability(capability)
            return self._json_call(function, params)

    def _append(self, kind: str, record: Mapping[str, Any]) -> Any:
        # Kept only for compatibility with old callers.  Production service
        # paths must call one of the typed methods below; generic mirroring is
        # deliberately not an accepted durability boundary.
        raise ExternalActionStoreError(
            "generic external-action append is disabled; use a typed transition RPC"
        )

    @property
    def is_durable(self) -> bool:
        return True

    @property
    def nomos_storage_adapter(self) -> "PostgresNomosStorageAdapter":
        return PostgresNomosStorageAdapter(self)

    def _typed(self, function: str, request: Mapping[str, Any]) -> Any:
        """Call one fixed JSONB command RPC on the caller transaction."""

        if function not in _RPC_STATEMENTS or function in {
            "external_action_store_snapshot",
            "external_action_store_resolve_principal",
            "external_action_insert_dpop_replay_nonce",
            "external_action_store_startup",
        }:
            raise ExternalActionStoreError("invalid typed external-action RPC")
        return self._verifier_call_in_transaction(
            function, (_jsonb_parameter(_jsonable(dict(request))),)
        )

    def startup_command(self, request: Mapping[str, Any]) -> Any:
        """Persist one startup-fence command on the active transaction.

        Startup leases and fences are deliberately behind the same fixed
        JSONB RPC as the rest of the runtime.  The envelope is an operation
        discriminator owned by this module, never a relation/table name.
        """

        return self._verifier_call_in_transaction(
            "external_action_store_startup", (_jsonb_parameter(_jsonable(dict(request))),)
        )

    def run_transaction_context(self) -> str:
        value = self._verifier_call_in_transaction("external_automation_transaction_context")
        if not isinstance(value, Mapping) or not isinstance(value.get("transaction_id"), str):
            raise ExternalActionStoreError("run transaction authority unavailable")
        return value["transaction_id"]

    def run_record_command(self, operation: str, command: Mapping[str, Any]) -> Any:
        names = {"register": "external_automation_register", "read": "external_automation_read", "link": "external_automation_link"}
        if operation not in names:
            raise ExternalActionStoreError("unknown run operation")
        try:
            result = self._typed(names[operation], command)
        except ExternalActionStoreError as exc:
            from .service import ExternalActionError

            statuses = {"run_identity_conflict": 409, "run_not_found": 404, "run_generation_invalid": 422, "run_proposal_mismatch": 409, "run_link_limit": 409, "run_restore_quarantined": 409, "run_authority_invalid": 403, "invalid_run_record": 422}
            if exc.code in statuses:
                raise ExternalActionError(exc.code, exc.code, status=statuses[exc.code]) from exc
            raise
        return int(result["status"]), {}, result["body"]

    @staticmethod
    def _authorization_mapping(actor: Any = None, authorization: Any = None) -> dict[str, Any]:
        refs = authorization
        if refs is None:
            refs = AuthorizationRefs(
                getattr(actor, "authentication_audit_id", None) if actor is not None else None,
                getattr(actor, "nomos_decision_id", None) if actor is not None else None,
                None,
            )
        if isinstance(refs, AuthorizationRefs):
            return refs.to_mapping()
        if isinstance(refs, Mapping):
            return {
                "authentication_audit_id": refs.get("authentication_audit_id", refs.get("auth_audit_id")),
                "nomos_decision_id": refs.get("nomos_decision_id", refs.get("nomos_decision_ref")),
                "policy_digest": refs.get("policy_digest"),
            }
        return {
            "authentication_audit_id": getattr(refs, "authentication_audit_id", None),
            "nomos_decision_id": getattr(refs, "nomos_decision_id", None),
            "policy_digest": getattr(refs, "policy_digest", None),
        }

    def record_auth_audit(
        self,
        audit: Mapping[str, Any] | Any = None,
        *,
        request_id: str | None = None,
        deployment: str | None = None,
        **metadata: Any,
    ) -> Any:
        """Persist safe verified-identity evidence through a fixed RPC.

        ``ExternalActionAPI`` passes its ``AuthAudit`` object here.  Only its
        redacted ``as_dict`` representation is accepted; raw credentials or
        token contents never enter the command envelope.  Missing trust
        configuration is an authority error, not a fabricated digest.
        """

        if hasattr(audit, "as_dict"):
            audit = audit.as_dict()
        if not isinstance(audit, Mapping):
            raise ExternalActionStoreError("authentication audit authority unavailable")
        command = dict(audit)
        command.update({key: value for key, value in metadata.items() if value is not None})
        command["deployment"] = deployment or command.get("deployment") or self.deployment
        command["request_id"] = request_id or command.get("request_id")
        command["result_code"] = command.get("result_code") or "authenticated"
        pending = self._pending_verifier_assertion
        if (
            not isinstance(pending, Mapping)
            or not isinstance(pending.get("assertion"), Mapping)
            or not isinstance(pending.get("tag"), str)
        ):
            raise ExternalActionStoreError(
                "external-action verifier assertion is unavailable"
            )
        assertion = dict(pending["assertion"])
        command["authentication_audit_id"] = assertion.get(
            "authentication_audit_id"
        )
        command["verifier_assertion"] = assertion
        command["verifier_assertion_tag"] = pending["tag"]
        observed_credential_digest = self._verifier_digest(
            "okg-external-action-credential-evidence-v1",
            self._credential_evidence_projection(command),
        )
        if observed_credential_digest != assertion.get(
            "credential_evidence_digest"
        ):
            raise ExternalActionStoreError(
                "external-action verifier credential evidence changed"
            )
        if not command.get("deployment") or not command.get("request_id") or not command.get("auth_config_digest"):
            raise ExternalActionStoreError("authentication audit authority unavailable")
        value = self._typed("external_action_record_auth_audit", command)
        self._pending_verifier_assertion = None
        self._set_verifier_capability("")
        # The HTTP adapter's writer contract is deliberately narrow: it gets
        # only the opaque audit reference, never the audit row.  The service
        # path also accepts this string and carries it into the semantic RPC.
        if isinstance(value, Mapping):
            audit_id = value.get("authentication_audit_id", value.get("auth_audit_id"))
            if isinstance(audit_id, str) and audit_id:
                return audit_id
        if isinstance(value, str) and value:
            return value
        raise ExternalActionStoreError("authentication audit authority unavailable")

    def load_active_policy_bundle(
        self,
        conn: Any = None,
        *,
        deployment_name: str | None = None,
        scope: str = "external_action",
    ) -> Any:
        """Load the locked active policy and identity barrier via fixed SQL."""

        if conn is not None and conn is not self.connection:
            raise ExternalActionStoreError("Nomos adapter connection mismatch")
        if (
            deployment_name is not None
            and self.deployment is not None
            and deployment_name != self.deployment
        ):
            raise ExternalActionStoreError("Nomos deployment does not match the durable store")
        provider_id = self._nomos_provider_id
        binding_id = self._nomos_subject_binding_id
        if not isinstance(provider_id, str) or not provider_id:
            raise ExternalActionStoreError("Nomos provider identity is unavailable")
        if not isinstance(binding_id, str) or not binding_id:
            raise ExternalActionStoreError("Nomos subject binding identity is unavailable")
        value = self._verifier_call_in_transaction(
            "external_action_nomos_policy_bundle",
            (scope, provider_id, binding_id),
        )
        if not isinstance(value, Mapping):
            raise ExternalActionStoreError("Nomos policy authority unavailable")
        bundle = value.get("bundle")
        if not isinstance(bundle, Mapping):
            raise ExternalActionStoreError("Nomos policy bundle is unavailable")
        try:
            from okg.substrate.nomos.core import policy_bundle_from_mapping

            parsed = policy_bundle_from_mapping(bundle)
        except Exception as exc:
            raise ExternalActionStoreError("Nomos policy bundle is invalid") from exc
        # Retain only the fixed barrier fields used by the decision RPC.  The
        # raw policy bundle remains in Nomos' typed object, not in a mutable
        # service projection.
        barrier = value.get("authority_barrier")
        if not isinstance(barrier, Mapping):
            raise ExternalActionStoreError("Nomos authority barrier is unavailable")
        self._nomos_authority = {
            "policy_scope": scope,
            "policy_bundle_id": value.get("policy_bundle_id"),
            "policy_version": value.get("policy_version"),
            "policy_fingerprint": value.get("policy_fingerprint"),
            "provider_id": value.get("provider_id"),
            "provider_status": value.get("provider_status"),
            "provider_updated_at": self._canonical_verifier_timestamp(
                value.get("provider_updated_at")
            ),
            "binding_id": value.get("binding_id"),
            "binding_status": value.get("binding_status"),
            "binding_updated_at": self._canonical_verifier_timestamp(
                value.get("binding_updated_at")
            ),
        }
        required_barrier = (
            "policy_bundle_id", "policy_version", "policy_fingerprint",
            "provider_id", "provider_status", "provider_updated_at",
            "binding_id", "binding_status", "binding_updated_at",
        )
        if any(self._nomos_authority.get(key) is None for key in required_barrier):
            raise ExternalActionStoreError("Nomos authority barrier is incomplete")
        return parsed

    def bind_nomos_identity(self, *, provider_id: str, subject_binding_id: str) -> None:
        """Bind verified authentication identity for the current transaction."""

        if not isinstance(provider_id, str) or not provider_id:
            raise ExternalActionStoreError("Nomos provider identity is unavailable")
        if not isinstance(subject_binding_id, str) or not subject_binding_id:
            raise ExternalActionStoreError("Nomos subject binding identity is unavailable")
        self._nomos_provider_id = provider_id
        self._nomos_subject_binding_id = subject_binding_id
        self._nomos_authority = None

    def verify_nomos_authority(
        self,
        *,
        deployment: str,
        authentication_audit_id: str,
        nomos_decision_id: str,
        expected_action: str,
        expected_transition_digest: str,
    ) -> Any:
        """Verify the transaction-bound auth/Nomos pair through fixed SQL."""

        value = self._verifier_call_in_transaction(
            "external_action_nomos_authority_snapshot",
            (
                deployment,
                authentication_audit_id,
                nomos_decision_id,
                expected_action,
                expected_transition_digest,
            ),
        )
        if not isinstance(value, Mapping):
            return value
        normalized = dict(value)
        for field in (
            "provider_updated_at",
            "binding_updated_at",
            "token_issued_at",
            "certificate_not_before",
            "certificate_not_after",
        ):
            if normalized.get(field) is not None:
                normalized[field] = self._canonical_verifier_timestamp(
                    normalized[field]
                )
        return normalized

    def persist_nomos_decision(
        self,
        conn: Any = None,
        *,
        deployment_name: str | None = None,
        decision: Any,
        target: Mapping[str, Any] | None = None,
    ) -> Any:
        """Reject decision-only persistence at the external-action boundary.

        The strict runtime outcome is required so the verifier assertion can
        bind the final effect and completed obligations, not merely the
        policy engine's preliminary decision.
        """

        raise ExternalActionStoreError(
            "strict Nomos runtime outcome is unavailable"
        )

    def persist_nomos_runtime_outcome(
        self,
        conn: Any = None,
        *,
        deployment_name: str | None = None,
        decision: Any,
        outcome: Any,
        target: Mapping[str, Any] | None = None,
    ) -> Any:
        """Persist a decision plus its strict, verifier-authenticated result."""

        # The fixed SQL boundary accepts the same flattened shape as
        # ``nomos.identity_store._decision_row_payload``.  Do not wrap the
        # decision in an opaque ``{"decision": ...}`` envelope: that would
        # make the command fail closed at the SQL allow-list and, more
        # importantly, would hide fields that are part of the decision
        # identity from the idempotency comparison.
        if conn is not None and conn is not self.connection:
            raise ExternalActionStoreError("Nomos adapter connection mismatch")
        if hasattr(decision, "to_dict"):
            decision = decision.to_dict()
        elif hasattr(decision, "__dataclass_fields__"):
            decision = _jsonable(decision)
        if not isinstance(decision, Mapping):
            raise ExternalActionStoreError("Nomos decision is not serializable")
        command = dict(decision)
        if (
            deployment_name is not None
            and self.deployment is not None
            and deployment_name != self.deployment
        ):
            raise ExternalActionStoreError("Nomos deployment does not match the durable store")
        command["deployment"] = deployment_name or self.deployment
        if not command.get("deployment"):
            raise ExternalActionStoreError("Nomos decision deployment is unavailable")
        command["target"] = dict(target or {})
        authority = self._nomos_authority
        if not isinstance(authority, Mapping):
            raise ExternalActionStoreError("Nomos authority barrier is unavailable")
        command.update({
            key: authority[key]
            for key in (
                "policy_scope", "policy_bundle_id", "policy_version",
                "policy_fingerprint", "provider_id", "provider_status",
                "provider_updated_at", "binding_id", "binding_status",
                "binding_updated_at",
            )
        })
        pending = self._pending_verifier_assertion
        if not isinstance(pending, Mapping):
            raise ExternalActionStoreError(
                "external-action verifier assertion is unavailable"
            )
        allowed = getattr(outcome, "allowed", None)
        final_effect = getattr(outcome, "final_effect", None)
        required = list(getattr(outcome, "required_obligations", ()) or ())
        completed = list(getattr(outcome, "enforced_obligations", ()) or ())
        missing = list(getattr(outcome, "missing_obligations", ()) or ())
        if _is_explicit_policy_denial(outcome, authority.get("policy_fingerprint")):
            raise ExternalActionStoreError("operation is denied by active policy", code="nomos_policy_denied")
        if allowed is not True or final_effect != "allow" or missing:
            raise ExternalActionStoreError(
                "strict Nomos runtime outcome did not authorize the lifecycle command"
            )
        assertion = {
            **dict(pending),
            "nomos_decision_id": command.get("decision_id"),
            "policy_fingerprint": command.get("policy_fingerprint"),
            "provider_id": authority.get("provider_id"),
            "provider_status": authority.get("provider_status"),
            "provider_updated_at": authority.get("provider_updated_at"),
            "binding_id": authority.get("binding_id"),
            "binding_status": authority.get("binding_status"),
            "binding_updated_at": authority.get("binding_updated_at"),
            "nomos_input_hash": command.get("input_hash"),
            "allowed": True,
            "final_effect": final_effect,
            "required_obligations_digest": self._verifier_digest(
                "okg-external-action-required-obligations-v1", required
            ),
            "completed_obligations_digest": self._verifier_digest(
                "okg-external-action-completed-obligations-v1", completed
            ),
            "missing_obligations_digest": self._verifier_digest(
                "okg-external-action-missing-obligations-v1", missing
            ),
        }
        tag = self._verifier_tag(assertion)
        command.update(
            {
                "verifier_assertion": assertion,
                "verifier_assertion_tag": tag,
                "allowed": True,
                "final_effect": final_effect,
                "required_obligations": required,
                "completed_obligations": completed,
                "missing_obligations": missing,
            }
        )
        self._pending_verifier_assertion = {
            "assertion": assertion,
            "tag": tag,
        }
        capability = self._verifier_capability
        if capability is None:
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            )
        self._set_verifier_capability(capability)
        value = self._typed(
            "external_action_nomos_persist_decision",
            command,
        )
        if isinstance(value, Mapping):
            return value.get("decision_id", value)
        return value

    def submit_command(self, command: SubmitCommand | Mapping[str, Any]) -> Any:
        request = command.to_mapping() if isinstance(command, SubmitCommand) else dict(command)
        return self._typed("external_action_submit", request)

    def review_transition_command(self, command: ReviewTransitionCommand | Mapping[str, Any]) -> Any:
        request = command.to_mapping() if isinstance(command, ReviewTransitionCommand) else dict(command)
        return self._typed("external_action_review_transition", request)

    def claim_command(self, command: ClaimCommand | Mapping[str, Any]) -> Any:
        request = command.to_mapping() if isinstance(command, ClaimCommand) else dict(command)
        value = self._typed("external_action_claim", request)
        _raise_closed_claim_refusal(value)
        return _unjsonable(value)

    def authorized_read(
        self,
        *,
        operation: str,
        request_hash: str,
        actor: Any = None,
        authorization: Any = None,
        include_payload: bool = False,
        resource: str | None = None,
        proposal_id: str | None = None,
        effect_instance_key: str | None = None,
        deployment: str | None = None,
        state: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Return one authorization-bound durable read projection.

        The caller supplies the hash already used by the verifier and Nomos
        decision.  PostgreSQL binds that exact digest to the retained audit
        pair before selecting any row.  Payload bytes are available only for
        the explicit ``read_payload`` operation.
        """

        deployment_value = deployment or self.deployment
        if not isinstance(deployment_value, str) or not deployment_value:
            raise ExternalActionStoreError("external-action read deployment unavailable")
        if self.deployment is not None and deployment_value != self.deployment:
            raise ExternalActionStoreError("external-action read deployment mismatch")
        if operation not in {
            "read",
            "read_payload",
            "list",
            "timeline",
            "recovery_record",
        }:
            raise ExternalActionStoreError("external-action read operation unavailable")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", request_hash or ""):
            raise ExternalActionStoreError("external-action read digest unavailable")
        refs = self._authorization_mapping(actor, authorization)
        request = {
            "deployment": deployment_value,
            "operation": operation,
            "request_hash": request_hash,
            "include_payload": bool(include_payload),
            "resource": resource,
            "proposal_id": proposal_id,
            "effect_instance_key": effect_instance_key,
            "state": state,
            "limit": limit,
            **refs,
        }
        value = _unjsonable(self._typed("external_action_authorized_read", request))
        if not isinstance(value, Mapping):
            raise ExternalActionStoreError("external-action read authority unavailable")
        return dict(value)

    def prepare_claim(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Predict the exact claim event under the active database transaction."""

        value = self._typed("external_action_prepare_claim", request)
        _raise_closed_claim_refusal(value)
        if not isinstance(value, Mapping):
            raise ExternalActionStoreError("claim preparation authority unavailable")
        return dict(value)

    def outcome_command(self, command: OutcomeCommand | Mapping[str, Any]) -> Any:
        request = command.to_mapping() if isinstance(command, OutcomeCommand) else dict(command)
        return self._typed("external_action_report_outcome", request)

    def owner_unavailability_command(self, command: OwnerUnavailabilityCommand | Mapping[str, Any]) -> Any:
        request = command.to_mapping() if isinstance(command, OwnerUnavailabilityCommand) else dict(command)
        return self._typed("external_action_owner_unavailability", request)

    def restore_create(self, request: Mapping[str, Any]) -> Any:
        return self._typed("external_action_restore_create", request)

    def restore_effect_inventory(self, *, deployment: str | None = None, restore_id: str) -> Any:
        if deployment is not None and self.deployment is not None and deployment != self.deployment:
            raise ExternalActionStoreError("restore inventory deployment mismatch")
        value = self._verifier_call_in_transaction(
            "external_action_restore_inventory",
            (restore_id,),
        )
        if isinstance(value, Mapping):
            return value
        raise ExternalActionStoreError("restore inventory authority unavailable")

    def restore_batch(self, request: Mapping[str, Any]) -> Any:
        value = self._typed("external_action_restore_batch", request)
        # The cumulative batch response is derived from the committed
        # staging projection, not from an untrusted caller count.  Refresh
        # while the surrounding service transaction is still active so the
        # next response observes all batches in this restore session.
        self._refresh_snapshot()
        return value

    def restore_prepare(self, request: Mapping[str, Any]) -> Any:
        return self._typed("external_action_restore_prepare", request)

    def restore_finalize(self, request: Mapping[str, Any]) -> Any:
        return self._typed("external_action_restore_finalize", request)

    def restore_session_context(self, restore_id: str) -> Mapping[str, Any] | None:
        """Read the lock-local immutable restore session projection.

        The PostgreSQL snapshot is populated by the surrounding store lock;
        this method performs no ad-hoc SQL and is only a compatibility read
        for service-side checkpoint binding.
        """

        value = self.restore_sessions.get(restore_id)
        if isinstance(value, Mapping):
            return value
        return None

    def startup_login_binding(self, *, deployment: str | None = None) -> dict[str, Any]:
        """Return the database-owned, session-login startup projection."""

        if deployment is not None and self.deployment is not None and deployment != self.deployment:
            raise ExternalActionStoreError("startup login binding deployment mismatch")
        value = self._call_in_transaction("external_action_startup_login_binding")
        if not isinstance(value, Mapping):
            raise ExternalActionStoreError("startup login binding authority unavailable")
        result = dict(value)
        if deployment is not None and result.get("deployment") != deployment:
            raise ExternalActionStoreError("startup login binding deployment mismatch")
        return result

    def verify_claim_owner_unavailable(
        self,
        *,
        deployment: str,
        execution_id: str,
        **_verified_context: Any,
    ) -> dict[str, Any]:
        """Read the database-owned identity revocation/replacement decision."""

        if self.deployment is not None and deployment != self.deployment:
            raise ExternalActionStoreError("claim owner identity deployment mismatch")
        if not isinstance(execution_id, str) or not execution_id:
            raise ExternalActionStoreError("claim owner execution identity unavailable")
        value = self._verifier_call_in_transaction(
            "external_action_claim_owner_unavailability",
            (execution_id,),
        )
        if not isinstance(value, Mapping):
            raise ExternalActionStoreError("claim owner identity authority unavailable")
        return dict(value)

    def materialize_expiry(self, proposal_id: str) -> Proposal | None:
        if not isinstance(proposal_id, str) or not proposal_id:
            raise ExternalActionStoreError("proposal id is unavailable")
        value = self._verifier_call_in_transaction(
            "external_action_materialize_expiry",
            (proposal_id,),
        )
        self._refresh_snapshot()
        hydrated = self.proposals.get(proposal_id)
        if isinstance(hydrated, Proposal):
            return hydrated
        if isinstance(value, Mapping):
            candidate = value.get("proposal") or value.get("value")
            if isinstance(candidate, Mapping):
                self._load_map(self.proposals, {"key": proposal_id, "value": candidate}, Proposal)
                return self.proposals.get(proposal_id)
        return None

    def _refresh_snapshot(self) -> None:
        capability = self._verifier_capability
        if capability is None:
            raise ExternalActionStoreError(
                "external-action verifier capability is unavailable"
            )
        # The restart snapshot contains internal hydration records, including
        # payload bytes needed by later typed transitions.  Requiring the
        # server-held capability makes a mapped database login insufficient
        # to call this surface directly.
        self._set_verifier_capability(capability)
        snapshot = self._json_call("external_action_store_snapshot")
        if not isinstance(snapshot, Mapping):
            raise ExternalActionStoreError("external-action snapshot authority unavailable")
        snapshot = _unjsonable(dict(snapshot))
        required = {
            "execution_epoch", "execution_epoch_lease_digest",
            "proposals", "payload_versions", "decisions", "executions", "outcomes",
            "events", "transition_idempotency", "attestations",
            "contracts", "leases", "restore_fences", "auth_audits",
            "restore_attestations", "restore_sessions", "restore_checkpoint_staging",
            "restore_recoveries", "dpop_replay_nonces",
        }
        if not required.issubset(snapshot):
            missing = ", ".join(sorted(required - set(snapshot)))
            raise ExternalActionStoreError("external-action snapshot is incomplete; missing " + missing)
        self.execution_epoch = int(snapshot.get("execution_epoch", self.execution_epoch or 0))
        self.execution_epoch_lease_digest = snapshot.get("execution_epoch_lease_digest", self.execution_epoch_lease_digest)
        self.policy_digest = snapshot.get("policy_digest")
        # Snapshot rows are the only source used to rebuild process state on a
        # restart.  The function may omit unrelated/deployment-hidden rows.
        self._load_map(self.proposals, snapshot.get("proposals"), Proposal)
        self._load_bucket(self.payload_versions, snapshot.get("payload_versions"), PayloadVersion)
        self._load_bucket(self.decisions, snapshot.get("decisions"), Decision)
        self._load_map(self.executions, snapshot.get("executions"), ExecutionClaim)
        self._load_map(self.outcomes, snapshot.get("outcomes"), Outcome)
        self._load_bucket(self.events, snapshot.get("events"), AuditEvent)
        self._load_map(self.auth_audits, snapshot.get("auth_audits"), None)
        self._load_map(self.transition_idempotency, snapshot.get("transition_idempotency"), TransitionRecord)
        self._load_map(self.submit_idempotency, snapshot.get("submit_idempotency"), None)
        self._load_map(self.attestations, snapshot.get("attestations"), None)
        self._load_map(self.contracts, snapshot.get("contracts"), None)
        self._load_bucket(self.leases, snapshot.get("leases"), None)
        self._hydrate_leases()
        self._load_map(self.restore_fences, snapshot.get("restore_fences"), None)
        self._load_map(self.restore_attestations, snapshot.get("restore_attestations"), None)
        self._load_map(self.restore_sessions, snapshot.get("restore_sessions"), None)
        self._load_map(self.restore_checkpoint_staging, snapshot.get("restore_checkpoint_staging"), None)
        self._load_map(self.restore_checkpoint_entries, snapshot.get("restore_checkpoint_entries"), None)
        self._load_map(self.restore_recoveries, snapshot.get("restore_recoveries"), None)
        self._load_map(self.dpop_replay_nonces, snapshot.get("dpop_replay_nonces"), None)
        if self.policy_digest is None:
            # Older fixed snapshots did not expose a top-level digest.  The
            # command RPC stores the digest on each proposal/decision, so
            # recover it from the latest durable projection without inventing
            # a default.  A deployment with mixed policy digests remains
            # unavailable rather than choosing one silently.
            digests = {
                value.policy_digest
                for value in self.proposals.values()
                if isinstance(value, Proposal) and value.policy_digest
            }
            digests.update(
                value.policy_digest
                for rows in self.decisions.values()
                for value in rows
                if isinstance(value, Decision) and value.policy_digest
            )
            if len(digests) == 1:
                self.policy_digest = next(iter(digests))
            elif len(digests) > 1:
                raise ExternalActionStoreError(
                    "external-action snapshot contains mixed policy digests"
                )
        if "submit_idempotency" not in snapshot:
            # The submit idempotency table is represented by the unique
            # proposal key in the current SQL schema.  Rebuild the read cache
            # from hydrated proposals; no database mutation is performed.
            self.submit_idempotency._data.clear()
            for proposal in self.proposals.values():
                if (
                    isinstance(proposal, Proposal)
                    and proposal.submission_idempotency_key_hash
                ):
                    self.submit_idempotency._load(
                        (
                            proposal.deployment,
                            proposal.proposer_separation_subject,
                            proposal.submission_idempotency_key_hash,
                        ),
                        (proposal.proposal_id, proposal.proposal_request_hash),
                    )

    def _hydrate_leases(self) -> None:
        """Restore verified lease records needed by the pre-bind fence."""

        from .restore_fence import VerifiedLease

        for rows in self.leases.values():
            for index, value in enumerate(rows._data):
                if isinstance(value, VerifiedLease) or not isinstance(value, Mapping):
                    continue
                envelope = value.get("envelope")
                payload = value.get("payload")
                digest = value.get("envelope_digest")
                signer_key_id_digest = value.get("signer_key_id_digest")
                if envelope is None and payload is None:
                    envelope_fields = ("protected_b64", "payload_b64", "signature_b64")
                    payload_fields = (
                        "deployment", "epoch", "lease_sequence", "previous_lease_digest",
                        "previous_epoch_digest", "issued_at", "expires_at",
                        "signer_separation_subject", "nonce",
                    )
                    if all(isinstance(value.get(field), str) and value.get(field) for field in envelope_fields) and all(field in value for field in payload_fields) and isinstance(digest, str) and digest:
                        envelope = {field: value[field] for field in envelope_fields} | {"envelope_digest": digest}
                        payload = {field: value[field] for field in payload_fields}
                if (
                    isinstance(envelope, Mapping)
                    and isinstance(payload, Mapping)
                    and isinstance(digest, str)
                    and isinstance(signer_key_id_digest, str)
                ):
                    rows._data[index] = VerifiedLease(
                        dict(envelope),
                        dict(payload),
                        digest,
                        signer_key_id_digest,
                    )

    @staticmethod
    def _record_mapping(value: Any) -> Mapping[str, Any] | None:
        if isinstance(value, Mapping):
            return value
        return None

    @staticmethod
    def _snapshot_key(value: Any) -> Any:
        if isinstance(value, list):
            return tuple(PostgresExternalActionStore._snapshot_key(item) for item in value)
        if isinstance(value, Mapping):
            return tuple(sorted((str(key), PostgresExternalActionStore._snapshot_key(child)) for key, child in value.items()))
        return value

    def _load_map(self, target: _DurableDict, rows: Any, cls: Any) -> None:
        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise ExternalActionStoreError("external-action snapshot is incomplete")
        target._data.clear()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            key, value = self._snapshot_key(row.get("key")), row.get("value")
            if value is None and "store_record" in row:
                value = row.get("store_record")
            if key is None:
                key = row.get("proposal_id", row.get("execution_id", row.get("deployment")))
            if cls is not None and isinstance(value, Mapping):
                try:
                    value = cls(**self._coerce_record(value, cls))
                except (TypeError, ValueError) as exc:
                    raise ExternalActionStoreError(
                        "external-action snapshot contains an invalid "
                        f"{cls.__name__} record"
                    ) from exc
            if key is not None:
                target._load(key, value)

    def _load_bucket(self, target: _DurableBuckets, rows: Any, cls: Any) -> None:
        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise ExternalActionStoreError("external-action snapshot is incomplete")
        target._data.clear()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            bucket, value = self._snapshot_key(row.get("bucket")), row.get("value")
            if value is None and "store_record" in row:
                value = row.get("store_record")
            if bucket is None:
                bucket = row.get("proposal_id", row.get("effect_instance_key"))
            if cls is not None and isinstance(value, Mapping):
                try:
                    value = cls(**self._coerce_record(value, cls))
                except (TypeError, ValueError) as exc:
                    raise ExternalActionStoreError(
                        "external-action snapshot contains an invalid "
                        f"{cls.__name__} record"
                    ) from exc
            if bucket is not None:
                target._load(bucket, value)

    @staticmethod
    def _coerce_record(value: Mapping[str, Any], cls: Any = None) -> dict[str, Any]:
        result = dict(value)
        # A store_record is preferred by SQL because it preserves the exact
        # domain shape.  When a deployment returns normalized columns, accept
        # only known aliases and discard database-only columns below.
        if isinstance(result.get("store_record"), Mapping):
            result = {**dict(result["store_record"]), **{k: v for k, v in result.items() if k != "store_record"}}
        if cls is PayloadVersion:
            for alias in ("current_payload_hash", "replacement_payload_hash"):
                if alias in result and (
                    not isinstance(result[alias], str) or not result[alias]
                ):
                    raise ExternalActionStoreError("invalid payload-version command projection")
            if "replacement_payload_hash" in result:
                if result.get("operation") not in ("edit", "approve_with_edit"):
                    raise ExternalActionStoreError("invalid payload-version command projection")
                selected_hash = result["replacement_payload_hash"]
            elif "current_payload_hash" in result:
                if "operation" in result and result["operation"] != "submit":
                    raise ExternalActionStoreError("invalid payload-version command projection")
                selected_hash = result["current_payload_hash"]
            else:
                selected_hash = None
            if selected_hash is not None:
                if "payload_hash" in result and result["payload_hash"] != selected_hash:
                    raise ExternalActionStoreError("contradictory payload-version hash projection")
                result.setdefault("payload_hash", selected_hash)
        aliases = {
            "submission_request_hash": "proposal_request_hash",
            "submission_idempotency_hash": "submission_idempotency_key_hash",
            "payload_version": "version",
            "difference_field_codes": "difference",
            "authentication_audit_ref": "authentication_audit_id",
            "nomos_decision_ref": "nomos_decision_id",
            "provider_idempotency_key_digest": "provider_idempotency_key",
            "request_hash": "outcome_request_hash",
            "recorded_at": "created_at",
            # The semantic transition and claim commands use role-specific
            # names, while the domain projections retain their own names.
            "actor_principal": "approver_principal",
            "actor_separation_subject": "approver_separation_subject",
            "worker_separation_subject": "worker_separation_subject",
            "approved_lifecycle_version": "lifecycle_version",
            "approved_payload_version": "payload_version",
            "approved_payload_hash": "payload_hash",
        }
        for source, destination in aliases.items():
            if cls is PayloadVersion and source == "approved_payload_hash":
                continue
            if source in result and destination not in result:
                result[destination] = result[source]
        if cls is Proposal and "proposer_principal" not in result:
            result["proposer_principal"] = result.get("proposer_separation_subject")
        if cls is Decision and result.get("verdict") == "approved":
            result["verdict"] = "approve"
        if cls is Decision and result.get("verdict") == "rejected":
            result["verdict"] = "reject"
        if cls is Decision and "verdict" not in result:
            operation = result.get("operation")
            if operation in {"approve", "approve_with_edit"}:
                result["verdict"] = "approve"
            elif operation == "reject":
                result["verdict"] = "reject"
        for field in ("created_at", "updated_at", "requested_review_expires_at", "effective_review_expires_at", "execute_before", "claimed_at", "recorded_at", "observed_at", "effective_at", "credential_expires_at"):
            if field in result:
                result[field] = _as_datetime(result[field])
        if "evidence_digests" in result and isinstance(result["evidence_digests"], list):
            result["evidence_digests"] = tuple(result["evidence_digests"])
        if "difference" in result and isinstance(result["difference"], list):
            result["difference"] = tuple(result["difference"])
        if "canonical_payload" in result and isinstance(result["canonical_payload"], str):
            result["canonical_payload"] = result["canonical_payload"].encode("utf-8")
        if cls is TransitionRecord:
            # The SQL idempotency row retains a command plus a safe response
            # projection.  Rebuild the domain record from those values; the
            # response body is intentionally not required for replay reads.
            command = result.get("command")
            response_projection = result.get("response_projection")
            if isinstance(command, Mapping):
                result = {**dict(command), **result}
                result.setdefault("operation", command.get("operation"))
                result.setdefault("actor_separation_subject", command.get("actor_separation_subject"))
                result.setdefault("idempotency_key_hash", command.get("idempotency_key_hash"))
                result.setdefault("transition_request_hash", command.get("transition_request_hash"))
            if isinstance(response_projection, Mapping):
                response_body = result.get("response_body_jcs", b"") or b""
                if isinstance(response_body, str):
                    response_body = response_body.encode("utf-8")
                if not isinstance(response_body, bytes):
                    raise ExternalActionStoreError(
                        "external-action idempotency response bytes are invalid"
                    )
                result["result"] = ServiceResult(
                    value=dict(response_projection),
                    status=int(response_projection.get("response_status", result.get("response_status", 200))),
                    replay=bool(response_projection.get("replay", False)),
                    response_body=response_body,
                )
            elif isinstance(result.get("result"), Mapping):
                result["result"] = ServiceResult(**result["result"])
            elif "response_status" in result:
                result["result"] = ServiceResult(
                    value={},
                    status=int(result.get("response_status", 200)),
                    response_body=result.get("response_body_jcs", b"") or b"",
                )
        if cls is not None:
            allowed = {field.name for field in fields(cls)}
            result = {key: value for key, value in result.items() if key in allowed}
        return result

    def now(self) -> datetime:
        if self._active_transaction:
            rows = self._query("SELECT clock_timestamp()")
        else:
            with _StoreTransaction(self):
                rows = self._query("SELECT clock_timestamp()")
        if not rows or not isinstance(rows[0][0], datetime):
            raise ExternalActionStoreError("database clock authority unavailable")
        return rows[0][0].astimezone(timezone.utc)

    def set_now(self, value: datetime) -> None:
        raise ExternalActionStoreError("production store clock is PostgreSQL-owned")

    def advance(self, **kwargs: Any) -> datetime:
        raise ExternalActionStoreError("production store clock is PostgreSQL-owned")

    def next_id(self) -> str:
        return new_ulid()

    def get(self, proposal_id: str) -> Proposal | None:
        return self.proposals.get(proposal_id)

    def get_payload(self, proposal_id: str, version: int | None = None) -> PayloadVersion | None:
        versions = self.payload_versions.get(proposal_id)
        if not versions:
            return None
        if version is None:
            return versions[-1] if len(versions) else None
        return next((item for item in versions if item.version == version), None)

    def get_decision(self, proposal_id: str, decision_id: str | None = None) -> Decision | None:
        rows = self.decisions.get(proposal_id)
        if not rows:
            return None
        if decision_id is None:
            return rows[-1] if len(rows) else None
        return next((item for item in rows if item.decision_id == decision_id), None)

    def get_execution(self, proposal_id: str) -> ExecutionClaim | None:
        value = self.executions.get(proposal_id)
        if isinstance(value, ExecutionClaim):
            return value
        return next(
            (item for item in self.executions.values()
             if isinstance(item, ExecutionClaim) and item.proposal_id == proposal_id),
            None,
        )

    def get_outcome(self, execution_id: str) -> Outcome | None:
        value = self.outcomes.get(execution_id)
        if isinstance(value, Outcome):
            return value
        return next(
            (item for item in self.outcomes.values()
             if isinstance(item, Outcome) and item.execution_id == execution_id),
            None,
        )

    def get_events(self, effect_key: str) -> tuple[AuditEvent, ...]:
        rows = self.events.get(effect_key)
        return tuple(rows or ())

    def all_proposals(self, *, deployment: str | None = None) -> tuple[Proposal, ...]:
        values = tuple(value for value in self.proposals.values() if isinstance(value, Proposal))
        if deployment is not None:
            values = tuple(value for value in values if value.deployment == deployment)
        return values

    def counts(self) -> dict[str, int]:
        return {
            "proposals": len(self.proposals),
            "payload_versions": sum(len(items) for items in self.payload_versions.values()),
            "decisions": sum(len(items) for items in self.decisions.values()),
            "executions": len(self.executions),
            "outcomes": len(self.outcomes),
            "events": sum(len(items) for items in self.events.values()),
            "transition_idempotency": len(self.transition_idempotency),
        }

    def transaction(self) -> _StoreTransaction:
        return _StoreTransaction(self)

    def retained_leases(self, *, deployment: str | None = None) -> tuple[Any, ...]:
        values = tuple(item for rows in self.leases.values() for item in rows)
        if deployment is not None:
            values = tuple(item for item in values if getattr(item, "payload", {}).get("deployment") == deployment)
        return values

    def insert_lease(self, *, lease: Any, **metadata: Any) -> Any:
        deployment = getattr(lease, "payload", {}).get("deployment") or getattr(lease, "deployment", None)
        if not deployment:
            raise ExternalActionStoreError("lease has no deployment")
        existing = next((item for item in self.leases[deployment] if getattr(item, "envelope_digest", None) == getattr(lease, "envelope_digest", None)), None)
        if existing is not None:
            return existing
        payload = getattr(lease, "payload", None)
        if not isinstance(payload, Mapping):
            raise ExternalActionStoreError("lease payload is unavailable")
        request = dict(payload)
        envelope = getattr(lease, "envelope", None)
        if not isinstance(envelope, Mapping):
            raise ExternalActionStoreError("lease envelope is unavailable")
        for field in ("protected_b64", "payload_b64", "signature_b64"):
            value = envelope.get(field)
            if not isinstance(value, str) or not value:
                raise ExternalActionStoreError("lease envelope is incomplete")
            request[field] = value
        # The verifier owns the signer-key identifier digest.  A detached JWS
        # protected header names the key by ``kid`` but does not carry the
        # retained digest column, so accept the verified value only from the
        # lease metadata/envelope and fail closed when it is absent.  Never
        # substitute a locally derived or legacy hash here.
        signer_digests = [
            getattr(lease, "signer_key_id_digest", None),
            envelope.get("signer_key_id_digest"),
            metadata.get("signer_key_id_digest"),
        ]
        signer_digests = [value for value in signer_digests if value is not None]
        if not signer_digests or any(not isinstance(value, str) or not value for value in signer_digests):
            raise ExternalActionStoreError("verified lease signer key id digest is unavailable")
        if len(set(signer_digests)) != 1:
            raise ExternalActionStoreError("verified lease signer key id digest is inconsistent")
        request["signer_key_id_digest"] = signer_digests[0]
        request.update({key: value for key, value in metadata.items() if value is not None})
        request.setdefault("deployment", deployment)
        request.setdefault("envelope_digest", getattr(lease, "envelope_digest", None))
        value = self._typed("external_action_store_insert_lease", request)
        self.leases[deployment]._load(lease)
        if isinstance(value, Mapping) and value.get("replay") is True:
            return existing or lease
        return lease

    def restore_fence(self, *, deployment: str | None = None) -> dict[str, Any] | None:
        value = self.restore_fences.get(deployment or "")
        return value if isinstance(value, dict) else None

    def set_restore_fence(self, *, deployment: str, **fields: Any) -> None:
        self.startup_command({"deployment": deployment, **fields})
        self.restore_fences._load(deployment, dict(fields))

    def resolve_principal(self, issuer: str, subject: str) -> Mapping[str, Any]:
        value = self._verifier_call_in_transaction(
            "external_action_store_resolve_principal", (issuer, subject)
        )
        if not isinstance(value, Mapping):
            raise ExternalActionStoreError("deployment-pinned principal mapping unavailable")
        result = dict(value)
        # PostgreSQL JSON timestamptz spelling is not the verifier's wire
        # spelling. Preserve the SQL-owned instants; never invent a timezone
        # or relax the authentication parser for caller-supplied timestamps.
        for field in ("provider_updated_at", "binding_updated_at"):
            raw = result.get(field)
            if raw is None:
                continue
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00")) if isinstance(raw, str) else raw
                if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise ValueError("unmeasured timestamp")
                result[field] = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            except (ValueError, TypeError, OverflowError) as exc:
                raise ExternalActionStoreError("deployment-pinned identity timestamp unavailable") from exc
        return result

    def insert_nonce(self, **kwargs: Any) -> bool:
        values = tuple(kwargs[name] for name in ("deployment", "issuer_digest", "sender_key_thumbprint", "jti_digest", "request_method", "request_uri_digest", "access_token_hash", "observed_at", "expires_at"))
        value = self._verifier_call_in_transaction(
            "external_action_insert_dpop_replay_nonce", values
        )
        return value is True or value == 1


class PostgresNomosStorageAdapter:
    """Same-connection Nomos storage bridge used by external actions."""

    def __init__(self, store: PostgresExternalActionStore) -> None:
        self.store = store

    def bind_identity(self, *, provider_id: str, subject_binding_id: str) -> None:
        self.store.bind_nomos_identity(
            provider_id=provider_id,
            subject_binding_id=subject_binding_id,
        )

    def load_active_policy_bundle(
        self,
        conn: Any,
        *,
        deployment_name: str,
        scope: str,
    ) -> Any:
        # ``conn`` is intentionally accepted to satisfy the existing Nomos
        # adapter protocol.  The store verifies that it is the connection
        # owning the current transaction; it never opens a second connection.
        if conn is not None and conn is not self.store.connection:
            raise ExternalActionStoreError("Nomos adapter connection mismatch")
        return self.store.load_active_policy_bundle(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        )

    def persist_nomos_decision(
        self,
        conn: Any,
        *,
        deployment_name: str,
        decision: Any,
        target: Mapping[str, Any],
    ) -> Any:
        if conn is not None and conn is not self.store.connection:
            raise ExternalActionStoreError("Nomos adapter connection mismatch")
        return self.store.persist_nomos_decision(
            conn,
            deployment_name=deployment_name,
            decision=decision,
            target=target,
        )

    def persist_nomos_runtime_outcome(
        self,
        conn: Any,
        *,
        deployment_name: str,
        decision: Any,
        outcome: Any,
        target: Mapping[str, Any],
    ) -> Any:
        if conn is not None and conn is not self.store.connection:
            raise ExternalActionStoreError("Nomos adapter connection mismatch")
        return self.store.persist_nomos_runtime_outcome(
            conn,
            deployment_name=deployment_name,
            decision=decision,
            outcome=outcome,
            target=target,
        )


class PostgresReplayNonceStore:
    """Cross-process DPoP replay fence backed by a unique SQL insert."""

    def __init__(self, dsn: str | None = None, *, connection: Any = None, connection_factory: Callable[..., Any] | None = None, check_contract: bool = True) -> None:
        if connection is None:
            if not dsn:
                raise ValueError("a PostgreSQL DSN is required")
            if connection_factory is None:
                # Same factory, same reason, as PostgresExternalActionStore
                # above (issue #1968).
                from okg.substrate.db.connection import connect as _okg_connect

                connection_factory = _okg_connect
            connection = connection_factory(dsn)
        self._conn = connection
        if check_contract:
            try:
                with UnitOfWork(connection, label="DPoP replay store contract check"):
                    self._check_contract()
            except NestedTransactionRefused as exc:
                raise ExternalActionStoreError(str(exc)) from exc

    def _check_contract(self) -> None:
        names = [name for name, _ in REQUIRED_SECURITY_DEFINER_FUNCTIONS if name == "external_action_insert_dpop_replay_nonce"] + [name for name, _ in _DEPLOYMENT_FUNCTIONS]
        rows = []
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    "SELECT p.proname, pg_catalog.oidvectortypes(p.proargtypes), p.prosecdef, pg_get_userbyid(p.proowner), p.proconfig FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'okg' AND p.proname = ANY(%s)",
                    (names,),
                )
                rows = list(cursor.fetchall())
        except Exception as exc:
            raise ExternalActionStoreError("DPoP replay authority unavailable") from exc
        found = {(str(row[0]), str(row[1])) for row in rows}
        expected = [(name, args) for name, args in REQUIRED_SECURITY_DEFINER_FUNCTIONS if name == "external_action_insert_dpop_replay_nonce"] + list(_DEPLOYMENT_FUNCTIONS)
        missing = [_function_label(name, args) for name, args in expected if (name, args) not in found]
        insecure: list[str] = []
        for row in rows:
            label = _function_label(str(row[0]), str(row[1]))
            if row[2] is not True:
                insecure.append(label + " is not SECURITY DEFINER")
                continue
            owner = row[3]
            try:
                with self._conn.cursor() as cursor:
                    cursor.execute("SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = %s", (owner,))
                    owner_row = cursor.fetchone()
            except Exception as exc:
                raise ExternalActionStoreError("DPoP replay authority unavailable") from exc
            if not owner_row or owner_row[0] is not False:
                insecure.append(label + " is not owned by a NOLOGIN role")
            config = row[4] or ()
            if config and not any(str(item).replace(" ", "") == "search_path=pg_catalog,okg" for item in config):
                insecure.append(label + " has an unpinned search_path")
        if missing or insecure:
            details = ("missing fixed SECURITY DEFINER entry points: " + ", ".join(missing)) if missing else ""
            if insecure:
                details += ("; " if details else "") + "insecure entry points: " + ", ".join(insecure)
            raise ExternalActionStoreError("DPoP replay PostgreSQL contract unavailable; " + details + "; raw-table fallback is forbidden")

    def insert_nonce(self, **kwargs: Any) -> bool:
        names = ("deployment", "issuer_digest", "sender_key_thumbprint", "jti_digest", "request_method", "request_uri_digest", "access_token_hash", "observed_at", "expires_at")
        values = tuple(kwargs[name] for name in names)
        try:
            with self._conn.transaction():
                with self._conn.cursor() as cursor:
                    cursor.execute("SELECT okg.external_action_insert_dpop_replay_nonce(%s,%s,%s,%s,%s,%s,%s,%s,%s)", values)
                    row = cursor.fetchone()
            return bool(row and (row[0] is True or row[0] == 1))
        except Exception as exc:
            # A unique conflict is a replay, not an authority outage. The
            # fixed function returns false for that case; all other failures
            # remain safe authentication failures at the caller.
            raise ExternalActionStoreError("DPoP replay authority unavailable") from exc


__all__ = [
    "ExternalActionStore",
    "ExternalActionStoreError",
    "InMemoryExternalActionStore",
    "PostgresExternalActionStore",
    "PostgresNomosStorageAdapter",
    "PostgresReplayNonceStore",
    "REQUIRED_SECURITY_DEFINER_FUNCTIONS",
    "TransitionRecord",
    "new_ulid",
]
