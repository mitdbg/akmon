"""Versioned, streamed graph-output contract for Producer-authorized facts.

``FactBatch`` is a logical protocol, not an eagerly materialized container. A
caller constructs one immutable header, feeds bounded chunks through
``FactBatchValidator``, and finishes with a terminal summary. Durable
``ApplicationReceipt`` rows bridge Chronos graph application and the separate
progress-marker / optional audit-fact transaction.

This module owns protocol shape and integrity. Domain invocation remains in
``SourceAdapter``, Record, Poros, enrichers, lenses, and system workflows.
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from psycopg.types.json import Jsonb

from okg.substrate.admission import FactRangeRef
from okg.substrate.library.sources.base import EdgeFact, NodeFact, ProgressMarker
from okg.substrate.producer_authority import (
    ProducerProfile,
    evaluate_payload_signature,
)


FACT_BATCH_SCHEMA_VERSION = 1
FACT_BATCH_COMPLETIONS = frozenset({"snapshot", "delta", "partial"})
APPLICATION_RECEIPT_STATES = frozenset(
    {
        "prepared",
        "applied",
        "compensating",
        "compensated",
    }
)
MAX_FACTS_PER_CHUNK = 10_000
MAX_MARKERS_PER_CHUNK = 10_000

_RECEIPT_TRANSITIONS = {
    "prepared": frozenset({"applied"}),
    "applied": frozenset({"compensating"}),
    "compensating": frozenset({"compensated"}),
    "compensated": frozenset(),
}
# Key names ``_validate_safe_json`` refuses at any nesting depth. Two sets,
# because the same scanner guards two different surfaces:
#
#  * ``_FORBIDDEN_CONTROL_KEYS`` guards the AUTHORITY/CONTROL surfaces -- a
#    header's ``requested_scope`` and a summary's ``observed_scope`` /
#    ``source_delta``. Authority must come from the server-resolved profile and
#    source policy, "never self-granted batch fields", so control-plane names
#    (command(s), client(s), publication_handle(s)) stay rejected there
#    alongside secrets.
#
#  * ``_FORBIDDEN_FACT_DATA_KEYS`` guards persisted fact DATA -- a
#    NodeFact/EdgeFact's ``attrs``, ``source_record_id``, and
#    ``source_revision``. Here only secret VALUES and capability HANDLES are
#    forbidden: persisting one would be a credential leak or authority smuggle.
#    The control-plane STRUCTURAL names are deliberately ABSENT -- they are
#    legitimate graph data (a node routinely records the CI/agent/verification
#    ``command`` it observed or an API ``client`` name). Listing them here made
#    the recursive scan reject that legitimate data at any depth and hard-block
#    every publish for a deployment whose facts carry a ``command`` (#822).
#
# A live client/connection object smuggled into either surface is still
# rejected regardless of key name by the callable / non-serializable checks in
# ``_validate_safe_json``. This scan matches key NAMES only, not values, so
# sources remain responsible for redacting secret values under innocuous keys.
_FORBIDDEN_CONTROL_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "client",
        "clients",
        "command",
        "commands",
        "credential",
        "credentials",
        "oauth_token",
        "password",
        "publication_handle",
        "publication_handles",
        "refresh_token",
        "secret",
        "secret_ref",
        "secrets",
    }
)
_FORBIDDEN_FACT_DATA_KEYS = _FORBIDDEN_CONTROL_KEYS - frozenset(
    {"client", "clients", "command", "commands"}
)
_CHAIN_SEED = hashlib.sha256(b"okg.fact-batch.digest-chain.v1").digest()


class FactBatchError(ValueError):
    """Base error for a malformed or unauthorized FactBatch value."""


class FactBatchVersionError(FactBatchError):
    """Raised when a header uses an unsupported protocol version."""


class FactBatchIntegrityError(FactBatchError):
    """Raised when identities, ordinals, or digests do not agree."""


class FactBatchAuthorityError(FactBatchError):
    """Raised when caller content attempts to exceed resolved authority."""


class FactBatchFieldError(FactBatchError):
    """Raised for unknown, secret-bearing, or non-serializable fields."""


class ApplicationReceiptConflict(FactBatchIntegrityError):
    """Raised when a receipt key is replayed with different immutable input."""


class ApplicationReceiptTransitionError(FactBatchIntegrityError):
    """Raised for an invalid durable receipt state transition."""


@dataclass(frozen=True)
class FactBatchHeader:
    schema_version: int
    run_id: uuid.UUID
    admission_id: uuid.UUID
    producer_profile_fingerprint: str
    deployment: str
    target_branch: str
    catalog_version_id: int
    intent: str
    idempotency_key: str
    requested_scope: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.schema_version != FACT_BATCH_SCHEMA_VERSION:
            raise FactBatchVersionError(
                "unsupported FactBatch schema_version "
                f"{self.schema_version!r}; expected {FACT_BATCH_SCHEMA_VERSION}"
            )
        object.__setattr__(self, "run_id", _coerce_uuid(self.run_id, "run_id"))
        object.__setattr__(
            self,
            "admission_id",
            _coerce_uuid(self.admission_id, "admission_id"),
        )
        for name in (
            "producer_profile_fingerprint",
            "deployment",
            "target_branch",
            "intent",
            "idempotency_key",
        ):
            _require_nonempty(getattr(self, name), name)
        if (
            isinstance(self.catalog_version_id, bool)
            or not isinstance(self.catalog_version_id, int)
            or self.catalog_version_id < 0
        ):
            raise FactBatchFieldError(
                "catalog_version_id must be a non-negative integer"
            )
        if self.requested_scope is not None:
            _validate_safe_json(self.requested_scope, "requested_scope")
            object.__setattr__(
                self,
                "requested_scope",
                _freeze_json(self.requested_scope),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": str(self.run_id),
            "admission_id": str(self.admission_id),
            "producer_profile_fingerprint": self.producer_profile_fingerprint,
            "deployment": self.deployment,
            "target_branch": self.target_branch,
            "catalog_version_id": self.catalog_version_id,
            "intent": self.intent,
            "idempotency_key": self.idempotency_key,
            "requested_scope": _thaw_json(self.requested_scope),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FactBatchHeader":
        fields = {
            "schema_version",
            "run_id",
            "admission_id",
            "producer_profile_fingerprint",
            "deployment",
            "target_branch",
            "catalog_version_id",
            "intent",
            "idempotency_key",
            "requested_scope",
        }
        _require_mapping_fields(value, fields, fields - {"requested_scope"})
        return cls(**{name: value.get(name) for name in fields})


@dataclass(frozen=True)
class FactChunk:
    run_id: uuid.UUID
    ordinal: int
    facts: tuple[NodeFact | EdgeFact, ...]
    progress_markers: tuple[ProgressMarker, ...]
    digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _coerce_uuid(self.run_id, "run_id"))
        if (
            isinstance(self.ordinal, bool)
            or not isinstance(self.ordinal, int)
            or self.ordinal < 0
        ):
            raise FactBatchFieldError("chunk ordinal must be non-negative")
        facts = tuple(_freeze_fact(fact) for fact in self.facts)
        markers = tuple(_freeze_marker(marker) for marker in self.progress_markers)
        if len(facts) > MAX_FACTS_PER_CHUNK:
            raise FactBatchFieldError(
                f"chunk exceeds MAX_FACTS_PER_CHUNK={MAX_FACTS_PER_CHUNK}"
            )
        if len(markers) > MAX_MARKERS_PER_CHUNK:
            raise FactBatchFieldError(
                f"chunk exceeds MAX_MARKERS_PER_CHUNK={MAX_MARKERS_PER_CHUNK}"
            )
        object.__setattr__(self, "facts", facts)
        object.__setattr__(self, "progress_markers", markers)
        expected = canonical_chunk_digest(
            run_id=self.run_id,
            ordinal=self.ordinal,
            facts=facts,
            progress_markers=markers,
        )
        if self.digest != expected:
            raise FactBatchIntegrityError(
                f"chunk digest mismatch for ordinal {self.ordinal}: "
                f"expected {expected}, got {self.digest!r}"
            )

    @classmethod
    def create(
        cls,
        *,
        run_id: uuid.UUID | str,
        ordinal: int,
        facts: Iterable[NodeFact | EdgeFact] = (),
        progress_markers: Iterable[ProgressMarker] = (),
    ) -> "FactChunk":
        run_uuid = _coerce_uuid(run_id, "run_id")
        # Order matters and is the order this factory ALREADY exhibited: it
        # froze the facts before calling the constructor, so a bad fact or
        # marker was reported ahead of a bad ordinal or an oversized chunk.
        # Checking the ordinal first here would change which refusal a caller
        # sees when a chunk is wrong in two ways at once.
        fact_tuple = tuple(_freeze_fact(fact) for fact in facts)
        marker_tuple = tuple(_freeze_marker(marker) for marker in progress_markers)
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
            raise FactBatchFieldError("chunk ordinal must be non-negative")
        if len(fact_tuple) > MAX_FACTS_PER_CHUNK:
            raise FactBatchFieldError(
                f"chunk exceeds MAX_FACTS_PER_CHUNK={MAX_FACTS_PER_CHUNK}"
            )
        if len(marker_tuple) > MAX_MARKERS_PER_CHUNK:
            raise FactBatchFieldError(
                f"chunk exceeds MAX_MARKERS_PER_CHUNK={MAX_MARKERS_PER_CHUNK}"
            )
        digest = canonical_chunk_digest(
            run_id=run_uuid,
            ordinal=ordinal,
            facts=fact_tuple,
            progress_markers=marker_tuple,
        )
        # ``__post_init__`` exists to validate a CALLER-supplied digest, so it
        # re-freezes every fact and recomputes the digest it was just handed.
        # This factory produced that digest from the content it just froze and
        # has now repeated every field, size, and ordinal check above, so
        # routing through the constructor would run the whole normalize+hash
        # pass a second time. Populate the frozen instance directly instead.
        # ``FactChunk`` is a plain frozen dataclass with no ``__slots__`` and
        # no other ``__post_init__`` effect, and all five fields are set, so
        # equality, hashing, ``replace``, and pickling are unchanged.
        chunk = object.__new__(cls)
        object.__setattr__(chunk, "run_id", run_uuid)
        object.__setattr__(chunk, "ordinal", ordinal)
        object.__setattr__(chunk, "facts", fact_tuple)
        object.__setattr__(chunk, "progress_markers", marker_tuple)
        object.__setattr__(chunk, "digest", digest)
        return chunk

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "ordinal": self.ordinal,
            "facts": [_fact_to_dict(fact) for fact in self.facts],
            "progress_markers": [
                _marker_to_dict(marker) for marker in self.progress_markers
            ],
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FactChunk":
        fields = {"run_id", "ordinal", "facts", "progress_markers", "digest"}
        _require_mapping_fields(value, fields, fields)
        raw_facts = value["facts"]
        raw_markers = value["progress_markers"]
        if not isinstance(raw_facts, Sequence) or isinstance(raw_facts, str):
            raise FactBatchFieldError("facts must be a sequence")
        if not isinstance(raw_markers, Sequence) or isinstance(raw_markers, str):
            raise FactBatchFieldError("progress_markers must be a sequence")
        return cls(
            run_id=value["run_id"],
            ordinal=value["ordinal"],
            facts=tuple(_fact_from_dict(item) for item in raw_facts),
            progress_markers=tuple(_marker_from_dict(item) for item in raw_markers),
            digest=value["digest"],
        )


@dataclass(frozen=True)
class ApplicationReceipt:
    admission_id: uuid.UUID
    run_id: uuid.UUID
    chunk_ordinal: int
    canonical_digest: str
    target_branch: str
    state: str
    marker_digest: str
    graph_result: Mapping[str, int] = field(default_factory=dict)
    branch_token: str | None = None
    audit_fact_ranges: tuple[FactRangeRef, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "admission_id",
            _coerce_uuid(self.admission_id, "admission_id"),
        )
        object.__setattr__(self, "run_id", _coerce_uuid(self.run_id, "run_id"))
        if (
            isinstance(self.chunk_ordinal, bool)
            or not isinstance(self.chunk_ordinal, int)
            or self.chunk_ordinal < 0
        ):
            raise FactBatchFieldError("chunk_ordinal must be non-negative")
        _require_sha256(self.canonical_digest, "canonical_digest")
        _require_sha256(self.marker_digest, "marker_digest")
        _require_nonempty(self.target_branch, "target_branch")
        if self.state not in APPLICATION_RECEIPT_STATES:
            raise FactBatchFieldError(
                f"unknown receipt state {self.state!r}; "
                f"expected one of {sorted(APPLICATION_RECEIPT_STATES)}"
            )
        graph_result: dict[str, int] = {}
        for name, count in self.graph_result.items():
            if (
                not isinstance(name, str)
                or not name
                or isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
            ):
                raise FactBatchFieldError(
                    "graph_result must map non-empty names to non-negative ints"
                )
            graph_result[name] = count
        object.__setattr__(self, "graph_result", MappingProxyType(graph_result))
        if self.branch_token is not None:
            _require_nonempty(self.branch_token, "branch_token")
        ranges = tuple(self.audit_fact_ranges)
        if not all(isinstance(ref, FactRangeRef) for ref in ranges):
            raise FactBatchFieldError(
                "audit_fact_ranges must contain FactRangeRef values"
            )
        object.__setattr__(self, "audit_fact_ranges", ranges)

    @property
    def key(self) -> tuple[uuid.UUID, uuid.UUID, int]:
        return (self.admission_id, self.run_id, self.chunk_ordinal)

    @classmethod
    def prepared(
        cls,
        header: FactBatchHeader,
        chunk: FactChunk,
    ) -> "ApplicationReceipt":
        if chunk.run_id != header.run_id:
            raise FactBatchIntegrityError("chunk run_id does not match header")
        return cls(
            admission_id=header.admission_id,
            run_id=header.run_id,
            chunk_ordinal=chunk.ordinal,
            canonical_digest=chunk.digest,
            target_branch=header.target_branch,
            state="prepared",
            marker_digest=canonical_marker_digest(chunk.progress_markers),
        )

    def transition(
        self,
        state: str,
        *,
        graph_result: Mapping[str, int] | None = None,
        branch_token: str | None = None,
        audit_fact_ranges: Iterable[FactRangeRef] | None = None,
    ) -> "ApplicationReceipt":
        if state == self.state:
            candidate = replace(
                self,
                graph_result=(
                    self.graph_result if graph_result is None else graph_result
                ),
                branch_token=(
                    self.branch_token if branch_token is None else branch_token
                ),
                audit_fact_ranges=(
                    self.audit_fact_ranges
                    if audit_fact_ranges is None
                    else tuple(audit_fact_ranges)
                ),
            )
            if candidate != self:
                raise ApplicationReceiptConflict(
                    "idempotent receipt replay changed durable evidence"
                )
            return self
        if state not in _RECEIPT_TRANSITIONS[self.state]:
            raise ApplicationReceiptTransitionError(
                f"invalid receipt transition {self.state!r} -> {state!r}"
            )
        return replace(
            self,
            state=state,
            graph_result=(self.graph_result if graph_result is None else graph_result),
            branch_token=(self.branch_token if branch_token is None else branch_token),
            audit_fact_ranges=(
                self.audit_fact_ranges
                if audit_fact_ranges is None
                else tuple(audit_fact_ranges)
            ),
        )

    def verify_chunk(self, chunk: FactChunk) -> None:
        if self.run_id != chunk.run_id or self.chunk_ordinal != chunk.ordinal:
            raise ApplicationReceiptConflict(
                "receipt identity does not match recovery chunk"
            )
        if self.canonical_digest != chunk.digest:
            raise ApplicationReceiptConflict(
                "receipt canonical digest does not match recovery chunk"
            )
        if self.marker_digest != canonical_marker_digest(chunk.progress_markers):
            raise ApplicationReceiptConflict(
                "receipt marker digest does not match recovery chunk"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "admission_id": str(self.admission_id),
            "run_id": str(self.run_id),
            "chunk_ordinal": self.chunk_ordinal,
            "canonical_digest": self.canonical_digest,
            "target_branch": self.target_branch,
            "state": self.state,
            "marker_digest": self.marker_digest,
            "graph_result": dict(self.graph_result),
            "branch_token": self.branch_token,
            "audit_fact_ranges": [
                _fact_range_to_dict(ref) for ref in self.audit_fact_ranges
            ],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ApplicationReceipt":
        fields = {
            "admission_id",
            "run_id",
            "chunk_ordinal",
            "canonical_digest",
            "target_branch",
            "state",
            "marker_digest",
            "graph_result",
            "branch_token",
            "audit_fact_ranges",
        }
        required = fields - {"branch_token", "audit_fact_ranges"}
        _require_mapping_fields(value, fields, required)
        return cls(
            admission_id=value["admission_id"],
            run_id=value["run_id"],
            chunk_ordinal=value["chunk_ordinal"],
            canonical_digest=value["canonical_digest"],
            target_branch=value["target_branch"],
            state=value["state"],
            marker_digest=value["marker_digest"],
            graph_result=value["graph_result"],
            branch_token=value.get("branch_token"),
            audit_fact_ranges=tuple(
                _fact_range_from_dict(item)
                for item in value.get("audit_fact_ranges") or ()
            ),
        )


@dataclass(frozen=True)
class FactBatchSummary:
    run_id: uuid.UUID
    terminal_chunk_count: int
    digest_chain: str
    application_receipts: tuple[ApplicationReceipt, ...]
    observed_scope: Mapping[str, Any] | None
    completion: str
    source_delta: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _coerce_uuid(self.run_id, "run_id"))
        if (
            isinstance(self.terminal_chunk_count, bool)
            or not isinstance(self.terminal_chunk_count, int)
            or self.terminal_chunk_count < 0
        ):
            raise FactBatchFieldError("terminal_chunk_count must be non-negative")
        _require_sha256(self.digest_chain, "digest_chain")
        receipts = tuple(self.application_receipts)
        object.__setattr__(self, "application_receipts", receipts)
        if self.completion not in FACT_BATCH_COMPLETIONS:
            raise FactBatchFieldError(
                f"unknown completion {self.completion!r}; "
                f"expected one of {sorted(FACT_BATCH_COMPLETIONS)}"
            )
        for name in ("observed_scope", "source_delta"):
            value = getattr(self, name)
            if value is not None:
                _validate_safe_json(value, name)
                object.__setattr__(self, name, _freeze_json(value))

    @classmethod
    def from_receipts(
        cls,
        *,
        run_id: uuid.UUID | str,
        receipts: Iterable[ApplicationReceipt],
        observed_scope: Mapping[str, Any] | None,
        completion: str,
        source_delta: Mapping[str, Any] | None = None,
    ) -> "FactBatchSummary":
        receipt_tuple = tuple(receipts)
        return cls(
            run_id=_coerce_uuid(run_id, "run_id"),
            terminal_chunk_count=len(receipt_tuple),
            digest_chain=digest_chain_from_receipts(receipt_tuple),
            application_receipts=receipt_tuple,
            observed_scope=observed_scope,
            completion=completion,
            source_delta=source_delta,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "terminal_chunk_count": self.terminal_chunk_count,
            "digest_chain": self.digest_chain,
            "application_receipts": [
                receipt.to_dict() for receipt in self.application_receipts
            ],
            "observed_scope": _thaw_json(self.observed_scope),
            "completion": self.completion,
            "source_delta": _thaw_json(self.source_delta),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FactBatchSummary":
        fields = {
            "run_id",
            "terminal_chunk_count",
            "digest_chain",
            "application_receipts",
            "observed_scope",
            "completion",
            "source_delta",
        }
        required = fields - {"observed_scope", "source_delta"}
        _require_mapping_fields(value, fields, required)
        raw_receipts = value["application_receipts"]
        if not isinstance(raw_receipts, Sequence) or isinstance(raw_receipts, str):
            raise FactBatchFieldError("application_receipts must be a sequence")
        return cls(
            run_id=value["run_id"],
            terminal_chunk_count=value["terminal_chunk_count"],
            digest_chain=value["digest_chain"],
            application_receipts=tuple(
                ApplicationReceipt.from_dict(item) for item in raw_receipts
            ),
            observed_scope=value.get("observed_scope"),
            completion=value["completion"],
            source_delta=value.get("source_delta"),
        )


class FactBatchValidator:
    """O(1)-state validator for one logical streamed FactBatch."""

    def __init__(
        self,
        header: FactBatchHeader,
        *,
        profile: ProducerProfile,
        retractions_allowed: bool = False,
        validate_signature: bool = True,
    ) -> None:
        validate_header_authority(header, profile)
        self.header = header
        self.profile = profile
        self.retractions_allowed = retractions_allowed
        self.validate_signature = validate_signature
        self._next_ordinal = 0
        self._chain_state = _CHAIN_SEED
        self._finished = False

    @property
    def accepted_chunk_count(self) -> int:
        return self._next_ordinal

    def accept(self, chunk: FactChunk) -> FactChunk:
        if self._finished:
            raise FactBatchIntegrityError("batch is already finished")
        if chunk.run_id != self.header.run_id:
            raise FactBatchIntegrityError("chunk run_id does not match header")
        if chunk.ordinal != self._next_ordinal:
            raise FactBatchIntegrityError(
                f"expected chunk ordinal {self._next_ordinal}, got {chunk.ordinal}"
            )
        validate_chunk_authority(
            chunk,
            profile=self.profile,
            retractions_allowed=self.retractions_allowed,
            validate_signature=self.validate_signature,
        )
        self._chain_state = _advance_digest_chain(
            self._chain_state,
            chunk.ordinal,
            chunk.digest,
        )
        self._next_ordinal += 1
        return chunk

    def finish(self, summary: FactBatchSummary) -> FactBatchSummary:
        if self._finished:
            raise FactBatchIntegrityError("batch is already finished")
        if summary.run_id != self.header.run_id:
            raise FactBatchIntegrityError("summary run_id does not match header")
        if summary.terminal_chunk_count != self._next_ordinal:
            raise FactBatchIntegrityError(
                "summary terminal_chunk_count does not match streamed chunks"
            )
        expected_chain = f"sha256:{self._chain_state.hex()}"
        if summary.digest_chain != expected_chain:
            raise FactBatchIntegrityError("summary digest chain mismatch")
        if len(summary.application_receipts) != self._next_ordinal:
            raise FactBatchIntegrityError(
                "summary receipt count does not match streamed chunks"
            )
        receipt_state = _CHAIN_SEED
        for ordinal, receipt in enumerate(summary.application_receipts):
            if receipt.admission_id != self.header.admission_id:
                raise FactBatchIntegrityError(
                    "receipt admission_id does not match header"
                )
            if receipt.run_id != self.header.run_id:
                raise FactBatchIntegrityError("receipt run_id does not match header")
            if receipt.chunk_ordinal != ordinal:
                raise FactBatchIntegrityError("receipt ordinal gap")
            if receipt.target_branch != self.header.target_branch:
                raise FactBatchIntegrityError(
                    "receipt target_branch does not match header"
                )
            if receipt.state != "applied":
                raise FactBatchIntegrityError(
                    "terminal summary requires applied receipts"
                )
            receipt_state = _advance_digest_chain(
                receipt_state,
                ordinal,
                receipt.canonical_digest,
            )
        if receipt_state != self._chain_state:
            raise FactBatchIntegrityError(
                "receipt digests do not match streamed chunk digests"
            )
        self._finished = True
        return summary


class ApplicationReceiptStore(Protocol):
    """Durable receipt interface used by application/recovery orchestration."""

    def get(
        self,
        admission_id: uuid.UUID,
        run_id: uuid.UUID,
        chunk_ordinal: int,
    ) -> ApplicationReceipt | None: ...

    def prepare(self, receipt: ApplicationReceipt) -> ApplicationReceipt: ...

    def transition(
        self,
        key: tuple[uuid.UUID, uuid.UUID, int],
        state: str,
        *,
        graph_result: Mapping[str, int] | None = None,
        branch_token: str | None = None,
        audit_fact_ranges: Iterable[FactRangeRef] | None = None,
    ) -> ApplicationReceipt: ...

    def transition_with_evidence(
        self,
        key: tuple[uuid.UUID, uuid.UUID, int],
        state: str,
        *,
        graph_result: Mapping[str, int] | None = None,
        branch_token: str | None = None,
        evidence_writer: Callable[[], Iterable[FactRangeRef] | None],
    ) -> ApplicationReceipt: ...


class MemoryApplicationReceiptStore:
    """Deterministic non-durable store for contract/property tests only."""

    def __init__(self) -> None:
        self._receipts: dict[tuple[uuid.UUID, uuid.UUID, int], ApplicationReceipt] = {}

    def get(
        self,
        admission_id: uuid.UUID,
        run_id: uuid.UUID,
        chunk_ordinal: int,
    ) -> ApplicationReceipt | None:
        return self._receipts.get((admission_id, run_id, chunk_ordinal))

    def prepare(self, receipt: ApplicationReceipt) -> ApplicationReceipt:
        if receipt.state != "prepared":
            raise ApplicationReceiptTransitionError(
                "new receipt must start in prepared state"
            )
        existing = self._receipts.get(receipt.key)
        if existing is not None:
            _assert_receipt_replay(existing, receipt)
            return existing
        self._receipts[receipt.key] = receipt
        return receipt

    def transition(
        self,
        key: tuple[uuid.UUID, uuid.UUID, int],
        state: str,
        *,
        graph_result: Mapping[str, int] | None = None,
        branch_token: str | None = None,
        audit_fact_ranges: Iterable[FactRangeRef] | None = None,
    ) -> ApplicationReceipt:
        receipt = self._receipts.get(key)
        if receipt is None:
            raise ApplicationReceiptConflict("receipt does not exist")
        updated = receipt.transition(
            state,
            graph_result=graph_result,
            branch_token=branch_token,
            audit_fact_ranges=audit_fact_ranges,
        )
        self._receipts[key] = updated
        return updated

    def transition_with_evidence(
        self,
        key: tuple[uuid.UUID, uuid.UUID, int],
        state: str,
        *,
        graph_result: Mapping[str, int] | None = None,
        branch_token: str | None = None,
        evidence_writer: Callable[[], Iterable[FactRangeRef] | None],
    ) -> ApplicationReceipt:
        ranges = tuple(evidence_writer() or ())
        return self.transition(
            key,
            state,
            graph_result=graph_result,
            branch_token=branch_token,
            audit_fact_ranges=ranges,
        )


class PostgresApplicationReceiptStore:
    """Postgres-backed authoritative receipt store.

    Each method owns a short transaction. SourceRunner integration later groups
    the ``applied`` transition with progress markers and optional audit ranges in
    the same transaction; the row shape and conflict semantics are already final.
    """

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def get(
        self,
        admission_id: uuid.UUID,
        run_id: uuid.UUID,
        chunk_ordinal: int,
    ) -> ApplicationReceipt | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT admission_id, run_id, chunk_ordinal, canonical_digest,
                       target_branch, state, marker_digest, graph_result,
                       branch_token, audit_fact_ranges
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s AND run_id = %s
                   AND chunk_ordinal = %s
                """,
                (admission_id, run_id, chunk_ordinal),
            )
            row = cur.fetchone()
        return None if row is None else _receipt_from_row(row)

    def prepare(self, receipt: ApplicationReceipt) -> ApplicationReceipt:
        if receipt.state != "prepared":
            raise ApplicationReceiptTransitionError(
                "new receipt must start in prepared state"
            )
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.fact_batch_receipts
                  (admission_id, run_id, chunk_ordinal, canonical_digest,
                   target_branch, state, marker_digest, graph_result,
                   branch_token, audit_fact_ranges)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (admission_id, run_id, chunk_ordinal) DO NOTHING
                """,
                (
                    receipt.admission_id,
                    receipt.run_id,
                    receipt.chunk_ordinal,
                    receipt.canonical_digest,
                    receipt.target_branch,
                    receipt.state,
                    receipt.marker_digest,
                    Jsonb(dict(receipt.graph_result)),
                    receipt.branch_token,
                    Jsonb(
                        [_fact_range_to_dict(ref) for ref in receipt.audit_fact_ranges]
                    ),
                ),
            )
            cur.execute(
                """
                SELECT admission_id, run_id, chunk_ordinal, canonical_digest,
                       target_branch, state, marker_digest, graph_result,
                       branch_token, audit_fact_ranges
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s AND run_id = %s
                   AND chunk_ordinal = %s
                 FOR UPDATE
                """,
                receipt.key,
            )
            row = cur.fetchone()
            if row is None:
                raise ApplicationReceiptConflict(
                    "receipt upsert returned no durable row"
                )
            existing = _receipt_from_row(row)
            _assert_receipt_replay(existing, receipt)
            return existing

    def transition(
        self,
        key: tuple[uuid.UUID, uuid.UUID, int],
        state: str,
        *,
        graph_result: Mapping[str, int] | None = None,
        branch_token: str | None = None,
        audit_fact_ranges: Iterable[FactRangeRef] | None = None,
    ) -> ApplicationReceipt:
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT admission_id, run_id, chunk_ordinal, canonical_digest,
                       target_branch, state, marker_digest, graph_result,
                       branch_token, audit_fact_ranges
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s AND run_id = %s
                   AND chunk_ordinal = %s
                 FOR UPDATE
                """,
                key,
            )
            row = cur.fetchone()
            if row is None:
                raise ApplicationReceiptConflict("receipt does not exist")
            receipt = _receipt_from_row(row)
            updated = receipt.transition(
                state,
                graph_result=graph_result,
                branch_token=branch_token,
                audit_fact_ranges=audit_fact_ranges,
            )
            if updated == receipt:
                return receipt
            cur.execute(
                """
                UPDATE okg.fact_batch_receipts
                   SET state = %s,
                       graph_result = %s,
                       branch_token = %s,
                       audit_fact_ranges = %s,
                       updated_at = now()
                 WHERE admission_id = %s AND run_id = %s
                   AND chunk_ordinal = %s
                """,
                (
                    updated.state,
                    Jsonb(dict(updated.graph_result)),
                    updated.branch_token,
                    Jsonb(
                        [_fact_range_to_dict(ref) for ref in updated.audit_fact_ranges]
                    ),
                    *key,
                ),
            )
            return updated

    def transition_with_evidence(
        self,
        key: tuple[uuid.UUID, uuid.UUID, int],
        state: str,
        *,
        graph_result: Mapping[str, int] | None = None,
        branch_token: str | None = None,
        evidence_writer: Callable[[], Iterable[FactRangeRef] | None],
    ) -> ApplicationReceipt:
        """Commit marker/audit evidence and the applied receipt together."""
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT admission_id, run_id, chunk_ordinal, canonical_digest,
                       target_branch, state, marker_digest, graph_result,
                       branch_token, audit_fact_ranges
                  FROM okg.fact_batch_receipts
                 WHERE admission_id = %s AND run_id = %s
                   AND chunk_ordinal = %s
                 FOR UPDATE
                """,
                key,
            )
            row = cur.fetchone()
            if row is None:
                raise ApplicationReceiptConflict("receipt does not exist")
            receipt = _receipt_from_row(row)
            if receipt.state == state:
                return receipt.transition(
                    state,
                    graph_result=graph_result,
                    branch_token=branch_token,
                    audit_fact_ranges=receipt.audit_fact_ranges,
                )
            ranges = tuple(evidence_writer() or ())
            updated = receipt.transition(
                state,
                graph_result=graph_result,
                branch_token=branch_token,
                audit_fact_ranges=ranges,
            )
            cur.execute(
                """
                UPDATE okg.fact_batch_receipts
                   SET state = %s,
                       graph_result = %s,
                       branch_token = %s,
                       audit_fact_ranges = %s,
                       updated_at = now()
                 WHERE admission_id = %s AND run_id = %s
                   AND chunk_ordinal = %s
                """,
                (
                    updated.state,
                    Jsonb(dict(updated.graph_result)),
                    updated.branch_token,
                    Jsonb(
                        [_fact_range_to_dict(ref) for ref in updated.audit_fact_ranges]
                    ),
                    *key,
                ),
            )
            return updated


def validate_header_authority(
    header: FactBatchHeader,
    profile: ProducerProfile,
) -> None:
    """Bind caller-visible header scope to a server-resolved profile."""
    if profile.status not in {"active", "compat"}:
        raise FactBatchAuthorityError(
            f"producer profile status {profile.status!r} is not writable"
        )
    if header.producer_profile_fingerprint != profile.profile_fingerprint:
        raise FactBatchAuthorityError("producer profile fingerprint mismatch")
    if header.deployment != profile.deployment_name:
        raise FactBatchAuthorityError("producer deployment mismatch")
    if (
        profile.catalog_version_id is not None
        and header.catalog_version_id != profile.catalog_version_id
    ):
        raise FactBatchAuthorityError("producer catalog version mismatch")
    if header.requested_scope is not None and not _mapping_is_subset(
        header.requested_scope,
        profile.authority_scope,
    ):
        raise FactBatchAuthorityError(
            "requested scope exceeds server-resolved producer authority"
        )


def validate_chunk_authority(
    chunk: FactChunk,
    *,
    profile: ProducerProfile,
    retractions_allowed: bool,
    validate_signature: bool = True,
) -> None:
    node_records: list[dict[str, Any]] = []
    edge_records: list[dict[str, Any]] = []
    for fact in chunk.facts:
        kind = "node" if isinstance(fact, NodeFact) else "edge"
        compatibility_unbounded_kinds = (
            profile.status == "compat" and not profile.allowed_record_kinds
        )
        if kind not in profile.allowed_record_kinds and not compatibility_unbounded_kinds:
            raise FactBatchAuthorityError(
                f"producer is not allowed to emit {kind} facts"
            )
        if fact.op.upper() == "R" and not retractions_allowed:
            raise FactBatchAuthorityError(
                "retraction requires server-resolved source/profile authority"
            )
        if validate_signature:
            # These records exist only as the signature payload. Building them
            # for a caller that is not validating a signature thawed and
            # reallocated every fact for nothing.
            record = _fact_to_dict(fact)
            record.pop("kind", None)
            (node_records if kind == "node" else edge_records).append(record)
    if validate_signature and (node_records or edge_records):
        evaluate_payload_signature(
            profile=profile,
            nodes=node_records,
            edges=edge_records,
        )


def canonical_chunk_digest(
    *,
    run_id: uuid.UUID | str,
    ordinal: int,
    facts: Iterable[NodeFact | EdgeFact],
    progress_markers: Iterable[ProgressMarker],
) -> str:
    payload = {
        "run_id": str(_coerce_uuid(run_id, "run_id")),
        "ordinal": ordinal,
        "facts": [_fact_payload(fact, thaw=False) for fact in facts],
        "progress_markers": [_marker_to_dict(marker) for marker in progress_markers],
    }
    return _canonical_digest(payload)


def canonical_marker_digest(markers: Iterable[ProgressMarker]) -> str:
    return _canonical_digest([_marker_to_dict(marker) for marker in markers])


def digest_chain_from_receipts(
    receipts: Iterable[ApplicationReceipt],
) -> str:
    state = _CHAIN_SEED
    expected = 0
    for receipt in receipts:
        if receipt.chunk_ordinal != expected:
            raise FactBatchIntegrityError("receipt ordinal gap")
        state = _advance_digest_chain(
            state,
            receipt.chunk_ordinal,
            receipt.canonical_digest,
        )
        expected += 1
    return f"sha256:{state.hex()}"


def recover_prepared_receipt(
    *,
    store: ApplicationReceiptStore,
    receipt: ApplicationReceipt,
    chunk: FactChunk,
    apply_chunk: Callable[[FactChunk], Mapping[str, int]],
    branch_token: str | None = None,
    audit_fact_ranges: Iterable[FactRangeRef] = (),
) -> ApplicationReceipt:
    """Idempotently reapply a prepared chunk and finalize its receipt."""
    if receipt.state == "applied":
        receipt.verify_chunk(chunk)
        return receipt
    if receipt.state != "prepared":
        raise ApplicationReceiptTransitionError(
            f"cannot recover receipt in state {receipt.state!r}"
        )
    receipt.verify_chunk(chunk)
    graph_result = apply_chunk(chunk)
    return store.transition(
        receipt.key,
        "applied",
        graph_result=graph_result,
        branch_token=branch_token,
        audit_fact_ranges=audit_fact_ranges,
    )


def _assert_receipt_replay(
    existing: ApplicationReceipt,
    candidate: ApplicationReceipt,
) -> None:
    immutable_existing = (
        existing.admission_id,
        existing.run_id,
        existing.chunk_ordinal,
        existing.canonical_digest,
        existing.target_branch,
        existing.marker_digest,
    )
    immutable_candidate = (
        candidate.admission_id,
        candidate.run_id,
        candidate.chunk_ordinal,
        candidate.canonical_digest,
        candidate.target_branch,
        candidate.marker_digest,
    )
    if immutable_existing != immutable_candidate:
        raise ApplicationReceiptConflict(
            "receipt replay changed immutable identity or digest"
        )


def _advance_digest_chain(state: bytes, ordinal: int, digest: str) -> bytes:
    return hashlib.sha256(
        state + _canonical_json_bytes({"ordinal": ordinal, "digest": digest})
    ).digest()


def _canonical_digest(value: Any) -> str:
    return f"sha256:{hashlib.sha256(_canonical_json_bytes(value)).hexdigest()}"


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=_canonical_json_default,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FactBatchFieldError(
            f"FactBatch value is not canonical JSON: {exc}"
        ) from exc


def _canonical_json_default(value: Any) -> Any:
    """Expose an immutable JSON mapping to the standard canonical encoder.

    FactBatch freezes mappings with ``MappingProxyType``. Thawing the whole
    tree before every ``json.dumps`` walked and reallocated the entire payload
    a second time. ``json`` already traverses ordinary mappings and sequences,
    so converting only the immutable mapping it is currently encoding yields
    the same canonical bytes without materializing a second payload. Anything
    else still raises, so a non-serializable value stays a refusal rather than
    becoming a silently encoded string.
    """

    if isinstance(value, Mapping):
        # ``str(key)`` reproduces the coercion ``_thaw_json`` performed. Frozen
        # fact data always has string keys, so this only matters for a caller
        # that hands ``canonical_chunk_digest`` an immutable mapping the fact
        # freezer never saw -- and there it keeps the bytes where they were.
        return {str(key): item for key, item in value.items()}
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _freeze_fact(fact: NodeFact | EdgeFact) -> NodeFact | EdgeFact:
    if isinstance(fact, NodeFact):
        _require_nonempty(fact.node_id, "node_id")
        _require_nonempty(fact.subtype, "subtype")
        _require_op(fact.op)
        attrs = _freeze_safe_json(
            fact.attrs, "node.attrs", forbidden_keys=_FORBIDDEN_FACT_DATA_KEYS
        )
        source_record_id = _freeze_safe_json(
            fact.source_record_id,
            "node.source_record_id",
            forbidden_keys=_FORBIDDEN_FACT_DATA_KEYS,
        )
        source_revision = _freeze_safe_json(
            fact.source_revision,
            "node.source_revision",
            forbidden_keys=_FORBIDDEN_FACT_DATA_KEYS,
        )
        return NodeFact(
            node_id=fact.node_id,
            subtype=fact.subtype,
            attrs=attrs,
            source_record_id=source_record_id,
            source_revision=source_revision,
            op=fact.op.upper(),
            ownership_mode=fact.ownership_mode,
        )
    if isinstance(fact, EdgeFact):
        for name in ("src", "dst", "edge_type"):
            _require_nonempty(getattr(fact, name), name)
        _require_op(fact.op)
        attrs = _freeze_safe_json(
            fact.attrs, "edge.attrs", forbidden_keys=_FORBIDDEN_FACT_DATA_KEYS
        )
        source_record_id = _freeze_safe_json(
            fact.source_record_id,
            "edge.source_record_id",
            forbidden_keys=_FORBIDDEN_FACT_DATA_KEYS,
        )
        source_revision = _freeze_safe_json(
            fact.source_revision,
            "edge.source_revision",
            forbidden_keys=_FORBIDDEN_FACT_DATA_KEYS,
        )
        if not math.isfinite(float(fact.confidence)):
            raise FactBatchFieldError("edge confidence must be finite")
        return EdgeFact(
            src=fact.src,
            dst=fact.dst,
            edge_type=fact.edge_type,
            attrs=attrs,
            provenance=fact.provenance,
            confidence=float(fact.confidence),
            source_record_id=source_record_id,
            source_revision=source_revision,
            op=fact.op.upper(),
        )
    raise FactBatchFieldError(
        f"chunk facts must be NodeFact or EdgeFact, got {type(fact).__name__}"
    )


def _freeze_marker(marker: ProgressMarker) -> ProgressMarker:
    if not isinstance(marker, ProgressMarker):
        raise FactBatchFieldError("progress_markers must contain ProgressMarker values")
    _require_nonempty(marker.record_id, "progress_marker.record_id")
    _require_nonempty(marker.fingerprint, "progress_marker.fingerprint")
    return ProgressMarker(marker.record_id, marker.fingerprint)


def _fact_to_dict(fact: NodeFact | EdgeFact) -> dict[str, Any]:
    """Plain-JSON view of a fact, for ``to_dict`` and signature payloads."""

    return _fact_payload(fact, thaw=True)


def _fact_payload(fact: NodeFact | EdgeFact, *, thaw: bool) -> dict[str, Any]:
    """One shape for both the public dict view and the canonical digest.

    ``thaw=True`` materializes plain dicts and lists, which callers that hand
    the payload to psycopg or to a producer signature require. ``thaw=False``
    leaves an already-frozen value in place for the digest, where
    ``_canonical_json_bytes`` converts each mapping lazily and the encoded
    bytes are byte-for-byte identical.
    """

    convert = _thaw_json if thaw else _canonical_value
    if isinstance(fact, NodeFact):
        payload = {
            "kind": "node",
            "node_id": fact.node_id,
            "subtype": fact.subtype,
            "attrs": convert(fact.attrs),
            "source_record_id": convert(fact.source_record_id),
            "source_revision": convert(fact.source_revision),
            "op": fact.op.upper(),
        }
        if fact.ownership_mode != "authoritative":
            payload["ownership_mode"] = fact.ownership_mode
        return payload
    if isinstance(fact, EdgeFact):
        return {
            "kind": "edge",
            "src": fact.src,
            "dst": fact.dst,
            "edge_type": fact.edge_type,
            "attrs": convert(fact.attrs),
            "provenance": fact.provenance,
            "confidence": float(fact.confidence),
            "source_record_id": convert(fact.source_record_id),
            "source_revision": convert(fact.source_revision),
            "op": fact.op.upper(),
        }
    raise FactBatchFieldError(f"unsupported fact type {type(fact).__name__}")


def _canonical_value(value: Any) -> Any:
    """Pass a frozen value straight to the encoder; thaw anything else.

    ``_freeze_fact`` produces ``MappingProxyType`` trees whose keys are already
    strings, so the encoder can walk them directly and the second full-tree
    copy disappears. ``canonical_chunk_digest`` is public, though, and a caller
    may hand it a fact the freezer never saw, whose plain dict could carry
    non-string keys. Those keep the old ``_thaw_json`` normalization so their
    digest bytes do not move.
    """

    if isinstance(value, MappingProxyType):
        return value
    return _thaw_json(value)


def _fact_from_dict(value: Mapping[str, Any]) -> NodeFact | EdgeFact:
    if not isinstance(value, Mapping):
        raise FactBatchFieldError("fact must be a mapping")
    kind = value.get("kind")
    if kind == "node":
        fields = {
            "kind",
            "node_id",
            "subtype",
            "attrs",
            "source_record_id",
            "source_revision",
            "op",
            "ownership_mode",
        }
        required_fields = fields - {"ownership_mode"}
        _require_mapping_fields(value, fields, required_fields)
        return NodeFact(
            node_id=value["node_id"],
            subtype=value["subtype"],
            attrs=value["attrs"],
            source_record_id=value["source_record_id"],
            source_revision=value["source_revision"],
            op=value["op"],
            ownership_mode=value.get("ownership_mode", "authoritative"),
        )
    if kind == "edge":
        fields = {
            "kind",
            "src",
            "dst",
            "edge_type",
            "attrs",
            "provenance",
            "confidence",
            "source_record_id",
            "source_revision",
            "op",
        }
        _require_mapping_fields(value, fields, fields)
        return EdgeFact(
            src=value["src"],
            dst=value["dst"],
            edge_type=value["edge_type"],
            attrs=value["attrs"],
            provenance=value["provenance"],
            confidence=value["confidence"],
            source_record_id=value["source_record_id"],
            source_revision=value["source_revision"],
            op=value["op"],
        )
    raise FactBatchFieldError(f"unknown fact kind {kind!r}")


def _marker_to_dict(marker: ProgressMarker) -> dict[str, str]:
    return {"record_id": marker.record_id, "fingerprint": marker.fingerprint}


def _marker_from_dict(value: Mapping[str, Any]) -> ProgressMarker:
    fields = {"record_id", "fingerprint"}
    _require_mapping_fields(value, fields, fields)
    return ProgressMarker(value["record_id"], value["fingerprint"])


def _fact_range_to_dict(ref: FactRangeRef) -> dict[str, Any]:
    return {
        "ref_kind": ref.ref_kind,
        "ref_key": ref.ref_key,
        "source": ref.source,
        "run_id": ref.run_id,
        "fact_id_min": ref.fact_id_min,
        "fact_id_max": ref.fact_id_max,
        "record_count": ref.record_count,
    }


def _fact_range_from_dict(value: Mapping[str, Any]) -> FactRangeRef:
    fields = {
        "ref_kind",
        "ref_key",
        "source",
        "run_id",
        "fact_id_min",
        "fact_id_max",
        "record_count",
    }
    _require_mapping_fields(value, fields, fields)
    return FactRangeRef(**{name: value[name] for name in fields})


def _receipt_from_row(row: Sequence[Any]) -> ApplicationReceipt:
    return ApplicationReceipt(
        admission_id=row[0],
        run_id=row[1],
        chunk_ordinal=int(row[2]),
        canonical_digest=str(row[3]),
        target_branch=str(row[4]),
        state=str(row[5]),
        marker_digest=str(row[6]),
        graph_result=dict(row[7] or {}),
        branch_token=(str(row[8]) if row[8] is not None else None),
        audit_fact_ranges=tuple(_fact_range_from_dict(item) for item in (row[9] or [])),
    )


def _require_mapping_fields(
    value: Mapping[str, Any],
    allowed: set[str],
    required: set[str],
) -> None:
    if not isinstance(value, Mapping):
        raise FactBatchFieldError("serialized value must be a mapping")
    extras = sorted(set(value) - allowed)
    if extras:
        raise FactBatchFieldError(f"unknown control fields: {extras}")
    missing = sorted(required - set(value))
    if missing:
        raise FactBatchFieldError(f"missing required fields: {missing}")


def _coerce_uuid(value: uuid.UUID | str, field_name: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise FactBatchFieldError(f"{field_name} must be a UUID") from exc


def _require_nonempty(value: Any, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise FactBatchFieldError(f"{field_name} must be a non-empty string")


def _require_sha256(value: Any, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
    ):
        raise FactBatchFieldError(f"{field_name} must be a sha256:<64 hex> digest")
    try:
        int(value[7:], 16)
    except ValueError as exc:
        raise FactBatchFieldError(
            f"{field_name} must be a sha256:<64 hex> digest"
        ) from exc


def _require_op(value: Any) -> None:
    if not isinstance(value, str) or value.upper() not in {"I", "R"}:
        raise FactBatchFieldError(f"unsupported fact op {value!r}")


def _validate_safe_json(
    value: Any,
    path: str,
    *,
    forbidden_keys: frozenset[str] = _FORBIDDEN_CONTROL_KEYS,
) -> None:
    # ``forbidden_keys`` selects the surface's policy: the full control set for
    # authority surfaces (requested_scope / observed_scope / source_delta), the
    # narrowed secrets-only set for persisted fact data. See the sets' comment.
    if callable(value):
        raise FactBatchFieldError(f"{path} contains a callback/client object")
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise FactBatchFieldError(f"{path} contains a non-finite number")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise FactBatchFieldError(f"{path} has a non-string key")
            if key.strip().lower() in forbidden_keys:
                raise FactBatchFieldError(
                    f"{path} contains forbidden control/secret field {key!r}"
                )
            _validate_safe_json(item, f"{path}.{key}", forbidden_keys=forbidden_keys)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_safe_json(item, f"{path}[{index}]", forbidden_keys=forbidden_keys)
        return
    raise FactBatchFieldError(
        f"{path} contains non-serializable {type(value).__name__}"
    )


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _freeze_safe_json(
    value: Any,
    path: str,
    *,
    forbidden_keys: frozenset[str],
) -> Any:
    """Validate and freeze fact JSON in ONE recursive traversal.

    ``_validate_safe_json`` followed by ``_freeze_json`` walked the same tree
    twice per fact. This performs the identical checks, in the identical order,
    and returns the frozen value, so every refusal message and every frozen
    shape is unchanged; it exists only so the walk happens once.
    """

    if callable(value):
        raise FactBatchFieldError(f"{path} contains a callback/client object")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise FactBatchFieldError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise FactBatchFieldError(f"{path} has a non-string key")
            if key.strip().lower() in forbidden_keys:
                raise FactBatchFieldError(
                    f"{path} contains forbidden control/secret field {key!r}"
                )
            frozen[key] = _freeze_safe_json(
                item,
                f"{path}.{key}",
                forbidden_keys=forbidden_keys,
            )
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(
            _freeze_safe_json(
                item,
                f"{path}[{index}]",
                forbidden_keys=forbidden_keys,
            )
            for index, item in enumerate(value)
        )
    raise FactBatchFieldError(
        f"{path} contains non-serializable {type(value).__name__}"
    )


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(item) for item in value]
    return value


def _mapping_is_subset(requested: Mapping[str, Any], allowed: Any) -> bool:
    if not isinstance(allowed, Mapping):
        return False
    for key, requested_value in requested.items():
        if key not in allowed:
            return False
        if not _value_is_subset(requested_value, allowed[key]):
            return False
    return True


def _value_is_subset(requested: Any, allowed: Any) -> bool:
    if isinstance(requested, Mapping):
        return _mapping_is_subset(requested, allowed)
    if isinstance(requested, (list, tuple, set, frozenset)):
        if not isinstance(allowed, (list, tuple, set, frozenset)):
            return False
        allowed_values = list(allowed)
        return all(
            any(_value_is_subset(item, candidate) for candidate in allowed_values)
            for item in requested
        )
    return requested == allowed


__all__ = [
    "APPLICATION_RECEIPT_STATES",
    "FACT_BATCH_COMPLETIONS",
    "FACT_BATCH_SCHEMA_VERSION",
    "MAX_FACTS_PER_CHUNK",
    "MAX_MARKERS_PER_CHUNK",
    "ApplicationReceipt",
    "ApplicationReceiptConflict",
    "ApplicationReceiptStore",
    "ApplicationReceiptTransitionError",
    "FactBatchAuthorityError",
    "FactBatchError",
    "FactBatchFieldError",
    "FactBatchHeader",
    "FactBatchIntegrityError",
    "FactBatchSummary",
    "FactBatchValidator",
    "FactBatchVersionError",
    "FactChunk",
    "MemoryApplicationReceiptStore",
    "PostgresApplicationReceiptStore",
    "canonical_chunk_digest",
    "canonical_marker_digest",
    "digest_chain_from_receipts",
    "recover_prepared_receipt",
    "validate_chunk_authority",
    "validate_header_authority",
]
