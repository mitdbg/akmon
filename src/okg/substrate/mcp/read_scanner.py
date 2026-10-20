"""Shared reader-scanner contracts, independent of rendered result views.

Adapters own source enumeration and reader finishing. Short native index pages
are not enumeration evidence. These helpers add no BM25 ordering capability.

Stage A contract (PACT shared-read-scanners-3199): no production read path
imports this module yet. Grep (#3218) and windows (#3223) are the first
planned adapters; until one lands, these types are proven by contract tests
only.

What the types can and cannot check:

* ``EnumerationEvidence(method="whole_sql" | "terminal_keyset")`` on a source
  batch is the adapter's own assertion that it ran the whole SQL statement or
  walked a keyset to its end. The type checks that the batch is terminal,
  unbounded and exact; it cannot see the SQL. Review each adapter that emits
  it against the statement it actually runs.
* ``finished=True`` evidence comes only from :func:`finish_scan_batch`; the
  constructor refuses it without the module's private seal, which is not an
  init argument. ``dataclasses.replace`` of finished evidence therefore
  refuses: a finished proof cannot be moved to another domain or scope.
* Range leaves compare with ``COLLATE "C"`` (see ``operator_sql``). A btree
  index built under a locale collation cannot serve such a comparison, so a
  future keyset adapter must order and index its key columns with
  ``COLLATE "C"`` (or prove an equivalent order) before claiming an ordering
  identity.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import zlib
from dataclasses import dataclass, field
from collections.abc import Callable, Mapping
from typing import Any

from okg.substrate.mcp.continuation import (
    ContinuationTokenInvalid,
    consume_continuation,
    issue_continuation,
)
from okg.substrate.mcp.operator_sql import CompiledPredicate

__all__ = [
    "MAX_SCAN_BODY_BYTES",
    "MAX_SCAN_TOKEN_CHARS",
    "EnumerationEvidence",
    "FinishedCandidate",
    "FinishedScan",
    "ScanBatch",
    "ScanContext",
    "ScanContinuationUnavailable",
    "ScanInputInvalid",
    "consume_scan_continuation",
    "finish_scan_batch",
    "issue_scan_continuation",
    "make_scan_context",
    "scan_token_body_bytes",
]

MAX_SCAN_BODY_BYTES = 8192
MAX_SCAN_TOKEN_CHARS = 16384
_MIN_BUDGET_BYTES = 4096
_REQUIRED_BINDING = frozenset({
    "kind", "deployment", "database", "branch", "generation",
    "catalog_version", "authority", "policy", "key_id", "projection", "sort",
})
#: ``policy`` is the active Nomos policy fingerprint, legitimately null when no
#: policy bundle is active (the authority digest still binds that absence).
#: Every other required part must carry a value.
_NULLABLE_BINDING = frozenset({"policy"})


class _FinishSeal:
    """Private seal: only :func:`finish_scan_batch` may mint finished evidence.

    Copies and pickles resolve to the one module instance, so a copied
    finished proof stays valid and a fresh object never passes as the seal.
    """
    __slots__ = ()

    def __copy__(self) -> "_FinishSeal":
        return self

    def __deepcopy__(self, memo: Any) -> "_FinishSeal":
        return self

    def __reduce__(self) -> str:
        return "_FINISH_SEAL"


_FINISH_SEAL = _FinishSeal()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


class ScanContinuationUnavailable(ValueError):
    """The issuer withholds continuation state; the adapter must say so.

    This is an internal contract signal, not a wire error code: the adapter
    that catches it withholds `next` and discloses ``reason`` honestly.
    """

    def __init__(self, message: str, *, reason: str, **data: Any) -> None:
        super().__init__(message)
        self.reason = reason
        self.data = {"reason": reason, **data}


class ScanInputInvalid(ValueError):
    """A scanner query or scope has no canonical JSON form to fingerprint.

    The scanner-side counterpart of the ``where`` refusal
    (``reason="invalid_where_value"``): NaN, Infinity, a lone surrogate or a
    non-JSON value is refused by name instead of surfacing as a raw
    ``UnicodeEncodeError`` from fingerprinting. ``data`` names the part.
    """

    def __init__(self, part: str) -> None:
        super().__init__(
            f"scanner {part} must be JSON values: finite numbers and valid Unicode text")
        self.reason = "invalid_scan_value"
        self.data = {"reason": self.reason, "part": part}


def _require_canonical(part: str, value: Any) -> None:
    try:
        _canonical(value)
    except (TypeError, ValueError) as exc:  # UnicodeEncodeError is a ValueError
        raise ScanInputInvalid(part) from exc


def _nonempty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


@dataclass(frozen=True)
class ScanContext:
    """Immutable complete semantic identity over an existing authority binding."""
    _binding: bytes
    scope_identity: str
    source: str
    result_unit: str
    membership_target: str
    ordering_identity: str | None
    max_bytes: int

    @property
    def binding(self) -> dict[str, Any]:
        return json.loads(self._binding)


def make_scan_context(
    binding: Mapping[str, Any], *, query: Any, scope: Any,
    compiled: CompiledPredicate, source: str, result_unit: str,
    membership_target: str, max_bytes: int,
    ordering_identity: str | None = None,
) -> ScanContext:
    """Extend a real continuation_binding without dropping any authority part.

    The complete query/scope is fingerprinted, not retained as source text.
    Adapter original IDs, budget and path stay in the existing projection.
    An ordering identity must represent actual source order evidence, not merely
    the query fingerprint. Leave it absent for unproved native BM25 traversal.
    """
    if not isinstance(binding, Mapping) or not _REQUIRED_BINDING.issubset(binding):
        raise ValueError("scanner requires the full existing continuation binding")
    missing = sorted(name for name in _REQUIRED_BINDING - _NULLABLE_BINDING
                     if binding[name] is None)
    if missing:
        raise ValueError("scanner binding parts must not be null: " + ", ".join(missing))
    if not isinstance(compiled, CompiledPredicate):
        raise ValueError("scanner requires a compiled predicate")
    for name, value in (("source", source), ("result_unit", result_unit),
                        ("membership_target", membership_target)):
        if not _nonempty_text(value):
            raise ValueError(f"scanner {name} must be a nonempty string")
    if ordering_identity is not None and not _nonempty_text(ordering_identity):
        raise ValueError("scanner ordering identity must be a nonempty string or absent")
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < _MIN_BUDGET_BYTES:
        raise ValueError(f"scanner budget must be at least {_MIN_BUDGET_BYTES} bytes")
    _require_canonical("query", query)
    _require_canonical("scope", scope)
    identity = {"query": query, "scope": scope, "predicate": compiled.predicate,
                "compiler_version": compiled.compiler_version,
                "field_map_identity": compiled.field_map_identity,
                "compiler_fingerprint": compiled.fingerprint, "source": source,
                "result_unit": result_unit, "membership_target": membership_target,
                "authority_binding": dict(binding), "order": ordering_identity}
    digest = hashlib.sha256(_canonical(identity)).hexdigest()
    projection = copy.deepcopy(binding.get("projection"))
    if not isinstance(projection, dict):
        raise ValueError("scanner projection must be an object")
    if "scanner" in projection:
        raise ValueError("scanner projection namespace is reserved")
    projection.update(max_bytes=max_bytes, scanner={
        "identity": digest, "source": source, "result_unit": result_unit,
        "membership_target": membership_target, "order": ordering_identity,
    })
    bound = {**copy.deepcopy(dict(binding)), "projection": projection}
    return ScanContext(_canonical(bound), digest, source, result_unit,
                       membership_target, ordering_identity, max_bytes)


@dataclass(frozen=True)
class EnumerationEvidence:
    """Positive proof that a named domain was traversed to its end.

    ``finished=False`` names prefilter candidates only. Only
    :func:`finish_scan_batch` produces ``finished=True`` evidence, after every
    candidate's authorization and residual finishing completed; the
    constructor refuses ``finished=True`` without its private seal. The seal
    is not an init field, so ``dataclasses.replace`` on finished evidence
    refuses rather than carrying the seal to a new domain or scope. Copies
    and pickles keep it (they restore state without calling ``__init__``).

    ``method`` is asserted by the adapter, not observed: the type cannot see
    the SQL it ran. Each adapter emitting ``whole_sql`` or ``terminal_keyset``
    must be reviewed against its actual statement.
    """
    domain: str
    scope_identity: str
    method: str
    finished: bool = False
    _seal: Any = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if (not _nonempty_text(self.domain) or not _nonempty_text(self.scope_identity)
                or self.method not in {"whole_sql", "terminal_keyset"}):
            raise ValueError("enumeration requires a named scope and actual terminal SQL traversal")
        if self.finished is not False and (
                self.finished is not True or self._seal is not _FINISH_SEAL):
            raise ValueError("only finish_scan_batch can mark enumeration finished")


@dataclass(frozen=True)
class ScanBatch:
    """Adapter-owned rows may be nodes, occurrences or edge-position payloads."""
    rows: tuple[Mapping[str, Any], ...]
    source_cursor: Mapping[str, Any] | None = None
    count_unit: str = "row"
    count_set: str = "prefilter_candidates"
    count_basis: str = "unknown"
    total_count: int | None = None
    work_bounds: tuple[str, ...] = ()
    enumeration: EnumerationEvidence | None = None
    terminal: bool = False
    approximate: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.rows, (tuple, list)) or not all(
                isinstance(row, Mapping) for row in self.rows):
            raise ValueError("scanner rows must be a sequence of mappings")
        object.__setattr__(self, "rows", tuple(self.rows))
        if isinstance(self.work_bounds, str) or not all(
                _nonempty_text(bound) for bound in self.work_bounds):
            raise ValueError("scanner work bounds must name each actual stop")
        object.__setattr__(self, "work_bounds", tuple(self.work_bounds))
        if not _nonempty_text(self.count_unit) or not _nonempty_text(self.count_set):
            raise ValueError("scanner counts must name their unit and set")
        if self.count_basis not in {"exact", "lower_bound", "unknown"}:
            raise ValueError("invalid scanner count basis")
        if self.count_basis == "unknown" and self.total_count is not None:
            raise ValueError("unknown scanner totals must be null")
        if self.count_basis != "unknown" and self.total_count is None:
            raise ValueError("exact or lower-bound scanner totals must be stated")
        if self.total_count is not None and (not isinstance(self.total_count, int)
                or isinstance(self.total_count, bool) or self.total_count < 0):
            raise ValueError("scanner count must be nonnegative or null")
        if self.count_basis == "exact" and (self.work_bounds or self.approximate):
            # A timeout or other work stop leaves the population unseen: it may
            # report what it saw as a lower bound, never as an exact total.
            raise ValueError("bounded or approximate traversal cannot report an exact total")
        if self.enumeration is not None:
            if not isinstance(self.enumeration, EnumerationEvidence):
                raise ValueError("scanner enumeration must be EnumerationEvidence")
            if not self.terminal or self.work_bounds or self.approximate:
                raise ValueError("bounded or approximate traversal cannot prove exhaustion")
            if self.enumeration.finished:
                raise ValueError("a source batch proves candidates only; finishing proves matches")


@dataclass(frozen=True)
class FinishedCandidate:
    rows: tuple[Mapping[str, Any], ...] = ()
    auxiliary: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class FinishedScan:
    rows: tuple[Mapping[str, Any], ...]
    auxiliary: tuple[Mapping[str, Any], ...]
    candidates_finished: int
    batch: ScanBatch
    enumeration: EnumerationEvidence | None


def finish_scan_batch(
    context: ScanContext, batch: ScanBatch,
    finish: Callable[[Mapping[str, Any]], FinishedCandidate | None], *,
    finished_domain: str | None = None,
) -> FinishedScan:
    """Finish every candidate using the adapter's existing read authority.

    Auxiliary parent/member or sealed raw-position rows stay separate from
    delivered output units. This does not count or deduplicate records; adapters
    own grouping. Exceptions (a statement timeout included) propagate: a failed
    finish never becomes empty exact output.
    """
    evidence = batch.enumeration
    if evidence and evidence.scope_identity != context.scope_identity:
        raise ValueError("enumeration proof belongs to another scanner scope")
    if finished_domain is not None and not _nonempty_text(finished_domain):
        raise ValueError("a finished domain must be named")
    rows: list[Mapping[str, Any]] = []
    auxiliary: list[Mapping[str, Any]] = []
    candidates = copy.deepcopy(batch.rows)
    for candidate in candidates:
        value = finish(candidate)
        if value is None:
            continue
        if not isinstance(value, FinishedCandidate):
            raise ValueError("finish must return FinishedCandidate or None")
        rows.extend(copy.deepcopy(tuple(value.rows)))
        auxiliary.extend(copy.deepcopy(tuple(value.auxiliary)))
    # Only terminal prefilter enumeration plus every finish completing proves
    # the visible-match domain. Candidate counts alone never promote.
    finished_proof = None
    if evidence and finished_domain:
        finished_proof = EnumerationEvidence(
            finished_domain, evidence.scope_identity, evidence.method)
        object.__setattr__(finished_proof, "_seal", _FINISH_SEAL)
        object.__setattr__(finished_proof, "finished", True)
    return FinishedScan(tuple(rows), tuple(auxiliary), len(candidates), batch, finished_proof)


def scan_token_body_bytes(token: str) -> int:
    """Decoded UTF-8 body size of a continuation token (no signature check)."""
    encoded = token.split(".", 1)[0]
    raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    inflater = zlib.decompressobj()
    body = inflater.decompress(raw, MAX_SCAN_BODY_BYTES + 1)
    if inflater.unconsumed_tail or not inflater.eof:
        return MAX_SCAN_BODY_BYTES + 1
    return len(body)


def issue_scan_continuation(
    context: ScanContext, cursor: Mapping[str, Any], *,
    measure: Callable[[str], int],
) -> str:
    """Issue only usable state that fits both state and actual wire budgets.

    ``measure`` returns the actual response size with this token in place; it
    is compared against the original budget, never a token truncation.
    """
    if context.ordering_identity is None:
        raise ScanContinuationUnavailable("source ordering has no continuation proof",
                                          reason="ordering_unproven")
    if not isinstance(cursor, Mapping):
        raise ValueError("scanner cursor must be a mapping")
    token = issue_continuation(context.binding, cursor=cursor)
    if len(token) > MAX_SCAN_TOKEN_CHARS:
        raise ScanContinuationUnavailable("scanner token exceeds the parser bound",
                                          reason="token_bound", max_token_chars=MAX_SCAN_TOKEN_CHARS)
    if scan_token_body_bytes(token) > MAX_SCAN_BODY_BYTES:
        raise ScanContinuationUnavailable("scanner state exceeds its byte bound",
                                          reason="state_bound", max_body_bytes=MAX_SCAN_BODY_BYTES)
    # Round-trip through the real receiver before handing the token out.
    if consume_scan_continuation(context, token) != json.loads(_canonical(dict(cursor))):
        raise ScanContinuationUnavailable("scanner cursor does not round-trip",
                                          reason="cursor_not_canonical")
    if measure(token) > context.max_bytes:
        raise ScanContinuationUnavailable("scanner continuation does not fit the response budget",
                                          reason="output_bound", max_bytes=context.max_bytes)
    return token


def consume_scan_continuation(context: ScanContext, token: str) -> dict[str, Any]:
    """Verify signature, complete binding and the receiver-side state bound."""
    cursor = consume_continuation(token, expected_binding=context.binding)
    if context.ordering_identity is None or scan_token_body_bytes(token) > MAX_SCAN_BODY_BYTES:
        raise ContinuationTokenInvalid(
            "the continuation token exceeds this scanner's state contract",
            data={"reason": "state_bound",
                  "pinned_generation_id": context.binding.get("generation"),
                  "authority_fingerprint": context.binding.get("authority")},
            fix_hint="restart the read without a continuation token",
        )
    return cursor
