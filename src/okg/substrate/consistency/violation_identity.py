"""Violation identity and the canonical order (spec slice 1, req.svc.identity).

PACT ``concord-spec-views-contract``, task.svc.implement. Normative design:
``designs/spec-schema.md`` v2 §3, implementing program design v8 §2.1
(witness keys and violation identity) and §4 step 3 (the canonical order).

Identity
--------
A **violation** is one equivalence class, never a row:

* Formal tier: ``(invariant id, version, subject, witness-key tuple)`` —
  the witness key is the declared subset of the property query's output
  columns EXCLUDING offending values, so a row re-observed with the same
  key and a different disagreeing value is the SAME violation re-observed.
* Judged tier: the witness key comes only from a REGISTERED deterministic
  parser over the recorded verdict text (regex/grammar — the registry
  refuses model-backed parsers at registration); parser failure or empty
  extraction falls back to the whole subject, so a subject carries at most
  one violation per judged invariant.

Every count is over these equivalence classes: splitting or duplicating
reports cannot mint extra violations (:func:`distinct_violations`).

Canonical order
---------------
Specified as math, not by library (slice doc §3): every item's sort key is
a tuple of canonical-JSON-encoded components compared BYTEWISE
(locale-free). An evaluation's key is ``(invariant id, subject id)``; a
violation's key is its witnessing evaluation's key EXTENDED by the witness
key tuple, so repair work sorts immediately after its evaluation by prefix
order; unequal-length tuples compare by the shorter-is-prefix rule.
:class:`CanonicalQueue` is a priority queue over this order — mid-pass
discoveries insert at canonical position, and the recorded property is
output-sequence identity under shuffled arrival order (the property test's
contract; insertion-timing invariance is NOT claimed — priority queues do
not have it).

The comparator ships as a data-format definition — the one code the parent
design permits the plan verifier to share with the optimizer, so sharing it
does not breach verifier independence.
"""
from __future__ import annotations

import heapq
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence

from okg.substrate.errors import SubstrateError

__all__ = [
    "CanonicalQueue",
    "PARSER_KINDS",
    "ViolationIdentity",
    "IdentityError",
    "WitnessParserRegistry",
    "canonical_sort_key",
    "compare_canonical",
    "distinct_violations",
    "formal_violation_identity",
    "judged_violation_identity",
]


class IdentityError(SubstrateError):
    """Identity or ordering could not be established — refuse, never guess."""


def _encode(component: Any) -> bytes:
    """One component's locale-free byte encoding (canonical JSON)."""
    return json.dumps(
        component, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


# --------------------------------------------------------------------------- #
# Identity.                                                                    #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ViolationIdentity:
    """One violation equivalence class. Hash/equality are the identity."""

    invariant_id: str
    invariant_version: int
    subject: str
    witness_key: tuple[str, ...]

    def sort_key(self) -> tuple[bytes, ...]:
        """The canonical-order key: the witnessing evaluation's key extended
        by the witness-key tuple (prefix rule ⇒ repair work sorts
        immediately after its evaluation)."""
        return (
            _encode(self.invariant_id),
            _encode(self.subject),
            *(_encode(c) for c in self.witness_key),
        )


def formal_violation_identity(
    *,
    invariant_id: str,
    invariant_version: int,
    subject: str,
    witness_key_columns: Sequence[str],
    row: Mapping[str, Any],
) -> ViolationIdentity:
    """Identity of one formal-tier offending row.

    Only the DECLARED witness-key columns participate; every other column
    (the offending values) is excluded, so a later row with the same key
    and a different disagreeing value collapses to the same identity. A
    declared column missing from the row REFUSES — identity is never
    guessed from a partial row."""
    values: list[str] = []
    for column in witness_key_columns:
        if column not in row:
            raise IdentityError(
                "formal witness-key column missing from the offending row — "
                "identity is never derived from a partial row",
                data={"invariant_id": invariant_id, "column": column,
                      "row_columns": sorted(map(str, row))},
            )
        values.append(str(row[column]))
    return ViolationIdentity(
        invariant_id=invariant_id,
        invariant_version=int(invariant_version),
        subject=subject,
        witness_key=tuple(values),
    )


# -- judged tier ------------------------------------------------------------- #
#: Admissible parser kinds. ``model`` is deliberately NOT here: a parser
#: that itself requires model interpretation would make identity as soft as
#: the judge (design v8 §2.1) — the registry refuses it at registration.
PARSER_KINDS = ("regex", "grammar")


class WitnessParserRegistry:
    """Registered deterministic witness parsers, addressed ``parser://<name>@v<N>``.

    A parser is a pure function ``verdict_text -> tuple[str, ...]`` (the
    extracted witness-key components; empty tuple = extracted nothing). It
    may raise — parser failure is the declared whole-subject fallback, not
    an error. Registration REFUSES kinds outside :data:`PARSER_KINDS`."""

    def __init__(self) -> None:
        self._parsers: dict[str, Callable[[str], tuple[str, ...]]] = {}

    def register(
        self, ref: str, kind: str, parser: Callable[[str], tuple[str, ...]]
    ) -> None:
        if kind not in PARSER_KINDS:
            raise IdentityError(
                "witness parser kind refused — a parser that requires model "
                "interpretation would make identity as soft as the judge "
                "(design v8 §2.1)",
                data={"ref": ref, "kind": kind,
                      "admissible": list(PARSER_KINDS)},
            )
        if not ref.startswith("parser://"):
            raise IdentityError(
                "witness parser refs are parser://<name>@v<N>",
                data={"ref": ref},
            )
        if ref in self._parsers:
            raise IdentityError(
                "witness parser ref already registered — versions are new "
                "refs, never in-place edits",
                data={"ref": ref},
            )
        self._parsers[ref] = parser

    def has(self, ref: str) -> bool:
        return ref in self._parsers

    def extract(self, ref: str, verdict_text: str) -> tuple[str, ...] | None:
        """The extracted witness key, or ``None`` for the whole-subject
        fallback (parser failure or empty extraction)."""
        parser = self._parsers.get(ref)
        if parser is None:
            raise IdentityError(
                "witness parser is not registered",
                data={"ref": ref, "registered": sorted(self._parsers)},
            )
        try:
            result = parser(verdict_text)
        except Exception:
            return None
        # a parser that returns the wrong TYPE is an authoring bug, not a
        # parse failure: a bare string would silently explode into
        # per-character key components (review finding EWD-SVC-8) — refuse
        # loudly instead of falling back.
        if isinstance(result, str) or not isinstance(result, (list, tuple)):
            raise IdentityError(
                "a witness parser must return a list/tuple of strings — a "
                "bare string would be exploded per character and corrupt "
                "identity silently",
                data={"ref": ref, "got_type": type(result).__name__},
            )
        components = []
        for component in result:
            if not isinstance(component, str) or not component:
                raise IdentityError(
                    "witness-key components must be non-empty strings",
                    data={"ref": ref,
                          "got": [type(c).__name__ for c in result]},
                )
            components.append(component)
        return tuple(components) or None


def judged_violation_identity(
    *,
    invariant_id: str,
    invariant_version: int,
    subject: str,
    verdict_text: str,
    witness_parser: str = "",
    registry: WitnessParserRegistry | None = None,
) -> ViolationIdentity:
    """Identity of one judged-tier violation.

    With no declared parser — or on parser failure / empty extraction — the
    witness key is the WHOLE SUBJECT (the empty tuple here: subject is
    already an identity component), so a subject carries at most one
    violation per judged invariant."""
    extracted: tuple[str, ...] | None = None
    if witness_parser:
        if registry is None:
            raise IdentityError(
                "a declared witness parser needs the registry",
                data={"witness_parser": witness_parser},
            )
        extracted = registry.extract(witness_parser, verdict_text)
    return ViolationIdentity(
        invariant_id=invariant_id,
        invariant_version=int(invariant_version),
        subject=subject,
        witness_key=extracted or (),
    )


def distinct_violations(
    identities: Iterable[ViolationIdentity],
) -> frozenset[ViolationIdentity]:
    """Counting is over equivalence classes: duplicates collapse, so clone/
    split mutations mint no credit."""
    return frozenset(identities)


# --------------------------------------------------------------------------- #
# The canonical order.                                                         #
# --------------------------------------------------------------------------- #
def canonical_sort_key(components: Sequence[Any]) -> tuple[bytes, ...]:
    """The data-format definition: canonical-JSON-encode each component and
    compare the tuples bytewise. An evaluation passes
    ``(invariant id, subject id)``; a violation passes that extended by its
    witness-key components."""
    if not components:
        raise IdentityError("a canonical sort key needs at least one component")
    return tuple(_encode(c) for c in components)


def compare_canonical(
    a: Sequence[bytes], b: Sequence[bytes]
) -> int:
    """-1 / 0 / +1 under the canonical order: componentwise bytewise, with
    the shorter-is-prefix rule for unequal lengths (an evaluation sorts
    immediately before its own violations)."""
    for x, y in zip(a, b):
        if x != y:
            return -1 if x < y else 1
    if len(a) == len(b):
        return 0
    return -1 if len(a) < len(b) else 1


@dataclass(order=True)
class _Entry:
    key: tuple[bytes, ...]
    tie: int
    item: Any = field(compare=False)


class CanonicalQueue:
    """A priority queue over the canonical order (design v8 §4, step 3).

    ``push`` admits mid-pass discoveries at canonical position — never at
    the back, never by discovery timing. Items with EQUAL keys pop in
    insertion order (the tie counter), which the determinism property makes
    irrelevant for real workloads: two distinct work items never share a
    full canonical key. The recorded property (the test replays it) is that
    the popped sequence is identical under shuffled arrival order within
    the initial batch and within each discovery step."""

    def __init__(self) -> None:
        self._heap: list[_Entry] = []
        self._tie = 0

    def push(self, components: Sequence[Any], item: Any) -> None:
        heapq.heappush(
            self._heap,
            _Entry(key=canonical_sort_key(components), tie=self._tie,
                   item=item),
        )
        self._tie += 1

    def pop(self) -> tuple[tuple[bytes, ...], Any]:
        if not self._heap:
            raise IdentityError("pop from an empty canonical queue")
        entry = heapq.heappop(self._heap)
        return entry.key, entry.item

    def __len__(self) -> int:
        return len(self._heap)

    def drain(self) -> list[Any]:
        out = []
        while self._heap:
            out.append(self.pop()[1])
        return out
