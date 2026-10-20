"""The BLIND applier for `manifest_edits` descriptors.

`pact.yaml` is authored, not generated: no `okg pact` verb writes the fields a
manifest-content refusal names, so those refusals are AUTHOR ACTIONS. The
design's claim is that an author action can still be *mechanically* repaired —
that the refusal carries enough to apply the edit without reading anything
else. This module is the instrument that tests that claim, and it is blind on
purpose:

* it imports NOTHING from ``okg`` — not the models it edits, not the validator
  whose codes it reads, not the schema. Its only inputs are the descriptor
  fields the refusal published (``pointer``, ``action``, ``legal_values``,
  ``offending_value``) and the YAML document itself. A test pins that import
  graph, because an applier that peeked at the models would prove the models
  are complete rather than that the REFUSAL is;
* there is ONE code path for every validation code. Nothing here branches on
  ``code``: a per-code repair table would mean the applier knows the repairs
  and the refusal only has to name a code — the opposite of the contract;
* an underdetermined descriptor is a FAILURE, never a guess. There is no
  ``legal_values[0]`` fallback: picking the first member of a 20-kind enum
  produces an edit that clears the code while asserting something nobody
  checked, which is the same class of defect as a fabricated proof row.

Success is judged by the CALLER (re-validate, no new codes, no evidence rows
added, manifest quality not lower) — this module only applies and reports.
"""
from __future__ import annotations

import copy
import difflib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml

# Why an edit could not be applied. Reported, never swallowed: a descriptor
# the applier cannot use is a hole in the REFUSAL, and it has to be visible as
# one.
UNDERDETERMINED = "underdetermined_descriptor"
NO_POINTER = "descriptor_has_no_pointer"
POINTER_UNRESOLVED = "pointer_does_not_resolve"
VALUE_NOT_AT_POINTER = "offending_value_not_at_pointer"

# A difflib hit below this is not a correction, it is a coincidence. Mirrors
# `repair.NEAR_MISS_CUTOFF` — the cutoff the EMITTER uses to decide whether a
# candidate list is a correction or a menu — restated rather than imported
# because this module imports nothing from `okg`. A test pins the two equal, so
# the mirror cannot drift.
MATCH_CUTOFF = 0.55

_INDEX = re.compile(r"^(?P<name>[^\[\]]+)(?P<indices>(\[\d+\])*)$")


class Underdetermined(Exception):
    """The descriptor does not determine a single edit."""


@dataclass
class AppliedEdit:
    code: str
    pointer: str
    before: Any
    after: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "pointer": self.pointer,
            "before": self.before,
            "after": self.after,
        }


@dataclass
class FailedEdit:
    code: str
    pointer: str
    reason: str
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "pointer": self.pointer,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass
class ApplyResult:
    applied: list[AppliedEdit] = field(default_factory=list)
    failed: list[FailedEdit] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Every descriptor applied. A partial apply is not a repair."""
        return bool(self.applied) and not self.failed

    def to_dict(self) -> dict[str, Any]:
        return {
            "applied": [row.to_dict() for row in self.applied],
            "failed": [row.to_dict() for row in self.failed],
            "ok": self.ok,
        }


def parse_pointer(pointer: str) -> list[Any]:
    """``requirements[0].evidence_policy.required`` -> tokens.

    Dotted segments and bracket indices only. An id-addressed pointer
    (``tasks.task.one``) is deliberately unparseable here: ids contain dots, so
    a walker cannot tell a key from an id, and the EMITTER is responsible for
    publishing the index form.
    """
    tokens: list[Any] = []
    for segment in str(pointer).split("."):
        if not segment:
            raise Underdetermined(f"empty pointer segment in {pointer!r}")
        match = _INDEX.match(segment)
        if match is None:
            raise Underdetermined(f"unparseable pointer segment {segment!r}")
        tokens.append(match.group("name"))
        for index in re.findall(r"\[(\d+)\]", match.group("indices") or ""):
            tokens.append(int(index))
    return tokens


def _walk(document: Any, tokens: list[Any]) -> tuple[Any, Any]:
    """Return ``(container, key)`` for the pointer's LAST token."""
    node = document
    for token in tokens[:-1]:
        try:
            node = node[token]
        except (KeyError, IndexError, TypeError) as exc:
            raise LookupError(f"{token!r}: {exc}") from exc
    last = tokens[-1]
    if isinstance(last, int):
        if not isinstance(node, list) or last >= len(node):
            raise LookupError(f"index {last} out of range")
    elif not isinstance(node, dict) or last not in node:
        raise LookupError(f"key {last!r} absent")
    return node, last


def choose_value(offending: Any, legal: list[str]) -> str:
    """The ONE legal value this descriptor determines, or refuse.

    Two determinate cases, and nothing else:

    * the legal set has exactly one member — the descriptor named the answer;
    * the offending value has exactly one close match in the legal set — a
      typo with one correction.

    Everything else raises. A 20-member enum with no offending value to match
    against does not determine an edit, and an applier that picked anyway would
    be authoring, not repairing.
    """
    values = [str(value) for value in legal or [] if str(value)]
    if not values:
        raise Underdetermined("descriptor carries no legal values")
    if len(values) == 1:
        return values[0]
    if offending in (None, "", [], {}):
        raise Underdetermined(
            f"{len(values)} legal values and no offending value to match"
        )
    close = difflib.get_close_matches(str(offending), values, n=3, cutoff=MATCH_CUTOFF)
    if len(close) == 1:
        return close[0]
    raise Underdetermined(
        f"{len(close)} close matches for {offending!r} among {len(values)} "
        "legal values"
    )


def apply_edit(document: dict[str, Any], edit: dict[str, Any]) -> AppliedEdit:
    """Apply ONE descriptor to ``document`` in place. Raises on any doubt.

    The single code path, keyed on the descriptor's own fields:

    * the pointer resolves to a LIST containing the offending value — replace
      that element;
    * the pointer resolves to a SCALAR equal to the offending value — replace
      it;
    * the pointer resolves to an EMPTY list or a blank scalar (the
      subject-deletion codes: an unlinked task, an undeclared policy) — fill
      it with the determined value.
    """
    pointer = str(edit.get("pointer") or "")
    if not pointer:
        raise Underdetermined("descriptor has no pointer")
    tokens = parse_pointer(pointer)
    container, key = _walk(document, tokens)
    current = container[key]
    offending = edit.get("offending_value")
    legal = list(edit.get("legal_values") or [])

    if isinstance(current, list):
        before = list(current)
        if isinstance(offending, list) or offending is None:
            # Subject deletion: the list itself is the offending value.
            if current:
                raise Underdetermined(
                    f"pointer {pointer} holds {len(current)} entries; the "
                    "descriptor names no element to replace"
                )
            container[key] = [choose_value(None, legal)]
        elif offending in current:
            replacement = choose_value(offending, legal)
            container[key] = [
                replacement if entry == offending else entry for entry in current
            ]
        else:
            raise LookupError(
                f"{offending!r} is not in the list at {pointer}"
            )
        return AppliedEdit(
            code=str(edit.get("code") or ""),
            pointer=pointer,
            before=before,
            after=container[key],
        )

    before = current
    if offending is not None and current != offending and str(current).strip():
        raise LookupError(f"{pointer} does not hold {offending!r}")
    container[key] = choose_value(
        offending if str(offending or "").strip() else None, legal
    )
    return AppliedEdit(
        code=str(edit.get("code") or ""),
        pointer=pointer,
        before=before,
        after=container[key],
    )


def apply_edits(document: dict[str, Any], edits: Iterable[dict[str, Any]]) -> ApplyResult:
    """Apply every descriptor to a COPY-SAFE document, reporting each outcome.

    Edits are applied to ``document`` in place; the caller owns the copy. Every
    failure is recorded with its reason — silence here would let an unapplied
    descriptor read as a repaired one.
    """
    result = ApplyResult()
    for edit in edits or ():
        code = str((edit or {}).get("code") or "")
        pointer = str((edit or {}).get("pointer") or "")
        try:
            result.applied.append(apply_edit(document, dict(edit or {})))
        except Underdetermined as exc:
            result.failed.append(
                FailedEdit(code=code, pointer=pointer, reason=UNDERDETERMINED,
                           detail=str(exc))
            )
        except LookupError as exc:
            reason = POINTER_UNRESOLVED if not pointer else VALUE_NOT_AT_POINTER
            result.failed.append(
                FailedEdit(code=code, pointer=pointer, reason=reason,
                           detail=str(exc))
            )
    return result


def apply_to_file(
    path: Path,
    edits: Iterable[dict[str, Any]],
) -> tuple[ApplyResult, dict[str, Any]]:
    """Load, apply, and (only on a clean apply) write the manifest back.

    A partial apply leaves the file untouched: writing half a repair produces
    a manifest neither the author nor the applier intended.
    """
    document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        return (
            ApplyResult(
                failed=[FailedEdit(code="", pointer="", reason=POINTER_UNRESOLVED,
                                   detail="manifest is not a mapping")]
            ),
            {},
        )
    working = copy.deepcopy(document)
    result = apply_edits(working, edits)
    if result.ok:
        Path(path).write_text(
            yaml.safe_dump(working, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )
    return result, working
