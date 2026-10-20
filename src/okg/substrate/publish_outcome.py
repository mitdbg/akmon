"""What one publish did: the one typed result every command reads.

A publish report entry (``Publisher.cycle``'s summary, or the entry
``okg ingest`` adds to ``report["publishes"]``) means exactly one of:

* ``published``  -- a new graph version is live; ``generation_id`` names it.
* ``no_change``  -- nothing new; ``generation_id`` (when known) is the
  version that stays current.
* ``blocked``    -- a gate refused the version this run built; readers
  still see the previous one. ``failed_checks`` names the gates.
* ``failed``     -- the publish errored.
* ``skipped``    -- no publish ran; ``reason`` says why (for example
  ``no_source_work``: the run had nothing to import).
* ``unknown``    -- the entry does not say; callers fail closed.

A new version id is the proof of a publish, not a zero row delta: the
publisher can mint a new version whose own delta counters are zero (a
retirement, a refreshed policy) and readers still move to it (#2472). A
cycle is ``no_change`` only when the version stayed the same: the publisher
reused the current one, or the reported id equals its predecessor.

Blocked is decided before published: the publisher mints the version id
before its gates run, so an id alone never proves a publish.

This module imports nothing from ``okg``.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

PUBLISHED = "published"
NO_CHANGE = "no_change"
BLOCKED = "blocked"
FAILED = "failed"
SKIPPED = "skipped"
UNKNOWN = "unknown"

BLOCKED_STATUSES = frozenset({"blocked", "gate_blocked"})
FAILED_STATUSES = frozenset({"fail", "failed", "error", "errored"})
SKIPPED_STATUSES = frozenset({"skipped"})

#: The skip reason ``okg ingest`` reports when no selected source had work.
NO_SOURCE_WORK = "no_source_work"


@dataclass(frozen=True)
class PublishOutcome:
    """One publish's meaning; see the module docstring."""

    kind: str
    generation_id: Optional[str] = None
    previous_generation_id: Optional[str] = None
    failed_checks: tuple[str, ...] = ()
    reason: Optional[str] = None
    fix_hint: Optional[str] = None

    @property
    def readers_current(self) -> bool:
        """True when readers see a good version after this publish."""
        return self.kind in (PUBLISHED, NO_CHANGE)


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def failed_checks(blocked_reason: Any) -> tuple[str, ...]:
    """The gate names in a publisher ``blocked_reason``, sorted."""
    if isinstance(blocked_reason, Mapping):
        return tuple(sorted(str(key) for key in blocked_reason))
    if isinstance(blocked_reason, (list, tuple, set, frozenset)):
        return tuple(sorted(str(item) for item in blocked_reason))
    text = _text(blocked_reason)
    return (text,) if text else ()


def classify_publish(entry: Optional[Mapping[str, Any]]) -> PublishOutcome:
    """Classify one publish report entry. A missing entry is ``unknown``."""
    if not entry:
        return PublishOutcome(UNKNOWN, reason="no publish reported")
    status = str(entry.get("status") or "").strip().lower()
    generation_id = _text(entry.get("generation_id"))
    previous = _text(entry.get("predecessor_generation_id"))
    fix_hint = _text(entry.get("fix_hint"))
    blocked_reason = entry.get("blocked_reason")
    if (
        status in BLOCKED_STATUSES
        or bool(blocked_reason)
        or entry.get("phase_b_blocked") is True
    ):
        return PublishOutcome(
            BLOCKED, generation_id, previous,
            failed_checks=failed_checks(blocked_reason),
            reason=_text(entry.get("reason")), fix_hint=fix_hint,
        )
    if status in FAILED_STATUSES:
        return PublishOutcome(
            FAILED, generation_id, previous,
            reason=_text(entry.get("error")) or _text(entry.get("reason")),
            fix_hint=fix_hint,
        )
    if status in SKIPPED_STATUSES:
        return PublishOutcome(
            SKIPPED, generation_id, previous,
            reason=(
                _text(entry.get("reason"))
                or _text(entry.get("skip_reason"))
                or _text(entry.get("skipped"))
            ),
            fix_hint=fix_hint,
        )
    if status == "published":
        # The publisher's reuse path says so with ``reused_generation``; a
        # minted version always reports its predecessor, which is None on a
        # deployment's first publish. So a no-op flag with no predecessor is
        # a first version (an empty install's first publish), not a reuse:
        # readers move to it, and ``okg init`` requires it as the proof of
        # an empty first publish.
        same_version = entry.get("reused_generation") is True or (
            previous is not None and previous == generation_id
        )
        kind = NO_CHANGE if same_version else PUBLISHED
        return PublishOutcome(kind, generation_id, previous, fix_hint=fix_hint)
    return PublishOutcome(
        UNKNOWN, generation_id, previous,
        reason=f"unrecognized publish status {status or '<empty>'}",
        fix_hint=fix_hint,
    )


def describe_generation_command(generation_id: str, deployment: str) -> str:
    """The command that shows a version's gate results, blocked ones included."""
    return (
        f"okg generation describe {generation_id} --deployment {deployment} --json"
    )


__all__ = [
    "BLOCKED",
    "FAILED",
    "NO_CHANGE",
    "NO_SOURCE_WORK",
    "PUBLISHED",
    "PublishOutcome",
    "SKIPPED",
    "UNKNOWN",
    "classify_publish",
    "describe_generation_command",
    "failed_checks",
]
