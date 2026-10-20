"""Merge safety gate (worktree-fork-merge-back: decider-boundary + fail-closed-two-way).

A guarded entry that EVERY merge into a protected branch must pass. It enforces the
two authorization/soundness boundaries that ride OUTSIDE chronos's per-diff merge
(RowDiff carries no endpoint subtypes + MergeContext has no generation pin, so
per-diff narrowing is unimplementable -- catalog soundness rides the publish gate
over the staged candidate instead):

  1. fail-closed-two-way: refuse when source and target share NO base. Chronos's
     runtime masks this -- when the backend's three-way `merge_preview` returns None
     (no common ancestor), the runtime fabricates a preview from a raw diff with
     conflicts=[] and `merge_apply` silently applies it as a two-way OVERWRITE
     (re-applying inherited rows as additions; abort_on_conflict never fires). We
     detect it at the BACKEND `merge_preview` and refuse. This also catches the
     `okg branch compact`-then-merge case (compact severs fork lineage -> no base).

  2. decider-boundary: refuse merging into a protected branch (default) without a
     verified HUMAN decider -- the authorization boundary for landing fork work on
     default. Mirrors `okg admission decide`: a named human (not a system/agent/
     derived source), an explicit `accept`, and NOT a fast_track auto-accept.

This module is pure policy (no DB, no chronos): `ChronosGraphStore.merge` computes
`shared_base_exists` from the backend and calls `assert_merge_allowed`. Concurrency
(check-then-apply races) is handled by the later concurrent-merges-serialize task.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from okg.substrate.errors import SubstrateError

DEFAULT_BRANCH = "default"

# Branches whose published head is canonical: a merge INTO one of these requires a
# verified human decider. Forks (experiment:*), staging (staging:*), and quarantine
# (quarantine:*) are working branches -- no decider required to merge onto them.
PROTECTED_BRANCHES = frozenset({DEFAULT_BRANCH})

# Source/actor prefixes that are NOT a named human decider: system actors, enrichers,
# derived/extracted producers, and the agent write surface. A decider must be a human
# operator coming through `okg admission decide`, never one of these (the same stance
# as the trust-restamp boundary, which refuses null/system-prefix sources).
_NON_HUMAN_DECIDER_PREFIXES = (
    "_enricher:",
    "_system",
    "derived_",
    "extracted_",
    "system:",
    "service:",
    "agent:",
    "agent_memory",
    "bot:",
)


class MergeRefused(SubstrateError):
    """A merge was refused at the safety gate: a two-way fallback (no shared base) or
    a missing / invalid decider for a protected target. `data['reason']` discriminates."""

    code = "merge_refused"


@dataclass(frozen=True)
class MergeDecision:
    """A human decision authorizing a merge into a protected branch. Mirrors a decided
    escalation / `okg admission decide` record: a named decider + an explicit decision.
    A `fast_track` decision is an automated auto-accept, NOT a human authorization."""

    decided_by: str
    decision: str = "accept"
    fast_track: bool = False


def is_protected_target(target: str) -> bool:
    return target in PROTECTED_BRANCHES


def _is_named_human(actor: str | None) -> bool:
    if not actor:
        return False
    normalized = actor.strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    return not any(lowered.startswith(prefix) for prefix in _NON_HUMAN_DECIDER_PREFIXES)


def require_valid_decider(decision: MergeDecision | None, *, target: str) -> None:
    """Raise MergeRefused unless `decision` is a valid HUMAN accept authorizing a merge
    into the protected `target`. Deny-by-default: missing, fast_track, non-accept, or
    non-human deciders are all refused."""
    if decision is None:
        raise MergeRefused(
            f"merge into protected branch {target!r} requires a decider",
            fix_hint=(
                "supply a MergeDecision from `okg admission decide` "
                "(named human, --decision accept)"
            ),
            data={"target": target, "reason": "missing_decider"},
        )
    if decision.fast_track:
        raise MergeRefused(
            f"merge into protected branch {target!r} cannot be authorized by a "
            "fast_track (auto-accept) decision",
            data={
                "target": target,
                "reason": "fast_track_decider",
                "decided_by": decision.decided_by,
            },
        )
    if str(decision.decision).strip().lower() != "accept":
        raise MergeRefused(
            f"merge into protected branch {target!r} requires an explicit accept "
            f"decision, got {decision.decision!r}",
            data={
                "target": target,
                "reason": "non_accept_decision",
                "decision": decision.decision,
            },
        )
    if not _is_named_human(decision.decided_by):
        raise MergeRefused(
            f"merge into protected branch {target!r} requires a named human decider, "
            f"not {decision.decided_by!r}",
            data={
                "target": target,
                "reason": "non_human_decider",
                "decided_by": decision.decided_by,
            },
        )


def assert_merge_allowed(
    source: str,
    target: str,
    *,
    shared_base_exists: bool,
    decision: MergeDecision | None,
) -> None:
    """Pure policy. Raise MergeRefused if this merge must not proceed.

    1. fail-closed-two-way (ALL targets): no shared base -> refuse (would overwrite).
    2. decider-boundary (protected targets only): require a valid human decider.
    """
    if not shared_base_exists:
        raise MergeRefused(
            f"refusing merge {source!r} -> {target!r}: source and target share no base "
            "(two-way fallback would overwrite, not three-way merge)",
            fix_hint=(
                "a fork must share a base with its target; do NOT `okg branch compact` "
                "then merge (compact severs fork lineage)"
            ),
            data={
                "source": source,
                "target": target,
                "reason": "two_way_fallback_no_shared_base",
            },
        )
    if is_protected_target(target):
        require_valid_decider(decision, target=target)


# --------------------------------------------------------------------------- #
# Deletion authority (worktree-fork-merge-back: deletion-and-catalog-preconditions)
# --------------------------------------------------------------------------- #
# In a THREE-WAY merge changeset a change=="deleted" is, by backend construction, a
# deletion of a row present on the merge base (a fork-CREATED row deleted yields no diff:
# base_row is None so source==base==None produces nothing). So every "deleted" change in a
# merge into default retracts a DEFAULT-ORIGIN (inherited) row -- which must not land
# without an explicit per-delete opt-in. (before["source"] is only corroborating audit
# data; it is forgeable and never the gate.)


@dataclass(frozen=True)
class DeleteOptIn:
    """A per-delete authorization token: the exact (table, key) a merge is allowed to
    delete from default."""

    table: str
    key: dict[str, Any]


def _opt_in_matches(diff: Any, opt_ins: Iterable[DeleteOptIn]) -> bool:
    dtable = getattr(diff, "table", None)
    dkey = getattr(diff, "key", None)
    return any(o.table == dtable and o.key == dkey for o in opt_ins)


def _deleted_changes(changes: Iterable[Any]) -> list[Any]:
    return [d for d in changes if getattr(d, "change", None) == "deleted"]


def assert_deletions_allowed(
    changes: Iterable[Any],
    *,
    source: str,
    target: str,
    delete_opt_ins: Iterable[DeleteOptIn] = (),
) -> None:
    """Raise MergeRefused if a merge into a protected target deletes any default-origin
    row without a matching per-delete opt-in. Deletions onto working branches are
    unguarded; non-deletion changes are ignored."""
    if not is_protected_target(target):
        return
    opt_ins = tuple(delete_opt_ins)
    offending = [d for d in _deleted_changes(changes) if not _opt_in_matches(d, opt_ins)]
    if offending:
        raise MergeRefused(
            f"merge into {target!r} deletes {len(offending)} default-origin row(s) "
            "without a per-delete opt-in",
            fix_hint=(
                "pass a DeleteOptIn(table, key) per intended deletion, or route "
                "deletions to quarantine"
            ),
            data={
                "source": source,
                "target": target,
                "reason": "deletion_of_default_origin_row",
                "deleted_keys": [
                    {"table": getattr(d, "table", None), "key": getattr(d, "key", None)}
                    for d in offending
                ],
            },
        )


def partition_deletions(
    changes: Iterable[Any], *, delete_opt_ins: Iterable[DeleteOptIn] = ()
) -> tuple[list[Any], list[Any]]:
    """Pure partition for the quarantine path: (allowed, quarantine) where un-opted-in
    deletions go to quarantine and everything else (opted-in deletes + non-deletes) is
    allowed."""
    opt_ins = tuple(delete_opt_ins)
    allowed: list[Any] = []
    quarantine: list[Any] = []
    for diff in changes:
        if getattr(diff, "change", None) == "deleted" and not _opt_in_matches(diff, opt_ins):
            quarantine.append(diff)
        else:
            allowed.append(diff)
    return allowed, quarantine


# --------------------------------------------------------------------------- #
# Catalog precondition (worktree-fork-merge-back: deletion-and-catalog-preconditions)
# --------------------------------------------------------------------------- #


def require_no_catalog_drift(
    fork_base_cv: int | None, default_current_cv: int | None, *, target: str
) -> None:
    """Raise MergeRefused if a merge into a protected target would land a fork built on a
    different catalog than default's current one. Any drift (forward bump OR a since-
    retired base catalog) means a fork node could carry a subtype absent from default's
    active catalog, so the merge must rebase onto the current catalog first. An
    unrecorded/unresolvable base catalog fails closed (cannot prove conformance)."""
    if not is_protected_target(target):
        return
    if fork_base_cv is None:
        raise MergeRefused(
            f"merge into {target!r}: the fork's base catalog version is "
            "unrecorded/unresolvable; refusing (cannot prove catalog conformance)",
            data={"target": target, "reason": "catalog_base_unrecorded"},
            fix_hint="recreate or rebase the fork so its base generation resolves",
        )
    if default_current_cv is None or int(fork_base_cv) != int(default_current_cv):
        raise MergeRefused(
            f"merge into {target!r}: fork base catalog {fork_base_cv} != default current "
            f"catalog {default_current_cv}; rebase the fork onto the current catalog first",
            data={
                "target": target,
                "reason": "catalog_drift_requires_rebase",
                "fork_base_cv": fork_base_cv,
                "default_current_cv": default_current_cv,
            },
            fix_hint="okg branch compact / rebase the fork onto default, then re-merge",
        )


__all__ = [
    "DEFAULT_BRANCH",
    "PROTECTED_BRANCHES",
    "DeleteOptIn",
    "MergeDecision",
    "MergeRefused",
    "assert_deletions_allowed",
    "assert_merge_allowed",
    "is_protected_target",
    "partition_deletions",
    "require_no_catalog_drift",
    "require_valid_decider",
]
