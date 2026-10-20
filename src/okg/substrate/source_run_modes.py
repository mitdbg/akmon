"""The run-mode policy for each source profile.

This is the one table of which run modes a source profile accepts, which
mode reads its whole scope, and which modes complete its scope. The runner's
retraction and replay tables, the ingest per-source mode check, guided
add-source and the source-sync completed-scope set are all derived from it;
no other module keeps its own copy.

Callers ask for a source's whole scope with the generic request
``scope_complete``. Only ``discovery_crawl`` accepts that mode; every other
profile has its own whole-scope mode, and the runner refuses a retraction
under a (profile, mode) pair this table does not list.
:func:`resolve_whole_scope_request` turns the generic request into the
profile's own mode.

The module imports nothing from ``okg`` so that low-level modules
(``source_sync``) can read it without an import cycle.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType


#: The mode callers pass to ask for a source's whole scope, whatever its
#: profile.
WHOLE_SCOPE_REQUEST = "scope_complete"


@dataclass(frozen=True)
class ProfileRunModes:
    """The run modes one source profile accepts."""

    #: Every mode the runner has a retraction policy for.
    accepted: frozenset[str]
    #: The mode that reads the profile's whole scope.
    whole_scope: str
    #: The accepted modes whose run covers the whole scope.
    completes_scope: frozenset[str]

    def __post_init__(self) -> None:
        if self.whole_scope not in self.accepted:
            raise ValueError(f"whole-scope mode {self.whole_scope!r} is not accepted")
        if not self.completes_scope <= self.accepted:
            raise ValueError("a scope-completing mode must be accepted")


PROFILE_RUN_MODES: Mapping[str, ProfileRunModes] = MappingProxyType({
    "mutable_api": ProfileRunModes(
        accepted=frozenset({"cursor", "reconcile"}),
        whole_scope="reconcile",
        completes_scope=frozenset({"reconcile"}),
    ),
    "discovery_crawl": ProfileRunModes(
        accepted=frozenset({"scope_complete", "scope_incomplete"}),
        whole_scope="scope_complete",
        completes_scope=frozenset({"scope_complete"}),
    ),
    "reference_catalog": ProfileRunModes(
        accepted=frozenset({"release_new", "release_unchanged"}),
        whole_scope="release_new",
        completes_scope=frozenset({"release_new", "release_unchanged"}),
    ),
    # Append feeds never retract, but a durable change-identity latch still
    # needs one complete replay of the feed before it can be acknowledged.
    "append_feed": ProfileRunModes(
        accepted=frozenset({"append"}),
        whole_scope="append",
        completes_scope=frozenset(),
    ),
    # live_overlay and registry_only sources never run through the runner.
})

#: Run modes whose run covers a source's whole scope, for any profile.
COMPLETED_SCOPE_RUN_MODES: frozenset[str] = frozenset().union(
    *(modes.completes_scope for modes in PROFILE_RUN_MODES.values())
)


def accepted_modes(profile: str | None) -> frozenset[str]:
    """The modes ``profile`` accepts; empty for an unknown profile."""
    modes = PROFILE_RUN_MODES.get(str(profile)) if profile else None
    return modes.accepted if modes is not None else frozenset()


def accepts(profile: str | None, mode: str) -> bool:
    return mode in accepted_modes(profile)


def whole_scope_mode(profile: str | None) -> str | None:
    """The mode that reads ``profile``'s whole scope, or None if unknown."""
    modes = PROFILE_RUN_MODES.get(str(profile)) if profile else None
    return modes.whole_scope if modes is not None else None


def resolve_whole_scope_request(profile: str | None, mode: str) -> str:
    """Turn the generic whole-scope request into ``profile``'s own mode.

    Only :data:`WHOLE_SCOPE_REQUEST` is translated, and only when the
    profile is known and does not already accept it. Any other mode, and
    any mode for a source with no known profile, is returned unchanged.
    """
    if mode != WHOLE_SCOPE_REQUEST or accepts(profile, mode):
        return mode
    return whole_scope_mode(profile) or mode


def profile_mode_fix_hint(profile: str) -> str:
    """Name the modes a source profile accepts, whole-scope mode first."""
    whole = whole_scope_mode(profile)
    if whole is None:
        return "use the profile-native source run mode"
    accepted = sorted(accepted_modes(profile))
    return (
        f"a {profile} source reads its whole scope with --mode {whole} "
        f"(accepted modes: {', '.join(accepted)}); omit --mode to let "
        "okg ingest pick it per source"
    )


__all__ = [
    "COMPLETED_SCOPE_RUN_MODES",
    "PROFILE_RUN_MODES",
    "ProfileRunModes",
    "WHOLE_SCOPE_REQUEST",
    "accepted_modes",
    "accepts",
    "profile_mode_fix_hint",
    "resolve_whole_scope_request",
    "whole_scope_mode",
]
