"""Retraction-safety guard (M3.5).

Per the source contracts and docs/develop/architecture/consistency.md
"Retraction safety invariant": each profile constrains when a run
may emit retraction facts (op='R'). The actual emitter (lands
later, with M5 reconcile) MUST call `assert_retraction_permitted`
before writing any retraction; if the guard refuses, the writer
raises `RetractionNotPermitted` and the run aborts that batch.

The rules implemented here are the literal four bullets from
docs/develop/architecture/consistency.md:

  1. mutable_api: cursor-only run produces zero retractions.
     Only RECONCILE mode is permitted to retract.
  2. discovery_crawl: requires a COMPLETED scope walk; an
     incomplete crawl produces zero retractions.
  3. append_feed: produces zero retractions ever.
  4. reference_catalog: requires a NEWER release observed; a run
     against the same release produces zero retractions.
  5. live_overlay: profile bypasses fact emission entirely;
     retraction is structurally meaningless and refused.

The `RunMode` enum names the run-shape the caller is performing.
The guard takes (profile, mode) plus optional context (e.g.
`release_observed_newer=True/False` for reference_catalog) and
returns a verdict.

The verdict is structural: this module does NOT know whether the
caller actually has a retraction to emit; it only certifies that
emission would be policy-legal in the given mode. The caller still
has to compute whether records are missing.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from okg.substrate.errors import RetractionNotPermitted
from okg.substrate.sources.profiles import PROFILE_NAMES


class RunMode(str, Enum):
    """The shape of work the caller is performing.

    Profiles consume different subsets — e.g. mutable_api cares
    about CURSOR vs RECONCILE; discovery_crawl cares about
    SCOPE_COMPLETE vs SCOPE_INCOMPLETE.
    """

    # mutable_api modes
    CURSOR = "cursor"
    RECONCILE = "reconcile"

    # discovery_crawl modes
    SCOPE_COMPLETE = "scope_complete"
    SCOPE_INCOMPLETE = "scope_incomplete"

    # reference_catalog modes
    RELEASE_NEW = "release_new"            # newer release observed
    RELEASE_UNCHANGED = "release_unchanged"

    # append_feed mode (single, no retractions)
    APPEND = "append"

    # Catalog-edit mode (M5.4): the operator explicitly ran
    # `okg catalog load --reconcile`. These retractions are
    # AUTHORED, not observed, and always permitted.
    CATALOG_EDIT = "catalog_edit"


def retraction_permitted(
    *,
    source_class: str,
    run_mode: RunMode,
    bounded_record_set_authority: bool = False,
) -> tuple[bool, Optional[str]]:
    """Pure verdict: (permitted, reason_if_refused).

    `reason_if_refused` is None when permitted is True; it carries
    a human-readable explanation otherwise.

    `bounded_record_set_authority` names a different kind of
    authority from the scope walk. The rules above refuse an
    incomplete discovery crawl because ABSENCE from a partial walk
    proves nothing. An explicit RecordSet does not argue from
    absence: the adapter declares exactly which prior record keys
    this run replaces, so the retraction is bounded to those keys
    and every other prior record is carried forward untouched. The
    caller may also pass this flag for a set the adapter says it
    merged the durable prior into, but only after the runner has
    compared that set against the durable prior and found every
    out-of-scope prior key present. A bare `completed_scope=True`
    on a narrowed run is never enough. Issue #2145: without this
    distinction a scoped re-ingest after a delete retracted
    nothing and the graph kept serving the removed file.
    """
    # `catalog_edit` is a synthetic profile for M5.4 reconcile
    # retractions — authored by operator, always permitted.
    if source_class == "catalog_edit":
        if run_mode == RunMode.CATALOG_EDIT:
            return True, None
        return False, (
            "catalog_edit requires run_mode=CATALOG_EDIT; got "
            f"{run_mode.value!r}"
        )

    if source_class not in PROFILE_NAMES:
        return False, (
            f"unknown source_class {source_class!r}; valid profiles: "
            f"{sorted(PROFILE_NAMES)}"
        )

    if source_class == "mutable_api":
        if run_mode == RunMode.RECONCILE:
            return True, None
        if run_mode == RunMode.CURSOR:
            return False, (
                "mutable_api in cursor mode cannot emit retractions; "
                "cursor-only runs are blind to deletes. Run in "
                "reconcile mode to retract."
            )
        return False, (
            f"mutable_api: run_mode {run_mode.value!r} is not a "
            f"valid mode for this profile (use cursor or reconcile)"
        )

    if source_class == "discovery_crawl":
        if run_mode == RunMode.SCOPE_COMPLETE:
            return True, None
        if run_mode == RunMode.SCOPE_INCOMPLETE:
            if bounded_record_set_authority:
                return True, None
            return False, (
                "discovery_crawl with an incomplete scope walk cannot "
                "emit retractions; absence-from-scope is meaningful "
                "only when the scope walk is complete."
            )
        return False, (
            f"discovery_crawl: run_mode {run_mode.value!r} is not a "
            f"valid mode for this profile (use scope_complete or "
            f"scope_incomplete)"
        )

    if source_class == "reference_catalog":
        if run_mode == RunMode.RELEASE_NEW:
            return True, None
        if run_mode == RunMode.RELEASE_UNCHANGED:
            return False, (
                "reference_catalog without a newer release observed "
                "cannot emit retractions; absence is meaningful only "
                "across release boundaries."
            )
        return False, (
            f"reference_catalog: run_mode {run_mode.value!r} is not "
            f"valid (use release_new or release_unchanged)"
        )

    if source_class == "append_feed":
        # No mode permits retraction. The append_feed profile is
        # append-only by definition; restatements come via
        # restatement_version, not op='R'.
        return False, (
            "append_feed never emits retractions (deletion_semantics="
            "'append_only'); restatements use restatement_version."
        )

    if source_class == "live_overlay":
        return False, (
            "live_overlay sources bypass fact emission entirely; "
            "retraction is structurally meaningless for an overlay."
        )

    if source_class == "registry_only":
        return False, (
            "registry_only sources have no runnable factory; they "
            "cannot emit any facts, including retractions."
        )

    return False, f"no retraction policy defined for {source_class!r}"


def assert_retraction_permitted(
    *,
    source_class: str,
    run_mode: RunMode,
    source_name: Optional[str] = None,
    bounded_record_set_authority: bool = False,
) -> None:
    """Raise RetractionNotPermitted if the rule refuses.

    `source_name` is included in the error data for runbook lookup.
    """
    ok, reason = retraction_permitted(
        source_class=source_class,
        run_mode=run_mode,
        bounded_record_set_authority=bounded_record_set_authority,
    )
    if ok:
        return
    raise RetractionNotPermitted(
        f"retraction refused for source_class={source_class!r} "
        f"in mode {run_mode.value!r}: {reason}",
        data={
            "source_class": source_class,
            "run_mode": run_mode.value,
            "source_name": source_name,
            "bounded_record_set_authority": bounded_record_set_authority,
        },
        fix_hint=(
            "either run in a mode that permits retraction (e.g. "
            "reconcile / scope_complete / release_new), or remove the "
            "retraction emission from this run path"
        ),
    )
