"""What ONE update attempt actually recorded, for the console's detail page.

PACT operator-console-live-updates. The attempts LIST answers "what happened
and what changed"; until this composer existed the detail page answered almost
nothing -- a headline, an id, one change line, and sentences about what is not
recorded. Measured against the published ``lane-d-home-render`` deployment on
2026-09-21, six facts ARE recorded per attempt and none of them reached a
browser:

* the checkpoint's own ``node_count`` / ``edge_count`` (the totals this version
  holds) and its ``projection_deltas`` (added, UPDATED and retracted, for nodes
  and edges separately -- the list's four-number delta is derived from the
  totals and cannot see an update);
* the maintained count rollup at this version AND at its predecessor, which
  turns "988 records added" into which record types, which relationship types
  and which sources moved;
* ``invariant_summary.consistency_gates`` -- the publication checks that ran,
  how long each took, and whether it could block;
* the per-producer runs in ``okg.derived_artifact_runs`` for this attempt;
* the predecessor the checkpoint names, and this attempt's position in the
  list;
* what the deployment was configured with when it ran -- attestations, Nomos
  bundles, excluded sources, the enricher governance record count and the
  catalog version.

Three rules run through all of it.

**Nothing is faked.** Every part that could not be read becomes a NAMED marker
inside its own section, carrying a reason code. A missing rollup does not blank
a section and does not 500 the whole read; it says
``rollup_unavailable_for_version`` and why.

**Nothing unsafe is served.** The gate summaries go through
``generations.safe_gate_summary`` -- the same allowlist the blocker detail
uses, so a gate cannot be safe in one place and unsafe in the other. The two
gate blobs measured live are 146,414 and 113,924 bytes of graph row ids and
``str(exc)``; they are withheld with their size stated. The rollup's own
failure carries ``str(exc)`` from psycopg in its ``detail`` (a host, an IP and
a role name), so only its ``reason`` CODE is served, never its detail.

**The cost is bounded.** One statement for the producer runs, one rollup read
per version (this one and its parent), and the same single history read the
list already does -- never one query per attempt, per gate or per source. The
rollup sections are capped at a stated ceiling with a ``capped`` flag.
"""

from __future__ import annotations

import dataclasses
import re
from typing import Any, Mapping

from chronos_core.branching import BranchNotFoundError

from okg.substrate.console_views._chronos import (
    GenerationCountsUnavailable,
    generation_counts,
)
from okg.substrate.console_views.console_count_rollup_read import (
    COUNT_KIND_SOURCE,
    COUNT_KIND_SUBTYPE,
    ConsoleCountRollupUnavailable,
    read_console_count_rollup,
    require_console_count_rollup_enabled,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    COUNT_KIND_EDGE_TYPE,
)
from okg.substrate.console_views.producer_identity import source_display_names
from okg.substrate.console_views.generations import (
    NO_PREDECESSOR_IN_WINDOW,
    _excluded_source_names,
    _is_servable_string,
    _view_runs_by_target,
    compose_attempt_row,
    safe_gate_summary,
)
from okg.substrate.errors import GenerationCompacted
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp.session import Session
from okg.substrate.console_views.generations import _parse_created_at
import psycopg


class MalformedAttemptId(ValueError):
    """The requested id is not the shape any checkpoint id can take.

    Separated from "not found" on purpose. An id carrying a scheme, a path or
    a quote is not an attempt this deployment has lost -- it is a request this
    read will not make, and saying so by name is what stops it being retried.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class AttemptNotFound(LookupError):
    """No attempt with this id in THIS deployment and branch.

    One exception for three cases -- no such id anywhere, an id belonging to
    another deployment, and an id on another branch -- because the answer must
    be identical for all three. A reader outside this deployment must not be
    able to tell "does not exist" from "exists, elsewhere": the difference is
    itself information about another deployment's history.
    """


#: Every checkpoint metadata key this page reads, declared so the bounded
#: reader projects them and only them.
#:
#: `compose_attempt_row`'s four are here because this page composes that same
#: row, and the seven after them are this page's own sections. They are
#: checked against the literals in this module's source, and against the
#: store's contract, by
#: `tests/substrate/console_views/test_generation_projection_contract.py`: a
#: key read here and not declared here would come back `None` from the
#: projected reader and the page would print "not recorded" for a fact the
#: deployment recorded.
ATTEMPT_DETAIL_METADATA_KEYS = (
    # `compose_attempt_row`, shared with the attempts list.
    "status",
    "target_generation_id",
    "blocked_reason",
    "excluded_sources",
    # This page's own sections.
    "predecessor_generation_id",
    "node_count",
    "edge_count",
    "projection_deltas",
    "invariant_summary",
    "enricher_governance",
    "deployment_attestations",
    "nomos_policy_bundle_ids",
    "catalog_version_id",
)

#: How far back this page's history read scans.
#:
#: The page needs FOUR rows out of the history: the attempt, the one either
#: side of it and its predecessor. Everything past that is scanned so the page
#: can say "attempt 3 of 88" without a second read -- and on okg-workspace
#: every one of those rows carries a blocked-reason document of tens of
#: thousands of bytes, so the scan's depth IS the read's cost. Measured
#: against the 88-checkpoint `default` branch on 2026-09-22, same keys, best
#: of three:
#:
#:     window 200 (whole branch)   2,103 ms
#:     window  48                    449 ms
#:     window  24                    296 ms
#:     window  12                     50 ms
#:
#: 24 is twice the attempts list's own page of 12, so the two newest pages
#: open from the window and an attempt older than that is fetched by id -- one
#: row rather than a deeper scan. It is a bound the page STATES: `neighbours`
#: carries `scanned_window` and `window_exhausted`, so a reader is never shown
#: the window's edge as the history's.
ATTEMPT_DETAIL_SCAN_WINDOW = 24

#: Why the neighbours are not served for an attempt fetched by id.
_NEIGHBOURS_OUTSIDE_WINDOW = "attempt_is_older_than_the_window_this_read_scanned"

#: The charset any checkpoint id this reader will look up is drawn from. No
#: `/`, no `@`, no `://`: an id that could be a DSN or a path is refused as
#: malformed rather than carried into a query and echoed back in a message.
#: 512 matches the endpoint's own max_length, so the two agree.
_ATTEMPT_ID_SHAPED = re.compile(r"^[A-Za-z0-9_.:+-]{1,512}$")

#: How many rows ONE rollup section will serve. Record types, relationship
#: types and sources are all unbounded in principle; a deployment with 5,000
#: subtypes must not push 5,000 rows into a detail page. Each section states
#: its own total beside what it shows, so a capped list never reads as the
#: whole of it.
ROLLUP_ROWS_PER_SECTION = 200

#: The rollup dimensions this read asks for, by section name. Asking for the
#: three together is ONE read of the maintained table per version.
_SECTION_KINDS = {
    "by_record_type": COUNT_KIND_SUBTYPE,
    "by_relationship_type": COUNT_KIND_EDGE_TYPE,
    "by_source": COUNT_KIND_SOURCE,
}

#: The fields of ``projection_deltas`` this read serves, by the name it serves
#: them under. ``updated`` is the one the list's derived delta cannot see: a
#: version whose totals did not move can still have rewritten every row in it.
_DELTA_FIELDS = (("added", "added"), ("updated", "updated"),
                 ("retracted", "retracted"))


def _marker(reason: str, **extra: Any) -> dict[str, Any]:
    """An unavailable part, named. Never a blank and never a zero."""
    return {"reason": reason, **extra}


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _rollup_marker(exc: ConsoleCountRollupUnavailable, *, reason: str) -> dict[str, Any]:
    """Why a rollup could not be read, as a CODE and nothing else.

    ``exc.detail`` is ``str(exc)`` from psycopg on the ``table_unregistered``
    path and names the host, the IP and the role. The discriminator
    (``checkpoint_absent`` / ``table_unregistered`` / ``no_sentinel``) is a
    fixed vocabulary this reader wrote, so it is the only part that is served.
    """
    return _marker(reason, rollup_reason=str(getattr(exc, "reason", "") or "unknown"))


def _read_rollup(
    conn: Any,
    *,
    generation_id: Any,
) -> tuple[dict[tuple[str, str], int] | None, ConsoleCountRollupUnavailable | None]:
    """The maintained counts at one version, or the loud failure that refused.

    The enablement check lives HERE and not at the top of the composer. It
    refuses by raising ``ConsoleCountRollupUnavailable``, so standing at the
    top it 500'd the whole read whenever the flag was off -- costing the
    operator the holds, the checks, the runs, the neighbours and the
    configuration as well, none of which come from the rollup. Inside this
    function it is one more reason a count SECTION could not be read, which
    is what the module docstring above has always claimed.
    """
    try:
        require_console_count_rollup_enabled(generation_id=generation_id)
        return read_console_count_rollup(
            conn,
            generation_id=generation_id,
            kinds=set(_SECTION_KINDS.values()),
        ), None
    except ConsoleCountRollupUnavailable as exc:
        return None, exc


def _count_section(
    kind: str,
    *,
    counts: dict[tuple[str, str], int] | None,
    counts_error: ConsoleCountRollupUnavailable | None,
    parent_counts: dict[tuple[str, str], int] | None,
    parent_marker: dict[str, Any] | None,
) -> dict[str, Any]:
    """One by-dimension section: this version's counts against the parent's.

    A row's ``parent_count`` is null and ``parent_available`` false whenever
    the parent side could not be read -- which is a different fact from a
    parent count of zero, and the two must not render the same. A name present
    here and absent from a readable parent IS a real zero on the parent side,
    and says so.
    """
    if counts is None:
        return {
            "rows": [],
            "shown": 0,
            "total": 0,
            "capped": False,
            "limit": ROLLUP_ROWS_PER_SECTION,
            "unavailable": _rollup_marker(
                counts_error, reason="rollup_unavailable_for_version",
            ) if counts_error is not None else _marker("rollup_unavailable_for_version"),
            # Always present, so the page reads ONE field instead of
            # inferring the fact from `version_published` and a row count.
            "unpublished_version": None,
        }
    mine = {value: count for (dimension, value), count in counts.items()
            if dimension == kind}
    parent_available = parent_counts is not None
    parent = {
        value: count for (dimension, value), count in (parent_counts or {}).items()
        if dimension == kind
    }
    # A name is a row when it is recorded at EITHER end.
    #
    # Built from this version's names alone, a retraction lost the very rows
    # that explain it. Measured on the published lane-d-home-render
    # deployment, 2026-09-21: attempt e71bec30ed62 retracted 988 records, all
    # of them from `deployment_configs`, whose count at that version is
    # therefore 0 -- so the source vanished from the section and the -988 with
    # it, leaving one row holding 735 with no change beside a header that said
    # -988 records. A section whose rows do not sum to the change the attempt
    # recorded is a section the reader cannot use.
    #
    # A name that is zero at both ends is cut: a row of two zeros states
    # nothing, and the parent rollup can carry one. Only when the parent WAS
    # read, though -- with no parent side there is no delta to be zero, and a
    # recorded count of zero at this version is still a fact this section
    # measured.
    names = set(mine) | (set(parent) if parent_available else set())
    ordered = sorted(
        (
            (name, mine.get(name, 0)) for name in names
            if not parent_available
            or mine.get(name, 0)
            or parent.get(name, 0)
        ),
        key=lambda item: (-item[1], item[0]),
    )
    rows = [
        {
            "name": name,
            "count": count,
            # Null, not zero, when the parent side could not be read. Zero is
            # a measured fact about the parent version; null is the absence of
            # one, and `parent_available` says which this is.
            "parent_count": parent.get(name, 0) if parent_available else None,
            "parent_available": parent_available,
        }
        for name, count in ordered[:ROLLUP_ROWS_PER_SECTION]
    ]
    section: dict[str, Any] = {
        "rows": rows,
        "shown": len(rows),
        "total": len(ordered),
        "capped": len(ordered) > ROLLUP_ROWS_PER_SECTION,
        "limit": ROLLUP_ROWS_PER_SECTION,
        "unavailable": None,
        "unpublished_version": None,
    }
    if parent_marker is not None:
        section["parent_unavailable"] = parent_marker
    return section


#: Measured on a real refused publish, 2026-09-21: a BLOCKED checkpoint has a
#: maintained count rollup, and it counts the records the refused cycle
#: staged. Its `page` count came back 3 against a parent of 2 for an attempt
#: that never published. The counts are real facts about that checkpoint and
#: are not withheld -- but a detail page that printed "3 pages, up from 2" for
#: a refused publish would be telling the operator the graph grew when it did
#: not. So every section that reads from a non-published checkpoint carries
#: this marker, and the view is obliged to say so on the page.
_UNPUBLISHED_MARKER_REASON = "counts_describe_an_unpublished_version"


def _holds(metadata: dict[str, Any]) -> dict[str, Any]:
    """What this version holds, and what the publish moved to get there.

    The totals and the projection deltas are two different measurements and
    both are served: the list's four-number delta is computed from consecutive
    TOTALS, so it cannot show an update -- a version that rewrote every row
    without changing its size looks identical to one that did nothing.
    """
    deltas = metadata.get("projection_deltas")
    out: dict[str, Any] = {
        "node_count": _int_or_none(metadata.get("node_count")),
        "edge_count": _int_or_none(metadata.get("edge_count")),
        "nodes": None,
        "edges": None,
        "unavailable": [],
        # Its own key, never an entry in `unavailable` above. The totals WERE
        # recorded; what this marker says is which version they describe, and
        # a page reading it out of `unavailable` prints it under "part of
        # this version's totals was not recorded", which is false.
        "unpublished_version": None,
    }
    if out["node_count"] is None:
        out["unavailable"].append(_marker("node_count_not_recorded"))
    if out["edge_count"] is None:
        out["unavailable"].append(_marker("edge_count_not_recorded"))
    if not isinstance(deltas, dict):
        out["unavailable"].append(_marker("projection_deltas_not_recorded"))
        return out
    for noun, prefix in (("nodes", "nodes"), ("edges", "edges")):
        moved: dict[str, Any] = {}
        missing: list[str] = []
        for served_as, recorded_as in _DELTA_FIELDS:
            value = _int_or_none(deltas.get(f"{prefix}_{recorded_as}"))
            moved[served_as] = value
            if value is None:
                missing.append(served_as)
        out[noun] = moved
        if missing:
            out["unavailable"].append(_marker(
                "projection_delta_field_not_recorded",
                scope=noun,
                fields=sorted(missing),
            ))
    return out


def _checks(metadata: dict[str, Any]) -> dict[str, Any]:
    """The publication checks that ran, through the blocker allowlist.

    Passed gates are served too: "three checks ran and all passed" is the
    answer to "what did this publish verify", and a page that showed only
    failures would show nothing on every published attempt. Each gate's
    ``details`` and ``preflight`` are withheld by ``safe_gate_summary`` with
    their byte size stated -- measured live at 146,414 and 113,924 bytes of
    graph row ids and exception text.
    """
    summary = metadata.get("invariant_summary")
    if not isinstance(summary, dict):
        return {"gates": [], "total": None, "passed": None, "blocked": None,
                "unavailable": _marker("invariant_summary_not_recorded")}
    gates = summary.get("consistency_gates")
    if not isinstance(gates, dict):
        return {"gates": [], "total": None, "passed": None, "blocked": None,
                "unavailable": _marker("consistency_gates_not_recorded")}
    results = gates.get("results")
    if not isinstance(results, (list, tuple)):
        # `total` and `passed` come out of the same blob this list does.
        # Keeping them while silently emptying the list prints "3 checks ran,
        # 3 passed" over no rows -- a summary of gates this read could not
        # see. The section is named instead and states no number.
        return {"gates": [], "total": None, "passed": None, "blocked": None,
                "unavailable": _marker("consistency_gates_malformed")}
    return {
        "gates": [safe_gate_summary(gate) for gate in results],
        "total": _int_or_none(gates.get("total")),
        "passed": _int_or_none(gates.get("passed")),
        "blocked": _int_or_none(gates.get("blocked")),
        "unavailable": None,
    }


#: The runs recorded between the previous version and this one, per source.
#: ONE grouped statement over `okg.v_source_run_diagnostics`, bounded by the
#: two checkpoints' own creation times -- never a statement per source.
_SOURCE_WRITES_SQL = """
SELECT
  COALESCE(registry_source_name, source_name) AS source_name,
  source_name AS ownership_id,
  count(*) AS runs,
  sum(nodes_appended) AS records_written,
  sum(edges_appended) AS relationships_written
FROM okg.v_source_run_diagnostics
WHERE deployment = %(deployment)s
  AND observed_at <= %(until)s
  AND (%(after)s::timestamptz IS NULL OR observed_at > %(after)s::timestamptz)
GROUP BY 1, 2
ORDER BY 1, 2
"""


def _source_writes(
    conn: Any,
    *,
    deployment_name: str,
    created_at: Any,
    previous_created_at: Any,
    previous: str,
) -> dict[str, Any]:
    """Which sources' runs wrote into this version, and how much.

    The projection delta (`holds`) is one number for the whole version: a
    version that rewrote 571 records says so and cannot say whose. The run
    rows can, but no run records the version it fed. What a run DOES record
    is when it finished, so this reads the runs recorded after the previous
    version was created and up to this one -- the only runs that can have fed
    it -- and serves that window beside the rows, so a reader sees the basis
    rather than a claim that each run is attributed.

    A run that wrote rows the publish found unchanged still counts here:
    these are writes, not the version's net change, and the page says so.

    ``previous`` is ``"read"`` when the version before this one was read,
    ``"none"`` when this is the first version (every earlier run fed it), and
    ``"unknown"`` when neither could be established -- then no window can be
    drawn and the section says so rather than counting the whole history.
    """
    until = _parse_created_at(created_at)
    after = _parse_created_at(previous_created_at) if previous == "read" else None
    base: dict[str, Any] = {
        "rows": [],
        "window": {
            "after": after.isoformat() if after else None,
            "until": until.isoformat() if until else None,
        },
        "basis": "source_runs_recorded_between_versions",
        "unavailable": None,
    }
    if until is None:
        base["unavailable"] = _marker("attempt_created_at_not_recorded")
        return base
    if previous == "unknown":
        base["unavailable"] = _marker("previous_version_not_read")
        return base
    if previous == "read" and after is None:
        base["unavailable"] = _marker("previous_version_created_at_not_recorded")
        return base
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    _SOURCE_WRITES_SQL,
                    {"deployment": deployment_name, "until": until, "after": after},
                )
                fetched = cur.fetchall()
    except psycopg.Error as exc:
        # The class only: a psycopg message can carry a host or a path.
        base["unavailable"] = _marker(
            "source_run_diagnostics_query_failed", error_type=type(exc).__name__,
        )
        return base
    base["rows"] = [
        {
            # The name Sources prints, and the runtime key the count rollup
            # (and so `by_source`) keys the same source by. The run row
            # records both, so a page can name a by-source row as Sources does.
            "source_name": str(row[0]),
            "ownership_id": str(row[1]) if row[1] is not None else None,
            "runs": int(row[2]),
            "records_written": int(row[3]) if row[3] is not None else None,
            "relationships_written": int(row[4]) if row[4] is not None else None,
        }
        for row in fetched
    ]
    return base


def _configured_names(value: Any, *, key: str) -> dict[str, Any]:
    """A recorded list of identifiers, code-shaped ones only.

    ``recorded`` distinguishes an empty list the deployment WROTE from a key
    it never wrote: "no attestation was configured" and "this checkpoint
    predates attestations" are different answers and the page must be able to
    say which. Entries that are not code-shaped are counted, not echoed.
    """
    if value is None:
        return {"recorded": False, "names": [], "withheld_count": 0}
    items: list[Any]
    if isinstance(value, dict):
        items = list(value.keys())
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        return {"recorded": True, "names": [], "withheld_count": 1,
                "unavailable": _marker("unexpected_shape")}
    names: list[str] = []
    withheld = 0
    for item in items:
        text = item if isinstance(item, str) else None
        if text is not None and _is_servable_string(key, text):
            names.append(text)
        else:
            withheld += 1
    return {"recorded": True, "names": names, "withheld_count": withheld}


def _configured(metadata: dict[str, Any]) -> dict[str, Any]:
    """What the deployment was configured with when this attempt ran.

    Every entry distinguishes "recorded as empty" from "not recorded at all".
    ``deployment_attestations`` is null on the live deployment measured, which
    is the first case, not the second, and the payload says so.
    """
    governance = metadata.get("enricher_governance")
    return {
        "deployment_attestations": _configured_names(
            metadata.get("deployment_attestations"), key="attestor_id",
        ),
        "nomos_policy_bundle_ids": _configured_names(
            metadata.get("nomos_policy_bundle_ids"), key="code",
        ),
        "excluded_sources": {
            "recorded": "excluded_sources" in metadata,
            "names": _excluded_source_names(metadata.get("excluded_sources")),
        },
        "enricher_governance_record_count": (
            _int_or_none(governance.get("record_count"))
            if isinstance(governance, dict) else None
        ),
        "catalog_version_id": _int_or_none(metadata.get("catalog_version_id")),
    }


def _neighbours(
    ordered: list[Any],
    index: int | None,
    metadata: dict[str, Any],
    *,
    total: int | None,
    total_basis: str,
    window_exhausted: bool,
    unavailable_reason: str | None = None,
) -> dict[str, Any]:
    """Where this attempt sits, and which version it built on.

    ``parent_generation_id`` prefers the predecessor the CHECKPOINT names over
    the attempt that happens to precede it in the list: a blocked attempt
    between two publishes sits in the list without being anybody's parent, and
    list order would name it as one. ``parent_source`` says which of the two
    answered.

    ``ordered`` is a BOUNDED window of the newest attempts now, not the whole
    history, so three of these fields can no longer be read off its length:

    * ``total`` is counted separately, and ``total_basis`` names the basis --
      ``scan_window`` when the backend could not count, in which case the
      number is what the window saw and no more.
    * ``position`` counts from the newest, which a newest-first window still
      answers exactly.
    * ``older_attempt_id`` is null at the END of the window for two different
      reasons: there is no older attempt, or there is one this read did not
      scan. ``older_attempt_unavailable_reason`` says which, so a page cannot
      draw "this is the oldest attempt" over a history it never reached.

    ``index is None`` is the attempt fetched by id from outside the window:
    its neighbours are simply not known, and every positional field is null
    beside ``unavailable_reason`` rather than guessed from a window that does
    not hold it.
    """
    recorded_parent = metadata.get("predecessor_generation_id")
    parent = recorded_parent if isinstance(recorded_parent, str) else None
    parent_source = "recorded_predecessor" if parent else None
    in_window = index is not None
    has_older = in_window and index + 1 < len(ordered)
    if parent is None and has_older:
        parent = ordered[index + 1].checkpoint_id
        parent_source = "list_order"
    return {
        # The list is newest first, so "newer" is the row above and "older" is
        # the row below -- named by direction rather than by "previous" and
        # "next", which mean opposite things depending on which end you read
        # the history from.
        "newer_attempt_id": (
            ordered[index - 1].checkpoint_id if in_window and index > 0 else None
        ),
        "older_attempt_id": (
            ordered[index + 1].checkpoint_id if has_older else None
        ),
        "older_attempt_unavailable_reason": (
            _NEIGHBOURS_OUTSIDE_WINDOW
            if not in_window or (not has_older and window_exhausted)
            else None
        ),
        "position": index + 1 if in_window else None,
        "total": total,
        "total_basis": total_basis,
        "scanned_window": len(ordered),
        "window_exhausted": window_exhausted,
        "unavailable_reason": unavailable_reason,
        "parent_generation_id": parent,
        "parent_source": parent_source,
    }


def _attempt_by_id(
    store: Any,
    attempt_id: str,
    *,
    deployment_name: str,
    branch_name: str,
) -> Any | None:
    """ONE checkpoint by id, or None when it is not this branch's attempt.

    The named fallback for an attempt older than the window. It answers the
    same question the window read answers and by the same rules, so the two
    cannot disagree about what counts as this deployment's attempt: the
    checkpoint has to be an `okg_generation`, of THIS deployment, on THIS
    branch. A checkpoint that fails any of the three is not found, which is
    the same answer an id belonging to nobody gets -- telling them apart is
    itself information about another deployment's history.

    None for exactly two named answers, which the caller turns into
    `AttemptNotFound`: the id names no checkpoint (`BranchNotFoundError`), or
    the checkpoint was retired by history compaction (`GenerationCompacted`).
    The compaction lookup is not scoped to a deployment, so serving it as its
    own answer would tell a reader that ANOTHER deployment once held the id.
    Every other failure -- a lost connection, a permission error, a backend
    fault -- propagates, so the endpoint reports a failed read rather than a
    404 that claims the attempt does not exist.
    """
    try:
        checkpoint = store.get_generation(attempt_id)
    except (BranchNotFoundError, GenerationCompacted):
        return None
    if checkpoint is None:
        return None
    metadata = getattr(checkpoint, "metadata", None) or {}
    if metadata.get("kind") != "okg_generation":
        return None
    if metadata.get("deployment_name") != deployment_name:
        return None
    branch = getattr(checkpoint, "branch_id", None)
    if branch is not None and str(branch) != branch_name:
        return None
    return checkpoint


def _name_source_rows(
    section: dict[str, Any],
    names: Mapping[str, str],
    reason: str | None,
) -> None:
    """Give each by-source row the source's declared name beside its id.

    The rollup keys a source by its ownership id, and the row's ``name`` stays
    that key. ``ownership_id`` repeats it under its own name and
    ``source_name`` is the name Sources prints (null for a key no registry
    entry declares; ``source_names_unavailable_reason`` says when no name
    could be read at all).
    """
    for row in section.get("rows") or []:
        key = str(row.get("name"))
        row["ownership_id"] = key
        row["source_name"] = names.get(key)
    section["source_names_unavailable_reason"] = reason


def compose_attempt_detail(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    attempt_id: str,
) -> dict[str, Any]:
    """Everything this deployment recorded about ONE update attempt.

    Scoped to this deployment and this branch through the same BOUNDED reader
    the attempts list uses, so an id belonging to another deployment raises
    the SAME ``AttemptNotFound`` as an id belonging to nobody.

    THE HISTORY READ IS BOUNDED, in two steps. It used to call
    ``store.list_generations``, which has no LIMIT and no projection and so
    moved every checkpoint's whole metadata document to find one row --
    measured at 1.75-1.88 s on okg-workspace's 88-checkpoint branch, for a
    page that reads twelve keys off ONE of them. It now reads a window of the
    newest attempts with only this page's declared keys projected, and an
    attempt older than that window is fetched BY ID rather than scanned for.

    The fallback is named rather than silent, and it is not a second answer to
    the same question: the window read answers "which attempts are recent",
    the id read answers "is this checkpoint an attempt of this deployment and
    branch", and ``AttemptNotFound`` is raised only when the SECOND says no.
    An attempt served from it carries what the window cannot tell it --
    neighbours, position -- as nulls with a reason, never as a guess.

    Raises ``MalformedAttemptId`` for an id no checkpoint id could be, and
    ``AttemptNotFound`` for one this branch's history does not hold. Every
    other failure is a named marker inside its own section: a missing rollup,
    an unreadable parent or an unrecorded gate summary does not refuse the
    read.
    """
    if not isinstance(attempt_id, str) or not attempt_id:
        raise MalformedAttemptId("attempt_id_empty")
    if not _ATTEMPT_ID_SHAPED.match(attempt_id):
        raise MalformedAttemptId("attempt_id_not_checkpoint_shaped")

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    # Newest first already, and projected to this page's declared keys.
    recent = store.recent_generations(
        deployment_name=deployment_name,
        branch=session.branch_name,
        status=None,
        keys=ATTEMPT_DETAIL_METADATA_KEYS,
        scan_window=ATTEMPT_DETAIL_SCAN_WINDOW,
    )
    ordered = list(recent.rows)
    index = next(
        (
            position
            for position, generation in enumerate(ordered)
            if generation.checkpoint_id == attempt_id
        ),
        None,
    )
    if index is None:
        # Not in the window. If the window reached the beginning of the
        # history there is nowhere else for it to be; otherwise ask for it by
        # id, which is one row rather than a scan.
        if not recent.window_exhausted:
            raise AttemptNotFound(attempt_id)
        generation = _attempt_by_id(
            store,
            attempt_id,
            deployment_name=deployment_name,
            branch_name=session.branch_name,
        )
        if generation is None:
            raise AttemptNotFound(attempt_id)
        neighbours_reason: str | None = _NEIGHBOURS_OUTSIDE_WINDOW
    else:
        generation = ordered[index]
        neighbours_reason = None
    metadata = generation.metadata or {}
    total = store.count_generation_checkpoints(
        deployment_name=deployment_name, branch=session.branch_name,
    )
    total_basis = "checkpoint_id_prefix"
    if total is None:
        total = len(ordered)
        total_basis = "scan_window"

    count_cache: dict[str, tuple[int, int]] = {}

    def _counts_or_reason(candidate: Any) -> tuple[tuple[int, int] | None, str | None]:
        cached = count_cache.get(candidate.checkpoint_id)
        if cached is not None:
            return cached, None
        try:
            counts = generation_counts(
                conn,
                dataclasses.replace(session, generation_id=candidate.checkpoint_id),
            )
        except GenerationCountsUnavailable as exc:
            return None, exc.message
        count_cache[candidate.checkpoint_id] = counts
        return counts, None

    target = metadata.get("target_generation_id")
    # ONE statement for the runs, for this one attempt.
    view_runs = _view_runs_by_target(
        conn,
        deployment_name=deployment_name,
        targets=[int(target)]
        if isinstance(target, int) and not isinstance(target, bool) else [],
    )

    neighbours = _neighbours(
        ordered,
        index,
        metadata,
        total=total,
        total_basis=total_basis,
        window_exhausted=recent.window_exhausted,
        unavailable_reason=neighbours_reason,
    )
    # The attempt immediately older in the history -- the same basis the list
    # uses for its delta, so the detail's headline delta equals the row the
    # operator clicked.
    #
    # Two ways it can be absent and they are not the same fact. Inside the
    # window, the row below it; at the END of an exhausted window, or for an
    # attempt fetched by id, the checkpoint's own recorded predecessor, read
    # by id. With neither, there is no baseline and the row says so instead of
    # pricing the attempt against an empty graph.
    previous = (
        ordered[index + 1]
        if index is not None and index + 1 < len(ordered)
        else None
    )
    previous_reason: str | None = None
    if previous is None:
        recorded = metadata.get("predecessor_generation_id")
        if isinstance(recorded, str) and recorded:
            previous = _attempt_by_id(
                store,
                recorded,
                deployment_name=deployment_name,
                branch_name=session.branch_name,
            )
        # A window that reached the beginning of the history proves this is
        # the first attempt, and an empty baseline is then the fact.
        if previous is None and recent.window_exhausted:
            previous_reason = NO_PREDECESSOR_IN_WINDOW
    attempt = compose_attempt_row(
        generation,
        session=session,
        view_runs=view_runs,
        previous_generation=previous,
        counts_or_reason=_counts_or_reason,
        previous_unavailable_reason=previous_reason,
    )

    # ONE rollup read for this version, and ONE for its parent. Not per row,
    # per source or per record type.
    counts, counts_error = _read_rollup(conn, generation_id=attempt_id)
    parent_id = neighbours["parent_generation_id"]
    parent_counts: dict[tuple[str, str], int] | None = None
    parent_marker: dict[str, Any] | None = None
    if parent_id is None:
        parent_marker = _marker("no_predecessor_version")
    else:
        parent_counts, parent_error = _read_rollup(conn, generation_id=parent_id)
        if parent_counts is None and parent_error is not None:
            parent_marker = _rollup_marker(
                parent_error, reason="rollup_unavailable_for_parent_version",
            )

    status = str(metadata.get("status") or "unknown")
    published = status == "published"
    unpublished_marker = (
        None if published
        else _marker(_UNPUBLISHED_MARKER_REASON, attempt_status=status)
    )

    source_writes = _source_writes(
        conn,
        deployment_name=deployment_name,
        created_at=generation.created_at,
        previous_created_at=previous.created_at if previous is not None else None,
        # First version: nothing before it in a history the window read to
        # its start, and no predecessor recorded. Anything else without a
        # read predecessor has no lower bound to draw the window from.
        previous=(
            "read" if previous is not None
            else "none" if (
                not recent.window_exhausted
                and not metadata.get("predecessor_generation_id")
            )
            else "unknown"
        ),
    )

    payload: dict[str, Any] = {
        "deployment": deployment_name,
        "branch": session.branch_name,
        "attempt_id": attempt_id,
        # One flag the view cannot miss, beside the per-section markers: a
        # refused publish's numbers describe a version the graph never took.
        "version_published": published,
        "attempt": attempt,
        "holds": _holds(metadata),
        "checks": _checks(metadata),
        "runs": {
            "runs": attempt["view_runs"],
            "total": attempt["view_runs_total"],
            "capped": attempt["view_runs_capped"],
            # "Could not look" is not "looked and found none": an attempt
            # recording no target_generation_id cannot be joined to the runs
            # table at all, and an empty list would read as a measured zero.
            "unavailable": attempt["view_runs_unavailable"],
        },
        "neighbours": neighbours,
        "configured": _configured(metadata),
        "source_writes": source_writes,
    }
    for section, kind in _SECTION_KINDS.items():
        payload[section] = _count_section(
            kind,
            counts=counts,
            counts_error=counts_error,
            parent_counts=parent_counts,
            parent_marker=parent_marker,
        )
    source_names, source_names_reason = source_display_names(deployment_name)
    _name_source_rows(payload["by_source"], source_names, source_names_reason)
    if unpublished_marker is not None:
        payload["holds"]["unpublished_version"] = dict(unpublished_marker)
        for section in _SECTION_KINDS:
            payload[section]["unpublished_version"] = dict(unpublished_marker)
    return payload


__all__ = [
    "ROLLUP_ROWS_PER_SECTION",
    "AttemptNotFound",
    "MalformedAttemptId",
    "compose_attempt_detail",
]
