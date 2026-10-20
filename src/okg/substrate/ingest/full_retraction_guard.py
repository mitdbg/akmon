"""Refuse a source run that would retract everything while emitting nothing.

Issue #2818 backstop. A failed or empty read that reaches the runner as
"the source now has nothing" deletes every record the source published. The
09-22 replay rule already keeps the prior state when a streaming run emits
nothing without asserting its scope; this guard covers what is left, before
any fact of the run is written:

* **Whole source.** A completed, unnarrowed run whose record set holds zero
  records while its planned retractions are non-empty and the source still
  has live rows in ``okg.graph_nodes`` / ``okg.graph_edges``. This is how an
  empty record set and a ``--reset-cursor`` stray pass wipe a source.
* **Empty-scope assertion.** A run that sets ``SourceRun.asserts_empty_scope``
  with whole-scope deletion authority while the source has live rows.
* **Per-record sub-scope.** For an adapter that sets
  ``record_keys_are_sub_scopes = True`` (each record is a container such as
  one repository), a record the source still emits that had child rows
  (descendant nodes or edges) before and has none now. A record that
  disappears entirely is an ordinary deletion and passes.

The operator overrides one run of one source with
``okg ingest --allow-full-retraction <source>``. The override is recorded in
the run's ``sync.run.<run_id>`` audit row and on ``RunResult``.
"""

from __future__ import annotations

import getpass
import os
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import RecordSet

OVERRIDE_FLAG = "--allow-full-retraction"
#: Adapter class attribute that declares each record key a sub-scope.
SUB_SCOPE_ATTRIBUTE = "record_keys_are_sub_scopes"
_MAX_NAMED_RECORD_KEYS = 20


class SourceRunFullRetractionRefused(SubstrateError):
    """A run would retract all of a source's (or a sub-scope's) live rows
    while emitting nothing for it, and no override was given."""

    code = "source_run_full_retraction_refused"


@dataclass(frozen=True)
class FullRetractionOverride:
    """One operator's permission to retract everything, for one source run."""

    source: str
    requested_by: str
    requested_at: datetime

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "requested_by": self.requested_by,
            "requested_at": self.requested_at.isoformat(),
            "flag": OVERRIDE_FLAG,
        }


@dataclass(frozen=True)
class LiveRowCount:
    nodes: int
    edges: int
    basis: str

    @property
    def total(self) -> int:
        return self.nodes + self.edges


def _operator_identity(owner_id: str | None) -> str:
    if owner_id:
        return str(owner_id)
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return f"uid:{os.getuid()}"


def full_retraction_override_for(
    options: Any,
    name: str,
    ownership_id: str | None,
) -> FullRetractionOverride | None:
    """Return the override for this source if the operator named it."""
    allowed = set(getattr(options, "allow_full_retraction", ()) or ())
    matched = next(
        (candidate for candidate in (name, ownership_id) if candidate in allowed),
        None,
    )
    if matched is None:
        return None
    return FullRetractionOverride(
        source=str(matched),
        requested_by=_operator_identity(getattr(options, "owner_id", None)),
        requested_at=datetime.now(timezone.utc),
    )


def validate_full_retraction_override_names(
    names: Collection[str],
    entries: Mapping[str, Any],
    selected: Iterable[str],
) -> None:
    """Refuse an override naming no selected source, before any source runs."""
    if not names:
        return
    known: set[str] = set()
    for name in selected:
        known.add(str(name))
        ownership_id = getattr(entries.get(name), "ownership_id", None)
        if ownership_id:
            known.add(str(ownership_id))
    unknown = sorted(str(name) for name in names if str(name) not in known)
    if unknown:
        raise SubstrateError(
            f"{OVERRIDE_FLAG} names no source selected for this ingest",
            data={"unknown_sources": unknown, "selected_sources": sorted(known)},
            fix_hint=(
                f"pass {OVERRIDE_FLAG} a source name or ownership id that "
                "this ingest runs"
            ),
        )


def full_retraction_report_fields(result: Any) -> dict[str, Any]:
    """The override record for the per-source ingest report, when one was given."""
    record = getattr(result, "full_retraction_override", None)
    return {"full_retraction_override": record} if record else {}


def count_live_source_rows(session: Any, source_name: str) -> LiveRowCount:
    """Count the source's live graph rows on the runner's branch."""

    def _count(table: str) -> int:
        rows = list(session.query(
            f"SELECT count(*) AS n FROM okg.{table} WHERE source = :source",
            {"source": source_name},
        ))
        return int(rows[0]["n"]) if rows else 0

    return LiveRowCount(
        nodes=_count("graph_nodes"),
        edges=_count("graph_edges"),
        basis="graph",
    )


def live_rows_from_record_set(record_set: RecordSet) -> LiveRowCount:
    """Row count from the prior record-set authority (no graph store)."""
    nodes: set[str] = set()
    edges: set[tuple[str, str, str]] = set()
    for emission in record_set.records.values():
        nodes.update(emission.nodes_by_id())
        edges.update(emission.edges_by_identity())
    return LiveRowCount(
        nodes=len(nodes), edges=len(edges), basis="prior_record_set",
    )


def _child_rows(emission: Any) -> int:
    return len(emission.descendants) + len(emission.edges)


def emptied_sub_scopes(
    prior: RecordSet,
    current: RecordSet,
) -> list[tuple[str, int]]:
    """Records still emitted that had child rows before and have none now."""
    emptied: list[tuple[str, int]] = []
    for key in sorted(prior.records):
        emission = current.records.get(key)
        if emission is None:
            continue
        prior_children = _child_rows(prior.records[key])
        if prior_children and not _child_rows(emission):
            emptied.append((key, prior_children))
    return emptied


@dataclass
class FullRetractionGuard:
    """Per-run guard state held by ``SourceRunner``."""

    source_name: str
    registry_name: str | None = None
    override: FullRetractionOverride | None = None
    exercises: list[dict[str, Any]] = field(default_factory=list)

    def _refuse_or_record(
        self,
        *,
        kind: str,
        message: str,
        live_rows: int,
        detail: dict[str, Any],
    ) -> None:
        if self.override is not None:
            self.exercises.append({
                "kind": kind,
                "live_rows": live_rows,
                **detail,
            })
            return
        name = self.registry_name or self.source_name
        raise SourceRunFullRetractionRefused(
            message,
            data={
                "source_name": self.source_name,
                "registry_name": self.registry_name,
                "kind": kind,
                "live_rows": live_rows,
                "override_flag": f"{OVERRIDE_FLAG} {name}",
                **detail,
            },
            fix_hint=(
                "check why the source read returned nothing (a failed clone, "
                "an unreadable path, an empty API page); if the source really "
                f"is empty, rerun once with `okg ingest {OVERRIDE_FLAG} "
                f"{name} --inline ...`; to decommission the source, use "
                "`okg remediate retire-source` instead"
            ),
        )

    def check_record_set(
        self,
        *,
        whole_scope: bool,
        record_set: RecordSet,
        prior_record_set: RecordSet,
        planned_retractions: int,
        sub_scopes: bool,
        live_rows: Callable[[], LiveRowCount],
    ) -> None:
        """Check a record-set run before its retractions are written."""
        if (
            whole_scope
            and not record_set.records
            and planned_retractions
        ):
            live = live_rows()
            if live.total:
                self._refuse_or_record(
                    kind="whole_source",
                    message=(
                        f"source {self.source_name!r} emitted zero records "
                        f"but its run would retract {planned_retractions} "
                        f"rows while {live.total} are live"
                    ),
                    live_rows=live.total,
                    detail={
                        "live_nodes": live.nodes,
                        "live_edges": live.edges,
                        "live_row_basis": live.basis,
                        "planned_retractions": planned_retractions,
                        "emitted_records": 0,
                    },
                )
        if not sub_scopes:
            return
        emptied = emptied_sub_scopes(prior_record_set, record_set)
        if not emptied:
            return
        child_rows = sum(count for _key, count in emptied)
        keys = [key for key, _count in emptied]
        self._refuse_or_record(
            kind="sub_scope",
            message=(
                f"source {self.source_name!r} still emits "
                f"{len(emptied)} record(s) whose {child_rows} prior child "
                f"rows would all be retracted: {keys[0]!r}"
                + (f" and {len(keys) - 1} more" if len(keys) > 1 else "")
            ),
            live_rows=child_rows,
            detail={
                "record_keys": keys[:_MAX_NAMED_RECORD_KEYS],
                "record_key_count": len(keys),
                "prior_child_rows_by_record_key": {
                    key: count
                    for key, count in emptied[:_MAX_NAMED_RECORD_KEYS]
                },
            },
        )

    def check_empty_scope_assertion(
        self,
        *,
        live_rows: Callable[[], LiveRowCount],
    ) -> None:
        """Check ``asserts_empty_scope`` before the run's stream is walked."""
        live = live_rows()
        if not live.total:
            return
        self._refuse_or_record(
            kind="empty_scope_assertion",
            message=(
                f"source {self.source_name!r} asserts an empty scope while "
                f"{live.total} of its rows are live"
            ),
            live_rows=live.total,
            detail={
                "live_nodes": live.nodes,
                "live_edges": live.edges,
                "live_row_basis": live.basis,
            },
        )

    def audit_record(self) -> dict[str, Any] | None:
        """The override record for the run audit row and ``RunResult``."""
        if self.override is None:
            return None
        return {
            **self.override.as_dict(),
            "exercised": bool(self.exercises),
            "exercises": list(self.exercises),
        }
