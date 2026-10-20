"""Read-side companion for the console count rollup.

The operator console's count endpoints (the generations panel, the ontology
graph's per-subtype sizing, the per-source sidebar, the per-subtype node count,
the knowledge top-15, the health overview) used to recompute a count by scanning
the ~1M-node reconstructed Chronos graph on every read. The count rollup is now a
**Chronos-registered table** (``okg.console_count_rollup``) maintained by a
publish-stage running sum (``okg.substrate.publisher.console_count_rollup``); this
module reads it AS-OF the pinned generation's checkpoint -- an O(1) stab of a
compact table -- instead of re-scanning the graph.

Two shapes
----------
* ``read_console_count_rollup`` returns a ``{(dimension_kind, dimension_value):
  count}`` map (optionally filtered to a set of kinds) for callers that index by
  key (the per-source / per-subtype-bucket / top-15 endpoints);
* ``read_console_count_subtype_rows`` returns ``[{"subtype": v, "c": count}]``,
  shaped EXACTLY like the ``_ONTOLOGY_NODES_SQL`` rows the two ``graph.py``
  consumers read (``row["subtype"]`` / ``row["c"]``), so the cutover is drop-in.

The TOTALS dimension is NOT served here -- node/edge totals come O(1) from
checkpoint metadata via ``accelerate-operator-read-path``'s ``generation_counts``.
This module owns only the by-subtype / by-edge-type / by-source dimensions.

Fail-loud contract: both reads raise ``ConsoleCountRollupUnavailable`` when the
pinned generation has no maintained rollup -- the checkpoint is absent, the table
is unregistered, or no maintenance SENTINEL is present (the publish-stage hook
never ran). Callers MUST surface that rather than silently falling back to the
O(graph) scan, a stale value, or zero (P0). A maintained-but-empty generation
(sentinel present, zero data rows) returns an empty map -- absence of a row for a
present sentinel is a real zero bucket, never a loud failure.

Import-light: like the ontology rollup read, this pulls no ``graph_store`` at
module level (the kit's ``read_maintained_rows`` defers that import into its body),
so a read-side import stays cheap and free of the heavy substrate graph stack.
"""
from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import psycopg

from okg.substrate.derived_artifacts.maintained_derivation import (
    MaintainedDerivationUnavailable,
    SentinelSpec,
    read_maintained_rows,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    COUNT_KIND_EDGE_SOURCE,
    COUNT_KIND_ENRICHER_EDGE,
    COUNT_KIND_ENRICHER_NODE,
    COUNT_KIND_SOURCE,
    COUNT_KIND_SOURCE_SUBTYPE,
    COUNT_KIND_SUBTYPE,
    SENTINEL_TOKEN,
    parse_source_subtype,
)

#: The single flag governing BOTH the publish-time maintainer and these reads (the
#: #433 ``OKG_IVM_*`` pattern), default ON after the operator UI latency incident.
#: An explicit ``0`` is an opt-out for tests or emergency rollback; request-time
#: callers must fail loud instead of reopening raw graph scans.
COUNT_ROLLUP_FLAG = "OKG_IVM_CONSOLE_COUNT_ROLLUP"


def console_count_rollup_enabled() -> bool:
    """True iff the count-rollup read path is enabled (``OKG_IVM_CONSOLE_COUNT_ROLLUP``
    is not ``"0"``). One flag governs maintain AND read so they can never disagree."""
    return os.environ.get(COUNT_ROLLUP_FLAG, "1") == "1"

# The Chronos-registered table this read reconstructs as-of a pinned generation,
# plus its sentinel schema. Redefined as a literal (not imported from graph_store)
# to keep this read import-light, exactly as the ontology rollup read does. The kit
# detects the sentinel by the shared SENTINEL_TOKEN on every PK column.
CONSOLE_COUNT_ROLLUP_TABLE = "okg.console_count_rollup"
_COUNT_SENTINEL = SentinelSpec(
    pk_columns=("dimension_kind", "dimension_value"),
    cadence_column="item_count",
)

CountKey = tuple[str, str]


class ConsoleCountRollupUnavailable(MaintainedDerivationUnavailable):
    """Raised when no maintained count rollup exists for the pinned generation.

    A typed subclass of the kit's ``MaintainedDerivationUnavailable``: generic
    maintained-derivation handlers catch it too, and callers that catch
    ``ConsoleCountRollupUnavailable`` specifically keep working. A loud failure on
    purpose -- the read must never silently fall back to the O(graph) scan, a stale
    value, or zero when the rollup is missing for the pinned generation.
    """


def require_console_count_rollup_enabled(*, generation_id: int | str | None) -> None:
    """Fail loud when an operator UI count read would otherwise take the raw path."""
    if console_count_rollup_enabled():
        return
    raise ConsoleCountRollupUnavailable(
        generation_id or "<unknown>",
        "count_rollup_disabled",
        f"{COUNT_ROLLUP_FLAG}=0 disables maintained count reads; "
        "operator UI requests must not fall back to raw graph scans",
    )


def _to_count_row(row: Mapping[str, Any]) -> tuple[str, str, int]:
    return (
        str(row["dimension_kind"]),
        str(row["dimension_value"]),
        int(row["item_count"] or 0),
    )


def read_console_count_rollup(
    conn: psycopg.Connection[Any],
    *,
    generation_id: int | str,
    kinds: Iterable[str] | None = None,
) -> dict[CountKey, int]:
    """Return the maintained counts AS-OF the pinned generation as a
    ``{(dimension_kind, dimension_value): count}`` map.

    ``generation_id`` is the generation CHECKPOINT id (e.g. ``generation:1234``)
    OR the integer ``target_generation_id``. ``kinds`` optionally restricts the
    result to a set of dimension kinds (e.g. ``{"source"}``); ``None`` returns all
    dimensions.

    Raises ``ConsoleCountRollupUnavailable`` when the generation has no maintained
    rollup (checkpoint absent / table unregistered / no maintenance sentinel) -- the
    caller decides how to surface it (never a silent scan). The as-of-generation
    read, the three loud-fail reasons, and the sentinel separation are delegated to
    the maintained-derivation kit (``read_maintained_rows``).
    """
    wanted = set(kinds) if kinds is not None else None
    try:
        rows = read_maintained_rows(
            conn,
            table=CONSOLE_COUNT_ROLLUP_TABLE,
            generation_id=generation_id,
            select_columns=("dimension_kind", "dimension_value", "item_count"),
            sentinel_spec=_COUNT_SENTINEL,
            row_mapper=_to_count_row,
            # The sentinel row must come back with the filtered rows: it is
            # the proof the maintainer ran for this generation.
            where=(
                None
                if wanted is None
                else "dimension_kind = ANY(:kinds) OR dimension_kind = :sentinel"
            ),
            params=(
                None
                if wanted is None
                else {"kinds": sorted(wanted), "sentinel": SENTINEL_TOKEN}
            ),
        )
    except MaintainedDerivationUnavailable as exc:
        raise ConsoleCountRollupUnavailable(
            exc.generation_id, exc.reason, exc.detail
        ) from exc
    out: dict[CountKey, int] = {}
    for kind, value, count in rows:
        if wanted is None or kind in wanted:
            out[(kind, value)] = count
    return out


def read_console_count_subtype_rows(
    conn: psycopg.Connection[Any],
    *,
    generation_id: int | str,
) -> list[dict[str, Any]]:
    """Return the by-subtype counts AS-OF the pinned generation shaped EXACTLY like
    ``_ONTOLOGY_NODES_SQL``'s rows (``[{"subtype": v, "c": count}]``), a drop-in for
    the two ``graph.py`` consumers that read ``row["subtype"]`` / ``row["c"]``.

    Raises ``ConsoleCountRollupUnavailable`` on a missing rollup (never a silent scan).
    """
    counts = read_console_count_rollup(
        conn, generation_id=generation_id, kinds={COUNT_KIND_SUBTYPE}
    )
    return [{"subtype": value, "c": count} for (_kind, value), count in counts.items()]


@dataclass(frozen=True)
class ProducerEmission:
    """The per-(producer, subtype) node-emission detail at a pinned generation.

    ``rows`` is ``[(producer, subtype, count)]`` parsed from the
    ``source_subtype`` dimension. ``dimension_available`` is False iff the
    maintained rollup at this generation carries ``source`` rows but NO
    ``source_subtype`` rows — i.e. the rows were maintained by a release that
    predates the dimension (the two dimensions share the ``source IS NOT NULL``
    domain, so on a same-shape table they are present or absent together).
    Callers MUST surface that as an explicit unavailable marker, never render
    it as an honest zero-emission graph.
    """

    rows: list[tuple[str, str, int]]
    dimension_available: bool


def read_producer_emission(
    conn: psycopg.Connection[Any],
    *,
    generation_id: int | str,
) -> ProducerEmission:
    """Return the ``source_subtype`` rows AS-OF the pinned generation, parsed to
    ``(producer, subtype, count)``, plus the predates-dimension detection.

    Raises ``ConsoleCountRollupUnavailable`` when the generation has no
    maintained rollup at all (same loud contract as every read here), and
    ``ValueError`` on a malformed ``dimension_value`` (never a silent skip).
    """
    counts = read_console_count_rollup(
        conn,
        generation_id=generation_id,
        kinds={COUNT_KIND_SOURCE_SUBTYPE, COUNT_KIND_SOURCE},
    )
    rows: list[tuple[str, str, int]] = []
    sources_present = False
    for (kind, value), count in counts.items():
        if kind == COUNT_KIND_SOURCE:
            sources_present = True
            continue
        producer, subtype = parse_source_subtype(value)
        rows.append((producer, subtype, count))
    dimension_available = bool(rows) or not sources_present
    rows.sort()
    return ProducerEmission(rows=rows, dimension_available=dimension_available)


class ConsoleCountRollupCache:
    """The maintained counts a single console request needs, read ONCE.

    `/console/producers` composes three reads that each want the rollup (once
    in `compose_sources`, once in `compose_enricher_diagnostics`, once for
    records-by-type). Each read used to take a Chronos generation read LEASE --
    a write that deleted expired pins and inserted one -- so a read-only page
    wrote three times and a read-only session was refused (issue #2202). The
    read itself no longer takes a lease (`read_maintained_rows`); this cache
    still makes the composed page ONE statement instead of three.

    This holds the rows for one request. It is NOT a cross-request cache and
    must not become one: the counts are as-of a pinned generation, and a
    cache that outlived the request would serve one generation's numbers
    under another's id.

    A caller asking for kinds this cache has not read yet triggers another
    read for the union, and `reads` counts it -- so the cost is visible to a
    test rather than hidden behind a hit rate.
    """

    __slots__ = (
        "_conn", "_generation_id", "_kinds", "_prime", "_rows", "reads",
    )

    def __init__(
        self,
        conn: psycopg.Connection[Any],
        *,
        generation_id: int | str,
    ) -> None:
        self._conn = conn
        self._generation_id = generation_id
        # `None` means every dimension has been read; a set means exactly
        # those kinds have.
        self._kinds: set[str] | None = set()
        # Kinds a composed read has ANNOUNCED it will want. They are added
        # to the first real read's filter so one request costs one statement,
        # and they are only announced -- a request that never reads the
        # rollup still issues no rollup statement at all.
        self._prime: set[str] = set()
        self._rows: dict[CountKey, int] | None = None
        self.reads = 0

    def prime(self, kinds: Iterable[str]) -> None:
        """Announce kinds a later composer will want, without reading now.

        `compose_producers` composes three reads that each want a different
        set of dimensions. Announcing all of them up front means the first one
        to actually read fetches the union, and the other two are served from
        it: one statement.
        """
        if self._rows is None:
            self._prime |= set(kinds)

    def read(self, kinds: Iterable[str] | None = None) -> dict[CountKey, int]:
        """The counts for `kinds`, reading only if this request has not yet.

        `kinds=None` means every dimension, which is what it means to
        `read_console_count_rollup` too. `self._kinds is None` records that
        every dimension has already been read, so a later narrower request is
        served from what is held rather than costing a second statement.
        """
        wanted = set(kinds) if kinds is not None else None
        held_all = self._rows is not None and self._kinds is None
        covered = (
            self._rows is not None
            and wanted is not None
            and self._kinds is not None
            and wanted <= self._kinds
        )
        if not (held_all or covered):
            if wanted is None or self._kinds is None:
                union = None
            else:
                union = wanted | self._kinds | self._prime
            self.reads += 1
            self._rows = read_console_count_rollup(
                self._conn,
                generation_id=self._generation_id,
                kinds=union,
            )
            self._kinds = None if union is None else set(union)
        rows = self._rows or {}
        if wanted is None:
            return dict(rows)
        return {key: count for key, count in rows.items() if key[0] in wanted}


def rollup_reader(
    cache: "ConsoleCountRollupCache | None",
    conn: psycopg.Connection[Any],
    *,
    generation_id: int | str,
) -> "ConsoleCountRollupCache":
    """The request's cache, or a fresh one for a composer called on its own.

    Each console composer is also its own route, so none of them may REQUIRE
    a cache from a caller. Passing one in is what makes the composed read
    (`/console/producers`) issue a single rollup statement instead of one per
    composer.
    """
    if cache is not None:
        return cache
    return ConsoleCountRollupCache(conn, generation_id=generation_id)


__all__ = [
    "COUNT_KIND_EDGE_SOURCE",
    "COUNT_KIND_ENRICHER_EDGE",
    "COUNT_KIND_ENRICHER_NODE",
    "COUNT_KIND_SOURCE",
    "COUNT_KIND_SOURCE_SUBTYPE",
    "COUNT_KIND_SUBTYPE",
    "ConsoleCountRollupCache",
    "ConsoleCountRollupUnavailable",
    "ProducerEmission",
    "console_count_rollup_enabled",
    "read_console_count_rollup",
    "read_console_count_subtype_rows",
    "read_producer_emission",
    "rollup_reader",
    "require_console_count_rollup_enabled",
]
