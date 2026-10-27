"""One node's row versions across generations, read from Chronos interval rows.

PACT okg-attribute-history-read (req.store). Chronos keeps every version of a
graph row in its physical interval table (``public._chronos_b_interval_*``):
each physical row is visible on the half-open position range
``[live_lo, live_hi)`` and records the segment that wrote it
(``writer_segment_id``). A generation is a checkpoint; its snapshot segment's
``branch_point`` is the position a pinned checkout reads (``checkout_generation``
reads exactly the rows with ``live_lo <= branch_point < live_hi``).

The rules this module keeps in the store, so no MCP code writes interval SQL:

* **Segment chain.** A branch's history is the rows of its segment chain: the
  branch's current segment, then each ``parent_segment_id`` in turn. A fork's
  chain runs through its fork base into the parent's segments as they were
  when the fork was cut, so the parent's later writes are not in it, and the
  fork's own writes are not in the parent's chain.
* **Lineage generations.** The generations of a branch are the checkpoints
  whose snapshot segment hangs off a chain segment, oldest first by chain
  depth. A parent checkpoint taken after the fork hangs off a parent segment
  outside the fork's chain and is not in the fork's lineage.
* **Positions to generation ids.** Each lineage checkpoint is named and ordered
  by ``okg.v_generation_interval_stab`` (the generation-ordinal registry), on
  the checkpoint's own branch. A published generation in the read window with
  no ordinal, an ordinal on another branch, or ordinals that do not rise along
  the chain is refused (``history_generation_unordered``); nothing is ordered
  by wall-clock time.
* **Below the branch point.** A row maps to a generation only when it contains
  that generation's stab position, so a parent segment's rows count for a
  fork only below the fork's branch point, and a parent row's right remainder
  (the copy that keeps the parent visible after a fork writes) never maps to
  a fork generation.
* **Bounded.** The rows of one node on the chain that overlap the window's
  stab positions are read in one statement on the ``(node_id, live_lo)``
  primary key, stopping one row past the budget; more rows than the budget is
  refused (``history_row_budget_exceeded``), never truncated. A narrow window
  of a long-lived node scans only that window's versions.
* **Compaction.** The checkpoints of the window are re-read after the rows.
  Compaction commits its tombstone (and removes the checkpoint) before it
  deletes any interval row, so a window checkpoint still present after the
  row read proves its rows were intact; a missing one is refused as
  compacted, never served from partial rows.

Reads use only ``public._chronos_*`` and ``okg.v_generation_interval_stab``,
both readable by ``okg_mcp`` (``views.sql`` grant registry), so the MCP read
role needs no new grant.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from okg.substrate.errors import GenerationOutOfScope, SubstrateError
from okg.substrate.graph_store_internal.common import NODE_TABLE
from okg.substrate.graph_store_internal.generation_ordinal import _as_checkpoint_id


#: Physical row versions of one node a history read may scan.
NODE_HISTORY_ROW_BUDGET = 5_000


class HistoryRowBudgetExceeded(SubstrateError):
    """A history read would scan more rows than its budget.

    Refused rather than answered with a shortened list: a history with
    versions silently missing reads as a history with fewer changes.
    """

    code = "history_row_budget_exceeded"


class HistoryGenerationUnordered(SubstrateError):
    """A generation in a history window has no usable ordinal.

    The ordinal registry (``okg.v_generation_interval_stab``) names and
    orders every generation a history read reports. A published generation
    without an ordinal, with an ordinal on another branch, or with ordinals
    that do not rise along the branch's segment chain cannot be placed, and is
    refused rather than ordered by wall-clock time.
    """

    code = "history_generation_unordered"


class HistoryRangeInvalid(SubstrateError):
    """The history read's `from` generation is newer than its `to`."""

    code = "history_range_invalid"


@dataclass(frozen=True)
class LineageGeneration:
    """One published generation on a branch's segment chain."""

    generation_id: str
    #: The Chronos branch the checkpoint was taken on (a fork's lineage
    #: carries its parent's generations, on the parent's branch).
    branch: str
    #: The ordinal registry's position; None when the registry has none.
    ordinal: Optional[int]
    #: The position a pinned checkout of this generation reads.
    stab: int
    created_at: str
    #: Depth of the checkpoint's parent segment on the chain (0 = head).
    depth: int


@dataclass(frozen=True)
class NodeRowVersion:
    """One physical row version of a node, as Chronos stored it."""

    live_lo: int
    live_hi: int
    subtype: Optional[str]
    attrs: dict[str, Any]
    source: Optional[str]
    source_record_id: Any
    source_revision: Any


@dataclass(frozen=True)
class NodeHistory:
    """A node's state at every lineage generation of a window.

    ``generations`` is oldest first; ``states[i]`` is the row version visible
    at ``generations[i]``, or None when the node is absent there (never
    projected, or retracted). ``rows_scanned`` counts the physical rows read.
    """

    generations: tuple[LineageGeneration, ...]
    states: tuple[Optional[NodeRowVersion], ...]
    rows_scanned: int
    row_budget: int


def _interval_rows(db: Any, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(sql, list(params)).fetchall()]


class ChronosGraphNodeHistoryMixin:
    """Lineage and per-node version reads over Chronos interval storage."""

    context: Any

    def _history_db(self) -> Any:
        context = self.context
        db = getattr(context, "db", None)
        if (
            getattr(context, "backend_name", None) != "interval"
            or getattr(db, "dialect", None) != "postgres"
        ):
            raise SubstrateError(
                "history reads need the Chronos interval backend on PostgreSQL",
                data={
                    "backend": getattr(context, "backend_name", None),
                    "dialect": getattr(db, "dialect", None),
                },
                fix_hint="read history from a PostgreSQL deployment graph",
            )
        return db

    def _history_segment_chain(self, db: Any, branch: str) -> list[int]:
        """The branch's segment chain, head first."""
        rows = _interval_rows(
            db,
            """
            WITH RECURSIVE chain(segment_id, parent_segment_id, depth) AS (
                SELECT s.segment_id, s.parent_segment_id, 0
                  FROM _chronos_branch_interval_branches AS b
                  JOIN _chronos_branch_interval_segments AS s
                    ON s.segment_id = b.current_segment_id
                 WHERE b.branch_id = ?
              UNION ALL
                SELECT p.segment_id, p.parent_segment_id, c.depth + 1
                  FROM _chronos_branch_interval_segments AS p
                  JOIN chain AS c ON p.segment_id = c.parent_segment_id
            )
            SELECT segment_id, depth FROM chain ORDER BY depth
            """,
            (branch,),
        )
        if not rows:
            raise SubstrateError(
                f"branch {branch!r} has no Chronos segment chain",
                data={"branch": branch},
                fix_hint="read history on a branch that exists",
            )
        return [int(row["segment_id"]) for row in rows]

    def lineage_generations(
        self,
        *,
        branch: str,
        deployment_name: str,
    ) -> tuple[list[int], list[LineageGeneration]]:
        """The branch's segment chain and its candidate lineage generations.

        Returns ``(chain, generations)``: the chain head first, and every
        checkpoint hanging off a chain segment, oldest first. A checkpoint the
        ordinal registry names for ``deployment_name`` carries its ordinal; one
        the registry does not name carries ``ordinal=None`` and may still be a
        published generation (``_published_without_ordinal`` decides, only for
        the ones a read window covers). A checkpoint the registry names for
        another deployment is left out.
        """
        db = self._history_db()
        chain = self._history_segment_chain(db, branch)
        depth_of = {segment_id: depth for depth, segment_id in enumerate(chain)}
        rows = _interval_rows(
            db,
            """
            SELECT cp.checkpoint_id, cp.branch_id, cp.created_at,
                   s.branch_point, s.parent_segment_id,
                   o.chronos_branch_id AS ordinal_branch,
                   o.stab_ordinal, o.deployment_name AS ordinal_deployment
              FROM _chronos_branch_interval_checkpoints AS cp
              JOIN _chronos_branch_interval_segments AS s
                ON s.segment_id = cp.segment_id
              LEFT JOIN okg.v_generation_interval_stab AS o
                ON o.generation_ref = cp.checkpoint_id
             WHERE s.parent_segment_id = ANY(?::bigint[])
            """,
            (chain,),
        )
        generations: list[LineageGeneration] = []
        for row in rows:
            ordinal_deployment = row.get("ordinal_deployment")
            if row.get("stab_ordinal") is not None and ordinal_deployment not in (
                None, deployment_name,
            ):
                continue
            ordinal = row.get("stab_ordinal")
            if ordinal is not None and str(row.get("ordinal_branch")) != str(
                row["branch_id"]
            ):
                # An ordinal minted on another branch does not place this
                # checkpoint on its own branch: treated as no ordinal.
                ordinal = None
            generations.append(
                LineageGeneration(
                    generation_id=str(row["checkpoint_id"]),
                    branch=str(row["branch_id"]),
                    ordinal=None if ordinal is None else int(ordinal),
                    stab=int(row["branch_point"]),
                    created_at=str(row["created_at"]),
                    depth=depth_of[int(row["parent_segment_id"])],
                )
            )
        generations.sort(
            key=lambda g: (
                -g.depth,
                g.ordinal if g.ordinal is not None else -1,
                g.generation_id,
            )
        )
        return chain, generations

    def _published_without_ordinal(
        self,
        db: Any,
        candidates: Sequence[LineageGeneration],
        *,
        deployment_name: str,
    ) -> list[str]:
        """The ids among ``candidates`` that are published generations of
        ``deployment_name`` although the ordinal registry does not name them.

        Reads only the three metadata fields, and only for checkpoints a read
        window covers that carry no ordinal (a blocked generation, a Chronos
        checkpoint that is no generation, or a published generation whose
        ordinal mint was skipped).
        """
        ids = [g.generation_id for g in candidates if g.ordinal is None]
        if not ids:
            return []
        rows = _interval_rows(
            db,
            """
            SELECT t.checkpoint_id
              FROM _chronos_branch_interval_checkpoints AS t,
                   LATERAL json_to_record(t.metadata::json)
                        AS m(kind text, deployment_name text, status text)
             WHERE t.checkpoint_id = ANY(?::text[])
               AND m.kind = 'okg_generation'
               AND m.status = 'published'
               AND m.deployment_name = ?
            """,
            (ids, deployment_name),
        )
        return sorted(str(row["checkpoint_id"]) for row in rows)

    def history_window(
        self,
        *,
        branch: str,
        deployment_name: str,
        from_generation: Optional[str],
        to_generation: str,
    ) -> tuple[list[int], list[LineageGeneration]]:
        """The branch's chain and the ordered generations from ``from`` to ``to``.

        ``from_generation=None`` starts at the oldest published generation in
        the branch's lineage. Both ends must be lineage generations of this
        deployment (``GenerationOutOfScope`` otherwise); ``from`` newer than
        ``to`` is ``HistoryRangeInvalid``. Every published generation inside
        the window must carry an ordinal on its own branch and the ordinals
        must rise along the chain, else ``HistoryGenerationUnordered``.
        """
        db = self._history_db()
        chain, lineage = self.lineage_generations(
            branch=branch, deployment_name=deployment_name,
        )
        index = {g.generation_id: i for i, g in enumerate(lineage)}

        def position(requested: str, end: str) -> int:
            found = index.get(_as_checkpoint_id(requested))
            if found is None:
                raise GenerationOutOfScope(
                    f"generation {requested!r} is not in the history of branch "
                    f"{branch!r} for deployment {deployment_name!r}",
                    data={
                        "requested_generation": requested,
                        "deployment_name": deployment_name,
                        "branch_name": branch,
                        "generation_deployment_name": None,
                        "generation_branch": None,
                        "history_end": end,
                    },
                    fix_hint=(
                        "pass a generation published on this session's branch "
                        "or before its fork point"
                    ),
                )
            return found

        to_index = position(to_generation, "to")
        if from_generation is None:
            # The oldest generation at or before `to` that is a published
            # generation of this deployment: an ordinal says so, and a
            # checkpoint without one is read for its status.
            older = lineage[: to_index + 1]
            unordered_published = set(
                self._published_without_ordinal(
                    db, older, deployment_name=deployment_name,
                )
            )
            from_index = next(
                (
                    i for i, g in enumerate(older)
                    if g.ordinal is not None or g.generation_id in unordered_published
                ),
                to_index,
            )
        else:
            from_index = position(from_generation, "from")
        if from_index > to_index:
            raise HistoryRangeInvalid(
                f"history `from` generation {lineage[from_index].generation_id!r} "
                f"is newer than `to` generation {lineage[to_index].generation_id!r}",
                data={
                    "from_generation": lineage[from_index].generation_id,
                    "to_generation": lineage[to_index].generation_id,
                },
                fix_hint="swap `from` and `to`, or omit `from` to start at the oldest",
            )
        window = lineage[from_index: to_index + 1]
        unordered = self._published_without_ordinal(
            db, window, deployment_name=deployment_name,
        )
        if unordered:
            raise HistoryGenerationUnordered(
                "published generations in the history window have no ordinal on "
                "their branch, so their order cannot be read",
                data={
                    "generation_ids": unordered[:20],
                    "unordered_count": len(unordered),
                    "branch": branch,
                    "deployment_name": deployment_name,
                },
                fix_hint=(
                    "backfill the generation-ordinal registry for this branch, "
                    "or pass a `from` after these generations"
                ),
            )
        published = [g for g in window if g.ordinal is not None]
        for older, newer in zip(published, published[1:]):
            # A fork's first ordinal continues from its base generation's, so
            # ordinals rise along the whole chain, across the fork point too.
            if int(newer.ordinal) <= int(older.ordinal):  # type: ignore[arg-type]
                raise HistoryGenerationUnordered(
                    "the ordinal registry's order disagrees with the branch's "
                    "segment chain",
                    data={
                        "generation_ids": [older.generation_id, newer.generation_id],
                        "unordered_count": 2,
                        "branch": branch,
                        "deployment_name": deployment_name,
                    },
                    fix_hint="repair the generation-ordinal registry for this branch",
                )
        return chain, published

    def _node_physical_table(self, db: Any) -> str:
        row = db.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = ?",
            (NODE_TABLE,),
        ).fetchone()
        if row is None:
            raise SubstrateError(
                "the node table has no Chronos interval storage registered",
                data={"table": NODE_TABLE},
                fix_hint="provision the deployment so the graph tables exist",
            )
        physical = str(row["physical_table"])
        if not physical.startswith("_chronos_b_interval_") or not all(
            ch.isalnum() or ch == "_" for ch in physical
        ):
            raise SubstrateError(
                "the node table's interval storage has an unexpected name",
                data={"physical_table": physical},
            )
        return physical

    def _assert_window_retained(
        self,
        db: Any,
        generations: Sequence[LineageGeneration],
        *,
        deployment_name: str,
    ) -> None:
        """Refuse a read whose window lost a checkpoint while it ran."""
        from okg.substrate.history_read_guard import raise_if_compacted

        ids = [g.generation_id for g in generations]
        rows = _interval_rows(
            db,
            "SELECT checkpoint_id FROM _chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = ANY(?::text[])",
            (ids,),
        )
        present = {str(row["checkpoint_id"]) for row in rows}
        for generation_id in ids:
            if generation_id in present:
                continue
            raise_if_compacted(db, generation_id, deployment_name=deployment_name)
            raise SubstrateError(
                f"generation {generation_id!r} was removed while its history was read",
                data={"generation_id": generation_id},
                fix_hint="read the history again",
            )

    def node_version_history(
        self,
        node_id: str,
        *,
        branch: str,
        deployment_name: str,
        from_generation: Optional[str] = None,
        to_generation: str,
        row_budget: int = NODE_HISTORY_ROW_BUDGET,
    ) -> NodeHistory:
        """The node's row version at every generation from ``from`` to ``to``.

        One read of the node's physical rows on the branch's segment chain
        (``(node_id, live_lo)`` primary key), mapped to each window generation
        by its stab position. Over ``row_budget`` rows is refused.
        """
        db = self._history_db()
        chain, window = self.history_window(
            branch=branch,
            deployment_name=deployment_name,
            from_generation=from_generation,
            to_generation=to_generation,
        )
        physical = self._node_physical_table(db)
        # Only rows that can hold a window generation's stab position: a row
        # maps to a generation only when live_lo <= stab < live_hi, so a row
        # outside [min stab, max stab] maps to none, and the budget counts
        # the versions of the range read, not of the node's whole life.
        stabs = [int(generation.stab) for generation in window]
        rows = _interval_rows(
            db,
            f"""
            SELECT subtype, attrs, source, source_record_id, source_revision,
                   live_lo, live_hi, deleted
              FROM public."{physical}"
             WHERE node_id = ?
               AND writer_segment_id = ANY(?::bigint[])
               AND live_lo <= ?
               AND live_hi > ?
             ORDER BY live_lo
             LIMIT ?
            """,
            (str(node_id), chain, max(stabs), min(stabs), int(row_budget) + 1),
        )
        if len(rows) > row_budget:
            raise HistoryRowBudgetExceeded(
                f"node history scan exceeds its budget of {row_budget} row versions",
                data={
                    "budget": int(row_budget),
                    "budget_name": "node_history_row_budget",
                    "target_kind": "node_history",
                },
                fix_hint=(
                    "this node has too many stored versions in the range to read "
                    "as one history; pass a narrower from and to"
                ),
            )
        self._assert_window_retained(db, window, deployment_name=deployment_name)
        for earlier, later in zip(rows, rows[1:]):
            if int(earlier["live_hi"]) > int(later["live_lo"]):
                raise SubstrateError(
                    "two stored versions of one node overlap on the branch's chain",
                    data={
                        "node_id": node_id,
                        "positions": [int(earlier["live_lo"]), int(later["live_lo"])],
                    },
                    fix_hint="report this storage invariant violation",
                )
        lows = [int(row["live_lo"]) for row in rows]
        states: list[Optional[NodeRowVersion]] = []
        for generation in window:
            at = bisect_right(lows, generation.stab) - 1
            row = rows[at] if at >= 0 else None
            if (
                row is None
                or not (int(row["live_lo"]) <= generation.stab < int(row["live_hi"]))
                or bool(row["deleted"])
            ):
                states.append(None)
                continue
            states.append(
                NodeRowVersion(
                    live_lo=int(row["live_lo"]),
                    live_hi=int(row["live_hi"]),
                    subtype=None if row["subtype"] is None else str(row["subtype"]),
                    attrs=dict(row["attrs"] or {}),
                    source=None if row["source"] is None else str(row["source"]),
                    source_record_id=row["source_record_id"],
                    source_revision=row["source_revision"],
                )
            )
        return NodeHistory(
            generations=tuple(window),
            states=tuple(states),
            rows_scanned=len(rows),
            row_budget=int(row_budget),
        )


__all__ = [
    "ChronosGraphNodeHistoryMixin",
    "HistoryGenerationUnordered",
    "HistoryRangeInvalid",
    "HistoryRowBudgetExceeded",
    "LineageGeneration",
    "NODE_HISTORY_ROW_BUDGET",
    "NodeHistory",
    "NodeRowVersion",
]
