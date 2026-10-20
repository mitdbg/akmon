"""Pre-lock planning for deterministic edge linkers (issue 2941 fix 1).

A linker that mixes in :class:`PrelockPlannableLinker` lets the publisher
compute its output before the publish lock. The linker supplies:

* ``_prelock_edges(cur, incremental, timings_ms)``: the edges its ``enrich``
  would emit, or None when ``enrich`` returns early with nothing;
* ``_edge_rows(edges, *, generation_id)``: the emit rows, in the shared
  layout ``(edge_id, src, dst, edge_type, attrs_json, source_record_id_json,
  source_revision_json, observed_at, dedupe_key, run_id)``;
* ``_rows_to_insert(cur, rows)``: its "already said this" check;
* ``_insert_rows(cur, rows)``: the INSERT.

``prelock_plan`` runs the first three and writes nothing. It keeps every
candidate row (the resolve output before the "already said this" check) as
well as the rows that check would insert now. ``enrich`` given a plan runs
the check again on the candidate rows, under the lock, and inserts what it
returns, stamping ``observed_at`` at insert time. The check reads the fact
log across branches, which a concurrent writer can change and then restore
(the merge-back gate inserts facts and deletes them again under the lock);
no fingerprint of the fact log proves it unchanged, so the inserted set is
always decided under the lock and equals what the in-lock path would insert.
The publisher passes a plan back only when the graph fingerprint and the
enricher's ``IncrementalContext`` are the ones the plan was made from; the
linker checks the context again before inserting.

Only linkers that read nothing but the branch graph and their own prior edge
facts, and are deterministic, may opt in.

The pre-lock pass runs with ``default_transaction_read_only`` on. That guard
refuses writes to ordinary tables only: PostgreSQL still allows creating and
writing temporary tables, taking session advisory locks, and ``SET`` /
``set_config`` under it. An opted-in linker must not rely on the guard to
stop those; its ``_prelock_edges`` and ``_rows_to_insert`` must not create
temporary tables, take advisory locks or change session settings.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from okg.substrate.enrichers.base import (
    EnricherPrelockPlan,
    EnrichResult,
    IncrementalContext,
)


class PrelockPlannableLinker:
    """Shared ``prelock_plan`` / plan-driven ``enrich`` for edge linkers."""

    name: str
    prelock_plannable = True
    # Whether the linker's own emit wraps its INSERTs in one transaction;
    # the plan-driven insert keeps the same behaviour.
    _prelock_insert_in_transaction = False

    def _prelock_edges(
        self,
        cur: Any,
        incremental: IncrementalContext | None,
        timings_ms: dict[str, float],
    ) -> list[tuple[Any, ...]] | None:
        raise NotImplementedError

    def _prelock_resolve(
        self,
        cur: Any,
        incremental: IncrementalContext | None,
        timings_ms: dict[str, float],
    ) -> tuple[list[tuple[Any, ...]] | None, Any]:
        """``_prelock_edges`` plus state kept on the plan as
        ``linker_state`` for :meth:`_finish_from_plan`."""
        return self._prelock_edges(cur, incremental, timings_ms), None

    def _finish_from_plan(
        self,
        cur: Any,
        plan: EnricherPrelockPlan,
        *,
        generation_id: int,
        timings_ms: dict[str, float],
    ) -> int:
        """Work the linker finishes under the lock after the plan's insert;
        returns the facts it wrote. None by default."""
        return 0

    def _edge_rows(
        self, edges: list[tuple[Any, ...]], *, generation_id: int,
    ) -> list[tuple[Any, ...]]:
        raise NotImplementedError

    def _rows_to_insert(
        self, cur: Any, rows: list[tuple[Any, ...]],
    ) -> list[tuple[Any, ...]]:
        raise NotImplementedError

    def _insert_rows(self, cur: Any, rows: list[tuple[Any, ...]]) -> None:
        raise NotImplementedError

    def prelock_plan(
        self,
        conn: Any,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnricherPrelockPlan:
        """Compute the rows ``enrich`` would insert, writing nothing."""
        timings_ms: dict[str, float] = {}
        started = time.perf_counter()
        with conn.cursor() as cur:
            edges, linker_state = self._prelock_resolve(
                cur, incremental, timings_ms,
            )
            timings_ms.setdefault("resolve_edges", round(
                (time.perf_counter() - started) * 1000.0, 3,
            ))
            if not edges:
                return EnricherPrelockPlan(
                    enricher_name=self.name,
                    generation_id=generation_id,
                    incremental=incremental,
                    rows=(),
                    candidate_rows=(),
                    n_skipped=0,
                    run_id=None,
                    timings_ms=timings_ms,
                    linker_state=linker_state,
                )
            started = time.perf_counter()
            rows = self._edge_rows(edges, generation_id=generation_id)
            to_insert = self._rows_to_insert(cur, rows)
            timings_ms["emit_edges.plan"] = round(
                (time.perf_counter() - started) * 1000.0, 3,
            )
        return EnricherPrelockPlan(
            enricher_name=self.name,
            generation_id=generation_id,
            incremental=incremental,
            rows=tuple(to_insert),
            candidate_rows=tuple(rows),
            n_skipped=len(rows) - len(to_insert),
            run_id=str(rows[0][9]) if rows else None,
            timings_ms=timings_ms,
            linker_state=linker_state,
        )

    def _enrich_from_plan(
        self,
        cur: Any,
        plan: EnricherPrelockPlan,
        *,
        generation_id: int,
        incremental: IncrementalContext | None,
    ) -> EnrichResult:
        """Insert a pre-lock plan's rows; refuses a plan for other inputs."""
        if (
            plan.enricher_name != self.name
            or plan.generation_id != generation_id
            or plan.incremental != incremental
        ):
            raise ValueError(
                f"{self.name} was offered a pre-lock plan made for "
                "different inputs"
            )
        timings_ms = {
            f"prelock.{label}": value
            for label, value in plan.timings_ms.items()
        }
        started = time.perf_counter()
        candidates = list(plan.candidate_rows)

        def _recheck_and_insert() -> tuple[int, int]:
            # The "already said this" check, again, under the lock: the
            # pre-lock answer is only a forecast (see the module docstring).
            to_insert = self._rows_to_insert(cur, candidates)
            timings_ms["emit_edges.dedupe_recheck"] = round(
                (time.perf_counter() - started) * 1000.0, 3,
            )
            if [row[8] for row in to_insert] != [row[8] for row in plan.rows]:
                # The derived-fact apply's prepared chunks were computed
                # from plan.rows; each chunk compares its facts and
                # recomputes on a mismatch.
                timings_ms["prelock.dedupe_changed"] = 1.0
            if to_insert:
                observed_at = datetime.now(timezone.utc)
                self._insert_rows(cur, [
                    (*row[:7], observed_at, *row[8:]) for row in to_insert
                ])
            return len(to_insert), len(candidates) - len(to_insert)

        emitted = skipped = 0
        if candidates:
            if self._prelock_insert_in_transaction:
                with cur.connection.transaction():
                    emitted, skipped = _recheck_and_insert()
            else:
                emitted, skipped = _recheck_and_insert()
        timings_ms["emit_edges"] = round(
            (time.perf_counter() - started) * 1000.0, 3,
        )
        emitted += self._finish_from_plan(
            cur, plan, generation_id=generation_id, timings_ms=timings_ms,
        )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=emitted,
            n_edges_skipped=skipped,
            timings_ms=timings_ms,
        )


__all__ = ["PrelockPlannableLinker"]
