"""Publish-cycle adapter for native V1 view artifacts.

This is the replacement publication path for ``ViewMaterializer``:
the views framework still owns planning, refresh, and assembly, but
Postgres derived-artifact manifests are the authority. This stage does
not write deployment-local ``manifest.json`` / ``view.md`` files or
legacy ``view`` graph nodes.
"""

from __future__ import annotations

from contextlib import nullcontext
import hashlib
import inspect
import json
import logging
import threading
import time
from collections.abc import Sequence
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Mapping
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.catalog.invariants import sql_contains_writes
from okg.substrate.derived_artifacts import (
    DerivedArtifactDependency,
    InlineViewOutput,
    RelationArtifactDefinition,
    RelationRow,
    RelationScheduleTask,
    ViewArtifactDefinition,
    ViewScheduleTask,
    register_dependency,
    register_jsonb_relation,
    register_view_artifact_definition,
    schedule_derived_artifacts,
    stable_hash,
)
from okg.substrate.derived_artifacts.dependency_index import publish_lineage_envelopes
from okg.substrate.derived_artifacts.output_lineage import (
    build_output_lineage_envelope,
    source_record_lineage,
    source_records_from_node_rows,
)
from okg.substrate.derived_artifacts.relations import (
    RELATION_SLOT_NAME,
    relation_visible_hash_rows_for_manifest,
    relation_visible_stats_for_manifest,
)
from okg.substrate.derived_artifacts.scheduler import LineageInvalidation
from okg.substrate.derived_artifacts.views import _carry_forward_view_artifact_in_cursor
from okg.substrate.views.base import (
    MaterializeOutcome,
    ViewDeltaScope,
    ViewMaterializer,
)
from okg.views.assembly import (
    AssembledWithMarkers,
    assemble as default_assemble,
)
from okg.views.persistence import view_from_dict, view_to_dict
from okg.views.planners.base import (
    ViewOutputTemplate,
    default_output_templates,
)
from okg.views.refresh import refresh as refresh_view

logger = logging.getLogger(__name__)

_PERSIST_DEGRADED_OUTCOMES_SQL = """
UPDATE okg.derived_output_manifests
   SET metadata = metadata || %(degradation)s
 WHERE manifest_id = %(manifest_id)s
"""

# `unify-dirty-set-authority`: the dependency kinds a native-view envelope
# declares (graph_node from cited nodes; source/component keys from the
# provenance). graph_edge is intentionally excluded — view envelopes do not
# declare cited edges, so feeding edge keys to lookup_dirty_outputs would make
# every kind "unavailable" and force a full rebuild. View-level edge dependency
# is a follow-up that requires envelopes to declare cited edges.
_INDEXED_VIEW_INVALIDATION_KINDS = frozenset(
    {
        "graph_node",
        "source_record",
        "source_content_hash",
        "component",
        "component_hash",
    }
)
_INPUT_SIGNATURE_MATCH_CACHE_MAX = 4096
_INPUT_SIGNATURE_MATCH_CACHE: dict[tuple[str, str, int, int], bool] = {}
_INPUT_SIGNATURE_MATCH_CACHE_LOCK = threading.Lock()


class _GovernedLineagePublicationError(ValueError):
    """Lineage failed after the scheduler may have committed output runs."""

    def __init__(self, *, run_ids: Sequence[uuid.UUID]) -> None:
        self.run_ids = tuple(run_ids)
        super().__init__("governed native view lineage could not be persisted")


def _require_read_asset_execution_token(materializer: ViewMaterializer) -> None:
    from okg.substrate.contracts.deployment_product.read_assets import (
        ReadAssetExecutionToken,
    )

    token = getattr(materializer, "asset_execution_token", None)
    asset_id = getattr(materializer, "asset_id", None)
    asset_digest = getattr(materializer, "asset_digest", None)
    observed_policy_digest = None
    if isinstance(token, ReadAssetExecutionToken):
        observed_policy_digest = "sha256:" + hashlib.sha256(
            json.dumps(
                dict(token.policy_requirements),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
    decisions = (
        tuple(token.policy_requirements.get("decision_ids") or ())
        if isinstance(token, ReadAssetExecutionToken)
        else ()
    )
    if (
        not isinstance(token, ReadAssetExecutionToken)
        or token.asset_id != asset_id
        or token.asset_digest != asset_digest
        or token.policy_digest != observed_policy_digest
        or not token.lineage_sources
        or not decisions
        or any(not item.startswith("nomos:") for item in decisions)
    ):
        raise ValueError(
            "native view canonical identity lacks its exact governed read-asset resolution token"
        )


def _governed_view_definition(materializer: ViewMaterializer):
    """Resolve one factory only after proving its canonical runtime identity."""

    _require_read_asset_execution_token(materializer)
    view_def = materializer.view_def_factory()
    token = materializer.asset_execution_token
    runtime_view_id = getattr(view_def, "view_id", None)
    if (
        runtime_view_id != token.runtime_identity
        or (materializer.name and materializer.name != token.runtime_identity)
    ):
        raise ValueError(
            "native view factory/materializer identity does not match its canonical read asset"
        )
    return view_def


class NativeViewStage:
    """Run registered ``ViewMaterializer`` adapters as V1 artifacts."""

    def __init__(
        self,
        materializers: list[ViewMaterializer],
        *,
        deployment_name: str,
    ) -> None:
        self._materializers = list(materializers)
        for materializer in self._materializers:
            _require_read_asset_execution_token(materializer)
        self._deployment_name = deployment_name
        self._last_failures: tuple[dict[str, str], ...] = ()
        self._last_degradations: tuple[dict[str, Any], ...] = ()

    @property
    def last_failures(self) -> tuple[dict[str, str], ...]:
        """``{"view_id", "error"}`` for every view that raised in the last
        :meth:`run`, newest run only; empty when every view materialized.

        The stage isolates per view (see :meth:`run`), which is correct —
        but the caller MUST read this. A caller that only counts outcomes
        reports a clean publish while a view has been failing on every
        cycle, which is what happened to okg-workspace's `today_brief`
        after #1153 removed `valid_from_gen` from `okg.v_nodes`: the
        materializer raised on every publish for weeks and nothing said so.
        """
        return self._last_failures

    @property
    def last_degradations(self) -> tuple[dict[str, Any], ...]:
        """Typed optional-work omissions from the newest stage run."""
        return self._last_degradations

    @property
    def materializers(self) -> list[ViewMaterializer]:
        """Read-only view of registered materializers."""

        return list(self._materializers)

    def latest_completed_at(
        self,
        conn: psycopg.Connection[Any],
    ) -> datetime | None:
        """Return the latest completed native-view run timestamp.

        Used by the publisher's refresh coalescer to enforce a minimum
        refresh interval without adding a new scheduler table. If view
        definitions cannot be resolved, fail loudly rather than materializing
        on an unknown authority basis.
        """

        definition_ids: list[str] = []
        try:
            for materializer in self._materializers:
                view_def = _governed_view_definition(materializer)
                definition_ids.append(f"view:{view_def.view_id}")
        except Exception:
            logger.exception("native view refresh timestamp lookup failed")
            raise
        if not definition_ids:
            return None
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(completed_at)
                  FROM okg.derived_artifact_runs
                 WHERE status = 'completed'
                   AND definition_id = ANY(%s)
                """,
                (definition_ids,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return row[0]

    def completed_watermark_at(
        self,
        conn: psycopg.Connection[Any],
    ) -> datetime | None:
        """Return the time all registered native views have completed through.

        A single latest completed run is not a safe consumption watermark for
        cross-view deltas: if one materializer fails while another completes, the
        global max advances even though the failed view has not consumed the
        delta. This returns the minimum of each view definition's latest
        completion, or ``None`` until every registered definition has completed at
        least once.
        """

        definition_ids: list[str] = []
        try:
            for materializer in self._materializers:
                view_def = _governed_view_definition(materializer)
                definition_ids.append(f"view:{view_def.view_id}")
        except Exception:
            logger.exception("native view completion watermark lookup failed")
            raise
        if not definition_ids:
            return None
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH requested(definition_id) AS (
                  SELECT unnest(%(definition_ids)s::text[])
                ),
                latest AS (
                  SELECT r.definition_id, max(run.completed_at) AS completed_at
                    FROM requested r
                    LEFT JOIN okg.derived_artifact_runs run
                      ON run.definition_id = r.definition_id
                     AND run.status = 'completed'
                   GROUP BY r.definition_id
                )
                SELECT
                  count(*) AS requested_count,
                  count(completed_at) AS completed_count,
                  min(completed_at) AS watermark
                  FROM latest
                """,
                {"definition_ids": definition_ids},
            )
            row = cur.fetchone()
        if row is None or int(row[1] or 0) < int(row[0] or 0):
            return None
        return row[2]

    def run(
        self,
        conn: psycopg.Connection[Any],
        *,
        generation_id: int,
        published_at: datetime | None = None,
        catalog_version_id: int | None = None,
        lineage_invalidations: Sequence[LineageInvalidation] = (),
        reads_affected_view_ids: frozenset[str] = frozenset(),
        reads_deltas: Mapping[
            str,
            tuple[
                dict[str, list[str]],
                dict[str, list[int]],
                bool,
                dict[str, list[dict[str, Any]]],
                dict[str, dict[str, list[str]]],
            ],
        ]
        | None = None,
        dirty_scope_unavailable: bool = False,
        graph_store: Any | None = None,
        caller_owned_transaction: bool = False,
    ) -> list[MaterializeOutcome]:
        """Render every registered materializer through the V1 scheduler.

        Failures are isolated per view, matching the old materializer
        stage and the enricher discipline: a broken view records a
        failed outcome and does not abort the publish cycle. This stage runs
        after graph admission gates but before the Chronos checkpoint is
        inserted. Its artifacts are explicitly non-gating; isolating a broken
        deployment-authored dashboard keeps that display bug from preventing
        the admitted graph checkpoint while still reporting degradation.

        Isolation is not silence, though, and that distinction is the
        point of `last_failures`: every failed view is collected, logged
        once as a named stage-level error, and exposed for the caller to
        put in the publish output. Per-view logging alone was not enough —
        it left a green publish result with no failure count in it.

        `deltaflow-publish-projection`: when the publisher supplies the
        cycle's change set (`lineage_invalidations`, the graph nodes
        written this cycle), a view that would otherwise carry its corpus
        forward is rebuilt iff one of its cited nodes is in that change
        set. This only ever turns a carry-forward into a rebuild — never
        the reverse — so it cannot serve stale outputs; it makes views
        update on a real input change that the (fact-log) watermark misses
        under chronos-native publishing.
        """

        outcomes: list[MaterializeOutcome] = []
        self._last_failures = ()
        self._last_degradations = ()
        if not self._materializers:
            return outcomes

        input_fact_watermark, input_edge_fact_watermark = self._read_input_watermarks(
            conn
        )
        for materializer in self._materializers:
            started = time.perf_counter()
            view_savepoint: str | None = None
            view_transaction = nullcontext()
            _require_read_asset_execution_token(materializer)
            if caller_owned_transaction:
                if (
                    conn.info.transaction_status
                    != psycopg.pq.TransactionStatus.INTRANS
                ):
                    raise RuntimeError(
                        "caller-owned publication transaction ended before "
                        f"native view {materializer.name!r}"
                    )
                # Atomic publication owns the outer transaction. A psycopg
                # subtransaction both isolates this non-gating view and
                # rejects any nested ``conn.commit()`` / ``conn.rollback()``.
                view_transaction = conn.transaction()
            elif getattr(conn, "autocommit", True) is False:
                # Use a raw savepoint instead of ``conn.transaction()``.
                # Native-view citation/lineage work can perform Chronos
                # checkouts whose adapter explicitly commits; psycopg forbids
                # that inside a Transaction context.  The raw savepoint still
                # contains ordinary materializer SQL (including build_corpus)
                # without imposing that incompatible client-side guard.
                view_savepoint = f"okg_native_view_{uuid.uuid4().hex}"
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SAVEPOINT {}").format(
                            sql.Identifier(view_savepoint)
                        )
                    )
            try:
                with view_transaction:
                    outcome = self._run_one(
                        conn,
                        materializer,
                        generation_id=generation_id,
                        published_at=published_at,
                        catalog_version_id=catalog_version_id,
                        input_fact_watermark=input_fact_watermark,
                        input_edge_fact_watermark=input_edge_fact_watermark,
                        lineage_invalidations=lineage_invalidations,
                        reads_affected=(
                            materializer.name in reads_affected_view_ids
                        ),
                        reads_delta=(reads_deltas or {}).get(materializer.name),
                        dirty_scope_unavailable=dirty_scope_unavailable,
                        graph_store=graph_store,
                    )
                outcome.asset_id = materializer.asset_id
                outcome.asset_digest = materializer.asset_digest
                outcome.elapsed_ms = round(
                    max(0.0, (time.perf_counter() - started) * 1000.0),
                    3,
                )
                outcomes.append(outcome)
            except Exception as exc:  # noqa: BLE001
                if view_savepoint is not None:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                sql.SQL("ROLLBACK TO SAVEPOINT {}").format(
                                    sql.Identifier(view_savepoint)
                                )
                            )
                            cur.execute(
                                sql.SQL("RELEASE SAVEPOINT {}").format(
                                    sql.Identifier(view_savepoint)
                                )
                            )
                    except psycopg.Error:
                        # A Chronos checkout can explicitly commit and thereby
                        # release the savepoint.  If a later view operation
                        # then fails, only that post-checkout transaction is
                        # still pending and needs rollback.
                        conn.rollback()
                if isinstance(exc, _GovernedLineagePublicationError):
                    self._invalidate_unlined_runs(
                        conn,
                        run_ids=exc.run_ids,
                        generation_id=generation_id,
                    )
                view_id = materializer.name or "<unresolved>"
                logger.exception(
                    "native view materializer %r raised at gen=%d; "
                    "continuing without blocking the publish",
                    view_id,
                    generation_id,
                )
                outcomes.append(
                    MaterializeOutcome(
                        view_id=view_id,
                        generation_id=generation_id,
                        asset_id=materializer.asset_id,
                        asset_digest=materializer.asset_digest,
                        freshness="failed",
                        status="failed",
                        error=str(exc),
                        elapsed_ms=round(
                            max(0.0, (time.perf_counter() - started) * 1000.0),
                            3,
                        ),
                    )
                )
        # One named, stage-level error for the whole run. The per-view
        # `logger.exception` above is a stack trace among many; this is the
        # line that says the STAGE is degraded and which views are down, and
        # `last_failures` is the same fact in a form the publish output can
        # carry. Without both, "continuing without blocking the publish"
        # reads as a clean cycle to everything downstream.
        self._last_failures = tuple(
            {"view_id": outcome.view_id, "error": str(outcome.error)}
            for outcome in outcomes
            if outcome.freshness == "failed"
        )
        self._last_degradations = tuple(
            {
                "view_id": outcome.view_id,
                "status": outcome.status,
                "cause": outcome.cause,
                "omitted_work": list(outcome.omitted_work),
                "scope": dict(outcome.scope),
                "retryable": outcome.retryable,
                "freshness": outcome.freshness,
            }
            for outcome in outcomes
            if outcome.status == "degraded"
        )
        if self._last_failures:
            logger.error(
                "native view stage DEGRADED at gen=%d: %d of %d view(s) "
                "failed to materialize and are serving no fresh output: %s. "
                "The publish is not blocked (native views are non-gating "
                "derived artifacts), but this is a failure, not a no-op.",
                generation_id,
                len(self._last_failures),
                len(self._materializers),
                "; ".join(
                    f"{failure['view_id']}: {failure['error']}"
                    for failure in self._last_failures
                ),
            )
        return outcomes

    @staticmethod
    def _invalidate_unlined_runs(
        conn: psycopg.Connection[Any],
        *,
        run_ids: Sequence[uuid.UUID],
        generation_id: int,
    ) -> None:
        """Hide scheduler-committed output whose governed lineage failed.

        Chronos checkout can commit during one materializer and release the
        caller's savepoint. In that case the artifact run is already durable
        when lineage collection fails. Mark only the exact scheduler results
        failed so canonical readers cannot select the unlined manifests.
        """

        if not run_ids:
            return
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.derived_artifact_runs
                   SET status = 'failed',
                       error = %(error)s,
                       completed_at = now()
                 WHERE run_id = ANY(%(run_ids)s)
                   AND target_generation_id = %(generation_id)s
                   AND status IN ('completed', 'carried_forward')
                """,
                {
                    "run_ids": list(run_ids),
                    "generation_id": generation_id,
                    "error": Jsonb({
                        "code": "governed_native_lineage_unavailable",
                        "message": (
                            "native view output refused because its exact "
                            "lineage envelope could not be persisted"
                        ),
                    }),
                },
            )

    def can_reuse_noop_generation(
        self,
        conn: psycopg.Connection[Any],
        *,
        generation_id: int,
    ) -> bool:
        """Return true when a proven no-op may reuse prior view artifacts.

        The publisher only calls this after proving the graph delta is zero and
        checking catalog, gate-input, policy-pin, and lens invalidators. Under
        Chronos-native publishing the fact watermark can be stale even when the
        graph is unchanged, so no-op generation reuse must key on artifact
        definition stability rather than the view carry-forward input signature.
        """

        if not self._materializers:
            return True
        for materializer in self._materializers:
            try:
                view_def = _governed_view_definition(materializer)
                output_templates = _output_templates_for(view_def)
                definition = _definition_for(
                    view_def=view_def,
                    deployment_name=self._deployment_name,
                    anchor_resolver=materializer.anchor_resolver,
                    output_templates=output_templates,
                )
                relation_plan = _relation_plan_for_templates(
                    templates=output_templates,
                    view_def=view_def,
                    deployment_name=self._deployment_name,
                )
                if not _definitions_refreshable(
                    conn,
                    view_definition=definition,
                    relation_definitions=[item.definition for item in relation_plan],
                    generation_id=generation_id,
                ):
                    return False
            except Exception:
                logger.exception(
                    "native view no-op reuse check failed for %r",
                    materializer.name,
                )
                raise
        return True

    def _run_one(
        self,
        conn: psycopg.Connection[Any],
        materializer: ViewMaterializer,
        *,
        generation_id: int,
        published_at: datetime | None,
        catalog_version_id: int | None,
        input_fact_watermark: int,
        input_edge_fact_watermark: int,
        lineage_invalidations: Sequence[LineageInvalidation] = (),
        reads_affected: bool = False,
        reads_delta: tuple[dict[str, list[str]], dict[str, list[int]], bool]
        | tuple[
            dict[str, list[str]],
            dict[str, list[int]],
            bool,
            dict[str, list[dict[str, Any]]],
            dict[str, dict[str, list[str]]],
        ]
        | None = None,
        dirty_scope_unavailable: bool = False,
        graph_store: Any | None = None,
    ) -> MaterializeOutcome:
        total_started = time.perf_counter()
        timings_ms: dict[str, float] = {}

        def _record_timing(name: str, started: float) -> None:
            elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
            timings_ms[name] = round(timings_ms.get(name, 0.0) + elapsed, 3)

        _stage_t0 = time.perf_counter()
        view_def = _governed_view_definition(materializer)
        view_id = view_def.view_id
        published_at_aware = (
            published_at is not None
            and _build_corpus_accepts_kwarg(
                materializer.build_corpus,
                "published_at",
            )
        )
        output_templates = _output_templates_for(view_def)
        definition = _definition_for(
            view_def=view_def,
            deployment_name=self._deployment_name,
            anchor_resolver=materializer.anchor_resolver,
            output_templates=output_templates,
        )
        relation_plan = _relation_plan_for_templates(
            templates=output_templates,
            view_def=view_def,
            deployment_name=self._deployment_name,
        )
        _record_timing("definition_setup", _stage_t0)
        _stage_t0 = time.perf_counter()
        refreshable_manifest_ids = _refreshable_manifest_ids(
            conn,
            view_definition=definition,
            relation_definitions=[item.definition for item in relation_plan],
            generation_id=generation_id,
        )
        definition_stable = refreshable_manifest_ids is not None
        _record_timing("definition_stability_probe", _stage_t0)
        _stage_t0 = time.perf_counter()
        carry_forward = False
        declared_reads = bool(
            getattr(materializer, "reads_subtypes", ())
            or getattr(materializer, "reads_edge_types", ())
        )
        if (
            definition_stable
            and dirty_scope_unavailable
        ):
            timings_ms["carry_forward_probe.skipped_dirty_scope_unavailable"] = 1.0
        elif (
            definition_stable
            and declared_reads
            and reads_affected
        ):
            # A complete declared-read delta touched this view's input surface,
            # so the input watermark signature cannot justify carry-forward.
            timings_ms["carry_forward_probe.skipped_reads_affected"] = 1.0
        elif (
            definition_stable
            and declared_reads
            and reads_delta is None
        ):
            carry_forward = True
            timings_ms["chronos_semantic_carry_forward"] = 1.0
            timings_ms["carry_forward_probe.skipped_declared_reads"] = 1.0
        elif definition_stable:
            carry_forward = _cached_input_signature_matches(
                conn,
                definition_id=definition.definition_id,
                before_generation_id=generation_id,
                input_fact_watermark=input_fact_watermark,
                input_edge_fact_watermark=input_edge_fact_watermark,
            )
        _record_timing("carry_forward_probe", _stage_t0)
        # A corpus that explicitly accepts the per-generation publication
        # timestamp declares that value as an input. Every new checkpoint gets
        # a new timestamp even when graph facts are unchanged, so neither a
        # whole-view carry-forward nor a scoped partial rebuild can preserve
        # prior outputs honestly: eyebrow labels and relation observed_at rows
        # would retain the predecessor's timestamp. Rebuild the full corpus.
        if carry_forward and published_at_aware:
            carry_forward = False
            timings_ms["published_at_invalidation"] = 1.0
        # `harden-deltaflow-frontier-blockers`: when the publisher could not
        # compute the cycle's Chronos dirty scope (the segment-delta query
        # returned None — non-interval tables or a swallowed query error), there
        # is NO trustworthy change signal for this cycle. Under chronos-native
        # the input-fact watermark is dead, so `_can_carry_forward_without_corpus`
        # returns True and a carry-forward here would silently serve a stale
        # corpus. We cannot prove the view's inputs are unchanged, so force a
        # rebuild (the publisher logs the unavailability loudly). Like the
        # frontier/reads gates below, this only ever cancels a carry-forward —
        # it never skips a rebuild — so it cannot serve stale output.
        if carry_forward and dirty_scope_unavailable:
            carry_forward = False
        # Definition-stability gate for compute-incremental extraction below: True
        # iff every output definition has a prior completed manifest whose hashes
        # match the current definition, so the scheduler will take the scope-level
        # `refresh_relation_artifact` carry-forward path. Only then is it SAFE to
        # hand build_corpus a delta scope and emit partial rows — otherwise a full
        # publish would treat partial rows as the complete relation and drop clean
        # scopes (a P0). NOT `carry_forward`: that also requires the input-fact
        # watermark to match, which is always stale under chronos-native (empty
        # fact log), so it would suppress every scoped run.
        # `deltaflow-publish-projection`: the change set is the authority on
        # whether THIS view's inputs moved. If the (fact-log) watermark
        # would carry the corpus forward but a node this view cited last
        # time is in the cycle's change set, rebuild instead. This only
        # ever cancels a carry-forward — it can never skip a rebuild — so
        # it cannot serve a stale output.
        #
        # `harden-akmon-incremental-machine`: the declared reads gate is a
        # cheaper proof than the lineage frontier. If the cycle touched a
        # declared subtype/edge type, the view is definitely affected, so avoid
        # the reverse-index probe that the old ordering paid anyway.
        if carry_forward and reads_affected:
            carry_forward = False
            timings_ms["reads_affected_gate"] = 1.0

        _stage_t0 = time.perf_counter()
        if (
            carry_forward
            and lineage_invalidations
            and (
                self._view_affected_by_frontier(
                    conn,
                    definition=definition,
                    generation_id=generation_id,
                    lineage_invalidations=lineage_invalidations,
                )
            )
        ):
            carry_forward = False
        _record_timing("frontier_probe", _stage_t0)
        if carry_forward:
            outcome = _carry_forward_without_corpus(
                conn,
                view_def=view_def,
                definition=definition,
                relation_plan=relation_plan,
                deployment_name=self._deployment_name,
                generation_id=generation_id,
                catalog_version_id=catalog_version_id,
                trusted_previous_manifest_ids=refreshable_manifest_ids,
                use_transaction=False,
            )
            outcome.timings_ms = {
                **timings_ms,
                **{
                    f"carry_forward.{label}": float(elapsed_ms)
                    for label, elapsed_ms in outcome.timings_ms.items()
                },
            }
            return outcome
        # Compute-incremental extraction (opt-in). When the definition is stable
        # (refresh path guaranteed), this cycle's change set is enumerable, and
        # build_corpus accepts a `delta_scope`, hand it the changed entities so
        # it scans O(delta) and stages only the affected scopes' rows. It
        # populates delta_scope.dirty_scope_keys with the COMPLETE set of
        # affected scopes (incl. deletions); we feed that to the relation task
        # so the scheduler carries every other scope forward.
        delta_scope: ViewDeltaScope | None = None
        if (
            definition_stable
            and not published_at_aware
            and reads_delta is not None
            and reads_delta[2]  # can_scope: changed entities fully enumerable
            and _build_corpus_accepts_delta(materializer.build_corpus)
        ):
            changed_nodes = reads_delta[0]
            changed_edges = reads_delta[1]
            changed_edge_endpoints = reads_delta[3] if len(reads_delta) > 3 else {}
            changed_node_attr_fields = (
                reads_delta[4] if len(reads_delta) > 4 else {}
            )
            delta_scope = ViewDeltaScope(
                changed_nodes={k: list(v) for k, v in changed_nodes.items()},
                changed_edges={k: list(v) for k, v in changed_edges.items()},
                changed_node_attr_fields={
                    k: {
                        str(node_id): list(fields)
                        for node_id, fields in v.items()
                    }
                    for k, v in changed_node_attr_fields.items()
                },
                changed_edge_endpoints={
                    k: [dict(endpoint) for endpoint in v]
                    for k, v in changed_edge_endpoints.items()
                },
            )
        _stage_t0 = time.perf_counter()
        build_corpus_kwargs: dict[str, Any] = {"generation_id": generation_id}
        if published_at_aware:
            build_corpus_kwargs["published_at"] = published_at
        if delta_scope is not None:
            build_corpus_kwargs["delta_scope"] = delta_scope
        if graph_store is not None and _build_corpus_accepts_kwarg(
            materializer.build_corpus,
            "graph_store",
        ):
            build_corpus_kwargs["graph_store"] = graph_store
        corpus = materializer.build_corpus(conn, **build_corpus_kwargs)
        _record_timing("build_corpus", _stage_t0)
        if delta_scope is not None:
            for label, elapsed_ms in delta_scope.timings_ms.items():
                try:
                    timings_ms[f"build_corpus.{label}"] = round(
                        float(elapsed_ms),
                        3,
                    )
                except (TypeError, ValueError):
                    continue
        corpus_build_ms = timings_ms["build_corpus"]
        if delta_scope is not None and delta_scope.scoped:
            logger.info(
                "native view %r build_corpus scoped to %d dirty scope(s) in "
                "%.1f ms (O(delta) extraction)",
                view_id,
                len(delta_scope.dirty_scope_keys),
                corpus_build_ms,
            )
            if not delta_scope.dirty_scope_keys:
                timings_ms["delta_scope_empty_carry_forward"] = 1.0
                outcome = _carry_forward_without_corpus(
                    conn,
                    view_def=view_def,
                    definition=definition,
                    relation_plan=relation_plan,
                    deployment_name=self._deployment_name,
                    generation_id=generation_id,
                    catalog_version_id=catalog_version_id,
                    trusted_previous_manifest_ids=refreshable_manifest_ids,
                    use_transaction=False,
                )
                outcome.corpus_build_ms = corpus_build_ms
                outcome.corpus_scan_scoped = True
                outcome.dirty_scope_count = 0
                timings_ms.update(
                    {
                        f"carry_forward.{label}": float(elapsed_ms)
                        for label, elapsed_ms in outcome.timings_ms.items()
                    }
                )
                timings_ms["total_instrumented"] = round(
                    max(0.0, (time.perf_counter() - total_started) * 1000.0),
                    3,
                )
                outcome.timings_ms = dict(timings_ms)
                return outcome
        _stage_t0 = time.perf_counter()
        cached_view = _load_cached_view(
            conn,
            definition_id=definition.definition_id,
            before_generation_id=generation_id,
        )
        _record_timing("load_cached_view", _stage_t0)
        if (
            cached_view is not None
            and cached_view.view_def_hash != view_def.view_def_hash
        ):
            cached_view = None
        # Only honor the delta scope when build_corpus confirmed it staged a
        # COMPLETE dirty set (delta_scope.scoped). If it fell back to a full scan
        # (could not guarantee completeness), use the full row diff — never risk
        # a stale carry-forward on a partial set.
        explicit_dirty_scope_keys: frozenset[str] | None = (
            frozenset(delta_scope.dirty_scope_keys)
            if delta_scope is not None and delta_scope.scoped
            else None
        )
        _stage_t0 = time.perf_counter()
        new_view, stats = refresh_view(
            cached_view=cached_view,
            new_corpus=corpus,
            view_def=view_def,
            llm_client=materializer.llm_client,
            generation_id=generation_id,
        )
        _record_timing("refresh_view", _stage_t0)
        if explicit_dirty_scope_keys is not None and cached_view is not None:
            _stage_t0 = time.perf_counter()
            new_view = _merge_scoped_view_fragments(
                cached_view=cached_view,
                dirty_view=new_view,
                dirty_scope_keys=explicit_dirty_scope_keys,
                resolver=materializer.anchor_resolver,
                stats=stats,
            )
            _record_timing("merge_scoped_view_fragments", _stage_t0)
        new_view.generation_id = generation_id
        citation_fragment_ids: frozenset[str] | None = None
        if definition_stable and cached_view is not None:
            citation_fragment_ids = _changed_fragment_ids(
                cached_view=cached_view,
                new_view=new_view,
            )

        _stage_t0 = time.perf_counter()
        assembled = _assemble_view(new_view, view_def.assembler)
        _record_timing("assemble_view", _stage_t0)
        _stage_t0 = time.perf_counter()
        materializer_input_signature = _input_signature(
            view_def=view_def,
            llm_client=materializer.llm_client,
            input_fact_watermark=input_fact_watermark,
            input_edge_fact_watermark=input_edge_fact_watermark,
        )
        json_payload = {
            "view_manifest": view_to_dict(new_view),
            "refresh_stats": stats.as_dict(),
            "materializer_input_signature": materializer_input_signature,
            "native_view_stage": {
                "version": 1,
                "authority": "derived_artifact_manifest",
                "output_templates": [
                    template.to_contract() for template in output_templates
                ],
            },
        }
        if assembled.markered is not None:
            json_payload["markered"] = assembled.markered
            json_payload["fragment_offsets"] = assembled.fragment_offsets
        _record_timing("build_json_payload", _stage_t0)

        _stage_t0 = time.perf_counter()
        rendered_outputs = _render_outputs_from_templates(
            conn=conn,
            templates=output_templates,
            view_def=view_def,
            deployment_name=self._deployment_name,
            target_generation_id=generation_id,
            published_at=published_at,
            assembled=assembled,
            json_payload=json_payload,
            view=new_view,
            explicit_dirty_scope_keys=explicit_dirty_scope_keys,
            direct_relation_rows=(
                delta_scope.relation_rows
                if delta_scope is not None and delta_scope.scoped
                else None
            ),
            direct_relation_rows_known_changed=(
                delta_scope.relation_rows_known_changed
                if delta_scope is not None and delta_scope.scoped
                else None
            ),
        )
        _record_timing("render_outputs", _stage_t0)
        for label, elapsed_ms in rendered_outputs.timings_ms.items():
            timings_ms[f"render_outputs.{label}"] = round(float(elapsed_ms), 3)
        _stage_t0 = time.perf_counter()
        _register_started = _stage_t0
        if definition_stable:
            # The scheduler still needs dependency bindings to be current, but
            # publish/carry-forward paths upsert stable definitions themselves.
            timings_ms["register_definitions.stable_upserts_skipped"] = 1.0
        else:
            register_view_artifact_definition(conn, definition)
            _record_timing("register_definitions.view_definition", _stage_t0)
            _stage_t0 = time.perf_counter()
            for relation_task in rendered_outputs.relation_tasks:
                register_jsonb_relation(conn, relation_task.definition)
            _record_timing("register_definitions.relation_definitions", _stage_t0)
        _stage_t0 = time.perf_counter()
        _replace_view_template_dependencies(
            conn,
            downstream_definition_id=definition.definition_id,
            dependencies=rendered_outputs.dependencies,
        )
        _record_timing("register_definitions.dependencies", _stage_t0)
        _record_timing("register_definitions", _register_started)

        _stage_t0 = time.perf_counter()
        result = schedule_derived_artifacts(
            conn,
            deployment_name=self._deployment_name,
            target_generation_id=generation_id,
            catalog_version_id=catalog_version_id,
            execution_phase="post_publish",
            register_tasks=False,
            coalesce_task_transactions=True,
            trusted_previous_manifest_ids=refreshable_manifest_ids,
            tasks=[
                *rendered_outputs.relation_tasks,
                ViewScheduleTask(
                    definition=definition,
                    outputs=rendered_outputs.outputs,
                    dirty_scope_keys=(
                        sorted(explicit_dirty_scope_keys)
                        if explicit_dirty_scope_keys is not None
                        else None
                    ),
                    consume_dirty_scopes=explicit_dirty_scope_keys is None,
                    metrics={
                        **stats.as_dict(),
                        "publisher_native_view_stage": True,
                        "read_asset_authority": {
                            "asset_id": materializer.asset_execution_token.asset_id,
                            "asset_digest": materializer.asset_execution_token.asset_digest,
                            "policy_digest": materializer.asset_execution_token.policy_digest,
                            "runtime_identity": materializer.asset_execution_token.runtime_identity,
                            "allowed_slots": list(
                                materializer.asset_execution_token.allowed_slots
                            ),
                            "lineage_sources": list(
                                materializer.asset_execution_token.lineage_sources
                            ),
                        },
                        "corpus_build_ms": corpus_build_ms,
                        "timings_ms": dict(timings_ms),
                        "corpus_scan_scoped": explicit_dirty_scope_keys is not None,
                        "dirty_scope_count": (
                            len(explicit_dirty_scope_keys)
                            if explicit_dirty_scope_keys is not None
                            else None
                        ),
                    },
                ),
            ],
        )
        for label, elapsed_ms in result.timings_ms.items():
            timings_ms[f"schedule.{label}"] = round(float(elapsed_ms), 3)
        _record_timing("schedule_artifacts", _stage_t0)
        node = next(
            item
            for item in result.nodes
            if item.definition_id == definition.definition_id
        )
        fragment_dependencies_unchanged = (
            bool(stats.fragments_total)
            and int(stats.fragments_total) == int(stats.reused)
            and not (
                stats.refreshed
                or stats.added
                or stats.dropped
                or stats.llm_calls
            )
        )
        cited_node_ids: tuple[str, ...] = ()
        citation_degradation: dict[str, Any] | None = None
        if materializer.anchor_resolver is not None:
            _stage_t0 = time.perf_counter()
            cited_node_ids = _resolve_cited_node_ids(
                fragments=new_view.fragments,
                resolver=materializer.anchor_resolver,
            )
            _record_timing("resolve_cited_nodes", _stage_t0)
            view_cites_current = False
            if citation_fragment_ids is not None and not citation_fragment_ids:
                timings_ms["view_cites.skipped_empty_fragment_delta"] = 1.0
                view_cites_current = True
            elif fragment_dependencies_unchanged:
                _stage_t0 = time.perf_counter()
                try:
                    view_cites_current = self._view_cites_current(
                        conn,
                        view_node_id=f"view:{view_id}",
                        fragments=new_view.fragments,
                        resolver=materializer.anchor_resolver,
                    )
                except Exception:  # noqa: BLE001
                    view_cites_current = False
                _record_timing("view_cites_current_probe", _stage_t0)
            if view_cites_current:
                timings_ms["view_cites.skipped_reused_fragments"] = 1.0
            else:
                _stage_t0 = time.perf_counter()
                try:
                    self._project_view_cites(
                        conn,
                        view_node_id=f"view:{view_id}",
                        view_def_id=view_id,
                        definition_id=definition.definition_id,
                        fragments=new_view.fragments,
                        resolver=materializer.anchor_resolver,
                        generation_id=generation_id,
                        fragment_ids=citation_fragment_ids,
                        timing_sink=timings_ms,
                    )
                except Exception:  # noqa: BLE001
                    citation_degradation = {
                        "status": "degraded",
                        "cause": "view_cites_projection_failed",
                        "omitted_work": ["view_cites_node_projection"],
                        "scope": {
                            "view_id": view_id,
                            "generation_id": generation_id,
                        },
                        "retryable": True,
                        "freshness": "degraded",
                    }
                    with conn.cursor() as cur:
                        cur.execute(
                            _PERSIST_DEGRADED_OUTCOMES_SQL,
                            {
                                "manifest_id": node.manifest_id,
                                "degradation": Jsonb({
                                    "degraded_outcomes": [citation_degradation],
                                }),
                            },
                        )
                    logger.exception(
                        "native view_cites projection failed for view_id=%r "
                        "at gen=%d; continuing without blocking",
                        view_id,
                        generation_id,
                    )
                _record_timing("project_view_cites", _stage_t0)
                if citation_fragment_ids is not None:
                    timings_ms["view_cites.fragment_delta_count"] = float(
                        len(citation_fragment_ids)
                    )
        # Emit persisted lineage envelopes for every governed native output.
        # The canonical token supplies at least one declaration record; graph
        # citations add live source records. Any gap raises inside the view
        # savepoint so policy-visible output is never published without lineage.
        #
        # Each new manifest receives its own envelope, including carry-forward
        # manifests, so an exact-generation public read never borrows provenance
        # from a different materialization identity.
        _stage_t0 = time.perf_counter()
        lineage_timings_ms = self._emit_lineage_envelopes(
            conn,
            result=result,
            view_id=view_id,
            view_definition=definition,
            view_def=view_def,
            view_outputs=rendered_outputs.outputs,
            relation_tasks=rendered_outputs.relation_tasks,
            cited_node_ids=cited_node_ids,
            generation_id=generation_id,
            catalog_version_id=catalog_version_id,
            execution_token=materializer.asset_execution_token,
        )
        for label, elapsed_ms in lineage_timings_ms.items():
            timings_ms[f"lineage.{label}"] = round(float(elapsed_ms), 3)
        _record_timing("emit_lineage_envelopes", _stage_t0)
        relation_row_total = sum(
            len(task.rows) for task in rendered_outputs.relation_tasks
        )
        timings_ms["total_instrumented"] = round(
            max(0.0, (time.perf_counter() - total_started) * 1000.0),
            3,
        )
        return MaterializeOutcome(
            view_id=view_id,
            generation_id=generation_id,
            fragments_total=stats.fragments_total,
            fragments_reused=stats.reused,
            fragments_refreshed=stats.refreshed,
            fragments_added=stats.added,
            fragments_dropped=stats.dropped,
            llm_calls=stats.llm_calls,
            manifest_path=None,
            manifest_hash=_manifest_hash_for(conn, node.manifest_id),
            freshness=("degraded" if citation_degradation else "fresh"),
            status=("degraded" if citation_degradation else "success"),
            cause=(
                str(citation_degradation["cause"])
                if citation_degradation else None
            ),
            omitted_work=(
                list(citation_degradation["omitted_work"])
                if citation_degradation else []
            ),
            scope=(
                dict(citation_degradation["scope"])
                if citation_degradation else {}
            ),
            retryable=(
                bool(citation_degradation["retryable"])
                if citation_degradation else None
            ),
            corpus_build_ms=corpus_build_ms,
            corpus_scan_scoped=explicit_dirty_scope_keys is not None,
            dirty_scope_count=(
                len(explicit_dirty_scope_keys)
                if explicit_dirty_scope_keys is not None
                else None
            ),
            relation_rows=relation_row_total,
            timings_ms=timings_ms,
        )

    def _emit_lineage_envelopes(
        self,
        conn: psycopg.Connection[Any],
        *,
        result,
        view_id: str,
        view_definition,
        view_def,
        view_outputs,
        relation_tasks,
        cited_node_ids: Sequence[str],
        generation_id: int,
        catalog_version_id: int | None,
        execution_token,
    ) -> dict[str, float]:
        """Record dependency envelopes for this view AND its relations.

        Every output this stage materializes — the view itself and each of
        its derived-relation tables — gets an envelope keyed on the view's
        cited graph nodes (`graph_node`), the source records behind them
        (`source_record`), and the producing component's hash
        (`component_hash`). A later change frontier built from a
        source/graph delta or a definition edit can then find exactly
        these outputs. Recording the relations too is required before the
        frontier is consumed: a relation with no envelope would be invisible
        to the reverse index and could be wrongly reused while stale.

        The relations conservatively share the view's provenance (any cited
        node changing dirties them), while the canonical declaration records
        cover static outputs. Any source or persistence gap aborts this view
        inside the caller-owned savepoint.
        """
        timings_ms: dict[str, float] = {}

        def _record_timing(name: str, started: float) -> None:
            elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
            timings_ms[name] = round(timings_ms.get(name, 0.0) + elapsed, 3)

        try:
            _stage_t0 = time.perf_counter()
            source_records = [
                source_record_lineage(
                    "deployment-product.read-asset",
                    {
                        "asset_id": execution_token.asset_id,
                        "lineage_source": lineage_source,
                    },
                )
                for lineage_source in execution_token.lineage_sources
            ]
            if cited_node_ids:
                source_records.extend(self._collect_source_records(
                    conn,
                    cited_node_ids,
                    timing_sink=timings_ms,
                ))
            _record_timing("collect_source_records", _stage_t0)
            timings_ms["cited_node_count"] = float(len(cited_node_ids))
            timings_ms["source_record_count"] = float(len(source_records))
            _stage_t0 = time.perf_counter()
            manifest_by_definition = {
                node.definition_id: node.manifest_id for node in result.nodes
            }
            catalog_version = (
                catalog_version_id if catalog_version_id is not None else generation_id
            )
            _record_timing("manifest_map", _stage_t0)

            # The view's own output slots share one manifest and one dirty
            # decision: if any cited node changes, the view manifest is dirty.
            # Emit a single envelope instead of one per inline slot so the
            # reverse index stays proportional to cited inputs, not
            # cited inputs × rendered slots.
            view_manifest = manifest_by_definition.get(
                view_definition.definition_id,
            )
            if view_manifest is not None:
                _stage_t0 = time.perf_counter()
                view_envelope = build_output_lineage_envelope(
                    output_id=f"{view_id}:__view__",
                    output_kind="native_view",
                    source_records=source_records,
                    component_id=view_definition.definition_id,
                    component_hash=view_def.view_def_hash,
                    component_kind="view_component",
                    output_digest=stable_hash(
                        {
                            "output_id": f"{view_id}:__view__",
                            "manifest_id": str(view_manifest),
                            "component_hash": view_def.view_def_hash,
                            "slot_count": len(view_outputs),
                        }
                    ),
                    catalog_version_id=catalog_version,
                    graph_generation_id=generation_id,
                    node_ids=cited_node_ids,
                    decision_ids=tuple(
                        execution_token.policy_requirements.get("decision_ids") or ()
                    ),
                    invalidation_scope="per-output",
                    metadata={
                        "view_id": view_id,
                        "stage": "native_view",
                        "slot_count": len(view_outputs),
                        "declared_lineage_sources": list(
                            execution_token.lineage_sources
                        ),
                        "policy_digest": execution_token.policy_digest,
                    },
                )
                _record_timing("build_view_envelope", _stage_t0)
                _stage_t0 = time.perf_counter()
                publish_result = publish_lineage_envelopes(
                    conn,
                    manifest_id=view_manifest,
                    envelopes=[view_envelope],
                )
                for label, elapsed_ms in (publish_result.timings_ms or {}).items():
                    timings_ms[f"publish_view.{label}"] = round(float(elapsed_ms), 3)
                _record_timing("publish_view_envelope", _stage_t0)

            # Each derived-relation table → its own manifest.
            for relation_task in relation_tasks:
                relation_def = relation_task.definition
                relation_manifest = manifest_by_definition.get(
                    relation_def.definition_id,
                )
                if relation_manifest is None:
                    continue
                _stage_t0 = time.perf_counter()
                relation_envelope = build_output_lineage_envelope(
                    output_id=relation_def.definition_id,
                    output_kind="derived_relation",
                    source_records=source_records,
                    component_id=relation_def.definition_id,
                    component_hash=relation_def.compute_hash,
                    component_kind="renderer",
                    output_digest=stable_hash(
                        {
                            "output_id": relation_def.definition_id,
                            "manifest_id": str(relation_manifest),
                            "component_hash": relation_def.compute_hash,
                        }
                    ),
                    catalog_version_id=catalog_version,
                    graph_generation_id=generation_id,
                    node_ids=cited_node_ids,
                    decision_ids=tuple(
                        execution_token.policy_requirements.get("decision_ids") or ()
                    ),
                    invalidation_scope="per-scope",
                    metadata={
                        "view_id": view_id,
                        "stage": "native_view_relation",
                        "declared_lineage_sources": list(
                            execution_token.lineage_sources
                        ),
                        "policy_digest": execution_token.policy_digest,
                    },
                )
                _record_timing("build_relation_envelope", _stage_t0)
                _stage_t0 = time.perf_counter()
                publish_result = publish_lineage_envelopes(
                    conn,
                    manifest_id=relation_manifest,
                    envelopes=[relation_envelope],
                )
                for label, elapsed_ms in (publish_result.timings_ms or {}).items():
                    timings_ms[f"publish_relation.{label}"] = round(
                        float(elapsed_ms),
                        3,
                    )
                _record_timing("publish_relation_envelope", _stage_t0)
        except Exception as exc:  # noqa: BLE001 - governed lineage fails closed.
            logger.exception(
                "governed native view lineage failed for view_id=%r at gen=%d",
                view_id,
                generation_id,
            )
            raise _GovernedLineagePublicationError(
                run_ids=[node.run_id for node in result.nodes],
            ) from exc
        return timings_ms

    def _collect_source_records(
        self,
        conn: psycopg.Connection[Any],
        node_ids: Sequence[str],
        *,
        branch: str = "default",  # graph_store.DEFAULT_BRANCH
        timing_sink: dict[str, float] | None = None,
    ):
        """Return deduped source-record lineage behind the given nodes.

        Reads the live Chronos graph (`okg.graph_nodes` via a branch
        checkout), the same way the publisher reads current node state
        mid-cycle (`_query_dirty_from_chronos_branch`). Chronos-native
        ingestion writes node provenance directly to the interval-backed
        graph and leaves the legacy `node_facts` log empty, so the graph —
        not the fact log — is the authority. Derived and materializer
        nodes are excluded: a view's provenance is its upstream source
        records, not other derived artifacts. A node with no nameable
        source contributes nothing (the view is then not lineage-reusable
        on that record rather than carrying a fabricated one).
        """
        if not node_ids:
            return []

        def _record_timing(name: str, started: float) -> None:
            if timing_sink is None:
                return
            elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
            timing_sink[name] = round(
                timing_sink.get(name, 0.0) + elapsed,
                3,
            )

        _stage_t0 = time.perf_counter()
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=bool(getattr(conn, "autocommit", True)),
        )
        _record_timing("source_records.graph_store", _stage_t0)
        _stage_t0 = time.perf_counter()
        graph = store.checkout(branch)
        _record_timing("source_records.checkout", _stage_t0)
        _stage_t0 = time.perf_counter()
        rows = graph.query(
            """
            SELECT node_id, source, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
               AND source IS NOT NULL
               AND source NOT LIKE 'materializer:%'
               AND source NOT LIKE 'derived_artifact:%'
               AND source <> '_publisher_derived'
            """,
            {"node_ids": list(node_ids)},
        )
        _record_timing("source_records.query", _stage_t0)
        covered_node_ids = {
            str(row.get("node_id"))
            for row in rows
            if row.get("node_id") is not None and row.get("source") is not None
        }
        file_node_ids = [
            str(node_id)
            for node_id in node_ids
            if str(node_id).startswith("file:")
            and str(node_id) not in covered_node_ids
        ]
        if file_node_ids:
            _stage_t0 = time.perf_counter()
            rows.extend(
                graph.query(
                    """
                    SELECT s.node_id, s.source, s.source_record_id,
                           s.source_revision
                      FROM okg.graph_edges e
                      JOIN okg.graph_nodes s
                        ON s.node_id = e.dst
                       AND s.subtype = 'code_symbol'
                     WHERE e.edge_type = 'contains'
                       AND e.src = ANY(:node_ids)
                       AND s.source IS NOT NULL
                       AND s.source NOT LIKE 'materializer:%'
                       AND s.source NOT LIKE 'derived_artifact:%'
                       AND s.source <> '_publisher_derived'
                    """,
                    {"node_ids": file_node_ids},
                )
            )
            _record_timing("source_records.file_symbol_fallback_query", _stage_t0)
        _stage_t0 = time.perf_counter()
        source_records = source_records_from_node_rows(rows)
        _record_timing("source_records.hydrate", _stage_t0)
        return source_records

    @staticmethod
    def _lineage_output_ids(
        *,
        view_id: str,
        relation_tasks: Sequence[RelationScheduleTask],
    ) -> tuple[str, ...]:
        return (
            f"{view_id}:__view__",
            *(
                task.definition.definition_id
                for task in relation_tasks
            ),
        )

    @staticmethod
    def _lineage_reuse_available(
        conn: psycopg.Connection[Any],
        *,
        output_ids: Sequence[str],
        cited_node_ids: Sequence[str],
    ) -> bool:
        expected = {str(output_id) for output_id in output_ids if output_id}
        cited = {str(node_id) for node_id in cited_node_ids if node_id}
        if not expected or not cited:
            return False
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH expected_outputs AS (
                          SELECT unnest(%(output_ids)s::text[]) AS output_id
                        ),
                        cited_nodes AS (
                          SELECT unnest(%(cited_node_ids)s::text[]) AS dependency_ref
                        ),
                        missing AS (
                          SELECT 1
                            FROM expected_outputs o
                            CROSS JOIN cited_nodes c
                           WHERE NOT EXISTS (
                                 SELECT 1
                                   FROM okg.derived_output_dependency_index i
                                  WHERE i.output_id = o.output_id
                                    AND i.dependency_kind = 'graph_node'
                                    AND i.dependency_ref = c.dependency_ref
                                  LIMIT 1
                           )
                           LIMIT 1
                        )
                        SELECT NOT EXISTS (SELECT 1 FROM missing)
                        """,
                        {
                            "output_ids": sorted(expected),
                            "cited_node_ids": sorted(cited),
                        },
                    )
                    row = cur.fetchone()
        except Exception:
            logger.exception("native view lineage reuse probe failed")
            raise
        return bool(row and row[0])

    def _view_affected_by_frontier(
        self,
        conn: psycopg.Connection[Any],
        *,
        definition,
        generation_id: int,
        lineage_invalidations: Sequence[LineageInvalidation],
    ) -> bool:
        """True iff the change set dirties one of this view's outputs.

        `unify-dirty-set-authority`: the single reverse-dependency authority is
        the lineage index (`okg.derived_output_dependency_index`) the stage
        already populates via `_emit_lineage_envelopes` — NOT a second
        hand-rolled read of `view_cites_node` edges. Carry-forward can only
        reuse the latest completed manifest for this view definition, so the
        probe checks that manifest directly instead of scanning historical
        lineage rows. Missing lineage coverage still fails closed as "cannot
        prove unaffected → rebuild".
        """
        keys = [
            invalidation.lookup_key()
            for invalidation in lineage_invalidations
            if invalidation.dependency_kind in _INDEXED_VIEW_INVALIDATION_KINDS
        ]
        if not keys:
            return False
        try:
            latest_manifest_id = _latest_completed_manifest_id(
                conn,
                definition_id=definition.definition_id,
                before_generation_id=generation_id,
            )
            if latest_manifest_id is None:
                return True
            requested_kinds = sorted({key.dependency_kind for key in keys})
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dependency_kind
                      FROM okg.derived_output_dependency_index
                     WHERE dependency_kind = ANY(%(dependency_kinds)s)
                     GROUP BY dependency_kind
                    """,
                    {"dependency_kinds": requested_kinds},
                )
                covered_kinds = {str(row[0]) for row in cur.fetchall()}
                if set(requested_kinds) - covered_kinds:
                    return True
                cur.execute(
                    """
                    WITH RECURSIVE lineage_manifest AS (
                      SELECT manifest_id, reused_from_manifest_id, 0 AS depth
                        FROM okg.derived_output_manifests
                       WHERE manifest_id = %(latest_manifest_id)s
                      UNION ALL
                      SELECT m.manifest_id, m.reused_from_manifest_id,
                             lm.depth + 1
                        FROM lineage_manifest lm
                        JOIN okg.derived_output_manifests m
                          ON m.manifest_id = lm.reused_from_manifest_id
                       WHERE lm.depth < 50
                    )
                    SELECT lm.manifest_id
                      FROM lineage_manifest lm
                     WHERE EXISTS (
                           SELECT 1
                             FROM okg.derived_output_dependency_index i
                            WHERE i.manifest_id = lm.manifest_id
                              AND i.dependency_kind = ANY(%(dependency_kinds)s)
                     )
                     ORDER BY lm.depth
                     LIMIT 1
                    """,
                    {
                        "latest_manifest_id": latest_manifest_id,
                        "dependency_kinds": requested_kinds,
                    },
                )
                row = cur.fetchone()
                if row is None:
                    return True
                lineage_manifest_id = row[0]
                cur.execute(
                    """
                    WITH requested AS (
                      SELECT dependency_kind, dependency_ref
                        FROM jsonb_to_recordset(%(dependency_keys)s) AS item(
                          dependency_kind TEXT,
                          dependency_ref TEXT
                        )
                    )
                    SELECT 1
                      FROM requested r
                      JOIN okg.derived_output_dependency_index i
                        ON i.dependency_kind = r.dependency_kind
                       AND i.dependency_ref = r.dependency_ref
                     WHERE i.manifest_id = %(lineage_manifest_id)s
                     LIMIT 1
                    """,
                    {
                        "dependency_keys": Jsonb(
                            [key.to_dict() for key in keys],
                        ),
                        "lineage_manifest_id": lineage_manifest_id,
                    },
                )
                return cur.fetchone() is not None
        except Exception:
            logger.exception(
                "native view frontier-affect probe failed for definition=%s "
                "generation_id=%s",
                getattr(definition, "definition_id", "<unknown>"),
                generation_id,
            )
            raise

    def _read_input_watermarks(
        self,
        conn: psycopg.Connection[Any],
    ) -> tuple[int, int]:
        """Return source-side fact-log watermarks (legacy signal only).

        `unify-dirty-set-authority`: under chronos-native publishing the
        fact log (`okg.node_facts` / `okg.edge_facts`) is empty — sources
        write the interval-backed Chronos graph, not the log — so this
        returns (0, 0) and the watermark contributes NO signal to the
        carry-forward decision. The authoritative "did this view's inputs
        move?" signal is the reverse-dependency lineage index consulted by
        `_view_affected_by_frontier`. The watermark read is retained only so
        a legacy fact-log deployment (write_fact_log=True) still gets a
        meaningful signature; it is intentionally vestigial under
        chronos-native and must not be treated as the dirty authority.
        """

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(max(fact_id), 0)
                  FROM okg.node_facts
                 WHERE source NOT LIKE 'materializer:%%'
                   AND source NOT LIKE 'derived_artifact:%%'
                """,
            )
            fact_watermark = int(cur.fetchone()[0] or 0)
            cur.execute(
                """
                SELECT COALESCE(max(fact_id), 0)
                  FROM okg.edge_facts
                 WHERE source NOT LIKE 'materializer:%%'
                   AND source NOT LIKE 'derived_artifact:%%'
                """,
            )
            edge_fact_watermark = int(cur.fetchone()[0] or 0)
        return fact_watermark, edge_fact_watermark

    @staticmethod
    def _view_cites_current(
        conn: psycopg.Connection[Any],
        *,
        view_node_id: str,
        fragments,
        resolver,
    ) -> bool:
        desired: list[dict[str, str]] = []
        seen: set[tuple[str, str, str]] = set()
        for fragment in fragments:
            for anchor_id in fragment.sources:
                cited = resolver(anchor_id)
                if cited is None:
                    continue
                key = (str(cited), str(fragment.id), str(anchor_id))
                if key in seen:
                    continue
                seen.add(key)
                desired.append({
                    "cited": key[0],
                    "fragment_id": key[1],
                    "anchor_id": key[2],
                })

        with conn.transaction():
            with conn.cursor() as cur:
                if not desired:
                    cur.execute(
                        """
                        SELECT NOT EXISTS (
                          SELECT 1
                            FROM okg.graph_edges
                           WHERE src = %(src)s
                             AND edge_type = 'view_cites_node'
                        )
                        """,
                        {"src": view_node_id},
                    )
                    row = cur.fetchone()
                    return bool(row and row[0])
                cur.execute(
                    """
                    WITH desired AS (
                      SELECT cited, fragment_id, anchor_id
                        FROM jsonb_to_recordset(%(desired)s::jsonb) AS item(
                          cited text,
                          fragment_id text,
                          anchor_id text
                        )
                    ),
                    live AS (
                      SELECT dst AS cited,
                             attrs->>'fragment_id' AS fragment_id,
                             attrs->>'anchor_id' AS anchor_id
                        FROM okg.graph_edges
                       WHERE src = %(src)s
                         AND edge_type = 'view_cites_node'
                    )
                    SELECT
                      (SELECT count(*) FROM desired) AS desired_count,
                      (SELECT count(*) FROM live) AS live_count,
                      (
                        SELECT count(*)
                          FROM desired d
                          JOIN live l
                            ON l.cited = d.cited
                           AND l.fragment_id = d.fragment_id
                           AND l.anchor_id = d.anchor_id
                      ) AS matched_count
                    """,
                    {
                        "src": view_node_id,
                        "desired": Jsonb(desired),
                    },
                )
                row = cur.fetchone()
        if row is None:
            return False
        desired_count = int(row[0] or 0)
        live_count = int(row[1] or 0)
        matched_count = int(row[2] or 0)
        return desired_count == live_count == matched_count

    def _project_view_cites(
        self,
        conn: psycopg.Connection[Any],
        *,
        view_node_id: str,
        view_def_id: str,
        definition_id: str,
        fragments,
        resolver,
        generation_id: int,
        fragment_ids: frozenset[str] | None = None,
        timing_sink: dict[str, float] | None = None,
    ) -> tuple[int, int]:
        def _record_timing(name: str, started: float) -> None:
            if timing_sink is None:
                return
            elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
            key = f"view_cites.{name}"
            timing_sink[key] = round(timing_sink.get(key, 0.0) + elapsed, 3)

        if fragment_ids is not None and not fragment_ids:
            if timing_sink is not None:
                timing_sink["view_cites.project.skipped_empty_fragment_delta"] = 1.0
            return (0, 0)
        _stage_t0 = time.perf_counter()
        desired: dict[tuple[str, str, str], None] = {}
        for fragment in fragments:
            if fragment_ids is not None and str(fragment.id) not in fragment_ids:
                continue
            for anchor_id in fragment.sources:
                cited = resolver(anchor_id)
                if cited is None:
                    continue
                desired[(cited, fragment.id, anchor_id)] = None
        _record_timing("project.build_desired", _stage_t0)

        # Chronos-native publishing keeps current citation edges in the graph
        # store, not the legacy live tables. Diff against that branch head so
        # reused fragments do not re-emit identical view_cites_node facts on
        # every materialization cycle.
        from okg.substrate.graph_store import ChronosGraphStore

        _stage_t0 = time.perf_counter()
        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=bool(getattr(conn, "autocommit", True)),
        )
        _record_timing("project.graph_store", _stage_t0)
        _stage_t0 = time.perf_counter()
        graph = store.checkout("default")
        _record_timing("project.checkout", _stage_t0)
        _stage_t0 = time.perf_counter()
        if fragment_ids is None:
            live_rows = graph.query(
                """
                SELECT edge_id, dst, attrs
                  FROM okg.graph_edges
                 WHERE src = :src
                   AND edge_type = 'view_cites_node'
                """,
                {"src": view_node_id},
            )
        else:
            live_rows = graph.query(
                """
                SELECT edge_id, dst, attrs
                  FROM okg.graph_edges
                 WHERE src = :src
                   AND edge_type = 'view_cites_node'
                   AND attrs->>'fragment_id' = ANY(:fragment_ids)
                """,
                {"src": view_node_id, "fragment_ids": sorted(fragment_ids)},
            )
        _record_timing("project.live_query", _stage_t0)
        _stage_t0 = time.perf_counter()
        live: dict[tuple[str, str, str], str] = {}
        for row in live_rows:
            attrs = dict(row.get("attrs") or {})
            live[
                (
                    str(row["dst"]),
                    str(attrs.get("fragment_id") or ""),
                    str(attrs.get("anchor_id") or ""),
                )
            ] = str(row["edge_id"])
        _record_timing("project.build_live", _stage_t0)

        _stage_t0 = time.perf_counter()
        desired_keys = set(desired)
        live_keys = set(live)
        to_retract = live_keys - desired_keys
        to_insert = desired_keys - live_keys
        _record_timing("project.diff", _stage_t0)

        source = f"derived_artifact:{definition_id}"
        _stage_t0 = time.perf_counter()
        retract_rows = []
        for key in sorted(to_retract):
            cited, fragment_id, anchor_id = key
            retract_rows.append(
                {
                    "edge_id": live[key],
                    "src": view_node_id,
                    "dst": cited,
                    "attrs": {
                        "fragment_id": fragment_id,
                        "anchor_id": anchor_id,
                        "view_def_id": view_def_id,
                    },
                    "source_record_id": {
                        "definition_id": definition_id,
                        "view_node_id": view_node_id,
                        "cited_node_id": cited,
                    },
                    "source_revision": {
                        "generation_id": generation_id,
                    },
                    "dedupe_key": (
                        f"view_cites:{view_node_id}|{cited}|"
                        f"{fragment_id}|{anchor_id}|"
                        f"gen:{generation_id}|R"
                    ),
                }
            )
        _record_timing("project.build_retract_rows", _stage_t0)

        _stage_t0 = time.perf_counter()
        insert_rows = []
        for key in sorted(to_insert):
            cited, fragment_id, anchor_id = key
            edge_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                (f"view_cites:{view_node_id}|{cited}|{fragment_id}|{anchor_id}"),
            ).int & ((1 << 63) - 1)
            insert_rows.append(
                {
                    "edge_id": edge_id,
                    "src": view_node_id,
                    "dst": cited,
                    "attrs": {
                        "fragment_id": fragment_id,
                        "anchor_id": anchor_id,
                        "view_def_id": view_def_id,
                    },
                    "source_record_id": {
                        "definition_id": definition_id,
                        "view_node_id": view_node_id,
                        "cited_node_id": cited,
                    },
                    "source_revision": {
                        "generation_id": generation_id,
                    },
                    "dedupe_key": (
                        f"view_cites:{view_node_id}|{cited}|"
                        f"{fragment_id}|{anchor_id}|"
                        f"gen:{generation_id}|I"
                    ),
                }
            )
        _record_timing("project.build_insert_rows", _stage_t0)

        # Both sides of citation replacement are one PostgreSQL statement,
        # inside a transaction context. On an autocommit publisher connection
        # this opens an atomic transaction; on a caller-owned transaction it
        # opens a savepoint. Therefore an insert failure cannot leave only the
        # retractions visible, and the context restores a usable connection
        # before the caller persists the typed degraded outcome.
        _stage_t0 = time.perf_counter()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                WITH retract_payload AS (
                  SELECT *
                    FROM jsonb_to_recordset(%s::jsonb) AS x(
                      edge_id bigint,
                      src text,
                      dst text,
                      attrs jsonb,
                      source_record_id jsonb,
                      source_revision jsonb,
                      dedupe_key text
                    )
                ),
                retracted AS (
                  INSERT INTO okg.edge_facts
                    (edge_id, src, dst, edge_type, attrs,
                     provenance, confidence, source,
                     source_record_id, source_revision,
                     observed_at, dedupe_key, run_id, op)
                  SELECT edge_id, src, dst, 'view_cites_node',
                         attrs,
                         'derived_deterministic', 1.0,
                         %s,
                         source_record_id, source_revision,
                         now(), dedupe_key, gen_random_uuid(), 'R'
                    FROM retract_payload
                  ON CONFLICT (dedupe_key, observed_at) DO NOTHING
                  RETURNING 1
                ),
                insert_payload AS (
                  SELECT *
                    FROM jsonb_to_recordset(%s::jsonb) AS x(
                          edge_id bigint,
                          src text,
                          dst text,
                          attrs jsonb,
                          source_record_id jsonb,
                          source_revision jsonb,
                          dedupe_key text
                        )
                ),
                inserted AS (
                  INSERT INTO okg.edge_facts
                      (edge_id, src, dst, edge_type, attrs,
                       provenance, confidence, source,
                       source_record_id, source_revision,
                       observed_at, dedupe_key, run_id, op)
                    SELECT edge_id, src, dst, 'view_cites_node',
                           attrs,
                           'derived_deterministic', 1.0,
                           %s,
                           source_record_id, source_revision,
                           now(), dedupe_key, gen_random_uuid(), 'I'
                      FROM insert_payload
                    ON CONFLICT (dedupe_key, observed_at) DO NOTHING
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM retracted),
                       (SELECT count(*) FROM inserted)
                """,
                (
                    json.dumps(retract_rows),
                    source,
                    json.dumps(insert_rows),
                    source,
                ),
            )
            cur.fetchone()
        _record_timing("project.write_rows", _stage_t0)

        return (len(to_insert), len(to_retract))


def _output_templates_for(view_def) -> tuple[ViewOutputTemplate, ...]:
    templates = getattr(view_def, "output_templates", None)
    if templates is None:
        return default_output_templates()
    return tuple(templates)


@dataclass(frozen=True)
class _RenderedTemplateOutputs:
    outputs: list[InlineViewOutput]
    relation_tasks: list[RelationScheduleTask]
    dependencies: list[DerivedArtifactDependency]
    timings_ms: dict[str, float] = field(default_factory=dict)


def _resolve_cited_node_ids(*, fragments, resolver) -> tuple[str, ...]:
    """Resolve the distinct graph node ids a view's fragments cite.

    Mirrors the resolution in `_project_view_cites` so the lineage
    envelope's `graph_node` keys are exactly the nodes the view cites.
    """
    cited: dict[str, None] = {}
    for fragment in fragments:
        for anchor_id in fragment.sources:
            node_id = resolver(anchor_id)
            if node_id is not None:
                cited[node_id] = None
    return tuple(cited)


def _changed_fragment_ids(*, cached_view, new_view) -> frozenset[str]:
    """Return fragment ids whose citation set may differ from the cache."""

    cached_by_id = {str(fragment.id): fragment for fragment in cached_view.fragments}
    changed: set[str] = set()
    for fragment in new_view.fragments:
        fragment_id = str(fragment.id)
        cached = cached_by_id.pop(fragment_id, None)
        if cached is not fragment:
            changed.add(fragment_id)
    changed.update(cached_by_id)
    return frozenset(changed)


def _merge_scoped_view_fragments(
    *,
    cached_view,
    dirty_view,
    dirty_scope_keys: frozenset[str],
    resolver,
    stats,
):
    """Return a full view after replacing only dirty-scope fragments.

    ``ViewDeltaScope`` lets a materializer hand the publisher only the dirty
    corpus. Relation slots already carry clean scopes forward in the scheduler;
    the native view manifest and its citation edges need the same replacement
    rule or a dirty publish shrinks the view to the changed slice.
    """

    if not dirty_scope_keys:
        return dirty_view

    cached_by_id = {
        str(fragment.id): fragment for fragment in cached_view.fragments
    }
    dirty_by_id = {
        str(fragment.id): fragment for fragment in dirty_view.fragments
    }
    dirty_cached_ids = {
        fragment_id
        for fragment_id, fragment in cached_by_id.items()
        if _fragment_touches_dirty_scope(
            fragment,
            dirty_scope_keys=dirty_scope_keys,
            resolver=resolver,
        )
    }

    merged_by_id: dict[str, Any] = {}
    for fragment_id, fragment in cached_by_id.items():
        if fragment_id in dirty_cached_ids:
            replacement = dirty_by_id.get(fragment_id)
            if replacement is not None:
                merged_by_id[fragment_id] = replacement
            continue
        merged_by_id[fragment_id] = fragment

    for fragment_id, fragment in dirty_by_id.items():
        if fragment_id not in cached_by_id:
            merged_by_id[fragment_id] = fragment

    merged_fragments = [
        merged_by_id[fragment_id] for fragment_id in sorted(merged_by_id)
    ]
    dirty_reused = sum(
        1
        for fragment_id, fragment in dirty_by_id.items()
        if cached_by_id.get(fragment_id) is fragment
    )
    clean_reused = sum(
        1 for fragment_id in cached_by_id if fragment_id not in dirty_cached_ids
    )
    stats.fragments_total = len(merged_fragments)
    stats.reused = clean_reused + dirty_reused
    stats.refreshed = sum(
        1
        for fragment_id, fragment in dirty_by_id.items()
        if fragment_id in cached_by_id and cached_by_id[fragment_id] is not fragment
    )
    stats.added = sum(1 for fragment_id in dirty_by_id if fragment_id not in cached_by_id)
    stats.dropped = sum(
        1 for fragment_id in dirty_cached_ids if fragment_id not in dirty_by_id
    )
    dirty_view.fragments = merged_fragments
    return dirty_view


def _fragment_touches_dirty_scope(
    fragment,
    *,
    dirty_scope_keys: frozenset[str],
    resolver,
) -> bool:
    candidates = {str(fragment.id)}
    for anchor_id in fragment.sources:
        candidates.add(str(anchor_id))
        if resolver is None:
            continue
        cited = resolver(anchor_id)
        if cited is not None:
            candidates.add(str(cited))
    return bool(candidates.intersection(dirty_scope_keys))


def _build_corpus_accepts_delta(build_corpus: Any) -> bool:
    """True iff a view's ``build_corpus`` opted into compute-incrementality.

    A view migrates to O(delta) extraction by adding a ``delta_scope`` keyword to
    its ``build_corpus`` signature; the publisher only hands it the delta when the
    parameter is present. Views that don't opt in stay full-scan-but-correct. A
    bare ``**kwargs`` is NOT treated as opt-in — it would silently swallow the
    kwarg, and the repo avoids such sinks.
    """
    try:
        params = inspect.signature(build_corpus).parameters
    except (TypeError, ValueError):
        return False
    return "delta_scope" in params


def _build_corpus_accepts_kwarg(build_corpus: Any, name: str) -> bool:
    """True iff ``build_corpus`` explicitly accepts the named keyword."""

    try:
        params = inspect.signature(build_corpus).parameters
    except (TypeError, ValueError):
        return False
    return name in params


def _render_outputs_from_templates(
    *,
    conn: psycopg.Connection[Any],
    templates: tuple[ViewOutputTemplate, ...],
    view_def,
    deployment_name: str,
    target_generation_id: int,
    published_at: datetime | None,
    assembled: "_AssembledView",
    json_payload: Mapping[str, Any],
    view,
    explicit_dirty_scope_keys: frozenset[str] | None = None,
    direct_relation_rows: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    direct_relation_rows_known_changed: set[str] | None = None,
) -> _RenderedTemplateOutputs:
    outputs: list[InlineViewOutput] = []
    relation_tasks: list[RelationScheduleTask] = []
    dependencies: list[DerivedArtifactDependency] = []
    timings_ms: dict[str, float] = {}

    def record(label: str, started: float) -> None:
        timings_ms[label] = round(
            timings_ms.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    for template in templates:
        template_started = time.perf_counter()
        if template.source == "sql_relation":
            relation_output = _render_sql_relation_template(
                conn=conn,
                template=template,
                view_def=view_def,
                deployment_name=deployment_name,
                target_generation_id=target_generation_id,
                published_at=published_at,
                explicit_dirty_scope_keys=explicit_dirty_scope_keys,
                direct_relation_rows=direct_relation_rows,
                direct_relation_rows_known_changed=(
                    direct_relation_rows_known_changed
                ),
                timing_sink=timings_ms,
            )
            record("template.sql_relation", template_started)
            if relation_output is None:
                continue
            output, relation_task, dependency = relation_output
            outputs.append(output)
            relation_tasks.append(relation_task)
            dependencies.append(dependency)
            continue

        template_started = time.perf_counter()
        output = _render_output_from_template(
            template=template,
            assembled=assembled,
            json_payload=json_payload,
            view=view,
        )
        record(f"template.{template.source}", template_started)
        if output is not None:
            outputs.append(output)
    if not outputs:
        raise ValueError(
            f"view {view.view_id!r} rendered no output slots from its output templates"
        )
    return _RenderedTemplateOutputs(
        outputs=outputs,
        relation_tasks=relation_tasks,
        dependencies=dependencies,
        timings_ms=timings_ms,
    )


def _render_output_from_template(
    *,
    template: ViewOutputTemplate,
    assembled: "_AssembledView",
    json_payload: Mapping[str, Any],
    view,
) -> InlineViewOutput | None:
    metadata = {
        "source": "views_framework",
        "template_source": template.source,
        **dict(template.metadata),
    }
    if template.source == "assembled_markdown":
        payload: Any = assembled.markdown
    elif template.source == "native_view_manifest":
        payload = json_payload
    elif template.source == "fragment_json_tables":
        payload = _table_payload_for_view(view)
        if not payload["tables"] and not template.required:
            return None
    elif template.source == "view_graph_json":
        from okg.views.view_graphs import view_graph_payload_from_view

        payload = view_graph_payload_from_view(view)
    elif template.source == "sql_relation":
        raise ValueError(
            "sql_relation templates must be rendered by the relation task planner"
        )
    else:
        raise ValueError(
            f"view {view.view_id!r} declares unsupported output "
            f"template source {template.source!r} for slot "
            f"{template.slot_name!r}"
        )

    return InlineViewOutput(
        slot_name=template.slot_name,
        media_type=template.media_type,
        artifact_type=template.artifact_type,
        renderer_ref=template.renderer_ref,
        payload=payload,
        metadata=metadata,
    )


def _render_sql_relation_template(
    *,
    conn: psycopg.Connection[Any],
    template: ViewOutputTemplate,
    view_def,
    deployment_name: str,
    target_generation_id: int,
    published_at: datetime | None = None,
    explicit_dirty_scope_keys: frozenset[str] | None = None,
    direct_relation_rows: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    direct_relation_rows_known_changed: set[str] | None = None,
    timing_sink: dict[str, float] | None = None,
) -> tuple[InlineViewOutput, RelationScheduleTask, DerivedArtifactDependency] | None:
    def record(label: str, started: float) -> None:
        if timing_sink is None:
            return
        timing_sink[label] = round(
            timing_sink.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    step_started = time.perf_counter()
    relation_plan = _relation_plan_for_template(
        view_def=view_def,
        deployment_name=deployment_name,
        template=template,
    )
    relation_definition = relation_plan.definition
    record("sql_relation.plan", step_started)
    step_started = time.perf_counter()
    rows = _relation_rows_for_template(
        conn=conn,
        template=template,
        view_def=view_def,
        deployment_name=deployment_name,
        target_generation_id=target_generation_id,
        published_at=published_at,
        direct_relation_rows=direct_relation_rows,
    )
    record("sql_relation.rows", step_started)
    # In the compute-incremental path the rows are already only the dirty scopes
    # and may be empty (every dirty scope was a deletion); the task must still be
    # produced so the scheduler empties those scopes. Only skip an optional empty
    # relation on the full (non-incremental) path.
    if not rows and explicit_dirty_scope_keys is None and not template.required:
        return None
    if explicit_dirty_scope_keys is not None:
        # Authoritative dirty scopes from build_corpus (the changed/deleted
        # scopes it rescanned). Skips the O(graph) full-table diff; the scheduler
        # carries every other scope forward via refresh_relation_artifact.
        dirty_scope_keys = sorted(explicit_dirty_scope_keys)
        # The scheduler binds this relation output after refreshing the upstream
        # relation manifest, so precomputing the post-refresh visible count here
        # duplicates the dirty-scope stats query that the refresh path already
        # performs. Leave it unset and let _bind_view_relation_outputs fill the
        # authoritative slot row_count.
        visible_row_count = None
    else:
        step_started = time.perf_counter()
        dirty_scope_keys = _relation_dirty_scope_keys(
            conn,
            definition=relation_definition,
            current_rows=rows,
            target_generation_id=target_generation_id,
        )
        record("sql_relation.dirty_scope_keys", step_started)
        visible_row_count = len(rows)
    step_started = time.perf_counter()
    metadata = {
        "source": "views_framework",
        "template_source": template.source,
        **dict(template.metadata),
    }
    output = InlineViewOutput.relation_ref(
        slot_name=template.slot_name,
        upstream_definition_id=relation_definition.definition_id,
        logical_relation_id=relation_definition.logical_relation_id,
        renderer_ref=template.renderer_ref,
        media_type=template.media_type,
        artifact_type=template.artifact_type,
        metadata=metadata,
        storage_ref={
            "view_id": view_def.view_id,
            "view_slot_name": template.slot_name,
            "title": metadata.get("title") or template.slot_name,
            "columns": _relation_column_contracts(template),
            "column_names": _relation_column_names(template),
            "scope": _relation_scope_contract(template),
            "row_identity": _relation_row_identity_contract(template),
        },
        row_count=visible_row_count,
    )
    direct_rows_key = str(dict(template.config).get("direct_rows_key") or "")
    relation_task = RelationScheduleTask(
        definition=relation_definition,
        rows=rows,
        dirty_scope_keys=dirty_scope_keys,
        dirty_rows_known_changed=(
            direct_rows_key in direct_relation_rows_known_changed
            if direct_relation_rows_known_changed is not None
            else False
        ),
        consume_dirty_scopes=explicit_dirty_scope_keys is None,
        metrics={
            "publisher_native_view_stage": True,
            "view_id": view_def.view_id,
            "view_slot_name": template.slot_name,
            "relation_rows": len(rows),
        },
    )
    record("sql_relation.output_task", step_started)
    return output, relation_task, relation_plan.dependency


@dataclass(frozen=True)
class _RelationTemplatePlan:
    template: ViewOutputTemplate
    definition: RelationArtifactDefinition
    dependency: DerivedArtifactDependency


def _relation_plan_for_templates(
    *,
    templates: tuple[ViewOutputTemplate, ...],
    view_def,
    deployment_name: str,
) -> list[_RelationTemplatePlan]:
    plans: list[_RelationTemplatePlan] = []
    for template in templates:
        if template.source != "sql_relation":
            continue
        plans.append(
            _relation_plan_for_template(
                view_def=view_def,
                deployment_name=deployment_name,
                template=template,
            )
        )
    return plans


def _relation_plan_for_template(
    *,
    view_def,
    deployment_name: str,
    template: ViewOutputTemplate,
) -> _RelationTemplatePlan:
    relation_definition = _relation_definition_for_template(
        view_def=view_def,
        deployment_name=deployment_name,
        template=template,
    )
    dependency = DerivedArtifactDependency(
        downstream_definition_id=f"view:{view_def.view_id}",
        upstream_definition_id=relation_definition.definition_id,
        upstream_slot_name=RELATION_SLOT_NAME,
        required_output_schema_hash=relation_definition.output_schema_hash,
        metadata={
            "reason": "view output relation slot",
            "view_id": view_def.view_id,
            "view_slot_name": template.slot_name,
        },
    )
    return _RelationTemplatePlan(
        template=template,
        definition=relation_definition,
        dependency=dependency,
    )


def _relation_definition_for_template(
    *,
    view_def,
    deployment_name: str,
    template: ViewOutputTemplate,
) -> RelationArtifactDefinition:
    config = dict(template.config)
    logical_relation_id = str(config.get("logical_relation_id") or "")
    sql_text = str(config.get("sql") or "")
    columns = _relation_column_contracts(template)
    scope = _relation_scope_contract(template)
    row_identity = _relation_row_identity_contract(template)
    output_contract = {
        "source": "view_output_template",
        "view_id": view_def.view_id,
        "slot_name": template.slot_name,
        "logical_relation_id": logical_relation_id,
        "media_type": template.media_type,
        "artifact_type": template.artifact_type,
        "columns": columns,
        "column_names": [column["name"] for column in columns],
        "scope": scope,
        "row_identity": row_identity,
    }
    dirty_rule = {
        "mode": "publisher_conservative_sql_relation_refresh",
        "dependencies": ["node_facts", "edge_facts", "view_definition"],
    }
    return RelationArtifactDefinition(
        definition_id=f"view-relation:{view_def.view_id}:{template.slot_name}",
        deployment_name=deployment_name,
        logical_relation_id=logical_relation_id,
        compute_hash=stable_hash(
            {
                "view_id": view_def.view_id,
                "slot_name": template.slot_name,
                "sql": sql_text,
                "sql_params": dict(config.get("sql_params") or {}),
                "direct_rows_key": str(config.get("direct_rows_key") or ""),
                "scope": scope,
                "row_identity": row_identity,
                "params": view_def.params or {},
            }
        ),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash(
            {
                "renderer_ref": template.renderer_ref,
                "metadata": dict(template.metadata),
            }
        ),
        dirty_rule_hash=stable_hash(dirty_rule),
        artifact_kind="derived_artifact",
        renderer_ref="okg.renderer.sql_relation.jsonb.v1",
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _relation_rows_for_template(
    *,
    conn: psycopg.Connection[Any],
    template: ViewOutputTemplate,
    view_def,
    deployment_name: str,
    target_generation_id: int,
    published_at: datetime | None = None,
    direct_relation_rows: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> list[RelationRow]:
    config = dict(template.config)
    sql_text = str(config.get("sql") or "")
    write_hit = sql_contains_writes(sql_text)
    if write_hit is not None:
        raise ValueError(
            f"view output SQL relation template {template.slot_name!r} "
            f"contains write-shaped SQL token {write_hit!r}"
        )

    direct_rows_key = str(config.get("direct_rows_key") or "")
    if (
        direct_relation_rows is not None
        and direct_rows_key
        and direct_rows_key in direct_relation_rows
    ):
        raw_rows = [dict(row) for row in direct_relation_rows[direct_rows_key]]
    else:
        timeout_ms = int(config.get("timeout_ms") or 5000)
        params = {
            **dict(config.get("sql_params") or {}),
            **dict(view_def.params or {}),
            "target_generation_id": target_generation_id,
            "deployment_name": deployment_name,
            "view_id": view_def.view_id,
        }
        if published_at is not None:
            params["published_at"] = published_at
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(f"SET LOCAL statement_timeout = {timeout_ms}")
                cur.execute(sql_text, params)
                raw_rows = [dict(row) for row in cur.fetchall()]

    configured_columns = _relation_column_contracts(template)
    configured_column_names = [column["name"] for column in configured_columns]
    rows: list[RelationRow] = []
    for index, raw_row in enumerate(raw_rows):
        _validate_required_relation_columns(
            template=template,
            raw_row=raw_row,
            columns=configured_columns,
        )
        columns = (
            configured_column_names
            if configured_column_names
            else _infer_relation_row_columns(template, raw_row)
        )
        row_data = {column: raw_row.get(column) for column in columns}
        rows.append(
            RelationRow(
                scope_key=_relation_scope_key_for_row(
                    template=template,
                    view_def=view_def,
                    raw_row=raw_row,
                ),
                row_key=_relation_row_key_for_row(
                    template=template,
                    raw_row=raw_row,
                    index=index,
                ),
                row_data=_json_compatible(row_data),
                dependency_hash=(
                    None
                    if raw_row.get("dependency_hash") is None
                    else str(raw_row.get("dependency_hash"))
                ),
            )
        )
    return rows


def _relation_column_contracts(
    template: ViewOutputTemplate,
) -> list[dict[str, Any]]:
    config = dict(template.config)
    contracts = config.get("column_contracts")
    if isinstance(contracts, list) and contracts:
        out: list[dict[str, Any]] = []
        for contract in contracts:
            if not isinstance(contract, Mapping):
                raise ValueError(
                    f"view output SQL relation template "
                    f"{template.slot_name!r} has non-object column contract"
                )
            name = str(contract.get("name") or "")
            if not name:
                raise ValueError(
                    f"view output SQL relation template "
                    f"{template.slot_name!r} has a column contract "
                    "without name"
                )
            out.append(
                {
                    "name": name,
                    "label": str(contract.get("label") or name),
                    "data_type": str(contract.get("data_type") or ""),
                    "required": bool(contract.get("required", True)),
                }
            )
        return out

    columns = []
    for column in config.get("columns") or ():
        column_name = str(column)
        if not column_name:
            raise ValueError(
                f"view output SQL relation template {template.slot_name!r} "
                "declares an empty column"
            )
        columns.append(
            {
                "name": column_name,
                "label": column_name,
                "data_type": "",
                "required": True,
            }
        )
    return columns


def _relation_column_names(template: ViewOutputTemplate) -> list[str]:
    return [column["name"] for column in _relation_column_contracts(template)]


def _relation_scope_contract(
    template: ViewOutputTemplate,
) -> dict[str, Any]:
    config = dict(template.config)
    scope = config.get("scope")
    if isinstance(scope, Mapping) and scope.get("policy"):
        return dict(scope)

    scope_key_column = str(config.get("scope_key_column") or "")
    if scope_key_column:
        return {"policy": "column", "column": scope_key_column}
    scope_key = str(config.get("scope_key") or "")
    if scope_key:
        return {"policy": "constant", "scope_key": scope_key}
    return {"policy": "view_slot"}


def _relation_row_identity_contract(
    template: ViewOutputTemplate,
) -> dict[str, Any]:
    config = dict(template.config)
    row_identity = config.get("row_identity")
    if isinstance(row_identity, Mapping) and row_identity.get("policy"):
        return dict(row_identity)

    row_key_columns = [str(column) for column in config.get("row_key_columns") or ()]
    if row_key_columns:
        return {"policy": "columns_hash", "columns": row_key_columns}
    row_key_column = str(config.get("row_key_column") or "")
    if row_key_column:
        return {"policy": "column", "column": row_key_column}
    if bool(config.get("allow_ordinal_row_key", False)):
        return {"policy": "ordinal"}
    return {"policy": "column", "column": "row_key"}


def _validate_required_relation_columns(
    *,
    template: ViewOutputTemplate,
    raw_row: Mapping[str, Any],
    columns: list[dict[str, Any]],
) -> None:
    missing = [
        column["name"]
        for column in columns
        if column.get("required", True) and column["name"] not in raw_row
    ]
    if missing:
        raise ValueError(
            f"view output SQL relation template {template.slot_name!r} "
            f"did not return required columns {missing}"
        )


def _infer_relation_row_columns(
    template: ViewOutputTemplate,
    raw_row: Mapping[str, Any],
) -> list[str]:
    excluded = {"dependency_hash"}
    scope = _relation_scope_contract(template)
    if scope.get("policy") == "column":
        excluded.add(str(scope.get("column") or ""))
    row_identity = _relation_row_identity_contract(template)
    if row_identity.get("policy") == "column":
        excluded.add(str(row_identity.get("column") or ""))
    elif row_identity.get("policy") == "columns_hash":
        excluded.update(str(column) for column in row_identity.get("columns") or ())
    return [key for key in raw_row if key not in excluded]


def _relation_scope_key_for_row(
    *,
    template: ViewOutputTemplate,
    view_def,
    raw_row: Mapping[str, Any],
) -> str:
    scope = _relation_scope_contract(template)
    policy = str(scope.get("policy") or "")
    if policy == "view_slot":
        return f"view:{view_def.view_id}:{template.slot_name}"
    if policy == "constant":
        return _relation_key_value(
            scope.get("scope_key"),
            context=(
                f"view output SQL relation template {template.slot_name!r} scope_key"
            ),
        )
    if policy == "column":
        column = str(scope.get("column") or "")
        if column not in raw_row:
            raise ValueError(
                f"view output SQL relation template {template.slot_name!r} "
                f"did not return scope column {column!r}"
            )
        return _relation_key_value(
            raw_row[column],
            context=(
                f"view output SQL relation template {template.slot_name!r} "
                f"scope column {column!r}"
            ),
        )
    raise ValueError(
        f"view output SQL relation template {template.slot_name!r} "
        f"declares unsupported scope policy {policy!r}"
    )


def _relation_row_key_for_row(
    *,
    template: ViewOutputTemplate,
    raw_row: Mapping[str, Any],
    index: int,
) -> str:
    row_identity = _relation_row_identity_contract(template)
    policy = str(row_identity.get("policy") or "")
    if policy == "column":
        column = str(row_identity.get("column") or "")
        if column not in raw_row:
            raise ValueError(
                f"view output SQL relation template {template.slot_name!r} "
                f"did not return row identity column {column!r}"
            )
        return _relation_key_value(
            raw_row[column],
            context=(
                f"view output SQL relation template {template.slot_name!r} "
                f"row identity column {column!r}"
            ),
        )
    if policy == "columns_hash":
        columns = [str(column) for column in row_identity.get("columns") or ()]
        if not columns:
            raise ValueError(
                f"view output SQL relation template {template.slot_name!r} "
                "declares columns_hash row identity without columns"
            )
        missing = [column for column in columns if column not in raw_row]
        if missing:
            raise ValueError(
                f"view output SQL relation template {template.slot_name!r} "
                f"did not return row identity columns {missing}"
            )
        values = {column: raw_row[column] for column in columns}
        return stable_hash(
            {
                "columns": columns,
                "values": _json_compatible(values),
            }
        )
    if policy == "ordinal":
        if not bool(dict(template.config).get("allow_ordinal_row_key", False)):
            raise ValueError(
                f"view output SQL relation template {template.slot_name!r} "
                "declares ordinal row identity without explicit allowance"
            )
        return str(index)
    raise ValueError(
        f"view output SQL relation template {template.slot_name!r} "
        f"declares unsupported row identity policy {policy!r}"
    )


def _relation_key_value(value: Any, *, context: str) -> str:
    if value is None:
        raise ValueError(f"{context} is null")
    text = str(value)
    if not text.strip():
        raise ValueError(f"{context} is blank")
    return text


def _json_compatible(value: Any) -> Any:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _relation_dirty_scope_keys(
    conn: psycopg.Connection[Any],
    *,
    definition: RelationArtifactDefinition,
    current_rows: list[RelationRow],
    target_generation_id: int,
) -> list[str]:
    current_hashes = _relation_scope_hashes(current_rows)
    scope_keys = set(current_hashes)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.manifest_id
              FROM okg.derived_output_manifests m
              JOIN okg.derived_output_slots s
                ON s.manifest_id = m.manifest_id
               AND s.slot_name = %(slot_name)s
             WHERE m.definition_id = %(definition_id)s
               AND m.target_generation_id < %(target_generation_id)s
               AND m.status = 'completed'
             ORDER BY m.target_generation_id DESC, m.promoted_at DESC
             LIMIT 1
            """,
            {
                "slot_name": RELATION_SLOT_NAME,
                "definition_id": definition.definition_id,
                "target_generation_id": target_generation_id,
            },
        )
        row = cur.fetchone()
    if row is None:
        return sorted(scope_keys)

    previous_hashes: dict[str, list[tuple[str, str, str | None]]] = {}
    for scope_key, row_key, row_hash, dependency_hash in (
        relation_visible_hash_rows_for_manifest(
            conn,
            definition,
            manifest_id=row[0],
        )
    ):
        previous_hashes.setdefault(str(scope_key), []).append(
            (
                str(row_key),
                str(row_hash),
                None if dependency_hash is None else str(dependency_hash),
            )
        )
    dirty_scope_keys = {
        scope_key
        for scope_key in set(current_hashes) | set(previous_hashes)
        if current_hashes.get(scope_key) != previous_hashes.get(scope_key)
    }
    return sorted(dirty_scope_keys)


def _relation_visible_row_count_after_scoped_refresh(
    conn: psycopg.Connection[Any],
    *,
    definition: RelationArtifactDefinition,
    target_generation_id: int,
    dirty_scope_keys: Sequence[str],
    dirty_row_count: int,
) -> int:
    """Visible row count for a relation-ref slot after a scoped refresh.

    ``dirty_row_count`` is only the rows staged by ``build_corpus`` for dirty
    scopes. The relation scheduler carries clean scopes forward, so the parent
    view's relation-reference metadata must count those carried-forward rows
    too.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.manifest_id, s.row_count
              FROM okg.derived_output_manifests m
              JOIN okg.derived_output_slots s
                ON s.manifest_id = m.manifest_id
               AND s.slot_name = %(slot_name)s
             WHERE m.definition_id = %(definition_id)s
               AND m.target_generation_id < %(target_generation_id)s
               AND m.status = 'completed'
             ORDER BY m.target_generation_id DESC, m.promoted_at DESC
             LIMIT 1
            """,
            {
                "slot_name": RELATION_SLOT_NAME,
                "definition_id": definition.definition_id,
                "target_generation_id": target_generation_id,
            },
        )
        row = cur.fetchone()
    if row is None:
        return dirty_row_count

    source_manifest_id, source_row_count = row
    previous_dirty_stats = relation_visible_stats_for_manifest(
        conn,
        definition,
        manifest_id=source_manifest_id,
        scope_keys=sorted(set(dirty_scope_keys)),
    )
    clean_row_count = max(
        0,
        int(source_row_count or 0)
        - int(previous_dirty_stats["row_count"] or 0),
    )
    return clean_row_count + dirty_row_count


def _relation_scope_hashes(
    rows: list[RelationRow],
) -> dict[str, list[tuple[str, str, str | None]]]:
    out: dict[str, list[tuple[str, str, str | None]]] = {}
    for row in rows:
        row_hash = stable_hash(
            {
                "scope_key": row.scope_key,
                "row_key": row.row_key,
                "row_data": row.row_data,
                "dependency_hash": row.dependency_hash,
            }
        )
        out.setdefault(row.scope_key, []).append(
            (
                row.row_key,
                row_hash,
                row.dependency_hash,
            )
        )
    for values in out.values():
        values.sort(key=lambda item: item[0])
    return out


def _replace_view_template_dependencies(
    conn: psycopg.Connection[Any],
    *,
    downstream_definition_id: str,
    dependencies: list[DerivedArtifactDependency],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            if _view_template_dependencies_current(
                cur,
                downstream_definition_id=downstream_definition_id,
                dependencies=dependencies,
            ):
                return
            cur.execute(
                """
                DELETE FROM okg.derived_artifact_dependencies
                 WHERE downstream_definition_id = %(downstream_definition_id)s
                   AND metadata->>'reason' = 'view output relation slot'
                """,
                {"downstream_definition_id": downstream_definition_id},
            )
    for dependency in dependencies:
        register_dependency(conn, dependency)


def _view_template_dependencies_current(
    cur: psycopg.Cursor[Any],
    *,
    downstream_definition_id: str,
    dependencies: list[DerivedArtifactDependency],
) -> bool:
    cur.execute(
        """
        SELECT upstream_definition_id, upstream_slot_name, dependency_kind,
               scope_key, required_compute_hash, required_output_schema_hash,
               required_renderer_hash, required_dirty_rule_hash, metadata
          FROM okg.derived_artifact_dependencies
         WHERE downstream_definition_id = %(downstream_definition_id)s
           AND metadata->>'reason' = 'view output relation slot'
        """,
        {"downstream_definition_id": downstream_definition_id},
    )
    existing = sorted(
        (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            stable_hash(dict(row[8] or {})),
        )
        for row in cur.fetchall()
    )
    expected = sorted(
        (
            dependency.upstream_definition_id,
            dependency.upstream_slot_name,
            dependency.dependency_kind,
            dependency.scope_key,
            dependency.required_compute_hash,
            dependency.required_output_schema_hash,
            dependency.required_renderer_hash,
            dependency.required_dirty_rule_hash,
            stable_hash(dict(dependency.metadata)),
        )
        for dependency in dependencies
    )
    return existing == expected


def _definitions_refreshable(
    conn: psycopg.Connection[Any],
    *,
    view_definition: ViewArtifactDefinition,
    relation_definitions: list[RelationArtifactDefinition],
    generation_id: int,
) -> bool:
    """True iff every output definition has a prior completed manifest whose
    hashes match the current definition (and no pending dirty scopes).

    This is the "scope-level refresh path is available" precondition: when it
    holds, the scheduler routes a relation through ``refresh_relation_artifact``
    (carry the clean scopes forward, replace only the dirty ones) rather than a
    full publish. It deliberately omits the input-fact watermark check (see
    ``_can_carry_forward_without_corpus``), which is unreliable under
    chronos-native publishing, so it can gate compute-incremental extraction
    without being suppressed by a stale watermark.
    """
    return (
        _refreshable_manifest_ids(
            conn,
            view_definition=view_definition,
            relation_definitions=relation_definitions,
            generation_id=generation_id,
        )
        is not None
    )


def _refreshable_manifest_ids(
    conn: psycopg.Connection[Any],
    *,
    view_definition: ViewArtifactDefinition,
    relation_definitions: list[RelationArtifactDefinition],
    generation_id: int,
) -> dict[str, Any] | None:
    definitions = {
        definition.definition_id: definition
        for definition in [*relation_definitions, view_definition]
    }
    definition_ids = sorted(definitions)
    if not definition_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM okg.derived_dirty_scopes
             WHERE definition_id = ANY(%(definition_ids)s)
               AND target_generation_id = %(target_generation_id)s
               AND consumed_run_id IS NULL
             LIMIT 1
            """,
            {
                "definition_ids": definition_ids,
                "target_generation_id": generation_id,
            },
        )
        if cur.fetchone() is not None:
            return None
        cur.execute(
            """
            WITH requested(definition_id) AS (
              SELECT unnest(%(definition_ids)s::text[])
            )
            SELECT requested.definition_id, latest.manifest_id,
                   latest.metadata,
                   run.compute_hash, run.output_schema_hash,
                   run.renderer_hash, run.dirty_rule_hash
              FROM requested
              CROSS JOIN LATERAL (
                SELECT m.manifest_id, m.run_id, m.metadata
                  FROM okg.derived_output_manifests m
                 WHERE m.definition_id = requested.definition_id
                   AND m.target_generation_id < %(before_generation_id)s
                   AND m.status = 'completed'
                 ORDER BY m.target_generation_id DESC, m.promoted_at DESC
                 LIMIT 1
              ) latest
              JOIN okg.derived_artifact_runs run
                ON run.run_id = latest.run_id
            """,
            {
                "definition_ids": definition_ids,
                "before_generation_id": generation_id,
            },
        )
        rows = {str(row[0]): row for row in cur.fetchall()}
    if set(rows) != set(definition_ids):
        return None
    manifest_ids: dict[str, Any] = {}
    for definition_id, definition in definitions.items():
        row = rows[definition_id]
        if (
            definition_id == view_definition.definition_id
            and (row[2] or {}).get("degraded_outcomes")
        ):
            return None
        if row[3:] != (
            definition.compute_hash,
            definition.output_schema_hash,
            definition.renderer_hash,
            definition.dirty_rule_hash,
        ):
            return None
        manifest_ids[definition_id] = row[1]
    return manifest_ids


def _can_carry_forward_without_corpus(
    conn: psycopg.Connection[Any],
    *,
    view_definition: ViewArtifactDefinition,
    relation_definitions: list[RelationArtifactDefinition],
    generation_id: int,
    input_fact_watermark: int,
    input_edge_fact_watermark: int,
) -> bool:
    if not _definitions_refreshable(
        conn,
        view_definition=view_definition,
        relation_definitions=relation_definitions,
        generation_id=generation_id,
    ):
        return False
    return _cached_input_signature_matches(
        conn,
        definition_id=view_definition.definition_id,
        before_generation_id=generation_id,
        input_fact_watermark=input_fact_watermark,
        input_edge_fact_watermark=input_edge_fact_watermark,
    )


def _carry_forward_without_corpus(
    conn: psycopg.Connection[Any],
    *,
    view_def,
    definition: ViewArtifactDefinition,
    relation_plan: list[_RelationTemplatePlan],
    deployment_name: str,
    generation_id: int,
    catalog_version_id: int | None,
    trusted_previous_manifest_ids: Mapping[str, uuid.UUID] | None = None,
    use_transaction: bool = True,
) -> MaterializeOutcome:
    total_started = time.perf_counter()
    timings_ms: dict[str, float] = {}

    def _record_timing(name: str, started: float) -> None:
        elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
        timings_ms[name] = round(timings_ms.get(name, 0.0) + elapsed, 3)

    metrics = {
        "publisher_native_view_stage": True,
        "native_view_fast_carry_forward": True,
        "scheduler_bypassed": True,
    }
    if catalog_version_id is not None:
        metrics["catalog_version_id"] = int(catalog_version_id)

    relation_source_count = 0
    for plan in relation_plan:
        source_manifest_id = (
            None
            if trusted_previous_manifest_ids is None
            else trusted_previous_manifest_ids.get(plan.definition.definition_id)
        )
        if source_manifest_id is None:
            _stage_t0 = time.perf_counter()
            source_manifest_id = _latest_completed_manifest_id(
                conn,
                definition_id=plan.definition.definition_id,
                before_generation_id=generation_id,
            )
            _record_timing("relation.latest_completed_manifest", _stage_t0)
        else:
            timings_ms["relation.latest_completed_manifest.trusted"] = 1.0
        if source_manifest_id is None:
            raise ValueError(
                "cannot carry forward relation artifact without a "
                f"completed manifest: {plan.definition.definition_id}"
            )
        relation_source_count += 1
    source_view_manifest_id = (
        None
        if trusted_previous_manifest_ids is None
        else trusted_previous_manifest_ids.get(definition.definition_id)
    )
    if source_view_manifest_id is None:
        _stage_t0 = time.perf_counter()
        source_view_manifest_id = _latest_completed_manifest_id(
            conn,
            definition_id=definition.definition_id,
            before_generation_id=generation_id,
        )
        _record_timing("view.latest_completed_manifest", _stage_t0)
    else:
        timings_ms["view.latest_completed_manifest.trusted"] = 1.0
    if source_view_manifest_id is None:
        raise ValueError(
            "cannot carry forward view artifact without a completed "
            f"manifest: {definition.definition_id}"
        )
    _stage_t0 = time.perf_counter()
    fragments_total = _manifest_fragment_count(
        conn,
        manifest_id=source_view_manifest_id,
    )
    _record_timing("fragment_count", _stage_t0)

    _group_t0 = time.perf_counter()
    transaction_context = conn.transaction() if use_transaction else nullcontext()
    with transaction_context:
        with conn.cursor() as cur:
            _group_body_t0 = time.perf_counter()
            if relation_source_count:
                timings_ms["relation.carry_forward.skipped_view_slot_reuse"] = (
                    float(relation_source_count)
                )

            _stage_t0 = time.perf_counter()
            view_timings: dict[str, float] = {}
            carried = _carry_forward_view_artifact_in_cursor(
                conn,
                cur,
                definition,
                source_manifest_id=source_view_manifest_id,
                target_generation_id=generation_id,
                metrics=metrics,
                definition_registered=True,
                timings_ms=view_timings,
            )
            for label, elapsed_ms in carried.timings_ms.items():
                timings_ms[f"view.{label}"] = round(float(elapsed_ms), 3)
            _record_timing("view.carry_forward", _stage_t0)
    _record_timing("carry_forward.transaction.body", _group_body_t0)
    _record_timing("carry_forward.transaction", _group_t0)
    _record_timing("total", total_started)
    return MaterializeOutcome(
        view_id=view_def.view_id,
        generation_id=generation_id,
        fragments_total=fragments_total,
        fragments_reused=fragments_total,
        manifest_path=None,
        manifest_hash=carried.manifest_hash,
        freshness="carried_forward",
        timings_ms=timings_ms,
    )


def _pending_dirty_scope_keys(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    target_generation_id: int,
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT scope_key
              FROM okg.derived_dirty_scopes
             WHERE definition_id = %(definition_id)s
               AND target_generation_id <= %(target_generation_id)s
               AND consumed_run_id IS NULL
             GROUP BY scope_key
             ORDER BY scope_key
            """,
            {
                "definition_id": definition_id,
                "target_generation_id": target_generation_id,
            },
        )
        return [str(row[0]) for row in cur.fetchall()]


def _latest_completed_manifest_id(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    before_generation_id: int,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT manifest_id
              FROM okg.derived_output_manifests
             WHERE definition_id = %(definition_id)s
               AND target_generation_id < %(before_generation_id)s
               AND status = 'completed'
             ORDER BY target_generation_id DESC, promoted_at DESC
             LIMIT 1
            """,
            {
                "definition_id": definition_id,
                "before_generation_id": before_generation_id,
            },
        )
        row = cur.fetchone()
    return None if row is None else row[0]


def _manifest_required_slot_count(
    conn: psycopg.Connection[Any],
    *,
    manifest_id,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cardinality(required_slots)
              FROM okg.derived_output_manifests
             WHERE manifest_id = %(manifest_id)s
               AND status = 'completed'
            """,
            {"manifest_id": manifest_id},
        )
        row = cur.fetchone()
    return 0 if row is None or row[0] is None else int(row[0])


def _manifest_fragment_count(
    conn: psycopg.Connection[Any],
    *,
    manifest_id,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.inline_payload->>'kind' AS payload_kind,
                   jsonb_typeof(s.inline_payload->'value') AS value_type,
                   jsonb_typeof(
                     s.inline_payload #> '{value,refresh_stats}'
                   ) AS refresh_stats_type,
                   s.inline_payload #>> '{value,refresh_stats,fragments_total}'
                     AS fragments_total,
                   cardinality(m.required_slots) AS required_slot_count
              FROM okg.derived_output_manifests m
              LEFT JOIN okg.derived_output_slots s
                ON s.manifest_id = m.manifest_id
               AND s.slot_name = 'json'
             WHERE m.manifest_id = %(manifest_id)s
               AND m.status = 'completed'
             LIMIT 1
            """,
            {"manifest_id": manifest_id},
        )
        row = cur.fetchone()
    if row is None:
        return 0
    payload_kind, value_type, refresh_stats_type, fragments_total, slot_count = row
    required_slot_count = 0 if slot_count is None else int(slot_count)
    if payload_kind == "json" and value_type == "object":
        if refresh_stats_type is None or refresh_stats_type == "null":
            return 0
        if refresh_stats_type == "object":
            try:
                return int(fragments_total or 0)
            except (TypeError, ValueError):
                pass
    return required_slot_count


def _definition_hashes_match(
    conn: psycopg.Connection[Any],
    *,
    definition: RelationArtifactDefinition | ViewArtifactDefinition,
    manifest_id,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.compute_hash, r.output_schema_hash,
                   r.renderer_hash, r.dirty_rule_hash
              FROM okg.derived_output_manifests m
              JOIN okg.derived_artifact_runs r
                ON r.run_id = m.run_id
             WHERE m.manifest_id = %(manifest_id)s
               AND m.definition_id = %(definition_id)s
               AND m.status = 'completed'
            """,
            {
                "manifest_id": manifest_id,
                "definition_id": definition.definition_id,
            },
        )
        row = cur.fetchone()
    return row == (
        definition.compute_hash,
        definition.output_schema_hash,
        definition.renderer_hash,
        definition.dirty_rule_hash,
    )


def _cached_input_signature_matches(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    before_generation_id: int,
    input_fact_watermark: int,
    input_edge_fact_watermark: int,
) -> bool:
    cache_key = _input_signature_match_cache_key(
        conn,
        definition_id=definition_id,
        input_fact_watermark=input_fact_watermark,
        input_edge_fact_watermark=input_edge_fact_watermark,
    )
    with _INPUT_SIGNATURE_MATCH_CACHE_LOCK:
        if _INPUT_SIGNATURE_MATCH_CACHE.get(cache_key):
            return True
    payload = _load_cached_view_payload(
        conn,
        definition_id=definition_id,
        before_generation_id=before_generation_id,
    )
    if payload is None:
        return False
    signature = payload.get("materializer_input_signature")
    if not isinstance(signature, Mapping):
        return False
    fact_watermark = signature.get("input_fact_watermark")
    edge_fact_watermark = signature.get("input_edge_fact_watermark")
    if fact_watermark is None or edge_fact_watermark is None:
        return False
    matches = int(fact_watermark) == int(input_fact_watermark) and int(
        edge_fact_watermark
    ) == int(input_edge_fact_watermark)
    if matches:
        with _INPUT_SIGNATURE_MATCH_CACHE_LOCK:
            if len(_INPUT_SIGNATURE_MATCH_CACHE) >= _INPUT_SIGNATURE_MATCH_CACHE_MAX:
                _INPUT_SIGNATURE_MATCH_CACHE.clear()
            _INPUT_SIGNATURE_MATCH_CACHE[cache_key] = True
    return matches


def _input_signature_match_cache_key(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    input_fact_watermark: int,
    input_edge_fact_watermark: int,
) -> tuple[str, str, int, int]:
    try:
        params = conn.info.get_parameters()
    except Exception:  # noqa: BLE001 - tests and alternate connection objects
        params = {}
    dsn_key = "|".join(
        f"{name}={params.get(name) or ''}"
        for name in ("host", "port", "dbname", "user")
    )
    return (
        dsn_key,
        definition_id,
        int(input_fact_watermark),
        int(input_edge_fact_watermark),
    )


def _definition_for(
    *,
    view_def,
    deployment_name: str,
    anchor_resolver: Any,
    output_templates: tuple[ViewOutputTemplate, ...],
) -> ViewArtifactDefinition:
    template_contracts = [template.to_contract() for template in output_templates]
    output_contract = {
        "slots": {
            template.slot_name: template.media_type
            for template in output_templates
            if template.required
        },
        "optional_slots": {
            template.slot_name: template.media_type
            for template in output_templates
            if not template.required
        },
        "output_templates": template_contracts,
        "view_manifest_version": 1,
    }
    dirty_rule = {
        "mode": "publisher_conservative_full_refresh",
        "scope_key": "publisher:view:<view_id>",
        "dependencies": ["node_facts", "edge_facts", "view_definition"],
    }
    return ViewArtifactDefinition(
        definition_id=f"view:{view_def.view_id}",
        deployment_name=deployment_name,
        view_id=view_def.view_id,
        view_name=view_def.display_name or view_def.view_id,
        compute_hash=stable_hash(
            {
                "view_id": view_def.view_id,
                "view_def_hash": view_def.view_def_hash,
                "planner": _callable_ref(view_def.planner),
                "planner_src": _planner_code_hash(view_def.planner),
                "params": view_def.params or {},
            }
        ),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash(
            {
                "assembler": _callable_ref(view_def.assembler),
                "anchor_resolver": _callable_ref(anchor_resolver),
                "output_templates": template_contracts,
            }
        ),
        dirty_rule_hash=stable_hash(dirty_rule),
        renderer_ref="okg.renderer.markdown.v1",
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _callable_ref(fn: Any) -> str:
    if fn is None:
        return "none"
    return f"{fn.__module__}.{getattr(fn, '__qualname__', fn.__class__.__name__)}"


def _planner_code_hash(planner: Any) -> str:
    """Hash the planner's source so a prompt edit changes the view's
    definition identity and busts the publisher carry-forward path.

    A planner owns its prompts in code (``planners/base.py``: "planners
    own their prompts"), not in ``view_def.params`` or ``view_def_hash``
    (which hashes only YAML wiring). Without this, a prompt-only edit
    leaves ``compute_hash`` constant, so ``definition_stable`` stays True
    and ``chronos_semantic_carry_forward`` re-serves the stale completion
    indefinitely for a quiescent-data view (#693).

    Fails closed: if the source cannot be read (a dynamically assembled
    lambda/closure, or a C-level callable), return a per-call unique
    token so the definition is treated as changed and the view is
    rebuilt this cycle. This is intentionally conservative — it forgoes
    carry-forward for unhashable planners rather than defaulting to a
    prompt-blind constant that would silently serve a stale completion
    after a prompt edit. Production planners are module-level and
    source-hashable, so this path is not hit in practice.
    """
    if planner is None:
        return "none"
    try:
        source = inspect.getsource(planner)
    except (OSError, TypeError):
        token = f"unhashable:{uuid.uuid4().hex}"
        logger.warning(
            "planner source unavailable for %r; forcing view rebuild "
            "(no carry-forward) this cycle to stay prompt-safe",
            _callable_ref(planner),
        )
        return token
    return stable_hash(source)


def _llm_signature(llm_client: Any) -> dict[str, Any]:
    if llm_client is None:
        return {"client": "none", "model": None}
    cls = llm_client.__class__
    return {
        "client": f"{cls.__module__}.{cls.__qualname__}",
        "model": getattr(llm_client, "model", None),
    }


def _input_signature(
    *,
    view_def,
    llm_client: Any,
    input_fact_watermark: int,
    input_edge_fact_watermark: int,
) -> dict[str, Any]:
    return {
        "version": 2,
        "authority": "derived_artifact_manifest",
        "view_def_hash": view_def.view_def_hash,
        "params": view_def.params or {},
        "llm": _llm_signature(llm_client),
        "input_fact_watermark": int(input_fact_watermark),
        "input_edge_fact_watermark": int(input_edge_fact_watermark),
    }


def _table_payload_for_view(view) -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    for position, fragment in enumerate(view.fragments):
        parsed = _parse_fragment_json(fragment.content)
        if parsed is None:
            continue
        tables.extend(
            _tables_from_parsed_fragment(
                fragment=fragment,
                parsed=parsed,
                position=position,
            )
        )
    return {
        "version": 1,
        "view_id": view.view_id,
        "generation_id": view.generation_id,
        "tables": tables,
    }


def _parse_fragment_json(content: str) -> Any | None:
    try:
        return json.loads(content)
    except (TypeError, json.JSONDecodeError):
        return None


def _tables_from_parsed_fragment(
    *,
    fragment,
    parsed: Any,
    position: int,
) -> list[dict[str, Any]]:
    if isinstance(parsed, list):
        rows = [row for row in parsed if isinstance(row, Mapping)]
        if len(rows) == len(parsed) and rows:
            return [
                _table_record(
                    fragment=fragment,
                    table_id=fragment.id,
                    title=_fragment_title(fragment),
                    columns=_columns_for_rows(rows),
                    rows=rows,
                    position=position,
                    table_kind="rows",
                )
            ]
        return []

    if not isinstance(parsed, Mapping):
        return []

    tables: list[dict[str, Any]] = []
    rows = parsed.get("rows")
    if isinstance(rows, list) and rows:
        table_rows = [row for row in rows if isinstance(row, Mapping)]
        if len(table_rows) == len(rows):
            tables.append(
                _table_record(
                    fragment=fragment,
                    table_id=fragment.id,
                    title=_fragment_title(fragment),
                    columns=_columns_for_rows(
                        table_rows,
                        preferred=parsed.get("columns"),
                    ),
                    rows=table_rows,
                    position=position,
                    table_kind="rows",
                )
            )

    scalar_rows = [
        {"field": key, "value": value}
        for key, value in parsed.items()
        if key not in {"rows", "columns"} and _is_scalar(value)
    ]
    if scalar_rows:
        tables.append(
            _table_record(
                fragment=fragment,
                table_id=f"{fragment.id}:fields",
                title=f"{_fragment_title(fragment)} fields",
                columns=["field", "value"],
                rows=scalar_rows,
                position=position,
                table_kind="fields",
            )
        )

    for key, value in parsed.items():
        if key in {"rows", "columns"}:
            continue
        if not isinstance(value, list) or not value:
            continue
        nested_rows = [row for row in value if isinstance(row, Mapping)]
        if len(nested_rows) != len(value):
            continue
        tables.append(
            _table_record(
                fragment=fragment,
                table_id=f"{fragment.id}:{key}",
                title=f"{_fragment_title(fragment)} / {key}",
                columns=_columns_for_rows(nested_rows),
                rows=nested_rows,
                position=position,
                table_kind="nested_rows",
            )
        )

    return tables


def _table_record(
    *,
    fragment,
    table_id: str,
    title: str,
    columns: list[str],
    rows: list[Mapping[str, Any]],
    position: int,
    table_kind: str,
) -> dict[str, Any]:
    return {
        "table_id": table_id,
        "title": title,
        "columns": columns,
        "rows": [dict(row) for row in rows],
        "source": {
            "fragment_id": fragment.id,
            "fragment_kind": fragment.kind,
            "fragment_position": position,
            "source_anchor_ids": list(fragment.sources),
        },
        "table_kind": table_kind,
    }


def _columns_for_rows(
    rows: list[Mapping[str, Any]],
    *,
    preferred: Any = None,
) -> list[str]:
    columns: list[str] = []
    if isinstance(preferred, list):
        for column in preferred:
            column_name = str(column)
            if column_name not in columns:
                columns.append(column_name)
    for row in rows:
        for key in row:
            column_name = str(key)
            if column_name not in columns:
                columns.append(column_name)
    return columns


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _fragment_title(fragment) -> str:
    label = getattr(fragment, "display_label", "") or ""
    return label or fragment.id


class _AssembledView:
    def __init__(
        self,
        *,
        markdown: str,
        markered: str | None = None,
        fragment_offsets: list[dict[str, Any]] | None = None,
    ) -> None:
        self.markdown = markdown
        self.markered = markered
        self.fragment_offsets = fragment_offsets


def _assemble_view(view, assembler) -> _AssembledView:
    raw = (assembler or default_assemble)(view)
    if isinstance(raw, AssembledWithMarkers):
        return _AssembledView(
            markdown=raw.markdown,
            markered=raw.markered,
            fragment_offsets=[offset.to_dict() for offset in raw.fragment_offsets],
        )
    return _AssembledView(markdown=str(raw))


def _load_cached_view(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    before_generation_id: int,
):
    value = _load_cached_view_payload(
        conn,
        definition_id=definition_id,
        before_generation_id=before_generation_id,
    )
    if value is None:
        return None
    view_manifest = value.get("view_manifest")
    if not isinstance(view_manifest, Mapping):
        return None
    try:
        return view_from_dict(dict(view_manifest))
    except Exception:
        logger.exception(
            "could not load cached native view manifest for %r",
            definition_id,
        )
        raise


def _load_cached_view_payload(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    before_generation_id: int,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.inline_payload
              FROM okg.derived_output_manifests m
              JOIN okg.derived_output_slots s
                ON s.manifest_id = m.manifest_id
               AND s.slot_name = 'json'
             WHERE m.definition_id = %(definition_id)s
               AND m.target_generation_id < %(before_generation_id)s
               AND m.status = 'completed'
             ORDER BY m.target_generation_id DESC, m.promoted_at DESC
             LIMIT 1
            """,
            {
                "definition_id": definition_id,
                "before_generation_id": before_generation_id,
            },
        )
        row = cur.fetchone()
    if row is None:
        return None
    inline_payload = dict(row[0] or {})
    if inline_payload.get("kind") != "json":
        return None
    value = inline_payload.get("value") or {}
    return dict(value) if isinstance(value, Mapping) else None


def _manifest_hash_for(
    conn: psycopg.Connection[Any],
    manifest_id,
) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT manifest_hash
              FROM okg.derived_output_manifests
             WHERE manifest_id = %(manifest_id)s
            """,
            {"manifest_id": manifest_id},
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


__all__ = ["NativeViewStage"]
