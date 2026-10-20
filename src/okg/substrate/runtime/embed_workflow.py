"""Embed worker as a DBOS workflow (migrate-substrate-runtime-to-dbos
Phase 3 §3.1).

Replaces the legacy ``okg.jobs`` ``embed`` job kind with a
``@DBOS.workflow``-decorated batch processor. The existing per-node
embed logic (cache-hit check on ``(text_hash, embedding_model)``,
verbatim-grounding-style ``WHERE node_id = ? AND text_hash = ?``
UPDATE) is preserved verbatim from ``jobs/embed_handler.py`` and
wrapped as a ``@DBOS.step`` so each per-node embed gets DBOS step
caching for free.

## Cache-hit + idempotency

Two layers of idempotency, both load-bearing:

  1. **In-Postgres cache** (preserved from the legacy handler): a
     node whose current row has ``embedding IS NOT NULL`` and
     ``embedding_model = <our model_id>`` skips the embedder call.
     This is what saves the provider bill on the second workflow
     run for an unchanged node.

  2. **DBOS step cache**: a step retry (worker crash mid-step) gets
     the cached step output rather than re-running. The step's
     input hash is over ``(deployment, node_id, text, text_hash,
     embedder_spec)`` so a retry with the same inputs returns the
     same output. The actual embed call inside the step still runs
     once — DBOS doesn't dedupe by side-effect, only by output.

Together: cold-cache embed runs once and bills once; retry / re-run
runs zero embed calls.

## Manifest contract

The worker admits one typed embedding configuration and passes a backend plan
whose declared and observed capability was verified before DBOS registration.
The workflow never re-reads raw embedding YAML or invents a backend default.
"""
from __future__ import annotations

import logging
from typing import Any


__all__ = [
    "EMBED_WORKFLOW_NAME",
    "EMBED_DEFAULT_BATCH_SIZE",
    "EMBED_DEFAULT_CRON",
    "EMBED_SCHEDULE_ID",
    "EMBEDDING_DIMENSION",
    "register_embed_workflow",
    "register_embed_schedule",
    "apply_embed_schedule",
]


log = logging.getLogger(__name__)


# Stable names used by operator tooling.
EMBED_WORKFLOW_NAME = "okg_embed_batch"
EMBED_SCHEDULE_ID = "okg_embed_batch"

EMBED_DEFAULT_BATCH_SIZE = 64

# Embedding-table column is vector(2560). The runtime preserves the
# legacy `embedder_dim_mismatch` failure mode if a deployment-config
# embedder doesn't match.
#
# RE-EXPORTED, not redefined (PACT reachable-semantic-search): this was a
# second copy of a fact the embeddings package already held, and the read
# side needs the same constant — it must not have to import a schema
# width from a DBOS workflow module.
from okg.substrate.embeddings.identity import (  # noqa: E402
    EMBEDDING_DIMENSION,
)

# Default cron for the embed workflow. Every 5 minutes — embed batches
# are bounded so the cycle finishes quickly when there's work, and
# returns no-op-fast when there isn't. Manifest-overridable via
# `runtime.schedules.okg_embed_batch`.
EMBED_DEFAULT_CRON = "*/5 * * * *"


_DECORATED: dict[str, Any] = {}
_SCHEDULE_DECORATED: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Workflow registration
# ---------------------------------------------------------------------------


def register_embed_workflow(*, deployment: str, embedding_plan) -> None:
    """Decorate the embed batch workflow + its per-node step.

    Called from the worker entry point AFTER ``DBOS()`` and BEFORE
    ``DBOS.launch()``. Idempotent.
    """
    from dbos import DBOS
    from okg.substrate.runtime.workflow_operational_outcomes import (
        define_operational_transactions,
        participant_for,
    )

    _DECORATED.clear()
    operational = define_operational_transactions(
        participant=participant_for(
            deployment=deployment,
            workflow_name=EMBED_WORKFLOW_NAME,
        ),
    )

    @DBOS.step(
        name=f"{EMBED_WORKFLOW_NAME}_one",
        retries_allowed=True,
        max_attempts=3,
        interval_seconds=2.0,
        backoff_rate=2.0,
    )
    def _embed_one_step(
        node_id: str,
        text: str,
        text_hash: str,
        plan_fingerprint: str,
    ) -> dict:
        """Embed one node's text + write to the Chronos graph.

        Step caching: the step's input hash is over
        ``(node_id, text, text_hash, plan_fingerprint)`` so a retry on
        the same inputs returns the cached output. The actual embed
        runs once per cold-cache input.
        """
        from okg.substrate.deployments import (
            require_postgres_dsn, resolve_deployment,
        )

        manifest = resolve_deployment(deployment)
        if manifest.embedding_config != embedding_plan.config:
            raise RuntimeError(
                "embed step execution plan disagrees with the current "
                "admitted manifest configuration"
            )
        dsn = require_postgres_dsn(manifest, deployment=deployment)
        if plan_fingerprint != embedding_plan.fingerprint:
            raise RuntimeError(
                "embed step execution-plan fingerprint disagrees with the "
                "worker's verified plan"
            )
        return _embed_and_write_one(
            dsn=dsn,
            node_id=node_id,
            text=text,
            text_hash=text_hash,
            embedding_plan=embedding_plan,
        )

    @DBOS.workflow(
        name=EMBED_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def _embed_workflow_impl(
        batch_size: int = EMBED_DEFAULT_BATCH_SIZE,
    ) -> dict:
        """One batch cycle. Returns a summary dict so operators can
        track via ``okg runtime debug-workflow``.

        Body shape:
          1. Resolve manifest; bail if embedding disabled.
          2. Claim a batch of unembedded nodes.
          3. For each, invoke the per-node embed step.
          4. Return summary.
        """
        operational.register_execution_tx()
        summary = _run_one_embed_batch(
            deployment=deployment,
            batch_size=batch_size,
            embed_one=_embed_one_step,
            embedding_plan=embedding_plan,
        )
        # Vector sets with a free backend: fill what the latest publish
        # left without a vector (PACT openrouter-embeddings-hnsw). A set's
        # own failure is in its report; a failure outside any set is
        # recorded in the summary and logged, not raised, so it cannot fail
        # the node-column batch. Both land in the outcome row's details,
        # which `okg doctor` reads (`vector_set_fill_failed`). Nothing is
        # re-raised: the node-column batch already ran, so failing the
        # workflow would only hide its outcome; DBOS cancellation is a
        # BaseException and still stops the workflow.
        try:
            summary["vector_sets"] = _fill_vector_sets_step()
        except Exception as exc:  # noqa: BLE001 -- recorded, logged
            log.error(
                "embed workflow: deployment=%s vector set fill failed: %s",
                deployment, exc,
            )
            summary["vector_sets_error"] = {
                "error": type(exc).__name__,
                "code": getattr(exc, "code", None),
                "reason": str(exc),
            }
        # Recorded after the fill: the health reader requires the outcome
        # within five minutes of the workflow's end, and a first fill can
        # run for hours.
        operational.record_outcome_tx(_embed_operational_outcome(summary))
        return summary

    @DBOS.step(name=f"{EMBED_WORKFLOW_NAME}_vector_sets")
    def _fill_vector_sets_step() -> list:
        return fill_vector_sets(deployment=deployment, embedding_plan=embedding_plan)

    _DECORATED[EMBED_WORKFLOW_NAME] = _embed_workflow_impl
    log.info(
        "registered embed workflow: deployment=%s name=%s",
        deployment, EMBED_WORKFLOW_NAME,
    )


def register_embed_schedule(*, deployment: str, dsn: str) -> None:
    """Decorate the scheduled enqueue workflow for the embed batch.

    Fires periodically (default every 5 minutes); enqueues
    ``okg_embed_batch`` onto the deployment's queue. Manifest-
    overridable cron.
    """
    from datetime import datetime
    from dbos import DBOS, DBOSClient

    _SCHEDULE_DECORATED.clear()

    @DBOS.workflow(name=f"{EMBED_SCHEDULE_ID}_enqueue")
    def _enqueue_embed_workflow(
        scheduled_time: datetime,
        context: Any,
    ) -> str | None:
        try:
            client = DBOSClient(
                database_url=dsn,
                system_database_url=dsn,
                application_database_url=dsn,
            )
            from okg.substrate.runtime.promotion_fence import (
                promotion_enqueue_permit,
            )

            with promotion_enqueue_permit(deployment=deployment, dsn=dsn):
                handle = client.enqueue(
                    {
                        "queue_name": f"okg-{deployment}",
                        "workflow_name": EMBED_WORKFLOW_NAME,
                    },
                )
            log.info(
                "scheduled embed enqueue: deployment=%s "
                "scheduled=%s workflow_id=%s",
                deployment, scheduled_time, handle.workflow_id,
            )
            return handle.workflow_id
        except Exception as e:  # noqa: BLE001 — fire-and-log
            log.error(
                "embed enqueue failed: deployment=%s error=%s",
                deployment, e,
            )
            return None

    _SCHEDULE_DECORATED[EMBED_SCHEDULE_ID] = _enqueue_embed_workflow
    log.info(
        "decorated embed schedule: %s", EMBED_SCHEDULE_ID,
    )


def apply_embed_schedule(
    *,
    schedule_overrides: dict[str, str],
) -> None:
    """Persist the embed schedule via ``DBOS.apply_schedules``."""
    from dbos import DBOS

    if EMBED_SCHEDULE_ID not in _SCHEDULE_DECORATED:
        raise RuntimeError(
            "apply_embed_schedule called before register_embed_schedule"
        )

    cron = schedule_overrides.get(
        EMBED_SCHEDULE_ID, EMBED_DEFAULT_CRON,
    )

    DBOS.apply_schedules([
        {
            "schedule_name": EMBED_SCHEDULE_ID,
            "workflow_fn": _SCHEDULE_DECORATED[EMBED_SCHEDULE_ID],
            "schedule": cron,
            "context": None,
            "automatic_backfill": False,
            "cron_timezone": None,
            "queue_name": None,
        },
    ])
    log.info(
        "applied embed schedule: %s cron=%r",
        EMBED_SCHEDULE_ID, cron,
    )


# ---------------------------------------------------------------------------
# Workflow body
# ---------------------------------------------------------------------------


def _run_one_embed_batch(
    *,
    deployment: str,
    batch_size: int,
    embed_one,
    embedding_plan,
) -> dict:
    """Resolve manifest, claim a batch, embed each node.

    `embed_one` is the @DBOS.step-decorated callable; passed in so
    the workflow body's signature doesn't need to know about its
    decorator state.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.deployments import (
        require_postgres_dsn, resolve_deployment,
    )

    manifest = resolve_deployment(deployment)
    if manifest.embedding_config != embedding_plan.config:
        raise RuntimeError(
            "embed workflow execution plan disagrees with the current "
            "admitted manifest configuration"
        )
    if not embedding_plan.enabled:
        log.info(
            "embed workflow: deployment=%s embedding disabled in "
            "manifest; skipping",
            deployment,
        )
        return {
            "skipped": True,
            "reason": "embedding disabled in manifest",
            "processed": 0,
        }

    from okg.substrate.embeddings.identity import is_paid_model_id

    if is_paid_model_id(embedding_plan.model_id):
        # The worker has no spend cap and records no cost, so it never makes
        # paid calls (PACT openrouter-embeddings-hnsw). A paid backend is
        # embedded by `okg db embed-backfill --max-usd`, which does both.
        log.warning(
            "embed workflow: deployment=%s backend %s is paid; refusing. "
            "Run `okg db embed-backfill --deployment %s --max-usd <usd>`.",
            deployment, embedding_plan.model_id, deployment,
        )
        return {
            "skipped": True,
            "reason": "paid_backend_requires_embed_backfill",
            "model_id": embedding_plan.model_id,
            "processed": 0,
            "errors": 0,
            "processed_node_ids": [],
            "failed_units": [],
        }

    if not getattr(embedding_plan, "node_column", True):
        # The search set's vectors have their own width, which the 2560-wide
        # node column cannot hold (PACT vector-set-pgvector-widths). The set
        # fill after this batch embeds them; the node column is not written.
        log.info(
            "embed workflow: deployment=%s search set %s is %s wide; the "
            "node column is not embedded",
            deployment, embedding_plan.model_id, embedding_plan.observed_dimension,
        )
        return {
            "skipped": True,
            "reason": "search_set_width_not_node_column",
            "model_id": embedding_plan.model_id,
            "processed": 0,
            "errors": 0,
            "processed_node_ids": [],
            "failed_units": [],
        }

    dsn = require_postgres_dsn(manifest, deployment=deployment)
    embedder_spec = embedding_plan.config.backend

    # Claim a batch of nodes that have text but no embedding (or an
    # embedding under a different model). The Chronos checkout exposes
    # the current graph, so no generation-window predicate is needed.
    from okg.substrate.graph_store import ChronosGraphStore

    claim_sql = """
        SELECT node_id, attrs->>'text' AS text, text_hash
          FROM okg.graph_nodes
         WHERE text_hash IS NOT NULL
           AND attrs ? 'text'
           AND (
                embedding IS NULL
                OR embedding_model IS DISTINCT FROM :model_id
           )
         ORDER BY node_id
         LIMIT :batch_size
    """
    with db_connect(dsn) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        rows = [
            (row["node_id"], row["text"], row["text_hash"])
            for row in graph.query(
                claim_sql,
                {"batch_size": batch_size, "model_id": embedding_plan.model_id},
            )
        ]

    if not rows:
        log.info(
            "embed workflow: deployment=%s no nodes pending; done",
            deployment,
        )
        return {
            "skipped": False,
            "any_failed": False,
            "processed": 0,
            "errors": 0,
            "embedder": embedder_spec,
            "processed_node_ids": [],
            "failed_units": [],
        }

    log.info(
        "embed workflow: deployment=%s claimed %d nodes; "
        "embedder=%s",
        deployment, len(rows), embedder_spec,
    )

    # `restore-publisher-perf-and-profiling` §B: profile only the
    # real-work branch (we've already short-circuited on disabled +
    # on empty claim).
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )

    processed: list[dict] = []
    errors: list[dict] = []
    with workflow_profiling_context(
        manifest=manifest,
        deployment=deployment,
        target_kind="embed",
        command="okg_embed_batch",
    ):
        # Per-node embeds run CONCURRENTLY as in-workflow DBOS steps.
        #
        # SHAPE DECISION (req.enqueue-shape-and-row-growth): concurrent
        # in-workflow steps, NOT enqueued child workflows. `Queue.enqueue`
        # dispatches WORKFLOWS, so enqueuing per item would create one
        # `dbos.workflow_status` row per node — a 50k-node batch would go from
        # one status row to 50k, plus 50k queue rows, per batch. This
        # deployment has already survived a multi-million-row DBOS-side blowup
        # requiring a sidecar drain and a staged repack. In-workflow steps keep
        # the row growth at what it already was: one `operation_outputs` row per
        # node, one `workflow_status` row per batch, and step caching still
        # makes a resumed batch skip completed nodes.
        #
        # Each step opens its own short chronos session, so there is no shared
        # connection to serialize on. Results are collected in completion order
        # into `processed`, which is unordered by contract (the caller counts
        # them); `errors` likewise.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from okg.substrate.db.concurrency_budget import width

        fanout = min(width("embeddings.batch"), max(1, len(rows)))
        if fanout <= 1 or len(rows) <= 1:
            for node_id, text, text_hash in rows:
                try:
                    processed.append(
                        embed_one(
                            node_id, text, text_hash,
                            embedding_plan.fingerprint,
                        )
                    )
                except Exception as e:  # noqa: BLE001
                    log.error(
                        "embed step failed: node_id=%s error=%s", node_id, e,
                    )
                    errors.append({"node_id": node_id, "error": str(e)})
        else:
            with ThreadPoolExecutor(
                max_workers=fanout, thread_name_prefix="okg-embed",
            ) as pool:
                futures = {
                    pool.submit(
                        embed_one, node_id, text, text_hash,
                        embedding_plan.fingerprint,
                    ): node_id
                    for node_id, text, text_hash in rows
                }
                for fut in as_completed(futures):
                    node_id = futures[fut]
                    try:
                        processed.append(fut.result())
                    except Exception as e:  # noqa: BLE001
                        log.error(
                            "embed step failed: node_id=%s error=%s", node_id, e,
                        )
                        errors.append({"node_id": node_id, "error": str(e)})

    n_processed = len(processed)
    n_errors = len(errors)
    any_failed = n_errors > 0
    all_failed = n_processed == 0 and n_errors > 0

    # Mirror source_sync_workflow.py's fail-loud contract
    # (fix-substrate-silent-failures + audit #681, req.audit-2026-07.
    # fail-closed-sweep): a batch in which EVERY claimed node's embed
    # raised produced no useful output, so raise — DBOS marks the
    # workflow row ERROR instead of a SUCCESS whose failure is buried
    # in the payload. This total-outage RAISE is the actual #681 fix and
    # does NOT depend on the silent-failure net.
    #
    # A partial failure still returns SUCCESS (the embeds that worked are
    # real work) and stamps ``any_failed: true`` on the return dict. The
    # operational-outcome transaction records the queryable health evidence.
    if all_failed:
        first_err = str(errors[0].get("error", "")) if errors else ""
        raise RuntimeError(
            f"embed workflow: all {n_errors} node embeds failed for "
            f"deployment {deployment!r}; no embeddings written. "
            f"First error: {first_err[:300]}"
        )
    return {
        "skipped": False,
        "any_failed": any_failed,
        "processed": n_processed,
        "errors": n_errors,
        "embedder": embedder_spec,
        "processed_node_ids": sorted(
            str(item.get("node_id")) for item in processed
            if item.get("node_id")
        ),
        "failed_units": sorted(
            f"{item.get('node_id')}|embed_failure" for item in errors
        ),
        "error_details": errors[:10],  # cap to avoid huge outputs
    }


def fill_vector_sets(*, deployment: str, embedding_plan) -> list[dict]:
    """Fill the deployment's vector sets that have a free backend, after a
    publish (Jason, lane chat 2026-10-14, "ehfill yes"); a paid set is
    refused, as the paid search backend is. See
    :func:`okg.substrate.embeddings.backfill.fill_free_sets`."""
    from okg.substrate.deployments import require_postgres_dsn, resolve_deployment
    from okg.substrate.embeddings.backfill import fill_free_sets

    manifest = resolve_deployment(deployment)
    if manifest.embedding_config != embedding_plan.config:
        raise RuntimeError(
            "embed workflow execution plan disagrees with the current "
            "admitted manifest configuration"
        )
    if not embedding_plan.enabled:
        return []
    reports = fill_free_sets(
        require_postgres_dsn(manifest, deployment=deployment),
        deployment=deployment,
        config=embedding_plan.config,
    )
    for report in reports:
        if report["status"] in ("refused_paid", "error"):
            log.warning("embed workflow: deployment=%s %s", deployment, report["reason"])
        else:
            log.info(
                "embed workflow: deployment=%s vector set %s: %s",
                deployment, report["set"], report["status"],
            )
    return reports


def _embed_operational_outcome(summary: dict[str, Any]) -> dict[str, Any]:
    """Map an embed summary to replay-stable typed evidence."""
    from okg.substrate.runtime.workflow_operational_outcomes import (
        WorkflowOperationalEvidenceError,
    )
    required = ("skipped", "processed", "errors", "processed_node_ids", "failed_units")
    if any(key not in summary for key in required):
        raise WorkflowOperationalEvidenceError(
            "embed summary lacks required operational outcome fields"
        )
    succeeded = summary["processed"]
    failed = summary["errors"]
    if (
        isinstance(succeeded, bool) or not isinstance(succeeded, int) or succeeded < 0
        or isinstance(failed, bool) or not isinstance(failed, int) or failed < 0
        or not isinstance(summary["processed_node_ids"], list)
        or not isinstance(summary["failed_units"], list)
    ):
        raise WorkflowOperationalEvidenceError("embed summary outcome fields are malformed")
    if bool(summary.get("skipped")):
        outcome = "skipped"
    elif failed and succeeded:
        outcome = "partial_failure"
    elif failed:
        outcome = "retained_failure"
    else:
        outcome = "clean"
    return {
        "outcome": outcome,
        "succeeded_count": succeeded,
        "failed_count": failed,
        "semantic_units": {
            "succeeded_node_ids": sorted(
                str(value) for value in summary.get("processed_node_ids") or ()
            ),
            "failed_units": sorted(
                str(value) for value in summary.get("failed_units") or ()
            ),
        },
        "details": {
            "embedder": summary.get("embedder"),
            **_vector_set_fill_details(summary),
        },
    }


#: Characters of a fill error's message kept in the outcome details (the
#: details object is capped at 8 KiB).
_FILL_REASON_CHARS = 300


def _vector_set_fill_details(summary: dict[str, Any]) -> dict[str, Any]:
    """The vector-set fill's result for the outcome row's details: each
    set's status, with the error of a failed one, and a failure outside any
    set. ``okg doctor`` reads the newest row (``vector_set_fill_failed``)."""
    out: dict[str, Any] = {}
    if "vector_sets" in summary:
        sets = []
        for report in summary["vector_sets"] or ():
            entry = {"set": report.get("set"), "status": report.get("status")}
            if report.get("status") == "error":
                entry.update(
                    error=report.get("error"),
                    code=report.get("code"),
                    reason=str(report.get("reason") or "")[:_FILL_REASON_CHARS],
                )
            sets.append(entry)
        out["vector_sets"] = sets
    error = summary.get("vector_sets_error")
    if error:
        out["vector_sets_error"] = {
            "error": error.get("error"),
            "code": error.get("code"),
            "reason": str(error.get("reason") or "")[:_FILL_REASON_CHARS],
        }
    return out


# ---------------------------------------------------------------------------
# Per-node embed (preserves legacy handler logic verbatim)
# ---------------------------------------------------------------------------


def _embed_and_write_one(
    *,
    dsn: str,
    node_id: str,
    text: str,
    text_hash: str,
    embedding_plan,
) -> dict:
    """Embed one node's text + write to the Chronos graph.

      * Cache hit on `(text_hash, embedding_model)` skips the embed
        call (saves provider cost).
      * The embedding is upserted onto the node's ``okg.graph_nodes``
        row (the Chronos graph) only when the row's ``text_hash``
        still matches the one we embedded.
      * `embedder.dimension` must equal `EMBEDDING_DIMENSION` or the
        embed step raises ``embedder_dim_mismatch``.
    """
    from okg.substrate.errors import SubstrateError
    from okg.substrate.graph_store import ChronosGraphStore

    if not embedding_plan.enabled or embedding_plan.embedder is None:
        raise RuntimeError("embedding write requires an enabled verified plan")
    if not getattr(embedding_plan, "node_column", True):
        raise RuntimeError(
            "embedding write requires a plan whose vectors fit the node column"
        )
    embedder = embedding_plan
    model_id = embedding_plan.model_id
    if model_id is None:
        raise RuntimeError("verified embedding plan has no model identity")

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        session = store.checkout()
        rows = session.query(
            """
            SELECT node_id, subtype, attrs, text_hash, embedding,
                   embedding_model, source, source_record_id,
                   source_revision, run_id, observed_at
              FROM okg.graph_nodes
             WHERE node_id = :node_id
               AND text_hash = :text_hash
             LIMIT 1
            """,
            {"node_id": node_id, "text_hash": text_hash},
        )
        if not rows:
            # The node's text changed (or it was retracted) since the
            # claim — nothing to embed against this text_hash.
            return {
                "node_id": node_id,
                "text_hash": text_hash,
                "model_id": model_id,
                "cache_hit": False,
                "skipped": True,
            }
        row = rows[0]
        # Cache hit: skip the embed API call.
        if row["embedding"] is not None and row["embedding_model"] == model_id:
            log.debug(
                "embed: node_id=%s cache hit (model=%s); skipping",
                node_id, model_id,
            )
            return {
                "node_id": node_id,
                "text_hash": text_hash,
                "model_id": model_id,
                "cache_hit": True,
            }

        vec = embedder.embed(text)
        if len(vec) != embedding_plan.observed_dimension:
            err = SubstrateError(
                f"embedder returned vector of length {len(vec)} for node "
                f"{node_id!r}; verified plan requires "
                f"{embedding_plan.observed_dimension}",
                data={"node_id": node_id, "length": len(vec)},
            )
            err.code = "embedder_dim_mismatch"
            raise err
        vecstr = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"

        from okg.substrate.nomos.read_class import stamp_row

        with session.transaction():
            # The new version's Nomos read class, from its own content
            # (PACT lane-h-nomos-read-class-stored); never the prior version's.
            # Its catalog row is written first, in this transaction (PACT
            # lane-h-nomos-read-class-sql).
            session.upsert_rows("okg.graph_nodes", [stamp_row({
                "node_id": row["node_id"],
                "subtype": row["subtype"],
                "attrs": dict(row["attrs"] or {}),
                "text_hash": row["text_hash"],
                "embedding": vecstr,
                "embedding_model": model_id,
                "source": row["source"],
                "source_record_id": dict(row["source_record_id"] or {}),
                "source_revision": dict(row["source_revision"] or {}),
                "run_id": row["run_id"],
                "observed_at": row["observed_at"],
            }, "node", catalog=store.context.db)])
    finally:
        store.close()

    return {
        "node_id": node_id,
        "text_hash": text_hash,
        "model_id": model_id,
        "cache_hit": False,
    }


def _build_embedder_from_spec(spec: str):
    """Spec → Embedder instance.

    Delegates to the ONE parser in `okg.substrate.embeddings.identity`. This
    used to be a second copy that "mirrored" `cli.main._build_embedder`, which
    meant the WRITER of embeddings and the READER parsed specs independently
    — one copy changing its default model would have made the same spec
    string mean two different embedders (PACT reachable-semantic-search).
    """
    from okg.substrate.embeddings.identity import build_embedder

    return build_embedder(spec)
