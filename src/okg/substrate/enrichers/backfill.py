"""Targeted, chunked backfill for one registered enricher."""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.enrichers.base import IncrementalContext
from okg.substrate.errors import (
    EnricherBackfillAdmissionRejected,
    EnricherBackfillBranchUnsupported,
    EnricherBackfillChunkFailed,
    EnricherBackfillSelectionError,
    EnricherBackfillUnsafeEnricher,
    EnricherBackfillUnsupportedOutput,
    SubstrateError,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact
from okg.substrate.publisher.publisher import (
    _ENRICHMENT_CHECKSUM,
    _ENRICHMENT_PROGRAM,
    Publisher,
)
from okg.substrate.publisher.factory import build_deployment_publisher
from okg.substrate.transform.commit import (
    persist_transform_program,
    pin_commit_witness,
)
from okg.substrate.db.connection import connect as db_connect


@dataclass(frozen=True)
class EnricherBackfillChunk:
    chunk_index: int
    input_node_ids: tuple[str, ...]
    status: str
    n_edges_emitted: int
    n_edges_skipped: int
    edges_applied: int
    duration_ms: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_index": self.chunk_index,
            "input_node_ids": list(self.input_node_ids),
            "input_count": len(self.input_node_ids),
            "status": self.status,
            "n_edges_emitted": self.n_edges_emitted,
            "n_edges_skipped": self.n_edges_skipped,
            "edges_applied": self.edges_applied,
            "duration_ms": round(self.duration_ms, 3),
        }


@dataclass(frozen=True)
class EnricherBackfillResult:
    deployment: str
    branch_name: str
    enricher_name: str
    status: str
    requested_limit: int
    chunk_size: int
    processed_inputs: int
    remaining_missing: int
    progress_advanced: bool
    catalog_version_id: int
    profile_fingerprint: str
    transform_checksum: str
    input_fact_watermark: int
    input_edge_watermark: int
    chunks: tuple[EnricherBackfillChunk, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            "deployment": self.deployment,
            "branch_name": self.branch_name,
            "enricher_name": self.enricher_name,
            "status": self.status,
            "requested_limit": self.requested_limit,
            "chunk_size": self.chunk_size,
            "processed_inputs": self.processed_inputs,
            "remaining_missing": self.remaining_missing,
            "progress_advanced": self.progress_advanced,
            "catalog_version_id": self.catalog_version_id,
            "profile_fingerprint": self.profile_fingerprint,
            "transform_checksum": self.transform_checksum,
            "input_fact_watermark": self.input_fact_watermark,
            "input_edge_watermark": self.input_edge_watermark,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
        }


def select_enricher(
    enrichers: Sequence[Any],
    enricher_name: str,
    *,
    deployment: str | None = None,
) -> Any:
    matches = [
        enricher for enricher in enrichers
        if str(getattr(enricher, "name", enricher.__class__.__name__))
        == enricher_name
    ]
    if len(matches) == 1:
        return matches[0]
    available = [
        str(getattr(enricher, "name", enricher.__class__.__name__))
        for enricher in enrichers
    ]
    if not matches:
        raise EnricherBackfillSelectionError(
            f"enricher {enricher_name!r} is not registered",
            data={
                "deployment": deployment,
                "enricher_name": enricher_name,
                "matches": [],
                "available": available,
            },
            fix_hint="check the deployment's enrichers block",
        )
    raise EnricherBackfillSelectionError(
        f"enricher name {enricher_name!r} is ambiguous",
        data={
            "deployment": deployment,
            "enricher_name": enricher_name,
            "matches": [enricher_name for _ in matches],
            "available": available,
        },
        fix_hint="give registered enrichers unique names",
    )


def run_enricher_backfill(
    *,
    dsn: str,
    deployment: str,
    enricher_name: str,
    enrichers: Sequence[Any],
    limit: int,
    chunk_size: int,
    branch_name: str = "default",
    publisher_kwargs: dict[str, Any] | None = None,
    publisher_stack: Any | None = None,
) -> EnricherBackfillResult:
    if limit <= 0:
        raise SubstrateError("--limit must be greater than zero")
    if chunk_size <= 0:
        raise SubstrateError("--chunk-size must be greater than zero")
    if branch_name != "default":
        raise EnricherBackfillBranchUnsupported(
            "targeted enricher backfill currently supports only the default branch",
            data={
                "deployment": deployment,
                "enricher_name": enricher_name,
                "branch_name": branch_name,
            },
            fix_hint=(
                "run targeted backfill on the default branch; branch-scoped "
                "normal enricher progress is required before non-default "
                "branches are supported"
            ),
        )

    selected = select_enricher(
        enrichers,
        enricher_name,
        deployment=deployment,
    )
    _require_chunk_safe_enricher(
        selected,
        deployment=deployment,
        enricher_name=enricher_name,
    )
    _read_subtypes_for_backfill(
        selected,
        deployment=deployment,
        enricher_name=enricher_name,
    )
    _reject_node_output_enricher(
        selected,
        deployment=deployment,
        enricher_name=enricher_name,
    )

    kwargs = dict(publisher_kwargs or {})
    kwargs["enrichers"] = [selected]
    publisher = build_deployment_publisher(
        dsn=dsn,
        deployment_name=deployment,
        command="okg enricher backfill",
        branch_name=branch_name,
        stack=publisher_stack,
        publisher_overrides=kwargs,
    )

    chunks: list[EnricherBackfillChunk] = []
    # `idle_in_transaction_session_timeout=0` disables the bound for THIS
    # session only. The whole backfill runs in one transaction: the advisory
    # lock is taken here and every chunk calls `enricher.enrich(conn, ...)`,
    # which for an LLM enricher spends its time on client-side model calls
    # between statements. The session is legitimately idle in transaction for
    # as long as one chunk's enrichment takes, which on a real backfill
    # exceeds the 300-second default and would be killed with SQLSTATE 25P03,
    # losing the whole run. The factory's other protections (keepalives,
    # connect_timeout) still apply, and libpq takes the last `-c` for a
    # setting, so this overrides the factory's own value.
    with db_connect(
        dsn, autocommit=False, options="-c idle_in_transaction_session_timeout=0",
    ) as conn:
        with publisher._locked_connection(conn) as locked_conn:
            catalog_version_id = publisher._current_catalog_version_id(
                locked_conn,
            )
            result = _run_backfill_locked(
                locked_conn,
                publisher=publisher,
                selected=selected,
                enricher_name=enricher_name,
                deployment=deployment,
                branch_name=branch_name,
                limit=limit,
                chunk_size=chunk_size,
                chunks=chunks,
                catalog_version_id=catalog_version_id,
            )
    return result


def _run_backfill_locked(
    conn: psycopg.Connection,
    *,
    publisher: Publisher,
    selected: Any,
    enricher_name: str,
    deployment: str,
    branch_name: str,
    limit: int,
    chunk_size: int,
    chunks: list[EnricherBackfillChunk],
    catalog_version_id: int | None,
) -> EnricherBackfillResult:
    if catalog_version_id is None:
        err = SubstrateError(
            "targeted enricher backfill requires an applied catalog",
            data={"deployment": deployment},
            fix_hint="run `okg catalog load --apply` first",
        )
        err.code = "no_applied_catalog"
        raise err
    catalog_identity = int(catalog_version_id)
    producer_id = f"_enricher:{enricher_name}"
    profile_fingerprint = publisher._expected_enricher_profile_fingerprint(
        conn,
        enricher=selected,
        producer_id=producer_id,
        catalog_version_id=catalog_identity,
    )
    input_fact_watermark, input_edge_watermark = (
        publisher._current_fact_watermarks(conn)
    )
    worklist = _missing_input_ids(
        conn,
        deployment=deployment,
        branch_name=branch_name,
        enricher=selected,
        enricher_name=enricher_name,
        catalog_version_id=catalog_identity,
        profile_fingerprint=profile_fingerprint,
        transform_checksum=_ENRICHMENT_CHECKSUM,
        limit=limit,
    )
    processed = 0
    for chunk_index, chunk_ids in enumerate(
        _chunks(worklist, min(chunk_size, limit)),
        start=1,
    ):
        chunk_result = _run_chunk(
            conn,
            publisher=publisher,
            enricher=selected,
            enricher_name=enricher_name,
            producer_id=producer_id,
            deployment=deployment,
            branch_name=branch_name,
            catalog_version_id=catalog_identity,
            profile_fingerprint=profile_fingerprint,
            input_fact_watermark=input_fact_watermark,
            input_edge_watermark=input_edge_watermark,
            input_node_ids=chunk_ids,
            chunk_index=chunk_index,
        )
        chunks.append(chunk_result)
        processed += len(chunk_ids)

    remaining = _missing_input_count(
        conn,
        deployment=deployment,
        branch_name=branch_name,
        enricher=selected,
        enricher_name=enricher_name,
        catalog_version_id=catalog_identity,
        profile_fingerprint=profile_fingerprint,
        transform_checksum=_ENRICHMENT_CHECKSUM,
    )
    progress_advanced = False
    if remaining == 0:
        publisher._advance_enricher_progress(
            conn,
            enricher_name,
            success=True,
            failure_repr=None,
            generation_id=0,
            catalog_version_id=catalog_identity,
            current_fact_watermark=input_fact_watermark,
            current_edge_watermark=input_edge_watermark,
            emitted_count=sum(int(c.n_edges_emitted) for c in chunks),
            dormant_ok=bool(getattr(selected, "dormant_ok", False)),
        )
        conn.commit()
        progress_advanced = True
    status = "completed" if remaining == 0 else "partial"
    return EnricherBackfillResult(
        deployment=deployment,
        branch_name=branch_name,
        enricher_name=enricher_name,
        status=status,
        requested_limit=limit,
        chunk_size=chunk_size,
        processed_inputs=processed,
        remaining_missing=remaining,
        progress_advanced=progress_advanced,
        catalog_version_id=catalog_identity,
        profile_fingerprint=profile_fingerprint,
        transform_checksum=_ENRICHMENT_CHECKSUM,
        input_fact_watermark=input_fact_watermark,
        input_edge_watermark=input_edge_watermark,
        chunks=tuple(chunks),
    )


def _read_subtypes_for_backfill(
    enricher: Any,
    *,
    deployment: str,
    enricher_name: str,
) -> tuple[str, ...]:
    reads_subtypes = tuple(getattr(enricher, "reads_subtypes", ()) or ())
    if reads_subtypes:
        return tuple(str(subtype) for subtype in reads_subtypes)
    raise EnricherBackfillSelectionError(
        "targeted enricher backfill requires reads_subtypes",
        data={
            "deployment": deployment,
            "enricher_name": enricher_name,
            "matches": [enricher_name],
            "missing_contract": "reads_subtypes",
        },
        fix_hint=(
            "declare reads_subtypes for the enricher, or use normal publish "
            "for an intentionally whole-graph first run"
        ),
    )


def _reject_node_output_enricher(
    enricher: Any,
    *,
    deployment: str,
    enricher_name: str,
) -> None:
    unsupported_fields: list[str] = []
    for attr in (
        "emits_node_types",
        "emits_node_subtypes",
        "emits_nodes",
        "emits_record_kinds",
        "node_output_types",
    ):
        value = getattr(enricher, attr, None)
        if not value:
            continue
        if attr == "emits_record_kinds" and "node" not in {
            str(item) for item in value
        }:
            continue
        unsupported_fields.append(attr)
    for attr in ("producer_policy", "authority"):
        policy = getattr(enricher, attr, None)
        if not isinstance(policy, dict):
            continue
        raw_policy = policy.get("producer_policy")
        if isinstance(raw_policy, dict):
            allowed = raw_policy.get("allowed_record_kinds") or ()
            signature = raw_policy.get("output_signature") or {}
        else:
            allowed = policy.get("allowed_record_kinds") or ()
            signature = policy.get("output_signature") or {}
        if "node" in {str(item) for item in allowed}:
            unsupported_fields.append(f"{attr}.allowed_record_kinds")
        if isinstance(signature, dict) and signature.get("nodes"):
            unsupported_fields.append(f"{attr}.output_signature.nodes")
    if unsupported_fields:
        raise EnricherBackfillUnsupportedOutput(
            "targeted enricher backfill supports edge-only enrichers",
            data={
                "deployment": deployment,
                "enricher_name": enricher_name,
                "unsupported_fields": sorted(set(unsupported_fields)),
            },
            fix_hint=(
                "use normal publish for node-emitting enrichers until "
                "targeted node-fact admission is implemented"
            ),
        )


def _require_chunk_safe_enricher(
    enricher: Any,
    *,
    deployment: str,
    enricher_name: str,
) -> None:
    if getattr(enricher, "targeted_backfill_chunk_safe", False) is True:
        return
    raise EnricherBackfillUnsafeEnricher(
        "targeted enricher backfill requires a chunk-safe enricher",
        data={
            "deployment": deployment,
            "enricher_name": enricher_name,
            "required_capability": "targeted_backfill_chunk_safe",
        },
        fix_hint=(
            "set targeted_backfill_chunk_safe only after the enricher "
            "strictly scopes work and emitted edge facts to "
            "incremental.dirty_node_ids"
        ),
    )


def _missing_input_ids(
    conn: psycopg.Connection,
    *,
    deployment: str,
    branch_name: str,
    enricher: Any,
    enricher_name: str,
    catalog_version_id: int,
    profile_fingerprint: str,
    transform_checksum: str,
    limit: int,
) -> list[str]:
    rows = _query_missing_inputs(
        conn,
        deployment=deployment,
        branch_name=branch_name,
        enricher=enricher,
        enricher_name=enricher_name,
        catalog_version_id=catalog_version_id,
        profile_fingerprint=profile_fingerprint,
        transform_checksum=transform_checksum,
        select_sql="n.node_id",
        limit=limit,
    )
    return [str(row["node_id"]) for row in rows]


def _missing_input_count(
    conn: psycopg.Connection,
    *,
    deployment: str,
    branch_name: str,
    enricher: Any,
    enricher_name: str,
    catalog_version_id: int,
    profile_fingerprint: str,
    transform_checksum: str,
) -> int:
    rows = _query_missing_inputs(
        conn,
        deployment=deployment,
        branch_name=branch_name,
        enricher=enricher,
        enricher_name=enricher_name,
        catalog_version_id=catalog_version_id,
        profile_fingerprint=profile_fingerprint,
        transform_checksum=transform_checksum,
        select_sql="count(*) AS n",
        limit=None,
    )
    return int(rows[0]["n"] if rows else 0)


def _query_missing_inputs(
    conn: psycopg.Connection,
    *,
    deployment: str,
    branch_name: str,
    enricher: Any,
    enricher_name: str,
    catalog_version_id: int,
    profile_fingerprint: str,
    transform_checksum: str,
    select_sql: str,
    limit: int | None,
) -> list[dict[str, Any]]:
    reads_subtypes = _read_subtypes_for_backfill(
        enricher,
        deployment=deployment,
        enricher_name=enricher_name,
    )
    store = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
    )
    params: dict[str, Any] = {
        "deployment": deployment,
        "branch_name": branch_name,
        "enricher_name": enricher_name,
        "catalog_version_id": catalog_version_id,
        "profile_fingerprint": profile_fingerprint,
        "transform_checksum": transform_checksum,
        "subtypes": list(reads_subtypes),
    }
    order_limit = ""
    if limit is not None:
        order_limit = "ORDER BY n.node_id LIMIT :limit"
        params["limit"] = int(limit)
    sql = f"""
        SELECT {select_sql}
          FROM okg.graph_nodes n
         WHERE 1 = 1
           AND n.subtype = ANY(:subtypes)
           AND NOT EXISTS (
             SELECT 1
               FROM okg.enricher_backfill_progress p
              WHERE p.deployment_name = :deployment
                AND p.branch_name = :branch_name
                AND p.enricher_name = :enricher_name
                AND p.input_node_id = n.node_id
                AND p.catalog_version_id = :catalog_version_id
                AND p.profile_fingerprint = :profile_fingerprint
                AND p.transform_checksum = :transform_checksum
                AND p.status = 'complete'
           )
         {order_limit}
    """
    return store.checkout(branch_name).query(sql, params)


def _run_chunk(
    conn: psycopg.Connection,
    *,
    publisher: Publisher,
    enricher: Any,
    enricher_name: str,
    producer_id: str,
    deployment: str,
    branch_name: str,
    catalog_version_id: int,
    profile_fingerprint: str,
    input_fact_watermark: int,
    input_edge_watermark: int,
    input_node_ids: Sequence[str],
    chunk_index: int,
) -> EnricherBackfillChunk:
    started = time.perf_counter()
    run_id = str(uuid.uuid4())
    try:
        persist_transform_program(conn, _ENRICHMENT_PROGRAM)
        # This connection is outside autocommit, so the pin lasts for this
        # chunk's transaction (issue #2965) and cannot outlive it; a checkout
        # read through library.linkers._chronos restores it.
        pin_commit_witness(conn, _ENRICHMENT_CHECKSUM)
        node_before, edge_before = publisher._current_fact_watermarks(conn)
        publisher._materialize_enricher_profile(
            conn,
            enricher=enricher,
            producer_id=producer_id,
            catalog_version_id=catalog_version_id,
        )
        ctx = IncrementalContext(
            prev_fact_watermark=input_fact_watermark,
            prev_edge_watermark=input_edge_watermark,
            dirty_node_ids=frozenset(str(node_id) for node_id in input_node_ids),
            is_first_run=False,
            last_status="targeted_backfill",
            current_fact_watermark=input_fact_watermark,
            current_edge_watermark=input_edge_watermark,
            prev_catalog_version_id=catalog_version_id,
            current_catalog_version_id=catalog_version_id,
            change_basis="targeted_enricher_backfill",
        )
        failure_repr = None
        result = None
        try:
            result = enricher.enrich(
                conn,
                generation_id=0,
                incremental=ctx,
            )
        except Exception as exc:  # noqa: BLE001 - rollback keeps chunk retryable
            failure_repr = f"failed:{type(exc).__name__}"
            _admit_chunk_outputs(
                publisher,
                conn,
                enricher=enricher,
                generation_id=0,
                catalog_version_id=catalog_version_id,
                edge_before=edge_before,
                failure_reason=failure_repr,
            )
            raise EnricherBackfillChunkFailed(
                "targeted enricher backfill chunk failed",
                data={
                    "deployment": deployment,
                    "enricher_name": enricher_name,
                    "chunk_index": chunk_index,
                    "input_node_ids": list(input_node_ids),
                    "error": repr(exc),
                },
                fix_hint="fix the enricher exception and retry the chunk",
            ) from exc

        unexpected_sources = _unexpected_fact_sources(
            conn,
            source_prefix=producer_id,
            min_node_fact_id=node_before,
            min_edge_fact_id=edge_before,
        )
        if unexpected_sources:
            raise EnricherBackfillChunkFailed(
                "targeted enricher backfill emitted facts for another producer",
                data={
                    "deployment": deployment,
                    "enricher_name": enricher_name,
                    "chunk_index": chunk_index,
                    "input_node_ids": list(input_node_ids),
                    "unexpected_sources": unexpected_sources,
                },
                fix_hint=(
                    "fix the selected enricher to emit facts only under "
                    f"{producer_id!r}"
                ),
            )

        node_fact_count = _selected_node_fact_count(
            conn,
            source_prefix=producer_id,
            min_node_fact_id=node_before,
        )
        if node_fact_count:
            raise EnricherBackfillUnsupportedOutput(
                "targeted enricher backfill does not support node facts",
                data={
                    "deployment": deployment,
                    "enricher_name": enricher_name,
                    "chunk_index": chunk_index,
                    "input_node_ids": list(input_node_ids),
                    "node_fact_count": node_fact_count,
                    "unsupported_fields": ["emitted_node_facts"],
                },
                fix_hint=(
                    "use normal publish for node-emitting enrichers until "
                    "targeted node-fact admission is implemented"
                ),
            )

        verdicts = _admit_chunk_outputs(
            publisher,
            conn,
            enricher=enricher,
            generation_id=0,
            catalog_version_id=catalog_version_id,
            edge_before=edge_before,
            failure_reason=None,
        )
        rejected = [
            verdict for verdict in verdicts
            if str(verdict.get("status")) in {"rejected", "failed"}
        ]
        if rejected:
            raise EnricherBackfillAdmissionRejected(
                "targeted enricher backfill chunk failed admission",
                data={
                    "deployment": deployment,
                    "enricher_name": enricher_name,
                    "chunk_index": chunk_index,
                    "input_node_ids": list(input_node_ids),
                    "verdicts": rejected,
                },
                fix_hint=(
                    "fix the enricher producer policy or emitted edge triples, "
                    "then retry"
                ),
            )

        derived_facts = _load_selected_derived_edge_facts(
            conn,
            min_edge_fact_id=edge_before,
            source_prefix=producer_id,
        )
        _validate_chunk_edge_scope(
            derived_facts,
            deployment=deployment,
            enricher_name=enricher_name,
            chunk_index=chunk_index,
            input_node_ids=input_node_ids,
        )
        apply_result = None
        if derived_facts:
            from okg.substrate.fact_application import apply_bounded_fact_batch

            store = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
                autocommit=False,
            )
            applied_batch = apply_bounded_fact_batch(
                conn,
                store,
                derived_facts,
                deployment_name=deployment,
                producer_id=producer_id,
                producer_kind="enricher",
                intent="targeted_enricher_backfill",
                catalog_version_id=catalog_version_id,
                authority_scope={
                    "dependency_generation": 0,
                    "input_lineage": {
                        "node_ids": list(input_node_ids),
                        "profile_fingerprint": profile_fingerprint,
                    },
                    "invalidation_scope": {
                        "node_ids": list(input_node_ids),
                        "chunk_index": chunk_index,
                    },
                    "determinism": "deterministic",
                    "incomplete_result": False,
                    "watermark": {
                        "node_fact": input_fact_watermark,
                        "edge_fact": input_edge_watermark,
                    },
                },
                source=producer_id,
                target_branch=branch_name,
                run_id=run_id,
                completion="partial",
                strict_endpoints=False,
            )
            apply_result = applied_batch.last_primitive_result
        emitted_counts_by_input = _emitted_edge_counts_by_input(
            derived_facts,
            input_node_ids=input_node_ids,
        )
        _mark_inputs_complete(
            conn,
            deployment=deployment,
            branch_name=branch_name,
            enricher_name=enricher_name,
            input_node_ids=input_node_ids,
            catalog_version_id=catalog_version_id,
            profile_fingerprint=profile_fingerprint,
            emitted_edge_counts_by_input=emitted_counts_by_input,
            run_id=run_id,
            metadata={
                "chunk_index": chunk_index,
                "verdicts": verdicts,
                "chunk_emitted_edge_count": int(
                    getattr(result, "n_edges_emitted", 0) or 0
                ),
            },
        )
        conn.commit()
        duration_ms = (time.perf_counter() - started) * 1000
        return EnricherBackfillChunk(
            chunk_index=chunk_index,
            input_node_ids=tuple(str(node_id) for node_id in input_node_ids),
            status="completed",
            n_edges_emitted=int(getattr(result, "n_edges_emitted", 0) or 0),
            n_edges_skipped=int(getattr(result, "n_edges_skipped", 0) or 0),
            edges_applied=int(getattr(apply_result, "edges_upserted", 0) or 0),
            duration_ms=duration_ms,
        )
    except (
        EnricherBackfillAdmissionRejected,
        EnricherBackfillChunkFailed,
        EnricherBackfillUnsupportedOutput,
    ):
        conn.rollback()
        raise
    except Exception as exc:  # noqa: BLE001 - rollback keeps chunk retryable
        conn.rollback()
        raise EnricherBackfillChunkFailed(
            "targeted enricher backfill chunk failed",
            data={
                "deployment": deployment,
                "enricher_name": enricher_name,
                "chunk_index": chunk_index,
                "input_node_ids": list(input_node_ids),
                "error": repr(exc),
            },
            fix_hint="fix the chunk failure and retry",
        ) from exc


def _admit_chunk_outputs(
    publisher: Publisher,
    conn: psycopg.Connection,
    *,
    enricher: Any,
    generation_id: int,
    catalog_version_id: int,
    edge_before: int,
    failure_reason: str | None,
) -> list[dict[str, Any]]:
    events_before = len(publisher._derived_verdict_events)
    publisher._admit_enricher_outputs(
        conn,
        enricher=enricher,
        generation_id=generation_id,
        catalog_version_id=catalog_version_id,
        prev_edge_fact_watermark=edge_before,
        failure_reason=failure_reason,
        profile_already_materialized=True,
    )
    return [
        dict(event)
        for event in publisher._derived_verdict_events[events_before:]
    ]


def _selected_source_filter(source_prefix: str) -> tuple[str, str]:
    return source_prefix, f"{source_prefix}:%"


def _selected_node_fact_count(
    conn: psycopg.Connection,
    *,
    source_prefix: str,
    min_node_fact_id: int,
) -> int:
    exact_source, family_prefix = _selected_source_filter(source_prefix)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.node_facts
             WHERE fact_id > %s
               AND (source = %s OR source LIKE %s)
            """,
            (min_node_fact_id, exact_source, family_prefix),
        )
        return int(cur.fetchone()[0] or 0)


def _unexpected_fact_sources(
    conn: psycopg.Connection,
    *,
    source_prefix: str,
    min_node_fact_id: int,
    min_edge_fact_id: int,
) -> list[str]:
    exact_source, family_prefix = _selected_source_filter(source_prefix)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT source
              FROM okg.node_facts
             WHERE fact_id > %s
               AND NOT (source = %s OR source LIKE %s)
            UNION
            SELECT DISTINCT source
              FROM okg.edge_facts
             WHERE fact_id > %s
               AND NOT (source = %s OR source LIKE %s)
             ORDER BY source
            """,
            (
                min_node_fact_id,
                exact_source,
                family_prefix,
                min_edge_fact_id,
                exact_source,
                family_prefix,
            ),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _load_selected_derived_edge_facts(
    conn: psycopg.Connection,
    *,
    min_edge_fact_id: int,
    source_prefix: str,
) -> list[EdgeFact]:
    exact_source, family_prefix = _selected_source_filter(source_prefix)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (ef.src, ef.edge_type, ef.dst)
                   ef.src, ef.dst, ef.edge_type, ef.attrs, ef.provenance,
                   ef.confidence, ef.source_record_id, ef.source_revision,
                   ef.op
              FROM okg.edge_facts ef
             WHERE ef.fact_id > %s
               AND (ef.source = %s OR ef.source LIKE %s)
               AND EXISTS (
                 SELECT 1
                   FROM okg.admission_envelopes e
                   JOIN okg.admission_record_refs r
                     ON r.admission_id = e.admission_id
                  WHERE r.ref_kind = 'edge_fact_range'
                    AND r.source = ef.source
                    AND r.run_id::text = ef.run_id::text
                    AND ef.fact_id BETWEEN r.fact_id_min AND r.fact_id_max
                    AND e.status IN ('accepted', 'published')
               )
             ORDER BY ef.src, ef.edge_type, ef.dst,
                      ef.observed_at DESC, ef.fact_id DESC
            """,
            (min_edge_fact_id, exact_source, family_prefix),
        )
        rows = cur.fetchall()
    return [
        EdgeFact(
            src=str(src),
            dst=str(dst),
            edge_type=str(edge_type),
            attrs=dict(attrs or {}),
            provenance=str(provenance),
            confidence=float(confidence),
            source_record_id=dict(source_record_id or {}),
            source_revision=dict(source_revision or {}),
            op=str(op),
        )
        for (
            src,
            dst,
            edge_type,
            attrs,
            provenance,
            confidence,
            source_record_id,
            source_revision,
            op,
        ) in rows
    ]


def _emitted_edge_counts_by_input(
    derived_facts: Sequence[EdgeFact],
    *,
    input_node_ids: Sequence[str],
) -> dict[str, int]:
    counts = {str(node_id): 0 for node_id in input_node_ids}
    for fact in derived_facts:
        src = str(fact.src)
        dst = str(fact.dst)
        if src in counts:
            counts[src] += 1
        elif dst in counts:
            counts[dst] += 1
    return counts


def _validate_chunk_edge_scope(
    facts: Sequence[EdgeFact],
    *,
    deployment: str,
    enricher_name: str,
    chunk_index: int,
    input_node_ids: Sequence[str],
) -> None:
    chunk = {str(node_id) for node_id in input_node_ids}
    out_of_scope = [
        {
            "src": fact.src,
            "edge_type": fact.edge_type,
            "dst": fact.dst,
        }
        for fact in facts
        if str(fact.src) not in chunk and str(fact.dst) not in chunk
    ]
    if not out_of_scope:
        return
    raise EnricherBackfillChunkFailed(
        "targeted enricher backfill emitted out-of-chunk edge facts",
        data={
            "deployment": deployment,
            "enricher_name": enricher_name,
            "chunk_index": chunk_index,
            "input_node_ids": list(input_node_ids),
            "out_of_scope_edge_count": len(out_of_scope),
            "out_of_scope_edges": out_of_scope[:20],
        },
        fix_hint=(
            "fix the enricher to emit only edge facts incident to "
            "incremental.dirty_node_ids before marking it chunk-safe"
        ),
    )


def _mark_inputs_complete(
    conn: psycopg.Connection,
    *,
    deployment: str,
    branch_name: str,
    enricher_name: str,
    input_node_ids: Sequence[str],
    catalog_version_id: int,
    profile_fingerprint: str,
    emitted_edge_counts_by_input: dict[str, int],
    run_id: str,
    metadata: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO okg.enricher_backfill_progress
              (deployment_name, branch_name, enricher_name, input_node_id,
               catalog_version_id, profile_fingerprint, transform_checksum,
               status, attempts, emitted_edge_count, last_run_id, metadata,
               completed_at, updated_at)
            VALUES
              (%(deployment)s, %(branch_name)s, %(enricher_name)s,
               %(input_node_id)s, %(catalog_version_id)s,
               %(profile_fingerprint)s, %(transform_checksum)s,
               'complete', 1, %(emitted_edge_count)s, %(last_run_id)s::uuid,
               %(metadata)s, now(), now())
            ON CONFLICT (
              deployment_name, branch_name, enricher_name, input_node_id,
              catalog_version_id, profile_fingerprint, transform_checksum
            )
            DO UPDATE SET
              attempts = okg.enricher_backfill_progress.attempts + 1,
              emitted_edge_count = EXCLUDED.emitted_edge_count,
              last_run_id = EXCLUDED.last_run_id,
              metadata = EXCLUDED.metadata,
              completed_at = EXCLUDED.completed_at,
              updated_at = EXCLUDED.updated_at,
              status = 'complete'
            """,
            [
                {
                    "deployment": deployment,
                    "branch_name": branch_name,
                    "enricher_name": enricher_name,
                    "input_node_id": str(input_node_id),
                    "catalog_version_id": catalog_version_id,
                    "profile_fingerprint": profile_fingerprint,
                    "transform_checksum": _ENRICHMENT_CHECKSUM,
                    "emitted_edge_count": int(
                        emitted_edge_counts_by_input.get(str(input_node_id), 0)
                    ),
                    "last_run_id": run_id,
                    "metadata": Jsonb(dict(metadata)),
                }
                for input_node_id in input_node_ids
            ],
        )


def _chunks(items: Sequence[str], chunk_size: int) -> Iterable[tuple[str, ...]]:
    for start in range(0, len(items), chunk_size):
        yield tuple(items[start:start + chunk_size])
