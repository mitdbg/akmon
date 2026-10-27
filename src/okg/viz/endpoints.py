"""Read-only HTTP endpoints for the OKG viz (substrate-native).

Every handler is a plain ``def`` (not ``async def``) so FastAPI
runs it in its threadpool. Each handler borrows a connection
from the `VizContext` pool, runs SQL pinned to the session's
generation, and returns a JSON or msgpack envelope.

No `Backbone`, no `OKGGraph`, no in-memory iteration. The data
layer is a Chronos checkout pinned to the session's generation
(`okg.graph_nodes` / `okg.graph_edges` via `reads._graph`), plus
the catalog tables (`subtypes`, `narrowings`).

Some advanced features from the legacy viz (filter predicate
AST, time-axis attr binding, BM25-ranked expand, trace overlay,
matrix view, time axis) are intentionally dropped or stubbed
in this initial substrate port. Each is tractable to bring back
as its own substrate-native follow-up proposal driven by real
need.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

import msgpack
from fastapi import APIRouter, Body, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field
from psycopg import sql as pg_sql
from okg.substrate.file_capability import resolve_within_approved_root
from okg.viz import reads
from okg.viz import reads_system
from okg.viz import workflow_projection
from okg.viz.context import VizContext
from okg.substrate.console_views.served_text import (
    console_failure_detail,
    log_console_failure,
)
from okg.viz.router_groups import (
    register_console_routes,
    register_graph_routes,
    register_ontology_overview_routes,
    register_portal_lens_routes,
    register_portal_routes,
    register_publication_routes,
    register_system_routes,
    register_trace_routes,
)
from okg.viz.record_endpoints import register_operator_record_routes

import psycopg

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.errors import SubstrateError
from okg.substrate.searchable_text import without_producer_text
from okg.substrate.mcp.session import open_session
from okg.substrate.unknown import to_wire, wire_callable

logger = logging.getLogger(__name__)

_SLOW_SEARCH_TIER_TIMEOUT_MS = 750
_LARGE_GRAPH_EDGE_SCAN_THRESHOLD = 1_000_000
_SUBTYPE_COUNTS_CACHE: dict[tuple[str, int], dict[str, int]] = {}
_UNLABELED_TRACE_SENTINEL = "<unlabeled>"
_QUERY_TRACE_LIMIT = 100


def _query_trace_id(session_id: str, question_label: str | None) -> str:
    """Stable public identity for one query slice.

    A process session can contain several explicit question-label slices.  The
    digest keeps the label out of the visible id while preventing React/URL
    identity from collapsing those slices back into the process session.
    """
    discriminator = question_label or _UNLABELED_TRACE_SENTINEL
    digest = hashlib.sha256(
        f"{session_id}\0{discriminator}".encode("utf-8")
    ).hexdigest()[:12]
    return f"{session_id}:{digest}"


def _query_trace_summary(
    question_label: str | None,
    tool_name: str | None,
    request_json: Any,
) -> tuple[str, str]:
    """Return a truthful human label and its provenance.

    Explicit question labels win.  Legacy/current clients record no label, so
    the fallback is derived only from the first meaningful *recorded request*;
    it is never presented as the user's original question.
    """
    if isinstance(question_label, str) and question_label.strip():
        return question_label.strip()[:160], "explicit"

    request = request_json if isinstance(request_json, dict) else {}
    kwargs = request.get("kwargs")
    kwargs = kwargs if isinstance(kwargs, dict) else {}
    tool = str(tool_name or "query").strip() or "query"

    def text(key: str, limit: int = 120) -> str | None:
        value = kwargs.get(key)
        if not isinstance(value, str):
            return None
        value = " ".join(value.split())
        if not value:
            return None
        return value if len(value) <= limit else f"{value[:limit - 1].rstrip()}…"

    query = text("query")
    if query:
        return f"Search · {query}", "derived"
    subtype = text("subtype_filter")
    if subtype:
        return f"Search {subtype.replace('_', ' ')} records", "derived"
    target = kwargs.get("target")
    if isinstance(target, dict):
        kind = str(target.get("kind") or "object").replace("_", " ")
        identity = (
            target.get("node_id")
            or target.get("name")
            or target.get("subtype")
        )
        if identity:
            return f"Inspect {kind} · {identity}", "derived"
        return f"Inspect {kind}", "derived"
    node_id = text("node_id")
    if node_id:
        verb = "Expand" if tool == "expand" else "Inspect"
        return f"{verb} node · {node_id}", "derived"
    method = text("method")
    if tool == "search" and method:
        return f"{method.title()} search", "derived"
    return f"{tool.replace('_', ' ').title()} run", "derived"



def _set_statement_timeout(
    conn: psycopg.Connection, timeout_ms: int,
) -> None:
    """Set a session timeout on a viz pool connection.

    Viz uses autocommit pool connections, so SET LOCAL is ineffective.
    Callers must reset this before returning the connection to the pool.
    """
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {int(timeout_ms)}")


def _reset_statement_timeout(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")


def _count_by_subtype_cached(
    conn: psycopg.Connection, ctx: VizContext,
) -> dict[str, int]:
    key = (ctx.deployment_name, ctx.session.generation_id)
    cached = _SUBTYPE_COUNTS_CACHE.get(key)
    if cached is not None:
        return dict(cached)
    counts = reads.count_by_subtype(conn, ctx.session)
    _SUBTYPE_COUNTS_CACHE[key] = dict(counts)
    return counts


def _estimated_reltuples(
    conn: psycopg.Connection,
    relation: str,
    *,
    preferred_index: str | None = None,
) -> int:
    candidates = [c for c in (preferred_index, relation) if c]
    with conn.cursor() as cur:
        for candidate in candidates:
            cur.execute(
                "SELECT COALESCE(reltuples, 0)::bigint "
                "FROM pg_class "
                "WHERE oid = to_regclass(%s)",
                (candidate,),
            )
            row = cur.fetchone()
            if row is not None:
                return int(row[0] or 0)
    return 0


# task.console-view-perf: the first-node-per-subtype sample. The old read
# looped over the ~94 live subtypes, issuing a separate single-row
# `reads.iter_nodes(..., limit=1, order_by_id=True)` per subtype — ~94
# Chronos checkouts/round-trips, the dominant cost (~80-90s warm) of the two
# ontology endpoints. One `DISTINCT ON (subtype)` over the pinned
# `okg.graph_nodes` returns the same {subtype: first-node-by-id} map in a
# single checkout. `ORDER BY subtype, node_id` makes the per-subtype pick the
# first node id, byte-identical to the loop's `ORDER BY node_id LIMIT 1`.
_SAMPLE_BY_SUBTYPE_SQL = """
SELECT DISTINCT ON (subtype) subtype, node_id
FROM okg.graph_nodes
WHERE subtype = ANY(:live)
ORDER BY subtype, node_id
"""


def _sample_by_subtype(
    conn: psycopg.Connection,
    ctx: VizContext,
    subtypes: list[str],
    counts: dict[str, int],
) -> dict[str, str]:
    live_subtypes = [st for st in subtypes if counts.get(st, 0) > 0]
    if not live_subtypes:
        return {}
    graph = reads._graph(conn, ctx.session)
    rows = graph.query(_SAMPLE_BY_SUBTYPE_SQL, {"live": live_subtypes})
    return {str(row["subtype"]): str(row["node_id"]) for row in rows}


# Per `add-view-edit-source-recheck` — POST body for the
# /views/<view_id>/recheck endpoint. Defined at module scope
# (rather than inside `build_router()`) because FastAPI's
# pydantic forward-reference resolution looks in module globals,
# not the local closure.
class RecheckRequest(BaseModel):
    fragment_id: str
    original_md: str
    edited_md: str


class PactReviewBody(BaseModel):
    change_id: str
    decision: str = "approved"  # 'approved' | 'changes-requested'
    notes: str | None = None
    reviewer: str | None = None


class LensPutbackDeltaRequest(BaseModel):
    slot_name: str
    scope_key: str
    row_key: str
    field_path: list[str]
    expected_value: Any
    new_value: Any
    source_authority: str
    source_record_id: dict[str, Any]
    expected_source_revision: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class LensPutbackRequestBody(BaseModel):
    view_id: str
    lens_id: str | None = None
    actor_id: str = "operator:viz"
    idempotency_key: str | None = None
    commit_overlay: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)
    deltas: list[LensPutbackDeltaRequest]


class LensOverlayRevokeBody(BaseModel):
    actor_id: str = "operator:viz"
    reason: str | None = None


# Hard caps. Same shape as the legacy viz so the frontend
# doesn't surprise itself.
_MAX_EXPAND_LIMIT = 10000
_MAX_SAMPLE_LIMIT = 500


def _view_id_from_node_id(node_id: str) -> str | None:
    if not node_id.startswith("view:"):
        return None
    view_id = node_id[len("view:"):]
    return view_id or None


def _approved_pdf_roots(ctx: VizContext) -> list[Path]:
    """Approved corpus roots for serving graph-controlled PDF paths, from the
    viz config key `approved_pdf_corpus_roots` (a path or list of paths;
    relative entries resolve against the deployment dir). Empty when unconfigured
    so pub_pdf fails closed (constrain-graph-file-and-parser-ingestion)."""
    raw = (ctx.config or {}).get("approved_pdf_corpus_roots") or []
    if isinstance(raw, str):
        raw = [raw]
    roots: list[Path] = []
    for entry in raw:
        if not entry:
            continue
        p = Path(str(entry)).expanduser()
        if not p.is_absolute():
            p = ctx.deployment_dir / p
        roots.append(p)
    return roots


def _raise_native_view_http_error(
    exc: Exception,
    *,
    view_id: str,
    pin: int,
) -> None:
    from okg.substrate.derived_artifacts import (
        ViewArtifactAmbiguous,
        ViewArtifactReadError,
    )

    if isinstance(exc, ViewArtifactAmbiguous):
        raise HTTPException(
            status_code=409,
            detail={
                "reason": "ambiguous_native_view_definition",
                "view_id": view_id,
                "generation_id": pin,
                "definition_ids": exc.definition_ids,
            },
        ) from exc
    if isinstance(exc, ViewArtifactReadError):
        raise HTTPException(
            status_code=404,
            detail=(
                f"native view {view_id!r} has no completed artifact "
                f"at generation {pin}"
            ),
        ) from exc
    raise exc


def _read_viz_view_slot(conn: psycopg.Connection, **kwargs: Any):
    """Refuse governed view bytes on viz's unauthenticated payload routes."""

    from okg.substrate.derived_artifacts import read_view_slot

    read = read_view_slot(conn, **kwargs)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metrics ? 'read_asset_authority'
              FROM okg.derived_artifact_runs
             WHERE run_id = %s
            """,
            (read.artifact.run_id,),
        )
        row = cur.fetchone()
    if row is not None and row[0] is True:
        raise HTTPException(
            status_code=403,
            detail={
                "reason": "governed_native_view_principal_required",
                "view_id": read.artifact.view_id,
                "generation_id": read.artifact.target_generation_id,
                "hint": (
                    "use a governed read surface with an attested Nomos "
                    "audience and tenant"
                ),
            },
        )
    return read


def _raise_lens_putback_http_error(
    exc: Exception,
    *,
    view_id: str,
    pin: int,
) -> None:
    from okg.substrate.lenses import (
        LensDefinitionAmbiguous,
        LensDefinitionNotFound,
        LensPutbackError,
        LensPutbackStateError,
    )

    if isinstance(exc, LensDefinitionAmbiguous):
        raise HTTPException(
            status_code=409,
            detail={
                "reason": "ambiguous_lens_definition",
                "view_id": view_id,
                "generation_id": pin,
                "lens_ids": list(exc.lens_ids),
            },
        ) from exc
    if isinstance(exc, LensDefinitionNotFound):
        raise HTTPException(
            status_code=404,
            detail={
                "reason": "lens_definition_not_found",
                "view_id": view_id,
                "generation_id": pin,
            },
        ) from exc
    if isinstance(exc, LensPutbackStateError):
        raise HTTPException(
            status_code=409,
            detail={
                "reason": "lens_putback_state_error",
                "view_id": view_id,
                "generation_id": pin,
                "message": str(exc),
            },
        ) from exc
    if isinstance(exc, (LensPutbackError, ValueError)):
        raise HTTPException(
            status_code=422,
            detail={
                "reason": "lens_putback_rejected",
                "view_id": view_id,
                "generation_id": pin,
                "message": str(exc),
            },
        ) from exc
    raise exc


def _native_view_markdown_node(
    conn: psycopg.Connection,
    ctx: VizContext,
    *,
    view_id: str,
) -> dict[str, Any]:
    pin = ctx.compatibility_generation_id
    try:
        markdown = _read_viz_view_slot(
            conn,
            deployment_name=ctx.deployment_name,
            view_id=view_id,
            generation_id=pin,
            slot_name="markdown",
        )
    except Exception as exc:
        _raise_native_view_http_error(exc, view_id=view_id, pin=pin)
        raise AssertionError("unreachable")

    from okg.substrate.derived_artifacts.read_views import (
        current_registry_view_ids,
    )

    registry_ids = current_registry_view_ids(ctx.deployment_name)
    artifact = markdown.artifact
    markdown_body = str(markdown.slot.payload_value() or "")
    view_name = str(
        artifact.output_contract.get("view_name")
        or artifact.view_id
    )
    attrs = {
        "view_id": artifact.view_id,
        "view_name": view_name,
        "markdown_body": markdown_body,
        "markdown_format_version": "native-v1",
        "last_refreshed_generation": artifact.target_generation_id,
        "freshness": (
            "fresh"
            if artifact.run_status in ("completed", "carried_forward")
            else artifact.run_status
        ),
        "manifest_uri": (
            f"derived-artifact://manifest/{artifact.manifest_id}"
        ),
        "manifest_hash": artifact.manifest_hash,
        "retired": (
            None if registry_ids is None
            else artifact.view_id not in registry_ids
        ),
        "definition_id": artifact.definition_id,
        "required_slots": list(artifact.required_slots),
        "renderer_ref": markdown.slot.renderer_ref,
        "text": f"{view_name}\n{markdown_body}",
    }
    return {
        "node_id": f"view:{artifact.view_id}",
        "node_type": "view",
        "node_label": view_name,
        "attrs": attrs,
    }


def _ensure_node_or_native_view(
    conn: psycopg.Connection,
    ctx: VizContext,
    node_id: str,
) -> None:
    view_id = _view_id_from_node_id(node_id)
    if view_id is None:
        if not reads.has_node(conn, ctx.session, node_id):
            raise HTTPException(
                status_code=404, detail=f"Unknown node: {node_id!r}",
            )
        return

    from okg.substrate.derived_artifacts import (
        get_view_artifact_at_generation,
    )

    pin = ctx.compatibility_generation_id
    try:
        get_view_artifact_at_generation(
            conn,
            deployment_name=ctx.deployment_name,
            view_id=view_id,
            generation_id=pin,
        )
    except Exception as exc:
        _raise_native_view_http_error(exc, view_id=view_id, pin=pin)


def _native_view_summary(
    conn: psycopg.Connection,
    ctx: VizContext,
    node_id: str,
) -> dict[str, Any] | None:
    view_id = _view_id_from_node_id(node_id)
    if view_id is None:
        return None
    native = _native_view_markdown_node(conn, ctx, view_id=view_id)
    return {
        "id": native["node_id"],
        "type": native["node_type"],
        "label": native["node_label"],
    }


def _relation_preview_for_slot(
    conn: psycopg.Connection,
    slot,
    *,
    view_definition_id: str | None = None,
    view_target_generation_id: int | None = None,
    limit: int = 100,
    scope_key: str | None = None,
    row_key: str | None = None,
) -> dict[str, Any] | None:
    if slot.storage_kind != "relation":
        return None
    preview_limit = max(0, min(int(limit), _MAX_SAMPLE_LIMIT))
    storage_ref = dict(slot.payload_value() or {})
    schema_name = str(storage_ref.get("schema") or "")
    relation_name = str(storage_ref.get("relation") or "")
    definition_id = str(storage_ref.get("upstream_definition_id") or "")
    view_generation_id = (
        view_target_generation_id
        if view_target_generation_id is not None
        else storage_ref.get("target_generation_id")
    )
    # Resolve where the relation ROWS physically live. The two carry-forward
    # paths use opposite conventions for the slot's ``target_generation_id``
    # (relation carry-forward keeps it on the row gen; view carry-forward
    # rebinds it to the row-less new gen), so probe the candidate generations
    # and use whichever actually holds rows. Mirrors
    # ``derived_artifacts.read_views._inspect_relation_slot`` — keep the two in
    # sync so the inspect and viz preview paths agree. Fall back to the first
    # candidate so a genuinely empty relation reports an honest zero.
    relation_generation_candidates: list[int] = []
    for _candidate in (
        storage_ref.get("target_generation_id"),
        storage_ref.get("upstream_target_generation_id"),
        storage_ref.get("source_generation_id"),
    ):
        if _candidate is None:
            continue
        _candidate_int = int(_candidate)
        if _candidate_int not in relation_generation_candidates:
            relation_generation_candidates.append(_candidate_int)
    relation_generation_id = (
        relation_generation_candidates[0]
        if relation_generation_candidates
        else None
    )
    if (
        not schema_name
        or not relation_name
        or not definition_id
        or view_generation_id is None
        or relation_generation_id is None
    ):
        return {
            "available": False,
            "reason": "incomplete_relation_storage_ref",
            "rows": [],
        }
    with conn.cursor() as cur:
        for _candidate_int in relation_generation_candidates:
            cur.execute(
                pg_sql.SQL(
                    """
                    SELECT EXISTS (
                      SELECT 1
                        FROM {}.{}
                       WHERE definition_id = %(definition_id)s
                         AND target_generation_id = %(target_generation_id)s
                    )
                    """
                ).format(
                    pg_sql.Identifier(schema_name),
                    pg_sql.Identifier(relation_name),
                ),
                {
                    "definition_id": definition_id,
                    "target_generation_id": _candidate_int,
                },
            )
            if cur.fetchone()[0]:
                relation_generation_id = _candidate_int
                break
        filter_params = {
            "definition_id": definition_id,
            "target_generation_id": int(relation_generation_id),
            "scope_key": scope_key,
            "row_key": row_key,
        }
        cur.execute(
            pg_sql.SQL(
                """
                SELECT count(*)::bigint
                  FROM {}.{}
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(target_generation_id)s
                   AND (
                     %(scope_key)s::text IS NULL
                     OR scope_key = %(scope_key)s::text
                   )
                   AND (
                     %(row_key)s::text IS NULL
                     OR row_key = %(row_key)s::text
                   )
                """
            ).format(
                pg_sql.Identifier(schema_name),
                pg_sql.Identifier(relation_name),
            ),
            filter_params,
        )
        filtered_row_count = int(cur.fetchone()[0] or 0)
        cur.execute(
            pg_sql.SQL(
                """
                SELECT scope_key, row_key, row_data
                  FROM {}.{}
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(target_generation_id)s
                   AND (
                     %(scope_key)s::text IS NULL
                     OR scope_key = %(scope_key)s::text
                   )
                   AND (
                     %(row_key)s::text IS NULL
                     OR row_key = %(row_key)s::text
                   )
                 ORDER BY scope_key, row_key
                 LIMIT %(limit)s
                """
            ).format(
                pg_sql.Identifier(schema_name),
                pg_sql.Identifier(relation_name),
            ),
            {
                **filter_params,
                "limit": preview_limit,
            },
        )
        rows = [
            {
                "scope_key": row[0],
                "row_key": row[1],
                "row_data": row[2],
            }
            for row in cur.fetchall()
        ]
        if rows:
            row_keys = [str(row["row_key"]) for row in rows]
            cur.execute(
                """
                SELECT anchor->>'anchor' AS anchor,
                       observation_id, check_id, predicate_key,
                       normalized_predicate, severity, confidence,
                       finding_status, evaluation_status
                  FROM okg.observation_current oc
                  CROSS JOIN LATERAL jsonb_array_elements(
                    oc.participant_anchors
                  ) AS anchor
                 WHERE oc.finding_status = 'open'
                   AND oc.latest_generation_id <= %(generation_id)s
                   AND anchor->>'anchor' = ANY(%(row_keys)s)
                 ORDER BY severity NULLS LAST, check_id, predicate_key
                """,
                {
                    "generation_id": int(view_generation_id),
                    "row_keys": row_keys,
                },
            )
            observations_by_anchor: dict[str, list[dict[str, Any]]] = {}
            for obs in cur.fetchall():
                observations_by_anchor.setdefault(str(obs[0]), []).append(
                    {
                        "observation_id": obs[1],
                        "check_id": obs[2],
                        "predicate_key": obs[3],
                        "normalized_predicate": obs[4],
                        "severity": obs[5],
                        "confidence": (
                            str(obs[6]) if obs[6] is not None else None
                        ),
                        "finding_status": obs[7],
                        "evaluation_status": obs[8],
                    }
                )
            for row in rows:
                observations = observations_by_anchor.get(
                    str(row["row_key"]),
                    [],
                )
                if observations:
                    row["observations"] = observations
            _attach_lens_overlays_to_relation_rows(
                cur,
                rows,
                view_definition_id=view_definition_id,
                target_generation_id=int(view_generation_id),
                slot_name=slot.slot_name,
            )
    return {
        "available": True,
        "rows": rows,
        "row_count": filtered_row_count,
        "slot_row_count": slot.row_count,
        "limit": preview_limit,
        "scope_key": scope_key,
        "row_key": row_key,
        "truncated": filtered_row_count > len(rows),
        "target_generation_id": view_generation_id,
        "view_target_generation_id": view_generation_id,
        "relation_target_generation_id": relation_generation_id,
    }


def _attach_lens_overlays_to_relation_rows(
    cur: psycopg.Cursor,
    rows: list[dict[str, Any]],
    *,
    view_definition_id: str | None,
    target_generation_id: int,
    slot_name: str,
) -> None:
    from okg.substrate.lenses import applicable_lens_overlays_for_row
    from okg.substrate.lenses import load_lens_overlays_for_rows

    row_identities = [
        (str(row["scope_key"]), str(row["row_key"]))
        for row in rows
    ]
    overlays_by_row = load_lens_overlays_for_rows(
        cur,
        view_definition_id=view_definition_id,
        target_generation_id=target_generation_id,
        slot_name=slot_name,
        row_identities=row_identities,
    )
    for row in rows:
        overlays = overlays_by_row.get(
            (str(row["scope_key"]), str(row["row_key"])),
            [],
        )
        row_data = row.get("row_data")
        applicable = applicable_lens_overlays_for_row(
            row_data if isinstance(row_data, dict) else {},
            overlays,
        )
        if applicable:
            row["lens_overlays"] = applicable


def _looks_like_identifier_query(q: str) -> bool:
    """Return true for pasted IDs/pub numbers where fuzzy BM25 is risky."""
    s = (q or "").strip()
    if not s:
        return False
    if any(ch in s for ch in (":", "_", "/", "\\")):
        return True
    has_digit = any(ch.isdigit() for ch in s)
    has_id_punct = any(ch in s for ch in ("-", "."))
    alpha = [ch for ch in s if ch.isalpha()]
    mostly_upper = bool(alpha) and sum(
        ch.isupper() for ch in alpha
    ) >= max(1, len(alpha) - 1)
    return has_digit and (has_id_punct or mostly_upper)


def _normalize_subtype_query(q: str) -> str:
    """Normalize a human-entered type-ish query to snake_case."""
    return " ".join((q or "").strip().lower().split()).replace(" ", "_").replace("-", "_")


def _collapse_ws(value: str) -> str:
    return " ".join(value.split())


def _snippet_for_query(text: Any, query: str, *, max_len: int = 180) -> str | None:
    """Return a compact snippet around the first query occurrence."""
    if not isinstance(text, str):
        return None
    clean = _collapse_ws(text)
    if not clean:
        return None
    q = (query or "").strip().lower()
    lower = clean.lower()
    idx = lower.find(q) if q else -1
    if idx < 0:
        return clean[:max_len] + ("..." if len(clean) > max_len else "")
    half = max(24, max_len // 2)
    start = max(0, idx - half)
    end = min(len(clean), idx + len(q) + half)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(clean) else ""
    return f"{prefix}{clean[start:end]}{suffix}"


def _label_from_attrs(node_id: str, subtype: str, attrs: Any, query: str = "") -> str:
    attrs = attrs if isinstance(attrs, dict) else {}
    for key in ("label", "title", "name"):
        value = attrs.get(key)
        if isinstance(value, str) and value.strip():
            return _collapse_ws(value)
    heading = attrs.get("heading_path")
    if isinstance(heading, str) and heading.strip():
        return _collapse_ws(heading).split(" / ")[-1]
    text = _snippet_for_query(attrs.get("text"), query, max_len=96)
    if text:
        return text
    return node_id or subtype


_READER_HEAVY_ATTRS = (
    "body_text",
    "body_text_md",
    "body",
    "text",
    "content",
    "markdown_body",
    "diff",
    "raw",
    "html",
    "embedding",
)

_READER_SPEC_DOC_SUBTYPES = (
    "document",
    "open_spec_change",
    "proposal_task",
    "spec_delta",
    "design_decision",
    "goal",
    "non_goal",
    "alternative",
    "risk",
)

_READER_EXCLUDED_PATH_RE = (
    r"(^|/)\.[^/]+(/|$)"
    r"|(^|/)(node_modules|test-results|html-report|__pycache__)(/|$)"
    r"|(^|/)[^/]*(secret|token|credential|cookie|password|private[_-]?key)[^/]*($|[./_-])"
)
_READER_MAX_LOCAL_TEXT_BYTES = 2_000_000


def _reader_label(node_id: str, subtype: str, attrs: Any) -> str:
    """Best-effort display label for reader catalog rows."""
    attrs = attrs if isinstance(attrs, dict) else {}
    path_value = None
    for key in ("path", "file_path"):
        value = attrs.get(key)
        if isinstance(value, str) and value.strip():
            path_value = value.strip()
            break
    if subtype in {"document", "source_file"} and path_value:
        for key in ("title", "label", "name"):
            value = attrs.get(key)
            if (
                isinstance(value, str)
                and value.strip()
                and value.strip() != path_value
            ):
                return _collapse_ws(value)
        return path_value.replace("\\", "/").rstrip("/").split("/")[-1]
    for key in (
        "view_name",
        "view_id",
        "change_id",
        "pub_number",
        "title",
        "label",
        "name",
    ):
        value = attrs.get(key)
        if isinstance(value, str) and value.strip():
            return _collapse_ws(value)
    return _label_from_attrs(node_id, subtype, attrs)


def _reader_subtitle(subtype: str, attrs: Any) -> str | None:
    attrs = attrs if isinstance(attrs, dict) else {}
    if subtype == "document":
        path_value = attrs.get("path") or attrs.get("file_path")
        parent = None
        if isinstance(path_value, str) and "/" in path_value:
            parent = path_value.replace("\\", "/").rsplit("/", 1)[0]
        fmt = attrs.get("format")
        bits = [str(v) for v in (parent, fmt) if v]
        return " · ".join(bits) or "document"
    if subtype == "view":
        freshness = attrs.get("freshness")
        gen = attrs.get("last_refreshed_generation")
        bits = []
        if freshness:
            bits.append(str(freshness))
        if gen is not None:
            bits.append(f"gen {gen}")
        return " · ".join(bits) or None
    if subtype == "source_file":
        for key in ("language", "mime_type", "source_adapter", "adapter"):
            value = attrs.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None
    if subtype == "publication":
        bits = []
        if attrs.get("status"):
            bits.append(str(attrs["status"]))
        if attrs.get("certified_date"):
            bits.append(str(attrs["certified_date"]))
        return " · ".join(bits) or None
    return subtype


def _reader_document_body_from_chunks(
    graph: Any,
    *,
    document_id: str,
) -> str | None:
    """Assemble a document body from its ordered document_chunk children.

    Order is the member_of edge's `chunk_ordinal` (the document's emission
    order), then the chunk's `char_offset`, then its id: a chunk whose
    producer reports no span stores no offset, and older graphs store 0 on
    every such chunk, so offsets alone scramble the body.

    Some source adapters keep the parent `document` node lightweight and
    store renderable text on contained `document_chunk` children. The
    reader is a read-side surface, so hydrate that shape here instead of
    requiring the frontend to graph-walk for a basic document open.
    """
    rows = graph.query(
        """
        SELECT child.attrs->>'text' AS text
          FROM okg.graph_edges AS edge
          JOIN okg.graph_nodes AS child
            ON child.node_id = edge.dst
          LEFT JOIN okg.graph_edges AS member
            ON member.src = child.node_id
           AND member.dst = edge.src
           AND member.edge_type = 'member_of'
         WHERE edge.src = :document_id
           AND edge.edge_type = 'contains'
           AND child.subtype = 'document_chunk'
           AND NULLIF(child.attrs->>'text', '') IS NOT NULL
         ORDER BY CASE
                    WHEN member.attrs->>'chunk_ordinal' ~ '^[0-9]+$'
                    THEN (member.attrs->>'chunk_ordinal')::bigint
                    ELSE 9223372036854775807
                  END,
                  CASE
                    WHEN child.attrs->>'char_offset' ~ '^[0-9]+$'
                    THEN (child.attrs->>'char_offset')::bigint
                    ELSE 9223372036854775807
                  END,
                  child.node_id
        """,
        {"document_id": document_id},
    )
    parts = [str(row["text"]).strip() for row in rows if row["text"]]
    return "\n\n".join(part for part in parts if part) or None


def _reader_document_body_from_local_file(attrs: Any) -> str | None:
    """Return file text when the local file still matches graph metadata."""
    attrs = attrs if isinstance(attrs, dict) else {}
    path_value = attrs.get("path") or attrs.get("file_path")
    if not isinstance(path_value, str) or not path_value.strip():
        return None
    normalized = path_value.replace("\\", "/").lstrip("/")
    if re.search(_READER_EXCLUDED_PATH_RE, normalized, re.IGNORECASE):
        return None
    # Shared containment primitive: the graph-controlled path must resolve
    # (symlinks followed) inside the CWD root, be a regular file, and be within
    # the text-size cap — the same boundary used by the pub_pdf read side.
    root = Path.cwd().resolve()
    target = resolve_within_approved_root(
        root / normalized, [root],
        must_be_file=True, max_bytes=_READER_MAX_LOCAL_TEXT_BYTES,
    )
    if target is None:
        return None
    try:
        data = target.read_bytes()
    except OSError:
        return None
    expected_sha = attrs.get("content_sha256")
    if isinstance(expected_sha, str) and expected_sha.strip():
        actual_sha = hashlib.sha256(data).hexdigest()
        if actual_sha != expected_sha.strip():
            return None
    return data.decode("utf-8", errors="replace")


def _reader_hydrate_document_attrs(
    graph: Any,
    *,
    node_id: str,
    attrs: Any,
) -> dict[str, Any]:
    attrs_dict = dict(attrs) if isinstance(attrs, dict) else {}
    body = attrs_dict.get("body") or attrs_dict.get("markdown_body")
    if isinstance(body, str) and body.strip():
        return attrs_dict
    local_body = _reader_document_body_from_local_file(attrs_dict)
    if local_body:
        attrs_dict["body"] = local_body
        attrs_dict["content_source"] = "local_file_sha256"
        return attrs_dict
    assembled = _reader_document_body_from_chunks(
        graph, document_id=node_id,
    )
    if not assembled:
        return attrs_dict
    attrs_dict["body"] = assembled
    attrs_dict["content_source"] = "document_chunks"
    return attrs_dict


def _reader_leaf_item(
    node_id: str,
    subtype: str,
    attrs: Any,
    *,
    source_family: str,
) -> dict[str, Any]:
    attrs = attrs if isinstance(attrs, dict) else {}
    label = _reader_label(node_id, subtype, attrs)
    title = attrs.get("title") or attrs.get("path") or attrs.get("file_path")
    item = {
        "id": node_id,
        "node_id": node_id,
        "node_type": subtype,
        "label": label,
        "title": title,
        "subtitle": _reader_subtitle(subtype, attrs),
        "source_family": source_family,
        "leaf": True,
    }
    for key in (
        "view_id",
        "freshness",
        "last_refreshed_generation",
        "manifest_path",
        "path",
        "file_path",
        "change_id",
        "pub_number",
        "status",
        "certified_date",
    ):
        if attrs.get(key) not in (None, ""):
            item[key] = attrs[key]
    return item


def _search_item(
    node_id: str,
    subtype: str,
    attrs: Any,
    query: str,
    *,
    match_field: str,
    match_tier: str,
) -> dict[str, Any]:
    attrs = attrs if isinstance(attrs, dict) else {}
    snippet = None
    if match_field in {"text", "heading_path"}:
        snippet = _snippet_for_query(attrs.get(match_field), query)
    if snippet is None and isinstance(attrs.get("text"), str) and subtype == "document_chunk":
        snippet = _snippet_for_query(attrs.get("text"), query)
    context = attrs.get("heading_path") if isinstance(attrs.get("heading_path"), str) else None
    return {
        "id": node_id,
        "type": subtype,
        "label": _label_from_attrs(node_id, subtype, attrs, query),
        "degree": 0,
        "match_field": match_field,
        "match_tier": match_tier,
        "snippet": snippet,
        "context": _collapse_ws(context) if context else None,
    }


def _parse_type_filter(predicate_json: str | None) -> tuple[set[str], set[str] | None]:
    """Extract `(excluded_types, included_types_or_None)` from a
    sidebar-style predicate JSON. Supports `type_not_in`, `type_in`,
    and top-level `and`/`all_of` groups containing those terms. Anything else is
    silently ignored — the legacy predicate AST is not fully ported
    server-side; we only honour the type-filter terms the sidebar
    actually emits today."""
    if not predicate_json:
        return set(), None
    try:
        import json as _json
        pred = _json.loads(predicate_json)
    except Exception:
        return set(), None
    excluded: set[str] = set()
    included: set[str] | None = None
    if not isinstance(pred, dict):
        return set(), None

    def visit(t: Any) -> None:
        nonlocal included
        if not isinstance(t, dict):
            return
        if t.get("kind") in {"and", "all_of"}:
            for child in (t.get("terms") or []):
                visit(child)
            return
        if t.get("kind") == "type_not_in":
            for v in (t.get("values") or []):
                excluded.add(str(v))
        elif t.get("kind") == "type_in":
            included = included or set()
            for v in (t.get("values") or []):
                included.add(str(v))

    visit(pred)
    return excluded, included


def _envelope(
    *,
    nodes: list[dict[str, Any]] | None = None,
    edges: list[dict[str, Any]] | None = None,
    meta: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "nodes": nodes or [],
        "edges": edges or [],
        "meta": meta or {},
        **extra,
    }


class WireRoute(APIRoute):
    """The viz read exit: every route's return value passes ``to_wire``.

    An ``Unknown`` fact is served as ``null`` with its reason under
    ``unknown_reasons`` (issue #2573). A route that builds its own
    ``Response`` is passed through unchanged; the two helpers below that
    build read responses by hand call ``to_wire`` themselves.
    """

    def __init__(self, path: str, endpoint: Callable[..., Any], **kwargs: Any) -> None:
        super().__init__(path, wire_callable(endpoint), **kwargs)


def _msgpack_response(payload: dict[str, Any]) -> Response:
    return Response(
        content=msgpack.packb(to_wire(payload), use_bin_type=True),
        media_type="application/msgpack",
    )


def _envelope_response(
    payload: dict[str, Any],
    fmt: str | None,
) -> Response:
    """Return msgpack by default, JSON when `fmt == "json"`. Used by
    `/expand` and `/khop` so browser consumers without a msgpack
    decoder can opt into JSON via `?format=json`. The default stays
    msgpack to keep the existing viz frontend wire format unchanged."""
    if fmt == "json":
        import json as _json
        return Response(
            content=_json.dumps(to_wire(payload), default=str),
            media_type="application/json",
        )
    return _msgpack_response(payload)


def _node_to_dict(n: reads.NodeRow) -> dict[str, Any]:
    return {
        "id": n.id,
        "type": n.type,
        "label": n.label or n.id,
        "attrs": n.attrs,
    }


def _numeric_or_lex(v: Any) -> tuple[int, float | str]:
    """Sort key that puts numeric values first (in numeric order),
    then string values. Used to order absorbed children by `seq`."""
    if v is None:
        return (2, "")
    try:
        return (0, float(v))
    except (TypeError, ValueError):
        return (1, str(v))


def _pack_children_inside_parent(
    children: list[dict[str, Any]],
    sequence_attr: str | None,
    parent_radius: float = 100.0,
) -> None:
    """Compute deterministic positions for absorbed children inside
    their parent. Mutates each child dict to add `viz_x` / `viz_y`
    relative to the parent's CENTER (i.e. parent_x and parent_y at
    render time get added on top).

    Sequence-aware path: when `sequence_attr` is set and every child
    has a parseable value at `child["seq"]`, sort by it and lay
    out as a grid (K columns × M rows). Sequence reads top-left
    to bottom-right.

    Unordered fallback: hexagonal packing.
    (add-compound-graph-with-semantic-zoom §3)
    """
    n = len(children)
    if n == 0:
        return

    # Try the sequence-aware path first.
    if sequence_attr:
        # Split into sequenced (with valid seq) and unsequenced.
        sequenced = []
        unsequenced = []
        for c in children:
            v = c.get("seq")
            if v is None:
                unsequenced.append(c)
                continue
            try:
                sequenced.append((float(v), c))
            except (TypeError, ValueError):
                # Lexical fallback if numeric coercion fails.
                sequenced.append((str(v), c))
        if sequenced:
            sequenced.sort(key=lambda x: x[0])
            ordered = [c for _, c in sequenced] + unsequenced
            _grid_layout(ordered, parent_radius)
            return

    # Hex pack for unordered children.
    _hex_pack(children, parent_radius)


def _grid_layout(children: list[dict[str, Any]], parent_radius: float) -> None:
    """Lay out children in a chunked grid that fits inside the
    parent's bounding circle. Mutates each child to set viz_x / viz_y."""
    import math
    n = len(children)
    if n == 0:
        return
    # Square-ish: K = ceil(sqrt(N)), M = ceil(N/K)
    k = max(1, int(math.ceil(math.sqrt(n))))
    m = max(1, int(math.ceil(n / k)))
    # Cell size: fit (k × m) cells inside a square of side
    # 2 * parent_radius * 0.85 (leave 15% padding).
    side = parent_radius * 2 * 0.85
    cell_w = side / k
    cell_h = side / m
    # Origin at top-left of grid; relative to parent CENTER, so
    # offset by -side/2.
    origin_x = -side / 2 + cell_w / 2
    origin_y = -side / 2 + cell_h / 2
    for i, c in enumerate(children):
        col = i % k
        row = i // k
        c["viz_x"] = origin_x + col * cell_w
        c["viz_y"] = origin_y + row * cell_h


def _hex_pack(children: list[dict[str, Any]], parent_radius: float) -> None:
    """Hexagonal packing inside the parent's bounding circle. Mutates
    each child to set viz_x / viz_y."""
    import math
    n = len(children)
    if n == 0:
        return
    # Hex packing: rings of 1, 6, 12, 18, ... around a center.
    # Ring r holds 6r nodes (r >= 1); ring 0 holds 1.
    # Find smallest R such that 1 + 6 * R*(R+1)/2 >= n
    # = 1 + 3R(R+1) >= n
    if n == 1:
        children[0]["viz_x"] = 0.0
        children[0]["viz_y"] = 0.0
        return
    def capacity(r: int) -> int:
        return 1 + 3 * r * (r + 1)

    rings = 1
    while capacity(rings) < n:
        rings += 1
    # Hex spacing: distance between adjacent hex centers.
    # Total radius needed = rings * spacing. Solve for spacing.
    spacing = (parent_radius * 0.85) / max(1, rings)
    # Place ring 0.
    placed = 0
    children[placed]["viz_x"] = 0.0
    children[placed]["viz_y"] = 0.0
    placed += 1
    # For each ring, place 6 * ring nodes evenly around.
    for r in range(1, rings + 1):
        nodes_in_ring = 6 * r
        for i in range(nodes_in_ring):
            if placed >= n:
                return
            theta = 2 * math.pi * i / nodes_in_ring
            children[placed]["viz_x"] = r * spacing * math.cos(theta)
            children[placed]["viz_y"] = r * spacing * math.sin(theta)
            placed += 1


def _aggregated_edges_for_absorbed(
    ctx: VizContext,
    absorbed: dict[str, str],
    contains_archs: set[str],
) -> list[dict[str, Any]]:
    """Compute aggregated edges between parent-level nodes.

    For each *reference*-archetype edge between two instances,
    resolve each endpoint to its absorbed-parent (or the endpoint
    itself if not absorbed). Group by (src_parent, dst_parent,
    edge_type) and count. Returns rows ready to be JSON-encoded
    into `aggregated_edges` in the tree response.
    (add-compound-graph-with-semantic-zoom §2)
    """
    if not absorbed or not contains_archs:
        return []

    absorbed_subtypes = list(absorbed.keys())
    contains_arch_list = list(contains_archs)

    sql = """
        WITH live_n AS (
          SELECT node_id, subtype
            FROM okg.graph_nodes
        ),
        live_e AS (
          SELECT src, dst, edge_type
            FROM okg.graph_edges
        ),
        absorbed_map AS (
          -- Map every absorbed-child node to its parent-level node
          -- via the contains-edge from its parent.
          SELECT cn.node_id  AS child_id,
                 pn.node_id  AS parent_id
            FROM live_n cn
            JOIN live_e ce ON ce.dst = cn.node_id
                            AND ce.edge_type = ANY(:archs)
            JOIN live_n pn ON pn.node_id = ce.src
           WHERE cn.subtype = ANY(:subs)
        ),
        resolve AS (
          -- For every reference edge, swap each endpoint for its
          -- absorbed-parent if absorbed; else keep.
          SELECT
            COALESCE(am_src.parent_id, e.src)  AS src_parent,
            COALESCE(am_dst.parent_id, e.dst)  AS dst_parent,
            e.edge_type
          FROM live_e e
          LEFT JOIN absorbed_map am_src ON am_src.child_id = e.src
          LEFT JOIN absorbed_map am_dst ON am_dst.child_id = e.dst
          WHERE NOT (e.edge_type = ANY(:archs))
        )
        SELECT src_parent, dst_parent, edge_type, count(*) AS n
          FROM resolve
         WHERE src_parent <> dst_parent
      GROUP BY src_parent, dst_parent, edge_type
      ORDER BY n DESC
         LIMIT 5000
    """

    with ctx.conn() as conn:
        rows = reads._graph(conn, ctx.session).query(
            sql,
            {
                "archs": contains_arch_list,
                "subs": absorbed_subtypes,
            },
        )

    return [
        {
            "src": r["src_parent"],
            "dst": r["dst_parent"],
            "edge_archetype": r["edge_type"],
            "count": r["n"],
        }
        for r in rows
    ]


def _hierarchy_tree_grouped(
    ctx: VizContext,
    hierarchy: str,
    max_nodes: int,
    sequence_attrs: dict[str, str],
) -> Response:
    """Build a 2-level hierarchy tree under a non-containment grouping.
    `hierarchy` is either `subtype` or `attribute:<attr_name>`.

    Output structure:
      depth 0 = bucket for each unique value of the grouping key
      depth 1 = every node whose value matches that bucket
    """
    nodes: list[dict[str, Any]] = []
    capped = False

    if hierarchy == "subtype":
        group_sql = "subtype"
        group_label = "subtype"
    elif hierarchy.startswith("attribute:"):
        attr_name = hierarchy[len("attribute:"):].strip()
        if not attr_name:
            raise HTTPException(status_code=400, detail="attribute: requires a name")
        group_sql = f"COALESCE(attrs->>'{attr_name}', '(none)')"
        group_label = attr_name
    else:
        raise HTTPException(status_code=400, detail=f"unknown hierarchy: {hierarchy}")

    with ctx.conn() as conn:
        graph = reads._graph(conn, ctx.session)
        # Bucket counts for depth-0 group buckets.
        bucket_rows = graph.query(
            f"""
            SELECT {group_sql} AS bucket, count(*) AS n
              FROM okg.graph_nodes
          GROUP BY bucket
          ORDER BY n DESC
            """,
        )
        for row in bucket_rows:
            bucket, n = row["bucket"], row["n"]
            bucket_id = f"_grp:{group_label}:{bucket}"
            nodes.append({
                "id":      bucket_id,
                "label":   f"{bucket} ({n})",
                "parent":  None,
                "depth":   0,
                "count":   n,
                "subtype": "__meta__",
                "seq":     None,
            })

        # Depth 1: every node, parent = its group bucket id.
        node_rows = graph.query(
            f"""
            SELECT node_id, subtype,
                   (attrs - ARRAY[
                     'body_text','body_text_md','body','text',
                     'pdf_url','local_pdf_path','pdf_bytes',
                     'raw','content','html'
                   ])::jsonb AS attrs,
                   COALESCE(
                             attrs->>'label',
                             attrs->>'title',
                             attrs->>'name',
                             attrs->>'subject',
                             attrs->>'summary',
                             node_id
                           ) AS lbl,
                   {group_sql} AS bucket
              FROM okg.graph_nodes
          ORDER BY bucket, lbl
            LIMIT :max_nodes
            """,
            {"max_nodes": max_nodes},
        )
        for row in node_rows:
            nid, sub, attrs, lbl, bucket = (
                row["node_id"], row["subtype"], row["attrs"],
                row["lbl"], row["bucket"],
            )
            if len(nodes) >= max_nodes:
                capped = True
                break
            seq_attr = sequence_attrs.get(sub)
            seq_val = attrs.get(seq_attr) if (seq_attr and isinstance(attrs, dict)) else None
            nodes.append({
                "id":      nid,
                "label":   lbl,
                "parent":  f"_grp:{group_label}:{bucket}",
                "depth":   1,
                "count":   1,
                "subtype": sub,
                "seq":     seq_val,
            })

    return _msgpack_response(_envelope(
        nodes=nodes,
        edges=[],
        meta={
            "request_kind":  "hierarchy_tree",
            "hierarchy":     hierarchy,
            "fetched_depth": 1,
            "total_nodes":   len(nodes),
            "capped":        capped,
            "max_nodes":     max_nodes,
            "generation_id": ctx.session.generation_id,
        },
    ))


def _hierarchy_tree_nested(
    ctx: VizContext,
    keys: list[str],
    max_nodes: int,
    sequence_attrs: dict[str, str],
) -> Response:
    """Multi-level grouping. Each key is either `subtype` or an
    attribute name. Buckets at each level; leaves are graph nodes.

    Behavior tuned for af_pubs's "Pubs (OPR/Series) + People + Views":
      - First key supports `subtype=v1|v2|v3` syntax to filter the
        leaf set to specific subtypes.
      - Missing attribute values collapse the level (the leaf rolls
        up to the nearest non-(none) ancestor). So `nested:subtype,
        org_id,series_number` with subtypes person/view (which lack
        those attrs) puts persons/views directly under their subtype
        bucket; publications get the full org→series breakdown.
    """
    if not keys:
        raise HTTPException(status_code=400, detail="nested: requires at least one key")

    # Parse first key for an optional `=v1|v2` filter.
    first_raw = keys[0]
    subtype_filter: list[str] | None = None
    if first_raw.startswith("subtype=") or first_raw == "subtype":
        if "=" in first_raw:
            subtype_filter = [v.strip() for v in first_raw.split("=", 1)[1].split("|") if v.strip()]
            keys = ["subtype"] + keys[1:]

    def expr(k: str) -> str:
        if k == "subtype":
            return "subtype"
        if not k.replace("_", "").isalnum():
            raise HTTPException(status_code=400, detail=f"bad nested key: {k}")
        return f"COALESCE(attrs->>'{k}', '')"

    select_cols = ", ".join(f"{expr(k)} AS k{i}" for i, k in enumerate(keys))
    nodes: list[dict[str, Any]] = []
    capped = False

    where: list[str] = []
    params: dict[str, Any] = {"max_nodes": max_nodes}
    if subtype_filter:
        where.append("subtype = ANY(:subs)")
        params["subs"] = subtype_filter
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    bucket_seen: set[str] = set()

    def bucket_id(path: tuple) -> str:
        return "_grp:" + "/".join(str(p).replace("/", "_") for p in path)

    with ctx.conn() as conn:
        rows = reads._graph(conn, ctx.session).query(
            f"""
            SELECT node_id, subtype,
                   (attrs - ARRAY[
                     'body_text','body_text_md','body','text',
                     'pdf_url','local_pdf_path','pdf_bytes',
                     'raw','content','html'
                   ])::jsonb AS attrs,
                   COALESCE(
                             attrs->>'label', attrs->>'title',
                             attrs->>'name', attrs->>'subject',
                             attrs->>'summary', node_id
                           ) AS lbl,
                   {select_cols}
              FROM okg.graph_nodes
              {where_sql}
          ORDER BY {", ".join(f"k{i}" for i in range(len(keys)))}, lbl
            LIMIT :max_nodes
            """,
            params,
        )
        for row in rows:
            nid = row["node_id"]
            sub = row["subtype"]
            attrs = row["attrs"]
            lbl = row["lbl"]
            bucket_keys = [row[f"k{i}"] for i in range(len(keys))]
            # Collapse empty levels: drop "(none)" / "" values
            # from the path so person/view (no org_id) end up
            # under their subtype bucket.
            effective_path = tuple(v for v in bucket_keys if v not in ("", "(none)"))
            # Emit ancestor buckets (one per depth step).
            for i, val in enumerate(effective_path):
                sub_path = effective_path[: i + 1]
                bid = bucket_id(sub_path)
                if bid in bucket_seen:
                    continue
                bucket_seen.add(bid)
                parent_id = bucket_id(effective_path[:i]) if i > 0 else None
                nodes.append({
                    "id":      bid,
                    "label":   str(val),
                    "parent":  parent_id,
                    "depth":   i,
                    "count":   0,
                    "subtype": "__meta__",
                    "seq":     None,
                })
            if len(nodes) >= max_nodes:
                capped = True
                break
            seq_attr = sequence_attrs.get(sub)
            seq_val = attrs.get(seq_attr) if (seq_attr and isinstance(attrs, dict)) else None
            nodes.append({
                "id":      nid,
                "label":   lbl,
                "parent":  bucket_id(effective_path) if effective_path else None,
                "depth":   len(effective_path),
                "count":   1,
                "subtype": sub,
                "seq":     seq_val,
            })

    # Roll instance counts upward for buckets so each disc shows the
    # total nodes inside it.
    by_id = {n["id"]: n for n in nodes}
    for n in nodes:
        if n["subtype"] == "__meta__":
            continue
        cur_id = n["parent"]
        while cur_id and cur_id in by_id:
            by_id[cur_id]["count"] += 1
            cur_id = by_id[cur_id]["parent"]

    # Build a pack tree that mirrors the parent/child links and run
    # pack_hierarchy_balanced. Top-level buckets become balanced
    # forest-roots; ancestors fit around their descendants.
    from okg.viz.circle_pack import PackNode, pack_hierarchy_balanced
    pack_by_id: dict[str, PackNode] = {}
    for n in nodes:
        pack_by_id[n["id"]] = PackNode(id=n["id"], value=1.0)
    synth_root = PackNode(id="__root__")
    for n in nodes:
        pn = pack_by_id[n["id"]]
        if n["parent"] is None:
            synth_root.children.append(pn)
        else:
            parent_pn = pack_by_id.get(n["parent"])
            if parent_pn is not None:
                parent_pn.children.append(pn)
    pack_hierarchy_balanced(synth_root, padding=4.0, top_padding=12.0)
    SCALE = 12.0
    for pn in pack_by_id.values():
        pn.x *= SCALE
        pn.y *= SCALE
        pn.r *= SCALE
    for n in nodes:
        pn = pack_by_id.get(n["id"])
        if pn is not None:
            n["viz_x"], n["viz_y"], n["viz_r"] = pn.x, pn.y, pn.r

    # Re-flag depth so the client sees a 2-level "forest-root +
    # instance" structure: top-level buckets stay depth 0; everything
    # else becomes depth 1 (no further nesting in the bird's-eye).
    for n in nodes:
        if n["depth"] > 0:
            n["depth"] = 1

    return _msgpack_response(_envelope(
        nodes=nodes,
        edges=[],
        meta={
            "request_kind":     "hierarchy_tree",
            "hierarchy":        "nested:" + ",".join(keys),
            "fetched_depth":    len(keys),
            "total_top_nodes":  sum(1 for n in nodes if n["depth"] == 0),
            "total_absorbed":   0,
            "capped":           capped,
            "max_nodes":        max_nodes,
            "root_subtypes":    [],
            "absorbed_subtypes": {},
            "generation_id":    ctx.session.generation_id,
            "aggregated_edges": [],
        },
    ))


def _node_summary(n: reads.NodeRow) -> dict[str, Any]:
    return {
        "id": n.id,
        "type": n.type,
        "label": n.label or n.id,
    }


def build_router(
    ctx_provider: Callable[[], VizContext],
) -> APIRouter:
    """Register read-only data endpoints on an APIRouter.

    `ctx_provider` returns the live `VizContext` (called per
    request so lazy load still works).
    """
    router = APIRouter(prefix="/viz/api", route_class=WireRoute)

    # ---------- search ----------------------------------------------------

    def search(
        q: str = Query(..., min_length=1),
        limit: int = Query(20, ge=1, le=100),
    ) -> dict[str, Any]:
        """Search nodes by query.

        Tries indexed paths first, taking the first non-empty
        result set:
          1. Exact node-id match — pasted graph ids should win.
          2. Exact subtype intent — type-like queries such as
             `open_spec_change` should not rank arbitrary mentions
             above actual nodes of that type.
          3. Exact alias match — operator names and short labels.
          4. Substrate BM25 lexical (`attrs->>'text'`) — best
             when adapters populated the searchable text field.
          5. Bounded legacy substring tiers over readable labels
             and node ids.

        Each fallback fires only if the prior path returned zero.
        Pre-fix, this handler did `from okg.substrate.mcp import
        search as mcp_search` then called `mcp_search.search(...)`
        which AttributeError'd — `mcp_search` was already the
        function, not the module.
        """
        ctx = ctx_provider()
        from okg.substrate.mcp import search as mcp_search_fn

        items: list[dict[str, Any]] = []
        identifier_like = _looks_like_identifier_query(q)
        subtype_query = _normalize_subtype_query(q)
        strategy: str | None = None
        with ctx.conn() as conn:
            graph = reads._graph(conn, ctx.session)
            # Tier 0: exact node-id match. Exact identifiers must win
            # over BM25 hits in attrs text; otherwise queries like
            # `view:openspec_health` can be buried by unrelated chunks
            # that mention the same string.
            rows = graph.query(
                "SELECT node_id, subtype, attrs "
                "  FROM okg.graph_nodes "
                " WHERE node_id = :node_id "
                " LIMIT :limit",
                {
                    "node_id": q,
                    "limit": limit,
                },
            )
            items = [
                _search_item(
                    r["node_id"], r["subtype"], r["attrs"], q,
                    match_field="node_id",
                    match_tier="exact_node_id",
                )
                for r in rows
            ]
            if items:
                strategy = "exact_node_id"

            # Tier 1: exact subtype/type intent. This keeps
            # `open_spec_change` and `agent_session` from falling
            # through to arbitrary label mentions.
            if not items and subtype_query:
                rows = graph.query(
                    "SELECT node_id, subtype, attrs "
                    "  FROM okg.graph_nodes "
                    " WHERE subtype = :subtype "
                    " ORDER BY "
                    "   COALESCE(attrs->>'label', attrs->>'title', "
                    "            attrs->>'name', node_id) ASC "
                    " LIMIT :candidate_limit",
                    {
                        "subtype": subtype_query,
                        "candidate_limit": max(limit * 4, 40),
                    },
                )
                items = [
                    _search_item(
                        r["node_id"], r["subtype"], r["attrs"], q,
                        match_field="subtype",
                        match_tier="subtype_exact",
                    )
                    for r in rows
                ]
                if items:
                    deg = reads.degrees_for(
                        conn, ctx.session,
                        [it["id"] for it in items if it.get("id")],
                    )
                    for it in items:
                        it["degree"] = deg.get(it["id"], 0)
                    items.sort(key=lambda it: (
                        -int(it.get("degree") or 0),
                        str(it.get("label") or "").lower(),
                        str(it.get("id") or ""),
                    ))
                    items = items[:limit]
                    strategy = "subtype_exact"

            # Tier 2: exact alias match. This is the fast path for
            # short operator-entered names that are not full node ids,
            # e.g. service/site aliases. It avoids the legacy
            # label-substring scan on large deployments.
            if not items:
                aliases = [q]
                for alias in (q.lower(), q.upper()):
                    if alias not in aliases:
                        aliases.append(alias)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT DISTINCT ON (a.canonical) "
                        "       a.canonical, a.alias, a.scope "
                        "  FROM okg.aliases a "
                        " WHERE a.catalog_version_id = %(cv)s "
                        "   AND a.alias = ANY(%(aliases)s) "
                        " ORDER BY a.canonical, "
                        "          (a.alias = %(q)s) DESC, "
                        "          (a.scope IS NULL) "
                        "LIMIT %(limit)s",
                        {
                            "cv": ctx.session.catalog_version_id,
                            "aliases": aliases,
                            "q": q,
                            "limit": limit,
                        },
                    )
                    alias_rows = cur.fetchall()
                    canonicals = [r[0] for r in alias_rows]
                    nodes = reads.fetch_nodes(conn, ctx.session, canonicals, light=True)
                    items = []
                    for canonical, _alias, _scope in alias_rows:
                        node = nodes.get(canonical)
                        if node is None:
                            continue
                        items.append(
                            _search_item(
                                node.id, node.type, node.attrs, q,
                                match_field="alias",
                                match_tier="exact_alias",
                            )
                        )
                    if items:
                        strategy = "exact_alias"

            # Tier 3: BM25 lexical via the substrate MCP path. This
            # uses the partial BM25 index and is safe as the first
            # fuzzy tier; exact id/alias/type matches already won.
            if not items:
                try:
                    _set_statement_timeout(conn, 5000)
                    res = mcp_search_fn(
                        conn, ctx.session, q,
                        method="lexical", filter={}, limit=limit,
                        embedder=None,
                    )
                    items = [
                        _search_item(
                            hit.get("node_id"),
                            hit.get("subtype"),
                            hit.get("attrs") or {},
                            q,
                            match_field="text",
                            match_tier="lexical",
                        )
                        for hit in res.get("results", [])
                    ]
                    if items:
                        strategy = "lexical"
                except Exception as e:
                    conn.rollback()
                    logger.debug(
                        "viz lexical search failed (%s); falling back "
                        "to bounded label/id substring", e,
                    )
                finally:
                    _reset_statement_timeout(conn)

            # Tier 4: substring match over readable label fields.
            # This is a legacy convenience tier, but it is unindexed
            # on JSON attr expressions. Keep it bounded so large
            # deployments do not freeze the interactive search box.
            # DISTINCT ON (node_id) collapses multi-generation rows.
            if not items:
                _set_statement_timeout(conn, _SLOW_SEARCH_TIER_TIMEOUT_MS)
                try:
                    rows = graph.query(
                        "SELECT * FROM ( "
                        "  SELECT node_id, subtype, attrs, "
                        "         COALESCE(NULLIF(attrs->>'label', ''), "
                        "                  NULLIF(attrs->>'title', ''), "
                        "                  NULLIF(attrs->>'name', '')) "
                        "                  AS label "
                        "    FROM okg.graph_nodes "
                        "   WHERE (attrs->>'label' ILIKE :needle "
                        "          OR attrs->>'title' ILIKE :needle "
                        "          OR attrs->>'name' ILIKE :needle) "
                        ") sub "
                        "ORDER BY "
                        "  CASE "
                        "    WHEN lower(label) = lower(:q) THEN 0 "
                        "    WHEN label ILIKE :prefix THEN 1 "
                        "    ELSE 2 "
                        "  END, "
                        "  length(label) ASC NULLS LAST "
                        "LIMIT :limit",
                        {
                            "needle": f"%{q}%",
                            "prefix": f"{q}%",
                            "q": q,
                            "limit": limit,
                        },
                    )
                    items = [
                        _search_item(
                            r["node_id"], r["subtype"], r["attrs"], q,
                            match_field="label",
                            match_tier="label_substring",
                        )
                        for r in rows
                    ]
                    if items:
                        strategy = "label_substring"
                except psycopg.errors.QueryCanceled:
                    conn.rollback()
                    logger.debug(
                        "viz label substring search skipped after "
                        "%sms timeout (query=%r)",
                        _SLOW_SEARCH_TIER_TIMEOUT_MS, q,
                    )
                finally:
                    _reset_statement_timeout(conn)

            # Tier 5: substring match over node_id. Catches id
            # fragments, but is also unindexed for leading-wildcard
            # searches, so it gets the same bounded budget.
            if not items:
                _set_statement_timeout(conn, _SLOW_SEARCH_TIER_TIMEOUT_MS)
                try:
                    rows = graph.query(
                        "SELECT node_id, subtype, attrs "
                        "  FROM okg.graph_nodes "
                        " WHERE node_id ILIKE :needle "
                        " ORDER BY node_id "
                        " LIMIT :limit",
                        {
                            "needle": f"%{q}%",
                            "limit": limit,
                        },
                    )
                    items = [
                        _search_item(
                            r["node_id"], r["subtype"], r["attrs"], q,
                            match_field="node_id",
                            match_tier="node_id_substring",
                        )
                        for r in rows
                    ]
                    if items:
                        strategy = "node_id_substring"
                except psycopg.errors.QueryCanceled:
                    conn.rollback()
                    logger.debug(
                        "viz node-id substring search skipped after "
                        "%sms timeout (query=%r)",
                        _SLOW_SEARCH_TIER_TIMEOUT_MS, q,
                    )
                finally:
                    _reset_statement_timeout(conn)

            # Attach real degrees in one round-trip.
            if items:
                try:
                    _set_statement_timeout(conn, _SLOW_SEARCH_TIER_TIMEOUT_MS)
                    deg = reads.degrees_for(
                        conn, ctx.session,
                        [it["id"] for it in items if it.get("id")],
                    )
                except psycopg.errors.QueryCanceled:
                    conn.rollback()
                    logger.debug(
                        "viz search degree attachment skipped after "
                        "%sms timeout (query=%r)",
                        _SLOW_SEARCH_TIER_TIMEOUT_MS, q,
                    )
                    deg = {}
                finally:
                    _reset_statement_timeout(conn)
                for it in items:
                    it["degree"] = deg.get(it["id"], 0)

        return _envelope(
            nodes=items,
            meta={
                "request_kind": "search",
                "query": q,
                "limit": limit,
                "returned_nodes": len(items),
                "returned_edges": 0,
                "strategy": strategy,
                "identifier_like": identifier_like,
            },
            items=items,
        )

    # ---------- types -----------------------------------------------------

    def types() -> dict[str, int]:
        """Node count per subtype, restricted to subtypes with count > 0."""
        ctx = ctx_provider()
        with ctx.conn() as conn:
            counts = _count_by_subtype_cached(conn, ctx)
        return {t: c for t, c in counts.items() if c > 0}

    def type_sample(
        type: str,
        limit: int = Query(50, ge=1, le=_MAX_SAMPLE_LIMIT),
        predicate: str | None = Query(None),
        include_attrs: bool = Query(False),
    ) -> dict[str, Any]:
        """Sample nodes of the given subtype.

        Setting `include_attrs=true` switches `light` off so each
        item carries its `attrs` dict — useful for browser callers
        (e.g. the okg-workspace supervisor console's session index)
        that need to sort or filter on a payload field without doing
        N+1 follow-up `/node/{id}` calls.
        """
        excluded_types, included_types = _parse_type_filter(predicate)
        # If the requested type is itself filtered out, return empty.
        if (excluded_types and type in excluded_types) or (
            included_types and type not in included_types
        ):
            return _envelope(
                nodes=[], items=[],
                meta={
                    "request_kind": "type_sample", "type": type,
                    "limit": limit, "total": 0,
                    "returned_nodes": 0, "returned_edges": 0,
                    "truncated": False, "filtered": True,
                },
                total=0, truncated=False,
            )
        ctx = ctx_provider()
        with ctx.conn() as conn:
            count = reads.count_nodes(conn, ctx.session, subtype=type)
            sampled = list(reads.iter_nodes(
                conn, ctx.session, subtype=type, limit=limit,
                light=not include_attrs,
                order_by_id=True,
            ))
            try:
                _set_statement_timeout(conn, _SLOW_SEARCH_TIER_TIMEOUT_MS)
                degrees = reads.degrees_for(
                    conn, ctx.session, [n.id for n in sampled],
                )
            except psycopg.errors.QueryCanceled:
                conn.rollback()
                logger.debug(
                    "viz type sample degree attachment skipped after "
                    "%sms timeout (type=%r)",
                    _SLOW_SEARCH_TIER_TIMEOUT_MS, type,
                )
                degrees = {}
            finally:
                _reset_statement_timeout(conn)
        # Sort returned items by degree desc so the canvas's
        # "MOST CONNECTED" panel (and degree-aware layouts) get
        # a meaningful prefix.
        sampled = sorted(
            sampled, key=lambda n: -degrees.get(n.id, 0),
        )
        items = [
            {
                "id": n.id, "type": n.type,
                "label": n.label or n.id,
                "degree": degrees.get(n.id, 0),
                **({"attrs": n.attrs} if include_attrs else {}),
            }
            for n in sampled
        ]
        return _envelope(
            nodes=items,
            meta={
                "request_kind": "type_sample",
                "type": type,
                "limit": limit,
                "total": count,
                "returned_nodes": len(items),
                "returned_edges": 0,
                "truncated": count > len(items),
            },
            items=items,
            total=count,
            truncated=count > len(items),
        )

    # ---------- single node ----------------------------------------------

    def node(id: str) -> dict[str, Any]:
        """Full node payload + per-edge-type neighbor counts."""
        ctx = ctx_provider()
        with ctx.conn() as conn:
            view_id = _view_id_from_node_id(id)
            if view_id is not None:
                native = _native_view_markdown_node(
                    conn,
                    ctx,
                    view_id=view_id,
                )
                out_edges = []
                in_edges = []
            else:
                n = reads.fetch_node(conn, ctx.session, id)
                if n is None:
                    raise HTTPException(
                        status_code=404, detail=f"Node {id!r} not found",
                    )
                node_id = n.id
                attrs = n.attrs
                if n.type == "document":
                    attrs = _reader_hydrate_document_attrs(
                        reads._graph(conn, ctx.session),
                        node_id=n.id,
                        attrs=n.attrs,
                    )
                native = {
                    "node_id": n.id,
                    "node_type": n.type,
                    "node_label": n.label,
                    "attrs": attrs,
                }
                out_edges = reads.iter_out_edges(conn, ctx.session, node_id)
                in_edges = reads.iter_in_edges(conn, ctx.session, node_id)

        # Aggregate by edge_type, separately for in/out.
        edge_counts: dict[str, dict[str, int]] = {"out": {}, "in": {}}
        for e in out_edges:
            edge_counts["out"][e.edge_type] = (
                edge_counts["out"].get(e.edge_type, 0) + 1
            )
        for e in in_edges:
            edge_counts["in"][e.edge_type] = (
                edge_counts["in"].get(e.edge_type, 0) + 1
            )
        return {
            "node_id": native["node_id"],
            "node_type": native["node_type"],
            "node_label": native["node_label"],
            "generation_id": ctx.session.generation_id,
            "catalog_version_id": ctx.session.catalog_version_id,
            "attrs": native["attrs"],
            "edge_counts": edge_counts,
            "degree": len(out_edges) + len(in_edges),
        }

    # ---------- expand ---------------------------------------------------

    def expand(
        id: str,
        edge_type: str | None = Query(None),
        direction: str = Query("out", pattern="^(out|in|both)$"),
        limit: int = Query(100, ge=1),
        offset: int = Query(0, ge=0),
        predicate: str | None = Query(None),
        format: str | None = Query(None, pattern="^(json|msgpack)$"),
    ) -> Response:
        """Expand neighbors of a node. Returns msgpack-encoded
        envelope `{nodes, edges, meta}`.

        `offset` enables sampled-expansion load-more
        (viz-expansion-and-supernodes §2): the underlying SQL is
        ORDER BY (edge_type, dst|src) so paging is deterministic
        across calls within a generation.
        """
        if limit > _MAX_EXPAND_LIMIT:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"limit={limit} exceeds maximum "
                    f"{_MAX_EXPAND_LIMIT}."
                ),
            )
        # Parse the predicate UP FRONT so we can push the
        # type filter into SQL (server-side) rather than
        # over-fetching and dropping post-hoc. The user's "limit"
        # then applies to neighbors that ALREADY pass the filter,
        # so asking for 40 returns up to 40 visible neighbors —
        # not 40 raw edges of which most are filtered.
        excluded_types, included_types = _parse_type_filter(predicate)
        excluded_list = list(excluded_types) if excluded_types else None
        included_list = list(included_types) if included_types else None
        ctx = ctx_provider()
        with ctx.conn() as conn:
            _ensure_node_or_native_view(conn, ctx, id)
            if _view_id_from_node_id(id) is not None:
                payload = _envelope(
                    nodes=[],
                    edges=[],
                    meta={
                        "request_kind": "expand",
                        "source": id,
                        "edge_type": edge_type,
                        "direction": direction,
                        "limit": limit,
                        "total": 0,
                        "returned_nodes": 0,
                        "returned_edges": 0,
                        "truncated": False,
                    },
                )
                return _envelope_response(payload, format)
            collected_edges: list[tuple[str, reads.EdgeRow]] = []
            total_neighbors = 0
            directions = ("out", "in") if direction == "both" else (direction,)
            # When fetching BOTH directions, split the budget so we
            # don't return all-out + 0-in (the prior `[:limit]` slice
            # always favoured the first direction — out — and dropped
            # every inbound edge).
            per_dir_limit = (
                max(1, limit // len(directions)) if direction == "both" else limit
            )
            for d in directions:
                if d == "out":
                    total_neighbors += reads.count_out_neighbors(
                        conn, ctx.session, id,
                        edge_type=edge_type,
                        excluded_subtypes=excluded_list,
                        included_subtypes=included_list,
                    )
                    es = reads.iter_out_edges(
                        conn, ctx.session, id,
                        edge_type=edge_type, limit=per_dir_limit, offset=offset,
                        excluded_subtypes=excluded_list,
                        included_subtypes=included_list,
                    )
                else:
                    total_neighbors += reads.count_in_neighbors(
                        conn, ctx.session, id,
                        edge_type=edge_type,
                        excluded_subtypes=excluded_list,
                        included_subtypes=included_list,
                    )
                    es = reads.iter_in_edges(
                        conn, ctx.session, id,
                        edge_type=edge_type, limit=per_dir_limit, offset=offset,
                        excluded_subtypes=excluded_list,
                        included_subtypes=included_list,
                    )
                collected_edges.extend((d, e) for e in es)

            # Resolve neighbor node summaries in ONE bulk query
            # instead of N round-trips. fetch_nodes(light=True) skips
            # body_text and other heavy attrs we don't render.
            neighbor_ids = {
                (e.dst if d == "out" else e.src) for d, e in collected_edges
            }
            nbs = reads.fetch_nodes(
                conn, ctx.session, list(neighbor_ids), light=True,
            )
            summaries = {nid: _node_summary(nb) for nid, nb in nbs.items()}
            for nid in neighbor_ids - summaries.keys():
                native_summary = _native_view_summary(conn, ctx, nid)
                if native_summary is not None:
                    summaries[nid] = native_summary
            degrees = reads.degrees_for(
                conn, ctx.session, list(neighbor_ids),
            )

        # Apply sidebar type-filter (predicate). Drops neighbors
        # whose subtype is in the excluded set OR not in the
        # included set. Edges to filtered neighbors are dropped too.
        excluded_types, included_types = _parse_type_filter(predicate)
        def _keep_neighbor(nb_id: str) -> bool:
            nb = summaries.get(nb_id)
            if nb is None:
                return False
            t = nb.get("type")
            if excluded_types and t in excluded_types:
                return False
            if included_types and t not in included_types:
                return False
            return True

        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        seen: set[str] = set()
        # Each direction is already capped to `per_dir_limit`, so the
        # combined list is at most `limit` (or 2× when both). Emit it
        # all rather than slicing — the slice was hiding inbound edges
        # whenever outbound came first.
        for d, e in collected_edges:
            nb_id = e.dst if d == "out" else e.src
            if not _keep_neighbor(nb_id):
                continue
            if nb_id in summaries and nb_id not in seen:
                seen.add(nb_id)
                nodes.append({
                    **summaries[nb_id],
                    "degree": degrees.get(nb_id, 0),
                    "rank": len(nodes),
                })
            edges.append({
                "source": id if d == "out" else nb_id,
                "target": nb_id if d == "out" else id,
                "type": e.edge_type,
            })
        truncated = total_neighbors > len(nodes)
        payload = _envelope(
            nodes=nodes,
            edges=edges,
            meta={
                "request_kind": "expand",
                "source": id,
                "edge_type": edge_type,
                "direction": direction,
                "limit": limit,
                "total": total_neighbors,
                "returned_nodes": len(nodes),
                "returned_edges": len(edges),
                "truncated": truncated,
            },
        )
        return _envelope_response(payload, format)

    # ---------- k-hop ----------------------------------------------------

    def khop(
        id: str,
        k: int | None = Query(None, ge=1, le=4),
        depth: int = Query(2, ge=1, le=4),
        direction: str = Query("both", pattern="^(both|out|in)$"),
        limit: int = Query(200, ge=1, le=_MAX_EXPAND_LIMIT),
        format: str | None = Query(None, pattern="^(json|msgpack)$"),
    ) -> Response:
        """K-hop expansion via repeated edge fan-out.

        The frontend sends the hop count as `k`; legacy clients pass
        `depth`. `k` wins when both are present.
        """
        hops = k if k is not None else depth
        ctx = ctx_provider()
        with ctx.conn() as conn:
            _ensure_node_or_native_view(conn, ctx, id)
            if _view_id_from_node_id(id) is not None:
                native_summary = _native_view_summary(conn, ctx, id)
                nodes = [native_summary] if native_summary is not None else []
                payload = _envelope(
                    nodes=nodes,
                    edges=[],
                    meta={
                        "request_kind": "khop",
                        "source": id,
                        "depth": hops,
                        "direction": direction,
                        "limit": limit,
                        "returned_nodes": len(nodes),
                        "returned_edges": 0,
                        "truncated": False,
                    },
                )
                return _envelope_response(payload, format)

            visited: set[str] = {id}
            frontier: set[str] = {id}
            edges: list[dict[str, Any]] = []
            seen_edges: set[tuple[str, str, str]] = set()
            for _ in range(hops):
                if len(visited) >= limit:
                    break
                next_frontier: set[str] = set()
                for src in list(frontier):
                    if len(visited) >= limit:
                        break
                    if direction in ("both", "out"):
                        for e in reads.iter_out_edges(conn, ctx.session, src):
                            key = (src, e.dst, e.edge_type)
                            if key not in seen_edges:
                                seen_edges.add(key)
                                edges.append({
                                    "source": src, "target": e.dst,
                                    "type": e.edge_type,
                                })
                            if e.dst not in visited:
                                visited.add(e.dst)
                                next_frontier.add(e.dst)
                                if len(visited) >= limit:
                                    break
                    if direction in ("both", "in") and len(visited) < limit:
                        for e in reads.iter_in_edges(conn, ctx.session, src):
                            key = (e.src, src, e.edge_type)
                            if key not in seen_edges:
                                seen_edges.add(key)
                                edges.append({
                                    "source": e.src, "target": src,
                                    "type": e.edge_type,
                                })
                            if e.src not in visited:
                                visited.add(e.src)
                                next_frontier.add(e.src)
                                if len(visited) >= limit:
                                    break
                frontier = next_frontier
                if not frontier:
                    break

            # Resolve summaries for visited nodes.
            nodes: list[dict[str, Any]] = []
            for nid in visited:
                nb = reads.fetch_node(conn, ctx.session, nid)
                if nb is not None:
                    nodes.append(_node_summary(nb))
                    continue
                native_summary = _native_view_summary(conn, ctx, nid)
                if native_summary is not None:
                    nodes.append(native_summary)

        payload = _envelope(
            nodes=nodes,
            edges=edges,
            meta={
                "request_kind": "khop",
                "source": id,
                "depth": hops,
                "direction": direction,
                "limit": limit,
                "returned_nodes": len(nodes),
                "returned_edges": len(edges),
                "truncated": len(visited) >= limit,
            },
        )
        return _envelope_response(payload, format)

    # ---------- stats ----------------------------------------------------

    def stats() -> dict[str, Any]:
        """Graph-wide totals + node-type breakdown."""
        ctx = ctx_provider()
        edge_types_truncated = False
        edge_count_estimated = False
        with ctx.conn() as conn:
            counts = _count_by_subtype_cached(conn, ctx)
            node_count = sum(counts.values())
            edge_count = reads.count_edges(conn, ctx.session)
            edge_counts = reads.count_by_edge_type(conn, ctx.session)
        return _envelope(
            meta={
                "request_kind": "stats",
                "edge_types_truncated": edge_types_truncated,
                "edge_count_estimated": edge_count_estimated,
            },
            node_count=node_count,
            edge_count=edge_count,
            node_types=counts,
            edge_types=edge_counts,
            generation_id=ctx.session.generation_id,
        )

    # ---------- generation switcher --------------------------------------
    # Lists published (and pending) generations for the current
    # deployment, plus the generation the viz is currently pinned to.
    # Lets the user roll back to an earlier published gen when the
    # active pin has stale or in-flight view state.

    def generations() -> dict[str, Any]:
        ctx = ctx_provider()
        with ctx.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT generation_id, status, published_at
                  FROM okg.graph_generations
                 WHERE deployment_name = %(dep)s
              ORDER BY generation_id DESC
                 LIMIT 50
                """,
                {"dep": ctx.deployment_name},
            )
            rows = cur.fetchall()
        items = [
            {
                "generation_id": int(r[0]),
                "status": r[1],
                "published_at": r[2].isoformat() if r[2] is not None else None,
            }
            for r in rows
        ]
        return {
            "items": items,
            "current": ctx.session.generation_id,
            "deployment": ctx.deployment_name,
        }

    def repin(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        """Re-pin the viz session to a different generation.

        Replaces `ctx.session` in place. All subsequent reads will
        see the requested generation. Only published generations
        are accepted to avoid pinning to a half-built one.
        """
        requested = payload.get("generation_id")
        if requested is None or requested == "":
            raise HTTPException(
                status_code=400,
                detail="generation_id required (int or string)",
            )
        if not isinstance(requested, (int, str)):
            raise HTTPException(
                status_code=400,
                detail="generation_id must be an int or string",
            )
        ctx = ctx_provider()
        try:
            # okg's factory carries the session search_path pin (issue #1968).
            with db_connect(ctx.dsn, autocommit=True) as seed_conn:
                new_session = open_session(
                    seed_conn,
                    deployment_name=ctx.deployment_name,
                    requested_generation=requested,
                )
        except SubstrateError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        ctx.session = new_session
        ctx._archetype_kinds_cache = None
        ctx._absorption_cache = None
        return {
            "ok": True,
            "current": new_session.generation_id,
            "catalog_version": new_session.catalog_version_id,
        }

    # ---------- system overview ------------------------------------------
    # Operator-facing dashboard data: database contents, inflight work,
    # operations summary. Read-only; not generation-pinned (asks
    # "what is the latest", not "what was true at gen N").

    def system_snapshot() -> dict[str, Any]:
        ctx = ctx_provider()
        from okg.substrate.profiling import profile_stage, profiling_context
        from okg.substrate.profiling.policy import resolve_policy
        policy = resolve_policy(
            ctx.manifest.get("profiling")
            if isinstance(ctx.manifest.get("profiling"), dict)
            else None,
        )
        with profiling_context(
            deployment=ctx.deployment_name,
            target_kind="viz-request",
            command="GET /viz/api/system/snapshot",
            policy=policy,
        ) as prof:
            prof.request_path = "/viz/api/system/snapshot"
            with ctx.conn() as conn:
                with profile_stage("viz.system_snapshot"):
                    return reads_system.fetch_snapshot(conn, ctx.deployment_name)

    def system_profile(profile_id: str) -> dict[str, Any]:
        ctx = ctx_provider()
        try:
            from okg.substrate.profiling.artifacts import (
                find_profile,
                load_profile,
            )
            path = find_profile(profile_id, ctx.deployment_name)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid profile id")
        if path is None:
            raise HTTPException(status_code=404, detail="profile not found")
        return load_profile(path)

    def system_profile_artifact(
        profile_id: str, artifact_path: str,
    ) -> FileResponse:
        ctx = ctx_provider()
        try:
            from okg.substrate.profiling.artifacts import find_profile
            profile_json = find_profile(profile_id, ctx.deployment_name)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid profile id")
        if profile_json is None:
            raise HTTPException(status_code=404, detail="profile not found")
        # Shared containment primitive: the artifact path must resolve inside
        # the profile's own directory (symlinks followed) and be a regular file;
        # a traversal / out-of-root / missing artifact all deny as 404 (not
        # found), without leaking which.
        root = profile_json.parent.resolve()
        target = resolve_within_approved_root(
            root / artifact_path, [root], must_be_file=True,
        )
        if target is None:
            raise HTTPException(status_code=404, detail="artifact not found")
        return FileResponse(target)

    # ---------- config ---------------------------------------------------

    def config() -> dict[str, Any]:
        """Return the merged viz config for this deployment."""
        return ctx_provider().config

    # ---------- ontology -------------------------------------------------

    def ontology() -> dict[str, Any]:
        """Subtypes (with per-subtype node counts + sample id) and
        edge narrowings at the pinned catalog version."""
        ctx = ctx_provider()
        with ctx.conn() as conn:
            subtypes_meta = reads.fetch_subtypes(conn, ctx.session)
            narrowings = reads.fetch_edge_narrowings(conn, ctx.session)
            counts = _count_by_subtype_cached(conn, ctx)
            samples = _sample_by_subtype(
                conn, ctx, [s["name"] for s in subtypes_meta], counts,
            )

        subtypes: list[dict[str, Any]] = [
            {
                "name": s["name"],
                "archetype": (s.get("metadata") or {}).get("archetype", ""),
                "description": (s.get("metadata") or {}).get("description", ""),
                "fields": (s.get("metadata") or {}).get("fields", []),
                "count": counts.get(s["name"], 0),
                "sample": samples.get(s["name"]),
                "transient": (
                    (s.get("metadata") or {}).get("transient", False)
                ),
            }
            for s in subtypes_meta
        ]
        subtypes.sort(key=lambda r: -r["count"])

        edges_view = [
            {
                "archetype": n["edge_type"],
                "src_subtypes": [n["src_subtype"]],
                "dst_subtypes": [n["dst_subtype"]],
                "slice_origin": "",
            }
            for n in narrowings
        ]

        registered = {st["name"] for st in subtypes}
        unregistered = sorted(
            (t for t in counts if t not in registered),
            key=lambda t: -counts[t],
        )
        return {
            "subtypes": subtypes,
            "edges": edges_view,
            "unregistered_subtypes": [
                {"name": t, "count": counts[t], "sample": samples.get(t)}
                for t in unregistered
            ],
        }

    # ---------- overview mode (viz-overview-mode) ------------------------

    def overview(
        aggregation: str | None = Query(None),
    ) -> Response:
        """Aggregated global graph at the pinned generation.

        Groups nodes by either:
          - the user-picked `?aggregation=<comma-list>` query
            (refactor-viz-layout-architecture §6 Group-by picker), or
          - the deployment-declared `viz.overview.aggregation_key`,
            falling back to `["subtype"]`.

        The first key may be `subtype` (the column) or any attribute
        name (extracted via `attrs->>'name'`). Subsequent keys are
        always attribute names. Missing attribute values render as
        the literal `(none)` so they bucket together.

        Returns a msgpack envelope:
          {
            "groups": [{id, subtype, key, count, ...}, ...],
            "edges":  [{src, dst, edge_types, count, dominant_edge_type}, ...],
            "meta":   {aggregation_key, group_count, edge_count}
          }
        """
        ctx = ctx_provider()
        viz_cfg = ctx.config or {}
        ov = (viz_cfg.get("overview") or {})

        if aggregation:
            agg_key = [k.strip() for k in aggregation.split(",") if k.strip()]
        else:
            agg_key = ov.get("aggregation_key") or ["subtype"]
        if not isinstance(agg_key, list) or not agg_key:
            agg_key = ["subtype"]

        # First key is either `subtype` (the column) or an attribute
        # name. When it's an attribute, we group by jsonb-extracted
        # value only — no subtype dimension. This is what the
        # Group-by picker uses for "By year" (one bucket per year,
        # ignoring subtype).
        first_key = agg_key[0]
        first_is_subtype = (first_key == "subtype")
        attr_keys = list(agg_key[1:]) if first_is_subtype else list(agg_key)

        pin = ctx.compatibility_generation_id

        # Build the SQL group-key expression. Group id is the joined
        # values: "subtype/attrA/attrB" or "attrA/attrB" depending on
        # whether the leading dimension is subtype.
        attr_select = "".join(
            f", COALESCE(attrs->>'{k}', '(none)') AS k{i}"
            for i, k in enumerate(attr_keys)
        )
        attr_group_cols = "".join(f", k{i}" for i in range(len(attr_keys)))
        leading_select = "subtype" if first_is_subtype else "NULL::text AS subtype"
        # Build GROUP BY clause for the group-counts query.
        group_parts: list[str] = []
        if first_is_subtype:
            group_parts.append("subtype")
        group_parts.extend(f"k{i}" for i in range(len(attr_keys)))
        groups_group_clause = ", ".join(group_parts) if group_parts else "()"

        edge_aggregation_truncated = False
        edge_rows = []
        with ctx.conn() as conn:
            graph = reads._graph(conn, ctx.session)
            estimated_edges = _estimated_reltuples(
                conn,
                "okg.graph_edges",
            )
            if first_is_subtype and not attr_keys:
                counts = _count_by_subtype_cached(conn, ctx)
                group_rows = [
                    (subtype, count)
                    for subtype, count in counts.items()
                    if count > 0
                ]
                group_rows.sort(key=lambda row: -row[1])
            else:
                # 1. Group counts. When the first dimension is an
                #    attribute (not subtype), subtype drops out of
                #    GROUP BY and we project NULL so column indexing
                #    in the row-handler stays consistent.
                group_rows = [
                    (
                        r["subtype"],
                        *(r[f"k{i}"] for i in range(len(attr_keys))),
                        r["n"],
                    )
                    for r in graph.query(
                        f"""
                        WITH live AS (
                          SELECT subtype{attr_select}
                            FROM okg.graph_nodes
                        )
                        SELECT {leading_select}{attr_group_cols}, count(*) AS n
                          FROM live
                      GROUP BY {groups_group_clause}
                      ORDER BY n DESC
                        """,
                    )
                ]

            # 2. Inter-group edges (bundled per src_group / dst_group / edge_type).
            # This is useful on small graphs but pathological on CMS-scale
            # overlays because it joins every live edge to both endpoint
            # nodes. Keep the overview interactive and surface the omission.
            if estimated_edges > _LARGE_GRAPH_EDGE_SCAN_THRESHOLD:
                edge_aggregation_truncated = True
            else:
                src_leading = "ns.subtype AS src_subtype" if first_is_subtype else "NULL::text AS src_subtype"
                dst_leading = "nd.subtype AS dst_subtype" if first_is_subtype else "NULL::text AS dst_subtype"
                src_group_lead = "ns.subtype" if first_is_subtype else ""
                dst_group_lead = "nd.subtype" if first_is_subtype else ""
                edge_group_parts: list[str] = []
                if src_group_lead:
                    edge_group_parts.append(src_group_lead)
                edge_group_parts.extend(f"ns.k{i}" for i in range(len(attr_keys)))
                if dst_group_lead:
                    edge_group_parts.append(dst_group_lead)
                edge_group_parts.extend(f"nd.k{i}" for i in range(len(attr_keys)))
                edge_group_parts.append("e.edge_type")
                edge_group_clause = ", ".join(edge_group_parts)

                try:
                    _set_statement_timeout(conn, 5000)
                    edge_rows = [
                        (
                            r["src_subtype"],
                            *(r[f"src_k{i}"] for i in range(len(attr_keys))),
                            r["dst_subtype"],
                            *(r[f"dst_k{i}"] for i in range(len(attr_keys))),
                            r["edge_type"],
                            r["n"],
                        )
                        for r in graph.query(
                            f"""
                            WITH n AS (
                              SELECT node_id, subtype{attr_select}
                                FROM okg.graph_nodes
                            ),
                            e AS (
                              SELECT src, dst, edge_type
                                FROM okg.graph_edges
                            )
                            SELECT {src_leading}{
                                ''.join(f', ns.k{i} AS src_k{i}' for i in range(len(attr_keys)))},
                                   {dst_leading}{
                                ''.join(f', nd.k{i} AS dst_k{i}' for i in range(len(attr_keys)))},
                                   e.edge_type, count(*) AS n
                              FROM e
                              JOIN n ns ON ns.node_id = e.src
                              JOIN n nd ON nd.node_id = e.dst
                          GROUP BY {edge_group_clause}
                            """,
                        )
                    ]
                except psycopg.errors.QueryCanceled:
                    conn.rollback()
                    edge_aggregation_truncated = True
                finally:
                    _reset_statement_timeout(conn)

        # Build the JSON payload. Group id is the joined values:
        # "subtype/attrA/attrB" or "attrA/attrB" depending on whether
        # the leading dimension is subtype.
        def gid(parts: list) -> str:
            return "/".join(str(p) for p in parts if p is not None)

        groups: list[dict[str, Any]] = []
        for row in group_rows:
            subtype = row[0]
            attr_vals = list(row[1:1 + len(attr_keys)])
            count = row[1 + len(attr_keys)]
            id_parts = [subtype] + attr_vals if first_is_subtype else attr_vals
            label_parts = [subtype] + attr_vals if first_is_subtype else attr_vals
            groups.append({
                "id": gid(id_parts),
                "subtype": subtype,
                "key": label_parts,
                "count": count,
            })

        bundled: dict[tuple, dict[str, int]] = {}
        for row in edge_rows:
            src_subtype = row[0]
            src_attrs = list(row[1:1 + len(attr_keys)])
            dst_subtype = row[1 + len(attr_keys)]
            dst_attrs = list(row[2 + len(attr_keys):2 + 2 * len(attr_keys)])
            edge_type = row[2 + 2 * len(attr_keys)]
            n = row[3 + 2 * len(attr_keys)]
            src_parts = ([src_subtype] + src_attrs) if first_is_subtype else src_attrs
            dst_parts = ([dst_subtype] + dst_attrs) if first_is_subtype else dst_attrs
            src_id = gid(src_parts)
            dst_id = gid(dst_parts)
            key = (src_id, dst_id)
            slot = bundled.setdefault(key, {})
            slot[edge_type] = slot.get(edge_type, 0) + n

        edges: list[dict[str, Any]] = []
        for (src_id, dst_id), et_counts in bundled.items():
            total = sum(et_counts.values())
            dominant = max(et_counts.items(), key=lambda kv: kv[1])[0]
            edges.append({
                "src": src_id,
                "dst": dst_id,
                "edge_types": et_counts,
                "count": total,
                "dominant_edge_type": dominant,
            })

        payload = _envelope(
            nodes=[],  # not used — frontend reads "groups"
            edges=edges,
            meta={
                "request_kind": "overview",
                "aggregation_key": agg_key,
                "group_count": len(groups),
                "edge_count": len(edges),
                "edge_aggregation_truncated": edge_aggregation_truncated,
                "estimated_edge_rows": estimated_edges,
                "generation_id": pin,
            },
            groups=groups,
        )
        return _msgpack_response(payload)

    # ---------- hierarchy navigation -------------------------------
    # add-hierarchical-graph-navigation §3. Returns the navigation
    # tree rooted at the deployment level. Containment edges (per
    # archetype `relation_kind`) define the hierarchy walk; reference
    # edges show as cross-links at the current level.

    _HIERARCHY_INSTANCE_CAP = 200

    def hierarchy_root() -> Response:
        """Top-level buckets: forest-root subtypes + cross-refs.

        A "forest root" is a primary-tier subtype that is NOT a
        containment target of any other primary-tier subtype. In
        okg-workspace, `open_spec_change` is a forest root because
        it contains `proposal_task` / `design_decision` / etc.;
        `proposal_task` is NOT a root because it's contained by
        `open_spec_change` and would appear after drilling in.

        Returns:
          {
            "buckets":    [{id, label, subtype, count}, ...],
            "cross_refs": [{src, dst, count, edge_archetype}, ...],
            "breadcrumb": [{id, label}]
          }
        """
        ctx = ctx_provider()
        viz_cfg = ctx.config or {}
        tiers = (viz_cfg.get("tiers") or {})
        primary_subtypes = {
            name for name, tier in tiers.items() if tier == "primary"
        }
        archetype_kinds = ctx.archetype_kinds()
        contains_archs = {
            a for a, k in archetype_kinds.items() if k == "contains"
        }

        # Forest-root computation: BFS through `contains` narrowings
        # to find every subtype reachable from a primary subtype.
        # Primary subtypes that are reachable from ANOTHER primary
        # are excluded — they're intermediate / contained, not roots.
        # The reachability is transitive across non-primary
        # intermediates (e.g. open_spec_change → task_group →
        # proposal_task means proposal_task is excluded from root).
        contained_primaries: set[str] = set()
        try:
            from okg.substrate.catalog.modules_compose import compose_catalog
            composed = compose_catalog(ctx.deployment_dir)
            # Build adjacency: parent_subtype → list of child_subtypes
            # over contains edges only.
            adj: dict[str, set[str]] = {}
            for row in composed.narrowings:
                if row.edge_archetype not in contains_archs:
                    continue
                adj.setdefault(row.parent_subtype, set()).add(row.child_subtype)
            # For each primary subtype P, BFS over `contains` and
            # mark any OTHER primary it can reach as contained.
            for src in primary_subtypes:
                seen = {src}
                stack = [src]
                while stack:
                    cur = stack.pop()
                    for child in adj.get(cur, ()):
                        if child in seen:
                            continue
                        seen.add(child)
                        if child in primary_subtypes:
                            contained_primaries.add(child)
                        stack.append(child)
        except Exception:
            logger.debug("failed to derive contained primary subtypes", exc_info=True)

        root_subtypes = primary_subtypes - contained_primaries
        ref_archetypes = [
            a for a, k in archetype_kinds.items() if k == "references"
        ]

        pin = ctx.compatibility_generation_id
        # The set of subtypes whose count we want at root.
        bucket_subtypes = root_subtypes if root_subtypes else primary_subtypes
        if not bucket_subtypes:
            bucket_subtypes = None  # = "all subtypes"

        cross_refs: list[dict[str, Any]] = []
        cross_refs_truncated = False
        with ctx.conn() as conn:
            counts = _count_by_subtype_cached(conn, ctx)
            if bucket_subtypes:
                bucket_rows = [
                    (st, counts.get(st, 0))
                    for st in sorted(bucket_subtypes)
                    if counts.get(st, 0) > 0
                ]
            else:
                bucket_rows = [
                    (st, n) for st, n in counts.items() if n > 0
                ]
            bucket_rows.sort(key=lambda row: -row[1])
            estimated_edges = _estimated_reltuples(
                conn,
                "okg.graph_edges",
            )

            # Cross-refs are optional navigation hints. On high-cardinality
            # deployments, aggregating every reference edge blocks the root
            # navigator for tens of seconds, so return the root buckets and
            # explicitly mark the cross-ref layer truncated.
            if ref_archetypes and bucket_subtypes:
                if estimated_edges > _LARGE_GRAPH_EDGE_SCAN_THRESHOLD:
                    cross_refs_truncated = True
                else:
                    query_params = {
                        "bucket_subtypes": list(bucket_subtypes),
                        "ref_archetypes": ref_archetypes,
                    }
                    try:
                        _set_statement_timeout(conn, 2000)
                        for row in reads._graph(conn, ctx.session).query(
                            """
                            WITH live_n AS (
                              SELECT node_id, subtype
                                FROM okg.graph_nodes
                               WHERE subtype = ANY(:bucket_subtypes)
                            ),
                            live_e AS (
                              SELECT src, dst, edge_type
                                FROM okg.graph_edges
                               WHERE edge_type = ANY(:ref_archetypes)
                            )
                            SELECT ns.subtype AS src_sub,
                                   nd.subtype AS dst_sub,
                                   e.edge_type,
                                   count(*)   AS n
                              FROM live_e e
                              JOIN live_n ns ON ns.node_id = e.src
                              JOIN live_n nd ON nd.node_id = e.dst
                          GROUP BY ns.subtype, nd.subtype, e.edge_type
                            """,
                            query_params,
                        ):
                            cross_refs.append({
                                "src": row["src_sub"],
                                "dst": row["dst_sub"],
                                "edge_archetype": row["edge_type"],
                                "count": row["n"],
                            })
                    except psycopg.errors.QueryCanceled:
                        conn.rollback()
                        cross_refs_truncated = True
                    finally:
                        _reset_statement_timeout(conn)

        buckets = [
            {
                "id":      st,
                "label":   st,
                "subtype": st,
                "count":   n,
            }
            for st, n in bucket_rows
        ]

        return _msgpack_response(_envelope(
            buckets=buckets,
            cross_refs=cross_refs,
            breadcrumb=[
                {"id": ctx.deployment_name, "label": ctx.deployment_name}
            ],
            meta={
                "request_kind":       "hierarchy_root",
                "primary_subtypes":   sorted(primary_subtypes),
                "root_subtypes":      sorted(bucket_subtypes or []),
                "contained_excluded": sorted(contained_primaries),
                "containment_archs":  sorted(ctx.containment_archetypes()),
                "cross_refs_truncated": cross_refs_truncated,
                "estimated_edge_rows": estimated_edges,
                "generation_id":      pin,
            },
        ))

    def hierarchy_tree(
        max_depth: int = Query(2, ge=1, le=4),
        max_nodes: int = Query(5000, ge=10, le=200000),
        hierarchy: str = Query("containment",
            description="hierarchy mode: containment | subtype | attribute:<attr_name>"),
    ) -> Response:
        """Return the hierarchy tree, lazily picking the grouping
        strategy. Default `containment` walks contains-edges from
        forest roots. `subtype` groups every node by its subtype
        (one bucket per subtype, every node a depth-1 leaf).
        `attribute:<name>` groups by the value of a node attribute
        (e.g. `attribute:status`, `attribute:source_module`).
        """
        ctx = ctx_provider()
        viz_cfg = ctx.config or {}
        tiers = (viz_cfg.get("tiers") or {})
        # Secondary subtypes also participate as forest-root candidates:
        # things like `document`, `source_file`, `git_commit` should
        # be visible at bird's-eye, not silently dropped because they
        # aren't `primary`. Both tiers are eligible roots, with
        # absorbed-by-tier handling unchanged.
        rootable_subtypes = {
            n for n, t in tiers.items() if t in ("primary", "secondary")
        }
        # Fallback for deployments without tier config (e.g. af_pubs):
        # treat every subtype seen in nodes_live as rootable. Keeps
        # the bird's-eye useful even when tiers haven't been authored.
        if not rootable_subtypes:
            with ctx.conn() as conn:
                rootable_subtypes = {
                    r["subtype"]
                    for r in reads._graph(conn, ctx.session).query(
                        """
                        SELECT DISTINCT subtype
                          FROM okg.graph_nodes
                        """,
                    )
                    if r["subtype"]
                }
        sequence_attrs: dict[str, str] = viz_cfg.get("sequence_attrs") or {}

        archetype_kinds = ctx.archetype_kinds()
        contains_archs = {a for a, k in archetype_kinds.items() if k == "contains"}

        # Non-default hierarchy modes — short-circuit and return a
        # 2-level (subtype/attribute) or multi-level (nested) tree.
        if hierarchy == "subtype" or hierarchy.startswith("attribute:"):
            return _hierarchy_tree_grouped(
                ctx, hierarchy, max_nodes, sequence_attrs,
            )
        if hierarchy.startswith("nested:"):
            # nested:subtype,attr1,attr2 → multi-level grouping. Each
            # level is one attribute (or `subtype`). Leaves are the
            # actual nodes. Used by af_pubs for OPR → Series → Doc.
            spec = hierarchy[len("nested:"):]
            keys = [k.strip() for k in spec.split(",") if k.strip()]
            return _hierarchy_tree_nested(
                ctx, keys, max_nodes, sequence_attrs,
            )

        # Forest roots: rootable subtypes that aren't transitively
        # contained by another rootable subtype. Same logic as
        # /hierarchy/root, but over primary ∪ secondary.
        contained_rootables: set[str] = set()
        try:
            from okg.substrate.catalog.modules_compose import compose_catalog
            composed = compose_catalog(ctx.deployment_dir)
            adj_subtype: dict[str, set[str]] = {}
            for row in composed.narrowings:
                if row.edge_archetype not in contains_archs:
                    continue
                adj_subtype.setdefault(row.parent_subtype, set()).add(row.child_subtype)
            for src in rootable_subtypes:
                seen = {src}
                stack = [src]
                while stack:
                    cur = stack.pop()
                    for child in adj_subtype.get(cur, ()):
                        if child in seen:
                            continue
                        seen.add(child)
                        if child in rootable_subtypes:
                            contained_rootables.add(child)
                        stack.append(child)
        except Exception:
            logger.debug("failed to derive contained rootable subtypes", exc_info=True)
        root_subtypes = rootable_subtypes - contained_rootables
        if not root_subtypes:
            root_subtypes = rootable_subtypes
        # Exclude absorbed subtypes from forest-roots: those should
        # appear inside their parent's disc, not as their own root.
        # Critical for af_pubs where `section` (61K) was being emitted
        # as a forest-root despite being absorbed under publication.
        absorbed_map = ctx.absorbed_subtypes()
        root_subtypes = {s for s in root_subtypes if s not in absorbed_map}

        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        node_ids_seen: set[str] = set()
        capped = False

        # Depth 0: forest-root subtype buckets.
        if root_subtypes:
            with ctx.conn() as conn:
                for row in reads._graph(conn, ctx.session).query(
                    """
                    SELECT subtype, count(*) AS n
                      FROM okg.graph_nodes
                     WHERE subtype = ANY(:root_subtypes)
                  GROUP BY subtype
                  ORDER BY n DESC
                    """,
                    {"root_subtypes": list(root_subtypes)},
                ):
                    st, n = row["subtype"], row["n"]
                    nodes.append({
                        "id":      st,
                        "label":   st,
                        "parent":  None,
                        "depth":   0,
                        "count":   n,
                        "subtype": st,
                    })
                    node_ids_seen.add(st)

        # Depth 1: every instance whose subtype is a forest root.
        # Strip heavy attrs (body_text, pdf_url, etc.) — for the bird's-
        # eye we only need label + the seq attr. Pulling full attrs from
        # 9380 af_pubs publications transfers ~470MB of body_text we
        # never use and dominates the response time. Inspector loads
        # full node details on demand via /node/{id}.
        if max_depth >= 1 and root_subtypes:
            with ctx.conn() as conn:
                for row in reads._graph(conn, ctx.session).query(
                    """
                    SELECT node_id, subtype,
                           (attrs - ARRAY[
                             'body_text','body_text_md','body','text',
                             'pdf_url','local_pdf_path','pdf_bytes',
                             'raw','content','html'
                           ])::jsonb AS attrs,
                           COALESCE(
                             attrs->>'label',
                             attrs->>'title',
                             attrs->>'name',
                             attrs->>'subject',
                             attrs->>'summary',
                             node_id
                           ) AS lbl
                      FROM okg.graph_nodes
                     WHERE subtype = ANY(:root_subtypes)
                  ORDER BY subtype, lbl
                    """,
                    {"root_subtypes": list(root_subtypes)},
                ):
                    nid, sub, attrs, lbl = (
                        row["node_id"], row["subtype"], row["attrs"], row["lbl"],
                    )
                    if len(nodes) >= max_nodes:
                        capped = True
                        break
                    if nid in node_ids_seen:
                        continue
                    seq_attr = sequence_attrs.get(sub)
                    seq_val = attrs.get(seq_attr) if (seq_attr and isinstance(attrs, dict)) else None
                    nodes.append({
                        "id":      nid,
                        "label":   lbl,
                        "parent":  sub,
                        "depth":   1,
                        "count":   1,
                        "subtype": sub,
                        "seq":     seq_val,
                    })
                    node_ids_seen.add(nid)

        # add-compound-graph-with-semantic-zoom §1: which child
        # subtypes are absorbed into their parent? Absorbed children
        # don't appear as top-level nodes — they go into their
        # parent's `absorbed_children` block with deterministic
        # in-parent positions.
        absorbed_subtypes = ctx.absorbed_subtypes()
        # Per-parent collection: parent_node_id -> list of child dicts
        absorbed_children_by_parent: dict[str, list[dict[str, Any]]] = {}

        # Determine which edge archetypes carry the absorption
        # relationships. For tier-aware deployments these are the
        # `contains` archetypes; for tier-less ones (e.g. af_pubs)
        # absorption is inferred from `references`-style narrowings,
        # so we widen the walk to all narrowing archetypes that
        # connect a known absorbed pair.
        walk_archs = set(contains_archs)
        if absorbed_subtypes:
            try:
                from okg.substrate.catalog.modules_compose import compose_catalog
                composed = compose_catalog(ctx.deployment_dir)
                for row in composed.narrowings:
                    if absorbed_subtypes.get(row.child_subtype) == row.parent_subtype:
                        walk_archs.add(row.edge_archetype)
            except Exception:
                logger.debug("failed to compose absorbed subtype catalog hints", exc_info=True)

        # Depth 2+: walk via any archetype that carries absorption
        # (contains for principled deployments + the inferred
        # archetypes for tier-less ones).
        if max_depth >= 2 and walk_archs and not capped:
            current_frontier = [n["id"] for n in nodes if n["depth"] == 1]
            depth = 2
            while depth <= max_depth and current_frontier and not capped:
                next_frontier: list[str] = []
                with ctx.conn() as conn:
                    graph = reads._graph(conn, ctx.session)
                    chunk_size = 500
                    for i in range(0, len(current_frontier), chunk_size):
                        chunk = current_frontier[i:i + chunk_size]
                        rows = graph.query(
                            """
                            WITH live_e AS (
                              SELECT src, dst, edge_type
                                FROM okg.graph_edges
                               WHERE edge_type = ANY(:walk_archs)
                                 AND src = ANY(:chunk)
                            )
                            SELECT e.src, e.dst, e.edge_type, n.subtype,
                                   (n.attrs - ARRAY[
                                     'body_text','body_text_md','body','text',
                                     'pdf_url','local_pdf_path','pdf_bytes',
                                     'raw','content','html'
                                   ])::jsonb AS attrs,
                                   COALESCE(n.attrs->>'label', n.node_id) AS lbl
                              FROM live_e e
                              JOIN okg.graph_nodes n ON n.node_id = e.dst
                            """,
                            {
                                "walk_archs": list(walk_archs),
                                "chunk": list(chunk),
                            },
                        )
                        for row in rows:
                            src = row["src"]
                            dst = row["dst"]
                            sub = row["subtype"]
                            attrs = row["attrs"]
                            lbl = row["lbl"]
                            if dst in node_ids_seen:
                                continue
                            seq_attr = sequence_attrs.get(sub)
                            seq_val = attrs.get(seq_attr) if (seq_attr and isinstance(attrs, dict)) else None

                            child_entry = {
                                "id":      dst,
                                "label":   lbl,
                                "parent":  src,
                                "depth":   depth,
                                "count":   1,
                                "subtype": sub,
                                "seq":     seq_val,
                            }

                            # If this child's subtype is absorbed,
                            # collect it under its parent and don't
                            # emit as top-level. Don't recurse: its
                            # children are also absorbed-into-the-
                            # same-parent.
                            if sub in absorbed_subtypes:
                                bucket = absorbed_children_by_parent.setdefault(src, [])
                                # Per-parent cap — without it, deployments
                                # like af_pubs (avg 12 sections+tables+
                                # figures per publication, max 100s) blow
                                # response sizes and pack times. The
                                # client's bird's-eye samples down anyway.
                                if len(bucket) < 80:
                                    bucket.append(child_entry)
                                    node_ids_seen.add(dst)
                                continue

                            if len(nodes) >= max_nodes:
                                capped = True
                                break
                            nodes.append(child_entry)
                            node_ids_seen.add(dst)
                            next_frontier.append(dst)
                        if capped:
                            break
                current_frontier = next_frontier
                depth += 1

        # Hierarchical circle pack — every node gets a deterministic
        # (x, y, r) such that children pack perfectly inside their
        # parent's circle. No force layout, no convergence, no
        # randomness. (add-compound-graph-with-semantic-zoom — circle
        # pack architecture.)
        from okg.viz.circle_pack import PackNode, pack_hierarchy_balanced

        # Build the pack tree. Top of the hierarchy: a synthetic root
        # whose children are the forest-root subtype buckets. Each
        # bucket's children are its instances. Each instance's
        # children are its absorbed children.
        pack_nodes_by_id: dict[str, PackNode] = {}

        # Synthetic root that contains all forest roots.
        synth_root = PackNode(id="__root__", value=1.0)

        # Forest-root subtype buckets at depth 0.
        for root_node in [n for n in nodes if n["depth"] == 0]:
            p = PackNode(
                id=root_node["id"],
                value=max(1.0, float(root_node.get("count") or 1)),
            )
            pack_nodes_by_id[root_node["id"]] = p
            synth_root.children.append(p)

        # Depth-1 instances → children of their forest-root bucket.
        for inst in [n for n in nodes if n["depth"] == 1]:
            absorbed = absorbed_children_by_parent.get(inst["id"], [])
            # Instance's pack-value = 1 + count of its absorbed kids,
            # so larger instances claim proportionally bigger circles.
            p = PackNode(
                id=inst["id"],
                value=1.0 + float(len(absorbed)),
            )
            pack_nodes_by_id[inst["id"]] = p
            parent_pack = pack_nodes_by_id.get(inst["parent"])
            if parent_pack is not None:
                parent_pack.children.append(p)

        # Absorbed children → leaf circles inside their instance.
        for parent_id, kids in absorbed_children_by_parent.items():
            parent_pack = pack_nodes_by_id.get(parent_id)
            if parent_pack is None:
                continue
            # Sequence-aware ordering inside each subtype-bucket so
            # child circles within a subtype lay out in seq order
            # along the front chain (gives an arc / spiral of seq).
            # Sort key: subtype first, then seq value (numeric or lexical), then
            # id for stability.
            kids.sort(key=lambda c: (
                c["subtype"],
                _numeric_or_lex(c.get("seq")),
                c["id"],
            ))
            for c in kids:
                pn = PackNode(id=c["id"], value=1.0)
                pack_nodes_by_id[c["id"]] = pn
                parent_pack.children.append(pn)

        # Run the circle pack. Forest-roots at the synth_root level
        # are log-balanced so all are visible at bird's-eye even when
        # one subtree (e.g., open_spec_change with 138 deep changes)
        # is naturally 35× another (e.g., spec_capability with 15).
        pack_hierarchy_balanced(synth_root, padding=4.0, top_padding=12.0)

        # Scale-up: pack-tree positions are "small" (root r ~50–200);
        # multiply so the canvas isn't a tiny dot. SCALE chosen so a
        # 1-radius leaf circle becomes ~6 px on canvas.
        SCALE = 12.0
        for nid, pn in pack_nodes_by_id.items():
            pn.x *= SCALE
            pn.y *= SCALE
            pn.r *= SCALE

        # Write the packed positions back onto the response nodes.
        # Top-level: full (x, y, r). Absorbed children: (viz_x, viz_y)
        # ABSOLUTE (the frontend can use them directly without
        # parent-offset arithmetic).
        for n in nodes:
            pn = pack_nodes_by_id.get(n["id"])
            if pn is not None:
                n["viz_x"] = pn.x
                n["viz_y"] = pn.y
                n["viz_r"] = pn.r

        # Absorbed children — same fields, absolute positions.
        for kids in absorbed_children_by_parent.values():
            for c in kids:
                pn = pack_nodes_by_id.get(c["id"])
                if pn is not None:
                    c["viz_x"] = pn.x
                    c["viz_y"] = pn.y
                    c["viz_r"] = pn.r

        # Aggregate reference edges between parent-level nodes.
        aggregated_edges = _aggregated_edges_for_absorbed(
            ctx, absorbed_subtypes, contains_archs,
        )

        # Attach absorbed children to their parent node entry.
        for n in nodes:
            kids = absorbed_children_by_parent.get(n["id"])
            if kids:
                n["absorbed_children"] = kids

        return _msgpack_response(_envelope(
            nodes=nodes,
            edges=edges,
            aggregated_edges=aggregated_edges,
            meta={
                "request_kind":      "hierarchy_tree",
                "fetched_depth":     max_depth,
                "total_top_nodes":   len(nodes),
                "total_absorbed":    sum(len(v) for v in absorbed_children_by_parent.values()),
                "capped":            capped,
                "max_nodes":         max_nodes,
                "root_subtypes":     sorted(root_subtypes),
                "absorbed_subtypes": absorbed_subtypes,
                "generation_id":     ctx.session.generation_id,
            },
        ))

    def hierarchy_drill(
        path: str = Query(..., description="comma-separated drill path"),
        offset: int = Query(0, ge=0, description="paginate single-subtype drill"),
    ) -> Response:
        """Drill one level: children of the bucket at the path tip.

        Path semantics:
          - `<subtype>` (one segment): drill into a primary subtype.
            Returns instances if count <= cap; otherwise sub-aggregated.
          - `<subtype>,<node_id>` (two segments): drill into one
            instance. Returns its containment children grouped by
            subtype.
          - `<subtype>,<node_id>,...,<node_id>` (deeper): same idea,
            walking down the containment edges.
        """
        ctx = ctx_provider()
        segments = [s for s in path.split(",") if s.strip()]
        if not segments:
            raise HTTPException(status_code=400, detail="empty path")
        archetype_kinds = ctx.archetype_kinds()
        contains_archs = [
            a for a, k in archetype_kinds.items() if k == "contains"
        ]

        # Build breadcrumb. First segment is always a subtype; deeper
        # segments are node IDs.
        breadcrumb = [
            {"id": ctx.deployment_name, "label": ctx.deployment_name},
            {"id": segments[0],         "label": segments[0]},
        ]
        for seg in segments[1:]:
            breadcrumb.append({"id": seg, "label": seg})

        # Single-segment drill: primary subtype → instances or sub-aggs.
        if len(segments) == 1:
            subtype = segments[0]
            with ctx.conn() as conn:
                graph = reads._graph(conn, ctx.session)
                total = graph.query(
                    """
                    SELECT count(*) AS n
                      FROM okg.graph_nodes
                     WHERE subtype = :subtype
                    """,
                    {"subtype": subtype},
                )[0]["n"] or 0

                # Always return a page of instances. ID-prefix
                # buckets were a hack — meaningless pseudo-buckets
                # like `task:add-substra…` told the user nothing.
                # Honest answer: a real page of nodes with explicit
                # offset / page_size pagination.
                buckets = [
                    {
                        "id":      r["node_id"],
                        "label":   r["lbl"],
                        "subtype": subtype,
                        "count":   1,
                    }
                    for r in graph.query(
                        """
                        SELECT node_id,
                               COALESCE(
                                 attrs->>'label',
                                 attrs->>'title',
                                 attrs->>'name',
                                 attrs->>'subject',
                                 attrs->>'summary',
                                 node_id
                               ) AS lbl
                          FROM okg.graph_nodes
                         WHERE subtype = :subtype
                      ORDER BY lbl
                         LIMIT :lim OFFSET :off
                        """,
                        {
                            "subtype": subtype,
                            "lim": _HIERARCHY_INSTANCE_CAP,
                            "off": offset,
                        },
                    )
                ]

            return _msgpack_response(_envelope(
                buckets=buckets,
                cross_refs=[],
                breadcrumb=breadcrumb,
                meta={
                    "request_kind":  "hierarchy_drill",
                    "path":          segments,
                    "total":         total,
                    "offset":        offset,
                    "page_size":     _HIERARCHY_INSTANCE_CAP,
                    "has_more":      offset + len(buckets) < total,
                    "generation_id": ctx.session.generation_id,
                },
            ))

        # Multi-segment drill: tip is a node id. Children = nodes
        # reachable via one containment edge from the tip.
        focal_id = segments[-1]
        if not contains_archs:
            return _msgpack_response(_envelope(
                buckets=[],
                cross_refs=[],
                breadcrumb=breadcrumb,
                meta={
                    "request_kind":  "hierarchy_drill",
                    "path":          segments,
                    "reason":        "no containment archetypes registered",
                    "generation_id": ctx.session.generation_id,
                },
            ))

        with ctx.conn() as conn:
            child_rows = reads._graph(conn, ctx.session).query(
                """
                WITH live_e AS (
                  SELECT src, dst, edge_type
                    FROM okg.graph_edges
                   WHERE edge_type = ANY(:contains_archs)
                )
                SELECT n.node_id,
                       n.subtype,
                       COALESCE(n.attrs->>'label', n.node_id) AS lbl
                  FROM live_e e
                  JOIN okg.graph_nodes n ON n.node_id = e.dst
                 WHERE e.src = :focal_id
              ORDER BY n.subtype, lbl
                 LIMIT :lim
                """,
                {
                    "contains_archs": list(contains_archs),
                    "focal_id": focal_id,
                    "lim": _HIERARCHY_INSTANCE_CAP,
                },
            )

        buckets = [
            {
                "id":      r["node_id"],
                "label":   r["lbl"],
                "subtype": r["subtype"],
                "count":   1,
            }
            for r in child_rows
        ]

        return _msgpack_response(_envelope(
            buckets=buckets,
            cross_refs=[],
            breadcrumb=breadcrumb,
            meta={
                "request_kind":  "hierarchy_drill",
                "path":          segments,
                "child_count":   len(buckets),
                "generation_id": ctx.session.generation_id,
            },
        ))

    def overview_group_drill(
        subtype: str = Query(...),
        attr_key: str | None = Query(None),
        attr_value: str | None = Query(None),
        limit: int = Query(100, ge=1, le=_MAX_SAMPLE_LIMIT),
    ) -> dict[str, Any]:
        """Drill into one overview group: top-N nodes by degree
        within (subtype, attr_key=attr_value). Returns the same JSON
        shape as /types/{type}/sample so the canvas can ingest it
        directly."""
        ctx = ctx_provider()

        # Build SQL — degree = count of graph_edges referencing the
        # node as src or dst at the pin.
        sql = """
        WITH live AS (
          SELECT node_id, subtype, attrs
            FROM okg.graph_nodes
           WHERE subtype = :subtype
        )
        SELECT n.node_id,
               n.subtype,
               n.attrs,
               COALESCE(d.deg, 0) AS deg
          FROM live n
          LEFT JOIN (
            SELECT node_id, count(*)::bigint AS deg
              FROM (
                SELECT src AS node_id FROM okg.graph_edges
                UNION ALL
                SELECT dst AS node_id FROM okg.graph_edges
              ) e
              GROUP BY node_id
          ) d ON d.node_id = n.node_id
        """
        params: dict[str, Any] = {"subtype": subtype}
        if attr_key:
            if attr_value == "(none)" or attr_value is None:
                # Match nodes missing the attribute or with null value.
                sql += " WHERE (n.attrs ->> :attr_key) IS NULL"
                params["attr_key"] = attr_key
            else:
                sql += " WHERE (n.attrs ->> :attr_key) = :attr_value"
                params["attr_key"] = attr_key
                params["attr_value"] = attr_value
        sql += " ORDER BY deg DESC LIMIT :limit"
        params["limit"] = limit

        with ctx.conn() as conn:
            rows = reads._graph(conn, ctx.session).query(sql, params)

        nodes: list[dict[str, Any]] = []
        for r in rows:
            attrs = without_producer_text(r["attrs"] or {})
            nodes.append({
                "id": r["node_id"],
                "type": r["subtype"],
                "label": attrs.get("label") or attrs.get("title") or r["node_id"],
                "degree": int(r["deg"] or 0),
                "attrs": attrs,
            })

        return _envelope(
            nodes=nodes,
            edges=[],
            meta={
                "request_kind": "overview_group_drill",
                "subtype": subtype,
                "attr_key": attr_key,
                "attr_value": attr_value,
                "limit": limit,
                "returned_nodes": len(nodes),
            },
        )

    def ontology_graph() -> Response:
        """Meta-graph of the ontology — one node per subtype, one
        edge per narrowing. Msgpack envelope."""
        ctx = ctx_provider()
        with ctx.conn() as conn:
            subtypes_meta = reads.fetch_subtypes(conn, ctx.session)
            narrowings = reads.fetch_edge_narrowings(conn, ctx.session)
            counts = _count_by_subtype_cached(conn, ctx)
            samples = _sample_by_subtype(
                conn, ctx, [s["name"] for s in subtypes_meta], counts,
            )

        nodes: list[dict[str, Any]] = []
        for s in subtypes_meta:
            metadata = s.get("metadata") or {}
            subtype = s["name"]
            count = int(counts.get(subtype, 0) or 0)
            sample = samples.get(subtype)
            nodes.append({
                "id": subtype,
                "type": "subtype",
                "label": subtype,
                "archetype": metadata.get("archetype", ""),
                "description": metadata.get("description", ""),
                "fields": metadata.get("fields", []),
                "count": count,
                "sample": sample,
                "sample_id": sample,
                "transient": metadata.get("transient", False),
                "has_live_instances": count > 0,
                "schema_only": count == 0,
                "attrs": {
                    "archetype": metadata.get("archetype", ""),
                    "description": metadata.get("description", ""),
                    "fields": metadata.get("fields", []),
                    "sample_id": sample,
                    "transient": metadata.get("transient", False),
                    "has_live_instances": count > 0,
                    "schema_only": count == 0,
                },
            })
        edges: list[dict[str, Any]] = []
        for n in narrowings:
            edges.append({
                "source": n["src_subtype"],
                "target": n["dst_subtype"],
                "type": n["edge_type"],
            })
        payload = _envelope(
            nodes=nodes,
            edges=edges,
            meta={
                "request_kind": "ontology_graph",
                "returned_nodes": len(nodes),
                "returned_edges": len(edges),
                "subtype_count": len(nodes),
                "narrowing_count": len(edges),
                "zero_live_subtype_count": sum(
                    1 for node in nodes if not node["has_live_instances"]
                ),
                "sampled_subtype_count": len(samples),
            },
        )
        return _msgpack_response(payload)

    def traces() -> dict[str, Any]:
        """MCP trace listing — backed by `okg.mcp_trace_events`.

        Returns every recent bounded query run, including introspection-only
        and failed sessions.  Identity is the composite of process session and
        question label; graph/error counts are aggregated at that same grain.
        Since current MCP clients do not stamp question labels, the response
        also carries a human summary derived from the first meaningful
        recorded request and labels that provenance explicitly.
        """
        ctx = ctx_provider()
        items: list[dict[str, Any]] = []
        # One grouped read avoids the old label-loss bug: graph calls, failures,
        # duration, sample request, branch, and generation all use the exact
        # (session_id, deployment_name, question_label) identity.
        query_sql = """
            SELECT
              session_id,
              deployment_name,
              question_label,
              min(event_time) AS first_call_at,
              max(event_time) AS last_call_at,
              count(*) AS tool_call_count,
              count(DISTINCT tool_name) AS unique_tool_count,
              count(*) FILTER (WHERE error_text IS NOT NULL) AS error_count,
              COALESCE(sum(duration_ms), 0) AS total_duration_ms,
              count(*) FILTER (WHERE error_text IS NULL AND (
                CASE WHEN jsonb_typeof(response_json->'neighbors') = 'array'
                  THEN jsonb_array_length(response_json->'neighbors') ELSE 0 END > 0
                OR CASE WHEN jsonb_typeof(response_json->'results') = 'array'
                  THEN jsonb_array_length(response_json->'results') ELSE 0 END > 0
                OR CASE WHEN jsonb_typeof(response_json->'candidates') = 'array'
                  THEN jsonb_array_length(response_json->'candidates') ELSE 0 END > 0
                OR CASE WHEN jsonb_typeof(response_json->'rows') = 'array'
                  THEN jsonb_array_length(response_json->'rows') ELSE 0 END > 0
                OR (tool_name IN ('inspect', 'get_node') AND (
                    request_json->'kwargs'->'target'->>'kind' = 'node'
                    OR request_json->'kwargs' ? 'node_id'))
              )) AS graph_call_count,
              (array_agg(tool_name ORDER BY
                CASE
                  WHEN NULLIF(request_json->'kwargs'->>'query', '') IS NOT NULL THEN 0
                  WHEN NULLIF(request_json->'kwargs'->>'subtype_filter', '') IS NOT NULL THEN 1
                  WHEN COALESCE(
                    NULLIF(request_json->'kwargs'->'target'->>'kind', ''),
                    NULLIF(request_json->'kwargs'->>'node_id', '')
                  ) IS NOT NULL THEN 2 ELSE 3 END,
                event_time ASC, event_id ASC
              ))[1] AS sample_tool,
              (array_agg(request_json ORDER BY
                CASE
                  WHEN NULLIF(request_json->'kwargs'->>'query', '') IS NOT NULL THEN 0
                  WHEN NULLIF(request_json->'kwargs'->>'subtype_filter', '') IS NOT NULL THEN 1
                  WHEN COALESCE(
                    NULLIF(request_json->'kwargs'->'target'->>'kind', ''),
                    NULLIF(request_json->'kwargs'->>'node_id', '')
                  ) IS NOT NULL THEN 2 ELSE 3 END,
                event_time ASC, event_id ASC
              ))[1] AS sample_request,
              (array_agg(branch_name ORDER BY event_time DESC, event_id DESC))[1]
                AS branch_name,
              (array_agg(generation_id ORDER BY event_time DESC, event_id DESC)
                FILTER (WHERE generation_id IS NOT NULL))[1] AS generation_id,
              count(*) OVER () AS available_count
            FROM okg.mcp_trace_events
            WHERE deployment_name = %s
            GROUP BY session_id, deployment_name, question_label
            ORDER BY max(event_time) DESC
            LIMIT %s
        """
        available_count = 0
        try:
            with ctx.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query_sql,
                        (ctx.deployment_name, _QUERY_TRACE_LIMIT),
                    )
                    for row in cur.fetchall():
                        (
                            sid, dep, q_label, first, last, count,
                            unique_tool_count, error_count, total_duration_ms,
                            graph_count, sample_tool, sample_request,
                            branch_name, generation_id, total_available,
                        ) = row
                        available_count = int(total_available or 0)
                        summary, label_source = _query_trace_summary(
                            q_label,
                            sample_tool,
                            sample_request,
                        )
                        status = (
                            "failed"
                            if count and error_count == count
                            else "partial"
                            if error_count
                            else "ok"
                        )
                        items.append({
                            "query_id": _query_trace_id(str(sid), q_label),
                            "session_id": sid,
                            "deployment_name": dep,
                            "question_label": q_label,
                            "query_summary": summary,
                            "label_source": label_source,
                            "status": status,
                            "first_call_at": (
                                first.isoformat() if first else None
                            ),
                            "last_call_at": (
                                last.isoformat() if last else None
                            ),
                            "tool_call_count": count,
                            "unique_tool_count": unique_tool_count,
                            "error_count": error_count,
                            "total_duration_ms": total_duration_ms,
                            "graph_call_count": graph_count,
                            "branch_name": branch_name or "default",
                            "generation_id": generation_id,
                        })
        except psycopg.errors.UndefinedTable:
            # This deployment has never recorded MCP traces. A legitimate empty,
            # but SIGNALED (trace_tables_present=False) so the caller can tell
            # it apart from "queried fine, zero sessions".
            logger.info(
                "/traces: no trace tables on deployment %s",
                ctx.deployment_name,
            )
            return _envelope(
                meta={
                    "request_kind": "traces",
                    "count": 0,
                    "trace_tables_present": False,
                },
                items=[],
            )
        except Exception as exc:
            # A real backend failure (connection loss, permission, query
            # error) MUST surface loudly. Returning an empty list here would
            # masquerade as "no traces" and hide a broken deployment — the
            # default-as-fallback P0 this endpoint used to commit.
            logger.exception("/traces: trace query failed")
            raise HTTPException(
                status_code=500,
                detail=f"trace listing failed: {type(exc).__name__}",
            ) from exc
        return _envelope(
            meta={
                "request_kind": "traces",
                "count": len(items),
                "available_count": available_count,
                "limit": _QUERY_TRACE_LIMIT,
                "truncated": available_count > len(items),
                "trace_tables_present": True,
                "deployment_name": ctx.deployment_name,
            },
            items=items,
        )

    # ---------- workflow traces (telemetry span timelines) ----------------
    # The query-trace endpoint above reads okg.mcp_trace_events (MCP tool
    # calls). This one reads the UNIFIED telemetry store (okg.telemetry_traces /
    # okg.telemetry_spans) so the operator can see PIPELINE workflow executions
    # — source_run, publisher — as a list of recent runs, not just queries.
    def workflows(
        order: str = Query("richness", pattern="^(richness|recent)$"),
        limit: int = Query(50, ge=1, le=100),
    ) -> dict[str, Any]:
        """Workflow traces from telemetry, with an explicit bounded ordering.

        Honest empties: if the telemetry tables are absent (a deployment that
        never recorded spans), signal workflow_tables_present=False rather than
        500. A real query failure surfaces loudly — no default-as-fallback.

        Home requests ``recent`` so its operational activity list is
        chronological instead of being biased by span count. ``richness``
        remains the default ordering and stays exercised by the route tests;
        the Trace view that used to request it was removed in issue #1924.
        """
        ctx = ctx_provider()
        kinds = list(workflow_projection.WORKFLOW_ROOT_KINDS)
        # DBOS workflow roots are the only durable traces that are written at
        # start and finalized in place. Home needs them for its chronological
        # "Running now" read; the default ``richness`` ordering stays focused
        # on source/publisher executions.
        if order == "recent":
            kinds.append("dbos_workflow")
        order_sql = (
            "t.started_at DESC, span_count DESC"
            if order == "recent"
            else "span_count DESC, t.started_at DESC"
        )
        sql = (
            "SELECT t.trace_id, t.root_name, t.root_kind, "
            "       CASE WHEN t.root_kind = 'dbos_workflow' "
            "                  AND t.status_code = 'unset' "
            "            THEN 'running' ELSE t.status_code END AS status_code, "
            "       t.duration_ms, t.started_at, t.generation_id, t.workflow_id, "
            "       (SELECT count(*) FROM okg.telemetry_spans s "
            "        WHERE s.deployment_name = t.deployment_name "
            "          AND s.trace_id = t.trace_id) AS span_count "
            "FROM okg.telemetry_traces t "
            "WHERE t.deployment_name = %s AND t.root_kind = ANY(%s) "
            f"ORDER BY {order_sql} LIMIT %s"
        )
        items: list[dict[str, Any]] = []
        try:
            with ctx.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (ctx.deployment_name, kinds, limit))
                    cols = [c.name for c in cur.description]
                    for row in cur.fetchall():
                        projected_row = dict(zip(cols, row))
                        projected = workflow_projection.project_workflow_list_item(
                            projected_row
                        )
                        # ``running`` is a Home-only lifecycle state derived by
                        # this endpoint; the shared workflow projection's
                        # historical trace vocabulary remains unchanged.
                        if projected_row.get("status_code") == "running":
                            projected["status"] = "running"
                        items.append(projected)
        except psycopg.errors.UndefinedTable:
            logger.info(
                "/workflows: no telemetry tables on %s", ctx.deployment_name
            )
            return _envelope(
                meta={
                    "request_kind": "workflows",
                    "order": order,
                    "limit": limit,
                    "count": 0,
                    "workflow_tables_present": False,
                },
                items=[],
            )
        except Exception as exc:
            logger.exception("/workflows: query failed")
            raise HTTPException(
                status_code=500,
                detail=f"workflow listing failed: {type(exc).__name__}",
            ) from exc
        return _envelope(
            meta={
                "request_kind": "workflows",
                "order": order,
                "limit": limit,
                "count": len(items),
                "workflow_tables_present": True,
                "root_kinds": kinds,
            },
            items=items,
        )

    # ---------- publication PDF byte-streaming ---------------------------
    # Spike for viz-pdf-annotations: the source-tab renderer for the
    # `publication` subtype iframes the vendored PDF.js viewer pointed at
    # this endpoint. The PDF lives on local disk (cached by the af_pubs
    # `pdf_text` source in attrs.local_pdf_path); we just stream the bytes.
    def pub_pdf(id: str) -> FileResponse:
        ctx = ctx_provider()
        with ctx.conn() as conn:
            n = reads.fetch_node(conn, ctx.session, id)
        if n is None:
            raise HTTPException(status_code=404, detail=f"node {id!r} not found")
        if n.type != "publication":
            raise HTTPException(
                status_code=403,
                detail=f"node {id!r} is subtype {n.type!r}, not publication",
            )
        local = (n.attrs or {}).get("local_pdf_path")
        if not local:
            raise HTTPException(
                status_code=404,
                detail=f"publication {id!r} has no local_pdf_path",
            )
        # `local_pdf_path` is a graph attribute (source-controlled). Resolve it
        # inside an explicitly-configured approved corpus root (realpath +
        # symlink-followed containment) before serving, so a tampered absolute
        # path / traversal / symlink cannot read arbitrary local files (per
        # constrain-graph-file-and-parser-ingestion). No approved root configured
        # is a loud misconfiguration, never a silent serve of an arbitrary path.
        roots = _approved_pdf_roots(ctx)
        if not roots:
            raise HTTPException(
                status_code=500,
                detail=(
                    "viz misconfiguration: viz.approved_pdf_corpus_roots is not "
                    "configured; refusing to serve graph-controlled PDF paths"
                ),
            )
        resolved = resolve_within_approved_root(local, roots)
        if resolved is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"publication {id!r} pdf is not available within an approved "
                    "corpus root"
                ),
            )
        return FileResponse(
            resolved,
            media_type="application/pdf",
            filename=resolved.name,
        )

    # ---------- publication annotations index ---------------------------
    # Read-only list of `publication_annotation` nodes pointing at the
    # given publication via the `annotation_annotates_publication`
    # narrowing. Sorted page asc, then bbox.y desc (top-down per page
    # in PDF user-space, where y origin is bottom-left).
    #
    # Optional `?page=N` filter for the overlay layer's per-page lazy
    # fetch. Empty list when no annotations exist (NOT 404 — absence
    # of annotations is a normal state, distinct from "publication
    # missing").
    def pub_annotations(
        id: str,
        page: int | None = Query(None, ge=1),
    ) -> dict[str, Any]:
        ctx = ctx_provider()
        with ctx.conn() as conn:
            pub = reads.fetch_node(conn, ctx.session, id)
            if pub is None:
                raise HTTPException(
                    status_code=404, detail=f"node {id!r} not found",
                )
            if pub.type != "publication":
                raise HTTPException(
                    status_code=403,
                    detail=(
                        f"node {id!r} is subtype {pub.type!r}, "
                        "not publication"
                    ),
                )
            # Inbound `references` edges from publication_annotation
            # nodes. The substrate's edges_live.edge_type column stores
            # the edge ARCHETYPE, not the narrowing name — so we filter
            # by archetype + the joined source subtype, which is
            # equivalent to selecting the `annotation_annotates_publication`
            # narrowing (the only `publication_annotation -> publication`
            # references narrowing in the catalog).
            rows = reads._graph(conn, ctx.session).query(
                """
                SELECT n.node_id, n.attrs
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes n
                    ON n.node_id = e.src
                   AND n.subtype = 'publication_annotation'
                 WHERE e.dst = :pub_id
                   AND e.edge_type = 'references'
                """,
                {"pub_id": id},
            )

        items: list[dict[str, Any]] = []
        for row in rows:
            node_id = row["node_id"]
            attrs = row["attrs"]
            a = attrs or {}
            try:
                a_page = int(a.get("page")) if a.get("page") is not None else None
            except (TypeError, ValueError):
                a_page = None
            if page is not None and a_page != page:
                continue
            # bbox is JSON-encoded `[x, y, w, h]` per the views_annotations
            # ontology. Pass through as-is; the overlay layer parses.
            items.append({
                "annotation_id": a.get("annotation_id") or node_id,
                "node_id": node_id,
                "pub_id": a.get("pub_id") or id,
                "page": a_page,
                "bbox": a.get("bbox"),
                "kind": a.get("kind"),
                "severity": a.get("severity"),
                "title": a.get("title"),
                "body": a.get("body"),
                "confidence": a.get("confidence"),
                "source_id": a.get("source_id"),
                "created_at": a.get("created_at"),
            })

        # Sort: page asc, then bbox.y desc (top of page first).
        def _y_desc_key(item: dict[str, Any]) -> tuple[int, float]:
            p = item.get("page") or 0
            bb = item.get("bbox")
            y = 0.0
            if isinstance(bb, list) and len(bb) >= 2:
                try:
                    y = float(bb[1])
                except (TypeError, ValueError):
                    y = 0.0
            elif isinstance(bb, str):
                try:
                    parsed = json.loads(bb)
                    if isinstance(parsed, list) and len(parsed) >= 2:
                        y = float(parsed[1])
                except Exception:
                    y = 0.0
            return (p, -y)

        items.sort(key=_y_desc_key)

        # Surface the pdf_text_index health signals so the overlay can
        # show a banner when annotations exist but the index they were
        # derived from is missing or in an unsupported format. The
        # `pdf_text_index` attr lands with `add-af-pubs-pdf-text-index`;
        # for now both fields default to None and the overlay treats
        # absent-meta as "fine".
        pub_attrs = pub.attrs or {}
        pdf_text_index = pub_attrs.get("pdf_text_index")
        index_format_version: str | None = None
        if isinstance(pdf_text_index, dict):
            index_format_version = pdf_text_index.get("format_version")
        return {
            "pub_id": id,
            "annotations": items,
            "meta": {
                "page_filter": page,
                "count": len(items),
                "generation_id": ctx.session.generation_id,
                "pdf_text_index_warning":
                    pub_attrs.get("pdf_text_index_warning"),
                "pdf_text_index_format_version": index_format_version,
            },
        }

    # ---------- fast pub-number → node_id resolver -----------------------
    # Optimized for the side-by-side mode trigger: the in-iframe overlay
    # body emits chips like `[open AFMAN11-246V6 ▾]` and the renderer
    # needs to land on the publication node fast. The 3-tier search
    # endpoint takes 2-31 s per pub_number (sequential scan over
    # nodes_live attrs); the catalog alias table is 0.3 ms. No
    # substring fallback here — if the alias misses, the caller
    # surfaces a loud failure.
    def resolve_pub(
        pub_number: str = Query(..., min_length=1, max_length=80),
    ) -> dict[str, Any]:
        ctx = ctx_provider()
        with ctx.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT canonical
                  FROM okg.aliases
                 WHERE alias = %(alias)s
                   AND catalog_version_id = %(catalog_version_id)s
                 LIMIT 16
                """,
                {
                    "alias": pub_number,
                    "catalog_version_id": ctx.session.catalog_version_id,
                },
            )
            canonicals = [r[0] for r in cur.fetchall()]
            if not canonicals:
                raise HTTPException(
                    status_code=404,
                    detail=f"no alias for pub_number {pub_number!r}",
                )
            # Filter to publication subtype at the live pin — multiple
            # subtypes (publication, publication_family) can alias the
            # same number; the demo wants the document.
            pub_rows = reads._graph(conn, ctx.session).query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:ids)
                   AND subtype = 'publication'
                 LIMIT 1
                """,
                {"ids": canonicals},
            )
            row = pub_rows[0] if pub_rows else None
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"alias {pub_number!r} resolves but no live "
                    "publication node matches"
                ),
            )
        return {"node_id": row["node_id"], "pub_number": pub_number}

    # ---------- portal catalog (deployment-adaptive reader) ---------------
    def portal_catalog(
        limit: int = Query(200, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Deployment-neutral reader catalog for Document mode.

        This endpoint advertises only the readable families present in
        the active generation. Hierarchical portal taxonomies are loaded
        from deployment config by `/portal/tree`.
        """
        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        sections: list[dict[str, Any]] = []
        unavailable: list[dict[str, str]] = []
        counts: dict[str, int] = {}

        def add_flat_section(
            section_id: str,
            label: str,
            source_family: str,
            items: list[dict[str, Any]],
            *,
            description: str | None = None,
            deployment_specific: bool = False,
        ) -> None:
            counts[section_id] = len(items)
            if not items:
                unavailable.append({
                    "id": section_id,
                    "label": label,
                    "reason": "no readable rows in active generation",
                })
                return
            sections.append({
                "id": section_id,
                "label": label,
                "kind": "flat",
                "source_family": source_family,
                "description": description,
                "deployment_specific": deployment_specific,
                "count": len(items),
                "items": items,
            })

        with ctx.conn() as conn:
            from okg.substrate.derived_artifacts import list_view_artifacts

            graph = reads._graph(conn, ctx.session)
            view_items = []
            for summary in list_view_artifacts(
                conn,
                deployment_name=ctx.deployment_name,
                generation_id=pin,
            )[:limit]:
                attrs = {
                    "view_id": summary.view_id,
                    "view_name": summary.view_name or summary.view_id,
                    "last_refreshed_generation": (
                        summary.target_generation_id
                    ),
                    "freshness": summary.freshness,
                    "retired": summary.retired,
                    "manifest_uri": (
                        f"derived-artifact://manifest/{summary.manifest_id}"
                    ),
                    "manifest_hash": summary.manifest_hash,
                    "definition_id": summary.definition_id,
                    "required_slots": list(summary.required_slots),
                    "slot_names": list(summary.slot_names),
                }
                view_items.append(
                    _reader_leaf_item(
                        f"view:{summary.view_id}",
                        "view",
                        attrs,
                        source_family="view",
                    ),
                )
            add_flat_section(
                "views",
                "Views",
                "view",
                view_items,
                description="Materialized view documents",
            )

            heavy_projection = "attrs"
            for key in _READER_HEAVY_ATTRS:
                heavy_projection += f" - '{key}'"

            spec_rows = graph.query(
                f"""
                SELECT parent.node_id, parent.subtype,
                       {heavy_projection} AS attrs_light
                  FROM okg.graph_nodes AS parent
                 WHERE parent.subtype = ANY(:subtypes)
                   AND NOT (
                       COALESCE(parent.attrs->>'path',
                                parent.attrs->>'file_path', '')
                       ~* :excluded_path_re
                   )
                   AND (
                        parent.subtype NOT IN (
                            'goal', 'non_goal', 'alternative', 'risk'
                        )
                        OR (
                            parent.subtype = 'goal'
                            AND parent.attrs ? 'goal_id'
                        )
                        OR (
                            parent.subtype = 'non_goal'
                            AND parent.attrs ? 'nongoal_id'
                        )
                        OR (
                            parent.subtype = 'alternative'
                            AND parent.attrs ? 'alternative_id'
                        )
                        OR (
                            parent.subtype = 'risk'
                            AND parent.attrs ? 'risk_id'
                        )
                   )
                   AND (
                        parent.attrs ? 'body'
                     OR parent.attrs ? 'body_text'
                     OR parent.attrs ? 'markdown_body'
                     OR parent.attrs ? 'content'
                     OR parent.attrs ? 'description'
                     OR (
                         parent.attrs ? 'text'
                         AND COALESCE(parent.attrs->>'text', '') NOT IN (
                             COALESCE(parent.attrs->>'path', ''),
                             COALESCE(parent.attrs->>'file_path', ''),
                             COALESCE(parent.attrs->>'title', '')
                         )
                     )
                     OR EXISTS (
                         SELECT 1
                           FROM okg.graph_edges AS edge
                           JOIN okg.graph_nodes AS child
                             ON child.node_id = edge.dst
                          WHERE edge.src = parent.node_id
                            AND edge.edge_type = 'contains'
                            AND child.subtype = 'document_chunk'
                            AND NULLIF(child.attrs->>'text', '') IS NOT NULL
                     )
                   )
                 ORDER BY CASE parent.subtype
                            WHEN 'document' THEN 0
                            WHEN 'open_spec_change' THEN 1
                            WHEN 'proposal_task' THEN 2
                            WHEN 'spec_delta' THEN 3
                            WHEN 'design_decision' THEN 4
                            ELSE 5
                          END,
                          COALESCE(NULLIF(parent.attrs->>'path', ''),
                                   NULLIF(parent.attrs->>'title', ''),
                                   NULLIF(parent.attrs->>'label', ''),
                                   NULLIF(parent.attrs->>'name', ''),
                                   parent.node_id)
                 LIMIT :limit
                """,
                {
                    "subtypes": list(_READER_SPEC_DOC_SUBTYPES),
                    "limit": limit,
                    "excluded_path_re": _READER_EXCLUDED_PATH_RE,
                },
            )
            spec_items = [
                _reader_leaf_item(
                    row["node_id"], row["subtype"], row["attrs_light"],
                    source_family="spec_doc",
                )
                for row in spec_rows
            ]
            add_flat_section(
                "specs_docs",
                "Specs and docs",
                "spec_doc",
                spec_items,
                description="Repository docs, OpenSpec, and proposal artifacts",
            )

            source_rows = graph.query(
                f"""
                SELECT node_id, subtype, {heavy_projection} AS attrs_light
                  FROM okg.graph_nodes
                 WHERE subtype = 'source_file'
                   AND NOT (
                       COALESCE(attrs->>'path', attrs->>'file_path', '')
                       ~* :excluded_path_re
                   )
                   AND (
                        attrs ? 'body'
                     OR attrs ? 'text'
                     OR attrs ? 'body_text'
                     OR attrs ? 'content'
                     OR attrs ? 'diff'
                     OR attrs ? 'description'
                   )
                 ORDER BY COALESCE(NULLIF(attrs->>'path', ''),
                                   NULLIF(attrs->>'file_path', ''),
                                   NULLIF(attrs->>'title', ''),
                                   node_id)
                 LIMIT :limit
                """,
                {
                    "limit": limit,
                    "excluded_path_re": _READER_EXCLUDED_PATH_RE,
                },
            )
            source_items = [
                _reader_leaf_item(
                    row["node_id"], row["subtype"], row["attrs_light"],
                    source_family="source_file",
                )
                for row in source_rows
            ]
            add_flat_section(
                "source_files",
                "Source files",
                "source_file",
                source_items,
                description="Renderable source-file snapshots",
            )

            publication_count = int(
                graph.query(
                    """
                    SELECT count(*) AS n
                      FROM okg.graph_nodes
                     WHERE subtype = 'publication'
                    """,
                )[0]["n"] or 0
            )
            counts["publications"] = publication_count
            if publication_count > 0:
                pub_rows = graph.query(
                    """
                    SELECT node_id, attrs->>'pub_number' AS pub_number,
                           attrs->>'title' AS title,
                           attrs->>'status' AS status,
                           attrs->>'certified_date' AS certified_date
                      FROM okg.graph_nodes
                     WHERE subtype = 'publication'
                     ORDER BY COALESCE(NULLIF(attrs->>'pub_number', ''),
                                       NULLIF(attrs->>'title', ''),
                                       node_id)
                     LIMIT :limit
                    """,
                    {"limit": min(limit, 500)},
                )
                publication_items = [
                    _reader_leaf_item(
                        row["node_id"],
                        "publication",
                        {
                            "pub_number": row["pub_number"],
                            "title": row["title"],
                            "status": row["status"],
                            "certified_date": row["certified_date"],
                        },
                        source_family="publication",
                    )
                    for row in pub_rows
                ]
                add_flat_section(
                    "publications",
                    "Publications",
                    "publication",
                    publication_items,
                    description="Readable publication records",
                    deployment_specific=True,
                )

        first_item = None
        for section in sections:
            for item in section.get("items") or []:
                if item.get("node_id"):
                    first_item = {
                        "section_id": section["id"],
                        "node_id": item["node_id"],
                        "label": item.get("label") or item["node_id"],
                        "node_type": item.get("node_type"),
                    }
                    break
            if first_item:
                break

        return {
            "sections": sections,
            "first_item": first_item,
            "meta": {
                "pin": pin,
                "deployment": ctx.deployment_name,
                "counts": counts,
                "unavailable_source_families": unavailable,
            },
        }

    # ---------- portal tree (viz-document-portal) ------------------------
    # Returns one level of grouping at a time so the frontend can lazy-
    # expand. Taxonomies are deployment-owned in deployment.yaml under
    # `viz.portal.taxonomies`; the endpoint only executes the generic
    # attribute-tree / flat readers.
    def portal_tree(
        taxonomy: str = Query(..., min_length=1),
        path: str = Query("", description="Slash-joined ancestry"),
        limit: int = Query(500, ge=1, le=5000),
    ) -> dict[str, Any]:
        ctx = ctx_provider()
        from okg.viz.portal_taxonomy import (
            load_portal_taxonomies,
            read_portal_tree,
        )

        with ctx.conn() as conn:
            tree = read_portal_tree(
                reads._graph(conn, ctx.session),
                taxonomies=load_portal_taxonomies(ctx.deployment_name),
                taxonomy=taxonomy,
                path=path,
                limit=limit,
            )
        if tree is None:
            raise HTTPException(
                status_code=404,
                detail=f"unknown taxonomy {taxonomy!r}",
            )
        return tree

    # ---------- views index (native V1 view artifacts) ---------------
    # Returns completed native V1 view manifests at the session pin.
    # The portal's Views tab consumes this to populate the document
    # tree's third taxonomy. Manifest detail is returned via
    # `/node/view:<view_id>` and sibling endpoints below.
    def portal_views() -> dict[str, Any]:
        from okg.substrate.derived_artifacts import list_view_artifacts

        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        with ctx.conn() as conn:
            summaries = list_view_artifacts(
                conn,
                deployment_name=ctx.deployment_name,
                generation_id=pin,
            )
        items = [
            {
                "node_id": f"view:{summary.view_id}",
                "view_id": summary.view_id,
                "view_name": summary.view_name,
                "last_refreshed_generation": (
                    summary.target_generation_id
                ),
                "freshness": summary.freshness,
                "retired": summary.retired,
                "definition_id": summary.definition_id,
                "manifest_uri": (
                    f"derived-artifact://manifest/{summary.manifest_id}"
                ),
                "manifest_hash": summary.manifest_hash,
                "required_slots": list(summary.required_slots),
                "slot_names": list(summary.slot_names),
            }
            for summary in summaries
        ]
        return {"items": items, "meta": {"pin": pin}}

    # ---------- per-view output slot -------------------------------
    # Generic read endpoint for native V1 view slots. Markdown keeps
    # its dedicated `/node/view:<id>` path; this endpoint lets the
    # portal inspect additional modalities such as table JSON without
    # reintroducing compatibility graph nodes.
    def portal_view_slot(
        view_id: str = Query(...),
        slot_name: str = Query(...),
        scope_key: str | None = Query(None),
        row_key: str | None = Query(None),
        limit: int = Query(100, ge=1, le=_MAX_SAMPLE_LIMIT),
    ) -> dict[str, Any]:
        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        with ctx.conn() as conn:
            try:
                read = _read_viz_view_slot(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=view_id,
                    generation_id=pin,
                    slot_name=slot_name,
                )
            except Exception as exc:
                _raise_native_view_http_error(exc, view_id=view_id, pin=pin)
                raise AssertionError("unreachable")
            relation_preview = _relation_preview_for_slot(
                conn,
                read.slot,
                view_definition_id=read.artifact.definition_id,
                view_target_generation_id=read.artifact.target_generation_id,
                scope_key=scope_key,
                row_key=row_key,
                limit=limit,
            )
        return {
            "view_id": view_id,
            "slot_name": slot_name,
            "artifact": read.artifact.to_json(),
            "slot": read.slot.to_json(),
            "relation_preview": relation_preview,
            "meta": {"pin": pin},
        }

    # ---------- console view payloads --------------------------------
    # Canonical endpoint the supervisor-console renderers fetch from.
    # Per `back-console-views-with-akmon-views`:
    #   * `id` query param is a `view_def_id` (e.g.
    #     `okg_workspace.today_brief`).
    #   * Returns `{generation_id, payload}` where `payload`
    #     conforms to the typed TS schema for the view.
    #   * Missing view: HTTP 404 with a body naming the id.
    #   * Registered-but-unmaterialized: HTTP 503 with the
    #     latest published generation in the body.
    #   * No `summary.json` or JS composer — reads materialized
    #     fragments via `read_view_slot` only.
    #
    # The composer registry maps view_def_id -> composer
    # callable. The first instance is `okg_workspace.today_brief`;
    # follow-on views add entries here.
    def console_view(
        id: str = Query(..., description="view_def_id"),
        focus_id: str | None = Query(
            None,
            description=(
                "Optional focus node id for views that render an "
                "arbitrary subgraph (currently: okg_workspace.knowledge)."
            ),
        ),
    ) -> dict[str, Any]:
        from okg.substrate.console_views.today_brief import (
            compose_today_brief_from_view_manifest,
            compose_today_pi_scientist_from_view_manifest,
        )
        from okg.substrate.console_views.today_workspace import (
            compose_today_workspace,
        )
        from okg.substrate.console_views.decisions import (
            compose_decisions,
        )
        from okg.substrate.console_views.knowledge import (
            compose_overview,
            compose_map,
            compose_sources,
        )
        from okg.substrate.derived_artifacts import (
            ViewArtifactReadError,
        )

        # ---- Direct-SQL composers ---------------------------------
        # Some views (notably `okg_workspace.today`) project the
        # contract shape from the substrate via direct SQL rather
        # than through the view_manifest materialization pipeline.
        # Backs `req.contract-views.today-renderer-passes-contract`
        # of the rebuild-console-with-view-contracts PACT.
        if id == "okg_workspace.today":
            from okg.substrate.console_views._chronos import (  # noqa: PLC0415
                generation_published_at,
            )
            ctx = ctx_provider()
            target_gen = ctx.session.generation_id

            # Resolve live operator identity. Precedence:
            #   1. OKG_OPERATOR_EMAIL env (explicit override)
            #   2. git config user.email
            # display_name comes from OKG_OPERATOR_NAME → git
            # config user.name → email-localpart. Same path as
            # today_brief uses for its greeting.
            import os
            import subprocess
            operator_email: str | None = os.environ.get("OKG_OPERATOR_EMAIL") or None
            if not operator_email:
                try:
                    operator_email = subprocess.check_output(
                        ["git", "config", "user.email"],
                        text=True, timeout=2.0,
                    ).strip() or None
                except Exception:  # noqa: BLE001
                    operator_email = None
            operator_display_name: str | None = (
                os.environ.get("OKG_OPERATOR_NAME") or None
            )
            if not operator_display_name:
                try:
                    operator_display_name = subprocess.check_output(
                        ["git", "config", "user.name"],
                        text=True, timeout=2.0,
                    ).strip() or None
                except Exception:  # noqa: BLE001
                    operator_display_name = None

            with ctx.conn() as conn:
                # Resolve published_at for eyebrow/greeting.
                published_at = generation_published_at(conn, ctx.session)
                generated_at = (
                    published_at.isoformat() if published_at is not None else ""
                )
                try:
                    payload = compose_today_workspace(
                        conn,
                        session=ctx.session,
                        generated_at=generated_at,
                        operator_email=operator_email,
                        operator_display_name=operator_display_name,
                    )
                except Exception as exc:  # noqa: BLE001
                    # A 500 naming the composer and the exception's type.
                    # The exception's text goes to the server log only.
                    raise HTTPException(
                        status_code=500,
                        detail=console_failure_detail(
                            "today_workspace_composer_failed",
                            exc,
                            view_def_id=id,
                            generation_id=target_gen,
                        ),
                    ) from exc
            return {
                "generation_id": target_gen,
                "payload": payload,
            }

        # Direct-SQL composer for Knowledge — same pattern as the
        # others. Accepts an optional `focus_id` query param so the
        # client can recenter the Map without state on the server.
        # Knowledge — split into three dedicated views: overview,
        # map (arbitrary subgraph navigator with focus_id), sources.
        # Each is its own endpoint backed by a single-section
        # composer.
        if id in {
            "okg_workspace.overview",
            "okg_workspace.map",
            "okg_workspace.sources",
        }:
            from okg.substrate.console_views._chronos import (  # noqa: PLC0415
                generation_published_at,
            )
            from okg.substrate.console_views.knowledge import (  # noqa: PLC0415
                ConsoleSourceRegistryUnreadable,
            )
            ctx = ctx_provider()
            target_gen = ctx.session.generation_id
            with ctx.conn() as conn:
                published_at = generation_published_at(conn, ctx.session)
                generated_at = (
                    published_at.isoformat() if published_at is not None else ""
                )
                try:
                    if id == "okg_workspace.overview":
                        payload = compose_overview(
                            conn,
                            session=ctx.session,
                            generated_at=generated_at,
                        )
                    elif id == "okg_workspace.map":
                        payload = compose_map(
                            conn,
                            session=ctx.session,
                            generated_at=generated_at,
                            focus_id=focus_id,
                        )
                    else:  # okg_workspace.sources
                        payload = compose_sources(
                            conn,
                            session=ctx.session,
                            generated_at=generated_at,
                            deployment_name=ctx.deployment_name,
                        )
                except ConsoleSourceRegistryUnreadable as exc:
                    # The registered refusal, named, instead of an empty
                    # source list that reads as "nothing configured".
                    raise HTTPException(
                        status_code=500,
                        detail=console_failure_detail(
                            exc.code,
                            exc,
                            view_def_id=id,
                            generation_id=target_gen,
                            deployment=ctx.deployment_name,
                            # Which step failed and the underlying class
                            # name (never its text), so a parse error, an
                            # admission failure and an overlap differ.
                            stage=exc.data.get("stage"),
                            cause_type=exc.data.get("cause_type"),
                            # A fixed sentence per stage, never exception text.
                            fix_hint=exc.fix_hint,
                        ),
                    ) from exc
                except Exception as exc:  # noqa: BLE001
                    raise HTTPException(
                        status_code=500,
                        detail=console_failure_detail(
                            "knowledge_subview_composer_failed",
                            exc,
                            view_def_id=id,
                            generation_id=target_gen,
                        ),
                    ) from exc
            return {
                "generation_id": target_gen,
                "payload": payload,
            }

        # Direct-SQL composer for Decisions — same pattern as today.
        if id == "okg_workspace.decisions":
            ctx = ctx_provider()
            target_gen = ctx.session.generation_id
            with ctx.conn() as conn:
                from okg.substrate.console_views._chronos import (
                    generation_published_at,
                )
                pub = generation_published_at(conn, ctx.session)
                generated_at = pub.isoformat() if pub is not None else ""
                try:
                    payload = compose_decisions(
                        conn,
                        session=ctx.session,
                        generated_at=generated_at,
                    )
                except Exception as exc:  # noqa: BLE001
                    raise HTTPException(
                        status_code=500,
                        detail=console_failure_detail(
                            "decisions_composer_failed",
                            exc,
                            view_def_id=id,
                            generation_id=target_gen,
                        ),
                    ) from exc
            return {
                "generation_id": target_gen,
                "payload": payload,
            }

        # Per-view composer registry. Each composer takes
        # `(view_manifest_dict, *, generation_id, generated_at,
        # operator_name)` and returns a Pydantic model whose
        # `model_dump()` is the wire payload.
        composers = {
            "okg_workspace.today_brief": compose_today_brief_from_view_manifest,
            "okg_workspace.today_brief.pi_scientist": compose_today_pi_scientist_from_view_manifest,
        }
        if id not in composers:
            raise HTTPException(
                status_code=404,
                detail={
                    "reason": "unknown_view_def_id",
                    "view_def_id": id,
                    "known": sorted([
                        *composers,
                        "okg_workspace.today",
                        "okg_workspace.decisions",
                        "okg_workspace.overview",
                        "okg_workspace.map",
                        "okg_workspace.sources",
                    ]),
                },
            )
        compose = composers[id]

        ctx = ctx_provider()
        pin = ctx.session.generation_id
        with ctx.conn() as conn:
            # Verify the view_definition is in the catalog.
            # The error case is "operator typo'd the id but it
            # IS a real shape we support" vs. "the id is a real
            # registered view that just hasn't materialized yet".
            if not reads.view_definition_registered(
                conn, ctx.session, view_def_id=id,
                deployment=ctx.deployment_name,
            ):
                raise HTTPException(
                    status_code=404,
                    detail={
                        "reason": "view_definition_not_registered",
                        "view_def_id": id,
                        "deployment": ctx.deployment_name,
                        "hint": (
                            "add the view to "
                            "deployments/<deployment>/views_registry.yaml "
                            "and run `okg catalog load --apply`"
                        ),
                    },
                )

            # Read the materialized JSON slot (the view_manifest
            # with fragments). 503 when no completed manifest yet.
            try:
                read = _read_viz_view_slot(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=id,
                    slot_name="json",
                )
            except ViewArtifactReadError as exc:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "reason": "view_not_materialized",
                        "view_def_id": id,
                        "deployment": ctx.deployment_name,
                        "latest_published_generation_id": pin,
                        "hint": (
                            "the publisher's native view stage has not "
                            "produced a completed manifest for this "
                            "view at any published generation; verify "
                            "publisher cycles are succeeding (`okg "
                            "metrics`, `okg doctor`)"
                        ),
                    },
                ) from exc

            # Resolve `published_at` for the artifact's
            # generation so eyebrow/greeting are stable.
            target_gen = read.artifact.target_generation_id
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT published_at
                      FROM okg.graph_generations
                     WHERE generation_id = %s
                    """,
                    (target_gen,),
                )
                row = cur.fetchone()
            published_at = row[0] if row and row[0] is not None else None

        # Unwrap the inline JSON slot. `payload_value()` returns
        # the raw object the substrate stored (the
        # {view_manifest, refresh_stats, ...} dict).
        raw = read.slot.payload_value()
        if not isinstance(raw, dict):
            raise HTTPException(
                status_code=500,
                detail={
                    "reason": "slot_payload_unexpected_shape",
                    "view_def_id": id,
                    "type": type(raw).__name__,
                },
            )
        view_manifest = raw.get("view_manifest") or {}
        # `promoted_at` is the timestamp the manifest moved to
        # status=completed. Falling back to it (over `staged_at`)
        # keeps the field stable across re-reads.
        if published_at is not None:
            generated_at = published_at.isoformat()
        elif read.artifact.promoted_at is not None:
            generated_at = read.artifact.promoted_at.isoformat()
        else:
            generated_at = ""

        # Resolve operator name from runtime config. The substrate
        # does not know who's looking at the dashboard — that's a
        # presentation concern owned at the wire boundary. Order
        # of precedence: env var → git user.name → $USER → empty.
        import os
        import subprocess
        operator_name = os.environ.get("OKG_OPERATOR_NAME") or ""
        if not operator_name:
            try:
                operator_name = subprocess.check_output(
                    ["git", "config", "user.name"],
                    text=True, timeout=2.0,
                ).strip()
            except Exception:
                operator_name = ""
        if not operator_name:
            operator_name = os.environ.get("USER") or ""

        try:
            brief = compose(
                view_manifest,
                generation_id=target_gen,
                generated_at=generated_at,
                operator_name=operator_name,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=500,
                detail=console_failure_detail(
                    "composer_failed",
                    exc,
                    view_def_id=id,
                    generation_id=target_gen,
                ),
            ) from exc

        return {
            "generation_id": target_gen,
            "payload": brief.model_dump(),
        }

    def _resolve_operator_identity() -> tuple[str | None, str | None]:
        """Resolve (email, display_name) for the live operator.
        Precedence: OKG_OPERATOR_* env → git config. Shared by the
        topbar status + the today view."""
        import os
        import subprocess
        email = os.environ.get("OKG_OPERATOR_EMAIL") or None
        if not email:
            try:
                email = subprocess.check_output(
                    ["git", "config", "user.email"], text=True, timeout=2.0,
                ).strip() or None
            except Exception:  # noqa: BLE001
                email = None
        name = os.environ.get("OKG_OPERATOR_NAME") or None
        if not name:
            try:
                name = subprocess.check_output(
                    ["git", "config", "user.name"], text=True, timeout=2.0,
                ).strip() or None
            except Exception:  # noqa: BLE001
                name = None
        return email, name

    def _operator_actor_id() -> str:
        """Server-derived actor id for governance/overlay attribution. A
        caller-supplied actor_id/reviewer is NEVER honored — identity comes
        only from the server-resolved operator principal, so a request body
        cannot impersonate another operator (secure-viz-operator-routes)."""
        email, name = _resolve_operator_identity()
        ident = (email or name or "").strip()
        return f"operator:{ident}" if ident else "operator:viz"

    def console_status() -> dict[str, Any]:
        """Topbar status: live generation + freshness health +
        operator + node/edge counts."""
        from okg.substrate.console_views.status import compose_status
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        email, name = _resolve_operator_identity()
        with ctx.conn() as conn:
            try:
                payload = compose_status(
                    conn,
                    session=ctx.session,
                    deployment_name=ctx.deployment_name,
                    operator_email=email,
                    operator_display_name=name,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "status_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_pact_review(body: PactReviewBody) -> dict[str, Any]:
        """Record a PACT intent-review verdict from the console — the
        backend for Today's Approve / Request-changes controls. Wraps
        `okg pact review approve` (operations.approve_intent): decision
        'approved' records a passing intent_review, anything else
        records changes-requested. Reviewer defaults to the resolved
        operator."""
        from pathlib import Path  # noqa: PLC0415
        from okg.substrate.pact.operations import approve_intent
        from okg.substrate.errors import SubstrateError

        if body.decision not in {"approved", "changes-requested"}:
            raise HTTPException(
                status_code=422,
                detail={
                    "reason": "invalid_decision",
                    "decision": body.decision,
                    "allowed": ["approved", "changes-requested"],
                },
            )
        if body.decision == "changes-requested" and not (body.notes or "").strip():
            # Mirror the contract: request-changes requires a note.
            raise HTTPException(
                status_code=422,
                detail={
                    "reason": "note_required_for_changes_requested",
                    "change_id": body.change_id,
                },
            )
        email, name = _resolve_operator_identity()
        # Reviewer identity is server-derived only; body.reviewer is ignored so
        # a caller cannot record a verdict under another operator's name.
        reviewer = name or email or "okg-workspace-operator"
        try:
            manifest = approve_intent(
                Path.cwd(),
                body.change_id,
                reviewer=reviewer,
                decision=body.decision,
                notes=body.notes,
            )
        except SubstrateError as exc:
            # change-not-found / stale-view → surface the real reason +
            # fix_hint rather than a blank 500.
            raise HTTPException(
                status_code=409,
                detail={
                    "reason": "pact_review_rejected",
                    "change_id": body.change_id,
                    "error": str(exc),
                    "fix_hint": getattr(exc, "fix_hint", None),
                },
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=500,
                detail={
                    "reason": "pact_review_failed",
                    "change_id": body.change_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
            ) from exc
        # The newest evidence entry is the one we just appended.
        ev = manifest.evidence[-1] if manifest.evidence else None
        return {
            "ok": True,
            "change_id": body.change_id,
            "decision": body.decision,
            "reviewer": reviewer,
            "evidence_id": getattr(ev, "id", None),
            "status": getattr(ev, "status", None),
        }

    def console_subtype(
        name: str = Query(..., description="subtype to list nodes for"),
    ) -> dict[str, Any]:
        """Sample of nodes for a subtype — backs the right-rail
        subtype panel (Overview bar → node list)."""
        from okg.substrate.console_views.node_detail import (
            compose_subtype_list,
        )
        ctx = ctx_provider()
        target_gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_subtype_list(conn, session=ctx.session, subtype=name)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "subtype_list_composer_failed",
                        exc,
                        subtype=name,
                        generation_id=target_gen,
                    ),
                ) from exc
        return {"generation_id": target_gen, "payload": payload}

    def console_generations(
        limit: int = Query(12, ge=1, le=50, description="recent generations"),
        cursor: str | None = Query(
            None,
            min_length=1,
            max_length=512,
            description=(
                "the next_cursor of the previous page: this page answers with "
                "the attempts older than the checkpoint it names"
            ),
        ),
    ) -> dict[str, Any]:
        """Recent publish history with per-generation node/edge deltas +
        status. Backs the right-rail generation panel opened from the
        topbar gen pill — the operator's 'what did the last publish
        change?' surface."""
        from okg.substrate.console_views.generations import (
            UnknownGenerationCursor,
            compose_generations,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_generations(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                    limit=limit,
                    cursor=cursor,
                )
            # A cursor this history does not hold is refused by name. Falling
            # through to the 500 below would tell a client nothing it could
            # act on, and answering with page one would silently duplicate
            # every row the client already has.
            except UnknownGenerationCursor as exc:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "reason": "generations_cursor_unknown",
                        "generation_id": gen,
                        "cursor": exc.cursor,
                        "error": str(exc),
                    },
                ) from exc
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "generations_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_attempt_detail(
        request: Request,
        id: str = Query(  # noqa: A002 - the console sends `id`
            ...,
            min_length=1,
            max_length=512,
            description="the full generation (checkpoint) id of one attempt",
        ),
    ) -> dict[str, Any]:
        """Everything this deployment recorded about ONE update attempt.

        Authorized exactly like the attempts list read above -- the same
        context, the same session, no second identity -- and scoped to this
        deployment and branch by the composer's own history read, so an id
        from another deployment gets the SAME named not-found as an absent
        one. A reader must not be able to tell those two apart.

        An unknown query parameter is refused rather than ignored: FastAPI
        drops extras silently, which would let a client believe a filter it
        sent was applied. The refusal names the parameters this route accepts
        and COUNTS the unknown ones; it does not echo their names, which are
        caller-supplied text.
        """
        from okg.substrate.console_views.attempt_detail import (
            AttemptNotFound,
            MalformedAttemptId,
            compose_attempt_detail,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        accepted = ("id",)
        unknown = [key for key in request.query_params if key not in accepted]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={
                    "reason": "unknown_query_parameter",
                    "accepted": list(accepted),
                    "unknown_count": len(unknown),
                },
            )
        with ctx.conn() as conn:
            try:
                payload = compose_attempt_detail(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                    attempt_id=id,
                )
            except MalformedAttemptId as exc:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "reason": exc.reason,
                        "accepted": list(accepted),
                    },
                ) from exc
            # A CONSTANT body. It carries no id, no count and no deployment
            # name: the answer for an id from another deployment must be
            # byte-for-byte the answer for an id that never existed.
            except AttemptNotFound as exc:
                raise HTTPException(
                    status_code=404,
                    detail={"reason": "attempt_not_found"},
                ) from exc
            except Exception as exc:  # noqa: BLE001
                # The TYPE, not str(exc): a psycopg failure's message carries
                # the host, the IP and the role name. A console read serves no
                # exception text.
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "attempt_detail_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_attempts(
        range: str = Query(  # noqa: A002 - the console sends `range`
            "7d",
            description="span: 1d, 7d, 14d, 30d or 90d",
        ),
    ) -> dict[str, Any]:
        """Attempts per UTC day by outcome, over the requested span.

        Backs Home's chart. Reads the same Chronos checkpoints the generations
        panel reads, once, and buckets them in one pass. A span the console
        does not offer is refused by name rather than served as the default,
        and a store failure is named rather than drawn as a flat line of
        zeros.
        """
        from okg.substrate.console_views.attempts_by_day import (
            AttemptsRangeInvalid,
            compose_attempts_by_day,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_attempts_by_day(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                    requested_range=range,
                )
            except AttemptsRangeInvalid as exc:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "reason": "attempts_range_invalid",
                        "requested": exc.requested,
                        "accepted": list(exc.accepted),
                    },
                ) from exc
            except Exception as exc:  # noqa: BLE001
                # The TYPE and a fixed sentence, never str(exc): a database
                # error's text carries the DSN and the failing SQL, and this
                # detail is rendered verbatim on the operator's page. The full
                # exception is still raised into the server log.
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "attempts_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_sources() -> dict[str, Any]:
        """Per-source live contribution counts over the published generation.

        Live node/edge counts come from the maintained count rollup. Legacy
        fact-log totals are explicitly unavailable here because computing them
        by grouping node_facts/edge_facts is an unbounded request-time scan.
        """
        from okg.substrate.console_views.sources import compose_sources
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_sources(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "sources_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_tasks() -> dict[str, Any]:
        """Open Work: pact_change nodes grouped by lifecycle status
        (proposal / implementation / verification / ...) for the published
        generation, with real blocked / evidence-gap / next-task counts.
        Backs the Tasks view — the substrate-grounded replacement for the
        retired summary.json coordination board."""
        from okg.substrate.console_views.tasks import compose_tasks
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_tasks(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "tasks_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_agents() -> dict[str, Any]:
        """Teams/Agents: agent_session nodes for the published generation —
        the real coding agents (claude / codex) with branch, model, and live
        tool-call / turn / token / cost / denial counts. Backs the Agents
        view; the substrate-grounded replacement for the summary.json
        role_agent board."""
        from okg.substrate.console_views.agents import compose_agents
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_agents(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "agents_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_health() -> dict[str, Any]:
        """Substrate-health dashboard: the pinned Chronos generation's graph
        counts/deltas, queue depth, dead-letter count, invariants, and a
        derived verdict + per-check findings. Backs the Health view; the heavy
        okg doctor suite runs out-of-band."""
        from okg.substrate.console_views.health import (
            compose_health,
            producer_states_from_source_diagnostics,
        )
        from okg.substrate.console_views.producers import PRODUCER_LIMIT_MAX
        from okg.substrate.console_views.source_diagnostics import (
            compose_source_diagnostics,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        # The source breakdown is the producers read's own state per source,
        # composed with the arguments `console/producers` uses, so Health and
        # Sources cannot disagree about one source. Its own connection: a
        # failed read must not leave the health read's transaction aborted.
        # A failure is named in the breakdown (`status_basis` falls back to
        # the progress rule and says why), never silently replaced.
        producer_states: dict[str, Any] | None = None
        producer_states_reason: str | None = None
        with ctx.conn() as conn:
            try:
                producer_states = producer_states_from_source_diagnostics(
                    compose_source_diagnostics(
                        conn,
                        deployment_name=ctx.deployment_name,
                        deployment_dir=ctx.deployment_dir,
                        dsn=ctx.dsn,
                        session=ctx.session,
                        source_limit=PRODUCER_LIMIT_MAX,
                        run_limit=1,
                        run_scope="all",
                    )
                )
            except Exception as exc:  # noqa: BLE001 -- named in the payload
                logger.exception("/console/health: producers read failed")
                producer_states_reason = (
                    f"producers_read_failed:{type(exc).__name__}"
                )
        with ctx.conn() as conn:
            try:
                payload = compose_health(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                    producer_states=producer_states,
                    producer_states_unavailable_reason=producer_states_reason,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "health_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_decisions() -> dict[str, Any]:
        """Decision queue: pact_change awaiting a verdict (proposal /
        intent_review / changes_requested / design_review) with why + impact
        context. Backs the Decisions view — the substrate analog of the
        retired coordination decide_queue."""
        from okg.substrate.console_views.decision_queue import (
            compose_decision_queue,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_decision_queue(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "decisions_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_worktrees() -> dict[str, Any]:
        """Worktree inventory: agent_session grouped by cwd — the real git
        worktrees agents ran in, with branch(es) + session/tool-call activity.
        Backs the Worktrees view; the substrate analog of the retired
        coordination workspace_leases."""
        from okg.substrate.console_views.worktrees import compose_worktrees
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_worktrees(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "worktrees_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_source_detail(
        name: str = Query(..., description="source_name to detail"),
    ) -> dict[str, Any]:
        """Per-source sync state from okg.source_progress: the parsed poll
        summary and the source's other progress keys (watermarks,
        fingerprints) with ages. `sync_progress_recorded` says whether any
        progress row exists under this name; it is not a record count, and
        the console reads published records from `console/producers`."""
        from okg.substrate.console_views.source_detail import (
            compose_source_detail,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_source_detail(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                    source_name=name,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=500,
                    detail=console_failure_detail(
                        "source_detail_composer_failed",
                        exc,
                        source_name=name,
                        generation_id=gen,
                    ),
                ) from exc
        return {"generation_id": gen, "payload": payload}

    def console_source_diagnostics(
        name: str | None = Query(
            None,
            description="optional selected source name",
        ),
        mode: str = Query(
            "full",
            description="diagnostic depth: counts, detail, or full",
        ),
        run_limit: int = Query(
            20,
            ge=1,
            le=50,
            description="maximum recent runs per selected source",
        ),
    ) -> dict[str, Any]:
        """Source-fleet diagnostics with selected-source detail.

        Read-only composition of source contribution counts, source-sync
        readiness, source_progress detail, and recent run telemetry.
        """
        from okg.substrate.console_views.source_diagnostics import (
            compose_source_diagnostics,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        if mode not in {"counts", "detail", "full"}:
            raise HTTPException(
                status_code=400,
                detail={
                    "reason": "invalid_source_diagnostics_mode",
                    "mode": mode,
                    "expected": ["counts", "detail", "full"],
                },
            )
        with ctx.conn() as conn:
            try:
                payload = compose_source_diagnostics(
                    conn,
                    deployment_name=ctx.deployment_name,
                    deployment_dir=ctx.deployment_dir,
                    dsn=ctx.dsn,
                    session=ctx.session,
                    selected_source=name,
                    include_runtime=mode == "full",
                    run_limit=run_limit,
                    run_scope=(
                        "none" if mode == "counts"
                        else "selected" if mode == "detail"
                        else "all"
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                # The type only, below; the text goes to the server log.
                log_console_failure("source_diagnostics_composer_failed", exc)
                return {
                    "ok": False,
                    "status": 503,
                    "generation_id": gen,
                    "error": "Source diagnostics are unavailable.",
                    "reason": "source_diagnostics_composer_failed",
                    "next_check": (
                        "check the source diagnostics backend log and retry "
                        "the counts read"
                    ),
                    "failure": {
                        "route": "/viz/api/console/source-diagnostics",
                        "params": {
                            "mode": mode,
                            "name": name,
                            "run_limit": run_limit,
                        },
                        "generation_id": gen,
                        "error_type": type(exc).__name__,
                    },
                }
        return {"ok": True, "generation_id": gen, "payload": payload}

    def console_producers(
        name: str | None = Query(
            None,
            description="one producer's name; returns its row and run history",
        ),
        producer_limit: int | None = Query(
            None,
            description="maximum producers listed; refused above 200 and below 1",
        ),
        run_limit: int | None = Query(
            None,
            description="runs per page of the detail form; refused above 50 and below 1",
        ),
        run_offset: int | None = Query(
            None,
            description="how many runs of the detail form to skip",
        ),
    ) -> dict[str, Any]:
        """Every producer of the deployment, once, in one row shape.

        Sources, enrichers and views together, so the console's one producer
        table is one read. Policy posture is the same as
        `/viz/api/console/source-diagnostics`, the sibling this route is
        modelled on: the same request context and session, the same pinned
        generation, the same `{ok, generation_id, payload}` envelope and the
        same 503 body naming the composer that failed. It sits beside that
        route rather than replacing it; the old Home and Health views still
        read the old routes.

        Parameters are refused by name rather than clamped: a caller that asks
        for 10,000 runs is told no, not silently served 50.
        """
        from okg.substrate.console_views.producers import (
            ProducersParameterError,
            compose_producers,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_producers(
                    conn,
                    deployment_name=ctx.deployment_name,
                    deployment_dir=ctx.deployment_dir,
                    dsn=ctx.dsn,
                    session=ctx.session,
                    name=name,
                    producer_limit=producer_limit,
                    run_limit=run_limit,
                    run_offset=run_offset,
                )
            except ProducersParameterError as exc:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "reason": "producers_parameter_invalid",
                        "parameter": exc.parameter,
                        "detail": exc.detail,
                        "generation_id": gen,
                    },
                ) from exc
            except Exception as exc:  # noqa: BLE001
                # The type only, below; the text goes to the server log.
                log_console_failure("producers_composer_failed", exc)
                return {
                    "ok": False,
                    "status": 503,
                    "generation_id": gen,
                    "error": "The producers read is unavailable.",
                    "reason": "producers_composer_failed",
                    "next_check": (
                        "check the console producers backend log and retry "
                        "the read"
                    ),
                    "failure": {
                        "route": "/viz/api/console/producers",
                        "params": {
                            "name": name,
                            "producer_limit": producer_limit,
                            "run_limit": run_limit,
                            "run_offset": run_offset,
                        },
                        "generation_id": gen,
                        "error_type": type(exc).__name__,
                    },
                }
        return {"ok": True, "generation_id": gen, "payload": payload}

    @router.get("/console/enricher-diagnostics")
    def console_enricher_diagnostics(
        name: str | None = Query(
            None,
            description="optional selected enricher name",
        ),
        mode: str = Query(
            "full",
            description="diagnostic depth: counts, detail, or full",
        ),
        run_limit: int = Query(
            20,
            ge=1,
            le=20,
            description="maximum recent runs per selected enricher",
        ),
    ) -> dict[str, Any]:
        """Enricher diagnostics with current graph contribution and recent
        generation-run output. Read-only; backs Health producer detail rows."""
        from okg.substrate.console_views.enricher_diagnostics import (
            compose_enricher_diagnostics,
        )
        ctx = ctx_provider()
        gen = ctx.session.generation_id
        if mode not in {"counts", "detail", "full"}:
            raise HTTPException(
                status_code=400,
                detail={
                    "reason": "invalid_enricher_diagnostics_mode",
                    "mode": mode,
                    "expected": ["counts", "detail", "full"],
                },
            )
        with ctx.conn() as conn:
            try:
                payload = compose_enricher_diagnostics(
                    conn,
                    deployment_name=ctx.deployment_name,
                    session=ctx.session,
                    selected_enricher=name,
                    run_limit=run_limit,
                    run_scope=(
                        "none" if mode == "counts"
                        else "selected" if mode == "detail"
                        else "all"
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                # The type only, below; the text goes to the server log.
                log_console_failure("enricher_diagnostics_composer_failed", exc)
                return {
                    "ok": False,
                    "status": 503,
                    "generation_id": gen,
                    "error": "Enricher diagnostics are unavailable.",
                    "reason": "enricher_diagnostics_composer_failed",
                    "next_check": (
                        "check the enricher diagnostics backend log and retry "
                        "the counts read"
                    ),
                    "failure": {
                        "route": "/viz/api/console/enricher-diagnostics",
                        "params": {
                            "mode": mode,
                            "name": name,
                            "run_limit": run_limit,
                        },
                        "generation_id": gen,
                        "error_type": type(exc).__name__,
                    },
                }
        return {"ok": True, "generation_id": gen, "payload": payload}

    @router.get("/console/view-diagnostics")
    def console_view_diagnostics(
        name: str | None = Query(None, description="optional selected view id"),
        run_limit: int = Query(20, ge=1, le=50),
    ) -> dict[str, Any]:
        """Registry-complete native-view inventory and bounded run history."""
        from okg.substrate.console_views.view_diagnostics import compose_view_diagnostics

        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_view_diagnostics(
                    conn,
                    deployment_name=ctx.deployment_name,
                    deployment_dir=ctx.deployment_dir,
                    session=ctx.session,
                    selected_view=name,
                    run_limit=run_limit,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=503,
                    detail=console_failure_detail(
                        "view_diagnostics_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"ok": True, "generation_id": gen, "payload": payload}

    @router.get("/console/producer-metrics")
    def console_producer_metrics(
        producers: str = Query("", description="comma-separated producer keys"),
        metrics: str = Query("duration,output,throughput,failures,observation_lag"),
        start: datetime | None = Query(None),
        end: datetime | None = Query(None),
        period: str = Query("auto"),
        statistic: str = Query("p95"),
        max_buckets: int = Query(720, ge=1, le=720),
    ) -> dict[str, Any]:
        """Bounded source, enricher, and view metric timelines."""
        from okg.substrate.console_views.producer_metrics import (
            ProducerMetricsParameterError,
            compose_producer_metrics,
        )

        ctx = ctx_provider()
        gen = ctx.session.generation_id
        with ctx.conn() as conn:
            try:
                payload = compose_producer_metrics(
                    conn,
                    deployment_name=ctx.deployment_name,
                    deployment_dir=ctx.deployment_dir,
                    session=ctx.session,
                    producer_keys=[value for value in producers.split(",") if value],
                    metrics=[value for value in metrics.split(",") if value],
                    start=start,
                    end=end,
                    period=period,
                    statistic=statistic,
                    max_buckets=max_buckets,
                )
            # Only a refusal that names the PARAMETER is served with its
            # message; any other ValueError is a server failure like the rest
            # and serves no text (#2130).
            except ProducerMetricsParameterError as exc:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "reason": "producer_metrics_parameter_invalid",
                        "parameter": exc.parameter,
                        "error": exc.detail,
                        "generation_id": gen,
                    },
                ) from exc
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=503,
                    detail=console_failure_detail(
                        "producer_metrics_composer_failed",
                        exc,
                        generation_id=gen,
                    ),
                ) from exc
        return {"ok": True, "generation_id": gen, "payload": payload}

    def portal_lenses(
        view_id: str = Query(...),
    ) -> dict[str, Any]:
        from okg.substrate.derived_artifacts import (
            get_view_artifact_at_generation,
        )
        from okg.substrate.lenses import list_lens_definitions_for_view

        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        with ctx.conn() as conn:
            try:
                get_view_artifact_at_generation(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=view_id,
                    generation_id=pin,
                )
            except Exception as exc:
                _raise_native_view_http_error(exc, view_id=view_id, pin=pin)
            lenses = list_lens_definitions_for_view(
                conn,
                deployment_name=ctx.deployment_name,
                view_id=view_id,
            )
        return {
            "view_id": view_id,
            "lenses": [
                {
                    "definition_id": lens.definition_id,
                    "lens_id": lens.lens_id,
                    "view_definition_id": lens.view_definition_id,
                    "view_id": lens.view_id,
                    "renderer_ref": lens.renderer_ref,
                    "lens_contract": dict(lens.lens_contract),
                    "putback_policy": dict(lens.putback_policy),
                    "source_authority_policy": dict(
                        lens.source_authority_policy
                    ),
                    "hashes": {
                        "compute_hash": lens.compute_hash,
                        "output_schema_hash": lens.output_schema_hash,
                        "renderer_hash": lens.renderer_hash,
                        "dirty_rule_hash": lens.dirty_rule_hash,
                    },
                }
                for lens in lenses
            ],
            "meta": {"pin": pin},
        }

    def portal_lens_overlays(
        view_id: str | None = Query(None),
        lens_id: str | None = Query(None),
        status: str | None = Query("active"),
        limit: int = Query(100, ge=1, le=500),
    ) -> dict[str, Any]:
        """List lens overlays visible to the current pinned generation."""

        from okg.substrate.lenses import list_lens_overlays

        normalized_status = None if status in {"", "all"} else status
        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        with ctx.conn() as conn:
            overlays = list_lens_overlays(
                conn,
                deployment_name=ctx.deployment_name,
                view_id=view_id,
                lens_id=lens_id,
                status=normalized_status,
                target_generation_id=pin,
                limit=limit,
            )
        return {
            "overlays": overlays,
            "meta": {
                "pin": pin,
                "view_id": view_id,
                "lens_id": lens_id,
                "status": normalized_status,
                "limit": limit,
            },
        }

    def portal_lens_overlay_revoke(
        overlay_id: UUID,
        body: LensOverlayRevokeBody = Body(...),
    ) -> dict[str, Any]:
        """Supersede an active overlay without touching source truth."""

        from okg.substrate.lenses import (
            LensOverlayNotFound,
            revoke_lens_overlay,
        )

        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        with ctx.conn() as conn:
            try:
                overlay = revoke_lens_overlay(
                    conn,
                    overlay_id=overlay_id,
                    deployment_name=ctx.deployment_name,
                    # Server-derived actor; body.actor_id is not honored.
                    actor_id=_operator_actor_id(),
                    reason=body.reason,
                )
            except LensOverlayNotFound as exc:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "reason": "lens_overlay_not_found",
                        "overlay_id": str(overlay_id),
                        "generation_id": pin,
                    },
                ) from exc
        return {
            "overlay": overlay,
            "meta": {"pin": pin, "revoked": overlay["status"] != "active"},
        }

    def portal_lens_putback(
        body: LensPutbackRequestBody = Body(...),
    ) -> dict[str, Any]:
        """Prepare and optionally commit an exact-base lens overlay."""

        from okg.substrate.lenses import (
            LensPutbackDelta,
            commit_lens_overlay,
            load_lens_definition_for_view,
            prepare_lens_putback,
        )

        if not body.deltas:
            raise HTTPException(
                status_code=400,
                detail="at least one lens putback delta is required",
            )

        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        with ctx.conn() as conn:
            try:
                lens = load_lens_definition_for_view(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=body.view_id,
                    lens_id=body.lens_id,
                )
                base_read = _read_viz_view_slot(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=body.view_id,
                    generation_id=pin,
                    slot_name=body.deltas[0].slot_name,
                    definition_id=lens.view_definition_id,
                )
                deltas = [
                    LensPutbackDelta(
                        slot_name=delta.slot_name,
                        scope_key=delta.scope_key,
                        row_key=delta.row_key,
                        field_path=tuple(delta.field_path),
                        expected_value=delta.expected_value,
                        new_value=delta.new_value,
                        source_authority=delta.source_authority,
                        source_record_id=dict(delta.source_record_id),
                        expected_source_revision=(
                            dict(delta.expected_source_revision)
                            if delta.expected_source_revision is not None
                            else None
                        ),
                        metadata=dict(delta.metadata),
                    )
                    for delta in body.deltas
                ]
                request = prepare_lens_putback(
                    conn,
                    lens,
                    base_view_manifest_id=base_read.artifact.manifest_id,
                    target_generation_id=pin,
                    # Server-derived actor; body.actor_id is not honored.
                    actor_id=_operator_actor_id(),
                    idempotency_key=body.idempotency_key,
                    deltas=deltas,
                    metadata=dict(body.metadata),
                )
                if request.status == "conflict":
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "reason": "lens_putback_conflict",
                            "request": request.to_json(),
                        },
                    )
                if body.commit_overlay:
                    request = commit_lens_overlay(
                        conn,
                        request_id=request.request_id,
                    )
                relation_previews = {}
                for slot_name in sorted({d.slot_name for d in body.deltas}):
                    slot_read = _read_viz_view_slot(
                        conn,
                        deployment_name=ctx.deployment_name,
                        view_id=body.view_id,
                        generation_id=pin,
                        slot_name=slot_name,
                        definition_id=lens.view_definition_id,
                    )
                    relation_previews[slot_name] = _relation_preview_for_slot(
                        conn,
                        slot_read.slot,
                        view_definition_id=slot_read.artifact.definition_id,
                        view_target_generation_id=(
                            slot_read.artifact.target_generation_id
                        ),
                    )
            except HTTPException:
                raise
            except Exception as exc:
                from okg.substrate.derived_artifacts import (
                    ViewArtifactReadError,
                )
                from okg.substrate.lenses import LensPutbackError

                if isinstance(exc, ViewArtifactReadError):
                    _raise_native_view_http_error(
                        exc,
                        view_id=body.view_id,
                        pin=pin,
                    )
                    raise AssertionError("unreachable")
                if isinstance(exc, (LensPutbackError, ValueError)):
                    _raise_lens_putback_http_error(
                        exc,
                        view_id=body.view_id,
                        pin=pin,
                    )
                    raise AssertionError("unreachable")
                _raise_lens_putback_http_error(
                    exc,
                    view_id=body.view_id,
                    pin=pin,
                )
                raise AssertionError("unreachable")
        return {
            "request": request.to_json(),
            "relation_previews": relation_previews,
            "meta": {
                "pin": pin,
                "view_id": body.view_id,
                "lens_id": lens.lens_id,
                "committed": request.status == "committed_overlay",
            },
        }

    # ---------- per-view markered output --------------------------
    # Returns the markered markdown variant + per-fragment byte
    # offsets from the native V1 JSON slot.
    # Read by `view_markdown.js`'s markered-mode render path so
    # each rendered fragment's text gets wrapped in a
    # <span class="view-fragment" data-fragment-id="..."> element.
    # Returns available=false when the assembler returned plain `str`
    # and no markered artifact exists, so the browser can fall back
    # without logging an expected 404 as a console error.
    def portal_view_markered(view_id: str) -> dict[str, Any]:
        from okg.substrate.derived_artifacts import (
            ViewSlotNotFound,
        )

        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        with ctx.conn() as conn:
            try:
                json_slot = _read_viz_view_slot(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=view_id,
                    generation_id=pin,
                    slot_name="json",
                )
            except ViewSlotNotFound:
                return {
                    "view_id": view_id,
                    "markered": None,
                    "fragment_offsets": {},
                    "meta": {
                        "pin": pin,
                        "available": False,
                        "reason": "native JSON slot is not present",
                    },
                }
            except Exception as exc:
                _raise_native_view_http_error(exc, view_id=view_id, pin=pin)
                raise AssertionError("unreachable")

        payload = json_slot.slot.payload_value()
        markered = (
            payload.get("markered")
            if isinstance(payload, dict) else None
        )
        offsets = (
            payload.get("fragment_offsets")
            if isinstance(payload, dict) else None
        )
        if not isinstance(markered, str) or not markered:
            return {
                "view_id": view_id,
                "markered": None,
                "fragment_offsets": {},
                "meta": {
                    "pin": pin,
                    "available": False,
                    "reason": "native JSON slot has no markered output",
                },
            }
        return {
            "view_id": view_id,
            "markered": markered,
            "fragment_offsets": offsets if isinstance(offsets, dict) else {},
            "meta": {"pin": pin, "available": True},
        }

    # `attrs.fragment_id` so the portal's cited-nodes sidebar can
    # render them per-fragment. Reads `view_cites_node` edges
    # projected by the native V1 view publisher.
    def portal_view_cited(view_id: str) -> dict[str, Any]:
        from okg.substrate.derived_artifacts import (
            get_view_artifact_at_generation,
        )

        ctx = ctx_provider()
        pin = ctx.compatibility_generation_id
        view_node_id = f"view:{view_id}"
        with ctx.conn() as conn:
            try:
                get_view_artifact_at_generation(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=view_id,
                    generation_id=pin,
                )
            except Exception as exc:
                _raise_native_view_http_error(exc, view_id=view_id, pin=pin)
            # Cited nodes via `view_cites_node` edges. Joins
            # graph_nodes so the portal can render each target's
            # subtype + display label.
            cited_rows = reads._graph(conn, ctx.session).query(
                """
                SELECT el.dst,
                       el.attrs->>'fragment_id' AS fragment_id,
                       el.attrs->>'anchor_id' AS anchor_id,
                       nl.subtype,
                       nl.attrs->>'pub_number' AS pub_number,
                       nl.attrs->>'title' AS title
                  FROM okg.graph_edges el
             LEFT JOIN okg.graph_nodes nl
                    ON nl.node_id = el.dst
                 WHERE el.src = :view_node_id
                   AND el.edge_type = 'view_cites_node'
                 ORDER BY fragment_id, dst
                """,
                {"view_node_id": view_node_id},
            )
            citations: list[dict[str, Any]] = []
            for row in cited_rows:
                citations.append({
                    "node_id": row["dst"],
                    "fragment_id": row["fragment_id"],
                    "anchor_id": row["anchor_id"],
                    "subtype": row["subtype"],
                    "pub_number": row["pub_number"],
                    "title": row["title"],
                })
        return {
            "view_id": view_id,
            "citations": citations,
            "meta": {"pin": pin},
        }

    # ---------- view-fragment recheck (add-view-edit-source-recheck) ------
    #
    #
    # POST /viz/api/views/<view_id>/recheck
    # Body: { fragment_id, original_md, edited_md }
    # Response: { verdict, highlights[], meta }
    #
    # Loud-failure error mappings (per spec):
    #   - PdfTextIndexMissing  -> 422  pdf_text_index_missing
    #   - LLMCallFailed        -> 502  judge_call_failure
    #   - RecheckJudgeMalformed -> 502  judge_call_failure
    #   - ValueError (no cited sources) -> 404
    #
    # The recheck pipeline ITSELF does not INSERT into okg.llm_calls;
    # LiteLLMClient (when constructed with tracking_dsn=) records
    # one row per .complete() call with caller=f"recheck:{view_id}".
    def view_recheck(
        view_id: str,
        body: RecheckRequest = Body(...),
    ) -> dict[str, Any]:
        from okg.views.execution.llm import (
            LiteLLMClient,
            LLMCallFailed,
        )
        from okg.views.execution.recheck import (
            PdfTextIndexMissing,
            RecheckJudgeMalformed,
            RecheckSpendConfigInvalid,
            recheck_fragment,
        )

        ctx = ctx_provider()
        # Verify the native view artifact exists at the pin.
        pin = ctx.session.generation_id
        from okg.substrate.derived_artifacts import (
            get_view_artifact_at_generation,
        )
        with ctx.conn() as conn:
            try:
                get_view_artifact_at_generation(
                    conn,
                    deployment_name=ctx.deployment_name,
                    view_id=view_id,
                    generation_id=pin,
                )
            except Exception as exc:
                _raise_native_view_http_error(exc, view_id=view_id, pin=pin)

        # Pick the recheck model from deployment config.
        # Default is the same model af_pubs views use for
        # paraphrase generation; deployments override via
        # `viz.recheck.llm_client` (e.g., "litellm:openrouter/...").
        viz_cfg = ctx.config or {}
        recheck_cfg = (viz_cfg.get("recheck") or {})
        model = (
            recheck_cfg.get("model")
            or "openrouter/anthropic/claude-haiku-4.5"
        )
        # tracking_dsn enables per-call insertion into
        # okg.llm_calls; pass the deployment's DSN so a
        # `okg metrics --llm` rollup surfaces recheck spend.
        llm = LiteLLMClient(
            model=model,
            tracking_dsn=ctx.dsn,
            deployment=ctx.deployment_name,
        )

        try:
            with ctx.conn() as conn:
                result = recheck_fragment(
                    reads._graph(conn, ctx.session),
                    deployment_name=ctx.deployment_name,
                    view_id=view_id,
                    fragment_id=body.fragment_id,
                    original_md=body.original_md,
                    edited_md=body.edited_md,
                    llm_client=llm,
                    # Spend admission for the judge fan-out. The recheck
                    # config object owns the budget (same surface the
                    # judge model comes from); undeclared leaves the lane
                    # spend-unbounded, reported by recheck_fragment, never
                    # silently defaulted. Settlement lands on the same DSN
                    # okg.llm_calls tracking already uses, so one query
                    # answers "what did recheck spend".
                    recheck_spend_cap_usd=recheck_cfg.get(
                        "recheck_spend_cap_usd"
                    ),
                    recheck_spend_estimate_usd=recheck_cfg.get(
                        "recheck_spend_estimate_usd"
                    ),
                    spend_settlement_dsn=ctx.dsn,
                )
        except RecheckSpendConfigInvalid as exc:
            # Deployment config error, not user input and not a provider
            # failure — 500 with its own reason so the operator fixes the
            # declaration instead of hunting a missing fragment or outage.
            raise HTTPException(
                status_code=500,
                detail={
                    "reason": "recheck_spend_config_invalid",
                    "detail": str(exc),
                },
            )
        except PdfTextIndexMissing as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "reason": "pdf_text_index_missing",
                    "node_id": exc.node_id,
                    "cause": exc.reason,
                },
            )
        except RecheckJudgeMalformed as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "reason": "judge_call_failure",
                    "kind": "malformed_response",
                    "detail": exc.detail,
                },
            )
        except LLMCallFailed as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "reason": "judge_call_failure",
                    "kind": "call_failed",
                    "detail": str(exc),
                },
            )
        except ValueError as exc:
            # No cited sources for this fragment.
            raise HTTPException(status_code=404, detail=str(exc))

        return {
            "view_id": view_id,
            "fragment_id": body.fragment_id,
            "verdict": result.verdict,
            "highlights": [
                {
                    "source_node_id": h.source_node_id,
                    "page": h.page,
                    "bbox": h.bbox,
                    "line_idx": h.line_idx,
                    "severity": h.severity,
                    "rationale": h.rationale,
                }
                for h in result.highlights
            ],
            "meta": {
                "pin": pin,
                "model": model,
                "validation_warnings": result.validation_warnings,
            },
        }

    # The operator-local chat route that used to live here (a Claude Code
    # subprocess streamed as Server-Sent Events) is retired: PACT
    # retire-viz-chat, archived under archive/chat/viz-chat-2026-08-04/ (now
    # only in git, commit aa9a7eed8015). The
    # governed replacement is the chat: manifest block and the site projected
    # by okg chat sync.

    register_graph_routes(
        router,
        search=search,
        types=types,
        type_sample=type_sample,
        node=node,
        expand=expand,
        khop=khop,
        stats=stats,
        generations=generations,
        repin=repin,
    )
    register_system_routes(
        router,
        system_snapshot=system_snapshot,
        system_profile=system_profile,
        system_profile_artifact=system_profile_artifact,
    )
    register_ontology_overview_routes(
        router,
        config=config,
        ontology=ontology,
        overview=overview,
        hierarchy_root=hierarchy_root,
        hierarchy_tree=hierarchy_tree,
        hierarchy_drill=hierarchy_drill,
        overview_group_drill=overview_group_drill,
        ontology_graph=ontology_graph,
    )
    register_trace_routes(router, traces=traces, workflows=workflows)
    register_publication_routes(
        router,
        pub_pdf=pub_pdf,
        pub_annotations=pub_annotations,
        resolve_pub=resolve_pub,
    )
    register_portal_routes(
        router,
        portal_catalog=portal_catalog,
        portal_tree=portal_tree,
        portal_views=portal_views,
        portal_view_slot=portal_view_slot,
    )
    register_console_routes(
        router,
        console_view=console_view,
        console_status=console_status,
        console_pact_review=console_pact_review,
        console_subtype=console_subtype,
        console_generations=console_generations,
        console_attempt_detail=console_attempt_detail,
        console_attempts=console_attempts,
        console_sources=console_sources,
        console_tasks=console_tasks,
        console_agents=console_agents,
        console_health=console_health,
        console_decisions=console_decisions,
        console_worktrees=console_worktrees,
        console_source_detail=console_source_detail,
        console_source_diagnostics=console_source_diagnostics,
        console_producers=console_producers,
    )
    register_portal_lens_routes(
        router,
        portal_lenses=portal_lenses,
        portal_lens_overlays=portal_lens_overlays,
        portal_lens_overlay_revoke=portal_lens_overlay_revoke,
        portal_lens_putback=portal_lens_putback,
        portal_view_markered=portal_view_markered,
        portal_view_cited=portal_view_cited,
        view_recheck=view_recheck,
    )
    register_operator_record_routes(router, ctx_provider=ctx_provider)

    return router
