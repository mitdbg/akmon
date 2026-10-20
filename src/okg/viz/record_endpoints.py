"""Record read routes for the operator UI (PACT operator-ui-search-view).

Every record route names a published version explicitly. The route opens a
session pinned to that version under the operator UI's read identity
(``graph.read.local_operator``), authorizes the read at the UI's Nomos boundary
before touching the graph, and only then composes the response. There is no
"current version" fallback: a version that is not a published version of this
deployment is refused by name.

These routes are deliberately absent from the response cache allowlist
(``read_cache.CACHEABLE_PATHS``): each read must pass the policy check, so a
cached body must never answer a request the policy has not authorized.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from okg.viz.context import VizContext
from okg.viz.graph_endpoints import register_graph_routes
from okg.viz.read_cache import CLIENT_ONLY_PARAMS

logger = logging.getLogger(__name__)

#: What a server-side fault tells the browser. The exception's own text is
#: written to the log and never to the response: a psycopg error renders the
#: connection string, and a composer's error renders the failing SQL.
_OPAQUE_FAULT = (
    "this read failed inside the server; the cause is in the server log"
)

#: Headers every byte the record routes serve is sent with. Together they say:
#: do not guess a type, do not run anything, do not let another origin read it.
#: `inline` rather than `attachment` because the console DISPLAYS these images;
#: the safety comes from the media-type allowlist (raster only -- no SVG), the
#: nosniff, and a policy that permits no subresource of any kind.
_BYTE_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": "default-src 'none'; sandbox",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Content-Disposition": "inline",
    "Cache-Control": "private, no-store",
}

#: Every query parameter each record route knows, beside the client-only ones
#: every route accepts (`read_cache.CLIENT_ONLY_PARAMS` -- today `refresh`,
#: which the shipped client appends to every read of a refreshed view). A name
#: outside the union is REFUSED, not ignored: the console and this route
#: drifted apart once already -- the console sent `source`, `sort` and `since`
#: and the route read neither of the last two nor said so -- and a silently
#: dropped filter shows the reader a result set the rail claims is something
#: else.
#:
#: `refresh` does nothing on these routes: they are deliberately outside the
#: response cache, and each names its own version, so there is no entry to
#: bypass and no latest to re-resolve. Accepting it is not ignoring a filter --
#: it selects no records and changes no result set -- and refusing it would
#: answer a Refresh click with an error.
_KNOWN_PARAMS: dict[str, frozenset[str]] = {
    "search": frozenset({"version", "q", "type", "source", "limit", "offset"}),
    "types": frozenset({"version"}),
    "list": frozenset({"version", "type", "offset", "limit"}),
    "record": frozenset({"version", "id"}),
    "versions": frozenset(),
    "raw": frozenset({"version", "id"}),
    "artifact": frozenset({"version", "id"}),
    "artifact_bytes": frozenset({"version", "id"}),
    "payload": frozenset({"version", "id"}),
}

#: Parameters the published graph cannot answer, and why. There is no
#: per-record date to order or filter by: `okg.graph_nodes.observed_at` is a
#: WRITE time shared by a whole fact batch, and the publisher's re-application
#: of derived facts drops it entirely. Ordering by it would rank records by
#: which publish run wrote them -- on a wholesale rebuild, one value for every
#: record, so `sort=newest` would be a no-op ordering and `since` an
#: all-or-nothing gate, both looking like they worked. The only other basis is
#: a per-subtype date key inside `attrs`: no universal key, no index, an
#: unbounded scan. So they are refused, and carrying a real per-record date
#: through publish is a separate change.
_NO_STORED_BASIS = "the published graph stores no per-record date"

#: Per read, the parameters that get a NAMED cause instead of being reported as
#: names the route does not know. Only the search route offers ranking or a
#: date window, so only it can owe the caller that explanation. This used to be
#: a blanket `- {"sort"}` on every route, which quietly accepted
#: `?sort=relevance` on reads that rank nothing: the record page would have
#: sent a sort the route neither honored nor mentioned, which is the exact
#: silence the guard exists to prevent.
_NAMED_CAUSE_PARAMS: dict[str, frozenset[str]] = {
    "search": frozenset({"sort", "since"}),
}


def _filter_value(raw: str | None) -> str | None:
    """One reading of a filter value, used by every tier of the read.

    Normalised ONCE, at the edge. ``?source=`` and ``?source=%20`` mean "no
    filter", and they have to mean it everywhere: the exact-ID tier and the
    text tier used to disagree about an empty string -- one filtered on it, the
    other dropped it -- and the echo then reported a filter that only half the
    read had applied.
    """
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _reject_unhonorable_params(request: Request, read: str) -> None:
    """Refuse a request naming a parameter this route cannot honor.

    Ordered deliberately: the two parameters with a known cause are named with
    that cause, then a repeat of a parameter that takes one value, and only
    then is an unrecognised name reported as such.
    """
    known = _KNOWN_PARAMS[read] | CLIENT_ONLY_PARAMS
    named_cause = _NAMED_CAUSE_PARAMS.get(read, frozenset())
    given = set(request.query_params.keys())
    if (
        "sort" in given
        and "sort" in named_cause
        and request.query_params.get("sort") != "relevance"
    ):
        raise HTTPException(
            status_code=422,
            detail={
                "reason": "filter_has_no_stored_basis",
                "parameter": "sort",
                "value": request.query_params.get("sort"),
                "error": f"cannot sort these records: {_NO_STORED_BASIS}.",
                "next_check": "Drop sort, or omit it to rank by relevance.",
            },
        )
    if "since" in given and "since" in named_cause:
        raise HTTPException(
            status_code=422,
            detail={
                "reason": "filter_has_no_stored_basis",
                "parameter": "since",
                "value": request.query_params.get("since"),
                "error": f"cannot filter these records by date: {_NO_STORED_BASIS}.",
                "next_check": "Drop since.",
            },
        )
    # Each of these parameters names ONE value. Starlette hands the route the
    # last of a repeat, so `?source=a&source=b` would search only b while the
    # caller believes it asked for both -- the same silence, one level down.
    seen: dict[str, int] = {}
    for name, _value in request.query_params.multi_items():
        seen[name] = seen.get(name, 0) + 1
    repeated = sorted(name for name, count in seen.items() if count > 1)
    if repeated:
        raise HTTPException(
            status_code=422,
            detail={
                "reason": "repeated_parameter",
                "parameters": repeated,
                "error": (
                    f"the parameter(s) {repeated} were given more than once, "
                    "and each takes one value."
                ),
                "next_check": "Send each parameter once.",
            },
        )
    # A read that owes `sort` a named cause has already raised above for every
    # value but `relevance`, so only that no-op survives to be excused here. A
    # read that names no cause for it does not know it, and says so.
    unknown = sorted(given - known - named_cause)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail={
                "reason": "unknown_parameter",
                "parameters": unknown,
                "error": (
                    "this route does not know the parameter(s) "
                    f"{unknown}, so it cannot honor them."
                ),
                "next_check": f"Send only: {sorted(known)}.",
            },
        )


class _UnpinnedRead:
    """What an unpinned read can still say about itself in the audit record.

    `authorize_local_operator_read` reads a session's generation, branch and
    catalog version to build the decision's target. A read that names no
    version has no generation and no catalog version -- those are correctly
    absent -- but it does read one branch, and the audit record has to name
    the branch that was actually read. Passing no session at all would let
    the target default to "default" and record an access against a branch the
    request never touched.
    """

    __slots__ = ("branch_name",)

    generation_id = None
    catalog_version_id = None

    def __init__(self, branch_name: str) -> None:
        self.branch_name = branch_name


def _authorize_or_raise(
    conn: Any,
    *,
    deployment: str,
    session: Any,
    read: str,
    request: dict[str, Any],
    subject: str,
    subject_meta: Any,
    version: str | None,
) -> None:
    """Authorize one operator read, turning a denial into the HTTP refusal.

    ``session`` is an ``_UnpinnedRead`` for a read that names no version --
    the version listing itself. The boundary decision does not depend on a
    pinned generation; the generation appears only in the audit target, so an
    unpinned read is authorized exactly like a pinned one and a denial reads
    the same to the caller.
    """
    from okg.substrate.errors import SubstrateError
    from okg.substrate.operator_read import (
        OPERATOR_READ_ROUTE_FAULTS,
        authorize_local_operator_read,
    )

    try:
        authorize_local_operator_read(
            conn,
            deployment=deployment,
            session=session,
            read=read,
            request=request,
            subject=subject,
            subject_meta=subject_meta,
        )
    except OPERATOR_READ_ROUTE_FAULTS as exc:
        # The route built a request the boundary refuses (#2115): a code bug
        # that no retry or republish fixes, so it is a server fault, not the
        # 503 that tells the operator authority was unavailable. The message
        # names the read, keys and value types only, never a value.
        logger.error("console read %s refused by the boundary: %s", read, exc.message)
        raise HTTPException(
            status_code=500,
            detail={
                "reason": exc.code,
                "version": version,
                "error": exc.message,
                "error_type": type(exc).__name__,
            },
        ) from exc
    except SubstrateError as exc:
        if exc.code == "nomos_runtime_denied":
            raise HTTPException(
                status_code=403,
                detail={
                    "reason": "policy_denied",
                    "version": version,
                    "error": exc.message,
                    "next_check": exc.fix_hint,
                    "data": exc.data,
                },
            ) from exc
        raise HTTPException(
            status_code=503,
            detail={
                "reason": "policy_check_failed",
                "version": version,
                "error": exc.message,
                "error_type": type(exc).__name__,
            },
        ) from exc


def _operator_read(
    ctx: VizContext,
    *,
    version: str,
    read: str,
    request: dict[str, Any],
    compose: Callable[[Any, Any], dict[str, Any]],
) -> dict[str, Any]:
    from okg.substrate.errors import GenerationCompacted, SubstrateError
    from okg.substrate.mcp.session import open_session
    from okg.substrate.operator_read import (
        LOCAL_OPERATOR_READ_ACTION,
        LOCAL_OPERATOR_READ_SURFACE,
        resolve_operator_read_subject,
    )

    requested = version.strip()
    if not requested.startswith(f"okg:{ctx.deployment_name}:branch:"):
        raise HTTPException(
            status_code=422,
            detail={
                "reason": "version_not_a_published_checkpoint",
                "version": requested,
                "next_check": "Choose a version from the version picker.",
            },
        )
    subject, subject_meta = resolve_operator_read_subject(
        deployment=ctx.deployment_name, default_kind="local_operator",
    )
    with ctx.pool.connection() as conn:
        conn.autocommit = True
        try:
            session = open_session(
                conn,
                deployment_name=ctx.deployment_name,
                requested_generation=requested,
                branch_name=ctx.session.branch_name,
                nomos_subject=subject,
                nomos_read_action=LOCAL_OPERATOR_READ_ACTION,
                nomos_read_surface=LOCAL_OPERATOR_READ_SURFACE,
            )
        except GenerationCompacted as exc:
            raise HTTPException(
                status_code=410,
                detail={
                    "reason": "version_compacted",
                    "version": requested,
                    "error": exc.message,
                    "next_check": "This version is no longer retained. Choose a newer one.",
                },
            ) from exc
        except SubstrateError as exc:
            if "requested_generation" in (exc.data or {}):
                raise HTTPException(
                    status_code=404,
                    detail={
                        "reason": "version_not_published",
                        "version": requested,
                        "error": exc.message,
                        "next_check": "Choose a published version from the version picker.",
                    },
                ) from exc
            # The version may be fine: the branch or the catalog behind it
            # could not be opened, and that is what the operator has to fix.
            raise HTTPException(
                status_code=503,
                detail={
                    "reason": "version_unavailable",
                    "version": requested,
                    "error": exc.message,
                    "next_check": exc.fix_hint,
                },
            ) from exc
        except Exception as exc:  # noqa: BLE001 — a database fault is not a missing version
            # The exception TYPE, never its text. A database fault's message
            # carries the DSN, the failing SQL or a filesystem path, and this
            # detail is rendered in a browser. The full exception still reaches
            # the server log through the `raise ... from exc` chain.
            logger.exception(
                "console record read: opening version %s failed", requested,
            )
            raise HTTPException(
                status_code=503,
                detail={
                    "reason": "version_unavailable",
                    "version": requested,
                    "error": _OPAQUE_FAULT,
                    "error_type": type(exc).__name__,
                },
            ) from exc
        if session.generation_id != requested:
            raise HTTPException(
                status_code=404,
                detail={
                    "reason": "version_not_published",
                    "version": requested,
                    "resolved_version": session.generation_id,
                    "next_check": "Choose a published version from the version picker.",
                },
            )
        _authorize_or_raise(
            conn,
            deployment=ctx.deployment_name,
            session=session,
            read=read,
            request=request,
            subject=subject,
            subject_meta=subject_meta,
            version=requested,
        )
        try:
            payload = compose(conn, session)
        except HTTPException:
            # A composer's own typed refusal (the search scope below) already
            # says what failed; it must not be flattened into the opaque 500.
            raise
        except Exception as exc:  # noqa: BLE001 — surfaced with its cause
            # Same rule as the version-open fault above: a composer that fails
            # inside the database hands back a message that can name the DSN or
            # the query, so the response names the type and the log keeps the
            # rest. `SubstrateError.message` is no safer -- its `data` and text
            # are built from the same places.
            logger.exception(
                "console record read %s failed at version %s", read, requested,
            )
            raise HTTPException(
                status_code=500,
                detail={
                    "reason": f"record_{read}_failed",
                    "version": requested,
                    "error": _OPAQUE_FAULT,
                    "error_type": type(exc).__name__,
                },
            ) from exc
    return {"generation_id": session.generation_id, "payload": payload}


def register_operator_record_routes(
    router: APIRouter,
    *,
    ctx_provider: Callable[[], VizContext],
) -> None:
    """Register the operator record and version routes on ``router``."""
    from okg.substrate.console_views import records

    def console_versions(request: Request) -> dict[str, Any]:
        """Published versions of this deployment's branch, newest first.

        This read names no version -- it is what tells the caller which
        versions exist -- so it opens no pinned session. It is still
        authorized at the operator UI's boundary before it reads, under the
        same identity and with the same refusal as every other record read.
        """
        from okg.substrate.operator_read import resolve_operator_read_subject

        _reject_unhonorable_params(request, "versions")
        ctx = ctx_provider()
        branch_name = getattr(ctx.session, "branch_name", None)
        if not branch_name:
            # The audit record must name the branch this read reads. With no
            # branch to name, the read does not happen: a decision recorded
            # against a guessed branch is worse than no read at all.
            raise HTTPException(
                status_code=503,
                detail={
                    "reason": "branch_unavailable",
                    "error": "this deployment's read context names no branch",
                    "next_check": "Check the deployment's session branch before reading.",
                },
            )
        subject, subject_meta = resolve_operator_read_subject(
            deployment=ctx.deployment_name, default_kind="local_operator",
        )
        with ctx.conn() as conn:
            _authorize_or_raise(
                conn,
                deployment=ctx.deployment_name,
                session=_UnpinnedRead(branch_name),
                read="versions",
                request={"branch": branch_name},
                subject=subject,
                subject_meta=subject_meta,
                version=None,
            )
            payload = records.compose_published_versions(
                conn,
                deployment_name=ctx.deployment_name,
                branch_name=branch_name,
            )
        return {"payload": payload}

    def console_ontology_rollup() -> dict[str, Any]:
        """Record types and relationship types at the pinned generation.

        Backs Home's contents panel. The counts come from the maintained
        console count rollup as of the generation this context is pinned to,
        so the read is O(1) in the size of the graph.

        Authorized exactly like ``console_versions``: the operator UI's read
        identity, then the Nomos boundary check before the read touches the
        graph. A denial is refused as ``policy_denied``. It names no version
        of its own -- the pinned generation IS the version -- so the pinned
        session goes to the boundary as the audit target.

        An unmaintained generation raises ``ConsoleCountRollupUnavailable``,
        which is refused as a named 503 rather than answered with zeros: an
        empty contents panel and a graph with nothing in it must not look the
        same.
        """
        from okg.substrate.console_views.console_count_rollup_read import (
            ConsoleCountRollupUnavailable,
        )
        from okg.substrate.console_views.ontology_rollup_console import (
            compose_ontology_rollup,
        )
        from okg.substrate.operator_read import resolve_operator_read_subject

        ctx = ctx_provider()
        generation_id = ctx.session.generation_id
        subject, subject_meta = resolve_operator_read_subject(
            deployment=ctx.deployment_name, default_kind="local_operator",
        )
        with ctx.conn() as conn:
            _authorize_or_raise(
                conn,
                deployment=ctx.deployment_name,
                session=ctx.session,
                read="ontology_rollup",
                request={"branch": ctx.session.branch_name},
                subject=subject,
                subject_meta=subject_meta,
                version=generation_id,
            )
            try:
                payload = compose_ontology_rollup(
                    conn,
                    deployment_name=ctx.deployment_name,
                    generation_id=generation_id,
                )
            except ConsoleCountRollupUnavailable as exc:
                # The reader's own message is a fixed, operator-facing
                # sentence about a missing maintained rollup, so it is safe to
                # pass on -- but it is read from the typed exception's
                # `message`, not from str(exc), so a subclass that stringifies
                # a query cannot leak one onto the page.
                raise HTTPException(
                    status_code=503,
                    detail={
                        "reason": "ontology_rollup_unavailable",
                        "generation_id": generation_id,
                        "error": getattr(exc, "message", None)
                                 or "The maintained count rollup is not "
                                    "available for this generation.",
                        "error_type": type(exc).__name__,
                    },
                ) from exc
        return {"generation_id": generation_id, "payload": payload}

    def console_record_search(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
        q: str = Query(..., description="words or an exact record ID"),
        record_type: str | None = Query(None, alias="type", description="restrict to one record type"),
        source: str | None = Query(None, description="restrict to one producer id"),
        limit: int = Query(20, ge=1, le=records.SEARCH_LIMIT_MAX),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        from okg.substrate.operator_read import resolve_operator_search_scope
        from okg.viz.console_search_scope import resolve_console_search_scope

        # Before anything reads: a filter this route cannot honor is refused
        # without opening a pinned session or touching the graph.
        _reject_unhonorable_params(request, "search")
        record_type = _filter_value(record_type)
        source = _filter_value(source)
        ctx = ctx_provider()

        def compose(conn: Any, session: Any) -> dict[str, Any]:
            scope = resolve_console_search_scope(
                resolve_operator_search_scope,
                conn,
                session,
                deployment=ctx.deployment_name,
                subtype_filter=record_type,
                version=version,
            )
            return records.compose_record_search(
                conn,
                session=session,
                query=q,
                subtype=record_type,
                source=source,
                limit=limit,
                offset=offset,
                scope=scope,
            )

        return _operator_read(
            ctx,
            version=version,
            read="search",
            # These keys go to the audit target and a request digest, never
            # to the policy (operator_read.authorize_local_operator_read,
            # #2115). A key named `source` is reserved -- it is the producer
            # fact the policy reads for source lineage -- and is refused
            # `operator_read_request_key_reserved`. The requested filter is a
            # different thing from the record's producer, so it is named as
            # one.
            request={
                "query": q,
                "type": record_type,
                "source_filter": source,
                "limit": limit,
                "offset": offset,
            },
            compose=compose,
        )

    def console_record_types(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
    ) -> dict[str, Any]:
        _reject_unhonorable_params(request, "types")
        ctx = ctx_provider()
        return _operator_read(
            ctx,
            version=version,
            read="types",
            request={},
            compose=lambda conn, session: records.compose_record_types(conn, session=session),
        )

    def console_record_list(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
        record_type: str = Query(..., min_length=1, alias="type", description="record type to list"),
        offset: int = Query(0, ge=0, description="position among the type's records"),
        limit: int = Query(50, ge=1, le=records.LIST_LIMIT_MAX),
    ) -> dict[str, Any]:
        _reject_unhonorable_params(request, "list")
        ctx = ctx_provider()
        return _operator_read(
            ctx,
            version=version,
            read="list",
            request={"type": record_type, "offset": offset, "limit": limit},
            compose=lambda conn, session: records.compose_record_list(
                conn, session=session, subtype=record_type, offset=offset, limit=limit,
            ),
        )

    def console_record(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
        record_id: str = Query(..., min_length=1, alias="id", description="record ID"),
    ) -> dict[str, Any]:
        _reject_unhonorable_params(request, "record")
        ctx = ctx_provider()
        return _operator_read(
            ctx,
            version=version,
            read="record",
            request={"record_id": record_id},
            compose=lambda conn, session: records.compose_record_detail(
                conn, session=session, node_id=record_id,
            ),
        )

    def console_record_raw(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
        record_id: str = Query(..., min_length=1, alias="id", description="record ID"),
    ) -> dict[str, Any]:
        """The record's stored row and edge rows, plus its payload or refusal."""
        _reject_unhonorable_params(request, "raw")
        ctx = ctx_provider()
        return _operator_read(
            ctx,
            version=version,
            read="raw",
            request={"record_id": record_id},
            compose=lambda conn, session: records.compose_record_raw(
                conn, session=session, node_id=record_id,
            ),
        )

    def console_record_artifact(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
        record_id: str = Query(..., min_length=1, alias="id", description="record ID"),
    ) -> dict[str, Any]:
        """Whether the record's original file is retained here, and what it is."""
        _reject_unhonorable_params(request, "artifact")
        ctx = ctx_provider()
        return _operator_read(
            ctx,
            version=version,
            read="artifact",
            request={"record_id": record_id},
            compose=lambda conn, session: records.compose_record_artifact(
                conn, session=session, node_id=record_id,
            ),
        )

    def _bytes_route(
        request: Request,
        *,
        version: str,
        record_id: str,
        read: str,
        fetch: Callable[[Any, Any], tuple[bytes, str] | None],
    ) -> Response:
        """One byte read: authorized first, then fetched, then served inert.

        The authorization and the version pin are `_operator_read`'s, the same
        ones the metadata read used, so bytes can never be served under a
        weaker check than the read that named them. Nothing is fetched for a
        record the reader may not see, and that case is rendered exactly as an
        absent record is.
        """
        _reject_unhonorable_params(request, read)
        ctx = ctx_provider()
        served = _operator_read(
            ctx,
            version=version,
            read=read,
            request={"record_id": record_id},
            compose=lambda conn, session: {"bytes": fetch(conn, session)},
        )
        found = served["payload"]["bytes"]
        if found is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "reason": "not_found",
                    "version": served["generation_id"],
                    "error": "no such retained bytes in this version",
                    "next_check": "Open the record and read what it says is retained.",
                },
            )
        data, media_type = found
        return Response(
            content=data,
            media_type=media_type,
            headers=dict(_BYTE_HEADERS),
        )

    def console_record_artifact_bytes(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
        record_id: str = Query(..., min_length=1, alias="id", description="record ID"),
    ) -> Response:
        """The retained image bytes the artifact read named."""
        return _bytes_route(
            request,
            version=version,
            record_id=record_id,
            read="artifact_bytes",
            fetch=lambda conn, session: records.read_artifact_bytes(
                conn, session=session, node_id=record_id,
            ),
        )

    def console_record_payload(
        request: Request,
        version: str = Query(..., min_length=1, description="published version checkpoint id"),
        record_id: str = Query(..., min_length=1, alias="id", description="record ID"),
    ) -> Response:
        """The retained payload the raw read named, whole."""
        return _bytes_route(
            request,
            version=version,
            record_id=record_id,
            read="payload",
            fetch=lambda conn, session: records.read_payload_bytes(
                conn, session=session, node_id=record_id,
            ),
        )

    router.get("/console/versions")(console_versions)
    router.get("/console/ontology-rollup")(console_ontology_rollup)
    router.get("/console/records/search")(console_record_search)
    router.get("/console/records/types")(console_record_types)
    router.get("/console/records/list")(console_record_list)
    router.get("/console/records/detail")(console_record)
    router.get("/console/records/raw")(console_record_raw)
    router.get("/console/records/artifact")(console_record_artifact)
    router.get("/console/records/artifact/bytes")(console_record_artifact_bytes)
    router.get("/console/records/payload")(console_record_payload)
    register_graph_routes(router, ctx_provider=ctx_provider)  # shares this module's guards
