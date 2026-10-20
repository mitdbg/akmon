"""The console Graph view's read routes (PACT console-graph-views).

Four GET reads under ``/viz/api/console/graph/``: the type map (``types``),
which sources link to which (``source-links``), one record's grouped
neighbours (``record``) and the declared ontology beside what is populated
(``ontology``). The composers are in ``okg.substrate.console_views``
(``graph_views``, ``graph_record``, ``graph_ontology``).

Each read goes through the record routes' ``_operator_read``: a session pinned
to one published version under the operator UI's read identity, the Nomos
boundary check before the graph is touched, and the same refusals (422 for a
version that is not a checkpoint id, 404 for one not published, 410 for one
compacted, 403 for a policy denial). ``version`` is optional here: omitted, the
read answers for the generation the read API pinned at start-up, as the
console's other non-record reads do. A composer's own refusal
(``GraphReadRefused``) keeps its status and body; any other fault is the
opaque 500 every record route serves.

Like the record routes these are outside the response cache: each read must
pass the policy check.
"""
from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Query, Request

from okg.viz.context import VizContext

#: The parameters each Graph read honours; anything else is refused by the
#: record routes' ``_reject_unhonorable_params``.
GRAPH_KNOWN_PARAMS: dict[str, frozenset[str]] = {
    "graph_types": frozenset({"version"}),
    "graph_source_links": frozenset({"version"}),
    "graph_record": frozenset({"version", "id"}),
    "graph_ontology": frozenset({"version"}),
}


def register_graph_routes(
    router: APIRouter,
    *,
    ctx_provider: Callable[[], VizContext],
) -> None:
    """Register the four console Graph reads on ``router``.

    Called from ``register_operator_record_routes``: these reads share that
    module's version pin, boundary check and parameter guard, so the guard's
    table learns the parameters each Graph read honours here.
    """
    from okg.viz.record_endpoints import _KNOWN_PARAMS, _operator_read, _reject_unhonorable_params

    _KNOWN_PARAMS.update(GRAPH_KNOWN_PARAMS)

    def _read(
        request: Request,
        *,
        read: str,
        version: str | None,
        request_facts: dict[str, Any],
        compose: Callable[[Any, Any, VizContext], dict[str, Any]],
    ) -> dict[str, Any]:
        from okg.substrate.console_views.graph_views import GraphReadRefused

        _reject_unhonorable_params(request, read)
        if version is not None and not version.strip():
            # An empty `version=` is a malformed link, not "the latest": refuse
            # it as the record routes do rather than answer another version.
            raise HTTPException(status_code=422, detail={
                "reason": "empty_version",
                "error": "version= is empty; name a published version id or leave the parameter out.",
            })
        ctx = ctx_provider()
        pinned = version if version is not None else str(ctx.session.generation_id)

        # A composer's named refusal is carried out of the read and raised
        # here with its own status: inside it, any exception is the opaque 500.
        refused: list[GraphReadRefused] = []

        def run(conn: Any, session: Any) -> dict[str, Any]:
            try:
                return compose(conn, session, ctx)
            except GraphReadRefused as exc:
                refused.append(exc)
                return {}

        answer = _operator_read(
            ctx, version=pinned, read=read, request=request_facts, compose=run,
        )
        if refused:
            raise HTTPException(
                status_code=refused[0].status,
                detail={**refused[0].detail, "version": pinned},
            ) from refused[0]
        return answer

    def console_graph_types(
        request: Request,
        version: str | None = Query(None, min_length=1, description="published version checkpoint id"),
    ) -> dict[str, Any]:
        """Record, relationship and producer counts and type patterns, from rollups."""
        from okg.substrate.console_views.graph_views import compose_graph_types

        return _read(
            request,
            read="graph_types",
            version=version,
            request_facts={},
            compose=lambda conn, session, ctx: compose_graph_types(
                conn, session=session, deployment_name=ctx.deployment_name,
            ),
        )

    def console_graph_source_links(
        request: Request,
        version: str | None = Query(None, min_length=1, description="published version checkpoint id"),
    ) -> dict[str, Any]:
        """Relationships by the sources of their two ends, computed on request and kept."""
        from okg.substrate.console_views.graph_views import compose_source_links

        return _read(
            request,
            read="graph_source_links",
            version=version,
            request_facts={},
            compose=lambda conn, session, ctx: compose_source_links(
                conn, session=session, deployment_name=ctx.deployment_name,
            ),
        )

    def console_graph_record(
        request: Request,
        record_id: str = Query(..., min_length=1, alias="id", description="record ID"),
        version: str | None = Query(None, min_length=1, description="published version checkpoint id"),
    ) -> dict[str, Any]:
        """One record's neighbours, grouped by direction, relationship and type."""
        from okg.substrate.console_views.graph_record import compose_graph_record

        return _read(
            request,
            read="graph_record",
            version=version,
            request_facts={"record_id": record_id},
            compose=lambda conn, session, ctx: compose_graph_record(
                conn,
                session=session,
                deployment_name=ctx.deployment_name,
                node_id=record_id,
            ),
        )

    def console_graph_ontology(
        request: Request,
        version: str | None = Query(None, min_length=1, description="published version checkpoint id"),
    ) -> dict[str, Any]:
        """Declared types, attributes and rules beside the version's counts."""
        from okg.substrate.console_views.graph_ontology import compose_graph_ontology

        return _read(
            request,
            read="graph_ontology",
            version=version,
            request_facts={},
            compose=lambda conn, session, ctx: compose_graph_ontology(
                conn,
                session=session,
                deployment_name=ctx.deployment_name,
                deployment_dir=ctx.deployment_dir,
            ),
        )

    router.get("/console/graph/types")(console_graph_types)
    router.get("/console/graph/source-links")(console_graph_source_links)
    router.get("/console/graph/record")(console_graph_record)
    router.get("/console/graph/ontology")(console_graph_ontology)
