"""Grouped registration helpers for the substrate viz API router."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import APIRouter

RouteHandler = Callable[..., Any]


def register_graph_routes(
    router: APIRouter,
    *,
    search: RouteHandler,
    types: RouteHandler,
    type_sample: RouteHandler,
    node: RouteHandler,
    expand: RouteHandler,
    khop: RouteHandler,
    stats: RouteHandler,
    generations: RouteHandler,
    repin: RouteHandler,
) -> None:
    router.get("/search")(search)
    router.get("/types")(types)
    router.get("/types/{type}/sample")(type_sample)
    router.get("/node/{id:path}")(node)
    router.get("/expand/{id:path}")(expand)
    router.get("/khop/{id:path}")(khop)
    router.get("/stats")(stats)
    router.get("/generations")(generations)
    router.post("/repin")(repin)


def register_ontology_overview_routes(
    router: APIRouter,
    *,
    config: RouteHandler,
    ontology: RouteHandler,
    overview: RouteHandler,
    hierarchy_root: RouteHandler,
    hierarchy_tree: RouteHandler,
    hierarchy_drill: RouteHandler,
    overview_group_drill: RouteHandler,
    ontology_graph: RouteHandler,
) -> None:
    router.get("/config")(config)
    router.get("/ontology")(ontology)
    router.get("/overview")(overview)
    router.get("/hierarchy/root")(hierarchy_root)
    router.get("/hierarchy/tree")(hierarchy_tree)
    router.get("/hierarchy/drill")(hierarchy_drill)
    router.get("/overview/group")(overview_group_drill)
    router.get("/ontology_graph")(ontology_graph)


def register_system_routes(
    router: APIRouter,
    *,
    system_snapshot: RouteHandler,
    system_profile: RouteHandler,
    system_profile_artifact: RouteHandler,
) -> None:
    router.get("/system/snapshot")(system_snapshot)
    router.get("/system/profile/{profile_id}")(system_profile)
    router.get("/system/profile/{profile_id}/artifact/{artifact_path:path}")(
        system_profile_artifact
    )


def register_trace_routes(
    router: APIRouter,
    *,
    traces: RouteHandler,
    workflows: RouteHandler,
) -> None:
    # Home's two activity reads. The per-trace detail routes (`/trace`,
    # `/workflow`) and the seven `/telemetry/*` routes left with the Trace and
    # Performance views in issue #1924 — nothing else called them.
    router.get("/traces")(traces)
    # Workflow (pipeline/DBOS) span-timeline traces — the telemetry-store
    # sibling of the MCP query traces above.
    router.get("/workflows")(workflows)


def register_publication_routes(
    router: APIRouter,
    *,
    pub_pdf: RouteHandler,
    pub_annotations: RouteHandler,
    resolve_pub: RouteHandler,
) -> None:
    router.get("/pubs/{id:path}/pdf")(pub_pdf)
    router.get("/pubs/{id:path}/annotations")(pub_annotations)
    router.get("/resolve_pub")(resolve_pub)


def register_portal_routes(
    router: APIRouter,
    *,
    portal_catalog: RouteHandler,
    portal_tree: RouteHandler,
    portal_views: RouteHandler,
    portal_view_slot: RouteHandler,
) -> None:
    router.get("/portal/catalog")(portal_catalog)
    router.get("/portal/tree")(portal_tree)
    router.get("/portal/views")(portal_views)
    router.get("/portal/view-slot")(portal_view_slot)


def register_portal_lens_routes(
    router: APIRouter,
    *,
    portal_lenses: RouteHandler,
    portal_lens_overlays: RouteHandler,
    portal_lens_overlay_revoke: RouteHandler,
    portal_lens_putback: RouteHandler,
    portal_view_markered: RouteHandler,
    portal_view_cited: RouteHandler,
    view_recheck: RouteHandler,
) -> None:
    router.get("/portal/lenses")(portal_lenses)
    router.get("/portal/lens-overlays")(portal_lens_overlays)
    router.post("/portal/lens-overlays/{overlay_id}/revoke")(
        portal_lens_overlay_revoke
    )
    router.post("/portal/lens-putback")(portal_lens_putback)
    router.get("/portal/views/{view_id:path}/markered")(portal_view_markered)
    router.get("/portal/views/{view_id:path}/cited")(portal_view_cited)
    router.post("/views/{view_id:path}/recheck")(view_recheck)


def register_console_routes(
    router: APIRouter,
    *,
    console_view: RouteHandler,
    console_status: RouteHandler,
    console_pact_review: RouteHandler,
    console_subtype: RouteHandler,
    console_generations: RouteHandler,
    console_attempt_detail: RouteHandler,
    console_attempts: RouteHandler,
    console_sources: RouteHandler,
    console_tasks: RouteHandler,
    console_agents: RouteHandler,
    console_health: RouteHandler,
    console_decisions: RouteHandler,
    console_worktrees: RouteHandler,
    console_source_detail: RouteHandler,
    console_source_diagnostics: RouteHandler,
    console_producers: RouteHandler,
) -> None:
    router.get("/console/view")(console_view)
    router.get("/console/status")(console_status)
    router.post("/console/pact-review")(console_pact_review)
    router.get("/console/subtype")(console_subtype)
    router.get("/console/generations")(console_generations)
    router.get("/console/attempts/detail")(console_attempt_detail)
    router.get("/console/attempts")(console_attempts)
    router.get("/console/sources")(console_sources)
    router.get("/console/tasks")(console_tasks)
    router.get("/console/agents")(console_agents)
    router.get("/console/health")(console_health)
    router.get("/console/decisions")(console_decisions)
    router.get("/console/worktrees")(console_worktrees)
    router.get("/console/source-detail")(console_source_detail)
    router.get("/console/source-diagnostics")(console_source_diagnostics)
    # Beside source-diagnostics, not in place of it: the old Home and Health
    # views still read the old routes until the slice that deletes them.
    router.get("/console/producers")(console_producers)


# There is deliberately no chat route group. The operator-local chat POST
# route served a Claude Code subprocess streamed to the browser; it was retired
# by PACT retire-viz-chat and archived under
# archive/chat/viz-chat-2026-08-04/ (now only in git, commit aa9a7eed8015). The
# governed replacement is the chat:
# manifest block, not a viz route.
