from __future__ import annotations

from types import SimpleNamespace

from fastapi.routing import APIRoute

from okg.viz.context import VizContext
from okg.viz.endpoints import build_router


EXPECTED_VIZ_ROUTES = [
    (
        "/viz/api/console/enricher-diagnostics",
        ("GET",),
        "console_enricher_diagnostics",
    ),
    (
        "/viz/api/console/view-diagnostics",
        ("GET",),
        "console_view_diagnostics",
    ),
    (
        "/viz/api/console/producer-metrics",
        ("GET",),
        "console_producer_metrics",
    ),
    ("/viz/api/search", ("GET",), "search"),
    ("/viz/api/types", ("GET",), "types"),
    ("/viz/api/types/{type}/sample", ("GET",), "type_sample"),
    ("/viz/api/node/{id:path}", ("GET",), "node"),
    ("/viz/api/expand/{id:path}", ("GET",), "expand"),
    ("/viz/api/khop/{id:path}", ("GET",), "khop"),
    ("/viz/api/stats", ("GET",), "stats"),
    ("/viz/api/generations", ("GET",), "generations"),
    ("/viz/api/repin", ("POST",), "repin"),
    ("/viz/api/system/snapshot", ("GET",), "system_snapshot"),
    ("/viz/api/system/profile/{profile_id}", ("GET",), "system_profile"),
    (
        "/viz/api/system/profile/{profile_id}/artifact/{artifact_path:path}",
        ("GET",),
        "system_profile_artifact",
    ),
    ("/viz/api/config", ("GET",), "config"),
    ("/viz/api/ontology", ("GET",), "ontology"),
    ("/viz/api/overview", ("GET",), "overview"),
    ("/viz/api/hierarchy/root", ("GET",), "hierarchy_root"),
    ("/viz/api/hierarchy/tree", ("GET",), "hierarchy_tree"),
    ("/viz/api/hierarchy/drill", ("GET",), "hierarchy_drill"),
    ("/viz/api/overview/group", ("GET",), "overview_group_drill"),
    ("/viz/api/ontology_graph", ("GET",), "ontology_graph"),
    ("/viz/api/traces", ("GET",), "traces"),
    ("/viz/api/workflows", ("GET",), "workflows"),
    ("/viz/api/pubs/{id:path}/pdf", ("GET",), "pub_pdf"),
    ("/viz/api/pubs/{id:path}/annotations", ("GET",), "pub_annotations"),
    ("/viz/api/resolve_pub", ("GET",), "resolve_pub"),
    ("/viz/api/portal/catalog", ("GET",), "portal_catalog"),
    ("/viz/api/portal/tree", ("GET",), "portal_tree"),
    ("/viz/api/portal/views", ("GET",), "portal_views"),
    ("/viz/api/portal/view-slot", ("GET",), "portal_view_slot"),
    ("/viz/api/console/view", ("GET",), "console_view"),
    ("/viz/api/console/status", ("GET",), "console_status"),
    ("/viz/api/console/pact-review", ("POST",), "console_pact_review"),
    ("/viz/api/console/subtype", ("GET",), "console_subtype"),
    ("/viz/api/console/generations", ("GET",), "console_generations"),
    # PACT operator-console-live-updates (#1924): one attempt's recorded
    # detail -- what the version holds, what moved by record type,
    # relationship type and source against its parent, the publication checks
    # that ran, and the per-producer runs.
    (
        "/viz/api/console/attempts/detail",
        ("GET",),
        "console_attempt_detail",
    ),
    ("/viz/api/console/attempts", ("GET",), "console_attempts"),
    ("/viz/api/console/sources", ("GET",), "console_sources"),
    ("/viz/api/console/tasks", ("GET",), "console_tasks"),
    ("/viz/api/console/agents", ("GET",), "console_agents"),
    ("/viz/api/console/health", ("GET",), "console_health"),
    ("/viz/api/console/decisions", ("GET",), "console_decisions"),
    ("/viz/api/console/worktrees", ("GET",), "console_worktrees"),
    ("/viz/api/console/source-detail", ("GET",), "console_source_detail"),
    (
        "/viz/api/console/source-diagnostics",
        ("GET",),
        "console_source_diagnostics",
    ),
    ("/viz/api/console/producers", ("GET",), "console_producers"),
    ("/viz/api/portal/lenses", ("GET",), "portal_lenses"),
    ("/viz/api/portal/lens-overlays", ("GET",), "portal_lens_overlays"),
    (
        "/viz/api/portal/lens-overlays/{overlay_id}/revoke",
        ("POST",),
        "portal_lens_overlay_revoke",
    ),
    ("/viz/api/portal/lens-putback", ("POST",), "portal_lens_putback"),
    (
        "/viz/api/portal/views/{view_id:path}/markered",
        ("GET",),
        "portal_view_markered",
    ),
    (
        "/viz/api/portal/views/{view_id:path}/cited",
        ("GET",),
        "portal_view_cited",
    ),
    ("/viz/api/views/{view_id:path}/recheck", ("POST",), "view_recheck"),
    # PACT operator-ui-search-view: version-pinned, policy-checked record reads.
    ("/viz/api/console/versions", ("GET",), "console_versions"),
    # PACT operator-console-live-home: Home's contents panel, policy-checked
    # like every other read in this module.
    ("/viz/api/console/ontology-rollup", ("GET",), "console_ontology_rollup"),
    ("/viz/api/console/records/search", ("GET",), "console_record_search"),
    ("/viz/api/console/records/types", ("GET",), "console_record_types"),
    ("/viz/api/console/records/list", ("GET",), "console_record_list"),
    ("/viz/api/console/records/detail", ("GET",), "console_record"),
    # PACT operator-console-record-layers: the Record page's Raw and Content
    # tabs, and the two byte reads they name.
    ("/viz/api/console/records/raw", ("GET",), "console_record_raw"),
    ("/viz/api/console/records/artifact", ("GET",), "console_record_artifact"),
    (
        "/viz/api/console/records/artifact/bytes",
        ("GET",),
        "console_record_artifact_bytes",
    ),
    ("/viz/api/console/records/payload", ("GET",), "console_record_payload"),
    # PACT console-graph-views: the Graph view's four reads, registered with
    # the record reads and pinned, authorized and refused the same way.
    ("/viz/api/console/graph/types", ("GET",), "console_graph_types"),
    ("/viz/api/console/graph/source-links", ("GET",), "console_graph_source_links"),
    ("/viz/api/console/graph/record", ("GET",), "console_graph_record"),
    ("/viz/api/console/graph/ontology", ("GET",), "console_graph_ontology"),
]

# Read routes retired with the Graph, Trace, Performance and Deployment views
# (issue #1924). Asserted absent so a reintroduction is a deliberate, reviewed
# change and not a silent merge artefact.
REMOVED_VIZ_ROUTES = (
    "/viz/api/trace",
    "/viz/api/workflow",
    "/viz/api/telemetry/performance",
    "/viz/api/telemetry/metrics",
    "/viz/api/telemetry/distribution",
    "/viz/api/telemetry/flamegraph",
    "/viz/api/telemetry/timeline",
    "/viz/api/telemetry/coverage",
    "/viz/api/telemetry/artifacts/{artifact_id:path}",
    "/viz/api/console/node",
    "/viz/api/console/neighborhood",
    "/viz/api/console/graph",
    "/viz/api/console/graph-batch",
    "/viz/api/console/search",
    "/viz/api/console/deployment",
    "/viz/api/console/producer-traces",
)


def _viz_routes() -> list[tuple[str, tuple[str, ...], str]]:
    router = build_router(lambda: None)
    routes = []
    for route in router.routes:
        if not isinstance(route, APIRoute):
            continue
        methods = tuple(
            sorted(method for method in route.methods if method not in {"HEAD", "OPTIONS"})
        )
        routes.append((route.path, methods, route.endpoint.__name__))
    return routes


def test_build_router_preserves_viz_route_surface() -> None:
    assert _viz_routes() == EXPECTED_VIZ_ROUTES


def test_retired_four_view_read_routes_are_not_registered() -> None:
    served = {path for path, _methods, _name in _viz_routes()}
    still_served = sorted(set(REMOVED_VIZ_ROUTES) & served)
    assert not still_served, (
        "these reads were retired with the Graph, Trace, Performance and "
        f"Deployment views (issue #1924) but are registered again: {still_served}"
    )


def test_runtime_readiness_route_is_deleted() -> None:
    """Issue #2258: the console runtime-readiness read is gone, not bounded.

    No console view requested it and it rebuilt the whole `okg dogfood
    status` report (18.5 s on okg-workspace). `okg dogfood status` stays the
    full readiness reader; the route must not come back by accident.
    """
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    served = {path for path, _methods, _name in _viz_routes()}
    assert "/viz/api/console/runtime-readiness" not in served

    app = FastAPI()
    app.include_router(build_router(lambda: SimpleNamespace()))
    response = TestClient(app).get("/viz/api/console/runtime-readiness")
    assert response.status_code == 404


def test_compatibility_generation_id_prefers_session_bridge() -> None:
    ctx = object.__new__(VizContext)
    ctx.session = SimpleNamespace(
        compat_generation_id=7,
        generation_id="checkpoint:abc",
    )

    assert ctx.compatibility_generation_id == 7
    assert ctx.legacy_generation_id == 7

    ctx.session = SimpleNamespace(
        compat_generation_id=None,
        generation_id="checkpoint:abc",
    )

    assert ctx.compatibility_generation_id == "checkpoint:abc"
    assert ctx.legacy_generation_id == "checkpoint:abc"


# Today's record reads. This is the guard on the DERIVATION below, not the
# input to it: the cache check asks the registrar which routes it registers,
# so a sixth route is covered the day it is added, and this tuple only stops
# the derivation from silently going empty or losing one of these five.
KNOWN_OPERATOR_RECORD_ROUTES = frozenset({
    "/viz/api/console/versions",
    "/viz/api/console/ontology-rollup",
    "/viz/api/console/records/search",
    "/viz/api/console/records/types",
    "/viz/api/console/records/list",
    "/viz/api/console/records/detail",
    "/viz/api/console/records/raw",
    "/viz/api/console/records/artifact",
    "/viz/api/console/records/artifact/bytes",
    "/viz/api/console/records/payload",
})


def _operator_record_routes() -> frozenset[str]:
    """Every path `register_operator_record_routes` registers, asked of it directly.

    Registering onto a throwaway router with the real prefix means the answer
    comes from the registrar, not from a list a later change has to remember
    to update.
    """
    from fastapi import APIRouter

    from okg.viz.record_endpoints import register_operator_record_routes

    probe = APIRouter(prefix="/viz/api")
    register_operator_record_routes(probe, ctx_provider=lambda: None)
    return frozenset(
        route.path for route in probe.routes if isinstance(route, APIRoute)
    )


def test_operator_record_route_derivation_sees_todays_routes() -> None:
    """Guards the guard: an empty or shrunken derivation would check nothing."""
    derived = _operator_record_routes()

    assert derived, "the record-route derivation returned nothing to check"
    assert KNOWN_OPERATOR_RECORD_ROUTES <= derived, (
        "a record read this contract knows about is no longer registered: "
        f"{sorted(KNOWN_OPERATOR_RECORD_ROUTES - derived)}"
    )


def test_operator_record_routes_are_not_response_cached() -> None:
    """Their module docstring claims this; nothing else enforced it.

    The response cache keys on path, query and generation, not on the reader,
    so one reader's allowed response could answer another reader's request
    that the policy would have refused. Every read this registrar registers
    must reach the graph.read.local_operator boundary on every request.
    """
    from okg.viz import read_cache

    derived = _operator_record_routes()
    cached = sorted(derived & read_cache.CACHEABLE_PATHS)

    assert not cached, (
        "these reads are authorized per request at the graph.read.local_operator "
        f"boundary and must not be served from the response cache: {cached}"
    )
    assert not [path for path in sorted(derived) if read_cache.is_cacheable_path(path)]


def test_operator_record_routes_are_actually_registered() -> None:
    """The derived routes are the ones the real router serves, under the real prefix."""
    served = {path for path, _methods, _name in _viz_routes()}

    assert _operator_record_routes() <= served
