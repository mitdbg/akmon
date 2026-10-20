"""FastAPI sub-app for the OKG visualization surface (substrate-native).

Mounted by `python -m okg.viz` against a substrate DSN +
deployment. Exposes read-only HTTP endpoints over substrate
tables, pinned to a single generation per process.

Endpoints in this module are defined as plain ``def`` (not
``async def``) so FastAPI runs them in its threadpool. SQL
calls run on those threadpool workers; the asyncio event
loop stays free for static-file IO and other async paths.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlencode, urlparse

from fastapi import Body, FastAPI, HTTPException, Request, Response
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from okg.viz.auth import (
    OPERATOR_PRINCIPAL,
    OPERATOR_SESSION_ID,
    API_AUTH_REALM,
    OPERATOR_TOKEN_HEADER,
    api_request_authorized,
    is_api_path,
    is_mutating_route,
    is_operator_approval_route,
    is_operator_control_route,
    is_operator_setup_plan_route,
    resolve_operator_token,
    token_matches,
)
from okg.viz.context import VizContext
from okg.viz.endpoints import build_router
from okg.viz.operator_control import (
    ControlOperation,
    OperatorControlRequest,
    OperatorControlService,
    invoke_operator_control,
)
from okg.viz.approval_control import (
    ApprovalControlRequest,
    ApprovalControlService,
    ApprovalDecisionRequest,
    ApprovalRevokeRequest,
    invoke_approval_control,
)
from okg.viz.coordination_services import (
    SetupPlanRequest,
    SetupPlanService,
    invoke_setup_plan,
)
from okg.viz.read_cache import (
    CLIENT_ONLY_PARAMS,
    DEFAULT_CAPACITY,
    CachedResponse,
    ReadCache,
    cache_key,
    extract_generation_id,
    is_cacheable_path,
    make_etag,
    wants_bypass,
)
from okg.viz.registry import current_deployment, load_registry
from okg.viz.tool_allowlist import ALLOWLIST, describe_allowlist

logger = logging.getLogger(__name__)
#: Durable audit channel for privileged viz operator actions. Emits even in
#: production (build_app no longer needs an injected sink to record anything).
_audit_logger = logging.getLogger("okg.viz.audit")

#: Where the archived legacy frontend can still be read. PACT
#: layout-history-openspec deleted ``archive/`` from the working tree, so the
#: 410 payload names a git object instead of a path: the last commit that
#: changed the archive, and the archive's path inside that commit
#: (``git show <commit>:<path>/INDEX.md``).
LEGACY_VIZ_ARCHIVE_COMMIT = "3253beb069ba049eb4adbc201609e6a08c8ca757"
LEGACY_VIZ_ARCHIVE_PATH = "archive/ui/legacy-viz-2026-06-17"
_LEGACY_VIZ_ARCHIVE = f"{LEGACY_VIZ_ARCHIVE_COMMIT}:{LEGACY_VIZ_ARCHIVE_PATH}"


def _is_loopback_request(request: Request) -> bool:
    """True unless the request comes from a REAL, non-loopback network peer.

    The no-token policy refuses only requests from a genuine network IP (a remote
    attacker), so a loopback peer (local dev) and an in-process caller (the ASGI
    test transport, which has no socket peer or a non-IP host like "testclient")
    are trusted. uvicorn always populates a real IP for an actual network
    connection, so a network attacker can never present a non-IP / absent peer —
    this is precise, not fail-open."""
    import ipaddress

    client = request.client
    if client is None or not client.host:
        return True  # no socket peer -> in-process ASGI call, not a network peer
    try:
        return ipaddress.ip_address(client.host).is_loopback
    except ValueError:
        return True  # non-IP host (test transport) -> in-process, trusted


def _bound_port() -> int | None:
    raw = os.environ.get("OKG_VIZ_PORT")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _bound_host() -> str:
    return os.environ.get("OKG_VIZ_HOST", "127.0.0.1")


def _deployments_root() -> Path | None:
    """Repo root deployments/ dir if present."""
    here = Path(__file__).resolve()
    candidate = here.parents[3] / "deployments"
    return candidate if candidate.is_dir() else None


def build_app(
    ctx_provider: Callable[[], VizContext],
    is_ready: Callable[[], bool] | None = None,
    *,
    operator_token: str | None = None,
    audit_sink: list[dict[str, Any]] | None = None,
    operator_control_service: OperatorControlService | None = None,
    approval_control_service: ApprovalControlService | None = None,
    setup_plan_service: SetupPlanService | None = None,
    api_token: str | None = None,
) -> FastAPI:
    """Construct the FastAPI app with the given VizContext provider.

    `ctx_provider` is a zero-arg callable returning the live
    `VizContext`. The lazy-init pattern lets the page render its
    loading state without forcing the substrate connection to
    resolve before the server is ready.

    `is_ready` is a non-blocking callable returning True once the
    substrate session is open. Used by `/viz/api/health` to avoid
    blocking on first request.

    `api_token` (issue #3278), when given, gates EVERY request under
    `/viz/api/` -- reads included -- behind `Authorization: Bearer <token>`
    (or HTTP Basic with the token as the password). Without it the read
    routes stay open, as before.
    """
    if api_token is not None and not api_token:
        raise ValueError("api_token must be a non-empty string or None")
    app = FastAPI(
        title="OKG Visualization",
        description="Read-only HTTP surface over the substrate.",
        version="0.2.0",
        docs_url="/viz/api/docs",
        openapi_url="/viz/api/openapi.json",
    )

    @app.exception_handler(RequestValidationError)
    async def dedicated_control_validation_failure(
        request: Request,
        exc: RequestValidationError,
    ) -> Response:
        # FastAPI's default 422 payload includes the offending input.  That is
        # useful for ordinary APIs but could echo a token/DSN supplied to this
        # privileged boundary.  Keep this dedicated error deliberately terse;
        # all other Viz validation behavior remains unchanged.
        if (
            is_operator_control_route(request.method, request.url.path)
            or is_operator_approval_route(request.method, request.url.path)
            or is_operator_setup_plan_route(request.method, request.url.path)
        ):
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "error": "invalid operator control request",
                    "path": request.url.path,
                },
            )
        return await request_validation_exception_handler(request, exc)

    # Deployment code may inject the native coordination writer later.  Viz
    # itself never loads a DSN or creates a database-backed implementation.
    app.state.operator_control_service = operator_control_service
    app.state.approval_control_service = approval_control_service
    app.state.setup_plan_service = setup_plan_service

    # CORS is registered LAST (see end of build_app) so it is the OUTERMOST
    # middleware -- it must wrap the read-cache middleware's short-circuit
    # responses (HIT/304) too, or every cache hit would reach a cross-origin
    # client without CORS headers and be blocked by the browser.

    # Operator auth + CSRF: gate every mutating viz route behind the configured
    # operator token (a custom header, so it is not an ambient cookie and a
    # cross-origin page cannot drive a mutation). When NO token is configured the
    # boundary FAILS CLOSED for network exposure: a mutating route is served only
    # to a loopback peer, so binding 0.0.0.0 without a token does NOT expose
    # mutations to the network (the prior default ran every mutating route
    # unauthenticated regardless of bind — the rejected silent-insecure-default).
    # The launcher additionally refuses to start a non-loopback bind without a
    # token. Set OKG_VIZ_OPERATOR_TOKEN to allow authenticated remote operators.
    _operator_token = resolve_operator_token(operator_token)

    def _emit_audit(request: Request, status: int, principal: str) -> None:
        record = {
            "action": request.url.path,
            "method": request.method,
            "principal": principal,
            "status": status,
            "peer": request.client.host if request.client else None,
        }
        # Durable audit by default (production), plus the injectable in-memory
        # sink for tests. Without this, production emitted NO audit event.
        _audit_logger.info("viz operator action: %s", json.dumps(record))
        if audit_sink is not None:
            audit_sink.append(record)

    @app.middleware("http")
    async def operator_auth(request: Request, call_next):
        # Operator controls are a separate closed set.  They always require
        # the configured Viz token, including loopback and in-process ASGI
        # callers; do not let them inherit the generic loopback trust policy.
        if (
            is_operator_control_route(request.method, request.url.path)
            or is_operator_approval_route(request.method, request.url.path)
            or is_operator_setup_plan_route(request.method, request.url.path)
        ):
            if not _operator_token:
                return JSONResponse(
                    status_code=401,
                    content={
                        "ok": False,
                        "error": "operator authentication required",
                        "path": request.url.path,
                    },
                )
            presented = request.headers.get(OPERATOR_TOKEN_HEADER)
            if not token_matches(presented, _operator_token):
                return JSONResponse(
                    status_code=401,
                    content={
                        "ok": False,
                        "error": "operator authentication required",
                        "path": request.url.path,
                    },
                )
            # Only this authenticated transport can set the principal.  The
            # request body has no actor/principal field and is never consulted.
            request.state.operator_principal = OPERATOR_PRINCIPAL
            request.state.operator_session_id = OPERATOR_SESSION_ID
            response = await call_next(request)
            if response.status_code < 400:
                _emit_audit(request, response.status_code, OPERATOR_PRINCIPAL)
            return response

        if not is_mutating_route(request.method, request.url.path):
            return await call_next(request)
        if _operator_token:
            presented = request.headers.get(OPERATOR_TOKEN_HEADER)
            if not token_matches(presented, _operator_token):
                # Reject BEFORE the route runs -> no governance/overlay/LLM/
                # subprocess side effect, and no audit record for a denial.
                return JSONResponse(
                    status_code=401,
                    content={
                        "ok": False,
                        "error": "operator authentication required",
                        "path": request.url.path,
                    },
                )
            principal = OPERATOR_PRINCIPAL
        elif _is_loopback_request(request):
            # No token configured: local-dev loopback-trust only.
            principal = "operator:viz-loopback"
        else:
            # No token AND a non-loopback peer: refuse (fail closed).
            return JSONResponse(
                status_code=401,
                content={
                    "ok": False,
                    "error": (
                        "operator authentication required: this viz server has no "
                        "operator token configured, so mutating routes are "
                        "restricted to loopback. Set OKG_VIZ_OPERATOR_TOKEN to "
                        "allow authenticated remote operators."
                    ),
                    "path": request.url.path,
                },
            )
        response = await call_next(request)
        if response.status_code < 400:
            _emit_audit(request, response.status_code, principal)
        return response

    # Generation-keyed read cache + ETag revalidation for the console reads.
    # A console read is pinned to an immutable published generation, so a body for
    # (path, query, generation_id) never changes -- cache it once per generation
    # and let the browser revalidate cheaply (304) instead of recomputing ~0.85s
    # on every load. Read-only; an explicit refresh (Cache-Control: no-cache or
    # ?refresh=1) bypasses + refreshes the entry. See okg.viz.read_cache.
    read_cache = ReadCache(
        capacity=int(os.environ.get("OKG_VIZ_READ_CACHE_CAP", str(DEFAULT_CAPACITY)))
    )
    app.state.read_cache = read_cache

    @app.middleware("http")
    async def cache_console_reads(request: Request, call_next):
        if request.method != "GET" or not is_cacheable_path(request.url.path):
            return await call_next(request)
        # Resolve the live context; if the server is not ready yet we cannot key
        # correctly, so pass through uncached rather than risk a stale key.
        try:
            ctx = ctx_provider()
        except Exception:  # noqa: BLE001 - not-ready / boot races: never cache
            return await call_next(request)

        bypass = wants_bypass(
            request.headers.get("cache-control"),
            request.query_params.get("refresh"),
        )
        # An explicit refresh re-resolves the LATEST published generation before
        # reading, so the operator picks up newer publishes without a restart. A
        # FAILED re-resolve fails LOUD (the operator asked for the latest) -- never
        # a silent fall-back to the stale boot generation (the repo's P0 rule).
        if bypass:
            try:
                ctx.refresh_to_latest()
            except Exception as exc:  # noqa: BLE001
                logger.warning("viz refresh_to_latest failed", exc_info=True)
                return JSONResponse(
                    status_code=503,
                    content={
                        "ok": False,
                        "error": "refresh failed: could not re-resolve the latest published generation",
                        "detail": str(exc),
                        "path": request.url.path,
                    },
                )

        try:
            generation_id = ctx.session.generation_id
        except Exception:  # noqa: BLE001
            return await call_next(request)

        path = request.url.path
        # Normalize the query for a stable key/etag: sort params and drop the
        # client-only `refresh` flag so a refresh hits the SAME logical entry.
        norm_query = urlencode(
            sorted(
                (k, v)
                for k, v in request.query_params.multi_items()
                if k not in CLIENT_ONLY_PARAMS
            )
        )
        # The pre-read generation keys the LOOKUP + the 304 decision. A hit/304 is
        # safe because the etag is a pure function of the generation, and stored
        # entries are keyed on the body's own generation (below) -- so a lookup hit
        # is always a body of exactly that generation.
        etag = make_etag(path, norm_query, generation_id)
        key = cache_key(path, norm_query, generation_id)

        # Conditional revalidation: a matching ETag means the generation has not
        # advanced, so the client's copy is current -> cheap 304, no recompute.
        if not bypass and request.headers.get("if-none-match") == etag:
            return Response(
                status_code=304,
                headers={"ETag": etag, "Cache-Control": "no-cache", "X-Cache": "REVALIDATED"},
            )

        if not bypass:
            cached = read_cache.get(key)
            if cached is not None:
                return Response(
                    content=cached.body,
                    media_type=cached.media_type,
                    headers={"ETag": cached.etag, "Cache-Control": "no-cache", "X-Cache": "HIT"},
                )

        # MISS or explicit bypass: compute, capture the body, store it keyed on the
        # body's OWN generation_id (not the pre-read), so a concurrent refresh that
        # swaps the shared session mid-compose can never store a new-generation body
        # under an old-generation key (the cross-generation poisoning race).
        response = await call_next(request)
        if response.status_code != 200:
            return response
        body = b""
        async for chunk in response.body_iterator:
            body += chunk
        media_type = response.media_type or "application/json"

        def _rebuild(extra_headers: dict[str, str]) -> Response:
            headers = dict(response.headers)
            headers.pop("content-length", None)  # Response recomputes it
            headers.update(extra_headers)
            return Response(
                content=body, status_code=200, headers=headers, media_type=media_type
            )

        body_gen = extract_generation_id(body)
        if body_gen is None:
            # No generation marker -> cannot key safely; serve fresh, uncached.
            return _rebuild({"Cache-Control": "no-cache", "X-Cache": "UNCACHED"})
        store_etag = make_etag(path, norm_query, body_gen)
        store_key = cache_key(path, norm_query, body_gen)
        read_cache.put(
            store_key, CachedResponse(body=body, media_type=media_type, etag=store_etag)
        )
        return _rebuild({
            "ETag": store_etag,
            "Cache-Control": "no-cache",
            "X-Cache": "BYPASS" if bypass else "MISS",
        })

    # Registered AFTER the read cache, so it is OUTSIDE it -- Starlette makes
    # the LAST-registered middleware the outermost one. Registered before the
    # cache, this measured only the composes the cache missed: a HIT or a 304
    # returned from inside the cache middleware and never reached the timer, so
    # the response carried no `console;dur` at all and the perf suite -- whose
    # whole measurement is that header -- had nothing to read for the two
    # cacheable reads (`health`, `subtype`). Outside the cache it times what the
    # browser actually waits for, cache hit or miss (issue #1924).
    @app.middleware("http")
    async def console_server_timing(request: Request, call_next):
        """Expose end-to-end console endpoint cost for browser perf evidence."""
        if not request.url.path.startswith("/viz/api/console/"):
            return await call_next(request)
        started = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
        metric = f"console;dur={elapsed_ms:.1f}"
        existing = response.headers.get("Server-Timing")
        response.headers["Server-Timing"] = (
            f"{existing}, {metric}" if existing else metric
        )
        return response

    # Read-API authentication (issue #3278). Registered after the read cache,
    # the timer and the operator gate, so it is OUTSIDE all of them: a cache
    # HIT or a 304 can never answer an unauthenticated caller, and no route,
    # cache lookup or database read runs before the token is checked. CORS
    # stays outermost so a browser can still read the 401.
    if api_token is not None:
        _api_token = api_token

        @app.middleware("http")
        async def api_read_auth(request: Request, call_next):
            path = request.url.path
            if not is_api_path(path):
                return await call_next(request)
            if api_request_authorized(
                request.headers.get("authorization"), _api_token
            ):
                return await call_next(request)
            return JSONResponse(
                status_code=401,
                content={
                    "ok": False,
                    "error": "authentication required",
                    "path": path,
                },
                headers={
                    "WWW-Authenticate": f'Bearer realm="{API_AUTH_REALM}"',
                },
            )

    # CORS registered LAST -> OUTERMOST, so it wraps every other middleware
    # including the read-cache short-circuits (HIT / 304). The viz server lives on
    # its own port (default 8765); local-dev browser clients (e.g. the supervisor
    # console on :8768) call /viz/api/* cross-origin. Same-origin frontend reads
    # are unaffected.
    from fastapi.middleware.cors import CORSMiddleware
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"http://(127\.0\.0\.1|localhost)(:\d+)?",
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["Server-Timing"],
        max_age=600,
    )

    @app.exception_handler(Exception)
    async def structured_api_failure(request: Request, exc: Exception) -> JSONResponse:
        """Keep API failures machine-readable during DB/session churn."""
        path = request.url.path
        if not path.startswith("/viz/api/"):
            raise exc
        from okg.viz.context import CatalogSemanticsUnavailable
        if isinstance(exc, CatalogSemanticsUnavailable):
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "ready": False,
                    "error": "catalog_semantics_unavailable",
                    "path": path,
                    **exc.to_dict(),
                },
            )
        # The full text and traceback go to the server log, and only there:
        # an exception's text names the host, the address, the role and the
        # SQL (#2130). The body carries the one console failure shape.
        from okg.substrate.console_views.served_text import (
            console_failure_detail,
        )
        logger.error("unhandled viz API failure: %s", path)
        detail = console_failure_detail("unhandled_api_failure", exc)
        message = str(exc)
        lowered = message.lower()
        transient = any(
            token in lowered for token in (
                "connection",
                "database",
                "recovery",
                "session was opened",
                "viz boot failed",
                "generation",
            )
        )
        return JSONResponse(
            status_code=503 if transient else 500,
            content={
                "ok": False,
                "ready": False,
                **detail,
                "path": path,
            },
        )

    @app.get("/viz/api/health", response_model=None)
    def health() -> Any:
        """Liveness + load-status. Non-blocking: returns
        ``ready: false`` while the substrate boot is in progress.
        """
        try:
            from okg.viz.boot import get_load_status
            status = get_load_status()
        except ImportError:
            status = {}

        if is_ready is not None and not is_ready():
            ok = status.get("phase") != "error"
            payload = {**status, "ok": ok, "ready": False}
            return (
                JSONResponse(status_code=503, content=payload)
                if not ok else payload
            )
        try:
            ctx = ctx_provider()
            # A readiness probe is an explicit request for the UI's current
            # serving generation. Re-pin the actual context before reporting,
            # so health never claims alignment while handlers remain stale.
            ctx.refresh_to_latest()
            snapshot = getattr(ctx, "health_snapshot", None) or {}
            node_count = int(snapshot.get("node_count") or 0)
            edge_count = int(snapshot.get("edge_count") or 0)
            count_estimated = bool(snapshot.get("count_estimated", False))
            if node_count <= 0 and edge_count <= 0 and hasattr(ctx, "conn"):
                # Compatibility for tests or manually constructed contexts that
                # predate the boot-time metadata snapshot.
                with ctx.conn() as conn:
                    from okg.viz import reads
                    stats = reads.graph_stats(conn, ctx.session)
                    node_count = int(stats["node_count"])
                    edge_count = int(stats["edge_count"])
                    count_estimated = False
        except Exception as exc:
            logger.exception("/viz/api/health failed")
            payload = {
                **status,
                "ok": False,
                "ready": False,
                "phase": status.get("phase") or "error",
                "error": str(exc),
            }
            return JSONResponse(status_code=503, content=payload)
        return {
            **status,
            "ok": True,
            "ready": True,
            "node_count": node_count,
            "edge_count": edge_count,
            "count_estimated": count_estimated,
            "deployment_name": getattr(ctx, "deployment_name", None),
            "generation_id": getattr(getattr(ctx, "session", None), "generation_id", None),
            "pid": os.getpid(),
        }

    # --- Deployment chrome ------------------------------------------------
    # Resolved per-request so the manifest reflects the loaded ctx
    # rather than process-start environment. Cheap (just dict lookups).

    @app.get("/viz/api/deployment")
    def deployment_metadata() -> dict[str, Any]:
        """Identity of the deployment this viz process serves.

        This payload used to carry two extra keys advertising the
        operator-local chat drawer's availability. That surface is retired
        (PACT ``retire-viz-chat``); a deployment's chat identity is now
        ``chat.enabled`` in its manifest, not a file beside it, so there is
        nothing here for a viz client to probe. The removed keys are named
        in ``archive/chat/viz-chat-2026-08-04/INDEX.md`` at git commit
        ``aa9a7eed8015`` (the tree no longer carries ``archive/``).
        """
        port = _bound_port()
        host = _bound_host()
        try:
            ctx = ctx_provider()
            payload = current_deployment(
                manifest=ctx.manifest,
                bound_port=port,
                host=host,
            )
        except Exception:
            logger.exception("/viz/api/deployment: failed to resolve ctx")
            payload = current_deployment(
                manifest=None, bound_port=port, host=host,
            )
        return payload

    @app.get("/viz/api/deployments")
    def deployments_registry() -> dict[str, Any]:
        """Switch-target bookmarks discovered from the repo manifests.

        Also the console's deployment-theme read (PACT
        ``operator-ui-deployment-theme``): each entry carries a ``viz_theme``
        payload of ``{"status", "tokens"}`` built by ``okg.viz.registry``.
        Served here rather than on a new route because this is the read the
        console already performs — the theme is another property of a
        deployment the switcher already enumerates, not a new resource.
        """
        port = _bound_port()
        host = _bound_host()
        current_name = None
        current_manifest = None
        try:
            ctx = ctx_provider()
            current_name = ctx.deployment_name
            # The served manifest, so this deployment's entry carries the same
            # browser links /viz/api/deployment serves, even when the manifest
            # lives outside the repo's deployments/ (OKG_DEPLOYMENTS_DIR).
            current_manifest = ctx.manifest
        except Exception:
            logger.debug("deployment context unavailable for registry endpoint", exc_info=True)
        entries = load_registry(
            deployments_root=_deployments_root(),
            current_name=current_name,
            current_port=port,
            host=host,
            current_manifest=current_manifest,
        )
        return {"entries": entries}

    def _registry_probe_ports() -> set[int]:
        """Ports the deployment registry actually lists (plus the server's own
        bound port). The probe is restricted to these so it cannot be used as a
        localhost port scanner (secure-viz-operator-routes)."""
        current_name = None
        try:
            current_name = ctx_provider().deployment_name
        except Exception:
            logger.debug("probe: ctx unavailable", exc_info=True)
        ports: set[int] = set()
        try:
            entries = load_registry(
                deployments_root=_deployments_root(),
                current_name=current_name,
                current_port=_bound_port(),
                host=_bound_host(),
            )
        except Exception:
            logger.debug("probe: registry load failed", exc_info=True)
            entries = []
        for entry in entries or []:
            raw = entry.get("port")
            if not raw and entry.get("url"):
                raw = urlparse(str(entry["url"])).port
            try:
                if raw:
                    ports.add(int(raw))
            except (TypeError, ValueError):
                continue
        bound = _bound_port()
        if bound:
            try:
                ports.add(int(bound))
            except (TypeError, ValueError):
                # Ignore malformed bound-port metadata and keep scanning candidates.
                pass
        return ports

    @app.get("/viz/api/deployments/probe")
    def deployments_probe(url: str) -> dict[str, Any]:
        """Same-origin reachability probe for switcher targets.

        The browser logs cross-origin connection refusals as console
        errors even when fetch failures are caught. Probe localhost
        deployment ports from the server side instead so the switcher
        can show unreachable siblings without polluting the console.
        """
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise HTTPException(status_code=400, detail="unsupported probe scheme")
        host = parsed.hostname
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise HTTPException(status_code=400, detail="only local deployment probes are supported")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        # Restrict to registry-listed ports (fail closed on an empty registry)
        # so the probe cannot scan arbitrary local ports.
        allowed_ports = _registry_probe_ports()
        if not allowed_ports:
            raise HTTPException(
                status_code=403,
                detail="no configured deployment targets to probe",
            )
        if port not in allowed_ports:
            raise HTTPException(
                status_code=403,
                detail="probe target port is not a configured deployment",
            )
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return {"reachable": True}
        except OSError as exc:
            return {"reachable": False, "error": exc.__class__.__name__}

    # --- Curated tool palette ---------------------------------------------
    @app.get("/viz/api/tools")
    def tools_describe() -> dict[str, Any]:
        """Return the palette manifest (name → input schema)."""
        return {"tools": describe_allowlist()}

    @app.post("/viz/api/tools/{name}")
    def tools_dispatch(
        name: str,
        body: dict[str, Any] | None = Body(default=None),
    ) -> dict[str, Any]:
        entry = ALLOWLIST.get(name)
        if entry is None:
            raise HTTPException(status_code=404, detail="tool not available")
        model, handler = entry
        raw = body or {}
        try:
            parsed = model(**raw)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        ctx = ctx_provider()
        try:
            result = handler(ctx, parsed)
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("tool %s raised", name)
            raise HTTPException(status_code=500, detail=str(exc))
        return {"name": name, "result": result}

    # --- Dedicated coordination operator controls ------------------------
    # Registered directly on this app, outside build_router and the generic
    # tools/MCP dispatcher.  The exact three routes are intentionally omitted
    # from OpenAPI and the browser route inventory.

    def _dispatch_operator_control(
        request: Request,
        operation: ControlOperation,
        body: OperatorControlRequest,
    ) -> Mapping[str, Any] | Response:
        if body.operation != operation:
            raise HTTPException(
                status_code=422,
                detail="request operation does not match the control path",
            )
        principal = getattr(request.state, "operator_principal", None)
        # Defense in depth if this route is ever mounted under a different ASGI
        # stack: the service must still see the one server-stamped principal.
        if principal != OPERATOR_PRINCIPAL:
            raise HTTPException(
                status_code=401,
                detail="operator authentication required",
            )
        service = getattr(app.state, "operator_control_service", None)
        if service is None:
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error": "operator coordination control unavailable",
                    "path": request.url.path,
                },
            )
        # The native service receives no Request, context, token, DSN, or
        # process identity: only the server-stamped principal and validated
        # non-secret request contract.
        try:
            return invoke_operator_control(
                service,
                principal=principal,
                request=body,
            )
        except PermissionError:
            raise HTTPException(
                status_code=401,
                detail="operator authentication required",
            ) from None
        except Exception as exc:  # noqa: BLE001 - redact native adapter failures.
            logger.error(
                "operator coordination control unavailable (%s)",
                type(exc).__name__,
            )
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error": "operator coordination control unavailable",
                    "path": request.url.path,
                },
            )

    def _dispatch_setup_plan(
        request: Request,
        body: SetupPlanRequest,
    ) -> Mapping[str, Any] | Response:
        principal = getattr(request.state, "operator_principal", None)
        if principal != OPERATOR_PRINCIPAL:
            raise HTTPException(
                status_code=401,
                detail="operator authentication required",
            )
        service = getattr(app.state, "setup_plan_service", None)
        if service is None:
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error": "coordination setup plan unavailable",
                    "path": request.url.path,
                },
            )
        try:
            return invoke_setup_plan(service, principal=principal, request=body)
        except PermissionError:
            raise HTTPException(
                status_code=401,
                detail="operator authentication required",
            ) from None
        except Exception as exc:  # noqa: BLE001 - fail closed and redact adapter errors.
            logger.error(
                "coordination setup plan unavailable (%s)", type(exc).__name__
            )
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error": "coordination setup plan unavailable",
                    "path": request.url.path,
                },
            )

    @app.post(
        "/viz/api/coordination/setup/plan",
        include_in_schema=False,
        response_model=None,
    )
    def coordination_setup_plan(
        request: Request,
        body: SetupPlanRequest,
    ) -> Mapping[str, Any] | Response:
        """Return a read-only, authority-probed setup plan."""
        return _dispatch_setup_plan(request, body)

    @app.post(
        "/viz/api/coordination/control/bootstrap",
        include_in_schema=False,
        response_model=None,
    )
    def coordination_control_bootstrap(
        request: Request,
        body: OperatorControlRequest,
    ) -> Mapping[str, Any]:
        return _dispatch_operator_control(request, "bootstrap", body)

    @app.post(
        "/viz/api/coordination/control/fallback",
        include_in_schema=False,
        response_model=None,
    )
    def coordination_control_fallback(
        request: Request,
        body: OperatorControlRequest,
    ) -> Mapping[str, Any]:
        return _dispatch_operator_control(request, "fallback", body)

    @app.post(
        "/viz/api/coordination/control/reactivation",
        include_in_schema=False,
        response_model=None,
    )
    def coordination_control_reactivation(
        request: Request,
        body: OperatorControlRequest,
    ) -> Mapping[str, Any]:
        return _dispatch_operator_control(request, "reactivation", body)

    # --- Dedicated coordination approval surface ------------------------
    # These routes are separate from the exact three setup-control routes
    # above.  They are deliberately absent from the generic tools/MCP
    # dispatcher and OpenAPI.  The body names only an existing digest-bound
    # approval; native coordination remains authoritative for action,
    # resource, scope, and requester identity.

    def _dispatch_operator_approval(
        request: Request,
        body: ApprovalControlRequest,
    ) -> Mapping[str, Any] | Response:
        principal = getattr(request.state, "operator_principal", None)
        session_id = getattr(request.state, "operator_session_id", None)
        if principal != OPERATOR_PRINCIPAL or session_id != OPERATOR_SESSION_ID:
            raise HTTPException(
                status_code=401,
                detail="operator authentication required",
            )
        service = getattr(app.state, "approval_control_service", None)
        if service is None:
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error": "operator approval service unavailable",
                    "path": request.url.path,
                },
            )
        try:
            # Only the server-stamped identity and validated digest/decision
            # cross this boundary.  The adapter must derive protected fields
            # from the stored native approval row.
            return invoke_approval_control(
                service,
                principal=principal,
                session_id=session_id,
                request=body,
            )
        except PermissionError as exc:
            # An adapter refusing an identity that did not originate at this
            # route is an authentication failure, not a database error.
            del exc
            raise HTTPException(
                status_code=401,
                detail="operator authentication required",
            ) from None
        except Exception as exc:  # noqa: BLE001 - production service failures are redacted.
            # Never echo a DSN/token or native database exception through the
            # HTTP response.  Keep the log type-only for the same reason.
            logger.error(
                "operator approval service unavailable (%s)",
                type(exc).__name__,
            )
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error": "operator approval service unavailable",
                    "path": request.url.path,
                },
            )

    @app.post(
        "/viz/api/coordination/approval/decide",
        include_in_schema=False,
        response_model=None,
    )
    def coordination_approval_decide(
        request: Request,
        body: ApprovalDecisionRequest,
    ) -> Mapping[str, Any] | Response:
        return _dispatch_operator_approval(request, body)

    @app.post(
        "/viz/api/coordination/approval/revoke",
        include_in_schema=False,
        response_model=None,
    )
    def coordination_approval_revoke(
        request: Request,
        body: ApprovalRevokeRequest,
    ) -> Mapping[str, Any] | Response:
        return _dispatch_operator_approval(request, body)

    # --- Data endpoints ----------------------------------------------------
    app.include_router(build_router(ctx_provider))

    def _archived_frontend_response(
        path: str,
        *,
        route_path: str | None = None,
    ) -> JSONResponse:
        if path == "api" or path.startswith("api/"):
            raise HTTPException(status_code=404, detail="unknown viz API route")
        return JSONResponse(
            status_code=410,
            content={
                "ok": False,
                "archived": True,
                "path": route_path or (f"/viz/{path}" if path else "/"),
                "message": (
                    "The legacy OKG frontend was archived. Use /viz/api/* "
                    "read endpoints or build against "
                    "docs/reference/operator-ui-single-app-contract.md."
                ),
                # A git object, not a working-tree path: PACT
                # layout-history-openspec deleted archive/ from the tree.
                "archive": _LEGACY_VIZ_ARCHIVE,
                "contract": "docs/reference/operator-ui-single-app-contract.md",
            },
        )

    # --- Archived frontend boundary ---------------------------------------
    # The old static /viz app, deployment supervisor console, markdown UI view
    # loader, and console-router shim were archived under
    # archive/ui/legacy-viz-2026-06-17, which now lives only in git at
    # _LEGACY_VIZ_ARCHIVE. Keep /viz/api/* active above, but fail
    # old frontend routes loudly instead of silently mounting archived bundles.
    @app.get("/", include_in_schema=False)
    def _root_archived() -> JSONResponse:
        return _archived_frontend_response("")

    @app.get("/viz", include_in_schema=False)
    @app.get("/viz/", include_in_schema=False)
    def _viz_archived() -> JSONResponse:
        return _archived_frontend_response("", route_path="/viz")

    @app.get("/viz/{path:path}", include_in_schema=False)
    def _viz_archived_path(path: str) -> JSONResponse:
        return _archived_frontend_response(path)

    return app


def mount_viz(
    ctx_provider: Callable[[], VizContext],
    port: int | None = None,
    host: str = "127.0.0.1",
    is_ready: Callable[[], bool] | None = None,
) -> threading.Thread | None:
    """Start the viz HTTP server in a background thread.

    Returns the daemon thread running uvicorn, or `None` if no
    port is configured (`OKG_VIZ_PORT` unset and `port` not
    passed).
    """
    resolved_port = port if port is not None else _bound_port()
    if resolved_port is None:
        return None

    import asyncio

    import uvicorn

    app = build_app(ctx_provider, is_ready=is_ready)
    config = uvicorn.Config(
        app,
        host=host,
        port=resolved_port,
        log_level="info",
        access_log=False,
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]

    def _run() -> None:
        try:
            asyncio.run(server.serve())
        except Exception:  # pragma: no cover - background thread
            logger.exception("viz server crashed")

    thread = threading.Thread(target=_run, name="okg-viz", daemon=True)
    thread.start()
    logger.info(
        "OKG viz server listening on http://%s:%d/viz/", host, resolved_port,
    )
    return thread
