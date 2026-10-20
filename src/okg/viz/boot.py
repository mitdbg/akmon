"""Substrate-native viz boot.

Resolves (DSN, deployment_name, generation_id) → a pinned
`VizContext`. No `Backbone`, no snapshot pickle, no module
ontology rehydration — the substrate catalog is the ontology
source of truth and substrate tables are the graph.

Background loader: `preload_in_background` opens the substrate
session in a daemon thread so the UI can render the loading
state immediately. `get_ctx_lazy` returns a `(provider,
is_ready)` pair that `app.build_app` consumes — same contract
the legacy boot exposed, but the underlying state is a
`VizContext` instead of `OKGTools`.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

import psycopg
from okg.substrate import fastyaml
from psycopg_pool import ConnectionPool

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.db.connection import connection_kwargs
from okg.substrate.db.pool import release_session_advisory_locks

from okg.substrate.deployments.chat_block import reject_retired_viz_chat
from okg.substrate.mcp.session import open_session
from okg.viz.config import load_default_config, load_viz_config
from okg.viz.context import VizContext

logger = logging.getLogger(__name__)


# Module-level state for the lazy loader.
_lock = threading.Lock()
_state: dict[str, Any] = {
    "phase": "idle",
    "started_at": None,
    "ctx": None,
    "error": None,
}


def _set_phase(p: str) -> None:
    with _lock:
        _state["phase"] = p
        if _state["started_at"] is None:
            _state["started_at"] = time.monotonic()


def get_load_status() -> dict[str, Any]:
    """Snapshot of the current load progress for `/viz/api/health`."""
    with _lock:
        elapsed = (
            time.monotonic() - _state["started_at"]
            if _state["started_at"] is not None
            else 0.0
        )
        return {
            "phase": _state["phase"],
            "elapsed_seconds": round(elapsed, 1),
            "error": _state["error"],
        }


def _find_deployment_dir(deployment_name: str) -> Path:
    """Locate `deployments/<name>/` under the repo root.

    Delegates to the shared substrate helper
    (`okg.substrate.deployments.find_deployment_dir`) per
    `wire-deployment-enrichers` (2026-04-26). Wraps the
    None-return into a raise for callers that need an
    existence guarantee."""
    from okg.substrate.deployments import find_deployment_dir
    found = find_deployment_dir(deployment_name)
    if found is None:
        raise FileNotFoundError(
            f"deployment directory not found: {deployment_name}"
        )
    return found


def _load_manifest(deployment_dir: Path) -> dict[str, Any]:
    """Parse `deployment.yaml` directly. No `resolve_deployment`.

    Reading raw is deliberate — this boots the viz server and must not pull
    in the full strict resolve — but ONE refusal is checked by hand: a
    retired `viz.chat` block (PACT `retire-viz-chat`). This process used to
    host that surface, so it is the likeliest place an operator with a stale
    manifest lands, and `_load_viz_config_for` below merges the whole `viz:`
    block without validating a single sub-key. Skipping the check here would
    start the server, swallow the retired block, and configure nothing —
    exactly the silent outcome the refusal exists to prevent.
    """
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"deployment.yaml missing under {deployment_dir}"
        )
    with manifest_path.open() as f:
        raw = fastyaml.safe_load(f) or {}
    reject_retired_viz_chat(raw, manifest_path=manifest_path)
    raw["_deployment_dir"] = str(deployment_dir)
    return raw


def _load_viz_config_for(
    manifest: dict[str, Any], deployment_dir: Path,
) -> dict[str, Any]:
    """Viz-config resolution: inline `viz:` block, else
    `viz_config:` reference, else built-in default.

    `load_viz_config(*, inline=, path=)` is keyword-only with
    inline > path > default precedence; pass the deployment's
    inline block when it exists, else the referenced path, else
    nothing (the default is returned).
    """
    if "viz" in manifest and isinstance(manifest["viz"], dict):
        return load_viz_config(inline=manifest["viz"])
    ref = manifest.get("viz_config")
    if ref:
        path = deployment_dir / ref
        if path.is_file():
            return load_viz_config(path=path)
    return load_default_config()


def _generation_health_snapshot(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    generation_id: str,
) -> dict[str, Any]:
    """Cheap liveness payload seeded from publish-time metadata."""
    from okg.substrate.graph_store import ChronosGraphStore

    try:
        generation = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).get_generation(generation_id)
        metadata = dict(generation.metadata or {})
    except Exception:  # noqa: BLE001 - liveness falls back to an empty snapshot.
        logger.debug(
            "viz boot could not read generation health metadata",
            exc_info=True,
        )
        metadata = {}

    def _count(key: str) -> int:
        value = metadata.get(key)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    return {
        "deployment_name": deployment_name,
        "generation_id": generation_id,
        "node_count": _count("node_count"),
        "edge_count": _count("edge_count"),
        "count_estimated": False,
    }


def load(
    *,
    dsn: Optional[str] = None,
    deployment_name: str = "default",
    generation_id: Optional[int | str] = None,
    min_conns: int = 2,
    max_conns: int = 16,
) -> VizContext:
    """Build a `VizContext` against a substrate DSN. Synchronous.

    `dsn` falls back to `OKG_DSN` env var. `generation_id`
    defaults to the latest published generation for the named
    deployment.
    """
    effective_dsn = dsn or os.environ.get("OKG_DSN")
    if not effective_dsn:
        raise RuntimeError(
            "no substrate DSN — pass --dsn or set OKG_DSN"
        )

    _set_phase("locating deployment")
    deployment_dir = _find_deployment_dir(deployment_name)
    manifest = _load_manifest(deployment_dir)
    config = _load_viz_config_for(manifest, deployment_dir)

    _set_phase("opening connection pool")
    # `connection_kwargs` is the same factory `okg.substrate.db.pool` uses, and
    # it reads the DSN's own `options` first so nothing an operator encoded is
    # dropped. Beyond the session search_path pin this change is about (issue
    # #1968), it gives this pool three bounds it did not have: TCP keepalives
    # (~60s to notice a dead client), a 10-second connect timeout in place of
    # blocking forever on a TCP stall, and a 5-minute
    # idle_in_transaction_session_timeout so an abandoned viz session cannot
    # hold locks indefinitely. The pool itself stays viz-owned and viz-sized:
    # `get_pool` is process-shared and takes its width from the
    # `db.shared_pool` term, which is not what this caller wants.
    pool = ConnectionPool(
        conninfo=effective_dsn,
        min_size=min_conns,
        max_size=max_conns,
        kwargs={**connection_kwargs(dsn=effective_dsn), "autocommit": True},
        reset=release_session_advisory_locks,
        open=True,
        name="okg-viz",
    )

    _set_phase("opening substrate session")
    with db_connect(effective_dsn, autocommit=True) as seed_conn:
        session = open_session(
            seed_conn,
            deployment_name=deployment_name,
            requested_generation=generation_id,
        )
        health_snapshot = _generation_health_snapshot(
            seed_conn,
            deployment_name=deployment_name,
            generation_id=session.generation_id,
        )

    logger.info(
        "viz boot ok: deployment=%s generation=%s catalog_version=%d",
        deployment_name, session.generation_id, session.catalog_version_id,
    )
    _set_phase("ready")

    return VizContext(
        dsn=effective_dsn,
        deployment_name=deployment_name,
        manifest=manifest,
        config=config,
        pool=pool,
        session=session,
        deployment_dir=deployment_dir,
        health_snapshot=health_snapshot,
        generation_refresh_enabled=generation_id is None,
    )


def preload_in_background(
    *,
    dsn: Optional[str] = None,
    deployment_name: Optional[str] = None,
    generation_id: Optional[int | str] = None,
) -> None:
    """Kick off the substrate load on a daemon thread. Resolves
    deployment_name from `OKG_DEPLOYMENT` env var when not given.
    """
    target_deployment = (
        deployment_name
        or os.environ.get("OKG_DEPLOYMENT")
        or "default"
    )

    def _run() -> None:
        try:
            ctx = load(
                dsn=dsn,
                deployment_name=target_deployment,
                generation_id=generation_id,
            )
            with _lock:
                _state["ctx"] = ctx
        except Exception as exc:
            logger.exception("viz boot failed")
            with _lock:
                _state["error"] = str(exc)
                _state["phase"] = "error"

    thread = threading.Thread(target=_run, name="okg-viz-boot", daemon=True)
    thread.start()


def get_ctx_lazy() -> tuple:
    """Return `(ctx_provider, is_ready)`.

    `ctx_provider()` blocks until the load completes (or raises).
    `is_ready()` is non-blocking — returns True once `ctx` is set.
    """
    def is_ready() -> bool:
        with _lock:
            return _state["ctx"] is not None

    def provider() -> VizContext:
        # Wait for the background load to finish. If it's not
        # yet started, run it inline.
        with _lock:
            ctx = _state["ctx"]
            err = _state["error"]
        if ctx is not None:
            return ctx
        if err is not None:
            raise RuntimeError(f"viz boot failed: {err}")
        # Block until ready — poll loop with short sleep. Background
        # load runs in another thread.
        if _state["phase"] == "idle":
            preload_in_background()
        while True:
            with _lock:
                ctx = _state["ctx"]
                err = _state["error"]
            if ctx is not None:
                return ctx
            if err is not None:
                raise RuntimeError(f"viz boot failed: {err}")
            time.sleep(0.05)

    return provider, is_ready
