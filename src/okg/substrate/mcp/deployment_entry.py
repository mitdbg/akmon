"""Deployment-side entry point for the substrate MCP server.

Each deployment's ``deployments/<name>/server.py`` calls
``run_for_deployment(<name>)`` and nothing else. This module
owns the environment preflight + substrate launch so the
deployment stubs stay trivial.

Preflight:

- The deployment DSN must resolve from the manifest/environment contract.
- The named deployment must have at least one published
  generation in the read-side ``okg.v_generations`` ledger.

If either check fails, the process exits non-zero with a
human-readable message pointing at
``docs/migrating_to_published_generations.md``. The 4
dev-artifact deployments (cms, intersystems, memex, af_pubs)
go dark by design until re-bootstrapped onto substrate.
"""

from __future__ import annotations

import os
import sys
import logging
from typing import NoReturn

_MIGRATION_GUIDE = "docs/migrating_to_published_generations.md"
log = logging.getLogger(__name__)


def _die(msg: str) -> NoReturn:
    # Every message may quote the DSN or a driver error about it (#2285).
    from okg.substrate.db.dsn import redact_text

    print(
        f"Error: {redact_text(msg)}\n"
        f"See {_MIGRATION_GUIDE} for how to bootstrap a "
        f"deployment onto the substrate.",
        file=sys.stderr,
    )
    raise SystemExit(1)


def _preflight(dsn: str, deployment_name: str) -> None:
    """Reachability + published-generation check before launching
    the MCP stdio loop. Errors are routed through ``_die``.
    """
    from okg.substrate.db.dsn import redact

    try:
        from okg.substrate.db.connection import connect as db_connect
    except ImportError:
        _die("psycopg not installed; substrate MCP requires it")

    try:
        # okg's factory carries the session search_path pin (issue #1968).
        with db_connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM okg.v_generations "
                    "WHERE deployment_name = %s "
                    "AND status = 'published'",
                    (deployment_name,),
                )
                (n,) = cur.fetchone()
    except Exception as exc:
        _die(f"could not reach substrate DSN or query okg.v_generations: {exc}")

    if n == 0:
        _die(
            f"no published generation for deployment "
            f"{deployment_name!r} in the substrate at {redact(dsn)}"
        )


def run_for_deployment(deployment_name: str) -> None:
    """Preflight the environment + launch the substrate MCP
    stdio server for the named deployment. Blocks until the
    MCP client disconnects.

    Usage — one line in every ``deployments/<name>/server.py``::

        from okg.substrate.mcp.deployment_entry import run_for_deployment
        run_for_deployment("<name>")
    """
    # A deployment's server.py is a process entry point: keep the bytecode of
    # the deployment's own modules out of its repository
    # (okg.substrate.bytecode_cache).
    from okg.substrate.bytecode_cache import enter_user_bytecode_cache

    enter_user_bytecode_cache()
    from okg.substrate.deployments.admission import require_active_deployment
    require_active_deployment(deployment_name)
    dsn_resolution = _deployment_entry_dsn_resolution(deployment_name)
    dsn = dsn_resolution.get("dsn")
    if not dsn:
        _die(
            f"{dsn_resolution.get('reason') or 'deployment DSN is not set'}; "
            f"cannot launch substrate MCP for {deployment_name!r}"
        )

    _preflight(dsn, deployment_name)

    from okg.substrate.deployments import resolve_deployment

    try:
        manifest = resolve_deployment(deployment_name)
    except Exception as exc:
        _die(f"could not resolve deployment manifest for {deployment_name!r}: {exc}")

    instructions = None
    if manifest.mcp_instructions is not None:
        try:
            instructions = manifest.mcp_instructions.read_text()
        except OSError as exc:
            _die(f"could not read MCP instructions for {deployment_name!r}: {exc}")
        log.info(
            "loaded MCP instructions for deployment %s from %s",
            deployment_name,
            manifest.mcp_instructions,
        )

    from okg.substrate.mcp.server import run_stdio
    embedder = _verified_embedder_for_manifest(manifest)
    stdio_authority = {
        "dsn": dsn,
        "authority": dsn_resolution.get("authority", {}),
        "target": dsn_resolution.get("target"),
    }
    if instructions is None:
        run_stdio(
            dsn=dsn,
            dsn_authority=stdio_authority,
            deployment_name=deployment_name,
            embedder=embedder,
        )
    else:
        run_stdio(
            dsn=dsn,
            dsn_authority=stdio_authority,
            deployment_name=deployment_name,
            embedder=embedder,
            instructions=instructions,
        )


def _deployment_entry_dsn_resolution(deployment_name: str) -> dict:
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    resolution = resolve_deployment_dsn_authority(
        deployment_name,
        local_profile_authoritative=deployment_self_hosted_authoritative(
            deployment_name
        ),
    )
    if resolution.get("dsn") or resolution.get("reason"):
        return resolution
    return {
        "dsn": None,
        "reason": f"deployment {deployment_name!r} DSN authority is unresolved",
        "next_action": "configure_okg_dsn",
        "authority": {
            "mode": "unresolved",
            "deployment": deployment_name,
        },
    }


def _verified_embedder_for_manifest(manifest):
    """Build the MCP query backend from deployment authority and probe it."""
    import warnings

    from okg.substrate.deployments import DeploymentConfigError
    from okg.substrate.embeddings import build_embedding_execution_plan

    configured = manifest.embedding_config
    legacy_env = os.environ.get("OKG_MCP_EMBEDDER")
    if legacy_env and legacy_env.strip():
        env_spec = legacy_env.strip()
        if not configured.enabled or env_spec != configured.backend:
            raise DeploymentConfigError(
                "OKG_MCP_EMBEDDER conflicts with deployment.yaml's canonical "
                "embeddings backend; process-local environment cannot override "
                "deployment authority"
            )
        warnings.warn(
            "OKG_MCP_EMBEDDER is deprecated for deployment MCP entry; the "
            "canonical deployments backend already supplies this value",
            UserWarning,
            stacklevel=2,
        )
    # A paid backend makes no probe call at server start; its only paid call
    # is each semantic query's own embedding (PACT openrouter-embeddings-hnsw).
    plan = build_embedding_execution_plan(configured, paid_backend="no_probe")
    # The verified plan implements the Embedder protocol. Keep the capability
    # evidence attached to the object that crosses into semantic search rather
    # than unwrapping it back into an unlabelled backend instance.
    return plan if plan.enabled else None
