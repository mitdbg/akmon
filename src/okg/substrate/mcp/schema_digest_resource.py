"""Schema-digest payload builder for the MCP `okg://schema-digest/<dep>` resource.

Extracted from ``mcp/server.py`` by ``extract-mcp-schema-digest-resource``
(2026-05) per the MCP audit. The `@mcp.resource` decorator stays in
``server.py`` (it needs the `mcp` FastMCP instance from build_server's
closure), but the payload-building body lifts out as a function that
takes its closure as explicit parameters.

Public surface:

  - ``build_digest_payload(pool, deployment_name, digest_cache,
    resolve_latest_fn, digest_ttl_s=3600.0)`` — borrow a conn,
    resolve the latest pin, build + cache the JSON digest. Falls
    back to a minimal digest if the full encoding exceeds
    ``MAX_DIGEST_BYTES`` (a warning is logged).

The cache shape is ``{deployment_name: (expires_at, cv_id, payload)}``;
TTL defaults to 1 hour. The cache is owned by the caller (server.py
keeps it in build_server's closure so resources stay per-server).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable



__all__ = ["build_digest_payload"]


_log = logging.getLogger(__name__)


def build_digest_payload(
    *,
    pool,
    deployment_name: str,
    digest_cache: dict[str, tuple[float, int, str]],
    resolve_latest_fn: Callable[..., tuple[Any, ...]],
    digest_ttl_s: float = 3600.0,
) -> str:
    """Resolve latest pin, build the digest, JSON-encode.

    Falls back to a minimal digest when the full encoding exceeds
    ``MAX_DIGEST_BYTES`` (logged as a warning so operators can see
    when a deployment outgrew the budget).
    """
    from okg.substrate.mcp.schema_digest import (
        fetch_minimal_schema_counts,
        generate_schema_digest,
        minimal_digest,
    )
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    conn = pool.getconn()
    try:
        # `_resolve_latest` also returns the compat id and the branch the
        # checkpoint was published on; the digest needs only the pin and the
        # catalog version. Slice rather than unpack so a later change to the
        # resolver's arity cannot break this resource.
        gen_id, cv_id = resolve_latest_fn(conn, use_cache=False)[:2]
        now = time.monotonic()

        # The digest is a subject-less resource: its counts and attr keys
        # are whole-graph row aggregates that per-subject Nomos visibility
        # cannot be applied to. Under `enforce` (the only mode that can
        # hide rows — issue #627) the row-derived sections are omitted
        # rather than leaked; catalog-shaped discovery stays available via
        # inspect(ontology) and the per-row read surfaces. The mode check
        # runs BEFORE the cache so flipping a deployment to enforce takes
        # effect immediately instead of serving a stale full digest for up
        # to the cache TTL.
        if deployment_nomos_enforcement_mode(deployment_name) == "enforce":
            payload = json.dumps(
                {
                    "deployment": deployment_name,
                    "pinned_generation_id": gen_id,
                    "subtype_count": None,
                    "subtypes": None,
                    "edge_type_count": None,
                    "edge_types": None,
                    "edge_type_counts": None,
                    "truncated": True,
                    "note": (
                        "Nomos enforce mode: row-derived schema counts "
                        "are omitted (a subject-less resource cannot "
                        "apply per-subject visibility) — use "
                        "inspect(target={'kind':'subtype','subtype':<type>}) "
                        "for one type's catalog entry, in pages, and "
                        "search/filter/aggregate for visibility-accurate "
                        "reads"
                    ),
                },
                separators=(",", ":"),
            )
            # Deliberately NOT cached: the stub is free to build, and
            # keeping it out of the cache means a mode flip in either
            # direction takes effect immediately (the cache key carries no
            # mode).
            return payload

        cached = digest_cache.get(deployment_name)
        if (
            cached is not None
            and cached[0] > now
            and cached[1] == cv_id
        ):
            return cached[2]

        # Build inside a transaction — the digest queries set
        # the pin GUC on the conn and we want it scoped. With autocommit
        # off, psycopg opens that transaction with its own BEGIN before
        # the first digest query; an explicit "BEGIN" here was a second
        # one inside it, which PostgreSQL ignores with a warning (lane C
        # item 238a). psycopg refuses to switch autocommit off on a
        # session that is already in a transaction, so the transaction
        # below is always this function's own.
        try:
            conn.autocommit = False
            try:
                digest = generate_schema_digest(
                    conn,
                    deployment_name=deployment_name,
                    pinned_generation_id=gen_id,
                )
            except ValueError as e:
                # Budget overflow — fall back to minimal.
                _log.warning(
                    "schema digest oversize for %s: %s; "
                    "falling back to minimal digest",
                    deployment_name, e,
                )
                counts, edge_counts = fetch_minimal_schema_counts(
                    conn,
                    pinned_generation_id=gen_id,
                )
                digest = minimal_digest(
                    deployment_name=deployment_name,
                    pinned_generation_id=gen_id,
                    subtype_counts=counts,
                    edge_type_counts=edge_counts,
                    minimal_reason=str(e),
                )
            payload = json.dumps(digest, separators=(",", ":"))
            with conn.cursor() as cur:
                cur.execute("ROLLBACK")
        finally:
            # A failed digest query (e.g. okg.require_graph_checkout()'s
            # descriptive checkout-required error on chronos-native
            # deployments, per fix-nomos-read-path-ux) leaves the
            # transaction INERROR; restoring autocommit on an aborted
            # transaction raises an unrelated ProgrammingError that MASKS
            # the descriptive error. Roll back first so the original
            # exception propagates intact.
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 — never shadow the original error
                pass
            conn.autocommit = True
    finally:
        pool.putconn(conn)

    digest_cache[deployment_name] = (
        now + digest_ttl_s, cv_id, payload,
    )
    return payload
