"""Agent coding-context packet builder.

The packet is intentionally advisory: it summarizes generation-pinned
graph evidence and names the live checks an agent still needs before
editing. It does not read the filesystem or infer dirty worktree state.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
from okg.substrate.db.connection import connect as db_connect

from okg.substrate.errors import SubstrateError


# Substrate-owned focused-test mapping. Per
# `retire-pi-prefix-commands` Phase 4: deployment-specific focus
# tuples (formerly the `okg-workspace/doctor_checks.py` entries)
# live in the owning deployment at
# `deployments/<name>/agent_context_focus.py`; this substrate
# table covers only generic substrate paths.
_FOCUSED_TESTS_BY_PATH: tuple[tuple[str, str, str], ...] = (
    (
        "src/okg/substrate/agent_context.py",
        "uv run --extra dev pytest tests/substrate/cli/test_agent_context_cli.py -q",
        "agent context packet builder changed",
    ),
    (
        "src/okg/substrate/cli/main.py",
        "uv run --extra dev pytest tests/substrate/cli/test_argparse_acceptance.py "
        "tests/substrate/cli/test_agent_context_cli.py -q",
        "agent context CLI dispatch changed",
    ),
    (
        "src/okg/substrate/mcp/server.py",
        "uv run --extra dev pytest tests/substrate/test_mcp_server.py "
        "tests/substrate/test_mcp_tracing.py -q",
        "agent context MCP surface changed",
    ),
    (
        "tests/substrate/cli/test_agent_context_cli.py",
        "uv run --extra dev pytest tests/substrate/cli/test_agent_context_cli.py -q",
        "agent context CLI tests changed",
    ),
    (
        "tests/substrate/cli/test_argparse_acceptance.py",
        "uv run --extra dev pytest tests/substrate/cli/test_argparse_acceptance.py -q",
        "argparse acceptance tests changed",
    ),
)


def _deployment_focused_tests(
    deployment: str,
) -> tuple[tuple[str, str, str], ...]:
    """Load per-deployment focused-test pairs from
    `deployments/<deployment>/agent_context_focus.py` if present.

    The deployment file SHALL expose a `FOCUSED_TESTS_BY_PATH`
    module attribute matching the substrate's shape: a tuple of
    `(path_prefix, command, reason)` triples. Returns an empty
    tuple when the file or the attribute is absent.
    """
    if not deployment:
        return ()
    # Walk up from cwd for a `deployments/<name>/` directory; this
    # mirrors how the substrate's deployment resolver locates a
    # named deployment without importing it as a Python package.
    here = Path.cwd()
    candidates: list[Path] = []
    for parent in [here, *here.parents]:
        path = parent / "deployments" / deployment / "agent_context_focus.py"
        if path.is_file():
            candidates.append(path)
            break
    if not candidates:
        return ()
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        f"_okg_agent_focus_{deployment.replace('-', '_')}",
        candidates[0],
    )
    if spec is None or spec.loader is None:
        return ()
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:  # noqa: BLE001 — focused-tests module is operator-authored; bad import returns empty tuple, agent_context falls back to broad scan.
        return ()
    raw = getattr(module, "FOCUSED_TESTS_BY_PATH", ())
    if not isinstance(raw, tuple):
        return ()
    return tuple(
        (p, c, r) for (p, c, r) in raw
        if isinstance(p, str) and isinstance(c, str) and isinstance(r, str)
    )


def _substrate_error(
    message: str,
    *,
    code: str,
    fix_hint: str | None = None,
    data: dict[str, Any] | None = None,
) -> SubstrateError:
    err = SubstrateError(message, fix_hint=fix_hint, data=data or {})
    err.code = code
    return err


def _row_to_node(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "node_id": row["node_id"],
        "subtype": row["subtype"],
        "attrs": row.get("attrs") or {},
    }


def _node_summary(node: dict[str, Any] | None) -> dict[str, Any] | None:
    if node is None:
        return None
    attrs = node.get("attrs") or {}
    return {
        "node_id": node["node_id"],
        "subtype": node["subtype"],
        "label": attrs.get("label") or attrs.get("title") or attrs.get("path"),
        "status": attrs.get("status"),
        "path": attrs.get("path"),
        "task_number": attrs.get("task_number"),
    }


def _live_node_by_id(
    graph: Any,
    *,
    node_id: str,
) -> dict[str, Any] | None:
    rows = graph.query(
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = :node_id
         LIMIT 1
        """,
        {"node_id": node_id},
    )
    return _row_to_node(rows[0] if rows else None)


def _latest_published_generation(
    cur: psycopg.Cursor[Any],
    *,
    deployment: str,
    requested_generation: int | None,
) -> dict[str, Any]:
    if requested_generation is None:
        cur.execute(
            """
            SELECT generation_id, deployment_name, status,
                   catalog_version_id, published_at
              FROM okg.graph_generations
             WHERE deployment_name = %s
               AND status = 'published'
             ORDER BY generation_id DESC
             LIMIT 1
            """,
            (deployment,),
        )
    else:
        cur.execute(
            """
            SELECT generation_id, deployment_name, status,
                   catalog_version_id, published_at
              FROM okg.graph_generations
             WHERE deployment_name = %s
               AND generation_id = %s
             LIMIT 1
            """,
            (deployment, requested_generation),
        )
    row = cur.fetchone()
    if row is None:
        raise _substrate_error(
            "no published generation found for agent context",
            code="agent_context_generation_not_found",
            fix_hint=(
                "publish the deployment first or pass --generation with "
                "an existing published generation"
            ),
            data={
                "deployment": deployment,
                "generation_id": requested_generation,
            },
        )
    generation_id, deployment_name, status, catalog_version_id, published_at = row
    if status != "published":
        raise _substrate_error(
            "agent context requires a published generation",
            code="agent_context_generation_not_published",
            fix_hint="run okg generation describe <id> and publish a healthy generation",
            data={"deployment": deployment, "generation_id": generation_id},
        )
    return {
        "generation_id": generation_id,
        "deployment": deployment_name,
        "catalog_version_id": catalog_version_id,
        "published_status": status,
        "published_at": published_at.isoformat() if published_at else None,
    }


def _task_node_id(change: str, task: str | None) -> str | None:
    if task is None:
        return None
    if task.startswith("task:"):
        return task
    return f"task:{change}:{task}"


def _first_pending_task(
    graph: Any,
    *,
    change: str,
) -> dict[str, Any] | None:
    rows = graph.query(
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'proposal_task'
           AND attrs->>'change_id' = :change
         ORDER BY
           COALESCE((attrs->>'group_index')::int, 9999),
           attrs->>'task_number',
           node_id
         LIMIT 1
        """,
        {"change": change},
    )
    return _row_to_node(rows[0] if rows else None)


def _related_sessions(
    graph: Any,
    *,
    change_node_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    rows = graph.query(
        """
        WITH related AS (
          SELECT CASE WHEN e.src = :focal THEN e.dst ELSE e.src END AS node_id
            FROM okg.graph_edges e
           WHERE e.edge_type = 'references'
             AND (e.src = :focal OR e.dst = :focal)
        )
        SELECT DISTINCT n.node_id, n.subtype, n.attrs
          FROM related r
          JOIN okg.graph_nodes n ON n.node_id = r.node_id
         WHERE n.subtype IN ('agent_session', 'sub_agent_session')
         ORDER BY n.node_id DESC
         LIMIT :limit
        """,
        {"focal": change_node_id, "limit": limit},
    )
    return [
        {
            "node_id": row["node_id"],
            "subtype": row["subtype"],
            "cwd": (row["attrs"] or {}).get("cwd"),
            "git_branch": (row["attrs"] or {}).get("git_branch"),
            "tool_call_count": (row["attrs"] or {}).get("tool_call_count"),
        }
        for row in rows
    ]


def _source_file_for_path(
    graph: Any,
    *,
    path: str,
) -> dict[str, Any] | None:
    normalized = path
    if Path(path).is_absolute():
        normalized = Path(path).name
    candidates = [path, normalized]
    rows = graph.query(
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'source_file'
           AND (
             attrs->>'path' = ANY(:candidates)
             OR node_id = ANY(:candidates)
             OR node_id LIKE :like
           )
         ORDER BY node_id
         LIMIT 1
        """,
        {"candidates": candidates, "like": f"%:{normalized}"},
    )
    return _row_to_node(rows[0] if rows else None)


def _file_path(node: dict[str, Any] | None, fallback: str | None = None) -> str | None:
    if node is None:
        return fallback
    attrs = node.get("attrs") or {}
    return attrs.get("path") or fallback


def _ranked_file_entry(
    *,
    node_id: str | None,
    path: str | None,
    source: str,
    confidence: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "path": path,
        "source": source,
        "confidence": confidence,
        "reason": reason,
    }


def _rank_files(
    *,
    explicit_file: dict[str, Any] | None,
    explicit_path: str | None,
    session_files: list[dict[str, Any]],
    limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    primary: list[dict[str, Any]] = []
    secondary: list[dict[str, Any]] = []
    seen: set[str] = set()

    if explicit_path:
        node_id = explicit_file["node_id"] if explicit_file else None
        path = _file_path(explicit_file, explicit_path)
        key = node_id or f"path:{path}"
        primary.append(_ranked_file_entry(
            node_id=node_id,
            path=path,
            source="explicit_file",
            confidence="direct" if explicit_file else "live_input",
            reason=(
                "file was explicitly requested and matched graph evidence"
                if explicit_file
                else "file was explicitly requested but not found in the pinned graph"
            ),
        ))
        seen.add(key)

    for item in session_files:
        node_id = item.get("node_id")
        path = item.get("path")
        key = node_id or f"path:{path}"
        if key in seen:
            continue
        secondary.append(_ranked_file_entry(
            node_id=node_id,
            path=path,
            source=item.get("source") or "related_session",
            confidence="historical",
            reason="file was affected by a related prior agent session",
        ))
        seen.add(key)

    primary = primary[:limit]
    remaining = max(limit - len(primary), 0)
    secondary = secondary[:remaining]
    related = [*primary, *secondary][:limit]
    return primary, secondary, related


def _files_from_sessions(
    graph: Any,
    *,
    session_ids: list[str],
    explicit_file: dict[str, Any] | None,
    limit: int,
) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    if explicit_file is not None:
        files.append({
            "node_id": explicit_file["node_id"],
            "path": (explicit_file.get("attrs") or {}).get("path"),
            "source": "explicit_file",
        })
        seen.add(explicit_file["node_id"])
    if not session_ids:
        return files
    rows = graph.query(
        """
        SELECT DISTINCT f.node_id, f.attrs
          FROM okg.graph_edges e
          JOIN okg.graph_nodes f ON f.node_id = e.dst
         WHERE e.src = ANY(:session_ids)
           AND e.edge_type = 'affects'
           AND f.subtype = 'source_file'
         ORDER BY f.node_id
         LIMIT :limit
        """,
        {"session_ids": session_ids, "limit": limit},
    )
    for row in rows:
        node_id = row["node_id"]
        if node_id in seen:
            continue
        files.append({
            "node_id": node_id,
            "path": (row["attrs"] or {}).get("path"),
            "source": "related_session",
        })
        seen.add(node_id)
    return files


def _symbol_line_start(node_id: str, attrs: dict[str, Any]) -> int | None:
    raw = attrs.get("line_start") or attrs.get("start_line") or attrs.get("line")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    suffix = node_id.rsplit(":", 1)[-1]
    if suffix.isdigit():
        return int(suffix)
    return None


def _symbols_for_files(
    graph: Any,
    *,
    file_ids: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    if not file_ids:
        return []
    rows = graph.query(
        """
        SELECT DISTINCT s.node_id, s.attrs
          FROM okg.graph_edges e
          JOIN okg.graph_nodes s ON s.node_id = e.dst
         WHERE e.src = ANY(:file_ids)
           AND e.edge_type = 'contains'
           AND s.subtype = 'code_symbol'
         ORDER BY s.node_id
         LIMIT :limit
        """,
        {"file_ids": file_ids, "limit": limit},
    )
    return [
        {
            "node_id": row["node_id"],
            "name": (row["attrs"] or {}).get("name"),
            "path": (row["attrs"] or {}).get("path"),
            "line_start": _symbol_line_start(row["node_id"], row["attrs"] or {}),
        }
        for row in rows
    ]


def _suggested_verification(
    *,
    change: str,
    files: list[dict[str, Any]],
    symbols: list[dict[str, Any]],
    deployment: str = "",
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = [
        {
            "kind": "openspec",
            "command": (
                "uv run --extra dev openspec validate "
                f"{change} --strict --no-interactive"
            ),
            "graph_supported": True,
            "reason": "active OpenSpec change context",
        },
        {
            "kind": "local",
            "command": "git diff --check",
            "graph_supported": False,
            "reason": "live workspace whitespace check",
        },
    ]
    paths = [str(f.get("path") or "") for f in files if f.get("path")]
    paths.extend(str(s.get("path") or "") for s in symbols if s.get("path"))
    # Substrate-owned mapping + per-deployment overrides (when the
    # deployment ships a `deployments/<name>/agent_context_focus.py`).
    focus_map = _FOCUSED_TESTS_BY_PATH + _deployment_focused_tests(deployment)
    seen_commands: set[str] = set()
    for path in paths:
        for prefix, command, reason in focus_map:
            if path == prefix or path.startswith(prefix):
                if command in seen_commands:
                    continue
                suggestions.append({
                    "kind": "tests",
                    "command": command,
                    "graph_supported": True,
                    "reason": reason,
                })
                seen_commands.add(command)
    if not seen_commands:
        suggestions.append({
            "kind": "coverage_gap",
            "command": None,
            "graph_supported": False,
            "reason": (
                "no deterministic focused test mapping was available for "
                "the ranked files"
            ),
        })
    return suggestions


def _worktree_context(
    graph: Any,
    *,
    cwd: str | None,
) -> dict[str, Any] | None:
    if cwd is None:
        return None
    rows = graph.query(
        """
        SELECT count(*)::int AS n
          FROM okg.graph_nodes
         WHERE subtype = 'git_worktree'
        """,
        {},
    )
    worktree_count = int(rows[0]["n"])
    if worktree_count == 0:
        return {
            "cwd": cwd,
            "graph_resolved": False,
            "status": "unavailable",
            "reason": "pinned generation has no git_worktree nodes",
            "recommended_live_checks": [
                "git -C <cwd> status --short",
                "git -C <cwd> branch --show-current",
                "git -C <cwd> rev-parse HEAD",
            ],
        }
    return {
        "cwd": cwd,
        "graph_resolved": False,
        "status": "unresolved",
        "reason": (
            "git_worktree nodes exist, but this packet version has not "
            "resolved cwd to a worktree edge"
        ),
        "git_worktree_count": worktree_count,
        "recommended_live_checks": ["git -C <cwd> status --short"],
    }


def build_agent_context_from_connection(
    conn: psycopg.Connection[Any],
    *,
    deployment: str,
    change: str,
    task: str | None = None,
    file: str | None = None,
    cwd: str | None = None,
    generation_id: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Build a generation-pinned context packet using an existing conn."""

    with conn.cursor() as cur:
        generation = _latest_published_generation(
            cur,
            deployment=deployment,
            requested_generation=generation_id,
        )
        gid = int(generation["generation_id"])
        # Chronos cutover: graph-content reads resolve through a Chronos checkout
        # pinned to the published generation (okg.graph_nodes/graph_edges); the
        # graph_generations metadata read above stays on the raw cursor.
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(gid)
        change_node = _live_node_by_id(
            graph,
            node_id=f"change:{change}",
        )
        if change_node is None:
            raise _substrate_error(
                "OpenSpec change not found in pinned graph",
                code="agent_context_change_not_found",
                fix_hint=(
                    f"ingest/publish deployment {deployment!r} "
                    "or check --change"
                ),
                data={"change": change, "generation_id": gid},
            )
        task_node_id = _task_node_id(change, task)
        task_node = (
            _live_node_by_id(graph, node_id=task_node_id)
            if task_node_id
            else _first_pending_task(graph, change=change)
        )
        explicit_file = (
            _source_file_for_path(graph, path=file)
            if file
            else None
        )
        sessions = _related_sessions(
            graph,
            change_node_id=change_node["node_id"],
            limit=limit,
        )
        files = _files_from_sessions(
            graph,
            session_ids=[s["node_id"] for s in sessions],
            explicit_file=explicit_file,
            limit=limit,
        )
        primary_files, secondary_files, related_files = _rank_files(
            explicit_file=explicit_file,
            explicit_path=file,
            session_files=files,
            limit=limit,
        )
        symbols = _symbols_for_files(
            graph,
            file_ids=[
                f["node_id"]
                for f in related_files
                if f.get("node_id")
            ],
            limit=limit,
        )
        worktree = _worktree_context(graph, cwd=cwd)

    freshness_warnings = [
        "Graph evidence is pinned to the returned generation; verify live files before editing.",
    ]
    if worktree and not worktree.get("graph_resolved"):
        freshness_warnings.append(
            "Worktree context is not graph-resolved at this generation; use live git checks."
        )
    if not related_files:
        freshness_warnings.append(
            "No related source files were found from graph evidence; use rg and file reads."
        )
    if not sessions:
        freshness_warnings.append(
            "No related agent sessions were found for this change at the pinned generation."
        )

    suggested_verification = _suggested_verification(
        change=change,
        files=related_files,
        symbols=symbols,
        deployment=deployment,
    )

    live_verification_required = [
        "read the active OpenSpec tasks.md before editing",
        "use rg and file reads to verify current code shape",
        "run focused tests for executable changes",
    ]
    if cwd:
        live_verification_required.append(
            "use live git status/branch/head checks for current worktree state",
        )

    recommended_routes, manifest_checksum = _recommended_routes_payload(
        deployment=deployment,
        change=change,
        file=file,
        cwd=cwd,
    )

    return {
        **generation,
        "change": _node_summary(change_node),
        "task": _node_summary(task_node),
        "primary_files": primary_files,
        "secondary_files": secondary_files,
        "related_files": related_files,
        "related_symbols": symbols,
        "related_sessions": sessions,
        "prior_attempts": sessions,
        "suggested_verification": suggested_verification,
        "freshness_warnings": freshness_warnings,
        "live_verification_required": live_verification_required,
        "worktree_context": worktree,
        "recommended_routes": recommended_routes,
        "route_manifest_checksum": manifest_checksum,
        "graph_evidence": {
            "change_node_id": change_node["node_id"],
            "task_node_id": task_node["node_id"] if task_node else None,
            "related_session_count": len(sessions),
            "primary_file_count": len(primary_files),
            "secondary_file_count": len(secondary_files),
            "related_file_count": len(related_files),
            "related_symbol_count": len(symbols),
            "recommended_route_count": len(recommended_routes),
        },
    }


def _recommended_routes_payload(
    *,
    deployment: str,
    change: str | None,
    file: str | None,
    cwd: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Resolve the deployment's routes.yaml and run the recommender.

    Best-effort: a missing or malformed routes.yaml returns ([], None)
    so the agent_context call still succeeds. Errors are intentionally
    swallowed here (the deployment doctor surface is the right place
    to surface them); the return type lets callers detect "no routes
    were considered" via the empty list + None checksum.
    """
    try:
        from okg.substrate.deployments import (
            DeploymentResolutionError,
            resolve_deployment,
        )
        from okg.substrate.routes import (
            RouteManifestError,
            RouteRecommendationContext,
            load_manifest_for_deployment,
            recommend_routes,
        )
    except Exception:  # noqa: BLE001 — routes module may not be available in minimal substrates; route recommendations are informational.
        return [], None
    try:
        manifest_obj = resolve_deployment(deployment)
    except DeploymentResolutionError:
        return [], None
    try:
        manifest = load_manifest_for_deployment(manifest_obj.deployment_dir)
    except RouteManifestError:
        return [], None
    if manifest is None:
        return [], None
    file_extension = None
    if file:
        from pathlib import PurePosixPath

        suffix = PurePosixPath(file).suffix
        if suffix:
            file_extension = suffix
    context = RouteRecommendationContext(
        deployment=deployment,
        change=change,
        file=file,
        file_extension=file_extension,
        cwd=cwd,
    )
    matches = recommend_routes(manifest, context)
    return [r.to_json() for r in matches], manifest.checksum


def build_agent_context(
    *,
    dsn: str,
    deployment: str,
    change: str,
    task: str | None = None,
    file: str | None = None,
    cwd: str | None = None,
    generation_id: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Build a generation-pinned context packet for coding agents."""

    with db_connect(dsn) as conn:
        return build_agent_context_from_connection(
            conn,
            deployment=deployment,
            change=change,
            task=task,
            file=file,
            cwd=cwd,
            generation_id=generation_id,
            limit=limit,
        )
