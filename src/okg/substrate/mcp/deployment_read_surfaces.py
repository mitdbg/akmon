"""MCP resource projection over the shared deployment read envelope."""

from __future__ import annotations

import json
from pathlib import Path
from collections.abc import Callable
from typing import Any

from okg.substrate.contracts.deployment_product.projections import (
    READ_STATUS_SCHEMA,
    ReadbackProjectionAuthority,
    load_deployment_read_status,
    load_readback_projection_authority,
)


def deployment_read_resource(
    state_file: Path,
    *,
    authority: ReadbackProjectionAuthority | None = None,
) -> dict[str, Any]:
    if authority is None:
        sidecar = state_file.with_suffix(".readback.json")
        if sidecar.is_file():
            authority = load_readback_projection_authority(sidecar)
    status = load_deployment_read_status(state_file, authority=authority)
    return {
        "uri": f"okg://deployment/{status.deployment_id}/operations/{status.operation_id}",
        "mime_type": "application/json",
        "schema_version": READ_STATUS_SCHEMA,
        "contents": status.model_dump(mode="json"),
    }


def deployment_state_root(deployment_dir: Path | None) -> Path | None:
    """The `.okg/up` directory of the Git repository holding the deployment.

    `okg deployment up` writes its lifecycle record to
    `.okg/up/<deployment>-<profile>.json` under the directory it runs from,
    which is the root of the repository that holds the deployment (okg's
    checkout for an in-tree deployment, the deployment's own repository
    otherwise; see `runtime_dirs`). The MCP server does not run from there:
    the agent launcher starts it from `/`. So the record is located from the
    deployment's directory, never from the server's working directory.

    The repository root is the nearest ancestor holding `.git` (a directory,
    or the file a linked worktree has). A deployment in no repository, such
    as one served from an installed package, has no record and returns None.
    """
    if deployment_dir is None:
        return None
    start = Path(deployment_dir).resolve()
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate / ".okg" / "up"
    return None


def register_deployment_read_resources(
    mcp: Any,
    *,
    deployment_name: str,
    state_root: Path | None,
    require_principal: Callable[[str], None],
) -> dict[str, Callable[[], str]]:
    """Register a dev/prod resource for each lifecycle record that exists.

    A profile is listed only when its record is on disk when the server
    starts: a listed resource that can only answer "no such file" is worse
    than none, because an agent reads resources first and gives up on the
    error. The profile is never guessed.
    """

    resources: dict[str, Callable[[], str]] = {}
    if state_root is None:
        return resources
    for profile in ("dev", "prod"):
        state_file = state_root / f"{deployment_name}-{profile}.json"
        if not state_file.is_file():
            continue

        def _register(
            selected_profile: str = profile,
            selected_state_file: Path = state_file,
        ) -> Callable[[], str]:
            @mcp.resource(
                f"okg://deployment/{deployment_name}/read-status/{selected_profile}",
                name=f"deployment-read-status-{deployment_name}-{selected_profile}",
                description="Typed read-only deployment operation status and provenance.",
                mime_type="application/json",
            )
            def resource() -> str:
                require_principal("deployment-read-status")
                return json.dumps(
                    deployment_read_resource(selected_state_file),
                    sort_keys=True,
                    separators=(",", ":"),
                )

            return resource

        resources[profile] = _register()
    return resources


__all__ = [
    "deployment_read_resource",
    "deployment_state_root",
    "register_deployment_read_resources",
]
