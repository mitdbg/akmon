"""Thin CLI projections over the shared typed deployment read model."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from okg.substrate.contracts.deployment_product.projections import (
    ReadbackProjectionAuthority,
    load_deployment_read_status,
    load_readback_projection_authority,
    render_human_read_status,
)


def deployment_read_agent_payload(
    state_file: Path,
    *,
    authority: ReadbackProjectionAuthority | None = None,
) -> dict[str, Any]:
    if authority is None:
        sidecar = state_file.with_suffix(".readback.json")
        if sidecar.is_file():
            authority = load_readback_projection_authority(sidecar)
    status = load_deployment_read_status(state_file, authority=authority)
    return status.model_dump(mode="json")


def deployment_read_human_text(
    state_file: Path,
    *,
    authority: ReadbackProjectionAuthority | None = None,
) -> str:
    if authority is None:
        sidecar = state_file.with_suffix(".readback.json")
        if sidecar.is_file():
            authority = load_readback_projection_authority(sidecar)
    status = load_deployment_read_status(state_file, authority=authority)
    return render_human_read_status(status)


__all__ = ["deployment_read_agent_payload", "deployment_read_human_text"]
