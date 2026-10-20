"""The file paths an `agent_surface:` block names, in one list.

Issue #2067 moved the MCP instructions (and, for chat, the system prompt and
description overrides) from top-level manifest keys into the block. Anything
that treats a manifest's path keys specially -- the definition digest, the
staging/production substance comparison, the Kubernetes lifecycle bundle that
rewrites paths into the installed image, and the installed-package asset
check -- must treat these nested fields the same way, or a moved file drops
out of it with nothing said. They read this list rather than repeating it.

Dependency-free on purpose: `distributions.package_assets` must stay light.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

#: Each entry is the key path from the top of a manifest.
AGENT_SURFACE_PATH_FIELDS: tuple[tuple[str, ...], ...] = (
    ("agent_surface", "instructions"),
    ("agent_surface", "projections", "chat", "system_prompt_ref"),
    ("agent_surface", "projections", "chat", "description_overrides"),
)


def agent_surface_path_values(
    raw: Mapping[str, Any],
) -> Iterator[tuple[tuple[str, ...], Any]]:
    """`(field path, value)` for each field present in `raw`, in list order.

    The value is returned as declared, not validated: each caller owns its
    own refusal for a value that is not a usable path.
    """
    for field_path in AGENT_SURFACE_PATH_FIELDS:
        value: Any = raw
        for part in field_path:
            value = value.get(part) if isinstance(value, Mapping) else None
        if value is not None:
            yield field_path, value


def set_agent_surface_path(
    raw: dict[str, Any], field_path: tuple[str, ...], value: Any,
) -> None:
    """Replace one declared field in place; the parents must already exist."""
    parent: Any = raw
    for part in field_path[:-1]:
        parent = parent[part]
    parent[field_path[-1]] = value


__all__ = [
    "AGENT_SURFACE_PATH_FIELDS",
    "agent_surface_path_values",
    "set_agent_surface_path",
]
