"""OKG visualization HTTP surface.

A FastAPI sub-app, optionally mounted on the MCP server process via
:func:`mount_viz`.  Off by default; enabled when ``OKG_VIZ_PORT`` is
set in the environment.

The viz layer is read-only over the loaded graph and shares the live
``OKGTools`` instance with the MCP server — no second graph load.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from okg.viz.app import build_app, mount_viz

__all__ = ["build_app", "mount_viz"]


def __getattr__(name: str) -> object:
    if name in __all__:
        from okg.viz.app import build_app, mount_viz

        return {"build_app": build_app, "mount_viz": mount_viz}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
