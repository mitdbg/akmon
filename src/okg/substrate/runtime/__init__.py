"""OKG runtime layer — DBOS composition.

This package is the substrate-side bridge to DBOS (the durable workflow
runtime composed beneath OKG per the `rebuild-substrate-on-dbos` proposal).
It provides the configuration surface that the deployment manifest's
``runtime:`` block lands on, the bootstrap entry point that creates the
DBOS schema in a deployment's Postgres database, and (in subsequent
phases) the worker process entry point and the operational CLI commands.

This package is substrate-internal. The deployment-author SDK lives at
``okg.deployment`` and is the only public surface deployment code targets.

Phase 1 scope (current): configuration loader + bootstrap stub +
package skeleton. Subsequent phases add: smoke-test workflow,
worker-process CLI, provenance-split audit, dev-loop reset, recovery
measurement infrastructure.
"""
from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .config import RUNTIME_BLOCK_DEFAULTS, RuntimeConfig

if TYPE_CHECKING:
    from .bootstrap import (
        BootstrapResult,
        bootstrap_dbos_schema,
        is_bootstrapped,
    )

__all__ = [
    "BootstrapResult",
    "RUNTIME_BLOCK_DEFAULTS",
    "RuntimeConfig",
    "bootstrap_dbos_schema",
    "is_bootstrapped",
]

_LAZY_BOOTSTRAP_EXPORTS = frozenset({
    "BootstrapResult",
    "bootstrap_dbos_schema",
    "is_bootstrapped",
})


def __getattr__(name: str) -> Any:
    """Resolve DBOS bootstrap exports only when a caller requests them."""
    if name not in _LAZY_BOOTSTRAP_EXPORTS:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )
    bootstrap = import_module(f"{__name__}.bootstrap")
    value = getattr(bootstrap, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Expose lazy exports to introspection without importing DBOS bootstrap."""
    return sorted(set(globals()) | set(__all__))
