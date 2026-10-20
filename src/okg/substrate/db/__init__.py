"""Substrate DB layer: connection helpers, migrate, doctor.

The package exports are resolved lazily (PEP 562) so that importing a light
submodule such as :mod:`okg.substrate.db.dsn` does not import ``psycopg``
through :mod:`okg.substrate.db.connection` as a side effect. ``from
okg.substrate.db import connect`` and ``okg.substrate.db.connection`` keep
working; they load the connection module on first use.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from okg.substrate.db.connection import connect, dsn_from_env

__all__ = ["connect", "dsn_from_env"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from okg.substrate.db import connection

        return getattr(connection, name)
    if name == "connection":
        # Callers that did `import okg.substrate.db` and then used
        # `okg.substrate.db.connection` relied on the eager import above.
        return importlib.import_module(f"{__name__}.connection")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
