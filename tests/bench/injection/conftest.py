"""Shared fixtures for the injection benchmark suites.

The DB-backed witnesses reuse the substrate publisher harness by fixture
re-export, the same way ``tests/bench/synth/conftest.py`` does: pytest
registers fixtures found in a conftest module's namespace, so importing them
here makes ``clean_runtime`` resolvable under ``tests/bench/injection/``.
These are the identical fixtures the suites resolved from
``tests/substrate/conftest.py`` before the package moved out of the
substrate; only the path they are reached by has changed.
"""

from __future__ import annotations

from tests.substrate.conftest import (  # noqa: F401  (fixture re-export)
    clean_runtime,
    okg_dsn,
    okg_migrated,
)
