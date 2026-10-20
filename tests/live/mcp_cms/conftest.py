"""Database fixtures for the hand-run `kind: mcp` proof.

`clean_runtime` and `okg_migrated` are the substrate suite's own, re-exported
so the proof gets the same freshly migrated database the CI arm gets.
`okg_dsn` is overridden: the substrate version SKIPS when no test database
is configured, and here the operator has already asked for the run
(`OKG_MCP_CMS_LIVE=1`), so a missing database is a failure.
"""
from __future__ import annotations

import os

import pytest

from tests.substrate.conftest import clean_runtime, okg_migrated
from tests.substrate.external_backed.test_live_relation_mcp_adapter import (
    masked_dsn,
)

__all__ = [
    "clean_runtime", "close_built_cms_servers", "cms_dsn", "okg_dsn",
    "okg_migrated", "pytest_configure",
]

#: Traceback styles that print a frame's arguments, and so a DSN passed to
#: any helper. Replaced by `short`; an explicit `line` or `no` is kept.
_ARGUMENT_PRINTING_STYLES = frozenset({"auto", "long"})


def pytest_configure(config) -> None:
    """Short tracebacks for this proof.

    A long traceback prints every frame's arguments, and the helpers this
    proof calls take the database DSN. `short` prints the failing lines
    only, in the terminal and in the JUnit XML alike. Set here rather than
    in an ini file because this folder is only collected on request, and
    the rest of the suite keeps its own style.
    """
    if getattr(config.option, "tbstyle", None) in _ARGUMENT_PRINTING_STYLES:
        config.option.tbstyle = "short"


@pytest.fixture
def cms_dsn(clean_runtime: str):
    """The fresh database, as a `Dsn` whose repr and str hide the password."""
    return masked_dsn(clean_runtime)


@pytest.fixture(autouse=True)
def close_built_cms_servers():
    """Close every server the proof built, so no pool or reservation leaks.

    The CI arm closes them with its own autouse fixture in the test module;
    that fixture does not run here, so this one does the same job.
    """
    from tests.substrate.external_backed.test_live_relation_mcp_adapter import (
        _BUILT,
    )

    yield
    while _BUILT:
        server = _BUILT.pop()
        for attr in ("_okg_pool", "_okg_write_pool"):
            pool = getattr(server, attr, None)
            if pool is not None:
                pool.close()
        release = getattr(server, "_okg_release_reservation", None)
        if callable(release):
            release()


@pytest.fixture(scope="session")
def okg_dsn() -> str:
    dsn = os.environ.get("OKG_TEST_DSN", "").strip()
    if not dsn:
        pytest.fail(
            "OKG_MCP_CMS_LIVE=1 was set but OKG_TEST_DSN is not. The proof "
            "drops and recreates the database it names, so point it at a "
            "disposable Postgres you own."
        )
    return dsn
