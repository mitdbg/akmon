"""Real DBOS singleton fixture (Phase 2 §6.2).

Relocates the Phase 1 ``dbos_test_runtime`` fixture from
``tests/conftest.py`` into the SDK package so deployment-author test
suites can import it directly:

    from okg.deployment.testing import dbos_test_runtime

The factory yields a generator that pytest binds via the
``dbos_test_runtime`` fixture re-exported from
``okg.deployment.testing.__init__``.

Per Decision 11 of `rebuild-substrate-on-dbos`, ``system_database_url``
and ``database_url`` MUST point to the same Postgres database. The
factory honors this by passing the same DSN as both URLs.

Reads the DSN from ``OKG_TEST_DSN``, falling back to the project-
default test DSN. Tests should be marked
``@pytest.mark.dbos_integration`` since the DBOS init cycle is slow;
CI runs them on a separate (nightly) job per Phase 0 F15.
"""
from __future__ import annotations

import os
from typing import Iterator

import pytest


__all__ = ["dbos_test_runtime_factory"]


def dbos_test_runtime_factory() -> Iterator:
    """Yield a live DBOS singleton against the test DSN.

    Use as a pytest fixture generator::

        @pytest.fixture
        def dbos_test_runtime():
            yield from dbos_test_runtime_factory()
    """
    pytest.importorskip("dbos")
    from dbos import DBOS, DBOSConfig

    test_dsn = os.environ.get(
        "OKG_TEST_DSN",
        "postgres://postgres:okg@localhost:5433/okg_test",
    )

    # Defensive destroy in case a previous fixture left state.
    try:
        DBOS.destroy()
    except Exception:  # noqa: BLE001
        pass

    cfg = DBOSConfig(
        name="okg-test-runtime",
        system_database_url=test_dsn,
        database_url=test_dsn,
    )
    DBOS(config=cfg)
    DBOS.reset_system_database()
    DBOS.launch()
    try:
        yield DBOS
    finally:
        DBOS.destroy()
