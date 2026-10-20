"""Test fixtures for deployment-author test suites.

Per `sdk-design.md` §2.6 and Phase 2 §6, the SDK ships two fixtures:

  * :func:`mock_dbos` — fast unit-test shim. Decorators become
    pass-throughs; functions run synchronously; SQL writes via SDK
    helpers go to a real Postgres (caller supplies the conn via
    :func:`bind_test_context`). Durability semantics are NOT in
    scope per Phase 0 C4 — tests that need durability use
    :func:`dbos_test_runtime`.

  * :func:`dbos_test_runtime` — full DBOS singleton against a test
    Postgres. Slow; mark integration tests with
    ``@pytest.mark.dbos_integration`` and run them on a separate CI
    job per Phase 0 F15.

Use::

    import pytest
    from okg.deployment.testing import dbos_test_runtime, mock_dbos
    # NB: pytest fixtures need to be re-bound to your test module:
    pytest_plugins = ["okg.deployment.testing"]

Or import the underlying callable directly:

    from okg.deployment.testing import mock_dbos_context

    def test_my_workflow():
        with mock_dbos_context() as mdb:
            ...
"""
from __future__ import annotations

import pytest

from ._mock_dbos import MockDBOS, mock_dbos_context
from ._real_dbos import dbos_test_runtime_factory


__all__ = [
    "MockDBOS",
    "dbos_test_runtime",
    "mock_dbos",
    "mock_dbos_context",
]


@pytest.fixture
def mock_dbos() -> "MockDBOS":
    """Pytest fixture: yields a :class:`MockDBOS` for the duration of
    one test. Decorators become pass-through; functions run sync.

    SQL writes from SDK helpers still need a real connection bound via
    :func:`okg.deployment._internal.context.bind_test_context`.
    """
    with mock_dbos_context() as mdb:
        yield mdb


@pytest.fixture
def dbos_test_runtime():
    """Pytest fixture: real DBOS singleton on a per-test Postgres DB.

    Slow; mark tests with ``@pytest.mark.dbos_integration``.
    """
    yield from dbos_test_runtime_factory()
