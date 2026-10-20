"""Shared fixtures for deployment-owned tests.

The deployment-owned postgres tests (for example a wiki gate, a Nomos seed
or a Poros message board) need the ``clean_runtime`` DB
fixture and its session-scoped ``okg_migrated`` / ``okg_dsn`` chain, which live
in ``tests/substrate/conftest.py``.

They previously pulled those in with
``pytest_plugins = ("tests.substrate.conftest",)``. That registers the
substrate conftest as a *named* plugin and raises
``ValueError: Plugin already registered under a different name`` whenever
``tests/deployments`` is collected in the same session as ``tests/substrate``
(pytest already auto-loads ``tests/substrate/conftest.py`` as a plugin). That
conflict blocked adding ``tests/deployments`` to the ``fast`` /
``substrate-db-full`` qa lanes.

Re-exporting the fixture *functions* here instead has no such conflict -- it is
a plain import, not a plugin registration -- and makes the chain visible to
deployment tests (auto-loaded sibling conftests are not). ``okg_dsn`` /
``okg_migrated`` are session-scoped, but ``okg_migrated`` is idempotent with a
non-destructive teardown, so the at-most-second setup under co-collection is
safe.
"""
from tests.substrate.conftest import (  # noqa: F401  (re-exported as fixtures)
    clean_runtime,
    okg_dsn,
    okg_migrated,
)

# Keep the fixture re-export contract explicit for static analyzers as well as
# pytest.  The names must remain in this module's namespace for fixture lookup.
__all__ = ["clean_runtime", "okg_dsn", "okg_migrated"]
