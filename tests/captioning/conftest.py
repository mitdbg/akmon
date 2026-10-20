"""Shared fixtures for captioning tests.

The visual-caption-enricher PACT adds real-DB tests (the ``okg.visual_crop_blobs``
crop store and the VisualCaptionEnricher) that need the ``clean_runtime`` fixture
and its session-scoped ``okg_migrated`` / ``okg_dsn`` chain from
``tests/substrate/conftest.py``.

Re-export the fixture *functions* here (a plain import, not a
``pytest_plugins`` registration) so they are visible under ``tests/captioning``
without the "Plugin already registered under a different name" conflict that
``pytest_plugins`` triggers when ``tests/substrate`` is co-collected. This
mirrors ``tests/deployments/conftest.py``.
"""
from tests.substrate import conftest as _substrate_fixtures


# Pytest discovers these assigned fixture functions in this directory. Using
# explicit assignments keeps the intentional re-export visible to static
# analysis, which otherwise reports the direct imports as unused.
clean_runtime = _substrate_fixtures.clean_runtime
okg_dsn = _substrate_fixtures.okg_dsn
okg_migrated = _substrate_fixtures.okg_migrated
