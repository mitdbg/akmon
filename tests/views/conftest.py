"""Pytest fixtures for the views test suite.

Copies the substrate's session-scoped DB fixtures verbatim
(``okg_dsn``, ``okg_migrated``, ``clean_runtime``) so the views
layer's Postgres-backed tests share the same DB-safety guards
and schema migration as substrate tests.

Also injects ``deployments/af_pubs`` onto ``sys.path`` so tests
that exercise that deployment's views can import ``from
views.supersedure_health import ...`` — matching the deployment
convention documented in history/openspec/project.md (deployment
modules import siblings via top-level package names like
``sources.X`` / ``views.X`` rather than via the absolute
``deployments.<name>.X`` path, since ``deployments/`` is not a
package).

If the substrate's fixtures change, sync this file. The
implementations are short and stable.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from tests.substrate.conftest import (
    _migrated_okg_test_template,
    recreate_okg_test_database,
    # Re-exported as a fixture: views' Postgres tests use the substrate's
    # session DSN fixture verbatim, so there is ONE resolver and one skip
    # (issue #1913 -- the local override returned None and every consumer
    # then failed inside psycopg with an AttributeError instead of skipping).
    okg_dsn,  # noqa: F401
)
from tests.pytest.optional_deployments import (
    inject_optional_deployment_path,
    mark_optional_deployment_items,
    should_ignore_optional_deployment_test,
)

# ---------------------------------------------------------------------------
# sys.path injection — runs at import time so that
# `from views.supersedure_health import ...` works in test
# modules even before fixtures are resolved.
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[2]
_TEST_ROOT = Path(__file__).resolve().parent
for _dep in ["af_pubs"]:
    inject_optional_deployment_path(_REPO_ROOT, _dep)

_AF_PUBS_DEPENDENT_TESTS = {
    "test_af_pubs_concept_views.py",
    "test_af_pubs_structural_observation_emitter.py",
    "test_invariants_health.py",
    "test_invariants_health_live.py",
    "test_recent_changes.py",
    "test_supersedure_health.py",
    "test_supersedure_health_counts.py",
}


def pytest_ignore_collect(
    collection_path: Path, config: pytest.Config
) -> bool | None:
    """Skip deployment-view tests when the af_pubs deployment is absent."""
    return should_ignore_optional_deployment_test(
        collection_path,
        repo_root=_REPO_ROOT,
        deployment="af_pubs",
        filenames=_AF_PUBS_DEPENDENT_TESTS,
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    mark_optional_deployment_items(
        items,
        deployment="af_pubs",
        test_root=_TEST_ROOT,
        filenames=_AF_PUBS_DEPENDENT_TESTS,
    )


@pytest.fixture(autouse=True)
def _views_deployment_path() -> None:
    inject_optional_deployment_path(_REPO_ROOT, "af_pubs")


def _install_af_pubs_views(dsn: str) -> None:
    """Install the af_pubs deployment-side views.sql into the migrated DB.

    Per `add-deployment-sql-views`: the af_pubs views (e.g.
    okg.v_af_pubs_supersedure_summary) are referenced by build_corpus in the
    supersedure_health view; tests against a corpus without these would fail
    with UndefinedTable. Installed before the database becomes the clone
    template, so every per-test clone carries them.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(_AF_PUBS_VIEWS_SQL.read_text())
        conn.commit()


#: The optional af_pubs deployment views. Absent from this checkout today.
_AF_PUBS_VIEWS_SQL = (
    Path(__file__).resolve().parents[2] / "deployments" / "af_pubs" / "views.sql"
)


def _views_after_migrate_hook():
    """The hook to install the af_pubs views, or None when there are none.

    A hook the substrate template was not built with makes the worker drop its
    database and migrate a second time. With no views file the hook would do
    nothing, so passing None lets the worker reuse the template it already
    has instead of paying for a migrate that changes nothing (issue #2460).
    """
    return _install_af_pubs_views if _AF_PUBS_VIEWS_SQL.exists() else None


@pytest.fixture(scope="session")
def okg_migrated(okg_dsn: str) -> Iterator[str]:  # noqa: F811 (fixture arg)
    """Session-scoped: a fresh migrated database, kept as the clone template.

    The substrate's ``okg_migrated`` path (issue #2271), plus the af_pubs
    deployment views. ``pg_cron`` is dropped from the migrate's required
    extensions for the session, so the test DSN can be a separate DB on a
    single-pg_cron-instance Postgres; the migrate still creates the same
    schema.
    """
    yield from _migrated_okg_test_template(
        okg_dsn,
        after_migrate=_views_after_migrate_hook(),
    )


@pytest.fixture
def clean_runtime(okg_migrated: str) -> Iterator[str]:
    """Per-test: a fresh clone of the migrated template database."""
    from okg.substrate.db.pool import close_all

    recreate_okg_test_database(okg_migrated)
    try:
        yield okg_migrated
    finally:
        close_all()


# ---------------------------------------------------------------------------
# Live-substrate read-only fixtures
# ---------------------------------------------------------------------------
#
# Stage 5's exact-counts test (test_supersedure_health_counts.py)
# and the live invariants-health checks read a LIVE af_pubs
# published generation — not the truncated test schema. These
# deployment-backed checks are opt-in so the default substrate
# suite cannot fail because of local production state.
#
# Critical: these fixtures NEVER write to the substrate. They
# open autocommit=True connections used only for SELECT.


@pytest.fixture(scope="session")
def live_okg_dsn() -> str:
    """The production OKG DSN (NOT the test DSN).

    Resolution order: ``OKG_DSN`` env var, then the local-dev
    compose default. Skips the test if no DSN is reachable —
    we do NOT fall back to the test DSN, since by definition
    the live af_pubs data lives in the production DB.
    """
    if os.environ.get("OKG_RUN_LIVE_DEPLOYMENT_TESTS") != "1":
        pytest.skip(
            "live deployment tests are opt-in; set "
            "OKG_RUN_LIVE_DEPLOYMENT_TESTS=1 to enable"
        )
    return (
        os.environ.get("OKG_DSN")
        or "postgres://postgres:okg@localhost:5433/okg"
    )


@pytest.fixture(scope="session")
def live_af_pubs_generation(live_okg_dsn: str) -> tuple[str, int]:
    """Resolve af_pubs's latest published generation in the live DB.

    Returns ``(dsn, generation_id)``. Skips the calling test
    (rather than failing) when:

    * the live DB is unreachable;
    * af_pubs has no published generation;
    * the substrate schema isn't installed.
    """
    try:
        with psycopg.connect(live_okg_dsn, connect_timeout=5) as conn, \
             conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('okg.graph_generations')"
            )
            if cur.fetchone()[0] is None:
                pytest.skip(
                    f"substrate schema not installed at "
                    f"{live_okg_dsn}; live af_pubs tests skipped"
                )
                raise AssertionError("pytest.skip did not raise")
            cur.execute(
                "SELECT generation_id "
                "  FROM okg.graph_generations "
                " WHERE deployment_name = 'af_pubs' "
                "   AND status = 'published' "
                " ORDER BY generation_id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                pytest.skip(
                    f"af_pubs has no published generation in "
                    f"{live_okg_dsn}; nothing to count"
                )
                raise AssertionError("pytest.skip did not raise")
            return (live_okg_dsn, int(row[0]))
    except psycopg.OperationalError as exc:
        pytest.skip(
            f"live DB at {live_okg_dsn} unreachable: {exc}"
        )
        raise AssertionError("pytest.skip did not raise")
