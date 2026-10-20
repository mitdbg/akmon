"""PACT test fixtures shared through pytest's normal discovery path."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from xml.sax.saxutils import escape

import pytest

from okg.substrate.pact import decisions as ledger
from okg.substrate.pact.operations import ingest_pytest_report
from okg.substrate.pact.store import load_manifest


def _resolve_test_dsn() -> str | None:
    """Resolve the same per-worker test database contract as CI.

    ``None`` when neither variable is set. There is deliberately no default
    (issue #1913): the old fallback was the shared `okg-pg` Compose service,
    and `_ensure_test_database` plus the `postgres_ledger` fixture CREATE and
    DROP against whatever this returns. On a maintainer workstation with that
    service up, a forgotten variable dropped the ledger tables of the database
    other sessions were using instead of skipping.
    """
    if explicit := os.environ.get("OKG_TEST_DSN"):
        return explicit
    if template := os.environ.get("OKG_TEST_DSN_TEMPLATE"):
        workerid = os.environ.get("PYTEST_XDIST_WORKER", "master")
        return template.format(workerid=workerid)
    return None


TEST_DSN = _resolve_test_dsn()

NO_TEST_DSN_REASON = (
    "no test database is configured: set OKG_TEST_DSN (or "
    "OKG_TEST_DSN_TEMPLATE with a {workerid} placeholder) to a database you "
    "own and may destroy"
)


def _postgres_is_declared() -> bool:
    """True when a CI lane (or an operator) DECLARED a database for this run.

    Either `OKG_TEST_DSN` env var is the lane's explicit contract. When one is
    present, an unreachable database is a LANE FAULT and must be loud: the lane
    asked for these tests to run against Postgres, so silently skipping them
    reports green for work that never happened. Without either variable this is
    a developer checkout with no database, where skipping is the honest answer.
    """
    return bool(
        os.environ.get("OKG_TEST_DSN") or os.environ.get("OKG_TEST_DSN_TEMPLATE")
    )


def _ensure_test_database(dsn: str) -> None:
    """Create the target database when it is missing.

    `tests/pact` had no provisioning of its own: the auto-create lives in
    `tests/substrate/conftest.py` and applies only under that directory. A DB
    lane pointing `OKG_TEST_DSN_TEMPLATE` at a per-worker database therefore
    found nothing, every Postgres class skipped, and the skip read as green.
    Refuses any target whose name does not mark it a test database, so this can
    never create or touch a real deployment.
    """
    import psycopg

    info = psycopg.conninfo.conninfo_to_dict(dsn)
    dbname = str(info.get("dbname") or "")
    if "test" not in dbname:
        raise RuntimeError(
            f"refusing to provision non-test database {dbname!r}; "
            "tests/pact only creates databases whose name marks them as tests"
        )
    if not dbname.replace("_", "").replace("-", "").isalnum():
        raise RuntimeError(f"refusing to provision unsafe database name {dbname!r}")
    try:
        with psycopg.connect(dsn, connect_timeout=5):
            return
    except psycopg.OperationalError as exc:
        if "does not exist" not in str(exc):
            raise
    maintenance = dict(info)
    maintenance["dbname"] = "postgres"
    with psycopg.connect(
        psycopg.conninfo.make_conninfo(**maintenance),
        connect_timeout=5,
        autocommit=True,
    ) as conn, conn.cursor() as cur:
        try:
            cur.execute(f'CREATE DATABASE "{dbname}"')
        except psycopg.errors.DuplicateDatabase:
            pass


@lru_cache(maxsize=1)
def _postgres_unavailable_reason() -> str | None:
    """Provision the database on first use; return why it is unusable, or None.

    Deliberately LAZY and never called at import: collection-only passes (the
    affected-test classifier walks the whole tree with no database) must not
    need Postgres to enumerate tests. The decision to skip or fail belongs at
    test setup, where it is a statement about THAT test rather than about
    whether the tree can be read.
    """
    if TEST_DSN is None:
        # Return BEFORE any connect: with nothing declared there is no
        # database to probe, and probing would mean guessing at one.
        return NO_TEST_DSN_REASON
    try:
        import psycopg

        _ensure_test_database(TEST_DSN)
        with psycopg.connect(TEST_DSN, connect_timeout=5):
            return None
    except Exception as exc:  # noqa: BLE001 - the reason is the return value
        return f"{TEST_DSN} is unusable: {exc}"


def pytest_runtest_setup(item) -> None:
    """Skip or FAIL a Postgres test, never silently pass it.

    The distinction this exists to draw (PR #1460): when a lane DECLARED a
    database and it is unusable, that is a lane fault and must be loud. A
    skipped Postgres class reports green for a test that never ran, which is
    exactly how 35 of these went unexecuted on every pull request while the
    affected-test gate believed `lane:fast/pytest` was running them.
    """
    if item.get_closest_marker("postgres") is None:
        return
    reason = _postgres_unavailable_reason()
    if reason is None:
        return
    if _postgres_is_declared():
        raise RuntimeError(
            "tests/pact requires Postgres because this run declares "
            f"OKG_TEST_DSN/OKG_TEST_DSN_TEMPLATE, but {reason}. Refusing to "
            "skip: a skipped Postgres class reports green for a test that "
            "never ran."
        )
    pytest.skip(f"test Postgres unreachable ({reason})")


def ingest_passing_named_plan(
    root: Path,
    change_id: str,
    *,
    stem: str = "passing-plan",
):
    """Ingest a real JUnit-shaped witness for every named pytest plan entry."""
    manifest = load_manifest(root, change_id)
    cases: list[str] = []
    for entry in manifest.test_plan:
        if ".py::" not in entry.test_id:
            continue
        path, _, selector = entry.test_id.partition("::")
        classname = path.replace("/", ".")
        if classname.endswith(".py"):
            classname = classname[:-3]
        name = selector.rsplit("::", 1)[-1]
        cases.append(
            f'<testcase classname="{escape(classname)}" name="{escape(name)}">'
            "<properties>"
            f'<property name="requirement" value="{escape(entry.requirement)}"/>'
            "</properties></testcase>"
        )
    assert cases, f"{change_id} declares no named pytest plan entry"
    report = root / f"{stem}.xml"
    report.write_text(
        f'<testsuite tests="{len(cases)}">{"".join(cases)}</testsuite>',
        encoding="utf-8",
    )
    ingest_pytest_report(root, change_id, report)
    return load_manifest(root, change_id)


@pytest.fixture
def clean_ledger(monkeypatch, tmp_path):
    """Provide an empty decision ledger for PACT tests that use Postgres."""
    import psycopg

    deployment_dir = tmp_path / "deployments" / "test"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        "postgres:\n  dsn: ${OKG_DSN}\n  database: okg_test\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "test")
    with psycopg.connect(TEST_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp'
                    ) THEN
                        CREATE ROLE okg_mcp NOSUPERUSER;
                    END IF;
                END
                $$;
                """
            )
            cur.execute("GRANT okg_mcp TO CURRENT_USER")
            # Drop BOTH namespaces. A developer's long-lived okg_test carries
            # pre-qualification `public.pact_*` tables, and the ledger guard
            # refuses outright while any of them exist — so leaving them would
            # hard-fail every test in this directory with no recovery path
            # short of dropping the database by hand. The bootstrap DDL
            # recreates the okg copies, so dropping is the reset, not a loss.
            for _bare, qualified in ledger.LEDGER_RELATIONS:
                cur.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE")
                cur.execute(f"DROP TABLE IF EXISTS public.{_bare} CASCADE")
        conn.commit()
    yield TEST_DSN
