from __future__ import annotations

import pytest
import psycopg

from tests.substrate import conftest as substrate_conftest
from tests.pact import conftest as pact_conftest


def test_okg_test_dsn_explicit_env_wins(monkeypatch) -> None:
    monkeypatch.setenv(
        "OKG_TEST_DSN",
        "postgres://postgres:okg@localhost:5433/explicit",
    )
    monkeypatch.setenv(
        "OKG_TEST_DSN_TEMPLATE",
        "postgres://postgres:okg@localhost:5433/templated_{workerid}",
    )
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")

    assert substrate_conftest._resolve_okg_test_dsn().endswith("/explicit")
    assert pact_conftest._resolve_test_dsn().endswith("/explicit")


def test_okg_test_dsn_template_uses_xdist_worker(monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_DSN", raising=False)
    monkeypatch.setenv(
        "OKG_TEST_DSN_TEMPLATE",
        "postgres://postgres:okg@localhost:5433/okg_test_{workerid}",
    )
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw2")

    assert substrate_conftest._resolve_okg_test_dsn().endswith("/okg_test_gw2")
    assert pact_conftest._resolve_test_dsn().endswith("/okg_test_gw2")


def test_okg_test_dsn_template_uses_master_without_xdist(monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_DSN", raising=False)
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.setenv(
        "OKG_TEST_DSN_TEMPLATE",
        "postgres://postgres:okg@localhost:5433/okg_test_{workerid}",
    )

    assert substrate_conftest._resolve_okg_test_dsn().endswith("/okg_test_master")
    assert pact_conftest._resolve_test_dsn().endswith("/okg_test_master")


def test_okg_test_dsn_is_unresolved_when_no_env(monkeypatch) -> None:
    """No variable set means no DSN at all (issue #1913).

    Both resolvers used to answer with the shared `okg-pg` Compose service,
    which their callers then truncated, dropped and recreated.
    """
    monkeypatch.delenv("OKG_TEST_DSN", raising=False)
    monkeypatch.delenv("OKG_TEST_DSN_TEMPLATE", raising=False)
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)

    assert substrate_conftest._resolve_okg_test_dsn() is None
    assert pact_conftest._resolve_test_dsn() is None


def test_safe_worker_test_dbname_passes_mutation_guard(monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_DSN_TRUST_AS_EPHEMERAL", raising=False)

    substrate_conftest._assert_test_db_target_is_safe_for_mutation(
        "postgres://postgres:okg@localhost:5433/okg_test_gw2",
        operation="clean test runtime",
    )


def test_non_test_dbname_aborts_before_mutation(monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_DSN_TRUST_AS_EPHEMERAL", raising=False)

    with pytest.raises(pytest.exit.Exception) as excinfo:
        substrate_conftest._assert_test_db_target_is_safe_for_mutation(
            "postgres://postgres:okg@localhost:5433/okg",
            operation="clean test runtime",
        )

    assert "OKG_TEST_DSN_TEMPLATE" in str(excinfo.value)
    assert "okg_test_{workerid}" in str(excinfo.value)


def test_trust_env_allows_nonstandard_ephemeral_dbname(monkeypatch) -> None:
    monkeypatch.setenv("OKG_TEST_DSN_TRUST_AS_EPHEMERAL", "1")

    substrate_conftest._assert_test_db_target_is_safe_for_mutation(
        "postgres://postgres:okg@localhost:5433/custom_ephemeral",
        operation="clean test runtime",
    )


def test_ensure_test_db_checks_safety_before_connect(monkeypatch) -> None:
    monkeypatch.delenv("OKG_TEST_DSN_TRUST_AS_EPHEMERAL", raising=False)

    def fail_connect(*args, **kwargs):
        raise AssertionError("database connection attempted before safety check")

    monkeypatch.setattr(substrate_conftest.psycopg, "connect", fail_connect)

    with pytest.raises(pytest.exit.Exception):
        substrate_conftest._ensure_test_db_exists(
            "postgres://postgres:okg@localhost:5433/okg"
        )


@pytest.mark.postgres
def test_scoped_cleanup_preserves_bootstrap_generation(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (generation_id, deployment_name, status,
                   catalog_version_id, published_at, correlation_id)
                VALUES
                  (880001, 'default', 'published', 1, now(),
                   'test:scoped-cleanup')
                ON CONFLICT (generation_id) DO NOTHING
                """
            )
            # A live-graph row: the reset must remove it along with the
            # published generation, and keep the migration's seed rows.
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES
                  ('fixture:scoped-cleanup', 'fixture', '{}'::jsonb,
                   'fixture', '{"id":"fixture:scoped-cleanup"}'::jsonb,
                   '{"v":1}'::jsonb, now(),
                   'fixture:scoped-cleanup:I', gen_random_uuid(), 'I')
                """
            )

    # The per-test reset is now a fresh clone of the migrated template
    # (issue #2271); the bootstrap generation comes back with it.
    substrate_conftest.recreate_okg_test_database(clean_runtime)

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM okg.graph_generations WHERE generation_id = 0
            )
            """
        )
        assert cur.fetchone()[0] is True
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM okg.graph_generations WHERE generation_id = 880001
            )
            """
        )
        assert cur.fetchone()[0] is False
        cur.execute("SELECT count(*) FROM okg.node_facts")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM okg.catalog_versions")
        assert cur.fetchone()[0] > 0


def _publish_one_document(dsn: str, node_id: str) -> dict:
    """Apply one document node and publish it under deployment ``default``."""
    from okg.substrate.db.pool import close_all
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
    from tests.substrate._db_helpers import seed_catalog_version

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(
            [
                NodeFact(
                    node_id=node_id, subtype="document", attrs={},
                    source_record_id={"id": node_id}, source_revision={"v": 1},
                )
            ],
            source="git",
            strict_endpoints=False,
        )
    close_all()
    try:
        return Publisher(
            dsn, "default",
            catalog_identity=bind_test_catalog_writer(dsn, "default"),
        ).cycle()
    finally:
        close_all()


@pytest.mark.postgres
def test_generation_reset_clears_published_chronos_checkpoints(
    clean_runtime: str,
) -> None:
    """A mid-test reset leaves no published generation behind, so the next
    publish on the same deployment succeeds.

    Issue #2238: the reset truncated ``generation_ordinals`` but kept the Chronos
    checkpoint tables, so the publisher found a published predecessor with no
    ordinal row and refused (the retired interval precondition,
    ``predecessor_ordinal_unavailable``). Without the fix the second publish
    below raises exactly that.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    assert _publish_one_document(clean_runtime, "doc:before-reset")["status"] == (
        "published"
    )

    # The reset is a fresh clone of the migrated template (issue #2271), so
    # the Chronos checkpoint tables go with everything else.
    substrate_conftest.reset_okg_test_runtime(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        leftover = store.latest_generation(
            deployment_name="default", branch="default", status="published",
        )
    assert leftover is None, (
        "a generation reset left a published Chronos checkpoint: "
        f"{getattr(leftover, 'checkpoint_id', leftover)!r}"
    )

    assert _publish_one_document(clean_runtime, "doc:after-reset")["status"] == (
        "published"
    )
