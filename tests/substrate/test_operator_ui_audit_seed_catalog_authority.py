"""Real catalog-authority regression for the operator audit seed."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
from psycopg import sql

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.ownership import (
    WRITER_SCOPE_DEPLOYMENT_ROOT,
    WRITER_SCOPE_ENVIRONMENT,
    catalog_identity_for,
    ensure_catalog_write_allowed,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore


pytestmark = pytest.mark.postgres


REPO_ROOT = Path(__file__).resolve().parents[2]
SEED_SCRIPT = (
    REPO_ROOT / "src" / "okg" / "operator_ui" / "web" / "audit" / "seed_fixture.py"
)
FIXTURE_DEPLOYMENT = (
    SEED_SCRIPT.parent / "fixture-deployment" / "audit-fixture"
).resolve()
SEALED_RUNTIME_ENV = {
    "OKG_ACTIVE_RELEASE_POINTER",
    "OKG_DATABASE_IDENTITY",
    "OKG_DEFINITION_DIGEST",
    "OKG_DEPLOYMENT",
    "OKG_DSN",
    "OKG_ENVIRONMENT_DESCRIPTOR",
    "OKG_ENVIRONMENT_DESCRIPTOR_DIGEST",
    "OKG_ENVIRONMENT_ID",
    "OKG_ENVIRONMENT_TIER",
    "OKG_RELEASE_ARTIFACT_ID",
    "OKG_RELEASE_INPUT_ID",
    "OKG_RELEASE_ROOT",
}


def _load_seed_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "audit_seed_catalog_authority", SEED_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _swap_dbname(dsn: str, dbname: str) -> str:
    parts = urlsplit(dsn)
    return urlunsplit(
        (parts.scheme, parts.netloc, f"/{dbname}", parts.query, parts.fragment)
    )


def _drop_isolated_audit_seed_database(
    admin_dsn: str,
    database: str,
    *,
    after_drain: Callable[[], None] | None = None,
) -> None:
    """Fence, drain, and drop this test's disposable database."""
    if not database.startswith("okg_test_audit_seed_"):
        raise RuntimeError(f"refusing to drop unexpected database {database!r}")
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (database,)
        ).fetchone()
        if exists is None:
            return
        conn.execute(
            sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                sql.Identifier(database)
            )
        )
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (database,),
        )
        if after_drain is not None:
            after_drain()
        conn.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
        )


@pytest.fixture
def isolated_audit_seed_dsn(okg_dsn: str) -> Iterator[str]:
    """Create one disposable database for the real seed/publish cycle."""
    database = f"okg_test_audit_seed_{os.getpid()}"
    admin_dsn = _swap_dbname(okg_dsn, "postgres")
    test_dsn = _swap_dbname(okg_dsn, database)
    _drop_isolated_audit_seed_database(admin_dsn, database)
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        yield test_dsn
    finally:
        _drop_isolated_audit_seed_database(admin_dsn, database)


def test_isolated_audit_seed_drop_fences_and_drains_its_database(
    okg_dsn: str,
) -> None:
    database = f"okg_test_audit_seed_{os.getpid()}_drop"
    admin_dsn = _swap_dbname(okg_dsn, "postgres")
    test_dsn = _swap_dbname(okg_dsn, database)
    _drop_isolated_audit_seed_database(admin_dsn, database)
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))

    remaining_session = psycopg.connect(test_dsn, autocommit=True)
    racing_session: psycopg.Connection | None = None

    def attempt_connection_after_drain() -> None:
        nonlocal racing_session
        try:
            racing_session = psycopg.connect(test_dsn, autocommit=True)
        except psycopg.OperationalError as exc:
            assert "not currently accepting connections" in str(exc)
            return
        raise AssertionError("database accepted a new connection after its drain")

    try:
        _drop_isolated_audit_seed_database(
            admin_dsn,
            database,
            after_drain=attempt_connection_after_drain,
        )
        with pytest.raises(psycopg.Error):
            remaining_session.execute("SELECT 1")
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            assert (
                conn.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s", (database,)
                ).fetchone()
                is None
            )
    finally:
        if racing_session is not None:
            racing_session.close()
        remaining_session.close()
        _drop_isolated_audit_seed_database(admin_dsn, database)


def _configured_seed_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    for name in SEALED_RUNTIME_ENV:
        monkeypatch.delenv(name, raising=False)
    module = _load_seed_module()
    monkeypatch.setattr(
        module,
        "validate_destructive_fixture_authority",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(module, "_reset_db", lambda _dsn: None)
    monkeypatch.setattr(module, "_dsn_identity", lambda _dsn: "isolated-test-dsn")
    return module


def _generation_ids(dsn: str) -> list[str]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return [
            str(generation.checkpoint_id)
            for generation in store.list_generations(deployment_name="audit-fixture")
        ]
    finally:
        store.close()


def _prime_catalog_before_seed(
    module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    *,
    authority_dir: Path,
    writer_scope: str,
    environment_id: str | None = None,
) -> None:
    migrate = module._migrate

    def migrate_and_prime(dsn: str) -> None:
        migrate(dsn)
        composed = compose_catalog(authority_dir)
        identity = catalog_identity_for(
            composed=composed,
            deployment_dir=authority_dir,
            command="prime mismatched audit authority",
            writer_scope=writer_scope,
            environment_id=environment_id,
        )
        with psycopg.connect(dsn, autocommit=True) as conn:
            module._catalog_version_id(conn)
            ensure_catalog_write_allowed(conn, identity=identity)
            apply_composed_catalog(conn, composed, identity=identity)

    monkeypatch.setattr(module, "_migrate", migrate_and_prime)


def test_audit_seed_publishes_with_canonical_fixture_authority(
    isolated_audit_seed_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _configured_seed_module(monkeypatch)
    factory = module.build_deployment_publisher
    factory_calls: list[dict[str, object]] = []

    def tracked_factory(**kwargs: object) -> object:
        factory_calls.append(kwargs)
        return factory(**kwargs)

    def direct_publisher_is_forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("audit seed bypassed the canonical publisher factory")

    monkeypatch.setattr(module, "build_deployment_publisher", tracked_factory)
    monkeypatch.setattr(module, "Publisher", direct_publisher_is_forbidden)

    receipt_path = tmp_path / "audit-publish-receipt.json"
    result = module.publish_fixture_receipt(
        dsn=isolated_audit_seed_dsn,
        deployment="audit-fixture",
        receipt_path=receipt_path,
        exact_head="a" * 40,
        reset=True,
        reset_authority="isolated-test-database",
    )

    assert factory_calls and len(factory_calls) == 1
    assert factory_calls[0]["deployment_dir"] == FIXTURE_DEPLOYMENT
    assert factory_calls[0]["deployment_name"] == "audit-fixture"
    assert factory_calls[0]["command"] == "operator-ui audit fixture publish"
    assert set(factory_calls[0]["publisher_overrides"]) == {
        "consistency_gates",
        "materializers",
    }
    assert result["status"] == "published"
    assert result["generation_id"].startswith("okg:audit-fixture:branch:default:gen:")
    assert result["nodes_live"] == 243
    assert result["edges_live"] == 294
    assert result["doctor"] == "clean"
    assert result["queue_depth"] == 0
    assert result["dlq_count"] == 0
    assert json.loads(receipt_path.read_text(encoding="utf-8")) == result
    assert _generation_ids(isolated_audit_seed_dsn) == [result["generation_id"]]
    with psycopg.connect(isolated_audit_seed_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT cv.writer_fingerprint,
                   cv.writer_provenance->>'writer_scope',
                   cv.writer_provenance->>'environment_id',
                   own.owner_fingerprint
              FROM okg.catalog_versions AS cv
              JOIN okg.deployment_write_ownership AS own
                ON own.deployment_name = %s
             WHERE cv.status = 'applied'
               AND cv.writer_fingerprint IS NOT NULL
             ORDER BY cv.catalog_version_id DESC
             LIMIT 1
            """,
            ("audit-fixture",),
        )
        authority = cur.fetchone()
    assert authority is not None
    assert authority[0] == authority[3]
    assert authority[1] == WRITER_SCOPE_DEPLOYMENT_ROOT
    assert authority[2] is None


def test_audit_seed_refuses_missing_authority_without_ambient_fallback(
    isolated_audit_seed_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _configured_seed_module(monkeypatch)
    monkeypatch.setattr(module, "_fixture_deployment_dir", lambda _deployment: None)
    receipt_path = tmp_path / "missing-authority.json"

    with pytest.raises(SubstrateError) as exc_info:
        module.publish_fixture_receipt(
            dsn=isolated_audit_seed_dsn,
            deployment="audit-fixture",
            receipt_path=receipt_path,
            exact_head="b" * 40,
            reset=True,
            reset_authority="isolated-test-database",
        )

    assert exc_info.value.code == "catalog_writer_scope_required"
    assert exc_info.value.data["reason_code"] == "catalog_writer_scope_required"
    assert not receipt_path.exists()
    assert _generation_ids(isolated_audit_seed_dsn) == []


def test_audit_seed_refuses_partial_ambient_environment_authority(
    isolated_audit_seed_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _configured_seed_module(monkeypatch)
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "ambient-is-not-server-authority")
    receipt_path = tmp_path / "partial-ambient-authority.json"

    with pytest.raises(SubstrateError) as exc_info:
        module.publish_fixture_receipt(
            dsn=isolated_audit_seed_dsn,
            deployment="audit-fixture",
            receipt_path=receipt_path,
            exact_head="d" * 40,
            reset=True,
            reset_authority="isolated-test-database",
        )

    assert exc_info.value.code == "sealed_release_invalid"
    assert not receipt_path.exists()
    assert _generation_ids(isolated_audit_seed_dsn) == []


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("stale-scope", "catalog_writer_scope_change"),
        ("foreign-writer", "catalog_ownership_foreign_writer"),
    ],
)
def test_audit_seed_refuses_mismatched_authority_before_receipt_or_generation(
    isolated_audit_seed_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    case: str,
    expected_code: str,
) -> None:
    module = _configured_seed_module(monkeypatch)
    if case == "stale-scope":
        authority_dir = FIXTURE_DEPLOYMENT
        writer_scope = WRITER_SCOPE_ENVIRONMENT
        environment_id = "stale-audit-environment"
    else:
        authority_dir = tmp_path / "foreign-root" / "audit-fixture"
        shutil.copytree(FIXTURE_DEPLOYMENT, authority_dir)
        writer_scope = WRITER_SCOPE_DEPLOYMENT_ROOT
        environment_id = None
    _prime_catalog_before_seed(
        module,
        monkeypatch,
        authority_dir=authority_dir,
        writer_scope=writer_scope,
        environment_id=environment_id,
    )
    receipt_path = tmp_path / f"{case}.json"

    with pytest.raises(SubstrateError) as exc_info:
        module.publish_fixture_receipt(
            dsn=isolated_audit_seed_dsn,
            deployment="audit-fixture",
            receipt_path=receipt_path,
            exact_head="c" * 40,
            reset=True,
            reset_authority="isolated-test-database",
        )

    assert exc_info.value.code == expected_code
    assert exc_info.value.data["reason_code"] == expected_code
    assert not receipt_path.exists()
    assert _generation_ids(isolated_audit_seed_dsn) == []
