"""Real PostgreSQL proof for the scoped generation-read lease extension point."""

from __future__ import annotations

from dataclasses import dataclass, field
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import sql

from okg.substrate.db import migrate as migration
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.history_read_guard import (
    GenerationReadLeaseState,
    _write_lease,
    acquire_generation_read_lease,
    release_generation_read_lease,
)
from tests.substrate.conftest import require_okg_test_dsn


pytestmark = pytest.mark.postgres


@dataclass(frozen=True)
class _Database:
    admin_dsn: str
    reader_dsn: str
    role_name: str
    checkpoint: str
    deployment: str


@pytest.fixture(scope="module")
def scoped_lease_database() -> Iterator[_Database]:
    base_dsn = require_okg_test_dsn()
    info = psycopg.conninfo.conninfo_to_dict(base_dsn)
    base_name = str(info.get("dbname") or "")
    if not (base_name == "okg_test" or base_name.startswith("okg_test_")):
        pytest.fail("OKG_TEST_DSN must name an explicitly disposable okg_test database")

    suffix = uuid.uuid4().hex[:12]
    database_name = f"okg_test_scoped_lease_{suffix}"
    role_name = f"okg_test_scoped_reader_{suffix}"
    # The reader logs in with its own password. It used to inherit the admin
    # DSN's password while the role had none, which only a trust-auth server
    # accepts; CI's server checks passwords and refused every reader login.
    role_password = uuid.uuid4().hex
    maintenance_info = dict(info)
    maintenance_info["dbname"] = "postgres"
    maintenance_dsn = psycopg.conninfo.make_conninfo(**maintenance_info)
    target_info = dict(info)
    target_info["dbname"] = database_name
    target_dsn = psycopg.conninfo.make_conninfo(**target_info)

    with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
        conn.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}"
            ).format(sql.Identifier(role_name), sql.Literal(role_password))
        )
    try:
        migration.ensure_required_extensions(target_dsn)
        migration.migrate(target_dsn)
        deployment = f"scoped-lease-{uuid.uuid4().hex}"
        with psycopg.connect(target_dsn, autocommit=True) as admin:
            store = ChronosGraphStore.from_connection(admin, ensure_schema=True)
            checkpoint = store.checkpoint_generation(
                branch="default",
                deployment_name=deployment,
                catalog_version_id=1,
                status="published",
                metadata={"node_count": 0, "edge_count": 0},
            )
            _install_test_authority(
                admin,
                role_name=role_name,
                checkpoint=checkpoint,
                deployment=deployment,
            )

        reader_info = psycopg.conninfo.conninfo_to_dict(target_dsn)
        reader_info["user"] = role_name
        reader_info["password"] = role_password
        yield _Database(
            admin_dsn=target_dsn,
            reader_dsn=psycopg.conninfo.make_conninfo(**reader_info),
            role_name=role_name,
            checkpoint=checkpoint,
            deployment=deployment,
        )
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(database_name)
                )
            )
            conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role_name)))


@pytest.fixture(autouse=True)
def no_scoped_pin_debris(scoped_lease_database: _Database) -> Iterator[None]:
    with psycopg.connect(scoped_lease_database.admin_dsn, autocommit=True) as admin:
        admin.execute(
            "DELETE FROM okg.history_generation_pins "
            "WHERE pin_name LIKE 'read:test-scoped:%'"
        )
    yield
    with psycopg.connect(scoped_lease_database.admin_dsn, autocommit=True) as admin:
        remaining = admin.execute(
            "SELECT count(*) FROM okg.history_generation_pins "
            "WHERE pin_name LIKE 'read:test-scoped:%'"
        ).fetchone()
        assert remaining == (0,)


def _install_test_authority(
    admin: psycopg.Connection[Any],
    *,
    role_name: str,
    checkpoint: str,
    deployment: str,
) -> None:
    admin.execute(
        "CREATE TABLE okg.test_scoped_history_read_scope ("
        "checkpoint_id text PRIMARY KEY, deployment_name text NOT NULL, "
        "branch_id text NOT NULL)"
    )
    admin.execute(
        "INSERT INTO okg.test_scoped_history_read_scope VALUES (%s, %s, 'default')",
        (checkpoint, deployment),
    )
    admin.execute(
        """
        CREATE FUNCTION okg.test_acquire_scoped_history_read_lease(p_behavior text)
        RETURNS text
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog, okg, public
        AS $$
        DECLARE scope okg.test_scoped_history_read_scope%ROWTYPE;
                generated_pin text;
        BEGIN
          SELECT * INTO STRICT scope FROM okg.test_scoped_history_read_scope;
          generated_pin := 'read:test-scoped:' || gen_random_uuid()::text;
          IF p_behavior = 'absent' THEN
            RETURN generated_pin;
          END IF;
          INSERT INTO okg.history_generation_pins
            (deployment_name, branch_id, pin_name, checkpoint_id, expires_at)
          VALUES
            (scope.deployment_name,
             CASE WHEN p_behavior = 'wrong_target' THEN 'wrong' ELSE scope.branch_id END,
             generated_pin, scope.checkpoint_id,
             CASE WHEN p_behavior = 'expired' THEN now() - interval '1 second'
                  ELSE now() + interval '5 minutes' END);
          IF p_behavior = 'raise_after_write' THEN
            RAISE EXCEPTION 'forced acquire failure';
          END IF;
          RETURN generated_pin;
        END;
        $$
        """
    )
    admin.execute(
        """
        CREATE FUNCTION okg.test_refresh_scoped_history_read_lease(
          p_pin_name text, p_behavior text)
        RETURNS void
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog, okg, public
        AS $$
        BEGIN
          IF p_behavior <> 'noop' THEN
            UPDATE okg.history_generation_pins
               SET branch_id = CASE WHEN p_behavior = 'wrong_target' THEN 'wrong'
                                    ELSE branch_id END,
                   expires_at = CASE WHEN p_behavior = 'expired'
                                     THEN now() - interval '1 second'
                                     ELSE expires_at + interval '1 minute' END
             WHERE pin_name = p_pin_name;
          END IF;
          IF p_behavior = 'raise_after_write' THEN
            RAISE EXCEPTION 'forced refresh failure';
          END IF;
        END;
        $$
        """
    )
    admin.execute(
        """
        CREATE FUNCTION okg.test_release_scoped_history_read_lease(
          p_pin_name text, p_behavior text)
        RETURNS void
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog, okg, public
        AS $$
        BEGIN
          IF p_behavior <> 'noop' THEN
            DELETE FROM okg.history_generation_pins
             WHERE pin_name = p_pin_name
               AND (p_behavior <> 'wrong_scope' OR branch_id = 'wrong');
          END IF;
          IF p_behavior = 'raise_after_write' THEN
            RAISE EXCEPTION 'forced release failure';
          END IF;
        END;
        $$
        """
    )
    admin.execute(
        """
        CREATE FUNCTION okg.test_inspect_scoped_history_read_lease(p_pin_name text)
        RETURNS TABLE (
          pin_name text, deployment_name text, branch_id text, checkpoint_id text,
          observed_at_epoch double precision, expires_at_epoch double precision)
        LANGUAGE sql STABLE SECURITY DEFINER
        SET search_path = pg_catalog, okg, public
        AS $$
          SELECT p.pin_name, p.deployment_name, p.branch_id, p.checkpoint_id,
                 extract(epoch FROM clock_timestamp())::double precision,
                 extract(epoch FROM p.expires_at)::double precision
            FROM okg.history_generation_pins AS p
           WHERE p.pin_name = p_pin_name
             AND p.pin_name LIKE 'read:test-scoped:%'
        $$
        """
    )
    identifier = sql.Identifier(role_name)
    admin.execute(sql.SQL("GRANT USAGE ON SCHEMA public, okg TO {}").format(identifier))
    admin.execute(
        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public, okg TO {}").format(
            identifier
        )
    )
    admin.execute(
        sql.SQL(
            "REVOKE ALL ON okg.history_generation_pins, "
            "okg.test_scoped_history_read_scope FROM {}"
        ).format(identifier)
    )
    functions = (
        "okg.test_acquire_scoped_history_read_lease(text)",
        "okg.test_refresh_scoped_history_read_lease(text,text)",
        "okg.test_release_scoped_history_read_lease(text,text)",
        "okg.test_inspect_scoped_history_read_lease(text)",
    )
    for function in functions:
        admin.execute(f"REVOKE EXECUTE ON FUNCTION {function} FROM PUBLIC")
        admin.execute(
            sql.SQL(f"GRANT EXECUTE ON FUNCTION {function} TO {{}}").format(identifier)
        )
    admin.execute(
        sql.SQL(
            "GRANT EXECUTE ON FUNCTION "
            "okg.compacted_generation_record(text,text) TO {}"
        ).format(identifier)
    )
    for function in (
        "okg.acquire_history_generation_read_lease(text,text,text,text)",
        "okg.release_history_generation_read_lease(text,text,text)",
    ):
        admin.execute(
            sql.SQL(f"REVOKE EXECUTE ON FUNCTION {function} FROM {{}}").format(identifier)
        )


@dataclass
class _ScopedLeaseBackend:
    acquire_behavior: str = "normal"
    refresh_behavior: str = "normal"
    release_behavior: str = "normal"
    calls: list[str] = field(default_factory=list)

    def acquire(self, context: Any, **_scope: Any) -> str:
        self.calls.append(f"acquire:{self.acquire_behavior}")
        row = context.db.execute(
            "SELECT okg.test_acquire_scoped_history_read_lease(?) AS pin_name",
            (self.acquire_behavior,),
        ).fetchone()
        assert row is not None
        return str(row["pin_name"])

    def refresh(self, context: Any, *, pin_name: str, **_scope: Any) -> None:
        self.calls.append(f"refresh:{self.refresh_behavior}")
        context.db.execute(
            "SELECT okg.test_refresh_scoped_history_read_lease(?, ?)",
            (pin_name, self.refresh_behavior),
        )

    def release(self, context: Any, *, pin_name: str, **_scope: Any) -> None:
        self.calls.append(f"release:{self.release_behavior}")
        context.db.execute(
            "SELECT okg.test_release_scoped_history_read_lease(?, ?)",
            (pin_name, self.release_behavior),
        )

    def inspect(
        self, context: Any, *, pin_name: str
    ) -> GenerationReadLeaseState | None:
        self.calls.append("inspect")
        row = context.db.execute(
            "SELECT * FROM okg.test_inspect_scoped_history_read_lease(?)",
            (pin_name,),
        ).fetchone()
        if row is None:
            return None
        return GenerationReadLeaseState(
            pin_name=str(row["pin_name"]),
            deployment_name=str(row["deployment_name"]),
            branch=str(row["branch_id"]),
            checkpoint_id=str(row["checkpoint_id"]),
            observed_at_epoch=float(row["observed_at_epoch"]),
            expires_at_epoch=float(row["expires_at_epoch"]),
        )


def _store(
    database: _Database,
    backend: _ScopedLeaseBackend,
    *,
    context_autocommit: bool,
) -> tuple[psycopg.Connection[Any], ChronosGraphStore]:
    conn = psycopg.connect(database.reader_dsn, autocommit=True)
    return conn, ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=context_autocommit,
        catalog_version_id=1,
        history_read_lease_backend=backend,
    )


def _pin_count(database: _Database) -> int:
    with psycopg.connect(database.admin_dsn, autocommit=True) as admin:
        row = admin.execute(
            "SELECT count(*) FROM okg.history_generation_pins "
            "WHERE pin_name LIKE 'read:test-scoped:%'"
        ).fetchone()
        assert row is not None
        return int(row[0])


def test_supported_checkout_uses_attested_scoped_authority(
    scoped_lease_database: _Database,
) -> None:
    database = scoped_lease_database
    with psycopg.connect(database.admin_dsn, autocommit=True) as admin:
        acl = admin.execute(
            "SELECT has_table_privilege(%s, 'okg.history_generation_pins', 'SELECT'), "
            "has_table_privilege(%s, 'okg.history_generation_pins', 'INSERT'), "
            "has_function_privilege(%s, "
            "'okg.acquire_history_generation_read_lease(text,text,text,text)', 'EXECUTE')",
            (database.role_name, database.role_name, database.role_name),
        ).fetchone()
        assert acl == (False, False, False)

    backend = _ScopedLeaseBackend()
    conn, store = _store(database, backend, context_autocommit=False)
    with conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("SELECT * FROM okg.history_generation_pins")
        with conn.transaction():
            session = store.checkout_generation(database.checkpoint, branch="default")
        with conn.transaction():
            assert session.query("SELECT count(*) AS n FROM okg.graph_nodes") == [
                {"n": 0}
            ]
        with conn.transaction():
            session.close()

    assert backend.calls.count("inspect") >= 3
    assert _pin_count(database) == 0


@pytest.mark.parametrize(
    ("behavior", "message"),
    [
        ("absent", "did not persist"),
        ("wrong_target", "wrong lease target"),
        ("expired", "expired lease"),
        ("raise_after_write", "forced acquire failure"),
    ],
)
def test_acquire_refuses_unattested_state_and_rolls_back(
    scoped_lease_database: _Database,
    behavior: str,
    message: str,
) -> None:
    database = scoped_lease_database
    backend = _ScopedLeaseBackend(acquire_behavior=behavior)
    conn, store = _store(database, backend, context_autocommit=True)
    with conn, pytest.raises(Exception, match=message):
        acquire_generation_read_lease(
            store.context,
            checkpoint_id=database.checkpoint,
            deployment_name=database.deployment,
            branch="default",
            lease_backend=backend,
        )
    assert _pin_count(database) == 0


def test_acquire_refuses_when_the_pin_relation_is_missing(
    scoped_lease_database: _Database,
) -> None:
    database = scoped_lease_database
    with psycopg.connect(database.admin_dsn, autocommit=True) as admin:
        admin.execute(
            "ALTER TABLE okg.history_generation_pins "
            "RENAME TO test_missing_history_generation_pins"
        )
    try:
        backend = _ScopedLeaseBackend()
        conn, store = _store(database, backend, context_autocommit=True)
        with conn, pytest.raises(psycopg.errors.UndefinedTable):
            acquire_generation_read_lease(
                store.context,
                checkpoint_id=database.checkpoint,
                deployment_name=database.deployment,
                branch="default",
                lease_backend=backend,
            )
    finally:
        with psycopg.connect(database.admin_dsn, autocommit=True) as admin:
            admin.execute(
                "ALTER TABLE okg.test_missing_history_generation_pins "
                "RENAME TO history_generation_pins"
            )


@pytest.mark.parametrize(
    ("operation", "behavior", "message"),
    [
        ("refresh", "noop", "did not extend"),
        ("refresh", "wrong_target", "wrong lease target"),
        ("refresh", "expired", "expired lease"),
        ("refresh", "raise_after_write", "forced refresh failure"),
        ("release", "noop", "did not release"),
        ("release", "wrong_scope", "did not release"),
        ("release", "raise_after_write", "forced release failure"),
    ],
)
def test_refresh_and_release_refuse_noop_wrong_expired_or_exception(
    scoped_lease_database: _Database,
    operation: str,
    behavior: str,
    message: str,
) -> None:
    database = scoped_lease_database
    backend = _ScopedLeaseBackend()
    conn, store = _store(database, backend, context_autocommit=True)
    with conn:
        pin_name = acquire_generation_read_lease(
            store.context,
            checkpoint_id=database.checkpoint,
            deployment_name=database.deployment,
            branch="default",
            lease_backend=backend,
        )
        assert pin_name is not None
        if operation == "refresh":
            backend.refresh_behavior = behavior

            def call() -> None:
                _write_lease(
                    store.context,
                    checkpoint_id=database.checkpoint,
                    deployment_name=database.deployment,
                    branch="default",
                    pin_name=pin_name,
                    lease_backend=backend,
                )

        else:
            backend.release_behavior = behavior

            def call() -> None:
                release_generation_read_lease(
                    store.context,
                    checkpoint_id=database.checkpoint,
                    deployment_name=database.deployment,
                    branch="default",
                    pin_name=pin_name,
                    lease_backend=backend,
                )

        with pytest.raises(Exception, match=message):
            call()
        assert _pin_count(database) == 1
        backend.release_behavior = "normal"
        release_generation_read_lease(
            store.context,
            checkpoint_id=database.checkpoint,
            deployment_name=database.deployment,
            branch="default",
            pin_name=pin_name,
            lease_backend=backend,
        )


def test_release_refuses_an_absent_pin(scoped_lease_database: _Database) -> None:
    database = scoped_lease_database
    backend = _ScopedLeaseBackend()
    conn, store = _store(database, backend, context_autocommit=True)
    with conn, pytest.raises(Exception, match="did not persist"):
        release_generation_read_lease(
            store.context,
            checkpoint_id=database.checkpoint,
            deployment_name=database.deployment,
            branch="default",
            pin_name="read:test-scoped:absent",
            lease_backend=backend,
        )
    assert not any(call.startswith("release:") for call in backend.calls)


@pytest.mark.parametrize("context_autocommit", [False, True])
def test_checkout_failure_releases_the_acquired_pin(
    scoped_lease_database: _Database,
    monkeypatch: pytest.MonkeyPatch,
    context_autocommit: bool,
) -> None:
    database = scoped_lease_database
    backend = _ScopedLeaseBackend()
    conn, store = _store(
        database, backend, context_autocommit=context_autocommit
    )

    def fail_checkout(_checkpoint: str) -> None:
        raise RuntimeError("forced checkout failure")

    monkeypatch.setattr(store.context, "checkout_checkpoint", fail_checkout)
    with conn:
        if context_autocommit:
            with pytest.raises(KeyError, match="not found"):
                store.checkout_generation(database.checkpoint, branch="default")
        else:
            with pytest.raises(KeyError, match="not found"), conn.transaction():
                store.checkout_generation(database.checkpoint, branch="default")

    assert "release:normal" in backend.calls
    assert _pin_count(database) == 0
