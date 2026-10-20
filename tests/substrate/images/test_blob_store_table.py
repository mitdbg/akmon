"""The general blob store ``okg.blobs`` and its store API.

PACT first-class-images, task.first-class-images.table (issue #3046):

* migration 0037 creates the same table, index, view and grants as schema.sql;
* the checks refuse a row with both or neither of bytes and error, and an
  unknown kind;
* ``okg.substrate.db.blob_store`` put/put_error/touch/get, including the
  ``last_used_at`` refresh and the conflict rule;
* okg_mcp reads image rows through ``okg.image_blobs`` only, cannot read
  ``okg.blobs``, and cannot write either.
"""
from __future__ import annotations

import hashlib
import importlib.resources

import psycopg
import pytest

from okg.substrate.db import blob_store
from okg.substrate.errors import SubstrateError

pytestmark = pytest.mark.postgres

PNG = b"\x89PNG\r\n\x1a\nnot-really-a-png"


def _migration_sql() -> str:
    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath("migrations/0041_blob_store.sql")
        .read_text()
    )


def _shape(conn: psycopg.Connection) -> dict[str, object]:
    """The catalog facts schema.sql and the migration must agree on."""
    columns = conn.execute(
        """
        SELECT column_name, data_type, is_nullable, column_default
          FROM information_schema.columns
         WHERE table_schema = 'okg' AND table_name = 'blobs'
         ORDER BY ordinal_position
        """
    ).fetchall()
    constraints = conn.execute(
        """
        SELECT conname, pg_get_constraintdef(oid)
          FROM pg_constraint
         WHERE conrelid = 'okg.blobs'::regclass
         ORDER BY conname
        """
    ).fetchall()
    indexes = conn.execute(
        """
        SELECT indexname, indexdef FROM pg_indexes
         WHERE schemaname = 'okg' AND tablename = 'blobs'
         ORDER BY indexname
        """
    ).fetchall()
    view = conn.execute(
        "SELECT pg_get_viewdef('okg.image_blobs'::regclass, true), "
        "       (SELECT reloptions FROM pg_class "
        "         WHERE oid = 'okg.image_blobs'::regclass)"
    ).fetchone()
    privileges = conn.execute(
        """
        SELECT r.rolname, rel.relation, p.privilege,
               has_table_privilege(r.rolname, rel.relation, p.privilege)
          FROM (VALUES ('app_rw'), ('app_ro'), ('okg_mcp')) AS r(rolname)
         CROSS JOIN (VALUES ('okg.blobs'), ('okg.image_blobs')) AS rel(relation)
         CROSS JOIN (VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'))
                 AS p(privilege)
         ORDER BY 1, 2, 3
        """
    ).fetchall()
    return {
        "columns": columns,
        "constraints": constraints,
        "indexes": indexes,
        "view": view,
        "privileges": privileges,
    }


def test_migration_applies_and_converges_with_schema_sql(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fresh = _shape(conn)
        applied = conn.execute(
            "SELECT 1 FROM okg.schema_migrations WHERE migration_id = %s",
            ("0041_blob_store",),
        ).fetchone()
        assert applied is not None, "migrate did not record 0041_blob_store"

        # Re-running the migration over the fresh install is a no-op.
        with conn.transaction():
            conn.execute(_migration_sql())
        assert _shape(conn) == fresh

        # An upgraded database: the relations are absent until 0037 runs.
        # Then views.sql's registry applies okg_mcp's grant, as migrate
        # does after the ordered migrations.
        conn.execute("DROP VIEW okg.image_blobs")
        conn.execute("DROP TABLE okg.blobs")
        with conn.transaction():
            conn.execute(_migration_sql())
        conn.execute("SELECT okg.apply_grant_registry()")
        assert _shape(conn) == fresh

    expected = {
        ("app_rw", "okg.blobs", privilege): True
        for privilege in ("DELETE", "INSERT", "SELECT", "UPDATE")
    }
    expected[("okg_mcp", "okg.image_blobs", "SELECT")] = True
    granted = {
        (role, relation, privilege): allowed
        for role, relation, privilege, allowed in fresh["privileges"]
        if allowed
    }
    assert granted == expected


def test_registry_holds_the_okg_mcp_view_grant_and_nothing_on_the_table(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT relation, role_name, privileges FROM okg.grant_registry "
            "WHERE relation IN ('okg.blobs', 'okg.image_blobs') ORDER BY 1"
        ).fetchall()
        drift = conn.execute(
            "SELECT finding_kind, relation, role_name "
            "  FROM okg.grant_registry_drift() "
            " WHERE relation IN ('okg.blobs', 'okg.image_blobs')"
        ).fetchall()
    assert rows == [("okg.image_blobs", "okg_mcp", "SELECT")]
    assert drift == []


@pytest.mark.parametrize(
    ("columns", "values"),
    [
        pytest.param(
            "bytes, sha256, mime_type, size_bytes, error",
            (PNG, "a" * 64, "image/png", len(PNG), "image_decode_failed"),
            id="both-bytes-and-error",
        ),
        pytest.param(
            "bytes, sha256, mime_type, size_bytes, error",
            (None, None, None, None, None),
            id="neither-bytes-nor-error",
        ),
        pytest.param(
            "bytes, sha256, mime_type, size_bytes, error",
            (PNG, None, "image/png", len(PNG), None),
            id="bytes-without-sha256",
        ),
        pytest.param(
            "bytes, sha256, mime_type, size_bytes, error",
            (None, "a" * 64, None, None, "image_decode_failed"),
            id="error-with-sha256",
        ),
    ],
)
def test_payload_shape_check_refuses(
    clean_runtime: str, columns: str, values: tuple[object, ...]
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation) as raised:
            conn.execute(
                f"INSERT INTO okg.blobs (id, kind, {columns}) "
                "VALUES (%s, 'image', %s, %s, %s, %s, %s)",
                ("shape-probe", *values),
            )
        assert raised.value.diag.constraint_name == "blobs_payload_shape"


def test_kind_check_refuses_an_unknown_kind_in_sql_and_in_the_api(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation) as raised:
            conn.execute(
                "INSERT INTO okg.blobs (id, kind, error) "
                "VALUES ('kind-probe', 'video', 'x')"
            )
        assert raised.value.diag.constraint_name == "blobs_kind_known"

        for call in (
            lambda: blob_store.touch(conn, kind="video", id="x"),
            lambda: blob_store.get(conn, kind="video", id="x"),
            lambda: blob_store.put(
                conn, kind="video", id="x", data=PNG,
                mime_type="image/png", meta={},
            ),
            lambda: blob_store.put_error(
                conn, kind="video", id="x", error="e", meta={},
            ),
        ):
            with pytest.raises(SubstrateError) as refused:
                call()
            assert refused.value.data["reason"] == "blob_kind_unknown"


def _age(conn: psycopg.Connection, id: str) -> None:
    conn.execute(
        "UPDATE okg.blobs SET last_used_at = now() - interval '30 days' "
        "WHERE id = %s",
        (id,),
    )


def test_put_get_touch_and_the_conflict_rule(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert blob_store.touch(conn, kind="image", id="r1") is None
        assert blob_store.get(conn, kind="image", id="r1") is None

        stored = blob_store.put(
            conn, kind="image", id="r1", data=PNG, mime_type="image/png",
            meta={"width": 3, "height": 2},
        )
        assert stored.sha256 == hashlib.sha256(PNG).hexdigest()
        assert stored.size_bytes == len(PNG)
        assert stored.error is None
        assert stored.data is None
        assert stored.meta == {"width": 3, "height": 2}

        read = blob_store.get(conn, kind="image", id="r1")
        assert read is not None and read.data == PNG
        assert read.sha256 == stored.sha256

        # get does not refresh; touch does.
        _age(conn, "r1")
        aged = blob_store.get(conn, kind="image", id="r1")
        assert aged is not None
        assert blob_store.get(conn, kind="image", id="r1").last_used_at == (
            aged.last_used_at
        )
        touched = blob_store.touch(conn, kind="image", id="r1")
        assert touched is not None
        assert touched.last_used_at > aged.last_used_at
        assert touched.created_at == stored.created_at

        # A second put under the same id keeps the stored bytes, refreshes
        # last_used_at, and returns the stored row.
        _age(conn, "r1")
        again = blob_store.put(
            conn, kind="image", id="r1", data=b"other bytes",
            mime_type="image/jpeg", meta={"width": 9},
        )
        assert again.sha256 == stored.sha256
        assert again.mime_type == "image/png"
        assert again.meta == {"width": 3, "height": 2}
        assert again.last_used_at > aged.last_used_at
        assert blob_store.get(conn, kind="image", id="r1").data == PNG

        # A failure row under its own id; a later put keeps the failure.
        failed = blob_store.put_error(
            conn, kind="image", id="r2", error="pdf_render_failed",
            meta={"source": "x.pdf"},
        )
        assert failed.error == "pdf_render_failed"
        assert (failed.sha256, failed.mime_type, failed.size_bytes) == (
            None, None, None,
        )
        kept = blob_store.put(
            conn, kind="image", id="r2", data=PNG, mime_type="image/png",
            meta={},
        )
        assert kept.error == "pdf_render_failed"
        assert blob_store.get(conn, kind="image", id="r2").data is None

        with pytest.raises(SubstrateError) as empty:
            blob_store.put_error(conn, kind="image", id="r3", error="", meta={})
        assert empty.value.data["reason"] == "blob_error_empty"

        count = conn.execute("SELECT count(*) FROM okg.blobs").fetchone()[0]
        assert count == 2


def test_put_refuses_an_id_stored_under_another_kind(clean_runtime: str) -> None:
    """Only one kind exists, so the other kind is planted with the check
    dropped -- the API must refuse rather than overwrite or return it."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("ALTER TABLE okg.blobs DROP CONSTRAINT blobs_kind_known")
        conn.execute(
            "INSERT INTO okg.blobs (id, kind, error) "
            "VALUES ('shared', 'other', 'x')"
        )
        with pytest.raises(SubstrateError) as refused:
            blob_store.put(
                conn, kind="image", id="shared", data=PNG,
                mime_type="image/png", meta={},
            )
        assert refused.value.data == {
            "reason": "blob_kind_mismatch",
            "id": "shared",
            "kind": "image",
            "stored_kind": "other",
        }
        assert blob_store.touch(conn, kind="image", id="shared") is None
        # The image view never shows the other kind.
        assert conn.execute(
            "SELECT count(*) FROM okg.image_blobs WHERE id = 'shared'"
        ).fetchone()[0] == 0


def test_okg_mcp_reads_the_view_only_and_writes_nothing(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        blob_store.put(
            conn, kind="image", id="mcp-probe", data=PNG,
            mime_type="image/png", meta={},
        )
    with psycopg.connect(clean_runtime) as conn:
        conn.execute("SET ROLE okg_mcp")
        row = conn.execute(
            "SELECT bytes, sha256 FROM okg.image_blobs WHERE id = 'mcp-probe'"
        ).fetchone()
        assert row is not None and bytes(row[0]) == PNG
        assert row[1] == hashlib.sha256(PNG).hexdigest()
        conn.rollback()

        statements = (
            "SELECT count(*) FROM okg.blobs",
            "INSERT INTO okg.blobs (id, kind, error) VALUES ('w', 'image', 'x')",
            "UPDATE okg.blobs SET last_used_at = now()",
            "DELETE FROM okg.blobs",
            "INSERT INTO okg.image_blobs (id, error) VALUES ('w', 'x')",
            "UPDATE okg.image_blobs SET last_used_at = now()",
            "DELETE FROM okg.image_blobs",
        )
        for statement in statements:
            conn.execute("SET ROLE okg_mcp")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(statement)
            conn.rollback()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        remaining = conn.execute(
            "SELECT id, error FROM okg.blobs ORDER BY id"
        ).fetchall()
    assert remaining == [("mcp-probe", None)]
