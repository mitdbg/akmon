from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import (
    ComposedCatalog,
    NarrowingRow,
)
from okg.substrate.catalog.ownership import (
    catalog_identity_for,
    ensure_catalog_write_allowed,
    ensure_catalog_ownership_schema,
    resolve_catalog_for_publish,
)
from okg.substrate.errors import CatalogOwnershipMismatch


pytestmark = pytest.mark.postgres


def _composed(name: str) -> ComposedCatalog:
    return ComposedCatalog(
        deployment_name=name,
        modules=[],
        subtypes={"source_file", "git_commit"},
        subtype_origin={
            "source_file": "test",
            "git_commit": "test",
        },
        narrowings=[
            NarrowingRow(
                parent_subtype="source_file",
                edge_archetype="references",
                child_subtype="git_commit",
                origin="test",
                narrowing_name="file_references_commit",
            )
        ],
    )


def _identity(tmp_path: Path, name: str):
    d = tmp_path / name
    d.mkdir()
    (d / "deployment.yaml").write_text(f"name: {name}\nmodules: []\n")
    return catalog_identity_for(
        composed=_composed(name),
        deployment_dir=d,
        command="test publisher",
    )


def _reset_catalog(conn: psycopg.Connection) -> None:
    ensure_catalog_ownership_schema(conn)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE okg.deployment_write_ownership")
        cur.execute("TRUNCATE okg.catalog_versions CASCADE")
        cur.execute(
            """
            INSERT INTO okg.catalog_versions
              (catalog_version_id, status, applied_at,
               diff_summary, reconcile_plan)
            VALUES
              (1, 'applied', now(),
               '{"reason":"test seed"}'::jsonb,
               '{"reconcile":[]}'::jsonb)
            """
        )
        cur.execute(
            """
            SELECT setval(
              pg_get_serial_sequence(
                'okg.catalog_versions', 'catalog_version_id'
              ),
              1
            )
            """
        )


def test_publisher_rejects_incompatible_latest_catalog(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    identity = _identity(tmp_path, "publisher-catalog-owner-test")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=identity)
        compatible_cv = apply_composed_catalog(
            conn,
            _composed("publisher-catalog-owner-test"),
            identity=identity,
        )
        assert resolve_catalog_for_publish(
            conn,
            identity=identity,
        ) == compatible_cv

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.catalog_versions (status, applied_at)
                VALUES ('applied', now())
                """
            )

        with pytest.raises(CatalogOwnershipMismatch):
            resolve_catalog_for_publish(conn, identity=identity)
