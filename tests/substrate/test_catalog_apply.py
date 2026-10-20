"""`debug-substrate-okg-self` Phase 6 — `okg catalog load --apply`.

Locks in the atomic-DDL apply path:
  - `apply_composed_catalog` inserts a fresh
    `catalog_versions` row (status=pending) and stamps every
    subtype / narrowing / alias / identifier_pattern under that
    cv;
  - the row is bumped to `applied` only after every insert
    succeeds;
  - on any DB failure mid-stream, the txn rolls back and
    nothing is observable from the new cv.

Failure-mode coverage:
  - prior `'applied'` cv rows are untouched;
  - a duplicate-cv re-apply does NOT alias under the OLD cv.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.catalog.modules_apply import (
    apply_composed_catalog,
)
from okg.substrate.catalog.modules_compose import (
    ComposedCatalog,
    NarrowingRow,
)


pytestmark = pytest.mark.postgres


def _make_minimal_composed() -> ComposedCatalog:
    """A small but realistic composed catalog: 3 subtypes, 1
    narrowing, 1 alias, 1 identifier_pattern."""
    composed = ComposedCatalog(
        deployment_name="test-deploy",
        modules=["mod_a"],
        subtypes={"concept", "document", "section"},
        subtype_origin={
            "concept": "mod_a",
            "document": "mod_a",
            "section": "mod_a",
        },
        narrowings=[
            NarrowingRow(
                parent_subtype="document",
                edge_archetype="contains",
                child_subtype="section",
                origin="mod_a",
                narrowing_name="DocumentContainsSection",
            ),
        ],
        aliases=[
            {"alias": "doc", "canonical": "document", "scope": None},
        ],
        identifier_patterns=[
            {
                "id_type": "test_id",
                "regex": r"X[0-9]+",
                "validator_kind": None,
                "description": "test pattern",
            },
        ],
    )
    return composed


def test_apply_composed_catalog_writes_all_tables(
    clean_runtime: str,
) -> None:
    """Apply once; assert every table got rows under the new cv."""
    composed = _make_minimal_composed()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = apply_composed_catalog(conn, composed)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM okg.catalog_versions "
                " WHERE catalog_version_id = %s", (cv,),
            )
            assert cur.fetchone()[0] == "applied"
            cur.execute(
                "SELECT count(*) FROM okg.subtypes "
                " WHERE catalog_version_id = %s", (cv,),
            )
            assert cur.fetchone()[0] == 3
            cur.execute(
                "SELECT count(*) FROM okg.narrowings "
                " WHERE catalog_version_id = %s", (cv,),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM okg.aliases "
                " WHERE catalog_version_id = %s", (cv,),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM okg.identifier_patterns "
                " WHERE catalog_version_id = %s", (cv,),
            )
            assert cur.fetchone()[0] == 1


def test_apply_does_not_touch_prior_cv(clean_runtime: str) -> None:
    """A second apply creates a new cv; the prior cv's rows are
    untouched and remain 'applied'."""
    composed = _make_minimal_composed()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv1 = apply_composed_catalog(conn, composed)
        cv2 = apply_composed_catalog(conn, composed)
        assert cv1 != cv2
        assert cv2 > cv1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM okg.catalog_versions "
                " WHERE catalog_version_id = %s", (cv1,),
            )
            assert cur.fetchone()[0] == "applied"
            cur.execute(
                "SELECT count(*) FROM okg.subtypes "
                " WHERE catalog_version_id = %s", (cv1,),
            )
            assert cur.fetchone()[0] == 3


def test_apply_failure_rolls_back_cv(clean_runtime: str) -> None:
    """Inject an invalid identifier_pattern (bad validator_kind);
    apply must raise, and the partially-attempted cv must NOT be
    visible (rolled back)."""
    composed = _make_minimal_composed()
    composed.identifier_patterns = [
        {
            "id_type": "bad",
            "regex": r"Y[0-9]+",
            "validator_kind": "not_a_real_validator",
            "description": "intentionally invalid",
        },
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) "
                "  FROM okg.catalog_versions"
            )
            (max_before,) = cur.fetchone()

        with pytest.raises(psycopg.errors.CheckViolation):
            apply_composed_catalog(conn, composed)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) "
                "  FROM okg.catalog_versions"
            )
            (max_after,) = cur.fetchone()
        # No new cv visible (the failed apply rolled the row back
        # together with its child writes).
        assert max_after == max_before
