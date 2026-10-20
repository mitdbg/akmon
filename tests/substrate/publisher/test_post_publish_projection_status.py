"""Tests for the post-publish view-edge projection status surface.

Pinned by ``surface-post-publish-projection-failures``. The publisher's
``_run_post_publish_views`` upserts one row into
``okg.post_publish_projection_status`` on every invocation — success or
failure. The publish itself is not gated by projection success; the
row persists the failure so ``okg doctor`` can surface it.

This module covers the publisher's upsert write-path. The ``okg doctor
--check post-publish-projection`` reader is covered in
tests/substrate/cli/test_doctor_post_publish_projection.py.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import psycopg
import pytest


pytestmark = pytest.mark.postgres


def _record_status(
    dsn: str,
    *,
    deployment_name: str,
    generation_id: int,
    error: str | None,
) -> None:
    """Drive the publisher's upsert helper directly.

    Constructing a full Publisher requires many dependencies (catalog
    composer, projector, enrichers, etc.). The upsert method is a
    self-contained instance method that only needs ``self.deployment_name``
    and a connection — exercise it via ``MagicMock`` to keep the test
    focused on the contract.
    """
    from okg.substrate.publisher.publisher import Publisher

    fake_self = MagicMock()
    fake_self.deployment_name = deployment_name
    with psycopg.connect(dsn, autocommit=True) as conn:
        Publisher._record_post_publish_projection_status(
            fake_self,
            conn,
            generation_id=generation_id,
            error=error,
        )


def test_record_status_inserts_success_row(clean_runtime: str) -> None:
    """A clean projection records status='success' with NULL error."""
    _record_status(
        clean_runtime,
        deployment_name="default",
        generation_id=42,
        error=None,
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, error
              FROM okg.post_publish_projection_status
             WHERE deployment_name = %s
               AND generation_id = %s
            """,
            ("default", 42),
        )
        row = cur.fetchone()
    assert row is not None, "upsert produced no row"
    assert row[0] == "success"
    assert row[1] is None


def test_record_status_inserts_failure_row(clean_runtime: str) -> None:
    """A failing projection records status='failed' with the exc string."""
    _record_status(
        clean_runtime,
        deployment_name="default",
        generation_id=43,
        error="RuntimeError: schema mismatch",
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, error
              FROM okg.post_publish_projection_status
             WHERE deployment_name = %s
               AND generation_id = %s
            """,
            ("default", 43),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "failed"
    assert row[1] == "RuntimeError: schema mismatch"


def test_record_status_upserts_on_conflict(clean_runtime: str) -> None:
    """A second call for the same (deployment, generation) overwrites.

    The publisher may re-run the post-publish projection on retry; the
    table contract is keyed on (deployment_name, generation_id) so the
    most-recent run wins.
    """
    _record_status(
        clean_runtime,
        deployment_name="default",
        generation_id=44,
        error="RuntimeError: first try",
    )
    _record_status(
        clean_runtime,
        deployment_name="default",
        generation_id=44,
        error=None,  # second run succeeds
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, error
              FROM okg.post_publish_projection_status
             WHERE deployment_name = %s
               AND generation_id = %s
            """,
            ("default", 44),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "success"
    assert row[1] is None
