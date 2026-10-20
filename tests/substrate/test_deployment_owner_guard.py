"""P0 #419 prevent-recurrence: the per-database owner stamp must fail closed
when a process configured for deployment X tries to publish into deployment Y's
database.

The contamination that motivated this (tinydb-code-lsp-head10 publishing 1713
generations into the live okg_workspace DB) reached the database because no
guard on the publish WRITE path cross-checked "does this database belong to the
deployment that is writing." These tests pin that guard at its two surfaces: the
``assert_db_owner`` unit logic, and the integration point inside
``Publisher.reserve`` where a refused publish must commit ZERO rows.

Stamping is a provisioning action, so an UNSTAMPED database stays permissive —
the guard arms once a deployment is deliberately stamped.

Every test runs on a fresh clone of the migrated template (``clean_runtime``).
Clearing only the owner stamp on the shared worker database was not enough:
``claim_db_owner`` also refuses an unstamped database that holds another
deployment's published generations, and an earlier publishing test on the same
worker leaves those behind (#3718).
"""
from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.db.deployment_owner import (
    assert_db_owner,
    claim_db_owner,
    foreign_published_deployments,
    read_db_owner,
    stamp_db_owner,
)
from okg.substrate.errors import DeploymentOwnerMismatch
from okg.substrate.publisher import Publisher

pytestmark = pytest.mark.postgres


def _gen_count(cur) -> int:
    cur.execute("SELECT count(*) FROM okg.graph_generations")
    return int(cur.fetchone()[0])


@pytest.fixture
def owner_db(clean_runtime: str) -> Iterator[str]:
    """A fresh database with no owner stamp and no deployment's generations
    (only the template's bootstrap generation).

    ``clean_runtime`` recreates the worker database at setup only, so the
    teardown clears the stamp these tests write: a later test on the same
    worker that uses the shared database directly must not inherit it.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            assert read_db_owner(cur) is None
            assert foreign_published_deployments(cur, f"nobody-{uuid4().hex[:8]}") == []
    yield clean_runtime
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("DELETE FROM okg.database_owner")


def test_unstamped_db_is_permissive(owner_db: str) -> None:
    """An unstamped DB does not block any publisher — the guard is armed by
    stamping (provisioning), not by publishing."""
    with psycopg.connect(owner_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            assert read_db_owner(cur) is None
            assert_db_owner(cur, f"anyone-{uuid4().hex[:8]}")   # no raise
            assert read_db_owner(cur) is None                   # publishing never stamps


def test_matching_owner_allowed_foreign_refused(owner_db: str) -> None:
    """A stamped DB allows its owner and refuses any other deployment."""
    owner = f"owner-A-{uuid4().hex[:8]}"
    intruder = f"owner-B-{uuid4().hex[:8]}"
    with psycopg.connect(owner_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            stamp_db_owner(cur, owner)
            assert read_db_owner(cur) == owner
            assert_db_owner(cur, owner)                         # owner -> OK, no raise
            with pytest.raises(DeploymentOwnerMismatch) as ei:
                assert_db_owner(cur, intruder)
    assert ei.value.code == "deployment_owner_mismatch"
    assert ei.value.data["owner_deployment_name"] == owner
    assert ei.value.data["claimed_deployment_name"] == intruder


def test_stamp_is_singleton(owner_db: str) -> None:
    """Stamping is idempotent and a second deployment cannot also become an
    active owner (the singleton PK enforces one owner per physical DB)."""
    first = f"first-{uuid4().hex[:8]}"
    second = f"second-{uuid4().hex[:8]}"
    with psycopg.connect(owner_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            stamp_db_owner(cur, first)
            stamp_db_owner(cur, first)                          # idempotent
            stamp_db_owner(cur, second)                         # ON CONFLICT DO NOTHING
            assert read_db_owner(cur) == first                  # first owner wins, stays


def test_claim_stamps_unowned_database_idempotently(owner_db: str) -> None:
    owner = f"claim-owner-{uuid4().hex[:8]}"
    with psycopg.connect(owner_db) as conn, conn.cursor() as cur:
        first = claim_db_owner(cur, owner, command="test claim")
        second = claim_db_owner(cur, owner, command="test claim")
        conn.commit()

    assert first["owner_before"] is None
    assert first["owner_after"] == owner
    assert second["owner_before"] == owner
    assert second["owner_after"] == owner


def test_reserve_refuses_foreign_owner_and_commits_zero_rows(owner_db: str) -> None:
    """The integration point: Publisher.reserve into a foreign-owned DB raises
    and leaves the generation ledger byte-for-byte unchanged (the contamination
    write never lands)."""
    owner = f"reserve-owner-{uuid4().hex[:8]}"
    intruder = f"reserve-intruder-{uuid4().hex[:8]}"
    with psycopg.connect(owner_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            stamp_db_owner(cur, owner)
            before = _gen_count(cur)

    pub = Publisher(dsn=owner_db, deployment_name=intruder, coordination_mode="dbos")
    with psycopg.connect(owner_db) as conn:
        with pytest.raises(DeploymentOwnerMismatch):
            pub.reserve(conn)

    with psycopg.connect(owner_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            assert _gen_count(cur) == before                    # zero rows committed
            assert read_db_owner(cur) == owner                  # owner unchanged
