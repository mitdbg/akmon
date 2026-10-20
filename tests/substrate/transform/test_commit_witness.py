"""Commit-witness ledger coverage after retiring DeltaSet commit clients."""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.transform import no_dangling_edges_program  # noqa: E402
from okg.substrate.transform.commit import persist_transform_program  # noqa: E402

pytestmark = pytest.mark.postgres


def _unique_program():
    base = no_dangling_edges_program()
    return replace(base, attrs={**base.attrs, "nonce": uuid4().hex})


def test_persist_transform_program_is_content_addressed(clean_runtime: str) -> None:
    program = _unique_program()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first_id = persist_transform_program(conn, program)
        second_id = persist_transform_program(conn, program)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT program_checksum, program_ref
                  FROM okg.ir_programs
                 WHERE program_id = %s::uuid
                """,
                (first_id,),
            )
            checksum, program_ref = cur.fetchone()

    assert first_id == second_id
    assert checksum == program.checksum()
    assert program_ref == program.program_id


def test_sql_deltaset_commit_function_is_retired(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regprocedure('okg.commit_delta_set(jsonb,bigint)')")
        assert cur.fetchone()[0] is None
