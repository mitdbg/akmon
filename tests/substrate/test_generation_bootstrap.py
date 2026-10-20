from __future__ import annotations

import psycopg
import pytest

from okg.substrate.generation_bootstrap import (
    formal_bootstrap_generation_inventory,
)


@pytest.mark.postgres
def test_formal_bootstrap_inventory_rejects_generation_and_branch_near_misses(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        canonical = formal_bootstrap_generation_inventory(conn)
        assert canonical == {
            "ok": True,
            "canonical": True,
            "generation_zero_count": 1,
            "non_bootstrap_count": 0,
        }

        conn.execute(
            "UPDATE okg.graph_generations "
            "SET invariant_summary = '{\"reason\":\"near miss\"}'::jsonb "
            "WHERE generation_id = 0"
        )
        assert formal_bootstrap_generation_inventory(conn)["ok"] is False
        conn.rollback()

        conn.execute(
            "UPDATE okg.branches SET role = 'experiment' "
            "WHERE branch_id = ("
            "SELECT branch_id FROM okg.graph_generations WHERE generation_id = 0"
            ")"
        )
        assert formal_bootstrap_generation_inventory(conn)["ok"] is False
        conn.rollback()
