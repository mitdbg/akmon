"""Honest reads for Chronos-registered maintained-derivation kept tables (#706).

okg.graph_nodes / okg.graph_edges are guarded by okg.require_graph_checkout:
a bare SQL read of the never-materialized base table raises instead of
returning a deceptive zero. The maintained-derivation kept tables
okg.claim_effective and okg.claim_projection are registered on the SAME
Chronos branch-table path but had NO guard, NO honest-read view, and NO
doctor check -- a live read returned 0 where a checkout served 81,165 rows.

These regressions register a kept table, seed real Chronos branch/checkout
state, then read the vestigial base surface and assert a loud refusal
(okg.v_current_<table> RAISES ObjectNotInPrerequisiteState) instead of a
confident zero. On the pre-fix code the honest-read view does not exist, so
the bare read silently returns 0 and these tests fail.
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

# Warm the consistency package before importing claim_effective_derivation.
# dev's module graph has a circular edge: claim_effective_derivation ->
# consistency.claim_model.effective -> consistency/__init__ -> reuse_scheduler
# -> possible_effects -> claim_effective_derivation.CLAIM_SUBTYPE. Importing
# claim_effective_derivation as the FIRST module (as this isolated test module
# does) trips the cycle; importing the consistency package first initializes it
# so CLAIM_SUBTYPE resolves. The full suite masks this by importing consistency
# earlier (see tests/substrate/test_claim_effective_derivation.py).
import okg.substrate.consistency as _consistency_warmup

from okg.substrate.db.management import (
    honest_read_reconciliation_report,
    honest_read_view_name,
    honest_read_view_sql,
)
from okg.substrate.derived_artifacts.claim_effective_derivation import (
    CLAIM_EFFECTIVE_TABLE,
    claim_effective_derivation_row,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    CLAIM_PROJECTION_TABLE,
    claim_projection_derivation_row,
)
from okg.substrate.graph_store import ChronosGraphStore

assert _consistency_warmup is not None  # import-order warmup is intentional

pytestmark = pytest.mark.postgres

_SEEDED_ROWS = 5


def _storage_for(derivation_row: Any) -> Any:
    return derivation_row().materializations[0].storage


def _seed_kept_table(
    dsn: str,
    *,
    storage: Any,
    table: str,
    deployment: str,
) -> str:
    """Register ``table`` with Chronos, upsert ``_SEEDED_ROWS`` rows through a
    checkout, checkpoint a published generation, and return its id. The base
    table stays a never-materialized template (0 rows) by construction."""
    store = ChronosGraphStore.connect(dsn)
    try:
        storage.ensure_registered(store)
        session = store.checkout("default")
        rows = [
            {"row_kind": "finding", "row_key": f"k{i}", "item_cadence": 1}
            for i in range(_SEEDED_ROWS)
        ]
        with session.transaction():
            session.upsert_rows(table, rows)
        generation = store.checkpoint_generation(
            f"gen-{deployment}", deployment_name=deployment
        )
        checkout = store.checkout_generation(generation)
        served = int(
            checkout.query(f"SELECT count(*) AS n FROM {table}")[0]["n"]
        )
        assert served == _SEEDED_ROWS
        return str(generation)
    finally:
        store.close()


@pytest.mark.parametrize(
    ("derivation_row", "table"),
    [
        (claim_effective_derivation_row, CLAIM_EFFECTIVE_TABLE),
        (claim_projection_derivation_row, CLAIM_PROJECTION_TABLE),
    ],
)
def test_vestigial_base_read_refuses_instead_of_confident_zero(
    clean_runtime: str,
    derivation_row: Any,
    table: str,
) -> None:
    storage = _storage_for(derivation_row)
    _seed_kept_table(
        clean_runtime,
        storage=storage,
        table=table,
        deployment="honest-reads-refuse",
    )
    view = honest_read_view_name(table)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        # app_ro default-deny invariant: the lazily-created honest-read view MUST
        # carry the app_ro SELECT grant so MCP run_sql (downgraded to app_ro)
        # reaches the honest RAISE, not permission-denied. Pin it here so a
        # regression fails in-file instead of flaking the app_ro census suite
        # (tests/sql/roles/{test_app_ro_default_deny.py,app_ro_read_only.sql}).
        cur.execute(
            "SELECT has_table_privilege('app_ro', %s, 'SELECT')", (view,)
        )
        assert cur.fetchone()[0] is True, (
            f"honest-read view {view} is not app_ro-readable"
        )

        # The #706 trap: the bare base surface is a never-materialized
        # template, so it silently reports zero even though a checkout serves
        # _SEEDED_ROWS rows.
        cur.execute(f"SELECT count(*) FROM {table}")
        assert int(cur.fetchone()[0]) == 0

        # The honest-read view must REFUSE (raise), not serve that deceptive
        # zero. Pre-fix this view does not exist, so this read fails loudly
        # with UndefinedTable rather than the honest-refusal error code.
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState) as exc:
            cur.execute(f"SELECT count(*) FROM {view}")
        assert "requires a Chronos checkout" in str(exc.value)

        # The refusal did not poison the connection.
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


def test_doctor_reconciliation_green_when_views_refuse(
    clean_runtime: str,
) -> None:
    storage = _storage_for(claim_effective_derivation_row)
    _seed_kept_table(
        clean_runtime,
        storage=storage,
        table=CLAIM_EFFECTIVE_TABLE,
        deployment="honest-reads-doctor",
    )

    report = honest_read_reconciliation_report(
        clean_runtime, deployment_name="honest-reads-doctor"
    )

    assert report["ok"] is True
    assert report["governed_tables"] >= 1
    entry = next(
        e for e in report["reconciled"] if e["table"] == CLAIM_EFFECTIVE_TABLE
    )
    assert entry["base_count"] == 0
    assert entry["checkout_count"] == _SEEDED_ROWS
    assert entry["view_refused"] is True
    assert report["findings"] == []


def test_doctor_reconciliation_red_on_unrefusing_vestigial_base(
    clean_runtime: str,
) -> None:
    """Re-introduce the #706 wrong-lens: replace the honest-read view with a
    naive one that serves the base table's confident zero instead of refusing.
    The reconciliation must go red because base=0 diverges from checkout=5."""
    storage = _storage_for(claim_effective_derivation_row)
    _seed_kept_table(
        clean_runtime,
        storage=storage,
        table=CLAIM_EFFECTIVE_TABLE,
        deployment="honest-reads-doctor-red",
    )
    view = honest_read_view_name(CLAIM_EFFECTIVE_TABLE)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"CREATE OR REPLACE VIEW {view} AS "
            f"SELECT * FROM {CLAIM_EFFECTIVE_TABLE}"
        )
        try:
            report = honest_read_reconciliation_report(
                clean_runtime, deployment_name="honest-reads-doctor-red"
            )
        finally:
            # Restore the refusing honest-read view so the naive one does not
            # leak into the shared test database.
            cur.execute(honest_read_view_sql(CLAIM_EFFECTIVE_TABLE))

    assert report["ok"] is False
    finding = next(
        f for f in report["findings"] if f["table"] == CLAIM_EFFECTIVE_TABLE
    )
    assert finding["reason"] == "vestigial_base_not_refusing"
    assert finding["base_count"] == 0
    assert finding["view_count"] == 0
    assert finding["checkout_count"] == _SEEDED_ROWS
