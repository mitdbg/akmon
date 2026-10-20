"""Guards for the chronos delta-segment interval index column order.

PACT okg-workspace-recovery-tail / req.delta-index-order. The per-publish
delta-count scan filters selectively on ``live_lo >= s.live_lo`` (current-segment
writes), so the ``node_delta_segment`` / ``edge_delta_segment`` indexes must lead
with ``live_lo``. A ``live_hi``-leading order forced a near-full-index scan
(EXPLAIN-verified 3173ms -> 309ms on okg-workspace). Because the index name is
suffix-derived (column-independent) and created ``IF NOT EXISTS``, the spec change
only takes effect once the stale index is dropped -- the self-heal in
``_ensure_interval_source_read_indexes`` does that on the next attach.
"""

from okg.substrate.graph_store import (
    _index_matches_columns,
)
from okg.substrate.db.cold_history import INTERVAL_SOURCE_READ_INDEX_SPECS

EDGE_DESIRED = ("live_lo", "live_hi", "edge_id", "deleted")
NODE_DESIRED = ("live_lo", "live_hi", "node_id", "deleted")


def _indexdef(cols: str, *, partial: bool = False) -> str:
    where = " WHERE (deleted = false)" if partial else ""
    return (
        "CREATE INDEX idx_okg_chronos_abc123_edge_delta_segment "
        "ON public._chronos_b_interval_okg_graph_edges_x "
        f"USING btree ({cols}){where}"
    )


def test_old_order_reports_mismatch_so_it_is_rebuilt():
    # The pre-fix (live_hi, live_lo, ...) order must report False -> drop+rebuild.
    assert (
        _index_matches_columns(_indexdef("live_hi, live_lo, edge_id, deleted"), EDGE_DESIRED)
        is False
    )


def test_corrected_order_matches_so_no_churn():
    # Once rebuilt with the correct order, it must report True -> no rebuild.
    assert (
        _index_matches_columns(_indexdef("live_lo, live_hi, edge_id, deleted"), EDGE_DESIRED)
        is True
    )


def test_match_handles_quoted_idents_and_sort_qualifiers():
    assert (
        _index_matches_columns(
            _indexdef('"live_lo" DESC, "live_hi", edge_id, deleted'), EDGE_DESIRED
        )
        is True
    )


def test_partial_index_where_clause_is_ignored():
    assert (
        _index_matches_columns(
            _indexdef("live_lo, live_hi, edge_id, deleted", partial=True), EDGE_DESIRED
        )
        is True
    )


def test_absent_or_unparseable_returns_none():
    assert _index_matches_columns(None, EDGE_DESIRED) is None
    assert _index_matches_columns("", EDGE_DESIRED) is None
    assert _index_matches_columns("garbage with no parens", EDGE_DESIRED) is None


def test_specs_lead_with_live_lo_regression_guard():
    # The cold-history module is the canonical authority consumed by both
    # ensure-schema and online index rotation.  Assert its structured specs
    # directly so moving that authority does not weaken this regression guard.
    node_spec = next(
        spec
        for spec in INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_nodes"]
        if spec.suffix == "node_delta_segment"
    )
    edge_spec = next(
        spec
        for spec in INTERVAL_SOURCE_READ_INDEX_SPECS["okg.graph_edges"]
        if spec.suffix == "edge_delta_segment"
    )
    assert node_spec.columns == NODE_DESIRED
    assert edge_spec.columns == EDGE_DESIRED
    assert node_spec.rotate is False
    assert edge_spec.rotate is False
