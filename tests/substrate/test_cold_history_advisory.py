from __future__ import annotations

from okg.substrate.db import cold_history


ADVISORY_KEYS = {
    "applicable",
    "status",
    "read_only",
    "rotation_performed",
    "headroom_generations",
    "floor",
    "rotatable_index_count",
    "rotatable_full_index_bytes",
    "estimate_kind",
    "rotatable_indexes",
    "inspect_command",
    "activate_command",
    "warning",
    "error",
}


def _full_index(name: str, byte_count: int) -> dict[str, object]:
    return {
        "index_name": name,
        "bytes": byte_count,
        "indisvalid": True,
        "indisready": True,
    }


def _index(
    *,
    table: str,
    suffix: str,
    rotatable: bool,
    full_index: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "logical_table": table,
        "physical_table": f"physical_{table.rsplit('.', 1)[-1]}",
        "suffix": suffix,
        "rotatable": rotatable,
        "full_index": full_index,
    }


def test_summary_filters_and_sums_only_existing_rotatable_full_indexes() -> None:
    report = {
        "floor": {"floor": 1234, "headroom_generations": 100},
        "indexes": [
            _index(
                table="okg.graph_edges",
                suffix="edge_source_run_visible",
                rotatable=True,
                full_index=_full_index("edge_source_run", 70),
            ),
            _index(
                table="okg.graph_nodes",
                suffix="node_id_visible",
                rotatable=True,
                full_index=_full_index("node_id", 30),
            ),
            _index(
                table="okg.graph_edges",
                suffix="edge_delta_segment",
                rotatable=False,
                full_index=_full_index("edge_delta", 999),
            ),
            _index(
                table="okg.graph_nodes",
                suffix="node_live_hi_visible",
                rotatable=True,
                full_index=None,
            ),
        ],
        "support_indexes": [{"index": {"bytes": 888}}],
    }

    advisory = cold_history.summarize_cold_history_reclaim(
        report,
        deployment="deployment with spaces",
    )

    assert set(advisory) == ADVISORY_KEYS
    assert advisory["status"] == "candidates"
    assert advisory["rotatable_index_count"] == 2
    assert advisory["rotatable_full_index_bytes"] == 100
    assert [row["full_index_name"] for row in advisory["rotatable_indexes"]] == [
        "edge_source_run",
        "node_id",
    ]
    assert advisory["estimate_kind"] == (
        "gross_full_index_bytes_before_hot_replacement"
    )
    assert advisory["inspect_command"] == (
        "uv run okg db cold-history --deployment "
        "'deployment with spaces' --json"
    )
    assert advisory["activate_command"] == (
        "uv run okg db cold-history --deployment "
        "'deployment with spaces' --activate --confirm --apply --json"
    )
    assert advisory["rotation_performed"] is False
    assert "net free space is lower" in advisory["warning"]


def test_summary_reports_no_candidates_without_inventing_unavailable() -> None:
    advisory = cold_history.summarize_cold_history_reclaim(
        {
            "floor": {"floor": 5678, "headroom_generations": 100},
            "indexes": [
                _index(
                    table="okg.graph_edges",
                    suffix="edge_delta_segment",
                    rotatable=False,
                    full_index=_full_index("edge_delta", 999),
                ),
                _index(
                    table="okg.graph_nodes",
                    suffix="node_id_visible",
                    rotatable=True,
                    full_index=None,
                ),
            ],
        },
        deployment="dep",
    )

    assert set(advisory) == ADVISORY_KEYS
    assert advisory["status"] == "no_candidates"
    assert advisory["floor"] == 5678
    assert advisory["rotatable_index_count"] == 0
    assert advisory["rotatable_full_index_bytes"] == 0
    assert advisory["rotatable_indexes"] == []
    assert advisory["error"] is None


def test_planner_failure_is_explicit_and_never_invents_zero(
    monkeypatch,
) -> None:
    def fail(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("catalog read failed")

    monkeypatch.setattr(cold_history, "cold_history_report", fail)

    advisory = cold_history.cold_history_reclaim_advisory(
        "postgres://unused",
        deployment="dep",
    )

    assert set(advisory) == ADVISORY_KEYS
    assert advisory["status"] == "unavailable"
    assert advisory["floor"] is None
    assert advisory["rotatable_index_count"] is None
    assert advisory["rotatable_full_index_bytes"] is None
    assert advisory["rotatable_indexes"] is None
    assert advisory["error"] == {
        "type": "RuntimeError",
        "message": "catalog read failed",
    }
    assert advisory["inspect_command"].endswith("--deployment dep --json")
