"""The cleanup frontier is recorded as a count and a digest (issue #2718).

The derived-node cleanup plan lands in every checkpoint's metadata. It used to
carry the whole frontier id list -- about 788,000 ids (62 MB) on
okg-workspace -- so every metadata reader paid for it. These tests pin the
replacement: a fixed-size count and content digest, a digest that changes
when the frontier changes, and a reader that still understands old
checkpoints that carry the list.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from okg.substrate.consistency.observations import (
    SCOPE_DIGEST_ALGORITHM,
    scope_digest,
)
from okg.substrate.derived_node_liveness import (
    frontier_digest,
    plan_derived_node_cleanup,
    read_frontier_summary,
)
from okg.substrate.errors import SubstrateError


def _source_rows(count: int, *, prefix: str = "n") -> list[dict[str, Any]]:
    return [
        {
            "node_id": f"{prefix}:{index:08d}",
            "subtype": "thing",
            "source": "source:fixture",
            "source_record_id": {"id": index},
        }
        for index in range(count)
    ]


def _plan_bytes(plan: dict[str, Any]) -> int:
    return len(json.dumps(plan, sort_keys=True).encode("utf-8"))


def test_plan_size_does_not_grow_with_the_frontier() -> None:
    """Ten ids or fifty thousand, the plan is the same size."""
    small = plan_derived_node_cleanup(_source_rows(10), [])
    large = plan_derived_node_cleanup(_source_rows(50_000), [])

    assert "frontier_node_ids" not in small
    assert "frontier_node_ids" not in large
    assert small["frontier_node_count"] == 10
    assert large["frontier_node_count"] == 50_000
    # Only the count's digits may differ (2 vs 5 characters).
    assert abs(_plan_bytes(large) - _plan_bytes(small)) <= 8
    assert _plan_bytes(large) < 1024


def test_digest_names_the_frontier_content() -> None:
    rows = _source_rows(3)
    ids = [row["node_id"] for row in rows]
    plan = plan_derived_node_cleanup(rows, [])

    assert plan["frontier_node_digest_sha256"] == frontier_digest(ids)
    # Order and repeats do not change it; one different id does.
    assert frontier_digest(reversed(ids + ids)) == frontier_digest(ids)
    assert frontier_digest(ids[:2] + ["n:other"]) != frontier_digest(ids)
    assert frontier_digest(ids[:2]) != frontier_digest(ids)
    # Ids are newline-joined, not length-prefixed: splitting an id with no
    # newline in it gives a different frontier. An id that itself contains a
    # newline could collide with two ids; scope_digest is shared with the
    # gate, so it keeps that encoding.
    assert frontier_digest(["ab"]) != frontier_digest(["a", "b"])
    # The same digest the gate's scope_identity records for a node set.
    assert plan["frontier_node_digest_sha256"] == scope_digest(sorted(ids))
    assert plan["frontier_node_digest_algorithm"] == SCOPE_DIGEST_ALGORITHM


def test_a_caller_supplied_frontier_changes_the_digest() -> None:
    rows = _source_rows(4)
    same = plan_derived_node_cleanup(rows, rows, frontier_node_ids={"n:00000001"})
    other = plan_derived_node_cleanup(rows, rows, frontier_node_ids={"n:00000002"})

    assert same["frontier_node_count"] == other["frontier_node_count"] == 1
    assert same["frontier_node_digest_sha256"] != other["frontier_node_digest_sha256"]


def test_reader_accepts_a_new_record() -> None:
    plan = plan_derived_node_cleanup(_source_rows(5), [])

    # The shape changed (a field removed), so the version moved.
    assert plan["schema"] == "okg.derived_node_cleanup_plan.v2"
    assert read_frontier_summary(plan) == {
        "frontier_node_count": 5,
        "frontier_node_digest_sha256": plan["frontier_node_digest_sha256"],
        "basis": "recorded",
    }


def test_reader_accepts_an_old_checkpoint_that_carries_the_list() -> None:
    ids = [row["node_id"] for row in _source_rows(5)]
    legacy = {"schema": "okg.derived_node_cleanup.v1", "frontier_node_ids": ids}
    new = plan_derived_node_cleanup(_source_rows(5), [])

    summary = read_frontier_summary(legacy)

    assert summary == {
        "frontier_node_count": 5,
        "frontier_node_digest_sha256": new["frontier_node_digest_sha256"],
        "basis": "legacy_id_list",
    }


def test_reader_returns_none_when_no_frontier_was_recorded() -> None:
    assert read_frontier_summary({"schema": "okg.derived_node_cleanup.v1"}) is None


_OTHER_DIGEST = "0" * 64


@pytest.mark.parametrize(
    "record",
    [
        {"frontier_node_ids": "n:1"},
        {"frontier_node_ids": ["n:1"], "frontier_node_count": 2},
        {"frontier_node_ids": ["n:1"], "frontier_node_digest_sha256": _OTHER_DIGEST},
        {"frontier_node_count": 1},
        {"frontier_node_digest_sha256": _OTHER_DIGEST},
        {"frontier_node_count": -1, "frontier_node_digest_sha256": _OTHER_DIGEST},
        {"frontier_node_count": True, "frontier_node_digest_sha256": _OTHER_DIGEST},
        {"frontier_node_count": 1, "frontier_node_digest_sha256": "sha256:" + "0" * 57},
        {"frontier_node_count": 1, "frontier_node_digest_sha256": "A" * 64},
    ],
)
def test_reader_refuses_a_malformed_record(record: dict[str, Any]) -> None:
    with pytest.raises(SubstrateError) as caught:
        read_frontier_summary(record)
    assert caught.value.data == {"reason": "derived_cleanup_frontier_malformed"}
