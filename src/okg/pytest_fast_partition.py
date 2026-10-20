"""Deterministic whole-file partitioning for the required fast CI lane.

The partition is deliberately a pytest collection filter instead of a second
selector registry.  Every unit starts from the canonical ``fast`` selector in
``tests/qa_lanes.yaml``; this plugin only assigns each collected test file to
one of the five required units.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Protocol

import pytest


FAST_PARTITION_COUNT = 5
FAST_PARTITION_SEED = 1945
_HASH_KEY = FAST_PARTITION_SEED.to_bytes(8, byteorder="big", signed=False)


class _CollectedItem(Protocol):
    nodeid: str


def normalize_nodeid(nodeid: str, *, root: Path | None = None) -> str:
    """Return a stable slash-separated node id without a checkout prefix."""

    normalized = nodeid.replace("\\", "/")
    file_part, separator, test_part = normalized.partition("::")
    path = Path(file_part)
    if path.is_absolute():
        base = (root or Path.cwd()).resolve()
        try:
            path = path.resolve().relative_to(base)
        except ValueError:
            path = path.resolve()
    file_part = path.as_posix()
    while file_part.startswith("./"):
        file_part = file_part[2:]
    return f"{file_part}{separator}{test_part}" if separator else file_part


def test_file(nodeid: str, *, root: Path | None = None) -> str:
    """Return the normalized file that owns ``nodeid``."""

    return normalize_nodeid(nodeid, root=root).split("::", 1)[0]


def partition_owner(nodeid: str, *, root: Path | None = None) -> int:
    """Assign a node id to one of five units using keyed BLAKE2b.

    Only the file path is hashed.  This is the contract that prevents tests in
    one file (including parametrized cases) from being split across jobs.
    """

    digest = hashlib.blake2b(
        test_file(nodeid, root=root).encode("utf-8"),
        key=_HASH_KEY,
        digest_size=8,
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) % FAST_PARTITION_COUNT


def partition_nodeids(nodeids: Iterable[str]) -> tuple[frozenset[str], ...]:
    """Partition normalized node ids with duplicate input rejected."""

    units: list[set[str]] = [set() for _ in range(FAST_PARTITION_COUNT)]
    seen: set[str] = set()
    for raw_nodeid in nodeids:
        nodeid = normalize_nodeid(raw_nodeid)
        if nodeid in seen:
            raise ValueError(f"duplicate normalized node id: {nodeid}")
        seen.add(nodeid)
        units[partition_owner(nodeid)].add(nodeid)
    return tuple(frozenset(unit) for unit in units)


def assert_selection_equivalence(
    baseline: Iterable[str],
    successors: Sequence[Iterable[str]],
) -> dict[str, int]:
    """Prove exact union, disjointness, and non-emptiness for five units."""

    if len(successors) != FAST_PARTITION_COUNT:
        raise ValueError(
            f"expected {FAST_PARTITION_COUNT} successor units, got {len(successors)}"
        )
    baseline_items = [normalize_nodeid(nodeid) for nodeid in baseline]
    baseline_set = set(baseline_items)
    if len(baseline_items) != len(baseline_set):
        raise ValueError("canonical fast selection contains duplicate node ids")
    if not baseline_set:
        raise ValueError("canonical fast selection is empty")
    normalized_units: list[set[str]] = []
    for index, successor in enumerate(successors):
        items = [normalize_nodeid(nodeid) for nodeid in successor]
        unit = set(items)
        if len(items) != len(unit):
            raise ValueError(f"successor unit {index} contains duplicate node ids")
        normalized_units.append(unit)
    empty = [str(index) for index, unit in enumerate(normalized_units) if not unit]
    if empty:
        raise ValueError(f"empty successor units: {', '.join(empty)}")
    overlaps: set[str] = set()
    for index, unit in enumerate(normalized_units):
        for other in normalized_units[index + 1 :]:
            overlaps.update(unit & other)
    if overlaps:
        raise ValueError("successor units overlap: " + ", ".join(sorted(overlaps)[:10]))
    successor_union = set().union(*normalized_units)
    if successor_union != baseline_set:
        missing = sorted(baseline_set - successor_union)
        unexpected = sorted(successor_union - baseline_set)
        raise ValueError(
            "successor union differs from canonical fast selection: "
            f"missing={missing[:10]!r}, unexpected={unexpected[:10]!r}"
        )
    return {
        "baseline": len(baseline_set),
        **{
            f"partition_{index}": len(unit)
            for index, unit in enumerate(normalized_units)
        },
    }


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("okg-fast-partition")
    group.addoption("--okg-fast-partition-index", type=int, default=None)
    group.addoption("--okg-fast-partition-count", type=int, default=None)


def _partition_from_config(config: pytest.Config) -> int:
    index = config.getoption("--okg-fast-partition-index")
    count = config.getoption("--okg-fast-partition-count")
    if index is None or count is None:
        raise pytest.UsageError(
            "both --okg-fast-partition-index and --okg-fast-partition-count "
            "are required"
        )
    if count != FAST_PARTITION_COUNT:
        raise pytest.UsageError(
            f"fast partition count must be {FAST_PARTITION_COUNT}, got {count}"
        )
    if not 0 <= index < count:
        raise pytest.UsageError(f"fast partition index must be in [0, {count})")
    return index


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[_CollectedItem],
) -> None:
    """Keep only the files owned by the requested required unit."""

    index = _partition_from_config(config)
    selected: list[_CollectedItem] = []
    deselected: list[_CollectedItem] = []
    for item in items:
        target = selected if partition_owner(item.nodeid) == index else deselected
        target.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
