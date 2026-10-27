"""Weighted whole-file partitioning for the substrate-db core-db-contracts step.

The step is one pytest selection in ``tests/qa_lanes.yaml``
(``substrate-db`` / ``core-db-contracts``). CI runs it as
:data:`DB_PARTITION_COUNT` jobs -- ``substrate-db-linux`` (shard 0),
``substrate-db-linux-2`` (shard 1) and ``substrate-db-linux-3`` (shard 2) --
and this plugin is the collection filter that gives each job its share. It is
the database sibling of ``okg.pytest_fast_partition`` and reuses its node-id
normalization, so both lanes name a file the same way.

THE SPLIT. Every collected test file is placed in exactly one shard, and a file
is never split, so ``--dist=loadscope`` and module fixtures behave as in the
unsplit step. Files are placed largest first, each into the shard with the
least estimated work so far (ties: the lower shard index; equal weights: the
path). A file's weight is its measured worker-seconds in the committed table
``tests/db_partition_weights.json`` (written by
``scripts/ci/db_partition_weights.py`` from a substrate-db job log); a file
the table does not name -- a test added since the measurement -- weighs the
table's median. The placement is computed from the COLLECTED file set, which
is identical in every shard and every xdist worker of one checkout, so all of
them derive the same assignment without talking to each other.

A table may also carry ``reserved_worker_seconds``, one number per shard: work
a shard does outside this split, which placement counts as already there. The
nightly substrate-db-full table reserves leg 0's time for the steps only that
leg runs (serial-db and the synth golden pins), so leg 0 takes fewer files.

A table may name the shard count it was balanced for as ``partition_count``;
a table that names none is balanced for :data:`DB_PARTITION_COUNT`. The hook
refuses any other count, so a shard can never read a table built for a
different split. The nightly coverage-full table names 5 (PACT
nightly-coverage-full-shards).

Why weights rather than the fast lane's keyed hash: on the 2026-10-06
measurement (12,944 worker-seconds over 273 files) the hash split's worst of
three shards is 43 minutes at the median seed and 49 at p90, against 36 for
the weighted split. A stale table degrades toward that, not below it: the
assignment stays complete and disjoint whatever the weights say.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Protocol

import pytest

from okg.pytest_fast_partition import normalize_nodeid
from okg.pytest_fast_partition import test_file as _file_of

DB_PARTITION_COUNT = 3
WEIGHTS_PATH = Path("tests/db_partition_weights.json")
WEIGHTS_SCHEMA = "okg.ci.db_partition_weights.v1"

class _CollectedItem(Protocol):
    nodeid: str


class PartitionWeightsError(ValueError):
    """The weights table is missing or malformed."""


def load_weights(path: Path) -> dict[str, float]:
    """The table's file -> worker-seconds map, refusing a malformed table."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PartitionWeightsError(f"unreadable weights table {path}: {error}") from error
    if not isinstance(document, dict) or document.get("schema") != WEIGHTS_SCHEMA:
        raise PartitionWeightsError(
            f"weights table {path} must declare schema {WEIGHTS_SCHEMA!r}"
        )
    files = document.get("files")
    if not isinstance(files, dict) or not files:
        raise PartitionWeightsError(f"weights table {path} names no files")
    weights: dict[str, float] = {}
    for name, seconds in files.items():
        if (
            not isinstance(name, str)
            or isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or seconds <= 0
        ):
            raise PartitionWeightsError(
                f"weights table {path}: {name!r} needs a positive number of seconds"
            )
        normalized = _file_of(name)
        if normalized in weights:
            raise PartitionWeightsError(f"weights table {path} names {normalized} twice")
        weights[normalized] = float(seconds)
    return weights


def load_partition_count(path: Path) -> int:
    """The shard count the table was balanced for; DB_PARTITION_COUNT when unnamed."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PartitionWeightsError(f"unreadable weights table {path}: {error}") from error
    count = document.get("partition_count") if isinstance(document, dict) else None
    if count is None:
        return DB_PARTITION_COUNT
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise PartitionWeightsError(
            f"weights table {path}: partition_count must be a positive integer"
        )
    return count


def load_reserved(path: Path, count: int) -> list[float]:
    """The table's per-shard reserved worker-seconds; zeros when it names none."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise PartitionWeightsError(f"unreadable weights table {path}: {error}") from error
    reserved = document.get("reserved_worker_seconds") if isinstance(document, dict) else None
    if reserved is None:
        return [0.0] * count
    if (
        not isinstance(reserved, list)
        or len(reserved) != count
        or any(
            isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0
            for value in reserved
        )
    ):
        raise PartitionWeightsError(
            f"weights table {path}: reserved_worker_seconds must list {count} "
            "non-negative numbers"
        )
    return [float(value) for value in reserved]


def assign_files(
    files: Iterable[str],
    weights: Mapping[str, float],
    count: int,
    reserved: Sequence[float] | None = None,
) -> dict[str, int]:
    """file -> shard index for every distinct file, by largest-first placement."""

    if count < 1:
        raise ValueError(f"partition count must be positive, got {count}")
    if not weights:
        raise ValueError("weights must name at least one file")
    if reserved is not None and len(reserved) != count:
        raise ValueError(f"reserved must name {count} shards, got {len(reserved)}")
    fallback = statistics.median(weights.values())
    distinct = {_file_of(name) for name in files}
    order = sorted(distinct, key=lambda name: (-weights.get(name, fallback), name))
    loads = [float(value) for value in reserved] if reserved is not None else [0.0] * count
    owner: dict[str, int] = {}
    for name in order:
        shard = min(range(count), key=lambda index: (loads[index], index))
        owner[name] = shard
        loads[shard] += weights.get(name, fallback)
    return owner


def shard_loads(
    files: Iterable[str],
    weights: Mapping[str, float],
    count: int,
    reserved: Sequence[float] | None = None,
) -> list[float]:
    """Estimated worker-seconds per shard for ``files``, reserved work included."""

    fallback = statistics.median(weights.values())
    loads = [float(value) for value in reserved] if reserved is not None else [0.0] * count
    for name, shard in assign_files(files, weights, count, reserved).items():
        loads[shard] += weights.get(name, fallback)
    return loads


def partition_nodeids(
    nodeids: Iterable[str], weights: Mapping[str, float], count: int
) -> tuple[frozenset[str], ...]:
    """Split normalized node ids into ``count`` shards, refusing duplicates."""

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in nodeids:
        nodeid = normalize_nodeid(raw)
        if nodeid in seen:
            raise ValueError(f"duplicate normalized node id: {nodeid}")
        seen.add(nodeid)
        normalized.append(nodeid)
    owner = assign_files(normalized, weights, count)
    shards: list[set[str]] = [set() for _ in range(count)]
    for nodeid in normalized:
        shards[owner[_file_of(nodeid)]].add(nodeid)
    return tuple(frozenset(shard) for shard in shards)


def owned_entries(
    entries: Sequence[str],
    weights: Mapping[str, float],
    count: int,
    index: int,
    reserved: Sequence[float] | None = None,
) -> list[str]:
    """The step path entries (``file`` or ``file::node``) shard ``index`` owns.

    The placement is :func:`assign_files` over the files the entries name, so
    it is the same function the collection hook applies. A runner that passes
    only these entries to pytest skips collecting the other shards' files.
    """

    owner = assign_files((_file_of(entry) for entry in entries), weights, count, reserved)
    return [entry for entry in entries if owner[_file_of(entry)] == index]


def universe_option(entries: Iterable[str]) -> str:
    """The value of ``--okg-db-partition-universe`` for a step's path entries."""

    return ",".join(sorted({_file_of(entry) for entry in entries}))


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("okg-db-partition")
    group.addoption("--okg-db-partition-index", type=int, default=None)
    group.addoption("--okg-db-partition-count", type=int, default=None)
    group.addoption(
        "--okg-db-partition-universe",
        default=None,
        help=(
            "Comma-separated files of the WHOLE step. Given when the runner passes "
            "pytest only this shard's files: placement is computed over this list "
            "(the same on every shard) instead of over the files collected here."
        ),
    )
    group.addoption(
        "--okg-db-partition-weights",
        default=None,
        help=f"Weights table (default: <rootdir>/{WEIGHTS_PATH}).",
    )


def _table_path(config: pytest.Config) -> Path:
    configured = config.getoption("--okg-db-partition-weights")
    return Path(configured) if configured else Path(config.rootpath) / WEIGHTS_PATH


def _partition_from_config(config: pytest.Config) -> tuple[int, int]:
    index = config.getoption("--okg-db-partition-index")
    count = config.getoption("--okg-db-partition-count")
    if index is None or count is None:
        raise pytest.UsageError(
            "both --okg-db-partition-index and --okg-db-partition-count are required"
        )
    path = _table_path(config)
    try:
        expected = load_partition_count(path)
    except PartitionWeightsError as error:
        raise pytest.UsageError(str(error)) from error
    if count != expected:
        raise pytest.UsageError(
            f"db partition count must be {expected} for {path.name}, got {count}"
        )
    if not 0 <= index < count:
        raise pytest.UsageError(f"db partition index must be in [0, {count})")
    return index, count


def _weights_from_config(
    config: pytest.Config, count: int
) -> tuple[dict[str, float], list[float]]:
    path = _table_path(config)
    try:
        return load_weights(path), load_reserved(path, count)
    except PartitionWeightsError as error:
        raise pytest.UsageError(str(error)) from error


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[_CollectedItem],
) -> None:
    """Keep only the files this shard owns."""

    index, count = _partition_from_config(config)
    weights, reserved = _weights_from_config(config, count)
    root = Path(config.rootpath)
    universe = config.getoption("--okg-db-partition-universe")
    if universe:
        # The runner passed only this shard's files, so the collected set is a
        # third of the step; place over the whole step the runner named.
        owner = assign_files(
            (name for name in universe.split(",") if name), weights, count, reserved
        )
        stray = sorted(
            {_file_of(item.nodeid, root=root) for item in items}.difference(owner)
        )
        if stray:
            raise pytest.UsageError(
                "collected files outside --okg-db-partition-universe: " + ", ".join(stray)
            )
    else:
        owner = assign_files(
            (_file_of(item.nodeid, root=root) for item in items), weights, count, reserved
        )
    selected: list[_CollectedItem] = []
    deselected: list[_CollectedItem] = []
    for item in items:
        target = selected if owner[_file_of(item.nodeid, root=root)] == index else deselected
        target.append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
