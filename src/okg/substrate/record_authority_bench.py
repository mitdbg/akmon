"""Stage-and-seal microbenchmark for the indexed-v2 record authority.

The batched write path (`ProductionAuthorityStore._flush_batch` and its binary
COPYs) was measured once by hand while it was being written.  This module makes
that measurement reproducible: it generates a deterministic synthetic source of
N records, stages and seals it through the real store against a migrated
Postgres, and reports wall time, throughput, a lower bound on database round
trips and the resulting row counts as JSON.

The harness only writes; it performs no network or model calls.  Its target DSN
must already carry the substrate schema (migration 0031 or later) and should be
a throwaway database — an authority write is immutable by trigger and is not
cleaned up here.

Run it as::

    python -m okg.substrate.record_authority_bench \\
        --dsn postgres://postgres:okg@127.0.0.1:5433/okg_bench --records 20000
"""

from __future__ import annotations

import argparse
import json
import platform
import random
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import Any


from okg.substrate.db.connection import connect
from okg.substrate.ingest.record_authority import (
    ProductionAuthorityStore,
    StreamingRecord,
    legacy_record_set_digest,
)
from okg.substrate.library.sources.base import (
    EdgeKey,
    RecordAuthorityOperation,
    RecordAuthorityStream,
    RecordNode,
)

BENCH_SCHEMA = "okg.record_authority.bench.v1"

#: Authority-scoped tables the harness counts after a seal, in report order.
#: The payload tables are content-addressed and carry no authority column, so
#: they are reported separately as the payloads this authority references.
COUNTED_TABLES = (
    "okg.source_record_authority_operations",
    "okg.source_record_authority_records",
    "okg.source_record_authority_components",
    "okg.source_record_authority_component_fields",
    "okg.source_record_authority_identities",
)

_WORDS = (
    "authority", "batch", "chronos", "digest", "edge", "fact", "generation",
    "identity", "ingest", "ledger", "narrowing", "ordinal", "payload",
    "publish", "record", "revision", "seal", "source", "substrate", "watermark",
)


@dataclass(frozen=True)
class BenchShape:
    """Per-record component shape.

    The defaults approximate a mid-sized library source: one primary node with
    a prose-bearing revision payload, a couple of descendant nodes and a few
    edges.  ``body_bytes`` is the approximate size of the primary node's
    revision text, which dominates the payload bytes the store has to COPY.
    """

    descendants: int = 2
    edges: int = 3
    body_bytes: int = 384

    def components_per_record(self) -> int:
        return 1 + self.descendants + self.edges

    def to_json(self) -> dict[str, int]:
        return {
            "descendants": self.descendants,
            "edges": self.edges,
            "body_bytes": self.body_bytes,
        }


def _body(rng: random.Random, size: int) -> str:
    """Deterministic prose of roughly ``size`` bytes."""

    if size <= 0:
        return ""
    parts: list[str] = []
    length = 0
    while length < size:
        word = _WORDS[rng.randrange(len(_WORDS))]
        parts.append(word)
        length += len(word) + 1
    return " ".join(parts)[:size]


def _record_id(index: int, width: int) -> str:
    return "bench-record-%0*d" % (width, index)


def _emission_cursors(
    index: int, shape: BenchShape, seed: int, width: int
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build one record's component cursors.

    The RNG is seeded per record, so any record can be regenerated on its own
    and the two passes the harness makes (digest, then stage) agree byte for
    byte.
    """

    rng = random.Random("%d:%d" % (seed, index))
    record_id = _record_id(index, width)
    revision = {
        "body": _body(rng, shape.body_bytes),
        "checksum": "%016x" % rng.getrandbits(64),
        "observed_at": "2026-01-%02dT%02d:%02d:00Z"
        % (1 + index % 28, index % 24, index % 60),
        "revision": index,
    }
    source_record_id = {"index": index, "table": "bench_records"}
    primary = RecordNode(
        record_id, "dataset", source_record_id, revision,
    ).to_cursor()
    descendants = [
        RecordNode(
            "%s-child-%d" % (record_id, ordinal),
            "dataset",
            {"index": index, "ordinal": ordinal, "table": "bench_children"},
            {"label": _body(rng, 48), "revision": index},
        ).to_cursor()
        for ordinal in range(shape.descendants)
    ]
    edges = [
        EdgeKey(
            record_id,
            "%s-child-%d" % (record_id, ordinal % max(shape.descendants, 1)),
            "contains",
            {"kind": "child", "ordinal": ordinal},
            source_record_id=source_record_id,
            source_revision={"revision": index},
        ).to_cursor()
        for ordinal in range(shape.edges)
    ]
    return primary, descendants, edges


def iter_streaming_records(
    count: int, shape: BenchShape, seed: int
) -> Iterator[StreamingRecord]:
    """Yield the same records the stage pass stages, for the legacy digest."""

    width = _id_width(count)
    for index in range(count):
        primary, descendants, edges = _emission_cursors(index, shape, seed, width)
        yield StreamingRecord(
            record_id=_record_id(index, width),
            primary=primary,
            descendants=descendants,
            edges=edges,
        )


def iter_operations(
    count: int, shape: BenchShape, seed: int
) -> Iterator[RecordAuthorityOperation]:
    """Yield the ordered upsert operations for the synthetic source."""

    width = _id_width(count)
    for index in range(count):
        primary, descendants, edges = _emission_cursors(index, shape, seed, width)
        yield RecordAuthorityOperation(
            ordinal=index,
            record_id=_record_id(index, width),
            primary=primary,
            descendants=tuple(descendants),
            edges=tuple(edges),
        )


def _id_width(count: int) -> int:
    return max(6, len(str(max(count - 1, 0))))


def build_stream(
    count: int, shape: BenchShape, seed: int
) -> RecordAuthorityStream:
    """Build the authority stream, hashing one pass and staging the next.

    Neither pass materializes the whole source: the digest is folded over a
    generator and the operations are consumed one at a time by the store.
    """

    logical_sha256 = legacy_record_set_digest(
        iter_streaming_records(count, shape, seed)
    )
    return RecordAuthorityStream(
        operations=iter_operations(count, shape, seed),
        logical_sha256=logical_sha256,
        record_count=count,
        operation_count=count,
        component_count=count * shape.components_per_record(),
    )


class CountingConnection:
    """Count the database round trips the store issues on this connection.

    Counted: every statement executed on the connection or on one of its
    cursors, every COPY, the ``FETCH FORWARD`` each server-side cursor issues
    per ``itersize`` page while it is iterated, and the two statements a
    ``transaction()`` block costs (``BEGIN``/``COMMIT``, or
    ``SAVEPOINT``/``RELEASE`` when nested).

    Not counted: rows drained from a server-side cursor by ``fetchmany`` or
    ``fetchall`` rather than by iteration, connection setup and teardown, and
    any traffic psycopg issues on its own behalf (prepared-statement
    preparation, type-catalogue lookups).  The number is therefore close to,
    but still a lower bound on, the real round-trip count.  It is the quantity
    a batching regression would move: an unbatched write path costs round trips
    proportional to the record count.
    """

    def __init__(self, conn) -> None:
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "round_trips", 0)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def _count(self) -> None:
        object.__setattr__(self, "round_trips", self.round_trips + 1)

    def execute(self, *args, **kwargs):
        self._count()
        return self._conn.execute(*args, **kwargs)

    def cursor(self, *args, **kwargs):
        return _CountingCursor(self, self._conn.cursor(*args, **kwargs))

    def transaction(self, *args, **kwargs):
        return _CountingTransaction(self, self._conn.transaction(*args, **kwargs))


class _CountingTransaction:
    """Charge a transaction block its two statements as they are issued."""

    def __init__(self, owner, transaction) -> None:
        self._owner = owner
        self._transaction = transaction

    def __enter__(self):
        self._owner._count()
        return self._transaction.__enter__()

    def __exit__(self, *exc):
        self._owner._count()
        return self._transaction.__exit__(*exc)


class _CountingCursor:
    def __init__(self, owner, cursor) -> None:
        object.__setattr__(self, "_owner", owner)
        object.__setattr__(self, "_cursor", cursor)

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __setattr__(self, name, value):
        setattr(self._cursor, name, value)

    def __iter__(self):
        """Iterate, charging each ``FETCH FORWARD`` a server cursor issues.

        A client-side cursor already holds its whole result, so iterating it
        costs nothing.  A server-side cursor fetches one ``itersize`` page per
        round trip, plus a final empty page when the row count lands exactly on
        a page boundary (psycopg ``_server_cursor.ServerCursor.__next__``).
        """

        if getattr(self._cursor, "name", None) is None:
            yield from self._cursor
            return
        itersize = self._cursor.itersize
        seen = 0
        self._count()
        for row in self._cursor:
            if seen and seen % itersize == 0:
                self._count()
            seen += 1
            yield row
        if seen and seen % itersize == 0:
            self._count()

    def __enter__(self):
        self._cursor.__enter__()
        return self

    def __exit__(self, *exc):
        return self._cursor.__exit__(*exc)

    def _count(self) -> None:
        self._owner._count()

    def execute(self, *args, **kwargs):
        self._count()
        return self._cursor.execute(*args, **kwargs)

    def copy(self, *args, **kwargs):
        self._count()
        return self._cursor.copy(*args, **kwargs)


def _table_counts(conn, authority: str) -> dict[str, int]:
    """Row counts this authority actually produced.

    Every table name is a module constant, never caller input, so the counts
    can be composed by name without an injection surface.
    """

    counts: dict[str, int] = {}
    for table in COUNTED_TABLES:
        row = conn.execute(
            "SELECT count(*) FROM " + table + " WHERE authority_id=%s",
            (authority,),
        ).fetchone()
        counts[table] = int(row[0])
    row = conn.execute(
        """SELECT count(*), coalesce(sum(p.byte_count),0),
                  coalesce(sum(p.chunk_count),0)
           FROM okg.source_record_authority_payloads p
           WHERE p.sha256 IN (
             SELECT payload_sha256
             FROM okg.source_record_authority_component_fields
             WHERE authority_id=%s)""",
        (authority,),
    ).fetchone()
    counts["payloads_referenced"] = int(row[0])
    counts["payload_bytes"] = int(row[1])
    counts["payload_chunks_referenced"] = int(row[2])
    return counts


def _baseline_rows(conn) -> dict[str, int]:
    """Unfiltered row counts of the counted tables before this run stages.

    A benchmark run on a database that already holds authority rows is still
    valid, but its timings are not comparable with a run on an empty one, so
    the starting state is recorded rather than assumed.
    """

    baseline: dict[str, int] = {}
    for table in COUNTED_TABLES:
        row = conn.execute("SELECT count(*) FROM " + table).fetchone()
        baseline[table] = int(row[0])
    row = conn.execute(
        "SELECT count(*) FROM okg.source_record_authority_payloads"
    ).fetchone()
    baseline["okg.source_record_authority_payloads"] = int(row[0])
    return baseline


def _rate(count: int, seconds: float) -> float:
    return round(count / seconds, 2) if seconds > 0 else 0.0


def run_bench(
    dsn: str,
    *,
    records: int,
    shape: BenchShape | None = None,
    seed: int = 1664,
    source: str | None = None,
) -> dict[str, Any]:
    """Stage and seal ``records`` synthetic records; return the report.

    The DSN must point at a migrated, throwaway database.  Raises whatever the
    store raises — a failed stage or seal is a benchmark failure, never a
    silently degraded number.  The report records the unfiltered row counts of
    the counted tables before staging (``baseline_rows``) so a run against a
    database that was not empty is visible rather than assumed away.
    """

    if records < 1:
        raise ValueError("record count must be at least 1")
    shape = shape or BenchShape()
    if shape.descendants < 0 or shape.edges < 0 or shape.body_bytes < 0:
        raise ValueError("record shape components must not be negative")
    # Every edge points at a descendant of the same record, so a shape with no
    # descendants has nothing to point at.  Rather than stage edges whose
    # target node never exists, drop them and report that it happened.
    edges_clamped = shape.descendants == 0 and shape.edges > 0
    if edges_clamped:
        shape = replace(shape, edges=0)
    source = source or "bench-source-" + uuid.uuid4().hex

    build_started = time.perf_counter()
    stream = build_stream(records, shape, seed)
    digest_seconds = time.perf_counter() - build_started

    with connect(dsn, autocommit=True) as raw:
        baseline = _baseline_rows(raw)
        counting = CountingConnection(raw)
        store = ProductionAuthorityStore(counting)

        stage_started = time.perf_counter()
        candidate = store.stage_stream(source, "incarnation-1", "attempt-1", stream)
        stage_seconds = time.perf_counter() - stage_started
        stage_round_trips = counting.round_trips

        seal_started = time.perf_counter()
        reference = store.seal_and_bind(candidate)
        seal_seconds = time.perf_counter() - seal_started
        total_round_trips = counting.round_trips

        counts = _table_counts(raw, reference.authority)

    total_seconds = stage_seconds + seal_seconds
    return {
        "schema": BENCH_SCHEMA,
        "records": records,
        "seed": seed,
        "shape": shape.to_json(),
        "shape_edges_clamped": edges_clamped,
        "authority_id": reference.authority,
        "source": source,
        "logical_sha256": reference.sha256,
        "digest_seconds": round(digest_seconds, 3),
        "stage_seconds": round(stage_seconds, 3),
        "seal_seconds": round(seal_seconds, 3),
        "total_seconds": round(total_seconds, 3),
        "records_per_second": _rate(records, total_seconds),
        "stage_records_per_second": _rate(records, stage_seconds),
        "round_trips": {
            "stage": stage_round_trips,
            "seal": total_round_trips - stage_round_trips,
            "total": total_round_trips,
        },
        "round_trips_per_record": round(total_round_trips / records, 6),
        "row_counts": counts,
        "baseline_rows": baseline,
        "machine": {
            "system": platform.system(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
        },
    }



def _peak_rss_bytes() -> int:
    import resource
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def run_real_bench(dsn: str, *, manifest_path: str,
                   sample_records: int | None = None) -> dict[str, Any]:
    """Validate a complete frozen snapshot, then stage/seal an exact selection.

    This measures authority persistence, not source ingestion or publication.
    Original source/provenance assertions are preserved, not authenticated by
    the generic bundle consumer. The database must be migrated and disposable.
    """
    from okg.substrate.record_authority_bench_input import prepare_snapshot

    started = time.perf_counter()
    rss_before = _peak_rss_bytes()
    with prepare_snapshot(manifest_path) as prepared:
        selection = prepared.select(sample_records)
        manifest = prepared.manifest
        source = "real-authority-bench-" + uuid.uuid4().hex
        incarnation, attempt = uuid.uuid4().hex, uuid.uuid4().hex
        stream = prepared.stream(selection)
        with connect(dsn, autocommit=True) as raw:
            baseline = _baseline_rows(raw)
            storage_before = raw.execute(
                "SELECT pg_database_size(current_database()), pg_current_wal_lsn()::text"
            ).fetchone()
            counting = CountingConnection(raw)
            store = ProductionAuthorityStore(counting)
            stage_started = time.perf_counter()
            candidate = store.stage_stream(source, incarnation, attempt, stream)
            stage_seconds = time.perf_counter() - stage_started
            stage_trips = counting.round_trips
            seal_started = time.perf_counter()
            reference = store.seal_and_bind(candidate)
            seal_seconds = time.perf_counter() - seal_started
            total_trips = counting.round_trips
            rows = _table_counts(raw, reference.authority)
            storage_after = raw.execute(
                "SELECT pg_database_size(current_database()), pg_current_wal_lsn()::text, "
                "pg_wal_lsn_diff(pg_current_wal_lsn(), %s::pg_lsn)",
                (storage_before[1],),
            ).fetchone()
        total = stage_seconds + seal_seconds
        return {
            "schema": "okg.record_authority.real_bench.v1",
            "scope": "authority stage and seal only; no ingestion, reconciliation or publication",
            "bundle": manifest,
            "bundle_verification": {
                "verified": "captured manifest and records bytes, hashes, strict structure, order, counts and logical digest",
                "external_attestations": "source snapshot, revision, adapter, exporter and normalization provenance; not authenticated here",
                "manifest_sha256": prepared.manifest_sha256,
            },
            "selection": {"policy": selection.policy, "records": selection.records,
                          "components": selection.components,
                          "logical_sha256": selection.logical_sha256},
            "authority_id": reference.authority, "source": source,
            "incarnation": incarnation, "attempt": attempt,
            "logical_sha256": reference.sha256,
            "timings": {"external_preparation_seconds": None,
                        "external_preparation_status": "not measured here; see external exporter evidence",
                        "capture_seconds": round(prepared.capture_seconds, 3),
                        "validation_seconds": round(prepared.validation_seconds, 3),
                        "selection_digest_seconds": round(selection.digest_seconds, 3),
                        "stage_seconds": round(stage_seconds, 3),
                        "seal_seconds": round(seal_seconds, 3),
                        "stage_seal_seconds": round(total, 3),
                        "harness_seconds": round(time.perf_counter() - started, 3)},
            "records_per_second": _rate(selection.records, total),
            "stage_records_per_second": _rate(selection.records, stage_seconds),
            "round_trips": {"stage": stage_trips, "seal": total_trips - stage_trips,
                            "total": total_trips},
            "round_trips_per_record": (round(total_trips / selection.records, 6)
                                       if selection.records else None),
            "row_counts": rows, "baseline_rows": baseline,
            "storage": {"database_bytes_before": int(storage_before[0]),
                        "database_bytes_after": int(storage_after[0]),
                        "wal_lsn_before": storage_before[1], "wal_lsn_after": storage_after[1],
                        "cluster_wal_bytes_generated": int(storage_after[2]),
                        "wal_scope": "cluster-wide; may include concurrent workloads"},
            "memory": {"max_record_bytes": prepared.max_record_bytes,
                       "manifest_bytes": prepared.manifest_bytes,
                       "spool_bytes": manifest["records_bytes"],
                       "bound": "one decoded JSONL record and manifest plus bounded store batches; no source-size collection",
                       "peak_rss_before_bytes": rss_before,
                       "peak_rss_after_bytes": _peak_rss_bytes(),
                       "rss_scope": "process lifetime high-water mark; not an incremental allocation measure"},
            "machine": {"system": platform.system(), "machine": platform.machine(),
                        "processor": platform.processor(), "python": platform.python_version()},
        }


def _positive_int(text: str) -> int:
    value = _non_negative_int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1, got %r" % text)
    return value


def _non_negative_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer, got %r" % text) from None
    if value < 0:
        raise argparse.ArgumentTypeError("must not be negative, got %r" % text)
    return value


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m okg.substrate.record_authority_bench",
        description=(
            "Stage and seal N deterministic synthetic records through the "
            "indexed-v2 record authority and report the cost as JSON."
        ),
    )
    parser.add_argument(
        "--dsn", required=True,
        help="DSN of a migrated, throwaway Postgres database.",
    )
    parser.add_argument(
        "--records", type=_positive_int, default=None,
        help="Number of synthetic records to stage (default: 20000).",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="RNG seed; the same seed reproduces the same bytes.",
    )
    parser.add_argument(
        "--descendants", type=_non_negative_int, default=None,
        help=(
            "Descendant nodes per record (default: %d).  With 0 descendants "
            "there is nothing for an edge to point at, so --edges is forced "
            "to 0 and the report says so." % BenchShape.descendants
        ),
    )
    parser.add_argument(
        "--edges", type=_non_negative_int, default=None,
        help=(
            "Edges per record, each pointing at one of that record's "
            "descendants (default: %d)." % BenchShape.edges
        ),
    )
    parser.add_argument(
        "--body-bytes", type=_non_negative_int, default=None,
        help=(
            "Approximate size in bytes of each record's revision body, which "
            "dominates the payload bytes copied (default: %d)."
            % BenchShape.body_bytes
        ),
    )
    parser.add_argument(
        "--output", default=None,
        help="Write the JSON report here instead of stdout.",
    )
    parser.add_argument("--input-manifest", default=None,
                        help="Complete ordered real-record snapshot; selects the separate real benchmark.")
    parser.add_argument("--sample-records", type=_non_negative_int, default=None,
                        help="Exact canonical prefix of a fully validated real snapshot; omitted means full.")
    args = parser.parse_args(argv)
    if args.sample_records is not None and args.input_manifest is None:
        parser.error("--sample-records requires --input-manifest")
    if args.input_manifest is not None:
        if any(getattr(args, name) is not None for name in
               ("records", "seed", "descendants", "edges", "body_bytes")):
            parser.error("synthetic shape/seed/count options cannot accompany --input-manifest")
    # Apply the existing synthetic defaults after detecting explicit options,
    # including argparse's supported unambiguous abbreviations.
    if args.records is None:
        args.records = 20_000
    if args.seed is None:
        args.seed = 1664
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    shape = BenchShape()
    overrides = {
        name: value
        for name, value in (
            ("descendants", args.descendants),
            ("edges", args.edges),
            ("body_bytes", args.body_bytes),
        )
        if value is not None
    }
    if overrides:
        shape = replace(shape, **overrides)
    if args.input_manifest is not None:
        report = run_real_bench(args.dsn, manifest_path=args.input_manifest,
                                sample_records=args.sample_records)
    else:
        report = run_bench(
            args.dsn, records=args.records, shape=shape, seed=args.seed,
        )
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
