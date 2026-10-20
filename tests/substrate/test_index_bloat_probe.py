"""Index-bloat probe regressions (okg#1324).

`okg.attribute_effective_view` on okg-workspace carried 2,333 MB of indexes over
a 16 kB table after its rows were drained into
`attribute_effective_view_intervals` -- VACUUM had reclaimed every dead tuple and
left the btree structure behind, so its primary key kept walking a
multi-gigabyte tree on all 24.7M of its scans. Nothing reported that; an
operator found it by hand.

These tests build the same condition in a real Postgres database and prove that
the disk report names the table, its index sizes, the per-index scan counts, and
the exact `REINDEX TABLE CONCURRENTLY` -- and that it does NOT reindex on its
own, and does NOT flag a table that either arm of the threshold spares.

Every DB-backed test asserts the FLAGGED case before any negative one: a probe
that flagged nothing would otherwise pass every negative arm vacuously. Both
controls are real -- one clears the byte floor and is spared by the ratio, the
other clears the ratio and is spared by the byte floor -- so neither arm can be
deleted from the query without reddening this file.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import uuid

import psycopg
from psycopg import sql
import pytest

from okg.substrate.db.management import (
    _DISK_INDEX_BLOAT_MIN_INDEX_BYTES,
    _DISK_INDEX_BLOAT_RATIO,
    _disk_findings,
    _quote_qualified,
    disk_usage_report,
)

# 200k rows carrying a 320-byte pad leave ~87 MB behind in a (pad, id) index --
# 1.75x the 50 MB floor, so the probe's real default threshold is exercised
# rather than a test-only relaxation, and a fillfactor or PG-version change
# cannot silently drop the fixture under it. Wide rows rather than many rows:
# index bytes are what the floor measures, and this reaches them in a third of
# the time 1M narrow rows took (measured 3.5s for both tables on this host).
# 320 bytes stays under the 2 kB TOAST threshold, so the rows are inline and
# pg_table_size is the heap.
_BLOAT_ROWS = 200_000
_PAD_WIDTH = 320
_SURVIVORS = 16  # the live row count okg#1324 actually observed

# Drained: rows deleted in bulk, then vacuumed. Over both thresholds.
_DRAINED = "index_bloat_probe_drained"
# Ratio control: same indexes, rows still present. Its index is over the byte
# floor, so only the ratio arm can spare it.
_POPULATED = "index_bloat_probe_populated"
# Floor control: many indexes over an 8 kB table. Its ratio is over 3x, so only
# the byte floor can spare it. The test asserts the ratio it actually reaches,
# so a Postgres change to empty-index or map sizes reds this rather than
# silently turning the control into a table both arms spare.
_TINY = "index_bloat_probe_tiny"
_TINY_INDEXES = 16


# --- pure: thresholds, quoting, finding shape ------------------------------


def test_index_bloat_thresholds_are_the_operator_signature():
    # The scan that found okg#1324: indexes more than 3x the table, and over
    # 50 MB. Either arm alone is useless -- the ratio flags every small healthy
    # table, the floor flags every large healthy index.
    assert _DISK_INDEX_BLOAT_RATIO == 3.0
    assert _DISK_INDEX_BLOAT_MIN_INDEX_BYTES == 50 * 1024**2


def test_index_bloat_quotes_identifiers_for_a_runnable_command():
    assert _quote_qualified("okg", "attribute_effective_view") == (
        '"okg"."attribute_effective_view"'
    )
    # An embedded quote must be doubled, not pasted into broken DDL.
    assert _quote_qualified("okg", 'we"ird') == '"okg"."we""ird"'


def test_index_bloat_finding_carries_the_reindex_command():
    table = {
        "qualified": "okg.attribute_effective_view",
        "table_bytes": 16 * 1024,
        "index_bytes": 2_446_327_808,
        "index_to_table_ratio": 149336.0,
        "est_rows": 16,
        "indexes": [
            {
                "index": "okg.attribute_effective_view_pkey",
                "index_bytes": 1_021_313_024,
                "idx_scan": 24_701_292,
            }
        ],
        "reindex_command": (
            'REINDEX TABLE CONCURRENTLY "okg"."attribute_effective_view";'
        ),
    }
    findings = _disk_findings(
        index_usage={"unused_index_bytes": 0, "unused_index_count": 0},
        compression=[],
        dead_tuples=[],
        generations=1,
        deep=False,
        index_bloat=[table],
    )
    bloat = [f for f in findings if f["code"] == "index_larger_than_table"]
    assert len(bloat) == 1
    assert bloat[0]["severity"] == "warning"
    assert bloat[0]["relation"] == "okg.attribute_effective_view"
    assert bloat[0]["reindex_command"] == table["reindex_command"]
    assert bloat[0]["indexes"][0]["idx_scan"] == 24_701_292
    assert table["reindex_command"] in bloat[0]["fix_hint"]


def test_index_bloat_findings_absent_when_nothing_is_flagged():
    findings = _disk_findings(
        index_usage={"unused_index_bytes": 0, "unused_index_count": 0},
        compression=[],
        dead_tuples=[],
        generations=1,
        deep=False,
        index_bloat=[],
    )
    assert not [f for f in findings if f["code"] == "index_larger_than_table"]


def test_size_bracket_absorbs_fsm_drift_but_not_a_wrong_value():
    # The exact drift that reddened okg#1629 and okg#1634 on an ~87 MB index.
    _assert_within_live_bracket(87_498_752, 87_433_216, 87_433_216, what="fsm drift")
    # A zero, whatever the slack works out to.
    with pytest.raises(AssertionError):
        _assert_within_live_bracket(0, 87_433_216, 87_433_216, what="zero")
    # A rebuilt index (measured post-REINDEX size).
    with pytest.raises(AssertionError):
        _assert_within_live_bracket(32_768, 87_433_216, 87_433_216, what="reindexed")
    # A value off by more than the slack -- 1/64th of ~87 MB is ~1.37 MB, so a
    # 2 MB error must be caught, not absorbed.
    with pytest.raises(AssertionError):
        _assert_within_live_bracket(
            87_433_216 + 2_000_000, 87_433_216, 87_433_216, what="2 MB off"
        )
    # At heap scale the floor is 4 blocks, so a small table cannot be swapped
    # for one a few blocks different.
    with pytest.raises(AssertionError):
        _assert_within_live_bracket(
            49_152 + 5 * _BLOCK_BYTES, 49_152, 49_152, what="heap scale"
        )


# --- DB-backed: build the real condition -----------------------------------


@contextmanager
def _isolated_bloat_database(base_dsn: str):
    """Create from template0, never clone or drop a caller's database."""
    options = psycopg.conninfo.conninfo_to_dict(base_dsn)
    name = f"okg_test_bloat_{uuid.uuid4().hex}"
    maintenance = psycopg.conninfo.make_conninfo(**{**options, "dbname": "postgres"})
    dsn = psycopg.conninfo.make_conninfo(**{**options, "dbname": name})
    with psycopg.connect(maintenance, autocommit=True, connect_timeout=10) as conn:
        # No preemptive DROP or IF NOT EXISTS: a collision must fail without
        # claiming the pre-existing database. Register cleanup immediately.
        conn.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                sql.Identifier(name)
            )
        )
        # If identity cannot be resolved, do not drop an unknown database.
        oid = conn.execute(
            "SELECT oid FROM pg_database WHERE datname = %s", (name,)
        ).fetchone()[0]
        try:
            with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as setup:
                setup.execute("CREATE SCHEMA okg")
            yield dsn
        finally:
            current = conn.execute(
                "SELECT oid FROM pg_database WHERE datname = %s", (name,)
            ).fetchone()
            if current is not None:
                if current[0] != oid:
                    raise RuntimeError(
                        f"refusing to drop replaced bloat database {name}"
                    )
                conn.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(name)
                    )
                )


def _assert_drained_premise(dsn: str, vacuum_notices: list[str]) -> dict:
    table_bytes, index_bytes = _sizes(dsn, f"okg.{_DRAINED}")
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f"SELECT count(*) FROM okg.{_DRAINED}").fetchone()[0]
    measured = {
        "rows": rows,
        "table_bytes": table_bytes,
        "index_bytes": index_bytes,
        "vacuum_notices": vacuum_notices,
    }
    assert (
        rows == _SURVIVORS
        and index_bytes > _DISK_INDEX_BLOAT_MIN_INDEX_BYTES
        and index_bytes > _DISK_INDEX_BLOAT_RATIO * table_bytes
    ), "index-bloat fixture premise failed: " + json.dumps(measured, sort_keys=True)
    return measured


def _build_tables(dsn: str) -> dict:
    notices: list[str] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.add_notice_handler(
            lambda diagnostic: notices.append(
                f"{diagnostic.message_primary}\n{diagnostic.message_detail or ''}"
            )
        )
        for name in (_DRAINED, _POPULATED):
            conn.execute(f"DROP TABLE IF EXISTS okg.{name} CASCADE")
            conn.execute(
                f"CREATE TABLE okg.{name} (id bigint PRIMARY KEY, pad text NOT NULL)"
            )
            conn.execute(
                f"INSERT INTO okg.{name} "
                "SELECT g, lpad(g::text, %s, '0') "
                "FROM generate_series(1, %s) g",
                (_PAD_WIDTH, _BLOAT_ROWS),
            )
            conn.execute(f"CREATE INDEX {name}_pad_idx ON okg.{name} (pad, id)")
        # Only the drained table loses its rows. VACUUM reclaims the tuples and
        # truncates the table; the indexes keep their pages -- okg#1324.
        conn.execute(f"DELETE FROM okg.{_DRAINED} WHERE id > %s", (_SURVIVORS,))

        # Floor control: a handful of rows carrying many indexes. Its index
        # bytes are several times its table bytes, so the RATIO arm alone would
        # flag it; only the 50 MB floor spares it.
        conn.execute(f"DROP TABLE IF EXISTS okg.{_TINY} CASCADE")
        conn.execute(f"CREATE TABLE okg.{_TINY} (id bigint PRIMARY KEY)")
        conn.execute(f"INSERT INTO okg.{_TINY} SELECT g FROM generate_series(1, 5) g")
        # Enough indexes that their total clears 3x this table's size. Fewer
        # would leave the ratio arm untested here, because pg_table_size
        # counts the free-space and visibility maps alongside the 8 kB heap.
        for n in range(_TINY_INDEXES):
            conn.execute(f"CREATE INDEX {_TINY}_x{n}_idx ON okg.{_TINY} ((id + {n}))")
        # The drained table can only be truncated down to its last page that
        # still holds a row, so this VACUUM must remove every deleted one. A
        # plain VACUUM skips a page that another process holds at that instant
        # (for example while writing it out) and leaves its deleted rows: on
        # #1894 (run 34882578111) three skipped pages left the table at 4,041
        # pages and the index only 2.6x it, failing the premise below. FREEZE
        # makes VACUUM wait for such a page instead, because rows older than
        # its freeze cutoff (every row here) must be handled in this pass. It
        # still keeps deleted rows an older open transaction can see; those
        # would still block the cut, and the premise message prints VACUUM's
        # report, which counts them. The controls do not depend on truncation.
        conn.execute(f"VACUUM (FREEZE, ANALYZE, VERBOSE) okg.{_DRAINED}")
        for name in (_POPULATED, _TINY):
            conn.execute(f"VACUUM (ANALYZE, VERBOSE) okg.{name}")
    return _assert_drained_premise(dsn, notices)


def _sizes(dsn: str, relation: str) -> tuple[int, int]:
    """(table_bytes, index_bytes) read straight from Postgres, independent of
    anything the probe reports."""
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT pg_table_size(%s::regclass), pg_indexes_size(%s::regclass)",
            (relation, relation),
        ).fetchone()
    return int(row[0]), int(row[1])


# Relation sizes are filesystem reads, not MVCC-snapshot values, so they are NOT
# stable across two connections. `pg_indexes_size` counts every fork of every
# index, and autovacuum creates an index free-space map on its own schedule. A
# byte-exact comparison between a live read here and the probe read on its own
# connection therefore fails, and did: okg#1629 (run 33988833830) got
# `assert 87433216 == 87490560` and okg#1634 (run 33988843021) got
# `assert 87433216 == 87457792` -- same left side, different right side, on a
# table neither test had written to. Measured locally on this fixture: a second
# VACUUM takes pg_indexes_size from 87,433,216 to 87,498,752, the 65,536-byte
# difference being two FSM forks (24,576 + 40,960). Indexes have no visibility
# map; the free-space map is the whole mechanism.
#
# What the probe owes us is that it reports a real measurement of THIS relation
# rather than a fabricated, zeroed or stale number. So the assertion brackets:
# the reported value must lie between live reads taken before and after the
# report, widened by a slack that scales with the value. FSM growth is monotonic
# and bounded by a few blocks, so 4 blocks or 1/64th of the value -- whichever
# is larger -- absorbs it while staying far tighter than the failures this
# guards against: a zero, or the ~87 MB drop a REINDEX causes.
_BLOCK_BYTES = 8192


def _size_slack(live: int) -> int:
    return max(4 * _BLOCK_BYTES, live // 64)


def _assert_within_live_bracket(
    reported: int, live_before: int, live_after: int, *, what: str
) -> None:
    # A reported zero must never pass, whatever the slack works out to.
    assert reported > 0, f"{what}: probe reported {reported} bytes"
    slack = _size_slack(max(live_before, live_after))
    low = min(live_before, live_after) - slack
    high = max(live_before, live_after) + slack
    assert low <= reported <= high, (
        f"{what}: probe reported {reported} bytes, outside the live bracket "
        f"[{low}, {high}] (before={live_before}, after={live_after}, "
        f"slack={slack})"
    )


@pytest.fixture(scope="module")
def bloat_fixture(okg_dsn: str):
    """Build the three tables ONCE for this module. Two ~95 MB index sets are
    what exercising the real 50 MB threshold costs, so both tests share one
    build rather than paying it twice inside a per-pull-request lane."""
    with _isolated_bloat_database(okg_dsn) as dsn:
        measured = _build_tables(dsn)
        print("index-bloat fixture: " + json.dumps(measured, sort_keys=True))
        yield dsn


@pytest.mark.postgres
def test_disk_report_flags_a_drained_table(bloat_fixture: str):
    dsn = bloat_fixture
    table_before, index_before = _sizes(dsn, f"okg.{_DRAINED}")
    report = disk_usage_report(dsn, deployment_name="okg_test")
    table_after, index_after = _sizes(dsn, f"okg.{_DRAINED}")
    flagged = {t["qualified"]: t for t in report["index_bloat"]}

    assert f"okg.{_DRAINED}" in flagged, (
        f"the drained table was not flagged; index_bloat={report['index_bloat']}"
    )
    entry = flagged[f"okg.{_DRAINED}"]
    # Sizes are a real measurement of this relation, taken between two live
    # reads -- not a fabricated, zeroed or stale number.
    _assert_within_live_bracket(
        entry["table_bytes"], table_before, table_after, what="table_bytes"
    )
    _assert_within_live_bracket(
        entry["index_bytes"], index_before, index_after, what="index_bytes"
    )
    # The bracket is coarse at heap scale (the drained heap is ~6 blocks), so
    # pin table_bytes with the probe's own arithmetic too. Both sides come from
    # one read, so these stay byte-exact.
    assert entry["index_to_table_ratio"] == round(
        entry["index_bytes"] / entry["table_bytes"], 1
    )
    assert entry["index_bytes"] > _DISK_INDEX_BLOAT_RATIO * entry["table_bytes"]
    assert entry["est_rows"] == _SURVIVORS
    # The per-index sizes account for the whole reported footprint. This is
    # byte-exact only because the probe reads both with pg_total_relation_size;
    # reading the parts with pg_relation_size (main fork) and the total with
    # pg_indexes_size (every fork) is what reddened okg#1629 and okg#1634.
    assert sum(i["index_bytes"] for i in entry["indexes"]) == entry["index_bytes"]
    assert entry["reindex_command"] == (
        f'REINDEX TABLE CONCURRENTLY "okg"."{_DRAINED}";'
    )
    # Names its indexes with sizes and scan counts, so an operator can see
    # which tree the hot lookups were walking.
    names = {i["index"] for i in entry["indexes"]}
    assert names == {f"okg.{_DRAINED}_pkey", f"okg.{_DRAINED}_pad_idx"}
    for index in entry["indexes"]:
        assert isinstance(index["idx_scan"], int)

    # The doctor surfaces it as an advisory warning naming the command.
    bloat_findings = [
        f
        for f in report["findings"]
        if f["code"] == "index_larger_than_table" and f["relation"] == f"okg.{_DRAINED}"
    ]
    assert len(bloat_findings) == 1
    assert bloat_findings[0]["severity"] == "warning"
    assert entry["reindex_command"] in bloat_findings[0]["fix_hint"]
    # Advisory, not a failure: a bloated index is not an unreadable database.
    assert report["ok"] is True

    # READ-ONLY: the probe must not rebuild anything itself. A REINDEX takes
    # this index from ~87 MB to ~32 kB (measured), so a bracket around the
    # pre-report read catches it with orders of magnitude to spare, without
    # asserting a byte-identical filesystem size across connections.
    index_final = _sizes(dsn, f"okg.{_DRAINED}")[1]
    _assert_within_live_bracket(
        index_final,
        index_before,
        index_after,
        what="index_bytes after the report (the probe must not reindex)",
    )
    assert index_final > _DISK_INDEX_BLOAT_MIN_INDEX_BYTES


@pytest.mark.postgres
def test_disk_report_spares_the_table_each_threshold_arm_protects(
    bloat_fixture: str,
):
    dsn = bloat_fixture
    report = disk_usage_report(dsn, deployment_name="okg_test")
    reported = {t["qualified"] for t in report["index_bloat"]}

    # POSITIVE arm first. Without it, an empty report would pass both
    # exclusions below for the wrong reason.
    assert f"okg.{_DRAINED}" in reported, (
        "the drained table was not flagged, so the exclusions below prove "
        f"nothing; index_bloat={report['index_bloat']}"
    )

    # The RATIO arm does work: this table's indexes are over the 50 MB floor,
    # so the floor cannot be what spares it.
    populated_table, populated_index = _sizes(dsn, f"okg.{_POPULATED}")
    assert populated_index > _DISK_INDEX_BLOAT_MIN_INDEX_BYTES
    assert populated_index < _DISK_INDEX_BLOAT_RATIO * populated_table
    assert f"okg.{_POPULATED}" not in reported

    # The FLOOR arm does work: this table's indexes are several times its
    # table size, so the ratio cannot be what spares it.
    tiny_table, tiny_index = _sizes(dsn, f"okg.{_TINY}")
    assert tiny_index > _DISK_INDEX_BLOAT_RATIO * tiny_table
    assert tiny_index < _DISK_INDEX_BLOAT_MIN_INDEX_BYTES
    assert f"okg.{_TINY}" not in reported

    # Findings and entries agree: no finding without a reported table.
    finding_relations = {
        f["relation"]
        for f in report["findings"]
        if f["code"] == "index_larger_than_table"
    }
    assert finding_relations == reported
