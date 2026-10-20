"""PACT add-emit-once-dedupe-scope (issue #901).

The enricher emit helpers could only collapse two copies of a fact that
arrived at the same instant, so a producer re-deriving the same
deterministic fact every publish cycle appended a fact-log row every
cycle. These tests pin the new `dedupe_scope="key"` mode and, just as
importantly, the two things it deliberately does NOT do:

- it must never suppress a RETRACTION with the assertion it retracts, and
  never suppress one producer's fact with another producer's, which is
  why the probe is keyed `(source, dedupe_key, op)` and not `dedupe_key`;
- it DOES suppress re-assertion after retraction, which is the literal
  meaning of emit-once and a trap for any producer whose facts can become
  false and then true again.

`test_every_op_sequence_up_to_length_three` checks the op-sequence
behaviour EXHAUSTIVELY rather than by example. No TLC model is added for
this change: the property that decides it is the semantics of one SQL
predicate under real Postgres visibility rules on a Timescale-partitioned
table, which an abstract model cannot represent -- so the state-machine
part is enumerated in full against the real schema instead.
"""
from __future__ import annotations

import itertools
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest

from okg.substrate import fastyaml
from okg.substrate.library.enrichers._fact_emitters import (
    DEDUPE_SCOPES,
    emit_edge_fact,
    emit_node_fact,
)

ROOT = Path(__file__).resolve().parents[4]

pytestmark = pytest.mark.postgres

# Relative to now, never a fixed calendar date. okg.node_facts and
# okg.edge_facts carry a 180-day Timescale retention policy and a 30-day
# compression policy (schema.sql), and the scheduler runs both on its own
# timer -- on CI, on a freshly cloned test database, often in the middle of
# this file. A fact dated past the retention horizon is deleted whenever
# that run lands between an emit and the count that checks it, which is
# how a fixed 2026-01-01 turned into an order-dependent `assert 0 == 2`
# once the calendar passed 180 days; a chunk past the compression horizon
# can be compressed under the file's inserts. Seven days back, plus the
# furthest offset this file adds (`_LATEST_OFFSET`), keeps every row inside
# both horizons and never in the future.
T0 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=7)
T1 = T0 + timedelta(days=1)
#: The furthest this file writes from T0 (`test_every_op_sequence_...`
#: and `test_reassertion_...` go to T0 + 2 days).
_LATEST_OFFSET = timedelta(days=2)


def _unique(prefix: str) -> str:
    """A per-test producer name, so no test can see another's rows."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _emit_node(conn, name, *, dk, op="I", at=T0, scope="key_and_observed_at"):
    return emit_node_fact(
        conn,
        enricher_name=name,
        node_id=f"node:{dk}",
        subtype="Document",
        attrs={"title": "t"},
        dedupe_key=dk,
        source_record_id={"k": dk},
        source_revision={"rev": "1"},
        run_id=str(uuid.uuid4()),
        observed_at=at,
        op=op,
        derived_from=[f"input:{dk}"],
        support_policy="all_members",
        dedupe_scope=scope,
    )


def _emit_edge(conn, name, *, dk, op="I", at=T0, scope="key_and_observed_at"):
    return emit_edge_fact(
        conn,
        enricher_name=name,
        src=f"node:{dk}:src",
        dst=f"node:{dk}:dst",
        edge_type="references",
        attrs={},
        provenance="derived_deterministic",
        confidence=1.0,
        dedupe_key=dk,
        source_record_id={"k": dk},
        source_revision={"rev": "1"},
        run_id=str(uuid.uuid4()),
        observed_at=at,
        op=op,
        dedupe_scope=scope,
    )


def _count(conn, table: str, name: str, dk: str, op: str | None = None) -> int:
    sql = (
        f"SELECT count(*) FROM okg.{table} "
        "WHERE source = %s AND dedupe_key = %s"
    )
    params: list[object] = [f"_enricher:{name}", dk]
    if op is not None:
        sql += " AND op = %s"
        params.append(op)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0])


# --------------------------------------------------------------------
# req.emit-once-dedupe-scope
# --------------------------------------------------------------------


def test_emit_once_scope_collapses_across_observed_at(okg_migrated: str) -> None:
    """The bug in issue #901: same fact, later cycle, new observed_at."""
    name, dk = _unique("once"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        for at in (T0, T1):
            _emit_node(conn, name, dk=dk, at=at, scope="key")
            _emit_edge(conn, name, dk=dk, at=at, scope="key")
        conn.commit()
        assert _count(conn, "node_facts", name, dk) == 1
        assert _count(conn, "edge_facts", name, dk) == 1


def test_default_scope_is_unchanged(okg_migrated: str) -> None:
    """Two observed_at values still append; same instant still collapses."""
    name, dk = _unique("default"), _unique("dk")
    same = _unique("dk-same")
    with psycopg.connect(okg_migrated) as conn:
        for at in (T0, T1):
            _emit_node(conn, name, dk=dk, at=at)
            _emit_edge(conn, name, dk=dk, at=at)
        for _ in range(2):
            _emit_node(conn, name, dk=same, at=T0)
            _emit_edge(conn, name, dk=same, at=T0)
        conn.commit()
        assert _count(conn, "node_facts", name, dk) == 2
        assert _count(conn, "edge_facts", name, dk) == 2
        assert _count(conn, "node_facts", name, same) == 1
        assert _count(conn, "edge_facts", name, same) == 1


def test_unknown_scope_raises(okg_migrated: str) -> None:
    """A typo must not silently pick either mode, and must not write."""
    name, dk = _unique("bad"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        for emit in (_emit_node, _emit_edge):
            with pytest.raises(ValueError) as excinfo:
                emit(conn, name, dk=dk, scope="whatever")
            message = str(excinfo.value)
            # Both accepted values must be named. "key" is checked as a
            # standalone quoted token, not as a substring -- it occurs
            # inside "key_and_observed_at" and would pass vacuously.
            assert "'key_and_observed_at'" in message
            assert "'key'" in message
            assert "whatever" in message
        conn.commit()
        assert _count(conn, "node_facts", name, dk) == 0
        assert _count(conn, "edge_facts", name, dk) == 0


def test_fixture_dates_sit_inside_the_fact_retention_window(
    okg_migrated: str,
) -> None:
    """Every observed_at this file writes must be younger than the fact
    tables' retention and compression horizons, read from the migrated
    database rather than copied here, so a longer or shorter policy moves
    the bound with it. Otherwise a retention run that lands mid-test
    deletes the rows the test is about to count, and a compression run
    compresses the chunk the test is writing into."""
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT proc_name, hypertable_name,
                   COALESCE(config->>'drop_after',
                            config->>'compress_after')::interval
              FROM timescaledb_information.jobs
             WHERE proc_name IN ('policy_retention', 'policy_compression')
               AND hypertable_schema = 'okg'
               AND hypertable_name IN ('node_facts', 'edge_facts')
            """,
        )
        horizons = {(proc, table): after for proc, table, after in cur.fetchall()}
    assert set(horizons) == {
        (proc, table)
        for proc in ("policy_retention", "policy_compression")
        for table in ("node_facts", "edge_facts")
    }, horizons
    oldest = T0
    newest = T0 + _LATEST_OFFSET
    now = datetime.now(timezone.utc)
    assert newest <= now, "a future observed_at would test something else"
    for key, after in horizons.items():
        assert after is not None, key
        # A week of margin: Timescale acts on whole 7-day chunks, and a
        # long session must not age the first row past the horizon.
        assert now - oldest < after - timedelta(days=7), (key, oldest, after)


def test_declared_scopes_are_exactly_the_two_documented() -> None:
    assert DEDUPE_SCOPES == ("key_and_observed_at", "key")


@pytest.mark.parametrize("scope", DEDUPE_SCOPES)
def test_an_out_of_vocabulary_op_is_rejected_not_truncated(
    okg_migrated: str, scope: str,
) -> None:
    """`op` is identity-bearing and its column is CHAR(1). An explicit
    `::char(1)` cast in the statement would TRUNCATE 'Insert' to 'I' --
    storing a fact the caller never asked for, and (under emit-once)
    probing for the untruncated string so the dedupe silently never
    matches. Postgres raises on assignment instead, and this test keeps
    it that way for BOTH scopes."""
    name, dk = _unique(f"badop-{scope}"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        with pytest.raises(psycopg.errors.StringDataRightTruncation):
            _emit_edge(conn, name, dk=dk, op="Insert", at=T0, scope=scope)
        conn.rollback()
        assert _count(conn, "edge_facts", name, dk) == 0


# --------------------------------------------------------------------
# req.emit-once-never-suppresses-a-retraction
# --------------------------------------------------------------------


def test_emit_once_does_not_suppress_a_retraction(okg_migrated: str) -> None:
    """The P0 an op-blind probe would introduce: a retraction swallowed by
    the assertion it retracts, leaving the edge live forever."""
    name, dk = _unique("retract"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        _emit_node(conn, name, dk=dk, op="I", at=T0, scope="key")
        _emit_edge(conn, name, dk=dk, op="I", at=T0, scope="key")
        out_n = _emit_node(conn, name, dk=dk, op="R", at=T1, scope="key")
        out_e = _emit_edge(conn, name, dk=dk, op="R", at=T1, scope="key")
        conn.commit()
        assert out_n.inserted is True
        assert out_e.inserted is True
        assert _count(conn, "node_facts", name, dk, "R") == 1
        assert _count(conn, "edge_facts", name, dk, "R") == 1


def test_emit_once_is_scoped_to_one_producer(okg_migrated: str) -> None:
    """dedupe_key is only conventionally namespaced, so a key-only probe
    would let one producer suppress another's fact for all time.

    The two producers emit at DIFFERENT observed_at deliberately. The
    pre-existing `UNIQUE (dedupe_key, observed_at)` is itself source-
    blind, so at the same instant two producers sharing a key already
    collapse today, with or without this change; emitting at the same
    instant here would test that constraint instead of the new probe.
    That asymmetry is real and is called out in the helper docstring:
    emit-once is source-scoped, same-instant collapse is not."""
    a, b, dk = _unique("prod-a"), _unique("prod-b"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        for name, at in ((a, T0), (b, T1)):
            assert _emit_node(conn, name, dk=dk, at=at, scope="key").inserted
            assert _emit_edge(conn, name, dk=dk, at=at, scope="key").inserted
        conn.commit()
        for name in (a, b):
            assert _count(conn, "node_facts", name, dk) == 1
            assert _count(conn, "edge_facts", name, dk) == 1


def test_reassertion_after_retraction_is_suppressed_by_design(
    okg_migrated: str,
) -> None:
    """Emit-once means once. I -> R -> I drops the trailing I, because the
    first I is still in the log. Pinned so it is a known property of the
    mode rather than something a producer discovers in production."""
    name, dk = _unique("reassert"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        _emit_edge(conn, name, dk=dk, op="I", at=T0, scope="key")
        _emit_edge(conn, name, dk=dk, op="R", at=T1, scope="key")
        again = _emit_edge(
            conn, name, dk=dk, op="I", at=T1 + timedelta(days=1), scope="key",
        )
        conn.commit()
        assert again.inserted is False
        assert again.emitted is True
        assert _count(conn, "edge_facts", name, dk, "I") == 1


@pytest.mark.parametrize(
    "sequence",
    [
        seq
        for length in (1, 2, 3)
        for seq in itertools.product("IR", repeat=length)
    ],
    ids=lambda seq: "".join(seq),
)
def test_every_op_sequence_up_to_length_three(
    okg_migrated: str, sequence: tuple[str, ...],
) -> None:
    """Exhaustive, not by example: for EVERY op sequence over {I, R} up to
    length 3, emit-once must store the first occurrence of each op and
    nothing more. Each emit uses a distinct observed_at, so nothing here
    is caught by the same-instant ON CONFLICT clause -- every collapse is
    the new probe's doing."""
    name, dk = _unique("seq"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        for step, op in enumerate(sequence):
            _emit_edge(
                conn, name, dk=dk, op=op,
                at=T0 + timedelta(days=step), scope="key",
            )
        conn.commit()
        for op in ("I", "R"):
            expected = 1 if op in sequence else 0
            assert _count(conn, "edge_facts", name, dk, op) == expected


# --------------------------------------------------------------------
# req.emit-outcome-reports-insertion
# --------------------------------------------------------------------


def test_outcome_reports_inserted(okg_migrated: str) -> None:
    """`inserted` reports what Postgres did; `emitted` keeps meaning "the
    fact was acceptable", so a benign duplicate is not miscounted as a
    rejection by callers that count on `emitted`."""
    with psycopg.connect(okg_migrated) as conn:
        for scope in DEDUPE_SCOPES:
            name, dk = _unique(f"outcome-{scope}"), _unique("dk")
            for emit in (_emit_node, _emit_edge):
                first = emit(conn, name, dk=dk, at=T0, scope=scope)
                assert first.emitted is True
                assert first.inserted is True
                # Same instant: suppressed under BOTH scopes, by the
                # retained ON CONFLICT clause.
                second = emit(conn, name, dk=dk, at=T0, scope=scope)
                assert second.emitted is True
                assert second.inserted is False
                assert second.rejected_fields == ()
                assert second.held is False
        conn.commit()


# --------------------------------------------------------------------
# req.emit-once-probe-is-indexed-and-guarded-in-ci
# --------------------------------------------------------------------


def test_dedupe_probe_indexes_exist(okg_migrated: str) -> None:
    """Without these the probe scans every chunk of a hypertable holding
    up to 180 days at 7 days per chunk."""
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname, indexdef
              FROM pg_indexes
             WHERE schemaname = 'okg'
               AND indexname IN ('node_facts_dedupe_probe_idx',
                                 'edge_facts_dedupe_probe_idx')
            """,
        )
        found = {name: definition for name, definition in cur.fetchall()}
    assert set(found) == {
        "node_facts_dedupe_probe_idx", "edge_facts_dedupe_probe_idx",
    }
    for definition in found.values():
        # Column order must match the probe's equality predicate.
        assert "(source, dedupe_key, op)" in definition
        # And it must stay PARTIAL: an unconditional btree here would tax
        # every source's admission traffic to serve an enricher-only probe.
        assert "WHERE" in definition and "_enricher:%" in definition


def test_probe_can_use_the_partial_index(okg_migrated: str) -> None:
    """The index is partial, so the planner may only use it if it can
    PROVE the query implies the index predicate. A parameterised
    `source = $1` alone cannot be proven; the probe therefore also spells
    the LIKE out as a constant. If someone removes that redundant-looking
    line, the probe silently degrades to scanning every chunk -- which is
    a performance cliff no functional test would catch. This asserts the
    implication still holds.

    seqscan is disabled so this tests PREDICATE IMPLICATION, not the
    planner's cost preference on a near-empty table -- on a table this
    small the costs of every candidate index are within noise of each
    other, so which one wins is not a meaningful signal.

    The op='R' probe is the discriminating case: the only other index
    that can serve these columns, edge_facts_project_i_fact_idx, is
    itself partial `WHERE op = 'I'` and so cannot answer it. If the
    dedupe probe index is unusable there, nothing is left but a scan.
    Chunk-local index names are the parent name with a
    `_hyper_<n>_<n>_chunk_` prefix, hence the substring match."""
    name, dk = _unique("plan"), _unique("dk")
    with psycopg.connect(okg_migrated) as conn:
        for op in ("I", "R"):
            _emit_edge(conn, name, dk=dk, op=op, at=T0, scope="key")
        conn.commit()
        plans = {}
        with conn.cursor() as cur:
            cur.execute("SET LOCAL enable_seqscan = off")
            for op in ("I", "R"):
                cur.execute(
                    """
                    EXPLAIN SELECT 1 FROM okg.edge_facts
                     WHERE source = %s
                       AND source LIKE '\\_enricher:%%'
                       AND dedupe_key = %s
                       AND op = %s
                    """,
                    (f"_enricher:{name}", dk, op),
                )
                plans[op] = "\n".join(row[0] for row in cur.fetchall())
    # Neither op may fall back to scanning chunks.
    for op, plan in plans.items():
        assert "Seq Scan" not in plan, (op, plan)
    # And the partial index must be provably usable.
    assert "edge_facts_dedupe_probe_idx" in plans["R"], plans["R"]


def test_this_file_runs_in_a_per_pr_lane() -> None:
    """This file is postgres-marked. The broad coverage lane filters
    `not postgres` and the substrate-db lane enumerates its files by
    path, so an unlisted postgres file runs in NO per-PR job and reaches
    only the nightly sweep."""
    lanes = fastyaml.safe_load((ROOT / "tests/qa_lanes.yaml").read_text())
    rel = "tests/substrate/library/enrichers/test_fact_emitter_dedupe_scope.py"
    paths = [
        path
        for step in lanes["lanes"]["substrate-db"]["steps"]
        for path in step.get("paths", [])
    ]
    assert rel in paths


def test_concurrent_probe_costs_a_row_not_a_fact(okg_migrated: str) -> None:
    """The documented degradation, demonstrated rather than asserted in
    prose. The probe is a WHERE NOT EXISTS, not an ON CONFLICT against a
    unique index -- Timescale forbids one that omits the partitioning
    column -- so two overlapping transactions can both find the key
    absent. The cost is ONE EXTRA ROW, which is what every caller gets
    today with no emit-once mode at all: no fact is lost."""
    name, dk = _unique("race"), _unique("dk")
    # Create the T0 and T1 chunks first, committed, under another producer.
    # If `a` had to create a chunk inside its open transaction, `b`'s
    # insert would wait for `a`'s chunk-creation lock on okg.edge_facts --
    # and this one thread never reaches `a.commit()`, so the test would
    # hang until the job timeout instead of failing. It relied on earlier
    # tests in this file having created those chunks; run alone, on a
    # fresh database, or after a retention run dropped the chunks, it hung.
    with psycopg.connect(okg_migrated) as setup:
        for at in (T0, T1):
            _emit_edge(setup, _unique("race-chunk"), dk=_unique("dk"), at=at)
        setup.commit()
    with (
        psycopg.connect(okg_migrated) as a,
        psycopg.connect(okg_migrated) as b,
    ):
        # Should the two transactions ever block each other again, fail
        # with LockNotAvailable instead of waiting forever.
        for conn in (a, b):
            conn.execute("SET lock_timeout = '30s'")
        # Neither transaction has committed, so neither probe can see the
        # other's row. Distinct observed_at, so the ON CONFLICT backstop
        # does not apply either.
        first = _emit_edge(a, name, dk=dk, at=T0, scope="key")
        second = _emit_edge(b, name, dk=dk, at=T1, scope="key")
        a.commit()
        b.commit()
        assert first.inserted is True
        assert second.inserted is True
        with psycopg.connect(okg_migrated) as reader:
            assert _count(reader, "edge_facts", name, dk) == 2
