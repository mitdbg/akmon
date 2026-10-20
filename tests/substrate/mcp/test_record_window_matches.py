"""PostgreSQL event positions must equal its original-string ordinal engine."""

from types import SimpleNamespace

import psycopg
from psycopg.rows import dict_row
import pytest

from okg.substrate.mcp.record_windows import (
    MatchEvent,
    WindowWorkBound,
    scan_chain_matches,
)


@pytest.fixture
def window_pg(okg_migrated):
    with psycopg.connect(okg_migrated, autocommit=True, row_factory=dict_row) as conn:
        yield conn


def term(pattern):
    return SimpleNamespace(kind="regex", pg_regex=pattern, negated=False)


@pytest.mark.parametrize("text, pattern", [
    ("alpha beta alpha", "alpha"),
    ("alpha beta alpha", "alpha$"),
    ("alpha beta alpha", "^alpha"),
    ("alpha\nbeta\nalpha", "^alpha|alpha$"),
    ("é甲🙂 é甲🙂", "é甲🙂"),
    ("ALPHA alpha Alpha", "alpha"),
    ("abc abc", "a(b)c"),
    ("alpha beta alpha\nbeta", "alpha[[:space:]]+beta"),
    ("banana", "ana"),
    ("word words word", r"\mword\M"),
    ("alpha gap gap beta", r"\malpha(?:[[:space:]]+[^[:space:]]+){0,3}[[:space:]]+beta\M"),
    ("abc", "^|$"),
    ("abc", "a*"),
    ("", "^$"),
])
def test_event_positions_equal_postgresql_ordinal_reference(window_pg, text, pattern):
    result = scan_chain_matches(window_pg, [term(pattern)], [text])
    expected = []
    with window_pg.cursor(row_factory=psycopg.rows.tuple_row) as cursor:
        for occurrence in range(1, 30):
            cursor.execute(
                "SELECT regexp_instr(%s, %s, 1, %s, 0, 'i'), "
                "regexp_instr(%s, %s, 1, %s, 1, 'i')",
                (text, pattern, occurrence, text, pattern, occurrence),
            )
            start, end = cursor.fetchone()
            if start == 0:
                break
            expected.append(MatchEvent(0, start - 1, end - 1))
    assert list(result.events[0]) == expected
    assert result.match_events == len(expected)
    assert result.source_bytes == len(text.encode("utf-8"))


def test_distinct_terms_at_same_span_retain_attribution(window_pg):
    result = scan_chain_matches(window_pg, [term("alpha"), term("alpha")], ["alpha"])
    assert result.events == ((MatchEvent(0, 0, 5), MatchEvent(1, 0, 5)),)
    assert result.match_events == 2


def test_event_bound_refuses_before_unbounded_split_array(window_pg):
    with pytest.raises(WindowWorkBound) as error:
        scan_chain_matches(window_pg, [term("alpha")], ["alpha " * 10000], max_events=20)
    assert error.value.data["bound"] == "match_events"
    # The failed call must leave the outer session usable and settings intact.
    assert window_pg.execute("SELECT 1 AS n").fetchone()["n"] == 1


def test_empty_match_bound_also_refuses(window_pg):
    with pytest.raises(WindowWorkBound) as error:
        scan_chain_matches(window_pg, [term("a*")], ["bcdef"], max_events=2)
    assert error.value.data["bound"] == "match_events"


def test_statement_timeout_is_restored(window_pg):
    window_pg.execute("SELECT set_config('statement_timeout', '3500ms', false)")
    scan_chain_matches(window_pg, [term("alpha")], ["alpha beta"])
    assert window_pg.execute("SHOW statement_timeout").fetchone()["statement_timeout"] == "3500ms"


def test_stricter_caller_timeout_is_never_relaxed_and_cancel_restores_it(window_pg):
    window_pg.execute("SELECT set_config('statement_timeout', '5ms', false)")
    with pytest.raises(WindowWorkBound) as error:
        scan_chain_matches(window_pg, [term("alpha")], ["alpha " * 200000])
    assert error.value.data["bound"] == "source_time"
    assert window_pg.execute("SHOW statement_timeout").fetchone()["statement_timeout"] == "5ms"
    assert window_pg.execute("SELECT 1 AS n").fetchone()["n"] == 1


def test_late_negative_events_are_returned_in_the_same_domain(window_pg):
    negative = SimpleNamespace(kind="word", pg_regex="forbidden", negated=True)
    result = scan_chain_matches(window_pg, [term("alpha"), negative], ["alpha", "late forbidden"])
    assert result.events[0] == (MatchEvent(0, 0, 5),)
    assert result.events[1] == (MatchEvent(1, 5, 14),)


def test_dense_permitted_event_scan_is_bounded_and_complete(window_pg):
    result = scan_chain_matches(window_pg, [term("alpha")], ["alpha " * 2000])
    assert result.match_events == 2000
    assert result.events[0][-1] == MatchEvent(0, 11994, 11999)
    assert result.elapsed_ms <= 2000


@pytest.mark.postgres
def test_many_chains_use_bounded_cursor_execute_calls_without_losing_pairs(okg_migrated):
    calls = []

    class RecordedCursor(psycopg.Cursor):
        def execute(self, *args, **kwargs):
            calls.append(args[0])
            return super().execute(*args, **kwargs)

    # The fixture's own DSN: `conn.info.dsn` omits the password, so a
    # reconnect from it fails on a password-auth server.
    with psycopg.connect(
        okg_migrated, autocommit=True, cursor_factory=RecordedCursor,
    ) as conn:
        result = scan_chain_matches(
            conn, [term(word) for word in ("alpha", "beta", "gamma", "delta")],
            ["alpha beta gamma delta"] * 1000,
        )
    assert result.match_events == 4000
    assert len(result.events) == 1000
    assert result.events[0] == result.events[-1] == (
        MatchEvent(0, 0, 5), MatchEvent(1, 6, 10),
        MatchEvent(2, 11, 16), MatchEvent(3, 17, 22),
    )
    # This measures actual cursor execute calls (including setting management),
    # excluding transaction protocol commands and the later public read path.
    assert len(calls) <= 12


def test_global_event_cap_is_not_one_independent_cap_per_pair(window_pg):
    with pytest.raises(WindowWorkBound) as error:
        scan_chain_matches(window_pg, [term("alpha"), term("beta")], ["alpha beta"] * 10, max_events=15)
    assert error.value.data["bound"] == "match_events"


def test_zero_event_pairs_are_examined_without_becoming_missing_arrays(window_pg):
    result = scan_chain_matches(
        window_pg, [term("alpha"), term("absent")], ["no requested text", "alpha", ""],
    )
    assert result.events == ((), (MatchEvent(0, 0, 5),), ())


def test_mixed_zero_and_nonzero_pairs_retain_exact_identities(window_pg):
    result = scan_chain_matches(window_pg, [term("^|$"), term("alpha")], ["alpha", "beta"])
    assert result.events == (
        (MatchEvent(0, 0, 0), MatchEvent(1, 0, 5), MatchEvent(0, 5, 5)),
        (MatchEvent(0, 0, 0), MatchEvent(0, 4, 4)),
    )
