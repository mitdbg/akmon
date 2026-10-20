"""Issue #2161: the Sources view's two fact-log scans use a literal prefix.

`enricher_diagnostics` counts enricher facts in `okg.node_facts` and
`okg.edge_facts` on every read. In LIKE a bare `_` matches any one
character, so the old pattern `'_enricher:%'` had no fixed prefix: Postgres
could not turn it into an index range and filtered every fact-log row. The
fix escapes the underscore. These tests read the pattern Postgres receives
and evaluate it with LIKE's rules, so they need no database; the plan and
row-count proof against real Postgres lives in
tests/okg-workspace/test_console_view_performance.py.
"""
from __future__ import annotations

import re

import pytest

from okg.substrate.console_views import enricher_diagnostics

_SCANS = {
    "node_facts": enricher_diagnostics._NODE_FACTS_SQL,
    "edge_facts": enricher_diagnostics._EDGE_FACTS_SQL,
}
_LIKE_RE = re.compile(r"source LIKE (E?)'((?:[^'\\]|''|\\.)*)'")


def _like_to_regex(pattern: str) -> re.Pattern[str]:
    """Postgres LIKE with the default escape character (backslash)."""
    out: list[str] = []
    chars = iter(pattern)
    for ch in chars:
        if ch == "\\":
            out.append(re.escape(next(chars)))
        elif ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return re.compile("".join(out), re.DOTALL)


def _pattern(sql: str) -> str:
    """The pattern VALUE Postgres builds from the SQL literal."""
    matches = _LIKE_RE.findall(sql)
    assert len(matches) == 1, sql
    escape_string, body = matches[0]
    value = body.replace("''", "'")
    if escape_string:
        # E'' strings: only the escapes this SQL may use are accepted.
        assert re.fullmatch(r"(?:[^\\]|\\\\)*", value), value
        value = value.replace("\\\\", "\\")
    return value


@pytest.mark.parametrize("table", sorted(_SCANS))
def test_scan_sends_the_escaped_prefix(table: str) -> None:
    sql = _SCANS[table]
    assert f"FROM okg.{table}" in sql
    # An escape string, so the value does not depend on
    # standard_conforming_strings.
    assert "LIKE E'" in sql
    # The value is the partial dedupe-probe index predicate in schema.sql.
    assert _pattern(sql) == "\\_enricher:%"


@pytest.mark.parametrize("table", sorted(_SCANS))
@pytest.mark.parametrize(
    ("source", "counted"),
    [
        ("_enricher:foo", True),
        ("_enricher:", True),
        ("_enricher:a/b:c", True),
        # The old pattern counted these two: its `_` matched any first
        # character. The rows below them neither pattern counts.
        ("xenricher:foo", False),
        ("Aenricher:foo", False),
        ("_enricher_foo", False),
        ("enricher:foo", False),
        ("_extractor:foo", False),
    ],
)
def test_scan_counts_only_enricher_sources(
    table: str, source: str, counted: bool
) -> None:
    regex = _like_to_regex(_pattern(_SCANS[table]))
    assert bool(regex.fullmatch(source)) is counted


def test_unescaped_pattern_is_the_bug_this_guards() -> None:
    """The evaluator must tell the two patterns apart, or the tests above
    prove nothing."""
    assert _like_to_regex("_enricher:%").fullmatch("xenricher:foo")
    assert not _like_to_regex("\\_enricher:%").fullmatch("xenricher:foo")
