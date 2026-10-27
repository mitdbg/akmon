"""No MCP read reads the three materialized copies (PostgreSQL).

PACT lane-s-storage-cutover, requirements ``no-copy-reads`` and
``date-forms-and-text-rest-graph``. The graph read path is the only read
path: every effective-attribute read takes the Chronos rows at the pin plus
the generation's overlays. Publish still writes the copies until stage S3
(``okg.attribute_versions``, ``okg.attribute_effective_view_intervals``,
``okg.attribute_effective_view``); here they are renamed away before the
reads, so a read that still names one fails with an undefined table, and
every statement the reads send is checked for their names too.

The calls cover the two readers that read the copies on both paths in S2:
the stored date forms of a date condition (``_read_date_forms``) and the
records a text clause is checked within (``_text_rest_ids``), together in
one filter whose text clause no index can narrow. filter, map, aggregate,
expand and the search supplement run beside them. Failure cases: the name
check flags each copy table; a pin with no overlay marker is refused by
name before any copy could be read.
"""
from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    OVERLAY_GENERATION_MARKER_TABLE,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.errors import SubstrateError
from tests.substrate.mcp.test_date_conditions import (
    IN_2025,
    YEAR_BY_DAY,
    _publish,
    _selected,
)

COPY_TABLES = (
    "okg.attribute_versions",
    "okg.attribute_effective_view_intervals",
    "okg.attribute_effective_view",
)
_COPY = re.compile(
    r"\b(okg\.)?attribute_(versions|effective_view_intervals|effective_view)\b"
)
#: S2's overlay validity ranges: written by publish, read by no MCP reader
#: (the reads take the per-generation overlay rows behind the marker), so a
#: generation whose ranges were never synced (a structural fork with a
#: recorded base) reads like any other. Hidden by the test; the copies are
#: already dropped (migration 0046).
RANGES_TABLE = "okg.attribute_overlay_ranges"
HIDDEN_TABLES = (RANGES_TABLE,)
#: No run of three letters or digits, so no text index narrows it: the text
#: is checked within the records the rest of the condition admits
#: (``_text_rest_ids``), and that rest holds a date condition, whose stored
#: forms are read first (``_read_date_forms``). Every seeded text is
#: "dated record <id>".
TEXT = {"op": "matches", "field": "text", "value": "re:/d.t.d/"}
TICKETS = {"kind": "nodes", "subtype": "ticket"}

pytestmark = pytest.mark.postgres


def names_a_copy(sql: str) -> bool:
    return bool(_COPY.search(sql))


def test_the_name_check_flags_each_copy_table_and_not_the_overlay_table() -> None:
    for table in COPY_TABLES:
        assert names_a_copy(f"SELECT 1 FROM {table} AS v"), table
        assert names_a_copy(f"JOIN {table.split('.', 1)[1]} USING (target_id)"), table
    assert not names_a_copy("SELECT 1 FROM okg.attribute_effective_attrs")
    assert not names_a_copy("SELECT 1 FROM okg.attribute_overlay_generations")


class _Statements:
    """Every statement any cursor sends while open."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.sent: list[str] = []
        real = psycopg.Cursor.execute
        sent = self.sent

        def execute(cur: Any, query: Any, params: Any = None, **kwargs: Any) -> Any:
            sent.append(query if isinstance(query, str) else str(query))
            return real(cur, query, params, **kwargs)

        monkeypatch.setattr(psycopg.Cursor, "execute", execute)

    def copies(self) -> list[str]:
        return [s[:160] for s in self.sent if names_a_copy(s)]

    def ranges(self) -> list[str]:
        return [s[:160] for s in self.sent if "attribute_overlay_ranges" in s]


def _hide_copies(dsn: str) -> None:
    """Check the copies are gone, rename the overlay ranges away, and mark
    every generation's ranges unsynced, as a structural fork's are."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        for table in COPY_TABLES:
            assert conn.execute(
                "SELECT to_regclass(%s)", (table,),
            ).fetchone()[0] is None, table
        conn.execute("UPDATE okg.attribute_overlay_generations SET ranges_synced = FALSE")
        for table in HIDDEN_TABLES:
            name = table.split(".", 1)[1]
            conn.execute(f"ALTER TABLE {table} RENAME TO {name}_hidden_by_test")


def _restore_copies(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        for table in HIDDEN_TABLES:
            name = table.split(".", 1)[1]
            if conn.execute(
                "SELECT to_regclass(%s)", (f"okg.{name}_hidden_by_test",),
            ).fetchone()[0] is not None:
                conn.execute(f"ALTER TABLE okg.{name}_hidden_by_test RENAME TO {name}")


@contextmanager
def _tools(dsn: str, deployment: str) -> Iterator[dict[str, Any]]:
    """A fresh server: its date-scope memo is empty, so a date condition's
    stored forms are read again (they are remembered per server)."""
    from okg.substrate.mcp import server as srv

    mcp = srv.build_server(dsn=dsn, deployment_name=deployment)
    try:
        yield mcp._tool_manager._tools
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@pytest.fixture
def served(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path) -> Iterator[dict[str, Any]]:
    deployment = _publish(tmp_path, clean_runtime)
    try:
        yield {"dsn": clean_runtime, "deployment": deployment}
    finally:
        _restore_copies(clean_runtime)


def _calls(tools: dict[str, Any]) -> dict[str, Any]:
    """One answer per read, keyed by what it is; each must be served."""
    filtered = tools["filter"].fn(
        input=TICKETS, predicate={"op": "and", "clauses": [TEXT, IN_2025]}, limit=100,
    )
    return {
        "text_and_date": filtered,
        "date_filter": tools["filter"].fn(input={"kind": "nodes"}, predicate=IN_2025, limit=100),
        "map": tools["map"].fn(
            input={"kind": "nodes", "filter": IN_2025}, projection=["node_id"], limit=100),
        "aggregate": tools["aggregate"].fn(
            input={"kind": "nodes", "filter": IN_2025}, by=["subtype"], metrics=["count"]),
        "aggregate_text": tools["aggregate"].fn(
            input={"kind": "nodes", "subtype": "ticket",
                   "filter": {"op": "and", "clauses": [TEXT, IN_2025]}},
            by=["subtype"], metrics=["count"]),
        "expand": tools["expand"].fn(input={"node_id": "board:hub"}, filter=IN_2025, limit=100),
        "supplement": tools["search"].fn(
            query="dated record", method="lexical",
            attr_filters={"status": "open"}, limit=50),
    }


def _facts(answers: dict[str, Any]) -> dict[str, Any]:
    def ids(page: dict[str, Any], key: str = "items") -> list[str]:
        assert key in page, (key, str(page)[:400])
        return sorted(str(row["node_id"]) for row in page[key])

    return {
        "text_and_date": ids(answers["text_and_date"]),
        "date_filter": ids(answers["date_filter"]),
        "map": ids(answers["map"]),
        "aggregate": sorted((g["subtype"], g["count"]) for g in answers["aggregate"]["groups"]),
        "aggregate_text": sorted(
            (g["subtype"], g["count"]) for g in answers["aggregate_text"]["groups"]),
        "expand": ids(answers["expand"], "neighbors"),
        "supplement": ids(answers["supplement"], "results"),
    }


def test_every_read_is_served_with_the_copies_gone(
    served: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn, deployment = served["dsn"], served["deployment"]
    with _tools(dsn, deployment) as tools:
        before = _facts(_calls(tools))
    expected = _selected(*YEAR_BY_DAY)
    tickets = [n for n in expected if n.startswith("ticket:")]
    assert before["text_and_date"] == tickets
    assert before["date_filter"] == before["map"] == before["expand"] == expected
    assert before["supplement"], "the supplement read found the open records"

    _hide_copies(dsn)
    statements = _Statements(monkeypatch)
    budgets: list[Any] = []
    from okg.substrate.mcp import server as srv

    monkeypatch.setattr(srv, "_DATE_BUDGET_OBSERVER", budgets.append)
    with _tools(dsn, deployment) as tools:
        answers = _calls(tools)
    # Neither the copies nor the overlay ranges (all marked unsynced) are read.
    assert statements.copies() == [] and statements.ranges() == []
    assert _facts(answers) == before
    # The text was checked within the rest set (not an index read), and the
    # date condition's stored forms were read for it.
    assert answers["text_and_date"].get("text_match", {}).get("scan") == "set", (
        answers["text_and_date"].get("text_match"))
    labels = [label for budget in budgets for label, _ms in budget.statements]
    assert "stored values of date.created" in labels, labels


def test_a_pin_with_no_overlay_marker_is_refused_by_name_with_the_copies_gone(
    served: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failure case: the date forms and text reads refuse an unmarked pin
    by name. Had any of them read a copy first, the hidden table would
    raise an undefined-table error instead."""
    dsn, deployment = served["dsn"], served["deployment"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        # The fixture published one generation; its marker goes.
        deleted = conn.execute(f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE}").rowcount
    assert deleted >= 1
    _hide_copies(dsn)
    statements = _Statements(monkeypatch)
    with _tools(dsn, deployment) as tools, pytest.raises(SubstrateError) as refused:
        tools["filter"].fn(
            input=TICKETS, predicate={"op": "and", "clauses": [TEXT, IN_2025]}, limit=100,
        )
    assert refused.value.code == "overlay_generation_unmaterialized", refused.value
    # The refusal names its cause and the read that serves.
    assert refused.value.data["cause"] == "overlay_step_not_run", refused.value.data
    assert "okg generation list" in (refused.value.fix_hint or ""), refused.value.fix_hint
    assert "OKG_EFFECTIVE_READ_PATH" not in (refused.value.fix_hint or "")
    assert statements.copies() == [] and statements.ranges() == []


def test_overlays_reach_the_text_rest_and_the_date_forms(
    served: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review S2: the moved readers read the overlay rows too. Two accepted
    overlays, recorded after publish: one moves `ticket:before` into the
    date condition (an ISO-8601 value inside 2025), and one gives
    `ticket:word` ("soon") a stored form SQL does not read by itself
    (RFC 2822, inside 2025), which only the stored date forms find. The
    text-and-date filter, whose text is checked within the rest set
    (`_text_rest_ids`) after the stored forms are read (`_read_date_forms`),
    returns both, with the copies and the overlay ranges renamed away. A
    reader that dropped the overlay-id branch misses both."""
    dsn, deployment = served["dsn"], served["deployment"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        [(generation,)] = conn.execute(
            f"SELECT generation_id FROM {OVERLAY_GENERATION_MARKER_TABLE}"
        ).fetchall()
        for target, value in (("ticket:before", "2025-05-05T00:00:00Z"),
                              ("ticket:word", "Fri, 07 Mar 2025 10:00:00 +0000")):
            record_attribute_observation(
                conn, target_kind="node", target_id=target, generation_id=generation,
                attr_key="created", value=value, producer="test:graph-only-readers",
            )
        materialize_effective_attributes_for_generation(conn, generation_id=generation)
    _hide_copies(dsn)
    statements = _Statements(monkeypatch)
    budgets: list[Any] = []
    from okg.substrate.mcp import server as srv

    monkeypatch.setattr(srv, "_DATE_BUDGET_OBSERVER", budgets.append)
    with _tools(dsn, deployment) as tools:
        page = tools["filter"].fn(
            input=TICKETS, predicate={"op": "and", "clauses": [TEXT, IN_2025]}, limit=100,
        )
    assert statements.copies() == [] and statements.ranges() == []
    stored = [n for n in _selected(*YEAR_BY_DAY) if n.startswith("ticket:")]
    assert "ticket:before" not in stored and "ticket:word" not in stored
    ids = sorted(str(row["node_id"]) for row in page["items"])
    assert ids == sorted([*stored, "ticket:before", "ticket:word"]), ids
    assert page.get("text_match", {}).get("scan") == "set", page.get("text_match")
    labels = [label for budget in budgets for label, _ms in budget.statements]
    assert "stored values of date.created" in labels, labels
