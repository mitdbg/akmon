"""grep `within="record"`, the sweep, and id-scoped text conditions (PACT read-search-caps-3666).

Through a real server and publish. A small graph of tickets with comment
passages: one ticket holds a query's two terms in two different units (its
own text and a comment, or two comments), so grep's unit form cannot find it
and the record form does. A text condition scoped to two node ids checks
those records only, and one whose index read times out reads the rest of
the condition's records instead, saying so.

At scale: 50,000 tickets inserted into the live node segment beside the
published ones, every 100th holding the swept words. The sweep (record form,
head-only select) returns under 5 s through the trigram index and through
the node-id walk, and its second page continues the first, by offset and by
token. A walk bound stops a page with a token that resumes after the last
record decided.
"""
from __future__ import annotations

import pathlib
import time
from typing import Any

import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import grep_record
from okg.substrate.mcp import text_match as tm
from okg.substrate.mcp.grep_search import resolve_trgm_segment
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp._result_payload import JSON_META, payload, result_text
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_page_next_db import _drive

pytestmark = pytest.mark.postgres

TICKET = SubtypeSpec("ticket", metadata={
    "searchable_text_fields": ["text"],
    "passage_rollup": {"edge_type": "contains", "child_subtype": "comment"},
})
COMMENT = SubtypeSpec("comment", metadata={"searchable_text_fields": ["text"]})
NARROWINGS = [("ticket", "contains", "comment")]

#: Its two terms in two units: the own text and a comment.
SPLIT = "ticket:split"
#: Both terms in one comment: grep's unit form finds it.
BOTH = "ticket:both"
#: Both terms, and the excluded word in another comment.
EXCLUDED = "ticket:excluded"
OTHER = "ticket:other"
OWNED_A, OWNED_B = "ticket:owned:a", "ticket:owned:b"
QUERY = "re:/paged.attn/ workspace"


def _node(node_id: str, subtype: str, text: str, **attrs: Any) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs={"text": text, **attrs},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _ticket(node_id: str, text: str, comments: list[str]) -> list[Any]:
    facts: list[Any] = [_node(node_id, "ticket", text, title=node_id)]
    for i, body in enumerate(comments):
        comment_id = f"{node_id}:c{i}"
        facts.append(_node(comment_id, "comment", body, chunk_ordinal=i))
        facts.append(EdgeFact(
            src=node_id, dst=comment_id, edge_type="contains",
            source_record_id={"id": f"{comment_id}-e"}, source_revision={"v": 1},
        ))
    return facts


def _facts() -> list[Any]:
    return [
        *_ticket(SPLIT, "Workspace overflow report", [
            "the paged-attn kernel leaked", "seen under load at 2am",
        ]),
        *_ticket(BOTH, "Kernel report", ["paged-attn needs a bigger workspace"]),
        *_ticket(EXCLUDED, "Another report", [
            "paged-attn workspace again", "closed as wontfix",
        ]),
        *_ticket(OTHER, "nothing to see", ["routine note"]),
        *_ticket(OWNED_A, "Postmortem.\nOwner team: storage.\nDone.", []),
        *_ticket(OWNED_B, "Postmortem.\nOwner team: network.\nDone.", []),
    ]


def _publish(dsn: str, tmp_path: pathlib.Path, extra: list[Any] | None = None) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    facts = [*_facts(), *(extra or [])]
    publish_nodes(
        dsn, deployment, subtypes=[TICKET, COMMENT], narrowings=NARROWINGS,
        nodes=[f for f in facts if isinstance(f, NodeFact)],
        edges=[f for f in facts if isinstance(f, EdgeFact)],
    )
    return deployment


@pytest.fixture
def deployment(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    return _publish(clean_runtime, tmp_path)


async def _json(client: Any, arguments: dict, tool: str = "search") -> dict:
    result = await client.call_tool(tool, arguments, meta=JSON_META)
    assert not result.isError, result_text(result)
    return payload(result)


async def _text(client: Any, arguments: dict, tool: str = "search") -> str:
    result = await client.call_tool(tool, arguments)
    assert not result.isError, result_text(result)
    return result_text(result)


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def test_terms_in_two_units_are_found_within_record_and_not_within_unit(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        unit = await _json(client, {"query": QUERY, "method": "grep"})
        record = await _json(
            client, {"query": QUERY, "method": "grep", "within": "record"},
        )
        # Failure case: grep's unit form never finds the split record.
        assert SPLIT not in _ids(unit)
        assert set(_ids(unit)) == {BOTH, EXCLUDED}
        assert unit["and_scope"] == "text_unit"
        # The record form finds it, in node-id order, exactly counted.
        assert _ids(record) == sorted([BOTH, EXCLUDED, SPLIT])
        assert record["and_scope"] == "record" and record["within"] == "record"
        assert record["total_count"] == 3 and record["total_count_basis"] == "exact"
        # On a graph this small PostgreSQL reads the table, not the trigram
        # index, so the scope is walked; the scale tests below cover both.
        assert record["record_scan"]["mode"] in ("index", "walk")
        split =next(row for row in record["results"] if row["node_id"] == SPLIT)
        assert split["match_count"] == 2
        units = {(line["unit"], line.get("unit_place")) for line in split["match_lines"]}
        assert units == {("text", None), ("comment", 0)}
        # An excluded word in any unit leaves the record out.
        without = await _json(
            client, {"query": QUERY + " -wontfix", "method": "grep", "within": "record"},
        )
        assert _ids(without) == sorted([BOTH, SPLIT])
        # The text form: the mode on the first line, labelled lines, the count.
        text = await _text(client, {"query": QUERY, "method": "grep", "within": "record"})
        first = text.splitlines()[0]
        assert "within record" in first and "within unit" not in first
        assert "2 matches" in text
        # Each matched line whole, labelled with its unit (a fence may wrap
        # the source's words).
        assert "text L1: " in text and "Workspace overflow report" in text
        assert "comment 0 L1: " in text and "the paged-attn kernel leaked" in text
        unit_text = await _text(client, {"query": QUERY, "method": "grep"})
        assert "within unit" in unit_text.splitlines()[0]

    _drive(clean_runtime, deployment, body)


def test_the_sweep_is_head_lines_with_match_counts(clean_runtime: str, deployment: str) -> None:
    async def body(client: Any) -> None:
        arguments = {
            "query": "re:/paged.attn|workspace/", "method": "grep", "within": "record",
            "select": ["node_id", "handle"], "where": {"type": "ticket"},
        }
        page = await _json(client, arguments)
        assert _ids(page) == sorted([BOTH, EXCLUDED, SPLIT])
        for row in page["results"]:
            assert "match_lines" not in row and row["match_count"] >= 1
        text = await _text(client, arguments)
        assert " L1:" not in text
        assert f"[3] {SPLIT}" in text.replace('"', "")

    _drive(clean_runtime, deployment, body)


@pytest.mark.parametrize(("arguments", "named"), [
    ({"query": "path:src paged", "method": "grep"}, "path: term"),
    ({"query": "-paged", "method": "grep"}, "exclusions only"),
    ({"query": "paged", "method": "grep", "view": "windows"}, "does not read view"),
    ({"query": "paged", "method": "lexical"}, "grep form"),
])
def test_record_form_refusals_name_the_argument(
    clean_runtime: str, deployment: str, arguments: dict, named: str,
) -> None:
    async def body(client: Any) -> None:
        result = await client.call_tool("search", {**arguments, "within": "record"})
        assert result.isError
        assert named in result_text(result)

    _drive(clean_runtime, deployment, body)


AGGREGATE_REGEX = "re:/Owner team: [^.\\n]+/"


def test_a_text_condition_scoped_to_ids_reads_only_those_ids(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 2-id aggregate that timed out on the hard-50 run: the index read
    over the whole graph ran before the ids were used."""
    calls: list[Any] = []
    original = tm.matching_record_ids

    def spy(*args: Any, **kwargs: Any) -> list[str]:
        calls.append(kwargs.get("record_ids"))
        return original(*args, **kwargs)

    monkeypatch.setattr(tm, "matching_record_ids", spy)

    async def body(client: Any) -> None:
        text = await _text(client, {
            "where": {"node_id": [OWNED_A, OWNED_B], "text": {"op": "matches", "value": AGGREGATE_REGEX}},
            "by": ["match"], "metrics": ["count"],
        }, tool="aggregate")
        assert "Owner team: storage" in text and "Owner team: network" in text

    _drive(clean_runtime, deployment, body)
    assert calls and all(ids is not None for ids in calls), calls
    assert sorted(calls[0]) == [OWNED_A, OWNED_B]


def test_an_index_read_timeout_reads_the_rest_instead_and_says_so(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = tm.matching_record_ids

    def slow_index(*args: Any, **kwargs: Any) -> list[str]:
        if kwargs.get("record_ids") is None:
            raise tm.timed_out(kwargs.get("tool", "aggregate"))
        return original(*args, **kwargs)

    monkeypatch.setattr(tm, "matching_record_ids", slow_index)
    condition = {"op": "matches", "value": AGGREGATE_REGEX}

    async def body(client: Any) -> None:
        text = await _text(client, {
            "where": {"type": "ticket", "text": condition},
            "by": ["match"], "metrics": ["count"],
        }, tool="aggregate")
        assert "Owner team: storage" in text
        assert "the word index read timed out" in text
        # Failure case: with no rest there is nothing smaller to read.
        refused = await client.call_tool("aggregate", {
            "where": {"text": condition}, "by": ["match"], "metrics": ["count"],
        })
        assert refused.isError and "grep_timeout" in result_text(refused)

    _drive(clean_runtime, deployment, body)


# ---------- at scale ----------

BULK = 50_000
#: Every 100th bulk ticket holds the swept words.
EVERY = 100
#: The one bulk ticket holding the rare word.
RARE_AT = 45_000


def _bulk_id(g: int) -> str:
    return f"bulk:{g:06d}"


@pytest.fixture
def bulk(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    template = _node("ticket:template", "ticket", "template ticket", title="template")
    deployment = _publish(clean_runtime, tmp_path, extra=[template])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment = resolve_trgm_segment(conn)
        conn.execute(
            f"INSERT INTO {segment} (node_id, subtype, attrs, live_lo, live_hi, "
            "writer_segment_id, source) SELECT 'bulk:' || lpad(g::text, 6, '0'), 'ticket', "
            "jsonb_build_object('title', 'Bulk ' || g, 'text', "
            "CASE WHEN g %% %s = 0 THEN 'zeolite gauge ' || g ELSE 'routine bulk ticket ' || g END "
            "|| CASE WHEN g = %s THEN ' xenotime' ELSE '' END), "
            "t.live_lo, t.live_hi, t.writer_segment_id, t.source "
            f"FROM {segment} t CROSS JOIN generate_series(1, %s) g "
            "WHERE t.node_id = 'ticket:template' AND t.deleted = FALSE",
            (EVERY, RARE_AT, BULK),
        )
        conn.execute(f"ANALYZE {segment}")
    return deployment


SWEPT = [_bulk_id(g) for g in range(EVERY, BULK + 1, EVERY)]
SWEEP = {
    "query": "re:/zeolite gauge [0-9]+/", "method": "grep", "within": "record",
    "where": {"type": "ticket"}, "select": ["node_id", "handle"], "limit": 100,
}


def test_a_sweep_of_50000_records_through_the_index_pages_by_offset(
    clean_runtime: str, bulk: str,
) -> None:
    async def body(client: Any) -> None:
        started = time.monotonic()
        first = await _json(client, SWEEP)
        first_s = time.monotonic() - started
        started = time.monotonic()
        second = await _json(client, {**SWEEP, "offset": 100})
        second_s = time.monotonic() - started
        assert first_s < 5 and second_s < 5, (first_s, second_s)
        assert first["record_scan"]["mode"] == "index"
        assert first["total_count"] == len(SWEPT) and first["total_count_basis"] == "exact"
        assert _ids(first) == SWEPT[:100] and first["next_offset"] == 100
        assert _ids(second) == SWEPT[100:200]
        text = await _text(client, SWEEP)
        assert "within record" in text.splitlines()[0]
        assert "next: offset=100" in text.splitlines()[0]

    _drive(clean_runtime, bulk, body)


def test_a_walk_of_50000_records_pages_by_token(
    clean_runtime: str, bulk: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No index read (as for a regex the trigram index cannot serve): the
    scope is walked in node-id order and the next page resumes by token."""
    monkeypatch.setattr(grep_record, "INDEX_TERMS", 0)

    async def body(client: Any) -> None:
        arguments = {**SWEEP, "limit": 50}
        started = time.monotonic()
        first = await _json(client, arguments)
        first_s = time.monotonic() - started
        assert first["record_scan"]["mode"] == "walk"
        assert _ids(first) == SWEPT[:50]
        token = first["continuation_token"]
        started = time.monotonic()
        second = await _json(client, {**arguments, "continuation_token": token})
        second_s = time.monotonic() - started
        assert first_s < 5 and second_s < 5, (first_s, second_s)
        assert _ids(second) == SWEPT[50:100]
        assert second["offset"] == 50

    _drive(clean_runtime, bulk, body)


def test_a_walk_bound_ends_the_page_with_a_token_that_resumes_after_it(
    clean_runtime: str, bulk: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(grep_record, "INDEX_TERMS", 0)
    monkeypatch.setattr(grep_record, "WALK_ROWS", 20_000)

    async def body(client: Any) -> None:
        arguments = {
            "query": "xenotime", "method": "grep", "within": "record",
            "where": {"type": "ticket"},
        }
        first = await _json(client, arguments)
        assert first["results"] == [] and first["page_scan_bound_hit"] == "record_rows"
        text = await _text(client, arguments)
        assert "no match in the first 20,000 records of the scope" in text
        pages = [first]
        while pages[-1].get("continuation_token") and len(pages) < 5:
            pages.append(await _json(
                client, {**arguments, "continuation_token": pages[-1]["continuation_token"]},
            ))
        found = [node_id for page in pages for node_id in _ids(page)]
        assert found == [_bulk_id(RARE_AT)]
        assert pages[-1].get("continuation_token") is None
        assert pages[-1]["page_basis"] == "top_of_all_visible_matches"

    _drive(clean_runtime, bulk, body)
