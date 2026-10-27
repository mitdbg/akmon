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


# ---------- a profile's passage type (PACT false-complete-record-grep-4220) ----------
#
# The CompOps default profile lists `document_chunk` (a passage type) and not
# `document`, the record type its chunks belong to. The record form dropped
# the passage type from its scope and never added the record type, so
# `within="record"` with no type read none of those records and answered
# "rows 0 of 0 · complete" while `within="unit"` found them (eval run d356,
# q009 and q020). A `where` naming a passage type in a list or under `or` was
# dropped the same way, in both grep forms.

DOC = SubtypeSpec("doc", metadata={
    "searchable_text_fields": ["text"],
    "passage_rollup": {"edge_type": "contains", "child_subtype": "chunk"},
})
CHUNK = SubtypeSpec("chunk", metadata={"searchable_text_fields": ["text"]})
LOCK = "doc:lib/core/lock.py"
RULE = "doc:lib/core/rule.py"
VIEW = "doc:lib/web/view.py"
HELD = "ticket:suspended"
SUSPENDED = "RuleState.SUSPENDED"
FIXTURE_SOURCE = "system:analyst_search_fixture"
PROFILES = {
    "default_profile": "content",
    "profiles": {
        "content": {"subtypes": ["chunk", "ticket"]},
        "chunks": {"subtypes": ["chunk"]},
        "globbed": {"subtypes": ["chunk", "ticket"], "exclude_path_globs": ["lib/web/**"]},
        "docs": {"subtypes": ["doc", "ticket"]},
    },
}
REPORTED = {
    "query": SUSPENDED, "method": "grep", "within": "record",
    "path_prefix": "lib/core/", "select": ["node_id", "handle"],
}
CHUNK_EQ = {"op": "eq", "field": "type", "value": "chunk"}
TICKET_EQ = {"op": "eq", "field": "type", "value": "ticket"}
CHUNK_TICKET_IN = {"op": "in", "field": "type", "values": ["chunk", "ticket"]}
CHUNK_OR_TICKET = {"op": "or", "clauses": [CHUNK_EQ, TICKET_EQ]}
DOC_TICKET_IN = {"op": "in", "field": "type", "values": ["doc", "ticket"]}
NOT_CHUNK = {"op": "not", "clause": CHUNK_EQ}
#: Two negations: `type in [chunk, ticket]` and `chunk or ticket` again.
NOT_NOT_IN = {"op": "not", "clause": {"op": "not", "clause": CHUNK_TICKET_IN}}
NOT_BOTH_NOT = {"op": "not", "clause": {"op": "and", "clauses": [
    {"op": "not", "clause": CHUNK_EQ}, {"op": "not", "clause": TICKET_EQ},
]}}


def _doc(node_id: str, path: str, chunks: list[str]) -> list[Any]:
    facts: list[Any] = [_node(node_id, "doc", f"module {path}", title=path, path=path)]
    for i, body in enumerate(chunks):
        chunk_id = f"{node_id}:k{i}"
        facts.append(_node(chunk_id, "chunk", body, chunk_ordinal=i))
        facts.append(EdgeFact(
            src=node_id, dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"{chunk_id}-e"}, source_revision={"v": 1},
        ))
    return facts


@pytest.fixture
def profiled(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """The ticket graph plus `doc` records whose text is in `chunk`
    passages, under a default profile that lists `chunk` and not `doc`."""
    import json

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    facts = [
        *_facts(),
        *_ticket(HELD, f"Rules stuck in {SUSPENDED} after a quota change", []),
        *_doc(LOCK, "lib/core/lock.py", [
            f"if rule.state == {SUSPENDED}:", f"    rule.state = {SUSPENDED}",
        ]),
        *_doc(RULE, "lib/core/rule.py", [f"{SUSPENDED} is set by the injector"]),
        *_doc(VIEW, "lib/web/view.py", [f"shows {SUSPENDED} rules"]),
    ]
    publish_nodes(
        clean_runtime, deployment, subtypes=[TICKET, COMMENT, DOC, CHUNK],
        narrowings=[*NARROWINGS, ("doc", "contains", "chunk")],
        nodes=[f for f in facts if isinstance(f, NodeFact)],
        edges=[f for f in facts if isinstance(f, EdgeFact)],
    )
    manifest = pathlib.Path(deployment) / "deployment.yaml"
    body = json.loads(manifest.read_text(encoding="utf-8"))
    manifest.write_text(json.dumps({**body, "search": PROFILES}), encoding="utf-8")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    return deployment


def _says_empty_complete(text: str) -> bool:
    first = text.splitlines()[0]
    return "0 of 0" in first and "complete" in first


def _unit(arguments: dict) -> dict:
    return {k: v for k, v in arguments.items() if k not in ("within", "select")}


def _docs_of(page: dict) -> set[str]:
    """The docs a page lists, as records or through their chunks."""
    return {
        row["node_id"].split(":k")[0] for row in page["results"]
        if row["node_id"].startswith("doc:")
    }


@pytest.mark.parametrize(("extra", "expected", "unit_agrees"), [
    ({}, [LOCK, RULE], True),
    ({"path_prefix": None}, [LOCK, RULE, VIEW, HELD], True),
    ({"profile": "content"}, [LOCK, RULE], True),
    ({"profile": "chunks"}, [LOCK, RULE], True),
    ({"profile": "chunks", "path_prefix": None}, [LOCK, RULE, VIEW], True),
    # A row check (a `where`, an excluded path glob) re-checks each record
    # under the types read, not the profile's own list.
    ({"where": {"node_id": [LOCK, RULE, VIEW, HELD]}}, [LOCK, RULE], False),
    ({"profile": "globbed", "path_prefix": None}, [LOCK, RULE, HELD], False),
    ({"where": {"source": FIXTURE_SOURCE}, "path_prefix": None}, [LOCK, RULE, VIEW, HELD], False),
], ids=["reported", "no-path-prefix", "explicit-profile", "passage-only-profile",
        "passage-only-profile-no-path", "where-node-id", "excluded-glob", "where-source"])
def test_a_profiled_passage_type_is_read_through_its_record(
    clean_runtime: str, profiled: str, extra: dict, expected: list[str], unit_agrees: bool,
) -> None:
    """Must-fail on the base: each call answered "0 of 0 · complete" (or
    left the docs out). The record form now reads the records the profile's
    passages belong to, counts them exactly, delivers every type it says it
    searched, and says it widened the profile."""
    arguments = {k: v for k, v in {**REPORTED, **extra}.items() if v is not None}
    profile = extra.get("profile", "content")

    async def body(client: Any) -> None:
        text = await _text(client, arguments)
        assert not _says_empty_complete(text), text
        assert f"! adjusted: profile `{profile}` type `chunk` read as whole `doc` records" in text
        assert "own text and all its passages (`chunk`)" in text, text
        page = await _json(client, arguments)
        assert _ids(page) == sorted(expected)
        assert page["total_count"] == len(expected)
        assert page["total_count_basis"] == "exact"
        searched = page["grep_searched"]["subtypes"]
        assert "doc" in searched and "chunk" not in searched
        # Every type the reply says it searched can be delivered.
        assert {row["subtype"] for row in page["results"]} <= set(searched)
        if unit_agrees:
            unit = await _json(client, _unit(arguments))
            assert _docs_of(unit) == {i for i in expected if i.startswith("doc:")}

    _drive(clean_runtime, profiled, body)


def test_a_passages_only_profile_says_it_reads_the_records_own_text(
    clean_runtime: str, profiled: str,
) -> None:
    """The record form reads whole records: under a profile of chunks only,
    a word held only by a doc's own text (`module`) matches that doc, and
    the reply says the record's own text was read."""
    arguments = {**REPORTED, "query": "module", "profile": "chunks", "path_prefix": "lib/"}

    async def body(client: Any) -> None:
        page = await _json(client, arguments)
        assert _ids(page) == sorted([LOCK, RULE, VIEW])
        text = await _text(client, arguments)
        assert "each record's own text and all its passages" in text, text

    _drive(clean_runtime, profiled, body)


@pytest.mark.parametrize("within", ["record", "unit"])
@pytest.mark.parametrize(
    "where", [CHUNK_TICKET_IN, CHUNK_OR_TICKET, NOT_NOT_IN, NOT_BOTH_NOT],
    ids=["in-list", "under-or", "not-not-in-list", "not-and-of-nots"],
)
def test_a_where_naming_a_passage_type_with_others_is_refused_with_forms_that_run(
    clean_runtime: str, profiled: str, where: dict, within: str,
) -> None:
    """Must-fail on the base: both grep forms judged the passage type on the
    record, where it never holds, and answered with the ticket alone:
    "1 record, all shown" (unit) or "rows 1-1 of 1 · complete" (record).
    Both now refuse, and each fix the refusal names runs and finds the docs
    or their chunks."""
    sent = {"query": SUSPENDED, "method": "grep", "where": where}
    if within == "record":
        sent["within"] = "record"

    async def body(client: Any) -> None:
        result = await client.call_tool("search", sent)
        text = result_text(result)
        assert result.isError, text
        assert "unsupported_operator_form" in text
        assert "name the record type `doc` instead of `chunk`" in text
        assert "send where type = `chunk` as the only type condition" in text
        # Fix 1: the record type instead, in the same form.
        records = await _json(client, {**sent, "where": DOC_TICKET_IN})
        assert {LOCK, RULE, VIEW} <= _docs_of(records)
        assert HELD in _ids(records)
        # Fix 2: the passage type alone, unit form: its passages.
        chunks = await _json(client, {"query": SUSPENDED, "method": "grep", "where": CHUNK_EQ})
        assert _docs_of(chunks) == {LOCK, RULE, VIEW}
        assert all(row["subtype"] == "chunk" for row in chunks["results"])

    _drive(clean_runtime, profiled, body)


@pytest.mark.parametrize("within", ["record", "unit"])
def test_under_an_explicit_profile_the_refusal_names_profile_all_and_it_runs(
    clean_runtime: str, profiled: str, within: str,
) -> None:
    """Under `profile="content"` a `where` naming `doc` is refused
    `subtype_outside_profile`, so the refusal says to pass profile "all"
    too, and that call runs."""
    sent = {"query": SUSPENDED, "method": "grep", "where": CHUNK_TICKET_IN, "profile": "content"}
    if within == "record":
        sent["within"] = "record"

    async def body(client: Any) -> None:
        result = await client.call_tool("search", sent)
        text = result_text(result)
        assert result.isError and "unsupported_operator_form" in text, text
        assert 'pass profile="all"' in text, text
        fixed = await _json(client, {**sent, "where": DOC_TICKET_IN, "profile": "all"})
        assert {LOCK, RULE, VIEW} <= _docs_of(fixed) and HELD in _ids(fixed)

    _drive(clean_runtime, profiled, body)


@pytest.mark.parametrize("profile", [None, "docs"], ids=["default-profile", "docs-profile"])
def test_not_a_passage_type_drops_nothing_and_is_served(
    clean_runtime: str, profiled: str, profile: str | None,
) -> None:
    """`not type = chunk` is true of every record, so it drops nothing: it
    is served, under the default profile (passage mapped to `doc`) and
    under a profile that lists `doc` itself (as on dev)."""
    arguments = {
        "query": SUSPENDED, "method": "grep", "within": "record", "where": NOT_CHUNK,
        **({"profile": profile} if profile else {}),
    }

    async def body(client: Any) -> None:
        page = await _json(client, arguments)
        assert _ids(page) == sorted([LOCK, RULE, VIEW, HELD])
        assert page["total_count"] == 4 and page["total_count_basis"] == "exact"

    _drive(clean_runtime, profiled, body)


def test_a_record_type_with_the_record_form_still_reads_exactly(
    clean_runtime: str, profiled: str,
) -> None:
    """Control: naming the record type was never affected. Same records,
    exact count, a complete page, nothing adjusted."""
    arguments = {**REPORTED, "where": {"type": "doc"}}

    async def body(client: Any) -> None:
        page = await _json(client, arguments)
        assert _ids(page) == [LOCK, RULE]
        assert page["total_count"] == 2 and page["total_count_basis"] == "exact"
        assert page["coverage"]["delivery"]["complete"] is True
        text = await _text(client, arguments)
        assert "complete" in text.splitlines()[0]
        assert "grep_record_passage_profile" not in text and "whole `doc`" not in text

    _drive(clean_runtime, profiled, body)


def test_access_rules_refuse_the_profiled_passage_shape_before_any_read(
    clean_runtime: str, profiled: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nomos `enforce`: the record form reads stored text before the
    reader's check, so it is refused before any read, also for the scope a
    profile's passage type widens. No row, no count, nothing hidden reached."""
    from okg.substrate.mcp import grep_record as gr
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.search_profiles import ResolvedScope
    from tests.substrate.mcp.test_search_records import READER, _activate, _allow

    _activate(clean_runtime, profiled, [_allow("connector:nothing-allowed")])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    reads: list[Any] = []
    for name in ("_index_candidates", "_walk", "_decide", "_record_rows"):
        original = getattr(gr, name)

        def spy(*args: Any, _original: Any = original, _name: str = name, **kwargs: Any) -> Any:
            reads.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(gr, name, spy)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=profiled, nomos_subject=READER)
        with pytest.raises(Exception) as refused:
            search(
                conn, session, SUSPENDED, method="grep", within="record",
                path_prefix="lib/core/",
                scope=ResolvedScope(profile="content", subtypes=("chunk", "ticket")),
            )
    assert getattr(refused.value, "code", None) == "unsupported_operator_form"
    assert refused.value.data["reason"] == "access_rules"
    assert reads == []


@pytest.mark.parametrize("profile", [None, "content"], ids=["default-profile", "explicit-profile"])
def test_the_lone_passage_type_refusal_names_a_fix_that_runs_under_the_profile(
    clean_runtime: str, profiled: str, profile: str | None,
) -> None:
    """`within="record"` with `where type = chunk` is refused (a passage is
    read through its record). Under an explicit profile that leaves `doc`
    out, naming `doc` alone is refused `subtype_outside_profile`, so the
    hint adds profile "all"; under the default profile it does not need to.
    Both named fixes run."""
    profiled_args = {"profile": profile} if profile else {}
    sent = {"query": SUSPENDED, "method": "grep", "within": "record",
            "where": CHUNK_EQ, **profiled_args}

    async def body(client: Any) -> None:
        result = await client.call_tool("search", sent)
        text = result_text(result)
        assert result.isError and "unsupported_operator_form" in text, text
        assert "drop within to list the passages" in text
        assert ("pass profile='all'" in text) is (profile is not None), text
        fixed = {**sent, "where": {"type": "doc"}, **({"profile": "all"} if profile else {})}
        records = await _json(client, fixed)
        assert _ids(records) == sorted([LOCK, RULE, VIEW])
        unit = {k: v for k, v in sent.items() if k != "within"}
        chunks = await _json(client, unit)
        assert _docs_of(chunks) == {LOCK, RULE, VIEW}

    _drive(clean_runtime, profiled, body)


def test_access_rules_are_refused_before_a_where_passage_type(
    clean_runtime: str, profiled: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under Nomos `enforce` the record form is refused `access_rules` first,
    so a `where` naming a passage type beside another type is not sent to a
    record-form fix that would be refused anyway. Nothing is read."""
    from okg.substrate.mcp import grep_record as gr
    from okg.substrate.mcp import open_session, search
    from tests.substrate.mcp.test_search_records import READER, _activate, _allow

    _activate(clean_runtime, profiled, [_allow("connector:nothing-allowed")])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    reads: list[Any] = []
    for name in ("_index_candidates", "_walk", "_decide", "_record_rows"):
        original = getattr(gr, name)

        def spy(*args: Any, _original: Any = original, _name: str = name, **kwargs: Any) -> Any:
            reads.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(gr, name, spy)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=profiled, nomos_subject=READER)
        with pytest.raises(Exception) as refused:
            search(
                conn, session, SUSPENDED, method="grep", within="record",
                where=CHUNK_TICKET_IN,
            )
    assert getattr(refused.value, "code", None) == "unsupported_operator_form"
    assert refused.value.data["reason"] == "access_rules"
    assert reads == []
