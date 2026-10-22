"""aggregate merges the variants of one value (PACT read-aggregate-variants-3666).

Through a real server and publish. Postmortem tickets list action items
with an owning team written several ways ("Owner team: Platform",
"Owner team: platform", "Owning team: Eng Platform", "Owning team:
Platform Eng"). One ticket's comment repeats its own body whole, as the
ERAG documents' chunks repeat their own text.

* A regex with one capture group counts the group's text.
* `norm(match)` merges case, spacing and punctuation; `words(match)` word
  order too; each merged group lists its variants, and groups whose words
  another group's hold are named as possibly one value.
* A passage repeating its record's own text is counted once.
* A regex with two capture groups is refused before anything is read.
* The index read gives way to the next literal when one literal's read
  stops at its slice.
"""
from __future__ import annotations

import pathlib
import re
from typing import Any

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import text_match as tm
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

#: Its comment repeats its body whole (longer than the repeat threshold).
PM1, PM2, PM3 = "ticket:pm1", "ticket:pm2", "ticket:pm3"
BODY1 = (
    "## Follow-up action items\n"
    "1. Add a stall alert for streams. Owner team: Platform. Priority: P0.\n"
    "2. Add a load test for many open connections. Owner team: platform. Priority: P1.\n"
    "3. Update the streaming runbook with tracing steps. Owner team: SRE. Priority: P1.\n"
)
BODY2 = (
    "## Follow-up action items\n"
    "1) Enforce a hop limit. Owning team: Eng Platform.\n"
    "2) Add a loop metric. Owning team: Platform Eng.\n"
    "3) Document routing headers. Owning team: Eng SRE.\n"
)
OWNER_GROUP = "re:/Own(?:er|ing) team: ([A-Za-z ]+)/"

_FENCE = re.compile(r"⟦/?UNTRUSTED_DATA[^⟧]*⟧\n?")


def unfence(text: str) -> str:
    return _FENCE.sub("", text).strip()


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


@pytest.fixture
def deployment(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    # Longer than the repeat threshold (`text_match.REPEATED_PASSAGE_MIN_CHARS`).
    assert len(BODY1) >= 200
    facts = [
        *_ticket(PM1, "Postmortem: streaming stalls\n\n" + BODY1, [BODY1]),
        *_ticket(PM2, "Postmortem: routing loop\n\n" + BODY2, ["a short unrelated note"]),
        *_ticket(PM3, "Review. Owner team: Security. Nothing else.", []),
    ]
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[TICKET, COMMENT], narrowings=NARROWINGS,
        nodes=[f for f in facts if isinstance(f, NodeFact)],
        edges=[f for f in facts if isinstance(f, EdgeFact)],
    )
    return deployment


async def _json(client: Any, arguments: dict) -> dict:
    result = await client.call_tool("aggregate", arguments, meta=JSON_META)
    assert not result.isError, result_text(result)
    return payload(result)


async def _text(client: Any, arguments: dict) -> str:
    result = await client.call_tool("aggregate", arguments)
    assert not result.isError, result_text(result)
    return result_text(result)


async def _refused(client: Any, arguments: dict) -> dict:
    result = await client.call_tool("aggregate", arguments, meta=JSON_META)
    assert result.isError, result_text(result)
    return payload(result)["error"]


def _call(value: str, by: str, **where: Any) -> dict:
    return {
        "where": {"type": "ticket", "text": {"op": "matches", "value": value}, **where},
        "by": [by], "metrics": ["count"],
    }


def _groups(page: dict, key: str) -> dict[str, tuple[int, int]]:
    return {
        unfence(g[key]): (g["count"], g["occurrences"]) for g in page["groups"]
    }


def test_a_capture_group_is_the_text_counted_and_a_repeated_passage_counts_once(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page = await _json(client, _call(OWNER_GROUP, "match"))
        assert _groups(page, "match") == {
            "Platform": (1, 1), "platform": (1, 1), "SRE": (1, 1),
            "Eng Platform": (1, 1), "Platform Eng": (1, 1), "Eng SRE": (1, 1),
            "Security": (1, 1),
        }, page["groups"]
        facts = page["text_match"]
        assert facts["capture_group"] is True and facts["repeated_passages"] == 1
        # Failure case: the whole match, no group, still counts its text
        # once although PM1's comment repeats it.
        whole = await _json(client, _call("re:/Owner team: [A-Za-z]+/", "match"))
        assert _groups(whole, "match")["Owner team: Platform"] == (1, 1), whole["groups"]
        text = await _text(client, _call(OWNER_GROUP, "match"))
        assert "the capture group's text in 3 records counted" in text
        assert "1 passages repeating their record's own text counted once" in text

    _drive(clean_runtime, deployment, body)


def test_norm_and_words_merge_variants_and_list_them(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        norm = await _json(client, _call(OWNER_GROUP, "norm(match)"))
        assert _groups(norm, "match") == {
            "platform": (1, 2), "sre": (1, 1), "eng platform": (1, 1),
            "platform eng": (1, 1), "eng sre": (1, 1), "security": (1, 1),
        }, norm["groups"]
        merged = next(g for g in norm["groups"] if unfence(g["match"]) == "platform")
        variants = unfence(merged["variants"])
        assert sorted(variants.split("; ")) == ["Platform (1)", "platform (1)"], variants
        # A group of one text lists that text.
        sre = next(g for g in norm["groups"] if unfence(g["match"]) == "sre")
        assert unfence(sre["variants"]) == "SRE (1)"
        words = await _json(client, _call(OWNER_GROUP, "words(match)"))
        assert _groups(words, "match") == {
            "platform": (1, 2), "eng platform": (1, 2), "sre": (1, 1),
            "eng sre": (1, 1), "security": (1, 1),
        }, words["groups"]
        # The fixture's source is not trusted, so the groups that may be one
        # are named by place, never by their text outside its fence.
        order = [unfence(g["match"]) for g in words["groups"]]
        assert order[:2] == ["eng platform", "platform"], order
        assert words["text_match"]["may_be_one"] == [
            ["group 1", "group 2"],
            [f"group {order.index('eng sre') + 1}", f"group {order.index('sre') + 1}"],
        ]
        text = await _text(client, _call(OWNER_GROUP, "words(match)"))
        assert "groups merge letter case, spacing, punctuation and word order" in text
        assert '! may be one value: "group 1" holds "group 2"' in text
        assert "variants" in text
        # Failure case: a merged key with no text condition is refused by name.
        error = await _refused(client, {
            "where": {"type": "ticket"}, "by": ["norm(match)"], "metrics": ["count"],
        })
        assert error["code"] == "invalid_operator_arguments"
        assert error["data"]["reason"] == "match_without_text"
        assert "norm(match)" in error["message"]

    _drive(clean_runtime, deployment, body)


def test_two_capture_groups_are_refused_before_any_text_is_read(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    read: list[Any] = []
    original = tm.matching_record_ids

    def spy(*args: Any, **kwargs: Any) -> list[str]:
        read.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(tm, "matching_record_ids", spy)

    async def body(client: Any) -> None:
        error = await _refused(
            client, _call("re:/(Owner|Owning) team: ([A-Za-z ]+)/", "match"),
        )
        assert error["code"] == "invalid_operator_arguments"
        assert error["data"]["reason"] == "several_capture_groups"
        assert "(?:...)" in error["fix_hint"]

    _drive(clean_runtime, deployment, body)
    assert read == []


def test_a_literal_whose_index_read_stops_gives_way_to_the_next(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The longest literal ("Platform") stands for a common word whose read
    passes its slice: the next ("Owning") is read and the call is served
    from the index, with no fallback to the rest of the condition."""
    stopped: list[str] = []
    original = tm._bounded

    def bounded(conn: Any, sql: str, params: Any) -> Any:
        literal = str(params.get("tml0", ""))
        if "SELECT DISTINCT u.record_id" in sql and "latform" in literal:
            stopped.append(literal)
            raise tm._Cancelled()
        return original(conn, sql, params)

    monkeypatch.setattr(tm, "_bounded", bounded)

    async def body(client: Any) -> None:
        page = await _json(client, _call('"Owning team" Platform', "match"))
        assert page["text_match"]["scan"] == "index", page["text_match"]
        assert "index_timed_out" not in page["text_match"]
        assert page["text_match"]["records_counted"] == 1
        # Failure case: one literal only, and it stops: the rest of the
        # condition (type ticket) is read instead, and the reply says so.
        only = await _json(client, _call("Platform", "match"))
        assert only["text_match"].get("index_timed_out") is True, only["text_match"]

    _drive(clean_runtime, deployment, body)
    assert stopped


def test_when_every_slice_stops_and_the_rest_is_too_large_the_longest_is_read_whole(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On ERAG `"Owning team: Eng SRE"` over every document: each literal's
    900 ms slice stopped, and the rest (every document) is over the limit.
    The longest literal is then read with the whole timeout, as before the
    slices, and the call is served from the index."""
    from okg.substrate.mcp.grep_search import _DEADLINE

    sliced: list[str] = []
    original = tm._bounded

    def bounded(conn: Any, sql: str, params: Any) -> Any:
        if "SELECT DISTINCT u.record_id" in sql and _DEADLINE.get() is not None:
            sliced.append(str(params.get("tml0", "")))
            raise tm._Cancelled()
        return original(conn, sql, params)

    monkeypatch.setattr(tm, "_bounded", bounded)
    # Three tickets: over a limit of 2, so the rest cannot be read instead.
    monkeypatch.setattr(tm, "SCAN_LIMIT", 2)

    async def body(client: Any) -> None:
        page = await _json(client, _call('"Owning team" Platform', "match"))
        assert page["text_match"]["scan"] == "index", page["text_match"]
        assert page["text_match"]["records_counted"] == 1
        assert _groups(page, "match") == {
            "Owning team": (1, 3), "Platform": (1, 2),
        }, page["groups"]

    _drive(clean_runtime, deployment, body)
    # Every literal was tried in its slice first, longest first.
    assert len(sliced) == 3 and "latform" in sliced[0], sliced
