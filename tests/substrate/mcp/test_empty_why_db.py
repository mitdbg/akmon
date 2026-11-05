"""An empty search or filter page says why, and what the call finds loosened (lane ri F6).

The archi smoke test: a model sent one search with a record type the
deployment does not have, got `0 rows`, and gave up. Read-eval tries ended
right after an empty page. Each test here sends such a call through a real
MCP client, as JSON and as text, and checks the reply names the cause, lists
what is valid, and shows loosened ids that are never results; the served
loosened call returns exactly those ids. Under access rules a hidden record
must change nothing in the reply.
"""
from __future__ import annotations

import json
import pathlib
from typing import Any

import psycopg
import pytest

from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp._result_payload import JSON_META, payload, result_text
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
from tests.substrate.mcp.test_page_next_db import _drive

pytestmark = pytest.mark.postgres

HIDDEN = "connector:hidden_pages"
SUBTYPES = [
    SubtypeSpec("ticket", metadata={"searchable_text_fields": ["key", "text"]}),
    SubtypeSpec("ticket_comment", metadata={"searchable_text_fields": ["text"]}),
    SubtypeSpec("wiki_page", metadata={"searchable_text_fields": ["title", "text"]}),
]
NODES = [
    node("ticket:OPS-1", "ticket", "Transfer queue stuck at the Tier1 site",
         key="OPS-1", status="open"),
    node("ticket:OPS-2", "ticket", "Reaper deletes replicas too slowly",
         key="OPS-2", status="closed"),
    node("wiki_page:transfers", "wiki_page", "How file transfers work with FTS",
         title="Transfers"),
]
HIDDEN_NODES = [
    node("wiki_page:secret", "wiki_page", "zebrafish transfer embargo plan",
         title="Zebrafish", status="embargoed"),
    node("ticket:hidden", "ticket", "quokka migration ticket", key="OPS-9",
         status="classified"),
]
HIDDEN_IDS = ("wiki_page:secret", "ticket:hidden")
HIDDEN_WORDS = ("embargoed", "zebrafish plan", "classified")


def _publish(clean_runtime: str, tmp_path: pathlib.Path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, nodes=NODES,
        node_groups=[(HIDDEN, HIDDEN_NODES)],
    )
    return deployment


@pytest.fixture
def deployment(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    return _publish(clean_runtime, tmp_path)


@pytest.fixture
def profiled(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    """The same graph with a deployment default search profile `work`
    (tickets only)."""
    deployment = _publish(clean_runtime, tmp_path)
    manifest = pathlib.Path(deployment) / "deployment.yaml"
    body = json.loads(manifest.read_text(encoding="utf-8"))
    body["search"] = {"default_profile": "work",
                      "profiles": {"work": {"subtypes": ["ticket"]}}}
    manifest.write_text(json.dumps(body), encoding="utf-8")
    return deployment


def _enforce(dsn: str, deployment: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.substrate.mcp.test_search_records import MAIN, READER, _activate, _allow

    # Records of the main producer are readable; the MCP call and read
    # boundaries are allowed (as tests/substrate/nomos/
    # test_nomos_fine_grained_access_control.py grants them).
    def boundary(rule_id: str, action: str, **match: str) -> dict:
        return {"rule_id": rule_id, "effect": "allow", "actions": [action],
                "subjects": [READER], "match": match, "priority": 200}

    _activate(dsn, deployment, [
        _allow(MAIN),
        boundary("allow_tool", "mcp.tool.call", resource_kind="mcp_tool"),
        boundary("allow_boundary", "graph.read.mcp", resource_kind="mcp_graph_read_boundary"),
        boundary("allow_operator", "graph.read.mcp", operator_name="mcp.read"),
    ])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)


async def _call(client: Any, arguments: dict, tool: str = "search") -> tuple[dict, str]:
    """The JSON and the text form of one call."""
    as_json = await client.call_tool(tool, arguments, meta=JSON_META)
    assert not as_json.isError, result_text(as_json)
    as_text = await client.call_tool(tool, arguments)
    assert not as_text.isError, result_text(as_text)
    return payload(as_json), result_text(as_text)


def _ids(page: dict) -> list[str]:
    rows = page.get("results")
    if rows is None:
        rows = page.get("items") or []
    return [str(row["node_id"]) for row in rows]


def _notices(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("! ")]


def _cause(text: str) -> str:
    return next(n for n in _notices(text) if n.startswith("! 0 rows"))


def _why(page: dict) -> dict:
    assert _ids(page) == [], page
    why = page.get("empty_why")
    assert isinstance(why, dict), page
    return why


def _count_is_zero_or_unknown(page: dict) -> None:
    """Loosened ids are never counted as results."""
    assert page.get("total_count") in (0, None), page


async def _follow(client: Any, why: dict) -> None:
    """The served loosened call, at the probe's limit, returns exactly the
    loosened ids."""
    call = why["loosened"]["call"]
    ids = why["loosened"]["ids"]
    page, _text = await _call(client, {**call["arguments"], "limit": len(ids)},
                              tool=call["tool"])
    assert sorted(_ids(page)) == sorted(ids), (why, _ids(page))


# ── an unknown type: valid types listed, loosened ids shown ──────────────


def test_an_unknown_where_type_lists_the_types_and_loosened_ids(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer queue",
                                           "where": {"type": "tickets"}})
        why = _why(page)
        _count_is_zero_or_unknown(page)
        assert why["cause"] == "unknown_type"
        assert why["unknown_types"] == ["tickets"]
        assert why["types"][0] == "ticket"
        assert set(why["types"]) == {"ticket", "ticket_comment", "wiki_page"}
        assert why["loosened"]["status"] == "found"
        assert "ticket:OPS-1" in why["loosened"]["ids"]
        assert why["loosened"]["call"]["arguments"] == {"query": "transfer queue"}
        assert (
            '! 0 rows because type "tickets" is not a record type here; types: ticket, '
        ) in text
        line = next(n for n in _notices(text) if n.startswith("! loosened"))
        assert line.startswith(
            '! loosened, not results (not counted): without type = "tickets": ')
        assert "ticket:OPS-1" in line
        assert line.endswith('; search(query="transfer queue")'), line
        await _follow(client, why)

    _drive(clean_runtime, deployment, body)


def test_grep_and_the_lookup_say_the_same_for_an_unknown_type(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer", "method": "grep",
                                           "where": {"type": "tickets"}})
        why = _why(page)
        assert why["cause"] == "unknown_type"
        assert "ticket:OPS-1" in why["loosened"]["ids"]
        assert why["loosened"]["call"]["arguments"] == {"query": "transfer", "method": "grep"}
        assert '! 0 rows because type "tickets" is not a record type here' in text
        await _follow(client, why)

        page, text = await _call(client, {"where": {"type": "tickets",
                                                    "attrs.status": "open"}})
        why = _why(page)
        assert why["cause"] == "unknown_type"
        assert why["loosened"]["ids"] == ["ticket:OPS-1"]
        assert why["loosened"]["call"]["arguments"] == {"where": {"attrs.status": "open"}}
        assert '! 0 rows because type "tickets" is not a record type here' in text
        await _follow(client, why)

    _drive(clean_runtime, deployment, body)


def test_an_in_list_that_keeps_a_real_type_blames_the_condition_not_the_type(
    clean_runtime: str, deployment: str,
) -> None:
    """Review M2: `type in ["ticket", "tickets"]` still allows tickets, so
    the unknown name is a note, not the cause."""
    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer", "where": {
            "type": {"op": "in", "values": ["ticket", "tickets"]},
            "attrs.status": "closed"}})
        why = _why(page)
        assert why["cause"] == "condition"
        assert why["removed_by"] == ['attrs.status = "closed"']
        assert why["unknown_types"] == ["tickets"]
        assert why["loosened"]["ids"] == ["ticket:OPS-1"]
        assert ('! type "tickets" is not a record type here; the other types named '
                "still applied; types: ticket") in text
        assert "0 rows because type" not in text
        await _follow(client, why)

    _drive(clean_runtime, deployment, body)


# ── known conditions that remove everything ───────────────────────────────


def test_the_condition_that_removed_every_row_is_named_with_other_values_seen(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, {
            "query": "transfer", "where": {"type": "ticket", "attrs.status": "closed"},
        })
        why = _why(page)
        _count_is_zero_or_unknown(page)
        assert why["cause"] == "condition"
        assert why["removed_by"] == ['attrs.status = "closed"']
        # Review L3: the sent value is no typo hint and is left out.
        assert "open" in why["values_seen"]["values"]
        assert "closed" not in why["values_seen"]["values"]
        assert why["loosened"]["ids"] == ["ticket:OPS-1"]
        assert why["loosened"]["call"]["arguments"] == {
            "query": "transfer", "where": {"type": "ticket"}}
        assert _cause(text).startswith(
            '! 0 rows because of attrs.status = "closed"; other values of attrs.status seen: ')
        assert any(n.startswith('! loosened, not results (not counted): without '
                                'attrs.status = "closed": ticket:OPS-1') for n in _notices(text))
        await _follow(client, why)

    _drive(clean_runtime, deployment, body)


def test_conditions_each_of_which_empties_the_page_are_said_as_such(
    clean_runtime: str, deployment: str,
) -> None:
    """Review M1: dropping any one condition still leaves 0 rows. The text
    says so (and, with a query, that dropping all brings rows back), never
    that no one of them alone removes every match."""
    async def body(client: Any) -> None:
        both = {"attrs.status": "zzstatus", "attrs.key": "zzkey"}
        page, text = await _call(client, {"where": both})
        why = _why(page)
        assert why["cause"] == "conditions"
        assert why["each_alone"] == "none"
        assert "loosened" not in why
        assert _cause(text) == (
            '! 0 rows because of the conditions (attrs.status = "zzstatus"; '
            'attrs.key = "zzkey"): dropping any one still leaves 0 rows')

        page, text = await _call(client, {"query": "transfer", "where": both})
        why = _why(page)
        assert why["cause"] == "conditions"
        assert why["all_dropped"] == "found"
        assert (": dropping any one still leaves 0 rows; dropping all brings rows back"
                in _cause(text))
        assert "no one of them alone" not in text
        await _follow(client, why)

    _drive(clean_runtime, deployment, body)


def test_a_rewritten_profile_is_not_put_back_in_the_loosened_call(
    clean_runtime: str, deployment: str,
) -> None:
    """Review M3: `profile="ticket"` is read as a type (#3666 step 2b). The
    loosened call names the type in `where`, not the sent profile, and
    returns exactly the loosened ids."""
    async def body(client: Any) -> None:
        page, _text = await _call(client, {"query": "transfer", "profile": "ticket",
                                           "where": {"attrs.status": "closed"}})
        why = _why(page)
        arguments = why["loosened"]["call"]["arguments"]
        assert "profile" not in arguments
        assert arguments == {"query": "transfer", "where": {"type": "ticket"}}
        await _follow(client, why)

        # The caller's pin carries into the loosened call.
        pinned = page["pinned_generation_id"]
        page, _text = await _call(client, {"query": "transfer", "generation_id": pinned,
                                           "where": {"type": "tickets"}})
        why = _why(page)
        assert why["loosened"]["call"]["arguments"]["generation_id"] == pinned
        await _follow(client, why)

    _drive(clean_runtime, deployment, body)


def test_a_scoped_search_never_claims_the_query_matches_nothing_at_all(
    clean_runtime: str, profiled: str,
) -> None:
    """Review M4: under the default profile `work` (tickets) the query is
    checked within that profile, and the reply says so."""
    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "fts"})
        why = _why(page)
        assert why["cause"] == "query"
        assert why["scope"] == 'profile "work"'
        assert 'matches no record within profile "work"' in _cause(text)
        assert "matches no record," not in text

    _drive(clean_runtime, profiled, body)


# ── the query's own words ─────────────────────────────────────────────────


def test_a_word_no_text_holds_is_named_and_the_rest_is_loosened(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer zzqxv", "method": "grep"})
        why = _why(page)
        assert why["cause"] == "terms"
        assert why["absent_terms"] == ["zzqxv"]
        assert why["loosened"]["dropped"] == ['word "zzqxv"']
        assert "ticket:OPS-1" in why["loosened"]["ids"]
        assert '! 0 rows: word "zzqxv" alone matches no record' in text
        await _follow(client, why)

        page, text = await _call(client, {"query": "zzqxv", "method": "grep"})
        why = _why(page)
        assert why["cause"] == "query"
        assert "loosened" not in why
        assert ("! 0 rows: the query itself matches no record, and the call has no "
                "condition to loosen") in text

        # Each word matches alone; no one text holds both.
        page, text = await _call(client, {"query": "reaper fts", "method": "grep"})
        why = _why(page)
        assert why["cause"] == "terms_together"
        assert _cause(text) == "! 0 rows: each word matches alone; no single text holds them all"

    _drive(clean_runtime, deployment, body)


# ── reads and probes that do not finish claim nothing ─────────────────────


def test_a_timed_out_page_claims_nothing_and_runs_no_probe(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review H1: a grep whose read timed out serves an empty page with
    basis `timed_out`; its reply must never say the query matches nothing."""
    import okg.substrate.mcp.server as srv

    real = srv._search

    def timing_out(*args: Any, **kwargs: Any) -> dict:
        page = real(*args, **kwargs)
        if not page.get("results"):
            page = {**page, "total_count": None, "total_count_basis": "timed_out"}
        return page

    monkeypatch.setattr(srv, "_search", timing_out)

    async def body(client: Any) -> None:
        for arguments in ({"query": "zzqxv", "method": "grep"},
                          {"query": "re:/zzq.*xv/", "method": "grep"},
                          {"query": "transfer", "method": "grep",
                           "where": {"attrs.status": "zz"}}):
            page, text = await _call(client, arguments)
            why = _why(page)
            assert why["cause"] == "page_timed_out", (arguments, why)
            assert "probes" not in why and "loosened" not in why
            assert _cause(text) == ("! 0 rows: the read timed out before it finished; "
                                    "the cause is unknown, nothing claimed")
            assert "matches no record" not in text

    _drive(clean_runtime, deployment, body)


def test_a_timed_out_or_skipped_probe_is_said_and_claims_nothing(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def cancelled(cur: Any, milliseconds: int, *, local: bool) -> None:
        raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

    monkeypatch.setattr("okg.substrate.mcp.empty_why._arm_probe", cancelled)

    async def timed_out(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer queue",
                                           "where": {"type": "tickets"}})
        why = _why(page)
        assert why["cause"] == "unknown_type"
        assert why["loosened"] == {"dropped": ['type = "tickets"'], "status": "timed_out"}
        assert '! loosened: without type = "tickets" not checked (timed out)' in text
        assert "still no record" not in text

        page, text = await _call(client, {"query": "transfer zzqxv", "method": "grep"})
        why = _why(page)
        assert why["cause"] == "not_found"
        assert "absent_terms" not in why
        assert "! 0 rows: cause not found (a check timed out)" in text
        assert "alone matches" not in text

    _drive(clean_runtime, deployment, timed_out)
    monkeypatch.undo()
    monkeypatch.setattr("okg.substrate.mcp.empty_why.BUDGET_S", 0.0)

    async def skipped(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer queue",
                                           "where": {"type": "tickets"}})
        why = _why(page)
        assert why["loosened"]["status"] == "skipped"
        assert '! loosened: without type = "tickets" not checked (not run)' in text

    _drive(clean_runtime, deployment, skipped)


def test_a_probe_puts_the_statement_timeout_back(clean_runtime: str) -> None:
    """A probe's timeout lasts only the probe, on an autocommit handle and
    inside a transaction, whether the probe answers or times out."""
    from okg.substrate.mcp import empty_why as why

    def answered(call: Any, limit: int) -> dict:
        return {"results": [{"node_id": "n:1"}]}

    def cancelled(call: Any, limit: int) -> dict:
        raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

    for autocommit in (True, False):
        with psycopg.connect(clean_runtime, autocommit=autocommit) as conn:
            conn.execute("SET statement_timeout = '7s'")
            for run, status in ((answered, "found"), (cancelled, "timed_out")):
                prober = why.Prober(conn, run, every_row_readable=True)
                found = prober(why.Call(method="grep", query="x"), limit=1, label={})
                assert found.status == status
                # On an autocommit handle the timeout is put back once, after
                # the reply's probes (`close`, which the hooks always call).
                prober.close()
                (now,) = conn.execute("SHOW statement_timeout").fetchone()
                assert now == "7s", (autocommit, status, now)


def test_a_page_with_rows_or_past_the_first_carries_no_explanation(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer queue"})
        assert _ids(page)
        assert "empty_why" not in page
        assert "! 0 rows" not in text
        page, text = await _call(client, {"query": "zzqxv", "method": "grep", "offset": 5})
        assert _ids(page) == []
        assert "empty_why" not in page
        page, text = await _call(client, {"input": {"kind": "nodes", "subtype": "ticket"}},
                                 tool="filter")
        assert _ids(page) and "empty_why" not in page

    _drive(clean_runtime, deployment, body)


# ── the filter tool ───────────────────────────────────────────────────────


def test_an_empty_filter_page_says_why_and_shows_loosened_ids(
    clean_runtime: str, deployment: str,
) -> None:
    """Review H2: `filter` answers an empty first page as search does."""
    async def body(client: Any) -> None:
        open_status = {"op": "eq", "field": "attrs.status", "value": "open"}
        page, text = await _call(client, {"input": {"kind": "nodes", "subtype": "tickets"},
                                          "predicate": open_status}, tool="filter")
        why = _why(page)
        _count_is_zero_or_unknown(page)
        assert why["cause"] == "unknown_type"
        assert why["types"][0] == "ticket"
        assert why["loosened"]["ids"] == ["ticket:OPS-1"]
        assert why["loosened"]["call"]["tool"] == "filter"
        assert '! 0 rows because type "tickets" is not a record type here' in text
        assert any(n.startswith('! loosened, not results (not counted): without type = '
                                '"tickets": ticket:OPS-1') for n in _notices(text))
        await _follow(client, why)

        page, text = await _call(client, {
            "input": {"kind": "nodes", "subtype": "ticket"},
            "predicate": {"op": "eq", "field": "attrs.status", "value": "pending"},
        }, tool="filter")
        why = _why(page)
        assert why["cause"] == "condition"
        assert why["removed_by"] == ['attrs.status = "pending"']
        assert {"open", "closed"} <= set(why["values_seen"]["values"])
        assert sorted(why["loosened"]["ids"]) == sorted(
            ["ticket:OPS-1", "ticket:OPS-2", "ticket:hidden"])
        assert _cause(text).startswith('! 0 rows because of attrs.status = "pending"')
        await _follow(client, why)

        # A text condition is read through more than one page read: no
        # probe runs, and the reply says so.
        page, text = await _call(client, {
            "input": {"kind": "nodes", "subtype": "tickets"},
            "predicate": {"op": "matches", "field": "text", "value": "transfer"},
        }, tool="filter")
        why = _why(page)
        assert why["cause"] == "unknown_type"
        assert why["loosened"]["status"] == "skipped"
        assert "not checked (not run)" in text

    _drive(clean_runtime, deployment, body)


# ── access rules: a hidden record changes nothing ─────────────────────────


def _same(reply: tuple[dict, str], words: dict[str, str]) -> tuple[str, str]:
    """The reply's explanation and notices, each word of `words` written as
    its stand-in."""
    page, text = reply
    why = json.dumps(page.get("empty_why"), sort_keys=True)
    lines = "\n".join(_notices(text))
    for word, other in words.items():
        why, lines = why.replace(word, other), lines.replace(word, other)
    return why, lines


def _no_hidden(reply: tuple[dict, str], *, sent: tuple[str, ...] = ()) -> None:
    """No hidden id or value in the reply, beyond what the caller `sent`."""
    page, text = reply
    served = json.dumps(page) + text
    for hidden in (*HIDDEN_IDS, *HIDDEN_WORDS):
        if hidden in sent:
            continue
        assert hidden not in served, hidden


def test_under_enforce_a_hidden_record_reads_like_no_record(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enforce(clean_runtime, deployment, monkeypatch)

    async def body(client: Any) -> None:
        hidden_word = {"zebrafish": "zzqxv"}
        hidden_pair = {"quokka": "zzqxv", "migration": "yyqxw"}
        pairs = [
            # A word only a hidden record holds, alone and under a type.
            ({"query": "zebrafish"}, hidden_word),
            ({"query": "zebrafish", "where": {"type": "ticket"}}, hidden_word),
            ({"query": "zebrafish", "method": "grep"}, hidden_word),
            # Two words a hidden ticket holds together.
            ({"query": "quokka migration", "where": {"type": "ticket"}}, hidden_pair),
            ({"query": "quokka migration", "method": "grep"}, hidden_pair),
        ]
        for arguments, words in pairs:
            hidden = await _call(client, arguments)
            other = " ".join(words[w] for w in arguments["query"].split())
            absent = await _call(client, {**arguments, "query": other})
            why = _why(hidden[0])
            assert why["access_rules"] is True
            _no_hidden(hidden)
            assert _same(hidden, words) == _same(absent, words), "\n".join(
                [str(arguments), *_same(hidden, words), "--- absent ---",
                 *_same(absent, words)])
            assert why["cause"] == "query", why
            assert "the query itself matches no record you can read" in _cause(hidden[1])

        # A lookup and a filter on a value only a hidden record holds read
        # like one on a value no record holds.
        lookup = {"where": {"type": "ticket", "attrs.status": "classified"}}
        hidden = await _call(client, lookup)
        absent = await _call(client, {"where": {"type": "ticket", "attrs.status": "zzz"}})
        _no_hidden(hidden, sent=("classified",))
        assert _same(hidden, {"classified": "zzz"}) == _same(absent, {})
        listed = {"input": {"kind": "nodes", "subtype": "ticket"},
                  "predicate": {"op": "eq", "field": "attrs.status", "value": "classified"}}
        hidden = await _call(client, listed, tool="filter")
        absent = await _call(client, {**listed, "predicate": {
            "op": "eq", "field": "attrs.status", "value": "zzz"}}, tool="filter")
        _no_hidden(hidden, sent=("classified",))
        assert _same(hidden, {"classified": "zzz"}) == _same(absent, {})
        assert _why(hidden[0])["values_listed"] is False

        # An attribute condition under access rules: named, its values not
        # listed (a hidden ticket holds another status value).
        reply = await _call(client, {"query": "transfer",
                                     "where": {"type": "ticket", "attrs.status": "closed"}})
        why = _why(reply[0])
        assert why["cause"] == "condition"
        assert why["values_listed"] is False
        assert "values_seen" not in why
        assert "values not listed (access rules)" in reply[1]
        assert why["loosened"]["ids"] == ["ticket:OPS-1"]
        _no_hidden(reply)

        # The type list is the catalog's, as without access rules.
        reply = await _call(client, {"query": "transfer queue", "where": {"type": "tickets"}})
        why = _why(reply[0])
        assert set(why["types"]) == {"ticket", "ticket_comment", "wiki_page"}
        assert "ticket:OPS-1" in why["loosened"]["ids"]
        _no_hidden(reply)

    _drive(clean_runtime, deployment, body)


def test_a_page_with_rows_or_past_the_first_makes_no_f6_read(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Orchestrator ruling on read counts: F6 adds no database read to a page
    with rows, or past the first page, on search or filter. Every F6 entry
    point fails the call if it is reached; the calls still answer, so the
    statements they run are the statements the pin runs (measured in
    pact/changes/read-empty-why-f6/evidence/runs.txt)."""
    from okg.substrate.mcp import empty_why

    def reached(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("F6 ran on a page it must not touch")

    for name in ("catalog_types", "explain", "Prober", "outcome"):
        monkeypatch.setattr(empty_why, name, reached)

    async def body(client: Any) -> None:
        with_rows = [
            ("search", {"query": "transfer queue"}),
            ("search", {"query": "transfer", "method": "grep"}),
            ("search", {"where": {"type": "ticket", "attrs.status": "open"}}),
            ("filter", {"input": {"kind": "nodes", "subtype": "ticket"},
                        "predicate": {"op": "eq", "field": "attrs.status", "value": "open"}}),
            ("filter", {"input": {"kind": "nodes", "subtype": "ticket"}}),
        ]
        for tool, arguments in with_rows:
            page, _text = await _call(client, arguments, tool=tool)
            assert _ids(page) and "empty_why" not in page, (tool, arguments)
        past_first = [
            ("search", {"query": "zzqxv", "method": "grep", "offset": 5}),
            ("search", {"query": "transfer queue", "where": {"type": "tickets"}, "offset": 5}),
            ("filter", {"input": {"kind": "nodes", "subtype": "tickets"}, "offset": 50}),
        ]
        for tool, arguments in past_first:
            page, _text = await _call(client, arguments, tool=tool)
            assert _ids(page) == [] and "empty_why" not in page, (tool, arguments)

    _drive(clean_runtime, deployment, body)


# ── review r2: the cost of the worst shapes, and what the probe limit says ─


def _eq(field: str, value: Any) -> dict:
    return {"op": "eq", "field": field, "value": value}


def _and(*clauses: dict) -> dict:
    return {"op": "and", "clauses": list(clauses)}


_FIVE = [_eq("attrs.status", "z1"), _eq("attrs.key", "z2"), _eq("attrs.title", "z3"),
         _eq("attrs.k4", "z4"), _eq("attrs.k5", "z5")]
#: The shapes that cost the most (review r2 M1): every condition is a probe.
WORST = [
    ("lexical, 5 conditions", "search", "lexical", {"query": "transfer", "where": _and(*_FIVE)}),
    ("grep, 5 conditions", "search", "grep",
     {"query": "transfer", "method": "grep", "where": _and(*_FIVE)}),
    ("grep, 4 absent words", "search", "grep",
     {"query": "zzqxv yyqxw aaqxw bbqxw", "method": "grep"}),
    ("lookup, 5 conditions", "search", "attrs", {"where": _and(_eq("type", "ticket"), *_FIVE)}),
    ("filter, 4 clauses and a type", "filter", "filter",
     {"input": {"kind": "nodes", "subtype": "ticket"}, "predicate": _and(*_FIVE[:4])}),
]


@pytest.mark.parametrize("enforced", [False, True], ids=["off", "enforce"])
def test_the_worst_empty_page_costs_at_most_twice_the_call(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch, enforced: bool,
) -> None:
    """Review r2 M1: every statement an empty first page adds is counted
    against the call's own (F6 off), shape by shape; F6 adds at most twice
    the call's own statements and runs at most `read_cap` reads."""
    from okg.substrate.mcp import empty_why
    from okg.substrate.mcp import server as srv

    if enforced:
        _enforce(clean_runtime, deployment, monkeypatch)
    count = {"n": 0}
    real = psycopg.Cursor.execute

    def counted(self: Any, query: Any, params: Any = None, **kwargs: Any) -> Any:
        count["n"] += 1
        return real(self, query, params, **kwargs)

    def statements(fn: Any, arguments: dict) -> tuple[int, dict]:
        fn(**arguments)  # warm caches
        count["n"] = 0
        with monkeypatch.context() as patch:
            patch.setattr(psycopg.Cursor, "execute", counted)
            page = fn(**arguments)
        return count["n"], page

    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    tools = mcp._tool_manager._tools
    try:
        for name, tool, method, arguments in WORST:
            with monkeypatch.context() as off:
                # The call's own statements: F6 does no read.
                off.setattr(empty_why, "explain", lambda *a, **k: {"cause": "not_checked"})
                off.setattr(empty_why, "catalog_types", lambda *a, **k: [])
                own, _ = statements(tools[tool].fn, arguments)
            total, page = statements(tools[tool].fn, arguments)
            why = page["empty_why"]
            ran = [p for p in why.get("probes", [])
                   if p["status"] not in (empty_why.SKIPPED, empty_why.LIMITED)]
            reads = len(ran) + (1 if why.get("values_read") else 0)
            assert reads <= empty_why.read_cap(method, not enforced), (name, why)
            assert total - own <= 2 * own, (name, own, total, why)
    finally:
        mcp._okg_pool.close()


def test_conditions_that_fill_the_reads_probe_the_query_first_and_say_the_limit(
    clean_runtime: str, deployment: str,
) -> None:
    """Review r2 M1: with as many conditions as reads, the query alone is
    probed first; the reply says the probe limit was reached, not that a
    check timed out."""
    five = {"attrs.status": "z1", "attrs.key": "z2", "attrs.title": "z3",
            "attrs.k4": "z4", "attrs.k5": "z5"}

    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "transfer", "method": "grep", "where": five})
        why = _why(page)
        assert why["probes"][0]["dropped"] == [f'{k} = "{v}"' for k, v in five.items()]
        assert why["cause"] == "conditions" and why["all_dropped"] == "found"
        assert why["each_alone"] == "unchecked" and why["unchecked"] == "probe_limit"
        assert _cause(text).endswith(
            "not each checked alone (probe limit reached); dropping all brings rows back")
        assert "timed out" not in text
        await _follow(client, why)

        # A lookup has no query to try first: its reads go to the
        # conditions, and the limit is said.
        page, text = await _call(client, {"where": {"type": "ticket", **five}})
        why = _why(page)
        assert why["cause"] == "not_found" and why["unchecked"] == "probe_limit"
        assert _cause(text) == "! 0 rows: cause not found (probe limit reached)"

    _drive(clean_runtime, deployment, body)


def test_grep_within_record_says_no_record_holds_every_word(
    clean_runtime: str, deployment: str,
) -> None:
    """Review r2 L6: with within="record" the words are matched across a
    record, so the line names a record, not a text."""
    async def body(client: Any) -> None:
        page, text = await _call(client, {"query": "reaper fts", "method": "grep",
                                           "within": "record"})
        why = _why(page)
        assert why["cause"] == "terms_together" and why["within_record"] is True
        assert _cause(text) == "! 0 rows: each word matches alone; no single record holds them all"

    _drive(clean_runtime, deployment, body)


def test_a_rewritten_source_family_is_not_put_back_in_the_loosened_call(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
) -> None:
    """Review r2 L7: a source id sent as `source_family` is read as a
    `source` condition (#3666 step 2b). The loosened call keeps it in
    `where`, never as the sent argument, and returns exactly the ids shown.
    The corpus of test_search_arguments_db (its sources are listed)."""
    from tests.substrate.mcp.test_read_grammar_db import Tools
    from tests.substrate.mcp.test_search_list import MAIN, TERM, _corpus

    tools = Tools(clean_runtime, _corpus(tmp_path, clean_runtime))

    def reply(arguments: dict) -> dict:
        (result,) = tools.run(("search", arguments, "json"))
        return json.loads(result.content[0].text)

    try:
        page = reply({"query": TERM, "source_family": MAIN, "where": {
            "op": "and", "clauses": [{"op": "eq", "field": "type", "value": "doc"},
                                     {"op": "eq", "field": "attrs.zzkey", "value": "zz"}]}})
        why = _why(page)
        assert [n["rule"] for n in page.get("adjusted", [])] == ["source_family_as_source"]
        assert why["loosened"]["status"] == "found", why
        arguments = why["loosened"]["call"]["arguments"]
        assert "source_family" not in arguments, arguments
        assert arguments["where"]["source"] == MAIN, arguments
        followed = reply({**arguments, "limit": len(why["loosened"]["ids"])})
        assert sorted(_ids(followed)) == sorted(why["loosened"]["ids"])
    finally:
        tools.close()


def test_an_empty_filter_page_read_by_the_chronos_program_says_why(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review r2 L5: a filter on stable fields only is read by the Chronos
    program, not the effective-view page; its empty page says why too, and
    the loosened call (read the same way) returns exactly the ids shown."""
    from okg.substrate.transform import operators

    real = operators.lower_program_over_chronos
    lowered = {"n": 0}

    def counting(*args: Any, **kwargs: Any) -> Any:
        lowered["n"] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(operators, "lower_program_over_chronos", counting)

    async def body(client: Any) -> None:
        page, text = await _call(client, {
            "input": {"kind": "nodes", "subtype": "tickets"},
            "predicate": {"op": "eq", "field": "node_id", "value": "ticket:OPS-1"},
        }, tool="filter")
        assert lowered["n"] == 2, lowered  # the JSON and the text call
        why = _why(page)
        assert why["cause"] == "unknown_type"
        assert why["loosened"]["ids"] == ["ticket:OPS-1"]
        assert '! 0 rows because type "tickets" is not a record type here' in text
        await _follow(client, why)

    _drive(clean_runtime, deployment, body)


# ── with #4226 (a grep `where` on a passage type) ─────────────────────────


PASSAGE_SUBTYPES = [
    SubtypeSpec("ticket", metadata={
        "searchable_text_fields": ["key", "text"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "ticket_comment"},
    }),
    SubtypeSpec("ticket_comment", metadata={"searchable_text_fields": ["text"]}),
]


@pytest.fixture
def with_passages(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    """Tickets whose comments are declared passages (`contains`)."""
    from okg.substrate.library.sources.base import EdgeFact

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=PASSAGE_SUBTYPES,
        narrowings=[("ticket", "contains", "ticket_comment")],
        nodes=[node("ticket:OPS-1", "ticket", "Transfer queue stuck", key="OPS-1"),
               node("comment:OPS-1:0", "ticket_comment", "the transfer agent restarted")],
        edges=[EdgeFact(src="ticket:OPS-1", dst="comment:OPS-1:0", edge_type="contains",
                        source_record_id={"id": "c0"}, source_revision={"v": 1})],
    )
    return deployment


def test_a_page_4226_refuses_gets_no_block_and_one_it_serves_still_says_why(
    clean_runtime: str, with_passages: str,
) -> None:
    """#4226 refuses a grep `where` naming a passage type beside other types:
    that call is refused, with no F6 block. A lone passage type is served as
    its passages (with an `adjusted` note); an empty page of it still says
    why, and the probes add no note of their own."""
    async def body(client: Any) -> None:
        refused = await client.call_tool("search", {
            "query": "transfer", "method": "grep",
            "where": {"type": {"op": "in", "values": ["ticket_comment", "ticket"]}},
        }, meta=JSON_META)
        assert refused.isError, result_text(refused)
        assert "empty_why" not in result_text(refused)

        page, text = await _call(client, {"query": "zzqxv", "method": "grep",
                                           "where": {"type": "ticket_comment"}})
        why = _why(page)
        assert why["cause"] == "query", why
        assert [n["rule"] for n in page.get("adjusted", [])] == ["grep_passage_type"], page
        assert text.count("! adjusted") == 1, text

    _drive(clean_runtime, with_passages, body)
