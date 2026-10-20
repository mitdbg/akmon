"""like and like_exact through the served tools over a publish (PACT
loose-match-3666), the SQL fold against the Python fold on every code point,
and the refusals under access rules.

Every reply is read as a client receives it: JSON through one MCP client
session, and the text form where a notice is the claim.
"""
from __future__ import annotations

import json
import re
import unicodedata
from typing import Any

import anyio
import psycopg
import psycopg.sql
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import text_fold
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_search_list import DOC, MAIN, TERM, TICKET, _edge, _node, _trust

pytestmark = pytest.mark.postgres

#: One surname in the spellings an author field holds, and two that are not it.
SURNAME = {
    "doc:a": "Müller",
    "doc:b": "MULLER",
    "doc:c": "Mueller-Muller",
    "doc:d": "muller.x.org",
    "doc:e": "Miller",
    "doc:f": "Müller",  # u + a combining diaeresis
    "doc:g": "Straße",
    "doc:h": "Łukasz Øster",
}
LOOSE_MULLER = ["doc:a", "doc:b", "doc:c", "doc:d", "doc:f"]

#: The parity corpus: at least 200 strings over ASCII, Latin-1, precomposed
#: and combining accents, sharp s, stroke letters, dotted and dotless i,
#: Greek with final sigma, Cyrillic, CJK, emoji and controls other than NUL.
_SEEDS = [
    "plain ascii", "Müller", "Müller", "MÜLLER", "Straße", "STRASSE", "ẞig",
    "Øster", "Łódź", "Đoàn", "Æsir", "Œuvre", "Þór", "İstanbul", "ıi", "Kırşehir",
    "ΟΔΟΣ ΟΔΟΣ", "οδος", "ΣΊΣΥΦΟΣ", "Москва", "ᲀ old ve", "東京都", "ｆｕｌｌ ｗｉｄｔｈ",
    "ﬁle ﬂow", "x² y³", "😀 grin", "tab\there", "bell\x07", "line\nfeed", "café",
    "café", "naïve", "Ångström", "Å", "Ǆemal", "ǅ", "Ꭰ Cherokee", "ꭰ cherokee",
    "émigré", "ÉMIGRÉ", "Ññ", "Çç", "Ğğ", "Şş", "ŒœŒ", "ß ss SS", "ﬀ", "Ⅻ", "①②",
]


def _corpus_strings() -> list[str]:
    out = list(_SEEDS)
    i = 0
    while len(out) < 200:
        a, b = _SEEDS[i % len(_SEEDS)], _SEEDS[(i * 7 + 3) % len(_SEEDS)]
        out.append(f"{a} {b}" if i % 2 else f"{b}-{a}".upper())
        i += 1
    return out


CORPUS = _corpus_strings()
#: Operands the parity case reads the corpus with.
_PARITY_OPERANDS = [
    "muller", "MÜLLER", "strasse", "ss", "oster", "lodz", "ae", "oe", "th", "i", "ı",
    "οδοσ", "σ", "москва", "東京", "full", "fi", "2", "😀", "cafe", "é", "ǆ", "Ꭰ",
]


def _deployment(tmp_path, dsn: str) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [
        _node(node_id, "doc", f"{TERM} {node_id}.", author_name=name)
        for node_id, name in SURNAME.items()
    ]
    nodes.append(_node("doc:list", "doc", f"{TERM} list.", labels=["x", 3, "Müllerei"]))
    nodes.append(_node("doc:number", "doc", f"{TERM} number.", author_name=42))
    nodes += [
        _node(f"doc:p{i:03d}", "doc", f"probe {i}.", v=value)
        for i, value in enumerate(CORPUS)
    ]
    nodes.append(_node("ticket:hub", "ticket", f"{TERM} hub."))
    edges = [_edge("ticket:hub", node_id) for node_id in SURNAME]
    publish_nodes(
        dsn, deployment, subtypes=[DOC, TICKET], nodes=nodes, edges=edges,
        narrowings=[("ticket", "contains", "doc")],
    )
    _trust(dsn, deployment, MAIN)
    return deployment


class Tools:
    def __init__(self, dsn: str, deployment: str, mcp: Any = None) -> None:
        from okg.substrate.mcp import server as srv

        self.mcp = mcp if mcp is not None else srv.build_server(dsn=dsn, deployment_name=deployment)

    def close(self) -> None:
        self.mcp._okg_pool.close()
        if self.mcp._okg_write_pool:
            self.mcp._okg_write_pool.close()

    def run(self, steps) -> list:
        """`steps(call)`: an async function given `call(tool, arguments,
        format="json")`, all in one client session."""
        from mcp.shared.memory import create_connected_server_and_client_session

        out: list = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(self.mcp) as client:
                async def call(tool: str, arguments: dict, fmt: str = "json") -> Any:
                    result = await client.call_tool(
                        tool, arguments, meta={"okg/result_format": fmt},
                    )
                    text = result.content[0].text
                    return json.loads(text) if fmt == "json" else text

                out.append(await steps(call))

        anyio.run(drive)
        return out[0]

    def call(self, tool: str, arguments: dict, fmt: str = "json") -> Any:
        async def one(call):
            return await call(tool, arguments, fmt)
        return self.run(one)


@pytest.fixture
def tools(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Tools(clean_runtime, _deployment(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def _served(reply: dict) -> dict:
    assert reply.get("status") != "refused", json.dumps(reply)[:1200]
    return reply


def _ids(reply: dict, key: str = "items") -> list[str]:
    return sorted(str(row["node_id"]) for row in _served(reply)[key])


def _like(field: str, value: str, op: str = "like") -> dict:
    return {"type": "doc", field: {"op": op, "value": value}}


def test_like_finds_every_spelling_of_one_surname(tools):
    reply = tools.call("filter", {"where": _like("attrs.author_name", "muller"),
                                  "select": ["node_id", "attrs"]})
    assert _ids(reply) == LOOSE_MULLER
    assert reply["total_count"] == len(LOOSE_MULLER)
    assert reply["matched_loosely"] == [{
        "field": "attrs.author_name", "op": "like", "operand": "muller", "folded": "muller",
    }]
    # Values are served as stored: matching never rewrites one.
    stored = {row["node_id"]: row["attrs"]["author_name"] for row in reply["items"]}
    assert stored == {node_id: SURNAME[node_id] for node_id in LOOSE_MULLER}
    text = tools.call("filter", {"where": _like("attrs.author_name", "MÜLLER")}, "text")
    assert '! matched loosely: attrs.author_name contains "muller" ignoring accents and case' in text
    assert {"doc:g"} == set(_ids(tools.call("filter", {"where": _like("attrs.author_name", "STRASSE")})))
    assert _ids(tools.call("filter", {"where": _like("attrs.author_name", "oster")})) == ["doc:h"]


def test_like_on_a_list_attribute_and_a_column(tools):
    assert _ids(tools.call("filter", {"where": _like("attrs.labels", "mullerei")})) == ["doc:list"]
    # A number is never text: it never matches, and the read is not refused.
    assert _ids(tools.call("filter", {"where": _like("attrs.author_name", "42")})) == []
    assert _ids(tools.call("filter", {"where": _like("node_id", "DOC:C")})) == ["doc:c"]


def test_like_percent_is_stripped_with_a_note(tools):
    reply = tools.call("filter", {"where": _like("attrs.author_name", "%muller%")})
    assert _ids(reply) == LOOSE_MULLER
    assert [r["rule"] for r in reply["adjusted"]] == ["like_percent_stripped"]


def test_like_interior_percent_and_text_are_refused_with_what_to_write(tools):
    interior = tools.call("filter", {"where": _like("attrs.author_name", "mu%er")})
    assert interior["status"] == "refused" and interior["error"].get("retry_call") is None
    on_text = tools.call("filter", {"where": {"type": "doc", "text": {"op": "like", "value": "list"}}})
    assert on_text["status"] == "refused"
    # The server offers the matches call only once its own checks take it:
    # a text clause beside a type, on records, with access rules off.
    assert "retry_call" in on_text["error"], json.dumps(on_text)[:1500]
    retry = on_text["error"]["retry_call"]
    assert retry["mode"] == "guess"
    assert retry["arguments"]["where"]["text"] == {"op": "matches", "value": "list"}
    # The offered call is served as written, and finds the record.
    assert _ids(tools.call(retry["tool"], retry["arguments"])) == ["doc:list"]


def test_like_exact_is_strict_on_case_and_accents(tools):
    reply = tools.call("filter", {"where": _like("attrs.author_name", "Müller", "like_exact")})
    # The combining sequence equals the precomposed letter after NFC.
    assert _ids(reply) == ["doc:a", "doc:f"]
    assert reply["matched_loosely"][0]["op"] == "like_exact"
    text = tools.call("filter", {"where": _like("attrs.author_name", "Müller", "like_exact")}, "text")
    assert '! matched exactly: attrs.author_name contains "Müller"' in text


def test_map_aggregate_and_expand_read_like_the_same_way(tools):
    mapped = tools.call("map", {"where": _like("attrs.author_name", "muller"),
                                "projection": ["node_id"]})
    rows = _served(mapped).get("items") or mapped.get("rows") or []
    assert sorted(row["node_id"] for row in rows) == LOOSE_MULLER, json.dumps(mapped)[:1200]
    counted = tools.call("aggregate", {"where": _like("attrs.author_name", "muller"),
                                       "by": ["subtype"], "metrics": ["count"]})
    assert [(g["subtype"], g["count"]) for g in _served(counted)["groups"]] == [("doc", 5)]
    far = tools.call("expand", {"input": "ticket:hub", "where": {
        "attrs.author_name": {"op": "like", "value": "muller"}}})
    found = sorted(
        str(row.get("node_id") or row.get("dst"))
        for row in _served(far).get("neighbors") or far.get("items") or []
    )
    assert found == LOOSE_MULLER, json.dumps(far)[:1200]


def test_like_on_a_saved_result_reads_the_same_fold(tools):
    async def steps(call):
        saved = await call("search", {"query": TERM, "retain": True, "limit": 50,
                                      "where": {"type": "doc"}})
        ref = saved["result_reference"]["id"]
        return await call("filter", {"input": {"ref": ref}, "predicate": {
            "op": "like", "field": "attrs.author_name", "value": "muller"}})

    assert _ids(tools.run(steps)) == LOOSE_MULLER


def test_search_where_like_offers_search_s_own_contains_call(tools):
    """Search's `where` takes no like. On an attribute, the refusal offers
    search's own call with `contains` and the same text (review of read
    stack PR 2, #3666), served as written: a guess, as contains ignores
    ASCII case but not accents, so it finds a subset of like's rows."""
    reply = tools.call("search", {"query": TERM, "where": _like("attrs.author_name", "%muller%")})
    assert reply["status"] == "refused"
    assert reply["error"]["code"] == "invalid_operator_arguments"
    retry = reply["error"]["retry_call"]
    assert retry["tool"] == "search" and retry["mode"] == "guess"
    assert retry["arguments"] == {
        "query": TERM, "where": _like("attrs.author_name", "muller", "contains")}
    found = _ids(tools.call("search", retry["arguments"]), "results")
    assert found and set(found) <= set(LOOSE_MULLER), found


def test_the_sql_fold_agrees_with_the_python_fold_row_for_row(tools):
    """The parity corpus through filter: the lowerer's rows are exactly the
    rows `text_fold.matches` keeps, for like and like_exact."""
    by_id = {f"doc:p{i:03d}": value for i, value in enumerate(CORPUS)}
    assert len(by_id) >= 200
    for op in ("like", "like_exact"):
        for operand in _PARITY_OPERANDS:
            if text_fold.operand_problem(op, operand) is not None:
                continue
            reply = tools.call("filter", {"where": _like("attrs.v", operand, op), "limit": 1000,
                                          "select": ["node_id"]})
            expected = sorted(i for i, v in by_id.items() if text_fold.matches(op, v, operand))
            assert _ids(reply) == expected, (op, operand)


def test_every_code_point_folds_alike_in_sql_and_python(clean_runtime):
    """Both folds over every code point but NUL and the surrogates. A code
    point assigned in only one Unicode version is listed, never compared."""
    marks = text_fold.mark_pattern()
    differ: list[str] = []
    only_one: list[str] = []
    with psycopg.connect(clean_runtime) as conn:
        pg_unicode = conn.execute("SELECT unicode_version()").fetchone()[0]
        print(f"python unicode {unicodedata.unidata_version}, postgresql unicode {pg_unicode}")
        sql = (
            f"SELECT n, {text_fold.fold_sql('chr(n)', '%(marks)s')}, normalize(chr(n), NFC) "
            "FROM generate_series(%(lo)s::int, %(hi)s::int) AS n "
            "WHERE n NOT BETWEEN 55296 AND 57343"
        )
        step = 65536
        for lo in range(1, 0x110000, step):
            rows = conn.execute(sql, {"marks": marks, "lo": lo, "hi": min(lo + step, 0x110000) - 1})
            for n, folded, composed in rows:
                char = chr(n)
                if folded == text_fold.fold(char) and composed == text_fold.nfc(char):
                    continue
                if unicodedata.category(char) == "Cn":
                    only_one.append(f"U+{n:04X}")
                else:
                    differ.append(f"U+{n:04X} sql={folded!r} python={text_fold.fold(char)!r}")
    print(f"assigned in only one version: {len(only_one)} {only_one[:40]}")
    assert not differ, differ[:40]


#: The relations a graph read names: the stored records and links, and the
#: views and fact tables over them. Bookkeeping around a call (the trace
#: insert, lock release, catalog lookups) names none of them.
_GRAPH_READ = re.compile(
    r"\bokg\.(graph_nodes|graph_edges|v_nodes|v_edges|node_facts|edge_facts"
    r"|attr_ts_v|attribute_versions|nodes_live|edges_live)\b"
)


def _record_statements(monkeypatch) -> list[str]:
    """Every statement sent to the database from now on, as its full text."""
    seen: list[str] = []
    for cls in (psycopg.Connection, psycopg.Cursor, psycopg.ServerCursor):
        original = cls.execute

        def recorded(self, query, *args, _original=original, **kwargs):
            conn = self if isinstance(self, psycopg.Connection) else self.connection
            if isinstance(query, psycopg.sql.Composable):
                seen.append(query.as_string(conn))
            elif isinstance(query, bytes):
                seen.append(query.decode("utf-8", "replace"))
            else:
                seen.append(str(query))
            return _original(self, query, *args, **kwargs)

        monkeypatch.setattr(cls, "execute", recorded)
    return seen


def _graph_reads(seen: list[str]) -> list[str]:
    return [statement[:200] for statement in seen if _GRAPH_READ.search(statement)]


@pytest.mark.parametrize("mode", ["audit", "enforce"])
@pytest.mark.parametrize("op", ["like", "like_exact"])
def test_under_access_rules_like_is_refused_before_any_graph_read(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode, op,
):
    from okg.substrate.mcp import server as srv

    deployment = _deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        test = {"op": op, "field": "attrs.author_name", "value": "muller"}
        calls = [
            ("filter", {"input": {"kind": "nodes", "subtype": "doc"}, "predicate": test}),
            ("map", {"input": {"kind": "nodes", "subtype": "doc", "filter": test}}),
            ("aggregate", {"input": {"kind": "nodes", "subtype": "doc", "filter": test},
                           "by": ["subtype"], "metrics": ["count"]}),
            ("expand", {"input": "ticket:hub", "filter": test}),
        ]
        for tool, kwargs in calls:
            # The detector proves itself per tool: the same call with eq in
            # place of the like test reads the graph, in this mode.
            control = json.loads(json.dumps(kwargs).replace(f'"op": "{op}"', '"op": "eq"'))
            # Two have no served call to prove it with, and say so: under
            # access rules every set count (aggregate) is refused before it
            # reads, since no rule check runs inside a GROUP BY; and enforce
            # with no active policy bundle denies every call before it
            # reads. There the detector is the one proven under audit, on
            # the same tool and the same refusal path.
            seen = _record_statements(monkeypatch)
            try:
                controlled = mcp._tool_manager._tools[tool].fn(**control)
            except SubstrateError as exc:
                denied = str(exc)
                assert (
                    (tool == "aggregate" and "counting is not available" in denied)
                    or (mode == "enforce" and "no policy bundle is active" in denied)
                ), (tool, mode, denied)
            else:
                assert mode == "audit" and tool != "aggregate", (tool, mode, controlled)
                assert _graph_reads(seen), (tool, mode, controlled, seen)
            # What matters is that no graph record is read before the
            # refusal; the trace layer around a tool call still writes.
            seen = _record_statements(monkeypatch)
            reply = mcp._tool_manager._tools[tool].fn(**kwargs)
            assert reply["status"] == "refused", (tool, reply)
            assert reply["error"]["code"] == "unsupported_operator_form"
            assert reply["error"]["data"] == {"reason": "loose_match_under_access_rules"}
            assert reply["error"]["message"] == (
                f"{tool}: like is not served under access rules yet; use eq, in, "
                "prefix or suffix"
            )
            assert _graph_reads(seen) == [], (tool, seen)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


#: Refusals whose offered call holds a substring test: an operator written
#: as a key (like, ilike, like_exact), offered as like where like is
#: served, and a like in search's `where`, offered as search's own call
#: with `contains` (review of read stack PR 2, #3666).
_LIKE_OFFERS = [
    ("filter", {"where": {"type": "doc", "attrs.author_name": {"like": "muller"}}}),
    ("filter", {"where": {"type": "doc", "attrs.author_name": {"ilike": "muller"}}}),
    ("map", {"where": {"type": "doc", "attrs.author_name": {"like": "muller"}}}),
    ("aggregate", {"where": {"type": "doc", "attrs.author_name": {"like": "muller"}},
                   "by": ["subtype"], "metrics": ["count"]}),
    ("expand", {"input": "ticket:hub",
                "where": {"attrs.author_name": {"like_exact": "Müller"}}}),
    ("search", {"query": TERM, "where": _like("attrs.author_name", "%muller%")}),
]


def _offers(tools: Tools) -> list[tuple[str, dict, Any, Any]]:
    """`(tool, refusal, offered call, what the offered call returned)` for
    each of `_LIKE_OFFERS`, in one client session, as a client reads them."""
    async def steps(call):
        out = []
        for tool, arguments in _LIKE_OFFERS:
            reply = await call(tool, arguments)
            retry = (reply.get("error") or {}).get("retry_call")
            ran = await call(retry["tool"], retry["arguments"]) if retry else None
            out.append((tool, reply, retry, ran))
        return out

    return tools.run(steps)


def test_where_like_is_served_a_refusal_offers_the_like_call_and_it_is_served(tools):
    """The control for the case below: with access rules off on a database
    that folds, each refusal offers a call with a like test, served."""
    for tool, reply, retry, ran in _offers(tools):
        assert reply["status"] == "refused", (tool, reply)
        offered = '"op": "contains"' if tool == "search" else '"op": "like'
        assert retry is not None and offered in json.dumps(retry["arguments"]), (tool, reply)
        assert ran.get("status") != "refused", (tool, retry, ran)


@pytest.mark.parametrize("situation", ["audit", "enforce", "database_below_17"])
def test_where_like_is_refused_no_refusal_offers_a_like_call(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, situation,
):
    """An offered call is never itself refused (loose-match-3666): where the
    read refuses like (access rules on, or a database that cannot fold), a
    refusal offers no call with a like test, and any call it does offer is
    served when run as written."""
    deployment = _deployment(tmp_path, clean_runtime)
    if situation == "database_below_17":
        monkeypatch.setattr(text_fold, "check_database", lambda execute: text_fold.database_problem(
            {"version": 160004, "collation": False, "encoding": "UTF8"}))
    else:
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", situation)
    tools = Tools(clean_runtime, deployment)
    try:
        # The like call itself is refused here, by name.
        direct = tools.call("filter", {"where": _like("attrs.author_name", "muller")})
        assert direct["status"] == "refused", direct
        assert direct["error"]["data"]["reason"] in (
            "loose_match_under_access_rules", "loose_match_needs_postgresql_17",
        ), direct
        for tool, reply, retry, ran in _offers(tools):
            assert reply["status"] == "refused", (tool, reply)
            if retry is None:
                continue
            assert '"op": "like' not in json.dumps(retry["arguments"]), (tool, reply)
            if (ran.get("error") or {}).get("code") == "nomos_runtime_denied":
                # Search's own contains call (review of read stack PR 2,
                # #3666): where the access rules deny search to this reader
                # outright, the offered search call is denied as the plain
                # search call is; the offer itself is not what is refused.
                plain = tools.call(retry["tool"], {"query": TERM, "where": {"type": "doc"}})
                assert retry["tool"] == "search", (tool, retry, ran)
                assert (plain.get("error") or {}).get("code") == "nomos_runtime_denied", (
                    tool, plain)
                continue
            assert ran.get("status") != "refused", (tool, retry, ran)
    finally:
        tools.close()


def test_a_like_read_that_does_not_finish_is_refused_not_served_short(tools, monkeypatch):
    """A like read stopped by its statement timeout is never served as a
    shorter answer. The timeout is the error PostgreSQL raises for it,
    raised on the read's own statement (the one that folds)."""
    for cls in (psycopg.Cursor, psycopg.ServerCursor):
        original = cls.execute

        def cancelled(self, query, *args, _original=original, **kwargs):
            text = (query.as_string(self.connection)
                    if isinstance(query, psycopg.sql.Composable) else str(query))
            if "COLLATE pg_c_utf8" in text:
                raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")
            return _original(self, query, *args, **kwargs)

        monkeypatch.setattr(cls, "execute", cancelled)
    text = tools.call("filter", {"where": _like("attrs.author_name", "muller")}, "text")
    # The refusal is the general one for a read that stopped (no rows, no
    # count); a named did-not-finish refusal for like is not built here.
    assert text.startswith("filter refused operator_execution_failed"), text
    assert "doc:" not in text and "total" not in text, text
