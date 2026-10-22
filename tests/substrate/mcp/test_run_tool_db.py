"""`run`: several read calls in order through the served tool (issue #3666).

PostgreSQL tests on a small published graph: one file changed by six
commits (one a merge, one a revert), a second file changed by one, three
tickets and 60 notes. Every call goes through one MCP client session, so
each step meets the checks, rewrites and byte bounds a call sent alone
meets, and every assertion reads the reply as a client receives it.
"""
from __future__ import annotations

import json

import anyio
import pytest

from okg.substrate.library.sources.base import EdgeFact
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

pytestmark = pytest.mark.postgres

FILE = "file:repo:lib/rule.py"
OTHER = "file:repo:lib/other.py"
COMMITS = [
    # id, committed_at, is_merge, subject, ticket key
    ("commit:repo:c1", "2025-01-05T10:00:00Z", False, "Add rule cache", "T-1"),
    ("commit:repo:c2", "2025-01-20T10:00:00Z", False, "Fix rule lock", "T-2"),
    ("commit:repo:c3", "2025-02-02T10:00:00Z", True, "Merge branch main", None),
    ("commit:repo:c4", "2025-02-10T10:00:00Z", False, "Tune rule timeout", None),
    ("commit:repo:c5", "2025-03-01T10:00:00Z", False, 'Revert "Add rule cache"', None),
    ("commit:repo:c6", "2025-03-15T10:00:00Z", False, "Rule docs", "T-9"),
]
LONE = ("commit:repo:c7", "2025-03-20T10:00:00Z", False, "Other change", "T-3")


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type="affects",
        source_record_id={"id": f"{src}|affects|{dst}"}, source_revision={"v": 1},
    )


def _corpus(tmp_path, dsn: str) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [
        node(FILE, "file", "rule module", path="lib/rule.py"),
        node(OTHER, "file", "other module", path="lib/other.py"),
    ]
    edges = []
    for commit_id, when, merge, subject, key in [*COMMITS, LONE]:
        attrs = {"committed_at": when, "is_merge": merge, "subject": subject}
        if key is not None:
            attrs["ticket_key"] = key
        nodes.append(node(commit_id, "commit", subject, **attrs))
    for commit_id, *_rest in COMMITS:
        edges.append(_edge(commit_id, FILE))
    edges.append(_edge(LONE[0], OTHER))
    for key, status in (("T-1", "Open"), ("T-2", "Closed"), ("T-3", "Open")):
        nodes.append(node(f"ticket:{key}", "ticket", f"ticket {key}", key=key, status=status))
    # 60 notes: more than inspect reads at once. One label holds text that
    # looks like an SQL placeholder; two hold values JSON compares apart.
    for i in range(NOTES):
        label = PLACEHOLDER_LABEL if i == 0 else f"note {i:02d}"
        nodes.append(node(f"note:{i:02d}", "note", f"note {i}", label=label,
                          weight=1 if i == 1 else (True if i == 2 else None)))
    publish_nodes(
        dsn, deployment, subtypes=SUBTYPES,
        narrowings=[("commit", "affects", "file")],
        edges=edges, nodes=nodes,
    )
    return deployment


NOTES = 60
PLACEHOLDER_LABEL = "a%(x)s 100%"
SUBTYPES = [SubtypeSpec("file"), SubtypeSpec("commit"), SubtypeSpec("ticket"), SubtypeSpec("note")]
SET_SENTENCE = "a set lasts for one run call; put both reads in one run(steps=[...])"


class Server:
    def __init__(self, dsn: str, deployment: str) -> None:
        from okg.substrate.mcp import server as srv

        self.mcp = srv.build_server(dsn=dsn, deployment_name=deployment)

    def close(self) -> None:
        self.mcp._okg_pool.close()
        if self.mcp._okg_write_pool:
            self.mcp._okg_write_pool.close()

    def call(self, tool: str, arguments: dict, result_format: str = "text"):
        from mcp.shared.memory import create_connected_server_and_client_session

        out: list = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(self.mcp) as client:
                out.append(await client.call_tool(
                    tool, arguments, meta={"okg/result_format": result_format},
                ))

        anyio.run(drive)
        return out[0]

    def text(self, steps: list, **extra) -> str:
        result = self.call("run", {"steps": steps, **extra})
        assert not result.isError, result.content[0].text
        return result.content[0].text

    def json(self, steps: list, **extra) -> dict:
        result = self.call("run", {"steps": steps, **extra}, "json")
        assert not result.isError, result.content[0].text
        return json.loads(result.content[0].text)


@pytest.fixture
def served(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Server(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


EXPAND_FILE = {"expand": {
    "from": FILE, "edge_type": "affects", "direction": "in",
    "where": {"attrs.is_merge": False},
}, "show": False}


def _step(reply: dict, number: int) -> dict:
    step = reply["steps"][number - 1]
    assert step["status"] == "ok", step
    return step["result"]


def _ids(result: dict, key: str = "items") -> list[str]:
    return sorted(str(row["node_id"]) for row in result[key])


def _step_text(text: str, number: int) -> str:
    """The lines of step `number` in a text reply."""
    out, inside = [], False
    for line in text.splitlines():
        if line.startswith("[step "):
            inside = line.startswith(f"[step {number}]")
        if inside:
            out.append(line)
    return "\n".join(out)


def _step_status(reply: dict, number: int) -> str:
    return reply["steps"][number - 1]["status"]


def test_expand_then_count_per_month_over_the_set(served):
    """The commits that changed a file, without merges, counted per month;
    the set is not printed, and each step says its exact count."""
    steps = [
        EXPAND_FILE,
        {"aggregate": {"where": "$1", "by": ["month(date.attrs.committed_at)"],
                       "metrics": ["count"]}, "show": True},
        {"map": {"where": "$1", "select": ["node_id", "attrs.subject"]}},
    ]
    reply = served.json(steps)
    counts = {
        str(group["month(date.attrs.committed_at)"])[:7]: group["count"]
        for group in _step(reply, 2)["groups"]
    }
    assert counts == {"2025-01": 2, "2025-02": 1, "2025-03": 2}
    assert _ids(_step(reply, 3)) == [
        "commit:repo:c1", "commit:repo:c2", "commit:repo:c4",
        "commit:repo:c5", "commit:repo:c6",
    ]
    first = reply["steps"][0]
    assert first["rows_shown"] == 0 and first["count"] == 5 and "result" not in first
    assert reply["steps"][1]["count"] == 5 and reply["steps"][1]["groups"] == 3
    lines = served.text(steps).splitlines()
    assert lines[0] == "run ok · 3 steps"
    assert lines[1] == "[step 1] expand: 5 records"
    assert "[step 2] aggregate: 5 records in 3 groups" in lines
    assert "[step 3] map: 5 records" in lines


def test_values_of_one_step_select_records_in_the_next(served):
    """Ticket keys found on commits, then those tickets."""
    reply = served.json([
        {"filter": {"where": {"type": "commit", "attrs.ticket_key": {"op": "exists"}}}},
        {"filter": {"where": {"type": "ticket", "attrs.key": "$1.attrs.ticket_key"},
                    "select": ["node_id", "attrs.status"]}},
    ])
    # T-9 is on a commit and names no ticket: no reshaping, no guess.
    assert _ids(_step(reply, 2)) == ["ticket:T-1", "ticket:T-2", "ticket:T-3"]
    narrowed = served.json([
        {"filter": {"where": {"type": "commit", "attrs.is_merge": False,
                              "attrs.subject": {"op": "prefix", "value": "Fix"}}}},
        {"filter": {"where": {"type": "ticket",
                              "attrs.key": {"op": "in", "values": "$1.attrs.ticket_key"}}}},
    ])
    assert _ids(_step(narrowed, 2)) == ["ticket:T-2"]


def test_a_set_combines_with_and_and_not(served):
    reply = served.json([
        {"expand": {"from": FILE, "edge_type": "affects", "direction": "in"}},
        {"filter": {"where": {"op": "and", "clauses": [
            "$1", {"attrs.subject": {"op": "prefix", "value": "Revert"}}]}}, "show": True},
        {"filter": {"where": {"op": "and", "clauses": [
            "$1", {"op": "not", "clause": {"attrs.is_merge": True}}]}}, "show": True},
        {"aggregate": {"where": {"op": "and", "clauses": [
            {"type": "commit"}, {"op": "not", "clause": "$1"}]},
            "by": ["attrs.subject"], "metrics": ["count"]}},
    ])
    assert _ids(_step(reply, 2)) == ["commit:repo:c5"]
    assert len(_step(reply, 3)["items"]) == 5
    assert "commit:repo:c3" not in _ids(_step(reply, 3))
    groups = _step(reply, 4)["groups"]
    assert [(g["attrs.subject"], g["count"]) for g in groups] == [("Other change", 1)]


def test_inspect_reads_a_set(served):
    reply = served.json([
        {"filter": {"where": {"type": "ticket", "attrs.status": "Open"}}},
        {"inspect": {"target": {"kind": "nodes", "set": "$1"}}},
    ])
    assert reply["steps"][1]["status"] == "ok", reply["steps"][1]
    assert "ticket:T-1" in json.dumps(reply["steps"][1]["result"])
    assert "ticket:T-2" not in json.dumps(reply["steps"][1]["result"])


def test_a_refused_step_skips_the_steps_that_need_it(served):
    text = served.text([
        {"filter": {"where": {"type": "commit"}, "bogus_argument": 1}},
        {"aggregate": {"where": "$1", "by": ["attrs.subject"], "metrics": ["count"]}},
        {"filter": {"where": {"type": "ticket"}}},
    ])
    lines = text.splitlines()
    assert lines[0] == "run ok · 3 steps · 1 refused · 1 skipped"
    assert lines[1].startswith("[step 1] filter refused")
    assert "bogus_argument" in text
    assert "[step 2] aggregate skipped: needs step 1" in lines
    assert "[step 3] filter: 3 records" in lines


def test_a_later_reference_is_refused_before_any_step_runs(served):
    """Failure case: the whole run is refused, naming the steps that exist."""
    result = served.call("run", {"steps": [
        {"filter": {"where": "$2"}},
        {"filter": {"where": {"type": "ticket"}}},
    ]})
    text = result.content[0].text
    assert result.isError, text
    assert "step 1 names $2" in text and "steps 1 to 2" in text
    assert "ticket:T-1" not in text


def test_the_printed_steps_share_max_bytes(served):
    steps = [
        {"filter": {"where": {"type": "note"}}, "show": True},
        {"map": {"where": {"type": "note"}, "select": ["node_id", "attrs.label"]}, "show": True},
    ]
    text = served.text(steps * 3, max_bytes=9000)
    assert len(text.encode("utf-8")) <= 9000, len(text.encode("utf-8"))
    assert "! cut to fit" in text
    small = served.call("run", {"steps": steps * 3, "max_bytes": 4096})
    assert small.isError
    assert "cannot hold 6 printed steps" in small.content[0].text


def test_run_is_refused_under_access_rules(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    server = Server(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
        result = server.call("run", {"steps": [{"filter": {"where": {"type": "commit"}}}]})
        text = result.content[0].text
        assert result.isError
        assert "access rules" in text
        assert 'send the first step as its own call' in text
        assert "filter(where=" in text
        assert "commit:repo:c1" not in text
    finally:
        server.close()


# --------------------------------------------------------------------------- #
# no set name outlives one call                                               #
# --------------------------------------------------------------------------- #


def _session(server: Server, *requests: tuple[str, dict, str]) -> list:
    """Each ``(tool, arguments, format)`` through ONE client session."""
    from mcp.shared.memory import create_connected_server_and_client_session

    out: list = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(server.mcp) as client:
            for tool, arguments, result_format in requests:
                out.append(await client.call_tool(
                    tool, arguments, meta={"okg/result_format": result_format},
                ))

    anyio.run(drive)
    return out


def _served_text(result) -> str:
    assert not result.isError, result.content[0].text
    return result.content[0].text


RETAIN_CALLS = {
    "filter": {"where": {"type": "ticket"}, "retain": True},
    "map": {"where": {"type": "ticket"}, "select": ["node_id"], "retain": True},
    "aggregate": {"where": {"type": "commit"}, "by": ["attrs.is_merge"],
                  "metrics": ["count"], "retain": True},
    "expand": {"input": {"node_id": FILE}, "edge_type": "affects", "direction": "in",
               "retain": True},
    "search": {"method": "attrs", "attr_filters": {"status": "Open"},
               "subtype_filter": "ticket", "retain": True},
}


def test_retain_is_refused_with_the_run_pointer(served):
    """Failure cases: retain where only a set name answered it is refused
    with the sentence that sends both reads to one run call; no reply
    carries a set name."""
    replies = _session(served, *[(tool, args, "text") for tool, args in RETAIN_CALLS.items()])
    for tool, reply in zip(RETAIN_CALLS, replies):
        text = reply.content[0].text
        assert reply.isError, (tool, text)
        assert "set_names_removed" in text and SET_SENTENCE in text, (tool, text)
        assert "set $r" not in text


def test_a_retained_name_is_refused_by_name(served):
    for tool, arguments in (
        ("filter", {"where": "$r1"}),
        ("aggregate", {"where": {"op": "and", "clauses": ["$r2", {"type": "commit"}]},
                       "by": ["subtype"], "metrics": ["count"]}),
        ("inspect", {"target": {"kind": "nodes", "set": "$r1"}}),
    ):
        result = served.call(tool, arguments)
        text = result.content[0].text
        assert result.isError, text
        assert "set_names_removed" in text and SET_SENTENCE in text
    # Inside a run too, and a step name outside its run says where it works.
    reply = served.json([{"filter": {"where": "$r1"}}])
    assert reply["steps"][0]["status"] == "refused"
    assert "set_names_removed" in json.dumps(reply["steps"][0])
    alone = served.call("filter", {"where": "$1"})
    assert alone.isError and SET_SENTENCE in alone.content[0].text


def test_lexical_search_retain_is_unchanged(served):
    """A lexical search keeps its result reference, as on dev."""
    (result,) = _session(served, ("search", {"query": "rule", "retain": True}, "json"))
    reply = json.loads(_served_text(result))
    assert reply.get("result_reference") is not None, reply
    assert "set" not in reply


def test_retain_on_a_filter_over_a_saved_result_still_makes_a_child_reference(served):
    """`filter(input={"ref": id}, retain=true)` keeps working end to end, as
    on dev: a lexical search's reference, filtered with retain, answers a
    new reference whose members a later read returns."""
    (search,) = _session(served, ("search", {"query": "rule", "retain": True}, "json"))
    parent = json.loads(_served_text(search))["result_reference"]["id"]
    child, = _session(served, ("filter", {
        "input": {"ref": parent}, "predicate": {"op": "eq", "field": "subtype", "value": "commit"},
        "retain": True,
    }, "json"))
    reply = json.loads(_served_text(child))
    child_id = reply["result_reference"]["id"]
    assert child_id != parent
    filtered = sorted(str(row["node_id"]) for row in reply["items"])
    assert filtered and all(node.startswith("commit:") for node in filtered)
    read_back, = _session(served, ("map", {"input": {"ref": child_id},
                                           "projection": ["node_id"]}, "json"))
    assert sorted(str(r["node_id"]) for r in json.loads(_served_text(read_back))["items"]) == filtered
    assert "set_names_removed" not in _served_text(child)


def test_retain_and_set_names_under_access_rules(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """Under access rules retain is refused as on dev, and a set name is
    refused by name before any read."""
    server = Server(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
        retained, named = _session(
            server,
            ("filter", {"where": {"type": "ticket"}, "retain": True}, "text"),
            ("filter", {"where": "$r1"}, "text"),
        )
        assert retained.isError
        assert "result_reference_unsupported" in retained.content[0].text
        assert named.isError and "set_names_removed" in named.content[0].text
    finally:
        server.close()


# --------------------------------------------------------------------------- #
# review cases: stored text, budgets, pins, sizes, and each producer           #
# --------------------------------------------------------------------------- #


def test_a_stored_value_that_looks_like_a_placeholder_is_only_text(served):
    reply = served.json([
        {"filter": {"where": {"type": "note", "attrs.label": PLACEHOLDER_LABEL}}},
        {"filter": {"where": {"type": "note", "attrs.label": "$1.attrs.label"}}, "show": True},
        {"map": {"where": "$2", "select": ["node_id", "attrs.label"]}, "show": True},
        {"aggregate": {"where": {"op": "and", "clauses": ["$3", {"type": "note"}]},
                       "by": ["attrs.label"], "metrics": ["count"]}},
    ])
    assert _ids(_step(reply, 2)) == ["note:00"]
    assert _ids(_step(reply, 3)) == ["note:00"]
    assert [g["count"] for g in _step(reply, 4)["groups"]] == [1]


def test_json_values_compare_as_sql_does(served):
    reply = served.json([
        {"filter": {"where": {"node_id": ["note:01", "note:02", "note:03"]}}},
        {"filter": {"where": {"type": "note", "attrs.weight": "$1.attrs.weight"}}, "show": True},
        {"aggregate": {"where": {"type": "note", "attrs.weight": "$1.attrs.weight"},
                       "by": ["attrs.weight"], "metrics": ["count"]}},
    ])
    assert _ids(_step(reply, 2)) == ["note:01", "note:02"]
    assert sum(g["count"] for g in _step(reply, 3)["groups"]) == 2


def test_a_set_in_a_search_condition_is_refused_by_name(served):
    text = served.text([
        {"filter": {"where": {"type": "ticket"}}},
        {"search": {"query": "rule", "where": "$1"}},
    ])
    step = _step_text(text, 2)
    assert step.startswith("[step 2] search refused"), step
    assert "does not take a set" in step


def test_an_aggregate_step_names_the_records_it_counted(served):
    reply = served.json([
        {"aggregate": {"where": {"type": "commit", "attrs.is_merge": True},
                       "by": ["attrs.subject"], "metrics": ["count"]}},
        {"filter": {"where": "$1", "select": ["node_id"]}},
    ])
    assert _ids(_step(reply, 2)) == ["commit:repo:c3"]


def test_node_id_and_type_values_pass_from_one_step_to_the_next(served):
    reply = served.json([
        {"filter": {"where": {"type": "ticket", "attrs.status": "Open"}}},
        {"filter": {"where": {"node_id": "$1.node_id"}, "select": ["node_id"]}, "show": True},
        {"filter": {"where": {"node_id": "ticket:T-2"}}},
        {"aggregate": {"where": {"subtype": "$3.type"}, "by": ["subtype"],
                       "metrics": ["count"]}},
    ])
    assert _ids(_step(reply, 2)) == ["ticket:T-1", "ticket:T-3"]
    assert [(g["subtype"], g["count"]) for g in _step(reply, 4)["groups"]] == [("ticket", 3)]


def test_a_value_list_in_an_operator_keyed_test_is_served_with_a_note(served):
    """`{"node_id": {"in": ["$1.node_id"]}}` selects what the long form does,
    and the step's reply says how it was read, in text and in JSON."""
    steps = [
        {"filter": {"where": {"type": "ticket", "attrs.status": "Open"}}, "show": False},
        {"filter": {"where": {"node_id": {"in": ["$1.node_id"]}}, "select": ["node_id"]}},
    ]
    reply = served.json(steps)
    assert _ids(_step(reply, 2)) == ["ticket:T-1", "ticket:T-3"]
    assert {
        "rule": "operator_as_key",
        "sent": '{"node_id": {"in": ...}}',
        "served": '{"node_id": {"op": "in", "values": ...}}',
    } in _step(reply, 2)["adjusted"]
    step = _step_text(served.text(steps), 2).splitlines()
    assert step[0] == "[step 2] filter: 2 records", step
    assert any(line.startswith(
        '! adjusted: {"node_id": {"in": ...}} read as '
        '{"node_id": {"op": "in", "values": ...}}'
    ) for line in step), step
    assert any("ticket:T-3" in line for line in step)


def test_a_set_field_as_expand_input_and_an_unread_reference_are_refused_truthfully(served):
    """Failure cases inside a run: a field's values as expand's `input` is
    refused with the join form that works, and a reference where no set is
    read is refused by name; neither reply sends the caller to run."""
    text = served.text([
        {"filter": {"where": {"type": "ticket"}}, "show": False},
        {"expand": {"input": "$1.node_id", "edge_type": "affects", "direction": "in"}},
        {"filter": {"where": {"attrs.key": {"in": ["$1.attrs.key"], "gt": 1}}}},
    ])
    assert "run(steps=" not in text and "send the steps together" not in text, text
    expand = _step_text(text, 2)
    assert expand.startswith("[step 2] expand refused"), expand
    assert "expand from a set takes $1 itself as from, with one link type" in expand
    assert "expand-records" not in text and "does not serve yet" not in text
    unread = _step_text(text, 3)
    assert unread.startswith("[step 3] filter refused"), unread
    assert "$1 stands where no set is read" in unread


def test_inspect_over_more_than_fifty_records_is_refused_by_name(served):
    reply = served.json([
        {"filter": {"where": {"type": "note"}}, "show": False},
        {"inspect": {"target": {"kind": "nodes", "set": "$1"}}},
    ])
    assert _step_status(reply, 2) == "refused"
    assert "inspect reads at most 50 records" in json.dumps(reply["steps"][1])


def test_a_row_check_over_a_set_past_the_limit_is_refused_by_name(served, monkeypatch):
    from okg.substrate.mcp import run_steps

    monkeypatch.setattr(run_steps, "MATERIALIZE_CAP", 10)
    # A plain filter's set is bound as its own condition (no row check);
    # this reads the same set as a marker, which a row check reads.
    monkeypatch.setattr(run_steps, "inlinable_predicate", lambda definition: None)
    reply = served.json([
        {"filter": {"where": {"type": "note"}}, "show": False},
        {"filter": {"where": "$1"}, "show": True},
        {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
    ])
    assert _step_status(reply, 2) == "refused"
    assert "holds more than 10 records or values" in json.dumps(reply["steps"][1])
    # A count reads the set in SQL alone and is not limited.
    assert [g["count"] for g in _step(reply, 3)["groups"]] == [NOTES]


def test_a_step_past_its_time_budget_is_refused_by_name(served, monkeypatch):
    from okg.substrate.mcp import run_steps

    monkeypatch.setattr(run_steps, "STEP_BUDGET_MS", 0)
    reply = served.json([
        {"filter": {"where": {"type": "ticket"}}},
        {"filter": {"where": {"type": "commit"}}},
    ])
    assert _step_status(reply, 1) == "refused"
    assert "time budget" in json.dumps(reply["steps"][0])
    assert _step_status(reply, 2) == "refused"


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_no_record_enters_a_set_or_a_value_list_under_access_rules(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode,
):
    """Under audit or enforce the run is refused before any set is read, and
    a set name is refused by name, so a record the rules would deny never
    enters `$n` or a `$n.<field>` list."""
    server = Server(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
        run_reply, named = _session(
            server,
            ("run", {"steps": [
                {"filter": {"where": {"type": "commit", "attrs.ticket_key": {"op": "exists"}}}},
                {"filter": {"where": {"type": "ticket", "attrs.key": "$1.attrs.ticket_key"}}},
                {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
            ]}, "text"),
            ("filter", {"where": "$r1"}, "text"),
        )
        for result in (run_reply, named):
            text = result.content[0].text
            assert result.isError, text
            assert "ticket:T-1" not in text and "commit:repo:c1" not in text
        assert "access rules" in run_reply.content[0].text
        assert "set_names_removed" in named.content[0].text
    finally:
        server.close()


def test_a_date_step_inside_a_run_stops_at_the_run_budget(served, monkeypatch):
    import time

    from okg.substrate.mcp import run_steps

    monkeypatch.setattr(run_steps, "STEP_BUDGET_MS", 120)
    started = time.monotonic()
    reply = served.json([
        {"filter": {"where": {"type": "commit", "date.attrs.committed_at": {
            "op": "between", "values": ["2025-01-01", "2025-12-31"]}}}},
    ])
    elapsed = time.monotonic() - started
    assert _step_status(reply, 1) == "refused"
    assert "budget" in json.dumps(reply["steps"][0])
    assert elapsed < 3.0


def _pooled_timeouts(server: Server) -> list[str]:
    """`statement_timeout` on every connection the read pool holds now."""
    pool = server.mcp._okg_pool
    held = []
    try:
        for _ in range(pool.get_stats().get("pool_available", 0)):
            held.append(pool.getconn(timeout=5))
        out = []
        for conn in held:
            with conn.cursor() as cur:
                cur.execute("SELECT current_setting('statement_timeout')")
                out.append(cur.fetchone()[0])
        return out
    finally:
        for conn in held:
            pool.putconn(conn)


def test_a_statement_the_step_budget_cancels_is_refused_and_the_timeout_restored(
    served, monkeypatch,
):
    from okg.substrate.mcp import run_steps

    real = run_steps.statement_budget_ms
    # A 1 ms step budget, so the deadline has come when the 1 ms statement
    # timeout the budget sets cancels the step's statement.
    monkeypatch.setattr(run_steps, "STEP_BUDGET_MS", 1)
    monkeypatch.setattr(
        run_steps, "statement_budget_ms",
        # 1 ms for a step's statements only (a step opens its budget events).
        lambda: (1 if run_steps._EVENTS.get() is not None else real()),
    )
    reply = served.json([
        {"aggregate": {"where": {"type": "note"}, "by": ["attrs.label"],
                       "metrics": ["count"]}},
    ])
    assert _step_status(reply, 1) == "refused"
    assert "run_over_budget" in json.dumps(reply["steps"][0])
    monkeypatch.setattr(run_steps, "statement_budget_ms", real)
    monkeypatch.setattr(run_steps, "STEP_BUDGET_MS", 5_000)
    assert "1ms" not in _pooled_timeouts(served)
    after = served.json([{"filter": {"where": {"type": "ticket"}}}])
    assert _step_status(after, 1) == "ok"


TEXT_SET_STEPS = {
    "filter": {"select": ["node_id"]},
    "map": {"select": ["node_id"]},
    "aggregate": {"by": ["attrs.subject"], "metrics": ["count"]},
}


@pytest.mark.parametrize("tool", sorted(TEXT_SET_STEPS))
@pytest.mark.parametrize(("words", "expected"), [
    ("cache", ["commit:repo:c1", "commit:repo:c5"]),
    # Failure case: text no commit holds names an empty set, never every
    # commit the rest of the condition admits.
    ("zzqxnomatch", []),
])
def test_a_step_with_a_text_condition_names_only_the_records_it_matched(
    served, tool, words, expected,
):
    """A text clause is read as the ids of the matching records beside the
    rest of the condition; the set a later step reads is those records, not
    every record of the type."""
    where = {"type": "commit", "text": {"op": "matches", "value": words}}
    reply = served.json([
        {tool: {"where": where, **TEXT_SET_STEPS[tool]}},
        {"filter": {"where": "$1", "select": ["node_id"]}, "show": True},
        {"aggregate": {"where": "$1", "by": ["attrs.subject"], "metrics": ["count"]}},
    ])
    # Each step states its exact count; only the shown steps carry rows.
    assert [step["count"] for step in reply["steps"]] == [len(expected)] * 3
    assert "result" not in reply["steps"][0]
    assert _ids(_step(reply, 2)) == expected
    assert sum(group["count"] for group in _step(reply, 3)["groups"]) == len(expected)


ALL_FILE_COMMITS = sorted(commit_id for commit_id, *_rest in COMMITS)


@pytest.mark.parametrize(("subtype", "expected"), [
    ("commit", ALL_FILE_COMMITS),
    # Failure case: no ticket links to the file, so the set is empty, never
    # every far record the link reaches (the set once dropped the type).
    ("ticket", []),
])
def test_an_expand_step_set_keeps_its_target_type(served, subtype, expected):
    """#3666: "$1" from an expand step with target_subtype names only the far
    records of that type, and the step's count equals its page's total."""
    reply = served.json([
        {"expand": {"from": FILE, "edge_type": "affects", "direction": "in",
                    "target_subtype": subtype}, "show": True},
        {"filter": {"where": "$1", "select": ["node_id"]}, "show": True},
    ])
    assert [step["status"] for step in reply["steps"]] == ["ok"] * 2, reply["steps"]
    assert [step["count"] for step in reply["steps"]] == [len(expected)] * 2
    page = _step(reply, 1)
    assert page["total_count"] == reply["steps"][0]["count"], page
    assert _ids(_step(reply, 2)) == expected


TEXT_PRODUCERS = {
    "expand": {"from": FILE, "edge_type": "affects", "direction": "in"},
    "filter": {"select": ["node_id"]},
}


@pytest.mark.parametrize("tool", sorted(TEXT_PRODUCERS))
@pytest.mark.parametrize(("words", "expected"), [
    ("cache", ["commit:repo:c1", "commit:repo:c5"]),
    # Failure case: no linked record holds the text, so the set is empty,
    # never every record the rest of the condition admits.
    ("zzqxnomatch", []),
])
def test_a_text_step_set_is_exactly_its_matches_and_is_never_inlined(
    served, monkeypatch, tool, words, expected,
):
    """#3666: an expand step whose far-end condition holds a text clause
    names exactly the expanded records as "$1" (it was refused "cannot be
    lowered exactly"), and a text step's set, the ids its text matched, is
    read by later steps as the set, never inlined as an id-list condition."""
    from okg.substrate.mcp import run_steps

    leaves: list = []
    real_leaf = run_steps._set_leaf

    def spy(sets, key, inline=False):
        leaf = real_leaf(sets, key, inline)
        leaves.append(leaf)
        return leaf

    monkeypatch.setattr(run_steps, "_set_leaf", spy)
    where = {"attrs.is_merge": False, "text": {"op": "matches", "value": words}}
    reply = served.json([
        {tool: {"where": where, **TEXT_PRODUCERS[tool]}},
        {"filter": {"where": "$1", "select": ["node_id"]}, "show": True},
        {"aggregate": {"where": "$1", "by": ["attrs.subject"], "metrics": ["count"]}},
    ])
    assert [step["status"] for step in reply["steps"]] == ["ok"] * 3, reply["steps"]
    assert [step["count"] for step in reply["steps"]] == [len(expected)] * 3
    assert _ids(_step(reply, 2)) == expected
    assert sum(group["count"] for group in _step(reply, 3)["groups"]) == len(expected)
    # Both later steps read "$1" as the set's membership subquery.
    assert len(leaves) == 2
    for leaf in leaves:
        assert leaf.get("field") == "node_id" and leaf.get("op") == "in", leaf
        assert [str(v).startswith("okg-run-set:") for v in leaf["values"]] == [True], leaf


def test_a_set_marker_outside_its_run_is_refused_not_matched(served):
    stale = served.call("filter", {"where": {"node_id": "okg-run-set:0123456789abcdef:1"}})
    assert stale.isError
    assert "a set lasts for one run call" in stale.content[0].text


def test_a_run_reads_one_version_of_the_graph(clean_runtime, okg_mcp_can_log_in, tmp_path):
    """A publish between calls: a run pinned to the old generation reads it
    in every step, and a step naming another generation is refused."""
    deployment = _corpus(tmp_path, clean_runtime)
    server = Server(clean_runtime, deployment)
    try:
        first = server.json([{"filter": {"where": {"type": "ticket"}}}])
        old = first["generation_id"]
        _, new = publish_nodes(
            clean_runtime, deployment, subtypes=SUBTYPES,
            nodes=[node("ticket:T-4", "ticket", "ticket T-4", key="T-4", status="Open")],
        )
        assert str(new) != str(old)
        pinned = server.json([
            {"filter": {"where": {"type": "ticket"}}, "show": True},
            {"filter": {"where": {"type": "ticket"}, "generation_id": new}},
        ], generation_id=old)
        assert pinned["steps"][0]["count"] == first["steps"][0]["count"]
        assert pinned["steps"][1]["status"] == "refused"
        assert "a run reads one version of the graph" in json.dumps(pinned["steps"][1])
    finally:
        server.close()


# --------------------------------------------------------------------------- #
# the run example in the served instructions, built from the catalog          #
# --------------------------------------------------------------------------- #


def _served_example(server: Server) -> list:
    instructions = server.mcp.instructions or ""
    line = next(line for line in instructions.splitlines() if "run(steps=" in line)
    start = line.index("run(steps=") + len("run(steps=")
    return json.JSONDecoder().raw_decode(line[start:])[0]


def test_the_served_run_example_names_only_this_catalog_and_answers(served):
    """The example names a type of this catalog and a key its records hold,
    no row value, and answers as written: one count per value."""
    steps = _served_example(served)
    assert [next(k for k in s if k != "show") for s in steps] == ["filter", "aggregate"]
    assert steps[0]["show"] is False
    subtype = steps[0]["filter"]["where"]["type"]
    (field,) = steps[1]["aggregate"]["by"]
    assert subtype in {spec.name for spec in SUBTYPES}
    assert field.startswith("attrs.")
    held = served.json([{"filter": {"where": {"type": subtype, field: {"op": "exists"}}}}])
    assert held["steps"][0]["count"] > 0
    reply = served.json(steps)
    total = sum(group["count"] for group in _step(reply, 2)["groups"])
    assert total == reply["steps"][0]["count"] > 0


def test_the_run_example_reads_a_small_type_the_largest_types_leave_out(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """Review F2: when every one of the largest types is over the example's
    bound, the start-up sample also reads the largest types under it, so an
    example is still served (here: 60 notes over a bound of 10; the 7
    commits are the largest type under it)."""
    from okg.substrate.mcp import dataset_card

    monkeypatch.setattr(dataset_card, "ATTR_SAMPLE_TYPES", 1)
    monkeypatch.setattr(dataset_card, "RUN_EXAMPLE_MAX_ROWS", 10)
    server = Server(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        steps = _served_example(server)
        assert steps[0]["filter"]["where"]["type"] == "commit"
        reply = server.json(steps)
        assert reply["steps"][0]["count"] == len(COMMITS) + 1
    finally:
        server.close()
